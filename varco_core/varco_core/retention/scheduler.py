"""
varco_core.retention.scheduler
=================================
``RetentionScheduler`` — the driver closing §D-S20-driver's gap 2: nothing
in ``varco_core.schedule`` calls ``ScheduleMaterializer.materialize()``
outside tests. This is a ``ScheduleRematerializer``-shaped loop (Plan 011
T2, ``varco_core/varco_core/job/reschedule.py``) and *only* a loop — it
computes nothing itself.

DESIGN: no fenced lease, runs on every pod (§D-S20-multiproc)
    ✅ Both halves of the exactly-once guarantee are shipped, unmodified:
       materialization converges via the materializer's own ``uuid5`` +
       ``AbstractJobStore.save()`` upsert (``materializer.py:24-30``);
       execution is exclusive via ``JobRunner.recover()``'s ``try_claim()``
       (``runner.py:533``). This scheduler adds neither a lease nor a second
       locking model — see ``materializer.py``'s own DESIGN block for why a
       synthetic lease row would corrupt every ``delete_where()``-based
       invariant downstream, ``JobRetentionTarget`` included.
    ✅ It computes nothing: no cron evaluation, no occurrence maths, no
       claim logic — every decision is delegated to
       ``ScheduleMaterializer.materialize()``, ``find_all_enabled()``,
       ``save()``, and ``JobRunner.recover()``. That is what keeps "no
       second scheduler" literally true (CLAUDE.md's standing rule).
    ❌ Two pods redundantly call ``find_all_enabled()``/``recover()`` every
       sweep. Accepted — cheap, and idempotent by construction.

DESIGN: ``job_runner=None`` means "do not dispatch" (Open question 1)
    ✅ ``recover()`` sweeps **every** PENDING task-payload job in the store,
       not just retention's — correct after Phase 1's fix, but broad. An
       app running its own dispatch loop passes ``job_runner=None`` so this
       scheduler only materializes, never calls ``recover()`` itself.
    ❌ An app with ``job_runner=None`` and no dispatch of its own never
       executes a materialized occurrence via this path. Accepted and
       documented — the CLI's ``--policy`` verb is the fallback
       (§D-S20-cli).

DESIGN: ``purge_policy`` is a bound method, not a module-level ``@varco_task``
(§D-S20-dispatch)
    ✅ A module-level task function would need a module-global registry to
       find the policy — the process-global mutable state §D-S20-shape
       rejected. A ``VarcoTask`` wrapping ``scheduler.purge_policy`` closes
       over *this* scheduler's own ``RetentionRegistry``, so two apps in
       one process stay independent.
    ❌ Requires the app to register
       ``VarcoTask(name="varco.retention.purge", fn=scheduler.purge_policy)``
       itself — one line, documented in the README snippet.

Thread safety:  N/A — single-process, ``asyncio``-only.
Async safety:   ✅ ``asyncio.Task``/state are created lazily inside
                   ``start()``, never at ``__init__``/module scope
                   (CLAUDE.md's lazy-lock rule, applied to the task too).
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import time
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from varco_core.retention.policy import RetentionResult
from varco_core.schedule.entity import Schedule
from varco_core.schedule.materializer import ScheduleMaterializer
from varco_core.service.tenant import tenant_context

if TYPE_CHECKING:
    from varco_core.job.base import AbstractJobStore
    from varco_core.retention.policy import RetentionPolicy, RetentionRegistry
    from varco_core.schedule.repository import AbstractScheduleRepository

logger = logging.getLogger(__name__)

__all__ = ["RetentionPolicyNotFoundError", "RetentionScheduler", "execute_policy"]

#: The stable TaskRegistry name every materialized retention occurrence is
#: dispatched under — see §D-S20-dispatch. Registered by the app:
#: ``task_registry.register(VarcoTask(name=RETENTION_TASK_NAME, fn=scheduler.purge_policy))``.
RETENTION_TASK_NAME = "varco.retention.purge"


class RetentionPolicyNotFoundError(Exception):
    """
    Raised by ``purge_policy()`` when the named policy is not (or is no
    longer) registered.

    Edge cases:
        - A policy renamed between materialization and execution: the
          in-flight ``Job`` was claimed (RUNNING) before this raises, and
          ``JobRunner.recover()`` has no mechanism to un-claim it — the job
          stays RUNNING until a ``JobPoller`` lease-reap eventually notices
          it stalled. Documented as a Pitfall, not silently accepted.
    """


async def execute_policy(policy: RetentionPolicy) -> RetentionResult:
    """
    Execute one ``RetentionPolicy`` once, now — the shared implementation
    behind both ``RetentionScheduler.purge_policy()`` (the scheduled/dispatch
    path) and ``varco retention prune --policy`` (the CLI path, §D-S20-cli)
    — so an operator's manual run and the scheduled run take the identical
    code path, never a second one to reason about.

    Args:
        policy: The policy to run.

    Returns:
        A ``RetentionResult`` aggregating every batch across every tenant
        (or the single platform-wide call when ``tenant_ids is None``).

    Edge cases:
        - ``tenant_ids=None`` → one call, no ``tenant_context()`` entered
          (ambient ``current_tenant()`` stays whatever it already was —
          typically ``None`` in a background job), ``tenant_id=None``
          effectively reaches the target's own ``current_tenant()`` read.
        - ``tenant_ids=("a", "b")`` → one call per tenant, each inside its
          own ``tenant_context(tid)`` block (§D-S20-tenancy).
        - A batch whose matched count is smaller than ``policy.batch_size``
          ends the sweep for that tenant — no more rows match.
        - Reaching ``policy.max_batches`` while every batch was still full
          sets ``truncated=True`` and stops — the next scheduled occurrence
          continues where this one left off.
        - Any exception raised mid-sweep is caught, recorded in
          ``RetentionResult.error``, and the function returns normally — a
          broken policy must not raise out of a sweep that may be executing
          other policies too (the scheduler's dispatch loop calls this once
          per occurrence, already isolated per ``Job``).

    Async safety: ✅ All I/O is awaited; ``tenant_context()`` is a plain
        (synchronous) ``ContextVar`` context manager, safe to nest inside
        an ``async def``.
    """
    start = time.monotonic()
    tenant_ids: tuple[str | None, ...] = (
        policy.tenant_ids if policy.tenant_ids is not None else (None,)
    )
    # RetentionPolicy.older_than is a RELATIVE window ("prune entries older
    # than 30 days") but RetentionTarget.purge()'s older_than is an ABSOLUTE
    # cutoff (matches every shipped verb's own older_than=datetime
    # parameter, e.g. AbstractDeadLetterQueue.delete_where). Resolved once
    # per execute_policy() call (not per-batch) so every batch/tenant in one
    # sweep uses the identical cutoff.
    cutoff = datetime.now(UTC) - policy.older_than if policy.older_than is not None else None

    total_examined: int | None = 0
    examined_unknown = False
    total_deleted = 0
    total_would_delete = 0
    batches = 0
    truncated = False
    skipped_reason: str | None = None
    error: str | None = None

    try:
        for tenant_id in tenant_ids:
            for _ in range(policy.max_batches):
                if tenant_id is not None:
                    with tenant_context(tenant_id):
                        outcome = await policy.target.purge(
                            older_than=cutoff,
                            limit=policy.batch_size,
                            dry_run=policy.dry_run,
                        )
                else:
                    outcome = await policy.target.purge(
                        older_than=cutoff,
                        limit=policy.batch_size,
                        dry_run=policy.dry_run,
                    )

                batches += 1
                total_deleted += outcome.deleted
                total_would_delete += outcome.would_delete
                if outcome.examined is None:
                    examined_unknown = True
                elif not examined_unknown:
                    total_examined = (total_examined or 0) + outcome.examined
                if outcome.skipped_reason:
                    skipped_reason = outcome.skipped_reason
                    break

                matched = outcome.would_delete if policy.dry_run else outcome.deleted
                if matched < policy.batch_size:
                    break
            else:
                truncated = True
    except Exception as exc:  # noqa: BLE001 — recorded, not swallowed silently
        error = str(exc)

    result = RetentionResult(
        policy=policy.name,
        kind=policy.target.kind,
        examined=None if examined_unknown else total_examined,
        deleted=total_deleted,
        would_delete=total_would_delete,
        batches=batches,
        truncated=truncated,
        dry_run=policy.dry_run,
        skipped_reason=skipped_reason,
        duration_s=time.monotonic() - start,
        error=error,
    )

    # One INFO log per occurrence (§D-S20-obs) — always emitted, independent
    # of the opt-in metric below.
    logger.info(
        "RetentionScheduler: policy=%r kind=%r dry_run=%s deleted=%d would_delete=%d "
        "batches=%d truncated=%s skipped_reason=%r error=%r duration_s=%.3f",
        result.policy,
        result.kind,
        result.dry_run,
        result.deleted,
        result.would_delete,
        result.batches,
        result.truncated,
        result.skipped_reason,
        result.error,
        result.duration_s,
    )

    # A no-op unless install_retention_metrics() was called — see that
    # module's docstring for why this is always safe to call unconditionally.
    from varco_core.observability.retention import record_retention_result  # noqa: PLC0415

    record_retention_result(result)

    return result


class RetentionScheduler:
    """
    Materializes every registered policy's ``Schedule`` and (optionally)
    dispatches due occurrences, on a timer.

    Args:
        registry: The app's ``RetentionRegistry``.
        schedule_repo: The ``AbstractScheduleRepository`` to sweep —
            typically shared with any other ``varco_core.schedule`` use in
            the app (this scheduler only ever touches schedules whose
            ``schedule_id`` it itself derived from a registered policy name
            — see ``sweep_once()``'s skip rule).
        job_store: The same ``AbstractJobStore``
            ``ScheduleMaterializer``/``AbstractJobRunner`` use.
        task_registry: The app's ``TaskRegistry`` — forwarded to
            ``job_runner.recover()``. Register
            ``VarcoTask(name=RETENTION_TASK_NAME, fn=self.purge_policy)``
            on it before the first sweep for dispatch to find the handler.
        job_runner: An ``AbstractJobRunner`` implementing ``recover()``, or
            ``None`` (default) to materialize only, never dispatch — see
            the module DESIGN block.
        interval: Seconds between sweeps. ``0.0`` (default) — ``start()``
            spawns no background task at all, byte-identical to not using
            this feature.

    Async safety: ✅ ``asyncio.Task`` created lazily inside ``start()``,
        never at ``__init__``/module scope.
    """

    def __init__(
        self,
        registry: RetentionRegistry,
        *,
        schedule_repo: AbstractScheduleRepository,
        job_store: AbstractJobStore,
        task_registry: Any,
        job_runner: Any | None = None,
        interval: float = 0.0,
    ) -> None:
        self._registry = registry
        self._schedule_repo = schedule_repo
        self._job_store = job_store
        self._task_registry = task_registry
        self._job_runner = job_runner
        self._interval = interval
        self._materializer = ScheduleMaterializer(job_store=job_store)
        self._task: asyncio.Task[None] | None = None
        self._stopped = False

    async def start(self) -> None:
        """Start the periodic sweep. No-op (no task created) when
        ``interval <= 0.0`` — mirrors ``ScheduleRematerializer.start()``."""
        if self._interval <= 0.0:
            return
        self._stopped = False
        self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        """Cancel the periodic sweep, if one was started. Safe to call
        before ``start()`` — a no-op."""
        self._stopped = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run_forever(self) -> None:
        while not self._stopped:
            try:
                await self.sweep_once()
            except Exception:
                logger.exception("RetentionScheduler sweep failed")
            await asyncio.sleep(self._interval)

    async def ensure_schedules(self) -> None:
        """
        Idempotent upsert of one ``Schedule`` per registered policy —
        ``schedule_id=registry.schedule_id_for(name)``,
        ``task_name=RETENTION_TASK_NAME``, ``payload={"policy": name}``.

        Safe to call repeatedly (e.g. once per deploy): an existing
        ``Schedule`` with the same ``schedule_id`` is updated in place
        (its own ``pk`` reused) rather than duplicated.
        """
        existing_by_schedule_id = {
            s.schedule_id: s for s in await self._schedule_repo.find_all_enabled()
        }
        for name in self._registry:
            policy = self._registry.get(name)
            assert policy is not None  # names come from the registry's own keys
            schedule_id = self._registry.schedule_id_for(name)
            prior = existing_by_schedule_id.get(schedule_id)
            schedule = Schedule(
                schedule_id=schedule_id,
                cron_expr=policy.cron_expr,
                timezone=policy.timezone,
                enabled=policy.enabled,
                task_name=RETENTION_TASK_NAME,
                payload={"policy": name},
            )
            if prior is not None:
                # pk is not a constructor kwarg (PKStrategy.UUID_AUTO,
                # meta.py:575-589) — reuse the existing row's pk so save()
                # takes the update branch instead of inserting a duplicate.
                schedule.pk = prior.pk
            await self._schedule_repo.save(schedule)

    async def sweep_once(self) -> int:
        """
        Materialize every registered policy's due occurrence(s), then (if
        ``job_runner`` was given) dispatch via ``recover()``.

        Returns:
            The number of jobs materialized this sweep (not the number
            dispatched/executed).

        Edge cases:
            - A ``Schedule`` whose ``schedule_id`` is not one this
              registry derived (i.e. belongs to the app's own,
              non-retention use of ``varco_core.schedule``) is skipped —
              this scheduler never touches a schedule it did not create.
        """
        known_ids = {self._registry.schedule_id_for(name) for name in self._registry}
        materialized = 0
        for schedule in await self._schedule_repo.find_all_enabled():
            if schedule.schedule_id not in known_ids:
                continue
            jobs = await self._materializer.materialize(schedule)
            if jobs:
                updated = dataclasses.replace(schedule, last_materialized_at=datetime.now(UTC))
                await self._schedule_repo.save(updated)
                materialized += len(jobs)

        if self._job_runner is not None:
            await self._job_runner.recover(self._task_registry)

        return materialized

    async def purge_policy(self, *, policy: str) -> RetentionResult:
        """
        Run one policy once, now — the handler ``VarcoTask`` invokes via
        ``TaskRegistry.invoke()`` (§D-S20-dispatch), and what the CLI's
        ``--policy`` verb also calls (§D-S20-cli).

        Args:
            policy: The registered policy name.

        Returns:
            A ``RetentionResult``.

        Raises:
            RetentionPolicyNotFoundError: ``policy`` is not registered —
                e.g. it was renamed between materialization and execution.
        """
        found = self._registry.get(policy)
        if found is None:
            raise RetentionPolicyNotFoundError(
                f"No retention policy named {policy!r} is registered — it may have "
                "been renamed or removed between materialization and execution."
            )
        return await execute_policy(found)
