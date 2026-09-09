"""
varco_fastapi.retention
==========================
``RetentionLifecycle`` — the FastAPI-side startup/shutdown wiring for a
``varco_core.retention.RetentionScheduler`` (Plan 039 / S20,
§D-S20-lifecycle). Byte-for-byte the shipped
``ReliabilityLifecycle``/``TenancyLifecycle``/``MigrationLifecycle``/
``SecurityPostureLifecycle`` shape: ``startup()``/``shutdown()`` **and**
``start()``/``stop()`` aliases, resolved from the container, appended
(never prepended) to ``create_varco_app``'s lifespan components.

Imports only ``varco_core.retention``/``varco_core.job``/
``varco_core.schedule`` — never a backend, same seam rule as every other
``varco_fastapi`` lifecycle component.

⛔ **``varco_fastapi/varco_fastapi/lifespan.py`` is not modified by this
module or anywhere else in this plan.** ``RetentionLifecycle`` satisfies the
existing ``AbstractLifecycle`` Protocol (``lifespan.py:73-89``) and is
registered through the existing ``register()``/``VarcoLifespan`` mechanism —
same constraint Plan 041's JWKS refresher shares (CLAUDE.md).

DESIGN: appended, not prepended (§D-S20-lifecycle)
    ✅ Like ``ReliabilityLifecycle``, retention resolves bindings
       (``AbstractJobStore``, ``AbstractScheduleRepository``, optionally
       ``AbstractJobRunner``) that earlier lifecycle components create.
       Migrations and tenancy prepend because *they* must run before
       anything touches a table that may not exist yet
       (``app.py:375-378``, ``:394-395``); this must not.
    ✅ ``retention=None`` (the default) registers nothing and
       ``interval=0.0`` starts nothing — a default ``create_varco_app()``
       schedules nothing and deletes nothing (DoD item 1).

DESIGN: an app-supplied ``TaskRegistry``/``AbstractJobRunner`` are optional,
not required
    ✅ An app with no shared ``TaskRegistry`` bound still gets a working
       scheduler — ``startup()`` creates a private one and self-registers
       the ``"varco.retention.purge"`` handler on it, so
       ``ensure_schedules()``/dispatch work standalone.
    ✅ An app with no ``AbstractJobRunner`` bound still materializes
       schedules — it simply never calls ``recover()`` (mirrors
       ``RetentionScheduler(job_runner=None)``'s "do not dispatch" mode,
       Open question 1).
    ❌ Two apps that each want a *shared* TaskRegistry/JobRunner must bind
       them explicitly — silently sharing an implicit default would be
       the opposite of explicit wiring. Accepted.

Thread safety:  ⚠️ Not thread-safe — construct/use from the app's own
                    startup/shutdown lifespan hooks, single-threaded.
Async safety:   ✅ ``startup()``/``shutdown()`` are ``async def``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from varco_core.retention.policy import RetentionRegistry
    from varco_core.retention.scheduler import RetentionScheduler

__all__ = ["RetentionLifecycle"]


class RetentionLifecycle:
    """
    Drives a ``RetentionScheduler``'s startup/shutdown side effects.

    Args:
        registry: The app's ``RetentionRegistry``.
        container: A ``providify.DIContainer`` used to resolve
            ``AbstractJobStore``/``AbstractScheduleRepository`` (required)
            and ``TaskRegistry``/``AbstractJobRunner`` (optional — see
            module DESIGN block).
        interval: Seconds between sweeps, forwarded to
            ``RetentionScheduler``. ``0.0`` (default) — no background task
            at all, byte-identical to not wiring this in.

    Edge cases:
        - Constructing this class does nothing by itself — only
          ``startup()``/``start()`` resolves bindings and (if
          ``interval > 0``) starts a task.
    """

    def __init__(
        self, registry: RetentionRegistry, *, container: Any, interval: float = 0.0
    ) -> None:
        self._registry = registry
        self._container = container
        self._interval = interval
        self._scheduler: RetentionScheduler | None = None

    async def startup(self) -> None:
        """
        Resolve required bindings, build the ``RetentionScheduler``,
        ``ensure_schedules()``, then ``start()`` it (a no-op task-wise when
        ``interval <= 0.0``).

        Raises:
            LookupError: ``AbstractJobStore``/``AbstractScheduleRepository``
                is not resolvable from the container. The message names the
                missing interface.
        """
        from varco_core.job.base import AbstractJobRunner, AbstractJobStore
        from varco_core.job.task import TaskRegistry, VarcoTask
        from varco_core.retention.scheduler import RETENTION_TASK_NAME, RetentionScheduler
        from varco_core.schedule.repository import AbstractScheduleRepository

        job_store = await self._resolve(AbstractJobStore)
        schedule_repo = await self._resolve(AbstractScheduleRepository)

        try:
            task_registry = await self._container.aget(TaskRegistry)
        except LookupError:
            task_registry = TaskRegistry()

        try:
            job_runner = await self._container.aget(AbstractJobRunner)
        except LookupError:
            job_runner = None

        scheduler = RetentionScheduler(
            self._registry,
            schedule_repo=schedule_repo,
            job_store=job_store,
            task_registry=task_registry,
            job_runner=job_runner,
            interval=self._interval,
        )

        # Register the dispatch handler under its stable name, whether
        # task_registry was DI-resolved (shared with the rest of the app)
        # or created above (this scheduler's own, private registry) — either
        # way, recover() must be able to find "varco.retention.purge".
        if task_registry.get(RETENTION_TASK_NAME) is None:
            task_registry.register(VarcoTask(name=RETENTION_TASK_NAME, fn=scheduler.purge_policy))

        await scheduler.ensure_schedules()
        await scheduler.start()
        self._scheduler = scheduler

    async def shutdown(self) -> None:
        """Stop the scheduler started by ``startup()``, if any."""
        if self._scheduler is not None:
            await self._scheduler.stop()
            self._scheduler = None

    # ``start``/``stop`` aliases — VarcoLifespan drives every lifecycle
    # component through AbstractLifecycle's start()/stop() names (see
    # varco_fastapi.lifespan); mirrors ReliabilityLifecycle's identical pair
    # (varco_fastapi/reliability.py:114-123).
    async def start(self) -> None:
        await self.startup()

    async def stop(self) -> None:
        await self.shutdown()

    async def _resolve(self, interface: Any) -> Any:
        """
        Resolve ``interface`` from the container, or raise a message naming
        it explicitly.
        """
        try:
            return await self._container.aget(interface)
        except LookupError as exc:
            raise LookupError(
                f"RetentionLifecycle requires a binding for {interface.__name__} — "
                "bind it in the container before passing this lifecycle to "
                f"create_varco_app(retention=...). Original error: {exc}"
            ) from exc
