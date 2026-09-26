"""
varco_core.job.memory
========================
In-memory implementation of ``AbstractJobStore`` — the ``job_store``
seam's in-process default (Plan 047 §D-vocab).

Moved here from ``varco_fastapi.job.store`` (Plan 047 / Step 4) — the
default now lives beside every other in-process seam default
(``varco_core.event.memory``, ``varco_core.idempotency.memory``, ...)
rather than in a downstream package. ``varco_fastapi.job.store`` becomes
a re-export module so ``from varco_fastapi.job.store import
InMemoryJobStore`` keeps working (Migration table).

``InMemoryJobStore`` backs jobs with a plain ``dict`` protected by a lazily
created ``asyncio.Lock``.  All state is lost on process restart — suitable for
development, testing, and single-process deployments where jobs complete within
the process lifetime.

For durable job storage, use backend-specific implementations (e.g.
``SaJobStore`` in ``varco_sa`` or ``RedisJobStore`` in ``varco_redis``).

DESIGN: dict + lazy lock over asyncio.Queue or third-party job library
    ✅ No external dependencies — works out of the box
    ✅ Lazy lock creation avoids "no running event loop" at module import
    ✅ O(1) get/save by UUID
    ✅ Easy to inspect in tests — just read store._jobs
    ❌ No persistence — all jobs lost on restart
    ❌ No cross-process visibility — only meaningful in single-process deployments

Thread safety:  ✅ All mutations protected by asyncio.Lock.
Async safety:   ✅ Lock is created lazily inside the running event loop.
"""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from uuid import UUID

from providify import Fallback, Singleton

from varco_core.job.base import AbstractJobStore, Job, JobStatus, StaleLeaseError


@Fallback
@Singleton(qualifier="in_memory")
class InMemoryJobStore(AbstractJobStore):
    """
    Dict-backed in-memory job store.

    All jobs are stored in a plain ``dict[UUID, Job]`` protected by a lazy
    ``asyncio.Lock``.  State is not persisted across process restarts.

    Thread safety:  ✅ All mutations guarded by asyncio.Lock.
    Async safety:   ✅ Lock is created lazily on first access inside the event loop.

    Edge cases:
        - ``get(unknown_id)`` → returns ``None`` cleanly
        - ``delete(unknown_id)`` → no-op (does not raise)
        - ``list_by_status(status, limit=100)`` → returns at most ``limit`` jobs
          in insertion order (dict preserves insertion order in Python 3.7+)
        - Concurrent saves to the same job_id → last write wins (lock-protected)
    """

    #: Plan 011 / RD-5 — InMemoryJobStore persists run_at_wall/run_at_tz/
    #: run_at_fold verbatim (plain dict of the whole Job), so it declares
    #: zoned-schedule support unconditionally.
    supports_zoned_schedules = True

    def __init__(self) -> None:
        # Jobs stored by UUID — dict preserves insertion order (Python 3.7+)
        self._jobs: dict[UUID, Job] = {}
        # Lazy lock — created on first use inside the running event loop.
        # NEVER create asyncio.Lock at module level or __init__ — it must be
        # created after the event loop is started.
        self._lock: asyncio.Lock | None = None

    def __repr__(self) -> str:
        return f"InMemoryJobStore(jobs={len(self._jobs)})"

    def _get_lock(self) -> asyncio.Lock:
        """
        Return the asyncio.Lock, creating it lazily on first call.

        DESIGN: lazy lock over __init__ creation
            ✅ asyncio.Lock must be created inside a running event loop
            ✅ Safe to construct InMemoryJobStore outside async context (e.g. at module level)
            ❌ Tiny branch on every access — negligible overhead

        Returns:
            The shared asyncio.Lock for this store.
        """
        if self._lock is None:
            # Created inside the event loop on first use — safe at any point
            self._lock = asyncio.Lock()
        return self._lock

    async def save(self, job: Job, *, expected_epoch: int | None = None) -> None:
        """
        Persist a job, replacing any existing entry with the same ``job_id``.

        Args:
            job: The ``Job`` to save.
            expected_epoch: Fencing token (Plan 005 Phase 4, U-11 §3).
                ``None`` (default) — no fencing check, today's behaviour
                exactly. When supplied, the write is refused with
                ``StaleLeaseError`` if the stored job's ``lease_epoch`` no
                longer matches.

        Raises:
            StaleLeaseError: ``expected_epoch`` is supplied and does not
                match the stored ``lease_epoch``.

        Thread safety:  ✅ Protected by asyncio.Lock — concurrent saves are serialized.
        Async safety:   ✅ Lock acquisition is awaitable.
        """
        async with self._get_lock():
            if expected_epoch is not None:
                current = self._jobs.get(job.job_id)
                if current is None or current.lease_epoch != expected_epoch:
                    raise StaleLeaseError(
                        f"save() refused for job {job.job_id}: expected_epoch="
                        f"{expected_epoch} does not match stored lease_epoch "
                        f"({current.lease_epoch if current else 'job not found'})."
                    )
            self._jobs[job.job_id] = job

    async def get(self, job_id: UUID) -> Job | None:
        """
        Retrieve a job by its UUID.

        Args:
            job_id: UUID of the job to fetch.

        Returns:
            The ``Job`` if found, or ``None`` if no job with this ID exists.

        Thread safety:  ✅ Lock prevents torn reads during a concurrent save.
        """
        async with self._get_lock():
            return self._jobs.get(job_id)

    async def list_by_status(
        self,
        status: JobStatus,
        *,
        limit: int = 100,
    ) -> list[Job]:
        """
        Return jobs matching the given status, ordered by insertion (creation) time.

        Args:
            status: The ``JobStatus`` to filter by.
            limit:  Maximum number of results (default: 100).

        Returns:
            List of matching ``Job`` objects, capped to ``limit`` entries.

        Edge cases:
            - No matching jobs → empty list
            - ``limit=0`` → empty list (valid, not an error)
        """
        async with self._get_lock():
            # Iterate dict values (insertion order) and filter by status
            results: list[Job] = []
            for job in self._jobs.values():
                if job.status == status:
                    results.append(job)
                    if len(results) >= limit:
                        break
            return results

    async def delete(self, job_id: UUID) -> None:
        """
        Remove a job from the store.  No-op if the job does not exist.

        Args:
            job_id: UUID of the job to remove.

        Thread safety:  ✅ Protected by asyncio.Lock.
        """
        async with self._get_lock():
            # dict.pop with default avoids KeyError for unknown IDs
            self._jobs.pop(job_id, None)

    async def delete_where(
        self,
        *,
        status: JobStatus | Sequence[JobStatus] | None = None,
        completed_before: datetime | None = None,
        expires_before: datetime | None = None,
        limit: int | None = None,
    ) -> int:
        """
        Native bulk delete under the shared lock (Plan 005 Phase 6, U-18) —
        a single lock acquisition over the whole sweep instead of the ABC
        default's per-job ``list_by_status``/``delete`` round trips.

        Args:
            status: A single ``JobStatus`` or sequence of them.  ``None``
                (default) does not filter by status.
            completed_before: Only match jobs whose ``completed_at`` is set
                AND strictly before this timestamp.
            expires_before: Only match jobs whose ``expires_at`` is set AND
                strictly before this timestamp.
            limit: Maximum rows to delete.  ``None`` deletes every match.

        Returns:
            The number of jobs actually deleted.

        Raises:
            ValueError: No predicate at all was supplied.

        Thread safety:  ✅ Entire sweep runs under one asyncio.Lock acquisition.
        """
        if status is None and completed_before is None and expires_before is None:
            raise ValueError(
                "delete_where() requires at least one predicate (status, "
                "completed_before, or expires_before) — refusing to delete "
                "every row in the store. Pass an explicit predicate, e.g. "
                "delete_where(status=JobStatus.COMPLETED)."
            )

        if status is None:
            statuses: set[JobStatus] | None = None
        elif isinstance(status, JobStatus):
            statuses = {status}
        else:
            statuses = set(status)

        async with self._get_lock():
            to_delete: list[UUID] = []
            for job in self._jobs.values():
                if statuses is not None and job.status not in statuses:
                    continue
                if completed_before is not None and (
                    job.completed_at is None or job.completed_at >= completed_before
                ):
                    continue
                if expires_before is not None and (
                    job.expires_at is None or job.expires_at >= expires_before
                ):
                    continue
                to_delete.append(job.job_id)
                if limit is not None and len(to_delete) >= limit:
                    break

            for job_id in to_delete:
                self._jobs.pop(job_id, None)

            return len(to_delete)

    async def try_claim(
        self,
        job_id: UUID,
        *,
        owner_id: str | None = None,
        lease_ttl: float | None = None,
    ) -> Job | None:
        """
        Atomically transition a PENDING job to RUNNING state.

        The check (status == PENDING) and the write (status = RUNNING) happen
        inside a single ``asyncio.Lock`` acquisition, making this atomic within
        the same event loop.  This prevents duplicate execution when multiple
        concurrent recovery coroutines discover the same PENDING job.

        Plan 005 Phase 4 (U-17 §1, U-11): also honours
        ``run_at IS NULL OR run_at <= now``, and when ``lease_ttl`` is given,
        sets ``owner_id``/``lease_expires_at``/increments ``lease_epoch``.

        Args:
            job_id: UUID of the job to claim.
            owner_id: Identifies the lease holder. ``None`` (default) takes
                no lease — today's behaviour exactly.
            lease_ttl: Lease duration in seconds from now. ``None``
                (default) takes no lease.

        Returns:
            The claimed ``Job`` in RUNNING state, or ``None`` if the job does
            not exist, is not PENDING, ``run_at`` is in the future, or was
            already claimed by another caller.

        Thread safety:  ✅ Lock makes check-and-set atomic within a single process.
                           Cross-process safety requires a distributed store (Redis, SQL).
        Async safety:   ✅ Lock acquisition is awaitable.

        Edge cases:
            - Unknown ``job_id`` → returns ``None`` cleanly.
            - Job already RUNNING / terminal → returns ``None`` (not an error).
            - Concurrent calls on the same ``job_id`` within one process → exactly
              one returns the Job (the lock serializes them); all others see
              non-PENDING status and return ``None``.
        """
        async with self._get_lock():
            job = self._jobs.get(job_id)
            if job is None or job.status != JobStatus.PENDING:
                # Already claimed by another caller, or not found
                return None
            now = datetime.now(UTC)
            if job.run_at is not None and job.run_at > now:
                # Scheduled for the future — not yet eligible.
                return None
            # Transition to RUNNING — as_running() creates a new frozen Job instance
            claimed = job.as_running()
            if lease_ttl is not None:
                claimed = dataclasses.replace(
                    claimed,
                    owner_id=owner_id,
                    lease_expires_at=now + timedelta(seconds=lease_ttl),
                    lease_epoch=job.lease_epoch + 1,
                )
            self._jobs[job_id] = claimed
            return claimed

    async def claim_next(
        self,
        *,
        owner_id: str | None = None,
        lease_ttl: float | None = None,
        now: datetime | None = None,
    ) -> Job | None:
        """
        Claim the oldest eligible PENDING job under the shared lock.

        Args:
            owner_id: Forwarded to the claim.
            lease_ttl: Forwarded to the claim.
            now: The "current time" to evaluate ``run_at`` against.

        Returns:
            The claimed ``Job`` in RUNNING state, or ``None`` if no eligible
            PENDING job exists.

        Async safety: ✅ Runs under the store's lock — atomic within-process.
        """
        current = now if now is not None else datetime.now(UTC)
        async with self._get_lock():
            for job in self._jobs.values():
                if job.status != JobStatus.PENDING:
                    continue
                if job.run_at is not None and job.run_at > current:
                    continue
                claimed = job.as_running()
                if lease_ttl is not None:
                    claimed = dataclasses.replace(
                        claimed,
                        owner_id=owner_id,
                        lease_expires_at=current + timedelta(seconds=lease_ttl),
                        lease_epoch=job.lease_epoch + 1,
                    )
                self._jobs[job.job_id] = claimed
                return claimed
            return None

    async def renew(
        self,
        job_id: UUID,
        *,
        owner_id: str,
        epoch: int,
        lease_ttl: float,
    ) -> Job | None:
        """
        Heartbeat an in-progress lease under the shared lock.

        Args:
            job_id: The job whose lease is being renewed.
            owner_id: Must match the job's current ``owner_id``.
            epoch: Must match the job's current ``lease_epoch``.
            lease_ttl: New lease duration in seconds, from now.

        Returns:
            The renewed ``Job`` with an extended ``lease_expires_at``, or
            ``None`` if the job/owner/epoch does not match (fenced out).

        Async safety: ✅ Runs under the store's lock — atomic within-process.
        """
        async with self._get_lock():
            job = self._jobs.get(job_id)
            if job is None or job.owner_id != owner_id or job.lease_epoch != epoch:
                return None
            renewed = dataclasses.replace(
                job,
                lease_expires_at=datetime.now(UTC) + timedelta(seconds=lease_ttl),
            )
            self._jobs[job_id] = renewed
            return renewed

    async def reap_expired_leases(
        self,
        *,
        now: datetime | None = None,
        limit: int = 100,
    ) -> list[Job]:
        """
        Move RUNNING jobs whose lease has **expired** back to PENDING,
        incrementing ``lease_epoch`` to fence out the stalled owner.

        A RUNNING job with **no lease at all** (``lease_expires_at is None``)
        is skipped, not reaped. Plan 005 Step 48 scopes this method to
        "RUNNING rows whose ``lease_expires_at <= now``" — a NULL lease is
        not an expired lease, it is the absence of the signal this method
        reads.

        DESIGN: NULL lease is skipped rather than treated as reapable
            ✅ Preserves today's behaviour byte-for-byte for every caller
               that never passes ``lease_ttl`` — the plan's compatibility
               posture. Leases are opt-in; enabling the lease-aware poller
               must not retroactively reclassify unleased in-flight jobs.
            ✅ Keeps the two death signals separate and composable:
               ``JobPoller`` reaps leased jobs by expiry and falls back to
               the wall-clock age threshold for unleased ones.
            ❌ A job whose claimant crashed *before* taking a lease is
               recovered by the slower age threshold rather than instantly.
               Accepted: that is exactly today's behaviour, and the fix is
               to claim with ``lease_ttl``.

        Args:
            now: The "current time" to compare ``lease_expires_at`` against.
            limit: Maximum number of jobs to reap in one call.

        Returns:
            The list of jobs moved back to PENDING (post-reap state).

        Async safety: ✅ Runs under the store's lock — atomic within-process.
        """
        current = now if now is not None else datetime.now(UTC)
        async with self._get_lock():
            reaped: list[Job] = []
            for job in list(self._jobs.values()):
                if job.status != JobStatus.RUNNING:
                    continue
                # No lease taken → nothing to expire; the age threshold owns
                # this job, not lease reaping.
                if job.lease_expires_at is None:
                    continue
                if job.lease_expires_at > current:
                    continue
                new_job = dataclasses.replace(
                    job,
                    status=JobStatus.PENDING,
                    lease_epoch=job.lease_epoch + 1,
                    owner_id=None,
                    lease_expires_at=None,
                )
                self._jobs[job.job_id] = new_job
                reaped.append(new_job)
                if len(reaped) >= limit:
                    break
            return reaped


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "InMemoryJobStore",
]
