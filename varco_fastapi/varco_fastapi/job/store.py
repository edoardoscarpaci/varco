"""
varco_fastapi.job.store
========================
In-memory implementation of ``AbstractJobStore``.

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
import sys
from uuid import UUID

from varco_core.job.base import AbstractJobStore, Job, JobStatus
from providify import Singleton


@Singleton(priority=-sys.maxsize - 1)  # MIN_INT as priority
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

    async def save(self, job: Job) -> None:
        """
        Persist a job, replacing any existing entry with the same ``job_id``.

        Args:
            job: The ``Job`` to save.

        Thread safety:  ✅ Protected by asyncio.Lock — concurrent saves are serialized.
        Async safety:   ✅ Lock acquisition is awaitable.
        """
        async with self._get_lock():
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

    async def try_claim(self, job_id: UUID) -> Job | None:
        """
        Atomically transition a PENDING job to RUNNING state.

        The check (status == PENDING) and the write (status = RUNNING) happen
        inside a single ``asyncio.Lock`` acquisition, making this atomic within
        the same event loop.  This prevents duplicate execution when multiple
        concurrent recovery coroutines discover the same PENDING job.

        Args:
            job_id: UUID of the job to claim.

        Returns:
            The claimed ``Job`` in RUNNING state, or ``None`` if the job does
            not exist, is not PENDING, or was already claimed by another caller.

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
            # Transition to RUNNING — as_running() creates a new frozen Job instance
            claimed = job.as_running()
            self._jobs[job_id] = claimed
            return claimed


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "InMemoryJobStore",
]
