"""
varco_fastapi.job.poller
=========================
``JobPoller`` — background recovery for stale RUNNING jobs.

After a process restart, jobs that were in the RUNNING state never transition
to COMPLETED or FAILED — their asyncio.Tasks were lost but the store still
shows them as RUNNING.  ``JobPoller`` detects these and marks them FAILED.

This is only meaningful with persistent stores (``SaJobStore``,
``RedisJobStore``).  With ``InMemoryJobStore`` the store is empty after restart
so there are no stale jobs to recover.

Mirrors the ``OutboxRelay`` polling pattern from ``varco_core``:
    - ``start()`` spawns a background asyncio.Task
    - ``stop()`` cancels it and waits for shutdown
    - Poll interval and stale threshold are configurable

DESIGN: poller over locking / distributed coordination
    ✅ Simple — just a poll + mark-as-failed loop
    ✅ Stateless between polls — no coordination between instances needed
       (multiple pods may mark the same job failed, but that's idempotent)
    ✅ Configurable thresholds — tune for your expected job duration
    ❌ Not real-time — stale jobs persist until the next poll
    ❌ False positives if a job legitimately takes longer than stale_threshold
       → set stale_threshold conservatively (5-10x your p99 job duration)

Thread safety:  ✅ Uses asyncio.Task — runs in the event loop, no threads.
Async safety:   ✅ Background task cooperatively yields at each poll interval.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from varco_core.job.base import AbstractJobStore, JobStatus
from providify import Singleton, Inject

logger = logging.getLogger(__name__)


@Singleton
class JobPoller:
    """
    Background loop that marks stale RUNNING jobs as FAILED.

    Args:
        store:            ``AbstractJobStore`` to poll.
        stale_threshold:  Jobs in RUNNING status older than this are considered
                          stale (default: 5 minutes).
        poll_interval:    Seconds between polls (default: 60.0).
        batch_size:       Maximum stale jobs to process per poll cycle (default: 100).

    Lifecycle::

        poller = JobPoller(store=store, stale_threshold=timedelta(minutes=10))
        await poller.start()
        # ... runs in background ...
        await poller.stop()

    Thread safety:  ✅ asyncio.Task — runs in event loop, not threads.
    Async safety:   ✅ Background task cooperatively sleeps between polls.

    Edge cases:
        - No stale jobs → poll completes immediately, sleeps until next cycle
        - ``stop()`` called before ``start()`` → no-op
        - Store raises during poll → error logged, loop continues on next cycle
    """

    def __init__(
        self,
        store: Inject[AbstractJobStore],
        *,
        stale_threshold: timedelta | None = None,
        poll_interval: float = 60.0,
        batch_size: int = 100,
    ) -> None:
        self._store = store
        # 5-minute default is conservative — adjust to 10x your p99 job duration
        self._stale_threshold = stale_threshold or timedelta(minutes=5)
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        self._task: asyncio.Task | None = None  # type: ignore[type-arg]

    def __repr__(self) -> str:
        return (
            f"JobPoller("
            f"stale_threshold={self._stale_threshold}, "
            f"poll_interval={self._poll_interval}s)"
        )

    async def start(self) -> None:
        """
        Start the background polling task, running an immediate recovery pass first.

        On startup any job left in RUNNING state from a previous process
        (before a crash) is stuck — its asyncio.Task is gone but the store
        still shows it as RUNNING.  Calling ``_recover_stale_jobs()`` before the
        loop starts marks those jobs FAILED immediately rather than waiting up to
        ``poll_interval`` seconds for the first scheduled pass.

        DESIGN: eager recovery pass on start() over waiting for the first loop tick
            ✅ Stale jobs are visible as FAILED within milliseconds of startup,
               not up to ``poll_interval`` (default 60 s) later.
            ✅ Callers that check job status right after app startup get correct state.
            ❌ Adds one store query to the startup path — acceptable overhead.

        Calling ``start()`` multiple times is idempotent — the existing task is
        reused if it is still running.

        Thread safety:  ✅ asyncio.create_task is event-loop thread-safe.
        Async safety:   ✅ Safe to await from any async context.

        Edge cases:
            - With ``InMemoryJobStore`` the store is empty on restart so the recovery
              pass is a no-op (one cheap empty-list query).
            - If the recovery pass itself raises, the error is logged and the poller
              loop still starts — a failed recovery is better than no poller at all.
        """
        if self._task is not None and not self._task.done():
            # Already running — idempotent, don't create a second task
            return

        # Eagerly recover stale RUNNING jobs before the polling loop starts.
        # With persistent stores (SAJobStore, RedisJobStore) this is critical —
        # jobs stuck in RUNNING from a previous crash would otherwise appear
        # indefinitely stuck until the first scheduled poll cycle fires.
        try:
            await self._recover_stale_jobs()
        except Exception:
            # Recovery failure must never prevent the poller from starting
            logger.exception(
                "JobPoller: startup recovery pass failed; continuing anyway"
            )

        self._task = asyncio.create_task(
            self._poll_loop(),
            name="varco-job-poller",
        )
        logger.info(
            "JobPoller started (interval=%.1fs, stale_threshold=%s)",
            self._poll_interval,
            self._stale_threshold,
        )

    async def stop(self) -> None:
        """
        Stop the background polling task.

        Cancels the task and waits for it to finish.  Idempotent — safe to call
        if the poller was never started or has already stopped.

        Async safety:   ✅ Awaitable — waits for clean task shutdown.
        """
        if self._task is None or self._task.done():
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        finally:
            self._task = None
        logger.info("JobPoller stopped")

    async def _poll_loop(self) -> None:
        """
        Main polling loop — runs until cancelled.

        Each iteration:
        1. Fetch RUNNING jobs older than ``stale_threshold``
        2. Mark each as FAILED with message ``"stale_job_timeout"``
        3. Sleep for ``poll_interval`` seconds

        Edge cases:
            - Store raises → log exception, continue on next cycle (don't crash)
            - CancelledError → propagate (clean shutdown)
        """
        while True:
            try:
                await self._recover_stale_jobs()
            except asyncio.CancelledError:
                # Clean shutdown — let the loop exit
                raise
            except Exception:
                logger.exception("JobPoller: unexpected error during poll; will retry")
            await asyncio.sleep(self._poll_interval)

    async def _recover_stale_jobs(self) -> None:
        """
        Identify and mark stale RUNNING jobs as FAILED.

        A job is considered stale if it has been in RUNNING status with a
        ``started_at`` older than ``now - stale_threshold``.

        Edge cases:
            - ``started_at=None`` for a RUNNING job (shouldn't happen) → skipped
            - Store save fails for a specific job → logged, remaining jobs processed
        """
        now = datetime.now(UTC)
        stale_cutoff = now - self._stale_threshold

        running_jobs = await self._store.list_by_status(
            JobStatus.RUNNING,
            limit=self._batch_size,
        )

        stale_count = 0
        for job in running_jobs:
            # Skip jobs without started_at (defensive — should always be set)
            if job.started_at is None:
                continue
            # Skip recently started jobs — they may still be healthy
            if job.started_at >= stale_cutoff:
                continue

            # Mark as failed with a clear message for operators
            failed_job = job.as_failed(
                f"stale_job_timeout: job was RUNNING for more than {self._stale_threshold}"
            )
            try:
                await self._store.save(failed_job)
                stale_count += 1
            except Exception:
                logger.exception(
                    "JobPoller: failed to mark job %s as stale", job.job_id
                )

        if stale_count:
            logger.info("JobPoller: recovered %d stale RUNNING jobs", stale_count)


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "JobPoller",
]
