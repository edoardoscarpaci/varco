"""
varco_fastapi.job.runner
=========================
``JobRunner`` — asyncio-Task-per-job execution engine.

Each submitted job gets its own ``asyncio.Task``.  The runner:
1. Saves the job as PENDING in the store
2. Creates an asyncio.Task that transitions PENDING → RUNNING → COMPLETED/FAILED
3. Fires webhook callbacks on completion (with auth forwarding)
4. Emits ``JobProgressEvent`` to the event bus on each state transition (SSE)
5. Optionally creates OpenTelemetry spans per job

``job_progress()`` is a context-function that job coroutines can call to
report progress.  It reads the current job ID from a ContextVar set by the runner.

``job_progress_var`` is the ContextVar that holds the current job's progress
emitter function while a job is running.

DESIGN: asyncio.Task per job over a thread pool or Celery
    ✅ No external dependencies — asyncio tasks are cheap (< 1 KB each)
    ✅ Coroutines share the event loop — no serialization of arguments needed
    ✅ cancel() maps directly to Task.cancel() — immediate and cooperative
    ✅ max_concurrent maps to asyncio.Semaphore — bulkhead at zero overhead
    ❌ Not durable — in-flight tasks lost on process restart
       (JobPoller recovers stale RUNNING jobs marked in the store)
    ❌ Not distributed — tasks run in the same process as the web server
       (use Celery / Arq / SAQ for multi-process job execution)

Thread safety:  ✅ _tasks dict and _semaphore are accessed only from the event loop.
Async safety:   ✅ All mutations are protected by the event loop's single-thread guarantee.
                   asyncio.Semaphore and _tasks are NOT thread-safe — use only from async code.
"""

from __future__ import annotations

import asyncio
import logging
from contextvars import ContextVar
from typing import Any, Coroutine
from uuid import UUID, uuid4

from varco_core.job.base import AbstractJobRunner, AbstractJobStore, Job, JobStatus
from varco_fastapi.job.response import JobProgressEvent
from varco_core.event import AbstractEventBus
from providify import Inject, Instance, Singleton

logger = logging.getLogger(__name__)

# ── Progress ContextVar ────────────────────────────────────────────────────────

# Set by JobRunner before executing each job coroutine.
# Holds a callable (progress, message) → None that job code can call
# to report progress without importing the runner.
_current_job_id_var: ContextVar[UUID | None] = ContextVar(
    "varco_current_job_id", default=None
)

# Holds the event bus emit function so job_progress() can publish without
# directly holding a reference to the runner.
_progress_emit_var: ContextVar[Any | None] = ContextVar(
    "varco_progress_emit", default=None
)


def job_progress(progress: float, message: str | None = None) -> None:
    """
    Report progress from within a running job coroutine.

    Reads the current job ID from ``_current_job_id_var`` (set by ``JobRunner``
    before the coroutine starts) and calls the progress emitter function stored
    in ``_progress_emit_var``.

    Must be called from inside a job coroutine — calling it outside a job context
    is a no-op (emitter will be ``None``).

    Args:
        progress: Progress fraction 0.0–1.0.
        message:  Optional human-readable status message.

    Edge cases:
        - Called outside a job context → no-op; no exception raised
        - Called with ``progress > 1.0`` or ``progress < 0.0`` → passed through;
          Pydantic validation in ``JobProgressEvent`` will catch it
    """
    emitter = _progress_emit_var.get(None)
    if emitter is None:
        # Not running inside a job context — silently ignore
        return
    job_id = _current_job_id_var.get(None)
    if job_id is None:
        return
    # Fire-and-forget — we're in a sync context so can't await
    # Use the emitter which schedules the coroutine on the event loop
    emitter(job_id, progress, message)


# ── JobRunner ─────────────────────────────────────────────────────────────────


@Singleton
class JobRunner(AbstractJobRunner):
    """
    asyncio-Task-per-job execution engine.

    Manages the full lifecycle of background jobs: submission, execution,
    cancellation, graceful shutdown, and optional webhook callbacks.

    Args:
        store:                 ``AbstractJobStore`` for job persistence.
        event_bus:             Optional event bus for progress events (SSE).
                               If ``None``, state transitions are not published.
        max_concurrent:        Optional cap on in-flight jobs (``asyncio.Semaphore``).
                               ``None`` means no limit.
        callback_retry_policy: Optional ``RetryPolicy`` for webhook callbacks.
                               ``None`` means no retry — single attempt.
        enable_otel:           If ``True``, create OTel spans per job.
                               Auto-disabled if ``opentelemetry`` is not installed.
        tracer_name:           OpenTelemetry tracer name.

    Lifecycle::

        runner = JobRunner(store=InMemoryJobStore())
        await runner.start()
        await runner.submit(job_id, coro)
        # ... later ...
        await runner.stop()

    Thread safety:  ⚠️ Conditional — _tasks and _semaphore are asyncio primitives.
                       Use only from async code on the same event loop.
    Async safety:   ✅ All methods are async-safe within a single event loop.

    Edge cases:
        - ``stop(timeout=30)`` cancels in-flight tasks and waits up to 30s
        - ``cancel(unknown_id)`` returns ``False`` without raising
        - If the event bus is not set, progress events are silently dropped
    """

    def __init__(
        self,
        store: Inject[AbstractJobStore],
        *,
        event_bus: Instance[AbstractEventBus] | None = None,
        max_concurrent: int | None = None,
        callback_retry_policy: Any | None = None,
        enable_otel: bool = True,
        tracer_name: str = "varco_fastapi.job",
    ) -> None:
        self._store = store
        self._event_bus = event_bus
        self._max_concurrent = max_concurrent
        self._callback_retry_policy = callback_retry_policy
        self._enable_otel = enable_otel and self._check_otel()
        self._tracer_name = tracer_name

        # asyncio.Task registry — keyed by job_id for cancel() and stop()
        # Lazy: populated at submit() time, never at __init__ (no event loop yet)
        self._tasks: dict[UUID, asyncio.Task[Any]] = {}
        # Lazy semaphore — created on first submit() call inside the event loop
        self._semaphore: asyncio.Semaphore | None = None

        # Signals whether start() has been called
        self._started = False

    def __repr__(self) -> str:
        return (
            f"JobRunner("
            f"in_flight={len(self._tasks)}, "
            f"max_concurrent={self._max_concurrent!r})"
        )

    @staticmethod
    def _check_otel() -> bool:
        """Return True if the opentelemetry SDK is importable."""
        try:
            import opentelemetry.trace  # noqa: F401

            return True
        except ImportError:
            return False

    def _get_semaphore(self) -> asyncio.Semaphore | None:
        """
        Return the concurrency semaphore, creating it lazily on first call.

        DESIGN: lazy semaphore over __init__ creation
            ✅ asyncio.Semaphore must be created inside a running event loop
            ✅ max_concurrent=None → semaphore stays None → no overhead

        Returns:
            asyncio.Semaphore if max_concurrent was set, else None.
        """
        if self._max_concurrent is None:
            return None
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self._max_concurrent)
        return self._semaphore

    async def start(self) -> None:
        """
        Start the runner.

        Currently a no-op — the runner starts accepting jobs immediately.
        Reserved for future use (e.g. warm-up, persistent store connection).
        """
        if self._started:
            raise RuntimeError("Job Runner already starteds")
        self._started = True
        if (
            self._event_bus
            and isinstance(self._event_bus, Instance)
            and self._event_bus.resolvable()
        ):
            self._event_bus = await self._event_bus.aget()
        logger.info("JobRunner started (max_concurrent=%s)", self._max_concurrent)

    async def stop(self, *, timeout: float = 30.0) -> None:
        """
        Gracefully shut down the runner.

        Cancels all in-flight tasks and waits up to ``timeout`` seconds for them
        to finish.  Tasks that do not finish within the timeout are abandoned.

        Args:
            timeout: Maximum seconds to wait for in-flight tasks.

        Edge cases:
            - No in-flight tasks → returns immediately
            - Tasks that swallow CancelledError → abandoned after timeout
        """
        self._started = False

        if not self._tasks:
            logger.info("JobRunner stopped (no in-flight tasks)")
            return

        in_flight = list(self._tasks.values())
        logger.info(
            "JobRunner stopping — cancelling %d in-flight tasks", len(in_flight)
        )

        for task in in_flight:
            task.cancel()

        # Wait for all tasks to acknowledge cancellation
        done, pending = await asyncio.wait(in_flight, timeout=timeout)
        if pending:
            logger.warning(
                "JobRunner: %d tasks did not cancel within %.1fs — abandoning",
                len(pending),
                timeout,
            )

        self._tasks.clear()
        logger.info("JobRunner stopped")

    async def enqueue(self, job: Job, coro: Coroutine[Any, Any, Any]) -> None:
        """
        Persist ``job`` to the store as PENDING, then schedule the coroutine.

        This is the **primary submission API**.  Callers should always use
        ``enqueue()`` rather than ``submit()`` directly, because it guarantees
        the job is durably stored before the asyncio.Task is created.  This
        makes the job recoverable by ``JobPoller`` even if the process crashes
        between persistence and execution.

        Steps:
        1. ``self._store.save(job)`` — PENDING record written first.
        2. ``self.submit(job.job_id, coro)`` — asyncio.Task scheduled.

        Args:
            job:  A ``Job`` in PENDING state to persist and execute.
            coro: The coroutine to run in the background.

        Raises:
            Any store-specific error from ``store.save()``.  If saving fails,
            ``coro`` is closed so no coroutine is leaked.

        Async safety:   ✅ Returns after scheduling; does not wait for task completion.

        Edge cases:
            - Store raises → coro is closed, exception propagates to caller.
            - Process crashes after save but before task starts → job stays PENDING.
              JobPoller will transition it to FAILED on next recovery pass.
        """
        try:
            # Persist BEFORE creating the task — crash-safe ordering
            await self._store.save(job)
        except Exception:
            # Close the coroutine to prevent "coroutine was never awaited" warning
            coro.close()
            raise
        await self.submit(job.job_id, coro)

    async def submit(self, job_id: UUID, coro: Coroutine[Any, Any, Any]) -> None:
        """
        Schedule a coroutine for background execution under a pre-existing job.

        **Prefer ``enqueue()`` over calling this directly.**  This method assumes
        the job already exists in the store in PENDING state.  Calling it without
        a prior ``store.save()`` causes ``_run_job`` to log an error and silently
        close the coroutine.

        Creates an asyncio.Task that manages the full PENDING → RUNNING →
        COMPLETED/FAILED lifecycle, publishes progress events, and fires
        webhook callbacks on completion.

        Args:
            job_id: UUID of the job (must already exist in the store as PENDING).
            coro:   Coroutine to execute.  Should not capture request-scoped state
                    (ContextVars are reset after the request ends).

        Thread safety:  ⚠️ Must be called from the event loop thread.
        Async safety:   ✅ asyncio.create_task is safe from async context.

        Edge cases:
            - ``job_id`` not found in store → task logs error and closes coro.
            - ``coro`` raises immediately → job transitions to FAILED.
            - Duplicate ``job_id`` → replaces previous task entry (last write wins).
        """
        task = asyncio.create_task(
            self._run_job(job_id, coro),
            name=f"job-{job_id}",
        )
        self._tasks[job_id] = task
        # Remove from registry when done to avoid memory leak
        task.add_done_callback(lambda t: self._tasks.pop(job_id, None))

    async def enqueue_task(
        self,
        task: Any,  # VarcoTask — typed as Any to avoid circular import at module level
        *args: Any,
        callback_url: str | None = None,
        auth_snapshot: dict[str, Any] | None = None,
        request_token: str | None = None,
        **kwargs: Any,
    ) -> UUID:
        """
        Submit a named task for background execution with a serialized ``TaskPayload``.

        Unlike ``enqueue(job, coro)`` which takes a plain coroutine, this method
        builds a ``TaskPayload`` from the task's name and call arguments, attaches
        it to the ``Job`` record, and persists everything to the store before
        scheduling execution.

        The stored ``TaskPayload`` makes the job recoverable after a process
        restart via ``recover(registry)``.  On recovery, the runner re-creates
        the coroutine by looking up the task by name in the ``TaskRegistry`` and
        calling it with the stored args.

        Args:
            task:          The ``VarcoTask`` to execute.
            *args:         Positional arguments forwarded to the task function.
            callback_url:  Optional webhook URL for completion notification.
            auth_snapshot: Serialized ``AuthContext`` from the originating request.
            request_token: Raw Bearer JWT for audit trail and callback auth.
            **kwargs:      Keyword arguments forwarded to the task function.

        Returns:
            The ``UUID`` of the newly created job.

        Raises:
            Any store-specific error from ``store.save()``.  If saving fails,
            the coroutine is closed to prevent resource leaks.

        Async safety:   ✅ Returns after scheduling; does not block until completion.

        Edge cases:
            - ``args`` / ``kwargs`` must be JSON-serializable.  The job will fail at
              recovery if non-serializable values slip through.
            - Crash after ``store.save()`` but before task execution → job stays PENDING;
              ``recover()`` will re-submit on next startup.
        """

        payload = task.payload(*args, **kwargs)

        job = Job(
            job_id=uuid4(),
            callback_url=callback_url,
            auth_snapshot=auth_snapshot,
            request_token=request_token,
            task_payload=payload,
        )

        coro = task(*args, **kwargs)
        try:
            # Persist PENDING record with task_payload before scheduling
            await self._store.save(job)
        except Exception:
            coro.close()  # prevent "coroutine was never awaited" ResourceWarning
            raise

        await self.submit(job.job_id, coro)
        return job.job_id

    async def recover(self, registry: Any) -> int:  # registry: TaskRegistry
        """
        Re-submit all PENDING jobs that have a ``task_payload`` after process restart.

        For each PENDING job with a ``task_payload`` in the store:
        1. ``store.try_claim(job_id)`` — atomically claim PENDING → RUNNING.
           Only one runner instance succeeds per job, preventing double-execution
           in multi-replica deployments.
        2. Look up the task name in ``registry``.
        3. Re-create the coroutine from the stored args and schedule via
           ``_submit_claimed()``, which skips the duplicate PENDING → RUNNING
           transition (the job is already RUNNING from ``try_claim()``).

        Args:
            registry: The ``TaskRegistry`` with all recoverable tasks registered.
                      Must be populated before calling ``recover()``.

        Returns:
            The number of jobs successfully re-submitted.

        Edge cases:
            - ``try_claim()`` returns ``None`` → job claimed by another instance; skip.
            - Task name not in registry → log warning; job stays RUNNING (won't progress).
            - ``task_payload is None`` → job submitted via bare-coro path; skip (no recovery).
            - No PENDING jobs with task_payload → returns 0.

        Thread safety:  ✅ ``try_claim()`` provides atomicity per job.
        Async safety:   ✅ All I/O awaited; safe to call from lifespan startup handler.
        """
        # Find all PENDING jobs that have a task_payload
        pending_jobs = await self._store.list_by_status(JobStatus.PENDING)
        recoverable = [j for j in pending_jobs if j.task_payload is not None]

        if not recoverable:
            logger.info("JobRunner.recover(): no recoverable PENDING jobs found")
            return 0

        recovered = 0
        for job in recoverable:
            # Atomically claim the job — prevents duplicate execution in multi-instance
            claimed = await self._store.try_claim(job.job_id)
            if claimed is None:
                # Another runner instance already claimed this job — skip
                logger.debug(
                    "JobRunner.recover(): job %s already claimed by another instance, skipping",
                    job.job_id,
                )
                continue

            # Look up the task function by the persisted name
            task_fn = registry.get(claimed.task_payload.task_name)  # type: ignore[union-attr]
            if task_fn is None:
                logger.warning(
                    "JobRunner.recover(): task %r not found in registry for job %s — "
                    "job will stay RUNNING but never complete. "
                    "Ensure VarcoCRUDRouter.build_router() was called before recover().",
                    claimed.task_payload.task_name,  # type: ignore[union-attr]
                    claimed.job_id,
                )
                continue

            # Re-create the coroutine from stored args and schedule it
            coro = registry.invoke(claimed.task_payload)  # type: ignore[union-attr]
            await self._submit_claimed(claimed, coro)
            recovered += 1
            logger.info(
                "JobRunner.recover(): re-submitted job %s (task=%r)",
                claimed.job_id,
                claimed.task_payload.task_name,  # type: ignore[union-attr]
            )

        logger.info(
            "JobRunner.recover(): recovered %d/%d PENDING jobs",
            recovered,
            len(recoverable),
        )
        return recovered

    async def _submit_claimed(
        self,
        job: Job,
        coro: Coroutine[Any, Any, Any],
    ) -> None:
        """
        Schedule a coroutine for an already-RUNNING job (claimed via ``try_claim()``).

        Unlike ``submit()``, this method skips the PENDING → RUNNING transition
        because ``try_claim()`` already performed it atomically.  Using this
        instead of ``submit()`` prevents a second ``as_running()`` call, which
        would raise ``ValueError`` (already RUNNING).

        Args:
            job:  The ``Job`` already in RUNNING state (returned by ``try_claim()``).
            coro: The coroutine to execute.

        Async safety:   ✅ Returns immediately after scheduling the asyncio.Task.

        Edge cases:
            - Duplicate ``job_id`` in ``_tasks`` → replaces previous entry (last write wins).
        """
        task = asyncio.create_task(
            self._run_claimed_job(job, coro),
            name=f"job-{job.job_id}",
        )
        self._tasks[job.job_id] = task
        task.add_done_callback(lambda t: self._tasks.pop(job.job_id, None))

    async def _run_claimed_job(
        self,
        job: Job,
        coro: Coroutine[Any, Any, Any],
    ) -> None:
        """
        Execute a pre-claimed (already-RUNNING) job coroutine.

        Like ``_run_job()`` but skips the PENDING → RUNNING fetch-and-transition
        because the job was already claimed by ``try_claim()`` before this method
        is called.  This avoids a redundant store lookup and prevents the
        ``ValueError: Cannot transition from RUNNING`` error.

        Args:
            job:  The ``Job`` already in RUNNING state.
            coro: The coroutine to execute.

        Thread safety:  ✅ Runs in single event loop — no cross-task shared state.
        Async safety:   ✅ All I/O is awaited.

        Edge cases:
            - CancelledError → job transitions to CANCELLED
            - Any other exception → job transitions to FAILED
        """
        # Set ContextVar so job_progress() can emit without holding a runner ref
        _current_job_id_var.set(job.job_id)
        _progress_emit_var.set(self._sync_progress_emitter)

        span: Any = None
        if self._enable_otel:
            span = self._start_otel_span(job)

        try:
            result = await coro

            import json

            try:
                result_bytes = (
                    json.dumps(result).encode() if result is not None else None
                )
            except (TypeError, ValueError):
                result_bytes = str(result).encode()

            job = job.as_completed(result_bytes)
            await self._store.save(job)
            await self._publish_progress(job.job_id, JobStatus.COMPLETED)

            if span is not None:
                self._finish_otel_span(span, success=True)

            if job.callback_url:
                await self._fire_callback(job, result)

        except asyncio.CancelledError:
            job = job.as_cancelled()
            await self._store.save(job)
            await self._publish_progress(job.job_id, JobStatus.CANCELLED)
            if span is not None:
                self._finish_otel_span(span, success=False, cancelled=True)
            raise

        except Exception as exc:
            error_msg = str(exc)
            job = job.as_failed(error_msg)
            await self._store.save(job)
            await self._publish_progress(job.job_id, JobStatus.FAILED, error=error_msg)
            if span is not None:
                self._finish_otel_span(span, success=False, exc=exc)
            logger.exception("JobRunner: recovered job %s failed", job.job_id)

    async def cancel(self, job_id: UUID) -> bool:
        """
        Cancel an in-flight job.

        Args:
            job_id: UUID of the job to cancel.

        Returns:
            ``True`` if the task was found and cancelled.
            ``False`` if no in-flight task exists for this ``job_id``.

        Edge cases:
            - Task already completed → not in _tasks → returns ``False``
            - Task ignores CancelledError → not forcibly killed
        """
        task = self._tasks.get(job_id)
        if task is None:
            return False
        task.cancel()
        return True

    async def _run_job(
        self,
        job_id: UUID,
        coro: Coroutine[Any, Any, Any],
    ) -> None:
        """
        Execute the job coroutine and manage all state transitions.

        This is the core execution loop:
        1. Fetch job from store
        2. Transition to RUNNING, set ContextVars
        3. Execute coroutine
        4. Transition to COMPLETED or FAILED
        5. Fire callback if configured

        Args:
            job_id: UUID of the job to run.
            coro:   The coroutine to execute.

        Thread safety:  ✅ Runs in single event loop — no cross-task shared state.
        Async safety:   ✅ All I/O is awaited.

        Edge cases:
            - Job missing from store → logs error, coroutine is closed without execution
            - CancelledError → job transitions to CANCELLED
            - Any other exception → job transitions to FAILED
        """
        job = await self._store.get(job_id)
        if job is None:
            logger.error("JobRunner: job %s not found in store", job_id)
            coro.close()
            return

        # ── Transition to RUNNING ─────────────────────────────────────────────
        job = job.as_running()
        await self._store.save(job)
        await self._publish_progress(job_id, JobStatus.RUNNING)

        # Set ContextVar so job_progress() can emit without holding a runner ref
        _current_job_id_var.set(job_id)
        _progress_emit_var.set(self._sync_progress_emitter)

        span: Any = None
        if self._enable_otel:
            span = self._start_otel_span(job)

        result: Any = None
        try:
            result = await coro

            # ── Transition to COMPLETED ───────────────────────────────────────
            # Serialize result as bytes — store is backend-agnostic
            import json

            try:
                result_bytes = (
                    json.dumps(result).encode() if result is not None else None
                )
            except (TypeError, ValueError):
                result_bytes = str(result).encode()

            job = job.as_completed(result_bytes)
            await self._store.save(job)
            await self._publish_progress(job_id, JobStatus.COMPLETED)

            if span is not None:
                self._finish_otel_span(span, success=True)

            # Fire webhook callback if configured
            if job.callback_url:
                await self._fire_callback(job, result)

        except asyncio.CancelledError:
            # ── Transition to CANCELLED ───────────────────────────────────────
            job = job.as_cancelled()
            await self._store.save(job)
            await self._publish_progress(job_id, JobStatus.CANCELLED)
            if span is not None:
                self._finish_otel_span(span, success=False, cancelled=True)
            # Re-raise so asyncio marks the task as cancelled
            raise

        except Exception as exc:
            # ── Transition to FAILED ──────────────────────────────────────────
            error_msg = str(exc)
            job = job.as_failed(error_msg)
            await self._store.save(job)
            await self._publish_progress(job_id, JobStatus.FAILED, error=error_msg)
            if span is not None:
                self._finish_otel_span(span, success=False, exc=exc)

            logger.exception("JobRunner: job %s failed", job_id)
            # Don't re-raise — the task is done; error is persisted in the store

    def _sync_progress_emitter(
        self,
        job_id: UUID,
        progress: float,
        message: str | None,
    ) -> None:
        """
        Synchronous progress emitter called from ``job_progress()``.

        Schedules an async progress publish without awaiting it (fire-and-forget).
        This is necessary because ``job_progress()`` is synchronous but publishing
        to the event bus is async.

        Args:
            job_id:   UUID of the reporting job.
            progress: Progress fraction 0.0–1.0.
            message:  Optional status message.
        """
        # schedule_coroutine for the running event loop
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running event loop — silently skip
            return
        loop.create_task(
            self._publish_progress(
                job_id, JobStatus.RUNNING, progress=progress, message=message
            ),
            name=f"progress-{job_id}",
        )

    async def _publish_progress(
        self,
        job_id: UUID,
        status: JobStatus,
        *,
        progress: float | None = None,
        message: str | None = None,
        error: str | None = None,
    ) -> None:
        """
        Publish a ``JobProgressEvent`` to the event bus (no-op if no bus).

        Errors during publish are caught and logged — progress events must
        never crash the job execution itself.

        Args:
            job_id:   UUID of the job.
            status:   New status after transition.
            progress: Optional progress fraction.
            message:  Optional status message.
            error:    Error message for FAILED transitions.
        """
        if self._event_bus is None:
            return

        event = JobProgressEvent(
            job_id=job_id,
            status=status,
            progress=progress,
            message=message,
            error=error,
        )
        try:
            await self._event_bus.publish(event)
        except Exception:
            # Progress event publish must never crash the job
            logger.exception(
                "JobRunner: failed to publish progress event for job %s (status=%s)",
                job_id,
                status,
            )

    async def _fire_callback(self, job: Job, result: Any) -> None:
        """
        POST the job result to the callback URL with auth forwarding.

        Forwards ``job.request_token`` as ``Authorization: Bearer`` so the
        receiving service can verify the caller's identity.

        Never raises — callback failures are logged and swallowed.

        Args:
            job:    The completed ``Job`` with ``callback_url`` set.
            result: The raw result value to include in the callback body.
        """
        if not job.callback_url:
            return

        try:
            import httpx
            from varco_fastapi.job.response import JobStatusResponse

            headers: dict[str, str] = {"Content-Type": "application/json"}
            if job.request_token:
                # Forward original request token so callback can authenticate
                headers["Authorization"] = f"Bearer {job.request_token}"

            payload = JobStatusResponse(
                job_id=job.job_id,
                status=job.status,
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=job.completed_at,
                result=result,
                error=job.error,
            )

            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    job.callback_url,
                    content=payload.model_dump_json(),
                    headers=headers,
                )
                if response.is_error:
                    logger.warning(
                        "JobRunner: callback to %s returned %s for job %s",
                        job.callback_url,
                        response.status_code,
                        job.job_id,
                    )
        except Exception:
            # Callback failure must never propagate — same contract as DLQ.push()
            logger.exception(
                "JobRunner: callback to %s failed for job %s",
                job.callback_url,
                job.job_id,
            )

    def _start_otel_span(self, job: Job) -> Any:
        """
        Start an OpenTelemetry span for a job execution.

        Args:
            job: The ``Job`` being started.

        Returns:
            The started span, or ``None`` if OTel is unavailable.
        """
        try:
            from opentelemetry import trace

            tracer = trace.get_tracer(self._tracer_name)
            span = tracer.start_span(f"job.run {job.job_id}")
            span.set_attribute("job.id", str(job.job_id))
            span.set_attribute("job.status", job.status.value)
            span.set_attribute("job.created_at", job.created_at.isoformat())
            if job.callback_url:
                span.set_attribute("job.callback_url", job.callback_url)
            return span
        except Exception:
            return None

    def _finish_otel_span(
        self,
        span: Any,
        *,
        success: bool,
        cancelled: bool = False,
        exc: Exception | None = None,
    ) -> None:
        """
        Finish an OTel span, setting status and recording exceptions.

        Args:
            span:      The span to finish.
            success:   Whether the job completed successfully.
            cancelled: Whether the job was cancelled.
            exc:       Exception if the job failed.
        """
        try:
            from opentelemetry.trace import Status, StatusCode

            if success:
                span.set_status(Status(StatusCode.OK))
            elif cancelled:
                span.set_status(Status(StatusCode.UNSET))
            else:
                span.set_status(Status(StatusCode.ERROR))
                if exc is not None:
                    span.record_exception(exc)
            span.end()
        except Exception:
            pass


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "JobRunner",
    "job_progress",
]
