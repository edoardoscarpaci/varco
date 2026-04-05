"""
Milestone B tests — job layer.

Tests cover:
- InMemoryJobStore — save, get, list_by_status, delete
- JobRunner — submit, state transitions, cancel, stop
- JobPoller — stale job recovery
- JobProgressEvent / JobStatusResponse / JobAcceptedResponse — model validation
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4


from varco_core.job.base import Job, JobStatus
from varco_fastapi.job.poller import JobPoller
from varco_fastapi.job.response import (
    JobAcceptedResponse,
    JobProgressEvent,
    JobStatusResponse,
)
from varco_fastapi.job.runner import JobRunner, job_progress
from varco_fastapi.job.store import InMemoryJobStore


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_job(**kwargs) -> Job:
    """Create a test Job with sensible defaults."""
    return Job(
        job_id=kwargs.pop("job_id", uuid4()),
        status=kwargs.pop("status", JobStatus.PENDING),
        **kwargs,
    )


# ── InMemoryJobStore ──────────────────────────────────────────────────────────


async def test_store_save_and_get():
    """save() + get() round-trip preserves the Job."""
    store = InMemoryJobStore()
    job = _make_job()
    await store.save(job)
    retrieved = await store.get(job.job_id)
    assert retrieved == job


async def test_store_get_missing_returns_none():
    """get() returns None for unknown UUID."""
    store = InMemoryJobStore()
    result = await store.get(uuid4())
    assert result is None


async def test_store_save_overwrites():
    """Second save() with same job_id replaces the first."""
    store = InMemoryJobStore()
    job = _make_job()
    await store.save(job)
    updated = job.as_running()
    await store.save(updated)
    retrieved = await store.get(job.job_id)
    assert retrieved.status == JobStatus.RUNNING


async def test_store_delete_removes_job():
    """delete() removes a job; subsequent get() returns None."""
    store = InMemoryJobStore()
    job = _make_job()
    await store.save(job)
    await store.delete(job.job_id)
    assert await store.get(job.job_id) is None


async def test_store_delete_missing_is_noop():
    """delete() on unknown UUID does not raise."""
    store = InMemoryJobStore()
    await store.delete(uuid4())  # Should not raise


async def test_store_list_by_status_filters():
    """list_by_status() returns only jobs with the requested status."""
    store = InMemoryJobStore()
    pending = _make_job(status=JobStatus.PENDING)
    running = _make_job(status=JobStatus.RUNNING)
    await store.save(pending)
    await store.save(running)

    pending_list = await store.list_by_status(JobStatus.PENDING)
    assert len(pending_list) == 1
    assert pending_list[0].job_id == pending.job_id


async def test_store_list_by_status_limit():
    """list_by_status() respects the limit parameter."""
    store = InMemoryJobStore()
    for _ in range(5):
        await store.save(_make_job(status=JobStatus.PENDING))

    result = await store.list_by_status(JobStatus.PENDING, limit=3)
    assert len(result) == 3


async def test_store_list_by_status_empty():
    """list_by_status() returns empty list when no jobs match."""
    store = InMemoryJobStore()
    result = await store.list_by_status(JobStatus.RUNNING)
    assert result == []


# ── JobRunner ──────────────────────────────────────────────────────────────────


async def test_runner_submit_transitions_to_completed():
    """submit() runs coro and transitions job to COMPLETED."""
    store = InMemoryJobStore()
    runner = JobRunner(store=store)
    await runner.start()

    job_id = uuid4()
    job = _make_job(job_id=job_id)
    await store.save(job)

    result_holder: list[str] = []

    async def _coro() -> str:
        result_holder.append("ran")
        return "done"

    await runner.submit(job_id, _coro())
    # Wait for the task to complete
    await asyncio.sleep(0.05)

    completed = await store.get(job_id)
    assert completed is not None
    assert completed.status == JobStatus.COMPLETED
    assert result_holder == ["ran"]
    await runner.stop()


async def test_runner_submit_transitions_to_failed_on_exception():
    """submit() transitions job to FAILED when coro raises."""
    store = InMemoryJobStore()
    runner = JobRunner(store=store)
    await runner.start()

    job_id = uuid4()
    await store.save(_make_job(job_id=job_id))

    async def _failing_coro() -> None:
        raise ValueError("boom")

    await runner.submit(job_id, _failing_coro())
    await asyncio.sleep(0.05)

    failed = await store.get(job_id)
    assert failed is not None
    assert failed.status == JobStatus.FAILED
    assert "boom" in (failed.error or "")
    await runner.stop()


async def test_runner_cancel_returns_true_for_existing_task():
    """cancel() returns True and cancels an in-flight task."""
    store = InMemoryJobStore()
    runner = JobRunner(store=store)
    await runner.start()

    job_id = uuid4()
    await store.save(_make_job(job_id=job_id))

    async def _long_running() -> None:
        await asyncio.sleep(10)

    await runner.submit(job_id, _long_running())
    # Small delay to let the task start
    await asyncio.sleep(0.01)
    cancelled = await runner.cancel(job_id)
    assert cancelled is True
    await runner.stop()


async def test_runner_cancel_unknown_returns_false():
    """cancel() returns False for an unknown job_id."""
    store = InMemoryJobStore()
    runner = JobRunner(store=store)
    await runner.start()
    result = await runner.cancel(uuid4())
    assert result is False
    await runner.stop()


async def test_runner_stop_clears_tasks():
    """stop() cancels all in-flight tasks."""
    store = InMemoryJobStore()
    runner = JobRunner(store=store)
    await runner.start()

    job_id = uuid4()
    await store.save(_make_job(job_id=job_id))

    async def _long() -> None:
        await asyncio.sleep(100)

    await runner.submit(job_id, _long())
    await asyncio.sleep(0.01)
    assert len(runner._tasks) == 1

    await runner.stop(timeout=1.0)
    assert len(runner._tasks) == 0


async def test_runner_max_concurrent_limits_tasks():
    """max_concurrent creates a semaphore that limits concurrency."""
    store = InMemoryJobStore()
    runner = JobRunner(store=store, max_concurrent=2)
    await runner.start()
    # Semaphore is lazy — check it's None before first access
    assert runner._semaphore is None
    # After accessing the semaphore it should be created
    sem = runner._get_semaphore()
    assert sem is not None
    assert sem._value == 2
    await runner.stop()


# ── job_progress ──────────────────────────────────────────────────────────────


def test_job_progress_noop_outside_context():
    """job_progress() outside a job context is a no-op — no exception."""
    # _progress_emit_var is None by default
    job_progress(0.5, "test")  # Should not raise


# ── JobPoller ─────────────────────────────────────────────────────────────────


async def test_poller_marks_stale_running_jobs_failed():
    """JobPoller marks RUNNING jobs older than stale_threshold as FAILED."""
    store = InMemoryJobStore()

    # Create a RUNNING job with started_at far in the past
    stale_job_id = uuid4()
    stale_job = Job(
        job_id=stale_job_id,
        status=JobStatus.RUNNING,
        started_at=datetime.now(UTC) - timedelta(hours=1),
    )
    await store.save(stale_job)

    poller = JobPoller(
        store=store,
        stale_threshold=timedelta(minutes=5),
        poll_interval=999.0,  # Don't auto-poll — we'll call manually
    )
    # Call the internal recovery method directly
    await poller._recover_stale_jobs()

    recovered = await store.get(stale_job_id)
    assert recovered is not None
    assert recovered.status == JobStatus.FAILED
    assert "stale_job_timeout" in (recovered.error or "")


async def test_poller_ignores_recently_started_jobs():
    """JobPoller leaves recently started RUNNING jobs alone."""
    store = InMemoryJobStore()

    recent_job_id = uuid4()
    recent_job = Job(
        job_id=recent_job_id,
        status=JobStatus.RUNNING,
        # Started 10 seconds ago — well within threshold
        started_at=datetime.now(UTC) - timedelta(seconds=10),
    )
    await store.save(recent_job)

    poller = JobPoller(store=store, stale_threshold=timedelta(minutes=5))
    await poller._recover_stale_jobs()

    still_running = await store.get(recent_job_id)
    assert still_running is not None
    assert still_running.status == JobStatus.RUNNING


async def test_poller_start_stop_lifecycle():
    """JobPoller start/stop creates and cancels a background task."""
    store = InMemoryJobStore()
    poller = JobPoller(store=store, poll_interval=999.0)
    await poller.start()
    assert poller._task is not None
    assert not poller._task.done()
    await poller.stop()
    assert poller._task is None


async def test_poller_double_start_is_idempotent():
    """Calling start() twice does not create a second task."""
    store = InMemoryJobStore()
    poller = JobPoller(store=store, poll_interval=999.0)
    await poller.start()
    task_1 = poller._task
    await poller.start()  # Second call — same task
    assert poller._task is task_1
    await poller.stop()


# ── Response models ───────────────────────────────────────────────────────────


def test_job_accepted_response_defaults():
    """JobAcceptedResponse defaults to PENDING status."""
    resp = JobAcceptedResponse(job_id=uuid4())
    assert resp.status == "pending"
    assert resp.events_url is None


def test_job_status_response_all_fields():
    """JobStatusResponse accepts all optional fields."""
    job_id = uuid4()
    now = datetime.now(UTC)
    resp = JobStatusResponse(
        job_id=job_id,
        status=JobStatus.COMPLETED,
        created_at=now,
        started_at=now,
        completed_at=now,
        progress=1.0,
        result={"key": "value"},
        error=None,
    )
    assert resp.job_id == job_id
    assert resp.status == "completed"
    assert resp.progress == 1.0


def test_job_progress_event_validation():
    """JobProgressEvent validates progress range via Pydantic."""
    event = JobProgressEvent(job_id=uuid4(), status=JobStatus.RUNNING, progress=0.5)
    assert event.progress == 0.5


def test_job_progress_event_null_progress():
    """JobProgressEvent allows null progress."""
    event = JobProgressEvent(job_id=uuid4(), status=JobStatus.RUNNING)
    assert event.progress is None


# ── HealthRouter ──────────────────────────────────────────────────────────────


async def test_health_router_no_checks_returns_healthy():
    """HealthRouter with no checks returns HEALTHY."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from varco_fastapi.router.health import HealthRouter

    health = HealthRouter(checks=[], prefix="/health")
    app = FastAPI()
    app.include_router(health.build_router())
    client = TestClient(app)

    resp = client.get("/health/")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"


async def test_health_router_live_always_200():
    """GET /health/live always returns 200."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from varco_fastapi.router.health import HealthRouter

    health = HealthRouter(prefix="/health")
    app = FastAPI()
    app.include_router(health.build_router())
    client = TestClient(app)

    resp = client.get("/health/live")
    assert resp.status_code == 200
    assert resp.json()["status"] == "healthy"


async def test_health_router_ready_503_on_unhealthy():
    """GET /health/ready returns 503 when a check is UNHEALTHY."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from varco_core.health import HealthCheck, HealthResult, HealthStatus
    from varco_fastapi.router.health import HealthRouter

    class FailingCheck(HealthCheck):
        @property
        def name(self) -> str:
            return "failing"

        async def check(self) -> HealthResult:
            return HealthResult(status=HealthStatus.UNHEALTHY, component="failing")

    health = HealthRouter(checks=[FailingCheck()], prefix="/health")
    app = FastAPI()
    app.include_router(health.build_router())
    client = TestClient(app)

    resp = client.get("/health/ready")
    assert resp.status_code == 503
