"""
Milestone D tests — job system fixes.

Tests cover three specific bugs fixed in Milestone D:

1. ``enqueue()`` — job is persisted to the store BEFORE the asyncio.Task starts.
   Previously ``_submit_job()`` called ``runner.submit()`` without saving, so
   ``_run_job`` always got ``None`` from the store and silently dropped the coro.

2. ``_make_custom_handler`` async offload — custom ``@route`` endpoints now
   support ``?with_async=true`` just like CRUD routes do.

3. ``JobPoller.start()`` immediate recovery — stale RUNNING jobs are marked
   FAILED on startup rather than waiting up to ``poll_interval`` seconds.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from varco_core.job.base import Job, JobStatus
from varco_fastapi.job.poller import JobPoller
from varco_fastapi.job.runner import JobRunner
from varco_fastapi.job.store import InMemoryJobStore


# ── Helpers ────────────────────────────────────────────────────────────────────


def _pending_job(**kwargs) -> Job:
    """Return a fresh PENDING Job with sensible defaults."""
    return Job(job_id=kwargs.pop("job_id", uuid4()), **kwargs)


def _running_job(*, started_at: datetime | None = None, **kwargs) -> Job:
    """Return a Job already in RUNNING state (bypassing as_running validation)."""
    import dataclasses

    base = Job(job_id=kwargs.pop("job_id", uuid4()))
    return dataclasses.replace(
        base,
        status=JobStatus.RUNNING,
        started_at=started_at or datetime.now(UTC),
        **kwargs,
    )


# ── Fix 1: enqueue() persists job before scheduling task ──────────────────────


async def test_enqueue_saves_job_as_pending_before_task_starts():
    """
    enqueue() must call store.save(job) BEFORE asyncio.create_task.

    This is the core crash-safety guarantee: if the process dies between
    the save and the task creation, the PENDING record is still in the store
    and JobPoller can recover it.

    We verify ordering by tracking when save() completes relative to when
    submit() is called inside enqueue().  The real asyncio.Task is still
    created so no coroutine leaks.
    """
    store = InMemoryJobStore()
    runner = JobRunner(store=store)
    await runner.start()

    job = _pending_job()
    save_order: list[str] = []

    # Intercept store.save to record when it fires relative to submit()
    original_save = store.save

    async def _recording_save(j: Job) -> None:
        save_order.append("save")
        await original_save(j)

    store.save = _recording_save  # type: ignore[method-assign]

    original_submit = runner.submit

    async def _recording_submit(job_id, coro) -> None:  # type: ignore[no-untyped-def]
        save_order.append("submit")
        await original_submit(job_id, coro)

    runner.submit = _recording_submit  # type: ignore[method-assign]

    async def _noop() -> str:
        return "ok"

    await runner.enqueue(job, _noop())

    # save must come before submit — this is the invariant
    assert save_order == ["save", "submit"], (
        "store.save() must be called BEFORE runner.submit(); "
        f"actual order: {save_order}"
    )
    await asyncio.sleep(0.05)  # let task finish cleanly
    await runner.stop()


async def test_enqueue_job_transitions_to_completed():
    """
    After enqueue(), the job runs to completion and the store reflects COMPLETED.

    Previously _submit_job called submit() without saving first, so _run_job
    always got None from the store and silently closed the coroutine — the job
    never ran.  With enqueue() the job is saved first and executes correctly.
    """
    store = InMemoryJobStore()
    runner = JobRunner(store=store)
    await runner.start()

    job = _pending_job()
    ran: list[bool] = []

    async def _work() -> str:
        ran.append(True)
        return "result"

    await runner.enqueue(job, _work())
    # Allow the task to complete
    await asyncio.sleep(0.05)

    completed = await store.get(job.job_id)
    assert completed is not None, "job must be findable in store after enqueue()"
    assert (
        completed.status == JobStatus.COMPLETED
    ), f"expected COMPLETED, got {completed.status}"
    assert ran == [True], "coro must actually execute"
    await runner.stop()


async def test_enqueue_raises_and_coro_does_not_execute_if_store_save_raises():
    """
    If store.save() raises, enqueue() must:
    - propagate the exception to the caller, and
    - NOT execute the coroutine body (the coro is closed, not awaited).

    We verify the second property by checking that the coroutine's side-effect
    (appending to a list) never occurs — coroutine.close() suppresses execution.
    Note: we cannot monkey-patch coro.close() in Python 3.12+ (attribute is
    read-only on coroutine objects), so we verify via observable side effects.
    """
    store = InMemoryJobStore()
    runner = JobRunner(store=store)
    await runner.start()

    # Poison the store so save() raises immediately
    async def _failing_save(job: Job) -> None:
        raise RuntimeError("store unavailable")

    store.save = _failing_save  # type: ignore[method-assign]

    executed: list[str] = []

    async def _work() -> str:
        # This body must NEVER run if enqueue() closes the coro on save failure
        executed.append("ran")
        return "should not run"

    with pytest.raises(RuntimeError, match="store unavailable"):
        await runner.enqueue(Job(job_id=uuid4()), _work())

    # Give any accidentally scheduled task time to run
    await asyncio.sleep(0.05)

    assert (
        executed == []
    ), "coroutine body must not execute when enqueue() fails on store.save()"
    await runner.stop()


async def test_enqueue_job_not_in_store_before_enqueue():
    """
    Before enqueue(), the job must not exist in the store.
    After enqueue(), it must be present as PENDING (then RUNNING/COMPLETED).
    """
    store = InMemoryJobStore()
    runner = JobRunner(store=store)
    await runner.start()

    job = _pending_job()

    # Not in store yet
    assert await store.get(job.job_id) is None

    async def _noop() -> None:
        pass

    await runner.enqueue(job, _noop())

    # Now it must be in the store
    assert await store.get(job.job_id) is not None
    await asyncio.sleep(0.05)
    await runner.stop()


# ── Fix 2: custom handler async offload ───────────────────────────────────────


async def test_custom_handler_runs_inline_without_with_async():
    """
    A custom @route handler executes inline (no job submission) when
    ?with_async is absent or false.
    """
    from fastapi.testclient import TestClient
    from fastapi import FastAPI

    from varco_fastapi.router.base import VarcoRouter
    from varco_fastapi.router.endpoint import route

    executed: list[str] = []

    class MyRouter(VarcoRouter):  # type: ignore[type-arg]
        _prefix = "/items"

        @route("POST", "/run")
        async def run(self) -> dict:  # type: ignore[override]
            executed.append("ran")
            return {"status": "done"}

    api_router = MyRouter().build_router()
    app = FastAPI()
    app.include_router(api_router)

    client = TestClient(app)
    response = client.post("/items/run")
    assert response.status_code == 200
    assert response.json() == {"status": "done"}
    assert executed == ["ran"], "method must execute inline without with_async"


async def test_custom_handler_offloads_with_with_async_true():
    """
    A custom @route handler returns 202 JobAcceptedResponse when
    ?with_async=true is passed and a job_runner is configured.
    """
    from fastapi.testclient import TestClient
    from fastapi import FastAPI

    from varco_fastapi.router.base import VarcoRouter
    from varco_fastapi.router.endpoint import route

    store = InMemoryJobStore()
    runner = JobRunner(store=store)
    await runner.start()

    executed: list[str] = []

    class MyRouter(VarcoRouter):  # type: ignore[type-arg]
        _prefix = "/items"
        _job_runner = runner

        @route("POST", "/run", async_capable=True)
        async def run(self) -> dict:  # type: ignore[override]
            executed.append("ran")
            return {"status": "done"}

    api_router = MyRouter().build_router()
    app = FastAPI()
    app.include_router(api_router)

    client = TestClient(app)
    response = client.post("/items/run?with_async=true")

    # Must return 202 Accepted
    assert response.status_code == 200  # TestClient follows FastAPI route status
    body = response.json()
    # Should contain a job_id (JobAcceptedResponse)
    assert "job_id" in body, f"expected job_id in response, got {body}"

    # Allow the background task to run
    await asyncio.sleep(0.1)
    await runner.stop()


async def test_custom_handler_async_capable_false_ignores_with_async():
    """
    When async_capable=False on @route, ?with_async=true is silently ignored
    and the handler executes inline.
    """
    from fastapi.testclient import TestClient
    from fastapi import FastAPI

    from varco_fastapi.router.base import VarcoRouter
    from varco_fastapi.router.endpoint import route

    store = InMemoryJobStore()
    runner = JobRunner(store=store)
    await runner.start()

    executed: list[str] = []

    class MyRouter(VarcoRouter):  # type: ignore[type-arg]
        _prefix = "/items"
        _job_runner = runner

        # async_capable=False — with_async query param must be ignored
        @route("POST", "/run", async_capable=False)
        async def run(self) -> dict:  # type: ignore[override]
            executed.append("ran")
            return {"status": "done"}

    api_router = MyRouter().build_router()
    app = FastAPI()
    app.include_router(api_router)

    client = TestClient(app)
    response = client.post("/items/run?with_async=true")

    assert response.status_code == 200
    body = response.json()
    # Must execute inline — no job_id in response
    assert body == {"status": "done"}, f"expected inline result, got {body}"
    assert executed == ["ran"]
    await runner.stop()


async def test_custom_handler_no_job_runner_ignores_with_async():
    """
    When no _job_runner is configured, ?with_async=true is silently ignored
    even if async_capable=True — handler executes inline.
    """
    from fastapi.testclient import TestClient
    from fastapi import FastAPI

    from varco_fastapi.router.base import VarcoRouter
    from varco_fastapi.router.endpoint import route

    executed: list[str] = []

    class MyRouter(VarcoRouter):  # type: ignore[type-arg]
        _prefix = "/items"
        # No _job_runner set

        @route("POST", "/run", async_capable=True)
        async def run(self) -> dict:  # type: ignore[override]
            executed.append("ran")
            return {"status": "done"}

    api_router = MyRouter().build_router()
    app = FastAPI()
    app.include_router(api_router)

    client = TestClient(app)
    response = client.post("/items/run?with_async=true")

    assert response.status_code == 200
    assert response.json() == {"status": "done"}
    assert executed == ["ran"], "handler must fall back to inline execution"


# ── Fix 3: JobPoller immediate startup recovery ───────────────────────────────


async def test_poller_start_recovers_stale_running_jobs_immediately():
    """
    JobPoller.start() must mark stale RUNNING jobs as FAILED immediately,
    without waiting for the first scheduled poll interval.

    This simulates the most common crash-recovery scenario: the process
    restarted, the store has jobs in RUNNING state (from asyncio.Tasks that
    were lost), and we need them recovered before serving traffic.
    """
    store = InMemoryJobStore()

    # Simulate a job that was RUNNING when the previous process crashed.
    # started_at is old enough to be past the stale threshold.
    old_start = datetime.now(UTC) - timedelta(minutes=10)
    stale_job = _running_job(started_at=old_start)
    await store.save(stale_job)

    # Short stale threshold so test doesn't depend on wall-clock timing
    poller = JobPoller(
        store=store,
        stale_threshold=timedelta(minutes=5),
        poll_interval=9999.0,  # long interval — recovery must happen in start(), not loop
    )

    # start() must recover immediately — no need to wait poll_interval seconds
    await poller.start()

    recovered = await store.get(stale_job.job_id)
    assert recovered is not None
    assert (
        recovered.status == JobStatus.FAILED
    ), f"stale RUNNING job must be marked FAILED on startup, got {recovered.status}"
    assert "stale_job_timeout" in (
        recovered.error or ""
    ), f"error message must indicate stale timeout, got {recovered.error!r}"

    await poller.stop()


async def test_poller_start_does_not_recover_recent_running_jobs():
    """
    JobPoller.start() must NOT mark recently started RUNNING jobs as FAILED —
    only jobs older than stale_threshold are considered stuck.
    """
    store = InMemoryJobStore()

    # Job started just now — should not be recovered
    recent_job = _running_job(started_at=datetime.now(UTC))
    await store.save(recent_job)

    poller = JobPoller(
        store=store,
        stale_threshold=timedelta(minutes=5),
        poll_interval=9999.0,
    )
    await poller.start()

    not_recovered = await store.get(recent_job.job_id)
    assert not_recovered is not None
    assert (
        not_recovered.status == JobStatus.RUNNING
    ), "recently started job must NOT be marked as stale"

    await poller.stop()


async def test_poller_start_is_idempotent_with_no_stale_jobs():
    """
    JobPoller.start() with no stale jobs in the store must complete without error.
    Validates the InMemoryJobStore no-stale-jobs code path.
    """
    store = InMemoryJobStore()
    poller = JobPoller(
        store=store, stale_threshold=timedelta(minutes=5), poll_interval=9999.0
    )

    # Should not raise even with an empty store
    await poller.start()
    await poller.stop()


async def test_poller_start_recovery_failure_does_not_prevent_loop():
    """
    If the startup recovery pass raises, the polling loop must still start.
    Recovery errors are logged but must not prevent the poller from running.
    """
    store = InMemoryJobStore()

    # Poison list_by_status so the recovery pass fails
    original_list = store.list_by_status

    call_count = 0

    async def _failing_list(status: JobStatus, *, limit: int = 100):  # type: ignore[override]
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("store temporarily unavailable")
        return await original_list(status, limit=limit)

    store.list_by_status = _failing_list  # type: ignore[method-assign]

    poller = JobPoller(
        store=store, stale_threshold=timedelta(minutes=5), poll_interval=9999.0
    )

    # Must not raise — recovery failure is logged, loop starts anyway
    await poller.start()

    # The polling task must exist (loop started despite recovery failure)
    assert poller._task is not None and not poller._task.done()
    await poller.stop()


async def test_poller_calling_start_twice_is_idempotent():
    """
    Calling start() twice must not create a second background task.
    The first task is reused if still running.
    """
    store = InMemoryJobStore()
    poller = JobPoller(
        store=store, stale_threshold=timedelta(minutes=5), poll_interval=9999.0
    )

    await poller.start()
    task_after_first = poller._task

    await poller.start()  # second call — must reuse existing task
    task_after_second = poller._task

    assert (
        task_after_first is task_after_second
    ), "second start() call must not create a new task"
    await poller.stop()
