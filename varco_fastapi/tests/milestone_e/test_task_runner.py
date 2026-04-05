"""
Milestone E — task runner tests.

Tests cover:
- InMemoryJobStore.try_claim() — atomic PENDING→RUNNING claim, double-claim prevention
- JobRunner.enqueue_task() — submits named task, stores TaskPayload in Job
- JobRunner.recover() — re-submits PENDING jobs with task_payload using try_claim()
- Distributed safety — concurrent try_claim() calls on the same job
"""

from __future__ import annotations

import asyncio
from uuid import uuid4


from varco_core.job.base import Job, JobStatus
from varco_core.job.task import TaskPayload, TaskRegistry, VarcoTask
from varco_fastapi.job.runner import JobRunner
from varco_fastapi.job.store import InMemoryJobStore


# ── try_claim ──────────────────────────────────────────────────────────────────


class TestTryClaim:
    async def test_claim_pending_job_succeeds(self):
        """try_claim() on a PENDING job returns the job in RUNNING state."""
        store = InMemoryJobStore()
        job = Job(job_id=uuid4())
        await store.save(job)

        claimed = await store.try_claim(job.job_id)
        assert claimed is not None
        assert claimed.status == JobStatus.RUNNING
        assert claimed.started_at is not None

    async def test_double_claim_returns_none(self):
        """try_claim() on an already-RUNNING job returns None."""
        store = InMemoryJobStore()
        job = Job(job_id=uuid4())
        await store.save(job)

        await store.try_claim(job.job_id)
        second = await store.try_claim(job.job_id)
        assert second is None

    async def test_claim_unknown_job_returns_none(self):
        """try_claim() on an unknown job_id returns None cleanly."""
        store = InMemoryJobStore()
        result = await store.try_claim(uuid4())
        assert result is None

    async def test_claim_completed_job_returns_none(self):
        """try_claim() on a COMPLETED job returns None."""
        store = InMemoryJobStore()
        job = Job(job_id=uuid4())
        await store.save(job)
        # Manually run it through to COMPLETED
        running = job.as_running()
        completed = running.as_completed(b"ok")
        await store.save(completed)

        result = await store.try_claim(job.job_id)
        assert result is None

    async def test_concurrent_claims_only_one_succeeds(self):
        """Concurrent try_claim() calls on the same job — exactly one succeeds."""
        store = InMemoryJobStore()
        job = Job(job_id=uuid4())
        await store.save(job)

        # Fire 10 concurrent claims — only one should succeed
        results = await asyncio.gather(
            *[store.try_claim(job.job_id) for _ in range(10)]
        )
        successes = [r for r in results if r is not None]
        assert len(successes) == 1
        assert successes[0].status == JobStatus.RUNNING

    async def test_claim_updates_store(self):
        """After try_claim(), store.get() returns the job in RUNNING state."""
        store = InMemoryJobStore()
        job = Job(job_id=uuid4())
        await store.save(job)

        await store.try_claim(job.job_id)
        stored = await store.get(job.job_id)
        assert stored is not None
        assert stored.status == JobStatus.RUNNING


# ── enqueue_task ───────────────────────────────────────────────────────────────


class TestEnqueueTask:
    async def test_enqueue_task_returns_uuid(self):
        """enqueue_task() returns a UUID job_id."""
        from uuid import UUID

        store = InMemoryJobStore()
        runner = JobRunner(store=store)
        await runner.start()

        results: list[None] = []

        async def fn() -> None:
            results.append(None)

        task = VarcoTask(name="noop", fn=fn)
        job_id = await runner.enqueue_task(task)
        assert isinstance(job_id, UUID)
        await asyncio.sleep(0.05)  # let background task complete
        await runner.stop()

    async def test_enqueue_task_stores_task_payload(self):
        """Job record in store has task_payload after enqueue_task()."""
        store = InMemoryJobStore()
        runner = JobRunner(store=store)
        await runner.start()

        async def add(x: int, y: int) -> int:
            return x + y

        task = VarcoTask(name="add", fn=add)
        job_id = await runner.enqueue_task(task, 3, 7)
        await asyncio.sleep(0.05)  # let background task complete

        job = await store.get(job_id)
        assert job is not None
        assert job.task_payload is not None
        assert job.task_payload.task_name == "add"
        assert job.task_payload.args == [3, 7]
        await runner.stop()

    async def test_enqueue_task_executes_and_completes(self):
        """The task coroutine is executed and the job reaches COMPLETED."""
        store = InMemoryJobStore()
        runner = JobRunner(store=store)
        await runner.start()

        results: list[int] = []

        async def capture(x: int) -> int:
            results.append(x)
            return x

        task = VarcoTask(name="capture", fn=capture)
        job_id = await runner.enqueue_task(task, 99)

        await asyncio.sleep(0.05)  # let the background task complete

        job = await store.get(job_id)
        assert job is not None
        assert job.status == JobStatus.COMPLETED
        assert results == [99]
        await runner.stop()

    async def test_enqueue_task_with_auth_snapshot(self):
        """auth_snapshot is stored in the Job record."""
        store = InMemoryJobStore()
        runner = JobRunner(store=store)
        await runner.start()

        results: list[None] = []

        async def fn() -> None:
            results.append(None)

        task = VarcoTask(name="fn", fn=fn)
        auth_snap = {"user_id": "usr_1", "roles": ["admin"]}
        job_id = await runner.enqueue_task(task, auth_snapshot=auth_snap)
        await asyncio.sleep(0.05)

        job = await store.get(job_id)
        assert job is not None
        assert job.auth_snapshot == auth_snap
        await runner.stop()


# ── recover ────────────────────────────────────────────────────────────────────


class TestRecover:
    async def test_recover_no_pending_jobs(self):
        """recover() returns 0 when no PENDING jobs with task_payload exist."""
        store = InMemoryJobStore()
        runner = JobRunner(store=store)
        await runner.start()
        registry = TaskRegistry()

        count = await runner.recover(registry)
        assert count == 0
        await runner.stop()

    async def test_recover_resubmits_pending_task(self):
        """recover() re-submits a PENDING job and executes its task."""
        store = InMemoryJobStore()
        results: list[int] = []

        async def fn(x: int) -> int:
            results.append(x)
            return x

        registry = TaskRegistry()
        registry.register(VarcoTask(name="fn", fn=fn))

        # Create a PENDING job with task_payload (simulates restart scenario)
        job = Job(
            job_id=uuid4(),
            task_payload=TaskPayload(task_name="fn", args=[42]),
        )
        await store.save(job)

        runner = JobRunner(store=store)
        await runner.start()

        count = await runner.recover(registry)
        assert count == 1

        await asyncio.sleep(0.05)

        recovered = await store.get(job.job_id)
        assert recovered is not None
        assert recovered.status == JobStatus.COMPLETED
        assert results == [42]

        await runner.stop()

    async def test_recover_skips_jobs_without_task_payload(self):
        """recover() ignores PENDING jobs that have no task_payload."""
        store = InMemoryJobStore()
        # Job without task_payload (submitted via legacy bare-coro path)
        job = Job(job_id=uuid4())  # task_payload=None
        await store.save(job)

        registry = TaskRegistry()
        runner = JobRunner(store=store)
        await runner.start()

        count = await runner.recover(registry)
        assert count == 0
        await runner.stop()

    async def test_recover_skips_unregistered_task(self):
        """recover() logs a warning and skips if the task name is not in the registry."""
        store = InMemoryJobStore()
        job = Job(
            job_id=uuid4(),
            task_payload=TaskPayload(task_name="missing_task"),
        )
        await store.save(job)

        registry = TaskRegistry()  # empty registry — task not registered
        runner = JobRunner(store=store)
        await runner.start()

        count = await runner.recover(registry)
        assert count == 0

        # Job stays in RUNNING state (claimed but not executed)
        stored = await store.get(job.job_id)
        assert stored is not None
        assert (
            stored.status == JobStatus.RUNNING
        )  # claimed via try_claim but not executed

        await runner.stop()

    async def test_recover_prevents_double_execution(self):
        """Two runners recovering the same store only execute each job once."""
        store = InMemoryJobStore()
        counter: list[int] = []

        async def increment() -> None:
            counter.append(1)

        registry = TaskRegistry()
        registry.register(VarcoTask(name="increment", fn=increment))

        job = Job(
            job_id=uuid4(),
            task_payload=TaskPayload(task_name="increment"),
        )
        await store.save(job)

        runner1 = JobRunner(store=store)
        runner2 = JobRunner(store=store)
        await runner1.start()
        await runner2.start()

        # Both runners race to recover the same job
        counts = await asyncio.gather(
            runner1.recover(registry),
            runner2.recover(registry),
        )

        # Total recovered must be exactly 1 — try_claim prevents double-execution
        assert sum(counts) == 1

        await asyncio.sleep(0.05)
        assert len(counter) == 1  # executed exactly once

        await runner1.stop()
        await runner2.stop()
