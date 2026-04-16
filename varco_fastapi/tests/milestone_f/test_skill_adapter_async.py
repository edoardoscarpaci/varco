"""
milestone_f / test_skill_adapter_async.py
==========================================
Tests for ``SkillAdapter`` async task support — P0-1 feature.

Covers:
- ``handle_task()`` in async mode (job_runner wired) returns ``working`` state
- ``handle_task()`` in sync mode (no job_runner) returns ``completed``/``failed``
- ``GET /tasks/{task_id}`` returns 404 without a store
- ``GET /tasks/{task_id}`` maps JobStatus → A2A states correctly
- ``agent_card()`` advertises async capabilities when runner is present
- Unknown skill still returns ``failed`` in async mode
- Store ``save()`` failure falls back to immediate ``failed`` response
- ``_job_to_task_response`` maps all JobStatus variants correctly
- ``bind_skill_adapter()`` resolves runner/store from container

All tests are pure unit tests — no HTTP server, no real broker.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from varco_core.job.base import AbstractJobRunner, AbstractJobStore, Job, JobStatus
from varco_fastapi.router.base import VarcoRouter
from varco_fastapi.router.mixins import CreateMixin, ReadMixin
from varco_fastapi.router.skill import (
    SkillAdapter,
    _job_to_task_response,
    _working_response,
)


# ── Stub models ───────────────────────────────────────────────────────────────


class ItemCreate(BaseModel):
    name: str


class ItemRead(BaseModel):
    id: UUID
    name: str


# ── Fixture router ────────────────────────────────────────────────────────────


class ItemRouter(CreateMixin, ReadMixin, VarcoRouter):
    _prefix = "/items"
    _create_skill = True
    _read_skill = True


# ── Stub runner / store ────────────────────────────────────────────────────────


class StubJobStore(AbstractJobStore):
    """In-memory store stub for tests — no asyncio.Lock needed (single-threaded tests)."""

    def __init__(self) -> None:
        self._jobs: dict[UUID, Job] = {}

    async def save(self, job: Job) -> None:
        self._jobs[job.job_id] = job

    async def get(self, job_id: UUID) -> Job | None:
        return self._jobs.get(job_id)

    async def list_by_status(self, status: JobStatus, *, limit: int = 100) -> list[Job]:
        return [j for j in self._jobs.values() if j.status == status][:limit]

    async def delete(self, job_id: UUID) -> None:
        self._jobs.pop(job_id, None)

    async def try_claim(self, job_id: UUID) -> Job | None:
        job = self._jobs.get(job_id)
        if job is None or job.status != JobStatus.PENDING:
            return None
        import dataclasses

        claimed = dataclasses.replace(job, status=JobStatus.RUNNING)
        self._jobs[job_id] = claimed
        return claimed


class StubJobRunner(AbstractJobRunner):
    """Runner stub that records enqueued jobs without executing the coroutine."""

    def __init__(self, store: StubJobStore) -> None:
        self._store = store
        self.enqueued: list[tuple[Job, object]] = []

    async def enqueue(self, job: Job, coro: object) -> None:
        await self._store.save(job)
        self.enqueued.append((job, coro))
        # Close the coroutine to avoid ResourceWarning
        if hasattr(coro, "close"):
            coro.close()  # type: ignore[union-attr]

    async def submit(self, job_id: UUID, coro: object) -> None:
        pass

    async def enqueue_task(self, task: object, *args: object, **kwargs: object) -> UUID:
        return uuid4()

    async def recover(self, registry: object) -> int:
        return 0

    async def cancel(self, job_id: UUID) -> bool:
        return False

    async def start(self) -> None:
        pass

    async def stop(self, *, timeout: float = 30.0) -> None:
        pass


# ── Helper: build a minimal SkillAdapter with a mock client ───────────────────


def _make_adapter(
    *,
    runner: StubJobRunner | None = None,
    store: StubJobStore | None = None,
) -> tuple[SkillAdapter, MagicMock]:
    """Return (adapter, mock_client)."""
    mock_client = MagicMock()
    mock_client.create = AsyncMock(return_value={"id": str(uuid4()), "name": "x"})
    mock_client.read = AsyncMock(return_value={"id": str(uuid4()), "name": "x"})
    return (
        SkillAdapter(
            ItemRouter,
            agent_name="ItemAgent",
            agent_description="Manages items",
            client=mock_client,
            job_runner=runner,
            job_store=store,
        ),
        mock_client,
    )


# ── handle_task() — sync mode ─────────────────────────────────────────────────


async def test_sync_handle_task_returns_completed():
    adapter, mock_client = _make_adapter()
    response = await adapter.handle_task(
        {
            "id": str(uuid4()),
            "skill_id": "create_item",
            "message": {"parts": [{"type": "data", "data": {"name": "foo"}}]},
        }
    )
    assert response["status"]["state"] == "completed"
    assert response["artifacts"]


async def test_sync_handle_task_unknown_skill_returns_failed():
    adapter, _ = _make_adapter()
    response = await adapter.handle_task({"skill_id": "does_not_exist"})
    assert response["status"]["state"] == "failed"
    assert "does_not_exist" in response["status"]["message"]


async def test_sync_handle_task_dispatch_error_returns_failed():
    adapter, mock_client = _make_adapter()
    mock_client.create = AsyncMock(side_effect=RuntimeError("boom"))
    response = await adapter.handle_task(
        {
            "skill_id": "create_item",
            "message": {"parts": [{"data": {"name": "x"}}]},
        }
    )
    assert response["status"]["state"] == "failed"
    assert "boom" in response["status"]["message"]


# ── handle_task() — async mode ────────────────────────────────────────────────


async def test_async_handle_task_returns_working():
    store = StubJobStore()
    runner = StubJobRunner(store)
    adapter, _ = _make_adapter(runner=runner, store=store)

    task_id = str(uuid4())
    response = await adapter.handle_task(
        {
            "id": task_id,
            "skill_id": "create_item",
            "message": {"parts": [{"data": {"name": "item1"}}]},
        }
    )

    # Must return "working" immediately — not "completed"
    assert response["status"]["state"] == "working"
    assert response["id"] == task_id
    # Runner should have one job enqueued
    assert len(runner.enqueued) == 1
    job, _ = runner.enqueued[0]
    assert str(job.job_id) == task_id


async def test_async_handle_task_unknown_skill_returns_failed():
    """Unknown skill short-circuits before submitting to runner."""
    store = StubJobStore()
    runner = StubJobRunner(store)
    adapter, _ = _make_adapter(runner=runner, store=store)

    response = await adapter.handle_task({"skill_id": "ghost_skill"})
    assert response["status"]["state"] == "failed"
    assert len(runner.enqueued) == 0


async def test_async_handle_task_store_save_failure_returns_failed():
    """When the store raises during enqueue, return a failed response."""
    store = StubJobStore()
    store.save = AsyncMock(side_effect=RuntimeError("disk full"))
    runner = StubJobRunner(store)
    adapter, _ = _make_adapter(runner=runner, store=store)

    response = await adapter.handle_task(
        {
            "id": str(uuid4()),
            "skill_id": "create_item",
            "message": {"parts": [{"data": {"name": "x"}}]},
        }
    )
    assert response["status"]["state"] == "failed"
    assert "disk full" in response["status"]["message"]


async def test_async_handle_task_invalid_task_id_uses_new_uuid():
    """Non-UUID task_id should not crash — a new UUID is generated for the job."""
    store = StubJobStore()
    runner = StubJobRunner(store)
    adapter, _ = _make_adapter(runner=runner, store=store)

    response = await adapter.handle_task(
        {
            "id": "not-a-uuid",
            "skill_id": "create_item",
            "message": {"parts": [{"data": {"name": "x"}}]},
        }
    )
    # task_id in response is unchanged (the original string)
    assert response["id"] == "not-a-uuid"
    assert response["status"]["state"] == "working"
    # A new UUID was generated for the internal job
    assert len(runner.enqueued) == 1


# ── GET /tasks/{task_id} — mounting ───────────────────────────────────────────


def _mount_and_client(adapter: SkillAdapter) -> TestClient:
    app = FastAPI()
    adapter.mount(app)
    return TestClient(app, raise_server_exceptions=False)


def test_polling_without_store_returns_404():
    adapter, _ = _make_adapter()  # no store
    client = _mount_and_client(adapter)
    resp = client.get(f"/tasks/{uuid4()}")
    assert resp.status_code == 404


def test_polling_invalid_task_id_returns_400():
    store = StubJobStore()
    runner = StubJobRunner(store)
    adapter, _ = _make_adapter(runner=runner, store=store)
    client = _mount_and_client(adapter)
    resp = client.get("/tasks/not-a-uuid")
    assert resp.status_code == 400


def test_polling_unknown_job_returns_404():
    store = StubJobStore()
    runner = StubJobRunner(store)
    adapter, _ = _make_adapter(runner=runner, store=store)
    client = _mount_and_client(adapter)
    resp = client.get(f"/tasks/{uuid4()}")
    assert resp.status_code == 404


async def test_polling_pending_job_returns_working():
    store = StubJobStore()
    runner = StubJobRunner(store)
    adapter, _ = _make_adapter(runner=runner, store=store)
    client = _mount_and_client(adapter)

    task_id = str(uuid4())
    # Enqueue, which saves a PENDING job in the store
    await adapter.handle_task(
        {
            "id": task_id,
            "skill_id": "create_item",
            "message": {"parts": [{"data": {"name": "x"}}]},
        }
    )

    resp = client.get(f"/tasks/{task_id}")
    assert resp.status_code == 200
    assert resp.json()["status"]["state"] == "working"


# ── _job_to_task_response ─────────────────────────────────────────────────────


def _make_job(
    status: JobStatus, result: bytes | None = None, error: str | None = None
) -> Job:
    job = Job(job_id=uuid4())
    if status == JobStatus.RUNNING:
        job = job.as_running()
    elif status == JobStatus.COMPLETED:
        job = job.as_running()
        job = job.as_completed(result)
    elif status == JobStatus.FAILED:
        job = job.as_running()
        job = job.as_failed(error or "err")
    return job


def test_job_to_task_pending_is_working():
    job = Job(job_id=uuid4())  # PENDING by default
    r = _job_to_task_response(str(job.job_id), job)
    assert r["status"]["state"] == "working"


def test_job_to_task_running_is_working():
    job = _make_job(JobStatus.RUNNING)
    r = _job_to_task_response(str(job.job_id), job)
    assert r["status"]["state"] == "working"


def test_job_to_task_completed_decodes_result():
    payload = {"order_id": "123", "status": "ok"}
    job = _make_job(JobStatus.COMPLETED, result=json.dumps(payload).encode())
    r = _job_to_task_response(str(job.job_id), job)
    assert r["status"]["state"] == "completed"
    # artifact should contain the decoded payload
    data = r["artifacts"][0]["parts"][0]["data"]
    assert data["order_id"] == "123"


def test_job_to_task_completed_no_result():
    job = _make_job(JobStatus.COMPLETED, result=None)
    r = _job_to_task_response(str(job.job_id), job)
    assert r["status"]["state"] == "completed"


def test_job_to_task_failed_is_failed():
    job = _make_job(JobStatus.FAILED, error="timeout")
    r = _job_to_task_response(str(job.job_id), job)
    assert r["status"]["state"] == "failed"
    assert "timeout" in r["status"]["message"]


def test_job_to_task_cancelled_is_failed():
    # CANCELLED maps to "failed" in A2A (no "cancelled" state in the spec)
    job = Job(job_id=uuid4())
    job = job.as_running()
    job = job.as_cancelled()
    r = _job_to_task_response(str(job.job_id), job)
    assert r["status"]["state"] == "failed"


# ── agent_card() capabilities ─────────────────────────────────────────────────


def test_agent_card_sync_mode_no_async_flags():
    adapter, _ = _make_adapter()
    card = adapter.agent_card(base_url="http://localhost:8080")
    caps = card["capabilities"]
    assert caps["asyncTaskExecution"] is False
    assert caps["stateTransitionHistory"] is False


def test_agent_card_async_mode_flags_set():
    store = StubJobStore()
    runner = StubJobRunner(store)
    adapter, _ = _make_adapter(runner=runner, store=store)
    card = adapter.agent_card(base_url="http://localhost:8080")
    caps = card["capabilities"]
    assert caps["asyncTaskExecution"] is True
    assert caps["stateTransitionHistory"] is True


# ── _working_response ─────────────────────────────────────────────────────────


def test_working_response_shape():
    r = _working_response("task-123")
    assert r["id"] == "task-123"
    assert r["status"]["state"] == "working"
    assert r["artifacts"] == []
