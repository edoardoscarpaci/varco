"""
Unit tests for varco_redis.job_store
=======================================
All tests use an in-memory ``FakeRedis`` — no real Redis instance required.

``FakeRedis`` implements the subset of ``redis.asyncio`` commands used by
``RedisJobStore``: ``get``, ``set`` (with NX/EX), ``delete``, ``zrange``,
``zadd``, ``zrem``, ``aclose``.

Sections
--------
- ``RedisJobStore`` save/get      — round-trip, field preservation
- ``RedisJobStore`` list_by_status — filter, limit, ordering
- ``RedisJobStore`` delete         — removes job and index entry; idempotent
- ``RedisJobStore`` try_claim      — NX claim; PENDING→RUNNING; races
- Serialization helpers            — result bytes (hex), task_payload, auth_snapshot
- Full lifecycle                   — save→claim→complete cycle
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4


from varco_core.job.base import Job, JobStatus
from varco_core.job.task import TaskPayload
from varco_redis.job_store import RedisJobStore


# ── FakeRedis ─────────────────────────────────────────────────────────────────


class FakeRedis:
    """
    In-memory Redis double for unit tests.

    Implements: get, set (NX, EX), delete, zrange, zadd, zrem, aclose.
    Does NOT implement real TTL expiry — NX=True is simulated as a
    simple conditional set (NX key = "set if not present in _store").

    Thread safety:  ✅ asyncio — single-threaded in tests.
    """

    def __init__(self) -> None:
        self._store: dict[str, bytes | str] = {}
        self._zsets: dict[str, dict[str, float]] = {}

    async def get(self, key: str) -> bytes | None:
        val = self._store.get(key)
        if val is None:
            return None
        return val if isinstance(val, bytes) else val.encode()

    async def set(
        self,
        key: str,
        value: Any,
        *,
        nx: bool = False,
        ex: int | None = None,
    ) -> bool | None:
        """
        SET with optional NX (set-if-not-exists) simulation.

        Returns True if the key was set, None if NX was specified and the key
        already existed (mirrors real Redis behaviour).
        """
        if nx and key in self._store:
            return None  # NX failed — key already present
        self._store[key] = value if isinstance(value, bytes) else str(value)
        return True

    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self._store:
                del self._store[key]
                count += 1
        return count

    async def zadd(self, name: str, mapping: dict[str, float]) -> int:
        zset = self._zsets.setdefault(name, {})
        added = 0
        for member, score in mapping.items():
            if member not in zset:
                added += 1
            zset[member] = score
        return added

    async def zrem(self, name: str, *members: str) -> int:
        zset = self._zsets.get(name, {})
        removed = 0
        for m in members:
            if m in zset:
                del zset[m]
                removed += 1
        return removed

    async def zrange(self, name: str, start: int, stop: int) -> list[bytes]:
        zset = self._zsets.get(name, {})
        # Sort by score ascending (oldest first), then slice.
        sorted_members = sorted(zset.items(), key=lambda x: x[1])
        # stop=-1 means "all" in Redis; adjust for Python slicing.
        end = stop + 1 if stop != -1 else None
        return [m.encode() for m, _ in sorted_members[start:end]]

    async def aclose(self) -> None:
        pass


# ── Fixtures ───────────────────────────────────────────────────────────────────


def _make_store(*, claim_ttl: int = 30) -> tuple[FakeRedis, RedisJobStore]:
    """Return a ``(FakeRedis, RedisJobStore)`` pair with a fresh in-memory backend."""
    fake = FakeRedis()
    store = RedisJobStore(fake, key_prefix="varco:job:", claim_ttl=claim_ttl)  # type: ignore[arg-type]
    return fake, store


def _pending_job(**kwargs: Any) -> Job:
    return Job(job_id=uuid4(), **kwargs)


# ── save / get ────────────────────────────────────────────────────────────────


class TestRedisJobStoreSaveGet:
    async def test_save_and_get_round_trip(self) -> None:
        """save() then get() returns the same job with correct status."""
        _, store = _make_store()
        job = _pending_job()
        await store.save(job)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.job_id == job.job_id
        assert fetched.status == JobStatus.PENDING

    async def test_get_unknown_returns_none(self) -> None:
        _, store = _make_store()
        assert await store.get(uuid4()) is None

    async def test_save_upserts_on_status_change(self) -> None:
        """Saving a job again with a new status replaces the stored value."""
        _, store = _make_store()
        job = _pending_job()
        await store.save(job)

        running = job.as_running()
        await store.save(running)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.status == JobStatus.RUNNING

    async def test_result_bytes_survive_round_trip(self) -> None:
        """Opaque result bytes (including null bytes) round-trip correctly."""
        _, store = _make_store()
        job = _pending_job().as_running().as_completed(result=b"\x00\xff\xab")
        await store.save(job)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.result == b"\x00\xff\xab"

    async def test_task_payload_round_trip(self) -> None:
        """``task_payload`` serializes and deserializes correctly."""
        _, store = _make_store()
        payload = TaskPayload(task_name="orders.create", args=["x"], kwargs={"n": 1})
        job = _pending_job(task_payload=payload)
        await store.save(job)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.task_payload is not None
        assert fetched.task_payload.task_name == "orders.create"
        assert fetched.task_payload.args == ["x"]

    async def test_auth_snapshot_round_trip(self) -> None:
        _, store = _make_store()
        snap = {"user_id": "u1", "roles": ["admin"], "scopes": [], "grants": []}
        job = _pending_job(auth_snapshot=snap)
        await store.save(job)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.auth_snapshot == snap

    async def test_metadata_round_trip(self) -> None:
        _, store = _make_store()
        meta = {"tenant": "t1", "flag": True}
        job = _pending_job(metadata=meta)
        await store.save(job)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.metadata == meta

    async def test_none_result_round_trip(self) -> None:
        _, store = _make_store()
        job = _pending_job()
        await store.save(job)
        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.result is None

    async def test_repr(self) -> None:
        _, store = _make_store()
        assert "RedisJobStore" in repr(store)


# ── list_by_status ────────────────────────────────────────────────────────────


class TestRedisJobStoreListByStatus:
    async def test_list_filters_by_status(self) -> None:
        _, store = _make_store()
        pending = _pending_job()
        running = _pending_job().as_running()
        await store.save(pending)
        await store.save(running)

        pending_list = await store.list_by_status(JobStatus.PENDING)
        running_list = await store.list_by_status(JobStatus.RUNNING)

        assert len(pending_list) == 1
        assert pending_list[0].job_id == pending.job_id
        assert len(running_list) == 1
        assert running_list[0].job_id == running.job_id

    async def test_list_empty_returns_empty_list(self) -> None:
        _, store = _make_store()
        result = await store.list_by_status(JobStatus.COMPLETED)
        assert result == []

    async def test_list_respects_limit(self) -> None:
        _, store = _make_store()
        for _ in range(5):
            await store.save(_pending_job())
        result = await store.list_by_status(JobStatus.PENDING, limit=2)
        assert len(result) == 2

    async def test_list_oldest_first(self) -> None:
        """Sorted Set score (created_at) ensures oldest-first ordering."""
        _, store = _make_store()
        from datetime import timedelta

        # Create jobs with different created_at timestamps
        t0 = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)
        j1 = Job(job_id=uuid4(), created_at=t0)
        j2 = Job(job_id=uuid4(), created_at=t0 + timedelta(seconds=1))
        await store.save(j1)
        await store.save(j2)

        result = await store.list_by_status(JobStatus.PENDING)
        assert len(result) == 2
        assert result[0].job_id == j1.job_id
        assert result[1].job_id == j2.job_id

    async def test_status_index_updated_on_status_change(self) -> None:
        """When job transitions PENDING→RUNNING, it moves between status sets."""
        _, store = _make_store()
        job = _pending_job()
        await store.save(job)

        running = job.as_running()
        await store.save(running)

        # Must no longer appear in PENDING list
        pending_list = await store.list_by_status(JobStatus.PENDING)
        assert all(j.job_id != job.job_id for j in pending_list)

        # Must appear in RUNNING list
        running_list = await store.list_by_status(JobStatus.RUNNING)
        assert any(j.job_id == job.job_id for j in running_list)


# ── delete ────────────────────────────────────────────────────────────────────


class TestRedisJobStoreDelete:
    async def test_delete_removes_job(self) -> None:
        _, store = _make_store()
        job = _pending_job()
        await store.save(job)

        await store.delete(job.job_id)

        assert await store.get(job.job_id) is None

    async def test_delete_removes_from_status_index(self) -> None:
        _, store = _make_store()
        job = _pending_job()
        await store.save(job)

        await store.delete(job.job_id)

        pending_list = await store.list_by_status(JobStatus.PENDING)
        assert all(j.job_id != job.job_id for j in pending_list)

    async def test_delete_unknown_id_is_noop(self) -> None:
        _, store = _make_store()
        await store.delete(uuid4())  # must not raise

    async def test_delete_does_not_affect_other_jobs(self) -> None:
        _, store = _make_store()
        j1 = _pending_job()
        j2 = _pending_job()
        await store.save(j1)
        await store.save(j2)

        await store.delete(j1.job_id)

        assert await store.get(j1.job_id) is None
        assert await store.get(j2.job_id) is not None


# ── try_claim ─────────────────────────────────────────────────────────────────


class TestRedisJobStoreTryClaim:
    async def test_try_claim_pending_returns_running(self) -> None:
        _, store = _make_store()
        job = _pending_job()
        await store.save(job)

        claimed = await store.try_claim(job.job_id)
        assert claimed is not None
        assert claimed.status == JobStatus.RUNNING
        assert claimed.started_at is not None

    async def test_try_claim_updates_store(self) -> None:
        """After try_claim(), the stored job is in RUNNING state."""
        _, store = _make_store()
        job = _pending_job()
        await store.save(job)

        await store.try_claim(job.job_id)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.status == JobStatus.RUNNING

    async def test_try_claim_running_returns_none(self) -> None:
        _, store = _make_store()
        job = _pending_job().as_running()
        await store.save(job)

        result = await store.try_claim(job.job_id)
        assert result is None

    async def test_try_claim_completed_returns_none(self) -> None:
        _, store = _make_store()
        job = _pending_job().as_running().as_completed(result=b"")
        await store.save(job)

        result = await store.try_claim(job.job_id)
        assert result is None

    async def test_try_claim_unknown_id_returns_none(self) -> None:
        _, store = _make_store()
        result = await store.try_claim(uuid4())
        assert result is None

    async def test_try_claim_second_call_returns_none(self) -> None:
        """
        Calling try_claim() twice — first call wins; second is blocked by NX key.

        This test verifies the claim guard: after the first successful claim,
        the NX key is present so the second caller gets ``None``.
        """
        fake, store = _make_store()
        job = _pending_job()
        await store.save(job)

        first = await store.try_claim(job.job_id)
        second = await store.try_claim(job.job_id)

        assert first is not None
        assert first.status == JobStatus.RUNNING
        assert second is None


# ── Full lifecycle ─────────────────────────────────────────────────────────────


class TestRedisJobStoreLifecycle:
    async def test_full_pending_running_completed_delete(self) -> None:
        """End-to-end: save PENDING → claim → complete → delete."""
        _, store = _make_store()

        job = _pending_job(
            task_payload=TaskPayload(task_name="test.task"),
            auth_snapshot={"user_id": "u1", "roles": [], "scopes": [], "grants": []},
        )
        await store.save(job)
        assert (await store.get(job.job_id)).status == JobStatus.PENDING  # type: ignore[union-attr]

        claimed = await store.try_claim(job.job_id)
        assert claimed is not None

        completed = claimed.as_completed(result=b'{"ok": true}')
        await store.save(completed)

        stored = await store.get(job.job_id)
        assert stored is not None
        assert stored.status == JobStatus.COMPLETED
        assert stored.result == b'{"ok": true}'

        completed_list = await store.list_by_status(JobStatus.COMPLETED)
        assert any(j.job_id == job.job_id for j in completed_list)

        await store.delete(job.job_id)
        assert await store.get(job.job_id) is None

    async def test_failed_lifecycle(self) -> None:
        _, store = _make_store()

        job = _pending_job()
        await store.save(job)

        claimed = await store.try_claim(job.job_id)
        assert claimed is not None

        failed = claimed.as_failed("external timeout")
        await store.save(failed)

        stored = await store.get(job.job_id)
        assert stored is not None
        assert stored.status == JobStatus.FAILED
        assert stored.error == "external timeout"
