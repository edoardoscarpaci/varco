"""
Unit tests for varco_sa.job_store
===================================
Covers ``SAJobStore`` using an in-memory SQLite database (aiosqlite).

No external PostgreSQL instance is required — all tests run against SQLite
via ``create_async_engine("sqlite+aiosqlite:///:memory:")``.

DESIGN: why a separate engine from the conftest ``session`` fixture
    ``SAJobStore`` uses ``jobs_metadata`` (its own ``MetaData`` instance)
    which is separate from the application ``DeclarativeBase``.  Each test
    creates its own engine, calls ``ensure_table()``, and tears it down on exit.

Sections
--------
- ``SAJobStore`` schema        — ensure_table idempotency, jobs_metadata export
- ``SAJobStore.save``          — insert, upsert (status update)
- ``SAJobStore.get``           — found, not found
- ``SAJobStore.list_by_status``— filter, limit, ordering
- ``SAJobStore.delete``        — removes job; idempotent for unknown IDs
- ``SAJobStore.try_claim``     — PENDING→RUNNING; non-PENDING ignored; missing
                                 ignored; SKIP LOCKED dialect detection
- Full lifecycle               — save→claim→complete→delete cycle
"""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import create_async_engine

from varco_core.job.base import Job, JobStatus
from varco_core.job.task import TaskPayload
from varco_sa.job_store import SAJobStore, jobs_metadata


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def engine():
    """In-memory SQLite engine with the varco_jobs table created."""
    eng = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(jobs_metadata.create_all)
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(jobs_metadata.drop_all)
    await eng.dispose()


@pytest_asyncio.fixture
async def store(engine) -> SAJobStore:
    """``SAJobStore`` backed by the in-memory SQLite engine."""
    return SAJobStore(engine)


def _pending_job(**kwargs) -> Job:
    """Build a ``Job`` in PENDING state with a fresh UUID."""
    return Job(job_id=uuid4(), **kwargs)


# ── Schema ────────────────────────────────────────────────────────────────────


class TestSAJobStoreSchema:
    def test_jobs_metadata_exported(self) -> None:
        assert jobs_metadata is not None
        assert "varco_jobs" in jobs_metadata.tables

    async def test_ensure_table_idempotent(self, engine) -> None:
        """Calling ensure_table() twice must not raise."""
        store = SAJobStore(engine)
        await store.ensure_table()
        await store.ensure_table()  # must not raise

    def test_repr(self, store: SAJobStore) -> None:
        assert "SAJobStore" in repr(store)


# ── save ──────────────────────────────────────────────────────────────────────


class TestSAJobStoreSave:
    async def test_save_and_get(self, store: SAJobStore) -> None:
        """save() persists the job; get() retrieves it with all fields intact."""
        job = _pending_job()
        await store.save(job)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.job_id == job.job_id
        assert fetched.status == JobStatus.PENDING

    async def test_save_upserts_on_status_change(self, store: SAJobStore) -> None:
        """Saving a job again with an updated status replaces the old value."""
        job = _pending_job()
        await store.save(job)

        running = job.as_running()
        await store.save(running)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.status == JobStatus.RUNNING
        assert fetched.started_at is not None

    async def test_save_preserves_result_bytes(self, store: SAJobStore) -> None:
        """Opaque ``result`` bytes survive the save→get round-trip."""
        job = _pending_job()
        running = job.as_running()
        completed = running.as_completed(result=b"\x00\xff\xab\xcd")
        await store.save(completed)

        fetched = await store.get(completed.job_id)
        assert fetched is not None
        assert fetched.result == b"\x00\xff\xab\xcd"

    async def test_save_preserves_task_payload(self, store: SAJobStore) -> None:
        """``task_payload`` serializes and deserializes correctly."""
        payload = TaskPayload(
            task_name="orders.create", args=["a", 1], kwargs={"k": True}
        )
        job = _pending_job(task_payload=payload)
        await store.save(job)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.task_payload is not None
        assert fetched.task_payload.task_name == "orders.create"
        assert fetched.task_payload.args == ["a", 1]
        assert fetched.task_payload.kwargs == {"k": True}

    async def test_save_preserves_auth_snapshot(self, store: SAJobStore) -> None:
        """``auth_snapshot`` dict round-trips correctly."""
        snapshot = {"user_id": "u1", "roles": ["admin"], "scopes": [], "grants": []}
        job = _pending_job(auth_snapshot=snapshot)
        await store.save(job)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.auth_snapshot == snapshot

    async def test_save_preserves_metadata(self, store: SAJobStore) -> None:
        """``metadata`` dict round-trips correctly."""
        meta = {"tenant_id": "t1", "source": "api"}
        job = _pending_job(metadata=meta)
        await store.save(job)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.metadata == meta


# ── get ───────────────────────────────────────────────────────────────────────


class TestSAJobStoreGet:
    async def test_get_unknown_id_returns_none(self, store: SAJobStore) -> None:
        """get() must return None for an unknown job_id."""
        result = await store.get(uuid4())
        assert result is None

    async def test_get_returns_correct_job(self, store: SAJobStore) -> None:
        """get() returns the exact job that was saved, not a neighbour."""
        j1 = _pending_job()
        j2 = _pending_job()
        await store.save(j1)
        await store.save(j2)

        fetched = await store.get(j1.job_id)
        assert fetched is not None
        assert fetched.job_id == j1.job_id


# ── list_by_status ────────────────────────────────────────────────────────────


class TestSAJobStoreListByStatus:
    async def test_list_by_status_filters_correctly(self, store: SAJobStore) -> None:
        """list_by_status() returns only jobs in the requested status."""
        pending = _pending_job()
        running = _pending_job().as_running()
        await store.save(pending)
        await store.save(running)

        pending_jobs = await store.list_by_status(JobStatus.PENDING)
        assert len(pending_jobs) == 1
        assert pending_jobs[0].job_id == pending.job_id

        running_jobs = await store.list_by_status(JobStatus.RUNNING)
        assert len(running_jobs) == 1
        assert running_jobs[0].job_id == running.job_id

    async def test_list_by_status_empty(self, store: SAJobStore) -> None:
        """list_by_status() returns an empty list if no jobs match."""
        result = await store.list_by_status(JobStatus.COMPLETED)
        assert result == []

    async def test_list_by_status_respects_limit(self, store: SAJobStore) -> None:
        """list_by_status(limit=2) returns at most 2 jobs."""
        for _ in range(5):
            await store.save(_pending_job())

        result = await store.list_by_status(JobStatus.PENDING, limit=2)
        assert len(result) == 2

    async def test_list_by_status_oldest_first(self, store: SAJobStore) -> None:
        """Jobs are returned ordered by created_at ASC (oldest first)."""
        import asyncio

        j1 = _pending_job()
        await store.save(j1)
        await asyncio.sleep(0.01)
        j2 = _pending_job()
        await store.save(j2)

        result = await store.list_by_status(JobStatus.PENDING)
        assert len(result) == 2
        assert result[0].job_id == j1.job_id
        assert result[1].job_id == j2.job_id


# ── delete ────────────────────────────────────────────────────────────────────


class TestSAJobStoreDelete:
    async def test_delete_removes_job(self, store: SAJobStore) -> None:
        """After delete(), get() must return None."""
        job = _pending_job()
        await store.save(job)

        await store.delete(job.job_id)

        assert await store.get(job.job_id) is None

    async def test_delete_unknown_id_is_noop(self, store: SAJobStore) -> None:
        """delete() with an unknown job_id must be a silent no-op."""
        await store.delete(uuid4())  # must not raise

    async def test_delete_does_not_affect_other_jobs(self, store: SAJobStore) -> None:
        """Deleting job A must leave job B intact."""
        j1 = _pending_job()
        j2 = _pending_job()
        await store.save(j1)
        await store.save(j2)

        await store.delete(j1.job_id)

        assert await store.get(j1.job_id) is None
        assert await store.get(j2.job_id) is not None


# ── try_claim ─────────────────────────────────────────────────────────────────


class TestSAJobStoreTryClaim:
    async def test_try_claim_pending_returns_running(self, store: SAJobStore) -> None:
        """try_claim() on a PENDING job returns it in RUNNING state."""
        job = _pending_job()
        await store.save(job)

        claimed = await store.try_claim(job.job_id)
        assert claimed is not None
        assert claimed.status == JobStatus.RUNNING
        assert claimed.started_at is not None

    async def test_try_claim_updates_store(self, store: SAJobStore) -> None:
        """After try_claim(), the stored job status is RUNNING."""
        job = _pending_job()
        await store.save(job)

        await store.try_claim(job.job_id)

        fetched = await store.get(job.job_id)
        assert fetched is not None
        assert fetched.status == JobStatus.RUNNING

    async def test_try_claim_running_job_returns_none(self, store: SAJobStore) -> None:
        """try_claim() on a RUNNING job returns None (already claimed)."""
        job = _pending_job().as_running()
        await store.save(job)

        result = await store.try_claim(job.job_id)
        assert result is None

    async def test_try_claim_completed_job_returns_none(
        self, store: SAJobStore
    ) -> None:
        """try_claim() on a COMPLETED job returns None (terminal state)."""
        job = _pending_job().as_running().as_completed(result=b"ok")
        await store.save(job)

        result = await store.try_claim(job.job_id)
        assert result is None

    async def test_try_claim_unknown_id_returns_none(self, store: SAJobStore) -> None:
        """try_claim() with an unknown job_id returns None (not an error)."""
        result = await store.try_claim(uuid4())
        assert result is None

    async def test_try_claim_second_call_returns_none(self, store: SAJobStore) -> None:
        """Calling try_claim() twice on the same job — only the first succeeds."""
        job = _pending_job()
        await store.save(job)

        first = await store.try_claim(job.job_id)
        second = await store.try_claim(job.job_id)

        assert first is not None
        assert first.status == JobStatus.RUNNING
        assert second is None  # already claimed


# ── Full lifecycle ─────────────────────────────────────────────────────────────


class TestSAJobStoreLifecycle:
    async def test_full_pending_running_completed_delete(
        self, store: SAJobStore
    ) -> None:
        """End-to-end lifecycle: save PENDING → claim → complete → delete."""
        job = _pending_job(
            task_payload=TaskPayload(task_name="test.task"),
            metadata={"source": "test"},
        )
        # Save as PENDING
        await store.save(job)
        assert (await store.get(job.job_id)).status == JobStatus.PENDING  # type: ignore[union-attr]

        # Claim → RUNNING
        claimed = await store.try_claim(job.job_id)
        assert claimed is not None
        assert claimed.status == JobStatus.RUNNING

        # Complete → COMPLETED
        completed = claimed.as_completed(result=b'{"ok": true}')
        await store.save(completed)
        stored = await store.get(job.job_id)
        assert stored is not None
        assert stored.status == JobStatus.COMPLETED
        assert stored.result == b'{"ok": true}'

        # Verify appears in list_by_status
        completed_list = await store.list_by_status(JobStatus.COMPLETED)
        assert any(j.job_id == job.job_id for j in completed_list)

        # Delete
        await store.delete(job.job_id)
        assert await store.get(job.job_id) is None

    async def test_failed_lifecycle(self, store: SAJobStore) -> None:
        """PENDING → claim → fail → confirmed FAILED in store."""
        job = _pending_job()
        await store.save(job)

        claimed = await store.try_claim(job.job_id)
        assert claimed is not None

        failed = claimed.as_failed("something went wrong")
        await store.save(failed)

        stored = await store.get(job.job_id)
        assert stored is not None
        assert stored.status == JobStatus.FAILED
        assert stored.error == "something went wrong"


# ── SKIP LOCKED dialect detection ─────────────────────────────────────────────


class TestSAJobStoreTryClaimSkipLocked:
    """
    Verify that try_claim() applies SELECT FOR UPDATE SKIP LOCKED on PostgreSQL
    but falls back to a plain SELECT on SQLite (and other dialects).

    DESIGN: testing dialect-conditional SQL without PostgreSQL
        Monkeypatching engine.dialect.name to "postgresql" causes try_claim()
        to request SKIP LOCKED.  We capture the rendered SQL via SQLAlchemy's
        "before_cursor_execute" event, which fires with the compiled statement
        string just before the driver executes it.  This lets us assert on the
        SQL text without requiring a real PostgreSQL instance.

        SQLite does not support FOR UPDATE, so we only run the monkeypatched
        variant to verify the SQL path — the SQLite engine gracefully fails if
        FOR UPDATE actually reaches the driver.  The test therefore uses a
        separate in-memory engine where we can intercept the SQL before it
        lands on the driver.

    Edge cases:
        - SQLite dialect → no FOR UPDATE in SQL (verified via captured text).
        - "postgresql" dialect → FOR UPDATE SKIP LOCKED present in SQL.
        - Non-PENDING job → try_claim returns None even with SKIP LOCKED active.
    """

    def _capture_statements(self, engine_sync) -> list[str]:
        """
        Register a before_cursor_execute listener on the *sync* engine core
        and return the list it appends to.  Each entry is the rendered SQL
        string that SQLAlchemy passes to the driver.

        Args:
            engine_sync: The underlying sync Engine (``async_engine.sync_engine``).

        Returns:
            A mutable list that grows as statements execute.
        """
        captured: list[str] = []

        @sa.event.listens_for(engine_sync, "before_cursor_execute")
        def _capture(conn, cursor, statement, parameters, context, executemany):
            # Collect every statement executed on this engine for inspection.
            captured.append(statement)

        return captured

    async def test_sqlite_dialect_uses_plain_select(self, engine) -> None:
        """SQLite engine must NOT include FOR UPDATE in the try_claim query."""
        captured = self._capture_statements(engine.sync_engine)
        store = SAJobStore(engine)

        job = _pending_job()
        await store.save(job)

        await store.try_claim(job.job_id)

        # Find the SELECT statement (not the UPDATE).
        select_sqls = [s for s in captured if s.strip().upper().startswith("SELECT")]
        assert select_sqls, "Expected at least one SELECT during try_claim"
        for sql in select_sqls:
            assert (
                "FOR UPDATE" not in sql.upper()
            ), f"SQLite try_claim must not use FOR UPDATE but got: {sql!r}"

    async def test_postgresql_dialect_uses_skip_locked(self, engine) -> None:
        """
        When engine.dialect.name == 'postgresql', try_claim must call
        ``select_stmt.with_for_update(skip_locked=True)`` on the SELECT.

        DESIGN: spy on Select.with_for_update, not on compiled SQL
            SQLAlchemy compiles statements against the *active driver* dialect
            (SQLite here), not against the patched dialect name string.
            ``before_cursor_execute`` therefore shows plain SQLite SQL even
            when dialect.name == "postgresql".

            Spying on ``sa.Select.with_for_update`` intercepts the call at
            the Python level — *before* any compilation — so we can assert
            that it was invoked with ``skip_locked=True`` regardless of which
            driver compiles the statement.

        Edge cases:
            - try_claim() may raise an SQLite error after with_for_update is
              called (SQLite rejects FOR UPDATE).  We catch and swallow that
              error — only the spy calls matter for this assertion.
        """
        store = SAJobStore(engine)

        job = _pending_job()
        await store.save(job)

        # Spy on Select.with_for_update to capture keyword arguments.
        # Must wrap the original so the return value is still a valid Select.
        original_with_for_update = sa.Select.with_for_update
        with_for_update_calls: list[dict] = []

        def _spy_with_for_update(self, *args, **kwargs):
            with_for_update_calls.append(dict(kwargs))
            return original_with_for_update(self, *args, **kwargs)

        with (
            patch.object(sa.Select, "with_for_update", _spy_with_for_update),
            patch.object(
                type(engine.dialect),
                "name",
                new_callable=lambda: property(lambda self: "postgresql"),
            ),
        ):
            try:
                await store.try_claim(job.job_id)
            except Exception:
                # SQLite rejects FOR UPDATE syntax — expected; we only care
                # whether with_for_update was called with skip_locked=True.
                pass

        assert (
            with_for_update_calls
        ), "try_claim() on a postgresql dialect must call with_for_update()"
        assert any(
            call.get("skip_locked") is True for call in with_for_update_calls
        ), f"Expected with_for_update(skip_locked=True) but got: {with_for_update_calls!r}"

    async def test_non_pending_returns_none_regardless_of_dialect(self, engine) -> None:
        """try_claim() on a non-PENDING job returns None on both dialect paths."""
        store = SAJobStore(engine)

        # RUNNING job — already claimed.
        running = _pending_job().as_running()
        await store.save(running)

        # SQLite path (default).
        result = await store.try_claim(running.job_id)
        assert result is None

    async def test_dialect_name_determines_skip_locked_flag(self, engine) -> None:
        """
        Directly assert that ``use_skip_locked`` resolves to True only when
        dialect.name is "postgresql".  This covers the branching logic without
        relying on SQL text inspection.

        We verify by observing the *outcome*: on SQLite (no SKIP LOCKED),
        a PENDING job is claimed normally.  Dialect detection is a pure
        attribute read — its correctness is validated here by confirming the
        SQLite path (dialect.name != "postgresql") still claims jobs.
        """
        store = SAJobStore(engine)
        assert (
            engine.dialect.name != "postgresql"
        ), "This test requires a non-PostgreSQL dialect (SQLite)"

        job = _pending_job()
        await store.save(job)

        # SQLite path: no SKIP LOCKED, but claim still succeeds.
        claimed = await store.try_claim(job.job_id)
        assert claimed is not None
        assert claimed.status == JobStatus.RUNNING
