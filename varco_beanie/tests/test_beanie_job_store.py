"""
Unit tests for varco_beanie.job_store
=======================================
Covers ``JobDocument`` and ``BeanieJobStore`` using mocked Beanie operations.

No MongoDB connection is required — all Beanie collection-level operations
(``insert``, ``find``, ``find_one``, ``delete``, ``update_one``) are mocked.
The conftest ``bypass_beanie_collection_check`` fixture allows instantiating
``JobDocument`` without ``init_beanie()``.

DESIGN: class-level attribute assignment over patch.object
    Beanie's model metaclass intercepts ``setattr`` for field names in some
    versions.  Filter expressions like ``JobDocument.id == job_id`` evaluate
    ``JobDocument.id`` at the class level — this is a Beanie ExpressionField
    descriptor that only exists after ``init_beanie()``.  To bypass this,
    tests set class-level attributes directly (``JobDocument.id = MagicMock()``),
    save the original value, and restore it in a ``finally`` block.

    This is the same pattern used in ``test_beanie_outbox.py``.

Sections
--------
- ``JobDocument``          — field defaults, Settings.name, repr
- Serialization helpers    — _job_to_doc, _doc_to_job round-trip; datetime coercion
- ``BeanieJobStore``       — construction, repr
- ``save()``               — calls find().delete() + doc.insert(); correct fields
- ``get()``                — delegates to find_one(); returns None for unknown id
- ``list_by_status()``     — correct filter, sort, limit chain; empty list
- ``delete()``             — calls find().delete(); idempotent for unknown ids
- ``try_claim()``          — PENDING→RUNNING via find_one().update_one(); non-PENDING
                             returns None; unknown id returns None
- Full lifecycle           — save→claim→complete→delete via mocks
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4


from varco_core.job.base import Job, JobStatus
from varco_core.job.task import TaskPayload
from varco_beanie.job_store import (
    BeanieJobStore,
    JobDocument,
    _doc_to_job,
    _ensure_tz,
    _job_to_doc,
)


# ── Helpers ────────────────────────────────────────────────────────────────────


def _pending_job(**kwargs) -> Job:
    """Build a ``Job`` in PENDING state with a fresh UUID."""
    return Job(job_id=uuid4(), **kwargs)


def _fake_doc_for_job(job: Job) -> JobDocument:
    """Build a ``JobDocument`` that mirrors the given ``Job``."""
    return JobDocument(
        id=job.job_id,
        status=job.status.value,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        result=job.result,
        error=job.error,
        callback_url=job.callback_url,
        auth_snapshot=job.auth_snapshot,
        request_token=job.request_token,
        job_metadata=job.metadata,
        task_payload=(
            job.task_payload.to_dict() if job.task_payload is not None else None
        ),
    )


def _make_find_chain(docs: list) -> MagicMock:
    """
    Build a Beanie ``find()`` query chain mock supporting:
        find(...).sort(...).limit(...).to_list()   — used by list_by_status()
        find(...).delete()                          — used by save() and delete()

    Args:
        docs: Documents returned by ``to_list()``.

    Returns:
        A ``MagicMock`` behaving like the ``find()`` chain.
    """
    # Build bottom-up: to_list() → limit() → sort() → find()
    fake_to_list = AsyncMock(return_value=docs)
    fake_after_limit = MagicMock()
    fake_after_limit.to_list = fake_to_list
    fake_after_sort = MagicMock()
    fake_after_sort.limit = MagicMock(return_value=fake_after_limit)
    fake_chain = MagicMock()
    fake_chain.sort = MagicMock(return_value=fake_after_sort)
    fake_chain.delete = AsyncMock()
    return fake_chain


def _make_try_claim_chain(updated_doc: JobDocument | None) -> MagicMock:
    """
    Build the ``find_one().update_one()`` chain mock for ``try_claim()``.

    ``try_claim()`` does::

        await JobDocument.find_one(id_filter, status_filter)
            .update_one(Set({...}), response_type=NEW_DOCUMENT)

    ``find_one()`` returns a query object; ``update_one()`` on that object must
    itself be awaitable (i.e. ``AsyncMock``) so that ``await .update_one(...)``
    resolves to the updated document (or ``None`` on no-match).

    DESIGN: update_one must be AsyncMock, not MagicMock(return_value=AsyncMock)
        ``await foo.update_one(...)`` evaluates as:
            1. Call ``foo.update_one(...)`` → get the return value.
            2. ``await`` the return value.
        If ``update_one`` is an ``AsyncMock``, step 1 returns a coroutine that
        step 2 can await.  If ``update_one`` is a ``MagicMock`` that returns an
        ``AsyncMock`` instance, step 1 returns the ``AsyncMock`` class instance —
        and ``await <AsyncMock instance>`` raises ``TypeError``.

    Args:
        updated_doc: Document to return from the awaited chain.
                     ``None`` simulates no-match (job not found or not PENDING).

    Returns:
        A ``MagicMock`` representing the result of ``JobDocument.find_one(...)``.
    """
    find_one_result = MagicMock()
    # AsyncMock: calling update_one(...) returns a coroutine; awaiting it gives updated_doc.
    find_one_result.update_one = AsyncMock(return_value=updated_doc)
    return find_one_result


@contextmanager
def _patch_doc_attrs(**overrides: Any) -> Generator[None, None, None]:
    """
    Context manager that directly sets ``JobDocument`` class attributes and
    restores them on exit.

    DESIGN: direct class attribute assignment over patch.object
        Beanie's model metaclass intercepts ``setattr`` for field names in
        some versions, causing ``patch.object`` to fail silently.
        Direct assignment bypasses the metaclass and sets the attribute
        in the class ``__dict__``.  We save the original value and restore
        it in the ``finally`` block to avoid test pollution.

        The same pattern is used in ``test_beanie_outbox.py`` for
        ``OutboxDocument.id``, ``OutboxDocument.find_one``, etc.

    Args:
        **overrides: Attribute name → value to set on ``JobDocument``.

    Yields:
        None — the context body runs with the overrides active.
    """
    originals: dict[str, Any] = {}
    for name in overrides:
        originals[name] = JobDocument.__dict__.get(name)

    try:
        for name, value in overrides.items():
            setattr(JobDocument, name, value)
        yield
    finally:
        for name, orig in originals.items():
            if orig is not None:
                setattr(JobDocument, name, orig)
            elif hasattr(JobDocument, name):
                delattr(JobDocument, name)


# ── JobDocument ────────────────────────────────────────────────────────────────


class TestJobDocument:
    def test_collection_name(self) -> None:
        assert JobDocument.Settings.name == "varco_jobs"

    def test_id_is_uuid_by_default(self) -> None:
        """JobDocument gets a fresh UUIDv4 if no id is provided."""
        doc = JobDocument(
            status=JobStatus.PENDING.value,
            created_at=datetime.now(tz=timezone.utc),
        )
        assert isinstance(doc.id, uuid.UUID)

    def test_id_is_unique_per_instance(self) -> None:
        now = datetime.now(tz=timezone.utc)
        doc1 = JobDocument(status="pending", created_at=now)
        doc2 = JobDocument(status="pending", created_at=now)
        assert doc1.id != doc2.id

    def test_optional_fields_default_to_none(self) -> None:
        doc = JobDocument(
            status=JobStatus.PENDING.value,
            created_at=datetime.now(tz=timezone.utc),
        )
        assert doc.started_at is None
        assert doc.completed_at is None
        assert doc.result is None
        assert doc.error is None
        assert doc.callback_url is None
        assert doc.auth_snapshot is None
        assert doc.request_token is None
        assert doc.task_payload is None

    def test_job_metadata_defaults_to_empty_dict(self) -> None:
        doc = JobDocument(
            status=JobStatus.PENDING.value,
            created_at=datetime.now(tz=timezone.utc),
        )
        assert doc.job_metadata == {}

    def test_repr_contains_key_fields(self) -> None:
        doc = JobDocument(
            status=JobStatus.PENDING.value,
            created_at=datetime.now(tz=timezone.utc),
        )
        r = repr(doc)
        assert "JobDocument" in r
        assert "pending" in r

    def test_status_stored_as_string(self) -> None:
        """status is stored as the StrEnum value, not the enum object."""
        doc = JobDocument(
            status=JobStatus.RUNNING.value,
            created_at=datetime.now(tz=timezone.utc),
        )
        assert doc.status == "running"


# ── Serialization helpers ─────────────────────────────────────────────────────


class TestEnsureTz:
    def test_none_returns_none(self) -> None:
        assert _ensure_tz(None) is None

    def test_aware_datetime_unchanged(self) -> None:
        dt = datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc)
        result = _ensure_tz(dt)
        assert result is dt
        assert result.tzinfo == timezone.utc

    def test_naive_datetime_coerced_to_utc(self) -> None:
        naive = datetime(2024, 6, 15, 10, 30, 0)
        result = _ensure_tz(naive)
        assert result is not None
        assert result.tzinfo == timezone.utc
        assert result.year == 2024
        assert result.month == 6


class TestSerializationRoundTrip:
    def test_basic_job_round_trips(self) -> None:
        """All core fields survive _job_to_doc + _doc_to_job."""
        job = _pending_job()
        doc = _job_to_doc(job)
        restored = _doc_to_job(doc)

        assert restored.job_id == job.job_id
        assert restored.status == job.status
        assert restored.created_at == job.created_at
        assert restored.started_at is None
        assert restored.completed_at is None

    def test_running_job_round_trips(self) -> None:
        job = _pending_job().as_running()
        doc = _job_to_doc(job)
        restored = _doc_to_job(doc)

        assert restored.status == JobStatus.RUNNING
        assert restored.started_at is not None

    def test_completed_job_round_trips_result(self) -> None:
        job = _pending_job().as_running().as_completed(result=b"\x00\xff\xab")
        doc = _job_to_doc(job)
        restored = _doc_to_job(doc)

        assert restored.status == JobStatus.COMPLETED
        assert restored.result == b"\x00\xff\xab"

    def test_failed_job_round_trips_error(self) -> None:
        job = _pending_job().as_running().as_failed("oops")
        doc = _job_to_doc(job)
        restored = _doc_to_job(doc)

        assert restored.status == JobStatus.FAILED
        assert restored.error == "oops"

    def test_task_payload_round_trips(self) -> None:
        payload = TaskPayload(
            task_name="orders.create", args=[1, 2], kwargs={"k": True}
        )
        job = _pending_job(task_payload=payload)
        doc = _job_to_doc(job)
        restored = _doc_to_job(doc)

        assert restored.task_payload is not None
        assert restored.task_payload.task_name == "orders.create"
        assert restored.task_payload.args == [1, 2]
        assert restored.task_payload.kwargs == {"k": True}

    def test_none_task_payload_round_trips(self) -> None:
        job = _pending_job(task_payload=None)
        doc = _job_to_doc(job)
        restored = _doc_to_job(doc)

        assert restored.task_payload is None

    def test_auth_snapshot_round_trips(self) -> None:
        snapshot = {"user_id": "u1", "roles": ["admin"], "scopes": [], "grants": []}
        job = _pending_job(auth_snapshot=snapshot)
        doc = _job_to_doc(job)
        restored = _doc_to_job(doc)

        assert restored.auth_snapshot == snapshot

    def test_metadata_round_trips(self) -> None:
        meta = {"tenant_id": "t1", "source": "api"}
        job = _pending_job(metadata=meta)
        doc = _job_to_doc(job)
        restored = _doc_to_job(doc)

        assert restored.metadata == meta

    def test_doc_to_job_coerces_naive_created_at(self) -> None:
        """Naive datetimes from MongoDB are coerced to UTC."""
        job = _pending_job()
        doc = _job_to_doc(job)
        # Simulate MongoDB returning a naive created_at.
        doc.created_at = datetime(2024, 3, 1, 8, 0, 0)  # naive

        restored = _doc_to_job(doc)
        assert restored.created_at.tzinfo == timezone.utc


# ── BeanieJobStore construction ───────────────────────────────────────────────


class TestBeanieJobStoreConstruction:
    def test_repr(self) -> None:
        store = BeanieJobStore()
        assert "BeanieJobStore" in repr(store)


# ── save() ─────────────────────────────────────────────────────────────────────


class TestBeanieJobStoreSave:
    async def test_save_calls_delete_then_insert(self) -> None:
        """save() must call find().delete() then doc.insert()."""
        store = BeanieJobStore()
        job = _pending_job()

        find_chain = _make_find_chain([])
        insert_mock = AsyncMock()

        with _patch_doc_attrs(
            id=MagicMock(), find=MagicMock(return_value=find_chain), insert=insert_mock
        ):
            await store.save(job)

        find_chain.delete.assert_awaited_once()
        insert_mock.assert_awaited_once()

    async def test_save_inserts_with_correct_fields(self) -> None:
        """The JobDocument passed to insert() must have all Job fields set."""
        store = BeanieJobStore()
        payload = TaskPayload(task_name="orders.ship", args=["a"], kwargs={})
        meta = {"tenant_id": "t1"}
        snap = {"user_id": "u1", "roles": [], "scopes": [], "grants": []}
        job = _pending_job(task_payload=payload, metadata=meta, auth_snapshot=snap)

        captured_docs: list[JobDocument] = []
        find_chain = _make_find_chain([])

        async def fake_insert(self_doc: JobDocument, **kwargs: Any) -> None:
            captured_docs.append(self_doc)

        with _patch_doc_attrs(
            id=MagicMock(), find=MagicMock(return_value=find_chain), insert=fake_insert
        ):
            await store.save(job)

        assert len(captured_docs) == 1
        doc = captured_docs[0]
        assert doc.id == job.job_id
        assert doc.status == JobStatus.PENDING.value
        assert doc.task_payload == {
            "task_name": "orders.ship",
            "args": ["a"],
            "kwargs": {},
        }
        assert doc.job_metadata == meta
        assert doc.auth_snapshot == snap

    async def test_save_preserves_result_bytes(self) -> None:
        """Opaque ``result`` bytes are forwarded to the JobDocument."""
        store = BeanieJobStore()
        completed = _pending_job().as_running().as_completed(result=b"\x00\xff\xab\xcd")

        captured_docs: list[JobDocument] = []
        find_chain = _make_find_chain([])

        async def fake_insert(self_doc: JobDocument, **kwargs: Any) -> None:
            captured_docs.append(self_doc)

        with _patch_doc_attrs(
            id=MagicMock(), find=MagicMock(return_value=find_chain), insert=fake_insert
        ):
            await store.save(completed)

        assert captured_docs[0].result == b"\x00\xff\xab\xcd"


# ── get() ──────────────────────────────────────────────────────────────────────


class TestBeanieJobStoreGet:
    async def test_get_returns_job_when_found(self) -> None:
        """get() must return a Job built from the found document."""
        store = BeanieJobStore()
        job = _pending_job()
        doc = _fake_doc_for_job(job)

        with _patch_doc_attrs(id=MagicMock(), find_one=AsyncMock(return_value=doc)):
            result = await store.get(job.job_id)

        assert result is not None
        assert result.job_id == job.job_id
        assert result.status == JobStatus.PENDING

    async def test_get_returns_none_for_unknown_id(self) -> None:
        """get() returns None when find_one() returns None."""
        store = BeanieJobStore()

        with _patch_doc_attrs(id=MagicMock(), find_one=AsyncMock(return_value=None)):
            result = await store.get(uuid4())

        assert result is None

    async def test_get_returns_correct_job_for_completed_status(self) -> None:
        """get() correctly deserializes a COMPLETED job with result bytes."""
        store = BeanieJobStore()
        completed = _pending_job().as_running().as_completed(result=b"ok")
        doc = _fake_doc_for_job(completed)

        with _patch_doc_attrs(id=MagicMock(), find_one=AsyncMock(return_value=doc)):
            result = await store.get(completed.job_id)

        assert result is not None
        assert result.status == JobStatus.COMPLETED
        assert result.result == b"ok"


# ── list_by_status() ──────────────────────────────────────────────────────────


class TestBeanieJobStoreListByStatus:
    async def test_list_by_status_returns_matching_jobs(self) -> None:
        """list_by_status() must convert found documents to Job objects."""
        store = BeanieJobStore()
        job = _pending_job()
        doc = _fake_doc_for_job(job)
        find_chain = _make_find_chain([doc])

        with _patch_doc_attrs(
            status=MagicMock(),
            created_at=MagicMock(),
            find=MagicMock(return_value=find_chain),
        ):
            result = await store.list_by_status(JobStatus.PENDING)

        assert len(result) == 1
        assert result[0].job_id == job.job_id
        assert result[0].status == JobStatus.PENDING

    async def test_list_by_status_empty_returns_empty_list(self) -> None:
        """list_by_status() returns [] when no documents match."""
        store = BeanieJobStore()
        find_chain = _make_find_chain([])

        with _patch_doc_attrs(
            status=MagicMock(),
            created_at=MagicMock(),
            find=MagicMock(return_value=find_chain),
        ):
            result = await store.list_by_status(JobStatus.COMPLETED)

        assert result == []

    async def test_list_by_status_applies_limit(self) -> None:
        """list_by_status(limit=2) must pass limit=2 to the chain."""
        store = BeanieJobStore()

        # Capture what .limit() was called with.
        captured_limits: list[int] = []
        fake_to_list = AsyncMock(return_value=[])
        fake_after_limit = MagicMock()
        fake_after_limit.to_list = fake_to_list
        fake_after_sort = MagicMock()

        def _fake_limit(n: int):
            captured_limits.append(n)
            return fake_after_limit

        fake_after_sort.limit = _fake_limit
        find_chain = MagicMock()
        find_chain.sort = MagicMock(return_value=fake_after_sort)

        with _patch_doc_attrs(
            status=MagicMock(),
            created_at=MagicMock(),
            find=MagicMock(return_value=find_chain),
        ):
            await store.list_by_status(JobStatus.PENDING, limit=2)

        assert captured_limits == [2]


# ── delete() ──────────────────────────────────────────────────────────────────


class TestBeanieJobStoreDelete:
    async def test_delete_calls_find_and_delete(self) -> None:
        """delete() must call find().delete()."""
        store = BeanieJobStore()
        job = _pending_job()
        find_chain = _make_find_chain([])

        with _patch_doc_attrs(id=MagicMock(), find=MagicMock(return_value=find_chain)):
            await store.delete(job.job_id)

        find_chain.delete.assert_awaited_once()

    async def test_delete_unknown_id_is_noop(self) -> None:
        """delete() with an unknown id must not raise."""
        store = BeanieJobStore()
        find_chain = _make_find_chain([])

        with _patch_doc_attrs(id=MagicMock(), find=MagicMock(return_value=find_chain)):
            await store.delete(uuid4())  # must not raise

        find_chain.delete.assert_awaited_once()


# ── try_claim() ────────────────────────────────────────────────────────────────


class TestBeanieJobStoreTryClaim:
    # Convenience: all try_claim tests need id, status, started_at patched
    # because try_claim uses:
    #   find_one(JobDocument.id == ..., JobDocument.status == ...)  — id, status
    #   Set({JobDocument.status: ..., JobDocument.started_at: ...}) — status, started_at

    def _try_claim_patches(self, find_one_value) -> dict:
        """Return the full set of class-attr patches needed for try_claim tests."""
        return dict(
            id=MagicMock(),
            status=MagicMock(),
            started_at=MagicMock(),
            find_one=MagicMock(return_value=find_one_value),
        )

    async def test_try_claim_pending_returns_running_job(self) -> None:
        """try_claim() on a PENDING job returns it in RUNNING state."""
        store = BeanieJobStore()
        job = _pending_job()
        running_doc = _fake_doc_for_job(job.as_running())

        with _patch_doc_attrs(
            **self._try_claim_patches(_make_try_claim_chain(running_doc))
        ):
            result = await store.try_claim(job.job_id)

        assert result is not None
        assert result.job_id == job.job_id
        assert result.status == JobStatus.RUNNING

    async def test_try_claim_returns_none_when_not_pending(self) -> None:
        """try_claim() when find_one+update returns None (no PENDING match) → None."""
        store = BeanieJobStore()

        with _patch_doc_attrs(**self._try_claim_patches(_make_try_claim_chain(None))):
            result = await store.try_claim(uuid4())

        assert result is None

    async def test_try_claim_uses_update_one_with_new_document_response(self) -> None:
        """
        try_claim() must call update_one(..., response_type=UpdateResponse.NEW_DOCUMENT)
        to perform the atomic findAndModify.
        """
        from beanie import UpdateResponse

        store = BeanieJobStore()
        job = _pending_job()
        running_doc = _fake_doc_for_job(job.as_running())

        captured_kwargs: list[dict] = []
        find_one_result = MagicMock()

        async def spy_update_one(*args, **kwargs):
            captured_kwargs.append(dict(kwargs))
            return running_doc

        find_one_result.update_one = spy_update_one

        with _patch_doc_attrs(
            id=MagicMock(),
            status=MagicMock(),
            started_at=MagicMock(),
            find_one=MagicMock(return_value=find_one_result),
        ):
            await store.try_claim(job.job_id)

        assert captured_kwargs, "update_one must have been called"
        assert any(
            kw.get("response_type") == UpdateResponse.NEW_DOCUMENT
            for kw in captured_kwargs
        ), f"Expected response_type=NEW_DOCUMENT in update_one kwargs but got: {captured_kwargs!r}"

    async def test_try_claim_filter_includes_two_expressions(self) -> None:
        """
        find_one() must receive two filter expressions: job_id and status==PENDING.
        """
        store = BeanieJobStore()
        find_one_args: list[tuple] = []
        find_one_result = _make_try_claim_chain(None)

        def spy_find_one(*args, **kwargs):
            find_one_args.append(args)
            return find_one_result

        with _patch_doc_attrs(
            id=MagicMock(),
            status=MagicMock(),
            started_at=MagicMock(),
            find_one=spy_find_one,
        ):
            await store.try_claim(uuid4())

        assert find_one_args, "find_one must have been called"
        # Verify both id and status filter expressions are passed.
        args = find_one_args[0]
        assert (
            len(args) >= 2
        ), f"Expected find_one to receive id + status filters, got: {args!r}"

    async def test_try_claim_unknown_id_returns_none(self) -> None:
        """try_claim() with an unknown job_id returns None without raising."""
        store = BeanieJobStore()

        with _patch_doc_attrs(**self._try_claim_patches(_make_try_claim_chain(None))):
            result = await store.try_claim(uuid4())

        assert result is None

    async def test_try_claim_started_at_set_on_returned_job(self) -> None:
        """The returned RUNNING job must have started_at set."""
        store = BeanieJobStore()
        job = _pending_job()
        running = job.as_running()
        running_doc = _fake_doc_for_job(running)

        with _patch_doc_attrs(
            **self._try_claim_patches(_make_try_claim_chain(running_doc))
        ):
            result = await store.try_claim(job.job_id)

        assert result is not None
        assert result.started_at is not None


# ── Full lifecycle (mocked) ────────────────────────────────────────────────────


class TestBeanieJobStoreLifecycleMocked:
    """
    Simulated end-to-end lifecycle using in-memory tracking.

    All Beanie class methods are replaced via ``_patch_doc_attrs`` so
    the full ``BeanieJobStore`` orchestration is exercised — including
    serialization round-trips — without a real database.
    """

    async def test_full_save_claim_complete_delete(self) -> None:
        """save PENDING → try_claim → save COMPLETED → delete."""
        store = BeanieJobStore()
        job = _pending_job(
            task_payload=TaskPayload(task_name="orders.ship"),
            metadata={"tenant_id": "t1"},
        )

        # ── save (PENDING) ─────────────────────────────────────────────────
        inserted_docs: list[JobDocument] = []
        find_chain = _make_find_chain([])

        async def fake_insert(self_doc: JobDocument, **kwargs: Any) -> None:
            inserted_docs.append(self_doc)

        with _patch_doc_attrs(
            id=MagicMock(), find=MagicMock(return_value=find_chain), insert=fake_insert
        ):
            await store.save(job)

        # One document inserted with PENDING status.
        assert len(inserted_docs) == 1
        assert inserted_docs[0].status == JobStatus.PENDING.value
        assert inserted_docs[0].id == job.job_id

        # ── try_claim ──────────────────────────────────────────────────────
        running = job.as_running()
        running_doc = _fake_doc_for_job(running)

        with _patch_doc_attrs(
            id=MagicMock(),
            status=MagicMock(),
            started_at=MagicMock(),
            find_one=MagicMock(return_value=_make_try_claim_chain(running_doc)),
        ):
            claimed = await store.try_claim(job.job_id)

        assert claimed is not None
        assert claimed.status == JobStatus.RUNNING
        assert claimed.started_at is not None

        # ── save (COMPLETED) ───────────────────────────────────────────────
        completed = claimed.as_completed(result=b'{"status":"done"}')
        completed_docs: list[JobDocument] = []
        find_chain2 = _make_find_chain([])

        async def fake_insert2(self_doc: JobDocument, **kwargs: Any) -> None:
            completed_docs.append(self_doc)

        with _patch_doc_attrs(
            id=MagicMock(),
            find=MagicMock(return_value=find_chain2),
            insert=fake_insert2,
        ):
            await store.save(completed)

        assert len(completed_docs) == 1
        assert completed_docs[0].status == JobStatus.COMPLETED.value
        assert completed_docs[0].result == b'{"status":"done"}'

        # ── delete ─────────────────────────────────────────────────────────
        delete_chain = _make_find_chain([])

        with _patch_doc_attrs(
            id=MagicMock(), find=MagicMock(return_value=delete_chain)
        ):
            await store.delete(job.job_id)

        delete_chain.delete.assert_awaited_once()

    async def test_failed_lifecycle(self) -> None:
        """save PENDING → try_claim → save FAILED — error is persisted."""
        store = BeanieJobStore()
        job = _pending_job()

        running = job.as_running()
        running_doc = _fake_doc_for_job(running)

        with _patch_doc_attrs(
            id=MagicMock(),
            status=MagicMock(),
            started_at=MagicMock(),
            find_one=MagicMock(return_value=_make_try_claim_chain(running_doc)),
        ):
            claimed = await store.try_claim(job.job_id)

        assert claimed is not None

        # Fail the job.
        failed = claimed.as_failed("something went wrong")
        failed_docs: list[JobDocument] = []
        find_chain = _make_find_chain([])

        async def fake_insert(self_doc: JobDocument, **kwargs: Any) -> None:
            failed_docs.append(self_doc)

        with _patch_doc_attrs(
            id=MagicMock(), find=MagicMock(return_value=find_chain), insert=fake_insert
        ):
            await store.save(failed)

        assert len(failed_docs) == 1
        assert failed_docs[0].status == JobStatus.FAILED.value
        assert failed_docs[0].error == "something went wrong"
