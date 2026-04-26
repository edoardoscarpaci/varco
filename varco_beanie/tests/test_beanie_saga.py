"""
Unit tests for varco_beanie.saga
===================================
Covers ``SagaDocument`` and ``BeanieSagaRepository``.

All Beanie collection-level operations (``insert``, ``find_one``, ``delete``) are
mocked — no MongoDB connection is required.  The conftest ``bypass_beanie_collection_check``
fixture allows instantiating ``SagaDocument`` without ``init_beanie()``.

DESIGN: SagaDocument.id mocking pattern (mirrors test_beanie_outbox.py)
    Beanie's Expression DSL (``SagaDocument.id == value``) requires
    ``init_beanie()`` to set up ExpressionField descriptors at the class level.
    Without it, ``SagaDocument.id`` is a Pydantic field descriptor that raises
    ``AttributeError`` when accessed as a class attribute via ``__getattr__``.

    Fix: temporarily replace ``SagaDocument.id`` with a ``MagicMock()`` in each
    test so the expression ``SagaDocument.id == x`` produces a mock (accepted by
    the mocked ``find_one``).  Restore the original in a ``finally`` block.

Sections
--------
- ``SagaDocument``            — field defaults, Settings.name, repr
- ``BeanieSagaRepository``    — construction, repr
- ``save()``                  — find_one + delete + insert; session forwarding
- ``load()``                  — find_one; field mapping; not found; session forwarding
- Round-trip                  — save → load (via mocked find_one)
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Generator
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

from varco_core.service.saga import SagaState, SagaStatus
from varco_beanie.saga import BeanieSagaRepository, SagaDocument


# ── Mock helpers ──────────────────────────────────────────────────────────────


@contextmanager
def _mock_saga_doc_class(
    find_one_return: Any = None,
) -> Generator[dict[str, Any], None, None]:
    """
    Context manager that temporarily replaces ``SagaDocument.id`` and
    ``SagaDocument.find_one`` with mocks so tests can call repository methods
    without a real MongoDB connection.

    Yields a ``mocks`` dict with keys ``"find_one"`` and ``"insert"``.

    DESIGN: direct class attribute assignment over ``patch.object``
        Beanie's metaclass intercepts ``patch.object`` for class-level
        descriptors.  Direct assignment (``SagaDocument.id = MagicMock()``)
        places the mock in ``__dict__`` and shadows the descriptor.
        ``delattr`` in the finally block un-shadows and restores the original.
    """
    original_id = SagaDocument.__dict__.get("id")
    original_find_one = SagaDocument.__dict__.get("find_one")

    find_one_mock = AsyncMock(return_value=find_one_return)
    insert_mock = AsyncMock()

    SagaDocument.id = MagicMock()  # type: ignore[assignment]
    SagaDocument.find_one = find_one_mock  # type: ignore[method-assign]
    SagaDocument.insert = insert_mock  # type: ignore[method-assign]

    try:
        yield {"find_one": find_one_mock, "insert": insert_mock}
    finally:
        # Restore id
        if original_id is not None:
            SagaDocument.id = original_id  # type: ignore[assignment]
        elif hasattr(SagaDocument, "id"):
            delattr(SagaDocument, "id")
        # Restore find_one
        if original_find_one is not None:
            SagaDocument.find_one = original_find_one  # type: ignore[method-assign]
        elif hasattr(SagaDocument, "find_one"):
            del SagaDocument.find_one  # type: ignore[attr-defined]
        # Always restore insert
        if hasattr(SagaDocument, "insert"):
            del SagaDocument.insert  # type: ignore[attr-defined]


# ── SagaDocument ──────────────────────────────────────────────────────────────


class TestSagaDocument:
    def test_collection_name(self) -> None:
        assert SagaDocument.Settings.name == "varco_sagas"

    def test_id_is_uuid(self) -> None:
        doc = SagaDocument(status="PENDING")
        assert isinstance(doc.id, UUID)

    def test_default_completed_steps(self) -> None:
        doc = SagaDocument(status="PENDING")
        assert doc.completed_steps == 0

    def test_default_context_is_empty(self) -> None:
        doc = SagaDocument(status="PENDING")
        assert doc.context == {}

    def test_default_error_is_none(self) -> None:
        doc = SagaDocument(status="PENDING")
        assert doc.error is None

    def test_updated_at_is_recent_utc(self) -> None:
        before = datetime.now(tz=timezone.utc)
        doc = SagaDocument(status="RUNNING")
        after = datetime.now(tz=timezone.utc)
        assert before <= doc.updated_at <= after

    def test_repr_contains_id_and_status(self) -> None:
        doc = SagaDocument(status="PENDING")
        r = repr(doc)
        assert "SagaDocument" in r
        assert "PENDING" in r


# ── BeanieSagaRepository construction ─────────────────────────────────────────


class TestBeanieSagaRepositoryConstruction:
    def test_default_no_session(self) -> None:
        repo = BeanieSagaRepository()
        assert repo._session is None

    def test_with_session(self) -> None:
        fake_session = MagicMock()
        repo = BeanieSagaRepository(session=fake_session)
        assert repo._session is fake_session

    def test_repr_contains_class_name(self) -> None:
        repo = BeanieSagaRepository()
        assert "BeanieSagaRepository" in repr(repo)


# ── save() ────────────────────────────────────────────────────────────────────


class TestBeanieSagaRepositorySave:
    async def test_save_new_saga_calls_insert(self) -> None:
        """save() calls insert() when no existing document is found."""
        repo = BeanieSagaRepository()
        state = SagaState(saga_id=uuid4(), status=SagaStatus.PENDING)

        with _mock_saga_doc_class(find_one_return=None) as mocks:
            await repo.save(state)

        mocks["insert"].assert_called_once()

    async def test_save_existing_saga_deletes_then_inserts(self) -> None:
        """save() deletes the old document before inserting the new one."""
        repo = BeanieSagaRepository()
        state = SagaState(saga_id=uuid4(), status=SagaStatus.RUNNING)

        existing_doc = MagicMock()
        existing_doc.delete = AsyncMock()

        with _mock_saga_doc_class(find_one_return=existing_doc) as mocks:
            await repo.save(state)

        existing_doc.delete.assert_called_once()
        mocks["insert"].assert_called_once()

    async def test_save_passes_session_to_find_one(self) -> None:
        """save() passes ``session`` kwarg to find_one() when set."""
        fake_session = MagicMock()
        repo = BeanieSagaRepository(session=fake_session)
        state = SagaState(saga_id=uuid4())

        with _mock_saga_doc_class(find_one_return=None) as mocks:
            await repo.save(state)

        call_kwargs = mocks["find_one"].call_args.kwargs
        assert call_kwargs.get("session") is fake_session

    async def test_save_passes_session_to_insert(self) -> None:
        """save() passes ``session`` kwarg to insert() when set."""
        fake_session = MagicMock()
        repo = BeanieSagaRepository(session=fake_session)
        state = SagaState(saga_id=uuid4())

        with _mock_saga_doc_class(find_one_return=None) as mocks:
            await repo.save(state)

        mocks["insert"].assert_called_once_with(session=fake_session)

    async def test_save_without_session_calls_insert_without_session(self) -> None:
        """When session=None, insert() is called without a session kwarg."""
        repo = BeanieSagaRepository()
        state = SagaState(saga_id=uuid4())

        with _mock_saga_doc_class(find_one_return=None) as mocks:
            await repo.save(state)

        mocks["insert"].assert_called_once_with()

    async def test_save_builds_document_with_correct_fields(self) -> None:
        """The SagaDocument created in save() must match the SagaState fields."""
        repo = BeanieSagaRepository()
        state = SagaState(
            saga_id=uuid4(),
            status=SagaStatus.RUNNING,
            completed_steps=2,
            context={"order_id": "ord-1"},
            error="some error",
        )

        captured: list[SagaDocument] = []

        async def fake_insert(self_doc: SagaDocument, **kwargs: Any) -> None:
            captured.append(self_doc)

        with _mock_saga_doc_class(find_one_return=None):
            # Override insert with our capturing version.
            SagaDocument.insert = fake_insert  # type: ignore[method-assign]
            await repo.save(state)

        assert len(captured) == 1
        doc = captured[0]
        assert doc.id == state.saga_id
        assert doc.status == "RUNNING"
        assert doc.completed_steps == 2
        assert doc.context == {"order_id": "ord-1"}
        assert doc.error == "some error"


# ── load() ────────────────────────────────────────────────────────────────────


class TestBeanieSagaRepositoryLoad:
    async def test_load_unknown_id_returns_none(self) -> None:
        """load() must return None when find_one() returns None."""
        repo = BeanieSagaRepository()

        with _mock_saga_doc_class(find_one_return=None):
            result = await repo.load(uuid4())

        assert result is None

    async def test_load_maps_document_to_state(self) -> None:
        """load() converts SagaDocument fields to a SagaState correctly."""
        saga_id = uuid4()
        doc = SagaDocument(
            status="RUNNING",
            completed_steps=3,
            context={"payment_id": "pay-99"},
            error=None,
        )
        doc.id = saga_id  # type: ignore[assignment]

        repo = BeanieSagaRepository()
        with _mock_saga_doc_class(find_one_return=doc):
            state = await repo.load(saga_id)

        assert state is not None
        assert state.saga_id == saga_id
        assert state.status == SagaStatus.RUNNING
        assert state.completed_steps == 3
        assert state.context == {"payment_id": "pay-99"}
        assert state.error is None

    async def test_load_maps_error_field(self) -> None:
        """load() preserves the error field for FAILED sagas."""
        saga_id = uuid4()
        doc = SagaDocument(status="FAILED", error="compensation failed")
        doc.id = saga_id  # type: ignore[assignment]

        repo = BeanieSagaRepository()
        with _mock_saga_doc_class(find_one_return=doc):
            state = await repo.load(saga_id)

        assert state is not None
        assert state.status == SagaStatus.FAILED
        assert state.error == "compensation failed"

    async def test_load_passes_session_to_find_one(self) -> None:
        """load() passes ``session`` kwarg to find_one() when set."""
        fake_session = MagicMock()
        repo = BeanieSagaRepository(session=fake_session)

        with _mock_saga_doc_class(find_one_return=None) as mocks:
            await repo.load(uuid4())

        call_kwargs = mocks["find_one"].call_args.kwargs
        assert call_kwargs.get("session") is fake_session

    async def test_load_all_statuses(self) -> None:
        """All SagaStatus values map back correctly from a SagaDocument."""
        repo = BeanieSagaRepository()
        for status in SagaStatus:
            saga_id = uuid4()
            doc = SagaDocument(status=status.value)
            doc.id = saga_id  # type: ignore[assignment]

            with _mock_saga_doc_class(find_one_return=doc):
                state = await repo.load(saga_id)

            assert state is not None
            assert state.status == status


# ── Round-trip ────────────────────────────────────────────────────────────────


class TestBeanieSagaRepositoryRoundTrip:
    async def test_save_then_load_returns_same_state(self) -> None:
        """save() then load() — the state returned matches what was saved."""
        repo = BeanieSagaRepository()
        original = SagaState(
            saga_id=uuid4(),
            status=SagaStatus.COMPENSATING,
            completed_steps=2,
            context={"order_id": "ord-7", "amount": 49.99},
            error="Step [2] failed: card declined",
        )

        # Capture the document inserted so we can return it from load's find_one.
        captured_docs: list[SagaDocument] = []

        async def fake_insert(self_doc: SagaDocument, **kwargs: Any) -> None:
            captured_docs.append(self_doc)

        with _mock_saga_doc_class(find_one_return=None):
            SagaDocument.insert = fake_insert  # type: ignore[method-assign]
            await repo.save(original)

        assert len(captured_docs) == 1
        saved_doc = captured_docs[0]

        # Load using the saved document as the mock return value.
        with _mock_saga_doc_class(find_one_return=saved_doc):
            loaded = await repo.load(original.saga_id)

        assert loaded is not None
        assert loaded.saga_id == original.saga_id
        assert loaded.status == original.status
        assert loaded.completed_steps == original.completed_steps
        assert loaded.context == original.context
        assert loaded.error == original.error
