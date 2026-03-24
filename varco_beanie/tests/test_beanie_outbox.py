"""
Unit tests for varco_beanie.outbox
=====================================
Covers ``OutboxDocument`` and ``BeanieOutboxRepository``.

All Beanie collection-level operations (``insert``, ``find``, ``delete``) are
mocked — no MongoDB connection is required.  The conftest ``bypass_beanie_collection_check``
fixture allows instantiating ``OutboxDocument`` without ``init_beanie()``.

Sections
--------
- ``OutboxDocument``          — field defaults, Settings.name, repr
- ``BeanieOutboxRepository``  — construction, repr
- ``save()``                  — calls doc.insert(); passes session if set
- ``get_pending()``           — calls find().sort().limit().to_list(); applies session
- ``delete()``                — calls find_one() + delete(); noop if not found
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch


from varco_core.event import Event
from varco_core.service.outbox import OutboxEntry
from varco_beanie.outbox import BeanieOutboxRepository, OutboxDocument


# ── Minimal event fixture ──────────────────────────────────────────────────────


class OrderPlacedEvent(Event):
    __event_type__ = "test.order.placed.beanie"
    order_id: str = "ord-1"


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_entry(channel: str = "orders") -> OutboxEntry:
    return OutboxEntry.from_event(OrderPlacedEvent(), channel=channel)


# ── OutboxDocument ────────────────────────────────────────────────────────────


class TestOutboxDocument:
    def test_collection_name(self) -> None:
        assert OutboxDocument.Settings.name == "varco_outbox"

    def test_id_is_uuid(self) -> None:
        doc = OutboxDocument(
            event_type="test.event",
            channel="orders",
            payload=b'{"test": 1}',
        )
        assert isinstance(doc.id, uuid.UUID)

    def test_id_is_unique_per_instance(self) -> None:
        doc1 = OutboxDocument(event_type="t", channel="c", payload=b"p")
        doc2 = OutboxDocument(event_type="t", channel="c", payload=b"p")
        assert doc1.id != doc2.id

    def test_created_at_is_utc(self) -> None:
        before = datetime.now(tz=timezone.utc)
        doc = OutboxDocument(event_type="t", channel="c", payload=b"p")
        after = datetime.now(tz=timezone.utc)
        assert before <= doc.created_at <= after

    def test_repr_contains_id_and_event_type(self) -> None:
        doc = OutboxDocument(event_type="test.event", channel="orders", payload=b"p")
        r = repr(doc)
        assert "OutboxDocument" in r
        assert "test.event" in r


# ── BeanieOutboxRepository construction ──────────────────────────────────────


class TestBeanieOutboxRepositoryConstruction:
    def test_default_no_session(self) -> None:
        repo = BeanieOutboxRepository()
        assert repo._session is None

    def test_repr_no_session(self) -> None:
        repo = BeanieOutboxRepository()
        assert "BeanieOutboxRepository" in repr(repo)
        assert "None" in repr(repo)

    def test_repr_with_session(self) -> None:
        fake_session = MagicMock()
        repo = BeanieOutboxRepository(session=fake_session)
        assert "set" in repr(repo)


# ── save() ────────────────────────────────────────────────────────────────────


class TestBeanieOutboxRepositorySave:
    async def test_save_calls_insert_without_session(self) -> None:
        """save() must call doc.insert() with no session kwarg when session is None."""
        repo = BeanieOutboxRepository()
        entry = _make_entry()

        insert_mock = AsyncMock()
        with patch.object(OutboxDocument, "insert", insert_mock):
            await repo.save(entry)

        insert_mock.assert_called_once_with()

    async def test_save_passes_session_when_set(self) -> None:
        """save() must call doc.insert(session=...) when session is provided."""
        fake_session = MagicMock()
        repo = BeanieOutboxRepository(session=fake_session)
        entry = _make_entry()

        insert_mock = AsyncMock()
        with patch.object(OutboxDocument, "insert", insert_mock):
            await repo.save(entry)

        insert_mock.assert_called_once_with(session=fake_session)

    async def test_save_builds_document_with_correct_fields(self) -> None:
        """The OutboxDocument created in save() must match the OutboxEntry fields."""
        repo = BeanieOutboxRepository()
        entry = _make_entry()

        captured: list[OutboxDocument] = []

        async def fake_insert(self_doc: OutboxDocument, **kwargs: Any) -> None:
            captured.append(self_doc)

        with patch.object(OutboxDocument, "insert", fake_insert):
            await repo.save(entry)

        assert len(captured) == 1
        doc = captured[0]
        assert doc.id == entry.entry_id
        assert doc.event_type == entry.event_type
        assert doc.channel == entry.channel
        assert doc.payload == entry.payload


# ── get_pending() ─────────────────────────────────────────────────────────────


def _make_find_chain(docs: list) -> MagicMock:
    """
    Build the Beanie find() query chain mock: find().sort().limit().to_list().

    DESIGN: Beanie's ``+OutboxDocument.created_at`` expression accesses
    ``OutboxDocument.created_at`` as a class-level descriptor that Beanie
    attaches after ``init_beanie()``.  In unit tests without ``init_beanie()``,
    this attribute is not a Beanie ``ExpressionField`` — it raises
    ``AttributeError`` when ``__pos__`` is called.

    We patch ``OutboxDocument.created_at`` at the class level to a MagicMock
    so ``+OutboxDocument.created_at`` returns a mock the sort() chain accepts.
    Direct class attribute assignment (not ``patch.object``) is required because
    Beanie's metaclass intercepts ``patch.object`` in some versions.
    """
    # Build the chain bottom-up: to_list() is the terminal async call.
    fake_to_list = AsyncMock(return_value=docs)
    # after_limit = the object returned by .limit(n); must expose .to_list()
    fake_after_limit = MagicMock()
    fake_after_limit.to_list = fake_to_list
    # after_sort = the object returned by .sort(...); must expose .limit()
    fake_after_sort = MagicMock()
    fake_after_sort.limit = MagicMock(return_value=fake_after_limit)
    # find() result must expose .sort()
    fake_chain = MagicMock()
    fake_chain.sort = MagicMock(return_value=fake_after_sort)
    return fake_chain


class TestBeanieOutboxRepositoryGetPending:
    async def test_get_pending_returns_outbox_entries(self) -> None:
        """get_pending() must convert OutboxDocuments to OutboxEntry value objects."""
        repo = BeanieOutboxRepository()
        entry = _make_entry()

        # Build a fake OutboxDocument that mimics what would come from MongoDB.
        fake_doc = OutboxDocument(
            id=entry.entry_id,
            event_type=entry.event_type,
            channel=entry.channel,
            payload=entry.payload,
            created_at=entry.created_at,
        )

        # Patch find() on the class and patch created_at to support __pos__.
        original_created_at = OutboxDocument.__dict__.get("created_at")
        try:
            OutboxDocument.created_at = MagicMock()  # type: ignore[assignment]
            OutboxDocument.find = MagicMock(return_value=_make_find_chain([fake_doc]))  # type: ignore[method-assign]
            result = await repo.get_pending(limit=10)
        finally:
            if original_created_at is not None:
                OutboxDocument.created_at = original_created_at  # type: ignore[assignment]
            elif hasattr(OutboxDocument, "created_at"):
                delattr(OutboxDocument, "created_at")
            if hasattr(OutboxDocument, "find"):
                del OutboxDocument.find  # type: ignore[attr-defined]

        assert len(result) == 1
        assert isinstance(result[0], OutboxEntry)
        assert result[0].entry_id == entry.entry_id
        assert result[0].channel == "orders"

    async def test_get_pending_empty_returns_empty_list(self) -> None:
        repo = BeanieOutboxRepository()

        original_created_at = OutboxDocument.__dict__.get("created_at")
        try:
            OutboxDocument.created_at = MagicMock()  # type: ignore[assignment]
            OutboxDocument.find = MagicMock(return_value=_make_find_chain([]))  # type: ignore[method-assign]
            result = await repo.get_pending()
        finally:
            if original_created_at is not None:
                OutboxDocument.created_at = original_created_at  # type: ignore[assignment]
            elif hasattr(OutboxDocument, "created_at"):
                delattr(OutboxDocument, "created_at")
            if hasattr(OutboxDocument, "find"):
                del OutboxDocument.find  # type: ignore[attr-defined]

        assert result == []

    async def test_get_pending_coerces_naive_datetime_to_utc(self) -> None:
        """created_at without tzinfo must be coerced to UTC."""
        repo = BeanieOutboxRepository()
        entry = _make_entry()

        fake_doc = OutboxDocument(
            id=entry.entry_id,
            event_type=entry.event_type,
            channel=entry.channel,
            payload=entry.payload,
            created_at=entry.created_at,
        )
        # Simulate MongoDB returning a naive datetime (no tzinfo).
        fake_doc.created_at = datetime(2024, 1, 1, 12, 0, 0)  # naive

        original_created_at = OutboxDocument.__dict__.get("created_at")
        try:
            OutboxDocument.created_at = MagicMock()  # type: ignore[assignment]
            OutboxDocument.find = MagicMock(return_value=_make_find_chain([fake_doc]))  # type: ignore[method-assign]
            result = await repo.get_pending()
        finally:
            if original_created_at is not None:
                OutboxDocument.created_at = original_created_at  # type: ignore[assignment]
            elif hasattr(OutboxDocument, "created_at"):
                delattr(OutboxDocument, "created_at")
            if hasattr(OutboxDocument, "find"):
                del OutboxDocument.find  # type: ignore[attr-defined]

        assert result[0].created_at.tzinfo is not None


# ── delete() ─────────────────────────────────────────────────────────────────


class TestBeanieOutboxRepositoryDelete:
    async def test_delete_calls_delete_when_doc_found(self) -> None:
        """delete() must call doc.delete() if the document exists."""
        repo = BeanieOutboxRepository()
        entry = _make_entry()

        fake_doc = MagicMock()
        fake_doc.delete = AsyncMock()

        # Direct assignment to bypass Beanie metaclass intercepts.
        original_id = OutboxDocument.__dict__.get("id")
        try:
            OutboxDocument.id = MagicMock()  # type: ignore[assignment]
            OutboxDocument.find_one = AsyncMock(return_value=fake_doc)  # type: ignore[method-assign]
            await repo.delete(entry.entry_id)
        finally:
            if original_id is not None:
                OutboxDocument.id = original_id  # type: ignore[assignment]
            elif hasattr(OutboxDocument, "id"):
                delattr(OutboxDocument, "id")
            if hasattr(OutboxDocument, "find_one"):
                del OutboxDocument.find_one  # type: ignore[attr-defined]

        fake_doc.delete.assert_called_once()

    async def test_delete_noop_when_not_found(self) -> None:
        """delete() must be silent when the document does not exist."""
        repo = BeanieOutboxRepository()
        entry = _make_entry()

        original_id = OutboxDocument.__dict__.get("id")
        try:
            OutboxDocument.id = MagicMock()  # type: ignore[assignment]
            OutboxDocument.find_one = AsyncMock(return_value=None)  # type: ignore[method-assign]
            await repo.delete(entry.entry_id)  # must not raise
        finally:
            if original_id is not None:
                OutboxDocument.id = original_id  # type: ignore[assignment]
            elif hasattr(OutboxDocument, "id"):
                delattr(OutboxDocument, "id")
            if hasattr(OutboxDocument, "find_one"):
                del OutboxDocument.find_one  # type: ignore[attr-defined]

    async def test_delete_passes_session_to_find_one(self) -> None:
        """When a session is set, it must be forwarded to find_one()."""
        fake_session = MagicMock()
        repo = BeanieOutboxRepository(session=fake_session)
        entry = _make_entry()

        fake_doc = MagicMock()
        fake_doc.delete = AsyncMock()
        find_one_mock = AsyncMock(return_value=fake_doc)

        original_id = OutboxDocument.__dict__.get("id")
        try:
            OutboxDocument.id = MagicMock()  # type: ignore[assignment]
            OutboxDocument.find_one = find_one_mock  # type: ignore[method-assign]
            await repo.delete(entry.entry_id)
        finally:
            if original_id is not None:
                OutboxDocument.id = original_id  # type: ignore[assignment]
            elif hasattr(OutboxDocument, "id"):
                delattr(OutboxDocument, "id")
            if hasattr(OutboxDocument, "find_one"):
                del OutboxDocument.find_one  # type: ignore[attr-defined]

        # find_one must have been called with session=fake_session
        call_kwargs = find_one_mock.call_args[1]
        assert call_kwargs.get("session") is fake_session

    async def test_delete_passes_session_to_doc_delete(self) -> None:
        """When a session is set, doc.delete() must also receive session=..."""
        fake_session = MagicMock()
        repo = BeanieOutboxRepository(session=fake_session)
        entry = _make_entry()

        fake_doc = MagicMock()
        fake_doc.delete = AsyncMock()

        original_id = OutboxDocument.__dict__.get("id")
        try:
            OutboxDocument.id = MagicMock()  # type: ignore[assignment]
            OutboxDocument.find_one = AsyncMock(return_value=fake_doc)  # type: ignore[method-assign]
            await repo.delete(entry.entry_id)
        finally:
            if original_id is not None:
                OutboxDocument.id = original_id  # type: ignore[assignment]
            elif hasattr(OutboxDocument, "id"):
                delattr(OutboxDocument, "id")
            if hasattr(OutboxDocument, "find_one"):
                del OutboxDocument.find_one  # type: ignore[attr-defined]

        fake_doc.delete.assert_called_once_with(session=fake_session)
