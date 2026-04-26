"""
Unit tests for varco_sa.inbox
================================
Covers ``SAInboxRepository``, ``SAPollerInboxRepository``, and
``InboxEntryModel`` using an in-memory SQLite database (aiosqlite).

No external PostgreSQL instance is required — the inbox table is created
in a fresh SQLite database for each test.

DESIGN: why a separate engine from the conftest ``session`` fixture
    ``InboxEntryModel`` uses ``_InboxBase`` — its own isolated
    ``DeclarativeBase`` that is separate from the per-test ``_FreshBase``
    used in the rest of the varco_sa suite.  We cannot simply add its table
    to the conftest's ``base.metadata`` because the two bases are distinct.
    Instead, each test builds its own engine, creates the inbox schema, and
    tears it down on exit.

Sections
--------
- ``InboxEntryModel``         — table name, columns, inbox_metadata exported
- ``SAInboxRepository``       — save, get_unprocessed, mark_processed (optimistic)
- ``SAPollerInboxRepository`` — save (auto-commit), get_unprocessed, mark_processed
- Full cycle: service saves → poller marks processed
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from varco_core.event import Event
from varco_core.service.inbox import InboxEntry
from varco_sa.inbox import (
    InboxEntryModel,
    SAInboxRepository,
    SAPollerInboxRepository,
    inbox_metadata,
)


# ── Minimal event for tests ────────────────────────────────────────────────────


class OrderPlacedEvent(Event):
    __event_type__ = "test.order.placed.sa_inbox"
    order_id: str = "ord-1"


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def engine():
    """
    In-memory SQLite engine with the inbox schema created.

    Uses ``inbox_metadata`` from ``varco_sa.inbox`` — the ``_InboxBase``
    metadata that is separate from the app's DeclarativeBase.
    """
    eng = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(inbox_metadata.create_all)
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(inbox_metadata.drop_all)
    await eng.dispose()


@pytest_asyncio.fixture
async def session(engine) -> AsyncSession:
    """Fresh ``AsyncSession`` per test (not auto-committed)."""
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        yield s


@pytest_asyncio.fixture
async def poller_factory(engine) -> async_sessionmaker:
    """``async_sessionmaker`` for ``SAPollerInboxRepository``."""
    return async_sessionmaker(engine, expire_on_commit=False)


def _make_entry(channel: str = "orders") -> InboxEntry:
    return InboxEntry.from_event(OrderPlacedEvent(), channel=channel)


# ── InboxEntryModel ───────────────────────────────────────────────────────────


class TestInboxEntryModel:
    def test_table_name(self) -> None:
        assert InboxEntryModel.__tablename__ == "varco_inbox"

    def test_inbox_metadata_exported(self) -> None:
        # inbox_metadata is exported from the module for Alembic wiring.
        assert inbox_metadata is not None
        # The varco_inbox table must be in this metadata.
        assert "varco_inbox" in inbox_metadata.tables

    def test_model_has_entry_id_column(self) -> None:
        assert hasattr(InboxEntryModel, "entry_id")

    def test_model_has_event_type_column(self) -> None:
        assert hasattr(InboxEntryModel, "event_type")

    def test_model_has_channel_column(self) -> None:
        assert hasattr(InboxEntryModel, "channel")

    def test_model_has_payload_column(self) -> None:
        assert hasattr(InboxEntryModel, "payload")

    def test_model_has_received_at_column(self) -> None:
        assert hasattr(InboxEntryModel, "received_at")

    def test_model_has_processed_at_column(self) -> None:
        assert hasattr(InboxEntryModel, "processed_at")


# ── SAInboxRepository ─────────────────────────────────────────────────────────


class TestSAInboxRepositorySave:
    async def test_save_and_get_unprocessed(self, session: AsyncSession) -> None:
        """save() adds the entry to the session; visible after flush via get_unprocessed."""
        repo = SAInboxRepository(session)
        entry = _make_entry()
        await repo.save(entry)

        # Flush so the INSERT runs, then verify via get_unprocessed.
        await session.flush()
        unprocessed = await repo.get_unprocessed()
        assert len(unprocessed) == 1
        assert unprocessed[0].entry_id == entry.entry_id

    async def test_save_does_not_auto_commit(
        self, session: AsyncSession, engine
    ) -> None:
        """save() must not commit — the UoW controls the transaction boundary."""
        repo = SAInboxRepository(session)
        entry = _make_entry()
        await repo.save(entry)

        # Do NOT commit the session — read via a separate session to verify
        # the entry is NOT yet visible.
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as other_session:
            other_repo = SAInboxRepository(other_session)
            unprocessed = await other_repo.get_unprocessed()
        # Not committed yet — must be empty in the other session.
        assert len(unprocessed) == 0

    async def test_repr(self, session: AsyncSession) -> None:
        repo = SAInboxRepository(session)
        assert "SAInboxRepository" in repr(repo)


class TestSAInboxRepositoryMarkProcessed:
    async def test_mark_processed_removes_from_unprocessed(
        self, session: AsyncSession
    ) -> None:
        """After mark_processed(), get_unprocessed() must return an empty list."""
        repo = SAInboxRepository(session)
        entry = _make_entry()
        await repo.save(entry)
        await session.flush()

        await repo.mark_processed(entry.entry_id)
        await session.flush()

        unprocessed = await repo.get_unprocessed()
        assert len(unprocessed) == 0

    async def test_mark_processed_is_idempotent(self, session: AsyncSession) -> None:
        """Calling mark_processed() twice on the same entry must not raise."""
        repo = SAInboxRepository(session)
        entry = _make_entry()
        await repo.save(entry)
        await session.flush()

        # First call — marks the row.
        await repo.mark_processed(entry.entry_id)
        await session.flush()

        # Second call — must be a silent no-op (0 rows updated).
        await repo.mark_processed(entry.entry_id)  # must not raise
        await session.flush()

        # Still no unprocessed entries after idempotent second call.
        unprocessed = await repo.get_unprocessed()
        assert len(unprocessed) == 0

    async def test_mark_processed_unknown_id_is_noop(
        self, session: AsyncSession
    ) -> None:
        """mark_processed() with an unknown UUID must be a silent no-op."""
        repo = SAInboxRepository(session)
        await repo.mark_processed(uuid4())  # must not raise


class TestSAInboxRepositoryGetUnprocessed:
    async def test_get_unprocessed_respects_limit(self, session: AsyncSession) -> None:
        """get_unprocessed(limit=2) must return at most 2 entries even if 5 exist."""
        repo = SAInboxRepository(session)
        for _ in range(5):
            await repo.save(_make_entry())
        await session.flush()

        unprocessed = await repo.get_unprocessed(limit=2)
        assert len(unprocessed) == 2

    async def test_get_unprocessed_oldest_first(self, session: AsyncSession) -> None:
        """Entries must be returned in received_at ascending order."""
        repo = SAInboxRepository(session)
        e1 = _make_entry("ch1")
        # Small delay to ensure distinct received_at values.
        await asyncio.sleep(0.01)
        e2 = _make_entry("ch2")
        await repo.save(e1)
        await repo.save(e2)
        await session.flush()

        unprocessed = await repo.get_unprocessed()
        assert len(unprocessed) == 2
        # Oldest entry (e1) must be first.
        assert unprocessed[0].entry_id == e1.entry_id
        assert unprocessed[1].entry_id == e2.entry_id

    async def test_get_unprocessed_empty(self, session: AsyncSession) -> None:
        repo = SAInboxRepository(session)
        assert await repo.get_unprocessed() == []

    async def test_get_unprocessed_returns_inbox_entry_objects(
        self, session: AsyncSession
    ) -> None:
        """get_unprocessed() must return InboxEntry value objects with correct fields."""
        repo = SAInboxRepository(session)
        entry = _make_entry()
        await repo.save(entry)
        await session.flush()

        unprocessed = await repo.get_unprocessed()
        assert isinstance(unprocessed[0], InboxEntry)
        assert unprocessed[0].channel == "orders"
        assert unprocessed[0].event_type == "test.order.placed.sa_inbox"
        # processed_at must be None for unprocessed entries.
        assert unprocessed[0].processed_at is None


# ── SAPollerInboxRepository ───────────────────────────────────────────────────


class TestSAPollerInboxRepositorySave:
    async def test_poller_repo_save_and_get(
        self, poller_factory: async_sessionmaker
    ) -> None:
        """SAPollerInboxRepository.save() must commit so the entry is visible."""
        repo = SAPollerInboxRepository(poller_factory)
        entry = _make_entry()
        await repo.save(entry)

        unprocessed = await repo.get_unprocessed()
        assert len(unprocessed) == 1
        assert unprocessed[0].entry_id == entry.entry_id

    async def test_poller_repo_save_commits_immediately(
        self, poller_factory: async_sessionmaker, engine
    ) -> None:
        """Entry saved via SAPollerInboxRepository is immediately visible in other sessions."""
        repo = SAPollerInboxRepository(poller_factory)
        entry = _make_entry()
        await repo.save(entry)

        # Read via a separate session to confirm the commit.
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as s:
            other_repo = SAInboxRepository(s)
            unprocessed = await other_repo.get_unprocessed()
        assert len(unprocessed) == 1
        assert unprocessed[0].entry_id == entry.entry_id

    async def test_repr(self, poller_factory: async_sessionmaker) -> None:
        repo = SAPollerInboxRepository(poller_factory)
        assert "SAPollerInboxRepository" in repr(repo)


class TestSAPollerInboxRepositoryMarkProcessed:
    async def test_poller_repo_mark_processed(
        self, poller_factory: async_sessionmaker
    ) -> None:
        """SAPollerInboxRepository.mark_processed() must commit correctly."""
        repo = SAPollerInboxRepository(poller_factory)
        entry = _make_entry()
        await repo.save(entry)

        # Verify entry is unprocessed before marking.
        unprocessed_before = await repo.get_unprocessed()
        assert len(unprocessed_before) == 1

        await repo.mark_processed(entry.entry_id)

        # After marking, get_unprocessed must return empty.
        unprocessed_after = await repo.get_unprocessed()
        assert len(unprocessed_after) == 0

    async def test_poller_repo_mark_processed_idempotent(
        self, poller_factory: async_sessionmaker
    ) -> None:
        """Calling mark_processed() twice on the poller repo must not raise."""
        repo = SAPollerInboxRepository(poller_factory)
        entry = _make_entry()
        await repo.save(entry)

        await repo.mark_processed(entry.entry_id)
        await repo.mark_processed(entry.entry_id)  # must not raise

        assert await repo.get_unprocessed() == []

    async def test_poller_repo_mark_processed_unknown_id(
        self, poller_factory: async_sessionmaker
    ) -> None:
        """mark_processed() with an unknown UUID must be a silent no-op."""
        repo = SAPollerInboxRepository(poller_factory)
        await repo.mark_processed(uuid4())  # must not raise


class TestSAPollerInboxRepositoryGetUnprocessed:
    async def test_poller_repo_get_unprocessed_respects_limit(
        self, poller_factory: async_sessionmaker
    ) -> None:
        repo = SAPollerInboxRepository(poller_factory)
        for _ in range(5):
            await repo.save(_make_entry())
        unprocessed = await repo.get_unprocessed(limit=2)
        assert len(unprocessed) == 2

    async def test_poller_repo_get_unprocessed_oldest_first(
        self, poller_factory: async_sessionmaker
    ) -> None:
        repo = SAPollerInboxRepository(poller_factory)
        e1 = _make_entry("ch1")
        await asyncio.sleep(0.01)
        e2 = _make_entry("ch2")
        await repo.save(e1)
        await repo.save(e2)

        unprocessed = await repo.get_unprocessed()
        assert unprocessed[0].entry_id == e1.entry_id
        assert unprocessed[1].entry_id == e2.entry_id

    async def test_poller_repo_get_unprocessed_empty(
        self, poller_factory: async_sessionmaker
    ) -> None:
        repo = SAPollerInboxRepository(poller_factory)
        assert await repo.get_unprocessed() == []


# ── Full cycle ────────────────────────────────────────────────────────────────


class TestSAInboxFullCycle:
    async def test_service_saves_poller_marks_processed(
        self,
        session: AsyncSession,
        poller_factory: async_sessionmaker,
    ) -> None:
        """Service saves entry in-transaction; poller reads and marks it processed."""
        # 1. Consumer dispatch path saves via SAInboxRepository (not yet committed).
        service_repo = SAInboxRepository(session)
        entry = _make_entry()
        await service_repo.save(entry)
        await session.commit()  # commit the transaction

        # 2. Poller reads via SAPollerInboxRepository (fresh session).
        poller_repo = SAPollerInboxRepository(poller_factory)
        unprocessed = await poller_repo.get_unprocessed()
        assert len(unprocessed) == 1

        # 3. Poller marks processed after successful re-publish.
        await poller_repo.mark_processed(unprocessed[0].entry_id)

        # 4. No more unprocessed entries.
        assert await poller_repo.get_unprocessed() == []

    async def test_mark_processed_only_affects_unprocessed_rows(
        self,
        session: AsyncSession,
    ) -> None:
        """mark_processed() must leave other unprocessed entries intact."""
        repo = SAInboxRepository(session)
        e1 = _make_entry("ch1")
        e2 = _make_entry("ch2")
        await repo.save(e1)
        await repo.save(e2)
        await session.flush()

        # Mark only e1 as processed.
        await repo.mark_processed(e1.entry_id)
        await session.flush()

        # e2 must still be in the unprocessed list.
        unprocessed = await repo.get_unprocessed()
        assert len(unprocessed) == 1
        assert unprocessed[0].entry_id == e2.entry_id
