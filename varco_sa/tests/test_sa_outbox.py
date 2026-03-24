"""
Unit tests for varco_sa.outbox
================================
Covers ``SAOutboxRepository``, ``SARelayOutboxRepository``, and
``OutboxEntryModel`` using an in-memory SQLite database (aiosqlite).

No external PostgreSQL instance is required — the outbox table is created
in a fresh SQLite database for each test.

DESIGN: why a separate engine from the conftest ``session`` fixture
    ``OutboxEntryModel`` uses ``_OutboxBase`` — its own isolated
    ``DeclarativeBase`` that is separate from the per-test ``_FreshBase``
    used in the rest of the varco_sa suite.  We cannot simply add its table
    to the conftest's ``base.metadata`` because the two bases are distinct.
    Instead, each test builds its own engine, creates the outbox schema, and
    tears it down on exit.

Sections
--------
- ``OutboxEntryModel``         — table name, columns, outbox_metadata exported
- ``SAOutboxRepository``       — save (staged), get_pending (ordering), delete (noop)
- ``SARelayOutboxRepository``  — save (auto-commit), get_pending (fresh session), delete (commit)
- repr of both repositories
"""

from __future__ import annotations


import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from varco_core.event import Event
from varco_core.service.outbox import OutboxEntry
from varco_sa.outbox import (
    OutboxEntryModel,
    SAOutboxRepository,
    SARelayOutboxRepository,
    outbox_metadata,
)


# ── Minimal event for tests ────────────────────────────────────────────────────


class OrderPlacedEvent(Event):
    __event_type__ = "test.order.placed.sa_outbox"
    order_id: str = "ord-1"


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def engine():
    """
    In-memory SQLite engine with the outbox schema created.

    Uses ``outbox_metadata`` from ``varco_sa.outbox`` — the ``_OutboxBase``
    metadata that is separate from the app's DeclarativeBase.
    """
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(outbox_metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(outbox_metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture
async def session(engine) -> AsyncSession:
    """Fresh ``AsyncSession`` per test (not auto-committed)."""
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as s:
        yield s


@pytest_asyncio.fixture
async def relay_factory(engine) -> async_sessionmaker:
    """``async_sessionmaker`` for ``SARelayOutboxRepository``."""
    return async_sessionmaker(engine, expire_on_commit=False)


def _make_entry(channel: str = "orders") -> OutboxEntry:
    return OutboxEntry.from_event(OrderPlacedEvent(), channel=channel)


# ── OutboxEntryModel ──────────────────────────────────────────────────────────


class TestOutboxEntryModel:
    def test_table_name(self) -> None:
        assert OutboxEntryModel.__tablename__ == "varco_outbox"

    def test_outbox_metadata_exported(self) -> None:
        # outbox_metadata is exported from the module for Alembic wiring.
        assert outbox_metadata is not None
        # The varco_outbox table must be in this metadata.
        assert "varco_outbox" in outbox_metadata.tables

    def test_model_has_entry_id_column(self) -> None:
        assert hasattr(OutboxEntryModel, "entry_id")

    def test_model_has_payload_column(self) -> None:
        assert hasattr(OutboxEntryModel, "payload")

    def test_model_has_channel_column(self) -> None:
        assert hasattr(OutboxEntryModel, "channel")

    def test_model_has_event_type_column(self) -> None:
        assert hasattr(OutboxEntryModel, "event_type")

    def test_model_has_created_at_column(self) -> None:
        assert hasattr(OutboxEntryModel, "created_at")


# ── SAOutboxRepository ────────────────────────────────────────────────────────


class TestSAOutboxRepositorySave:
    async def test_save_stages_entry_in_session(self, session: AsyncSession) -> None:
        """save() adds the entry to the session; entry is visible after flush."""
        repo = SAOutboxRepository(session)
        entry = _make_entry()
        await repo.save(entry)

        # Flush so the INSERT runs, then verify via get_pending.
        await session.flush()
        pending = await repo.get_pending()
        assert len(pending) == 1
        assert pending[0].entry_id == entry.entry_id

    async def test_save_does_not_auto_commit(
        self, session: AsyncSession, engine
    ) -> None:
        """save() must not commit — the UoW controls the transaction boundary."""
        repo = SAOutboxRepository(session)
        entry = _make_entry()
        await repo.save(entry)

        # Do NOT commit the session — read via a separate session to verify
        # the entry is NOT yet visible.
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as other_session:
            other_repo = SAOutboxRepository(other_session)
            pending = await other_repo.get_pending()
        # Not committed yet — must be empty in the other session.
        assert len(pending) == 0

    async def test_save_repr(self, session: AsyncSession) -> None:
        repo = SAOutboxRepository(session)
        assert "SAOutboxRepository" in repr(repo)


class TestSAOutboxRepositoryGetPending:
    async def test_get_pending_returns_oldest_first(
        self, session: AsyncSession
    ) -> None:
        """get_pending() must return entries ordered by created_at ASC (oldest first)."""
        import asyncio

        repo = SAOutboxRepository(session)
        e1 = _make_entry("ch1")
        await asyncio.sleep(0.01)  # small delay to ensure distinct created_at
        e2 = _make_entry("ch2")
        await repo.save(e1)
        await repo.save(e2)
        await session.flush()

        pending = await repo.get_pending()
        assert len(pending) == 2
        # Oldest (e1) must be first.
        assert pending[0].entry_id == e1.entry_id
        assert pending[1].entry_id == e2.entry_id

    async def test_get_pending_respects_limit(self, session: AsyncSession) -> None:
        repo = SAOutboxRepository(session)
        for _ in range(5):
            await repo.save(_make_entry())
        await session.flush()

        pending = await repo.get_pending(limit=3)
        assert len(pending) == 3

    async def test_get_pending_empty(self, session: AsyncSession) -> None:
        repo = SAOutboxRepository(session)
        pending = await repo.get_pending()
        assert pending == []

    async def test_get_pending_returns_outbox_entry_objects(
        self, session: AsyncSession
    ) -> None:
        repo = SAOutboxRepository(session)
        entry = _make_entry()
        await repo.save(entry)
        await session.flush()

        pending = await repo.get_pending()
        assert isinstance(pending[0], OutboxEntry)
        assert pending[0].channel == "orders"
        assert pending[0].event_type == "test.order.placed.sa_outbox"


class TestSAOutboxRepositoryDelete:
    async def test_delete_removes_entry(self, session: AsyncSession) -> None:
        repo = SAOutboxRepository(session)
        entry = _make_entry()
        await repo.save(entry)
        await session.flush()

        await repo.delete(entry.entry_id)
        await session.flush()

        pending = await repo.get_pending()
        assert len(pending) == 0

    async def test_delete_nonexistent_is_noop(self, session: AsyncSession) -> None:
        from uuid import uuid4

        repo = SAOutboxRepository(session)
        await repo.delete(uuid4())  # must not raise


# ── SARelayOutboxRepository ───────────────────────────────────────────────────


class TestSARelayOutboxRepositorySave:
    async def test_save_commits_immediately(
        self, relay_factory: async_sessionmaker, engine
    ) -> None:
        """SARelayOutboxRepository.save() must commit so the entry is immediately visible."""
        repo = SARelayOutboxRepository(relay_factory)
        entry = _make_entry()
        await repo.save(entry)

        # Read in a separate session to confirm the commit.
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as s:
            other_repo = SAOutboxRepository(s)
            pending = await other_repo.get_pending()
        assert len(pending) == 1
        assert pending[0].entry_id == entry.entry_id

    async def test_repr(self, relay_factory: async_sessionmaker) -> None:
        repo = SARelayOutboxRepository(relay_factory)
        assert "SARelayOutboxRepository" in repr(repo)


class TestSARelayOutboxRepositoryGetPending:
    async def test_get_pending_fresh_session_sees_committed_rows(
        self, relay_factory: async_sessionmaker
    ) -> None:
        """get_pending() must open a fresh session to avoid stale first-level cache."""
        repo = SARelayOutboxRepository(relay_factory)
        entry = _make_entry()
        await repo.save(entry)  # commits

        pending = await repo.get_pending()
        assert len(pending) == 1
        assert pending[0].entry_id == entry.entry_id

    async def test_get_pending_empty(self, relay_factory: async_sessionmaker) -> None:
        repo = SARelayOutboxRepository(relay_factory)
        assert await repo.get_pending() == []

    async def test_get_pending_ordered_oldest_first(
        self, relay_factory: async_sessionmaker
    ) -> None:
        import asyncio

        repo = SARelayOutboxRepository(relay_factory)
        e1 = _make_entry("ch1")
        await asyncio.sleep(0.01)
        e2 = _make_entry("ch2")

        await repo.save(e1)
        await repo.save(e2)

        pending = await repo.get_pending()
        assert pending[0].entry_id == e1.entry_id
        assert pending[1].entry_id == e2.entry_id


class TestSARelayOutboxRepositoryDelete:
    async def test_delete_commits_and_removes_entry(
        self, relay_factory: async_sessionmaker, engine
    ) -> None:
        repo = SARelayOutboxRepository(relay_factory)
        entry = _make_entry()
        await repo.save(entry)
        await repo.delete(entry.entry_id)

        # Verify deletion is persisted.
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as s:
            other_repo = SAOutboxRepository(s)
            pending = await other_repo.get_pending()
        assert len(pending) == 0

    async def test_delete_nonexistent_is_noop(
        self, relay_factory: async_sessionmaker
    ) -> None:
        from uuid import uuid4

        repo = SARelayOutboxRepository(relay_factory)
        await repo.delete(uuid4())  # must not raise


# ── Integration-like: full relay cycle ────────────────────────────────────────


class TestSAOutboxFullCycle:
    async def test_save_then_relay_delete(
        self, session: AsyncSession, relay_factory: async_sessionmaker, engine
    ) -> None:
        """Service saves entry in-transaction; relay reads and deletes it."""
        # 1. Service saves via SAOutboxRepository (not yet committed).
        service_repo = SAOutboxRepository(session)
        entry = _make_entry()
        await service_repo.save(entry)
        await session.commit()  # commit transaction

        # 2. Relay reads via SARelayOutboxRepository (fresh session).
        relay_repo = SARelayOutboxRepository(relay_factory)
        pending = await relay_repo.get_pending()
        assert len(pending) == 1

        # 3. Relay deletes after successful publish.
        await relay_repo.delete(pending[0].entry_id)

        # 4. No more pending entries.
        assert await relay_repo.get_pending() == []
