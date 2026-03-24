"""
varco_sa.outbox
===============
SQLAlchemy async implementation of ``OutboxRepository``.

This module provides two classes for the two distinct usage roles of the
transactional outbox pattern:

``SAOutboxRepository``
    For the **service layer** — uses an existing ``AsyncSession`` so the
    ``save()`` call participates in the same DB transaction as the domain
    entity write.  Does NOT commit: the caller's ``AsyncUnitOfWork`` controls
    the transaction boundary.

``SARelayOutboxRepository``
    For ``OutboxRelay`` — uses an ``async_sessionmaker`` to create a fresh
    ``AsyncSession`` per operation and auto-commits after each ``delete()``.
    This is necessary because ``OutboxRelay`` calls ``delete()`` independently
    (after ``bus.publish()`` succeeds) without managing sessions itself.

DESIGN: two classes over a union-type constructor
    ✅ Each class has a single, clear purpose — no conditional logic inside.
    ✅ Type annotations are precise — no ``Session | Maker`` union in the public API.
    ❌ Two imports instead of one.  Acceptable — the two use cases are genuinely
       different and the distinction matters for correctness.

Table
-----
Both classes operate on the ``varco_outbox`` table, defined by
``OutboxEntryModel`` (a standalone ``DeclarativeBase`` subclass).
Expose ``outbox_metadata`` in your Alembic ``env.py``::

    from varco_sa.outbox import outbox_metadata

    target_metadata = [Base.metadata, outbox_metadata]

    def run_migrations_offline() -> None:
        context.configure(..., target_metadata=target_metadata)

Usage — service layer::

    from varco_sa.outbox import SAOutboxRepository
    from varco_core.service.outbox import OutboxEntry

    async with provider.make_uow() as uow:
        await uow.orders.save(order)
        repo = SAOutboxRepository(uow.session)
        await repo.save(
            OutboxEntry.from_event(OrderCreatedEvent(order_id=order.id), channel="orders")
        )
    # UoW commit persists BOTH the order row AND the outbox entry atomically.

Usage — relay::

    from sqlalchemy.ext.asyncio import async_sessionmaker
    from varco_sa.outbox import SARelayOutboxRepository
    from varco_core.service.outbox import OutboxRelay

    session_factory = async_sessionmaker(engine)
    relay_repo = SARelayOutboxRepository(session_factory)

    relay = OutboxRelay(outbox=relay_repo, bus=my_bus)
    await relay.start()
    # ... relay runs in background, deleting rows after successful publish ...
    await relay.stop()

Thread safety:  ⚠️ One ``SAOutboxRepository`` per request/task — ``AsyncSession``
                    is not thread-safe.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🐍 https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
  SQLAlchemy async ORM — AsyncSession usage patterns
- 📐 https://microservices.io/patterns/data/transactional-outbox.html
  Transactional Outbox pattern — original pattern reference
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import DateTime, LargeBinary, String, delete as sa_delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from varco_core.service.outbox import OutboxEntry, OutboxRepository

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import async_sessionmaker

_logger = logging.getLogger(__name__)

# ── ORM model ─────────────────────────────────────────────────────────────────


class _OutboxBase(DeclarativeBase):
    """
    Isolated ``DeclarativeBase`` for the outbox model.

    DESIGN: separate base over reusing the app's DeclarativeBase
        ✅ varco_sa can define the outbox schema without knowing the app's Base.
        ✅ Users opt-in to Alembic management by including ``outbox_metadata``
           in their ``target_metadata`` — no silent schema pollution.
        ❌ Users must explicitly include ``outbox_metadata`` in Alembic config.
           See module docstring for the two-line setup.
    """


class OutboxEntryModel(_OutboxBase):
    """
    SQLAlchemy ORM row for a single pending outbox entry.

    Table name: ``varco_outbox``.

    Thread safety:  ✅ Mapped class definition is read-only after creation.
    Async safety:   ✅ Class definition; no I/O.

    Edge cases:
        - ``payload`` is ``LargeBinary`` (BYTEA on Postgres, BLOB on MySQL/SQLite).
          For payloads >1 MB, consider using a streaming column type.
        - ``entry_id`` is stored as a native UUID on Postgres, and as a
          VARCHAR(36) on databases that lack a UUID type (SQLAlchemy handles
          this automatically via ``Uuid`` — we use ``UUID`` mapped type).
        - No ``__table_args__`` index on ``created_at`` is defined here to
          avoid assumptions about DB performance requirements.  Add one in a
          migration if the outbox table grows large.
    """

    __tablename__ = "varco_outbox"

    # Primary key — matches OutboxEntry.entry_id (UUIDv4).
    entry_id: Mapped[UUID] = mapped_column(primary_key=True)

    # Human-readable event class name — for monitoring only; not used in routing.
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)

    # Logical event channel — forwarded verbatim to bus.publish(channel=...).
    channel: Mapped[str] = mapped_column(String(255), nullable=False)

    # Pre-serialized JSON bytes — the relay forwards these without re-parsing.
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    # UTC creation time — used to ORDER BY created_at ASC in get_pending(),
    # ensuring oldest-first (approximate FIFO) relay order.
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )


# Expose the metadata so users can wire it into Alembic target_metadata.
outbox_metadata = _OutboxBase.metadata


# ── Helper ─────────────────────────────────────────────────────────────────────


def _model_to_entry(row: OutboxEntryModel) -> OutboxEntry:
    """
    Convert an ``OutboxEntryModel`` ORM row back to an ``OutboxEntry`` value object.

    Args:
        row: ORM row fetched from the ``varco_outbox`` table.

    Returns:
        An immutable ``OutboxEntry`` with the same field values.

    Edge cases:
        - ``row.created_at`` is expected to be timezone-aware (stored as
          ``TIMESTAMPTZ``).  If the column returns a naive datetime (SQLite
          does this), it is treated as UTC without raising.
    """
    # Ensure created_at is always timezone-aware — SQLite returns naive datetimes.
    created_at = row.created_at
    if created_at is not None and created_at.tzinfo is None:
        # Assume UTC if no timezone info — matches how OutboxEntry stores it.
        created_at = created_at.replace(tzinfo=timezone.utc)

    return OutboxEntry(
        entry_id=row.entry_id,
        event_type=row.event_type,
        channel=row.channel,
        payload=row.payload,
        created_at=created_at,
    )


# ── SAOutboxRepository ────────────────────────────────────────────────────────


class SAOutboxRepository(OutboxRepository):
    """
    SQLAlchemy implementation of ``OutboxRepository`` for **service layer** use.

    Uses an injected ``AsyncSession`` — the same session opened by the caller's
    ``AsyncUnitOfWork`` — so ``save()`` participates in the same DB transaction
    as the domain entity write.

    DESIGN: no auto-commit in save()/delete()
        ✅ The caller's UoW controls the transaction boundary — both the domain
           entity AND the outbox entry are committed (or rolled back) together.
           This is the entire point of the transactional outbox pattern.
        ❌ delete() does NOT commit, so calling it outside a UoW context means
           the deletion is never persisted.  Use ``SARelayOutboxRepository`` for
           relay use cases where auto-commit is required.

    Thread safety:  ❌ AsyncSession is not thread-safe.  Use one per request.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        session: The active ``AsyncSession``.  Typically obtained from
                 ``uow.session`` inside an ``async with uow:`` block.

    Edge cases:
        - Calling ``save()`` after the session is closed raises ``InvalidRequestError``.
        - ``get_pending()`` flushes the session (SQLAlchemy default) before
          executing the SELECT — pending unsaved changes are flushed automatically.
        - ``delete()`` does NOT flush or commit — the caller must commit.
          This means deletes via this class are only useful in the same
          transaction as a subsequent save (rarely needed).

    Example::

        async with provider.make_uow() as uow:
            await uow.orders.save(order)
            repo = SAOutboxRepository(uow.session)
            await repo.save(
                OutboxEntry.from_event(
                    OrderCreatedEvent(order_id=order.id),
                    channel="orders",
                )
            )
        # Both the order AND the outbox entry are committed atomically here.
    """

    def __init__(self, session: AsyncSession) -> None:
        """
        Args:
            session: Active ``AsyncSession`` from the caller's UoW.

        Edge cases:
            - If ``session`` is already closed, subsequent method calls will raise.
        """
        # Store the session reference — used for all operations.
        # This class never closes or commits the session.
        self._session = session

    async def save(self, entry: OutboxEntry) -> None:
        """
        Add ``entry`` to the session (same transaction as the domain entity write).

        Does NOT flush or commit — the caller's ``AsyncUnitOfWork`` controls when
        the transaction is committed.  The entry is persisted only when the UoW
        commits successfully.

        Args:
            entry: The ``OutboxEntry`` to persist in the outbox table.

        Raises:
            sqlalchemy.exc.IntegrityError: If ``entry.entry_id`` already exists
                                           (duplicate outbox insert).

        Edge cases:
            - If the UoW is rolled back after this call, the outbox entry is
              never written — no event is lost.  This is by design.
            - Two concurrent saves with the same ``entry.entry_id`` raise
              ``IntegrityError`` at commit time.  ``entry_id`` is a UUIDv4 —
              collisions are astronomically unlikely.

        Async safety: ✅ Protected by the session lock (SQLAlchemy asyncio layer).
        """
        model = OutboxEntryModel(
            entry_id=entry.entry_id,
            event_type=entry.event_type,
            channel=entry.channel,
            payload=entry.payload,
            created_at=entry.created_at,
        )
        # add() is synchronous — session tracks the new object in its identity map.
        # The INSERT is deferred to the next flush (or commit).
        self._session.add(model)
        _logger.debug(
            "SAOutboxRepository.save: staged entry_id=%s event_type=%r channel=%r",
            entry.entry_id,
            entry.event_type,
            entry.channel,
        )

    async def get_pending(self, *, limit: int = 100) -> list[OutboxEntry]:
        """
        Return up to ``limit`` unsent entries ordered by ``created_at ASC``.

        Executes a SELECT on the ``varco_outbox`` table using the current session.
        SQLAlchemy auto-flushes pending changes before the SELECT (default behaviour)
        — this ensures entries added in the same session via ``save()`` are visible.

        Args:
            limit: Maximum number of entries to return.  Default ``100``.

        Returns:
            List of ``OutboxEntry`` value objects, oldest-first.
            Empty list if no pending entries exist.

        Edge cases:
            - If the session has uncommitted changes (e.g. a ``save()`` was called
              but not yet committed), the auto-flush makes those rows visible here.
              This is usually the correct behaviour but can be surprising.
            - ``limit=0`` returns an empty list (handled by SQLAlchemy).

        Async safety: ✅ Awaits ``session.execute()``.
        """
        stmt = (
            select(OutboxEntryModel)
            # Oldest first — FIFO delivery order within a channel.
            .order_by(OutboxEntryModel.created_at.asc()).limit(limit)
        )
        result = await self._session.execute(stmt)
        rows = result.scalars().all()

        _logger.debug(
            "SAOutboxRepository.get_pending: fetched %d entries (limit=%d)",
            len(rows),
            limit,
        )
        return [_model_to_entry(row) for row in rows]

    async def delete(self, entry_id: UUID) -> None:
        """
        Delete the outbox entry identified by ``entry_id``.

        Does NOT commit.  When used with ``SAOutboxRepository`` (service mode),
        the caller must commit.  For relay use, see ``SARelayOutboxRepository``.

        Args:
            entry_id: The ``OutboxEntry.entry_id`` to delete.

        Edge cases:
            - Deleting a non-existent ``entry_id`` is a silent no-op.
            - Delete is not committed until the session commits.

        Async safety: ✅ Awaits ``session.execute()``.
        """
        stmt = sa_delete(OutboxEntryModel).where(OutboxEntryModel.entry_id == entry_id)
        await self._session.execute(stmt)
        _logger.debug(
            "SAOutboxRepository.delete: deleted entry_id=%s (uncommitted)",
            entry_id,
        )

    def __repr__(self) -> str:
        return f"SAOutboxRepository(session={self._session!r})"


# ── SARelayOutboxRepository ───────────────────────────────────────────────────


class SARelayOutboxRepository(OutboxRepository):
    """
    SQLAlchemy implementation of ``OutboxRepository`` for **OutboxRelay** use.

    Creates a fresh ``AsyncSession`` per operation and auto-commits after each
    ``delete()`` call.  This is necessary because ``OutboxRelay`` calls
    ``delete()`` after a successful ``bus.publish()`` without managing
    SQLAlchemy sessions itself — the relay has no concept of DB transactions.

    DESIGN: session-per-operation with auto-commit over a long-lived session
        ✅ Each poll cycle reads fresh data — no stale first-level cache.
        ✅ ``delete()`` commits immediately — relay is guaranteed persistence
           after each successful publish even if the process restarts.
        ✅ ``OutboxRelay`` remains ignorant of SQLAlchemy — clean separation.
        ❌ Slightly higher connection-pool churn vs a shared session.
           For typical poll intervals (≥100 ms) this is negligible.

    Alternative considered: shared session with explicit flush after delete
        Rejected — the session's first-level cache would accumulate deleted
        ORM objects between poll cycles, and ``expire_on_commit=False`` (the
        varco_sa default) means stale rows could be returned by ``get_pending()``.

    Thread safety:  ❌ ``async_sessionmaker`` is shared but individual sessions
                        created per call are NOT thread-safe.  Use one relay
                        per event loop.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        session_factory: ``async_sessionmaker`` that creates ``AsyncSession``
                         instances.  Typically obtained from
                         ``SQLAlchemyRepositoryProvider.session_factory`` or
                         built with ``async_sessionmaker(engine)``.

    Edge cases:
        - If the DB is unreachable, all three methods will raise the underlying
          ``sqlalchemy.exc.OperationalError`` — ``OutboxRelay`` catches this
          and logs it, retrying on the next tick.
        - ``save()`` creates its own session and commits — suitable for
          testing, but in production ``save()`` should be called via
          ``SAOutboxRepository`` inside a UoW.

    Example::

        from sqlalchemy.ext.asyncio import async_sessionmaker
        from varco_sa.outbox import SARelayOutboxRepository
        from varco_core.service.outbox import OutboxRelay

        session_factory = async_sessionmaker(engine)
        relay = OutboxRelay(
            outbox=SARelayOutboxRepository(session_factory),
            bus=my_bus,
        )
        async with relay:
            ...  # relay polls in background
    """

    def __init__(self, session_factory: async_sessionmaker) -> None:  # type: ignore[type-arg]
        """
        Args:
            session_factory: ``async_sessionmaker`` for creating sessions.
                             Must produce sessions with the same engine as the
                             ``SAOutboxRepository`` used for ``save()`` operations.

        Edge cases:
            - ``session_factory`` is NOT type-annotated with a generic parameter
              to avoid importing ``async_sessionmaker[AsyncSession]`` at runtime
              in Python < 3.12 without ``from __future__ import annotations``.
        """
        # session_factory is called once per operation — no shared session state.
        self._session_factory = session_factory

    async def save(self, entry: OutboxEntry) -> None:
        """
        Insert ``entry`` into the outbox table in its own committed transaction.

        In production, prefer ``SAOutboxRepository.save()`` inside a UoW so the
        outbox entry is committed atomically with the domain entity.  This method
        is provided for completeness and testing.

        Args:
            entry: The ``OutboxEntry`` to persist.

        Raises:
            sqlalchemy.exc.IntegrityError: On duplicate ``entry_id``.

        Async safety: ✅ Each call creates and commits its own session.
        """
        # Create a session, add the row, commit, and close.
        async with self._session_factory() as session:
            session.add(
                OutboxEntryModel(
                    entry_id=entry.entry_id,
                    event_type=entry.event_type,
                    channel=entry.channel,
                    payload=entry.payload,
                    created_at=entry.created_at,
                )
            )
            await session.commit()
        _logger.debug(
            "SARelayOutboxRepository.save: committed entry_id=%s",
            entry.entry_id,
        )

    async def get_pending(self, *, limit: int = 100) -> list[OutboxEntry]:
        """
        Return up to ``limit`` unsent entries, oldest-first, in a fresh session.

        Each call opens a new session so there is no stale first-level cache
        from previous ``delete()`` calls.  The session is closed (not committed)
        after the SELECT.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of ``OutboxEntry`` value objects, oldest-first.

        Async safety: ✅ Each call creates and closes its own session.
        """
        # Use autocommit=False (default) — SELECT requires no explicit commit.
        # Session is closed on context-manager exit.
        async with self._session_factory() as session:
            stmt = (
                select(OutboxEntryModel)
                .order_by(OutboxEntryModel.created_at.asc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            entries = [_model_to_entry(row) for row in rows]

        _logger.debug(
            "SARelayOutboxRepository.get_pending: fetched %d entries",
            len(entries),
        )
        return entries

    async def delete(self, entry_id: UUID) -> None:
        """
        Delete the row for ``entry_id`` and commit immediately.

        Auto-commits because ``OutboxRelay`` calls this after a successful
        ``bus.publish()`` and does not manage DB transactions itself.  Failing
        to commit here means the entry accumulates indefinitely.

        Args:
            entry_id: The ``OutboxEntry.entry_id`` to delete.

        Raises:
            sqlalchemy.exc.OperationalError: If the DB is unavailable.

        Edge cases:
            - Deleting a non-existent ``entry_id`` is a silent no-op (DELETE
              WHERE pk=... affects 0 rows — not an error).
            - If the process crashes after ``bus.publish()`` but before this
              commit completes, the entry is re-published on the next relay tick
              (at-least-once semantics — event handlers must be idempotent).

        Async safety: ✅ Each call creates, commits, and closes its own session.
        """
        async with self._session_factory() as session:
            stmt = sa_delete(OutboxEntryModel).where(
                OutboxEntryModel.entry_id == entry_id
            )
            await session.execute(stmt)
            # Commit here — relay has no session context to commit later.
            await session.commit()
        _logger.debug(
            "SARelayOutboxRepository.delete: committed deletion of entry_id=%s",
            entry_id,
        )

    def __repr__(self) -> str:
        return f"SARelayOutboxRepository(session_factory={self._session_factory!r})"


# ── Public API ────────────────────────────────────────────────────────────────


__all__ = [
    "OutboxEntryModel",
    "outbox_metadata",
    "SAOutboxRepository",
    "SARelayOutboxRepository",
]
