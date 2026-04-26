"""
varco_sa.inbox
==============
SQLAlchemy async implementation of ``InboxRepository``.

This module provides two classes for the two distinct usage roles of the
inbox pattern:

``SAInboxRepository``
    For the **consumer dispatch path** — uses an existing ``AsyncSession`` so
    the ``save()`` call participates in the same DB transaction as the consumer's
    other writes.  Does NOT commit: the caller's ``AsyncUnitOfWork`` controls
    the transaction boundary.

``SAPollerInboxRepository``
    For ``InboxPoller`` — uses an ``async_sessionmaker`` to create a fresh
    ``AsyncSession`` per operation and auto-commits after each ``mark_processed()``
    call.  This is necessary because ``InboxPoller`` marks entries processed
    independently (after ``bus.publish()`` succeeds) without managing sessions
    itself.

DESIGN: two classes over a union-type constructor
    ✅ Each class has a single, clear purpose — no conditional logic inside.
    ✅ Type annotations are precise — no ``Session | Maker`` union in the public API.
    ❌ Two imports instead of one.  Acceptable — the two use cases are genuinely
       different and the distinction matters for correctness.

DESIGN: mark_processed uses optimistic WHERE processed_at IS NULL
    ✅ Safe under concurrent InboxPoller + live handler execution — only the
       first caller succeeds; the second is a silent no-op.
    ✅ No exception raised on duplicate calls — idempotent by construction.
    ❌ No feedback on whether the mark actually changed a row.  ``InboxPoller``
       does not need this; it simply continues to the next entry.

Table
-----
Both classes operate on the ``varco_inbox`` table, defined by
``InboxEntryModel`` (a standalone ``DeclarativeBase`` subclass).
Expose ``inbox_metadata`` in your Alembic ``env.py``::

    from varco_sa.inbox import inbox_metadata

    target_metadata = [Base.metadata, inbox_metadata]

    def run_migrations_offline() -> None:
        context.configure(..., target_metadata=target_metadata)

Usage — consumer dispatch path::

    from varco_sa.inbox import SAInboxRepository
    from varco_core.service.inbox import InboxEntry

    async with provider.make_uow() as uow:
        await uow.orders.save(order)
        repo = SAInboxRepository(uow.session)
        await repo.save(InboxEntry.from_event(event, channel="orders"))
    # UoW commit persists BOTH the order row AND the inbox entry atomically.

Usage — InboxPoller::

    from sqlalchemy.ext.asyncio import async_sessionmaker
    from varco_sa.inbox import SAPollerInboxRepository
    from varco_core.service.inbox import InboxPoller

    session_factory = async_sessionmaker(engine)
    poller_repo = SAPollerInboxRepository(session_factory)

    poller = InboxPoller(inbox=poller_repo, bus=my_bus)
    await poller.start()
    # ... poller runs in background, marking entries as processed
    # after successful re-publish ...
    await poller.stop()

Thread safety:  ⚠️ One ``SAInboxRepository`` per request/task — ``AsyncSession``
                    is not thread-safe.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🐍 https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
  SQLAlchemy async ORM — AsyncSession usage patterns
- 📐 https://microservices.io/patterns/reliability/transactional-outbox.html
  Transactional Outbox / Inbox pattern — original pattern reference
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import DateTime, LargeBinary, String, update as sa_update, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from varco_core.service.inbox import InboxEntry, InboxRepository

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import async_sessionmaker

_logger = logging.getLogger(__name__)

# ── ORM model ─────────────────────────────────────────────────────────────────


class _InboxBase(DeclarativeBase):
    """
    Isolated ``DeclarativeBase`` for the inbox model.

    DESIGN: separate base over reusing the app's DeclarativeBase
        ✅ varco_sa can define the inbox schema without knowing the app's Base.
        ✅ Users opt-in to Alembic management by including ``inbox_metadata``
           in their ``target_metadata`` — no silent schema pollution.
        ❌ Users must explicitly include ``inbox_metadata`` in Alembic config.
           See module docstring for the two-line setup.
    """


class InboxEntryModel(_InboxBase):
    """
    SQLAlchemy ORM row for a single pending inbox entry.

    Table name: ``varco_inbox``.

    Thread safety:  ✅ Mapped class definition is read-only after creation.
    Async safety:   ✅ Class definition; no I/O.

    Edge cases:
        - ``payload`` is ``LargeBinary`` (BYTEA on Postgres, BLOB on MySQL/SQLite).
          For payloads >1 MB, consider a streaming column type.
        - ``entry_id`` is stored as a native UUID on Postgres, and as a
          VARCHAR(36) on databases that lack a UUID type (SQLAlchemy handles
          this automatically via the ``Uuid`` mapped type).
        - ``processed_at`` is NULL until the handler completes successfully —
          it is the sentinel that ``get_unprocessed()`` filters on.
        - No ``__table_args__`` index on ``received_at`` is declared here to
          avoid assumptions about DB performance requirements.  Add one in a
          migration if the inbox table grows large.
    """

    __tablename__ = "varco_inbox"

    # Primary key — matches InboxEntry.entry_id (UUIDv4).
    entry_id: Mapped[UUID] = mapped_column(primary_key=True)

    # Human-readable event class name — for monitoring and tracing only.
    event_type: Mapped[str] = mapped_column(String(255), nullable=False)

    # Logical event channel — stored so InboxPoller can re-publish to the
    # same channel on re-delivery.
    channel: Mapped[str] = mapped_column(String(255), nullable=False)

    # Pre-serialized JSON bytes — allows re-delivery without re-parsing the
    # original event class at replay time.
    payload: Mapped[bytes] = mapped_column(LargeBinary, nullable=False)

    # UTC arrival time — used to ORDER BY received_at ASC in get_unprocessed(),
    # ensuring oldest-first (approximate FIFO) replay order.
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    # NULL until the handler completes successfully.  The IS NULL predicate
    # in get_unprocessed() and the WHERE clause in mark_processed() both
    # depend on this sentinel value.
    processed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        default=None,
    )


# Expose the metadata so users can wire it into Alembic target_metadata.
inbox_metadata = _InboxBase.metadata


# ── Helper ─────────────────────────────────────────────────────────────────────


def _model_to_entry(row: InboxEntryModel) -> InboxEntry:
    """
    Convert an ``InboxEntryModel`` ORM row back to an ``InboxEntry`` value object.

    Args:
        row: ORM row fetched from the ``varco_inbox`` table.

    Returns:
        An immutable ``InboxEntry`` with the same field values.

    Edge cases:
        - ``row.received_at`` is expected to be timezone-aware (stored as
          ``TIMESTAMPTZ``).  If the column returns a naive datetime (SQLite
          does this), it is treated as UTC without raising.
        - ``row.processed_at`` may be ``None`` — passed through as-is so the
          caller can distinguish unprocessed from processed entries.
        - A naive ``processed_at`` is also coerced to UTC — consistent with
          the ``received_at`` handling.
    """
    # Coerce naive datetimes to UTC — SQLite returns naive datetimes.
    received_at = row.received_at
    if received_at is not None and received_at.tzinfo is None:
        received_at = received_at.replace(tzinfo=timezone.utc)

    processed_at = row.processed_at
    if processed_at is not None and processed_at.tzinfo is None:
        processed_at = processed_at.replace(tzinfo=timezone.utc)

    return InboxEntry(
        entry_id=row.entry_id,
        event_type=row.event_type,
        channel=row.channel,
        payload=row.payload,
        received_at=received_at,
        processed_at=processed_at,
    )


# ── SAInboxRepository ─────────────────────────────────────────────────────────


class SAInboxRepository(InboxRepository):
    """
    SQLAlchemy implementation of ``InboxRepository`` for **consumer dispatch** use.

    Uses an injected ``AsyncSession`` — the same session opened by the caller's
    ``AsyncUnitOfWork`` — so ``save()`` participates in the same DB transaction
    as the handler's other domain writes.

    DESIGN: no auto-commit in save() / mark_processed()
        ✅ The caller's UoW controls the transaction boundary — both the handler's
           domain writes AND the inbox entry are committed (or rolled back) together.
        ✅ If the handler is rolled back, the inbox entry is never written either —
           no phantom unprocessed rows accumulate.
        ❌ ``mark_processed()`` does NOT commit, so calling it outside a UoW context
           means the update is never persisted.  Use ``SAPollerInboxRepository``
           for InboxPoller use cases where auto-commit is required.

    Thread safety:  ❌ AsyncSession is not thread-safe.  Use one per request.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        session: The active ``AsyncSession``.  Typically obtained from
                 ``uow.session`` inside an ``async with uow:`` block.

    Edge cases:
        - Calling ``save()`` after the session is closed raises ``InvalidRequestError``.
        - ``get_unprocessed()`` auto-flushes before the SELECT (SQLAlchemy default)
          — pending unsaved changes in the session are flushed automatically.
        - ``mark_processed()`` does NOT flush or commit — the caller must commit.

    Example::

        async with provider.make_uow() as uow:
            repo = SAInboxRepository(uow.session)
            await repo.save(InboxEntry.from_event(event, channel="orders"))
            await uow.orders.save(result)
        # Both the inbox entry AND the domain entity are committed atomically here.
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

    async def save(self, entry: InboxEntry) -> None:
        """
        Add ``entry`` to the session (same transaction as the handler's writes).

        Does NOT flush or commit — the caller's ``AsyncUnitOfWork`` controls when
        the transaction is committed.  The entry is persisted only when the UoW
        commits successfully.

        Args:
            entry: The ``InboxEntry`` to persist in the inbox table.

        Raises:
            sqlalchemy.exc.IntegrityError: If ``entry.entry_id`` already exists
                                           (duplicate inbox insert).

        Edge cases:
            - If the UoW is rolled back after this call, the inbox entry is
              never written — no phantom rows accumulate.
            - Two concurrent saves with the same ``entry.entry_id`` raise
              ``IntegrityError`` at commit time.  ``entry_id`` is a UUIDv4 —
              collisions are astronomically unlikely.

        Async safety: ✅ Protected by the session lock (SQLAlchemy asyncio layer).
        """
        model = InboxEntryModel(
            entry_id=entry.entry_id,
            event_type=entry.event_type,
            channel=entry.channel,
            payload=entry.payload,
            received_at=entry.received_at,
            processed_at=entry.processed_at,
        )
        # add() is synchronous — session tracks the new object in its identity map.
        # The INSERT is deferred to the next flush (or commit).
        self._session.add(model)
        _logger.debug(
            "SAInboxRepository.save: staged entry_id=%s event_type=%r channel=%r",
            entry.entry_id,
            entry.event_type,
            entry.channel,
        )

    async def mark_processed(self, entry_id: UUID) -> None:
        """
        Mark the inbox entry as processed via an optimistic UPDATE.

        Executes:
        ``UPDATE varco_inbox SET processed_at = now()
          WHERE entry_id = :id AND processed_at IS NULL``

        The ``AND processed_at IS NULL`` predicate makes this call idempotent:
        if the entry is already processed (or does not exist), the UPDATE
        touches 0 rows and returns silently — no exception is raised.

        Args:
            entry_id: The ``InboxEntry.entry_id`` to mark.

        Edge cases:
            - Unknown ``entry_id`` → silent no-op (0 rows updated).
            - Already-processed entry → silent no-op (optimistic WHERE).
            - Must NOT raise on no-match — ``InboxPoller`` relies on this.
            - Does NOT commit — the caller must commit (or use
              ``SAPollerInboxRepository`` which auto-commits).

        Async safety: ✅ Awaits ``session.execute()``.
        """
        now = datetime.now(timezone.utc)
        stmt = (
            sa_update(InboxEntryModel)
            # Optimistic locking: only update rows that are still unprocessed.
            # This ensures at-most-once marking even if InboxPoller and the
            # live handler race to mark the same entry.
            .where(
                InboxEntryModel.entry_id == entry_id,
                InboxEntryModel.processed_at.is_(None),
            ).values(processed_at=now)
            # Avoid loading the ORM object into the identity map — we only
            # need the UPDATE to execute, not the updated row's state.
            .execution_options(synchronize_session=False)
        )
        await self._session.execute(stmt)
        _logger.debug(
            "SAInboxRepository.mark_processed: executed optimistic update for entry_id=%s",
            entry_id,
        )

    async def get_unprocessed(self, *, limit: int = 100) -> list[InboxEntry]:
        """
        Return up to ``limit`` unprocessed entries ordered by ``received_at ASC``.

        Executes a SELECT WHERE ``processed_at IS NULL`` on the ``varco_inbox``
        table, ordered oldest-first.  SQLAlchemy auto-flushes pending changes
        before the SELECT (default behaviour) — this ensures entries added in
        the same session via ``save()`` are visible.

        Args:
            limit: Maximum number of entries to return.  Default ``100``.

        Returns:
            List of ``InboxEntry`` value objects, oldest-first.
            Empty list if no unprocessed entries exist.

        Edge cases:
            - If the session has uncommitted changes (e.g. a ``save()`` was
              called but not yet committed), the auto-flush makes those rows
              visible here.  This is usually the correct behaviour.
            - ``limit=0`` returns an empty list (handled by SQLAlchemy).

        Async safety: ✅ Awaits ``session.execute()``.
        """
        stmt = (
            select(InboxEntryModel)
            # Filter on NULL processed_at — the sentinel for unprocessed entries.
            .where(InboxEntryModel.processed_at.is_(None))
            # Oldest-first — FIFO replay order within a channel.
            .order_by(InboxEntryModel.received_at.asc()).limit(limit)
        )
        result = await self._session.execute(stmt)
        rows = result.scalars().all()

        _logger.debug(
            "SAInboxRepository.get_unprocessed: fetched %d entries (limit=%d)",
            len(rows),
            limit,
        )
        return [_model_to_entry(row) for row in rows]

    def __repr__(self) -> str:
        return f"SAInboxRepository(session={self._session!r})"


# ── SAPollerInboxRepository ───────────────────────────────────────────────────


class SAPollerInboxRepository(InboxRepository):
    """
    SQLAlchemy implementation of ``InboxRepository`` for **InboxPoller** use.

    Creates a fresh ``AsyncSession`` per operation and auto-commits after each
    ``mark_processed()`` call.  This is necessary because ``InboxPoller`` marks
    entries after a successful ``bus.publish()`` without managing SQLAlchemy
    sessions itself — the poller has no concept of DB transactions.

    DESIGN: session-per-operation with auto-commit over a long-lived session
        ✅ Each poll cycle reads fresh data — no stale first-level cache.
        ✅ ``mark_processed()`` commits immediately — poller is guaranteed
           persistence after each successful re-publish even if the process
           restarts.
        ✅ ``InboxPoller`` remains ignorant of SQLAlchemy — clean separation.
        ❌ Slightly higher connection-pool churn vs a shared session.
           For typical poll intervals (≥100 ms) this is negligible.

    Alternative considered: shared session with explicit flush after mark_processed
        Rejected — the session's first-level cache would accumulate stale
        ORM objects between poll cycles, and ``expire_on_commit=False`` (the
        varco_sa default) means stale rows could be returned by
        ``get_unprocessed()``.

    Thread safety:  ❌ ``async_sessionmaker`` is shared but individual sessions
                        created per call are NOT thread-safe.  Use one poller
                        per event loop.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        session_factory: ``async_sessionmaker`` that creates ``AsyncSession``
                         instances.  Typically obtained from
                         ``SQLAlchemyRepositoryProvider.session_factory`` or
                         built with ``async_sessionmaker(engine)``.

    Edge cases:
        - If the DB is unreachable, all three methods will raise the underlying
          ``sqlalchemy.exc.OperationalError`` — ``InboxPoller`` catches this
          and logs it, retrying on the next tick.
        - ``save()`` creates its own session and commits — suitable for
          testing, but in production ``save()`` should be called via
          ``SAInboxRepository`` inside a UoW so the inbox entry commits
          atomically with the handler's domain writes.

    Example::

        from sqlalchemy.ext.asyncio import async_sessionmaker
        from varco_sa.inbox import SAPollerInboxRepository
        from varco_core.service.inbox import InboxPoller

        session_factory = async_sessionmaker(engine)
        poller = InboxPoller(
            inbox=SAPollerInboxRepository(session_factory),
            bus=my_bus,
        )
        async with poller:
            ...  # poller re-publishes unprocessed entries in background
    """

    def __init__(self, session_factory: async_sessionmaker) -> None:  # type: ignore[type-arg]
        """
        Args:
            session_factory: ``async_sessionmaker`` for creating sessions.
                             Must produce sessions with the same engine as the
                             ``SAInboxRepository`` used for ``save()`` operations.

        Edge cases:
            - ``session_factory`` is NOT type-annotated with a generic parameter
              to avoid importing ``async_sessionmaker[AsyncSession]`` at runtime
              in Python < 3.12 without ``from __future__ import annotations``.
        """
        # session_factory is called once per operation — no shared session state.
        self._session_factory = session_factory

    async def save(self, entry: InboxEntry) -> None:
        """
        Insert ``entry`` into the inbox table in its own committed transaction.

        In production, prefer ``SAInboxRepository.save()`` inside a UoW so the
        inbox entry commits atomically with the handler's domain entity writes.
        This method is provided for completeness and testing.

        Args:
            entry: The ``InboxEntry`` to persist.

        Raises:
            sqlalchemy.exc.IntegrityError: On duplicate ``entry_id``.

        Async safety: ✅ Each call creates and commits its own session.
        """
        # Create a session, add the row, commit, and close.
        async with self._session_factory() as session:
            session.add(
                InboxEntryModel(
                    entry_id=entry.entry_id,
                    event_type=entry.event_type,
                    channel=entry.channel,
                    payload=entry.payload,
                    received_at=entry.received_at,
                    processed_at=entry.processed_at,
                )
            )
            await session.commit()
        _logger.debug(
            "SAPollerInboxRepository.save: committed entry_id=%s",
            entry.entry_id,
        )

    async def mark_processed(self, entry_id: UUID) -> None:
        """
        Mark the inbox entry as processed and commit immediately.

        Executes an optimistic UPDATE and commits so ``InboxPoller``'s mark
        is durably persisted even if the process restarts before the next tick.

        Optimistic WHERE clause:
        ``UPDATE varco_inbox SET processed_at = now()
          WHERE entry_id = :id AND processed_at IS NULL``

        The ``AND processed_at IS NULL`` makes this idempotent — calling it
        twice on the same ``entry_id`` is a silent no-op on the second call.

        Args:
            entry_id: The ``InboxEntry.entry_id`` to mark.

        Raises:
            sqlalchemy.exc.OperationalError: If the DB is unavailable.

        Edge cases:
            - Unknown ``entry_id`` → silent no-op (0 rows affected — not an error).
            - Already-processed entry → silent no-op (optimistic WHERE).
            - If the process crashes after ``bus.publish()`` but before this
              commit completes, the entry is re-published on the next poller tick
              (at-least-once semantics — handlers must be idempotent).

        Async safety: ✅ Each call creates, commits, and closes its own session.
        """
        now = datetime.now(timezone.utc)
        async with self._session_factory() as session:
            stmt = (
                sa_update(InboxEntryModel)
                # Optimistic locking: only update still-unprocessed rows.
                .where(
                    InboxEntryModel.entry_id == entry_id,
                    InboxEntryModel.processed_at.is_(None),
                ).values(processed_at=now)
                # No ORM state sync needed — UPDATE only; no result row returned.
                .execution_options(synchronize_session=False)
            )
            await session.execute(stmt)
            # Commit here — poller has no session context to commit later.
            await session.commit()
        _logger.debug(
            "SAPollerInboxRepository.mark_processed: committed optimistic update "
            "for entry_id=%s",
            entry_id,
        )

    async def get_unprocessed(self, *, limit: int = 100) -> list[InboxEntry]:
        """
        Return up to ``limit`` unprocessed entries, oldest-first, in a fresh session.

        Each call opens a new session so there is no stale first-level cache
        from previous ``mark_processed()`` calls.  The session is closed (not
        committed) after the SELECT.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of ``InboxEntry`` value objects, oldest-first.

        Async safety: ✅ Each call creates and closes its own session.
        """
        # Use autocommit=False (default) — SELECT requires no explicit commit.
        # Session is closed on context-manager exit.
        async with self._session_factory() as session:
            stmt = (
                select(InboxEntryModel)
                .where(InboxEntryModel.processed_at.is_(None))
                .order_by(InboxEntryModel.received_at.asc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            entries = [_model_to_entry(row) for row in rows]

        _logger.debug(
            "SAPollerInboxRepository.get_unprocessed: fetched %d entries",
            len(entries),
        )
        return entries

    def __repr__(self) -> str:
        return f"SAPollerInboxRepository(session_factory={self._session_factory!r})"


# ── Public API ────────────────────────────────────────────────────────────────


__all__ = [
    "InboxEntryModel",
    "inbox_metadata",
    "SAInboxRepository",
    "SAPollerInboxRepository",
]
