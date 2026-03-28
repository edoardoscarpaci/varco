"""
varco_beanie.outbox
===================
Beanie (pymongo / MongoDB) implementation of ``OutboxRepository``.

MongoDB differs from relational databases in how "same transaction" is handled:

- **Single-node MongoDB**: Multi-document transactions are NOT supported.
  The outbox entry and the domain entity are in separate documents — they
  cannot be committed atomically.  Use this for eventual-consistency scenarios
  where a small window of inconsistency between entity write and outbox entry
  is acceptable.

- **Replica set / sharded cluster (``transactional=True``)**: Multi-document
  transactions ARE supported.  Pass the same ``AsyncClientSession`` (with an
  active transaction) to both the entity repository and ``BeanieOutboxRepository``
  so both documents are written in the same transaction.

DESIGN: session-optional approach
    ✅ Works on single-node MongoDB (no session required).
    ✅ Works with transactions on replica sets — pass the session explicitly.
    ✅ ``OutboxRelay`` can use ``BeanieOutboxRepository(session=None)`` since
       ``get_pending()`` and ``delete()`` are independent operations that don't
       need transactional isolation from the domain layer.
    ❌ On single-node MongoDB without transactions, there is a small window
       where the entity is saved but the outbox entry is not yet written (or
       vice versa).  This is an inherent limitation of single-node MongoDB.

Collection
----------
``OutboxDocument`` maps to the ``varco_outbox`` MongoDB collection.
It must be included in your ``init_beanie()`` (or ``provider.register()``)
call::

    from varco_beanie.outbox import OutboxDocument

    # Option A — pass to init_beanie directly
    await init_beanie(database=db, document_models=[..., OutboxDocument])

    # Option B — register with BeanieRepositoryProvider before provider.init()
    provider.register(OutboxDocument)       # note: register accepts Document too
    await provider.init()

Usage — service layer (replica set, transactional=True)::

    from varco_beanie.outbox import BeanieOutboxRepository
    from varco_core.service.outbox import OutboxEntry

    async with provider.make_uow() as uow:
        await uow.orders.save(order)
        repo = BeanieOutboxRepository(session=uow.session)
        await repo.save(
            OutboxEntry.from_event(OrderCreatedEvent(order_id=order.id), channel="orders")
        )
    # UoW commit persists BOTH order doc AND outbox doc in the same transaction.

Usage — service layer (single node, no transactions)::

    async with provider.make_uow() as uow:
        await uow.orders.save(order)

    # Separate insert — not in the same transaction (single-node MongoDB limitation)
    repo = BeanieOutboxRepository(session=None)
    await repo.save(OutboxEntry.from_event(OrderCreatedEvent(...), channel="orders"))

Usage — relay::

    from varco_beanie.outbox import BeanieOutboxRepository
    from varco_core.service.outbox import OutboxRelay

    relay = OutboxRelay(
        outbox=BeanieOutboxRepository(session=None),
        bus=my_bus,
    )
    async with relay:
        ...  # relay polls in background

Thread safety:  ⚠️ ``AsyncClientSession`` is NOT thread-safe.  Use one
                    ``BeanieOutboxRepository`` per request/task when passing a
                    session.  Session-less (``session=None``) instances are safe.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🔍 https://beanie-odm.dev/tutorial/defining-a-document/
  Beanie Document — class definition and collection configuration
- 🔍 https://www.mongodb.com/docs/manual/core/transactions/
  MongoDB transactions — replica set requirement
- 🐍 https://pymongo.readthedocs.io/en/stable/api/pymongo/client_session.html
  AsyncClientSession — pymongo transaction API
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Sequence
from uuid import UUID, uuid4

from beanie import Document
from pydantic import Field

from varco_core.service.outbox import OutboxEntry, OutboxRepository

if TYPE_CHECKING:
    from pymongo.asynchronous.client_session import AsyncClientSession

_logger = logging.getLogger(__name__)


# ── OutboxDocument ────────────────────────────────────────────────────────────


class OutboxDocument(Document):
    """
    Beanie document representing a single pending outbox entry.

    Maps to the ``varco_outbox`` MongoDB collection.

    Register this document in your ``init_beanie()`` or
    ``BeanieRepositoryProvider.register()`` call before using
    ``BeanieOutboxRepository``.

    DESIGN: Beanie Document over raw pymongo collection operations
        ✅ Consistent with the rest of the varco_beanie layer.
        ✅ Beanie handles ObjectId ↔ UUID mapping automatically via
           ``model_config``.
        ✅ Built-in async methods (``insert``, ``find``, ``delete``) — no raw
           BSON marshalling needed.
        ❌ Requires ``init_beanie()`` at startup — cannot be used without it.

    Thread safety:  ✅ Document class is a static definition — no mutable state.
    Async safety:   ✅ All Beanie methods are ``async def``.

    Attributes:
        id:          UUIDv4 — matches ``OutboxEntry.entry_id``.  Overrides
                     Beanie's default ``ObjectId`` primary key.
        event_type:  Human-readable event class name — for monitoring.
        channel:     Target event channel — forwarded to ``bus.publish()``.
        payload:     Pre-serialized JSON bytes from ``JsonEventSerializer``.
        created_at:  UTC timestamp — used for FIFO relay ordering.

    Edge cases:
        - ``payload`` is stored as ``bytes`` — Beanie/pymongo stores this as
          Binary (BSON subtype 0).  Round-trip is lossless.
        - ``id`` (entry_id) is a UUID — pymongo stores it as a Binary(UUID subtype 4)
          or as a string depending on codec options.  Beanie's default codec
          stores UUID as ``Binary(subtype=3)`` for BSON compliance.
        - No MongoDB index on ``created_at`` is created by default.  Add one
          via ``Settings.indexes`` if the collection grows large.
    """

    # Override Beanie's ObjectId pk with a UUID so it matches OutboxEntry.entry_id.
    id: UUID = Field(default_factory=uuid4)

    event_type: str
    """Human-readable event class name — stored for monitoring, not routing."""

    channel: str
    """Logical event channel forwarded to bus.publish(channel=...)."""

    payload: bytes
    """Pre-serialized JSON bytes from JsonEventSerializer."""

    created_at: datetime = Field(
        # Always UTC — avoids naive datetime ambiguity.
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    """UTC timestamp of when this entry was created."""

    class Settings:
        """Beanie collection and index configuration."""

        # Collection name in MongoDB — all varco outbox entries go here.
        name = "varco_outbox"

        # DESIGN: no index on created_at declared here — callers can add one
        # via a separate migration or Beanie's index management if the
        # collection grows large.  Avoid surprising schema side-effects.
        indexes: list = []

    def __repr__(self) -> str:
        return (
            f"OutboxDocument("
            f"id={self.id}, "
            f"event_type={self.event_type!r}, "
            f"channel={self.channel!r})"
        )


# ── BeanieOutboxRepository ────────────────────────────────────────────────────


class BeanieOutboxRepository(OutboxRepository):
    """
    Beanie implementation of ``OutboxRepository``.

    Persists outbox entries in the ``varco_outbox`` MongoDB collection via
    ``OutboxDocument``.  Optionally accepts an ``AsyncClientSession`` so
    operations participate in the caller's MongoDB transaction (replica set only).

    Pass ``session=None`` for single-node MongoDB or when using with
    ``OutboxRelay`` (relay operations are independent, not transactional).

    Thread safety:  ⚠️ If ``session`` is provided, the same constraints as
                        ``AsyncClientSession`` apply — one per request/task.
                        ``session=None`` instances are safe to share.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        session: Optional pymongo ``AsyncClientSession``.  Pass the session
                 from ``BeanieUnitOfWork`` when running inside a replica set
                 transaction.  ``None`` for non-transactional use (relay,
                 single-node MongoDB).

    Edge cases:
        - ``OutboxDocument`` must be registered with Beanie (via
          ``init_beanie()`` or ``provider.register(OutboxDocument)``) before
          any method is called.  ``RuntimeError`` is raised by Beanie if not.
        - ``get_pending()`` uses ``sort(+created_at).limit(n)`` — this is an
          in-memory sort if no index exists.  Add ``{ created_at: 1 }`` to
          ``OutboxDocument.Settings.indexes`` for production workloads.
        - MongoDB ``find()`` does not lock documents — the relay may return
          the same entries on concurrent ``get_pending()`` calls.  Event
          handlers must be idempotent.

    Example::

        # Relay use (no transactions needed)
        repo = BeanieOutboxRepository(session=None)
        relay = OutboxRelay(outbox=repo, bus=my_bus)
        await relay.start()
    """

    def __init__(
        self,
        *,
        session: AsyncClientSession | None = None,
    ) -> None:
        """
        Args:
            session: Optional ``AsyncClientSession`` for transaction support.
                     Pass ``None`` for relay use or single-node MongoDB.
        """
        # Stored as optional — passed to every Beanie operation that accepts it.
        # None means operations run outside any explicit MongoDB transaction.
        self._session = session

    async def save(self, entry: OutboxEntry) -> None:
        """
        Insert ``entry`` into the ``varco_outbox`` collection.

        When ``session`` is set, the insert joins the caller's transaction
        (replica set only).  Without a session, the insert is immediate.

        Args:
            entry: The ``OutboxEntry`` to persist.

        Raises:
            beanie.exceptions.DocumentWasNotSaved: If Beanie fails to insert.
            RuntimeError: If ``OutboxDocument`` was not registered with Beanie.

        Edge cases:
            - If the session's transaction is aborted after this call, the
              document is never committed — the entry is cleanly rolled back.
            - Without a transaction (single-node), the insert is immediately
              visible — there is no rollback if the domain entity write fails
              in a subsequent step.

        Async safety: ✅ Awaits ``document.insert()``.
        """
        doc = OutboxDocument(
            id=entry.entry_id,
            event_type=entry.event_type,
            channel=entry.channel,
            payload=entry.payload,
            created_at=entry.created_at,
        )
        # Pass session if available — Beanie's insert() accepts an optional
        # session keyword arg that routes the operation through the transaction.
        if self._session is not None:
            await doc.insert(session=self._session)
        else:
            await doc.insert()

        _logger.debug(
            "BeanieOutboxRepository.save: inserted entry_id=%s event_type=%r",
            entry.entry_id,
            entry.event_type,
        )

    async def get_pending(self, *, limit: int = 100) -> list[OutboxEntry]:
        """
        Return up to ``limit`` unsent entries, sorted oldest-first.

        Uses ``OutboxDocument.find().sort(+created_at).limit(n)`` — an
        in-memory sort unless a ``{ created_at: 1 }`` index exists.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of ``OutboxEntry`` value objects, oldest-first.
            Empty list if the collection has no documents.

        Edge cases:
            - Sort without an index is O(N) — acceptable for small collections
              (<10k entries).  Create an index in production.
            - Concurrent ``get_pending()`` calls may return overlapping results —
              handlers must be idempotent.

        Async safety: ✅ Awaits Beanie ``find()`` cursor.
        """
        # sort(+OutboxDocument.created_at) = ascending order (oldest first)
        # session kwarg is accepted by Beanie's find() for transaction isolation
        find_kwargs: dict = {}
        if self._session is not None:
            find_kwargs["session"] = self._session

        docs = (
            await OutboxDocument.find(
                **find_kwargs,
            )
            .sort(+OutboxDocument.created_at)
            .limit(limit)
            .to_list()
        )

        entries = [
            OutboxEntry(
                entry_id=doc.id,
                event_type=doc.event_type,
                channel=doc.channel,
                payload=doc.payload,
                created_at=(
                    doc.created_at
                    if doc.created_at.tzinfo is not None
                    # Coerce naive datetimes to UTC (MongoDB can return naive
                    # datetimes depending on codec configuration).
                    else doc.created_at.replace(tzinfo=timezone.utc)
                ),
            )
            for doc in docs
        ]

        _logger.debug(
            "BeanieOutboxRepository.get_pending: fetched %d entries (limit=%d)",
            len(entries),
            limit,
        )
        return entries

    async def delete(self, entry_id: UUID) -> None:
        """
        Delete the outbox document for ``entry_id``.

        Uses ``OutboxDocument.find_one(id == entry_id).delete()`` — idempotent:
        if the document does not exist, the operation is a no-op.

        Args:
            entry_id: The ``OutboxEntry.entry_id`` to delete.

        Edge cases:
            - If ``entry_id`` is not found, no exception is raised — at-least-once
              delivery means a concurrent relay instance may have deleted it first.
            - When ``session`` is set, the delete joins the transaction.

        Async safety: ✅ Awaits Beanie ``delete()`` call.
        """
        delete_kwargs: dict = {}
        if self._session is not None:
            delete_kwargs["session"] = self._session

        # find_one() returns None if not found — check before calling delete()
        # to avoid a NoneType error.
        doc = await OutboxDocument.find_one(
            OutboxDocument.id == entry_id,
            **delete_kwargs,
        )
        if doc is not None:
            await doc.delete(**delete_kwargs)
            _logger.debug(
                "BeanieOutboxRepository.delete: deleted entry_id=%s", entry_id
            )
        else:
            # Not found — no-op.  Another relay instance may have deleted it.
            _logger.debug(
                "BeanieOutboxRepository.delete: entry_id=%s not found (already deleted?)",
                entry_id,
            )

    async def save_many(self, entries: Sequence[OutboxEntry]) -> None:
        """
        Bulk INSERT multiple outbox entries using a single ``insert_many()`` call.

        Overrides the default loop in ``OutboxRepository.save_many()`` to use
        Beanie's ``insert_many()``, which issues a single Motor ``insertMany``
        command.  One DB round-trip for the whole batch.

        DESIGN: insert_many() over N individual insert() calls
          ✅ Single Motor round-trip — constant latency regardless of batch size.
          ✅ If a session is provided all inserts join the same transaction.
          ❌ All inserts succeed or all fail — no partial batch recovery.

        Args:
            entries: Sequence of ``OutboxEntry`` objects to persist.

        Edge cases:
            - Empty sequence → no-op.
            - On replica sets with a session, the inserts join the caller's
              transaction and are atomic with the domain entity write.
            - Without a session, each document is immediately visible on insert
              (no rollback if a subsequent domain write fails).

        Async safety: ✅ Awaits ``insert_many()`` once.
        """
        if not entries:
            return

        docs = [
            OutboxDocument(
                id=entry.entry_id,
                event_type=entry.event_type,
                channel=entry.channel,
                payload=entry.payload,
                created_at=entry.created_at,
            )
            for entry in entries
        ]

        insert_kwargs: dict = {}
        if self._session is not None:
            insert_kwargs["session"] = self._session

        # insert_many() sends a single insertMany Motor command.
        await OutboxDocument.insert_many(docs, **insert_kwargs)
        _logger.debug(
            "BeanieOutboxRepository.save_many: inserted %d entries",
            len(docs),
        )

    def __repr__(self) -> str:
        return (
            f"BeanieOutboxRepository(" f"session={'set' if self._session else 'None'})"
        )


# ── Public API ────────────────────────────────────────────────────────────────


__all__ = [
    "OutboxDocument",
    "BeanieOutboxRepository",
]
