"""
varco_beanie.inbox
==================
Beanie (pymongo / MongoDB) implementation of ``InboxRepository``.

Like ``varco_beanie.outbox``, this implementation is session-optional so it
works on both single-node MongoDB and replica sets with multi-document
transactions.

DESIGN: single class (no poller-specific subclass)
    Unlike the SQLAlchemy implementation which needs a separate
    ``SAPollerInboxRepository`` to manage session lifecycles, Beanie
    operations are inherently independent.  Each method issues its own
    Motor command — there is no concept of a shared session that accumulates
    state.  ``BeanieInboxRepository`` works equally well in the consumer
    dispatch path and in ``InboxPoller``.
    ✅ Simpler API — one class, no "poller vs service" distinction.
    ✅ ``session=None`` (relay use) is the common case — no extra import needed.
    ❌ For replica set transactions, callers must pass a session themselves.
       There is no "poller session factory" equivalent.

DESIGN: mark_processed uses find_one + update(Set(...)) with processed_at IS None
    ✅ Optimistic — only updates rows where processed_at is already None.
    ✅ Idempotent — second call finds processed_at != None and updates nothing.
    ✅ Safe under concurrent InboxPoller + live handler execution.
    ❌ Two round-trips (find_one + update) vs a single atomic findAndModify.
       For typical inbox workloads this overhead is negligible.

Collection
----------
``InboxDocument`` maps to the ``varco_inbox`` MongoDB collection.
It must be included in your ``init_beanie()`` (or ``provider.register()``)
call::

    from varco_beanie.inbox import InboxDocument

    # Option A — pass to init_beanie directly
    await init_beanie(database=db, document_models=[..., InboxDocument])

    # Option B — register with BeanieRepositoryProvider before provider.init()
    provider.register(InboxDocument)       # note: register accepts Document too
    await provider.init()

Usage — consumer dispatch path (replica set, transactional=True)::

    from varco_beanie.inbox import BeanieInboxRepository
    from varco_core.service.inbox import InboxEntry

    async with provider.make_uow() as uow:
        repo = BeanieInboxRepository(session=uow.session)
        await repo.save(InboxEntry.from_event(event, channel="orders"))
        await uow.orders.save(result)
    # UoW commit persists BOTH the order doc AND the inbox doc in one transaction.

Usage — InboxPoller (no transactions needed)::

    from varco_beanie.inbox import BeanieInboxRepository
    from varco_core.service.inbox import InboxPoller

    poller = InboxPoller(
        inbox=BeanieInboxRepository(session=None),
        bus=my_bus,
    )
    async with poller:
        ...  # poller re-publishes unprocessed entries in background

Thread safety:  ⚠️ ``AsyncClientSession`` is NOT thread-safe.  Use one
                    ``BeanieInboxRepository`` per request/task when passing a
                    session.  Session-less (``session=None``) instances are safe.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🔍 https://beanie-odm.dev/tutorial/defining-a-document/
  Beanie Document — class definition and collection configuration
- 🔍 https://beanie-odm.dev/tutorial/update/
  Beanie update operators — Set, Inc, etc.
- 🔍 https://www.mongodb.com/docs/manual/core/transactions/
  MongoDB transactions — replica set requirement
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from beanie import Document
from beanie.operators import Set
from pydantic import Field

from varco_core.service.inbox import InboxEntry, InboxRepository

if TYPE_CHECKING:
    from pymongo.asynchronous.client_session import AsyncClientSession

_logger = logging.getLogger(__name__)


# ── InboxDocument ─────────────────────────────────────────────────────────────


class InboxDocument(Document):
    """
    Beanie document representing a single pending inbox entry.

    Maps to the ``varco_inbox`` MongoDB collection.

    Register this document in your ``init_beanie()`` or
    ``BeanieRepositoryProvider.register()`` call before using
    ``BeanieInboxRepository``.

    DESIGN: Beanie Document over raw pymongo collection operations
        ✅ Consistent with the rest of the varco_beanie layer.
        ✅ Built-in async methods (``insert``, ``find``, ``find_one``) — no
           raw BSON marshalling needed.
        ✅ Beanie handles ObjectId ↔ UUID mapping via the model config.
        ❌ Requires ``init_beanie()`` at startup — cannot be used without it.

    Thread safety:  ✅ Document class is a static definition — no mutable state.
    Async safety:   ✅ All Beanie methods are ``async def``.

    Attributes:
        id:           UUIDv4 — matches ``InboxEntry.entry_id``.  Overrides
                      Beanie's default ``ObjectId`` primary key.
        event_type:   Human-readable event class name — for monitoring.
        channel:      Bus channel the event arrived on — stored so
                      ``InboxPoller`` can re-publish to the same channel.
        payload:      Pre-serialized JSON bytes from ``JsonEventSerializer``.
        received_at:  UTC timestamp of when this entry was first received.
        processed_at: ``None`` until the handler completes successfully.

    Edge cases:
        - ``payload`` is stored as ``bytes`` — Beanie/pymongo stores this as
          Binary (BSON subtype 0).  Round-trip is lossless.
        - ``id`` (entry_id) is a UUID — stored as Binary(subtype=3) for BSON
          compliance (Beanie 2.x default codec).
        - ``processed_at=None`` is the sentinel for unprocessed entries —
          ``get_unprocessed()`` queries on this field.
        - No MongoDB index on ``received_at`` is created by default.  Add one
          via ``Settings.indexes`` if the collection grows large.
    """

    # Override Beanie's ObjectId pk with UUID so it matches InboxEntry.entry_id.
    id: UUID = Field(default_factory=uuid4)

    event_type: str
    """Human-readable event class name — stored for monitoring, not routing."""

    channel: str
    """Bus channel the event arrived on — stored for re-delivery by InboxPoller."""

    payload: bytes
    """Pre-serialized JSON bytes from JsonEventSerializer."""

    received_at: datetime = Field(
        # Always UTC — avoids naive datetime ambiguity.
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    """UTC timestamp of when this entry was first received."""

    # None until the handler completes — sentinel for get_unprocessed() queries.
    processed_at: datetime | None = None

    class Settings:
        """Beanie collection and index configuration."""

        # Collection name in MongoDB — all varco inbox entries go here.
        name = "varco_inbox"

        # DESIGN: no index on received_at declared here — callers can add one
        # via a separate migration or Beanie's index management if the
        # collection grows large.  Avoid surprising schema side-effects.
        indexes: list = []

    def __repr__(self) -> str:
        return (
            f"InboxDocument("
            f"id={self.id}, "
            f"event_type={self.event_type!r}, "
            f"channel={self.channel!r}, "
            f"processed_at={self.processed_at!r})"
        )


# ── BeanieInboxRepository ─────────────────────────────────────────────────────


class BeanieInboxRepository(InboxRepository):
    """
    Beanie implementation of ``InboxRepository``.

    Persists inbox entries in the ``varco_inbox`` MongoDB collection via
    ``InboxDocument``.  Optionally accepts an ``AsyncClientSession`` so
    operations participate in the caller's MongoDB transaction (replica set only).

    Pass ``session=None`` for single-node MongoDB or when using with
    ``InboxPoller`` (poller operations are independent, not transactional).

    Thread safety:  ⚠️ If ``session`` is provided, the same constraints as
                        ``AsyncClientSession`` apply — one per request/task.
                        ``session=None`` instances are safe to share.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        session: Optional pymongo ``AsyncClientSession``.  Pass the session
                 from ``BeanieUnitOfWork`` when running inside a replica set
                 transaction.  ``None`` for non-transactional use (poller,
                 single-node MongoDB).

    Edge cases:
        - ``InboxDocument`` must be registered with Beanie (via
          ``init_beanie()`` or ``provider.register(InboxDocument)``) before
          any method is called.  ``RuntimeError`` is raised by Beanie if not.
        - ``get_unprocessed()`` uses ``find(processed_at=None).sort(+received_at).limit(n)``
          — this is an in-memory sort if no index exists.  Add
          ``{ received_at: 1 }`` to ``InboxDocument.Settings.indexes`` for
          production workloads.
        - MongoDB ``find()`` does not lock documents — the poller may return
          the same entries on concurrent ``get_unprocessed()`` calls.
          ``mark_processed()`` is optimistic and idempotent.

    Example::

        # Poller use (no transactions needed)
        repo = BeanieInboxRepository(session=None)
        poller = InboxPoller(inbox=repo, bus=my_bus)
        await poller.start()
    """

    def __init__(
        self,
        *,
        session: AsyncClientSession | None = None,
    ) -> None:
        """
        Args:
            session: Optional ``AsyncClientSession`` for transaction support.
                     Pass ``None`` for poller use or single-node MongoDB.
        """
        # Stored as optional — passed to every Beanie operation that accepts it.
        # None means operations run outside any explicit MongoDB transaction.
        self._session = session

    async def save(self, entry: InboxEntry) -> None:
        """
        Insert ``entry`` into the ``varco_inbox`` collection.

        When ``session`` is set, the insert joins the caller's transaction
        (replica set only).  Without a session, the insert is immediate.

        Args:
            entry: The ``InboxEntry`` to persist.

        Raises:
            beanie.exceptions.DocumentWasNotSaved: If Beanie fails to insert.
            RuntimeError: If ``InboxDocument`` was not registered with Beanie.

        Edge cases:
            - If the session's transaction is aborted after this call, the
              document is never committed — the entry is cleanly rolled back.
            - Without a transaction (single-node), the insert is immediately
              visible — there is no rollback if the handler fails after this
              point.  ``InboxPoller`` will replay the entry.

        Async safety: ✅ Awaits ``document.insert()``.
        """
        doc = InboxDocument(
            id=entry.entry_id,
            event_type=entry.event_type,
            channel=entry.channel,
            payload=entry.payload,
            received_at=entry.received_at,
            processed_at=entry.processed_at,
        )
        # Pass session if available — Beanie's insert() accepts an optional
        # session keyword arg that routes the operation through the transaction.
        if self._session is not None:
            await doc.insert(session=self._session)
        else:
            await doc.insert()

        _logger.debug(
            "BeanieInboxRepository.save: inserted entry_id=%s event_type=%r",
            entry.entry_id,
            entry.event_type,
        )

    async def mark_processed(self, entry_id: UUID) -> None:
        """
        Mark the inbox entry as processed using an optimistic update.

        Fetches the document and only updates it when ``processed_at`` is
        currently ``None``.  This makes the call idempotent — a second call
        on the same ``entry_id`` will find ``processed_at != None`` and skip
        the update silently.

        DESIGN: find_one check + conditional update vs atomic findAndModify
            ✅ Two separate Motor calls is simpler to reason about and test.
            ✅ Idempotent and safe under concurrent access — only the caller
               that sees ``processed_at=None`` proceeds with the update.
            ❌ Two round-trips instead of a single atomic operation.  For
               typical inbox workloads this overhead is negligible.

        Args:
            entry_id: The ``InboxEntry.entry_id`` to mark.

        Edge cases:
            - Unknown ``entry_id`` → ``find_one`` returns ``None`` → silent no-op.
            - Already-processed entry → ``processed_at`` is not ``None`` →
              update is skipped silently.
            - Must NOT raise — callers rely on the silent no-op contract.

        Async safety: ✅ Awaits ``find_one()`` and ``update()``.
        """
        find_kwargs: dict = {}
        if self._session is not None:
            find_kwargs["session"] = self._session

        # Fetch the document — returns None if not found.
        doc = await InboxDocument.find_one(
            InboxDocument.id == entry_id,
            **find_kwargs,
        )

        if doc is None:
            # Not found — no-op.  Another caller may have deleted or never
            # saved this entry.
            _logger.debug(
                "BeanieInboxRepository.mark_processed: entry_id=%s not found — no-op",
                entry_id,
            )
            return

        if doc.processed_at is not None:
            # Already processed — optimistic no-op.  Concurrent InboxPoller
            # or live handler already claimed this entry.
            _logger.debug(
                "BeanieInboxRepository.mark_processed: entry_id=%s already processed at %s — no-op",
                entry_id,
                doc.processed_at,
            )
            return

        # Document exists and is unprocessed — apply the update.
        now = datetime.now(timezone.utc)
        update_kwargs: dict = {}
        if self._session is not None:
            update_kwargs["session"] = self._session

        await doc.update(Set({InboxDocument.processed_at: now}), **update_kwargs)
        _logger.debug(
            "BeanieInboxRepository.mark_processed: marked entry_id=%s as processed",
            entry_id,
        )

    async def get_unprocessed(self, *, limit: int = 100) -> list[InboxEntry]:
        """
        Return up to ``limit`` unprocessed entries, sorted oldest-first.

        Queries on ``processed_at == None`` and sorts by ``received_at`` ascending.
        This is an in-memory sort unless a ``{ received_at: 1 }`` index exists.

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of ``InboxEntry`` value objects, oldest-first.
            Empty list if the collection has no unprocessed documents.

        Edge cases:
            - Sort without an index is O(N) — acceptable for small collections
              (<10k entries).  Create an index in production.
            - Concurrent ``get_unprocessed()`` calls may return overlapping
              results — ``mark_processed()`` is idempotent to handle races.

        Async safety: ✅ Awaits Beanie ``find()`` cursor.
        """
        find_kwargs: dict = {}
        if self._session is not None:
            find_kwargs["session"] = self._session

        # Filter on processed_at == None (unprocessed entries only).
        # Sort ascending by received_at for oldest-first FIFO replay order.
        docs = (
            await InboxDocument.find(
                InboxDocument.processed_at == None,  # noqa: E711
                **find_kwargs,
            )
            .sort(+InboxDocument.received_at)
            .limit(limit)
            .to_list()
        )

        entries = [
            InboxEntry(
                entry_id=doc.id,
                event_type=doc.event_type,
                channel=doc.channel,
                payload=doc.payload,
                received_at=(
                    doc.received_at
                    if doc.received_at.tzinfo is not None
                    # Coerce naive datetimes to UTC — MongoDB can return naive
                    # datetimes depending on codec configuration.
                    else doc.received_at.replace(tzinfo=timezone.utc)
                ),
                processed_at=doc.processed_at,
            )
            for doc in docs
        ]

        _logger.debug(
            "BeanieInboxRepository.get_unprocessed: fetched %d entries (limit=%d)",
            len(entries),
            limit,
        )
        return entries

    def __repr__(self) -> str:
        return f"BeanieInboxRepository(session={'set' if self._session else 'None'})"


# ── Public API ────────────────────────────────────────────────────────────────


__all__ = [
    "InboxDocument",
    "BeanieInboxRepository",
]
