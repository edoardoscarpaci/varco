"""
varco_beanie.dlq
==================
Beanie (pymongo / MongoDB) implementation of ``AbstractDeadLetterQueue``
(Plan 009, Phase 5 — R7).

``BeanieDeadLetterQueue`` mirrors ``SADeadLetterQueue``'s semantics exactly:
``pop_batch()`` is a non-destructive read, ``ack()`` deletes. There is no
visibility window (SA has none either) — the single-relay assumption is
documented, not silently assumed (RD-2).

Collection
----------
``varco_dead_letters`` — matches the SA table name
(``varco_sa/dlq.py:varco_dead_letters``) and the ``varco_audit_log``
precedent. Register ``DeadLetterDocument`` with ``init_beanie()`` before use.

DESIGN: no TTL index by default (RD-2)
    ✅ Dead letters are never silently deleted — the exact failure mode
       ("nobody notices it died") this release exists to fix, made worse
       here because the delete would happen without an operator ever
       *seeing* the entry.
    ❌ Operators must run an explicit retention sweep
       (``delete_where()`` / ``varco retention prune``).
    ``ttl_seconds=`` is an opt-in escape hatch — logs one WARNING at
    construction naming the data-loss implication.

DESIGN: indexes declared, not built, here (Plan 006 precedent)
    ✅ ``Settings.indexes`` documents the intended shape; ``varco migrate
       index --create`` is the only builder — an index build inside the
       request path or lifespan stalls a rolling deploy.
    ❌ One extra pre-deploy step, same as every other framework Beanie index.

DESIGN: ``entry_id`` as Mongo ``_id``
    ✅ ``get()``/``ack()`` are ``_id`` lookups — O(1) and index-free.
    ✅ A duplicate ``push()`` of the same ``entry_id`` is a
       ``DuplicateKeyError`` treated as "already stored" — idempotent on
       redelivery, a property SA only gets on Postgres (``ON CONFLICT``).
    ❌ Requires a UUID-``_id`` codec — Beanie/pymongo handle it natively.

Thread safety:  ⚠️ Beanie ``Document`` classes are process-global state (bound
                   to whatever database ``init_beanie()`` last pointed at).
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

import inspect
import logging
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Annotated, Any, ClassVar
from uuid import UUID, uuid4

from beanie import Document
from providify import Fallback, InjectMeta, Singleton
from pydantic import Field
from pymongo.errors import DuplicateKeyError
from varco_core.di import BACKEND_PRIORITY
from varco_core.event.base import Event
from varco_core.event.dlq import (
    AbstractDeadLetterQueue,
    DeadLetterEntry,
    DeadLetterSource,
)
from varco_core.event.serializer import JsonEventSerializer
from varco_core.serialization import Serializer

from ._bson_time import ceil_to_bson_millisecond

_logger = logging.getLogger(__name__)


# ── DeadLetterDocument ────────────────────────────────────────────────────────


class DeadLetterDocument(Document):
    """
    Beanie document for one persisted dead letter.

    ``id`` (Mongo ``_id``) IS ``DeadLetterEntry.entry_id`` — see the class
    DESIGN block above for why.
    """

    id: UUID = Field(default_factory=uuid4)  # type: ignore[assignment]

    source: str = DeadLetterSource.CONSUMER.value
    source_ref: str | None = None
    channel: str
    handler_name: str
    event_type: str | None = None
    payload: bytes | None = None
    error_type: str
    error_message: str
    attempts: int
    first_failed_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    last_failed_at: datetime = Field(default_factory=lambda: datetime.now(tz=UTC))
    tenant_id: str | None = None

    class Settings:
        name = "varco_dead_letters"  # RD-2 — matches the SA table name.
        # DECLARED, not built — varco migrate index --create is the builder
        # (Plan 006's index_mode="check" default precedent).
        indexes: ClassVar[list[Any]] = [
            [("channel", 1), ("last_failed_at", -1)],
            [("source", 1), ("last_failed_at", -1)],
            [("tenant_id", 1), ("last_failed_at", -1)],
        ]

    def __repr__(self) -> str:
        return f"DeadLetterDocument(id={self.id}, channel={self.channel!r})"


def _ttl_index_entry(seconds: int) -> Any:
    """Build a partial-TTL index spec on ``last_failed_at`` — opt-in only."""
    return [("last_failed_at", 1)], {"expireAfterSeconds": seconds}


# ── BeanieDeadLetterQueue ─────────────────────────────────────────────────────


@Fallback
@Singleton(priority=BACKEND_PRIORITY, qualifier="beanie")
class BeanieDeadLetterQueue(AbstractDeadLetterQueue):
    """
    Beanie-backed ``AbstractDeadLetterQueue`` — mirrors ``SADeadLetterQueue``.

    Args:
        ttl_seconds: Opt-in TTL (in seconds) after which Mongo auto-deletes
            entries. ``None`` (default) — no TTL, matching RD-2. Passing a
            value logs one WARNING at construction naming the data-loss
            implication; the index itself must still be built via
            ``varco migrate index --create`` (declaring it here does not
            build it).
        serializer: Serializer for the ``Event`` stored in the ``payload``
            field.  Optionally injected from the container under
            ``Serializer[Event]``, so an app that binds
            ``CloudEventsJsonSerializer`` gets CloudEvents envelopes in its dead
            letters too.  Defaults to ``JsonEventSerializer()``.

    Edge cases:
        - ``push()`` on a ``DuplicateKeyError`` is treated as success
          (idempotent redelivery) and logged at DEBUG — never raises.
        - ``push()`` swallows every other exception too (the ABC contract)
          and logs at ERROR.
        - Register ``DeadLetterDocument`` with ``init_beanie()`` before use.

    Thread safety:  ⚠️ Shares Beanie's process-global Document binding.
    Async safety:   ✅ All methods are ``async def``.
    """

    supports_random_access = True

    def __init__(
        self,
        *,
        ttl_seconds: int | None = None,
        serializer: Annotated[Serializer[Event] | None, InjectMeta(optional=True)] = None,
    ) -> None:
        # DESIGN: the nested Event's serializer is injected, not hard-coded
        #   A dead letter stores the event as bytes, and those bytes must be
        #   readable by whoever redrives them.  Constructing `JsonEventSerializer()`
        #   here meant a CloudEvents-configured app wrote CloudEvents envelopes onto
        #   the bus but plain varco JSON into the DLQ — two wire formats for the
        #   same event, and a redrive that republished the wrong one.
        #   ✅ The DLQ now round-trips in whatever format the bus speaks.
        #   ✅ `None` keeps the previous behaviour exactly.
        #   ❌ A DLQ populated before a serializer swap holds the old format; drain
        #      its backlog before swapping (see the feature doc's migration timeline).
        self._serializer: Serializer[Event] = serializer or JsonEventSerializer()
        self._ttl_seconds = ttl_seconds
        if ttl_seconds is not None:
            _logger.warning(
                "BeanieDeadLetterQueue(ttl_seconds=%d): dead letters will be "
                "auto-deleted by Mongo's TTL monitor — this is a DATA LOSS "
                "risk (RD-2): the entry disappears without an operator ever "
                "seeing it. Prefer explicit retention (delete_where() / "
                "`varco retention prune`). The TTL index must still be built "
                "via `varco migrate index --create`.",
                ttl_seconds,
            )

    async def push(self, entry: DeadLetterEntry) -> None:
        """Persist ``entry``. Never raises — logs and swallows every failure."""
        try:
            if entry.event is not None:
                payload = self._serializer.serialize(entry.event)
                event_type = type(entry.event).__name__
            else:
                payload = entry.payload  # type: ignore[assignment]
                event_type = None

            doc = DeadLetterDocument(
                id=entry.entry_id,
                source=entry.source.value,
                source_ref=entry.source_ref,
                channel=entry.channel,
                handler_name=entry.handler_name,
                event_type=event_type,
                payload=payload,
                error_type=entry.error_type,
                error_message=entry.error_message,
                attempts=entry.attempts,
                first_failed_at=entry.first_failed_at,
                last_failed_at=entry.last_failed_at,
                tenant_id=entry.tenant_id,
            )
            await doc.insert()
            _logger.debug("BeanieDeadLetterQueue.push: stored entry_id=%s", entry.entry_id)
        except DuplicateKeyError:
            # Idempotent redelivery — same entry_id already stored. Success,
            # not an error (RD-2's "one definition per model name" cousin —
            # here it's "one row per entry_id").
            _logger.debug(
                "BeanieDeadLetterQueue.push: entry_id=%s already stored (idempotent).",
                entry.entry_id,
            )
        except Exception as exc:
            _logger.error(
                "BeanieDeadLetterQueue.push() failed unexpectedly — entry dropped "
                "(entry_id=%s): %s",
                entry.entry_id,
                exc,
                exc_info=True,
            )

    async def pop_batch(self, *, limit: int = 10) -> list[DeadLetterEntry]:
        """Non-destructive read of the oldest ``limit`` entries."""
        if limit < 1:
            raise ValueError(f"pop_batch limit must be ≥ 1, got {limit}.")
        docs = await DeadLetterDocument.find_all().sort("+first_failed_at").limit(limit).to_list()
        return [self._doc_to_entry(d) for d in docs]

    async def ack(self, entry_id: UUID) -> None:
        """Delete the entry. Idempotent — unknown id is a no-op."""
        doc = await DeadLetterDocument.get(entry_id)
        if doc is not None:
            await doc.delete()

    async def count(self) -> int:
        """Exact ``count_documents`` — O(n) on an unindexed filter."""
        return await DeadLetterDocument.find_all().count()

    async def get(self, entry_id: UUID) -> DeadLetterEntry | None:
        doc = await DeadLetterDocument.get(entry_id)
        return self._doc_to_entry(doc) if doc is not None else None

    async def list_entries(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        channel: str | None = None,
        source: DeadLetterSource | None = None,
        tenant_id: str | None = None,
        older_than: datetime | None = None,
        newer_than: datetime | None = None,
    ) -> list[DeadLetterEntry]:
        query: dict[str, Any] = {}
        if channel is not None:
            query["channel"] = channel
        if source is not None:
            query["source"] = source.value
        if tenant_id is not None:
            query["tenant_id"] = tenant_id
        if older_than is not None or newer_than is not None:
            rng: dict[str, Any] = {}
            if older_than is not None:
                # WHY ceil: BSON is millisecond-precision and pymongo floors
                # the operand too, so a raw `$lt` silently drops every entry
                # sharing a millisecond with the cutoff — see
                # _bson_time.ceil_to_bson_millisecond. `$gt` needs no such
                # adjustment (floor is already correct for a lower bound).
                rng["$lt"] = ceil_to_bson_millisecond(older_than)
            if newer_than is not None:
                rng["$gt"] = newer_than
            query["last_failed_at"] = rng

        docs = (
            await DeadLetterDocument.find(query)
            .sort("+first_failed_at")
            .skip(offset)
            .limit(limit)
            .to_list()
        )
        return [self._doc_to_entry(d) for d in docs]

    async def delete_where(
        self,
        *,
        older_than: datetime | None = None,
        source: DeadLetterSource | Sequence[DeadLetterSource] | None = None,
        channel: str | None = None,
        tenant_id: str | None = None,
        limit: int | None = None,
    ) -> int:
        """Chunked-sweep-friendly bulk delete — one predicate is required."""
        if older_than is None and source is None and channel is None and tenant_id is None:
            raise ValueError(
                "delete_where() requires at least one predicate "
                "(older_than/source/channel/tenant_id) — refusing to delete "
                "every entry."
            )
        if limit is not None and limit < 1:
            raise ValueError(f"delete_where limit must be ≥ 1, got {limit}.")

        query: dict[str, Any] = {}
        if older_than is not None:
            # WHY ceil: without it a chunked sweep re-passing a fixed cutoff
            # returns 0 while entries stored in the cutoff's own millisecond
            # remain — the sweep reports "done" and is not. See
            # _bson_time.ceil_to_bson_millisecond.
            query["last_failed_at"] = {"$lt": ceil_to_bson_millisecond(older_than)}
        if channel is not None:
            query["channel"] = channel
        if tenant_id is not None:
            query["tenant_id"] = tenant_id
        if source is not None:
            sources = source if isinstance(source, (list, tuple, set)) else [source]
            query["source"] = {"$in": [s.value for s in sources]}  # type: ignore[union-attr]

        find = DeadLetterDocument.find(query)
        if limit is not None:
            ids = [d.id async for d in find.limit(limit)]
        else:
            ids = [d.id async for d in find]
        if not ids:
            return 0
        result = await DeadLetterDocument.find({"_id": {"$in": ids}}).delete()
        return int(result.deleted_count) if result is not None else len(ids)

    async def count_by_channel(self) -> dict[str, int]:
        """Return ``{channel: count}`` via an aggregation pipeline.

        WHY: ``beanie``'s ``AggregationQuery.get_cursor()`` unconditionally
        ``await``s the underlying collection's ``aggregate()`` call, but the
        installed motor version's ``AsyncIOMotorCollection.aggregate()``
        returns its (already-async-iterable) cursor synchronously rather
        than as a coroutine — ``await``ing it raises ``TypeError: object
        AsyncIOMotorLatentCommandCursor can't be used in 'await'
        expression``. Drive the pymongo/motor collection directly instead
        of routing through ``Document.aggregate().to_list()`` to sidestep
        beanie's incompatible cursor plumbing, while still tolerating a
        driver whose ``aggregate()`` *does* return a coroutine (pymongo's
        native async API) so this keeps working across driver versions.
        """
        pipeline = [{"$group": {"_id": "$channel", "count": {"$sum": 1}}}]
        collection = DeadLetterDocument.get_pymongo_collection()
        cursor = collection.aggregate(pipeline)
        if inspect.isawaitable(cursor):
            cursor = await cursor  # type: ignore[assignment]
        results = [doc async for doc in cursor]  # type: ignore[attr-defined]
        return {r["_id"]: r["count"] for r in results}

    def _doc_to_entry(self, doc: DeadLetterDocument) -> DeadLetterEntry:
        event = None
        payload = doc.payload
        if doc.event_type is not None and doc.payload is not None:
            try:
                event = self._serializer.deserialize(doc.payload)
                payload = None
            except Exception as exc:  # noqa: BLE001
                _logger.warning(
                    "BeanieDeadLetterQueue: failed to deserialize event for "
                    "entry_id=%s: %s — returning raw payload instead.",
                    doc.id,
                    exc,
                )
        return DeadLetterEntry(
            entry_id=doc.id,
            event=event,
            channel=doc.channel,
            handler_name=doc.handler_name,
            error_type=doc.error_type,
            error_message=doc.error_message,
            attempts=doc.attempts,
            first_failed_at=doc.first_failed_at,
            last_failed_at=doc.last_failed_at,
            source=DeadLetterSource(doc.source),
            source_ref=doc.source_ref,
            payload=payload,
            tenant_id=doc.tenant_id,
        )

    def __repr__(self) -> str:
        return "BeanieDeadLetterQueue()"


__all__ = [
    "BeanieDeadLetterQueue",
    "DeadLetterDocument",
]
