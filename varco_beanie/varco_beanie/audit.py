"""
varco_beanie.audit
==================
Beanie (pymongo / MongoDB) implementation of ``AuditRepository``.

``BeanieAuditRepository`` maps ``AuditEntry`` value objects to the
``varco_audit_log`` MongoDB collection using the ``AuditDocument`` Beanie
document class.

Unlike the outbox pattern, audit records are append-only — there are no
``get_pending()`` / ``delete()`` operations.  The interface is simpler:
``save()`` inserts; ``list_for_entity()`` queries.

Collection
----------
``AuditDocument`` maps to the ``varco_audit_log`` collection.  Register it
with Beanie before use::

    from varco_beanie.audit import AuditDocument

    # Option A — init_beanie
    await init_beanie(database=db, document_models=[..., AuditDocument])

    # Option B — BeanieRepositoryProvider
    provider.register(AuditDocument)
    await provider.init()

Usage::

    from varco_beanie.audit import BeanieAuditRepository
    from varco_core.service.audit import AuditConsumer

    consumer = AuditConsumer(audit_repo=BeanieAuditRepository())
    consumer.register_to(event_bus)

    # Direct query
    entries = await BeanieAuditRepository().list_for_entity("Order", str(order_id))

DESIGN: no-session default (session=None)
    ✅ Audit records are written by AuditConsumer asynchronously after the
       domain commit — they do NOT need to be in the same MongoDB transaction.
    ✅ ``session=None`` works on single-node MongoDB (no replica set required).
    ✅ Passing a session enables replica-set-transactional audit writes for
       strict-consistency scenarios.
    ❌ On single-node MongoDB without a session, there is no rollback if the
       consumer crashes after insert.  This is acceptable — at-least-once
       delivery means duplicates are possible; ``entry_id`` uniqueness guards
       against double-persist.

DESIGN: ``insert(..., ignore_errors=False)`` + no conflict handling on Beanie
    MongoDB does not have INSERT OR IGNORE equivalent — instead ``entry_id``
    uniqueness is enforced by a sparse unique index on ``_id`` (Beanie's default
    for ``id`` fields).  Duplicate inserts raise ``DuplicateKeyError`` which
    ``AuditConsumer`` should treat as already-processed (idempotent consumer).

Thread safety:  ⚠️ ``AsyncClientSession``, if provided, is NOT thread-safe.
                    Session-less instances are safe to share.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🔍 https://beanie-odm.dev/tutorial/defining-a-document/
  Beanie Document — collection configuration and field mapping
- 🔍 https://www.mongodb.com/docs/manual/core/transactions/
  MongoDB transactions — replica set requirement for multi-doc atomicity
- 🐍 https://pymongo.readthedocs.io/en/stable/api/pymongo/client_session.html
  AsyncClientSession — pymongo transaction API
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from beanie import Document
from pydantic import Field

from varco_core.service.audit import AuditEntry, AuditRepository

if TYPE_CHECKING:
    from pymongo.asynchronous.client_session import AsyncClientSession

_logger = logging.getLogger(__name__)


# ── AuditDocument ─────────────────────────────────────────────────────────────


class AuditDocument(Document):
    """
    Beanie document representing a single persisted audit log entry.

    Maps to the ``varco_audit_log`` MongoDB collection.

    Register this document in your ``init_beanie()`` or
    ``BeanieRepositoryProvider.register()`` call before using
    ``BeanieAuditRepository``.

    DESIGN: Beanie Document over raw pymongo collection
        ✅ Consistent with varco_beanie repository layer conventions.
        ✅ Beanie auto-generates the unique ``_id`` index on ``id`` —
           duplicate ``entry_id`` raises ``DuplicateKeyError`` (idempotency).
        ✅ Pydantic validation ensures ``diff`` is always a ``dict``.
        ❌ Requires ``init_beanie()`` at startup.

    Thread safety:  ✅ Document class is a static definition — no mutable state.
    Async safety:   ✅ All Beanie methods are ``async def``.

    Attributes:
        id:             UUIDv4 — matches ``AuditEntry.entry_id``.
        entity_type:    Entity class name (e.g. ``"Order"``).
        entity_id:      String representation of the entity primary key.
        action:         One of ``"create"``, ``"update"``, ``"delete"``.
        actor_id:       Identity of the mutation actor — ``None`` for system.
        diff:           Field-level change data.  Structure varies by action.
        occurred_at:    UTC timestamp when the service emitted the audit event.
        correlation_id: Optional request-tracing identifier.
        tenant_id:      Optional tenant identifier.

    Edge cases:
        - ``diff`` is stored as a plain BSON document (dict) — all values must
          be BSON-serializable.  Pydantic types (e.g. UUID, datetime) must be
          converted to JSON-compatible types before storing in ``diff``.
        - ``id`` collision (duplicate ``entry_id``) raises
          ``pymongo.errors.DuplicateKeyError`` — the caller should treat this
          as "already persisted" (at-least-once consumer idempotency).
        - No additional indexes are created by default.  For production
          ``list_for_entity()`` performance, add a compound index on
          ``{ entity_type: 1, entity_id: 1, occurred_at: -1 }``.
    """

    # Override Beanie's ObjectId pk with a UUID — matches AuditEntry.entry_id.
    id: UUID = Field(default_factory=uuid4)

    entity_type: str
    """Entity class name — e.g. ``"Order"``."""

    entity_id: str
    """String representation of the entity primary key."""

    action: str
    """Mutation action — one of ``"create"``, ``"update"``, ``"delete"``."""

    actor_id: str | None = None
    """Identity of the mutation actor — ``None`` for system-initiated mutations."""

    diff: dict[str, Any] = Field(default_factory=dict)
    """Field-level change data.  Structure varies by action."""

    occurred_at: datetime = Field(
        # Always UTC — avoids naive datetime ambiguity on round-trip.
        default_factory=lambda: datetime.now(tz=timezone.utc)
    )
    """UTC timestamp of when the service emitted the audit event."""

    correlation_id: str | None = None
    """Optional request-tracing identifier."""

    tenant_id: str | None = None
    """Optional tenant identifier for multi-tenant deployments."""

    class Settings:
        """Beanie collection and index configuration."""

        # Collection name — all audit log entries go here.
        name = "varco_audit_log"

        # DESIGN: no compound index declared here — callers can add
        # ``{ entity_type: 1, entity_id: 1, occurred_at: -1 }`` via a
        # Beanie migration or Atlas UI for production ``list_for_entity()``
        # performance.  Declaring indexes here would affect all deployments.
        indexes: list = []

    def __repr__(self) -> str:
        return (
            f"AuditDocument("
            f"id={self.id}, "
            f"entity={self.entity_type}/{self.entity_id}, "
            f"action={self.action!r})"
        )


# ── BeanieAuditRepository ─────────────────────────────────────────────────────


class BeanieAuditRepository(AuditRepository):
    """
    Beanie implementation of ``AuditRepository``.

    Persists audit entries in the ``varco_audit_log`` MongoDB collection via
    ``AuditDocument``.  Optionally accepts an ``AsyncClientSession`` so
    operations participate in the caller's MongoDB transaction (replica set
    only).

    Pass ``session=None`` (the default) for:
    - Single-node MongoDB deployments.
    - ``AuditConsumer`` use (asynchronous, no transaction required).
    - Testing against an in-memory / embedded MongoDB.

    Pass ``session=...`` for:
    - Strict-consistency audit writes inside a replica-set UoW transaction.

    Thread safety:  ⚠️ If ``session`` is provided, one instance per request.
                        ``session=None`` instances are safe to share.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        session: Optional pymongo ``AsyncClientSession``.  Pass ``None``
                 (default) for non-transactional use.

    Edge cases:
        - ``AuditDocument`` must be registered with Beanie before calling
          any method.  Raises ``RuntimeError`` if not.
        - ``save()`` with a duplicate ``entry_id`` raises
          ``pymongo.errors.DuplicateKeyError`` (idempotency guard).
          ``AuditConsumer`` should be wired with a ``retry_policy`` that
          does NOT retry on ``DuplicateKeyError``.
        - ``list_for_entity()`` sorts in-memory if no compound index exists —
          acceptable for small collections, but add an index in production.

    Example::

        consumer = AuditConsumer(audit_repo=BeanieAuditRepository())
        consumer.register_to(event_bus)
    """

    def __init__(
        self,
        *,
        session: AsyncClientSession | None = None,
    ) -> None:
        """
        Args:
            session: Optional pymongo ``AsyncClientSession``.
                     Defaults to ``None`` — non-transactional use.
        """
        # Stored as optional — passed to every Beanie operation that accepts it.
        self._session = session

    async def save(self, entry: AuditEntry) -> None:
        """
        Insert ``entry`` into the ``varco_audit_log`` collection.

        When ``session`` is set, the insert joins the caller's transaction
        (replica set only).  Without a session, the insert is immediate.

        Args:
            entry: The ``AuditEntry`` to persist.

        Raises:
            pymongo.errors.DuplicateKeyError: If ``entry.entry_id`` already
                exists — treat as already-persisted (idempotency).
            RuntimeError: If ``AuditDocument`` was not registered with Beanie.

        Edge cases:
            - ``diff`` values must be BSON-serializable.  Pydantic model dumps
              (strings, numbers, lists, dicts) are fine.  Raw UUIDs or
              datetimes must be stringified before landing in ``diff``.
            - Without a transaction, the insert is immediately durable —
              no rollback if a subsequent step fails.

        Async safety: ✅ Awaits ``document.insert()``.
        """
        doc = AuditDocument(
            id=entry.entry_id,
            entity_type=entry.entity_type,
            entity_id=entry.entity_id,
            action=entry.action,
            actor_id=entry.actor_id,
            # Copy diff by value — AuditEntry.diff may be shared.
            diff=dict(entry.diff),
            occurred_at=entry.occurred_at,
            correlation_id=entry.correlation_id,
            tenant_id=entry.tenant_id,
        )
        # Pass session if available — routes the insert through the transaction.
        if self._session is not None:
            await doc.insert(session=self._session)
        else:
            await doc.insert()

        _logger.debug(
            "BeanieAuditRepository.save: inserted entry_id=%s entity=%s/%s action=%s",
            entry.entry_id,
            entry.entity_type,
            entry.entity_id,
            entry.action,
        )

    async def list_for_entity(
        self,
        entity_type: str,
        entity_id: str,
        *,
        limit: int = 100,
    ) -> list[AuditEntry]:
        """
        Return audit entries for a specific entity, newest-first.

        Uses ``AuditDocument.find(entity_type=..., entity_id=...).sort(-occurred_at).limit(n)``.

        Args:
            entity_type: Entity class name to filter by (e.g. ``"Order"``).
            entity_id:   Entity primary key string to filter by.
            limit:       Maximum number of entries to return.  Default ``100``.

        Returns:
            List of ``AuditEntry`` objects ordered by ``occurred_at DESC``.
            Empty list if no audit records exist for the given entity.

        Edge cases:
            - Without a compound index on ``(entity_type, entity_id, occurred_at)``,
              this is an in-memory sort over all matching documents — O(N).
              Add the index for production workloads.
            - ``limit=0`` returns an empty list (Beanie / Motor behaviour).
            - If ``session`` is set, the query runs within the caller's
              transaction (replica set only).

        Async safety: ✅ Awaits Beanie ``find()`` cursor.
        """
        find_kwargs: dict[str, Any] = {}
        if self._session is not None:
            find_kwargs["session"] = self._session

        docs = (
            await AuditDocument.find(
                # Filter by entity identity — compound equality filter.
                AuditDocument.entity_type == entity_type,
                AuditDocument.entity_id == entity_id,
                **find_kwargs,
            )
            # -occurred_at = descending order (newest first).
            .sort(-AuditDocument.occurred_at)
            .limit(limit)
            .to_list()
        )

        entries = [
            AuditEntry(
                entry_id=doc.id,
                entity_type=doc.entity_type,
                entity_id=doc.entity_id,
                action=doc.action,
                actor_id=doc.actor_id,
                # Copy diff by value — document dict is mutable.
                diff=dict(doc.diff) if doc.diff else {},
                occurred_at=(
                    doc.occurred_at
                    if doc.occurred_at.tzinfo is not None
                    # Coerce naive datetimes to UTC (MongoDB can return naive
                    # datetimes depending on codec configuration).
                    else doc.occurred_at.replace(tzinfo=timezone.utc)
                ),
                correlation_id=doc.correlation_id,
                tenant_id=doc.tenant_id,
            )
            for doc in docs
        ]

        _logger.debug(
            "BeanieAuditRepository.list_for_entity: entity=%s/%s fetched %d entries",
            entity_type,
            entity_id,
            len(entries),
        )
        return entries

    def __repr__(self) -> str:
        return (
            f"BeanieAuditRepository(" f"session={'set' if self._session else 'None'})"
        )


# ── Public API ────────────────────────────────────────────────────────────────


__all__ = [
    "AuditDocument",
    "BeanieAuditRepository",
]
