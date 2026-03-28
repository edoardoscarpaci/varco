"""
varco_sa.audit
==============
SQLAlchemy async implementation of ``AuditRepository``.

Provides a single class ``SAAuditRepository`` that writes ``AuditEntry``
value objects to a ``varco_audit_log`` table using an injected
``AsyncSession``.

Unlike the outbox pattern (which has two classes for service-layer vs relay
use), the audit repository is always written by ``AuditConsumer`` inside a
self-managed session — there is no service-layer same-transaction requirement
because audit records are persisted asynchronously after the domain commit.

Table
-----
``AuditEntryModel`` maps to the ``varco_audit_log`` table.
Include ``audit_metadata`` in your Alembic ``env.py``::

    from varco_sa.audit import audit_metadata

    target_metadata = [Base.metadata, outbox_metadata, audit_metadata]

Usage — with AuditConsumer::

    from sqlalchemy.ext.asyncio import async_sessionmaker
    from varco_sa.audit import SAAuditRepository
    from varco_core.service.audit import AuditConsumer

    session_factory = async_sessionmaker(engine)
    audit_repo = SAAuditRepository(session_factory)

    consumer = AuditConsumer(audit_repo=audit_repo)
    consumer.register_to(event_bus)

Usage — direct query::

    entries = await audit_repo.list_for_entity("Order", str(order_id), limit=50)

DESIGN: single-class over service/relay split (unlike SAOutboxRepository)
    ✅ AuditConsumer is the only writer — it always manages its own session.
    ✅ ``list_for_entity()`` is a read-only query — no UoW involvement.
    ✅ Simpler API surface — one class, one responsibility.
    ❌ If you need to write audit entries inside a UoW transaction (strict
       consistency), inject an ``AsyncSession`` directly via the private
       ``_from_session()`` factory instead.

Thread safety:  ⚠️ ``async_sessionmaker`` is shared; sessions created per op
                    are NOT thread-safe.  Use one relay/consumer per event loop.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🐍 https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
  SQLAlchemy async ORM — AsyncSession usage patterns
- 🐍 https://docs.sqlalchemy.org/en/20/core/type_basics.html#sqlalchemy.types.JSON
  SQLAlchemy JSON type — used for the ``diff`` column
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from uuid import UUID

from sqlalchemy import DateTime, JSON, String, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from varco_core.service.audit import AuditEntry, AuditRepository

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import async_sessionmaker

_logger = logging.getLogger(__name__)

# ── ORM model ─────────────────────────────────────────────────────────────────


class _AuditBase(DeclarativeBase):
    """
    Isolated ``DeclarativeBase`` for the audit log model.

    DESIGN: separate base over reusing the app's DeclarativeBase
        ✅ varco_sa can define the audit schema without knowing the app's Base.
        ✅ Users opt-in to Alembic management by including ``audit_metadata``
           in their ``target_metadata`` — no silent schema pollution.
        ❌ Users must explicitly include ``audit_metadata`` in Alembic config.
           See module docstring for setup.
    """


class AuditEntryModel(_AuditBase):
    """
    SQLAlchemy ORM row for a single audit log entry.

    Table name: ``varco_audit_log``.

    Thread safety:  ✅ Mapped class definition is read-only after creation.
    Async safety:   ✅ Class definition; no I/O.

    Edge cases:
        - ``diff`` is stored as JSON — type varies per action (dict for
          create/update, empty dict for delete).  Use ``JSON`` rather than
          ``JSONB`` so the model works on SQLite (for tests) and Postgres.
        - ``actor_id``, ``correlation_id``, ``tenant_id`` are nullable —
          None values map to SQL NULL.
        - No composite index on ``(entity_type, entity_id, occurred_at)`` is
          defined here — add one in a migration for production workloads where
          ``list_for_entity()`` is called frequently.
    """

    __tablename__ = "varco_audit_log"

    # Primary key — matches AuditEntry.entry_id (UUIDv4).
    entry_id: Mapped[UUID] = mapped_column(primary_key=True)

    # Entity class name — e.g. "Order", "User".
    entity_type: Mapped[str] = mapped_column(String(255), nullable=False)

    # String representation of the entity primary key.
    entity_id: Mapped[str] = mapped_column(String(255), nullable=False)

    # Mutation action — one of "create", "update", "delete".
    action: Mapped[str] = mapped_column(String(16), nullable=False)

    # Identity of the actor who triggered the mutation — None for system jobs.
    actor_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Field-level change data — structure varies by action.
    diff: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)

    # UTC timestamp when the audit event was emitted by the service.
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    # Optional request-scoped tracing ID — groups audit records by request.
    correlation_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # Optional tenant identifier — for multi-tenant deployments.
    tenant_id: Mapped[str | None] = mapped_column(String(255), nullable=True)


# Expose the metadata so users can wire it into Alembic target_metadata.
audit_metadata = _AuditBase.metadata


# ── Helper ─────────────────────────────────────────────────────────────────────


def _model_to_entry(row: AuditEntryModel) -> AuditEntry:
    """
    Convert an ``AuditEntryModel`` ORM row back to an ``AuditEntry`` value object.

    Args:
        row: ORM row fetched from the ``varco_audit_log`` table.

    Returns:
        An immutable ``AuditEntry`` with the same field values.

    Edge cases:
        - ``occurred_at`` is expected to be timezone-aware.  SQLite returns
          naive datetimes — coerced to UTC without raising.
    """
    # Ensure occurred_at is always timezone-aware — SQLite returns naive datetimes.
    occurred_at = row.occurred_at
    if occurred_at is not None and occurred_at.tzinfo is None:
        # Treat naive datetime as UTC — matches how AuditEntry stores it.
        occurred_at = occurred_at.replace(tzinfo=timezone.utc)

    return AuditEntry(
        entry_id=row.entry_id,
        entity_type=row.entity_type,
        entity_id=row.entity_id,
        action=row.action,
        actor_id=row.actor_id,
        # diff is stored as JSON dict — copy by value to ensure immutability.
        diff=dict(row.diff) if row.diff else {},
        occurred_at=occurred_at,
        correlation_id=row.correlation_id,
        tenant_id=row.tenant_id,
    )


# ── SAAuditRepository ─────────────────────────────────────────────────────────


class SAAuditRepository(AuditRepository):
    """
    SQLAlchemy implementation of ``AuditRepository``.

    Creates a fresh ``AsyncSession`` per operation via the injected
    ``async_sessionmaker`` and auto-commits after ``save()``.  This design
    mirrors ``SARelayOutboxRepository`` — the consumer (or caller) does not
    manage DB sessions itself.

    DESIGN: session-per-operation over injected-session
        ✅ ``AuditConsumer`` has no concept of SQLAlchemy sessions —
           keeps the consumer infrastructure-agnostic.
        ✅ Each ``save()`` commits immediately — audit record is durable
           before the consumer ACKs the event bus message.
        ✅ ``list_for_entity()`` always reads fresh data (no stale cache).
        ❌ Slightly higher connection-pool churn vs a shared session.
           Audit logging is low-frequency enough that this is negligible.

    Alternative: ``_from_session(session)`` factory
        For strict-consistency use cases (audit inside the same UoW transaction),
        instantiate directly with an ``AsyncSession`` via ``_from_session()``.
        Note: that variant does NOT auto-commit — the caller must commit.

    Thread safety:  ⚠️ ``async_sessionmaker`` is shared; sessions created per
                        call are NOT thread-safe.  Use one instance per event loop.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        session_factory: ``async_sessionmaker`` for creating sessions.

    Edge cases:
        - If the DB is unreachable, ``save()`` and ``list_for_entity()`` raise
          ``sqlalchemy.exc.OperationalError`` — ``AuditConsumer`` should be
          wired with a ``retry_policy`` to handle transient DB failures.
        - ``save()`` uses INSERT OR IGNORE (via ``ON CONFLICT DO NOTHING``) for
          Postgres.  On SQLite/MySQL the fallback is a plain INSERT (duplicate
          ``entry_id`` raises ``IntegrityError`` — harmless for at-least-once
          consumer delivery).

    Example::

        from sqlalchemy.ext.asyncio import async_sessionmaker
        from varco_sa.audit import SAAuditRepository
        from varco_core.service.audit import AuditConsumer

        repo = SAAuditRepository(async_sessionmaker(engine))
        consumer = AuditConsumer(audit_repo=repo)
        consumer.register_to(event_bus)
    """

    def __init__(self, session_factory: async_sessionmaker) -> None:  # type: ignore[type-arg]
        """
        Args:
            session_factory: ``async_sessionmaker`` for creating ``AsyncSession``
                             instances.  Must target the same database as the
                             rest of the application.

        Edge cases:
            - ``session_factory`` is not type-annotated with a generic parameter
              to avoid importing ``async_sessionmaker[AsyncSession]`` at runtime
              on Python < 3.12 without ``from __future__ import annotations``.
        """
        # session_factory is called once per operation — no shared session state.
        self._session_factory = session_factory

    @classmethod
    def _from_session(cls, session: AsyncSession) -> _SAAuditRepositoryInSession:
        """
        Factory for strict-consistency use — wraps an existing ``AsyncSession``.

        Use this when you need to write audit entries inside the same UoW
        transaction as the domain entity write (synchronous audit trail).

        Args:
            session: Active ``AsyncSession`` from a caller's UoW.

        Returns:
            A ``_SAAuditRepositoryInSession`` that uses the provided session.
            The caller controls commit/rollback — this variant does NOT
            auto-commit.

        Edge cases:
            - If ``session`` is closed or the transaction is rolled back, all
              pending audit writes are discarded.
        """
        return _SAAuditRepositoryInSession(session)

    async def save(self, entry: AuditEntry) -> None:
        """
        Persist ``entry`` to the ``varco_audit_log`` table and commit.

        Uses ``INSERT ... ON CONFLICT DO NOTHING`` on Postgres to make the
        operation idempotent for at-least-once consumer delivery (where the same
        ``AuditEvent`` might be delivered twice after a consumer crash).  On
        other dialects, a plain INSERT is used — duplicate ``entry_id`` will
        raise ``IntegrityError``.

        Args:
            entry: The ``AuditEntry`` to persist.

        Raises:
            sqlalchemy.exc.OperationalError: If the DB is unreachable.
            sqlalchemy.exc.IntegrityError: On duplicate ``entry_id`` on
                                           non-Postgres dialects.

        Edge cases:
            - On Postgres, re-inserting the same ``entry_id`` is silently
              ignored — safe for at-least-once AuditConsumer delivery.
            - ``diff`` is serialized to JSON by SQLAlchemy — dict values must
              be JSON-serializable.

        Async safety: ✅ Each call creates, commits, and closes its own session.
        """
        async with self._session_factory() as session:
            # DESIGN: pg_insert with ON CONFLICT DO NOTHING for idempotency.
            # On non-Postgres dialects we fall back to a plain INSERT via
            # session.add() — the caller must handle IntegrityError if needed.
            try:
                # Use Postgres-specific upsert if available — most production
                # deployments of varco_sa use Postgres.
                stmt = (
                    pg_insert(AuditEntryModel)
                    .values(
                        entry_id=entry.entry_id,
                        entity_type=entry.entity_type,
                        entity_id=entry.entity_id,
                        action=entry.action,
                        actor_id=entry.actor_id,
                        diff=entry.diff,
                        occurred_at=entry.occurred_at,
                        correlation_id=entry.correlation_id,
                        tenant_id=entry.tenant_id,
                    )
                    .on_conflict_do_nothing(index_elements=["entry_id"])
                )
                await session.execute(stmt)
            except Exception:
                # pg_insert may not be supported on non-Postgres dialects
                # (e.g. SQLite used in tests) — fall back to plain ORM add().
                # Rollback any partial state from the failed execute() first.
                await session.rollback()
                session.add(
                    AuditEntryModel(
                        entry_id=entry.entry_id,
                        entity_type=entry.entity_type,
                        entity_id=entry.entity_id,
                        action=entry.action,
                        actor_id=entry.actor_id,
                        diff=entry.diff,
                        occurred_at=entry.occurred_at,
                        correlation_id=entry.correlation_id,
                        tenant_id=entry.tenant_id,
                    )
                )
            await session.commit()

        _logger.debug(
            "SAAuditRepository.save: committed entry_id=%s entity=%s/%s action=%s",
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

        Opens a fresh session, executes a SELECT with ``WHERE entity_type=?
        AND entity_id=? ORDER BY occurred_at DESC LIMIT ?``, and closes the
        session.

        Args:
            entity_type: Entity class name to filter by (e.g. ``"Order"``).
            entity_id:   Entity primary key string to filter by.
            limit:       Maximum number of entries to return.  Default ``100``.

        Returns:
            List of ``AuditEntry`` objects ordered by ``occurred_at DESC``.
            Empty list if no audit records exist for the given entity.

        Edge cases:
            - Without a DB index on ``(entity_type, entity_id, occurred_at)``
              this query is O(N) — add a composite index in production.
            - ``limit=0`` returns an empty list (handled by SQLAlchemy).

        Async safety: ✅ Each call creates and closes its own session.
        """
        async with self._session_factory() as session:
            stmt = (
                select(AuditEntryModel)
                .where(
                    # Filter by entity identity — both columns required.
                    AuditEntryModel.entity_type == entity_type,
                    AuditEntryModel.entity_id == entity_id,
                )
                # Newest-first — most recent mutations appear at the top.
                .order_by(AuditEntryModel.occurred_at.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()
            entries = [_model_to_entry(row) for row in rows]

        _logger.debug(
            "SAAuditRepository.list_for_entity: entity=%s/%s fetched %d entries",
            entity_type,
            entity_id,
            len(entries),
        )
        return entries

    def __repr__(self) -> str:
        return f"SAAuditRepository(session_factory={self._session_factory!r})"


# ── _SAAuditRepositoryInSession ───────────────────────────────────────────────


class _SAAuditRepositoryInSession(AuditRepository):
    """
    Private variant of ``SAAuditRepository`` that uses a provided session.

    Intended for strict-consistency use cases where audit entries must be
    written inside the same UoW transaction as the domain entity write.

    Obtained via ``SAAuditRepository._from_session(session)``.

    Thread safety:  ❌ AsyncSession is not thread-safe.  Use one per request.
    Async safety:   ✅ All methods are ``async def``.

    Edge cases:
        - ``save()`` does NOT commit — the caller's UoW controls the boundary.
        - ``list_for_entity()`` executes on the provided session — pending
          unsaved changes are flushed automatically (SA default).
    """

    def __init__(self, session: AsyncSession) -> None:
        """
        Args:
            session: Active ``AsyncSession``.  Typically ``uow.session``.
        """
        # Store session — all operations use this single session.
        self._session = session

    async def save(self, entry: AuditEntry) -> None:
        """
        Stage ``entry`` in the session without committing.

        The entry is persisted only when the enclosing UoW commits.

        Args:
            entry: The ``AuditEntry`` to stage.

        Edge cases:
            - If the UoW is rolled back, this entry is never written.
            - Duplicate ``entry_id`` raises ``IntegrityError`` at commit time.

        Async safety: ✅ Protected by the SA session lock.
        """
        self._session.add(
            AuditEntryModel(
                entry_id=entry.entry_id,
                entity_type=entry.entity_type,
                entity_id=entry.entity_id,
                action=entry.action,
                actor_id=entry.actor_id,
                diff=entry.diff,
                occurred_at=entry.occurred_at,
                correlation_id=entry.correlation_id,
                tenant_id=entry.tenant_id,
            )
        )
        _logger.debug(
            "_SAAuditRepositoryInSession.save: staged entry_id=%s (uncommitted)",
            entry.entry_id,
        )

    async def list_for_entity(
        self,
        entity_type: str,
        entity_id: str,
        *,
        limit: int = 100,
    ) -> list[AuditEntry]:
        """
        Return audit entries using the injected session.

        Args:
            entity_type: Entity class name to filter by.
            entity_id:   Entity primary key string to filter by.
            limit:       Maximum number of entries to return.

        Returns:
            List of ``AuditEntry`` objects ordered by ``occurred_at DESC``.

        Async safety: ✅ Awaits ``session.execute()``.
        """
        stmt = (
            select(AuditEntryModel)
            .where(
                AuditEntryModel.entity_type == entity_type,
                AuditEntryModel.entity_id == entity_id,
            )
            .order_by(AuditEntryModel.occurred_at.desc())
            .limit(limit)
        )
        result = await self._session.execute(stmt)
        rows = result.scalars().all()
        return [_model_to_entry(row) for row in rows]

    def __repr__(self) -> str:
        return f"_SAAuditRepositoryInSession(session={self._session!r})"


# ── Public API ────────────────────────────────────────────────────────────────


__all__ = [
    "AuditEntryModel",
    "audit_metadata",
    "SAAuditRepository",
]
