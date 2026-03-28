"""
varco_core.service.audit
========================
Audit / Mutation Log infrastructure for the varco service layer.

Provides three components that together implement a complete audit trail:

``AuditEntry``
    Frozen value object.  Stores the persisted form of an ``AuditEvent``.

``AuditRepository``
    ABC.  Implement against your storage backend (SA, Beanie, etc.).

``AuditLogMixin``
    Service mixin.  Overrides ``_after_create``, ``_after_update``, and
    ``_after_delete`` to emit ``AuditEvent`` instances via the service's
    existing ``AbstractEventProducer``.  Compose via MRO with ``AsyncService``.

``AuditConsumer``
    ``EventConsumer`` subclass.  Listens to ``AuditEvent`` on the
    ``"varco.audit"`` channel and persists each event as an ``AuditEntry``
    via an injected ``AuditRepository``.

Wiring example::

    class OrderService(
        AuditLogMixin,
        AsyncService[Order, UUID, CreateOrderDTO, OrderReadDTO, UpdateOrderDTO],
    ):
        def _get_repo(self, uow): return uow.orders

        def _get_audit_actor(self, ctx): return ctx.sub  # JWT subject

    # Wire the consumer
    audit_consumer = AuditConsumer(bus=event_bus, audit_repo=SAuditRepository(session))
    audit_consumer.register_to(event_bus)

DESIGN: emit via AbstractEventProducer (not direct AuditRepository write)
    ✅ Decoupled — the service does not need to know the AuditRepository type.
    ✅ Retry + DLQ + inbox pattern all apply to AuditConsumer's @listen handler.
    ✅ Works with existing event bus infrastructure; no new write path needed.
    ❌ Adds latency — audit record is eventually consistent (async consumer).
       For synchronous audit needs, override ``_after_create`` directly.

DESIGN: AuditLogMixin calls super() on each hook
    ✅ MRO-safe — multiple mixins can chain audit hooks without overriding
       each other.  Always call ``await super()._after_create(...)`` at the END
       of each override so the MRO chain completes.

Thread safety:  ❌ Not thread-safe.  Use from a single event loop.
Async safety:   ✅ All mixin hooks and consumer methods are ``async def``.

📚 Docs
- 🐍 https://docs.python.org/3/library/dataclasses.html
  dataclasses — used for ``AuditEntry`` value object.
- 📐 https://microservices.io/patterns/data/event-sourcing.html
  Event Sourcing / Audit Log pattern.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING
from uuid import UUID, uuid4

from varco_core.event.audit_event import AuditEvent
from varco_core.event.base import Event
from varco_core.event.consumer import EventConsumer, listen
from varco_core.service.mixin import ServiceMixin

if TYPE_CHECKING:
    from varco_core.auth import AuthContext
    from varco_core.dto import ReadDTO
    from varco_core.model import DomainModel

_logger = logging.getLogger(__name__)

# Channel for AuditConsumer.  Using a dedicated channel avoids interference
# with domain event channels and lets operators route audit events separately.
_AUDIT_CHANNEL: str = "varco.audit"


# ── AuditEntry ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AuditEntry:
    """
    Immutable value object representing a persisted audit record.

    Constructed from an ``AuditEvent`` by ``AuditEntry.from_event()``.
    Stored by ``AuditRepository.save()`` in the backing DB table.

    DESIGN: frozen dataclass over a Pydantic model
        ✅ No Pydantic dependency in the service layer.
        ✅ Hashable — safe to use in sets or as dict keys.
        ❌ No built-in JSON serialization — backends use their own ORM mapping.

    Thread safety:  ✅ Frozen — immutable after construction.
    Async safety:   ✅ Pure value object; no I/O.

    Attributes:
        entry_id:       Unique identifier for this audit row.
        entity_type:    Name of the mutated entity class (e.g. ``"Order"``).
        entity_id:      String representation of the entity primary key.
        action:         One of ``"create"``, ``"update"``, ``"delete"``.
        actor_id:       Identity of the caller.  ``None`` for system-initiated.
        diff:           Field-level change data.  Structure varies by action.
        occurred_at:    UTC timestamp when the audit event was emitted.
        correlation_id: Optional request-tracing identifier.
        tenant_id:      Optional tenant identifier.

    Example::

        entry = AuditEntry.from_event(audit_event)
        await audit_repo.save(entry)
    """

    entry_id: UUID = field(default_factory=uuid4)
    entity_type: str = ""
    entity_id: str = ""
    action: str = ""  # "create" | "update" | "delete"
    actor_id: str | None = None
    diff: dict[str, Any] = field(default_factory=dict)
    occurred_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    correlation_id: str | None = None
    tenant_id: str | None = None

    @classmethod
    def from_event(cls, event: AuditEvent) -> AuditEntry:
        """
        Construct an ``AuditEntry`` from an ``AuditEvent``.

        Args:
            event: The ``AuditEvent`` to convert.

        Returns:
            A fully populated ``AuditEntry`` ready for persistence.

        Edge cases:
            - ``occurred_at`` is set to ``now()`` at conversion time —
              NOT to ``event.event_time``.  This records when the consumer
              persisted the event, not when the service emitted it.  Use
              ``event.event_time`` for the emission timestamp if needed.
        """
        return cls(
            entity_type=event.entity_type,
            entity_id=event.entity_id,
            action=event.action,
            actor_id=event.actor_id,
            # Copy diff by value — AuditEvent.diff is a mutable dict even
            # though AuditEvent itself is immutable.  dict() ensures the
            # AuditEntry's diff is independent.
            diff=dict(event.diff),
            correlation_id=event.correlation_id,
            tenant_id=event.tenant_id,
        )


# ── AuditRepository ───────────────────────────────────────────────────────────


class AuditRepository(ABC):
    """
    Abstract persistence contract for the audit log store.

    Implement this against your DB backend:
    - ``varco_sa``: ``SAAuditRepository`` using ``AsyncSession``.

    Thread safety:  ⚠️ Implementations must not share state between concurrent
                       callers — use one repository instance per task/request.
    Async safety:   ✅ All methods are ``async def``.
    """

    @abstractmethod
    async def save(self, entry: AuditEntry) -> None:
        """
        Persist an audit entry.

        Args:
            entry: The ``AuditEntry`` to persist.

        Raises:
            Any DB-level exception from the underlying driver.

        Edge cases:
            - Should be idempotent on ``entry_id`` collision — use INSERT
              OR IGNORE / INSERT ON CONFLICT DO NOTHING.
        """

    @abstractmethod
    async def list_for_entity(
        self,
        entity_type: str,
        entity_id: str,
        *,
        limit: int = 100,
    ) -> list[AuditEntry]:
        """
        Return audit entries for a specific entity, newest-first.

        Args:
            entity_type: Entity class name to filter by.
            entity_id:   Entity primary key string to filter by.
            limit:       Maximum number of entries to return.  Default ``100``.

        Returns:
            List of ``AuditEntry`` objects ordered by ``occurred_at DESC``.
            Empty list if no audit records exist for the given entity.

        Edge cases:
            - Sorting without a DB index on ``(entity_type, entity_id, occurred_at)``
              is O(N) — add an index in production schemas.
        """


# ── AuditLogMixin ─────────────────────────────────────────────────────────────


class AuditLogMixin(ServiceMixin):
    """
    Service mixin that emits ``AuditEvent`` on each entity mutation.

    Overrides ``_after_create``, ``_after_update``, and ``_after_delete``
    (added to ``AsyncService`` as no-op hooks) to publish an ``AuditEvent``
    via the service's ``_producer`` (``AbstractEventProducer``).

    Compose via MRO — place ``AuditLogMixin`` to the LEFT of ``AsyncService``
    in the class header so its hooks run before the base no-op::

        class OrderService(
            AuditLogMixin,
            AsyncService[Order, UUID, ...],
        ):
            ...

    DESIGN: publish via _producer (not write directly to AuditRepository)
        ✅ The mixin has no DB coupling — it only needs an AbstractEventProducer.
        ✅ The audit event flows through retry + DLQ + inbox wrappers on
           AuditConsumer — reliable persistence without mixin complexity.
        ❌ Audit record is eventually consistent — not in the same transaction
           as the entity write.  For strict consistency, override the hook to
           write to AuditRepository directly inside the same UoW instead.

    DESIGN: _get_audit_actor() override pattern
        ✅ Provides a safe default (None) so the mixin works out of the box.
        ✅ Concrete services override only what they need — no forced dependency
           on a specific AuthContext schema.
        ❌ Must be overridden to get meaningful actor_id — base returns None.

    Thread safety:  ❌ Inherits from AsyncService — use from a single event loop.
    Async safety:   ✅ All override hooks are ``async def``.

    Edge cases:
        - If ``_produce`` raises, the exception propagates to the caller —
          the entity IS already persisted (hooks run after commit).
        - Chain ALL hooks with ``await super()._after_*()`` — other mixins
          in the MRO may also override these hooks.
        - ``_get_audit_actor`` is NOT async — keep it pure / synchronous.
    """

    def _get_audit_actor(self, ctx: AuthContext) -> str | None:
        """
        Extract the actor identity from the auth context.

        Override in concrete service classes to return the caller's identifier
        (e.g. ``ctx.sub`` for JWT subject, ``ctx.metadata.get("user_id")``).

        Args:
            ctx: Caller's ``AuthContext``.

        Returns:
            Actor identifier string, or ``None`` if not determinable.

        Edge cases:
            - Returns ``None`` by default — override or the audit record will
              have ``actor_id=None`` (acceptable for system-initiated mutations).
        """
        # Base: no actor extraction — override in concrete service.
        return None

    async def _after_create(
        self,
        entity: DomainModel,
        read_dto: ReadDTO,
        ctx: AuthContext,
    ) -> None:
        """
        Emit an ``AuditEvent`` for the entity creation.

        Args:
            entity:   The newly created domain entity (with pk assigned).
            read_dto: The ``ReadDTO`` returned to the caller.
            ctx:      Caller's identity.

        Edge cases:
            - ``diff`` contains the full ``read_dto.model_dump()`` — every field
              visible to the caller is recorded.  Redact sensitive fields by
              overriding ``_get_audit_diff_create()`` if needed.
        """
        await self._producer._produce(  # type: ignore[attr-defined]
            AuditEvent(
                entity_type=type(entity).__name__,
                entity_id=str(entity.pk),  # type: ignore[attr-defined]
                action="create",
                actor_id=self._get_audit_actor(ctx),
                # Record the full read_dto fields as the creation diff.
                diff=read_dto.model_dump(),  # type: ignore[attr-defined]
                tenant_id=ctx.metadata.get("tenant_id") if ctx else None,  # type: ignore[attr-defined]
            ),
            channel=_AUDIT_CHANNEL,
        )
        await super()._after_create(entity, read_dto, ctx)  # type: ignore[misc]

    async def _after_update(
        self,
        before_dto: ReadDTO,
        entity: DomainModel,
        read_dto: ReadDTO,
        ctx: AuthContext,
    ) -> None:
        """
        Emit an ``AuditEvent`` for the entity update, including before/after diff.

        Args:
            before_dto: ``ReadDTO`` before the update was applied.
            entity:     The saved entity after update.
            read_dto:   The ``ReadDTO`` returned to the caller (post-update).
            ctx:        Caller's identity.

        Edge cases:
            - ``diff["before"]`` and ``diff["after"]`` contain the full
              ``model_dump()`` of each DTO — not just changed fields.  A
              field-level diff can be computed by comparing the two dicts.
        """
        await self._producer._produce(  # type: ignore[attr-defined]
            AuditEvent(
                entity_type=type(entity).__name__,
                entity_id=str(entity.pk),  # type: ignore[attr-defined]
                action="update",
                actor_id=self._get_audit_actor(ctx),
                diff={
                    "before": before_dto.model_dump(),  # type: ignore[attr-defined]
                    "after": read_dto.model_dump(),  # type: ignore[attr-defined]
                },
                tenant_id=ctx.metadata.get("tenant_id") if ctx else None,  # type: ignore[attr-defined]
            ),
            channel=_AUDIT_CHANNEL,
        )
        await super()._after_update(before_dto, entity, read_dto, ctx)  # type: ignore[misc]

    async def _after_delete(self, pk: Any, ctx: AuthContext) -> None:
        """
        Emit an ``AuditEvent`` for the entity deletion.

        Args:
            pk:  Primary key of the deleted entity.
            ctx: Caller's identity.

        Edge cases:
            - ``diff`` is empty (``{}``) — the entity is gone at hook call time.
              If you need to record the final state, fetch it in ``delete()``
              before the UoW commits and pass it here via a subclass override.
        """
        await self._producer._produce(  # type: ignore[attr-defined]
            AuditEvent(
                entity_type=self._entity_type().__name__,  # type: ignore[attr-defined]
                entity_id=str(pk),
                action="delete",
                actor_id=self._get_audit_actor(ctx),
                diff={},  # Entity is gone — no fields to record.
                tenant_id=ctx.metadata.get("tenant_id") if ctx else None,  # type: ignore[attr-defined]
            ),
            channel=_AUDIT_CHANNEL,
        )
        await super()._after_delete(pk, ctx)  # type: ignore[misc]


# ── AuditConsumer ─────────────────────────────────────────────────────────────


class AuditConsumer(EventConsumer):
    """
    ``EventConsumer`` that persists ``AuditEvent`` instances to an ``AuditRepository``.

    Wire via ``register_to(bus)`` after constructing with an ``AuditRepository``
    implementation.  The ``@listen`` decorator subscribes only to events whose
    ``__event_type__`` matches ``"varco.audit"`` on the ``"varco.audit"`` channel.

    DESIGN: dedicated consumer over inline repository write in AuditLogMixin
        ✅ Separation of concerns — the mixin emits; the consumer persists.
        ✅ Retry + DLQ wrappers on ``@listen`` provide reliable persistence.
        ✅ Consumer can be deployed on a separate process / pod from the service.
        ❌ Eventual consistency — audit record lands after the event is consumed,
           not atomically with the entity write.

    Thread safety:  ❌ Not thread-safe.  Use from a single event loop.
    Async safety:   ✅ Handler is ``async def``.

    Args:
        audit_repo: ``AuditRepository`` implementation for persistence.

    Edge cases:
        - ``register_to`` must be called before any events are published —
          the subscription is created at wiring time, not at handler call time.
        - If ``audit_repo.save`` raises, the exception propagates to the bus
          error policy.  Pair with ``retry_policy`` on ``@listen`` for resilience.

    Example::

        consumer = AuditConsumer(audit_repo=SAuditRepository(session))
        consumer.register_to(event_bus)
    """

    def __init__(self, *, audit_repo: AuditRepository) -> None:
        """
        Args:
            audit_repo: Repository implementation used to persist audit entries.
        """
        # Stored as an instance attribute so the @listen handler can access it
        # via self — the handler is a bound method resolved at register_to time.
        self._audit_repo = audit_repo

    @listen(AuditEvent, channel=_AUDIT_CHANNEL)
    async def on_audit_event(self, event: Event) -> None:
        """
        Persist an incoming ``AuditEvent`` as an ``AuditEntry``.

        Args:
            event: The incoming event — narrowed to ``AuditEvent`` by the
                   ``@listen(AuditEvent, ...)`` dispatch.

        Edge cases:
            - Non-``AuditEvent`` events cannot reach this handler because the
              bus filters by ``__event_type__ == "varco.audit"`` before dispatch.
            - If ``audit_repo.save`` fails, the exception propagates to the bus.
              Add ``retry_policy=RetryPolicy(...)`` to the ``@listen`` call for
              automatic retries on transient DB errors.
        """
        # The @listen(AuditEvent) filter guarantees this is always an AuditEvent.
        # The isinstance check is defensive and helps type narrowing.
        if not isinstance(event, AuditEvent):
            _logger.warning(
                "AuditConsumer received non-AuditEvent type=%r — ignoring",
                type(event).__name__,
            )
            return

        entry = AuditEntry.from_event(event)
        await self._audit_repo.save(entry)
        _logger.debug(
            "AuditConsumer: persisted audit entry_id=%s entity=%s/%s action=%s",
            entry.entry_id,
            entry.entity_type,
            entry.entity_id,
            entry.action,
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "AuditEntry",
    "AuditRepository",
    "AuditLogMixin",
    "AuditConsumer",
]
