"""
varco_core.event.audit_event
============================
``AuditEvent`` — a typed domain event for entity mutation audit records.

Published by ``AuditLogMixin`` whenever a service mutates an entity via
``create()``, ``update()``, or ``delete()``.  Consumed by ``AuditConsumer``
which persists the event to an ``AuditRepository`` backend.

Design rationale
----------------
Using a typed ``Event`` subclass (rather than a raw dict or log line) keeps
the audit trail within the existing event infrastructure:
- AuditConsumer can use ``@listen`` + retry + DLQ for reliable persistence.
- The inbox pattern can guarantee at-least-once delivery to the DB.
- Filtering by ``channel`` lets you route audit events to a dedicated consumer
  without polluting domain event channels.

``diff`` is an untyped ``dict[str, Any]`` because the field structure varies
by entity type and action:
- create:  ``{"field": value, ...}``
- update:  ``{"before": {...}, "after": {...}}``
- delete:  ``{}``

``entity_id`` is a ``str`` (not a UUID) so the same field works for entities
with integer, UUID, or composite primary keys without type proliferation.

Thread safety:  ✅ Frozen Pydantic model — immutable after construction.
Async safety:   ✅ Pure value object; no I/O.

📚 Docs
- 📐 https://microservices.io/patterns/data/event-sourcing.html
  Event Sourcing — original rationale for audit via events.
- 🐍 https://docs.pydantic.dev/latest/
  Pydantic — used for ``Event`` field serialization and validation.
"""

from __future__ import annotations

from typing import Any, ClassVar, Literal

from varco_core.event.base import Event


class AuditEvent(Event):
    """
    Domain event emitted when an entity is created, updated, or deleted.

    Published by ``AuditLogMixin`` hooks in ``AsyncService``.  The ``channel``
    should be set to ``"varco.audit"`` (or a consumer-specific channel) at
    publish time so ``AuditConsumer`` can subscribe to it without interfering
    with domain-level event channels.

    DESIGN: AuditEvent as a typed Event subclass over a log entry
        ✅ Re-uses the existing event bus infrastructure — retry, DLQ, inbox
           pattern all apply with zero additional code.
        ✅ Type-safe: mypy verifies field access on the consumer side.
        ✅ Serializable via ``JsonEventSerializer`` — stored in Inbox/Outbox.
        ❌ Adds a new event type to the bus — audit consumers must filter by
           channel to avoid interference with domain consumers.

    Thread safety:  ✅ Pydantic model — immutable after construction.
    Async safety:   ✅ Pure value object; no I/O.

    Attributes:
        entity_type:    Name of the entity class (e.g. ``"Order"``).
        entity_id:      String representation of the entity's primary key.
                        Stored as ``str`` to support UUID, int, and composite PKs.
        action:         One of ``"create"``, ``"update"``, or ``"delete"``.
        actor_id:       Identity of the caller who triggered the mutation.
                        Sourced from ``AuthContext`` via ``_get_audit_actor()``.
                        ``None`` if the context provides no identity (e.g. system jobs).
        diff:           Field-level change data.
                        - create: ``{"field": new_value, ...}``
                        - update: ``{"before": {...}, "after": {...}}``
                        - delete: ``{}``
        correlation_id: Optional tracing identifier — propagated from the
                        request context so audit records can be grouped by request.
        tenant_id:      Optional tenant identifier — for multi-tenant systems.
                        Sourced from ``ctx.metadata.get("tenant_id")``.

    Edge cases:
        - ``diff`` for delete actions is always ``{}`` — the entity is gone.
        - ``actor_id`` is ``None`` for system-initiated mutations.  Log a
          warning in ``AuditConsumer`` if ``actor_id`` is None for user-visible
          mutations.
        - Audit events for the same entity in rapid succession may arrive
          out-of-order in the audit log if the consumer is distributed.  Use
          ``received_at`` from the audit log entry, not ``AuditEvent.event_time``,
          for total ordering within a single consumer.

    Example::

        AuditEvent(
            entity_type="Order",
            entity_id=str(order.pk),
            action="create",
            actor_id="user:abc123",
            diff={"total": 99.0, "status": "pending"},
            correlation_id="req-xyz",
            tenant_id="tenant:acme",
        )
    """

    # __event_type__ identifies this event class in the bus registry.
    # Using a namespaced string prevents collision with domain events.
    __event_type__: ClassVar[str] = "varco.audit"

    entity_type: str
    """Name of the entity class (e.g. ``"Order"``, ``"User"``)."""

    entity_id: str
    """String representation of the primary key — accommodates UUID, int, composite."""

    action: Literal["create", "update", "delete"]
    """The mutation action that produced this audit record."""

    actor_id: str | None = None
    """Caller identity from AuthContext — None for system-initiated mutations."""

    diff: dict[str, Any] = {}
    """Field-level change data.  Structure varies by action — see class docstring."""

    correlation_id: str | None = None
    """Request-scoped tracing ID for grouping audit records by request."""

    tenant_id: str | None = None
    """Tenant identifier for multi-tenant deployments."""


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = ["AuditEvent"]
