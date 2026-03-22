"""
varco_core.event.domain
=======================
Domain-level events emitted by ``AsyncService`` for create, update, and
delete operations.

These events are built on top of the general-purpose event system and are
published automatically by ``AsyncService`` after each mutating operation
commits.  User code never constructs them directly — they are assembled
inside the service using the ReadDTO produced by the assembler.

Event hierarchy::

    Event
    └── EntityEvent              — base for all entity lifecycle events
        ├── EntityCreatedEvent   — emitted after a successful create()
        ├── EntityUpdatedEvent   — emitted after a successful update()
        └── EntityDeletedEvent   — emitted after a successful delete()

Channel convention
------------------
``AsyncService`` publishes domain events to a channel derived from the
entity class name (lowercase), e.g. ``"post"``, ``"user"``, ``"order"``.

This gives subscribers two independent routing axes:

- **By event type** (what happened):
  ``@listen(EntityCreatedEvent)`` — all creates across all entities.

- **By channel** (which entity):
  ``@listen(EntityEvent, channel="order")`` — all order events (create +
  update + delete).

- **Narrowed**:
  ``@listen(EntityCreatedEvent, channel="order")`` — only order creates.

- **Wildcard**:
  ``@listen(EntityEvent, channel="*")`` — every entity lifecycle event.

Correlation ID
--------------
``EntityEvent.correlation_id`` is auto-populated from the active tracing
context (``current_correlation_id()``) at event construction time inside
the service.  No caller action is required — the ID is threaded through
automatically when ``correlation_context()`` is active on the request.

Payload
-------
``EntityCreatedEvent`` and ``EntityUpdatedEvent`` carry a ``payload`` dict
— the result of ``assembler.to_read_dto(entity).model_dump()``.  This is
the serialized ReadDTO at the time of the operation.  Downstream consumers
can reconstruct the full DTO from this dict.

``EntityDeletedEvent`` does NOT carry a payload — the entity no longer
exists and the ReadDTO cannot be reconstructed.  The ``pk`` field alone
identifies what was deleted.

Thread safety:  ✅ All events are immutable (Pydantic frozen model).
Async safety:   ✅ Immutable — safe to pass across coroutines.
"""

from __future__ import annotations

from typing import Any, ClassVar

from varco_core.event.base import Event


# ── EntityEvent ────────────────────────────────────────────────────────────────


class EntityEvent(Event):
    """
    Base class for all entity lifecycle events.

    Carries the identity of the affected entity (``entity_type``, ``pk``)
    and an optional correlation ID for request tracing.

    Subscribing to ``EntityEvent`` receives ALL subclass events
    (``EntityCreatedEvent``, ``EntityUpdatedEvent``, ``EntityDeletedEvent``)
    because ``InMemoryEventBus`` uses ``isinstance`` for class-based dispatch.

    Attributes:
        entity_type:     Short name of the domain class, e.g. ``"Post"``.
                         Matches ``type(entity).__name__`` on the service side.
        pk:              Primary key of the affected entity.  Type is ``Any``
                         because PKs may be ``int``, ``UUID``, or composite.
        correlation_id:  Active request correlation ID, or ``None`` if the
                         event was produced outside a ``correlation_context``.
    """

    entity_type: str
    pk: Any
    # Populated from current_correlation_id() at construction time inside
    # AsyncService._publish_domain_event().  Optional — None when produced
    # outside an active correlation context (e.g. background jobs).
    correlation_id: str | None = None


# ── EntityCreatedEvent ────────────────────────────────────────────────────────


class EntityCreatedEvent(EntityEvent):
    """
    Emitted by ``AsyncService.create()`` after the UoW commits.

    The ``payload`` field contains the serialized ReadDTO of the newly created
    entity, produced by ``assembler.to_read_dto(saved_entity).model_dump()``.

    Attributes:
        payload: Serialized ReadDTO as a plain dict.  Keys and values match
                 the ReadDTO's Pydantic model fields, with datetimes serialized
                 to ISO-8601 strings (Pydantic's default ``model_dump()``
                 behaviour).

    Edge cases:
        - ``payload`` serialization uses Pydantic's default ``model_dump()``
          — complex nested types (nested models, custom types) are serialized
          as Pydantic would serialize them.  For round-tripping, use the
          matching ReadDTO class to reconstruct.
    """

    __event_type__: ClassVar[str] = "entity.created"

    # Serialized ReadDTO — dict rather than ReadDTO to remain serializable
    # across process boundaries (Kafka, Redis).  Dict is always JSON-safe.
    payload: dict[str, Any]


# ── EntityUpdatedEvent ────────────────────────────────────────────────────────


class EntityUpdatedEvent(EntityEvent):
    """
    Emitted by ``AsyncService.update()`` after the UoW commits.

    The ``payload`` field contains the serialized ReadDTO reflecting the
    entity's new state after the update was applied.

    Attributes:
        payload: Serialized ReadDTO as a plain dict (post-update state).

    Edge cases:
        - The payload reflects the NEW state — not the diff or the old state.
          If you need the old state, fetch it before the update or implement
          an event-sourced approach.
    """

    __event_type__: ClassVar[str] = "entity.updated"

    payload: dict[str, Any]


# ── EntityDeletedEvent ────────────────────────────────────────────────────────


class EntityDeletedEvent(EntityEvent):
    """
    Emitted by ``AsyncService.delete()`` after the UoW commits.

    Does NOT carry a payload — the entity has been removed and the ReadDTO
    can no longer be reconstructed from the DB.  The ``pk`` field (inherited
    from ``EntityEvent``) identifies what was deleted.

    DESIGN: no payload on delete
        ✅ The entity no longer exists — no authoritative source for payload.
        ✅ Keeps the event lightweight.
        ❌ Consumers that need the last known state must capture it before
           deletion (e.g. via a ``@listen(EntityUpdatedEvent)`` that caches
           the last known DTO).  This is intentional — it forces consumers
           to be explicit about their data needs.
    """

    __event_type__: ClassVar[str] = "entity.deleted"
