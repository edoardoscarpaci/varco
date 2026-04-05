"""
example.events
==============
Domain events emitted by ``PostService``.

Events are published AFTER the unit-of-work commits so consumers never
observe an event for a transaction that was rolled back.  For guaranteed
delivery (at-least-once), use the outbox pattern instead of direct
``_producer._produce()`` calls — see ``varco_core.service.outbox``.

``PostCreatedEvent`` and ``PostDeletedEvent`` extend ``Event`` (Pydantic
``BaseModel``) so they are JSON-serialisable by the bus serializer.

DESIGN: domain events separate from ``EntityCreatedEvent``
    ``AsyncService.create()`` already emits a generic ``EntityCreatedEvent``
    with ``payload=read_dto.model_dump()``.  The domain-specific events here
    carry strongly-typed fields (``post_id: UUID``) rather than an opaque
    ``payload: dict``.  This makes consumer ``@listen`` handlers type-safe
    and avoids dict-key errors at handler time.

    ✅ Strongly-typed event payloads — no ``event.payload["post_id"]`` magic.
    ✅ Event schema is checked by mypy/pyright — impossible to publish a
       ``PostCreatedEvent`` with a missing ``author_id``.
    ❌ Two event emissions per ``create()`` (generic + domain-specific) —
       consumers that need both must subscribe to both.  Acceptable tradeoff.

Thread safety:  ✅ ``Event`` is a Pydantic BaseModel — immutable after creation.
Async safety:   ✅ Pure value objects — no I/O.
"""

from __future__ import annotations

from uuid import UUID

from varco_core.event.base import Event


class PostCreatedEvent(Event):
    """
    Emitted after a ``Post`` is successfully created and committed.

    Consumers use this event to trigger side-effects: sending notifications,
    updating a search index, incrementing counters, etc.

    Args:
        post_id:   UUID of the newly created post.
        author_id: UUID of the user who authored the post.

    Edge cases:
        - Published AFTER the UoW commits — the post IS durable when this
          event fires.  Consumers may safely fetch the post by ``post_id``.
        - If the bus is unavailable, the event is lost (no outbox pattern here).
          For guaranteed delivery, persist in the same transaction via
          ``OutboxRepository``.
    """

    post_id: UUID
    author_id: UUID | None


class PostDeletedEvent(Event):
    """
    Emitted after a ``Post`` is successfully deleted and committed.

    Consumers use this event to clean up related data: remove from search
    index, revoke cached entries in other services, audit logging, etc.

    Args:
        post_id: UUID of the deleted post.  The post is no longer retrievable
                 from the repository when this event fires.

    Edge cases:
        - The post is GONE when this fires — consumers must not call
          ``service.get(post_id)`` expecting to find it.
        - Use ``author_id`` if needed — look it up BEFORE calling ``delete()``
          or extend this event to carry it.
    """

    post_id: UUID


__all__ = ["PostCreatedEvent", "PostDeletedEvent"]
