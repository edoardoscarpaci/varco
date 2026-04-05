"""
example.consumer
================
Event consumer for the ``Post`` entity.

``PostEventConsumer`` subscribes to domain events emitted by ``PostService``
and reacts to them — for example, updating a search index or sending
notifications.

Lifecycle integration
---------------------
``PostEventConsumer`` now satisfies ``AbstractLifecycle`` via the
``start()`` / ``stop()`` methods added to ``EventConsumer`` (GAP #3 fix).
Register it with ``VarcoLifespan`` so subscriptions are created at startup
and cancelled cleanly at shutdown::

    lifespan.register(bus)          # started first — must be up before consumers
    lifespan.register(consumer)     # started second — registers handlers
    ...
    # Shutdown: consumer.stop() → bus.stop() (LIFO)

The ``self._bus`` attribute must be set in ``__init__`` (via DI injection)
for ``start()`` to know which bus to register to.

Retry and DLQ
-------------
Handlers use ``retry_policy`` to retry transient failures (e.g. network
errors talking to a search service) with exponential back-off.  After
exhausting retries, events land in the ``InMemoryDeadLetterQueue`` (swap for
``RedisDLQ`` in production) for manual inspection and replay.

DESIGN: EventConsumer standalone class over mixing with PostService
    Separating the consumer from the service keeps each class focused on
    a single responsibility:
    - ``PostService``: CRUD operations and event PUBLISHING.
    - ``PostEventConsumer``: event CONSUMING and side-effect handling.

    ✅ Service stays small — easy to test without bus infrastructure.
    ✅ Consumer can be deployed as a separate process (just don't install
       the HTTP router — install only the consumer module).
    ❌ Two classes per entity instead of one mixin — acceptable SRP trade-off.

Thread safety:  ✅ ``register_to()`` is called once at startup.
Async safety:   ✅ Handlers are ``async def`` — never block the event loop.
"""

from __future__ import annotations

import logging

from providify import Inject, Singleton

from varco_core.event.base import AbstractEventBus
from varco_core.event.consumer import EventConsumer, listen
from varco_core.event.dlq import InMemoryDeadLetterQueue
from varco_core.resilience.retry import RetryPolicy

from example.events import PostCreatedEvent, PostDeletedEvent

_logger = logging.getLogger(__name__)

# ── Dead Letter Queue ─────────────────────────────────────────────────────────
# Shared across all @listen handlers in this consumer — events that exhaust
# all retries land here for manual inspection.
#
# DESIGN: InMemoryDeadLetterQueue for the example
#   In production, swap for RedisDLQ (varco_redis) or KafkaDLQ (varco_kafka)
#   so dead letters survive process restarts.
#   ✅ No external dependency for the example / tests.
#   ❌ Lost on process restart — not suitable for production use.
_dlq: InMemoryDeadLetterQueue = InMemoryDeadLetterQueue()

# ── Retry policy ──────────────────────────────────────────────────────────────
# 3 attempts with exponential back-off (0.5s, 1s, 2s).
# Suitable for transient failures: network errors, downstream service timeouts.
#
# DESIGN: shared policy over per-handler instances
#   ✅ One definition, consistent behaviour across all handlers.
#   ✅ Easy to override per-handler by declaring a local policy inline.
#   ❌ Handlers with very different SLAs may need individual policies.
_RETRY_POLICY = RetryPolicy(max_attempts=3, base_delay=0.5)


@Singleton
class PostEventConsumer(EventConsumer):
    """
    Consumes ``PostCreatedEvent`` and ``PostDeletedEvent`` from the event bus.

    All ``@listen`` handlers retry up to 3 times on transient failure.
    Exhausted events are routed to ``_dlq`` for manual inspection.

    **Lifecycle** — when using ``VarcoLifespan``, register this consumer
    AFTER the bus so subscriptions are created in the correct startup order
    and cancelled before the bus shuts down::

        lifespan.register(bus)
        lifespan.register(consumer)  # self._bus must be set in __init__

    **Standalone usage** (tests, scripts) — call ``register_to(bus)`` directly
    and manage subscription cancellation yourself.

    Thread safety:  ✅ ``register_to`` is called once at setup.
    Async safety:   ✅ Handlers are async — safe to await inside the event loop.

    Edge cases:
        - If the bus is unavailable at startup, ``start()`` raises immediately
          (``AbstractEventBus.subscribe`` may fail) — ``VarcoLifespan`` will
          roll back already-started components.
        - Registering the same consumer to two buses doubles all subscriptions —
          avoid unless intentional (e.g. fan-in from two channels).
    """

    def __init__(self, bus: Inject[AbstractEventBus]) -> None:
        """
        Args:
            bus: The event bus injected by the DI container.
                 Stored as ``self._bus`` so ``EventConsumer.start()`` can
                 call ``register_to(bus)`` during the lifespan startup phase.

        Edge cases:
            - DI injects ``AbstractEventBus`` — the concrete implementation
              (Redis, Kafka, In-Memory) is determined by which bus module
              is installed.  The consumer is bus-agnostic.
        """
        # Store on self so EventConsumer.start() can call register_to(self._bus).
        # This is the required convention for VarcoLifespan integration.
        self._bus = bus

    @listen(
        PostCreatedEvent,
        channel="posts",
        retry_policy=_RETRY_POLICY,
        dlq=_dlq,
    )
    async def on_post_created(self, event: PostCreatedEvent) -> None:
        """
        React to a newly created post.

        Examples of what to do here (not implemented — extend as needed):
        - Index the post in Elasticsearch / OpenSearch.
        - Send a notification to the author's followers.
        - Warm the post's cache entry.

        Args:
            event: ``PostCreatedEvent`` carrying ``post_id`` and ``author_id``.

        Raises:
            Any exception on transient failure → triggers retry via ``_RETRY_POLICY``.
            On exhaustion, event is routed to ``_dlq``.

        Edge cases:
            - The post IS durable in the DB when this fires (published after commit).
            - ``event.author_id`` is the verified JWT subject — safe to trust.
        """
        _logger.info(
            "PostEventConsumer: post created — post_id=%s author_id=%s",
            event.post_id,
            event.author_id,
        )
        # ── Add real side-effects here ────────────────────────────────────────
        # Example: await self._search_index.index(event.post_id)
        # Example: await self._notifier.notify_followers(event.author_id, event.post_id)

    @listen(
        PostDeletedEvent,
        channel="posts",
        retry_policy=_RETRY_POLICY,
        dlq=_dlq,
    )
    async def on_post_deleted(self, event: PostDeletedEvent) -> None:
        """
        React to a deleted post.

        Examples:
        - Remove the post from the search index.
        - Invalidate the post in any secondary caches.
        - Audit-log the deletion.

        Args:
            event: ``PostDeletedEvent`` carrying ``post_id``.

        Edge cases:
            - The post is GONE from the DB when this fires — do not attempt
              to fetch it from the repository.
        """
        _logger.info(
            "PostEventConsumer: post deleted — post_id=%s",
            event.post_id,
        )
        # ── Add real side-effects here ────────────────────────────────────────
        # Example: await self._search_index.remove(event.post_id)


__all__ = ["PostEventConsumer"]
