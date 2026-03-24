"""
varco_core.event.base
=====================
Core event system primitives.

Provides the ``Event`` base class, ``AbstractEventBus``, ``ErrorPolicy``,
``Subscription``, ``EventMiddleware``, and channel routing constants.

This module is the lowest layer of the event system — it defines interfaces
only.  No concrete implementations live here.  User-facing code should depend
on ``AbstractEventProducer`` (from ``event.producer``) and ``EventConsumer``
(from ``event.consumer``), NOT on ``AbstractEventBus`` directly.

Layer map::

    User code (services, handlers)
        ↓ depends on
    AbstractEventProducer  /  EventConsumer
        ↓ delegates to
    AbstractEventBus                    ← THIS MODULE
        ↓ implemented by
    InMemoryEventBus / KafkaEventBus / RedisEventBus

Channel routing
---------------
Every ``publish`` call targets a single named channel string.  Subscribers
filter by channel at subscription time:

- ``channel="orders"``   — receive only events published to ``"orders"``
- ``channel="*"``        — receive events from ALL channels (wildcard)
- no ``channel`` arg     — defaults to wildcard (same as ``"*"``)

The special constant ``CHANNEL_ALL = "*"`` is the canonical wildcard.
The constant ``CHANNEL_DEFAULT = "default"`` is used when the producer
does not specify a channel.

Error policies
--------------
``ErrorPolicy.COLLECT_ALL`` (default)
    All handlers always run.  If one or more raise, errors are collected
    and re-raised as an ``ExceptionGroup`` after all handlers have run.
    This is the safest default — no handler is ever skipped because a
    peer failed.

``ErrorPolicy.FAIL_FAST``
    The first handler error is re-raised immediately.  Remaining handlers
    are skipped.  Use when strict ordering matters and partial execution
    is worse than no execution.

``ErrorPolicy.FIRE_FORGET``
    Errors are logged at WARNING level and never propagated.  Use for
    non-critical side-effects (audit logs, metrics) where handler failure
    must not affect the caller.

Thread safety:  ⚠️  ``AbstractEventBus`` implementations must document
                    their own thread safety.  ``InMemoryEventBus`` is NOT
                    thread-safe (no locking on subscription list).
Async safety:   ✅  ``publish`` is ``async def`` — safe to await from any
                    coroutine.  Sync handlers are called directly (no
                    ``run_in_executor``); async handlers are awaited.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Final
from uuid import UUID, uuid4

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    # EventConsumer is only needed for the type hint on register_consumer —
    # importing it at runtime would create a circular dependency between
    # base.py and consumer.py (consumer imports base).
    from varco_core.event.consumer import EventConsumer

_logger = logging.getLogger(__name__)

# ── Channel routing constants ─────────────────────────────────────────────────

# Wildcard sentinel — subscribe with this channel to receive events published
# to ANY channel.  Compared by identity and value in bus dispatch logic.
CHANNEL_ALL: Final[str] = "*"

# Default channel used by producers when no channel is explicitly specified.
# Consumers that want only "unrouted" events subscribe to this channel;
# consumers that want everything subscribe to CHANNEL_ALL.
CHANNEL_DEFAULT: Final[str] = "default"


# ── ErrorPolicy ───────────────────────────────────────────────────────────────


class ErrorPolicy(str, Enum):
    """
    Controls how ``AbstractEventBus`` handles exceptions raised by handlers.

    Choose the policy that matches the failure semantics of your handlers:

    - ``COLLECT_ALL`` (default) — safest; no handler is ever skipped.
    - ``FAIL_FAST`` — strict; first error wins.
    - ``FIRE_FORGET`` — lenient; errors are logged and swallowed.

    Interaction with ``DispatchMode.BACKGROUND``
    --------------------------------------------
    In ``BACKGROUND`` mode the publisher has already returned by the time
    handlers run, so errors can never be raised to the caller.  The policy
    still controls *which* handlers run and *what is logged*:

    - ``COLLECT_ALL`` — all handlers run; any errors are logged together.
    - ``FAIL_FAST`` — first error is logged; remaining handlers are skipped.
    - ``FIRE_FORGET`` — errors are logged individually and swallowed (same as sync).

    DESIGN: enum over a plain string
        ✅ Exhaustive — IDEs and type checkers catch invalid policy names.
        ✅ Importable constant — no stringly-typed comparisons in bus code.
        ❌ Slightly more verbose than a string literal — justified by safety.
    """

    COLLECT_ALL = "collect_all"
    """Run all handlers; collect errors; raise ``ExceptionGroup`` if any failed.
    In BACKGROUND mode: all handlers run; errors are logged, not raised."""

    FAIL_FAST = "fail_fast"
    """Stop at first handler error and propagate it immediately.
    In BACKGROUND mode: first error is logged; remaining handlers are skipped."""

    FIRE_FORGET = "fire_forget"
    """Log handler errors at WARNING level; never propagate them."""


class DispatchMode(str, Enum):
    """
    Controls whether ``AbstractEventBus.publish`` waits for handlers to complete.

    ``SYNC`` (default for tests)
        ``publish()`` awaits all handlers before returning.  The caller
        blocks until every handler finishes.  Use in tests so that assertions
        can inspect handler side-effects immediately after ``publish`` returns.

    ``BACKGROUND`` (recommended for production)
        ``publish()`` schedules handlers as a background ``asyncio.Task`` and
        returns immediately.  The caller is never blocked by handler latency.
        Use in production so that event side-effects do not add latency to the
        HTTP response path.

        In ``BACKGROUND`` mode, call ``await bus.drain()`` in tests to wait
        for all pending tasks before making assertions.

    DESIGN: explicit mode over always-background
        ✅ Tests can use ``SYNC`` for straightforward assert-after-publish
           patterns without needing ``drain()``.
        ✅ Production uses ``BACKGROUND`` so HTTP response time is never
           inflated by slow handlers.
        ❌ Two modes to document and reason about — justified because the
           semantics differ meaningfully between test and production use.

    Note: Kafka and Redis buses are inherently ``BACKGROUND`` — the broker
    receives the message and consumers process it asynchronously.
    """

    SYNC = "sync"
    """Await all handlers before ``publish()`` returns."""

    BACKGROUND = "background"
    """Schedule handlers as a background task; ``publish()`` returns immediately."""


# ── ChannelConfig ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ChannelConfig:
    """
    Immutable configuration options for a declared event channel.

    Most fields are specific to broker-backed buses (Kafka, Redis Streams).
    ``InMemoryEventBus`` accepts and stores these values but does not act on
    them — they exist so channel declarations can be written once and shared
    across backends without branching.

    Attributes:
        num_partitions:     Kafka — number of partitions for the topic.
                            Defaults to ``1``.  Ignored by Redis/InMemory.
        replication_factor: Kafka — number of replicas.
                            Defaults to ``1``.  Ignored by Redis/InMemory.
        retention_ms:       Kafka — how long to retain messages in the topic
                            (milliseconds).  ``None`` uses the broker default.
                            Ignored by Redis/InMemory.
        extra:              Backend-specific key/value settings forwarded
                            verbatim.  E.g. for Kafka:
                            ``{"cleanup.policy": "compact"}``.

    DESIGN: shared ChannelConfig vs. backend-specific config objects
        ✅ One declaration — works across all buses without if/else branches.
        ✅ Unknown fields are silently stored in ``extra`` — forward-compatible.
        ❌ Kafka-only fields are visible even on InMemoryEventBus — justified
           because ignoring unknown config is better than breaking declarations.

    Example::

        from varco_kafka import KafkaChannelManager, KafkaChannelManagerSettings

        config = ChannelConfig(
            num_partitions=6,
            replication_factor=3,
            retention_ms=7 * 24 * 3600 * 1000,   # 7 days
        )
        async with KafkaChannelManager(KafkaChannelManagerSettings()) as manager:
            await manager.declare_channel("orders", config=config)
    """

    num_partitions: int = 1
    """Kafka: number of partitions.  Ignored by other backends."""

    replication_factor: int = 1
    """Kafka: replication factor.  Ignored by other backends."""

    retention_ms: int | None = None
    """Kafka: message retention in milliseconds.  ``None`` = broker default."""

    extra: dict[str, Any] = field(default_factory=dict)
    """Backend-specific key/value config forwarded verbatim."""


# ── Event ─────────────────────────────────────────────────────────────────────


def _utcnow() -> datetime:
    # Timezone-aware UTC — avoids the deprecation warning from datetime.utcnow()
    return datetime.now(tz=timezone.utc)


class Event(BaseModel, frozen=True):
    """
    Immutable base class for all events in the system.

    Subclass this to define domain-specific or application-specific events.
    The subclass IS the event type identifier — no registration step required.
    For serialization (Kafka, Redis), declare the optional ``__event_type__``
    class variable to give the event a stable string name::

        class OrderPlacedEvent(Event):
            __event_type__ = "order.placed"   # optional — defaults to class name
            order_id: str
            total: float

    ``event_id`` and ``timestamp`` are populated automatically — callers do
    not need to supply them.

    DESIGN: Pydantic frozen model over a dataclass
        ✅ Free JSON serialization via ``model_dump()`` / ``model_json()`` —
           required for Kafka/Redis backends that serialize to bytes.
        ✅ Field validation at construction — invalid event payloads fail fast.
        ✅ ``frozen=True`` → immutable, hashable, safe to share across tasks.
        ❌ Slightly heavier than a plain dataclass — justified by serialization.

    Thread safety:  ✅ Immutable — safe to share across threads and tasks.
    Async safety:   ✅ Immutable — no mutable state.

    Attributes:
        event_id:  Auto-generated UUID4.  Used for idempotency checks in
                   Kafka/Redis consumers.
        timestamp: UTC datetime at construction time.

    Edge cases:
        - ``__event_type__`` is a ``ClassVar`` — Pydantic ignores it as a
          field so it is NOT included in ``model_dump()`` output.  Use
          ``type(event).__event_type__`` to access the string name.
        - Two ``Event`` instances with the same payload but different
          ``event_id`` values are NOT equal — identity is unique per instance.
    """

    # ClassVar — Pydantic ignores ClassVar fields during validation and
    # serialization.  Subclasses override this with a stable string name
    # for serialization-friendly dispatch.
    __event_type__: ClassVar[str]

    # Global registry mapping event_type_name → Event subclass.
    # Populated automatically by __init_subclass__ — enables JsonEventSerializer
    # to deserialize events from bytes without a manual registration step.
    # DESIGN: dict on the base class (not each subclass) — Pydantic creates a
    # fresh namespace per subclass, so reading cls._registry would see an empty
    # dict.  Writing to Event._registry directly mutates the one shared dict.
    _registry: ClassVar[dict[str, type[Event]]] = {}

    event_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=_utcnow)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        Auto-register every ``Event`` subclass in the global type registry.

        Called by Python at class-definition time whenever a new class
        inherits from ``Event`` (directly or indirectly).

        Registered under ``cls.event_type_name()`` — which returns
        ``__event_type__`` if declared, otherwise ``cls.__name__``.
        ``JsonEventSerializer`` uses this registry to deserialize events
        from bytes without a manual ``register()`` call.

        Edge cases:
            - Classes without ``__event_type__`` are registered under their
              ``__name__``.  Renaming the class changes the key — use
              ``__event_type__`` for any event that crosses a process boundary.
            - Registering two classes with the same ``event_type_name``
              overwrites the first.  Avoid duplicate names across subclasses.

        Args:
            **kwargs: Forwarded to ``super().__init_subclass__``.
        """
        super().__init_subclass__(**kwargs)
        # Write to Event._registry — the shared dict on the base class.
        # cls.event_type_name() reads __event_type__ if declared, else __name__.
        Event._registry[cls.event_type_name()] = cls

    @classmethod
    def event_type_name(cls) -> str:
        """
        Return the string name for this event type.

        Returns ``__event_type__`` if declared, otherwise falls back to the
        class name.  Backends (Kafka, Redis) use this for topic/channel routing
        and for deserializing events from bytes.

        Returns:
            String event type name, e.g. ``"order.placed"`` or ``"OrderPlacedEvent"``.

        Edge cases:
            - Subclasses that do not declare ``__event_type__`` return their
              ``__name__``.  This is stable within a process but NOT across
              renames — declare ``__event_type__`` explicitly for any event
              that crosses a process boundary.
        """
        return getattr(cls, "__event_type__", cls.__name__)


# ── Subscription ──────────────────────────────────────────────────────────────


class Subscription:
    """
    Handle returned by ``AbstractEventBus.subscribe()``.

    Calling ``cancel()`` deregisters the handler from the bus.  After
    cancellation, the handler will no longer receive events.  Safe to call
    multiple times — subsequent calls are no-ops.

    DESIGN: handle object over unsubscribe(handler) method
        ✅ Handler functions can be lambdas or closures — no stable identity
           required.  ``unsubscribe(handler)`` would require identity
           comparison which breaks for lambdas.
        ✅ Cancellation is O(1) via a flag — no list scan needed.
        ✅ Callers can hold the handle and cancel from any scope.
        ❌ Caller must retain the handle — if it is discarded, the subscription
           cannot be cancelled programmatically (only bus shutdown clears all).

    Thread safety:  ✅ ``cancel()`` sets a boolean flag — GIL makes this safe.
    Async safety:   ✅ ``cancel()`` is synchronous and non-blocking.

    Attributes:
        is_cancelled: ``True`` after ``cancel()`` has been called.

    Edge cases:
        - Calling ``cancel()`` during event dispatch is safe — the bus
          checks ``is_cancelled`` on each dispatch iteration.
        - Calling ``cancel()`` after the bus is shut down is a no-op.
    """

    __slots__ = ("_entry",)

    def __init__(self, entry: _SubscriptionEntry) -> None:
        """
        Args:
            entry: The internal subscription entry managed by the bus.
                   The entry's ``cancelled`` flag is set by ``cancel()``.
        """
        # Store entry reference so cancel() sets the flag on the same object
        # the bus iterates — avoids any bus-level lookup.
        self._entry = entry

    def cancel(self) -> None:
        """
        Deregister this subscription.

        Idempotent — safe to call multiple times.

        Edge cases:
            - Calling during an active dispatch is safe — the bus skips
              cancelled entries without raising.
        """
        self._entry.cancelled = True

    @property
    def is_cancelled(self) -> bool:
        """``True`` if ``cancel()`` has been called."""
        return self._entry.cancelled

    def __repr__(self) -> str:
        state = "cancelled" if self.is_cancelled else "active"
        return f"Subscription({state})"


# ── Internal subscription entry (not part of public API) ─────────────────────


class _SubscriptionEntry:
    """
    Internal mutable record for a single handler subscription.

    Holds all routing data alongside a ``cancelled`` flag so ``Subscription``
    handles can deregister without mutating the bus's subscription list
    (which avoids list-resize bugs during iteration).

    DESIGN: flag over list removal
        ✅ ``cancel()`` during dispatch is safe — no list mutation mid-loop.
        ✅ O(1) cancel — no scan required.
        ❌ Cancelled entries accumulate; they are flushed at publish time.
           For typical usage (hundreds of subscriptions) this is negligible.

    Priority
    --------
    Higher ``priority`` values run first.  Default is ``0``.  Handlers with
    equal priority run in subscription order (FIFO).  The bus sorts matching
    entries by priority descending before dispatching.
    """

    __slots__ = ("event_type", "channel", "handler", "filter", "priority", "cancelled")

    def __init__(
        self,
        event_type: type[Event] | str,
        channel: str,
        handler: Callable[[Event], Awaitable[None] | None],
        filter: Callable[[Event], bool] | None,  # noqa: A002 — intentional shadowing
        priority: int = 0,
    ) -> None:
        self.event_type = event_type
        self.channel = channel
        self.handler = handler
        self.filter = filter
        # Higher = runs first.  Default 0 — neutral priority.
        self.priority: int = priority
        # Starts False — set to True by Subscription.cancel()
        self.cancelled: bool = False


# ── EventMiddleware ───────────────────────────────────────────────────────────


class EventMiddleware(ABC):
    """
    Abstract base for bus-level middleware.

    Middleware wraps the full dispatch pipeline.  Each middleware receives
    the event, the target channel, and a ``next`` callable that continues
    the chain.  Calling ``await next(event, channel)`` passes control to
    the next middleware (or to the final handler dispatch if at the end of
    the chain).

    DESIGN: ASGI-style middleware over a decorator pattern
        ✅ Middleware can modify the event or channel before passing on.
        ✅ Middleware can short-circuit by not calling ``next``.
        ✅ Consistent with widely-understood ASGI/WSGI patterns.
        ❌ Slightly more ceremony than a simple callback — justified by
           the power of pre/post hooks and the ability to suppress dispatch.

    Thread safety:  ✅ Middleware instances are stateless by convention.
    Async safety:   ✅ ``__call__`` is ``async def`` — awaited by the bus.

    Example — logging middleware::

        class LoggingMiddleware(EventMiddleware):
            async def __call__(self, event, channel, next):
                logger.info("publishing %s to %s", type(event).__name__, channel)
                await next(event, channel)
                logger.info("published %s", type(event).__name__)
    """

    @abstractmethod
    async def __call__(
        self,
        event: Event,
        channel: str,
        next: Callable[[Event, str], Awaitable[None]],
    ) -> None:
        """
        Process an event before and/or after the rest of the chain.

        Args:
            event:   The event being published.
            channel: The target channel.
            next:    Callable that continues the middleware chain.  Must be
                     awaited to pass the event to downstream middleware and
                     ultimately to the handlers.

        Edge cases:
            - Not calling ``next`` suppresses all downstream handlers.
            - Passing a different ``event`` or ``channel`` to ``next``
              redirects the event — useful for enrichment or routing.
        """


# ── AbstractEventBus ──────────────────────────────────────────────────────────


class AbstractEventBus(ABC):
    """
    Low-level event bus interface.

    This is an infrastructure-level abstraction.  **User code should NOT
    depend on this directly.**  Use ``AbstractEventProducer`` for publishing
    and ``EventConsumer`` with the ``@listen`` decorator for consuming.

    Implementations:
        - ``InMemoryEventBus`` — synchronous, in-process, for tests / dev.
        - ``KafkaEventBus`` (``varco_kafka``) — async Kafka producer/consumer.
        - ``RedisEventBus`` (``varco_redis``) — async Redis Pub/Sub.

    Channel management (topic creation, deletion) is intentionally NOT part
    of this interface.  Use ``ChannelManager`` (``varco_core.event.channel``)
    for admin-level operations that require elevated privileges.

    Subscription dispatch rules
    ---------------------------
    A handler is called when ALL of the following match:

    1. ``event_type`` — by class (``isinstance`` check, supports inheritance)
       OR by ``__event_type__`` string match.
    2. ``channel`` — subscriber's ``channel`` is ``CHANNEL_ALL`` (``"*"``)
       OR equals the channel the event was published to.
    3. ``filter`` — the optional predicate returns ``True`` for this event
       (or no predicate was provided).

    Thread safety:  ⚠️  Implementations must document their own threading
                        guarantees.
    Async safety:   ✅  ``publish`` is ``async def``.

    Edge cases:
        - ``publish_many`` publishes events sequentially by default.
          Implementations may override it for batching efficiency (Kafka).
        - Subscribing to a parent class (e.g. ``EntityEvent``) receives ALL
          subclass events (``EntityCreatedEvent``, ``EntityUpdatedEvent``, …).
        - The ``on()`` decorator wires a standalone function to this specific
          bus instance — it is NOT suitable for dependency-injected buses
          (the bus instance must be known at decoration time).
    """

    @abstractmethod
    async def publish(
        self,
        event: Event,
        *,
        channel: str = CHANNEL_DEFAULT,
    ) -> asyncio.Task[None] | None:
        """
        Publish ``event`` to the given ``channel``.

        Dispatches to all matching handlers according to the subscription
        dispatch rules above.  Handlers are called in subscription order.

        Args:
            event:   The event to publish.
            channel: Target channel.  Defaults to ``CHANNEL_DEFAULT``.
                     Only ONE channel per publish call is supported — see
                     design docs for rationale.

        Raises:
            ExceptionGroup: (``COLLECT_ALL`` policy) One or more handlers
                            raised.  All handlers ran before this is raised.
            Exception:      (``FAIL_FAST`` policy) First handler error.
                            Remaining handlers are NOT called.

        Edge cases:
            - ``FIRE_FORGET`` policy swallows all handler errors — callers
              will never see them regardless of handler behaviour.
            - Publishing to a channel with no subscribers is a no-op.
        """

    @abstractmethod
    def subscribe(
        self,
        event_type: type[Event] | str,
        handler: Callable[[Event], Awaitable[None] | None],
        *,
        channel: str = CHANNEL_ALL,
        filter: Callable[[Event], bool] | None = None,  # noqa: A002
        priority: int = 0,
    ) -> Subscription:
        """
        Register a handler for events matching ``event_type`` and ``channel``.

        Args:
            event_type: A concrete ``Event`` subclass (class-based dispatch,
                        supports inheritance) OR a ``__event_type__`` string
                        (string-based dispatch, exact match only).
            handler:    Async or sync callable invoked with the event.
                        Sync handlers are called directly (not in an executor).
            channel:    Channel filter.  ``CHANNEL_ALL`` (``"*"``) matches
                        any publish channel.  Defaults to ``CHANNEL_ALL``.
            filter:     Optional predicate for fine-grained filtering.  Called
                        only when ``event_type`` and ``channel`` already match.

        Returns:
            A ``Subscription`` handle.  Call ``.cancel()`` to deregister.

        Edge cases:
            - Subscribing to a parent class receives subclass events too.
            - The same handler can be subscribed multiple times — each
              subscription is independent and produces its own ``Subscription``
              handle.
            - Async and sync handlers are both accepted; the bus detects
              coroutine functions at dispatch time.
            - Higher ``priority`` values run first.  Equal priorities run in
              subscription order (FIFO).
        """

    async def publish_many(
        self,
        events: list[tuple[Event, str]],
    ) -> None:
        """
        Publish multiple ``(event, channel)`` pairs sequentially.

        The default implementation is a simple loop over ``publish()``.
        Override in Kafka/Redis implementations for batching efficiency.

        Args:
            events: List of ``(event, channel)`` tuples to publish in order.

        Raises:
            Any exception raised by ``publish()`` for any individual event.

        Edge cases:
            - An empty list is a no-op.
            - Errors on one event do NOT prevent subsequent events from
              being published in the default implementation.
        """
        for event, channel in events:
            await self.publish(event, channel=channel)

    def on(
        self,
        *event_types: type[Event] | str,
        channel: str = CHANNEL_ALL,
        filter: Callable[[Event], bool] | None = None,  # noqa: A002
    ) -> Callable:
        """
        Decorator that registers a standalone function as an event handler.

        Unlike ``EventConsumer.register_to(bus)``, this binds a function
        directly to THIS bus instance at decoration time.  Use ``EventConsumer``
        + ``@listen`` for dependency-injected buses where the bus instance is
        not known at class-definition time.

        Args:
            *event_types: One or more event types (class or string) to
                          subscribe to.
            channel:      Channel filter.  Defaults to ``CHANNEL_ALL``.
            filter:       Optional predicate for fine-grained filtering.

        Returns:
            The unmodified function (decorator does not wrap it).

        Example::

            @bus.on(OrderPlacedEvent, channel="orders")
            async def handle_order(event: OrderPlacedEvent) -> None:
                ...

        Edge cases:
            - Registers ALL ``event_types`` as separate subscriptions.
            - The function is returned unchanged — no wrapping occurs.
        """

        def decorator(func: Callable) -> Callable:
            for et in event_types:
                # Each event_type gets its own independent subscription
                self.subscribe(et, func, channel=channel, filter=filter)
            return func

        return decorator

    def register_consumer(self, consumer: EventConsumer) -> list[Subscription]:
        """
        Register all ``@listen``-decorated methods of ``consumer`` to this bus.

        Delegates to ``consumer.register_to(self)``.  Provided as a convenience
        so wiring code can be written from the bus side::

            bus.register_consumer(notification_consumer)
            # equivalent to:
            notification_consumer.register_to(bus)

        Args:
            consumer: An ``EventConsumer`` instance.

        Returns:
            List of ``Subscription`` handles — one per ``@listen`` entry found.

        Edge cases:
            - If ``consumer`` has no ``@listen`` methods, an empty list is
              returned and no subscriptions are created.
        """
        return consumer.register_to(self)
