"""
varco_kafka.bus
===============
Apache Kafka implementation of ``AbstractEventBus``.

``KafkaEventBus`` publishes events as JSON bytes to Kafka topics (one topic
per channel) and consumes messages from those topics in a background
``asyncio.Task``.  Local handler dispatch reuses the same priority-sorted
matching logic as ``InMemoryEventBus``.

Architecture
------------
::

    Publisher side:
        bus.publish(event, channel="orders")
            → JsonEventSerializer.serialize(event)
            → AIOKafkaProducer.send_and_wait(topic="orders", value=bytes)

    Consumer side (background):
        AIOKafkaConsumer.poll()
            → JsonEventSerializer.deserialize(bytes)
            → local _dispatch(event, channel)
                → priority-sorted matching handler calls

Topic naming
------------
Each channel maps to one Kafka topic.  ``KafkaEventBusSettings.topic_name(channel)``
applies the optional ``topic_prefix``, e.g.::

    channel = "orders"  →  topic = "prod.orders"  (with prefix "prod.")

Lifecycle
---------
``KafkaEventBus`` must be started and stopped explicitly::

    bus = KafkaEventBus(config)
    await bus.start()      # connects producer and starts consumer task

    # ... use the bus ...

    await bus.stop()       # flushes producer, cancels consumer task

Or use it as an async context manager::

    async with KafkaEventBus(config) as bus:
        ...

DESIGN: single consumer task consuming ALL topics over injected subscriptions
    ✅ One consumer group — messages consumed once per group instance.
    ✅ Topics subscribed dynamically as ``subscribe()`` is called.
    ✅ Local handler dispatch reuses the same priority / filter logic.
    ❌ Consumer task must restart when new topics are added after ``start()``
       — ``subscribe()`` after ``start()`` updates the topic list; the
       consumer is re-subscribed automatically via ``assign()`` override.
    ❌ No built-in dead-letter queue — handler errors are logged.  Implement
       retry logic in the handler itself.

Thread safety:  ❌  Not thread-safe.  All access must be from the same event loop.
Async safety:   ✅  ``publish`` and ``start``/``stop`` are ``async def``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

# aiokafka is a hard dependency of this package — imported at module level so
# unit tests can patch varco_kafka.bus.AIOKafkaProducer etc. without needing
# to reach into the aiokafka namespace directly.
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from providify import PreDestroy

from varco_core.event.base import (
    CHANNEL_ALL,
    CHANNEL_DEFAULT,
    AbstractEventBus,
    ErrorPolicy,
    Event,
    EventMiddleware,
    Subscription,
    _SubscriptionEntry,
)
from varco_core.event.serializer import EventSerializer, JsonEventSerializer

from varco_kafka.config import KafkaEventBusSettings

_logger = logging.getLogger(__name__)


class KafkaEventBus(AbstractEventBus):
    """
    ``AbstractEventBus`` backed by Apache Kafka via ``aiokafka``.

    Published events are serialized to JSON and sent to a Kafka topic
    named after the channel.  A background consumer task reads from all
    subscribed topics and dispatches to locally registered handlers.

    Args:
        config:        Kafka connection and routing configuration.
        error_policy:  Controls handler error behaviour on the consumer side.
                       Defaults to ``ErrorPolicy.COLLECT_ALL``.
        middleware:    Optional list of ``EventMiddleware`` instances applied
                       before local handler dispatch.

    Lifecycle:
        ``start()`` / ``stop()`` — explicit lifecycle management.
        Or use as an async context manager (``async with KafkaEventBus(...) as bus``).

    Thread safety:  ❌  Not thread-safe — use from a single event loop only.
    Async safety:   ✅  All async methods are safe to await.

    Example::

        config = KafkaEventBusSettings(bootstrap_servers="localhost:9092", group_id="svc-a")
        async with KafkaEventBus(config) as bus:
            bus.subscribe(OrderPlacedEvent, my_handler, channel="orders")
            await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")

    Edge cases:
        - Calling ``publish()`` before ``start()`` raises ``RuntimeError``.
        - ``subscribe()`` called after ``start()`` is safe — the consumer is
          re-subscribed to include the new topic automatically.
        - Events published to channels with no local subscribers are still
          sent to Kafka (other services may consume them).
        - Consumer errors (deserialization, handler exceptions) are logged
          but do NOT stop the consumer loop.
    """

    def __init__(
        self,
        config: KafkaEventBusSettings | None = None,
        *,
        error_policy: ErrorPolicy = ErrorPolicy.COLLECT_ALL,
        middleware: list[EventMiddleware] | None = None,
        serializer: EventSerializer | None = None,
    ) -> None:
        """
        Args:
            config:       Kafka settings.  Defaults to ``KafkaEventBusSettings()``
                          (localhost, reads from ``VARCO_KAFKA_*`` env vars).
            error_policy: Handler error policy.
            middleware:   Ordered middleware list (index 0 is outermost).
            serializer:   Pluggable event serializer.  Defaults to
                          ``JsonEventSerializer()``.  Swap in a custom
                          ``Serializer[Event]`` for msgpack, protobuf, etc.
        """
        self._config = config or KafkaEventBusSettings()
        self._error_policy = error_policy
        self._middleware: list[EventMiddleware] = middleware or []

        # Use the provided serializer or fall back to JSON.
        # Stored as an instance so it is pluggable and stateful serializers
        # (e.g. ones that cache TypeAdapters) work correctly.
        self._serializer: EventSerializer = serializer or JsonEventSerializer()

        # Local subscription list — same model as InMemoryEventBus.
        # Handler dispatch happens in-process after a message arrives from Kafka.
        self._subscriptions: list[_SubscriptionEntry] = []

        # Set of topics the consumer is currently assigned to.
        # Updated whenever subscribe() is called with a specific channel.
        self._subscribed_topics: set[str] = set()

        # Producer and consumer are created in start() — not here,
        # because aiokafka objects must be created inside a running event loop.
        # Channel management (admin client) is now handled by KafkaChannelManager.
        self._producer: Any | None = None
        self._consumer: Any | None = None
        self._consumer_task: asyncio.Task | None = None
        self._started = False

        # Pre-build middleware chain — same approach as InMemoryEventBus.
        self._chain: Callable[[Event, str], Coroutine[Any, Any, None]] = (
            self._build_chain()
        )

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Connect the Kafka producer and start the background consumer task.

        Must be called before ``publish()``.  Idempotent — calling ``start()``
        twice on an already-started bus is a no-op.

        Raises:
            NoBrokersAvailable: If the configured ``bootstrap_servers`` is
                                unreachable (raised by aiokafka).

        Edge cases:
            - Calling ``start()`` with no subscriptions is valid — the consumer
              will have no topics to poll until ``subscribe()`` is called.
        """
        if self._started:
            return

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._config.bootstrap_servers,
            **self._config.producer_kwargs,
        )
        await self._producer.start()

        # Build consumer — subscribed topics are updated dynamically.
        # Subscribing to an empty list is valid; update_topics() adds more.
        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=self._config.group_id,
            auto_offset_reset=self._config.auto_offset_reset,
            enable_auto_commit=self._config.enable_auto_commit,
            **self._config.consumer_kwargs,
        )
        await self._consumer.start()

        # Subscribe to any topics already queued by subscribe() calls before start()
        if self._subscribed_topics:
            self._consumer.subscribe(list(self._subscribed_topics))

        self._consumer_task = asyncio.create_task(
            self._consume_loop(),
            name="kafka-consumer-loop",
        )
        self._started = True
        _logger.info(
            "KafkaEventBus started (brokers=%s, group=%s)",
            self._config.bootstrap_servers,
            self._config.group_id,
        )

    @PreDestroy
    async def stop(self) -> None:
        """
        Flush the producer, cancel the consumer task, and close connections.

        Idempotent — safe to call on a bus that was never started.

        Edge cases:
            - In-flight consumer messages being dispatched when ``stop()``
              is called will be interrupted on the next ``await`` point.
            - Pending producer messages are flushed before the producer stops.
        """
        if not self._started:
            return

        if self._consumer_task is not None:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass  # Expected — consumer task was cancelled by design

        if self._consumer is not None:
            await self._consumer.stop()

        if self._producer is not None:
            await self._producer.stop()

        self._started = False
        _logger.info("KafkaEventBus stopped.")

    async def __aenter__(self) -> KafkaEventBus:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.stop()

    # ── AbstractEventBus interface ─────────────────────────────────────────────

    async def publish(
        self,
        event: Event,
        *,
        channel: str = CHANNEL_DEFAULT,
    ) -> asyncio.Task[None] | None:
        """
        Serialize ``event`` and send it to the Kafka topic for ``channel``.

        Kafka delivery is inherently asynchronous — the consumer (this or
        another service) processes the message at a later point.  This method
        blocks until the broker acknowledges the message (``send_and_wait``).

        Args:
            event:   The event to publish.
            channel: Target channel.  Maps to a Kafka topic via
                     ``KafkaEventBusSettings.topic_name(channel)``.

        Returns:
            ``None`` — Kafka is always background; there is no local task.

        Raises:
            RuntimeError:     If called before ``start()``.
            KafkaTimeoutError: (aiokafka) If the broker does not respond.

        Edge cases:
            - Events published before any subscriber exists are still sent to
              Kafka — they may be consumed by another service or picked up
              after a subscriber is added (depending on ``auto_offset_reset``).
        """
        if self._producer is None:
            raise RuntimeError(
                "KafkaEventBus.publish() called before start(). "
                "Call await bus.start() or use 'async with bus' first."
            )

        topic = self._config.topic_name(channel)
        value = self._serializer.serialize(event)
        await self._producer.send_and_wait(topic, value=value)
        _logger.debug("Published %s to topic %s", type(event).__name__, topic)
        # Return None — Kafka delivery is always async (broker-side), no local Task.
        return None

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
        Register a local handler for matching events arriving from Kafka.

        If ``channel`` is a specific channel (not ``CHANNEL_ALL``), the
        corresponding Kafka topic is added to the consumer's subscription.

        Args:
            event_type: ``Event`` subclass or ``__event_type__`` string.
            handler:    Async or sync callable invoked on matching events.
            channel:    Channel filter.  If not ``CHANNEL_ALL``, the Kafka
                        topic for this channel is auto-subscribed.
            filter:     Optional predicate for fine-grained filtering.
            priority:   Dispatch order — higher runs first.

        Returns:
            A ``Subscription`` handle.

        Edge cases:
            - Subscribing with ``channel=CHANNEL_ALL`` does NOT add any Kafka
              topic to the consumer — the handler receives events dispatched
              locally for any topic the consumer is already subscribed to.
            - Adding a new channel-specific subscription after ``start()`` is
              safe — the consumer is re-subscribed automatically.
        """
        entry = _SubscriptionEntry(
            event_type=event_type,
            channel=channel,
            handler=handler,
            filter=filter,
            priority=priority,
        )
        self._subscriptions.append(entry)

        # Register the Kafka topic when a specific channel is requested.
        # CHANNEL_ALL ("*") is not a valid Kafka topic name — only register
        # concrete channel names.
        if channel != CHANNEL_ALL:
            topic = self._config.topic_name(channel)
            if topic not in self._subscribed_topics:
                self._subscribed_topics.add(topic)
                # If the consumer is already running, update its subscription.
                if self._consumer is not None:
                    self._consumer.subscribe(list(self._subscribed_topics))
                    _logger.debug(
                        "Consumer re-subscribed to topics: %s", self._subscribed_topics
                    )

        return Subscription(entry)

    # ── Consumer loop ──────────────────────────────────────────────────────────

    async def _consume_loop(self) -> None:
        """
        Background task that polls Kafka and dispatches received messages.

        Runs until cancelled (by ``stop()``).  Errors during message
        processing are logged but never stop the loop — a single bad message
        should not bring down the consumer.

        Edge cases:
            - ``asyncio.CancelledError`` is not caught — it propagates cleanly
              to the task, which is expected behaviour on shutdown.
            - ``JsonEventSerializer.deserialize()`` raises ``KeyError`` if the
              event type is unknown (class not imported) — logged as WARNING,
              message is skipped.
        """
        assert self._consumer is not None

        _logger.debug("Kafka consumer loop started.")
        try:
            async for msg in self._consumer:
                try:
                    event = self._serializer.deserialize(msg.value)
                    # Topic name → channel: strip the prefix to recover the channel
                    channel = msg.topic.removeprefix(self._config.channel_prefix)
                    await self._chain(event, channel)
                except asyncio.CancelledError:
                    # Re-raise — CancelledError must propagate so the task ends.
                    raise
                except Exception as exc:  # noqa: BLE001
                    _logger.warning(
                        "Failed to process Kafka message from topic %s: %s",
                        msg.topic,
                        exc,
                        exc_info=True,
                    )
        except asyncio.CancelledError:
            _logger.debug("Kafka consumer loop cancelled.")
            raise

    # ── Dispatch helpers — mirror InMemoryEventBus ─────────────────────────────

    async def _dispatch(self, event: Event, channel: str) -> None:
        """
        Dispatch ``event`` to matching local handlers with error policy applied.

        Mirrors ``InMemoryEventBus._dispatch`` — priority-sorted, filter-aware,
        policy-controlled.

        Args:
            event:   The deserialized event from Kafka.
            channel: The channel derived from the Kafka topic name.
        """
        errors: list[BaseException] = []

        matching_entries = sorted(
            (
                e
                for e in self._subscriptions
                if not e.cancelled
                and self._matches_event_type(event, e.event_type)
                and self._matches_channel(channel, e.channel)
                and (e.filter is None or e.filter(event))
            ),
            key=lambda e: e.priority,
            reverse=True,
        )

        for entry in matching_entries:
            try:
                result = entry.handler(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:  # noqa: BLE001
                if self._error_policy is ErrorPolicy.FAIL_FAST:
                    raise
                elif self._error_policy is ErrorPolicy.COLLECT_ALL:
                    errors.append(exc)
                elif self._error_policy is ErrorPolicy.FIRE_FORGET:
                    _logger.warning(
                        "Kafka event handler %r raised and was ignored "
                        "(FIRE_FORGET): %s",
                        entry.handler,
                        exc,
                        exc_info=True,
                    )

        if errors:
            if len(errors) == 1:
                raise errors[0]
            raise ExceptionGroup(
                f"Kafka handlers raised {len(errors)} error(s) "
                f"for {type(event).__name__!r} on channel {channel!r}",
                errors,
            )

    def _build_chain(self) -> Callable[[Event, str], Coroutine[Any, Any, None]]:
        """Build the pre-middleware-chain coroutine function once at construction."""

        async def core(event: Event, channel: str) -> None:
            await self._dispatch(event, channel)

        chain: Callable[[Event, str], Coroutine[Any, Any, None]] = core
        for mw in reversed(self._middleware):

            def _make_step(
                _mw: EventMiddleware,
                _next: Callable[[Event, str], Coroutine[Any, Any, None]],
            ) -> Callable[[Event, str], Coroutine[Any, Any, None]]:
                async def step(event: Event, channel: str) -> None:
                    await _mw(event, channel, _next)

                return step

            chain = _make_step(mw, chain)
        return chain

    @staticmethod
    def _matches_event_type(event: Event, event_type: type[Event] | str) -> bool:
        if isinstance(event_type, type):
            return isinstance(event, event_type)
        declared_name = getattr(type(event), "__event_type__", type(event).__name__)
        return event_type == declared_name

    @staticmethod
    def _matches_channel(publish_channel: str, subscribe_channel: str) -> bool:
        return subscribe_channel == CHANNEL_ALL or subscribe_channel == publish_channel

    def __repr__(self) -> str:
        active = sum(1 for s in self._subscriptions if not s.cancelled)
        return (
            f"KafkaEventBus("
            f"brokers={self._config.bootstrap_servers!r}, "
            f"group={self._config.group_id!r}, "
            f"subscriptions={active}, "
            f"started={self._started})"
        )
