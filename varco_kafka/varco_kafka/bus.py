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
import sys
from collections.abc import Awaitable, Callable, Coroutine
from typing import Annotated, Any

# aiokafka is a hard dependency of this package — imported at module level so
# unit tests can patch varco_kafka.bus.AIOKafkaProducer etc. without needing
# to reach into the aiokafka namespace directly.
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.structs import OffsetAndMetadata

from providify import Inject, Instance, InjectMeta, PostConstruct, PreDestroy, Singleton

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

from varco_kafka.config import KafkaDeliverySemantics, KafkaEventBusSettings

_logger = logging.getLogger(__name__)


@Singleton(priority=-sys.maxsize, qualifier="kafka")
class KafkaEventBus(AbstractEventBus):
    """
    ``AbstractEventBus`` backed by Apache Kafka via ``aiokafka``.

    Published events are serialized to JSON and sent to a Kafka topic
    named after the channel.  A background consumer task reads from all
    subscribed topics and dispatches to locally registered handlers.

    Delivery semantics
    ------------------
    Controlled by ``KafkaEventBusSettings.delivery_semantics``:

    ``AT_LEAST_ONCE`` (default)
        Auto-commit enabled.  Offsets committed after dispatch.  On crash,
        the message is redelivered — handlers may see duplicates.

    ``AT_MOST_ONCE``
        Offsets committed manually **before** dispatch.  A crash between
        commit and dispatch loses the message permanently.  No duplicates.

    ``EXACTLY_ONCE``
        Transactional producer (``transactional_id`` required in settings).
        Consumer uses ``isolation_level=read_committed``.  After dispatch,
        offsets are committed inside the same Kafka transaction via
        ``send_offsets_to_transaction`` — atomically with any produced
        output events.  No duplicates, no message loss.  Throughput is lower
        than the other modes (transaction round-trips add latency).

    DESIGN: semantics-aware publish / consume over always-transactional
        ✅ AT_LEAST_ONCE has zero transaction overhead (default path).
        ✅ EXACTLY_ONCE is opt-in — only services that need it pay the cost.
        ❌ EXACTLY_ONCE requires a stable ``transactional_id`` per process.

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
        - ``EXACTLY_ONCE`` without ``transactional_id`` set raises
          ``ValueError`` at ``start()`` time.
    """

    def __init__(
        self,
        config: Inject[KafkaEventBusSettings],
        *,
        error_policy: ErrorPolicy = ErrorPolicy.COLLECT_ALL,
        middleware: Instance[EventMiddleware] | list[EventMiddleware] | None = None,
        serializer: Annotated[EventSerializer, InjectMeta(optional=True)] = None,
    ) -> None:
        """
        Args:
            config:       Kafka settings injected from the container.
            error_policy: Handler error policy.
            middleware:   DI instance handle for ``EventMiddleware`` bindings.
                          All registered middlewares are resolved via
                          ``middleware.get_all()`` when provided.
            serializer:   Pluggable event serializer.  Injected optionally;
                          defaults to ``JsonEventSerializer()`` when absent.
        """
        self._config = config
        self._error_policy = error_policy
        # Support both DI-injected Instance[EventMiddleware] and direct list
        # construction (used in tests and non-DI usage patterns).
        if middleware is None:
            self._middleware: list[EventMiddleware] = []
        elif isinstance(middleware, list):
            self._middleware = middleware
        else:
            self._middleware = (
                list(middleware.get_all()) if middleware.resolvable() else []
            )

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

    @PostConstruct
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

        semantics = self._config.delivery_semantics

        if semantics == KafkaDeliverySemantics.EXACTLY_ONCE:
            txn_id = self._config.transactional_id
            if not txn_id:
                raise ValueError(
                    "KafkaEventBus: delivery_semantics=EXACTLY_ONCE requires "
                    "'transactional_id' to be set in KafkaEventBusSettings.  "
                    "Set a stable, per-process unique ID "
                    "(e.g. 'my-service-{hostname}-{partition}')."
                )
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._config.bootstrap_servers,
                transactional_id=txn_id,
                enable_idempotence=True,
                **self._config.producer_kwargs,
            )
        else:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._config.bootstrap_servers,
                **self._config.producer_kwargs,
            )
        await self._producer.start()

        # Consumer options differ per semantics:
        #   AT_MOST_ONCE  → auto_commit=False (we commit manually before dispatch)
        #   AT_LEAST_ONCE → auto_commit per settings (default True)
        #   EXACTLY_ONCE  → auto_commit=False + read_committed isolation
        if semantics == KafkaDeliverySemantics.EXACTLY_ONCE:
            consumer_kwargs = {
                "isolation_level": "read_committed",
                "enable_auto_commit": False,
                **self._config.consumer_kwargs,
            }
        elif semantics == KafkaDeliverySemantics.AT_MOST_ONCE:
            consumer_kwargs = {
                "enable_auto_commit": False,
                **self._config.consumer_kwargs,
            }
        else:
            # AT_LEAST_ONCE — honour the config value (default True).
            consumer_kwargs = {
                "enable_auto_commit": self._config.enable_auto_commit,
                **self._config.consumer_kwargs,
            }

        self._consumer = AIOKafkaConsumer(
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=self._config.group_id,
            auto_offset_reset=self._config.auto_offset_reset,
            **consumer_kwargs,
        )
        await self._consumer.start()

        # Subscribe to any topics already queued by subscribe() calls before start().
        if self._subscribed_topics:
            self._consumer.subscribe(list(self._subscribed_topics))

        self._consumer_task = asyncio.create_task(
            self._consume_loop(),
            name="kafka-consumer-loop",
        )
        self._started = True
        _logger.info(
            "KafkaEventBus started (brokers=%s, group=%s, semantics=%s)",
            self._config.bootstrap_servers,
            self._config.group_id,
            semantics.value,
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

        if self._config.delivery_semantics == KafkaDeliverySemantics.EXACTLY_ONCE:
            # Each standalone publish is wrapped in its own transaction.
            # For atomic multi-event publishes, callers should use the
            # producer's transaction() context manager directly via
            # ``bus._producer.transaction()``.
            async with self._producer.transaction():
                await self._producer.send_and_wait(topic, value=value)
        else:
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

        Semantics-specific behaviour:

        ``AT_LEAST_ONCE`` (default)
            Auto-commit handles offset progression.  On error, the message
            is logged and skipped (no retry at the bus level).

        ``AT_MOST_ONCE``
            Offset committed manually **before** dispatch via
            ``consumer.commit()``.  A crash after the commit but before
            dispatch loses the event permanently.

        ``EXACTLY_ONCE``
            Dispatch is wrapped in ``producer.transaction()``.  Offsets are
            committed inside the transaction via ``send_offsets_to_transaction``
            so dispatch and offset commit are atomic.  An exception during
            dispatch aborts the transaction — the offset is not committed and
            the message is redelivered.

        Edge cases:
            - ``asyncio.CancelledError`` is not caught — it propagates cleanly.
            - Deserialization errors are logged and the message is skipped
              (offset still advances — bad payloads are not infinitely retried).
        """
        assert self._consumer is not None
        assert self._producer is not None

        semantics = self._config.delivery_semantics
        _logger.debug("Kafka consumer loop started (semantics=%s).", semantics.value)
        try:
            async for msg in self._consumer:
                try:
                    event = self._serializer.deserialize(msg.value)
                    channel = msg.topic.removeprefix(self._config.channel_prefix)

                    if semantics == KafkaDeliverySemantics.AT_MOST_ONCE:
                        # Commit offset BEFORE dispatch — message may be lost on crash.
                        await self._consumer.commit()
                        await self._chain(event, channel)

                    elif semantics == KafkaDeliverySemantics.EXACTLY_ONCE:
                        # Wrap dispatch + offset commit in one atomic transaction.
                        async with self._producer.transaction():
                            await self._chain(event, channel)
                            tp = TopicPartition(msg.topic, msg.partition)
                            await self._producer.send_offsets_to_transaction(
                                {tp: OffsetAndMetadata(msg.offset + 1, "")},
                                self._config.group_id,
                            )

                    else:
                        # AT_LEAST_ONCE — auto-commit handles progression.
                        await self._chain(event, channel)

                except asyncio.CancelledError:
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
            f"semantics={self._config.delivery_semantics.value!r}, "
            f"subscriptions={active}, "
            f"started={self._started})"
        )
