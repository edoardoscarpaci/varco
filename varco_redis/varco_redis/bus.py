"""
varco_redis.bus
===============
Redis Pub/Sub implementation of ``AbstractEventBus``.

``RedisEventBus`` publishes events as JSON bytes to Redis Pub/Sub channels
(one Redis channel per logical event channel) and subscribes to those channels
in a background ``asyncio.Task``.  Local handler dispatch reuses the same
priority-sorted matching logic as ``InMemoryEventBus``.

Architecture
------------
::

    Publisher side:
        bus.publish(event, channel="orders")
            → JsonEventSerializer.serialize(event)
            → redis.publish("orders", bytes)

    Consumer side (background):
        pubsub.get_message()   (polling loop)
            → JsonEventSerializer.deserialize(bytes)
            → local _dispatch(event, channel)
                → priority-sorted matching handler calls

DESIGN: Redis Pub/Sub over Redis Streams
    ✅ Simple — no consumer groups, no offsets to manage.
    ✅ No message persistence — lightweight for ephemeral events.
    ❌ At-most-once delivery — if the subscriber is down, messages are lost.
       Use Redis Streams (``varco_redis.streams``, planned) for at-least-once.
    ❌ Wildcard channel subscriptions (``CHANNEL_ALL = "*"``) require
       ``psubscribe`` (pattern subscribe) — handled automatically by the bus.

Lifecycle
---------
``RedisEventBus`` must be started and stopped explicitly::

    bus = RedisEventBus(config)
    await bus.start()
    # ... use ...
    await bus.stop()

Or use as an async context manager::

    async with RedisEventBus(config) as bus:
        ...

Thread safety:  ❌  Not thread-safe.  All access must be from the same event loop.
Async safety:   ✅  ``publish`` and lifecycle methods are ``async def``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine
from typing import Any

# redis is a hard dependency of this package — imported at module level so
# unit tests can patch varco_redis.bus.aioredis without reaching into the
# redis package namespace directly.
import redis.asyncio as aioredis

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

from varco_redis.config import RedisEventBusSettings

_logger = logging.getLogger(__name__)

# Polling interval for the Pub/Sub listener loop — small enough to be
# responsive, large enough not to spin the CPU under no-message conditions.
_POLL_INTERVAL: float = 0.01  # 10 ms


class RedisEventBus(AbstractEventBus):
    """
    ``AbstractEventBus`` backed by Redis Pub/Sub via ``redis.asyncio``.

    Published events are serialized to JSON and sent to a Redis Pub/Sub
    channel.  A background listener task subscribes to all registered
    channels and dispatches arriving messages to local handlers.

    Args:
        config:        Redis connection and channel configuration.
        error_policy:  Handler error policy on the consumer side.
                       Defaults to ``ErrorPolicy.COLLECT_ALL``.
        middleware:    Optional ``EventMiddleware`` list.

    Lifecycle:
        ``start()`` / ``stop()`` — explicit lifecycle management.
        Or use as an async context manager.

    Thread safety:  ❌  Not thread-safe.
    Async safety:   ✅  All async methods are safe.

    Example::

        config = RedisEventBusSettings(url="redis://localhost:6379/0")
        async with RedisEventBus(config) as bus:
            bus.subscribe(OrderPlacedEvent, my_handler, channel="orders")
            await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")

    Edge cases:
        - Redis Pub/Sub delivers messages only to CURRENTLY connected subscribers.
          Messages published while a subscriber is disconnected are lost.
        - ``subscribe(channel=CHANNEL_ALL)`` uses Redis pattern subscribe (``"*"``)
          — the handler receives events from ALL channels on this Redis instance.
          Use a ``channel_prefix`` in ``RedisEventBusSettings`` to limit scope.
        - Calling ``publish()`` before ``start()`` raises ``RuntimeError``.
    """

    def __init__(
        self,
        config: RedisEventBusSettings | None = None,
        *,
        error_policy: ErrorPolicy = ErrorPolicy.COLLECT_ALL,
        middleware: list[EventMiddleware] | None = None,
        serializer: EventSerializer | None = None,
    ) -> None:
        """
        Args:
            config:       Redis settings.  Defaults to ``RedisEventBusSettings()``
                          (localhost, reads from ``VARCO_REDIS_*`` env vars).
            error_policy: Handler error policy.
            middleware:   Ordered middleware list (index 0 is outermost).
            serializer:   Pluggable event serializer.  Defaults to
                          ``JsonEventSerializer()``.
        """
        self._config = config or RedisEventBusSettings()
        self._error_policy = error_policy
        self._middleware: list[EventMiddleware] = middleware or []

        # Use provided serializer or fall back to JSON.
        self._serializer: EventSerializer = serializer or JsonEventSerializer()

        self._subscriptions: list[_SubscriptionEntry] = []

        # Tracks the set of Redis channels actively subscribed by the pubsub
        # client.  Updated whenever subscribe() adds a new channel.
        self._subscribed_channels: set[str] = set()
        # True if a CHANNEL_ALL subscription was added — requires psubscribe("*")
        self._has_wildcard: bool = False

        self._redis: Any | None = None
        self._pubsub: Any | None = None
        self._listener_task: asyncio.Task | None = None
        self._started = False

        self._chain: Callable[[Event, str], Coroutine[Any, Any, None]] = (
            self._build_chain()
        )

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Connect to Redis and start the background Pub/Sub listener task.

        Must be called before ``publish()``.  Idempotent.

        Raises:
            ConnectionError: If Redis is unreachable (raised by redis.asyncio).
        """
        if self._started:
            return

        self._redis = aioredis.from_url(
            self._config.url,
            decode_responses=self._config.decode_responses,
            socket_timeout=self._config.socket_timeout,
            **self._config.redis_kwargs,
        )
        self._pubsub = self._redis.pubsub()

        # Subscribe to channels already registered before start()
        if self._has_wildcard:
            await self._pubsub.psubscribe("*")
        if self._subscribed_channels:
            await self._pubsub.subscribe(*self._subscribed_channels)

        self._listener_task = asyncio.create_task(
            self._listen_loop(),
            name="redis-pubsub-listener",
        )
        self._started = True
        _logger.info("RedisEventBus started (url=%s)", self._config.url)

    @PreDestroy
    async def stop(self) -> None:
        """
        Cancel the listener task and close the Redis connection.  Idempotent.
        """
        if not self._started:
            return

        if self._listener_task is not None:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

        if self._pubsub is not None:
            await self._pubsub.close()

        if self._redis is not None:
            await self._redis.aclose()

        self._started = False
        _logger.info("RedisEventBus stopped.")

    async def __aenter__(self) -> RedisEventBus:
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
        Serialize ``event`` and publish it to the Redis Pub/Sub channel.

        Args:
            event:   The event to publish.
            channel: Target channel.  Maps to a Redis channel via
                     ``RedisEventBusSettings.channel_name(channel)``.

        Returns:
            ``None`` — Redis Pub/Sub is always asynchronous (no local task).

        Raises:
            RuntimeError: If called before ``start()``.
        """
        if self._redis is None:
            raise RuntimeError(
                "RedisEventBus.publish() called before start(). "
                "Call await bus.start() or use 'async with bus' first."
            )

        redis_channel = self._config.channel_name(channel)
        value = self._serializer.serialize(event)
        await self._redis.publish(redis_channel, value)
        _logger.debug(
            "Published %s to Redis channel %s", type(event).__name__, redis_channel
        )
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
        Register a local handler for matching events arriving from Redis.

        If ``channel`` is specific (not ``CHANNEL_ALL``), the corresponding
        Redis channel is added to the Pub/Sub subscription.  If
        ``CHANNEL_ALL`` is used, a pattern subscription ``"*"`` is added.

        Args:
            event_type: ``Event`` subclass or ``__event_type__`` string.
            handler:    Async or sync callable invoked on matching events.
            channel:    Channel filter.  ``CHANNEL_ALL`` → receive from all channels.
            filter:     Optional predicate for fine-grained filtering.
            priority:   Dispatch order — higher runs first.

        Returns:
            A ``Subscription`` handle.

        Edge cases:
            - ``subscribe()`` after ``start()`` is safe — the Pub/Sub
              subscription is updated immediately.
        """
        entry = _SubscriptionEntry(
            event_type=event_type,
            channel=channel,
            handler=handler,
            filter=filter,
            priority=priority,
        )
        self._subscriptions.append(entry)

        if channel == CHANNEL_ALL:
            # Pattern subscribe — matches ALL channels published to this Redis
            if not self._has_wildcard:
                self._has_wildcard = True
                if self._pubsub is not None:
                    # Already started — subscribe immediately
                    asyncio.ensure_future(self._pubsub.psubscribe("*"))
        else:
            redis_channel = self._config.channel_name(channel)
            if redis_channel not in self._subscribed_channels:
                self._subscribed_channels.add(redis_channel)
                if self._pubsub is not None:
                    asyncio.ensure_future(self._pubsub.subscribe(redis_channel))

        return Subscription(entry)

    # ── Listener loop ──────────────────────────────────────────────────────────

    async def _listen_loop(self) -> None:
        """
        Background task that polls Redis Pub/Sub and dispatches messages.

        Runs until cancelled (by ``stop()``).  Uses ``get_message()`` with a
        small timeout so the loop yields control regularly.

        Edge cases:
            - ``asyncio.CancelledError`` propagates immediately — expected on shutdown.
            - Subscribe/unsubscribe confirmation messages (type ``"subscribe"``,
              ``"unsubscribe"``, ``"psubscribe"``, ``"punsubscribe"``) are
              silently ignored — only ``"message"`` and ``"pmessage"`` types
              carry actual event data.
            - Deserialization errors are logged at WARNING level and skipped.
        """
        assert self._pubsub is not None

        _logger.debug("Redis Pub/Sub listener started.")
        try:
            while True:
                # get_message() is non-blocking — returns None if no message.
                # sleep allows other coroutines to run between polls.
                try:
                    message = await self._pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=_POLL_INTERVAL,
                    )
                except RuntimeError as exc:
                    # redis>=7.0 raises RuntimeError("pubsub connection not set")
                    # when get_message() is called before any subscribe() call.
                    # This is a startup race — the listener task may start
                    # before the first subscription is registered.  Sleep and
                    # retry; the next iteration will succeed once subscribe()
                    # has been called and the connection is established.
                    if "connection not set" in str(exc):
                        await asyncio.sleep(_POLL_INTERVAL)
                        continue
                    raise
                if message is None:
                    await asyncio.sleep(_POLL_INTERVAL)
                    continue

                msg_type = message.get("type")
                # "message" → specific channel; "pmessage" → pattern match
                if msg_type not in ("message", "pmessage"):
                    continue

                try:
                    event = self._serializer.deserialize(message["data"])
                    # Recover logical channel by stripping the prefix
                    redis_channel: str = message["channel"]
                    if isinstance(redis_channel, bytes):
                        redis_channel = redis_channel.decode("utf-8")
                    logical_channel = redis_channel.removeprefix(
                        self._config.channel_prefix
                    )
                    await self._chain(event, logical_channel)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    _logger.warning(
                        "Failed to process Redis message from channel %s: %s",
                        message.get("channel"),
                        exc,
                        exc_info=True,
                    )
        except asyncio.CancelledError:
            _logger.debug("Redis Pub/Sub listener cancelled.")
            raise

    # ── Dispatch helpers — mirror InMemoryEventBus ─────────────────────────────

    async def _dispatch(self, event: Event, channel: str) -> None:
        """Dispatch ``event`` to matching local handlers with error policy applied."""
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
                        "Redis event handler %r raised and was ignored "
                        "(FIRE_FORGET): %s",
                        entry.handler,
                        exc,
                        exc_info=True,
                    )

        if errors:
            if len(errors) == 1:
                raise errors[0]
            raise ExceptionGroup(
                f"Redis handlers raised {len(errors)} error(s) "
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
            f"RedisEventBus("
            f"url={self._config.url!r}, "
            f"subscriptions={active}, "
            f"started={self._started})"
        )
