"""
varco_redis.streams
===================
Redis Streams implementation of ``AbstractEventBus`` — at-least-once delivery.

``RedisStreamEventBus`` publishes events via ``XADD`` (append to a stream)
and consumes them via ``XREADGROUP`` (consumer-group read).  Messages are
acknowledged with ``XACK`` only after all local handlers have run, so a crash
between arrival and ``XACK`` causes the broker to re-deliver the message on
the next startup.

Architecture
------------
::

    Publisher side:
        bus.publish(event, channel="orders")
            → JsonEventSerializer.serialize(event)
            → redis.xadd("{prefix}orders", {"payload": bytes})

    Consumer side (background):
        redis.xreadgroup(GROUP group CONSUMER name STREAMS stream ">")
            → JsonEventSerializer.deserialize(entry["payload"])
            → local _dispatch(event, channel)
                → priority-sorted matching handler calls
            → redis.xack(stream_key, group, message_id)   ← only on success

Comparison to Pub/Sub (varco_redis.bus)
----------------------------------------
+--------------------------+-------------------+----------------------+
| Characteristic           | Pub/Sub           | Streams              |
+==========================+===================+======================+
| Delivery guarantee       | At-most-once      | At-least-once ✅     |
| Offline subscriber       | Messages lost     | Retained ✅          |
| Consumer groups          | No                | Yes ✅               |
| CHANNEL_ALL wildcard     | psubscribe("*")   | No (tracked set) ⚠️ |
| Message ordering         | Best-effort       | Per-stream FIFO ✅   |
| Memory usage             | Low               | Grows until ack'd    |
+--------------------------+-------------------+----------------------+

CHANNEL_ALL limitation
----------------------
Redis Streams has no wildcard subscribe equivalent.  When a subscriber calls
``subscribe(SomeEvent, handler, channel=CHANNEL_ALL)``, the relay loop reads
from **all streams known at subscribe time**.  Streams published to after the
last ``subscribe()`` call are invisible to CHANNEL_ALL handlers until the next
explicit ``subscribe()`` for that channel.

For reliable fan-out, prefer explicit channel subscriptions.

Lifecycle
---------
``RedisStreamEventBus`` must be started and stopped explicitly::

    bus = RedisStreamEventBus(config)
    await bus.start()
    # ... use ...
    await bus.stop()

Or use as an async context manager::

    async with RedisStreamEventBus(config) as bus:
        ...

Thread safety:  ❌  Not thread-safe.  All access must be from the same event loop.
Async safety:   ✅  ``publish`` and lifecycle methods are ``async def``.

📚 Docs
- 🔍 https://redis.io/docs/data-types/streams/
  Redis Streams — XADD, XREADGROUP, XACK, consumer groups
- 🔍 https://redis-py.readthedocs.io/en/stable/commands.html#redis.commands.core.CoreCommands.xadd
  redis-py xadd / xreadgroup API reference
"""

from __future__ import annotations

import asyncio
import logging
import socket
from collections.abc import Awaitable, Callable, Coroutine
from typing import Annotated, Any

import redis.asyncio as aioredis

from providify import Inject, Instance, InjectMeta, PostConstruct, PreDestroy

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

# Payload field name used inside each Redis Streams entry.
# A single field "payload" carries the full JSON-serialized event bytes.
# DESIGN: single "payload" field over multiple fields per event attribute
#   ✅ Schema-free — event shape changes don't require stream schema migration.
#   ✅ Consistent with Pub/Sub (raw bytes) — same serializer works for both.
#   ❌ Not human-readable in redis-cli XRANGE output — use JsonEventSerializer
#      externally to decode for debugging.
_PAYLOAD_FIELD: str = "payload"

# How long to block on XREADGROUP when no messages are available.
# 100 ms balances responsiveness and CPU usage.
# DESIGN: blocking read over polling with sleep
#   ✅ The event loop is unblocked immediately when a message arrives.
#   ✅ No wasted CPU on tight sleep loops.
#   ❌ A blocked XREADGROUP cannot be interrupted except by cancellation —
#      the relay loop must cancel the pending Redis call via task.cancel().
_BLOCK_MS: int = 100

# Max messages fetched per XREADGROUP call.  Tune per workload.
# Smaller batches = lower latency; larger batches = higher throughput.
_BATCH_SIZE: int = 10

# Consumer group ID used when no explicit group is configured.
# All instances sharing the same group share load (load-balanced fan-out).
# Use distinct group names for independent fan-out (each group gets all msgs).
_DEFAULT_GROUP: str = "varco"


class RedisStreamEventBus(AbstractEventBus):
    """
    ``AbstractEventBus`` backed by Redis Streams via ``redis.asyncio``.

    Publishes events via ``XADD`` (appended to a per-channel stream) and
    consumes them via ``XREADGROUP`` with a consumer group.  Messages are
    acknowledged (``XACK``) only after all local handlers succeed, enabling
    at-least-once redelivery on failure or crash.

    Args:
        config:         Redis connection and channel configuration.
        consumer_group: Consumer group name.  All instances with the same
                        group share load (load-balanced).  Use distinct
                        group names for independent fan-out.
                        Defaults to ``"varco"``.
        consumer_name:  This consumer's name within the group.  Defaults to
                        ``"{hostname}-{pid}"`` — unique per process.
        batch_size:     Max messages fetched per ``XREADGROUP`` call.
                        Default ``10``.
        error_policy:   Handler error policy.  Defaults to ``COLLECT_ALL``.
        middleware:     Optional ``EventMiddleware`` list.
        serializer:     Pluggable event serializer.  Defaults to
                        ``JsonEventSerializer()``.

    Thread safety:  ❌  Not thread-safe.  Entire bus must run on one event loop.
    Async safety:   ✅  ``publish``, ``start``, and ``stop`` are ``async def``.

    Edge cases:
        - On ``start()``, the bus creates consumer groups (``XGROUP CREATE``)
          for all streams already subscribed.  If the stream doesn't exist yet,
          ``MKSTREAM`` creates it automatically.
        - On ``stop()``, any pending (unacknowledged) messages that were fetched
          but not yet dispatched remain in the stream's PEL (Pending Entry List).
          They will be redelivered on the next start — at-least-once delivery.
        - Calling ``publish()`` before ``start()`` raises ``RuntimeError``.
        - ``CHANNEL_ALL`` subscriptions only receive messages from streams
          that were known at the time of the ``subscribe()`` call.

    Example::

        config = RedisEventBusSettings(url="redis://localhost:6379/0")
        async with RedisStreamEventBus(config) as bus:
            bus.subscribe(OrderPlacedEvent, my_handler, channel="orders")
            await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")
    """

    def __init__(
        self,
        config: Inject[RedisEventBusSettings],
        *,
        consumer_group: str = _DEFAULT_GROUP,
        consumer_name: str | None = None,
        batch_size: int = _BATCH_SIZE,
        error_policy: ErrorPolicy = ErrorPolicy.COLLECT_ALL,
        middleware: Instance[EventMiddleware] | list[EventMiddleware] | None = None,
        serializer: Annotated[EventSerializer, InjectMeta(optional=True)] = None,
    ) -> None:
        """
        Args:
            config:         Redis settings injected from the container.
            consumer_group: Group name for ``XREADGROUP``.  Instances sharing
                            the same group load-balance consumption.
            consumer_name:  Per-consumer identity within the group.  Must be
                            unique per running instance.  Defaults to
                            ``"{hostname}-{pid}"`` which is unique per process.
            batch_size:     Max entries to read per XREADGROUP call.
            error_policy:   Handler error policy.
            middleware:     DI instance handle for ``EventMiddleware`` bindings.
            serializer:     Pluggable event serializer.  Defaults to
                            ``JsonEventSerializer()``.
        """
        import os  # deferred — only needed once at construction time

        self._config = config
        self._consumer_group = consumer_group
        # Default consumer name: hostname + PID — unique per process,
        # survives restarts with a recognisable identity for redis-cli XPENDING.
        self._consumer_name = consumer_name or f"{socket.gethostname()}-{os.getpid()}"
        self._batch_size = batch_size
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
        self._serializer: EventSerializer = serializer or JsonEventSerializer()

        self._subscriptions: list[_SubscriptionEntry] = []

        # Stream names (full Redis key) that have at least one specific-channel
        # subscription.  Used by the listener to know which streams to read.
        # CHANNEL_ALL subscribers are serviced by reading from ALL tracked streams.
        self._tracked_streams: set[str] = set()

        # True if any CHANNEL_ALL subscription was registered — affects
        # which streams the listener loop reads from.
        self._has_wildcard: bool = False

        self._redis: Any | None = None
        self._listener_task: asyncio.Task[None] | None = None
        self._started: bool = False

        self._chain: Callable[[Event, str], Coroutine[Any, Any, None]] = (
            self._build_chain()
        )

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    @PostConstruct
    async def start(self) -> None:
        """
        Connect to Redis, create consumer groups, and start the listener task.

        Must be called before ``publish()``.  Idempotent.

        Creates the consumer group (``XGROUP CREATE ... MKSTREAM``) for every
        tracked stream.  ``MKSTREAM`` creates the stream if it doesn't exist yet,
        so this is safe to call before any events have been published.

        Consumer group ID ``0`` means the group starts from the oldest unseen
        message — pending (unacknowledged) entries from previous runs are
        redelivered first.

        Raises:
            ConnectionError: If Redis is unreachable (raised by redis.asyncio).
        """
        if self._started:
            return

        self._redis = aioredis.from_url(
            self._config.url,
            decode_responses=False,  # we need raw bytes for payload deserialization
            socket_timeout=self._config.socket_timeout,
            **self._config.redis_kwargs,
        )

        # Create consumer groups for all streams already subscribed.
        # Streams subscribed AFTER start() are handled in subscribe() below.
        for stream_key in self._tracked_streams:
            await self._ensure_group(stream_key)

        self._listener_task = asyncio.create_task(
            self._listen_loop(),
            name="redis-streams-listener",
        )
        self._started = True
        _logger.info(
            "RedisStreamEventBus started (url=%s, group=%s, consumer=%s).",
            self._config.url,
            self._consumer_group,
            self._consumer_name,
        )

    @PreDestroy
    async def stop(self) -> None:
        """
        Cancel the listener task and close the Redis connection.  Idempotent.

        Any messages fetched but not yet dispatched remain in the PEL
        and will be redelivered on the next ``start()``.
        """
        if not self._started:
            return

        if self._listener_task is not None:
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                pass

        if self._redis is not None:
            await self._redis.aclose()

        self._started = False
        _logger.info("RedisStreamEventBus stopped.")

    async def __aenter__(self) -> RedisStreamEventBus:
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
        Serialize ``event`` and append it to the Redis Stream for ``channel``.

        Uses ``XADD {stream_key} * payload {bytes}`` — Redis auto-generates
        the message ID (``*`` = auto-increment).

        Args:
            event:   The event to publish.
            channel: Target channel.  Maps to a Redis stream key via
                     ``RedisEventBusSettings.channel_name(channel)``.

        Returns:
            ``None`` — stream appends are fire-and-forget from the caller's
            perspective.  Delivery guarantee is on the consumer side (XACK).

        Raises:
            RuntimeError: If called before ``start()``.
        """
        if self._redis is None:
            raise RuntimeError(
                "RedisStreamEventBus.publish() called before start(). "
                "Call await bus.start() or use 'async with bus' first."
            )

        stream_key = self._config.channel_name(channel)
        value = self._serializer.serialize(event)

        # XADD with auto-ID (*) and a single "payload" field.
        await self._redis.xadd(stream_key, {_PAYLOAD_FIELD: value})
        _logger.debug(
            "Published %s to Redis stream %s", type(event).__name__, stream_key
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
        Register a local handler for matching events from Redis Streams.

        If ``channel`` is specific (not ``CHANNEL_ALL``), the corresponding
        stream key is added to the tracked set and a consumer group is
        created/ensured immediately if the bus is already started.

        If ``CHANNEL_ALL`` is used, the handler receives events from ALL
        currently tracked streams.  Streams added after this call are NOT
        included automatically — subscribe to them explicitly to receive
        their events with CHANNEL_ALL handlers.

        Args:
            event_type: ``Event`` subclass or ``__event_type__`` string.
            handler:    Async or sync callable invoked on matching events.
            channel:    Channel filter.  ``CHANNEL_ALL`` → receive from
                        all currently tracked streams.
            filter:     Optional predicate for fine-grained filtering.
            priority:   Dispatch order — higher runs first.

        Returns:
            A ``Subscription`` handle.

        Edge cases:
            - ``subscribe()`` after ``start()`` is safe — consumer group is
              created immediately for the new stream.
            - CHANNEL_ALL only covers streams tracked at call time, not
              streams published to later.  This is a deliberate trade-off
              to avoid a Redis KEYS scan on every poll.
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
            # Mark wildcard — listener loop will read from all tracked streams.
            self._has_wildcard = True
        else:
            stream_key = self._config.channel_name(channel)
            if stream_key not in self._tracked_streams:
                self._tracked_streams.add(stream_key)
                if self._started:
                    # Bus already running — create group for this new stream now.
                    asyncio.ensure_future(self._ensure_group(stream_key))

        return Subscription(entry)

    # ── Listener loop ──────────────────────────────────────────────────────────

    async def _listen_loop(self) -> None:
        """
        Background task that reads from all tracked Redis Streams and dispatches.

        Uses ``XREADGROUP`` with a blocking timeout (``_BLOCK_MS``) so the
        event loop is not blocked when no messages are available.

        Runs until cancelled by ``stop()``.

        Edge cases:
            - If no streams are tracked yet (subscribe() not called before
              start()), the loop sleeps until a subscription is added.
            - ``asyncio.CancelledError`` propagates immediately — expected on
              shutdown.  Unacknowledged messages remain in the PEL.
            - Deserialization errors are logged at WARNING level and the
              message is acknowledged to prevent infinite retry loops on
              permanently bad payloads.
        """
        _logger.debug("Redis Streams listener started.")
        try:
            while True:
                if not self._tracked_streams:
                    # No streams yet — yield and check again shortly.
                    await asyncio.sleep(_BLOCK_MS / 1000)
                    continue

                await self._read_and_dispatch()
        except asyncio.CancelledError:
            _logger.debug("Redis Streams listener cancelled.")
            raise

    async def _read_and_dispatch(self) -> None:
        """
        Execute one XREADGROUP read cycle across all tracked streams.

        Reads ``self._batch_size`` new messages from each tracked stream and
        dispatches them.  Messages are acknowledged only after successful
        dispatch.

        Edge cases:
            - Empty result (no new messages) is a no-op — XREADGROUP blocks
              up to ``_BLOCK_MS`` then returns ``None`` or ``[]``.
            - If ``_dispatch`` raises, the message is NOT acknowledged —
              it will be redelivered on the next call.
            - ``asyncio.CancelledError`` propagates without acknowledging
              any in-flight messages.
        """
        assert self._redis is not None

        streams = list(self._tracked_streams)
        if not streams:
            return

        # Build the streams dict: {stream_key: ">"} where ">" means
        # "give me messages not yet delivered to this consumer".
        streams_arg = {s: ">" for s in streams}

        try:
            # XREADGROUP with a blocking timeout so the event loop is not
            # spun tight.  Returns None if no messages within _BLOCK_MS.
            results = await self._redis.xreadgroup(
                groupname=self._consumer_group,
                consumername=self._consumer_name,
                streams=streams_arg,
                count=self._batch_size,
                block=_BLOCK_MS,
            )
        except asyncio.CancelledError:
            raise
        except aioredis.ResponseError as exc:
            if "NOGROUP" in str(exc):
                # Race condition: subscribe() added a stream to _tracked_streams
                # and scheduled asyncio.ensure_future(_ensure_group()) but the
                # listener loop ran before that coroutine executed, so the
                # consumer group doesn't exist yet.
                # Recovery: eagerly create groups for all tracked streams now.
                # The next _read_and_dispatch() call will succeed.
                _logger.debug(
                    "RedisStreamEventBus: NOGROUP on XREADGROUP — ensuring groups "
                    "for %d tracked stream(s) before next poll cycle.",
                    len(self._tracked_streams),
                )
                for stream_key in list(self._tracked_streams):
                    await self._ensure_group(stream_key)
            else:
                _logger.warning(
                    "RedisStreamEventBus: XREADGROUP failed: %s",
                    exc,
                    exc_info=True,
                )
            return
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "RedisStreamEventBus: XREADGROUP failed: %s",
                exc,
                exc_info=True,
            )
            return

        if not results:
            return

        # results shape: [(stream_key_bytes, [(msg_id_bytes, {field: value}), ...])]
        for stream_key_raw, messages in results:
            stream_key: str = (
                stream_key_raw.decode("utf-8")
                if isinstance(stream_key_raw, bytes)
                else stream_key_raw
            )
            # Recover the logical channel by stripping the prefix.
            logical_channel = stream_key.removeprefix(self._config.channel_prefix)

            for msg_id, fields in messages:
                await self._process_message(stream_key, msg_id, fields, logical_channel)

    async def _process_message(
        self,
        stream_key: str,
        msg_id: bytes,
        fields: dict[bytes, bytes],
        logical_channel: str,
    ) -> None:
        """
        Deserialize, dispatch, and acknowledge a single stream message.

        Acknowledges with ``XACK`` only if dispatch succeeds.  On failure,
        the message stays in the PEL for redelivery.

        DESIGN: per-message try/except for acknowledgement control
            ✅ Messages that fail dispatch are not acknowledged — they remain
               in PEL and will be redelivered (at-least-once guarantee).
            ✅ Bad payloads (deserialization error) are acknowledged to prevent
               infinite poison-pill loops — logged at ERROR level.
            ❌ A crashing handler will cause the same message to be retried
               indefinitely.  Use the P1 DLQ feature to handle persistent
               failures.

        Args:
            stream_key:      Full Redis stream key (with prefix).
            msg_id:          Redis Streams message ID bytes.
            fields:          Raw field dict from XREADGROUP response.
            logical_channel: Channel name with prefix stripped — used for
                             handler matching.

        Edge cases:
            - Missing ``_PAYLOAD_FIELD`` in ``fields`` logs an ERROR and
              the message is acknowledged to avoid infinite retry.
            - ``asyncio.CancelledError`` from dispatch propagates without
              acknowledging the message.
        """
        assert self._redis is not None

        payload = fields.get(_PAYLOAD_FIELD.encode()) or fields.get(_PAYLOAD_FIELD)
        if not payload:
            _logger.error(
                "RedisStreamEventBus: message %s on stream %s has no '%s' field — "
                "acknowledging to avoid infinite retry loop.",
                msg_id,
                stream_key,
                _PAYLOAD_FIELD,
            )
            await self._redis.xack(stream_key, self._consumer_group, msg_id)
            return

        try:
            event = self._serializer.deserialize(payload)
        except Exception as exc:  # noqa: BLE001
            # Unrecoverable bad payload — acknowledge so it's not redelivered
            # endlessly, but log at ERROR so operators notice it.
            _logger.error(
                "RedisStreamEventBus: deserialization failed for message %s "
                "on stream %s: %s — acknowledging unrecoverable message.",
                msg_id,
                stream_key,
                exc,
                exc_info=True,
            )
            await self._redis.xack(stream_key, self._consumer_group, msg_id)
            return

        try:
            await self._chain(event, logical_channel)
        except asyncio.CancelledError:
            # Task cancelled — do NOT acknowledge; message stays in PEL.
            raise
        except Exception as exc:  # noqa: BLE001
            # Dispatch failed — do NOT acknowledge; message redelivered next tick.
            _logger.warning(
                "RedisStreamEventBus: dispatch failed for message %s "
                "(type=%s, channel=%s): %s — message stays in PEL for retry.",
                msg_id,
                type(event).__name__,
                logical_channel,
                exc,
                exc_info=True,
            )
            return

        # All handlers ran without error — acknowledge the message.
        await self._redis.xack(stream_key, self._consumer_group, msg_id)

    # ── Consumer group management ─────────────────────────────────────────────

    async def _ensure_group(self, stream_key: str) -> None:
        """
        Create the consumer group for ``stream_key`` if it doesn't exist.

        Uses ``XGROUP CREATE ... MKSTREAM`` so the stream is created if it
        doesn't exist yet.  The group starts from ID ``0`` so any existing
        (unacknowledged) messages are redelivered on startup.

        DESIGN: start from ID "0" over "$" (latest)
            ✅ At-least-once: any messages written to the stream before this
               consumer group was attached will be processed.
            ❌ On first deploy, all historical messages in the stream are
               redelivered.  If stream retention is long, this can be noisy.
               Use ``$`` to start from the current tail if historical messages
               are irrelevant.

        Args:
            stream_key: Full Redis stream key (with prefix).

        Edge cases:
            - ``BUSYGROUP`` error means the group already exists — silently
              ignored.
            - Any other ``ResponseError`` propagates.
        """
        assert self._redis is not None
        try:
            # id="0" → deliver all messages from the beginning (including PEL).
            # mkstream=True → create the stream if it doesn't exist yet.
            await self._redis.xgroup_create(
                stream_key,
                self._consumer_group,
                id="0",
                mkstream=True,
            )
            _logger.debug(
                "RedisStreamEventBus: created consumer group %r on stream %r.",
                self._consumer_group,
                stream_key,
            )
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                # Group already exists — this is expected on restart or when
                # multiple instances start concurrently.  Not an error.
                _logger.debug(
                    "RedisStreamEventBus: group %r already exists on stream %r.",
                    self._consumer_group,
                    stream_key,
                )
            else:
                raise

    # ── Dispatch helpers ──────────────────────────────────────────────────────

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
                        "Streams event handler %r raised and was ignored "
                        "(FIRE_FORGET): %s",
                        entry.handler,
                        exc,
                        exc_info=True,
                    )

        if errors:
            if len(errors) == 1:
                raise errors[0]
            raise ExceptionGroup(
                f"Redis Streams handlers raised {len(errors)} error(s) "
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
            f"RedisStreamEventBus("
            f"url={self._config.url!r}, "
            f"group={self._consumer_group!r}, "
            f"consumer={self._consumer_name!r}, "
            f"streams={len(self._tracked_streams)}, "
            f"subscriptions={active}, "
            f"started={self._started})"
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "RedisStreamEventBus",
]
