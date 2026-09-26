"""
varco_nats.bus
==============
NATS JetStream implementation of ``AbstractEventBus``.

``NatsEventBus`` publishes events as JSON bytes to JetStream subjects and
consumes them through durable JetStream consumers.  Local handler dispatch
reuses the same priority-sorted matching logic as ``InMemoryEventBus`` and
``KafkaEventBus``.

Architecture
------------
::

    Publisher side:
        bus.publish(event, channel="orders")
            → JsonEventSerializer.serialize(event)
            → JetStreamContext.publish(subject="varco.orders", payload=bytes)

    Consumer side (JetStream push consumer + callback):
        durable consumer delivers msg
            → JsonEventSerializer.deserialize(msg.data)
            → local _dispatch(event, channel)
                → priority-sorted matching handler calls
            → msg.ack()   (after dispatch — AT_LEAST_ONCE / EXACTLY_ONCE)

Subject naming
--------------
Each channel maps to one JetStream subject under a shared prefix.
``NatsEventBusSettings.subject_name(channel)`` builds it::

    channel = "orders"  →  subject = "varco.orders"
    channel = "orders"  →  subject = "varco.prod.orders"  (channel_prefix="prod.")

A single JetStream stream (``stream_name``) captures ``{subject_prefix}.>`` so
all channels share one stream.  Durable consumers are created per concrete
channel — one ``js.subscribe`` per subject.

Acknowledgement model
---------------------
``NatsEventBus`` mirrors ``KafkaEventBus``: JetStream redelivery is the
*broker-level* safety net, while *handler-level* retries are the job of
varco's ``@listen(retry_policy=..., dlq=...)`` machinery.

``AT_LEAST_ONCE`` (default)
    The message is acked **after** the in-process dispatch chain completes —
    whether or not a handler raised.  JetStream only redelivers if the process
    crashes before the ack.  Handler failures are handled in-process by the
    retry/DLQ wrapper, never by JetStream redelivery.

``AT_MOST_ONCE``
    The message is acked **before** dispatch.  A crash between ack and dispatch
    loses the event permanently.  No duplicates.

``EXACTLY_ONCE``
    Acked after dispatch (as AT_LEAST_ONCE), plus every published message
    carries a ``Nats-Msg-Id`` header equal to ``event.event_id`` so JetStream
    drops producer-retry duplicates inside the stream's ``duplicate_window``.

Lifecycle
---------
``NatsEventBus`` must be started and stopped explicitly::

    bus = NatsEventBus(config)
    await bus.start()      # connects, ensures the stream, opens consumers
    # ... use the bus ...
    await bus.stop()       # drains and closes the NATS connection

Or use it as an async context manager::

    async with NatsEventBus(config) as bus:
        ...

DESIGN: one durable consumer per concrete channel subject
    ✅ Each channel resumes independently after a restart.
    ✅ Subjects subscribed dynamically as ``subscribe()`` is called.
    ✅ ``CHANNEL_ALL`` creates NO consumer — like ``KafkaEventBus`` it only
       receives events for channels other subscriptions already listen to.
    ❌ Many channels → many durable consumers.  Acceptable: JetStream consumers
       are cheap and this keeps per-channel cursors independent.

Thread safety:  ❌  Not thread-safe.  All access must be from the same event loop.
Async safety:   ✅  ``publish`` and ``start``/``stop`` are ``async def``.

📚 Docs
- 🔍 https://docs.nats.io/nats-concepts/jetstream — JetStream streams & consumers
- 🔍 https://nats-io.github.io/nats.py/ — nats-py JetStreamContext API
- 🔍 https://docs.nats.io/using-nats/developer/develop_jetstream/model_deep_dive#message-deduplication
  Nats-Msg-Id deduplication
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Coroutine
from typing import Annotated, Any

# nats-py is a hard dependency of this package — imported at module level so
# unit tests can patch varco_nats.bus.connect without reaching into the nats
# namespace directly.
from nats import connect
from nats.js.api import ConsumerConfig
from nats.js.errors import NotFoundError
from providify import Fallback, Inject, InjectMeta, Instance, PostConstruct, PreDestroy, Singleton
from varco_core.di import BACKEND_PRIORITY
from varco_core.di.select import selected_by
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
from varco_core.event.serializer import JsonEventSerializer
from varco_core.serialization import Serializer

from varco_nats.config import NatsDeliverySemantics, NatsEventBusSettings

_logger = logging.getLogger(__name__)

# JetStream header key for producer-side deduplication.  A message whose
# Nats-Msg-Id repeats within the stream's duplicate_window is dropped.
_MSG_ID_HEADER = "Nats-Msg-Id"


@Fallback
@Singleton(priority=BACKEND_PRIORITY, qualifier="nats")
@selected_by("event_bus", "nats")
class NatsEventBus(AbstractEventBus):
    """
    ``AbstractEventBus`` backed by NATS JetStream via ``nats-py``.

    Published events are serialized to JSON and sent to a JetStream subject
    named after the channel.  Durable JetStream consumers deliver messages to
    a callback that dispatches to locally registered handlers.

    Args:
        config:        NATS connection and routing configuration.
        error_policy:  Controls handler error behaviour on the consumer side.
                       Defaults to ``ErrorPolicy.COLLECT_ALL``.
        middleware:    Optional list of ``EventMiddleware`` instances applied
                       before local handler dispatch.
        serializer:    Pluggable event serializer.  Injected optionally;
                       defaults to ``JsonEventSerializer()`` when absent.

    Lifecycle:
        ``start()`` / ``stop()`` — explicit lifecycle management.
        Or use as an async context manager (``async with NatsEventBus(...) as bus``).

    Thread safety:  ❌  Not thread-safe — use from a single event loop only.
    Async safety:   ✅  All async methods are safe to await.

    Example::

        config = NatsEventBusSettings(servers="nats://localhost:4222")
        async with NatsEventBus(config) as bus:
            bus.subscribe(OrderPlacedEvent, my_handler, channel="orders")
            await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")

    Edge cases:
        - Calling ``publish()`` before ``start()`` raises ``RuntimeError``.
        - ``subscribe()`` called after ``start()`` is safe — the JetStream
          consumer for the new channel is created on the event loop.
        - Subscribing with ``channel=CHANNEL_ALL`` does NOT open a JetStream
          consumer — the handler only receives events dispatched locally for
          channels other subscriptions already listen to.
        - JetStream streams are not auto-created by the server.  With
          ``auto_create_stream=True`` (default) the bus creates the backing
          stream on ``start()``.
        - Consumer errors (deserialization, handler exceptions) are logged but
          do NOT stop message consumption.
    """

    def __init__(
        self,
        config: Inject[NatsEventBusSettings],
        *,
        error_policy: ErrorPolicy = ErrorPolicy.COLLECT_ALL,
        middleware: Instance[EventMiddleware] | list[EventMiddleware] | None = None,
        serializer: Annotated[Serializer[Event] | None, InjectMeta(optional=True)] = None,
    ) -> None:
        """
        Args:
            config:       NATS settings injected from the container.
            error_policy: Handler error policy.
            middleware:   DI instance handle for ``EventMiddleware`` bindings,
                          or a direct list.  All registered middlewares are
                          resolved via ``middleware.get_all()`` when provided.
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
            self._middleware = list(middleware.get_all()) if middleware.resolvable() else []

        # Use the provided serializer or fall back to JSON.  Stored as an
        # instance so stateful serializers (e.g. ones caching TypeAdapters)
        # work correctly.
        self._serializer: Serializer[Event] = serializer or JsonEventSerializer()

        # Local subscription list — same model as InMemoryEventBus / KafkaEventBus.
        # Handler dispatch happens in-process after a message arrives from NATS.
        self._subscriptions: list[_SubscriptionEntry] = []

        # Logical channels (excluding CHANNEL_ALL) that need a JetStream
        # consumer.  Used to (re)create consumers on start().
        self._subscribed_channels: set[str] = set()

        # Active JetStream push subscriptions, keyed by subject.  Deduplicates:
        # one consumer per subject regardless of how many handlers subscribed.
        self._jetstream_subs: dict[str, Any] = {}

        # NATS client and JetStream context — created in start() because nats-py
        # objects must be created inside a running event loop.
        self._nc: Any | None = None
        self._js: Any | None = None
        self._started = False

        # Pre-build the middleware chain — same approach as KafkaEventBus.
        self._chain: Callable[[Event, str], Coroutine[Any, Any, None]] = self._build_chain()

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    @PostConstruct
    async def start(self) -> None:
        """
        Connect to NATS, ensure the JetStream stream, and open consumers.

        Must be called before ``publish()``.  Idempotent — calling ``start()``
        twice on an already-started bus is a no-op.

        Raises:
            NoServersError:   (nats-py) If no configured server is reachable.
            APIError:         (nats-py) If JetStream is not enabled on the server
                              or the stream cannot be created.

        Edge cases:
            - Calling ``start()`` with no subscriptions is valid — no consumers
              are opened until ``subscribe()`` is called.
            - With ``auto_create_stream=False`` the backing stream must already
              exist or ``publish()`` will fail.
        """
        if self._started:
            return

        # Connect using the parsed server list plus any pass-through kwargs
        # (TLS, nkey, JWT credentials, client name, ...).
        self._nc = await connect(
            servers=self._config.to_servers_list(),
            **self._config.connect_kwargs,
        )
        # jetstream() is synchronous — it just wraps the client with the
        # JetStream API surface.
        self._js = self._nc.jetstream()

        if self._config.auto_create_stream:
            await self._ensure_stream()

        # Open a JetStream consumer for every channel queued by subscribe()
        # calls made before start().
        for channel in self._subscribed_channels:
            await self._ensure_jetstream_sub(channel)

        self._started = True
        _logger.info(
            "NatsEventBus started (servers=%s, stream=%s, durable=%s, semantics=%s)",
            self._config.servers,
            self._config.stream_name,
            self._config.durable_name,
            self._config.delivery_semantics.value,
        )

    @PreDestroy
    async def stop(self) -> None:
        """
        Unsubscribe all consumers and close the NATS connection.

        Idempotent — safe to call on a bus that was never started.

        Edge cases:
            - In-flight callback dispatches are interrupted on the next
              ``await`` point once the connection closes.
            - ``drain()`` is attempted first to flush pending acks; if it fails
              the connection is force-closed.
        """
        if not self._started:
            return

        # Best-effort unsubscribe of each JetStream consumer.
        for subject, sub in self._jetstream_subs.items():
            try:
                await sub.unsubscribe()
            except Exception as exc:  # noqa: BLE001 — teardown is best-effort
                _logger.debug(
                    "NatsEventBus: unsubscribe failed for subject %r: %s",
                    subject,
                    exc,
                )
        self._jetstream_subs.clear()

        if self._nc is not None:
            try:
                # drain() flushes pending acks and unsubscribes cleanly, then
                # closes the connection.
                await self._nc.drain()
            except Exception as exc:  # noqa: BLE001
                # If draining fails (already draining / closed), force-close.
                _logger.debug("NatsEventBus: drain failed, force-closing: %s", exc)
                await self._nc.close()

        self._nc = None
        self._js = None
        self._started = False
        _logger.info("NatsEventBus stopped.")

    async def __aenter__(self) -> NatsEventBus:
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
        Serialize ``event`` and publish it to the JetStream subject for ``channel``.

        JetStream delivery is inherently asynchronous — the consumer (this or
        another service) processes the message at a later point.  This method
        blocks until JetStream acknowledges the publish (the ``PubAck``).

        Args:
            event:   The event to publish.
            channel: Target channel.  Maps to a JetStream subject via
                     ``NatsEventBusSettings.subject_name(channel)``.

        Returns:
            ``None`` — JetStream delivery is always background; no local task.

        Raises:
            RuntimeError:           If called before ``start()``.
            NoStreamResponseError:  (nats-py) If no stream captures the subject.

        Edge cases:
            - Under ``EXACTLY_ONCE`` the publish carries a ``Nats-Msg-Id``
              header equal to ``event.event_id`` — JetStream drops a repeat of
              the same id within the stream's ``duplicate_window``.
            - Events published before any subscriber exists are still stored in
              the stream — they are delivered when a consumer is opened.
        """
        if self._js is None:
            raise RuntimeError(
                "NatsEventBus.publish() called before start(). "
                "Call await bus.start() or use 'async with bus' first."
            )

        subject = self._config.subject_name(channel)
        value = self._serializer.serialize(event)

        # EXACTLY_ONCE → attach the dedup header so JetStream collapses
        # producer-retry duplicates.  Other modes publish without it.
        headers: dict[str, str] | None = None
        if self._config.delivery_semantics is NatsDeliverySemantics.EXACTLY_ONCE:
            headers = {_MSG_ID_HEADER: str(event.event_id)}

        await self._js.publish(subject, value, headers=headers)
        _logger.debug("Published %s to subject %s", type(event).__name__, subject)
        # Return None — JetStream delivery is always async (broker-side).
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
        Register a local handler for matching events arriving from JetStream.

        If ``channel`` is a specific channel (not ``CHANNEL_ALL``), a durable
        JetStream consumer for the corresponding subject is opened.

        Args:
            event_type: ``Event`` subclass or ``__event_type__`` string.
            handler:    Async or sync callable invoked on matching events.
            channel:    Channel filter.  If not ``CHANNEL_ALL``, a JetStream
                        consumer for this channel is opened.
            filter:     Optional predicate for fine-grained filtering.
            priority:   Dispatch order — higher runs first.

        Returns:
            A ``Subscription`` handle.

        Edge cases:
            - Subscribing with ``channel=CHANNEL_ALL`` does NOT open a JetStream
              consumer — the handler receives events dispatched locally for any
              channel other subscriptions already listen to.
            - Adding a new channel-specific subscription after ``start()`` is
              safe — the consumer is opened as a background task on the loop.
            - Two subscriptions on the same channel share a single JetStream
              consumer (deduplicated by subject).
        """
        entry = _SubscriptionEntry(
            event_type=event_type,
            channel=channel,
            handler=handler,
            filter=filter,
            priority=priority,
        )
        self._subscriptions.append(entry)

        # CHANNEL_ALL ("*") opens no JetStream consumer — it is a local-only
        # filter, identical to KafkaEventBus behaviour.  Only concrete channels
        # get a durable consumer.
        if channel != CHANNEL_ALL and channel not in self._subscribed_channels:
            self._subscribed_channels.add(channel)
            # If already running, open the consumer on the loop.  subscribe()
            # is sync, so the async js.subscribe() is scheduled as a task —
            # the same pattern RedisEventBus uses for late subscriptions.
            if self._js is not None:
                asyncio.ensure_future(  # noqa: RUF006 — fire-and-forget by design
                    self._ensure_jetstream_sub(channel)
                )

        return Subscription(entry)

    # ── JetStream wiring ───────────────────────────────────────────────────────

    async def _ensure_stream(self) -> None:
        """
        Create the backing JetStream stream if it does not already exist.

        JetStream streams are never auto-created by the NATS server — a publish
        to an uncaptured subject fails.  This makes the bus self-sufficient
        when ``auto_create_stream`` is enabled.

        DESIGN: stream_info() probe over a blind add_stream()
            ✅ ``add_stream`` on an existing stream with a *different* config
               raises — probing first keeps the call idempotent.
            ❌ One extra round-trip on every ``start()``.  Negligible — start()
               runs once per process.

        Edge cases:
            - If another stream already captures the wildcard subject, the
              ``add_stream`` call raises ``BadRequestError`` — overlapping
              subjects are a configuration error and must surface.
        """
        assert self._js is not None  # guaranteed by start() ordering
        stream = self._config.stream_name
        try:
            await self._js.stream_info(stream)
            _logger.debug("JetStream stream %r already exists.", stream)
            return
        except NotFoundError:
            # Stream is absent — create it below.
            pass

        # duplicate_window is only consulted for EXACTLY_ONCE producer dedup,
        # but setting it unconditionally lets the delivery mode be switched
        # later without recreating the stream.
        await self._js.add_stream(
            name=stream,
            subjects=[self._config.wildcard_subject()],
            duplicate_window=self._config.duplicate_window_seconds,
        )
        _logger.info(
            "Created JetStream stream %r (subjects=[%s])",
            stream,
            self._config.wildcard_subject(),
        )

    async def _ensure_jetstream_sub(self, channel: str) -> None:
        """
        Open a durable JetStream consumer for ``channel`` if not already open.

        Args:
            channel: The logical channel to open a consumer for.

        Edge cases:
            - Idempotent — keyed by subject, so repeated calls (or concurrent
              ``subscribe()`` calls for the same channel) open at most one
              consumer.
            - Called both from ``start()`` (awaited, sequential) and from
              ``subscribe()`` (scheduled as a task) — the subject-keyed guard
              makes both paths safe.

        Async safety: ✅ The ``_jetstream_subs`` membership check makes
                      repeated calls a no-op.
        """
        if self._js is None:
            return  # not started yet — start() will open it later

        subject = self._config.subject_name(channel)
        if subject in self._jetstream_subs:
            return

        durable = self._config.durable_for(channel)
        # manual_ack=True → the callback is responsible for ack/nak.  We ack
        # explicitly in _on_message per the delivery semantics.
        #
        # config= wires ack_wait_seconds and max_deliver into the actual
        # JetStream consumer (Plan 019 / RT2-B, Status corrections) — prior to
        # this the settings were read from env but never reached the broker,
        # so ack_wait_seconds was dead configuration and max_deliver did not
        # exist at all (JetStream's server-side default is unlimited
        # redelivery, research 005 §B).
        sub = await self._js.subscribe(
            subject,
            durable=durable,
            cb=self._on_message,
            manual_ack=True,
            config=ConsumerConfig(
                ack_wait=self._config.ack_wait_seconds,
                max_deliver=self._config.max_deliver,
            ),
        )
        self._jetstream_subs[subject] = sub
        _logger.debug("Opened JetStream consumer (subject=%r, durable=%r)", subject, durable)

    async def _on_message(self, msg: Any) -> None:
        """
        JetStream delivery callback — deserialize, dispatch, acknowledge.

        Acknowledgement / redelivery outcome (Plan 019 / RT2-B — this is the
        specification the prior "ack in a finally regardless of outcome"
        implementation violated; see the DESIGN block below):

        - ``AT_MOST_ONCE``            → ``ack()`` BEFORE dispatch (unchanged;
          a crash after this ack loses the message).
        - deserialization error       → ``term()`` (a poison payload can never
          succeed on retry — naking it would be an infinite loop).
        - dispatch succeeded          → ``ack()``.
        - dispatch raised, redelivery budget exhausted
          (``msg.metadata.num_delivered >= max_deliver``) → ``term()`` + a
          WARNING naming the subject and the delivery count.
        - dispatch raised, budget remaining → ``nak()`` — immediate
          redelivery (research 005 §D; a bare non-acked message instead waits
          out the full ``ack_wait`` before JetStream retries).

        Args:
            msg: The nats-py ``Msg`` delivered by the durable consumer.

        Edge cases:
            - ``asyncio.CancelledError`` propagates — it is shutdown, not a
              message error.
            - Under ``ErrorPolicy.FIRE_FORGET`` a raising handler's exception
              never leaves ``_dispatch`` (``_dispatch``'s ``FIRE_FORGET``
              branch only logs), so this method observes a *successful*
              dispatch and ``ack()``s — FIRE_FORGET opts out of JetStream
              redelivery, which is coherent with its name but must be known
              by anyone relying on ``AT_LEAST_ONCE`` + ``FIRE_FORGET``
              together.

        DESIGN: outcome-driven ack/nak/term, bounded by max_deliver
            ✅ ``nak()`` on a raising handler triggers **immediate**
               redelivery (research 005 §D) rather than waiting out the full
               ``ack_wait`` (default 30s) for JetStream's own timeout-based
               retry — the difference between a ~1s test/production retry and
               a 30s one.
            ❌ Rejected — do not ack at all, let ``ack_wait`` expire: no new
               API surface, but redelivery latency becomes ``ack_wait`` per
               attempt and the broker cannot distinguish "handler failed" from
               "consumer hung".
            ✅ ``max_deliver`` bounds the redelivery count — JetStream's
               server-side default is *unlimited* redelivery (research 005
               §B), so a permanently-failing handler naking forever is an
               infinite hot loop without this bound. The bound is
               load-bearing, not polish.
            ❌ Rejected — ``nak(delay=...)`` with an exponential backoff
               schedule: varco already owns one retry model
               (``varco_core.resilience.RetryPolicy``, reused by
               ``@listen(retry_policy=...)``); a broker-side backoff schedule
               configured separately would be a second one. Bare ``nak()``,
               with the handler-level policy remaining the place backoff is
               expressed.
            ❌ ``ErrorPolicy.FIRE_FORGET`` opts OUT of redelivery — its
               handler exceptions are swallowed inside ``_dispatch`` before
               this method ever sees them, so a FIRE_FORGET + AT_LEAST_ONCE
               combination behaves like AT_MOST_ONCE for retry purposes. This
               is coherent with FIRE_FORGET's name but must be documented
               (README, CLAUDE.md pitfalls) rather than silently discovered.
            ❌ Rejected — route exhausted messages to ``NatsDLQ`` via the
               ``$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES`` advisory subject
               (research 005 §B): a second subscription with its own
               lifecycle and ``@PostConstruct`` wiring — an L-sized feature
               bolted onto this S-sized fix. ``@listen(dlq=...)`` already
               gives handlers a DLQ path; filed as a BACKLOG row instead.
        """
        semantics = self._config.delivery_semantics
        pre_ack = semantics is NatsDeliverySemantics.AT_MOST_ONCE

        # AT_MOST_ONCE commits the message before any handler runs.
        if pre_ack:
            await self._safe_ack(msg)
            try:
                event = self._serializer.deserialize(msg.data)
                channel = self._config.channel_from_subject(msg.subject)
                await self._chain(event, channel)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 — already acked; must not stop consumption
                _logger.warning(
                    "Failed to process NATS message from subject %s (AT_MOST_ONCE, "
                    "already acked): %s",
                    msg.subject,
                    exc,
                    exc_info=True,
                )
            return

        # AT_LEAST_ONCE / EXACTLY_ONCE — outcome-driven ack/nak/term.
        try:
            event = self._serializer.deserialize(msg.data)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — poison payload, never retryable
            _logger.warning(
                "Failed to deserialize NATS message from subject %s — terminating "
                "(will not be redelivered): %s",
                msg.subject,
                exc,
                exc_info=True,
            )
            await self._safe_term(msg)
            return

        channel = self._config.channel_from_subject(msg.subject)
        try:
            await self._chain(event, channel)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — decide nak vs term below
            num_delivered = getattr(msg.metadata, "num_delivered", 1)
            if num_delivered >= self._config.max_deliver:
                _logger.warning(
                    "NATS message from subject %s exhausted its redelivery budget "
                    "(num_delivered=%d, max_deliver=%d) — terminating: %s",
                    msg.subject,
                    num_delivered,
                    self._config.max_deliver,
                    exc,
                    exc_info=True,
                )
                await self._safe_term(msg)
            else:
                _logger.warning(
                    "Failed to process NATS message from subject %s "
                    "(num_delivered=%d/%d) — nak() for immediate redelivery: %s",
                    msg.subject,
                    num_delivered,
                    self._config.max_deliver,
                    exc,
                    exc_info=True,
                )
                await self._safe_nak(msg)
        else:
            await self._safe_ack(msg)

    async def _safe_ack(self, msg: Any) -> None:
        """
        Acknowledge ``msg`` without ever propagating an ack failure.

        Args:
            msg: The nats-py ``Msg`` to acknowledge.

        Edge cases:
            - An ack after the JetStream ack-wait deadline raises — it is
              logged and swallowed so the consumer keeps running.
        """
        try:
            await msg.ack()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — ack failure must not crash the consumer
            _logger.warning(
                "NATS message ack failed (subject=%s): %s",
                getattr(msg, "subject", "<unknown>"),
                exc,
                exc_info=True,
            )

    async def _safe_nak(self, msg: Any) -> None:
        """
        Negatively-acknowledge ``msg`` (request immediate redelivery) without
        ever propagating a nak failure.

        Args:
            msg: The nats-py ``Msg`` to nak.

        Edge cases:
            - Mirrors ``_safe_ack`` — a nak failure (e.g. the ack-wait deadline
              already passed) is logged and swallowed rather than killing the
              consumer; JetStream will still redeliver once ``ack_wait``
              elapses even if the explicit ``nak()`` itself failed.
        """
        try:
            await msg.nak()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — nak failure must not crash the consumer
            _logger.warning(
                "NATS message nak failed (subject=%s): %s",
                getattr(msg, "subject", "<unknown>"),
                exc,
                exc_info=True,
            )

    async def _safe_term(self, msg: Any) -> None:
        """
        Terminate ``msg`` (disable all future redeliveries) without ever
        propagating a term failure.

        Args:
            msg: The nats-py ``Msg`` to terminate.

        Edge cases:
            - Mirrors ``_safe_ack`` / ``_safe_nak``. A term failure is logged
              and swallowed — the message is still gone from this consumer's
              perspective (either it was terminated, or JetStream will
              eventually redeliver it again and this callback will re-evaluate
              the same outcome table).
        """
        try:
            await msg.term()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 — term failure must not crash the consumer
            _logger.warning(
                "NATS message term failed (subject=%s): %s",
                getattr(msg, "subject", "<unknown>"),
                exc,
                exc_info=True,
            )

    # ── Dispatch helpers — mirror KafkaEventBus / InMemoryEventBus ─────────────

    async def _dispatch(self, event: Event, channel: str) -> None:
        """
        Dispatch ``event`` to matching local handlers with error policy applied.

        Mirrors ``KafkaEventBus._dispatch`` — priority-sorted, filter-aware,
        policy-controlled.

        Args:
            event:   The deserialized event from JetStream.
            channel: The channel derived from the JetStream subject.
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
                        "NATS event handler %r raised and was ignored (FIRE_FORGET): %s",
                        entry.handler,
                        exc,
                        exc_info=True,
                    )

        if errors:
            if len(errors) == 1:
                raise errors[0]
            raise ExceptionGroup(  # type: ignore[type-var]
                f"NATS handlers raised {len(errors)} error(s) "
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
            f"NatsEventBus("
            f"servers={self._config.servers!r}, "
            f"stream={self._config.stream_name!r}, "
            f"durable={self._config.durable_name!r}, "
            f"semantics={self._config.delivery_semantics.value!r}, "
            f"subscriptions={active}, "
            f"started={self._started})"
        )


__all__ = ["NatsEventBus"]
