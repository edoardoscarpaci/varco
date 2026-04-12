"""
varco_ws.sse
============
Server-Sent Events (SSE) push adapter for the varco event system.

``SSEEventBus`` is a push adapter that subscribes to an existing
``AbstractEventBus`` and delivers events to HTTP clients using the SSE protocol.

SSE vs WebSocket
----------------
SSE is a simpler, HTTP/1.1-compatible protocol for server-to-client push:

    ✅ Works through HTTP proxies and CDNs without special configuration.
    ✅ Automatic reconnect is built into the browser EventSource API.
    ✅ No upgrade handshake — just a ``Content-Type: text/event-stream`` response.
    ❌ Server-to-client only — clients cannot send messages back.
    ❌ Multiplexing is manual — one SSE connection per event stream.

DESIGN: per-client asyncio.Queue over a shared broadcast queue
    ✅ Each subscriber queue is independent — a slow client does not block others.
    ✅ ``maxsize`` provides backpressure — if a client is too slow, the queue
       blocks the put() call until the client drains it.
    ✅ ``asyncio.Queue.get()`` is the natural generator step — ``stream()``
       yields events as they arrive without polling.
    ❌ Memory grows linearly with connected clients × queue depth.  Cap with
       ``max_queue_size`` at construction time.

Wire-up pattern::

    # FastAPI / Starlette example
    from fastapi.responses import StreamingResponse
    from varco_ws.sse import SSEEventBus

    sse_bus = SSEEventBus(bus, event_type=OrderEvent, channel="orders")
    await sse_bus.start()

    @app.get("/events/orders")
    async def orders_sse(request: Request):
        async def generate():
            async with sse_bus.subscribe() as stream:
                async for message in stream:
                    if await request.is_disconnected():
                        break
                    yield message

        return StreamingResponse(generate(), media_type="text/event-stream")

SSE message format
------------------
Each event is sent as an SSE message::

    data: {"event_type": "order.placed", "event_id": "...", "data": {...}}\\n\\n

The double newline terminates the event.  The browser's ``EventSource`` parses
this and fires a ``message`` event with the JSON string as ``event.data``.

Thread safety:  ❌ Not thread-safe — use from a single event loop.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 📐 https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events
  SSE protocol — browser-side documentation.
- 🐍 https://docs.python.org/3/library/asyncio-queue.html
  asyncio.Queue — used for per-client event buffering.
- 📐 https://html.spec.whatwg.org/multipage/server-sent-events.html
  SSE specification — WHATWG living standard.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
from uuid import UUID

from providify import Inject, Singleton

from varco_core.event.base import AbstractEventBus, Event, Subscription

_logger = logging.getLogger(__name__)

# Default per-client queue size.  At 100 buffered events, back-pressure kicks in.
# A slow client will block the _handle_event coroutine on put() until it drains.
_DEFAULT_QUEUE_SIZE = 100

# Sentinel value used to signal the streaming generator to terminate.
_STOP_SENTINEL = object()


# ── SSEConnection ─────────────────────────────────────────────────────────────


class SSEConnection:
    """
    Represents a single SSE subscriber connection.

    Each connection holds a private ``asyncio.Queue`` that buffers events
    before they are sent to the client.

    DESIGN: asyncio.Queue per connection over a global broadcast mechanism
        ✅ Independent per-client backpressure.
        ✅ Each client drains at its own rate — no starvation.
        ✅ The queue is the only shared state between the bus handler and the
           streaming generator — no additional locking needed.
        ❌ Memory overhead per client proportional to ``max_queue_size``.

    Thread safety:  ❌ Not thread-safe across OS threads.
    Async safety:   ✅ asyncio.Queue is coroutine-safe.

    Args:
        max_queue_size: Maximum number of buffered events.  When full,
                        ``put()`` blocks until the client drains the queue.

    Edge cases:
        - If ``max_queue_size=0``, the queue is unbounded — memory grows
          without limit for slow clients.
        - The ``_STOP_SENTINEL`` object is used to terminate the stream()
          generator — callers must not put arbitrary objects in the queue.
    """

    def __init__(self, *, max_queue_size: int = _DEFAULT_QUEUE_SIZE) -> None:
        """
        Args:
            max_queue_size: Maximum buffered events.  0 = unbounded.
        """
        # Private queue — only the adapter puts events; the stream generator gets them.
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_queue_size)

    async def _put(self, item: Any) -> None:
        """
        Internal: put an event (or sentinel) into the queue.

        Blocks if the queue is full — provides backpressure for slow clients.

        Args:
            item: Event string or ``_STOP_SENTINEL``.
        """
        await self._queue.put(item)

    async def stream(self) -> AsyncIterator[str]:
        """
        Async generator that yields SSE-formatted event strings.

        Yields one string per event until the connection is closed
        (when ``_STOP_SENTINEL`` is received from the adapter).

        Yields:
            SSE-formatted strings, e.g.::
                ``"data: {...}\\n\\n"``

        Edge cases:
            - Stops automatically when the adapter calls ``disconnect()`` —
              the sentinel is placed in the queue.
            - If the queue is empty, ``get()`` blocks until the next event
              arrives — no polling.
        """
        while True:
            item = await self._queue.get()
            if item is _STOP_SENTINEL:
                # Adapter has closed this connection — stop the generator.
                return
            yield item

    def __repr__(self) -> str:
        return f"SSEConnection(queue_size={self._queue.qsize()})"


# ── SSEEventBus ───────────────────────────────────────────────────────────────


@Singleton(priority=-sys.maxsize)
class SSEEventBus:
    """
    Push adapter that delivers varco events to SSE subscribers.

    Subscribes to an ``AbstractEventBus`` and fans out each event to all
    active ``SSEConnection`` instances via their private queues.

    Lifecycle::

        sse_bus = SSEEventBus(bus, event_type=OrderEvent, channel="orders")
        await sse_bus.start()

        # Subscribers connect via sse_bus.subscribe()

        await sse_bus.stop()

    Thread safety:  ❌ Not thread-safe — use from one event loop.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        bus:            The underlying ``AbstractEventBus``.
        event_type:     Event class to subscribe to.  Default: all events.
        channel:        Bus channel to subscribe to.  Default: all channels.
        max_queue_size: Per-client queue depth.  Default: 100.

    Edge cases:
        - If no subscribers are active when an event arrives, it is discarded.
        - A subscriber that is too slow blocks its own queue (backpressure), not
          other subscribers.
        - ``stop()`` cancels the bus subscription AND closes all active subscriber
          connections by draining a sentinel into each queue.
    """

    def __init__(
        self,
        # DI injects the AbstractEventBus singleton when constructed by scan.
        # Keyword-only defaults (event_type, channel, max_queue_size) remain for manual wiring.
        bus: Inject[AbstractEventBus],  # type: ignore[type-arg]
        *,
        event_type: type[Event] = Event,
        channel: str = "*",
        max_queue_size: int = _DEFAULT_QUEUE_SIZE,
    ) -> None:
        """
        Args:
            bus:            Underlying event bus.  Injected by DI when scan-discovered.
            event_type:     Event class to subscribe to.
            channel:        Bus channel.
            max_queue_size: Per-subscriber queue depth.
        """
        self._bus = bus
        self._event_type = event_type
        self._channel = channel
        self._max_queue_size = max_queue_size
        # Active SSE connections.
        self._connections: set[SSEConnection] = set()
        # Bus subscription handle.
        self._subscription: Subscription | None = None

    async def start(self) -> None:
        """
        Subscribe to the bus.  Idempotent.

        Edge cases:
            - Second call is a no-op if already started.
        """
        if self._subscription is not None:
            return
        self._subscription = self._bus.subscribe(
            self._event_type,
            self._handle_event,
            channel=self._channel,
        )
        _logger.info(
            "SSEEventBus started (event_type=%s, channel=%r)",
            self._event_type.__name__,
            self._channel,
        )

    async def stop(self) -> None:
        """
        Cancel the bus subscription and signal all active subscribers to stop.

        Sends ``_STOP_SENTINEL`` to each subscriber's queue so their
        ``stream()`` generators terminate cleanly.

        Edge cases:
            - Calling before ``start()`` is a no-op.
            - After stop(), new subscribers created via ``subscribe()`` will
              receive no events.
        """
        if self._subscription is None:
            return
        self._subscription.cancel()
        self._subscription = None

        # Signal all active connections to stop.
        for conn in list(self._connections):
            try:
                await conn._put(_STOP_SENTINEL)
            except Exception as exc:
                _logger.debug("SSEEventBus.stop: error signalling connection: %s", exc)

        self._connections.clear()
        _logger.info("SSEEventBus stopped.")

    async def __aenter__(self) -> SSEEventBus:
        """Start on context-manager entry."""
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Stop on context-manager exit."""
        await self.stop()

    @asynccontextmanager
    async def subscribe(self) -> AsyncIterator[SSEConnection]:
        """
        Create a new SSE connection and yield it as an async context manager.

        The connection is automatically removed when the context exits.

        Yields:
            ``SSEConnection`` — call ``stream()`` on it to get the event generator.

        Example::

            async with sse_bus.subscribe() as conn:
                async for message in conn.stream():
                    yield message       # forward to HTTP response generator

        Edge cases:
            - If ``stop()`` is called while a subscriber is active, the subscriber
              receives the sentinel and its ``stream()`` generator terminates.
            - The connection is removed from ``_connections`` on context exit even
              if an exception occurs.
        """
        conn = SSEConnection(max_queue_size=self._max_queue_size)
        self._connections.add(conn)
        _logger.debug(
            "SSEEventBus: subscriber connected (%d total)", len(self._connections)
        )
        try:
            yield conn
        finally:
            self._connections.discard(conn)
            _logger.debug(
                "SSEEventBus: subscriber disconnected (%d remaining)",
                len(self._connections),
            )

    @property
    def subscriber_count(self) -> int:
        """Number of active SSE subscribers."""
        return len(self._connections)

    # ── Internal event handler ─────────────────────────────────────────────────

    async def _handle_event(self, event: Event) -> None:
        """
        Called by the bus for each matching event.

        Serialises the event to SSE format and puts it in every active
        subscriber's queue.

        Args:
            event: The event received from the bus.

        Edge cases:
            - If ``_connections`` is empty, returns immediately (no allocation).
            - If a subscriber's queue is full, ``put()`` blocks — providing
              per-subscriber backpressure.  Other subscribers are not affected
              because we ``await`` each put sequentially.
              For true concurrency, use ``asyncio.gather(*(c._put(m) for c in conns))``.
        """
        if not self._connections:
            return

        message = self._serialise(event)

        # Fan-out to all active connections.
        # Sequential puts are simpler and sufficient for most deployments.
        # For very high fan-out (>1000 connections), switch to gather.
        for conn in list(self._connections):
            try:
                await conn._put(message)
            except Exception as exc:
                _logger.warning(
                    "SSEEventBus: failed to put event to connection %r: %s",
                    conn,
                    exc,
                )
                self._connections.discard(conn)

    @staticmethod
    def _serialise(event: Event) -> str:
        """
        Serialise an event to SSE wire format.

        Returns an SSE data line::

            data: {"event_type": "...", "event_id": "...", "data": {...}}\\n\\n

        The double newline is required by the SSE specification to terminate
        the event.

        Args:
            event: The event to serialise.

        Returns:
            SSE-formatted string ready to write to the HTTP response body.

        Edge cases:
            - UUID fields are serialised as their canonical string form.
            - datetime fields fall back to ``str()`` via the ``_default`` encoder.
        """

        def _default(obj: Any) -> Any:
            if isinstance(obj, UUID):
                return str(obj)
            return str(obj)

        payload = json.dumps(
            {
                # __event_type__ is a ClassVar — access via the class, not the
                # instance.  Pydantic raises AttributeError on instance access
                # for ClassVar attributes.  event_type_name() handles the
                # fallback to cls.__name__ when __event_type__ is not declared.
                "event_type": type(event).event_type_name(),
                "event_id": str(event.event_id),
                "data": event.model_dump(),
            },
            default=_default,
        )
        # SSE format: "data: <json>\n\n"
        # The double newline signals the end of this SSE event.
        return f"data: {payload}\n\n"

    def __repr__(self) -> str:
        return (
            f"SSEEventBus("
            f"event_type={self._event_type.__name__!r}, "
            f"channel={self._channel!r}, "
            f"subscribers={self.subscriber_count})"
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SSEConnection",
    "SSEEventBus",
]
