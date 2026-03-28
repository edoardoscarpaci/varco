"""
varco_ws.websocket
==================
WebSocket push adapter for the varco event system.

``WebSocketEventBus`` is **not** a standalone ``AbstractEventBus`` — it is
a push adapter that subscribes to an existing ``AbstractEventBus`` and
broadcasts serialised events to all connected WebSocket clients.

DESIGN: adapter over subclass of AbstractEventBus
    ✅ Separation of concerns — the underlying bus (Kafka, Redis, in-memory)
       handles routing, retries, DLQ.  The adapter only handles the push layer.
    ✅ Application code keeps injecting ``AbstractEventBus`` — the adapter is
       a sidecar that shares the same bus reference.
    ✅ Multiple WebSocket clients can connect to the same adapter simultaneously.
    ❌ Not an AbstractEventBus — cannot be passed where a bus is expected.
       Use the underlying bus for service-to-service messaging.

Wire-up pattern::

    # FastAPI example
    from varco_ws.websocket import WebSocketEventBus

    # One adapter per event type/channel (or one for all events)
    ws_bus = WebSocketEventBus(bus, event_type=OrderEvent, channel="orders")

    @app.on_event("startup")
    async def startup():
        await ws_bus.start()

    @app.websocket("/ws/orders")
    async def orders_ws(websocket: WebSocket):
        await websocket.accept()
        async with ws_bus.connect(websocket):
            await asyncio.sleep(3600)    # keep alive until client disconnects

    @app.on_event("shutdown")
    async def shutdown():
        await ws_bus.stop()

Message format
--------------
Each event is serialised to a JSON string::

    {
        "event_type": "order.placed",
        "event_id": "...",
        "data": { ... }
    }

The ``data`` field is the event's model_dump() output.

Thread safety:  ❌ Not thread-safe — use from a single event loop.
Async safety:   ✅ All methods are ``async def`` or acquire the internal set
                    under asyncio coordination (no threading primitives needed).

📚 Docs
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
  asyncio.create_task — used for broadcast tasks; strong ref stored to prevent GC.
- 📐 https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API
  WebSocket API — browser-side documentation.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator
from uuid import UUID

from varco_core.event.base import AbstractEventBus, Event, Subscription

_logger = logging.getLogger(__name__)


# ── WebSocketConnection ────────────────────────────────────────────────────────


class WebSocketConnection:
    """
    Represents a single connected WebSocket client.

    The connection holds a reference to the underlying WebSocket object and
    provides a ``send(event)`` method that serialises and sends the event.

    DESIGN: thin wrapper over the raw WebSocket
        ✅ Decouples ``WebSocketEventBus`` from any specific WebSocket library
           (FastAPI, Starlette, aiohttp, etc.).  The wrapper only calls
           ``send_text(str)`` — a method present in all major libraries.
        ✅ The ``connection_id`` allows targeted disconnection.
        ❌ No reconnect logic — reconnection is the client's responsibility.

    Thread safety:  ❌ Not thread-safe.
    Async safety:   ✅ ``send()`` is ``async def``.

    Args:
        websocket: The underlying WebSocket object.  Must have a
                   ``send_text(str) -> Awaitable[None]`` method.
        connection_id: Optional unique ID.  Defaults to a UUID.

    Edge cases:
        - If the WebSocket is closed when ``send()`` is called, the exception
          propagates to the caller (``WebSocketEventBus._broadcast``).
        - ``send()`` does not buffer — slow clients receive events at the rate
          they can consume them.
    """

    def __init__(self, websocket: Any, *, connection_id: str | None = None) -> None:
        """
        Args:
            websocket:     WebSocket object (any library with ``send_text``).
            connection_id: Optional unique ID for this connection.
        """
        self._ws = websocket
        self.connection_id = connection_id or str(id(websocket))

    async def send(self, message: str) -> None:
        """
        Send a text message to this WebSocket client.

        Args:
            message: JSON-serialised event string.

        Raises:
            Exception: Any exception from the underlying WebSocket send
                       (e.g. connection closed, network error).
        """
        await self._ws.send_text(message)

    def __repr__(self) -> str:
        return f"WebSocketConnection(id={self.connection_id!r})"


# ── WebSocketEventBus ─────────────────────────────────────────────────────────


class WebSocketEventBus:
    """
    Push adapter that broadcasts varco events to connected WebSocket clients.

    Subscribes to a given ``AbstractEventBus`` at ``start()`` time and pushes
    each received event as a JSON message to all currently-connected clients.

    Lifecycle::

        ws_bus = WebSocketEventBus(bus, event_type=OrderEvent, channel="orders")
        await ws_bus.start()

        # Clients connect via ws_bus.connect(websocket)

        await ws_bus.stop()

    DESIGN: one subscription per WebSocketEventBus over one per connected client
        ✅ O(1) bus subscriptions regardless of the number of WebSocket clients.
        ✅ Broadcast is fan-out at the adapter level — the bus only delivers once.
        ❌ A single slow client's ``send_text`` call blocks the broadcast loop
           for other clients.  Use ``asyncio.gather`` for concurrent sends (see
           ``_broadcast``).

    Thread safety:  ❌ Not thread-safe — all operations from one event loop.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        bus:        The underlying ``AbstractEventBus`` to subscribe to.
        event_type: The event class to forward.  Defaults to ``Event`` (all events).
        channel:    The bus channel to subscribe to.  Defaults to ``"*"`` (all).
        queue_size: Per-client queue size for backpressure (unused in WebSocket
                    mode — see SSEEventBus for per-client queuing).

    Edge cases:
        - If no clients are connected, events are discarded after the bus
          delivers them (the broadcast loop runs but ``_connections`` is empty).
        - A ``send_text`` error for one client is logged and the client is
          disconnected — other clients are not affected.
        - ``stop()`` cancels the bus subscription.  Existing clients remain
          connected but receive no further events.
    """

    def __init__(
        self,
        bus: AbstractEventBus,
        *,
        event_type: type[Event] = Event,
        channel: str = "*",
    ) -> None:
        """
        Args:
            bus:        Underlying event bus.
            event_type: Event class to subscribe to.  Default: all events.
            channel:    Bus channel to subscribe to.  Default: all channels.
        """
        self._bus = bus
        self._event_type = event_type
        self._channel = channel
        # Connected clients — set for O(1) add/remove.
        self._connections: set[WebSocketConnection] = set()
        # Bus subscription handle — set in start(), cancelled in stop().
        self._subscription: Subscription | None = None
        # Strong references to broadcast tasks — prevents GC during send.
        # asyncio docs recommend keeping references to tasks while they run.
        self._pending_tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        """
        Subscribe to the bus and begin routing events to connected clients.

        Must be called before any clients connect.  Idempotent — calling
        a second time is a no-op if already started.

        Edge cases:
            - Calling start() while already started is a no-op.
        """
        if self._subscription is not None:
            return
        self._subscription = self._bus.subscribe(
            self._event_type,
            self._handle_event,
            channel=self._channel,
        )
        _logger.info(
            "WebSocketEventBus started (event_type=%s, channel=%r)",
            self._event_type.__name__,
            self._channel,
        )

    async def stop(self) -> None:
        """
        Cancel the bus subscription.  Existing client connections are not
        closed — they simply stop receiving new events.

        Idempotent — safe to call multiple times.

        Edge cases:
            - Clients that connected after stop() will never receive events
              (the subscription is cancelled).  Use ``disconnect_all()`` before
              stop() to close existing clients gracefully.
        """
        if self._subscription is None:
            return
        self._subscription.cancel()
        self._subscription = None
        _logger.info("WebSocketEventBus stopped.")

    async def __aenter__(self) -> WebSocketEventBus:
        """Start on context-manager entry."""
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Stop on context-manager exit."""
        await self.stop()

    @asynccontextmanager
    async def connect(
        self,
        websocket: Any,
        *,
        connection_id: str | None = None,
    ) -> AsyncIterator[WebSocketConnection]:
        """
        Register a WebSocket client and yield a ``WebSocketConnection`` handle.

        The client is automatically disconnected when the context exits (even
        on exception).

        Args:
            websocket:     WebSocket object with ``send_text(str)`` method.
            connection_id: Optional ID for this connection.

        Yields:
            ``WebSocketConnection`` handle.

        Edge cases:
            - If ``start()`` was never called, events are not routed — register
              before connecting clients.
            - The context manager does NOT call ``websocket.close()`` on exit —
              framework-level teardown is the caller's responsibility.

        Example (FastAPI)::

            @app.websocket("/ws")
            async def endpoint(ws: WebSocket):
                await ws.accept()
                async with ws_bus.connect(ws):
                    while True:
                        await asyncio.sleep(30)   # send keepalives or wait
        """
        conn = WebSocketConnection(websocket, connection_id=connection_id)
        self._connections.add(conn)
        _logger.debug("WebSocketEventBus: client connected (id=%s)", conn.connection_id)
        try:
            yield conn
        finally:
            self._connections.discard(conn)
            _logger.debug(
                "WebSocketEventBus: client disconnected (id=%s)", conn.connection_id
            )

    async def disconnect_all(self) -> None:
        """
        Remove all connected clients from the connection set.

        Does NOT close underlying WebSocket connections — the framework
        manages connection lifecycle.  After this call, events are discarded
        until new clients connect.
        """
        self._connections.clear()

    @property
    def connected_count(self) -> int:
        """Number of currently connected WebSocket clients."""
        return len(self._connections)

    # ── Internal event handler ─────────────────────────────────────────────────

    async def _handle_event(self, event: Event) -> None:
        """
        Called by the bus for each matching event.  Schedules a broadcast task.

        DESIGN: schedule broadcast as a Task over awaiting inline
            ✅ The bus handler returns quickly — slow clients don't block the
               bus delivery loop.
            ✅ Strong reference in ``_pending_tasks`` prevents GC mid-broadcast.
            ❌ Broadcast is "fire and forget" from the bus perspective — errors
               are logged but not re-raised.

        Args:
            event: The event received from the bus.
        """
        if not self._connections:
            return  # No clients — discard immediately.

        message = self._serialise(event)
        # Create broadcast task and keep a strong reference.
        task = asyncio.create_task(self._broadcast(message))
        self._pending_tasks.add(task)
        # Remove the task from the set when done — prevents the set growing forever.
        task.add_done_callback(self._pending_tasks.discard)

    async def _broadcast(self, message: str) -> None:
        """
        Send ``message`` to all connected clients concurrently.

        Disconnects clients that raise during send — network errors or
        closed connections should not block other clients.

        Args:
            message: JSON string to send to all clients.

        Edge cases:
            - A client that raises is removed from ``_connections`` and logged.
              Other clients are not affected.
            - If ``_connections`` is empty by broadcast time, this is a no-op.
        """
        # Snapshot the connection set — avoid "set changed size during iteration"
        # if a client disconnects concurrently.
        connections = list(self._connections)
        if not connections:
            return

        # Broadcast to all clients concurrently — one slow send doesn't block others.
        results = await asyncio.gather(
            *(conn.send(message) for conn in connections),
            return_exceptions=True,
        )

        for conn, result in zip(connections, results):
            if isinstance(result, Exception):
                _logger.warning(
                    "WebSocketEventBus: send to client %s failed: %s — disconnecting.",
                    conn.connection_id,
                    result,
                )
                self._connections.discard(conn)

    @staticmethod
    def _serialise(event: Event) -> str:
        """
        Serialise an event to a JSON string for WebSocket transmission.

        Format::

            {
                "event_type": "order.placed",
                "event_id": "<uuid>",
                "data": { ... }
            }

        Args:
            event: The event to serialise.

        Returns:
            JSON string.

        Edge cases:
            - ``event.model_dump()`` may include datetime objects — these are
              serialised by the ``default`` encoder below.
            - UUID objects are serialised as their canonical string form.
        """

        def _default(obj: Any) -> Any:
            """JSON encoder fallback for non-standard types."""
            if isinstance(obj, UUID):
                return str(obj)
            # datetime, date, Enum — try __str__ as a safe fallback.
            return str(obj)

        payload = {
            "event_type": event.__event_type__,
            "event_id": str(event.event_id),
            "data": event.model_dump(),
        }
        return json.dumps(payload, default=_default)

    def __repr__(self) -> str:
        return (
            f"WebSocketEventBus("
            f"event_type={self._event_type.__name__!r}, "
            f"channel={self._channel!r}, "
            f"clients={self.connected_count})"
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "WebSocketConnection",
    "WebSocketEventBus",
]
