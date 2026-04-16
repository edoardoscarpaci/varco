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

DESIGN: per-client asyncio.Queue + drain task over asyncio.gather
    ✅ Each client drains at its own rate — a slow client never blocks others.
    ✅ ``BackpressurePolicy`` governs full-queue behaviour per client:
       DROP_OLDEST, DROP_NEWEST, BLOCK, or DISCONNECT.
    ✅ ``_handle_event`` returns immediately after enqueuing — the bus handler
       is never blocked by a slow WebSocket send.
    ❌ Memory grows with connected clients × ``max_queue_size``.
    ❌ One background drain task per connected client (Task overhead ≈ 2 KB).

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
Async safety:   ✅ All methods are ``async def`` or run on the event loop.

📚 Docs
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.create_task
  asyncio.create_task — used for per-client drain tasks.
- 🐍 https://docs.python.org/3/library/asyncio-queue.html
  asyncio.Queue — per-client event buffer with backpressure support.
- 📐 https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API
  WebSocket API — browser-side documentation.
"""

from __future__ import annotations

import asyncio
import enum
import json
import logging
import sys
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Callable
from uuid import UUID

from providify import Inject, Singleton

from varco_core.event.base import AbstractEventBus, Event, Subscription

_logger = logging.getLogger(__name__)

# Default per-client queue depth.  At 100 buffered messages, backpressure kicks in.
_DEFAULT_QUEUE_SIZE = 100

# Sentinel placed in a client queue to signal the drain task to exit.
_WS_STOP_SENTINEL = object()


# ── BackpressurePolicy ─────────────────────────────────────────────────────────


class BackpressurePolicy(str, enum.Enum):
    """
    Policy applied when a per-client outbound queue is full.

    DROP_OLDEST  — discard the oldest buffered message to make room for the new one.
                   Good for live feeds where freshness matters more than completeness.
    DROP_NEWEST  — discard the incoming message; the queue contents are preserved.
                   Good when clients must always receive the earliest events in order.
    BLOCK        — ``await`` until space is available; blocks the event handler.
                   Ensures delivery but may stall other clients if many are slow.
    DISCONNECT   — remove the client immediately when its queue is full.
                   Hard limit: a consistently slow client is ejected.

    DESIGN: str + enum.Enum
        ✅ Values are plain strings — JSON-serialisable for config / logging.
        ✅ ``isinstance`` checks work with both enum members and raw strings.
    """

    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"
    BLOCK = "block"
    DISCONNECT = "disconnect"


# ── WebSocketConnection ────────────────────────────────────────────────────────


class WebSocketConnection:
    """
    Represents a single connected WebSocket client.

    Each connection owns a private ``asyncio.Queue`` and a background drain task
    that dequeues messages and sends them via the underlying WebSocket object.

    DESIGN: per-client Queue + drain task over direct send_text
        ✅ The event handler puts messages into the queue and returns immediately —
           slow clients never block the broadcast fan-out.
        ✅ The drain task sends messages in order, one at a time per client.
        ✅ ``BackpressurePolicy`` controls what happens when the queue is full.
        ❌ Each connection spawns one background asyncio.Task (≈ 2 KB overhead).
        ❌ Ordering guarantee is per-client — clients may be at different offsets.

    Thread safety:  ❌ Not thread-safe.
    Async safety:   ✅ All methods are ``async def`` or coroutine-safe.

    Args:
        websocket:          WebSocket object (any library with ``send_text``).
        connection_id:      Optional unique ID.  Defaults to ``id(websocket)``.
        max_queue_size:     Maximum buffered messages.  0 = unbounded.
        backpressure_policy: Policy when queue is full.  Default: DROP_OLDEST.

    Edge cases:
        - ``_start_drain`` must be called before any messages are enqueued.
        - ``_stop_drain`` cancels the drain task and drains the sentinel; safe
          to call even if the drain task already exited on error.
        - If the WebSocket closes while the drain task is running, the task
          calls the ``on_send_error`` callback and exits — the bus removes the
          connection from its active set.
    """

    def __init__(
        self,
        websocket: Any,
        *,
        connection_id: str | None = None,
        max_queue_size: int = _DEFAULT_QUEUE_SIZE,
        backpressure_policy: BackpressurePolicy = BackpressurePolicy.DROP_OLDEST,
    ) -> None:
        """
        Args:
            websocket:           WebSocket object with ``send_text(str)`` method.
            connection_id:       Optional unique connection ID.
            max_queue_size:      Per-client queue depth.  0 = unbounded.
            backpressure_policy: Action when queue is full.
        """
        self._ws = websocket
        self.connection_id = connection_id or str(id(websocket))
        self._policy = backpressure_policy
        # Per-client outbound queue.
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max_queue_size)
        # Drain task — created in _start_drain, cancelled in _stop_drain.
        self._drain_task: asyncio.Task[None] | None = None
        # Callback invoked when send_text raises — set by _start_drain.
        self._on_send_error: Callable[[WebSocketConnection, Exception], None] | None = (
            None
        )

    async def _enqueue(self, message: str) -> bool:
        """
        Put ``message`` into the per-client queue, applying the backpressure policy.

        Args:
            message: JSON-serialised event string.

        Returns:
            ``True`` if the message was accepted (or dropped by policy).
            ``False`` if the client should be disconnected (DISCONNECT policy).

        Edge cases:
            - BLOCK policy may suspend the caller if the queue is full.
            - DROP_OLDEST discards the front of the queue to make room.
            - DROP_NEWEST silently discards the incoming message.
            - DISCONNECT signals the bus to remove this client.
        """
        if self._policy == BackpressurePolicy.BLOCK:
            # Backpressure: caller (event handler) blocks until space available.
            await self._queue.put(message)
            return True

        if not self._queue.full():
            # Fast path: room in the queue — no policy decision needed.
            self._queue.put_nowait(message)
            return True

        # Queue is full — apply policy.
        if self._policy == BackpressurePolicy.DROP_NEWEST:
            # Discard incoming — do nothing.
            _logger.debug(
                "WebSocketConnection %s: queue full — dropped newest message.",
                self.connection_id,
            )
            return True

        if self._policy == BackpressurePolicy.DROP_OLDEST:
            # Evict the oldest item and enqueue the new one.
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass  # Drained concurrently — safe to continue.
            self._queue.put_nowait(message)
            _logger.debug(
                "WebSocketConnection %s: queue full — dropped oldest message.",
                self.connection_id,
            )
            return True

        # DISCONNECT: caller removes this client.
        _logger.warning(
            "WebSocketConnection %s: queue full — disconnecting client.",
            self.connection_id,
        )
        return False

    def _start_drain(
        self,
        on_send_error: Callable[[WebSocketConnection, Exception], None],
    ) -> None:
        """
        Spawn the background drain task.

        The drain task dequeues messages one-by-one and calls
        ``websocket.send_text()``.  On error, it calls ``on_send_error`` and
        exits — the caller is responsible for removing the connection.

        Args:
            on_send_error: Callback invoked with ``(connection, exception)``
                           when ``send_text`` raises.  Called from the event
                           loop; must not block.
        """
        self._on_send_error = on_send_error
        self._drain_task = asyncio.create_task(self._drain_loop())
        # Suppress "Task exception was never retrieved" if the task exits
        # before the caller awaits it.
        self._drain_task.add_done_callback(_suppress_task_exception)

    async def _stop_drain(self) -> None:
        """
        Signal the drain task to stop and wait for it to exit.

        Sends ``_WS_STOP_SENTINEL`` to the queue so the drain loop returns
        cleanly.  Then cancels the task to handle the edge case where the
        queue is full and the sentinel cannot be enqueued.

        Edge cases:
            - Safe to call even if the drain task already exited.
            - If the queue is full, ``put_nowait`` raises — we fall back to
              ``task.cancel()``.
        """
        try:
            self._queue.put_nowait(_WS_STOP_SENTINEL)
        except asyncio.QueueFull:
            pass  # Drain task will be cancelled below.

        if self._drain_task is not None and not self._drain_task.done():
            self._drain_task.cancel()
            try:
                await self._drain_task
            except (asyncio.CancelledError, Exception):
                pass

    async def _drain_loop(self) -> None:
        """
        Background coroutine: dequeue and send messages until stopped.

        Runs until ``_WS_STOP_SENTINEL`` is received or ``send_text`` raises.
        On error, calls the ``on_send_error`` callback so the bus can remove
        this connection.

        Edge cases:
            - ``asyncio.CancelledError`` exits the loop cleanly (called from
              ``_stop_drain``).
            - If ``send_text`` raises, the callback is invoked and the loop
              exits — no retry, no re-queue.
        """
        try:
            while True:
                message = await self._queue.get()
                if message is _WS_STOP_SENTINEL:
                    return
                try:
                    await self._ws.send_text(message)
                except Exception as exc:
                    if self._on_send_error is not None:
                        self._on_send_error(self, exc)
                    return
        except asyncio.CancelledError:
            return

    async def send(self, message: str) -> None:
        """
        Send a text message directly to this WebSocket client, bypassing the queue.

        .. note::
            This method is retained for backward compatibility and direct usage.
            In ``WebSocketEventBus``, messages are routed through ``_enqueue``
            and the drain task instead.

        Args:
            message: JSON-serialised event string.

        Raises:
            Exception: Any exception from the underlying WebSocket send.
        """
        await self._ws.send_text(message)

    def __repr__(self) -> str:
        return (
            f"WebSocketConnection("
            f"id={self.connection_id!r}, "
            f"queue_size={self._queue.qsize()}, "
            f"policy={self._policy.value!r})"
        )


def _suppress_task_exception(task: asyncio.Task[Any]) -> None:
    """Done callback: retrieve the exception so Python doesn't log it."""
    if not task.cancelled():
        task.exception()  # Marks the exception as retrieved.


# ── WebSocketEventBus ─────────────────────────────────────────────────────────


@Singleton(priority=-sys.maxsize)
class WebSocketEventBus:
    """
    Push adapter that broadcasts varco events to connected WebSocket clients.

    Subscribes to a given ``AbstractEventBus`` at ``start()`` time and pushes
    each received event as a JSON message to all currently-connected clients.
    Each client has its own outbound ``asyncio.Queue`` — a slow or stalled
    client never blocks other clients.

    Lifecycle::

        ws_bus = WebSocketEventBus(bus, event_type=OrderEvent, channel="orders")
        await ws_bus.start()

        # Clients connect via ws_bus.connect(websocket)

        await ws_bus.stop()

    DESIGN: one bus subscription + per-client queue + per-client drain task
        ✅ O(1) bus subscriptions regardless of connected clients.
        ✅ Fan-out is non-blocking: ``_handle_event`` enqueues and returns fast.
        ✅ Per-client ``BackpressurePolicy`` handles slow consumers gracefully.
        ❌ One ``asyncio.Task`` per connected client (≈ 2 KB overhead each).

    Thread safety:  ❌ Not thread-safe — all operations from one event loop.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        bus:                 The underlying ``AbstractEventBus`` to subscribe to.
        event_type:          Event class to forward.  Defaults to ``Event``.
        channel:             Bus channel.  Defaults to ``"*"`` (all).
        max_queue_size:      Per-client queue depth.  Default: 100.
        backpressure_policy: Default policy when a client queue is full.
                             Default: DROP_OLDEST.

    Edge cases:
        - Events are discarded if no clients are connected at delivery time.
        - A ``send_text`` error causes the drain task to remove the client;
          the framework-level WebSocket is NOT closed — that is the caller's job.
        - ``stop()`` cancels the bus subscription.  Existing clients remain
          registered but receive no further events until new ones arrive (they
          won't, since the subscription is cancelled).
    """

    def __init__(
        self,
        # DI injects the AbstractEventBus singleton when constructed by scan.
        bus: Inject[AbstractEventBus],  # type: ignore[type-arg]
        *,
        event_type: type[Event] = Event,
        channel: str = "*",
        max_queue_size: int = _DEFAULT_QUEUE_SIZE,
        backpressure_policy: BackpressurePolicy = BackpressurePolicy.DROP_OLDEST,
    ) -> None:
        """
        Args:
            bus:                 Underlying event bus.  Injected by DI when scan-discovered.
            event_type:          Event class to subscribe to.  Default: all events.
            channel:             Bus channel.  Default: all channels.
            max_queue_size:      Per-client outbound queue depth.
            backpressure_policy: Action when a client queue is full.
        """
        self._bus = bus
        self._event_type = event_type
        self._channel = channel
        self._max_queue_size = max_queue_size
        self._backpressure_policy = backpressure_policy
        # Connected clients — set for O(1) add/remove.
        self._connections: set[WebSocketConnection] = set()
        # Bus subscription handle — set in start(), cancelled in stop().
        self._subscription: Subscription | None = None

    async def start(self) -> None:
        """
        Subscribe to the bus and begin routing events to connected clients.

        Idempotent — second call is a no-op if already started.
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
        Cancel the bus subscription.

        Existing client connections are NOT closed — they stop receiving events.
        Call ``disconnect_all()`` before this to clean up drain tasks.

        Idempotent — safe to call multiple times.
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
        max_queue_size: int | None = None,
        backpressure_policy: BackpressurePolicy | None = None,
    ) -> AsyncIterator[WebSocketConnection]:
        """
        Register a WebSocket client, start its drain task, and yield a handle.

        The client is automatically unregistered and its drain task stopped
        when the context exits (even on exception).

        Args:
            websocket:           WebSocket object with ``send_text(str)`` method.
            connection_id:       Optional ID for this connection.
            max_queue_size:      Override the bus-level ``max_queue_size``.
            backpressure_policy: Override the bus-level ``backpressure_policy``.

        Yields:
            ``WebSocketConnection`` handle.

        Edge cases:
            - If ``start()`` was never called, messages are enqueued but never
              delivered (no bus subscription).
            - The context manager does NOT call ``websocket.close()`` on exit.

        Example (FastAPI)::

            @app.websocket("/ws")
            async def endpoint(ws: WebSocket):
                await ws.accept()
                async with ws_bus.connect(ws):
                    while True:
                        await asyncio.sleep(30)   # keepalive
        """
        conn = WebSocketConnection(
            websocket,
            connection_id=connection_id,
            max_queue_size=(
                max_queue_size if max_queue_size is not None else self._max_queue_size
            ),
            backpressure_policy=(
                backpressure_policy
                if backpressure_policy is not None
                else self._backpressure_policy
            ),
        )
        # Start drain task before registering so no events are lost.
        conn._start_drain(self._on_client_send_error)
        self._connections.add(conn)
        _logger.debug(
            "WebSocketEventBus: client connected (id=%s, total=%d)",
            conn.connection_id,
            len(self._connections),
        )
        try:
            yield conn
        finally:
            self._connections.discard(conn)
            await conn._stop_drain()
            _logger.debug(
                "WebSocketEventBus: client disconnected (id=%s, remaining=%d)",
                conn.connection_id,
                len(self._connections),
            )

    def _on_client_send_error(self, conn: WebSocketConnection, exc: Exception) -> None:
        """
        Callback from a client's drain task when ``send_text`` fails.

        Removes the connection from the active set so the next broadcast
        skips it.  The drain task has already exited.

        Args:
            conn: The failing connection.
            exc:  The exception raised by ``send_text``.
        """
        _logger.warning(
            "WebSocketEventBus: send to client %s failed: %s — disconnecting.",
            conn.connection_id,
            exc,
        )
        self._connections.discard(conn)

    async def disconnect_all(self) -> None:
        """
        Stop all drain tasks and remove all connected clients.

        Does NOT close underlying WebSocket connections — the framework
        manages connection lifecycle.  After this call, events are discarded
        until new clients connect.
        """
        conns = list(self._connections)
        self._connections.clear()
        for conn in conns:
            await conn._stop_drain()

    @property
    def connected_count(self) -> int:
        """Number of currently connected WebSocket clients."""
        return len(self._connections)

    # ── Internal event handler ─────────────────────────────────────────────────

    async def _handle_event(self, event: Event) -> None:
        """
        Called by the bus for each matching event.

        Serialises the event and enqueues it into each active client's
        ``asyncio.Queue``.  Returns immediately — per-client drain tasks
        handle the actual ``send_text`` calls asynchronously.

        DESIGN: enqueue-and-return over gather
            ✅ Bus handler is never blocked by a slow client.
            ✅ Per-client backpressure policy controls what happens when full.
            ❌ Clients receive events slightly later than with direct send —
               one event-loop iteration of delay per drain task wake-up.

        Args:
            event: The event received from the bus.

        Edge cases:
            - If ``_connections`` is empty, returns immediately (no allocation).
            - DISCONNECT-policy clients that return ``False`` from ``_enqueue``
              are removed inline; their drain task is also stopped.
        """
        if not self._connections:
            return

        message = self._serialise(event)
        to_disconnect: list[WebSocketConnection] = []

        for conn in list(self._connections):
            ok = await conn._enqueue(message)
            if not ok:
                to_disconnect.append(conn)

        for conn in to_disconnect:
            self._connections.discard(conn)
            await conn._stop_drain()

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
            - UUID objects are serialised as canonical string form.
            - datetime, date, Enum — fall back to ``str()``.
        """

        def _default(obj: Any) -> Any:
            if isinstance(obj, UUID):
                return str(obj)
            return str(obj)

        payload = {
            # __event_type__ is a ClassVar — access via the class, not the
            # instance.  Pydantic raises AttributeError on instance access
            # for ClassVar attributes.
            "event_type": type(event).event_type_name(),
            "event_id": str(event.event_id),
            "data": event.model_dump(),
        }
        return json.dumps(payload, default=_default)

    def __repr__(self) -> str:
        return (
            f"WebSocketEventBus("
            f"event_type={self._event_type.__name__!r}, "
            f"channel={self._channel!r}, "
            f"clients={self.connected_count}, "
            f"policy={self._backpressure_policy.value!r})"
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "BackpressurePolicy",
    "WebSocketConnection",
    "WebSocketEventBus",
]
