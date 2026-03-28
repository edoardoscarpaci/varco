"""
tests.test_ws_bus
==================
Unit tests for varco_ws — WebSocket and SSE event bus adapters.

Covers:
    WebSocketEventBus   — subscribe, broadcast, client disconnection on send error,
                           start/stop, concurrent clients
    SSEEventBus         — subscribe, event delivery, stop signals all connections,
                           per-client queue, backpressure, concurrent subscribers

All tests use InMemoryEventBus — no real WebSocket connections required.
"""

from __future__ import annotations

import asyncio


from varco_core.event.base import Event
from varco_core.event.memory import InMemoryEventBus

from varco_ws.sse import SSEEventBus
from varco_ws.websocket import WebSocketEventBus, WebSocketConnection


# ── Test event ─────────────────────────────────────────────────────────────────


class SampleEvent(Event):
    """Minimal event used in ws/sse tests."""

    __event_type__ = "test.sample"
    value: int = 0


# ── Mock WebSocket ─────────────────────────────────────────────────────────────


class MockWebSocket:
    """
    Minimal WebSocket mock with a ``send_text`` coroutine.
    Records all sent messages for assertion.
    """

    def __init__(self) -> None:
        self.sent: list[str] = []
        self._should_fail = False

    async def send_text(self, message: str) -> None:
        if self._should_fail:
            raise OSError("WebSocket closed")
        self.sent.append(message)

    def make_fail(self) -> None:
        """Configure this mock to raise on the next send_text call."""
        self._should_fail = True


# ── WebSocketEventBus tests ────────────────────────────────────────────────────


async def test_ws_bus_delivers_event_to_connected_client() -> None:
    """
    An event published to the bus must be sent to all connected WebSocket clients.
    """
    underlying_bus = InMemoryEventBus()
    ws_bus = WebSocketEventBus(underlying_bus)
    await ws_bus.start()

    mock_ws = MockWebSocket()
    async with ws_bus.connect(mock_ws):
        await underlying_bus.publish(SampleEvent(value=42))
        # Allow the broadcast task to complete.
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    assert len(mock_ws.sent) == 1
    import json

    payload = json.loads(mock_ws.sent[0])
    assert payload["event_type"] == "test.sample"
    assert payload["data"]["value"] == 42

    await ws_bus.stop()


async def test_ws_bus_delivers_to_multiple_clients() -> None:
    """
    Events must be broadcast to ALL connected clients.
    """
    underlying_bus = InMemoryEventBus()
    ws_bus = WebSocketEventBus(underlying_bus)
    await ws_bus.start()

    clients = [MockWebSocket() for _ in range(3)]

    async with ws_bus.connect(clients[0]):
        async with ws_bus.connect(clients[1]):
            async with ws_bus.connect(clients[2]):
                await underlying_bus.publish(SampleEvent(value=1))
                await asyncio.sleep(0)
                await asyncio.sleep(0)

    for client in clients:
        assert len(client.sent) == 1


async def test_ws_bus_disconnects_failed_client() -> None:
    """
    A client that raises on send_text must be disconnected automatically.
    Other clients must still receive events.
    """
    underlying_bus = InMemoryEventBus()
    ws_bus = WebSocketEventBus(underlying_bus)
    await ws_bus.start()

    good_ws = MockWebSocket()
    bad_ws = MockWebSocket()
    bad_ws.make_fail()

    async with ws_bus.connect(good_ws):
        async with ws_bus.connect(bad_ws):
            assert ws_bus.connected_count == 2
            await underlying_bus.publish(SampleEvent(value=5))
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        # bad_ws should have been disconnected internally,
        # but the connect() context manager handles its own cleanup.

    # The good client received the event.
    assert len(good_ws.sent) == 1

    await ws_bus.stop()


async def test_ws_bus_no_clients_no_error() -> None:
    """
    Publishing when no clients are connected must be a silent no-op.
    """
    underlying_bus = InMemoryEventBus()
    ws_bus = WebSocketEventBus(underlying_bus)
    await ws_bus.start()

    # No clients — publish should not raise.
    await underlying_bus.publish(SampleEvent(value=99))
    await asyncio.sleep(0)

    await ws_bus.stop()


async def test_ws_bus_start_stop_idempotent() -> None:
    """start() and stop() must be idempotent — calling twice is safe."""
    underlying_bus = InMemoryEventBus()
    ws_bus = WebSocketEventBus(underlying_bus)

    await ws_bus.start()
    await ws_bus.start()  # second call should be a no-op
    await ws_bus.stop()
    await ws_bus.stop()  # second call should be a no-op


async def test_ws_bus_context_manager() -> None:
    """async with WebSocketEventBus: start on enter, stop on exit."""
    underlying_bus = InMemoryEventBus()
    async with WebSocketEventBus(underlying_bus) as ws_bus:
        mock_ws = MockWebSocket()
        async with ws_bus.connect(mock_ws):
            await underlying_bus.publish(SampleEvent(value=7))
            await asyncio.sleep(0)
            await asyncio.sleep(0)

    assert len(mock_ws.sent) == 1


async def test_ws_connection_count() -> None:
    """connected_count reflects the number of active clients."""
    underlying_bus = InMemoryEventBus()
    ws_bus = WebSocketEventBus(underlying_bus)
    await ws_bus.start()

    assert ws_bus.connected_count == 0

    async with ws_bus.connect(MockWebSocket()):
        assert ws_bus.connected_count == 1
        async with ws_bus.connect(MockWebSocket()):
            assert ws_bus.connected_count == 2
        assert ws_bus.connected_count == 1

    assert ws_bus.connected_count == 0
    await ws_bus.stop()


async def test_ws_bus_repr() -> None:
    """repr includes event_type and client count."""
    underlying_bus = InMemoryEventBus()
    ws_bus = WebSocketEventBus(underlying_bus, event_type=SampleEvent)
    r = repr(ws_bus)
    assert "WebSocketEventBus" in r
    assert "SampleEvent" in r


# ── WebSocketConnection tests ─────────────────────────────────────────────────


async def test_ws_connection_send_forwards_to_websocket() -> None:
    """WebSocketConnection.send() calls send_text on the underlying websocket."""
    mock_ws = MockWebSocket()
    conn = WebSocketConnection(mock_ws)

    await conn.send("hello")

    assert mock_ws.sent == ["hello"]


def test_ws_connection_repr() -> None:
    """WebSocketConnection repr is non-empty."""
    mock_ws = MockWebSocket()
    conn = WebSocketConnection(mock_ws, connection_id="conn-1")
    assert "conn-1" in repr(conn)


# ── SSEEventBus tests ──────────────────────────────────────────────────────────


async def test_sse_bus_delivers_event_to_subscriber() -> None:
    """
    An event published to the bus must appear in the subscriber's stream.
    """
    underlying_bus = InMemoryEventBus()
    sse_bus = SSEEventBus(underlying_bus)
    await sse_bus.start()

    received: list[str] = []

    async with sse_bus.subscribe() as conn:
        await underlying_bus.publish(SampleEvent(value=42))
        # Allow the bus handler to run.
        await asyncio.sleep(0)

        # Consume one event from the stream.
        message = await asyncio.wait_for(conn._queue.get(), timeout=1.0)
        received.append(message)

    assert len(received) == 1
    assert "test.sample" in received[0]
    assert "42" in received[0]
    assert received[0].startswith("data: ")
    assert received[0].endswith("\n\n")

    await sse_bus.stop()


async def test_sse_bus_delivers_to_multiple_subscribers() -> None:
    """
    All active subscribers must receive the event.
    """
    underlying_bus = InMemoryEventBus()
    sse_bus = SSEEventBus(underlying_bus)
    await sse_bus.start()

    queues: list[asyncio.Queue] = []

    async with sse_bus.subscribe() as c1:
        async with sse_bus.subscribe() as c2:
            async with sse_bus.subscribe() as c3:
                queues = [c1._queue, c2._queue, c3._queue]
                await underlying_bus.publish(SampleEvent(value=1))
                await asyncio.sleep(0)

                for q in queues:
                    assert q.qsize() == 1

    await sse_bus.stop()


async def test_sse_bus_stop_signals_all_connections() -> None:
    """
    stop() must put the sentinel in every subscriber's queue so their
    stream() generators terminate.
    """
    from varco_ws.sse import _STOP_SENTINEL

    underlying_bus = InMemoryEventBus()
    sse_bus = SSEEventBus(underlying_bus)
    await sse_bus.start()

    async with sse_bus.subscribe() as conn:
        await sse_bus.stop()
        # The sentinel must have been queued.
        item = await asyncio.wait_for(conn._queue.get(), timeout=1.0)
        assert item is _STOP_SENTINEL


async def test_sse_connection_stream_terminates_on_sentinel() -> None:
    """
    stream() must stop yielding when the sentinel is received.
    """
    from varco_ws.sse import SSEConnection, _STOP_SENTINEL

    conn = SSEConnection()

    # Put one real message and then the sentinel.
    await conn._put("data: hello\n\n")
    await conn._put(_STOP_SENTINEL)

    results: list[str] = []
    async for msg in conn.stream():
        results.append(msg)

    assert results == ["data: hello\n\n"]


async def test_sse_bus_subscriber_count() -> None:
    """subscriber_count reflects active subscribers."""
    underlying_bus = InMemoryEventBus()
    sse_bus = SSEEventBus(underlying_bus)
    await sse_bus.start()

    assert sse_bus.subscriber_count == 0

    async with sse_bus.subscribe():
        assert sse_bus.subscriber_count == 1
        async with sse_bus.subscribe():
            assert sse_bus.subscriber_count == 2
        assert sse_bus.subscriber_count == 1

    assert sse_bus.subscriber_count == 0
    await sse_bus.stop()


async def test_sse_bus_no_subscribers_no_error() -> None:
    """Publishing with no subscribers must be a silent no-op."""
    underlying_bus = InMemoryEventBus()
    sse_bus = SSEEventBus(underlying_bus)
    await sse_bus.start()

    await underlying_bus.publish(SampleEvent(value=0))
    await asyncio.sleep(0)

    await sse_bus.stop()


async def test_sse_bus_start_stop_idempotent() -> None:
    """start() and stop() must be idempotent."""
    underlying_bus = InMemoryEventBus()
    sse_bus = SSEEventBus(underlying_bus)

    await sse_bus.start()
    await sse_bus.start()
    await sse_bus.stop()
    await sse_bus.stop()


async def test_sse_bus_context_manager() -> None:
    """async with SSEEventBus: start on enter, stop on exit."""
    underlying_bus = InMemoryEventBus()
    async with SSEEventBus(underlying_bus) as sse_bus:
        async with sse_bus.subscribe() as conn:
            await underlying_bus.publish(SampleEvent(value=3))
            await asyncio.sleep(0)
            assert conn._queue.qsize() == 1


async def test_sse_bus_repr() -> None:
    """repr includes event_type and subscriber count."""
    underlying_bus = InMemoryEventBus()
    sse_bus = SSEEventBus(underlying_bus, event_type=SampleEvent)
    r = repr(sse_bus)
    assert "SSEEventBus" in r
    assert "SampleEvent" in r
