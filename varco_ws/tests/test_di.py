"""
tests.test_di
=============
Unit tests for varco_ws.di — WebSocketConfiguration and SSEConfiguration.

Covers:
    WebSocketConfiguration  — provides a WebSocketEventBus singleton that
                               wraps the container's AbstractEventBus.
    SSEConfiguration        — provides an SSEEventBus singleton that wraps
                               the container's AbstractEventBus.
    varco_ws public __init__ — DI classes are exported at the package level.

All tests use InMemoryEventBus — no real broker required.
"""

from __future__ import annotations

import pytest

from varco_core.event.base import AbstractEventBus
from varco_core.event.memory import InMemoryEventBus

from varco_ws import SSEConfiguration, WebSocketConfiguration
from varco_ws.di import SSEConfiguration as SSEConfigurationDirect
from varco_ws.di import WebSocketConfiguration as WebSocketConfigurationDirect
from varco_ws.sse import SSEEventBus
from varco_ws.websocket import WebSocketEventBus


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_container_with_bus() -> tuple[object, InMemoryEventBus]:
    """
    Build a minimal providify DIContainer with an InMemoryEventBus registered
    as AbstractEventBus.  Returns (container, bus).
    """
    try:
        from providify import DIContainer
    except ImportError:
        pytest.skip("providify not installed — skipping DI tests")

    container = DIContainer()
    bus = InMemoryEventBus()
    # Register InMemoryEventBus as the AbstractEventBus implementation.
    container.provide(lambda: bus, AbstractEventBus)
    return container, bus


# ── __init__ re-export tests ───────────────────────────────────────────────────


def test_websocket_configuration_exported_from_init() -> None:
    """
    WebSocketConfiguration must be importable from the top-level varco_ws package.
    This guards against accidental removal from __init__.py.
    """
    assert WebSocketConfiguration is WebSocketConfigurationDirect


def test_sse_configuration_exported_from_init() -> None:
    """
    SSEConfiguration must be importable from the top-level varco_ws package.
    """
    assert SSEConfiguration is SSEConfigurationDirect


# ── WebSocketConfiguration tests ──────────────────────────────────────────────


def test_websocket_configuration_provides_websocket_event_bus() -> None:
    """
    After installing WebSocketConfiguration, the container must resolve
    WebSocketEventBus.
    """
    container, _ = _make_container_with_bus()
    container.install(WebSocketConfiguration)

    ws_bus = container.get(WebSocketEventBus)
    assert isinstance(ws_bus, WebSocketEventBus)


def test_websocket_configuration_wraps_registered_bus() -> None:
    """
    The WebSocketEventBus provided by WebSocketConfiguration must wrap
    the AbstractEventBus that was registered in the container.
    """
    container, bus = _make_container_with_bus()
    container.install(WebSocketConfiguration)

    ws_bus = container.get(WebSocketEventBus)
    # Internal attribute _bus must be the registered InMemoryEventBus.
    assert ws_bus._bus is bus


def test_websocket_configuration_singleton() -> None:
    """
    WebSocketEventBus must be a singleton — resolving it twice returns the
    same instance.
    """
    container, _ = _make_container_with_bus()
    container.install(WebSocketConfiguration)

    first = container.get(WebSocketEventBus)
    second = container.get(WebSocketEventBus)
    assert first is second


def test_websocket_configuration_bus_not_started_after_install() -> None:
    """
    The WebSocketEventBus must NOT be started automatically by the DI module.
    Callers must call start() explicitly in their lifespan handler.

    DESIGN: not starting automatically avoids an asyncio.Loop dependency at
    installation time — the container may be built synchronously before an
    event loop is running.
    """
    container, _ = _make_container_with_bus()
    container.install(WebSocketConfiguration)

    ws_bus = container.get(WebSocketEventBus)
    # Internal subscription handle is None when not started.
    assert ws_bus._subscription is None


# ── SSEConfiguration tests ────────────────────────────────────────────────────


def test_sse_configuration_provides_sse_event_bus() -> None:
    """
    After installing SSEConfiguration, the container must resolve SSEEventBus.
    """
    container, _ = _make_container_with_bus()
    container.install(SSEConfiguration)

    sse_bus = container.get(SSEEventBus)
    assert isinstance(sse_bus, SSEEventBus)


def test_sse_configuration_wraps_registered_bus() -> None:
    """
    The SSEEventBus provided by SSEConfiguration must wrap the
    AbstractEventBus that was registered in the container.
    """
    container, bus = _make_container_with_bus()
    container.install(SSEConfiguration)

    sse_bus = container.get(SSEEventBus)
    assert sse_bus._bus is bus


def test_sse_configuration_singleton() -> None:
    """SSEEventBus must be a singleton."""
    container, _ = _make_container_with_bus()
    container.install(SSEConfiguration)

    first = container.get(SSEEventBus)
    second = container.get(SSEEventBus)
    assert first is second


def test_sse_configuration_bus_not_started_after_install() -> None:
    """
    The SSEEventBus must NOT be started automatically.  Callers start it
    in the FastAPI lifespan handler.
    """
    container, _ = _make_container_with_bus()
    container.install(SSEConfiguration)

    sse_bus = container.get(SSEEventBus)
    assert sse_bus._subscription is None


# ── Combined WS + SSE tests ───────────────────────────────────────────────────


def test_both_configurations_can_be_installed_together() -> None:
    """
    Installing both WebSocketConfiguration and SSEConfiguration in the same
    container must not conflict — they provide different types.
    """
    container, _ = _make_container_with_bus()
    container.install(WebSocketConfiguration)
    container.install(SSEConfiguration)

    ws_bus = container.get(WebSocketEventBus)
    sse_bus = container.get(SSEEventBus)

    # Different types — different instances.
    assert isinstance(ws_bus, WebSocketEventBus)
    assert isinstance(sse_bus, SSEEventBus)
    assert ws_bus is not sse_bus


async def test_ws_bus_is_functional_after_start() -> None:
    """
    The WebSocketEventBus obtained from the container must be fully functional
    after manually calling start().

    DESIGN: functional test to confirm the DI-provided adapter delivers events,
    not just that it was constructed correctly.
    """
    import asyncio

    from varco_core.event.base import Event

    class PingEvent(Event):
        __event_type__ = "test.ping"
        count: int = 0

    container, bus = _make_container_with_bus()
    container.install(WebSocketConfiguration)

    ws_bus = container.get(WebSocketEventBus)
    await ws_bus.start()

    class FakeWebSocket:
        sent: list[str] = []

        async def send_text(self, msg: str) -> None:
            self.sent.append(msg)

    fake_ws = FakeWebSocket()
    async with ws_bus.connect(fake_ws):
        await bus.publish(PingEvent(count=1))
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    assert len(fake_ws.sent) == 1

    await ws_bus.stop()


async def test_sse_bus_is_functional_after_start() -> None:
    """
    The SSEEventBus obtained from the container must be fully functional
    after manually calling start().
    """
    import asyncio

    from varco_core.event.base import Event

    class PongEvent(Event):
        __event_type__ = "test.pong"
        value: str = ""

    container, bus = _make_container_with_bus()
    container.install(SSEConfiguration)

    sse_bus = container.get(SSEEventBus)
    await sse_bus.start()

    async with sse_bus.subscribe() as conn:
        await bus.publish(PongEvent(value="hello"))
        await asyncio.sleep(0)

        message = await asyncio.wait_for(conn._queue.get(), timeout=1.0)
        assert "test.pong" in message
        assert "hello" in message

    await sse_bus.stop()
