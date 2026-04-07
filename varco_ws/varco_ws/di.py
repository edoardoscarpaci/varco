"""
varco_ws.di
===========
Providify DI configuration for ``varco_ws``.

This module ships two independent ``@Configuration`` classes — one for
WebSocket push and one for SSE push.  Each creates a singleton adapter that
wraps the ``AbstractEventBus`` already registered in the container.

``WebSocketConfiguration``
    Provides a general-purpose ``WebSocketEventBus`` singleton that subscribes
    to **all** events on **all** channels of the underlying bus.

``SSEConfiguration``
    Provides a general-purpose ``SSEEventBus`` singleton that subscribes to
    **all** events on **all** channels of the underlying bus.

Per-channel adapters
--------------------
Most real applications need one adapter per resource channel
(e.g. one for ``"orders"`` and another for ``"inventory"``).  The
general-purpose singletons provided here suit simple use cases.  For
per-channel adapters, wire them manually in the FastAPI lifespan handler::

    from contextlib import asynccontextmanager
    from fastapi import FastAPI
    from varco_ws import WebSocketEventBus

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        bus = container.get(AbstractEventBus)
        orders_ws = WebSocketEventBus(bus, event_type=OrderEvent, channel="orders")
        await orders_ws.start()
        yield
        await orders_ws.stop()

Usage — general-purpose DI
--------------------------
WebSocket only::

    from varco_ws.di import WebSocketConfiguration
    from varco_core.event import AbstractEventBus

    container = DIContainer()
    await container.ainstall(KafkaEventBusConfiguration)   # or Redis, etc.
    container.install(WebSocketConfiguration)

    ws_bus = container.get(WebSocketEventBus)
    await ws_bus.start()

    @app.websocket("/ws")
    async def ws_endpoint(websocket: WebSocket):
        await websocket.accept()
        async with ws_bus.connect(websocket):
            await asyncio.sleep(3600)

SSE only::

    from varco_ws.di import SSEConfiguration

    container = DIContainer()
    await container.ainstall(RedisEventBusConfiguration)
    container.install(SSEConfiguration)

    sse_bus = container.get(SSEEventBus)
    await sse_bus.start()

    @app.get("/events")
    async def sse_endpoint(request: Request):
        async def generate():
            async with sse_bus.subscribe() as stream:
                async for msg in stream:
                    if await request.is_disconnected():
                        break
                    yield msg
        return StreamingResponse(generate(), media_type="text/event-stream")

Lifecycle
---------
Both ``WebSocketEventBus`` and ``SSEEventBus`` must be explicitly started and
stopped.  The recommended pattern is to do this in the FastAPI ``lifespan``
handler::

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        ws_bus = container.get(WebSocketEventBus)
        await ws_bus.start()
        yield
        await ws_bus.stop()

DESIGN: synchronous @Configuration.install() over async @Configuration.ainstall()
    ✅ WebSocketEventBus and SSEEventBus construction is synchronous — they only
       subscribe to the bus (I/O) in ``start()``, not during ``__init__``.
    ✅ Keeps the DI module consistent with SAModule / VarcoFastAPIModule which
       both use synchronous container.install().
    ❌ If start() must be called before the container is ready, callers must
       explicitly call it in their lifespan handler — this is the intended pattern.

Thread safety:  ✅ Safe — DI registration is single-threaded at startup.
Async safety:   ✅ No I/O at installation time; I/O happens in start().
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from providify import Configuration, Inject, Provider

from varco_ws.sse import SSEEventBus
from varco_ws.websocket import WebSocketEventBus

if TYPE_CHECKING:
    from varco_core.event.base import AbstractEventBus


# ── WebSocketConfiguration ────────────────────────────────────────────────────


@Configuration
class WebSocketConfiguration:
    """
    Providify ``@Configuration`` providing a general-purpose
    ``WebSocketEventBus`` singleton.

    The adapter subscribes to all events (``event_type=Event``,
    ``channel="*"``) on the ``AbstractEventBus`` already registered in the
    container.  For per-channel adapters, wire them manually in the FastAPI
    lifespan handler instead.

    Prerequisites
    -------------
    An ``AbstractEventBus`` implementation must be registered before installing
    this configuration::

        await container.ainstall(KafkaEventBusConfiguration)
        container.scan("varco_kafka", recursive=True)
        container.install(WebSocketConfiguration)  # ← requires bus in container

    Example::

        container.install(WebSocketConfiguration)
        ws_bus = container.get(WebSocketEventBus)
        await ws_bus.start()   # call in lifespan handler
        # ... FastAPI routes use ws_bus directly

    Edge cases:
        - If ``AbstractEventBus`` is not yet registered, the provider raises
          ``LookupError`` when ``WebSocketEventBus`` is first resolved.
        - Calling ``install`` twice replaces the previous binding.

    Thread safety:  ✅ Safe — registration is single-threaded at startup.
    Async safety:   ✅ No I/O at installation time.
    """

    @Provider(singleton=True)
    def websocket_event_bus(
        self,
        # DESIGN: Inject[AbstractEventBus] over direct container.get()
        #   ✅ Relies on providify's dependency resolution — if the bus is not
        #      registered, the error message names the missing type explicitly.
        #   ✅ Consistent with how other @Configuration classes declare deps.
        #   ❌ Cannot accept per-channel config at DI time — use manual wiring
        #      when you need separate adapters per event type / channel.
        bus: Inject[AbstractEventBus],  # type: ignore[type-arg]
    ) -> WebSocketEventBus:
        """
        Create a general-purpose ``WebSocketEventBus`` singleton.

        The adapter subscribes to all events on all channels.  It must be
        started explicitly via ``await ws_bus.start()`` before it can deliver
        events to clients.

        Args:
            bus: The ``AbstractEventBus`` resolved from the DI container.
                 Must already be registered (e.g. via ``KafkaEventBusConfiguration``).

        Returns:
            A ``WebSocketEventBus`` instance wrapping ``bus``.

        Edge cases:
            - The returned adapter is **not started** — call ``start()`` in the
              FastAPI lifespan handler.
            - Subscribe clients with ``async with ws_bus.connect(websocket):``.

        Thread safety:  ✅ ``WebSocketEventBus.__init__`` has no I/O.
        """
        # channel="*" and the default event_type=Event subscribe to all events.
        # This is the "broadcast everything" default — override with manual
        # wiring for per-channel adapters.
        return WebSocketEventBus(bus)


# ── SSEConfiguration ──────────────────────────────────────────────────────────


@Configuration
class SSEConfiguration:
    """
    Providify ``@Configuration`` providing a general-purpose ``SSEEventBus``
    singleton.

    The adapter subscribes to all events (``event_type=Event``,
    ``channel="*"``) on the ``AbstractEventBus`` already registered in the
    container.  For per-channel adapters, wire them manually in the FastAPI
    lifespan handler instead.

    Prerequisites
    -------------
    An ``AbstractEventBus`` implementation must be registered before installing
    this configuration::

        await container.ainstall(RedisEventBusConfiguration)
        container.scan("varco_redis", recursive=True)
        container.install(SSEConfiguration)   # ← requires bus in container

    Example::

        container.install(SSEConfiguration)
        sse_bus = container.get(SSEEventBus)
        await sse_bus.start()   # call in lifespan handler

        @app.get("/events")
        async def sse_endpoint(request: Request):
            async def generate():
                async with sse_bus.subscribe() as stream:
                    async for msg in stream:
                        if await request.is_disconnected():
                            break
                        yield msg
            return StreamingResponse(generate(), media_type="text/event-stream")

    Edge cases:
        - If ``AbstractEventBus`` is not yet registered, the provider raises
          ``LookupError`` when ``SSEEventBus`` is first resolved.
        - SSE connections are per-request — call ``sse_bus.subscribe()`` inside
          the endpoint handler, not at module level.

    Thread safety:  ✅ Safe — registration is single-threaded at startup.
    Async safety:   ✅ No I/O at installation time.
    """

    @Provider(singleton=True)
    def sse_event_bus(
        self,
        bus: Inject[AbstractEventBus],  # type: ignore[type-arg]
    ) -> SSEEventBus:
        """
        Create a general-purpose ``SSEEventBus`` singleton.

        The adapter subscribes to all events on all channels.  It must be
        started explicitly via ``await sse_bus.start()`` before it can stream
        events to clients.

        Args:
            bus: The ``AbstractEventBus`` resolved from the DI container.

        Returns:
            An ``SSEEventBus`` instance wrapping ``bus``.

        Edge cases:
            - The returned adapter is **not started** — call ``start()`` in the
              FastAPI lifespan handler.
            - Per-client streaming uses ``async with sse_bus.subscribe() as stream:``.

        Thread safety:  ✅ ``SSEEventBus.__init__`` has no I/O.
        """
        # channel="*" and the default event_type=Event subscribe to all events.
        return SSEEventBus(bus)


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "WebSocketConfiguration",
    "SSEConfiguration",
]
