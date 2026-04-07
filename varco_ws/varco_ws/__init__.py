"""
varco_ws
========
WebSocket and SSE event bus backends for varco.

Provides ``WebSocketEventBus`` and ``SSEEventBus`` — two ``AbstractEventBus``
implementations that push events to browser clients without polling.

    WebSocketEventBus — bidirectional, full-duplex; suitable for real-time UIs.
    SSEEventBus       — server-to-client only; simpler, HTTP/1.1 compatible.

Quick start::

    # WebSocket (FastAPI example)
    from varco_ws import WebSocketEventBus
    bus = WebSocketEventBus()

    @app.websocket("/ws")
    async def ws_endpoint(websocket: WebSocket):
        await websocket.accept()
        async with bus.connect_websocket(websocket):
            await asyncio.sleep(60)   # keep connection alive

    # SSE (FastAPI example)
    from varco_ws import SSEEventBus
    bus = SSEEventBus()

    @app.get("/events")
    async def sse_endpoint(request: Request):
        return bus.sse_response(request)
"""

from varco_ws.di import SSEConfiguration, WebSocketConfiguration
from varco_ws.sse import SSEEventBus, SSEConnection
from varco_ws.websocket import WebSocketEventBus, WebSocketConnection

__all__ = [
    "WebSocketEventBus",
    "WebSocketConnection",
    "SSEEventBus",
    "SSEConnection",
    "WebSocketConfiguration",
    "SSEConfiguration",
]
