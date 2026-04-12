"""
example.streams
===============
WebSocket and Server-Sent Events (SSE) endpoints for real-time post event streaming.

Two endpoints expose the same Redis event bus via different push protocols:

    ``GET /ws/posts``      — WebSocket: bidirectional upgrade, but we only push.
                             Best for clients that need sub-second latency and
                             can handle the protocol upgrade.

    ``GET /events/posts``  — Server-Sent Events: HTTP/1.1 long-lived response.
                             Best for browsers using the native ``EventSource``
                             API — works through most HTTP proxies.

Both endpoints stream ``PostCreatedEvent`` and ``PostDeletedEvent`` serialised
as JSON.  Authentication is not required to subscribe (posts are public), but
you can add auth by checking ``get_request_context().auth`` inside the handler.

Wire-up pattern
---------------
``StreamsRouter`` is a ``@Singleton`` that injects ``WebSocketEventBus`` and
``SSEEventBus`` automatically via DI.  No manual factory call is needed::

    # Resolved automatically once container.scan("example") has run.
    streams_inst = await container.aget(StreamsRouter)
    app.include_router(streams_inst.build_router())

The adapters must be started (via ``VarcoLifespan.register()``) before the
first client connects.

WebSocket message format
------------------------
Each event is sent as a JSON object::

    {
        "event_type": "posts.post.created",
        "event_id": "<uuid>",
        "data": { "post_id": "<uuid>", "author_id": "<uuid>" }
    }

SSE message format
------------------
Each event is sent as an SSE data line::

    data: {"event_type": "...", "event_id": "...", "data": {...}}\\n\\n

The browser's native ``EventSource`` API parses the ``data:`` prefix and fires
a ``"message"`` event.

DESIGN: one shared adapter per protocol over one per endpoint
    ✅ O(1) bus subscriptions regardless of connected client count.
    ✅ Events are fan-out at the adapter level — the bus delivers once per adapter.
    ❌ All ``/ws/posts`` clients receive the same event stream (no per-client filtering).
       For per-user streams, create per-request adapters and wire them manually.

DESIGN: @Singleton StreamsRouter over build_streams_router() factory function
    ✅ DI injects WebSocketEventBus and SSEEventBus automatically — no manual
       resolution + argument passing in _bootstrap.
    ✅ container.scan("example") discovers StreamsRouter for free — no extra
       container.bind() call required.
    ✅ Consistent with the rest of the codebase (@Singleton services/routers).
    ❌ Slightly more ceremony for a simple router — the factory function was
       only 1 call; the @Singleton saves 2 lines in _bootstrap.

Thread safety:  ❌ Not thread-safe — FastAPI runs in a single event loop.
Async safety:   ✅ Handlers are ``async def`` — safe to ``await``.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse
from providify import Inject, Singleton

from varco_ws.sse import SSEEventBus
from varco_ws.websocket import WebSocketEventBus

_logger = logging.getLogger(__name__)


@Singleton
class StreamsRouter:
    """
    FastAPI router providing WebSocket and SSE streaming endpoints for post events.

    Both endpoints fan-out all post events (``PostCreatedEvent``,
    ``PostDeletedEvent``) from the Redis bus to connected clients via the
    ``WebSocketEventBus`` and ``SSEEventBus`` adapters.

    Registered as a ``@Singleton`` so ``container.scan("example")`` discovers
    it automatically.  The adapters are injected by the DI container — no manual
    resolution or argument passing is needed in ``_bootstrap``.

    Usage::

        # Resolved automatically by scan — no container.bind() needed.
        streams_inst = await container.aget(StreamsRouter)
        app.include_router(streams_inst.build_router())

    Attributes:
        _ws_bus:  Injected ``WebSocketEventBus`` adapter singleton.
        _sse_bus: Injected ``SSEEventBus`` adapter singleton.

    Thread safety:  ✅ Singleton — constructed once, shared across requests.
    Async safety:   ✅ ``build_router()`` is synchronous; handlers are async.
    """

    def __init__(
        self,
        ws_bus: Inject[WebSocketEventBus],
        sse_bus: Inject[SSEEventBus],
    ) -> None:
        """
        Construct the ``StreamsRouter`` with injected adapter singletons.

        Args:
            ws_bus:  ``WebSocketEventBus`` adapter — injected by DI container.
                     Must be started (``lifespan.register(ws_bus)``) before the
                     first WebSocket client connects.
            sse_bus: ``SSEEventBus`` adapter — injected by DI container.
                     Must be started (``lifespan.register(sse_bus)``) before the
                     first SSE subscriber connects.

        Edge cases:
            - Construction succeeds even if the adapters are not yet started.
              Clients connecting before ``start()`` will receive no events.
        """
        # Store adapter references for the route handlers below.
        # Both are @Singleton — same instances used across all requests.
        self._ws_bus = ws_bus
        self._sse_bus = sse_bus

    def build_router(self) -> APIRouter:
        """
        Build and return the ``APIRouter`` with WebSocket and SSE endpoints.

        Registers two routes on a new ``APIRouter``:
        - ``WS  /ws/posts``      — WebSocket push endpoint
        - ``GET /events/posts``  — SSE push endpoint

        Route handlers capture ``self._ws_bus`` and ``self._sse_bus`` via closure
        over ``self`` — the same adapter instances are reused for all connections.

        Returns:
            A configured ``APIRouter`` with tag ``"streams"``.

        Edge cases:
            - Calling ``build_router()`` twice returns a second router with the
              same routes — include it only once on the FastAPI app.

        Thread safety:  ✅ Synchronous — no shared state modified.
        Async safety:   ✅ Handlers registered, not called, at build time.
        """
        router = APIRouter(tags=["streams"])

        @router.websocket("/ws/posts")
        async def posts_websocket(websocket: WebSocket) -> None:
            """
            WebSocket endpoint — streams all post events to the connected client.

            Connect with any WebSocket client::

                wscat -c ws://localhost:8000/ws/posts
                # or in the browser:
                const ws = new WebSocket("ws://localhost:8000/ws/posts");
                ws.onmessage = (e) => console.log(JSON.parse(e.data));

            Each message is a JSON object::

                {
                    "event_type": "posts.post.created",
                    "event_id": "<uuid>",
                    "data": { "post_id": "<uuid>", "author_id": "<uuid>" }
                }

            The connection stays open until the client disconnects.  The server
            sends a ``{"type": "ping"}`` keepalive every 30 seconds to prevent
            idle-connection timeouts from proxies.

            Edge cases:
                - Reconnect is the client's responsibility.
                - No authentication required — posts are publicly readable.
                  Extend this handler with auth checks if needed.
                - If the bus goes down (restart), events stop arriving but
                  the WebSocket connection stays open until the ping times out.
            """
            await websocket.accept()
            _logger.info("WebSocket client connected to /ws/posts")

            async with self._ws_bus.connect(websocket):
                # Keep the connection alive.  The adapter's broadcast mechanism
                # pushes events to the client via the WebSocketConnection handle
                # registered above.  We just need to keep the task alive and
                # send periodic pings so the connection is not dropped by proxies.
                try:
                    while True:
                        # Wait 30 s, then send a keepalive ping frame.
                        # The client's receive() will raise WebSocketDisconnect
                        # as soon as the client closes the connection.
                        await asyncio.sleep(30)
                        await websocket.send_json({"type": "ping"})
                except WebSocketDisconnect:
                    # Clean disconnect — context manager handles cleanup.
                    _logger.info("WebSocket client disconnected from /ws/posts")

        @router.get(
            "/events/posts",
            summary="SSE stream of post events",
            description=(
                "Server-Sent Events stream.  Connect with the browser ``EventSource`` API "
                "or ``curl -N http://localhost:8000/events/posts``.\n\n"
                "Each event is a JSON object with ``event_type``, ``event_id``, and ``data``."
            ),
            response_class=StreamingResponse,
            # Exclude from OpenAPI schema — StreamingResponse has no fixed JSON schema.
            responses={
                200: {
                    "description": "SSE stream",
                    "content": {"text/event-stream": {"schema": {"type": "string"}}},
                }
            },
        )
        async def posts_sse() -> StreamingResponse:
            """
            SSE endpoint — streams all post events to the connected HTTP client.

            Connect with curl::

                curl -N http://localhost:8000/events/posts

            Connect in the browser::

                const src = new EventSource("/events/posts");
                src.onmessage = (e) => console.log(JSON.parse(e.data));

            Each event is sent as::

                data: {"event_type": "...", "event_id": "...", "data": {...}}\\n\\n

            Edge cases:
                - The response is ``text/event-stream`` — browsers automatically
                  reconnect after a dropped connection via the ``EventSource`` API.
                - If the bus is restarted, the SSE adapter stops delivering events.
                  The client's ``EventSource`` will reconnect and resume after the
                  adapter is restarted.
                - No authentication required for public post events.  Extend with
                  auth by calling ``get_request_context().auth`` and checking roles.

            Returns:
                A ``StreamingResponse`` with ``Content-Type: text/event-stream``.
            """

            async def _generate():
                # Subscribe to the SSE adapter.  The context manager registers this
                # connection and removes it on exit (even on client disconnect).
                async with self._sse_bus.subscribe() as conn:
                    async for message in conn.stream():
                        # Each message is already formatted as "data: {...}\n\n".
                        yield message

            return StreamingResponse(
                _generate(),
                media_type="text/event-stream",
                # Cache-Control and X-Accel-Buffering are required for SSE to work
                # correctly through nginx and other buffering reverse proxies.
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                    "Connection": "keep-alive",
                },
            )

        return router


__all__ = ["StreamsRouter"]
