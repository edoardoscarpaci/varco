"""
varco_ws.di
===========
Providify DI integration for ``varco_ws``.

``WebSocketEventBus`` and ``SSEEventBus`` are decorated with ``@Singleton``
and ``Inject[AbstractEventBus]`` on their constructors.  They self-register
when ``container.scan("varco_ws", recursive=True)`` is called — no
``@Configuration`` class or ``install()`` call is needed.

An ``AbstractEventBus`` implementation must already be registered in the
container before scanning ``varco_ws`` — both adapters inject it.

Usage
-----
General-purpose DI (all-events, all-channels)::

    from varco_redis.di import bootstrap as redis_bootstrap
    from varco_ws.di import bootstrap as ws_bootstrap

    redis_bootstrap()               # registers AbstractEventBus
    ws_bootstrap()                  # scans varco_ws, finds both adapters

    ws_bus  = container.get(WebSocketEventBus)
    sse_bus = container.get(SSEEventBus)

    # Start adapters in the FastAPI lifespan handler
    @asynccontextmanager
    async def lifespan(app):
        await ws_bus.start()
        await sse_bus.start()
        yield
        await ws_bus.stop()
        await sse_bus.stop()

Or manually::

    container = DIContainer()
    container.scan("varco_redis", recursive=True)   # provides AbstractEventBus
    container.scan("varco_ws", recursive=True)       # finds WebSocket + SSE adapters

Per-channel adapters
--------------------
The scan-discovered singletons subscribe to all events on all channels.
For per-channel adapters, wire them manually in the FastAPI lifespan handler::

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        bus = container.get(AbstractEventBus)
        orders_ws = WebSocketEventBus(bus, event_type=OrderEvent, channel="orders")
        await orders_ws.start()
        yield
        await orders_ws.stop()

Lifecycle
---------
Both adapters must be explicitly started and stopped via ``start()``/``stop()``
in the FastAPI lifespan handler.  They are **not** started by the container.

DESIGN: @Singleton on adapter classes over @Provider in @Configuration
    ✅ Scan discovers adapters automatically — no install() call needed.
    ✅ Eliminates the WebSocketConfiguration / SSEConfiguration classes entirely.
    ❌ The scan-discovered singleton always uses event_type=Event, channel="*"
       (all events, all channels) — per-channel adapters still need manual wiring.

Thread safety:  ✅ Safe — DI registration is single-threaded at startup.
Async safety:   ✅ No I/O at scan time; I/O happens in start().
"""

from __future__ import annotations

from typing import Any


# ── Backward-compatibility aliases ────────────────────────────────────────────
# WebSocketConfiguration and SSEConfiguration are kept as no-op @Configuration
# markers so existing code that calls container.install(WebSocketConfiguration)
# does not break.  They no longer register any providers — the @Singleton
# decorators on WebSocketEventBus and SSEEventBus handle registration via scan.

try:
    from providify import Configuration  # noqa: PLC0415

    @Configuration
    class WebSocketConfiguration:
        """
        Backward-compatibility alias — no longer registers any providers.

        ``WebSocketEventBus`` is now ``@Singleton``-decorated and
        discovered automatically by ``container.scan("varco_ws", recursive=True)``.
        Calling ``container.install(WebSocketConfiguration)`` is a no-op.
        """

    @Configuration
    class SSEConfiguration:
        """
        Backward-compatibility alias — no longer registers any providers.

        ``SSEEventBus`` is now ``@Singleton``-decorated and discovered
        automatically by ``container.scan("varco_ws", recursive=True)``.
        Calling ``container.install(SSEConfiguration)`` is a no-op.
        """

except ImportError:
    WebSocketConfiguration = None  # type: ignore[assignment,misc]
    SSEConfiguration = None  # type: ignore[assignment,misc]


# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(
    container: Any = None,
) -> Any:
    """
    Bootstrap ``varco_ws`` into a ``DIContainer``.

    Calls ``container.scan("varco_ws", recursive=True)`` to discover
    ``WebSocketEventBus`` and ``SSEEventBus`` (both ``@Singleton``-decorated).

    An ``AbstractEventBus`` implementation **must** already be registered in
    the container before calling this function — both adapters inject it::

        from varco_redis.di import bootstrap as redis_bootstrap
        from varco_ws.di import bootstrap as ws_bootstrap

        redis_bootstrap()   # scan varco_redis → registers AbstractEventBus
        ws_bootstrap()      # scan varco_ws → registers WebSocketEventBus + SSEEventBus

        ws_bus  = container.get(WebSocketEventBus)
        sse_bus = container.get(SSEEventBus)

        # Start both in the FastAPI lifespan handler — not here
        @asynccontextmanager
        async def lifespan(app):
            await ws_bus.start()
            await sse_bus.start()
            yield
            await ws_bus.stop()
            await sse_bus.stop()

    Args:
        container: An existing ``DIContainer`` to scan into.
                   When ``None``, ``DIContainer.current()`` is used —
                   the process-level singleton.

    Returns:
        The ``DIContainer`` after scanning.

    Edge cases:
        - Calling twice is safe — scanning is idempotent.
        - The adapters are **not started** — call ``start()`` in the lifespan
          handler before serving clients.
        - ``AbstractEventBus`` must already be registered; otherwise
          resolution raises ``LookupError`` when an adapter is first resolved.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ Scanning is synchronous.  I/O happens in ``start()``.
    """
    try:
        from providify import DIContainer  # noqa: PLC0415
    except ImportError:
        return None

    if container is None:
        # Use the process-level singleton so callers don't need to pass it
        # around — consistent with every other bootstrap() in varco packages.
        container = DIContainer.current()

    # Scan discovers WebSocketEventBus and SSEEventBus via their @Singleton
    # decorators.  Both inject Inject[AbstractEventBus] from the container.
    container.scan("varco_ws", recursive=True)

    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    # Backward-compat aliases — kept so existing code doesn't break.
    "WebSocketConfiguration",
    "SSEConfiguration",
    "bootstrap",
]
