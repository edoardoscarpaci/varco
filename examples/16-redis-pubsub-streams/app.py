"""
app.py
======
Application factory for the Redis Pub/Sub & Streams notification-hub example.

``create_app(redis_url)`` returns a FastAPI application wired with:

- ``RedisEventBus``         — pub/sub bus (at-most-once delivery)
- ``BusEventProducer``      — publishes events without exposing the bus
- ``OrderConsumer``         — listens to ``OrderPlacedEvent`` on ``"orders"``
- ``build_router``          — HTTP endpoints for orders + notifications

Lifecycle
---------
``RedisEventBus`` requires ``start()``/``stop()`` around the ASGI lifecycle.
In production the ``lifespan`` context manager handles this automatically.

In tests, ``httpx.ASGITransport`` does NOT trigger the FastAPI lifespan, so
``create_app`` accepts a pre-started ``bus`` argument — the test fixture owns
the lifecycle and passes the live bus in directly.  This avoids the need for
any lifespan workaround in test code.

DESIGN: accept pre-built bus for test isolation
    ✅ Eliminates the ASGITransport-lifespan problem (see FINDINGS F06).
    ✅ ``create_app(redis_url)`` is the simple production path.
    ✅ Tests pass a live bus; production just passes the URL.
    ❌ Not DI-wired — intentional for a focused event-bus example.

Usage::

    from app import create_app
    app = create_app("redis://localhost:6379/0")
    # uvicorn app:app

Thread safety:  ❌  Single event loop.
Async safety:   ✅  Lifespan context manages bus start/stop.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING

from consumer import OrderConsumer
from fastapi import FastAPI
from router import build_router
from varco_core.event import BusEventProducer

from varco_redis import RedisEventBus, RedisEventBusSettings

if TYPE_CHECKING:
    from varco_core.event import AbstractEventBus


def create_app(
    redis_url: str,
    *,
    bus: AbstractEventBus | None = None,
) -> FastAPI:
    """
    Create and return a configured FastAPI application.

    When ``bus`` is ``None`` (production), a new ``RedisEventBus`` is
    constructed from ``redis_url`` and started/stopped inside the ASGI
    ``lifespan``.  When ``bus`` is provided (tests), the lifespan does NOT
    touch it — the caller owns the lifecycle.

    Args:
        redis_url: Redis connection URL (e.g. ``"redis://localhost:6379/0"``).
                   Only used when ``bus`` is ``None``.
        bus:       Pre-started ``AbstractEventBus``.  When provided, the
                   lifespan skips ``start()``/``stop()``.  Pass this from
                   test fixtures to avoid relying on the ASGI lifespan.

    Returns:
        A ``FastAPI`` instance ready to serve requests.

    DESIGN: optional pre-built bus argument
        ``ASGITransport`` used in tests does NOT trigger FastAPI lifespan,
        so the bus would never be started if we created it inside lifespan
        only.  Accepting a pre-started bus lets tests manage the lifecycle
        themselves (see F06 in FINDINGS.md).

    Edge cases:
        - If ``bus`` is ``None`` and Redis is unreachable, ``bus.start()``
          raises ``ConnectionError`` during the ASGI lifespan — FastAPI
          propagates this as a startup error.
        - ``create_app`` calls ``consumer.register_to(bus)``
          immediately after construction so subscriptions are active before
          the first request is served.
    """
    # Build the bus from the URL only when not given a pre-built one.
    # _manage tracks whether THIS factory owns the lifecycle.
    _manage = bus is None
    if _manage:
        config = RedisEventBusSettings(url=redis_url)
        _bus = RedisEventBus(config=config)
    else:
        _bus = bus  # type: ignore[assignment]

    # Producer wraps the bus — router handlers call _produce(), never bus.publish().
    producer = BusEventProducer(bus=_bus)

    # register_to(bus) installs the @listen subscriptions on the bus.  It is
    # called explicitly because this example is not DI-wired — a container-
    # bound @Singleton consumer would be registered by CoreModule.start()
    # (Plan 051 / DI-5).  Subscriptions are recorded even before bus.start().
    consumer = OrderConsumer()
    consumer.register_to(_bus)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
        """Start and stop the bus only when this factory created it."""
        if _manage:
            await _bus.start()
        try:
            yield
        finally:
            if _manage:
                await _bus.stop()

    app = FastAPI(
        title="Notification Hub — Redis Pub/Sub & Streams",
        description=(
            "Demonstrates varco_redis eventing: RedisEventBus (pub/sub, "
            "at-most-once) with EventConsumer + @listen, and AbstractEventProducer "
            "as the publish interface for HTTP handlers."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    app.include_router(build_router(producer, consumer))
    return app


__all__ = ["create_app"]
