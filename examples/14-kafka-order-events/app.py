"""
app.py
======
Application factory for the Kafka order-events example.

``create_app(bootstrap_servers)`` returns a FastAPI application wired with:

- ``KafkaEventBus``         — durable Kafka-backed bus (at-least-once delivery)
- ``BusEventProducer``      — publishes events without exposing the bus
- ``OrderConsumer``         — listens to ``OrderPlacedEvent`` on ``"orders"``
- ``build_router``          — HTTP endpoints for orders + notifications

Lifecycle
---------
``KafkaEventBus`` requires ``start()``/``stop()`` around the ASGI lifecycle.
In production the ``lifespan`` context manager handles this automatically.

In tests, ``httpx.ASGITransport`` does NOT trigger the FastAPI lifespan, so
``create_app`` accepts a pre-started ``bus`` argument — the test fixture owns
the lifecycle and passes the live bus in directly.  This avoids the need for
any lifespan workaround in test code.

Key difference from the Redis example (16)
------------------------------------------
Kafka delivers at-least-once — messages are persisted to the broker and
replayed after consumer restarts.  The consumer uses ``auto_offset_reset``
to control where it starts reading.  Tests use ``"earliest"`` so messages
published before the consumer fully joined are still received.

DESIGN: accept pre-built bus for test isolation
    ✅ Eliminates the ASGITransport-lifespan problem (see FINDINGS F06).
    ✅ ``create_app(bootstrap_servers)`` is the simple production path.
    ✅ Tests pass a live bus; production just passes the broker address.
    ❌ Not DI-wired — intentional for a focused event-bus example.

Usage::

    from app import create_app
    app = create_app("localhost:9092")
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
from varco_kafka.config import KafkaEventBusSettings

from varco_kafka import KafkaEventBus

if TYPE_CHECKING:
    from varco_core.event import AbstractEventBus


def create_app(
    bootstrap_servers: str,
    *,
    bus: AbstractEventBus | None = None,
) -> FastAPI:
    """
    Create and return a configured FastAPI application.

    When ``bus`` is ``None`` (production), a new ``KafkaEventBus`` is
    constructed from ``bootstrap_servers`` and started/stopped inside the ASGI
    ``lifespan``.  When ``bus`` is provided (tests), the lifespan does NOT
    touch it — the caller owns the lifecycle.

    Args:
        bootstrap_servers: Comma-separated Kafka broker addresses
                           (e.g. ``"localhost:9092"``).  Only used when
                           ``bus`` is ``None``.
        bus:               Pre-started ``AbstractEventBus``.  When provided,
                           the lifespan skips ``start()``/``stop()``.  Pass
                           this from test fixtures to avoid relying on the
                           ASGI lifespan.

    Returns:
        A ``FastAPI`` instance ready to serve requests.

    DESIGN: optional pre-built bus argument
        ``ASGITransport`` used in tests does NOT trigger FastAPI lifespan,
        so the bus would never be started if we created it inside lifespan
        only.  Accepting a pre-started bus lets tests manage the lifecycle
        themselves (see F06 in FINDINGS.md).

    Edge cases:
        - If ``bus`` is ``None`` and Kafka is unreachable, ``bus.start()``
          raises during the ASGI lifespan — FastAPI propagates this as a
          startup error.
        - ``create_app`` calls ``consumer.register_to(bus)``
          immediately after construction so subscriptions are active before
          the first request is served.
        - Kafka consumer group formation may take a few hundred milliseconds
          after ``start()`` — in production this is negligible, but tests
          must wait for the consumer to receive messages (see test fixtures).
    """
    # Build the bus from bootstrap_servers only when not given a pre-built one.
    # _manage tracks whether THIS factory owns the lifecycle.
    _manage = bus is None
    if _manage:
        config = KafkaEventBusSettings(
            bootstrap_servers=bootstrap_servers,
            # Use a stable group ID for production; tests override per-run.
            group_id="kafka-order-events-example",
            # latest — only process messages published after the consumer starts.
            auto_offset_reset="latest",
        )
        _bus = KafkaEventBus(config=config)
    else:
        # Type ignore: caller guarantees it is a fully-started AbstractEventBus.
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
        title="Kafka Order Events",
        description=(
            "Demonstrates varco_kafka eventing: KafkaEventBus (at-least-once) "
            "with EventConsumer + @listen + retry_policy, and AbstractEventProducer "
            "as the publish interface for HTTP handlers."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    app.include_router(build_router(producer, consumer))
    return app


__all__ = ["create_app"]
