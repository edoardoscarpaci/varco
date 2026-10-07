"""
app.py
======
Application factory for the ``17-transactional-outbox`` example.

Demonstrates the transactional outbox pattern end-to-end:

1. ``OrderService.create()`` persists an ``Order`` row and an ``OutboxEntry``
   in the same SQLAlchemy DB transaction — one atomic commit, zero event loss.
2. ``OutboxRelay`` (background task) polls the ``varco_outbox`` table and
   publishes pending entries to ``InMemoryEventBus``.
3. ``OrderConsumer`` receives ``OrderCreatedEvent`` from the bus and appends it
   to ``consumer.received`` — visible via ``GET /v1/events``.

The ``SADeduplicator`` (backed by the ``varco_dedup_log`` table) is wired into
the ``OrderConsumer`` to guard against duplicate delivery.  The relay guarantees
at-least-once: if it crashes between ``bus.publish()`` and the outbox-row
``delete()``, the entry will be replayed on the next poll tick.

Why ``InMemoryEventBus``?
    The outbox pattern is transport-agnostic.  ``InMemoryEventBus`` keeps this
    example focused on the outbox mechanics without requiring a Redis or Kafka
    container.  Swap it for ``KafkaEventBus`` or ``RedisEventBus`` in
    production — only ``create_app`` needs to change.

DDL created at startup
    - ``Base.metadata`` (``orders`` table) via SA auto-generation.
    - ``outbox_metadata`` (``varco_outbox`` table) from ``varco_sa.outbox``.
    - ``dedup_metadata`` (``varco_dedup_log`` table) from ``varco_sa.deduplication``.

DESIGN: InMemoryEventBus over Redis/Kafka for the example bus
    ✅ Zero external infrastructure — Postgres alone is sufficient.
    ✅ Example stays focused on the outbox pattern, not the bus.
    ✅ Tests need only a Postgres testcontainer.
    ❌ InMemoryEventBus has no durability; relay crash = lost delivery.
       In production, use a durable bus (Kafka, NATS, Redis Streams).

DESIGN: OutboxRelay as background task in FastAPI lifespan
    ✅ Relay starts/stops with the app — no separate process needed.
    ✅ ``await relay.start()`` creates the asyncio background task inside
       the running event loop (``asyncio.Lock`` is created lazily, as required).
    ✅ ``await relay.stop()`` cancels the task cleanly on shutdown.
    ❌ Single process only.  For multi-process deployments, run one relay
       process per shard to avoid duplicate delivery.

Thread safety:  ✅ Single-process factory; all async within one event loop.
Async safety:   ✅ Lifespan manages bus, relay, and deduplicator lifecycle.
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from assembler import OrderAssembler  # noqa: F401 — stamps @Singleton metadata
from consumer import OrderConsumer
from fastapi import FastAPI
from models import Order
from providify import DIContainer, Provider
from service import OrderService  # noqa: F401 — stamps @Singleton metadata
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from varco_core.event import InMemoryEventBus
from varco_core.service.base import IUoWProvider
from varco_core.service.outbox import OutboxRelay
from varco_fastapi.di import bootstrap as fastapi_bootstrap
from varco_sa.deduplication import SADeduplicator, dedup_metadata
from varco_sa.outbox import SARelayOutboxRepository, outbox_metadata
from varco_sa.provider import SQLAlchemyRepositoryProvider

# ── Shared SA declarative base ─────────────────────────────────────────────────


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for the orders table."""


# ── DI container bootstrap ─────────────────────────────────────────────────────


def _build_container(db_url: str) -> tuple[DIContainer, object]:
    """
    Build and configure a ``DIContainer`` for the order service.

    Returns:
        ``(container, engine)`` — the DI container and the async engine
        (the engine is returned so ``create_app`` can run DDL at startup).
    """
    container = DIContainer()

    engine = create_async_engine(db_url, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    provider = SQLAlchemyRepositoryProvider.from_components(
        base=Base,
        session_factory=session_factory,
    )
    provider.register(Order)

    @Provider(singleton=True)
    def _uow_provider() -> IUoWProvider:
        return provider  # type: ignore[return-value]

    container.provide(_uow_provider)
    # 4.0 (Plan 049): the framework @Configuration is VarcoFastAPIConfiguration; the
    # 4.0 hand-wired form is the package bootstrap() (scan-installs it, idempotent,
    # so create_varco_app's FastAPIModule re-running it later is safe).
    fastapi_bootstrap(container)

    from varco_core.auth.authorizer import BaseAuthorizer  # noqa: PLC0415
    from varco_core.auth.base import AbstractAuthorizer  # noqa: PLC0415

    # 4.0 (Plan 053 / FLIP-5): a class-keyed bind of BaseAuthorizer is gated
    # exactly like the scanned fallback — run with VARCO_SECURITY_ENV=development
    # (see README). A production app registers a real AbstractAuthorizer.
    container.bind(AbstractAuthorizer, BaseAuthorizer)
    container.bind(OrderAssembler, OrderAssembler)
    container.bind(OrderService, OrderService)

    return container, engine


# ── Application factory ────────────────────────────────────────────────────────


def create_app(db_url: str | None = None) -> tuple[FastAPI, DIContainer]:
    """
    Build and return the configured FastAPI application.

    Args:
        db_url: PostgreSQL connection URL with ``postgresql+asyncpg://`` scheme.
                Falls back to the ``DATABASE_URL`` environment variable.

    Returns:
        ``(FastAPI, DIContainer)`` — the app and the container.  Tests receive
        the container so they can query state without HTTP round-trips.

    Edge cases:
        - Calling ``create_app()`` twice in the same process re-registers ORM
          models in ``Base.metadata`` via ``SAModelFactory``.  Tests call it
          once per session to avoid conflicts.
        - ``OutboxRelay.start()`` must run inside a live asyncio event loop —
          it creates a ``Lock`` lazily.  The ASGI ``lifespan`` guarantees this.
    """
    url = db_url or os.environ["DATABASE_URL"]
    container, engine = _build_container(url)
    order_service = container.get(OrderService)

    # Event bus — InMemoryEventBus for this example; swap for Kafka/Redis in prod.
    bus = InMemoryEventBus()

    # Build session factory for the relay (it needs its own sessions to auto-commit).
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    relay_outbox_repo = SARelayOutboxRepository(session_factory)

    # Deduplicator — backs the @listen handler to skip replayed events.
    dedup = SADeduplicator(engine)

    relay = OutboxRelay(outbox=relay_outbox_repo, bus=bus, poll_interval=0.1)

    # register_to(bus) installs the @listen subscriptions; the deduplicator
    # guards against relay replays.  Called explicitly because this example is
    # not DI-wired — a container-bound @Singleton consumer would be registered
    # by CoreModule.start() (Plan 051 / DI-5).
    consumer = OrderConsumer(deduplicator=dedup)
    consumer.register_to(bus)

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncGenerator[None, None]:
        """
        Start the bus and relay; create all DB tables; stop cleanly on exit.

        Startup order:
        1. Create all DB tables (orders, varco_outbox, varco_dedup_log).
        2. Ensure dedup table exists (``SADeduplicator.ensure_table()``).
        3. Start the ``OutboxRelay`` background polling task.

        Shutdown order:
        1. Stop the relay — cancels the background polling task.
        """
        async with engine.begin() as conn:
            # orders table from DomainModel auto-generation
            await conn.run_sync(Base.metadata.create_all)
            # varco_outbox table from varco_sa.outbox
            await conn.run_sync(outbox_metadata.create_all)
            # varco_dedup_log table from varco_sa.deduplication
            await conn.run_sync(dedup_metadata.create_all)

        await dedup.ensure_table()
        await relay.start()
        try:
            yield
        finally:
            await relay.stop()

    from fastapi import FastAPI as _FastAPI  # noqa: PLC0415

    app = _FastAPI(
        title="Transactional Outbox Example",
        description=(
            "Demonstrates varco's transactional outbox pattern:\n\n"
            "- ``OrderService.create()`` writes order + ``OutboxEntry`` atomically\n"
            "- ``OutboxRelay`` background task publishes events from the outbox\n"
            "- ``OrderConsumer`` receives events with ``SADeduplicator`` guard\n\n"
            "POST an order, then poll ``GET /v1/events`` to see delivery."
        ),
        version="1.0.0",
        lifespan=lifespan,
    )

    from router import build_router  # noqa: PLC0415

    app.include_router(build_router(order_service, consumer))

    return app, container


# ── Module-level app for ``uvicorn app:app`` ──────────────────────────────────
_app_result: tuple[FastAPI, DIContainer] | None = None

try:
    if "DATABASE_URL" in os.environ:
        _app_result = create_app()
except Exception:
    pass

app: FastAPI | None = _app_result[0] if _app_result else None


__all__ = ["app", "create_app", "Base"]
