"""
example.app
===========
FastAPI application bootstrap for the Post example.

Wires the complete stack in the correct order:

    1.  Event bus     (Redis Pub/Sub)
    2.  Cache backend (Redis)
    3.  ORM           (SQLAlchemy)
    4.  FastAPI defaults + event producer
    5.  Post domain module
    6.  Router construction → ``include_router``
    7.  Lifespan: bus → consumer → job runner

Environment variables required
-------------------------------
    VARCO_REDIS_URL          redis://localhost:6379      (read by RedisEventBusSettings.from_env)
    VARCO_REDIS_CACHE_URL    redis://localhost:6379      (read by RedisCacheSettings.from_env)
    DATABASE_URL             postgresql+asyncpg://user:pass@localhost/db

Environment variables optional
-------------------------------
    VARCO_CORS_ORIGINS  comma-separated allowed origins (default: *)
    VARCO_TRUST_STORE_DIR  path to TLS trust store directory

Quick start (Docker Compose not required for unit tests)::

    docker run -d -p 6379:6379 redis
    docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=pass postgres
    DATABASE_URL="postgresql+asyncpg://postgres:pass@localhost/example" \\
    REDIS_URL="redis://localhost:6379" \\
    uv run uvicorn example.app:create_app --factory --reload

DESIGN: create_app() factory over module-level ``app = FastAPI()``
    A factory function allows:
    ✅ Async DI bootstrap (``await container.ainstall(...)``).
    ✅ Multiple app instances per process (e.g. for testing).
    ✅ Clean separation of configuration from app creation.
    ❌ Uvicorn needs ``--factory`` flag and the fully-qualified factory name.

Thread safety:  ✅ ``create_app()`` is called once at startup.
Async safety:   ✅ ``create_app()`` is ``async def`` — use with an ASGI server.
"""

from __future__ import annotations

import os

from fastapi import FastAPI
from providify import DIContainer
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase

from varco_core.event.base import AbstractEventBus
from varco_core.job.base import AbstractJobRunner
from varco_fastapi.di import (
    VarcoFastAPIModule,
    setup_event_producer,
    setup_varco_defaults,
)
from varco_fastapi.lifespan import VarcoLifespan
from varco_fastapi.middleware.cors import install_cors
from varco_fastapi.middleware.error import ErrorMiddleware
from varco_sa.bootstrap import SAConfig
from varco_sa.di import SAModule, bind_repositories

from example.consumer import PostEventConsumer
from example.di import PostModule
from example.models import Post
from example.router import PostRouter


# ── SQLAlchemy declarative base ───────────────────────────────────────────────
# All varco_sa-managed entities inherit from DomainModel, but SQLAlchemy still
# needs a shared DeclarativeBase so table metadata is collected in one place.
# Declare it here (not in models.py) to keep the domain model backend-agnostic.
class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base for all entities in this app."""


async def create_app() -> FastAPI:
    """
    Build and return the configured FastAPI application.

    Bootstrap sequence:
    1. Create DI container.
    2. Install Redis event bus (``AbstractEventBus``).
    3. Install Redis cache (``CacheBackend``).
    4. Install SQLAlchemy (``RepositoryProvider`` + ``IUoWProvider``).
    5. Install FastAPI defaults (``AbstractJobRunner``, ``TaskRegistry``, etc.).
    6. Register ``AbstractEventProducer`` → ``BusEventProducer``.
    7. Install domain module (``PostService``, ``PostAssembler``, ``PostEventConsumer``).
    8. Bind per-entity repositories (``AsyncRepository[Post]``).
    9. Resolve singletons with lifecycle.
    10. Register lifecycle components with ``VarcoLifespan``.
    11. Build routers.
    12. Create ``FastAPI`` app with lifespan and middleware.

    Returns:
        A configured ``FastAPI`` application ready for an ASGI server.

    Raises:
        KeyError: ``DATABASE_URL`` or ``REDIS_URL`` env var missing.

    Edge cases:
        - If the database or Redis is unavailable, the app STARTS but
          the first request that touches them will fail.  Add a health
          endpoint (``HealthRouter`` from varco_fastapi) for readiness probes.
        - ``create_app()`` is idempotent — calling twice creates two independent
          ``DIContainer`` instances.  Do not call more than once per process.
    """
    container = DIContainer()

    # ── 1. Event bus ──────────────────────────────────────────────────────────
    # RedisEventBusConfiguration binds AbstractEventBus → RedisEventBus.
    # Must be installed before setup_event_producer() is called.
    # DESIGN: use Pub/Sub (at-most-once) for this example.
    #         Swap for RedisStreamConfiguration (at-least-once) in production.
    from varco_redis.di import RedisEventBusConfiguration  # noqa: PLC0415

    await container.ainstall(RedisEventBusConfiguration)

    # ── 2. Cache backend ──────────────────────────────────────────────────────
    # RedisCacheConfiguration binds CacheBackend → RedisCache.
    # PostService._cache (ClassVar[Inject[CacheBackend]]) resolves to this.
    from varco_redis.cache import RedisCacheConfiguration  # noqa: PLC0415

    await container.ainstall(RedisCacheConfiguration)

    # ── 3. SQLAlchemy ORM ─────────────────────────────────────────────────────
    # SAConfig is registered manually (not via @Provider) because it requires
    # the engine and Base which are app-specific.
    #
    # DESIGN: sync @Provider for SAConfig so container.install(SAModule) works
    # without await.  SQLAlchemy has no async init step.
    from providify import Provider  # noqa: PLC0415

    @Provider(singleton=True)
    def _sa_config() -> SAConfig:
        return SAConfig(
            engine=create_async_engine(
                os.environ["DATABASE_URL"],
                # echo=True for SQL debug logging — disable in production
                echo=False,
            ),
            base=Base,
            # All entity classes must be listed so SAModelFactory maps their
            # tables before the first make_uow() call.
            entity_classes=(Post,),
        )

    container.provide(_sa_config)
    container.install(SAModule)
    # SAModule now provides:
    #   RepositoryProvider → SQLAlchemyRepositoryProvider
    #   IUoWProvider       → same RepositoryProvider (GAP #5 fix)

    # ── 4. FastAPI defaults ───────────────────────────────────────────────────
    container.install(VarcoFastAPIModule)
    setup_varco_defaults(container)  # binds TaskSerializer → DefaultTaskSerializer

    # ── 5. Event producer ─────────────────────────────────────────────────────
    # Binds AbstractEventProducer → BusEventProducer (GAP #4 fix).
    # Must be called AFTER the bus module is installed so BusEventProducer
    # can inject the AbstractEventBus singleton.
    setup_event_producer(container)

    # ── 6. Domain module ──────────────────────────────────────────────────────
    # Registers PostAssembler, PostService, PostEventConsumer (all @Singleton).
    container.install(PostModule)

    # ── 6b. Scan the example package to register @Singleton-decorated classes ──
    # ``container.install(PostModule)`` only processes @Provider methods on the
    # @Configuration class — it does NOT auto-discover @Singleton classes that
    # were merely imported in di.py.  The scanner picks up DI-metadata-decorated
    # members from all submodules and registers them.  Classes without DI metadata
    # (test helpers, plain dataclasses) are skipped automatically.
    #
    # DESIGN: scan() over explicit container.register() calls in PostModule
    #   ✅ Keeps PostModule free of boilerplate registration factories.
    #   ✅ New @Singleton classes in the example package are discovered automatically.
    #   ❌ scan() inspects every member in every submodule — slightly slower at
    #      startup.  Acceptable for an application package of this size.
    container.scan("example", recursive=True)

    # scanner._autoregister_class only self-binds when a class has NO abstract bases.
    # PostRouter extends VarcoCRUDRouter, so the scanner registers it under the
    # parameterized VarcoCRUDRouter[...] interface — NOT under PostRouter itself.
    # container.bind() adds an explicit self-binding without requiring a DI decorator
    # on the class (only container.register() has that requirement).
    container.bind(PostRouter, PostRouter)

    # ── 7. Per-entity repository bindings ─────────────────────────────────────
    # Must come AFTER SAModule is installed.
    bind_repositories(container, Post)

    # ── 8. Resolve lifecycle singletons ───────────────────────────────────────
    # Resolve now (not lazily) so startup failures surface immediately rather
    # than on the first request.
    bus: AbstractEventBus = await container.aget(AbstractEventBus)
    consumer: PostEventConsumer = await container.aget(PostEventConsumer)
    job_runner: AbstractJobRunner = await container.aget(AbstractJobRunner)

    # ── 9. Lifespan ───────────────────────────────────────────────────────────
    # Registration order = startup order.
    # Shutdown is LIFO — job_runner stops before consumer, consumer before bus.
    # This ensures no new events arrive after subscriptions are cancelled.
    lifespan = VarcoLifespan()
    lifespan.register(bus)  # bus must start before consumer subscribes
    lifespan.register(consumer)  # start() calls register_to(bus) — GAP #3 fix
    lifespan.register(job_runner)  # stops last so in-flight jobs finish first

    # After startup, recover any PENDING jobs from the previous process run.
    # This is a no-op if the InMemoryJobStore is used (jobs don't survive restart).
    # In production, use a persistent job store (e.g. SAJobStore) to benefit
    # from recovery — ``recover()`` will re-queue and resume PENDING jobs.
    #
    # Note: we do NOT call job_runner.recover() here because it requires the
    # app to be fully started (event loop running, DB connections ready).
    # A startup hook or a separate script is the right place for recovery.

    # ── 10. Build routers ─────────────────────────────────────────────────────
    # VarcoCRUDRouter.__init__ declares `service: Inject[AsyncService[D, PK, C, R, U]]`
    # where D, PK, C, R, U are unresolved TypeVars.  Providify cannot map bare TypeVars
    # to concrete types at resolution time, so it falls back to the `| None` default.
    # The correct pattern is to resolve the service with concrete types first, then
    # pass it directly to the router constructor — bypassing generic TypeVar resolution.
    #
    # DESIGN: manual constructor call over container.aget(PostRouter)
    #   ✅ Concrete service type is explicit — no TypeVar ambiguity.
    #   ✅ Matches the VarcoCRUDRouter docstring example: ``OrderRouter().build_router()``.
    #   ❌ Wiring is in app.py rather than the DI container — acceptable for a top-level
    #      bootstrap function that already knows all concrete types.
    from varco_core.service.base import AsyncService as _AsyncService  # noqa: PLC0415
    from uuid import UUID as _UUID  # noqa: PLC0415
    from example.dtos import (
        PostCreate as _PostCreate,
        PostRead as _PostRead,
        PostUpdate as _PostUpdate,
    )  # noqa: PLC0415

    post_service = await container.aget(
        _AsyncService[Post, _UUID, _PostCreate, _PostRead, _PostUpdate]
    )
    post_router_instance = PostRouter(service=post_service)
    api_router = post_router_instance.build_router()

    # ── 11. Create FastAPI app ─────────────────────────────────────────────────
    app = FastAPI(
        title="Varco Post Example",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Standard middleware — error handler must be outermost
    # ErrorMiddleware must be outermost so it catches exceptions from all inner layers.
    app.add_middleware(ErrorMiddleware)
    install_cors(app)

    app.include_router(api_router)
    return app
