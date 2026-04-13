"""
example.app
===========
FastAPI application bootstrap for the Post example.

Wires the complete stack in the correct order:

    1.  JWT authority     (RSA-2048; from env PEM or auto-generated in dev)
    2.  Trusted issuer registry + JwtBearerAuth (per-request auth)
    3.  Event bus         (Redis Pub/Sub)
    4.  Cache backend     (Redis)
    5.  ORM               (SQLAlchemy)
    6.  FastAPI defaults + event producer
    7.  Post domain module (PostService, PostAssembler, PostEventConsumer,
                            PostAuthorizer)
    8.  Router construction → ``include_router``
    9.  Lifespan: bus → consumer → job runner

Environment variables required
-------------------------------
    VARCO_REDIS_URL          redis://localhost:6379
    VARCO_REDIS_CACHE_URL    redis://localhost:6379
    DATABASE_URL             postgresql+asyncpg://user:pass@localhost/db

Environment variables optional
-------------------------------
    VARCO_JWT_PRIVATE_KEY_PEM  PEM-encoded RSA/EC private key for signing JWTs.
                               When absent, an ephemeral RSA-2048 key is generated
                               at startup (tokens invalidated on restart).
    VARCO_CORS_ORIGINS         Comma-separated allowed origins (default: *)

Quick start (with Docker Compose)::

    cd example
    docker compose up

Quick start (manual)::

    docker run -d -p 6379:6379 redis
    docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=pass postgres
    DATABASE_URL="postgresql+asyncpg://postgres:pass@localhost/example" \\
    VARCO_REDIS_URL="redis://localhost:6379" \\
    VARCO_REDIS_CACHE_URL="redis://localhost:6379" \\
    uv run uvicorn example.app:create_app --factory --reload

    # Get a token
    curl -X POST http://localhost:8000/auth/login \\
         -H "Content-Type: application/json" \\
         -d '{"username": "alice", "password": "alice123"}'

    # Use the token
    curl http://localhost:8000/v1/posts \\
         -H "Authorization: Bearer <token>"

DESIGN: create_app() factory over module-level ``app = FastAPI()``
    ✅ Async DI bootstrap (``await container.ainstall(...)``).
    ✅ Multiple independent instances (testing, multi-tenant).
    ✅ Auth wiring happens at factory time — authority is not a global.
    ❌ Uvicorn needs ``--factory`` flag.

Thread safety:  ✅ ``create_app()`` is called once at startup.
Async safety:   ✅ ``create_app()`` is ``async def`` — use with an ASGI server.
"""

from __future__ import annotations

from fastapi import FastAPI
from providify import DIContainer
from sqlalchemy.orm import DeclarativeBase

from varco_core.event.base import AbstractEventBus
from varco_core.job.base import AbstractJobRunner
from varco_fastapi.lifespan import VarcoLifespan
from varco_fastapi.middleware import (
    ErrorMiddleware,
    RequestContextMiddleware,
    install_cors,
    install_middleware_stack,
)
from varco_sa.bootstrap import make_sa_provider
from varco_sa.di import bind_repositories, create_tables

from example.auth import AuthRouter, build_jwt_authority
from example.consumer import PostEventConsumer
from example.models import Post
from example.router import PostRouter
from example.service import PostService
from example.streams import StreamsRouter

from varco_sa.di import bootstrap as sa_bootstrap
from varco_fastapi.di import bootstrap as fastapi_bootstrap
from varco_redis.di import (
    bootstrap as redis_bootstrap,
    async_bootstrap as redis_async_bootstrap,
)
from varco_ws.di import bootstrap as ws_bootstrap


# ── SQLAlchemy declarative base ───────────────────────────────────────────────
# All varco_sa-managed entities inherit from DomainModel, but SQLAlchemy still
# needs a shared DeclarativeBase so table metadata is collected in one place.
# Declared here (not in models.py) to keep the domain model backend-agnostic.
class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base for all entities in this app."""


def create_app() -> FastAPI:
    """
    Build and return the configured FastAPI application.

    This is a **synchronous** factory — uvicorn loads it without ``--factory``.
    All async initialization (DI async installs, registry key loading, component
    resolution) is deferred to a ``_bootstrap`` closure that runs inside
    ``VarcoLifespan`` at startup, before any request is served.

    Bootstrap sequence:
    1.  Build JWT authority + construct ``TrustedIssuerRegistry`` (sync).
    2.  Build ``JwtBearerAuth`` from the registry (sync).
    3.  Build ``DIContainer``, provide ``SAConfig``, and scan all packages
        (``sa_bootstrap`` → ``redis_bootstrap`` → ``fastapi_bootstrap``
        → ``container.scan("example")``).  All sync.
    4.  Build ``VarcoLifespan`` with a ``_bootstrap`` async setup hook.
    5.  Create ``FastAPI`` app with lifespan + middleware stack (sync).
    6.  At startup (inside ``_bootstrap``):
        a.  ``await redis_async_bootstrap(container, setup_cache=True)`` — cache start.
        b.  ``await registry.load_all()`` — populate public keyset.
        c.  Resolve bus, consumer, job_runner and register with lifespan.
        d.  Resolve ``PostService`` and include routers on the app.

    Middleware stack (outermost first, via ``install_middleware_stack``)::

        ErrorMiddleware          ← catches all exceptions, formats JSON
          CORSMiddleware         ← adds CORS headers
            RequestContextMiddleware ← sets AuthContext ContextVar per request

    Returns:
        A configured ``FastAPI`` application ready for an ASGI server.
        Async initialization runs inside ``VarcoLifespan`` at startup.

    Raises:
        KeyError: ``DATABASE_URL``, ``VARCO_REDIS_URL``, or
                  ``VARCO_REDIS_CACHE_URL`` env var is missing.
        KeyLoadError: ``VARCO_JWT_PRIVATE_KEY_PEM`` is set but malformed.

    Edge cases:
        - If the database or Redis is unavailable, the app STARTS but
          the first request that touches them fails.
        - ``create_app()`` is NOT idempotent when auto-generating RSA keys —
          each call produces a different authority.  Call once per process.
        - In dev mode (no PEM env var), tokens issued before an app restart
          will fail verification after restart.  Users must re-login.
        - Routers are registered on the app inside ``_bootstrap`` (at startup),
          not at factory time.  This is safe — FastAPI allows ``include_router``
          before the first request arrives.

    Thread safety:  ✅ Called once at startup; not thread-safe to call twice.
    Async safety:   ✅ Sync — no event loop required at call time.
    """
    # ── 1. JWT authority (auth infrastructure) ────────────────────────────────
    # Build before the DI container — the authority is passed directly to
    # AuthRouter and TrustedIssuerRegistry; no DI binding needed.
    #
    # DESIGN: authority outside DI over @Provider
    #   ✅ Avoids chicken-and-egg with async DI setup.
    #   ✅ Authority is a pure value object (key material) — no lifecycle.
    #   ✅ Simplifies DI scan (no JwtAuthority binding to accidentally shadow).
    #   ❌ Authority not injectable — acceptable for a quickstart; add a
    #      @Provider binding if services need to sign tokens themselves.
    authority = build_jwt_authority()

    # Construct the registry now (sync) but defer load_all() to _bootstrap —
    # load_all() is async and must run inside the event loop.
    # JwtBearerAuth holds a reference to the registry and calls verify() per
    # request; verify() works correctly once load_all() has been awaited.
    from varco_core.authority.registry import TrustedIssuerRegistry  # noqa: PLC0415

    registry = TrustedIssuerRegistry()
    registry.register_authority(authority, label="VARCO_EXAMPLE")

    # JwtBearerAuth verifies Authorization: Bearer <token> on every request.
    # required=False so anonymous callers can still hit public endpoints
    # (list posts, read posts) without providing a token.
    # PostAuthorizer enforces access rules — the auth layer only extracts identity.
    from varco_fastapi.auth import JwtBearerAuth  # noqa: PLC0415

    server_auth = JwtBearerAuth(registry=registry, required=False)

    # ── 2. DI container (sync installs only) ─────────────────────────────────
    # Async installs (RedisEventBusConfiguration, RedisCacheConfiguration)
    # happen inside _bootstrap so they can be awaited in the event loop.
    container = DIContainer()

    # ── 3. SQLAlchemy ORM ─────────────────────────────────────────────────────
    # make_sa_provider reads DATABASE_URL from env lazily (at first resolution).
    # Equivalent to the manual @Provider + create_async_engine + SAConfig block,
    # collapsed to a single line.
    container.provide(make_sa_provider(Base, Post))

    # ── 4. Scan all backend + framework packages ──────────────────────────────
    # Each scan call is idempotent and discovers all @Singleton/@Component classes
    # and module-level @Provider functions in that package.  No install() calls
    # are needed — all @Configuration classes are either empty or replaced by
    # module-level @Provider functions that scan finds automatically.
    sa_bootstrap(container)  # SA repo provider + UoW
    redis_bootstrap(container, streams=True)  # Redis event bus (streams)
    ws_bootstrap(container)  # WebSocket + SSE adapters
    # setup_producer=True binds AbstractEventProducer → BusEventProducer so
    # PostService._produce() publishes events to the Redis bus instead of no-op.
    fastapi_bootstrap(
        container, setup_producer=True
    )  # FastAPI defaults + event producer

    # ── 5. Domain module ──────────────────────────────────────────────────────
    # container.scan() discovers @Singleton-decorated classes in the example
    # package: PostAssembler, PostService, PostEventConsumer, PostAuthorizer.
    # PostAuthorizer (@Singleton at priority 0) shadows BaseAuthorizer
    # (@Singleton at priority -(2**31)) — no explicit bind() needed.
    container.scan("example", recursive=True)

    # PostRouter's base class (VarcoCRUDRouter) has abstract bases so the
    # scanner does not self-bind it.  Explicit bind() makes it resolvable.
    container.bind(PostRouter, PostRouter)

    # PostService extends AsyncService (abstract) and CacheServiceMixin (abstract).
    # The scanner binds PostService to those ABCs but does NOT self-bind it, so
    # container.aget(PostService) would raise LookupError without this explicit
    # self-bind.  Same root cause as PostRouter above.
    container.bind(PostService, PostService)

    # ── 6. Per-entity repository bindings ─────────────────────────────────────
    bind_repositories(container, Post)

    # ── 7. Lifespan + async bootstrap ─────────────────────────────────────────
    # VarcoLifespan.setup runs before any component is started, so _bootstrap
    # can register components (bus, consumer, job_runner) that are then started
    # in the normal FIFO order by VarcoLifespan.
    lifespan = VarcoLifespan()

    # ── 8. Create FastAPI app ─────────────────────────────────────────────────
    # The app is created here (sync) so the middleware stack can be assembled.
    # Routers are added inside _bootstrap (after async DI resolution) — FastAPI
    # allows include_router() at any point before the first request.
    app = FastAPI(
        title="Varco Post Example",
        version="1.0.0",
        description=(
            "End-to-end reference implementation of the varco stack. "
            "Demonstrates: JWT auth, RBAC + ownership authorization, "
            "Redis caching, Redis Streams event bus, SQLAlchemy ORM, "
            "async job runner, CRUD routing, WebSocket push, and SSE streaming.\n\n"
            "**Demo users**: alice/alice123 (admin), bob/bob123 (editor).\n\n"
            "Use ``POST /auth/login`` to obtain a Bearer token, then include "
            "``Authorization: Bearer <token>`` on all requests.\n\n"
            "**Real-time streams**:\n"
            "- WebSocket: ``ws://localhost:8000/ws/posts``\n"
            "- SSE: ``GET /events/posts``"
        ),
        lifespan=lifespan,
    )

    # Middleware stack — outermost first (install_middleware_stack handles reversal).
    # DESIGN: install_middleware_stack over three separate add_middleware calls
    #   ✅ Outermost-first order matches reading order — no mental reversal needed.
    #   ✅ Adding a new middleware is a single list entry, not a position hunt.
    #   ❌ install_cors callable form is slightly less obvious than the direct call.
    #
    # Stack (outermost → innermost):
    #   ErrorMiddleware            — catches all exceptions, returns JSON
    #   CORSMiddleware (via cors)  — adds CORS headers
    #   RequestContextMiddleware   — sets AuthContext ContextVar per request
    install_middleware_stack(
        app,
        [
            ErrorMiddleware,
            (install_cors, {}),  # reads VARCO_CORS_* from env
            (RequestContextMiddleware, {"server_auth": server_auth}),
        ],
    )

    # ── 9. Async bootstrap (runs at startup inside VarcoLifespan) ─────────────
    # Captures app, container, registry, authority, lifespan via closure.
    # All awaitable work lives here — create_app() itself stays sync.
    async def _bootstrap() -> None:
        """
        Async initialization deferred from ``create_app()``.

        Runs inside ``VarcoLifespan.__call__`` before any lifecycle component
        is started, giving _bootstrap a chance to register components that
        VarcoLifespan will then start in order.

        Steps:
        1. ``await create_tables(container)`` — populate ORM metadata + DDL.
        2. ``await redis_async_bootstrap(container, setup_cache=True)`` — cache start.
        3. ``await registry.load_all()`` — populate issuer public keyset.
        4. Resolve and register lifecycle singletons (bus, consumer, ws_bus, sse_bus,
           job_runner) — startup order matters.
        5. Resolve ``PostService``, build routers, include on the app.

        Raises:
            KeyError: ``VARCO_REDIS_CACHE_URL`` missing.
            Exception: Any error from ``registry.load_all()`` or DI resolution.

        Edge cases:
            - If Redis is down, ``ainstall`` may raise — app startup fails cleanly.
            - ``app.include_router()`` called here (not in ``create_app()``) is
              safe: FastAPI processes the route table before the first request.
        """
        # Create DB tables if they don't exist — idempotent (CREATE TABLE IF NOT EXISTS).
        # create_tables() resolves RepositoryProvider first to populate base.metadata,
        # then runs DDL via the async engine.  Not for production migrations — use Alembic.
        await create_tables(container)

        # Install the Redis cache backend (async init — cache.start() must be awaited).
        # redis_async_bootstrap wraps the sync scan + optional ainstall in one call so
        # the _bootstrap closure doesn't need to import RedisCacheConfiguration directly.
        # DESIGN: redis_async_bootstrap over ainstall(RedisCacheConfiguration) here
        #   ✅ Hides the RedisCacheConfiguration import and the two-step pattern.
        #   ✅ setup_cache=True is self-documenting — intent is clear at a glance.
        #   ✅ redis_async_bootstrap(setup_cache=False) is a no-op scan — safe to call twice.
        #   ❌ Slightly less explicit — developer must know setup_cache starts the cache.
        await redis_async_bootstrap(container, setup_cache=True)

        # Populate the issuer public keyset.  Must happen before any request
        # arrives — JwtBearerAuth.verify() will fail on an empty keyset.
        await registry.load_all()
        # Resolve lifecycle singletons and register with lifespan so they are
        # started (and stopped) in the correct order by VarcoLifespan.
        from varco_ws.websocket import WebSocketEventBus  # noqa: PLC0415
        from varco_ws.sse import SSEEventBus  # noqa: PLC0415

        bus: AbstractEventBus = await container.aget(AbstractEventBus)
        consumer: PostEventConsumer = await container.aget(PostEventConsumer)
        ws_bus: WebSocketEventBus = await container.aget(WebSocketEventBus)
        sse_bus: SSEEventBus = await container.aget(SSEEventBus)
        job_runner: AbstractJobRunner = await container.aget(AbstractJobRunner)

        # Startup order: bus first (must be running before consumers/adapters subscribe).
        # ws_bus and sse_bus call bus.subscribe() in their start() — bus must be up.
        # Shutdown is LIFO: job_runner → sse_bus → ws_bus → consumer → bus.
        lifespan.register(bus)
        lifespan.register(consumer)
        lifespan.register(ws_bus)
        lifespan.register(sse_bus)
        lifespan.register(job_runner)

        # PostService is @Singleton — resolve directly by its concrete type.
        # No need to import 5 generic type parameters and use the abstract base.
        post_service: PostService = await container.aget(PostService)
        # Pass auth= in the constructor — no need for the post-construction
        # attribute patch that required a `type: ignore` comment.
        post_router_instance = PostRouter(service=post_service, auth=server_auth)
        api_router = post_router_instance.build_router()

        # Auth router — exposes POST /auth/login and GET /auth/me.
        # The authority is passed directly (not via DI) so the router can sign
        # tokens without needing a container reference at handler time.
        auth_router_instance = AuthRouter(authority=authority)
        auth_api_router = auth_router_instance.build_router()

        # Streams router — WebSocket + SSE push endpoints for post events.
        # StreamsRouter is @Singleton and was discovered by container.scan("example"),
        # so the container injects ws_bus and sse_bus automatically — no manual
        # argument passing needed.
        streams_inst: StreamsRouter = await container.aget(StreamsRouter)
        streams_router = streams_inst.build_router()

        app.include_router(api_router)
        app.include_router(auth_api_router)
        app.include_router(streams_router)

    # Wire _bootstrap as the lifespan setup hook — VarcoLifespan awaits it
    # before starting any registered component.
    #
    # NOTE: VarcoLifespan(setup=_bootstrap) would be equivalent, but _bootstrap
    # captures `lifespan` by reference (to call lifespan.register()) so
    # lifespan must be created first.  Assigning _setup after construction is
    # the idiomatic two-step for this mutual-capture pattern.
    lifespan._setup = _bootstrap

    return app
