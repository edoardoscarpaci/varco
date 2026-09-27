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
    VARCO_CORS_ORIGINS         Comma-separated allowed origins (default: none —
                               unset means no cross-origin access, since 3.0.0)

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

import os

from fastapi import FastAPI
from providify import DIContainer
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase
from varco_fastapi.app import create_varco_app
from varco_fastapi.modules.defaults import default_modules
from varco_redis.modules import RedisModule
from varco_sa.config import SAConfig
from varco_sa.modules import SAModule
from varco_ws.modules import WsModule

from example.auth import build_jwt_authority
from example.models import Post
from example.module import PostAppModule


# ── SQLAlchemy declarative base ───────────────────────────────────────────────
# All varco_sa-managed entities inherit from DomainModel, but SQLAlchemy still
# needs a shared DeclarativeBase so table metadata is collected in one place.
# Declared here (not in models.py) to keep the domain model backend-agnostic.
class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base for all entities in this app."""


def create_app() -> FastAPI:
    """
    Build and return the configured FastAPI application.

    Plan 049 / DI-1 (§D-example): this factory now builds one
    ``DIContainer`` and calls ``create_varco_app(container, modules=[...])``
    instead of hand-building ``FastAPI`` + ``VarcoLifespan`` — the module
    list *is* the bootstrap sequence, in order:

    1. Build the JWT authority + ``TrustedIssuerRegistry`` + ``JwtBearerAuth``
       (sync, outside DI — a pure value object, no lifecycle).
    2. Build ``SAConfig`` from ``DATABASE_URL`` (sync — the engine itself
       connects lazily on first use).
    3. ``create_varco_app(container, modules=[*default_modules(),
       SAModule(config=..., entities=(Post,)), RedisModule(cache=True),
       WsModule(), PostAppModule(authority, registry, server_auth)],
       routers=[])`` — ``default_modules()`` already lists ``CoreModule``/
       ``ObservabilityModule`` first; every module's ``register()`` runs
       synchronously here; ``async_init()``/``start()``/``stop()`` run inside
       the returned app's ASGI lifespan (unchanged ``VarcoLifespan`` timing).
    4. ``routers=[]`` — ``PostRouter``/``AuthRouter``/``StreamsRouter`` all
       need a DI-injected or hand-built constructor argument
       ``create_varco_app``'s ``_mount_router`` cannot supply (a synchronous,
       no-argument ``router_cls()`` call) — ``PostAppModule.async_init()``
       mounts all three instead (see its own docstring).

    Returns:
        A configured ``fastapi.FastAPI`` application ready for an ASGI
        server. Async initialization runs inside the ASGI lifespan at
        startup, exactly as before this plan.

    Raises:
        KeyError: ``DATABASE_URL``, ``VARCO_REDIS_URL``, or
                  ``VARCO_REDIS_CACHE_URL`` env var is missing.
        KeyLoadError: ``VARCO_JWT_PRIVATE_KEY_PEM`` is set but malformed.

    Edge cases:
        - If the database or Redis is unavailable, the app STARTS but the
          first request that touches them fails.
        - ``create_app()`` is NOT idempotent when auto-generating RSA keys —
          each call produces a different authority. Call once per process.
        - In dev mode (no PEM env var), tokens issued before an app restart
          will fail verification after restart. Users must re-login.

    Thread safety:  ✅ Called once at startup; not thread-safe to call twice.
    Async safety:   ✅ Sync — no event loop required at call time.
    """
    # ── 1. JWT authority (auth infrastructure) ────────────────────────────────
    # Built outside DI — a pure value object (key material), no lifecycle;
    # passed directly to PostAppModule for AuthRouter/TrustedIssuerRegistry.
    authority = build_jwt_authority()

    from varco_core.authority.registry import TrustedIssuerRegistry  # noqa: PLC0415

    registry = TrustedIssuerRegistry()
    registry.register_authority(authority, label="VARCO_EXAMPLE")

    # JwtBearerAuth verifies Authorization: Bearer <token> on every request.
    # required=False so anonymous callers can still hit public endpoints
    # (list posts, read posts) without providing a token.
    from varco_fastapi.auth import JwtBearerAuth  # noqa: PLC0415

    server_auth = JwtBearerAuth(registry=registry, required=False)

    # ── 2. SAConfig from DATABASE_URL ──────────────────────────────────────────
    # Eager here (not the lazy make_sa_provider() shape) — SAModule's own
    # contract is "bind_instance(container, SAConfig, config)" with an
    # already-constructed value; DATABASE_URL is already set by the time
    # create_app() is called (the same constraint the pre-049 lazy provider
    # carried — its first resolution always happened after env setup too).
    sa_config = SAConfig(
        engine=create_async_engine(os.environ["DATABASE_URL"]),
        base=Base,
        entity_classes=(Post,),
    )

    # ── 3. Build the app through create_varco_app(modules=[...]) ──────────────
    container = DIContainer()
    app = create_varco_app(
        container,
        modules=[
            *default_modules(),
            SAModule(config=sa_config, entities=(Post,)),
            RedisModule(cache=True),
            WsModule(),
            PostAppModule(authority, registry, server_auth),
        ],
        routers=[],  # PostAppModule.async_init() mounts the three app routers
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
        # validate defaults to True — AbstractServerAuth (PostAppModule's
        # bind_instance) and AbstractJobStore/AbstractJobRunner
        # (FastAPIModule, via default_modules()) are both satisfied.
    )

    return app
