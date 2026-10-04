"""
app.py
======
Application factory for the ``09-sqlalchemy-postgres`` example.

Demonstrates the full ``varco_sa`` SQLAlchemy async ORM backend:

- ``DomainModel`` + ``FieldHint`` / ``PrimaryKey`` → ``SAModelFactory`` auto-generates
  the ORM model
- ``SAConfig`` — engine, declarative base, entity class list
- ``SARepository`` — async CRUD via ``AsyncSQLAlchemyRepository``
- ``SAUoWProvider`` — unit-of-work pattern backed by SQLAlchemy sessions
- Schema creation via ``create_tables(container)``
- Query filtering: ``?filter=author__eq=alice`` → SQL WHERE clause via
  ``SQLAlchemyQueryCompiler``

Bootstrap sequence (inside ``VarcoLifespan``)
---------------------------------------------
1. ``SQLAlchemyRepositoryProvider`` is resolved → builds ORM model for ``Post``
   and populates ``base.metadata`` with the table DDL.
2. ``create_tables(container)`` runs ``base.metadata.create_all`` via the async
   engine — idempotent (``CREATE TABLE IF NOT EXISTS``).
3. FastAPI starts accepting requests.

No auth, no events, no cache — the example is focused entirely on the SA backend.

Run locally (requires PostgreSQL)::

    export DATABASE_URL="postgresql+asyncpg://user:pw@localhost:5432/mydb"
    cd examples/09-sqlalchemy-postgres
    uv run uvicorn app:app --reload

Try filtering::

    curl -X POST http://localhost:8000/v1/posts \\
         -H "Content-Type: application/json" \\
         -d '{"title": "Hello", "body": "World", "author": "alice"}'

    curl "http://localhost:8000/v1/posts?filter=author__eq=alice"

DESIGN: ``create_varco_app`` with explicit router + no auth
    ✅ Keeps the example focused on SA backend patterns, not auth setup.
    ✅ ``VarcoLifespan`` handles both ``create_tables`` and graceful shutdown.
    ✅ ``validate=False`` avoids validation errors from the missing ``_auth``
       ClassVar on ``PostRouter`` — intentional for a public-API example.
    ❌ No authentication — never use this config in production.

Thread safety:  ✅ ``create_app()`` is called once at module import.
Async safety:   ✅ No async operations at factory time — ``create_tables``
                   runs inside the lifespan startup hook.
"""

from __future__ import annotations

import os

# Import the @Singleton-decorated classes so their DI metadata is stamped
# before the container tries to resolve them.
from assembler import PostAssembler  # noqa: F401 — registers PostAssembler
from dtos import PostCreate, PostRead, PostUpdate
from fastapi import FastAPI
from models import Post
from providify import DIContainer, Provider
from service import PostService  # noqa: F401 — registers PostService
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from varco_core.service.base import IUoWProvider
from varco_fastapi.di import bootstrap as fastapi_bootstrap
from varco_sa.provider import SQLAlchemyRepositoryProvider

from varco_fastapi import create_varco_app

# ── Shared SQLAlchemy DeclarativeBase ──────────────────────────────────────────


# DESIGN: module-level Base (one per process)
#   ✅ All SA ORM classes generated from entity_classes land in the same
#      metadata — ``create_all`` creates all tables in one call.
#   ❌ Running ``create_app()`` twice in the same process would attempt to
#      re-register the same table in the same metadata — raise ``InvalidRequestError``.
#      Tests call ``create_app()`` once per session to avoid this.
class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base for this example."""


# ── FastAPI router ─────────────────────────────────────────────────────────────


def _make_router(container: DIContainer) -> type:
    """
    Build and return the ``PostRouter`` class with ``_service`` wired from the container.

    Resolves ``PostService`` by its concrete class (not via the generic alias)
    because generic-alias resolution depends on providify's type-matching
    internals, which vary across versions.  The concrete lookup is simpler and
    always unambiguous.

    Args:
        container: Fully configured ``DIContainer`` with ``PostService`` bound.

    Returns:
        A ``CRUDRouter`` subclass for the Post entity, with ``_service`` set.
    """
    from uuid import UUID

    from varco_fastapi.router.presets import CRUDRouter

    svc = container.get(PostService)

    class PostRouter(CRUDRouter[Post, UUID, PostCreate, PostRead, PostUpdate]):
        """
        REST router for blog posts.

        Provides standard CRUD endpoints:
            POST   /v1/posts/           → 201 Created + PostRead
            GET    /v1/posts/           → 200 + list of PostRead (supports ?filter=)
            GET    /v1/posts/{id}       → 200 + PostRead
            PUT    /v1/posts/{id}       → 200 + PostRead
            PATCH  /v1/posts/{id}       → 200 + PostRead
            DELETE /v1/posts/{id}       → 204 No Content

        No ``_auth`` is set — all endpoints are public for this example.
        In production, add ``_auth = JwtBearerAuth(...)`` and a proper
        ``AbstractAuthorizer`` implementation.
        """

        _prefix = "/v1/posts"
        _tags = ["posts"]
        _service = svc

    return PostRouter


# ── DI container bootstrap ─────────────────────────────────────────────────────


def _build_container(db_url: str) -> tuple[DIContainer, object]:
    """
    Build and configure a ``DIContainer`` for the blog post service.

    Uses ``SQLAlchemyRepositoryProvider.from_components()`` directly rather than
    the DI auto-injection path.  The DI-injection path relies on ``@Provider``
    return-type annotations resolving inside local functions; under
    ``from __future__ import annotations`` these annotations become strings and
    providify cannot always resolve them at registration time.  The direct path
    is the same one used by the field-encryption example.

    Registration order:
    1. Build ``SQLAlchemyRepositoryProvider`` from the engine directly.
    2. Bind ``IUoWProvider`` → the pre-built provider.
    3. Run ``varco_fastapi.di.bootstrap()`` for framework defaults.
    4. Scan local ``@Singleton`` classes (``PostAssembler``, ``PostService``).

    Args:
        db_url: PostgreSQL connection URL with ``postgresql+asyncpg://`` scheme.

    Returns:
        ``(DIContainer, engine)`` — the container and the engine (so
        ``create_app`` can run DDL without going back through DI).

    Raises:
        KeyError: ``DATABASE_URL`` is missing (caught at startup in ``create_app``).
    """
    container = DIContainer()

    engine = create_async_engine(db_url, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # ── 1. Build the provider directly ────────────────────────────────────────
    # from_components() bypasses the DI-injection path entirely, avoiding the
    # annotation-resolution issue with local @Provider functions.
    provider = SQLAlchemyRepositoryProvider.from_components(
        base=Base,
        session_factory=session_factory,
    )
    # Register the Post entity so SAModelFactory generates the ORM mapping and
    # Base.metadata is populated (needed for create_all in startup hook).
    provider.register(Post)

    # ── 2. Bind IUoWProvider directly ─────────────────────────────────────────
    # AsyncService.__init__ injects IUoWProvider.  IUoWProvider is imported at
    # module level in varco_fastapi.di, so the return-type string resolves
    # correctly from _uow_provider.__globals__.
    @Provider(singleton=True)
    def _uow_provider() -> IUoWProvider:
        """Return the pre-built SA provider as IUoWProvider."""
        return provider  # type: ignore[return-value]

    container.provide(_uow_provider)

    # ── 3. Bootstrap varco_fastapi ─────────────────────────────────────────────
    # 4.0 (Plan 049): the framework @Configuration is VarcoFastAPIConfiguration; the
    # 4.0 hand-wired form is the package bootstrap() (scan-installs it, idempotent,
    # so create_varco_app's FastAPIModule re-running it later is safe).
    fastapi_bootstrap(container)

    # ── 3b. Permissive authorizer ─────────────────────────────────────────────
    # AsyncService injects AbstractAuthorizer; bind BaseAuthorizer (permissive)
    # since this example has no auth.
    from varco_core.auth.authorizer import BaseAuthorizer  # noqa: PLC0415
    from varco_core.auth.base import AbstractAuthorizer  # noqa: PLC0415

    # 4.0 (Plan 053 / FLIP-5): a class-keyed bind of BaseAuthorizer is gated
    # exactly like the scanned fallback — run with VARCO_SECURITY_ENV=development
    # (see README). A production app registers a real AbstractAuthorizer.
    container.bind(AbstractAuthorizer, BaseAuthorizer)

    # ── 4. Register local @Singleton classes ──────────────────────────────────
    # container.scan() requires an installed package string; local example modules
    # are not installed packages, so we bind them explicitly.
    container.bind(PostAssembler, PostAssembler)
    container.bind(PostService, PostService)

    return container, engine


# ── Application factory ────────────────────────────────────────────────────────


def create_app(db_url: str | None = None) -> FastAPI:
    """
    Build and return the configured FastAPI application.

    This is the canonical entry point used by both uvicorn (``app:app``) and
    the test suite (``create_app(postgres_url)``).

    Steps:
    1. Resolve the database URL (argument > env var).
    2. Build the DI container with SAConfig, varco_fastapi, and varco_sa.
    3. Build the router class for the Post entity.
    4. Create the FastAPI app via ``create_varco_app`` with a custom lifespan
       that runs ``create_tables`` on startup.

    Args:
        db_url: PostgreSQL connection URL.  When ``None``, reads from the
                ``DATABASE_URL`` environment variable.

    Returns:
        A ``FastAPI`` instance ready to serve requests.

    Raises:
        KeyError: ``DATABASE_URL`` is not set and ``db_url`` is ``None``.

    Edge cases:
        - Calling ``create_app()`` twice in the same process may cause SA
          ``InvalidRequestError`` because ``Base.metadata`` is module-level and
          ``SAModelFactory.build(Post)`` is idempotent (uses a cache), but the
          two calls share the same ``Base`` object.  Tests call ``create_app()``
          once per session.
    """
    url = db_url or os.environ["DATABASE_URL"]
    container, engine = _build_container(url)
    PostRouter = _make_router(container)

    app = create_varco_app(
        container,
        routers=[PostRouter],
        title="SQLAlchemy/PostgreSQL Example",
        version="0.1.0",
        description=(
            "Demonstrates varco_sa: SAModelFactory, SARepository, SAUnitOfWork,\n"
            "and QueryParams → SQL WHERE filtering.\n\n"
            "**Filter syntax** (``?filter=field__op=value``):\n"
            "- ``?filter=author__eq=alice``\n"
            "- ``?filter=title__contains=hello``"
        ),
        validate=False,
    )

    # Create tables on startup using the engine directly.
    # provider.register(Post) already populated Base.metadata, so create_all
    # works here without needing to go through the DI container.
    @app.on_event("startup")
    async def _create_schema() -> None:
        """Create all SA-managed tables on startup (idempotent DDL)."""
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    return app


# ── Module-level app for ``uvicorn app:app`` ─────────────────────────────────
# The module-level ``app`` reads ``DATABASE_URL`` from the environment when
# the module is imported (at uvicorn startup time).  Tests do NOT import this
# module-level symbol — they call ``create_app(url)`` directly with the
# testcontainers URL to avoid environment variable dependency.
app: FastAPI | None = None

try:
    if "DATABASE_URL" in os.environ:
        app = create_app()
except Exception:
    pass


__all__ = ["app", "create_app", "Base"]
