"""
app.py
======
Application factory for the ``10-beanie-mongo`` example.

Demonstrates the full ``varco_beanie`` Beanie/MongoDB async ODM backend:

- ``DomainModel`` + ``FieldHint`` / ``PrimaryKey`` → ``BeanieModelFactory``
  auto-generates the Beanie Document class.
- ``BeanieSettings`` — typed configuration bundle (mongo client, db name, entity classes).
- ``BeanieRepositoryProvider`` — resolves ``AsyncRepository[Post]`` from Beanie.
- ``BeanieUnitOfWork`` — unit-of-work pattern backed by Motor sessions.
- ``await provider.init()`` — calls ``init_beanie()`` at startup to register
  all Document classes with the Motor database connection.

Bootstrap sequence
------------------
1. ``BeanieRepositoryProvider`` is built directly from ``BeanieSettings``.
2. ``await provider.init()`` registers all Beanie Document classes (must happen
   before any DB operation).
3. ``IUoWProvider`` is bound to the pre-built provider.
4. FastAPI starts accepting requests.

Why init_beanie is called explicitly in tests
---------------------------------------------
``ASGITransport`` does NOT trigger FastAPI lifespan / startup events, so the
``@app.on_event("startup")`` hook that calls ``await provider.init()`` never fires
during tests.  The test fixture calls ``await provider.init()`` directly before
creating the HTTP client.  ``create_app()`` therefore accepts an optional
pre-initialized ``provider`` argument so tests can inject their own instance.

No auth, no events, no cache — the example is focused entirely on the Beanie backend.

Run locally (requires MongoDB)::

    export MONGODB_URL="mongodb://localhost:27017"
    cd examples/10-beanie-mongo
    uv run uvicorn app:app --reload

Try it::

    curl -X POST http://localhost:8000/v1/posts \\
         -H "Content-Type: application/json" \\
         -d '{"title": "Hello", "content": "World", "author": "alice"}'

    curl "http://localhost:8000/v1/posts"

DESIGN: direct construction over DI scan for BeanieRepositoryProvider
    The DI scan path requires ``BeanieSettings`` to be registered as a
    ``@Provider`` and then ``container.scan("varco_beanie", recursive=True)``
    to discover ``BeanieRepositoryProvider``.  The direct construction path
    (build the provider manually, bind ``IUoWProvider``) is simpler and avoids
    annotation resolution issues with local ``@Provider`` functions under
    ``from __future__ import annotations``.

    ✅ No ``container.scan()`` needed — fewer moving parts.
    ✅ ``provider.init()`` can be called explicitly in the test fixture.
    ✅ Consistent with how example 09 wires SQLAlchemyRepositoryProvider.
    ❌ Not idiomatic DI — production apps should use the full scan+install path
       documented in ``varco_beanie.di``.

Thread safety:  ✅ ``create_app()`` is called once at module import.
Async safety:   ✅ No async operations at factory time — ``provider.init()``
                   runs inside the lifespan startup hook or explicit test setup.
"""

from __future__ import annotations

import os
from uuid import UUID

# Import @Singleton-decorated classes so DI metadata is stamped before the
# container tries to resolve them.
from assembler import PostAssembler  # noqa: F401 — registers PostAssembler
from dtos import PostCreate, PostRead, PostUpdate
from fastapi import FastAPI
from models import Post
from providify import DIContainer, Provider
from pymongo import AsyncMongoClient
from service import PostService  # noqa: F401 — registers PostService
from varco_beanie.config import BeanieSettings
from varco_beanie.provider import BeanieRepositoryProvider
from varco_core.service.base import IUoWProvider
from varco_fastapi.di import bootstrap as fastapi_bootstrap

from varco_fastapi import create_varco_app

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
    from varco_fastapi.router.presets import CRUDRouter  # noqa: PLC0415

    svc = container.get(PostService)

    class PostRouter(CRUDRouter[Post, UUID, PostCreate, PostRead, PostUpdate]):
        """
        REST router for blog posts.

        Provides standard CRUD endpoints:
            POST   /v1/posts/           → 201 Created + PostRead
            GET    /v1/posts/           → 200 + list of PostRead
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


def _build_container(
    mongo_url: str,
    *,
    provider: BeanieRepositoryProvider | None = None,
) -> tuple[DIContainer, BeanieRepositoryProvider]:
    """
    Build and configure a ``DIContainer`` for the blog post service.

    Constructs ``BeanieRepositoryProvider`` directly rather than relying on
    the DI scan path.  This avoids annotation-resolution issues with local
    ``@Provider`` functions under ``from __future__ import annotations``.

    If a pre-initialized ``provider`` is passed (for tests), it is used
    directly without calling ``init()`` again.  The caller is responsible for
    ensuring ``init()`` has already been called.

    Registration order:
    1. Build (or reuse) ``BeanieRepositoryProvider`` from settings.
    2. Bind ``IUoWProvider`` → the pre-built provider.
    3. Run ``varco_fastapi.di.bootstrap()`` for framework defaults.
    4. Bind permissive ``BaseAuthorizer`` (no auth for this example).
    5. Register local ``@Singleton`` classes (``PostAssembler``, ``PostService``).

    Args:
        mongo_url: MongoDB connection URL (``mongodb://...``).
        provider:  Optional pre-initialized provider — used by tests to bypass
                   ``@app.on_event("startup")`` which ASGITransport does not fire.

    Returns:
        ``(DIContainer, BeanieRepositoryProvider)`` — the container and the
        provider (so ``create_app`` can call ``provider.init()`` at startup).

    Raises:
        KeyError: ``MONGODB_URL`` is missing (caught at startup in ``create_app``).
    """
    container = DIContainer()

    if provider is None:
        # Build the provider directly — bypass DI scan to avoid annotation issues.
        client = AsyncMongoClient(mongo_url)
        settings = BeanieSettings(
            mongo_client=client,
            db_name="example_db",
            entity_classes=(Post,),
        )
        provider = BeanieRepositoryProvider(settings=settings)
        # Note: provider.init() (init_beanie) is NOT called here — it must be
        # called inside an async context (event loop).  Call it in the startup
        # hook or explicitly in tests before issuing any DB operations.

    # ── Bind IUoWProvider ──────────────────────────────────────────────────────
    # AsyncService.__init__ injects IUoWProvider.  We use a @Provider closure
    # to return the pre-built provider as IUoWProvider.
    _prov = provider  # capture reference so the closure doesn't capture the loop var

    @Provider(singleton=True)
    def _uow_provider() -> IUoWProvider:
        """Return the pre-built Beanie provider as IUoWProvider."""
        return _prov  # type: ignore[return-value]

    container.provide(_uow_provider)

    # ── Bootstrap varco_fastapi ─────────────────────────────────────────────────
    # 4.0 (Plan 049): the framework @Configuration is VarcoFastAPIConfiguration; the
    # 4.0 hand-wired form is the package bootstrap() (scan-installs it, idempotent,
    # so create_varco_app's FastAPIModule re-running it later is safe).
    fastapi_bootstrap(container)

    # ── Permissive authorizer ──────────────────────────────────────────────────
    # AsyncService injects AbstractAuthorizer; bind BaseAuthorizer (permissive)
    # since this example has no auth.
    from varco_core.auth.authorizer import BaseAuthorizer  # noqa: PLC0415
    from varco_core.auth.base import AbstractAuthorizer  # noqa: PLC0415

    # 4.0 (Plan 053 / FLIP-5): a class-keyed bind of BaseAuthorizer is gated
    # exactly like the scanned fallback — run with VARCO_SECURITY_ENV=development
    # (see README). A production app registers a real AbstractAuthorizer.
    container.bind(AbstractAuthorizer, BaseAuthorizer)

    # ── Register local @Singleton classes ──────────────────────────────────────
    # container.scan() requires an installed package string; local example modules
    # are not installed packages, so we bind them explicitly.
    container.bind(PostAssembler, PostAssembler)
    container.bind(PostService, PostService)

    return container, provider


# ── Application factory ────────────────────────────────────────────────────────


def create_app(
    mongo_url: str | None = None,
    *,
    provider: BeanieRepositoryProvider | None = None,
) -> FastAPI:
    """
    Build and return the configured FastAPI application.

    This is the canonical entry point used by both uvicorn (``app:app``) and
    the test suite (``create_app(mongo_url, provider=pre_initialized_provider)``).

    Steps:
    1. Resolve the MongoDB URL (argument > env var).
    2. Build the DI container with BeanieSettings, varco_fastapi, and varco_beanie.
    3. Build the router class for the Post entity.
    4. Create the FastAPI app via ``create_varco_app`` with a startup hook
       that calls ``await provider.init()`` (registers Beanie Document classes).

    Args:
        mongo_url: MongoDB connection URL.  When ``None``, reads from the
                   ``MONGODB_URL`` environment variable.
        provider:  Optional pre-initialized ``BeanieRepositoryProvider`` for
                   tests.  When provided, ``provider.init()`` is NOT called
                   in the startup hook — the caller has already done so.

    Returns:
        A ``FastAPI`` instance ready to serve requests.

    Raises:
        KeyError: ``MONGODB_URL`` is not set and ``mongo_url`` is ``None``.

    Edge cases:
        - When ``provider`` is passed, ``mongo_url`` is ignored for the provider
          construction but still read for the module-level ``app`` fallback.
        - The startup hook uses ``await provider.init()`` which is idempotent —
          calling it twice is safe (Beanie handles duplicate Document registration).
        - ``ASGITransport`` (used in tests) does NOT trigger the startup hook.
          Tests must call ``await provider.init()`` explicitly before issuing
          any requests.  Pass the pre-initialized provider via the ``provider``
          argument to avoid init being called twice.
    """
    url = mongo_url or os.environ["MONGODB_URL"]
    container, _provider = _build_container(url, provider=provider)
    PostRouter = _make_router(container)

    app = create_varco_app(
        container,
        routers=[PostRouter],
        title="Beanie/MongoDB Example",
        version="0.1.0",
        description=(
            "Demonstrates varco_beanie: BeanieModelFactory, BeanieRepository,\n"
            "BeanieUnitOfWork, and full CRUD via CRUDRouter.\n\n"
            "**No auth** — all endpoints are public for demonstration purposes."
        ),
        validate=False,
    )

    # Only register the startup hook when the provider was NOT pre-initialized
    # by the caller (i.e., in production / uvicorn, not in tests).
    if provider is None:

        @app.on_event("startup")
        async def _init_beanie() -> None:
            """
            Register all Beanie Document classes with the Motor database connection.

            Must be called before any DB operation.  ``ASGITransport`` does NOT
            trigger this hook — tests call ``await provider.init()`` explicitly.
            """
            # _provider is captured from the outer scope — same instance used
            # to build IUoWProvider above.
            await _provider.init()

    return app


# ── Module-level app for ``uvicorn app:app`` ──────────────────────────────────
# The module-level ``app`` reads ``MONGODB_URL`` from the environment when
# the module is imported (at uvicorn startup time).  Tests do NOT import this
# module-level symbol — they call ``create_app(url, provider=provider)`` directly
# with the testcontainers URL and a pre-initialized provider.
app: FastAPI | None = None

try:
    if "MONGODB_URL" in os.environ:
        app = create_app()
except Exception:
    pass


__all__ = ["app", "create_app"]
