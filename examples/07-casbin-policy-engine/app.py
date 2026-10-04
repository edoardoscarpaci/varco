"""
app.py
======
Application factory for the ``07-casbin-policy-engine`` example.

Demonstrates Casbin RBAC authorization integrated at the service layer via
``PolicyEngineAuthorizer``.  The policy store is backed by SQLite (in tests)
or PostgreSQL (in production via ``VARCO_CASBIN_DB_URL``).

Bootstrap sequence
------------------
1.  Create a fresh ``DIContainer``.
2.  Scan ``varco_core`` — registers ``BaseAuthorizer`` and defaults.
3.  Scan ``varco_fastapi`` — discovers ``VarcoFastAPIConfiguration`` and framework defaults.
4.  Install ``DocumentModule`` — registers ``IUoWProvider``.
5.  Scan ``assembler``, ``service`` — discovers ``DocumentAssembler``,
    ``DocumentService``.
6.  Build ``CasbinPolicyEngine`` with ``CasbinSettings`` from ``db_url`` /
    ``model_preset``.  The engine is started in the FastAPI lifespan.
7.  Register the engine as ``PolicyEngine`` / ``PolicyManagement`` and call
    ``enable_policy_authorizer(container)`` — this shadows ``BaseAuthorizer``
    with ``PolicyEngineAuthorizer``.
8.  Resolve ``DocumentService`` from the container (manual injection
    workaround — see DESIGN note below).
9.  Set ``DocumentRouter._auth`` and mount the router.
10. Wire the lifespan to start / stop the engine.

Run locally::

    cd examples/07-casbin-policy-engine
    VARCO_CASBIN_DB_URL=sqlite+aiosqlite:///./policy.db \\
        uv run uvicorn app:app --reload

DESIGN: manual engine construction over container.scan("varco_casbin")
    ``container.scan("varco_casbin")`` would discover ``CasbinPolicyEngine``
    and register it, BUT we need to pass a runtime ``db_url`` parameter that
    is not available as an env var in test fixtures.  Constructing the engine
    directly and registering it via ``container.provide`` gives us full
    control over the settings object.

    ✅ ``db_url`` can be supplied programmatically — essential for tests that
       spin up a fresh SQLite file per run.
    ✅ Engine lifecycle (``start()`` / ``stop()``) is managed by the lifespan.
    ❌ One extra ``container.bind`` call versus a pure scan approach.

DESIGN: manual service injection over container.get(DocumentRouter)
    ``VarcoCRUDRouter`` uses ``from __future__ import annotations`` which
    prevents providify from resolving generic ``Inject[AsyncService[D, PK, ...]]``
    for a concrete subclass.  Resolve the service directly and pass it manually.

    ✅ Works reliably across Python 3.12+.
    ❌ Two extra lines — acceptable for a clear, explicit wiring.

Thread safety:  ✅ ``create_app()`` is called once at startup.
Async safety:   ✅ Engine start/stop deferred to lifespan — safe for asyncio.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from auth import HeaderAuth
from di import DocumentModule
from dtos import DocumentCreate, DocumentRead, DocumentUpdate
from fastapi import FastAPI
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from models import Document
from providify import DIContainer, Provider
from router import DocumentRouter
from starlette.requests import Request
from varco_core.auth import AbstractAuthorizer, PolicyEngine, PolicyManagement
from varco_core.auth.policy import PolicyEngineAuthorizer, RequestMapper
from varco_core.service.base import AsyncService
from varco_fastapi.exceptions import add_exception_handlers
from varco_fastapi.middleware import (
    ErrorMiddleware,
    RequestContextMiddleware,
    install_middleware_stack,
)


def create_app(
    *,
    db_url: str = "sqlite+aiosqlite:///./policy.db",
    model_preset: str = "rbac",
) -> FastAPI:
    """
    Build and return the configured FastAPI application.

    This function is synchronous — uvicorn can load it without ``--factory``.
    Engine startup (``CasbinPolicyEngine.start()``) is deferred to the ASGI
    lifespan so the event loop exists when asyncio primitives are created.

    Args:
        db_url:       SQLAlchemy async URL for the Casbin policy store.
                      Default is a local SQLite file for development.
                      Use ``sqlite+aiosqlite:///./policy.db`` for local dev;
                      ``postgresql+asyncpg://...`` for production.
        model_preset: Casbin model preset — ``"acl"`` | ``"rbac"`` |
                      ``"rbac_domains"`` | ``"abac"``.  Defaults to ``"rbac"``.

    Returns:
        A configured ``FastAPI`` application ready for an ASGI server.

    Edge cases:
        - Each call produces an independent ``FastAPI`` instance with its own
          ``DIContainer``, engine, and in-memory document store — safe for
          test isolation.
        - The Casbin engine creates the ``casbin_rule`` table automatically
          on first ``start()`` (idempotent ``CREATE TABLE IF NOT EXISTS``).
        - Tests using ``ASGITransport`` must trigger the lifespan explicitly
          (e.g. ``async with app_lifespan_context(app):``); the fixture in
          ``conftest.py`` handles this via ``httpx.ASGITransport`` with a
          manual ``lifespan_startup=True`` pattern or by calling
          ``engine.start()`` directly.

    Thread safety:  ✅ Intended to be called once per process.
    Async safety:   ✅ Synchronous factory; async init deferred to lifespan.
    """
    # ── 1. Import here to avoid circular imports at module level ─────────────
    from varco_casbin.config import CasbinSettings
    from varco_casbin.engine import CasbinPolicyEngine

    # ── 2. Build the Casbin engine with explicit settings ─────────────────────
    # Constructed synchronously; started asynchronously inside the lifespan.
    # The ``adapter="sqlalchemy"`` backend persists rules across restarts.
    # Tests pass ``db_url="sqlite+aiosqlite:///..."`` for a fixture-local DB.
    settings = CasbinSettings(
        model_preset=model_preset,
        adapter="sqlalchemy",
        db_url=db_url,
        auto_save=True,
    )
    engine = CasbinPolicyEngine(settings)

    # ── 3. DI container bootstrap ─────────────────────────────────────────────
    container = DIContainer()

    # Scan varco_core — registers ``BaseAuthorizer`` (@Fallback; development-only since 4.0)
    # and other @Singleton defaults (InMemoryEventBus, etc.).
    container.scan("varco_core", recursive=True)

    # Scan varco_fastapi — discovers VarcoFastAPIConfiguration (@Configuration)
    # and all framework @Singleton defaults.
    container.scan("varco_fastapi", recursive=True)

    # DocumentModule registers IUoWProvider → InMemoryUoWProvider.
    container.install(DocumentModule)

    # Scan assembler and service — discovers @Singleton classes.
    container.scan("assembler")  # DocumentAssembler
    container.scan("service")  # DocumentService

    # ── 4. Register the Casbin engine in the container ────────────────────────
    # Bind the already-constructed engine to both interfaces so the authorizer
    # provider can resolve ``PolicyEngine`` and the router can resolve
    # ``PolicyManagement`` if needed.

    @Provider(singleton=True)
    def _provide_engine_as_policy_engine() -> PolicyEngine:
        """Return the pre-built engine as PolicyEngine."""
        return engine  # type: ignore[return-value]

    @Provider(singleton=True)
    def _provide_engine_as_policy_management() -> PolicyManagement:
        """Return the pre-built engine as PolicyManagement."""
        return engine  # type: ignore[return-value]

    @Provider(singleton=True)
    def _provide_authorizer(
        policy_engine: PolicyEngine,  # type: ignore[valid-type]
    ) -> AbstractAuthorizer:
        """
        Provide ``PolicyEngineAuthorizer`` as the application authorizer.

        The ``RequestMapper`` uses ``ctx.user_id`` as the Casbin subject,
        which is the ``X-User-Id`` header value set by ``HeaderAuth``.

        DESIGN: the mapper derives the Casbin subject from ``ctx.user_id``
            ✅ ``X-User-Id`` header → ``ctx.user_id`` → Casbin ``sub``.
            ✅ Same key convention as the token-based examples.
        """
        return PolicyEngineAuthorizer(policy_engine, RequestMapper())

    # Register all three providers — scan does not auto-register them.
    container.provide(_provide_engine_as_policy_engine)
    container.provide(_provide_engine_as_policy_management)
    container.provide(_provide_authorizer)

    # ── 5. Auth wiring ─────────────────────────────────────────────────────────
    # Use the lightweight header-based auth for this example.
    # Real applications would use JwtBearerAuth(registry=registry) here.
    server_auth = HeaderAuth()
    DocumentRouter._auth = server_auth  # type: ignore[attr-defined]

    # ── 6. Resolve service and construct router ─────────────────────────────────
    service = container.get(
        AsyncService[Document, UUID, DocumentCreate, DocumentRead, DocumentUpdate]
    )
    document_router_instance = DocumentRouter(service=service)
    api_router = document_router_instance.build_router()

    # ── 7. Lifespan — start the engine inside the running event loop ───────────
    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        """
        ASGI lifespan that starts and stops the Casbin engine.

        ``CasbinPolicyEngine.start()`` must run inside the event loop so the
        lazy ``asyncio.Lock`` is created on the correct loop, and so
        ``create_table()`` (on the SQLAlchemy adapter) can ``await`` properly.

        Raises:
            RuntimeError: If the engine fails to start (e.g. DB unreachable).
        """
        await engine.start()
        try:
            yield
        finally:
            # Graceful teardown — clears in-memory enforcer state.
            await engine.stop()

    # ── 8. FastAPI app ─────────────────────────────────────────────────────────
    app = FastAPI(
        title="Casbin Policy Engine Example",
        version="0.1.0",
        description=(
            "Demonstrates Casbin RBAC authorization at the service layer.\n\n"
            "Endpoints:\n"
            "- ``POST   /v1/documents``      — requires Casbin policy (create)\n"
            "- ``GET    /v1/documents/{id}`` — requires Casbin policy (read)\n"
            "- ``PUT    /v1/documents/{id}`` — requires Casbin policy (update)\n"
            "- ``DELETE /v1/documents/{id}`` — requires Casbin policy (delete)\n"
        ),
        lifespan=lifespan,
    )

    # Middleware stack (outermost → innermost):
    #   ErrorMiddleware           — catches all exceptions, returns JSON
    #   RequestContextMiddleware  — calls HeaderAuth, sets AuthContext ContextVar
    install_middleware_stack(
        app,
        [
            ErrorMiddleware,
            (RequestContextMiddleware, {"server_auth": server_auth}),
        ],
    )

    # Register varco service-exception → HTTP status-code handlers.
    add_exception_handlers(app)

    # Explicit HTTPException handler so 401s from HeaderAuth reach the client
    # as JSON, not HTML (Starlette's default plain-text 401 body).
    @app.exception_handler(HTTPException)
    async def _http_exc_handler(request: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=dict(exc.headers) if exc.headers else {},
        )

    app.include_router(api_router)

    # Attach the engine reference to the app state so tests can access it
    # without re-resolving from the container (useful for seeding policies).
    app.state.engine = engine

    return app


# Module-level app instance for ``uvicorn app:app``.
# Uses the default SQLite file; override via VARCO_CASBIN_DB_URL env var.
app = create_app()

__all__ = ["app", "create_app"]
