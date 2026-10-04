"""
app.py
======
Application factory for the ``06-grant-based-authz`` example.

Demonstrates service-layer authorization via JWT-embedded ``ResourceGrant``s
and ownership checks — no external services, no Docker required.

Bootstrap sequence
------------------
1. Create a fresh ``DIContainer``.
2. Scan ``varco_core`` — registers ``BaseAuthorizer`` and other defaults.
3. Scan ``varco_fastapi`` — discovers framework ``@Singleton``/``@Configuration``
   defaults (``TaskRegistry``, ``AbstractJobRunner``, etc.).
4. Install ``DocumentModule`` — registers ``IUoWProvider``.
5. Scan ``assembler``, ``service`` — discovers ``DocumentAssembler``,
   ``DocumentService``, and ``DocumentAuthorizer``.
6. Resolve ``DocumentService`` via ``container.get(AsyncService[Document, ...])``.
7. Set ``DocumentRouter._auth`` to ``JwtBearerAuth(registry=registry)``.
8. Construct ``DocumentRouter(service=service)`` and mount the API router.
9. Wire the ``VarcoLifespan`` to call ``registry.load_all()`` on startup.

Run locally::

    cd examples/06-grant-based-authz
    uv run uvicorn app:app --reload

DESIGN: manual service injection over container.get(DocumentRouter)
    Same workaround as example 01 — ``VarcoCRUDRouter`` uses
    ``from __future__ import annotations`` which prevents providify from
    resolving ``Inject[AsyncService[D, PK, C, R, U]]`` for a concrete
    router subclass.  Resolve the service directly, pass it manually.

    ✅ ``_service`` is correctly set — all CRUD endpoints work.
    ✅ No ``container.bind()`` bookkeeping.
    ❌ Two extra lines — acceptable for a clear, explicit wiring.

Thread safety:  ✅ ``create_app()`` is called once at startup.
Async safety:   ✅ ``create_app()`` is synchronous; async init deferred to lifespan.
"""

from __future__ import annotations

from uuid import UUID

from auth import registry
from di import DocumentModule
from dtos import DocumentCreate, DocumentRead, DocumentUpdate
from fastapi import FastAPI
from fastapi.exceptions import HTTPException
from fastapi.responses import JSONResponse
from models import Document
from providify import DIContainer
from router import DocumentRouter
from starlette.requests import Request
from varco_core.service.base import AsyncService
from varco_fastapi.auth import JwtBearerAuth
from varco_fastapi.exceptions import add_exception_handlers
from varco_fastapi.lifespan import VarcoLifespan
from varco_fastapi.middleware import (
    ErrorMiddleware,
    RequestContextMiddleware,
    install_middleware_stack,
)


def create_app() -> FastAPI:
    """
    Build and return the configured FastAPI application.

    This function is synchronous — uvicorn can load it without ``--factory``.
    All async initialization (``registry.load_all()``) is deferred to a
    ``_bootstrap`` closure that runs inside ``VarcoLifespan`` at startup.

    Returns:
        A configured ``FastAPI`` application ready for an ASGI server.

    Edge cases:
        - Each call to ``create_app()`` produces an independent ``FastAPI``
          instance with its own ``DIContainer`` and in-memory store — safe
          for test isolation.
        - ``DocumentRouter._auth`` is set at the class level here; multiple
          calls to ``create_app()`` overwrite it with a fresh ``JwtBearerAuth``
          pointing at the same shared ``registry`` — harmless.
        - ``registry.load_all()`` must be awaited inside the lifespan so
          token verification works.  Tests that bypass the lifespan must call
          ``await registry.load_all()`` explicitly in their fixture.

    Thread safety:  ✅ Intended to be called once per process.
    Async safety:   ✅ Synchronous — no event loop required at call time.
    """
    # ── 1. DI container bootstrap ─────────────────────────────────────────────
    container = DIContainer()

    # Scan varco_core — registers BaseAuthorizer (@Fallback; development-only since 4.0)
    # and other @Singleton defaults (InMemoryEventBus, etc.).
    container.scan("varco_core", recursive=True)

    # Scan varco_fastapi — discovers VarcoFastAPIConfiguration (@Configuration)
    # and all framework @Singleton defaults.
    container.scan("varco_fastapi", recursive=True)

    # DocumentModule registers IUoWProvider → InMemoryUoWProvider.
    container.install(DocumentModule)

    # Scan assembler and service — discovers @Singleton classes.
    # DocumentAuthorizer (@Singleton at priority 0) is in service.py;
    # scanning service.py registers it so it shadows BaseAuthorizer.
    container.scan("assembler")  # DocumentAssembler
    container.scan("service")  # DocumentService + DocumentAuthorizer

    # ── 2. Auth wiring ────────────────────────────────────────────────────────
    # JwtBearerAuth wraps the registry and verifies every Bearer token.
    # required=True: all endpoints in this example require authentication —
    # unauthenticated requests will receive 401.
    server_auth = JwtBearerAuth(registry=registry, required=True)

    # Set _auth on the class before build_router() is called so the
    # RequestContextMiddleware injects AuthContext on every request.
    DocumentRouter._auth = server_auth  # type: ignore[attr-defined]

    # ── 3. Resolve service and construct router ────────────────────────────────
    service = container.get(
        AsyncService[Document, UUID, DocumentCreate, DocumentRead, DocumentUpdate]
    )
    document_router_instance = DocumentRouter(service=service)
    api_router = document_router_instance.build_router()

    # ── 4. FastAPI app ─────────────────────────────────────────────────────────
    lifespan = VarcoLifespan()

    app = FastAPI(
        title="Grant-Based Authorization Example",
        version="0.1.0",
        description=(
            "Demonstrates service-layer authorization via JWT-embedded "
            "``ResourceGrant``s and ``OwnershipAuthorizer``.\n\n"
            "**Endpoints**:\n"
            "- ``POST   /v1/documents``       — requires ``docs:write`` grant\n"
            "- ``GET    /v1/documents/{id}``  — any authenticated token\n"
            "- ``DELETE /v1/documents/{id}``  — owner or admin role\n"
        ),
        lifespan=lifespan,
    )

    # Middleware stack (outermost → innermost):
    #   ErrorMiddleware           — catches all exceptions, returns JSON
    #   RequestContextMiddleware  — verifies JWT, sets AuthContext ContextVar
    install_middleware_stack(
        app,
        [
            ErrorMiddleware,
            (RequestContextMiddleware, {"server_auth": server_auth}),
        ],
    )

    # Register varco service-exception handlers (403 for ServiceAuthorizationError,
    # 404 for ServiceNotFoundError, etc.).
    add_exception_handlers(app)

    # Explicit HTTPException handler so 401s raised inside
    # RequestContextMiddleware reach the client as JSON responses.
    @app.exception_handler(HTTPException)
    async def _http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content={"detail": exc.detail},
            headers=dict(exc.headers) if exc.headers else {},
        )

    # Mount document routes — registered now so ASGITransport tests work
    # even when the lifespan is not triggered.
    app.include_router(api_router)

    # ── 5. Async bootstrap (deferred to lifespan) ─────────────────────────────
    async def _bootstrap() -> None:
        """
        Async initialisation deferred from ``create_app()``.

        Populates the issuer keyset so ``JwtBearerAuth.verify()`` can
        validate incoming tokens.

        Edge cases:
            - Tests using ``ASGITransport`` must call ``await registry.load_all()``
              explicitly in their fixture — the ASGI lifespan is not triggered
              by ``ASGITransport``.
        """
        await registry.load_all()

    lifespan._setup = _bootstrap

    return app


# Module-level app instance for ``uvicorn app:app``.
app = create_app()

__all__ = ["app", "create_app"]
