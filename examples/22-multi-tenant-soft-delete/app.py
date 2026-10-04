"""
app.py
======
Application factory for the ``22-multi-tenant-soft-delete`` example.

Demonstrates:

- ``TenantAwareService`` — row-level tenant isolation via ``_scoped_params``
  and ``_check_entity`` hooks, without a single query-level guard in the HTTP layer.
- ``SoftDeleteService`` — ``delete()`` stamps ``deleted_at`` instead of issuing
  ``DELETE``; ``list()`` automatically excludes soft-deleted rows.
- ``ValidatorServiceMixin`` — ``title`` is validated non-blank before write.
- ``SAConfig`` / ``varco_sa`` — async PostgreSQL backend via asyncpg.
- No JWT — tenant identity is passed as ``X-Tenant-Id`` header (demo only).

Bootstrap sequence
------------------
1. ``SAConfig`` is registered with the DI container.
2. ``varco_fastapi.di.bootstrap()`` installs the framework defaults.
3. ``sa_bootstrap(container, Note)`` scans varco_sa and registers
   ``AsyncRepository[Note]`` + ``IUoWProvider`` bindings.
4. On FastAPI startup, ``create_tables(container)`` runs DDL (idempotent).
5. Requests arrive; the ``X-Tenant-Id`` header is extracted in each handler
   and placed in ``AuthContext(metadata={"tenant_id": ...})`` so that
   ``TenantAwareService._require_tenant()`` can read it without coupling the
   service layer to HTTP.

DESIGN: tenant via header, not JWT, for simplicity
    ✅ Zero key management in a demo context.
    ✅ The service-layer pattern is identical to a JWT-based app — only the
       extraction point changes.
    ❌ Never use plain header-based tenant identity in production; use a signed
       JWT or session token so the caller cannot spoof another tenant's ID.

DESIGN: plain ``APIRouter`` instead of ``VarcoRouter``
    ✅ Handlers need the ``X-Tenant-Id`` header — ``@route`` on ``GenericRouter``
       does not inject the raw ``Request`` (see Finding F10 — ``@route`` only
       injects ``ctx`` and path params).
    ✅ No ``_auth`` ClassVar to configure — no JWT auth in this example.
    ❌ No automatic OpenAPI schema for the ``ctx`` injection — acceptable for
       a demo focused on service-layer patterns.

Thread safety:  ✅ ``create_app()`` is called once; the result is immutable.
Async safety:   ✅ No async calls at factory time; ``create_tables`` deferred
                   to the FastAPI startup event.
"""

from __future__ import annotations

import os
from uuid import UUID

# Import @Singleton classes so DI metadata is stamped before the container
# resolves them.  The noqa comments suppress "unused import" warnings —
# the side-effects of importing are the point.
from assembler import NoteAssembler  # noqa: F401
from dtos import NoteCreate, NoteRead
from fastapi import APIRouter, FastAPI, Header, Response
from fastapi.responses import JSONResponse
from models import Note
from providify import DIContainer, Provider
from service import NoteService  # noqa: F401
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from varco_core.auth import AuthContext
from varco_core.auth.authorizer import BaseAuthorizer
from varco_core.auth.base import AbstractAuthorizer
from varco_core.query.params import QueryParams
from varco_core.service.base import IUoWProvider
from varco_fastapi.di import bootstrap as fastapi_bootstrap
from varco_sa.provider import SQLAlchemyRepositoryProvider

# ── Shared SQLAlchemy declarative base ────────────────────────────────────────


# DESIGN: module-level Base (one per process)
#   ✅ All SA ORM classes generated for entity_classes share one metadata,
#      so ``create_all`` creates every table in a single call.
#   ❌ Calling ``create_app()`` twice in the same process would try to re-register
#      the same table in the same metadata — tests must call create_app() once per
#      session and reuse the result.
class Base(DeclarativeBase):
    """Shared SQLAlchemy declarative base for this example."""


# ── DI container bootstrap ────────────────────────────────────────────────────


def _build_container(db_url: str) -> tuple[DIContainer, object]:
    """
    Assemble and return a fully configured ``DIContainer`` and the SA engine.

    Uses ``SQLAlchemyRepositoryProvider.from_components()`` directly rather than
    the DI auto-injection path — the same pattern as the field-encryption example.
    This avoids an annotation-resolution issue with ``@Provider`` on local
    functions under ``from __future__ import annotations``.

    Registration order:
    1. Build ``SQLAlchemyRepositoryProvider`` from the engine.
    2. Bind ``IUoWProvider`` → the pre-built provider.
    3. Run ``varco_fastapi.di.bootstrap()`` for framework defaults.
    4. Bind ``AbstractAuthorizer`` → ``BaseAuthorizer`` (permissive).
    5. Bind example-local ``@Singleton`` classes.

    Args:
        db_url: PostgreSQL connection URL using the ``postgresql+asyncpg://`` scheme.

    Returns:
        ``(DIContainer, engine)`` — the container and the engine (so
        ``create_app`` can run DDL without going back through DI).
    """
    container = DIContainer()

    engine = create_async_engine(db_url, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # ── 1. Build the provider directly ────────────────────────────────────────
    provider = SQLAlchemyRepositoryProvider.from_components(
        base=Base,
        session_factory=session_factory,
    )
    provider.register(Note)

    # ── 2. Bind IUoWProvider directly ─────────────────────────────────────────
    @Provider(singleton=True)
    def _uow_provider() -> IUoWProvider:
        """Return the pre-built SA provider as IUoWProvider."""
        return provider  # type: ignore[return-value]

    container.provide(_uow_provider)

    # ── 3. VarcoFastAPI defaults ──────────────────────────────────────────────
    # 4.0 (Plan 049): the framework @Configuration is VarcoFastAPIConfiguration; the
    # 4.0 hand-wired form is the package bootstrap() (scan-installs it, idempotent,
    # so create_varco_app's FastAPIModule re-running it later is safe).
    fastapi_bootstrap(container)

    # ── 4. Permissive authorizer ──────────────────────────────────────────────
    # 4.0 (Plan 053 / FLIP-5): a class-keyed bind of BaseAuthorizer is gated
    # exactly like the scanned fallback — run with VARCO_SECURITY_ENV=development
    # (see README). A production app registers a real AbstractAuthorizer.
    container.bind(AbstractAuthorizer, BaseAuthorizer)

    # ── 5. Example-local @Singleton classes ───────────────────────────────────
    container.bind(NoteAssembler, NoteAssembler)
    container.bind(NoteService, NoteService)

    return container, engine


# ── HTTP router ───────────────────────────────────────────────────────────────


def _build_router(service: NoteService) -> APIRouter:
    """
    Build the ``APIRouter`` for note endpoints.

    Uses a plain ``APIRouter`` (not ``VarcoRouter``) because handlers need the
    ``X-Tenant-Id`` header, which ``@route`` on ``GenericRouter`` does not inject
    (Finding F10 — ``@route`` only injects ``ctx`` and path params).

    ``X-Tenant-Id`` is placed in ``AuthContext.metadata`` so that
    ``TenantAwareService._require_tenant()`` extracts it transparently without
    knowing about HTTP headers.

    Args:
        service: The fully resolved ``NoteService`` singleton.

    Returns:
        A configured ``APIRouter`` with all note endpoints plus health check.

    DESIGN: service injected at router-build time (not per-request)
        ✅ ``NoteService`` is a ``@Singleton`` — safe to share across requests.
        ✅ Avoids re-resolving the DI graph on every request.
        ❌ The router cannot be built before the DI container is ready.
    """
    router = APIRouter()

    # ── GET /health ────────────────────────────────────────────────────────────
    @router.get("/health", tags=["ops"])
    async def health() -> dict:
        """
        Liveness probe — no DB access, always 200.

        Returns:
            ``{"status": "ok"}``
        """
        return {"status": "ok"}

    # ── POST /v1/notes ─────────────────────────────────────────────────────────
    @router.post("/v1/notes", response_model=NoteRead, status_code=201, tags=["notes"])
    async def create_note(
        body: NoteCreate,
        x_tenant_id: str = Header(..., alias="X-Tenant-Id"),
    ) -> NoteRead:
        """
        Create a new note for the caller's tenant.

        ``tenant_id`` is stamped by ``TenantAwareService._prepare_for_create``
        from ``ctx.metadata["tenant_id"]`` — the request body cannot carry a
        foreign tenant ID.

        Args:
            body: ``NoteCreate`` payload — ``title`` is required.
            x_tenant_id: Tenant identifier from the ``X-Tenant-Id`` header.

        Returns:
            The persisted ``NoteRead`` DTO with a 201 status.

        Raises:
            422: ``title`` is blank or the header is missing.
        """
        ctx = AuthContext(user_id=x_tenant_id, metadata={"tenant_id": x_tenant_id})
        return await service.create(body, ctx)

    # ── GET /v1/notes ──────────────────────────────────────────────────────────
    @router.get("/v1/notes", response_model=list[NoteRead], tags=["notes"])
    async def list_notes(
        x_tenant_id: str = Header(..., alias="X-Tenant-Id"),
    ) -> list[NoteRead]:
        """
        List active (non-soft-deleted) notes for the calling tenant.

        ``SoftDeleteService._scoped_params`` appends ``deleted_at IS NULL``
        automatically; ``TenantAwareService._scoped_params`` prepends
        ``tenant_id = <tid>``.  The caller never sees another tenant's notes or
        soft-deleted rows.

        Args:
            x_tenant_id: Tenant identifier from the ``X-Tenant-Id`` header.

        Returns:
            List of ``NoteRead`` DTOs.  Empty list when no active notes exist.
        """
        ctx = AuthContext(user_id=x_tenant_id, metadata={"tenant_id": x_tenant_id})
        # service.list() returns list[NoteRead] directly (no PageResult envelope).
        # QueryParams() with all defaults = no filter, no sort, no pagination limit
        # — TenantAwareService._scoped_params adds tenant_id + soft-delete filter.
        return await service.list(QueryParams(), ctx)

    # ── GET /v1/notes/{id} ─────────────────────────────────────────────────────
    @router.get("/v1/notes/{note_id}", response_model=NoteRead, tags=["notes"])
    async def get_note(
        note_id: UUID,
        x_tenant_id: str = Header(..., alias="X-Tenant-Id"),
    ) -> NoteRead:
        """
        Fetch a single note by UUID.

        Raises 404 when the note does not exist, belongs to a different tenant,
        or has been soft-deleted.  A 403 is never returned here so the existence
        of cross-tenant notes cannot be inferred (existence oracle prevention).

        Args:
            note_id: UUID from the URL path.
            x_tenant_id: Tenant identifier from header.

        Returns:
            ``NoteRead`` DTO.

        Raises:
            404: Note not found, wrong tenant, or soft-deleted.
        """
        ctx = AuthContext(user_id=x_tenant_id, metadata={"tenant_id": x_tenant_id})
        return await service.get(note_id, ctx)

    # ── DELETE /v1/notes/{id} ──────────────────────────────────────────────────
    @router.delete("/v1/notes/{note_id}", status_code=204, tags=["notes"])
    async def delete_note(
        note_id: UUID,
        x_tenant_id: str = Header(..., alias="X-Tenant-Id"),
    ) -> Response:
        """
        Soft-delete a note by setting ``deleted_at`` to the current UTC time.

        The note row remains in the database and is visible via the
        ``/v1/notes/deleted`` endpoint.  Physical removal is intentionally
        unsupported — implement a background retention job for hard deletes.

        Args:
            note_id: UUID from the URL path.
            x_tenant_id: Tenant identifier from header.

        Returns:
            204 No Content on success.

        Raises:
            404: Note not found, wrong tenant, or already soft-deleted.
        """
        ctx = AuthContext(user_id=x_tenant_id, metadata={"tenant_id": x_tenant_id})
        await service.delete(note_id, ctx)
        # Return an explicit Response so FastAPI does not try to serialize None.
        return Response(status_code=204)

    return router


# ── Exception mapping ─────────────────────────────────────────────────────────


def _add_exception_handlers(app: FastAPI) -> None:
    """
    Map service-layer exceptions to HTTP status codes.

    ``AsyncService`` raises typed exceptions from ``varco_core.exception.service``
    rather than ``HTTPException`` — the service layer stays HTTP-agnostic.
    The HTTP adapter layer translates them here.

    Args:
        app: The ``FastAPI`` application to attach handlers to.
    """
    from varco_core.exception.service import (
        ServiceNotFoundError,
        ServiceValidationError,
    )

    @app.exception_handler(ServiceNotFoundError)
    async def _not_found(_req, exc: ServiceNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(ServiceValidationError)
    async def _validation(_req, exc: ServiceValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})


# ── Application factory ────────────────────────────────────────────────────────


def create_app(db_url: str | None = None) -> tuple[FastAPI, DIContainer]:
    """
    Build and return the configured FastAPI application and its DI container.

    Returns a ``(app, container)`` pair so that test fixtures can call
    ``create_tables(container)`` directly without building a second container —
    ``ASGITransport`` does not trigger the FastAPI lifespan (Finding F06).

    This is the canonical entry point for both uvicorn (``app:app``) and
    the test suite (``create_app(postgres_url)``).

    Steps:
    1. Resolve the database URL from the argument or ``DATABASE_URL`` env var.
    2. Build the DI container (SAConfig, varco_fastapi bootstrap, varco_sa, authorizer).
    3. Resolve ``NoteService`` from the container (singleton).
    4. Build the HTTP router around the resolved service.
    5. Register service-layer exception handlers.
    6. Register a startup event that calls ``create_tables`` (idempotent DDL).

    Args:
        db_url: PostgreSQL connection URL (``postgresql+asyncpg://`` scheme).
                ``None`` → read from the ``DATABASE_URL`` environment variable.

    Returns:
        ``(FastAPI, DIContainer)`` — the fully wired app and its container.
        Tests use the container directly to call ``create_tables``.

    Raises:
        KeyError: ``DATABASE_URL`` is not set and ``db_url`` is ``None``.

    Edge cases:
        - Calling ``create_app()`` twice in the same process shares the
          module-level ``Base`` object.  ``SAModelFactory.build(Note)`` is
          cached (idempotent), so the second call does not raise — but both
          apps point at the same ORM metadata.  Tests call ``create_app()``
          once per session.
        - ``create_tables`` runs ``CREATE TABLE IF NOT EXISTS`` — idempotent
          and safe on every restart; it is not a substitute for Alembic
          migrations in production.

    Thread safety:  ✅ Called once at module import; result is immutable.
    Async safety:   ✅ No async operations at factory time.
    """
    url = db_url or os.environ["DATABASE_URL"]
    container, engine = _build_container(url)

    # Resolve NoteService once — it is a @Singleton, safe to share.
    note_service = container.get(NoteService)

    app = FastAPI(
        title="Multi-Tenant Soft-Delete Notes API",
        version="0.1.0",
        description=(
            "Demonstrates ``TenantAwareService`` + ``SoftDeleteService`` "
            "with the ``varco_sa`` SQLAlchemy backend.\n\n"
            "Pass ``X-Tenant-Id: <tenant>`` header on every request."
        ),
    )

    # Exception handlers must be registered before routes so they take effect
    # for all subsequently registered endpoint errors.
    _add_exception_handlers(app)

    app.include_router(_build_router(note_service))

    # DESIGN: startup event vs lifespan
    #   ✅ Simpler — no need to replicate create_varco_app lifespan logic.
    #   ✅ create_tables() is idempotent — safe on every restart.
    #   ❌ Startup events run AFTER Starlette's lifespan startup — if a request
    #      arrives in that tiny gap, it would hit missing tables.  Acceptable
    #      for a demo; use Alembic migrations in production.
    @app.on_event("startup")
    async def _create_schema() -> None:
        """Create all SA-managed tables on startup (idempotent DDL)."""
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    # Return both app and container so test fixtures can call create_tables
    # directly without re-building the container (avoids the double-container
    # issue where a second _build_container call creates a second engine).
    return app, container


# ── Module-level app for ``uvicorn app:app`` ─────────────────────────────────
# Built only when DATABASE_URL is set so importing this module in a test
# context (where no env var is configured) does not crash.
app: FastAPI | None = None

try:
    if "DATABASE_URL" in os.environ:
        app, _ = create_app()
except Exception:
    pass

__all__ = ["app", "create_app"]
