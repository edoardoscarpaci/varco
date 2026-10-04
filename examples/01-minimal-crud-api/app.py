"""
app
===
FastAPI application bootstrap for the minimal CRUD example.

``create_app()`` wires the container, installs ``VarcoFastAPIConfiguration`` and
``ProductModule``, registers ``@Singleton`` classes, resolves the router via
DI (so service injection works), and returns a FastAPI app with all product
CRUD endpoints.

No database, no broker, no Docker required — the in-memory repository
keeps everything in process.

Quick start::

    cd examples/01-minimal-crud-api
    uv run uvicorn app:app --reload

Then open http://localhost:8000/docs.

DESIGN: manual service injection over container.get(ProductRouter)
    ``VarcoCRUDRouter`` uses ``from __future__ import annotations``, which
    causes ``get_type_hints()`` to fail on its ``__init__`` signature.
    Providify cannot resolve ``Inject[AsyncService[D, PK, C, R, U]]`` for a
    concrete router subclass through the container.

    The workaround is to:
    1. Build the container and scan ``assembler`` / ``service``.
    2. Resolve ``ProductService`` directly via
       ``container.get(AsyncService[Product, UUID, ...])``.
    3. Construct ``ProductRouter(service=service)`` manually.
    4. Build and include the ``APIRouter`` on the FastAPI app.

    ✅ ``_service`` is correctly set — all CRUD endpoints work.
    ✅ Middleware, error handling, and health endpoint are still provided by
       ``create_varco_app``.
    ✅ No ``container.bind()`` bookkeeping required.
    ❌ Two extra lines — acceptable for a clear, explicit wiring.

Thread safety:  ✅ ``create_app()`` is called once at startup.
Async safety:   ✅ ``create_app()`` is synchronous — no async bootstrap needed.
"""

from __future__ import annotations

from uuid import UUID

from di import ProductModule
from dtos import ProductCreate, ProductRead, ProductUpdate
from fastapi import FastAPI
from models import Product
from providify import DIContainer
from router import ProductRouter
from varco_core.service.base import AsyncService

from varco_fastapi import create_varco_app


def create_app() -> FastAPI:
    """
    Build and return the configured FastAPI application.

    Bootstrap sequence:
        1. Create a fresh ``DIContainer``.
        2. Scan ``varco_core`` to register ``BaseAuthorizer`` and other defaults.
        3. Scan ``varco_fastapi`` to discover ``VarcoFastAPIConfiguration`` and all
           framework ``@Singleton`` defaults.
        4. Install ``ProductModule`` — registers ``IUoWProvider →
           InMemoryUoWProvider``.
        5. Scan ``assembler`` and ``service`` to register ``ProductAssembler``
           and ``ProductService`` under their generic base types.
        6. Call ``create_varco_app(container, routers=[])`` to build the
           FastAPI app with middleware and health endpoint (no routers yet).
        7. Resolve ``ProductService`` via
           ``container.get(AsyncService[Product, ...])``.
        8. Construct ``ProductRouter(service=service)`` manually.
        9. Build and include the ``APIRouter`` on the app.

    Returns:
        A configured ``FastAPI`` application ready for an ASGI server.

    Edge cases:
        - Calling ``create_app()`` multiple times is safe — each call returns
          an independent app with its own container and in-memory store.
        - If ``VARCO_CORS_ORIGINS`` is not set, ``CORSConfig.from_env()``
          allows **no** cross-origin requests (Plan 022 / AB-5 — it defaulted
          to allowing all origins until 3.0.0). Set the env var to enable CORS.

    Thread safety:  ✅ Intended to be called once at startup.
    Async safety:   ✅ Synchronous — no async bootstrap needed.
    """
    container = DIContainer()

    # Scan varco_core first — registers BaseAuthorizer (a @Fallback default that is
    # a binding only under VARCO_SECURITY_ENV=development since 4.0, Plan 053)
    # and other @Singleton defaults (InMemoryEventBus, etc.).
    container.scan("varco_core", recursive=True)

    # Scan varco_fastapi — auto-discovers VarcoFastAPIConfiguration (@Configuration)
    # and all framework @Singleton defaults: TaskRegistry, AbstractJobRunner, etc.
    container.scan("varco_fastapi", recursive=True)

    # ProductModule registers IUoWProvider → InMemoryUoWProvider.
    container.install(ProductModule)

    # Register @Singleton-decorated classes explicitly.
    #
    # DESIGN: explicit register() calls over container.scan(".")
    #   The example lives in a flat directory, not a Python package with an
    #   __init__.py under a scannable parent.  container.scan(".") requires
    #   the directory to be importable as a dotted module name, which is not
    #   guaranteed when pytest runs from the workspace root.  Explicit
    #   register() calls work regardless of the working directory.
    #
    #   In a real app: use container.scan("myapp", recursive=True).
    # Scan each module so providify registers @Singleton classes under their
    # full generic base types (e.g. ProductService → AsyncService[Product, ...]).
    # container.scan() imports the module and collects all @Singleton metadata,
    # which is how the generic injection in VarcoCRUDRouter.__init__ resolves.
    container.scan("assembler")  # discovers ProductAssembler
    container.scan("service")  # discovers ProductService

    # Build the base FastAPI app — middleware, error handling, health endpoint.
    # routers=[] means create_varco_app will NOT try to mount any routers
    # automatically (which would bypass DI injection).
    app = create_varco_app(
        container,
        routers=[],
        title="Minimal CRUD API",
        version="1.0.0",
        description=(
            "Demonstrates VarcoCRUDRouter with CRUD mixins, "
            "DomainModel/DTO/assembler, providify DI, and an "
            "in-memory repository — no database required."
        ),
        validate=False,
    )

    # Resolve the service via DI, then pass it directly to the router.
    #
    # DESIGN: manual service injection over container.get(ProductRouter)
    #   VarcoCRUDRouter uses ``from __future__ import annotations``, which
    #   causes ``get_type_hints()`` to fail on its ``__init__`` signature —
    #   providify cannot resolve ``Inject[AsyncService[D, PK, C, R, U]]``
    #   through the container for a concrete subclass.
    #
    #   Instead, resolve ``ProductService`` directly (registered under the
    #   generic alias ``AsyncService[Product, UUID, ...]`` via ``@Singleton``
    #   + ``container.scan("service")``) and hand it to the router constructor.
    #
    #   ✅ ``_service`` is set correctly — all CRUD endpoints work.
    #   ✅ No ``container.bind()`` bookkeeping required.
    #   ❌ Two extra lines — acceptable for a clear, explicit wiring.
    service = container.get(AsyncService[Product, UUID, ProductCreate, ProductRead, ProductUpdate])
    product_router_instance = ProductRouter(service=service)
    api_router = product_router_instance.build_router()
    app.include_router(api_router)

    return app


# ── Module-level app instance for ``uvicorn app:app`` ─────────────────────────
# DESIGN: module-level app over --factory
#   ✅ Simpler uvicorn command — no ``--factory`` flag needed.
#   ✅ Compatible with tools that import ``app`` directly (e.g. gunicorn).
#   ❌ App is created at import time — tests should call create_app() directly
#      and not import this module-level ``app``.
app = create_app()

__all__ = ["app", "create_app"]
