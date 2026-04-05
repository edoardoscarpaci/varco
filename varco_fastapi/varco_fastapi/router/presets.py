"""
varco_fastapi.router.presets
=============================
Pre-composed ``VarcoRouter`` subclasses for common CRUD combinations.

These are convenience base classes — subclass them and set ``_prefix``,
``_service``, and ``_auth`` to get a fully-wired ``APIRouter`` with zero
boilerplate.

Available presets:

    ``CRUDRouter``      — All six HTTP CRUD endpoints (POST, GET /, GET /{id}, PUT, PATCH, DELETE)
    ``ReadOnlyRouter``  — Read-only (GET / + GET /{id})
    ``WriteRouter``     — Write-only (POST, PUT, PATCH, DELETE — no read/list)
    ``NoDeleteRouter``  — Everything except DELETE
    ``AllRouteMixin``   — Standalone mixin combining all six CRUD mixins (no VarcoRouter base)

Usage::

    from varco_fastapi.router.presets import CRUDRouter
    from varco_core.auth import AuthContext

    class OrderRouter(CRUDRouter[Order, UUID, OrderCreate, OrderRead, OrderUpdate]):
        _prefix = "/orders"
        _tags = ["orders"]
        _service = container.get(OrderService)
        _auth = container.get(JwtBearerAuth)

    app.include_router(OrderRouter().build_router())

For WebSocket or SSE, compose with the opt-in mixins separately::

    from varco_fastapi.router.mixins import SSEMixin

    class OrderRouter(AllRouteMixin, SSEMixin, VarcoRouter[Order, UUID, C, R, U]):
        _prefix = "/orders"
        # Adds GET /orders/{id}/events SSE stream alongside CRUD

DESIGN: pre-composed subclasses over manual MRO stacking
    ✅ Zero boilerplate for the 4 common CRUD patterns
    ✅ WS/SSE explicitly opt-in — no varco_ws dependency unless explicitly added
    ✅ AllRouteMixin available for custom VarcoRouter compositions
    ❌ Users must still subclass (can't use directly without _service/_auth)

Thread safety:  ✅ All ClassVars are read-only after class definition.
Async safety:   ✅ No I/O at class-definition time.
"""

from __future__ import annotations

from typing import TypeVar

from varco_fastapi.router.crud import VarcoCRUDRouter
from varco_fastapi.router.mixins import (
    CreateMixin,
    DeleteMixin,
    ListMixin,
    PatchMixin,
    ReadMixin,
    UpdateMixin,
)

# Generic type parameters — mirrors VarcoRouter[D, PK, C, R, U]
D = TypeVar("D")
PK = TypeVar("PK")
C = TypeVar("C")
R = TypeVar("R")
U = TypeVar("U")


# ── AllRouteMixin ─────────────────────────────────────────────────────────────


class AllRouteMixin(
    CreateMixin,
    ListMixin,
    ReadMixin,
    UpdateMixin,
    PatchMixin,
    DeleteMixin,
):
    """
    Standalone mixin combining all six HTTP CRUD endpoints.

    Use with a custom ``VarcoRouter`` composition when you want full CRUD plus
    additional mixins (e.g. ``SSEMixin``, ``WebSocketMixin``)::

        class OrderRouter(AllRouteMixin, SSEMixin, VarcoRouter[Order, UUID, C, R, U]):
            _prefix = "/orders"
            # Adds 6 CRUD routes + GET /orders/{id}/events SSE stream

    For WebSocket/SSE opt-in without a custom base, subclass ``CRUDRouter`` and
    add the streaming mixin explicitly::

        class OrderRouter(SSEMixin, CRUDRouter[Order, UUID, C, R, U]):
            _prefix = "/orders"

    DESIGN: CRUD-only mixin, WS/SSE opt-in separately
        ✅ No varco_ws dependency unless WS/SSE mixins are explicitly included
        ✅ Individual routes still customizable via per-mixin ClassVars
        ✅ Composable with any extra ``RouterMixin``
        ❌ One extra line for routers that need WS/SSE — acceptable trade-off

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ No I/O at class-definition time.
    """


# ── CRUDRouter ────────────────────────────────────────────────────────────────


class CRUDRouter(
    AllRouteMixin,
    VarcoCRUDRouter[D, PK, C, R, U],  # type: ignore[misc]
):
    """
    Pre-composed router with all six HTTP CRUD endpoints.

    Provides:
        POST   /{prefix}/           → 201 + ReadDTO
        GET    /{prefix}/           → 200 + PagedReadDTO[ReadDTO]
        GET    /{prefix}/{id}       → 200 + ReadDTO
        PUT    /{prefix}/{id}       → 200 + ReadDTO
        PATCH  /{prefix}/{id}       → 200 + ReadDTO  (JSON Merge Patch, RFC 7386)
        DELETE /{prefix}/{id}       → 204 No Content

    Required ClassVars::

        _prefix = "/orders"
        _service = container.get(OrderService)  # AsyncService[Order, UUID, C, R, U]
        _auth = container.get(JwtBearerAuth)    # AbstractServerAuth

    Optional ClassVars (inherited from each mixin)::

        _create_status_code = 201
        _list_max_limit = 200
        _delete_include_in_schema = False  # hide DELETE from OpenAPI

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ build_router() is synchronous.
    """


# ── ReadOnlyRouter ────────────────────────────────────────────────────────────


class ReadOnlyRouter(
    ListMixin,
    ReadMixin,
    VarcoCRUDRouter[D, PK, C, R, U],  # type: ignore[misc]
):
    """
    Pre-composed router with read-only access (GET / + GET /{id}).

    Provides:
        GET    /{prefix}/           → 200 + PagedReadDTO[ReadDTO]
        GET    /{prefix}/{id}       → 200 + ReadDTO

    Suitable for public-facing catalog or reference data endpoints where
    writes should be blocked at the router level.

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ build_router() is synchronous.
    """


# ── WriteRouter ───────────────────────────────────────────────────────────────


class WriteRouter(
    CreateMixin,
    UpdateMixin,
    PatchMixin,
    DeleteMixin,
    VarcoCRUDRouter[D, PK, C, R, U],  # type: ignore[misc]
):
    """
    Pre-composed router with write-only access (POST, PUT, PATCH, DELETE).

    Provides:
        POST   /{prefix}/           → 201 + ReadDTO
        PUT    /{prefix}/{id}       → 200 + ReadDTO
        PATCH  /{prefix}/{id}       → 200 + ReadDTO
        DELETE /{prefix}/{id}       → 204 No Content

    Does NOT include GET endpoints.  Useful for command-side routers in
    CQRS architectures where reads and writes are served by different services.

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ build_router() is synchronous.
    """


# ── NoDeleteRouter ────────────────────────────────────────────────────────────


class NoDeleteRouter(
    CreateMixin,
    ListMixin,
    ReadMixin,
    UpdateMixin,
    PatchMixin,
    VarcoCRUDRouter[D, PK, C, R, U],  # type: ignore[misc]
):
    """
    Pre-composed router with all CRUD endpoints except DELETE.

    Provides:
        POST   /{prefix}/           → 201 + ReadDTO
        GET    /{prefix}/           → 200 + PagedReadDTO[ReadDTO]
        GET    /{prefix}/{id}       → 200 + ReadDTO
        PUT    /{prefix}/{id}       → 200 + ReadDTO
        PATCH  /{prefix}/{id}       → 200 + ReadDTO

    Suitable for resources that support soft-delete (mark as deleted)
    rather than hard-delete (remove from DB).  Combine with a
    ``_delete_include_in_schema = False`` override on a ``CRUDRouter``
    subclass to hide the endpoint from OpenAPI while still allowing
    internal delete operations.

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ build_router() is synchronous.
    """


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "AllRouteMixin",
    "CRUDRouter",
    "ReadOnlyRouter",
    "WriteRouter",
    "NoDeleteRouter",
]
