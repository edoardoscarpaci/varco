"""
Milestone B tests — router layer.

Tests cover:
- HttpQueryParams.to_query_params() — filter parsing, sort parsing, limit capping
- AsyncModeParams — default and custom values
- _parse_sort_string — sort directive parsing
- VarcoRouter.build_router() — route registration, prefix, CRUD mixins
- RouterMixin discovery — _CRUD_ACTION ClassVar
- Route ordering — literals before parameterized paths
- CRUD handler factories — create, read, update, patch, delete, list
"""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from varco_fastapi.router.base import (
    AsyncModeParams,
    HttpQueryParams,
    RouterMixin,
    VarcoRouter,
    _parse_sort_string,
)
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.introspection import introspect_routes
from varco_fastapi.router.mixins import (
    CreateMixin,
    DeleteMixin,
    ListMixin,
    PatchMixin,
    ReadMixin,
    UpdateMixin,
)
from varco_fastapi.router.presets import (
    AllRouteMixin,
    CRUDRouter,
    NoDeleteRouter,
    ReadOnlyRouter,
    WriteRouter,
)
from varco_core.query import SortOrder


# ── Test models ───────────────────────────────────────────────────────────────


class ItemCreate(BaseModel):
    name: str


class ItemRead(BaseModel):
    id: UUID
    name: str


class ItemUpdate(BaseModel):
    name: str | None = None


# ── _parse_sort_string ────────────────────────────────────────────────────────


def test_parse_sort_string_asc_prefix():
    """'+created_at' → SortField(created_at, ASC)."""
    result = _parse_sort_string("+created_at")
    assert len(result) == 1
    assert result[0].field == "created_at"
    assert result[0].order == SortOrder.ASC


def test_parse_sort_string_desc_prefix():
    """'-name' → SortField(name, DESC)."""
    result = _parse_sort_string("-name")
    assert len(result) == 1
    assert result[0].field == "name"
    assert result[0].order == SortOrder.DESC


def test_parse_sort_string_no_prefix_defaults_asc():
    """'status' → SortField(status, ASC) — no prefix defaults to ascending."""
    result = _parse_sort_string("status")
    assert result[0].order == SortOrder.ASC


def test_parse_sort_string_multiple():
    """'+created_at,-name,status' → three fields."""
    result = _parse_sort_string("+created_at,-name,status")
    assert len(result) == 3
    assert result[0].field == "created_at"
    assert result[1].field == "name"
    assert result[1].order == SortOrder.DESC
    assert result[2].field == "status"


def test_parse_sort_string_empty():
    """Empty string → empty list."""
    assert _parse_sort_string("") == []


def test_parse_sort_string_trailing_comma():
    """'name,' → one field (trailing comma skipped)."""
    result = _parse_sort_string("name,")
    assert len(result) == 1
    assert result[0].field == "name"


# ── HttpQueryParams.to_query_params ──────────────────────────────────────────


def test_http_query_params_no_filter():
    """to_query_params() with q=None → node is None."""
    params = HttpQueryParams()
    qp = params.to_query_params()
    assert qp.node is None


def test_http_query_params_with_filter():
    """to_query_params() with a valid q string → parsed AST node."""
    # lark grammar expects SIGNED_NUMBER or ESCAPED_STRING on the right side
    params = HttpQueryParams(q='name = "alice"')
    qp = params.to_query_params()
    assert qp.node is not None


def test_http_query_params_sort_parsed():
    """to_query_params() with sort string → SortField list."""
    params = HttpQueryParams(sort="+created_at,-name")
    qp = params.to_query_params()
    assert len(qp.sort) == 2
    assert qp.sort[0].field == "created_at"
    assert qp.sort[1].field == "name"
    assert qp.sort[1].order == SortOrder.DESC


def test_http_query_params_limit_cap():
    """to_query_params() caps limit at max_limit."""
    params = HttpQueryParams(limit=9999)
    qp = params.to_query_params(max_limit=100)
    assert qp.limit == 100


def test_http_query_params_default_limit():
    """to_query_params() uses default_limit when limit is None."""
    params = HttpQueryParams()
    qp = params.to_query_params(default_limit=25)
    assert qp.limit == 25


def test_http_query_params_offset_defaults_zero():
    """to_query_params() defaults offset to 0 when None."""
    params = HttpQueryParams()
    qp = params.to_query_params()
    assert qp.offset == 0


def test_http_query_params_offset_explicit():
    """to_query_params() passes through explicit offset."""
    params = HttpQueryParams(offset=40)
    qp = params.to_query_params()
    assert qp.offset == 40


# ── AsyncModeParams ───────────────────────────────────────────────────────────


def test_async_mode_params_defaults():
    """AsyncModeParams defaults to with_async=False, callback_url=None."""
    params = AsyncModeParams()
    assert params.with_async is False
    assert params.callback_url is None


def test_async_mode_params_custom():
    """AsyncModeParams accepts custom values."""
    params = AsyncModeParams(with_async=True, callback_url="https://example.com/cb")
    assert params.with_async is True
    assert params.callback_url == "https://example.com/cb"


def test_async_mode_params_frozen():
    """AsyncModeParams is a frozen dataclass — mutation raises."""
    params = AsyncModeParams()
    with pytest.raises((AttributeError, TypeError)):
        params.with_async = True  # type: ignore[misc]


# ── RouterMixin ───────────────────────────────────────────────────────────────


def test_router_mixin_is_abc():
    """RouterMixin is abstract — cannot be instantiated directly."""
    from abc import ABC

    assert issubclass(RouterMixin, ABC)


def test_crud_mixin_has_action():
    """Each CRUD mixin declares the correct _CRUD_ACTION ClassVar."""
    assert CreateMixin._CRUD_ACTION == "create"
    assert ReadMixin._CRUD_ACTION == "read"
    assert UpdateMixin._CRUD_ACTION == "update"
    assert PatchMixin._CRUD_ACTION == "patch"
    assert DeleteMixin._CRUD_ACTION == "delete"
    assert ListMixin._CRUD_ACTION == "list"


# ── VarcoRouter.build_router ──────────────────────────────────────────────────


class _MockService:
    """Minimal service stub that records calls."""

    async def create(self, body: Any, auth: Any = None) -> Any:
        return {"id": str(uuid4()), "name": body.name}

    async def get(self, id: Any, auth: Any = None) -> Any:
        # Renamed from read() to match AsyncService.get() in varco_core
        return {"id": str(id), "name": "item"}

    async def update(self, id: Any, body: Any, auth: Any = None) -> Any:
        return {"id": str(id), "name": body.name}

    async def patch(self, id: Any, body: Any, auth: Any = None) -> Any:
        return {"id": str(id), "name": body.name or "patched"}

    async def delete(self, id: Any, auth: Any = None) -> None:
        pass

    async def list(self, params: Any = None, auth: Any = None) -> list[Any]:
        return []

    async def count(self, params: Any = None, auth: Any = None) -> int:
        return 0


class ItemRouter(
    AllRouteMixin, VarcoRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]
):
    _prefix = "/items"
    _tags = ["items"]
    _service = _MockService()


def test_build_router_returns_api_router():
    """build_router() returns a FastAPI APIRouter."""
    from fastapi.routing import APIRouter

    router = ItemRouter().build_router()
    assert isinstance(router, APIRouter)


def test_build_router_prefix():
    """build_router() uses _prefix for route prefix."""
    router = ItemRouter().build_router()
    assert router.prefix == "/items"


def test_build_router_six_crud_routes():
    """AllRouteMixin router has 6 CRUD routes."""
    router = ItemRouter().build_router()
    # Routes: POST /, GET /, GET /{id}, PUT /{id}, PATCH /{id}, DELETE /{id}
    assert len(router.routes) == 6


def test_build_router_version_prefix():
    """_version prepends a versioned prefix."""

    class V2ItemRouter(ItemRouter):
        _version = "v2"

    router = V2ItemRouter().build_router()
    assert router.prefix == "/v2/items"


def test_read_only_router_has_two_routes():
    """ReadOnlyRouter exposes only GET / and GET /{id}."""

    class MyReadOnly(ReadOnlyRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]):  # type: ignore[misc]
        _service = _MockService()

    router = MyReadOnly().build_router()
    # GET / + GET /{id} = 2 routes
    assert len(router.routes) == 2


def test_no_delete_router_has_five_routes():
    """NoDeleteRouter has 5 routes (no DELETE)."""

    class MyNoDelete(NoDeleteRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]):  # type: ignore[misc]
        _service = _MockService()

    router = MyNoDelete().build_router()
    assert len(router.routes) == 5


# ── Route ordering ────────────────────────────────────────────────────────────


class OrderedRouter(
    CreateMixin,
    ReadMixin,
    ListMixin,
    VarcoRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate],
):
    _prefix = "/ordered"
    _service = _MockService()

    @route("GET", "/summary", route_order=0)
    async def get_summary(self) -> dict:
        return {}


def test_literal_routes_before_parameterized():
    """Literal paths (GET /summary) are registered before parameterized (GET /{id})."""
    routes = introspect_routes(OrderedRouter)
    paths = [r.path for r in routes]
    # /summary must appear before /{id}
    assert paths.index("/summary") < paths.index("/{id}")


# ── Integration — CRUD endpoints respond correctly ────────────────────────────


def _make_test_client(router_cls: type) -> TestClient:
    """Build a FastAPI test client from a router class."""
    app = FastAPI()
    api_router = router_cls().build_router()
    app.include_router(api_router)
    return TestClient(app)


def test_crud_post_create():
    """POST /items/ returns 201 with item data."""
    client = _make_test_client(ItemRouter)
    resp = client.post("/items/", json={"name": "widget"})
    assert resp.status_code == 201


def test_crud_get_read():
    """GET /items/{id} returns 200."""
    client = _make_test_client(ItemRouter)
    item_id = str(uuid4())
    resp = client.get(f"/items/{item_id}")
    assert resp.status_code == 200


def test_crud_get_list():
    """GET /items/ returns 200."""
    client = _make_test_client(ItemRouter)
    resp = client.get("/items/")
    assert resp.status_code == 200


def test_crud_put_update():
    """PUT /items/{id} returns 200."""
    client = _make_test_client(ItemRouter)
    item_id = str(uuid4())
    resp = client.put(f"/items/{item_id}", json={"name": "updated"})
    assert resp.status_code == 200


def test_crud_patch():
    """PATCH /items/{id} returns 200."""
    client = _make_test_client(ItemRouter)
    item_id = str(uuid4())
    resp = client.patch(f"/items/{item_id}", json={"name": "patched"})
    assert resp.status_code == 200


def test_crud_delete():
    """DELETE /items/{id} returns 204."""
    client = _make_test_client(ItemRouter)
    item_id = str(uuid4())
    resp = client.delete(f"/items/{item_id}")
    assert resp.status_code == 204


# ── Custom @route endpoints ────────────────────────────────────────────────────


class CustomRouter(VarcoRouter[Any, UUID, ItemCreate, ItemRead, ItemUpdate]):
    _prefix = "/custom"
    _service = _MockService()

    @route("GET", "/hello", status_code=200)
    async def hello(self) -> dict:
        return {"message": "hello"}


def test_custom_route_registered():
    """@route decorator registers a custom endpoint."""
    client = _make_test_client(CustomRouter)
    resp = client.get("/custom/hello")
    assert resp.status_code == 200
    assert resp.json() == {"message": "hello"}


# ── Presets sanity check ──────────────────────────────────────────────────────


def test_all_route_mixin_combines_six_mixins():
    """AllRouteMixin inherits from all six CRUD mixins."""
    for mixin in (
        CreateMixin,
        ReadMixin,
        UpdateMixin,
        PatchMixin,
        DeleteMixin,
        ListMixin,
    ):
        assert issubclass(
            AllRouteMixin, mixin
        ), f"AllRouteMixin missing {mixin.__name__}"


def test_crud_router_inherits_all_route_mixin():
    """CRUDRouter inherits AllRouteMixin."""
    assert issubclass(CRUDRouter, AllRouteMixin)


def test_write_router_has_no_read():
    """WriteRouter does not inherit ReadMixin or ListMixin."""
    assert not issubclass(WriteRouter, ReadMixin)
    assert not issubclass(WriteRouter, ListMixin)
