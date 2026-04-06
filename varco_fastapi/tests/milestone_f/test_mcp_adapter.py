"""
milestone_f / test_mcp_adapter.py
===================================
Tests for ``MCPAdapter`` — MCP (Model Context Protocol) integration.

Covers:
- Tool list generation from ``mcp_enabled`` routes only
- Fine-grained metadata: ``mcp_name``, ``mcp_description``, ``mcp_tags``
- Fallback chain: explicit → summary → description → auto-sentence
- Auto tool name derivation (CRUD vs custom @route)
- Input schema generation: path params, request body model, list params
- ``execute()`` dispatch: CRUD actions and custom routes
- ``execute()`` error: unknown tool, missing client
- ``_resource_name`` suffix stripping
- DI helper: ``bind_mcp_adapter`` is a no-op when providify absent

All tests are pure unit tests — no HTTP server, no real client.
``AsyncVarcoClient`` is replaced with ``AsyncMock`` throughout.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import BaseModel

from varco_fastapi.router.base import VarcoRouter
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.mixins import (
    CreateMixin,
    ListMixin,
    ReadMixin,
)
from varco_fastapi.router.mcp import (
    MCPAdapter,
    MCPToolDefinition,
    _build_input_schema,
    _resolve_description,
    _resource_name,
)
from varco_fastapi.router.introspection import introspect_routes


# ── Test models ───────────────────────────────────────────────────────────────


class OrderCreate(BaseModel):
    name: str
    amount: float


class OrderRead(BaseModel):
    id: UUID
    name: str
    amount: float


class OrderUpdate(BaseModel):
    name: str | None = None
    amount: float | None = None


# ── Fixture routers ───────────────────────────────────────────────────────────


class OrderRouter(CreateMixin, ReadMixin, ListMixin, VarcoRouter):
    """Router with selectively enabled MCP routes and fine-grained metadata."""

    _prefix = "/orders"

    # create is exposed with an explicit description override
    _create_mcp = True
    _create_mcp_description = "Place a new customer order"
    _create_mcp_name = "place_order"
    _create_mcp_tags = ["orders", "write"]

    # read is exposed with only mcp=True (description falls back to summary)
    _read_mcp = True
    _read_summary = "Retrieve an order by ID"

    # list is exposed with no overrides (fully auto-generated)
    _list_mcp = True

    # update is NOT exposed as MCP
    _update_mcp = False

    @route(
        "POST",
        "/{id}/ship",
        mcp=True,
        mcp_name="ship_order",
        mcp_description="Mark an order as shipped",
        mcp_tags=["orders", "fulfillment"],
        summary="Ship order",
    )
    async def ship(self, id: UUID) -> OrderRead: ...

    @route("GET", "/{id}/label", summary="Print shipping label")
    async def label(self, id: UUID) -> dict[str, Any]:
        # mcp=False (default) — should NOT appear in tools
        ...


class MinimalRouter(VarcoRouter):
    """Router with no mcp-enabled routes — tools list should be empty."""

    _prefix = "/noop"

    @route("GET", "/ping")
    async def ping(self) -> dict: ...


# ── _resource_name ─────────────────────────────────────────────────────────────


def test_resource_name_strips_router_suffix():
    """``OrderRouter`` → ``'order'``."""
    assert _resource_name(OrderRouter) == "order"


def test_resource_name_strips_controller_suffix():
    class UserController(VarcoRouter):
        _prefix = "/users"

    assert _resource_name(UserController) == "user"


def test_resource_name_no_recognised_suffix():
    class ProductView(VarcoRouter):
        _prefix = "/products"

    assert _resource_name(ProductView) == "product_view"


def test_resource_name_bare_router_class_falls_back():
    """Class literally named 'Router' → fallback 'resource'."""

    class Router(VarcoRouter):
        _prefix = "/x"

    assert _resource_name(Router) == "resource"


# ── _resolve_description ───────────────────────────────────────────────────────


def test_resolve_description_explicit_wins():
    """Explicit override takes priority over all fallbacks."""
    assert _resolve_description("explicit", "summary", "desc", "auto") == "explicit"


def test_resolve_description_falls_back_to_summary():
    assert _resolve_description(None, "summary", "desc", "auto") == "summary"


def test_resolve_description_falls_back_to_description():
    assert _resolve_description(None, None, "desc", "auto") == "desc"


def test_resolve_description_falls_back_to_auto():
    assert _resolve_description(None, None, None, "auto") == "auto"


# ── tool list generation ───────────────────────────────────────────────────────


def test_tools_only_contain_mcp_enabled_routes():
    """Only routes with ``mcp_enabled=True`` appear in the tool list."""
    adapter = MCPAdapter(OrderRouter)
    names = {t.name for t in adapter.tools}
    # create (as 'place_order'), read (auto), list (auto), ship (custom)
    assert "place_order" in names  # _create_mcp_name override
    assert "read_order" in names
    assert "list_order" in names
    assert "ship_order" in names
    # update and label must NOT appear
    assert not any("update" in n for n in names)
    assert "label_order" not in names
    assert "label" not in names


def test_tools_empty_when_no_mcp_routes():
    """Adapter with no mcp_enabled routes → empty tools list."""
    adapter = MCPAdapter(MinimalRouter)
    assert adapter.tools == []


def test_tool_definition_is_frozen_dataclass():
    """MCPToolDefinition must be immutable (frozen dataclass)."""
    adapter = MCPAdapter(OrderRouter)
    tool = adapter.tools[0]
    assert isinstance(tool, MCPToolDefinition)
    with pytest.raises((AttributeError, TypeError)):
        tool.name = "mutated"  # type: ignore[misc]


def test_tool_count_matches_mcp_enabled_routes():
    """Exactly 4 tools: create (place_order), read, list, ship."""
    adapter = MCPAdapter(OrderRouter)
    assert len(adapter.tools) == 4


# ── fine-grained metadata ─────────────────────────────────────────────────────


def test_explicit_mcp_name_used():
    """``_create_mcp_name = 'place_order'`` overrides the auto-name."""
    adapter = MCPAdapter(OrderRouter)
    create_tool = next(t for t in adapter.tools if "place" in t.name)
    assert create_tool.name == "place_order"


def test_auto_name_for_crud_route():
    """``ReadMixin`` with no override → ``'read_order'``."""
    adapter = MCPAdapter(OrderRouter)
    read_tool = next(t for t in adapter.tools if t.name == "read_order")
    assert read_tool is not None


def test_explicit_mcp_description_used():
    """``_create_mcp_description`` overrides the fallback chain."""
    adapter = MCPAdapter(OrderRouter)
    create_tool = next(t for t in adapter.tools if t.name == "place_order")
    assert create_tool.description == "Place a new customer order"


def test_description_falls_back_to_summary():
    """``ReadMixin`` has no ``mcp_description`` but has ``summary`` → uses summary."""
    adapter = MCPAdapter(OrderRouter)
    read_tool = next(t for t in adapter.tools if t.name == "read_order")
    assert read_tool.description == "Retrieve an order by ID"


def test_description_auto_generated_for_list():
    """``ListMixin`` has no overrides → auto-sentence is used."""
    adapter = MCPAdapter(OrderRouter)
    list_tool = next(t for t in adapter.tools if t.name == "list_order")
    assert "order" in list_tool.description.lower()


def test_mcp_tags_stored_on_tool():
    """``_create_mcp_tags`` is forwarded to the tool definition."""
    adapter = MCPAdapter(OrderRouter)
    create_tool = next(t for t in adapter.tools if t.name == "place_order")
    assert "orders" in create_tool.tags
    assert "write" in create_tool.tags


def test_custom_route_mcp_name_used():
    """``@route(..., mcp_name='ship_order')`` sets the tool name directly."""
    adapter = MCPAdapter(OrderRouter)
    ship_tool = next(t for t in adapter.tools if t.name == "ship_order")
    assert ship_tool.description == "Mark an order as shipped"
    assert "fulfillment" in ship_tool.tags


def test_tool_name_prefix_prepended():
    """``tool_name_prefix='store_'`` prepends to every auto-generated tool name."""
    adapter = MCPAdapter(OrderRouter, tool_name_prefix="store_")
    names = {t.name for t in adapter.tools}
    # mcp_name override also gets prefix
    assert "store_place_order" in names
    assert "store_read_order" in names


def test_enabled_routes_filters_tools():
    """``enabled_routes={'read'}`` restricts the tool list to that route only."""
    adapter = MCPAdapter(OrderRouter, enabled_routes={"read"})
    assert len(adapter.tools) == 1
    assert adapter.tools[0].name == "read_order"


# ── input schema ──────────────────────────────────────────────────────────────


def test_create_schema_includes_body_fields():
    """Create route schema must include Pydantic model fields."""
    _ = introspect_routes(OrderRouter)
    # Attach request_model manually for schema test (introspect_routes needs type_args)
    from varco_fastapi.router.introspection import ResolvedRoute

    route_with_model = ResolvedRoute(
        name="create",
        method="POST",
        path="",
        request_model=OrderCreate,
        is_crud=True,
        crud_action="create",
        mcp_enabled=True,
    )
    schema = _build_input_schema(route_with_model)
    assert "name" in schema["properties"]
    assert "amount" in schema["properties"]
    assert "name" in schema.get("required", [])
    assert "amount" in schema.get("required", [])


def test_read_schema_includes_path_param():
    """Read route with ``/{id}`` → schema must have ``id`` in properties."""
    from varco_fastapi.router.introspection import ResolvedRoute

    route_read = ResolvedRoute(
        name="read",
        method="GET",
        path="/{id}",
        path_params=("id",),
        is_crud=True,
        crud_action="read",
        mcp_enabled=True,
    )
    schema = _build_input_schema(route_read)
    assert "id" in schema["properties"]
    assert "id" in schema["required"]


def test_list_schema_includes_pagination_params():
    """List routes must include ``q``, ``sort``, ``limit``, ``offset``."""
    from varco_fastapi.router.introspection import ResolvedRoute

    route_list = ResolvedRoute(
        name="list",
        method="GET",
        path="",
        is_crud=True,
        crud_action="list",
        mcp_enabled=True,
    )
    schema = _build_input_schema(route_list)
    for param in ("q", "sort", "limit", "offset"):
        assert param in schema["properties"], f"Missing pagination param: {param}"


def test_delete_schema_has_only_path_param():
    """Delete route has no body — only ``id`` path param."""
    from varco_fastapi.router.introspection import ResolvedRoute

    route_delete = ResolvedRoute(
        name="delete",
        method="DELETE",
        path="/{id}",
        path_params=("id",),
        is_crud=True,
        crud_action="delete",
        mcp_enabled=True,
    )
    schema = _build_input_schema(route_delete)
    assert "id" in schema["properties"]
    # Should not have list params on delete
    assert "q" not in schema["properties"]


# ── execute() dispatch ────────────────────────────────────────────────────────


async def test_execute_create_calls_client_create():
    """``execute('place_order', {...})`` → ``client.create(body)``."""
    mock_client = MagicMock()
    mock_client.create = AsyncMock(return_value={"id": "123"})
    adapter = MCPAdapter(OrderRouter, client=mock_client)

    result = await adapter.execute("place_order", {"name": "Widget", "amount": 9.99})

    mock_client.create.assert_called_once_with({"name": "Widget", "amount": 9.99})
    assert result == {"id": "123"}


async def test_execute_read_calls_client_read():
    """``execute('read_order', {'id': 'abc'})`` → ``client.read('abc')``."""
    mock_client = MagicMock()
    mock_client.read = AsyncMock(return_value={"id": "abc"})
    adapter = MCPAdapter(OrderRouter, client=mock_client)

    _ = await adapter.execute("read_order", {"id": "abc"})

    mock_client.read.assert_called_once_with("abc")


async def test_execute_list_calls_client_list_with_params():
    """``execute('list_order', {q=..., limit=...})`` → ``client.list(...)``."""
    mock_client = MagicMock()
    mock_client.list = AsyncMock(return_value=[])
    adapter = MCPAdapter(OrderRouter, client=mock_client)

    await adapter.execute(
        "list_order", {"q": "status=active", "limit": 10, "offset": 0}
    )

    mock_client.list.assert_called_once_with(
        q="status=active", sort=None, limit=10, offset=0
    )


async def test_execute_custom_route_calls_client_request():
    """Custom ``@route`` method → ``client.request(method, path, json=body)``."""
    mock_client = MagicMock()
    mock_client.request = AsyncMock(return_value={"shipped": True})
    adapter = MCPAdapter(OrderRouter, client=mock_client)

    _ = await adapter.execute("ship_order", {"id": "order-1"})

    mock_client.request.assert_called_once()
    call_kwargs = mock_client.request.call_args
    # method should be POST, path should include the resolved id
    assert call_kwargs[1]["method"] == "POST" or call_kwargs[0][0] == "POST"


async def test_execute_unknown_tool_raises_value_error():
    """Calling an unregistered tool name raises ``ValueError`` with helpful message."""
    mock_client = MagicMock()
    adapter = MCPAdapter(OrderRouter, client=mock_client)

    with pytest.raises(ValueError, match="Unknown MCP tool"):
        await adapter.execute("nonexistent_tool", {})


async def test_execute_without_client_raises_runtime_error():
    """Adapter constructed without a client → ``RuntimeError`` on execute."""
    adapter = MCPAdapter(OrderRouter)  # no client=

    with pytest.raises(RuntimeError, match="no client"):
        await adapter.execute("place_order", {"name": "X", "amount": 1.0})


# ── router_class property ─────────────────────────────────────────────────────


def test_router_class_property():
    """``adapter.router_class`` returns the class passed at construction."""
    adapter = MCPAdapter(OrderRouter)
    assert adapter.router_class is OrderRouter


# ── repr ──────────────────────────────────────────────────────────────────────


def test_repr_includes_router_name():
    adapter = MCPAdapter(OrderRouter)
    assert "OrderRouter" in repr(adapter)
    assert "4" in repr(adapter)  # 4 tools


# ── bind_mcp_adapter without providify ───────────────────────────────────────


def test_bind_mcp_adapter_noop_without_providify():
    """``bind_mcp_adapter`` returns silently (does not raise) when providify is absent."""
    import sys  # noqa: PLC0415

    mock_container = MagicMock()
    orig = sys.modules.pop("providify", None)
    try:
        # Should not raise — bind_mcp_adapter is a no-op without providify
        from varco_fastapi.router.mcp import bind_mcp_adapter as _bind  # noqa: PLC0415

        _bind(mock_container, OrderRouter)  # must not raise
    finally:
        if orig is not None:
            sys.modules["providify"] = orig
