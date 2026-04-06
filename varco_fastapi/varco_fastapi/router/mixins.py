"""
varco_fastapi.router.mixins
============================
CRUD and streaming route mixins for ``VarcoRouter``.

Each mixin contributes one route to the router by:
1. Setting ``_CRUD_ACTION`` — picked up by ``introspect_routes()``
2. Exposing ClassVars for per-mixin OpenAPI / rate-limit customization
3. Inheriting from ``RouterMixin`` so MRO-based discovery works

Composition pattern::

    class OrderRouter(CreateMixin, ReadMixin, ListMixin, VarcoRouter[Order, UUID, C, R, U]):
        _prefix = "/orders"
        # Override per-mixin ClassVars as needed:
        _create_status_code = 201
        _list_max_limit = 200

CRUD mixins mirror ``ServiceMixin`` from ``varco_core.service`` — they compose
via MRO and each mixin contributes exactly one route.

WebSocketMixin and SSEMixin are opt-in extras; they require ``varco_ws`` to be
installed and ``_event_bus`` to be set on the router.

Thread safety:  ✅ ClassVars are read-only after class definition.
Async safety:   ✅ No I/O at class definition time.
"""

from __future__ import annotations

from typing import Any, Callable, ClassVar

from varco_fastapi.router.base import RouterMixin


# ── CreateMixin ────────────────────────────────────────────────────────────────


class CreateMixin(RouterMixin):
    """
    Adds ``POST /`` → ``201 Created`` + ``ReadDTO`` to a ``VarcoRouter``.

    Delegates to ``service.create(body, auth)``.

    ClassVars (all optional — override to customize OpenAPI / behaviour):
        _create_summary:              OpenAPI summary.
        _create_description:          OpenAPI description (markdown supported).
        _create_response_description: OpenAPI response description.
        _create_responses:            Additional OpenAPI response schemas.
        _create_dependencies:         Extra FastAPI ``Depends()`` callables.
        _create_tags:                 Per-endpoint tags (override router-level tags).
        _create_operation_id:         Custom OpenAPI operation ID.
        _create_deprecated:           Mark endpoint as deprecated.
        _create_include_in_schema:    Include in OpenAPI schema (default: ``True``).
        _create_status_code:          HTTP status code (default: ``201``).
        _create_async_capable:        Allow ``?with_async=true`` (default: ``True``).
        _create_rate_limiter:         Per-endpoint rate limiter instance.
        _create_rate_limit_key_fn:    Key extractor for the rate limiter.

    Usage::

        class OrderRouter(CreateMixin, VarcoRouter[Order, UUID, OrderCreate, OrderRead, OrderUpdate]):
            _prefix = "/orders"
            _create_summary = "Place a new order"
            _create_status_code = 201

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ No I/O at class-definition time.
    """

    # Tells introspect_routes() which CRUD route this mixin contributes
    _CRUD_ACTION: ClassVar[str | None] = "create"

    # ── OpenAPI metadata ──────────────────────────────────────────────────────
    _create_summary: ClassVar[str | None] = None
    _create_description: ClassVar[str | None] = None
    _create_response_description: ClassVar[str] = "Created"
    _create_responses: ClassVar[dict | None] = None
    _create_dependencies: ClassVar[list | None] = None
    _create_tags: ClassVar[list[str] | None] = None
    _create_operation_id: ClassVar[str | None] = None
    _create_deprecated: ClassVar[bool] = False
    _create_include_in_schema: ClassVar[bool] = True

    # ── Behaviour configuration ───────────────────────────────────────────────
    _create_status_code: ClassVar[int] = 201
    _create_async_capable: ClassVar[bool] = True

    # ── Rate limiting (per-router or per-endpoint) ────────────────────────────
    # DESIGN: ClassVar rate limiter over per-request instantiation
    #   ✅ Shared instance accumulates calls — actually limits rate
    #   ❌ Must be set at class definition time (not per-request)
    _create_rate_limiter: ClassVar[Any | None] = None
    _create_rate_limit_key_fn: ClassVar[Callable[..., str] | None] = None

    # ── MCP / A2A metadata ────────────────────────────────────────────────────
    # mcp=True → MCPAdapter exposes POST / as an MCP tool "create_{resource}"
    # skill=True → SkillAdapter exposes it as a Google A2A skill
    # Fine-grained fields override the auto-generated names / descriptions.
    _create_mcp: ClassVar[bool] = False
    _create_mcp_name: ClassVar[str | None] = None
    _create_mcp_description: ClassVar[str | None] = None
    _create_mcp_tags: ClassVar[list[str] | None] = None
    _create_skill: ClassVar[bool] = False
    _create_skill_id: ClassVar[str | None] = None
    _create_skill_name: ClassVar[str | None] = None
    _create_skill_description: ClassVar[str | None] = None
    _create_skill_input_modes: ClassVar[list[str] | None] = None
    _create_skill_output_modes: ClassVar[list[str] | None] = None


# ── ReadMixin ─────────────────────────────────────────────────────────────────


class ReadMixin(RouterMixin):
    """
    Adds ``GET /{id}`` → ``200 OK`` + ``ReadDTO`` to a ``VarcoRouter``.

    Delegates to ``service.read(id, auth)``.

    ClassVars follow the same pattern as ``CreateMixin`` with ``_read_`` prefix.

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ No I/O at class-definition time.
    """

    _CRUD_ACTION: ClassVar[str | None] = "read"

    _read_summary: ClassVar[str | None] = None
    _read_description: ClassVar[str | None] = None
    _read_response_description: ClassVar[str] = "Successful Response"
    _read_responses: ClassVar[dict | None] = None
    _read_dependencies: ClassVar[list | None] = None
    _read_tags: ClassVar[list[str] | None] = None
    _read_operation_id: ClassVar[str | None] = None
    _read_deprecated: ClassVar[bool] = False
    _read_include_in_schema: ClassVar[bool] = True
    _read_status_code: ClassVar[int] = 200
    _read_async_capable: ClassVar[bool] = True
    _read_rate_limiter: ClassVar[Any | None] = None
    _read_rate_limit_key_fn: ClassVar[Callable[..., str] | None] = None

    # ── MCP / A2A metadata ────────────────────────────────────────────────────
    _read_mcp: ClassVar[bool] = False
    _read_mcp_name: ClassVar[str | None] = None
    _read_mcp_description: ClassVar[str | None] = None
    _read_mcp_tags: ClassVar[list[str] | None] = None
    _read_skill: ClassVar[bool] = False
    _read_skill_id: ClassVar[str | None] = None
    _read_skill_name: ClassVar[str | None] = None
    _read_skill_description: ClassVar[str | None] = None
    _read_skill_input_modes: ClassVar[list[str] | None] = None
    _read_skill_output_modes: ClassVar[list[str] | None] = None


# ── UpdateMixin ────────────────────────────────────────────────────────────────


class UpdateMixin(RouterMixin):
    """
    Adds ``PUT /{id}`` → ``200 OK`` + ``ReadDTO`` to a ``VarcoRouter``.

    Full-replacement semantics: all required fields must be present in the body.
    Delegates to ``service.update(id, body, auth)``.

    ClassVars follow the same pattern as ``CreateMixin`` with ``_update_`` prefix.

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ No I/O at class-definition time.
    """

    _CRUD_ACTION: ClassVar[str | None] = "update"

    _update_summary: ClassVar[str | None] = None
    _update_description: ClassVar[str | None] = None
    _update_response_description: ClassVar[str] = "Successful Response"
    _update_responses: ClassVar[dict | None] = None
    _update_dependencies: ClassVar[list | None] = None
    _update_tags: ClassVar[list[str] | None] = None
    _update_operation_id: ClassVar[str | None] = None
    _update_deprecated: ClassVar[bool] = False
    _update_include_in_schema: ClassVar[bool] = True
    _update_status_code: ClassVar[int] = 200
    _update_async_capable: ClassVar[bool] = True
    _update_rate_limiter: ClassVar[Any | None] = None
    _update_rate_limit_key_fn: ClassVar[Callable[..., str] | None] = None

    # ── MCP / A2A metadata ────────────────────────────────────────────────────
    _update_mcp: ClassVar[bool] = False
    _update_mcp_name: ClassVar[str | None] = None
    _update_mcp_description: ClassVar[str | None] = None
    _update_mcp_tags: ClassVar[list[str] | None] = None
    _update_skill: ClassVar[bool] = False
    _update_skill_id: ClassVar[str | None] = None
    _update_skill_name: ClassVar[str | None] = None
    _update_skill_description: ClassVar[str | None] = None
    _update_skill_input_modes: ClassVar[list[str] | None] = None
    _update_skill_output_modes: ClassVar[list[str] | None] = None


# ── PatchMixin ────────────────────────────────────────────────────────────────


class PatchMixin(RouterMixin):
    """
    Adds ``PATCH /{id}`` → ``200 OK`` + ``ReadDTO`` to a ``VarcoRouter``.

    Implements JSON Merge Patch (RFC 7386): present fields are updated, absent
    fields are unchanged.  Delegates to ``service.patch(id, body, auth)``, which
    uses Pydantic's ``model_fields_set`` to distinguish present from absent fields.

    Content-Type: ``application/merge-patch+json`` (also accepts ``application/json``).

    DESIGN: merge-patch over JSON Patch (RFC 6902)
        ✅ Pydantic ``UpdateDTO`` with optional fields maps directly to merge-patch
        ✅ Simpler for clients — just send the fields you want to change
        ✅ Matches ``varco_core service.patch()`` which uses ``model_fields_set``
        ❌ Cannot set a field to ``null`` (null means "omit") — use PUT for that

    ClassVars follow the same pattern as ``CreateMixin`` with ``_patch_`` prefix.

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ No I/O at class-definition time.
    """

    _CRUD_ACTION: ClassVar[str | None] = "patch"

    _patch_summary: ClassVar[str | None] = None
    _patch_description: ClassVar[str | None] = None
    _patch_response_description: ClassVar[str] = "Successful Response"
    _patch_responses: ClassVar[dict | None] = None
    _patch_dependencies: ClassVar[list | None] = None
    _patch_tags: ClassVar[list[str] | None] = None
    _patch_operation_id: ClassVar[str | None] = None
    _patch_deprecated: ClassVar[bool] = False
    _patch_include_in_schema: ClassVar[bool] = True
    _patch_status_code: ClassVar[int] = 200
    _patch_async_capable: ClassVar[bool] = True
    _patch_rate_limiter: ClassVar[Any | None] = None
    _patch_rate_limit_key_fn: ClassVar[Callable[..., str] | None] = None

    # ── MCP / A2A metadata ────────────────────────────────────────────────────
    _patch_mcp: ClassVar[bool] = False
    _patch_mcp_name: ClassVar[str | None] = None
    _patch_mcp_description: ClassVar[str | None] = None
    _patch_mcp_tags: ClassVar[list[str] | None] = None
    _patch_skill: ClassVar[bool] = False
    _patch_skill_id: ClassVar[str | None] = None
    _patch_skill_name: ClassVar[str | None] = None
    _patch_skill_description: ClassVar[str | None] = None
    _patch_skill_input_modes: ClassVar[list[str] | None] = None
    _patch_skill_output_modes: ClassVar[list[str] | None] = None


# ── DeleteMixin ───────────────────────────────────────────────────────────────


class DeleteMixin(RouterMixin):
    """
    Adds ``DELETE /{id}`` → ``204 No Content`` to a ``VarcoRouter``.

    Delegates to ``service.delete(id, auth)``.  Returns no body.

    Note: ``async_capable`` is ``False`` by default for delete — an async delete
    is ambiguous (is the resource gone yet?).  Set ``_delete_async_capable=True``
    explicitly if you want to offload deletes to the job runner.

    ClassVars follow the same pattern as ``CreateMixin`` with ``_delete_`` prefix.

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ No I/O at class-definition time.
    """

    _CRUD_ACTION: ClassVar[str | None] = "delete"

    _delete_summary: ClassVar[str | None] = None
    _delete_description: ClassVar[str | None] = None
    _delete_response_description: ClassVar[str] = "No Content"
    _delete_responses: ClassVar[dict | None] = None
    _delete_dependencies: ClassVar[list | None] = None
    _delete_tags: ClassVar[list[str] | None] = None
    _delete_operation_id: ClassVar[str | None] = None
    _delete_deprecated: ClassVar[bool] = False
    _delete_include_in_schema: ClassVar[bool] = True
    _delete_status_code: ClassVar[int] = 204
    # Async delete is disabled by default — semantics are ambiguous
    _delete_async_capable: ClassVar[bool] = False
    _delete_rate_limiter: ClassVar[Any | None] = None
    _delete_rate_limit_key_fn: ClassVar[Callable[..., str] | None] = None

    # ── MCP / A2A metadata ────────────────────────────────────────────────────
    _delete_mcp: ClassVar[bool] = False
    _delete_mcp_name: ClassVar[str | None] = None
    _delete_mcp_description: ClassVar[str | None] = None
    _delete_mcp_tags: ClassVar[list[str] | None] = None
    _delete_skill: ClassVar[bool] = False
    _delete_skill_id: ClassVar[str | None] = None
    _delete_skill_name: ClassVar[str | None] = None
    _delete_skill_description: ClassVar[str | None] = None
    _delete_skill_input_modes: ClassVar[list[str] | None] = None
    _delete_skill_output_modes: ClassVar[list[str] | None] = None


# ── ListMixin ─────────────────────────────────────────────────────────────────


class ListMixin(RouterMixin):
    """
    Adds ``GET /`` → ``200 OK`` + ``PagedReadDTO[ReadDTO]`` to a ``VarcoRouter``.

    Accepts HTTP query parameters for filtering, sorting, and pagination via
    ``HttpQueryParams``, converted to ``varco_core.query.QueryParams`` via
    ``to_query_params()``.  Delegates to ``service.list(params, auth)``.

    URL query parameters:
        ``?q=status = 'active' AND age > 18``  — filter expression
        ``?sort=+created_at,-name``             — sort directives
        ``?limit=20&offset=40``                 — pagination

    ClassVars:
        _list_max_limit:      Hard cap on ``?limit`` (default: 100).
        _list_default_limit:  Used when ``?limit`` is omitted (default: 50).
        _list_include_total:  Call ``service.count()`` for ``total_count``
                              in ``PagedReadDTO`` (default: True).
        Plus all standard OpenAPI / rate-limit ClassVars with ``_list_`` prefix.

    Usage::

        class OrderRouter(ListMixin, VarcoRouter[Order, UUID, OrderCreate, OrderRead, OrderUpdate]):
            _list_max_limit = 200
            _list_default_limit = 25
            _list_include_total = False  # skip COUNT query for performance

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ No I/O at class-definition time.
    """

    _CRUD_ACTION: ClassVar[str | None] = "list"

    # ── Pagination configuration ───────────────────────────────────────────────
    _list_max_limit: ClassVar[int] = 100
    _list_default_limit: ClassVar[int] = 50
    # Disable to skip COUNT(*) query for performance-sensitive endpoints
    _list_include_total: ClassVar[bool] = True

    # ── OpenAPI metadata ──────────────────────────────────────────────────────
    _list_summary: ClassVar[str | None] = None
    _list_description: ClassVar[str | None] = None
    _list_response_description: ClassVar[str] = "Successful Response"
    _list_responses: ClassVar[dict | None] = None
    _list_dependencies: ClassVar[list | None] = None
    _list_tags: ClassVar[list[str] | None] = None
    _list_operation_id: ClassVar[str | None] = None
    _list_deprecated: ClassVar[bool] = False
    _list_include_in_schema: ClassVar[bool] = True
    _list_status_code: ClassVar[int] = 200
    _list_async_capable: ClassVar[bool] = True
    _list_rate_limiter: ClassVar[Any | None] = None
    _list_rate_limit_key_fn: ClassVar[Callable[..., str] | None] = None

    # ── MCP / A2A metadata ────────────────────────────────────────────────────
    _list_mcp: ClassVar[bool] = False
    _list_mcp_name: ClassVar[str | None] = None
    _list_mcp_description: ClassVar[str | None] = None
    _list_mcp_tags: ClassVar[list[str] | None] = None
    _list_skill: ClassVar[bool] = False
    _list_skill_id: ClassVar[str | None] = None
    _list_skill_name: ClassVar[str | None] = None
    _list_skill_description: ClassVar[str | None] = None
    _list_skill_input_modes: ClassVar[list[str] | None] = None
    _list_skill_output_modes: ClassVar[list[str] | None] = None


# ── WebSocketMixin ────────────────────────────────────────────────────────────


class WebSocketMixin(RouterMixin):
    """
    Adds ``WS /{id}/ws`` WebSocket endpoint to a ``VarcoRouter``.

    Requires ``varco_ws`` package to be installed and ``_event_bus`` to be set
    on the router.

    The default handler streams events for the entity identified by ``{id}``
    via a ``WebSocketEventBus`` subscription filtered by entity ID.
    Override ``_ws_handler()`` to customize behaviour.

    ClassVars:
        _ws_path:        WebSocket path (default: ``"/{id}/ws"``).
        _ws_auth:        Override auth for WS (default: ``WebSocketAuth(_auth)``).
        _ws_event_type:  Filter events by this type (default: all types).
        _ws_channel:     Filter events by channel (default: ``"*"`` = all).
        _ws_summary:     OpenAPI summary.
        _ws_include_in_schema: Include in OpenAPI schema (default: ``True``).

    DESIGN: mixin over manual @ws_route on every router
        ✅ Zero boilerplate for "stream entity updates" pattern
        ✅ Auth handled automatically (WebSocketAuth wraps router's _auth)
        ✅ Override _ws_handler() for custom logic
        ✅ Disable by setting _ws_path = None
        ❌ Only one WS endpoint per mixin — use @ws_route for additional endpoints

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ No I/O at class-definition time.
    """

    _CRUD_ACTION: ClassVar[str | None] = None  # WS is not a CRUD action

    _ws_path: ClassVar[str | None] = "/{id}/ws"
    _ws_auth: ClassVar[Any | None] = None
    _ws_event_type: ClassVar[type | None] = None
    _ws_channel: ClassVar[str] = "*"
    _ws_summary: ClassVar[str | None] = None
    _ws_include_in_schema: ClassVar[bool] = True


# ── SSEMixin ──────────────────────────────────────────────────────────────────


class SSEMixin(RouterMixin):
    """
    Adds ``GET /{id}/events`` SSE (Server-Sent Events) endpoint to a ``VarcoRouter``.

    Requires ``varco_ws`` package to be installed and ``_event_bus`` to be set
    on the router.

    Creates an SSE stream via ``SSEEventBus`` subscription filtered by entity ID
    and optional event type / channel.  Returns a ``StreamingResponse`` with
    ``Content-Type: text/event-stream``.

    ClassVars:
        _sse_path:              SSE path (default: ``"/{id}/events"``).
        _sse_event_type:        Filter events by type (default: all types).
        _sse_channel:           Filter events by channel (default: ``"*"``).
        _sse_rate_limiter:      Per-endpoint rate limiter.
        _sse_rate_limit_key_fn: Key extractor for the rate limiter.
        _sse_summary:           OpenAPI summary.
        _sse_include_in_schema: Include in OpenAPI schema (default: ``True``).

    DESIGN: mixin over manual @sse_route on every router
        ✅ Zero boilerplate for "stream entity events" pattern
        ✅ Uses varco_ws.SSEEventBus — no duplicate SSE implementation
        ✅ Rate limiting supported (prevents SSE connection flood)
        ✅ Disable by setting _sse_path = None
        ❌ Only one SSE endpoint per mixin — use @sse_route for additional endpoints

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ No I/O at class-definition time.
    """

    _CRUD_ACTION: ClassVar[str | None] = None  # SSE is not a CRUD action

    _sse_path: ClassVar[str | None] = "/{id}/events"
    _sse_event_type: ClassVar[type | None] = None
    _sse_channel: ClassVar[str] = "*"
    _sse_rate_limiter: ClassVar[Any | None] = None
    _sse_rate_limit_key_fn: ClassVar[Callable[..., str] | None] = None
    _sse_summary: ClassVar[str | None] = None
    _sse_include_in_schema: ClassVar[bool] = True


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "CreateMixin",
    "ReadMixin",
    "UpdateMixin",
    "PatchMixin",
    "DeleteMixin",
    "ListMixin",
    "WebSocketMixin",
    "SSEMixin",
]
