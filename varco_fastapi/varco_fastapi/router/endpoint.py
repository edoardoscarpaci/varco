"""
varco_fastapi.router.endpoint
==============================
Decorators for custom endpoints on ``VarcoRouter`` subclasses.

``@route``     — HTTP endpoint (GET, POST, PUT, PATCH, DELETE, etc.)
``@ws_route``  — WebSocket endpoint
``@sse_route`` — Server-Sent Events endpoint

Each decorator stores metadata on the function object at class-definition time.
Routes are materialized into FastAPI route registrations only when
``VarcoRouter.build_router()`` is called — this decouples route declaration from
route registration, enabling testing without FastAPI initialization.

This is the exact pattern used by ``@listen`` / ``_ListenEntry`` in
``varco_core.event.consumer``:

- ``@listen`` stores ``_ListenEntry`` on ``func.__listen_entries__``
- ``@route`` stores ``_RouteEntry`` on ``func.__route_entry__``
- Subscriptions/routes are created at wire-time (``register_to()`` / ``build_router()``)

Rate limiting:
    When ``rate_limiter`` is provided, ``build_router()`` wraps the endpoint
    closure with the rate limiter.  ``RateLimitExceededError`` from
    ``varco_core.resilience`` maps to HTTP 429 with a ``Retry-After`` header.

Thread safety:  ✅ Decorators only set function attributes — no shared state.
Async safety:   ✅ No I/O at decoration time.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, TYPE_CHECKING

if TYPE_CHECKING:
    pass


# ── _RouteEntry ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _RouteEntry:
    """
    Metadata for a custom HTTP endpoint on a ``VarcoRouter``.

    Stored on ``func.__route_entry__`` by the ``@route`` decorator.
    Read by ``build_router()`` and ``introspect_routes()`` to materialize the route.

    Attributes:
        method:            HTTP method (``"GET"``, ``"POST"``, etc.).
        path:              URL path relative to the router prefix
                           (e.g. ``"/{order_id}/summary"``).
        status_code:       Default HTTP status code for successful responses.
        async_capable:     If ``True``, the endpoint supports ``?with_async=true``
                           to offload execution to a background job.
        route_order:       Explicit sort order (``None`` = auto-sort by path type).
                           Lower values are registered first.  Default: ``None``.
        rate_limiter:      Optional rate limiter instance (from varco_core.resilience).
        rate_limit_key_fn: Callable ``(request, auth) → str`` for rate limit key.
        summary:           OpenAPI summary string.
        description:       OpenAPI description (markdown supported).
        response_description: OpenAPI response description.
        responses:         Additional response schemas for OpenAPI docs.
        dependencies:      Extra FastAPI ``Depends()`` callables.
        tags:              OpenAPI tags (override router-level tags).
        operation_id:      Custom OpenAPI operation ID.
        deprecated:        Mark this endpoint as deprecated in OpenAPI.
        include_in_schema: Whether to include in the OpenAPI schema.

    Thread safety:  ✅ frozen=True — immutable after construction.
    Async safety:   ✅ Pure metadata; no I/O.
    """

    method: str
    path: str
    status_code: int = 200
    async_capable: bool = True
    route_order: int | None = None
    rate_limiter: Any | None = None
    rate_limit_key_fn: Callable[..., str] | None = None
    summary: str | None = None
    description: str | None = None
    response_description: str = "Successful Response"
    responses: dict | None = None
    dependencies: list | None = None
    tags: list[str] | None = None
    operation_id: str | None = None
    deprecated: bool = False
    include_in_schema: bool = True

    # ── MCP (Model Context Protocol) metadata ─────────────────────────────────
    # When mcp=True, MCPAdapter exposes this route as an MCP tool.
    # Fine-grained fields override the auto-generated values so callers can
    # tune the LLM-facing interface without touching the HTTP/OpenAPI docs.
    mcp: bool = False
    # Override the auto-generated tool name (default: crud_action_resource or method_name)
    mcp_name: str | None = None
    # Override tool description shown to the LLM (fallback: summary → description → auto)
    mcp_description: str | None = None
    # Arbitrary tags passed through to the MCP tool definition for LLM context
    mcp_tags: list[str] | None = None

    # ── A2A / Google Skill metadata ───────────────────────────────────────────
    # When skill=True, SkillAdapter exposes this route as a Google A2A skill.
    skill: bool = False
    # Override skill ID in the Agent Card (default: crud_action_resource or method_name)
    skill_id: str | None = None
    # Override skill display name (default: title-case of skill_id)
    skill_name: str | None = None
    # Override skill description in the Agent Card (fallback: summary → description → auto)
    skill_description: str | None = None
    # MIME types the skill accepts.  Default: ["application/json"]
    skill_input_modes: list[str] | None = None
    # MIME types the skill returns.  Default: ["application/json"]
    skill_output_modes: list[str] | None = None


# ── @route decorator ───────────────────────────────────────────────────────────


def route(
    method: str,
    path: str,
    *,
    status_code: int = 200,
    async_capable: bool = True,
    route_order: int | None = None,
    rate_limiter: Any | None = None,
    rate_limit_key_fn: Callable[..., str] | None = None,
    summary: str | None = None,
    description: str | None = None,
    response_description: str = "Successful Response",
    responses: dict | None = None,
    dependencies: list | None = None,
    tags: list[str] | None = None,
    operation_id: str | None = None,
    deprecated: bool = False,
    include_in_schema: bool = True,
    # ── MCP (Model Context Protocol) ─────────────────────────────────────────
    mcp: bool = False,
    mcp_name: str | None = None,
    mcp_description: str | None = None,
    mcp_tags: list[str] | None = None,
    # ── A2A / Google Skill ────────────────────────────────────────────────────
    skill: bool = False,
    skill_id: str | None = None,
    skill_name: str | None = None,
    skill_description: str | None = None,
    skill_input_modes: list[str] | None = None,
    skill_output_modes: list[str] | None = None,
) -> Callable:
    """
    Decorator to declare a custom HTTP endpoint on a ``VarcoRouter`` subclass.

    Stores a ``_RouteEntry`` on ``func.__route_entry__`` at class-definition time.
    The route is materialized by ``build_router()`` / ``introspect_routes()``.

    Args:
        method:            HTTP method (e.g. ``"GET"``, ``"POST"``).
        path:              URL path relative to the router prefix
                           (e.g. ``"/{order_id}/summary"``).
        status_code:       HTTP status code for successful responses.  Default: 200.
        async_capable:     Allow ``?with_async=true`` to offload to a background job.
        route_order:       Explicit registration order (``None`` = auto-sort).
        rate_limiter:      Per-route rate limiter from ``varco_core.resilience``.
        rate_limit_key_fn: ``(request, auth) → str`` key extractor for the limiter.
        summary:           OpenAPI summary.
        description:       OpenAPI description (markdown supported).
        response_description: OpenAPI response description.
        responses:         Additional OpenAPI response schemas.
        dependencies:      Extra FastAPI ``Depends()`` callables.
        tags:              OpenAPI tags (override router-level tags).
        operation_id:      Custom OpenAPI operation ID.
        deprecated:           Mark as deprecated in OpenAPI docs.
        include_in_schema:    Whether to include in the OpenAPI schema.
        mcp:                  If ``True``, expose this route as an MCP tool via
                              ``MCPAdapter``.  Defaults to ``False``.
        mcp_name:             Override the auto-generated MCP tool name.
                              Default: ``"{crud_action}_{resource}"`` or method name.
        mcp_description:      Override the description shown to the LLM.
                              Fallback chain: ``mcp_description → summary →
                              description → auto-sentence``.
        mcp_tags:             Arbitrary tags forwarded to the MCP tool definition
                              for LLM context (e.g. ``["orders", "write"]``).
        skill:                If ``True``, expose this route as a Google A2A skill
                              via ``SkillAdapter``.  Defaults to ``False``.
        skill_id:             Override the skill ID in the Agent Card.
                              Default: same as ``mcp_name`` derivation.
        skill_name:           Override the skill display name in the Agent Card.
                              Default: title-case of ``skill_id``.
        skill_description:    Override the skill description in the Agent Card.
                              Fallback chain: ``skill_description → summary →
                              description → auto-sentence``.
        skill_input_modes:    MIME types the skill accepts.
                              Default: ``["application/json"]``.
        skill_output_modes:   MIME types the skill returns.
                              Default: ``["application/json"]``.

    Returns:
        The decorated function (unchanged).

    Usage::

        class OrderRouter(VarcoRouter[...]):
            @route("GET", "/{order_id}/summary", summary="Order summary")
            async def get_summary(self, order_id: UUID, ctx: AuthContext) -> OrderSummary:
                return await self._service.get_summary(order_id, ctx)

    Edge cases:
        - The decorated method receives both path parameters (positional, from
          FastAPI URL pattern matching) and an injected ``AuthContext`` argument.
        - ``rate_limiter`` must be a SHARED instance per use case — creating one
          per router subclass is fine; creating one per request is not.
        - Applying ``@route`` to a non-async function will be caught by
          ``build_router()`` at registration time.

    Thread safety:  ✅ Sets a function attribute — safe at class-definition time.
    Async safety:   ✅ No I/O.
    """

    def decorator(func: Callable) -> Callable:
        entry = _RouteEntry(
            method=method.upper(),
            path=path,
            status_code=status_code,
            async_capable=async_capable,
            route_order=route_order,
            rate_limiter=rate_limiter,
            rate_limit_key_fn=rate_limit_key_fn,
            summary=summary,
            description=description,
            response_description=response_description,
            responses=responses,
            dependencies=dependencies,
            tags=tags,
            operation_id=operation_id,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            # MCP fields — stored as-is; MCPAdapter resolves the fallback chain
            mcp=mcp,
            mcp_name=mcp_name,
            mcp_description=mcp_description,
            mcp_tags=mcp_tags,
            # Skill fields — stored as-is; SkillAdapter resolves the fallback chain
            skill=skill,
            skill_id=skill_id,
            skill_name=skill_name,
            skill_description=skill_description,
            skill_input_modes=skill_input_modes,
            skill_output_modes=skill_output_modes,
        )
        func.__route_entry__ = entry
        return func

    return decorator


# ── _WSRouteEntry ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _WSRouteEntry:
    """
    Metadata for a WebSocket endpoint on a ``VarcoRouter``.

    Stored on ``func.__ws_route_entry__`` by the ``@ws_route`` decorator.

    Attributes:
        path:          URL path relative to the router prefix.
        route_order:   Explicit sort order (``None`` = auto-sort).
        auth:          Override the router-level auth strategy for this endpoint.
                       If ``None``, the router's ``_auth`` is used.
        summary:       OpenAPI summary.
        tags:          OpenAPI tags.
        include_in_schema: Whether to include in OpenAPI.
    """

    path: str
    route_order: int | None = None
    auth: Any | None = None
    summary: str | None = None
    tags: list[str] | None = None
    include_in_schema: bool = True


def ws_route(
    path: str,
    *,
    route_order: int | None = None,
    auth: Any | None = None,
    summary: str | None = None,
    tags: list[str] | None = None,
    include_in_schema: bool = True,
) -> Callable:
    """
    Decorator to declare a WebSocket endpoint on a ``VarcoRouter`` subclass.

    Stores a ``_WSRouteEntry`` on ``func.__ws_route_entry__``.

    The decorated method receives:
        - ``websocket: WebSocket`` — the FastAPI WebSocket object
        - ``ctx: AuthContext`` — verified identity (from ``WebSocketAuth`` or router ``_auth``)
        - ``**path_params`` — path parameters from the URL pattern

    The method is responsible for the accept/receive/send loop.

    Args:
        path:              URL path relative to the router prefix.
        route_order:       Explicit registration order.
        auth:              Override auth strategy for this WS endpoint.
        summary:           OpenAPI summary.
        tags:              OpenAPI tags.
        include_in_schema: Whether to include in OpenAPI.

    Usage::

        @ws_route("/{order_id}/ws", auth=WebSocketAuth)
        async def stream_order(self, websocket: WebSocket, order_id: UUID, ctx: AuthContext):
            await websocket.accept()
            while True:
                data = await websocket.receive_text()
                await websocket.send_text(f"echo: {data}")
    """

    def decorator(func: Callable) -> Callable:
        func.__ws_route_entry__ = _WSRouteEntry(
            path=path,
            route_order=route_order,
            auth=auth,
            summary=summary,
            tags=tags,
            include_in_schema=include_in_schema,
        )
        return func

    return decorator


# ── _SSERouteEntry ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _SSERouteEntry:
    """
    Metadata for an SSE (Server-Sent Events) endpoint on a ``VarcoRouter``.

    Stored on ``func.__sse_route_entry__`` by the ``@sse_route`` decorator.

    Attributes:
        path:              URL path relative to the router prefix.
        route_order:       Explicit sort order (``None`` = auto-sort).
        event_type:        Filter SSE stream by event type (from varco_core.event).
        channel:           Filter SSE stream by channel.  Default: ``"*"`` (all).
        rate_limiter:      Per-endpoint rate limiter.
        rate_limit_key_fn: Key extractor for the rate limiter.
        summary:           OpenAPI summary.
        tags:              OpenAPI tags.
        include_in_schema: Whether to include in OpenAPI.
    """

    path: str
    route_order: int | None = None
    event_type: type | None = None
    channel: str = "*"
    rate_limiter: Any | None = None
    rate_limit_key_fn: Callable[..., str] | None = None
    summary: str | None = None
    tags: list[str] | None = None
    include_in_schema: bool = True


def sse_route(
    path: str,
    *,
    route_order: int | None = None,
    event_type: type | None = None,
    channel: str = "*",
    rate_limiter: Any | None = None,
    rate_limit_key_fn: Callable[..., str] | None = None,
    summary: str | None = None,
    tags: list[str] | None = None,
    include_in_schema: bool = True,
) -> Callable:
    """
    Decorator to declare an SSE endpoint on a ``VarcoRouter`` subclass.

    Stores a ``_SSERouteEntry`` on ``func.__sse_route_entry__``.

    Two usage modes:

    1. **Auto-streaming** (``event_type`` is set): The decorator wires an
       ``SSEEventBus`` subscription filtered by ``event_type`` and ``channel``.
       The handler body is NOT called — events are streamed automatically.

    2. **Manual streaming** (``event_type`` is ``None``): The decorated method
       is an async generator that yields SSE event strings.  ``build_router()``
       wraps it in a ``StreamingResponse`` with ``text/event-stream``.

    Args:
        path:              URL path relative to the router prefix.
        route_order:       Explicit registration order.
        event_type:        Filter events by this type (auto-streaming mode).
        channel:           Filter events by this channel.  Default: ``"*"``.
        rate_limiter:      Per-endpoint rate limiter.
        rate_limit_key_fn: Key extractor for the rate limiter.
        summary:           OpenAPI summary.
        tags:              OpenAPI tags.
        include_in_schema: Whether to include in OpenAPI.
    """

    def decorator(func: Callable) -> Callable:
        func.__sse_route_entry__ = _SSERouteEntry(
            path=path,
            route_order=route_order,
            event_type=event_type,
            channel=channel,
            rate_limiter=rate_limiter,
            rate_limit_key_fn=rate_limit_key_fn,
            summary=summary,
            tags=tags,
            include_in_schema=include_in_schema,
        )
        return func

    return decorator


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "_RouteEntry",
    "_WSRouteEntry",
    "_SSERouteEntry",
    "route",
    "ws_route",
    "sse_route",
]
