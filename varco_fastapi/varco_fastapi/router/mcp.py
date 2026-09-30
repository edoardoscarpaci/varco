"""
varco_fastapi.router.mcp
=========================
``MCPAdapter`` — convert any ``VarcoRouter`` into an MCP (Model Context Protocol) server.

The adapter reads ``ResolvedRoute`` metadata via ``introspect_routes()`` and exposes
every route flagged with ``mcp_enabled=True`` as an MCP tool.  Execution is delegated
to ``AsyncVarcoClient``, which calls the already-registered HTTP handlers — no handler
logic is duplicated.

Typical usage::

    from varco_fastapi.router.mcp import MCPAdapter
    from myapp.routers import OrderRouter
    from myapp.clients import OrderClient

    # Build adapter — no FastAPI app needed at this point
    adapter = MCPAdapter(OrderRouter, client=OrderClient(base_url="http://localhost:8080"))

    # Option A: mount as HTTP+SSE endpoint on an existing FastAPI app
    adapter.mount(app)   # adds GET {path}/sse + POST {path}/messages/

    # Option B: run as standalone stdio MCP server (for local LLMs)
    server = adapter.to_mcp_server()   # an mcp.server.Server (v2)
    # run it over a transport, e.g.:
    #   from mcp.server.stdio import stdio_server
    #   async with stdio_server() as (read, write):
    #       await server.run(read, write, server.create_initialization_options())

DI registration::

    # MCPAdapter is a @Singleton — injectable via Inject[MCPAdapter]
    # Register via bind_mcp_adapter() after container setup:
    from varco_fastapi.router.mcp import bind_mcp_adapter
    bind_mcp_adapter(container, OrderRouter, client_cls=OrderClient)

DESIGN: adapter delegates to AsyncVarcoClient over direct service calls
    ✅ All auth, rate limiting, middleware, and response serialisation pass
       through the existing HTTP stack — no duplicated logic
    ✅ Adapters are protocol translators only — the implementation lives once
    ✅ Client can target a remote service, not just localhost
    ❌ One extra HTTP hop vs. calling the service directly — acceptable for
       agentic workloads where latency is dominated by the LLM round-trip

DESIGN: mcp SDK import deferred to mount() / to_mcp_server()
    ✅ MCPAdapter is constructible and usable (tools, execute()) without the
       optional [mcp] extra — unit tests need not install the SDK
    ✅ ImportError is raised only when the user tries to actually run the server,
       surfacing the "pip install varco-fastapi[mcp]" message at the right time
    ❌ Late ImportError vs. early startup validation — mitigated by clear message

Thread safety:  ✅ MCPAdapter is constructed once and read-only after construction.
Async safety:   ✅ execute() is async; tools is a pure property with no I/O.
"""

from __future__ import annotations

import json as _json
import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from fastapi import HTTPException

from varco_fastapi.router.introspection import ResolvedRoute, introspect_routes

if TYPE_CHECKING:
    from fastapi import FastAPI
    from mcp.types import Tool as _MCPTool
    from providify import DIContainer

    from varco_fastapi.auth.server_auth import AbstractServerAuth
    from varco_fastapi.client.base import AsyncVarcoClient

_logger = logging.getLogger(__name__)

# ── Default MIME modes ─────────────────────────────────────────────────────────

_DEFAULT_INPUT_MODES: tuple[str, ...] = ("application/json",)
_DEFAULT_OUTPUT_MODES: tuple[str, ...] = ("application/json",)

# ── Helper: resource name derivation ──────────────────────────────────────────


def _resource_name(router_cls: type) -> str:
    """
    Derive a snake_case resource name from a router class name.

    Strips common suffixes (``Router``, ``Controller``, ``View``) and
    converts CamelCase to snake_case.

    Args:
        router_cls: The ``VarcoRouter`` subclass.

    Returns:
        Lower-case snake_case resource name (e.g. ``"order"`` for
        ``OrderRouter``).

    Edge cases:
        - Class named exactly ``"Router"`` → returns ``"resource"`` as fallback
        - No recognised suffix → whole class name is snake_cased
    """
    name = router_cls.__name__
    # Bare "Router" or "Controller" → reserved word; fall back to sentinel
    if name in ("Router", "Controller"):
        return "resource"
    # Strip common suffix pairs (Router, Controller only — View/Handler are kept)
    for suffix in ("Router", "Controller"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    # CamelCase → snake_case
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    return snake or "resource"


def _auto_tool_name(route: ResolvedRoute, resource: str, prefix: str) -> str:
    """
    Generate an MCP tool name from a route when no explicit override is set.

    Convention:
        - CRUD routes: ``{prefix}{crud_action}_{resource}``
          e.g. ``create_order``, ``list_order``
        - Custom ``@route`` methods: ``{prefix}{method_name}``
          e.g. ``ship_order``

    Args:
        route:    The ``ResolvedRoute`` to name.
        resource: Snake-case resource name derived from the router class.
        prefix:   Optional prefix string (e.g. ``"store_"``).

    Returns:
        MCP-compatible tool name string (lowercase, underscores only).
    """
    if route.is_crud and route.crud_action:
        base = f"{route.crud_action}_{resource}"
    else:
        base = route.name
    return f"{prefix}{base}"


def _resolve_description(
    mcp_desc: str | None, summary: str | None, description: str | None, auto: str
) -> str:
    """
    Apply the MCP description fallback chain.

    Priority: explicit ``mcp_description`` → OpenAPI ``summary`` →
    OpenAPI ``description`` → auto-generated sentence.

    Args:
        mcp_desc:    Explicit override from ``_*_mcp_description`` or ``@route(mcp_description=...)``.
        summary:     OpenAPI summary.
        description: OpenAPI description.
        auto:        Auto-generated fallback sentence.

    Returns:
        Resolved description string (never empty).
    """
    return mcp_desc or summary or description or auto


# ── MCPToolDefinition ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class MCPToolDefinition:
    """
    Immutable descriptor for a single MCP tool derived from a ``ResolvedRoute``.

    Consumed by ``MCPAdapter.tools`` and forwarded to the ``mcp`` SDK.

    Attributes:
        name:         MCP tool name (unique within the adapter).
        description:  Human-readable description shown to the LLM.
        input_schema: JSON Schema dict for tool arguments.
        tags:         Arbitrary tags forwarded to the MCP tool for LLM context.
        route:        The source ``ResolvedRoute`` for traceability.

    Thread safety:  ✅ frozen=True — immutable.
    Async safety:   ✅ Pure value object.
    """

    name: str
    description: str
    input_schema: dict[str, Any]
    tags: tuple[str, ...]
    route: ResolvedRoute


# ── Input schema builder ───────────────────────────────────────────────────────


def _build_input_schema(route: ResolvedRoute) -> dict[str, Any]:
    """
    Build a JSON Schema dict for a route's tool arguments.

    Schema sources (merged in priority order):
    1. Path parameters → ``{"type": "string"}`` for each ``{param}`` in the path.
    2. Request body model → ``model.model_json_schema()["properties"]`` if available.
    3. List routes → standard pagination / filter query params added.

    Args:
        route: The ``ResolvedRoute`` to build a schema for.

    Returns:
        JSON Schema ``{"type": "object", "properties": {...}, "required": [...]}``.

    Edge cases:
        - No path params and no request model → returns empty ``{}`` properties.
        - DELETE routes have no body — only path params are included.
        - ``model_json_schema()`` may include ``$defs`` — we include them as-is
          so the LLM can resolve ``$ref`` pointers.
    """
    properties: dict[str, Any] = {}
    required: list[str] = []

    # ── 1. Path parameters ────────────────────────────────────────────────────
    for param in route.path_params:
        properties[param] = {
            "type": "string",
            "description": f"Path parameter: {param}",
        }
        required.append(param)

    # ── 2. Request body model ─────────────────────────────────────────────────
    defs: dict[str, Any] = {}
    if route.request_model is not None:
        try:
            body_schema = route.request_model.model_json_schema()  # type: ignore[attr-defined]
            # model_json_schema() may return $defs for nested models
            defs = body_schema.pop("$defs", {})
            body_props = body_schema.get("properties", {})
            body_required = body_schema.get("required", [])
            # Merge body properties — path params already present take precedence
            for key, val in body_props.items():
                if key not in properties:
                    properties[key] = val
            # Add body required fields (excluding path params already listed)
            for field in body_required:
                if field not in required:
                    required.append(field)
        except Exception:  # noqa: BLE001
            # model_json_schema() can raise for complex generics — skip gracefully
            _logger.debug("MCPAdapter: could not build schema for %s", route.request_model)

    # ── 3. List-specific pagination/filter params ─────────────────────────────
    if route.crud_action == "list":
        properties["q"] = {
            "type": "string",
            "description": "Filter expression (e.g. 'status = active AND age > 18')",
        }
        properties["sort"] = {
            "type": "string",
            "description": "Sort directives (e.g. '+created_at,-name')",
        }
        properties["limit"] = {
            "type": "integer",
            "description": "Max results to return",
            "default": 50,
        }
        properties["offset"] = {
            "type": "integer",
            "description": "Number of results to skip",
            "default": 0,
        }

    schema: dict[str, Any] = {"type": "object", "properties": properties}
    if required:
        schema["required"] = required
    if defs:
        # Include $defs so LLMs that resolve $ref pointers can follow them
        schema["$defs"] = defs
    return schema


# ── mcp SDK Tool builder (§KI-11-testability, Plan 020) ────────────────────────


def _to_mcp_tools(tools: Sequence[MCPToolDefinition]) -> list[_MCPTool]:
    """
    Build one ``mcp.types.Tool`` per ``MCPToolDefinition``, carrying varco's
    own JSON Schema dict verbatim.

    This is the fix for BACKLOG KI-11: the high-level ``FastMCP.add_tool()``
    has never accepted an ``input_schema=`` parameter (SDK issue #761, open
    since May 2025) — it derives the schema from the handler's type hints,
    which is fundamentally incompatible with varco's generic ``**kwargs``
    dispatch shim. ``mcp.types.Tool`` accepts a plain dict for its schema
    field with no synthesis or post-processing required (research brief 003
    §finding 2), so this function is a pure, SDK-internals-free mapping.

    Args:
        tools: The adapter's ``MCPToolDefinition`` sequence (``adapter.tools``).

    Returns:
        One ``mcp.types.Tool`` per input definition, in the same order.

    Edge cases:
        - Empty ``tools`` → empty list. Never raises.

    Async safety: N/A — pure, synchronous, no I/O.
    """
    from mcp.types import Tool as MCPTool  # noqa: PLC0415 — deferred SDK import (mcp.py:42)

    return [
        MCPTool(
            name=tool_def.name,
            description=tool_def.description,
            # mcp v1.x's `Tool` model spelled this field `inputSchema`
            # (camelCase) — verified against the resolved SDK's
            # `mcp/types.py` per Plan 020 Step 26. mcp v2's `Tool` renamed
            # the Python-facing attribute to `input_schema` (snake_case),
            # keeping `inputSchema` only as the wire/serialization alias —
            # both spellings still construct an identical object at
            # runtime (pydantic's `populate_by_name`), but mypy resolves
            # the constructor against the real field name, so this uses
            # `input_schema=` for the v2 migration (Plan 029 / N1a, Step 19)
            # rather than carrying a mypy suppression for a spelling with
            # no remaining reason to prefer it.
            input_schema=tool_def.input_schema,
        )
        for tool_def in tools
    ]


# ── MCPAuthMiddleware ──────────────────────────────────────────────────────────


async def _send_http_error(send: Any, status_code: int, detail: str) -> None:
    """
    Send a minimal JSON HTTP error response over the ASGI ``send`` channel.

    Args:
        send:        ASGI send callable.
        status_code: HTTP status code (e.g. 401, 403, 500).
        detail:      Human-readable error detail string.

    Async safety:   ✅ Sends two ASGI messages and returns.
    """
    body = _json.dumps({"detail": detail}).encode("utf-8")
    await send(
        {
            "type": "http.response.start",
            "status": status_code,
            "headers": [
                (b"content-type", b"application/json"),
                (b"content-length", str(len(body)).encode()),
            ],
        }
    )
    await send({"type": "http.response.body", "body": body, "more_body": False})


class MCPAuthMiddleware:
    """
    Pure-ASGI middleware that protects a mounted MCP endpoint with an auth strategy.

    Intercepts requests whose path starts with ``mount_path`` and applies an
    ``AbstractServerAuth`` check before forwarding them to the wrapped ASGI app.
    Requests to other paths pass through unchanged.

    This middleware is independent of the main ``RequestContextMiddleware`` —
    MCP clients can authenticate with a different credential strategy (e.g. an
    API key) while the rest of the application uses JWT Bearer tokens.

    Usage::

        app = FastAPI()
        auth = ApiKeyAuth(keys={"secret-key": AuthContext(user_id="mcp-client")})
        adapter = MCPAdapter(OrderRouter, client=...)
        adapter.mount(app, server_auth=auth)   # convenience — adds middleware + mounts

        # Or add the middleware manually:
        app.add_middleware(MCPAuthMiddleware, server_auth=auth, mount_path="/mcp")
        adapter.mount(app)

    Args:
        app:         The ASGI application to wrap.
        server_auth: ``AbstractServerAuth`` instance for credential verification.
                     Any ``AbstractServerAuth`` subclass is accepted — ``ApiKeyAuth``,
                     ``JwtBearerAuth``, ``CompositeServerAuth``, etc.
        mount_path:  Only requests whose path starts with this prefix are auth-checked.
                     Default: ``"/mcp"``.

    DESIGN: pure ASGI over ``BaseHTTPMiddleware``
        ✅ Works correctly with SSE streaming responses used by MCP — ``BaseHTTPMiddleware``
           buffers the full response body, which breaks streaming.
        ✅ Zero additional dependencies beyond ``fastapi`` / ``starlette``.
        ❌ Slightly more boilerplate than ``BaseHTTPMiddleware``.

    Thread safety:  ✅ Stateless per-call; ``server_auth`` must be thread-safe.
    Async safety:   ✅ All methods are ``async def``.

    Edge cases:
        - Requests to paths outside ``mount_path`` always pass through (never auth-checked).
        - ``HTTPException`` from ``server_auth`` is forwarded as a JSON HTTP response.
        - Unexpected non-HTTP exceptions from ``server_auth`` are logged and
          return a 500 response — they must never propagate to the ASGI server.
        - Lifespan scope and other non-http/non-websocket scopes pass through unchanged.
    """

    def __init__(
        self,
        app: Any,
        *,
        server_auth: AbstractServerAuth,
        mount_path: str = "/mcp",
    ) -> None:
        """
        Args:
            app:         ASGI application to wrap.
            server_auth: Auth strategy; applied only to requests under ``mount_path``.
            mount_path:  URL path prefix to protect.  Default: ``"/mcp"``.
        """
        self._app = app
        self._server_auth = server_auth
        # Normalize: strip trailing slash so "/mcp" and "/mcp/" are equivalent
        self._mount_path = mount_path.rstrip("/") or "/"

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        """
        ASGI entry point.

        Auth is applied only to ``http`` and ``websocket`` connection scopes
        whose path starts with ``mount_path``.  All other scopes (lifespan,
        etc.) pass through unconditionally.

        Args:
            scope:   ASGI connection scope dict.
            receive: ASGI receive channel.
            send:    ASGI send channel.
        """
        # Non-HTTP scopes (lifespan, etc.) always pass through
        if scope.get("type") not in ("http", "websocket"):
            await self._app(scope, receive, send)
            return

        path: str = scope.get("path", "")
        if not path.startswith(self._mount_path):
            # Outside the protected path — pass through unchanged
            await self._app(scope, receive, send)
            return

        # Import inside the method to keep module-level imports minimal and
        # to avoid any potential circular import issues at startup.
        from fastapi import Request  # noqa: PLC0415

        request = Request(scope, receive)
        try:
            await self._server_auth(request)
        except HTTPException as exc:
            # Auth strategy rejected the request — return the appropriate error
            await _send_http_error(send, exc.status_code, str(exc.detail))
            return
        except Exception as exc:  # noqa: BLE001
            # Unexpected error in the auth strategy itself — return 500 and log
            _logger.error(
                "MCPAuthMiddleware: unexpected auth error on %s: %s",
                path,
                exc,
                exc_info=True,
            )
            await _send_http_error(send, 500, "Internal authentication error")
            return

        # Auth passed — forward to the wrapped app unchanged
        await self._app(scope, receive, send)

    def __repr__(self) -> str:
        return (
            f"MCPAuthMiddleware(server_auth={self._server_auth!r}, mount_path={self._mount_path!r})"
        )


# ── MCPAdapter ─────────────────────────────────────────────────────────────────


class MCPAdapter:
    """
    Converts a ``VarcoRouter`` class into an MCP server.

    Reads ``ResolvedRoute`` metadata via ``introspect_routes()`` to build tool
    definitions.  Delegates tool execution to ``AsyncVarcoClient`` so no
    handler logic is duplicated.

    Typical usage — mount on FastAPI::

        adapter = MCPAdapter(OrderRouter, client=OrderClient(base_url="http://localhost:8080"))
        adapter.mount(app)   # registers GET {path}/sse + POST {path}/messages/

    Typical usage — stdio transport (local LLM)::

        server = adapter.to_mcp_server()   # an mcp.server.Server (v2)
        # run it over a transport, e.g. mcp.server.stdio.stdio_server() +
        # server.run(read, write, server.create_initialization_options())

    DI-friendly usage::

        bind_mcp_adapter(container, OrderRouter, client_cls=OrderClient)
        # Now injectable: Inject[MCPAdapter]

    Args:
        router_cls:      The ``VarcoRouter`` subclass to expose as MCP tools.
        client:          ``AsyncVarcoClient`` instance used for tool execution.
                         If ``None``, only ``tools`` / schema generation works;
                         ``execute()`` will raise ``RuntimeError``.
        base_url:        Convenience shortcut — if ``client`` is ``None`` and
                         ``base_url`` is provided, a bare ``AsyncVarcoClient``
                         is constructed with this URL.  For production use,
                         pass a fully configured client instead.
        tool_name_prefix: Optional prefix prepended to every tool name
                          (e.g. ``"store_"`` → ``"store_create_order"``).
        enabled_routes:  Explicit allowlist of route names to include.
                         ``None`` means include all ``mcp_enabled`` routes.
        ttl_ms:          Plan 029 / N1a, §D-N1-parity. Milliseconds the MCP
                         client MAY cache a ``tools/list`` response for,
                         emitted as ``ListToolsResult.ttl_ms`` (wire key
                         ``ttlMs``) by ``to_mcp_server()``. Default
                         ``60_000`` — varco's tool list never changes after
                         construction, so a full minute of client-side
                         caching is safe. ``0`` (the SDK's own default)
                         would mean "immediately stale," defeating the
                         point of advertising a TTL at all.

    Thread safety:  ✅ Read-only after construction — safe to share across tasks.
    Async safety:   ✅ ``execute()`` is async; ``tools`` has no I/O.
    """

    def __init__(
        self,
        router_cls: type,
        *,
        client: AsyncVarcoClient[Any] | None = None,
        base_url: str | None = None,
        tool_name_prefix: str = "",
        enabled_routes: set[str] | None = None,
        ttl_ms: int = 60_000,
    ) -> None:
        self._router_cls = router_cls
        self._prefix = tool_name_prefix
        self._resource = _resource_name(router_cls)
        self._ttl_ms = ttl_ms

        # Resolve client — prefer explicit instance, then build from base_url
        self._client = client
        if self._client is None and base_url is not None:
            # Lazy import to avoid circular dependency at module level
            from varco_fastapi.client.base import (
                AsyncVarcoClient as _Client,
            )  # noqa: PLC0415

            # Bare client with no auth / middleware — suitable for internal calls
            # DESIGN: bare client for convenience; callers should inject a configured
            # client for production (with JwtClientAuth, tracing, etc.)
            self._client = _Client(base_url=base_url)

        # Pre-compute tool list at construction time — routes don't change
        # after class definition so this is safe and avoids re-introspecting
        # on every access to .tools.
        all_routes = introspect_routes(router_cls)
        self._tools: list[MCPToolDefinition] = []
        for route in all_routes:
            # Skip routes not flagged for MCP exposure
            if not route.mcp_enabled:
                continue
            # Respect caller-supplied allowlist
            if enabled_routes is not None and route.name not in enabled_routes:
                continue
            # Apply prefix to explicit mcp_name overrides too, so tool_name_prefix is consistent
            if route.mcp_name:
                tool_name = f"{self._prefix}{route.mcp_name}"
            else:
                tool_name = _auto_tool_name(route, self._resource, self._prefix)
            auto_desc = (
                f"Perform the '{route.crud_action or route.name}' operation on {self._resource}."
            )
            description = _resolve_description(
                route.mcp_description,
                route.summary,
                route.description,
                auto_desc,
            )
            self._tools.append(
                MCPToolDefinition(
                    name=tool_name,
                    description=description,
                    input_schema=_build_input_schema(route),
                    tags=route.mcp_tags,
                    route=route,
                )
            )
        # Build a lookup from tool name → tool definition for O(1) dispatch
        self._tool_by_name: dict[str, MCPToolDefinition] = {t.name: t for t in self._tools}

    # ── Public read-only properties ────────────────────────────────────────────

    @property
    def tools(self) -> list[MCPToolDefinition]:
        """
        All MCP tool definitions derived from ``mcp_enabled`` routes.

        Returns:
            Immutable list of ``MCPToolDefinition`` objects in route-declaration order.

        Thread safety:  ✅ Returns reference to pre-computed list — no mutation.
        """
        return self._tools

    @property
    def router_class(self) -> type:
        """The ``VarcoRouter`` class this adapter was built from."""
        return self._router_cls

    # ── Tool execution ─────────────────────────────────────────────────────────

    async def execute(self, tool_name: str, arguments: dict[str, Any]) -> Any:
        """
        Dispatch an MCP tool call to the underlying ``AsyncVarcoClient``.

        Splits ``arguments`` into path parameters and a body dict, then calls
        the appropriate client method (``create``, ``read``, ``update``, etc.).

        Args:
            tool_name:  Name of the MCP tool to invoke (must match a tool in
                        ``self.tools``).
            arguments:  Key-value arguments from the LLM tool call.

        Returns:
            JSON-serialisable result from the client method.

        Raises:
            ValueError:    ``tool_name`` is not registered in this adapter.
            RuntimeError:  Adapter was constructed without a ``client``.

        Edge cases:
            - Unknown tool name → ``ValueError`` with list of known tools.
            - Missing required path params in ``arguments`` → ``KeyError`` from
              the client method (propagated as-is for the LLM to retry).
            - List arguments include ``q``, ``sort``, ``limit``, ``offset`` —
              these are passed as kwargs to the client's list method.

        Async safety:   ✅ Delegates to ``AsyncVarcoClient`` which is async-safe.
        """
        tool = self._tool_by_name.get(tool_name)
        if tool is None:
            known = list(self._tool_by_name.keys())
            raise ValueError(
                f"Unknown MCP tool '{tool_name}'. "
                f"Available tools: {known}. "
                "Did you forget to set mcp=True on the route?"
            )
        if self._client is None:
            raise RuntimeError(
                "MCPAdapter has no client — pass client= or base_url= at construction."
            )

        route = tool.route
        # Extract path parameters from arguments so they're not sent as body fields
        path_params = {p: arguments.pop(p) for p in route.path_params if p in arguments}

        return await self._dispatch(route, path_params, arguments)

    async def _dispatch(
        self,
        route: ResolvedRoute,
        path_params: dict[str, Any],
        body: dict[str, Any],
    ) -> Any:
        """
        Route a tool call to the correct ``AsyncVarcoClient`` method.

        Args:
            route:       The matched ``ResolvedRoute``.
            path_params: Extracted ``{param}`` values from the tool arguments.
            body:        Remaining arguments (request body / query params).

        Returns:
            Client method result (Pydantic model or dict).

        Edge cases:
            - Custom ``@route`` methods → called via ``client.request()``.
            - WS/SSE routes are never MCP-enabled (filtered at construction).
        """
        action = route.crud_action
        client = self._client

        # Pull the entity ID from path params (CRUD routes use "{id}")
        entity_id = path_params.get("id")

        if action == "create":
            return await client.create(body)  # type: ignore[union-attr]
        elif action == "read":
            return await client.read(entity_id)  # type: ignore[union-attr]
        elif action == "update":
            return await client.update(entity_id, body)  # type: ignore[union-attr]
        elif action == "patch":
            return await client.patch(entity_id, body)  # type: ignore[union-attr]
        elif action == "delete":
            return await client.delete(entity_id)  # type: ignore[union-attr]
        elif action == "list":
            # list() accepts filter/sort/pagination as keyword args
            return await client.list(  # type: ignore[union-attr]
                q=body.get("q"),
                sort=body.get("sort"),
                limit=body.get("limit", 50),
                offset=body.get("offset", 0),
            )
        else:
            # Custom @route — call via generic request() passing all args as body
            # DESIGN: generic fallback over per-method dispatch
            #   ✅ Works for any custom endpoint without knowing its signature
            #   ❌ No type-safe argument mapping — args must match the endpoint's contract
            return await client.request(  # type: ignore[union-attr]
                method=route.method,
                path=route.path.format(**path_params),
                json=body or None,
            )

    # ── MCP SDK integration ────────────────────────────────────────────────────

    def to_mcp_server(self) -> Any:
        """
        Build an ``mcp.server.Server`` from the adapter's tool list (mcp v2).

        Requires the ``mcp`` SDK (``pip install varco-fastapi[mcp]``).

        DESIGN: ``mcp.server.Server`` + ``mcp.types.Tool`` carrying varco's
        schema verbatim, over the high-level ``FastMCP`` (BACKLOG KI-11,
        research brief 003; migrated to v2's constructor-argument handler
        registration by Plan 029 / N1a, §D-N1-rewrite)
          ✅ Works today with what varco already has — ``Tool(name=…,
             description=…, inputSchema={…})`` accepts a plain dict; no
             signature synthesis, no post-processing (brief 003 §finding 2).
             ``FastMCP.add_tool()`` has **never** accepted an ``input_schema=``
             parameter (SDK issue #761, open since May 2025) — it derives the
             schema from the handler's type hints, which cannot express
             varco's generic ``execute(tool_name, arguments)`` dispatch (an
             untyped ``**kwargs`` shim, deliberately — dispatch is generic).
          ✅ **The v1→v2 portability claim this DESIGN block originally made
             was tested and held** (Plan 029 / N1a, Step 20): the low-level
             ``Tool``-with-schema construction in ``_to_mcp_tools`` needed
             ZERO changes migrating from v1.29.1 to v2.1.1 — only handler
             *registration* changed (this method), exactly as predicted
             (brief 003 §finding 3).
          ✅ Reversible — if SDK issue #761 ever lands ``input_schema=`` on the
             high-level server, reverting to decorators is a small diff.
          ❌ More verbose than ``FastMCP``, and loses its automatic argument
             validation (mitigated: v2 drops that validation anyway — "schemas
             are advertised but not validated", brief 003 §finding 3 — so this
             is a cost varco pays one release early rather than a cost it
             avoids). ``mount()`` also loses ``FastMCP.sse_app()`` and must
             build its own Starlette SSE routes (§mount below) — bounded cost,
             since ``mount()`` was already broken by this same defect.
          Rejected: synthesizing a typed handler signature from the JSON
          Schema so FastMCP derives the "right" schema — brief 003 calls this
          "fragile; no robust off-the-shelf tool" and issue #761 itself
          records the same workaround as unreliable. Rejected: reaching into
          ``FastMCP._tool_manager``/``._mcp_server`` — a private-attribute
          workaround around an upstream gap, the exact thing CLAUDE.md's
          providify discipline forbids ("use the sanctioned API, file the
          gap").

        DESIGN: ``on_list_tools`` returns a ``ListToolsResult``, not a bare
        ``list[Tool]`` (§D-N1-parity, resolved by experiment against the
        installed mcp 2.1.1 SDK, not by reading — Step 21)
          The 2026-07-28 wire requires ``resultType``/``ttlMs``/``cacheScope``
          on every list-shaped result. Verified via
          ``inspect.signature(Server.__init__)``: ``on_list_tools`` is typed
          ``Callable[..., Awaitable[types.ListToolsResult]]`` — a bare list is
          not the contracted return type at all. Verified via
          ``inspect.getsource(types.CacheableResult)``: the SDK **does**
          default ``ttl_ms=0``/``cache_scope="private"`` on construction, so
          the only obligation on varco's side is constructing the
          ``ListToolsResult`` wrapper — ``ttl_ms=`` is set explicitly here
          from the adapter's own ``ttl_ms`` parameter (default ``60_000``)
          rather than accepting the SDK's ``0`` (immediately-stale) default,
          since varco's tool list does not change after construction.

        Returns:
            A configured ``mcp.server.Server`` instance with
            ``on_list_tools``/``on_call_tool`` handlers registered, ready to
            be run over stdio or wrapped in an ASGI transport (see
            ``mount()``).

        Raises:
            ImportError: If the ``mcp`` package is not installed.

        Usage::

            server = adapter.to_mcp_server()
            # stdio transport (for local LLMs) — see mcp.server.stdio

        Thread safety:  ✅ Creates a new Server instance — no shared state.
        """
        try:
            from mcp.server import Server  # noqa: PLC0415
            from mcp.types import (  # noqa: PLC0415
                CallToolRequestParams,
                CallToolResult,
                ListToolsResult,
            )
        except ImportError as exc:
            raise ImportError(
                "The 'mcp' package is required to run MCPAdapter as an MCP server. "
                "Install it with: pip install 'varco-fastapi[mcp]'"
            ) from exc

        mcp_tools = _to_mcp_tools(self._tools)
        ttl_ms = self._ttl_ms

        async def _list_tools(ctx: Any, params: Any = None) -> ListToolsResult:
            # §D-N1-parity: a list-shaped result must carry cacheScope/ttlMs
            # on the 2026-07-28 wire — see the method DESIGN block above.
            return ListToolsResult(tools=mcp_tools, ttl_ms=ttl_ms, cache_scope="private")

        async def _call_tool(ctx: Any, params: CallToolRequestParams) -> CallToolResult:
            from mcp.types import TextContent  # noqa: PLC0415

            result = await self.execute(params.name, params.arguments or {})
            return CallToolResult(
                content=[TextContent(type="text", text=_json.dumps(result, default=str))]
            )

        server: Any = Server(
            name=f"{self._router_cls.__name__}MCP",
            on_list_tools=_list_tools,
            on_call_tool=_call_tool,
        )
        return server

    def mount(
        self,
        app: FastAPI,
        *,
        path: str = "/mcp",
        server_auth: AbstractServerAuth | None = None,
    ) -> None:
        """
        Mount the MCP adapter as an HTTP+SSE endpoint on a FastAPI application.

        Requires the ``mcp`` SDK (``pip install varco-fastapi[mcp]``).

        Builds a Starlette sub-application wiring ``mcp.server.sse.SseServerTransport``
        (the SDK's own documented recipe — ``mcp/server/sse.py``'s module
        docstring) directly, since the low-level ``Server`` (unlike the retired
        ``FastMCP``) has no ``sse_app()``/``asgi_app()`` convenience method.
        ``mount()`` was already broken before this change (both of its only
        two call sites — ``to_mcp_server()`` and ``server.sse_app()`` — raised),
        so this is construction, not repair.

        Args:
            app:         The ``FastAPI`` application to mount onto.
            path:        URL path prefix for the MCP endpoint.  Default: ``"/mcp"``.
            server_auth: Optional ``AbstractServerAuth`` instance.  When provided,
                         ``MCPAuthMiddleware`` is added to ``app`` to protect all
                         requests under ``path``.  If ``None``, no auth is applied
                         to the MCP endpoint (only suitable for internal networks).

        Raises:
            ImportError: If the ``mcp`` package is not installed.

        Usage::

            app = FastAPI()
            auth = ApiKeyAuth(keys={"secret": AuthContext(user_id="agent")})
            adapter.mount(app, server_auth=auth)   # MCP endpoint requires auth
            adapter.mount(app)                     # MCP endpoint is open (no auth)

        Thread safety:  ✅ Called once at startup before requests arrive.
        Async safety:   ✅ No I/O at mount time — only FastAPI/Starlette route
                        registration; the SSE connection itself is async.
        """
        # Wire authentication middleware BEFORE mounting the MCP app so the
        # middleware stack executes in the correct order: auth → mcp app.
        if server_auth is not None:
            app.add_middleware(
                MCPAuthMiddleware,
                server_auth=server_auth,
                mount_path=path,
            )

        server = self.to_mcp_server()

        try:
            from mcp.server.sse import SseServerTransport  # noqa: PLC0415
            from starlette.applications import Starlette  # noqa: PLC0415
            from starlette.responses import Response  # noqa: PLC0415
            from starlette.routing import Mount, Route  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "The 'mcp' package is required to run MCPAdapter as an MCP server. "
                "Install it with: pip install 'varco-fastapi[mcp]'"
            ) from exc

        # SseServerTransport takes the message-POST endpoint path (relative to
        # this sub-app's own mount point) — mcp/server/sse.py's own module
        # docstring recipe.
        sse_transport = SseServerTransport("/messages/")

        async def _handle_sse(request: Any) -> Any:
            async with sse_transport.connect_sse(
                request.scope, request.receive, request._send
            ) as streams:
                await server.run(streams[0], streams[1], server.create_initialization_options())
            # Must return a Response — the SDK's own docstring warns that
            # returning None causes "TypeError: 'NoneType' object is not
            # callable" once the client disconnects.
            return Response()

        mcp_app = Starlette(
            routes=[
                Route("/sse", endpoint=_handle_sse, methods=["GET"]),
                Mount("/messages/", app=sse_transport.handle_post_message),
            ]
        )
        app.mount(path, mcp_app)
        _logger.info(
            "MCPAdapter: mounted %d tools at %s for %s%s",
            len(self._tools),
            path,
            self._router_cls.__name__,
            " (auth=enabled)" if server_auth is not None else "",
        )

    def __repr__(self) -> str:
        return (
            f"MCPAdapter("
            f"router={self._router_cls.__name__!r}, "
            f"tools={len(self._tools)}, "
            f"prefix={self._prefix!r})"
        )


# ── DI helper ─────────────────────────────────────────────────────────────────


def bind_mcp_adapter(
    container: DIContainer,
    router_cls: type,
    *,
    qualifier: str | None = None,
    client_cls: type | None = None,
    base_url: str | None = None,
    tool_name_prefix: str = "",
    enabled_routes: set[str] | None = None,
) -> DIContainer:
    """
    Register an ``MCPAdapter`` singleton in a providify ``DIContainer``.

    After this call, ``Inject[MCPAdapter]`` resolves to the adapter for
    ``router_cls``.  If multiple routers need MCP exposure, register them
    with different qualifiers::

        bind_mcp_adapter(container, OrderRouter, client_cls=OrderClient)
        bind_mcp_adapter(container, UserRouter, client_cls=UserClient, qualifier="users")

    Args:
        container:        ``DIContainer`` instance.
        router_cls:       The ``VarcoRouter`` subclass to expose.
        qualifier:        Optional qualifier so multiple routers can each
                          have their own ``MCPAdapter`` binding (§D-bind).
                          ``None`` registers the unqualified default.
        client_cls:       ``AsyncVarcoClient`` subclass for execution.
                          Resolved from the container if registered; constructed
                          directly otherwise.
        base_url:         Fallback base URL if ``client_cls`` is not provided.
        tool_name_prefix: Prefix prepended to all tool names.
        enabled_routes:   Explicit route allowlist (``None`` = all mcp_enabled).

    Returns:
        The same ``container`` passed in.

    Raises:
        AlreadyBoundError: A non-fallback ``MCPAdapter`` binding of the same
            ``qualifier`` already exists.

    Edge cases:
        - If ``client_cls`` is not registered in the container, a bare client is
          constructed with ``base_url`` — suitable for local/test use only.

    Thread safety:  ✅ Registration is expected at bootstrap (single-threaded).
    Async safety:   ✅ No I/O during registration.
    """
    from providify import Provider  # noqa: PLC0415
    from varco_core.di import AlreadyBoundError, is_user_bound  # noqa: PLC0415

    if is_user_bound(container, MCPAdapter, qualifier=qualifier):
        raise AlreadyBoundError(
            f"A non-fallback MCPAdapter binding qualifier={qualifier!r} already exists — "
            "pass a different qualifier= to register another router's adapter, or use "
            "providify's di_overrides/ContainerOverrides in a test."
        )

    # Capture args in closure — avoids late-binding if bind_mcp_adapter is
    # called in a loop for multiple routers.
    _router_cls = router_cls
    _client_cls = client_cls
    _base_url = base_url
    _prefix = tool_name_prefix
    _enabled = enabled_routes

    def _mcp_adapter_factory() -> MCPAdapter:
        """Singleton MCPAdapter factory — built once at first injection.

        Registered via ``container.provide(Provider(...)(...), returns=...)``
        — the decoration-time ``returns=`` override means this closure's
        placeholder return annotation never needs patching.
        """
        from varco_fastapi.router._di import _resolve_optional  # noqa: PLC0415

        client: Any = None
        if _client_cls is not None:
            # Try to resolve from container first (preferred — gets auth,
            # etc.); fall back to bare construction if not registered
            # (Plan 050 / §D-asks P-INJ shim — see _di.py's module docstring
            # for why this cannot be a declared Inject[...] parameter).
            client = _resolve_optional(container, _client_cls) or _client_cls(base_url=_base_url)
        return MCPAdapter(
            _router_cls,
            client=client,
            base_url=_base_url,
            tool_name_prefix=_prefix,
            enabled_routes=_enabled,
        )

    container.provide(
        Provider(singleton=True, qualifier=qualifier)(_mcp_adapter_factory), returns=MCPAdapter
    )
    return container


# ── Public API ─────────────────────────────────────────────────────────────────

__all__ = [
    "MCPAdapter",
    "MCPAuthMiddleware",
    "MCPToolDefinition",
    "bind_mcp_adapter",
]
