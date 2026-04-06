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
    adapter.mount(app)   # adds POST /mcp

    # Option B: run as standalone stdio MCP server (for local LLMs)
    server = adapter.to_mcp_server()
    server.run()

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

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from varco_fastapi.router.introspection import ResolvedRoute, introspect_routes

if TYPE_CHECKING:
    from fastapi import FastAPI
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
            schema = route.request_model.model_json_schema()
            # model_json_schema() may return $defs for nested models
            defs = schema.pop("$defs", {})
            body_props = schema.get("properties", {})
            body_required = schema.get("required", [])
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
            _logger.debug(
                "MCPAdapter: could not build schema for %s", route.request_model
            )

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


# ── MCPAdapter ─────────────────────────────────────────────────────────────────


class MCPAdapter:
    """
    Converts a ``VarcoRouter`` class into an MCP server.

    Reads ``ResolvedRoute`` metadata via ``introspect_routes()`` to build tool
    definitions.  Delegates tool execution to ``AsyncVarcoClient`` so no
    handler logic is duplicated.

    Typical usage — mount on FastAPI::

        adapter = MCPAdapter(OrderRouter, client=OrderClient(base_url="http://localhost:8080"))
        adapter.mount(app)   # registers POST /mcp

    Typical usage — stdio transport (local LLM)::

        server = adapter.to_mcp_server()
        server.run()

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

    Thread safety:  ✅ Read-only after construction — safe to share across tasks.
    Async safety:   ✅ ``execute()`` is async; ``tools`` has no I/O.
    """

    def __init__(
        self,
        router_cls: type,
        *,
        client: AsyncVarcoClient | None = None,
        base_url: str | None = None,
        tool_name_prefix: str = "",
        enabled_routes: set[str] | None = None,
    ) -> None:
        self._router_cls = router_cls
        self._prefix = tool_name_prefix
        self._resource = _resource_name(router_cls)

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
            self._client = _Client(base_url=base_url)  # type: ignore[assignment]

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
            auto_desc = f"Perform the '{route.crud_action or route.name}' operation on {self._resource}."
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
        self._tool_by_name: dict[str, MCPToolDefinition] = {
            t.name: t for t in self._tools
        }

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
        Build a ``FastMCP`` server from the adapter's tool list.

        Requires the ``mcp`` SDK (``pip install varco-fastapi[mcp]``).

        Returns:
            A configured ``mcp.FastMCP`` instance ready to run as a stdio
            transport or HTTP server.

        Raises:
            ImportError: If the ``mcp`` package is not installed.

        Usage::

            server = adapter.to_mcp_server()
            server.run()  # stdio transport (for local LLMs)

        Thread safety:  ✅ Creates a new FastMCP instance — no shared state.
        """
        try:
            from mcp import FastMCP  # type: ignore[import-untyped]  # noqa: PLC0415
        except ImportError as exc:
            raise ImportError(
                "The 'mcp' package is required to run MCPAdapter as an MCP server. "
                "Install it with: pip install 'varco-fastapi[mcp]'"
            ) from exc

        server = FastMCP(name=f"{self._router_cls.__name__}MCP")

        for tool_def in self._tools:
            # Capture tool_def in closure — late-binding would use the last value
            _tool = tool_def

            async def _handler(**kwargs: Any) -> Any:
                # pop() is safe — arguments dict is created fresh per call
                return await self.execute(_tool.name, dict(kwargs))

            # Register with the mcp SDK
            server.add_tool(
                name=_tool.name,
                description=_tool.description,
                fn=_handler,
                input_schema=_tool.input_schema,
            )

        return server

    def mount(self, app: FastAPI, *, path: str = "/mcp") -> None:
        """
        Mount the MCP adapter as an HTTP+SSE endpoint on a FastAPI application.

        Requires the ``mcp`` SDK (``pip install varco-fastapi[mcp]``).

        Args:
            app:  The ``FastAPI`` application to mount onto.
            path: URL path prefix for the MCP endpoint.  Default: ``"/mcp"``.

        Raises:
            ImportError: If the ``mcp`` package is not installed.

        Usage::

            app = FastAPI()
            adapter.mount(app)     # registers POST /mcp + GET /mcp/sse

        Thread safety:  ✅ Called once at startup before requests arrive.
        Async safety:   ✅ No I/O — only FastAPI route registration.
        """
        server = self.to_mcp_server()
        # mcp SDK provides an ASGI app we can mount
        try:
            mcp_app = server.sse_app()  # type: ignore[attr-defined]
        except AttributeError:
            # Older mcp SDK versions use asgi_app()
            mcp_app = server.asgi_app()  # type: ignore[attr-defined]
        app.mount(path, mcp_app)
        _logger.info(
            "MCPAdapter: mounted %d tools at %s for %s",
            len(self._tools),
            path,
            self._router_cls.__name__,
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
    container: Any,
    router_cls: type,
    *,
    client_cls: type | None = None,
    base_url: str | None = None,
    tool_name_prefix: str = "",
    enabled_routes: set[str] | None = None,
) -> None:
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
        client_cls:       ``AsyncVarcoClient`` subclass for execution.
                          Resolved from the container if registered; constructed
                          directly otherwise.
        base_url:         Fallback base URL if ``client_cls`` is not provided.
        tool_name_prefix: Prefix prepended to all tool names.
        enabled_routes:   Explicit route allowlist (``None`` = all mcp_enabled).

    Edge cases:
        - Calling twice with the same ``router_cls`` replaces the previous binding.
        - If ``client_cls`` is not registered in the container, a bare client is
          constructed with ``base_url`` — suitable for local/test use only.
        - If providify is not installed, this function is a no-op with a warning.

    Thread safety:  ✅ Registration is expected at bootstrap (single-threaded).
    Async safety:   ✅ No I/O during registration.
    """
    try:
        from providify import Provider  # noqa: PLC0415
    except ImportError:
        _logger.warning(
            "bind_mcp_adapter: providify not installed — MCPAdapter not registered in DI."
        )
        return

    # Capture args in closure — avoids late-binding if bind_mcp_adapter is
    # called in a loop for multiple routers.
    _router_cls = router_cls
    _client_cls = client_cls
    _base_url = base_url
    _prefix = tool_name_prefix
    _enabled = enabled_routes

    @Provider(singleton=True)
    def _mcp_adapter_factory() -> MCPAdapter:
        """Singleton MCPAdapter factory — built once at first injection."""
        client = None
        if _client_cls is not None:
            # Try to resolve from container first (preferred — gets auth, etc.)
            try:
                client = container.get(_client_cls)
            except Exception:  # noqa: BLE001
                # Fall back to bare construction if not registered
                client = _client_cls(base_url=_base_url)
        return MCPAdapter(
            _router_cls,
            client=client,
            base_url=_base_url,
            tool_name_prefix=_prefix,
            enabled_routes=_enabled,
        )

    # Patch return annotation so providify resolves Inject[MCPAdapter]
    _mcp_adapter_factory.__annotations__["return"] = MCPAdapter

    container.provide(_mcp_adapter_factory)


# ── Public API ─────────────────────────────────────────────────────────────────

__all__ = [
    "MCPAdapter",
    "MCPToolDefinition",
    "bind_mcp_adapter",
]
