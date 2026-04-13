"""
varco_fastapi.router.base
==========================
Core router infrastructure for ``VarcoRouter``.

``VarcoRouter[D, PK, C, R, U]`` is the generic base class for all FastAPI
routers in varco.  Subclasses mix in ``RouterMixin`` subclasses (``CreateMixin``,
``ReadMixin``, etc.) and declare ClassVar configuration.  ``build_router()``
materializes the route declarations into a FastAPI ``APIRouter``.

Key types:

- ``RouterMixin``      — ABC marker for CRUD mixins (mirrors ``ServiceMixin``)
- ``AsyncModeParams``  — Query params for background job offload (``?with_async=true``)
- ``HttpQueryParams``  — Raw HTTP query strings bridged to ``varco_core.query.QueryParams``
- ``VarcoRouter``      — Generic router base; ``build_router()`` produces an ``APIRouter``

Design — metadata-on-function (mirrors ``@listen`` / ``@route``):

    Route declarations are stored on the **class** (via ClassVars for CRUD mixins
    or ``__route_entry__`` for ``@route``).  No FastAPI registration happens until
    ``build_router()`` is called.  This allows:

    - Tests to inspect the class without needing a running FastAPI app
    - Multiple ``build_router()`` calls with different prefixes/auth strategies
    - Adapters (MCPAdapter, StubGenerator) to read ``introspect_routes()`` without
      FastAPI involved

DESIGN: ClassVar service injection over FastAPI Depends()
    ✅ Service is fully typed at class definition — IDE autocomplete works
    ✅ No @Depends() indirection in route handler signatures
    ✅ Consistent with varco's DI pattern (providify Inject[])
    ❌ Service is captured at class definition time — hard to swap per-request
       (use Depends() for request-scoped resources like DB sessions instead)

DESIGN: Generic type arg resolution at build_router() time
    ✅ Works with standard Python generics — no metaclass tricks needed
    ✅ Type args available for OpenAPI schema generation (request/response models)
    ✅ No conflict with FastAPI's internal class machinery
    ❌ Requires __orig_bases__ walk — silently uses Any if type args are missing

Thread safety:  ✅ All ClassVars are read-only after class definition.
                   ``build_router()`` creates new objects without mutating shared state.
Async safety:   ✅ ``build_router()`` is synchronous; closures are async-safe.
"""

from __future__ import annotations

import logging
from abc import ABC
from dataclasses import dataclass
from typing import Any, ClassVar, Generic, get_args, get_origin

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import Response

from varco_core.auth.base import AuthContext
from varco_core.query import QueryParams, QueryParser, SortField, SortOrder

from varco_fastapi.router.introspection import introspect_routes
from providify import Instance
from varco_core.job import AbstractJobRunner
from varco_core.service.base import D, PK, C, R, U

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from varco_fastapi import AbstractServerAuth


logger = logging.getLogger(__name__)

# ── Sort string parser ─────────────────────────────────────────────────────────


def _parse_sort_string(sort_str: str) -> list[SortField]:
    """
    Parse a sort directive string into a list of ``SortField`` objects.

    Format: comma-separated field names, each optionally prefixed with
    ``+`` (ASC) or ``-`` (DESC).  Unprefixed fields default to ASC.

    Args:
        sort_str: Sort directive string, e.g. ``"+created_at,-name,status"``.

    Returns:
        Ordered list of ``SortField`` objects.

    Edge cases:
        - Empty string → empty list
        - Whitespace-only segments are skipped
        - Leading/trailing whitespace on each segment is stripped
    """
    fields: list[SortField] = []
    for part in sort_str.split(","):
        part = part.strip()
        if not part:
            # Skip empty segments from trailing commas or double commas
            continue
        if part.startswith("-"):
            fields.append(SortField(field=part[1:], order=SortOrder.DESC))
        elif part.startswith("+"):
            fields.append(SortField(field=part[1:], order=SortOrder.ASC))
        else:
            # No prefix → ascending (most natural default)
            fields.append(SortField(field=part, order=SortOrder.ASC))
    return fields


# ── AsyncModeParams ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AsyncModeParams:
    """
    Query parameters that control background job offload for an endpoint.

    When ``with_async=true`` is sent, the endpoint submits the operation as a
    background ``Job`` and returns ``202 Accepted`` with a ``JobAcceptedResponse``
    instead of blocking for the result.

    Injected into every ``async_capable=True`` endpoint as a FastAPI dependency
    via ``_async_mode_params``.

    Attributes:
        with_async:   If ``True``, offload the operation to the job runner.
        callback_url: Optional URL to POST the result to when the job completes.

    DESIGN: separate params object over individual query params
        ✅ Single dependency — cleaner handler signatures
        ✅ Frozen dataclass — safe to pass around without copying
        ❌ One extra Depends() vs. two flat params — negligible overhead

    Thread safety:  ✅ Frozen dataclass — immutable.
    Async safety:   ✅ Pure value object.
    """

    with_async: bool = False
    callback_url: str | None = None


async def _async_mode_params(
    with_async: bool = Query(
        False, alias="with_async", description="Offload to background job"
    ),
    callback_url: str | None = Query(
        None, alias="callback_url", description="Callback URL on job completion"
    ),
) -> AsyncModeParams:
    """
    FastAPI dependency that captures async-mode query parameters.

    Returns:
        ``AsyncModeParams`` with values from the query string.
    """
    return AsyncModeParams(with_async=with_async, callback_url=callback_url)


# ── HttpQueryParams ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class HttpQueryParams:
    """
    HTTP-level query parameters bridging raw URL strings to ``QueryParams``.

    This is the HTTP adapter for ``varco_core.query.QueryParams``.  It receives
    raw string values from the request URL and converts them to typed AST nodes
    and pagination directives via ``to_query_params()``.

    Injected into ``ListMixin`` endpoints as a FastAPI dependency via
    ``_http_query_params``.

    Attributes:
        q:      Raw filter expression, e.g. ``"status = 'active' AND age > 18"``.
                Parsed by ``QueryParser`` into an AST node.
        sort:   Sort directives, e.g. ``"+created_at,-name"``.  Comma-separated
                field names prefixed with ``+`` (ASC) or ``-`` (DESC).
        limit:  Maximum number of results.  ``None`` → use ``default_limit``.
        offset: Number of results to skip.  ``None`` → 0.

    DESIGN: separate HttpQueryParams over direct QueryParams injection
        ✅ Keeps HTTP concerns (raw strings) separate from domain query model
        ✅ to_query_params() is the single parsing site — easy to test in isolation
        ✅ Mirrors existing QueryParser / QueryParams separation in varco_core
        ❌ One extra conversion step vs. parsing inline in the route handler

    Thread safety:  ✅ Frozen dataclass — immutable.
    Async safety:   ✅ to_query_params() is synchronous; safe to call from async.

    Edge cases:
        - ``q=None`` → no filter node (``QueryParams.node = None``)
        - ``sort=None`` → empty sort list (database default order)
        - ``limit > max_limit`` → clamped to ``max_limit``
        - ``limit=0`` → valid (returns empty page); use ``None`` for "all results"
        - Malformed ``q`` string → ``QueryParser`` raises; caught by ``ErrorMiddleware``
          and mapped to HTTP 400.
    """

    q: str | None = None
    sort: str | None = None
    limit: int | None = None
    offset: int | None = None

    def to_query_params(
        self,
        *,
        max_limit: int = 100,
        default_limit: int = 50,
    ) -> QueryParams:
        """
        Parse raw HTTP query strings into a typed ``QueryParams`` value object.

        Args:
            max_limit:     Hard upper bound for ``limit``.  Prevents unbounded
                           ``SELECT *`` from overwhelming the database.
            default_limit: Used when ``self.limit`` is ``None``.

        Returns:
            ``QueryParams`` ready for ``service.list()`` or ``repo.find_by_query()``.

        Raises:
            lark.UnexpectedToken: Syntax error in ``self.q`` filter string.
                                  Propagates up to ``ErrorMiddleware`` → HTTP 400.
            OperationNotFound:    Unknown operator in ``self.q`` filter string.

        Edge cases:
            - ``q=None`` → node is ``None`` (no filter applied)
            - ``sort=None`` → empty list (database default order preserved)
            - ``limit`` is clamped: ``min(self.limit or default_limit, max_limit)``
            - ``offset`` defaults to 0 when ``None``
        """
        # Parse filter expression into AST — None means "no WHERE clause"
        node = QueryParser().parse(self.q) if self.q is not None else None

        # Parse sort string — empty list means "no ORDER BY"
        sort = _parse_sort_string(self.sort) if self.sort is not None else []

        # Clamp limit to prevent accidentally fetching millions of rows
        effective_limit = min(
            self.limit if self.limit is not None else default_limit, max_limit
        )

        return QueryParams(
            node=node,
            sort=sort,
            limit=effective_limit,
            offset=self.offset if self.offset is not None else 0,
        )


async def _http_query_params(
    q: str | None = Query(
        None,
        description="Filter expression (e.g. status = 'active' AND age > 18)",
    ),
    sort: str | None = Query(
        None,
        description="Sort directives (e.g. +created_at,-name)",
    ),
    limit: int | None = Query(
        None,
        ge=0,
        description="Maximum number of results",
    ),
    offset: int | None = Query(
        None,
        ge=0,
        description="Number of results to skip",
    ),
) -> HttpQueryParams:
    """
    FastAPI dependency that captures raw HTTP query parameters.

    Returns:
        ``HttpQueryParams`` populated from the request's query string.
    """
    return HttpQueryParams(q=q, sort=sort, limit=limit, offset=offset)


# ── RouterMixin ────────────────────────────────────────────────────────────────


class RouterMixin(ABC):
    """
    Marker ABC for CRUD and custom route mixins.

    This is the router-layer equivalent of ``ServiceMixin`` in ``varco_core``.
    Each concrete mixin (``CreateMixin``, ``ReadMixin``, etc.) subclasses
    ``RouterMixin`` and declares a ``_CRUD_ACTION`` ClassVar that is read by
    ``introspect_routes()`` to identify which CRUD route to generate.

    DESIGN: marker ABC over protocol or duck typing
        ✅ ``isinstance(cls, RouterMixin)`` works for runtime checks
        ✅ Signals intent clearly — mixins are distinguishable from other classes
        ✅ Mirrors ``ServiceMixin`` convention already established in varco_core
        ❌ Requires explicit inheritance — cannot mix in structural-typing classes

    Thread safety:  ✅ No state — abstract marker only.
    Async safety:   ✅ No I/O.
    """

    # Subclasses override with their action name (e.g. "create", "read", ...)
    # This ClassVar is read by introspect_routes() to identify CRUD routes.
    _CRUD_ACTION: ClassVar[str | None] = None


# ── Generic type arg resolution ────────────────────────────────────────────────


def _resolve_type_args(router_cls: type) -> tuple[type, ...] | None:
    """
    Walk ``__orig_bases__`` to find the ``VarcoRouter[D, PK, C, R, U]`` base
    and extract its generic type arguments.

    Args:
        router_cls: The ``VarcoRouter`` subclass to inspect.

    Returns:
        Tuple ``(D, PK, C, R, U)`` of resolved types, or ``None`` if not found.

    Edge cases:
        - No VarcoRouter base → returns None (all models will be Any)
        - VarcoRouter used without type args → returns None
        - Type args are TypeVars (unresolved generics) → returns None
    """
    for base in getattr(router_cls, "__orig_bases__", ()):
        origin = get_origin(base)
        if origin is None:
            continue
        # Check if this base is VarcoRouter or a subclass of it
        try:
            if not issubclass(origin, VarcoRouter):
                continue
        except TypeError:
            continue
        args = get_args(base)
        if not args:
            continue
        # Filter out unresolved TypeVars — they carry no type information
        from typing import TypeVar as TypingTypeVar

        resolved = tuple(a for a in args if not isinstance(a, TypingTypeVar))
        if len(resolved) == len(args):
            return args
    return None


# ── VarcoRouter ────────────────────────────────────────────────────────────────


class VarcoRouter(Generic[D, PK, C, R, U]):
    """
    Generic base class for FastAPI routers in the varco framework.

    Subclass this with ``RouterMixin`` subclasses and declare ClassVar
    configuration to get a fully-wired ``APIRouter`` from ``build_router()``.

    Type parameters (mirrors ``AsyncService[D, PK, C, R, U]``):
        D:   ``DomainModel`` subclass
        PK:  Primary key type (e.g. ``UUID``, ``int``)
        C:   Create DTO
        R:   Read DTO (also the update/patch response)
        U:   Update / Patch DTO

    ClassVars (configure at class definition time):
        _prefix:     URL prefix for all routes (e.g. ``"/orders"``).
        _tags:       OpenAPI tags (e.g. ``["orders"]``).
        _version:    API version prefix (e.g. ``"v2"`` → ``/v2/orders/``).
        _service:    Injected service instance (``Inject[ConcreteService]``).
        _job_runner: Injected ``AbstractJobRunner`` for async offload.
        _auth:       ``AbstractServerAuth`` instance used for all routes.
        _event_bus:  ``AbstractEventBus`` for WebSocket/SSE mixins.

    Usage::

        class OrderRouter(CreateMixin, ReadMixin, ListMixin, VarcoRouter[Order, UUID, OrderCreateDTO, OrderReadDTO, OrderUpdateDTO]):
            _prefix = "/orders"
            _tags = ["orders"]
            _service = container.get(OrderService)
            _auth = container.get(JwtBearerAuth)

        router = OrderRouter().build_router()
        app.include_router(router)

    Thread safety:  ✅ ClassVars are read-only after class definition.
    Async safety:   ✅ build_router() is synchronous; generated closures are async-safe.
    """

    # ── ClassVar configuration ────────────────────────────────────────────────

    _prefix: ClassVar[str] = ""
    _tags: ClassVar[list[str]] = []

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """
        Ensure every subclass owns an independent copy of ``_tags``.

        Without this hook, all subclasses that do NOT declare ``_tags``
        share the *same* list object inherited from ``VarcoRouter``.
        A mutating call (e.g. ``SomeRouter._tags.append("foo")``) would
        silently contaminate every router that inherits the default.

        The ``"_tags" not in cls.__dict__`` guard is intentional:
        - ``cls.__dict__`` checks the subclass's own namespace only —
          it does NOT walk the MRO, unlike ``hasattr``.
        - If the subclass already declared ``_tags = ["foo"]`` we must
          leave that declaration untouched; only the inherited case needs
          a fresh copy.

        Edge cases:
            - ``_tags`` declared on an intermediate mixin and not on the
              concrete subclass → the mixin's copy is safe because it was
              already isolated by a previous ``__init_subclass__`` call.
            - Multiple inheritance → Python calls ``__init_subclass__``
              once per class, so each class in the MRO gets its own copy
              the first time it is defined.
        """
        super().__init_subclass__(**kwargs)
        # Copy the inherited list so mutations on one subclass do not
        # bleed into sibling subclasses or the base class default.
        if "_tags" not in cls.__dict__:
            cls._tags = list(cls._tags)

    # API version prefix — when set, prepended to _prefix.
    # e.g. _prefix="/orders", _version="v2" → effective prefix="/v2/orders"
    _version: ClassVar[str | None] = None

    # Server auth strategy — set directly at class definition time.
    # None means no auth dependency is injected into route handlers.
    # DESIGN: ClassVar (not __init__ param) for _auth because it is the common case to
    # declare the auth strategy once for the whole router type at class definition:
    #   class PostRouter(VarcoCRUDRouter): _auth = JwtBearerAuth(...)
    # Per-instance override is still possible via __init__(auth=...) in VarcoCRUDRouter,
    # which writes self._auth as an instance attribute that shadows this ClassVar.
    _auth: ClassVar[AbstractServerAuth | None]

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"prefix={self._effective_prefix()!r}, "
            f"tags={self._tags!r})"
        )

    def __init__(
        self,
        *,
        job_runner: Instance[AbstractJobRunner] | None = None,
    ) -> None:
        """
        Base ``VarcoRouter`` constructor.

        ``VarcoRouter`` itself has no service injection — it is pure routing
        infrastructure.  For service-backed CRUD routes, subclass
        ``VarcoCRUDRouter`` instead, which adds the service injection.

        Args:
            job_runner: Optional ``AbstractJobRunner`` for async route offload
                        (``?with_async=true``).  When ``None``, async mode falls
                        through to synchronous execution.  Injected by DI when
                        ``VarcoFastAPIModule`` is installed; pass directly in
                        tests or DI-less setups.

        Edge cases:
            - ``job_runner=None`` → ``?with_async=true`` is silently ignored;
              the handler executes synchronously.

        Thread safety:  ✅ Instance attribute set once at construction.
        Async safety:   ✅ Safe — no shared mutable state.

        DESIGN: instance attribute (not ClassVar) for _job_runner
            ✅ DI injects reliably via __init__ kwargs — avoids the broken
               ClassVar injection path (get_type_hints fails on forward refs
               with ``from __future__ import annotations``, silently skipped).
            ✅ Each router instance owns its proxy — no cross-subclass sharing.
            ✅ getattr(self, ...) correctly finds instance state before MRO.
            ❌ Cannot be set declaratively at class-definition time; use
               __init__(job_runner=...) or a class-level assignment that will
               still be found via MRO when no instance attribute exists.
        """
        # Only write the instance attribute when a value is actually provided.
        # If job_runner=None (default, no DI), we leave the class namespace alone so
        # that a class-level ``_job_runner = runner`` assignment (common in tests) is
        # still found by getattr(self, "_job_runner") via normal MRO lookup.
        if job_runner is not None:
            self._job_runner = job_runner

    def _effective_prefix(self) -> str:
        """
        Compute the effective URL prefix with version prepended if set.

        Returns:
            ``"/{version}{prefix}"`` if ``_version`` is set, else ``_prefix``.

        Edge cases:
            - ``_version=None`` → returns ``_prefix`` unchanged
            - ``_prefix=""`` with ``_version="v1"`` → ``"/v1"``
        """
        if self._version is not None:
            # Strip leading slash from version to avoid double slashes
            version = self._version.lstrip("/")
            return f"/{version}{self._prefix}"
        return self._prefix

    def build_router(self) -> APIRouter:
        """
        Materialize route declarations into a FastAPI ``APIRouter``.

        Steps:
        1. Resolve generic type args ``(D, PK, C, R, U)`` from ``__orig_bases__``.
        2. Compute effective prefix (with version if set).
        3. Call ``introspect_routes()`` to get sorted ``ResolvedRoute`` list.
        4. For each route, create a typed endpoint closure and register it.
        5. Return the completed ``APIRouter``.

        Returns:
            A FastAPI ``APIRouter`` with all declared routes registered.

        Raises:
            RuntimeError: If ``_service`` is ``None`` and a CRUD route requires it.

        Edge cases:
            - No CRUD mixins, only ``@route`` decorators → works fine
            - ``_auth=None`` → routes have no auth dependency injected
            - ``_job_runner=None`` → ``with_async=true`` falls through to sync execution
            - Generic type args not resolvable → request/response models are ``None``
              (FastAPI accepts any body / skips response validation)
        """
        router_cls = type(self)
        type_args = _resolve_type_args(router_cls)
        prefix = self._effective_prefix()
        tags = list(self._tags)

        # Collect sorted routes via shared introspection
        routes = introspect_routes(router_cls, type_args=type_args)

        api_router = APIRouter(prefix=prefix, tags=tags)

        for resolved_route in routes:
            self._register_route(api_router, resolved_route, type_args)

        logger.debug(
            "VarcoRouter %s built %d routes on prefix %r",
            router_cls.__name__,
            len(routes),
            prefix,
        )
        return api_router

    def _register_route(
        self,
        api_router: APIRouter,
        route: Any,  # ResolvedRoute — avoid circular at module level
        type_args: tuple[type, ...] | None,
    ) -> None:
        """
        Register a single ``ResolvedRoute`` on the ``APIRouter``.

        Dispatches to the appropriate factory based on route type:
        - ``"WS"``  → ``api_router.add_api_websocket_route()``
        - other methods → ``api_router.add_api_route()``

        Args:
            api_router:  The ``APIRouter`` to register on.
            route:       The ``ResolvedRoute`` describing the endpoint.
            type_args:   Resolved generic type args for model annotation.
        """
        from varco_fastapi.router.introspection import ResolvedRoute as RR

        if not isinstance(route, RR):
            return

        method = route.method.upper()

        if method == "WS":
            # WebSocket routes use a different FastAPI registration method
            handler = self._make_ws_handler(route)
            api_router.add_api_websocket_route(
                path=route.path,
                endpoint=handler,
                name=route.name,
            )
            return

        # Build the endpoint closure
        handler = self._make_http_handler(route, type_args)

        # Collect extra FastAPI kwargs from route metadata
        extra = route.extra or {}
        responses = extra.get("responses") or {}
        dependencies = extra.get("dependencies") or []
        operation_id = extra.get("operation_id")
        response_description = extra.get("response_description", "Successful Response")
        include_in_schema = extra.get("include_in_schema", True)

        # For list routes, response_model is set via handler.__annotations__["return"]
        # (wrapped in PagedReadDTO[R] by _make_list_handler). Do not pass the raw R
        # type here or FastAPI will try to validate PagedReadDTO against R directly.
        explicit_response_model = (
            None if route.crud_action == "list" else route.response_model
        )
        api_router.add_api_route(
            path=route.path,
            endpoint=handler,
            methods=[method],
            status_code=route.status_code,
            summary=route.summary,
            description=route.description,
            response_model=explicit_response_model,
            tags=list(route.tags) if route.tags else None,
            deprecated=route.deprecated,
            responses=responses,
            dependencies=dependencies,
            operation_id=operation_id,
            response_description=response_description,
            include_in_schema=include_in_schema,
        )

    def _make_http_handler(
        self,
        route: Any,
        type_args: tuple[type, ...] | None,
    ) -> Any:
        """
        Create a FastAPI endpoint closure for an HTTP route.

        The closure captures the router instance so it can call ``self._service``,
        ``self._auth``, and ``self._job_runner``.  The closure's ``__annotations__``
        are overridden at creation time so FastAPI reads the correct Pydantic models
        for request body validation and response schema generation.

        Args:
            route:     The ``ResolvedRoute`` describing the endpoint.
            type_args: Resolved generic type args for request/response model annotation.

        Returns:
            An async callable suitable for ``APIRouter.add_api_route()``.

        DESIGN: __annotations__ override over creating a new function per type
            ✅ Works with FastAPI's inspect.signature() introspection
            ✅ No code generation (eval, exec) needed
            ✅ Same pattern used by fastapi-crudrouter, fastapi-utils
            ❌ Relies on CPython function attribute mutation — not typed
        """
        # Resolve auth and job_runner — use getattr with None fallback so subclasses
        # that don't set these ClassVars don't crash during introspection.
        server_auth = getattr(self, "_auth", None)
        # Resolve job_runner — instance attribute (set in __init__) takes priority;
        # falls back to a class-level assignment via MRO (used in tests).
        # Instance[...] proxy is resolved lazily here, not at construction time.
        _jr_proxy = getattr(self, "_job_runner", None)
        job_runner: AbstractJobRunner | None = None
        if (
            _jr_proxy is not None
            and hasattr(_jr_proxy, "is_resolvable")
            and _jr_proxy.is_resolvable()
        ):
            job_runner = _jr_proxy.get()
        elif _jr_proxy is not None and not hasattr(_jr_proxy, "is_resolvable"):
            # Directly assigned runner (test setup or class-level) — use as-is
            job_runner = _jr_proxy

        crud_action = route.crud_action

        # ── CRUD dispatch (when _service is available directly on this class) ──
        # VarcoCRUDRouter overrides this method and does its own dispatch.
        # For backward compat, VarcoRouter subclasses that set _service as a
        # ClassVar directly (without using VarcoCRUDRouter) still get CRUD handlers.
        service = getattr(self, "_service", None)
        if service is not None and crud_action in _CRUD_HANDLER_FACTORIES:
            factory = _CRUD_HANDLER_FACTORIES[crud_action]
            return factory(
                router=self,
                route=route,
                service=service,
                server_auth=server_auth,
                job_runner=job_runner,
            )

        # ── Custom @route endpoints ────────────────────────────────────────────
        method_name = route.name
        method_fn = getattr(type(self), method_name, None)
        if method_fn is None:
            logger.warning(
                "VarcoRouter: no method %r found for route %r", method_name, route.path
            )
            return _make_noop_handler(route)

        return _make_custom_handler(
            router_instance=self,
            method_fn=method_fn,
            route=route,
            server_auth=server_auth,
            job_runner=job_runner,
        )

    def _make_ws_handler(self, route: Any) -> Any:
        """
        Create a FastAPI WebSocket handler closure for a WS route.

        Args:
            route: The ``ResolvedRoute`` with ``method="WS"``.

        Returns:
            An async callable suitable for ``APIRouter.add_api_websocket_route()``.
        """
        method_name = route.name
        method_fn = getattr(type(self), method_name, None)
        if method_fn is None:
            logger.warning(
                "VarcoRouter: no method %r found for WS route %r",
                method_name,
                route.path,
            )

            async def _noop_ws(websocket: Any) -> None:  # noqa: RUF029
                await websocket.close()

            return _noop_ws

        router_instance = self

        async def _ws_handler(websocket: Any) -> None:
            # Delegate the full websocket lifecycle to the decorated method
            await method_fn(router_instance, websocket)

        _ws_handler.__name__ = method_name
        return _ws_handler


# ── CRUD handler factories ─────────────────────────────────────────────────────
#
# Each factory builds a typed async closure for one CRUD action.  The closure
# captures the service, auth, and job_runner from the outer scope; its
# __annotations__ are overridden so FastAPI reads the correct Pydantic models.
#
# DESIGN: separate factory per CRUD action over one mega-factory with conditionals
#   ✅ Each factory is focused and readable
#   ✅ Type annotation overrides are action-specific (create needs body; read doesn't)
#   ❌ More functions — justified by clarity over cleverness


def _make_create_handler(
    *,
    router: VarcoRouter,  # type: ignore[type-arg]
    route: Any,
    service: Any,
    server_auth: Any,
    job_runner: Any,
) -> Any:
    """
    Build the ``POST /`` create endpoint closure.

    Signature (after annotation override):
        ``async (body: C, auth: AuthContext, async_params: AsyncModeParams) -> R``
    """
    create_model = route.request_model
    response_model = route.response_model
    auth_dep = Depends(server_auth) if server_auth is not None else None
    async_dep = Depends(_async_mode_params)

    if auth_dep is not None:

        async def create_handler(
            request: Request,
            body: Any,
            auth: AuthContext = auth_dep,
            async_params: AsyncModeParams = async_dep,
        ) -> Any:
            if async_params.with_async and job_runner is not None:
                return await _submit_job(
                    router, job_runner, auth, service.create, body, auth
                )
            return await service.create(body, auth)

    else:

        async def create_handler(  # type: ignore[misc]
            request: Request,
            body: Any,
            async_params: AsyncModeParams = async_dep,
        ) -> Any:
            if async_params.with_async and job_runner is not None:
                return await _submit_job(router, job_runner, None, service.create, body)
            return await service.create(body)

    create_handler.__name__ = "create"
    # Override annotations so FastAPI uses the real Pydantic models
    if create_model is not None:
        create_handler.__annotations__["body"] = create_model
    if response_model is not None:
        create_handler.__annotations__["return"] = response_model
    return create_handler


def _make_read_handler(
    *,
    router: VarcoRouter,  # type: ignore[type-arg]
    route: Any,
    service: Any,
    server_auth: Any,
    job_runner: Any,
) -> Any:
    """
    Build the ``GET /{id}`` read-by-ID endpoint closure.

    Signature (after annotation override):
        ``async (id: Any, auth: AuthContext) -> R``
    """
    response_model = route.response_model
    auth_dep = Depends(server_auth) if server_auth is not None else None

    if auth_dep is not None:

        async def read_handler(id: Any, auth: AuthContext = auth_dep) -> Any:
            # AsyncService exposes get() not read() — read() was renamed in varco_core.
            return await service.get(id, auth)

    else:

        async def read_handler(id: Any) -> Any:  # type: ignore[misc]
            return await service.get(id)

    read_handler.__name__ = "read"
    if response_model is not None:
        read_handler.__annotations__["return"] = response_model
    return read_handler


def _make_update_handler(
    *,
    router: VarcoRouter,  # type: ignore[type-arg]
    route: Any,
    service: Any,
    server_auth: Any,
    job_runner: Any,
) -> Any:
    """
    Build the ``PUT /{id}`` full-replace update endpoint closure.

    Signature (after annotation override):
        ``async (id: Any, body: U, auth: AuthContext) -> R``
    """
    update_model = route.request_model
    response_model = route.response_model
    auth_dep = Depends(server_auth) if server_auth is not None else None

    if auth_dep is not None:

        async def update_handler(
            id: Any, body: Any, auth: AuthContext = auth_dep
        ) -> Any:
            return await service.update(id, body, auth)

    else:

        async def update_handler(id: Any, body: Any) -> Any:  # type: ignore[misc]
            return await service.update(id, body)

    update_handler.__name__ = "update"
    if update_model is not None:
        update_handler.__annotations__["body"] = update_model
    if response_model is not None:
        update_handler.__annotations__["return"] = response_model
    return update_handler


def _make_patch_handler(
    *,
    router: VarcoRouter,  # type: ignore[type-arg]
    route: Any,
    service: Any,
    server_auth: Any,
    job_runner: Any,
) -> Any:
    """
    Build the ``PATCH /{id}`` partial-update endpoint closure.

    Implements JSON Merge Patch semantics (RFC 7386): present fields are updated,
    absent fields are unchanged.  Relies on ``service.patch()`` which uses
    Pydantic's ``model_fields_set`` to distinguish present from absent fields.

    Signature (after annotation override):
        ``async (id: Any, body: U, auth: AuthContext) -> R``
    """
    update_model = route.request_model
    response_model = route.response_model
    auth_dep = Depends(server_auth) if server_auth is not None else None

    if auth_dep is not None:

        async def patch_handler(
            id: Any, body: Any, auth: AuthContext = auth_dep
        ) -> Any:
            return await service.patch(id, body, auth)

    else:

        async def patch_handler(id: Any, body: Any) -> Any:  # type: ignore[misc]
            return await service.patch(id, body)

    patch_handler.__name__ = "patch"
    if update_model is not None:
        patch_handler.__annotations__["body"] = update_model
    if response_model is not None:
        patch_handler.__annotations__["return"] = response_model
    return patch_handler


def _make_delete_handler(
    *,
    router: VarcoRouter,  # type: ignore[type-arg]
    route: Any,
    service: Any,
    server_auth: Any,
    job_runner: Any,
) -> Any:
    """
    Build the ``DELETE /{id}`` endpoint closure.

    Returns ``204 No Content`` — no body, no response model.

    Signature:
        ``async (id: Any, auth: AuthContext) -> None``
    """
    auth_dep = Depends(server_auth) if server_auth is not None else None

    if auth_dep is not None:

        async def delete_handler(id: Any, auth: AuthContext = auth_dep) -> Response:
            await service.delete(id, auth)
            return Response(status_code=204)

    else:

        async def delete_handler(id: Any) -> Response:  # type: ignore[misc]
            await service.delete(id)
            return Response(status_code=204)

    delete_handler.__name__ = "delete"
    return delete_handler


def _make_list_handler(
    *,
    router: VarcoRouter,  # type: ignore[type-arg]
    route: Any,
    service: Any,
    server_auth: Any,
    job_runner: Any,
) -> Any:
    """
    Build the ``GET /`` list endpoint closure.

    Injects ``HttpQueryParams`` and converts to ``QueryParams`` via
    ``to_query_params()``.  Adds pagination headers to the response.

    Signature (after annotation override):
        ``async (http_params, auth, response) -> PagedReadDTO[R]``
    """
    from varco_fastapi.router.pagination import (
        add_pagination_headers,
        paged_response as _paged_response,
    )
    from varco_core.dto.pagination import PagedReadDTO

    response_model = route.response_model
    # Read list config from router ClassVars (with defaults)
    max_limit = getattr(type(router), "_list_max_limit", 100)
    default_limit = getattr(type(router), "_list_default_limit", 50)
    include_total = getattr(type(router), "_list_include_total", True)

    http_dep = Depends(_http_query_params)
    auth_dep = Depends(server_auth) if server_auth is not None else None
    async_dep = Depends(_async_mode_params)

    if auth_dep is not None:

        async def list_handler(
            request: Request,
            response: Response,
            http_params: HttpQueryParams = http_dep,
            auth: AuthContext = auth_dep,
            async_params: AsyncModeParams = async_dep,
        ) -> Any:
            params = http_params.to_query_params(
                max_limit=max_limit,
                default_limit=default_limit,
            )
            items = await service.list(params, auth)
            total = None
            if include_total:
                try:
                    total = await service.count(params, auth)
                except AttributeError:
                    # Service may not implement count() — degrade gracefully
                    pass
            page = _paged_response(
                items, params=params, total_count=total, raw_query=http_params.q
            )
            add_pagination_headers(response, page, request)
            return page

    else:

        async def list_handler(  # type: ignore[misc]
            request: Request,
            response: Response,
            http_params: HttpQueryParams = http_dep,
            async_params: AsyncModeParams = async_dep,
        ) -> Any:
            params = http_params.to_query_params(
                max_limit=max_limit,
                default_limit=default_limit,
            )
            items = await service.list(params)
            total = None
            if include_total:
                try:
                    total = await service.count(params)
                except AttributeError:
                    pass
            page = _paged_response(
                items, params=params, total_count=total, raw_query=http_params.q
            )
            add_pagination_headers(response, page, request)
            return page

    list_handler.__name__ = "list"
    if response_model is not None:
        # Wrap in PagedReadDTO for correct OpenAPI schema
        list_handler.__annotations__["return"] = PagedReadDTO[response_model]  # type: ignore[valid-type]
    return list_handler


# Registry of CRUD action name → handler factory function
_CRUD_HANDLER_FACTORIES: dict[str, Any] = {
    "create": _make_create_handler,
    "read": _make_read_handler,
    "update": _make_update_handler,
    "patch": _make_patch_handler,
    "delete": _make_delete_handler,
    "list": _make_list_handler,
}


# ── Custom @route handler factory ─────────────────────────────────────────────


def _make_custom_handler(
    *,
    router_instance: VarcoRouter,  # type: ignore[type-arg]
    method_fn: Any,
    route: Any,
    server_auth: Any,
    job_runner: Any,
) -> Any:
    """
    Create an endpoint closure for a method decorated with ``@route``.

    The method is expected to accept ``(self, ctx: AuthContext, **path_params)``
    or any subset thereof.  The closure injects auth and, when
    ``route.async_capable=True`` and ``?with_async=true`` is present, offloads
    the call to the job runner instead of executing it inline.

    Args:
        router_instance: The ``VarcoRouter`` instance (captures ``self``).
        method_fn:       The unbound method from the router class.
        route:           The ``ResolvedRoute`` describing the endpoint.
        server_auth:     Auth strategy to inject, or ``None``.
        job_runner:      Job runner for async offload, or ``None``.

    Returns:
        An async callable suitable for ``APIRouter.add_api_route()``.

    DESIGN: read with_async from request.query_params directly (not Depends())
        ✅ Avoids registering an internal Depends() that leaks into OpenAPI schema.
        ✅ One step toward removing FastAPI DI from framework internals — the
           application should control its own DI (providify) not the framework.
        ❌ Does not benefit from FastAPI's Query() validation (accepts any truthy
           string — "true", "1", "yes") — acceptable for an internal flag.

    Edge cases:
        - Method is not async → TypeError raised at call time, not build time.
        - ``route.async_capable=False`` → ``?with_async=true`` is silently ignored.
        - ``job_runner=None`` → async mode unavailable; falls back to inline execution.
        - ``with_async=true`` but job store raises → HTTP 500 propagated by ErrorMiddleware.
    """
    import inspect

    auth_dep = Depends(server_auth) if server_auth is not None else None
    sig = inspect.signature(method_fn)
    param_names = set(sig.parameters.keys()) - {"self"}

    # Whether this route can be offloaded — checked once at build time.
    # job_runner may be None even when async_capable=True (misconfigured app).
    can_offload = route.async_capable and job_runner is not None

    # DESIGN: no **kwargs in handler signature — FastAPI doesn't understand them
    #   ✅ FastAPI only injects declared parameters (path, query, body, Depends)
    #   ❌ Path param names/types must match the method's expectations; type is Any
    if auth_dep is not None:

        async def custom_handler(
            request: Request,
            auth: AuthContext = auth_dep,
        ) -> Any:
            # Read with_async from query string directly — avoids internal Depends()
            with_async = (
                request.query_params.get("with_async", "false").lower() == "true"
                if can_offload
                else False
            )

            # Build call kwargs now — these come from this request context and
            # must be captured before the coroutine is handed to the job runner.
            call_kwargs: dict[str, Any] = {}
            for _name in ("ctx", "auth", "context"):
                if _name in param_names:
                    call_kwargs[_name] = auth
            for _name in route.path_params:
                if _name in request.path_params:
                    call_kwargs[_name] = request.path_params[_name]

            if with_async:
                callback_url = request.query_params.get("callback_url")
                # Capture call_kwargs in a zero-arg coroutine function.
                # _submit_job calls coro_fn(*args) with no args, so coro_fn
                # must be a parameterless callable that closes over call_kwargs.
                captured = dict(call_kwargs)

                async def _bound_call_with_auth() -> Any:
                    return await method_fn(router_instance, **captured)

                return await _submit_job(
                    router_instance,
                    job_runner,
                    auth,
                    _bound_call_with_auth,
                    callback_url=callback_url,
                )

            # Synchronous path — execute inline
            return await method_fn(router_instance, **call_kwargs)

    else:

        async def custom_handler(request: Request) -> Any:  # type: ignore[misc]
            with_async = (
                request.query_params.get("with_async", "false").lower() == "true"
                if can_offload
                else False
            )

            call_kwargs = {}
            for _name in route.path_params:
                if _name in request.path_params:
                    call_kwargs[_name] = request.path_params[_name]

            if with_async:
                callback_url = request.query_params.get("callback_url")
                captured = dict(call_kwargs)

                async def _bound_call_no_auth() -> Any:
                    return await method_fn(router_instance, **captured)

                return await _submit_job(
                    router_instance,
                    job_runner,
                    None,
                    _bound_call_no_auth,
                    callback_url=callback_url,
                )

            return await method_fn(router_instance, **call_kwargs)

    custom_handler.__name__ = route.name
    return custom_handler


# ── Noop handler (fallback for missing methods) ────────────────────────────────


def _make_noop_handler(route: Any) -> Any:
    """
    Fallback endpoint that returns 501 Not Implemented.

    Used when ``build_router()`` finds a ``ResolvedRoute`` but no corresponding
    method on the router class.  This prevents the app from crashing while giving
    a useful error to API consumers.
    """
    from fastapi import HTTPException

    route_name = route.name

    async def noop(request: Request) -> Any:
        raise HTTPException(
            status_code=501,
            detail=f"Route {route_name!r} is declared but not implemented on this router.",
        )

    noop.__name__ = route_name
    return noop


# ── Background job submission helper ──────────────────────────────────────────


async def _submit_job(
    router: VarcoRouter,  # type: ignore[type-arg]
    job_runner: Any,
    auth: AuthContext | None,
    coro_fn: Any,
    *args: Any,
    callback_url: str | None = None,
) -> Any:
    """
    Persist a ``Job`` to the store and schedule the coroutine for background execution.

    Uses ``job_runner.enqueue()`` — not the lower-level ``submit()`` — so the job
    is durably written to the store *before* the asyncio.Task is created.  This
    ensures a process crash between submission and execution leaves a recoverable
    PENDING record rather than silently losing the request.

    Auth capture: the ``AuthContext`` is serialized to a plain ``dict`` snapshot
    because the live object is tied to a ContextVar that is reset after the HTTP
    request ends.  Background tasks run in a different async context and must
    restore auth from the snapshot via ``auth_context_from_snapshot()``.

    DESIGN: enqueue() over submit() here
        ✅ Job is persisted atomically before the task starts — crash-safe.
        ✅ JobPoller can find PENDING jobs that never transitioned to RUNNING.
        ❌ Two writes per submission (save PENDING + save RUNNING in _run_job)
           rather than one — acceptable overhead for the durability guarantee.

    Args:
        router:       The router instance (ClassVar access if needed).
        job_runner:   The ``AbstractJobRunner`` to enqueue on.
        auth:         The ``AuthContext`` from the current request; ``None`` for
                      unauthenticated routes.
        coro_fn:      The async callable (e.g. a service method) to execute.
        *args:        Positional arguments forwarded to ``coro_fn``.
        callback_url: Optional webhook URL to POST the result to on completion.

    Returns:
        A ``JobAcceptedResponse`` (HTTP 202) with the newly created ``job_id``.

    Raises:
        Any store-specific error from ``job_runner.enqueue()``; propagates as
        HTTP 500 via the error middleware.

    Edge cases:
        - ``auth=None`` → ``auth_snapshot=None``; background task has no auth context.
        - Store unavailable → ``enqueue()`` raises; caller gets HTTP 500.
        - Crash after enqueue save but before task runs → job stays PENDING;
          ``JobPoller`` marks it FAILED on the next recovery pass.
    """
    from uuid import uuid4

    from varco_core.job.base import Job, auth_context_to_snapshot
    from varco_fastapi.context import request_token_var
    from varco_fastapi.job.response import JobAcceptedResponse

    job_id = uuid4()

    # Serialize auth context to a plain dict — frozenset fields in AuthContext
    # are not JSON-safe and the live object cannot survive past this request.
    auth_snapshot = auth_context_to_snapshot(auth) if auth is not None else None

    # Capture raw Bearer token for webhook callback authentication
    raw_token = request_token_var.get(None)

    job = Job(
        job_id=job_id,
        auth_snapshot=auth_snapshot,
        request_token=raw_token,
        callback_url=callback_url,
    )

    # Closure captures coro_fn and args — the coroutine is not created until
    # the runner is ready, avoiding "coroutine was never awaited" if enqueue fails.
    async def _run() -> Any:
        return await coro_fn(*args)

    # enqueue() = store.save(job) THEN asyncio.create_task — order is critical.
    await job_runner.enqueue(job, _run())
    return JobAcceptedResponse(job_id=job_id)


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "RouterMixin",
    "VarcoRouter",
    "AsyncModeParams",
    "HttpQueryParams",
    "_async_mode_params",
    "_http_query_params",
    # Internal factory helpers — imported by VarcoCRUDRouter in crud.py
    "_make_create_handler",
    "_make_read_handler",
    "_make_update_handler",
    "_make_patch_handler",
    "_make_delete_handler",
    "_make_list_handler",
    "_make_custom_handler",
    "_make_noop_handler",
    "_CRUD_HANDLER_FACTORIES",
    "_submit_job",
]
