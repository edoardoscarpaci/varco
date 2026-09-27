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

import inspect
import logging
from abc import ABC
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Final,
    Generic,
    get_args,
    get_origin,
    get_type_hints,
)

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import Response
from lark import LarkError
from providify import Instance
from varco_core.auth.base import AuthContext
from varco_core.exception import ServiceValidationError
from varco_core.job import AbstractJobRunner
from varco_core.query import QueryParams, QueryParser, SortField, SortOrder
from varco_core.service.base import PK, C, D, R, U

from varco_fastapi.router.introspection import introspect_routes

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


# ── Filter parse error helper ──────────────────────────────────────────────────


def _format_parse_error(exc: Exception) -> str:
    """Translate a raw LarkError into a developer-friendly message.

    Lark's default error text is terse and cryptic for API consumers.  This
    helper rewrites the most common mistakes into actionable guidance while
    preserving the raw detail in the trailing ``Details:`` suffix.

    Args:
        exc: The exception raised by the Lark parser (typically
             ``UnexpectedCharacters``, ``UnexpectedEOF``, or
             ``UnexpectedToken``).

    Returns:
        A human-readable error string suitable for a ``ServiceValidationError``
        message that maps to HTTP 422.

    Edge cases:
        - Unknown error shapes fall back to the generic
          ``"Invalid filter expression: ..."`` message.
        - The raw Lark message is always appended after ``Details:`` so
          developers can still diagnose novel grammar failures.

    Thread safety:  ✅ Pure function — no shared state.
    Async safety:   ✅ Synchronous; safe to call from async contexts.
    """
    msg = str(exc)
    if "'" in msg and ("No terminal matches" in msg or "Unexpected" in msg):
        return (
            "Filter syntax error: use double-quotes for string values "
            f"(e.g. name = \"Alice\", not name = 'Alice'). Details: {msg}"
        )
    if "UnexpectedEOF" in type(exc).__name__ or "Unexpected end" in msg:
        return f"Filter syntax error: incomplete expression. Details: {msg}"
    return f"Invalid filter expression: {msg}"


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
    with_async: bool = Query(False, alias="with_async", description="Offload to background job"),
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
        - Malformed ``q`` string → ``to_query_params()`` raises ``ServiceValidationError``;
          caught by ``ErrorMiddleware`` and mapped to HTTP 422.
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
            ServiceValidationError: Syntax error in ``self.q`` filter string
                                    (wraps any ``lark.LarkError``).  Maps to HTTP 422
                                    via ``ErrorMiddleware``.
            OperationNotFound:      Unknown operator in ``self.q`` filter string.

        Edge cases:
            - ``q=None`` → node is ``None`` (no filter applied)
            - ``q=""`` → raises ``ServiceValidationError`` (empty expression is invalid grammar)
            - ``sort=None`` → empty list (database default order preserved)
            - ``limit`` is clamped: ``min(self.limit or default_limit, max_limit)``
            - ``offset`` defaults to 0 when ``None``
        """
        # Parse filter expression into AST — None means "no WHERE clause".
        # Wrap any Lark parse error as ServiceValidationError so ErrorMiddleware
        # maps it to HTTP 422 instead of leaking a 500 Internal Server Error.
        # LarkError is the common base for UnexpectedCharacters, UnexpectedEOF,
        # and UnexpectedToken — catching it covers all grammar-level parse failures.
        try:
            node = QueryParser().parse(self.q) if self.q is not None else None
        except LarkError as exc:
            raise ServiceValidationError(_format_parse_error(exc)) from exc

        # Parse sort string — empty list means "no ORDER BY"
        sort = _parse_sort_string(self.sort) if self.sort is not None else []

        # Clamp limit to prevent accidentally fetching millions of rows
        effective_limit = min(self.limit if self.limit is not None else default_limit, max_limit)

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
        - ``VarcoCRUDRouter[D, PK, C, R, U, S]`` (Plan 001's 6th ``S`` TypeVar,
          the concrete service type) → the trailing ``S`` arg is dropped; CRUD
          model resolution (task payload (de)serialization, PK coercion, etc.)
          always receives exactly the 5 model args ``(D, PK, C, R, U)``.
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
            if len(args) == 6:
                # VarcoCRUDRouter (and the CRUD presets) carry an optional 6th
                # `S` type arg — the concrete service type (Plan 001). It is a
                # static-narrowing seam only; it must never leak into CRUD
                # model resolution, so drop it here rather than at every
                # call site.
                from varco_fastapi.router.crud import VarcoCRUDRouter

                if issubclass(origin, VarcoCRUDRouter):
                    return args[:5]
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
        _store_raw_token: Plan 005 Phase 6 (U-19). Whether ``_submit_job()``'s
                     auto-populated ``Job`` carries the raw Bearer token
                     (``True``, default — required for the completion
                     callback's ``Authorization: Bearer`` forwarding) or only
                     its non-reversible reference fields (``False`` — pair
                     with a service-credential-authenticated callback).

    Service-backed usage (CRUD router)::

        class OrderRouter(CreateMixin, ReadMixin, ListMixin,
                          VarcoRouter[Order, UUID, OrderCreateDTO, OrderReadDTO, OrderUpdateDTO]):
            _prefix = "/orders"
            _tags = ["orders"]
            _service = container.get(OrderService)
            _auth = container.get(JwtBearerAuth)

        router = OrderRouter().build_router()
        app.include_router(router)

    Service-free usage (data-processing / proxy / computed endpoints):

        Use ``GenericRouter`` (a plain alias for ``VarcoRouter`` with no type args)
        when the server has no ``AsyncService`` or repository behind it — for example
        a data-transformation pipeline, an API gateway, or computed analytics routes.
        All cross-cutting features still apply: middleware, telemetry, auth, and
        ``RouteGuard`` authorization are identical to the service-backed path.

        from varco_fastapi.router.presets import GenericRouter
        from varco_fastapi.router.endpoint import route
        from varco_fastapi.auth.guard import require_scopes

        class ReportRouter(GenericRouter):
            _prefix = "/reports"
            _auth = JwtBearerAuth(...)

            @route("GET", "/summary", requires=require_scopes("reports:read"))
            async def get_summary(self, ctx: AuthContext) -> dict:
                return {"total": compute_total()}

            @route("POST", "/refresh", requires=require_roles("admin"))
            async def refresh_cache(self, ctx: AuthContext) -> None:
                ...

        Validation notes for service-free routers:
        - ``_prefix`` is still required.
        - At least one ``@route`` method is still required.
        - Generic type args ``[D, PK, C, R, U]`` are NOT required (and
          ``validate_router_class`` will not warn about them being absent).
        - Setting ``requires=`` without ``_auth`` raises ``RuntimeError``
          at ``build_router()`` time (unless ``allow_anonymous()`` guard is used).

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

    # Plan 005 Phase 6 (U-19) — controls whether _submit_job()'s
    # auto-populated Job carries the raw Bearer token or only its
    # reference fields (request_issuer/request_subject/request_token_hash).
    # DESIGN: default True over default False
    #   ✅ JobRunner._fire_callback() forwards job.request_token as the
    #      completion callback's `Authorization: Bearer` header — flipping
    #      the default to False would silently break callback auth for
    #      every existing async-capable route (Source correction 4).
    #   ✅ Setting this False on a router is an explicit, auditable opt-in —
    #      it also removes a token-replay surface, but requires the
    #      callback receiver to authenticate the request some other way
    #      (a service credential / mTLS / signed callback URL), since the
    #      caller's own token is never forwarded once this is False.
    #   ❌ A router that sets this False and relies on `request_token`-based
    #      callback auth will see 401s on its own webhook receiver — this
    #      is intentional (a token-replay surface removed on purpose), but
    #      it must be paired with a callback-side auth change.
    _store_raw_token: ClassVar[bool] = True

    def __repr__(self) -> str:
        return f"{type(self).__name__}(prefix={self._effective_prefix()!r}, tags={self._tags!r})"

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
                        ``VarcoFastAPIConfiguration`` is installed; pass directly in
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

        # Build-time safety: a RouteGuard without an auth strategy can never be
        # satisfied (there is no AuthContext to check).  Fail loudly so the
        # misconfiguration is caught at startup, not silently at request time.
        server_auth_present = getattr(self, "_auth", None) is not None
        if not server_auth_present:
            for r in routes:
                if r.requires is not None and not r.requires.allow_anonymous:
                    raise RuntimeError(
                        f"{router_cls.__name__}.{r.name}: route has `requires` guard "
                        f"but the router has no `_auth` strategy set.  Either set "
                        f"`_auth` on the router or use `allow_anonymous()` guard."
                    )

        api_router = APIRouter(prefix=prefix, tags=tags)  # type: ignore[arg-type]

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

        # Response-model handling differs between CRUD and custom routes:
        #   - CRUD routes declare the model explicitly (the R type).  "list" is set
        #     via handler.__annotations__["return"] (wrapped in PagedReadDTO[R]), so
        #     we pass None here — passing raw R would make FastAPI validate the paged
        #     wrapper against R directly.
        #   - Custom @route handlers let FastAPI INFER the model from the synthesized
        #     return annotation.  We must NOT pass an explicit None: FastAPI only
        #     infers when the argument is its Default() sentinel, never for a literal
        #     None (which means "no response model").
        # A custom route can actually offload only when async_capable AND a job
        # runner is wired — async_capable=True is the @route default, so it alone
        # doesn't mean the route ever returns a JobAcceptedResponse.
        can_offload = route.async_capable and getattr(self, "_job_runner", None) is not None

        route_kwargs: dict[str, Any] = {}
        if route.is_crud:
            route_kwargs["response_model"] = (
                None if route.crud_action == "list" else route.response_model
            )
        elif can_offload:
            # Offloadable custom routes may return EITHER the handler's own return
            # type OR a JobAcceptedResponse (when ?with_async=true).  Suppress
            # response_model inference so the polymorphic offload response is not
            # validated against the inline return type.
            route_kwargs["response_model"] = None
        elif route.response_model is not None:
            route_kwargs["response_model"] = route.response_model
        # else: plain custom route → FastAPI infers the model from the return type.

        api_router.add_api_route(
            path=route.path,
            endpoint=handler,
            methods=[method],
            status_code=route.status_code,
            summary=route.summary,
            description=route.description,
            tags=list(route.tags) if route.tags else None,
            deprecated=route.deprecated,
            responses=responses,
            dependencies=dependencies,
            operation_id=operation_id,
            response_description=response_description,
            include_in_schema=include_in_schema,
            **route_kwargs,
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
            logger.warning("VarcoRouter: no method %r found for route %r", method_name, route.path)
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
    router: VarcoRouter[Any, Any, Any, Any, Any],
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
                return await _submit_job(router, job_runner, auth, service.create, body, auth)
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
    router: VarcoRouter[Any, Any, Any, Any, Any],
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
    router: VarcoRouter[Any, Any, Any, Any, Any],
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

        async def update_handler(id: Any, body: Any, auth: AuthContext = auth_dep) -> Any:
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
    router: VarcoRouter[Any, Any, Any, Any, Any],
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

        async def patch_handler(id: Any, body: Any, auth: AuthContext = auth_dep) -> Any:
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
    router: VarcoRouter[Any, Any, Any, Any, Any],
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
    router: VarcoRouter[Any, Any, Any, Any, Any],
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
    from varco_core.dto.pagination import PagedReadDTO

    from varco_fastapi.router.pagination import (
        add_pagination_headers,
    )
    from varco_fastapi.router.pagination import (
        paged_response as _paged_response,
    )

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
            page = _paged_response(items, params=params, total_count=total, raw_query=http_params.q)
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
            page = _paged_response(items, params=params, total_count=total, raw_query=http_params.q)
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


# Parameter names on a ``@route`` handler that receive the request's
# ``AuthContext`` — kept for backward compatibility with the historical
# "ctx/auth/context is magically injected" contract.
_AUTH_PARAM_NAMES: Final[tuple[str, ...]] = ("ctx", "auth", "context")

# Names of the framework-only parameters we synthesize onto the FastAPI-visible
# signature when the handler itself does not declare them.  They are stripped
# from the kwargs before the user's method is invoked.  Leading dunders keep
# them clear of realistic user parameter names; they are plain dict keys /
# ``Parameter`` names, so Python name-mangling does not apply.
_HIDDEN_AUTH_PARAM: Final[str] = "__varco_auth"
_HIDDEN_REQUEST_PARAM: Final[str] = "__varco_request"


@dataclass(frozen=True)
class _CustomHandlerSig:
    """
    Synthesized FastAPI-visible signature for a custom ``@route`` handler.

    Attributes:
        signature:     The ``inspect.Signature`` FastAPI introspects — mirrors the
                       user's method params (minus ``self``) so FastAPI parses
                       ``Query``/``Body``/``Depends``/``Request``/path params natively.
        auth_kwarg:    Key in the resolved kwargs carrying the ``AuthContext`` (used
                       for the ``RouteGuard`` and the offload snapshot); ``None`` when
                       the router has no ``_auth``.
        request_kwarg: Key carrying the ``Request`` (used to read the ``?with_async``
                       offload flag); ``None`` when no request is available.
        hidden_kwargs: Keys we injected ourselves — stripped before the user's method
                       is called so they never leak into the handler signature.
    """

    signature: inspect.Signature
    auth_kwarg: str | None
    request_kwarg: str | None
    hidden_kwargs: frozenset[str]


def _resolve_handler_hints(method_fn: Any) -> dict[str, Any]:
    """
    Resolve a handler's annotations to runtime objects.

    Every router file uses ``from __future__ import annotations``, so a handler's
    raw annotations are **strings**.  FastAPI needs real objects (Pydantic models,
    ``int``, ``Request`` …) and evaluates string annotations against the *endpoint
    callable's* ``__globals__`` — which for our synthesized wrapper is this module,
    not the user's.  We therefore resolve here, against the user method's own
    module, and hand FastAPI real objects.

    Args:
        method_fn: The unbound router method decorated with ``@route``.

    Returns:
        A ``name -> type`` map (including the ``"return"`` key).  Names whose
        annotation cannot be resolved are omitted so the caller falls back to ``Any``.

    Edge cases:
        - A handler with no annotations                → empty-ish map, all fall back to ``Any``.
        - An annotation referencing a ``TYPE_CHECKING``-only import → resolution
          raises; we log at debug and return ``{}`` (that param degrades to ``Any``).
    """
    try:
        # include_extras keeps ``Annotated[int, Query()]`` metadata intact for FastAPI.
        return get_type_hints(method_fn, include_extras=True)
    except Exception as exc:  # noqa: BLE001
        # DESIGN: broad catch on purpose — a single unresolvable annotation must not
        # abort building the whole router.  The offending param simply degrades to
        # ``Any`` (FastAPI treats it as an optional query param) rather than crashing.
        logger.debug(
            "Could not resolve type hints for %r (%s); params degrade to Any",
            getattr(method_fn, "__qualname__", method_fn),
            exc,
        )
        return {}


def _synthesize_custom_signature(
    method_fn: Any,
    server_auth: Any,
    *,
    can_offload: bool,
) -> _CustomHandlerSig:
    """
    Build the FastAPI-visible signature for a custom ``@route`` handler.

    The synthesized signature mirrors the user's method parameters (minus ``self``)
    so FastAPI's dependency resolver parses, validates and coerces everything —
    ``Query``/``Body``/``Depends``/``Request``/``Response`` and type-coerced path
    params — exactly as it would for a hand-written endpoint.  Two framework-only
    parameters are added when needed:

    - an ``AuthContext = Depends(server_auth)`` param, so the ``RouteGuard`` and the
      async-offload snapshot always have the request's auth even when the handler
      does not declare ``ctx``/``auth``/``context``;
    - a ``Request`` param, so the wrapper can still read ``?with_async`` when the
      route can offload and the handler did not ask for a ``Request`` itself.

    Args:
        method_fn:   The unbound router method decorated with ``@route``.
        server_auth: The router's auth strategy, or ``None``.
        can_offload: Whether this route supports background offload (needs a Request).

    Returns:
        A ``_CustomHandlerSig`` describing the signature and how to route kwargs.

    Edge cases:
        - Handler declares ``ctx`` but ``server_auth is None`` → the param is dropped
          entirely (preserves the historical no-inject behavior; the route still
          builds, matching the pre-existing contract).
        - Handler declares its own ``Request`` param → reused for offload; no hidden
          Request is added, and it flows through to the handler.
        - Multiple auth-named params → only the first receives the ``AuthContext``.
    """
    P = inspect.Parameter
    raw_sig = inspect.signature(method_fn)
    hints = _resolve_handler_hints(method_fn)
    # Shared Depends object — same pattern the CRUD factories use for auth.
    auth_dep = Depends(server_auth) if server_auth is not None else None

    params: list[inspect.Parameter] = []
    auth_kwarg: str | None = None
    request_kwarg: str | None = None
    hidden: set[str] = set()

    for name, p in raw_sig.parameters.items():
        if name == "self":
            continue

        annotation = hints.get(name, p.annotation)
        if annotation is P.empty:
            annotation = Any

        # Auth-named param → fed from the router's _auth via Depends(server_auth),
        # keeping the historical ctx/auth/context injection contract.
        if name in _AUTH_PARAM_NAMES and auth_kwarg is None:
            if auth_dep is not None:
                auth_kwarg = name  # flows through to the user's method under its own name
                params.append(
                    P(
                        name,
                        kind=P.KEYWORD_ONLY,
                        default=auth_dep,
                        annotation=AuthContext,
                    )
                )
            # server_auth is None → drop it (preserves prior no-inject behavior).
            continue

        # Note a user-declared Request so we can read the offload flag from it.
        if annotation is Request and request_kwarg is None:
            request_kwarg = name

        # Pass-through: keep the user's default (Query()/Body()/Depends()/…) and type
        # so FastAPI resolves them natively.  All KEYWORD_ONLY — sidesteps the
        # "non-default before default" ordering rule and lets us call purely by kwargs.
        default = p.default if p.default is not P.empty else P.empty
        params.append(P(name, kind=P.KEYWORD_ONLY, default=default, annotation=annotation))

    # Router has _auth but the handler didn't declare ctx/auth/context — still resolve
    # the AuthContext (hidden) for the RouteGuard and the offload snapshot.
    if auth_dep is not None and auth_kwarg is None:
        auth_kwarg = _HIDDEN_AUTH_PARAM
        hidden.add(_HIDDEN_AUTH_PARAM)
        params.append(
            P(
                _HIDDEN_AUTH_PARAM,
                kind=P.KEYWORD_ONLY,
                default=auth_dep,
                annotation=AuthContext,
            )
        )

    # Offload needs a Request to read ?with_async / ?callback_url.  Add a hidden one
    # only when the route can offload and the handler didn't already ask for a Request.
    if can_offload and request_kwarg is None:
        request_kwarg = _HIDDEN_REQUEST_PARAM
        hidden.add(_HIDDEN_REQUEST_PARAM)
        params.append(
            P(
                _HIDDEN_REQUEST_PARAM,
                kind=P.KEYWORD_ONLY,
                default=P.empty,
                annotation=Request,
            )
        )

    # Return annotation drives FastAPI's response_model / OpenAPI schema.
    ret = hints.get("return", raw_sig.return_annotation)
    signature = inspect.Signature(params, return_annotation=ret)
    return _CustomHandlerSig(signature, auth_kwarg, request_kwarg, frozenset(hidden))


def _make_custom_handler(
    *,
    router_instance: VarcoRouter[Any, Any, Any, Any, Any],
    method_fn: Any,
    route: Any,
    server_auth: Any,
    job_runner: Any,
) -> Any:
    """
    Create an endpoint closure for a method decorated with ``@route``.

    The handler may declare any parameters a normal FastAPI endpoint can —
    ``Query(...)``, ``Body(...)`` (Pydantic models), ``Depends(...)``, ``Request``,
    ``Response``, ``BackgroundTasks`` and **type-coerced** path params.  The wrapper
    synthesizes a matching ``__signature__`` (see :func:`_synthesize_custom_signature`)
    so FastAPI's own dependency resolver parses and injects them.  On top of that it
    preserves varco's contract: ``ctx``/``auth``/``context`` receive the router's
    ``AuthContext``, any ``RouteGuard`` runs before the body, and when
    ``route.async_capable`` and ``?with_async=true`` the call is offloaded to the job
    runner.

    Args:
        router_instance: The ``VarcoRouter`` instance (captures ``self``).
        method_fn:       The unbound method from the router class.
        route:           The ``ResolvedRoute`` describing the endpoint.
        server_auth:     Auth strategy to inject, or ``None``.
        job_runner:      Job runner for async offload, or ``None``.

    Returns:
        An async callable (with an overridden ``__signature__``) suitable for
        ``APIRouter.add_api_route()``.

    DESIGN: synthesize the handler signature from the user's method
        ✅ Full FastAPI parity — Query/Body/Depends/Request + typed path params work.
        ✅ Reuses FastAPI's validation and OpenAPI schema generation for free.
        ✅ Backward compatible — ctx injection, guards and offload are unchanged.
        ❌ Requires resolving string annotations against the user's module and
           overriding ``__signature__`` — a little reflection machinery.

    Edge cases:
        - Method is not async → TypeError raised at call time, not build time.
        - ``route.async_capable=False`` → ``?with_async=true`` is silently ignored.
        - ``job_runner=None`` → async mode unavailable; falls back to inline execution.
        - ``with_async=true`` but job store raises → HTTP 500 propagated by ErrorMiddleware.
        - Handler declares ``ctx`` but router has no ``_auth`` → param dropped; calling
          the route raises (missing arg) exactly as before — set ``_auth`` to fix.
    """
    # Whether this route can be offloaded — checked once at build time.
    # job_runner may be None even when async_capable=True (misconfigured app).
    can_offload = route.async_capable and job_runner is not None
    guard = route.requires

    spec = _synthesize_custom_signature(method_fn, server_auth, can_offload=can_offload)
    auth_kwarg = spec.auth_kwarg
    request_kwarg = spec.request_kwarg
    hidden = spec.hidden_kwargs

    async def custom_handler(**kwargs: Any) -> Any:
        # FastAPI passes every resolved param by keyword (all synthesized params are
        # KEYWORD_ONLY), so **kwargs captures the full, validated, coerced argument set.
        auth = kwargs.get(auth_kwarg) if auth_kwarg is not None else None

        # Route-level authorization — checked before the handler body runs.
        if guard is not None:
            await guard.check(auth)

        request = kwargs.get(request_kwarg) if request_kwarg is not None else None
        with_async = (
            can_offload
            and request is not None
            and request.query_params.get("with_async", "false").lower() == "true"
        )

        # Args for the user's method: everything FastAPI resolved minus the
        # framework-only params we injected for auth/offload.
        call_kwargs = {k: v for k, v in kwargs.items() if k not in hidden}

        if with_async:
            callback_url = request.query_params.get("callback_url")  # type: ignore[union-attr]
            # _submit_job calls coro_fn() with no args, so close over the captured
            # kwargs in a parameterless coroutine.
            captured = dict(call_kwargs)

            async def _bound_call() -> Any:
                return await method_fn(router_instance, **captured)

            return await _submit_job(
                router_instance,
                job_runner,
                auth,
                _bound_call,
                callback_url=callback_url,
            )

        # Synchronous path — execute inline.
        return await method_fn(router_instance, **call_kwargs)

    custom_handler.__name__ = route.name
    custom_handler.__doc__ = getattr(method_fn, "__doc__", None)
    # FastAPI introspects THIS synthesized signature to build its dependency model.
    custom_handler.__signature__ = spec.signature  # type: ignore[attr-defined]
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
    router: VarcoRouter[Any, Any, Any, Any, Any],
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

    # Plan 005 Phase 6 (U-19) — opt-out via a router ClassVar. Default True:
    # forwarding job.request_token as the callback's Authorization: Bearer
    # header (see JobRunner._fire_callback) is the existing behaviour and
    # must not silently change for routers that don't set this. Setting it
    # False requires the callback to authenticate with a service credential
    # instead of replaying the caller's token — see VarcoRouter._store_raw_token.
    store_raw_token = getattr(router, "_store_raw_token", True)

    job = Job(
        job_id=job_id,
        auth_snapshot=auth_snapshot,
        request_token=raw_token,
        callback_url=callback_url,
        store_raw_token=store_raw_token,
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
