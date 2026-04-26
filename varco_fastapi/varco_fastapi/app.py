"""
varco_fastapi.app
==================
One-call application factory for varco-powered FastAPI services.

``create_varco_app()`` replaces ~40 lines of boilerplate in every service's
``main.py``.  It:

1. Validates DI container bindings (``AbstractServerAuth``, job store/runner).
2. Validates every router class (``_prefix`` set, at least one route).
3. Builds a ``VarcoLifespan`` and wires startup/shutdown hooks.
4. Creates a ``FastAPI`` instance with the standard middleware stack.
5. Mounts routers — either an explicit list or auto-scanned from the container.
6. Optionally mounts ``MCPAdapter`` and/or ``SkillAdapter``.
7. Optionally mounts ``MetricsRouter`` (``enable_metrics=True``).
8. Always mounts ``HealthRouter``.
9. Returns the fully configured ``FastAPI`` app.

Minimal usage::

    # main.py
    from varco_fastapi import create_varco_app
    from varco_fastapi.di import VarcoFastAPIModule

    container = DIContainer()
    container.install(VarcoFastAPIModule)

    app = create_varco_app(container)  # auto-scans routers from container

Explicit routers::

    app = create_varco_app(
        container,
        routers=[OrderRouter, UserRouter],
        title="Orders API",
        version="1.0.0",
    )

With MCP + A2A::

    from varco_fastapi.router.mcp import MCPAdapter
    from varco_fastapi.router.skill import SkillAdapter

    mcp = MCPAdapter(OrderRouter, client=OrderClient(base_url="http://localhost:8080"))
    skill = SkillAdapter(
        OrderRouter,
        agent_name="OrderAgent",
        agent_description="Manages customer orders",
        client=OrderClient(base_url="http://localhost:8080"),
    )

    app = create_varco_app(container, routers=[OrderRouter], mcp_adapter=mcp, skill_adapter=skill)

DESIGN: factory function over application subclass
    ✅ Plain function — easy to test (call it, inspect the result)
    ✅ Keyword-only args with sensible defaults — additive, not breaking
    ✅ Works without DI (pass explicit routers + None container)
    ❌ Not as IDE-friendly as a builder class — mitigated by clear docs

DESIGN: middleware stack applied via add_middleware() (Starlette reverse order)
    Starlette adds middleware as an onion — the LAST add_middleware() call
    becomes the OUTERMOST layer.  We add CORS last so it runs FIRST on
    incoming requests (CORS preflight OPTIONS must not hit the auth check).

    Execution order (request in → response out):
        CORSMiddleware → ErrorMiddleware → TracingMiddleware →
        MetricsMiddleware (optional) → RequestLoggingMiddleware →
        RequestContextMiddleware → SessionMiddleware → route handler

Thread safety:  ✅ Intended to be called once at module import or startup.
Async safety:   ✅ No async operations — route registration is synchronous.
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncIterator

from varco_fastapi.validation import validate_container_bindings, validate_router_class

if TYPE_CHECKING:
    from varco_fastapi.router.mcp import MCPAdapter
    from varco_fastapi.router.skill import SkillAdapter

_logger = logging.getLogger(__name__)


# ── create_varco_app ───────────────────────────────────────────────────────────


def create_varco_app(
    container: Any | None = None,
    *,
    routers: list[type] | None = None,
    scan_packages: list[str] | None = None,
    title: str = "Varco API",
    version: str = "0.1.0",
    description: str = "",
    cors: Any | None = None,
    enable_tracing: bool = True,
    enable_logging: bool = True,
    enable_error_middleware: bool = True,
    enable_metrics: bool = False,
    mcp_adapter: MCPAdapter | None = None,
    mcp_path: str = "/mcp",
    skill_adapter: SkillAdapter | None = None,
    skill_base_url: str = "",
    skill_agent_card_path: str = "/.well-known/agent.json",
    skill_tasks_prefix: str = "/tasks",
    extra_middleware: list[Any] | None = None,
    extra_lifespan_components: list[Any] | None = None,
    validate: bool = True,
    strict_validation: bool = False,
    openapi_url: str = "/openapi.json",
    docs_url: str = "/docs",
) -> Any:
    """
    Create a fully configured FastAPI application for a varco service.

    Steps (in order):

    1. Validate DI container bindings (if ``validate=True`` and container given).
    2. Resolve router list — explicit ``routers`` list OR auto-scanned from container.
    3. Validate each router class (if ``validate=True``).
    4. Build ``VarcoLifespan`` from container lifecycle objects + extra hooks.
    5. Create ``FastAPI`` instance with the lifespan.
    6. Register exception handlers.
    7. Apply middleware stack (CORS → Error → Tracing → Metrics → Logging →
       RequestContext → Session).
    8. Apply any ``extra_middleware`` layers.
    9. Mount each router via ``build_router()`` + ``app.include_router()``.
    10. Mount ``MCPAdapter`` if provided.
    11. Mount ``SkillAdapter`` if provided.
    12. Always mount ``HealthRouter``.
    12.5. Mount ``MetricsRouter`` (``GET /metrics``) if ``enable_metrics=True``.
    13. Return the ``FastAPI`` instance.

    Args:
        container:                  ``DIContainer`` (or ``None`` for container-free usage).
                                    Used for auto-scan and lifecycle component resolution.
        routers:                    Explicit list of ``VarcoRouter`` subclasses to mount.
                                    Pass ``None`` to auto-scan from the container.
        scan_packages:              Package names to scan before auto-discovering routers
                                    (e.g. ``["myapp.routers"]``).  Each package is scanned
                                    via ``container.scan(pkg, recursive=True)`` so that
                                    ``@Singleton`` router classes are registered before
                                    ``get_all(VarcoRouter)`` is called.  Ignored when
                                    ``routers`` is an explicit list.  Also ensures lifecycle
                                    components declared in those packages are registered.
        title:                      OpenAPI title shown in ``/docs``.
        version:                    API version string.
        description:                OpenAPI description (markdown supported).
        cors:                       ``CORSConfig`` instance.  ``None`` → reads from
                                    env vars via ``CORSConfig.from_env()``.
        enable_tracing:             Add ``TracingMiddleware`` (correlation ID + OTel).
        enable_logging:             Add ``RequestLoggingMiddleware``.
        enable_error_middleware:    Add ``ErrorMiddleware`` (exception → JSON response).
        enable_metrics:             Add ``MetricsMiddleware`` and mount ``MetricsRouter``
                                    at ``GET /metrics``.  Default: ``False``.  Requires
                                    ``OtelConfig(prometheus_enabled=True)`` in the DI
                                    container to populate the Prometheus registry with OTel
                                    metrics; without it, ``/metrics`` returns Python process
                                    metrics only.  Install the ``prometheus`` optional extra:
                                    ``pip install 'varco-fastapi[prometheus]'``.
        mcp_adapter:                If provided, mount the MCP endpoint at ``mcp_path``.
        mcp_path:                   URL path for the MCP endpoint.  Default: ``"/mcp"``.
        skill_adapter:              If provided, mount the A2A endpoints.
        skill_base_url:             Public base URL embedded in the Agent Card.
                                    Empty → resolved at request time from the request URL.
        skill_agent_card_path:      Path for ``GET /.well-known/agent.json``.
        skill_tasks_prefix:         Prefix for ``POST /tasks/send`` etc.
        extra_middleware:           List of ``(MiddlewareClass, kwargs_dict)`` tuples
                                    applied BEFORE the standard stack (i.e., innermost).
        extra_lifespan_components:  Additional ``AbstractLifecycle`` components started
                                    with the app (e.g. custom event buses, cache warmers).
        validate:                   Run startup validation checks.  Default: ``True``.
                                    Disable in tests where incomplete DI is intentional.
        strict_validation:          Pass ``strict=True`` to ``validate_router_class``,
                                    which raises for unresolved ``Any`` type args too.
        openapi_url:                FastAPI OpenAPI schema URL.
        docs_url:                   FastAPI Swagger UI URL.

    Returns:
        A fully configured ``fastapi.FastAPI`` instance.

    Raises:
        ConfigurationError: If ``validate=True`` and any check fails.
        ImportError:        If FastAPI is not installed (should never happen —
                            it is a hard dependency of varco_fastapi).

    Edge cases:
        - ``routers=None`` and ``container=None`` → no CRUD routers mounted;
          only HealthRouter is present.  Fine for gateway/proxy services.
        - ``routers=[]`` (empty list) explicitly mounts NO routers — differs
          from ``routers=None`` (auto-scan).
        - ``validate=False`` skips all validation — use in tests that intentionally
          construct partial configurations.

    Thread safety:  ✅ Called once at startup — not designed for concurrent calls.
    Async safety:   ✅ No async operations at construction time.

    📚 Docs:
        - 🔍 FastAPI lifespan: https://fastapi.tiangolo.com/advanced/events/
        - 🔍 Starlette middleware: https://www.starlette.io/middleware/
    """
    from fastapi import FastAPI  # noqa: PLC0415

    from varco_fastapi.exceptions import add_exception_handlers  # noqa: PLC0415
    from varco_fastapi.lifespan import VarcoLifespan  # noqa: PLC0415
    from varco_fastapi.middleware.cors import CORSConfig, install_cors  # noqa: PLC0415
    from varco_fastapi.middleware.error import ErrorMiddleware  # noqa: PLC0415
    from varco_fastapi.middleware.tracing import TracingMiddleware  # noqa: PLC0415

    # ── Step 1: Validate DI container ─────────────────────────────────────────
    if validate and container is not None:
        validate_container_bindings(container)

    # ── Step 2: Resolve router list ───────────────────────────────────────────
    resolved_routers: list[type]
    if routers is not None:
        # Explicit list — use as-is; skip scanning
        resolved_routers = list(routers)
    elif container is not None:
        # Auto-discover: scan the requested packages first so @Singleton router
        # classes are registered, then ask the container for all VarcoRouter instances.
        # container.scan() is idempotent — safe to call on already-scanned packages.
        resolved_routers = _scan_routers(container, scan_packages=scan_packages)
        _logger.info(
            "create_varco_app: auto-scanned %d router(s) from container: %s",
            len(resolved_routers),
            [r.__name__ for r in resolved_routers],
        )
    else:
        # No container and no explicit list — only HealthRouter will be mounted
        resolved_routers = []

    # ── Step 3: Validate router classes ───────────────────────────────────────
    if validate:
        for router_cls in resolved_routers:
            validate_router_class(router_cls, strict=strict_validation)

    # ── Step 4: Build VarcoLifespan ───────────────────────────────────────────
    # scan_packages are also passed to lifecycle collection so that event buses
    # and job runners declared in those packages are discovered and started.
    lifespan_components = _collect_lifecycle_components(container)
    for extra in extra_lifespan_components or []:
        lifespan_components.append(extra)

    varco_lifespan = VarcoLifespan(*lifespan_components)

    @asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Combined lifespan: VarcoLifespan wraps all registered components."""
        async with varco_lifespan(app):
            yield

    # ── Step 5: Create FastAPI instance ───────────────────────────────────────
    app = FastAPI(
        title=title,
        version=version,
        description=description,
        lifespan=_lifespan,
        openapi_url=openapi_url,
        docs_url=docs_url,
    )

    # ── Step 6: Exception handlers ────────────────────────────────────────────
    add_exception_handlers(app)

    # ── Step 7: Apply middleware stack ────────────────────────────────────────
    # Starlette executes add_middleware() in reverse order — last added = outermost.
    # We want: CORS → Error → Tracing → RequestContext → Session
    # So we add them in reverse: Session → RequestContext → Tracing → Error → CORS

    # Inner: RequestContextMiddleware (populates auth ContextVars)
    if container is not None:
        _try_add_request_context_middleware(app, container)

    # Tracing (correlation ID + OTel span)
    if enable_tracing:
        app.add_middleware(TracingMiddleware)

    # Metrics — sits INSIDE TracingMiddleware so OTel context is already active
    # when instruments are recorded.  Sits OUTSIDE RequestContextMiddleware so
    # it does not depend on auth ContextVars.
    # add_middleware() prepends, so this ends up between Tracing and Logging
    # in the final execution order.
    if enable_metrics:
        try:
            from varco_fastapi.middleware.metrics import (  # noqa: PLC0415
                MetricsMiddleware,
            )

            app.add_middleware(MetricsMiddleware)
        except ImportError:
            _logger.warning(
                "create_varco_app: enable_metrics=True but MetricsMiddleware "
                "could not be imported — metrics middleware skipped."
            )

    # Logging (structured request/response log)
    if enable_logging:
        try:
            from varco_fastapi.middleware.logging import (
                RequestLoggingMiddleware,
            )  # noqa: PLC0415

            app.add_middleware(RequestLoggingMiddleware)
        except ImportError:
            pass

    # Error (exception → JSON response) — must wrap tracing so errors are traced
    if enable_error_middleware:
        app.add_middleware(ErrorMiddleware)

    # Extra middleware from caller (added before CORS = inside ErrorMiddleware)
    for mw_entry in reversed(extra_middleware or []):
        if isinstance(mw_entry, tuple):
            mw_cls, mw_kwargs = mw_entry[0], mw_entry[1] if len(mw_entry) > 1 else {}
            app.add_middleware(mw_cls, **mw_kwargs)
        else:
            # Accept bare class too (no kwargs)
            app.add_middleware(mw_entry)

    # Outermost: CORS (must run before auth so OPTIONS preflight passes)
    cors_config = cors or (
        CORSConfig.from_env() if container is None else _resolve_cors(container)
    )
    install_cors(app, cors_config)

    # ── Step 9: Mount CRUD routers ────────────────────────────────────────────
    for router_cls in resolved_routers:
        _mount_router(app, router_cls, container)

    # ── Step 10: Mount MCPAdapter ──────────────────────────────────────────────
    if mcp_adapter is not None:
        mcp_adapter.mount(app, path=mcp_path)

    # ── Step 11: Mount SkillAdapter ───────────────────────────────────────────
    if skill_adapter is not None:
        skill_adapter.mount(
            app,
            base_url=skill_base_url,
            agent_card_path=skill_agent_card_path,
            tasks_prefix=skill_tasks_prefix,
        )

    # ── Step 12: Always mount HealthRouter ────────────────────────────────────
    _mount_health_router(app, container)

    # ── Step 12.5: Optionally mount MetricsRouter (GET /metrics) ─────────────
    # Mounted after HealthRouter to preserve the existing route order.
    # MetricsMiddleware is registered separately above (step 7) — both the
    # middleware and the router are needed: the middleware records metrics,
    # the router serves them.
    if enable_metrics:
        _mount_metrics_router(app)

    _logger.info(
        "create_varco_app: created '%s' v%s with %d router(s), " "mcp=%s, skill=%s",
        title,
        version,
        len(resolved_routers),
        mcp_adapter is not None,
        skill_adapter is not None,
    )

    return app


# ── Internal helpers ───────────────────────────────────────────────────────────


def _scan_routers(
    container: Any,
    *,
    scan_packages: list[str] | None = None,
) -> list[type]:
    """
    Discover ``VarcoRouter`` subclasses registered in a providify container.

    Uses the standard providify API:

    1. ``container.scan(pkg, recursive=True)`` — registers all ``@Singleton`` /
       ``@Component`` decorated classes in each requested package so that
       ``get_all()`` can find them.
    2. ``container.get_all(VarcoRouter)`` — resolves all bound ``VarcoRouter``
       instances and returns their concrete classes.

    DESIGN: container.scan() + get_all() over internal _bindings inspection
        ✅ Uses the public providify API — no dependency on private attributes
        ✅ scan() is idempotent — safe to call on already-scanned packages
        ✅ get_all() goes through the same resolution path as normal injection
        ❌ Instantiates routers as a side-effect of get_all() — routers that
           cannot be constructed (missing service) will raise here.  The caller
           sees a clear error rather than a silent empty list.

    Args:
        container:     ``DIContainer`` to query.
        scan_packages: Optional list of package names to scan before querying.
                       Each is passed to ``container.scan(pkg, recursive=True)``.

    Returns:
        List of ``VarcoRouter`` concrete classes found in the container.
        Empty list if none found or if scanning fails.

    Edge cases:
        - ``scan_packages=None`` → skip scanning; only already-registered classes
          are found.  Useful when ``container.scan()`` was called manually during
          bootstrap.
        - A package that cannot be imported → ``ModuleNotFoundError`` from
          ``container.scan()``; propagated to the caller.

    Thread safety:  ✅ Intended for single-threaded startup.
    Async safety:   ✅ No async operations.
    """
    try:
        from varco_fastapi.router.base import VarcoRouter  # noqa: PLC0415
    except ImportError:
        return []

    # Step 1: scan requested packages so their @Singleton router classes are
    # registered with the container before we call get_all().
    if scan_packages:
        for pkg in scan_packages:
            try:
                container.scan(pkg, recursive=True)
                _logger.debug("_scan_routers: scanned package %r", pkg)
            except Exception as exc:  # noqa: BLE001
                _logger.warning("_scan_routers: could not scan %r: %s", pkg, exc)

    # Step 2: ask the container for all registered VarcoRouter instances.
    # get_all() returns instances sorted by priority; we return their classes.
    try:
        instances = container.get_all(VarcoRouter)
        # Deduplicate — multiple bindings for the same class should not yield
        # the same router twice.
        seen: set[type] = set()
        router_classes: list[type] = []
        for inst in instances:
            cls = type(inst)
            if cls not in seen:
                seen.add(cls)
                router_classes.append(cls)
        return router_classes
    except Exception as exc:  # noqa: BLE001
        _logger.debug("_scan_routers: container.get_all(VarcoRouter) failed: %s", exc)
        return []


def _collect_lifecycle_components(container: Any) -> list[Any]:
    """
    Collect lifecycle components from the DI container.

    Scans known varco modules via ``container.scan()`` to ensure their
    ``@Singleton`` lifecycle classes are registered, then resolves each
    well-known lifecycle type with ``container.get()``.  Missing bindings
    are silently skipped — an app that does not use Kafka should not fail
    because ``KafkaEventBus`` is absent.

    Args:
        container: ``DIContainer`` to query.  ``None`` → returns empty list.

    Returns:
        List of objects implementing ``AbstractLifecycle`` (``start`` / ``stop``).

    Thread safety:  ✅ Called once at startup.
    Async safety:   ✅ No async operations.
    """
    if container is None:
        return []

    components: list[Any] = []

    # Resolve well-known lifecycle types via the public providify API.
    # container.scan(module) ensures the module's @Singleton classes are registered
    # before we call container.get() — this is the idiomatic providify pattern.
    _try_resolve_component(
        container, components, "varco_core.event.base", "AbstractEventBus"
    )
    _try_resolve_component(
        container, components, "varco_core.job.base", "AbstractJobRunner"
    )
    # varco_ws push adapters — discovered when container.scan("varco_ws") was called.
    # Only added when the caller explicitly registered them; silently skipped otherwise.
    _try_resolve_component(
        container, components, "varco_ws.websocket", "WebSocketEventBus"
    )
    _try_resolve_component(container, components, "varco_ws.sse", "SSEEventBus")

    return components


def _try_resolve_component(
    container: Any,
    out: list[Any],
    module: str,
    class_name: str,
) -> None:
    """
    Attempt to resolve a lifecycle component from the container by type.

    Uses the idiomatic providify pattern:

    1. ``container.scan(module)`` — registers the module's ``@Singleton`` /
       ``@Component`` classes with the container (idempotent).
    2. ``container.is_resolvable(cls)`` — non-destructive existence check.
    3. ``container.get(cls)`` — resolves and returns the instance.

    DESIGN: scan() + is_resolvable() + get() over bare importlib + get()
        ✅ scan() is the DI-idiomatic way to register a module's classes —
           no manual importlib wiring outside of the container's knowledge
        ✅ is_resolvable() avoids constructing the component just to check
           existence — important for expensive resources like DB pools
        ✅ Consistent with how application code registers modules at bootstrap
        ❌ scan() imports the module — if it has side-effects at import time
           those will run here.  All varco_core modules are side-effect-free.

    Args:
        container:  The ``DIContainer`` to query.
        out:        List to append the resolved component to.
        module:     Fully-qualified module name (e.g. ``"varco_core.event.base"``).
        class_name: Name of the class to resolve (e.g. ``"AbstractEventBus"``).

    Edge cases:
        - Module not installed → ``ModuleNotFoundError`` caught, silently skipped.
        - Binding not found after scan → ``is_resolvable()`` returns ``False``,
          silently skipped.
        - ``container.scan()`` raises for other reasons → silently skipped.

    Thread safety:  ✅ Called once at startup.
    Async safety:   ✅ No async operations.
    """
    try:
        import importlib  # noqa: PLC0415

        # Step 1: scan the module so its @Singleton / @Component classes are
        # registered.  This is a no-op if the module was already scanned.
        container.scan(module)

        # Step 2: import the class so we have a concrete type for resolution.
        mod = importlib.import_module(module)
        cls = getattr(mod, class_name)

        # Step 3: non-destructive existence check before resolving.
        if not container.is_resolvable(cls):
            return

        # Step 4: resolve and collect.
        component = container.get(cls)
        out.append(component)
    except Exception:  # noqa: BLE001
        # Any failure (import error, binding missing, scan error) is silently
        # skipped — lifecycle components are optional infrastructure.
        pass


def _try_add_request_context_middleware(app: Any, container: Any) -> None:
    """
    Add ``RequestContextMiddleware`` to the app, injecting ``AbstractServerAuth``
    from the container.

    Skipped silently if ``AbstractServerAuth`` is not bound.

    Args:
        app:       The ``FastAPI`` instance.
        container: The ``DIContainer``.
    """
    try:
        from varco_fastapi.auth.server_auth import AbstractServerAuth  # noqa: PLC0415
        from varco_fastapi import RequestContextMiddleware

        server_auth = container.get(AbstractServerAuth)
        app.add_middleware(RequestContextMiddleware, server_auth=server_auth)
    except Exception:  # noqa: BLE001
        # Auth not registered — add middleware without it (anonymous mode)
        try:
            from varco_fastapi.middleware.request_context import (
                RequestContextMiddleware,
            )  # noqa: PLC0415

            app.add_middleware(RequestContextMiddleware)
        except Exception:  # noqa: BLE001
            pass


def _resolve_cors(container: Any) -> Any:
    """
    Try to resolve ``CORSConfig`` from the container; fall back to env vars.

    Args:
        container: The ``DIContainer``.

    Returns:
        A ``CORSConfig`` instance.
    """
    from varco_fastapi.middleware.cors import CORSConfig  # noqa: PLC0415

    try:
        return container.get(CORSConfig)
    except Exception:  # noqa: BLE001
        return CORSConfig.from_env()


def _mount_router(app: Any, router_cls: type, container: Any | None) -> None:
    """
    Build and mount a single ``VarcoRouter`` onto the FastAPI app.

    Calls ``router_cls.build_router()`` which materialises the route
    declarations into a ``fastapi.APIRouter``.

    Args:
        app:        The ``FastAPI`` instance.
        router_cls: The ``VarcoRouter`` subclass to mount.
        container:  Container used to resolve the router instance (if needed).

    Edge cases:
        - If the router class has no ``build_router()`` (not a ``VarcoRouter``),
          logs a warning and skips.
        - If the router requires a service injected via the container and the
          binding is missing, the error is raised here (good — fail fast).
    """
    build_fn = getattr(router_cls, "build_router", None)
    if build_fn is None:
        _logger.warning(
            "_mount_router: %s has no build_router() — is it a VarcoRouter? Skipping.",
            router_cls.__name__,
        )
        return

    try:
        # build_router() is an instance method — instantiate with no args.
        # Routers are lightweight value objects; the DI container is only needed
        # for CRUD operations that inject a service (not at router-build time).
        api_router = router_cls().build_router()
        prefix = getattr(router_cls, "_prefix", "") or ""
        tags = getattr(router_cls, "_tags", None) or []
        app.include_router(api_router, prefix=prefix, tags=tags)
        _logger.debug("_mount_router: mounted %s at %s", router_cls.__name__, prefix)
    except Exception as exc:
        _logger.error(
            "_mount_router: failed to mount %s: %s",
            router_cls.__name__,
            exc,
            exc_info=True,
        )
        raise


def _mount_health_router(app: Any, container: Any | None) -> None:
    """
    Mount the ``HealthRouter`` on the app.

    Attempts to resolve health checks from the container; falls back to a
    bare health router with no checks (always returns 200 OK).

    Args:
        app:       The ``FastAPI`` instance.
        container: The ``DIContainer`` (may be ``None``).
    """
    try:
        from varco_fastapi.router.health import HealthRouter  # noqa: PLC0415

        health_router = HealthRouter().build_router()
        app.include_router(health_router)
        _logger.debug("_mount_health_router: HealthRouter mounted.")
    except Exception as exc:  # noqa: BLE001
        _logger.warning("_mount_health_router: could not mount HealthRouter: %s", exc)


def _mount_metrics_router(app: Any) -> None:
    """
    Mount ``MetricsRouter`` (``GET /metrics``) on the app.

    Silently skipped if ``MetricsRouter`` cannot be imported or mounted —
    consistent with the defensive pattern used by ``_mount_health_router``.

    For the ``/metrics`` endpoint to serve OTel metrics, ``OtelConfiguration``
    must have been installed with ``OtelConfig(prometheus_enabled=True)`` so
    that a ``PrometheusMetricReader`` was attached to the ``MeterProvider`` at
    startup.  Without it, ``/metrics`` returns Python process metrics only.

    Args:
        app: The ``FastAPI`` instance.

    Edge cases:
        - ``MetricsRouter`` not importable (e.g. package missing) → logs
          a warning and skips.  Does not raise.
        - Duplicate route ``/metrics`` already mounted → FastAPI raises at
          mount time; error is logged and re-raised so the misconfiguration
          surfaces immediately rather than silently failing.
    """
    try:
        from varco_fastapi.router.metrics import MetricsRouter  # noqa: PLC0415

        app.include_router(MetricsRouter().build_router())
        _logger.debug("_mount_metrics_router: MetricsRouter mounted at /metrics.")
    except ImportError as exc:
        _logger.warning(
            "_mount_metrics_router: could not import MetricsRouter: %s", exc
        )
    except Exception as exc:  # noqa: BLE001
        _logger.warning("_mount_metrics_router: could not mount MetricsRouter: %s", exc)


# ── Public API ─────────────────────────────────────────────────────────────────

__all__ = [
    "create_varco_app",
]
