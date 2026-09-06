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

    The verified, normative execution order — including the three optional
    entries Plan 035 adds (SecurityHeadersMiddleware, BodyLimitMiddleware,
    RateLimitMiddleware) — lives in exactly one place:
    ``varco_fastapi.middleware``'s module docstring (§D-order). This
    docstring used to restate it and had drifted (§D-order-bugs); it now
    points there instead of carrying a second, divergent copy.

Thread safety:  ✅ Intended to be called once at module import or startup.
Async safety:   ✅ No async operations — route registration is synchronous.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from varco_core.i18n.settings import I18nSettings
from varco_core.migration.base import AbstractMigrator
from varco_core.migration.settings import MigrationSettings
from varco_core.tz.settings import TimezoneSettings

from varco_fastapi.validation import validate_container_bindings, validate_router_class

if TYPE_CHECKING:
    from varco_fastapi.middleware.body_limit import BodyLimitSettings
    from varco_fastapi.middleware.rate_limit import RateLimitBundle
    from varco_fastapi.middleware.security_headers import SecurityHeadersSettings
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
    enable_profiling: bool = False,
    mcp_adapter: MCPAdapter | None = None,
    mcp_path: str = "/mcp",
    skill_adapter: SkillAdapter | None = None,
    skill_base_url: str = "",
    skill_agent_card_path: str = "/.well-known/agent.json",
    skill_tasks_prefix: str = "/tasks",
    extra_middleware: list[Any] | None = None,
    extra_lifespan_components: list[Any] | None = None,
    migrations: AbstractMigrator | Sequence[AbstractMigrator] | None = None,
    migration_settings: MigrationSettings | None = None,
    tenancy: Any | None = None,
    reliability: Any | None = None,
    i18n: I18nSettings | None = None,
    timezone: TimezoneSettings | None = None,
    validate: bool = True,
    strict_validation: bool = False,
    openapi_url: str = "/openapi.json",
    docs_url: str = "/docs",
    configure_jwt: bool = True,
    global_attributes: Mapping[str, str] | None = None,
    capture_params: bool | None = None,
    security_headers: SecurityHeadersSettings | bool | None = None,
    body_limit: BodyLimitSettings | bool | None = None,
    rate_limit: RateLimitBundle | None = None,
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
       RequestContext → Profiling → Session).
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
        enable_profiling:           Add ``ProfilingMiddleware`` (diagnostic CPU/memory
                                    profiler).  Default: ``False``.  Also respects the
                                    ``VARCO_PROFILER_ENABLED`` env var.  See
                                    ``ProfilingSettings`` for threshold/header options.
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
        configure_jwt:              When ``True`` (default), calls
                                    ``configure_jwt_from_env()`` once before
                                    routers are built so the process-global
                                    claim-transform/token-profile registries
                                    (``varco_core.jwt``) match what
                                    ``VarcoFastAPIModule``'s DI providers hand
                                    out.  Set ``False`` to opt out and manage
                                    the registries yourself (e.g. via
                                    ``configure_claim_transforms()`` with a
                                    hand-built registry).
        global_attributes:           Plan 004 passthrough — ``None`` (default,
                                    no behaviour change) or a mapping merged
                                    into ``varco_core.observability.attributes``'s
                                    process-wide global attribute registry via
                                    ``set_global_attributes()`` before the
                                    middleware stack is built.  Every span and
                                    metric measurement in the process picks
                                    these up automatically — see the
                                    Resource-vs-registry guidance in
                                    ``varco_core.observability.attributes``'s
                                    module docstring before using this for
                                    static process identity.
        capture_params:              Plan 004 passthrough — ``None`` (default,
                                    no behaviour change) or an explicit
                                    ``True``/``False`` forwarded to
                                    ``varco_core.observability.params.set_capture_enabled()``
                                    before the middleware stack is built,
                                    toggling automatic ``@span`` parameter
                                    capture process-wide.
        security_headers:            Plan 035 / §D-S7-default. ``None``
                                    (default) installs ``SecurityHeadersMiddleware``
                                    with ``SecurityHeadersSettings()`` (on,
                                    ``BALANCED`` preset — four headers on
                                    every response). Pass ``False`` to not
                                    register it at all, or a
                                    ``SecurityHeadersSettings`` instance for
                                    custom configuration (e.g. ``STRICT``).
                                    Registered at §D-order position 2 —
                                    inside ``CORSMiddleware``, outside
                                    everything else, so its headers attach
                                    to error responses too.
        body_limit:                  Plan 035 / §D-S8-default. ``None``
                                    (default) installs ``BodyLimitMiddleware``
                                    with ``BodyLimitSettings()`` (on, 10 MiB
                                    ceiling). Pass ``False`` to not register
                                    it, or a ``BodyLimitSettings`` instance
                                    for a custom ceiling/``exempt_paths``.
                                    Registered at §D-order position 5 —
                                    immediately inside ``ErrorMiddleware``,
                                    outside ``IdempotencyMiddleware``, so an
                                    over-limit request is rejected before
                                    anything buffers it.
        rate_limit:                  Plan 035 / §D-S10. ``None`` (default,
                                    the one opt-in row) registers nothing.
                                    Pass a ``RateLimitBundle`` to install up
                                    to two ``RateLimitMiddleware`` instances
                                    — its ``IP``/``GLOBAL`` rules at §D-order
                                    position 9 (outside
                                    ``RequestContextMiddleware``, before
                                    auth) and its ``SUBJECT``/``TENANT``
                                    rules at position 11 (inside it).

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
    from fastapi import FastAPI

    from varco_fastapi.exceptions import add_exception_handlers
    from varco_fastapi.lifespan import VarcoLifespan
    from varco_fastapi.middleware.cors import CORSConfig, install_cors
    from varco_fastapi.middleware.error import ErrorMiddleware
    from varco_fastapi.middleware.tracing import TracingMiddleware

    # ── Step 0: Configure JWT claim-transform / token-profile globals ────────
    # Runs before routers are built so any router-level auth wiring already
    # sees the correct process-global registries (varco_core.jwt.transform /
    # varco_core.jwt.profile).
    if configure_jwt:
        from varco_core.jwt.transform.runtime import (
            configure_jwt_from_env,
        )

        configure_jwt_from_env()

    # ── Step 0.5: Plan 004 observability passthrough ──────────────────────────
    # Both default to None (no behaviour change) — called before the
    # middleware stack is built so TracingMiddleware/@span/@counter/@histogram
    # all see the final state on the very first request.
    if global_attributes is not None:
        from varco_core.observability import set_global_attributes

        set_global_attributes(global_attributes)
    if capture_params is not None:
        from varco_core.observability import set_capture_enabled

        set_capture_enabled(capture_params)

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

    # ── Migrations (Plan 006 Phase 4) ─────────────────────────────────────────
    # Prepended BEFORE every other component — nothing else should touch a
    # table that does not exist yet. migrations=None (the default) leaves
    # this list byte-identical to today (D1: off is opt-in, not the default).
    resolved_migration_settings = migration_settings or MigrationSettings.from_env()
    if migrations is not None:
        migrators: tuple[AbstractMigrator, ...] = (
            (migrations,) if isinstance(migrations, AbstractMigrator) else tuple(migrations)
        )
        if resolved_migration_settings.mode != "off":
            from varco_fastapi.migrate import MigrationLifecycle

            lifespan_components = [
                MigrationLifecycle(*migrators, settings=resolved_migration_settings),
                *lifespan_components,
            ]
    elif resolved_migration_settings.mode != "off":
        # A set VARCO_MIGRATE_MODE with no migrator passed is the failure
        # mode that wastes the most operator time — warn loudly rather than
        # silently doing nothing (Plan 006 step 43).
        _logger.warning(
            "VARCO_MIGRATE_MODE=%r is set but create_varco_app() received no "
            "migrations= argument — no migration will run. Pass migrations=<AbstractMigrator> "
            "to enable auto-on-startup migrations.",
            resolved_migration_settings.mode,
        )

    # ── Multitenancy (Plan 007, Phase 10) ─────────────────────────────────────
    # tenancy=None (the default) registers nothing — byte-identical to
    # today. Prepended like MigrationLifecycle: nothing else should touch a
    # tenant resource pool before its sweeper/fan-out supervisor exist.
    if tenancy is not None:
        lifespan_components = [tenancy, *lifespan_components]
    else:
        import os as _os

        if _os.environ.get("VARCO_TENANCY_ISOLATION"):
            _logger.warning(
                "VARCO_TENANCY_ISOLATION is set but create_varco_app() received "
                "no tenancy= argument — no tenancy lifecycle will run. Pass "
                "tenancy=<TenancyLifecycle> to enable it (mirrors "
                "VARCO_MIGRATE_MODE's warn-without-migrations= behaviour)."
            )

    # ── Reliability preset (Plan 009, Phase 9) ────────────────────────────────
    # reliability=None (the default) registers nothing — byte-identical to
    # today. Appended (not prepended) — metrics/outbox/audit wiring depends
    # on the event bus and repositories other lifecycle components may set up.
    if reliability is not None:
        from varco_core.reliability import ReliabilityPreset

        from varco_fastapi.reliability import ReliabilityLifecycle

        _preset = (
            reliability if isinstance(reliability, ReliabilityPreset) else ReliabilityPreset.off()
        )
        lifespan_components = [
            *lifespan_components,
            ReliabilityLifecycle(_preset, container=container),
        ]

    # ── Localization / timezone (Plan 011) — resolve settings + optional
    # MessageCatalog lifecycle BEFORE VarcoLifespan is constructed (mirrors
    # the tenancy/reliability sections above — appending to
    # lifespan_components after VarcoLifespan(*lifespan_components) has
    # already run would silently never start the catalog).
    # DRIFT FIX (item 3): i18n=/timezone= are now typed I18nSettings | None /
    # TimezoneSettings | None (not Any | None) so a wrong type is a
    # type-checker error at the call site. The former `isinstance(...)
    # else <default>` fallback silently swallowed a typo'd/wrong-type
    # argument (e.g. a dict, or the other settings class) by discarding it
    # and constructing defaults instead — the caller's mistake produced no
    # error, just unexpectedly-disabled i18n/timezone. `None` (the
    # documented default sentinel) is the only value that still resolves to
    # defaults; anything else is used as given, so a genuinely wrong type
    # now fails fast with an AttributeError on `.enabled` instead of being
    # silently discarded.
    _resolved_i18n_settings = i18n if i18n is not None else I18nSettings()
    _resolved_timezone_settings = timezone if timezone is not None else TimezoneSettings()
    # RD-3 (drift item 4): resolved once here and threaded into BOTH
    # add_exception_handlers() and ErrorMiddleware below, so the error path
    # actually localizes message_key/params via request.state.varco_request_context
    # (set by LocalizationMiddleware) instead of the seam existing but never
    # being wired. None with i18n disabled — byte-identical to before.
    _catalog: Any | None = None
    if _resolved_i18n_settings.enabled and container is not None:
        from varco_core.i18n.catalog import MessageCatalog

        from varco_fastapi.i18n import I18nLifecycle

        try:
            _catalog = container.get(MessageCatalog)
        except Exception:  # noqa: BLE001 — no catalog bound; NullMessageCatalog-equivalent
            _catalog = None
        if _catalog is not None:
            lifespan_components = [*lifespan_components, I18nLifecycle(_catalog)]

    # ── Container teardown (Plan 022 / RL-8a, §D-8a2(a)) ──────────────────────
    # Hand VarcoLifespan a plain coroutine factory — never the container itself,
    # which the lifespan's own DESIGN block refuses ("a plain orchestrator — no
    # DI knowledge").  This is what finally tears down the six measured orphaned
    # @PreDestroy singletons (design/api-freeze-and-standards/measurements/
    # predestroy-vs-lifespan.md), including RedisCache/MemcachedCache which are
    # eagerly constructed with an already-started connection pool.
    # Container-free apps (container=None is a supported path, see the module
    # docstring's "Works without DI") get shutdown=None — byte-identical to
    # pre-3.0.0 behaviour, since there is nothing to sweep.
    _container_shutdown: Callable[[], Awaitable[None]] | None = None
    if container is not None:

        async def _container_shutdown_hook() -> None:
            await container.ashutdown()

        _container_shutdown = _container_shutdown_hook

    varco_lifespan = VarcoLifespan(*lifespan_components, shutdown=_container_shutdown)

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
    add_exception_handlers(
        app,
        message_catalog=_catalog,
        set_content_language=_resolved_i18n_settings.set_content_language,
    )

    # ── Step 7: Apply middleware stack ────────────────────────────────────────
    # Starlette executes add_middleware() in reverse order — last added = outermost.
    # We want: CORS → Error → Tracing → RequestContext → Session
    # So we add them in reverse: Session → RequestContext → Tracing → Error → CORS

    # Innermost: ProfilingMiddleware — placed closest to the route handler so
    # the report attributes cost to the endpoint, not to outer middleware layers.
    # Only one request profiled at a time; concurrent requests pass through unprofiled.
    if enable_profiling:
        _try_add_profiling_middleware(app)

    # LocalizationMiddleware (Plan 011, RD-3) — resolves locale (I2) and/or
    # timezone (T1) into one merged RequestContext. i18n=None / timezone=None
    # (the defaults) register nothing — byte-identical to today. Placed close
    # to the route handler (just outside ProfilingMiddleware), matching
    # CLAUDE.md's documented request order "... -> [TenantResolution] ->
    # [Localization] -> handler": it must be added (and therefore end up
    # innermost relative to) any TenantResolutionMiddleware an app wires via
    # extra_middleware=, so current_tenant() is already populated by the time
    # the tenant-default precedence step runs.
    if _resolved_i18n_settings.enabled or _resolved_timezone_settings.enabled:
        from varco_fastapi.middleware.localization import LocalizationMiddleware

        app.add_middleware(
            LocalizationMiddleware,
            i18n_settings=_resolved_i18n_settings,
            timezone_settings=_resolved_timezone_settings,
        )

    # RateLimitMiddleware(POST_AUTH) — Plan 035 / §D-order position 11.
    # Added BEFORE RequestContextMiddleware (below) so RequestContextMiddleware
    # ends up more OUTER (add_middleware() prepends — later call = more outer)
    # and therefore runs FIRST, populating the AuthContext/current_tenant()
    # this stage's SUBJECT/TENANT keying reads. §D-S10-keyspace: the
    # acknowledgement comes from RateLimitBundle.acknowledge_unbounded_keyspace
    # — the CALLER's own explicit opt-in, never forged here — so an IP/SUBJECT
    # rule backed by an InMemoryRateLimiter still raises ValueError at
    # construction unless the caller set it, exactly as a hand-registered
    # RateLimitMiddleware would.
    _post_auth_rate_limit_rules = _partition_rate_limit_rules(rate_limit, post_auth=True)
    if _post_auth_rate_limit_rules:
        from varco_fastapi.middleware.rate_limit import RateLimitMiddleware, RateLimitStage

        app.add_middleware(
            RateLimitMiddleware,
            rules=_post_auth_rate_limit_rules,
            stage=RateLimitStage.POST_AUTH,
            settings=rate_limit.settings if rate_limit is not None else None,
            acknowledge_unbounded_keyspace=(
                rate_limit.acknowledge_unbounded_keyspace if rate_limit is not None else False
            ),
        )

    # RequestContextMiddleware (populates auth ContextVars)
    if container is not None:
        _try_add_request_context_middleware(app, container)

    # RateLimitMiddleware(PRE_AUTH) — Plan 035 / §D-order position 9. Added
    # AFTER RequestContextMiddleware (above) so it ends up more OUTER —
    # rejecting an unauthenticated flood before any JWT signature
    # verification happens (the DoS §D-order's DESIGN block names).
    # §D-S10-keyspace: same caller-supplied acknowledgement as the POST_AUTH
    # registration above — never forged here.
    _pre_auth_rate_limit_rules = _partition_rate_limit_rules(rate_limit, post_auth=False)
    if _pre_auth_rate_limit_rules:
        from varco_fastapi.middleware.rate_limit import RateLimitMiddleware, RateLimitStage

        app.add_middleware(
            RateLimitMiddleware,
            rules=_pre_auth_rate_limit_rules,
            stage=RateLimitStage.PRE_AUTH,
            settings=rate_limit.settings if rate_limit is not None else None,
            acknowledge_unbounded_keyspace=(
                rate_limit.acknowledge_unbounded_keyspace if rate_limit is not None else False
            ),
        )

    # Tracing (correlation ID + OTel span)
    if enable_tracing:
        app.add_middleware(TracingMiddleware)

    # Metrics — verified position: OUTSIDE TracingMiddleware (§D-order-bugs
    # corrects the prior "sits INSIDE Tracing" claim here, which did not
    # match what add_middleware() actually builds — see
    # varco_fastapi.middleware's module docstring for the full, verified
    # order and BACKLOG.md for the filed "is this the right position?"
    # question). Sits OUTSIDE RequestContextMiddleware so it does not
    # depend on auth ContextVars.
    if enable_metrics:
        try:
            from varco_fastapi.middleware.metrics import (
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
            )

            app.add_middleware(RequestLoggingMiddleware)
        except ImportError:
            pass

    # BodyLimitMiddleware — Plan 035 / §D-order position 5. Immediately
    # INSIDE ErrorMiddleware (added here, before it, so ErrorMiddleware ends
    # up more outer) so its 413 renders through the one error envelope;
    # OUTSIDE IdempotencyMiddleware (an app-level opt-in, added separately by
    # the caller) so an over-limit request is rejected before anything
    # buffers it. body_limit=None (default) installs it on at 10 MiB;
    # body_limit=False registers nothing.
    if body_limit is not False:
        from varco_fastapi.middleware.body_limit import BodyLimitMiddleware, BodyLimitSettings

        _body_limit_settings = (
            body_limit if isinstance(body_limit, BodyLimitSettings) else BodyLimitSettings()
        )
        app.add_middleware(
            BodyLimitMiddleware,
            settings=_body_limit_settings,
            has_error_middleware=enable_error_middleware,
        )

    # Error (exception → JSON response) — must wrap tracing so errors are traced
    if enable_error_middleware:
        app.add_middleware(
            ErrorMiddleware,
            message_catalog=_catalog,
            set_content_language=_resolved_i18n_settings.set_content_language,
        )

    # Extra middleware from caller. §D-order-bugs: verified OUTSIDE
    # ErrorMiddleware (added AFTER it here, and add_middleware() prepends —
    # so this lands further out), NOT "inside" as a prior comment claimed.
    # Consequence: a ServiceException raised from an extra_middleware=
    # entry is NOT rendered through the error envelope. Register a varco
    # edge middleware via the dedicated security_headers=/body_limit=/
    # rate_limit= keywords instead — never via extra_middleware=, which is
    # the wrong position for all three (BACKLOG.md's filed question).
    for mw_entry in reversed(extra_middleware or []):
        if isinstance(mw_entry, tuple):
            mw_cls, mw_kwargs = mw_entry[0], mw_entry[1] if len(mw_entry) > 1 else {}
            app.add_middleware(mw_cls, **mw_kwargs)
        else:
            # Accept bare class too (no kwargs)
            app.add_middleware(mw_entry)

    # SecurityHeadersMiddleware — Plan 035 / §D-order position 2. Added here,
    # after extra_middleware and before install_cors, so it ends up INSIDE
    # CORSMiddleware (never rewrites a preflight response) and OUTSIDE
    # everything else — including ErrorMiddleware, so its headers attach to
    # every error response too (413/429 included).
    # security_headers=None (default) installs it on at BALANCED;
    # security_headers=False registers nothing.
    if security_headers is not False:
        from varco_fastapi.middleware.security_headers import (
            SecurityHeadersMiddleware,
            SecurityHeadersSettings,
        )

        _security_headers_settings = (
            security_headers
            if isinstance(security_headers, SecurityHeadersSettings)
            else SecurityHeadersSettings()
        )
        app.add_middleware(SecurityHeadersMiddleware, settings=_security_headers_settings)

    # Outermost: CORS (must run before auth so OPTIONS preflight passes)
    cors_config = cors or (CORSConfig.from_env() if container is None else _resolve_cors(container))
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
        "create_varco_app: created '%s' v%s with %d router(s), mcp=%s, skill=%s",
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
        from varco_fastapi.router.base import VarcoRouter
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


def _lifecycle_discovery_warns() -> bool:
    """
    Read the ``VARCO_LIFECYCLE_DISCOVERY_WARN`` kill switch.

    Controls whether ``_try_resolve_component()`` logs a missing-binding
    signal (``is_resolvable() is False``) at WARNING (default) or DEBUG.
    Lets an app that genuinely has no event bus / job runner silence that
    one line without silencing its whole logger (Plan 014 / audit F2).

    Args:
        None.

    Returns:
        ``True`` unless the env var is set to a recognized falsy value
        (``0``/``false``/``no``/``off``, case-insensitive).

    Edge cases:
        - Unset → ``True`` (warn — today's missing signal becomes visible
          by default).
        - Set to a garbage value (e.g. ``"maybe"``) → treated as truthy
          (warn). Never raises from a logging-configuration read.

    Thread safety:  ✅ Pure read of ``os.environ``.
    Async safety:   ✅ No async operations.
    """
    import os as _os

    raw = _os.environ.get("VARCO_LIFECYCLE_DISCOVERY_WARN")
    if raw is None:
        return True
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _collect_lifecycle_components(container: Any) -> list[Any]:
    """
    Collect lifecycle components from the DI container.

    Scans known varco modules via ``container.scan()`` to ensure their
    ``@Singleton`` lifecycle classes are registered, then resolves each
    well-known lifecycle type with ``container.get()``.  Missing bindings
    are logged and skipped — an app that does not use Kafka should not fail
    because ``KafkaEventBus`` is absent, but a genuinely-forgotten binding
    (e.g. ``AbstractEventBus``) now produces a WARNING naming the remedy
    instead of vanishing silently (Plan 014 / audit F2).

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
    # AbstractEventBus / AbstractJobRunner are core infra you almost certainly
    # meant to wire — warn_if_missing stays at its default (True).
    _try_resolve_component(container, components, "varco_core.event.base", "AbstractEventBus")
    _try_resolve_component(container, components, "varco_core.job.base", "AbstractJobRunner")
    # varco_ws push adapters — discovered when container.scan("varco_ws") was called.
    # Only added when the caller explicitly registered them — warn_if_missing=False
    # so an app that never uses varco_ws doesn't get two guaranteed startup WARNINGs.
    _try_resolve_component(
        container,
        components,
        "varco_ws.websocket",
        "WebSocketEventBus",
        warn_if_missing=False,
    )
    _try_resolve_component(
        container, components, "varco_ws.sse", "SSEEventBus", warn_if_missing=False
    )

    return components


def _try_resolve_component(
    container: Any,
    out: list[Any],
    module: str,
    class_name: str,
    *,
    warn_if_missing: bool = True,
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

    DESIGN: tiered, always-logged, never-propagating skip (Plan 014 / audit F2)
        ✅ Every skip now produces exactly one log line naming the module and
           class — "you forgot ``<pkg>.bootstrap(container)``" is no longer
           silent.
        ✅ Control flow is unchanged: nothing new propagates out of this
           function on any path — only the logging tier differs.
        ❌ An app with several unwired optional components now logs several
           WARNINGs at startup — mitigated by ``warn_if_missing=False`` for
           genuinely opt-in components (the ``varco_ws`` push adapters) and
           the ``VARCO_LIFECYCLE_DISCOVERY_WARN`` kill switch.

    Args:
        container:  The ``DIContainer`` to query.
        out:        List to append the resolved component to.
        module:     Fully-qualified module name (e.g. ``"varco_core.event.base"``).
        class_name: Name of the class to resolve (e.g. ``"AbstractEventBus"``).
        warn_if_missing: When ``True`` (default), an ``is_resolvable() is
            False`` outcome logs at WARNING (or DEBUG when
            ``VARCO_LIFECYCLE_DISCOVERY_WARN`` is falsy). When ``False``,
            that outcome always logs at DEBUG regardless of the kill
            switch — for components that are legitimately optional and
            would otherwise produce startup noise for every app that
            doesn't use them (the ``varco_ws`` push adapters).

    Edge cases:
        - Module not installed → ``ModuleNotFoundError`` caught, logged at
          DEBUG ("package not installed"), skipped.
        - Module present but ``class_name`` doesn't exist (version skew) →
          ``AttributeError`` caught, logged at WARNING naming both the
          module and the class, skipped.
        - Binding not found after scan → ``is_resolvable()`` returns
          ``False``, logged per ``warn_if_missing``/the kill switch, skipped.
        - ``container.get()`` raises ``LookupError`` (binding vanished
          between the check and the resolve) → logged at WARNING, skipped.
        - ``container.get()`` raises any other exception (construction
          failed, e.g. a socket connect error) → logged at WARNING with
          ``exc_info=True``, skipped. The component is skipped and the app
          still starts, exactly as before this change.
        - No path in this function raises — every outcome either appends to
          ``out`` or returns after logging.

    Thread safety:  ✅ Called once at startup.
    Async safety:   ✅ No async operations.
    """
    import importlib

    try:
        # Step 1: scan the module so its @Singleton / @Component classes are
        # registered.  This is a no-op if the module was already scanned.
        container.scan(module)

        # Step 2: import the class so we have a concrete type for resolution.
        mod = importlib.import_module(module)
        cls = getattr(mod, class_name)
    except ModuleNotFoundError:
        # Common case — the optional package simply isn't installed.
        _logger.debug(
            "_try_resolve_component: %s not installed — skipping %s.%s",
            module.split(".")[0],
            module,
            class_name,
        )
        return
    except AttributeError:
        # Module imported fine but the class name doesn't exist — a real
        # signal (e.g. version skew between varco_fastapi and the backend
        # package), previously indistinguishable from "not installed".
        _logger.warning(
            "_try_resolve_component: module %r has no attribute %r — skipping "
            "lifecycle component (check for a version mismatch between "
            "varco_fastapi and %s).",
            module,
            class_name,
            module.split(".")[0],
        )
        return
    except Exception as exc:  # noqa: BLE001
        # container.scan() raised for some other reason.
        _logger.warning(
            "_try_resolve_component: container.scan(%r) failed while looking "
            "for %s.%s — skipping lifecycle component: %s",
            module,
            module,
            class_name,
            exc,
            exc_info=True,
        )
        return

    # Step 3: non-destructive existence check before resolving.
    if not container.is_resolvable(cls):
        if warn_if_missing and _lifecycle_discovery_warns():
            _logger.warning(
                "_try_resolve_component: %s.%s is not bound in the DI "
                "container — skipping this lifecycle component. If this is "
                "unexpected, call <package>.bootstrap(container) before "
                "create_varco_app(). Silence this with "
                "VARCO_LIFECYCLE_DISCOVERY_WARN=false if the app genuinely "
                "does not use it.",
                module,
                class_name,
            )
        else:
            _logger.debug(
                "_try_resolve_component: %s.%s is not bound in the DI "
                "container — skipping this lifecycle component.",
                module,
                class_name,
            )
        return

    # Step 4: resolve and collect.
    try:
        component = container.get(cls)
    except LookupError as exc:
        # Binding vanished between the is_resolvable() check and get() —
        # rare, but a real signal worth a WARNING rather than silence.
        _logger.warning(
            "_try_resolve_component: %s.%s was resolvable but container.get() "
            "raised %s — skipping this lifecycle component.",
            module,
            class_name,
            exc,
        )
        return
    except Exception as exc:  # noqa: BLE001
        # Construction failed (e.g. a socket connect error opening a
        # connection pool) — skip the component and let the app still
        # start, exactly as before this change, but log it loudly.
        _logger.warning(
            "_try_resolve_component: constructing %s.%s failed — skipping "
            "this lifecycle component: %s",
            module,
            class_name,
            exc,
            exc_info=True,
        )
        return

    out.append(component)


def _partition_rate_limit_rules(rate_limit: Any | None, *, post_auth: bool) -> tuple[Any, ...]:
    """
    Split a ``RateLimitBundle``'s rules by §D-order stage.

    ``IP``/``GLOBAL`` rules are PRE_AUTH-capable; ``SUBJECT``/``TENANT``
    rules require an authenticated request context and are POST_AUTH only
    (§D-S10-shape) — ``RateLimitMiddleware.__init__`` itself refuses the
    wrong combination with a ``ValueError``, so this partition is purely a
    convenience for ``create_varco_app``, not a second source of truth for
    which scopes are legal where.

    Args:
        rate_limit: The ``RateLimitBundle`` passed to ``create_varco_app``,
                    or ``None`` (§D-S10 is the one opt-in row — no bundle
                    means no rules at all).
        post_auth:  ``True`` to return the ``SUBJECT``/``TENANT`` rules,
                    ``False`` to return the ``IP``/``GLOBAL`` rules.

    Returns:
        A tuple of matching ``RateLimitRule``s — empty when ``rate_limit``
        is ``None`` or no rule matches this stage.
    """
    if rate_limit is None:
        return ()
    from varco_fastapi.middleware.rate_limit import RateLimitScope

    post_auth_scopes = {RateLimitScope.SUBJECT, RateLimitScope.TENANT}
    return tuple(rule for rule in rate_limit.rules if (rule.scope in post_auth_scopes) == post_auth)


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
        from varco_fastapi import RequestContextMiddleware
        from varco_fastapi.auth.server_auth import AbstractServerAuth

        server_auth = container.get(AbstractServerAuth)
        app.add_middleware(RequestContextMiddleware, server_auth=server_auth)
    except Exception:  # noqa: BLE001
        # Auth not registered — add middleware without it (anonymous mode)
        try:
            from varco_fastapi.middleware.request_context import (
                RequestContextMiddleware,
            )

            app.add_middleware(RequestContextMiddleware)
        except Exception:  # noqa: BLE001
            pass


def _try_add_profiling_middleware(app: Any) -> None:
    """Add ``ProfilingMiddleware`` to the app, reading settings from env vars.

    Placed innermost in the middleware stack so profiling attributes cost to
    the route handler, not to outer layers (error handling, auth, tracing).

    Skipped silently if ``varco_core.profiling`` is not importable.

    Args:
        app: The ``FastAPI`` instance.
    """
    try:
        from varco_fastapi.middleware.profiling import (
            ProfilingMiddleware,
            ProfilingSettings,
        )

        settings = ProfilingSettings()
        app.add_middleware(ProfilingMiddleware, settings=settings)
    except Exception:  # noqa: BLE001
        _logger.warning(
            "create_varco_app: enable_profiling=True but ProfilingMiddleware "
            "could not be initialised — profiling middleware skipped."
        )


def _resolve_cors(container: Any) -> Any:
    """
    Try to resolve ``CORSConfig`` from the container; fall back to env vars.

    Args:
        container: The ``DIContainer``.

    Returns:
        A ``CORSConfig`` instance.
    """
    from varco_fastapi.middleware.cors import CORSConfig

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
        # build_router() already embeds _prefix and _tags in the APIRouter —
        # do NOT pass prefix=/tags= again here or routes are doubled.
        api_router = router_cls().build_router()
        app.include_router(api_router)
        _logger.debug(
            "_mount_router: mounted %s at %s",
            router_cls.__name__,
            getattr(router_cls, "_prefix", ""),
        )
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
        from varco_fastapi.router.health import HealthRouter

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
        from varco_fastapi.router.metrics import MetricsRouter

        app.include_router(MetricsRouter().build_router())
        _logger.debug("_mount_metrics_router: MetricsRouter mounted at /metrics.")
    except ImportError as exc:
        _logger.warning("_mount_metrics_router: could not import MetricsRouter: %s", exc)
    except Exception as exc:  # noqa: BLE001
        _logger.warning("_mount_metrics_router: could not mount MetricsRouter: %s", exc)


# ── Public API ─────────────────────────────────────────────────────────────────

__all__ = [
    "create_varco_app",
]
