"""
varco_fastapi.app
==================
One-call application factory for varco-powered FastAPI services.

Plan 049 / DI-1: ``create_varco_app(container, *, modules=None, ...)``
replaces the eleven hand-passed keyword objects (``migrations``,
``tenancy``, ``reliability``, ``retention``, ``jwks_refresh``, ``i18n``,
``timezone``, ``security_headers``, ``body_limit``, ``rate_limit``,
``extra_lifespan_components``) — plus six ``enable_*`` booleans and
``cors`` — with a single ``modules=`` list of ``VarcoModule`` instances.
``modules=None`` installs ``varco_fastapi.default_modules()``; an explicit
list installs **exactly** those (NestJS/Django ``INSTALLED_APPS`` semantics
— what you list is what you get).

Steps (in order):

1. ``container`` is required (§D-signature) — modules register into it.
2. Resolve the module list (``modules`` or ``default_modules()``),
   topologically order it by ``depends_on`` (``resolve_module_order``),
   and ``register()`` every one, in order — synchronous, at this call.
3. Validate DI container bindings (if ``validate=True``).
4. Resolve + validate the router list.
5. Build ``VarcoLifespan`` — one ``_ModuleComponent`` per *active*
   (enabled) module, ``setup=`` awaits every module's ``async_init()``
   before any ``start()``, ``shutdown=container.ashutdown``.
6. Create the ``FastAPI`` instance with that lifespan.
7. Call ``configure_app(app, container)`` on every active
   ``HttpContributor`` module (exception handlers, router mounts, the
   posture module capturing ``app``).
8. Collect every active ``HttpContributor`` module's ``middleware()``
   specs, sort by ``EdgeSlot``, install once via
   ``install_middleware_stack``.
9. Mount each router via ``build_router()`` + ``app.include_router()``.
10. Mount ``MCPAdapter``/``SkillAdapter`` if given.
11. Always mount ``HealthRouter``.
12. Return the ``FastAPI`` instance.

Minimal usage::

    from providify import DIContainer
    from varco_fastapi.app import create_varco_app

    container = DIContainer()
    app = create_varco_app(container)  # modules=None -> default_modules()

Extending the default set::

    from varco_fastapi.modules import default_modules
    from varco_sa.modules import SAModule

    app = create_varco_app(
        container,
        modules=[*default_modules(), SAModule(config=my_sa_config, entities=(User,))],
    )

**One rule for what stays a keyword argument on this function**: a keyword
configures the ``FastAPI`` object itself or names what the app exposes
(``routers``/``scan_packages``, ``title``/``version``/``description``/
``openapi_url``/``docs_url``, ``validate``/``strict_validation`` — this
function's own build-time checks, ``mcp_adapter``/``skill_adapter*`` — the
app's own mounted subject). Everything that binds, starts, stops, or wraps
a request is a ``VarcoModule`` — see ``varco_fastapi.modules`` and
``technical_docs/features/dependency-injection.md``'s §Modules.

DESIGN: factory function over application subclass
    ✅ Plain function — easy to test (call it, inspect the result)
    ✅ Keyword-only args with sensible defaults — additive, not breaking
    ❌ Not as IDE-friendly as a builder class — mitigated by clear docs

DESIGN: middleware stack applied via install_middleware_stack (§D-http)
    Every ``HttpContributor`` module names an ``EdgeSlot`` — the
    normative, outermost-first order table is
    ``varco_fastapi.middleware.slots.EdgeSlot`` (as code) and
    ``varco_fastapi.middleware``'s module docstring (as prose), pinned
    together by ``test_middleware_slots.py``.

Thread safety:  ✅ Intended to be called once at module import or startup.
Async safety:   ✅ No async operations — route registration is synchronous;
                    module ``async_init()``/``start()``/``stop()`` calls
                    happen inside the ASGI lifespan, not here.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, TypeGuard

from varco_core.di.module import ModuleRegistry, VarcoModule, resolve_module_order

from varco_fastapi.middleware.slots import sort_specs
from varco_fastapi.validation import validate_container_bindings, validate_router_class

if TYPE_CHECKING:
    from fastapi import FastAPI
    from providify import DIContainer

    from varco_fastapi.middleware import MiddlewareEntry
    from varco_fastapi.middleware.slots import MiddlewareSpec
    from varco_fastapi.modules.base import HttpContributor
    from varco_fastapi.router.mcp import MCPAdapter
    from varco_fastapi.router.skill import SkillAdapter

_logger = logging.getLogger(__name__)


# ── create_varco_app ───────────────────────────────────────────────────────────


def create_varco_app(
    container: DIContainer,
    *,
    modules: Sequence[VarcoModule] | None = None,
    routers: list[type] | None = None,
    scan_packages: list[str] | None = None,
    title: str = "Varco API",
    version: str = "0.1.0",
    description: str = "",
    openapi_url: str = "/openapi.json",
    docs_url: str = "/docs",
    validate: bool = True,
    strict_validation: bool = False,
    mcp_adapter: MCPAdapter | None = None,
    mcp_path: str = "/mcp",
    skill_adapter: SkillAdapter | None = None,
    skill_base_url: str = "",
    skill_agent_card_path: str = "/.well-known/agent.json",
    skill_tasks_prefix: str = "/tasks",
) -> FastAPI:
    """
    Create a fully configured FastAPI application for a varco service.

    Args:
        container: ``DIContainer`` — required (§D-signature). Every module
            registers into it; a container-free app is
            ``create_varco_app(DIContainer(), routers=[...])``.
        modules: ``None`` (default) installs
            ``varco_fastapi.default_modules()``. An explicit list installs
            **exactly** those — nothing is inserted implicitly.
        routers: Explicit list of ``VarcoRouter`` subclasses to mount.
            ``None`` auto-scans from the container.
        scan_packages: Package names to scan before auto-discovering
            routers. Ignored when ``routers`` is an explicit list.
        title, version, description, openapi_url, docs_url: Forwarded to
            the ``FastAPI(...)`` constructor.
        validate: Run startup validation checks (DI bindings + router
            classes). Default ``True``.
        strict_validation: Pass ``strict=True`` to ``validate_router_class``.
        mcp_adapter, mcp_path, skill_adapter, skill_base_url,
            skill_agent_card_path, skill_tasks_prefix: The app's own MCP/A2A
            mounts — unchanged from pre-049.

    Returns:
        A fully configured ``fastapi.FastAPI`` instance.

    Raises:
        TypeError: ``container`` was not passed.
        ValueError: Two distinct instances of the same ``VarcoModule``
            subclass were listed (``resolve_module_order``).
        varco_core.di.module.ModuleDependencyError: A module's
            ``depends_on`` names a class absent from the resolved list.
        varco_core.di.module.ModuleCycleError: A ``depends_on`` cycle.
        ConfigurationError: ``validate=True`` and a check failed.

    Edge cases:
        - ``modules=[]`` registers nothing — no middleware, no lifecycle,
          a bare ``FastAPI`` with only ``HealthRouter``. Allowed.
        - A module's ``_register()`` raising propagates before any
          ``FastAPI`` object exists; nothing is recorded for it.
        - A ``MigrationsModule``/``TenancyModule`` listed, in the raw
          *input* list, after ``CoreModule`` (or after
          ``*default_modules()``, which lists ``CoreModule`` first) logs a
          WARNING — nothing else touches the database schema until it has
          run, and Plan 050 replaces this list-order caveat with phases.

    Thread safety:  ✅ Called once at startup.
    Async safety:   ✅ No async operations at construction time.
    """
    from fastapi import FastAPI

    from varco_fastapi.lifespan import VarcoLifespan
    from varco_fastapi.middleware import install_middleware_stack

    # ── Step 1: resolve + register modules ────────────────────────────────
    raw_modules = list(modules) if modules is not None else _default_modules_list()
    _warn_migration_tenancy_listed_after_core(raw_modules)

    order = resolve_module_order(raw_modules)
    for m in order:
        m.register(container)

    # `modules=[]` never calls register() at all, so ModuleRegistry is
    # never bound — is_resolvable() first avoids a spurious LookupError
    # (or a MagicMock-container false positive) for that documented edge
    # case (§Edge cases: "modules=[] registers nothing").
    if order and container.is_resolvable(ModuleRegistry):
        registry: ModuleRegistry = container.get(ModuleRegistry)
        active = [m for m in order if registry.enabled(m)]
    else:
        active = []

    _warn_unowned_settings(container, active)

    # ── Step 2: config-driven backend selection check (Plan 047 §D-check) ──
    from varco_core.di import check_backend_selection  # noqa: PLC0415

    check_backend_selection(container)

    # ── Step 3: validate DI container ─────────────────────────────────────
    if validate:
        validate_container_bindings(container)

    # ── Step 4: resolve + validate routers ─────────────────────────────────
    if routers is not None:
        resolved_routers = list(routers)
    else:
        resolved_routers = _scan_routers(container, scan_packages=scan_packages)
        _logger.info(
            "create_varco_app: auto-scanned %d router(s) from container: %s",
            len(resolved_routers),
            [r.__name__ for r in resolved_routers],
        )
    if validate:
        for router_cls in resolved_routers:
            validate_router_class(router_cls, strict=strict_validation)

    # ── Step 5: build VarcoLifespan ─────────────────────────────────────────
    async def _async_init_all() -> None:
        for m in active:
            await m.async_init(container)

    async def _container_shutdown() -> None:
        await container.ashutdown()

    lifespan_components = [_ModuleComponent(m, container) for m in active]
    varco_lifespan = VarcoLifespan(
        *lifespan_components, setup=_async_init_all, shutdown=_container_shutdown
    )

    @asynccontextmanager
    async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
        async with varco_lifespan(app):
            yield

    # ── Step 6: create FastAPI instance ─────────────────────────────────────
    app = FastAPI(
        title=title,
        version=version,
        description=description,
        lifespan=_lifespan,
        openapi_url=openapi_url,
        docs_url=docs_url,
    )

    # ── Step 7: configure_app() on every active HttpContributor ────────────
    for m in active:
        if _is_http_contributor(m):
            m.configure_app(app, container)

    # ── Step 8: collect + install middleware, sorted by EdgeSlot ────────────
    specs: list[MiddlewareSpec] = []
    for m in active:
        if _is_http_contributor(m):
            specs.extend(m.middleware(container))
    entries: list[MiddlewareEntry] = [
        (spec.target, dict(spec.kwargs)) for spec in sort_specs(specs)
    ]
    install_middleware_stack(app, entries)

    # ── Step 9: mount CRUD routers ───────────────────────────────────────────
    for router_cls in resolved_routers:
        _mount_router(app, router_cls, container)

    # ── Step 10: mount MCPAdapter / SkillAdapter ─────────────────────────────
    if mcp_adapter is not None:
        mcp_adapter.mount(app, path=mcp_path)
    if skill_adapter is not None:
        skill_adapter.mount(
            app,
            base_url=skill_base_url,
            agent_card_path=skill_agent_card_path,
            tasks_prefix=skill_tasks_prefix,
        )

    # ── Step 11: always mount HealthRouter ───────────────────────────────────
    _mount_health_router(app, container)

    _logger.info(
        "create_varco_app: created '%s' v%s with %d router(s), %d module(s), mcp=%s, skill=%s",
        title,
        version,
        len(resolved_routers),
        len(active),
        mcp_adapter is not None,
        skill_adapter is not None,
    )

    return app


# ── Internal helpers ───────────────────────────────────────────────────────────


def _default_modules_list() -> list[VarcoModule]:
    """Lazily import to avoid a hard, eager ``varco_fastapi.modules`` import
    for every ``create_varco_app`` caller that always passes ``modules=``."""
    from varco_fastapi.modules.defaults import default_modules  # noqa: PLC0415

    return default_modules()


def _is_http_contributor(module: VarcoModule) -> TypeGuard[HttpContributor]:
    from varco_fastapi.modules.base import HttpContributor  # noqa: PLC0415

    return isinstance(module, HttpContributor)


class _ModuleComponent:
    """
    Adapts a ``VarcoModule`` to ``varco_fastapi.lifespan.AbstractLifecycle``
    (``start()``/``stop()`` with no arguments) so ``VarcoLifespan`` can
    drive it — binding the ``container`` a ``VarcoModule``'s ``start``/
    ``stop`` slots require.

    Args:
        module: The module to adapt.
        container: The container this module was registered on.
    """

    def __init__(self, module: VarcoModule, container: DIContainer) -> None:
        self._module = module
        self._container = container

    async def start(self) -> None:
        await self._module.start(self._container)

    async def stop(self) -> None:
        await self._module.stop(self._container)

    def __repr__(self) -> str:
        return f"_ModuleComponent({type(self._module).__name__})"


def _warn_migration_tenancy_listed_after_core(raw_modules: Sequence[VarcoModule]) -> None:
    """
    §D-order's WARNING: a ``MigrationsModule``/``TenancyModule`` present in
    the raw input list, **positioned after** ``CoreModule`` in that same
    raw list, means the bus/job runner may start before migrations ran —
    list it first. Checked against the caller's literal list order (not
    the ``depends_on``-resolved order, which always places ``CoreModule``
    first for any module that names it as a dependency) — this is
    precisely the "list order sensitivity" §D-order documents as
    deliberately temporary; Plan 050 replaces it with phases.

    Args:
        raw_modules: The exact list passed to (or defaulted by)
            ``create_varco_app``, before ``resolve_module_order``.
    """
    from varco_core.di.module import CoreModule  # noqa: PLC0415

    from varco_fastapi.modules.lifecycle import MigrationsModule, TenancyModule  # noqa: PLC0415

    core_index: int | None = None
    for i, m in enumerate(raw_modules):
        if isinstance(m, CoreModule):
            core_index = i
            break
    if core_index is None:
        return

    for i, m in enumerate(raw_modules):
        if isinstance(m, (MigrationsModule, TenancyModule)) and i > core_index:
            _logger.warning(
                "%s is listed after CoreModule — the bus/job runner will start "
                "before it runs. List it first: modules=[%s(...), *default_modules()]. "
                "Plan 050 replaces this list-order caveat with phases.",
                type(m).__name__,
                type(m).__name__,
            )


def _warn_unowned_settings(container: DIContainer, active: Sequence[VarcoModule]) -> None:
    """
    The two kept pre-049 warnings: a set ``VARCO_MIGRATE_MODE``/
    ``VARCO_TENANCY_ISOLATION`` with no corresponding module registered —
    the failure mode that wastes the most operator time (Plan 006 step 43).

    Args:
        container: The container modules were registered on.
        active: The resolved, enabled module list.
    """
    import os as _os  # noqa: PLC0415

    from varco_fastapi.modules.lifecycle import MigrationsModule, TenancyModule  # noqa: PLC0415

    has_migrations = any(isinstance(m, MigrationsModule) for m in active)
    has_tenancy = any(isinstance(m, TenancyModule) for m in active)

    if not has_migrations and "VARCO_MIGRATE_MODE" in _os.environ:
        _logger.warning(
            "VARCO_MIGRATE_MODE=%r is set but create_varco_app() received no "
            "MigrationsModule — no migration will run. Add MigrationsModule(<migrator>) "
            "to modules= to enable auto-on-startup migrations.",
            _os.environ["VARCO_MIGRATE_MODE"],
        )
    if not has_tenancy and "VARCO_TENANCY_ISOLATION" in _os.environ:
        _logger.warning(
            "VARCO_TENANCY_ISOLATION is set but create_varco_app() received no "
            "TenancyModule — no tenancy lifecycle will run. Add "
            "TenancyModule(pool=...) to modules= to enable it."
        )


def _scan_routers(
    container: DIContainer,
    *,
    scan_packages: list[str] | None = None,
) -> list[type]:
    """
    Discover ``VarcoRouter`` subclasses registered in a providify container.

    Args:
        container:     ``DIContainer`` to query.
        scan_packages: Optional list of package names to scan before
                       querying.

    Returns:
        List of ``VarcoRouter`` concrete classes found in the container.
    """
    try:
        from varco_fastapi.router.base import VarcoRouter
    except ImportError:
        return []

    if scan_packages:
        for pkg in scan_packages:
            try:
                container.scan(pkg, recursive=True)
                _logger.debug("_scan_routers: scanned package %r", pkg)
            except Exception as exc:  # noqa: BLE001
                _logger.warning("_scan_routers: could not scan %r: %s", pkg, exc)

    try:
        instances: list[Any] = container.get_all(VarcoRouter)
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


def _mount_router(app: Any, router_cls: type, container: Any | None) -> None:
    """
    Build and mount a single ``VarcoRouter`` onto the FastAPI app.

    Args:
        app:        The ``FastAPI`` instance.
        router_cls: The ``VarcoRouter`` subclass to mount.
        container:  Container used to resolve the router instance (if needed).
    """
    build_fn = getattr(router_cls, "build_router", None)
    if build_fn is None:
        _logger.warning(
            "_mount_router: %s has no build_router() — is it a VarcoRouter? Skipping.",
            router_cls.__name__,
        )
        return

    try:
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


# ── Public API ─────────────────────────────────────────────────────────────────

__all__ = [
    "create_varco_app",
]
