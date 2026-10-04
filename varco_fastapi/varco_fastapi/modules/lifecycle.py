"""
varco_fastapi.modules.lifecycle
===================================
The six subsystem lifecycle ``VarcoModule``s (Plan 049 / DI-1, §D-order /
§D-posture): each wraps an existing ``*Lifecycle`` class unchanged — the
module only owns construction, the tri-state ``enabled``, and delegating
``start``/``stop`` to the wrapped lifecycle's own ``start``/``stop``.

Every module here is opt-in (not part of ``default_modules()``) **except**
``SecurityPostureModule`` — FLIP-1 (``BACKLOG.md:81``): it is the one
subsystem lifecycle module in the default set, with
``VARCO_SECURITY_POSTURE_ENABLED=false`` as its escape hatch.

Thread safety:  ⚠️ Construct/register from single-threaded startup, like
                    every other ``VarcoModule``.
Async safety:   ✅ ``start``/``stop`` may perform I/O — they delegate to the
                    wrapped lifecycle object, which already documents its
                    own async safety.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from varco_core.di.lifecycle import Phase
from varco_core.di.module import CoreModule, VarcoModule

from varco_fastapi.middleware.slots import MiddlewareSpec

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from providify import DIContainer
    from varco_core.migration.base import AbstractMigrator
    from varco_core.migration.settings import MigrationSettings
    from varco_core.posture import PostureCollector, PostureFinding
    from varco_core.reliability import ReliabilityPreset
    from varco_core.retention.policy import RetentionRegistry
    from varco_core.tenancy.source import TenantSourceChain

    from varco_fastapi.posture import (
        SecurityPosture,
        SecurityPostureSettings,
    )
    from varco_fastapi.posture import SecurityPostureLifecycle as SecurityPostureLifecycleType


class MigrationsModule(VarcoModule):
    """
    Wraps ``varco_fastapi.migrate.MigrationLifecycle`` — prepend this
    module before ``*default_modules()`` so nothing else touches a table
    that does not exist yet (§D-order's WARNING guards a wrong list order).

    Args:
        *migrators: Zero or more ``AbstractMigrator`` instances, forwarded
            to ``MigrationLifecycle`` unchanged.
        settings: ``None`` → ``MigrationSettings.from_env()``.
    """

    name = "migrations"
    phase = Phase.INFRA
    depends_on = (CoreModule,)

    def __init__(
        self,
        *migrators: AbstractMigrator,
        settings: MigrationSettings | None = None,
        enabled: bool | None = None,
    ) -> None:
        super().__init__(enabled=enabled)
        from varco_core.migration.settings import MigrationSettings  # noqa: PLC0415

        from varco_fastapi.migrate import MigrationLifecycle  # noqa: PLC0415

        self._settings = settings or MigrationSettings.from_env()
        self._lifecycle = MigrationLifecycle(*migrators, settings=self._settings)

    def _register(self, container: DIContainer) -> None:
        pass

    def _enabled_from_settings(self, container: DIContainer) -> bool:
        return self._settings.mode != "off"

    async def start(self, container: DIContainer) -> None:
        await self._lifecycle.start()

    async def stop(self, container: DIContainer) -> None:
        await self._lifecycle.stop()


class TenancyModule(VarcoModule):
    """
    The composition root for tenant resolution (Plan 055 / FLIP-6): wraps
    ``varco_fastapi.tenancy.TenancyLifecycle`` (when a ``pool`` is given),
    contributes ``TenantResolutionMiddleware`` at
    ``EdgeSlot.TENANT_RESOLUTION``, and contributes the
    ``TenantPostureCollector`` — so ``tenant.*`` posture findings exist iff
    this module is present. Prepend it before ``*default_modules()``, same
    reasoning as ``MigrationsModule``.

    DESIGN: the module is the composition root for tenant resolution (Plan 055 / FLIP-6, §D-flip6-module)
        ✅ The module sees the container, so 051's multi-bound
           ``TenantSource`` pool and a bound ``DelegationPolicy`` reach the
           chain (``resolve_tenant_source_chain``) — the hand-wired
           middleware, reading env only, structurally cannot.
        ✅ The collector is built from the *same* resolved
           chain/membership the middleware is constructed with, so posture
           reports what actually runs — and a non-tenant app (no
           ``TenancyModule``) emits no ``tenant.*`` finding, which Plan
           054's derived ``refuse`` would otherwise turn into a production
           start-blocker for every app (§D-collector).
        ✅ ``TenancyLifecycle`` is unchanged; the module simply does not
           construct it without a pool.
        ❌ Grows from three keywords to nine. Accepted: a separate
           ``TenantResolutionModule`` would be two modules for one
           subsystem, and ``pool``/``catalog``/``chain`` are facts about
           one thing.
        ❌ A ``TenantResolutionMiddleware`` hand-placed via
           ``MiddlewareModule`` is unobserved by posture.
        Rejected — the middleware resolving from the process-global container:
        ❌ a service-locator site (Plan 046 removed every one).

    Args:
        pool: A ``TenantResourcePool``-shaped object, or ``None`` (default)
            — no ``TenancyLifecycle``, no ``ensure()``. Legal only under
            ``SHARED`` isolation: under ``SCHEMA``/``DATABASE`` startup
            refuses (``ConfigurationError`` from ``async_init()``, before
            any module's ``start()``, regardless of ``strict_validation``).
        catalog: An ``AbstractTenantCatalog``-shaped object, or ``None``
            (default) — no catalog-status gate at the edge (reported as
            ``tenant.no_catalog_gate``, INFO).
        supervisor: Optional ``TenantFanoutSupervisor``-shaped object.
        catalog_subscription: Optional callable wiring cache invalidation.
        chain: An explicit ``TenantSourceChain`` — wins over
            ``VARCO_TENANT_SOURCES`` (explicit code over env, never merged).
            ``None`` (default) resolves it from the container
            (``resolve_tenant_source_chain``), then — if that yields nothing
            — from ``VARCO_TENANT_SOURCES`` exactly as the middleware would,
            so the posture collector and the middleware share one answer.
        server_auth: An ``AbstractServerAuth``-shaped callable; ``None``
            (default) resolves the container's bound one, if any.
        membership: An ``AbstractTenantMembership``; ``None`` (default)
            resolves the container's bound one, if any.
        header: Forwarded to the middleware — only read by the deprecated
            implicit legacy fallback.
        reject_status: Forwarded to the middleware. Defaults to ``403``.
        enabled: Plan 047 tri-state.

    Edge cases:
        - ``chain=None`` + no ``VARCO_TENANT_SOURCES`` -> the middleware's
          deprecated implicit fallback, one ``DeprecationWarning``, and two
          WARN findings (``tenant.no_chain``,
          ``tenant.legacy_source_implicit``) that refuse a production start.
        - ``pool=None`` is legal only under ``SHARED`` isolation — anything
          else refuses startup before any ``start()``.
        - ``TenantProvenanceSettings(sources=())`` bound in code with
          ``VARCO_TENANT_SOURCES=jwt`` exported -> the env chain is used
          (and passed explicitly to the middleware), because the middleware
          would read the env itself; the collector reports that same chain.
        - ``NullTenantMembership`` (the scanned default) counts as *no*
          membership provider for posture — it always allows.
        - Inputs are resolved once and cached: ``create_varco_app`` calls
          ``configure_app()`` before ``middleware()``, and both must see
          the same chain.
    """

    name = "tenancy"
    phase = Phase.INFRA
    # ``(CoreModule,)``, not ``(FastAPIModule,)``: ``create_varco_app``
    # collects ``middleware()``/``configure_app()`` only after *every*
    # module's ``register()``, so ``AbstractServerAuth``'s binding is already
    # visible — and a hard edge would make this module unusable without
    # ``FastAPIModule`` (``ModuleDependencyError``).
    depends_on = (CoreModule,)

    def __init__(
        self,
        *,
        pool: Any | None = None,
        catalog: Any | None = None,
        supervisor: Any | None = None,
        catalog_subscription: Callable[[], Any] | None = None,
        chain: TenantSourceChain | None = None,
        server_auth: Any | None = None,
        membership: Any | None = None,
        header: str = "X-Tenant-Id",
        reject_status: int = 403,
        enabled: bool | None = None,
    ) -> None:
        super().__init__(enabled=enabled)
        self._pool = pool
        self._catalog = catalog
        self._chain = chain
        self._server_auth = server_auth
        self._membership = membership
        self._header = header
        self._reject_status = reject_status
        # (chain, server_auth, membership, delegation) — resolved once.
        self._resolved: tuple[Any, Any, Any, Any] | None = None
        self._lifecycle: Any | None = None
        if pool is not None:
            from varco_fastapi.tenancy import TenancyLifecycle  # noqa: PLC0415

            self._lifecycle = TenancyLifecycle(
                pool=pool, supervisor=supervisor, catalog_subscription=catalog_subscription
            )

    def _register(self, container: DIContainer) -> None:
        pass

    def _resolve(self, container: DIContainer) -> tuple[Any, Any, Any, Any]:
        """
        Resolve ``(chain, server_auth, membership, delegation)`` once.

        Args:
            container: The container modules were registered on.

        Returns:
            The cached tuple; each element is the explicit constructor
            value, else the container's binding when resolvable, else
            ``None``. ``chain`` additionally falls back to
            ``build_tenant_source_chain()`` (``VARCO_TENANT_SOURCES``) when
            the container yields none — the same read the middleware would
            make, so collector and middleware agree.

        Raises:
            ValueError: From ``resolve_tenant_source_chain`` or the env
                fallback — an unknown, colliding or duplicate source name
                (fail closed at startup).
        """
        if self._resolved is None:
            from varco_core.auth.delegation import DelegationPolicy  # noqa: PLC0415
            from varco_core.tenancy.di import resolve_tenant_source_chain  # noqa: PLC0415
            from varco_core.tenancy.membership import (  # noqa: PLC0415
                AbstractTenantMembership,
            )

            from varco_fastapi.auth.server_auth import AbstractServerAuth  # noqa: PLC0415

            chain = (
                self._chain if self._chain is not None else resolve_tenant_source_chain(container)
            )
            if chain is None:
                # The middleware's own chain=None path re-reads the env
                # (build_tenant_source_chain()). Do that read HERE, once, and
                # hand the result over explicitly — otherwise code-bound
                # settings and the env could disagree and the collector would
                # report a chain the middleware does not run.
                from varco_core.tenancy.settings import (  # noqa: PLC0415
                    build_tenant_source_chain,
                )

                chain = build_tenant_source_chain()
            server_auth = self._server_auth
            if server_auth is None and container.is_resolvable(AbstractServerAuth):
                server_auth = container.get(AbstractServerAuth)
            membership = self._membership
            if membership is None and container.is_resolvable(AbstractTenantMembership):
                membership = container.get(AbstractTenantMembership)
            delegation = (
                container.get(DelegationPolicy)
                if container.is_resolvable(DelegationPolicy)
                else None
            )
            self._resolved = (chain, server_auth, membership, delegation)
        return self._resolved

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        """
        Contribute ``TenantResolutionMiddleware`` at
        ``EdgeSlot.TENANT_RESOLUTION``.

        Args:
            container: The container modules were registered on.

        Returns:
            Exactly one ``MiddlewareSpec``. ``chain`` is passed as ``None``
            when nothing is configured — the middleware then takes its own
            deprecated implicit path (one deprecation site, one message).
        """
        from varco_fastapi.middleware.slots import EdgeSlot  # noqa: PLC0415
        from varco_fastapi.middleware.tenant_resolution import (  # noqa: PLC0415
            TenantResolutionMiddleware,
        )

        chain, server_auth, membership, _ = self._resolve(container)
        return [
            MiddlewareSpec(
                slot=EdgeSlot.TENANT_RESOLUTION,
                target=TenantResolutionMiddleware,
                kwargs={
                    "catalog": self._catalog,
                    "pool": self._pool,
                    "header": self._header,
                    "chain": chain,
                    "server_auth": server_auth,
                    "membership": membership,
                    "reject_status": self._reject_status,
                },
            )
        ]

    def configure_app(self, app: Any, container: DIContainer) -> None:
        """
        Contribute the ``TenantPostureCollector`` (§D-collector) built from
        the same resolved inputs the middleware receives.

        Args:
            app: The ``FastAPI`` app (unused).
            container: The container to contribute the collector to.
        """
        from varco_core.di.collect import contribute  # noqa: PLC0415
        from varco_core.posture import PostureCollector  # noqa: PLC0415

        from varco_fastapi.posture import TenantPostureCollector  # noqa: PLC0415

        chain, _, membership, delegation = self._resolve(container)
        contribute(
            container,
            PostureCollector,  # type: ignore[type-abstract]
            TenantPostureCollector(
                chain=chain,
                membership=membership,
                delegation=delegation,
                catalog_gated=self._catalog is not None,
                chain_implicit=chain is None,
            ),
        )

    async def async_init(self, container: DIContainer) -> None:
        """
        Refuse ``pool=None`` under ``SCHEMA``/``DATABASE`` isolation.

        DESIGN: a fatal check in this module's ``async_init()``, not 052's
        generic ``validate()`` severity
            ✅ ``async_init()`` runs for every active module after every
               ``register()`` (so ``TenancySettings`` is bound) and before
               any ``start()`` — the app refuses before anything routes.
            ✅ Fatal regardless of ``strict_validation`` — "optional pool"
               must never silently mean "unrouted" (§D-flip9-shared) —
               without changing 052's generic WARNING-unless-strict rule for
               every other module's ``validate()``.
            ❌ ``varco diagnose --static`` (no lifespan) only sees the
               ``validate()`` message, as a WARNING.

        Args:
            container: The container modules were registered on.

        Raises:
            ConfigurationError: Non-``SHARED`` isolation with ``pool=None``.
        """
        problems = self.validate(container)
        if problems:
            from varco_fastapi.validation import ConfigurationError  # noqa: PLC0415

            raise ConfigurationError("; ".join(problems))

    def validate(self, container: DIContainer) -> Sequence[str]:
        """
        Report ``pool=None`` under ``SCHEMA``/``DATABASE`` isolation — a
        per-tenant resource pool is load-bearing for routing there.

        Args:
            container: The container modules were registered on.

        Returns:
            One problem string, or ``()``. ``async_init()`` turns a
            non-empty result into a ``ConfigurationError`` that refuses
            startup; ``varco diagnose`` also surfaces it as
            ``module.tenancy.validate``.
        """
        from varco_core.tenancy.settings import TenancySettings, TenantIsolation  # noqa: PLC0415

        settings = (
            container.get(TenancySettings)
            if container.is_resolvable(TenancySettings)
            else TenancySettings()
        )
        if settings.isolation is not TenantIsolation.SHARED and self._pool is None:
            return (
                f"TenancyModule(pool=None) under isolation={settings.isolation.value}: a "
                "per-tenant resource pool is required to route requests; pass "
                "pool=TenantResourcePool(...)",
            )
        return ()

    async def start(self, container: DIContainer) -> None:
        """Start the wrapped ``TenancyLifecycle`` — a no-op when ``pool=None``."""
        if self._lifecycle is not None:
            await self._lifecycle.start()

    async def stop(self, container: DIContainer) -> None:
        """Stop the wrapped ``TenancyLifecycle`` — a no-op when ``pool=None``."""
        if self._lifecycle is not None:
            await self._lifecycle.stop()


class ReliabilityModule(VarcoModule):
    """
    Wraps ``varco_fastapi.reliability.ReliabilityLifecycle`` — built at
    ``start()`` time (not construction), because a ``ReliabilityLifecycle``
    needs the container to resolve ``OutboxRepository``/``AuditRepository``
    /``AbstractEventBus``, and that container is only handed to modules at
    ``register()``/``start()`` time, never at construction.

    Args:
        preset: The ``ReliabilityPreset`` to apply.
    """

    name = "reliability"
    phase = Phase.CONSUMERS
    depends_on = (CoreModule,)

    def __init__(self, preset: ReliabilityPreset, *, enabled: bool | None = None) -> None:
        super().__init__(enabled=enabled)
        self._preset = preset
        self._lifecycle: Any | None = None

    def _register(self, container: DIContainer) -> None:
        pass

    @property
    def relay(self) -> Any | None:
        """The ``OutboxRelay`` started by ``start()``, or ``None`` before
        ``start()``/when ``preset.outbox`` is ``False`` (Plan 050 / Step 10
        — a public read for tests and 052's ``varco diagnose``)."""
        if self._lifecycle is None:
            return None
        return self._lifecycle._relay  # noqa: SLF001 — same-package read

    async def start(self, container: DIContainer) -> None:
        from varco_fastapi.reliability import ReliabilityLifecycle  # noqa: PLC0415

        self._lifecycle = ReliabilityLifecycle(self._preset, container=container)
        await self._lifecycle.start()

    async def stop(self, container: DIContainer) -> None:
        if self._lifecycle is not None:
            await self._lifecycle.stop()


class RetentionModule(VarcoModule):
    """
    Wraps ``varco_fastapi.retention.RetentionLifecycle`` — appended (never
    prepended) after ``*default_modules()``, since it resolves bindings
    other lifecycle components create.

    Plan 051 / DI-5, §D-retention: no ``registry`` constructor argument any
    more — ``RetentionPolicy`` is a ``@Multibound`` collection seam.
    ``async_init()`` builds the ``RetentionRegistry`` from every
    ``RetentionPolicy`` contributed to the container (a scanned
    ``@Singleton``/``@Provider``, or ``contribute(container,
    RetentionPolicy, policy)``) — the three guards
    (``older_than``↔``supports_older_than``, duplicate ``name``) run there,
    so a misconfigured policy set aborts the lifespan at startup rather than
    at the first sweep.

    Args:
        interval: Seconds between sweeps. ``0.0`` (default) — no
            background task at all.
    """

    name = "retention"
    phase = Phase.SCHEDULERS
    depends_on = (CoreModule,)

    def __init__(self, *, interval: float = 0.0, enabled: bool | None = None) -> None:
        super().__init__(enabled=enabled)
        self._interval = interval
        self._registry: RetentionRegistry | None = None
        self._lifecycle: Any | None = None

    def _register(self, container: DIContainer) -> None:
        pass

    async def async_init(self, container: DIContainer) -> None:
        from varco_core.di.collect import acollect  # noqa: PLC0415
        from varco_core.retention.policy import RetentionPolicy, RetentionRegistry  # noqa: PLC0415

        policies = await acollect(container, RetentionPolicy)
        self._registry = RetentionRegistry(policies)

    def validate(self, container: DIContainer) -> Sequence[str]:
        from varco_core.retention.policy import RetentionPolicy  # noqa: PLC0415

        if not container.get_all_bindings(RetentionPolicy):
            return (
                "RetentionModule is enabled but no RetentionPolicy is contributed — "
                "contribute(container, RetentionPolicy, …) or remove the module",
            )
        return ()

    async def start(self, container: DIContainer) -> None:
        from varco_fastapi.retention import RetentionLifecycle  # noqa: PLC0415

        assert self._registry is not None, (  # noqa: S101 — async_init() always runs first
            "RetentionModule.start() called before async_init() — the composition root's "
            "own contract (async_init() before any module's start())."
        )
        self._lifecycle = RetentionLifecycle(
            self._registry, container=container, interval=self._interval
        )
        await self._lifecycle.start()

    async def stop(self, container: DIContainer) -> None:
        if self._lifecycle is not None:
            await self._lifecycle.stop()


class JwksRefreshModule(VarcoModule):
    """
    Wraps ``varco_fastapi.jwks.JwksRefreshLifecycle`` — appended, since the
    ``TrustedIssuerRegistry`` it drives is built by the app.

    Args:
        registry: The ``TrustedIssuerRegistry`` whose background refresh
            task this module starts/stops.
        interval: Forwarded verbatim to
            ``registry.start_refresh(interval=...)``. ``None`` (default)
            resolves to the registry's own ``ttl_seconds``.
    """

    name = "jwks_refresh"
    phase = Phase.SCHEDULERS
    depends_on = (CoreModule,)

    def __init__(
        self, registry: Any, *, interval: float | None = None, enabled: bool | None = None
    ) -> None:
        super().__init__(enabled=enabled)
        self._registry = registry
        self._interval = interval
        self._lifecycle: Any | None = None

    def _register(self, container: DIContainer) -> None:
        pass

    async def start(self, container: DIContainer) -> None:
        from varco_fastapi.jwks import JwksRefreshLifecycle  # noqa: PLC0415

        self._lifecycle = JwksRefreshLifecycle(self._registry, interval=self._interval)
        await self._lifecycle.start()

    async def stop(self, container: DIContainer) -> None:
        if self._lifecycle is not None:
            await self._lifecycle.stop()


class SecurityPostureModule(VarcoModule):
    """
    FLIP-1 (``BACKLOG.md:81``): ``SecurityPostureLifecycle`` as a module,
    **default-on** in ``default_modules()`` — every app gets the preflight
    with zero code, silenced with ``VARCO_SECURITY_POSTURE_ENABLED=false``
    or ``default_modules(exclude=(SecurityPostureModule,))``.

    Args:
        settings: ``None`` (default) reads ``SecurityPostureSettings`` from
            the container (bound by ``FastAPIModule``/``bootstrap``) or
            constructs a default. A passed instance is bound
            (non-fallback) so ``container.get(SecurityPostureSettings)``
            returns it afterward.
        collectors: ``None`` (default) contributes the four shipped default
            collectors — ``http_edge``, ``local``, ``auth``, ``redaction``
            (§D-posture; ``tenant`` moved to ``TenancyModule`` in Plan 055,
            ``data`` to ``varco_sa``'s ``SAModule`` in Plan 056) — via
            ``contribute(container, PostureCollector, …)`` in
            ``configure_app()``. A passed sequence replaces the default set
            entirely — nothing is contributed for you, though a plugin's own
            scanned/contributed ``PostureCollector`` still joins the report
            either way.
        audited_services: ``AuditLogMixin`` subclasses the default
            ``RedactionPostureCollector`` inspects for
            ``redaction.audit.disabled`` (Plan 051 §D-posture's one new
            kwarg; replaces 057's ``partial(_collect_redaction,
            service_classes=...)``). Explicit input, never discovered. Ignored
            when ``collectors=`` is passed.

    Note:
        ``DIPostureCollector`` (``di.in_process_backend``) is deliberately
        **not** a default — contribute it yourself to make in-process seams
        block a production start (``varco_fastapi.posture.DIPostureCollector``).
    """

    name = "security_posture"
    phase = Phase.EDGE
    depends_on = (CoreModule,)

    def __init__(
        self,
        settings: SecurityPostureSettings | None = None,
        *,
        enabled: bool | None = None,
        collectors: Sequence[PostureCollector | Callable[[], list[PostureFinding]]] | None = None,
        audited_services: Sequence[type] = (),
    ) -> None:
        super().__init__(enabled=enabled)
        self._settings_arg = settings
        self._collectors = collectors
        self._audited_services = tuple(audited_services)
        self._app: Any | None = None
        self._lifecycle: SecurityPostureLifecycleType | None = None

    def _register(self, container: DIContainer) -> None:
        if self._settings_arg is not None:
            # See varco_fastapi.modules.edge.SecurityHeadersModule._register()'s
            # comment — bind_instance, not bind_settings(..., instance=...):
            # FastAPIModule's bootstrap() always binds a plain @Fallback
            # SecurityPostureSettings first (default_modules()'s stable list
            # order lists FastAPIModule before SecurityPostureModule — NOT
            # this class's own depends_on=(CoreModule,), which says nothing
            # about ordering relative to FastAPIModule), and
            # bind_settings(..., instance=...) raises SettingsAlreadyBoundError
            # against any prior binding at all, fallback or not.
            from varco_core.di import bind_instance  # noqa: PLC0415

            from varco_fastapi.posture import SecurityPostureSettings  # noqa: PLC0415

            bind_instance(container, SecurityPostureSettings, self._settings_arg)

    def _resolved_settings(self, container: DIContainer) -> SecurityPostureSettings:
        from varco_fastapi.posture import SecurityPostureSettings  # noqa: PLC0415

        if self._settings_arg is not None:
            return self._settings_arg
        try:
            return container.get(SecurityPostureSettings)
        except Exception:  # noqa: BLE001
            return SecurityPostureSettings()

    def _enabled_from_settings(self, container: DIContainer) -> bool:
        return self._resolved_settings(container).enabled

    def configure_app(self, app: Any, container: DIContainer) -> None:
        """
        Contribute the four built-in ``PostureCollector``s to *container*
        (§D-posture) — or, when ``collectors=`` was passed to the
        constructor, contribute exactly those instead. Runs only for a
        module that is both enabled and reaches ``configure_app()`` (a
        disabled module leaves no collector bindings behind).
        """
        self._app = app
        from varco_core.di.collect import contribute  # noqa: PLC0415
        from varco_core.posture import PostureCollector  # noqa: PLC0415

        collectors: Sequence[PostureCollector | Callable[[], list[PostureFinding]]]
        collectors = (
            self._collectors
            if self._collectors is not None
            else _build_default_posture_collectors(
                app, container, audited_services=self._audited_services
            )
        )
        for collector in collectors:
            if isinstance(collector, PostureCollector):
                contribute(container, PostureCollector, collector)  # type: ignore[type-abstract]

    def middleware(self, container: DIContainer) -> list[MiddlewareSpec]:
        return []

    @property
    def report(self) -> SecurityPosture:
        """The report produced by the most recent ``start()``.

        Raises:
            RuntimeError: ``start()`` has not run yet.
        """
        if self._lifecycle is None:
            raise RuntimeError("SecurityPostureModule has not started — call start() first.")
        return self._lifecycle.report

    def validate(self, container: DIContainer) -> Sequence[str]:
        from varco_core.posture import PostureCollector  # noqa: PLC0415

        if self._collectors is None and not container.get_all_bindings(PostureCollector):
            return ("SecurityPostureModule is enabled but no PostureCollector is contributed",)
        return ()

    async def start(self, container: DIContainer) -> None:
        from varco_core.di.collect import acollect  # noqa: PLC0415
        from varco_core.posture import PostureCollector  # noqa: PLC0415

        from varco_fastapi.posture import SecurityPostureLifecycle  # noqa: PLC0415

        collectors: Sequence[PostureCollector | Callable[[], list[PostureFinding]]]
        if self._collectors is not None:
            collectors = self._collectors
        else:
            collectors = await acollect(container, PostureCollector)
        self._lifecycle = SecurityPostureLifecycle(
            collectors=collectors, settings=self._resolved_settings(container)
        )
        await self._lifecycle.start()

    async def stop(self, container: DIContainer) -> None:
        if self._lifecycle is not None:
            await self._lifecycle.stop()


def _build_default_posture_collectors(
    app: Any | None,
    container: DIContainer,
    *,
    audited_services: Sequence[type] = (),
) -> list[PostureCollector]:
    """
    Build the four shipped default posture collectors (http, local, auth,
    redaction), wired from what the composition root has on hand
    (§D-posture).

    Args:
        app: The ``FastAPI`` app (or ``None`` — the http collector then
            degrades to ``[]``, same as ``HttpEdgeCollector(app=None)``).
        container: The ``DIContainer`` this module was registered on.
        audited_services: Forwarded to ``RedactionPostureCollector`` as
            ``service_classes`` (``SecurityPostureModule(audited_services=)``).

    Returns:
        A list of ``PostureCollector`` instances, each resolved only if its
        dependency is resolvable — otherwise the collector is omitted
        entirely (§D-S9-degrade already covers the "sibling absent" case
        for the ones that are included).
    """
    from varco_fastapi.posture import (  # noqa: PLC0415
        AuthPostureCollector,
        HttpEdgeCollector,
        LocalPostureCollector,
        RedactionPostureCollector,
    )

    # DESIGN: envelope_settings=None for the redaction collector — the
    # inspector then reads ErrorEnvelopeSettings() fresh from the
    # environment, which is exactly what varco_fastapi.exceptions' handler
    # does per error. Resolving the container's binding instead could report
    # a code-bound instance the handler never reads.
    collectors: list[PostureCollector] = [
        HttpEdgeCollector(app=app),
        LocalPostureCollector(container=container),
        RedactionPostureCollector(service_classes=audited_services),
    ]

    try:
        from varco_core.authority.registry import TrustedIssuerRegistry  # noqa: PLC0415
        from varco_core.revocation import AbstractTokenRevocationStore  # noqa: PLC0415

        from varco_fastapi.auth.server_auth import AbstractServerAuth  # noqa: PLC0415

        auth = (
            container.get(AbstractServerAuth)
            if container.is_resolvable(AbstractServerAuth)
            else None
        )
        registry = (
            container.get(TrustedIssuerRegistry)
            if container.is_resolvable(TrustedIssuerRegistry)
            else None
        )
        store = (
            container.get(AbstractTokenRevocationStore)
            if container.is_resolvable(AbstractTokenRevocationStore)
            else None
        )
        collectors.append(AuthPostureCollector(auth=auth, registry=registry, store=store))
    except Exception:  # noqa: BLE001 — sibling module absent; skip this collector entirely
        pass

    # Plan 055 §D-collector: no TenantPostureCollector here — TenancyModule
    # contributes it, built from the chain/membership its middleware really
    # uses. Contributing one with chain=None for every app made a stock 4.0
    # app with no tenancy refuse a production start (tenant.no_chain WARN).

    # Plan 056 / FLIP-12: no DataPostureCollector here either — SAModule
    # contributes the live RlsPostureCollector (same "data" name). The old
    # default only ever produced data.rls_not_checked, and beside the live
    # collector it would duplicate the name (posture.duplicate_collector).

    # Plan 054 (decision on drift): no DIPostureCollector here — in-process
    # backends are a legitimate deployment shape, so di.in_process_backend is
    # opt-in (the app contributes DIPostureCollector(container) itself).

    return collectors


__all__ = [
    "JwksRefreshModule",
    "MigrationsModule",
    "ReliabilityModule",
    "RetentionModule",
    "SecurityPostureModule",
    "TenancyModule",
]
