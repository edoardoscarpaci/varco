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
    Wraps ``varco_fastapi.tenancy.TenancyLifecycle`` — prepend this module
    before ``*default_modules()``, same reasoning as ``MigrationsModule``.

    Args:
        pool: A ``TenantResourcePool``-shaped object.
        supervisor: Optional ``TenantFanoutSupervisor``-shaped object.
        catalog_subscription: Optional callable wiring cache invalidation.
    """

    name = "tenancy"
    phase = Phase.INFRA
    depends_on = (CoreModule,)

    def __init__(
        self,
        *,
        pool: Any,
        supervisor: Any | None = None,
        catalog_subscription: Callable[[], Any] | None = None,
        enabled: bool | None = None,
    ) -> None:
        super().__init__(enabled=enabled)
        from varco_fastapi.tenancy import TenancyLifecycle  # noqa: PLC0415

        self._lifecycle = TenancyLifecycle(
            pool=pool, supervisor=supervisor, catalog_subscription=catalog_subscription
        )

    def _register(self, container: DIContainer) -> None:
        pass

    async def start(self, container: DIContainer) -> None:
        await self._lifecycle.start()

    async def stop(self, container: DIContainer) -> None:
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
        collectors: ``None`` (default) contributes the five shipped default
            collectors (§D-posture) via ``contribute(container,
            PostureCollector, …)`` in ``configure_app()``. A passed sequence
            replaces the default set entirely — nothing is contributed for
            you, though a plugin's own scanned ``PostureCollector``
            contribution still joins the report either way.

    Note:
        057's ``audited_services=``/``RedactionPostureCollector`` handoff is
        not implemented here — this repo has not yet landed Plan 057's
        redaction posture collector (§D-posture's migration note applies
        only once 057 exists).
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
    ) -> None:
        super().__init__(enabled=enabled)
        self._settings_arg = settings
        self._collectors = collectors
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
        Contribute the five built-in ``PostureCollector``s to *container*
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
            else _build_default_posture_collectors(app, container)
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
    app: Any | None, container: DIContainer
) -> list[PostureCollector]:
    """
    Build the five shipped default posture collectors, wired from what the
    composition root has on hand (§D-posture).

    Args:
        app: The ``FastAPI`` app (or ``None`` — the http collector then
            degrades to ``[]``, same as ``HttpEdgeCollector(app=None)``).
        container: The ``DIContainer`` this module was registered on.

    Returns:
        A list of ``PostureCollector`` instances, each resolved only if its
        dependency is resolvable — otherwise the collector is omitted
        entirely (§D-S9-degrade already covers the "sibling absent" case
        for the ones that are included).
    """
    from varco_fastapi.posture import (  # noqa: PLC0415
        AuthPostureCollector,
        DataPostureCollector,
        HttpEdgeCollector,
        LocalPostureCollector,
        TenantPostureCollector,
    )

    collectors: list[PostureCollector] = [
        HttpEdgeCollector(app=app),
        LocalPostureCollector(container=container),
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

    try:
        from varco_core.tenancy.membership import AbstractTenantMembership  # noqa: PLC0415
        from varco_core.tenancy.source import TenantSourceChain  # noqa: PLC0415

        chain = (
            container.get(TenantSourceChain) if container.is_resolvable(TenantSourceChain) else None
        )
        membership = (
            container.get(AbstractTenantMembership)
            if container.is_resolvable(AbstractTenantMembership)
            else None
        )
        collectors.append(
            TenantPostureCollector(chain=chain, membership=membership, delegation=None)
        )
    except Exception:  # noqa: BLE001
        pass

    collectors.append(DataPostureCollector(rls_posture=None))

    return collectors


__all__ = [
    "JwksRefreshModule",
    "MigrationsModule",
    "ReliabilityModule",
    "RetentionModule",
    "SecurityPostureModule",
    "TenancyModule",
]
