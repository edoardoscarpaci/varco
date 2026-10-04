"""
varco_sa.modules
===================
``SAModule`` — ``varco_sa``'s one ``VarcoModule`` (Plan 049 / DI-1, Step 8).

Not to be confused with ``varco_sa.di.SAConfiguration`` (renamed from
``SAModule`` by §D-names) — that is the providify ``@Configuration``
grouping ``@Provider`` methods, scan-installed by ``bootstrap()`` below;
this ``SAModule`` is the app-constructed, never-scanned ``VarcoModule``.
"""

from __future__ import annotations

from functools import partial
from typing import TYPE_CHECKING, Any

from varco_core.di.lifecycle import Phase
from varco_core.di.module import CoreModule, VarcoModule

if TYPE_CHECKING:
    from collections.abc import Sequence

    from providify import DIContainer
    from sqlalchemy.ext.asyncio import AsyncEngine
    from varco_core.di.lifecycle import LifecycleComponent
    from varco_core.model import DomainModel

    from varco_sa.config import SAConfig
    from varco_sa.tenancy.rls_posture import RlsPostureCollector


class SAModule(VarcoModule):
    """
    Registers ``varco_sa``'s ``@Fallback`` defaults (``bootstrap()``), an
    optional ``SAConfig`` binding, and ``AsyncRepository[D]`` bindings for
    every entity passed.

    Args:
        config: A constructed ``SAConfig``. When given, bound
            (non-fallback) via ``bind_instance`` so
            ``container.get(SAConfig)`` returns it afterward.
        entities: Domain model classes to ``bind_repositories()`` for.

    Registers:
        - ``bootstrap()``'s ``@Fallback`` defaults.
        - ``SAConfig`` (when ``config=`` is given) and one
          ``AsyncRepository[D]`` per entity.
        - One ``RlsPostureCollector`` contributed to ``PostureCollector``
          (Plan 056 / FLIP-12) — refreshed by the ``sa.rls_posture``
          component (``RLS_POSTURE_PHASE``), read by ``SecurityPostureModule``
          at ``Phase.EDGE``.

    DESIGN: the RLS posture collector is contributed here, not by
        ``TenancyModule`` or ``SecurityPostureModule`` (§D-flip12-collector)
        ✅ Plan 051's handoff verbatim, and Plan 055's rule "the module that
           owns the fact contributes the collector" — the fact is a database's
           catalog state, and this module owns the engine.
        ✅ ``varco_fastapi.tenancy`` stays free of ``varco_sa`` imports (the
           seam rule); ``SecurityPostureModule`` no longer contributes the
           NOT_ASSESSED-only ``DataPostureCollector``, so the ``data`` name is
           never duplicated.
        ✅ The catalog read happens at startup (``RLS_POSTURE_PHASE``, see
           below), so the synchronous ``collect()`` at ``Phase.EDGE`` reads a
           live result.
        ❌ An app on ``varco_sa`` that hand-wires everything without this
           module gets no ``data.*`` finding at all — the hand form
           ``RlsPostureCollector(posture=...)`` exists for it.
        ❌ ``TenancySettings`` is resolved at ``register()`` time — a binding
           added after this module registers is not seen by the collector.
        Rejected: contributing from ``TenancyModule`` (lives in
        ``varco_fastapi``, has no engine); an async ``PostureCollector``
        variant (a 051 seam change for one consumer).

    DESIGN: the RLS catalog read is its own lifecycle component at
        ``RLS_POSTURE_PHASE`` (``Phase.INFRA + 50``) — drift decision on
        Plan 056, the fresh-database first deploy
        ✅ It runs after **every** ``Phase.INFRA`` component — in particular
           ``MigrationsModule`` (``VARCO_MIGRATE_MODE=upgrade``), wherever
           either module sits in the list — so a first deploy reads the
           tables the migrations just created, not an empty catalog.
        ✅ No hard edge: ``depends_on`` would make ``MigrationsModule``
           mandatory (``ModuleDependencyError`` when absent) and would need
           a ``varco_fastapi`` import in ``varco_sa`` (wrong direction). A
           phase offset is anonymous — it orders against whatever INFRA work
           exists and costs nothing when there is none.
        ✅ Still before ``Phase.BUS`` and long before ``SecurityPostureModule``
           (``Phase.EDGE``) collects.
        ❌ Two components named ``sa``/``sa.rls_posture`` in ``varco
           diagnose`` instead of one. Accepted — that is what is true.
        Rejected: refreshing lazily inside ``collect()`` (``collect()`` is
        synchronous by the 051 contract and cannot await a catalog read).
    """

    name = "sa"
    phase = Phase.INFRA
    depends_on = (CoreModule,)

    def __init__(
        self,
        config: SAConfig | None = None,
        *,
        entities: Sequence[type[DomainModel]] = (),
        enabled: bool | None = None,
        ensure_tables: bool = True,
        rls_tenant_column: str = "tenant_id",
    ) -> None:
        """
        Args:
            config: See class docstring.
            entities: See class docstring.
            enabled: The usual Plan 047 tri-state.
            ensure_tables: Whether ``start()`` (Plan 050 / §D-stores) calls
                ``start()``/``ensure_table()`` (``checkfirst`` DDL) on every
                resolvable SA-backed framework store. Set ``False`` when an
                Alembic migration already owns every framework table this
                deployment uses (``technical_docs/features/
                schema-migrations.md``'s ten-table story) — a duplicate
                ``checkfirst`` create is harmless but redundant, and this
                flag documents the intent explicitly rather than relying on
                that harmlessness.
            rls_tenant_column: Column the RLS posture collector looks for on
                each ``TENANT``-scoped table (Plan 056 / §D-flip12-candidates).
                Default ``"tenant_id"`` — set it when the app overrides
                ``TenantAwareService._tenant_field``.
        """
        super().__init__(enabled=enabled)
        self._config = config
        self._entities = tuple(entities)
        self._ensure_tables = ensure_tables
        self._rls_tenant_column = rls_tenant_column
        self._rls_collector: RlsPostureCollector | None = None

    def _register(self, container: DIContainer) -> None:
        from varco_core.di import bind_instance  # noqa: PLC0415

        from varco_sa.config import SAConfig  # noqa: PLC0415
        from varco_sa.di import bind_repositories, bootstrap  # noqa: PLC0415

        bootstrap(container)
        if self._config is not None:
            bind_instance(container, SAConfig, self._config)
        if self._entities:
            bind_repositories(container, *self._entities)
        self._contribute_rls_collector(container)

    def _contribute_rls_collector(self, container: DIContainer) -> None:
        """
        Build and ``contribute()`` the ``RlsPostureCollector`` (Plan 056).

        Settings come from the container when ``TenancySettings`` is bound,
        else from the environment (the same optional-settings stance
        ``SAConfiguration`` takes). The engine is resolved through a factory
        at ``start()`` time, not here, so ``SAConfig`` may be bound after this
        module registers.

        Args:
            container: The container to contribute to.
        """
        from varco_core.di.collect import contribute  # noqa: PLC0415
        from varco_core.posture import PostureCollector  # noqa: PLC0415
        from varco_core.tenancy.settings import TenancySettings  # noqa: PLC0415

        from varco_sa.config import SAConfig  # noqa: PLC0415
        from varco_sa.tenancy.rls_posture import RlsPostureCollector  # noqa: PLC0415

        settings = (
            container.get(TenancySettings)
            if container.is_resolvable(TenancySettings)
            else TenancySettings()
        )

        def _engine() -> AsyncEngine | None:
            if not container.is_resolvable(SAConfig):
                return None
            return container.get(SAConfig).engine

        self._rls_collector = RlsPostureCollector(
            settings=settings, engine_factory=_engine, tenant_column=self._rls_tenant_column
        )
        contribute(container, PostureCollector, self._rls_collector)  # type: ignore[type-abstract]

    def components(self, container: DIContainer) -> Sequence[LifecycleComponent]:
        """
        Two phased components: ``sa`` (``Phase.INFRA`` — framework-table
        readiness, then ``stop()``) and ``sa.rls_posture``
        (``RLS_POSTURE_PHASE`` — the RLS catalog read, after every INFRA
        component including migrations; see the class DESIGN block).

        Args:
            container: The ``DIContainer`` this module was registered on.

        Returns:
            The two components, ``sa`` first.
        """
        from varco_core.di.lifecycle import LifecycleComponent  # noqa: PLC0415

        return [
            LifecycleComponent(
                name=self.name,
                phase=self.phase,
                start=partial(self._start_stores, container),
                stop=partial(self.stop, container),
            ),
            LifecycleComponent(
                name=f"{self.name}.rls_posture",
                phase=RLS_POSTURE_PHASE,
                start=self._refresh_rls_posture,
                stop=_noop,
            ),
        ]

    async def start(self, container: DIContainer) -> None:
        """
        The whole startup in one call, for a caller driving the module by
        hand (no ``LifecycleRunner``): framework-table readiness, then the
        RLS posture refresh. ``create_varco_app`` never calls this — it runs
        ``components()``, which schedules the refresh after migrations.

        Args:
            container: The ``DIContainer`` this module was registered on.
        """
        await self._start_stores(container)
        await self._refresh_rls_posture()

    async def _refresh_rls_posture(self) -> None:
        """
        Refresh the ``RlsPostureCollector`` (Plan 056 / FLIP-12) — at most one
        pooled connection and three catalog queries, zero I/O when a
        §D-flip12-candidates gate short-circuits. Never raises for an
        unreachable database.
        """
        if self._rls_collector is not None:
            await self._rls_collector.refresh()

    async def _start_stores(self, container: DIContainer) -> None:
        """
        Own SA framework-table readiness (§D-stores): for each of
        ``AbstractIdempotencyStore``/``AbstractDeadLetterQueue``/
        ``AbstractJobStore``/``WebhookSubscriptionRepository`` that is
        resolvable **and** resolves to a ``varco_sa`` class, await
        ``start()`` if present, else ``ensure_table()`` if present and
        ``self._ensure_tables``.

        ``CoreModule``'s ``core.stores`` component starts the same seams
        *generically* (``start()`` only, never ``ensure_table()``) — the
        two never double-run because ``start()`` is idempotent and
        ``ensure_table()`` is ``checkfirst``.

        Args:
            container: The ``DIContainer`` this module was registered on.
        """
        from varco_core.event.dlq import AbstractDeadLetterQueue  # noqa: PLC0415
        from varco_core.idempotency.base import AbstractIdempotencyStore  # noqa: PLC0415
        from varco_core.job.base import AbstractJobStore  # noqa: PLC0415
        from varco_core.webhook.base import WebhookSubscriptionRepository  # noqa: PLC0415

        for seam in (
            AbstractIdempotencyStore,
            AbstractDeadLetterQueue,
            AbstractJobStore,
            WebhookSubscriptionRepository,
        ):
            if not container.is_resolvable(seam):
                continue
            obj: Any = await container.aget(seam)
            if type(obj).__module__.split(".")[0] != "varco_sa":
                continue
            start = getattr(obj, "start", None)
            if callable(start):
                await start()
            elif self._ensure_tables:
                ensure_table = getattr(obj, "ensure_table", None)
                if callable(ensure_table):
                    await ensure_table()


#: The phase of ``SAModule``'s ``sa.rls_posture`` component — after every
#: ``Phase.INFRA`` component (migrations included), before ``Phase.BUS``.
#: Uses the gap ``Phase`` leaves between its values for exactly this.
RLS_POSTURE_PHASE: int = Phase.INFRA + 50


async def _noop() -> None:
    """The ``sa.rls_posture`` component's ``stop`` — the refresh holds nothing."""


__all__ = ["RLS_POSTURE_PHASE", "SAModule"]
