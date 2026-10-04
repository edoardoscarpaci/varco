"""
Plan 056 / FLIP-12, Step 8 — ``SAModule`` contributes and refreshes the
``RlsPostureCollector``; ``SecurityPostureModule`` stops contributing the
NOT_ASSESSED-only ``DataPostureCollector`` (red until Step 8's wiring).

sqlite, Docker-free. The full-app legs opt out of any warn-only pinning by
``delenv``-ing the ``VARCO_SECURITY_*`` variables (054 §D-flip2-tests) and
look only at ``data.*`` ids in the refusal message — unrelated default
findings (e.g. ``posture.authorizer_unbound``) may also block a bare app.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

import pytest
import sqlalchemy as sa
from providify import DIContainer
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from varco_core.posture import PostureCollector, PostureSeverity
from varco_core.tenancy.settings import TenancySettings, TenantScope
from varco_sa.tenancy.rls_check import RlsPosture, TablePosture

pytest.importorskip("varco_fastapi")


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "VARCO_SECURITY_ENFORCE",
        "VARCO_SECURITY_ENV",
        "VARCO_SECURITY_SUPPRESS",
        "VARCO_TENANCY_ENFORCE_RLS",
        "VARCO_TENANCY_ISOLATION",
    ):
        monkeypatch.delenv(name, raising=False)


@pytest.fixture
def registry() -> Iterator[Any]:
    from varco_sa.factory import SAModelRegistry

    saved = dict(SAModelRegistry._registry)
    SAModelRegistry._registry.clear()
    yield SAModelRegistry
    SAModelRegistry._registry.clear()
    SAModelRegistry._registry.update(saved)


@pytest.fixture
async def engine() -> Any:
    eng = create_async_engine("sqlite+aiosqlite:///:memory:")
    yield eng
    await eng.dispose()


def _register_tenant_table(registry: Any) -> None:
    table = sa.Table(
        "m_orders",
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("tenant_id", sa.String),
    )
    domain = type("Order", (), {"Meta": type("Meta", (), {"tenant_scope": TenantScope.TENANT})})
    registry._register(domain, type("OrderORM", (), {"__table__": table}))


def _sa_module(engine: AsyncEngine, **kw: Any) -> Any:
    from varco_sa.config import SAConfig
    from varco_sa.modules import SAModule

    class _Base(DeclarativeBase):
        pass

    return SAModule(SAConfig(engine=engine, base=_Base, entity_classes=()), **kw)


def _rls_collectors(container: DIContainer) -> list[Any]:
    from varco_core.di.collect import collect
    from varco_sa.tenancy.rls_posture import RlsPostureCollector

    return [c for c in collect(container, PostureCollector) if isinstance(c, RlsPostureCollector)]


_HIGH = RlsPosture(
    current_role="app",
    is_superuser=False,
    rolbypassrls=False,
    owned_tables=frozenset(),
    tables={"m_orders": TablePosture(rls_enabled=False, rls_forced=False, has_policy=False)},
)


# ── module-level wiring ───────────────────────────────────────────────────────


def test_sa_module_accepts_rls_tenant_column_kwarg(engine: AsyncEngine) -> None:
    _sa_module(engine, rls_tenant_column="org_id")


def test_register_contributes_exactly_one_rls_collector(engine: AsyncEngine) -> None:
    from varco_core.di.module import CoreModule

    container = DIContainer()
    CoreModule().register(container)
    _sa_module(engine).register(container)

    assert len(_rls_collectors(container)) == 1


async def test_start_refreshes_collector_sqlite_is_not_applicable(
    registry: Any, engine: AsyncEngine
) -> None:
    from varco_core.di.module import CoreModule

    _register_tenant_table(registry)
    container = DIContainer()
    CoreModule().register(container)
    module = _sa_module(engine)
    module.register(container)

    await module.start(container)
    (collector,) = _rls_collectors(container)

    assert [f.check for f in collector.collect()] == ["data.rls_not_applicable"]


async def test_bound_tenancy_settings_reach_the_collector_dialect_gate_first(
    registry: Any, engine: AsyncEngine, di_container: DIContainer, di_overrides: Any
) -> None:
    # Pins §D-flip12-candidates' gate order: the dialect gate precedes the
    # enforce_rls gate, so sqlite + enforce_rls=False is not_applicable, not
    # not_enforced (Step 8's prose says not_enforced — contradicts the table).
    from varco_core.di.module import CoreModule

    _register_tenant_table(registry)
    container = di_container
    CoreModule().register(container)
    di_overrides.instance(TenancySettings, TenancySettings(enforce_rls=False))
    module = _sa_module(engine)
    module.register(container)

    await module.start(container)
    (collector,) = _rls_collectors(container)

    assert [f.check for f in collector.collect()] == ["data.rls_not_applicable"]


def test_security_posture_module_no_longer_contributes_data_collector() -> None:
    # With the live collector present, a second "data" collector would duplicate the id.
    from varco_fastapi.modules.lifecycle import _build_default_posture_collectors
    from varco_fastapi.posture import DataPostureCollector

    defaults = _build_default_posture_collectors(None, DIContainer())

    assert not [c for c in defaults if isinstance(c, DataPostureCollector)]


def test_exactly_one_data_collector_in_the_composed_set(engine: AsyncEngine) -> None:
    from varco_core.di.collect import collect
    from varco_core.di.module import CoreModule
    from varco_fastapi.modules.lifecycle import _build_default_posture_collectors

    container = DIContainer()
    CoreModule().register(container)
    _sa_module(engine).register(container)

    composed = [
        *_build_default_posture_collectors(None, container),
        *collect(container, PostureCollector),
    ]

    assert [type(c).__name__ for c in composed if c.name == "data"] == ["RlsPostureCollector"]


# ── full-app composition under 054's derived enforce ─────────────────────────


def _app(container: DIContainer, engine: AsyncEngine, *extra: Any) -> Any:
    from varco_core.di.collect import contribute
    from varco_fastapi.app import create_varco_app
    from varco_fastapi.modules.defaults import default_modules
    from varco_sa.tenancy.rls_posture import RlsPostureCollector

    # The pre-fetched HIGH posture enters through the collector's posture= seam.
    contribute(
        container,
        PostureCollector,
        RlsPostureCollector(settings=TenancySettings(), engine_factory=None, posture=_HIGH),
    )
    return create_varco_app(
        container,
        modules=[*default_modules(), _sa_module(engine), *extra],
        routers=[],
        validate=False,
    )


async def _blocking_data_ids(container: DIContainer, engine: AsyncEngine) -> set[str]:
    app = _app(container, engine)
    try:
        async with app.router.lifespan_context(app):
            return set()
    except RuntimeError as exc:
        return set(re.findall(r"data\.[a-z_]+", str(exc)))


async def test_high_rls_posture_refuses_production_start(engine: AsyncEngine) -> None:
    assert "data.rls_disabled" in await _blocking_data_ids(DIContainer(), engine)


async def test_development_starts_with_high_finding_reported(
    monkeypatch: pytest.MonkeyPatch, engine: AsyncEngine
) -> None:
    from varco_core.di.collect import contribute
    from varco_fastapi.app import create_varco_app
    from varco_fastapi.modules import SecurityPostureModule
    from varco_fastapi.modules.defaults import default_modules
    from varco_sa.tenancy.rls_posture import RlsPostureCollector

    monkeypatch.setenv("VARCO_SECURITY_ENV", "development")
    container = DIContainer()
    contribute(
        container,
        PostureCollector,
        RlsPostureCollector(settings=TenancySettings(), engine_factory=None, posture=_HIGH),
    )
    posture = SecurityPostureModule()
    app = create_varco_app(
        container,
        modules=[*default_modules(exclude=(SecurityPostureModule,)), posture, _sa_module(engine)],
        routers=[],
        validate=False,
    )

    async with app.router.lifespan_context(app):
        report = posture.report

    assert report.enforce == "warn"
    found = [f for f in report.findings if f.check == "data.rls_disabled"]
    assert found and found[0].severity is PostureSeverity.HIGH


async def test_suppressing_rls_disabled_starts(
    monkeypatch: pytest.MonkeyPatch, engine: AsyncEngine
) -> None:
    monkeypatch.setenv("VARCO_SECURITY_SUPPRESS", "data.rls_disabled")

    assert await _blocking_data_ids(DIContainer(), engine) == set()


# ── drift decision on Plan 056: the RLS read runs after migrations ───────────


def _migrations_module(call_log: list[str]) -> Any:
    from varco_core.migration.inmemory import InMemoryMigrator
    from varco_core.migration.settings import MigrationSettings
    from varco_fastapi.modules import MigrationsModule

    return MigrationsModule(
        InMemoryMigrator(name="upgrade", call_log=call_log),
        settings=MigrationSettings(mode="upgrade"),
    )


@pytest.mark.parametrize("sa_listed_first", [True, False])
async def test_rls_refresh_runs_after_migrations_regardless_of_list_order(
    engine: AsyncEngine, sa_listed_first: bool
) -> None:
    from varco_core.di.lifecycle import LifecycleRunner, phased_components
    from varco_core.di.module import CoreModule

    log: list[str] = []
    container = DIContainer()
    CoreModule().register(container)
    sa_module = _sa_module(engine)
    sa_module.register(container)
    migrations = _migrations_module(log)

    async def _record_refresh() -> None:
        log.append("rls_refresh")

    sa_module._rls_collector.refresh = _record_refresh  # type: ignore[union-attr,method-assign]
    modules = [sa_module, migrations] if sa_listed_first else [migrations, sa_module]

    async with LifecycleRunner(phased_components(modules, container)):
        pass

    assert "upgrade" in log
    assert log.index("rls_refresh") > log.index("upgrade"), log


def test_rls_component_phase_is_after_infra_and_before_bus(engine: AsyncEngine) -> None:
    from varco_core.di.lifecycle import Phase
    from varco_sa.modules import RLS_POSTURE_PHASE

    components = {c.name: c.phase for c in _sa_module(engine).components(DIContainer())}

    assert components == {"sa": Phase.INFRA, "sa.rls_posture": RLS_POSTURE_PHASE}
    assert Phase.INFRA < RLS_POSTURE_PHASE < Phase.BUS


def test_no_hard_dependency_on_migrations_module(engine: AsyncEngine) -> None:
    # The ordering is a phase offset, never a depends_on edge: an app without
    # MigrationsModule still resolves.
    from varco_core.di.lifecycle import resolve_lifecycle_order
    from varco_core.di.module import CoreModule
    from varco_sa.modules import SAModule

    assert not [d for d in SAModule.depends_on if d.__name__ == "MigrationsModule"]
    ordered = resolve_lifecycle_order([CoreModule(), _sa_module(engine)])

    assert [type(m).__name__ for m in ordered] == ["CoreModule", "SAModule"]
