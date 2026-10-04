"""
Plan 056 / FLIP-12, Step 6 — ``RlsPostureCollector`` (red until Step 7).

Docker-free: every gate in §D-flip12-candidates is exercised with either a
pre-fetched ``posture=``, a real in-memory SQLite engine, or a fake
Postgres-dialect engine whose connect raises. ``inspect_rls_posture`` is
spied so "no catalog query attempted" is asserted, not assumed.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine
from varco_core.posture import PostureSeverity
from varco_core.tenancy.settings import TenancySettings, TenantIsolation, TenantScope
from varco_sa.tenancy.rls_check import RlsPosture, TablePosture

# ── fixtures / helpers ─────────────────────────────────────────────────────────


@pytest.fixture
def registry() -> Iterator[Any]:
    """Isolated SAModelRegistry contents; restored after the test."""
    from varco_sa.factory import SAModelRegistry

    saved = dict(SAModelRegistry._registry)
    SAModelRegistry._registry.clear()
    yield SAModelRegistry
    SAModelRegistry._registry.clear()
    SAModelRegistry._registry.update(saved)


def _register_tenant_table(registry: Any, name: str = "p_orders") -> None:
    md = sa.MetaData()
    table = sa.Table(
        name, md, sa.Column("id", sa.Integer, primary_key=True), sa.Column("tenant_id", sa.String)
    )
    domain = type("Order", (), {"Meta": type("Meta", (), {"tenant_scope": TenantScope.TENANT})})
    orm = type("OrderORM", (), {"__table__": table})
    registry._register(domain, orm)


@pytest.fixture
def spy(monkeypatch: pytest.MonkeyPatch) -> list[Any]:
    """Record every inspect_rls_posture call, wherever the collector imports it from."""
    import varco_sa.tenancy.rls_check as rls_check

    calls: list[Any] = []

    async def _spy(*args: Any, **kwargs: Any) -> RlsPosture:
        calls.append((args, kwargs))
        raise AssertionError("catalog query must not be attempted")

    monkeypatch.setattr(rls_check, "inspect_rls_posture", _spy)
    try:
        import varco_sa.tenancy.rls_posture as rls_posture

        monkeypatch.setattr(rls_posture, "inspect_rls_posture", _spy, raising=False)
    except ImportError:
        pass  # red phase: the collector tests fail on their own import below
    return calls


class _FakeDialect:
    name = "postgresql"


class _RaisingCtx:
    async def __aenter__(self) -> Any:
        raise OperationalError("connect", {}, ConnectionRefusedError("refused"))

    async def __aexit__(self, *exc: Any) -> None:
        return None


class _UnreachablePgEngine:
    """Postgres-dialect engine whose every connection attempt fails."""

    dialect = _FakeDialect()

    def connect(self) -> _RaisingCtx:
        return _RaisingCtx()

    def begin(self) -> _RaisingCtx:
        return _RaisingCtx()


def _collector(**kw: Any) -> Any:
    from varco_sa.tenancy.rls_posture import RlsPostureCollector

    return RlsPostureCollector(**kw)


_HIGH_POSTURE = RlsPosture(
    current_role="app",
    is_superuser=False,
    rolbypassrls=False,
    owned_tables=frozenset(),
    tables={"p_orders": TablePosture(rls_enabled=False, rls_forced=False, has_policy=False)},
)


# ── tests ─────────────────────────────────────────────────────────────────────


def test_collector_is_a_posture_collector_named_data() -> None:
    from varco_core.posture import PostureCollector
    from varco_sa.tenancy.rls_posture import RlsPostureCollector

    assert issubclass(RlsPostureCollector, PostureCollector)
    assert RlsPostureCollector.name == "data"


def test_prefetched_posture_collect_equals_rls_findings() -> None:
    from varco_sa.tenancy.rls_posture import rls_findings

    c = _collector(settings=TenancySettings(), engine_factory=None, posture=_HIGH_POSTURE)

    assert c.collect() == rls_findings(_HIGH_POSTURE)
    assert "data.rls_disabled" in {f.check for f in c.collect()}


def test_never_refreshed_is_not_checked() -> None:
    # varco diagnose never starts modules: must report NOT_ASSESSED, never block.
    c = _collector(settings=TenancySettings(), engine_factory=lambda: _UnreachablePgEngine())

    result = c.collect()

    assert [f.check for f in result] == ["data.rls_not_checked"]
    assert result[0].severity is PostureSeverity.NOT_ASSESSED
    assert "the RLS posture refresh has not run" in result[0].detail
    # Both refresh paths are named — create_varco_app drives the component, not start().
    assert "sa.rls_posture" in result[0].detail
    assert "SAModule.start()" in result[0].detail


async def test_refresh_on_sqlite_is_not_applicable_without_query(
    registry: Any, spy: list[Any]
) -> None:
    _register_tenant_table(registry)
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        c = _collector(settings=TenancySettings(), engine_factory=lambda: engine)

        await c.refresh()
        result = c.collect()
    finally:
        await engine.dispose()

    assert [f.check for f in result] == ["data.rls_not_applicable"]
    assert result[0].severity is PostureSeverity.INFO
    assert spy == []


async def test_refresh_unreachable_db_is_not_checked_and_never_raises(
    registry: Any, caplog: pytest.LogCaptureFixture
) -> None:
    # An unreachable database is readiness's problem — never a posture refusal.
    # No spy here: this leg must reach the (failing) connection.
    _register_tenant_table(registry)
    c = _collector(settings=TenancySettings(), engine_factory=lambda: _UnreachablePgEngine())

    with caplog.at_level(logging.WARNING):
        await c.refresh()
    result = c.collect()

    assert [f.check for f in result] == ["data.rls_not_checked"]
    assert result[0].severity is PostureSeverity.NOT_ASSESSED
    assert "OperationalError" in result[0].detail
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "OperationalError" in r.getMessage()
    ]
    assert len(warnings) == 1


async def test_schema_isolation_is_not_applicable_without_query(
    registry: Any, spy: list[Any]
) -> None:
    _register_tenant_table(registry)
    c = _collector(
        settings=TenancySettings(isolation=TenantIsolation.SCHEMA),
        engine_factory=lambda: _UnreachablePgEngine(),
    )

    await c.refresh()
    result = c.collect()

    assert [f.check for f in result] == ["data.rls_not_applicable"]
    assert result[0].severity is PostureSeverity.INFO
    assert spy == []


async def test_empty_candidate_set_yields_nothing_without_query(
    registry: Any, spy: list[Any]
) -> None:
    # Single-tenant app: zero blast radius by construction.
    c = _collector(settings=TenancySettings(), engine_factory=lambda: _UnreachablePgEngine())

    await c.refresh()

    assert c.collect() == []
    assert spy == []


async def test_enforce_rls_false_with_candidates_is_not_enforced_warn(
    registry: Any, spy: list[Any]
) -> None:
    _register_tenant_table(registry)
    c = _collector(
        settings=TenancySettings(enforce_rls=False), engine_factory=lambda: _UnreachablePgEngine()
    )

    await c.refresh()
    result = c.collect()

    assert [f.check for f in result] == ["data.rls_not_enforced"]
    assert result[0].severity is PostureSeverity.WARN
    assert spy == []


async def test_enforce_rls_false_without_candidates_is_inert(registry: Any, spy: list[Any]) -> None:
    c = _collector(
        settings=TenancySettings(enforce_rls=False), engine_factory=lambda: _UnreachablePgEngine()
    )

    await c.refresh()

    assert c.collect() == []
