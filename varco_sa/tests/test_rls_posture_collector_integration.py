"""
Plan 056 / FLIP-12, Step 9 — ``RlsPostureCollector.refresh()`` against a real
Postgres, across the three role shapes (red until Step 7).

Roles: ``provision_rls_app_url`` (non-superuser, OWNS its tables),
``provision_rls_reader_url`` (non-superuser, owns nothing), and the
container's own role (superuser + BYPASSRLS). Every table is
``f"t_{uuid4().hex[:8]}"``; DDL comes from ``render_rls_ddl()`` so the
statement order is the shipped one. Each leg asserts the EXACT id set.

Legs that are not about the GUC setter construct the collector with
``rls_set_tenant=True`` so ``data.rls_guc_setter_absent`` does not mask the
id set under test.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
import pytest_asyncio
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine
from varco_core.tenancy.settings import TenancySettings, TenantScope

from tests.conftest import (
    RLS_READER_ROLE,
    asyncpg_url,
    provision_rls_app_url,
    provision_rls_reader_url,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("VARCO_RUN_INTEGRATION"),
        reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
    ),
]


@pytest.fixture
def registry() -> Iterator[Any]:
    from varco_sa.factory import SAModelRegistry

    saved = dict(SAModelRegistry._registry)
    SAModelRegistry._registry.clear()
    yield SAModelRegistry
    SAModelRegistry._registry.clear()
    SAModelRegistry._registry.update(saved)


def _register(registry: Any, table_name: str) -> None:
    table = sa.Table(
        table_name,
        sa.MetaData(),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("tenant_id", sa.Uuid),
    )
    domain = type(
        f"D_{table_name}", (), {"Meta": type("Meta", (), {"tenant_scope": TenantScope.TENANT})}
    )
    registry._register(domain, type(f"O_{table_name}", (), {"__table__": table}))


@pytest_asyncio.fixture
async def engines(postgres_container: Any) -> AsyncIterator[dict[str, AsyncEngine]]:
    app = create_async_engine(await provision_rls_app_url(postgres_container))
    reader = create_async_engine(await provision_rls_reader_url(postgres_container))
    superuser = create_async_engine(asyncpg_url(postgres_container))
    yield {"app": app, "reader": reader, "superuser": superuser}
    for eng in (app, reader, superuser):
        await eng.dispose()


@pytest_asyncio.fixture
async def table(engines: dict[str, AsyncEngine], registry: Any) -> AsyncIterator[str]:
    """A fresh tenant table OWNED by the app role, readable by the reader role, no RLS yet."""
    name = f"t_{uuid.uuid4().hex[:8]}"
    async with engines["app"].begin() as conn:
        await conn.execute(
            sa.text(
                f"CREATE TABLE {name} (id SERIAL PRIMARY KEY, tenant_id UUID NOT NULL, value TEXT)"
            )
        )
        await conn.execute(sa.text(f"GRANT SELECT ON {name} TO {RLS_READER_ROLE}"))
    _register(registry, name)
    yield name
    async with engines["superuser"].begin() as conn:
        await conn.execute(sa.text(f"DROP TABLE IF EXISTS {name}"))


async def _ddl(engine: AsyncEngine, statements: list[str]) -> None:
    async with engine.begin() as conn:
        for stmt in statements:
            await conn.execute(sa.text(stmt))


async def _ids(engine: AsyncEngine, *, rls_set_tenant: bool = True) -> set[str]:
    from varco_sa.tenancy.rls_posture import RlsPostureCollector

    collector = RlsPostureCollector(
        settings=TenancySettings(rls_set_tenant=rls_set_tenant), engine_factory=lambda: engine
    )
    await collector.refresh()
    return {f.check for f in collector.collect()}


async def test_a_no_rls_app_role_is_rls_disabled(
    engines: dict[str, AsyncEngine], table: str
) -> None:
    from varco_sa.tenancy.rls_posture import RlsPostureCollector

    collector = RlsPostureCollector(
        settings=TenancySettings(rls_set_tenant=True), engine_factory=lambda: engines["app"]
    )
    await collector.refresh()
    findings = collector.collect()

    assert {f.check for f in findings} == {"data.rls_disabled"}
    assert table in findings[0].detail


async def test_b_policy_enable_no_force_owner_vs_reader(
    engines: dict[str, AsyncEngine], table: str
) -> None:
    # The owner bypasses unforced RLS; a non-owner does not — premise 4's narrowing.
    ddl = render_without_force(table)
    await _ddl(engines["app"], ddl)

    assert await _ids(engines["app"]) == {"data.rls_not_forced", "data.role_bypasses_rls"}
    assert await _ids(engines["reader"]) == {"data.rls_not_forced"}


async def test_c_forced_app_role_is_clean_except_guc(
    engines: dict[str, AsyncEngine], table: str
) -> None:
    from varco_sa.rls import render_rls_ddl

    await _ddl(engines["app"], render_rls_ddl(table))

    assert await _ids(engines["app"], rls_set_tenant=True) == set()
    assert await _ids(engines["app"], rls_set_tenant=False) == {"data.rls_guc_setter_absent"}


async def test_c_hook_installed_marker_clears_guc_finding(
    engines: dict[str, AsyncEngine], table: str
) -> None:
    from varco_sa.rls import render_rls_ddl
    from varco_sa.tenancy.rls_session import install_rls_tenant_hook

    await _ddl(engines["app"], render_rls_ddl(table))
    uninstall = install_rls_tenant_hook(async_sessionmaker(engines["app"]))
    try:
        assert await _ids(engines["app"], rls_set_tenant=False) == set()
    finally:
        uninstall()


async def test_d_enable_without_policy_is_default_deny_warn(
    engines: dict[str, AsyncEngine], table: str
) -> None:
    await _ddl(
        engines["app"],
        [
            f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY",
            f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY",
        ],
    )

    assert await _ids(engines["app"]) == {"data.rls_enabled_without_policy"}


async def test_e_superuser_on_forced_table_bypasses(
    engines: dict[str, AsyncEngine], table: str
) -> None:
    from varco_sa.rls import render_rls_ddl

    await _ddl(engines["app"], render_rls_ddl(table))

    assert await _ids(engines["superuser"]) == {"data.role_bypasses_rls"}


async def test_f_hook_scopes_rows_and_collector_reports_clean(
    engines: dict[str, AsyncEngine], table: str
) -> None:
    # Ties FLIP-12's report to the mechanism it asserts: no set_tenant_local() anywhere here.
    from varco_core.service.tenant import tenant_context
    from varco_sa.rls import render_rls_ddl
    from varco_sa.tenancy.rls_session import install_rls_tenant_hook

    tenant_a, tenant_b = str(uuid.uuid4()), str(uuid.uuid4())
    async with engines["superuser"].begin() as conn:
        for t, v in ((tenant_a, "a"), (tenant_b, "b")):
            await conn.execute(
                sa.text(f"INSERT INTO {table} (tenant_id, value) VALUES (:t, :v)"), {"t": t, "v": v}
            )
    await _ddl(engines["app"], render_rls_ddl(table))

    factory = async_sessionmaker(engines["app"], expire_on_commit=False)
    uninstall = install_rls_tenant_hook(factory)
    try:
        async with factory() as session, session.begin():
            with tenant_context(tenant_a):
                rows = (await session.execute(sa.text(f"SELECT value FROM {table}"))).fetchall()
        ids = await _ids(engines["app"], rls_set_tenant=False)
    finally:
        uninstall()

    assert [r[0] for r in rows] == ["a"]
    assert ids == set()


async def test_g_missing_candidate_table_is_absent_not_disabled(
    engines: dict[str, AsyncEngine], registry: Any
) -> None:
    # Fresh database, first deploy: the candidate is registered but never created.
    missing = f"t_{uuid.uuid4().hex[:8]}"
    _register(registry, missing)

    from varco_sa.tenancy.rls_posture import RlsPostureCollector

    collector = RlsPostureCollector(
        settings=TenancySettings(rls_set_tenant=True), engine_factory=lambda: engines["app"]
    )
    await collector.refresh()
    findings = collector.collect()

    assert {f.check for f in findings} == {"data.rls_table_absent"}
    assert missing in findings[0].detail


async def test_g_missing_beside_existing_unprotected_reports_both(
    engines: dict[str, AsyncEngine], table: str, registry: Any
) -> None:
    missing = f"t_{uuid.uuid4().hex[:8]}"
    _register(registry, missing)

    from varco_sa.tenancy.rls_posture import RlsPostureCollector

    collector = RlsPostureCollector(
        settings=TenancySettings(rls_set_tenant=True), engine_factory=lambda: engines["app"]
    )
    await collector.refresh()
    by_id = {f.check: f for f in collector.collect()}

    assert set(by_id) == {"data.rls_disabled", "data.rls_table_absent"}
    assert table in by_id["data.rls_disabled"].detail
    assert missing not in by_id["data.rls_disabled"].detail


def render_without_force(table: str) -> list[str]:
    """render_rls_ddl()'s statements minus FORCE — the 'policy + ENABLE, no FORCE' state."""
    from varco_sa.rls import render_rls_ddl

    return [s for s in render_rls_ddl(table) if "FORCE" not in s.upper()]
