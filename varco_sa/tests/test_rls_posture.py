"""
Failing integration tests for varco_sa.tenancy.rls_check.inspect_rls_posture
(Plan 037 / Step 19, §D-S12-posture).

Real Postgres. Provisions THREE roles in a ``uuid4().hex[:8]``-namespaced
setup: the container superuser (baseline, never treated as "the app"), a
``BYPASSRLS`` role, and the plain non-superuser app role from
``provision_rls_app_url``. The superuser leg is the load-bearing assertion
that proves the test itself is not lying (brief 007 / postgres-rls.md:
"superusers/BYPASSRLS roles bypass RLS unconditionally").
"""

from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio

from tests.conftest import asyncpg_url, provision_rls_app_url

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("VARCO_RUN_INTEGRATION"),
        reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
    ),
]


@pytest_asyncio.fixture
async def posture_setup(postgres_container):
    """
    Sets up one protected table (RLS ENABLED, no FORCE — deliberately, to
    exercise the ``rls_forced=False`` assertion) plus one table with neither
    ENABLE nor a policy (``has_policy=False``), owned by the app role.

    Returns (superuser_engine, app_engine, bypassrls_engine, table_forced_off,
    table_no_policy).
    """
    import sqlalchemy as sa
    from sqlalchemy.ext.asyncio import create_async_engine

    run_id = uuid.uuid4().hex[:8]
    bypass_role = f"rls_bypass_{run_id}"
    bypass_password = "test_password"

    app_url = await provision_rls_app_url(postgres_container)
    app_engine = create_async_engine(app_url, echo=False)

    superuser_url = asyncpg_url(postgres_container)
    superuser_engine = create_async_engine(superuser_url, echo=False)

    async with superuser_engine.begin() as conn:
        await conn.execute(
            sa.text(f"CREATE ROLE {bypass_role} LOGIN BYPASSRLS PASSWORD '{bypass_password}'")
        )

    bypass_url = superuser_url.rsplit("@", 1)[1]
    bypass_full_url = f"postgresql+asyncpg://{bypass_role}:{bypass_password}@{bypass_url}"
    bypass_engine = create_async_engine(bypass_full_url, echo=False)

    table_forced_off = f"posture_forced_off_{run_id}"
    table_no_policy = f"posture_no_policy_{run_id}"

    async with app_engine.begin() as conn:
        await conn.execute(sa.text(f"CREATE TABLE {table_forced_off} (id SERIAL PRIMARY KEY)"))
        await conn.execute(sa.text(f"ALTER TABLE {table_forced_off} ENABLE ROW LEVEL SECURITY"))
        await conn.execute(sa.text(f"CREATE POLICY p ON {table_forced_off} USING (true)"))
        await conn.execute(sa.text(f"CREATE TABLE {table_no_policy} (id SERIAL PRIMARY KEY)"))

    yield superuser_engine, app_engine, bypass_engine, table_forced_off, table_no_policy

    async with superuser_engine.begin() as conn:
        await conn.execute(sa.text(f"DROP TABLE IF EXISTS {table_forced_off}"))
        await conn.execute(sa.text(f"DROP TABLE IF EXISTS {table_no_policy}"))
        await conn.execute(sa.text(f"DROP ROLE IF EXISTS {bypass_role}"))
    await superuser_engine.dispose()
    await app_engine.dispose()
    await bypass_engine.dispose()


class TestInspectRlsPostureAgainstRealPostgres:
    async def test_reports_is_superuser_and_bypassrls_correctly_per_role(
        self, posture_setup
    ) -> None:
        from varco_sa.tenancy.rls_check import inspect_rls_posture

        superuser_engine, app_engine, bypass_engine, table_forced_off, _ = posture_setup

        async with superuser_engine.connect() as conn:
            posture = await inspect_rls_posture(conn, tables=[table_forced_off])
            assert posture.is_superuser is True

        async with bypass_engine.connect() as conn:
            posture = await inspect_rls_posture(conn, tables=[table_forced_off])
            assert posture.is_superuser is False
            assert posture.rolbypassrls is True

        async with app_engine.connect() as conn:
            posture = await inspect_rls_posture(conn, tables=[table_forced_off])
            assert posture.is_superuser is False
            assert posture.rolbypassrls is False

    async def test_reports_rls_forced_false_for_enable_without_force(self, posture_setup) -> None:
        from varco_sa.tenancy.rls_check import inspect_rls_posture

        _, app_engine, _, table_forced_off, _ = posture_setup

        async with app_engine.connect() as conn:
            posture = await inspect_rls_posture(conn, tables=[table_forced_off])
            assert posture.tables[table_forced_off].rls_forced is False
            assert posture.tables[table_forced_off].rls_enabled is True
            assert posture.tables[table_forced_off].has_policy is True

    async def test_reports_has_policy_false_for_table_with_neither(self, posture_setup) -> None:
        from varco_sa.tenancy.rls_check import inspect_rls_posture

        _, app_engine, _, _, table_no_policy = posture_setup

        async with app_engine.connect() as conn:
            posture = await inspect_rls_posture(conn, tables=[table_no_policy])
            assert posture.tables[table_no_policy].has_policy is False
            assert posture.tables[table_no_policy].rls_enabled is False

    async def test_superuser_sees_every_row_despite_a_correct_policy(self, posture_setup) -> None:
        """
        The assertion that proves the test itself is not lying
        (postgres-rls.md:314-324): a table protected by a genuinely correct
        RLS policy is STILL fully visible to a superuser connection — this
        is exactly the footgun ``inspect_rls_posture`` exists to surface,
        and confirms the fixture's superuser leg is a real superuser.
        """
        import sqlalchemy as sa

        superuser_engine, app_engine, _, _, _ = posture_setup

        run_id = uuid.uuid4().hex[:8]
        table = f"posture_visibility_{run_id}"
        tenant_a = str(uuid.uuid4())
        tenant_b = str(uuid.uuid4())

        async with app_engine.begin() as conn:
            await conn.execute(
                sa.text(
                    f"CREATE TABLE {table} (id SERIAL PRIMARY KEY, "
                    "tenant_id UUID NOT NULL, value TEXT)"
                )
            )
            await conn.execute(
                sa.text(f"INSERT INTO {table} (tenant_id, value) VALUES (:t, 'a')"),
                {"t": tenant_a},
            )
            await conn.execute(
                sa.text(f"INSERT INTO {table} (tenant_id, value) VALUES (:t, 'b')"),
                {"t": tenant_b},
            )
            from varco_sa.rls import render_rls_ddl

            for stmt in render_rls_ddl(table):
                await conn.execute(sa.text(stmt))

        try:
            async with superuser_engine.connect() as conn:
                # No set_tenant_local() call — a superuser sees everything
                # regardless of RLS, by design of Postgres itself.
                result = await conn.execute(sa.text(f"SELECT value FROM {table}"))
                assert len(result.fetchall()) == 2
        finally:
            async with superuser_engine.begin() as conn:
                await conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
