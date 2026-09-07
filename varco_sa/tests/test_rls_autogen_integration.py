"""
Failing integration tests for varco_sa.rls_autogen (Plan 037 / Step 10).

Real Postgres, via the shared session-scoped ``postgres_container`` fixture.
Connects as the **non-superuser app role** (``provision_rls_app_url`` —
``varco_sa/tests/test_rls.py:126-136``) for every assertion that claims a
table is actually protected — the container's own role is a superuser and
would make every "tenant A cannot see tenant B" assertion pass regardless of
whether the generated DDL is correct (CLAUDE.md's DoD item 2).

RED until ``varco_sa/varco_sa/rls_autogen.py`` lands.
"""

from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio

from tests.conftest import provision_rls_app_url

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("VARCO_RUN_INTEGRATION"),
        reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
    ),
]


@pytest_asyncio.fixture
async def app_engine(postgres_container):
    from sqlalchemy.ext.asyncio import create_async_engine

    # Non-superuser app role — never the container's own superuser role.
    url = await provision_rls_app_url(postgres_container)
    eng = create_async_engine(url, echo=False)
    yield eng
    await eng.dispose()


class TestTenantRlsAutogenAgainstRealPostgres:
    async def test_generated_policies_isolate_tenants_on_both_tables_and_leave_global_unaffected(
        self, app_engine
    ) -> None:
        import sqlalchemy as sa
        from sqlalchemy.orm import DeclarativeBase
        from varco_sa.rls_autogen import plan_tenant_rls, tenant_rls_upgrade

        run_id = uuid.uuid4().hex[:8]
        uuid_table = f"rls_ag_uuid_{run_id}"
        string_table = f"rls_ag_string_{run_id}"
        global_table = f"rls_ag_global_{run_id}"

        class Base(DeclarativeBase):
            pass

        class _UuidTenantModel:
            class Meta:
                tenant_scope = "tenant"

        class _StringTenantModel:
            class Meta:
                tenant_scope = "tenant"

        class _GlobalModel:
            class Meta:
                tenant_scope = "global"

        tables = {
            uuid_table: sa.Table(
                uuid_table,
                Base.metadata,
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("tenant_id", sa.Uuid(), nullable=False),
                sa.Column("value", sa.Text()),
            ),
            string_table: sa.Table(
                string_table,
                Base.metadata,
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("tenant_id", sa.String(255), nullable=False),
                sa.Column("value", sa.Text()),
            ),
            global_table: sa.Table(
                global_table,
                Base.metadata,
                sa.Column("id", sa.Integer, primary_key=True),
                sa.Column("value", sa.Text()),
            ),
        }

        def lookup(cls: type) -> sa.Table:
            return {
                _UuidTenantModel: tables[uuid_table],
                _StringTenantModel: tables[string_table],
                _GlobalModel: tables[global_table],
            }[cls]

        tenant_a = str(uuid.uuid4())

        async with app_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.execute(
                sa.text(f"INSERT INTO {uuid_table} (tenant_id, value) VALUES (:t, 'a')"),
                {"t": tenant_a},
            )
            await conn.execute(
                sa.text(f"INSERT INTO {uuid_table} (tenant_id, value) VALUES (:t, 'b')"),
                {"t": str(uuid.uuid4())},
            )
            await conn.execute(sa.text(f"INSERT INTO {global_table} (value) VALUES ('shared')"))

        plans = plan_tenant_rls(
            [_UuidTenantModel, _StringTenantModel, _GlobalModel],
            base=Base,
            table_lookup=lookup,
        )

        class _SyncOpAdapter:
            def __init__(self, sync_conn):
                self._conn = sync_conn

            def execute(self, stmt: str) -> None:
                self._conn.execute(sa.text(stmt))

        try:
            async with app_engine.begin() as conn:
                await conn.run_sync(
                    lambda sync_conn: tenant_rls_upgrade(_SyncOpAdapter(sync_conn), plans=plans)
                )

            from varco_sa.rls import set_tenant_local

            async with app_engine.connect() as conn:
                await set_tenant_local(conn, tenant_a)
                result = await conn.execute(sa.text(f"SELECT value FROM {uuid_table}"))
                assert [r[0] for r in result.fetchall()] == ["a"]

            # GLOBAL table is unaffected by any tenant scoping.
            async with app_engine.connect() as conn:
                result = await conn.execute(sa.text(f"SELECT value FROM {global_table}"))
                assert [r[0] for r in result.fetchall()] == ["shared"]

            # WITH CHECK rejects a cross-tenant INSERT.
            async with app_engine.connect() as conn:
                await set_tenant_local(conn, tenant_a)
                with pytest.raises(Exception):  # noqa: B017 - a DBAPI IntegrityError subtype
                    await conn.execute(
                        sa.text(f"INSERT INTO {uuid_table} (tenant_id, value) VALUES (:t, 'x')"),
                        {"t": str(uuid.uuid4())},
                    )
                    await conn.commit()

            from varco_sa.rls_autogen import tenant_rls_downgrade

            async with app_engine.begin() as conn:
                await conn.run_sync(
                    lambda sync_conn: tenant_rls_downgrade(_SyncOpAdapter(sync_conn), plans=plans)
                )

            # Downgrade restores full visibility (both tenants' rows again).
            async with app_engine.connect() as conn:
                result = await conn.execute(sa.text(f"SELECT value FROM {uuid_table}"))
                assert len(result.fetchall()) == 2
        finally:
            async with app_engine.begin() as conn:
                await conn.run_sync(Base.metadata.drop_all)
