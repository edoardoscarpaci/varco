"""
Tests for varco_sa.rls — Postgres RLS DDL helpers (Plan 005, Phase 8, Step 86).
==================================================================================

Unit tests (no DB): ``render_rls_ddl()`` output shape — the non-negotiable
regression test for the 150x InitPlan cliff (Risks section of
``plans/005-upstream-gaps.md``): the literal ``(SELECT `` substring MUST be
present in the generated ``USING``/``WITH CHECK`` clauses, and the
``, true`` (``current_setting``'s missing-ok flag) must be present too.

Integration tests (``-m integration``, real Postgres via testcontainers):
with the policy applied via ``render_rls_ddl()``, a session that never calls
``set_tenant_local()`` sees zero rows; after ``set_tenant_local(t)`` it sees
exactly tenant ``t``'s rows; the setting does not survive the transaction.
"""

from __future__ import annotations

import os
import uuid

import pytest
import pytest_asyncio
from varco_sa.rls import render_rls_ddl, set_tenant_local

from tests.conftest import (
    RLS_READER_ROLE,
    provision_rls_app_url,
    provision_rls_reader_url,
)

# ════════════════════════════════════════════════════════════════════════════════
# Unit tests — render_rls_ddl() output shape (no DB required)
# ════════════════════════════════════════════════════════════════════════════════


class TestEnableRlsDdlInitPlanForm:
    def test_output_contains_literal_select_subquery(self) -> None:
        """
        Non-negotiable regression test for the 150x InitPlan cliff: the naive
        ``current_setting(...)`` form (no subquery) defeats the planner's
        index-usage InitPlan optimisation. Every generated clause referencing
        ``current_setting`` MUST wrap it in ``(SELECT ...)``.
        """
        ddl = render_rls_ddl("orders")
        joined = "\n".join(ddl)
        assert "(SELECT " in joined

    def test_missing_ok_true_flag_present(self) -> None:
        """
        ``current_setting(name, true)`` — the second, missing-ok argument —
        must be present so a session with no tenant set yet raises no error
        (returns NULL instead), which is what makes "unset session sees zero
        rows" the failure mode instead of a Postgres exception.
        """
        ddl = render_rls_ddl("orders")
        joined = "\n".join(ddl)
        assert ", true)" in joined

    def test_default_setting_name(self) -> None:
        ddl = render_rls_ddl("orders")
        joined = "\n".join(ddl)
        assert "rls.tenant_id" in joined

    def test_custom_setting_name(self) -> None:
        ddl = render_rls_ddl("orders", setting="rls.custom_tenant")
        joined = "\n".join(ddl)
        assert "rls.custom_tenant" in joined
        assert "rls.tenant_id" not in joined

    def test_default_tenant_column(self) -> None:
        ddl = render_rls_ddl("orders")
        joined = "\n".join(ddl)
        assert "tenant_id = " in joined

    def test_custom_tenant_column(self) -> None:
        ddl = render_rls_ddl("orders", tenant_column="org_id")
        joined = "\n".join(ddl)
        assert "org_id = " in joined
        assert "tenant_id = " not in joined

    def test_enables_and_forces_row_level_security(self) -> None:
        ddl = render_rls_ddl("orders")
        joined = "\n".join(ddl)
        assert "ENABLE ROW LEVEL SECURITY" in joined
        assert "FORCE ROW LEVEL SECURITY" in joined

    def test_creates_policy_with_using_and_with_check(self) -> None:
        ddl = render_rls_ddl("orders")
        joined = "\n".join(ddl)
        assert "CREATE POLICY" in joined
        assert "USING (" in joined
        assert "WITH CHECK (" in joined

    def test_default_policy_name(self) -> None:
        ddl = render_rls_ddl("orders")
        joined = "\n".join(ddl)
        assert "orders_tenant_isolation" in joined

    def test_custom_policy_name(self) -> None:
        ddl = render_rls_ddl("orders", policy_name="my_custom_policy")
        joined = "\n".join(ddl)
        assert "my_custom_policy" in joined
        assert "orders_tenant_isolation" not in joined

    def test_no_io_returns_plain_list_of_strings(self) -> None:
        ddl = render_rls_ddl("orders")
        assert isinstance(ddl, list)
        assert all(isinstance(stmt, str) for stmt in ddl)
        assert len(ddl) == 3


# ════════════════════════════════════════════════════════════════════════════════
# Integration tests — real Postgres, policy applied, set_tenant_local behaviour
# ════════════════════════════════════════════════════════════════════════════════

pytestmark_integration = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("VARCO_RUN_INTEGRATION"),
        reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
    ),
]


# pg_container (module-scoped) was replaced by the session-scoped
# postgres_container fixture in tests/conftest.py (Plan 012 / RT1, Step 6/9).


@pytest_asyncio.fixture
async def engine(postgres_container):
    from sqlalchemy.ext.asyncio import create_async_engine

    # Non-superuser role: the container's own role has BYPASSRLS and would
    # never see the policy enforced. See conftest.provision_rls_app_url.
    url = await provision_rls_app_url(postgres_container)
    eng = create_async_engine(url, echo=False)
    yield eng
    await eng.dispose()


@pytest_asyncio.fixture
async def session_factory(engine):
    from sqlalchemy.ext.asyncio import async_sessionmaker

    return async_sessionmaker(engine, expire_on_commit=False)


@pytest_asyncio.fixture
async def rls_protected_table(engine):
    """
    Creates a table with a non-superuser owner (RLS is a no-op for
    superusers/table owners unless FORCE is applied — testcontainers' default
    role IS the table owner, so FORCE ROW LEVEL SECURITY is what makes this
    fixture meaningful), inserts two tenants' rows, and applies the RLS
    policy via ``render_rls_ddl()``.
    """
    import sqlalchemy as sa

    table = f"rls_test_{uuid.uuid4().hex[:8]}"
    tenant_a = str(uuid.uuid4())
    tenant_b = str(uuid.uuid4())

    async with engine.begin() as conn:
        await conn.execute(
            sa.text(
                f"CREATE TABLE {table} (id SERIAL PRIMARY KEY, tenant_id UUID NOT NULL, value TEXT)"
            )
        )
        await conn.execute(
            sa.text(f"INSERT INTO {table} (tenant_id, value) VALUES (:t, 'a-row')"),
            {"t": tenant_a},
        )
        await conn.execute(
            sa.text(f"INSERT INTO {table} (tenant_id, value) VALUES (:t, 'b-row')"),
            {"t": tenant_b},
        )
        for stmt in render_rls_ddl(table):
            await conn.execute(sa.text(stmt))

    yield table, tenant_a, tenant_b

    async with engine.begin() as conn:
        await conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("VARCO_RUN_INTEGRATION"),
    reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
)
class TestRlsPolicyEnforcement:
    async def test_unset_session_sees_zero_rows(self, session_factory, rls_protected_table) -> None:
        import sqlalchemy as sa

        table, _tenant_a, _tenant_b = rls_protected_table
        async with session_factory() as session:
            result = await session.execute(sa.text(f"SELECT * FROM {table}"))
            assert result.fetchall() == []

    async def test_set_tenant_local_sees_exactly_that_tenants_rows(
        self, session_factory, rls_protected_table
    ) -> None:
        import sqlalchemy as sa

        table, tenant_a, _tenant_b = rls_protected_table
        async with session_factory() as session:
            async with session.begin():
                await set_tenant_local(session, tenant_a)
                result = await session.execute(sa.text(f"SELECT value FROM {table}"))
                rows = [r[0] for r in result.fetchall()]
                assert rows == ["a-row"]

    async def test_setting_does_not_survive_the_transaction(
        self, session_factory, rls_protected_table
    ) -> None:
        import sqlalchemy as sa

        table, tenant_a, _tenant_b = rls_protected_table
        async with session_factory() as session:
            async with session.begin():
                await set_tenant_local(session, tenant_a)
                result = await session.execute(sa.text(f"SELECT value FROM {table}"))
                assert len(result.fetchall()) == 1

            # New transaction on the same session — the SET LOCAL scope has
            # ended; no tenant is set, so RLS again hides every row.
            async with session.begin():
                result = await session.execute(sa.text(f"SELECT value FROM {table}"))
                assert result.fetchall() == []


# ════════════════════════════════════════════════════════════════════════════════
# Plan 037 / Step 2 — §D-S12-order: CREATE POLICY BEFORE ENABLE BEFORE FORCE.
#
# Brief 007 §5: enabling RLS with no policy yet applies a *default-deny*
# policy — every row invisible/immutable to non-superuser roles — for as
# long as the gap between ENABLE and CREATE POLICY lasts. render_rls_ddl()
# is a documented standalone generator (its own docstring, and
# postgres-rls.md's "Using the helpers directly"), so for a caller that does
# not run all three statements inside one transaction, today's
# ENABLE/FORCE/CREATE-POLICY order opens an unbounded default-deny window.
# The fix is a pure reorder — CREATE POLICY, ENABLE, FORCE — same three
# statements, same text, only the index changes.
# ════════════════════════════════════════════════════════════════════════════════


class TestRenderRlsDdlStatementOrder:
    def test_create_policy_is_first(self) -> None:
        ddl = render_rls_ddl("orders")
        assert ddl[0].startswith("CREATE POLICY"), ddl

    def test_enable_row_level_security_is_second(self) -> None:
        ddl = render_rls_ddl("orders")
        assert ddl[1] == "ALTER TABLE orders ENABLE ROW LEVEL SECURITY", ddl

    def test_force_row_level_security_is_third(self) -> None:
        ddl = render_rls_ddl("orders")
        assert ddl[2] == "ALTER TABLE orders FORCE ROW LEVEL SECURITY", ddl

    def test_still_exactly_three_statements(self) -> None:
        # The pre-existing len(ddl) == 3 assertion (TestEnableRlsDdlInitPlanForm
        # .test_no_io_returns_plain_list_of_strings, above) must keep passing
        # unchanged — reordering must never add/drop a statement.
        ddl = render_rls_ddl("orders")
        assert len(ddl) == 3

    def test_initplan_form_still_present_after_reorder(self) -> None:
        # The non-negotiable regression assertions from
        # TestEnableRlsDdlInitPlanForm must survive the reorder verbatim —
        # re-asserted here directly against the new statement positions.
        ddl = render_rls_ddl("orders")
        joined = "\n".join(ddl)
        assert "(SELECT " in joined
        assert ", true)" in joined


# ════════════════════════════════════════════════════════════════════════════════
# Plan 037 / Step 6 — integration regression for §D-S12-order.
#
# Executes the three render_rls_ddl() statements ONE TRANSACTION EACH, in the
# returned order, against real Postgres as the non-superuser app role
# (provision_rls_app_url) — never the container's own superuser role, which
# would make every "is the table protected" assertion below vacuously true.
# Between CREATE POLICY and ENABLE ROW LEVEL SECURITY the table must still be
# fully visible (RLS not yet on); once ENABLE has run (before FORCE, in the
# new order) the already-created policy must immediately take effect — the
# table must never pass through a policy-less, RLS-enabled, default-deny
# state, which is what today's ENABLE-before-CREATE-POLICY order permits.
# ════════════════════════════════════════════════════════════════════════════════


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("VARCO_RUN_INTEGRATION"),
    reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
)
class TestRenderRlsDdlOrderingRegressionAgainstRealPostgres:
    async def test_table_never_passes_through_a_default_deny_window(
        self, engine, postgres_container
    ) -> None:
        import sqlalchemy as sa
        from sqlalchemy.ext.asyncio import create_async_engine

        # Provisioned up front: the GRANT below references this role by name.
        reader_url = await provision_rls_reader_url(postgres_container)

        table = f"rls_order_{uuid.uuid4().hex[:8]}"
        tenant_a = str(uuid.uuid4())
        tenant_b = str(uuid.uuid4())

        # Table owned by the app role (the FORCE-matters case), created and
        # populated over the same non-superuser connection under test.
        async with engine.begin() as conn:
            await conn.execute(
                sa.text(
                    f"CREATE TABLE {table} (id SERIAL PRIMARY KEY, "
                    "tenant_id UUID NOT NULL, value TEXT)"
                )
            )
            await conn.execute(
                sa.text(f"INSERT INTO {table} (tenant_id, value) VALUES (:t, 'a-row')"),
                {"t": tenant_a},
            )
            await conn.execute(
                sa.text(f"INSERT INTO {table} (tenant_id, value) VALUES (:t, 'b-row')"),
                {"t": tenant_b},
            )

        ddl = render_rls_ddl(table)
        assert ddl[0].startswith("CREATE POLICY")
        assert ddl[1] == f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"
        assert ddl[2] == f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY"

        try:
            # Statement 1 — CREATE POLICY, RLS not enabled yet: every row
            # must still be visible (no gap opened; nothing enforced yet).
            async with engine.begin() as conn:
                await conn.execute(sa.text(ddl[0]))
                result = await conn.execute(sa.text(f"SELECT value FROM {table}"))
                assert len(result.fetchall()) == 2

            # Statement 2 — ENABLE ROW LEVEL SECURITY. This is the regression
            # point: under the OLD order (ENABLE before CREATE POLICY) the
            # table would now be RLS-enabled with NO policy, i.e. default-deny,
            # and a role subject to RLS would see ZERO rows here.
            #
            # It must be observed from a role that RLS actually applies to.
            # The `engine` role OWNS this table, and Postgres exempts an owner
            # from its policies until FORCE (statement 3) — so the owner sees
            # every row at this instant under BOTH orders and is blind to the
            # very failure being regressed. Hence the non-owning reader.
            async with engine.begin() as conn:
                await conn.execute(sa.text(ddl[1]))
                await conn.execute(sa.text(f"GRANT SELECT ON {table} TO {RLS_READER_ROLE}"))

            reader = create_async_engine(reader_url)
            try:
                async with reader.connect() as conn:
                    await set_tenant_local(conn, tenant_a)
                    result = await conn.execute(sa.text(f"SELECT value FROM {table}"))
                    # Not [] (default-deny window) and not both rows (unscoped):
                    # exactly tenant A's row. This assertion fails under the old order.
                    assert [r[0] for r in result.fetchall()] == ["a-row"]
            finally:
                await reader.dispose()

            # Statement 3 — FORCE ROW LEVEL SECURITY: still correctly scoped.
            async with engine.begin() as conn:
                await conn.execute(sa.text(ddl[2]))

            async with engine.connect() as conn:
                await set_tenant_local(conn, tenant_b)
                result = await conn.execute(sa.text(f"SELECT value FROM {table}"))
                assert [r[0] for r in result.fetchall()] == ["b-row"]
        finally:
            async with engine.begin() as conn:
                await conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))
