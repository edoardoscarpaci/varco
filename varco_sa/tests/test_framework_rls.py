"""
Unit + integration tests for varco_sa.rls_framework — Plan 009, Phase 6 (R4).
=================================================================================
``framework_rls_upgrade`` / ``framework_rls_downgrade`` wrap the existing
``varco_sa.rls.render_rls_ddl`` for the two framework tables
(``varco_audit_log``, ``varco_dead_letters``).

RED until ``varco_sa/rls_framework.py`` lands.

- The non-Docker unit test asserts the emitted DDL uses the InitPlan
  ``(SELECT current_setting(...))`` form (CLAUDE.md's non-negotiable
  regression rule) without needing a real Postgres.
- The ``@pytest.mark.integration`` class needs a real Postgres instance and
  is skipped by default (same convention as the rest of the suite).
"""

from __future__ import annotations

import os

import pytest

from tests.conftest import SyncOp as _SyncOp
from tests.conftest import provision_rls_app_url


class _FakeOp:
    """Records executed DDL statements instead of running them against a
    real Alembic/Postgres connection."""

    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, stmt: str) -> None:
        self.executed.append(stmt)


class TestFrameworkRlsTables:
    def test_framework_rls_tables_constant(self) -> None:
        from varco_sa.rls_framework import FRAMEWORK_RLS_TABLES

        assert "varco_audit_log" in FRAMEWORK_RLS_TABLES
        assert "varco_dead_letters" in FRAMEWORK_RLS_TABLES


class TestFrameworkRlsUpgradeEmitsInitPlanForm:
    def test_upgrade_emits_initplan_form_for_both_tables(self) -> None:
        from varco_sa.rls_framework import framework_rls_upgrade

        op = _FakeOp()
        framework_rls_upgrade(op)

        joined = "\n".join(op.executed)
        assert "varco_audit_log" in joined
        assert "varco_dead_letters" in joined
        # Non-negotiable regression: the naive (non-InitPlan) form must NEVER
        # be emitted -- always the scalar-subquery form.
        assert "(SELECT NULLIF(current_setting(" in joined
        assert "tenant_id = current_setting(" not in joined  # naive form absent

    def test_downgrade_drops_policies_for_both_tables(self) -> None:
        from varco_sa.rls_framework import framework_rls_downgrade

        op = _FakeOp()
        framework_rls_downgrade(op)

        joined = "\n".join(op.executed)
        assert "varco_audit_log" in joined
        assert "varco_dead_letters" in joined


class TestFrameworkRlsCustomTables:
    def test_upgrade_accepts_custom_table_subset(self) -> None:
        from varco_sa.rls_framework import framework_rls_upgrade

        op = _FakeOp()
        framework_rls_upgrade(op, tables=("varco_audit_log",))

        joined = "\n".join(op.executed)
        assert "varco_audit_log" in joined
        assert "varco_dead_letters" not in joined


pytestmark_integration = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("VARCO_RUN_INTEGRATION"),
        reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
    ),
]


# _pg_container (module-scoped) was replaced by the session-scoped
# postgres_container fixture in tests/conftest.py (Plan 012 / RT1, Step 6/9).


@pytest.fixture
async def _pg_engine(postgres_container):
    from sqlalchemy.ext.asyncio import create_async_engine

    url = await provision_rls_app_url(postgres_container)
    eng = create_async_engine(url, echo=False)
    yield eng
    # The container is module-scoped, so the framework tables (fixed names,
    # unlike test_rls.py's randomised ones) would otherwise leak between
    # tests and the second CREATE POLICY would hit "policy already exists".
    await _drop_framework_tables(eng)
    await eng.dispose()


async def _drop_framework_tables(engine) -> None:
    """Drop the framework tables this module creates, ignoring absence."""
    import sqlalchemy as sa

    async with engine.begin() as conn:
        for table in ("varco_dead_letters", "varco_audit_log"):
            await conn.execute(sa.text(f"DROP TABLE IF EXISTS {table} CASCADE"))


class TestFrameworkRlsPostgresIntegration:
    """Requires a real Postgres instance (testcontainers) — mirrors
    test_rls_migration_integration.py's fixture pattern."""

    pytestmark = pytestmark_integration

    async def test_tenant_a_cannot_see_tenant_b_dead_letters_with_rls_enabled(
        self, _pg_engine
    ) -> None:
        import uuid

        import sqlalchemy as sa
        from varco_core.event.dlq import DeadLetterEntry
        from varco_sa.dlq import SADeadLetterQueue, dead_letters_metadata
        from varco_sa.rls import set_tenant_local
        from varco_sa.rls_framework import framework_rls_upgrade

        tenant_a = str(uuid.uuid4())
        tenant_b = str(uuid.uuid4())

        async with _pg_engine.begin() as conn:
            await conn.run_sync(dead_letters_metadata.create_all, checkfirst=True)
            await conn.execute(
                sa.text("ALTER TABLE varco_dead_letters ADD COLUMN IF NOT EXISTS tenant_id UUID")
            )

        dlq = SADeadLetterQueue(_pg_engine)

        async def _push_for_tenant(tenant_id: str) -> None:
            entry = DeadLetterEntry(
                event=None,
                payload=b"{}",
                channel="orders",
                handler_name="H.h",
                error_type="E",
                error_message="msg",
                attempts=1,
                tenant_id=tenant_id,
            )
            await dlq.push(entry)

        await _push_for_tenant(tenant_a)
        await _push_for_tenant(tenant_b)

        async with _pg_engine.begin() as conn:
            await conn.run_sync(
                lambda sync_conn: framework_rls_upgrade(
                    _SyncOp(sync_conn), tables=("varco_dead_letters",)
                )
            )

        async with _pg_engine.connect() as conn:
            await set_tenant_local(conn, tenant_a)
            result = await conn.execute(sa.text("SELECT tenant_id FROM varco_dead_letters"))
            rows = [str(r[0]) for r in result.fetchall()]
            assert rows == [tenant_a]

    async def test_emitted_ddl_contains_initplan_form_against_real_postgres(
        self, _pg_engine
    ) -> None:
        """The DDL applied against a real Postgres must use the InitPlan
        form -- re-asserts the non-negotiable regression with a live
        EXPLAIN-able policy, not just string inspection."""
        import sqlalchemy as sa
        from varco_sa.dlq import dead_letters_metadata
        from varco_sa.rls_framework import framework_rls_upgrade

        async with _pg_engine.begin() as conn:
            await conn.run_sync(dead_letters_metadata.create_all, checkfirst=True)
            await conn.run_sync(
                lambda sync_conn: framework_rls_upgrade(
                    _SyncOp(sync_conn), tables=("varco_dead_letters",)
                )
            )

        async with _pg_engine.connect() as conn:
            result = await conn.execute(
                sa.text("SELECT qual FROM pg_policies WHERE tablename = 'varco_dead_letters'")
            )
            row = result.fetchone()
            assert row is not None
            assert "current_setting" in row[0]


# ════════════════════════════════════════════════════════════════════════════════
# Plan 037 / Step 5 — §D-S12-order & the "What already exists" table:
# FRAMEWORK_RLS_TABLES was hand-listed and stale (only two of at least five
# tenant-carrying framework tables). ``framework_rls_tables()`` must be a
# DERIVED helper — walking ``framework_metadata()`` for tables carrying the
# tenant column — so a future framework table cannot be forgotten silently.
# ════════════════════════════════════════════════════════════════════════════════


class TestFrameworkRlsTablesDerived:
    def test_derived_set_contains_the_five_known_tenant_carrying_tables(self) -> None:
        from varco_sa.rls_framework import framework_rls_tables

        derived = framework_rls_tables()

        assert "varco_dead_letters" in derived
        assert "varco_audit_log" in derived
        # Actual table names carry no "varco_" prefix (schedule.py:57,
        # webhook.py:57) — the plan's prose named them "varco_schedules"/
        # "varco_webhook_subscriptions", which was a naming guess, not the
        # real table name; corrected here to match reality.
        assert "schedules" in derived
        assert "webhook_subscriptions" in derived
        # varco_sa/varco_sa/encryption_store.py:100 — the encryption-key-store
        # table, name asserted via its owning module's table object rather
        # than hand-typing a name that could itself drift.
        from varco_sa.encryption_store import _KEY_TABLE

        assert _KEY_TABLE.name in derived

    def test_derived_set_never_contains_the_tenant_catalog(self) -> None:
        # varco_tenants' tenant_id is its PRIMARY KEY, not a filterable
        # tenant column — an RLS policy on it would show every connection
        # exactly one row and break provisioning/fan-out. Hard-excluded.
        from varco_sa.rls_framework import framework_rls_tables

        assert "varco_tenants" not in framework_rls_tables()

    def test_old_constant_still_importable_and_matches_derived_helper(self) -> None:
        # "Keep the old constant as a module attribute computed from it so no
        # import breaks" (Step 4).
        from varco_sa.rls_framework import FRAMEWORK_RLS_TABLES, framework_rls_tables

        assert set(FRAMEWORK_RLS_TABLES) == set(framework_rls_tables())

    def test_completeness_walk_every_tenant_carrying_table_is_accounted_for(self) -> None:
        """
        Every ``framework_metadata()`` table carrying a ``tenant_id`` column
        is either in ``framework_rls_tables()`` or in a named, asserted
        exclusion list (today: only ``varco_tenants``) — so a fourteenth
        framework table with a ``tenant_id`` column can never be silently
        forgotten by this generator.
        """
        from varco_sa.metadata import framework_metadata
        from varco_sa.rls_framework import framework_rls_tables

        known_exclusions = {"varco_tenants"}
        derived = set(framework_rls_tables())

        metadata = framework_metadata()
        carrying_tenant_column = {
            table.name for table in metadata.tables.values() if "tenant_id" in table.columns
        }

        unaccounted = carrying_tenant_column - derived - known_exclusions
        assert unaccounted == set(), (
            f"Framework table(s) {unaccounted} carry a tenant_id column but are "
            "neither in framework_rls_tables() nor in the named exclusion list."
        )
