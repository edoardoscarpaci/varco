"""
Failing tests for varco_sa.tenancy.rls_session (Plan 037 / Steps 14, 16, 18;
§D-S12-hook).

Step 14 — unit, SQLite in-memory, no Docker: the ``after_begin`` mechanics
themselves (fires per-transaction, fires again after commit, uninstall,
double-install, require_tenant behaviour).

Step 16 — integration, real Postgres, the **non-superuser app role**
(``provision_rls_app_url``): the hook actually enforces RLS through a plain
repository call with no explicit ``set_tenant_local()`` anywhere in the
test, across a commit boundary (the §D-S12-hook defect this design fixes).

Step 18 — DI wiring: ``container.scan("varco_sa", recursive=True)`` installs
no listener by default; installs exactly one when
``TenancySettings.rls_set_tenant=True``.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from sqlalchemy import text
from varco_core.tenancy.settings import TenancySettings

from tests.conftest import provision_rls_app_url

# ════════════════════════════════════════════════════════════════════════════════
# Step 14 — unit tests, SQLite in-memory, no Docker.
# ════════════════════════════════════════════════════════════════════════════════


@pytest_asyncio.fixture
async def sqlite_session_factory():
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    factory = async_sessionmaker(engine, expire_on_commit=False)
    yield factory
    await engine.dispose()


class TestInstallRlsTenantHookUnit:
    async def test_hook_fires_once_per_transaction(self, sqlite_session_factory) -> None:
        from varco_sa.tenancy.rls_session import install_rls_tenant_hook

        calls: list[str] = []

        async def _fake_set_tenant_local(session, tenant_id, **kwargs):
            calls.append(tenant_id)

        uninstall = install_rls_tenant_hook(
            sqlite_session_factory, _set_tenant_local=_fake_set_tenant_local
        )
        try:
            async with sqlite_session_factory() as session:
                async with session.begin():
                    await session.execute(text("SELECT 1"))
            assert len(calls) == 1
        finally:
            uninstall()

    async def test_hook_fires_again_after_commit_on_the_same_session(
        self, sqlite_session_factory
    ) -> None:
        """
        §D-S12-hook's defect, asserted directly: a _begin()-only wiring is
        silently correct until the first commit-then-read, because COMMIT
        ends the transaction and the GUC does not survive it. after_begin
        must fire again on the very next (autobegun) transaction.
        """
        from varco_sa.tenancy.rls_session import install_rls_tenant_hook

        calls: list[str] = []

        async def _fake_set_tenant_local(session, tenant_id, **kwargs):
            calls.append(tenant_id)

        uninstall = install_rls_tenant_hook(
            sqlite_session_factory, _set_tenant_local=_fake_set_tenant_local
        )
        try:
            async with sqlite_session_factory() as session:
                async with session.begin():
                    await session.execute(text("SELECT 1"))
                # New transaction on the SAME session, post-commit.
                async with session.begin():
                    await session.execute(text("SELECT 1"))
            assert len(calls) == 2
        finally:
            uninstall()

    async def test_uninstaller_removes_the_listener_and_is_idempotent(
        self, sqlite_session_factory
    ) -> None:
        from varco_sa.tenancy.rls_session import install_rls_tenant_hook

        calls: list[str] = []

        async def _fake_set_tenant_local(session, tenant_id, **kwargs):
            calls.append(tenant_id)

        uninstall = install_rls_tenant_hook(
            sqlite_session_factory, _set_tenant_local=_fake_set_tenant_local
        )
        uninstall()
        uninstall()  # idempotent — must not raise a second time

        async with sqlite_session_factory() as session:
            async with session.begin():
                await session.execute(text("SELECT 1"))
        assert calls == []

    async def test_require_tenant_true_with_no_ambient_tenant_raises_runtime_error(
        self, sqlite_session_factory
    ) -> None:
        from varco_sa.tenancy.rls_session import install_rls_tenant_hook

        uninstall = install_rls_tenant_hook(sqlite_session_factory, require_tenant=True)
        try:
            with pytest.raises(RuntimeError) as exc:
                async with sqlite_session_factory() as session:
                    async with session.begin():
                        await session.execute(text("SELECT 1"))
            assert "tenant_context" in str(exc.value)
        finally:
            uninstall()

    async def test_require_tenant_false_with_no_ambient_tenant_sets_empty_string_no_raise(
        self, sqlite_session_factory
    ) -> None:
        from varco_sa.tenancy.rls_session import install_rls_tenant_hook

        calls: list[str] = []

        async def _fake_set_tenant_local(session, tenant_id, **kwargs):
            calls.append(tenant_id)

        uninstall = install_rls_tenant_hook(
            sqlite_session_factory, require_tenant=False, _set_tenant_local=_fake_set_tenant_local
        )
        try:
            async with sqlite_session_factory() as session:
                async with session.begin():
                    await session.execute(text("SELECT 1"))
            assert calls == [""]
        finally:
            uninstall()

    async def test_installing_twice_on_the_same_target_registers_one_listener(
        self, sqlite_session_factory
    ) -> None:
        from varco_sa.tenancy.rls_session import install_rls_tenant_hook

        calls: list[str] = []

        async def _fake_set_tenant_local(session, tenant_id, **kwargs):
            calls.append(tenant_id)

        uninstall_1 = install_rls_tenant_hook(
            sqlite_session_factory, _set_tenant_local=_fake_set_tenant_local
        )
        uninstall_2 = install_rls_tenant_hook(
            sqlite_session_factory, _set_tenant_local=_fake_set_tenant_local
        )
        try:
            async with sqlite_session_factory() as session:
                async with session.begin():
                    await session.execute(text("SELECT 1"))
            assert len(calls) == 1
        finally:
            uninstall_1()
            uninstall_2()


# ════════════════════════════════════════════════════════════════════════════════
# Step 16 — integration, real Postgres, non-superuser app role.
# ════════════════════════════════════════════════════════════════════════════════

pytestmark_integration = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not os.environ.get("VARCO_RUN_INTEGRATION"),
        reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
    ),
]


@pytest_asyncio.fixture
async def hook_engine(postgres_container):
    from sqlalchemy.ext.asyncio import create_async_engine

    # Non-superuser app role — the hook's whole point is enforcing RLS,
    # which is meaningless against a BYPASSRLS/superuser connection.
    url = await provision_rls_app_url(postgres_container)
    eng = create_async_engine(url, echo=False)
    yield eng
    await eng.dispose()


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("VARCO_RUN_INTEGRATION"),
    reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
)
class TestInstallRlsTenantHookAgainstRealPostgres:
    async def test_plain_query_scoped_by_ambient_tenant_with_no_explicit_set_tenant_local_call(
        self, hook_engine
    ) -> None:
        import uuid

        import sqlalchemy as sa
        from sqlalchemy.ext.asyncio import async_sessionmaker
        from varco_core.service.tenant import tenant_context
        from varco_sa.rls import render_rls_ddl
        from varco_sa.tenancy.rls_session import install_rls_tenant_hook

        table = f"rls_hook_{uuid.uuid4().hex[:8]}"
        tenant_a = str(uuid.uuid4())
        tenant_b = str(uuid.uuid4())

        async with hook_engine.begin() as conn:
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
            for stmt in render_rls_ddl(table):
                await conn.execute(sa.text(stmt))

        session_factory = async_sessionmaker(hook_engine, expire_on_commit=False)
        uninstall = install_rls_tenant_hook(session_factory)
        try:
            async with session_factory() as session:
                async with session.begin():
                    with tenant_context(tenant_a):
                        result = await session.execute(sa.text(f"SELECT value FROM {table}"))
                        # No explicit set_tenant_local() call anywhere in
                        # this test — the hook is what scoped this query.
                        assert [r[0] for r in result.fetchall()] == ["a"]

                # After commit, a second query in the SAME session must
                # still be correctly scoped — the regression this design
                # exists for (§D-S12-hook's commit-boundary defect).
                async with session.begin():
                    with tenant_context(tenant_a):
                        result = await session.execute(sa.text(f"SELECT value FROM {table}"))
                        assert [r[0] for r in result.fetchall()] == ["a"]

                # No ambient tenant context: zero rows, no exception.
                async with session.begin():
                    result = await session.execute(sa.text(f"SELECT value FROM {table}"))
                    assert result.fetchall() == []
        finally:
            uninstall()
            async with hook_engine.begin() as conn:
                await conn.execute(sa.text(f"DROP TABLE IF EXISTS {table}"))

    async def test_require_tenant_true_raises_runtime_error_against_real_postgres(
        self, hook_engine
    ) -> None:
        from sqlalchemy.ext.asyncio import async_sessionmaker
        from varco_sa.tenancy.rls_session import install_rls_tenant_hook

        session_factory = async_sessionmaker(hook_engine, expire_on_commit=False)
        uninstall = install_rls_tenant_hook(session_factory, require_tenant=True)
        try:
            with pytest.raises(RuntimeError):
                async with session_factory() as session:
                    async with session.begin():
                        await session.execute(text("SELECT 1"))
        finally:
            uninstall()


# ════════════════════════════════════════════════════════════════════════════════
# Step 18 — DI wiring: container.scan("varco_sa") + TenancySettings.rls_set_tenant.
# ════════════════════════════════════════════════════════════════════════════════


class TestRlsTenantHookDiWiring:
    async def test_default_settings_scan_installs_no_listener(self, di_container) -> None:
        from unittest.mock import patch

        with patch("varco_sa.tenancy.rls_session.install_rls_tenant_hook") as mock_install:
            di_container.scan("varco_sa", recursive=True)
            mock_install.assert_not_called()

    async def test_rls_set_tenant_true_installs_exactly_one_listener(self, di_container) -> None:
        from unittest.mock import patch

        from providify import Provider

        @Provider(singleton=True)
        def _settings() -> TenancySettings:
            return TenancySettings(rls_set_tenant=True)

        di_container.provide(_settings)

        with patch("varco_sa.tenancy.rls_session.install_rls_tenant_hook") as mock_install:
            di_container.scan("varco_sa", recursive=True)
            assert mock_install.call_count == 1
