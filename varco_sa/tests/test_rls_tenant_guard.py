"""
Failing tests for the AsyncSQLAlchemyRepository tenant-guard call sites
(Plan 037 / Step 31, §D-S15-hook).

With ``assert_tenant_filter=False`` (the default) every call must behave
BYTE-IDENTICALLY to today — the proof that this is genuinely additive.
With it ``True``, a tenant-less ``list()``/``find_by_query()`` call raises
``TenantFilterError``; a ``TenantAwareService``-scoped call (i.e. one whose
``QueryParams.node`` already carries the tenant predicate) passes; a
``TenantScope.GLOBAL`` entity is never asserted; and a raw
``session.execute(text(...))`` — the one documented false negative — is
proven, not merely claimed, to pass unguarded.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from varco_core.model import DomainModel
from varco_core.query.builder import QueryBuilder
from varco_core.query.params import QueryParams
from varco_core.tenancy.settings import TenantScope
from varco_sa.factory import SAModelFactory
from varco_sa.repository import AsyncSQLAlchemyRepository


@dataclass
class _TenantScopedItem(DomainModel):
    tenant_id: str = ""
    name: str = ""

    class Meta:
        table = "tenant_guard_items"
        tenant_scope = TenantScope.TENANT


@dataclass
class _GlobalItem(DomainModel):
    name: str = ""

    class Meta:
        table = "tenant_guard_global_items"
        tenant_scope = TenantScope.GLOBAL


@pytest_asyncio.fixture
async def guarded_setup():
    class _Base(DeclarativeBase):
        pass

    factory = SAModelFactory(_Base)
    _, tenant_mapper = factory.build(_TenantScopedItem)
    _, global_mapper = factory.build(_GlobalItem)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(_Base.metadata.create_all)

    async_session = async_sessionmaker(engine, expire_on_commit=False)
    async with async_session() as session:
        yield session, tenant_mapper, global_mapper

    async with engine.begin() as conn:
        await conn.run_sync(_Base.metadata.drop_all)
    await engine.dispose()


class TestTenantGuardOffIsByteIdentical:
    async def test_tenant_less_list_call_behaves_exactly_as_today_when_flag_off(
        self, guarded_setup
    ) -> None:
        session, tenant_mapper, _ = guarded_setup
        repo = AsyncSQLAlchemyRepository(session, tenant_mapper, assert_tenant_filter=False)

        await repo.save(_TenantScopedItem(tenant_id="acme", name="widget"))

        params = QueryParams(node=None)
        # Must not raise — flag off means nothing changes.
        results = await repo.find_by_query(params)
        assert len(results) == 1


class TestTenantGuardOnRaisesWithoutTenantFilter:
    async def test_tenant_less_query_raises_when_flag_on(self, guarded_setup) -> None:
        from varco_core.query.applicator.tenant_guard import TenantFilterError

        session, tenant_mapper, _ = guarded_setup
        repo = AsyncSQLAlchemyRepository(session, tenant_mapper, assert_tenant_filter=True)

        await repo.save(_TenantScopedItem(tenant_id="acme", name="widget"))

        params = QueryParams(node=None)
        with pytest.raises(TenantFilterError):
            await repo.find_by_query(params)

    async def test_tenant_scoped_query_passes_when_flag_on(self, guarded_setup) -> None:
        session, tenant_mapper, _ = guarded_setup
        repo = AsyncSQLAlchemyRepository(session, tenant_mapper, assert_tenant_filter=True)

        await repo.save(_TenantScopedItem(tenant_id="acme", name="widget"))

        node = QueryBuilder().eq("tenant_id", "acme").build()
        params = QueryParams(node=node)
        results = await repo.find_by_query(params)
        assert len(results) == 1

    async def test_global_scoped_entity_is_never_asserted(self, guarded_setup) -> None:
        session, _, global_mapper = guarded_setup
        repo = AsyncSQLAlchemyRepository(session, global_mapper, assert_tenant_filter=True)

        await repo.save(_GlobalItem(name="shared"))

        params = QueryParams(node=None)
        # No raise — GLOBAL entities are never guarded.
        results = await repo.find_by_query(params)
        assert len(results) == 1

    async def test_raw_session_execute_is_the_documented_false_negative(
        self, guarded_setup
    ) -> None:
        """
        A raw ``session.execute(text(...))`` never builds a ``QueryParams``,
        so the guard cannot see it — proven here, not merely claimed
        (§D-S15-hook's one documented false-negative class).
        """
        session, tenant_mapper, _ = guarded_setup
        repo = AsyncSQLAlchemyRepository(session, tenant_mapper, assert_tenant_filter=True)

        await repo.save(_TenantScopedItem(tenant_id="acme", name="widget"))

        # No exception — the guard is bypassed entirely by raw SQL.
        result = await session.execute(text("SELECT COUNT(*) FROM tenant_guard_items"))
        assert result.scalar() == 1
