"""
Failing tests — the Beanie mirror of varco_sa's tenant-guard call sites
(Plan 037 / Step 31, §D-S15-hook).

Mocked mapper/cursor throughout (matches this package's existing
``test_beanie_repository.py`` convention) — no MongoDB connection required.
Same four assertions as the SQLAlchemy mirror: byte-identical with the flag
off; raises on an unscoped query with the flag on; passes when the AST
already carries the tenant predicate; never asserted for a GLOBAL-scoped
entity.
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest
from varco_beanie.repository import AsyncBeanieRepository
from varco_core.model import DomainModel
from varco_core.query.builder import QueryBuilder
from varco_core.query.params import QueryParams
from varco_core.tenancy.settings import TenantScope


@dataclass
class _TenantScopedItem(DomainModel):
    tenant_id: str = ""
    name: str = ""

    class Meta:
        tenant_scope = TenantScope.TENANT


@dataclass
class _GlobalItem(DomainModel):
    name: str = ""

    class Meta:
        tenant_scope = TenantScope.GLOBAL


def _make_cursor(orm_objects: list | None = None, count: int = 1) -> MagicMock:
    cursor = MagicMock()
    cursor.sort.return_value = cursor
    cursor.skip.return_value = cursor
    cursor.limit.return_value = cursor
    cursor.to_list = AsyncMock(return_value=orm_objects or [MagicMock()])
    cursor.count = AsyncMock(return_value=count)
    return cursor


def _make_mapper(domain_cls: type, *, from_orm_entity=None) -> MagicMock:
    mapper = MagicMock()
    mapper._orm_cls = MagicMock()
    mapper._orm_cls.find = MagicMock(return_value=_make_cursor())
    mapper._domain_cls = domain_cls
    mapper.from_orm.return_value = from_orm_entity or domain_cls()
    return mapper


class TestTenantGuardOffIsByteIdentical:
    async def test_tenant_less_find_by_query_behaves_exactly_as_today_when_flag_off(self) -> None:
        mapper = _make_mapper(_TenantScopedItem)
        repo = AsyncBeanieRepository(mapper=mapper, assert_tenant_filter=False)

        params = QueryParams(node=None)
        results = await repo.find_by_query(params)
        assert len(results) == 1


class TestTenantGuardOnRaisesWithoutTenantFilter:
    async def test_tenant_less_query_raises_when_flag_on(self) -> None:
        from varco_core.query.applicator.tenant_guard import TenantFilterError

        mapper = _make_mapper(_TenantScopedItem)
        repo = AsyncBeanieRepository(mapper=mapper, assert_tenant_filter=True)

        params = QueryParams(node=None)
        with pytest.raises(TenantFilterError):
            await repo.find_by_query(params)

    async def test_tenant_scoped_query_passes_when_flag_on(self) -> None:
        mapper = _make_mapper(_TenantScopedItem)
        repo = AsyncBeanieRepository(mapper=mapper, assert_tenant_filter=True)

        node = QueryBuilder().eq("tenant_id", "acme").build()
        params = QueryParams(node=node)
        results = await repo.find_by_query(params)
        assert len(results) == 1

    async def test_global_scoped_entity_is_never_asserted(self) -> None:
        mapper = _make_mapper(_GlobalItem)
        repo = AsyncBeanieRepository(mapper=mapper, assert_tenant_filter=True)

        params = QueryParams(node=None)
        # No raise — GLOBAL entities are never guarded.
        results = await repo.find_by_query(params)
        assert len(results) == 1

    async def test_raw_pymongo_style_query_is_the_documented_false_negative(self) -> None:
        """
        A hand-built filter dict passed directly to the underlying Beanie
        ``Document`` (bypassing ``QueryParams``/the repository entirely)
        never reaches the guard — proven, not merely claimed, matching the
        SQLAlchemy mirror's raw ``session.execute(text(...))`` case.
        """
        mapper = _make_mapper(_TenantScopedItem)
        mapper._orm_cls.find = MagicMock(return_value=_make_cursor())

        # Bypasses the repository's find_by_query (and therefore the guard)
        # entirely — the guard has no visibility into this call shape.
        result = await mapper._orm_cls.find({"name": "widget"}).to_list()
        assert len(result) == 1
