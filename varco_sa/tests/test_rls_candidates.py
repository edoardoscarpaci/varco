"""
Plan 056 / FLIP-12, Step 4 — ``rls_candidate_tables()`` and
``SAModelRegistry.registered()`` (red until Step 5).

Docker-free: every table is a hand-built ``sa.Table`` resolved through a
``table_lookup=`` stub, so no engine and no generated ORM class is needed.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
import sqlalchemy as sa
from varco_core.tenancy.settings import TenantScope

_MD = sa.MetaData()


def _table(name: str, *cols: str) -> sa.Table:
    return sa.Table(
        name,
        _MD,
        sa.Column("id", sa.Integer, primary_key=True),
        *(sa.Column(c, sa.String) for c in cols),
    )


def _cls(name: str, scope: TenantScope = TenantScope.TENANT) -> type:
    meta = type("Meta", (), {"tenant_scope": scope})
    return type(name, (), {"Meta": meta})


Tenanted = _cls("Tenanted")
Global = _cls("Global", TenantScope.GLOBAL)
NoColumn = _cls("NoColumn")
Unknown = _cls("Unknown")
OrgScoped = _cls("OrgScoped")
TenantsCatalog = _cls("TenantsCatalog")

_TABLES: dict[type, sa.Table] = {
    Tenanted: _table("c_tenanted", "tenant_id"),
    Global: _table("c_global", "tenant_id"),
    NoColumn: _table("c_nocolumn"),
    OrgScoped: _table("c_org", "org_id"),
    TenantsCatalog: _table("varco_tenants", "tenant_id"),
}


def _lookup(domain_cls: type) -> sa.Table:
    return _TABLES[domain_cls]  # KeyError for Unknown — the "never registered" shape


def test_tenant_class_with_tenant_column_is_a_candidate() -> None:
    from varco_sa.rls_autogen import rls_candidate_tables

    assert rls_candidate_tables([Tenanted], table_lookup=_lookup) == ("c_tenanted",)


def test_global_class_is_excluded() -> None:
    from varco_sa.rls_autogen import rls_candidate_tables

    assert rls_candidate_tables([Global], table_lookup=_lookup) == ()


def test_tenant_class_without_tenant_column_is_excluded() -> None:
    # The single-tenant shape: zero blast radius for apps with no tenant column.
    from varco_sa.rls_autogen import rls_candidate_tables

    assert rls_candidate_tables([NoColumn], table_lookup=_lookup) == ()


def test_unresolvable_class_is_skipped_without_raising() -> None:
    from varco_sa.rls_autogen import rls_candidate_tables

    assert rls_candidate_tables([Unknown, Tenanted], table_lookup=_lookup) == ("c_tenanted",)


def test_custom_tenant_column_is_honoured() -> None:
    from varco_sa.rls_autogen import rls_candidate_tables

    result = rls_candidate_tables(
        [Tenanted, OrgScoped], tenant_column="org_id", table_lookup=_lookup
    )

    assert result == ("c_org",)


def test_varco_tenants_is_excluded() -> None:
    from varco_sa.rls_autogen import rls_candidate_tables

    assert rls_candidate_tables([TenantsCatalog], table_lookup=_lookup) == ()


def test_framework_tables_are_excluded() -> None:
    # A framework table carrying tenant_id must never be reported as an app candidate.
    from varco_sa.rls_autogen import rls_candidate_tables
    from varco_sa.rls_framework import framework_rls_tables

    names = framework_rls_tables()
    assert names, "precondition: at least one framework RLS table exists"
    classes = [_cls(f"F{i}") for i, _ in enumerate(names)]
    tables = {c: _table(n, "tenant_id") for c, n in zip(classes, names, strict=True)}

    assert rls_candidate_tables(classes, table_lookup=tables.__getitem__) == ()


def test_rls_candidate_tables_is_exported_from_varco_sa() -> None:
    import varco_sa

    assert "rls_candidate_tables" in varco_sa.__all__


@pytest.fixture
def _restore_registry() -> Iterator[None]:
    from varco_sa.factory import SAModelRegistry

    saved = dict(SAModelRegistry._registry)
    yield
    SAModelRegistry._registry.clear()
    SAModelRegistry._registry.update(saved)


@pytest.mark.usefixtures("_restore_registry")
def test_registered_returns_tuple_containing_registered_class() -> None:
    from varco_sa.factory import SAModelRegistry

    domain = _cls("RegA")
    SAModelRegistry._register(domain, object)

    snap = SAModelRegistry.registered()

    assert isinstance(snap, tuple)
    assert domain in snap


@pytest.mark.usefixtures("_restore_registry")
def test_registered_is_a_snapshot() -> None:
    # A later registration must not leak into an earlier snapshot.
    from varco_sa.factory import SAModelRegistry

    snap = SAModelRegistry.registered()
    late = _cls("RegLate")
    SAModelRegistry._register(late, object)

    assert late not in snap
    assert late in SAModelRegistry.registered()
