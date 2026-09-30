"""
Plan 051 (DI-5) / §D-tenant — RED tests: ``TenantSource`` becomes a
``@Multibound`` seam; ``build_tenant_source_chain(..., sources=...)`` gains
a pool argument; ``resolve_tenant_source_chain(container)`` reads settings
+ the DI pool; the closed ``_LEGAL_TENANT_SOURCES`` allowlist is replaced
by "unknown name -> ValueError naming built-ins and bound names".

Expected to fail until Step 7 lands — today ``build_tenant_source_chain``
accepts no ``sources=`` kwarg and an unknown name error only lists the four
built-ins (no bound-name half), and ``resolve_tenant_source_chain`` does
not exist.
"""

from __future__ import annotations

import pytest
from varco_core.tenancy.settings import TenantProvenanceSettings, build_tenant_source_chain
from varco_core.tenancy.source import TenantSource, TenantTrust


class _PartnerSource(TenantSource):
    name = "partner"
    trust = TenantTrust.MEDIUM

    def resolve(self, request):  # pragma: no cover - not exercised
        return None


def test_tenant_source_is_multibound_seam() -> None:
    assert getattr(TenantSource, "__di_multibound__", None) is not None


def test_build_tenant_source_chain_accepts_a_pool_source_by_name() -> None:
    settings = TenantProvenanceSettings(sources=("partner",))
    partner = _PartnerSource()

    chain = build_tenant_source_chain(settings, sources=[partner])

    assert chain is not None
    assert partner in chain.sources


def test_unknown_name_lists_built_ins_and_bound_names() -> None:
    settings = TenantProvenanceSettings(sources=("nope",))

    with pytest.raises(ValueError, match="jwt"):
        build_tenant_source_chain(settings, sources=[_PartnerSource()])


def test_pool_source_colliding_with_a_builtin_name_is_refused() -> None:
    class _FakeJwt(TenantSource):
        name = "jwt"
        trust = TenantTrust.HIGH

        def resolve(self, request):  # pragma: no cover
            return None

    settings = TenantProvenanceSettings(sources=("jwt",))

    with pytest.raises(ValueError):
        build_tenant_source_chain(settings, sources=[_FakeJwt()])


def test_duplicate_pool_source_names_are_refused() -> None:
    settings = TenantProvenanceSettings(sources=("partner",))

    with pytest.raises(ValueError):
        build_tenant_source_chain(settings, sources=[_PartnerSource(), _PartnerSource()])


def test_pool_source_not_named_in_settings_is_inert() -> None:
    settings = TenantProvenanceSettings(sources=())

    chain = build_tenant_source_chain(settings, sources=[_PartnerSource()])

    assert chain is None


async def test_resolve_tenant_source_chain_reads_settings_and_di_pool() -> None:
    from providify import DIContainer
    from varco_core.di.collect import contribute
    from varco_core.tenancy.di import resolve_tenant_source_chain

    container = DIContainer()
    from varco_core.di import bind_instance

    bind_instance(
        container, TenantProvenanceSettings, TenantProvenanceSettings(sources=("partner",))
    )
    contribute(container, TenantSource, _PartnerSource())

    chain = resolve_tenant_source_chain(container)

    assert chain is not None
    assert any(s.name == "partner" for s in chain.sources)
