"""
Failing-first end-to-end tests over the real, shipped TenantSource
implementations composed into a TenantSourceChain — no HTTP (Plan 033,
Phase 1, Step 10).

DoD item 2: this file's "header claims A with a token for B" case (see
TestHeaderVsTokenDisagreement) must assert a genuine behavioural red on
today's unmodified tree, not merely an ImportError.
"""

from __future__ import annotations


def _smod():
    from varco_core.tenancy import source

    return source


def _sources_mod():
    from varco_core.tenancy import sources

    return sources


def _auth_ctx(**metadata):
    from varco_core.auth.base import AuthContext

    return AuthContext(user_id="u1", metadata=metadata)


class TestClaimVsHostDisagreement:
    def test_claim_a_host_b_rejects_with_conflict_naming_both(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        chain = smod.TenantSourceChain(
            sources=(
                srcs.JwtClaimTenantSource(),
                srcs.SubdomainTenantSource(base_domains=("example.com",)),
            )
        )
        req = smod.TenantRequest(
            headers={},
            host="beta.example.com",
            auth=_auth_ctx(tenant_id="acme"),
        )

        prov = chain.resolve(req)

        assert prov.rejected is True
        assert prov.conflict is not None
        ids = {c.tenant_id for c in prov.conflict}
        assert ids == {"acme", "beta"}


class TestHeaderVsTokenDisagreement:
    def test_header_a_token_b_with_legacy_in_chain_rejects(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        chain = smod.TenantSourceChain(
            sources=(
                srcs.JwtClaimTenantSource(),
                srcs.LegacyTenantSource(),
            )
        )
        req = smod.TenantRequest(
            headers={"x-tenant-id": "acme"},
            auth=_auth_ctx(tenant_id="beta"),
        )

        prov = chain.resolve(req)

        assert prov.rejected is True
        assert prov.rejection_reason == "conflict"

    def test_header_ignored_when_legacy_not_in_chain(self) -> None:
        # The proof that the chain, not merely a cross-check, is what makes
        # this safe: with legacy excluded from the chain entirely, the
        # header is never consulted at all and the JWT claim alone wins.
        smod, srcs = _smod(), _sources_mod()
        chain = smod.TenantSourceChain(sources=(srcs.JwtClaimTenantSource(),))
        req = smod.TenantRequest(
            headers={"x-tenant-id": "acme"},
            auth=_auth_ctx(tenant_id="beta"),
        )

        prov = chain.resolve(req)

        assert prov.rejected is False
        assert prov.tenant_id == "beta"


class TestClaimAloneUnderBothModes:
    def test_claim_a_no_subdomain_lenient_resolves(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        chain = smod.TenantSourceChain(
            sources=(
                srcs.JwtClaimTenantSource(),
                srcs.SubdomainTenantSource(base_domains=("example.com",)),
            ),
            mode=smod.CrossCheckMode.LENIENT,
        )
        req = smod.TenantRequest(headers={}, host=None, auth=_auth_ctx(tenant_id="acme"))

        prov = chain.resolve(req)

        assert prov.rejected is False
        assert prov.tenant_id == "acme"

    def test_claim_a_no_subdomain_strict_rejects(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        chain = smod.TenantSourceChain(
            sources=(
                srcs.JwtClaimTenantSource(),
                srcs.SubdomainTenantSource(base_domains=("example.com",)),
            ),
            mode=smod.CrossCheckMode.STRICT,
        )
        req = smod.TenantRequest(headers={}, host=None, auth=_auth_ctx(tenant_id="acme"))

        prov = chain.resolve(req)

        assert prov.rejected is True


class TestNoSourcesAtAll:
    def test_no_sources_none_in_both_modes(self) -> None:
        smod = _smod()
        for mode in (smod.CrossCheckMode.LENIENT, smod.CrossCheckMode.STRICT):
            chain = smod.TenantSourceChain(sources=(), mode=mode)
            req = smod.TenantRequest(headers={})
            prov = chain.resolve(req)
            assert prov.tenant_id is None
            assert prov.rejected is False
