"""
Failing-first tests for the three shipped TenantSource implementations
(Plan 033, Phase 1, Steps 7 + 9) — §D-S6-oq3, §D-S6-abc, §D-S6-conformance.
"""

from __future__ import annotations

import copy

import pytest


def _smod():
    from varco_core.tenancy import source

    return source


def _sources_mod():
    from varco_core.tenancy import sources

    return sources


def _auth_ctx(**metadata):
    from varco_core.auth.base import AuthContext

    return AuthContext(user_id="u1", metadata=metadata)


# ── JwtClaimTenantSource ─────────────────────────────────────────────────────


class TestJwtClaimTenantSource:
    def test_claim_present_resolves_at_highest_trust(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        source_obj = srcs.JwtClaimTenantSource()
        req = smod.TenantRequest(headers={}, auth=_auth_ctx(tenant_id="acme"))

        claim = source_obj.resolve(req)

        assert claim is not None
        assert claim.tenant_id == "acme"
        assert claim.trust is smod.TenantTrust.HIGHEST

    def test_no_auth_returns_none(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        source_obj = srcs.JwtClaimTenantSource()
        req = smod.TenantRequest(headers={}, auth=None)

        assert source_obj.resolve(req) is None

    def test_empty_metadata_returns_none(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        source_obj = srcs.JwtClaimTenantSource()
        req = smod.TenantRequest(headers={}, auth=_auth_ctx())

        assert source_obj.resolve(req) is None

    def test_non_str_claim_value_returns_none_never_coerced(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        source_obj = srcs.JwtClaimTenantSource()
        req = smod.TenantRequest(headers={}, auth=_auth_ctx(tenant_id=["acme"]))

        assert source_obj.resolve(req) is None

    def test_custom_metadata_key(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        source_obj = srcs.JwtClaimTenantSource(metadata_key="org_id")
        req = smod.TenantRequest(headers={}, auth=_auth_ctx(org_id="acme"))

        claim = source_obj.resolve(req)
        assert claim is not None
        assert claim.tenant_id == "acme"


# ── SubdomainTenantSource ────────────────────────────────────────────────────


class TestSubdomainTenantSource:
    def test_matching_subdomain_resolves(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com",))
        req = smod.TenantRequest(headers={}, host="acme.example.com")

        claim = s.resolve(req)
        assert claim is not None
        assert claim.tenant_id == "acme"
        assert claim.trust is smod.TenantTrust.HIGH

    def test_bare_base_domain_yields_none(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com",))
        req = smod.TenantRequest(headers={}, host="example.com")

        assert s.resolve(req) is None

    def test_reserved_label_yields_none(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com",))
        req = smod.TenantRequest(headers={}, host="www.example.com")

        assert s.resolve(req) is None

    def test_multi_label_prefix_yields_none_never_leftmost(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com",))
        req = smod.TenantRequest(headers={}, host="a.b.example.com")

        assert s.resolve(req) is None

    def test_case_port_and_normalisation(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com",))
        req = smod.TenantRequest(headers={}, host="ACME.Example.COM:8443")

        claim = s.resolve(req)
        assert claim is not None
        assert claim.tenant_id == "acme"

    def test_trailing_dot_normalises(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com",))
        req = smod.TenantRequest(headers={}, host="acme.example.com.")

        claim = s.resolve(req)
        assert claim is not None
        assert claim.tenant_id == "acme"

    def test_multi_level_tld_base_domain(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.co.uk",))
        req = smod.TenantRequest(headers={}, host="acme.example.co.uk")

        claim = s.resolve(req)
        assert claim is not None
        assert claim.tenant_id == "acme"

    def test_operator_error_base_domain_is_pinned_not_detected(self) -> None:
        # varco cannot detect that "co.uk" is a public suffix; it trusts
        # the operator's config exactly as documented — pinned behaviour.
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("co.uk",))
        req = smod.TenantRequest(headers={}, host="example.co.uk")

        claim = s.resolve(req)
        assert claim is not None
        assert claim.tenant_id == "example"

    def test_longest_base_domain_wins(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com", "eu.example.com"))
        req = smod.TenantRequest(headers={}, host="acme.eu.example.com")

        claim = s.resolve(req)
        assert claim is not None
        assert claim.tenant_id == "acme"

    def test_idna_host_resolves(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com",))
        # "acme" is plain ASCII but exercise the idna codec path with an
        # accented label that punycode-encodes cleanly.
        req = smod.TenantRequest(headers={}, host="xn--wgv71a.example.com")

        claim = s.resolve(req)
        assert claim is not None

    def test_empty_label_returns_none_no_exception(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com",))
        req = smod.TenantRequest(headers={}, host="..example.com")

        assert s.resolve(req) is None

    def test_overlong_label_returns_none_no_exception(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com",))
        req = smod.TenantRequest(headers={}, host=("a" * 64) + ".example.com")

        assert s.resolve(req) is None

    def test_empty_base_domains_raises_value_error_at_construction(self) -> None:
        srcs = _sources_mod()
        with pytest.raises(ValueError):
            srcs.SubdomainTenantSource(base_domains=())

    def test_forwarded_host_ignored_by_default(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com",))
        req = smod.TenantRequest(
            headers={"x-forwarded-host": "victim.example.com"},
            host="api.example.com",
        )

        assert s.resolve(req) is None

    def test_forwarded_host_trusted_resolves_at_medium(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.SubdomainTenantSource(base_domains=("example.com",), trust_forwarded_host=True)
        req = smod.TenantRequest(
            headers={"x-forwarded-host": "victim.example.com"},
            host="api.example.com",
        )

        claim = s.resolve(req)
        assert claim is not None
        assert claim.tenant_id == "victim"
        assert claim.trust is smod.TenantTrust.MEDIUM


# ── LegacyTenantSource ───────────────────────────────────────────────────────


class TestLegacyTenantSource:
    def test_header_present_resolves_at_low_trust(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.LegacyTenantSource()
        req = smod.TenantRequest(headers={"x-tenant-id": "acme"})

        claim = s.resolve(req)
        assert claim is not None
        assert claim.tenant_id == "acme"
        assert claim.trust is smod.TenantTrust.LOW

    def test_header_absent_returns_none(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.LegacyTenantSource()
        req = smod.TenantRequest(headers={})

        assert s.resolve(req) is None

    def test_custom_header_name(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.LegacyTenantSource(header="x-org-id")
        req = smod.TenantRequest(headers={"x-org-id": "acme"})

        claim = s.resolve(req)
        assert claim is not None
        assert claim.tenant_id == "acme"

    def test_empty_string_header_value_returns_none(self) -> None:
        smod, srcs = _smod(), _sources_mod()
        s = srcs.LegacyTenantSource()
        req = smod.TenantRequest(headers={"x-tenant-id": ""})

        assert s.resolve(req) is None


# ── Cross-source invariants (§D-S6-conformance) ─────────────────────────────


def _all_sources():
    srcs = _sources_mod()
    return [
        srcs.JwtClaimTenantSource(),
        srcs.SubdomainTenantSource(base_domains=("example.com",)),
        srcs.LegacyTenantSource(),
    ]


class TestSourceInvariants:
    def test_resolve_does_not_mutate_the_request(self) -> None:
        smod = _smod()
        req = smod.TenantRequest(
            headers={"x-tenant-id": "acme"},
            host="acme.example.com",
            auth=_auth_ctx(tenant_id="acme"),
        )
        before = copy.deepcopy(req)

        for src in _all_sources():
            src.resolve(req)

        assert req == before

    def test_declared_trust_and_unique_name_for_every_source(self) -> None:
        for src in _all_sources():
            assert isinstance(src.name, str) and src.name
            assert src.trust is not None

    def test_names_are_unique_across_shipped_sources(self) -> None:
        names = [s.name for s in _all_sources()]
        assert len(names) == len(set(names))

    def test_never_raises_on_empty_request_for_every_source(self) -> None:
        smod = _smod()
        req = smod.TenantRequest(headers={})
        for src in _all_sources():
            src.resolve(req)  # must not raise

    def test_never_returns_empty_string_tenant_for_every_source(self) -> None:
        smod = _smod()
        req = smod.TenantRequest(
            headers={"x-tenant-id": ""},
            host="www.example.com",
            auth=_auth_ctx(),
        )
        for src in _all_sources():
            claim = src.resolve(req)
            if claim is not None:
                assert claim.tenant_id != ""


class TestMandatoryDocstringSentences:
    def test_subdomain_source_docstring_states_not_a_security_control(self) -> None:
        srcs = _sources_mod()
        doc = srcs.SubdomainTenantSource.__doc__ or ""
        assert "not by itself a security control" in doc
        assert "TrustedHostMiddleware" in doc

    def test_tenant_source_abc_edge_cases_state_the_four_invariants(self) -> None:
        smod = _smod()
        doc = smod.TenantSource.__doc__ or ""
        assert "never" in doc.lower() and "raise" in doc.lower()
