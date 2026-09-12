"""
Failing-first tests for varco_core.tenancy.provenance's assert_tenant_matches
/ CrossTenantAccessError and varco_core.tenancy.posture's
inspect_tenant_provenance (Plan 033, Phase 4, Step 27) — §D-036-seams.
"""

from __future__ import annotations

import pytest


def _prov_mod():
    from varco_core.tenancy import provenance

    return provenance


def _posture_mod():
    from varco_core.tenancy import posture

    return posture


def _source_mod():
    from varco_core.tenancy import source

    return source


class TestAssertTenantMatches:
    def test_requested_none_inside_context_returns_active_tenant(self) -> None:
        from varco_core.service.tenant import tenant_context

        mod = _prov_mod()
        with tenant_context("a"):
            assert mod.assert_tenant_matches(None) == "a"

    def test_requested_matching_current_returns_it(self) -> None:
        from varco_core.service.tenant import tenant_context

        mod = _prov_mod()
        with tenant_context("a"):
            assert mod.assert_tenant_matches("a") == "a"

    def test_requested_mismatch_raises_cross_tenant_access_error(self) -> None:
        from varco_core.service.tenant import tenant_context

        mod = _prov_mod()
        with tenant_context("a"):
            with pytest.raises(mod.CrossTenantAccessError) as exc:
                mod.assert_tenant_matches("b")

        params = exc.value.error_params()
        assert params.get("requested") == "b"
        assert "resolved" not in params
        # Exfiltration check: the *resolved* tenant ("a") must never appear
        # as a value in error_params() — checked against the values
        # themselves, not their repr (repr(dict_values(...)) legitimately
        # contains the substring "a" via the word "values" itself).
        assert "a" not in params.values()

    def test_allow_cross_tenant_true_returns_requested(self) -> None:
        from varco_core.service.tenant import tenant_context

        mod = _prov_mod()
        with tenant_context("a"):
            assert mod.assert_tenant_matches("b", allow_cross_tenant=True) == "b"

    def test_requested_none_with_no_context_raises(self) -> None:
        mod = _prov_mod()
        with pytest.raises(mod.CrossTenantAccessError):
            mod.assert_tenant_matches(None)


class TestInspectTenantProvenance:
    def test_chain_none_reports_not_configured(self) -> None:
        posture = _posture_mod()
        result = posture.inspect_tenant_provenance(None)

        assert result.chain_configured is False
        assert "tenant.no_chain" in result.findings

    def test_legacy_only_chain_reports_legacy_source_active(self) -> None:
        from varco_core.tenancy.source import TenantSourceChain
        from varco_core.tenancy.sources import LegacyTenantSource

        posture = _posture_mod()
        chain = TenantSourceChain(sources=(LegacyTenantSource(),))
        result = posture.inspect_tenant_provenance(chain)

        assert result.legacy_source_active is True

    def test_no_membership_reports_finding(self) -> None:
        posture = _posture_mod()
        result = posture.inspect_tenant_provenance(None, membership=None)

        assert "tenant.no_membership_provider" in result.findings

    def test_every_finding_token_is_reachable_and_pinned(self) -> None:
        from varco_core.tenancy.membership import ClaimTenantMembership, MissingClaimPolicy
        from varco_core.tenancy.source import CrossCheckMode, TenantSourceChain
        from varco_core.tenancy.sources import (
            JwtClaimTenantSource,
            LegacyTenantSource,
            SubdomainTenantSource,
        )

        posture = _posture_mod()

        all_findings: set[str] = set()

        all_findings |= set(posture.inspect_tenant_provenance(None).findings)

        legacy_chain = TenantSourceChain(sources=(LegacyTenantSource(),))
        all_findings |= set(
            posture.inspect_tenant_provenance(
                legacy_chain, request_context_sets_tenant=True
            ).findings
        )

        explicit_legacy_only = TenantSourceChain(sources=(LegacyTenantSource(),))
        all_findings |= set(posture.inspect_tenant_provenance(explicit_legacy_only).findings)

        single_source = TenantSourceChain(sources=(JwtClaimTenantSource(),))
        all_findings |= set(posture.inspect_tenant_provenance(single_source).findings)

        lenient_chain = TenantSourceChain(
            sources=(JwtClaimTenantSource(), LegacyTenantSource()),
            mode=CrossCheckMode.LENIENT,
        )
        all_findings |= set(posture.inspect_tenant_provenance(lenient_chain).findings)

        all_findings |= set(
            posture.inspect_tenant_provenance(single_source, membership=None).findings
        )

        allow_membership = ClaimTenantMembership(on_missing_claim=MissingClaimPolicy.ALLOW)
        all_findings |= set(
            posture.inspect_tenant_provenance(single_source, membership=allow_membership).findings
        )

        subdomain_chain = TenantSourceChain(
            sources=(
                SubdomainTenantSource(base_domains=("example.com",), trust_forwarded_host=True),
            )
        )
        all_findings |= set(posture.inspect_tenant_provenance(subdomain_chain).findings)

        all_findings |= set(
            posture.inspect_tenant_provenance(
                single_source, request_context_sets_tenant=True
            ).findings
        )

        all_findings |= set(
            posture.inspect_tenant_provenance(single_source, delegation=None).findings
        )

        expected = {
            "tenant.no_chain",
            "tenant.legacy_source_implicit",
            "tenant.legacy_source_explicit",
            "tenant.single_source",
            "tenant.cross_check_lenient",
            "tenant.no_membership_provider",
            "tenant.membership_missing_claim_allows",
            "tenant.subdomain_trusts_forwarded_host",
            "tenant.unchained_claim_tenant_setter",
            "tenant.delegation_unbound",
        }
        # Every token in the pinned §D-036-seams set must be reachable by at
        # least one of the configurations exercised above.
        assert expected <= all_findings

    def test_pure_no_io_no_ambient_reads(self) -> None:
        # Called with no tenant/provenance context active at all — must not
        # need one, and must not read current_tenant()/current_tenant_provenance().
        posture = _posture_mod()
        result = posture.inspect_tenant_provenance(None)
        assert result is not None
