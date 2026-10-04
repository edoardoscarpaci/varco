"""
Failing-first tests for varco_core.tenancy.membership (Plan 033, Phase 3,
Step 23) — §D-S5-claim.
"""

from __future__ import annotations

import logging

import pytest


def _mod():
    from varco_core.tenancy import membership

    return membership


def _auth_ctx(**metadata):
    from varco_core.auth.base import AuthContext

    return AuthContext(user_id="u1", metadata=metadata)


def _anonymous_ctx():
    from varco_core.auth.base import AuthContext

    return AuthContext()


class TestNullTenantMembership:
    async def test_always_allows_with_no_provider_reason(self) -> None:
        mod = _mod()
        provider = mod.NullTenantMembership()

        decision = await provider.check(_auth_ctx(tenant_id="acme"), "acme")

        assert decision.allowed is True
        assert decision.reason == "no_provider"


class TestClaimTenantMembershipInClaim:
    async def test_requested_tenant_in_claim_list_allowed(self) -> None:
        mod = _mod()
        provider = mod.ClaimTenantMembership()
        ctx = _auth_ctx(tenants=["a", "b"])

        decision = await provider.check(ctx, "b")

        assert decision.allowed is True

    async def test_requested_tenant_not_in_claim_list_denied(self) -> None:
        mod = _mod()
        provider = mod.ClaimTenantMembership()
        ctx = _auth_ctx(tenants=["a", "b"])

        decision = await provider.check(ctx, "c")

        assert decision.allowed is False
        assert decision.reason == "not_in_claim"


class TestClaimTenantMembershipSelfTenant:
    async def test_no_tenants_claim_but_matching_tenant_id_allowed(self) -> None:
        mod = _mod()
        provider = mod.ClaimTenantMembership()
        ctx = _auth_ctx(tenant_id="acme")

        decision = await provider.check(ctx, "acme")

        assert decision.allowed is True
        assert decision.reason == "self_tenant"


class TestClaimTenantMembershipMissingClaim:
    async def test_missing_claim_with_allow_policy_allows_and_warns_once(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        mod = _mod()
        provider = mod.ClaimTenantMembership(on_missing_claim=mod.MissingClaimPolicy.ALLOW)
        ctx = _auth_ctx()

        with caplog.at_level(logging.WARNING):
            d1 = await provider.check(ctx, "acme")
            d2 = await provider.check(ctx, "acme")
            d3 = await provider.check(ctx, "acme")

        for decision in (d1, d2, d3):
            assert decision.allowed is True
            assert decision.reason == "claim_absent"

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1  # exactly one WARNING per process

    async def test_missing_claim_with_deny_policy_denies(self) -> None:
        mod = _mod()
        provider = mod.ClaimTenantMembership(on_missing_claim=mod.MissingClaimPolicy.DENY)
        ctx = _auth_ctx()

        decision = await provider.check(ctx, "acme")

        assert decision.allowed is False


class TestAnonymousSubject:
    async def test_anonymous_denied_under_deny(self) -> None:
        mod = _mod()
        provider = mod.ClaimTenantMembership(on_missing_claim=mod.MissingClaimPolicy.DENY)

        decision = await provider.check(_anonymous_ctx(), "acme")

        assert decision.allowed is False

    async def test_anonymous_allowed_under_allow(self) -> None:
        mod = _mod()
        provider = mod.ClaimTenantMembership(on_missing_claim=mod.MissingClaimPolicy.ALLOW)

        decision = await provider.check(_anonymous_ctx(), "acme")

        assert decision.allowed is True


class TestMalformedClaimNeverRaises:
    async def test_non_list_tenants_value_denies_never_type_error(self) -> None:
        mod = _mod()
        provider = mod.ClaimTenantMembership()
        ctx = _auth_ctx(tenants="not-a-list")

        decision = await provider.check(ctx, "acme")  # must not raise TypeError

        assert decision.allowed is False

    async def test_check_never_raises_for_any_input(self) -> None:
        mod = _mod()
        provider = mod.ClaimTenantMembership()
        weird_ctxs = [
            _auth_ctx(tenants=None),
            _auth_ctx(tenants=123),
            _auth_ctx(tenants=[1, 2, 3]),
            _anonymous_ctx(),
        ]
        for ctx in weird_ctxs:
            await provider.check(ctx, "acme")  # must not raise


# ── Plan 055 / FLIP-7 — on_missing_claim defaults DENY (red until Step 3) ────


class TestFlip7DenyByDefault:
    async def test_missing_claim_denies_by_default(self) -> None:
        # The flip itself: no `tenants` claim + non-matching tenant_id must
        # fail closed with no explicit policy (brief 006 §2 rule 5).
        mod = _mod()
        provider = mod.ClaimTenantMembership()

        decision = await provider.check(_auth_ctx(tenant_id="other"), "acme")

        assert decision.allowed is False
        assert decision.reason == "claim_absent"

    def test_default_policy_is_deny_signature_guard(self) -> None:
        # api_surface --check cannot see class __init__ defaults; this is the guard.
        import inspect

        mod = _mod()
        sig = inspect.signature(mod.ClaimTenantMembership.__init__)
        assert sig.parameters["on_missing_claim"].default is mod.MissingClaimPolicy.DENY
        assert mod.TenantMembershipSettings().on_missing_claim is mod.MissingClaimPolicy.DENY

    async def test_self_tenant_still_allows_under_default(self) -> None:
        # The no-IdP-change path for single-tenant fleets must survive the flip.
        mod = _mod()
        provider = mod.ClaimTenantMembership()

        decision = await provider.check(_auth_ctx(tenant_id="acme"), "acme")

        assert decision.allowed is True
        assert decision.reason == "self_tenant"

    async def test_explicit_allow_still_allows_and_warns_once(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Control: the explicit escape hatch keeps 3.2 behaviour.
        mod = _mod()
        monkeypatch.setattr(mod, "_missing_claim_warned", False)
        provider = mod.ClaimTenantMembership(on_missing_claim=mod.MissingClaimPolicy.ALLOW)

        with caplog.at_level(logging.WARNING):
            d1 = await provider.check(_auth_ctx(), "acme")
            d2 = await provider.check(_auth_ctx(), "acme")

        assert d1.allowed is True and d2.allowed is True
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1

    async def test_deny_logs_once_per_process_with_deny_in_message(
        self, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # The one log line is how an operator finds out why a fleet 403s.
        mod = _mod()
        monkeypatch.setattr(mod, "_missing_claim_warned", False)
        provider = mod.ClaimTenantMembership()

        with caplog.at_level(logging.WARNING):
            await provider.check(_auth_ctx(), "acme")
            await provider.check(_auth_ctx(), "acme")

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "on_missing_claim=deny" in warnings[0].getMessage()
