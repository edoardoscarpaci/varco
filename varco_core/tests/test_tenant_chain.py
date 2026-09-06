"""
Failing-first tests for TenantSourceChain.resolve() (Plan 033, Phase 0,
Step 3) — §D-S6-chain / §D-S6-oq2.

All sources here are tiny stubs returning a fixed claim (or raising), so the
chain's pure decision logic is exercised without any real source.
"""

from __future__ import annotations

import pytest


def _mod():
    from varco_core.tenancy import source

    return source


def _stub_source(mod, name, trust, tenant_id=None, *, raises=False):
    # `resolve` is assigned INSIDE the class body (not attached to the class
    # object afterwards) so `TenantSource` can stay a plain
    # `@abstractmethod`-based ABC — the standard house idiom throughout
    # varco_core. Attaching a concrete `resolve` after class creation would
    # leave a stock `ABCMeta` class's `__abstractmethods__` frozenset
    # unchanged (computed once, at class-creation time), making the class
    # permanently unconstructable regardless of the later assignment.
    if raises:

        def _resolve(self, request):
            raise RuntimeError("boom")

    elif tenant_id is None:

        def _resolve(self, request):
            return None

    else:

        def _resolve(self, request):
            return mod.TenantClaim(tenant_id=tenant_id, source=name, trust=trust)

    _Stub = type(
        "_Stub",
        (mod.TenantSource,),
        {"name": name, "trust": trust, "resolve": _resolve},
    )
    return _Stub()


@pytest.fixture
def mod():
    return _mod()


@pytest.fixture
def req(mod):
    return mod.TenantRequest(headers={})


class TestZeroClaims:
    def test_zero_claims_passes_through_in_lenient(self, mod, req) -> None:
        chain = mod.TenantSourceChain(sources=(), mode=mod.CrossCheckMode.LENIENT)
        prov = chain.resolve(req)

        assert prov.tenant_id is None
        assert prov.winner is None
        assert prov.rejected is False

    def test_zero_claims_passes_through_in_strict(self, mod, req) -> None:
        chain = mod.TenantSourceChain(sources=(), mode=mod.CrossCheckMode.STRICT)
        prov = chain.resolve(req)

        assert prov.tenant_id is None
        assert prov.winner is None
        assert prov.rejected is False


class TestOneClaim:
    def test_one_claim_wins_in_lenient(self, mod, req) -> None:
        s = _stub_source(mod, "s1", mod.TenantTrust.HIGH, "acme")
        chain = mod.TenantSourceChain(sources=(s,), mode=mod.CrossCheckMode.LENIENT)
        prov = chain.resolve(req)

        assert prov.tenant_id == "acme"
        assert prov.rejected is False

    def test_one_claim_rejects_in_strict(self, mod, req) -> None:
        # STRICT: any source spoke and fewer than two agreed -> reject.
        s = _stub_source(mod, "s1", mod.TenantTrust.HIGH, "acme")
        chain = mod.TenantSourceChain(sources=(s,), mode=mod.CrossCheckMode.STRICT)
        prov = chain.resolve(req)

        assert prov.rejected is True
        assert prov.rejection_reason == "insufficient_sources"


class TestTwoAgreeing:
    def test_two_agreeing_no_rejection_either_mode(self, mod, req) -> None:
        s1 = _stub_source(mod, "s1", mod.TenantTrust.HIGH, "acme")
        s2 = _stub_source(mod, "s2", mod.TenantTrust.HIGHEST, "acme")
        for mode in (mod.CrossCheckMode.LENIENT, mod.CrossCheckMode.STRICT):
            chain = mod.TenantSourceChain(sources=(s1, s2), mode=mode)
            prov = chain.resolve(req)
            assert prov.rejected is False
            assert prov.tenant_id == "acme"

    def test_winner_is_higher_trust_claim(self, mod, req) -> None:
        s1 = _stub_source(mod, "s1", mod.TenantTrust.HIGH, "acme")
        s2 = _stub_source(mod, "s2", mod.TenantTrust.HIGHEST, "acme")
        chain = mod.TenantSourceChain(sources=(s1, s2))
        prov = chain.resolve(req)

        assert prov.winner is not None
        assert prov.winner.trust is mod.TenantTrust.HIGHEST


class TestTwoDisagreeing:
    def test_disagreement_rejects_in_both_modes(self, mod, req) -> None:
        s1 = _stub_source(mod, "s1", mod.TenantTrust.HIGH, "acme")
        s2 = _stub_source(mod, "s2", mod.TenantTrust.HIGHEST, "beta")
        for mode in (mod.CrossCheckMode.LENIENT, mod.CrossCheckMode.STRICT):
            chain = mod.TenantSourceChain(sources=(s1, s2), mode=mode)
            prov = chain.resolve(req)
            assert prov.rejected is True
            assert prov.rejection_reason == "conflict"

    def test_conflict_names_both_claims(self, mod, req) -> None:
        s1 = _stub_source(mod, "s1", mod.TenantTrust.HIGH, "acme")
        s2 = _stub_source(mod, "s2", mod.TenantTrust.HIGHEST, "beta")
        chain = mod.TenantSourceChain(sources=(s1, s2))
        prov = chain.resolve(req)

        assert prov.conflict is not None
        ids = {c.tenant_id for c in prov.conflict}
        assert ids == {"acme", "beta"}


class TestThreeSourcesTwoAgreeOneDisagrees:
    def test_rejected(self, mod, req) -> None:
        s1 = _stub_source(mod, "s1", mod.TenantTrust.HIGH, "acme")
        s2 = _stub_source(mod, "s2", mod.TenantTrust.HIGH, "acme")
        s3 = _stub_source(mod, "s3", mod.TenantTrust.HIGHEST, "beta")
        chain = mod.TenantSourceChain(sources=(s1, s2, s3))
        prov = chain.resolve(req)

        assert prov.rejected is True
        assert prov.rejection_reason == "conflict"


class TestEqualTrustTie:
    def test_tie_broken_by_chain_order(self, mod, req) -> None:
        s1 = _stub_source(mod, "s1", mod.TenantTrust.HIGH, "acme")
        s2 = _stub_source(mod, "s2", mod.TenantTrust.HIGH, "acme")
        chain = mod.TenantSourceChain(sources=(s1, s2))
        prov = chain.resolve(req)

        assert prov.winner is not None
        assert prov.winner.source == "s1"


class TestMinTrustFilter:
    def test_below_floor_claim_is_dropped_not_a_conflict(self, mod, req) -> None:
        low = _stub_source(mod, "low", mod.TenantTrust.LOW, "acme")
        high = _stub_source(mod, "high", mod.TenantTrust.HIGH, "beta")
        chain = mod.TenantSourceChain(sources=(low, high), min_trust=mod.TenantTrust.MEDIUM)
        prov = chain.resolve(req)

        # Only "beta" ever spoke above the floor -> no conflict, plain winner.
        assert prov.rejected is False
        assert prov.tenant_id == "beta"
        assert prov.conflict is None


class TestSourceRaisesInternally:
    def test_resolve_never_raises_even_if_a_source_does(self, mod, req) -> None:
        bad = _stub_source(mod, "bad", mod.TenantTrust.HIGH, raises=True)
        good = _stub_source(mod, "good", mod.TenantTrust.HIGH, "acme")
        chain = mod.TenantSourceChain(sources=(bad, good))

        prov = chain.resolve(req)  # must not raise

        assert prov.tenant_id == "acme"
        assert prov.rejected is False


class TestDeterminism:
    def test_two_calls_are_identical(self, mod, req) -> None:
        s1 = _stub_source(mod, "s1", mod.TenantTrust.HIGH, "acme")
        s2 = _stub_source(mod, "s2", mod.TenantTrust.HIGHEST, "beta")
        chain = mod.TenantSourceChain(sources=(s1, s2))

        prov1 = chain.resolve(req)
        prov2 = chain.resolve(req)

        assert prov1.tenant_id == prov2.tenant_id
        assert prov1.rejected == prov2.rejected
        assert prov1.rejection_reason == prov2.rejection_reason


class TestRejectionReasonTokens:
    def test_rejection_reason_is_a_stable_token_never_a_formatted_string(self, mod, req) -> None:
        s1 = _stub_source(mod, "s1", mod.TenantTrust.HIGH, "acme")
        s2 = _stub_source(mod, "s2", mod.TenantTrust.HIGHEST, "beta")
        chain = mod.TenantSourceChain(sources=(s1, s2))
        prov = chain.resolve(req)

        assert prov.rejection_reason in {
            "conflict",
            "insufficient_sources",
            "not_a_member",
            "delegation_denied",
        }
        # Never echoes a tenant id (opaque-safe requirement).
        assert "acme" not in prov.rejection_reason
        assert "beta" not in prov.rejection_reason
