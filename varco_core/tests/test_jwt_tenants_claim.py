"""
Failing-first tests for CanonicalClaim.TENANTS + the "tenants" list claim
reaching AuthContext.metadata (Plan 033, Phase 3, Step 21) — §D-S5-claim.
"""

from __future__ import annotations

import jwt as _pyjwt
import pytest
from varco_core.jwt import JwtParser

_SECRET = "test-secret-do-not-use-in-production"


def _sign(**claims) -> str:
    return _pyjwt.encode({"sub": "usr_1", **claims}, _SECRET, algorithm="HS256")


class TestTenantsClaimReachesMetadata:
    def test_list_claim_lands_in_metadata(self) -> None:
        token = _sign(tenants=["a", "b"])
        parsed = JwtParser.parse(token, _SECRET, algorithms=["HS256"])

        assert parsed.auth_ctx is not None
        assert parsed.auth_ctx.metadata["tenants"] == ["a", "b"]

    def test_foreign_name_via_flat_env_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_TENANTS_FIELD", "organizations")

        token = _sign(organizations=["a", "b"])
        parsed = JwtParser.parse(token, _SECRET, algorithms=["HS256"])

        assert parsed.auth_ctx is not None
        assert parsed.auth_ctx.metadata["tenants"] == ["a", "b"]

    def test_per_issuer_labelled_form(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__ACME__ISS", "acme-issuer")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__ACME__TENANTS_FIELD", "orgs")

        token = _sign(iss="acme-issuer", orgs=["a", "b"])
        parsed = JwtParser.parse(token, _SECRET, algorithms=["HS256"])

        assert parsed.auth_ctx is not None
        assert parsed.auth_ctx.metadata["tenants"] == ["a", "b"]

    def test_scalar_value_normalises_to_a_list(self) -> None:
        token = _sign(tenants="a")
        parsed = JwtParser.parse(token, _SECRET, algorithms=["HS256"])

        assert parsed.auth_ctx is not None
        assert parsed.auth_ctx.metadata["tenants"] == ["a"]

    def test_tenants_only_token_now_materialises_auth_ctx(self) -> None:
        # ⚠️ The widened trigger (§D-S5-claim ❌): a token with ONLY a
        # tenants claim used to produce auth_ctx=None; it must not anymore.
        token = _sign(tenants=["a"])
        parsed = JwtParser.parse(token, _SECRET, algorithms=["HS256"])

        assert parsed.auth_ctx is not None

    def test_no_tenants_claim_is_byte_identical(self) -> None:
        token = _sign(roles=["admin"])
        parsed = JwtParser.parse(token, _SECRET, algorithms=["HS256"])

        assert parsed.auth_ctx is not None
        assert set(parsed.auth_ctx.metadata.keys()) == set()


class TestCanonicalClaimMemberSetPinned:
    def test_canonical_claim_members_are_pinned(self) -> None:
        # §D-030's "not a signature change, invisible to api_surface --check"
        # concern — this test is the guard the plan requires (Step 30).
        from varco_core.jwt.transform.mapping import CanonicalClaim

        assert {m.value for m in CanonicalClaim} == {
            "user_id",
            "roles",
            "scopes",
            "grants",
            "tenant_id",
            "actor",
            "token_type",
            "tenants",
        }
