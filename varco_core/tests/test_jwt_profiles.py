"""
Unit tests for varco_core.jwt.profile — TokenProfile + TokenProfileRegistry.

Covers (Plan 002, Phase 3, step 24):
    - TokenProfile.matches(): issuer-only, issuer+token_type, aud any-of (str
      and frozenset forms), required_claims present/absent, empty issuers = any.
    - TokenProfile.explain() naming the first failing condition.
    - TokenProfileRegistry.resolve() first-match-in-registration-order.
    - TokenProfileRegistry.get() raises TokenProfileError listing known names.
    - TokenProfileRegistry.from_env() with VARCO_JWT_PROFILE__INTERNAL__*.
    - A condition-less profile label raises TokenProfileError at load.
    - Parser integration: profile metadata + implied_roles merge + materialisation.
    - JwtUtil.matches_profile / profile_name / assert_profile.
    - Back-compat: existing is_system* behaviour unaffected without a "system"
      profile; VARCO_JWT_PROFILE__SYSTEM__ISS makes is_system() issuer-aware.
    - JwtBuilder.as_profile().

These tests are written RED — ``varco_core.jwt.profile`` does not exist yet
(nor do the profile-aware JwtUtil/JwtBuilder/JwtParser additions). They must
fail with ImportError / AttributeError until Phase 3 lands.
"""

from __future__ import annotations

import pytest
from varco_core.jwt import SYSTEM_ISSUER, JwtBuilder, JwtParser, JwtUtil

_SECRET = "test-secret-do-not-use-in-production"


# ── TokenProfile.matches ────────────────────────────────────────────────────────


class TestTokenProfileMatches:
    def test_issuer_only_match(self):
        from varco_core.jwt.profile import TokenProfile

        profile = TokenProfile(name="internal", issuers=frozenset({"mesh-signer"}))
        token = JwtBuilder().issuer("mesh-signer").build()
        assert profile.matches(token) is True

    def test_issuer_mismatch(self):
        from varco_core.jwt.profile import TokenProfile

        profile = TokenProfile(name="internal", issuers=frozenset({"mesh-signer"}))
        token = JwtBuilder().issuer("other").build()
        assert profile.matches(token) is False

    def test_issuer_and_token_type_both_required(self):
        from varco_core.jwt.profile import TokenProfile

        profile = TokenProfile(
            name="internal",
            issuers=frozenset({"mesh-signer"}),
            token_type="system",
        )
        matching = JwtBuilder().issuer("mesh-signer").type("system").build()
        wrong_type = JwtBuilder().issuer("mesh-signer").type("access").build()
        assert profile.matches(matching) is True
        assert profile.matches(wrong_type) is False

    def test_aud_any_of_matches_string_form(self):
        from varco_core.jwt.profile import TokenProfile

        profile = TokenProfile(name="p", audiences=frozenset({"orders", "billing"}))
        token = JwtBuilder().issuer("x").audience("orders").build()
        assert profile.matches(token) is True

    def test_aud_any_of_matches_frozenset_form(self):
        from varco_core.jwt.profile import TokenProfile

        profile = TokenProfile(name="p", audiences=frozenset({"orders"}))
        token = JwtBuilder().issuer("x").audience(frozenset({"orders", "misc"})).build()
        assert profile.matches(token) is True

    def test_required_claims_present(self):
        from varco_core.jwt.profile import TokenProfile

        profile = TokenProfile(
            name="p", issuers=frozenset({"x"}), required_claims=frozenset({"tenant_id"})
        )
        token = JwtBuilder().issuer("x").claim("tenant_id", "t1").build()
        assert profile.matches(token) is True

    def test_required_claims_missing_returns_false(self):
        from varco_core.jwt.profile import TokenProfile

        profile = TokenProfile(
            name="p", issuers=frozenset({"x"}), required_claims=frozenset({"tenant_id"})
        )
        token = JwtBuilder().issuer("x").build()
        assert profile.matches(token) is False

    def test_empty_issuers_matches_any_issuer(self):
        from varco_core.jwt.profile import TokenProfile

        profile = TokenProfile(name="p", token_type="system")
        token = JwtBuilder().issuer("literally-anything").type("system").build()
        assert profile.matches(token) is True

    def test_explain_names_first_failing_condition(self):
        from varco_core.jwt.profile import TokenProfile

        profile = TokenProfile(
            name="internal",
            issuers=frozenset({"mesh-signer"}),
            required_claims=frozenset({"tenant_id"}),
        )
        token = JwtBuilder().issuer("other").build()
        explanation = profile.explain(token)
        assert explanation is not None
        assert "issuer" in explanation.lower() or "iss" in explanation.lower()


# ── TokenProfileRegistry ─────────────────────────────────────────────────────────


class TestTokenProfileRegistry:
    def test_resolve_returns_first_match_in_registration_order(self):
        from varco_core.jwt.profile import TokenProfile, TokenProfileRegistry

        registry = TokenProfileRegistry()
        registry.register(TokenProfile(name="first", issuers=frozenset({"x"})))
        registry.register(TokenProfile(name="second", issuers=frozenset({"x"})))

        token = JwtBuilder().issuer("x").build()
        resolved = registry.resolve(token)
        assert resolved is not None
        assert resolved.name == "first"

    def test_get_unknown_name_raises_naming_known_names(self):
        from varco_core.jwt.profile import (
            TokenProfile,
            TokenProfileError,
            TokenProfileRegistry,
        )

        registry = TokenProfileRegistry()
        registry.register(TokenProfile(name="internal", issuers=frozenset({"x"})))
        with pytest.raises(TokenProfileError, match="internal"):
            registry.get("nope")

    def test_names_lists_registered_profiles(self):
        from varco_core.jwt.profile import TokenProfile, TokenProfileRegistry

        registry = TokenProfileRegistry()
        registry.register(TokenProfile(name="internal", issuers=frozenset({"x"})))
        assert "internal" in registry.names()

    def test_matches_delegates_to_named_profile(self):
        from varco_core.jwt.profile import TokenProfile, TokenProfileRegistry

        registry = TokenProfileRegistry()
        registry.register(TokenProfile(name="internal", issuers=frozenset({"x"})))
        token = JwtBuilder().issuer("x").build()
        assert registry.matches("internal", token) is True

    def test_from_env_registers_profile_from_labelled_vars(self, monkeypatch):
        from varco_core.jwt.profile import TokenProfileRegistry

        monkeypatch.setenv("VARCO_JWT_PROFILE__INTERNAL__ISS", "mesh-signer")
        monkeypatch.setenv("VARCO_JWT_PROFILE__INTERNAL__TOKEN_TYPE", "system")
        monkeypatch.setenv("VARCO_JWT_PROFILE__INTERNAL__ROLES", "internal")

        registry = TokenProfileRegistry.from_env()
        assert "internal" in registry.names()
        token = JwtBuilder().issuer("mesh-signer").type("system").build()
        assert registry.matches("internal", token) is True

    def test_condition_less_profile_label_raises_at_load(self, monkeypatch):
        from varco_core.jwt.profile import TokenProfileError, TokenProfileRegistry

        # No __ISS / __TOKEN_TYPE / __AUD / __REQUIRED_CLAIMS at all — a
        # match-everything profile is rejected as a footgun.
        monkeypatch.setenv("VARCO_JWT_PROFILE__OPEN__ROLES", "internal")
        with pytest.raises(TokenProfileError):
            TokenProfileRegistry.from_env()


# ── Parser integration ────────────────────────────────────────────────────────


class TestParserProfileIntegration:
    def test_matched_profile_sets_token_profile_metadata_and_implied_roles(self):
        from varco_core.jwt.profile import (
            PROFILE_METADATA_KEY,
            TokenProfile,
            TokenProfileRegistry,
        )

        profiles = TokenProfileRegistry()
        profiles.register(
            TokenProfile(
                name="internal",
                issuers=frozenset({"mesh-signer"}),
                implied_roles=frozenset({"internal"}),
            )
        )
        signed = JwtBuilder().subject("svc_1").issuer("mesh-signer").encode(_SECRET)
        token = JwtParser.parse(signed, _SECRET, profiles=profiles, algorithms=["HS256"])

        assert token.auth_ctx is not None
        assert token.auth_ctx.metadata[PROFILE_METADATA_KEY] == "internal"
        assert "internal" in token.auth_ctx.roles

    def test_matching_profile_with_implied_roles_materialises_auth_ctx(self):
        # A token with only sub+iss (no roles/scopes/grants) matching a
        # profile that declares implied_roles must get a non-None auth_ctx.
        from varco_core.jwt.profile import TokenProfile, TokenProfileRegistry

        profiles = TokenProfileRegistry()
        profiles.register(
            TokenProfile(
                name="internal",
                issuers=frozenset({"mesh-signer"}),
                implied_roles=frozenset({"internal"}),
            )
        )
        signed = JwtBuilder().subject("svc_1").issuer("mesh-signer").encode(_SECRET)
        token = JwtParser.parse(signed, _SECRET, profiles=profiles, algorithms=["HS256"])
        assert token.auth_ctx is not None

    def test_matching_profile_without_implied_anything_keeps_auth_ctx_none(self):
        from varco_core.jwt.profile import TokenProfile, TokenProfileRegistry

        profiles = TokenProfileRegistry()
        profiles.register(TokenProfile(name="internal", issuers=frozenset({"mesh-signer"})))
        signed = JwtBuilder().subject("svc_1").issuer("mesh-signer").encode(_SECRET)
        token = JwtParser.parse(signed, _SECRET, profiles=profiles, algorithms=["HS256"])
        assert token.auth_ctx is None


# ── JwtUtil profile helpers ───────────────────────────────────────────────────


class TestJwtUtilProfileHelpers:
    def test_matches_profile_delegates_to_registry(self):
        from varco_core.jwt.profile import TokenProfile, TokenProfileRegistry

        profiles = TokenProfileRegistry()
        profiles.register(TokenProfile(name="internal", issuers=frozenset({"x"})))
        token = JwtBuilder().issuer("x").build()
        util = JwtUtil(token)
        assert util.matches_profile("internal", registry=profiles) is True

    def test_profile_name_returns_resolved_name_or_none(self):
        from varco_core.jwt.profile import TokenProfile, TokenProfileRegistry

        profiles = TokenProfileRegistry()
        profiles.register(TokenProfile(name="internal", issuers=frozenset({"x"})))
        matching = JwtUtil(JwtBuilder().issuer("x").build())
        non_matching = JwtUtil(JwtBuilder().issuer("y").build())
        assert matching.profile_name(registry=profiles) == "internal"
        assert non_matching.profile_name(registry=profiles) is None

    def test_assert_profile_raises_when_no_match(self):
        from varco_core.jwt.exceptions import TokenProfileError
        from varco_core.jwt.profile import TokenProfile, TokenProfileRegistry

        profiles = TokenProfileRegistry()
        profiles.register(TokenProfile(name="internal", issuers=frozenset({"x"})))
        util = JwtUtil(JwtBuilder().issuer("y").build())
        with pytest.raises(TokenProfileError):
            util.assert_profile("internal", registry=profiles)


# ── Back-compat: SYSTEM_ISSUER / is_system() ────────────────────────────────────


class TestSystemIssuerBackCompat:
    def test_is_system_default_issuer_still_true(self):
        # test_jwt.py:428-447 equivalents — must still pass unmodified.
        tok = JwtBuilder().issuer(SYSTEM_ISSUER).build()
        assert JwtUtil(tok).is_system() is True

    def test_is_system_non_system_issuer_still_false(self):
        tok = JwtBuilder().issuer("some-other-issuer").build()
        assert JwtUtil(tok).is_system() is False

    def test_is_system_class_level_override_still_works(self, monkeypatch):
        monkeypatch.setattr(JwtUtil, "SYSTEM_ISSUER", "custom-system")
        tok = JwtBuilder().issuer("custom-system").build()
        assert JwtUtil(tok).is_system() is True
        tok_default = JwtBuilder().issuer(SYSTEM_ISSUER).build()
        assert JwtUtil(tok_default).is_system() is False

    def test_system_profile_env_var_makes_is_system_issuer_aware(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_PROFILE__SYSTEM__ISS", "my-org/internal")
        tok = JwtBuilder().issuer("my-org/internal").build()
        assert JwtUtil(tok).is_system() is True
        tok_default = JwtBuilder().issuer(SYSTEM_ISSUER).build()
        assert JwtUtil(tok_default).is_system() is False


# ── JwtBuilder.as_profile ────────────────────────────────────────────────────────


class TestJwtBuilderAsProfile:
    def test_as_profile_sets_iss_token_type_aud(self):
        from varco_core.jwt.profile import TokenProfile

        profile = TokenProfile(
            name="internal",
            issuers=frozenset({"mesh-signer"}),
            token_type="system",
            audiences=frozenset({"orders"}),
        )
        token = JwtBuilder().subject("svc_1").as_profile(profile).build()
        assert token.iss == "mesh-signer"
        assert token.token_type == "system"
        assert token.aud == "orders" or (isinstance(token.aud, frozenset) and "orders" in token.aud)
