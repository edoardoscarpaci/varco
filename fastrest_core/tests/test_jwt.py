"""
Unit tests for fastrest_core.jwt
=================================

Covers:
    - JsonWebToken  — construction, to_claims(), __repr__
    - JwtBuilder    — all setters, build(), encode(), reserved-key guard
    - JwtParser     — round-trip decode, auth-ctx reconstruction, unverified decode
    - JwtUtil       — every predicate and accessor method

Testing strategy:
    Pure-logic tests (JsonWebToken, JwtUtil, most of JwtBuilder) use
    ``JwtBuilder.build()`` so no secret is needed — fast and isolated.
    Round-trip tests (JwtBuilder.encode → JwtParser.parse) use a shared
    constant secret.  parse_unverified() tests use a real signed token so the
    three-segment structure is valid; then we verify without checking the sig.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from fastrest_core.auth import Action, AuthContext, ResourceGrant
from fastrest_core.jwt import (
    SYSTEM_ISSUER,
    JsonWebToken,
    JwtBuilder,
    JwtParser,
    JwtUtil,
)

# ── Shared test fixtures ───────────────────────────────────────────────────────

# Constant secret for all round-trip encode/decode tests.
_SECRET = "test-secret-do-not-use-in-production"

# A fixed UTC datetime used for deterministic timestamp comparisons.
_NOW = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
_ONE_HOUR = timedelta(hours=1)

# A minimal AuthContext used across multiple test classes.
_AUTH_CTX = AuthContext(
    user_id="usr_123",
    roles=frozenset({"editor"}),
    scopes=frozenset({"write:posts"}),
    grants=(
        ResourceGrant("posts", frozenset({Action.READ, Action.LIST})),
        ResourceGrant("posts:abc", frozenset({Action.UPDATE})),
    ),
)


# ── JsonWebToken ───────────────────────────────────────────────────────────────


class TestJsonWebToken:
    """JsonWebToken is a frozen dataclass — immutable, value-equal, hashable."""

    def test_minimal_construction_all_defaults_none(self):
        # A bare token with no arguments should have all fields as None / empty.
        tok = JsonWebToken()
        assert tok.sub is None
        assert tok.iss is None
        assert tok.aud is None
        assert tok.exp is None
        assert tok.iat is None
        assert tok.nbf is None
        assert tok.jti is None
        assert tok.token_type is None
        assert tok.auth_ctx is None
        assert tok.extra_claims == {}

    def test_frozen_raises_on_write(self):
        # frozen=True — any attribute write must raise FrozenInstanceError.
        tok = JsonWebToken(sub="usr_1")
        with pytest.raises(Exception):
            tok.sub = "other"  # type: ignore[misc]

    def test_equality_by_value(self):
        # Two tokens with identical registered claims and auth_ctx are equal.
        a = JsonWebToken(sub="usr_1", iss="svc", token_type="access")
        b = JsonWebToken(sub="usr_1", iss="svc", token_type="access")
        assert a == b

    def test_extra_claims_excluded_from_equality(self):
        # extra_claims is excluded from __eq__ — tokens differing only in
        # extra claims compare as equal (documented behaviour).
        a = JsonWebToken(sub="usr_1", extra_claims={"foo": "bar"})
        b = JsonWebToken(sub="usr_1", extra_claims={"foo": "different"})
        assert a == b

    def test_hashable(self):
        # frozen dataclasses with all hashable fields are hashable.
        # extra_claims is excluded from __hash__ for the same reason as __eq__.
        tok = JsonWebToken(sub="usr_1")
        assert hash(tok) is not None
        s = {tok, tok}
        assert len(s) == 1

    def test_to_claims_omits_none_fields(self):
        # Only non-None fields should appear in the serialized claims dict.
        tok = JsonWebToken(sub="usr_1", iss="svc")
        claims = tok.to_claims()
        assert claims["sub"] == "usr_1"
        assert claims["iss"] == "svc"
        # Fields that were not set must be absent — keeps tokens compact
        assert "exp" not in claims
        assert "iat" not in claims
        assert "aud" not in claims
        assert "jti" not in claims

    def test_to_claims_datetime_converted_to_int(self):
        # RFC 7519 requires exp/iat/nbf as integer Unix timestamps.
        exp = datetime(2030, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        tok = JsonWebToken(exp=exp, iat=exp, nbf=exp)
        claims = tok.to_claims()
        assert isinstance(claims["exp"], int)
        assert isinstance(claims["iat"], int)
        assert isinstance(claims["nbf"], int)

    def test_to_claims_frozenset_aud_becomes_sorted_list(self):
        # PyJWT expects str or list for aud; frozenset must be normalised.
        tok = JsonWebToken(aud=frozenset({"svc-b", "svc-a"}))
        claims = tok.to_claims()
        # Must be a list, and sorted for deterministic output
        assert claims["aud"] == ["svc-a", "svc-b"]

    def test_to_claims_string_aud_stays_string(self):
        tok = JsonWebToken(aud="my-service")
        assert tok.to_claims()["aud"] == "my-service"

    def test_to_claims_auth_ctx_roles_scopes_grants(self):
        # AuthContext must be serialized into roles / scopes / grants claims.
        tok = JsonWebToken(auth_ctx=_AUTH_CTX)
        claims = tok.to_claims()
        assert "editor" in claims["roles"]
        assert "write:posts" in claims["scopes"]
        # Find the type-level grant
        post_grant = next(g for g in claims["grants"] if g["resource"] == "posts")
        assert "read" in post_grant["actions"]
        assert "list" in post_grant["actions"]

    def test_to_claims_empty_roles_not_emitted(self):
        # Empty collections must not emit the claim to stay compact.
        ctx = AuthContext(user_id="usr_1")  # no roles, scopes, grants
        tok = JsonWebToken(sub="usr_1", auth_ctx=ctx)
        claims = tok.to_claims()
        # Empty frozensets should not appear in claims
        assert "roles" not in claims
        assert "scopes" not in claims
        assert "grants" not in claims

    def test_to_claims_extra_claims_merged_last(self):
        # Extra claims appear alongside standard claims in the output.
        tok = JsonWebToken(sub="usr_1", extra_claims={"tenant_id": "t_abc"})
        claims = tok.to_claims()
        assert claims["tenant_id"] == "t_abc"
        assert claims["sub"] == "usr_1"

    def test_repr_contains_key_fields(self):
        tok = JsonWebToken(sub="usr_1", iss="svc", token_type="access")
        r = repr(tok)
        assert "usr_1" in r
        assert "svc" in r
        assert "access" in r


# ── JwtBuilder ────────────────────────────────────────────────────────────────


class TestJwtBuilder:
    """JwtBuilder fluent interface and build() / encode() terminal operations."""

    def test_build_returns_json_web_token(self):
        tok = JwtBuilder().subject("usr_1").build()
        assert isinstance(tok, JsonWebToken)

    def test_build_sets_all_fields(self):
        exp = _NOW + _ONE_HOUR
        tok = (
            JwtBuilder()
            .subject("usr_1")
            .issuer("svc")
            .audience("aud-svc")
            .expires_at(exp)
            .issued_at(_NOW)
            .not_before(_NOW)
            .token_id("jti-abc")
            .type("access")
            .with_auth_ctx(_AUTH_CTX)
            .claim("tenant_id", "t_123")
            .build()
        )
        assert tok.sub == "usr_1"
        assert tok.iss == "svc"
        assert tok.aud == "aud-svc"
        assert tok.exp == exp
        assert tok.iat == _NOW
        assert tok.nbf == _NOW
        assert tok.jti == "jti-abc"
        assert tok.token_type == "access"
        assert tok.auth_ctx == _AUTH_CTX
        assert tok.extra_claims["tenant_id"] == "t_123"

    def test_expires_in_is_relative_to_now(self):
        # expires_in should produce an exp close to now + delta.
        before = datetime.now(timezone.utc) + timedelta(hours=1) - timedelta(seconds=2)
        tok = JwtBuilder().expires_in(timedelta(hours=1)).build()
        after = datetime.now(timezone.utc) + timedelta(hours=1) + timedelta(seconds=2)
        assert tok.exp is not None
        assert before <= tok.exp <= after

    def test_issued_now_is_close_to_now(self):
        before = datetime.now(timezone.utc) - timedelta(seconds=2)
        tok = JwtBuilder().issued_now().build()
        after = datetime.now(timezone.utc) + timedelta(seconds=2)
        assert tok.iat is not None
        assert before <= tok.iat <= after

    def test_with_random_jti_generates_uuid(self):
        # Each call must generate a non-empty, unique jti value.
        tok1 = JwtBuilder().with_random_jti().build()
        tok2 = JwtBuilder().with_random_jti().build()
        assert tok1.jti is not None
        assert tok2.jti is not None
        # Collision probability for two UUIDv4 values is astronomically small
        assert tok1.jti != tok2.jti

    def test_frozenset_audience(self):
        tok = JwtBuilder().audience(frozenset({"svc-a", "svc-b"})).build()
        assert isinstance(tok.aud, frozenset)
        assert "svc-a" in tok.aud

    def test_claim_raises_on_reserved_key(self):
        # Silently overwriting standard claims via .claim() must be blocked.
        for reserved in (
            "sub",
            "iss",
            "aud",
            "exp",
            "iat",
            "nbf",
            "jti",
            "token_type",
            "roles",
            "scopes",
            "grants",
        ):
            with pytest.raises(ValueError, match="reserved"):
                JwtBuilder().claim(reserved, "bad")

    def test_build_isolates_extra_claims(self):
        # Mutating the builder's internal dict after build() must not affect
        # the already-built token (the token must own its own copy).
        builder = JwtBuilder().claim("k", "v1")
        tok = builder.build()
        builder.claim("k2", "v2")  # mutate builder after build
        assert "k2" not in tok.extra_claims  # token is isolated

    def test_encode_returns_string(self):
        s = JwtBuilder().subject("usr_1").encode(_SECRET)
        assert isinstance(s, str)
        # A JWT has three dot-separated segments
        assert s.count(".") == 2

    def test_encode_then_parse_round_trip(self):
        # Full round-trip: encode with builder → decode with parser.
        signed = (
            JwtBuilder()
            .subject("usr_1")
            .issuer("svc")
            .type("access")
            .expires_in(timedelta(hours=1))
            .with_auth_ctx(_AUTH_CTX)
            .encode(_SECRET)
        )
        tok_decoded = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert tok_decoded.sub == "usr_1"
        assert tok_decoded.iss == "svc"
        assert tok_decoded.token_type == "access"

    def test_repr_contains_subject_and_issuer(self):
        b = JwtBuilder().subject("usr_1").issuer("svc")
        assert "usr_1" in repr(b)
        assert "svc" in repr(b)


# ── JwtParser ────────────────────────────────────────────────────────────────


class TestJwtParser:
    """JwtParser.parse() and parse_unverified() decoding correctness."""

    def _signed(self, **kwargs) -> str:
        """Helper: build and sign a token with the shared secret."""
        builder = JwtBuilder()
        if "sub" in kwargs:
            builder = builder.subject(kwargs.pop("sub"))
        if "iss" in kwargs:
            builder = builder.issuer(kwargs.pop("iss"))
        if "auth_ctx" in kwargs:
            builder = builder.with_auth_ctx(kwargs.pop("auth_ctx"))
        if "expires_in" in kwargs:
            builder = builder.expires_in(kwargs.pop("expires_in"))
        if "token_type" in kwargs:
            builder = builder.type(kwargs.pop("token_type"))
        return builder.encode(_SECRET)

    def test_parse_standard_claims(self):
        signed = self._signed(sub="usr_1", iss="svc", expires_in=_ONE_HOUR)
        tok = JwtParser.parse(signed, _SECRET)
        assert tok.sub == "usr_1"
        assert tok.iss == "svc"
        assert tok.exp is not None

    def test_parse_reconstructs_auth_ctx(self):
        signed = self._signed(sub="usr_1", auth_ctx=_AUTH_CTX)
        tok = JwtParser.parse(signed, _SECRET)
        assert tok.auth_ctx is not None
        assert tok.auth_ctx.user_id == "usr_1"
        assert "editor" in tok.auth_ctx.roles
        assert "write:posts" in tok.auth_ctx.scopes
        assert tok.auth_ctx.can(Action.READ, "posts")
        assert tok.auth_ctx.can(Action.UPDATE, "posts:abc")

    def test_parse_no_auth_claims_yields_none_auth_ctx(self):
        # A token with only sub/iss and no roles/scopes/grants → auth_ctx=None.
        # sub is already available as tok.sub; no AuthContext should be created.
        signed = self._signed(sub="usr_1", iss="svc")
        tok = JwtParser.parse(signed, _SECRET)
        assert tok.auth_ctx is None
        # But the sub claim must still be accessible directly on the token
        assert tok.sub == "usr_1"

    def test_parse_bad_secret_raises(self):
        import jwt as _jwt_lib

        signed = self._signed(sub="usr_1")
        with pytest.raises(_jwt_lib.InvalidSignatureError):
            JwtParser.parse(signed, "wrong-secret")

    def test_parse_expired_raises(self):
        import jwt as _jwt_lib

        # Build a token that expired in the past
        signed = (
            JwtBuilder()
            .subject("usr_1")
            .expires_at(datetime(2000, 1, 1, tzinfo=timezone.utc))
            .encode(_SECRET)
        )
        with pytest.raises(_jwt_lib.ExpiredSignatureError):
            JwtParser.parse(signed, _SECRET)

    def test_parse_expired_skipped_with_option(self):
        # Callers can opt out of expiry checking (e.g., token refresh flows).
        signed = (
            JwtBuilder()
            .subject("usr_1")
            .expires_at(datetime(2000, 1, 1, tzinfo=timezone.utc))
            .encode(_SECRET)
        )
        tok = JwtParser.parse(signed, _SECRET, options={"verify_exp": False})
        assert tok.sub == "usr_1"

    def test_parse_unverified_decodes_without_secret(self):
        signed = self._signed(sub="usr_1", iss="svc", auth_ctx=_AUTH_CTX)
        tok = JwtParser.parse_unverified(signed)
        assert tok.sub == "usr_1"
        assert tok.iss == "svc"
        assert tok.auth_ctx is not None
        assert "editor" in tok.auth_ctx.roles

    def test_parse_multi_audience_as_frozenset(self):
        signed = (
            JwtBuilder()
            .subject("usr_1")
            .audience(frozenset({"svc-a", "svc-b"}))
            .encode(_SECRET, algorithm="HS256")
        )
        # Must pass audience= to PyJWT or it raises InvalidAudienceError
        tok = JwtParser.parse(
            signed,
            _SECRET,
            audience=["svc-a", "svc-b"],
        )
        assert isinstance(tok.aud, frozenset)
        assert "svc-a" in tok.aud
        assert "svc-b" in tok.aud

    def test_parse_extra_claims_captured(self):
        signed = (
            JwtBuilder().subject("usr_1").claim("tenant_id", "t_abc").encode(_SECRET)
        )
        tok = JwtParser.parse(signed, _SECRET)
        assert tok.extra_claims.get("tenant_id") == "t_abc"

    def test_parse_grants_reconstruct_frozenset_of_action(self):
        # Action members must come back as typed Action, not raw strings.
        signed = self._signed(sub="usr_1", auth_ctx=_AUTH_CTX)
        tok = JwtParser.parse(signed, _SECRET)
        assert tok.auth_ctx is not None
        for grant in tok.auth_ctx.grants:
            for action in grant.actions:
                assert isinstance(action, Action)


# ── JwtUtil ───────────────────────────────────────────────────────────────────


class TestJwtUtilIssuer:
    """is_issuer() and is_system() predicate checks."""

    def test_is_issuer_matches(self):
        tok = JwtBuilder().issuer("my-service").build()
        assert JwtUtil(tok).is_issuer("my-service") is True

    def test_is_issuer_no_match(self):
        tok = JwtBuilder().issuer("other").build()
        assert JwtUtil(tok).is_issuer("my-service") is False

    def test_is_issuer_none_iss_returns_false(self):
        tok = JsonWebToken()  # no iss
        assert JwtUtil(tok).is_issuer("anything") is False

    def test_is_system_default_issuer(self):
        tok = JwtBuilder().issuer(SYSTEM_ISSUER).build()
        assert JwtUtil(tok).is_system() is True

    def test_is_system_non_system_issuer(self):
        tok = JwtBuilder().issuer("user-facing-service").build()
        assert JwtUtil(tok).is_system() is False

    def test_is_system_none_iss_returns_false(self):
        tok = JsonWebToken()
        assert JwtUtil(tok).is_system() is False

    def test_is_system_uses_class_level_override(self, monkeypatch):
        # JwtUtil.SYSTEM_ISSUER can be overridden at startup.
        monkeypatch.setattr(JwtUtil, "SYSTEM_ISSUER", "custom-system")
        tok = JwtBuilder().issuer("custom-system").build()
        assert JwtUtil(tok).is_system() is True
        # Make sure the original SYSTEM_ISSUER is not still treated as system
        tok_default = JwtBuilder().issuer(SYSTEM_ISSUER).build()
        assert JwtUtil(tok_default).is_system() is False


class TestJwtUtilTemporal:
    """is_expired() and is_valid_now() temporal predicate checks."""

    def test_is_expired_past_exp_returns_true(self):
        tok = JsonWebToken(exp=datetime(2000, 1, 1, tzinfo=timezone.utc))
        assert JwtUtil(tok).is_expired() is True

    def test_is_expired_future_exp_returns_false(self):
        tok = JsonWebToken(exp=datetime(2099, 1, 1, tzinfo=timezone.utc))
        assert JwtUtil(tok).is_expired() is False

    def test_is_expired_no_exp_returns_false(self):
        # Tokens without exp are treated as non-expiring.
        tok = JsonWebToken()
        assert JwtUtil(tok).is_expired() is False

    def test_is_valid_now_expired_returns_false(self):
        tok = JsonWebToken(exp=datetime(2000, 1, 1, tzinfo=timezone.utc))
        assert JwtUtil(tok).is_valid_now() is False

    def test_is_valid_now_future_exp_returns_true(self):
        tok = JsonWebToken(exp=datetime(2099, 1, 1, tzinfo=timezone.utc))
        assert JwtUtil(tok).is_valid_now() is True

    def test_is_valid_now_nbf_in_future_returns_false(self):
        tok = JsonWebToken(nbf=datetime(2099, 1, 1, tzinfo=timezone.utc))
        assert JwtUtil(tok).is_valid_now() is False

    def test_is_valid_now_nbf_in_past_returns_true(self):
        tok = JsonWebToken(nbf=datetime(2000, 1, 1, tzinfo=timezone.utc))
        assert JwtUtil(tok).is_valid_now() is True

    def test_is_valid_now_no_constraints_returns_true(self):
        # No exp, no nbf — token is perpetually valid.
        tok = JsonWebToken()
        assert JwtUtil(tok).is_valid_now() is True


class TestJwtUtilTokenType:
    """is_type(), is_access_token(), is_refresh_token()."""

    def test_is_type_matches(self):
        tok = JsonWebToken(token_type="access")
        assert JwtUtil(tok).is_type("access") is True

    def test_is_type_no_match(self):
        tok = JsonWebToken(token_type="refresh")
        assert JwtUtil(tok).is_type("access") is False

    def test_is_type_none_returns_false(self):
        tok = JsonWebToken()
        assert JwtUtil(tok).is_type("access") is False

    def test_is_access_token(self):
        assert JwtUtil(JsonWebToken(token_type="access")).is_access_token() is True
        assert JwtUtil(JsonWebToken(token_type="refresh")).is_access_token() is False

    def test_is_refresh_token(self):
        assert JwtUtil(JsonWebToken(token_type="refresh")).is_refresh_token() is True
        assert JwtUtil(JsonWebToken(token_type="access")).is_refresh_token() is False


class TestJwtUtilAuthCtx:
    """has_auth_ctx(), get_auth_ctx(), is_anonymous()."""

    def test_has_auth_ctx_true_when_set(self):
        tok = JsonWebToken(auth_ctx=_AUTH_CTX)
        assert JwtUtil(tok).has_auth_ctx() is True

    def test_has_auth_ctx_false_when_none(self):
        tok = JsonWebToken()
        assert JwtUtil(tok).has_auth_ctx() is False

    def test_get_auth_ctx_returns_context(self):
        tok = JsonWebToken(auth_ctx=_AUTH_CTX)
        assert JwtUtil(tok).get_auth_ctx() is _AUTH_CTX

    def test_get_auth_ctx_raises_when_absent(self):
        tok = JsonWebToken()
        with pytest.raises(ValueError, match="AuthContext"):
            JwtUtil(tok).get_auth_ctx()

    def test_is_anonymous_with_auth_ctx_set_user_id(self):
        # Delegates to AuthContext.is_anonymous() when auth_ctx is present.
        ctx = AuthContext(user_id="usr_1")
        tok = JsonWebToken(auth_ctx=ctx)
        assert JwtUtil(tok).is_anonymous() is False

    def test_is_anonymous_with_auth_ctx_no_user_id(self):
        ctx = AuthContext(user_id=None)
        tok = JsonWebToken(auth_ctx=ctx)
        assert JwtUtil(tok).is_anonymous() is True

    def test_is_anonymous_no_auth_ctx_no_sub(self):
        # Falls back to token.sub when no auth_ctx is present.
        tok = JsonWebToken()
        assert JwtUtil(tok).is_anonymous() is True

    def test_is_anonymous_no_auth_ctx_with_sub(self):
        tok = JsonWebToken(sub="usr_1")
        assert JwtUtil(tok).is_anonymous() is False


class TestJwtUtilAudience:
    """is_audience() handles both string and frozenset forms."""

    def test_is_audience_string_match(self):
        tok = JsonWebToken(aud="my-service")
        assert JwtUtil(tok).is_audience("my-service") is True

    def test_is_audience_string_no_match(self):
        tok = JsonWebToken(aud="my-service")
        assert JwtUtil(tok).is_audience("other") is False

    def test_is_audience_frozenset_contains(self):
        tok = JsonWebToken(aud=frozenset({"svc-a", "svc-b"}))
        assert JwtUtil(tok).is_audience("svc-a") is True
        assert JwtUtil(tok).is_audience("svc-b") is True

    def test_is_audience_frozenset_not_contains(self):
        tok = JsonWebToken(aud=frozenset({"svc-a"}))
        assert JwtUtil(tok).is_audience("svc-c") is False

    def test_is_audience_none_returns_false(self):
        tok = JsonWebToken()
        assert JwtUtil(tok).is_audience("anything") is False


class TestJwtUtilRepr:
    """__repr__ sanity check."""

    def test_repr_contains_key_info(self):
        tok = JsonWebToken(sub="usr_1", iss="svc", token_type="access")
        r = repr(JwtUtil(tok))
        assert "usr_1" in r
        assert "svc" in r
        assert "access" in r
        assert "has_auth_ctx" in r
        assert "is_expired" in r


# ── Integration: full round-trip with AuthContext ────────────────────────────


class TestFullRoundTrip:
    """Full encode → parse → util round-trip with an AuthContext."""

    def test_can_check_survives_round_trip(self):
        # After a full encode/decode, permission checks must still work.
        signed = (
            JwtBuilder()
            .subject("usr_123")
            .issuer("my-service")
            .type("access")
            .expires_in(timedelta(hours=1))
            .with_auth_ctx(_AUTH_CTX)
            .encode(_SECRET)
        )
        tok = JwtParser.parse(signed, _SECRET)
        util = JwtUtil(tok)

        assert util.has_auth_ctx()
        ctx = util.get_auth_ctx()
        assert ctx.can(Action.READ, "posts")
        assert ctx.can(Action.LIST, "posts")
        assert ctx.can(Action.UPDATE, "posts:abc")
        assert not ctx.can(Action.DELETE, "posts")

    def test_system_token_round_trip(self):
        # A system token should parse correctly and is_system() must be True.
        signed = (
            JwtBuilder()
            .subject("internal-worker")
            .issuer(SYSTEM_ISSUER)
            .type("system")
            .expires_in(timedelta(hours=24))
            .encode(_SECRET)
        )
        tok = JwtParser.parse(signed, _SECRET)
        util = JwtUtil(tok)

        assert util.is_system() is True
        assert util.is_type("system") is True
        assert util.has_auth_ctx() is False
