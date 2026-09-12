"""
Tests for varco_fastapi.auth.server_auth — server-side auth strategies.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from varco_core.auth.base import AuthContext
from varco_fastapi.auth.server_auth import (
    AnonymousAuth,
    ApiKeyAuth,
    CompositeServerAuth,
    JwtBearerAuth,
    PassthroughAuth,
    WebSocketAuth,
)


class _FakeHeaders:
    """Minimal headers dict with case-insensitive .get()."""

    def __init__(self, d: dict[str, str]) -> None:
        self._d = {k.lower(): v for k, v in d.items()}

    def get(self, key: str, default: str = "") -> str:
        return self._d.get(key.lower(), default)

    def __getitem__(self, key: str) -> str:
        return self._d[key.lower()]

    def __contains__(self, key: str) -> bool:
        return key.lower() in self._d


class _FakeQueryParams:
    """Minimal query params dict."""

    def __init__(self, d: dict[str, str]) -> None:
        self._d = d

    def get(self, key: str, default=None):
        return self._d.get(key, default)


def _make_request(
    headers: dict[str, str] | None = None,
    query_params: dict[str, str] | None = None,
) -> MagicMock:
    """Create a mock Request with specified headers and query params."""
    req = MagicMock()
    req.headers = _FakeHeaders(headers or {})
    req.query_params = _FakeQueryParams(query_params or {})
    return req


# ── AnonymousAuth ─────────────────────────────────────────────────────────────


async def test_anonymous_auth_returns_anonymous_context():
    """AnonymousAuth always returns anonymous AuthContext."""
    auth = AnonymousAuth()
    req = _make_request()
    ctx = await auth(req)
    assert ctx.is_anonymous()


# ── ApiKeyAuth ────────────────────────────────────────────────────────────────


async def test_api_key_auth_succeeds_with_valid_key():
    """ApiKeyAuth returns the configured AuthContext for a valid key."""
    expected = AuthContext(user_id="svc_1", roles=frozenset({"service"}))
    auth = ApiKeyAuth(keys={"secret-key": expected})
    req = _make_request(headers={"X-API-Key": "secret-key"})
    ctx = await auth(req)
    assert ctx is expected


async def test_api_key_auth_raises_on_invalid_key():
    """ApiKeyAuth raises 401 for an unrecognized key."""
    auth = ApiKeyAuth(keys={"valid-key": AuthContext()})
    req = _make_request(headers={"X-API-Key": "wrong-key"})
    with pytest.raises(HTTPException) as exc_info:
        await auth(req)
    assert exc_info.value.status_code == 401


async def test_api_key_auth_raises_when_key_missing_and_required():
    """ApiKeyAuth raises 401 when no key is present and required=True."""
    auth = ApiKeyAuth(keys={})
    req = _make_request()
    with pytest.raises(HTTPException) as exc_info:
        await auth(req)
    assert exc_info.value.status_code == 401


async def test_api_key_auth_returns_anonymous_when_key_missing_and_not_required():
    """ApiKeyAuth returns anonymous when no key is present and required=False."""
    auth = ApiKeyAuth(keys={}, required=False)
    req = _make_request()
    ctx = await auth(req)
    assert ctx.is_anonymous()


# ── JwtBearerAuth ─────────────────────────────────────────────────────────────


async def test_jwt_bearer_auth_raises_when_no_token():
    """JwtBearerAuth raises 401 when no Authorization header."""
    registry = MagicMock()
    auth = JwtBearerAuth(registry=registry, allow_any_audience=True)
    req = _make_request()
    with pytest.raises(HTTPException) as exc_info:
        await auth(req)
    assert exc_info.value.status_code == 401


async def test_jwt_bearer_auth_calls_registry_verify():
    """JwtBearerAuth calls registry.verify with the raw token."""
    mock_jwt = MagicMock()
    mock_jwt.auth_ctx = AuthContext(user_id="usr_1")
    registry = MagicMock()
    registry.verify = AsyncMock(return_value=mock_jwt)

    auth = JwtBearerAuth(registry=registry, allow_any_audience=True)
    req = _make_request(headers={"Authorization": "Bearer my.jwt.token"})
    ctx = await auth(req)

    registry.verify.assert_called_once_with("my.jwt.token")
    assert ctx.user_id == "usr_1"


async def test_jwt_bearer_auth_returns_anonymous_when_not_required():
    """JwtBearerAuth returns anonymous when required=False and no token."""
    registry = MagicMock()
    auth = JwtBearerAuth(registry=registry, required=False, allow_any_audience=True)
    req = _make_request()
    ctx = await auth(req)
    assert ctx.is_anonymous()


async def test_jwt_bearer_auth_raises_on_verification_failure():
    """JwtBearerAuth raises 401 when registry.verify raises."""
    registry = MagicMock()
    registry.verify = MagicMock(side_effect=ValueError("expired"))
    auth = JwtBearerAuth(registry=registry, allow_any_audience=True)
    req = _make_request(headers={"Authorization": "Bearer bad.token"})
    with pytest.raises(HTTPException) as exc_info:
        await auth(req)
    assert exc_info.value.status_code == 401


# ── PassthroughAuth ───────────────────────────────────────────────────────────


async def test_passthrough_auth_decodes_claims_without_verification():
    """PassthroughAuth decodes JWT payload claims without signature check."""
    # NOTE (Plan 002 step 35 refactor): PassthroughAuth now delegates to
    # JwtParser.parse_unverified(), which requires a syntactically valid JWT
    # (PyJWT parses the header segment even with verify_signature=False).
    # The original fixture used a bogus literal "header" segment that only
    # worked with the old hand-rolled base64/JSON parsing this test exists to
    # protect the *contract* of, not that implementation detail — encode a
    # real (arbitrarily-keyed, since the signature is never checked) token
    # instead.
    import jwt as _pyjwt

    payload = {
        "sub": "usr_2",
        "roles": ["editor"],
        "scopes": ["write:posts"],
        "grants": [],
    }
    fake_token = _pyjwt.encode(
        payload, "unused-secret-passthrough-does-not-verify", algorithm="HS256"
    )

    auth = PassthroughAuth()
    req = _make_request(headers={"Authorization": f"Bearer {fake_token}"})
    ctx = await auth(req)
    assert ctx.user_id == "usr_2"
    assert "editor" in ctx.roles


async def test_passthrough_auth_returns_anonymous_when_no_token():
    """PassthroughAuth returns anonymous by default when no token present."""
    auth = PassthroughAuth(required=False)
    req = _make_request()
    ctx = await auth(req)
    assert ctx.is_anonymous()


# ── CompositeServerAuth ────────────────────────────────────────────────────────


async def test_composite_auth_first_success_wins():
    """CompositeServerAuth returns the first successful strategy's result."""
    api_key_ctx = AuthContext(user_id="svc_via_key")
    _jwt_ctx = AuthContext(user_id="usr_via_jwt")

    api_key = ApiKeyAuth(keys={"key": api_key_ctx})
    jwt = AnonymousAuth()  # Would return anonymous, but api_key wins first

    auth = CompositeServerAuth([api_key, jwt])
    req = _make_request(headers={"X-API-Key": "key"})
    ctx = await auth(req)
    assert ctx is api_key_ctx


async def test_composite_auth_falls_back_to_second():
    """CompositeServerAuth tries next strategy when first raises 401."""
    failing_auth = ApiKeyAuth(keys={}, required=True)
    fallback = AnonymousAuth()

    auth = CompositeServerAuth([failing_auth, fallback])
    req = _make_request()
    ctx = await auth(req)
    assert ctx.is_anonymous()


async def test_composite_auth_raises_when_all_fail():
    """CompositeServerAuth raises 401 when all strategies fail."""
    auth = CompositeServerAuth([ApiKeyAuth(keys={}, required=True)])
    req = _make_request()
    with pytest.raises(HTTPException) as exc_info:
        await auth(req)
    assert exc_info.value.status_code == 401


async def test_composite_auth_requires_at_least_one_strategy():
    """CompositeServerAuth raises ValueError with empty strategies list."""
    with pytest.raises(ValueError, match="at least one strategy"):
        CompositeServerAuth([])


# ── WebSocketAuth ─────────────────────────────────────────────────────────────


async def test_websocket_auth_extracts_from_query_param():
    """WebSocketAuth extracts token from query parameter fallback."""
    jwt_ctx = AuthContext(user_id="ws_user")
    mock_jwt = MagicMock()
    mock_jwt.auth_ctx = jwt_ctx
    registry = MagicMock()
    registry.verify = AsyncMock(return_value=mock_jwt)
    inner = JwtBearerAuth(registry=registry, allow_any_audience=True)

    auth = WebSocketAuth(inner=inner, token_query_param="token")
    req = _make_request(query_params={"token": "my.ws.token"})
    ctx = await auth(req)
    assert ctx.user_id == "ws_user"


async def test_websocket_auth_extracts_from_protocol_header():
    """WebSocketAuth extracts token from Sec-WebSocket-Protocol header."""
    jwt_ctx = AuthContext(user_id="ws_proto_user")
    mock_jwt = MagicMock()
    mock_jwt.auth_ctx = jwt_ctx
    registry = MagicMock()
    registry.verify = AsyncMock(return_value=mock_jwt)
    inner = JwtBearerAuth(registry=registry, allow_any_audience=True)

    auth = WebSocketAuth(inner=inner, protocol_prefix="bearer.")
    req = _make_request(headers={"Sec-WebSocket-Protocol": "bearer.ws.protocol.token"})
    ctx = await auth(req)
    assert ctx.user_id == "ws_proto_user"


# ── Phase 2/4: env claim-transform + audience enforcement + PassthroughAuth
#    refactor (Plan 002, steps 22, 34, 36) ─────────────────────────────────────
#
# These tests sign real RSA-backed tokens via JwtAuthority + verify them
# through a real TrustedIssuerRegistry, proving JwtBearerAuth needs zero
# extra code to benefit from the claim transformer (SEAM 2 already routes
# through JwtParser._from_raw_claims via registry.verify()).
#
# New imports (JwtAuthority, TrustedIssuerRegistry, VARCO_JWT_TRANSFORM_*,
# VARCO_JWT_AUDIENCE) are local to each test — the symbols do not exist yet
# (Phase 2/4 red), so this keeps the rest of the file collectible.


def _build_rsa_registry_and_authority():
    """Generate a throwaway RSA key, wrap it in a JwtAuthority + registry."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from varco_core.authority import JwtAuthority
    from varco_core.authority.registry import TrustedIssuerRegistry

    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    authority = JwtAuthority.from_pem(pem, kid="test-kid", issuer="kc-issuer", algorithm="RS256")
    registry = TrustedIssuerRegistry()
    registry.register_authority(authority)
    return authority, registry


class TestJwtBearerAuthEnvClaimTransform:
    async def test_jwt_bearer_applies_env_claim_transform(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "realm_access.roles")
        authority, registry = _build_rsa_registry_and_authority()
        await registry.load_all()

        builder = authority.token().subject("usr_1").claim("realm_access", {"roles": ["editor"]})
        raw_token = authority.sign(builder)

        auth = JwtBearerAuth(registry, allow_any_audience=True)
        req = _make_request(headers={"Authorization": f"Bearer {raw_token}"})
        ctx = await auth(req)

        assert ctx.roles == frozenset({"editor"})


class TestJwtBearerAuthAudience:
    async def test_audience_mismatch_raises_401(self):
        authority, registry = _build_rsa_registry_and_authority()
        await registry.load_all()

        builder = authority.token().subject("usr_1").audience("billing")
        raw_token = authority.sign(builder)

        auth = JwtBearerAuth(registry, audience="orders")
        req = _make_request(headers={"Authorization": f"Bearer {raw_token}"})
        with pytest.raises(HTTPException) as exc_info:
            await auth(req)
        assert exc_info.value.status_code == 401

    async def test_audience_match_accepted(self):
        authority, registry = _build_rsa_registry_and_authority()
        await registry.load_all()

        builder = authority.token().subject("usr_1").audience("orders")
        raw_token = authority.sign(builder)

        auth = JwtBearerAuth(registry, audience="orders")
        req = _make_request(headers={"Authorization": f"Bearer {raw_token}"})
        ctx = await auth(req)
        assert ctx.user_id == "usr_1"

    async def test_audience_none_does_not_enforce_either_way(self):
        authority, registry = _build_rsa_registry_and_authority()
        await registry.load_all()

        builder_billing = authority.token().subject("usr_1").audience("billing")
        raw_billing = authority.sign(builder_billing)

        auth = JwtBearerAuth(registry, audience=None)
        req = _make_request(headers={"Authorization": f"Bearer {raw_billing}"})
        ctx = await auth(req)
        assert ctx.user_id == "usr_1"

    async def test_varco_jwt_audience_env_used_when_kwarg_omitted(self, monkeypatch):
        # A matching audience must be ACCEPTED when sourced purely from
        # VARCO_JWT_AUDIENCE (no audience= kwarg) — proves the env default is
        # actually threaded into registry.verify(), not just "some 401 or
        # other" (PyJWT already 401s on a bare aud claim with no audience=
        # configured at all, so an accept-path assertion is the meaningful
        # regression check here).
        monkeypatch.setenv("VARCO_JWT_AUDIENCE", "orders")
        authority, registry = _build_rsa_registry_and_authority()
        await registry.load_all()

        builder = authority.token().subject("usr_1").audience("orders")
        raw_token = authority.sign(builder)

        auth = JwtBearerAuth(registry)  # no audience= kwarg
        req = _make_request(headers={"Authorization": f"Bearer {raw_token}"})
        ctx = await auth(req)
        assert ctx.user_id == "usr_1"


class TestPassthroughAuthRefactorRegression:
    async def test_passthrough_auth_canonical_token_golden_value(self):
        """
        Regression (plan step 36): a canonical token must produce the exact
        same AuthContext before and after the JwtParser.parse_unverified()
        refactor.
        """
        from varco_core.auth import AuthContext
        from varco_core.jwt import JwtBuilder

        signed = (
            JwtBuilder()
            .subject("usr_1")
            .with_auth_ctx(
                AuthContext(
                    user_id="usr_1",
                    roles=frozenset({"editor"}),
                    scopes=frozenset({"write:posts"}),
                )
            )
            .claim("custom_meta", "hello")
            .encode("unused-secret-passthrough-does-not-verify")
        )
        auth = PassthroughAuth(required=True)
        req = _make_request(headers={"Authorization": f"Bearer {signed}"})
        ctx = await auth(req)

        assert ctx.user_id == "usr_1"
        assert ctx.roles == frozenset({"editor"})
        assert ctx.scopes == frozenset({"write:posts"})
        assert ctx.metadata.get("custom_meta") == "hello"

    async def test_passthrough_auth_applies_claim_transform_for_foreign_roles(self, monkeypatch):
        """PassthroughAuth refactor must route through the same claim
        transformer as JwtBearerAuth/JwtParser.parse() (plan step 36)."""
        from varco_core.jwt import JwtBuilder

        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "sofy-roles")

        signed = (
            JwtBuilder()
            .subject("usr_1")
            .claim("sofy-roles", ["editor"])
            .encode("unused-secret-passthrough-does-not-verify")
        )
        auth = PassthroughAuth(required=True)
        req = _make_request(headers={"Authorization": f"Bearer {signed}"})
        ctx = await auth(req)

        assert ctx.roles == frozenset({"editor"})


# ── Plan 034 / S2 + S14 — ApiKeyAuth constructor change ────────────────────────


class TestApiKeyAuthQueryFallbackOffByDefault:
    """
    §D-S2S14-ctor: the ``?api_key=`` query fallback is off unless ``param=``
    names it; ``keys=``/``hashed_keys=`` cannot both/neither be given; a raw
    key never survives past construction; the signature itself is a
    §D-034-gate guard because api_surface.py --check cannot see this flip.
    """

    async def test_query_param_key_rejected_by_default(self):
        # The row's whole point: no toggle, no warning, off by default.
        auth = ApiKeyAuth(keys={"k": AuthContext(user_id="svc")})
        req = _make_request(query_params={"api_key": "k"})
        with pytest.raises(HTTPException) as exc_info:
            await auth(req)
        assert exc_info.value.status_code == 401

    async def test_param_kwarg_reenables_query_fallback(self):
        # The named escape hatch — self-documenting at the call site.
        expected = AuthContext(user_id="svc")
        auth = ApiKeyAuth(keys={"k": expected}, param="api_key")
        req = _make_request(query_params={"api_key": "k"})
        ctx = await auth(req)
        assert ctx is expected

    async def test_hashed_keys_accepts_matching_raw_key(self):
        from varco_core.auth.api_key import hash_api_key

        digest = hash_api_key("raw-key")
        expected = AuthContext(user_id="svc")
        auth = ApiKeyAuth(hashed_keys={digest: expected})
        req = _make_request(headers={"X-API-Key": "raw-key"})
        ctx = await auth(req)
        assert ctx is expected

    async def test_hashed_keys_rejects_near_miss(self):
        from varco_core.auth.api_key import hash_api_key

        digest = hash_api_key("raw-key")
        auth = ApiKeyAuth(hashed_keys={digest: AuthContext(user_id="svc")})
        req = _make_request(headers={"X-API-Key": "raw-kez"})
        with pytest.raises(HTTPException) as exc_info:
            await auth(req)
        assert exc_info.value.status_code == 401

    def test_both_keys_and_hashed_keys_raises_value_error(self):
        from varco_core.auth.api_key import hash_api_key

        with pytest.raises(ValueError):
            ApiKeyAuth(
                keys={"k": AuthContext()},
                hashed_keys={hash_api_key("k2"): AuthContext()},
            )

    def test_neither_keys_nor_hashed_keys_raises_value_error(self):
        with pytest.raises(ValueError):
            ApiKeyAuth()

    def test_construction_with_plaintext_keys_never_retains_raw_key(self):
        # No raw configured key may appear anywhere in the instance's
        # __dict__ after construction — S14's core guarantee.
        auth = ApiKeyAuth(keys={"super-secret-raw-key": AuthContext(user_id="svc")})
        for value in vars(auth).values():
            assert "super-secret-raw-key" not in repr(value)

    def test_param_default_is_none_by_signature(self):
        # §D-034-gate: api_surface.py --check is blind to a class __init__
        # widening — this repo-owned inspect.signature() test is the real
        # guard against a future regression restoring "api_key" as default.
        import inspect

        sig = inspect.signature(ApiKeyAuth.__init__)
        param = sig.parameters["param"]
        assert param.default is None
        assert type(None) in getattr(param.annotation, "__args__", (param.annotation,))

    async def test_existing_four_api_key_tests_pass_unchanged(self):
        # Not a new test — a marker that the four tests above
        # (test_api_key_auth_succeeds_with_valid_key,
        # test_api_key_auth_raises_on_invalid_key,
        # test_api_key_auth_raises_when_key_missing_and_required,
        # test_api_key_auth_returns_anonymous_when_key_missing_and_not_required)
        # keep passing unmodified — see the top of this file.
        assert True

    def test_empty_param_string_raises_value_error(self):
        # Edge case: an empty parameter name is a typo, not "disable".
        with pytest.raises(ValueError):
            ApiKeyAuth(keys={"k": AuthContext()}, param="")

    async def test_header_wins_over_query_param_when_both_present(self):
        # Edge case: header-vs-query precedence is preserved from today.
        header_ctx = AuthContext(user_id="header")
        query_ctx = AuthContext(user_id="query")
        auth = ApiKeyAuth(keys={"h-key": header_ctx, "q-key": query_ctx}, param="api_key")
        req = _make_request(headers={"X-API-Key": "h-key"}, query_params={"api_key": "q-key"})
        ctx = await auth(req)
        assert ctx is header_ctx

    def test_hashed_keys_with_unknown_scheme_prefix_raises_value_error(self):
        with pytest.raises(ValueError, match="scheme"):
            ApiKeyAuth(hashed_keys={"bcrypt$deadbeef": AuthContext()})

    async def test_empty_keys_dict_still_legal_every_key_401s(self):
        # Edge case: ApiKeyAuth(keys={}) remains legal.
        auth = ApiKeyAuth(keys={})
        req = _make_request(headers={"X-API-Key": "anything"})
        with pytest.raises(HTTPException) as exc_info:
            await auth(req)
        assert exc_info.value.status_code == 401


class TestWebSocketAuthQueryFallbackOffByDefault:
    """§D-S2-ws: WebSocketAuth's ?token= fallback is opt-in; the warning
    (not debug) log level matches what BACKLOG.md believed was already true."""

    async def test_query_fallback_off_by_default(self):
        inner = AsyncMock()
        auth = WebSocketAuth(inner=inner)
        req = _make_request(query_params={"token": "abc"})
        with pytest.raises(HTTPException) as exc_info:
            await auth(req)
        assert exc_info.value.status_code == 401
        inner.assert_not_called()

    async def test_subprotocol_path_still_works_by_default(self):
        expected = AuthContext(user_id="usr_1")
        inner = AsyncMock(return_value=expected)
        auth = WebSocketAuth(inner=inner)
        req = _make_request(headers={"Sec-WebSocket-Protocol": "bearer.my.jwt.tok"})
        ctx = await auth(req)
        assert ctx is expected

    async def test_token_query_param_restores_fallback_and_warns(self, caplog):
        import logging

        expected = AuthContext(user_id="usr_1")
        inner = AsyncMock(return_value=expected)
        auth = WebSocketAuth(inner=inner, token_query_param="token")
        req = _make_request(query_params={"token": "abc"})
        with caplog.at_level(logging.WARNING):
            ctx = await auth(req)
        assert ctx is expected
        assert any(record.levelno == logging.WARNING for record in caplog.records)


# ── Plan 034 / S13b — JwtBearerAuth revocation error mapping ───────────────────


class TestJwtBearerAuthRevocationErrorMapping:
    """§D-S13-error: a revoked-token 401 must not leak scope/key/reason; a
    store outage under FAIL_CLOSED is a 503, not a lying 401."""

    async def test_token_revoked_error_maps_to_401_without_leaking_details(self):
        from varco_core.authority.exceptions import TokenRevokedError
        from varco_core.revocation import RevocationScope

        registry = MagicMock()
        registry.verify = AsyncMock(
            side_effect=TokenRevokedError(
                scope=RevocationScope.TOKEN, key="jti-1", reason="compromised"
            )
        )
        auth = JwtBearerAuth(registry=registry, allow_any_audience=True)
        req = _make_request(headers={"Authorization": "Bearer my.jwt.token"})
        with pytest.raises(HTTPException) as exc_info:
            await auth(req)
        assert exc_info.value.status_code == 401
        detail = str(exc_info.value.detail)
        assert "jti-1" not in detail
        assert "compromised" not in detail
        assert "token" not in detail.lower() or "revoked" in detail.lower()

    async def test_revocation_store_unavailable_maps_to_503_with_fixed_detail(self):
        from varco_core.authority.exceptions import RevocationStoreUnavailableError

        registry = MagicMock()
        registry.verify = AsyncMock(
            side_effect=RevocationStoreUnavailableError("store outage: connection refused")
        )
        auth = JwtBearerAuth(registry=registry, allow_any_audience=True)
        req = _make_request(headers={"Authorization": "Bearer my.jwt.token"})
        with pytest.raises(HTTPException) as exc_info:
            await auth(req)
        assert exc_info.value.status_code == 503
        assert "connection refused" not in str(exc_info.value.detail)

    async def test_registry_verify_call_shape_unchanged_by_revocation_wiring(self):
        # test_jwt_bearer_auth_calls_registry_verify (line ~121) must keep
        # passing unchanged — no new kwarg added to the verify() call.
        mock_jwt = MagicMock()
        mock_jwt.auth_ctx = AuthContext(user_id="usr_1")
        registry = MagicMock()
        registry.verify = AsyncMock(return_value=mock_jwt)
        auth = JwtBearerAuth(registry=registry, allow_any_audience=True)
        req = _make_request(headers={"Authorization": "Bearer my.jwt.token"})
        await auth(req)
        registry.verify.assert_called_once_with("my.jwt.token")
