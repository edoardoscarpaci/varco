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
    auth = JwtBearerAuth(registry=registry)
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

    auth = JwtBearerAuth(registry=registry)
    req = _make_request(headers={"Authorization": "Bearer my.jwt.token"})
    ctx = await auth(req)

    registry.verify.assert_called_once_with("my.jwt.token")
    assert ctx.user_id == "usr_1"


async def test_jwt_bearer_auth_returns_anonymous_when_not_required():
    """JwtBearerAuth returns anonymous when required=False and no token."""
    registry = MagicMock()
    auth = JwtBearerAuth(registry=registry, required=False)
    req = _make_request()
    ctx = await auth(req)
    assert ctx.is_anonymous()


async def test_jwt_bearer_auth_raises_on_verification_failure():
    """JwtBearerAuth raises 401 when registry.verify raises."""
    registry = MagicMock()
    registry.verify = MagicMock(side_effect=ValueError("expired"))
    auth = JwtBearerAuth(registry=registry)
    req = _make_request(headers={"Authorization": "Bearer bad.token"})
    with pytest.raises(HTTPException) as exc_info:
        await auth(req)
    assert exc_info.value.status_code == 401


# ── PassthroughAuth ───────────────────────────────────────────────────────────


async def test_passthrough_auth_decodes_claims_without_verification():
    """PassthroughAuth decodes JWT payload claims without signature check."""
    import base64
    import json

    payload = {
        "sub": "usr_2",
        "roles": ["editor"],
        "scopes": ["write:posts"],
        "grants": [],
    }
    encoded = (
        base64.urlsafe_b64encode(json.dumps(payload).encode()).rstrip(b"=").decode()
    )
    fake_token = f"header.{encoded}.signature"

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
    inner = JwtBearerAuth(registry=registry)

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
    inner = JwtBearerAuth(registry=registry)

    auth = WebSocketAuth(inner=inner, protocol_prefix="bearer.")
    req = _make_request(headers={"Sec-WebSocket-Protocol": "bearer.ws.protocol.token"})
    ctx = await auth(req)
    assert ctx.user_id == "ws_proto_user"
