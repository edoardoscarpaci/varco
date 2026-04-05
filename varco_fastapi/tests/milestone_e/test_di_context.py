"""
Milestone E — DI context token tests (RequestContext, JwtContext).

Tests cover:
- RequestContext frozen dataclass fields
- JwtContext frozen dataclass fields
- get_request_context() assembles from ContextVars
- get_jwt_context() parses and handles missing token gracefully
"""

from __future__ import annotations

import pytest

from varco_fastapi.context import (
    JwtContext,
    RequestContext,
    auth_context,
    get_jwt_context,
    get_request_context,
    request_scope,
)
from varco_core.auth.base import AuthContext


class TestRequestContext:
    def test_frozen_dataclass(self):
        """RequestContext is immutable."""
        ctx = AuthContext(
            user_id="u1", roles=frozenset(), scopes=frozenset(), grants=()
        )
        rc = RequestContext(auth=ctx, request_id="req_1", token="tok")
        with pytest.raises(Exception):
            rc.request_id = "other"  # type: ignore[misc]

    def test_fields_stored(self):
        """All fields are stored correctly."""
        ctx = AuthContext(
            user_id="u1", roles=frozenset(), scopes=frozenset(), grants=()
        )
        rc = RequestContext(auth=ctx, request_id="req_1", token=None)
        assert rc.auth is ctx
        assert rc.request_id == "req_1"
        assert rc.token is None


class TestJwtContext:
    def test_frozen_dataclass(self):
        """JwtContext is immutable."""
        jc = JwtContext(token=None, raw=None)
        with pytest.raises(Exception):
            jc.raw = "other"  # type: ignore[misc]

    def test_none_when_no_token(self):
        """JwtContext with raw=None has token=None."""
        jc = JwtContext(token=None, raw=None)
        assert jc.token is None
        assert jc.raw is None


class TestGetRequestContext:
    async def test_returns_request_context(self):
        """get_request_context() returns a RequestContext from ContextVars."""
        auth = AuthContext(
            user_id="u1", roles=frozenset(), scopes=frozenset(), grants=()
        )
        async with request_scope(request_id="req-test") as rid:
            async with auth_context(auth, token="raw_tok"):
                rc = get_request_context()
                assert isinstance(rc, RequestContext)
                assert rc.auth is auth
                assert rc.request_id == rid
                assert rc.token == "raw_tok"

    async def test_raises_outside_request_scope(self):
        """get_request_context() raises RuntimeError outside a request scope."""
        with pytest.raises(RuntimeError):
            get_request_context()


class TestGetJwtContext:
    async def test_returns_none_when_no_token(self):
        """get_jwt_context() returns None fields when no bearer token is set."""
        # No request_token_var set — token is None
        jc = get_jwt_context()
        assert jc.token is None
        assert jc.raw is None

    async def test_returns_raw_token_when_set(self):
        """get_jwt_context() captures the raw token from the ContextVar."""
        auth = AuthContext(
            user_id="u1", roles=frozenset(), scopes=frozenset(), grants=()
        )
        async with request_scope(request_id="req"):
            async with auth_context(auth, token="raw_bearer"):
                jc = get_jwt_context()
                assert jc.raw == "raw_bearer"
