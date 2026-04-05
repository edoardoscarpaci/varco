"""
Tests for varco_fastapi.context — ContextVars and context managers.
"""

from __future__ import annotations

import pytest

from varco_core.auth.base import AuthContext
from varco_fastapi.context import (
    auth_context,
    auth_context_var,
    get_auth_context,
    get_auth_context_or_none,
    get_request_id,
    get_request_token,
    request_scope,
    request_id_var,
)


async def test_get_auth_context_raises_outside_context():
    """get_auth_context() raises RuntimeError when no context is set."""
    auth_context_var.set(None)
    with pytest.raises(RuntimeError, match="No AuthContext"):
        get_auth_context()


async def test_get_auth_context_or_none_returns_none_outside_context():
    """get_auth_context_or_none() returns None when no context is set."""
    auth_context_var.set(None)
    assert get_auth_context_or_none() is None


async def test_auth_context_sets_and_restores():
    """auth_context() sets auth_context_var and restores on exit."""
    ctx = AuthContext(user_id="usr_1", roles=frozenset({"admin"}))
    auth_context_var.set(None)

    async with auth_context(ctx, token="tok_abc"):
        assert get_auth_context() is ctx
        assert get_request_token() == "tok_abc"

    # Restored to None after exit
    assert get_auth_context_or_none() is None
    assert get_request_token() is None


async def test_auth_context_nested():
    """Nested auth_context() restores each level correctly."""
    outer = AuthContext(user_id="usr_outer")
    inner = AuthContext(user_id="usr_inner")

    async with auth_context(outer, token="tok_outer"):
        assert get_auth_context().user_id == "usr_outer"
        async with auth_context(inner, token="tok_inner"):
            assert get_auth_context().user_id == "usr_inner"
            assert get_request_token() == "tok_inner"
        # Restored to outer
        assert get_auth_context().user_id == "usr_outer"
        assert get_request_token() == "tok_outer"

    # Fully cleaned up
    assert get_auth_context_or_none() is None


async def test_request_scope_generates_request_id():
    """request_scope() generates a request_id if not provided."""
    request_id_var.set(None)

    async with request_scope() as rid:
        assert rid is not None
        assert len(rid) > 0
        assert get_request_id() == rid

    # Restored
    assert request_id_var.get() is None


async def test_request_scope_uses_provided_id():
    """request_scope() uses the provided request_id."""
    async with request_scope(request_id="req_custom_123") as rid:
        assert rid == "req_custom_123"
        assert get_request_id() == "req_custom_123"


async def test_request_scope_sets_correlation_id():
    """request_scope() enters correlation_context with the request_id."""
    from varco_core.tracing import current_correlation_id

    async with request_scope(request_id="req_corr_456") as _rid:
        assert current_correlation_id() == "req_corr_456"

    # Correlation ID is restored to None after exit
    assert current_correlation_id() is None


async def test_get_request_id_raises_outside_scope():
    """get_request_id() raises RuntimeError when request_scope() is not entered."""
    request_id_var.set(None)
    with pytest.raises(RuntimeError, match="No request_id"):
        get_request_id()
