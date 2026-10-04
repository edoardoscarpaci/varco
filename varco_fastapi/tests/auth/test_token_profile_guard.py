"""
Tests for ``require_token_profile`` — RouteGuard checking
``ctx.metadata["token_profile"]`` (Plan 002, Phase 3, step 30).

A ``GenericRouter`` with ``@route(..., requires=require_token_profile("internal"))``:
    - matching profile → 200
    - non-matching profile → 403
    - anonymous caller → 403
    - ``ctx.metadata`` missing the key entirely → 403 with an actionable message

These tests are written RED — ``require_token_profile`` does not exist yet in
``varco_fastapi.auth.guard`` (Phase 3). They must fail with ImportError until
Phase 3 lands.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient
from providify import DIContainer
from varco_core.auth.api_key import hash_api_key
from varco_core.auth.base import AuthContext
from varco_fastapi.app import create_varco_app
from varco_fastapi.auth import ApiKeyAuth
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.presets import GenericRouter


def _make_app():
    from varco_fastapi.auth.guard import require_token_profile

    internal_ctx = AuthContext(user_id="svc_internal", metadata={"token_profile": "internal"})
    other_profile_ctx = AuthContext(user_id="svc_partner", metadata={"token_profile": "partner"})
    no_profile_ctx = AuthContext(user_id="usr_plain")  # metadata has no key at all

    auth = ApiKeyAuth(
        hashed_keys={
            hash_api_key("internal-key"): internal_ctx,
            hash_api_key("partner-key"): other_profile_ctx,
            hash_api_key("plain-key"): no_profile_ctx,
        },
        required=False,  # falls through to anonymous when no key given
    )

    class InternalRouter(GenericRouter):
        _prefix = "/internal"
        _auth = auth

        @route("GET", "/data", requires=require_token_profile("internal"))
        async def get_data(self, ctx: Any) -> dict:
            return {"ok": True}

    return create_varco_app(DIContainer(), routers=[InternalRouter], validate=False)


def test_matching_profile_returns_200():
    app = _make_app()
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/internal/data", headers={"X-API-Key": "internal-key"})
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_non_matching_profile_returns_403():
    app = _make_app()
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/internal/data", headers={"X-API-Key": "partner-key"})
    assert resp.status_code == 403


def test_anonymous_caller_returns_403():
    app = _make_app()
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/internal/data")  # no API key → anonymous
    assert resp.status_code == 403


def test_missing_token_profile_key_returns_403_with_actionable_message():
    app = _make_app()
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/internal/data", headers={"X-API-Key": "plain-key"})
    assert resp.status_code == 403
    body = resp.json()
    # Plan 054 / FLIP-10: `detail` is off by default; the RouteGuard denial
    # text is carried structurally in `params.operation`.
    assert "detail" not in body
    operation = body["params"]["operation"]
    assert "internal" in operation  # names the required profile
    assert "profile" in operation.lower()


# ── Unit-level RouteGuard.check() tests (no HTTP) ───────────────────────────────


async def test_guard_check_passes_for_matching_profile():
    from varco_core.exception.service import ServiceAuthorizationError  # noqa: F401
    from varco_fastapi.auth.guard import require_token_profile

    guard = require_token_profile("internal")
    ctx = AuthContext(user_id="svc", metadata={"token_profile": "internal"})
    await guard.check(ctx)  # must not raise


async def test_guard_check_denies_wrong_profile():
    from varco_core.exception.service import ServiceAuthorizationError
    from varco_fastapi.auth.guard import require_token_profile

    guard = require_token_profile("internal")
    ctx = AuthContext(user_id="svc", metadata={"token_profile": "partner"})
    with pytest.raises(ServiceAuthorizationError, match="internal"):
        await guard.check(ctx)
