"""
Tests for RouteGuard — declarative per-route authorization.

Covers:
- RouteGuard.check: scopes (AND/OR), roles (AND/OR), grant (ctx.can),
  allow_anonymous, sync predicate, async predicate.
- Fail-closed: anonymous AuthContext against a non-anonymous guard →
  ServiceAuthorizationError.
- Constructor helpers: require_scopes, require_roles, require_grant,
  require_predicate, allow_anonymous.
- ResolvedRoute round-trip: requires field propagates _RouteEntry → ResolvedRoute.
- End-to-end: guarded @route returns 403 for under-privileged callers,
  200 for privileged callers.
- Build-time safety: requires + no _auth (and not allow_anonymous) → RuntimeError.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient
from providify import DIContainer
from varco_core.auth.api_key import hash_api_key
from varco_core.auth.base import Action, AuthContext, ResourceGrant
from varco_core.exception.service import ServiceAuthorizationError
from varco_fastapi.app import create_varco_app
from varco_fastapi.auth import ApiKeyAuth
from varco_fastapi.auth.guard import (
    RouteGuard,
    allow_anonymous,
    require_grant,
    require_predicate,
    require_roles,
    require_scopes,
)
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.introspection import introspect_routes
from varco_fastapi.router.presets import GenericRouter

# ── AuthContext fixtures ──────────────────────────────────────────────────────


def _ctx(
    *,
    user_id: str = "usr_1",
    roles: frozenset[str] = frozenset(),
    scopes: frozenset[str] = frozenset(),
    grants: tuple[ResourceGrant, ...] = (),
) -> AuthContext:
    return AuthContext(user_id=user_id, roles=roles, scopes=scopes, grants=grants)


ANON = AuthContext()  # anonymous (user_id=None)
ADMIN = _ctx(
    user_id="admin_1",
    roles=frozenset({"admin"}),
    scopes=frozenset({"admin:read", "admin:write"}),
)
READER = _ctx(user_id="reader_1", roles=frozenset({"reader"}), scopes=frozenset({"reports:read"}))
GRANTED = _ctx(
    user_id="usr_granted",
    grants=(ResourceGrant(resource="reports", actions=frozenset({Action.READ})),),
)


# ── RouteGuard.check — unit tests ─────────────────────────────────────────────


async def test_empty_guard_allows_authenticated():
    """Empty guard (no scopes/roles/grant) allows any authenticated caller."""
    guard = RouteGuard()
    await guard.check(READER)  # must not raise


async def test_empty_guard_denies_anonymous():
    """Empty guard (allow_anonymous=False) denies anonymous callers."""
    guard = RouteGuard()
    with pytest.raises(ServiceAuthorizationError):
        await guard.check(ANON)


async def test_require_scopes_and_pass():
    """require_scopes (AND) passes when all scopes present."""
    guard = require_scopes("admin:read", "admin:write")
    await guard.check(ADMIN)  # has both


async def test_require_scopes_and_fail():
    """require_scopes (AND) denies when any scope is missing."""
    guard = require_scopes("admin:read", "admin:write")
    with pytest.raises(ServiceAuthorizationError, match="scope"):
        await guard.check(READER)  # only has reports:read


async def test_require_scopes_or_pass():
    """require_scopes (OR, all=False) passes when any one scope is present."""
    guard = require_scopes("admin:read", "reports:read", all=False)
    await guard.check(READER)  # has reports:read


async def test_require_scopes_or_fail():
    """require_scopes (OR) denies when no scope matches."""
    guard = require_scopes("admin:read", "admin:write", all=False)
    with pytest.raises(ServiceAuthorizationError, match="scope"):
        await guard.check(READER)


async def test_require_roles_and_pass():
    """require_roles (AND) passes when all roles present."""
    guard = require_roles("admin")
    await guard.check(ADMIN)


async def test_require_roles_and_fail():
    """require_roles (AND) denies when role is missing."""
    guard = require_roles("admin")
    with pytest.raises(ServiceAuthorizationError, match="role"):
        await guard.check(READER)


async def test_require_roles_or_pass():
    """require_roles (OR) passes when any role matches."""
    guard = require_roles("admin", "reader", all=False)
    await guard.check(READER)  # has "reader"


async def test_require_grant_pass():
    """require_grant passes when ctx.can() returns True."""
    guard = require_grant(Action.READ, "reports")
    await guard.check(GRANTED)


async def test_require_grant_fail():
    """require_grant denies when ctx.can() returns False."""
    guard = require_grant(Action.DELETE, "reports")
    with pytest.raises(ServiceAuthorizationError, match="grant"):
        await guard.check(GRANTED)  # only has READ


async def test_allow_anonymous_passes_anon():
    """allow_anonymous guard lets anonymous callers through."""
    guard = allow_anonymous()
    await guard.check(ANON)  # must not raise


async def test_allow_anonymous_passes_authenticated():
    """allow_anonymous guard also passes authenticated callers."""
    guard = allow_anonymous()
    await guard.check(ADMIN)


async def test_sync_predicate_allow():
    """Sync predicate returning True allows the caller."""
    guard = require_predicate(lambda ctx: ctx.has_role("admin"))
    await guard.check(ADMIN)


async def test_sync_predicate_deny():
    """Sync predicate returning False denies with generic message."""
    guard = require_predicate(lambda ctx: ctx.has_role("superuser"))
    with pytest.raises(ServiceAuthorizationError):
        await guard.check(ADMIN)


async def test_async_predicate_allow():
    """Async predicate returning True allows the caller."""

    async def _check(ctx: AuthContext) -> bool:
        return ctx.has_scope("admin:read")

    guard = require_predicate(_check)
    await guard.check(ADMIN)


async def test_async_predicate_deny():
    """Async predicate returning False denies."""

    async def _check(ctx: AuthContext) -> bool:
        return False

    guard = require_predicate(_check)
    with pytest.raises(ServiceAuthorizationError):
        await guard.check(ADMIN)


async def test_predicate_raising_authz_error_propagates():
    """Predicate that raises ServiceAuthorizationError propagates it directly."""

    def _check(ctx: AuthContext) -> bool:
        raise ServiceAuthorizationError("custom denial", str)

    guard = require_predicate(_check)
    with pytest.raises(ServiceAuthorizationError, match="custom denial"):
        await guard.check(ADMIN)


async def test_guard_is_frozen():
    """RouteGuard is a frozen dataclass — mutation raises."""
    guard = require_scopes("foo")
    with pytest.raises((AttributeError, TypeError)):
        guard.scopes = ("bar",)  # type: ignore[misc]


# ── ResolvedRoute round-trip ──────────────────────────────────────────────────


def test_requires_round_trips_through_resolved_route():
    """requires= on @route propagates through _RouteEntry → ResolvedRoute."""
    guard = require_scopes("reports:read")

    class ReportRouter(GenericRouter):
        _prefix = "/reports"

        @route("GET", "/summary", requires=guard)
        async def get_summary(self, ctx: Any) -> dict:
            return {}

    routes = introspect_routes(ReportRouter)
    summary_route = next(r for r in routes if r.name == "get_summary")
    assert summary_route.requires is guard


def test_requires_none_when_not_set():
    """requires defaults to None when not specified on @route."""

    class OpenRouter(GenericRouter):
        _prefix = "/open"

        @route("GET", "/")
        async def index(self, ctx: Any) -> dict:
            return {}

    routes = introspect_routes(OpenRouter)
    assert routes[0].requires is None


# ── Build-time safety check ───────────────────────────────────────────────────


def test_build_router_raises_if_requires_without_auth():
    """build_router raises RuntimeError if requires is set but _auth is None."""

    class GuardedRouter(GenericRouter):
        _prefix = "/guarded"
        # _auth NOT set

        @route("GET", "/secret", requires=require_scopes("secret:read"))
        async def secret(self, ctx: Any) -> dict:
            return {}

    with pytest.raises(RuntimeError, match="_auth"):
        GuardedRouter().build_router()


def test_build_router_allows_requires_with_allow_anonymous_no_auth():
    """allow_anonymous() guard is OK without _auth — no auth check needed."""
    from fastapi import APIRouter

    class AnonRouter(GenericRouter):
        _prefix = "/anon"

        @route("GET", "/public", requires=allow_anonymous())
        async def public(self, ctx: Any) -> dict:
            return {}

    api_router = AnonRouter().build_router()
    assert isinstance(api_router, APIRouter)


def test_build_router_allows_requires_with_auth():
    """requires= with _auth set builds successfully."""
    from fastapi import APIRouter

    ctx = AuthContext(user_id="svc", roles=frozenset({"svc"}))

    class AuthedRouter(GenericRouter):
        _prefix = "/authed"
        _auth = ApiKeyAuth(hashed_keys={hash_api_key("k"): ctx})

        @route("GET", "/data", requires=require_scopes("data:read"))
        async def get_data(self, ctx_: Any) -> dict:
            return {}

    api_router = AuthedRouter().build_router()
    assert isinstance(api_router, APIRouter)


# ── End-to-end: guarded route returns 403 / 200 ───────────────────────────────


def _make_guarded_app() -> tuple[Any, str]:
    """
    Build a test app with two routes:
    - GET /reports/public  — no guard (anonymous OK via AnonymousAuth)
    - GET /reports/secret  — requires scope "reports:read"

    Returns (TestClient, api_key_with_scope).
    """
    admin_ctx = AuthContext(
        user_id="adm",
        scopes=frozenset({"reports:read"}),
    )
    unpriv_ctx = AuthContext(user_id="unpriv", scopes=frozenset())

    auth = ApiKeyAuth(
        hashed_keys={
            hash_api_key("admin-key"): admin_ctx,
            hash_api_key("unpriv-key"): unpriv_ctx,
        },
        required=False,  # fall through to anon if no key
    )

    class ReportRouter(GenericRouter):
        _prefix = "/reports"
        _auth = auth

        @route("GET", "/public")
        async def public_info(self, ctx: Any) -> dict:
            return {"public": True}

        @route("GET", "/secret", requires=require_scopes("reports:read"))
        async def secret_data(self, ctx: Any) -> dict:
            return {"secret": True}

    app = create_varco_app(DIContainer(), routers=[ReportRouter], validate=False)
    return app


def test_guarded_route_allows_privileged_caller():
    """Caller with required scope receives 200."""
    app = _make_guarded_app()
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/reports/secret", headers={"X-API-Key": "admin-key"})
    assert resp.status_code == 200
    assert resp.json() == {"secret": True}


def test_guarded_route_denies_unprivileged_caller():
    """Caller without required scope receives 403."""
    app = _make_guarded_app()
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/reports/secret", headers={"X-API-Key": "unpriv-key"})
    assert resp.status_code == 403


def test_guarded_route_denies_anonymous_caller():
    """Anonymous caller (no key) receives 403 on guarded route."""
    app = _make_guarded_app()
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/reports/secret")  # no API key
    assert resp.status_code == 403


def test_open_route_allows_any_caller():
    """Route without requires= allows all callers including anonymous."""
    app = _make_guarded_app()
    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/reports/public")
    assert resp.status_code == 200
