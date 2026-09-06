"""
Red-mode tests for Plan 035 / Phase 5, Step 21 (S10) — ``RateLimitMiddleware``.

Unit tests only — uses ``InMemoryRateLimiter`` (with
``acknowledge_unbounded_keyspace=True`` where required by §D-S10-keyspace)
and a fake limiter that raises for the fail-open/fail-closed cases.
``varco_redis`` is never imported here (that is Step 26's integration test).

``varco_fastapi.middleware.rate_limit`` does not exist yet — every test
below must fail with ``ModuleNotFoundError``, not a fixture typo.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient
from varco_core.auth.base import AuthContext
from varco_core.resilience.rate_limit import InMemoryRateLimiter, RateLimitConfig
from varco_core.service.tenant import tenant_context
from varco_fastapi.middleware.error import ErrorMiddleware
from varco_fastapi.middleware.rate_limit import (  # type: ignore[attr-defined]
    RateLimitBundle,
    RateLimitMiddleware,
    RateLimitRule,
    RateLimitScope,
    RateLimitSettings,
    RateLimitStage,
)
from varco_fastapi.middleware.request_context import RequestContextMiddleware


def _limiter(rate: int = 1, period: float = 60.0) -> InMemoryRateLimiter:
    return InMemoryRateLimiter(RateLimitConfig(rate=rate, period=period))


class _FailingLimiter(InMemoryRateLimiter):
    """Raises on every acquire() — stands in for a RedisError."""

    def __init__(self, exc: Exception) -> None:
        super().__init__(RateLimitConfig(rate=1, period=60.0))
        self._exc = exc

    async def acquire(self, key: str = "default") -> bool:  # type: ignore[override]
        raise self._exc


class _StubServerAuth:
    def __init__(self, user_id: str | None) -> None:
        self._user_id = user_id

    async def __call__(self, request: Request) -> AuthContext:
        # AbstractServerAuth.__call__ contract: always an AuthContext, never
        # None — RequestContextMiddleware.dispatch reads ctx.metadata
        # unconditionally. Anonymous is AuthContext(user_id=None, ...), the
        # same shape AnonymousAuth() (the RequestContextMiddleware default)
        # returns.
        return AuthContext(user_id=self._user_id, roles=frozenset())


# ── Construction ─────────────────────────────────────────────────────────────


def test_subject_rule_in_pre_auth_stage_raises_value_error_naming_scope() -> None:
    rule = RateLimitRule(scope=RateLimitScope.SUBJECT, limiter=_limiter(), name="s")
    with pytest.raises(ValueError) as exc:
        RateLimitMiddleware(
            lambda scope, receive, send: None,
            rules=(rule,),
            stage=RateLimitStage.PRE_AUTH,
        )
    assert "SUBJECT" in str(exc.value)


def test_tenant_rule_in_pre_auth_stage_raises_value_error_naming_scope() -> None:
    rule = RateLimitRule(scope=RateLimitScope.TENANT, limiter=_limiter(), name="t")
    with pytest.raises(ValueError) as exc:
        RateLimitMiddleware(
            lambda scope, receive, send: None,
            rules=(rule,),
            stage=RateLimitStage.PRE_AUTH,
        )
    assert "TENANT" in str(exc.value)


def test_ip_rule_with_in_memory_limiter_and_no_acknowledgement_raises_value_error() -> None:
    rule = RateLimitRule(scope=RateLimitScope.IP, limiter=_limiter(), name="ip")
    with pytest.raises(ValueError) as exc:
        RateLimitMiddleware(
            lambda scope, receive, send: None,
            rules=(rule,),
            stage=RateLimitStage.PRE_AUTH,
        )
    assert "acknowledge_unbounded_keyspace" in str(exc.value)


def test_ip_rule_with_in_memory_limiter_and_acknowledgement_does_not_raise() -> None:
    rule = RateLimitRule(scope=RateLimitScope.IP, limiter=_limiter(), name="ip")
    RateLimitMiddleware(
        lambda scope, receive, send: None,
        rules=(rule,),
        stage=RateLimitStage.PRE_AUTH,
        acknowledge_unbounded_keyspace=True,
    )


def test_subject_rule_with_in_memory_limiter_and_no_acknowledgement_raises() -> None:
    rule = RateLimitRule(scope=RateLimitScope.SUBJECT, limiter=_limiter(), name="s")
    with pytest.raises(ValueError) as exc:
        RateLimitMiddleware(
            lambda scope, receive, send: None,
            rules=(rule,),
            stage=RateLimitStage.POST_AUTH,
        )
    assert "acknowledge_unbounded_keyspace" in str(exc.value)


def test_ip_rule_with_redis_rate_limiter_does_not_raise_without_acknowledgement() -> None:
    from varco_redis.rate_limit import RedisRateLimiter

    redis_limiter = RedisRateLimiter(RateLimitConfig(rate=1, period=60.0))
    rule = RateLimitRule(scope=RateLimitScope.IP, limiter=redis_limiter, name="ip")
    RateLimitMiddleware(
        lambda scope, receive, send: None,
        rules=(rule,),
        stage=RateLimitStage.PRE_AUTH,
    )


# ── Keying ───────────────────────────────────────────────────────────────────


def _make_app_post_auth(rule: RateLimitRule, *, user_id: str | None = "u1") -> FastAPI:
    app = FastAPI()
    # §D-order: RateLimitMiddleware(POST_AUTH) sits INSIDE RequestContextMiddleware
    # (position 11 vs. 10) so its SUBJECT/TENANT keying can read the AuthContext
    # RequestContextMiddleware just populated. Starlette's add_middleware()
    # prepends (last call = outermost) — so RequestContextMiddleware must be
    # added LAST for it to end up outermost of the two.
    app.add_middleware(
        RateLimitMiddleware,
        rules=(rule,),
        stage=RateLimitStage.POST_AUTH,
        acknowledge_unbounded_keyspace=True,
    )
    app.add_middleware(RequestContextMiddleware, server_auth=_StubServerAuth(user_id))
    app.add_middleware(ErrorMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    return app


async def _get(app: FastAPI, path: str = "/ok", **kwargs: object):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path, **kwargs)


async def test_tenant_scope_keys_on_current_tenant_two_tenants_independent() -> None:
    rule = RateLimitRule(scope=RateLimitScope.TENANT, limiter=_limiter(rate=1), name="t")
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        rules=(rule,),
        stage=RateLimitStage.POST_AUTH,
        acknowledge_unbounded_keyspace=True,
    )
    app.add_middleware(ErrorMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        with tenant_context("tenant-a"):
            r1 = await client.get("/ok")
        with tenant_context("tenant-b"):
            r2 = await client.get("/ok")
    assert r1.status_code == 200
    assert r2.status_code == 200


async def test_subject_scope_keys_on_auth_context_user_id() -> None:
    rule = RateLimitRule(scope=RateLimitScope.SUBJECT, limiter=_limiter(rate=1), name="s")
    app = _make_app_post_auth(rule, user_id="u1")
    r1 = await _get(app)
    r2 = await _get(app)
    assert r1.status_code == 200
    assert r2.status_code == 429


async def test_anonymous_request_skips_subject_rule_rather_than_sharing_bucket() -> None:
    rule = RateLimitRule(scope=RateLimitScope.SUBJECT, limiter=_limiter(rate=1), name="s")
    app = _make_app_post_auth(rule, user_id=None)
    r1 = await _get(app)
    r2 = await _get(app)
    # No user_id -> the rule is skipped for every anonymous request, never 429.
    assert r1.status_code == 200
    assert r2.status_code == 200


# ── IP ───────────────────────────────────────────────────────────────────────


async def test_x_forwarded_for_ignored_with_no_trusted_proxy() -> None:
    rule = RateLimitRule(scope=RateLimitScope.IP, limiter=_limiter(rate=1), name="ip")
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        rules=(rule,),
        stage=RateLimitStage.PRE_AUTH,
        acknowledge_unbounded_keyspace=True,
    )
    app.add_middleware(ErrorMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    r1 = await _get(app, headers={"X-Forwarded-For": "1.1.1.1"})
    r2 = await _get(app, headers={"X-Forwarded-For": "2.2.2.2"})
    # Both requests come from the same test-client peer regardless of the
    # spoofed header — the second must be rate-limited.
    assert r1.status_code == 200
    assert r2.status_code == 429


async def test_x_forwarded_for_honoured_when_peer_is_trusted_proxy_at_hop_count() -> None:
    rule = RateLimitRule(scope=RateLimitScope.IP, limiter=_limiter(rate=1), name="ip")
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        rules=(rule,),
        stage=RateLimitStage.PRE_AUTH,
        settings=RateLimitSettings(trusted_proxies=("127.0.0.1/32",), trusted_proxy_hops=1),
        acknowledge_unbounded_keyspace=True,
    )
    app.add_middleware(ErrorMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    r1 = await _get(app, headers={"X-Forwarded-For": "1.1.1.1"})
    r2 = await _get(app, headers={"X-Forwarded-For": "2.2.2.2"})
    # Different spoofed-but-trusted client IPs -> independent budgets.
    assert r1.status_code == 200
    assert r2.status_code == 200


# ── Response shape ───────────────────────────────────────────────────────────


async def test_429_has_retry_after_integer_and_envelope_body_with_correlation_id() -> None:
    rule = RateLimitRule(scope=RateLimitScope.GLOBAL, limiter=_limiter(rate=1), name="g")
    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, rules=(rule,), stage=RateLimitStage.PRE_AUTH)
    app.add_middleware(ErrorMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    await _get(app)
    response = await _get(app)
    assert response.status_code == 429
    assert "Retry-After" in response.headers
    assert int(response.headers["Retry-After"]) >= 1
    body = response.json()
    assert "correlation_id" in body


async def test_security_headers_present_on_429() -> None:
    from varco_fastapi.middleware.security_headers import (  # type: ignore[attr-defined]
        SecurityHeadersMiddleware,
    )

    rule = RateLimitRule(scope=RateLimitScope.GLOBAL, limiter=_limiter(rate=1), name="g")
    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, rules=(rule,), stage=RateLimitStage.PRE_AUTH)
    app.add_middleware(ErrorMiddleware)
    app.add_middleware(SecurityHeadersMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    await _get(app)
    response = await _get(app)
    assert response.status_code == 429
    assert "X-Content-Type-Options" in response.headers


async def test_rate_limit_policy_header_absent_by_default() -> None:
    rule = RateLimitRule(scope=RateLimitScope.GLOBAL, limiter=_limiter(rate=1), name="g")
    app = FastAPI()
    app.add_middleware(RateLimitMiddleware, rules=(rule,), stage=RateLimitStage.PRE_AUTH)
    app.add_middleware(ErrorMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    await _get(app)
    response = await _get(app)
    assert "RateLimit-Policy" not in response.headers


async def test_rate_limit_policy_header_present_with_emit_draft_headers_true() -> None:
    rule = RateLimitRule(scope=RateLimitScope.GLOBAL, limiter=_limiter(rate=1), name="g")
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        rules=(rule,),
        stage=RateLimitStage.PRE_AUTH,
        settings=RateLimitSettings(emit_draft_headers=True),
    )
    app.add_middleware(ErrorMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    await _get(app)
    response = await _get(app)
    assert "RateLimit-Policy" in response.headers


async def test_no_rate_limit_or_x_rate_limit_headers_ever_emitted() -> None:
    # §D-S10-headers: the ABC cannot report remaining quota — a lying header
    # must never appear, regardless of emit_draft_headers.
    rule = RateLimitRule(scope=RateLimitScope.GLOBAL, limiter=_limiter(rate=1), name="g")
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        rules=(rule,),
        stage=RateLimitStage.PRE_AUTH,
        settings=RateLimitSettings(emit_draft_headers=True),
    )
    app.add_middleware(ErrorMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    await _get(app)
    response = await _get(app)
    for name in response.headers:
        assert name != "RateLimit"
        assert not name.startswith("X-RateLimit-")


# ── Failure handling ─────────────────────────────────────────────────────────


async def test_limiter_raising_allows_request_and_logs_error_when_fail_open(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    caplog.set_level(logging.ERROR)

    class _RedisErrorLike(Exception):
        pass

    rule = RateLimitRule(
        scope=RateLimitScope.GLOBAL, limiter=_FailingLimiter(_RedisErrorLike("down")), name="g"
    )
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        rules=(rule,),
        stage=RateLimitStage.PRE_AUTH,
        settings=RateLimitSettings(fail_open=True),
    )
    app.add_middleware(ErrorMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    response = await _get(app)
    assert response.status_code == 200
    assert any(r.levelno >= logging.ERROR for r in caplog.records)


async def test_limiter_raising_returns_503_when_fail_open_false() -> None:
    class _RedisErrorLike(Exception):
        pass

    rule = RateLimitRule(
        scope=RateLimitScope.GLOBAL, limiter=_FailingLimiter(_RedisErrorLike("down")), name="g"
    )
    app = FastAPI()
    app.add_middleware(
        RateLimitMiddleware,
        rules=(rule,),
        stage=RateLimitStage.PRE_AUTH,
        settings=RateLimitSettings(fail_open=False),
    )
    app.add_middleware(ErrorMiddleware)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    response = await _get(app)
    assert response.status_code == 503


# ── Inertness ────────────────────────────────────────────────────────────────


async def test_no_rules_is_a_pass_through_byte_identical() -> None:
    app_with_middleware = FastAPI()
    app_with_middleware.add_middleware(RateLimitMiddleware, rules=(), stage=RateLimitStage.PRE_AUTH)
    app_with_middleware.add_middleware(ErrorMiddleware)

    @app_with_middleware.get("/ok")
    async def ok():
        return {"ok": True}

    baseline_app = FastAPI()
    baseline_app.add_middleware(ErrorMiddleware)

    @baseline_app.get("/ok")
    async def ok2():
        return {"ok": True}

    r1 = await _get(app_with_middleware)
    r2 = await _get(baseline_app)
    assert r1.status_code == r2.status_code == 200
    assert r1.json() == r2.json()


# ── §D-S10-keyspace via create_varco_app(rate_limit=RateLimitBundle(...)) ───
# Drift 1 regression: create_varco_app must never forge acknowledge_unbounded_
# keyspace=True on the caller's behalf — the guard must apply uniformly to
# the documented, recommended API, not just a hand-registered instance.


def _build_bundle_app(bundle: RateLimitBundle) -> FastAPI:
    from providify import DIContainer
    from varco_fastapi.app import create_varco_app

    return create_varco_app(
        container=DIContainer(),
        routers=[],
        validate=False,
        rate_limit=bundle,
    )


async def _first_request(app: FastAPI) -> None:
    # Starlette's add_middleware() only stores a deferred Middleware(cls,
    # **kwargs) descriptor (`.venv/.../starlette/applications.py`) — the
    # RateLimitMiddleware instance, and therefore its §D-S10-keyspace guard,
    # is only actually constructed on the first ASGI call (build_middleware_
    # stack()), not at create_varco_app()/add_middleware() call time.
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.get("/__nonexistent__")


async def test_create_varco_app_ip_rule_in_memory_limiter_no_ack_raises_value_error() -> None:

    rule = RateLimitRule(scope=RateLimitScope.IP, limiter=_limiter(), name="ip")
    bundle = RateLimitBundle(rules=(rule,))
    app = _build_bundle_app(bundle)
    with pytest.raises(ValueError) as exc:
        await _first_request(app)
    assert "acknowledge_unbounded_keyspace" in str(exc.value)


async def test_create_varco_app_ip_rule_in_memory_limiter_with_ack_constructs_fine() -> None:

    rule = RateLimitRule(scope=RateLimitScope.IP, limiter=_limiter(), name="ip")
    bundle = RateLimitBundle(rules=(rule,), acknowledge_unbounded_keyspace=True)
    app = _build_bundle_app(bundle)
    await _first_request(app)  # must not raise


async def test_create_varco_app_ip_rule_redis_style_limiter_needs_no_acknowledgement() -> None:
    from varco_redis.rate_limit import RedisRateLimiter

    redis_limiter = RedisRateLimiter(RateLimitConfig(rate=1, period=60.0))
    rule = RateLimitRule(scope=RateLimitScope.IP, limiter=redis_limiter, name="ip")
    bundle = RateLimitBundle(rules=(rule,))  # acknowledge_unbounded_keyspace default False
    app = _build_bundle_app(bundle)
    await _first_request(app)  # must not raise — RedisRateLimiter is exempt
