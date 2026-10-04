"""
Tests for varco_fastapi middleware stack.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from varco_core.auth.api_key import hash_api_key
from varco_core.auth.base import AuthContext
from varco_core.exception.query import OperationNotSupported
from varco_core.exception.service import ServiceNotFoundError
from varco_fastapi.auth.server_auth import AnonymousAuth, ApiKeyAuth
from varco_fastapi.middleware.cors import CORSConfig, install_cors
from varco_fastapi.middleware.error import ErrorMiddleware
from varco_fastapi.middleware.request_context import RequestContextMiddleware

# ── ErrorMiddleware ────────────────────────────────────────────────────────────


def _make_app_with_error_middleware(**kwargs) -> FastAPI:
    app = FastAPI()
    app.add_middleware(ErrorMiddleware, **kwargs)
    return app


async def test_error_middleware_maps_service_not_found_to_404():
    """ErrorMiddleware maps ServiceNotFoundError to HTTP 404."""
    app = _make_app_with_error_middleware()

    @app.get("/missing")
    async def missing_route():
        raise ServiceNotFoundError(entity_id="abc", entity_cls=object)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/missing")

    assert resp.status_code == 404
    body = resp.json()
    assert "code" in body
    assert "message" in body


async def test_error_middleware_returns_500_for_unexpected_exception():
    """ErrorMiddleware returns 500 for unexpected exceptions."""
    app = _make_app_with_error_middleware()

    @app.get("/boom")
    async def boom():
        raise RuntimeError("unexpected!")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/boom")

    assert resp.status_code == 500
    body = resp.json()
    assert body["code"] == "INTERNAL_SERVER_ERROR"


async def test_error_middleware_includes_detail_in_debug_mode():
    """ErrorMiddleware includes exception detail when debug=True."""
    app = _make_app_with_error_middleware(debug=True)

    @app.get("/debug-boom")
    async def debug_boom():
        raise ValueError("specific error message")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/debug-boom")

    assert resp.status_code == 500
    body = resp.json()
    assert "specific error message" in body.get("detail", "")


async def test_error_middleware_maps_timeout_error_to_504():
    """ErrorMiddleware maps asyncio.TimeoutError to HTTP 504 with GATEWAY_TIMEOUT code."""
    app = _make_app_with_error_middleware()

    @app.get("/timeout")
    async def timeout_route():
        # Simulates a downstream await that exceeded its deadline (e.g. @timeout decorator)
        raise TimeoutError

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/timeout")

    assert resp.status_code == 504
    body = resp.json()
    assert body["code"] == "GATEWAY_TIMEOUT"
    assert "timed out" in body["message"].lower()


async def test_error_middleware_unwraps_exception_group_with_http_exception():
    """ErrorMiddleware unwraps BaseExceptionGroup containing an HTTPException.

    Starlette's BaseHTTPMiddleware wraps exceptions from call_next() in a
    BaseExceptionGroup under anyio on Python 3.11+. An HTTPException(401) raised
    inside another middleware arrives as BaseExceptionGroup("...", [HTTPException(401)])
    which must NOT fall through to the generic 500 handler.
    """
    from fastapi import HTTPException

    app = _make_app_with_error_middleware()

    @app.get("/auth-wrapped")
    async def auth_wrapped_route():
        # Simulate what happens when inner middleware raises HTTPException that
        # anyio wraps into a BaseExceptionGroup before it reaches this middleware.
        raise BaseExceptionGroup(
            "unhandled request errors",
            [HTTPException(status_code=401, detail="Unauthorized")],
        )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/auth-wrapped")

    assert resp.status_code == 401
    body = resp.json()
    assert body["detail"] == "Unauthorized"


async def test_error_middleware_unwraps_exception_group_with_service_exception():
    """ErrorMiddleware unwraps BaseExceptionGroup containing a ServiceException.

    Regression: a BaseExceptionGroup wrapping a ServiceNotFoundError must return
    404, not 500. Verifies that the group-unwrap path delegates to
    _service_error_response correctly.
    """
    from varco_core.exception.service import ServiceNotFoundError

    app = _make_app_with_error_middleware()

    @app.get("/wrapped-service-error")
    async def wrapped_service_error_route():
        raise BaseExceptionGroup(
            "unhandled request errors",
            [ServiceNotFoundError(entity_id="xyz", entity_cls=object)],
        )

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/wrapped-service-error")

    assert resp.status_code == 404
    body = resp.json()
    # Service error responses always include a stable code string
    assert "code" in body
    assert "message" in body


async def test_error_middleware_maps_query_exception_to_400():
    """ErrorMiddleware maps QueryException to HTTP 400 Bad Request."""
    app = _make_app_with_error_middleware()

    @app.get("/bad-query")
    async def bad_query():
        raise OperationNotSupported("nested traversal not supported")

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/bad-query")

    assert resp.status_code == 400
    body = resp.json()
    assert body["code"] == "QUERY_ERROR"
    assert "nested traversal" in body["message"]


# ── RequestContextMiddleware ────────────────────────────────────────────────────


async def test_request_context_middleware_sets_auth_context():
    """RequestContextMiddleware sets auth_context_var with the auth result."""
    key_ctx = AuthContext(user_id="svc_user", roles=frozenset({"service"}))
    api_key_auth = ApiKeyAuth(hashed_keys={hash_api_key("test-key"): key_ctx})

    app = FastAPI()
    app.add_middleware(RequestContextMiddleware, server_auth=api_key_auth)

    from varco_fastapi.context import get_auth_context

    @app.get("/me")
    async def me():
        ctx = get_auth_context()
        return {"user_id": ctx.user_id}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/me", headers={"X-API-Key": "test-key"})

    assert resp.status_code == 200
    assert resp.json()["user_id"] == "svc_user"


async def test_request_context_middleware_adds_request_id_header():
    """RequestContextMiddleware adds X-Request-ID header to the response."""
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware, server_auth=AnonymousAuth())

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/ping")

    assert "x-request-id" in resp.headers


async def test_request_context_middleware_propagates_request_id():
    """RequestContextMiddleware propagates X-Request-ID from the request."""
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware, server_auth=AnonymousAuth())

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/ping", headers={"X-Request-ID": "custom-id-123"})

    assert resp.headers["x-request-id"] == "custom-id-123"


# ── CORSConfig ─────────────────────────────────────────────────────────────────


def test_cors_config_default_values():
    """CORSConfig has sensible defaults — and as of 3.0.0 a *closed* origin list."""
    cfg = CORSConfig()
    # AB-5 (Plan 022): allow_origins defaulted to ("*",) until 3.0.0, which
    # combined with allow_credentials=True to reflect any origin. The default
    # is now closed; opting in is explicit. Full coverage in
    # varco_fastapi/tests/test_cors_secure_default.py.
    assert cfg.allow_origins == ()
    assert "GET" in cfg.allow_methods
    assert "Authorization" in cfg.allow_headers


def test_cors_config_from_env(monkeypatch):
    """CORSConfig.from_env() reads VARCO_CORS_* env vars."""
    monkeypatch.setenv("VARCO_CORS_ORIGINS", "https://app.example.com,http://localhost:3000")
    monkeypatch.setenv("VARCO_CORS_CREDENTIALS", "false")
    monkeypatch.setenv("VARCO_CORS_MAX_AGE", "300")

    cfg = CORSConfig.from_env()
    assert "https://app.example.com" in cfg.allow_origins
    assert "http://localhost:3000" in cfg.allow_origins
    assert cfg.allow_credentials is False
    assert cfg.max_age == 300


def test_cors_config_restrictive():
    """CORSConfig.restrictive() has empty allow_origins."""
    cfg = CORSConfig.restrictive()
    assert cfg.allow_origins == ()


async def test_install_cors_adds_cors_headers():
    """install_cors() adds CORS headers to responses."""
    app = FastAPI()
    install_cors(app, CORSConfig(allow_origins=["http://test.com"]))

    @app.get("/data")
    async def data():
        return {"ok": True}

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.options(
            "/data",
            headers={
                "Origin": "http://test.com",
                "Access-Control-Request-Method": "GET",
            },
        )

    # CORS headers should be present
    assert "access-control-allow-origin" in resp.headers


# ── TrustStore ──────────────────────────────────────────────────────────────────


def test_trust_store_default_builds_ssl_context():
    """TrustStore() default builds a system-CA SSL context."""
    from varco_fastapi.auth.trust_store import TrustStore

    ts = TrustStore()
    ctx = ts.build_ssl_context()
    import ssl

    assert isinstance(ctx, ssl.SSLContext)


def test_trust_store_raises_on_partial_mtls():
    """TrustStore raises ValueError when only one of client_cert/key is set."""
    from pathlib import Path

    from varco_fastapi.auth.trust_store import TrustStore

    ts = TrustStore(client_cert=Path("/etc/ssl/client.crt"))  # no client_key
    with pytest.raises(ValueError, match="must both be set or both be None"):
        ts.build_ssl_context()
