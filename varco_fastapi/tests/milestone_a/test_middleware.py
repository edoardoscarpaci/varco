"""
Tests for varco_fastapi middleware stack.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from varco_core.auth.base import AuthContext
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

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
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

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
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

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/debug-boom")

    assert resp.status_code == 500
    body = resp.json()
    assert "specific error message" in body.get("detail", "")


# ── RequestContextMiddleware ────────────────────────────────────────────────────


async def test_request_context_middleware_sets_auth_context():
    """RequestContextMiddleware sets auth_context_var with the auth result."""
    key_ctx = AuthContext(user_id="svc_user", roles=frozenset({"service"}))
    api_key_auth = ApiKeyAuth(keys={"test-key": key_ctx})

    app = FastAPI()
    app.add_middleware(RequestContextMiddleware, server_auth=api_key_auth)

    from varco_fastapi.context import get_auth_context

    @app.get("/me")
    async def me():
        ctx = get_auth_context()
        return {"user_id": ctx.user_id}

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
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

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/ping")

    assert "x-request-id" in resp.headers


async def test_request_context_middleware_propagates_request_id():
    """RequestContextMiddleware propagates X-Request-ID from the request."""
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware, server_auth=AnonymousAuth())

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/ping", headers={"X-Request-ID": "custom-id-123"})

    assert resp.headers["x-request-id"] == "custom-id-123"


# ── CORSConfig ─────────────────────────────────────────────────────────────────


def test_cors_config_default_values():
    """CORSConfig has sensible defaults."""
    cfg = CORSConfig()
    assert "*" in cfg.allow_origins
    assert "GET" in cfg.allow_methods
    assert "Authorization" in cfg.allow_headers


def test_cors_config_from_env(monkeypatch):
    """CORSConfig.from_env() reads VARCO_CORS_* env vars."""
    monkeypatch.setenv(
        "VARCO_CORS_ORIGINS", "https://app.example.com,http://localhost:3000"
    )
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

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
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
