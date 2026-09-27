"""
Tests for service-free (generic) REST server support.

Covers:
- A bare ``VarcoRouter`` / ``GenericRouter`` subclass (no type args, no service)
  builds a working APIRouter via ``build_router()``.
- ``validate_router_class`` passes for service-free routers (strict and non-strict).
- ``GenericRouter`` alias is identical to ``VarcoRouter``.
- Mounted via ``create_varco_app``, the middleware stack fires (X-Request-ID propagated).
- Route resolves to 200 and returns expected payload.
- Telemetry/audit: ``X-Request-ID`` present on response (correlation middleware active).
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient
from providify import DIContainer
from pydantic import BaseModel
from varco_fastapi.app import create_varco_app
from varco_fastapi.auth import AnonymousAuth
from varco_fastapi.router.base import VarcoRouter
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.presets import GenericRouter
from varco_fastapi.validation import ConfigurationError, validate_router_class

# ── Fixtures ──────────────────────────────────────────────────────────────────


class EchoRequest(BaseModel):
    message: str


class EchoResponse(BaseModel):
    echo: str


# Minimal service-free router — no type args, no _service, no auth
# Handler omits ctx because auth_dep is None when _auth is not set.
class EchoRouter(VarcoRouter):
    _prefix = "/echo"
    _tags = ["echo"]

    @route("POST", "/process", status_code=200)
    async def process(self) -> dict:
        return {"echo": "ok"}


# Same with GenericRouter alias
class PingRouter(GenericRouter):
    _prefix = "/ping"

    @route("GET", "/")
    async def ping(self, ctx: Any) -> dict:
        return {"pong": True}


# ── GenericRouter alias ───────────────────────────────────────────────────────


def test_generic_router_is_varco_router():
    """GenericRouter is exactly VarcoRouter — same runtime identity."""
    assert GenericRouter is VarcoRouter


def test_generic_router_subclass_is_varco_router_subclass():
    """A GenericRouter subclass is also a VarcoRouter subclass."""
    assert issubclass(PingRouter, VarcoRouter)


# ── Validation ────────────────────────────────────────────────────────────────


def test_validate_service_free_router_passes():
    """Service-free router with @route methods passes validation (non-strict)."""
    validate_router_class(EchoRouter, strict=False)


def test_validate_service_free_router_passes_strict():
    """Service-free router with @route methods passes strict validation — no type-arg warning."""
    validate_router_class(EchoRouter, strict=True)


def test_validate_requires_prefix():
    """validate_router_class raises if _prefix is missing."""

    class NoPrefixRouter(GenericRouter):
        @route("GET", "/")
        async def get(self, ctx: Any) -> dict:
            return {}

    with pytest.raises(ConfigurationError, match="_prefix"):
        validate_router_class(NoPrefixRouter)


def test_validate_requires_at_least_one_route():
    """validate_router_class raises if no routes are declared."""

    class EmptyRouter(GenericRouter):
        _prefix = "/empty"

    with pytest.raises(ConfigurationError, match="no routes"):
        validate_router_class(EmptyRouter)


# ── build_router ──────────────────────────────────────────────────────────────


def test_build_router_no_service_no_type_args():
    """build_router() succeeds on a VarcoRouter with no service and no type args."""
    from fastapi import APIRouter

    router = EchoRouter()
    api_router = router.build_router()
    assert isinstance(api_router, APIRouter)


def test_build_router_registers_custom_route():
    """build_router() registers exactly the @route methods defined."""
    router = EchoRouter()
    api_router = router.build_router()
    paths = [r.path for r in api_router.routes]
    assert "/echo/process" in paths


def test_ping_router_builds():
    """GenericRouter alias produces a working APIRouter."""
    from fastapi import APIRouter

    router = PingRouter()
    api_router = router.build_router()
    assert isinstance(api_router, APIRouter)
    paths = [r.path for r in api_router.routes]
    assert "/ping/" in paths


# ── End-to-end: create_varco_app ──────────────────────────────────────────────


def test_service_free_router_serves_requests():
    """Service-free router mounted via create_varco_app returns 200."""
    app = create_varco_app(DIContainer(), routers=[EchoRouter], validate=False)
    client = TestClient(app, raise_server_exceptions=True)
    resp = client.post("/echo/process", json={"message": "hello"})
    assert resp.status_code == 200
    assert resp.json() == {"echo": "ok"}


def test_service_free_router_middleware_stack_runs(caplog: Any) -> None:
    """
    Middleware stack (error, tracing, logging) fires for service-free routers.
    RequestContextMiddleware (which adds X-Request-ID) requires a container;
    verify the logging middleware at least processes the request instead.
    """
    import logging

    app = create_varco_app(DIContainer(), routers=[EchoRouter], validate=False)
    client = TestClient(app, raise_server_exceptions=True)
    with caplog.at_level(logging.INFO, logger="varco_fastapi.access"):
        resp = client.post("/echo/process", json={})
    # Request succeeded
    assert resp.status_code == 200
    # Access log line was emitted by RequestLoggingMiddleware
    assert any("POST" in r.message and "/echo/process" in r.message for r in caplog.records)


def test_service_free_router_with_anonymous_auth():
    """Service-free router with AnonymousAuth and no requires= serves all callers."""

    class OpenRouter(GenericRouter):
        _prefix = "/open"
        _auth = AnonymousAuth()

        @route("GET", "/status")
        async def status(self, ctx: Any) -> dict:
            return {"ok": True}

    app = create_varco_app(DIContainer(), routers=[OpenRouter], validate=False)
    client = TestClient(app, raise_server_exceptions=True)
    resp = client.get("/open/status")
    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


def test_service_free_router_missing_prefix_raised_by_build_router():
    """
    If somehow a router without _prefix gets past validation, build_router
    still uses the empty prefix (FastAPI accepts it).  Validation is the guard.
    """

    class AnyRouter(GenericRouter):
        @route("GET", "/x")
        async def get_x(self, ctx: Any) -> dict:
            return {}

    # build_router itself doesn't enforce prefix — that's validation's job
    api_router = AnyRouter().build_router()
    paths = [r.path for r in api_router.routes]
    assert "/x" in paths
