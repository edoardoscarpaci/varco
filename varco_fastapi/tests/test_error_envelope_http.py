"""
Plan 054 / Phase C, Step 10 — FLIP-10 over HTTP, on both error paths
(§D-flip10, §D-flip10-parity).

* Route-handler path (``add_exception_handlers``): no ``detail`` by default,
  structured content in ``params``; ``VARCO_ERROR_INCLUDE_DETAIL=true``
  restores it.
* Middleware path (``ErrorMiddleware._service_error_response``): gains the
  handler path's ``message_key``/``params`` keys (parity) and also drops
  ``detail`` by default.
* Controls that must pass before and after: ``HTTPException`` bodies and the
  ``debug=True`` 500 body are not the envelope and never change.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware
from varco_core.exception.service import ServiceValidationError
from varco_fastapi.exceptions import add_exception_handlers
from varco_fastapi.middleware.error import ErrorMiddleware


@pytest.fixture(autouse=True)
def _no_detail_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    monkeypatch.delenv("VARCO_ERROR_INCLUDE_DETAIL", raising=False)
    return monkeypatch


def _handler_app(exc: Exception) -> FastAPI:
    app = FastAPI()
    add_exception_handlers(app)

    @app.get("/boom")
    async def boom() -> None:
        raise exc

    return app


class _RaiseInMiddleware(BaseHTTPMiddleware):
    """Raises the configured exception from inside the middleware stack, so it
    reaches ``ErrorMiddleware`` rather than a route exception handler."""

    def __init__(self, app, exc: Exception) -> None:  # noqa: ANN001
        super().__init__(app)
        self._exc = exc

    async def dispatch(self, request, call_next):  # noqa: ANN001, ANN201
        raise self._exc


def _middleware_app(exc: Exception, *, debug: bool = False) -> FastAPI:
    app = FastAPI()
    app.add_middleware(_RaiseInMiddleware, exc=exc)
    app.add_middleware(ErrorMiddleware, debug=debug)  # outermost

    @app.get("/boom")
    async def boom() -> None:
        return None

    return app


def _validation_error() -> ServiceValidationError:
    return ServiceValidationError("start before end", field="start")


# ── route-handler path ──────────────────────────────────────────────────────


def test_handler_path_default_omits_detail_keeps_params() -> None:
    body = TestClient(_handler_app(_validation_error())).get("/boom").json()

    assert "detail" not in body
    assert body["params"]["detail"] == "start before end"
    assert body["params"]["field"] == "start"


def test_handler_path_env_true_restores_detail(_no_detail_env: pytest.MonkeyPatch) -> None:
    _no_detail_env.setenv("VARCO_ERROR_INCLUDE_DETAIL", "true")

    body = TestClient(_handler_app(_validation_error())).get("/boom").json()

    assert "detail" in body


# ── middleware path (§D-flip10-parity) ──────────────────────────────────────


def test_middleware_path_default_has_parity_keys_and_no_detail() -> None:
    client = TestClient(_middleware_app(_validation_error()), raise_server_exceptions=False)

    body = client.get("/boom").json()

    for key in ("code", "message", "message_key", "params", "correlation_id"):
        assert key in body, f"missing {key!r} in middleware-path body {body}"
    assert "detail" not in body
    assert body["params"]["field"] == "start"


def test_middleware_path_env_true_adds_detail(_no_detail_env: pytest.MonkeyPatch) -> None:
    _no_detail_env.setenv("VARCO_ERROR_INCLUDE_DETAIL", "true")
    client = TestClient(_middleware_app(_validation_error()), raise_server_exceptions=False)

    body = client.get("/boom").json()

    assert "detail" in body
    assert "message_key" in body
    assert "params" in body


# ── controls: the flip touches only the ServiceException envelope ───────────


@pytest.mark.parametrize("path_kind", ["handler", "middleware"])
def test_http_exception_bodies_untouched(path_kind: str) -> None:
    exc = HTTPException(status_code=401, detail="x")
    app = _handler_app(exc) if path_kind == "handler" else _middleware_app(exc)
    client = TestClient(app, raise_server_exceptions=False)

    resp = client.get("/boom")

    assert resp.status_code == 401
    assert resp.json()["detail"] == "x"


def test_debug_500_detail_untouched() -> None:
    # `_internal_error_response`'s debug detail is not `include_detail` — unchanged.
    client = TestClient(
        _middleware_app(RuntimeError("kaboom"), debug=True), raise_server_exceptions=False
    )

    resp = client.get("/boom")

    assert resp.status_code == 500
    assert "kaboom" in resp.json()["detail"]
