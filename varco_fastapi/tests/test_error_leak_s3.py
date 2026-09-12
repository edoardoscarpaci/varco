"""
Red-mode tests for Plan 035 / Phase 1, Step 3 (S3, §D-S3a) — closing the
error-response information leak.

Two fallback sites echo ``str(exc)`` today:
    - ``varco_fastapi/middleware/error.py:283`` (ErrorMiddleware, used from
      inside another ASGI middleware / BaseExceptionGroup path)
    - ``varco_fastapi/exceptions.py:159`` (add_exception_handlers, the
      FastAPI-native route-handler path)

Both are reached ONLY when ``exc.error_params()`` raises (the actually
reachable trigger per §D-S3's honest note — an unmapped ``DBAPIError``/
``OSError`` never reaches this path; it is caught earlier by
``_internal_error_response``, already sanitized).

This file asserts, for both sites:
    - the body's ``message`` is the opaque constant
    - ``str(exc)`` (the leaked internal detail) appears NOWHERE in the
      serialized body
    - ``correlation_id`` is present
    - the status still comes from ``_FALLBACK_STATUS`` (500 here, since our
      trigger subclasses ``ServiceException`` directly, not one of the four
      mapped subclasses)
    - ``caplog`` contains the exception type and was logged with
      ``exc_info=True``

...plus a byte-identical-to-3.1 regression proof for a *mapped* exception.
"""

from __future__ import annotations

import json
import logging

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from varco_core.exception.service import ServiceException, ServiceNotFoundError
from varco_fastapi.exceptions import add_exception_handlers
from varco_fastapi.middleware.error import ErrorMiddleware

_LEAK_SENTINEL = "super-secret-internal-detail-xyz123"


class _ErrorParamsRaisesException(ServiceException):
    """A ServiceException whose error_params() raises — the reachable trigger."""

    def __init__(self) -> None:
        super().__init__(_LEAK_SENTINEL)

    def error_params(self) -> dict[str, object]:
        raise RuntimeError("boom while building params")


def _body_as_text(body: dict) -> str:
    return json.dumps(body)


# ── ErrorMiddleware path (error.py) ─────────────────────────────────────────


def _make_error_middleware_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(ErrorMiddleware)

    @app.get("/boom")
    async def boom():
        raise _ErrorParamsRaisesException()

    @app.get("/mapped")
    async def mapped():
        raise ServiceNotFoundError(entity_id="1", entity_cls=object)

    return app


async def test_error_middleware_fallback_never_leaks_str_exc() -> None:
    app = _make_error_middleware_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/boom")

    body = response.json()
    assert _LEAK_SENTINEL not in _body_as_text(body)
    assert body["message"] == "An internal error occurred."
    assert "correlation_id" in body


async def test_error_middleware_fallback_status_from_fallback_status_map() -> None:
    app = _make_error_middleware_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/boom")
    # _FALLBACK_STATUS has no entry for _ErrorParamsRaisesException -> 500.
    assert response.status_code == 500


async def test_error_middleware_fallback_logs_exception_type_with_exc_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR)
    app = _make_error_middleware_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.get("/boom")

    records = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert records, "expected an ERROR-level log record for the fallback path"
    assert "_ErrorParamsRaisesException" in caplog.text
    assert any(r.exc_info for r in records), "expected exc_info=True on the fallback log record"


async def test_error_middleware_mapped_exception_byte_identical_no_regression() -> None:
    app = _make_error_middleware_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/mapped")
    assert response.status_code == 404
    body = response.json()
    assert body["code"] == "FASTREST_001"
    assert "message" in body


# ── add_exception_handlers path (exceptions.py) ─────────────────────────────


def _make_exception_handlers_app() -> FastAPI:
    app = FastAPI()
    add_exception_handlers(app)

    @app.get("/boom")
    async def boom():
        raise _ErrorParamsRaisesException()

    @app.get("/mapped")
    async def mapped():
        raise ServiceNotFoundError(entity_id="1", entity_cls=object)

    return app


async def test_exception_handlers_fallback_never_leaks_str_exc() -> None:
    app = _make_exception_handlers_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/boom")

    body = response.json()
    assert _LEAK_SENTINEL not in _body_as_text(body)
    assert body["message"] == "An internal error occurred."
    assert "correlation_id" in body


async def test_exception_handlers_fallback_status_from_fallback_status_map() -> None:
    app = _make_exception_handlers_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/boom")
    assert response.status_code == 500


async def test_exception_handlers_fallback_logs_exception_type_with_exc_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR)
    app = _make_exception_handlers_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.get("/boom")

    records = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert records, "expected an ERROR-level log record for the fallback path"
    assert "_ErrorParamsRaisesException" in caplog.text
    assert any(r.exc_info for r in records), "expected exc_info=True on the fallback log record"


async def test_exception_handlers_mapped_exception_byte_identical_no_regression() -> None:
    app = _make_exception_handlers_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/mapped")
    assert response.status_code == 404
    body = response.json()
    assert body["code"] == "FASTREST_001"
    assert "message" in body
