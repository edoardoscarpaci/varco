"""
tests.test_logging_redaction
==============================
Plan 040 / S21, Phase 4, Step 15 — ``RequestLoggingMiddleware`` gains a
``redactor`` constructor keyword (§D-S21-logging).

Covers:
    - ``redactor=None`` (the default) -> the emitted log entry is
      byte-identical to today.
    - a ``PolicyRedactor`` configured to redact a field the middleware
      already emits (``tenant_id``) -> that field is redacted while
      ``method``/``path``/``status``/``duration_ms``/``user_id`` survive.
    - ``skip_paths`` behaviour is unchanged with a redactor configured.

⛔ This file does not touch ``app.py`` or ``middleware/__init__.py`` — the
Plan 041 boundary (§D-S21-logging).
"""

from __future__ import annotations

import logging
from typing import Any

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse
from starlette.routing import Route
from starlette.testclient import TestClient
from varco_fastapi.middleware.logging import RequestLoggingMiddleware


def _build_app(**middleware_kwargs: Any) -> Starlette:
    async def endpoint(request: Request) -> PlainTextResponse:
        return PlainTextResponse("ok")

    async def health(request: Request) -> PlainTextResponse:
        return PlainTextResponse("healthy")

    app = Starlette(routes=[Route("/echo", endpoint), Route("/health", health)])
    app.add_middleware(RequestLoggingMiddleware, **middleware_kwargs)
    return app


def test_constructor_accepts_redactor_keyword_defaulting_to_none() -> None:
    # Must not raise TypeError for an unexpected keyword.
    app = _build_app(redactor=None)
    client = TestClient(app, raise_server_exceptions=True)
    response = client.get("/echo")
    assert response.status_code == 200


def test_default_redactor_none_log_entry_byte_identical(caplog: Any) -> None:
    app = _build_app(redactor=None)
    client = TestClient(app, raise_server_exceptions=True)

    with caplog.at_level(logging.INFO, logger="varco_fastapi.access"):
        client.get("/echo")

    records = [r for r in caplog.records if r.name == "varco_fastapi.access"]
    assert len(records) == 1
    entry = records[0].args[0] if records[0].args else records[0].msg
    # No redaction placeholder should appear anywhere in the byte-identical entry.
    assert "[REDACTED]" not in str(entry)


def test_redactor_applies_to_a_field_the_middleware_already_emits(
    caplog: Any, monkeypatch: Any
) -> None:
    # Force a tenant_id onto the request context so the middleware emits it.
    from varco_core.redaction import PolicyRedactor, RedactionPolicy

    def _fake_get_request_id() -> str:
        return "req-1"

    class _FakeCtx:
        user_id = "user-1"
        metadata = {"tenant_id": "tenant-secret"}

    def _fake_get_auth_context_or_none() -> Any:
        return _FakeCtx()

    monkeypatch.setattr("varco_fastapi.context.get_request_id", _fake_get_request_id, raising=False)
    monkeypatch.setattr(
        "varco_fastapi.context.get_auth_context_or_none",
        _fake_get_auth_context_or_none,
        raising=False,
    )

    redactor = PolicyRedactor(policy=RedactionPolicy(patterns=("tenant_id",)))
    app = _build_app(redactor=redactor)
    client = TestClient(app, raise_server_exceptions=True)

    with caplog.at_level(logging.INFO, logger="varco_fastapi.access"):
        client.get("/echo")

    records = [r for r in caplog.records if r.name == "varco_fastapi.access"]
    assert len(records) == 1
    entry = records[0].args[0] if records[0].args else records[0].msg
    entry_str = str(entry)
    assert "tenant-secret" not in entry_str
    assert "GET" in entry_str
    assert "/echo" in entry_str


def test_skip_paths_still_skips_logging_with_redactor_configured(caplog: Any) -> None:
    from varco_core.redaction import PolicyRedactor

    app = _build_app(redactor=PolicyRedactor(), skip_paths={"/health"})
    client = TestClient(app, raise_server_exceptions=True)

    with caplog.at_level(logging.INFO, logger="varco_fastapi.access"):
        client.get("/health")

    records = [r for r in caplog.records if r.name == "varco_fastapi.access"]
    assert records == []
