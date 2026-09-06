"""
Red-mode tests for Plan 035 / Phase 3, Step 10 (S7, §D-S7-default) —
``SecurityHeadersMiddleware``.

``varco_fastapi.middleware.security_headers`` does not exist yet — every
test below must fail with ``ModuleNotFoundError``, not a fixture typo.
"""

from __future__ import annotations

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from varco_core.exception.service import ServiceNotFoundError
from varco_fastapi.middleware.error import ErrorMiddleware
from varco_fastapi.middleware.security_headers import (  # type: ignore[attr-defined]
    SecurityHeadersMiddleware,
    SecurityHeadersPreset,
    SecurityHeadersSettings,
)

_BALANCED_HEADERS = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "strict-origin-when-cross-origin",
}


def _make_app(**middleware_kwargs: object) -> FastAPI:
    app = FastAPI()
    # §D-order: SecurityHeadersMiddleware sits OUTSIDE ErrorMiddleware so its
    # headers attach to error responses too. Starlette's add_middleware()
    # prepends (last call = outermost, test_middleware_order.py's own
    # test_starlette_add_middleware_prepend_invariant) — so ErrorMiddleware
    # must be added FIRST for SecurityHeadersMiddleware (added second) to end
    # up outermost.
    app.add_middleware(ErrorMiddleware)
    app.add_middleware(SecurityHeadersMiddleware, **middleware_kwargs)

    @app.get("/ok")
    async def ok():
        return {"ok": True}

    @app.get("/boom")
    async def boom():
        raise ServiceNotFoundError(entity_id="1", entity_cls=object)

    @app.get("/crash")
    async def crash():
        raise RuntimeError("boom")

    @app.get("/set-header")
    async def set_header():
        from starlette.responses import JSONResponse

        resp = JSONResponse({"ok": True})
        resp.headers["X-Frame-Options"] = "SAMEORIGIN"
        return resp

    return app


async def _get(app: FastAPI, path: str, **kwargs: object):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path, **kwargs)


async def test_balanced_sends_exactly_four_headers_with_exact_values() -> None:
    app = _make_app(settings=SecurityHeadersSettings(preset=SecurityHeadersPreset.BALANCED))
    response = await _get(app, "/ok")
    for name, value in _BALANCED_HEADERS.items():
        assert response.headers[name] == value
    # HSTS absent over plain http:// (asserted more thoroughly below).
    assert "Strict-Transport-Security" not in response.headers


async def test_balanced_sends_no_csp_and_no_corp() -> None:
    # The two argued omissions — asserted so a future "completeness" edit
    # cannot silently add either back to BALANCED.
    app = _make_app(settings=SecurityHeadersSettings(preset=SecurityHeadersPreset.BALANCED))
    response = await _get(app, "/ok")
    assert "Content-Security-Policy" not in response.headers
    assert "Cross-Origin-Resource-Policy" not in response.headers


async def test_strict_additionally_sends_csp_coop_corp_permissions_policy() -> None:
    app = _make_app(settings=SecurityHeadersSettings(preset=SecurityHeadersPreset.STRICT))
    response = await _get(app, "/ok")
    assert "Content-Security-Policy" in response.headers
    assert "default-src 'none'" in response.headers["Content-Security-Policy"]
    assert response.headers["Cross-Origin-Opener-Policy"] == "same-origin"
    assert response.headers["Cross-Origin-Resource-Policy"] == "same-origin"
    assert "Permissions-Policy" in response.headers


async def test_hsts_absent_over_http() -> None:
    app = _make_app()
    response = await _get(app, "/ok")
    assert "Strict-Transport-Security" not in response.headers


async def test_hsts_present_over_https() -> None:
    app = _make_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="https://test") as client:
        response = await client.get("/ok")
    assert "Strict-Transport-Security" in response.headers
    assert response.headers["Strict-Transport-Security"] == "max-age=31536000; includeSubDomains"


async def test_hsts_absent_over_http_with_forwarded_proto_https_and_no_trusted_proxy() -> None:
    app = _make_app()
    response = await _get(app, "/ok", headers={"X-Forwarded-Proto": "https"})
    assert "Strict-Transport-Security" not in response.headers


async def test_hsts_present_when_forwarded_proto_https_and_peer_is_trusted_proxy() -> None:
    app = _make_app(settings=SecurityHeadersSettings(trusted_proxies=("127.0.0.1/32",)))
    response = await _get(app, "/ok", headers={"X-Forwarded-Proto": "https"})
    assert "Strict-Transport-Security" in response.headers


async def test_header_already_set_by_route_is_not_overwritten() -> None:
    app = _make_app()
    response = await _get(app, "/set-header")
    assert response.headers["X-Frame-Options"] == "SAMEORIGIN"


async def test_headers_present_on_a_404() -> None:
    app = _make_app()
    response = await _get(app, "/does-not-exist")
    assert response.status_code == 404
    for name, value in _BALANCED_HEADERS.items():
        assert response.headers[name] == value


async def test_headers_present_on_a_500() -> None:
    app = _make_app()
    response = await _get(app, "/crash")
    assert response.status_code == 500
    for name, value in _BALANCED_HEADERS.items():
        assert response.headers[name] == value


async def test_headers_present_on_a_service_exception_rendered_4xx() -> None:
    app = _make_app()
    response = await _get(app, "/boom")
    assert response.status_code == 404
    for name, value in _BALANCED_HEADERS.items():
        assert response.headers[name] == value


async def test_exclude_paths_defaults_exempt_docs_redoc_openapi_under_strict() -> None:
    settings = SecurityHeadersSettings(preset=SecurityHeadersPreset.STRICT)
    app = FastAPI(docs_url="/docs", redoc_url="/redoc", openapi_url="/openapi.json")
    app.add_middleware(SecurityHeadersMiddleware, settings=settings)

    response = await _get(app, "/docs")
    assert "Content-Security-Policy" not in response.headers

    response = await _get(app, "/openapi.json")
    assert "Content-Security-Policy" not in response.headers


async def test_header_field_set_to_none_omits_it() -> None:
    settings = SecurityHeadersSettings(x_frame_options=None)
    app = _make_app(settings=settings)
    response = await _get(app, "/ok")
    assert "X-Frame-Options" not in response.headers


async def test_enabled_false_is_byte_identical_to_no_middleware() -> None:
    disabled_app = _make_app(settings=SecurityHeadersSettings(enabled=False))
    baseline_app = FastAPI()
    baseline_app.add_middleware(ErrorMiddleware)

    @baseline_app.get("/ok")
    async def ok():
        return {"ok": True}

    disabled_response = await _get(disabled_app, "/ok")
    baseline_response = await _get(baseline_app, "/ok")

    for name in _BALANCED_HEADERS:
        assert name not in disabled_response.headers
    assert dict(disabled_response.headers) == dict(baseline_response.headers)
