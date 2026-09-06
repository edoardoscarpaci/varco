"""
Red-mode tests for Plan 035 / Phase 4, Steps 16 + 20 (S8, §D-S8-default) —
``BodyLimitMiddleware``.

``varco_fastapi.middleware.body_limit`` does not exist yet — every test
below must fail with ``ModuleNotFoundError``, not a fixture typo.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from httpx import ASGITransport, AsyncClient
from varco_core.idempotency.memory import InMemoryIdempotencyStore
from varco_fastapi.middleware.body_limit import (  # type: ignore[attr-defined]
    BodyLimitMiddleware,
    BodyLimitSettings,
)
from varco_fastapi.middleware.error import ErrorMiddleware
from varco_fastapi.middleware.idempotency import IdempotencyMiddleware

_TEN_MIB = 10 * 1024 * 1024


def _make_app(handler_sentinel: list[int], **middleware_kwargs: object) -> FastAPI:
    app = FastAPI()
    app.add_middleware(BodyLimitMiddleware, **middleware_kwargs)
    app.add_middleware(ErrorMiddleware)

    @app.post("/upload")
    async def upload(request: Request):
        handler_sentinel.append(1)
        await request.body()
        return {"ok": True}

    @app.get("/no-body")
    async def no_body():
        handler_sentinel.append(1)
        return {"ok": True}

    return app


async def _post(app: FastAPI, path: str, **kwargs: object):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.post(path, **kwargs)


async def test_content_length_above_ceiling_rejected_before_handler_runs() -> None:
    sentinel: list[int] = []
    app = _make_app(sentinel, settings=BodyLimitSettings(max_bytes=100))
    response = await _post(app, "/upload", content=b"x" * 200)
    assert response.status_code == 413
    assert sentinel == []


async def test_chunked_body_with_no_content_length_exceeding_ceiling_rejected() -> None:
    sentinel: list[int] = []
    app = _make_app(sentinel, settings=BodyLimitSettings(max_bytes=100))

    async def gen():
        yield b"x" * 60
        yield b"x" * 60

    response = await _post(app, "/upload", content=gen())
    assert response.status_code == 413


async def test_lying_content_length_below_ceiling_still_rejected() -> None:
    sentinel: list[int] = []
    app = _make_app(sentinel, settings=BodyLimitSettings(max_bytes=100))

    async def gen():
        yield b"x" * 200  # actual body far exceeds the (absent/short) declared length

    response = await _post(app, "/upload", content=gen(), headers={"Content-Length": "10"})
    assert response.status_code == 413


async def test_413_body_carries_envelope_code_message_correlation_id() -> None:
    sentinel: list[int] = []
    app = _make_app(sentinel, settings=BodyLimitSettings(max_bytes=100))
    response = await _post(app, "/upload", content=b"x" * 200)
    body = response.json()
    assert "code" in body
    assert "message" in body
    assert "correlation_id" in body
    assert "100" in body["message"] or "VARCO_BODY_LIMIT_MAX_BYTES" in body["message"]


async def test_body_exactly_at_ceiling_passes() -> None:
    sentinel: list[int] = []
    app = _make_app(sentinel, settings=BodyLimitSettings(max_bytes=100))
    response = await _post(app, "/upload", content=b"x" * 100)
    assert response.status_code == 200


async def test_get_with_no_body_passes() -> None:
    sentinel: list[int] = []
    app = _make_app(sentinel, settings=BodyLimitSettings(max_bytes=100))
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/no-body")
    assert response.status_code == 200
    assert sentinel == [1]


async def test_exempt_paths_bypasses_the_limit() -> None:
    sentinel: list[int] = []
    app = _make_app(sentinel, settings=BodyLimitSettings(max_bytes=100, exempt_paths=("/upload",)))
    response = await _post(app, "/upload", content=b"x" * 200)
    assert response.status_code == 200


async def test_enabled_false_is_byte_identical_to_no_middleware() -> None:
    sentinel_disabled: list[int] = []
    disabled_app = _make_app(
        sentinel_disabled, settings=BodyLimitSettings(enabled=False, max_bytes=1)
    )

    baseline_sentinel: list[int] = []
    baseline_app = FastAPI()
    baseline_app.add_middleware(ErrorMiddleware)

    @baseline_app.post("/upload")
    async def upload(request: Request):
        baseline_sentinel.append(1)
        await request.body()
        return {"ok": True}

    disabled_response = await _post(disabled_app, "/upload", content=b"x" * 1000)
    baseline_response = await _post(baseline_app, "/upload", content=b"x" * 1000)

    assert disabled_response.status_code == baseline_response.status_code == 200
    assert sentinel_disabled == baseline_sentinel == [1]


async def test_security_headers_present_on_413() -> None:
    from varco_fastapi.middleware.security_headers import (  # type: ignore[attr-defined]
        SecurityHeadersMiddleware,
    )

    sentinel: list[int] = []
    app = FastAPI()
    app.add_middleware(BodyLimitMiddleware, settings=BodyLimitSettings(max_bytes=10))
    app.add_middleware(ErrorMiddleware)
    app.add_middleware(SecurityHeadersMiddleware)

    @app.post("/upload")
    async def upload(request: Request):
        sentinel.append(1)
        await request.body()
        return {"ok": True}

    response = await _post(app, "/upload", content=b"x" * 200)
    assert response.status_code == 413
    assert "X-Content-Type-Options" in response.headers


# ── Step 20 — IdempotencyMiddleware never buffers an over-limit body ────────


# ── Drift 2 regression — enable_error_middleware=False self-renders a 413 ──


async def test_enable_error_middleware_false_over_limit_body_gets_plain_413_not_500() -> None:
    """
    Regression test for §Edge cases: with no ErrorMiddleware in the stack,
    an over-limit body must get a self-rendered 413 with a JSON body
    carrying code/message (naming the ceiling and the env var), never an
    unhandled 500.
    """
    from providify import DIContainer
    from varco_fastapi.app import create_varco_app

    app = create_varco_app(
        container=DIContainer(),
        routers=[],
        validate=False,
        enable_error_middleware=False,
        body_limit=BodyLimitSettings(max_bytes=100),
        rate_limit=None,
    )

    @app.post("/upload")
    async def upload(request: Request):
        await request.body()
        return {"ok": True}

    response = await _post(app, "/upload", content=b"x" * 200)
    assert response.status_code == 413
    body = response.json()
    assert "code" in body
    assert "message" in body
    assert "100" in body["message"] or "VARCO_BODY_LIMIT_MAX_BYTES" in body["message"]


async def test_enable_error_middleware_true_over_limit_body_still_renders_through_envelope() -> (
    None
):
    """The ErrorMiddleware-present path must stay unchanged (regression guard)."""
    from providify import DIContainer
    from varco_fastapi.app import create_varco_app

    app = create_varco_app(
        container=DIContainer(),
        routers=[],
        validate=False,
        enable_error_middleware=True,
        body_limit=BodyLimitSettings(max_bytes=100),
        rate_limit=None,
    )

    @app.post("/upload")
    async def upload(request: Request):
        await request.body()
        return {"ok": True}

    response = await _post(app, "/upload", content=b"x" * 200)
    assert response.status_code == 413
    body = response.json()
    assert "correlation_id" in body


async def test_over_limit_request_with_idempotency_key_never_buffered_by_idempotency() -> None:
    """
    Regression test for §D-order's placement decision: BodyLimitMiddleware
    sits OUTSIDE IdempotencyMiddleware, so a rejected over-limit request must
    never reach ``IdempotencyMiddleware.dispatch`` at all — asserted with a
    store spy on ``reserve()``.
    """

    class _SpyStore(InMemoryIdempotencyStore):
        def __init__(self) -> None:
            super().__init__()
            self.reserve_calls = 0

        async def reserve(self, *args: object, **kwargs: object):  # type: ignore[override]
            self.reserve_calls += 1
            return await super().reserve(*args, **kwargs)

    store = _SpyStore()
    app = FastAPI()
    app.add_middleware(IdempotencyMiddleware, store=store)
    app.add_middleware(BodyLimitMiddleware, settings=BodyLimitSettings(max_bytes=10))
    app.add_middleware(ErrorMiddleware)

    @app.post("/orders")
    async def create_order(request: Request):
        await request.body()
        return {"ok": True}

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/orders",
            content=b"x" * 200,
            headers={"Idempotency-Key": "abc123"},
        )

    assert response.status_code == 413
    assert store.reserve_calls == 0
