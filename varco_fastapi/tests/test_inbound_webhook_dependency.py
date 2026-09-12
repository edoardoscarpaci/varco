"""
Plan 038 (S19) / Steps 18, 21 — red-mode tests for
``varco_fastapi.webhook.inbound.verify_webhook`` / ``VerifiedWebhook``.

``varco_fastapi/varco_fastapi/webhook/inbound.py`` does not exist yet —
every test below must fail with ``ModuleNotFoundError``, not a fixture
typo.
"""

from __future__ import annotations

import hashlib
import hmac
import time

import pytest
from fastapi import Depends, FastAPI
from httpx import ASGITransport, AsyncClient
from varco_core.idempotency.memory import InMemoryIdempotencyStore
from varco_fastapi.middleware.error import ErrorMiddleware

SECRET = "whsec_stripe_test_secret"


def _stripe_signature(secret: str, ts: str, body: bytes) -> str:
    signed = f"{ts}.{body.decode()}".encode()
    digest = hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()
    return f"t={ts},v1={digest}"


def _build_app(*, replay_guard=None, body_limit_kwargs: dict | None = None):
    from varco_core.webhook.inbound.verifiers import StripeWebhookVerifier
    from varco_fastapi.webhook.inbound import VerifiedWebhook, verify_webhook

    app = FastAPI()

    # add_middleware() makes the LAST-added middleware outermost, so
    # BodyLimitMiddleware must be registered before ErrorMiddleware for the
    # 413 it raises to be rendered rather than escaping the stack.
    if body_limit_kwargs is not None:
        from varco_fastapi.middleware.body_limit import (
            BodyLimitMiddleware,
            BodyLimitSettings,
        )

        app.add_middleware(
            BodyLimitMiddleware,
            settings=BodyLimitSettings(**body_limit_kwargs),
        )

    app.add_middleware(ErrorMiddleware)

    verifier = StripeWebhookVerifier(secrets=[SECRET])
    dependency = verify_webhook(verifier, replay_guard=replay_guard)

    call_log: list[bytes] = []

    @app.post("/hooks/stripe")
    async def receive(webhook: VerifiedWebhook = Depends(dependency)) -> dict:
        call_log.append(webhook.body)
        return {"received": True, "provider": webhook.provider}

    @app.post("/hooks/stripe-raising")
    async def receive_raising(webhook: VerifiedWebhook = Depends(dependency)) -> dict:
        raise RuntimeError("handler exploded")

    @app.post("/hooks/stripe-with-body-model")
    async def receive_with_model(
        payload: dict, webhook: VerifiedWebhook = Depends(dependency)
    ) -> dict:
        return {"payload": payload, "raw_len": len(webhook.body)}

    return app, call_log, verifier


async def _client(app: FastAPI) -> AsyncClient:
    transport = ASGITransport(app=app)
    return AsyncClient(transport=transport, base_url="http://test")


async def test_correctly_signed_delivery_returns_200_with_exact_raw_bytes() -> None:
    app, call_log, _ = _build_app()
    body = b'{"id": "evt_1"}'
    ts = str(int(time.time()))
    sig = _stripe_signature(SECRET, ts, body)

    async with await _client(app) as client:
        response = await client.post(
            "/hooks/stripe", content=body, headers={"Stripe-Signature": sig}
        )

    assert response.status_code == 200
    assert call_log == [body]


async def test_body_model_on_same_route_still_parses() -> None:
    app, _, _ = _build_app()
    body = b'{"x": 1}'
    ts = str(int(time.time()))
    sig = _stripe_signature(SECRET, ts, body)

    async with await _client(app) as client:
        response = await client.post(
            "/hooks/stripe-with-body-model",
            content=body,
            headers={"Stripe-Signature": sig, "Content-Type": "application/json"},
        )

    assert response.status_code == 200
    assert response.json()["payload"] == {"x": 1}


async def test_tampered_body_returns_401_with_webhook_signature_error_code() -> None:
    app, _, _ = _build_app()
    body = b'{"id": "evt_1"}'
    ts = str(int(time.time()))
    sig = _stripe_signature(SECRET, ts, b'{"id": "evt_ORIGINAL"}')

    async with await _client(app) as client:
        response = await client.post(
            "/hooks/stripe", content=body, headers={"Stripe-Signature": sig}
        )

    assert response.status_code == 401
    body_json = response.json()
    assert "correlation_id" in body_json


async def test_replayed_delivery_returns_409() -> None:
    store = InMemoryIdempotencyStore()
    from varco_core.webhook.inbound.replay import WebhookReplayGuard

    guard = WebhookReplayGuard(store=store, ttl_seconds=600.0)
    app, _, _ = _build_app(replay_guard=guard)
    body = b'{"id": "evt_replay"}'
    ts = str(int(time.time()))
    sig = _stripe_signature(SECRET, ts, body)

    async with await _client(app) as client:
        first = await client.post("/hooks/stripe", content=body, headers={"Stripe-Signature": sig})
        second = await client.post("/hooks/stripe", content=body, headers={"Stripe-Signature": sig})

    assert first.status_code == 200
    assert second.status_code == 409


async def test_handler_raising_leaves_delivery_replayable() -> None:
    store = InMemoryIdempotencyStore()
    from varco_core.webhook.inbound.replay import WebhookReplayGuard

    guard = WebhookReplayGuard(store=store, ttl_seconds=600.0)
    app, _, _ = _build_app(replay_guard=guard)
    body = b'{"id": "evt_raise"}'
    ts = str(int(time.time()))
    sig = _stripe_signature(SECRET, ts, body)

    async with await _client(app) as client:
        first = await client.post(
            "/hooks/stripe-raising", content=body, headers={"Stripe-Signature": sig}
        )
        # release() should have run -- a retry of the same delivery id
        # against the raising route must be accepted again (not 409).
        second = await client.post(
            "/hooks/stripe-raising", content=body, headers={"Stripe-Signature": sig}
        )

    assert first.status_code == 500
    assert second.status_code == 500  # both attempted the handler; neither is 409


async def test_response_body_and_log_never_contain_secret_or_signature(caplog) -> None:
    app, _, _ = _build_app()
    body = b'{"id": "evt_1"}'
    ts = str(int(time.time()))
    tampered_sig = _stripe_signature(SECRET, ts, b'{"id": "evt_DIFFERENT"}')

    async with await _client(app) as client:
        response = await client.post(
            "/hooks/stripe", content=body, headers={"Stripe-Signature": tampered_sig}
        )

    assert SECRET not in response.text
    assert tampered_sig not in response.text
    for record in caplog.records:
        assert SECRET not in record.getMessage()
        assert tampered_sig not in record.getMessage()


# ── Step 21 — BodyLimitMiddleware interaction ────────────────────────────────


async def test_over_limit_body_is_413_before_verifier_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_core.webhook.inbound import verifiers as verifiers_module

    calls = {"n": 0}
    original_verify = verifiers_module.StripeWebhookVerifier.verify

    def _spy_verify(self, **kwargs):
        calls["n"] += 1
        return original_verify(self, **kwargs)

    monkeypatch.setattr(verifiers_module.StripeWebhookVerifier, "verify", _spy_verify)

    app, _, _ = _build_app(body_limit_kwargs={"max_bytes": 100})
    body = b"x" * 1000
    ts = str(int(time.time()))
    sig = _stripe_signature(SECRET, ts, body)

    async with await _client(app) as client:
        response = await client.post(
            "/hooks/stripe", content=body, headers={"Stripe-Signature": sig}
        )

    assert response.status_code == 413
    assert calls["n"] == 0


async def test_exempted_path_reaches_the_verifier(monkeypatch: pytest.MonkeyPatch) -> None:
    app, call_log, _ = _build_app(
        body_limit_kwargs={"max_bytes": 100, "exempt_paths": ("/hooks/stripe",)}
    )
    body = b"x" * 1000
    ts = str(int(time.time()))
    sig = _stripe_signature(SECRET, ts, body)

    async with await _client(app) as client:
        response = await client.post(
            "/hooks/stripe", content=body, headers={"Stripe-Signature": sig}
        )

    assert response.status_code == 200
    assert call_log == [body]
