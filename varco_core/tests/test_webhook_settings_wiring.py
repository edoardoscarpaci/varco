"""
Plan 031 (D4c) — drift fix: ``WebhookSettings`` must actually govern delivery.

The doc-sync pass found that ``WebhookSettings.allow_insecure_http`` /
``allow_list`` / ``extra_deny_ranges`` / ``signature_tolerance_seconds`` were
documented as configuration but reached no call site: ``WebhookDispatcher``
constructed no settings object, ``_send()`` called ``validate_target(url)`` with
no keyword arguments, and ``_deliver_one()`` called ``get_signer(...)`` without
``tolerance_seconds=``.

These tests pin the wiring end-to-end: what a caller puts in ``WebhookSettings``
is what reaches ``validate_target()`` and the signer, and an explicit constructor
keyword still wins over the settings value.
"""

from __future__ import annotations

import pytest
from varco_core.event import InMemoryEventBus
from varco_core.event.dlq import InMemoryDeadLetterQueue
from varco_core.resilience.retry import RetryPolicy
from varco_core.webhook.dispatcher import WebhookDispatcher, WebhookTriggerEvent
from varco_core.webhook.models import WebhookSubscription
from varco_core.webhook.settings import WebhookSettings


def _subscription(*, target_url: str = "https://example.com/hook") -> WebhookSubscription:
    return WebhookSubscription(
        tenant_id="tenant-1",
        target_url=target_url,
        event_patterns=["order.*"],
        active_secrets=["whsec_abc"],
        status="ACTIVE",
        consecutive_failures=0,
        signer="standard_webhooks",
        custom_headers={},
    )


@pytest.fixture
def repo():
    from varco_core.webhook.base import InMemoryWebhookSubscriptionRepository

    return InMemoryWebhookSubscriptionRepository()


# ── The four settings the drift report named ─────────────────────────────────


async def test_ssrf_settings_reach_validate_target(repo, monkeypatch) -> None:
    """allow_insecure_http / allow_list / extra_deny_ranges must be forwarded."""
    captured: dict[str, object] = {}

    async def _fake_validate(url, **kwargs):
        captured["url"] = url
        captured.update(kwargs)
        raise RuntimeError("stop after validation — nothing to send in this test")

    monkeypatch.setattr("varco_core.webhook.dispatcher.validate_target", _fake_validate)

    settings = WebhookSettings(
        allow_insecure_http=True,
        allow_list=("example.com",),
        extra_deny_ranges=("203.0.113.0/24",),
        retry_max_attempts=1,
    )
    dispatcher = WebhookDispatcher(repository=repo, settings=settings)
    await repo.save(_subscription(target_url="http://example.com/hook"))

    bus = InMemoryEventBus()
    dispatcher.register_to(bus, dlq=InMemoryDeadLetterQueue())
    await bus.publish(
        WebhookTriggerEvent(matched_event_type="order.created", payload={"tenant_id": "tenant-1"})
    )

    assert captured["url"] == "http://example.com/hook"
    assert captured["allow_insecure_http"] is True
    assert captured["allow_list"] == ("example.com",)
    assert captured["extra_deny_ranges"] == ("203.0.113.0/24",)


async def test_signature_tolerance_reaches_the_signer(repo) -> None:
    """signature_tolerance_seconds must be handed to the constructed signer."""
    settings = WebhookSettings(signature_tolerance_seconds=42.0)
    dispatcher = WebhookDispatcher(repository=repo, settings=settings)

    signer = dispatcher._build_signer(_subscription())

    assert signer._tolerance_seconds == 42.0


# ── Settings drive the retry/timeout/disable knobs too ───────────────────────


async def test_retry_and_timeout_settings_drive_the_defaults(repo) -> None:
    settings = WebhookSettings(
        retry_max_attempts=3,
        retry_base_delay_seconds=0.5,
        retry_max_delay_seconds=1.5,
        request_timeout_seconds=2.5,
        disable_after_failures=4,
    )
    dispatcher = WebhookDispatcher(repository=repo, settings=settings)

    assert dispatcher._retry_policy.max_attempts == 3
    assert dispatcher._retry_policy.base_delay == 0.5
    assert dispatcher._retry_policy.max_delay == 1.5
    assert dispatcher._request_timeout_seconds == 2.5
    assert dispatcher._disable_after_failures == 4


async def test_explicit_kwargs_win_over_settings(repo) -> None:
    """An explicit constructor keyword is an override, not a duplicate source."""
    settings = WebhookSettings(
        retry_max_attempts=3, request_timeout_seconds=2.5, disable_after_failures=4
    )
    explicit = RetryPolicy(max_attempts=9, base_delay=0.01, max_delay=0.02, jitter=False)
    dispatcher = WebhookDispatcher(
        repository=repo,
        settings=settings,
        retry_policy=explicit,
        request_timeout_seconds=99.0,
        disable_after_failures=7,
    )

    assert dispatcher._retry_policy is explicit
    assert dispatcher._request_timeout_seconds == 99.0
    assert dispatcher._disable_after_failures == 7


async def test_omitting_settings_uses_env_backed_defaults(repo, monkeypatch) -> None:
    """No settings= argument means WebhookSettings() — which reads VARCO_WEBHOOK_*."""
    monkeypatch.setenv("VARCO_WEBHOOK_DISABLE_AFTER_FAILURES", "3")
    monkeypatch.setenv("VARCO_WEBHOOK_REQUEST_TIMEOUT_SECONDS", "1.25")

    dispatcher = WebhookDispatcher(repository=repo)

    assert dispatcher._disable_after_failures == 3
    assert dispatcher._request_timeout_seconds == 1.25


# ── Plan 038 (S19) / Phase 5, Step 16 — inbound_* settings threading ────────


def test_inbound_settings_have_documented_defaults() -> None:
    settings = WebhookSettings()
    assert settings.inbound_tolerance_seconds == 300.0
    assert settings.inbound_replay_ttl_seconds == 600.0


def test_inbound_tolerance_env_var_parses(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VARCO_WEBHOOK_INBOUND_TOLERANCE_SECONDS", "60")
    settings = WebhookSettings()
    assert settings.inbound_tolerance_seconds == 60.0


async def test_get_verifier_with_settings_rejects_a_90s_old_delivery_at_60s_tolerance() -> None:
    import time

    from varco_core.webhook.inbound.verifiers import get_verifier

    settings = WebhookSettings(inbound_tolerance_seconds=60.0)
    verifier = get_verifier("stripe", secrets=["whsec_stripe_test_secret"], settings=settings)

    import hashlib
    import hmac as hmac_module

    ts = str(int(time.time()) - 90)
    body = b'{"id": "evt_1"}'
    sig = hmac_module.new(
        b"whsec_stripe_test_secret", f"{ts}.{body.decode()}".encode(), hashlib.sha256
    ).hexdigest()
    result = verifier.verify(body=body, headers={"Stripe-Signature": f"t={ts},v1={sig}"})
    assert result.verified is False


def test_explicit_tolerance_seconds_kwarg_wins_over_settings() -> None:
    from varco_core.webhook.inbound.verifiers import get_verifier

    settings = WebhookSettings(inbound_tolerance_seconds=60.0)
    verifier = get_verifier(
        "stripe",
        secrets=["whsec_stripe_test_secret"],
        settings=settings,
        tolerance_seconds=9999.0,
    )
    assert verifier._tolerance_seconds == 9999.0


def test_settings_none_constructs_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("VARCO_WEBHOOK_INBOUND_TOLERANCE_SECONDS", "45")
    from varco_core.webhook.inbound.verifiers import get_verifier

    verifier = get_verifier("stripe", secrets=["whsec_stripe_test_secret"], settings=None)
    assert verifier._tolerance_seconds == 45.0
