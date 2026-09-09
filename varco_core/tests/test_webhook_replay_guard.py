"""
Plan 038 (S19) / Step 12 — red-mode tests for
``varco_core.webhook.inbound.replay.WebhookReplayGuard``.

Fails with ``ModuleNotFoundError`` until Step 13 lands.
"""

from __future__ import annotations

import asyncio

import pytest
from varco_core.idempotency.memory import InMemoryIdempotencyStore


@pytest.fixture
def store() -> InMemoryIdempotencyStore:
    return InMemoryIdempotencyStore()


async def test_first_delivery_is_accepted(store: InMemoryIdempotencyStore) -> None:
    from varco_core.webhook.inbound.replay import WebhookReplayGuard

    guard = WebhookReplayGuard(store=store, ttl_seconds=600.0)
    await guard.claim("stripe", "evt_1", b"{}")  # must not raise


async def test_immediate_identical_replay_raises_replay_error(
    store: InMemoryIdempotencyStore,
) -> None:
    from varco_core.exception.webhook import WebhookReplayError
    from varco_core.webhook.inbound.replay import WebhookReplayGuard

    guard = WebhookReplayGuard(store=store, ttl_seconds=600.0)
    await guard.claim("stripe", "evt_1", b"{}")

    with pytest.raises(WebhookReplayError):
        await guard.claim("stripe", "evt_1", b"{}")


async def test_replay_rejected_within_ttl_after_complete(
    store: InMemoryIdempotencyStore,
) -> None:
    from varco_core.exception.webhook import WebhookReplayError
    from varco_core.webhook.inbound.replay import WebhookReplayGuard

    guard = WebhookReplayGuard(store=store, ttl_seconds=600.0)
    await guard.claim("stripe", "evt_1", b"{}")
    await guard.complete("stripe", "evt_1", b"{}")

    with pytest.raises(WebhookReplayError):
        await guard.claim("stripe", "evt_1", b"{}")


async def test_after_release_the_same_delivery_id_is_accepted_again(
    store: InMemoryIdempotencyStore,
) -> None:
    # The provider-retry-after-our-failure case §D-S19-replay names.
    from varco_core.webhook.inbound.replay import WebhookReplayGuard

    guard = WebhookReplayGuard(store=store, ttl_seconds=600.0)
    await guard.claim("stripe", "evt_1", b"{}")
    await guard.release("stripe", "evt_1")

    await guard.claim("stripe", "evt_1", b"{}")  # must not raise


async def test_concurrent_identical_deliveries_yield_exactly_one_acceptance(
    store: InMemoryIdempotencyStore,
) -> None:
    from varco_core.exception.webhook import WebhookReplayError
    from varco_core.webhook.inbound.replay import WebhookReplayGuard

    guard = WebhookReplayGuard(store=store, ttl_seconds=600.0)

    async def _attempt() -> bool:
        try:
            await guard.claim("stripe", "evt_concurrent", b"{}")
            return True
        except WebhookReplayError:
            return False

    results = await asyncio.gather(*(_attempt() for _ in range(10)))
    assert sum(results) == 1


async def test_storage_key_starts_with_webhook_provider_prefix(
    store: InMemoryIdempotencyStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    from varco_core.webhook.inbound.replay import WebhookReplayGuard

    captured: dict[str, str] = {}
    original_reserve = store.reserve

    async def _spy_reserve(key, fingerprint, *, ttl):
        captured["key"] = key
        return await original_reserve(key, fingerprint, ttl=ttl)

    monkeypatch.setattr(store, "reserve", _spy_reserve)

    guard = WebhookReplayGuard(store=store, ttl_seconds=600.0)
    await guard.claim("stripe", "evt_1", b"{}")

    assert captured["key"].startswith("webhook:stripe:")


async def test_ttl_not_positive_raises(store: InMemoryIdempotencyStore) -> None:
    from varco_core.webhook.inbound.replay import WebhookReplayGuard

    with pytest.raises(ValueError):
        WebhookReplayGuard(store=store, ttl_seconds=0.0)
