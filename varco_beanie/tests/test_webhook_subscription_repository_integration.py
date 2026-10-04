"""
Plan 031 (D4a) / Step 4 — red-mode integration tests for
``varco_beanie``'s ``WebhookSubscription`` repository.

Requires a real MongoDB broker (session-scoped ``mongo_url`` fixture,
CLAUDE.md shared-container convention). Every test namespaces its own
tenant_id with ``uuid4().hex[:8]`` since the container is shared across the
whole session.

Nothing under ``varco_beanie`` implements this repository yet — the import
below is expected to fail with ``ModuleNotFoundError``/``ImportError``.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from varco_conformance.webhook_subscription import WebhookSubscriptionRepositoryConformance

pytestmark = pytest.mark.integration


def _plaintext():
    # The conformance suite tests the repository contract, not the cipher —
    # the Fernet round-trip lives in test_webhook_repository_encryptor_required.py.
    from varco_core.encryption import PlaintextFieldEncryptor

    return PlaintextFieldEncryptor(acknowledge_plaintext=True)


def _tenant() -> str:
    return f"webhook-tenant-{uuid4().hex[:8]}"


class TestBeanieWebhookSubscriptionRepositoryConformance(WebhookSubscriptionRepositoryConformance):
    """Plan 031 / D4, Step 22 — shared cross-package conformance suite."""

    @pytest.fixture
    async def repo(self, mongo_url: str):
        from varco_beanie.webhook import BeanieWebhookSubscriptionRepository

        repo = BeanieWebhookSubscriptionRepository(
            url=mongo_url, db_name="webhook_conformance", encryptor=_plaintext()
        )
        await repo.start()
        yield repo
        await repo.stop()


@pytest.fixture
async def repo(mongo_url: str):
    from varco_beanie.webhook import BeanieWebhookSubscriptionRepository

    repo = BeanieWebhookSubscriptionRepository(
        url=mongo_url, db_name="webhook_test", encryptor=_plaintext()
    )
    await repo.start()
    yield repo
    await repo.stop()


async def test_save_then_find_by_id_round_trips(repo) -> None:
    from varco_core.webhook.models import WebhookSubscription

    tenant_id = _tenant()
    sub = WebhookSubscription(
        tenant_id=tenant_id,
        target_url="https://example.com/hook",
        event_patterns=["order.created"],
        active_secrets=["whsec_abc"],
        status="ACTIVE",
        consecutive_failures=0,
        signer="standard_webhooks",
        custom_headers={},
    )
    saved = await repo.save(sub)
    found = await repo.find_by_id(saved.pk)
    assert found is not None
    assert found.tenant_id == tenant_id


async def test_find_by_tenant_scopes_strictly(repo) -> None:
    from varco_core.webhook.models import WebhookSubscription

    tenant_a = _tenant()
    tenant_b = _tenant()
    sub_a = WebhookSubscription(
        tenant_id=tenant_a,
        target_url="https://example.com/hook-a",
        event_patterns=["order.*"],
        active_secrets=["whsec_a"],
        status="ACTIVE",
        consecutive_failures=0,
        signer="standard_webhooks",
        custom_headers={},
    )
    sub_b = WebhookSubscription(
        tenant_id=tenant_b,
        target_url="https://example.com/hook-b",
        event_patterns=["order.*"],
        active_secrets=["whsec_b"],
        status="ACTIVE",
        consecutive_failures=0,
        signer="standard_webhooks",
        custom_headers={},
    )
    await repo.save(sub_a)
    await repo.save(sub_b)

    results = await repo.find_by_tenant(tenant_a)
    assert all(r.tenant_id == tenant_a for r in results)
