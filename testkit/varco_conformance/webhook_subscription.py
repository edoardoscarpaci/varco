"""
WebhookSubscriptionRepositoryConformance — shared contract tests for
``WebhookSubscriptionRepository`` implementations (Plan 031 / D4, Step 22).

A **seventh ABC** outside the original five ``COVERAGE.md`` audits (the same
"new ABC outside the five, with a shared suite" treatment
``AbstractIdempotencyStore`` got in Plan 029) — it has three implementations
across three packages from day one
(``InMemoryWebhookSubscriptionRepository`` in ``varco_core``,
``SAWebhookSubscriptionRepository``, ``BeanieWebhookSubscriptionRepository``),
so it earns a real cross-package suite here rather than a same-package
contract module.

Subclass and override the ``repo`` fixture to opt a backend in::

    from varco_conformance.webhook_subscription import WebhookSubscriptionRepositoryConformance

    class TestSAWebhookSubscriptionRepositoryConformance(WebhookSubscriptionRepositoryConformance):
        @pytest.fixture
        async def repo(self, postgres_url):
            repo = SAWebhookSubscriptionRepository(
                url=postgres_url,
                encryptor=PlaintextFieldEncryptor(acknowledge_plaintext=True),
            )
            await repo.start()
            yield repo
            await repo.stop()

Not named ``Test*`` — never collected standalone (same convention as every
other module in this package).
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from varco_core.webhook.base import WebhookSubscriptionRepository
from varco_core.webhook.models import WebhookSubscription


class WebhookSubscriptionRepositoryConformance:
    """Shared behavioural contract for ``WebhookSubscriptionRepository``."""

    @pytest.fixture
    async def repo(self) -> WebhookSubscriptionRepository:
        """Abstract — must be overridden by every subclass."""
        raise NotImplementedError(
            "WebhookSubscriptionRepositoryConformance subclasses must override "
            "the `repo` fixture with a concrete WebhookSubscriptionRepository "
            "implementation."
        )

    def _tenant(self) -> str:
        return f"conformance-{uuid4().hex[:8]}"

    def _subscription(self, tenant_id: str, **overrides: object) -> WebhookSubscription:
        defaults: dict[str, object] = {
            "tenant_id": tenant_id,
            "target_url": "https://example.com/hook",
            "event_patterns": ["order.*"],
            "active_secrets": ["whsec_conformance"],
            "status": "ACTIVE",
            "consecutive_failures": 0,
            "signer": "standard_webhooks",
            "custom_headers": {},
        }
        defaults.update(overrides)
        return WebhookSubscription(**defaults)  # type: ignore[arg-type]

    async def test_save_assigns_a_pk_on_insert(self, repo: WebhookSubscriptionRepository) -> None:
        sub = self._subscription(self._tenant())
        saved = await repo.save(sub)
        assert saved.pk is not None

    async def test_find_by_id_round_trips(self, repo: WebhookSubscriptionRepository) -> None:
        tenant_id = self._tenant()
        saved = await repo.save(self._subscription(tenant_id))
        found = await repo.find_by_id(saved.pk)
        assert found is not None
        assert found.tenant_id == tenant_id
        assert found.target_url == saved.target_url

    async def test_find_by_id_unknown_pk_returns_none(
        self, repo: WebhookSubscriptionRepository
    ) -> None:
        assert await repo.find_by_id(uuid4()) is None

    async def test_find_by_tenant_never_leaks_another_tenant(
        self, repo: WebhookSubscriptionRepository
    ) -> None:
        tenant_a, tenant_b = self._tenant(), self._tenant()
        await repo.save(self._subscription(tenant_a))
        await repo.save(self._subscription(tenant_b))

        results = await repo.find_by_tenant(tenant_a)
        assert results
        assert all(r.tenant_id == tenant_a for r in results)

    async def test_find_active_matching_excludes_disabled(
        self, repo: WebhookSubscriptionRepository
    ) -> None:
        tenant_id = self._tenant()
        await repo.save(self._subscription(tenant_id, status="DISABLED"))
        results = await repo.find_active_matching("order.created", tenant_id=tenant_id)
        assert results == []

    async def test_find_active_matching_respects_event_pattern(
        self, repo: WebhookSubscriptionRepository
    ) -> None:
        tenant_id = self._tenant()
        await repo.save(self._subscription(tenant_id, event_patterns=["invoice.*"]))
        results = await repo.find_active_matching("order.created", tenant_id=tenant_id)
        assert results == []

        matching = await repo.find_active_matching("invoice.created", tenant_id=tenant_id)
        assert len(matching) == 1

    async def test_save_updates_an_existing_row(self, repo: WebhookSubscriptionRepository) -> None:
        tenant_id = self._tenant()
        saved = await repo.save(self._subscription(tenant_id))
        saved.status = "DISABLED"
        saved.consecutive_failures = 3
        updated = await repo.save(saved)

        reloaded = await repo.find_by_id(updated.pk)
        assert reloaded is not None
        assert reloaded.status == "DISABLED"
        assert reloaded.consecutive_failures == 3

    async def test_delete_removes_the_row(self, repo: WebhookSubscriptionRepository) -> None:
        saved = await repo.save(self._subscription(self._tenant()))
        await repo.delete(saved.pk)
        assert await repo.find_by_id(saved.pk) is None

    async def test_delete_unknown_pk_is_a_noop(self, repo: WebhookSubscriptionRepository) -> None:
        await repo.delete(uuid4())  # must not raise
