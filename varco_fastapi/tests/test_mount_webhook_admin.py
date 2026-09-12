"""
Plan 031 (D4d) / Step 19 — red-mode tests for
``varco_fastapi.webhook.mount_webhook_admin``.

Modelled on ``varco_fastapi/tests/test_mount_reliability_admin.py``'s shape
(the sanctioned ``mount_*`` pattern — RD-9, `acknowledge_bundled_admin`).

``varco_fastapi/varco_fastapi/webhook/`` does not exist yet — every test
fails with ``ModuleNotFoundError`` on the first import.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


def _subscription(tenant_id: str):
    from varco_core.webhook.models import WebhookSubscription

    return WebhookSubscription(
        tenant_id=tenant_id,
        target_url="https://example.com/hook",
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


def test_mount_without_acknowledgement_raises_value_error(repo) -> None:
    from varco_fastapi.webhook import mount_webhook_admin

    app = FastAPI()

    with pytest.raises(ValueError, match="acknowledge_bundled_admin"):
        mount_webhook_admin(app, repository=repo)


def test_mount_with_acknowledgement_exposes_subscriptions_route(repo) -> None:
    from varco_fastapi.webhook import mount_webhook_admin

    app = FastAPI()

    mount_webhook_admin(app, repository=repo, acknowledge_bundled_admin=True)

    client = TestClient(app)
    resp = client.get("/webhooks/subscriptions")
    assert resp.status_code != 404


async def test_role_enforcement_rejects_caller_without_admin_role(repo) -> None:
    from varco_fastapi.webhook import mount_webhook_admin

    app = FastAPI()

    class _FakeAuth:
        """Minimal server_auth stand-in asserting a caller without the admin role."""

        def __call__(self):
            class _Ctx:
                roles = ["not-admin"]

            return _Ctx()

    mount_webhook_admin(
        app,
        repository=repo,
        acknowledge_bundled_admin=True,
        server_auth=_FakeAuth(),
        admin_role="webhook-admin",
    )

    client = TestClient(app)
    resp = client.get("/webhooks/subscriptions")
    assert resp.status_code == 403


async def test_cross_tenant_subscription_list_never_leaks_across_tenants(repo) -> None:
    """
    A subscription list must never leak across tenants — the plan calls this
    out explicitly as a merge-gate-adjacent assertion for Step 19.

    Updated by Plan 036 (S4a) / §D-S4-bola, Step 3: ``list_subscriptions``
    no longer trusts a caller-supplied ``X-Tenant-Id`` header (that was
    itself the leak this test exists to catch — a header the middleware
    never validated) — it scopes to the ambient, resolved
    ``current_tenant()`` instead. A spoofed header alongside the real
    ``tenant_context()`` must have no effect, which is exactly what this
    test now asserts.
    """
    from varco_core.service.tenant import tenant_context
    from varco_fastapi.webhook import mount_webhook_admin

    await repo.save(_subscription("tenant-a"))
    await repo.save(_subscription("tenant-b"))

    app = FastAPI()
    mount_webhook_admin(app, repository=repo, acknowledge_bundled_admin=True)

    client = TestClient(app)
    with tenant_context("tenant-a"):
        resp = client.get("/webhooks/subscriptions", headers={"X-Tenant-Id": "tenant-b"})
    assert resp.status_code == 200
    body = resp.json()
    returned_tenants = {item["tenant_id"] for item in body}
    assert returned_tenants == {"tenant-a"}
