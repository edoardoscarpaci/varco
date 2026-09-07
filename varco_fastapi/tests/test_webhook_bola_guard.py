"""
Plan 036 (S4a) / Phase 0, Step 5 — red-mode tests for the webhook admin BOLA
fix (§D-S4-bola).

Today ``get_subscription``/``disable_subscription``/``enable_subscription``/
``rotate_secret``/``delete_subscription`` in
``varco_fastapi/varco_fastapi/webhook/router.py`` fetch by ``pk`` with no
tenant check at all — a caller scoped to tenant A can read/mutate/rotate a
subscription belonging to tenant B. These tests assert the fixed behaviour:
a cross-tenant ``pk`` must 404 (never 403 — no existence oracle), and
``rotate_secret`` in particular must neither rotate the secret nor reveal it.
``list_subscriptions`` must stop trusting the ``X-Tenant-Id`` header and use
the resolved ambient tenant instead.

Every test here is expected to FAIL against the current implementation,
which performs no tenant comparison at all — the by-id routes currently
succeed (200) against another tenant's subscription instead of 404ing.
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from varco_core.service.tenant import tenant_context


def _subscription(tenant_id: str):
    from varco_core.webhook.models import WebhookSubscription

    return WebhookSubscription(
        tenant_id=tenant_id,
        target_url="https://example.com/hook",
        event_patterns=["order.*"],
        active_secrets=["whsec_original"],
        status="ACTIVE",
        consecutive_failures=0,
        signer="standard_webhooks",
        custom_headers={},
    )


@pytest.fixture
def repo():
    from varco_core.webhook.base import InMemoryWebhookSubscriptionRepository

    return InMemoryWebhookSubscriptionRepository()


@pytest.fixture
def app_and_client(repo):
    from varco_fastapi.webhook import mount_webhook_admin

    app = FastAPI()
    mount_webhook_admin(app, repository=repo, acknowledge_bundled_admin=True)
    return app, TestClient(app)


async def _save_tenant_b_subscription(repo) -> str:
    saved = await repo.save(_subscription("tenant-b"))
    return str(saved.pk)


@pytest.mark.parametrize(
    "method,path_suffix",
    [
        ("GET", ""),
        ("PATCH", "/disable"),
        ("PATCH", "/enable"),
        ("POST", "/rotate-secret"),
        ("DELETE", ""),
    ],
)
async def test_cross_tenant_by_id_route_returns_404_not_403(
    repo, app_and_client, method, path_suffix
) -> None:
    """A tenant-A caller must not be able to distinguish "belongs to tenant
    B" from "does not exist" — both must be 404, never 403 (existence
    oracle)."""
    _app, client = app_and_client
    pk = await _save_tenant_b_subscription(repo)

    with tenant_context("tenant-a"):
        resp = client.request(method, f"/webhooks/subscriptions/{pk}{path_suffix}")

    assert resp.status_code == 404
    assert resp.status_code != 403


async def test_rotate_secret_on_cross_tenant_subscription_does_not_rotate(
    repo, app_and_client
) -> None:
    """The sharpest instance of the bug: rotate_secret must neither mutate
    nor reveal another tenant's secret."""
    _app, client = app_and_client
    saved = await repo.save(_subscription("tenant-b"))
    pk = str(saved.pk)
    original_secrets = list(saved.active_secrets)

    with tenant_context("tenant-a"):
        resp = client.post(f"/webhooks/subscriptions/{pk}/rotate-secret")

    assert resp.status_code == 404
    body = resp.json()
    assert "active_secrets" not in body

    reloaded = await repo.find_by_id(saved.pk)
    assert reloaded.active_secrets == original_secrets


async def test_list_subscriptions_ignores_spoofed_x_tenant_id_header(repo, app_and_client) -> None:
    """list_subscriptions must use the resolved ambient tenant, not a
    caller-supplied header — today it reads the header directly."""
    _app, client = app_and_client
    await repo.save(_subscription("tenant-a"))
    await repo.save(_subscription("tenant-b"))

    with tenant_context("tenant-a"):
        resp = client.get(
            "/webhooks/subscriptions",
            headers={"X-Tenant-Id": "tenant-b"},
        )

    assert resp.status_code == 200
    returned_tenants = {item["tenant_id"] for item in resp.json()}
    assert returned_tenants == {"tenant-a"}


async def test_same_tenant_by_id_route_still_succeeds(repo, app_and_client) -> None:
    """The fix must not break the same-tenant path."""
    _app, client = app_and_client
    saved = await repo.save(_subscription("tenant-a"))
    pk = str(saved.pk)

    with tenant_context("tenant-a"):
        resp = client.get(f"/webhooks/subscriptions/{pk}")

    assert resp.status_code == 200
    assert resp.json()["tenant_id"] == "tenant-a"
