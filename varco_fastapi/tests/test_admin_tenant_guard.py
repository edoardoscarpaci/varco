"""
Plan 036 (S4b) / Phase 1, Step 12 — red-mode tests for the cross-tenant
guard proper on the webhook and reliability admin surfaces (§D-S4-role,
§D-S4-scope, §D-S4-control).

Depends on 033's ``assert_tenant_matches()``/``CrossTenantAccessError``,
which already ship (``varco_core.tenancy``). None of these kwargs
(``cross_tenant_role=``) or behaviours exist yet on
``build_webhook_router``/``mount_webhook_admin``/``build_dlq_router``/
``build_audit_router``/``mount_reliability_admin`` — every test below is
expected to fail (``TypeError`` on the new kwarg, or an assertion on the
still-permissive current behaviour).
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from varco_core.service.tenant import tenant_context


class _CtxAuth:
    """A ``server_auth`` stand-in resolving to a fixed set of roles."""

    def __init__(self, roles: list[str]) -> None:
        self._roles = roles

    def __call__(self):
        class _Ctx:
            roles = self._roles

        _Ctx.roles = self._roles
        return _Ctx()


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
def webhook_repo():
    from varco_core.webhook.base import InMemoryWebhookSubscriptionRepository

    return InMemoryWebhookSubscriptionRepository()


# ── §D-S4-role: build_webhook_router / mount_webhook_admin accept cross_tenant_role= ──


def test_build_webhook_router_accepts_cross_tenant_role_kwarg(webhook_repo) -> None:
    from varco_fastapi.webhook.router import build_webhook_router

    # Must not raise TypeError — the plan adds this kwarg in Phase 1.
    build_webhook_router(webhook_repo, cross_tenant_role="cross-tenant-admin")


async def test_create_subscription_with_foreign_body_tenant_id_is_rejected(webhook_repo) -> None:
    """§D-S4-role Step 8: creating a subscription for a tenant other than
    the resolved one, with no cross-tenant role, must be rejected — not
    silently created (today's behaviour: always 201)."""
    from varco_fastapi.webhook import mount_webhook_admin

    app = FastAPI()
    mount_webhook_admin(app, repository=webhook_repo, acknowledge_bundled_admin=True)
    client = TestClient(app)

    with tenant_context("tenant-a"):
        resp = client.post(
            "/webhooks/subscriptions",
            json={"tenant_id": "tenant-b", "target_url": "https://evil.example.com/hook"},
        )

    assert resp.status_code == 403


async def test_caller_with_cross_tenant_role_may_read_another_tenants_subscription(
    webhook_repo,
) -> None:
    """A caller holding ``cross-tenant-admin`` is the deliberate escape
    hatch — the by-id route must succeed for them where an ordinary caller
    gets 404."""
    from varco_fastapi.webhook import mount_webhook_admin

    saved = await webhook_repo.save(_subscription("tenant-b"))
    pk = str(saved.pk)

    app = FastAPI()
    mount_webhook_admin(
        app,
        repository=webhook_repo,
        acknowledge_bundled_admin=True,
        server_auth=_CtxAuth(["webhook-admin", "cross-tenant-admin"]),
        admin_role="webhook-admin",
        cross_tenant_role="cross-tenant-admin",
    )
    client = TestClient(app)

    with tenant_context("tenant-a"):
        resp = client.get(f"/webhooks/subscriptions/{pk}")

    assert resp.status_code == 200
    assert resp.json()["tenant_id"] == "tenant-b"


async def test_unauthenticated_mount_never_grants_cross_tenant_access(webhook_repo) -> None:
    """§D-S4-role: ``ctx is None`` (``server_auth=None``) must resolve
    ``allow_cross_tenant=False`` unconditionally — the unauthenticated mount
    is strictly narrower than an authenticated one, never wider."""
    from varco_fastapi.webhook import mount_webhook_admin

    saved = await webhook_repo.save(_subscription("tenant-b"))
    pk = str(saved.pk)

    app = FastAPI()
    mount_webhook_admin(
        app,
        repository=webhook_repo,
        acknowledge_bundled_admin=True,
        server_auth=None,
        cross_tenant_role="cross-tenant-admin",
    )
    client = TestClient(app)

    with tenant_context("tenant-a"):
        resp = client.get(f"/webhooks/subscriptions/{pk}")

    assert resp.status_code == 404


# ── §D-S4-scope: reliability admin — absent tenant_id means "mine" ──


@pytest.fixture
def reliability_app_and_client():
    from varco_core.event.dlq import InMemoryDeadLetterQueue
    from varco_fastapi.admin.mount import mount_reliability_admin

    dlq = InMemoryDeadLetterQueue()
    app = FastAPI()
    mount_reliability_admin(
        app,
        dlq=dlq,
        acknowledge_bundled_admin=True,
        server_auth=_CtxAuth(["reliability-admin"]),
        cross_tenant_role="cross-tenant-admin",
    )
    return dlq, TestClient(app)


def test_build_dlq_router_accepts_cross_tenant_role_kwarg() -> None:
    from varco_core.event.dlq import InMemoryDeadLetterQueue
    from varco_fastapi.admin.dlq_router import build_dlq_router

    build_dlq_router(InMemoryDeadLetterQueue(), cross_tenant_role="cross-tenant-admin")


async def test_delete_where_with_omitted_tenant_id_scopes_to_callers_tenant(
    reliability_app_and_client,
) -> None:
    """§D-S4-scope: an omitted ``tenant_id`` must mean "my tenant", not
    "every tenant" — today ``delete_where(tenant_id=None)`` sweeps
    unscoped."""
    from datetime import UTC, datetime
    from uuid import uuid4

    from varco_core.event.dlq import DeadLetterEntry, DeadLetterSource

    dlq, client = reliability_app_and_client

    async def _push(tenant_id: str) -> None:
        await dlq.push(
            DeadLetterEntry(
                entry_id=uuid4(),
                channel="orders",
                handler_name="h",
                source=DeadLetterSource.CONSUMER,
                error_type="ValueError",
                error_message="boom",
                payload={},
                attempts=1,
                first_failed_at=datetime.now(UTC),
                last_failed_at=datetime.now(UTC),
                tenant_id=tenant_id,
            )
        )

    await _push("tenant-a")
    await _push("tenant-b")

    with tenant_context("tenant-a"):
        resp = client.request("DELETE", "/reliability/dlq/entries")

    assert resp.status_code == 200
    remaining = await dlq.list_entries(limit=50)
    remaining_tenants = {e.tenant_id for e in remaining}
    assert remaining_tenants == {"tenant-b"}


async def test_delete_where_with_omitted_tenant_id_and_cross_tenant_role_sweeps_all(
    reliability_app_and_client,
) -> None:
    """Same route, but the caller now holds the cross-tenant role — the
    unscoped sweep is restored exactly, per the upgrade note's escape."""
    from datetime import UTC, datetime
    from uuid import uuid4

    from varco_core.event.dlq import DeadLetterEntry, DeadLetterSource

    dlq, _client = reliability_app_and_client

    async def _push(tenant_id: str) -> None:
        await dlq.push(
            DeadLetterEntry(
                entry_id=uuid4(),
                channel="orders",
                handler_name="h",
                source=DeadLetterSource.CONSUMER,
                error_type="ValueError",
                error_message="boom",
                payload={},
                attempts=1,
                first_failed_at=datetime.now(UTC),
                last_failed_at=datetime.now(UTC),
                tenant_id=tenant_id,
            )
        )

    await _push("tenant-a")
    await _push("tenant-b")

    from varco_fastapi.admin.mount import mount_reliability_admin

    app = FastAPI()
    mount_reliability_admin(
        app,
        dlq=dlq,
        acknowledge_bundled_admin=True,
        server_auth=_CtxAuth(["reliability-admin", "cross-tenant-admin"]),
        cross_tenant_role="cross-tenant-admin",
    )
    client = TestClient(app)

    with tenant_context("tenant-a"):
        # channel="orders" is the required non-tenant predicate —
        # InMemoryDeadLetterQueue.delete_where() refuses a call with zero
        # predicates at all (older_than/source/channel/tenant_id), a
        # pre-existing, unrelated guard against an accidental full sweep.
        # Both pushed entries share this channel, so this remains an
        # unscoped-by-tenant sweep.
        resp = client.request("DELETE", "/reliability/dlq/entries?channel=orders")

    assert resp.status_code == 200
    remaining = await dlq.list_entries(limit=50)
    assert remaining == []


async def test_delete_where_with_no_tenant_context_at_all_raises_403(
    reliability_app_and_client,
) -> None:
    """Edge case: no tenant context and no cross-tenant role — 033's
    ``assert_tenant_matches`` refuses; this router must map that to 403,
    not a 500."""
    _dlq, client = reliability_app_and_client

    resp = client.request("DELETE", "/reliability/dlq/entries")

    assert resp.status_code == 403


# ── §D-S4-control: mount_tenant_admin is explicitly untouched ──


def test_mount_tenant_admin_does_not_accept_cross_tenant_role_kwarg() -> None:
    """§D-S4-control: the control plane is deliberately exempt. If a future
    edit adds a tenant guard here without revisiting the documented
    decision, this test starts failing (TypeError disappears) and the
    reviewer must re-read §D-S4-control before accepting it."""
    from varco_fastapi.tenancy.mount import mount_tenant_admin

    app = FastAPI()

    with pytest.raises(TypeError):
        mount_tenant_admin(
            app,
            control_service=object(),
            acknowledge_bundled_admin=True,
            server_auth=_CtxAuth(["tenant-admin"]),
            cross_tenant_role="cross-tenant-admin",
        )
