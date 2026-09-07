"""
varco_fastapi.webhook.router
===============================
``build_webhook_router`` — the REST admin surface over a
``WebhookSubscriptionRepository`` (Plan 031 / D4d, Step 17-18, §D-D4-admin).

A plain ``APIRouter``, mirroring ``build_dlq_router``/``build_tenant_router``
— a standalone admin surface with hand-written JSON handlers, not a
``VarcoRouter`` generic-CRUD generator (a webhook subscription's admin
surface needs replay-through-``DlqRedriver`` and secret rotation, neither of
which fit generic CRUD).

Tenant scoping (updated by Plan 036 / S4 — see §D-S4-bola/§D-S4-role in
``technical_docs/features/admin-surface-tenancy.md``): every route is
scoped to the ambient ``current_tenant()`` via 033's
``assert_tenant_matches()``, **not** a client-supplied ``X-Tenant-Id``
header — the header used to be trusted directly, which was a confused-
deputy bug (BOLA). A caller holding ``cross_tenant_role`` (default
``"cross-tenant-admin"``) may still address another tenant's resource;
``ctx is None`` (``server_auth=None``, unauthenticated) can never hold it,
so an unauthenticated mount is strictly narrower than any authenticated
one. By-id routes 404 (not 403) on a cross-tenant hit — see
``_scoped_subscription_or_404``.

Thread safety:  N/A — stateless route handlers.
Async safety:   ✅ All handlers are ``async def``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from fastapi import APIRouter, Body, HTTPException, Request

if TYPE_CHECKING:
    from varco_core.event.redrive import DlqRedriver
    from varco_core.webhook.base import WebhookSubscriptionRepository

__all__ = ["build_webhook_router"]


def _subscription_to_dict(sub: Any, *, reveal_secrets: bool = False) -> dict[str, Any]:
    """
    Serialize a ``WebhookSubscription`` for an API response.

    ``active_secrets`` is NEVER included unless ``reveal_secrets=True`` —
    only the create/rotate-secret responses set that (the plan's Risks
    table: "Secrets leaked via admin API or logs" — mitigation is "never
    returned by any read endpoint").
    """
    body = {
        "pk": str(sub.pk),
        "tenant_id": sub.tenant_id,
        "target_url": sub.target_url,
        "event_patterns": sub.event_patterns,
        "status": sub.status,
        "consecutive_failures": sub.consecutive_failures,
        "signer": sub.signer,
        "custom_headers": sub.custom_headers,
    }
    if reveal_secrets:
        body["active_secrets"] = sub.active_secrets
    return body


async def _resolve_ctx(server_auth: Any, request: Request) -> Any:
    """
    Resolve ``server_auth`` (a plain callable dependency, not necessarily an
    ``AbstractServerAuth`` — see module docstring) to its context object, or
    ``None`` when no auth strategy is configured at all.

    Returns:
        The resolved context (whatever ``server_auth`` returns), or
        ``None`` when ``server_auth is None`` — the §D-S4-role fail-closed
        signal: ``ctx is None`` must make every cross-tenant check resolve
        ``allow_cross_tenant=False`` unconditionally.
    """
    if server_auth is None:
        return None

    import inspect

    if inspect.signature(server_auth).parameters:
        return (
            await server_auth(request)
            if inspect.iscoroutinefunction(server_auth)
            else server_auth(request)
        )
    return await server_auth() if inspect.iscoroutinefunction(server_auth) else server_auth()


async def _require_role(ctx: Any, admin_role: str) -> None:
    """
    Enforce ``admin_role`` against an already-resolved ``ctx``.

    Raises:
        HTTPException: 403 if the resolved context lacks ``admin_role`` in
            its ``roles`` attribute.
    """
    roles = getattr(ctx, "roles", [])
    if admin_role not in roles:
        raise HTTPException(status_code=403, detail=f"{admin_role!r} role required.")


def _allow_cross_tenant(ctx: Any, cross_tenant_role: str) -> bool:
    """
    §D-S4-role: ``ctx is None`` ⇒ ``False``, unconditionally — an
    unauthenticated mount can never cross a tenant boundary.
    """
    return ctx is not None and cross_tenant_role in getattr(ctx, "roles", [])


async def _scoped_subscription_or_404(
    pk: UUID,
    repository: WebhookSubscriptionRepository,
    ctx: Any,
    cross_tenant_role: str,
) -> Any:
    """
    Fetch a subscription by ``pk`` and 404 when it does not exist **or**
    belongs to another tenant (§D-S4-bola) — the single mapping point every
    by-id route goes through, so the 404-not-403 remap cannot drift
    route-to-route.

    DESIGN: fetch-then-compare, 404 on either outcome
        ✅ A cross-tenant probe cannot distinguish "exists elsewhere" from
           "does not exist" — no existence oracle (brief 006 §2).
        ❌ The row is still read from the database before it is rejected.
           Accepted — no data crosses the boundary; 037's RLS closes even
           the read when enabled.
    """
    from varco_core.tenancy import CrossTenantAccessError, assert_tenant_matches

    sub = await repository.find_by_id(pk)
    if sub is None:
        raise HTTPException(status_code=404, detail="Webhook subscription not found.")
    try:
        assert_tenant_matches(
            sub.tenant_id, allow_cross_tenant=_allow_cross_tenant(ctx, cross_tenant_role)
        )
    except CrossTenantAccessError:
        raise HTTPException(status_code=404, detail="Webhook subscription not found.") from None
    return sub


def build_webhook_router(
    repository: WebhookSubscriptionRepository,
    *,
    redriver: DlqRedriver | None = None,
    server_auth: Any | None = None,
    admin_role: str = "webhook-admin",
    cross_tenant_role: str = "cross-tenant-admin",
    prefix: str = "/webhooks",
) -> APIRouter:
    """
    Build the webhook subscription admin ``APIRouter``.

    Args:
        repository:  The ``WebhookSubscriptionRepository`` to administer.
        redriver:    A ``DlqRedriver`` bound to the DLQ webhooks land in on
                     exhaustion. ``None`` (default) — the replay route is
                     not registered at all (same "absent capability, absent
                     route" rule as ``build_dlq_router``).
        server_auth: Optional callable resolving to an object exposing
                     ``.roles`` — enforced via ``admin_role`` on every
                     route when given. ``None`` mounts unauthenticated.
        admin_role:  Role required on every route (when ``server_auth`` is
                     given). Defaults to ``"webhook-admin"``.
        cross_tenant_role: Role required to address a subscription outside
                     the resolved (``current_tenant()``) tenant — §D-S4-role.
                     Never granted when ``server_auth is None`` (``ctx is
                     None`` ⇒ ``allow_cross_tenant=False`` unconditionally).
        prefix:      URL prefix. Defaults to ``"/webhooks"``.

    Routes:
        GET    {prefix}/subscriptions               list, scoped to the
                                                      resolved tenant
        POST   {prefix}/subscriptions                create — response
                                                      includes secrets ONCE
        GET    {prefix}/subscriptions/{pk}            404 on cross-tenant
        PATCH  {prefix}/subscriptions/{pk}/disable    404 on cross-tenant
        PATCH  {prefix}/subscriptions/{pk}/enable     404 on cross-tenant
        POST   {prefix}/subscriptions/{pk}/rotate-secret  404 on cross-tenant;
                                                      response includes the
                                                      new secret ONCE
        DELETE {prefix}/subscriptions/{pk}            404 on cross-tenant
        POST   {prefix}/deliveries/{entry_id}/replay      only when
                                                      ``redriver`` is given
    """
    router = APIRouter(prefix=prefix, tags=["webhook-admin"])

    async def _enforce(request: Request) -> Any:
        """Resolve ``ctx`` and (when ``server_auth`` is configured) enforce
        ``admin_role``. Always returns the resolved ``ctx`` (``None`` when
        unauthenticated) so callers can compute cross-tenant eligibility."""
        ctx = await _resolve_ctx(server_auth, request)
        if server_auth is not None:
            await _require_role(ctx, admin_role)
        return ctx

    @router.get("/subscriptions")
    async def list_subscriptions(request: Request) -> list[dict[str, Any]]:
        from varco_core.tenancy import CrossTenantAccessError, assert_tenant_matches

        ctx = await _enforce(request)
        try:
            tenant_id = assert_tenant_matches(
                None, allow_cross_tenant=_allow_cross_tenant(ctx, cross_tenant_role)
            )
        except CrossTenantAccessError:
            # No ambient tenant resolved — cross-tenant listing must be an
            # explicit, already-authenticated admin action, never the
            # default path a tenant-scoped caller would hit.
            return []
        subs = await repository.find_by_tenant(tenant_id)
        return [_subscription_to_dict(s) for s in subs]

    @router.post("/subscriptions", status_code=201)
    async def create_subscription(
        request: Request, body: dict[str, Any] = Body(...)
    ) -> dict[str, Any]:
        from varco_core.tenancy import CrossTenantAccessError, assert_tenant_matches
        from varco_core.webhook.models import WebhookSubscription

        ctx = await _enforce(request)
        try:
            tenant_id = assert_tenant_matches(
                body.get("tenant_id"),
                allow_cross_tenant=_allow_cross_tenant(ctx, cross_tenant_role),
            )
        except CrossTenantAccessError:
            raise HTTPException(
                status_code=403, detail="Cross-tenant subscription creation denied."
            ) from None

        sub = WebhookSubscription(
            tenant_id=tenant_id,
            target_url=body["target_url"],
            event_patterns=body.get("event_patterns", []),
            active_secrets=body.get("active_secrets") or [f"whsec_{UUID(int=0).hex}"],
            status="ACTIVE",
            consecutive_failures=0,
            signer=body.get("signer", "standard_webhooks"),
            custom_headers=body.get("custom_headers", {}),
        )
        saved = await repository.save(sub)
        return _subscription_to_dict(saved, reveal_secrets=True)

    @router.get("/subscriptions/{pk}")
    async def get_subscription(pk: UUID, request: Request) -> dict[str, Any]:
        ctx = await _enforce(request)
        sub = await _scoped_subscription_or_404(pk, repository, ctx, cross_tenant_role)
        return _subscription_to_dict(sub)

    @router.patch("/subscriptions/{pk}/disable")
    async def disable_subscription(pk: UUID, request: Request) -> dict[str, Any]:
        ctx = await _enforce(request)
        sub = await _scoped_subscription_or_404(pk, repository, ctx, cross_tenant_role)
        sub.status = "DISABLED"
        saved = await repository.save(sub)
        return _subscription_to_dict(saved)

    @router.patch("/subscriptions/{pk}/enable")
    async def enable_subscription(pk: UUID, request: Request) -> dict[str, Any]:
        ctx = await _enforce(request)
        sub = await _scoped_subscription_or_404(pk, repository, ctx, cross_tenant_role)
        sub.status = "ACTIVE"
        sub.consecutive_failures = 0
        saved = await repository.save(sub)
        return _subscription_to_dict(saved)

    @router.post("/subscriptions/{pk}/rotate-secret")
    async def rotate_secret(pk: UUID, request: Request) -> dict[str, Any]:
        import secrets as _secrets

        ctx = await _enforce(request)
        sub = await _scoped_subscription_or_404(pk, repository, ctx, cross_tenant_role)
        new_secret = f"whsec_{_secrets.token_urlsafe(32)}"
        # Keep the newest-last convention (§D-D4-signing rotation) — old
        # secret(s) remain active until an operator explicitly prunes them.
        sub.active_secrets = [*sub.active_secrets, new_secret]
        saved = await repository.save(sub)
        return _subscription_to_dict(saved, reveal_secrets=True)

    @router.delete("/subscriptions/{pk}")
    async def delete_subscription(pk: UUID, request: Request) -> dict[str, Any]:
        ctx = await _enforce(request)
        await _scoped_subscription_or_404(pk, repository, ctx, cross_tenant_role)
        await repository.delete(pk)
        return {"deleted": True}

    if redriver is not None:

        @router.post("/deliveries/{entry_id}/replay")
        async def replay_delivery(
            entry_id: UUID, request: Request, dry_run: bool = False
        ) -> dict[str, Any]:
            await _enforce(request)
            from varco_core.event.redrive import DeadLetterNotAddressable

            try:
                outcome = await redriver.redrive(entry_id, dry_run=dry_run)
            except DeadLetterNotAddressable as exc:
                raise HTTPException(status_code=501, detail=str(exc)) from exc
            return {
                "entry_id": str(outcome.entry_id),
                "published": outcome.published,
                "acked": outcome.acked,
                "error": outcome.error,
            }

    return router
