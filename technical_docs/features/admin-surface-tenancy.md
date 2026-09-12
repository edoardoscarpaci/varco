# Admin surface tenancy — cross-tenant write guard (S4)

Plan 036 (3.2 security release, BACKLOG row **S4**, 🔴 must). Research brief backing this
feature: `design/research/006-multi-tenant-identity-and-hardening.md` §2 (BOLA / confused
deputy).

## The bug this closes

Before this plan, three bundled admin surfaces (`mount_webhook_admin`, `mount_reliability_admin`,
`mount_tenant_admin`) let an authenticated admin of one tenant reach another tenant's data. The
sharpest instance: five webhook-admin routes fetched a subscription by `pk` with **no tenant
check at all** — `get_subscription`, `disable_subscription`, `enable_subscription`,
`rotate_secret`, `delete_subscription`. `rotate_secret` then returned the new secret in the
response body. A `webhook-admin` of tenant A who knew or guessed a subscription UUID could rotate
tenant B's signing secret **and read the replacement** — textbook OWASP API1:2023 BOLA (brief
006 §2), and it outranked the two issues the backlog originally named
(header-trusted `X-Tenant-Id`, a body-supplied `tenant_id` on create).

## The three surfaces are not symmetric

This plan narrows the backlog's "applies to all three `mount_*` surfaces" wording to **two**, in
writing, because the third is structurally different.

| Surface | Guard | Why |
|---|---|---|
| `mount_webhook_admin` | Yes — every by-id route, `list_subscriptions`, `create_subscription` | Every route addresses one tenant's own resource |
| `mount_reliability_admin` (DLQ + audit routers) | Yes — every route taking a `tenant_id` | An omitted `tenant_id` is a query parameter default, not a mismatch (see below) |
| `mount_tenant_admin` | **No** — deliberately exempt | Provisions/suspends/deletes *tenants themselves*; every route addresses a tenant that is by definition not the caller's own (§D-S4-control) |

### §D-S4-control: `mount_tenant_admin` is exempt, by argument, not by oversight

`build_tenant_router`'s routes (`POST /tenants`, `GET/PATCH/DELETE /tenants/{tenant_id}`,
`POST /tenants/{id}/migrate`) are the tenant control plane — provisioning is not tenant-scoped,
it is the operation that *creates* the scope. Binding it to `current_tenant()` would make
`POST /tenants` able to provision only the tenant that already exists, which is nonsensical.

Instead, in 3.2 the mount keeps what it already had — a **mandatory** `server_auth`
(`build_tenant_router` raises `ValueError` on `None`) and a non-generic `tenant-admin` role, both
already stricter than the other two surfaces — and gains exactly one thing: an S9 finding,
`posture.tenant_admin_mounted` (`INFO`), so the mount is visible in the security posture report.
This is a real gap, named openly: the most powerful surface in the platform still has a single
line of defence (a role). A separate provisioning-authority model is a named 4.0 candidate
(flip-list row 14 in `technical_docs/features/security-posture.md`), not a guard this plan builds.

## `cross_tenant_role` — one kwarg, fail-closed with no auth at all (§D-S4-role)

Every guarded mount and router gains `cross_tenant_role: str = "cross-tenant-admin"`:

```python
mount_webhook_admin(app, repository=repo, server_auth=my_auth, cross_tenant_role="cross-tenant-admin")
mount_reliability_admin(app, dlq=my_dlq, audit_repo=my_audit_repo, server_auth=my_auth,
                         cross_tenant_role="cross-tenant-admin")
```

The guard resolves `allow_cross_tenant = ctx is not None and cross_tenant_role in ctx.roles`.
**`ctx is None` ⇒ `False`, unconditionally** — an unauthenticated mount (`server_auth=None`)
can never cross a tenant boundary, in either direction. This is a distinct role from the surface's
`admin_role` (`"webhook-admin"` / `"reliability-admin"` / `"tenant-admin"`) deliberately: the
existing role answers *may you use this surface*, crossing a tenant boundary is a strictly
stronger permission that must be separately grantable — the same precedent
`build_tenant_router` already set by refusing to reuse a generic admin role.

## §D-S4-bola: 404, not 403, on the webhook by-id routes

Every webhook by-id route (`get_subscription`, `disable_subscription`, `enable_subscription`,
`rotate_secret`, `delete_subscription`) goes through one shared helper,
`_scoped_subscription_or_404()` (`varco_fastapi/varco_fastapi/webhook/router.py`) — fetch by
`pk`, then compare the row's `tenant_id` against the guard's verdict via 033's
`assert_tenant_matches()`, mapping `CrossTenantAccessError` to **404**, in one `except` block, so
the 404-not-403 remap cannot drift route-to-route.

**Why 404, not 403**: a 403 confirms the UUID exists in another tenant — an existence oracle. The
routes already 404 on a genuine miss, so making a cross-tenant hit indistinguishable from an
actual absence removes the oracle by construction (brief 006 §2's "unpredictable resource IDs"
defense, applied at the response-shape level rather than the ID-generation level). The row is
still read from the database before it is rejected — no data crosses the boundary, but the read
itself happens; 037's Row-Level Security is the layer that makes even the read impossible when
enabled.

`list_subscriptions` and `create_subscription` are guarded the same way `033`'s
`assert_tenant_matches()` is used everywhere else on this surface, but they map
`CrossTenantAccessError` differently — `list_subscriptions` returns an empty list (no ambient
tenant, no cross-tenant role ⇒ nothing to list, not an error), and `create_subscription` maps to
**403** (a write against an unauthorized tenant is refused outright, there is no existence-oracle
concern for a `POST`).

## §D-S4-scope: the reliability admin — an absent `tenant_id` means "mine"

`build_dlq_router`/`build_audit_router` take `tenant_id: str | None = None` as an optional query
parameter on `list_entries`, `delete_where`, `redrive_batch`, `verify_chain`. Before this plan,
omitting it meant *all tenants* — so `DELETE /reliability/dlq/entries` with every parameter
omitted was an unscoped cross-tenant delete, reachable by a caller who simply left a query
parameter off.

Every one of those routes now goes through `resolve_tenant_scope()`
(`varco_fastapi/varco_fastapi/admin/_tenant_scope.py` — shared by both routers, so the mapping
exists in exactly one place) which reuses 033's own `None` semantics: `assert_tenant_matches(None,
...)` resolves to `current_tenant()`, or raises if no tenant context exists at all. **An omitted
`tenant_id` now means "the caller's own tenant"**, and an unscoped, cross-tenant sweep requires
`cross_tenant_role`. `CrossTenantAccessError` maps to **403** on this surface — the opposite of
the webhook admin's 404 remap, because there is no existence-oracle concern here (the caller is
not probing for a specific resource's presence).

## Pitfalls

| Pitfall | Why it happens | Fix |
|---|---|---|
| A cross-tenant probe against a webhook by-id route gets 404, not 403 | §D-S4-bola's deliberate existence-oracle closure — the routes already 404 on a genuine miss, so the two cases are indistinguishable by construction | Not a bug — check the server-side log (with `correlation_id`) to distinguish a genuine miss from a cross-tenant hit while debugging |
| An omitted `tenant_id` on `mount_reliability_admin` no longer means "every tenant" | §D-S4-scope reuses 033's `None → current_tenant()` semantics | Grant `cross_tenant_role` to restore an unscoped sweep exactly, or pass `tenant_id=` explicitly |
| Mounting any of the three unauthenticated (`server_auth=None`) and expecting cross-tenant access to still work | §D-S4-role: `ctx is None` ⇒ `allow_cross_tenant=False`, unconditionally — there is no way to cross a tenant boundary through an unauthenticated mount | Pass `server_auth=` and grant `cross_tenant_role` on the token that needs it |
