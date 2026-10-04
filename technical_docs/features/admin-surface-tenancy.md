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
line of defence (a role). A separate provisioning-authority model (flip-list row 14 in
`technical_docs/features/security-posture.md`) is designed below, not built.

### The provisioning-authority model (designed in Plan 055, not built)

**The problem.** `POST /tenants`, `PATCH … suspend|resume`, `DELETE` and `migrate` sit behind one
role string (`"tenant-admin"`) on whatever token *any* issuer the `TrustedIssuerRegistry` trusts
chose to mint — including the tenant-user IdP. Membership binding is a category error here
(provisioning *creates* the scope), so the fix is about **who may hold provisioning power** and
**what each holder may do**, not which tenant they belong to.

| Option | ✅ | ❌ |
|---|---|---|
| **(A) Per-operation capability scopes** — every route requires a scope (`tenant:provision`, `tenant:suspend`, `tenant:deprovision`, `tenant:migrate`, `tenant:read`) via `require_scopes()` on top of `admin_role` | Least privilege per verb (a CI job that provisions cannot delete); brief 006 §3 *"scope it"*; shipped `RouteGuard` machinery; audit-visible through `AuditingAuthorizer` | Still purely token-borne — any trusted issuer, including the tenant-user IdP, can mint the scope. Necessary, not sufficient |
| **(B) A signed provisioning `TokenProfile`** — `TokenProfile(name="tenant-provisioning", issuers={<control-plane issuer>}, audiences={"varco:tenancy-admin"}, token_type="provisioning", required_claims={"actor"})`, every route gated by `require_token_profile("tenant-provisioning")` | Binds authority to a **dedicated issuer + audience** — an ordinary user token cannot match even if it carries the role; the `act` claim (RFC 8693) makes the human behind the automation visible, *"logged at issuance AND use"* (CVE-2025-55241's lesson); shipped machinery (`TokenProfile`, `require_token_profile`, `JwtAuthority`); declarative, so posture can report it | Needs an internal issuer or an IdP that does token exchange — real application work; the profile's `implied_roles` must **not** grant `tenant-admin` (a profile granting the role it gates is circular) |
| **(C) A control-plane tenant with membership** — a reserved tenant id; callers must be members of it (`AbstractTenantMembership.check(ctx, CONTROL_PLANE)`) | Reuses brief 006 §2's membership rule uniformly; fail-closed since FLIP-7 | Leaks a varco-reserved id into every IdP's `tenants` claim; collides with `reserved_labels`/subdomain semantics and with catalogs; minted by the same IdP as user tokens, so it does not separate issuers the way (B) does |
| (D) Status quo + S11 audit + `posture.tenant_admin_mounted` | Nothing to build | One line of defence. Rejected as the end state |

**Recommendation: (B) as the authority gate, layered with (A) as the capability gate, keeping
`admin_role` as the third barrier** — three independent statements per request: *the token is a
provisioning token* (issuer/audience/type), *it may perform this verb* (scope), *its subject
holds the role* (role); every decision through `AuditingAuthorizer`. (C) is the fallback shape
for a platform whose IdP cannot mint a second profile — weaker, but better than (D).

Sketch the future plan will refine: `mount_tenant_admin(app, control_service, *,
acknowledge_bundled_admin, server_auth, admin_role="tenant-admin", provisioning_profile: str |
None = None, operation_scopes: Mapping[str, str] | None = None, …)`; `provisioning_profile=None`
keeps today's role-only guard and emits a new **`posture.tenant_admin_role_only` (WARN)** — so
under the 4.0 derived `refuse` a production mount without a profile needs an explicit
suppression. No `TenantAdminModule` — RD-9 (never a `create_varco_app` kwarg/module for a
privileged mount) stands.

**Implementation is its own future plan; BACKLOG `FLIP-14` carries it as `designed`.** Do not
add a tenant guard to `mount_tenant_admin`, and do not build this model outside that plan.

## `cross_tenant_role` — one kwarg, fail-closed with no auth at all (§D-S4-role)

Every guarded mount and router gains `cross_tenant_role: str = "cross-tenant-admin"`:

```python
mount_webhook_admin(app, repository=repo, server_auth=my_auth, cross_tenant_role="cross-tenant-admin")
mount_reliability_admin(app, dlq=my_dlq, audit_repo=my_audit_repo, server_auth=my_auth,
                         cross_tenant_role="cross-tenant-admin")
```

The guard resolves `allow_cross_tenant = ctx is not None and cross_tenant_role in ctx.roles`.
**`ctx is None` ⇒ `False`, unconditionally** — a caller with no context can never cross a tenant
boundary, in either direction. Since 4.0 (Plan 054 / FLIP-3/4) this is reachable only through a
`server_auth` that returns `None` (an anonymous-allowing strategy) — no mount can be
unauthenticated any more (`server_auth=None` is a `ValueError`), and such a caller also fails the
`admin_role` check (403) before the cross-tenant question is asked. This is a distinct role from the surface's
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

**`admin_role` is enforced on every route (4.0, Plan 054 / FLIP-3).** Both routers install one
router-level dependency (`admin_guard()` in `_tenant_scope.py`) that resolves `server_auth` once
per request, 403s with `'reliability-admin' role required.` when the role is absent (the webhook
router's text), and stashes the ctx on `request.state` so the per-route cross-tenant reads never
call `server_auth` a second time. Before 4.0 `server_auth` was resolved on only 3 of 8 DLQ routes
and 3 of 6 audit routes, and `admin_role` was enforced on none unless the caller passed their own
`dependencies=` — which are now **additive** (an IP allowlist, an mTLS check), never the only gate.

## Pitfalls

| Pitfall | Why it happens | Fix |
|---|---|---|
| A cross-tenant probe against a webhook by-id route gets 404, not 403 | §D-S4-bola's deliberate existence-oracle closure — the routes already 404 on a genuine miss, so the two cases are indistinguishable by construction | Not a bug — check the server-side log (with `correlation_id`) to distinguish a genuine miss from a cross-tenant hit while debugging |
| An omitted `tenant_id` on `mount_reliability_admin` no longer means "every tenant" | §D-S4-scope reuses 033's `None → current_tenant()` semantics | Grant `cross_tenant_role` to restore an unscoped sweep exactly, or pass `tenant_id=` explicitly |
| `mount_*(server_auth=None)` / a builder with `server_auth=None` | `ValueError` since 4.0 (Plan 054 / FLIP-3/4) — omitting the keyword is a `TypeError`; there is no acknowledgement kwarg | Pass a strategy; behind a gateway that already verified the caller, `PassthroughAuth(acknowledge_unverified=True)` (expect `auth.passthrough_bound`) |
| A reliability admin token without `admin_role` now gets 403 | `admin_role` is enforced on every reliability route since 4.0 — a `dependencies=` that used to be the only gate is now additive | Grant `admin_role` (default `reliability-admin`) on the admin token |
