# Plan 036 — Authorization surface & posture: cross-tenant admin guard (S4), `SecurityPosture` preflight (S9), authz-decision audit (S11)

Covers BACKLOG 3.2 rows **S4** (🔴 must, S — *cross-tenant write guard on the three admin
surfaces*), **S9** (🟡 should, S–M — *`SecurityPosture` startup preflight*) and **S11**
(🟡 should, S–M — *authorization-decision audit*), and answers the cycle's **open question 1**
(§D-S9-oq1).

**Research brief backing this plan:**
`design/research/006-multi-tenant-identity-and-hardening.md` — §2 for S4 (BOLA / confused deputy),
§3 for S11 (audit must log both principal and actor; CVE-2025-55241). Every externally-grounded
claim below cites it by section.

## Scope and siblings

The **last** of five plans in the 3.2 security release, and the only one that is genuinely gated.
Per the index (`plans/000-index-3-2-security-release.md:82-84`), **every arrow into 036 is a seam
obligation on the source plan**: each of 033/034/035/037 defines and exports its check; none of
them builds the preflight. That is this plan's sole ownership, and it is why this plan is written
and built last.

| Plan | Rows | Boundary with this plan |
|---|---|---|
| 033 | S6, S5, S16 | **Hard edge.** Owns the resolved tenant. This plan consumes `assert_tenant_matches()` / `CrossTenantAccessError` / `current_tenant_provenance()` for S4 and `inspect_tenant_provenance()` for S9. It writes **no second tenant guard** |
| 034 | S1, S2, S13, S14 | **Hard edge.** Consumes `inspect_auth_posture()` and `inspect_revocation_posture()` for S9. Owns nothing this plan changes |
| 035 | S3, S7, S8, S10 | **Soft edge.** Consumes `inspect_http_edge()` for S9; degrades to `NOT_ASSESSED` if 035 slips |
| 037 | S12, S15 | **Soft edge.** Consumes `inspect_rls_posture()` for S9; ships standalone-usable, so 037 needs nothing from here |

**This plan owns the consolidated 4.0 flip list** (§D-S9-flip). The definition of done requires it
written down in one place; each sibling wrote its own rows and this is where they are gathered.

## Goal

Three things an operator can rely on that they cannot today:

1. **An admin surface cannot be used to cross a tenant boundary** — not by a body-supplied
   `tenant_id`, not by a header the middleware never validated, and not by guessing a resource
   UUID. Today all three work (§D-S4-bola is the sharpest of them, and the backlog does not name
   it).
2. **A varco app says out loud, at startup, which of its security defaults are still the
   permissive ones** — in one report, aggregated from the checks the other four plans export,
   with a stable id per finding so a knowingly-accepted one can be suppressed and the suppression
   itself is auditable.
3. **An authorization denial, and every delegated allow, lands in the audit trail** with both
   principal and actor — the exact property whose absence was the Entra actor-token attack vector
   (brief 006 §3, CVE-2025-55241).

## Non-goals

- **The preflight never fails startup in 3.2.** The locked blast-radius rule (`BACKLOG.md:54`) is
  explicit: defaults needing real application work get a *loud warn-only preflight* in 3.2 and
  flip in 4.0. An opt-in `enforce` mode exists (§D-S9-enforce) and is **off by default**.
- **No second tenant guard, no second posture inspector, no second audit trail.** S4 consumes
  033's `assert_tenant_matches()`; S9 aggregates four exported inspectors; S11 emits into the
  existing `varco_core.service.audit` path. If any of those three feels like it needs a
  reimplementation here, the seam is wrong and the fix belongs in the source plan.
- **No re-derivation of a sibling's checks.** S9 never reads `ApiKeyAuth._keys`, never walks
  `app.user_middleware`, never queries `pg_class`. It calls the four exported functions and
  formats what comes back. Reaching past a seam is the failure mode this ordering exists to
  prevent.
- **`mount_tenant_admin` is deliberately NOT bound to the resolved tenant** — it is the control
  plane, and provisioning is inherently cross-tenant. This is a documented deviation from the
  backlog's "applies to all three `mount_*` surfaces" wording, argued in §D-S4-control.
- **No new authorizer, and no change to `AbstractAuthorizer`'s abstract signature.** S11 is a
  decorator over whatever authorizer is bound (§D-S11-shape).
- **No `create_varco_app` kwarg for any admin mount**, and no env var that mounts one — RD-9 is
  unchanged. The preflight is a lifecycle component, which is a different thing.
- **No new conformance module.** No new implementation of one of the eight
  `testkit/varco_conformance` ABCs appears here — `AuditingAuthorizer` implements
  `AbstractAuthorizer`, which has no suite and is not one of the eight. Stated so the absence is a
  recorded decision, not an oversight; §D-S11-conformance carries the `COVERAGE.md` verdict.

---

## Design

### What already exists — verified against source while writing this plan

| Fact | Anchor |
|---|---|
| `AbstractAuthorizer.authorize(ctx, action, resource) -> None`, must raise on denial, must deny by default | `varco_core/varco_core/auth/base.py:425`, abstract method at `:495` |
| `BaseAuthorizer` is the permissive fallback, a `@Singleton(priority=-(2**31))` that allows everything unconditionally | `varco_core/varco_core/auth/authorizer.py:65-66`, `:96` |
| `authorize()` is called **from the service layer**, never from HTTP middleware — 11 call sites in `service/base.py` (`:782,815,877,989,1047,1095,1176,1272`), `service/bulk.py:163,252`, `service/soft_delete.py:256,309` | grep, listed in §D-S11-shape |
| `AuditEntry` (frozen), `AuditRepository` (ABC), `AuditLogMixin`, `AuditConsumer` all exist | `varco_core/varco_core/service/audit.py:100,238,450,612` |
| `mount_tenant_admin` **requires** `server_auth` — `build_tenant_router()` raises `ValueError` on `None` | `varco_fastapi/varco_fastapi/tenancy/mount.py:56`, `tenancy/router.py:113-116` |
| `mount_reliability_admin` **warns and mounts** on `server_auth=None` | `varco_fastapi/varco_fastapi/admin/mount.py:46`, `:113-118` |
| `build_webhook_router` **silently mounts unauthenticated** on `server_auth=None` — no warning at all | `varco_fastapi/varco_fastapi/webhook/router.py:91`, `:127-129` |
| The shipped webhook transport **does** connect to `target.pinned_ip` | `varco_core/varco_core/webhook/transport.py:100` |
| `SAWebhookSubscriptionRepository(encryptor=None)` is the default and stores secrets in plaintext | `varco_sa/varco_sa/webhook.py:105`, docstring `:15-16` |
| Lifecycle components are `AbstractLifecycle` (`start`/`stop`) collected into `VarcoLifespan` | `varco_fastapi/varco_fastapi/lifespan.py:73,84,88,96`; collection at `app.py:319-321,438` |

### Three corrections the verification forces

**Correction 1 — the webhook admin's real bug is BOLA, not the body-supplied `tenant_id`.**
`BACKLOG.md:69` names `router.py:134` (header-trusted read) and the body-supplied `tenant_id` on
create. Both are real. But **five routes take a `pk` and call `repository.find_by_id(pk)` with no
tenant check whatsoever** — `get_subscription` (`:165-167`), `disable_subscription` (`:173-175`),
`enable_subscription` (`:183-185`), `rotate_secret` (`:194-198`) and `delete_subscription`
(`:209`). `rotate_secret` then returns the new secret in the response body
(`_subscription_to_dict(saved, reveal_secrets=True)`, `:206`). A `webhook-admin` of tenant A who
knows or guesses a subscription UUID can rotate tenant B's signing secret **and read the
replacement**. That is textbook OWASP API1:2023 BOLA (brief 006 §2) and it outranks both named
issues. It is Phase 1.

**Correction 2 — the three surfaces are not symmetric, and treating them as one is wrong.**
`mount_tenant_admin` provisions, suspends and deletes *tenants*; every one of its routes takes a
`tenant_id` that is by definition not the caller's own. Binding it to the resolved tenant would
make it useless. §D-S4-control records the deviation and what it gets instead.

**Correction 3 — the reliability admin's cross-tenant path is `tenant_id=None`, not a mismatch.**
`build_dlq_router` and `build_audit_router` take `tenant_id: str | None = None` as an optional
**query parameter** (`dlq_router.py:83,121,177`; `audit_router.py:67,116,172`). Omitting it does
not mean "my tenant" — it means *all tenants*. `delete_where(tenant_id=None)`
(`dlq_router.py:118-130`, `audit_router.py:169-179`) is therefore an unscoped cross-tenant
**delete**, reachable by omitting a parameter. The guard for this surface is about the default
value of an absent parameter, not about a mismatch between two supplied values.

### Phase order

| Phase | Row | Content | Mergeable alone |
|---|---|---|---|
| 0 | S4a | The BOLA fix: tenant-scope every `find_by_id` route on the webhook admin | ✅ yes — no sibling dependency, fixes Correction 1 |
| 1 | S4b | The guard proper on webhook + reliability admin, via 033's `assert_tenant_matches()` | ❌ hard-gated on 033 |
| 2 | S9a | `SecurityPosture` core: the report model, severity ladder, suppression, settings | ✅ yes — zero sibling imports |
| 3 | S9b | The four collectors + `SecurityPostureLifecycle` | ❌ gated on 033/034; degrades for 035/037 |
| 4 | S11 | `AuditingAuthorizer` + `enable_authorization_audit()` | ✅ yes — independent of S4/S9 |
| 5 | — | Docs, README, CLAUDE.md, CHANGELOG, api-surface, BACKLOG, the 4.0 flip list | same commit as Phase 4 |

Phases 0, 2 and 4 can land before any sibling merges. Phases 1 and 3 are the genuinely gated
ones. If the cycle runs short, **Phase 0 alone is worth shipping** — it closes a live BOLA.

---

### §D-S4-bola — tenant-scope the by-id routes; a repository-level scope, not a post-fetch compare

Five webhook routes fetch by `pk` and never check the tenant (Correction 1). Two shapes were
available.

| ID | Choice | Consequence |
|---|---|---|
| D-S4-bola | **Fetch, then compare against the guard's verdict before any read or mutation** — `sub = await repository.find_by_id(pk)`, then `assert_tenant_matches(sub.tenant_id, allow_cross_tenant=...)`, and return **404, not 403**, when the compare fails | No `WebhookSubscriptionRepository` ABC change, so no out-of-tree implementation breaks. A cross-tenant probe cannot distinguish "exists elsewhere" from "does not exist" |

**DESIGN: compare after fetch, and answer 404 on a cross-tenant miss**

✅ Adding `find_by_id_for_tenant()` to the ABC would be the tighter fix, but it is a **breaking ABC
   change** in a security release for a bug the compare closes completely. The repository is not
   the confused deputy here; the router is.
✅ **404, not 403**, on a tenant mismatch: a 403 confirms the UUID exists in another tenant, which
   is an existence oracle. Brief 006 §2's defense 2 ("unpredictable resource IDs") is about not
   *needing* the oracle; not building one is the complement. The routes already 404 on a genuine
   miss (`router.py:168`), so the two cases become indistinguishable by construction.
✅ The compare is one line per route and is impossible to forget in review — five identical calls.
❌ The row is still read from the database before it is rejected. Accepted: no data crosses the
   boundary, and 037's RLS is the layer that makes even the read impossible when it is enabled.
❌ `CrossTenantAccessError` (033's exception) maps to 403 everywhere *else*; this surface
   deliberately maps it to 404. That asymmetry is documented in the Pitfalls table, and the
   handler is a single `except` in the router so it cannot drift route-to-route.
  Rejected — **`assert_tenant_matches(pk_owner)` without the 404 remap**: ❌ builds the existence
  oracle described above.
  Rejected — **filtering in the repository via `current_tenant()` implicitly**: ❌ an implicit
  ambient filter on an *admin* surface silently changes what a legitimately cross-tenant operator
  sees, with no error and no way to tell. Explicit is the house rule (`tenancy_cache_key()`).

### §D-S4-role — one role name, one kwarg, fail-closed when there is no auth at all

The guard needs an answer to "may this caller cross a tenant boundary". 033 exposes
`assert_tenant_matches(requested, *, allow_cross_tenant=False)`; this plan owns where the boolean
comes from.

| ID | Choice | Consequence |
|---|---|---|
| D-S4-role | A new keyword on each guarded mount: `cross_tenant_role: str = "cross-tenant-admin"`. The guard passes `allow_cross_tenant=ctx is not None and cross_tenant_role in ctx.roles`. **`ctx is None` ⇒ `False`, always** | An unauthenticated admin mount becomes strictly *narrower* than today, never wider. A deliberate cross-tenant operator adds one role to one token |

**DESIGN: a distinct role, not the existing `admin_role`, and not a settings flag**

✅ The existing `admin_role` (`"webhook-admin"`, `"tenant-admin"`) answers *may you use this
   surface*; crossing a tenant boundary is a strictly stronger permission and must be separately
   grantable. This is exactly the precedent `build_tenant_router` already set by refusing to
   reuse a generic admin role (`tenancy/router.py:73-75`).
✅ Defaulting to a role nobody holds means the guard is on by default with no configuration —
   and an operator who needs cross-tenant access grants one named thing.
✅ `server_auth=None` ⇒ no `ctx` ⇒ no cross-tenant ⇒ the *unauthenticated* mount is the most
   restricted one. That is the correct direction for a fail-closed default, and it is a fact S9
   reports rather than something this guard tries to fix.
❌ An operator who mounts unauthenticated **and** relied on cross-tenant listing loses it on
   upgrade. Named in the upgrade note with the exact fix (pass `server_auth=`, grant the role).
❌ A fourth keyword on mounts that already have three. Accepted; it is additive and defaulted.
  Rejected — **a `VARCO_*` env var enabling cross-tenant admin access**: ⛔ RD-9. An env var that
  widens a privileged surface is precisely what the three `mount_*` barriers exist to prevent.
  Rejected — **reusing `admin_role`**: ❌ every existing webhook-admin silently becomes a
  cross-tenant admin, which is today's bug with a new name.

### §D-S4-control — `mount_tenant_admin` is the control plane and is exempt, by argument

`build_tenant_router`'s routes are `POST /tenants`, `GET /tenants/{tenant_id}`,
`PATCH /tenants/{tenant_id}`, `DELETE /tenants/{tenant_id}`, `POST /tenants/{id}/migrate`
(`tenancy/router.py:138,162,171,181` and following). Every one addresses a tenant that is, by
construction, not the caller's.

| ID | Choice | Consequence |
|---|---|---|
| D-S4-control | **No `assert_tenant_matches()` on this surface.** Instead: (a) it keeps its mandatory `server_auth` and its non-default `tenant-admin` role — both already enforced (`router.py:113-116,120`); (b) it gains **nothing new in 3.2 except an S9 finding** (`posture.tenant_admin_mounted`, `info`) so the mount is visible in the report; (c) the deviation is written into the feature doc and the CHANGELOG | The backlog's "all three `mount_*` surfaces" wording is narrowed to two, in writing, with the reason |

**DESIGN: exempt the control plane rather than pretend the guard applies**

✅ A guard that must be bypassed on every route of a surface is not a guard — it is a lie in the
   documentation. Binding provisioning to `current_tenant()` would make `POST /tenants` able to
   provision only the tenant that already exists.
✅ The surface is already the *strictest* of the three: `server_auth` is mandatory (the other two
   mount without it), and its role deliberately is not the generic admin role.
✅ Brief 006 §2's "verify on every operation" is about `(authenticated_subject, target_tenant)`
   membership for *tenant-scoped* operations. Provisioning is not tenant-scoped; it is the
   operation that *creates* the scope. Applying the membership rule to it is a category error.
❌ It leaves the most powerful surface in the platform with a single line of defence (a role).
   Mitigated by S9 reporting the mount, and named as an explicit **4.0 candidate** (§D-S9-flip)
   for a separate provisioning-authority model — which is real work, not a guard.
  Rejected — **guard it anyway with `allow_cross_tenant=True` hardcoded**: ❌ dead code that
  reads as protection.

### §D-S4-scope — the reliability admin: an absent `tenant_id` means "mine", not "everyone"

Correction 3's shape. `list_entries`, `delete_where`, `verify_chain` and `redrive_batch` all take
`tenant_id: str | None = None` and treat `None` as unscoped.

| ID | Choice | Consequence |
|---|---|---|
| D-S4-scope | Every one of those routes routes its `tenant_id` through `assert_tenant_matches(tenant_id, allow_cross_tenant=...)`. 033's contract already specifies `requested is None -> current_tenant(), or CrossTenantAccessError if unset`, so **`None` resolves to the caller's tenant** and an unscoped sweep requires the cross-tenant role | `DELETE`-shaped routes stop being unscoped by omission. A cross-tenant operator passes the role and gets today's behaviour |

**DESIGN: reuse 033's `None` semantics rather than inventing a sentinel**

✅ 033 already had to decide what `None` means and chose "the current tenant, or refuse". Using it
   unchanged means one definition of "which tenant" across the release.
✅ It turns the most dangerous default in the file (`delete_where` with everything omitted) from
   *all tenants* into *my tenant*, with no new concept.
❌ An operator running a genuine cross-tenant retention sweep gets a `CrossTenantAccessError`
   until they hold the role. This is the intended behaviour change and it is in the upgrade note.
❌ `CrossTenantAccessError if unset` means these routes now fail when **no tenant context exists
   at all** — which is the common shape for an ops-only deployment with no tenant middleware.
   ⚠️ This is the highest-friction consequence in the plan; §Edge cases records the escape
   (`cross_tenant_role`), and Phase 1's tests cover it explicitly.

### §D-S9-oq1 — BACKLOG open question 1: **an explicit signal, defaulting to strict; inference rejected**

> *What is `SecurityPosture`'s "production" signal? An explicit `VARCO_ENV`/`VARCO_SECURITY_STRICT`
> setting, or inference from other configuration? Inference is convenient and gets it wrong at the
> worst moment; an explicit flag is honest but must default to strict or nobody sets it.*

**Answer: explicit, one variable, defaulting to the strict interpretation — and it changes
*severity presentation only*, never whether a check runs.**

| ID | Choice | Consequence |
|---|---|---|
| D-S9-oq1 | `SecurityPostureSettings.environment: Literal["production", "development"]`, env `VARCO_SECURITY_ENV`, **default `"production"`**. In `development`, findings at `warn` are emitted at `info` and the summary line says so. **Every check runs in both modes, and no finding is ever suppressed by the environment** | Nobody has to set anything to get the honest report. A developer who is tired of the noise downgrades presentation and still sees every finding |

**DESIGN: explicit and default-strict, severity-only, and never inferred**

✅ The backlog's own framing settles half of it: an explicit flag *must* default to strict or
   nobody sets it. Defaulting to `production` means the failure mode of forgetting the variable is
   *too much warning*, which is recoverable; the inverse is a silent production deployment.
✅ **Inference is rejected on a concrete failure mode**, not on principle: every candidate signal
   varco could infer from — `debug=True`, a `localhost` database URL, `PassthroughAuth` being
   bound — is *itself one of the things the preflight is checking*. Inferring the mode from the
   findings means the worst-configured deployment is the one that decides it is not production and
   downgrades its own warnings. That is the "gets it wrong at the worst moment" case, made exact.
✅ Making the flag govern **presentation only** removes the incentive to lie to it. There is no
   configuration of `VARCO_SECURITY_ENV` that makes a check stop running, so setting it to
   `development` in production is loud rather than dangerous.
✅ One variable, not two. A separate `VARCO_SECURITY_STRICT` alongside an environment name is two
   knobs for one axis and they will disagree.
❌ Every varco app that upgrades and starts up now logs security warnings it did not log before.
   That is the row's entire purpose (`BACKLOG.md:74`, "⭐ the vehicle for every warn-only half"),
   it is in the upgrade note, and the suppression list (§D-S9-suppress) is the supported answer
   for a knowingly-accepted finding.
  Rejected — **inferring from `app.debug` / a `localhost` DSN**: ❌ the self-referential failure
  above.
  Rejected — **a generic `VARCO_ENV` shared with other subsystems**: ❌ varco has no such variable
  today; introducing a process-wide environment concept as a side effect of a security row is a
  much larger decision than this plan should make. Named in §Parked with its un-park trigger.

### §D-S9-degrade — a missing seam reports `NOT_ASSESSED`, and that is never `OK`

The index marks 035→036 and 037→036 soft. The harness must therefore run when
`varco_fastapi.middleware.introspect` or `varco_sa.tenancy.rls_check.inspect_rls_posture` is not
importable — and 037's is in `varco_sa`, which an app may legitimately not have installed at all.

| ID | Choice | Consequence |
|---|---|---|
| D-S9-degrade | Four severities: `INFO` / `WARN` / `HIGH` / **`NOT_ASSESSED`**. Each collector is wrapped: `ImportError` or `ModuleNotFoundError` ⇒ exactly one `NOT_ASSESSED` finding naming the missing module and why it matters; any other exception ⇒ one `NOT_ASSESSED` finding carrying the exception **type name only**. A collector never takes the preflight down | An operator reading the report can tell "checked and fine" from "could not check". The summary counts them separately |

**DESIGN: a fourth severity, not a silent skip and not a crash**

✅ **A silently skipped check that renders as a clean report is a security-relevant lie** — this is
   the single most important property of the harness, and it is why `NOT_ASSESSED` is a severity
   rather than an absence. The summary line reads `3 warn, 1 high, 2 not assessed`, never
   `3 warn, 1 high`.
✅ A collector raising must not prevent the other three from running, or one slipped sibling takes
   the whole report with it.
✅ Exception **type name only**, never `str(exc)` — the same reasoning as 035's S3 fix and
   CLAUDE.md's `error_params()` rule. The full exception is logged server-side at `debug`.
✅ `varco_sa` genuinely not being installed is a *correct* configuration for a Mongo-only app, not
   an error — `NOT_ASSESSED` with the reason is the honest rendering of it.
❌ An operator can mistake `NOT_ASSESSED` for benign. Mitigated by counting it separately in the
   summary and by the Pitfalls table row.
  Rejected — **hard-depend on all four seams**: ❌ makes `varco_fastapi` import `varco_sa`,
  breaking the seam rule in CLAUDE.md, and makes 036 unmergeable until every sibling lands.
  Rejected — **treat a missing collector as a pass**: ⛔ the lie described above.

### §D-S9-shape — a lifecycle component that logs and returns a frozen report

| ID | Choice | Consequence |
|---|---|---|
| D-S9-shape | `SecurityPostureLifecycle(...)` — an `AbstractLifecycle` whose `start()` runs the collectors, logs the report, and stores it on the instance as `.report`. Registered like `TenancyLifecycle`/`ReliabilityLifecycle`: passed to `create_varco_app(..., extra_lifespan_components=[...])`, or constructed and awaited directly in a test | Emits **and** returns. A test asserts on `.report`; an operator reads the log; nothing new is invented for either |

The report model, all frozen (`@dataclass(frozen=True)`, house rule):

```python
# varco_fastapi/varco_fastapi/posture.py

class PostureSeverity(StrEnum):
    INFO = "info"
    WARN = "warn"
    HIGH = "high"
    NOT_ASSESSED = "not_assessed"

@dataclass(frozen=True)
class PostureFinding:
    check: str                      # stable id — the contract operators suppress on
    severity: PostureSeverity
    detail: str                     # what was observed. NEVER a raw exception string
    remediation: str                # the exact env var / kwarg / role that fixes it
    suppressed: bool = False

@dataclass(frozen=True)
class SecurityPosture:
    environment: str                              # "production" | "development"
    findings: tuple[PostureFinding, ...]
    not_assessed: tuple[str, ...]                 # collector names that could not run
    def counts(self) -> dict[PostureSeverity, int]: ...
    def worst(self) -> PostureSeverity | None: ...
```

**DESIGN: a lifecycle component, mirroring the two that already exist**

✅ `TenancyLifecycle` and `ReliabilityLifecycle` established the shape and `app.py:319-321,438`
   already collects them — no new wiring concept, and it runs after DI is populated, which is the
   only point at which "what is bound" is answerable.
✅ Returning the report as well as logging it makes the whole thing testable without a log-capture
   fixture, and gives an operator a programmatic hook for their own alerting.
✅ Frozen throughout, matching every sibling's inspector return type.
❌ It is opt-in — an app that never passes it gets no report. Accepted for 3.2 and it is the
   first row of the 4.0 flip list: auto-registration is a behaviour change on upgrade, which is
   exactly the class of thing the blast-radius rule holds to 4.0.
  Rejected — **run it inside `create_varco_app` unconditionally**: ❌ every existing app starts
  logging warnings on a patch upgrade with no way to have opted in. 4.0.
  Rejected — **a `varco security-posture` CLI verb instead**: ❌ a CLI process cannot see the
  container and the ASGI app the running service actually built. Named in §Parked as an
  *additional* surface, not a replacement.

### §D-S9-checks — what the harness reports, and where each finding comes from

Four collectors, each a thin adapter over a sibling's exported inspector, plus one local collector
for the facts only this plan can see (the admin mounts and the webhook repository).

| Collector | Source seam | Contributes |
|---|---|---|
| `_collect_tenant` | 033 `inspect_tenant_provenance()` | Re-emits 033's ten stable tokens (`tenant.no_chain`, `tenant.legacy_source_implicit`, `tenant.no_membership_provider`, `tenant.unchained_claim_tenant_setter`, …) with severity assigned here |
| `_collect_auth` | 034 `inspect_auth_posture()` + `inspect_revocation_posture()` | `auth.api_key_query_fallback`, `auth.api_key_plaintext`, `auth.passthrough_bound`, `auth.revocation_unbound`, `auth.revocation_registry_unwired`, `auth.revocation_fail_open` |
| `_collect_http` | 035 `inspect_http_edge()` | Re-emits 035's nine `http.*` ids unchanged |
| `_collect_data` | 037 `inspect_rls_posture()` | `data.rls_disabled`, `data.rls_not_forced`, `data.role_bypasses_rls` |
| `_collect_local` | this plan | `posture.admin_mount_unauthenticated`, `posture.tenant_admin_mounted`, `posture.webhook_secrets_plaintext`, `posture.base_authorizer_bound`, `posture.webhook_transport_unverified` |

**Severity assignment lives here, not in the sibling.** A sibling states the fact
(`legacy_source_implicit=True`); this plan decides it is `WARN` in production. 035 is the one
exception: it already commits to a severity per `check` id (`plans/035:§D-seam`), and those are
**re-emitted unchanged** so the two plans cannot disagree about their own contract.

⚠️ **`posture.base_authorizer_bound`** is the row's headline check and needs care: resolving
`AbstractAuthorizer` from the container and comparing `type(x) is BaseAuthorizer` is correct, but
the resolution itself can construct singletons. It is done inside the collector's `try`, and a
resolution failure is `NOT_ASSESSED`, never a crash of startup.

⚠️ **`posture.webhook_transport_unverified`** is the SSRF item the backlog folded in here
(`BACKLOG.md:88`: *"nothing asserts callers actually use the returned `pinned_ip`; fold that
assertion into `S9`'s preflight"*). The honest check is narrow, and the plan says so: the shipped
transport **does** use `target.pinned_ip` (`webhook/transport.py:100`), so the finding fires only
when a **non-default transport** is bound to the dispatcher — reported at `INFO` with the wording
*"a custom webhook transport is bound; varco cannot verify it connects to `target.pinned_ip`
rather than re-resolving the hostname"*. A static check cannot do better, and claiming more would
be the second lie this design exists to avoid.

### §D-S9-suppress — suppression is by stable id, and is itself reported

| ID | Choice | Consequence |
|---|---|---|
| D-S9-suppress | `VARCO_SECURITY_SUPPRESS` — a comma-separated list of `check` ids. A suppressed finding is **still produced**, with `suppressed=True`, is demoted to `INFO` in the log, and the summary line always ends with `(N suppressed)` | A knowingly-accepted finding stops being noise without disappearing. The suppression list is visible in the report an auditor reads |

✅ Suppressing by stable id is why every sibling was made to commit to id strings rather than
   prose — this is the payoff for that constraint.
✅ Never removing the finding means a suppression cannot hide a *newly appeared* problem that
   happens to share an id with an old accepted one — the count moves, and the entry is still there.
❌ An unknown id in the list is a typo that silently suppresses nothing. Mitigated: unknown ids are
   reported as a `posture.unknown_suppression` `INFO` finding naming them.

### §D-S9-enforce — an opt-in refuse mode, off by default, and the 4.0 default

`SecurityPostureSettings.enforce: Literal["warn", "refuse"] = "warn"` (env
`VARCO_SECURITY_ENFORCE`). In `refuse`, `start()` raises after logging if any finding is `HIGH`
(and, in `production`, `WARN`) and is not suppressed.

✅ The locked rule (`BACKLOG.md:54`) is warn-only in 3.2 — so `warn` is the default and nothing
   changes for anyone who does not opt in.
✅ An operator who *wants* fail-closed today gets it with one variable, which is the posture varco
   already prefers (`tenancy_cache_key()` raises rather than silently un-namespacing).
✅ `NOT_ASSESSED` **never** blocks startup, in either mode — "we could not check" is not evidence
   of a problem, and making it fatal would make a Mongo-only app unstartable because `varco_sa` is
   absent.
❌ Two knobs (`environment`, `enforce`). They are genuinely orthogonal — one is presentation, one
   is consequence — and collapsing them would make `development` mean "and never fail", which is
   the inference trap again.

### §D-S11-shape — a decorator over `AbstractAuthorizer`, not a middleware

The scout's suggestion was an HTTP middleware wrapping authorization. **That is wrong here**, and
the reason is structural: `authorize()` is called from the service layer at eleven verified sites
(`service/base.py:782,815,877,989,1047,1095,1176,1272`, `service/bulk.py:163,252`,
`service/soft_delete.py:256,309`) and **never from an HTTP middleware**. A middleware would miss
every authorization decision made by a job runner, an event consumer or a CLI verb.

| ID | Choice | Consequence |
|---|---|---|
| D-S11-shape | `AuditingAuthorizer(AbstractAuthorizer)` — wraps a delegate authorizer, calls it, and records the outcome. Bound via `enable_authorization_audit(container)`, the `enable_*` verb (opt-in DI binding flip), exactly like `varco_casbin.di.enable_policy_authorizer` | One wrapper covers all eleven call sites and every non-HTTP caller. No `AbstractAuthorizer` signature change, so no out-of-tree authorizer breaks |

```python
# varco_core/varco_core/auth/audit.py
class AuditingAuthorizer(AbstractAuthorizer):
    def __init__(
        self,
        delegate: AbstractAuthorizer,
        producer: AbstractEventProducer,
        *,
        policy: AuditDecisionPolicy = AuditDecisionPolicy.DENIALS,
    ) -> None: ...

    async def authorize(self, ctx: AuthContext, action: Action, resource: Resource) -> None:
        try:
            await self._delegate.authorize(ctx, action, resource)
        except Exception as exc:
            await self._record(ctx, action, resource, allowed=False, exc=exc)
            raise
        await self._record(ctx, action, resource, allowed=True)
```

**DESIGN: a decorator, opt-in via `enable_*`, holding a producer and never the bus**

✅ Covers every caller, HTTP or not — the property a middleware structurally cannot have.
✅ `enable_*` is the CLAUDE.md verb for "an opt-in DI binding that would shadow an app default if
   auto-registered", which is precisely what wrapping the app's authorizer is.
✅ It injects `AbstractEventProducer`, never `AbstractEventBus` — the standing rule, and the same
   shape `AuditLogMixin` already uses.
✅ The `raise` is re-raised untouched, so denial behaviour is byte-identical.
❌ Wrapping is order-sensitive: `enable_authorization_audit()` must resolve the *currently bound*
   authorizer and re-bind the wrapper, so calling it before the app registers its own authorizer
   would wrap `BaseAuthorizer` and then be shadowed. Guarded by a `RuntimeError` if the resolved
   delegate is `BaseAuthorizer` **and** the app has a higher-priority binding pending — and, more
   simply, by a documented "call it last" rule with a Pitfalls row.
❌ A second object in the authorization hot path. Measured against §D-S11-policy's default, the
   added cost on the common path is one `try` and one enum compare.
  Rejected — **an HTTP middleware**: ❌ misses jobs, consumers and the CLI, as above.
  Rejected — **a new abstract `_audit_authorize()` hook on `AbstractAuthorizer`**: ❌ a signature
  change on a public ABC in a security release; every out-of-tree authorizer would have to adopt
  it, and one that did not would silently log nothing.

### §D-S11-payload — exactly what is recorded, and what is deliberately excluded

CLAUDE.md's rule is explicit: `error_params()` is an exfiltration surface,
`ServiceAuthorizationError` deliberately excludes `reason`, and an override must apply the same
scrutiny and **never `vars(exc)`**. The audit payload is the same class of surface — it is written
to durable storage that a broad operator role can read.

**Recorded:**

| Field | Source | Why it is safe |
|---|---|---|
| `principal_id` | `ctx.user_id` | The subject already identified in every other audit row |
| `actor_id` | `ctx.metadata["actor"]`, or 033's delegation context when S16 shipped | brief 006 §3: *"every `act` delegation must be logged with both principal and actor"* — the CVE-2025-55241 lesson |
| `tenant_id` | `current_tenant()` | The existing audit trail's own scoping |
| `action` | `str(action)` | An enum-shaped verb, no payload |
| `entity_type` | `resource.entity_type.__name__` | A class name, not an instance |
| `entity_pk` | `resource.entity.pk` when not a collection, else `None` | An identifier the caller already supplied |
| `is_collection` | `resource.is_collection` | structural |
| `decision` | `"allow"` / `"deny"` | the point of the row |
| `denial_type` | `type(exc).__name__` on denial | a class name |
| `correlation_id` | ambient, as every other audit row | |

**Deliberately excluded, each for a stated reason:**

- **The denial `reason` / `str(exc)`** — the exact field `ServiceAuthorizationError` already
  excludes. Recording it here would reintroduce, into durable storage, the leak
  `ServiceAuthorizationError` and 035's S3 both close.
- **`resource.entity` itself** — the entity may carry encrypted or personal fields; the audit
  trail is not the place to duplicate them, and `AuditEntry` already has a modelled place for
  entity diffs that this is deliberately not reusing.
- **`ctx.grants`, `ctx.roles`, and the raw token** — a full permission dump per decision, in
  storage, is a map of the authorization system for anyone who can read the audit table.
- **`ctx.metadata` wholesale** — an app-controlled dict of unknown contents. Only the single
  `actor` key is read, by name.
- **⛔ `vars(exc)`, in any form.**

### §D-S11-policy — denials always; allows only when delegated, or when the app asks

An authorization decision happens on every service call, several times per request. An
unconditional synchronous audit write would be the single largest cost in the release.

| ID | Choice | Consequence |
|---|---|---|
| D-S11-policy | `AuditDecisionPolicy`: **`DENIALS`** (default) records every deny **plus every allow whose `ctx` carries an actor**; `ALL` records everything; `NONE` disables recording while keeping the wrapper installed | The security signal is captured at a fraction of the write volume. Delegated access is never silently allowed — the CVE-2025-55241 property is preserved under the default |

**DESIGN: denials plus delegated allows as the default, not all-or-nothing**

✅ **The delegated-allow carve-out is what makes `DENIALS` safe.** Brief 006 §3's lesson is not
   "log everything"; it is that *impersonation must be logged at issuance and at use, and silent
   actor tokens are a critical risk*. An `act`-carrying token is rare, so recording 100% of those
   costs almost nothing and preserves the exact property the CVE turned on.
✅ Denials are rare in a working system and are the row an investigator actually reads.
✅ `ALL` exists for a regulated deployment that must evidence every access — varco's stated
   competitive axis — and is one enum value away.
✅ `NONE` keeps the wrapper in place so the setting can be flipped without re-wiring DI.
❌ A pure-`DENIALS` deployment cannot answer "who read this record" from the authz trail.
   Accepted and documented: `AuditLogMixin` is the answer for mutations, and `ALL` for reads.
❌ Emission is through `AbstractEventProducer`, so it inherits the bus's delivery semantics — a
   dropped event is a missing audit row. The outbox pattern is the documented answer for a
   deployment that cannot tolerate that, and it is a Pitfalls row rather than a new mechanism.

### §D-S11-conformance — no new suite, one `COVERAGE.md` row

`AuditingAuthorizer` implements `AbstractAuthorizer`, which is not one of the ABCs
`testkit/varco_conformance` covers and has no suite. CLAUDE.md's rule triggers on *a new
implementation of one of those ABCs*, which this is not. **No new suite, no count change** — and a
one-line "Stated absence" row in `testkit/varco_conformance/COVERAGE.md` recording that
`AbstractAuthorizer` has no suite and why (it is a single-method, app-supplied policy hook whose
contract — *raise on denial, deny by default* — is asserted directly against `BaseAuthorizer` and
`AuditingAuthorizer` in `varco_core/tests/`).

### §D-S9-flip — the consolidated 4.0 flip list

The definition of done requires this written down in one place. Gathered from all five plans;
each row names the plan that owns the flip and the escape hatch that exists in 3.2.

| # | Default today (3.2) | 4.0 | Owner | 3.2 escape hatch / warn |
|---|---|---|---|---|
| 1 | `SecurityPostureLifecycle` is opt-in | Auto-registered by `create_varco_app` | 036 | Pass it in `extra_lifespan_components` |
| 2 | `SecurityPostureSettings.enforce="warn"` | `"refuse"` in `production` | 036 | `VARCO_SECURITY_ENFORCE=refuse` |
| 3 | `mount_reliability_admin(server_auth=None)` warns and mounts | Refuses | 036 reports; mount owns the flip | `posture.admin_mount_unauthenticated` |
| 4 | `build_webhook_router(server_auth=None)` mounts silently | Refuses | 036 reports (and adds the missing warning in Phase 1) | `posture.admin_mount_unauthenticated` |
| 5 | `BaseAuthorizer` is the permissive fallback | Refuses to bind outside `development` | 036 reports | `posture.base_authorizer_bound` |
| 6 | Tenant resolution defaults to header-only | The chain is the default; the header requires `LegacyTenantSource` | 033 | `tenant.no_chain`, `tenant.legacy_source_implicit` |
| 7 | Membership check is fail-open on a missing claim | Fail-closed | 033 | `tenant.membership_missing_claim_allows` |
| 8 | Cross-check is `LENIENT` | remains `LENIENT`; **not** a flip — recorded so it is not mistaken for one | 033 | `tenant.cross_check_lenient` |
| 9 | `RequestContextMiddleware` sets the tenant from a claim with no catalog/membership check | Removed in favour of the chain | 033 | `tenant.unchained_claim_tenant_setter` |
| 10 | `ErrorEnvelopeSettings.include_detail` echoes `detail` | `False` | 035 | `http.error.detail_exposed` |
| 11 | Rate limiting is opt-in | remains opt-in; **not** a flip — recorded for the same reason as row 8 | 035 | `http.rate_limit.absent` |
| 12 | `enforce_rls=False`; policies not `FORCE`d | RLS asserted for `TENANT` tables on Postgres | 037 | `data.rls_disabled`, `data.rls_not_forced` |
| 13 | `SAWebhookSubscriptionRepository(encryptor=None)` stores secrets in plaintext | Requires an encryptor | 036 reports; `varco_sa` owns the flip | `posture.webhook_secrets_plaintext` |
| 14 | `mount_tenant_admin` is guarded by a role alone | A provisioning-authority model (§D-S4-control) | 036 opens it; 4.0 designs it | `posture.tenant_admin_mounted` |

⚠️ Rows 8 and 11 are **deliberately not flips** and are listed so a 4.0 planner does not
"complete" the list by flipping them. Their siblings argued each on the merits.

### Alternatives considered (plan-level)

- **Build S9 first and let it define the seams top-down.** Rejected: ❌ it would make four plans
  wait on this one and invert the index's ordering; ✅ each sibling knows its own facts, and the
  stable-id contract already lets 036 be written before any of them merges — as this plan was.
- **Fold S4 into 033.** Rejected: ❌ 033 is already the longest pole, and S4 is an HTTP-surface
  change in three routers that has nothing to do with the trust model beyond consuming one
  function; ✅ the index's grouping decision already settles it.
- **Ship S11 as an `AuditLogMixin`-style service mixin.** Rejected: ❌ authorization is not a
  service mutation and has no UoW; ❌ it would require every service to adopt a mixin, so an
  un-migrated service would silently log nothing — the exact failure the decorator avoids.
- **Drop S11 to 4.0 and ship only S4/S9.** Rejected: ❌ it leaves the cycle's answer to
  CVE-2025-55241 unbuilt while shipping the delegation mechanism (033's S16) that makes the
  attack shape possible; the two belong in the same release.

---

## Steps

### Phase 0 — S4a: the BOLA fix on the webhook admin (🔴 must, S) — **independently mergeable**

1. [x] Add a module-level helper in `varco_fastapi/varco_fastapi/webhook/router.py` that resolves a
   subscription by `pk` and 404s when it does not exist **or** belongs to another tenant, with the
   tenant comparison behind a `cross_tenant: bool` the caller supplies. In Phase 0 the comparison
   is against `current_tenant()` directly; Phase 1 swaps it to `assert_tenant_matches()`.
   **Implementer's note**: 033's `assert_tenant_matches()`/`CrossTenantAccessError` were verified
   present and byte-identical to this plan's assumed signature *before* Phase 0 was written, so
   Phase 0 was implemented directly against the real seam — no throwaway local comparator was
   built only to be deleted in Phase 1. This collapses Phases 0 and 1 into one commit for the
   `_scoped_subscription_or_404`/`assert_tenant_matches` plumbing; the `cross_tenant_role=` kwarg
   and reliability-admin scoping (originally Phase 1 only) landed in the same pass.
2. [x] Route `get_subscription` (`:165`), `disable_subscription` (`:173`), `enable_subscription`
   (`:183`), `rotate_secret` (`:194`) and `delete_subscription` (`:209`) through it.
3. [x] Scope `list_subscriptions` (`:132-140`): the `X-Tenant-Id` header read at `:134` is replaced by
   the resolved tenant; the `else: subs = []` branch at `:139` stays as the no-tenant-context case.
4. [x] Add the missing `server_auth=None` warning to `build_webhook_router` — the other two mounts
   warn (`admin/mount.py:113-118`) or refuse (`tenancy/router.py:113-116`); this one does neither.
5. [x] Tests (`varco_fastapi/tests/`): a subscription owned by tenant B is **404, not 403**, on each of
   the five by-id routes under tenant A's context; `rotate_secret` on another tenant's
   subscription neither rotates nor reveals; `list_subscriptions` ignores a spoofed `X-Tenant-Id`;
   with no tenant context at all, the by-id routes behave as documented in §Edge cases.

**Verify:** `uv run pytest varco_fastapi/tests/ -k webhook` green; `make lint` green.

### Phase 1 — S4b: the guard proper (🔴 must, S) — **gated on 033**

6. [x] Swap Phase 0's local comparison for 033's `assert_tenant_matches(requested,
   allow_cross_tenant=...)`, and add `cross_tenant_role: str = "cross-tenant-admin"` to
   `build_webhook_router`, `mount_webhook_admin`, `build_dlq_router`, `build_audit_router` and
   `mount_reliability_admin`.
7. [x] Resolve `allow_cross_tenant` from the request's `AuthContext` per §D-S4-role — `ctx is None`
   ⇒ `False`, unconditionally.
8. [x] Guard `create_subscription`'s body-supplied `tenant_id` (`router.py:152`) — the row's originally
   named write.
9. [x] Reliability admin per §D-S4-scope: route `tenant_id` through `assert_tenant_matches()` in
   `dlq_router.list_entries` (`:83`), `delete_where` (`:121`), `redrive_batch` (`:177`) and
   `audit_router.list_entries` (`:67`), `verify_chain` (`:116`), `delete_where` (`:172`).
10. [x] Map `CrossTenantAccessError` to **403** on the reliability admin and to **404** on the webhook
    by-id routes (§D-S4-bola), each in exactly one place per router.
11. [x] `mount_tenant_admin` is **not** touched (§D-S4-control) — assert that in a test that fails if a
    future edit adds a guard there without revisiting the decision.
12. [x] Tests: cross-tenant body `tenant_id` on create; a caller with and without `cross-tenant-admin`;
    an omitted `tenant_id` on `delete_where` scoping to the caller's tenant; the same with the role
    reaching every tenant; `server_auth=None` never granting cross-tenant.

**Verify:** `uv run pytest varco_fastapi/tests/` green.

### Phase 2 — S9a: the posture core (🟡 should, S) — **no sibling imports**

13. [x] `varco_fastapi/varco_fastapi/posture.py`: `PostureSeverity`, `PostureFinding`,
    `SecurityPosture` (§D-S9-shape), all frozen, `from __future__ import annotations`.
14. [x] `SecurityPostureSettings` (pydantic `BaseSettings`, prefix `VARCO_SECURITY_`):
    `environment` (`VARCO_SECURITY_ENV`, default `production`), `enforce`
    (`VARCO_SECURITY_ENFORCE`, default `warn`), `suppress` (`VARCO_SECURITY_SUPPRESS`, default
    empty).
15. [x] The severity ladder, suppression (§D-S9-suppress) including the `posture.unknown_suppression`
    finding, the `development` demotion, and the summary-line renderer that always separates
    `not assessed` from the rest.
16. [x] `SecurityPostureLifecycle` with `start()`/`stop()` and `.report`; the `refuse` mode
    (§D-S9-enforce), with `NOT_ASSESSED` explicitly never fatal.
17. [x] Tests: severity demotion in `development`; suppression demotes but never removes; an unknown
    suppression id is reported; `refuse` raises on `HIGH` and never on `NOT_ASSESSED`.

**Verify:** `uv run pytest varco_fastapi/tests/ -k posture` green.

### Phase 3 — S9b: the five collectors (🟡 should, M) — **gated on 033/034, degrades for 035/037**

18. [x] `_collect_local` first (it needs no sibling): `posture.admin_mount_unauthenticated`,
    `posture.tenant_admin_mounted`, `posture.webhook_secrets_plaintext`,
    `posture.base_authorizer_bound`, `posture.webhook_transport_unverified` (§D-S9-checks, with
    its narrow wording).
19. [x] `_collect_tenant`, `_collect_auth`, `_collect_http`, `_collect_data` — each importing its
    sibling's inspector **inside the function body**, each wrapped per §D-S9-degrade.
20. [x] Assign severities for 033's, 034's and 037's tokens here; **re-emit 035's nine ids with 035's
    own severities unchanged.**
21. [x] Register the lifecycle in the docs' recommended wiring — `create_varco_app(...,
    extra_lifespan_components=[SecurityPostureLifecycle(...)])`. **No auto-registration** (4.0
    flip row 1).
22. [x] Tests: with every sibling module absent (monkeypatched `ImportError`), the report contains four
    `NOT_ASSESSED` entries, zero `OK`-shaped silence, and `start()` still returns; a collector
    raising a non-import exception yields one `NOT_ASSESSED` carrying **only** the type name; the
    full report against a deliberately-worst-case app has the expected ids.

**Verify:** `uv run pytest varco_fastapi/tests/ -k posture` green; `make type-check` green.

### Phase 4 — S11: the authorization-decision audit (🟡 should, M) — **independent**

23. [x] `varco_core/varco_core/auth/audit.py`: `AuditDecisionPolicy` enum and `AuditingAuthorizer`
    (§D-S11-shape), payload built strictly per §D-S11-payload.
24. [x] The event type and its route into the existing audit path — reusing `AuditEntry`'s storage via
    `AuditConsumer` where the fields fit, and adding the authz-specific fields as a distinct
    event rather than overloading a mutation row. **`AbstractEventProducer` only, never the bus.**
25. [x] `enable_authorization_audit(container, *, policy=AuditDecisionPolicy.DENIALS)` in the
    appropriate `di.py` — the `enable_*` verb — resolving the currently bound authorizer and
    re-binding the wrapper, with the "call it last" guard and error.
26. [x] Tests: a denial is recorded and the original exception re-raised unchanged; an allow with no
    actor is **not** recorded under `DENIALS`; an allow **with** an actor **is**; `ALL` records
    everything; `NONE` records nothing but keeps the delegate's behaviour identical; and an
    explicit assertion that the recorded payload contains none of the six excluded fields —
    written as a field allowlist so a future field addition fails the test.

**Verify:** `uv run pytest varco_core/tests/ -k "authoriz or audit"` green.

### Phase 5 — docs, snapshot, backlog (🟡 should, S — **same commit as Phase 4**)

27. `technical_docs/features/security-posture.md` — S9 and the collector table, the four-severity
    model, the suppression contract, **the consolidated 4.0 flip list (§D-S9-flip)**, and a
    **Pitfalls** table: `NOT_ASSESSED` is not a pass; `VARCO_SECURITY_ENV=development` changes
    presentation only; an unknown suppression id suppresses nothing; the preflight is opt-in in
    3.2.
28. `technical_docs/features/admin-surface-tenancy.md` — S4, the three surfaces' asymmetry,
    §D-S4-control's exemption stated plainly, and a **Pitfalls** table: 404-not-403 on the webhook
    by-id routes; an absent `tenant_id` on the reliability admin now means "mine"; an
    unauthenticated mount can never cross tenants.
29. `technical_docs/features/authorization-audit.md` — S11, the recorded/excluded field tables
    verbatim, the `DENIALS`-plus-delegated-allows rationale, and a **Pitfalls** table: call
    `enable_authorization_audit()` last; producer delivery semantics mean the outbox is the
    answer for guaranteed rows; `NONE` still wraps.
30. README: a "Security posture preflight" section with the `VARCO_SECURITY_*` env-var table, an
    "Authorization-decision audit" section, and the `cross_tenant_role` kwarg documented on the two
    guarded mounts.
31. CLAUDE.md: one-line pointers to the three feature docs, a decision-tree entry (*startup
    security check? → `varco_fastapi.posture`, never a second preflight; authorization decision
    logging? → `AuditingAuthorizer` via `enable_authorization_audit()`, never a middleware*), and
    the `mount_*` note that two of three surfaces are now tenant-bound and why the third is not.
32. `testkit/varco_conformance/COVERAGE.md` — the §D-S11-conformance "Stated absence" row.
33. CHANGELOG: S4 as a **behaviour change** (three sub-entries: BOLA fix, cross-tenant guard,
    reliability-admin default scoping), S9 and S11 as additions, and the upgrade note.
34. [x] `uv run python scripts/api_surface.py` — regenerate and commit. New public names:
    `PostureSeverity`, `PostureFinding`, `SecurityPosture`, `SecurityPostureSettings`,
    `SecurityPostureLifecycle`, `AuditDecisionPolicy`, `AuditingAuthorizer`,
    `enable_authorization_audit`.
35. [x] BACKLOG.md: mark S4, S9, S11 as shipped and record §D-S9-oq1 as the answer to open question 1.

**Verify:** `make lint` (runs `api-check`, `asyncapi-check`, `import-budget`), `make type-check`,
`make test` all green.

---

## Migration and upgrade note (existing deployments — read before shipping)

| Change | Who is affected | The fix |
|---|---|---|
| Webhook by-id routes 404 on another tenant's subscription | Anyone using one admin token to manage every tenant's webhooks | Grant `cross-tenant-admin` on that token |
| `list_subscriptions` ignores `X-Tenant-Id` and uses the resolved tenant | Anyone driving that surface by header alone | Same; or resolve the tenant properly (033) |
| `create_subscription` refuses a body `tenant_id` that is not the resolved tenant | Provisioning tooling | Same |
| Reliability admin: an omitted `tenant_id` means "mine", not "all" | Retention sweeps, cross-tenant DLQ triage | Same. ⚠️ **`delete_where` with everything omitted no longer deletes across tenants** — verify your sweep still does what you intend before granting the role |
| Reliability admin routes raise when no tenant context exists at all | An ops-only deployment with no tenant middleware | Grant `cross-tenant-admin`; §Edge cases |
| Startup logs security warnings | Everyone who opts the lifecycle in | Intended. Suppress a knowingly-accepted id via `VARCO_SECURITY_SUPPRESS` |
| Nothing else | An app that changes no code and does not add the lifecycle | **Byte-identical** except the S4 guards, which are the release's point |

---

## Edge cases

- **No tenant context at all, on a guarded route.** 033's `assert_tenant_matches()` raises when
  `requested is None` and `current_tenant()` is unset. On the reliability admin that is the
  ops-only deployment shape and it is the most likely upgrade break — the escape is the
  `cross-tenant-admin` role, and it is the first row of the upgrade note's ops section.
- **`server_auth=None` on a guarded mount.** No `ctx`, so never cross-tenant. The surface is
  unauthenticated *and* single-tenant, which is narrower than today in both axes.
- **A collection resource in S11's payload.** `resource.entity` does not exist; `entity_pk` is
  `None` and `is_collection` is `True`.
- **`AuditingAuthorizer` wrapping itself.** `enable_authorization_audit()` called twice would
  double-record. It detects an already-wrapped delegate and is a no-op with a warning.
- **A sibling seam present but returning an unexpected shape** (a version skew mid-release). The
  collector's `try` catches it as `NOT_ASSESSED` with the type name — not a crash, not a pass.
- **`varco_sa` absent.** `_collect_data` reports `NOT_ASSESSED`. Correct for a Mongo-only app, and
  never fatal even under `enforce=refuse`.
- **A suppression list naming every id.** The summary still reports the counts and
  `(N suppressed)`; nothing is silent.

## Verification

```bash
# Phase 0 alone (it ships alone)
uv run pytest varco_fastapi/tests/ -k webhook

# Phases 1-4
uv run pytest varco_fastapi/tests/ -k "posture or admin or tenant"
uv run pytest varco_core/tests/ -k "authoriz or audit"

# Whole set
make lint && make type-check && make test
uv run python scripts/api_surface.py --check
```

No integration test is owed by this plan: every collector is a pure read over already-constructed
objects, and the one seam with a real database (037's `inspect_rls_posture`) is exercised by 037's
own Postgres integration tests. Stated so the absence is a decision.

## Parked

| Item | Why parked | Un-park trigger |
|---|---|---|
| A process-wide `VARCO_ENV` shared across subsystems | Introducing a global environment concept as a side effect of a security row is a much larger decision than this plan should make (§D-S9-oq1) | A second subsystem needs the same signal; then `VARCO_SECURITY_ENV` becomes an override of it |
| `varco security-posture` CLI verb | A CLI process cannot see the container and ASGI app the running service built, so it would report on a *different* app | A packaged app factory a CLI can import and build identically |
| Auto-registering the preflight in `create_varco_app` | Behaviour change on upgrade; blast-radius rule | 4.0 (flip list row 1) |
| A provisioning-authority model for `mount_tenant_admin` | §D-S4-control: real work, not a guard | 4.0 (flip list row 14) |
| `find_by_id_for_tenant()` on `WebhookSubscriptionRepository` | A breaking ABC change; the post-fetch compare closes the bug completely | A 4.0 window with an ABC-break budget |
| Recording allows under `DENIALS` | §D-S11-policy; `ALL` exists | A regulated consumer needs full read auditing and finds `ALL` too coarse |

## Risks

| Risk | Severity | Mitigation |
|---|---|---|
| ⚠️ **ASSUMPTION** — 033's `assert_tenant_matches()`, `CrossTenantAccessError` and `inspect_tenant_provenance()` ship with the signatures in `plans/033:§D-036-seams`. None exists yet | High | Phase 0 is written against `current_tenant()` directly and merges without them; Phase 1 is the only thing gated. A signature change means a one-line adapter here |
| ⚠️ **ASSUMPTION** — 034's `inspect_auth_posture()`/`inspect_revocation_posture()` and 035's nine `check` ids ship as specified | Medium | §D-S9-degrade makes a missing or changed seam a `NOT_ASSESSED`, not a failure |
| ⚠️ **ASSUMPTION** — 037's `inspect_rls_posture()` name and signature, read from `plans/037:§D-S12-posture` | Low | Soft edge; same degradation |
| S4's reliability-admin scoping breaks an ops workflow quietly | High | It fails loudly (403), not quietly; upgrade note; the role restores it exactly |
| The 404-not-403 remap confuses an operator debugging a legitimate miss | Medium | Pitfalls row; the server-side log records the real cause with the correlation id |
| `posture.base_authorizer_bound` resolves the container and constructs singletons at startup | Medium | Inside the collector's `try`; failure is `NOT_ASSESSED`, never a startup crash |
| S11 adds cost to every authorization decision | Medium | `DENIALS` default; the common path is one `try` and one enum compare; `NONE` is available |
| The audit payload grows a leaky field later | Medium | The Phase 4 test is a field **allowlist**, so any added field fails until it is argued |
| The preflight becomes a checkbox nobody reads | Low | Stable ids + explicit suppression make an accepted finding an auditable decision rather than ignored noise |

## Open questions — RESOLVED at implementation time

1. **Does `AuditEntry` accommodate an authorization decision, or does the authz trail need its own
   row type?** **RESOLVED: a distinct event type, `AuthorizationDecisionEvent` (`varco_core.auth.audit`),
   carrying one generic `payload: dict[str, Any]` field — NOT `AuditEntry`'s typed shape, and no
   `AuditRepository` method addition.** `AuditEntry`'s fields (`entity_type`, `entity_id`, `diff`,
   `prev_hash`/hash-chain machinery) are shaped around a *mutation* — an authorization decision has
   no `diff` and needs `decision`/`denial_type`/`is_collection`, none of which fit without widening
   `AuditEntry` for every existing mutation-audit consumer too. §D-S11-shape's promise is
   "recorded via `AbstractEventProducer`" — not "persisted to a specific table" — so this plan ships
   exactly that: `AuditingAuthorizer` produces `AuthorizationDecisionEvent` onto the `"varco.audit"`
   channel (the same channel `AuditEvent` uses), and an app wires its own consumer to persist it,
   exactly as it would for any other event. No new `AuditRepository` ABC method was needed, so this
   did not turn into an ABC addition mid-phase, per the open question's own escape clause.
2. **Should `enable_authorization_audit()` live in `varco_core.auth.di` or `varco_fastapi.di`?**
   **RESOLVED: `varco_core.auth.di`.** `AuditingAuthorizer` wraps `AbstractAuthorizer` and injects
   `AbstractEventProducer` — both `varco_core` interfaces with zero HTTP dependency — so it follows
   the same precedent as `varco_casbin.di.enable_policy_authorizer`: the `enable_*` verb lives in
   the package that owns the implementation being wired, not in the package that happens to serve
   HTTP. Mirrors `varco_core.revocation.di.enable_token_revocation`'s shape exactly (an opt-in DI
   binding flip, never a scanned `@Configuration`).
