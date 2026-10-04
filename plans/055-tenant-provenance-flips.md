# Plan 055 — Tenant-provenance default flips (FLIP-6, FLIP-7, FLIP-9) + the FLIP-14 provisioning-authority design

Covers BACKLOG 4.0 rows **`FLIP-6`** (`BACKLOG.md:98` — *"Tenant resolution defaults to the chain;
header-only requires an explicit `LegacyTenantSource`; `TenantResolutionMiddleware(chain=None)` →
`TypeError`"*), **`FLIP-7`** (`:70` — *"`ClaimTenantMembership.on_missing_claim` default `ALLOW` →
`DENY`"*), **`FLIP-9`** (`:99` — *"`RequestContextMiddleware.enable_tenant_context` defaults
`False`; the unchained claim-tenant-setter path is removed"*) and, as a **design-only** section,
**`FLIP-14`** (`:101` — *"A provisioning-authority model for `mount_tenant_admin` (replacing the
role-only guard)"*). Flip track, slice row 055 (`plans/000-index-4-0-di-standardization.md:34`):
independently mergeable, no plan dependency — but it *targets* the shapes 049/051/053/054 land
(`VarcoModule`/`EdgeSlot`/`TenancyModule`, `@Multibound TenantSource` +
`resolve_tenant_source_chain(container)`, `PostureCollector`, `warn_deprecated()`, the posture
severity table + the derived-`refuse` default), and every step names its pre-/post- form.

**Neighbours (do not absorb):** 056 owns the data-layer flips (`enforce_rls`, `FORCE ROW LEVEL
SECURITY`, `SAWebhookSubscriptionRepository(encryptor=)`) — this plan never touches
`varco_sa`. 054 owns the severity table's *mechanism* and `enforce`'s derivation — this plan
only adds/re-tiers `tenant.*` rows in it. 051 owns the `TenantSource` pool, the allowlist rule and
`resolve_tenant_source_chain(container)` — this plan makes the middleware and the module *use*
it. 049 owns `VarcoModule`/`EdgeSlot`/`TenancyModule` — this plan extends `TenancyModule` and adds
one `EdgeSlot` member. 058 assembles this plan's §Migration into the one 3.x → 4.0 guide and
re-labels the nine `removed_in="4.0.0"` sites; this plan's one deprecation site is written
`removed_in="5.0.0"` from the start.

**Design of record:** `plans/033-tenant-identity-provenance.md` §D-S6-blast (`:624-650` — the
mechanism ships in 3.2, the default does not flip; the 4.0 flip list rows 1-3; *"Row 3 is the
sharp one"*), §D-S6-oq2 (`:179-202` — `LENIENT` default, **not** a flip), §D-S6-oq3 (`:238-272`),
§D-S5-claim (`:590` — `ALLOW` in 3.2 with one warning, on the flip list);
`plans/036-authorization-surface-and-posture.md` §D-S9-flip rows 6/7/9/14 (`:555-558,563`), rows
8/11 (`:557,560` — **deliberately not flips**), §D-S4-control (`:192-217` — the control plane is
exempt by argument; *"a separate provisioning-authority model — which is real work, not a
guard"*); `design/research/006-multi-tenant-identity-and-hardening.md` §1 (trust ranking), §2
(*"verify `(authenticated_subject, target_tenant_id)` on every operation"*, *"fail closed on
missing tenant"*), §3 (RFC 8693 — *"never grant unrestricted delegation … scope it"*; CVE-2025-55241
— *"impersonation must be logged at issuance AND use"*); `CONTRIBUTING.md:91-105` (the 12-month
removal floor); `CLAUDE.md`'s rules — `RequestContext` never holds the tenant, `current_tenant()`
is the single source of truth, `TenantProvenance` is its own `AmbientVar`,
`TenantResolutionMiddleware(chain=None)` is byte-identical to pre-3.2 with one `DeprecationWarning`.

> ⚠️ **Five premises corrected from source while writing this plan; each reshapes a §D block.**
> (1) **The 12-month floor forbids the row's `TypeError`.** `3.2.0` was tagged **2026-09-12**
> (`CHANGELOG.md:49`), and `TenantResolutionMiddleware(chain=None)` was deprecated in it with the
> text *"will be removed in 4.0.0"* (`varco_fastapi/varco_fastapi/middleware/tenant_resolution.py:150-162`).
> `CONTRIBUTING.md:98` — *"at least 12 months between the release that deprecates a symbol and the
> release that removes it"* — puts the earliest permitted removal at **2027-09-12**; 4.0.0 ships
> before that. The same floor is why the index keeps the nine `removed_in="4.0.0"` sites
> (`plans/000-…md:10-12`). §D-flip6-floor gives the compliant shape: **flip the default to the
> chain; keep the implicit-legacy fallback as the deprecated path, re-labelled `removed_in="5.0.0"`;
> make it a production start-blocker through posture, not through `TypeError`.** (2) The 3.2
> deprecation is an **ad-hoc `warnings.warn`**, not `varco_core.deprecation` — it therefore has no
> `removed_in=` keyword and is invisible to `rg 'removed_in="…"'`, the exact rot `deprecation.py:11-15`
> exists to prevent; and `LegacyTenantSource`'s docstring says the class *"is removed in 4.0"*
> (`varco_core/varco_core/tenancy/sources.py:255`), which contradicts 033 §D-S6-blast (the
> **explicit** source is the escape hatch and is never removed). Both are fixed here. (3) The task
> brief says *"a `TenancyModule` owns the tenancy middleware wiring now"* — **it does not.** 049's
> `TenancyModule(*, pool, supervisor=None, catalog_subscription=None)` wraps `TenancyLifecycle` only
> (`plans/049-…md:111,561`), its `EdgeSlot` table has **no** tenant-resolution slot (`:266-270`), and
> `TenantResolutionMiddleware` is still app-wired through `MiddlewareModule([...], slot=EdgeSlot.EXTRA)`
> — outside `ErrorMiddleware`, as `technical_docs/features/timezone-handling.md:325` records (*"no
> dedicated slot"*). This plan adds `EdgeSlot.TENANT_RESOLUTION` and makes `TenancyModule` an
> `HttpContributor` (§D-flip6-module). (4) **A latent 4.0 start-blocker for every non-tenant app.**
> 049 §D-posture has `SecurityPostureModule` contribute the tenant collector with `chain=None`
> whenever no chain is resolvable (`plans/049-…md:379-380`); `inspect_tenant_provenance(None)` emits
> `tenant.no_chain` + `tenant.legacy_source_implicit` (`varco_core/varco_core/tenancy/posture.py:117-119`);
> 054's severity table tiers both **WARN** (`plans/054-…md:284`); and 054's derived `refuse` makes a
> production `WARN` a start-blocker. Composed, a stock 4.0 app **with no tenancy at all** would
> refuse to start in production. §D-collector re-homes the collector into `TenancyModule` so
> `tenant.*` findings exist only where tenant resolution is actually wired. (5) FLIP-9's
> one-line fix does not exist for the largest affected class. `TenantResolutionMiddleware` requires
> `catalog` and `pool` (`tenant_resolution.py:134-135`, no defaults) and 404s a tenant the catalog
> does not know (`:243-244`); a `SHARED`-isolation app that today gets `current_tenant()` from the
> JWT claim via `RequestContextMiddleware` (`request_context.py:189-192`) has neither. Removing the
> setter without §D-flip9-shared (optional `catalog`/`pool`) would strand it.

## Goal

After this plan: a varco app that wires tenant resolution gets **the chain by default** — from
`TenancyModule` (container-resolved, 051's pool included) or from `VARCO_TENANT_SOURCES` when the
middleware is hand-wired with `chain=None` — and header-only resolution requires an **explicit**
`LegacyTenantSource`; the implicit header fallback still works (12-month floor) but emits one
`varco_core.deprecation` warning (`since="3.2.0", removed_in="5.0.0"`) and two `WARN` findings that
**block a production start** under 054's default. `ClaimTenantMembership` denies on a missing
membership claim unless `VARCO_TENANT_MEMBERSHIP_ON_MISSING=allow` (a `HIGH` finding, blocking
everywhere unless suppressed). `RequestContextMiddleware` **never** sets the tenant — the
`enable_tenant_context`/`tenant_field` kwargs and `_maybe_tenant_context()` are gone; the one HTTP
writer of `current_tenant()` is `TenantResolutionMiddleware._continue()`, which now also works for
`SHARED` apps with no catalog/pool. `TenancyModule` contributes the middleware at
`EdgeSlot.TENANT_RESOLUTION` (inside `ErrorMiddleware`, outside `RequestContextMiddleware`) and the
`TenantPostureCollector`; a non-tenant app emits no `tenant.*` finding. The provisioning-authority
model for `mount_tenant_admin` (FLIP-14) is **recorded as a design with a recommendation** in
`admin-surface-tenancy.md` and this plan — no implementation. Each flip has exactly one greppable
escape hatch, a posture id, a §Migration row, and tests for default, escape hatch, posture, the
ordering invariant, and a regression test that `current_tenant()` is still set by the chain path.

## Non-goals

- **Removing `TenantResolutionMiddleware(chain=None)`** (`TypeError`) — forbidden until
  2027-09-12 (premise (1)); it is the deprecated path this plan re-labels for **5.0.0**.
- **`CrossCheckMode.STRICT` as the default** — 036 row 8 / `BACKLOG.md:57`: a recorded non-flip.
  `tenant-provenance.md:192`'s flip-list row 4 is annotated, not executed (§D-flip6-nonflip).
- **Any `varco_sa`/`varco_beanie` change** (RLS, catalog backends) — 056. **Any change to
  `mount_tenant_admin`, `build_tenant_router`, or their guard** — FLIP-14 is design-only here.
- **Changing what `tenant.*` ids *mean* beyond the two narrowings named in §D-collector**, and
  never removing an id (036 §D-S9-suppress).
- **A new `TenantSource`, a new membership backend, or the parked repository-backed membership
  resolver** (`BACKLOG.md` parks).
- **Binding the resolved `TenantSourceChain` into the container** — 051 decided the chain is
  built, not bound; `TenancyModule` holds it.
- **The `X-Forwarded-Host`/`TrustedHostMiddleware` posture** — 033 §D-S6-oq3, unchanged.
- **A second environment variable for any flip here.** `VARCO_TENANT_SOURCES`,
  `VARCO_TENANT_MEMBERSHIP_ON_MISSING`, `VARCO_SECURITY_SUPPRESS`, `VARCO_SECURITY_ENV` already
  exist and are the only knobs this plan reads.

## Context (verified from source, 2026-09-18)

| Fact | Where | Why it matters |
|---|---|---|
| `TenantResolutionMiddleware.__init__(app, *, catalog, pool, header="X-Tenant-Id", chain=None, server_auth=None, membership=None, reject_status=403)`; `chain is None` → `warnings.warn("… will be removed in 4.0.0 …", DeprecationWarning, stacklevel=2)` then `TenantSourceChain(sources=(LegacyTenantSource(header),))` | `varco_fastapi/varco_fastapi/middleware/tenant_resolution.py:130-165` (warn `:150-162`) | FLIP-6's site. Ad-hoc `warnings.warn`, not `varco_core.deprecation` (premise (2)); `catalog`/`pool` have no default (premise (5)) |
| `dispatch()`: `server_auth` → `chain.resolve()` → act-as record → membership → reject (opaque 403) → `provenance_context` → `_continue()`: `catalog.get()` (404 on `TenantNotFoundError`) → `routing_decision_for_status` → `tenant_context(tenant_id)` → `pool.ensure()` → `call_next` | `:167-257` (`_continue` `:225-257`, `tenant_context` `:253-257`) | **The one chain-path writer of `current_tenant()`** — the regression test pins it. `_continue` is where `catalog`/`pool` become optional (§D-flip9-shared) |
| Module `DESIGN:` block: *"`extra_middleware=` sits OUTSIDE `ErrorMiddleware` … mitigation is mandatory: this middleware catches `HTTPException` from `server_auth`"* | `:42-54`, catch `:178-181` | The catch stays (hand-wiring at `EdgeSlot.EXTRA` remains possible); the block is rewritten to say the module path is inside `ErrorMiddleware` |
| `LegacyTenantSource(header="X-Tenant-Id")`, `name="legacy"`, `trust=LOW`; docstring *"is removed in 4.0"*; *"Does **not** warn on construction (§D-S6-blast)"* | `varco_core/varco_core/tenancy/sources.py:244-276` (`:255`, `:257-259`) | The **explicit** escape hatch; docstring `:255` is wrong (premise (2)) |
| `TenantProvenanceSettings.sources: tuple[str, ...] = ()`; `build_tenant_source_chain(settings=None, *, delegation_policy=None) -> TenantSourceChain \| None` — `None` when `sources` is empty (`:403-404`); `settings=None` reads env (`:401`); `act_as` without a policy → `ValueError` (`:422-427`) | `varco_core/varco_core/tenancy/settings.py:236-275, 371-435` | The env-derived default for a hand-wired middleware (§D-flip6-default). Post-048 the class is a pydantic `VarcoSettings` (`plans/048-…md:151,322`) and post-051 the builder takes `sources=` (`plans/051-…md:286-294`) — the call shape here is unchanged either way |
| 051: `varco_core.tenancy.di.resolve_tenant_source_chain(container) -> TenantSourceChain \| None` reads the 048-bound settings, `collect(container, TenantSource)`, an optional `DelegationPolicy`; unknown/colliding names → `ValueError` at build; *"055 makes the middleware use it"* | `plans/051-…md:97-99, 284-306, 583-587` | The container-resolved default `TenancyModule` passes (§D-flip6-module) |
| `TenantMembershipSettings.on_missing_claim: MissingClaimPolicy = MissingClaimPolicy.ALLOW`; `from_env()` default `"allow"`; `ClaimTenantMembership.__init__(*, claim_key="tenants", on_missing_claim=MissingClaimPolicy.ALLOW)`; `_missing_claim_decision()` warns once per process and allows iff `ALLOW` | `varco_core/varco_core/tenancy/membership.py:129, 149, 285, 328-340`; enum comments `:88-89` | FLIP-7's **three** default sites (post-048 `:149` is the pydantic field default) |
| `enable_tenant_membership(container, settings=None)` — `settings=None` → `TenantMembershipSettings.from_env()`; docstring cites `"allow"` as default | `varco_core/varco_core/tenancy/di.py:77-105` (`:92`) | Docstring updated; wiring unchanged |
| `NullTenantMembership` — scanned `@Singleton(priority=-sys.maxsize - 1)`, `name="null"`, always allows with `reason="no_provider"`; post-045 a `@Fallback` | `membership.py:237-255` | Passing it as `membership=` runs a check that always allows; `inspect_tenant_provenance()` treats a non-`None` provider as "bound" — so `tenant.no_membership_provider` **never fires** in a DI app today (§D-collector narrowing #2) |
| `RequestContextMiddleware.__init__(app, *, server_auth=None, tenant_field="tenant_id", enable_tenant_context=True)`; step 6 `if self._enable_tenant_context and not chain_already_ran: tenant_id = ctx.metadata.get(self._tenant_field); async with _maybe_tenant_context(tenant_id)`; `_maybe_tenant_context()` | `varco_fastapi/varco_fastapi/middleware/request_context.py:125-136, 189-192, 204-225`; docstring steps `:65-78`, deferral `:80-100` | FLIP-9's site — the second, unchained tenant setter (033 Correction 1). `create_varco_app` constructs it with `server_auth=` only (`app.py:1136,1144`), so every 3.x app has the setter on |
| `inspect_tenant_provenance(chain, *, membership=None, delegation=None, request_context_sets_tenant: bool = True)`; `chain is None` → `tenant.no_chain` + `tenant.legacy_source_implicit`; tokens `legacy_source_explicit`, `single_source`, `cross_check_lenient`, `subdomain_trusts_forwarded_host`, `no_membership_provider`, `membership_missing_claim_allows`, `unchained_claim_tenant_setter`, `delegation_unbound` | `varco_core/varco_core/tenancy/posture.py:85-187` (`:90`, `:117-119`, `:140-147`, `:172-183`) | `request_context_sets_tenant` default flips (a **function** — `api_surface.py --check` records it; snapshot regenerated). `_collect_tenant` never passes it (`varco_fastapi/posture.py:494`) → `tenant.unchained_claim_tenant_setter` WARN fires for **every** app today |
| `_tenant_finding_for_token()` severity table: `no_chain` WARN, `legacy_source_implicit` WARN, `no_membership_provider` WARN, `membership_missing_claim_allows` **HIGH**, `cross_check_lenient` INFO, `unchained_claim_tenant_setter` WARN, `delegation_unbound` INFO; **`legacy_source_explicit` and `single_source` are unmapped** → the "Unrecognised … token" INFO default (`:474-481`) | `varco_fastapi/varco_fastapi/posture.py:428-482` | Post-051 this is `TenantPostureCollector.collect()`'s body (`plans/051-…md:321-326`). Two unmapped tokens get rows (§D-collector) |
| 049 `EdgeSlot` (`IntEnum`, outermost first): `CORS=10 SECURITY_HEADERS=20 EXTRA=30 ERROR=40 BODY_LIMIT=50 LOGGING=60 TRACING=70 METRICS=80 RATE_LIMIT_PRE_AUTH=90 REQUEST_CONTEXT=100 RATE_LIMIT_POST_AUTH=110 LOCALIZATION=120 IDEMPOTENCY=130 PROFILING=140`; `HttpContributor` Protocol: `middleware(container) -> Sequence[MiddlewareSpec]`, `configure_app(app, container)` | `plans/049-…md:258-286`; normative order today `varco_fastapi/varco_fastapi/middleware/__init__.py:14-37` | No tenant slot (premise (3)). §D-flip6-slot adds `TENANT_RESOLUTION=95` |
| 049 `TenancyModule(*, pool, supervisor=None, catalog_subscription=None)` wraps `TenancyLifecycle`; `TenancyLifecycle(pool=…)` requires `pool` (`start_sweeper()`/`aclose()`) | `plans/049-…md:111,561`; `varco_fastapi/varco_fastapi/tenancy/lifecycle.py:39-64` | Extended in §D-flip6-module; `TenancyLifecycle` itself is untouched (the module skips it when `pool is None`) |
| 049 §D-posture / 051 §D-posture: `SecurityPostureModule.configure_app()` contributes `TenantPostureCollector(chain, membership, delegation)`, each input resolved *only if* `container.is_resolvable(...)`, else `None` | `plans/049-…md:374-385`; `plans/051-…md:321-333` | Premise (4): `chain=None` for every non-tenant app → two WARNs → production refusal. Re-homed (§D-collector) |
| 053 §D-apikey: `varco_core.deprecation.warn_deprecated(subject, *, since, removed_in, replacement=None, stacklevel=_CALLER_STACKLEVEL)` — the parameter-path deprecation helper | `plans/053-…md:322-337`; `varco_core/varco_core/deprecation.py:75-97` (`_build_message`) | Reused for the `chain=None` path (§D-flip6-floor). Whichever of 053/055 lands first adds it, with this exact signature |
| 054: `enforce` derives `refuse` unless `VARCO_SECURITY_ENV=development`; `HIGH` blocks everywhere, unsuppressed `WARN` blocks in `production`; severity-table rows `tenant.no_chain`/`legacy_source_implicit`/`no_membership_provider`/`unchained_claim_tenant_setter` WARN *"(flips: 055)"*, `membership_missing_claim_allows` HIGH, `cross_check_lenient`/`delegation_unbound` INFO; autouse `_posture_warn_only` fixture in `varco_fastapi/tests/conftest.py` | `plans/054-…md:148-192, 253-306 (rows :284-286), 239-251` | This plan's `tenant.*` rows replace the grouped row; new tests that assert refusal `delenv` the fixture's variable (054 §D-flip2-tests) |
| `test_tenant_chain_middleware.py`: `TestChainNoneDeprecationWarning` (one warning at construction, explicit legacy warns not at all `:98-131`); `TestMiddlewareSignaturesPinned` pins both `__init__` signatures — `RequestContextMiddleware`'s kw-only set is exactly `{server_auth, tenant_field, enable_tenant_context}` (`:323-338`); `_build_app()` passes `chain=None` by default (`:71-82`); `/ping` returns `current_tenant()` and the provenance tenant (`:84-93`) | `varco_fastapi/tests/test_tenant_chain_middleware.py` | The regression-test scaffold; the signature test is rewritten |
| In-repo `chain=None` constructions: `test_tenant_resolution_middleware.py:36`, `test_tenant_event_path_middleware.py:54`, `test_tenant_chain_middleware.py:71-82` (default), `test_localization_middleware_ordering.py:84` (never instantiated — ordering only), `test_request_context_deferral.py:190-202`, `test_act_as_middleware.py:46-50` | `rg -n 'TenantResolutionMiddleware' varco_fastapi/tests` | Sweep list for Step 9 |
| In-repo `enable_tenant_context`/`tenant_field=` (middleware sense): **only** `test_tenant_chain_middleware.py:329-337` and the posture remediation string `varco_fastapi/posture.py:465` | `rg -n 'enable_tenant_context' varco_*/ examples/` | FLIP-9's in-repo blast radius is two sites; `examples/00-full-stack-post-api` never reads `current_tenant()` (its cache key reads `ctx.metadata["tenant_id"]` directly, `example/service.py:36`) |
| `varco_fastapi/pyproject.toml` `[tool.pytest.ini_options] filterwarnings` — four `ignore::DeprecationWarning:<lib>` rules, no `error:` rule | `varco_fastapi/pyproject.toml:136-140` | Gains one scoped `error:` rule (053's mechanical-guard pattern) so no in-repo test relies on the implicit path silently |
| `mount_tenant_admin`/`build_tenant_router`: `server_auth` mandatory (`ValueError`), `admin_role="tenant-admin"` enforced via `require_roles(admin_role)` on every route through `_admin` dependency; routes provision/list/get/suspend-resume/delete/migrate/request-provision/activate/readiness | `varco_fastapi/varco_fastapi/tenancy/router.py:52-137` (`:113-136`) | FLIP-14's subject: a role is the single line of defence (036 §D-S4-control ❌) |
| `TokenProfile(name, issuers, token_type, audiences, required_claims, implied_roles, implied_scopes)` + `require_token_profile(*names) -> RouteGuard`; `AuditingAuthorizer`/`enable_authorization_audit` | `varco_core/varco_core/jwt/profile.py:60-92`; `varco_fastapi/varco_fastapi/auth/guard.py:332`; `varco_core/varco_core/auth/audit.py:97`, `auth/di.py:49` | Shipped machinery FLIP-14's recommended design composes (§D-flip14) |
| CHANGELOG release dates: `3.2.0 — 2026-09-12`, `3.1.0 — 2026-09-05`, `3.0.0 — 2026-08-31` | `CHANGELOG.md:49,425,823` | The floor arithmetic (premise (1)) |

## Design

### §D-flip6-floor — the default flips to the chain; the implicit-legacy path stays, deprecated for 5.0.0; posture is the production gate

**Chosen: `TenantResolutionMiddleware(chain=None)` now means "the configured chain" — the
container-resolved chain when constructed by `TenancyModule`, the env-derived chain
(`build_tenant_source_chain()`) when hand-wired — and only when *no* chain is configured does it
fall back to the implicit `LegacyTenantSource`, emitting `warn_deprecated("TenantResolutionMiddleware(chain=None) with no VARCO_TENANT_SOURCES", since="3.2.0", removed_in="5.0.0", replacement="TenantSourceChain(sources=(LegacyTenantSource(),)) or VARCO_TENANT_SOURCES=…")`
and reporting `tenant.no_chain` + `tenant.legacy_source_implicit` (WARN → blocks a production
start under 054).**

```python
# tenant_resolution.py — __init__, replacing :150-165
self._chain_implicit = False
if chain is None:
    chain = build_tenant_source_chain()            # env-derived (048: TenantProvenanceSettings() reads VARCO_TENANT_*)
if chain is None:
    warn_deprecated(
        "TenantResolutionMiddleware(chain=None) with no VARCO_TENANT_SOURCES configured",
        since="3.2.0", removed_in="5.0.0",
        replacement="an explicit TenantSourceChain(sources=(LegacyTenantSource(),)) for header-only "
                    "resolution, or VARCO_TENANT_SOURCES=jwt[,…] (see tenant-provenance.md)",
        stacklevel=3,
    )
    chain = TenantSourceChain(sources=(LegacyTenantSource(header),))
    self._chain_implicit = True
self._chain = chain
```

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: default = configured chain; implicit legacy survives as the deprecated path (`removed_in="5.0.0"`); WARN findings block production** | Honours `CONTRIBUTING.md:98`'s floor to the day (a 3.2 caller who ignored the warning keeps working through 4.x); delivers the row's *intent* — "tenant resolution defaults to the chain" — on both construction paths; the loud half is 054's derived `refuse`: a production app on the implicit fallback **refuses to start** with two named ids and a remediation, which is strictly stronger than a `TypeError` at import for the deployment that matters, and weaker (a warning) only in `development`; converts the ad-hoc `warnings.warn` into the one mechanism (`deprecation.py:17-19`), so `rg 'removed_in="5\.' varco_*/varco_*` finds it; `since="3.2.0"` is the truth — the window opened then | The BACKLOG row's literal `TypeError` does not land in 4.0 (⚠️ flagged; 058 carries it to 5.0.0's list); two resolution inputs for one builder — env-only when hand-wired, container when module-wired (§D-flip6-module says why that is acceptable) |
| (2) `chain=None` → `TypeError` (the row verbatim) | One line; the 3.2 warning promised it | **Removes a deprecated path 12 months early** — a direct `CONTRIBUTING.md:98` violation, the same reason the nine `removed_in="4.0.0"` sites are not swept (`plans/000-…md:10-12`, `BACKLOG.md:54`). Rejected outright |
| (3) `chain=None` with no sources → `ValueError` "configure VARCO_TENANT_SOURCES" (fail-closed at construction, not a `TypeError`) | Fail-closed at startup in every environment | The same removal under another exception class; the floor is about the *path*, not the exception. Rejected |
| (4) keep 3.2 semantics exactly (`chain=None` → implicit legacy, warning only) and flip only the module path | Zero surprise for a hand-wirer | A hand-wirer with `VARCO_TENANT_SOURCES=jwt` exported and `chain=None` would silently stay header-only — the exact "you think it is on" failure `inspect_*()` inspectors exist to catch; the README's shipped snippet (`README.md:1244-1250`) already does `chain = build_tenant_source_chain(...)` by hand, so folding it into the default is what the snippet promised. Rejected |

**Escape hatch (one greppable line):** header-only resolution is `chain=TenantSourceChain(sources=(LegacyTenantSource(),))`
in code, or `VARCO_TENANT_SOURCES=legacy` in env — both are the *explicit* form, warn not at all
(033 §D-S6-blast, unchanged), and report `tenant.legacy_source_explicit` (WARN — §D-collector) so
production needs `VARCO_SECURITY_SUPPRESS=tenant.legacy_source_explicit` too (the 054 FLIP-10 /
053 `posture.base_authorizer_bound` two-statement pattern: the opt-out and its acknowledgement).

**Posture id:** `tenant.no_chain` + `tenant.legacy_source_implicit` (both WARN, unchanged
severity; detail/remediation rewritten — §D-collector). No new id for the flip itself.

### §D-flip6-slot — `EdgeSlot.TENANT_RESOLUTION = 95`, inside `ErrorMiddleware`, outside `RequestContextMiddleware`

`varco_fastapi/middleware/slots.py` (049) gains `TENANT_RESOLUTION = 95` between
`RATE_LIMIT_PRE_AUTH = 90` and `REQUEST_CONTEXT = 100`. Resulting order (outermost first):

```
… METRICS=80  RATE_LIMIT_PRE_AUTH=90  **TENANT_RESOLUTION=95**  REQUEST_CONTEXT=100  RATE_LIMIT_POST_AUTH=110  LOCALIZATION=120 …
```

- ✅ **Outside `RequestContextMiddleware`** — the §D-S6-wiring deferral (`request_context.py:154-160`:
  provenance published ⇒ chain ran) requires tenant resolution to run first; pinned today by
  `test_request_context_deferral.py:196-202`. ✅ **Before `LocalizationMiddleware`** — RD-3 /
  CLAUDE.md's ordering rule (`test_localization_middleware_ordering.py:65-95` stays green
  unmodified). ✅ **Inside `RATE_LIMIT_PRE_AUTH`** — the chain calls `server_auth` (a JWT verify);
  IP-scoped pre-auth limiting must protect that cost. ✅ **Outside `RATE_LIMIT_POST_AUTH`** — a
  `TENANT`-scoped rule reads `current_tenant()`, which `_continue()` sets. ✅ **Inside
  `ErrorMiddleware`/logging/tracing/metrics** — a chain rejection is now an enveloped, logged,
  traced, metric'd 403 instead of an unobserved response outside the stack; the middleware's own
  `HTTPException` catch (`:178-181`) is kept (hand-wiring at `EXTRA` still works) and its `DESIGN:`
  block is rewritten to say so.
- ❌ An app that hand-placed the middleware at `EXTRA` (3.x `extra_middleware=`) and moves to
  `TenancyModule` sees rejected requests appear in access logs/metrics for the first time — a
  §Migration row, and the intended behaviour.
- Rejected — **`EXTRA` as the module's slot** (byte-identical position): ❌ leaves a
  `ServiceException` from `server_auth` unrendered by the envelope — the §D-order-bugs defect
  049 fixed for `IdempotencyMiddleware`, re-created for tenancy. Rejected — **between `ERROR=40`
  and `BODY_LIMIT=50`**: ❌ runs the JWT verify before the pre-auth rate limit.

### §D-flip6-module — `TenancyModule` becomes the composition root for tenant resolution

`TenancyModule` (049, `varco_fastapi/modules/…`) gains the `HttpContributor` methods and these
constructor keywords (all keyword-only, all optional — 049's `pool` becomes optional, §D-flip9-shared):

```python
class TenancyModule(VarcoModule):
    name = "tenancy"
    depends_on = (FastAPIModule,)          # needs AbstractServerAuth's binding to exist before middleware()

    def __init__(self, *, pool=None, catalog=None, supervisor=None, catalog_subscription=None,
                 chain: TenantSourceChain | None = None, server_auth=None, membership=None,
                 header: str = "X-Tenant-Id", reject_status: int = 403, enabled=None) -> None: ...

    def middleware(self, container) -> Sequence[MiddlewareSpec]:
        chain = self._chain if self._chain is not None else resolve_tenant_source_chain(container)   # 051
        server_auth = self._server_auth or (container.get(AbstractServerAuth) if container.is_resolvable(AbstractServerAuth) else None)
        membership = self._membership or (container.get(AbstractTenantMembership) if container.is_resolvable(AbstractTenantMembership) else None)
        self._resolved = (chain, server_auth, membership)
        return [MiddlewareSpec(EdgeSlot.TENANT_RESOLUTION, TenantResolutionMiddleware,
                               {"catalog": self._catalog, "pool": self._pool, "header": self._header,
                                "chain": chain, "server_auth": server_auth, "membership": membership,
                                "reject_status": self._reject_status})]

    def configure_app(self, app, container) -> None:
        contribute(container, PostureCollector, TenantPostureCollector(               # §D-collector
            chain=chain, membership=membership, delegation=<DelegationPolicy if resolvable>,
            catalog_gated=self._catalog is not None, chain_implicit=(chain is None)))

    def validate(self, container) -> Sequence[str]:
        # SCHEMA/DATABASE isolation with pool=None cannot route — fail before start()
        settings = container.get(TenancySettings)
        if settings.isolation is not TenantIsolation.SHARED and self._pool is None:
            return [f"TenancyModule(pool=None) under isolation={settings.isolation.value}: a per-tenant "
                    "resource pool is required to route requests; pass pool=TenantResourcePool(...)"]
        return ()

    async def start/stop(...): TenancyLifecycle(pool=…, supervisor=…, catalog_subscription=…) only when pool is not None
```

- `chain=None` passed *through* to the middleware when `resolve_tenant_source_chain()` returns
  `None` — the middleware then takes its own §D-flip6-floor path (env re-read returns `None`
  again → deprecated implicit legacy). One deprecation site, one message, regardless of path.
- ✅ The module sees the container, so 051's multi-bound pool sources and the bound
  `DelegationPolicy` reach the chain; the hand-wired path (no container) cannot — and 051's
  builder **fails closed** on a pool-only name (`"unknown tenant source 'x'"`, `plans/051-…md:297-298`),
  so a hand-wirer naming a plugin source gets a `ValueError` at construction whose message this
  plan extends with *"pass chain= or use TenancyModule"*. ✅ `depends_on=(FastAPIModule,)`
  makes `AbstractServerAuth`'s binding present before `middleware()` runs (049 orders
  `middleware()` collection after every `register()`). ✅ `TenancyLifecycle` is unchanged; the
  module simply does not construct it without a pool.
- ❌ `TenancyModule` grows from three keywords to nine. Accepted: the alternative — a separate
  `TenantResolutionModule` — is two modules for one subsystem, against 049's "one module per
  subsystem" and 015 §1's module unit; and `pool`/`catalog`/`chain` are facts about one thing.
- Rejected — **the middleware resolves from `DIContainer.current()`**: 046 removed every
  service-locator site (`DI-8`); a middleware reading a global container would be the 33rd.

### §D-flip6-nonflip — `CrossCheckMode.STRICT` stays out; the doc row is annotated

`tenant-provenance.md:192` lists *"`CrossCheckMode.STRICT` becomes the default"* as flip-list
row 4; 036 §D-S9-flip row 8 (`:557`) and `BACKLOG.md:57` record it as **not a flip**. This plan
annotates row 4 *"recorded non-flip — 036 row 8; `tenant.cross_check_lenient` stays INFO"* rather
than deleting it (answered-not-deleted style). No code change.

### §D-flip7 — `on_missing_claim` defaults `DENY` at all three sites; `allow` is the one hatch; the HIGH finding is the acknowledgement

`membership.py:129` (`TenantMembershipSettings` field), `:149` (`from_env`/pydantic default
`"allow"` → `"deny"`), `:285` (`ClaimTenantMembership.__init__`) → `MissingClaimPolicy.DENY`.
`_missing_claim_decision()` (`:328-340`) is unchanged in shape — it still logs **once per
process** (now: *"…on_missing_claim=deny — callers without a %r claim are denied (reason=claim_absent)"*;
a fleet whose IdP does not emit the claim will 403 everywhere, and the one log line is how an
operator finds out why) and returns `allowed = on_missing_claim is ALLOW`. The self-tenant branch
(`:297-299`) is untouched — a single-tenant token whose `tenant_id` equals the resolved tenant is
still its own proof, so `enable_tenant_membership()` on a single-tenant fleet keeps working with
**no IdP change** (033 §D-S5-claim ✅ #3, preserved).

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: flip all three defaults; keep `ALLOW` as an explicit value; `tenant.membership_missing_claim_allows` stays HIGH** | The row verbatim (033 row 2, 036 row 7); brief 006 §2 rule 5 (*"fail closed on missing tenant"*); the self-tenant branch keeps the no-IdP-change path; one env var restores 3.2 (`VARCO_TENANT_MEMBERSHIP_ON_MISSING=allow`), and because the finding is **HIGH** that opt-out **blocks every environment** under 054 unless `VARCO_SECURITY_SUPPRESS=tenant.membership_missing_claim_allows` — the opt-out and its acknowledgement, both greppable | A multi-org fleet whose IdP has not yet added the `tenants` claim goes 403 on every cross-tenant request at upgrade — the sharpest consequence in this plan; §Migration row 2 first, CHANGELOG **BREAKING** |
| (2) flip only the env default, keep the constructor default `ALLOW` | Smaller | Two defaults for one field; a code caller constructing `ClaimTenantMembership()` stays fail-open with no signal. Rejected — 048's rule is one default per field, stated once |
| (3) remove `MissingClaimPolicy.ALLOW` | No fail-open possible | Removes a 3.2 public enum member with no deprecation window (floor); and a deliberate, suppressed `allow` during an IdP rollout is a legitimate, time-boxed shape. Rejected |

**Escape hatch:** `VARCO_TENANT_MEMBERSHIP_ON_MISSING=allow` (env) or
`enable_tenant_membership(container, settings=TenantMembershipSettings(on_missing_claim=MissingClaimPolicy.ALLOW))` /
`ClaimTenantMembership(on_missing_claim=MissingClaimPolicy.ALLOW)` (code). **Posture id:**
`tenant.membership_missing_claim_allows` (HIGH, unchanged; detail text gains *"explicitly opted
out of the 4.0 default"*).

### §D-flip9 — the kwargs and `_maybe_tenant_context()` are removed outright; `RequestContextMiddleware` never sets the tenant

`RequestContextMiddleware.__init__(app, *, server_auth=None)` — `tenant_field` and
`enable_tenant_context` are **deleted**, `_maybe_tenant_context()` (`:204-225`) is **deleted**,
step 6 (`:189-192`) becomes `response = await call_next(request)` unconditionally inside
`auth_context()`. The `chain_already_ran and auth_context_var.get() is not None` short-circuit
(`:170-175`) stays — it is the zero-extra-`server_auth`-calls guarantee, not the tenant setter.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: remove the kwargs and the path** | 033 row 3's second clause verbatim (*"setting the tenant from a claim without a chain, a catalog check or a membership check is removed"*); after it, `rg -n 'tenant_context\(' varco_fastapi/varco_fastapi` hits **exactly one** HTTP writer (`tenant_resolution.py:255`) — the single-decision-point property §D-S6-wiring wanted, now structural; a wiring kwarg on a middleware `create_varco_app`/`FastAPIModule` constructs is a *wiring* shape — `BACKLOG.md:53` allows breaking those outright, and the kwarg was never deprecated so no floor clock is running on it; a 3.x caller passing either kwarg gets Python's `TypeError: unexpected keyword argument` — loud, at construction, `mypy`-visible | The row's first clause (*"defaults `False`"*) is **subsumed**: a removed path has no default. Recorded here so the BACKLOG row's wording is not read as "the kwarg must survive" |
| (2) `enable_tenant_context: bool = False`; `True` → `ValueError("removed; adopt a chain")` | Keeps the name for one cycle | A tombstone kwarg that exists only to raise; `tenant_field` would have to stay too. Rejected — the `TypeError` already names the kwarg |
| (3) keep `enable_tenant_context=True` as "mirror the chain's tenant" | Nothing to remove | The chain path already enters `tenant_context()` in `_continue()`; a second entry would be a no-op or a disagreement. Rejected |

**Escape hatch:** none — a removed path has no hatch. The replacement is `VARCO_TENANT_SOURCES=jwt`
(+ `VARCO_TENANT_CLAIM_METADATA_KEY=<your claim>` where `tenant_field` differed) with
`TenancyModule(...)` — §D-flip9-shared makes that possible for the class of app that used the
setter. **Posture id:** `tenant.unchained_claim_tenant_setter` stays (ids are never removed),
documented as *"unreachable through varco since 4.0; the id remains for a hand-wired setter"*
(054 §D-flip3-posture's precedent); `inspect_tenant_provenance(request_context_sets_tenant=False)`
becomes the default and `TenantPostureCollector` never passes `True`.

### §D-flip9-shared — `catalog`/`pool` become optional so a `SHARED` app can adopt the chain

`TenantResolutionMiddleware.__init__(…, catalog: Any | None = None, pool: Any | None = None, …)`;
`_continue()`:

```python
if tenant_id is None: return await call_next(request)
if self._catalog is not None:
    …existing status gate unchanged (:236-251)…
with tenant_context(tenant_id):
    if self._pool is not None:
        await self._pool.ensure(tenant_id)
    return await call_next(request)
```

- ✅ Premise (5): the class of app FLIP-9 hits hardest — `SHARED` isolation, JWT `tenant_id` claim,
  no control plane — now has a **one-line-per-fact** replacement (`TenancyModule()` +
  `VARCO_TENANT_SOURCES=jwt`) that is strictly stronger than what it loses: trust-ranked
  provenance, cross-check, optional membership, one decision point. ✅ Both kwargs stay
  keyword-only (the pinned signature test `test_tenant_chain_middleware.py:305-321` only adds
  `default is None` assertions). ✅ `TenancyModule.validate()` (§D-flip6-module) refuses
  `pool=None` under `SCHEMA`/`DATABASE` isolation, so "optional" cannot silently mean
  "unrouted" where routing is load-bearing.
- ❌ A `SHARED` app without a catalog has no suspend/delete gate at the edge — but it never had
  one (the setter it replaces checked nothing), and the new **`tenant.no_catalog_gate`** INFO
  finding says so (§D-collector). ❌ Widening two required kwargs to optional is a class
  `__init__` change `api_surface.py --check` cannot see — the signature test is the guard.
- Rejected — **a `NullTenantCatalog` that returns `ACTIVE` for every id**: ❌ a Null Object that
  *asserts a status* it does not know is a lie in the routing decision; `None` says "no gate".

### §D-collector — `TenantPostureCollector` is contributed by `TenancyModule`; two meanings narrow; four rows are added

**Chosen: `TenancyModule.configure_app()` contributes the collector (051's `contribute()` verb);
`SecurityPostureModule` stops contributing a tenant collector; a non-tenant app emits no
`tenant.*` finding.** `TenantPostureCollector(chain, membership, delegation, *, catalog_gated: bool,
chain_implicit: bool)`; its body is `inspect_tenant_provenance(chain, membership=…, delegation=…)`
(default `request_context_sets_tenant=False`) mapped through `_tenant_finding_for_token`, plus the
two additive facts below.

| `check` | Severity | Change | Detail / remediation (new text) |
|---|---|---|---|
| `tenant.no_chain` | WARN (unchanged) | **narrowed**: fires only when a `TenantResolutionMiddleware` is wired with no chain (implicit legacy) — never for an app with no tenancy | *"Tenant resolution is wired but no TenantSourceChain is configured — the deprecated implicit X-Tenant-Id fallback is active (removed in 5.0.0)."* / *"Set `VARCO_TENANT_SOURCES=jwt[,…]`, or name header-only explicitly: `TenantSourceChain(sources=(LegacyTenantSource(),))`."* |
| `tenant.legacy_source_implicit` | WARN (unchanged) | co-fires with the above, as today | remediation as above |
| `tenant.legacy_source_explicit` | **WARN (was the unmapped INFO default)** | mapped | *"An explicit LegacyTenantSource (LOW trust, unbound header) is in the chain — safe only behind an ingress that strips and re-writes the header."* / *"Prefer a claim/subdomain source; if the ingress guarantee holds, `VARCO_SECURITY_SUPPRESS=tenant.legacy_source_explicit`."* — the opt-out needs its acknowledgement (054/057's rule) |
| `tenant.single_source` | INFO (was the unmapped default) | mapped | *"One source in the chain — no cross-check is possible."* / *"Add a second source if your deployment has one."* |
| `tenant.no_membership_provider` | WARN (unchanged) | **narrowed #2**: also fires when the bound provider is `NullTenantMembership` (`name == "null"`) — today a DI app never fires it because Null is "bound" | *"No membership check runs — the resolved tenant is never verified against the caller (brief 006 §2)."* / *"`enable_tenant_membership(container)`; a deliberate single-tenant deployment: suppress."* |
| `tenant.membership_missing_claim_allows` | HIGH (unchanged) | reachable only by explicit opt-out after FLIP-7 | *"`on_missing_claim=allow` — explicitly opted out of the 4.0 fail-closed default."* / *"Unset `VARCO_TENANT_MEMBERSHIP_ON_MISSING`; during an IdP rollout, suppress by id."* |
| `tenant.unchained_claim_tenant_setter` | WARN (unchanged) | unreachable through varco after FLIP-9; id kept | *"…(the path was removed in 4.0; this id remains for hand-wired setters)"* |
| `tenant.no_catalog_gate` | **INFO (new)** | `catalog_gated is False` | *"TenantResolutionMiddleware has no catalog — a suspended/deleted tenant is not rejected at the edge (SHARED isolation without a control plane)."* / *"Pass `catalog=` to `TenancyModule` if you provision/suspend tenants."* |
| `tenant.third_party_source_high_trust` | INFO (051) | listed for completeness | 051's text |
| `tenant.cross_check_lenient`, `tenant.delegation_unbound`, `tenant.subdomain_trusts_forwarded_host` | INFO / INFO / (unmapped → INFO default; mapped here as **WARN**: *"Host header trusted from `X-Forwarded-Host` — only correct behind a proxy that overwrites it"*) | `subdomain_trusts_forwarded_host` mapped | — |

- ✅ Premise (4) closed: `tenant.*` exists iff `TenancyModule` is present — the same "the module
  that owns the fact contributes the collector" rule 051 gave `JwksRefreshModule`/`RetentionModule`
  (`plans/051-…md:100-102`). ✅ 049's *"resolved only if `is_resolvable`"* rule still applies
  inside the module for `DelegationPolicy`. ✅ Every previously unmapped token gets a real row,
  so the "Unrecognised … token" default (`posture.py:474-481`) is reachable only by a genuinely
  new token.
- ❌ A hand-wired `TenantResolutionMiddleware` (via `MiddlewareModule`) is **unobserved** by
  posture — a Pitfalls row; `varco diagnose` (052) lists `TenancyModule: absent`. Accepted: 049
  made the module the documented composition root.
- Rejected — **`SecurityPostureModule` keeps contributing, but only when `TenancyModule` is in
  `ModuleRegistry`**: ❌ cross-module knowledge in the posture module, and it still could not know
  the *resolved* chain/membership the middleware was actually built with.

### §D-flip14 — provisioning authority for `mount_tenant_admin` (**design recorded; implementation is its own future plan**)

**The problem (036 §D-S4-control ❌, `:213`):** the most powerful surface in the platform —
`POST /tenants`, `PATCH … suspend|resume`, `DELETE`, `migrate` — has a single line of defence: a
role string (`"tenant-admin"`) on whatever token any issuer the registry trusts chose to mint.
Membership binding (brief 006 §2) is a category error here (provisioning *creates* the scope), so
the fix must be about **who may hold provisioning power** and **what each holder may do**, not
about which tenant they belong to.

| Option | ✅ | ❌ |
|---|---|---|
| **(A) Per-operation capability scopes** — every route requires a scope (`tenant:provision`, `tenant:suspend`, `tenant:deprovision`, `tenant:migrate`, `tenant:read`) via `require_scopes()` on top of `admin_role` | brief 006 §3: *"never grant unrestricted delegation — scope it … read user data, not delete"*; least privilege per verb (a CI job that provisions cannot delete); shipped `RouteGuard` machinery; audit-visible (`AuditingAuthorizer` records the scope check) | Still purely token-borne: any issuer the `TrustedIssuerRegistry` trusts — including the tenant-user IdP — can mint the scope. Necessary, not sufficient |
| **(B) A signed provisioning `TokenProfile`** — `TokenProfile(name="tenant-provisioning", issuers={<control-plane issuer>}, audiences={"varco:tenancy-admin"}, token_type="provisioning", required_claims={"actor"})`; every control-plane route gated by `require_token_profile("tenant-provisioning")` | Binds authority to a **dedicated issuer + audience** — an ordinary user token from the tenant IdP cannot match even if it carries the role (closes the role-only weakness at its root); the `act` claim (RFC 8693 delegation, `required_claims={"actor"}`) makes the *human-behind-the-automation* visible, satisfying brief 006 §3's *"logged at issuance AND use"* (CVE-2025-55241's lesson); shipped machinery (`TokenProfile`, `TokenProfileRegistry`, `require_token_profile`, `JwtAuthority` for the internal issuer); the profile is declarative config, so `inspect_*` can report *"tenant admin is profile-gated"* | Requires an internal issuer or an IdP that does token exchange — real application work (the same class 033 §D-S6-blast called out); `implied_roles` on the profile must **not** be used to grant `tenant-admin` (the profile plan's Risks — a profile that grants the role it gates is circular) |
| **(C) A control-plane tenant with membership** — a reserved tenant id (`__control_plane__`); callers must be *members* of it (`AbstractTenantMembership.check(ctx, CONTROL_PLANE)`); `mount_tenant_admin` runs `assert_tenant_matches` against it | Reuses brief 006 §2's membership rule uniformly — dissolves 036's "category error" by making provisioning tenant-scoped *to the control plane*; membership is fail-closed after FLIP-7 | Leaks a varco-reserved id into every IdP's `tenants` claim; collides with `reserved_labels`/subdomain semantics and with catalogs that must never contain it; a membership claim is still minted by the same IdP that mints user tokens, so it does not separate issuers the way (B) does |
| (D) Status quo + S11 audit + `posture.tenant_admin_mounted` | Nothing to build | 036 §D-S4-control's own ❌: one line of defence. Rejected as the end state |

**Recommendation: (B) as the authority gate, layered with (A) as the capability gate, keeping
`admin_role` as the third barrier — three independent statements per request: *the token is a
provisioning token* (issuer/audience/type), *it may perform this verb* (scope), *its subject holds
the role* (role); every decision through `AuditingAuthorizer`.** (C) is recorded as the shape for
a platform whose IdP cannot mint a second profile (membership in a `platform-admins` organisation
stands in for the issuer split) — weaker, but better than (D).

Sketch the future plan will refine (not built here):
`mount_tenant_admin(app, control_service, *, acknowledge_bundled_admin, server_auth, admin_role="tenant-admin",
provisioning_profile: str | None = None, operation_scopes: Mapping[str, str] | None = None, …)`;
`provisioning_profile=None` keeps today's role-only guard and emits a new
**`posture.tenant_admin_role_only` (WARN)** — so under 054 a production mount without a profile
needs an explicit suppression; `operation_scopes` defaults to the five names above. **`TenantAdminModule`?** No — RD-9 (never a `create_varco_app` kwarg/module for a privileged mount) stands.

**BACKLOG status for `FLIP-14` after this plan: `designed (Plan 055 §D-flip14) — implementation is
its own future plan`.** No step below writes code for it; Step 13 writes the design into
`admin-surface-tenancy.md` and flips the row.

### §D-tests — where each proof lives

| Proof | File |
|---|---|
| FLIP-7 default/hatch/log | `varco_core/tests/test_tenant_membership.py`, `test_tenant_membership_di.py` |
| FLIP-7/9 posture defaults, narrowing #2, new rows | `varco_core/tests/test_tenant_posture.py`; `varco_fastapi/tests/test_posture_collectors.py` |
| FLIP-6 default resolution (env → chain; none → deprecated legacy), warning text/count, explicit legacy silent, `catalog`/`pool` optional, signature pins | `varco_fastapi/tests/test_tenant_chain_middleware.py` (extended) |
| FLIP-6 slot + module: order invariant, contribution, `validate()` refusal, collector homed in the module, no `tenant.*` without the module | `varco_fastapi/tests/test_middleware_slots.py` (049, extended), new `varco_fastapi/tests/test_tenancy_module.py` |
| FLIP-9 removal + **the regression test** that `current_tenant()` is still set by the chain path | `varco_fastapi/tests/test_request_context_deferral.py` (extended), new `varco_fastapi/tests/test_tenant_single_writer.py` |
| Production refusal under 054 for the implicit fallback / the `allow` opt-out (both `delenv` `VARCO_SECURITY_ENFORCE`) | `varco_fastapi/tests/test_tenant_flips_posture.py` (new) |

### §D-snapshots — what must and must not be regenerated

| Artefact | Effect | Action |
|---|---|---|
| `design/api-freeze-and-standards/measurements/api-surface.{json,md}` | `inspect_tenant_provenance` is a **function** — its `request_context_sets_tenant` default changes → `--check` fails until regenerated. `warn_deprecated` (if 055 lands before 053) is an addition (note only). `TenantResolutionMiddleware`, `RequestContextMiddleware`, `TenancyModule`, `EdgeSlot`, `TenantMembershipSettings` are classes — invisible; each gets a signature/field test | Regenerate + commit in Step 12 |
| 048's settings-reference snapshot | `TenantMembershipSettings.on_missing_claim` default `allow` → `deny` | regenerate in Step 3 |
| `asyncapi-example.json`, `import-budget.json`, 047's catalogue | none | untouched |

## Steps

All commands run from `/home/edoardo/projects/varco`. Phase A (FLIP-7) → Phase B (FLIP-6) →
Phase C (FLIP-9) → Docs → Verification. A is independent; C depends on B's optional
`catalog`/`pool` (§D-flip9-shared) so the regression test has a `SHARED`-shaped fixture.

1. [x] **Preflight.** Record in `/tmp/claude-1000/-home-edoardo-projects-varco/625af1a2-0cf8-4199-b924-0b3f986861e6/scratchpad/055-preflight.txt`:
       every `file:line` in §Context re-read (`tenant_resolution.py:130-165,167-257`,
       `sources.py:244-276`, `settings.py:236-275,371-435`, `membership.py:129,149,285,328-340`,
       `tenancy/di.py:77-105`, `request_context.py:125-136,154-194,204-225`,
       `tenancy/posture.py:85-187`, `varco_fastapi/posture.py:428-495`, `tenancy/lifecycle.py:39-64`,
       `tenancy/router.py:52-137`, `test_tenant_chain_middleware.py:64-131,298-338`,
       `varco_fastapi/pyproject.toml:136-140`); `CHANGELOG.md:49` still reads `3.2.0 — 2026-09-12`
       (the floor date 2027-09-12 in §D-flip6-floor depends on it); which of 048 (`VarcoSettings`,
       `bind_settings`, the settings snapshot script; whether `TenantMembershipSettings`/
       `TenantProvenanceSettings` are already pydantic), 049 (`varco_fastapi/middleware/slots.py`,
       `EdgeSlot` values, `TenancyModule`'s file and signature, `HttpContributor`, `MiddlewareSpec`,
       `default_modules()`), 051 (`resolve_tenant_source_chain`, `contribute`, `PostureCollector`,
       `TenantPostureCollector`'s file), 053 (`warn_deprecated` present?), 054 (`_posture_warn_only`
       fixture, the severity table section in `security-posture.md`) are in the tree — each later
       step names its pre-/post- form. Sweep lists: `rg -n 'TenantResolutionMiddleware' varco_fastapi/tests examples/`
       (chain=None constructions), `rg -n 'enable_tenant_context|tenant_field=' varco_*/tests examples/ README.md technical_docs/`,
       `rg -n 'on_missing_claim|VARCO_TENANT_MEMBERSHIP_ON_MISSING' varco_*/ README.md technical_docs/`,
       `rg -n 'removed in 4\.0|removed_in="4\.0\.0"' varco_core/varco_core/tenancy varco_fastapi/varco_fastapi/middleware`.
       Measure the blast radius: with `VARCO_SECURITY_ENV= VARCO_SECURITY_ENFORCE=` and a
       **temporary** local `DENY` default, `uv run pytest varco_core/tests/test_tenant_membership*.py varco_fastapi/tests/test_tenant_chain_middleware.py -q -x`
       → record which tests fail (expected: the `ALLOW`-default tests `test_tenant_membership.py:76-93,114-120`,
       `test_tenant_membership_di.py:44-56,100-107`). Revert. **Verify**: the file exists with ≥ 16
       confirmations and the measured lists.

### Phase A — FLIP-7

2. [x] `varco_core/tests/test_tenant_membership.py` (red) — **replace** the implicit-default
       assumptions: `test_missing_claim_denies_by_default` (`ClaimTenantMembership()` with no
       `tenants` claim and a non-matching `tenant_id` → `allowed is False`, `reason == "claim_absent"`);
       `test_default_policy_is_deny_signature_guard` (`inspect.signature(ClaimTenantMembership.__init__).parameters["on_missing_claim"].default is MissingClaimPolicy.DENY`
       and `TenantMembershipSettings().on_missing_claim is MissingClaimPolicy.DENY` — the class
       guard `api_surface --check` cannot provide); `test_self_tenant_still_allows_under_default`
       (the no-IdP-change path: `metadata={"tenant_id": "acme"}`, no `tenants` → `allowed`,
       `"self_tenant"`); `test_explicit_allow_still_allows_and_warns_once` (`:76-93` re-pointed at
       an explicit `on_missing_claim=ALLOW`); `test_deny_logs_once_per_process_with_deny_in_message`
       (`caplog`: exactly one WARNING containing `on_missing_claim=deny` across two denied checks;
       reset the module flag via `monkeypatch.setattr(mod, "_missing_claim_warned", False)`).
       `test_tenant_membership_di.py`: `test_settings_none_uses_env_backed_defaults` (`:44-56`) →
       `delenv VARCO_TENANT_MEMBERSHIP_ON_MISSING` → provider's `on_missing_claim is DENY`;
       `test_env_allow_is_the_escape_hatch` (`setenv allow` → `ALLOW`); `test_defaults_with_no_env`
       (`:100-107`) → `DENY`. **Verify**: red on every new/changed default test; the explicit-`ALLOW`
       tests pass today (controls).

3. [x] `varco_core/varco_core/tenancy/membership.py` — `:129`, `:149` (or the pydantic field
       default post-048), `:285` → `MissingClaimPolicy.DENY`; enum comments `:88-89` → `ALLOW`
       *"the 3.2 default — explicit opt-out since 4.0 (Plan 055 / FLIP-7); reported as
       `tenant.membership_missing_claim_allows` (HIGH)"*, `DENY` *"the default since 4.0"*; module
       `DESIGN:` block (`:7-35`) title *"fail-open in 3.2"* → a *"DESIGN: fail-closed by default
       since 4.0 (Plan 055 / FLIP-7)"* block with §D-flip7's ✅/❌ and the rejected (2)/(3);
       `_missing_claim_decision()` log text per §D-flip7; class docstrings `:115-117`, `:274-276`.
       `tenancy/di.py:89-92` docstring: default `"deny"`. If 048's settings snapshot exists,
       regenerate. **Verify**: Step 2 green; `uv run pytest varco_core/tests/test_tenant_membership*.py -q`.

### Phase B — FLIP-6

4. [x] `varco_core/varco_core/deprecation.py` — **only if 053 has not landed** `warn_deprecated()`:
       add it with exactly 053 §D-apikey's signature (`plans/053-…md:327-328`), docstring
       (`Args`/`Edge cases`: *"for one parameter path of a constructor that stays; `stacklevel`
       must count the constructor frame"*), `__all__` entry; test
       `varco_core/tests/test_deprecation.py::test_warn_deprecated_message_and_stacklevel`
       (`pytest.warns(DeprecationWarning, match=r"is deprecated since 3\.2\.0 and will be removed in 5\.0\.0")`;
       `filename` of the record is the caller's). **Verify**: green; if 053 landed first, this step
       is a no-op and the note is recorded in the preflight file.

5. [x] `varco_fastapi/tests/test_tenant_chain_middleware.py` (red) — in `TestChainNoneDeprecationWarning`
       (`:98-131`): `test_chain_none_warns_once…` (`:99-123`) gains `monkeypatch.delenv("VARCO_TENANT_SOURCES", raising=False)`
       and asserts the message matches `r"TenantResolutionMiddleware\(chain=None\).*since 3\.2\.0.*removed in 5\.0\.0"`;
       **add** `test_chain_none_with_env_sources_builds_the_chain_and_does_not_warn`
       (`setenv VARCO_TENANT_SOURCES=jwt`, `server_auth=_FakeServerAuth(tenant_id="acme")`, **no**
       header → `/ping` returns `tenant == "acme"`, `provenance_tenant == "acme"`; `warnings.simplefilter("error", DeprecationWarning)`
       around the first request — must not raise); `test_chain_none_with_env_legacy_is_explicit_not_implicit`
       (`setenv VARCO_TENANT_SOURCES=legacy` → header read, no warning, `inspect_tenant_provenance`
       on the middleware's chain reports `legacy_source_implicit is False`); `test_env_sources_naming_pool_only_source_fails_closed_when_hand_wired`
       (`setenv VARCO_TENANT_SOURCES=partner` → `ValueError` at first request whose message contains
       `"pass chain="` and `"TenancyModule"`); `test_header_kwarg_still_honoured_on_implicit_path`
       (`header="X-Org"` + no env → implicit legacy reads `X-Org`). In `TestMiddlewareSignaturesPinned`
       `:305-321`: add `assert sig.parameters["catalog"].default is None` and the same for `pool`.
       **New class** `TestOptionalCatalogAndPool`: `catalog=None, pool=None, chain=<jwt chain>` →
       `/ping` sets `current_tenant()`, no `ensure()` call (`_CountingPool` not passed → assert via a
       sentinel pool that fails if called), no 404 for an unknown tenant; `catalog=None, pool=<counting>`
       → `ensure()` called once; `catalog=<fake with suspended>, pool=None` → 403/503 per
       `routing_decision_for_status` (the gate still runs when a catalog is given). Then
       `varco_fastapi/pyproject.toml` `filterwarnings` gains
       `"error:TenantResolutionMiddleware\\(chain=None\\):DeprecationWarning"` (053's scoped-`error`
       pattern) — the three tests *about* the implicit path wrap in `pytest.warns(DeprecationWarning, match=…)`.
       **Verify**: red on the env-derived tests and the optional-kwarg tests; the `error:` rule makes
       `test_tenant_resolution_middleware.py`/`test_tenant_event_path_middleware.py` red (swept in Step 9).

6. [x] `varco_fastapi/varco_fastapi/middleware/tenant_resolution.py` — §D-flip6-floor's `__init__`
       body (import `build_tenant_source_chain` from `varco_core.tenancy.settings`, `warn_deprecated`
       from `varco_core.deprecation`; catch 051's `ValueError` from the builder and re-raise with
       *"… pass chain= explicitly or wire TenantResolutionMiddleware through TenancyModule, which
       can see multi-bound TenantSources"* appended); `catalog: Any | None = None`, `pool: Any | None = None`
       and §D-flip9-shared's `_continue()`; `self._chain_implicit` exposed as a read-only property
       `chain_implicit` (the module's collector reads it); module docstring: the `chain=None`
       `DESIGN:` block (`:18-40`) rewritten to §D-flip6-floor (✅/❌, the floor date, the rejected
       `TypeError`), the `extra_middleware` block (`:42-54`) rewritten (*"since 4.0 `TenancyModule`
       places this at `EdgeSlot.TENANT_RESOLUTION`, inside `ErrorMiddleware`; the `HTTPException`
       catch is kept for hand-wiring at `EXTRA`"*); class docstring `Args` for `catalog`/`pool`
       (*"`None` (default) — no status gate / no `ensure()`; `SHARED` isolation without a control
       plane"*), `chain` (*"`None` → the configured chain from `VARCO_TENANT_SOURCES`; nothing
       configured → the deprecated implicit `LegacyTenantSource` (removed in 5.0.0)"*), `Edge cases`
       gains the no-catalog and no-pool bullets. `varco_core/varco_core/tenancy/sources.py:255`
       → *"The **implicit** fallback that builds it is deprecated (removed in 5.0.0); an explicit
       `LegacyTenantSource` in a chain is the documented, permanent escape hatch."*; `:257-259`
       still true, kept. **Verify**: Step 5 green.

7. [x] `varco_fastapi/tests/test_middleware_slots.py` (049; red) — `test_tenant_resolution_slot_position`:
       `EdgeSlot.RATE_LIMIT_PRE_AUTH < EdgeSlot.TENANT_RESOLUTION < EdgeSlot.REQUEST_CONTEXT` and
       `EdgeSlot.ERROR < EdgeSlot.TENANT_RESOLUTION`; the existing "the enum *is* the order table"
       test's expected list gains `TENANT_RESOLUTION=95`. New **`varco_fastapi/tests/test_tenancy_module.py`**
       (red): `test_module_contributes_middleware_at_tenant_resolution_slot` (`TenancyModule(catalog=…, pool=…)`
       → `middleware(container)` yields one `MiddlewareSpec` with `slot is EdgeSlot.TENANT_RESOLUTION`,
       `target is TenantResolutionMiddleware`); `test_module_resolves_chain_from_container`
       (048-bound `TenantProvenanceSettings(sources=("jwt",))` via `bind_settings(instance=…)` → the
       spec's `kwargs["chain"]` has one `JwtClaimTenantSource`); `test_module_passes_multibound_pool_source`
       (a scanned `PartnerHeaderTenantSource` from 051's example package, named in settings → in the
       chain — the property the hand-wired path cannot have); `test_explicit_chain_kwarg_wins`;
       `test_module_resolves_server_auth_and_membership_when_bound` (bound `AbstractServerAuth` and
       `enable_tenant_membership(container)` → both in `kwargs`); `test_module_without_pool_skips_lifecycle`
       (`pool=None` → `start()`/`stop()` do not construct `TenancyLifecycle`; a sentinel pool's
       `start_sweeper` never called); `test_validate_refuses_pool_none_under_schema_isolation`
       (`TenancySettings(isolation=SCHEMA)` bound + `pool=None` → `validate()` returns one string
       naming `pool=`; `SHARED` → `()`); `test_full_app_order_invariant` — `create_varco_app(container, modules=[*default_modules(), TenancyModule(catalog=…, pool=…)])`
       → in `app.user_middleware` order, `TenantResolutionMiddleware` sits **after** `ErrorMiddleware`,
       after any `RateLimitMiddleware(stage=PRE_AUTH)`, **before** `RequestContextMiddleware` and
       `LocalizationMiddleware` (assert by class index, the `test_localization_middleware_ordering.py:65-95`
       style — that file stays green unmodified). **Verify**: red on the module tests.

8. [x] `varco_fastapi/varco_fastapi/middleware/slots.py` — `TENANT_RESOLUTION = 95` with a
       comment citing §D-flip6-slot; `middleware/__init__.py:14-37` order table gains the line
       `TenantResolutionMiddleware (TenancyModule; inside ErrorMiddleware, before RequestContext — Plan 055)`.
       `TenancyModule` (049's file) — §D-flip6-module's signature, `middleware()`, `configure_app()`
       (collector contribution lands in Step 11 — leave a `# Step 11` marker or land both together),
       `validate()`, the pool-conditional lifecycle; class docstring (`Args` for every kwarg, `Edge
       cases`: *"`chain=None` + no `VARCO_TENANT_SOURCES` → the middleware's deprecated implicit
       fallback, one warning, two WARN findings"*, *"`pool=None` is legal only under `SHARED`"*),
       a `DESIGN: the module is the composition root for tenant resolution (Plan 055 / FLIP-6)`
       block. `varco_fastapi/__init__.py` re-export unchanged (049 already exports `TenancyModule`).
       **Verify**: Step 7 green; `uv run pytest varco_fastapi/tests/test_localization_middleware_ordering.py varco_fastapi/tests/test_request_context_deferral.py -q` green **unmodified**.

9. [x] In-repo sweep — `test_tenant_resolution_middleware.py:36`, `test_tenant_event_path_middleware.py:54`
       and every other `chain=None` construction from the Step 1 list (except the three tests
       *about* the implicit path) pass `chain=TenantSourceChain(sources=(LegacyTenantSource(<header>),))`
       explicitly (the `_legacy_chain()` helper at `test_tenant_chain_middleware.py:64-68` moves to
       `varco_fastapi/tests/_tenant_fixtures.py` and is imported); `test_tenant_chain_middleware.py::_build_app`
       default `chain=None` → `chain=_legacy_chain()` so incidental tests stop exercising the
       deprecated path (the deprecation tests pass `chain=None` explicitly). README `:1238-1254`
       snippet: drop the hand `build_tenant_source_chain(...)` line (the default does it) and show
       the module form first. **Verify**: `make test PKG=varco_fastapi` green with the `error:` rule;
       `rg -n 'chain=None' varco_fastapi/tests` → only the three deprecation tests.

### Phase C — FLIP-9

10. [x] `varco_fastapi/tests/test_request_context_deferral.py` (red) + new
        **`varco_fastapi/tests/test_tenant_single_writer.py`** — `test_request_context_middleware_never_sets_tenant`
        (`RequestContextMiddleware(server_auth=<ctx with metadata tenant_id="acme">)` alone → a
        route reading `current_tenant()` returns `None`); `test_removed_kwargs_raise_type_error`
        (`RequestContextMiddleware(app, enable_tenant_context=True)` and `(…, tenant_field="org")`
        → `TypeError` mentioning the kwarg); **the regression test**
        `test_current_tenant_is_set_by_the_chain_path_after_setter_removal` — the real
        `TenantResolutionMiddleware(catalog=None, pool=None, chain=TenantSourceChain((JwtClaimTenantSource(),)), server_auth=auth)`
        outside the real `RequestContextMiddleware(server_auth=auth)` (the `test_request_context_deferral.py:190-202`
        construction), a JWT-shaped `AuthContext(metadata={"tenant_id": "acme"})`, **no**
        `X-Tenant-Id` header → `current_tenant() == "acme"`, `current_tenant_provenance().tenant_id == "acme"`,
        `server_auth` called **exactly once** (the deferral guarantee survives the removal), and
        `get_auth_context()` is the same object; the same through `create_varco_app(container, modules=[*default_modules(), TenancyModule()])`
        with `VARCO_TENANT_SOURCES=jwt` (the module path) — the two paths agree;
        `test_only_one_http_writer_of_tenant_context` — `rg`-style scan: the set of files under
        `varco_fastapi/varco_fastapi/` containing `tenant_context(` is exactly `{"middleware/tenant_resolution.py"}`
        (the structural single-decision-point property, mechanical). Update
        `test_tenant_chain_middleware.py:323-338` → the kw-only set is exactly `{"server_auth"}`.
        **Verify**: red on the removal tests; the regression test passes **today** (control —
        proves the chain path already writes the tenant; it must stay green after Step 11).

11. [x] `varco_fastapi/varco_fastapi/middleware/request_context.py` — delete `tenant_field`/
        `enable_tenant_context` (`:130-131,135-136`), step 6 (`:189-194` → the unconditional
        `call_next`), `_maybe_tenant_context` (`:201-225`) and the `AsyncIterator`/`asynccontextmanager`
        imports if now unused; docstring steps `:65-78` (step 6 → *"Never enters `tenant_context()`
        — since 4.0 the only HTTP writer of `current_tenant()` is `TenantResolutionMiddleware`
        (Plan 055 / FLIP-9)"*), the deferral paragraph `:80-100` reduced to the auth-reuse
        contract, `Args` `:106-110` removed, module docstring `:10` (*"tenant_context() — tenant
        isolation"*) removed from the wired list; a `DESIGN: no tenant setter here (Plan 055 / FLIP-9)`
        block with §D-flip9's ✅/❌. `varco_core/varco_core/tenancy/posture.py:90` →
        `request_context_sets_tenant: bool = False`; docstring `:103-105` (*"`True` only for a
        hand-wired setter — varco's own middleware never sets the tenant since 4.0"*); the
        `TenantProvenancePosture.unchained_claim_tenant_setter` docstring `:65-66`. Then the
        **collector** (§D-collector): `TenantPostureCollector.__init__(chain, membership, delegation, *, catalog_gated=True, chain_implicit=False)`;
        `collect()` = the mapped tokens + `tenant.no_catalog_gate` when `not catalog_gated`; the
        narrowing #2 (`membership` whose `name == "null"` → treated as `None` before calling
        `inspect_tenant_provenance`); `_tenant_finding_for_token`'s table gains
        `legacy_source_explicit` (WARN), `single_source` (INFO), `subdomain_trusts_forwarded_host`
        (WARN), `no_catalog_gate` (INFO), `third_party_source_high_trust` (INFO, 051's text) and the
        rewritten texts of §D-collector; `:465` remediation no longer names `enable_tenant_context`.
        `SecurityPostureModule.configure_app()` (049/051) **stops** contributing a tenant collector;
        `TenancyModule.configure_app()` contributes it (Step 8's marker). Tests (red first, same
        step): `varco_core/tests/test_tenant_posture.py` — `test_default_request_context_flag_is_false`
        (no `unchained_claim_tenant_setter` finding by default; present with `True`),
        `test_null_membership_counts_as_no_provider` (via the collector in `varco_fastapi/tests/test_posture_collectors.py`);
        `varco_fastapi/tests/test_posture_collectors.py` — `test_no_tenant_findings_without_tenancy_module`
        (`create_varco_app(container, modules=default_modules())` → posture report has **no**
        `tenant.*` id), `test_tenancy_module_contributes_tenant_collector`, `test_every_tenant_token_has_a_severity_row`
        (every token string in `varco_core/tenancy/posture.py` + 051's token appears as a key in
        `_tenant_finding_for_token`'s table — the unmapped-default is unreachable). New
        **`varco_fastapi/tests/test_tenant_flips_posture.py`** (both variables `delenv`ed, 054
        §D-flip2-tests): `test_implicit_legacy_refuses_production_start` (`TenancyModule()` with no
        `VARCO_TENANT_SOURCES` → `RuntimeError` naming `tenant.no_chain` and `tenant.legacy_source_implicit`);
        `test_explicit_legacy_refuses_until_suppressed` (`VARCO_TENANT_SOURCES=legacy` → refuses
        naming `tenant.legacy_source_explicit`; with `VARCO_SECURITY_SUPPRESS=tenant.legacy_source_explicit,tenant.no_membership_provider`
        → starts); `test_membership_allow_optout_refuses_everywhere` (`enable_tenant_membership` +
        `VARCO_TENANT_MEMBERSHIP_ON_MISSING=allow`, `VARCO_SECURITY_ENV=development` → **still**
        refuses — HIGH); `test_jwt_chain_with_membership_starts_clean_in_production`
        (`VARCO_TENANT_SOURCES=jwt` + `enable_tenant_membership` + `catalog=` → starts; findings
        are INFO only). **Verify**: Step 10 green; the regression test still green; `make test PKG=varco_core`;
        `make test PKG=varco_fastapi`; `make test PKG=examples`.

12. [x] `uv run python scripts/api_surface.py` — `inspect_tenant_provenance`'s signature changes
        (and `warn_deprecated` appears if Step 4 ran). Commit the snapshot. **Verify**:
        `uv run python scripts/api_surface.py --check` clean; the `.md` diff shows exactly those.

### Docs (same commit as the code — feedback memory)

13. [x] `technical_docs/features/tenant-provenance.md` — header `:6` (*"Nothing flips by default in
        3.2"*) → the 4.0 statement; §"`LegacyTenantSource`" `:144-152` (*"is removed in 4.0"* →
        the implicit fallback is deprecated for 5.0.0, the floor date, the explicit source is
        permanent); §"The membership model, and its 3.2 fail-open" `:92-120` → *"fail-closed since
        4.0"* with the self-tenant no-IdP-change path called out; **§"The 4.0 flip list"** `:185-192`:
        row 1 → *"**landed with the floor, Plan 055** — default is the chain; implicit fallback
        deprecated `since=3.2.0, removed_in=5.0.0` (not before 2027-09-12); production blocks via
        `tenant.no_chain`/`tenant.legacy_source_implicit`"*, row 2 → *"landed, Plan 055"*, row 3 →
        *"landed, Plan 055 — kwargs removed; `TenancyModule` + `VARCO_TENANT_SOURCES=jwt` is the
        replacement; `catalog`/`pool` optional for `SHARED`"*, row 4 → the §D-flip6-nonflip
        annotation; a new **§"Wiring tenant resolution in 4.0"** (the `TenancyModule` form,
        `EdgeSlot.TENANT_RESOLUTION`, the hand-wired form and what it cannot see); §Pitfalls
        `:194+` rows: *"`TenancyModule()` with nothing configured → refuses in production
        (`tenant.no_chain`) → `VARCO_TENANT_SOURCES=jwt`, or explicit legacy + suppression"*;
        *"Tokens without a `tenants` claim → 403 `claim_absent` since 4.0 → emit the claim (or
        rely on self-tenant), or `…ON_MISSING=allow` + suppress the HIGH id during rollout"*;
        *"`current_tenant()` is `None` after upgrade → the JWT-claim setter is gone → `TenancyModule`
        + `VARCO_TENANT_SOURCES=jwt` (+ `VARCO_TENANT_CLAIM_METADATA_KEY` if you used `tenant_field`)"*;
        *"Hand-wired `TenantResolutionMiddleware` is invisible to posture → use `TenancyModule`"*;
        *"Rejected requests now appear in logs/metrics/traces → the module slot is inside
        `ErrorMiddleware`; `MiddlewareModule([...], slot=EXTRA)` keeps the 3.x position"*;
        *"`VARCO_TENANT_SOURCES=partner` (a plugin source) hand-wired → `ValueError` → the module
        path"*; env table `:212+`: `VARCO_TENANT_MEMBERSHIP_ON_MISSING` default `deny`.
      - `technical_docs/features/security-posture.md` — flip list `:159,160,162` → *"landed, Plan
        055"* (row 6 with the floor note), `:167` (row 14) → *"designed, Plan 055 §D-flip14 —
        implementation is its own future plan"*; **the severity table (054)**: replace the grouped
        `tenant.*` row with one row per id per §D-collector's table (add `legacy_source_explicit`
        WARN, `single_source` INFO, `subdomain_trusts_forwarded_host` WARN, `no_catalog_gate` INFO,
        `third_party_source_high_trust` INFO; note `unchained_claim_tenant_setter` *"unreachable
        through varco since 4.0"*, `no_chain`/`legacy_source_implicit` *"only with `TenancyModule`"*);
        §Pitfalls: *"A stock app with `TenancyModule()` and no `VARCO_TENANT_SOURCES` refuses in
        production"*.
      - `technical_docs/features/admin-surface-tenancy.md` — §D-S4-control `:30-42`: append
        **§"The provisioning-authority model (designed in Plan 055, not built)"** = §D-flip14's
        table, recommendation, sketch, and the `posture.tenant_admin_role_only` intent, ending
        *"implementation is its own future plan; BACKLOG `FLIP-14` carries it as `designed`"*.
      - `technical_docs/features/multitenancy.md` `:41-43` (the provenance pointer: *"Nothing here
        changes"* → *"since 4.0 the chain is the default; `TenancyModule` wires the middleware"*),
        §Pitfalls `:954+`: the `pool=None`-under-`SCHEMA` refusal row.
      - `technical_docs/features/timezone-handling.md:314-325` and `http-edge-hardening.md`'s
        ordering table: `TenantResolutionMiddleware` now has a slot (`TENANT_RESOLUTION=95`).
      - `README.md` — `:1228-1254` (the "Nothing flips by default" sentence and the snippet →
        module form; hand-wired form second, with the `chain=None` default explained);
        `:1275-…` membership snippet: default `deny`, the self-tenant note; the multitenancy
        env table: `VARCO_TENANT_MEMBERSHIP_ON_MISSING` `deny`; a "Wiring an app" cross-reference
        to `TenancyModule` (049's section).
      - `ARCHITECTURE.md` `:1177` (the tenancy diagram line gains *"(EdgeSlot.TENANT_RESOLUTION, via
        TenancyModule)"*), `:2288` (the `extra_middleware=` sentence → the module slot).
      - `CLAUDE.md` — §"Tenant identity provenance & delegation" **Rule** `:1094` (*"`chain=None` is
        byte-identical to pre-3.2…"*) → *"`TenantResolutionMiddleware(chain=None)` means **the
        configured chain** (Plan 055 / FLIP-6); with nothing configured it falls back to the
        deprecated implicit `LegacyTenantSource` (`removed_in="5.0.0"` — the 12-month floor from
        3.2.0 forbids removing it in 4.0; never re-introduce the `TypeError` before 2027-09-12) and
        blocks a production start through `tenant.no_chain`"*; **add** *"`RequestContextMiddleware`
        never sets the tenant — the only HTTP writer of `current_tenant()` is
        `TenantResolutionMiddleware._continue()` (Plan 055 / FLIP-9; `test_tenant_single_writer.py`
        enforces it). Never add a second one"*; *"`ClaimTenantMembership.on_missing_claim` is `DENY`
        (Plan 055 / FLIP-7); `allow` is an explicit, HIGH-reported opt-out"*; *"`tenant.*` posture
        findings are contributed by `TenancyModule`, never by `SecurityPostureModule` — a
        non-tenant app must emit none (Plan 055 §D-collector)"*; §"Multitenancy" Rule on
        `mount_tenant_admin`: append *"its provisioning-authority model is designed in Plan 055
        §D-flip14 and unbuilt — do not add a tenant guard, and do not build the model without
        that plan"*; the decision tree's `TenantResolutionMiddleware` line `:1492` gains *"(wired by
        `TenancyModule`, slot `TENANT_RESOLUTION`)"*. One line each — link, don't inline.
      **Verify**: `rg -n 'Nothing flips by default|fail-open in 3\.2|removed in 4\.0|enable_tenant_context|chain=None\) → `TypeError`' README.md technical_docs/ CLAUDE.md ARCHITECTURE.md varco_*/varco_*`
      → 0 hits outside CHANGELOG history and the annotated flip-list rows.

14. [x] `CHANGELOG.md` — under `## [Unreleased]` (`:10`), above existing blocks:
        `### BREAKING — Tenant-provenance default flips (Plan 055, FLIP-6 / FLIP-7 / FLIP-9)` with
        bullets: FLIP-6 (default = configured chain on both construction paths; the implicit
        fallback **kept**, `warn_deprecated(since="3.2.0", removed_in="5.0.0")` — *"the 12-month
        floor from 3.2.0 (2026-09-12) forbids the `TypeError` the 3.2 warning promised; it moves to
        5.0.0"*; `EdgeSlot.TENANT_RESOLUTION=95`; `TenancyModule` gains `catalog`/`chain`/`server_auth`/
        `membership`/`header`/`reject_status`, `pool` optional, `validate()`; `catalog`/`pool`
        optional on the middleware; `tenant.legacy_source_explicit`/`single_source`/
        `subdomain_trusts_forwarded_host`/`no_catalog_gate` mapped; `tenant.*` only with
        `TenancyModule`; `NullTenantMembership` counts as no provider); FLIP-7 (three defaults →
        `DENY`; `VARCO_TENANT_MEMBERSHIP_ON_MISSING=allow`; HIGH finding blocks everywhere unless
        suppressed; self-tenant path unchanged); FLIP-9 (`tenant_field`/`enable_tenant_context`
        removed → `TypeError`; `_maybe_tenant_context` gone; `inspect_tenant_provenance(request_context_sets_tenant=False)`
        default; the replacement). A `### Documentation` bullet: *"FLIP-14 provisioning-authority
        model recorded (Plan 055 §D-flip14) — design only."* End with *"Migration: this plan's
        §Migration (assembled into the 4.0 guide by Plan 058)."* `BACKLOG.md:70,98,99` — **only if
        the launching thread has not already done it**: status → `done (Plan 055)`; `:98`'s feature
        cell gains *"(`TypeError` deferred to 5.0.0 — 12-month floor from 3.2.0)"*; `:101` (`FLIP-14`)
        → `designed (Plan 055 §D-flip14) — implementation is its own future plan`. **Verify**:
        `rg -n 'Plan 055' CHANGELOG.md BACKLOG.md` → ≥ 1 / ≥ 4.

### Verification

15. [x] `make test` (all eleven suites; 054's autouse fixture keeps default apps booting; the
        FLIP tests `delenv` it); `make lint` (no-`PKG` path: `api-check` clean **with the
        regenerated snapshot**, `asyncapi-check` unchanged, `import-budget --warn-only` no breach
        — no new top-level import in any `__init__.py`); `make type-check` clean (no new
        `type: ignore`); `rg -n 'tenant_context\(' varco_fastapi/varco_fastapi` → exactly
        `middleware/tenant_resolution.py`; `rg -n 'removed_in="5\.0\.0"' varco_fastapi/varco_fastapi/middleware/tenant_resolution.py`
        → 1; `rg -n 'warnings\.warn' varco_fastapi/varco_fastapi/middleware/tenant_resolution.py`
        → 0; `rg -n 'MissingClaimPolicy\.ALLOW' varco_core/varco_core` → only the enum member and
        docstrings (no default); the Step 1 blast-radius list re-run **without** the FLIP tests'
        `delenv` reproduces the refusals (proof the flips are real), then green with it.

## Edge cases

- **`chain=None`, `VARCO_TENANT_SOURCES` unset, hand-wired** → implicit `LegacyTenantSource(header)`,
  one `DeprecationWarning` (message: *"…deprecated since 3.2.0 and will be removed in 5.0.0…"*),
  byte-identical resolution to 3.2 (header absent → pass through; present → that tenant).
- **`chain=None`, `VARCO_TENANT_SOURCES=jwt`, hand-wired, `server_auth=None`** → the chain has a
  JWT source that never sees an `AuthContext` → zero claims → pass through; no warning; posture
  (module path only) shows `tenant.single_source`. Documented: a claim source needs `server_auth`.
- **`chain=None`, `VARCO_TENANT_SOURCES=act_as`, hand-wired** → 051's builder `ValueError` (no
  `DelegationPolicy`), re-raised with the *"pass chain= or use TenancyModule"* suffix. Fail-closed
  at construction (first request), never a silent header fallback.
- **`TenancyModule()` with nothing configured, `production`** → `RuntimeError` at startup naming
  `tenant.no_chain`, `tenant.legacy_source_implicit`, `tenant.no_membership_provider` (Null
  counts as none), `tenant.no_catalog_gate` is INFO. `development` → starts, all demoted.
- **`TenancyModule(pool=None)` under `SCHEMA`** → `validate()` string → 052's preflight refuses
  before any `start()`; under `SHARED` → fine.
- **`TenancyModule(chain=X)` and `VARCO_TENANT_SOURCES=jwt` both set** → `X` wins (explicit code
  over env, 048's precedence); the env value is not merged.
- **An app lists `TenancyModule` *and* hand-places `TenantResolutionMiddleware` via `MiddlewareModule`**
  → two instances at two slots; each resolves; the inner one sees provenance already set and
  re-resolves anyway (it is not `RequestContextMiddleware`). Not guarded — a Pitfalls row; 052's
  report shows both specs.
- **`ClaimTenantMembership()` (no args) with a token that has `tenant_id == resolved` and no
  `tenants`** → `allowed`, `"self_tenant"` — unchanged; **with neither** → denied, `"claim_absent"`,
  one WARNING per process.
- **`VARCO_TENANT_MEMBERSHIP_ON_MISSING=Allow`/`ALLOW`** → `ValueError` at settings construction
  (today's `_LEGAL_ON_MISSING` check / 048's `Literal`) — never normalised.
- **`RequestContextMiddleware(server_auth=auth)` behind a chain that ran with `server_auth=None`**
  → still authenticates (the "still authenticates" branch, `:176-181`), never touches the tenant.
- **A 3.x app that read `current_tenant()` in a service with only `RequestContextMiddleware`** →
  `None` → `TenantAwareService` raises its no-tenant error on every scoped call — loud, not a
  silent cross-tenant read (brief 006 §2 rule 5). §Migration row 3.
- **`inspect_tenant_provenance(chain)` called by hand with a `NullTenantMembership`** → unchanged
  (the narrowing lives in the collector, not the pure inspector — the inspector reports what it is
  given; a hand caller who wants the narrowing passes `membership=None`).
- **Suppressing `tenant.membership_missing_claim_allows`** → demoted to INFO, does not block (054
  `:382` semantics) — the sanctioned time-boxed IdP-rollout shape.

## Migration (3.x → 4.0; Plan 058 assembles this section verbatim)

| Before (3.2) | After (4.0) | Action if you relied on the old behaviour |
|---|---|---|
| `TenantResolutionMiddleware(catalog=…, pool=…)` (no `chain=`) read `X-Tenant-Id`, one `DeprecationWarning` | If `VARCO_TENANT_SOURCES` is set, `chain=None` **builds that chain** (new behaviour). If not, the implicit header fallback still runs — one `DeprecationWarning` (`since 3.2.0, removed in 5.0.0`) and `tenant.no_chain` + `tenant.legacy_source_implicit` (WARN) which **refuse a production start** when wired through `TenancyModule` | Adopt a chain: `VARCO_TENANT_SOURCES=jwt` (+ `server_auth`). Header-only on purpose: `chain=TenantSourceChain(sources=(LegacyTenantSource(),))` or `VARCO_TENANT_SOURCES=legacy`, plus `VARCO_SECURITY_SUPPRESS=tenant.legacy_source_explicit` in production. The `TypeError` the 3.2 warning promised arrives in **5.0.0**, not 4.0 |
| `enable_tenant_membership(container)` allowed a caller whose token had no `tenants` claim (one WARNING) | Denied — 403, `reason="claim_absent"`. A single-tenant token whose `tenant_id` equals the resolved tenant is still allowed (`self_tenant`) | Emit the `tenants` claim from your IdP. Rollout window: `VARCO_TENANT_MEMBERSHIP_ON_MISSING=allow` **and** `VARCO_SECURITY_SUPPRESS=tenant.membership_missing_claim_allows` (HIGH — blocks every environment otherwise) |
| `RequestContextMiddleware` set `current_tenant()` from the JWT `tenant_id` claim (on by default, no catalog/membership check) | **Removed.** `current_tenant()` is set only by `TenantResolutionMiddleware`. `TenantAwareService` raises on scoped calls; `tenancy_cache_key()` fails closed | Add `TenancyModule()` to `modules=` (`catalog=`/`pool=` optional under `SHARED`) and `VARCO_TENANT_SOURCES=jwt`; a custom `tenant_field="org_id"` → `VARCO_TENANT_CLAIM_METADATA_KEY=org_id`. Consider `enable_tenant_membership(container)` — it is now fail-closed and reported |
| `RequestContextMiddleware(tenant_field=…, enable_tenant_context=…)` | `TypeError: unexpected keyword argument` | Drop both kwargs |
| `TenantResolutionMiddleware` placed via `extra_middleware=` (outside `ErrorMiddleware`) | `TenancyModule` places it at `EdgeSlot.TENANT_RESOLUTION` (95) — inside `ErrorMiddleware`/logging/tracing/metrics, before `RequestContextMiddleware`. Rejections are now enveloped and observed | Nothing; to keep the 3.x position, `MiddlewareModule([(TenantResolutionMiddleware, {...})])` (slot `EXTRA`) — unobserved by posture |
| `TenancyModule(pool=…)` (049, `pool` required) | `pool` optional; `catalog`, `chain`, `server_auth`, `membership`, `header`, `reject_status` added; `validate()` refuses `pool=None` under `SCHEMA`/`DATABASE` | Nothing for existing callers |
| `inspect_tenant_provenance(chain)` defaulted `request_context_sets_tenant=True` (so `tenant.unchained_claim_tenant_setter` fired for every app) | Default `False`; the id is unreachable through varco | Pass `True` only for a hand-wired setter |
| `tenant.*` findings appeared for every app (chain `None` when no tenancy) | Only apps with `TenancyModule` emit them; `NullTenantMembership` now counts as *no* provider (`tenant.no_membership_provider` WARN → blocks production) | Tenant apps: `enable_tenant_membership(container)`, or suppress the id for a deliberate single-tenant deployment |
| `tenant.legacy_source_explicit`, `tenant.single_source`, `tenant.subdomain_trusts_forwarded_host` rendered as "Unrecognised … token" INFO | WARN / INFO / WARN with real text; new `tenant.no_catalog_gate` INFO | Suppress `tenant.legacy_source_explicit` if the ingress guarantee holds |

## Docs

Listed in Step 13 (feature docs, README, ARCHITECTURE, CLAUDE.md) and Step 14 (CHANGELOG,
BACKLOG). No new `technical_docs/features/*.md` — FLIP-14's design is a new **section** of
`admin-surface-tenancy.md` (the doc that owns §D-S4-control), the 4.0 wiring shape a new section
of `tenant-provenance.md`. Plan 058 folds §Migration into the consolidated guide and adds the
`chain=None` `TypeError` to 5.0.0's removal list.

## Verification

```bash
# Preflight (Step 1)
rg -n '3\.2\.0\] — 2026-09-12' CHANGELOG.md                                      # the floor clock
rg -n 'TenantResolutionMiddleware' varco_fastapi/tests examples/                # chain=None sweep list
rg -n 'enable_tenant_context|tenant_field=' varco_*/tests examples/ README.md technical_docs/

# Phase gates
uv run pytest varco_core/tests/test_tenant_membership.py varco_core/tests/test_tenant_membership_di.py -q          # A
uv run pytest varco_fastapi/tests/test_tenant_chain_middleware.py varco_fastapi/tests/test_middleware_slots.py \
              varco_fastapi/tests/test_tenancy_module.py varco_fastapi/tests/test_localization_middleware_ordering.py -q   # B
uv run pytest varco_fastapi/tests/test_request_context_deferral.py varco_fastapi/tests/test_tenant_single_writer.py \
              varco_core/tests/test_tenant_posture.py varco_fastapi/tests/test_posture_collectors.py \
              varco_fastapi/tests/test_tenant_flips_posture.py -q                                                  # C

# Package + gates
make test
uv run python scripts/api_surface.py            # regenerate: inspect_tenant_provenance default (+ warn_deprecated if new)
make lint                                        # api-check clean / asyncapi-check unchanged / import-budget no breach
make type-check

# Boundary checks
rg -n 'tenant_context\(' varco_fastapi/varco_fastapi                              # exactly middleware/tenant_resolution.py
rg -n 'warnings\.warn' varco_fastapi/varco_fastapi/middleware/tenant_resolution.py  # 0
rg -n 'removed_in="5\.0\.0"' varco_fastapi/varco_fastapi/middleware/tenant_resolution.py  # 1
rg -n 'MissingClaimPolicy\.ALLOW' varco_core/varco_core                           # enum member + docstrings only
rg -n 'chain=None' varco_fastapi/tests                                            # the three deprecation tests only
rg -n 'TENANT_RESOLUTION' varco_fastapi/varco_fastapi/middleware/slots.py         # = 95
git diff --stat -- design/api-freeze-and-standards/measurements/asyncapi-example.json   # empty
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| **The BACKLOG row's `TypeError` does not land in 4.0** (⚠️ premise (1)) | `CONTRIBUTING.md:98`'s floor is never violated: the implicit-legacy path works through 4.x; removal is a 5.0.0 item | §D-flip6-floor; the `removed_in="5.0.0"` string; 058's removal list; the BACKLOG cell says so |
| **A multi-org fleet 403s at upgrade** (FLIP-7) — the sharpest consequence here | The self-tenant path keeps single-tenant fleets working with no IdP change; the opt-out is one env var and is HIGH-reported | §Migration row 2 first; CHANGELOG **BREAKING**; the once-per-process log names the policy |
| **A `SHARED` app loses `current_tenant()`** (FLIP-9) | The replacement (`TenancyModule()` + `VARCO_TENANT_SOURCES=jwt`) works with no catalog/pool | §D-flip9-shared; the regression test through both construction paths |
| **Every non-tenant 4.0 app refuses in production** (⚠️ premise (4) — a latent 049/051/054 composition bug) | `tenant.*` findings exist iff `TenancyModule` is present | §D-collector; `test_no_tenant_findings_without_tenancy_module` |
| **Every tenant app must now adopt membership or suppress** (`NullTenantMembership` = no provider, WARN) | brief 006 §2 rule 1 is the default posture; suppression is the operator's statement | §Migration row 8; the module docstring; a Pitfalls row |
| ⚠️ ASSUMPTION — 049's `create_varco_app` collects `middleware()` after every module's `register()` (so `depends_on=(FastAPIModule,)` guarantees `AbstractServerAuth` is resolvable in `TenancyModule.middleware()`) | The module sees the auth binding | Step 1 reads 049's builder; if collection happens per-module in order, `depends_on` still orders it |
| ⚠️ ASSUMPTION — 051's `contribute()` may be called from `configure_app()` (051 §D-posture does exactly this for `SecurityPostureModule`) | The collector reaches `acollect(container, PostureCollector)` in `SecurityPostureModule.start()` | Step 1 reads 051's implementation |
| ⚠️ ASSUMPTION — 048 made `TenantProvenanceSettings()` env-reading (so `build_tenant_source_chain()` with no argument still reads `VARCO_TENANT_*`) | The hand-wired default resolves from env | Step 1; if the class is still a dataclass, `build_tenant_source_chain()` already calls `from_env()` (`settings.py:401`) |
| ⚠️ ASSUMPTION — 054's autouse `_posture_warn_only` fixture exists in `varco_fastapi/tests/conftest.py` | The FLIP posture tests `delenv` it; every other test boots | Step 1; if absent, the FLIP tests set `VARCO_SECURITY_ENFORCE=refuse` explicitly instead |
| Two resolution inputs (env vs container) for one builder | One builder (`build_tenant_source_chain`), one deprecation site, one message; the hand-wired path fails closed on a pool-only name | `test_env_sources_naming_pool_only_source_fails_closed_when_hand_wired` |
| The moved slot changes observability of rejections | Rejections are enveloped/logged — an improvement; the 3.x position is still reachable | §Migration row 5 |
| `api_surface.py --check` red after Step 11 | Snapshot regenerated with exactly the named changes | Step 12; Step 15's `make lint` |
| Docs drift — a surviving "nothing flips", "fail-open in 3.2", "removed in 4.0", or `enable_tenant_context` mention | No surviving 3.2 claim | Step 13's `rg` check |
| FLIP-14's design is mistaken for a commitment to build | The section, the CHANGELOG bullet and the BACKLOG cell all say *"implementation is its own future plan"* | Step 13/14 wording |

## Done

- [x] `ClaimTenantMembership`/`TenantMembershipSettings` default `DENY` at all three sites;
      `VARCO_TENANT_MEMBERSHIP_ON_MISSING=allow` is the hatch; Step 2 tests green.
- [x] `TenantResolutionMiddleware(chain=None)` → env-derived chain, else deprecated implicit legacy
      via `warn_deprecated(since="3.2.0", removed_in="5.0.0")`; no `warnings.warn` left in the file;
      `catalog`/`pool` optional; `sources.py:255` corrected.
- [x] `EdgeSlot.TENANT_RESOLUTION = 95`; `TenancyModule` contributes the middleware and the
      `TenantPostureCollector`, `validate()` refuses `pool=None` under non-`SHARED` isolation;
      `SecurityPostureModule` contributes no tenant collector; the order invariant tests green.
- [x] `RequestContextMiddleware(server_auth=)` only; `_maybe_tenant_context` gone;
      `test_only_one_http_writer_of_tenant_context` and the regression test green;
      `inspect_tenant_provenance(request_context_sets_tenant=False)` default.
- [x] All `tenant.*` tokens mapped; `tenant.no_catalog_gate` new; the severity table in
      `security-posture.md` has one row per id; `test_every_tenant_token_has_a_severity_row` green.
- [x] Production-refusal tests for the implicit fallback and the `allow` opt-out green with both
      posture variables `delenv`ed.
- [x] §D-flip14 written into `admin-surface-tenancy.md`; BACKLOG `FLIP-14` → `designed (Plan 055
      §D-flip14)`; `FLIP-6`/`FLIP-7`/`FLIP-9` → `done (Plan 055)`.
- [x] Docs per Step 13; CHANGELOG per Step 14; api-surface snapshot regenerated; `make test`,
      `make lint`, `make type-check` green; AsyncAPI snapshot untouched.

## Out of scope / Parked

- **`TenantResolutionMiddleware(chain=None)` → `TypeError`** — 5.0.0, not before 2027-09-12
  (058 lists it).
- **`CrossCheckMode.STRICT` default** — recorded non-flip (036 row 8); annotated, not executed.
- **FLIP-14 implementation** — `provisioning_profile=`/`operation_scopes=` on `mount_tenant_admin`,
  `posture.tenant_admin_role_only`, an internal provisioning issuer recipe. Its own plan.
- **A repository-backed `AbstractTenantMembership`** — parked (`BACKLOG.md` parks).
- **Guarding against a double `TenantResolutionMiddleware`** (module + `MiddlewareModule`) — 052's
  report shows both; a hard refusal would need cross-module knowledge 049 declined.
- **A live catalog-status posture check** (is the configured catalog reachable?) — 050/056's
  `AsyncConnection`-holding collectors are the home for anything that does I/O.
- **Moving the `HTTPException` catch out of `TenantResolutionMiddleware`** now that the module slot
  is inside `ErrorMiddleware` — kept for the hand-wired `EXTRA` path; revisit when that path goes.
