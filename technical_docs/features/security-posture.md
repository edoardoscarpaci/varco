# Security posture preflight — `SecurityPosture` (S9)

Plan 036 (3.2 security release, BACKLOG row **S9**). Research brief backing this feature:
`design/research/006-multi-tenant-identity-and-hardening.md`.

**Nothing runs unless an app opts in.** `SecurityPostureLifecycle` is a lifecycle component
passed to `create_varco_app(..., extra_lifespan_components=[...])` — like `TenancyLifecycle` and
`ReliabilityLifecycle` before it (`app.py:319-321,438` already collects them). An app that never
passes one gets no report and no behaviour change. Auto-registration is a named 4.0 flip
(row 1 below), not a 3.2 default.

## What it answers

A varco app cannot today say out loud, at startup, which of its security defaults are still the
permissive ones. `SecurityPostureLifecycle.start()` runs every collector, logs the result at
`WARNING`, stores it as `.report`, and (only in opt-in `enforce="refuse"` mode) can raise.

```python
from varco_fastapi import SecurityPostureLifecycle, SecurityPostureSettings, create_varco_app

lifecycle = SecurityPostureLifecycle(
    collectors=[...],  # see "Wiring a collector" below
    settings=SecurityPostureSettings(),  # reads VARCO_SECURITY_*
)
app = create_varco_app(container, extra_lifespan_components=[lifecycle])

# After the app has started (e.g. in a test, or a health endpoint):
lifecycle.report.summary()  # "2 info, 1 warn, 0 high, 1 not assessed (0 suppressed)"
```

## The four-severity model

`PostureSeverity`: `INFO` / `WARN` / `HIGH` / **`NOT_ASSESSED`**.

`NOT_ASSESSED` is a severity, not an absence. A collector whose sibling module cannot be
imported, or that raises for any other reason, is wrapped so it produces exactly one
`NOT_ASSESSED` finding rather than vanishing from the report or crashing startup
(§D-S9-degrade). A silently skipped check that renders as a clean report is a security-relevant
lie — this is the single property the harness exists to prevent. The summary line always
separates the four counts (`"N info, N warn, N high, N not assessed (N suppressed)"`), never
collapsing `NOT_ASSESSED` into a pass.

`ImportError`/`ModuleNotFoundError` → a finding naming the missing module (e.g. a Mongo-only app
that never installed `varco_sa`, which is a *correct* configuration, not an error). Any other
exception → a finding carrying the exception's **type name only**, never `str(exc)` — the same
rule as 035's error-response fix and CLAUDE.md's `error_params()` exfiltration guidance; the full
exception is still logged server-side at `debug`.

## The collector table (§D-S9-checks)

Four collectors are thin adapters over a sibling plan's exported inspector; one is local to this
plan. Severity assignment lives **here**, in the collector, not in the sibling — 035 is the one
exception, whose nine `http.*` ids are re-emitted with 035's own severities **unchanged**,
because the two plans already commit to those together.

| Collector | Source seam | Contributes |
|---|---|---|
| `_collect_tenant` | 033 `inspect_tenant_provenance()` | Re-emits 033's `tenant.*` stable tokens (`tenant.no_chain`, `tenant.legacy_source_implicit`, `tenant.no_membership_provider`, `tenant.membership_missing_claim_allows`, `tenant.cross_check_lenient`, `tenant.unchained_claim_tenant_setter`, `tenant.delegation_unbound`, …) with severity assigned here |
| `_collect_auth` | 034 `inspect_auth_posture()` + `inspect_revocation_posture()` | `auth.api_key_query_fallback`, `auth.api_key_plaintext`, `auth.passthrough_bound`, `auth.revocation_unbound`, `auth.revocation_registry_unwired`, `auth.revocation_fail_open` |
| `_collect_http` | 035 `inspect_http_edge()` | Re-emits 035's nine `http.*` ids and severities unchanged |
| `_collect_data` | 037 `inspect_rls_posture()` | `data.rls_disabled`, `data.rls_not_forced`, `data.role_bypasses_rls`, `data.rls_not_checked` |
| `_collect_local` | this plan | `posture.admin_mount_unauthenticated`, `posture.tenant_admin_mounted`, `posture.webhook_secrets_plaintext`, `posture.base_authorizer_bound`, `posture.webhook_transport_unverified` |

Each collector takes optional, already-resolved objects as keyword arguments (`app=`,
`container=`, `auth=`, `rls_posture=`, …) rather than resolving them itself from a `container` —
this keeps every collector a pure, synchronous, dependency-free function, callable with zero
arguments when nothing is wired. `inspect_rls_posture()` is `async def` and needs a live
`AsyncConnection`; `_collect_data` cannot call it inline, so a caller that wants this check live
must `await inspect_rls_posture(conn, tables=...)` itself and pass the result as `rls_posture=`
— otherwise `_collect_data` reports `data.rls_not_checked` at `NOT_ASSESSED`.

### Wiring a collector

```python
from functools import partial

from varco_fastapi.posture import (
    SecurityPostureLifecycle,
    _collect_auth,
    _collect_data,
    _collect_http,
    _collect_local,
    _collect_tenant,
)

lifecycle = SecurityPostureLifecycle(
    collectors=[
        partial(_collect_tenant, chain=my_chain, membership=my_membership),
        partial(_collect_auth, auth=my_server_auth, registry=my_issuer_registry),
        partial(_collect_http, app=app),
        partial(_collect_data, rls_posture=None),  # or an awaited RlsPosture
        partial(_collect_local, container=container, webhook_repository=webhook_repo),
    ],
)
```

`_collect_local`'s `posture.base_authorizer_bound` is the row's headline check: it resolves
`AbstractAuthorizer` from `container` and compares `type(x) is BaseAuthorizer`. The resolution
itself can construct singletons, so it runs inside the collector's own `try` — a resolution
failure is `NOT_ASSESSED`, never a crash of startup.

`posture.webhook_transport_unverified` is narrow by design: the shipped transport **does**
connect to `target.pinned_ip` (`varco_core/varco_core/webhook/transport.py:100`), so this finding
fires only when a **non-default transport** is bound to the dispatcher, at `INFO`, wording
*"a custom webhook transport is bound; varco cannot verify it connects to `target.pinned_ip`
rather than re-resolving the hostname."* A static check cannot do better without re-implementing
the transport's own connection logic.

## `VARCO_SECURITY_ENV` — presentation only (§D-S9-oq1)

| Env var | Default | Meaning |
|---|---|---|
| `VARCO_SECURITY_ENV` | `production` | `"production"` \| `"development"`. **Presentation only** — every check still runs, in both modes; `WARN` findings are demoted to `INFO` (and their entry in the log line, not the summary count, moves) in `development` |
| `VARCO_SECURITY_ENFORCE` | `warn` | `"warn"` \| `"refuse"`. `"refuse"` raises `RuntimeError` at `start()` if any **non-suppressed** finding is `HIGH` (or, in `production`, `WARN`). Never raised for `NOT_ASSESSED` alone |
| `VARCO_SECURITY_SUPPRESS` | `""` | Comma-separated `check` ids to suppress (see below) |

`environment` defaults to the strict interpretation deliberately — an explicit flag that defaults
to lenient is one nobody sets. Every candidate signal varco could *infer* the mode from
(`app.debug`, a `localhost` database URL, `PassthroughAuth` bound) is itself one of the things the
preflight is checking, so inferring the mode from the findings would let the worst-configured
deployment decide it is not production and downgrade its own warnings — the "gets it wrong at
the worst moment" failure the backlog's own framing warned against. There is deliberately no
generic `VARCO_ENV` shared with other subsystems; see Parked in the plan.

## The suppression contract (§D-S9-suppress)

`VARCO_SECURITY_SUPPRESS` is a comma-separated list of `check` ids — the stable identifiers every
sibling plan committed to for exactly this purpose. A suppressed finding is **still produced**,
with `PostureFinding.suppressed=True`, demoted to `INFO` in the log, and the summary line always
ends with `(N suppressed)`, even when that count is zero. Suppression never removes the finding —
a newly appeared problem that happens to share an id with an old, knowingly-accepted one is still
visible; only its noise is quieted.

An unknown id in `VARCO_SECURITY_SUPPRESS` — a typo — silently suppresses nothing. It is itself
reported as a `posture.unknown_suppression` finding at `INFO`, naming the unmatched id.

## `enforce="refuse"` (§D-S9-enforce)

Off by default (`"warn"`), per the locked 3.2 blast-radius rule (`BACKLOG.md:54`): defaults that
need real application work get a loud warn-only preflight in 3.2 and flip in 4.0. An operator who
wants fail-closed today sets `VARCO_SECURITY_ENFORCE=refuse` — one variable, the same posture
varco already prefers elsewhere (`tenancy_cache_key()` raises rather than silently
un-namespacing). `NOT_ASSESSED` never blocks startup in either mode: "we could not check" is not
evidence of a problem, and making it fatal would make a Mongo-only app unstartable because
`varco_sa` is absent.

## The consolidated 4.0 flip list (§D-S9-flip)

Gathered from all five 3.2-cycle plans — the definition of done for this row required it written
down in one place. Each row names the plan that owns the flip and the 3.2 escape hatch.

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
"complete" the list by flipping them. Their siblings argued each on the merits, in their own
feature docs (`tenant-provenance.md`, `http-edge-hardening.md`).

## Pitfalls

| Pitfall | Why it happens | Fix |
|---|---|---|
| Treating `NOT_ASSESSED` as a pass | It means "could not check", not "checked and fine" — a Mongo-only app with `varco_sa` uninstalled reports `data.*` as `NOT_ASSESSED` forever, correctly | Read the summary's fourth count separately; `worst()` ranks `NOT_ASSESSED` above `INFO` for exactly this reason |
| Setting `VARCO_SECURITY_ENV=development` to quiet startup | It changes **presentation only** — every check still runs and every `WARN` is still produced (just demoted to `INFO` and logged quieter); it never suppresses a finding or stops a check from running | Use `VARCO_SECURITY_SUPPRESS` for a knowingly-accepted finding, not the environment flag |
| A typo in `VARCO_SECURITY_SUPPRESS` | An unknown `check` id suppresses nothing — there is no fuzzy match | The typo itself surfaces as `posture.unknown_suppression` at `INFO`; check the report for it |
| Expecting a report with no wiring | The preflight is **opt-in** in 3.2 — an app that never constructs and passes a `SecurityPostureLifecycle` gets no findings and no log lines at all | Pass one via `extra_lifespan_components=[...]`; see flip-list row 1 for the 4.0 plan |
