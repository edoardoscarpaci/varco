# Security posture preflight — `SecurityPosture` (S9)

Plan 036 (3.2 security release, BACKLOG row **S9**). Research brief backing this feature:
`design/research/006-multi-tenant-identity-and-hardening.md`.

**Default-on since Plan 049 (FLIP-1).** `SecurityPostureModule` (a `VarcoModule` wrapping
`SecurityPostureLifecycle` unchanged) is part of `varco_fastapi.default_modules()` — every app
gets the preflight and its report with zero code. `VARCO_SECURITY_POSTURE_ENABLED=false` or
`default_modules(exclude=(SecurityPostureModule,))` turns it off; the pre-049 hand-wired form
(`LifecycleModule(SecurityPostureLifecycle(...))`) still works for an app that wants to fully
replace the module. Row 1 of the flip list below is now `done (Plan 049)`.

**Refuses a production start by default since Plan 054 (FLIP-2).** `enforce` is unset by default
and derives `refuse` unless `VARCO_SECURITY_ENV` is exactly `development` — one `HIGH` finding, or
one unsuppressed `WARN` in `production`, aborts startup. [The severity table](#the-severity-table--every-check-id-and-its-40-consequence)
lists every shipped `check` id and whether it blocks. **What a stock production app hits
today** (`default_modules()`, both `VARCO_SECURITY_*` variables unset, no tenancy configured):
`posture.authorizer_unbound` (HIGH — bind an `AbstractAuthorizer`). **No `tenant.*` finding**:
since Plan 055 (§D-collector) the `TenantPostureCollector` is contributed by `TenancyModule`, not
by `SecurityPostureModule`, so an app with no tenancy emits none. An app that lists
`TenancyModule()` with no `VARCO_TENANT_SOURCES` hits `tenant.no_chain` +
`tenant.legacy_source_implicit` (+ `tenant.no_membership_provider` without
`enable_tenant_membership`) — all WARN, all production start-blockers; adopt a chain per
`tenant-provenance.md`.
`di.in_process_backend` is **opt-in**, by design: an app may legitimately run in-process
backends, so its collector (`varco_fastapi.posture.DIPostureCollector`) is never a default. An
app that wants in-process seams to block a production start contributes it once —
`contribute(container, PostureCollector, DIPostureCollector(container))` — and from then on
every seam left on its in-process default is a WARN (a production start-blocker).

## What it answers

A varco app cannot today say out loud, at startup, which of its security defaults are still the
permissive ones. `SecurityPostureLifecycle.start()` runs every collector, logs the result at
`WARNING`, stores it as `.report`, and (in `enforce="refuse"` mode — the derived default outside
`development` since 4.0) can raise.

```python
from varco_fastapi.app import create_varco_app

app = create_varco_app(container)  # SecurityPostureModule runs its default collectors

# After the app has started (e.g. in a test, or a health endpoint):
app.state  # or capture the module instance yourself, see below
```

Passing a hand-built collector list (the pre-049 `SecurityPostureLifecycle(collectors=[...])`
shape) now goes through the module's constructor:

```python
from varco_fastapi.modules import SecurityPostureModule, default_modules
from varco_fastapi.posture import SecurityPostureSettings

module = SecurityPostureModule(
    collectors=[...],  # see "Wiring a collector" below
    settings=SecurityPostureSettings(),  # reads VARCO_SECURITY_*
)
app = create_varco_app(
    container, modules=[*default_modules(exclude=(SecurityPostureModule,)), module]
)

# After the app has started (e.g. in a test, or a health endpoint):
module.report.summary()  # "2 info, 1 warn, 0 high, 1 not assessed (0 suppressed) · enforce=refuse"
```

## The four-severity model

`PostureSeverity`: `INFO` / `WARN` / `HIGH` / **`NOT_ASSESSED`**.

`NOT_ASSESSED` is a severity, not an absence. A collector whose sibling module cannot be
imported, or that raises for any other reason, is wrapped so it produces exactly one
`NOT_ASSESSED` finding rather than vanishing from the report or crashing startup
(§D-S9-degrade). A silently skipped check that renders as a clean report is a security-relevant
lie — this is the single property the harness exists to prevent. The summary line always
separates the four counts (`"N info, N warn, N high, N not assessed (N suppressed) · enforce=<mode>"`
— the trailing mode since Plan 054, `SecurityPosture.enforce`), never collapsing `NOT_ASSESSED`
into a pass.

`ImportError`/`ModuleNotFoundError` → a finding naming the missing module (e.g. a Mongo-only app
that never installed `varco_sa`, which is a *correct* configuration, not an error). Any other
exception → a finding carrying the exception's **type name only**, never `str(exc)` — the same
rule as 035's error-response fix and CLAUDE.md's `error_params()` exfiltration guidance; the full
exception is still logged server-side at `debug`.

## The collector table (§D-S9-checks)

Five collectors are thin adapters over a sibling plan's exported inspector; one is local to this
plan. Severity assignment lives **here**, in the collector, not in the sibling — 035 is the one
exception, whose nine `http.*` ids are re-emitted with 035's own severities **unchanged**,
because the two plans already commit to those together.

| Collector | Source seam | Contributes |
|---|---|---|
| `TenantPostureCollector` (contributed by `TenancyModule` since Plan 055 — never by `SecurityPostureModule`) | 033 `inspect_tenant_provenance()` | Re-emits 033's `tenant.*` stable tokens (`tenant.no_chain`, `tenant.legacy_source_implicit`, `tenant.no_membership_provider`, `tenant.membership_missing_claim_allows`, `tenant.cross_check_lenient`, `tenant.unchained_claim_tenant_setter`, `tenant.delegation_unbound`, `tenant.third_party_source_high_trust` (Plan 051), `tenant.no_catalog_gate` (Plan 055), …) with severity assigned here; a bound `NullTenantMembership` counts as no provider |
| `AuthPostureCollector` | 034 `inspect_auth_posture()` + `inspect_revocation_posture()` | `auth.api_key_query_fallback`, `auth.api_key_plaintext`, `auth.passthrough_bound`, `auth.revocation_unbound`, `auth.revocation_registry_unwired`, `auth.revocation_fail_open`, `auth.revocation_jti_not_required` (Plan 053 — a real store is wired but `revocation_require_jti` was explicitly set `false`) |
| `HttpEdgeCollector` | 035 `inspect_http_edge()` | Re-emits 035's nine `http.*` ids and severities unchanged |
| `RlsPostureCollector` (`varco_sa.tenancy.rls_posture`, contributed by `SAModule` since Plan 056 — never by `SecurityPostureModule`; `data.*` exists only when `SAModule` is present) | 037 `inspect_rls_posture()`, read live by `SAModule`'s `sa.rls_posture` component (`Phase.INFRA + 50`, after migrations) | `data.rls_disabled`, `data.rls_not_forced`, `data.role_bypasses_rls`, `data.rls_enabled_without_policy`, `data.rls_guc_setter_absent`, `data.rls_not_enforced`, `data.rls_not_applicable`, `data.rls_not_checked`, `data.rls_table_absent` — severity assigned in `varco_sa`'s `rls_findings()` (the second exception to "severity lives here") |
| `DataPostureCollector` (hand form only — no longer a default) | a caller-supplied `RlsPosture` | delegates to `rls_findings()` |
| `JwksPostureCollector` (Plan 052 / RO-2, `CoreModule`) | `inspect_jwks_posture()` | `auth.jwks_refresh_off`, `auth.jwks_keysets_unloaded` |
| `RetentionPostureCollector` (Plan 052 / RO-2, `CoreModule`) | `inspect_retention_posture()` | `retention.platform_wide`, `retention.dry_run_only`, `retention.short_retention` |
| `LocalPostureCollector` | this plan | `posture.admin_mount_unauthenticated` (unreachable through varco's mounts since 4.0 — they refuse `server_auth=None`, Plan 054 / FLIP-3/4; the id remains for hand-wired surfaces), `posture.tenant_admin_mounted`, `posture.webhook_secrets_plaintext`, `posture.base_authorizer_bound`, `posture.authorizer_unbound` (Plan 053), `posture.webhook_transport_unverified` |
| (the lifecycle itself) | `SecurityPostureLifecycle.start()` | `posture.duplicate_collector`, `posture.unknown_suppression`, `posture.unrecognised_environment`, `posture.unrecognised_enforce` (Plan 054) |
| `RedactionPostureCollector` (default, contributed by `SecurityPostureModule`; audited services via `SecurityPostureModule(audited_services=(...))`) | 040 `inspect_redaction_posture()` | `redaction.audit.disabled` (warn — opted out), `redaction.error_params.disabled` (warn), `redaction.default.custom` (info), `redaction.patterns.substring_mode` (info — opted out); severities re-emitted unchanged, remediation assigned here; a default 4.0 app emits none |
| `DIPostureCollector` (**opt-in** — never a default; `contribute(container, PostureCollector, DIPostureCollector(container))`) | `inspect_backend_selection()` | `di.in_process_backend` — one per seam served by its in-process default |

Severities live in one place: [the severity table](#the-severity-table--every-check-id-and-its-40-consequence).

⚠️ **Plan 051 / DI-5**: these five (`_collect_tenant`/`_collect_auth`/`_collect_http`/
`_collect_data`/`_collect_local`) are now classes implementing `varco_core.posture.
PostureCollector` (`TenantPostureCollector`/`AuthPostureCollector`/`HttpEdgeCollector`/
`DataPostureCollector`/`LocalPostureCollector`, in `varco_fastapi.posture`) — `PostureCollector`
is a `@Multibound` collection seam; `SecurityPostureModule.configure_app()` contributes four
via `contribute()` (`HttpEdgeCollector`/`LocalPostureCollector`/`AuthPostureCollector`/
`RedactionPostureCollector` — the last fed by `audited_services=`) —
`TenantPostureCollector` is contributed by `TenancyModule` since Plan 055, built from the
chain/membership its middleware actually uses, and the live `data.*` collector
(`varco_sa`'s `RlsPostureCollector`) by `SAModule` since Plan 056; `DataPostureCollector` remains
only as the hand form. Each class's `__init__` takes exactly the old function's keyword arguments;
`collect()` is the old function's unchanged body.

Each collector takes optional, already-resolved objects as constructor keyword arguments (`app=`,
`container=`, `auth=`, `rls_posture=`, …) rather than resolving them itself from a `container` —
this keeps every collector's `collect()` a pure, synchronous, dependency-free method. `inspect_
rls_posture()` is `async def` and needs a live `AsyncConnection`; since Plan 056 `SAModule`'s
`sa.rls_posture` component (`Phase.INFRA + 50` — after migrations) awaits it once and `RlsPostureCollector.collect()` reads the stored result at
`Phase.EDGE` — so an app listing `SAModule` gets the live check with no wiring. Without
`SAModule`, the hand form (`DataPostureCollector(rls_posture=...)` or
`RlsPostureCollector(posture=...)`) takes a caller-fetched posture; with none it reports
`data.rls_not_checked` at `NOT_ASSESSED`.

### Wiring a collector

A plugin never needs to touch `SecurityPostureModule` at all — a scanned `@Singleton` subclass
of `varco_core.posture.PostureCollector` joins the report automatically:

```python
from providify import Singleton
from varco_core.posture import PostureCollector, PostureFinding, PostureSeverity


@Singleton
class MyPostureCollector(PostureCollector):
    name = "my_check"

    def collect(self) -> list[PostureFinding]:
        return [PostureFinding(check="my.check", severity=PostureSeverity.INFO, ...)]
```

The audited services the redaction collector inspects are explicit input (036's rule — never
discovered by walking `AuditLogMixin.__subclasses__()`):

```python
module = SecurityPostureModule(audited_services=(OrderService,))
```

To make in-process seams block a production start, opt in to `DIPostureCollector`:

```python
from varco_core.di import contribute
from varco_core.posture import PostureCollector
from varco_fastapi.posture import DIPostureCollector

contribute(container, PostureCollector, DIPostureCollector(container))
```

To replace the built-in four entirely, pass `collectors=` to `SecurityPostureModule` (still
accepts `PostureCollector` instances or the pre-051 zero-argument callable shape;
`audited_services=` is then ignored):

```python
from varco_fastapi.modules import SecurityPostureModule
from varco_fastapi.posture import (
    AuthPostureCollector,
    DataPostureCollector,
    HttpEdgeCollector,
    LocalPostureCollector,
    RedactionPostureCollector,
    TenantPostureCollector,
)

module = SecurityPostureModule(
    collectors=[
        TenantPostureCollector(chain=my_chain, membership=my_membership),
        AuthPostureCollector(auth=my_server_auth, registry=my_issuer_registry),
        HttpEdgeCollector(app=app),
        DataPostureCollector(rls_posture=None),  # or an awaited RlsPosture
        LocalPostureCollector(container=container, webhook_repository=webhook_repo),
        RedactionPostureCollector(service_classes=(OrderService,)),
    ],
)
```

`LocalPostureCollector`'s authorizer check is the row's headline check: it resolves
`AbstractAuthorizer` from `container` inside its own `try`, and splits three ways (Plan 053 /
FLIP-5):

| Outcome of `container.get(AbstractAuthorizer)` | Finding |
|---|---|
| `LookupError` — no active binding (outside `VARCO_SECURITY_ENV=development` with no app authorizer) | `posture.authorizer_unbound` — **HIGH**, never demoted, blocks under `enforce="refuse"` in every environment |
| Any other exception — construction failed | `posture.not_assessed.base_authorizer_bound` — `NOT_ASSESSED`, never a crash |
| An instance whose `type(x) is BaseAuthorizer` | `posture.base_authorizer_bound` — WARN (demoted to INFO in `development`); in production this can only be an explicit `bind_instance()`/`@Provider` of `BaseAuthorizer()` — suppress by id if deliberate |

⚠️ An app authorizer whose own `__init__` raises `LookupError` (an unbound dependency) is
reported as `posture.authorizer_unbound` — the remediation (fix the binding) still applies, and
`validate()`/`varco diagnose` name the real missing parameter.

`posture.webhook_transport_unverified` is narrow by design: the shipped transport **does**
connect to `target.pinned_ip` (`varco_core/varco_core/webhook/transport.py:100`), so this finding
fires only when a **non-default transport** is bound to the dispatcher, at `INFO`, wording
*"a custom webhook transport is bound; varco cannot verify it connects to `target.pinned_ip`
rather than re-resolving the hostname."* A static check cannot do better without re-implementing
the transport's own connection logic.

## `VARCO_SECURITY_ENV` — presentation, the one binding it gates (§D-S9-oq1, Plan 053), and the one derivation: `enforce` when unset (Plan 054)

| Env var | Default | Meaning |
|---|---|---|
| `VARCO_SECURITY_ENV` | `production` | `"production"` \| `"development"`. **Presentation** — every check still runs, in both modes; `WARN` findings are demoted to `INFO` (and their entry in the log line, not the summary count, moves) in `development`. **And it gates the permissive `BaseAuthorizer` binding** (4.0, Plan 053 / FLIP-5) — see below |
| `VARCO_SECURITY_ENFORCE` | unset → derived: `refuse` unless `VARCO_SECURITY_ENV=development` (4.0, Plan 054 / FLIP-2) | `"warn"` \| `"refuse"`; an explicit value (env or code) beats the derivation. `"refuse"` raises `RuntimeError` at `start()` if any **non-suppressed** finding is `HIGH` (or, in `production`, `WARN`). Never raised for `NOT_ASSESSED` alone. Any other value is treated as `warn` and reported as `posture.unrecognised_enforce` |
| `VARCO_SECURITY_SUPPRESS` | `""` | Comma-separated `check` ids to suppress (see below) |

`environment` defaults to the strict interpretation deliberately — an explicit flag that defaults
to lenient is one nobody sets. Every candidate signal varco could *infer* the mode from
(`app.debug`, a `localhost` database URL, `PassthroughAuth` bound) is itself one of the things the
preflight is checking, so inferring the mode from the findings would let the worst-configured
deployment decide it is not production and downgrade its own warnings — the "gets it wrong at
the worst moment" failure the backlog's own framing warned against. There is deliberately no
generic `VARCO_ENV` shared with other subsystems; see Parked in the plan.

**What changed in 4.0 (Plan 053 / FLIP-5).** 036's promise — *no finding is ever suppressed by
the environment* — still holds. What changed is that the same variable now also decides whether
`BaseAuthorizer` exists: the class carries `@Requires(env="VARCO_SECURITY_ENV",
value="development")` beside `@Fallback`, so outside development it is not a binding at all —
`container.get(AbstractAuthorizer)` raises `LookupError: No binding found for
'AbstractAuthorizer'. Did you forget container.bind() or container.provide()?`, `validate()`
reports `MISSING_BINDING` at every service that injects it plus one `CONDITION_INACTIVE` naming
the variable, and this collector reports `posture.authorizer_unbound` (HIGH). One variable, two
readers (the pydantic settings field and providify's `@Requires` marker), the same exact,
case-sensitive literal `"development"` — they can never disagree. A code-configured
`SecurityPostureSettings(environment="development")` demotes findings but does **not** open the
binding gate (the marker cannot see a container-bound settings object); export the variable.

## The suppression contract (§D-S9-suppress)

`VARCO_SECURITY_SUPPRESS` is a comma-separated list of `check` ids — the stable identifiers every
sibling plan committed to for exactly this purpose. A suppressed finding is **still produced**,
with `PostureFinding.suppressed=True`, demoted to `INFO` in the log, and the summary line always
ends with `(N suppressed)`, even when that count is zero. Suppression never removes the finding —
a newly appeared problem that happens to share an id with an old, knowingly-accepted one is still
visible; only its noise is quieted.

An unknown id in `VARCO_SECURITY_SUPPRESS` — a typo — silently suppresses nothing. It is itself
reported as a `posture.unknown_suppression` finding at `INFO`, naming the unmatched id.

## `enforce="refuse"` — the default in production (4.0, Plan 054 / FLIP-2; §D-S9-enforce)

`SecurityPostureSettings.enforce` is `None` (unset) by default; `effective_enforce` derives the
mode. What `refuse` *means* is unchanged since 3.2 — only the default moved:

| `VARCO_SECURITY_ENV` | effective `enforce` | `HIGH` | unsuppressed `WARN` | `WARN` presentation | `NOT_ASSESSED`/`INFO` |
|---|---|---|---|---|---|
| unset / `production` | `refuse` (derived) | blocks | **blocks** | `WARN` | never block |
| `development` | `warn` (derived) | logged | logged | demoted to `INFO` | — |
| anything else (`staging`, `prod`, `Development`, `""`) | `refuse` (derived) | blocks | logged (not `production`) | `WARN`, not demoted | — ; plus `posture.unrecognised_environment` (INFO) |
| any, `VARCO_SECURITY_ENFORCE=warn` | `warn` (env) | logged | logged | per environment | — |
| any, `VARCO_SECURITY_ENFORCE=refuse` | `refuse` (env) — the 3.2 opt-in, unchanged | blocks | blocks in `production` only | per environment | — |

The refusal names every blocking id, how the mode was decided, and the three fixes; the 3.2
fragment `refusing to start — unsuppressed blocking findings:` is kept verbatim for alert rules:

```
RuntimeError: SecurityPostureLifecycle(enforce='refuse', derived from VARCO_SECURITY_ENV='production'):
refusing to start — unsuppressed blocking findings: posture.authorizer_unbound, tenant.no_chain.
Fix each finding (see the SecurityPosture[...] lines above), suppress a knowingly-accepted id with
VARCO_SECURITY_SUPPRESS=<id>, or set VARCO_SECURITY_ENFORCE=warn to restore the 3.x warn-only preflight.
```

The three fixes: fix the finding; `VARCO_SECURITY_SUPPRESS=<id>` for a knowingly-accepted one; or
`VARCO_SECURITY_ENFORCE=warn` (code: `SecurityPostureSettings(enforce="warn")`) to restore 3.x
wholesale. `varco diagnose` prints `enforce=refuse (derived from VARCO_SECURITY_ENV=production)` in
its header, so the consequence is visible before deploy. `NOT_ASSESSED` never blocks startup in
any mode: "we could not check" is not evidence of a problem, and making it fatal would make a
Mongo-only app unstartable because `varco_sa` is absent.

## The severity table — every `check` id and its 4.0 consequence

**Rule: a new `check` id must add a row here** —
`varco_fastapi/tests/test_posture_severity_table.py::test_every_shipped_check_id_has_a_severity_table_row`
enforces it. The table is authoritative for *consequence*, not for *emission* — each owning plan's
doc still says when its id fires. Column 4 is mechanical: `HIGH` → blocks in every environment
**except `development`** (which derives `enforce="warn"`, so even `HIGH` starts there — it blocks in
`development` too only under an explicit `VARCO_SECURITY_ENFORCE=refuse`); `WARN` → blocks in
`production` (unless suppressed); `INFO`/`NOT_ASSESSED` → never. "Every environment" below is
shorthand for "every environment under `refuse`" — the derived default outside `development`. Suppression is an
operator statement for **any** tier — a suppressed `HIGH` is demoted to `INFO` and does not block.

| `check` | Severity | Owner | Blocks a `production` start under the 4.0 default? | Escape |
|---|---|---|---|---|
| `posture.authorizer_unbound` | HIGH | 053 | **yes, every environment** (development derives `warn`, so it starts there) | bind an `AbstractAuthorizer`; `VARCO_SECURITY_ENV=development` |
| `posture.base_authorizer_bound` | WARN | 036/053 | yes (only reachable via an explicit instance binding in production) | `VARCO_SECURITY_SUPPRESS=posture.base_authorizer_bound` |
| `posture.not_assessed.base_authorizer_bound` | NOT_ASSESSED | 053 | never | fix the authorizer's construction error |
| `posture.admin_mount_unauthenticated` | WARN | 036/054 | yes — but unreachable through varco's mounts since FLIP-3/4 | pass `server_auth=` |
| `posture.tenant_admin_mounted` | INFO | 036 | no | — |
| `posture.webhook_secrets_plaintext` | WARN | 036 (flip: **056**) | yes — fires on a bound `PlaintextFieldEncryptor` or an encryptor-less out-of-tree repository, never on the in-memory one; the default collector resolves the bound repository from the container (a provider that fails to construct → `posture.not_assessed.webhook_secrets_plaintext`) | bind a real `FieldEncryptor`; suppress for a deliberate dev run |
| `posture.not_assessed.webhook_secrets_plaintext` | NOT_ASSESSED | 056 | never | fix the `WebhookSubscriptionRepository` binding's construction error (a durable backend needs a bound `FieldEncryptor`) |
| `posture.webhook_transport_unverified` | INFO | 036 | no | — |
| `posture.unknown_suppression` | INFO | 036 | no | fix the typo |
| `posture.duplicate_collector` | WARN | 051 | yes — two collectors sharing a `name` is a wiring bug (both still ran) | give each `PostureCollector` a unique `name`; suppress |
| `posture.unrecognised_environment` | INFO | 054 | no | set `production`/`development` |
| `posture.unrecognised_enforce` | INFO | 054 | no — the value is treated as `warn` | set `VARCO_SECURITY_ENFORCE` to exactly `warn`/`refuse`, or unset it |
| `posture.not_assessed.*` | NOT_ASSESSED | 036 | never (§D-S9-enforce) | install the sibling package |
| `di.in_process_backend` | WARN | 047 (consequence: 054) | **yes, when opted in** (`DIPostureCollector` contributed — never a default collector, so a stock app never emits it) — an in-memory event bus / cache / DLQ / idempotency store / job store in production is lost on restart and single-process by construction; INFO in `development` by the standard demotion. Reported for an explicit `VARCO_<SEAM>_BACKEND=memory` too (only a code binding is silent) | `VARCO_<SEAM>_BACKEND=<real backend>`; a deliberate single-process deployment: `VARCO_SECURITY_SUPPRESS=di.in_process_backend` (one id covers every seam; the detail names each) |
| `auth.api_key_query_fallback` | WARN | 034/036 | yes | drop `param=`; suppress |
| `auth.api_key_plaintext` | WARN | 034/036 (053 deprecates the path) | yes | `hashed_keys=` |
| `auth.passthrough_bound` | WARN | 034/036 | yes | suppress if the gateway deployment is deliberate (053) |
| `auth.revocation_registry_unwired` | WARN | 034/036 | yes — the "bound but not wired" bug class | `TrustedIssuerRegistry(revocation_store=…)` |
| `auth.revocation_unbound` | INFO | 034/036 | no | — |
| `auth.revocation_fail_open` | HIGH | 034/036 | yes, every environment | `revocation_failure_mode=fail_closed` |
| `auth.revocation_jti_not_required` | INFO | 053 | no | — |
| `auth.jwks_refresh_off` | WARN | 052 | yes | `JwksRefreshModule`/`VARCO_JWT_JWKS_TTL_SECONDS` |
| `auth.jwks_keysets_unloaded` | INFO | 052 | no | — |
| `tenant.no_chain` | WARN | 033/036/055 | yes — only with `TenancyModule` (implicit header fallback active) | `VARCO_TENANT_SOURCES=jwt[,…]`, or an explicit `LegacyTenantSource` |
| `tenant.legacy_source_implicit` | WARN | 033/036/055 | yes — only with `TenancyModule`; co-fires with `tenant.no_chain` | as above |
| `tenant.legacy_source_explicit` | WARN | 033/055 | yes — the opt-out needs its acknowledgement | `VARCO_SECURITY_SUPPRESS=tenant.legacy_source_explicit` if the ingress strips the header |
| `tenant.single_source` | INFO | 033/055 | no | — |
| `tenant.no_membership_provider` | WARN | 033/036/055 | yes — `NullTenantMembership` counts as none | `enable_tenant_membership(container)`; suppress for a deliberate single-tenant deployment |
| `tenant.membership_missing_claim_allows` | HIGH | 033/036/055 | yes, every environment — reachable only by the explicit `on_missing_claim=allow` opt-out | unset `VARCO_TENANT_MEMBERSHIP_ON_MISSING`; suppress during an IdP rollout |
| `tenant.subdomain_trusts_forwarded_host` | WARN | 033/055 | yes | `trust_forwarded_host=False`, or suppress behind an overwriting proxy |
| `tenant.unchained_claim_tenant_setter` | WARN | 033/036/055 | yes — but unreachable through varco since 4.0 (FLIP-9); the id remains for hand-wired setters | remove the hand-wired setter |
| `tenant.no_catalog_gate` | INFO | 055 | no | pass `catalog=` to `TenancyModule` if you provision/suspend tenants |
| `tenant.cross_check_lenient`, `tenant.delegation_unbound` | INFO | 033/036 (recorded non-flip) | no | — |
| `tenant.third_party_source_high_trust` | INFO | 051 | no | confirm the plugin's trust level |
| `http.security_headers.absent` | WARN | 035 | yes — on by default; absent means an app turned it off | re-enable or suppress |
| `http.body_limit.absent` | WARN | 035 | yes — on by default; absent means an app turned it off | re-enable or suppress |
| `http.security_headers.hsts_absent` | INFO | 035 | no | — |
| `http.rate_limit.absent` | INFO (was WARN — §D-ratelimit) | 035 → 054 | **no** — rate limiting is a recorded non-flip (row 11 below) | — |
| `http.rate_limit.no_pre_auth_rule` | WARN | 035 | yes — an *installed* limiter with no pre-auth rule is a gap in an adopted feature | add a `PRE_AUTH` rule |
| `http.rate_limit.in_memory_limiter` | INFO | 035 | no | — |
| `http.rate_limit.fail_open` | INFO | 035 | no | — |
| `http.error.detail_exposed` | WARN | 035 (flip: 054) | yes — after FLIP-10 it fires only on an explicit `VARCO_ERROR_INCLUDE_DETAIL=true` | unset it; suppress if deliberate |
| `http.error.debug_enabled` | HIGH | 035 | yes, every environment | `debug=False` |
| `data.rls_disabled` | HIGH | 037 / **056** (emitted only on Postgres + `SHARED` + candidate tables) | yes, every environment | adopt RLS per `postgres-rls.md` §"Upgrading a 3.x deployment"; defer: `VARCO_TENANCY_ENFORCE_RLS=false` + suppress `data.rls_not_enforced` |
| `data.role_bypasses_rls` | HIGH | 037 / **056** (narrowed to an actual bypass: superuser, `BYPASSRLS`, or an owned table **without** `FORCE`) | yes, every environment | a non-superuser, non-`BYPASSRLS` role; `FORCE` every owned table; local docker: `VARCO_SECURITY_ENV=development` |
| `data.rls_not_forced` | WARN | 037 / 056 | yes | `ALTER TABLE … FORCE ROW LEVEL SECURITY` in a reviewed revision |
| `data.rls_enabled_without_policy` | WARN | 056 | yes — default-deny is a silent outage | `CREATE POLICY …` (the generator emits it before `ENABLE`); suppress |
| `data.rls_guc_setter_absent` | WARN | 056 | yes — live policies with no GUC setter return zero rows | `VARCO_TENANCY_RLS_SET_TENANT=true`; an app that sets the GUC itself: suppress |
| `data.rls_not_enforced` | WARN | 056 | yes — the `enforce_rls=false` opt-out needs its acknowledgement | unset `VARCO_TENANCY_ENFORCE_RLS`; or `VARCO_SECURITY_SUPPRESS=data.rls_not_enforced` |
| `data.rls_not_applicable` | INFO | 056 | no — non-Postgres dialect or `SCHEMA`/`DATABASE` isolation | — |
| `data.rls_table_absent` | INFO | 056 (drift decision) | no — a candidate table with no `pg_class` row is "not yet created" (fresh database, first deploy), never `data.rls_disabled`; assessed for real at the next start | none on a first deploy; otherwise check the table name/schema and that migrations ran |
| `data.rls_not_checked` | NOT_ASSESSED | 037 / 056 (also "DB unreachable at preflight", "the RLS posture refresh has not run (`SAModule`'s `sa.rls_posture` component / `SAModule.start()`)") | never | list `SAModule`; or pass `rls_posture=` to the hand form |
| `redaction.audit.disabled` | WARN | 040/057 | yes — an explicit opt-out needs an explicit suppression | per `redaction.md` |
| `redaction.error_params.disabled` | WARN | 040/057 | yes | per `redaction.md` |
| `redaction.default.custom` | INFO | 040/057 | no | — |
| `redaction.patterns.substring_mode` | INFO | 040/057 | no | — |
| `retention.platform_wide` | WARN | 052 | yes | scope the policy |
| `retention.dry_run_only` | INFO | 052 | no | — |
| `retention.short_retention` | INFO | 052 | no | — |

**`http.rate_limit.absent` was re-tiered WARN → INFO (Plan 054 / §D-ratelimit)** because under the
4.0 default a WARN id is a production start-blocker — leaving it at WARN would flip row 11 (a
recorded non-flip) by the back door. Rule for a new finding: a WARN must be a hole or a
misconfiguration, never the non-adoption of an opt-in feature (that is INFO).

## The consolidated 4.0 flip list (§D-S9-flip)

Gathered from all five 3.2-cycle plans — the definition of done for this row required it written
down in one place. Each row names the plan that owns the flip and the 3.2 escape hatch.

| # | Default today (3.2) | 4.0 | Owner | 3.2 escape hatch / warn |
|---|---|---|---|---|
| 1 | `SecurityPostureLifecycle` is opt-in | Auto-registered by `create_varco_app` | 036 | `done (Plan 049 / FLIP-1)` — `SecurityPostureModule` is in `default_modules()` |
| 2 | `SecurityPostureSettings.enforce="warn"` | `"refuse"` in `production` | **landed, Plan 054** | `VARCO_SECURITY_ENFORCE=warn` restores 3.x |
| 3 | `mount_reliability_admin(server_auth=None)` warns and mounts | Refuses | **landed, Plan 054** | `ValueError`; no escape hatch |
| 4 | `build_webhook_router(server_auth=None)` mounts silently | Refuses | **landed, Plan 054** | `ValueError`; no escape hatch |
| 5 | `BaseAuthorizer` is the permissive fallback | Refuses to bind outside `development` | **landed, Plan 053** | `posture.authorizer_unbound` (HIGH) / `posture.base_authorizer_bound` |
| 6 | Tenant resolution defaults to header-only | The chain is the default; the header requires `LegacyTenantSource` | **landed, Plan 055** — the implicit fallback is deprecated `removed_in="5.0.0"` (12-month floor from 3.2.0), not a `TypeError` | `tenant.no_chain`, `tenant.legacy_source_implicit` |
| 7 | Membership check is fail-open on a missing claim | Fail-closed | **landed, Plan 055** | `tenant.membership_missing_claim_allows` |
| 8 | Cross-check is `LENIENT` | remains `LENIENT`; **not** a flip — recorded so it is not mistaken for one | 033 | `tenant.cross_check_lenient` |
| 9 | `RequestContextMiddleware` sets the tenant from a claim with no catalog/membership check | Removed in favour of the chain | **landed, Plan 055** | `tenant.unchained_claim_tenant_setter` |
| 10 | `ErrorEnvelopeSettings.include_detail` echoes `detail` | `False` | **landed, Plan 054** | `VARCO_ERROR_INCLUDE_DETAIL=true` (reported as `http.error.detail_exposed`) |
| 11 | Rate limiting is opt-in | remains opt-in; **not** a flip — recorded for the same reason as row 8 (`http.rate_limit.absent` re-tiered to INFO so the FLIP-2 default cannot flip it by the back door — §D-ratelimit) | 035 | `http.rate_limit.absent` |
| 12 | `enforce_rls=False`; policies not `FORCE`d | RLS asserted for `TENANT` tables on Postgres | **landed, Plan 056** — `SAModule`'s `sa.rls_posture` component (`Phase.INFRA + 50`) reads the catalog; `VARCO_TENANCY_ENFORCE_RLS=false` is the hatch (reported as `data.rls_not_enforced`) | `data.rls_disabled`, `data.rls_not_forced`, `data.role_bypasses_rls`, `data.rls_enabled_without_policy`, `data.rls_guc_setter_absent` |
| 13 | `SAWebhookSubscriptionRepository(encryptor=None)` stores secrets in plaintext | Requires an encryptor | **landed, Plan 056** — `TypeError` on `None`; dev hatch `PlaintextFieldEncryptor(acknowledge_plaintext=True)` | `posture.webhook_secrets_plaintext` |
| 14 | `mount_tenant_admin` is guarded by a role alone | A provisioning-authority model (§D-S4-control) | **designed, Plan 055 §D-flip14** — implementation is its own future plan (`admin-surface-tenancy.md`) | `posture.tenant_admin_mounted` |

⚠️ Rows 8 and 11 are **deliberately not flips** and are listed so a 4.0 planner does not
"complete" the list by flipping them. Their siblings argued each on the merits, in their own
feature docs (`tenant-provenance.md`, `http-edge-hardening.md`).

## Pitfalls

| Pitfall | Why it happens | Fix |
|---|---|---|
| Treating `NOT_ASSESSED` as a pass | It means "could not check", not "checked and fine" — a hand-wired `DataPostureCollector()` with no `rls_posture=` (or with `varco_sa` uninstalled) reports `data.rls_not_checked` as `NOT_ASSESSED` forever, correctly (since Plan 056 a default app without `SAModule` has no `data.*` finding at all) | Read the summary's fourth count separately; `worst()` ranks `NOT_ASSESSED` above `INFO` for exactly this reason |
| Setting `VARCO_SECURITY_ENV=development` to quiet startup | For findings it changes **presentation only** — every check still runs and every `WARN` is still produced (just demoted to `INFO` and logged quieter); it never suppresses a finding or stops a check from running. Since 4.0 (Plan 053) it **also binds the permissive `BaseAuthorizer`** — in production that silently turns authorization off for any service with no app authorizer | Use `VARCO_SECURITY_SUPPRESS` for a knowingly-accepted finding, never the environment flag — and never set `development` on a production process |
| `VARCO_SECURITY_ENV=dev` / `Development` / `staging` | Strict for **both** readers — no permissive `BaseAuthorizer` binding and no WARN demotion; `varco diagnose` shows `(VARCO_SECURITY_ENV='dev')` in the `CONDITION_INACTIVE` row | Use the exact string `development` |
| `SecurityPostureModule(settings=SecurityPostureSettings(environment="development"))` with the env var unset | Findings demote, but `BaseAuthorizer` is still not a binding → `posture.authorizer_unbound` (HIGH) and the wiring preflight refuses | Export `VARCO_SECURITY_ENV=development`; the code form is presentation only |
| Expected `di.in_process_backend` to catch a forgotten `VARCO_<SEAM>_BACKEND`, but production started on in-memory seams | `DIPostureCollector` is opt-in — a stock app does not contribute it | `contribute(container, PostureCollector, DIPostureCollector(container))`; `varco diagnose` and `check_backend_selection()`'s WARNING report the seam either way |
| An audited service's `_audit_redactor = None` is not reported | `RedactionPostureCollector` inspects only the services passed as `SecurityPostureModule(audited_services=...)` | List every `AuditLogMixin` service there |
| A typo in `VARCO_SECURITY_SUPPRESS` | An unknown `check` id suppresses nothing — there is no fuzzy match | The typo itself surfaces as `posture.unknown_suppression` at `INFO`; check the report for it |
| Production app refuses to start with `di.in_process_backend` | The app contributed the opt-in `DIPostureCollector`; every in-process seam is then a WARN finding, and under the 4.0 default a production WARN blocks | `VARCO_<SEAM>_BACKEND=` per seam, or suppress for a deliberate single-process deployment |
| A stock app with `TenancyModule()` and no `VARCO_TENANT_SOURCES` refuses in production | `tenant.no_chain` + `tenant.legacy_source_implicit` (WARN) — the implicit header fallback is active; plus `tenant.no_membership_provider` without `enable_tenant_membership` | `VARCO_TENANT_SOURCES=jwt` (`tenant-provenance.md`), or explicit legacy + `VARCO_SECURITY_SUPPRESS=tenant.legacy_source_explicit` |
| A hand-wired `TenantResolutionMiddleware` (via `MiddlewareModule`) reports nothing | Only `TenancyModule` contributes the `TenantPostureCollector` (Plan 055) | Use `TenancyModule` |
| `VARCO_SECURITY_ENV=staging` (or any other non-`production`/`development` value) | Derives strict enforcement (`refuse` on HIGH), but WARN neither blocks (not `production`) nor demotes; `posture.unrecognised_environment` says so | Use exactly `production` or `development` |
| `VARCO_SECURITY_ENFORCE=warn` in a 3.2 `.env` you forgot about | The 4.0 refusal never fires | `varco diagnose`'s header shows `enforce=warn (env)` — remove the variable |
| `SecurityPostureSettings(environment='development')` in code | Derives `warn` for this process only; the env var is what your deployment reads (and what gates `BaseAuthorizer`) | Export `VARCO_SECURITY_ENV`; never rely on the code form in production |
| `VARCO_ERROR_INCLUDE_DETAIL=true` left on in production | `http.error.detail_exposed` (WARN) — under the derived `refuse` the app refuses to start | Unset it, or `VARCO_SECURITY_SUPPRESS=http.error.detail_exposed` if the echo is deliberate |
| Expecting no report at all (pre-3.3 assumption) | ⚠️ Historical — as of Plan 049 (FLIP-1) `SecurityPostureModule` is in `default_modules()`, so every app gets the preflight unless it opts out | `VARCO_SECURITY_POSTURE_ENABLED=false` or `default_modules(exclude=(SecurityPostureModule,))` |
