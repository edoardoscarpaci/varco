# BACKLOG

**No cycle is live.** 4.0 (dependency-injection standardization + the security-default flip
list) is built — plans/045–058, index `plans/000-index-4-0-di-standardization.md` — and awaits its
`v4.0.0` release (Plan 058 Phase F: bump, rc rehearsal, tag).

Everything from earlier cycles (3.0.0, 3.0.1, both 3.1 cycles, 3.2, and the 4.0 build) is
**complete** — compressed under `## ✅ Shipped`; the detail lives in `plans/`, `CHANGELOG.md`, and git
history. What is *never* trimmed are the **parks** (with their un-park triggers) and the
**answered decisions**: they are the reason this project does not re-argue settled questions
every cycle.

> ⚠️ An earlier incarnation of this ledger was lost wholesale in `cae7f33`. When trimming, delete
> *completed work rows*; never delete a park, a trigger, or an answered decision.

---

# 4.0 — dependency-injection standardization (discover 2026-09-12; built, plans/045–058)

Every row of this cycle is built; the design of record is `plans/000-index-4-0-di-standardization.md`
and plans/045–058, the user-facing record `CHANGELOG.md` `[4.0.0]` and
`technical_docs/migrations/3-to-4.md`. The decisions below are kept because they are answered
decisions, not work.

## Locked decisions (4.0 cycle)

| Decision | Choice | Consequence |
|---|---|---|
| **Breaking changes** | **Allowed outright** — wiring APIs may change shape without a parallel deprecated path | No two-shape cycle; a written migration guide is part of every breaking row's done criteria |
| **Version number** | **4.0.0, not 3.3.0** | CONTRIBUTING.md's SemVer policy stands unamended. ⚠️ The **12-month removal floor still applies**: symbols deprecated `since="3.0.0", removed_in="4.0.0"` (nine sites, `v3.0.0` tagged 2026-08-31) may not be removed before 2027-08-31 — the major-version gate is necessary, not sufficient. 4.0.0 breaks *wiring*; it does not sweep aliases |
| **Missing providify features** | **Filed upstream, never worked around** — `design/upstream-gaps/providify-*.md` + `UPSTREAM-GAPS.md` row + `strict=True` xfail, per CLAUDE.md's rule | `DI-15` is a gating row: the providify asks land first so the varco rows build on the real feature |
| **Scope** | **All 16 DI rows live; nothing cut for size** | "A huge jump" — more rows may be added mid-cycle. `DI-13` (wiring tests) and `DI-14` (the guide) are standalone rows *and* done criteria of every other row |
| **The recorded 4.0 flip list** | **Absorbed** as `FLIP-*` rows | A user migrates once. Rows 8 and 11 of Plan 036 §D-S9-flip are recorded non-flips and are **not** carried |

## Live

Only carried-over rows remain; every 4.0 `DI-*`/`FLIP-*` row shipped (see `## ✅ Shipped`) or is
parked with a trigger (`FLIP-14`, see `## Parked`).

### Carried over from 3.2 (open, not this cycle's theme)

Roll-forward rows that lost their un-IDed status here; none is DI-shaped and none blocks a `DI-*` row.

| ID | Feature | Severity | Complexity | Status | Rationale | Evidence |
|----|---------|----------|------------|--------|-----------|----------|
| `RO-1` | `MetricsMiddleware` records `status_code="500"` for exceptions `ErrorMiddleware` renders as 4xx | 🟡 should | S | todo | Every error-rate dashboard over `http.server.request.duration` is wrong for mapped exceptions; out of Plan 041's scope | `varco_fastapi/varco_fastapi/middleware/metrics.py:368,375-388`; `app.py:636-642` |
| `RO-3` | Confirm Stripe's inbound HMAC secret encoding against a real delivery | 🟡 should | S | todo | `StripeWebhookVerifier` pins `RAW_UTF8` against brief 013 §2's conflicting description; if wrong, one enum value on one class | `plans/038` Risks table |
| `RO-6` | `varco_core.schedule` general driver (a materialization loop for non-retention schedules) | 🟡 should | S | todo | Plan 032 shipped `ScheduleMaterializer` with no general caller; 039 closed it for retention only | `plans/039` §D-S20-driver |
| `RO-7` | `0007_schedules_table.py` vs. `materializer.py:29`'s claimed `UNIQUE(schedule_id, run_at)` index — fix the DESIGN block (the index cannot exist; `uuid5` `job_id` is the real backstop) | 🟡 should | S | todo | Verified absent by Plan 039 / Step 12 | `varco_sa/varco_sa/schedule.py:11-19` |
| `RO-8` | DLQ and `WebhookDelivery` payload redaction (fifth/sixth redaction surfaces) | 🟡 should | M | todo | Out of Plan 040's scope | `plans/040` §Parked |
| `RO-9` | A generic `JobDispatcher` poll loop over `claim_next()` | 🟡 should | M | todo | Plan 039 rode `JobRunner.recover()` instead; a second execution path belongs to Plan 005's subsystem | `plans/039` §D-S20-dispatch |
| `RO-10` | `CONF-AUDIT6` — assertion-audit the six unaudited conformance suites for Kind-B gaps | 🟡 should | M | todo | Plan 042 audited `cache` and `dlq` only | `testkit/varco_conformance/` |
| `RO-11` | Verify `opentelemetry-exporter-prometheus` emits exemplars end-to-end | 🟢 nice | S | todo | Plan 041 proves the SDK layer only; `router/metrics.py:45-51`'s promise is half-verified | brief 012 Evidence Gaps 2/3 |
| `RO-12` | `CONF-COUNT` — strengthen `DeadLetterQueueConformance.test_count_reflects_pushed_entries` (trivially true at `KafkaDLQ.count()`'s `-1`) | 🟢 nice | S | todo | May surface a Kind-A `KafkaDLQ` finding needing its own xfail + register row | `varco_kafka/tests/test_kafka_conformance.py:80-88` |
| `RO-13` | `CONF-RG` — automate the register↔marker cross-check in `make lint`'s no-`PKG` path | 🟢 nice | S | todo | Plan 042 Q2 left it manual | `COVERAGE.md` findings register |
| `RO-14` | `CoreModule.start()`'s 051 auto-`register_to()` loop passes no `dlq=` to any auto-registered `EventConsumer` — the app's own DLQ never reaches a consumer unless it hand-calls `register_to(bus, dlq=...)` | 🟡 should | S | todo | Deliberately out of Plan 050's scope (§Non-goals) — a behaviour change for every consumer; `WebhookDispatcher` got its own injected `dlq` instead, precisely to avoid needing this | `plans/050` §Non-goals |

## ✅ Shipped

Most recent individually; older cycles rolled up by theme. Detail: `plans/`, `CHANGELOG.md`.

**4.0 — DI standardization + the flip list: 27 rows shipped (plans/045–058)**
- `DI-14` — one DI guide, the 3.x → 4.0 migration guide, generated entry-point table, the 5.0.0 deprecation-floor re-label (plan: plans/058-di-guide-migration-and-4-0-release.md)
- `FLIP-040-1`, `FLIP-040-2`, `RO-5` — word-boundary redaction, audit redaction on by default, redaction posture collector (plan: plans/057-redaction-flips.md)
- `FLIP-12`, `FLIP-13` — `enforce_rls=True` via `SAModule` posture, required webhook-secret encryptor (plan: plans/056-data-layer-flips.md)
- `FLIP-6`, `FLIP-7`, `FLIP-9` — chain-by-default tenant resolution, fail-closed membership, single tenant writer (plan: plans/055-tenant-provenance-flips.md); `FLIP-14` designed there, parked below
- `FLIP-2`, `FLIP-3`, `FLIP-4`, `FLIP-10` — `enforce` derives `refuse`, admin mounts require `server_auth`, no `str(exc)` detail (plan: plans/054-http-edge-and-admin-surface-flips.md)
- `FLIP-5`, `FLIP-034-1`…`FLIP-034-4` — `BaseAuthorizer` development-only, `jti` by default, `ApiKeyAuth(keys=)` deprecated, `PassthroughAuth` acknowledgement (plan: plans/053-credential-and-auth-flips.md)
- `DI-7`, `DI-11`, `RO-2` — `varco diagnose`/`WiringReport`, runtime `validate()` gate, in-memory test profile, JWKS/retention posture collectors (plan: plans/052-diagnose-report-and-test-profile.md)
- `DI-5` — `@Multibound` collection seams (plan: plans/051-multi-binding-registries.md)
- `DI-9`, `DI-10` — phased lifecycle, `@Disposes` reachability, DI-wired infrastructure (plan: plans/050-phased-lifecycle-and-di-wired-infrastructure.md)
- `DI-1`, `FLIP-1`, `RO-4` — `VarcoModule` + `create_varco_app(modules=…)`, posture default-on, `LoggingModule(redactor=…)` (plan: plans/049-varco-module-and-composable-app.md)
- `DI-6`, `DI-16` — `bind_settings()`, generated settings reference, named instances (plan: plans/048-settings-standardization.md)
- `DI-4`, `DI-12`, `DI-17` — `VARCO_<SEAM>_BACKEND` selection, `enable_*`/`VARCO_<FEATURE>_ENABLED`, generated DI catalogue (plan: plans/047-config-driven-backend-selection-and-catalogue.md)
- `DI-2`, `DI-8`, `DI-13` — explicit-container wiring contract, `AlreadyBoundError`, `di_wiring` conformance (plan: plans/046-wiring-verb-collapse-and-explicit-container.md)
- `DI-15`, `DI-3` — providify 2.1.0 asks closed (P24–P27), `@Fallback` framework defaults (plans: plans/043, plans/044, plans/045)

**Earlier**
- 3.2 security & tenancy: 11 items shipped (`S4`, `S5`, `S6`, `S9`, `S11`, `S12`, `S15`, `S16`, `S17`, `S19`–`S23`; `S18` answered, not built — `extra_middleware=` landed outside `ErrorMiddleware`, dedicated modules instead) (plans/033, plans/036–042)
- 3.2 credential/HTTP-edge hardening: 8 items shipped (`S1`–`S3`, `S7`, `S8`, `S10`, `S13`, `S14`; plans/034, plans/035)
- 3.1 API surface & interop: 8 items shipped (`D1`, `D4`–`D8`, `N1`–`N3`; plans/029–032)
- 3.1 trust store, hot reload & performance: 9 items shipped (`T3`–`T7`, `P1`–`P4`; plans/025–028)
- 3.0.1 cleanup: 5 items shipped (`C1`, `C2`, `C5`, `C7`, `C8`; plans/024)
- 3.0.0 API freeze & first public release: providify 2.0.0 adoption, CI matrix, conformance/chaos suites, api-surface gate, lockstep release machinery (plans/012–023)

## Parked

Killed and deferred ideas with the reason and the un-park trigger. A row leaves only when its
trigger fires. Grouped by theme; each line is `item — why · ⏵ trigger`.

**This cycle (4.0)**
- **`FLIP-14` — provisioning-authority model for `mount_tenant_admin`** — **designed** (plans/055 §D-flip14, recorded in `technical_docs/features/admin-surface-tenancy.md`); building it is its own plan · ⏵ a second control-plane consumer, or an IdP that can mint a provisioning profile
- **`Literal[True]` sweep of the four `acknowledge_*: bool = False` → `ValueError` sites** (plans/053, plans/056 left them; the newer sites are `Literal[True]` → `TypeError`) — no unification in 4.0 · ⏵ a fifth acknowledgement kwarg is added, or mypy `strict` flags a `bool` site
- **A shared `scripts/_packages.py`** for the four verbatim `discover_packages()` copies (`api_surface.py`, `settings_reference.py`, `di_catalogue.py`, `entry_points.py`) · ⏵ a fifth script needs it
- **Redacting list-valued leaves in `redact_mapping()`** (plans/057-redaction-flips.md) · ⏵ a DTO dump with a nested list of secrets is reported
- **`register_webhook_signer()`** mirroring plans/051's `register_webhook_verifier()` · ⏵ an out-of-tree signing scheme
- **Deprecating `varco_fastapi.posture.PostureFinding`/`PostureSeverity`** (a silent re-export of `varco_core.posture`, soft deprecation) · ⏵ 5.0.0 planning
- **An HTTP endpoint for the wiring report** (plans/052-diagnose-report-and-test-profile.md) · ⏵ an operator needs the report from a running pod without log access
- **Per-phase stop timeout in `LifecycleRunner`** (plans/050-phased-lifecycle-and-di-wired-infrastructure.md) · ⏵ a component's `stop()` measured hanging shutdown in production
- **`varco diagnose --check` as a repo `make lint` gate** — environment-dependent (plans/052 §D-cli); the migration guide tells *apps* to run it · ⏵ a hermetic, environment-free mode exists
- **Flipping `import_budget.py` from warn-only to a gate** — Plan 028 Phase 2's ≥ 10 CI observations still unmet · ⏵ the observations are recorded
- **Rewriting pre-052 tests onto `InMemoryProfile`** — style, not a defect · ⏵ —
- **The 5.0.0 removal list** — written down, with floors, in `technical_docs/migrations/3-to-4.md` §10: the five AB-* names (2027-08-31), `TenantProvisionConsumer(provisioner=, catalog=)` (2027-08-31), `varco_fastapi.auth.TrustStore` (2027-09-05), `TenantResolutionMiddleware(chain=None)`'s implicit fallback → `TypeError` (2027-09-12), `ApiKeyAuth(keys=)` and Plan 048's legacy env names (4.0.0's date + 12 months) · ⏵ 5.0.0 planning, never before each floor (`test_deprecation_floor.py`)
- **Ship the DI break as 3.3.0** — rejected: CONTRIBUTING.md's SemVer policy; renumbered 4.0.0 · ⏵ never
- **Plan 036 §D-S9-flip rows 8 (`CrossCheckMode` stays `LENIENT`) and 11 (rate limiting stays opt-in)** — recorded non-flips, argued on the merits by Plans 033/035 · ⏵ new evidence only

**Security (3.2 cycle, brief 006/008/009/011/013)**
- **mTLS / `X-Forwarded-Client-Cert` `TenantSource`** — Envoy-specific, its own trust problem · ⏵ service-mesh guidance, or a consumer asks (additive out-of-tree)
- **Repository-backed `AbstractTenantMembership`** — signed-claim default covers the real case · ⏵ memberships that cannot fit in a token (out-of-tree impl of the shipped ABC)
- **DPoP / RFC 9449** — adoption early, FAPI-concentrated · ⏵ broad Python OAuth support, or a FAPI-regulated consumer
- **CAEP / SSF session revocation** — `S13`'s seam is the useful half · ⏵ a consumer needs cross-provider revocation; CAEP becomes a provider against the seam
- **SPIFFE / workload identity** — service identity, not tenant identity · ⏵ varco grows an opinion on service-to-service identity
- **`RateLimit`/`X-RateLimit-Remaining` headers** — `RateLimiter` ABC cannot report quota; design written (`RateLimitIntrospection` Protocol) · ⏵ `draft-ietf-httpapi-ratelimit-headers` becomes an RFC
- **Bounded/LRU key space on `InMemoryRateLimiter`** — changes a public class's memory semantics · ⏵ a memory-growth report
- **Token-bucket rate limiter** — two sliding-window impls exist · ⏵ burst intolerance reported
- **JSON nesting/complexity limits** — belongs in Pydantic validators (brief 008 §2) · ⏵ a standardized middleware approach
- **Per-route body ceilings / per-route CSP / per-route limits for A2A-MCP** — `exempt_paths` covers it; no authoritative pattern (brief 008 gap 1, brief 011 §E) · ⏵ two consumers needing different ceilings on two routes of one app
- **`SATokenRevocationStore` / `BeanieTokenRevocationStore`** — TTL model is Redis-native · ⏵ a denylist that must survive a cache flush
- **`RevocationFailureMode.FAIL_OPEN_WITHIN_GRACE`** — no data to size the grace window (brief 009 gap 2) · ⏵ a performance brief measuring cache-miss rate at realistic QPS
- **`TenantFanoutSupervisor`-backed per-tenant retention** — sequential loop is correct today · ⏵ a consumer with >100 tenants and per-tenant policies
- **OCSF / ECS audit-log export** ⭐ — strongest compliance candidate; same "second `Serializer`" shape as CloudEvents · ⏵ a consumer needs SIEM ingestion, or OCSF ratification completes (check ITU/OCSF status; NIS2's Jun-2026 date alone is not a trigger)
- **DSAR / right-to-erasure orchestration** — every primitive shipped, nothing composes them; no prior art; L · ⏵ a consumer with a real DSAR obligation, and its own cycle
- **Signed / externally-verifiable audit export** — pointless without OCSF export · ⏵ together with OCSF, or an auditor asks, or Sigstore Rekor gets a stable Python client
- **Crypto agility for field encryption** — cipher welded into `FernetFieldEncryptor`; cheap now, expensive later · ⏵ a PQC/FIPS requirement or a shipped cipher deprecated — un-park *before* the need
- **WebAuthn / passkeys** and **SCIM 2.0** — both need a user/credential/org model varco deliberately does not own · ⏵ varco decides to own a user model
- **OSCAL control-evidence automation** — FedRAMP-niche · ⏵ a consumer pursuing FedRAMP on varco
- **Post-quantum algorithms (FIPS 203/204/205)** — substrate not there in 2026 · ⏵ a maintained Python PQC binding; crypto-agility row is the prerequisite
- **Ed25519 Standard Webhooks verification** — needs a crypto dependency `varco_core` lacks · ⏵ an asymmetric-mode sender, or a crypto dep for another reason
- **Inbound `Rfc9421Verifier`** — additive behind the ABC; no provider uses it (brief 013 §2) · ⏵ a consumer receives RFC 9421-signed webhooks
- **`SecurityPosture` wiring for `inspect_retention_posture()`** — see `RO-2` (un-parked into a live row)

**Standing parks (all cycles)**
- **Durable execution / workflow-as-code** — XL, parked on size alone; brief 001 names it the largest strategic gap · ⏵ a major-version horizon with appetite, or a consumer requirement making the saga orchestrator's limits concrete. ⚠️ 4.0 is such a horizon — deliberately *not* absorbed: this cycle is DI-shaped
- **OpenFeature provider** — checked 2026-09-04, NOT FIRED (`openfeature-sdk` 0.10.0, spec 0.9.0); seam shipped, provider did not · ⏵ `openfeature-sdk` 1.0.0 (the SDK, not the spec)
- **RRULE / RFC 5545** — needs `dateutil` in `varco_core` · ⏵ a recurrence 5-field cron cannot express, and the dependency accepted
- **Seconds-precision / 6-field cron** — a different tool · ⏵ sub-minute scheduling needed
- **Packaging `varco_chaos`** — unstable by design, needs Docker · ⏵ a downstream writes chaos tests against varco backends
- **Per-tenant quotas & usage metering** — M-sized new surface · ⏵ metered billing events off the rate limiter (`S10` built the middleware this extends)
- **Secrets-manager sources (Vault / cloud KMS)** — now specific: an `AbstractKeyProvider` for envelope encryption (KEK in KMS, DEK via the shipped store); `EncryptionKeyStore` stores DEKs only · ⏵ signing-key rotation becoming concrete, or a consumer required to hold the KEK in an HSM
- **Server-side cert rotation via `sni_callback`** — 3.1 was outbound-only · ⏵ varco services terminate TLS directly
- **`truststore` dependency** — on Linux its verifier is a documented no-op · ⏵ official macOS/Windows support
- **Cross-platform (macOS / Windows)** — Linux-only keeps TLS design clean · ⏵ a release commits to multi-OS, which means auditing every Linux assumption (inotify, `SSL_CERT_DIR`, mtime granularity)
- **PEP 810 native lazy imports** — 3.15; matrix is 3.12/3.13 · ⏵ matrix reaches 3.15
- **Toxiproxy graded chaos** — no testcontainers module, client possibly orphaned · ⏵ a `testcontainers.toxiproxy` module or a maintained client
- **Integration tests gating PRs (RL-16)** — needs ≥30 consecutive nightly runs with ≤1 non-code failure, and is blocked by `cancel-in-progress` (research 001 §8) · ⏵ both resolved; `chaos` is never a candidate
- **GraphQL surface · event sourcing** — not on varco's axis · ⏵ concrete demand
- **Independent per-package versioning** / **`varco` umbrella meta-package** — lockstep gives one number to trust · ⏵ bump churn becoming a measured cost / lockstep abandoned
- **New dedicated e2e reference app** — per-feature chaos tests score better · ⏵ —
- **WD-1 WS backpressure margin** — a watch item · ⏵ the test failing twice on CI
- **RT4-ws-scale** — undocumented GitHub Actions fd limits · ⏵ documented limits or a measured local ceiling

## Verified as already sound (do not re-raise)

| Area | Finding |
|---|---|
| **CORS defaults** | Hardened in 3.0.0 (Plan 022 / AB-5): `allow_origins` defaults to `()` |
| **SSRF guard** | Five-layer model in `varco_core/webhook/ssrf.py` is solid; its only weakness (nothing asserts callers use `pinned_ip`) is a `SecurityPosture` collector, not a row |
| **Cache-key tenancy** | `tenancy_cache_key()` fails closed outside `tenant_context()` — the house pattern |
| **Admin mount acknowledgement** | Three barriers per mount, uniformly, since Plan 054 (`server_auth=None` → `ValueError` on all three mounts — `FLIP-3`/`FLIP-4` closed the gap) |
| **providify pytest fixtures** | `di_container`/`di_acontainer`/`di_overrides`/`di_global` are used under providify's own names (RL-3); `DI-11` documents, never wraps |

---

# Answered decisions (do not relitigate)

- **3.2 OQ1 — `SecurityPosture`'s production signal** (Plan 036 §D-S9-oq1) → an explicit
  `VARCO_SECURITY_ENV`, default `production`, governing severity presentation only; inference
  rejected because every inferable signal is itself one of the things the preflight checks.
- **3.2 OQ2 — cross-check with one source present** (Plan 033 §D-S6-oq2) → agreement-by-default
  (`LENIENT`); `STRICT` = "if any source spoke, at least two must agree".
- **3.2 OQ3 — subdomain parsing** (Plan 033 §D-S6-oq3) → explicit `base_domains`, no
  `publicsuffix2`; `X-Forwarded-Host` off by default.
- **3.2 OQ4 — does RLS-by-default change existing DDL?** (Plan 037 §D-S12-oq4) → no, additive;
  but policies must be created *before* `ENABLE ROW LEVEL SECURITY` (research 007).
- **RL-1 — providify un-vendoring sequence** (Plan 016) → two-step, one branch, two commits
  (1.1.0 sweep, then 2.0.0 sweep) so a red sweep has one cause.
- **RL-3 — providify pytest plugin adoption** (Plan 016) → document, do not wrap; a consumer
  conftest redefining `di_container` wins over the plugin default.
- **RL-9 — version bump mechanism** (Plan 023) → hand-rolled `scripts/bump.py` on tomlkit, not
  `uv version` and not hatch-vcs.
- **RL-9 — sibling pin exactness** (Plan 023) → compatible (`~=<major>.0`), never exact; lockstep
  is carried by the release process, not the metadata.
- **T3 — deprecation shim shape** (Plan 026) → a subclass, not an alias, because the new type
  widens behaviour; the `isinstance` asymmetry is a documented cost.
- **T3 — does `SSLConfig` gain reload?** (Plan 026) → no; `ReloadingTrustStore` is the only
  reloadable path.
- **T3 — where the reload task starts** (Plan 026) → the store owns `start()`/`stop()`; no
  `@Configuration` in `varco_core` because `container.scan("varco_core", recursive=True)`
  auto-activates every scanned one. ⚠️ `DI-1` must preserve this: a varco module in `varco_core`
  is *registered*, never *scanned into activation* (enforced by `test_modules_never_scanned.py`
  since Plan 049).
- **C2 — providify 2.0.1 gate** (Plan 024) → removed, not rescheduled; varco adopted `@Disposes`.
- **RT7 — chaos test placement** (Plan 018) → own job in `integration.yml`, nightly +
  `workflow_dispatch`, never a required check.
- **Standards alignment park — reversed** (Plan 022) → CloudEvents/AsyncAPI changed no public
  surface; both shipped in 3.1.
- **4.0 OQ1 — `VarcoModule` shape** (Plan 049 §D-shape) → a plain varco ABC, **never** a providify
  `@Configuration` — the scanner auto-installs every public `@Configuration` and `install()`
  constructs the class itself; a module is constructed by the app (`test_modules_never_scanned.py`).
- **4.0 OQ2 — config-selected alternatives** (Plan 047 §D-mechanism) → providify 2.1.0 ships
  `@Requires(env=, value=)`; used directly via `selected_by()` — the P25 `activate_profile()`
  translation (Plan 043) is superseded.
- **4.0 OQ3 — does 4.0.0 renumber the 3.x deprecation windows?** (Plan 058 §D-floor) → **yes**: every
  3.x window naming 4.0.0 is re-labelled `removed_in="5.0.0"` and nothing is removed (12-month floor:
  not before 2027-08-31); the floor is computed from `CHANGELOG.md`'s release-date headings and
  enforced by `varco_core/tests/test_deprecation_floor.py`. The `api_surface.py --check` gate is
  structurally unaffected — deprecated names live outside `__all__`. The same floor moved
  `TenantResolutionMiddleware(chain=None)` → `TypeError` to 5.0.0 (Plan 055).
- **4.0 — version number for the DI break** (discover 2026-09-12) → **4.0.0, not 3.3.0**;
  CONTRIBUTING.md's SemVer policy stands. The 12-month removal floor is unaffected by the major
  bump.
- **4.0 — missing providify features** (discover 2026-09-12) → filed upstream (`design/upstream-gaps/`
  + `UPSTREAM-GAPS.md` + strict xfail), never shimmed in varco.
- **4.0 — is providify's `@Default` the default-binding mechanism?** (discover follow-up
  2026-09-12) → **no.** `@Default` is CDI's qualifier, an alias of "no qualifier" (README line
  145-157). providify has no `@DefaultBean`; `@Alternative` is the inverse direction (the
  *replacement* must be decorated and enabled). Fallback binding becomes `DI-15`'s third upstream
  ask; `DI-3` keeps a *named* priority floor until it ships.
