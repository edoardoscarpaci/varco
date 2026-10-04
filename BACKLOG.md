# BACKLOG

**One cycle is live: `# 4.0 — dependency-injection standardization`, immediately below — fully planned
(plans/045–058, index `plans/000-index-4-0-di-standardization.md`), building next.**

Everything from earlier cycles (3.0.0, 3.0.1, both 3.1 cycles, and 3.2) is **complete and
shipped** — compressed under `## ✅ Shipped`; the detail lives in `plans/`, `CHANGELOG.md`, and git
history. What is *never* trimmed are the **parks** (with their un-park triggers) and the
**answered decisions**: they are the reason this project does not re-argue settled questions
every cycle.

> ⚠️ An earlier incarnation of this ledger was lost wholesale in `cae7f33`. When trimming, delete
> *completed work rows*; never delete a park, a trigger, or an answered decision.

---

# 4.0 — dependency-injection standardization (discover, 2026-09-12)

Produced by `/discover` with the focus *"fix and standardize the whole DI story: factories,
multiple injection, feature enabling, and the defaults varco provides — learn from Quarkus, Spring,
Django, and document everything."* Originally reserved as "3.3.0, the DI fix"; **renumbered
4.0.0 at the interview** because the wiring surface breaks outright and CONTRIBUTING.md's
SemVer policy does not allow that in a minor.

**Research briefs backing this cycle:**
[`010-default-implementation-di-conventions.md`](design/research/010-default-implementation-di-conventions.md)
(Quarkus `@DefaultBean` / Spring `@ConditionalOnMissingBean` / ASP.NET `TryAdd*`, startup
diagnostics, fail-closed security defaults),
[`014-multi-binding-qualifiers-and-conditional-di.md`](design/research/014-multi-binding-qualifiers-and-conditional-di.md)
(collection injection, qualifiers, conditional activation, async factories, lifecycle phases —
⚠️ its claim that providify has `@Requires` is wrong; providify's conditional primitives are
`@Profile`/`@Alternative`),
[`015-starters-modules-and-configuration-binding.md`](design/research/015-starters-modules-and-configuration-binding.md)
(module/starter unit, options binding, app-level composition, startup validation, test profiles).

## The problem this cycle exists to fix

The scout inventory (2026-09-12) found **~53 wiring entry points across six verb shapes**
(`bootstrap`/`async_bootstrap`/`bind_*`/`enable_*`/`mount_*`/`install_*`), each package with its
own return contract (`bootstrap()` returns the container in some, `None` in others, undocumented),
**15 framework defaults registered by hand at `priority=-sys.maxsize-1`**, every "all
implementations of X" case gathered by a hand-rolled registry (0 uses of `InjectInstances`/
`Multibound`), backend selection by "first-scanned wins", 32 `DIContainer.current()`
service-locator sites in 14 files, all 11 `create_varco_app` subsystems hand-constructed and
hand-passed, and DI documented in five places with none complete. Meanwhile varco uses **none** of
providify's `@Alternative`/`@Profile`/`Multibound`/`ConfigProperties`/`depends_on=`/
`validate()` features. The fix is one contract, one default model, one selection mechanism, one
report, one document.

## Locked decisions (this session)

| Decision | Choice | Consequence |
|---|---|---|
| **Breaking changes** | **Allowed outright** — wiring APIs may change shape without a parallel deprecated path | No two-shape cycle; a written migration guide is part of every breaking row's done criteria |
| **Version number** | **4.0.0, not 3.3.0** | CONTRIBUTING.md's SemVer policy stands unamended. ⚠️ The **12-month removal floor still applies**: symbols deprecated `since="3.0.0", removed_in="4.0.0"` (nine sites, `v3.0.0` tagged 2026-08-31) may not be removed before 2027-08-31 — the major-version gate is necessary, not sufficient. 4.0.0 breaks *wiring*; it does not sweep aliases |
| **Missing providify features** | **Filed upstream, never worked around** — `design/upstream-gaps/providify-*.md` + `UPSTREAM-GAPS.md` row + `strict=True` xfail, per CLAUDE.md's rule | `DI-15` is a gating row: the providify asks land first so the varco rows build on the real feature |
| **Scope** | **All 16 DI rows live; nothing cut for size** | "A huge jump" — more rows may be added mid-cycle. `DI-13` (wiring tests) and `DI-14` (the guide) are standalone rows *and* done criteria of every other row |
| **The recorded 4.0 flip list** | **Absorbed** as `FLIP-*` rows | A user migrates once. Rows 8 and 11 of Plan 036 §D-S9-flip are recorded non-flips and are **not** carried |

## Live

Ordered by severity, then complexity ascending. The 4.0 `DI-*`/`FLIP-*` rows are all planned — see
`## 📋 Planned — 4.0` below; only the carried-over rows remain live here.

_(4.0 theme: empty — every row is planned; carried-over rows below)_

## 📋 Planned — 4.0 (2026-09-20)

Every row of the 4.0 Live table is planned, one line per plan file. Build order, dependency edges, and
the whole-set done criteria: [`plans/000-index-4-0-di-standardization.md`](plans/000-index-4-0-di-standardization.md).
A row moves to `## ✅ Shipped` only when its plan is built and merged.

- `DI-2`, `DI-8`, `DI-13` — plans/046-wiring-verb-collapse-and-explicit-container.md — `done (Plan 046)`
- `DI-12`, `DI-4`, `DI-17` — plans/047-config-driven-backend-selection-and-catalogue.md — `done (Plan 047)`
- `DI-6`, `DI-16` — plans/048-settings-standardization.md — `done (Plan 048)`
- `DI-1`, `FLIP-1` — plans/049-varco-module-and-composable-app.md — `done (Plan 049)`
- `DI-9`, `DI-10` — plans/050-phased-lifecycle-and-di-wired-infrastructure.md — `done (Plan 050)`
- `DI-5` — plans/051-multi-binding-registries.md — `done (Plan 051)`
- `DI-7`, `DI-11` — plans/052-diagnose-report-and-test-profile.md — `done (Plan 052)`
- `FLIP-5`, `FLIP-034-4`, `FLIP-034-1`, `FLIP-034-2`, `FLIP-034-3` — plans/053-credential-and-auth-flips.md — `done (Plan 053)`
- `FLIP-2`, `FLIP-10`, `FLIP-3`, `FLIP-4` — plans/054-http-edge-and-admin-surface-flips.md — `done (Plan 054)`
- `FLIP-7`, `FLIP-6`, `FLIP-9` — plans/055-tenant-provenance-flips.md — `done (Plan 055)` (FLIP-6: the default flips to the chain; `chain=None` → `TypeError` deferred to 5.0.0 — 12-month floor from 3.2.0, the implicit fallback is `removed_in="5.0.0"`)
- `FLIP-14` — `designed (Plan 055 §D-flip14) — implementation is its own future plan` (`technical_docs/features/admin-surface-tenancy.md`)
- `FLIP-13`, `FLIP-12` — plans/056-data-layer-flips.md — `done (Plan 056)`
- `FLIP-040-1`, `FLIP-040-2`, `RO-5` — plans/057-redaction-flips.md — `done (Plan 057)`
- `DI-14` — plans/058-di-guide-migration-and-4-0-release.md

### Carried over from 3.2 (open, not this cycle's theme)

Roll-forward rows that lost their un-IDed status here; none is DI-shaped and none blocks a `DI-*` row.

| ID | Feature | Severity | Complexity | Status | Rationale | Evidence |
|----|---------|----------|------------|--------|-----------|----------|
| `RO-1` | `MetricsMiddleware` records `status_code="500"` for exceptions `ErrorMiddleware` renders as 4xx | 🟡 should | S | todo | Every error-rate dashboard over `http.server.request.duration` is wrong for mapped exceptions; out of Plan 041's scope | `varco_fastapi/varco_fastapi/middleware/metrics.py:368,375-388`; `app.py:636-642` |
| `RO-2` | Wire `inspect_jwks_posture()` and `inspect_retention_posture()` into `SecurityPosture` | 🟡 should | S | ✅ planned → plans/052-diagnose-report-and-test-profile.md | Plans 041/039 export the inspectors; the harness (`posture.py:258`, collectors `:400+`) does not call them. Natural to land inside `DI-7`. Post-Plan-051: each is now a ~15-line `PostureCollector` subclass contributed by `JwksRefreshModule`/`RetentionModule` — `PostureCollector` is `@Multibound`, so no `varco_fastapi/posture.py` edit is needed | `plans/039` §D-S20-posture |
| `RO-3` | Confirm Stripe's inbound HMAC secret encoding against a real delivery | 🟡 should | S | todo | `StripeWebhookVerifier` pins `RAW_UTF8` against brief 013 §2's conflicting description; if wrong, one enum value on one class | `plans/038` Risks table |
| `RO-4` | `create_varco_app` gains a `log_redactor=` keyword | 🟡 should | S | `done (Plan 049)` — landed as `LoggingModule(redactor=...)`, not a `create_varco_app` keyword | Deliberately not added by Plan 040; folds into `DI-1`'s module shape | `plans/040` §D-S21-logging |
| `RO-5` | `inspect_redaction_posture()` becomes a `SecurityPosture` collector | 🟡 should | S | `done (Plan 057)` — `varco_fastapi.posture.RedactionPostureCollector`, a default collector contributed by `SecurityPostureModule`; the audited services are explicit input via `SecurityPostureModule(audited_services=...)` | Un-parked by `FLIP-040-2` — the finding then means "opted out" | `plans/040` §D-S21-posture |
| `RO-6` | `varco_core.schedule` general driver (a materialization loop for non-retention schedules) | 🟡 should | S | todo | Plan 032 shipped `ScheduleMaterializer` with no general caller; 039 closed it for retention only | `plans/039` §D-S20-driver |
| `RO-7` | `0007_schedules_table.py` vs. `materializer.py:29`'s claimed `UNIQUE(schedule_id, run_at)` index — fix the DESIGN block (the index cannot exist; `uuid5` `job_id` is the real backstop) | 🟡 should | S | todo | Verified absent by Plan 039 / Step 12 | `varco_sa/varco_sa/schedule.py:11-19` |
| `RO-8` | DLQ and `WebhookDelivery` payload redaction (fifth/sixth redaction surfaces) | 🟡 should | M | todo | Out of Plan 040's scope | `plans/040` §Parked |
| `RO-9` | A generic `JobDispatcher` poll loop over `claim_next()` | 🟡 should | M | todo | Plan 039 rode `JobRunner.recover()` instead; a second execution path belongs to Plan 005's subsystem | `plans/039` §D-S20-dispatch |
| `RO-10` | `CONF-AUDIT6` — assertion-audit the six unaudited conformance suites for Kind-B gaps | 🟡 should | M | todo | Plan 042 audited `cache` and `dlq` only | `testkit/varco_conformance/` |
| `RO-11` | Verify `opentelemetry-exporter-prometheus` emits exemplars end-to-end | 🟢 nice | S | todo | Plan 041 proves the SDK layer only; `router/metrics.py:45-51`'s promise is half-verified | brief 012 Evidence Gaps 2/3 |
| `RO-12` | `CONF-COUNT` — strengthen `DeadLetterQueueConformance.test_count_reflects_pushed_entries` (trivially true at `KafkaDLQ.count()`'s `-1`) | 🟢 nice | S | todo | May surface a Kind-A `KafkaDLQ` finding needing its own xfail + register row | `varco_kafka/tests/test_kafka_conformance.py:80-88` |
| `RO-13` | `CONF-RG` — automate the register↔marker cross-check in `make lint`'s no-`PKG` path | 🟢 nice | S | todo | Plan 042 Q2 left it manual | `COVERAGE.md` findings register |
| `RO-14` | `CoreModule.start()`'s 051 auto-`register_to()` loop passes no `dlq=` to any auto-registered `EventConsumer` — the app's own DLQ never reaches a consumer unless it hand-calls `register_to(bus, dlq=...)` | 🟡 should | S | todo | Deliberately out of Plan 050's scope (§Non-goals) — a behaviour change for every consumer; `WebhookDispatcher` got its own injected `dlq` instead, precisely to avoid needing this | `plans/050` §Non-goals |

## Open questions (this cycle)

1. **`VarcoModule` shape (`DI-1`)** — a providify `@Configuration` subclass with `depends_on=`,
   or a varco ABC that *owns* a `@Configuration`? The former reuses providify's module ordering
   and `@Disposes`; the latter keeps `start()/stop()` phases (`DI-9`) out of providify's
   vocabulary. Decide in `/plan DI-1`, citing providify source for what `depends_on=` guarantees
   at install time.
   **Answered (Plan 049 §D-shape):** a plain varco ABC, never a `@Configuration` — providify's scanner
   auto-installs every public `@Configuration`, `depends_on` orders install only, and `install()`
   constructs the class itself.
2. **Config-selected alternatives (`DI-4`)** — can providify's `@Alternative` be *selected from
   configuration* (Quarkus `selected-alternatives`), or only from code? If only code, this is a
   `DI-15` upstream ask before `DI-4` can be planned.
   **Answered (Plan 043 / P25 §5):** code or @Profile only; DI-4 not blocked via a public-API
   activate_profile() translation — see the report. **Superseded (Plan 047):** providify 2.1.0's
   `@Requires(env=, value=)` replaces the profile translation outright.
   **Re-answered (Plan 047 §D-mechanism):** providify 2.1.0 ships `@Requires(env=, value=)`; used
   directly, no profile translation.
3. **Does 4.0.0 also renumber the 3.x → 4.0 deprecation windows?** `FLIP-034-1` assumes
   `since="4.0.0", removed_in="5.0.0"`. Confirm the nine existing `removed_in="4.0.0"` sites stay
   *un-removed* in 4.0.0 (12-month floor: not before 2027-08-31) and get re-labelled, or the
   `--check` gate in `scripts/api_surface.py` will read a removal as a break.
   **Answered (Plan 058):** un-removed and re-labelled `removed_in="5.0.0"`; a floor test guards it.
   Plan 055 found the same floor blocks `TenantResolutionMiddleware(chain=None)` → `TypeError`
   (3.2.0 tagged 2026-09-12) — the default flips to the chain, the error waits for 5.0.

## ✅ Shipped

Most recent ten individually; older cycles rolled up by theme. Detail: `plans/`, `CHANGELOG.md`.

- `DI-15` (done — Plan 043/044 filed P24-P27; Plan 045 closed all four against providify 2.1.0),
  `DI-3` (done — Plan 045; the interim `FRAMEWORK_DEFAULT_PRIORITY` named-floor clause never
  built, since providify 2.1.0 shipped `@Fallback` first) — `@Fallback` framework defaults across
  all ten packages (plan: plans/045)
- `S23` conformance-guard recovery — five lost findings recorded, Docker-free guards (plan: plans/042)
- `S21` unified redaction seam — `varco_core.redaction` behind spans/errors/audit (plan: plans/040)
- `S19` inbound webhook verification — `WebhookVerifier` ABC, four providers, replay guard (plan: plans/038)
- `S22` JWKS background refresh — `start_refresh()`, `JwksRefreshLifecycle`, posture inspector (plan: plans/041)
- `S17` `MetricsMiddleware` inside `TracingMiddleware` — exemplars reachable (plan: plans/041)
- `S20` retention & purge automation — `varco_core.retention`, scheduler, CLI, posture (plan: plans/039)
- `S18` answered, not built — `extra_middleware=` lands outside `ErrorMiddleware`; dedicated kwargs instead (rule in CLAUDE.md)
- `S12` `S15` data-layer tenant enforcement — RLS-by-default, `rls_autogen`, applicator assertion (plan: plans/037)
- `S4` `S9` `S11` authorization surface — admin cross-tenant guards, `SecurityPostureLifecycle`, `AuditingAuthorizer` (plan: plans/036)
- `S5` `S6` `S16` tenant identity — `TenantSourceChain`, membership, RFC 8693 act-as (plan: plans/033)
- 3.2 credential/HTTP-edge hardening: 8 items shipped (`S1`–`S3`, `S7`, `S8`, `S10`, `S13`, `S14`; plans/034, plans/035)
- 3.1 API surface & interop: 8 items shipped (`D1`, `D4`–`D8`, `N1`–`N3`; plans/029–032)
- 3.1 trust store, hot reload & performance: 9 items shipped (`T3`–`T7`, `P1`–`P4`; plans/025–028)
- 3.0.1 cleanup: 5 items shipped (`C1`, `C2`, `C5`, `C7`, `C8`; plans/024)
- 3.0.0 API freeze & first public release: providify 2.0.0 adoption, CI matrix, conformance/chaos suites, api-surface gate, lockstep release machinery (plans/012–023)

## Parked

Killed and deferred ideas with the reason and the un-park trigger. A row leaves only when its
trigger fires. Grouped by theme; each line is `item — why · ⏵ trigger`.

**This cycle (4.0)**
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
