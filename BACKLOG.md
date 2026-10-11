# BACKLOG

**Cycle live: 4.0.0 feature extension** (discover 2026-10-07; **planned 2026-10-11** → `plans/000-index-4-0-feature-extension.md`, plans 059–069b, ready for `/build`). The 4.0 DI standardization +
security-default flip list is built (plans/045–058, index `plans/000-index-4-0-di-standardization.md`);
its `v4.0.0` tag (Plan 058 Phase F) is **held** until the rows below ship, so they ride the same
major and may break without a deprecation cycle.

Everything from earlier cycles (3.0.0, 3.0.1, both 3.1 cycles, 3.2, and the 4.0 DI build) is
**complete** — compressed under `## ✅ Shipped`; the detail lives in `plans/`, `CHANGELOG.md`, and git
history. What is *never* trimmed are the **parks** (with their un-park triggers) and the
**answered decisions**: they are the reason this project does not re-argue settled questions
every cycle.

> ⚠️ An earlier incarnation of this ledger was lost wholesale in `cae7f33`. When trimming, delete
> *completed work rows*; never delete a park, a trigger, or an answered decision.

---

# 4.0.0 feature extension (discover 2026-10-07)

Research: [018 — transactionally correct endpoints & GDPR](design/research/018-transactionally-correct-endpoints-and-gdpr-frameworks.md),
[019 — streaming, orchestration & test performance](design/research/019-streaming-orchestration-and-test-performance-2026.md).
⚠️ A few citations in both briefs (DBOSify July 2026, FastAPI JSON Lines 0.134, EDPB 02/2025 on
crypto-shredding) were not independently verified at discover time — each row's `/plan` re-checks
the ones it relies on.

## Locked decisions (4.0.0 feature extension)

| Decision | Choice | Consequence |
|---|---|---|
| **Release vehicle** | **Ship inside 4.0.0**, not 4.1 | The `v4.0.0` tag waits for these rows; breaking changes allowed outright (same rule as the DI cycle), each with a `3-to-4.md` migration section |
| **Orchestration scope** | **First stage only** (`ORCH-1`) | Worker + schedule driver + chaining + fan-in; durable workflow replay stays parked |
| **ACID retry/isolation** | **Merged into `ACID-1`** | One `@transactional(isolation=…, retry=…)` boundary, designed once — retry must re-run the whole transaction |
| **Consent records (`GDPR-3`)** | **Cut → parked** | Consent shape is domain-specific; `GDPR-1`+`GDPR-2` cover Art. 15/17/20 |
| **Arrow/Parquet export** | **Parked** | NDJSON in/out is the big-data story for a REST server; no `pyarrow` extra |
| **`RO-14`** | **Pulled into 4.0.0** | Breaking behaviour change that was deferred only for want of a major |
| **Build order** | **`TEST-1` first**, despite 🟢 | Every other row's build loop runs the suites; faster tests pay back inside this cycle |

## Locked decisions (4.0 DI cycle, discover 2026-09-12 — still in force)

| Decision | Choice | Consequence |
|---|---|---|
| **Breaking changes** | **Allowed outright** — wiring APIs may change shape without a parallel deprecated path | No two-shape cycle; a written migration guide is part of every breaking row's done criteria |
| **Version number** | **4.0.0, not 3.3.0** | CONTRIBUTING.md's SemVer policy stands unamended. ⚠️ The **12-month removal floor still applies**: symbols deprecated `since="3.0.0", removed_in="4.0.0"` (nine sites, `v3.0.0` tagged 2026-08-31) may not be removed before 2027-08-31 — the major-version gate is necessary, not sufficient. 4.0.0 breaks *wiring*; it does not sweep aliases |
| **Missing providify features** | **Filed upstream, never worked around** — `design/upstream-gaps/providify-*.md` + `UPSTREAM-GAPS.md` row + `strict=True` xfail, per CLAUDE.md's rule | `DI-15` was a gating row: the providify asks landed first so the varco rows built on the real feature |
| **Scope** | **All 16 DI rows live; nothing cut for size** | `DI-13` (wiring tests) and `DI-14` (the guide) were standalone rows *and* done criteria of every other row |
| **The recorded 4.0 flip list** | **Absorbed** as `FLIP-*` rows | A user migrates once. Rows 8 and 11 of Plan 036 §D-S9-flip are recorded non-flips and are **not** carried |

## Live

### 4.0.0 scope

All nine rows planned 2026-10-11 → index [`plans/000-index-4-0-feature-extension.md`](plans/000-index-4-0-feature-extension.md);
folded into `## ✅ Shipped` as **planned, not yet built**. The `v4.0.0` tag waits for them.

| ID | Feature | Severity | Complexity | Status | Rationale | Evidence |
|----|---------|----------|------------|--------|-----------|----------|
_(empty — run /discover)_

### Carried over from 3.2 (open, unscheduled — not in the 4.0.0 scope)

Non-breaking fixes; none blocks a 4.0.0 row (except `RO-17`, which `GDPR-2`'s encrypted-field promise needs — schedule it with 065/066). `RO-6`/`RO-9` were absorbed into `ORCH-1`, `RO-14` into the scope above. `RO-15`–`RO-17` were found while planning (2026-10-11).

| ID | Feature | Severity | Complexity | Status | Rationale | Evidence |
|----|---------|----------|------------|--------|-----------|----------|
| `RO-1` | `MetricsMiddleware` records `status_code="500"` for exceptions `ErrorMiddleware` renders as 4xx | 🟡 should | S | todo | Every error-rate dashboard over `http.server.request.duration` is wrong for mapped exceptions; out of Plan 041's scope | `varco_fastapi/varco_fastapi/middleware/metrics.py:368,375-388`; `app.py:636-642` |
| `RO-3` | Confirm Stripe's inbound HMAC secret encoding against a real delivery | 🟡 should | S | todo | `StripeWebhookVerifier` pins `RAW_UTF8` against brief 013 §2's conflicting description; if wrong, one enum value on one class | `plans/038` Risks table |
| `RO-7` | `0007_schedules_table.py` vs. `materializer.py:29`'s claimed `UNIQUE(schedule_id, run_at)` index — fix the DESIGN block (the index cannot exist; `uuid5` `job_id` is the real backstop) | 🟡 should | S | todo | Verified absent by Plan 039 / Step 12 | `varco_sa/varco_sa/schedule.py:11-19` |
| `RO-8` | DLQ and `WebhookDelivery` payload redaction (fifth/sixth redaction surfaces) | 🟡 should | M | todo | Out of Plan 040's scope; natural follow-on to `GDPR-1` | `plans/040` §Parked |
| `RO-10` | `CONF-AUDIT6` — assertion-audit the six unaudited conformance suites for Kind-B gaps | 🟡 should | M | todo | Plan 042 audited `cache` and `dlq` only | `testkit/varco_conformance/` |
| `RO-11` | Verify `opentelemetry-exporter-prometheus` emits exemplars end-to-end | 🟢 nice | S | todo | Plan 041 proves the SDK layer only; `router/metrics.py:45-51`'s promise is half-verified | brief 012 Evidence Gaps 2/3 |
| `RO-12` | `CONF-COUNT` — strengthen `DeadLetterQueueConformance.test_count_reflects_pushed_entries` (trivially true at `KafkaDLQ.count()`'s `-1`) | 🟢 nice | S | todo | May surface a Kind-A `KafkaDLQ` finding needing its own xfail + register row | `varco_kafka/tests/test_kafka_conformance.py:80-88` |
| `RO-13` | `CONF-RG` — automate the register↔marker cross-check in `make lint`'s no-`PKG` path | 🟢 nice | S | todo | Plan 042 Q2 left it manual | `COVERAGE.md` findings register |
| `RO-15` | `EventConsumer` retry wrapper catches `BaseException` — `asyncio.CancelledError` (shutdown) is dead-lettered as a failure when no retry policy is set; re-raise non-`Exception`s first | 🔴 must | S | todo | Found by Plan 060's scout; out of its scope (060 Open Q3) — a cancelled handler at pod shutdown pollutes the DLQ | `varco_core/varco_core/event/consumer.py:589` |
| `RO-16` | `varco_casbin`'s `beanie` extra still pulls Motor (EOL May 2026) — migrate its Beanie adapter to PyMongo's `AsyncMongoClient` like `varco_beanie` | 🟡 should | S–M | todo | Found by Plan 063 (Open Q7); `varco_beanie` already migrated, `motor` 3.7.1 is in the lock only through casbin | `uv.lock:2866-2870`; `plans/063` premise 5 |
| `RO-17` | `EncryptedHint` is inert on the standard repository path — the DI-wired `SQLAlchemyRepositoryProvider` (and the Beanie factory) pass no encryptor, so the mapper silently no-ops | 🔴 must | M | todo | Contradicts 065 §D-reads ("encrypted fields arrive decrypted") and 064's posture remedy ("add `EncryptedHint()`"); should land before or with `GDPR-2` | `varco_sa/varco_sa/provider.py:168,174`; `mapper.py:130-132`; `plans/066` Open Q9 |

## ✅ Shipped

Most recent individually; older cycles rolled up by theme. Detail: `plans/`, `CHANGELOG.md`.

**4.0.0 feature extension — ✅ planned 2026-10-11, not yet built** (index: plans/000-index-4-0-feature-extension.md)
- `TEST-1` faster unit tests — xdist per suite, order audit, measured (plan: plans/059)
- `RO-14` auto-registered consumers get the app DLQ (plan: plans/060)
- `ACID-2` optimistic concurrency — `row_version` check in SQL, ETag/If-Match, 412/428 (plan: plans/061)
- `ACID-1` ambient transactions — `@transactional`, propagation, isolation, retry; SA + Mongo (plans: plans/062, plans/063)
- `GDPR-1` personal-data classification + `varco gdpr inventory` (plan: plans/064)
- `GDPR-2` data-subject export and erasure as jobs (plans: plans/065, plans/066)
- `STREAM-1` NDJSON export, keyset cursors, SA `stream()` fixed (plan: plans/067)
- `STREAM-2` streaming NDJSON bulk ingest with per-line report (plan: plans/068)
- `ORCH-1` (+`RO-9`, `RO-6`) job dispatcher, enqueue, schedule driver, chains/fan-in (plans: plans/069a, plans/069b)

Correction recorded at planning time: `ACID-2`'s discover rationale ("no version column … anywhere")
was wrong — `row_version` already exists, but the version check runs in Python after a read, so
lost updates are still possible between read and write; Plan 061 moves the check into the
`UPDATE`/`replace_one` filter.

**Earlier**
- 4.0 DI standardization + flip list: 37 items shipped — DI foundations (`DI-2`–`DI-4`, `DI-6`, `DI-8`, `DI-12`, `DI-13`, `DI-15`–`DI-17`; plans/043–048), modules/lifecycle/multi-binding/diagnostics (`DI-1`, `DI-5`, `DI-7`, `DI-9`–`DI-11`, `DI-14`, `RO-2`, `RO-4`; plans/049–052, 058), security-default flips (`FLIP-1`–`FLIP-7`, `FLIP-9`, `FLIP-10`, `FLIP-12`, `FLIP-13`, `FLIP-034-1`…`4`, `FLIP-040-1`/`2`, `RO-5`; plans/053–057). `v4.0.0` tag held for the feature extension
- 3.2 security & tenancy: 11 items shipped (`S4`, `S5`, `S6`, `S9`, `S11`, `S12`, `S15`, `S16`, `S17`, `S19`–`S23`; `S18` answered, not built — `extra_middleware=` landed outside `ErrorMiddleware`, dedicated modules instead) (plans/033, plans/036–042)
- 3.2 credential/HTTP-edge hardening: 8 items shipped (`S1`–`S3`, `S7`, `S8`, `S10`, `S13`, `S14`; plans/034, plans/035)
- 3.1 API surface & interop: 8 items shipped (`D1`, `D4`–`D8`, `N1`–`N3`; plans/029–032)
- 3.1 trust store, hot reload & performance: 9 items shipped (`T3`–`T7`, `P1`–`P4`; plans/025–028)
- 3.0.1 cleanup: 5 items shipped (`C1`, `C2`, `C5`, `C7`, `C8`; plans/024)
- 3.0.0 API freeze & first public release: providify 2.0.0 adoption, CI matrix, conformance/chaos suites, api-surface gate, lockstep release machinery (plans/012–023)

## Parked

Killed and deferred ideas with the reason and the un-park trigger. A row leaves only when its
trigger fires. Grouped by theme; each line is `item — why · ⏵ trigger`.

**4.0.0 feature extension (discover 2026-10-07)**
- **`GDPR-3` — consent ledger (Art. 7) + processing-purpose records** — consent shape (what, which terms version, which channel) is domain-specific; no framework precedent (brief 018 §B); `GDPR-1`/`GDPR-2` cover the strict Art. 15/17/20 requirements · ⏵ a consumer with a concrete consent model
- **Apache Arrow / Parquet export** (was part of `STREAM-2`) — `pyarrow` is a ~40 MB native extra; an optional differentiator, not table stakes (brief 019 §A) · ⏵ a consumer exporting to an analytics tool (pandas/DuckDB/Spark)
- **Full durable workflows** (`@workflow`/`@step` replay, durable timers, signals, compensation replacing the saga orchestrator) — XL; `ORCH-1` is the first stage · ⏵ `ORCH-1` shipped, plus a consumer whose workflow must survive a crash mid-step

**4.0.0 feature extension — parks recorded by the plans (2026-10-11)**. Full trigger tables live in each plan's `## Parked`/`## Out of scope` section.
- **looptime fake loop clock** (059) — xdist + settle removal first · ⏵ a loop-time-only wait adds ≥ 3 s / ≥ 10 % to a suite's post-xdist wall
- **Integration/chaos parallelism** (059) — needs an async FileLock pattern for session containers · ⏵ its own research brief
- **`OutboxEventProducer` — atomic audit/events inside a boundary** (062) — audit stays post-commit in 4.0 · ⏵ a user needs atomic audit, or 062 Open Q3 answered yes
- **`SUPPORTS`/`NOT_SUPPORTED`/`NEVER` propagation; snapshot-consistent multi-statement export** (062) · ⏵ a concrete caller
- **Retrying `23505` under `SERIALIZABLE`; `IntegrityError` → `ServiceConflictError` in `varco_sa`; `SQLAlchemyUnitOfWork.commit()` not closing its session** (062) — separate gaps · ⏵ a real report
- **Per-UoW Mongo transactions outside a boundary; Beanie `IUoWProvider` for `TenantIsolation.DATABASE`; removing `BeanieSettings.transactional`** (063) · ⏵ SA-parity request / a `BeanieTenantPool` service user / 5.0.0 (not before 2027-10)
- **Flip `ListMixin` default to cursor pagination** (067) · ⏵ two apps report offset drift *and* a major that can warn their clients
- **Null-aware keyset, Postgres row-value fast path, `rel="prev"`, export in `gen-client`/MCP/A2A** (067) · ⏵ per-item, see plans/067
- **Upsert / all-or-nothing / job-backed / compressed ingest; `Idempotency-Key` on ingest** (068) · ⏵ per-item, see plans/068
- **Push wake-up (`LISTEN/NOTIFY`, change streams), job metrics, poison-pill detection, named queues/priorities, cross-instance cancel of RUNNING jobs** (069a) · ⏵ per-item, see plans/069a
- **General DAG enqueue, implicit result passing, relative step delays, `ScheduleConcurrency.REPLACE`** (069b) · ⏵ per-item, see plans/069b
- **"Forgettable payloads" — per-subject encryption of audit diff values** (066) — erasure keeps past diffs under the Art. 17(3) assessment + one tombstone · ⏵ a regulator/DPA position that retained audit diffs are not covered by 17(3)
- **`varco gdpr erase --replay` — re-apply erasures after a backup restore** (066 §D-backups) — tombstones' `subject_key` makes it possible; procedure is documented · ⏵ the first real restore, or 066 Open Q8 answered "now"
- **Field-level crypto-shred strategy (`FieldErasure.SHRED`)** (066) — needs async per-subject key resolution in the mapper · ⏵ `RO-17` fixed plus a consumer needing it

**4.0 DI cycle**
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
- **Per-route body ceilings / per-route CSP / per-route limits for A2A-MCP** — `exempt_paths` covers it; no authoritative pattern (brief 008 gap 1, brief 011 §E) · ⏵ two consumers needing different ceilings on two routes of one app (⚠️ `STREAM-2` needs a body-limit exemption for its ingest route — resolve inside its plan, not by un-parking this)
- **`SATokenRevocationStore` / `BeanieTokenRevocationStore`** — TTL model is Redis-native · ⏵ a denylist that must survive a cache flush
- **`RevocationFailureMode.FAIL_OPEN_WITHIN_GRACE`** — no data to size the grace window (brief 009 gap 2) · ⏵ a performance brief measuring cache-miss rate at realistic QPS
- **`TenantFanoutSupervisor`-backed per-tenant retention** — sequential loop is correct today · ⏵ a consumer with >100 tenants and per-tenant policies
- **OCSF / ECS audit-log export** ⭐ — strongest compliance candidate; same "second `Serializer`" shape as CloudEvents · ⏵ a consumer needs SIEM ingestion, or OCSF ratification completes (check ITU/OCSF status; NIS2's Jun-2026 date alone is not a trigger)
- ~~**DSAR / right-to-erasure orchestration**~~ — **un-parked 2026-10-07 as `GDPR-2`** (trigger fired: its own cycle, with appetite)
- **Signed / externally-verifiable audit export** — pointless without OCSF export · ⏵ together with OCSF, or an auditor asks, or Sigstore Rekor gets a stable Python client
- **Crypto agility for field encryption** — cipher welded into `FernetFieldEncryptor`; cheap now, expensive later · ⏵ a PQC/FIPS requirement or a shipped cipher deprecated — un-park *before* the need
- **WebAuthn / passkeys** and **SCIM 2.0** — both need a user/credential/org model varco deliberately does not own · ⏵ varco decides to own a user model
- **OSCAL control-evidence automation** — FedRAMP-niche · ⏵ a consumer pursuing FedRAMP on varco
- **Post-quantum algorithms (FIPS 203/204/205)** — substrate not there in 2026 · ⏵ a maintained Python PQC binding; crypto-agility row is the prerequisite
- **Ed25519 Standard Webhooks verification** — needs a crypto dependency `varco_core` lacks · ⏵ an asymmetric-mode sender, or a crypto dep for another reason
- **Inbound `Rfc9421Verifier`** — additive behind the ABC; no provider uses it (brief 013 §2) · ⏵ a consumer receives RFC 9421-signed webhooks
- **`SecurityPosture` wiring for `inspect_retention_posture()`** — see `RO-2` (un-parked into a live row, shipped in plans/052)

**Standing parks (all cycles)**
- **Durable execution / workflow-as-code** — XL; brief 001 names it the largest strategic gap. **Partially un-parked 2026-10-07**: the first stage is `ORCH-1`; replay/timers/compensation are the "Full durable workflows" park above · ⏵ see that park
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
| **Repository streaming & bulk writes** | `AsyncRepository.stream_by_query()` (Beanie), `AsyncService.stream()`, and `save_many()` exist. ⚠️ **Corrected 2026-10-11**: the SQLAlchemy `stream()` is **broken** (`varco_sa/repository.py:392` — `stream_scalars` is a coroutine function, not awaited), so the 2026-10-07 "verified" claim was wrong for SA; Plan 067 fixes it. `STREAM-*` otherwise builds the HTTP layer, not the repository layer |

---

# Answered decisions (do not relitigate)

- **4.0.0 feature extension — release vehicle** (discover 2026-10-07) → the `ACID-*`/`GDPR-*`/`STREAM-*`/
  `ORCH-1`/`TEST-1`/`RO-14` rows ship **inside 4.0.0**, holding the tag; breaking changes allowed.
- **4.0.0 feature extension — orchestration scope** (discover 2026-10-07) → first stage only
  (`ORCH-1`); durable replay parked behind it.
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
