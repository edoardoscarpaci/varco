# BACKLOG

**One cycle is live: `# 3.2 — security release`, immediately below.**

Everything from earlier cycles (3.0.0, 3.0.1, and both 3.1 cycles) is **complete and shipped**.
Their work tables have been trimmed — the detail lives in `plans/`, `CHANGELOG.md`, and git
history, and restating it here only creates a second copy that drifts. What is *not* trimmed, and
must never be, are the **standing parks** and the **answered decisions**: those two sections are
the reason this project does not re-argue settled questions every cycle, and they are consolidated
across all cycles at the bottom of this file.

> ⚠️ An earlier incarnation of this ledger was lost wholesale in `cae7f33`. When trimming, delete
> *completed work rows*; never delete a park, a trigger, or an answered decision.

---

# 3.2 — security release (discover, 2026-09-05; extended 2026-09-07)

Produced by `/discover` with a focus on tenant-identity trust. **This cycle is a dedicated
security release** — its rows are not competing with feature work, and none of them were cut for
size. **3.2.0 shipped on 2026-09-12** (tag `v3.2.0`); the cycle is closed. Open rows in the
Live table below roll forward into the next cycle.

**Extended 2026-09-07 by a second `/discover` pass** (scout inventory + research brief
[`011-security-platform-table-stakes-2026.md`](design/research/011-security-platform-table-stakes-2026.md)),
run after `S1`–`S16` all landed. That pass verified every shipped row against source, then asked
what a post-3.2 security surface still lacks. **The interview chose a "finish the gaps" scope over
a new compliance-evidence theme** — five rows (`S17`, `S19`–`S23`), all of them verified holes in
subsystems varco already ships, none of them a new architectural commitment. The larger
compliance-evidence candidates the brief surfaced are **parked with triggers**, not dropped.

⚠️ **These rows ship in 3.2.0, not 3.3.0.** 3.3.0 is reserved for the DI fix.

**Research briefs backing this cycle:**
[`006-multi-tenant-identity-and-hardening.md`](design/research/006-multi-tenant-identity-and-hardening.md) (the original cycle),
[`007-postgres-rls-enforcement-mechanics.md`](design/research/007-postgres-rls-enforcement-mechanics.md) (S12/S15),
[`008-http-hardening-conventions-2026.md`](design/research/008-http-hardening-conventions-2026.md) (S7/S8/S10),
[`009-token-revocation-and-credential-storage.md`](design/research/009-token-revocation-and-credential-storage.md) (S13/S14),
[`011-security-platform-table-stakes-2026.md`](design/research/011-security-platform-table-stakes-2026.md) (the extension),
[`012-otel-exemplars-and-metrics-span-context.md`](design/research/012-otel-exemplars-and-metrics-span-context.md) (S17),
[`013-inbound-webhook-signature-verification.md`](design/research/013-inbound-webhook-signature-verification.md) (S19).

`S1`–`S16` were carved into five plans —
[`plans/000-index-3-2-security-release.md`](plans/000-index-3-2-security-release.md) — and **all
five have shipped**, verified against source on 2026-09-07 (see `## Shipped this cycle` below).
The **six extension rows (`S17`, `S19`–`S23`) were carved into five plans** —
[`plans/000-index-3-2-extensions.md`](plans/000-index-3-2-extensions.md) — with build order,
shared-file edges, and set-level done criteria, and **all six have shipped**, verified against
source on 2026-09-12 (see `## Shipped this cycle` below).

⚠️ **Planning found that every one of the six rows had a partly-wrong premise** — `S19`'s verifier
is half-shipped, `S20`'s outbox verb does not exist while two others already do, `S21` has one live
leak rather than four, and `S23`'s findings were never lost (only their index was). The index file's
"finding that shaped all five plans" table carries the detail. **Do not treat these rows' own
sentences as a spec** — read the plan.

## The problem this cycle exists to fix

`TenantResolutionMiddleware` read `X-Tenant-Id` off the request and fed it straight into
`tenant_context()`. The only validation was *does this tenant exist and is it active* — **nothing
bound the claimed tenant to the authenticated caller**, so any client that could reach the service
could act as any active tenant. The trusted value already existed and was already parsed (a
`tenant_id` JWT claim in `AuthContext.metadata`); it was simply never connected to the routing
decision. The audit that followed found the same shape — a documented-but-unguarded insecure
default — repeated across the platform. **That class, not the single header bug, is what this
cycle addresses**, and it is why the extension rows below are also "a hole in something we already
ship" rather than new surface.

## Locked decisions (this session)

| Decision | Choice | Consequence |
|---|---|---|
| **Horizon** | **3.2, a dedicated security release** | Nothing is cut for size. Rows are ranked by how much of the hole they close, not by what fits |
| **Migration posture** | **Split by blast radius** | Defaults with a cheap caller-side fix (add an argument, drop a fallback) **flip** in 3.2. Defaults needing real application work (authorizer, tenant membership) get a **loud warn-only preflight** in 3.2 and flip in 4.0 |
| **Legacy path** | **`LegacyTenantSource` ships as a named, documented escape hatch** | Header-only resolution remains available for anyone not ready to move, with its security properties stated plainly. Same shape as the `varco_fastapi.auth.TrustStore` deprecation subclass |
| **Tenant sources in scope** | **JWT claim + subdomain + legacy header.** mTLS parked | The cross-check ("claim says A, host says B → reject") is the half with real security value, and it needs two sources to exist at all. `X-Forwarded-Client-Cert` trust is a separate problem nobody has asked for |
| **Membership model** | **Signed-claim membership list; no external lookup** | `TenantSource` resolves a *requested* tenant; it is checked against a `tenants`/`orgs` claim in the token. Trust stays anchored in the signature. A repository-backed resolver is an out-of-tree implementation of the same ABC |
| **Tenant-filter guarantee** | **Both, sequenced** | RLS-by-default (`S12`) is the proven production backstop and lands first. The applicator-level assertion (`S15`) is the portable dev-time guard and may slip. Shipping the assertion **alone** is rejected — it advertises a guarantee it cannot fully make |
| **Extension scope (2026-09-07)** | **"Finish the gaps", not a compliance-evidence cycle** | Every extension row plugs a verified hole in a shipped subsystem and adds no new architectural commitment. OCSF export, DSAR orchestration, signed audit export, and crypto agility were the alternative theme; all four are parked with triggers rather than half-shipped alongside |
| **Extension release target (2026-09-07)** | **3.2.0** | These rows are not a new cycle. 3.3.0 is reserved for the DI fix and must not absorb them |
| **User/organization model** | **varco still does not own one** | Decisive against the brief's two top-ranked candidates (WebAuthn/passkeys, SCIM 2.0): both require a user, credential, and org lifecycle varco has deliberately never had across ten packages. Shipping either means becoming Keycloak/Ory, not extending varco. Both parked, below |

## Live

Ordered by severity, then complexity ascending.

| ID | Feature | Severity | Complexity | Status | Rationale | Evidence |
|----|---------|----------|------------|--------|-----------|----------|
| — | **`MetricsMiddleware` records `status_code="500"` for exceptions that `ErrorMiddleware` renders as 4xx** | 🟡 should | S | Open | `ErrorMiddleware` is outside `MetricsMiddleware` and stays there, so any `ServiceException` reaching metrics is counted as a 500 while the client receives 404/409/422. Every error-rate dashboard over `http.server.request.duration` is wrong for mapped exceptions. Pre-existing; explicitly out of Plan 041's scope | `varco_fastapi/varco_fastapi/middleware/metrics.py:368,375-388`; `varco_fastapi/varco_fastapi/app.py:636-642` |
| — | **Verify `opentelemetry-exporter-prometheus` actually emits exemplars end-to-end** | 🟢 nice | S | Open | Plan 041 makes exemplars reachable at the SDK layer and proves it with `InMemoryMetricReader`. Whether the Prometheus exporter path (`observability/di.py:406-418` → `router/metrics.py`) translates them into OpenMetrics output is unverified — brief 012 has no evidence either way. If it does not, `router/metrics.py:45-51`'s promise is still only half true | `design/research/012-otel-exemplars-and-metrics-span-context.md` Evidence Gaps 2/3 |
| — | **Wire `inspect_jwks_posture()` into `SecurityPosture`** | 🟡 should | S | Open | Plan 041 exports the pure inspector; Plan 036 owns the harness (`varco_fastapi/varco_fastapi/posture.py:258`, collectors at `:400+`). Same split 039 §D-S20-posture used for `inspect_retention_posture()`; both should land in one harness pass rather than two | `varco_core/varco_core/authority/posture.py`; `plans/039-retention-and-purge-automation.md:567-568` |
| `CONF-COUNT` | **Strengthen `DeadLetterQueueConformance.test_count_reflects_pushed_entries`** — `after >= before` trivially holds at `KafkaDLQ.count()`'s constant `-1`, so the assertion cannot fail on a real regression. The live Kind-B example recorded by Plan 042 (`COVERAGE.md`'s findings register). Strengthening it may surface a Kind-A `KafkaDLQ` finding needing its own xfail + register row — that is the work, and why it was not done inline | 🟢 nice | S | Open | Plan 042 §D-kinds/Open questions Q1 | `varco_kafka/tests/test_kafka_conformance.py:80-88`; `varco_kafka/varco_kafka/dlq.py:544` |
| `CONF-RG` | **Automate the register↔marker cross-check** — a `scripts/` check asserting every `BUG:` xfail names a `KI-N` and every `KI-N` marker has a `COVERAGE.md` register row, wired into `make lint`'s no-`PKG` path beside `api-check`. Plan 042 Q2 left it manual deliberately | 🟢 nice | S | Open | Plan 042 Open questions Q2 | `testkit/varco_conformance/COVERAGE.md`'s **Conformance findings register** |
| `CONF-AUDIT6` | **Assertion-audit the six unaudited conformance suites** — Plan 042 audited `cache` and `dlq` only. `event_bus`, `job_store`, `channel_manager`, `idempotency_store`, `webhook_subscription`, `token_revocation` have not been checked for Kind-B gaps (assertions too weak to fail on a real violation) | 🟡 should | M | Open | Plan 042 Open questions Q3 | `testkit/varco_conformance/` (eight suite modules) |
| — | **Confirm Stripe's inbound HMAC secret encoding against a real delivery** — `StripeWebhookVerifier` pins `SecretEncoding.RAW_UTF8` (matching stripe-python's own behaviour) against brief 013 §2's conflicting "base64 secret" description, with no known-answer vector available to settle it (Plan 038 / S19, §D-S19-secretbytes) | 🟡 should | S | Open | Filed as a question with evidence, not a fix — the assumption is documented in the verifier's docstring and the feature doc; if wrong, the fix is one `SecretEncoding` enum value on one class | `varco_core/varco_core/webhook/inbound/verifiers.py`'s `StripeWebhookVerifier` docstring; `plans/038-inbound-webhook-verification.md`'s Risks table |
| — | **Should `DEFAULT_REDACT_PATTERNS` move to word-boundary matching in 4.0?** `"pin"` matches `shipping_address`/`mapping`/`typing`; `"auth"` matches `author`/`authority`. Harmless on developer-named span parameters, wrong on domain-named payload keys (Plan 040 / S21) | 🟡 should | S | Open | `plans/040-unified-redaction-seam.md` §D-S21-falsepos; `varco_core/varco_core/observability/params.py:307-309` (pre-Plan-040 location) | — |
| — | **Should `AuditLogMixin._audit_redactor` default to `PolicyRedactor()` in 4.0?** ⚠️ Conditional on the row above — flipping it under substring matching would blank `shipping_address` in every order audit (Plan 040 / S21) | 🟡 should | S | Open | `plans/040-unified-redaction-seam.md` §D-S21-audit; `varco_core/varco_core/service/audit.py` (diff assembly in `_after_create`/`_after_update`) | — |
| — | **Should `create_varco_app` gain a `log_redactor=` keyword?** Deliberately not added by Plan 040 to keep a zero diff in `app.py` while Plan 041 owns middleware ordering | 🟡 should | S | Open | `plans/040-unified-redaction-seam.md` §D-S21-logging | — |
| — | **Should `inspect_redaction_posture()` become a `SecurityPosture` collector?** Un-park when the audit-redaction default flips, so the finding means "opted out" rather than "not opted in" (Plan 040 / S21) | 🟡 should | S | Open | `plans/040-unified-redaction-seam.md` §D-S21-posture; `varco_fastapi/varco_fastapi/posture.py:485-573` | — |
| — | **DLQ and `WebhookDelivery` payload redaction** — the fifth and sixth redaction surfaces, both deliberately out of Plan 040 / S21's scope | 🟡 should | M | Open | `plans/040-unified-redaction-seam.md` §Parked | — |
| — | **No generic `JobDispatcher`** — varco has no poll loop that claims and executes durable PENDING jobs between restarts; `JobRunner.recover()` is startup-shaped and over-broad (sweeps every PENDING task-payload job, not just one subsystem's). Filed while building Plan 039's retention dispatch, which deliberately rides `recover()` rather than building this (§D-S20-dispatch) | 🟡 should | M | Open | Plan 039's §D-S20-dispatch "Rejected" list — a poll loop over `claim_next()` is a second execution path for jobs and belongs to Plan 005's subsystem, not a retention row | `plans/039-retention-and-purge-automation.md` §D-S20-dispatch |
| — | **`varco_core.schedule` still has no *general* driver** — Plan 032 shipped `ScheduleMaterializer` with no caller outside tests; Plan 039 closed this for retention specifically (`RetentionScheduler`), but an app using `varco_core.schedule` for its own (non-retention) work still has to write its own materialization loop | 🟡 should | S | Open | `plans/039-retention-and-purge-automation.md` §D-S20-driver "Rejected — put the loop in `varco_core.schedule.sweeper`" | `plans/039-retention-and-purge-automation.md` §D-S20-driver |
| — | **`varco_sa/varco_sa/migrations/versions/0007_schedules_table.py` does not create a `UNIQUE(schedule_id, run_at)` index** — `varco_core/varco_core/schedule/materializer.py:29`'s DESIGN block cites this index as (half of) the cross-process double-materialization backstop, but the `schedules` table has no `run_at` column at all (only `Job` rows do, and `job_id` there is already deterministic — see below), so the claimed index cannot exist as described. Verified absent by Plan 039 / Step 12, not added there (an index on a shipped framework table is Plan 032's business, not a retention row's) | 🟡 should | S | Open | The real cross-process backstop already holds without it: `Job.job_id = uuid5(NAMESPACE_URL, f"varco:schedule:{schedule_id}:{wall.isoformat()}")` (`materializer.py:76-78`) makes two materializers computing the same occurrence converge on one physical row via `AbstractJobStore.save()`'s upsert semantics — the missing index would only add a second, redundant enforcement layer | `varco_sa/varco_sa/migrations/versions/0007_schedules_table.py`; `varco_sa/varco_sa/schedule.py:11-19`'s own docstring, which already documents this as a deliberate absence for a different reason (no `run_at` column on this table) |

## Shipped this cycle

`S1`–`S16`, all five plans, **verified present in source 2026-09-07 and 2026-09-12** (not merely
marked done). Detail lives in the plan files, `CHANGELOG.md`, and `technical_docs/features/`.

- `S1` `S2` `S13` `S14` — credential & token lifecycle: required `algorithms=`, `?api_key=`/`?token=` off by default, `varco_core.revocation` + Redis backend, hashed API keys (plan: `plans/034`)
- `S3` `S7` `S8` `S10` — HTTP edge: error-leak fix, security headers, body limits, rate-limit middleware, `inspect_http_edge()` (plan: `plans/035`)
- `S5` `S6` `S16` — tenant identity: `TenantSourceChain`, `AbstractTenantMembership`, RFC 8693 act-as, `inspect_tenant_provenance()` (plan: `plans/033`)
- `S4` `S9` `S11` — authorization surface: admin cross-tenant guards, `SecurityPostureLifecycle`, `AuditingAuthorizer` (plan: `plans/036`)
- `S12` `S15` — data-layer enforcement: RLS-by-default + `rls_autogen` + `install_rls_tenant_hook`, applicator tenant-filter assertion (plan: `plans/037`)
- `S18` — answered, not built: `extra_middleware=` lands **outside** `ErrorMiddleware` and `RequestContextMiddleware`; the three new middlewares got dedicated `create_varco_app` keywords instead, and the misleading comment was corrected. Now a standing rule in CLAUDE.md
- `S20` — retention & purge automation: `varco_core.retention` (`RetentionPolicy`/`RetentionRegistry`, `RetentionTarget` + six adapters, `RetentionScheduler`, `inspect_retention_posture()`, `bind_retention_registry`, `install_retention_metrics`), `varco_fastapi.RetentionLifecycle` + `create_varco_app(retention=...)`, `varco retention --policy`/`list`, plus the three Plan-032-driver fixes this needed (`Schedule.task_name`, `JobRunner.recover()` honouring `run_at`, `ScheduleMaterializer` finally has a caller) (plan: `plans/039`)
- `S17` — `MetricsMiddleware` moved inside `TracingMiddleware`; exemplars now reachable (plan: `plans/041`)
- `S22` — JWKS background refresh: `TrustedIssuerRegistry.start_refresh()`/`stop_refresh()`, `varco_fastapi.JwksRefreshLifecycle` + `create_varco_app(jwks_refresh=...)`, `inspect_jwks_posture()` (plan: `plans/041`)
- `S19` — inbound webhook signature verification: `varco_core.webhook.inbound` (`WebhookVerifier` ABC, four provider adapters, `get_verifier()`, `WebhookReplayGuard`) + `varco_fastapi.webhook.verify_webhook` route dependency (plan: `plans/038`)
- `S21` — unified redaction seam: `varco_core.redaction` (`Redactor`/`PolicyRedactor`/`redact_mapping()`/`inspect_redaction_posture()`) behind spans, `error_params()`, and the audit trail (plan: `plans/040`)
- `S23` — conformance-guard recovery: the five lost findings (`KI-2`, `KI-3`, `KI-5`, `KI-6`, `KI-7`) confirmed fixed/worked-around and recorded in `testkit/varco_conformance/COVERAGE.md`'s Conformance findings register, Docker-free regression tests added for `KI-2`/`KI-7`, CLAUDE.md's dangling pointer repointed (plan: `plans/042`)

## Verified as already sound (do not re-raise)

| Area | Finding |
|---|---|
| **CORS defaults** | Hardened in 3.0.0 (Plan 022 / AB-5). `allow_origins` defaults to `()`, not `("*",)` |
| **SSRF guard** | The five-layer model in `varco_core/webhook/ssrf.py` is genuinely solid — scheme allowlist, resolve-once-and-pin, CIDR deny, no redirect following, IPv6 equivalents. Its **only** weakness is that nothing asserts callers actually use the returned `pinned_ip`; fold that assertion into `S9`'s preflight rather than opening a row |
| **Cache-key tenancy** | `tenancy_cache_key()` already fails closed with `RuntimeError` outside `tenant_context()`. This is the house pattern the rest of the cycle is being held to |
| **Admin mount acknowledgement** | Three independent barriers per mount (`acknowledge_*` kwarg, `server_auth`, no env-var path). The gap is the `server_auth=None` warn-and-mount default, covered by `S9` — not the acknowledgement design |

## Parked — this cycle (do not relitigate without new evidence)

| Item | Why parked | Un-park trigger |
|---|---|---|
| **mTLS / `X-Forwarded-Client-Cert` `TenantSource`** | Scoped out at the interview. Client-cert trust across a proxy hop is Envoy-specific and is its own trust problem, not a variation on the others | varco commits to service-mesh deployment guidance, or a consumer asks. The ABC is designed so this is an additive out-of-tree implementation |
| **Repository-backed `AbstractTenantMembership`** | The signed-claim default covers the real multi-org case without a per-request query. Shipping both invites apps to pick the slower one by default | Memberships that genuinely cannot fit in a token. It is an out-of-tree implementation of the shipped ABC, so waiting costs nothing |
| **DPoP / sender-constrained tokens (RFC 9449)** | Published March 2024, but the brief finds adoption early and concentrated in FAPI 2.0 / open banking. Real, but not 3.2-shaped | Broad library support in the Python OAuth ecosystem, or a consumer in a FAPI-regulated context |
| **CAEP / SSF real-time session revocation** | Google Workspace beta; slow adoption outside enterprise. `S13`'s revocation seam is the useful, standard-independent half | The seam from `S13` exists and a consumer needs cross-provider session revocation. Then CAEP is a provider against it |
| **SPIFFE / workload identity** | Production at scale (Stripe, Netflix, Uber) but Kubernetes-first, and it answers *service* identity rather than *tenant* identity — a neighbouring problem | varco grows an opinion about service-to-service identity; likely alongside `S16` |
| **`RateLimit`/`X-RateLimit-Remaining` headers** (Plan 035 / S10, §D-S10-headers) | The `RateLimiter` ABC cannot report remaining quota, and adding one would break every out-of-tree implementation (the `BulkCache`-off-`AsyncCache` rule) | The design is written: an optional `@runtime_checkable RateLimitIntrospection` Protocol with `remaining(key) -> int`. Un-park when `draft-ietf-httpapi-ratelimit-headers` becomes an RFC |
| **A bounded/LRU key space on `InMemoryRateLimiter`** (Plan 035 / S10, §D-S10-keyspace) | The right long-term fix for an IP/subject-keyed limiter's attacker-controlled key space, but it changes a public `varco_core` class's memory semantics for every existing `@rate_limit` caller | A consumer reports memory growth, or the acknowledgement kwarg proves to be the common path rather than the exception |
| **Token-bucket rate limiter** (Plan 035 / S10, §D-S10-algorithm) | varco already ships two sliding-window implementations (in-memory + Redis sorted-set); a third algorithm is its own row | Burst intolerance is reported as a real problem by a consumer |
| **JSON nesting/complexity limits** (Plan 035 / S8) | Brief 008 §2: no framework middleware can enforce nesting depth without the schema; it belongs in Pydantic validators, not this row (S8 is renamed "Request body size limits" accordingly) | A standardized middleware-level approach appears, or varco grows a schema-aware deserialization layer |
| **Per-route body ceilings and per-route CSP** (Plan 035 / S7, S8) | `exempt_paths` covers the real cases without a decorator API; no authoritative per-route-CSP-in-FastAPI pattern exists (brief 008 Evidence Gap 1) | Two consumers need genuinely different ceilings/CSP on two routes of one app |
| **`SATokenRevocationStore` / `BeanieTokenRevocationStore`** (Plan 034 / S13, §D-S13-backends) | Brief 009 §5's TTL model is native to Redis and would need a hand-rolled sweep job in SQL/Mongo — both are additive out-of-tree implementations of the shipped `AbstractTokenRevocationStore` ABC | A consumer needs a revocation denylist that survives a cache flush, or `delete_expired()` needs to be driven by the existing `AbstractJobRunner` |
| **`RevocationFailureMode.FAIL_OPEN_WITHIN_GRACE`** (Plan 034 / S13, §D-S13-fail, brief 009 §4 row 3) | Brief 009's own Evidence Gap 2: the cache-miss-rate/Redis-latency data that would size the grace window does not exist — a tunable whose only honest default is a guess is worse than shipping two modes that mean exactly what they say | A performance brief measures cache-miss rates and Redis latency at realistic QPS, per that evidence gap's own suggestion |
| **`TenantFanoutSupervisor`-backed per-tenant retention** (Plan 039 / S20, §D-S20-tenancy) | A per-tenant `RetentionPolicy` loops `tenant_context(tid)` sequentially today — correct, but a 5 000-tenant fleet makes that a 5 000-iteration loop per occurrence. `TenantFanoutSupervisor` (`varco_core/varco_core/tenancy/fanout.py:36-44`) is the shipped fan-out primitive; wiring retention through it is additive | A consumer has >100 tenants with per-tenant retention policies |
| **`SecurityPosture` harness wiring for `inspect_retention_posture()`** (Plan 039 / S20, §D-S20-posture) | The pure inspector ships; Plan 036's `SecurityPosture` harness (which aggregates every sibling plan's own exported inspector) does not yet call it | Plan 036's harness gains a registration seam |

| **OCSF / ECS audit-log export** (discover 2026-09-07, brief 011 §B) | ⭐ The strongest of the four compliance-evidence candidates, and cut only because the interview chose "finish the gaps". varco owns the audit trail and `AuthorizationDecisionEvent` already; what is missing is a second serializer emitting Open Cybersecurity Schema Framework shape so a buyer's SIEM (Splunk/Elastic/Datadog/Security Hub) ingests it natively. Same "second `Serializer`, no change to the event type" shape that carried CloudEvents in 3.1 | A consumer needs SIEM ingestion, **or** OCSF ratification completes (ITU support Dec 2025, ratification target Jun 2026 — check the ITU/OCSF status before un-parking). ⚠️ Do not un-park on the NIS2 Jun 2026 date alone: that is a deadline for varco's *users*, not for varco |
| **DSAR / right-to-erasure orchestration** (discover 2026-09-07, brief 011 §B) | The genuine differentiator on varco's stated regulated-multitenant axis, and the only candidate with **no prior art to copy** — brief 011 §B finds no vendor SDK, only manual workflows (Osano/TrustArc/OneTrust). Every primitive is shipped — `destroy_scope()` crypto-shredding, `SoftDeleteService`, tenancy, the audit trail as proof-of-execution — and nothing composes them into GDPR Art. 15 collect / Art. 17 erase. Parked on size (L) and on the same rule that keeps `S15` 🟢: a half-shipped erasure guarantee is worse than none | A consumer with a real DSAR obligation, **or** an appetite for an L row in a cycle that is not maintenance-shaped. It needs its own cycle, not a slot in this one |
| **Signed / externally-verifiable audit export** (discover 2026-09-07, brief 011 §B) | `verify_chain()` proves the hash chain's integrity **inside the database** — which is exactly the party an auditor does not trust. A detached signature or Merkle inclusion proof over an *export* is the half that has evidentiary value. Small and natural, but it pairs with OCSF export and is pointless shipped alone | Un-park **together with** OCSF export. Independently: an auditor or consumer asking for non-repudiable log export, or Sigstore Rekor reaching a stable Python client |
| **Crypto agility (algorithm rotation) for field encryption** (discover 2026-09-07, brief 011 §D) | varco ships **key** rotation (`MultiKeyEncryptorRegistry`, `MultiKeyAuthority`) but not **algorithm** rotation — the cipher is welded into `FernetFieldEncryptor`. An algorithm identifier in the ciphertext envelope plus an algorithm registry would make a future cipher swap a rotation rather than a data migration. This is the cheap, honest half of PQC readiness. Parked because the deadline that motivates it is far off (NIST IR 8547 deprecates RSA/ECC by 2030; federal migration 2030–2031) and brief 011 Evidence Gap 7 finds **no production data** on anyone actually implementing algorithm rotation | A PQC or FIPS requirement from a real consumer, **or** a cipher varco ships being deprecated. ⚠️ Un-park **before** the need, not after: the envelope format change is cheap on an empty field and expensive on a populated one |
| **WebAuthn / passkeys server-side** (discover 2026-09-07, brief 011 §A) | Brief 011's top-ranked candidate and **rejected on architecture, not merit.** WebAuthn needs a user record, a credential table, ceremony state, and a user handle — varco owns **none** of these across ten packages, by long-standing design. Shipping it means becoming Keycloak/Ory. ⚠️ Also note the brief's own Evidence Gap 8: it could not name a production-grade Python server-side library | varco decides to own a user/credential model — a decision far larger than this row |
| **SCIM 2.0 provisioning server** (discover 2026-09-07, brief 011 §A, §C) | Same rejection as WebAuthn, same reason: SCIM's `/scim/v2/Users` and `/scim/v2/Groups` presuppose a user and group model varco does not have, and PATCH semantics (RFC 7644) are a substantial surface for something varco would then have to keep. Table stakes for a *product*; not for a framework with no identity store | varco decides to own a user/organization model |
| **OSCAL control-evidence automation** (discover 2026-09-07, brief 011 §B) | Very high effort (OSCAL schema, control registry, evidence extractor) for a federal/FedRAMP-niche audience varco has no evidence of serving. The FedRAMP CR26 OSCAL mandate (Sept 30, 2026) binds *authorization packages*, not libraries | A consumer pursuing FedRAMP authorization on varco |
| **Post-quantum algorithms (FIPS 203/204/205)** (discover 2026-09-07, brief 011 §D) | Not actionable for an application framework in 2026 — the substrate (Python PQC libraries, PQC TLS, cloud PQC cert issuance) is not there, and brief 011 Evidence Gap 6 could not establish any commercial-SaaS deadline earlier than the federal 2030–2031 one. The useful, shippable half is the crypto-agility row above | Federal-adjacent consumer demand, or a maintained Python PQC binding varco could depend on. The agility row is the prerequisite either way |
| **Ed25519 / asymmetric Standard Webhooks verification** (`whsk_`/`whpk_`, Plan 038 / S19) | Needs a crypto dependency `varco_core` does not have | A consumer receives from an asymmetric-mode sender, or `varco_core` gains a crypto dependency for another reason |
| **An inbound `Rfc9421Verifier`** (Plan 038 / S19) | `Rfc9421Signer.verify()` already takes `bytes` + the request line and is inbound-shaped; wiring it behind the `WebhookVerifier` ABC is additive. No provider in brief 013 §2 uses it | A consumer receives RFC 9421-signed webhooks |
| **Scoped delegation + per-route limits for the A2A/MCP surface** (discover 2026-09-07, brief 011 §E) | Brief 011 §E finds varco's agent-facing security posture already strong — A2A passes `ctx` (the U-3 auth-passthrough contract), authorization is enforced at the service layer, and `S16`'s act-as delegation shipped. The two named gaps are a `scope=` narrowing on delegated tokens and per-route rate limits. ⚠️ The second is **already parked independently** ("Per-route body ceilings and per-route CSP") on the same reasoning | Two consumers needing genuinely different limits per route, **or** an agent deployment where an over-broad delegated token is a demonstrated problem rather than a theoretical one |

## Open questions — all answered (do not relitigate)

1. ✅ **ANSWERED** in [`plans/036`](plans/036-authorization-surface-and-posture.md) §D-S9-oq1 —
   **an explicit `VARCO_SECURITY_ENV`, defaulting to `production`, governing severity presentation
   only.** Inference is rejected on a concrete failure mode: every signal varco could infer from is
   itself one of the things the preflight checks, so the worst-configured deployment would be the
   one that decides it is not production. §D-S9-flip carries the consolidated 4.0 flip list.
   **What is `SecurityPosture`'s "production" signal?** An explicit `VARCO_ENV`/`VARCO_SECURITY_STRICT`
   setting, or inference from other configuration? Inference is convenient and gets it wrong at the
   worst moment; an explicit flag is honest but must default to strict or nobody sets it.
2. ✅ **ANSWERED** in [`plans/033`](plans/033-tenant-identity-provenance.md) §D-S6-oq2 —
   **agreement-by-default (`LENIENT`)**, with a `STRICT` mode defined as *"if any source spoke, at
   least two must agree"*, so zero-claim requests stay exempt with no path allowlist.
   **Does `S6`'s cross-check fail-closed when only one source is present?** A request carrying a
   JWT claim but no matching subdomain: is that agreement-by-default or a rejection? Leaning
   agreement-by-default (a missing source is not a conflicting source), with a strict mode
   available — but this is the decision most likely to break a real deployment quietly.
3. ✅ **ANSWERED** in [`plans/033`](plans/033-tenant-identity-provenance.md) §D-S6-oq3 —
   **explicit `base_domains`, no `publicsuffix2` dependency**; a PSL is a staleable data file
   whose failure mode is tenant confusion, and `varco_core`'s zero-new-runtime-dependency rule is
   decisive. `X-Forwarded-Host` is off by default.
   **Subdomain parsing and the public-suffix trap.** `S6` needs a base-domain configuration.
   Deriving the tenant from `tenant.example.com` requires knowing where the registrable domain
   ends; getting this wrong on a multi-level TLD is a tenant-confusion bug. Explicit base-domain
   config avoids a `publicsuffix2` dependency — confirm that is acceptable.
**4.0 flip list (Plan 033 / S6, S5)** — recorded here, not relitigated, per the same
answered-not-deleted style as open question 1's `§D-S9-flip` pointer:

| # | 4.0 change | 3.2 signal |
|---|---|---|
| 1 | `TenantResolutionMiddleware(chain=None)` → `TypeError`; the implicit `LegacyTenantSource` fallback is removed. Conditional on `S16` having shipped first — see `S16`'s row above | `DeprecationWarning` at construction + posture finding `tenant.legacy_source_implicit` |
| 2 | `ClaimTenantMembership.on_missing_claim` default `ALLOW` → `DENY` | one `WARNING` per process + `reason="claim_absent"` on every decision |
| 3 | `RequestContextMiddleware.enable_tenant_context` defaults `True` → `False`; the unchained claim-tenant-setter path is removed | posture finding `tenant.unchained_claim_tenant_setter` |
| 4 | `CrossCheckMode.STRICT` becomes the default | posture finding `tenant.cross_check_lenient` |

Full reasoning: `plans/033-tenant-identity-provenance.md` §D-S6-blast;
`technical_docs/features/tenant-provenance.md`'s own flip-list table.

4. ✅ **ANSWERED** in [`plans/037`](plans/037-data-layer-tenant-enforcement.md) §D-S12-oq4.
   **Does `S12` (RLS-by-default) change existing generated DDL?** **No** — RLS is structurally
   additive, no existing DDL is rewritten. But the migration story is **still mandatory**, because
   policies must be created *before* `ENABLE ROW LEVEL SECURITY` or the table becomes default-deny
   and goes dark (research 007). The plan also records that today's emission order is wrong.

---

# Completed cycles — summary

Detail lives in `plans/`, `CHANGELOG.md`, and git history. These are one-paragraph records so a
reader knows what happened without a second copy of it drifting here.

### 3.1 — API surface & interop (discover, 2026-09-04) — ✅ complete

Shipped `D1` Idempotency-Key middleware (🔴), `N1` MCP v2 migration (🔴), `N2` CloudEvents
envelope, `N3` AsyncAPI export, `D4` outbound webhooks, `D5` CycloneDX SBOM + regulatory posture,
and the 🟢 row group `D6` recurring schedules / `D7` feature-flag seam / `D8` `varco-testkit`.
Plans 029–032. Backed by research briefs 001, 002, and 004.

### 3.1 — trust store, hot reload & performance — ✅ complete

Shipped `T3`/`T5`/`T7` (`varco_core.tls` unification, `ReloadingTrustStore`, client injection and
mTLS hardening), `T4`/`T6` (the four client adapters, `install_process_trust()`, PKCS#12 and
encrypted-key support), and `P1`–`P4` (PEP 562 lazy `varco_core` import — 289.6 ms → 6.6 ms —
the `scripts/import_budget.py` harness, and the CodSpeed benchmark suite). Plans 025–028.

### 3.0.1 — cleanup cycle — ✅ complete

Shipped `C1` (backlog/source reconciliation and the branch-protection ruleset), `C2` (providify
`@Disposes` adoption, closing `P22-PROVIDER-PREDESTROY`), `C5` (`api_surface.py --check` promoted
to a real gate), `C7` (`testkit/varco_conformance/COVERAGE.md`), and `C8` (the Kafka chaos
restart flake, time-boxed then documented). Plan 024.

### 3.0.0 — release cycle — ✅ complete

The API freeze and first public release: providify 2.0.0 adoption, CI green across a 3.12/3.13
matrix, the reliability floor (conformance suites, integration and chaos tests in CI), the API
freeze itself (`scripts/api_surface.py`, reserved seams, the `AB-1`/`AB-2`/`AB-3`/`AB-5` break
decisions), and the ten-package lockstep release machinery (`scripts/bump.py`, `release.yml` with
PyPI trusted publishing and PEP 740 attestations, `scorecard.yml`). Plans 012–023.

---

# Standing parks (do not relitigate without new evidence)

Consolidated from every cycle. A row leaves this table only when its trigger fires.

| Item | Why parked | Un-park trigger |
|---|---|---|
| **Durable execution / workflow-as-code** | **XL, parked on size alone — not on merit.** Brief 001 names it varco's largest strategic gap, and varco owns unusual amounts of the substrate (fenced-lease job store, outbox, saga orchestrator, DLQ). Half of it would be worse than none | A major-version horizon (4.0) with the appetite to build it properly, or a consumer requirement that makes the saga orchestrator's limits concrete rather than theoretical |
| **OpenFeature provider** | Trigger checked **2026-09-04** (Plan 032 / D7, brief 004 §1) — **NOT FIRED**. `openfeature-sdk` at **0.10.0** (2026-06-01), spec at **0.9.0** (2026-07-29); 0.10.0 itself shipped a breaking change (`set_provider()` no longer blocks) inside a minor bump. **Outcome: the seam shipped, the provider did not** — `varco_core.flags` is a varco-shaped ABC, deliberately not a transcription of OpenFeature's | `openfeature-sdk` reaches **1.0.0** — the SDK, not the spec. The adapter is then purely additive |
| **RRULE / RFC 5545 schedules** | A complete implementation means a `dateutil` runtime dependency, against the standing zero-new-runtime-dependencies rule for `varco_core` | A consumer needs a recurrence 5-field cron cannot express, and accepts the dependency |
| **Seconds-precision / 6-field cron** | 5-field covers the case; a scheduler with second granularity is a different tool | A consumer needs sub-minute scheduling |
| **Packaging `varco_chaos`** | Unstable by design, wraps testcontainers, needs Docker, and is the only sanctioned caller of `get_wrapped_container()`. Freezing it under the API gate buys nothing | A downstream writes chaos tests against varco backends and asks |
| **Per-tenant quotas & usage metering** | Real — multitenancy is a varco flagship and quotas are its missing half — but M-sized new surface, cut for scope | A consumer asking for metered billing events off the existing rate limiter. ⚠️ Note `S10` builds the per-tenant rate-limit middleware this would extend |
| **Secrets-manager sources (Vault / cloud KMS)** | The technical argument is good — credential rotation is the same problem as cert rotation, and Plans 025–027 built the machinery — but it is an extension, not a gap. ⚠️ **Re-examined 2026-09-07** (discover): the scout found the master key for field encryption is loaded from app config with comments pointing at Vault/KMS and **no seam to do it** — `EncryptionKeyStore` stores DEKs, not KEKs. So the shape this park would take is now specific: an `AbstractKeyProvider` for envelope encryption (KEK in Vault/KMS, DEK local via the shipped store). Still parked, but no longer vague | JWT-signing-key rotation becoming a concrete need rather than an analogy, **or** a consumer required to hold the KEK in an HSM/KMS rather than an environment variable |
| **Server-side cert rotation via `sni_callback`** | The 3.1 cycle was scoped to **outbound** calls; this was its only L item and dropping it made the cycle fit | varco services begin terminating TLS directly rather than sitting behind a proxy/ingress |
| **`truststore` dependency** | **Investigated and rejected on source evidence.** It *does* support custom CAs (the objection was half wrong), but **on Linux its verifier is a documented no-op** — OpenSSL's default paths already *are* the Linux system store. Zero behavioural gain for a dependency | varco officially supports macOS or Windows, where `create_default_context()` cannot see Keychain / CryptoAPI and an MDM-pushed corporate root is invisible |
| **Cross-platform (macOS / Windows) support** | Scoping to Linux keeps platform caveats out of the TLS design instead of scattering them through it | A release commits to multi-OS support — at which point the work is **not** just adding `truststore` but auditing every implementation for Linux-only assumptions (inotify, path handling, `SSL_CERT_DIR`, `StatPollWatcher` mtime granularity) |
| **PEP 810 native lazy imports** | Lands in Python 3.15; the matrix is 3.12/3.13. PEP 562 ships today and is what `varco_core` uses | The support matrix reaches 3.15 |
| **Toxiproxy graded latency/bandwidth chaos** | `testcontainers-python` ships no Toxiproxy module, and the standalone Python client is 0.x with no recent activity (possibly orphaned) | A `testcontainers.toxiproxy` module, or a maintained Python client |
| **Integration tests gating PRs (RL-16)** | Deliberate. Promotion needs **≥30 consecutive nightly runs with ≤1 non-code failure** — below 30 there is no measurement, only anecdote. ⚠️ Independently blocked by `cancel-in-progress: true`: a cancelled run resolves as neither success nor failure, so a required check that can be cancelled leaves a PR permanently pending (research 001 §8) | Reaching that count **and** resolving the concurrency interaction. The `chaos` job is **never** a promotion candidate, on any schedule |
| **GraphQL surface · event sourcing** | Named by research as absent vs comparable frameworks, but neither is on the axis varco competes on (multitenancy isolation, field-level encryption / crypto-shredding, audit trails for regulated workloads) | Concrete user demand |
| **Independent per-package versioning** | Rejected in favour of lockstep. A lockstep release gives users one number to trust | Release churn from ten-package bumps becoming a measured cost |
| **`varco` umbrella meta-package with pinned extras** | The most machinery for the least benefit under lockstep versioning | Lockstep versioning being abandoned |
| **New dedicated e2e reference application** | No comparable reliability-focused framework uses a monolithic reference app as its primary regression strategy; per-feature chaos tests score better, and an existing example covers the cross-feature case | — |
| **WD-1 — WS backpressure margin** | A watch item, not work. The margin is machine-dependent | The test failing **twice on CI**; then thread `ws_max_queue`/`write_limit` through the fixture |
| **RT4-ws-scale — many-connection WS scale test** | Blocked on undocumented GitHub Actions fd limits | Documented limits, or a measured local ceiling worth encoding |

---

# Answered decisions (do not relitigate)

- **RL-1 — providify un-vendoring sequence** (Plan 016) → **two-step, one branch, two commits**:
  un-vendor against 1.1.0 first, full sweep, *then* bump to 2.0.0 and sweep again. A red sweep at
  step one means "PyPI's artifact ≠ the vendored local build"; at step two, "2.0.0 changed
  behaviour" — two different fixes, and a conflated failure would have been expensive across ten
  packages.
- **RL-3 — providify pytest plugin adoption** (Plan 016) → **document, do not wrap.** The four
  fixtures are used under providify's own names; `testkit` deliberately does not re-export them,
  since it is never packaged and a second name for an identical fixture is pure confusion. A
  consumer conftest redefining `di_container` wins over the plugin default.
- **RL-9 — version bump mechanism** (Plan 023) → **a hand-rolled `scripts/bump.py` using
  tomlkit**, not `uv version` (no `--all-members`, and it cannot rewrite sibling requirement
  strings) and not hatch-vcs (unsuitable for a hand-chosen, not CI-derived, version).
- **RL-9 — sibling pin exactness** (Plan 023) → **compatible (`~=<major>.0`), never exact.** Exact
  pins force the resolver to reconcile two different exact `varco-core` demands the moment two
  siblings differ by a patch — a diamond conflict this monorepo would hit on its first post-3.0.0
  patch release. The lockstep guarantee is carried by the *release process*, not the metadata.
- **T3 — deprecation shim shape** (Plan 026) → **a subclass, not an alias.** A plain alias was
  never available, because the old and new names do not denote the same behaviour — the new type
  is recursive by default and globs a wider cert set, so aliasing would silently widen every
  existing construction on upgrade. The resulting `isinstance` asymmetry is a documented,
  CHANGELOG'd cost of the deprecation window.
- **T3 — does `SSLConfig` gain reload?** (Plan 026) → **no.** `SSLConfig` stays frozen and static;
  `ReloadingTrustStore` is the only reloadable path. Making `SSLConfig` reloadable would turn every
  settings object constructed at import/DI time into an unmanaged background-task owner.
- **T3 — where the reload task starts** (Plan 026) → **the store owns `start()`/`stop()` itself;
  no `@Configuration` is added to `varco_core`.** `container.scan("varco_core", recursive=True)`
  is a documented, in-use pattern that auto-activates every scanned `@Configuration`, which would
  start a filesystem watcher in every app that scans `varco_core`.
- **C2 — providify 2.0.1 gate** (Plan 024) → **removed, not rescheduled.** 2.0.1 shipped without
  the fix and the behaviour is declared intentional, so varco adopted `@Disposes` itself.
- **RT7 — chaos test shape and CI placement** (Plan 018) → chaos runs as its own job in
  `integration.yml`, nightly + `workflow_dispatch` only, **never** a required check.
- **Standards alignment park — reversed** (Plan 022) → the park protecting the 3.0.0 freeze from
  CloudEvents/AsyncAPI was lifted on the merits: neither changes the public surface, so neither
  consumed breaking-change budget. What the window owed them was a written seam reservation
  (`design/api-freeze-and-standards/reserved-seams.md`). Both shipped in 3.1.
