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

# 3.2 — security release (discover, 2026-09-05)

Produced by `/discover` with a focus on tenant-identity trust. **This cycle is a dedicated
security release** — its rows are not competing with feature work, and none of them were cut for
size.

**Research brief backing this cycle:**
[`design/research/006-multi-tenant-identity-and-hardening.md`](design/research/006-multi-tenant-identity-and-hardening.md)

Three further briefs were written during planning, one per hardening theme:
[`007-postgres-rls-enforcement-mechanics.md`](design/research/007-postgres-rls-enforcement-mechanics.md) (S12/S15),
[`008-http-hardening-conventions-2026.md`](design/research/008-http-hardening-conventions-2026.md) (S7/S8/S10),
[`009-token-revocation-and-credential-storage.md`](design/research/009-token-revocation-and-credential-storage.md) (S13/S14).

**Plan split:** these 16 rows are carved into **five plans** —
see [`plans/000-index-3-2-security-release.md`](plans/000-index-3-2-security-release.md) for the
slice boundaries, dependency edges, build order, and the definition of done for the whole set.
**All five plans are now written**, so every row below is `✅ planned`. The rows stay in this
table — they are *planned*, not *shipped*; they fold into `# Completed cycles — summary` only
once the code lands. Build order: **035 → 034 → 033 → 037 → 036** (036 last by design).

## The problem this cycle exists to fix

`TenantResolutionMiddleware` reads `X-Tenant-Id` off the request and feeds it straight into
`tenant_context()` (`varco_fastapi/varco_fastapi/middleware/tenant_resolution.py:61`). The only
validation is *does this tenant exist and is it active*. **Nothing binds the claimed tenant to the
authenticated caller** — any client that can reach the service can act as any active tenant.

The trusted value already exists and is already parsed: a `tenant_id` JWT claim lands in
`AuthContext.metadata` (`varco_core/varco_core/jwt/parser.py:384`). It is simply never connected to
the routing decision.

The audit that followed found the same shape — a documented-but-unguarded insecure default —
repeated across the platform. That class, not the single header bug, is what this cycle addresses.

## Locked decisions (this session)

| Decision | Choice | Consequence |
|---|---|---|
| **Horizon** | **3.2, a dedicated security release** | Nothing is cut for size. Rows are ranked by how much of the hole they close, not by what fits |
| **Migration posture** | **Split by blast radius** | Defaults with a cheap caller-side fix (add an argument, drop a fallback) **flip** in 3.2. Defaults needing real application work (authorizer, tenant membership) get a **loud warn-only preflight** in 3.2 and flip in 4.0 |
| **Legacy path** | **`LegacyTenantSource` ships as a named, documented escape hatch** | Header-only resolution remains available for anyone not ready to move, with its security properties stated plainly. Same shape as the `varco_fastapi.auth.TrustStore` deprecation subclass |
| **Tenant sources in scope** | **JWT claim + subdomain + legacy header.** mTLS parked | The cross-check ("claim says A, host says B → reject") is the half with real security value, and it needs two sources to exist at all. `X-Forwarded-Client-Cert` trust is a separate problem nobody has asked for |
| **Membership model** | **Signed-claim membership list; no external lookup** | `TenantSource` resolves a *requested* tenant; it is checked against a `tenants`/`orgs` claim in the token. Trust stays anchored in the signature. A repository-backed resolver is an out-of-tree implementation of the same ABC |
| **Tenant-filter guarantee** | **Both, sequenced** | RLS-by-default (`S12`) is the proven production backstop and lands first. The applicator-level assertion (`S15`) is the portable dev-time guard and may slip. Shipping the assertion **alone** is rejected — it advertises a guarantee it cannot fully make |

## The work

Ordered by severity, then complexity ascending.

| ID | Feature | Severity | Complexity | Rationale | Evidence |
|----|---------|----------|------------|-----------|----------|
| `S1` | ✅ **planned → [`plans/034-credential-and-token-lifecycle.md`](plans/034-credential-and-token-lifecycle.md)** — **Require `algorithms=` on `JwtParser.parse()`** — no silent default | 🔴 must | S | `parser.py:137-139` defaults to `["HS256"]` when the caller passes nothing, with a comment reading "always pass algorithms explicitly in production". An HMAC default reachable by an unaware caller is the classic algorithm-confusion setup. Making the argument required is a one-line caller fix, so it flips under the blast-radius rule | `varco_core/varco_core/jwt/parser.py:137` |
| `S2` | ✅ **planned → [`plans/034-credential-and-token-lifecycle.md`](plans/034-credential-and-token-lifecycle.md)** — **`?api_key=` query fallback off by default** | 🔴 must | S | `ApiKeyAuth` accepts the key as a query parameter with no warning and no toggle (`server_auth.py:358`), so keys land in access logs, proxy logs, and `Referer` headers. `WebSocketAuth`'s `?token=` fallback (`:626`) at least warns — align both on off-by-default with explicit opt-in | brief §5 (input handling); `varco_fastapi/varco_fastapi/auth/server_auth.py:358` |
| `S3` | ✅ **planned → [`plans/035-http-edge-hardening.md`](plans/035-http-edge-hardening.md)** — **Close the error-response information leak** | 🔴 must | S | `error.py:283` — when `error_message_for()` cannot map an exception, the fallback returns `str(exc)` to the client. For an unmapped `DBAPIError` or `OSError` that is a schema fragment or a filesystem path. Return an opaque message plus the existing `correlation_id`; log the detail server-side | `varco_fastapi/varco_fastapi/middleware/error.py:283` |
| `S4` | ✅ **planned → [`plans/036-authorization-surface-and-posture.md`](plans/036-authorization-surface-and-posture.md)** — **Cross-tenant write guard on the three admin surfaces** | 🔴 must | S | `webhook/router.py:134` trusts `X-Tenant-Id` for reads *independently of the middleware*, and `create_subscription` takes `tenant_id` from the **request body**, unchecked — a direct cross-tenant write. Bind admin routes to the resolved tenant unless the caller holds an explicit cross-tenant role. Applies to all three `mount_*` surfaces | brief §2 (BOLA); `varco_fastapi/varco_fastapi/webhook/router.py:134-160` |
| `S5` | ✅ **planned → [`plans/033-tenant-identity-provenance.md`](plans/033-tenant-identity-provenance.md)** — **Tenant↔subject membership binding** — `AbstractTenantMembership`, signed-claim default | 🔴 must | M | Stops a *legitimately authenticated* user of tenant A acting as tenant B. Default implementation checks the requested tenant against a `tenants`/`orgs` list claim — no external lookup, no per-request query, trust anchored in the signature. Depends on `S6` | brief §2 (explicit binding required; no ambient selection without re-verification) |
| `S6` | ✅ **planned → [`plans/033-tenant-identity-provenance.md`](plans/033-tenant-identity-provenance.md)** — **`TenantSource` provenance chain** — JWT claim + subdomain + cross-check + `LegacyTenantSource` | 🔴 must | M–L | ⭐ **The centerpiece.** Replaces the bare header read with an ordered, configurable chain; disagreement between two sources fails the request. Header-only becomes opt-in, not the default. Needs per-issuer claim mapping because **there is no standard OIDC claim name** — Auth0 uses `org_id`, Entra `tid`, Cognito a custom attribute — which `varco_core.jwt.transform` already provides | brief §1 (trust ranking; "header-only, never alone"); brief §1 (no standardized claim name) |
| `S7` | ✅ **planned → [`plans/035-http-edge-hardening.md`](plans/035-http-edge-hardening.md)** — **Security headers middleware** — CSP, HSTS, `X-Content-Type-Options`, `Referrer-Policy`, frame options | 🟡 should | S | **Zero of these exist anywhere in the repo today** — verified by grep across all ten packages. Pure addition, no breaking change, and the single cheapest row in the cycle | brief §5 (table stakes); OWASP Secure Headers Project |
| `S8` | ✅ **planned → [`plans/035-http-edge-hardening.md`](plans/035-http-edge-hardening.md)** — **Request body size limits** (renamed from "Request body size and complexity limits" — Plan 035's Non-goals: no framework middleware can enforce JSON nesting depth without the schema; parked, see below) | 🟡 should | S | Nothing enforces a ceiling today; only the metrics middleware even reads `Content-Length` (`middleware/metrics.py:396`), and reading is not enforcing | brief §5 |
| `S9` | ✅ **planned → [`plans/036-authorization-surface-and-posture.md`](plans/036-authorization-surface-and-posture.md)** — **`SecurityPosture` startup preflight** | 🟡 should | S–M | ⭐ **The vehicle for every warn-only half of the blast-radius decision.** One startup check that reports on: `BaseAuthorizer` still bound, `PassthroughAuth` on a public app, an admin mount with `server_auth=None`, a webhook repository with `encryptor=None`. varco already proves it likes fail-closed — `tenancy_cache_key()` raises rather than silently un-namespacing. Warn in 3.2, refuse in 4.0 | `intuition`, grounded in four verified in-repo instances (`auth/authorizer.py:65`, `auth/server_auth.py:447`, the three `mount_*`, `webhook/models.py`) |
| `S10` | ✅ **planned → [`plans/035-http-edge-hardening.md`](plans/035-http-edge-hardening.md)** — **HTTP rate-limit middleware, per tenant and per subject** | 🟡 should | S–M | The `RateLimiter` ABC and both backends already exist (`varco_core/resilience/rate_limit.py`, `RedisRateLimiter`); what is missing is the ASGI assembly. Same "varco already owns every part, it only lacks the wiring" argument that carried webhooks in 3.1 | brief §5 (global + per-tenant limiting is table stakes) |
| `S11` | ✅ **planned → [`plans/036-authorization-surface-and-posture.md`](plans/036-authorization-surface-and-posture.md)** — **Authorization-decision audit** | 🟡 should | S–M | Every allow/deny through `AbstractAuthorizer`, emitted into the existing audit trail with principal, actor, tenant, and resource. The audit subsystem exists; authz decisions are simply not in it. The Entra actor-token incident turned on impersonation being *unlogged*, not merely permitted | brief §3 (audit must log both principal and actor; CVE-2025-55241) |
| `S12` | ✅ **planned → [`plans/037-data-layer-tenant-enforcement.md`](plans/037-data-layer-tenant-enforcement.md)** — **RLS-by-default for `TenantScope.TENANT` tables** | 🟡 should | M | Extends `render_rls_ddl()` into a generated-for-you path, wires `set_tenant_local()` into the UoW automatically, and documents the never-connect-as-`BYPASSRLS` rule. The database enforces the invariant even when application code forgets. Postgres-only by nature | brief §4 (RLS mature since PG 9.5, fail-closed, OWASP-recommended); `varco_sa/varco_sa/rls.py` |
| `S13` | ✅ **planned → [`plans/034-credential-and-token-lifecycle.md`](plans/034-credential-and-token-lifecycle.md)** — **Token revocation seam** | 🟡 should | M | varco has **no way to invalidate a JWT before `exp`** — no logout, no compromise response, no per-tenant kill switch. A `TokenRevocationStore` consulted in `TrustedIssuerRegistry.verify()` closes it. Basic revocation is not CAEP-dependent; CAEP/SSF becomes a later provider against the same seam | brief §6 |
| `S14` | ✅ **planned → [`plans/034-credential-and-token-lifecycle.md`](plans/034-credential-and-token-lifecycle.md)** — **API keys hashed at rest** | 🟢 nice | S–M | `ApiKeyAuth` holds a `dict[str, AuthContext]` of raw keys in memory, loaded at startup. Hashed-at-rest with a constant-time verify is the expected shape | brief §5 (secrets management); `varco_fastapi/varco_fastapi/auth/server_auth.py:308-343` |
| `S15` | ✅ **planned → [`plans/037-data-layer-tenant-enforcement.md`](plans/037-data-layer-tenant-enforcement.md)** (Phase 5, explicitly droppable) — **Applicator-level tenant-filter assertion** | 🟢 nice | M–L | The portable half of the tenant-filter guarantee — a check that a `TENANT`-scoped entity's compiled query carries a tenant predicate, raising if not. Backend-agnostic and catches the bug in dev, but it is a whitebox assertion over compiled SQL/pipeline structure and is fooled by a raw query that bypasses the applicator. **Ships only after `S12`, never instead of it** | brief §4 notes **no framework ships this** — a differentiator with no prior art to copy, which is exactly why it is 🟢 and not 🔴 |
| `S16` | ✅ **planned → [`plans/033-tenant-identity-provenance.md`](plans/033-tenant-identity-provenance.md)** (Phase 6, explicitly droppable) — **Act-as / RFC 8693 token exchange with `act` claim** | 🟢 nice | M | The sanctioned way for an internal service to operate for an arbitrary tenant once the legacy header is gone. ⚠️ **Ranked 🟢 on absence of evidence, not low value** — varco is a framework with no deployments of its own, so no consumer is known to be blocked. Until it ships, `LegacyTenantSource` is the migration path for anyone who is, which makes that shim load-bearing rather than a courtesy | brief §3 (RFC 8693, `act` claim) |
| `S17` | ❓ **question, filed by [`plans/035-http-edge-hardening.md`](plans/035-http-edge-hardening.md) / §D-order-bugs — not fixed by that plan** — **Is `MetricsMiddleware` intended to sit outside `TracingMiddleware`?** | 🟡 should | — | Verifying Plan 035's middleware ordering contract found `MetricsMiddleware` is registered, and executes, **outside** `TracingMiddleware` (`app.py:511` runs after `:498`, and `add_middleware()` prepends) — but two in-repo comments (`app.py:500-504`'s "sits INSIDE TracingMiddleware so OTel context is already active" and `middleware/__init__.py:16-25`'s "3. Tracing / 4. Metrics") both assert the opposite order. Either the comments are wrong, or `MetricsMiddleware`'s position is a real OTel-context bug (a metric recorded without an active span context). This is a metrics-correctness question with its own blast radius — Plan 035 (a security release) corrected the comments to state the verified reality and stopped there | `varco_fastapi/varco_fastapi/app.py:500-504,511`; `varco_fastapi/varco_fastapi/middleware/__init__.py:16-25` |
| `S18` | ❓ **question, filed by [`plans/035-http-edge-hardening.md`](plans/035-http-edge-hardening.md) / §D-order-bugs — not fixed by that plan** — **Should `create_varco_app(extra_middleware=...)` land inside `ErrorMiddleware`?** | 🟡 should | — | Verified: an `extra_middleware=` entry is registered, and executes, **outside** `ErrorMiddleware` (`app.py:538` runs after `:531`) — the opposite of the in-repo comment that used to read "added before CORS = inside ErrorMiddleware". Consequence: a `ServiceException` raised from an `extra_middleware=` entry is never rendered through the error envelope, and (separately) it also sits outside `RequestContextMiddleware`, so it cannot read `current_tenant()`/the auth subject. This is why Plan 035's three new middlewares (`security_headers=`/`body_limit=`/`rate_limit=`) each got a dedicated `create_varco_app` keyword instead of relying on `extra_middleware=` | `varco_fastapi/varco_fastapi/app.py:537-544` |

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

## Open questions for `/plan`

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
| **Secrets-manager sources (Vault / cloud KMS)** | The technical argument is good — credential rotation is the same problem as cert rotation, and Plans 025–027 built the machinery — but it is an extension, not a gap | JWT-signing-key rotation becoming a concrete need rather than an analogy |
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
