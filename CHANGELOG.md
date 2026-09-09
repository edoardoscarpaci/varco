# Changelog

All notable changes to the varco framework are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Varco packages use [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Security

- **Closed the unmapped-exception information leak (Plan 035 / S3).** Two fallback sites —
  `ErrorMiddleware._service_error_response`'s `except` branch and `add_exception_handlers`'s
  `_make_error_response`'s `except` branch (reached when `error_message_for()` itself raises, e.g.
  a `ServiceException.error_params()` raises) — used to echo `str(exc)` directly into the response
  body. Both now return an opaque `"An internal error occurred."` message plus `correlation_id`,
  and log the exception type server-side at ERROR with `exc_info=True`. Unconditional — there is
  no toggle for this fix; correlate via the `correlation_id` in the log.

### BEHAVIOUR CHANGE — cross-tenant write guard on the admin surfaces (Plan 036, S4)

- **BOLA fix — five webhook-admin by-id routes now 404 on another tenant's subscription.**
  `get_subscription`, `disable_subscription`, `enable_subscription`, `rotate_secret`, and
  `delete_subscription` used to fetch by `pk` with no tenant check at all —
  `rotate_secret` returned the new secret in the response, so a `webhook-admin` of tenant A who
  guessed a subscription UUID could rotate tenant B's signing secret **and read the
  replacement** (OWASP API1:2023 BOLA). Every by-id route now goes through one shared
  fetch-then-compare helper, mapping a cross-tenant hit to **404, not 403** — the routes already
  404 on a genuine miss, so a cross-tenant hit is indistinguishable from an absent resource
  (no existence oracle). `list_subscriptions` no longer trusts the `X-Tenant-Id` header —
  it uses the resolved tenant instead.
- **Cross-tenant guard, opt-in role.** `build_webhook_router`/`mount_webhook_admin`,
  `build_dlq_router`/`build_audit_router`/`mount_reliability_admin` all gained
  `cross_tenant_role: str = "cross-tenant-admin"`. `create_subscription`'s body-supplied
  `tenant_id` is now checked against the resolved tenant (403 on mismatch without the role).
  `ctx is None` (no `server_auth` configured) always resolves `allow_cross_tenant=False` — an
  unauthenticated mount is strictly narrower than before, never wider.
  `mount_tenant_admin` is **deliberately not guarded** — it is the tenant control plane, and
  every one of its routes addresses a tenant that is by definition not the caller's own; see
  `technical_docs/features/admin-surface-tenancy.md`'s §D-S4-control.
- **Reliability-admin default scoping: an omitted `tenant_id` now means "mine", not "every
  tenant".** `dlq_router.list_entries`/`delete_where`/`redrive_batch` and
  `audit_router.list_entries`/`verify_chain`/`delete_where` used to treat an absent `tenant_id`
  query parameter as unscoped — `DELETE /reliability/dlq/entries` with everything omitted was a
  cross-tenant delete reachable by omitting a parameter. It now resolves to the caller's own
  tenant (via `current_tenant()`); an unscoped, cross-tenant sweep requires
  `cross_tenant_role`, and raises when no tenant context exists at all and the role is absent.
  ⚠️ **Upgrade note**: verify a retention/redrive sweep that relied on the old unscoped default
  still does what you intend before granting the role — see the plan's migration table for the
  full list of affected call shapes.

### Added

- **Retention & purge automation (Plan 039, S20).** `varco_core.retention` — `RetentionPolicy`/
  `RetentionRegistry`/`RetentionOutcome`/`RetentionResult` (`retention/policy.py`),
  `RetentionTarget` ABC + `CallableRetentionTarget` escape hatch (`retention/base.py`), six
  in-tree adapters over shipped bulk-delete verbs — `DlqRetentionTarget`/`AuditRetentionTarget`/
  `IdempotencyRetentionTarget`/`RevocationRetentionTarget`/`JobRetentionTarget`
  (`retention/targets.py`, no new abstract method on any shipped ABC),
  `RetentionScheduler`/`execute_policy()`/`RetentionPolicyNotFoundError`
  (`retention/scheduler.py`), `inspect_retention_posture()`/`RetentionPostureReport`
  (`retention/posture.py`), `bind_retention_registry()` (`retention/di.py`),
  `install_retention_metrics()` (`observability/retention.py`). `varco_fastapi.RetentionLifecycle`
  + `create_varco_app(retention=...)` (appended, never prepended). `varco retention` gains
  `--policy <name>` (a third resolution mode alongside `--type`/`--target`) and a `list` verb —
  `--type`/`--target` invocations are unchanged. `Schedule.task_name: str | None = None`
  (`varco_core.schedule.entity`, byte-identical when unset) plus SA migration
  `0008_schedule_task_name` — closes the gap where a materialized `Job` had no executable body.
  Eight safety guards (`dry_run` has no default and never will; `older_than` floors; capability
  mismatches raise at wiring; a `dry_run=True` sweep provably deletes nothing, including on the
  two verbs with no preview) — see `technical_docs/features/retention-and-purge.md`.
- **Inbound webhook signature verification (Plan 038, S19).**
  `varco_core.webhook.inbound` — `WebhookVerifier` (ABC), `VerificationResult`/
  `VerificationFailure`/`SecretEncoding`, and four provider adapters
  (`StandardWebhooksVerifier`/`SvixWebhookVerifier` — delegates to the shipped
  `StandardWebhooksSigner`, `StripeWebhookVerifier`, `GitHubWebhookVerifier`,
  `SlackWebhookVerifier`) plus `get_verifier()`. `WebhookReplayGuard`
  (`varco_core.webhook.inbound.replay`) adapts the existing
  `AbstractIdempotencyStore` into a message-id replay guard — no new ABC, no new backend.
  `WebhookSignatureError`/`WebhookReplayError` (`varco_core.exception.webhook`, 401/409).
  `varco_fastapi.webhook.verify_webhook`/`VerifiedWebhook` is the FastAPI route dependency
  (never a middleware — the secret/algorithm are per-route facts).
  `WebhookSettings` gains `inbound_tolerance_seconds` (`VARCO_WEBHOOK_INBOUND_TOLERANCE_SECONDS`,
  default `300.0`) and `inbound_replay_ttl_seconds`
  (`VARCO_WEBHOOK_INBOUND_REPLAY_TTL_SECONDS`, default `600.0`). Nothing enabled by default;
  see `technical_docs/features/inbound-webhooks.md`.
- **`SecurityPosture` startup preflight (Plan 036, S9).** `SecurityPostureLifecycle`
  (`varco_fastapi.posture`) aggregates 033's `inspect_tenant_provenance()`, 034's
  `inspect_auth_posture()`/`inspect_revocation_posture()`, 035's `inspect_http_edge()`, and 037's
  `inspect_rls_posture()`, plus a local collector for admin-mount/webhook-encryption/
  `BaseAuthorizer` facts, into one report logged at startup and returned as `.report`. Four
  severities (`INFO`/`WARN`/`HIGH`/`NOT_ASSESSED` — a missing sibling module is never silently a
  pass), `VARCO_SECURITY_SUPPRESS` for a knowingly-accepted finding, `VARCO_SECURITY_ENV`
  (presentation only, defaults `production`), and an opt-in `VARCO_SECURITY_ENFORCE=refuse` mode
  that never fails on `NOT_ASSESSED` alone. **Opt-in** — pass it to
  `create_varco_app(extra_lifespan_components=[...])`; nothing runs by default. See
  `technical_docs/features/security-posture.md`, including the consolidated 4.0 flip list
  gathering every warn-only default introduced across the 3.2 security cycle.
- **Authorization-decision audit (Plan 036, S11).** `AuditingAuthorizer`
  (`varco_core.auth.audit`) wraps whatever `AbstractAuthorizer` an app has bound and records
  every denial, plus every allow whose `AuthContext` carries a delegated `actor` — the property
  whose absence was the CVE-2025-55241 (Entra actor-token) attack vector. Opt in via
  `varco_core.auth.di.enable_authorization_audit(container)`, called **last**. Emits
  `AuthorizationDecisionEvent` via `AbstractEventProducer` (never the bus) onto the
  `"varco.audit"` channel; persistence is application wiring, same as any other event. See
  `technical_docs/features/authorization-audit.md`.
- **`SecurityHeadersMiddleware` (Plan 035 / S7).** Baseline security response headers on every
  response, including error responses — `X-Content-Type-Options`, `X-Frame-Options`,
  `Referrer-Policy`, and a scheme-guarded `Strict-Transport-Security` at the `BALANCED` preset
  (default); `STRICT` (opt-in) adds CSP/COOP/CORP/Permissions-Policy.
- **`BodyLimitMiddleware` (Plan 035 / S8).** A hard ceiling on request-body bytes — a
  `Content-Length` pre-check plus a cumulative count over the ASGI `receive()` stream, rejecting
  before Starlette finishes buffering. Raises `RequestBodyTooLargeError` (`varco_core.exception`,
  HTTP 413).
- **`RateLimitMiddleware` + `RateLimitBundle` (Plan 035 / S10).** ASGI assembly around the
  existing `RateLimiter`/`InMemoryRateLimiter`/`RedisRateLimiter` — per-`IP`/`GLOBAL`/`SUBJECT`/
  `TENANT` HTTP rate limiting, registered at up to two stack positions (`PRE_AUTH`/`POST_AUTH`) by
  `create_varco_app(rate_limit=...)`. Opt-in — the one row that ships off by default.
- **`inspect_http_edge()` (Plan 035 / §D-seam).** A pure, side-effect-free read reporting which of
  the above is wired into a given app — the seam Plan 036's `SecurityPosture` preflight consumes.
  Exported from both `varco_fastapi.middleware` and `varco_fastapi` directly.
- **`ErrorEnvelopeSettings.include_detail` (Plan 035 / §D-S3b).** Warn-only knob for the
  `ErrorMessage.detail` (`str(exc)`) echo on every mapped `ServiceException`. Defaults to `True`
  (byte-identical to 3.1) — a 4.0 flip candidate, reported by `inspect_http_edge()` as
  `http.error.detail_exposed`.
- **`varco_sa.rls_autogen` — the generated-for-you RLS DDL path (Plan 037 / S12).**
  `plan_tenant_rls()`/`render_tenant_rls_ddl()`/`tenant_rls_upgrade()`/`tenant_rls_downgrade()`
  build a reviewable, printable RLS plan for every `TenantScope.TENANT` table, deriving each
  table's Postgres cast type from its column and refusing (by default) to silently hide a
  nullable tenant column (`NullTenantPolicy`). Nothing is applied automatically — the caller's
  own reviewed Alembic revision still calls `tenant_rls_upgrade()`.
- **`install_rls_tenant_hook()` (Plan 037 / S12).** A SQLAlchemy `after_begin` listener
  (`varco_sa.tenancy.rls_session`) that sets the `rls.tenant_id` GUC from `current_tenant()` at
  the start of every transaction — covers `SQLAlchemyUnitOfWork`, `get_repository()`, and app
  code holding the session factory directly, including across a commit boundary. Opt-in via the
  new `TenancySettings.rls_set_tenant` (`VARCO_TENANCY_RLS_SET_TENANT`); wired automatically by
  `varco_sa.di.SAModule` when set.
- **`inspect_rls_posture()` (Plan 037 / S12).** `varco_sa.tenancy.rls_check` — reports whether the
  connecting role is a superuser/`BYPASSRLS` (bypasses RLS unconditionally) or owns a table
  without `FORCE` (bypasses that table), the two ways a correctly-created policy can be a silent
  no-op. Never raises; a report, not an assertion — `assert_rls_enabled()`'s existing raise
  condition is unchanged.
- **Two new `TenancySettings` fields, both `False` by default (Plan 037 / S12):
  `rls_set_tenant` (`VARCO_TENANCY_RLS_SET_TENANT`) and `rls_require_tenant`
  (`VARCO_TENANCY_RLS_REQUIRE_TENANT`, raise instead of clearing the GUC when no tenant is
  ambient).**
- **`assert_tenant_predicate()` (Plan 037 / S15, droppable, shipped).** An opt-in,
  **development-time** AST walk (`varco_core.query.applicator.tenant_guard`) asserting that a
  tenant-scoped query's `QueryParams` carries an equality filter on the tenant field. It is a
  development-time assertion that a tenant-scoped query was built with a tenant filter; **it is
  not a security control — Postgres RLS (S12, above) is.** Its one documented false-negative
  class is any query path that never builds a `QueryParams` AST at all (raw
  `session.execute(text(...))`, a hand-written `Select`, `get(pk)`, a Mongo aggregation
  pipeline). Opt in via `TenancySettings.assert_tenant_filter`
  (`VARCO_TENANCY_ASSERT_TENANT_FILTER`) plus `assert_tenant_filter=True` on
  `AsyncSQLAlchemyRepository`/`AsyncBeanieRepository`.

### Fixed

- **`JobRunner.recover()` now honours `Job.run_at` (Plan 039, Phase 1).** `Job.run_at` is
  documented as *"earliest time this job is eligible to be claimed"* and the ABC's own
  `claim_next()` default enforces exactly this predicate — `recover()` used `try_claim()`
  unconditionally and skipped the check, so a job scheduled for the future would fire immediately
  on process restart. ⚠️ **Behaviour change, narrow in practice**: `InMemoryJobStore.try_claim()`
  already enforced `run_at IS NULL OR run_at <= now` internally, so this was not observable
  through the in-memory store every unit test in this repo uses — the fix is a defence-in-depth
  filter at the runner layer, closing the gap for any `AbstractJobStore` whose own `try_claim()`
  does not filter. An app relying on `recover()` firing future-dated jobs at startup (against
  `run_at`'s own documented contract) will see a behaviour change; `run_at=None` (the common case)
  is unaffected.

### Changed

- **`StandardWebhooksSigner.sign()`/`verify()` accept `bytes` (Plan 038, S19, §D-S19-gap).**
  `payload` is now `str | bytes` on both methods — additive, byte-identical for an existing `str`
  caller. A `bytes` payload lets a raw inbound body that is not valid UTF-8 be verified directly,
  without a lossy decode.
- **`render_rls_ddl()`'s returned statement order (Plan 037 / §D-S12-order).** Now
  `CREATE POLICY`, `ENABLE ROW LEVEL SECURITY`, `FORCE ROW LEVEL SECURITY` — previously
  `ENABLE`, `FORCE`, `CREATE POLICY`. Same three statements, same text, only the index changes;
  every in-repo caller executes the whole list in order and is unaffected. Fixes a default-deny
  window (Postgres denies all rows the instant RLS is enabled with no policy yet present) for any
  standalone caller of `render_rls_ddl()` that does not run all three statements in one
  transaction. ⚠️ A caller that indexes the returned list positionally (e.g.
  `render_rls_ddl(t)[0]`) now gets a different statement — no such caller exists in this repo.
- **`varco_sa.rls_framework.FRAMEWORK_RLS_TABLES` is now derived, and wider (Plan 037 / S12a).**
  Previously a hand-listed two-table constant (`varco_audit_log`, `varco_dead_letters`); now
  computed by `framework_rls_tables()` walking `framework_metadata()` for every table carrying a
  tenant column — five-plus tables, including `varco_schedules`,
  `varco_webhook_subscriptions`, and the encryption-key-store table (`varco_tenants` remains
  hard-excluded — its `tenant_id` is a primary key, not a filterable column). The old constant
  name still resolves, computed from the new function, so no import breaks.

- The unmapped-exception fallback body no longer contains `str(exc)` (see Security, above) — no
  revert; this is the security fix.
- Four security headers are now sent by default on every response
  (`VARCO_SECURITY_HEADERS_ENABLED=false` reverts).
- A 10 MiB request-body ceiling is now enforced by default
  (`VARCO_BODY_LIMIT_ENABLED=false`, or `VARCO_BODY_LIMIT_MAX_BYTES=<bytes>`, reverts).
- `correlation_id` is now present on **every** error response body — `ErrorMiddleware` and
  `add_exception_handlers` generate a fresh id when no ambient one is set, rather than omitting the
  key entirely. This widens the S3 fix beyond the two fallback sites to every error path, including
  `_internal_error_response()`'s already-sanitized 500 body. No toggle; not called out in Plan 035
  as an explicit change — see the sync report's Drift section.

### BREAKING — required `algorithms=`, `?api_key=`/`?token=` off by default (Plan 034, S1/S2)

- **`JwtParser.parse()` now requires `algorithms=`** as a keyword-only argument — the previous
  silent `["HS256"]` default is gone, and there is no environment-variable escape hatch (never
  will be). Omitting it raises `TypeError` at the call site, caught by mypy `strict = true` before
  a test runs. **Measured blast radius: zero varco production call sites and zero examples** —
  `JwtBearerAuth` (via `TrustedIssuerRegistry.verify()`, which derives algorithms from the
  resolved key) and `PassthroughAuth` (via `parse_unverified()`, which has no `algorithms`
  parameter) are structurally unaffected. **The fix**: `JwtParser.parse(raw, secret,
  algorithms=["HS256"])` — one line, at every direct call site.
- **`ApiKeyAuth`'s `?api_key=` query-parameter fallback is now off by default** — `param` defaults
  to `None` instead of `"api_key"`. **Measured blast radius: zero in-repo consumers.** **The
  fix**: `ApiKeyAuth(..., param="api_key")` to opt back in; prefer moving clients to the
  `X-API-Key` header — the query param is otherwise in every access log, proxy log, and `Referer`
  header.
- **`WebSocketAuth`'s `?token=` query-parameter fallback is now off by default** — same treatment,
  `token_query_param` defaults to `None`. **The fix**: `WebSocketAuth(inner, token_query_param="token")`
  to opt back in; prefer the existing `Sec-WebSocket-Protocol: bearer.<token>` sub-protocol path
  for browser clients that cannot set headers.

⚠️ **`scripts/api_surface.py --check` does not catch either flip** — it records `inspect.signature()`
only for top-level `function`-kind exports; `JwtParser.parse` is a `classmethod` and
`ApiKeyAuth.__init__` is a class constructor, both outside its documented scope. Out-of-tree
callers must read this entry; the guard against regression is a dedicated `inspect.signature()`
test in each owning package's own suite (`varco_core/tests/test_jwt.py`,
`varco_fastapi/tests/milestone_a/test_server_auth.py`), not the snapshot gate.

### Added — token revocation, hashed API keys (Plan 034, S13/S14)

- **`varco_core.revocation`** — `AbstractTokenRevocationStore` (`TOKEN`/`SUBJECT`/`TENANT`/
  `ISSUER` scopes), `RevocationEntry`/`RevocationVerdict`, `NullTokenRevocationStore` (the scanned
  DI default — off by default, zero cost), `InMemoryTokenRevocationStore`, and
  `varco_redis.revocation.RedisTokenRevocationStore` (the production backend, one `MGET` per
  verification). `TrustedIssuerRegistry(revocation_store=...)` wires it in; the default
  (`None`) is byte-identical to before this feature existed. `varco_core.jwt.config
  .JwtVerificationSettings` gained four `VARCO_JWT_REVOCATION_*` fields (failure mode, `jti`
  requirement, clock-skew, master enable). Two new exceptions,
  `varco_core.authority.TokenRevokedError`/`RevocationStoreUnavailableError`, map to 401/503
  respectively in `JwtBearerAuth` without ever leaking the revocation reason to a client.
- **`varco_core.auth.api_key.hash_api_key()`/`verify_api_key()`** — stdlib-only (`hashlib`,
  `hmac`) SHA-256/HMAC-SHA-256 API-key hashing with `hmac.compare_digest` comparison.
  `ApiKeyAuth` gained `hashed_keys=`/`pepper=` — `keys=` (plaintext) is now hashed immediately at
  construction, so no raw key survives past `__init__`; all 21 existing in-repo
  `ApiKeyAuth(keys=...)` call sites keep working unchanged.
- **`inspect_auth_posture()`/`inspect_revocation_posture()`** (Plan 034 / Phase 4, §D-034-seam) —
  pure, read-only introspection functions (`varco_fastapi.auth.posture` /
  `varco_core.revocation.posture`) reporting facts about an auth tree's and a registry's
  revocation wiring. Definitions only — Plan 036 owns the judgement and any startup wiring built
  on top of them.

### Security — the two-tenant-setter finding (Plan 033 / S6, Correction 1)

- **Finding, not fixed by default in 3.2**: `RequestContextMiddleware` (on by default,
  `enable_tenant_context=True`) has always entered `tenant_context()` from a JWT's `tenant_id`
  claim with **no catalog-status check and no `pool.ensure()`** — the exact check
  `TenantResolutionMiddleware` performs. A token issued for a suspended or deleted tenant
  therefore still activates that tenant's context on any app using `create_varco_app` with a
  container. This is not new in 3.2 and the default is not changed by 3.2 (flipping it would be a
  silent fleet-wide 403 on upgrade, forbidden by the locked blast-radius rule). Fix: adopt a
  `varco_core.tenancy.source.TenantSourceChain` — `TenantResolutionMiddleware` becomes the single
  tenant decision point and `RequestContextMiddleware` defers to it (see Added, below). The
  unchained path is removed in 4.0 (see Deprecated). Full detail:
  `technical_docs/features/tenant-provenance.md`.

### Added — tenant identity provenance, membership binding, act-as (Plan 033, S6/S5/S16)

- **`varco_core.tenancy.source`** — `TenantSource` (ABC: sync, pure, never raises),
  `TenantTrust` (`LOW`/`MEDIUM`/`HIGH`/`HIGHEST`), `TenantRequest`/`TenantClaim`,
  `TenantSourceChain` (+ `CrossCheckMode.LENIENT`/`STRICT`), and the `TenantProvenance` verdict
  object. `varco_core.tenancy.sources` ships three sources —
  `JwtClaimTenantSource`/`SubdomainTenantSource`/`LegacyTenantSource` — plus, for S16 below,
  `ActAsTenantSource`. `varco_core.tenancy.provenance` adds a dedicated `AmbientVar`
  (`current_tenant_provenance()`/`provenance_context()`) — `current_tenant()` remains the single
  source of truth for *who* the tenant is; this is a separate answer to *how it was decided*.
- **`varco_fastapi.middleware.tenant_resolution.TenantResolutionMiddleware`** gained `chain=`,
  `server_auth=`, `membership=`, `reject_status=` keywords — with `chain=` given, it becomes the
  one tenant decision point: verify (`server_auth`) → resolve (`chain`) → check membership → the
  existing catalog-status + `pool.ensure()` gate, unchanged. `chain=None` (the default) is
  byte-identical to pre-3.2 behaviour, now routed through an internally-built
  `LegacyTenantSource` (one `DeprecationWarning` at construction, never per request).
  `varco_fastapi.middleware.request_context.RequestContextMiddleware` defers to an upstream
  chain's verdict — it no longer re-derives the tenant from a token claim once
  `current_tenant_provenance()` is set (Correction 2's fix; no new constructor keyword).
- **`varco_core.tenancy.membership`** — `AbstractTenantMembership`, `NullTenantMembership` (the
  scanned DI default — always allows), `ClaimTenantMembership` (opt-in via
  `varco_core.tenancy.di.enable_tenant_membership()`), `MissingClaimPolicy`
  (`ALLOW`, the 3.2 default; `DENY`, the 4.0 default), `MembershipDecision`,
  `TenantMembershipError`. `CanonicalClaim.TENANTS` (`varco_core.jwt.transform`) reuses the
  existing per-issuer claim-mapping mechanism to get a foreign `tenants`/`orgs`/`organizations`
  claim into `AuthContext.metadata["tenants"]`.
- **The two Plan-036 seams** — `assert_tenant_matches()`/`CrossTenantAccessError`
  (`varco_core.tenancy.provenance`) for S4's cross-tenant admin guard, and
  `inspect_tenant_provenance()`/`TenantProvenancePosture` (`varco_core.tenancy.posture`) for S9's
  preflight. Definitions only — this plan builds neither the guard nor the preflight.
- **`varco_core.auth.delegation`** (S16 — act-as / RFC 8693) — `ActorContext`,
  `DelegationPolicy`/`AllowlistDelegationPolicy` (deny-by-default), `DelegationRecord`. varco
  consumes an already-verified `act` claim; it never issues a token-exchange token. Every
  delegation decision — allow *and* deny — is logged at INFO with both principal and actor and
  attached to `TenantProvenance.delegation` (motivated by CVE-2025-55241's unlogged-impersonation
  finding).

### Deprecated

- **`TenantResolutionMiddleware(chain=None)`** (Plan 033 / S6, §D-S6-blast) — the implicit
  `LegacyTenantSource` fallback (today's `X-Tenant-Id`-header-only behaviour) is scheduled for
  removal in 4.0.0, at which point omitting `chain=` becomes a `TypeError`. Escape hatch: name the
  source explicitly — `TenantSourceChain(sources=(LegacyTenantSource(),))` — which warns not at
  all, or migrate onto a signed-claim source per `technical_docs/features/tenant-provenance.md`'s
  upgrade note.

## [3.1.0] — 2026-09-05

### BREAKING (optional extra) — MCP Python SDK bumped to v2 (Plan 029 / N1)

- **`varco-fastapi[mcp]` now requires `mcp>=2,<3`** (was `mcp>=1.28.1,<2`). `pip install mcp`
  already resolves to the v2 line and v1.x is maintenance-only (security fixes alone) — the old
  pin protected nobody, it only stranded users on an abandoned branch (research brief 003 §1).
  Dual v1/v2 support was evaluated and rejected: v1 registers MCP handlers by decorator
  post-construction, v2 by constructor argument, and one codebase cannot cleanly serve both
  (§D-N1-pin). `MCPAdapter.to_mcp_server()` now builds `mcp.server.Server(name=...,
  on_list_tools=..., on_call_tool=...)` instead of the v1 low-level `Server` +
  `@server.list_tools()`/`@server.call_tool()` decorator pair — deletes both
  `# type: ignore[untyped-decorator]` suppressions. `on_list_tools` now returns a
  `ListToolsResult` (carrying `ttl_ms`/`cache_scope`, required by the 2026-07-28 wire) rather
  than a bare `list[Tool]` — resolved by experiment against the installed SDK, not by reading
  (research brief 003, "Experiment evidence" addendum). New `MCPAdapter(..., ttl_ms=60_000)`
  constructor parameter controls the advertised `tools/list` cache TTL. `mount()`'s
  `SseServerTransport`-based recipe needed no structural change. Blast radius: only users who
  installed `varco-fastapi[mcp]` — nobody else sees a resolver change. Follow-up (not this
  plan): HTTP+SSE is deprecated in favour of Streamable HTTP by the 2026-07-28 spec but remains
  functional; `mount()` migrating transports is new scope, filed to BACKLOG.

### Added

- **Recurring schedules — `varco_core.schedule` (Plan 032 / D6).** A `Schedule` entity (5-field
  cron expression, IANA timezone, `GapPolicy`/`OverlapPolicy`, `CatchUpPolicy`) is materialized
  into ordinary `Job` rows by `ScheduleMaterializer` — no new execution path; the existing
  `AbstractJobRunner` runs the produced jobs unchanged. Cron parsing is a hand-rolled,
  zero-dependency 5-field parser (`varco_core.schedule.cron`); RRULE/RFC 5545 stays parked (would
  need a new `dateutil` runtime dependency). Three catch-up policies govern missed occurrences
  (`SKIP` default, `FIRE_ONCE`, `BACKFILL_ALL`). `AbstractScheduleRepository` +
  `InMemoryScheduleRepository` ship in `varco_core`; `SAScheduleRepository`
  (`varco_sa`, migration `0007_schedules_table`, the thirteenth framework table) and
  `BeanieScheduleRepository` (`varco_beanie`) back Postgres/Mongo. ⚠️ Cross-process
  double-materialization safety deliberately does **not** reuse the job store's fenced-lease
  primitives (a synthetic lease row would corrupt `list_by_status()`/`all_jobs()` counts) —
  instead a deterministic `uuid5` occurrence `Job.job_id` gives idempotent-upsert convergence
  across processes, backstopped by `UNIQUE(schedule_id)`, plus a lazily-created per-schedule
  `asyncio.Lock` for in-process exclusivity. Full design + a Pitfalls table (DST gaps, catch-up
  surprise, materializer downtime): `technical_docs/features/recurring-schedules.md`.
- **Feature-flag evaluation seam — `varco_core.flags` (Plan 032 / D7).** `AbstractFeatureFlags`
  (four typed resolutions: bool/string/numeric/object, each degrading to the caller's `default`
  on an unconfigured key) is a varco-shaped ABC, **not** a transcription of OpenFeature's
  pre-1.0 provider surface — the OpenFeature Python SDK sits at 0.10.0 and the spec at 0.9.0 as
  of the 2026-09-04 check, and 0.10.0 itself shipped a breaking change inside a minor release.
  `NullFeatureFlags` (scanned `@Singleton`, lowest priority) is the always-off DI default;
  `varco_core.flags.di.enable_feature_flags(container)` opts in `InMemoryFeatureFlags`, same
  `enable_*` verb shape as `varco_casbin.di.enable_policy_authorizer`.
  `FlagEvaluationContext.tenant_id` comes from `current_tenant()`, never `RequestContext` — the
  same tenant single-source-of-truth rule as everywhere else. No OpenFeature provider yet: an
  `OpenFeatureFlags` adapter is purely additive once the SDK (not the spec) reaches 1.0. Full
  version evidence and the un-park trigger: `technical_docs/features/feature-flags.md`.
- **CloudEvents v1.0.2 structured envelope — `varco_core.event.cloudevents` (Plan 030 / N2).**
  `CloudEventsJsonSerializer` is a *second* `Serializer[Event]` implementation, so an app can put
  every published event inside a spec-compliant CloudEvents JSON envelope by binding one object:
  `bind_cloudevents_serializer(container, CloudEventsSettings(source="/svc/orders"))`. `Event` is
  unchanged (no field added, no `model_dump()` shape moved — no DLQ/outbox/audit consumer sees a
  byte move) and no bus is changed. **Opt-in and never auto-active**: the module deliberately
  carries no `@Singleton`/`@Provider`, because providify's scanner auto-registers both shapes and
  `container.scan("varco_core", recursive=True)` is an in-use pattern. `source` is required with
  no default (`VARCO_CLOUDEVENTS_SOURCE`) — there is no correct default for "who am I".
  `correlationid`/`tenantid` ship as CloudEvents extensions, validated against the spec's
  `^[a-z0-9]{1,20}$` naming rule; `tenantid` reads `current_tenant()` and is documented and tested
  as **best-effort** (absent under an `OutboxRelay` publish). Structured mode only — binary mode
  needs a header channel and `AbstractEventBus.publish()` is promised never to gain `headers=`
  (reserved-seams RS-2). Zero new runtime dependency: the CNCF `cloudevents` SDK was re-evaluated
  against research brief 005 §3 and rejected again (it disclaims its own stability). Full design,
  the three-phase dual-emit migration and a Pitfalls table:
  `technical_docs/features/cloudevents-envelope.md`.
  Coverage is uniform: the binding reaches all four buses and every `@Configuration`-wired DLQ.
  ⚠️ `SADeadLetterQueue` and `OutboxRelay` are hand-constructed — pass `serializer=` explicitly
  there, and drain an existing DLQ backlog before swapping (stored rows are never converted).
- **Redis Streams CloudEvents convention (Plan 030 / N2).** `RedisStreamEventBus` now writes the
  serialized body to the field named by the bound serializer's optional `stream_field` attribute —
  `"ce"` under `CloudEventsJsonSerializer` (varco's own named, versioned convention: the *whole*
  envelope in one field, never one field per CloudEvents attribute), and the historical
  `"payload"` for every other serializer, byte-for-byte unchanged. Reads accept **either** field
  name, so entries already pending in a stream when the serializer is swapped still drain instead
  of being acknowledged away as unparseable.
- **AsyncAPI 3.1.0 export — `varco_core.asyncapi` + `varco export-asyncapi` (Plan 030 / N3).**
  `generate_asyncapi(consumers_or_container, *, title, version, protocol=…, group_id=…,
  queue_group=…, servers=…)` renders every wired `@listen` handler as a channel + `action: receive`
  operation + a message whose payload is Pydantic's `model_json_schema()`. Generation is
  **runtime, from live registered consumers, never a static import walk** — a `@listen` channel may
  be `Callable[[Any], str]` resolved at `register_to()` time against a bound `self`. Kafka channel
  (`topic`) and operation (`groupId`) bindings; NATS operation binding only when a queue group is
  configured; no Redis binding block at all — with all three choices explained inside the generated
  document's own `info.description`. No `servers` block by default. Zero new dependency (plain
  `dict` + `json`), JSON output only. The new `varco export-asyncapi --check` verb gates a
  committed snapshot inside `make lint`'s no-`PKG` path (`make asyncapi` / `make asyncapi-check`),
  with **no new CI job** and no Node toolchain in CI; spec conformance was validated once by hand
  (`@asyncapi/cli` 6.0.2 — valid, no governance issues) and recorded in
  `design/api-freeze-and-standards/measurements/asyncapi-validate.txt`. Details:
  `technical_docs/features/asyncapi-export.md`.
- **CycloneDX SBOMs per release, and a written regulatory posture (Plan 030 / D5).**
  `scripts/sbom.py` generates **one CycloneDX 1.6 SBOM per distribution** (not one workspace-wide
  document — that would over-report by ~6× and mislead the regulated consumer it exists to serve)
  from `uv.lock` via a version-pinned `cyclonedx-bom`. `release.yml` attaches all ten to the GitHub
  Release and embeds each in its own wheel at `.dist-info/sboms/` per **PEP 770** (verified
  supported by hatchling 1.31.0). ⚠️ PyPI does not yet serve embedded SBOMs, so the GitHub Release
  is the canonical location. New `docs/regulatory-posture.md` states varco's CRA/NIS2 position —
  the non-commercial FOSS exemption, why we believe it applies, and the funding facts that would
  void it — with an explicit "this is a position, not legal advice" disclaimer and **no claim of
  CRA compliance**. It also corrects two errors this repo carried: full CRA enforcement is
  2027-12-11 (not September 2026), and an SBOM is a credibility artifact for downstream consumers,
  not an obligation on a non-commercial upstream (BACKLOG's D5 row amended accordingly).

- **`Idempotency-Key` HTTP middleware — `varco_core.idempotency` + `IdempotencyMiddleware`
  (Plan 029 / D1).** A retried `POST`/`PATCH` carrying a repeated `Idempotency-Key` header
  replays the first response instead of executing twice. `AbstractIdempotencyStore`
  (`reserve`/`complete`/`get`/`release`/`delete_expired`) is the seam — `reserve()` is the one
  atomic primitive every implementation must offer (never emulated with `exists()`+`set()`,
  which is why `AsyncCache` was deliberately not extended). Four implementations ship:
  `InMemoryIdempotencyStore` (`varco_core`, single-process only), `RedisIdempotencyStore`
  (`SET NX PX`), `SAIdempotencyStore` (`UNIQUE` + `IntegrityError`), `BeanieIdempotencyStore`
  (unique index + `DuplicateKeyError`) — covered by a shared conformance suite
  (`testkit/varco_conformance/idempotency_store.py`) including a genuine concurrency race
  against all four backends. `IdempotencyMiddleware` (`varco_fastapi.middleware.idempotency`) is
  **opt-in** — never added by `create_varco_app()` — and must be registered inside
  `ErrorMiddleware` and inside `RequestContextMiddleware` (a correctness requirement, asserted
  by test). Fingerprint = `sha256(method + path + sorted_query + sha256(body))`; a reused key
  with a different fingerprint returns 422, an in-flight reservation returns 409 (with
  `Retry-After: 1`), a malformed/oversized key returns 400. Streaming responses and responses
  over `max_stored_body_bytes` (default 1 MiB) are never captured — the reservation is released
  so a retry re-executes. Storage key scoping fails closed when tenancy is enabled and no
  ambient tenant is set. Implements the (expired) IETF draft
  `draft-ietf-httpapi-idempotency-key-header-07` plus Stripe's de-facto conventions (24h TTL),
  not an RFC. Full design: `technical_docs/features/idempotency-key.md`.
- **Import-time budget — `scripts/import_budget.py` + `make import-budget` (Plan 028 / P1).**
  Measures each distribution package's `python -X importtime` cost above a bare-interpreter
  baseline measured in the same job (best-of-5, fresh subprocesses, self-times summed) and
  compares the **delta** against a hard ceiling committed in
  `design/async-performance-patterns/measurements/import-budget.json`. Derives its package list by
  executing `scripts/packages.sh` (RL-18) and fails loudly rather than silently measuring an empty
  target list. `--check` / `--update` (measured values only, never ceilings) / `--warn-only`.
  ⚠️ **Warn-only today**: wired into `make lint`'s no-`PKG` path and `test.yml`'s `lint` job with
  `--warn-only`, so a breach prints and CI stays green. The flip to a real gate is deliberately
  blocked on ≥10 recorded CI observations, because the ~2× ceiling headroom is an assumption about
  runner variance that no source quantifies — U-8 evidence discipline applied to our own gate. A
  ratchet was rejected on three independent grounds (see the script's `DESIGN:` block).
- **Benchmark harness — `benchmarks/` + `make bench` + `.github/workflows/bench.yml` (Plan 028 /
  P2).** Seven in-process, deterministic, Docker-free benchmarks (query parse; AST build + SA
  compile; DTO roundtrip; `AsyncService.create()` over an in-memory repo; event publish; cache
  get/set; subprocess `import varco_core`) run through `pytest-codspeed`, which lives in a root
  `bench` dependency group **excluded from `dev`** so a normal `uv sync` never installs it.
  Collected by `benchmarks/pytest.ini` (`python_files = bench_*.py`), so `scripts/unit_tests.sh`
  never picks them up and the unit legs are unaffected. ⛔ **Comment-only, never a gate**:
  `bench.yml` is a separate workflow, is not in `test.yml`'s `needs:`, must never appear in
  `all-green`'s `needs:` or in branch protection, and skips (never fails) on a fork PR with no
  `CODSPEED_TOKEN`. The asymmetry with the import budget is deliberate and argued in the plan's
  §D-P1-oq4.
- **`varco_core.watch` — `AbstractPathWatcher`, `StatPollWatcher`, `WatchfilesWatcher` (Plan 025 /
  T1).** Backend-agnostic filesystem watching with no new hard dependency: `StatPollWatcher`
  (stdlib-only, the default via `default_watcher()`) polls a `(st_mtime_ns, st_size, st_ino)`
  stat fingerprint of the *resolved* target, correct under atomic rename and under the
  Kubernetes `..data` symlink-swap cert/ConfigMap rotation — and correct on NFS/Docker bind
  mounts, where inotify never fires. `WatchfilesWatcher` (opt-in, `pip install
  "varco-core[watch]"`) is backed by the Rust `notify` crate via `watchfiles`, but re-derives
  every event from the same stat-fingerprint diff rather than trusting watchfiles' own event
  kind, so both implementations emit byte-identical event streams and share one contract test
  suite (`varco_core/tests/watch_contract.py::PathWatcherContract`). Both debounce: a rotation
  rewriting several files fires one coalesced callback, not several. New `varco-core[watch]`
  optional extra (`watchfiles>=1.2.0`).
- **`varco_core.reload.ReloadableResource[T]` (Plan 025 / T2).** Load → swap under a lock →
  notify subscribers, with **keep-last-good** semantics: the first `start()` load is fail-fast
  (no last-good value to fall back on), but every reload after that keeps serving the last
  successfully loaded value on a loader failure — a truncated or half-written file mid-rotation
  never takes a live service down. Composes with an optional `AbstractPathWatcher`, but
  `reload()` is always independently callable (e.g. from a `SIGHUP` handler). Both
  `AbstractPathWatcher` and `ReloadableResource` structurally satisfy
  `varco_fastapi.lifespan.AbstractLifecycle` with zero import from `varco_core` into
  `varco_fastapi`.
- **`varco_core.tls` — `TrustStore`, `ReloadingTrustStore` (Plan 026 / T3, T5, T7).**
  `TrustStore` (a frozen dataclass) is a strict superset of both `SSLConfig`'s `verify=False`
  escape hatch and the old `varco_fastapi.auth.TrustStore`'s `include_system_cas`/`bytes`-CA
  support, reachable from any backend — not just `varco_fastapi`. Recursive, multi-folder CA
  search (`ca_folders`, opt-in on `SSLConfig` via new `recursive`/`cert_patterns` fields,
  default-on for `TrustStore`), eager mTLS-pairing validation at construction, and
  `SSLConfig.to_trust_store()` / `TrustStore.to_ssl_config()` as a lossless bridge (carries
  both `verify` **and** `check_hostname`, unlike the pre-existing
  `HttpConnectionSettings.to_trust_store()`, which still drops `verify=False`).
  `ReloadingTrustStore` composes Plan 025's `ReloadableResource[ssl.SSLContext]` +
  `AbstractPathWatcher` for hot reload: additions-only batches mutate the live
  `ssl.SSLContext` in place (cheap, the common cert-renewal path); anything removed or
  replaced rebuilds and swaps the context (`generation` bumps, `subscribe()` fires) —
  `ssl.SSLContext` has no unload API, so mutation can only ever add trust, never revoke it.
  `varco_core.tls.bind_trust_store(container, store)` registers an already-owned store as a
  DI singleton with no lifecycle side effect; there is deliberately no scanned
  `@Configuration`. `varco_core.tls.iter_cert_files()` is the one cert-glob helper now shared
  by `SSLConfig`, the deprecated `varco_fastapi.auth.TrustStore`, and `PemFolderSource` — see
  the `### Changed` entry below for its behavioural delta. Full design:
  `technical_docs/features/tls-trust-and-hot-reload.md`.
- **`ssl_context=` on `JwksUrlSource` / `OidcDiscoverySource` (Plan 026 / T5).** Both fetched
  through a bare `urllib.request.urlopen()` with no SSL context, making a JWKS endpoint behind
  an internal PKI or an intercepting corporate proxy unverifiable without process-wide env
  vars. Now accept a keyword-only `ssl_context: ssl.SSLContext | None = None`, forwarded
  through `IssuerSourceFactory.from_string(ssl_context=)` and
  `AuthorizationConfig.to_registry(ssl_context=)`. `TrustedIssuerRegistry.from_env()` builds
  one automatically from `varco_core.tls.TrustStore.from_env()`, but only when at least one CA
  env var is actually set — `None` (stdlib default) otherwise, byte-identical to before. ⚠️
  This fixes certificate verification only, not proxy handling — `HTTP_PROXY` still applies.
- **`varco_core.tls.clients` — `to_httpx_verify()`, `to_aiohttp_connector()`,
  `to_urllib3_poolmanager()`, `to_requests_adapter()` (Plan 027 / T4).** Four thin adapters
  converting a `TrustStore`/`ReloadingTrustStore` into the shape each mainstream HTTP client
  wants (`ssl.SSLContext`, `aiohttp.TCPConnector`, `urllib3.PoolManager`, an
  `HTTPAdapter` subclass), with **no new hard dependency** on httpx, aiohttp, urllib3, or
  requests — every import is inside the function body that needs it, mechanically guarded by
  `test_tls_no_hard_client_deps.py`. Also exposed as delegating methods directly on
  `TrustStore`/`ReloadingTrustStore`. All four read the store's context at call time, so a
  `ReloadStrategy.MUTATE` rotation reaches an already-built client with no action; `SWAP`
  requires rebuilding via `store.subscribe(cb)`. A missing library raises
  `MissingClientDependencyError` naming the `pip install` package.
- **`varco_core.tls.install_process_trust()` (Plan 027 / T4).** An explicit, acknowledged,
  reversible process-global override of `ssl._create_default_https_context`, so any
  stdlib-ssl-backed HTTP client built after the call (via a `create_default_context()`-style
  path) picks up a `TrustStore`'s trust configuration process-wide. Requires
  `acknowledge_global_mutation=True` (`ValueError` otherwise, with no mutation) and returns a
  `RestoreHandle` (also a context manager) that undoes it. **varco itself never calls this
  function** — it is an application-level decision only, mechanically checked by an `rg` sweep.
- **`TrustStore.key_password` — encrypted private-key support (Plan 027 / T6).** A `str`,
  `bytes`, or zero-argument callable passed straight through to
  `ssl.SSLContext.load_cert_chain(..., password=...)`, so an mTLS client key encrypted with
  `BestAvailableEncryption` (previously unsupported — `build_ssl_context()` would raise, or on
  some platforms prompt on a TTY) now loads correctly. The callable form is recommended so the
  secret is fetched lazily (e.g. from Vault/KMS) rather than held in memory for the store's
  lifetime; the field is `repr=False` so it never appears in a `repr(store)`. Requires
  `client_key` to also be set (`ValueError` otherwise).
- **`TrustStore.pkcs12_file` / `pkcs12_password` / `pkcs12_trust_ca` — PKCS#12/`.pfx` support
  (Plan 027 / T6).** Ingests a PKCS#12 client-identity bundle as an alternative to
  `client_cert`/`client_key` (mutually exclusive with them), decoded via `cryptography`
  (already a hard `varco_core` dependency) with **no new dependency** and no
  `httpx-pkcs12`/`requests-pkcs12` shim. The private key is briefly materialized to a private
  (`0600`, `/dev/shm`-preferred) temp file for `load_cert_chain` to read, then unlinked in a
  `finally` on both the success and failure path. `pkcs12_trust_ca` defaults to `False` — the
  bundle's own CA chain is not automatically trusted for server verification unless opted in.
  A wrong password or corrupt bundle raises `Pkcs12LoadError`, never a raw `cryptography`
  traceback.
- **Outbound webhooks — `varco_core.webhook` + `varco_fastapi.webhook.mount_webhook_admin`
  (Plan 031 / D4).** A subscription registry (`WebhookSubscription`), signed retried delivery
  (`WebhookDispatcher`), and an admin surface, assembled entirely from parts varco already ships
  (`RetryPolicy`, `AbstractDeadLetterQueue`, `DlqRedriver`, `FieldEncryptor`) — no new reliability
  primitive, no new crypto path. Signing defaults to **Standard Webhooks**
  (`webhook-id`/`webhook-timestamp`/`webhook-signature`, HMAC-SHA256, space-delimited
  multi-signature for zero-downtime rotation); RFC 9421 ("HTTP Message Signatures" + RFC 9530
  `Content-Digest`) is available opt-in via `WebhookSubscription.signer = "rfc9421"` for
  consumers that require a standards-track scheme. Every delivery target is resolved, validated
  against the private/loopback/link-local/multicast/unspecified/reserved ranges (including the
  IPv4-mapped bypass form), and **pinned** to the first resolved address to defeat DNS rebinding
  (`varco_core.webhook.ssrf.validate_target()`) — `https` only unless a deployment opts into
  `http` via `WebhookSettings.allow_insecure_http` (never per-tenant), and no redirect following.
  `WebhookDispatcher` is an `EventConsumer` (never holds `AbstractEventBus`) and runs its own
  per-subscription retry loop on the existing `RetryPolicy`; exhaustion pushes to the existing
  DLQ (`push()` never raises) and a subscription auto-disables after
  `WebhookSettings.disable_after_failures` (default 20) consecutive failures.
  `WebhookSettings` (env prefix `VARCO_WEBHOOK_`) is the single configuration source —
  `WebhookDispatcher(settings=...)` forwards the SSRF knobs to `validate_target()`,
  `signature_tolerance_seconds` to the signer, and the retry/timeout/disable knobs into its own
  defaults; omitting `settings=` constructs one from the environment, and an explicit
  `retry_policy=`/`request_timeout_seconds=`/`disable_after_failures=` keyword overrides the
  corresponding field. ⚠️ The *shipped* retry defaults (`max_attempts=8`, sub-second delays)
  match Svix's documented attempt-count shape but not its real seconds-scale schedule — a
  production deployment must raise `VARCO_WEBHOOK_RETRY_BASE_DELAY_SECONDS`/
  `_RETRY_MAX_DELAY_SECONDS` or pass its own `retry_policy=`. `active_secrets` are encrypted at rest via the
  existing `FieldEncryptor` when a repository is constructed with `encryptor=`; `encryptor=None`
  (the default on both `SAWebhookSubscriptionRepository`/`BeanieWebhookSubscriptionRepository`)
  stores plaintext and is a documented dev/test-only escape hatch. `mount_webhook_admin(app, *,
  repository=, redriver=None, acknowledge_bundled_admin=True, ...)` follows the same gated shape
  as `mount_reliability_admin`/`mount_tenant_admin` (RD-9) — never a `create_varco_app()` kwarg,
  never an env var. Delivery is at-least-once and documented as such; a varco-app receiver should
  deduplicate on the stable `webhook-id` via plan 029's `Idempotency-Key` middleware. Full design,
  the SSRF model, and a Pitfalls table: `technical_docs/features/outbound-webhooks.md`.

### Deprecated

- **`varco_fastapi.auth.TrustStore` → `varco_core.tls.TrustStore` (Plan 026 / T3, §D-T3-oq1),
  removed in 4.0.0.** The `varco_fastapi` type is now a thin subclass of the new
  `varco_core.tls.TrustStore` that pins its exact 3.0 semantics (non-recursive folder scan,
  `("*.pem", "*.crt")` cert patterns, mTLS-pairing check deferred to `build_ssl_context()`
  rather than eager at construction) — every existing construction keeps producing a
  byte-identical `ssl.SSLContext`. A `DeprecationWarning` fires at construction, not at import.
  ⚠️ **Asymmetric `isinstance`**: `isinstance(legacy_instance, varco_core.tls.TrustStore)` is
  `True` (every new API accepts an old object), but
  `isinstance(core_instance, varco_fastapi.auth.TrustStore)` is `False` — a user who constructs
  the *new* type and passes it to code that `isinstance`-checks the *old* one will be
  surprised. Nothing in this repo does such a check
  (`varco_fastapi/tests/test_http_connection.py:110` checks the legacy type against a value
  that, by design, still comes back as a legacy instance from `HttpConnectionSettings.
  to_trust_store()`); audit your own call sites if you do.

### Changed

- **BREAKING-ADJACENT (but not a break): `import varco_core` is now lazy (Plan 028 / P1a).**
  `varco_core/__init__.py` was ~330 lines of eager `from varco_core.X import (...)` binding 235
  names and pulling ~700 modules; it is now a PEP 562 module `__getattr__` over a committed
  `_LAZY` name→submodule map, with the previous import block kept verbatim under
  `if TYPE_CHECKING:` so mypy `strict`, IDEs and `scripts/api_surface.py` are unaffected.
  **Measured: 289.6 ms → 6.6 ms** above a bare-interpreter baseline; `lark`, `jwt`, `psutil` and
  `opentelemetry.sdk` are no longer in `sys.modules` after a bare `import varco_core`.
  `varco_fastapi` and `varco_redis` improve ~22% for free. **Not one name was added or removed**
  — `scripts/api_surface.py --check` passes with no snapshot regeneration, which is the strongest
  available proof the change is invisible. Two accepted incompatibilities, both documented in the
  module's `DESIGN:` block: an `ImportError` inside a submodule now surfaces at first *attribute
  access* rather than at `import varco_core` (mitigated by a test that resolves every `__all__`
  name on every CI run), and `varco_core.__dict__["Name"]` raises `KeyError` before that name's
  first access — attribute access, `from`-import and `getattr` are all unaffected. The
  side-effect audit bounding the change (two `rg` sweeps, a module-scope decorator sweep, and a
  `sys.modules` differential showing an empty set difference in both directions) is committed at
  `design/async-performance-patterns/measurements/p1-side-effect-audit.md`.

- **A cert file matching a wider known pattern set, but not a call site's own patterns, in a
  `ca_folder`/folder-based JWT key source now logs a WARNING once instead of being silently
  skipped (Plan 026 / T7).** `SSLConfig`/the deprecated `varco_fastapi.auth.TrustStore` glob
  `("*.pem", "*.crt")`; `PemFolderSource` globs `("*.pem",)`; none of these defaults changed —
  a `.cer` file (or any other file matching `varco_core.tls.CERT_FILE_PATTERNS`) is still not
  loaded by default, but is now named in a WARNING (at most once per `(root, patterns)` per
  process) instead of vanishing with no trace. Opt a site into the wider set explicitly with
  `cert_patterns=varco_core.tls.CERT_FILE_PATTERNS` (`SSLConfig`) or
  `patterns=varco_core.tls.CERT_FILE_PATTERNS` (`PemFolderSource`).
- **CI: `integration.yml` now cancels stale runs on `main` (Plan 024).** A new workflow-level
  `concurrency` stanza — `group: ${{ github.workflow }}-${{ github.event_name }}-${{ github.ref
  }}`, `cancel-in-progress: true` — means that when several PRs merge to `main` in quick
  succession, only the newest merge commit's `integration` run reaches completion; earlier
  in-flight/pending runs on the same trigger are cancelled. The nightly `schedule` run and any
  `workflow_dispatch` run are scoped by `github.event_name` into *separate* groups and are never
  cancelled by, or able to cancel, a merge run — deliberately distinct from `test.yml`'s simpler
  ref-only group, which would otherwise let a merge cancel the nightly run (and its `chaos` job).
  **Test-only release — no runtime package changed.**

- **`scripts/api_surface.py --check` is now a CI gate**, not a tool a contributor had to remember
  to run by hand. Wired into `make lint`'s no-`PKG` path (`make lint PKG=<one package>` stays
  narrow and skips it, deliberately), a standalone `make api-check`, and CI's `lint` job (a step
  after `mypy`). Scope is unchanged and stated honestly in CLAUDE.md: catches removals and
  *function* signature changes only — a narrowed class `__init__` stays invisible; additions and
  module moves remain notes, never failures.
- **`testkit/varco_conformance/COVERAGE.md`** (new) records the conformance-suite coverage audit
  for all five shared ABCs (`event_bus`, `cache`, `job_store`, `dlq`, `channel_manager`) and every
  implementation's subclass-or-stated-reason status — test-surface only, no production change.

### Fixed

- **`bind_cloudevents_serializer()` silently never reached either Redis bus** (Plan 030 / N2
  follow-up). `RedisEventBusSelectorConfiguration.bus()` produces both Redis implementations from a
  `@Provider`, and providify injects only what the provider *method* declares — it declared
  `settings` alone, so the container binding never arrived and each bus fell back to
  `JsonEventSerializer()`. An app that opted into CloudEvents therefore published envelopes on
  Kafka/NATS and plain varco JSON on Redis, with no error, and Plan 030's `ce` stream-field
  convention never engaged outside a hand-constructed bus. Fixed by declaring and forwarding
  `Serializer[Event]` on the provider, and by widening `RedisStreamEventBus`'s `serializer`
  annotation from the concrete `JsonEventSerializer | None` to `Serializer[Event] | None`.
  Guarded by `varco_redis/tests/test_redis_cloudevents_di.py`.
- **Every dead-letter queue stored a different wire format from the bus it backed.** All five
  backends (`RedisDLQ`, `KafkaDLQ`, `NatsDLQ`, `SADeadLetterQueue`, `BeanieDeadLetterQueue`)
  constructed `JsonEventSerializer()` as a literal in `__init__`, so a CloudEvents app wrote
  envelopes onto the bus and native JSON into the DLQ — two formats for one event, and a redrive
  that republished the wrong one. Each now takes `serializer` as a parameter; the three
  `@Configuration`-wired backends forward the container binding and `BeanieDeadLetterQueue` injects
  it as a scanned `@Singleton`. `OutboxEntry.from_event()` and `OutboxRelay` were widened from
  `JsonEventSerializer | None` to `Serializer[Event] | None` for the same reason. Not a behaviour
  change when nothing is bound — the default stays `JsonEventSerializer()`.


- **`BeanieMigrator.upgrade()` never reached its `index_mode="create"` block when the migration
  registry had no pending revisions** (RT9). It returned early before the index-reconciliation
  block, silently disagreeing with `plan()`, which always reports index drift independently. Fixed
  by conditioning the early return on index drift too (a bounded extra `listIndexes` round-trip,
  paid only when the migration registry is empty) and reconciling indexes under the migration lock
  even on the zero-pending-migrations path. `upgrade(dry_run=True)` now also reports index
  revisions, agreeing with `plan()`.
- **`container.ashutdown()` orphaned nine started/connected resources produced by a `@Provider`**
  (`P22-PROVIDER-PREDESTROY`). providify never invokes `@PreDestroy` on a `@Provider`-produced
  instance (confirmed intentional upstream — providify 2.0.1's changelog, Jakarta CDI
  producer-method rule) — `@Disposes` is the supported teardown mechanism instead. All nine
  affected sites now carry a `@Disposes` method on their producing `@Configuration`:
  `RedisCache`, `LayeredCache`, `RedisEventBus`/`RedisStreamEventBus`, `RedisDLQ`,
  `RedisStreamDLQ`, `RedisBulkhead`, `KafkaDLQ`, `NatsDLQ`, `MemcachedCache`. `providify>=2.0.1`
  is now the floor across all ten packages. See `design/upstream-gaps/providify-provider-predestroy.md`
  §8 for the full resolution.

- **`varco_beanie`: a Mongo-backed `older_than` retention sweep could report "done" while a
  matching entry remained.** BSON `UTCDateTime` is millisecond-precision, and pymongo floors
  *both* the stored value and the query operand — so a raw `$lt` cutoff was actually evaluated
  as `stored_ms < floor_ms(cutoff)`, silently excluding every entry stored in the cutoff's own
  millisecond even though its own reported timestamp was genuinely strictly before the cutoff.
  Affected `BeanieDeadLetterQueue.delete_where(older_than=)`/`.list_entries(older_than=)` and
  `BeanieJobStore.delete_where(completed_before=/expires_before=)`. Fixed by widening only the
  exclusive-upper-bound operand to the next whole BSON millisecond
  (`varco_beanie._bson_time.ceil_to_bson_millisecond`) before it reaches pymongo — `newer_than`'s
  `$gt` and the job store's `$lte` lease/`run_at` predicates are untouched, since pymongo's floor
  is already correct there. Not a contract change: `<` semantics are unchanged and still match
  every sibling backend (`InMemoryDeadLetterQueue`, `SADeadLetterQueue`, `RedisDLQ`).

- **CI: the `Docs` workflow could never run `mike`.** Both the `dev` and `release` jobs installed
  the workspace with `uv sync --locked --all-packages --all-extras`, which resolves *extras* but
  not PEP 735 *dependency groups* — and `mike`/`mkdocs`/`mkdocstrings` live in the non-default
  `docs` group, so every docs deploy died with `error: Failed to spawn: mike`. Both jobs now pass
  `--group docs` as well (the CI equivalent of `make docs-deps`), keeping `--all-packages
  --all-extras` because mkdocstrings imports the packages live to render the API reference.

## [3.0.0] — 2026-08-31

### Packaging & release (Plan 023, RL-9 / RL-13)

- **Lockstep versioning.** All ten distribution packages now carry the identical version
  `3.0.0`, written by a tested `scripts/bump.py` (tomlkit-based, style-preserving) rather than by
  hand. Previous, divergent versions: `varco-core` 1.2.0, `varco-kafka` 2.1.1, `varco-nats` 2.1.1,
  `varco-redis` 2.1.2, `varco-sa` 2.2.0, `varco-beanie` 1.2.0, `varco-memcached` 1.1.1, `varco-ws`
  2.1.0, `varco-fastapi` 1.2.0, `varco-casbin` 2.1.1. A future release bumps all ten together;
  `scripts/bump.py --check` fails CI if they ever diverge again.
- **Sibling requirement strings are now bounded.** Every `varco-*` sibling dependency (previously
  a bare, unbounded `"varco-core"`) is now pinned `~=3.0` (PEP 440 compatible release), both in
  `[project].dependencies` and in the two shipped optional-dependency extras that reference a
  sibling (`varco-fastapi`'s `ws` extra, `varco-casbin`'s `fastapi` extra). See
  `CONTRIBUTING.md`'s versioning policy for the full rationale.
- **PEP 639 license metadata.** All ten packages now declare `license = "Apache-2.0"` (SPDX
  expression) + `license-files = ["LICENSE"]`, replacing the legacy `license = { text = "..." }`
  table form; the redundant `"License :: OSI Approved :: Apache Software License"` classifier is
  removed. `[build-system] requires` is raised to `hatchling>=1.27` in all ten.
- **`Development Status :: 5 - Production/Stable`** replaces `3 - Alpha` in all ten packages'
  classifiers — this release is the project's first production-stable statement.

### BREAKING — API-surface freeze audit (Plan 022, RL-8)

Four accepted breaks out of twelve audited candidates. Each was decided at an
explicit checkpoint recorded, with its reasoning and blast radius, in
`design/api-freeze-and-standards/api-break-candidates.md`; the remaining eight
candidates were verdicted `leave-and-document` and change nothing. Three of the
four ship a deprecated alias that resolves to the **identical object**, so
`isinstance` and `except` keep working; all three are removed in **4.0.0**.

- **AB-5 — `CORSConfig.allow_origins` now defaults to `()` instead of `("*",)`.**
  ⚠️ **The one break with a security consequence, and the only one with no
  alias — read this even if you skip the rest.** The old default combined with
  the (unchanged) `allow_credentials=True` default, and with
  `create_varco_app()`'s unconditional `install_cors()` call, to ship a
  reflect-any-origin-with-credentials policy to every app that set no
  `VARCO_CORS_*` env var. Starlette does not reject that combination and
  browsers do not block it: with credentials on, `CORSMiddleware` stops sending
  the literal `*` and instead **reflects the request's own `Origin`** alongside
  `access-control-allow-credentials: true`, which is valid to a browser. Verified
  against Starlette 1.0.0 — `install_cors(app, CORSConfig())` answered a request
  carrying `Origin: https://evil.example.com` with
  `access-control-allow-origin: https://evil.example.com`. `CORSConfig.from_env()`
  now falls back to `()` for an unset `VARCO_CORS_ORIGINS` too, since that is the
  path most production apps take. **An alias is impossible for a default value,
  so this is a silent behaviour change**: if you were relying on the permissive
  default, cross-origin requests will now be refused — set `VARCO_CORS_ORIGINS`
  or pass `allow_origins=` explicitly. Opting back in to `("*",)` still works and
  is now a visible decision. (A docstring claiming the combination was "invalid
  per the CORS spec" and that "Starlette will raise at app startup" was also
  corrected — both claims were false.)
- **AB-2 — schema migration's `MigrationError`/`MigrationPlan` renamed to
  `SchemaMigrationError`/`SchemaMigrationPlan`.** They collided at the
  `varco_core` top level with the older, unrelated `varco_core.migrator` (domain
  data/field migration) pair of the same names, which forced two deliberate
  re-export holes. **The whole schema-migration surface is now exported from
  `varco_core` directly.** `varco_core.migration.MigrationError` / `.MigrationPlan`
  still resolve there as deprecated aliases (removed in 4.0.0);
  `varco_core.MigrationError`/`.MigrationPlan` still mean the *domain* pair and
  are unchanged.
- **AB-1 — `varco_sa.rls.enable_rls_ddl()` renamed to `render_rls_ddl()`.** It
  reads as a member of the DI opt-in `enable_*` family while touching no
  container, registering no binding and performing no I/O — it is a pure
  DDL-string generator, which `render_*` states truthfully. `enable_rls_ddl`
  remains as a deprecated alias (removed in 4.0.0).
- **AB-4 — `varco_beanie.BeanieConfig` collapsed into `BeanieSettings`.** The two
  were one concept under two names with identical fields, bridged by KI-10's
  manual field-for-field remap, which is now deleted. `BeanieConfig` remains as a
  deprecated alias — resolving to the identical `BeanieSettings` class — from
  both `varco_beanie` and `varco_beanie.bootstrap` (removed in 4.0.0).
  `BeanieFastrestApp(config=...)` now takes a `BeanieSettings`.

**AB-3 (`install_*` vs providify's `container.install()`) was verdicted
`leave-and-document`** — no code changed. The audit did fix the CLAUDE.md
taxonomy row that described `install_*` as uniformly "container-free, a
process-global side effect": that is false of `install_middleware_stack` and
`install_cors`, which take and mutate an ASGI app. One verb, two shapes.

### BREAKING — container teardown at shutdown (Plan 022, RL-8a)

**The signature change is additive; the behaviour change is not.** `VarcoLifespan`
gains one optional, keyword-only `shutdown: Callable[[], Awaitable[None]] | None
= None` hook — symmetric to its existing `setup=` — and `create_varco_app()`
fills it with `container.ashutdown()` whenever it was given a container. Every
existing `VarcoLifespan(...)` call site compiles and behaves exactly as before;
apps built through `create_varco_app()` do not. **They will newly run every
`@PreDestroy` hook the DI container holds at ASGI shutdown**, including hooks
that have never run before in the process's life. Audit yours before upgrading.

Why this was worth a breaking behaviour change, measured rather than assumed
(`design/api-freeze-and-standards/measurements/predestroy-vs-lifespan.md`):
**6 of the 10 `@PreDestroy`-bearing singletons in the workspace were orphans** —
`KafkaChannelManager`, `NatsStreamManager`, `RedisChannelManager`, `RedisCache`,
`MemcachedCache`, `CasbinPolicyEngine`. `create_varco_app()` only ever registered
four well-known interfaces as lifecycle components (`AbstractEventBus`,
`AbstractJobRunner`, and the two `varco_ws` buses), so no `ChannelManager`,
`CacheBackend` or `PolicyEngine` was ever torn down. Two of the six were leaking
an **already-open connection**.

Mechanics, all three deliberate:

- **Order.** `_stop_all()` runs **first**, unchanged, so registered components
  still stop in documented LIFO dependency order; the container sweep only mops
  up afterwards. A component reachable by both paths therefore has `stop()`
  called twice — all ten shipped implementations were read and confirmed
  idempotent, and `VarcoLifespan.register()`'s docstring already required it.
- **Failures are logged, never raised.** providify's aggregated `ShutdownError`
  is unpacked into one ERROR line per `ShutdownFailure`, naming the component and
  its exception; any other exception from the hook is contained the same way.
  This matches `_stop_all()`'s pre-existing "logs errors but does not raise"
  contract — two teardown paths with opposite failure semantics would be
  indefensible — and avoids raising out of an `asynccontextmanager`'s `finally`
  during ASGI shutdown, which masks the real cause. There is deliberately no
  knob to make it raise.
- **Container-free apps are unaffected.** `create_varco_app(container=None)`
  passes `shutdown=None`, which is byte-identical to the previous behaviour.

⚠️ **Known limitation:** `RedisCache` and `MemcachedCache` are *not* fixed by
this. providify's teardown runs a `@Disposes` disposer for a provider-created
binding and never reaches the `@PreDestroy` of the object the provider returned;
both caches exist only as a `@Provider` return value. Pinned by a strict xfail in
`varco_redis/tests/test_redis_cache_lifespan_shutdown_integration.py` and filed
in BACKLOG.md rather than worked around in varco code.

### Added

- **`varco_core.deprecated` / `varco_core.deprecated_alias`** (Plan 022, §D-DEP)
  — one deprecation mechanism for the workspace, replacing ad-hoc
  `warnings.warn` calls. Both require `removed_in=` at authoring time, which is
  the one discipline an ad-hoc warning cannot enforce and which makes every
  scheduled removal greppable. `deprecated_alias()` builds a PEP 562 module
  `__getattr__` returning the *identical* target object, never a subclass, so an
  alias never breaks `except` or `isinstance`. PEP 702's `warnings.deprecated`
  is the intended migration once the Python floor moves to 3.13 (it is stdlib
  only from 3.13, and every package here is `requires-python = ">=3.12"`).
- **`scripts/api_surface.py`** — a reproducible snapshot of every distribution
  package's public `__all__` (471 exports across ten packages), with `--check`
  to diff a live tree against the committed snapshot and fail on a removal or a
  function-signature change. Not yet a CI gate; run it by hand after touching
  any `__all__`.

### Fixed

- **`varco_redis.di.async_bootstrap()` raised `AttributeError` instead of
  no-op'ing when providify is absent** (Plan 022, RIDER-2's sibling RIDER-1).
  `bootstrap()` returns `None` on its own `except ImportError` path, and the
  next line called `container.ainstall(...)` on it — so `setup_cache=True`
  without providify installed crashed on exactly the path documented as a
  graceful no-op. It now returns `None`, matching `bootstrap()`'s contract.
- **Both admin-surface double-mount guards could silently skip mounting**
  (Plan 022, RIDER-2). `varco_fastapi.tenancy.mount` and
  `varco_fastapi.admin.mount` each kept a `set[int]` of `id(app)`. `id()` is
  unique only among *live* objects, so a garbage-collected app released an
  address that a new, unrelated `FastAPI` instance could reuse — matching a
  stale entry and having its admin surface **silently not mounted**. Both are
  now `weakref.WeakSet`, changed together so the two cannot drift.

- **`varco_beanie`'s `BeanieUnitOfWork(transactional=True)` silently opened no
  transaction at all.** A leftover motor→pymongo migration bug: pymongo's
  `AsyncClientSession.start_transaction()` is a coroutine (motor's was sync),
  and `_begin()` called it without `await`. The resulting never-awaited
  coroutine was a no-op, so every "transactional" UoW ran uncommitted/
  unprotected — `commit()`/`abort()` acted on a transaction that never
  existed, with no error raised anywhere. Fixed by awaiting
  `start_transaction()`. Anyone relying on `transactional=True` for
  multi-document atomicity should treat prior runs as having received no
  transactional guarantee.
- **`varco_beanie.uow.BeanieUnitOfWork._begin()` raised `TypeError` against a
  real MongoDB client.** `await self._client.start_session()` failed with
  `TypeError: object AsyncClientSession can't be used in 'await' expression`
  — pymongo's `AsyncMongoClient.start_session()` is a plain sync method,
  unlike motor's coroutine of the same name. Fixed by removing the `await`.
  Undetected by the existing unit tests because the mock client faked
  `start_session` as an `AsyncMock`, which made the incorrect `await` pass;
  the fakes and three regression tests now pin the real pymongo call shapes.
- **`varco_beanie.tenancy.pool.BeanieTenantPool` leaked a connection pool on
  every `client_per_tenant=True` tenant eviction.** `entry.owned_client.close()`
  was called without `await` — pymongo's `AsyncMongoClient.close()` is a
  coroutine (motor's was sync), so the call silently did nothing. Fixed by
  awaiting `close()`.

### Internal / typing — mypy strictness ramp complete (Plan 021, RL-14/RL-14b/RL-14c/RL-14d)

**Not a BREAKING change.** `[tool.mypy]`'s root config now carries `strict = true` (mypy 2.3.1's
13-flag bundle) plus `disallow_any_unimported = true` across all ten `varco_*` packages, closing
the strictness ramp Plan 020 started. Every one of the ~280 newly-surfaced errors was fixed by
annotating varco's own code (bare `dict`/`list`/`Callable` generics, missing return types,
`Any`-returning functions) — no public signature gained or lost a type parameter. In particular,
`AsyncVarcoClient`, `VarcoRouter`, `AsyncCache`, `ClientConfigurator`, `AsyncRepository`, and
`AbstractMapper` keep their exact current declarations; downstream code writing a bare
`AsyncVarcoClient` still means `AsyncVarcoClient[Any]`, unchanged (Plan 021 §D3). A handful of
genuinely-untypable third-party call sites (redis-py's `Pipeline.reset()`/`multi()`,
prometheus_client's `generate_latest()`, beanie's `Set` operator and `casbin`/`aiokafka`'s
unshipped type stubs) are narrow-suppressed with a reasoned `# type: ignore[<code>]` each — not
debt, per the existing `§RL-14-metric` convention. See BACKLOG.md's RL-14/RL-14b/RL-14c/RL-14d
rows (all closed) and `plans/021-mypy-strict-full-ramp.md` for the full per-flag remediation
record.

### Fixed — Plan 017 findings remediation (Plan 020, KI-9/KI-10/KI-11/RL-15/RL-17/RL-18/RL-19)

Pays off `xfail(strict=True)` markers and BACKLOG rows filed during Plan 017's CI-green pass.
See BACKLOG.md's "Plan 017 findings" table for full evidence.

- 🔒 **Security-relevant fix — `varco_beanie`'s audit trail is now tenant-scoped.**
  `BeanieAuditRepository.list_for_entity()` was missing the `tenant_id: str | None = None`
  keyword-only parameter the `AuditRepository` ABC declares (Plan 009 / R4) — any caller passing
  `tenant_id=` got a `TypeError`, and the underlying query filtered only on
  `(entity_type, entity_id)`, i.e. across every tenant. Fixed to mirror
  `varco_sa.audit.SAAuditRepository` exactly: `tenant_id=None` (default) preserves the
  pre-existing unscoped behaviour; any other value filters the query. (KI-9)
- **`BeanieFastrestApp`'s non-DI construction path now actually constructs.** It called
  `BeanieRepositoryProvider(mongo_client=…, db_name=…, transactional=…)`, but that class's real
  `__init__` only accepts an injected `settings: Inject[BeanieSettings]` — every call raised
  `TypeError`. Undetected because the class had zero test coverage. Fixed by building a
  `BeanieSettings` from `BeanieConfig`'s field-for-field-compatible attributes and passing
  `settings=`; the now-redundant second `self._provider.register(...)` call is removed (the
  provider's own `__init__` already registers `entity_classes`). The stale docstring in
  `BeanieRepositoryProvider` that caused this bug (documenting the deleted `mongo_client=`/
  `db_name=` call shape) is also fixed. (KI-10)
- ⚠️ **BREAKING — `MCPAdapter.to_mcp_server()` now returns a low-level `mcp.server.lowlevel.Server`,
  not a `FastMCP` instance.** Both of `to_mcp_server()`'s public entry points (it, and `mount()`
  which calls it) were dead code before this fix: the high-level `FastMCP.add_tool()` has never
  accepted an `input_schema=` parameter (SDK issue #761, open since May 2025) — it derives the
  tool schema from the handler's type hints, which cannot express varco's generic
  `execute(tool_name, arguments)` dispatch. Fixed by dropping to the low-level `Server` API with a
  new `_to_mcp_tools()` builder that passes varco's own JSON Schema dicts to `mcp.types.Tool`
  verbatim — no synthesis, no post-processing. `mount()` is rebuilt on `mcp.server.sse.SseServerTransport`
  directly (the low-level `Server` has no `sse_app()`/`asgi_app()` convenience method `FastMCP`
  had). The `mcp` extra is now upper-bounded: `mcp = ["mcp>=1.28.1,<2"]` (previously unbounded
  `>=1.0`, which would have auto-upgraded to the v2 line and removed this API entirely). Anyone
  calling `to_mcp_server()` directly and relying on `FastMCP`-specific methods must migrate to the
  low-level `Server` API. (KI-11)

⚠️ **BREAKING — eight public enums migrated from `class Foo(str, Enum)` to `enum.StrEnum`**
(RL-15, spent inside the 3.0.0 breaking-change window per BACKLOG's Locked decisions —
after 3.0.0 this becomes a major-version-only change): `PKStrategy`, `HealthStatus`,
`ErrorPolicy`, `DispatchMode`, `CircuitState`, `KafkaDeliverySemantics`,
`NatsDeliverySemantics`, `BackpressurePolicy` (plus `examples/19-resilience-payment-gateway`'s
`FailMode`). `StrEnum` undoes Python 3.11's `Enum.__format__` change (CPython #100458): a
downstream consumer that formats one of these enums now gets its intended value.

| Expression | Before | After |
|---|---|---|
| `f"{HealthStatus.HEALTHY}"` | `"HealthStatus.HEALTHY"` | `"healthy"` |
| `str(CircuitState.OPEN)` | `"CircuitState.OPEN"` | `"open"` |
| `"%s" % ErrorPolicy.FAIL_FAST` | `"ErrorPolicy.FAIL_FAST"` | `"fail_fast"` |

**Unchanged** (verified by `varco_core/tests/test_strenum_serialization.py`, run both before and
after migration): `.value` access, `json.dumps(member)` (stdlib `JSONEncoder` does an
`isinstance(obj, str)` check — identical output either way), and pydantic v2
`model_dump(mode="json")`/`BaseSettings` env-var parsing (both serialize/parse by value under
either form). No in-tree caller was affected — measured zero bare `{member}`-style
interpolations across the repo before migrating (BACKLOG EC-3). If you `str()`- or `%s`-log one
of these eight types outside varco, update any log-scraping regex that depended on the old
`ClassName.MEMBER` text.

- **Fixed — `make docs` had never rendered `varco_casbin`'s API reference.** `scripts/
  gen_ref_pages.py` carried its own hand-written package list (a fourth, previously
  unreported copy of the same triplication RL-18 fixes below) and it was missing
  `varco_casbin`. Fixed as a side effect of deriving the list from `[tool.uv.workspace]
  members`. (RL-18)
- **`scripts/packages.sh` (new) is now the single source of truth for the workspace package
  list**, derived from root `pyproject.toml`'s `[tool.uv.workspace] members` via stdlib
  `tomllib`. Replaces four independent hand-written copies (`Makefile`'s `PACKAGES`,
  `scripts/unit_tests.sh`, `scripts/integration_tests.sh`, `scripts/gen_ref_pages.py`) — the
  exact defect class that let `varco_casbin` go missing from `make lint`/`make docs` in the
  first place. `make print-packages` prints the derived list. Guarded by
  `varco_core/tests/test_repo_package_lists.py`. (RL-18)
- **`ruff format --check` is now a CI/`make lint` gate**, at zero `.py` reformatting churn
  (measured with the pinned `ruff==0.16.4`: 0 of 1107 files would change). Added to
  `.github/workflows/test.yml`'s `lint` job and the `.pre-commit-config.yaml` `ruff-format`
  hook, alongside the pre-existing `ruff check`. `[tool.ruff.format]` sets
  `docstring-code-format = false` explicitly to keep the formatter out of hand-wrapped
  `Usage::` docstring blocks. (RL-17)
- **`.pre-commit-config.yaml`'s ruff `rev` is now guarded against drifting from the
  `ruff==0.16.4` pin** in root `pyproject.toml`'s `[dependency-groups] lint` group, by
  `varco_core/tests/test_repo_tooling_pins.py` — the prior divergence (`v0.4.1` vs.
  `ruff==0.16.4`) broke the pre-commit hook silently on the next `git commit`. (RL-19)

### Fixed — Plan 018 findings remediation (Plan 019, RT2-B/RT2-C/RT7a/RT7b/RT9-beanie)

Pays off the `xfail(strict=True)` markers Plan 018 filed rather than fixed. Every marker removed
in the same commit as its fix — see BACKLOG.md's "Plan 018 findings" table for full evidence.

- ⚠️ **BREAKING (behaviour) — `varco_nats` `AT_LEAST_ONCE`/`EXACTLY_ONCE` now redeliver a
  message whose handler raised** (RT2-B). Previously a handler that merely raised (no process
  crash) was silently acked and never retried — only a crash triggered JetStream redelivery,
  contradicting the documented "at-least-once" guarantee. `_on_message` now `nak()`s on handler
  failure (immediate redelivery) and `term()`s once `msg.metadata.num_delivered` reaches the new
  `NatsEventBusSettings.max_deliver` (default 5, env `VARCO_NATS_MAX_DELIVER`) — JetStream's own
  default is *unlimited* redelivery, so this bound is load-bearing, not polish. A deserialization
  failure is always `term()`ed regardless of delivery count. **A non-idempotent NATS handler
  with no `@listen(retry_policy=...)` may now see up to `max_deliver` deliveries where it
  previously saw exactly one.** `ErrorPolicy.FIRE_FORGET` opts out of this (its handler
  exceptions never leave `_dispatch`, so the bus still observes a "successful" dispatch and
  acks) — documented in `varco_nats/README.md` and CLAUDE.md's pitfall table.
- **`VARCO_NATS_ACK_WAIT_SECONDS` now actually reaches the broker.** Previously read from env
  but never passed to `js.subscribe()` — dead configuration. Fixed as part of the same
  `_open_jetstream_consumer` call site RT2-B needed anyway (`config=ConsumerConfig(ack_wait=...,
  max_deliver=...)`).
- ⚠️ **BREAKING (behaviour) — `NatsStreamManager.channel_exists()`/`list_channels()` now report
  declared channels, not only channels currently carrying messages** (RT2-C). Previously a
  freshly `declare_channel()`d, empty channel reported as not existing — violating the
  `ChannelManager` ABC's own documented "declare implies exists" contract, and disagreeing with
  Kafka's and Redis's conformant implementations. `NatsStreamManager` now tracks a process-local
  declaration registry (`declare_channel(c)` records `c`; `delete_channel(c)` discards it) layered
  on top of broker evidence, so a channel declared by *another* process remains discoverable once
  it carries data. The old "does the subject currently carry a message?" predicate is preserved
  under an honest name, `channel_has_messages()` (NATS-only, not on the ABC). New
  `testkit/varco_conformance/channel_manager.py` — the fifth conformance module — machine-checks
  the round-trip across Kafka, Redis, and NATS.
- **`RedisJobStore` releases its claim guard on reap, and stops leaking it on every failed
  `try_claim`** (RT7a). `reap_expired_leases()` now deletes the `SET NX EX` claim-guard key for
  each job it actually reaps, immediately after the `save()` that advances `lease_epoch` — a
  second worker's legitimate re-claim was previously refused for up to `claim_ttl` (default 30s)
  after a correct reap. `try_claim()` also now releases its guard on every non-success exit (a
  missing job, a non-PENDING job), not only the future-`run_at` and exception branches it
  covered before — closing a guard-key leak `SAJobStore` never exhibited.
- **Chaos test infrastructure — `ChaosContainer` owns its connection URL** (RT7b). Prior guidance
  (research 002 §1) that docker-py's `restart()` preserves a container's host port mapping is
  wrong — Docker documents the port as re-allocatable on every restart (research 006), confirmed
  in this session's environment. `ChaosContainer` gains a `url_factory` constructor argument and
  a `url` property that re-derives the connection URL fresh on **every** access, never memoised,
  making it structurally impossible for a chaos test to hold a stale DSN across a `restart()`.
  The three restart-based chaos modules (`test_sa_chaos.py`, `test_kafka_chaos.py`,
  `test_migration_chaos.py`) now read `chaos.url` instead of a DSN captured once at fixture
  boot; Kafka additionally pins its host port (`testkit/varco_chaos/ports.py`'s
  `reserve_host_port()`) because its advertised listener is baked into an on-disk script at
  first boot, so re-querying alone cannot fix it. Test-only — no runtime package changed.
- **`test_beanie_migration_integration.py` (new)** — real-`mongod` coverage for
  `MigrationStore`'s lock (`find_one_and_update` + upsert + `_id`-uniqueness, already the
  research-007-sanctioned pattern) and `BeanieMigrator`'s index-mode lifecycle: concurrent
  migrators serialize correctly, a crashed holder's lock is reclaimed at its `expires_at` (no
  TTL index involved — deliberately, seconds-scale not the 60-120s a TTL monitor would need),
  and a racing upsert's `DuplicateKeyError` is read as "lock lost". One genuine, pre-existing
  `BeanieMigrator` defect found and filed as `xfail(strict=True)` + BACKLOG (not fixed — outside
  this release's four-row licence): `upgrade()` returns early on an empty/fully-applied
  `MigrationRegistry`, before ever reaching its `index_mode="create"` reconciliation, so a
  missing index is silently never created even though `plan()` independently reports the drift.

### Added — real-broker reliability coverage + chaos test suite (Plan 018, RT2/RT3/RT4/RT5/RT7/RT9)

**Test-only release — no runtime package changed.** Every change in this section lives under a
`tests/` directory, `testkit/`, `scripts/`, the `Makefile`, or CI configuration.

- **`testkit/varco_chaos` (new)** — shared, never-packaged chaos-test helpers, the same shape as
  `testkit/varco_conformance`: `ChaosContainer` (`restart()`/`paused()`/`wait_ready()`, the only
  sanctioned caller of `DockerContainer.get_wrapped_container()` in the repo) and
  `abandon_lease()` (the shared worker-crash helper for the job-lease-fencing tests). Opted into
  via each participating package's existing `pythonpath = ["../testkit"]`.
- **New `chaos` pytest marker** — additive to `integration`, registered in
  `varco_kafka`/`varco_redis`/`varco_sa`/`varco_fastapi`/`varco_nats`'s
  `[tool.pytest.ini_options]`. `scripts/integration_tests.sh` now defaults
  `MARKER_EXPR="integration and not chaos"`, so `make integration-test` excludes chaos tests by
  default; `make chaos-test` / `make chaos-test-clean` (new `Makefile` targets, mirroring
  `integration-test`/`integration-test-clean`) run them explicitly.
- **`.github/workflows/integration.yml`'s new `chaos` job** — runs `make chaos-test-clean`,
  gated `if: github.event_name != 'push'` (nightly `schedule` + `workflow_dispatch` only, never
  on `push: main`). Independent of the existing `integration` job; never a required check.
- **Real-broker coverage added across five packages**: `varco_nats` (delivery-semantics
  round-trips, `NatsStreamManager` channel lifecycle, DLQ ack durability, a health-check chaos
  test), `varco_casbin` (concurrent-writer correctness against real Postgres), `varco_ws`
  (backpressure and the `DISCONNECT` ejection policy over a real ASGI socket), `varco_kafka`
  (exactly-once/at-least-once/at-most-once observable semantics against a real broker, deepened
  rebalance and offset-durability coverage), `varco_sa`/`varco_redis`/`varco_fastapi`
  (job-lease fencing after a simulated worker crash; a deterministic app-layer migration-lock
  timeout).
- **New chaos tests** (nightly-only, `varco_kafka`/`varco_sa`/`varco_redis`/`varco_fastapi`) —
  outbox durability across a real broker restart and a real database restart, a shared
  `CircuitBreaker` opening/recovering around a black-holed Redis dependency, and a crashed
  migration-lock holder correctly releasing so the next boot proceeds.
- **`varco_kafka/tests/conftest.py`** — the shared `kafka_bootstrap` fixture now forces
  `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1` and `_MIN_ISR=1`. A single-broker
  `KafkaContainer` cannot create Kafka's internal transaction-state topic at the default
  replication factor of 3, which hung every exactly-once-semantics test indefinitely before this
  fix — a test-fixture-only change, no production code affected.
- **Findings from real-broker testing, recorded as `xfail(strict=True)` + a BACKLOG.md row each,
  never a production-code fix** (per the repo's standing "a red integration test is a finding"
  rule): NATS `AT_LEAST_ONCE` does not redeliver after a handler merely raises (only on a
  process crash); NATS `ChannelManager.channel_exists()`/`list_channels()` implement a
  "has messages" predicate rather than an "exists" predicate; `RedisJobStore.reap_expired_leases()`
  does not release the claim-guard key `try_claim()` created, so a legitimate re-claim can be
  refused for up to `claim_ttl` seconds after a correct reap (`SAJobStore` does not exhibit this
  — a genuine cross-backend disagreement). See BACKLOG.md's Phase 3 table and its "Plan 018
  findings" subsection for full detail and evidence.

### Added — live CI, workspace lint/type gates (Plan 017, RL-5 / RL-6)

- **Two GitHub Actions workflows are now live** — `.github/workflows/test.yml` (lint + mypy +
  unit tests across a `[3.12, 3.13]` matrix, aggregated into a single `all-green` required
  check) and `.github/workflows/integration.yml` (testcontainers-backed integration suite on
  push-to-`main`, nightly, and manual dispatch). Both were previously 100% commented-out
  skeletons — nothing ran automatically before this release.
- **`ruff format` replaces `black`; ruff now owns both linting and formatting.** The
  `psf/black` pre-commit hook was removed and `ruff-format` added alongside `ruff-check`. black
  formatted to its 88-column default while `[tool.ruff]` pins `line-length = 100`, so the two
  had been in silent conflict; reconciling them cost one 618-file reformat, recorded in
  `.git-blame-ignore-revs`. ⚠️ **This reflows almost every source file** — rebase in-flight
  branches across the sweep commit rather than merging through it.
- **`mypy` now runs as a pre-commit hook**, via `make type-check` rather than `mirrors-mypy`, so
  the hook resolves the same pinned mypy that `uv.lock` and CI resolve instead of installing its
  own into an isolated venv.
- **Root `[tool.ruff]` and `[tool.mypy]` configuration** — the workspace now has real,
  version-pinned lint and type-check gates (`ruff==0.16.4`, `mypy==2.3.1`, declared in a new
  `[dependency-groups] lint` group and pulled into `dev` via PEP 735). Previously neither tool
  was declared as a dependency anywhere and both ran on ambient/unpinned versions if run at all.
- **`scripts/unit_tests.sh` (new)** — the shared entry point for `make test` and CI's `unit` job.
  Runs all ten packages plus the `examples/00-full-stack-post-api` suite and *accumulates*
  pass/fail/skip into one summary instead of aborting at the first red package (the previous
  `Makefile` loop used `|| exit 1`, hiding every failure after the first).
- **`.git-blame-ignore-revs` (new)** — lists the mechanical ruff autofix sweep commits so
  `git blame` skips them; run `git config blame.ignoreRevsFile .git-blame-ignore-revs` once per
  clone.
- **`varco_nats` now ships `py.typed`.** `varco_nats/pyproject.toml` has always declared the
  `"Typing :: Typed"` classifier, but the marker file itself was missing from the package —
  every downstream type-checker consuming `varco_nats` was silently treating its public API as
  untyped (`Any`), regardless of the classifier's promise. Fixed and wheel-verified; this is the
  only package that changes what a downstream type-checker sees.
- **`varco_casbin` added to the `Makefile` package list.** It was silently excluded from `make
  lint`, `make format`, `make type-check`, `make test`, `make build`, and `make publish` — it is
  now linted, type-checked, unit-tested, and built for the first time via the shared `make`
  targets (it was always covered by its own `varco_casbin/tests/` suite run directly, just never
  through the aggregate `make`/CI entry points).
- **`.pre-commit-config.yaml`'s ruff hook bumped `v0.4.1` → `v0.16.4`** — unplanned but required:
  `v0.4.1` predates the `UP046`/`UP047` rule codes now referenced in the new `[tool.ruff.lint]`
  ignore list and could not parse the config, blocking every commit.

### Fixed — runtime bugs surfaced by the new mypy gate (Plan 017, RL-6)

- **`varco_fastapi`'s `RequestContextMiddleware`** — `_maybe_tenant_context()` used `async with`
  on `varco_core.service.tenant.tenant_context()`, which is a **synchronous** context manager.
  Every request carrying a `tenant_id` raised `AttributeError` at this point. Fixed to `with`.
- **`varco_fastapi`'s A2A custom-route dispatch** (`router/a2a/router_source.py`) called a
  nonexistent `client.request(...)`; fixed to the real internal `client._request(method=,
  path=, path_params=, body=)`.
- **`varco_core.tenancy.control.consumer`** — the no-DLQ fallback path constructed
  `RetryExhaustedError` without its required `attempts`/`last_exc` arguments, raising `TypeError`
  instead of the intended retry error on that path.
- **`varco_core.job.base.enqueue_via()`** — the `overlap:` parameter was annotated with the wrong
  enum (`GapPolicy` instead of `OverlapPolicy`), and a `run_at_wall is None` guard was missing
  when `tz=` was supplied.

### Fixed — `examples/00-full-stack-post-api` unit-test double drifted from the UoW contract

- **`InMemoryUoW` (`example/tests/test_post_service.py`) now subclasses `AsyncUnitOfWork` and
  exposes the repository as `uow.posts`.** The double previously duck-typed the interface and
  offered a fictional `get_repository(entity_cls)` accessor — a method no `AsyncUnitOfWork` in
  `varco_core`/`varco_sa`/`varco_beanie` defines; only `RepositoryProvider` has it. Production
  code (`PostService._get_repo()`) was already correctly calling `uow.posts`, per the
  attribute-per-entity contract `RepositoryProvider.make_uow()` documents, so every CRUD test
  failed with `AttributeError: 'InMemoryUoW' object has no attribute 'posts'` (8/11 tests in the
  package, turning the CI `unit` job and the required `all-green` check red on `main`). The bug
  predates Plan 017 — it was merely made visible once `scripts/unit_tests.sh` started running
  the examples suite. No production code changed; five regression tests were added.

- **`providify>=2.0.0`** across all ten workspace members (`varco_core`,
  `varco_kafka`, `varco_nats`, `varco_redis`, `varco_beanie`, `varco_sa`,
  `varco_memcached`, `varco_ws`, `varco_fastapi`, `varco_casbin`), up from
  `>=1.1.0`.
- **The vendored-wheel `[tool.uv.sources]` override is removed** from the
  workspace root `pyproject.toml` — `providify` now resolves from PyPI for
  every workspace member instead of a locally-built wheel checked into
  `vendor/`.
- **`varco-ws` gains an explicit `providify` dependency.** It imports
  `providify` directly (`varco_ws/sse.py`, `varco_ws/websocket.py`) but
  previously declared no such dependency, relying entirely on the
  transitive dependency via `varco-core`. This is a strictly additive
  metadata fix — the dependency was already installed in practice.

### Removed — internal compat shim deleted; adopts providify 2.0.0's native override (Plan 016, Phase C)

- **Deleted `varco_core.providify_compat` (its `provide_factory()` helper)** —
  the internal compat module that patched a factory's return annotation by
  hand before registering it with providify. The only reason it existed was
  that providify had no supported way to state a factory's interface
  explicitly. Every former call site (`varco_ws/di.py`,
  `varco_fastapi/di.py`, `varco_fastapi/router/skill.py`,
  `varco_fastapi/router/mcp.py`, `varco_sa/di.py`, `varco_beanie/di.py`) now
  calls providify's native `container.provide(Provider(...)(factory),
  returns=...)` / `Provider(returns=...)(factory)` directly — no annotation
  patching. Closes UPSTREAM-GAPS.md U-20. No behaviour change; purely a
  mechanical migration off a now-removed internal shim.

### Documentation — repo-wide restructure (Plan 015, audit 002 F1–F11)

`CLAUDE.md` reduced from ~2020 lines to ~744 lines of agent guidance
(commands, layer rules, DI verb taxonomy, code-pattern pitfall table,
Decision Tree, Pre-Implementation Checklist). Package map / dependency
graph / type hierarchies consolidated into `ARCHITECTURE.md`. New README.md
sections with runnable usage examples: Profiling, Background Jobs, Database
Auditing, Dead Letter Queue, Composite Deployment, Durability preset
(one-line opt-in), plus an A2A non-router-subject subsection and a
Verification-hardening (`VARCO_JWT_*`) env-var table. Feature-specific
operator pitfalls moved out of `CLAUDE.md`'s pitfall table into a
`## Pitfalls` section on each owning `technical_docs/features/*.md` file
(created where missing). No runtime code changes.

### Fixed / Added — `varco-fastapi`, `varco-memcached`

Plan 014 (audit 001 Batch B) — three small, additive DI/wiring fixes. No
existing correct call site changes behaviour.

- **Fixed: `_try_resolve_component()` no longer swallows every lifecycle
  discovery failure into `except Exception: pass`.** Each skipped
  component now produces exactly one log line naming the module/class:
  `ModuleNotFoundError` → DEBUG ("package not installed"), `AttributeError`
  → WARNING ("module present but has no `<class>`" — a version-skew
  signal previously indistinguishable from "not installed"), a genuinely
  missing binding (`is_resolvable() is False`) → WARNING naming the remedy
  (`call <package>.bootstrap(container)` before `create_varco_app()`), a
  `LookupError`/other exception from `container.get()` → WARNING (the
  latter with `exc_info=True`). **Control flow is unchanged** — nothing new
  propagates out of `_try_resolve_component()`; a broken/missing lifecycle
  binding still does not stop the app from starting. New kill switch:
  `VARCO_LIFECYCLE_DISCOVERY_WARN` (default `true`) demotes the
  missing-binding WARNING to DEBUG. The two `varco_ws` push adapters pass
  `warn_if_missing=False` (they are genuinely opt-in — warning about them
  would be pure noise for every app that doesn't use `varco_ws`);
  `AbstractEventBus`/`AbstractJobRunner` keep the default `True`.
- **Fixed: `mount_reliability_admin()` now refuses a double mount.** Ports
  the same `id(app)`-keyed `_MOUNTED_APPS` guard `mount_tenant_admin()`
  already has — a second call for the same app now raises `ValueError`
  instead of silently duplicating routes (previously: same `prefix` doubled
  the mounted routes; a different `prefix` produced a second, fully live
  admin surface with no signal). One deliberate deviation from
  `mount_tenant_admin()`: calling with neither `audit_repo` nor `dlq` given
  mounts nothing and does **not** poison the app for a later real mount —
  the app id is only recorded once at least one router was actually
  included.
- **Added: `varco_memcached.di.async_bootstrap(setup_cache: bool = True)`.**
  Mirrors `varco_redis.di.async_bootstrap(..., setup_cache=...)` in *shape*.
  **The memcached default is unchanged** — `setup_cache=True` (the default)
  reproduces today's unconditional `await
  container.ainstall(MemcachedCacheConfiguration)` byte-for-byte for every
  existing `await async_bootstrap(container)` call site.
  `setup_cache=False` is now available for callers who want the sync scan
  only, matching how `varco_redis`'s `async_bootstrap` behaves with
  `setup_cache=False` — no connection pool is opened, no `CacheBackend`
  binding is installed.

### Changed — `varco-kafka`, `varco-nats`, `varco-redis`, `varco-core`, `varco-ws`, `varco-sa`, `varco-beanie`, `varco-fastapi`

Plan 014 (audit 001 Batches C + D) — internal DI-wiring refactor, no public
API change.

- **Changed: `KafkaEventBusSettings`/`NatsEventBusSettings`/`RedisEventBusSettings`
  are now registered by a `@Provider` factory instead of `@Singleton` on the
  class.** Consistency/robustness fix, not a bug fix — a characterization
  test (`varco_{kafka,nats,redis}/tests/test_*_di.py`) proved all three
  resolved correctly through the container even under `@Singleton` on
  today's providify (>= 1.1.0 skips pydantic's `**values` ctor param
  outright), but the shape contradicted CLAUDE.md's own pitfall table and
  each package's own sibling settings factory. `priority=-sys.maxsize` is
  preserved exactly; base-interface lookup (`container.get(EventBusSettings)`)
  and app-override-wins both still hold, proven by new regression tests.
- **Added, then later deleted (Plan 016 / RL-2, same `[Unreleased]` section):**
  `varco_core.providify_compat.provide_factory()`, a compat helper that
  replaced six of the seven independently hand-rolled
  `factory.__annotations__["return"] = ...` + `@Provider` +
  `container.provide()` closures found across four packages (audit F8; the
  audit itself named 5, two more were found during this plan's inventory)
  with one shared function. `varco_beanie.di`'s `_make_repo_provider()` was
  the one documented exception — it stayed a container-less builder because
  its tests import it directly and assert on its patched, unregistered
  `__annotations__`/`__name__`. The helper was never re-exported from
  `varco_core.__init__` and declared no bindings itself. Superseded by
  providify 2.0.0's native `@Provider(returns=...)` / `container.provide(fn,
  returns=...)` — see the "Removed" entry above and UPSTREAM-GAPS.md U-20.

### BREAKING (security default) — `varco-core`, `varco-fastapi`

Plan 005 Phase 2 (U-13, fail-closed JWT verification). Two fail-open holes
are now fail-closed by default:

- **`TrustedIssuerRegistry.verify()` now enforces the token's `iss` claim**
  against the resolved issuer's registered value. Previously `iss` was never
  checked here at all — a token signed by issuer A's key but claiming `iss`
  of issuer B verified successfully. **Rollback:** set
  `VARCO_JWT_ENFORCE_ISS=false`, or pass `enforce_issuer=False` to a specific
  `verify()` call.
- **`JwtBearerAuth` now refuses to construct** (`ValueError`) when no
  audience is configured (`audience=` kwarg omitted and `VARCO_JWT_AUDIENCE`
  unset). Previously this logged one warning and proceeded, accepting a
  token minted for any audience by any registered issuer. **Rollback:** set
  `VARCO_JWT_AUDIENCE=<your-service>`, or explicitly opt out with
  `allow_any_audience=True` / `VARCO_JWT_ALLOW_ANY_AUDIENCE=true` to restore
  the old warn-and-proceed behaviour.

  A caller who explicitly wrote `audience=None` (as opposed to omitting the
  kwarg) keeps working unmodified — that remains a deliberate per-instance
  opt-out, distinct from the process-wide `allow_any_audience` escape hatch.

**Why this is worth a breaking default**: a service that forgets one
environment variable used to silently accept a token minted for any
audience by any registered issuer. A startup failure is the control a log
warning was not.

### Changed (behaviour, non-security)

- **⚠️ `TenantProvisionConsumer`'s constructor changed — `provisioner=` is
  deprecated** (Plan 008, Phase 1, RD-11/RD-12). `TenantProvisionConsumer(
  provisioner=..., dlq=...)` no longer works as-is; construct with
  `control_service=TenantControlService(catalog=..., provisioner=..., producer=...)`
  instead — one catalog writer, shared with the REST admin surface. A one-release
  shim keeps `provisioner=`+`catalog=` working (raises `DeprecationWarning` and
  builds the `TenantControlService` internally); `provisioner=` **without**
  `catalog=` raises `ValueError` at construction naming the fix, rather than
  reproducing the unroutable-tenant defect the constructor change exists to close.
  **Rollback:** none — the old bare-`provisioner=` behaviour was itself the bug
  being fixed. See `technical_docs/features/multitenancy.md`'s migration box.

- **`AuditConsumer` now retries by default.** `_default_retry_policy` is
  `RetryPolicy.durable_delivery()` (`max_attempts=20, base_delay=15.0,
  max_delay=3600.0`) instead of no retry policy at all — an audit-persistence
  failure (DB hiccup, connection drop) is now retried for minutes before
  giving up, instead of being logged once and dropped. **Only the failure
  path changes** — a successfully-persisted `AuditEvent` behaves identically.
  **Rollback:** pass `retry_policy=None` explicitly to `register_to()` to
  restore the old fire-and-forget behaviour. See
  `technical_docs/features/database-auditing.md`.

### Added

- **Internationalization, timezones, and cache bulk operations** (Plan 011).
  Every item is off by default, reached only by an explicit setting/object —
  with one deliberate exception, called out below.

  - **`varco-core`** — `varco_core.context` (X1): `AmbientVar[T]` (the
    generic ambient-value primitive `tenant_context()`/
    `correlation_context()` already implemented independently) +
    `RequestContext` (one aggregate `locale`/`timezone`/`extras` value,
    merge-on-nest semantics) + `resolve_precedence()` (the "first non-`None`
    wins, with source attribution" helper I2/T1 both consume). Tenant is
    deliberately absent from `RequestContext` — `current_tenant()` stays the
    single source of truth. `varco_core.i18n` (I2): `MessageCatalog` ABC +
    `NullMessageCatalog`/`DictMessageCatalog`/`GettextMessageCatalog`
    (stdlib `gettext` only, zero new runtime dependency), a hand-rolled RFC
    4647 §3.4 Lookup negotiator, and a five-source locale precedence chain
    (`?lang=` → user profile → tenant default → `Accept-Language` →
    fallback). `varco_core.tz` (T1/T2/T3): `resolve_timezone()`
    (`?tz=`/`X-Timezone`/user profile/tenant default/fallback),
    `resolve_zoned()` (DST gap/overlap detection with no `dateutil`
    dependency — defaults `GapPolicy.NEXT_VALID`/`OverlapPolicy.FIRST`),
    `format_rfc9557()` (output-only — no parser ships), and
    `DatetimeCoercionPolicy` (`assume="naive"` by default, `"utc"`
    recommended, `"context"` opt-in) for the query layer.
    `varco_core.context.defaults.TenantDefaultsProvider` resolves per-tenant
    locale/timezone defaults **without** a `varco_tenants` schema change.
    `Job` gains three additive fields (`run_at_wall`/`run_at_tz`/
    `run_at_fold`) — `run_at` keeps its exact current meaning as the
    materialized UTC claim predicate; `AbstractJobStore.
    supports_zoned_schedules: ClassVar[bool] = False` is the RD-5 opt-in
    gate a store must declare before a zoned schedule may target it.
    `varco_core.job.reschedule.ScheduleRematerializer` is the opt-in
    recompute-on-read sweeper (`interval=0.0` default = never started).
    Every built-in `ServiceException` gains a `message_key: ClassVar[str |
    None]` (e.g. `varco.error.not_found`) and an `error_params()` method —
    `code` stays the stable machine identifier, `message_key` is the new
    i18n key; `VarcoErrorCodes = FastrestErrorCodes` is a bare alias to the
    same enum object (the backlog's `VARCO_XXXX` naming does not exist and
    the codes are **not** renamed). `varco_core.cache.base.BulkCache`
    (`get_many`/`set_many`/`delete_many`) is a **separate**
    `runtime_checkable` Protocol from `AsyncCache` (adding methods to
    `AsyncCache` itself would silently break `isinstance()` for out-of-tree
    caches); `CacheBackend` ships concrete, portable loop-based defaults, so
    every existing backend satisfies `BulkCache` immediately.
    `CacheBackend(serializer=)` reuses the existing
    `varco_core.serialization.Serializer` Protocol. `read_through_many()`
    shares `Singleflight` slots with `read_through()`.
  - **`varco-fastapi`** — `varco_fastapi.middleware.localization.
    LocalizationMiddleware` resolves locale and/or timezone in one ASGI
    pass with two independent toggles; `create_varco_app(i18n=, timezone=)`
    (both typed `Any | None`, resolved via `isinstance()` — not
    type-checked keywords) wires it plus an opt-in `I18nLifecycle` that
    starts/stops the DI-bound `MessageCatalog`. Default DI bindings:
    `NullMessageCatalog`, `NullTenantDefaults`, off-by-default
    `I18nSettings`/`TimezoneSettings`.
  - **`varco-sa` / `varco-beanie`** — `SAJobStore`/`BeanieJobStore` persist
    the three zoned-schedule columns/fields and set
    `supports_zoned_schedules = True`; `SAJobStore.list_pending_zoned()` is
    a native, indexed override of the portable default.
  - **`varco-redis` / `varco-memcached`** — native `MGET`/pipelined-`SET`
    and `get_multi`/`set_multi` overrides of `BulkCache`'s portable
    defaults.

  ⚠️ **The one deliberate exception to "off by default" (D-4)**: a
  built-in `ServiceException`'s JSON error body now includes up to two new
  keys, `message_key` and `params`, **by default** — an out-of-tree
  exception with no `message_key` set is unaffected (byte-identical body).
  **Rollback:** `VARCO_ERROR_INCLUDE_MESSAGE_KEY=false` /
  `VARCO_ERROR_INCLUDE_PARAMS=false` restores the exact pre-plan body.

  ⚠️ **Known gaps, not yet wired end-to-end** (see the linked feature docs
  for detail): the `message_resolver=` seam `error_message_for()` exposes
  is not called by either shipped HTTP error-rendering path, so a built-in
  error's `message` text is never actually catalog-localized today, and an
  error response never carries `Content-Language`; `DatetimeCoercionPolicy`
  is honoured by `coerce_datetime()` directly but not by the AST visitor
  the query pipeline actually drives (`ASTTypeCoercion`); the shipped
  `JobRunner.enqueue()` was not extended with the new zoned-schedule
  keyword arguments (construct the `Job` with `run_at_wall`/`run_at_tz` set
  directly); and `CacheServiceMixin._use_bulk_cache` is a flag with no
  reader — `list()` does not yet take the `BulkCache` batch path.

  See `technical_docs/features/i18n-and-localization.md`,
  `technical_docs/features/timezone-handling.md`,
  `technical_docs/features/error-taxonomy-and-i18n.md`, and
  `technical_docs/features/cache-hardening.md`'s "Bulk operations" /
  `technical_docs/features/job-scheduling-and-leases.md`'s "Zoned
  schedules" sections.

- **Cache hardening: singleflight, L1 coherence backplane, observability,
  stale-while-revalidate/jitter/negative caching** (Plan 010). Every default
  reproduces pre-Plan-010 behaviour byte-for-byte — reached only by passing
  an explicit object or keyword.

  - **`varco-core`** — `varco_core.cache.policy.CachePolicy` (frozen: `ttl`,
    `ttl_jitter`, `soft_ttl`, `negative_ttl`, `stale_if_error`,
    `singleflight`, `refresh_mode`) and `varco_core.cache.envelope.
    CacheEnvelope` (the wire format written only when the policy needs it)
    drive one shared algorithm, `varco_core.cache.readthrough.
    read_through()`. `varco_core.cache.singleflight.Singleflight` coalesces
    concurrent misses for the same key into one recompute per process
    (per-process only — `SingleflightProtocol` is the seam for a future
    distributed implementation). `varco_core.cache.backplane.CacheBackplane`
    (ABC) + `InMemoryBackplane` (test double) is the cross-node L1
    invalidation channel for `LayeredCache(backplane=...)` — construction
    now raises `ValueError` if `backplane` is given without `promote_ttl`
    (bounds how long a missed, fire-and-forget invalidation can leave L1
    stale). `varco_core.observability.cache.install_cache_metrics()` adds
    `varco.cache.{hits,misses,evictions,duration,stampede_suppressed,
    stale_served,backplane.published,backplane.received,backplane.dropped}`
    — a manual install function, same shape as
    `install_reliability_metrics()`. `@cached(policy=, singleflight=)` and
    `CacheServiceMixin._cache_policy` are the two call sites wired through
    `read_through()`.
  - **`varco-redis`** — `varco_redis.backplane.RedisPubSubBackplane`, the
    concrete Redis Pub/Sub `CacheBackplane` (RESP3 `CLIENT TRACKING` was
    evaluated and rejected — no `redis.asyncio` support, redis-py issue
    #3916 open with no ETA). Self-echo suppression, flush-L1-on-reconnect,
    and two key-name-exposure opt-outs (`channel_for=`, `hash_keys=`) for
    per-tenant-pod topologies.

  See `technical_docs/features/cache-hardening.md` for the full design,
  including the two-step rolling-deploy recipe required before enabling an
  envelope-requiring policy field (`soft_ttl`/`negative_ttl`/
  `stale_if_error`) against a shared L2 cache.

- **Multitenancy: selectable isolation strategies, a dynamic tenant control
  plane, and global/shared scope** (Plan 007). New across four packages:

  - **`varco-core`** — `varco_core.tenancy`: three `TenantIsolation` values
    (`SHARED` — today's behaviour, unchanged default; `SCHEMA` — Postgres
    schema-per-tenant; `DATABASE` — Postgres/Mongo database-per-tenant),
    `enforce_rls: bool` as an *additive* hardening flag on `SHARED` rather
    than a fourth enum value, and an orthogonal `TenantScope`
    (`TENANT`/`GLOBAL`) for shared reference data under every strategy.
    `TenancySettings.from_env()` reads the `VARCO_TENANCY_*` variables.
    `AbstractTenantCatalog`/`StaticTenantCatalog`/`CachedTenantCatalog` (the
    latter combining event invalidation, a TTL backstop, and read-through-
    on-miss for cross-pod visibility), `TenantResourcePool[T]` (bounded,
    LRU-evicting, lease-refcounted — never evicts a resource mid-request),
    `DynamicTenantUoWProvider` (a *new* `IUoWProvider`, leaving
    `TenantUoWProvider` untouched), `GlobalUoWProvider` (a distinct DI-token
    type for the non-tenant-routed global scope — no change to
    `IUoWProvider`'s ABC), `AbstractTenantProvisioner`/
    `ExternalTenantProvisioner` (the no-op/DBA-workflow path),
    `validate_service_scope()` and `tenancy_cache_key()` (both catch the
    `TENANT`/`GLOBAL` mismatch pitfalls in either direction), and
    `TenantFanoutSupervisor` (one supervised `OutboxRelay`/job-poller/audit-
    consumer per active, pool-resident tenant under `DATABASE`). A tenant
    control plane (`varco_core.tenancy.control`) adds
    `TenantProvisionRequested`/`TenantDeprovisionRequested`/
    `TenantCatalogChanged` events, `TenantControlService` (idempotent
    `provision`/`deprovision`/`suspend`/`resume`), and
    `TenantProvisionConsumer` (safe-by-default: `RetryPolicy.
    durable_delivery()` + DLQ, following `AuditConsumer`'s precedent).
    `TenantFanoutMigrator` (`varco_core.migration.fanout`) runs the
    global/framework migration **before** every tenant's, in sorted order.
    A new `varco tenant` CLI verb group (`provision`, `deprovision`, `list`)
    plus `--all-tenants`/`--tenant`/`--skip-global` flags on `varco migrate`.
  - **`varco-sa`** — `varco_sa.tenancy`: `SASchemaRouter` (schema-per-tenant
    via SQLAlchemy's `schema_translate_map`, chosen over `SET LOCAL
    search_path` because a forgotten routing call fails **closed**, not
    open — see `technical_docs/features/postgres-rls.md` §3),
    `SAEngineRegistry` (bounded per-tenant `AsyncEngine` pool),
    `SASchemaProvisioner`/`SADatabaseProvisioner` (the latter confined to an
    explicit `VARCO_TENANCY_ADMIN_DSN`-backed `SAAdminEngine`, refusing an
    admin DSN equal to the app's own request-path engine), `SATenantCatalog`
    (the durable catalog backing `varco_tenants`, the **tenth** framework
    table), `assert_rls_enabled()` (assert-only, never DDL; skips `GLOBAL`
    and framework tables), and the SQLSTATE `42501` →
    `GlobalScopeReadOnlyError` translation (RD-10: the app-facing global
    credential is read-only by default). `SAModelFactory.build(...,
    isolation=...)` stamps a symbolic schema token onto `TENANT`-scoped
    models under `SCHEMA` — under the default `SHARED`, generated
    `__table__.schema` stays `None`, byte-identical to today.
  - **`varco-beanie`** — `varco_beanie.tenancy`: `BeanieTenantPool`/
    `BeanieTenantBinding` (per-tenant Document class **clones** + one
    `init_beanie()` call per tenant database — `BeanieDocRegistry.get(User)`
    keeps returning the base class, the documented contract),
    `BeanieDatabaseProvisioner` (the per-tenant `dropDatabase` GDPR-erasure
    primitive), and `BeanieTenantCatalog`. `TenantIsolation.SCHEMA` raises
    `ValueError` at construction — MongoDB has no schema-per-tenant
    equivalent.
  - **`varco-fastapi`** — `varco_fastapi.tenancy`: `TenancyLifecycle`
    (prepended into `VarcoLifespan`, stopping the fan-out supervisor
    **before** `pool.aclose()`), `build_tenant_router()` (the REST
    provisioning admin surface — every route 403s on an unauthorised
    caller, never 500), `mount_tenant_admin()` (the **only** way to expose
    the admin surface in a bundled deployment — `acknowledge_bundled_admin`
    is mandatory and there is deliberately **no**
    `VARCO_TENANCY_MOUNT_ADMIN` env var anywhere in the codebase), and
    `TenantResolutionMiddleware` (checks catalog status **before**
    `pool.ensure()` — a non-`active` tenant never causes an engine/binding
    to be created). A new `create_varco_app(tenancy=...)` parameter,
    `None` by default (registers nothing).

  **Nothing runs by default.** `TenancySettings()` defaults to
  `isolation=SHARED`, `enforce_rls=False`, every model `TenantScope.TENANT`,
  `fanout_framework_tables=False` — no pool, no extra engine/client, no
  symbolic schema, no control-plane surface. See
  `technical_docs/features/multitenancy.md` for the decision table, all six
  strategy wiring recipes, the connection-budget sizing worksheet
  (informational — varco enforces no tenant-count cap), and the RD-7 Mongo
  clone-cost formula.

  ⚠️ **`ParsedMeta` gains a new field, `tenant_scope: TenantScope =
  TenantScope.TENANT`**, appended last and defaulted — constructible
  without it, so this is source-compatible for keyword construction, but a
  caller building `ParsedMeta` **positionally** picks up the new field.
  `Meta.tenant_scope` absent on a domain class defaults to `TENANT`
  (today's routing behaviour, fail-closed by design — see the multitenancy
  doc's "Global/shared scope" section for the rationale).

- **Tenant control plane: fleet broadcast, `origin` provenance, and a
  fleet-readiness coordinator** (Plan 008, Phases 2-3). Built on top of
  Plan 008 Phase 1's entry-point convergence (see Fixed/Changed above):

  - **`varco-core`** — `TenantControlService.request_provision(tenant_id)` /
    `.request_deprovision(tenant_id, *, confirm=)`: broadcast-only methods that
    emit `TenantProvisionRequested`/`TenantDeprovisionRequested` fleet-wide with
    **no** local catalog write and **no** local provisioner call — the caller is
    explicitly not included (RD-14). `TenantControlService(catalog_authority=
    False)` (worker mode, RD-16) makes `provision()` never write the catalog;
    it runs the local provisioner and emits the new `TenantNodeReady(tenant_id,
    node_id, store_id)` fact event instead. `TenantControlService.mark_active
    (tenant_id)` is the authority-only manual terminator (`ValueError` under
    `catalog_authority=False`). Both command events
    (`TenantProvisionRequested`/`TenantDeprovisionRequested`) gain
    `origin: str | None = None` (wire-compatible, defaulted) — a consumer whose
    own `node_id` matches `origin` skips the event (RD-15), so a node that
    broadcasts its own already-handled command does not re-process it.
    `TenantReadinessCoordinator` (`varco_core.tenancy.control.readiness`)
    aggregates per-store `TenantNodeReady` facts and calls
    `control_service.mark_active()` once every store in the required
    `expected_stores: frozenset[str]` has reported (RD-17 — a *store*, not a
    pod: ten pods of one service share one store and don't change the expected
    set). A timeout (`timeout_s`, default `900.0`) logs one ERROR and never
    auto-activates an incomplete fleet. Readiness state is in-memory only
    (RD-18) — recovery after a coordinator restart is one re-broadcast of
    `request_provision(tenant_id)`.
  - **`varco-fastapi`** — three new routes on `build_tenant_router()`:
    `POST /tenancy/tenants/{id}/request-provision` (202, broadcast-only),
    `POST /tenancy/tenants/{id}/activate` (manual `mark_active()` terminator),
    and `GET /tenancy/tenants/{id}/readiness` (only mounted when
    `build_tenant_router(..., coordinator=<TenantReadinessCoordinator>)` is
    given a coordinator). `DELETE /tenancy/tenants/{id}?broadcast=true` now
    calls `request_deprovision()` instead of the local `deprovision()`, behind
    the same explicit `{"confirm": true}` body every destructive route
    requires. Every new route is `admin_role`-guarded, same as the existing
    surface.
  - **New env vars**: `VARCO_TENANCY_NODE_ID` (`TenantControlService.node_id`
    — defaults to `f"{hostname}:{pid}"`, stamped as `origin` on broadcasts) and
    `VARCO_TENANCY_STORE_ID` (`TenantControlService.store_id` — only
    meaningful under `catalog_authority=False`, stamped on `TenantNodeReady`).

  - **`TenantControlService` gains the read side of the admin surface**:
    `list_tenants(*, status=)` (read-through to `AbstractTenantCatalog`), and
    `provision()`/`mark_active()` now **return** the post-transition
    `TenantDescriptor` instead of `None` — `POST /tenancy/tenants` and
    `POST /tenancy/tenants/{id}/activate` render it directly. Additive for
    callers that only want the side effect (`TenantProvisionConsumer` ignores
    the return value). ⚠️ A third-party `TenantControlService`-shaped object
    passed to `build_tenant_router()` must now provide `list_tenants()` and
    return a descriptor from `provision()`.
  - **`TenantReadinessCoordinator.readiness(tenant_id)` raises
    `TenantNotFoundError`** for a tenant it has not observed (no
    `TenantNodeReady` seen since process start), rendered as 404 by
    `GET /tenancy/tenants/{id}/readiness`. A tenant becomes *observed* on its
    first `TenantNodeReady` — including one carrying an unexpected `store_id`
    — after which the route answers 200 with a possibly-empty `seen` set. The
    404 describes the coordinator's in-memory state (RD-18), never the
    catalog's; use `GET /tenancy/tenants?status=pending` for tenant existence.

  See `technical_docs/features/multitenancy.md`'s "Fleet fan-out:
  `provision()` vs `request_provision()`" and "Fleet readiness" sections for
  the topology table, the command/fact diagram, and the worked 3-service
  readiness example. None of this is wired under the default
  `TenantIsolation.SHARED`.

- **Schema migrations for both persistence backends, with opt-in
  auto-on-startup** (Plan 006). New across four packages:

  - **`varco-core`** — `varco_core.migration`: the backend-agnostic
    `AbstractMigrator` contract (`plan`/`upgrade`/`downgrade`/`stamp` abstract;
    `check`/`close` **concrete**, so a third-party migrator is not broken by
    their addition), the frozen value objects `Revision`/`MigrationPlan`/
    `MigrationReport`, `MigrationSettings.from_env(env=...)` reading the
    `VARCO_MIGRATE_*` variables, the `MigrationError` exception family
    (`PendingMigrationsError`, `MigrationLockTimeout`,
    `IrreversibleMigrationError`, `MigrationBackendUnavailable`), and
    `InMemoryMigrator` — the standard unit-test double, mirroring
    `InMemoryEventBus`/`InMemoryDeadLetterQueue`.
  - **`varco-core`** — a `varco` **console script** (the first in the workspace),
    with `migrate` subcommands (`current`, `pending`, `check`, `upgrade`,
    `downgrade`, `stamp`, `adopt`) resolving an `AbstractMigrator` from a
    `module:callable` target. Backends contribute extra verbs through the
    `varco.commands` entry-point group, so `varco_core` keeps zero sibling
    dependencies. `pending` exits `1` when the schema is behind — a one-line CI
    gate. `downgrade` refuses to run without `--yes`.
  - **`varco-sa` 2.2.0** — `varco_sa.migration`: `AlembicMigrator` (headless
    Alembic — no `env.py` file required; `transaction_per_migration=True`,
    `compare_type=True`), `migration_lock()` (a dedicated `NullPool` connection
    holding `SAXactAdvisoryLock.xact()` open across Alembic's own transactions
    with `SET LOCAL idle_in_transaction_session_timeout = 0`; the `COMMIT` **is**
    the release, and process death releases it too, so there is no TTL to size),
    `env_template.include_object`/`configure_kwargs()`, and
    `ops.rls_upgrade`/`rls_downgrade` so Row-Level Security lands in a reviewed
    revision instead of a startup hook. A **framework Alembic branch**
    (`branch_labels=("varco",)`) ships inside the wheel covering all nine
    framework tables, so `pip install -U varco-sa` brings framework schema
    changes with it — note this makes `upgrade heads` (plural) the correct
    invocation. `AlembicMigrator.adopt_framework_tables()` / `varco migrate
    adopt` bridges an existing `ensure_table()`-built database into migration
    management (run once, before the first upgrade). `alembic` arrives as the
    optional `varco-sa[migrations]` extra.
  - **`varco-sa` 2.2.0** — `varco_sa.metadata`: `framework_metadata()`,
    `framework_table_names()`, `register_framework_metadata()`. Each owning
    module self-registers at import time, so a framework table added in a future
    release is picked up with no app-side change; a completeness test fails the
    day one is added without registering.
  - **`varco-beanie` 1.2.0** — `varco_beanie.migration`: `Migration` /
    `MigrationRegistry` (hand-written, ordered, checksum-verified migrations
    recorded in a `varco_migrations` collection), `BeanieMigrator`, and
    `IndexReconciler`. Multi-pod exclusion uses an owner-fenced lock document
    with a background heartbeat. **Index reconciliation defaults to
    `index_mode="check"`, independent of `mode`** — `mode="upgrade"` never
    silently starts an index build.
  - **`varco-fastapi` 1.2.0** — `MigrationLifecycle` and two new
    `create_varco_app` arguments, `migrations=` and `migration_settings=`. The
    component is **prepended** to the lifespan list, so migrations run before the
    event bus, the outbox relay, and the job runner. `varco_fastapi` imports only
    `varco_core.migration` — never `varco_sa`, `varco_beanie`, or `alembic`.

  **Nothing runs by default.** `VARCO_MIGRATE_MODE` defaults to `off`, and with
  `migrations=None` (the default) `create_varco_app` builds an identical
  `VarcoLifespan`. `check` (fail startup if the schema is behind, never write
  DDL) is the recommended production posture; `upgrade` is the
  single-instance/dev convenience. Setting `VARCO_MIGRATE_MODE` without passing
  `migrations=` now logs one WARNING naming the env var rather than silently
  doing nothing. See `technical_docs/features/schema-migrations.md`.

  ⚠️ `MigrationError` and `MigrationPlan` are **not** re-exported from
  `varco_core` — the pre-existing, unrelated `varco_core.migrator` (domain
  data/field migration) already owns those top-level names. Import them from
  `varco_core.migration`. Every other new name is available from `varco_core`
  directly.

- **`varco-core` — Dead Letter Queue gains a `source` field and `OutboxRelay`/
  `JobRunner` DLQ wiring** (Plan 005 Phase 3, U-6). `DeadLetterEntry.source`
  (`DeadLetterSource.CONSUMER` default, unchanged / `OUTBOX_RELAY` / `JOB`),
  `event: DomainEvent | None`, and new all-defaulted `source_ref`/`payload`
  fields. `OutboxRelay.__init__` gains `retry_policy=`/`dlq=`/`max_attempts=`
  — omitting `retry_policy` reproduces today's exact unbounded
  retry-in-place behaviour; passing `max_attempts` without `dlq` raises
  `ValueError` (refuses to silently drop a poison entry).
  `RetryPolicy.durable_delivery()` is a new named preset
  (`max_attempts=20, base_delay=15.0, max_delay=3600.0`). See
  `technical_docs/features/dead-letter-queues.md`.

- **`varco-core`/`varco-fastapi` — job scheduling, leases, retry, and
  retention** (Plan 005 Phase 4/6, U-17/U-11/U-18/U-19). `Job.run_at`,
  `AbstractJobRunner.enqueue(run_at=, delay=)` for delayed execution;
  `Job.attempt`/`max_attempts` + `JobRunner(retry_policy=, dlq=)` for bounded
  retry (reuses `varco_core.resilience.RetryPolicy`); a fenced lease
  (`try_claim(owner_id=, lease_ttl=)`, `renew()`, `reap_expired_leases()`,
  `save(expected_epoch=)` → `StaleLeaseError`); `delete_where(...)` retention
  primitive (refuses to run with no predicate — `ValueError`);
  `Job(store_raw_token=False)` to avoid persisting the raw Bearer JWT at
  rest (hashes it into `request_token_hash` instead). `JobPoller` gains
  `lease_aware=True` (default) and `retention_sweep=False` (default). Every
  new parameter is defaulted to reproduce pre-Phase-4 behaviour exactly. See
  `technical_docs/features/job-scheduling-and-leases.md`.

- **`varco-sa` — `SAXactAdvisoryLock`** (Plan 005 Phase 4, U-16). A
  transaction-scoped Postgres advisory lock released by the caller's own
  COMMIT/ROLLBACK (`xact(key, session)`), safe behind a transaction-mode
  connection pooler (PgBouncer) where the existing session-scoped
  `SAAdvisoryLock` is not — `release()` can be routed to a different
  physical connection than `try_acquire()` used.

- **`varco-redis` — `RedisBulkhead`** (Plan 005 Phase 8, U-7 second leg).
  Distributed concurrency limiting over Redis, complementing the existing
  `RedisRateLimiter` (distributed rate limiting, already shipped).

- **`varco-fastapi` — A2A v1 surface improvements and RLS DDL helper**
  (Plan 005 Phase 7/5). `varco_sa.rls.enable_rls_ddl(table, ...)` emits the
  `ALTER TABLE ... ENABLE ROW LEVEL SECURITY` + tenant-policy DDL for an
  application's own Alembic revision (varco never applies it itself — see
  `technical_docs/features/postgres-rls.md`). `varco_fastapi/router/a2a/`
  hardens the mounted A2A task surface. See
  `technical_docs/features/a2a-surface.md`.

- **`varco-core` — per-arbitrary-scope encryption keys + crypto-shredding**
  (Plan 005 Phase 1, U-1/U-2). `EncryptionKeyEntry` gains `scope` (defaults
  to `tenant_id` at the Python level — a persisted-store `scope = tenant_id`
  backfill is required before `load_for_scope`/`destroy_scope` see
  pre-existing rows; see `technical_docs/features/crypto-shredding.md`) and
  `destroyed_at`.
  `EncryptionKeyStore` gains `load_for_scope`/`list_scopes`/`destroy_scope`
  (via a capability shim so third-party Protocol implementations keep
  working). `EncryptionKeyManager` gains `build_scoped_registry`,
  `rotate_scope`, `destroy_scope`. `MultiKeyEncryptorRegistry.destroy(kid)`
  makes decrypting a crypto-shredded key raise the new, distinguishable
  `KeyDestroyedError`. See `technical_docs/features/crypto-shredding.md`.

### Fixed

- **`varco-core` — a bus-onboarded tenant was permanently unroutable** (Plan 008,
  Phase 1, RD-11). `TenantProvisionConsumer.on_provision_requested` called
  `AbstractTenantProvisioner.provision()` directly, so the tenant's schema/database
  was created but its catalog row never was — `routing.py`/
  `TenantResolutionMiddleware`'s catalog lookup 404'd it forever, with no code path
  that ever repaired it. The consumer now drives `TenantControlService.provision()`
  — the exact transition `POST /tenancy/tenants` uses — so an event-onboarded tenant
  is routable as soon as `TenantCatalogChanged` is emitted. Pre-existing
  bus-onboarded tenants remain unroutable until repaired with one idempotent
  `POST /tenancy/tenants` (or `provision()` call) per affected tenant.
- **`varco-core` — deprovision over the bus ran destructive DDL while the catalog
  still said `ACTIVE`** (Plan 008, Phase 1, RD-11). The mirror-image defect:
  `on_deprovision_requested` called `provisioner.deprovision(confirm_destroy=True)`
  directly, leaving a routable-looking tenant with no storage (500s instead of the
  intended 410) and never reaching the fan-out-supervisor-stop / pool-eviction steps
  `TenantControlService.deprovision()` performs before destructive DDL. Now routed
  through `TenantControlService.deprovision()`, same as the REST path.

- **`varco-sa` — `print_create_ddl()` was broken on SQLAlchemy 2.x and raised
  unconditionally** (Plan 006 Phase 0). It called
  `create_engine(f"{dialect}://", strategy="mock", executor=_capture)`; the
  `strategy=`/`executor=` arguments were removed in SQLAlchemy 1.4 (replaced by
  `sqlalchemy.create_mock_engine`), so on the pinned `sqlalchemy==2.0.48` every
  call raised `TypeError: Invalid argument(s) 'strategy','executor' sent to
  create_engine()`. The second half of the same bug was that `.compile()` alone
  never invoked the capture callback even had the engine constructed. Now uses
  `create_mock_engine` + `engine.execute(CreateTable(...))`, and
  `Table.tometadata()` was updated to the current `Table.to_metadata()` spelling.
  **No test covered this module at all** before now; it has two new test files.

- **`varco-sa` — three framework metadata objects were unreachable from the
  public API** (Plan 006 Phase 0). `audit_metadata` and `dead_letters_metadata`
  existed but were unexported, and the `varco_encryption_keys` table had only a
  module-private `_metadata` with no public alias — it was *impossible* to put
  that table into Alembic's `target_metadata` without touching a private name.
  All three (`audit_metadata`, `dead_letters_metadata`, `encryption_metadata`)
  are now exported, alongside the aggregated `framework_metadata()`.

- **`varco-core` — event serializers are now genuinely injectable.** The
  `EventSerializer` type alias was a quoted forward reference
  (`EventSerializer: TypeAlias = "Serializer[Event]"`) with `Event` imported only
  under `TYPE_CHECKING`, so at runtime the module-level name was bound to the
  **string** `"Serializer[Event]"` rather than to a type. Every bus backend
  annotated its constructor with
  `Annotated[EventSerializer | None, InjectMeta(optional=True)]`, which therefore
  evaluated `str | None` and raised `TypeError: unsupported operand type(s) for |`.

  **Impact:** under providify < 1.1.0 the failure was swallowed into an empty hints
  dict, silently dropping DI for the `serializer` parameter on `KafkaEventBus`,
  `RedisEventBus`, and `NatsEventBus` — each fell back to `JsonEventSerializer()`,
  so a user-supplied serializer was **never** injected on any bus. Under providify
  >= 1.1.0, which correctly refuses to report a clean bill of health it cannot
  prove, the same defect aborts `container.validate_bindings()` and any app that
  scanned `varco_kafka`, `varco_redis`, or `varco_nats` failed at startup with
  `AnnotationResolutionError`.

  ⚠️ `varco_redis`'s test suite passed green throughout — no redis test exercised a
  path that resolves binding annotations. New `validate_bindings()` regression tests
  in all three backend packages close that coverage gap.

- **`varco-fastapi` — `bind_clients()` now works.** It previously always raised and
  registered nothing: the internal `_factory` closure was never
  `@Provider`-decorated, so `container.bind()` raised (`issubclass()` on a
  function), the `provide()` fallback raised `ProviderBindingNotDecoratedError`, and
  the last-resort `bind(client_cls, client_cls)` raised
  `ClassBindingNotDecoratedError`. `_factory` is now decorated with
  `@Provider(singleton=True)` after its return annotation is patched, and the three
  nested `except Exception` fallbacks — which could never succeed and only masked
  the real cause — have been removed in favour of a single un-guarded
  `container.provide()` call.

- **`varco-core` — the `varco.dlq.depth` gauge silently reported no data for
  every real DLQ backend (Redis, Kafka, Beanie/Mongo, SQLAlchemy) in
  production.** `install_reliability_metrics()`'s observable-gauge callback
  ran the DLQ's async `count()` on a freshly created event loop instead of
  the loop that owns the DLQ's async client, raising `got Future attached to
  a different loop`. The gauge callback swallows exceptions by contract (it
  must never raise), so the metric appeared to work while emitting **zero**
  data points — including against `PeriodicExportingMetricReader`, which
  collects from its own thread with no loop of its own. Only
  `InMemoryDeadLetterQueue` (loop-agnostic) was ever exercised by tests,
  which is why the defect survived. `_run_sync` now captures the owning loop
  at `install_reliability_metrics()` time and submits via
  `asyncio.run_coroutine_threadsafe(coro, owner).result(timeout=5.0)` when
  called off that loop, keeping the previous fresh-loop behaviour when the
  owner is unknown or the caller is already on the owner's own thread. See
  `technical_docs/features/reliability-preset.md`'s pitfall table.

- **`varco-beanie` — `BeanieAuditRepository(hash_chain=True)` raised
  `CollectionWasNotInitialized` on the very first chained write.** The
  chain's `varco_audit_seq` counter was reached via
  `AuditSeqDocument.get_pymongo_collection()`, which silently required a
  *second* `init_beanie(document_models=...)` registration documented
  nowhere — contradicting `AuditSeqDocument`'s own docstring ("no separate
  bootstrap step needed") and `technical_docs/features/database-auditing.md`,
  which only ever documents registering `AuditDocument`. `_seq_collection()`
  now resolves the counter from `AuditDocument`'s own, already-initialised
  database, so registering `AuditDocument` remains the **only** required
  step, including with `hash_chain=True`. `AuditSeqDocument` is retained
  (exported from `varco_beanie.audit`) as schema documentation for operators
  who want to register it deliberately for index/migration tooling.

- **`varco-beanie` — `hash_chain=True`'s `verify_chain()` reported a
  `HashMismatch` on every single link.** `AuditEntry.entry_hash()` hashes
  `occurred_at.isoformat()` at microsecond precision, but BSON datetimes are
  millisecond-precision — a value saved as `…121255` read back as
  `…121000`, so the digest recomputed from a loaded entry never matched the
  one stored at write time. A chained Beanie entry's `occurred_at` is now
  truncated to whole milliseconds before it is both hashed and persisted, so
  the stored value round-trips byte-exact. Fixed in the backend rather than
  by weakening the portable hash contract — `SAAuditRepository` is
  unaffected and continues hashing microseconds. See
  `technical_docs/features/database-auditing.md`'s tamper-evidence section.

- **`varco-sa` — Postgres RLS on the framework tables could never be applied
  at all.** `enable_rls_ddl()` hardcoded a `::uuid` cast in the generated
  policy, but both framework tables (`varco_audit_log`, `varco_dead_letters`)
  declare `tenant_id` as `String(255)` — every call to
  `varco_sa.rls_framework.framework_rls_upgrade()` aborted with `operator
  does not exist: character varying = uuid`, and this had never been caught
  because the only test covering it sat behind an unrelated `NameError`.
  `enable_rls_ddl()` gains a `cast_type: str = "uuid"` parameter (application
  tables are unaffected — the default is unchanged); `framework_rls_upgrade()`
  now passes `cast_type="text"` to match the framework schema. **If you
  already wrote a hand-rolled RLS revision for a `VARCHAR`/`TEXT` tenant
  column, pass `cast_type="text"` to `enable_rls_ddl()`/`rls_upgrade()`
  explicitly** — the default stays `"uuid"`. See
  `technical_docs/features/postgres-rls.md`.

- **`varco-sa` — an RLS-protected query could raise instead of returning
  zero rows once a pooled connection's transaction ended.** Postgres resets
  a `set_config(..., true)` (`SET LOCAL`-equivalent) GUC to the **empty
  string**, not `NULL`, at `COMMIT`/`ROLLBACK`. The next unscoped statement
  on that (possibly pooled) connection then cast `''` to the policy's target
  type and raised (`invalid input syntax for type uuid: ""`) instead of the
  intended fail-closed "no tenant set → zero rows" behaviour. The generated
  policy now wraps the GUC read in `NULLIF(current_setting(...), '')`, still
  inside the load-bearing `(SELECT ...)` InitPlan subquery — the query-planner
  fix from the original RLS work is unchanged. See
  `technical_docs/features/postgres-rls.md`.

- **`varco-nats` — `NatsDLQ.ack()` returned before the JetStream server
  confirmed the acknowledgement**, so a `count()` (or a redrive check)
  immediately after `ack()` could still see the "acked" entry — observed
  taking up to ~1 s to actually clear — and a process exiting right after
  `ack()` could lose the ack outright, causing `DlqRedriver`'s
  publish-then-ack policy to redeliver an already-handled dead letter.
  `ack()` now calls nats-py's `Msg.ack_sync(timeout=2.0)` instead of the
  fire-and-forget `Msg.ack()`; on a timeout the entry is kept in-flight and
  retried on the next `ack()` call rather than silently dropped. `push()`
  is unchanged — it remains fire-and-forget-safe by contract. See
  `technical_docs/features/dead-letter-queues.md`.

- **`varco-sa`, `varco-nats` — integration test/harness hygiene.** RLS
  integration tests were unknowingly running as a Postgres superuser
  (`rolbypassrls=True`), which bypasses RLS unconditionally regardless of
  `FORCE ROW LEVEL SECURITY` and made every RLS assertion vacuously pass;
  tests now provision and connect as a dedicated non-superuser role. A
  migration-lock test asserted nothing because `AlembicMigrator.upgrade()`
  returns before acquiring the lock when there are zero pending revisions;
  it now creates real pending revisions against an isolated database. Seven
  call sites hand-rolled a broken `postgresql://` → `postgresql+asyncpg://`
  string replacement that was a no-op against testcontainers'
  `postgresql+psycopg2://` URLs; replaced with one shared `asyncpg_url()`
  helper plus a guard test. `scripts/integration_tests.sh` no longer treats
  pytest's "no tests collected" exit code (5) as a failure for packages with
  no `@pytest.mark.integration` tests yet.

- **`varco-fastapi` — DST-safe zoned job scheduling (T2) is now reachable
  from the shipped `JobRunner`.** `JobRunner.enqueue()` gains `run_at=`,
  `delay=`, `run_at_wall=`, `tz=`, `fold=`, `gap=`, `overlap=` kwargs and
  routes through `AbstractJobRunner._prepare_zoned_job()` (the RD-5 guard)
  before `store.save()`. Previously this guard was documented but dead
  code — the only way to use a zoned schedule was to construct a `Job`
  with `run_at_wall`/`run_at_tz`/`run_at_fold` set directly and bypass
  `enqueue()` entirely, silently skipping the RD-5 refusal for stores that
  never opted into `supports_zoned_schedules`. Callers who never pass the
  new kwargs see no change — `tz=None` is a pure passthrough. See
  `technical_docs/features/job-scheduling-and-leases.md`'s "Zoned
  schedules" section.

- **`varco-core` — `CacheServiceMixin._use_bulk_cache` now has an effect.**
  `list()` checks `self._use_bulk_cache and isinstance(self._cache,
  BulkCache)` and, when both hold, routes through `read_through_many()`
  (the existing C5 batch primitive) instead of a plain `cache.get()`/
  `cache.set()` pair. Note this still caches the entire list-query result
  under one hashed key — it is not a genuine per-item N-key batch read; a
  caller wanting a true multi-entity batch read should call
  `read_through_many()` directly with its own per-entity keys.
  `_use_bulk_cache=False` (the default) is unchanged. See
  `technical_docs/features/cache-hardening.md`'s "Bulk operations"
  section.

- **`varco-fastapi` — `create_varco_app(i18n=, timezone=)` now validates
  its argument types instead of silently discarding a wrong one.** The
  parameters are retyped from `Any | None` to `I18nSettings | None` /
  `TimezoneSettings | None`, and the previous `isinstance(...) else
  <default>` fallback — which silently swallowed a wrong-type value and
  fell back to the disabled default with no warning — is removed. A
  wrong-type argument now fails loudly (e.g. `AttributeError` on the first
  `.enabled` access) instead of producing a silently-disabled feature that
  looks like a configuration bug somewhere else.

- **`varco-fastapi` — error responses are now actually localized when
  i18n is enabled.** `add_exception_handlers()` and `ErrorMiddleware` both
  gained `message_catalog=`/`set_content_language=` parameters — wired
  automatically by `create_varco_app()` from its resolved `MessageCatalog`
  — and now read `request.state.varco_request_context` (the RD-3 mirror
  set by `LocalizationMiddleware`) to render the error `message` via
  `catalog.format_message()` and set the `Content-Language` response
  header. Previously `message_key`/`params` appeared on error bodies but
  the rendered `message` text was always the untranslated
  `default_message`, regardless of the resolved locale or a bound
  catalog — this was a documented gap, now closed. With no
  `message_catalog=` supplied (i18n disabled, the default), behaviour is
  unchanged. See `technical_docs/features/error-taxonomy-and-i18n.md` and
  `technical_docs/features/i18n-and-localization.md`.

- **`varco-kafka` — `AT_LEAST_ONCE` delivery could silently drop a message
  whose handler raised.** `KafkaEventBus` now disables aiokafka's periodic
  auto-commit for `AT_LEAST_ONCE` (previously honoured via
  `enable_auto_commit`, default `True`) and commits the offset manually,
  once per message, only after the handler chain returns without raising —
  a raised handler now leaves the offset uncommitted so a fresh consumer in
  the same `group_id` redelivers the message, instead of the offset being
  silently advanced by aiokafka's fixed-interval timer regardless of
  handler success. `KafkaEventBusSettings.enable_auto_commit` is retained
  for backward compatibility only and no longer has any effect on bus
  behaviour.
- **`varco-kafka` — `KafkaDLQ.delete_where()` now raises `ValueError` for a
  no-predicate call**, before its existing backend-support
  `NotImplementedError`, matching the `AbstractDeadLetterQueue` contract
  (previously a no-predicate call always raised `NotImplementedError`,
  skipping the "refuse to delete everything" check every other DLQ backend
  already had).
- **`varco-nats` — `NatsDLQ.delete_where()` now raises `ValueError` for a
  no-predicate call**, same fix and same contract gap as the `KafkaDLQ` fix
  above.
- **`varco-redis` — `RedisCache.set()`/`set_many()` now honour sub-second
  `ttl` values.** They switched from second-precision `SETEX`/`int(ttl)`
  (which truncated e.g. `ttl=0.05` to `0` and made Redis reject the call
  with `ResponseError: invalid expire time`) to millisecond-precision
  `PSETEX`/pipelined `psetex` (`round(ttl * 1000)`); a `ttl` that still
  rounds to `<=0`ms now raises a clear `ValueError` instead of Redis's
  cryptic `ResponseError`.
- **`varco-redis` — `RedisJobStore.save()` gained `expected_epoch=`
  fencing**, matching `SAJobStore`/`BeanieJobStore`. Previously passing
  `expected_epoch=` raised `TypeError: unexpected keyword argument`; it now
  performs the epoch-check-then-write inside a `WATCH`/`MULTI`/`EXEC`
  transaction and raises `StaleLeaseError` on a stale or concurrently-raced
  write, closing the gap for anyone relying on lease fencing with a Redis
  job store.
- **`varco-memcached` — `MemcachedCache.set()` no longer silently disables
  expiry for a sub-second `ttl`.** A positive sub-second `ttl` (e.g.
  `0.05`) is now rounded UP to `1` second (`math.ceil`) instead of
  truncated DOWN to `0`, which Memcached's protocol treats as "never
  expire" — previously the entry lived forever instead of expiring almost
  immediately. Memcached's `exptime` remains genuinely whole-seconds-only
  at the wire-protocol level (unlike Redis's millisecond `PSETEX`), so this
  closes the silent-no-expiry failure mode without claiming sub-second
  precision; an explicit `ttl=0`/`ttl=None` is still no-expiry, unchanged.
- **`varco-beanie` — `BeanieDeadLetterQueue.count_by_channel()` no longer
  raises `TypeError` on beanie 2.0.1 + motor 3.7.1.** It previously
  `await`ed beanie's `Document.aggregate(pipeline).to_list()`, which raises
  because this motor version's `aggregate()` returns its cursor
  synchronously rather than as a coroutine; it now drives
  `DeadLetterDocument.get_pymongo_collection().aggregate(pipeline)`
  directly (with an `inspect.isawaitable()` guard for driver versions that
  do return a coroutine) and iterates with `async for`. See
  `technical_docs/features/dead-letter-queues.md` for the same caveat
  applied to direct application code calling beanie aggregation.
- **`varco-casbin` — `CasbinPolicyEngine.reload()` no longer raises
  `TypeError` after any prior `enforce()` call.** `_AttrStr` (the `str`
  subclass used to wrap ABAC/RBAC subjects and objects) gained a
  `__reduce__` method so `copy.deepcopy` — used internally by Casbin's
  `load_policy()` — reconstructs it through its real constructor instead of
  the default `str`-subclass reconstruction, which called `cls(value)` and
  omitted the required `attrs` argument.

### Changed

- **`Serializer[Event]` is the event-serializer injection interface.** Bus
  constructors now annotate `Annotated[Serializer[Event] | None,
  InjectMeta(optional=True)]` instead of a type alias, and `JsonEventSerializer`
  explicitly subclasses `Serializer[Event]` and carries
  `@Singleton(priority=-sys.maxsize - 1)` — registered as the lowest-priority
  default, so it works out of the box but loses to any application-supplied
  serializer regardless of registration order:

  ```python
  @Provider(singleton=True)
  def my_serializer() -> Serializer[Event]:
      return MyCompactSerializer()


  container.provide(my_serializer)  # wins over JsonEventSerializer
  ```

  **Breaking:** the `EventSerializer` alias is removed. Replace
  `EventSerializer` with `Serializer[Event]` (from `varco_core.serialization`) in
  any annotation. `JsonSerializer`, `NoOpSerializer`, and `TypedJsonSerializer` now
  subclass `Serializer[Any]` explicitly for the same DI reason.

- **Workspace pins the vendored `providify` 1.1.0 wheel** (was 0.1.6). The previous
  pin did not satisfy the `providify>=1.1.0` constraint declared by every package,
  meaning varco was developed and tested against a version no PyPI consumer would
  resolve. Comments in `pyproject.toml` now note the sync requirement.

### varco-core

#### Added
- **JWT claim transformation** (`varco_core.jwt.transform`) — consume foreign-shaped
  JWTs (Keycloak, Cognito, Auth0, a bespoke claim, …) without any application code
  change. `ClaimMapping` / `ClaimRule` / `ClaimPath` (code-configured) and
  `JwtTransformSettings` / `JwtTransformConfig` (env-driven, `VARCO_JWT_TRANSFORM_*`
  + per-issuer `VARCO_JWT_TRANSFORM__<LABEL>__*`) both resolve through the
  `ClaimTransformer` Protocol; `JwtParser.parse()`, `TrustedIssuerRegistry.verify()`,
  and `varco-fastapi`'s `JwtBearerAuth`/`PassthroughAuth` all pick it up for free
  through one shared funnel. Zero-config behaviour is unchanged (`IDENTITY`
  transformer, no copy). See `technical_docs/features/jwt-claim-transformer.md`.
- **Named token profiles** (`varco_core.jwt.profile`) — `TokenProfile` /
  `TokenProfileRegistry` recognise multiple kinds of special/internal tokens
  (`system`, `internal`, `partner`, `service-mesh`, …) by issuer/token_type/audience/
  required claims, env-configured via `VARCO_JWT_PROFILE__<NAME>__*`, and can grant
  `implied_roles`/`implied_scopes`. `JwtUtil.matches_profile()` /
  `.profile_name()` / `.assert_profile()`; `JwtBuilder.as_profile()`. See
  `technical_docs/features/token-profiles.md`.
- **JWT verification hardening** — `VARCO_JWT_LEEWAY_SECONDS` (clock-skew leeway for
  `exp`/`nbf`, default `0.0`) and `VARCO_JWT_AUDIENCE` (expected `aud`, default
  `None` = not enforced) via `varco_core.jwt.config.JwtVerificationSettings`,
  threaded through `JwtParser.parse()`, `TrustedIssuerRegistry.verify()`, and
  `JwtBearerAuth`.
- **JWKS caching knobs** — `TrustedIssuerRegistry(min_refresh_interval=...,
  ttl_seconds=...)` (env: `VARCO_JWKS_MIN_REFRESH_SECONDS` default `10.0`,
  `VARCO_JWKS_TTL_SECONDS` default `0.0` = disabled) allow a proactive, age-based
  keyset reload in addition to the existing reactive kid-miss refresh. A background
  refresher task remains out of scope (needs its own lifespan wiring) — deferred.
- **`ValueShape.GRANTS`** validation gives an actionable `ClaimTransformError` naming
  the offending list index and missing key for a malformed `grants` claim, replacing
  a previously bare `KeyError`.

#### Changed
- ⚠️ **Widened `AuthContext` materialisation on JWT parse.** A token carrying only
  `tenant_id`/`actor` claims (no `roles`/`scopes`/`grants`), or matching a
  `TokenProfile` with `implied_roles`/`implied_scopes`, now materialises a non-`None`
  `auth_ctx` where it previously stayed `None`. Canonical tokens with none of
  `roles`/`scopes`/`grants`/`tenant_id`/`actor` and no matching profile still yield
  `auth_ctx is None`. Code doing `if token.auth_ctx is None: treat as machine token`
  should account for this.
- `JsonWebToken.to_claims()` now emits `tenant_id`/`act` claims when present in
  `auth_ctx.metadata["tenant_id"]`/`["actor"]`, so varco-minted tokens round-trip
  tenant/actor through re-parsing. `_RESERVED_CLAIM_KEYS` was **not** extended to
  include `tenant_id`/`act`/`user_id`/`actor` (a deviation from the original plan —
  the executable test suite requires `JwtBuilder().claim("tenant_id", ...)` /
  `.claim("act", ...)` to keep succeeding); `JwtBuilder.claim()` behaviour for these
  keys is unchanged.
- `JwtUtil.is_system()` now prefers a registered `"system"` `TokenProfile` when one
  exists, falling back to the historical `SYSTEM_ISSUER` `ClassVar` comparison
  otherwise. `SYSTEM_ISSUER` is documentation-deprecated in favour of
  `VARCO_JWT_PROFILE__SYSTEM__ISS` — it keeps working with no removal scheduled and
  no runtime `DeprecationWarning`.

#### Fixed
- Corrected every documented DI override example (`varco_core.observability.di`
  docstrings, README) that showed `container.install(OtelConfiguration,
  config=...)` or `container.provide(lambda: OtelConfig(...))` — neither call
  shape has ever worked: `install()` takes no `config=` keyword and `provide()`
  rejects undecorated callables (`ProviderBindingNotDecoratedError`). The
  correct pattern is a module-level `@Provider`-decorated factory function
  registered with `container.provide(fn)` **before** `install()`/`scan()`
  (equal-priority bindings resolve first-registered, not last). See
  `ARCHITECTURE.md`'s DI Wiring section for the full corrected pattern and the
  quoted-return-annotation landmine below.

### varco-kafka

#### Fixed
- 🐛 **`container.get(KafkaChannelManager)` / `KafkaChannelManagerSettings`
  was hard-broken** (`LookupError: Cannot resolve 'values: typing.Any'`) —
  `KafkaChannelManagerSettings` carried `@Singleton` directly on a pydantic
  `BaseSettings` subclass, and providify cannot constructor-inject a
  `**values: Any` signature. Replaced with a lowest-priority `@Provider`
  factory (`kafka_channel_manager_settings` in `varco_kafka.channel`), the
  same pattern already used for `varco_casbin` settings. Guarded by
  `varco_kafka/tests/test_kafka_di.py`.

### varco-nats

#### Fixed
- 🐛 **`container.get(NatsStreamManager)` / `NatsChannelManagerSettings` was
  hard-broken** — same root cause and fix as the `varco-kafka` entry above
  (`@Singleton` on a pydantic `BaseSettings` class replaced by a
  lowest-priority `nats_channel_manager_settings` `@Provider` factory in
  `varco_nats.channel`). Guarded by `varco_nats/tests/test_nats_di.py`.

### varco-fastapi

#### Changed
- ⚠️ **Error response bodies now include a `detail` field when present.**
  `add_exception_handlers()` and `ErrorMiddleware` both stopped silently dropping
  `ErrorMessage.detail` — a 403 from a denied `RouteGuard` (missing scope/role/token
  profile/grant) now surfaces its actionable message in the JSON body under
  `"detail"`, not just `"message"`. Clients parsing only `{"code", "message"}` are
  unaffected; clients that assert the *absence* of a `"detail"` key should update.
- **`PassthroughAuth` refactored** onto `JwtParser.parse_unverified()` instead of
  hand-rolled claim parsing — it now benefits from the claim-transformer pipeline
  (env-driven or explicit) like every other JWT entry point. A regression test pins
  the resulting `AuthContext` for a canonical token to the pre-refactor behaviour.

#### Added
- **`JwtBearerAuth(audience=..., leeway=...)`** — opt-in audience enforcement and
  configurable clock-skew leeway, both falling back to `VARCO_JWT_AUDIENCE` /
  `VARCO_JWT_LEEWAY_SECONDS` when omitted. Logs one warning at construction when
  audience is left unenforced.
- **`RouteGuard.token_profiles` / `require_token_profile(*names)`** — gate a
  `@route` on the JWT's resolved token profile (`ctx.metadata["token_profile"]`),
  checked between the role check and the grant check.
- **`create_varco_app(configure_jwt=True)`** — calls
  `configure_jwt_from_env()` once at startup so the process-global claim-transform
  and token-profile registries match what `VarcoFastAPIModule`'s DI providers hand
  out. Set `configure_jwt=False` to manage the registries yourself.

#### Fixed
- 🐛 **`container.get(TracerProvider)` raised `TypeError: tracer_provider()
  missing 1 required positional argument: 'config'`** when `VarcoFastAPIModule`
  and `varco_core.observability.di.OtelConfiguration` shared one container —
  `Inject[OtelConfig]` was silently not injected into `OtelConfiguration`'s
  provider method, even though the two modules looked unrelated. Root cause:
  `VarcoFastAPIModule.profiling_settings` declared a *quoted* return
  annotation (`-> "ProfilingSettings"`); under PEP 563 that annotation
  resolves to the literal string `"'ProfilingSettings'"`, and providify's
  `eval` fallback (`providify/binding.py`) registered the resulting **string**
  as a binding interface. That one malformed binding then made
  `DIContainer._build_localns()` raise, which `_collect_kwargs_sync()`
  silently swallowed (`except Exception: hints = {}`) — disabling constructor
  and provider injection for **every** binding in the container, not just the
  broken one. Fixed by dropping the quotes (`from __future__ import
  annotations` already made the annotation lazy) and keeping
  `ProfilingSettings` imported at module scope. The underlying defect is in
  `providify` (a sibling library) and is **not** fixed here — see
  `ARCHITECTURE.md`'s DI Wiring section for the landmine and its one-line
  diagnostic (`[b for b in container._bindings if isinstance(b.interface,
  str)]`). Guarded by `varco_fastapi/tests/test_di_binding_health.py` and
  `varco_core/tests/test_observability_di.py`.

---

## [0.1.0] — 2026-04-07

First public alpha release of the varco framework. All eight packages are published
to PyPI simultaneously. This release establishes the public API surface — expect
breaking changes between alpha versions while the API stabilises.

### New packages

| Package | Version | Description |
|---------|---------|-------------|
| `varco-core` | 0.1.0 | Domain model, service layer, event system, resilience, query AST, JWT authority |
| `varco-kafka` | 0.1.0 | Apache Kafka event bus backend (aiokafka) |
| `varco-redis` | 0.1.0 | Redis Pub/Sub event bus, cache, DLQ, rate limiter (redis.asyncio) |
| `varco-sa` | 0.1.0 | SQLAlchemy async ORM backend with auto-generated models |
| `varco-beanie` | 0.1.0 | Beanie/MongoDB async ODM backend |
| `varco-ws` | 0.1.0 | WebSocket and SSE event bus implementations |
| `varco-fastapi` | 0.1.0 | FastAPI adapter — routing mixins, auth middleware, typed HTTP client, DI wiring |
| `varco-memcached` | 0.1.0 | Memcached cache backend |

---

### varco-core

#### Added
- **`AsyncService`** — generic service base class over five type parameters
  (`DomainModel`, primary key, Create/Read/Update DTOs). Implements full CRUD
  with `_get_repo()` as the single required hook.
- **`AbstractEventBus` / `AbstractEventProducer` / `EventConsumer`** — layered
  event system. `@listen` stores metadata declaratively; `register_to()` wires
  subscriptions imperatively at startup.
- **`InMemoryEventBus`** — zero-dependency event bus for unit tests.
- **`AbstractDeadLetterQueue` / `InMemoryDeadLetterQueue`** — DLQ interface and
  in-memory implementation for test-time failure inspection.
- **`@retry` / `@timeout` / `@circuit_breaker`** — composable resilience
  decorators. `CircuitBreaker` and `Bulkhead` are shared-instance patterns.
- **`@rate_limit` / `InMemoryRateLimiter`** — per-process rate limiting. Use
  `varco-redis` `RedisRateLimiter` for multi-pod deployments.
- **`@hedge`** — hedged request decorator for tail-latency reduction on
  idempotent reads.
- **`QueryParser` / `ASTVisitor` / `QueryOptimizer` / `TypeCoercionVisitor`** —
  typed query AST pipeline. Filter strings (`age__gte=18`) are parsed into
  `FilterNode` trees, optimised, and applied to backends via visitor pattern.
- **`JwtAuthority` / `MultiKeyAuthority` / `TrustedIssuerRegistry`** — JWT
  signing and verification with zero-downtime key rotation.
- **Key sources** (`PemFile`, `PemFolder`, `JwksUrl`, `OidcDiscovery`) — pluggable
  key loading for `TrustedIssuerRegistry`.
- **`OutboxRepository` / `OutboxRelay`** — transactional outbox pattern.
  Events are saved in the same DB transaction as domain entities; `OutboxRelay`
  publishes them asynchronously.
- **`AsyncCache` / `CacheBackend` / `LayeredCache`** — cache protocol hierarchy.
  `InMemoryCache` and `NoOpCache` ship in core.
- **`InvalidationStrategy`** — pluggable cache invalidation.
  `TTLStrategy`, `TaggedStrategy`, `EventDrivenStrategy`, `CompositeStrategy`
  all ship in core.
- **`@cached` / `CacheServiceMixin`** — decorator and mixin for caching service
  methods.
- **`TenantAwareService` / `SoftDeleteService` / `ValidatorServiceMixin` /
  `AsyncValidatorServiceMixin`** — composable service mixins via MRO.
- **`@span` / `@counter` / `@histogram`** — OpenTelemetry tracing and metrics
  decorators. `OtelConfiguration` wires `TracerProvider` / `MeterProvider` via DI.
- **`VarcoSettings`** — base pydantic-settings class for all backend
  configuration objects.

---

### varco-kafka

#### Added
- **`KafkaEventBus`** — `AbstractEventBus` implementation backed by aiokafka.
  Supports topic-per-channel routing, configurable consumer groups, and
  backpressure via `DispatchMode`.
- **`KafkaDLQ`** — Dead letter queue that routes failed events to a dedicated
  Kafka topic after exhausting retry attempts.
- **`KafkaChannelManager`** — manages topic creation and partition assignment.
- **`KafkaEventBusConfiguration` / `KafkaChannelManagerConfiguration`** —
  `@Configuration` classes for DI wiring.
- **`KafkaHealthCheck`** — liveness / readiness probe for Kafka connectivity.

---

### varco-redis

#### Added
- **`RedisEventBus`** — `AbstractEventBus` implementation backed by Redis
  Pub/Sub.
- **`RedisStreamEventBus`** — alternative implementation backed by Redis Streams
  for durable, consumer-group-aware delivery.
- **`RedisDLQ`** — Dead letter queue using a Redis Hash + Sorted Set backend.
  `push()` never raises — failures are logged and swallowed per the DLQ contract.
- **`RedisCache`** — `CacheBackend` implementation backed by Redis.
- **`RedisRateLimiter`** — distributed rate limiter using Redis atomic counters.
  Use this instead of `InMemoryRateLimiter` in multi-pod deployments.
- **`RedisEncryptionKeyStore`** — encrypted key storage backed by Redis.
- **`RedisLock`** — distributed lock backed by Redis `SET NX EX`.
- **`RedisEventBusConfiguration` / `RedisCacheConfiguration` /
  `RedisStreamConfiguration` / `RedisDLQConfiguration`** — `@Configuration`
  classes for DI wiring.
- **`RedisHealthCheck`** — liveness / readiness probe for Redis connectivity.

---

### varco-sa

#### Added
- **`SAModelFactory`** — generates SQLAlchemy ORM models at import time from
  `DomainModel` subclasses. Models are never declared manually.
- **`SARepository`** — `AsyncRepository` implementation for SQLAlchemy.
- **`SAUnitOfWork` / `SAUoWProvider`** — unit-of-work pattern over SQLAlchemy
  async sessions.
- **`SAOutboxRepository`** — `OutboxRepository` implementation for SQLAlchemy.
- **`SAEncryptionKeyStore`** — encrypted key storage backed by SQLAlchemy.
- **`SQLAlchemyFilterVisitor` / `SQLAlchemyQueryApplicator`** — applies the
  `varco-core` query AST to SQLAlchemy `Select` statements.
- **`SAModule`** / **`bind_repositories()`** — DI wiring helpers.
- **`SAHealthCheck`** — liveness / readiness probe for database connectivity.

---

### varco-beanie

#### Added
- **`BeanieModelFactory`** — generates Beanie `Document` models from
  `DomainModel` subclasses.
- **`BeanieRepository`** — `AsyncRepository` implementation for Beanie/MongoDB.
- **`BeanieUnitOfWork` / `BeanieUoWProvider`** — unit-of-work pattern over
  Beanie sessions.
- **`BeanieOutboxRepository`** — `OutboxRepository` implementation for Beanie.
- **`BeanieModule`** / **`bind_repositories()`** — DI wiring helpers.
- **`BeanieHealthCheck`** — liveness / readiness probe for MongoDB connectivity.

---

### varco-ws

#### Added
- **`WebSocketEventBus` / `WebSocketConnection`** — `AbstractEventBus`
  implementation that delivers events over WebSocket connections.
- **`SSEEventBus` / `SSEConnection`** — `AbstractEventBus` implementation that
  delivers events as Server-Sent Events streams.

---

### varco-fastapi

#### Added
- **`VarcoRouter`** — base `APIRouter` subclass with built-in DI resolution.
- **CRUD mixins** — `CreateMixin`, `ReadMixin`, `UpdateMixin`, `DeleteMixin`,
  `ListMixin`, `StreamMixin` — compose standard HTTP endpoints without boilerplate.
- **`AuthMiddleware`** — validates JWT bearer tokens on every request using
  `TrustedIssuerRegistry`.
- **`CORSMiddleware`** — env-var-driven CORS configuration.
- **`AsyncVarcoClient` / `SyncVarcoClient`** — typed HTTP clients with
  automatic JWT injection, retry, and circuit breaker.
- **`SkillAdapter`** — mounts Google A2A (Agent-to-Agent) skill endpoints from
  a `SkillDefinition`. Install the `[a2a]` extra for the A2A SDK types.
- **`MCPAdapter`** — mounts Model Context Protocol (MCP) tool endpoints.
  Install the `[mcp]` extra (`mcp>=1.0`) for full support.
- **`VarcoFastAPIModule`** / **`bind_clients()`** — DI wiring for FastAPI.
- **Background job runner** — `AsyncJobRunner` backed by `asyncio.TaskGroup`
  for lifecycle-managed background tasks.

---

### varco-memcached

#### Added
- **`MemcachedCache`** — `CacheBackend` implementation backed by aiomcache.
- **`MemcachedCacheConfiguration`** — `@Configuration` class for DI wiring.

---

---

<!-- Links -->
[Unreleased]: https://github.com/edoardoscarpaci/varco/compare/v3.1.0...HEAD
[3.1.0]: https://github.com/edoardoscarpaci/varco/compare/v3.0.0...v3.1.0
[3.0.0]: https://github.com/edoardoscarpaci/varco/compare/v0.1.0...v3.0.0
[0.1.0]: https://github.com/edoardoscarpaci/varco/releases/tag/v0.1.0
