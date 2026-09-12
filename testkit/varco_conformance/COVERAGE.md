# Conformance suite coverage — audit outcome

**Never packaged** — lives alongside the eight conformance base classes it documents (five from
Plan 012/019, plus `idempotency_store`/`webhook_subscription`/`token_revocation` added by Plans
029/031/034), reached only via `pythonpath = ["../testkit"]` (CLAUDE.md's Test Conventions). This
file is the durable, audited record answering: *for every implementation of one of the eight
shared ABCs, does it subclass the matching conformance suite, and if not, why not?*

Produced by Plan 024 (§D-C7, C7), for the original five-suite matrix. See BACKLOG.md's "C7's audit
outcome" open question, answered: **two** real gaps out of a five-suite × ~24-implementation
matrix — the great majority of the apparent gaps scout tooling flagged were legitimate absences,
not real holes. The three later ABCs (below) were designed with a shared suite from day one and
audited at the same rigor as they landed.

**Rule (CLAUDE.md's Test Conventions)**: a new implementation of one of the eight ABCs either
subclasses its suite or gets a row in this file explaining why not — a future absence must be
argued against a written record, not rediscovered from scratch.

---

## The matrix

| Suite | Implementations | Subclassed | Gap |
|---|---|---|---|
| `channel_manager` | `KafkaChannelManager`, `NatsStreamManager`, `RedisChannelManager` | `varco_kafka/tests/test_kafka_channel_integration.py:111`, `varco_nats/tests/test_nats_channel_integration.py:122`, `varco_redis/tests/test_redis_channel.py:228` | none (no in-process implementation exists — see "Stated absences" below) |
| `cache` | `InMemoryCache`, `NoOpCache`, `RedisCache`, `LayeredCache`, `MemcachedCache` | `varco_core/tests/test_conformance_inmemory.py:59,70`; `varco_redis/tests/test_redis_conformance.py:49,73`; `varco_memcached/tests/test_memcached_conformance.py:26` | none |
| `job_store` | `InMemoryJobStore`, `SAJobStore`, `RedisJobStore`, `BeanieJobStore` | `varco_core/tests/test_conformance_inmemory.py:130` (cross-package import of `varco_fastapi/varco_fastapi/job/store.py:40`, documented); `varco_sa/tests/test_sa_conformance.py:26`; `varco_redis/tests/test_redis_conformance.py:56`; `varco_beanie/tests/test_beanie_conformance.py:34` | none |
| `event_bus` | `InMemoryEventBus`, `NoopEventBus`, `RedisEventBus`, `RedisStreamEventBus`, `KafkaEventBus`, `NatsEventBus` | all but `NoopEventBus` | **`NoopEventBus`** — resolved as a stated reason, not a subclass (see below) |
| `dlq` | `InMemoryDLQ`, `RedisDLQ`, `RedisStreamDLQ`, `KafkaDLQ`, `NatsDLQ`, `SADeadLetterQueue`, `BeanieDeadLetterQueue` | all — `RedisStreamDLQ` filled by Plan 024 Step 32 (`varco_redis/tests/test_redis_conformance.py::TestRedisStreamDLQConformance`) | **none remaining** — was `RedisStreamDLQ`, now closed |

## Stated absences

These are legitimate, permanent absences — not TODOs, not backlog rows.

- **`NoopEventBus`** (`varco_core/varco_core/event/memory.py:639`) — a Null Object: `publish()`
  discards, `subscribe()` returns a **pre-cancelled** `Subscription`
  (`memory.py:665-691`). It deliberately violates `EventBusConformance`'s
  deliver-what-you-publish contract by design — subclassing the suite for it would mean
  xfail-ing most of the suite, which teaches nothing and rots. It is intentionally uncovered by
  the shared suite; `varco_core/tests/test_conformance_inmemory.py`'s module docstring points here.
- **`channel_manager`** — there is no `InMemoryChannelManager`. `ChannelManager` is inherently a
  broker-admin concern (declare/delete/list a real topic/stream/channel on a real broker), so an
  in-process fake would test nothing meaningful. Only the three real-broker backends
  (`varco_kafka`, `varco_redis`, `varco_nats`) subclass it, and all three do.
- **`varco_ws`** — already resolved, not a hole. `varco_ws/tests/test_ws_conformance.py:1-27`
  explains that `WebSocketEventBus`/`SSEEventBus` are push adapters *wrapping* an
  `AbstractEventBus`, not a new bus implementation — they are covered by their own bespoke
  real-server tests instead of the `event_bus` suite, which would not exercise the adapter
  behaviour that actually matters (WS/SSE framing, connection lifecycle).
- **`varco_memcached`** — implements only `CacheBackend`. No event bus, DLQ, job store, or channel
  manager exists in this package, so it legitimately subscribes to only the `cache` suite.
- **`varco_casbin`** — implements **none** of the five ABCs (it is a policy engine, not a
  broker/cache/job-store backend), and therefore does not need `pythonpath = ["../testkit"]` at
  all — the only package of the ten deliberately without one. Confirmed present in all nine
  others.
- **`TenantSource`** (`varco_core.tenancy.source`, Plan 033 / S6) — no conformance suite, and
  none is planned (§D-S6-conformance). `testkit/varco_conformance` is **never packaged**, so the
  only audience a suite would serve — an out-of-tree implementer of a new source (mTLS, path-
  based, a cached custom-domain lookup) — structurally cannot reach it. `resolve()` is also a
  pure function over a frozen dataclass, unlike the five ABCs above, whose implementations all do
  I/O against a real external system — "does this backend really behave the same" is not the hard
  question here. The invariants a suite would assert (never raises; `None` not `""`; never
  mutates the `TenantRequest`; `name`/`trust` are class-level and unique) are asserted once,
  parametrised over the three shipped sources, in
  `varco_core/tests/test_tenant_sources_builtin.py`, and written into `TenantSource`'s own
  *Edge cases* docstring section — the contract's only home.
- **`AbstractTenantMembership`** (`varco_core.tenancy.membership`, Plan 033 / S5) — same reasoning
  as `TenantSource` immediately above: the parked repository-backed resolver
  (`BACKLOG.md`'s parked table) is the only realistic out-of-tree implementer, and the never-
  packaged testkit cannot reach it either way. `NullTenantMembership`/`ClaimTenantMembership` do
  no I/O, so the "does this backend behave the same under a real broker/store" question a suite
  exists to answer does not apply. Covered instead by
  `varco_core/tests/test_tenant_membership.py` directly against both shipped implementations.
- **Kind-B example, left unfixed on purpose (Plan 042 / §D-kinds, §D-fast)** —
  `DeadLetterQueueConformance.test_count_reflects_pushed_entries`
  (`varco_kafka/tests/test_kafka_conformance.py:80-88`) asserts only `after >= before`, which
  trivially holds at `KafkaDLQ.count()`'s constant `-1` (`varco_kafka/varco_kafka/dlq.py:544`).
  This is a suite-assertion gap (Kind B), not a backend violation — the backend is fine, the guard
  is weak. **Deliberately not strengthened in Plan 042**: doing so may turn `KafkaDLQ` red, which
  would itself be a Kind-A finding needing its own `KI-N` xfail and register row — real scope, not
  this plan's. Recorded here as the worked Kind-B candidate; tracked as BACKLOG `CONF-COUNT`.

## New ABC outside the five (Plan 025)

- **`AbstractPathWatcher`** (`varco_core.watch.base`, Plan 025 / T1) is a **new ABC that is not
  one of the five** this page audits. Both implementations (`StatPollWatcher`,
  `WatchfilesWatcher`) live in `varco_core` itself, so its shared contract base —
  `varco_core/tests/watch_contract.py::PathWatcherContract` — lives next to them rather than in
  `testkit/varco_conformance`, which exists specifically to reach *across* packages. If a future
  backend package ever ships a third implementation, promote the contract module into
  `testkit/varco_conformance` at that point. This row pre-empts the "why is there no suite for
  this?" audit question this page exists to answer.

## New ABC outside the five, with a shared suite (Plan 029)

- **`AbstractIdempotencyStore`** (`varco_core.idempotency.base`, Plan 029 / D1) is a **sixth ABC**,
  outside the original five this page audits, but — unlike `AbstractPathWatcher` above — it has
  **four** implementations across **three** packages from day one
  (`InMemoryIdempotencyStore` in `varco_core`, `RedisIdempotencyStore`, `SAIdempotencyStore`,
  `BeanieIdempotencyStore`), so it earns a real cross-package suite in this directory rather than
  a same-package contract module. `testkit/varco_conformance/idempotency_store.py`'s
  `IdempotencyStoreConformance` is subclassed by all four:
  `varco_core/tests/test_idempotency_conformance_inmemory.py`,
  `varco_redis/tests/test_idempotency_store_conformance.py`,
  `varco_sa/tests/test_idempotency_store_conformance.py`,
  `varco_beanie/tests/test_idempotency_store_conformance.py`. The load-bearing assertion —
  `test_concurrent_reserve_race_yields_exactly_one_acquired` — is exactly what §D-D1-atomic exists
  to guarantee, run against every backend's own native atomic primitive (`SET NX PX`, a unique
  index + `IntegrityError`/`DuplicateKeyError`, a lazily-created `asyncio.Lock`).

- **`WebhookSubscriptionRepository`** (`varco_core.webhook.base`, Plan 031 / D4) is a **seventh
  ABC**, same treatment as `AbstractIdempotencyStore` above — three implementations across three
  packages from day one (`InMemoryWebhookSubscriptionRepository` in `varco_core`,
  `SAWebhookSubscriptionRepository`, `BeanieWebhookSubscriptionRepository`), so it earns a real
  cross-package suite: `testkit/varco_conformance/webhook_subscription.py`'s
  `WebhookSubscriptionRepositoryConformance` is subclassed by all three:
  `varco_core/tests/test_webhook_conformance_inmemory.py`,
  `varco_sa/tests/test_webhook_subscription_repository_integration.py`,
  `varco_beanie/tests/test_webhook_subscription_repository_integration.py`. The tenant-scoping
  assertion (`test_find_by_tenant_never_leaks_another_tenant`) is the load-bearing one — a
  subscription belonging to one tenant must never be returned for another, across every backend.

## New ABC outside the five, with a shared suite (Plan 034)

- **`AbstractTokenRevocationStore`** (`varco_core.revocation.base`, Plan 034 / S13) is an **eighth
  ABC**, same treatment as `AbstractIdempotencyStore`/`WebhookSubscriptionRepository` above — a
  shared cross-package suite from day one:
  `testkit/varco_conformance/token_revocation.py`'s `TokenRevocationStoreConformance` is
  subclassed by `InMemoryTokenRevocationStore` (`varco_core/tests/test_conformance_inmemory.py`)
  and `RedisTokenRevocationStore` (`varco_redis/tests/test_redis_revocation.py`, integration-only).
  The load-bearing assertions are the not-valid-before watermark rule
  (`test_watermark_rule_iat_before_revoked_at_is_revoked`) and the missing-`iat` fail-closed rule
  (`test_missing_iat_is_treated_as_revoked`) — both are §D-S13-nvb/§D-S13-noiat's contract made
  executable across every backend.

  **Stated absence — `NullTokenRevocationStore`**: the scanned DI default (`varco_core.revocation.null`)
  is a Null Object, same shape as `NoopEventBus` above — `revoke()` silently discards every entry,
  which deliberately violates the suite's revoke→is_revoked contract. Subclassing the suite for it
  would mean xfail-ing nearly all of it, teaching nothing. It is intentionally uncovered by the
  shared suite; `varco_core/varco_core/revocation/null.py`'s class docstring points here.

## No conformance suite (Plan 036, §D-S11-conformance)

- **Stated absence — `AbstractAuthorizer`** (`varco_core.auth.base`, first shipped pre-3.2;
  `AuditingAuthorizer` added by Plan 036 / S11). Not one of the eight ABCs this page audits, and
  no suite is planned for it. `AbstractAuthorizer` is a single-method (`authorize(ctx, action,
  resource) -> None`), app-supplied policy hook, not a broker/cache/job-store/DLQ/idempotency-
  store/webhook-subscription/revocation-store backend doing I/O against a real external system —
  the "does this backend really behave the same under a real server" question a conformance suite
  exists to answer does not apply here; an authorizer's entire contract is "raise on denial, deny
  by default", which is a business-logic property, not an I/O-adapter property. That contract is
  asserted directly, in `varco_core/tests/`, against both shipped implementations
  (`BaseAuthorizer` — permissive by design, and `AuditingAuthorizer` — the decorator this plan
  adds), rather than through a shared suite `testkit/varco_conformance` (never packaged) could not
  usefully serve an out-of-tree authorizer implementer anyway.

## No conformance suite (Plan 038, §D-S19-conformance)

- **Stated absence — `WebhookVerifier`** (`varco_core.webhook.inbound.base`, added by Plan 038 /
  S19). Not a ninth `testkit/varco_conformance` module. All five in-tree classes, across four
  provider families (`StandardWebhooksVerifier`/`SvixWebhookVerifier`/`StripeWebhookVerifier`/
  `GitHubWebhookVerifier`/`SlackWebhookVerifier` — Standard Webhooks and Svix are one family,
  since `SvixWebhookVerifier` only overrides `.provider`), are pure-CPU, Docker-free, in-tree
  classes with no I/O — the "does this backend really behave the same under a real server"
  question a conformance suite exists to answer does not apply, unlike the eight ABCs above
  (every one of which has at least one durable, I/O-performing backend). A parametrized table in
  `varco_core/tests/test_webhook_inbound_verifiers.py` already runs the same contract assertions
  (missing/malformed header, timestamp tolerance, signature mismatch, rotation) against all five
  in-tree classes — identical coverage to a shared suite, for a package (`testkit`) that is
  never distributed and therefore could not reach an out-of-tree implementer anyway (the same
  argument Plan 016 / §RL-3d used to decline re-exporting providify's own pytest fixtures).
  **Un-park trigger:** the first out-of-tree `WebhookVerifier`, or a fifth in-tree provider
  family.

## No conformance suite (Plan 039, §D-S20-conformance)

- **Stated absence — `RetentionTarget`** (`varco_core.retention.base`, added by Plan 039 / S20).
  Not a ninth/tenth `testkit/varco_conformance` module. All six in-tree implementations
  (`DlqRetentionTarget`/`AuditRetentionTarget`/`IdempotencyRetentionTarget`/
  `RevocationRetentionTarget`/`JobRetentionTarget`/`CallableRetentionTarget`) are thin, in-tree
  adapters exercised against in-memory backends already covered by the eight shipped conformance
  suites (or, for `CallableRetentionTarget`, no backend at all — it wraps a caller-supplied
  function). A parametrized table in `varco_core/tests/test_retention_targets.py` already runs
  the same contract assertions (non-empty `kind`, the dry-run-calls-no-delete-method safety
  property, `limit` forwarding, no ValueError-swallowing) across all six — identical coverage to
  a shared suite, for a package (`testkit`) that is never distributed and therefore could not
  reach an out-of-tree implementer anyway (the same argument Plan 038 / §D-S19-conformance and
  Plan 016 / §RL-3d used before it). **Un-park trigger:** the first out-of-tree `RetentionTarget`,
  or a seventh in-tree one.

## No conformance suite (Plan 040, §D-S21-conformance)

- **Stated absence — `Redactor`** (`varco_core.redaction.redactor`, added by Plan 040 / S21).
  Not a ninth `testkit/varco_conformance` module. `Redactor` is a new public one-method Protocol,
  not an implementation of one of the eight ABCs this package covers, and there is exactly one
  in-tree implementation (`PolicyRedactor`) — a conformance suite would test that implementation
  against itself, which is not a contract test, it is a tautology. `Redactor`'s own contract
  (`redact(key, value)` is pure, never expected to raise, and every caller's fail-safe behaviour
  is asserted against a **deliberately broken** fake redactor, not against `PolicyRedactor`) is
  already covered directly in `varco_core/tests/test_redaction.py`'s `TestFailSafe` class.
  **Un-park trigger:** a second in-tree `Redactor` implementation.

## What Plan 024 filled

- **`RedisStreamDLQ` → subclassed.** `varco_redis/tests/test_redis_conformance.py` gained
  `TestRedisStreamDLQConformance(DeadLetterQueueConformance)`, mirroring `TestRedisDLQConformance`.
  It is a real, durable DLQ implementation with a real transport; there was no principled reason
  for it to be less proven than `RedisDLQ`.

## Conformance findings register

This is the durable home the Test Conventions convention points at for accumulated conformance
findings — **not `BACKLOG.md`**, whose own header (`BACKLOG.md:5-13`) states plainly that
completed-work tables are trimmed by design (and were, once: `cae7f33` took a whole findings
table with it). This section lives next to the suites it documents and, unlike `BACKLOG.md`, has
no other reason to be edited away.

| ID | Suite | Backend | Symptom | Kind | Status | Fix | Guard |
|---|---|---|---|---|---|---|---|
| KI-2 | `dlq` | `KafkaDLQ.delete_where()` | Always raised `NotImplementedError`, even with no predicate given — never reached the ABC's "no predicate → `ValueError`" check | A | FIXED | `varco_kafka/varco_kafka/dlq.py:575-580` (no-predicate check runs first; rationale `:558-566`) | `varco_kafka/tests/test_kafka_dlq.py::TestKafkaDLQDeleteWhereRaises::test_delete_where_with_no_predicate_raises_value_error` (Docker-free, added Plan 042) + the inherited Docker-backed `varco_kafka/tests/test_kafka_conformance.py:71-78` |
| KI-3 | `cache` | `RedisCache.set()` | Sub-second `ttl` truncated to whole seconds via `int()` | A | FIXED | `varco_redis/varco_redis/cache.py:291` (`PSETEX` with `ms = round(effective_ttl * 1000)`; rationale `:284-290`, non-positive guard `:292-300`) | `varco_redis/tests/test_redis_cache.py::test_set_with_subsecond_ttl_preserves_precision` (`:196-202`, Docker-free) |
| KI-5 | `cache` | `MemcachedCache.set()` | Sub-second `ttl` truncated to `exptime=0`, meaning *never expire* | A | FIXED | `varco_memcached/varco_memcached/cache.py:361` (`math.ceil()`, rounds up to the smallest expressible non-zero `exptime`; rationale `:340-349`) | `varco_memcached/tests/test_cache.py::test_set_with_subsecond_ttl_rounds_up_to_one` (`:215-252`, Docker-free) |
| KI-6 | `dlq` | `BeanieDeadLetterQueue.count_by_channel()` | beanie 2.0.1 / motor 3.7.1: `await`s a cursor `Document.aggregate().to_list()` returns synchronously, raising `TypeError` | A (upstream) | WORKED AROUND | Bypasses beanie's aggregation cursor via `get_pymongo_collection().aggregate(pipeline)` + `inspect.isawaitable()` guard — `technical_docs/features/dead-letter-queues.md:335-349` | n/a — an upstream-driver workaround, not an ABC violation; covered by the Beanie DLQ's own tests |
| KI-7 | `dlq` | `NatsDLQ.delete_where()` | Same defect as KI-2 — always raised `NotImplementedError` regardless of predicate | A | FIXED | `varco_nats/varco_nats/dlq.py:558-563` (no-predicate check runs first; rationale `:540-547`) | `varco_nats/tests/test_nats_dlq.py::TestNatsDLQDeleteWhereRaises::test_delete_where_with_no_predicate_raises_value_error` (Docker-free, added Plan 042) + the inherited Docker-backed `varco_nats/tests/test_nats_conformance.py:55` |

### How to file a new finding

A red conformance run means one of three things, and only one of them may touch `testkit/`:

| Kind | What it is | Action | May you edit `testkit/`? | In-tree precedent |
|---|---|---|---|---|
| **A — backend ABC violation** | The backend genuinely breaks the ABC's documented contract | `@pytest.mark.xfail(strict=True)` on the subclass's override, with a `reason=` beginning `BUG`, a colon, then the finding ID (e.g. `KI-13 …`), plus a register row here. **Never** an in-place production fix in the same pass | ❌ **Never** — weakening a shared assertion to accommodate one backend silently un-tests every other | KI-2/KI-7 (`varco_kafka/varco_kafka/dlq.py:575`, `varco_nats/varco_nats/dlq.py:558`) |
| **B — conformance-suite gap** | The suite's assertion is missing, or too weak to fail on a real violation. The backend is fine; the *guard* is not | **Fix in place in `testkit/`.** No xfail (there is no bug to pin), no register row required | ✅ **Yes — this is the one case you should** | `varco_kafka/tests/test_kafka_conformance.py:80-88` — `test_count_reflects_pushed_entries` asserts only `after >= before`, trivially true at `KafkaDLQ.count()`'s constant `-1` (`varco_kafka/varco_kafka/dlq.py:544`) — see "Stated absences" above |
| **C — legitimate backend capability divergence** | Both the backend and the suite are correct; the transport genuinely cannot express what the suite assumes | **Override the single test in the subclass** with a docstring arguing why, **or** a "Stated absences" bullet above. Never loosen the shared suite | ❌ Only the one overridden test, never the shared suite | `varco_memcached/tests/test_memcached_conformance.py:33-62` — Memcached `exptime` is whole-seconds at the wire protocol, so the subclass overrides `test_ttl_expiry`'s 0.3s window rather than relaxing it for every backend |

**The next free ID is `KI-13`.** KI-8…KI-12 were general BACKLOG "Known issues" rows, unrelated to
this register, already resolved via Plans 020/024 — recorded here so the series is never
restarted at a colliding number. Every finding-marker `reason=` **must** begin with the word
`BUG`, a colon, and its `KI-N` id — the liveness cross-check greps every `varco_*/tests/` and
`testkit/` file for that literal prefix (see CLAUDE.md's Test Conventions for the exact command);
today it returns exactly **one** hit — `varco_redis/tests/test_redis_cache_disposes.py:94` — an
**upstream providify gap** filed under the `UPSTREAM-GAPS.md` convention
(`design/upstream-gaps/providify-disposes-first-match.md`), correctly **not** a row in this
register. Do not "fix" that hit into a `KI-N` row; it belongs where it is. (This paragraph itself
deliberately avoids spelling that prefix as one contiguous token, so it does not inflate its own
cross-check.)

---

**Audited and written down**: 2026-09-02 (Plan 024, §D-C7). Findings register added by Plan 042
(2026-09-10). Referenced from CLAUDE.md's Test Conventions conformance paragraph.
