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

## What Plan 024 filled

- **`RedisStreamDLQ` → subclassed.** `varco_redis/tests/test_redis_conformance.py` gained
  `TestRedisStreamDLQConformance(DeadLetterQueueConformance)`, mirroring `TestRedisDLQConformance`.
  It is a real, durable DLQ implementation with a real transport; there was no principled reason
  for it to be less proven than `RedisDLQ`.

---

**Audited and written down**: 2026-09-02 (Plan 024, §D-C7). Referenced from CLAUDE.md's Test
Conventions conformance paragraph.
