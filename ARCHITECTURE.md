# Architecture Reference — varco

Complete technical map of all packages, modules, classes, and design patterns. Use this to navigate the codebase efficiently.
 
---

## Package Overview

```
varco_core/              — Domain model, service layer, event system, resilience, DI contracts
  ├── event/             — AbstractEventBus, AbstractEventProducer, EventConsumer, @listen
  ├── service/           — AsyncService[D, PK, C, R, U], mixins (validator, tenant, soft-delete)
  │   ├── saga.py        — SagaOrchestrator, SagaStep, SagaState, AbstractSagaRepository
  │   └── audit.py       — AuditEntry, AuditRepository (ABC), AuditLogMixin, AuditConsumer
  ├── cache/             — AsyncCache protocol, CacheBackend ABC, invalidation strategies
  │   └── warming.py     — CacheWarmer ABC, QueryCacheWarmer, SnapshotCacheWarmer, CompositeWarmer
  ├── query/             — QueryParser → AST → QueryTransformer → backend applicator
  │   └── aggregation.py — AggregationFunc, AggregationExpression, AggregationQuery, SA applicator
  ├── resilience/        — @timeout, @retry, @circuit_breaker decorators
  ├── profiling/         — @profile, profiled(), ProfileSession, pluggable CPU/memory backends
  ├── observability/     — @span/@counter/@histogram, TracingServiceMixin/TracingRepositoryMixin,
  │   │                    Metric/register_gauge, OtelConfig/OtelConfiguration
  │   ├── params.py      — ParamCaptureConfig, CapturePlan/build_capture_plan, sanitize_value —
  │   │                    automatic @span parameter capture (param.<name> attributes)
  │   ├── attributes.py  — GlobalAttributes registry, wrap_instrument()/wrap_gauge_callback() —
  │   │                    process-wide attrs stamped on every span + metric measurement
  │   └── cache.py       — install_cache_metrics(), CacheMetricsConfig, record_cache_hit/miss/
  │                        eviction/duration/stampede_suppressed/stale_served + backplane
  │                        published/received/dropped counters (Plan 010 / C3)
  ├── lock.py            — AbstractDistributedLock, InMemoryLock, LockHandle
  ├── watch/              — AbstractPathWatcher ABC, StatPollWatcher (default), WatchfilesWatcher
  │                         (opt-in `varco-core[watch]`); WatchEvent/WatchKind/WatchTarget
  ├── reload.py           — ReloadableResource[T] — load → swap under a lock → notify
  │                         subscribers, keep-last-good on any post-startup load failure
  ├── tls/                — TrustStore (unified TLS trust, superset of SSLConfig + the old
  │   │                     varco_fastapi.auth.TrustStore), ReloadingTrustStore (on watch/
  │   │                     reload above), iter_cert_files, bind_trust_store (Plan 026)
  │   ├── store.py        — TrustStore — the frozen-dataclass superset value object
  │   ├── discovery.py    — CERT_FILE_PATTERNS, iter_cert_files() — the one cert-glob helper
  │   ├── reload.py       — ReloadStrategy, ReloadingTrustStore
  │   └── di.py           — bind_trust_store() — no @Configuration, ever (see below)
  ├── idempotency/        — AbstractIdempotencyStore ABC, ReserveOutcome, IdempotencyRecord,
  │                         InMemoryIdempotencyStore (default), compute_fingerprint(),
  │                         IdempotencySettings (Plan 029 / D1a — HTTP adapter lives in
  │                         varco_fastapi.middleware.idempotency, not here)
  ├── webhook/            — WebhookSubscription, WebhookDelivery, WebhookSubscriptionRepository,
  │                         WebhookSigner, validate_target(), WebhookDispatcher (Plan 031 / D4 —
  │                         admin mount lives in varco_fastapi.webhook, not here)
  │   └── inbound/        — WebhookVerifier ABC, VerificationResult, four provider adapters,
  │                         get_verifier(), WebhookReplayGuard (Plan 038 / S19 — the FastAPI
  │                         verify_webhook() dependency lives in varco_fastapi.webhook, not here)
  ├── flags/              — AbstractFeatureFlags, FlagEvaluationContext, InMemoryFeatureFlags,
  │                         NullFeatureFlags (Plan 032 / D7 — opt-in via
  │                         varco_core.flags.di.enable_feature_flags(); OpenFeature provider
  │                         deferred, see technical_docs/features/feature-flags.md)
  ├── schedule/           — Schedule, CatchUpPolicy, parse_cron()/CronSchedule,
  │                         ScheduleMaterializer, AbstractScheduleRepository (Plan 032 / D6 — no
  │                         execution path; the existing AbstractJobRunner runs materialized jobs)
  ├── retention/          — RetentionPolicy, RetentionRegistry, RetentionTarget + six adapters,
  │                         RetentionScheduler, inspect_retention_posture(),
  │                         bind_retention_registry() (Plan 039 / S20 — no @Singleton/@Provider/
  │                         @Configuration anywhere in this package, ever; see its own docstring)
  ├── authority/         — JwtAuthority, TrustedIssuerRegistry, key rotation
  ├── auth/              — AbstractAuthorizer, user/role/permission models
  ├── repository.py      — AsyncRepository[D, PK] protocol
  ├── uow.py             — AsyncUnitOfWork, IUoWProvider protocols
  ├── model.py           — DomainModel ABC
  ├── dto/               — DTOBase, DTOFactory, pagination
  ├── mapper.py          — Type mapping utilities
  ├── meta.py            — FieldHint, ForeignKey, PrimaryKey; CompositeKey type aliases
  ├── providers.py       — DIContainer, DI wiring helpers
  └── exception/         — Domain, service, query, HTTP exception hierarchy

varco_kafka/             — Kafka event bus backend (aiokafka)
  ├── bus.py             — KafkaEventBus(AbstractEventBus)
  ├── channel.py         — KafkaChannel (topic routing)
  ├── dlq.py             — KafkaDLQ (dead letter queue)
  ├── config.py          — KafkaConfig (frozen dataclass)
  └── di.py              — bootstrap() scan helper

varco_nats/              — NATS JetStream event bus backend (nats-py)
  ├── bus.py             — NatsEventBus(AbstractEventBus) — JetStream, durable consumers
  ├── channel.py         — NatsStreamManager(ChannelManager) — backing-stream admin
  ├── connection.py      — NatsConnectionSettings(ConnectionSettings)
  ├── dlq.py             — NatsDLQ (dead letter queue, WorkQueue-retention stream)
  ├── health.py          — NatsHealthCheck — connect + account_info() probe
  ├── config.py          — NatsEventBusSettings, NatsDeliverySemantics
  └── di.py              — bootstrap() scan helper

varco_redis/             — Redis Pub/Sub event bus + cache backend (redis.asyncio)
  ├── bus.py             — RedisEventBus(AbstractEventBus)
  ├── cache.py           — RedisCache(CacheBackend[K, V])
  ├── backplane.py       — RedisPubSubBackplane(CacheBackplane) — LayeredCache L1 coherence (Plan 010)
  ├── lock.py            — RedisLock(AbstractDistributedLock) — SET NX PX + Lua atomic release
  ├── streams.py         — Redis streams utilities (for channels)
  ├── channel.py         — RedisChannel (pubsub or stream routing)
  ├── dlq.py             — RedisDLQ (dead letter queue)
  ├── idempotency.py     — RedisIdempotencyStore(AbstractIdempotencyStore) — SET NX PX (Plan 029 / D1b)
  ├── config.py          — RedisConfig, CacheConfig (frozen dataclasses)
  └── di.py              — bootstrap() scan helper; RedisCacheConfiguration (opt-in cache)

varco_sa/                — SQLAlchemy async ORM backend
  ├── __init__.py        — SAConfig, SAModelFactory, bind_repositories()
  ├── bootstrap.py       — SAFastrestApp.pool_metrics() → SAPoolMetrics
  ├── pool_metrics.py    — SAPoolMetrics frozen dataclass, pool_metrics(engine) helper
  ├── outbox.py          — SAOutboxRepository, SARelayOutboxRepository
  ├── inbox.py           — SAInboxRepository, SAPollerInboxRepository (dedup table: varco_inbox)
  ├── job_store.py       — SAJobStore (at-most-once jobs table: varco_jobs)
  ├── saga.py            — SASagaRepository (saga state table: varco_sagas)
  ├── conversation.py    — SAConversationStore (turn-per-row table: varco_conversation_turns)
  ├── advisory_lock.py   — SAAdvisoryLock (PostgreSQL pg_try_advisory_lock / pg_advisory_unlock)
  ├── schema_guard.py    — SchemaGuard, SchemaDrift, SchemaDriftReport
  ├── encryption_store.py — SAEncryptionKeyStore (varco_encryption_keys table)
  ├── idempotency.py     — SAIdempotencyStore(AbstractIdempotencyStore) — UNIQUE(key) +
  │                        IntegrityError; table: varco_idempotency (Plan 029 / D1b, the
  │                        eleventh framework table)
  ├── audit.py           — SAAuditRepository (AuditEntryModel; table: varco_audit_log;
  │                        Postgres ON CONFLICT DO NOTHING on entry_id, plain INSERT elsewhere)
  ├── webhook.py         — SAWebhookSubscriptionRepository (table: webhook_subscriptions;
  │                        Plan 031 / D4a, the twelfth framework table)
  ├── schedule.py        — SAScheduleRepository (table: schedules; UNIQUE(schedule_id);
  │                        Plan 032 / D6, the thirteenth framework table, migration
  │                        0007_schedules_table; task_name column added by migration
  │                        0008_schedule_task_name, Plan 039 / S20)
  ├── health.py          — SAHealthCheck (SELECT 1 probe)
  ├── di.py              — SAModule (@Configuration)
  └── (auto-generated)   — ORM models created from DomainModel subclasses at import time

> `SAConfig` doubles as the DI settings object, avoiding a parallel `SASettings` class.

varco_beanie/            — Beanie/MongoDB async ODM backend
  ├── __init__.py        — BeanieSettings, BeanieModelFactory
  ├── outbox.py          — BeanieOutboxRepository (OutboxDocument)
  ├── inbox.py           — BeanieInboxRepository (InboxDocument, dedup via unique index)
  ├── job_store.py       — BeanieJobStore (JobDocument; at-most-once jobs collection: varco_jobs)
  ├── saga.py            — BeanieSagaRepository (SagaDocument, varco_sagas collection)
  ├── audit.py           — BeanieAuditRepository (AuditDocument; collection: varco_audit_log;
  │                        plain insert() — no conflict handling, raises DuplicateKeyError)
  ├── idempotency.py     — BeanieIdempotencyStore(AbstractIdempotencyStore) — unique index +
  │                        DuplicateKeyError; collection: varco_idempotency (Plan 029 / D1b)
  ├── webhook.py         — BeanieWebhookSubscriptionRepository (WebhookSubscriptionDocument;
  │                        Plan 031 / D4a)
  ├── schedule.py        — BeanieScheduleRepository (ScheduleDocument; unique index on
  │                        schedule_id; Plan 032 / D6)
  ├── query/aggregation.py — BeanieAggregationApplicator (MongoDB aggregation pipeline)
  ├── index_guard.py     — BeanieIndexGuard, IndexDrift, IndexDriftReport
  ├── health.py          — BeanieHealthCheck (server_info() probe)
  └── di.py              — BeanieConfiguration (@Configuration)

varco_memcached/         — Memcached cache backend (aiomcache)
  ├── cache.py           — MemcachedCache(CacheBackend), MemcachedCacheSettings
  │                        MemcachedCacheConfiguration (@Configuration)
  └── health.py          — MemcachedHealthCheck (stats() probe)

varco_ws/                — WebSocket + Server-Sent Events (SSE) event bus backend (browser real-time events)
  ├── websocket.py       — WebSocketEventBus (push adapter), WebSocketConnection
  └── sse.py             — SSEEventBus (push adapter), SSEConnection, _STOP_SENTINEL

varco_casbin/            — Casbin policy-engine authorization backend (ACL/RBAC/ABAC)
  ├── engine.py          — CasbinPolicyEngine(PolicyEngine, PolicyManagement) — wraps casbin.AsyncEnforcer; _AttrStr ABAC bridge
  ├── config.py          — CasbinSettings (model preset/path/text + adapter selector)
  ├── adapter.py         — build_adapter() factory (memory | file | sqlalchemy async)
  ├── router.py          — build_policy_router() — FastAPI APIRouter REST admin ([fastapi] extra)
  ├── models/*.conf      — bundled Casbin models: acl, rbac, rbac_domains, abac
  └── di.py              — bootstrap() scan helper; enable_policy_authorizer() (opt-in authorizer)
```

> Authorization seam lives in **varco_core.auth.policy** (PolicyEngine, PolicyManagement,
> EnforcementRequest, RequestMapper, PolicyEngineAuthorizer). Full design:
> `technical_docs/features/casbin-authorization.md`. CLAUDE.md retains only the three
> wiring rules.

### Dependency graph

```
varco_kafka     ──┐
varco_nats      ──┤
varco_redis     ──┤
varco_sa        ──┤─→ varco_core
varco_beanie    ──┤
varco_memcached ──┤
varco_ws        ──┤
varco_fastapi   ──┤   (+ optional varco_core for the REST admin router it hosts)
varco_casbin    ──┘   (+ optional varco_fastapi[fastapi] extra for the REST admin router)
```

`varco_core` is the only package without a `[tool.uv.sources]` sibling reference. All backend packages resolve it from the workspace rather than PyPI during development.

---

## Type Hierarchy & Protocols

### Event System

```
AbstractEventBus (ABC)
  ├── InMemoryEventBus        (tests)
  ├── KafkaEventBus           (varco_kafka)
  ├── NatsEventBus            (varco_nats)    — NATS JetStream, durable, at-least-once (redelivers
  │                                             on a crash AND on a raising handler via nak(),
  │                                             bounded by max_deliver — Plan 019 / RT2-B)
  └── RedisEventBus           (varco_redis)

AbstractEventProducer (ABC)
  └── EventProducer(producer: AbstractEventBus)

EventConsumer (ABC)
  └── @listen decorator + register_to(bus) pattern
  └── Composable with: EventDrivenStrategy (cache invalidation)

AbstractDeadLetterQueue (ABC)
  ├── InMemoryDeadLetterQueue (tests)
  ├── KafkaDLQ                (varco_kafka)
  ├── NatsDLQ                 (varco_nats)    — WorkQueue-retention stream; exact count()
  ├── RedisDLQ                (varco_redis)
  └── SADeadLetterQueue       (varco_sa)      — durable, varco_dead_letters table (Plan 005 Phase 3)

DeadLetterEntry (Plan 005 Phase 3) — event: DomainEvent | None, source: DeadLetterSource
  (CONSUMER default / OUTBOX_RELAY / JOB), source_ref, payload — one shape for all three

Serializer[Event] (Protocol, varco_core.serialization)
  ├── JsonEventSerializer          (varco_core.event.serializer) — DEFAULT, @Singleton at
  │                                  priority=-sys.maxsize-1, so it loses to any app-supplied one
  └── CloudEventsJsonSerializer    (varco_core.event.cloudevents, Plan 030 / N2) — CloudEvents
                                     v1.0.2 STRUCTURED mode; OPT-IN only, no DI decorator at all
                                     (a module-level @Singleton/@Provider would be auto-registered
                                     by container.scan("varco_core", recursive=True)).
                                     Wire it with bind_cloudevents_serializer(container, settings).
                                     Every bus resolves Serializer[Event]: Kafka/NATS inject it
                                     as scanned singletons, both Redis shapes get it forwarded by
                                     RedisEventBusSelectorConfiguration.bus(). All five DLQ
                                     backends + the outbox take it as a parameter.
                                     ⚠️ SADeadLetterQueue and OutboxRelay are hand-constructed —
                                     pass serializer= explicitly there

generate_asyncapi(consumers | container, ...) -> dict   (varco_core.asyncapi, Plan 030 / N3)
  AsyncAPI 3.1.0 from LIVE, registered consumers — never a static import walk, because a
  @listen channel may be Callable[[Any], str] resolved at register_to() time.
  CLI: `varco export-asyncapi [--check]` (varco_core.cli.asyncapi)
```
### File watching (Plan 025 / T1)

```
AbstractPathWatcher (ABC)       — subscribe(cb) -> unsubscribe, async start()/stop(),
  │                                abstract async _run(); shared _notify() debounce/error
  │                                handling (§D-T1-errors)
  ├── StatPollWatcher            — default, stdlib-only; polls a 3-field stat fingerprint
  │                                (st_mtime_ns, st_size, st_ino) — correct on NFS/Docker
  │                                bind mounts and Kubernetes `..data` symlink swaps
  └── WatchfilesWatcher          — opt-in (`varco-core[watch]`); backed by the Rust `notify`
                                   crate via watchfiles, but still re-derives events from the
                                   same stat-fingerprint diff (never from watchfiles' own
                                   Change enum), so both implementations are behaviourally
                                   identical and share one contract test suite
                                   (varco_core/tests/watch_contract.py)

Both implementations structurally satisfy varco_fastapi.lifespan.AbstractLifecycle (a
runtime_checkable Protocol) — zero import from varco_core to varco_fastapi.

ReloadableResource[T] (varco_core.reload) — loader + optional AbstractPathWatcher +
  generation counter + subscribers. Keep-last-good on any post-startup reload() failure;
  the first start() load is fail-fast. Also structurally satisfies AbstractLifecycle.
  producers (EventConsumer retry exhaustion, OutboxRelay, JobRunner)

ChannelManager (ABC) — admin-level create/delete/exists/list, separate from AbstractEventBus
  ├── KafkaChannelManager    (varco_kafka)   — broker topic metadata is the existence predicate
  ├── RedisChannelManager    (varco_redis)   — process-local declaration registry (Pub/Sub has no per-channel object)
  └── NatsStreamManager      (varco_nats)    — declaration registry + broker evidence (Plan 019 / RT2-C)
  Contract (enforced by testkit/varco_conformance/channel_manager.py, one of five conformance
  modules): declare_channel(c) ⟹ channel_exists(c) is True until delete_channel(c) — "declared or
  present", not "carries data". NatsStreamManager additionally exposes channel_has_messages(),
  a NATS-only affordance preserving the old "subject carries a message" predicate.

EventMiddleware (Callable[[Event, str, next] → Awaitable[None]])
  ├── CorrelationMiddleware   — propagates correlation_id across the event chain
  ├── LoggingEventMiddleware  — structured log per dispatched event
  └── TracingEventMiddleware  — opens one OTel span per event dispatch
      ├── span name: "event.{EventTypeName}"
      ├── attributes: messaging.channel, event.type, correlation_id
      ├── records exception + sets ERROR status on failure
      └── graceful fallback: no-op if opentelemetry is not installed
```

### TLS trust (Plan 026 / T3, T5, T7; client injection + mTLS hardening Plan 027 / T4, T6)

One unified TLS trust model, plus its reloading wrapper, four HTTP-client injection adapters,
an opt-in process-global installer, and encrypted-key/PKCS#12 mTLS support. Full design
(mutate-vs-swap reload strategy, the additive `SSL_CERT_FILE`/`SSL_CERT_DIR` divergence, the
deprecation shim, the PKCS#12 temp-file discipline, a Pitfalls table):
`technical_docs/features/tls-trust-and-hot-reload.md`.

```
TrustStore (varco_core.tls.store, @dataclass(frozen=True))
  ├── ca_cert: Path | bytes | None,  ca_folders: Path | Sequence[Path] | None
  ├── cert_patterns: tuple[str, ...] = CERT_FILE_PATTERNS,  recursive: bool = True
  ├── client_cert / client_key,  include_system_cas,  verify,  check_hostname
  ├── key_password: str | bytes | Callable[[], str | bytes] | None    (repr=False, Plan 027 / T6a)
  ├── pkcs12_file / pkcs12_password (repr=False) / pkcs12_trust_ca    (Plan 027 / T6b)
  ├── from_env()            — VARCO_* names + SSL_CERT_FILE/SSL_CERT_DIR (additive, §D-T3-env)
  ├── build_ssl_context()   → ssl.SSLContext — union of the two predecessor orderings, plus
  │                           key_password/PKCS#12 branches (§D-T6-password, §D-T6-pkcs12)
  ├── to_ssl_config()       ⇄ SSLConfig.to_trust_store()   (lossless bridge, §D-T3-bridge)
  └── to_httpx_verify() / to_aiohttp_connector() / to_urllib3_poolmanager() /
      to_requests_adapter()                        — thin delegations to tls.clients (below)
        │
        └── varco_fastapi.auth.trust_store.TrustStore (subclass, §D-T3-oq1)
              — ⚠️ DEPRECATED, removed in 4.0.0. Pins 3.0 semantics: recursive=False,
                cert_patterns=("*.pem","*.crt"), deferred (not eager) mTLS-pairing check.
                isinstance(legacy, core.TrustStore) is True; the reverse is False.

ReloadingTrustStore (varco_core.tls.reload — composes, does not inherit)
  ├── spec: TrustStore                                (frozen; the config)
  ├── _resource: ReloadableResource[ssl.SSLContext]    (Plan 025 / T2 — keep-last-good)
  ├── _watcher: AbstractPathWatcher                    (Plan 025 / T1 — over ca_folders + cert/key)
  ├── .context / .generation / async start()/stop()/reload() / subscribe(cb)
  ├── to_httpx_verify() / to_aiohttp_connector() / to_urllib3_poolmanager() /
  │   to_requests_adapter()                        — same four delegations, reading .context live
  └── ReloadStrategy: AUTO (default) | MUTATE | SWAP
        AUTO → additions-only batch: MUTATE the live ssl.SSLContext (adds trust, never revokes —
               ssl.SSLContext has no unload API); anything removed/replaced: SWAP + bump generation

iter_cert_files(root, *, patterns, recursive)   (varco_core.tls.discovery, §D-T7)
  — the one cert-glob helper for 4 call sites that used to disagree silently:
    SSLConfig.build_ssl_context, the legacy TrustStore.build_ssl_context,
    PemFolderSource._has_changes/_scan. A file matching the wider CERT_FILE_PATTERNS set but
    not a site's own patterns is skipped AND logged once at WARNING per (root, patterns).

varco_core.tls.clients (Plan 027 / T4b, §D-T4-adapters) — zero hard client dependencies
  ├── to_httpx_verify(store)              → ssl.SSLContext          (httpx 0.28+)
  ├── to_aiohttp_connector(store, **kw)   → aiohttp.TCPConnector    (aiohttp 3.14+, async def)
  ├── to_urllib3_poolmanager(store, **kw) → urllib3.PoolManager     (urllib3 v2.x)
  ├── to_requests_adapter(store)          → requests.adapters.HTTPAdapter subclass (requests 2.32.3+)
  └── MissingClientDependencyError(ImportError)  — raised by any of the above if uninstalled
  — every httpx/aiohttp/urllib3/requests import is function-body-only; enforced by
    test_tls_no_hard_client_deps.py (ast walk + subprocess sys.modules check)

varco_core.tls.install (Plan 027 / T4c, §D-T4-install)
  ├── install_process_trust(store, *, acknowledge_global_mutation)  → RestoreHandle
  │     — patches ssl._create_default_https_context; ValueError without the ack kwarg;
  │       RuntimeError if the private hook is absent on the running interpreter
  └── RestoreHandle(previous_hook)   — .restore(), also usable as a context manager
  — varco itself never calls install_process_trust (mechanically checked by `rg`, Step 20)

varco_core.tls.pkcs12 (Plan 027 / T6b, §D-T6-pkcs12)
  ├── load_pkcs12_identity(path, password) → Pkcs12Identity(key_pem, cert_pem, ca_pems)
  │     — decodes via cryptography.hazmat...pkcs12, entirely in memory
  ├── materialize_chain()  → contextmanager yielding a private (0700 dir/0600 file,
  │     /dev/shm-preferred) temp Path; unlinked in `finally` on every exit path
  └── Pkcs12LoadError(ValueError)  — wrong password, corrupt bundle, or no private key
```

**Rule**: `varco_core.tls` imports **nothing** from `varco_core.connection`, `varco_fastapi`, or
any backend package — mechanically enforced by `varco_core/tests/test_tls_layering.py` (an AST
walk over module-level, non-`TYPE_CHECKING` imports; a `sys.modules` walk cannot work here
because the full test suite runs in one process and countless other tests already import
`varco_core.connection` before `test_tls_layering.py` ever runs, so `varco_core.connection`
would already be in `sys.modules` regardless of whether `varco_core.tls` itself imports it —
note this is no longer caused by `varco_core/__init__.py` itself, which is PEP 562-lazy as of
Plan 028/P1a and does not eagerly import `connection`). The bridge the other direction,
`SSLConfig.to_trust_store()`, lives in `varco_core.connection.ssl`, not in `varco_core.tls`,
precisely so this package stays a leaf.

**Rule**: no scanned `@Configuration` in `varco_core.tls`, ever (§D-T3-oq3) — see CLAUDE.md.

### Service Layer

```
AsyncService[D, PK, C, R, U] (ABC)
  ├── Abstract: _get_repo(uow) → AsyncRepository[D, PK]
  ├── Hooks (chainable via super()):
  │   ├── _scoped_params(user, ...) → dict  (tenant, authorization)
  │   ├── _check_entity(entity, ctx)        (sync cross-concern gate: soft-delete, tenant, etc.)
  │   ├── _async_check_entity(entity, ctx)  (async I/O-bound gate; runs after _check_entity)
  │   └── _prepare_for_create(data)         (normalization)
  │
  └── Methods:
      ├── create(data: C) → R
      ├── read(pk: PK, **scope) → R
      ├── update(pk: PK, data: U, **scope) → R
      ├── delete(pk: PK, **scope) → None
      └── list(query: QueryParams, **scope) → Page[R]

Mixins (MRO-composable):
  ├── ValidatorServiceMixin         — calls @validate at layer boundary
  ├── TenantAwareService            — injects tenant_id into scope
  ├── SoftDeleteService             — filters deleted entities by default
  ├── CacheServiceMixin             — caches read/list results
  ├── BulkServiceMixin              — adds create_many(dtos) + delete_many(pks); single UoW per batch
  ├── AuditLogMixin                 — emits AuditEvent on _after_create/_after_update/_after_delete
  └── EventConsumer                 — listens to events, composes via register_to()

Rule: _async_check_entity runs after _check_entity in get(), update(), delete(), and BulkServiceMixin.delete_many()
Rule: BulkServiceMixin.create_many() authorizes once (type-level); delete_many() authorizes per entity (ownership may differ)
```

### Cache System

```
AsyncCache[K, V] (Protocol, runtime_checkable)
  └── Methods: get, set, delete, exists, clear, delete_prefix
  └── ⚠️ Deliberately UNCHANGED by Plan 011 C5 — see BulkCache below (D-11)

BulkCache[K, V] (Protocol, runtime_checkable — Plan 011 / C5 / D-11)
  └── Methods: get_many, set_many, delete_many
  └── A SEPARATE Protocol from AsyncCache — adding these methods to AsyncCache
      itself would silently flip isinstance(third_party_cache, AsyncCache) to
      False for every out-of-tree implementation

CacheBackend[K, V] (ABC, extends AsyncCache)
  ├── Abstract: _get(key), _set(key, value), _delete(key), _clear()
  ├── Concrete: InMemoryCache, NoOpCache, RedisCache (varco_redis), LayeredCache
  ├── Lifecycle: __aenter__, __aexit__, start(), stop()
  ├── Warming hook: add_warmer(warmer) → runs warmers in __aenter__ after start()
  ├── serializer= (Plan 011) — reuses varco_core.serialization.Serializer,
  │   never a second cache-specific protocol; None default preserves each
  │   backend's exact current behaviour (RedisCache→JsonSerializer,
  │   MemcachedCache→its bytes codec, InMemoryCache→raw objects)
  └── Concrete portable get_many/set_many/delete_many (Plan 011) — loops
      over get/set/delete; every shipped backend satisfies BulkCache
      immediately; RedisCache/MemcachedCache override with MGET/get_multi

InvalidationStrategy (ABC)
  ├── Concrete: TTLStrategy, ExplicitStrategy, TaggedStrategy
  │             EventDrivenStrategy, CompositeStrategy
  └── Lifecycle: start(), stop() called by hosting backend
  └── Rule: Never instantiate outside backend lifecycle — may hold subscriptions

LayeredCache — optional CacheBackplane (Plan 010 / C1): cross-node L1 invalidation
  └── CacheBackplane (ABC, varco_core.cache.backplane)
        ├── InMemoryBackplane (varco_core, test double)
        └── RedisPubSubBackplane (varco_redis.backplane)
  └── Rule: backplane= requires promote_ttl= (ValueError otherwise — bounded L1 staleness)

read_through(cache, key, loader, policy, *, singleflight=) — varco_core.cache.readthrough (Plan 010)
  ├── CachePolicy (frozen) — ttl/ttl_jitter/soft_ttl/negative_ttl/stale_if_error/singleflight
  ├── CacheEnvelope — wire format written only when policy.requires_envelope
  └── Singleflight / SingleflightProtocol — per-process stampede coalescer (C2)
  └── Shared by: @cached(policy=, singleflight=), CacheServiceMixin._cache_policy

read_through_many(cache, keys, loader, policy, *, singleflight=) — Plan 011 / C5 / D-12
  ├── Uses cache.get_many/set_many when cache satisfies BulkCache, else loops
  ├── Shares the SAME Singleflight instance/slots as read_through() — a bulk
  │   read and a single read of the same key coalesce rather than race
  └── CacheServiceMixin._use_bulk_cache = True (opt-in) routes list()'s single,
      already-namespaced list key through this — still one key per list call,
      not a genuine N-key batch read (see cache-hardening.md "Bulk operations")

CacheWarmer (ABC) — varco_core.cache.warming
  ├── QueryCacheWarmer(query_fn, ttl)    — calls query_fn(), populates key→value pairs
  ├── SnapshotCacheWarmer(snapshot_fn, ttl) — calls snapshot_fn(), bulk-loads dict
  └── CompositeWarmer(warmers)           — runs multiple warmers sequentially; stops on error
  └── Hook: backend.add_warmer(warmer)  — invoked once during __aenter__
```

### Repository & UnitOfWork

```
AsyncRepository[D, PK] (Protocol, runtime_checkable)
  └── Methods: save, get, delete, filter, find_one, find_many, count, exists

AsyncUnitOfWork (Protocol, runtime_checkable)
  └── Methods: begin, commit, rollback
  └── Pattern: async with uow: ... (context manager)

IUoWProvider (Protocol)
  └── get_uow() → AsyncUnitOfWork
```

### Query System

```
QueryParams (dataclass)
  ├── filters: list[FilterSpec]        (string-encoded like "age__gte=18")
  ├── sort: list[SortSpec]             ("+name", "-created_at")
  ├── limit: int, offset: int

QueryParser → FilterNode AST
  ├── ComparisonNode(field, op, value)
  ├── AndNode(left, right)
  ├── OrNode(left, right)
  └── NotNode(operand)
  └── All frozen dataclasses (immutable, hashable, cacheable)

ASTVisitor (ABC, visitor pattern)
  └── visit(node: FilterNode) → backend-specific (WHERE clause, etc.)

Concrete visitors:
  ├── SQLAlchemyFilterVisitor      (varco_core.query.applicator.sqlalchemy)
  ├── QueryOptimizer               (constant-folding, dead-branch elimination)
  └── TypeCoercionVisitor          (coerce string scalars to field types)

QueryTransformer (wiring)
  └── parse(params) → visit(ast) → apply(backend_query)

Aggregation (varco_core.query.aggregation) — separate from QueryParams
  ├── AggregationFunc (StrEnum): COUNT, SUM, AVG, MIN, MAX
  ├── AggregationExpression(func, field, alias)  — frozen dataclass; field=None for COUNT(*)
  ├── AggregationQuery(group_by, aggregations, having, limit, offset)  — frozen dataclass
  │   └── having: FilterNode | None  — reuses existing AST for WHERE-like HAVING clauses
  └── SQLAlchemyAggregationApplicator.apply(stmt, agg_query) → Select
      └── Maps AggregationExpression → func.count()/func.sum()/etc.
      └── having compiled via SQLAlchemyQueryCompiler (reuses filter visitor)
  └── Rule: Keep AggregationQuery separate from QueryParams — different cardinality (groups vs rows)
```

### Authority / JWT

```
JwtAuthority
  ├── from_pem(pem_bytes, kid, issuer, algorithm)
  ├── sign(claims: JwtBuilder) → str
  └── verify(token: str) → JwtPayload

MultiKeyAuthority (rotation)
  ├── rotate(new_authority: JwtAuthority)
  ├── retire(kid: str)
  └── sign/verify delegate to active authority

TrustedIssuerRegistry (multi-issuer verification)
  ├── from_env()
  ├── await load_all()
  └── await verify(raw_token) → JwtPayload

KeySource (ABC)
  ├── PemFile(path)
  ├── PemFolder(path)
  ├── JwksUrl(url)
  └── OidcDiscovery(issuer_url)
```

#### `varco_core.jwt.transform` — claim transformation (Plan 002)

Consumes foreign-shaped JWTs (Keycloak, Cognito, Auth0, a bespoke claim, …) by
mapping their claims onto the canonical set `JwtParser` reads —
`technical_docs/features/jwt-claim-transformer.md` is the full reference.

```
varco_core/jwt/
├── model.py       — JsonWebToken, _RESERVED_CLAIM_KEYS
├── builder.py     — JwtBuilder (+ as_profile())
├── parser.py       — JwtParser._from_raw_claims — the single funnel (SEAM 1 + SEAM 2)
├── util.py        — JwtUtil (+ matches_profile/profile_name/assert_profile)
├── exceptions.py  — JwtException, ClaimTransformError, TokenProfileError
├── config.py      — JwtVerificationSettings (VARCO_JWT_LEEWAY_SECONDS / VARCO_JWT_AUDIENCE)
├── profile.py     — TokenProfile, TokenProfileRegistry, resolve_token_profile()
└── transform/
    ├── path.py     — ClaimPath, MISSING, read_claim()
    ├── shape.py    — ValueShape, normalize()
    ├── mapping.py  — CanonicalClaim, ClaimRule, ClaimMapping
    ├── protocol.py — ClaimTransformer (Protocol), IdentityClaimTransformer, IDENTITY
    ├── mapper.py   — MappingClaimTransformer
    ├── registry.py — ClaimTransformerRegistry (per-issuer lookup)
    ├── config.py   — JwtTransformSettings (env) + JwtTransformConfig (global + per-issuer)
    └── runtime.py  — resolve/configure/reset process-global registry
```

**Type hierarchies**:

```
ClaimTransformer (Protocol, runtime_checkable)
  ├── IdentityClaimTransformer  (IDENTITY singleton — no-op, no copy)
  └── MappingClaimTransformer   (wraps a ClaimMapping — code- or env-configured)

TokenProfile (frozen dataclass)                — issuers/token_type/audiences/required_claims + implied_roles/scopes
TokenProfileRegistry                            — register/get/resolve (first-match)/matches/from_env()
```

**Pipeline** — one insertion point (`JwtParser._from_raw_claims`) covers every entry
point, because `TrustedIssuerRegistry.verify()` and `JwtBearerAuth`/`PassthroughAuth`
(varco_fastapi) all delegate to it:

```
raw JWT → PyJWT decode → raw claims
   → resolve_claim_transformer(iss) or explicit transformer=
   → transformer.transform(raw) → canonical claims
   → _build_auth_ctx(canonical) → AuthContext (roles/scopes/grants/tenant_id/actor)
   → resolve_token_profile(token) → merge implied_roles/scopes, tag metadata["token_profile"]
   → JsonWebToken
```

#### Token revocation (varco_core.revocation, Plan 034 / S13)

```
AbstractTokenRevocationStore (ABC)
  ├── revoke(entry: RevocationEntry) -> None
  ├── unrevoke(scope, key) -> bool
  ├── is_revoked(*, jti, subject, issuer, tenant_id, issued_at) -> RevocationVerdict
  ├── list_entries(scope=None) -> Sequence[RevocationEntry]
  └── delete_expired() -> int

  ├── NullTokenRevocationStore   (varco_core, scanned @Singleton default — no I/O, never revokes)
  ├── InMemoryTokenRevocationStore (varco_core — dev/test/single-process)
  └── RedisTokenRevocationStore  (varco_redis — production; one MGET per verification)
```

Four independent `RevocationScope` members (`TOKEN`/`SUBJECT`/`TENANT`/`ISSUER`) so a kill switch
never depends on a `jti` claim some issuers (Auth0, Keycloak, Cognito) do not emit.
`TrustedIssuerRegistry.verify()` is the single hook point — it consults
`self._revocation_store` (default `None`, zero cost) *after* `iss` enforcement and *before*
returning the parsed token. Binding a store via DI (`enable_token_revocation()` /
`varco_redis.di.enable_redis_token_revocation()`) does not by itself wire the registry — the
store must also be passed to `TrustedIssuerRegistry(revocation_store=...)` explicitly.
`varco_fastapi.auth.posture.inspect_auth_posture()` and
`varco_core.revocation.posture.inspect_revocation_posture()` are pure, read-only introspection
functions Plan 036 aggregates — no warning/raise/lifespan hook lives in either. Full design:
`technical_docs/features/credential-and-token-lifecycle.md`.

### Authorization — policy engine (varco_core.auth.policy)

```
AbstractAuthorizer (ABC)                         — service-layer gate; authorize(ctx, action, resource)
  ├── BaseAuthorizer            (permissive fallback, priority -(2**31))
  ├── GrantBasedAuthorizer / RoleBasedAuthorizer / OwnershipAuthorizer  (static, token-derived)
  └── PolicyEngineAuthorizer    (dynamic — bridges to a PolicyEngine; opt-in)

PolicyEngine (ABC)                               — enforce(EnforcementRequest) → bool  (hot path)
  └── CasbinPolicyEngine        (varco_casbin)   — also implements PolicyManagement
      └── OpaPolicyEngine        (varco_opa, design only — see opa-design.md)

PolicyManagement (ABC)                           — add/remove/list policies + role assignments + reload
  └── CasbinPolicyEngine        (varco_casbin)

RequestMapper                                    — (AuthContext, Action, Resource) → EnforcementRequest
  └── override subject_for / object_for / domain_for for custom keying / multi-tenant domains
```

### Resilience

```
@timeout(seconds: float)                        — async only, raises asyncio.TimeoutError
@retry(policy: RetryPolicy)                     — sync or async, exponential backoff
@circuit_breaker(config: CircuitBreakerConfig)  — sync or async, failure threshold + half-open state
@rate_limit(limiter, key_fn=None)               — async only, sliding-window call budget per key
@bulkhead(config: BulkheadConfig)               — async only, max-concurrency cap per dependency
@hedge(config: HedgeConfig)                     — async only, speculative duplicate for tail latency

CircuitBreaker (shared instance pattern)
  └── Rule: one per external dependency, not per-call — must accumulate failures
  └── Methods: protect(fn), state property (CLOSED/OPEN/HALF_OPEN)

Bulkhead (shared instance pattern — same rule as CircuitBreaker)
  └── Rule: one Bulkhead per external dependency — shared semaphore counts across all callers
  └── Methods: call(fn, *args), protect(fn)
  └── BulkheadConfig: max_concurrent (semaphore slots), max_wait (0.0 = fail-fast)
  └── RedisBulkhead (varco_redis, Plan 005 Phase 8 / U-7) — distributed sibling, same
      call()/protect()/available_slots() surface, fleet-wide max_concurrent via a Redis
      sorted set of holders (score = acquisition time) + Lua acquire/release mirroring
      RedisLock; TTL-based (slot_ttl) reclaim for a crashed holder's slot. Concurrency
      limiting (Bulkhead) and rate limiting (RateLimiter) are different primitives — a
      service can be within its rate budget and still overwhelm a dependency with
      concurrent in-flight calls.

RateLimiter (ABC — two implementations)
  ├── InMemoryRateLimiter  — per-process sliding window (collections.deque), single-node
  └── RedisRateLimiter     — distributed sliding window (Redis sorted set + Lua), multi-pod
  └── RateLimitConfig: rate (calls), period (seconds rolling window)
  └── @rate_limit(limiter, key_fn) — gates async callables; key_fn(*args, **kwargs) → str

HedgeConfig: delay (seconds before hedge fires), max_hedges (default 1)
  └── ⚠️  ONLY for idempotent operations (reads, upserts) — both copies may execute

Built into @listen:
  └── @listen(..., retry_policy=..., dlq=...) → wrapper built at register_to() time
```

Type hierarchy (resilience)::

    RateLimiter (ABC, varco_core)
      ├── InMemoryRateLimiter  (varco_core)   — per-process, deque-based
      └── RedisRateLimiter     (varco_redis)  — distributed, sorted-set + Lua

    Bulkhead           (varco_core)   — asyncio.Semaphore, shared per dependency
      └── RedisBulkhead  (varco_redis) — distributed, sorted-set + Lua, TTL reclaim
    CircuitBreaker     (varco_core)   — shared state machine, lazy asyncio.Lock

### Outbox Pattern

```
OutboxEntry (frozen dataclass)
  ├── event_type: str
  ├── event_id: str
  ├── aggregate_id: str
  ├── serialized: bytes
  └── created_at: datetime

OutboxRepository (ABC)
  ├── save_outbox(entry: OutboxEntry)
  ├── get_pending(limit: int) → list[OutboxEntry]
  ├── delete(entry_id: str)
  └── mark_failed(entry_id, *, attempts, next_attempt_at, error) — concrete, not abstract
      (Plan 005 Phase 3); default no-ops with a one-time warning per repo class

OutboxEntry gains (Plan 005 Phase 3, all defaulted): attempts: int = 0,
  last_error: str | None = None, next_attempt_at: datetime | None = None

OutboxRelay (background task)
  ├── poll loop: get_pending() → publish() → delete()
  ├── Rule: only place allowed to call AbstractEventBus directly (besides register_to)
  ├── Contract: push() to DLQ must never raise — logs errors and swallows
  └── (Plan 005 Phase 3) retry_policy= / dlq= / max_attempts= — attempts bumped +
      next_attempt_at scheduled via mark_failed() on failure; exhausted entries are
      dead-lettered then deleted; ValueError if max_attempts set without a dlq
```

### Audit Trail (varco_core.service.audit)

```
AuditEntry (frozen dataclass) — the persisted record
  ├── entry_id: UUID, entity_type: str, entity_id: str, action: "create"|"update"|"delete"
  ├── actor_id: str | None, diff: dict, occurred_at: datetime
  └── correlation_id: str | None, tenant_id: str | None

AuditRepository (ABC)
  ├── save(entry: AuditEntry) → None
  └── list_for_entity(entity_type, entity_id, *, limit=100) → list[AuditEntry]
  ├── SAAuditRepository (varco_sa)     — Postgres ON CONFLICT DO NOTHING on entry_id (idempotent);
  │                                      plain INSERT on other dialects (IntegrityError on dup)
  └── BeanieAuditRepository (varco_beanie) — plain insert() always; DuplicateKeyError on dup

AuditLogMixin (service mixin, MRO — compose LEFT of AsyncService)
  ├── overrides _after_create / _after_update / _after_delete
  └── emits AuditEvent via self._producer._produce(event, channel="varco.audit")

AuditConsumer (EventConsumer)
  └── @listen(AuditEvent, channel="varco.audit") → AuditRepository.save()
  └── Rule: safe-by-default (Plan 005 Phase 3, U-6 §2) — _default_retry_policy =
            RetryPolicy.durable_delivery() (max_attempts=20, base_delay=15.0, max_delay=3600.0)
            unless the caller overrides it; pass retry_policy=None explicitly to
            register_to() to restore the old fire-and-forget behaviour
  └── Rule: eventually consistent (post-commit event) — route through the outbox for
            "must not lose an audit record" guarantees
```

### Distributed Locking

```
AbstractDistributedLock (ABC) — varco_core.lock
  ├── try_acquire(key, *, ttl) → LockHandle | None  (non-blocking)
  ├── release(key, token)                            (token-guarded, phantom-safe)
  └── acquire(key, *, ttl, timeout=10.0) → LockHandle  (blocking, polling loop)

LockHandle (context manager)
  ├── key: str, token: UUID
  └── async with handle: ...   (auto-releases on exit)

InMemoryLock (varco_core) — asyncio.Lock per key, lazy dict; for unit tests only

RedisLock (varco_redis) — SET key NX PX ttl; release via Lua script (token check + DEL)
  └── Rule: release uses Lua script to atomically check token before DEL
            — prevents a slow holder from releasing a new owner's lock after TTL expiry

SAAdvisoryLock (varco_sa) — PostgreSQL pg_try_advisory_lock(int8) / pg_advisory_unlock(int8)
  ├── One pinned connection per held lock — connection closed on release()
  ├── String keys hashed to int64 via MD5 (first 8 bytes masked to 63 bits)
  ├── TTL accepted for API compatibility but NOT enforced at DB level
  │   (session-level lock lasts until connection closes)
  ├── ⚠️ UNSUPPORTED behind transaction-mode connection pooling (PgBouncer
  │   pool_mode=transaction) — release() may be routed to a different physical
  │   connection than try_acquire() used, leaking the lock (Plan 005 Phase 5, U-16).
  │   See technical_docs/features/distributed-locks.md for the failure sequence.
  └── Rule: NOT compatible with SQLite — PostgreSQL-specific advisory lock functions

SAXactAdvisoryLock (varco_sa) — PostgreSQL pg_try_advisory_xact_lock(int8) (Plan 005 Phase 5, U-16)
  ├── Same module as SAAdvisoryLock (varco_sa/advisory_lock.py) — shares _key_to_int64
  ├── xact(key, session) → AsyncIterator[bool]   — PRIMARY API
  │   └── Runs on the CALLER's session/transaction; released automatically at
  │       COMMIT/ROLLBACK — no release() call, no extra pinned connection.
  │       Safe under transaction-mode pooling by construction.
  ├── try_acquire(key, *, ttl) / release(key, token) → AbstractDistributedLock ABC shape
  │   └── Opens and pins its OWN connection+transaction for the lock's lifetime
  │       (same cost as SAAdvisoryLock, just transaction- not session-scoped).
  │       ttl is documented as MEANINGLESS (not merely unenforced) — the
  │       transaction's own commit/rollback is what bounds the lock.
  └── Recommended default whenever the deployment might sit behind a pooler.

varco_sa.di.SAModule — AbstractDistributedLock → SAAdvisoryLock (default, upgrade-safe binding);
  SAXactAdvisoryLock also registered as a directly-injectable singleton
  (Inject[SAXactAdvisoryLock]); override recipe: provide() before install()/scan(),
  or @Provider(priority=100) — see varco_sa/varco_sa/di.py

LockNotAcquiredError(Exception)
  └── Raised by acquire() when timeout expires before the lock is free
```

### Saga Orchestration

```
SagaStatus (StrEnum): PENDING, RUNNING, COMPLETED, COMPENSATING, COMPENSATED, FAILED

SagaStep (frozen dataclass)
  ├── name: str
  ├── execute: Callable[[dict], Awaitable[None]]
  └── compensate: Callable[[dict], Awaitable[None]]

SagaState (frozen dataclass)
  ├── saga_id: UUID
  ├── status: SagaStatus
  ├── completed_steps: int          — how many steps ran successfully (for compensation index)
  ├── context: dict[str, Any]       — shared mutable bag passed to every step
  └── error: str | None

AbstractSagaRepository (ABC)
  ├── save(state: SagaState) → None
  └── load(saga_id: UUID) → SagaState | None

AbstractSagaRepository implementations:
  ├── InMemorySagaRepository (varco_core)  — dict-backed; for unit tests
  ├── SASagaRepository (varco_sa)          — varco_sagas table; DELETE+INSERT upsert; SQLite-compatible
  └── BeanieSagaRepository (varco_beanie)  — varco_sagas collection; SagaDocument (Beanie Document)

SagaOrchestrator(steps, repository)
  ├── run(initial_context, *, saga_id=None) → SagaState
  │   └── Executes steps in order; persists state after each step
  ├── resume(saga_id) → SagaState
  │   └── Loads persisted state and continues from completed_steps
  └── _compensate(state, error)
      └── Runs compensations in REVERSE order (steps[n-1] → steps[0])
      └── Compensation failures are logged but do not prevent other compensations

Rule: compensation runs in reverse — each step must be idempotent (safe to re-run)
Rule: SagaOrchestrator persists state after every step — crash-safe resume is possible
```

### Inbox Pattern

```
AbstractInboxRepository (ABC) — varco_core.service.inbox
  ├── is_duplicate(message_id: str) → bool   (idempotency check)
  └── record(message_id: str) → None          (mark as processed)

Implementations:
  ├── InMemoryInboxRepository (varco_core) — set-backed; for unit tests
  ├── SAInboxRepository (varco_sa)         — varco_inbox table; INSERT OR IGNORE pattern
  ├── SAPollerInboxRepository (varco_sa)   — extends SAInboxRepository with TTL cleanup
  └── BeanieInboxRepository (varco_beanie) — InboxDocument; dedup via unique compound index

Table schema (varco_inbox): message_id (PK), received_at
Collection schema: message_id (unique), received_at

Rule: Always check is_duplicate() before processing — idempotency guard for at-least-once delivery
Rule: record() inside the same DB transaction as the business operation to avoid partial commits
```

### Idempotency-Key store (Plan 029 / D1a, D1b)

```
AbstractIdempotencyStore (ABC) — varco_core.idempotency.base
  ├── reserve(key, fingerprint, *, ttl) → ReserveOutcome   (the one atomic primitive — MUST be
  │                                                          atomic; every implementation's
  │                                                          native set-if-absent, never
  │                                                          exists()+set())
  ├── complete(key, record: IdempotencyRecord) → None      (store the final captured response)
  ├── get(key) → IdempotencyRecord | None                  (fetch a completed record, or None)
  ├── release(key) → None                                  (free an un-completable reservation —
  │                                                          streaming/over-ceiling responses)
  └── delete_expired() → int                                (best-effort sweep; no-op where the
                                                              backend has native TTL)

ReserveOutcome (Enum): ACQUIRED | IN_FLIGHT | REPLAY
IdempotencyRecord (frozen dataclass): status, body: bytes, headers, fingerprint, created_at

Implementations, each using its own backend's native atomic primitive:
  ├── InMemoryIdempotencyStore (varco_core)   — a lazily-created asyncio.Lock; single-process
  │                                              only (same warning as InMemoryRateLimiter)
  ├── RedisIdempotencyStore (varco_redis)     — SET key value NX PX ttl
  ├── SAIdempotencyStore (varco_sa)           — varco_idempotency table; UNIQUE(key) +
  │                                              IntegrityError on a losing INSERT
  └── BeanieIdempotencyStore (varco_beanie)   — IdempotencyDocument; unique index +
                                                 DuplicateKeyError on a losing insert

Conformance suite: testkit/varco_conformance/idempotency_store.py's IdempotencyStoreConformance,
subclassed by all four — including a genuine asyncio.gather() concurrency race asserting exactly
one ACQUIRED (§D-D1-atomic's load-bearing guarantee).

Distinct from the Inbox Pattern above: the inbox persists an *incoming event* before a handler
runs (bus → handler gap) and deletes the entry once processed; this store persists an *outgoing
response* to suppress re-execution and the record must *survive* processing for its full TTL —
opposite lifecycles, not two implementations of the same idea.

HTTP adapter: IdempotencyMiddleware (varco_fastapi.middleware.idempotency) — the only piece that
knows about headers/status codes/streaming detection. Full design: `technical_docs/features/idempotency-key.md`.
```

### Job Store

```
AbstractJobStore (ABC) — varco_core.job.base
  ├── save(job: Job, *, expected_epoch: int | None = None) → None    (upsert; fenced write)
  ├── get(job_id: UUID) → Job | None
  ├── list_by_status(status, *, limit=100) → list[Job]  (ordered created_at ASC)
  ├── delete(job_id: UUID) → None        (silent no-op for unknown IDs)
  ├── delete_where(*, status=, completed_before=, expires_before=, limit=) → int
  │     (Plan 005 Phase 6, U-18 — concrete default; ValueError with no predicate at all)
  ├── try_claim(job_id, *, owner_id=None, lease_ttl=None) → Job | None
  │     (PENDING → RUNNING; atomic; honours run_at IS NULL OR run_at <= now; Plan 005 Phase 4)
  ├── claim_next(*, owner_id=, lease_ttl=, now=) → Job | None
  │     (concrete default: list_by_status(PENDING) + try_claim loop — correct, slower)
  ├── renew(job_id, *, owner_id, epoch, lease_ttl) → Job | None
  │     (concrete-but-raises NotImplementedError by default — no correct lease fallback)
  ├── reap_expired_leases(*, now=, limit=100) → list[Job]
  │     (concrete-but-raises NotImplementedError by default; RUNNING → PENDING, epoch+1)
  ├── supports_zoned_schedules: ClassVar[bool] = False  (Plan 011 / RD-5 — a store must
  │     declare this before AbstractJobRunner.enqueue(tz=...) may target it)
  └── list_pending_zoned(before, *, limit=100) → list[Job]  (Plan 011 / T2
        — portable default: list_by_status(PENDING) + in-Python filter;
        SAJobStore overrides with a real WHERE run_at_tz IS NOT NULL clause)

Job (frozen dataclass): job_id, status, created_at, started_at, completed_at,
                        result (bytes), error, callback_url, auth_snapshot, request_token,
                        metadata, task_payload (TaskPayload | None),
                        run_at, attempt, max_attempts, owner_id, lease_expires_at, lease_epoch,
                        expires_at, request_issuer, request_subject, request_token_hash,
                        store_raw_token: bool = True,  (Plan 005 Phases 4 & 6 — all defaulted)
                        run_at_wall: datetime | None = None,   (Plan 011 / T2 — the INTENT;
                        run_at_tz: str | None = None,           run_at above stays the
                        run_at_fold: int = 0                    MATERIALIZATION, D-7)
  ├── Transition helpers: as_running(), as_completed(result), as_failed(error), as_cancelled(),
  │                       as_retry(next_run_at), as_dead(error)
  ├── request_token: discouraged (Plan 005 Phase 6, U-19) — docstring-only, no
  │     DeprecationWarning, no removal (matches JwtUtil.SYSTEM_ISSUER precedent) —
  │     a JWT is base64-encoded not encrypted; prefer request_issuer/request_subject/
  │     request_token_hash instead
  └── __post_init__: when store_raw_token=False and request_token is set, computes
        request_token_hash = sha256(request_token).hexdigest() and clears request_token to None

JobStatus (StrEnum): PENDING, RUNNING, COMPLETED, FAILED, CANCELLED, DEAD (Plan 005 Phase 4)

StaleLeaseError (Exception) — raised by save(expected_epoch=...) on a fenced-out write

TaskPayload (dataclass): task_name, args, kwargs  — for recoverable background jobs

Implementations:
  ├── SAJobStore (varco_sa)
  │   ├── varco_jobs table (Core, not ORM — own MetaData: jobs_metadata)
  │   ├── save(): DELETE + INSERT (upsert, compatible with SQLite and PostgreSQL)
  │   ├── try_claim(): SELECT FOR UPDATE SKIP LOCKED on PostgreSQL (dialect-detected)
  │   │   └── plain SELECT + UPDATE on SQLite and other dialects (single-process safe)
  │   ├── native claim_next/renew/reap_expired_leases as single atomic UPDATEs
  │   ├── native delete_where(): single DELETE; limit form uses
  │   │     ctid IN (SELECT ctid ... LIMIT n) on PostgreSQL / rowid on SQLite
  │   │     (index-friendly given ix_varco_jobs_expires) — Plan 005 Phase 6, U-18
  │   ├── indexes (Plan 005 Phase 4): ix_varco_jobs_claim(status,run_at,created_at),
  │   │     ix_varco_jobs_lease(status,lease_expires_at), ix_varco_jobs_expires(expires_at)
  │   └── ensure_table() / jobs_metadata for Alembic integration
  ├── RedisJobStore (varco_redis) — JSON-per-key + status ZSET index; claim guard key (SET NX EX)
  │     now also honours run_at and writes lease fields on claim
  │     native delete_where(): walks the per-status ZSET index(es), skips/cleans stale entries
  └── BeanieJobStore (varco_beanie)
      ├── varco_jobs collection (JobDocument, UUID primary key)
      ├── save(): find().delete() + doc.insert() (upsert via two round-trips)
      ├── try_claim(): find_one(...).update_one({"$set": ...}, response_type=NEW_DOCUMENT)
      │   └── MongoDB findAndModify — atomic PENDING → RUNNING in one server-side op;
      │       run_at/lease fields referenced via plain dict filters (not typed
      │       ExpressionFields) so they work identically pre-/post-init_beanie()
      └── native delete_where(): single delete_many(); limit form selects matching
          _ids first then deletes by that id set (no native "delete with limit")

Rule: try_claim() must be atomic — SAJobStore uses SELECT FOR UPDATE SKIP LOCKED;
      BeanieJobStore uses MongoDB findAndModify — both prevent double-claiming across replicas
Rule: include jobs_metadata (SAJobStore) or JobDocument (BeanieJobStore) in your init call
Rule: save() has upsert semantics — always safe to call on terminal jobs (COMPLETED, FAILED)
Rule: delete_where() with no predicate at all raises ValueError — chunk large sweeps with
      limit= in a loop until it returns 0 (avoids pinning a pooled connection; Plan 005 Phase 6)

AbstractJobRunner (ABC) — enqueue(job, coro, *, run_at=None, delay=None,
  run_at_wall=None, tz=None, fold=0, gap=GapPolicy.NEXT_VALID,
  overlap=OverlapPolicy.FIRST) — abstract signature declares the Plan 011 T2
  kwargs; _prepare_zoned_job(job, store, ...) is the concrete RD-5 guard +
  materialization static helper every concrete enqueue() is expected to call.
  ├── JobRunner (varco_fastapi) — retry_policy=/dlq= (Plan 005 Phase 4): failure with
  │     attempt+1 < max_attempts → Job.as_retry(); attempts exhausted → as_dead()+DLQ push
  │     when dlq wired, else as_failed() (today's exact behaviour when both are None)
  │     enqueue(job, coro, *, run_at=, delay=, run_at_wall=, tz=, fold=, gap=, overlap=) —
  │     extended with the T2 kwargs (Plan 011 drift-fix pass) and calls _prepare_zoned_job()
  │     before store.save(); tz=None (default) is a pure passthrough
  └── enqueue_task(..., store_raw_token: bool = True) (Plan 005 Phase 6, U-19)
        — forwarded to Job(); False clears request_token via __post_init__, so
          _fire_callback()'s Authorization: Bearer forwarding is skipped (callback
          must then authenticate some other way — service credential / mTLS / signed URL)

VarcoRouter._store_raw_token: ClassVar[bool] = True (Plan 005 Phase 6, U-19)
  └── Read by router.base._submit_job() when auto-populating the Job for async-offloaded
      CRUD/custom routes — same True-default/False-opt-out contract as enqueue_task() above

JobPoller (varco_fastapi) — lease_aware: bool = True (Plan 005 Phase 4)
  ├── both signals run every tick, over disjoint sets: reap_expired_leases() reaps RUNNING
  │     jobs holding an EXPIRED lease (lease_expires_at <= now); the wall-clock stale_threshold
  │     still owns RUNNING jobs with NO lease at all (lease_expires_at IS NULL) — a NULL lease
  │     is not an expired lease, so it is invisible to reap_expired_leases(). The lease-reap
  │     step is skipped entirely (falling back to stale_threshold alone) when the store raises
  │     NotImplementedError (no lease support)
  └── retention_sweep: bool = False, retention_batch_size: int | None = None (Plan 005 Phase 6, U-18)
        — when True, each poll tick also calls
          store.delete_where(expires_before=now, limit=retention_batch_size); one bounded
          call per tick (not looped to drain in one shot) so a large backlog is worked off
          gradually; default False — no deployment starts deleting rows on upgrade
```

### Conversation Store

```
AbstractConversationStore (ABC) — varco_core.service.conversation
  ├── append(task_id: str, turn: ConversationTurn) → None
  ├── get(task_id: str) → list[ConversationTurn]
  ├── delete(task_id: str) → None
  └── turn_count(task_id: str) → int

ConversationTurn (frozen dataclass): role: str, content: Any, timestamp: datetime

Implementations:
  ├── RedisConversationStore (varco_redis) — Redis List per task_id; RPUSH / LRANGE / LLEN
  │   ├── key_prefix: str (default "varco:conv:")
  │   └── ttl_seconds: int | None (refreshed on each append)
  └── SAConversationStore (varco_sa) — varco_conversation_turns table; turn_id (UUID) PK
      └── turn_count uses COUNT(*) — O(1) via index on task_id

Table schema (varco_conversation_turns): turn_id (PK), task_id (indexed), role, content (JSON), turn_ts
```

### Connection Pool Metrics (varco_sa)

```
SAPoolMetrics (frozen dataclass) — varco_sa.pool_metrics
  ├── size: int               — engine pool_size
  ├── checked_out: int        — connections currently in use
  ├── checked_in: int         — idle connections in pool
  ├── overflow: int           — connections above pool_size (up to max_overflow)
  ├── max_overflow: int       — upper overflow limit (-1 = unlimited)
  ├── invalid: int            — invalidated (stale) connections
  ├── pool_type: str          — e.g. "QueuePool", "NullPool", "StaticPool"
  ├── captured_at: datetime   — UTC timestamp of the snapshot
  ├── is_saturated: bool      — True when checked_out >= size + max_overflow (and both > 0)
  └── utilisation: float      — fraction of total capacity in use, in [0.0, 1.0]

pool_metrics(engine: AsyncEngine) → SAPoolMetrics
  └── Reads engine.sync_engine.pool stats; returns zeroed snapshot for NullPool/StaticPool

SAFastrestApp.pool_metrics() → SAPoolMetrics  (convenience method on bootstrap object)
```

### Schema Migrations (varco_core.migration + backends)

```
AbstractMigrator (ABC) — varco_core.migration.base
  ├── async plan() → MigrationPlan                                    [abstract]
  ├── async upgrade(target="heads", *, dry_run=False) → MigrationReport [abstract]
  ├── async downgrade(target) → MigrationReport                       [abstract]
  ├── async stamp(target="heads") → None                              [abstract]
  ├── async check() → MigrationPlan   — plan() + raise if pending     [CONCRETE]
  └── async close() → None            — no-op; engines override       [CONCRETE]
        ↑
        ├── AlembicMigrator      — varco_sa.migration.migrator   (needs varco-sa[migrations])
        │     ├── __init__(engine, *, script_location=None, version_locations=(),
        │     │            include_framework_branch=True, lock=None, settings=None)
        │     ├── async adopt_framework_tables() → list[str]   — the ensure_table() bridge
        │     └── headless Alembic (EnvironmentContext/MigrationContext — no env.py needed);
        │          transaction_per_migration=True, compare_type=True
        ├── BeanieMigrator       — varco_beanie.migration.migrator
        │     └── __init__(db, registry, *, index_guard=None, index_mode="check",
        │                  settings=None, verify_checksums=True, owner_id=None)
        └── InMemoryMigrator     — varco_core.migration.inmemory  (standard unit-test double)

Value objects (all @dataclass(frozen=True)) — varco_core.migration.base
  ├── Revision(id, label, branch=None)      — branch="varco" for framework revisions
  ├── MigrationPlan(current, pending)       — .is_empty, .format()
  └── MigrationReport(applied, duration_s, skipped_locked=False)  — .format()

MigrationSettings (frozen dataclass) — varco_core.migration.settings
  └── from_env(env=None): mode="off" | on_failure="fail" | lock_key="varco:migrate"
      | lock_timeout=30.0 | timeout=300.0 | target="heads" | dry_run=False
      (VARCO_MIGRATE_MODE / _ON_FAILURE / _LOCK_KEY / _LOCK_TIMEOUT / _TIMEOUT
       / _TARGET_REV / _DRY_RUN)

SchemaMigrationError(Exception) — varco_core.migration.errors
  ├── PendingMigrationsError(plan)              — carries the SchemaMigrationPlan
  ├── MigrationLockTimeout(lock_key, waited_s)
  ├── IrreversibleMigrationError                — Mongo migration with no down()
  └── MigrationBackendUnavailable               — message names the pip install line

Supporting surfaces
  ├── varco_sa.migration.lock.migration_lock(engine, key, *, timeout, poll_interval, lock)
  │     └── dedicated NullPool conn · SET LOCAL idle_in_transaction_session_timeout = 0
  │          · SAXactAdvisoryLock.xact() · COMMIT IS the release
  ├── varco_sa.migration.env_template: include_object · configure_kwargs()
  ├── varco_sa.migration.ops: rls_upgrade(op, table, …) · rls_downgrade(op, table, …)
  ├── varco_sa.metadata: framework_metadata() · framework_table_names()
  │                       · register_framework_metadata(name, md)
  ├── varco_beanie.migration: Migration (version/name/up/down) · MigrationRegistry
  │                           (register/discover/ordered) · MigrationStore · IndexReconciler
  ├── varco_fastapi.migrate.MigrationLifecycle(*migrators, settings=None)
  │     └── structurally satisfies AbstractLifecycle; PREPENDED into VarcoLifespan
  └── varco_core.cli: `varco migrate …` (entry-point group "varco.commands")
```

Renamed in 3.0.0 (Plan 022 / AB-2) from `MigrationError`/`MigrationPlan`, which collided at the
`varco_core` top level with the unrelated `varco_core.migrator` (domain data/field migration)
pair. Both schema names are now re-exported from `varco_core` directly; the old names still
resolve from `varco_core.migration` as deprecated aliases to the identical objects until 4.0.0.

See `technical_docs/features/schema-migrations.md`.

### Multitenancy (varco_core.tenancy + backends)

```
varco_core.tenancy                            ← contracts only, zero third-party deps
  TenantIsolation(SHARED|SCHEMA|DATABASE) · TenantScope(TENANT|GLOBAL) · TenantStatus
  TenancySettings.from_env() · TenantDescriptor
  AbstractTenantCatalog · StaticTenantCatalog · CachedTenantCatalog
  TenantResourcePool[T]                        — bounded LRU pool, lease-refcounted
  DynamicTenantUoWProvider                     — new IUoWProvider (TenantUoWProvider untouched)
  GlobalUoWProvider                            — distinct DI-token type, no ABC change
  AbstractTenantProvisioner · ExternalTenantProvisioner
  validate_service_scope() · tenancy_cache_key() · GlobalScopeReadOnlyError
  control/  → TenantProvisionRequested/Deprovisioned · TenantCatalogChanged
              TenantControlService · TenantProvisionConsumer (retry+DLQ by default)
  fanout.py → TenantFanoutSupervisor           — one OutboxRelay/JobPoller/AuditConsumer
                                                   child per active, pool-resident tenant
  migration/fanout.py → TenantFanoutMigrator   — global run, then every tenant, sorted
        ▲                                      ▲
varco_sa.tenancy                        varco_beanie.tenancy
  SASchemaRouter (schema_translate_map)   BeanieTenantPool · BeanieTenantBinding
  SAEngineRegistry (per-tenant AsyncEngine) (per-tenant Document clones + init_beanie)
  SASchemaProvisioner · SADatabaseProvisioner  BeanieDatabaseProvisioner (dropDatabase)
  SATenantCatalog (varco_tenants, 10th table)  BeanieTenantCatalog
  rls_check.assert_rls_enabled (skips GLOBAL)
  global_scope (42501 → GlobalScopeReadOnlyError)
  admin/  → SAAdminEngine, SADatabaseProvisioner    (control plane ONLY, RD-4)
        ▲                                      ▲
        └────────────── varco_fastapi.tenancy ─┘
              TenancyLifecycle (stops supervisor before pool.aclose())
              TenantResolutionMiddleware (checks status BEFORE pool.ensure())
              build_tenant_router() · mount_tenant_admin() (RD-9, no env-var path)
```

`varco_fastapi.tenancy` imports only `varco_core.tenancy` — never `varco_sa`,
`varco_beanie`, `sqlalchemy`, or `pymongo` (import-guard test). `varco_tenants` is the
tenth framework table, picked up by the existing dynamic `0001_varco_framework_baseline`
Alembic revision via `framework_metadata()` — no dedicated migration file.

See `technical_docs/features/multitenancy.md`.

### Outbound webhooks (varco_core.webhook, Plan 031 / D4)

```
WebhookSubscriptionRepository (ABC) — varco_core.webhook.base
  ├── save(subscription) → WebhookSubscription
  ├── find_by_id(pk) → WebhookSubscription | None
  ├── find_by_tenant(tenant_id) → list[WebhookSubscription]     (never leaks across tenants)
  ├── find_active_matching(event_type, *, tenant_id=None) → list[WebhookSubscription]
  └── delete(pk) → None

Implementations (framework-table shape, same convention as the DLQ/idempotency/outbox tables):
  ├── InMemoryWebhookSubscriptionRepository (varco_core) — single-process, lazily-created lock
  ├── SAWebhookSubscriptionRepository (varco_sa)          — own Table/MetaData, manual mapping
  └── BeanieWebhookSubscriptionRepository (varco_beanie)  — self-managed Motor client + init_beanie

Conformance suite: testkit/varco_conformance/webhook_subscription.py's
WebhookSubscriptionRepositoryConformance, subclassed by all three (COVERAGE.md).

WebhookSigner (ABC) — varco_core.webhook.signing
  ├── StandardWebhooksSigner (default) — webhook-id / webhook-timestamp / webhook-signature,
  │                                        HMAC-SHA256 over "{id}.{timestamp}.{payload}",
  │                                        space-delimited multi-signature (rotation)
  └── Rfc9421Signer (opt-in)           — Signature-Input / Signature / Content-Digest (RFC 9530),
                                          covers @method/@target-uri/@authority/content-digest

validate_target(url, ...) — varco_core.webhook.ssrf — the five-layer SSRF guard: scheme
allowlist, resolve-then-validate-then-PIN (defeats DNS rebinding), blocked-by-default deny list
+ optional exclusive allowlist, no redirect following, explicit IPv6-equivalent coverage
(including the ::ffff:<private-v4> bypass form).

WebhookDispatcher (EventConsumer) — varco_core.webhook.dispatcher
  ├── never holds AbstractEventBus — wired via register_to(bus, dlq=...) from @PostConstruct
  ├── @listen(WebhookTriggerEvent) — an app republishes its own domain events as this envelope
  ├── runs its own per-subscription retry loop on RetryPolicy (NOT the generic
  │   @listen(retry_policy=..., dlq=...) wrapper — one event can match N subscriptions that must
  │   each retry/DLQ/auto-disable independently)
  └── exhaustion → AbstractDeadLetterQueue.push() (never raises); auto-disable after
      WebhookSettings.disable_after_failures consecutive failures

WebhookSubscription / WebhookDelivery (DomainModel) — varco_core.webhook.models
  hand-rolled framework tables (webhook_subscriptions / webhook_deliveries), not the generic
  @register + AsyncRepository[T] translation layer — same reasoning as the idempotency/DLQ tables.

Admin: varco_fastapi.webhook.mount_webhook_admin(app, *, repository=, redriver=None,
acknowledge_bundled_admin=True, server_auth=..., admin_role="webhook-admin", prefix="/webhooks")
— same acknowledge-gated shape as mount_reliability_admin/mount_tenant_admin (RD-9); replay goes
through the existing DlqRedriver.

Full design (signing-scheme decision, the SSRF model, retry-schedule convention, a Pitfalls
table): `technical_docs/features/outbound-webhooks.md`. Usage: README's "Outbound webhooks"
section.

### Inbound webhook verification (varco_core.webhook.inbound, Plan 038 / S19)

```
WebhookVerifier (ABC) — varco_core.webhook.inbound.base
  ├── provider: str                                       (property, e.g. "stripe")
  └── verify(*, body: bytes, headers: Mapping[str, str]) → VerificationResult

VerificationResult (frozen dataclass) — verified, provider, message_id, timestamp_checked, failure
VerificationFailure (StrEnum) — MISSING_HEADER / MALFORMED_HEADER /
                                 TIMESTAMP_OUT_OF_TOLERANCE / SIGNATURE_MISMATCH
SecretEncoding (StrEnum)      — STANDARD_WEBHOOKS_B64 / RAW_UTF8

HmacWebhookVerifier (WebhookVerifier) — varco_core.webhook.inbound.verifiers
  shared template: case-insensitive header lookup, tolerance-window check, constant-time
  any-secret-matches loop. Subclassed by:
  ├── StripeWebhookVerifier   — Stripe-Signature: t=<ts>,v1=<hex>[,v1=<hex>...]
  ├── GitHubWebhookVerifier   — X-Hub-Signature-256: sha256=<hex>; timestamp_checked always
  │                             False; refuses construction without replay_guard= or
  │                             acknowledge_no_replay_protection=True
  └── SlackWebhookVerifier    — X-Slack-Signature + X-Slack-Request-Timestamp

StandardWebhooksVerifier (WebhookVerifier) — delegates the signature decision to a held
  StandardWebhooksSigner (§D-S19-gap — no second HMAC implementation for the scheme varco
  already ships). Accepts both webhook-*/svix-* header names.
  └── SvixWebhookVerifier (StandardWebhooksVerifier) — provider == "svix" only; identical logic

get_verifier(provider, *, secrets, settings=None, **kwargs) → WebhookVerifier
  mirrors signing.get_signer; threads WebhookSettings.inbound_tolerance_seconds unless an
  explicit tolerance_seconds= override wins.

WebhookReplayGuard (frozen dataclass) — varco_core.webhook.inbound.replay
  ├── holds an AbstractIdempotencyStore (no new ABC — §D-S19-replay)
  ├── claim(provider, message_id, body)   — reserve(); IN_FLIGHT/REPLAY → WebhookReplayError
  ├── complete(provider, message_id, body) — call after a clean handler return
  └── release(provider, message_id)        — call on handler failure; makes a provider retry
                                              acceptable again

WebhookSignatureError (ServiceException, 401) / WebhookReplayError (ServiceConflictError, 409)
  — varco_core.exception.webhook; both error_params() → {} (never leak the secret/signature)
```

`varco_fastapi.webhook.verify_webhook(verifier, *, replay_guard=None)` builds a `yield`
dependency (`VerifiedWebhook`) — the only FastAPI-specific piece; no middleware, no ordering-table
edit (owned by a sibling plan). `varco_core/webhook/__init__.py` carries no new top-level name for
any of this — reached via `varco_core.webhook.inbound` directly.

Full design (the provider divergence table, the replay model, secret sourcing, the raw-body/
body-limit story, a Pitfalls table): `technical_docs/features/inbound-webhooks.md`. Usage:
README's "Inbound webhook verification" section.
```

### Feature flags (varco_core.flags, Plan 032 / D7)

```
AbstractFeatureFlags (ABC) — varco_core.flags.base
  ├── resolve_bool(key, default, *, context=None)   → FlagResolution[bool]
  ├── resolve_string(key, default, *, context=None) → FlagResolution[str]
  ├── resolve_numeric(key, default, *, context=None)→ FlagResolution[float]
  └── resolve_object(key, default, *, context=None) → FlagResolution[Any]
  (every resolver degrades to the caller's own `default` on an unconfigured key — never raises)

FlagEvaluationContext (frozen dataclass) — varco_core.flags.base
  ├── tenant_id: str | None    — from current_tenant(), NEVER RequestContext (tenant SSOT rule)
  ├── attributes: dict[str, str] — from current_request_context().extras
  └── .current() — builds one from ambient state

FlagResolution[T] (frozen dataclass, Generic) — value + optional `reason` string

Implementations:
  ├── NullFeatureFlags (varco_core) — scanned @Singleton, priority=-sys.maxsize-1, the DI default;
  │                                    every resolver returns the caller's default, reason="NULL"
  └── InMemoryFeatureFlags (varco_core) — dict-backed, optional per-tenant overrides;
                                           tenant override → global value → caller's default

DI: varco_core.flags.di.enable_feature_flags(container) — opt-in @Provider binding
InMemoryFeatureFlags → AbstractFeatureFlags, shadowing NullFeatureFlags. Deliberately NOT a
scanned @Configuration (would auto-activate on any `container.scan("varco_core", recursive=True)`).

Un-park trigger for an OpenFeatureFlags adapter: `openfeature-sdk` (the SDK, not the spec)
reaches 1.0.0 — checked NOT fired as of 2026-09-04 (SDK 0.10.0, spec 0.9.0). Full version
evidence: `technical_docs/features/feature-flags.md`. Usage: README's "Feature flags" section.
```

### Recurring schedules (varco_core.schedule, Plan 032 / D6)

```
Schedule (DomainModel) — varco_core.schedule.entity
  schedule_id: UUID (stable domain identity, independent of `pk`)
  tenant_id: str | None       cron_expr: str        timezone: str (IANA)
  enabled: bool = True
  gap_policy: GapPolicy = NEXT_VALID       overlap_policy: OverlapPolicy = FIRST
  catchup_policy: CatchUpPolicy = SKIP     max_backfill: int = 100
  last_materialized_at: datetime | None    payload: dict[str, Any]   callback_url: str | None
  task_name: str | None = None   (Plan 039/S20 — set ⇒ _build_job emits a TaskPayload, making a
                                   materialized Job reachable through JobRunner.recover(); None
                                   (default) ⇒ task_payload=None, byte-identical to pre-3.2)
  (no execution path here — the existing AbstractJobRunner runs materialized Job rows unchanged)

CatchUpPolicy (StrEnum) — varco_core.schedule.entity
  SKIP (default) | FIRE_ONCE | BACKFILL_ALL (bounded by max_backfill)

parse_cron(expr) → CronSchedule — varco_core.schedule.cron
  hand-rolled, zero-dependency 5-field parser + next_after()/at_or_before(); RRULE parked
  (§D-D6-cron — a complete RFC 5545 engine means a new dateutil.rrule runtime dependency)

ScheduleMaterializer(job_store=) — varco_core.schedule.materializer
  .materialize(schedule, *, now=None) → list[Job]
  ├── computes due occurrence(s) per catchup_policy, resolves each through resolve_zoned()
  │   (varco_core.tz.schedule) → Job.run_at (UTC) + run_at_wall/run_at_tz/run_at_fold (intent)
  └── ⚠️ deviates from the plan's draft mechanism — does NOT use the job store's fenced-lease
      primitives (a synthetic lease row would corrupt list_by_status()/all_jobs() counts).
      Instead: a deterministic uuid5 occurrence Job.job_id (cross-process idempotent-upsert
      convergence via AbstractJobStore.save()) + a lazily-created, per-schedule-id asyncio.Lock
      (in-process exclusivity only). UNIQUE(schedule_id) on the Schedule row backstops identity;
      occurrence-level uniqueness lives entirely in the deterministic job id, not a
      UNIQUE(schedule_id, run_at) index (Schedule has no run_at column).

AbstractScheduleRepository (ABC) — varco_core.schedule.repository
  ├── save(schedule) → Schedule      ├── find_by_id(pk) → Schedule | None
  ├── find_all_enabled() → list[Schedule]   └── delete(pk) → None
  Implementations:
  ├── InMemoryScheduleRepository (varco_core) — single-process, lazily-created lock
  ├── SAScheduleRepository (varco_sa)          — own Table/MetaData, migrations 0007_schedules_table
  │                                                + 0008_schedule_task_name (Plan 039/S20)
  └── BeanieScheduleRepository (varco_beanie)  — self-managed Motor client + init_beanie

Full design (the fenced-lease deviation, a Pitfalls table for DST gaps/catch-up surprise/
materializer downtime): `technical_docs/features/recurring-schedules.md`. Usage: README's
"Recurring schedules" section.
```

### Retention & purge automation (varco_core.retention, Plan 039 / S20)

```
RetentionPolicy (frozen dataclass) — varco_core.retention.policy
  name: str   target: RetentionTarget   cron_expr: str   timezone: str
  dry_run: bool  (REQUIRED, no default)
  older_than: timedelta | None = None   enabled: bool = True
  batch_size: int = 1000   max_batches: int = 100
  acknowledge_short_retention: bool = False   tenant_ids: tuple[str, ...] | None = None

RetentionRegistry — varco_core.retention.policy
  .register(policy)   .get(name)   .__contains__(name)   .__iter__()
  .schedule_id_for(name) → UUID   (uuid5 seed — the Schedule.schedule_id this policy materializes onto)

RetentionOutcome (frozen) — one RetentionTarget.purge() call's result
RetentionResult (frozen)  — one policy execution's aggregate (across every batch/tenant)
  policy  kind  examined: int | None  deleted  would_delete  batches  truncated  dry_run
  skipped_reason: str | None   duration_s: float   error: str | None

RetentionTarget (ABC) — varco_core.retention.base
  kind: str (property)   supports_older_than: ClassVar[bool]   supports_dry_run: ClassVar[bool]
  .purge(*, older_than, limit, dry_run) → RetentionOutcome
  Implementations — varco_core.retention.targets:
  ├── DlqRetentionTarget          — wraps AbstractDeadLetterQueue.delete_where; requires
  │                                  acknowledge_dead_letter_deletion=True
  ├── AuditRetentionTarget        — wraps AuditRepository.delete_where; allow_chain_break ctor arg
  ├── IdempotencyRetentionTarget  — wraps AbstractIdempotencyStore.delete_expired (no cutoff/preview)
  ├── RevocationRetentionTarget   — wraps AbstractTokenRevocationStore.delete_expired (same shape)
  └── JobRetentionTarget          — wraps AbstractJobStore.delete_where
  CallableRetentionTarget — the out-of-tree escape hatch (varco_core.retention.base)

RetentionScheduler(registry, *, schedule_repo, job_store, task_registry, job_runner=None,
                    interval=0.0) — varco_core.retention.scheduler
  .start()/.stop()   .ensure_schedules()   .sweep_once() → int   .purge_policy(*, policy) → RetentionResult
  ⚠️ No fenced lease — relies on the materializer's uuid5+upsert convergence and
     JobRunner.recover()'s try_claim(), exactly like any other Schedule.
  job_runner=None ⇒ materialize only, never dispatch (an app with its own dispatch loop).
execute_policy(policy) → RetentionResult   — the shared implementation behind both
  RetentionScheduler.purge_policy() and `varco retention prune --policy`
RetentionPolicyNotFoundError — raised when a dispatched policy name is no longer registered

inspect_retention_posture(registry=None) → RetentionPostureReport — varco_core.retention.posture
  configured  policy_count  destructive_count  dry_run_count
  platform_wide_policies: tuple[str, ...]   dlq_policies   short_retention_policies   kinds: frozenset[str]

bind_retention_registry(container, registry) — varco_core.retention.di (bind_* verb, no lifecycle
  side effect, same shape as varco_core.tls.bind_trust_store)
install_retention_metrics() — varco_core.observability.retention (install_* verb, opt-in counters)

RetentionLifecycle(registry, *, container, interval=0.0) — varco_fastapi.retention
  .startup()/.shutdown()  +  .start()/.stop() aliases (ReliabilityLifecycle shape)
  create_varco_app(retention=RetentionLifecycle(...))  — appended, never prepended

⛔ No module-level @Singleton/@Provider/@Configuration anywhere under varco_core.retention —
   a scanned @Configuration would start a deletion loop in every app scanning varco_core.

Full design (the eight safety guards, the three cron→Job driver gaps this closed, multi-process
convergence, tenancy scoping, a Pitfalls table): `technical_docs/features/retention-and-purge.md`.
Usage: README's "Retention & purge automation" section.
```

### WebSocket / SSE Push Adapters (varco_ws)

```
WebSocketEventBus (push adapter — NOT an AbstractEventBus subclass)
  ├── __init__(bus: AbstractEventBus, *, event_type, channel)
  ├── start() / stop()  — subscribe / cancel bus subscription (idempotent)
  ├── async with WebSocketEventBus(bus) as ws_bus:  — context manager
  ├── async with ws_bus.connect(websocket) as conn:  — register/deregister client
  ├── connected_count: int
  └── _broadcast(message) — asyncio.gather to all clients concurrently; disconnects failed clients

WebSocketConnection
  ├── connection_id: str   — defaults to id(websocket)
  └── send(message: str)   — calls websocket.send_text(message)

SSEEventBus (push adapter — NOT an AbstractEventBus subclass)
  ├── __init__(bus: AbstractEventBus, *, event_type, channel, max_queue_size=100)
  ├── start() / stop()  — subscribe / cancel + send _STOP_SENTINEL to all queues (idempotent)
  ├── async with SSEEventBus(bus) as sse_bus:  — context manager
  ├── async with sse_bus.subscribe() as conn:  — create/remove SSEConnection
  ├── subscriber_count: int
  └── _handle_event(event) — sequential fan-out to all SSEConnection queues

SSEConnection
  ├── _queue: asyncio.Queue[Any]  — per-connection event buffer; maxsize = max_queue_size
  ├── _put(item)                  — put event or sentinel (blocks if queue full = backpressure)
  └── stream() → AsyncIterator[str]  — yields SSE-formatted strings until _STOP_SENTINEL

SSE wire format: "data: {json}\n\n"   (double newline = event terminator per SSE spec)

DESIGN:
  ✅ Push adapters, not bus subclasses — bus handles routing; adapters handle push layer
  ✅ WebSocket: asyncio.gather fan-out — one slow client does not block others
  ✅ SSE: per-client asyncio.Queue — independent backpressure per subscriber
  ✅ SSE stop: _STOP_SENTINEL in queue — stream() generator terminates without polling
  ❌ WebSocket: no per-client queue — slow send_text blocks the broadcast coroutine
  ❌ SSE: memory grows with (clients × queue depth) — cap with max_queue_size
  ✅ varco_ws ships a DI module — ``bootstrap()`` scans the package; ``bind_websocket_adapter()`` / ``bind_sse_adapter()`` register per-channel singletons
```

---

## Design Patterns by Module

### Event Wiring (The "Register-to" Pattern)

```python
# ❌ WRONG: Subscribe at __init__ time
class OrderConsumer(EventConsumer):
    def __init__(self, bus: AbstractEventBus):
        self._bus = bus
        self._bus.subscribe(OrderPlacedEvent, self.on_order)  # ← too early!

    async def on_order(self, event: OrderPlacedEvent): ...


# ✅ CORRECT: Metadata at class-definition time, subscribe at @PostConstruct
class OrderConsumer(EventConsumer):
    def __init__(self, bus: AbstractEventBus):
        self._bus = bus

    @PostConstruct
    def _setup(self) -> None:
        self.register_to(self._bus)  # ← called once, after DI wiring

    @listen(OrderPlacedEvent, channel="orders")
    async def on_order(self, event: OrderPlacedEvent) -> None: ...
```

**Why**: `@listen` stores metadata on the function object at class-definition time. `register_to()` reads that metadata and creates the subscription. This split makes the consumer testable (can mock the bus) and bus-agnostic (same consumer works with in-memory, Kafka, Redis).

### Service Layer Composition (MRO Chains)

```python
class UserService(
    CacheServiceMixin,
    TenantAwareService,
    ValidatorServiceMixin,
    AsyncService[User, UUID, UserCreateDTO, UserReadDTO, UserUpdateDTO],
):
    def _get_repo(self, uow: AsyncUnitOfWork) -> AsyncRepository[User, UUID]:
        return uow.users


# Method resolution order (MRO):
# UserService → CacheServiceMixin → TenantAwareService → ValidatorServiceMixin
#   → AsyncService → ...

# Calling create(data):
# 1. CacheServiceMixin.__create__  (if caching enabled)
# 2. TenantAwareService._scoped_params (inject tenant_id)
# 3. ValidatorServiceMixin._check_entity (call @validate decorators)
# 4. AsyncService.create (core logic)
```

**Rule**: Each hook must call `super()` so the next mixin in the chain runs. Order matters (left-to-right in class inheritance).

### Cache Invalidation Strategies

```python
# TTL: evict after 60 seconds
cache = await InMemoryCache(invalidation_strategy=TTLStrategy(ttl_seconds=60)).__aenter__()

# Tag-based: invalidate by tag
cache = await InMemoryCache(invalidation_strategy=TaggedStrategy()).__aenter__()
# cache.set(key, value, tags=["user:123", "order:456"])
# cache.invalidate_by_tag("user:123")  # evicts all entries with that tag

# Event-driven: invalidate on domain events
cache = await InMemoryCache(
    invalidation_strategy=EventDrivenStrategy(
        bus=event_bus,
        mappings={UserCreatedEvent: ["user:*"]},  # wildcard patterns
    )
).__aenter__()

# Composite: combine multiple strategies
cache = await InMemoryCache(
    invalidation_strategy=CompositeStrategy(
        [
            TTLStrategy(ttl_seconds=300),
            TaggedStrategy(),
            EventDrivenStrategy(bus, mappings),
        ]
    )
).__aenter__()
```

### DI Wiring

```python
# Container setup
container = DIContainer()

# Register backends: scan discovers @Singleton bus classes; cache is opt-in
container.scan("varco_kafka", recursive=True)  # discovers Kafka bus
container.install(SAModule)  # sync setup
await container.ainstall(RedisCacheConfiguration)  # async opt-in cache

# Bind repositories (auto-derived from DomainModel fields)
bind_repositories(container, User, Order, Product)

# Resolve: DI knows concrete types, app code injects protocols
user_service: AsyncService[User, UUID, ...] = container.resolve(UserService)
event_bus: AbstractEventBus = container.resolve(AbstractEventBus)
```

**Corrected DI call shapes** (documented incorrectly in several places before this
was traced to a real bug — see below):
- `install()` / `ainstall()` take **only** the module class, never a `config=`
  kwarg — `container.install(OtelConfiguration, config=OtelConfig(...))` has
  never worked.
- `provide()` takes exactly **one** argument: a module-level, `@Provider`-decorated
  factory function. `container.provide(lambda: X())` and
  `container.provide(fn, SomeInterface)` both raise
  `ProviderBindingNotDecoratedError` / `TypeError` — there is no "pass the
  interface as a second argument" call shape.
- Equal-priority bindings for the same interface resolve to the **first one
  registered**, not the last (`DIContainer._get_best_candidate` does
  `max(candidates, key=lambda b: b.priority)`, and Python's `max()` keeps the
  first element on a tie). An override `provide()` call must run *before*
  `install()`/`scan()`, or it must declare a higher `priority=` explicitly.

⚠️ **The quoted-`@Provider`-return-annotation landmine.** A `@Provider` (or
`@Configuration` provider method) whose return annotation is a *string* —
either written as `-> "Foo"` or produced by a `from __future__ import
annotations` file combined with a `TYPE_CHECKING`-only import of `Foo` — does
not fail at registration. Providify's binding constructor falls back to
`eval(fn.__annotations__["return"], fn.__globals__)`, and for a quoted
annotation that eval yields the plain **string** `'Foo'`, which gets silently
registered as the binding *interface*. The container does not reject a
str-interface binding either: `DIContainer._build_localns()` only fails later,
when it does `binding.interface.__name__` on it (`AttributeError`) — and that
`AttributeError` is caught by `_collect_kwargs_sync()`'s
`except Exception: hints = {}`, which resolves **every** `Inject[...]`
parameter for **every** provider/constructor in that container as empty from
then on. The failure surfaces nowhere near the real defect: it looks like an
unrelated provider is missing a required constructor argument
(`TypeError: some_fn() missing 1 required positional argument: 'x'`). This is
exactly what happened to `VarcoFastAPIModule.profiling_settings` (fixed by
dropping the quotes and importing `ProfilingSettings` at module scope — see
`CHANGELOG.md`'s `[Unreleased]` → `varco-fastapi` → Fixed entry).

Diagnostic — run this against any container that exhibits a baffling
"missing N required positional arguments" error to find the poisoned binding
directly, rather than guessing:

```python
print([b for b in container._bindings if isinstance(b.interface, str)])
```

Guarded against regressing via `varco_fastapi/tests/test_di_binding_health.py`
(scans every varco package's `@Provider` callables for a quoted or otherwise
unresolvable return annotation) and `varco_core/tests/test_observability_di.py`.

---

## File Organization

### varco_core Submodules

| Module | Purpose | Key Classes |
|--------|---------|------------|
| `event/` | Event bus, producer, consumer, serialization | `AbstractEventBus`, `AbstractEventProducer`, `EventConsumer`, `@listen` |
| `service/` | Domain service layer, mixins, outbox | `AsyncService`, `ValidatorServiceMixin`, `CacheServiceMixin`, `OutboxRelay` |
| `cache/` | Cache abstraction, backends, invalidation | `AsyncCache`, `CacheBackend`, `InvalidationStrategy`, `@cached` decorator |
| `cache/policy.py` | Read-through cache policy (Plan 010) | `CachePolicy` — ttl/ttl_jitter/soft_ttl/negative_ttl/stale_if_error/singleflight |
| `cache/envelope.py` | Wire format for policy-driven cache entries (Plan 010) | `CacheEnvelope`, `wrap()`, `unwrap()`, `coerce()` |
| `cache/singleflight.py` | Per-process stampede protection (Plan 010 / C2) | `Singleflight`, `SingleflightProtocol` |
| `cache/readthrough.py` | Shared read-through algorithm for C2/C3/C4 (Plan 010) | `read_through()` |
| `cache/backplane.py` | L1 coherence backplane ABC (Plan 010 / C1) | `CacheBackplane`, `InMemoryBackplane`, `InvalidationMessage` |
| `observability/cache.py` | Cache metrics pack — manual install, not a scanned `@Configuration` (Plan 010 / C3) | `install_cache_metrics()`, `CacheMetricsConfig`, `record_cache_hit/miss/eviction/duration` |
| `query/` | Query AST, parser, visitors, transformers | `QueryParams`, `FilterNode`, `ASTVisitor`, `QueryTransformer` |
| `resilience/` | Retry, timeout, circuit breaker, rate limiting, bulkhead, hedged requests | `@retry`, `@timeout`, `@circuit_breaker`, `@rate_limit`, `@bulkhead`, `@hedge` |
| `profiling/` | Diagnostic CPU/memory profiler; pluggable backends | `@profile`, `profiled()`, `ProfileSession`, `ProfileConfig`, `ProfileReport`, `CpuProfilerBackend`, `MemoryProfilerBackend` |
| `authority/` | JWT signing, verification, key rotation | `JwtAuthority`, `TrustedIssuerRegistry`, `MultiKeyAuthority` |
| `auth/` | User/role/permission abstractions | `AbstractAuthorizer`, permission models |
| `repository.py` | Repository protocol | `AsyncRepository[D, PK]` |
| `uow.py` | Unit of work protocol | `AsyncUnitOfWork`, `IUoWProvider` |
| `model.py` | Domain model base | `DomainModel` |
| `dto/` | Data transfer objects | `DTOBase`, `DTOFactory`, `Page` |
| `meta.py` | Field metadata decorators + composite key aliases | `FieldHint`, `ForeignKey`, `PrimaryKey`, `CompositeKey`, `CompositeKey2[T1,T2]`, `CompositeKey3[T1,T2,T3]` |
| `lock.py` | Distributed locking ABC + in-memory impl | `AbstractDistributedLock`, `InMemoryLock`, `LockHandle`, `LockNotAcquiredError` |
| `service/saga.py` | Saga orchestration + compensation | `SagaOrchestrator`, `SagaStep`, `SagaState`, `AbstractSagaRepository`, `InMemorySagaRepository` |
| `cache/warming.py` | Cache pre-warming strategies | `CacheWarmer`, `QueryCacheWarmer`, `SnapshotCacheWarmer`, `CompositeWarmer` |
| `query/aggregation.py` | Aggregation query AST + SA applicator | `AggregationFunc`, `AggregationExpression`, `AggregationQuery`, `SQLAlchemyAggregationApplicator` |
| `exception/` | Exception hierarchy | `RepositoryException`, `ServiceException`, `QueryException` |
| `exception/settings.py` | D-4's kill switch (Plan 011 / I1) | `ErrorEnvelopeSettings` |
| `providers.py` | DI container | `DIContainer` |
| `context/` | Ambient request-scoped values — X1 (Plan 011) | `AmbientVar[T]`, `RequestContext`, `resolve_precedence()`, `Resolved`, `TenantDefaultsProvider`, `NullTenantDefaults`, `StaticTenantDefaults` |
| `context/ambient.py` | Generic `ContextVar[T \| None]` wrapper | `AmbientVar[T]` |
| `context/precedence.py` | Shared "first non-`None` wins" helper for I2/T1 | `resolve_precedence()`, `Resolved[T]` |
| `context/request.py` | The one aggregate ambient value I2/T1 build on | `RequestContext`, `current_request_context()`, `current_locale()`, `current_timezone()`, `request_context()`, `arequest_context()` |
| `context/defaults.py` | RD-2 — per-tenant locale/tz defaults, no `varco_tenants` schema change | `TenantLocalizationDefaults`, `TenantDefaultsProvider`, `NullTenantDefaults`, `StaticTenantDefaults` |
| `i18n/` | I2 (Plan 011) — message catalog + negotiation | `MessageCatalog`, `NullMessageCatalog`, `DictMessageCatalog`, `GettextMessageCatalog`, `I18nSettings` |
| `i18n/catalog.py` | Catalog ABC + null/dict implementations | `MessageCatalog`, `NullMessageCatalog`, `DictMessageCatalog` |
| `i18n/gettext_catalog.py` | Production-default catalog — stdlib `gettext` only | `GettextMessageCatalog` |
| `i18n/negotiation.py` | Hand-rolled RFC 4647 §3.4 Lookup | `parse_accept_language()`, `negotiate_locale()` |
| `i18n/resolve.py` | I2's five-source precedence chain | `resolve_locale()` |
| `i18n/settings.py` | Off-by-default I18n settings | `I18nSettings` |
| `i18n/cache_key.py` | RD-6 — locale never an implicit cache-key component | `localization_cache_key()` |
| `tz/` | T1/T2/T3 (Plan 011) — timezone resolution + DST-safe scheduling | `validate_iana_zone()`, `TimezoneSettings`, `resolve_timezone()`, `resolve_zoned()`, `format_rfc9557()` |
| `tz/zones.py` | Shared "is this a real IANA zone" gate | `validate_iana_zone()` |
| `tz/settings.py` | Off-by-default timezone settings, startup-validated | `TimezoneSettings` |
| `tz/resolve.py` | T1's five-source precedence chain + rendering helpers | `resolve_timezone()`, `to_user_tz()`, `now_local()` |
| `tz/schedule.py` | D-8 — DST gap/overlap detection + resolution, no `dateutil` | `GapPolicy`, `OverlapPolicy`, `ScheduleGapError`, `datetime_exists()`, `datetime_ambiguous()`, `resolve_zoned()` |
| `tz/format.py` | D-9 — RFC 9557 output-only formatting | `format_rfc9557()` |
| `job/reschedule.py` | T2's opt-in recompute-on-read sweeper | `ScheduleRematerializer` |
| `query/policy.py` | T3's declared datetime coercion contract | `DatetimeCoercionPolicy` |
| `idempotency/` | D1a (Plan 029) — HTTP idempotency storage contract, framework-agnostic | `AbstractIdempotencyStore`, `ReserveOutcome`, `IdempotencyRecord`, `InMemoryIdempotencyStore`, `compute_fingerprint()`, `IdempotencySettings` |
| `webhook/` | D4 (Plan 031) — outbound webhook subscription, signing, SSRF guard, dispatcher | `WebhookSubscription`, `WebhookDelivery`, `WebhookSubscriptionRepository`, `InMemoryWebhookSubscriptionRepository`, `WebhookSigner`, `StandardWebhooksSigner`, `Rfc9421Signer`, `validate_target()`, `WebhookDispatcher`, `WebhookSettings`, `install_webhook_metrics()` |
| `webhook/inbound/` | S19 (Plan 038) — inbound webhook signature verification, replay guard | `WebhookVerifier`, `VerificationResult`, `VerificationFailure`, `StandardWebhooksVerifier`, `SvixWebhookVerifier`, `StripeWebhookVerifier`, `GitHubWebhookVerifier`, `SlackWebhookVerifier`, `get_verifier()`, `WebhookReplayGuard` |
| `flags/` | D7 (Plan 032) — feature-flag evaluation seam, varco-shaped (not OpenFeature-shaped); OpenFeature provider deferred | `AbstractFeatureFlags`, `FlagEvaluationContext`, `FlagResolution`, `InMemoryFeatureFlags`, `NullFeatureFlags`, `enable_feature_flags()` |
| `schedule/` | D6 (Plan 032) — recurring cron `Schedule` → `Job` materialization, zero new dependencies | `Schedule`, `CatchUpPolicy`, `parse_cron()`, `CronSchedule`, `ScheduleMaterializer`, `AbstractScheduleRepository`, `InMemoryScheduleRepository` |
| `retention/` | S20 (Plan 039) — a `RetentionPolicy` registry materialized onto the shipped cron→`Job` path; adapters over shipped bulk-delete verbs, no new abstract methods | `RetentionPolicy`, `RetentionRegistry`, `RetentionOutcome`, `RetentionResult`, `RetentionTarget`, `CallableRetentionTarget`, `DlqRetentionTarget`, `AuditRetentionTarget`, `IdempotencyRetentionTarget`, `RevocationRetentionTarget`, `JobRetentionTarget`, `RetentionScheduler`, `execute_policy()`, `RetentionPolicyNotFoundError`, `inspect_retention_posture()`, `bind_retention_registry()` |
| `revocation/` | S13 (Plan 034) — invalidate a JWT before its `exp`, per token/subject/tenant/issuer | `AbstractTokenRevocationStore`, `RevocationScope`, `RevocationEntry`, `RevocationVerdict`, `RevocationFailureMode`, `NullTokenRevocationStore`, `InMemoryTokenRevocationStore`, `enable_token_revocation()`, `inspect_revocation_posture()` |
| `auth/api_key.py` | S14 (Plan 034) — stdlib-only (`hashlib`/`hmac`) offline API-key hashing behind `ApiKeyAuth`'s `hashed_keys=` path | `hash_api_key()`, `verify_api_key()` |

---

## Common Workflows

### Adding a New Service with Full Stack

1. **Define domain model** (`varco_core.model.DomainModel`)
   - Annotate fields with types, use `@FieldHint`, `@PrimaryKey`, `@ForeignKey` for metadata

2. **Define DTOs** (`varco_core.dto.DTOBase`)
   - `CreateDTO`, `ReadDTO`, `UpdateDTO` subclasses

3. **Implement service** (`varco_core.service.AsyncService`)
   - Implement `_get_repo()` method
   - Mix in `CacheServiceMixin`, `TenantAwareService`, `ValidatorServiceMixin` as needed

4. **Implement event consumer** (`varco_core.event.EventConsumer`)
   - Decorate handlers with `@listen(EventType, channel="name")`
   - Call `register_to(bus)` in `@PostConstruct` method

5. **Bind in DI** (in your app's DI setup)
   - `bind_repositories(container, DomainModel)`
   - `container.resolve(YourService)`

### Publishing Events Safely

```python
# ❌ WRONG: publishes after DB commit, broker failure silently drops event
async with uow:
    user = await repo.save(User(...))
# ← DB committed here
await producer.produce(UserCreatedEvent(user.id))  # ← can fail!

# ✅ CORRECT: persists event in same DB transaction
async with uow:
    user = await repo.save(User(...))
    await outbox_repo.save_outbox(OutboxEntry.from_event(UserCreatedEvent(user.id)))
# ← DB committed with both user and outbox entry
# OutboxRelay polls and publishes asynchronously
```

### Using the Query System

```python
from varco_core.query import QueryParams, QueryTransformer

# Client sends filter/sort as strings (e.g., HTTP query params)
params = QueryParams(
    filters=["age__gte=18", "status__eq=active"],
    sort=["+created_at"],
    limit=20,
    offset=0,
)

# Transform into backend query
transformer = QueryTransformer()
filtered_query = transformer.transform(base_query, params, User)
# ← base_query is now: base_query.where(...).order_by(...).limit(...).offset(...)
```

---

## Integration Points & Backend-Specific Implementations

### varco_sa (SQLAlchemy)

- **ORM generation**: `SAModelFactory` reads `DomainModel.fields` and creates SQLAlchemy models at import
- **Repository impl**: Standard async SQLAlchemy queries
- **Outbox impl**: `SAOutboxRepository`, `SARelayOutboxRepository` (SQL table-based)
- **Inbox impl**: `SAInboxRepository`, `SAPollerInboxRepository` — `varco_inbox` table; `InboxEntryModel`
- **Job store**: `SAJobStore` — `varco_jobs` table; `try_claim()` uses `SELECT FOR UPDATE SKIP LOCKED` on PostgreSQL, plain SELECT+UPDATE on other dialects
- **Saga impl**: `SASagaRepository` — `varco_sagas` table; DELETE+INSERT dialect-agnostic upsert
- **Conversation impl**: `SAConversationStore` — `varco_conversation_turns` table, turn-per-row
- **Advisory lock**: `SAAdvisoryLock` — `pg_try_advisory_lock` / `pg_advisory_unlock`; one pinned connection per held lock
  (session-scoped — see `technical_docs/features/distributed-locks.md` for the transaction-pooler
  hazard); `SAXactAdvisoryLock` — transaction-scoped sibling, released at COMMIT/ROLLBACK
- **Row-Level Security helpers**: `varco_sa.rls` (Plan 005 Phase 8 / U-5, helpers only, nothing
  wired) — `render_rls_ddl(table, ...)` returns DDL for the application's own Alembic revision,
  always emitting the `(SELECT current_setting(..., true))` InitPlan form (see the 150× cliff
  in `technical_docs/features/postgres-rls.md`); `set_tenant_local(session, tenant_id)` sets the
  RLS GUC via transaction-scoped `set_config(..., true)` — PgBouncer-transaction-mode safe, same
  defect class as the session-scoped advisory lock above
- **Query applicator**: `SQLAlchemyFilterVisitor` converts AST → WHERE clause
- **Pool metrics**: `pool_metrics(engine)` returns `SAPoolMetrics` snapshot; `SAFastrestApp.pool_metrics()` for convenience
- **Health check**: `SAHealthCheck` — `SELECT 1` probe against the engine
- **DI**: `SAModule` with engine, declarative base, entity classes
- **Encryption key store**: `SAEncryptionKeyStore` — stores encryption keys in a dedicated
  `varco_encryption_keys` table using SQLAlchemy Core (no `SAModelFactory` dependency).
  Call `await store.ensure_table()` at startup or add a manual Alembic migration.
  Table schema: `kid` (PK), `algorithm`, `key_material` (base64url), `created_at`,
  `tenant_id` (NULL = global), `is_primary`, `wrapped`, `scope` (indexed, Plan 005
  Phase 1 — `EncryptionKeyEntry.scope` defaults to `tenant_id` at the Python level, but
  `load_for_scope`/`destroy_scope` filter on the persisted `scope` column itself, so a
  one-time `scope = tenant_id` backfill is required before they see pre-existing rows),
  `destroyed_at` (nullable — crypto-shred tombstone). See
  `technical_docs/features/crypto-shredding.md`.

  ```python
  from varco_sa.encryption_store import SAEncryptionKeyStore
  from varco_core.authority import EncryptionKeyManager

  engine = create_async_engine("postgresql+asyncpg://...")
  store = SAEncryptionKeyStore(engine)
  await store.ensure_table()  # idempotent — uses CREATE TABLE IF NOT EXISTS
  manager = EncryptionKeyManager(store, master_encryptor=kek)
  registry = await manager.build_tenant_registry()
  ```

### varco_beanie (MongoDB / Beanie)

- **ODM generation**: `BeanieModelFactory` creates Beanie Document classes from `DomainModel` subclasses
- **Repository impl**: Async Beanie Document queries
- **Outbox impl**: `BeanieOutboxRepository` — `OutboxDocument` Beanie model
- **Inbox impl**: `BeanieInboxRepository` — `InboxDocument`; unique compound index for dedup
- **Job store impl**: `BeanieJobStore` — `JobDocument`; `varco_jobs` collection; `try_claim()` uses MongoDB `findAndModify` (atomic PENDING → RUNNING)
- **Saga impl**: `BeanieSagaRepository` — `SagaDocument`; `varco_sagas` collection
- **Aggregation**: `BeanieAggregationApplicator` — builds MongoDB aggregation pipeline (`$match`, `$sort`, `$skip`, `$limit`)
- **Index guard**: `BeanieIndexGuard` — detects drift between defined and actual MongoDB indexes
- **Health check**: `BeanieHealthCheck` — `server_info()` probe against the MongoClient
- **DI**: `BeanieConfiguration`

### varco_memcached (Memcached)

- **Cache impl**: `MemcachedCache(CacheBackend)` — `aiomcache` async client; TTL via Memcached-native `exptime`
- **Settings**: `MemcachedCacheSettings` — host, port, pool_size, key_prefix; reads `VARCO_MEMCACHED_CACHE_*` env vars
- **Key handling**: Keys encoded as bytes (`aiomcache` requirement); prefix applied via `memcached_key()`
- **clear()**: Registry-based (in-process `set[str]`) — no native SCAN equivalent in Memcached
- **Health check**: `MemcachedHealthCheck` — `stats()` probe (Memcached has no PING command)
- **DI**: `MemcachedCacheConfiguration`

### varco_kafka (Kafka)

- **Bus impl**: `KafkaEventBus` — uses `aiokafka.AIOKafkaProducer` / `AIOKafkaConsumer`
- **Channel routing**: Topic names from `@listen(event_type, channel="orders")` → Kafka topic
- **DLQ impl**: Dedicated Kafka topic for dead letters
- **Config**: `KafkaConfig` with broker addresses, consumer group, etc.

### varco_redis (Redis)

- **Bus impl**: `RedisEventBus` — uses Redis Pub/Sub or Streams
- **Cache impl**: `RedisCache` — async redis.asyncio, lazy connection pooling
- **Lock impl**: `RedisLock` — SET NX PX for acquisition; Lua script for token-guarded release
- **Conversation impl**: `RedisConversationStore` — Redis List per task_id; RPUSH/LRANGE; optional TTL
- **Rate limiter**: `RedisRateLimiter` — distributed sliding window via sorted set + Lua (multi-pod)
- **Bulkhead**: `RedisBulkhead` — distributed concurrency limiter (Plan 005 Phase 8 / U-7's
  second leg), sorted set of holders + Lua acquire/release, TTL-based crashed-holder reclaim;
  opt-in via `RedisBulkheadConfiguration` (`@Configuration`, not auto-scanned)
- **Channel routing**: Redis pubsub channels or streams
- **DLQ impl**: Dedicated Redis stream for dead letters
- **Invalidation**: `EventDrivenStrategy` can subscribe to events and invalidate cache keys
- **Health check**: `RedisHealthCheck` — PING probe (throw-away connection per check)
- **Config**: `RedisConfig` with host/port; `CacheConfig` with TTL, strategy

### varco_ws (WebSocket / SSE)

- **WebSocket adapter**: `WebSocketEventBus` wraps any `AbstractEventBus`; calls `websocket.send_text(str)` — compatible with FastAPI, Starlette, aiohttp
- **SSE adapter**: `SSEEventBus` delivers events as `data: {...}\n\n` strings; integrate with `StreamingResponse` in any ASGI framework
- **DI module** (`varco_ws.di`): `bootstrap()` scans the package, discovering both adapters as `@Singleton`s; `bind_websocket_adapter()` / `bind_sse_adapter()` register per-channel singletons
- **No broker dependency**: both adapters subscribe to an existing bus instance; the push layer is fully decoupled from transport

  ```python
  # DI wiring pattern for varco_ws (preferred)
  from varco_ws.di import bootstrap, bind_websocket_adapter, bind_sse_adapter
  from myapp.events import OrderEvent

  bootstrap(container)  # scans varco_ws
  bind_websocket_adapter(container, event_type=OrderEvent, channel="orders")
  bind_sse_adapter(container, event_type=OrderEvent, channel="orders")

  # Start/stop in the FastAPI lifespan handler (create_varco_app does this automatically)
  orders_ws = container.get(WebSocketEventBus)
  orders_sse = container.get(SSEEventBus)
  ```

  > **Note**: `WebSocketEventBus` and `SSEEventBus` are **push adapters**, not
  > `AbstractEventBus` implementations. Do not pass them where a bus is expected.
  > They subscribe to an existing bus and forward serialised events to connected clients.

### varco_fastapi (FastAPI adapter)

- **Router mixins**: `CreateMixin`, `ReadMixin`, `UpdateMixin`, `DeleteMixin`, `ListMixin`,
  `StreamMixin` — compose standard HTTP endpoints without boilerplate.
- **Service-free routers**: `GenericRouter` (alias for `VarcoRouter` with no type args) is the
  entry point for data-processing, proxy, or computed-endpoint servers with no `AsyncService`
  or repository.  All cross-cutting features (middleware, telemetry, auth) apply unchanged.
- **`VarcoCRUDRouter[D, PK, C, R, U, S]`** (`varco_fastapi.router.crud`) — service-backed CRUD
  router with task-based async recovery; `CRUDRouter`/`ReadOnlyRouter`/`WriteRouter`/
  `NoDeleteRouter` (`varco_fastapi.router.presets`) are pre-composed subclasses threading the
  same 6 type params. `S` is an optional, PEP-696-defaulted 6th type parameter (the concrete
  `AsyncService` subclass, via `typing_extensions.TypeVar`) that types `self._service` as
  `S | None` and the `self.service` property as non-Optional `S` — subscript it
  (`CRUDRouter[Order, UUID, OrderCreate, OrderRead, OrderUpdate, OrderService]`) to expose
  custom service methods (e.g. `self.service.cancel_order(...)`) with zero per-subclass
  boilerplate. 5-arg subscription still works — `S` defaults to `AsyncService[Any, ...]`.
- **Route-level authorization**: `RouteGuard` (`varco_fastapi.auth.guard`) is a declarative,
  immutable predicate attached to any `@route` via `requires=`.  Constructor helpers:
  `require_scopes`, `require_roles`, `require_grant`, `require_token_profile`,
  `require_predicate`, `allow_anonymous`. `require_token_profile(*names)` checks
  `ctx.metadata["token_profile"]` (populated by `varco_core.jwt.profile.resolve_token_profile()` —
  see `technical_docs/features/token-profiles.md`) between the role check and the
  grant check.  Evaluated against `AuthContext` before the handler runs; denial → HTTP 403.
- **Auth middleware**: `AuthMiddleware` validates JWT bearer tokens using `TrustedIssuerRegistry`.
- **`ApiKeyAuth`** (`varco_fastapi.auth.server_auth`) hashes plaintext `keys=` at construction via
  `varco_core.auth.api_key.hash_api_key()` and accepts pre-hashed `hashed_keys=` directly (Plan
  034 / S14); its `?api_key=` query-parameter fallback (and `WebSocketAuth`'s `?token=` fallback)
  are off by default — name `param=`/`token_query_param=` explicitly to re-enable either.
- **`inspect_auth_posture()`** (`varco_fastapi.auth.posture`, Plan 034 / Phase 4) is a pure,
  read-only introspection function walking an `AbstractServerAuth` tree (including nested
  `CompositeServerAuth`/`WebSocketAuth` wrappers) to report facts — API-key query-fallback state,
  plaintext-vs-hashed key source, `PassthroughAuth` presence. Reports facts only; Plan 036 owns
  any judgement built on top.
- **Lifecycle auto-discovery**: `create_varco_app` calls `_collect_lifecycle_components()` which
  discovers `AbstractEventBus`, `AbstractDistributedLock`, `CacheBackend`, and — if `varco_ws`
  is installed and registered — `WebSocketEventBus` and `SSEEventBus` from the DI container.
  All discovered components are started/stopped as part of the app lifespan.
- **Typed HTTP clients**: `AsyncVarcoClient` / `SyncVarcoClient` with retry, circuit breaker, and JWT injection.
- **DI wiring**: `VarcoFastAPIModule` + `bind_clients()`. ⚠️ `bind_clients()` is
  currently known-broken (raises `ClassBindingNotDecoratedError` — the internal
  factory it builds is never `@Provider`-decorated); see the `bind_clients()`
  docstring in `varco_fastapi/varco_fastapi/di.py` for the full failure chain.

#### HTTP Metrics — `MetricsMiddleware` + `MetricsRouter`

Varco FastAPI ships two complementary observability components:

**`MetricsMiddleware`** (`varco_fastapi.middleware.metrics`) — ASGI middleware that records
three OTel instruments following the [HTTP semantic conventions](https://opentelemetry.io/docs/specs/semconv/http/http-metrics/):

| Instrument | Type | Unit | Attributes |
|---|---|---|---|
| `http.server.request.duration` | Histogram | `s` | `http.request.method`, `http.route`, `http.response.status_code` |
| `http.server.active_requests` | UpDownCounter | `{request}` | `http.request.method` only |
| `http.server.request.body.size` | Histogram | `By` | `http.request.method`, `http.route` |

From `http.server.request.duration` alone, Grafana/Prometheus can derive RPS, latency
percentiles (p50/p95/p99), and error rates — no separate counter needed.

**Cardinality guard**: `http.route` uses the route *template* (`/orders/{order_id}`), not
the concrete URL (`/orders/123`).  This is extracted from `request.scope["route"].path`
in the `finally` block after `call_next()` completes (routing must finish first).
Unmatched paths (404s) use `http.route="unknown"` to group all scanner traffic.

**`MetricsRouter`** (`varco_fastapi.router.metrics`) — standalone router that serves
`GET /metrics` in Prometheus text or OpenMetrics format:
- Default: Prometheus text v0.0.4 via `prometheus_client.generate_latest()`
- `Accept: application/openmetrics-text` → OpenMetrics format with exemplar support
  (Prometheus ≥ 2.26 sends this header automatically)
- `prometheus_client` not installed → 503 with install instructions (graceful degradation)

**Middleware execution order** (outermost → innermost):
```
CORS → ErrorMiddleware → TracingMiddleware → MetricsMiddleware → RequestLoggingMiddleware → RequestContextMiddleware → route
```
`MetricsMiddleware` sits inside `TracingMiddleware` so trace context is active when metrics
are recorded, and outside `RequestLoggingMiddleware` so skipped paths (`/metrics`, `/health`)
don't generate access log noise.

**Skip paths**: `MetricsMiddleware` skips `/metrics` and `/health` by default to exclude
Prometheus scrape traffic and Kubernetes health probe noise from latency histograms.

**Wiring via `create_varco_app`**:

```python
# Auto-mounts MetricsMiddleware + MetricsRouter at /metrics
app = create_varco_app(container, enable_metrics=True, validate=False)

# Custom skip paths:
app.add_middleware(MetricsMiddleware, skip_paths=frozenset({"/metrics", "/health", "/readyz"}))
```

**End-to-end Prometheus pull flow**:
```
OtelConfig(service_name="myapp", prometheus_enabled=True)
  → OtelConfiguration.meter_provider()
  → PrometheusMetricReader() registered with prometheus_client.REGISTRY
  → MeterProvider(metric_readers=[prometheus_reader])

create_varco_app(enable_metrics=True)
  → add_middleware(MetricsMiddleware)    # records OTel instruments per request
  → include_router(MetricsRouter)       # serves GET /metrics

GET /orders/123
  → MetricsMiddleware: active_requests.add(+1, {method="GET"})
  → route handler returns 200
  → finally: route="/orders/{order_id}", duration=0.042s, status="200"
    duration.record(0.042, {method, route, status})
    active_requests.add(-1, {method="GET"})

GET /metrics  ← Prometheus scraper
  → MetricsMiddleware: skip (starts with /metrics)
  → MetricsRouter._handle(): generate_latest() from REGISTRY
  → Returns text/plain Prometheus exposition format
```

**OTLP push + Prometheus pull simultaneously**: both readers can be active:

```python
OtelConfig(
    service_name="myapp",
    otlp_endpoint="http://otel-collector:4317",  # push to Grafana Cloud / Datadog
    prometheus_enabled=True,  # pull from /metrics
)
```

**Optional extra**: `pip install varco-fastapi[prometheus]` adds `opentelemetry-exporter-prometheus`
which provides `PrometheusMetricReader` and `prometheus_client`. The `MetricsRouter` endpoint
returns 503 when `prometheus_client` is absent; the `OtelConfiguration` logs an ERROR when
`prometheus_enabled=True` but the exporter is not installed.

**Lazy instrument creation**: instruments are created on the first request (not at import or
`__init__` time) using a module-level `_instruments: dict[str, Any]` cache. This ensures they
are bound to the live `MeterProvider` set by `OtelConfiguration`, not the no-op provider
active at import time.

#### Diagnostic Profiler — `ProfilingMiddleware` + `varco_core.profiling`

The profiler fills the gap left by aggregate metrics: while `MetricsMiddleware` answers
*"how slow on average"*, the profiler answers *"which function is hot"* and *"what allocated
this memory"* via deep per-call introspection.

**`varco_core.profiling`** — backend-agnostic profiling primitives:

| Symbol | Role |
|---|---|
| `@profile()` / `profiled()` | Decorator and context-manager primitives |
| `ProfileSession` | Dual sync/async context manager; drives backends through start→collect |
| `ProfileConfig` | Frozen config: backends, top_n, sort_by, otel, track_rss |
| `ProfileReport` | Immutable result: wall/cpu time, top functions, mem delta, RSS, artifacts |
| `CpuProfilerBackend` | Protocol — implement to add pyinstrument, py-spy, etc. |
| `MemoryProfilerBackend` | Protocol — implement to add memray, etc. |
| `register_cpu_backend()` / `register_memory_backend()` | Name registry for backend factories |

**Built-in backends**: `"cprofile"` (deterministic CPU via `cProfile`) and
`"tracemalloc"` (memory snapshots + diff via `tracemalloc`). Both are registered at import time.

**Off by default** — zero overhead when disabled:

```python
from varco_core.profiling import profile, profiled, set_profiling_enabled

set_profiling_enabled(True)  # or set VARCO_PROFILING_ENABLED=true


@profile()
async def slow_query() -> list[Row]: ...


async with profiled("batch_job") as s:
    await process()
print(s.report.format())  # human-readable table
```

**Adding a new backend** (e.g. pyinstrument):

```python
from varco_core.profiling import CpuProfilerBackend, register_cpu_backend


class PyinstrumentBackend:
    name = "pyinstrument"

    def start(self) -> None: ...
    def collect(self, top_n, sort_by) -> CpuProfileResult: ...


register_cpu_backend("pyinstrument", PyinstrumentBackend)
# Then: ProfileConfig(cpu_backend="pyinstrument")
```

**`ProfilingMiddleware`** (`varco_fastapi.middleware.profiling`) — ASGI middleware that
profiles one HTTP request at a time:
- Config via `ProfilingSettings` (env prefix `VARCO_PROFILER_`): `enabled`, `slow_threshold_ms`,
  `mem_threshold_mb`, `attach_headers`, `skip_paths`, `top_n`, `track_rss`.
- Uses a process-wide `asyncio.Lock` (lazy); concurrent requests **pass through unprofiled**
  rather than blocking.
- Threshold gating: only logs when `wall_ms >= slow_threshold_ms`.
- `attach_headers=True` adds `X-Profile-Wall-Ms` / `X-Profile-Mem-Kb` response headers.

**Wiring**:
```python
app = create_varco_app(container, enable_profiling=True)
# or:
app.add_middleware(
    ProfilingMiddleware, settings=ProfilingSettings(enabled=True, attach_headers=True)
)
```

**Middleware stack placement** (innermost — closest to route handler):
```
CORS → Error → Tracing → Metrics → Logging → RequestContext → Profiling → route
```

#### SkillAdapter — Google A2A protocol (v1.0.0 + SkillSource)

`SkillAdapter` exposes an agent over the Google A2A protocol. Plan 005 Phase 7 (gaps U-3 +
U-4, "one piece of work upstream") did two things together: moved the protocol surface to
**A2A v1.0.0** and decoupled the adapter's *subject* from `VarcoRouter` introspection via a
new `SkillSource` seam. Both the v1.0.0 and pre-v1.0.0 surfaces are mounted for one minor
release.

```
SkillAdapter(router_cls | source=, ...)     ← exactly one of the two, ValueError otherwise
  router_cls   → wrapped into RouterSkillSource (introspect_routes(), extracted verbatim)
  source=      → any SkillSource — no VarcoRouter required
       ↓ both implement
  SkillSource (Protocol, varco_fastapi.router.a2a.source):
      skills() -> list[SkillDefinition]
      agent_metadata() -> AgentMetadata
      async invoke(skill_id, payload, *, ctx: AuthContext | None = None) -> Any
```

Modules (`varco_fastapi/varco_fastapi/router/a2a/`):
- `source.py` — `SkillDefinition`, `AgentMetadata`, the `SkillSource` Protocol.
- `router_source.py` — `RouterSkillSource`, today's route-introspection behaviour, **extracted
  verbatim** — no behaviour change, `tests/milestone_f/test_skill_adapter.py` stays green
  unmodified against it.
- `card.py` — the v1.0.0 Agent Card builder: capability flags nested under a `capabilities`
  object, **no top-level `id`** (the legacy card had flags at the top level and included `id`).
- `jsonrpc.py` — JSON-RPC 2.0 envelope + dispatch for `message/send`, `message/stream`,
  `tasks/get`, `tasks/list`, `tasks/cancel`, `tasks/resubscribe`. Maps onto the existing async
  machinery (`job_runner`/`job_store`, `router/skill.py:264-266`) and the v1 task states
  `submitted`/`working`/`completed`/`failed`/`canceled`. A `SkillSource.invoke()` that raises
  is mapped to a JSON-RPC error envelope (HTTP 200), never a bare 500.

A2A surfaces mounted by `adapter.mount(app, legacy_paths=True)`:

**v1.0.0 — always mounted:**
- `GET  /.well-known/agent-card.json` — Agent Card (nested `capabilities`, no top-level `id`)
- `POST /a2a` — JSON-RPC 2.0 dispatch

**Pre-v1.0.0 (legacy) — mounted only while `legacy_paths=True`, the default for one minor
release, with one deprecation warning logged per mount:**
- `GET  /.well-known/agent.json` — legacy Agent Card
- `POST /tasks/send` — execute a skill; **synchronous by default**, or **asynchronous** when
  `job_runner` + `job_store` are supplied to `SkillAdapter.__init__` — the response returns
  immediately with `state: working` and the client polls task status.
- `GET  /tasks/{task_id}` — poll task status. With `job_store` wired this reflects the real,
  persisted job state (`working`/`completed`/`failed`), not a stub response.
- `GET  /tasks/{task_id}/history` — full turn history, returned when a `conversation_store`
  is supplied (multi-turn mode); otherwise task history is not tracked (single-turn mode).

⚠️ **Async A2A already worked before Phase 7** (Source correction 2, `plans/005-upstream-gaps.md`)
— `job_runner`/`job_store`/`conversation_store` support predates the v1.0.0 surface. Phase 7
moved the *protocol shape*, not the async machinery.

```python
from varco_fastapi.router.skill import SkillAdapter, bind_skill_adapter

# router_cls path (unchanged, positional)
adapter = SkillAdapter(
    OrderRouter,
    agent_name="OrderAgent",
    agent_description="Manages customer orders",
    client=OrderClient(base_url="http://localhost:8080"),
)
adapter.mount(app)  # v1.0.0 (/.well-known/agent-card.json, /a2a) + legacy paths

# SkillSource path — no VarcoRouter required (U-3)
from varco_fastapi.router.a2a.source import AgentMetadata, SkillDefinition


class ReportSkillSource:
    def skills(self) -> list[SkillDefinition]: ...
    def agent_metadata(self) -> AgentMetadata: ...
    async def invoke(self, skill_id, payload, *, ctx=None): ...


adapter = SkillAdapter(
    None,
    source=ReportSkillSource(),
    agent_name="ReportAgent",
    agent_description="Generates PDF reports",
)
adapter.mount(app, legacy_paths=False)  # v1.0.0 surface only

# DI-friendly usage
bind_skill_adapter(
    container,
    OrderRouter,
    agent_name="OrderAgent",
    agent_description="Manages orders",
    client_cls=OrderClient,
)
# Inject[SkillAdapter] now resolves to the adapter
```

**The `ctx` auth-passthrough contract (U-3)**: `SkillSource.invoke(skill_id, payload, *, ctx=)`
receives the verified caller's `AuthContext` (resolved from
`varco_fastapi.context.get_auth_context_or_none()` at JSON-RPC dispatch), or `None` when no
auth middleware populated one — so end user / another agent / integrating platform are
distinguishable in a `SkillSource`'s own audit trail. `skills=` on `SkillAdapter.__init__`
accepts author-supplied `SkillDefinition` objects **verbatim**, appended to whatever the
source returns — hand-written skill text reaches the Agent Card unaltered, not regenerated
from route names.

**Design**: tasks are synchronous by default — all CRUD operations complete in the
`/tasks/send`/`message/send` response. Long-running operations (ML inference, file
processing) support async task storage: pass `job_runner` + `job_store` to `SkillAdapter`
and the response returns `state: working` while the client polls; pass `conversation_store`
for `/tasks/{task_id}/history`.

**Optional extra**: `pip install varco-fastapi[a2a]` for the Google A2A SDK types.
`SkillAdapter` itself works without it — the extra only adds A2A client utilities.

See `technical_docs/features/a2a-surface.md` for the full v1.0.0 path/method table, a
non-router `SkillSource` example, and the legacy-path deprecation timeline.

#### MCPAdapter — Model Context Protocol

`MCPAdapter` converts any `VarcoRouter` class into an MCP (Model Context Protocol) server.
Routes flagged with `mcp_enabled=True` are exposed as MCP tools. Execution is delegated
to `AsyncVarcoClient`.

**`MCPAdapter` exposes varco routes *as* an MCP server — it is not an MCP client.**
There is no varco component that calls out to a third-party MCP server; if a downstream
app needs to *consume* other MCP servers, that is a separate concern outside this adapter.

```python
from varco_fastapi.router.mcp import MCPAdapter, bind_mcp_adapter

# Option A: mount as HTTP+SSE endpoint on an existing FastAPI app
adapter = MCPAdapter(OrderRouter, client=OrderClient(base_url="http://localhost:8080"))
adapter.mount(app)  # registers GET {path}/sse + POST {path}/messages/

# Option B: run as standalone stdio MCP server (for local LLMs)
server = adapter.to_mcp_server()
# server is a low-level mcp.server.Server (v2 import path) — run it over a
# transport, e.g. mcp.server.stdio.stdio_server() + server.run(read, write, options)

# DI-friendly usage
bind_mcp_adapter(container, OrderRouter, client_cls=OrderClient)
# Inject[MCPAdapter] now resolves to the adapter
```

**Input schema generation**: `MCPAdapter` automatically builds a JSON Schema for each tool
from path parameters, request body model (`model_json_schema()`), and pagination/filter
params for list routes.

⚠️ **BREAKING (Plan 020 / KI-11)**: `to_mcp_server()` returns a low-level `Server`
(imported from `mcp.server.lowlevel` under v1; from `mcp.server` since the Plan 029 / N1 bump to
`mcp>=2,<3`), not a `FastMCP` instance — the high-level `FastMCP.add_tool()`
has never accepted an `input_schema=` parameter (SDK issue #761), so `to_mcp_server()`/`mount()`
were both dead code (`TypeError` on every call) before this fix. `_to_mcp_tools()`
(`varco_fastapi.router.mcp`) builds `mcp.types.Tool` objects carrying varco's JSON Schema
verbatim. Anyone calling `to_mcp_server()` directly and relying on `FastMCP`-specific methods
(`.run()`'s no-arg stdio shortcut, `.add_tool()`, `.sse_app()`) must update to the low-level
`Server` API. The v1→v2 portability this decision predicted held: only the *registration* lines
changed, and `_to_mcp_tools()` was untouched.

**Optional extra**: `pip install varco-fastapi[mcp]` (`mcp>=2,<3` as of 3.1 — Plan 029 / N1
migrated off the v1 low-level `Server` decorator API, which v2 removed). `to_mcp_server()` now
builds `mcp.server.Server(name=..., on_list_tools=..., on_call_tool=...)`: handlers are passed as
constructor arguments, each takes a `ctx` first argument, and `on_call_tool` receives a single
`CallToolRequestParams` rather than `(name, arguments)` positionals. `on_list_tools` returns a
`ListToolsResult` carrying the v2-required `ttl_ms` (from `MCPAdapter(ttl_ms=60_000)`) and
`cache_scope="private"` — verified by experiment against mcp 2.1.1, not inferred; see
`design/research/003-mcp-python-sdk-v2-migration.md`'s evidence addendum. `_to_mcp_tools()`'s
`Tool(name=, description=, input_schema=)` shape is unchanged across the major, which is the
payoff of the original low-level-`Server` choice. `mount()` is structurally unchanged —
`mcp.server.sse.SseServerTransport` + `connect_sse` still exist in v2, though HTTP+SSE is
deprecated in favour of Streamable HTTP (a filed BACKLOG follow-up, not this migration's scope).
The adapter is constructible without the extra — `to_mcp_server()` and `mount()` raise
`ImportError` with a clear install message if the SDK is absent.
⚠️ **BREAKING for that extra only**: an app pinned to mcp 1.x cannot take varco 3.1's
`varco-fastapi[mcp]`. Nothing changes for anyone not installing it.

**`IdempotencyMiddleware`** (`varco_fastapi.middleware.idempotency` — Plan 029 / D1b): replays
the stored response for a repeated `Idempotency-Key` on `POST`/`PATCH` instead of executing
twice. Opt-in — `create_varco_app()` never adds it; register it through
`install_middleware_stack` or `app.add_middleware(IdempotencyMiddleware, store=...)`. It holds an
`AbstractIdempotencyStore` (above) and is the only piece that knows about headers, status codes,
and streaming detection.

⚠️ **Its stack position is a correctness requirement, not a preference** — it must sit *inside*
`ErrorMiddleware` (so its 409/422/400 render through the RFC 9457 problem+json path rather than
escaping as a 500) and *inside* `RequestContextMiddleware` (so `current_tenant()` and the ambient
`AuthContext` are populated before the storage key is built). An explicit ordering test guards
this. Note Starlette's `add_middleware()` **prepends** — the last call ends up outermost — so an
outermost-first reading of the call sequence is backwards.

Storage key is `idempotency:{tenant}:{subject}:{key}` and **fails closed**: with tenancy enabled
and no ambient tenant it raises rather than falling back to a global key, since a cross-tenant
collision would replay one tenant's response to another. Streaming responses and bodies over
`max_stored_body_bytes` (1 MiB default) pass through and *release* the reservation, so a retry
re-executes. Full design + Pitfalls: `technical_docs/features/idempotency-key.md`.

**Localization / timezone middleware** (`varco_fastapi.middleware.localization`,
`varco_fastapi.i18n` — Plan 011): `LocalizationMiddleware` resolves locale (I2) and/or
timezone (T1) in one ASGI pass, gated by two independent settings
(`I18nSettings.enabled`/`TimezoneSettings.enabled`); with both off it is not added to the
stack. `create_varco_app(i18n=, timezone=)` — both typed `Any | None`, resolved via
`isinstance()` checks against `I18nSettings`/`TimezoneSettings`, **not** type-checked
keyword parameters; pass anything else and it silently falls back to default settings.
`I18nLifecycle` (`varco_fastapi.i18n`) starts/stops the DI-resolved `MessageCatalog` around
`VarcoLifespan`, only when i18n is enabled and a non-`None` catalog is found.
`LocalizationMiddleware` is the innermost built-in layer (added earliest, dispatches last),
so any app-supplied `TenantResolutionMiddleware` via `extra_middleware=` always dispatches
before it — see `technical_docs/features/timezone-handling.md`'s "Wiring" section for the
full verified request-order diagram and the `request.state` mirror's current (unread)
status on the error path.

---

## Anti-Patterns to Avoid

| Anti-Pattern | Why It's Wrong | Fix |
|---|---|---|
| Service calls `AbstractEventBus` directly | Only producer/consumer/outbox should touch the bus | Inject `AbstractEventProducer` |
| Publishes events after DB commit | Broker failure silently loses events | Use `OutboxRepository` + `OutboxRelay` |
| Instantiates `InvalidationStrategy` outside cache lifecycle | May hold subscriptions/background tasks | Let `CacheBackend` manage it via `start()`/`stop()` |
| Per-call `CircuitBreaker` instances | Never accumulates failures, so circuit never opens | Use shared instance per external dependency |
| Saga step not idempotent | Compensation re-runs a step that already partially ran — double side-effects | Design every step to be idempotent; check state before side-effecting |
| Saga without persistent repository | Crash mid-saga leaves system in half-applied state with no recovery path | Use `AbstractSagaRepository` to persist state after every step |
| `WebSocketEventBus` / `SSEEventBus` used as `AbstractEventBus` | They are push adapters, not bus implementations — cannot publish or route | Pass them a real bus; use the bus for service-to-service; use adapters only for browser push |
| Cache backend `add_warmer()` called after `__aenter__` | Warmers only run during `__aenter__` — adding one after start is a no-op | Register all warmers before `async with cache:` |
| Subscribes to events in `__init__` | Blocks service instantiation, makes testing hard | Defer to `@PostConstruct` + `register_to()` |
| Mixin hook doesn't call `super()` | Breaks the MRO chain, later mixins never run | Always chain with `return await super()._hook_name(...)` |

