# Plan 050 — Phased lifecycle with a reverse-order guarantee, `@Disposes` reachability, and DI-wired infrastructure (DI-9, DI-10)

Covers BACKLOG 4.0 rows **`DI-9`** (`BACKLOG.md:96` — *"Phased startup/shutdown with a
reverse-order guarantee — `VarcoLifespan` runs modules in declared phases (bus before consumers;
consumers stop before bus; stores before relays), shutdown is the exact reverse; `@Disposes`
reachable for every provider-produced singleton (closes `P24-DISPOSES-FIRSTMATCH` once upstream
ships)"*) and **`DI-10`** (`BACKLOG.md:97` — *"DI-wire the hand-constructed infrastructure —
`SADeadLetterQueue`, `OutboxRelay`, `WebhookDispatcher`, `ReloadingTrustStore`, `RetentionRegistry`,
and the four adapters get providers so `serializer=`/`encryptor=` are injected, not remembered"*).

Slice row 050 of `plans/000-index-4-0-di-standardization.md`. **Fixed inputs from landed plans,
referenced by number and never re-decided**: 045 (`@Fallback` defaults; the P24 `@Disposes` scoping
fix adopted — `@Disposes` wiring is scoped to the bindings *its own* `install()` registered), 046
(`bootstrap(container) -> DIContainer`; `bind_instance`; the `di_wiring` conformance suite, whose
`test_every_provider_singleton_has_a_reachable_disposer` carries `known_warnings` entries naming 050
— **that list is this plan's work list**), 047 (`@Requires`-gated selectable providers N1-N9 — N1
already gives `SADeadLetterQueue` a provider that *optionally* injects `Serializer[Event]`; "lifecycle
is 050's"), 048 (`bind_settings()`; `WebhookSettings` is bound by `bind_core_settings()`), 049
(`VarcoModule` with `register/async_init/validate/start/stop`, `depends_on`, `resolve_module_order`
DFS post-order, `create_varco_app(container, modules=…)`, `CoreModule.start()` starts the bus + job
runner, `_ModuleComponent`/`_async_init_all` named as *the seams 050 replaces*, the "list
`MigrationsModule` first" WARNING named as *the placeholder 050 replaces with phases*), 051
(`@Multibound EventConsumer`; `CoreModule.start()` = bus → auto-`register_to()` every singleton
consumer; `stop()` = consumers reversed → bus; `RetentionRegistry` is derived/frozen from
`@Multibound RetentionPolicy` — **nothing of `RetentionRegistry` remains for this plan except a
phase**; "once 050 binds `WebhookDispatcher` as a singleton it is discovered with no further
change"). **Neighbours, by number, never absorbed**: **052** wires `container.validate()` into the
lifespan and renders `varco diagnose` — this plan makes `validate()` *clean* of the three disposer
kinds and exposes the realised lifecycle order for 052 to print; **056** (FLIP-13) makes
`SAWebhookSubscriptionRepository(encryptor=None)` refuse — this plan injects `FieldEncryptor` through
the provider so the refusal structurally never fires in a wired app; **058** assembles §Migration
and the docs.

**Research**: `design/research/014-multi-binding-qualifiers-and-conditional-di.md` §5 (`:138-168`
— Spring `SmartLifecycle.getPhase()`: lower starts first, higher stops first, shutdown is the exact
reverse; "explicit depends-on relationships override phase order"; per-phase shutdown timeout; .NET
`IHostedService` reverse registration order; *"providify: `@PreDestroy` on container shutdown, no
phase/priority mechanism"*), §Librarian #3 (`:252` — adopt the phase model over registration
order: *"Phases are discoverable and composable; registration order is implicit and fragile"*),
Evidence Gap 5 (`:227` — Quarkus does not document reverse shutdown; varco must *test* it, not
assume it).

**providify source of record**: `/home/edoardo/projects/providify` at 2.1.0. Every
`providify/…:line` below was read on 2026-09-17.

## Goal

After this plan: every `VarcoModule` declares a `phase` (an `int`; `varco_core.di.Phase` names five)
and may expose several phased `LifecycleComponent`s; `varco_core.di.LifecycleRunner` starts them
`(phase, dependency order)` ascending and stops **exactly the reverse of what actually started**,
rolls back a partial start in reverse, keeps stopping through failures and records every failure;
`VarcoLifespan` is a thin adapter over it; `container.ashutdown()` still runs last. `Kafka`/`NATS`
buses no longer start themselves at construction, so *nothing* starts before the `INFRA` phase
(migrations, tenancy, stores) has completed. Every provider-produced singleton that holds a
connection has a `@Disposes` in its own `@Configuration` (or a documented module owner), and a
default container reports zero `UNREACHABLE_PRE_DESTROY`/`UNMATCHED_DISPOSER`/`DISPOSER_OVERWRITTEN`
— the 046 `known_warnings` entries naming 050 are deleted. `OutboxRelay`, `WebhookDispatcher`, the
SA/Beanie webhook repositories, `ReloadingTrustStore`, and the four adapters receive
`Serializer[Event]`/`FieldEncryptor`/`AbstractEventBus`/settings from the container — a CloudEvents
swap reaches the outbox and every DLQ with no `serializer=` to remember, and CLAUDE.md's ⚠️ paragraph
saying otherwise is rewritten. The hand-constructed path keeps working; explicit arguments still win.

## Non-goals

- **Calling `validate()` from the lifespan; `varco diagnose`** — 052. This plan exposes
  `LifecycleRunner.plan` (the realised order) and `last_failures` as the data 052 prints.
- **FLIP-13's constructor-level refusal** — 056. This plan only makes the DI path inject
  `FieldEncryptor` so the refusal is unreachable through a provider.
- **Per-phase stop timeouts** (Spring's 30 s `timeoutPerShutdownPhase`, brief 014 `:145`). Parked
  with a trigger (§Parked); `AbstractJobRunner.stop(timeout=30.0)` keeps its own.
- **A worker/CLI host** running `LifecycleRunner` without FastAPI — allowed by construction (the
  runner is in `varco_core`), not built.
- **Auto-DDL policy** beyond `SAModule(ensure_tables=True)` — the migration story stays
  `technical_docs/features/schema-migrations.md`'s.
- **Passing the app DLQ to every auto-registered consumer** (051's `register_to(bus)` passes no
  `dlq=`) — a behaviour change for every consumer; `WebhookDispatcher` gets its own injected `dlq`
  instead. Recorded as a 051 follow-up.
- **A chaos test.** Nothing here provokes container failure; the invariants are ordering
  properties provable in-process. Stated so nobody adds one for completeness.
- **Any change to providify** — two asks are *filed* (§D-asks), neither is waited on.

## Context

### What `VarcoLifespan` does today (`varco_fastapi/varco_fastapi/lifespan.py`, read 2026-09-17)

| Behaviour | Where | Verdict |
|---|---|---|
| Start in registration order; on the first `start()` failure, stop the already-started list and re-raise | `:239-254` | ⛔ **Bug**: `:253` is `await self._stop_all(started)` — **forward** order. The happy path `:260` uses `reversed(started)`. Module 3 of 7 failing → modules 1 then 2 are stopped (bus before its consumer). Test-pinned and fixed here |
| Normal stop = exact reverse of `started` | `:256-260` | Correct; kept, now proven by `test_lifecycle_runner.py` across phases |
| `stop()` failures: log at ERROR, continue, never raise | `:312-324` | Kept — raising out of an `asynccontextmanager`'s `finally` during ASGI shutdown masks the real cause (`:273-278`'s own argument). **Added**: failures are *recorded* (`last_failures`) and one summary line is emitted; on the partial-start path they become `__notes__` on the propagating exception (§D-failures) |
| `shutdown=` hook (`container.ashutdown()`) runs after `_stop_all()`, also after a failed `setup`/`start`, never raises | `:261-264`, `:266-310` | Kept verbatim; `test_lifespan_shutdown*.py` unchanged |
| A component reachable by both `stop()` and `@PreDestroy`/`@Disposes` is stopped twice; "all ten shipped `@PreDestroy` components were measured idempotent" | `:211-213` | Elevated from a measurement to a **contract** with a test per wired object (§D-owner) |

### What providify 2.1.0 guarantees about disposal (read 2026-09-17)

| Claim | Where | Consequence |
|---|---|---|
| `ashutdown()`: phase 1 tears down cached singletons in **reverse creation order** (`_teardown_plan` walks `reversed(self._singleton_order)` — "Reverse creation order == reverse-dependency order (deps cached before their dependent)"), then phase 2 runs each installed `@Configuration`'s own `@PreDestroy` in reverse install order; failures aggregate into one `ShutdownError`; caches always clear | `container.py:4995-5023`, `:5314-5412` (`:5324-5328`) | Disposal order is **reverse of first resolution**, not of module start. It is therefore the right *backstop* and the wrong *primary* teardown for anything whose order matters (consumers before bus): a bus resolved by a provider during `INFRA` is created *before* a consumer created at `CONSUMERS`, so providify disposes the consumer first anyway — but only because resolution happened to follow the phases. §D-owner makes the module path primary and disposal the backstop |
| For a `ProviderBinding`, `_adispose` runs **only** `binding.disposer`; the produced instance's `@PreDestroy` is never consulted (Jakarta CDI producer rule) | `container.py:5297-5304`; `validation.py:97-101` | Every provider-produced singleton with a `stop()`/`aclose()`/`close()` needs a `@Disposes`, or a module that owns it |
| `UNREACHABLE_PRE_DESTROY` fires only when the produced type **carries `@PreDestroy`**; a provider-produced type with a plain `stop()` and no marker is invisible to `validate()` ("Tier B", as `varco_redis/dlq.py:794` calls it) | `container.py:6255-6280`; `_unreachable_pre_destroy` | "`validate()` is clean" ≠ "nothing leaks". §D-suite adds a varco-side check the container cannot make |
| `@Disposes` wiring is scoped to the bindings *this* `install()` registered (`own`); an unmatched disposer is `UNMATCHED_DISPOSER`, a same-module overwrite is `DISPOSER_OVERWRITTEN`, both `WARNING` | `container.py:7347-7392`; `validation.py:102-109` | P24 closed (045). A `@Disposes` must live in the `@Configuration` that declares the provider — never in a sibling |
| `provide(fn, *, returns=)` returns the `ProviderBinding` (2.1.0) but takes **no `disposer=`**; `ProviderBinding.disposer` is a plain attribute providify's own install loop assigns (`binding.disposer = getattr(instance, name)`) | `container.py:1250-1258`, `:7379`; `binding.py:749` | A function-level provider (`container.provide()`, `bind_instance`, `enable_*`) has no sanctioned way to get a disposer → upstream ask P-DISP (§D-asks) |
| `_filter()` returns every binding whose interface is a **subclass** of the requested type | `container.py:2051-2057` | A `container.provide(factory, returns=WebhookDispatcher)` binding is visible to 051's `acollect(container, EventConsumer)` — no `@Multibound` re-declaration needed |
| Provider parameter annotations are evaluated against `fn.__globals__` plus a `localns` of every registered interface's class name | `_annotations.py:180-235`; `container.py:3453-3483` | A nested factory **can** declare `bus: Inject[AbstractEventBus]` when `Inject` and `AbstractEventBus` are module-level imports of the defining module — the "deferred annotations make `Inject[...]` a string providify cannot recognise" DESIGN note in `varco_ws/di.py:253-258` is true only for closure-local names. A dependency whose *type is a runtime value* (`bind_mcp_adapter(client_cls=…)`) still cannot be declared → upstream ask P-INJ (§D-asks) |
| `@Requires(condition=, env=, value=)` — a zero-arg predicate or an env var; **no** "gate on another binding's presence" | `decorator/scope.py:818-892` | A `WebhookDispatcher` provider cannot be auto-registered "iff a repository is bound"; it is registered by an explicit `WebhookModule` (§D-webhook) |

### Every `start()`/`stop()` owner today, and who starts it after this plan

| Object | Lifecycle | Started today by | Phase after 050 | Owner after 050 |
|---|---|---|---|---|
| `KafkaEventBus` / `NatsEventBus` | `start()` is **`@PostConstruct`** (`varco_kafka/bus.py:220`, `varco_nats/bus.py:232`), `stop()` `@PreDestroy` | providify, at first resolution — *whenever that is* | `BUS` | `CoreModule`'s `bus` component (§D-bus); `@PreDestroy stop()` stays as backstop |
| `RedisEventBus`/`RedisStreamEventBus` | provider-produced; `@Disposes(AbstractEventBus)` `varco_redis/bus.py:594` | `CoreModule.start()` (049) | `BUS` | same |
| `EventConsumer`s (incl. `WebhookDispatcher`, the example's `PostEventConsumer`) | `start()`/`stop()` `consumer.py:992,1045` | `CoreModule.start()` auto-`register_to` (051) | `CONSUMERS` | `CoreModule`'s `consumers` component |
| `OutboxRelay` | `outbox.py:615,641` | `ReliabilityLifecycle.startup()` (`reliability.py:90-97`) | `CONSUMERS` | `ReliabilityModule` |
| `AuditConsumer` (hand-built, `reliability.py:105-106`) | `register_to` only — **never stopped** (`shutdown()` `:108-112` stops the relay only) | `ReliabilityLifecycle` | `CONSUMERS` | `ReliabilityModule` — `stop()` added |
| `AbstractJobRunner` (`JobRunner`, `runner.py:228,246`) | plain | `CoreModule.start()` (049) | `SCHEDULERS` | `CoreModule`'s `job_runner` component |
| `RetentionScheduler` (`scheduler.py:280,288`) | plain | `RetentionLifecycle` | `SCHEDULERS` | `RetentionModule` (phase only — 051 did the rest) |
| `TrustedIssuerRegistry.start_refresh()/stop_refresh()` | plain | `JwksRefreshLifecycle` | `SCHEDULERS` | `JwksRefreshModule` |
| `ReloadingTrustStore` (`tls/reload.py:138,150`) | plain; `bind_trust_store` never starts it (`tls/di.py:44-78`) | the application, by hand | `INFRA` | `TrustStoreModule` (§D-trust) |
| `CacheBackend`s with `InvalidationStrategy` (`RedisCache`, `LayeredCache`, `MemcachedCache`) | provider-produced, `@Disposes` present | first resolution (`async_bootstrap`) / nobody | `INFRA` | `RedisModule`/`MemcachedModule` `start()` (starts a resolvable `CacheBackend`); `@Disposes` backstop |
| `SAIdempotencyStore` (`idempotency.py:129,136`), `SADeadLetterQueue.ensure_table()`, `SAJobStore.ensure_table()` | `start()` = `create_all(checkfirst)`; `stop()` disposes the engine | nobody after 047 ("call `start()` yourself until 050") | `INFRA` | `SAModule.start()` (§D-stores); `@Disposes(AbstractIdempotencyStore)` backstop |
| `RedisIdempotencyStore` (lazy client, **no close**), `RedisTokenRevocationStore` (eager client `revocation.py:99`, **no close**), `RedisJobStore` (client made by 047's N7 provider) | none today | — | — | `aclose()` added; `@Disposes` in 047's `RedisStoresConfiguration` (§D-disposes) |
| `WebSocketEventBus`/`SSEEventBus` | `websocket.py:425,444`, `sse.py:234,254` | `WsModule.start()` (049) | `CONSUMERS` | `WsModule` |
| `SecurityPostureLifecycle` | `posture.py:306,395` | `SecurityPostureModule` | `EDGE` | same |
| `MigrationLifecycle`, `TenancyLifecycle`, `I18nLifecycle` | plain | their 049 modules | `INFRA` | same |
| `TracerProvider`/`MeterProvider` (`OtelConfiguration.tracer_provider/meter_provider`, `observability/di.py:237-312`) | `.shutdown()` flushes the `BatchSpanProcessor`/`PeriodicExportingMetricReader` — **no `@Disposes`, no `@PreDestroy`** → the last batch of spans is dropped at every shutdown | nobody | — | `@Disposes` (§D-disposes) |
| `CasbinPolicyEngine` (`engine.py:169,212`) | `@PostConstruct start()` loads policy; `@PreDestroy stop()` no-op | providify | — (unchanged) | class binding; no ordering dependency on any phase |

### The `@Provider` inventory — provider-produced singletons that hold a connection or buffer, and whether a disposer exists (`rg -n '^\s*@Provider' varco_*/varco_*` minus docstring examples, cross-checked against `rg -n '@Disposes'`)

| # | Provider | Produces | Teardown method | `@Disposes` today | Action |
|---|---|---|---|---|---|
| 1 | `varco_redis/bus.py:532 RedisEventBusSelectorConfiguration.bus` | Redis bus | `stop()` | ✅ `:594` | none |
| 2 | `varco_redis/cache.py:578 redis_cache` | `RedisCache` | `stop()` | ✅ `:604` | none |
| 3 | `varco_redis/cache.py:798 layered_cache` | `LayeredCache` | `stop()` | ✅ `:861` | none |
| 4 | `varco_redis/dlq.py:755 redis_dlq` | `RedisDLQ` | `aclose` | ✅ `:785` | none |
| 5 | `varco_redis/stream_dlq.py:768 redis_stream_dlq` | `RedisStreamDLQ` | | ✅ `:793` | none |
| 6 | `varco_redis/bulkhead.py:484 redis_bulkhead` | `RedisBulkhead` | | ✅ `:503` | none |
| 7 | `varco_memcached/cache.py:647 memcached_cache` | `MemcachedCache` | `stop()` | ✅ `:676` | none |
| 8 | `varco_kafka/dlq.py:787 kafka_dlq` | `KafkaDLQ` | `stop()` | ✅ `:819` | none |
| 9 | `varco_nats/dlq.py:774 nats_dlq` | `NatsDLQ` | `stop()` | ✅ `:804` | none |
| 10 | `varco_core/observability/di.py:237 tracer_provider` | `TracerProvider` | `shutdown()` | ❌ | **add** `@Disposes(TracerProvider)` |
| 11 | `varco_core/observability/di.py:309 meter_provider` | `MeterProvider` | `shutdown()` | ❌ | **add** `@Disposes(MeterProvider)` |
| 12 | 047 N2 `SAConfiguration.idempotency_store` | `SAIdempotencyStore` | `start()/stop()` | ❌ | **add** `@Disposes(AbstractIdempotencyStore)`; `stop()` must not dispose an injected engine (⚠️ ASSUMPTION on 047's `engine=` shape — Step 1) |
| 13 | 047 N6 `RedisStoresConfiguration.idempotency_store` | `RedisIdempotencyStore` | **none** | ❌ | add `aclose()`; **add** `@Disposes(AbstractIdempotencyStore)` |
| 14 | 047 N7 `RedisStoresConfiguration.job_store` | `RedisJobStore` | none (client is the provider's) | ❌ | provider keeps the client on `self`; **add** `@Disposes(AbstractJobStore)` closing it |
| 15 | 047 N8 `RedisStoresConfiguration.revocation_store` | `RedisTokenRevocationStore` | **none** | ❌ | add `aclose()`; **add** `@Disposes(AbstractTokenRevocationStore)` |
| 16 | `varco_redis/di.py:284 enable_redis_token_revocation` (function-level, 046 `bind_instance` shape) | same | none | impossible today | `bind_instance(..., disposer=store.aclose)` via the P-DISP shim (§D-asks) |
| 17 | `varco_ws/di.py:273,386 bind_websocket_adapter/bind_sse_adapter` | push adapters | `start()/stop()` | impossible | **module-owned** (`WsModule`); construction opens nothing — no disposer needed (§D-owner) |
| 18 | `varco_core/tls/di.py:77 bind_trust_store` | `ReloadingTrustStore` | `start()/stop()` | impossible | **module-owned** (`TrustStoreModule`) |
| 19 | `varco_fastapi/router/mcp.py:1022`, `router/skill.py:1153` | adapters | none | — | none (no connection) |
| 20 | 047 N1 `SAConfiguration.dead_letter_queue`, N3 `job_store`, N4/N5 Beanie stores, `varco_sa/di.py:210,259` locks, `varco_fastapi/di.py:280-433`, `:619`, `varco_core/{flags,tenancy,revocation}/di.py` | value objects / engine-borrowing objects | none | — | none |

The three `@Disposes` in `RedisCacheConfiguration`/`RedisLayeredCacheConfiguration` for one
`CacheBackend` interface are **not** `DISPOSER_OVERWRITTEN` — they live in different modules, and
P24's scoping makes each attach to its own binding (045's `test_redis_cache_disposes.py` already
proves it). Rows 10-16 are the entire remaining work list; Step 1 diff-checks it against the 046
`known_warnings` entries naming 050.

## Design decisions

### §D-phase — a phase is coarse and decoupled; `depends_on` is fine and named; both, as Spring

**Chosen: `varco_core.di.Phase` is an `IntEnum` naming five values with room between them;
`VarcoModule.phase: ClassVar[int] = Phase.CONSUMERS`; every in-tree module declares `phase` in its
own `__dict__` (test-pinned). Lifecycle order = stable sort of 049's `resolve_module_order()` result
by `phase`. A `depends_on` edge pointing at a module in a *later* phase is a `ModulePhaseError`
raised before anything registers.**

```
INFRA      = 100   # migrations, tenancy pools, trust stores, catalogs, stores (DLQ/idempotency/job/revocation), repositories, caches
BUS        = 200   # the event bus
CONSUMERS  = 300   # EventConsumers, OutboxRelay, WebhookDispatcher, WS/SSE adapters, AuditConsumer   ← default for app modules
SCHEDULERS = 400   # AbstractJobRunner, RetentionScheduler, JWKS background refresh
EDGE       = 500   # SecurityPosture preflight — observes the fully-started world
```

Why both, and not `depends_on` alone: `depends_on` already yields "start after, stop before" for a
*named* module (049), so bus-before-consumer follows from `depends_on=(CoreModule,)`. It cannot
express "a relay starts after **whichever** package provides the store" or "migrations run before
the bus, even though `CoreModule` knows nothing about `MigrationsModule`" — 049 had to ship a
WARNING for exactly that hole. Spring's answer (brief 014 `:141-146`) is the same split: an integer
phase for the coarse, anonymous order and `depends-on` for the fine, named one. Phase is therefore
**not** documentation: it is the only thing that orders two modules that do not know each other.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: `int` phase + `depends_on`; cross-phase contradiction is an error** | Anonymous ordering across packages (the DI-9 row's three invariants need no class names); 049's sort is reused unchanged and merely re-keyed; a user may pick `Phase.BUS + 50` (Spring's "any integer", `:143`); explicit over implicit (brief 015 §Librarian #1, 049's `ModuleDependencyError` precedent) — a `WebhookModule(phase=INFRA)` that `depends_on` a `CONSUMERS` module is a wiring mistake, named at `create_varco_app()` time, not silently rescheduled | An app module spanning phases needs `components()` (§D-components); one more `ClassVar` on the ABC |
| (2) Spring-literal: "depends-on overrides phase" — pull the dependency forward | Never fails | The realised order then differs from what the phases say, so `varco diagnose` (052) printing phases would lie; silent reordering is the registration-order model 014 rejects wearing a different hat. Rejected |
| (3) `depends_on` only, phases as docstrings | Nothing new | Cannot order `MigrationsModule` before `CoreModule` without naming it (049's WARNING stays forever); every backend store module would need `ReliabilityModule.depends_on` to name it. Rejected |

### §D-components — one module may contribute several phased components; `CoreModule` and `WebhookModule` are the in-tree cases

**Chosen: a new slot `VarcoModule.components(container) -> Sequence[LifecycleComponent]` whose
default returns one component `(name, phase, start=partial(self.start, container),
stop=partial(self.stop, container))`. `LifecycleComponent(name: str, phase: int, start:
Callable[[], Awaitable[None]], stop: Callable[[], Awaitable[None]])` is a frozen dataclass.**

`CoreModule.components()` returns four, replacing its 049/051 `start()`/`stop()` bodies, in the
same internal order those bodies had: `("core.stores", INFRA)` — for each of
`AbstractIdempotencyStore`, `AbstractDeadLetterQueue`, `AbstractJobStore`,
`AbstractTokenRevocationStore`, `CacheBackend` that `is_resolvable` and whose resolved object has
`start`, await it (the generalised "seam's owner starts what resolves" rule 049 §D-core stated for
the bus; stop = reverse); `("core.bus", BUS)`; `("core.consumers", CONSUMERS)` — 051's
auto-`register_to` loop verbatim; `("core.job_runner", SCHEDULERS)`. `WebhookModule.components()`
returns `("webhook.repository", INFRA)` (the repository's `start()`/`stop()`) and
`("webhook.dispatcher", CONSUMERS)` (a no-op start, present so the module is visible in the plan).

✅ Spring's unit of phase is a bean, not a configuration — a module is a configuration; ✅ 051's
"keep `CoreModule.start()`'s internal order (bus → consumers) and `stop()`'s reverse" holds because
`BUS < CONSUMERS` and the runner reverses; ✅ the `di_wiring` suite can assert each component's
phase by name. ❌ A seventh slot on the ABC — 049's `dir()`-guard test is updated deliberately
(that is what the guard is for). Rejected — **split `CoreModule` into `StoresModule`/`BusModule`/
`ConsumersModule`/`JobRunnerModule`**: ✅ strictly one-phase-per-module; ❌ four more default
modules, every explicit `modules=[CoreModule(), FastAPIModule()]` test list from 049 stops
starting a bus, and `depends_on=(CoreModule,)` on ten backend modules would need re-pointing.

### §D-runner — `LifecycleRunner` in `varco_core`; `VarcoLifespan` becomes an adapter

**Chosen: `varco_core/varco_core/di/lifecycle.py::LifecycleRunner(components: Sequence[AbstractLifecycle
| LifecycleComponent], *, setup=None, shutdown=None)` with `async def __aenter__/__aexit__` (and a
`run()` `asynccontextmanager`), owning: start in the given order; `started` list; on a `start()`
exception stop `reversed(started)` (**the `:253` fix**), attach each rollback stop failure as
`exc.add_note(...)`, re-raise the original; on exit stop `reversed(started)`; `last_failures:
tuple[LifecycleFailure, ...]` and `plan: tuple[str, ...]` (component names in realised start
order) as public reads; `shutdown` hook last, contained, exactly `VarcoLifespan._run_shutdown_hook`'s
body moved. `VarcoLifespan.__call__(app)` becomes `async with LifecycleRunner(self._components,
setup=…, shutdown=…) as runner: self.last_runner = runner; yield`.** `AbstractLifecycle` moves to
`varco_core.di.lifecycle` and is re-exported from `varco_fastapi.lifespan` (identity preserved —
`test_tls_reload_lifecycle`'s `isinstance` check keeps passing).

✅ The five invariant tests are Docker-free and FastAPI-free (`varco_core/tests/`); ✅ a worker host
is possible without importing `varco_fastapi`; ✅ `VarcoLifespan`'s public surface (`register()`,
`setup=`, `shutdown=`, the hand-wired form in `tls/di.py`'s docstring) is unchanged. ❌ One class
moves packages — a re-export keeps every import site green. Rejected — **fix `:253` in place and
leave the orchestrator in `varco_fastapi`**: ✅ smaller; ❌ the reverse/rollback/aggregation tests
would need FastAPI's lifespan machinery to run, and 049 already noted the worker-host direction.

`create_varco_app` (049 §D-order's sequence) changes in exactly one line: the components handed to
`VarcoLifespan` are `[c for m in resolve_lifecycle_order(active) for c in m.components(container)]`,
then stable-sorted by `c.phase` (a module's own components may span phases, so the sort is over
components, keyed by phase, with module order as the tiebreak). `register()`, `async_init()`,
`middleware()`, `configure_app()` keep 049's dependency order — install order is not lifecycle
order. 049's "list `MigrationsModule` first" WARNING and its test are **deleted**.

### §D-failures — stop failures: collect, keep going, record, one summary line; never raise from shutdown

| Path | Behaviour |
|---|---|
| Normal shutdown, component N's `stop()` raises | ERROR line per failure (unchanged), all remaining components still stopped, **then** one `ERROR "LifecycleRunner: %d of %d components failed to stop: [...]"`, `last_failures` populated, nothing raised (the `:273-278` argument stands) |
| Partial start: component K's `start()` raises, rollback stop of J < K also raises | The rollback continues to 1; each rollback failure is `add_note()`d onto K's exception (`"while rolling back, stop() of <J> raised: <exc!r>"`); K's exception propagates unchanged in type and identity — FastAPI fails startup as before |
| `setup` (049's `_async_init_all`) raises | No component started; `shutdown` hook still runs (unchanged from `:231-237`) |
| `shutdown` hook (`ashutdown()`) raises `ShutdownError` | Enumerated per `ShutdownFailure`, contained (unchanged, `:295-304`) |

✅ One aggregate is readable by a test and by 052 without parsing logs; ✅ `add_note` (3.11+, repo
floor is 3.12) keeps the original exception's type so `pytest.raises(RuntimeError)` sites are
untouched. Rejected — **raise an `ExceptionGroup` from shutdown**: ❌ the ASGI-shutdown masking
argument; ❌ every `test_lifespan_shutdown*.py` assertion about "never raises" would flip.

### §D-bus — `@PostConstruct` comes off `KafkaEventBus.start()` / `NatsEventBus.start()`

**Chosen: remove the `@PostConstruct` marker from the two `start()` methods (`varco_kafka/bus.py:220`,
`varco_nats/bus.py:232`); keep `@PreDestroy stop()` as the idempotent backstop. The `BUS` phase is
the one starter.** With the marker, a Kafka bus starts *at first resolution* — during
`async_init()`, or inside `_bus_event_producer` (`varco_fastapi/di.py:619`) when any service
injecting `AbstractEventProducer` is built — i.e. before migrations have run, and at a moment no
phase controls. That is the "order depends on resolution" model this plan exists to remove.

✅ "Nothing starts before `INFRA` completes" becomes true for every bus, not just Redis; ✅
`CoreModule`'s `bus` component already calls `start()` (idempotent, `bus.py:221-223`'s guard);
❌ **BREAKING** for a hand-wired app that resolved the bus from a container and never called
`start()` — §Migration row; ⚠️ ASSUMPTION `publish()` on an unstarted Kafka/NATS bus raises a
clear error rather than hanging (Step 1 reads both `publish()` bodies; if either hangs, a
`RuntimeError("bus not started — Plan 050")` guard is added in the same step). `CasbinPolicyEngine`'s
`@PostConstruct start()` is **not** touched — it loads policy, opens no long-lived connection, and
has no ordering relationship with any phase.

### §D-owner — one ordered owner (the module), disposal is the backstop, `stop()` is idempotent by contract

**Chosen: a resource with a phase is stopped by its module's component; providify disposal
(`@Disposes`/`@PreDestroy`) is the backstop for the half-built app (`async_init()` failed, a
singleton was constructed but its phase never ran) and for a container used without a lifespan.
Therefore every wired object's `stop()`/`aclose()` is **idempotent** — a test per object
(§D-suite's `test_double_stop_is_noop`). Objects registered through `bind_instance`/`contribute`
are **caller-owned**: the container never disposes them unless `bind_instance(..., disposer=)` is
given; a module that constructs one on the caller's behalf (`enable_redis_token_revocation`) passes
`disposer=`.**

✅ Ordering lives in one place (the runner) and disposal order (reverse creation) no longer has to
coincide with it; ✅ matches what `lifespan.py:206-213` already promised, now tested rather than
measured; ❌ a resource stopped twice logs twice at DEBUG — accepted. Rejected — **exclusive
ownership (no `@Disposes` for anything a module stops)**: ❌ `RedisModule(cache=True)` installs the
cache provider at `async_init()`; a later `async_init()` failure leaves a started pool with no
module `stop()` ever reached — exactly Plan 022's six-orphans finding.

### §D-disposes — rows 10-16 get their disposer; `validate()` is clean of the three kinds

Each new `@Disposes` lives on the `@Configuration` that declares the provider (P24's scoping rule),
is `async def` where the teardown awaits, and is a one-liner delegating to the object's own
idempotent teardown: `OtelConfiguration.shutdown_tracer_provider(@Disposes(TracerProvider))` →
`provider.shutdown()` (sync — flushes the batch processor); `shutdown_meter_provider`;
`SAConfiguration.close_idempotency_store(@Disposes(AbstractIdempotencyStore))` → `store.stop()`;
`RedisStoresConfiguration.close_idempotency_store` → `store.aclose()`, `close_job_store` → closes
`self._job_store_client` (the provider keeps the `aioredis` client it created on the configuration
instance so no `RedisJobStore` signature changes), `close_revocation_store` → `store.aclose()`.
`RedisIdempotencyStore.aclose()` and `RedisTokenRevocationStore.aclose()` are additive methods
(close the client if open; idempotent). ⚠️ ASSUMPTION — 047's `SAIdempotencyStore(engine=)` path:
`stop()` must dispose only an engine the store created (`self._owns_engine`); if 047 did not add
the flag, this plan adds it (a caller's pool disposed at shutdown by a store that borrowed it is a
bug, not a nuance).

After this, the `di_wiring` suite's `known_warnings` has **no entry naming 050** in any of the ten
subclasses, and `test_every_provider_singleton_has_a_reachable_disposer` — whose message this plan
makes explicit: *"these three `IssueKind`s are what Plan 050 closed; a new entry here needs a
`@Disposes` in the declaring `@Configuration`, not an allowlist row"* — passes with
`known_warnings == frozenset()` for every package.

### §D-suite — the varco-side check providify cannot make, plus phase/reverse assertions

`testkit/varco_conformance/di_wiring.py` gains: `module_owned: ClassVar[frozenset[type]] =
frozenset()` (interfaces whose lifecycle a module owns, e.g. `varco_ws`: `{WebSocketEventBus,
SSEEventBus}`) and three tests: `test_every_lifecycle_bearing_provider_has_a_disposer_or_owner` —
for every interface in `container.describe().bindings`, for every `ProviderBinding` in
`get_all_bindings(iface)` with singleton scope, if the produced type (`get_origin(iface) or iface`,
or the provider's `returns`) has any of `stop`/`aclose`/`close`/`shutdown` then `binding.disposer
is not None or iface in module_owned` (the read of `.disposer` is the same public-attribute read
046 makes of `.fallback`/`.scope`); `test_package_module_declares_a_phase` — `"phase" in
module_cls.__dict__` and `Phase.INFRA <= phase <= Phase.EDGE`; `test_depends_on_never_points_forward`
— every `d in module_cls.depends_on` has `d.phase <= module_cls.phase`. `COVERAGE.md`'s
`di_wiring` row gains a "phase · module_owned" column.

### §D-wire — DI-10, object by object; explicit arguments always win

| Object | Provider / module | Injected (declared on the provider method — never `container.get()` inside) | Hand path |
|---|---|---|---|
| `OutboxRelay` | `ReliabilityModule.start()` builds it via `build_outbox_relay(container, preset)` (`varco_fastapi/reliability.py`) | `serializer = await container.aget(Serializer[Event])` when resolvable (the 045 fallback is `JsonEventSerializer`, so it always resolves); `dlq`/`retry_policy`/`max_attempts` from the preset as today | `OutboxRelay(outbox=…, bus=…, serializer=…)` unchanged; `ReliabilityPreset.serializer: Serializer[Event] \| None = None` **added** as the explicit override (frozen dataclass field, additive) |
| `SADeadLetterQueue` | 047 N1 — exists | `serializer` — already declared, optional; **this plan makes it required-when-bound**: the parameter stays `Annotated[Serializer[Event] \| None, InjectMeta(optional=True)]` and the guard test asserts the *bound* serializer arrives | unchanged |
| `BeanieDeadLetterQueue` (046 B9), `KafkaDLQ`/`NatsDLQ`/`RedisDLQ` | existing providers | Step 1 audits each provider method declares `serializer`; any that does not is fixed in the same step (the exact bug `test_redis_cloudevents_di.py` documents for Redis) | unchanged |
| `WebhookDispatcher` | **`WebhookModule`** (§D-webhook) | `repository: Inject[WebhookSubscriptionRepository]`, `settings: Inject[WebhookSettings]` (048-bound), `dlq: Annotated[AbstractDeadLetterQueue \| None, InjectMeta(optional=True)]` → new additive constructor kwarg `dlq=` (captured exactly as `register_to(dlq=)` captures it; `register_to(dlq=)` still overrides) | `WebhookDispatcher(repository=…)` + `register_to(bus, dlq=…)` unchanged |
| `SAWebhookSubscriptionRepository` / `BeanieWebhookSubscriptionRepository` | `SAConfiguration.webhook_repository` / `BeanieConfiguration.webhook_repository`, gated `@Requires(env="VARCO_WEBHOOK_BACKEND", value="sa"/"beanie")` in 047's N1-N9 shape; `InMemoryWebhookSubscriptionRepository` is the `@Fallback` registered by `WebhookModule._register()` | `encryptor: Inject[FieldEncryptor]` — **required, no default**: an app selecting a durable backend without binding a `FieldEncryptor` gets `MISSING_BINDING` from `validate()` (052 fails startup) instead of plaintext secrets; `config: Inject[SAConfig]` → additive `engine: AsyncEngine \| None = None` kwarg (047 N2's exact shape; `url` becomes optional, exactly one of the two, `stop()` disposes only an owned engine) | `SAWebhookSubscriptionRepository(url=…, encryptor=…)` unchanged (056 owns the `None` refusal) |
| `ReloadingTrustStore` | **`TrustStoreModule`** (§D-trust) | `spec: Inject[TrustStore]` | `bind_trust_store(container, store)` + `TrustStoreModule(store=store)` |
| `RetentionRegistry` | 051 — done | — | — |
| `WebSocketEventBus` / `SSEEventBus` | `bind_websocket_adapter` / `bind_sse_adapter` (046 shape kept) | factory declares `bus: Inject[AbstractEventBus]` (module-level imports of `Inject`/`AbstractEventBus` in `varco_ws/di.py`; the `:253-258` DESIGN note rewritten) — `validate()` now reports a missing bus as `MISSING_BINDING` instead of a `LookupError` at first request | unchanged |
| `MCPAdapter` / `SkillAdapter` | `bind_mcp_adapter` / `bind_skill_adapter` (046 shape kept) | static deps declared; the runtime-class deps (`client_cls`, `runner_cls`, `store_cls`, `conv_store_cls`) stay resolved inside the factory through **one** helper `varco_fastapi.router._di._resolve_optional(container, cls)` named as the P-INJ shim (§D-asks) | unchanged |

**Guard tests** — `test_<pkg>_cloudevents_di.py` in the `test_redis_cloudevents_di.py` shape for
`varco_sa` (N1 + outbox), `varco_beanie`, `varco_kafka`, `varco_nats`, and `varco_core` (`OutboxRelay`
through `ReliabilityModule`, `WebhookDispatcher` through `WebhookModule`): bind
`CloudEventsSettings(source="/varco/tests")` after bootstrap → the resolved object's serializer `is`
the container's `CloudEventsJsonSerializer`. Plus one **negative** per provider — a copy of the
provider method with `serializer` removed from its signature, installed into a fresh container,
resolves to `JsonEventSerializer` — pinning the exact failure shape ("an undeclared `Serializer[Event]`
silently never arrives") so a future refactor that drops the parameter fails a named test.

### §D-webhook — `WebhookModule`, the explicit home for a consumer that needs a repository

`varco_core/varco_core/webhook/module.py::WebhookModule(repository: WebhookSubscriptionRepository |
None = None, *, settings: WebhookSettings | None = None, enabled: bool | None = None)`,
`name="webhook"`, `phase=Phase.CONSUMERS`, `depends_on=(CoreModule,)`. `_register()`:
`bind_settings(container, WebhookSettings, instance=settings)` when given; `bind_instance(container,
WebhookSubscriptionRepository, repository)` when given, else the `@Fallback`
`InMemoryWebhookSubscriptionRepository` provider (the 045 shape, `BACKEND_PRIORITY`) so a durable
backend selected by env or bound by code wins; `container.provide(Provider(singleton=True)(
_dispatcher_factory), returns=WebhookDispatcher)` with the declared injections of §D-wire.
`components()` per §D-components. `_enabled_from_settings()` reads
`WebhookSettings.enabled` — ⚠️ ASSUMPTION the field exists after 047/048 (`feature_default(
"webhook")`); Step 1 reads it, else the module is enabled iff constructed.

✅ A `VarcoModule` is never scanned (049 §D-scan), so the ⛔ "no scanned `@Singleton`/`@Provider` in
`varco_core.webhook`" rule holds by construction; ✅ 051's `acollect(container, EventConsumer)`
finds the provider binding via `_filter`'s subclass match and auto-registers it at `CONSUMERS`,
after the repository component started at `INFRA`; ✅ `mount_webhook_admin(app, repository=…)`
keeps its explicit argument and may now also be handed `container.get(WebhookSubscriptionRepository)`.
❌ An app that wants webhooks lists one more module — the "exactly those" rule. Rejected — **a
`@Fallback` dispatcher provider registered by `varco_core.di.bootstrap()`**: ❌ requires a repository
binding to exist in every default container (or a `NullWebhookSubscriptionRepository` — a Null
Object that makes "webhooks silently do nothing" the default, the opposite of what a webhook
operator wants); `@Requires` cannot gate on a binding's presence (`scope.py:818-823`).

### §D-trust — `TrustStoreModule`, honouring the T3 decision

`varco_core/varco_core/tls/module.py::TrustStoreModule(store: ReloadingTrustStore | None = None, *,
spec: TrustStore | None = None, watcher: AbstractPathWatcher | None = None, enabled: bool | None =
None)`, `name="trust_store"`, `phase=Phase.INFRA`, `depends_on=(CoreModule,)`. `_register()`: with
`store` → `bind_trust_store(container, store)` (046's two `bind_instance` calls, unchanged); with
`spec` → `bind_instance(container, TrustStore, spec)` then a provider
`reloading_trust_store(spec: Inject[TrustStore]) -> ReloadingTrustStore` (constructs
`ReloadingTrustStore(spec, watcher=watcher)`); with neither → the same provider over whatever
`TrustStore` is bound (`varco_fastapi.di`'s env fallback, or the app's). `start()` awaits
`store.start()`; `stop()` `store.stop()`. No `@Configuration`, no scan side effect — the
`test_tls_di.py` guard stays green and gains a `TrustStoreModule` leg (constructing the module
registers nothing until `register()`; `register()` starts no watcher).

### §D-stores — `SAModule.start()` owns SA framework-table readiness

`SAModule(config=None, *, entities=(), ensure_tables: bool = True)`, `phase=Phase.INFRA`. `start()`:
for each of `AbstractIdempotencyStore`/`AbstractDeadLetterQueue`/`AbstractJobStore`/
`WebhookSubscriptionRepository` that `is_resolvable` **and** resolves to a `varco_sa` class: await
`start()` if present, else `ensure_table()` if present and `ensure_tables`. `ensure_tables=False`
for a deployment where Alembic owns every framework table (`schema-migrations.md`'s ten-table
story). `CoreModule`'s `core.stores` component (§D-components) starts the same seams *generically*
(`start()` only, never `ensure_table()`) — the two never double-run because `start()` is idempotent
(§D-owner) and `ensure_table()` is `checkfirst`. ✅ `SADeadLetterQueue`'s docstring "call
`ensure_table()` at startup" stops being a thing to remember. `BeanieModule` gets the mirror for
`init_beanie()`-needing stores only where 047 N4/N5's docstrings say the app must do it — ⚠️ Step 1
reads them; if `init_beanie` is already the module's `async_init()`, nothing to add.

### §D-asks — two upstream asks, filed, guarded, not waited on

Per CLAUDE.md §"When you hit a providify limitation": the report is a file, the ledger a row, the
guard a `strict=True` xfail. Numbers are the next free `P` after Step 1 reads `UPSTREAM-GAPS.md`
(P24-P27 in the ledger today; 045 filed P28, 046 P29; called **P-DISP** and **P-INJ** below).

- **P-DISP — `provide(fn, *, returns=, disposer=)`** (`design/upstream-gaps/providify-provide-disposer.md`).
  §1 cite `container.py:1250` (no kwarg), `:7379` (providify itself assigns `binding.disposer`),
  `binding.py:749`. §2 what varco pays: rows 16-18 of the inventory. §3 repro. §4 the ask (✅
  additive, symmetric with `returns=`; alt `@Provider(disposer=)` ✅ decoration-time ❌ needs the
  instance-bound method). §5 partly ours? **Partly** — rows 17-18 are solved by module ownership
  without upstream; only row 16's `enable_*`-constructed object needs it. §6 interim: `varco_core.di.
  bind_instance(container, I, obj, *, qualifier=None, disposer=None)` assigns `binding.disposer` on
  the returned binding through one private function `_attach_disposer()` named as the shim. §7
  guard: `test_providify_contracts.py::test_provide_accepts_disposer_kwarg`, `strict=True`,
  `raises=TypeError`.
- **P-INJ — `@Provider(inject={"client": SomeClass})` / `provide(fn, inject=…)`** — parameter
  injection overrides for a type only known at call time (`providify-provider-inject-override.md`).
  §1 cite `_annotations.py:180-235` (annotations are evaluated from `__globals__`/`localns`; a
  closure-local runtime type cannot be named) and `decorator/scope.py:497-511` (`returns=` exists,
  no parameter twin). §2: `bind_mcp_adapter`/`bind_skill_adapter`'s four runtime-class deps. §5
  partly ours? **Yes for ws/sse** (fixed here by module-level imports) — said plainly; **no** for a
  runtime class. §6 interim: `_resolve_optional()`. §7 guard: `test_provider_accepts_inject_override`,
  `strict=True`.

### Alternatives considered (plan-level)

- **Keep 049's list-order model and only fix `:253`**: ✅ tiny; ❌ leaves the DI-9 row open and the
  "list `MigrationsModule` first" WARNING as a permanent workaround. Rejected.
- **Providify-native phases (a `@Configuration` per phase, `depends_on=`)**: ❌ 049 §D-shape
  already rejected `@Configuration` as the module unit (scan activation, container-constructed
  only); providify's `@PreDestroy` on modules runs *after* every singleton is dead
  (`container.py:5380-5402`) — the wrong moment for "consumers stop before bus". Rejected.

## Steps

All commands from `/home/edoardo/projects/varco`. Steps 1-4 first, in order (the runner and the
phase model); 5-9 in any order (disposers, wiring); 10-13 after 5-9; 14-17 last. `make test` is
expected red from Step 3 until Step 4 lands — one commit.

### Preflight and the core primitive

1. [ ] **Preflight — measure every ⚠️ ASSUMPTION before writing code.** Record in
       `/tmp/claude-1000/…/scratchpad/050-preflight.txt`: (a) `rg -n '050' varco_*/tests/test_*_di_wiring*.py
       testkit/varco_conformance/` — the `known_warnings` entries naming this plan; diff against the
       inventory rows 10-16 (a row here and not there is a new finding; add it to the table). (b)
       `rg -n 'class CoreModule' -A 60 varco_core/varco_core/di/module.py` — 049/051's
       `start()`/`stop()` bodies to move into `components()`; confirm `register` is `@final` and the
       `dir()` guard test's slot list. (c) `rg -n 'def start|_owns_engine|engine:' varco_sa/varco_sa/idempotency.py`
       — 047's `engine=` shape and whether `stop()` disposes a borrowed engine. (d) `rg -n 'def publish' -A 25 varco_kafka/varco_kafka/bus.py varco_nats/varco_nats/bus.py`
       — what an unstarted bus does on `publish()` (§D-bus). (e) `rg -n 'enabled|feature_default' varco_core/varco_core/webhook/settings.py varco_core/varco_core/di/features.py`
       — `WebhookSettings.enabled` and 047's seam-registration shape (`selected_by`/catalogue) so
       `webhook_repository` is added as N1-N9 were. (f) `rg -n 'serializer' varco_beanie/varco_beanie/dlq.py varco_kafka/varco_kafka/dlq.py varco_nats/varco_nats/dlq.py varco_sa/varco_sa/di.py`
       — every DLQ provider method declares `serializer`. (g) `rg -n 'init_beanie' varco_beanie/varco_beanie/modules.py varco_beanie/varco_beanie/di.py`
       (§D-stores). (h) `tail -n 30 UPSTREAM-GAPS.md` — next free P number. (i) Confirm every
       providify citation in §Context against the installed 2.1.0
       (`.venv/lib/python3.12/site-packages/providify/{container,binding,validation,_annotations}.py`,
       `decorator/{lifecycle,scope,multibinding}.py`). (j) `uv run pytest varco_fastapi/tests/test_lifespan_shutdown.py varco_fastapi/tests/test_lifespan_shutdown_characterization.py -q`
       green before anything moves. **Verify**: the file lists (a)-(j) with outcomes; any moved
       citation is corrected in this plan before Step 2.

2. [ ] `varco_core/tests/test_lifecycle_runner.py` (**new, failing first**) — the runner contract
       from §D-runner/§D-failures, Docker-free, no FastAPI, using spy components that append to a
       shared log:
       - start order = given order; stop order = **exact reverse of what started** (five
         components, assert the log literally).
       - partial start: component 3 of 5 raises → log is `start1, start2, start3(raise), stop2,
         stop1` (reverse — the `:253` regression pinned by name), components 4-5 never touched, the
         original exception propagates with identity preserved (`is`), and when `stop2` also
         raises the propagated exception carries one `__notes__` entry naming component 2.
       - stop failures: components 2 and 4 raise on stop → all five stop calls happen, nothing is
         raised, `runner.last_failures` has two entries `(name, exc)` in stop order, `caplog` has
         the per-failure lines plus one summary line.
       - `shutdown` hook: awaited after the last stop; awaited even when `setup` raised; a hook
         raising `ShutdownError(failures=[...])` yields one ERROR line per failure and nothing
         raised; a hook raising `ValueError` is contained.
       - `runner.plan` equals the component names in realised order after enter; empty after a
         `setup` failure.
       - `AbstractLifecycle` is importable from `varco_core.di.lifecycle` and `varco_fastapi.lifespan`
         and is the same object.
       **Verify**: `uv run pytest varco_core/tests/test_lifecycle_runner.py -q` → red, `ImportError`.

3. [ ] `varco_core/varco_core/di/lifecycle.py` (**new**) — `Phase`, `LifecycleComponent`,
       `LifecycleFailure(name: str, exception: BaseException)` (frozen), `AbstractLifecycle` (moved),
       `LifecycleRunner` (§D-runner), `ModulePhaseError(ValueError)` with `.module`/`.dependency`,
       `resolve_lifecycle_order(modules) -> list[VarcoModule]` (049's `resolve_module_order` then a
       stable sort by `phase`, raising `ModulePhaseError` on a forward edge), and
       `phased_components(modules, container) -> list[LifecycleComponent]` (flatten + stable sort by
       `phase`). Module docstring = §D-phase's table condensed + the ✅/❌ block + the sentence
       "phase is the coarse, anonymous order; `depends_on` the fine, named one — both, as Spring
       `SmartLifecycle` (brief 014 §5)". `varco_fastapi/varco_fastapi/lifespan.py`: `VarcoLifespan.__call__`
       delegates; `_stop_all`/`_run_shutdown_hook` deleted (moved); module docstring's "Recommended
       startup order" block rewritten as "phases do this for a modules-built app; the hand form
       keeps registration order"; `AbstractLifecycle` re-exported. `varco_core/di/__init__.py`
       lazily exports the new names. **Verify**: Step 2 green; Step 1(j) suite still green;
       `uv run python scripts/import_budget.py --check --warn-only` unchanged (no eager import in
       `varco_core.di.__init__`).

4. [ ] **The phase model on `VarcoModule`** — `varco_core/tests/test_di_module.py` (049's,
       **extended, failing first**): `VarcoModule.phase` defaults to `Phase.CONSUMERS`; `components()`
       default yields one component with the module's `name`/`phase` whose `start`/`stop` call the
       slots with the container; `resolve_lifecycle_order([B(CONSUMERS), A(INFRA)])` with no edges
       → `[A, B]`; `B.depends_on=(A,)`, `A.phase=SCHEDULERS`, `B.phase=INFRA` → `ModulePhaseError`
       naming both; within one phase 049's dependency order is preserved (three modules, one edge);
       `phased_components()` interleaves two modules' multi-phase components correctly (`CoreModule`
       shape: `[core.stores, other.infra, core.bus, core.consumers, other.consumers, core.job_runner]`
       for `[core, other]` where `other` contributes `INFRA` + `CONSUMERS`); the `dir()` guard now
       lists seven slots + `is_enabled`. Then `varco_core/varco_core/di/module.py`: `phase`,
       `components()`, `CoreModule.components()` per §D-components (its 049/051 `start()`/`stop()`
       bodies become the four components' closures; `CoreModule.phase = Phase.INFRA` is set for
       the record but `components()` is what runs), `ObservabilityModule.phase = INFRA`,
       `LifecycleModule(component, *, name=None, depends_on=(), phase=Phase.CONSUMERS)` (instance
       attribute shadowing the `ClassVar`, the 049 `depends_on` precedent). **Verify**: `uv run pytest
       varco_core/tests/test_di_module.py -q`.

### Phases on every module; `create_varco_app`; the bus

5. [ ] **Declare `phase` on every in-tree module** — `varco_fastapi/varco_fastapi/modules/lifecycle.py`
       (`MigrationsModule`/`TenancyModule` `INFRA`; `ReliabilityModule` `CONSUMERS`;
       `RetentionModule`/`JwksRefreshModule` `SCHEDULERS`; `SecurityPostureModule` `EDGE`),
       `modules/edge.py` (`LocalizationModule` `INFRA`; every middleware-only module `INFRA` with a
       one-line comment "no lifecycle; declared so the suite's phase test is uniform";
       `FastAPIModule` `INFRA`), `varco_{sa,beanie,redis,memcached,kafka,nats,casbin}/modules.py`
       `INFRA`, `varco_ws/modules.py` `CONSUMERS`, `varco_core` `CoreModule` (Step 4). `RedisModule`/
       `MemcachedModule.start()`: `if is_resolvable(CacheBackend): await (await aget(CacheBackend)).start()`
       (the §Context "nobody starts the cache" row) — idempotent against `RedisCache.start()`'s own
       guard (`cache.py:191`; Step 1 confirms). `varco_fastapi/varco_fastapi/app.py`: the one-line
       change of §D-runner; delete the "list `MigrationsModule` first" WARNING, its helper and its
       test in `test_create_varco_app_modules.py`; add to that test: two spy modules `Migrations(INFRA)`
       listed **after** `Core` still start before `core.bus` and stop after it; `SecurityPostureModule`
       is the last component to start and the first to stop; `container.ashutdown()` is awaited
       after the last `stop` (the existing assertion, kept). **Verify**: `uv run pytest
       varco_fastapi/tests/test_create_varco_app_modules.py varco_fastapi/tests/test_middleware_order.py
       varco_fastapi/tests/test_lifecycle_modules.py -q`.

6. [ ] **§D-bus** — `varco_kafka/tests/test_kafka_bus_start_is_explicit.py` +
       `varco_nats/tests/test_nats_bus_start_is_explicit.py` (**new, failing first**, Docker-free):
       `container.get(AbstractEventBus)` after `bootstrap()` returns an object whose
       `_started`/`is_running` (whatever the class exposes — Step 1(d)) is `False`; `publish()` on it
       raises `RuntimeError` mentioning `start()` (add the guard if Step 1(d) found a hang);
       `@PostConstruct` marker absent from `start()` (`providify.metadata`'s own reader, the 049
       §D-scan test's import); `@PreDestroy` still on `stop()`; `stop()` twice is a no-op. Then
       remove the two markers (`varco_kafka/bus.py:220`, `varco_nats/bus.py:232`), update both
       class docstrings ("started by the `BUS` phase — `CoreModule` — or by an explicit
       `await bus.start()`"). The existing `-m integration` bus suites already call `start()`
       explicitly (⚠️ Step 1 greps `await bus.start()` in both `tests/`; any test that relied on
       construction-time start gets the one line). **Verify**: `make test PKG=varco_kafka`,
       `PKG=varco_nats`; `uv run pytest varco_kafka/tests -m integration -q` on a Docker host.

### Disposers, the shim, the asks

7. [ ] **§D-disposes rows 10-15** — tests first: `varco_core/tests/test_observability_disposes.py`
       (install `OtelConfiguration`, resolve `TracerProvider` with an `InMemorySpanExporter` +
       `SimpleSpanProcessor` swapped in via the config, emit a span, `await container.ashutdown()`
       → the provider's `shutdown()` ran — assert via a spy processor's `shutdown` flag; `validate()`
       has no `UNMATCHED_DISPOSER`); `varco_redis/tests/test_redis_stores_disposes.py` (install
       `RedisStoresConfiguration` with `VARCO_*_BACKEND=redis` for the three seams, `monkeypatch`
       `aioredis.from_url` to a fake with an `aclose` counter; resolve all three; `ashutdown()` →
       three `aclose` calls; `RedisIdempotencyStore.aclose()`/`RedisTokenRevocationStore.aclose()`
       twice → one close each); `varco_sa/tests/test_sa_idempotency_disposes.py` (sqlite
       `SAConfig`; `VARCO_IDEMPOTENCY_BACKEND=sa`; resolve; `ashutdown()` → `store.stop()` ran;
       the injected engine is **not** disposed — a query on it after shutdown still works).
       Then the six `@Disposes` methods + two `aclose()` methods + the `_owns_engine` fix if
       Step 1(c) needs it. Each `@Disposes` carries the `varco_redis/dlq.py:790-804` DESIGN block
       shape (why `@Disposes` not `@PreDestroy`, the dispatch-key note). **Verify**: the three
       tests; `rg -n '@Disposes' varco_*/varco_*` → 14 sites.

8. [ ] **P-DISP + P-INJ** — `varco_core/tests/test_providify_contracts.py`: two `strict=True` xfails
       (`test_provide_accepts_disposer_kwarg` → `TypeError` today; `test_provider_accepts_inject_override`
       → `TypeError` today) with `reason="GAP: P-DISP — …"`/`"GAP: P-INJ — …"`; the two report files
       per §D-asks; two `UPSTREAM-GAPS.md` rows (recreate from its header template if the ledger
       was cleared — CLAUDE.md's rule). `varco_core/varco_core/di/bind.py` (or wherever 046 put
       `bind_instance` — Step 1): `disposer: Callable[[Any], Any | Awaitable[Any]] | None = None`
       kwarg; `_attach_disposer(binding, disposer)` — one function, docstring "SHIM for P-DISP:
       delete when `provide(disposer=)` ships; assigns the public `ProviderBinding.disposer`
       attribute exactly as `providify/container.py:7379` does". `test_di_wiring_primitives.py`
       (046's) gains: `bind_instance(c, I, obj, disposer=spy)` → `ashutdown()` calls `spy(obj)` once;
       async disposer awaited. `varco_redis/varco_redis/di.py::enable_redis_token_revocation` passes
       `disposer=store.aclose`. **Verify**: `uv run pytest varco_core/tests/test_providify_contracts.py
       varco_core/tests/test_di_wiring_primitives.py -q` (two `xfail` lines, no `XPASS`).

9. [ ] **`di_wiring` suite (§D-suite)** — `testkit/varco_conformance/di_wiring.py`: `module_owned`,
       the three new tests, the explicit message on `test_every_provider_singleton_has_a_reachable_disposer`;
       `known_warnings` entries naming 050 **deleted** in all ten subclasses (they must now be
       empty for the three disposer kinds — a residue means a row missed in Step 7); `module_owned`
       set in `varco_ws` (`{WebSocketEventBus, SSEEventBus}`) and `varco_core` (`{ReloadingTrustStore}`
       once Step 12 lands — added there). `COVERAGE.md` column. **Verify**: `for p in kafka nats
       redis memcached ws casbin sa beanie fastapi; do uv run pytest varco_$p/tests/test_${p}_di_wiring.py -q; done;
       uv run pytest varco_core/tests/test_di_wiring_conformance.py -q`; `rg -n '050' varco_*/tests/test_*_di_wiring*.py`
       → 0.

### DI-10 wiring

10. [ ] **`OutboxRelay` + `AuditConsumer`** — `varco_fastapi/tests/test_reliability_module_wiring.py`
        (**new, failing first**): `ReliabilityModule(ReliabilityPreset(outbox=True))` on a container
        with an `InMemoryOutboxRepository` and `bind_cloudevents_serializer(...)` → after `start()`
        `module.relay._serializer is container.get(Serializer[Event])` (a `CloudEventsJsonSerializer`);
        `ReliabilityPreset(outbox=True, serializer=JsonEventSerializer())` wins over the bound one;
        `ReliabilityPreset(audit=True)` → after `stop()` the `AuditConsumer` is no longer registered
        to the bus (`is_registered_to(bus) is False` — 051's public read) and `bus.subscriptions`
        count returns to the pre-start value; the negative-shape test of §D-wire. Then
        `varco_fastapi/varco_fastapi/reliability.py`: `build_outbox_relay(container, preset)`,
        `ReliabilityPreset.serializer` (additive field, `varco_core/reliability/preset.py`),
        `shutdown()` stops the audit consumer before the relay (reverse of startup), `phase` per
        Step 5. Docstring `:80-96` region rewritten. **Verify**: the test; `uv run pytest
        varco_fastapi/tests/test_reliability*.py -q`.

11. [ ] **`WebhookModule` + repositories (§D-webhook, §D-wire rows 4-5)** — `varco_core/tests/test_webhook_module.py`
        (**new, failing first**): `[CoreModule(), WebhookModule()]` on a bare container → registers
        the `@Fallback` in-memory repository, `container.get(WebhookDispatcher)` resolves with
        `_repository` the in-memory one and `_settings is container.get(WebhookSettings)`; with
        `bind_instance(container, AbstractDeadLetterQueue, dlq)` → `dispatcher._dlq is dlq`;
        `register_to(bus, dlq=other)` overrides; `WebhookModule(repository=spy_repo)` → `bind_instance`d,
        and `phased_components` shows `webhook.repository` at `INFRA` and `webhook.dispatcher` at
        `CONSUMERS`; after `LifecycleRunner` runs `[core, webhook]`, `spy_repo.start` ran before
        `core.bus` started and `spy_repo.stop` after `core.bus` stopped; the dispatcher
        `is_registered_to(bus)` after start (051's auto path) and not after stop; a scan of
        `varco_core` still registers no `WebhookDispatcher`/`WebhookSubscriptionRepository`
        (`test_modules_never_scanned.py` gains the two names). `varco_sa/tests/test_sa_webhook_repository_di.py`
        + `varco_beanie/tests/test_beanie_webhook_repository_di.py` (**new**, Docker-free):
        `VARCO_WEBHOOK_BACKEND=sa` + sqlite `SAConfig` + **no** `FieldEncryptor` binding →
        `validate()` reports `MISSING_BINDING` naming `FieldEncryptor` on `webhook_repository`;
        with `bind_instance(container, FieldEncryptor, FernetFieldEncryptor(key))` → resolves,
        `repo._encryptor is` that instance, `repo._engine is config.engine`; the hand path
        `SAWebhookSubscriptionRepository(url=…, encryptor=None)` is **not** asserted here (056).
        Then: `webhook/module.py`; `WebhookDispatcher(dlq=)` additive kwarg (`dispatcher.py:186-216`);
        `SAConfiguration.webhook_repository` / `BeanieConfiguration.webhook_repository` gated
        providers + the `engine=`/`client=` additive constructor kwargs (`varco_sa/webhook.py:101-111`,
        `varco_beanie/webhook.py:104-113`); 047's seam registration for `webhook_repository`
        (Step 1(e)) and its catalogue snapshot regenerated. **Verify**: the three tests; `make
        test PKG=varco_core`.

12. [ ] **`TrustStoreModule` (§D-trust)** — `varco_core/tests/test_trust_store_module.py` (**new,
        failing first**): `TrustStoreModule(spec=TrustStore(ca_folders=tmp_path))` + `CoreModule()`
        → `container.get(ReloadingTrustStore).spec is spec`, not started (`.context` raises
        `ResourceNotLoadedError`) until the runner runs `INFRA`; started after; stopped after exit;
        `TrustStoreModule(store=store)` → `container.get(ReloadingTrustStore) is store`;
        `test_tls_di.py` gains "constructing + registering the module starts no watcher". Then
        `varco_core/varco_core/tls/module.py`; `tls/di.py` module docstring gains the pointer ("the
        module is the lifecycle owner; `bind_trust_store` remains the bind-only verb"); `module_owned`
        in the core `di_wiring` subclass. **Verify**: the tests; `uv run pytest varco_core/tests/test_tls*.py -q`.

13. [ ] **The four adapters + `SAModule.start()` (§D-wire rows 7-8, §D-stores)** —
        `varco_ws/tests/test_ws_adapter_injection.py` (**new, failing first**): after
        `bind_websocket_adapter(container, ...)` on a container **without** an `AbstractEventBus`,
        `validate()` reports `MISSING_BINDING` for `bus` on the adapter's provider (today: clean
        `validate()`, `LookupError` at first `get()` — pinned as the *old* behaviour in the negative
        test's docstring); with `CoreModule().register(container)` → resolves and `adapter._bus is
        container.get(AbstractEventBus)`. Then `varco_ws/varco_ws/di.py`: module-level `from providify
        import Inject, Provider` + `from varco_core.event.base import AbstractEventBus, Event`;
        both factories declare `bus: Inject[AbstractEventBus]`; the `:253-258` DESIGN block
        rewritten to state the true rule (closure-local names are unresolvable; module-level ones
        are). `varco_fastapi/varco_fastapi/router/_di.py::_resolve_optional(container, cls)` (the
        P-INJ shim) replaces the four inline `container.get(_x_cls)` blocks in `mcp.py:1010-1022`
        and `skill.py:1102-1153`; `test_mcp_adapter_di.py`/`test_skill_adapter_di.py` (Step 1 finds
        the names) unchanged in assertions. `varco_sa/tests/test_sa_module_start.py` (**new**,
        sqlite): `SAModule().start()` with `VARCO_DLQ_BACKEND=sa` → `varco_dead_letters` exists
        (`inspect` via `run_sync`); `ensure_tables=False` → it does not; `SAIdempotencyStore.start()`
        was awaited (spy). Then `SAModule.start()` per §D-stores. **Verify**: the tests;
        `make test PKG=varco_ws PKG=varco_sa PKG=varco_fastapi` (three invocations).

### Integration, CloudEvents guards, gates, docs

14. [ ] **Integration (`-m integration`)** — `varco_sa/tests/test_sa_dlq_cloudevents_integration.py`
        (`postgres_url` fixture, per-test `uuid4().hex[:8]` schema/table prefix per the namespacing
        rule): real `SAConfig` engine, `VARCO_DLQ_BACKEND=sa`, `bind_cloudevents_serializer(...)`,
        `SAModule().start()` (ensure_table), resolve `AbstractDeadLetterQueue`, `push()` one dead
        letter, `SELECT payload` raw → a JSON object with `"specversion": "1.0"` (the CloudEvents
        envelope, not varco's plain JSON); `count()` → 1; `DlqRedriver` republishes onto an
        `InMemoryEventBus` with the same serializer and the handler receives the original `Event`.
        `varco_redis/tests/test_redis_lifecycle_order_integration.py` (`redis_url`): `[CoreModule()]`
        with `RedisModule()` bound + one spy `EventConsumer` `@Singleton`; run `LifecycleRunner`
        over `phased_components`; record `bus.stop`/`consumer.stop` order via wrappers → consumer
        first; then `await container.ashutdown()` → `@Disposes(AbstractEventBus)` runs `stop()` a
        second time with no error and no reconnect (the `redis` client is closed once — assert on
        the connection-pool `disconnect` count via a spy). **No chaos test** — nothing here is a
        container-failure scenario. **Verify**: `uv run pytest varco_sa/tests/test_sa_dlq_cloudevents_integration.py
        varco_redis/tests/test_redis_lifecycle_order_integration.py -m integration -q`.

15. [ ] **CloudEvents guards for every wired object (§D-wire's last paragraph)** —
        `varco_sa/tests/test_sa_cloudevents_di.py`, `varco_beanie/tests/test_beanie_cloudevents_di.py`,
        `varco_kafka/tests/test_kafka_cloudevents_di.py`, `varco_nats/tests/test_nats_cloudevents_di.py`,
        `varco_core/tests/test_core_cloudevents_di.py` (`OutboxRelay` via `ReliabilityModule`,
        the in-memory DLQ) — each in `test_redis_cloudevents_di.py`'s shape with the negative
        provider-copy test. Any provider found in Step 1(f) not declaring `serializer` fixed here.
        **Verify**: `uv run pytest varco_*/tests/test_*cloudevents_di.py -q`.

16. [ ] **Gates** — `uv run python scripts/api_surface.py` (new: `varco_core.di.{Phase,
        LifecycleComponent, LifecycleFailure, LifecycleRunner, AbstractLifecycle,
        resolve_lifecycle_order, phased_components, ModulePhaseError}`, `varco_core.webhook.WebhookModule`,
        `varco_core.tls.TrustStoreModule`, `bind_instance(disposer=)`, `WebhookDispatcher(dlq=)`,
        `ReliabilityPreset.serializer`, `SAModule(ensure_tables=)`, the two `aclose()`s, the two
        repository `engine=`/`client=` kwargs; `VarcoLifespan` unchanged) and the diff reviewed
        line by line; `uv run python scripts/import_budget.py --check --warn-only` (`varco_ws.di`'s
        two new module-level imports measured — `varco_ws/__init__` must not import `di` eagerly;
        Step 1 checks); 047's catalogue `--check`; 048's `settings_reference.py --check` (no new
        settings class — `WebhookSettings.enabled` only if Step 1(e) found it missing); `make lint`;
        `make type-check` (M1 delta recorded).

17. [ ] **Docs + changelog + backlog** (same commit — `feedback_keep_docs_updated`):
        - `technical_docs/features/dependency-injection.md` — **§Lifecycle phases** (new): the five
          phases with the owner table from §Context, `components()`, the six invariants as a
          numbered list each naming its test, `ModulePhaseError`, the `@Disposes` rule ("in the
          declaring `@Configuration`, one per produced type, idempotent teardown; module-owned
          objects are the exception and are listed in `module_owned`"), the ownership rule
          (§D-owner), `bind_instance(disposer=)`, the P-DISP/P-INJ pointers; a **Pitfalls** table:
          a `depends_on` pointing forward; a user module left at the default `CONSUMERS` phase that
          opens a store; `enable_*` constructing an object without `disposer=`; a Kafka/NATS bus
          resolved by hand and never started; `ensure_tables=True` under an Alembic-managed schema.
        - `CLAUDE.md` — (a) §CloudEvents envelope: the ⚠️ paragraph *"`SADeadLetterQueue` and
          `OutboxRelay` are hand-constructed, not DI-wired — pass `serializer=` explicitly…"* is
          **rewritten**: *"Every DLQ backend, `OutboxRelay` (via `ReliabilityModule`) and
          `WebhookDispatcher` (via `WebhookModule`) receive `Serializer[Event]` from the container —
          a `bind_cloudevents_serializer()` swap reaches all of them (`test_*_cloudevents_di.py`,
          one per package). The hand-constructed forms still take `serializer=` and still default
          to `JsonEventSerializer`; a DLQ backlog stored before a swap is never converted — drain it
          first."* (b) §Transactional Outbox rule: `OutboxRelay` "is the only place allowed to
          call `AbstractEventBus`" — unchanged; add "wired by `ReliabilityModule`, phase
          `CONSUMERS`". (c) §Outbound webhooks: `WebhookModule` sentence + "the SA/Beanie
          repositories inject `FieldEncryptor` (required) through their providers — the plaintext
          path exists only by hand". (d) §DI wiring: one new paragraph "Lifecycle phases (Plan
          050)": the rule (*a module declares `phase`; the runner starts `(phase, depends_on)`
          ascending and stops the exact reverse of what started; `container.ashutdown()` is last;
          every provider-produced singleton with a teardown method has a `@Disposes` in its own
          `@Configuration` or a module owner — `di_wiring` enforces both*), a pointer to the guide.
          (e) §Test conventions: "`di_wiring` … `known_warnings`" sentence updated (the three
          disposer kinds are closed). (f) Decision tree: under "A subsystem the app should switch
          on/off…" (049's branch) add *"→ declare its `phase`; spans phases? override
          `components()` (`CoreModule` is the example)"*; under Event wire format: *"a new DLQ
          backend's provider must declare `serializer` — `test_<pkg>_cloudevents_di.py`"*. (g)
          §Key Abstractions "TLS trust store": `TrustStoreModule` is the lifecycle owner; the
          "never add a scanned `@Configuration`" rule unchanged.
        - `README.md` — "Wiring an app" (049's): the phase line + `WebhookModule()`/`TrustStoreModule(...)`
          in the module-list idiom; "Transactional Outbox": `ReliabilityModule(ReliabilityPreset(
          outbox=True))` replaces the hand `OutboxRelay(...)` snippet's lifecycle lines (the hand
          snippet stays as "by hand"); "Dead Letter Queue"/"CloudEvents envelope": the "pass
          `serializer=` explicitly" sentences → the DI sentence; "Outbound webhooks" `:4295-4316`:
          the `WebhookModule` form first, the hand form second; "TLS trust store": `TrustStoreModule`.
        - `technical_docs/features/cloudevents-envelope.md` Pitfalls: the SADLQ/Outbox row
          rewritten (still: backlog not converted); `outbound-webhooks.md` Pitfalls: "forgot
          `encryptor=`" row → "by hand only; the provider requires a `FieldEncryptor` binding";
          `reliability-preset.md`: `serializer` field; `tls-trust-and-hot-reload.md`: the module;
          `technical_docs/common-pitfalls.md`: one row — "`@PostConstruct` on a `start()` couples
          construction to lifecycle; a bus started at first resolution starts before migrations —
          removed from Kafka/NATS in 4.0 (Plan 050); never add it back".
        - `ARCHITECTURE.md` — the 049 Modules table gains a `phase` column; a "Lifecycle" entry
          under DI listing `LifecycleRunner`/`Phase`.
        - `CHANGELOG.md` `[Unreleased]` — `### BREAKING — Phased lifecycle, @Disposes reachability,
          DI-wired infrastructure (Plan 050, DI-9 / DI-10)` = §Migration; `### Fixed` — the
          `lifespan.py:253` forward-order rollback; `AuditConsumer` never stopped; OTel providers
          never flushed at shutdown; `RedisIdempotencyStore`/`RedisTokenRevocationStore` client
          leak. `BACKLOG.md:96,97` → `done (Plan 050)`; the §Non-goals 051 follow-up ("auto-registered
          consumers get no `dlq=`") added as an `RO-*` row. **Handoffs**: **052** — `LifecycleRunner.plan`
          / `last_failures` and `ModuleRegistry` are the "Modules · phase · started" section;
          `MISSING_BINDING` on `webhook_repository`'s `encryptor` is the finding to render first.
          **056** — the DI path never passes `encryptor=None`; the refusal is constructor-only.
          **058** — §Migration below.

## Edge cases

- **Two modules in one phase, no edge between them** → list order (stable sort), as 049.
- **`depends_on` to a module in the same phase** → 049's DFS order within the phase.
- **`depends_on` to a module in an earlier phase** → satisfied by the phase; no-op.
- **`depends_on` to a module in a later phase** → `ModulePhaseError` at `create_varco_app()`; no
  `register()` has run.
- **A user module keeps the default `phase` (`CONSUMERS`) and opens a store in `start()`** → works
  (the bus is up); documented as the pitfall "declare `INFRA` if others depend on it".
- **`components()` returns `()`** → the module has no lifecycle; nothing in the plan; allowed
  (middleware-only modules could do this instead of a dummy phase — they keep the default component
  so 052's "Modules" section lists every module uniformly).
- **`start()` of the `EDGE` posture component raises (`enforce="refuse"`)** → every earlier
  component, in every module, stopped in exact reverse; the `RuntimeError` propagates; 049's
  FLIP-1 test shape holds.
- **A component's `stop()` hangs** → shutdown hangs (no timeout — §Parked). `AbstractJobRunner.stop(timeout=)`
  is the only bounded one, as today.
- **`RedisModule(cache=True)` and `async_init()` of a later module fails** → the cache provider is
  installed and possibly resolved; no `start()` ran; `ashutdown()` runs `@Disposes(CacheBackend)` →
  `stop()` on a never-started cache is a no-op (`cache.py:214` guard; asserted in Step 7's test).
- **Kafka bus resolved by `_bus_event_producer` during `INFRA`** → constructed, not started (§D-bus);
  a service that publishes during `INFRA` gets `RuntimeError` — correct: nothing may publish before
  `BUS`; the outbox pattern is the answer, as CLAUDE.md already says.
- **`WebhookModule()` with a durable backend selected and no `FieldEncryptor`** → `validate()`
  `MISSING_BINDING`; without 052, the first resolution raises `LookupError` naming `FieldEncryptor`
  — never a plaintext repository.
- **`WebhookModule(repository=repo)` where `repo` has no `start()`** (in-memory) → the `INFRA`
  component is a no-op; still listed in the plan.
- **`bind_instance(..., disposer=)` twice for one object under two qualifiers** → two bindings,
  two disposer calls — the caller asked for it; documented.
- **`enable_redis_token_revocation()` then `ashutdown()` without a lifespan** → `aclose()` via the
  shim; before this plan the client leaked.
- **A hand-wired `VarcoLifespan(bus, consumer)`** (no modules) → registration order in, exact
  reverse out, the `:253` fix applies; byte-identical otherwise.
- **`SAModule(ensure_tables=True)` under `VARCO_MIGRATE_MODE=check`** → `checkfirst` DDL only for
  tables Alembic has not created; the pitfall row says to set `False` when Alembic owns them.

## Verification

```bash
# Runner + phases (Docker-free, FastAPI-free)
uv run pytest varco_core/tests/test_lifecycle_runner.py varco_core/tests/test_di_module.py -q
uv run pytest varco_fastapi/tests/test_create_varco_app_modules.py varco_fastapi/tests/test_lifespan_shutdown.py \
              varco_fastapi/tests/test_lifespan_shutdown_characterization.py varco_fastapi/tests/test_middleware_order.py -q

# Bus start is explicit
uv run pytest varco_kafka/tests/test_kafka_bus_start_is_explicit.py varco_nats/tests/test_nats_bus_start_is_explicit.py -q
rg -n '@PostConstruct' varco_kafka/varco_kafka/bus.py varco_nats/varco_nats/bus.py     # 0

# Disposers + suite
uv run pytest varco_core/tests/test_observability_disposes.py varco_redis/tests/test_redis_stores_disposes.py varco_sa/tests/test_sa_idempotency_disposes.py -q
for p in kafka nats redis memcached ws casbin sa beanie fastapi; do uv run pytest varco_$p/tests/test_${p}_di_wiring.py -q; done
uv run pytest varco_core/tests/test_di_wiring_conformance.py -q
rg -n '050' varco_*/tests/test_*_di_wiring*.py                                       # 0
rg -n '@Disposes' varco_*/varco_* | wc -l                                             # 14

# Asks
uv run pytest varco_core/tests/test_providify_contracts.py -q                         # 2 xfail, 0 xpass
ls design/upstream-gaps/providify-provide-disposer.md design/upstream-gaps/providify-provider-inject-override.md

# DI-10 wiring + CloudEvents guards
uv run pytest varco_fastapi/tests/test_reliability_module_wiring.py varco_core/tests/test_webhook_module.py \
              varco_core/tests/test_trust_store_module.py varco_ws/tests/test_ws_adapter_injection.py \
              varco_sa/tests/test_sa_module_start.py varco_sa/tests/test_sa_webhook_repository_di.py \
              varco_beanie/tests/test_beanie_webhook_repository_di.py -q
uv run pytest varco_*/tests/test_*cloudevents_di.py -q
rg -n 'pass .serializer=. explicitly|hand-constructed, not DI-wired' CLAUDE.md README.md technical_docs   # 0

# Integration (Docker)
uv run pytest varco_sa/tests/test_sa_dlq_cloudevents_integration.py varco_redis/tests/test_redis_lifecycle_order_integration.py -m integration -q

# Gates
uv run python scripts/api_surface.py --check
uv run python scripts/import_budget.py --check --warn-only
make lint && make type-check && make test && make docs
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — 049 landed `CoreModule.start()`/`stop()` in `varco_core/di/module.py` and 051 put the consumer loop there; `register` is `@final` | Step 4 moves the right bodies into `components()` | Step 1(b) |
| ⚠️ ASSUMPTION — 047's `SAIdempotencyStore(engine=)` `stop()` does not dispose a borrowed engine | A store never disposes the app's pool | Step 1(c); `_owns_engine` added here if absent |
| ⚠️ ASSUMPTION — `publish()` on an unstarted Kafka/NATS bus raises, not hangs | "Nothing publishes before `BUS`" fails loudly | Step 1(d); guard added in Step 6 otherwise |
| ⚠️ ASSUMPTION — `WebhookSettings.enabled` exists (047/048) and 047's seam registration accepts a new `webhook_repository` seam | `WebhookModule` tri-state and `VARCO_WEBHOOK_BACKEND` follow the shipped convention | Step 1(e); fallback: enabled-iff-constructed, `@Requires(env=)` used directly |
| ⚠️ ASSUMPTION — every DLQ provider declares `serializer` after 030/047 | A CloudEvents swap reaches every DLQ | Step 1(f); fixed in Step 15 |
| ⚠️ ASSUMPTION — `varco_ws/__init__.py` does not import `di` eagerly, so two module-level imports cost nothing at `import varco_ws` | Import budget unchanged | Step 16 measures; else the imports go behind a lazy `__getattr__` |
| ⚠️ ASSUMPTION — 046's `bind_instance` lives at a file this plan can extend with a kwarg without touching `__all__`'s signature snapshot | `api_surface --check` diff is the expected additive one | Step 16 review |
| Removing `@PostConstruct` from the Kafka/NATS bus breaks a hand-wired app | 4.0 window; one line (`await bus.start()`) | §Migration row; `test_*_bus_start_is_explicit.py` names the error |
| Disposal order (reverse creation) and module stop order both touch one object | `stop()`/`aclose()` idempotent for every wired object | `test_double_stop_is_noop` per object (Steps 6, 7, 14) |
| `add_note` text on a propagated start exception changes a test that asserts `str(exc)` | `str()` is unchanged — notes render only in tracebacks | Step 2 asserts `str(exc)` equality |
| `SAModule.start()` DDL surprises an Alembic-managed deployment | `checkfirst` only; `ensure_tables=False` documented | Pitfall row; `SAModule` docstring |
| The varco-side "has a teardown method → needs a disposer or owner" check false-positives on a value object with a `close()` | `module_owned` is the explicit escape, with a comment | The check reads only `stop`/`aclose`/`close`/`shutdown` attributes on the produced *type* |
| A future `known_warnings` row silently re-opens the disposer debt | The three kinds are never allowlisted again | The explicit test message + the CLAUDE.md sentence |
| M1 (`type: ignore` count) moves | Directional only | Step 16 records the delta |

## Migration (3.x → 4.0, DI-9 / DI-10 entry — Plan 058 assembles)

**BREAKING — lifecycle is phased; Kafka/NATS buses no longer start at construction.**

| Old shape | New |
|---|---|
| `KafkaEventBus`/`NatsEventBus` started when first resolved (`@PostConstruct`) | started by the `BUS` phase (`CoreModule`) — or `await bus.start()` in a hand-wired app; `publish()` before `start()` raises `RuntimeError` |
| `VarcoLifespan` rollback after a failed `start()` stopped components in **forward** order | exact reverse (bug fix) |
| a `VarcoModule` with no `phase` | defaults to `Phase.CONSUMERS`; in-tree modules all declare one; `depends_on` may not point at a later phase (`ModulePhaseError`) |
| `create_varco_app` WARNING "list `MigrationsModule` first" | gone — `MigrationsModule` is `INFRA` and starts before `CoreModule`'s bus wherever it is listed |
| `OutboxRelay(outbox=…, bus=…)` inside `ReliabilityLifecycle` with the default serializer | `ReliabilityModule` injects the container's `Serializer[Event]`; `ReliabilityPreset(serializer=…)` overrides |
| `ReliabilityLifecycle.shutdown()` left the `AuditConsumer` subscribed | stopped, before the relay |
| `dispatcher = WebhookDispatcher(repository=repo); dispatcher.register_to(bus, dlq=dlq)` | `modules=[…, WebhookModule()]` (in-memory fallback repository) or `WebhookModule(repository=repo)`; `VARCO_WEBHOOK_BACKEND=sa\|beanie` selects a durable one, which **requires** a `FieldEncryptor` binding; the hand form still works |
| `SAWebhookSubscriptionRepository(url=…, encryptor=…)` by hand | still works; DI path: `SAConfiguration.webhook_repository` (engine from `SAConfig`, encryptor injected) |
| `bind_trust_store(container, store)` + `lifespan.register(store)` | `TrustStoreModule(store=store)` or `TrustStoreModule(spec=…)`; `bind_trust_store` unchanged as bind-only |
| `bind_websocket_adapter`/`bind_sse_adapter` resolved the bus lazily inside the factory | declared injection — a missing `AbstractEventBus` is a `validate()` `MISSING_BINDING`, not a `LookupError` at first request |
| `enable_redis_token_revocation()` leaked its client at shutdown | closed by `ashutdown()` (`bind_instance(disposer=)`) |
| `OtelConfiguration` providers never flushed | `@Disposes` → `TracerProvider.shutdown()`/`MeterProvider.shutdown()` at `ashutdown()` |
| `SADeadLetterQueue`: "call `ensure_table()` at startup" | `SAModule(ensure_tables=True)` does it in `INFRA`; `False` when Alembic owns the tables |
| `di_wiring` subclasses with `known_warnings` naming Plan 050 | entries removed; a new disposer warning is a bug to fix in the declaring `@Configuration` |

**New public names**: `varco_core.di.{Phase, LifecycleComponent, LifecycleFailure, LifecycleRunner,
AbstractLifecycle, resolve_lifecycle_order, phased_components, ModulePhaseError}`;
`VarcoModule.phase`, `VarcoModule.components()`; `varco_core.webhook.WebhookModule`;
`varco_core.tls.TrustStoreModule`; `bind_instance(..., disposer=)`; `WebhookDispatcher(dlq=)`;
`ReliabilityPreset.serializer`; `SAModule(ensure_tables=)`; `RedisIdempotencyStore.aclose()`,
`RedisTokenRevocationStore.aclose()`; `SAWebhookSubscriptionRepository(engine=)`,
`BeanieWebhookSubscriptionRepository(client=)`; `VarcoLifespan.last_runner`.

## Docs / backlog updates

- `technical_docs/features/dependency-injection.md` §Lifecycle phases; `CLAUDE.md` (a)-(g);
  `README.md` five sections; `cloudevents-envelope.md`, `outbound-webhooks.md`,
  `reliability-preset.md`, `tls-trust-and-hot-reload.md`, `common-pitfalls.md`; `ARCHITECTURE.md` —
  Step 17.
- `CHANGELOG.md`, `BACKLOG.md:96,97` + one `RO-*` row — Step 17.
- `design/upstream-gaps/providify-provide-disposer.md`, `providify-provider-inject-override.md`,
  `UPSTREAM-GAPS.md` two rows — Step 8.
- `design/api-freeze-and-standards/measurements/api-surface.*` — Step 16; 047's catalogue — Step 11;
  `testkit/varco_conformance/COVERAGE.md` — Step 9.

## Parked

- **Per-phase stop timeout** (Spring `timeoutPerShutdownPhase`, brief 014 `:145`). Trigger: a
  recorded production hang in a `stop()` that was not `AbstractJobRunner`'s. Shape when it lands:
  one `VARCO_LIFECYCLE_STOP_TIMEOUT_SECONDS` on a 048 `VarcoSettings`, applied per component by
  `LifecycleRunner`, a timed-out stop recorded as a `LifecycleFailure(TimeoutError)`.
- **`register_to(bus, dlq=<app DLQ>)` for every auto-registered consumer** — 051's seam; filed as
  an `RO-*` row (Step 17).
- **Dropping the P-DISP/P-INJ shims** — when providify ships `provide(disposer=)`/`inject=`; the
  strict xfails flip and name the two functions to delete.

## Done criteria

- [ ] `LifecycleRunner` exists in `varco_core.di.lifecycle`; the six invariants (start order,
      exact-reverse stop, reverse rollback with the original exception, stop-failure aggregation
      without raising, `shutdown` last and contained, `plan`/`last_failures` reads) are green
      without Docker or FastAPI; `VarcoLifespan` delegates and `test_lifespan_shutdown*.py` are
      unchanged and green.
- [ ] Every in-tree module declares `phase`; `resolve_lifecycle_order` raises `ModulePhaseError`
      on a forward edge; 049's "list first" WARNING is gone; `SecurityPostureModule` starts last.
- [ ] `@PostConstruct` is off both bus `start()`s; `publish()` before `start()` raises.
- [ ] 14 `@Disposes` sites; `known_warnings` has no 050 entry in any package; the varco-side
      "teardown method ⇒ disposer or owner" test passes for all ten.
- [ ] P-DISP and P-INJ filed (report file + ledger row + strict xfail each).
- [ ] `OutboxRelay`, `WebhookDispatcher`, the two webhook repositories, `ReloadingTrustStore`, and
      the two ws adapters receive their dependencies by declaration; `test_*_cloudevents_di.py`
      exists for `sa`, `beanie`, `kafka`, `nats`, `core`; the CLAUDE.md ⚠️ paragraph is rewritten.
- [ ] Two `-m integration` tests green on a Docker host; no chaos test added.
- [ ] `api_surface`, `import_budget --warn-only`, 047 catalogue, 048 settings snapshots green;
      `make lint`, `make type-check`, `make test`, `make docs` green on 3.12/3.13; no file under
      `/home/edoardo/projects/providify` modified.
