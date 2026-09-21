# Plan 045 — providify 2.1.0 adoption and `@Fallback` framework defaults (DI-15 close-out, DI-3)

Covers BACKLOG 4.0 rows **`DI-15`** (`BACKLOG.md:67` — the close-out half: the four filed asks
P24/P25/P26/P27 have shipped in providify 2.1.0 and must be *adopted*, their strict xfail guards
converted, their ledger rows closed) and **`DI-3`** (`BACKLOG.md:76` — *"One default-binding
contract — every framework default … declared under one documented rule: a user binding always
wins, regardless of registration order. Target mechanism: the `@DefaultBean`-style fallback
binding asked for in `DI-15` … `setup_varco_defaults()` disappears into it"*). The interim
`FRAMEWORK_DEFAULT_PRIORITY` named floor that `DI-3` allowed "until providify ships it" is **not
built** — providify shipped it, so this plan goes straight to `@Fallback`.

This is the **first plan of the 4.0 DI track** (`plans/000-index-4-0-di-standardization.md`;
slice map row 045). Every later plan assumes the pin bump and the `@Fallback` declarations have
landed: 046 (verb collapse, `DIContainer.current()` removal, `di_wiring` conformance suite), 047
(backend selection + catalogue, which reads the `@Fallback` declarations), 050 (`@Disposes`
reachability — the P24 fix), 051 (multi-binding — relies on `get_all()` excluding a shadowed
fallback, verified in §Context), 052 (`varco diagnose` — renders `FALLBACK_SHADOWED`), 053
(FLIP-5 layers "refuse outside development" on top of the `@Fallback` `BaseAuthorizer`), 058
(completes the DI guide this plan starts).

**Research briefs backing this plan:**
[`010-default-implementation-di-conventions.md`](../design/research/010-default-implementation-di-conventions.md)
(§1 Quarkus `@DefaultBean` intent-over-order vs Spring order coupling; §4 "what implementation am
I running" diagnostics; §7 order of initialization; §"Options Compared" row 2 *conditional
decorator — user bean always wins, checked at resolution time*) and
[`017-fallback-binding-evaluation-timing-and-diagnostics.md`](../design/research/017-fallback-binding-evaluation-timing-and-diagnostics.md)
(§2 resolve-time evaluation; §3 shadowing diagnostics — the Spring `/actuator/conditions`
`negativeMatches` shape `FALLBACK_SHADOWED` mirrors; §4a two-fallback tie; §4b inactive shadower;
§4c qualifiers; Librarian's Note edge 4 — collections exclude a shadowed fallback).

**providify source of record**: `/home/edoardo/projects/providify`, `pyproject.toml:3`
`version = "2.1.0"`, HEAD **`3cb207f`** (`.git/refs/heads/main`), CHANGELOG `[2.1.0] — 2026-09-13`
(`CHANGELOG.md:12-129`). ⚠️ The varco venv still resolves **2.0.1** — every `providify/…` citation
below was read in the **2.1.0 source tree**, not the installed copy; Step 1 moves the pin and
re-confirms each cited line against the installed package afterwards.

> ⚠️ **Three scout/brief premises corrected from source, each reshaping a decision below.**
> (1) `BaseAuthorizer`'s default is **not** a `@Provider` in `VarcoFastAPIModule` (shared-context
> §2) — it is a scanned `@Singleton(priority=_FALLBACK_PRIORITY)` at
> `varco_core/varco_core/auth/authorizer.py:62-66`, with `_FALLBACK_PRIORITY = -(2**31)` — a
> **third** magic floor beside `-sys.maxsize - 1` and `-sys.maxsize` that the 71-hit `sys.maxsize`
> grep does not see (§D-tiers). (2) `FALLBACK_SHADOWED` is evaluated for the request
> `(b.interface, b.qualifier, None)` (`providify/container.py:6360-6370`), and a **scanned**
> `@Singleton` class is registered as `ClassBinding(cls, cls)` (`container.py:1248`) — so its
> "natural request" is the concrete class, never the ABC an app injects. The INFO is therefore
> emitted for `@Provider`/`bind(ABC, Impl)` defaults but **not** for scanned-class defaults
> shadowed at the ABC (§D-proof, §D-P28). (3) providify's CHANGELOG says the two new `WARNING`
> disposer kinds "can newly make `report.ok` `False`" (`CHANGELOG.md:28-29`); the source says
> `ok` is `not self.errors` (`providify/validation.py:279-287`) — warnings never touch `ok`. The
> source wins (U-8); this plan's validate-clean test asserts `errors`/`warnings` tuples
> directly and never reads `ok`.

## Goal

varco depends on `providify>=2.1.0`; every varco-owned binding of a varco seam is declared
`@Fallback` under one written rule — *a user binding always wins, regardless of registration
order* — with `container.validate()` producing zero `ERROR`/`WARNING` for a default varco
container and `FALLBACK_SHADOWED` `INFO` issues as the machine-readable proof; the three magic
priority floors (`-sys.maxsize - 1`, `-sys.maxsize`, `-(2**31)`) and `setup_varco_defaults()` are
gone; the four upstream-gap guards are passing contract tests; the four ledger rows are closed;
and `technical_docs/features/dependency-injection.md` exists with its default-binding section.

## Non-goals

- **Any change to providify** — the user owns it; 2.1.0 is consumed as shipped.
- **Verb collapse, `DIContainer.current()` removal, `bootstrap()` return contract** — Plan 046.
  `setup_event_producer()` and `bootstrap(setup_producer=)` are untouched here (046 decides
  their fate); only `setup_varco_defaults()` and `setup_defaults=` go, because they *are* the
  default-binding mechanism this plan replaces.
- **Backend selection** (`VARCO_<SEAM>_BACKEND`, `@Requires`, `_redis_active_bus` replacement,
  the two-backends-scanned tie) — Plan 047. This plan leaves `RedisEventBusSelectorConfiguration`
  in place and only re-declares its priority tier (§D-tiers).
- **Settings-provider standardization** (one `@Provider` recipe, fail-fast validation) — Plan 048.
  This plan touches settings providers only to mark them `@Fallback` and to remove the four
  duplicate copies that would otherwise tie (§D-dupes).
- **The `testkit/varco_conformance/di_wiring.py` suite** — Plan 046. The per-package tests this
  plan adds (§D-tests) are the seeds 046 generalises, deliberately kept as plain package tests.
- **`varco diagnose`** rendering of `FALLBACK_SHADOWED`/`CONDITION_INACTIVE` — Plan 052.
- **FLIP-5** (`BaseAuthorizer` refuses outside development) — Plan 053; this plan only converts
  its floor to `@Fallback`.
- **The nine `removed_in="4.0.0"` sites** — Plan 058 re-labels them; untouched here.
- **Open-generic adoption** (`bind_repositories` → one `returns=AsyncRepository[T]` provider) —
  the P26 feature is *verified* here (Step 5) but *used* by 046/051, not here.
- **A `varco_core` top-level re-export of the new `varco_core.di` module** — deliberately not
  added to the PEP 562 lazy map; `from varco_core.di import BACKEND_PRIORITY` is the import.

## Context

### What providify 2.1.0 ships (verified at `3cb207f`)

| Primitive | Where | Semantic this plan relies on |
|---|---|---|
| `@Fallback` | `providify/decorator/scope.py:976-1070`; exported `providify/__init__.py:23,159` | Bare marker, no arguments (`:1025-1028`). **Usable on classes and `@Provider` functions, including `@Configuration` bound methods and `@Provider @property` getters** (`:998-1001` — place it *beneath* `@property`). **Decorator order relative to `@Singleton`/`@Component`/`@Provider`/`@Profile`/`@Alternative` is irrelevant** (`:1001-1005`) — bindings read markers at `bind()`/`register()`/`provide()`/`scan()` time. Composes with `@Profile`/`@Alternative`/`@Requires` by AND (`:982-986`) |
| `FallbackMarker` / `_is_fallback` | `providify/metadata.py:201-226` / `:831-879` | Stored on the target's own `__dict__`; for a bound method falls back to `__func__.__dict__` (`:870-877`) — that is what makes a `@Configuration` method work. **Never walks the MRO** (`:858-861`): a subclass of a `@Fallback` class is *not* a fallback |
| `ClassBinding.fallback` / `ProviderBinding.fallback` | `providify/binding.py:207` / `:723` | Resolved **once at binding construction** — the marker must be stamped *before* `register()`/`provide()`/`scan()` sees the object (matters for the imperative `Singleton(...)(cls)` call sites, §D-fastapi) |
| `_filter()` post-step | `providify/container.py:2051-2170`; the rule `:2077-2085`; code `:2168-2169` | `if any(not b.fallback for b in candidates): candidates = [b for b in candidates if not b.fallback]` — runs **after** interface, `exact_only`, qualifier (`:2147`), priority (`:2148`) and activation (`:2150`) narrowing. Reaches `get`/`aget`/`get_all`/`aget_all`/`is_resolvable`/`get_binding`/`get_all_bindings`/`_collect_sync`/`_collect_async`/`validate()`'s `memo_filter` (`:2159-2162`) |
| Qualifier request-relativity | `container.py:2096-2101` (E3/E4) | An **unqualified** request matches qualified and unqualified candidates → an unqualified `@Fallback` yields to a *qualified* non-fallback sibling; a **qualified** request (`get(T, qualifier="in_memory")`) only sees that qualifier → a qualified fallback still wins its own qualified request even when shadowed for the unqualified one. This is exactly the `qualifier="in_memory"` escape hatch (§D-qualifier) |
| Priority narrowing | `container.py:2108-2112` (E14) | `get(T, priority=p)` narrows *before* the fallback drop — an explicit "give me the binding at that priority" request can return a fallback even when shadowed |
| Two fallbacks, no non-fallback | `scope.py:1042-1047` | Neither dropped → today's `max()` rule (`container.py:2853`), exact tie → first registered; `validate()` reports the tie as `AMBIGUOUS_BINDING` (`ERROR`, per injection point only — `container.py:6700-6712`) |
| Not evicted | `scope.py:1048-1053` | A fallback singleton cached *before* its shadower is registered stays cached — same caveat as `activate_profile()`; varco's bootstrap order (scan/bootstrap → app bindings → first `get()`) never hits it |
| `get_all()`/`aget_all()` exclusion | `container.py:1683-1686` / `:1883` | "Shadowed `@Fallback` bindings are excluded … never appears in the returned list". **Plan 051 relies on this**; Step 5 pins it in a varco test |
| `get_binding()` | `container.py:7422-7462` | Pure read, no instantiation, honours the fallback rule via `_get_best_candidate()` (`:2814-2853`) — the tool for every "who wins" test that must not open a socket (§D-tests) |
| `register(cls)` | `container.py:1228-1248` | `ClassBinding(cls, cls)` — a scanned `@Singleton` class's `interface` **is the class**; it serves `get(ABC)` through `_binding_serves`' `issubclass` match (`:2138`), not through an ABC-keyed binding |
| `Severity.INFO`, `ValidationReport.infos`, `ok` | `providify/validation.py:41-60`, `:274-277`, `:279-287` | `INFO` never raises, never affects `ok`; `ok` is `not self.errors` (warnings do not affect it either — contra `CHANGELOG.md:28-29`) |
| `IssueKind.FALLBACK_SHADOWED` | `validation.py:115-119`; emitter `container.py:6349-6394` (Pass 1d, `:6519-6524`) | One `INFO` per **active** fallback whose natural request `(b.interface, b.qualifier, None)` is won by a non-fallback; `shadowed_by` names the winner's owner (`:6393`); message ends "Expected for a default — no action needed" (`:6386-6389`) |
| `IssueKind.CONDITION_INACTIVE`, `@Requires` | `validation.py:110-114`; `scope.py:818-823` | Shipped; consumed by Plan 047. Verified here only so the P25 guard converts to a passing test |
| Open-generic provider bindings | `CHANGELOG.md:69-98` (eight rules on `DIContainer.provide()`); `binding.py:713` `type_params` | `provide(factory, returns=Repo[T])` serves `get(Repo[User])`; `type[T]` delivered by TypeVar name; singleton cached per closed alias; **bare `get(Repo)` against an open binding now raises `LookupError`** (`CHANGELOG.md:119-125`). Verified here (P26 conversion); used by 046/051 |
| `@Disposes` fix (P24) | `container.py:7260-7369`; `own` list `:7321`, scoped loop `:7359-7360` | Wiring scoped to the bindings *this* `install()`/`ainstall()` registered; `IssueKind.UNMATCHED_DISPOSER` (`validation.py:106-109`) and `DISPOSER_OVERWRITTEN` (`:102-105`), both `WARNING` |
| `provide()` returns the binding | `container.py:1250` | `-> ProviderBinding` (was `None`) — used by the P24 conversion's assertions |
| `MISSING_BINDING_DEFAULTED` skipped for optional params | `container.py:6653-6660` | `InjectMeta(optional=True)` / `T \| None` with no binding is **silent** — load-bearing for §D-dupes |

### Every varco priority-floor site, classified (measured 2026-09-14)

`rg -n 'sys\.maxsize' varco_*/varco_*` → **71 hits**; plus `_FALLBACK_PRIORITY` (`-(2**31)`,
`varco_core/varco_core/auth/authorizer.py:62,65`, 8 mentions) and the **priority-0 "provide
before install() — order matters" folk-rule sites** (`rg -n 'before install|before scan|first-registered|resolve to the first registered' varco_*/varco_* README.md ARCHITECTURE.md technical_docs`)
which `DI-3` names as the undocumented rule. Five classes; the new decoration column is what
Step 6-9 write. `BACKEND_PRIORITY` is the one constant §D-tiers introduces.

**Class A — in-process default (Null/InMemory/Json/env-settings). → `@Fallback`, priority omitted (providify default 0), qualifier kept.**

| # | Site | Binding (interface) | Today | New |
|---|---|---|---|---|
| A1 | `varco_core/varco_core/cache/memory.py:145` | `InMemoryCache` (`CacheBackend`) | `@Singleton(priority=-sys.maxsize - 1, qualifier="in_memory")` | `@Fallback @Singleton(qualifier="in_memory")` |
| A2 | `varco_core/varco_core/event/memory.py:87` | `InMemoryEventBus` (`AbstractEventBus`) | same | `@Fallback @Singleton(qualifier="in_memory")` |
| A3 | `varco_core/varco_core/event/dlq.py:524` | `InMemoryDeadLetterQueue` | same | same shape |
| A4 | `varco_core/varco_core/event/deduplication.py:157` | `InMemoryDeduplicator` | same | same shape |
| A5 | `varco_core/varco_core/lock.py:373` | `InMemoryLock` (`AbstractDistributedLock`) | same | same shape |
| A6 | `varco_core/varco_core/service/conversation.py:174` | `InMemoryConversationStore` | `@Singleton(priority=-sys.maxsize, qualifier="in_memory")` — the only core default at the *second* tier, an inconsistency | `@Fallback @Singleton(qualifier="in_memory")` |
| A7 | `varco_core/varco_core/flags/null.py:27` | `NullFeatureFlags` | `@Singleton(priority=-sys.maxsize - 1)` | `@Fallback @Singleton` |
| A8 | `varco_core/varco_core/revocation/null.py:35` | `NullTokenRevocationStore` | same | `@Fallback @Singleton` |
| A9 | `varco_core/varco_core/tenancy/membership.py:237` | `NullTenantMembership` | same | `@Fallback @Singleton` |
| A10 | `varco_core/varco_core/event/producer.py:146` | `BusEventProducer` (`AbstractEventProducer`) | same | `@Fallback @Singleton` |
| A11 | `varco_core/varco_core/event/serializer.py:116` | `JsonEventSerializer` (`Serializer[Event]`) | same | `@Fallback @Singleton` |
| A12 | `varco_core/varco_core/auth/authorizer.py:62-66` | `BaseAuthorizer` (`AbstractAuthorizer`) | `@Singleton(priority=_FALLBACK_PRIORITY)`, `-(2**31)` | `@Fallback @Singleton`; **delete `_FALLBACK_PRIORITY`**; rewrite module docstring `:8-45` |
| A13 | `varco_core/varco_core/observability/di.py:185` | `OtelConfiguration.otel_config` (`OtelConfig`) | `@Provider(singleton=True)` at 0 + "must be registered *before* `install()`" (`:190-208`, README `:3734-3735`) — the folk rule itself | `@Fallback @Provider(singleton=True)`; docstring: "register yours in any order" |
| A14 | `varco_fastapi/varco_fastapi/job/store.py:39` | `InMemoryJobStore` (`AbstractJobStore`) | `@Singleton(priority=-sys.maxsize - 1)` | `@Fallback @Singleton` |
| A15 | `varco_fastapi/varco_fastapi/di.py:280,290,301,311,334,350,371,384,395,400,410` | 11 singleton providers in `VarcoFastAPIModule` (`TrustStore`, `CORSConfig`, `ClientProfile`, `ProfilingSettings`, `JwtTransformSettings`, `ClaimTransformerRegistry`, `TaskRegistry`, `I18nSettings`, `TimezoneSettings`, `MessageCatalog`, `TenantDefaultsProvider`) | `@Provider(singleton=True, priority=-sys.maxsize - 1)` | `@Fallback @Provider(singleton=True)` |
| A16 | `varco_fastapi/varco_fastapi/di.py:418,432` | `request_context`, `jwt_context` (non-singleton) | `@Provider(priority=-sys.maxsize - 1)` | `@Fallback @Provider` |
| A17 | `varco_fastapi/varco_fastapi/di.py:104-105` | module-level `Singleton(priority=…)(DefaultTaskSerializer)` / `(TaskRegistry)` stamps | imperative stamps + stale "varco_core must stay providify-free" comment (`:98-103`; false — `varco_core` imports providify in 25+ modules) | **delete both**; `TaskRegistry` already has A15's provider; `DefaultTaskSerializer` gets A18 |
| A18 | `varco_fastapi/varco_fastapi/di.py:509-545` | `setup_varco_defaults()` → `container.bind(TaskSerializer, DefaultTaskSerializer)` | the helper shape | **delete**; replaced by `VarcoFastAPIModule.task_serializer(self) -> TaskSerializer` `@Fallback @Provider(singleton=True)` returning `DefaultTaskSerializer()` (the `message_catalog` shape, `:400-408`) |
| A19 | `varco_fastapi/varco_fastapi/di.py:615` | `Singleton(priority=-sys.maxsize - 1)(BusEventProducer)` inside `setup_event_producer()` | redundant re-stamp of A10 | **delete the stamp + its comment `:609-614`**; the `_bus_event_producer` provider `:619-624` stays (non-fallback, an explicit opt-in — 046 decides the verb) |
| A20 | `varco_kafka/varco_kafka/config.py:228`, `channel.py:129`; `varco_nats/varco_nats/config.py:309`, `channel.py:167`; `varco_redis/varco_redis/config.py:140`, `backplane.py:134`, `cache.py:567`, `cache.py:787`; `varco_memcached/varco_memcached/cache.py:636` | the packages' env-settings defaults (`KafkaEventBusSettings`, `KafkaChannelManagerSettings`, `NatsEventBusSettings`, `NatsChannelManagerSettings`, `RedisEventBusSettings`, `RedisBackplaneSettings`, `RedisCacheSettings`, `LayeredCacheSettings`, `MemcachedCacheSettings`) | `@Provider(singleton=True, priority=-sys.maxsize)` / `-sys.maxsize - 1` | `@Fallback @Provider(singleton=True)`; docstrings `:253`/`:147`/`:334`/`:185`/`:165` and the "BEFORE ainstall(): equal-priority bindings resolve first-registered" examples (`redis/cache.py:554-556,771-773`, `memcached/cache.py:622-624`) rewritten |

**Class B — backend implementation of a core seam (should yield to a user's own binding of the same interface, but outrank the in-process default). → `@Fallback` + `priority=BACKEND_PRIORITY`, backend qualifier kept.**

| # | Site | Binding | Today | New |
|---|---|---|---|---|
| B1 | `varco_kafka/varco_kafka/bus.py:95` | `KafkaEventBus` (`AbstractEventBus`, `kafka`) | `@Singleton(priority=-sys.maxsize, qualifier="kafka")` | `@Fallback @Singleton(priority=BACKEND_PRIORITY, qualifier="kafka")` |
| B2 | `varco_nats/varco_nats/bus.py:129` | `NatsEventBus` (`nats`) | same | same shape |
| B3 | `varco_redis/varco_redis/bus.py:532-537` | `RedisEventBusSelectorConfiguration.bus()` (`AbstractEventBus`) | `@Provider(singleton=True)` at 0 — the redis *unqualified winner* | `@Fallback @Provider(singleton=True, priority=BACKEND_PRIORITY)` |
| B4 | `varco_redis/varco_redis/bus.py:102` | `RedisEventBus` (`redis`) | `@Singleton(priority=-sys.maxsize, qualifier="redis")` — sits *beneath* B3 | `@Fallback @Singleton(qualifier="redis")` — **priority deliberately omitted** so it never ties with B3 for an unqualified request; it remains the `qualifier="redis"` escape hatch and the concrete self-binding (§D-tiers) |
| B5 | `varco_kafka/varco_kafka/channel.py:160`; `varco_nats/varco_nats/channel.py:198`; `varco_redis/varco_redis/channel.py:61` | `KafkaChannelManager` / `NatsStreamManager` / `RedisChannelManager` (`ChannelManager`) | `@Singleton(priority=-sys.maxsize, qualifier=…)` | `@Fallback @Singleton(priority=BACKEND_PRIORITY, qualifier=…)` (no in-process `ChannelManager` exists; the priority is tier consistency) |
| B6 | `varco_sa/varco_sa/provider.py:24`; `varco_beanie/varco_beanie/provider.py:23` | `SQLAlchemyRepositoryProvider` / `BeanieRepositoryProvider` (`RepositoryProvider`) | `@Singleton(priority=-sys.maxsize, qualifier="sa"/"beanie")` | `@Fallback @Singleton(priority=BACKEND_PRIORITY, qualifier=…)` |
| B7 | `varco_sa/varco_sa/di.py:176` | `SAModule.uow_provider` (`IUoWProvider`) | `@Provider(singleton=True, priority=-sys.maxsize - 1)` | `@Fallback @Provider(singleton=True, priority=BACKEND_PRIORITY)` |
| B8 | `varco_sa/varco_sa/di.py:210` | `SAModule.sa_advisory_lock` (`AbstractDistributedLock`) | `@Provider(singleton=True)` at 0 + the "Option A — provide() before install()/scan(), registered FIRST — wins" recipe (`:225-241`) | `@Fallback @Provider(singleton=True, priority=BACKEND_PRIORITY)` (outranks A5 `InMemoryLock`); recipe rewritten to "any order" |
| B9 | `varco_beanie/varco_beanie/dlq.py:122` | `BeanieDeadLetterQueue` (`AbstractDeadLetterQueue`, `beanie`) | `@Singleton(priority=-sys.maxsize, qualifier="beanie")` | `@Fallback @Singleton(priority=BACKEND_PRIORITY, qualifier="beanie")` |
| B10 | `varco_kafka/varco_kafka/dlq.py:787`; `varco_redis/varco_redis/dlq.py:755`; `varco_redis/varco_redis/stream_dlq.py:~766`; `varco_nats/varco_nats/dlq.py:~773` | the four `@Configuration` DLQ providers (`AbstractDeadLetterQueue`) | `@Provider(singleton=True)` at 0 | `@Fallback @Provider(singleton=True, priority=BACKEND_PRIORITY)` (outranks A3) |
| B11 | `varco_redis/varco_redis/cache.py:578`, `:798`; `varco_memcached/varco_memcached/cache.py:647` | `redis_cache` / `layered_cache` / `memcached_cache` (`CacheBackend`) | 0 / 0 / `-sys.maxsize - 1` (memcached alone at the floor — inconsistent) | `@Fallback @Provider(singleton=True, priority=BACKEND_PRIORITY)` (outranks A1) |
| B12 | `varco_beanie/varco_beanie/query/compiler.py:114`, `query/aggregation.py:71` | `BeanieQueryCompiler`, `BeanieAggregationApplicator` | `@Singleton(priority=-sys.maxsize, qualifier="beanie"/"beanie_aggregation")` | `@Fallback @Singleton(priority=BACKEND_PRIORITY, qualifier=…)` |
| B13 | `varco_casbin/varco_casbin/engine.py:108` | `CasbinPolicyEngine` (`PolicyEngine`, `PolicyManagement`; `casbin`) | `@Singleton(priority=-sys.maxsize, qualifier="casbin")` | `@Fallback @Singleton(priority=BACKEND_PRIORITY, qualifier="casbin")` |

**Class C — collection contributions (`HealthCheck`, consumed by `aget_all()` at `varco_fastapi/varco_fastapi/router/health.py:110`). → plain `@Singleton(qualifier=…)`, floor removed, NEVER `@Fallback`** — a fallback is *excluded from `get_all()` the moment any non-fallback sibling exists* (`container.py:1683-1686`), so marking a health check `@Fallback` would silently drop every varco probe from `/health` the instant an app added its own check.

| # | Site | Today | New |
|---|---|---|---|
| C1-7 | `varco_sa/varco_sa/health.py:81,204`; `varco_beanie/varco_beanie/health.py:49`; `varco_memcached/varco_memcached/health.py:62`; `varco_nats/varco_nats/health.py:47`; `varco_kafka/varco_kafka/health.py:47`; `varco_redis/varco_redis/health.py:46` | `@Singleton(priority=-sys.maxsize, qualifier=…)` | `@Singleton(qualifier=…)` — ⚠️ `aget_all()` sorts ascending by priority, so the *order* of `/health` entries changes from "varco probes first" to registration order; documented in §Migration, no test asserts the order today (Step 1 verifies with `rg -n 'HealthCheck' varco_fastapi/tests`) |

**Class D — self-bound adapters overridden by a `bind_*` provider. → `@Fallback @Singleton`, priority omitted.**

| # | Site | Today | New |
|---|---|---|---|
| D1-2 | `varco_ws/varco_ws/sse.py:174`, `websocket.py:352` | `@Singleton(priority=-sys.maxsize)` — beaten by `bind_sse_adapter`/`bind_websocket_adapter`'s `Provider(singleton=True)(…, returns=SSEEventBus)` at 0 (`varco_ws/di.py:273,386`) | `@Fallback @Singleton` — the `bind_*` provider is a non-fallback and shadows it in any order |

**Class E — duplicates and prose. → removed / rewritten.**

| # | Site | What | Action |
|---|---|---|---|
| E1 | `varco_kafka/varco_kafka/dlq.py:772`; `varco_redis/varco_redis/dlq.py:740`; `varco_redis/varco_redis/stream_dlq.py:753`; `varco_nats/varco_nats/dlq.py:761` | a *second* provider of the package's settings type inside each DLQ `@Configuration` ("if the bus was registered first, the container resolves that one instead" — `kafka/dlq.py:777-779`) | **delete the four providers**; the DLQ provider's `settings:` parameter becomes `Annotated[XSettings \| None, InjectMeta(optional=True)] = None` with `settings = settings or XSettings.from_env()` in the body — the identical idiom the same method already uses for `serializer` (`kafka/dlq.py:791`). §D-dupes |
| E2 | `varco_redis/varco_redis/revocation.py:256-274` | `RedisScanNullTokenRevocationStoreDefault`, a local `NullTokenRevocationStore` subclass so "scan `varco_redis` alone" still has a default | **delete**; `varco_redis.di.bootstrap()` gains `container.scan("varco_core.revocation", recursive=True)` before its own scan — the same class, registered from its owner. Referenced only at `varco_redis/di.py:252` (docstring). Not in any `__all__` |
| E3 | `varco_core/varco_core/event/serializer.py:14`; `event/cloudevents.py:81,516`; `varco_fastapi/varco_fastapi/di.py:16-37,98-103,266,609-614`; `varco_kafka/varco_kafka/config.py:253`, `channel.py:147`; `varco_redis/varco_redis/config.py:165`, `cache.py:554-556,771-773`, `di.py:161,252`; `varco_nats/varco_nats/config.py:334`, `channel.py:185`; `varco_memcached/varco_memcached/cache.py:622-624`; `varco_kafka/varco_kafka/di.py:53`, `varco_nats/varco_nats/di.py:57`, `varco_redis/varco_redis/di.py:61` ("before scan() — order matters"); `varco_fastapi/varco_fastapi/posture.py:764-767` ("priority higher than -(2**31)"); `varco_fastapi/varco_fastapi/job/store.py:39` (`# MIN_INT as priority`) | prose describing the floor | rewrite to the one rule (§D-rule); `rg -n 'sys\.maxsize\|2\*\*31\|first-registered\|before install()\|before scan()' varco_*/varco_*` → 0 hits after Step 9 |

### Where the in-process default meets a backend (why two tiers exist)

`AbstractEventBus`: A2 vs B1/B2/B3. `CacheBackend`: A1 vs B11. `AbstractDeadLetterQueue`: A3 vs
B9/B10. `AbstractDistributedLock`: A5 vs B8. Every other seam has either only an in-process
default (A7-A16) or only backend bindings (B5, B6, B12, B13). In the four overlapping seams the
backend must win **once scanned/installed**, and the user must win over both — the priority
between two varco fallbacks is the only lever providify offers for the first, and `@Fallback` is
the lever for the second (§D-tiers).

## Design decisions

### §D-rule — one rule, one marker, stated once

> **Every binding varco itself registers for a varco seam is `@Fallback`. A binding the
> application registers — any `@Singleton`, `container.bind()`, `container.provide()`, or
> `install()`ed `@Configuration` provider without `@Fallback` — always wins, regardless of
> registration order, priority, or qualifier. `container.validate()` reports each default that
> lost as `IssueKind.FALLBACK_SHADOWED` at `Severity.INFO` — expected, never an error.**

- ✅ Intent over order (brief 010 §1, §"Options Compared" row 2): the decorator *says* "I am a
  default"; `describe()`/`validate()` can render it (brief 017 §3; consumed by Plan 052).
- ✅ Survives the equal-priority collision: a user who copies a varco decorator line verbatim now
  copies `@Fallback` too — and two fallbacks tie by priority then order (`scope.py:1042-1047`),
  which is the *same* behaviour they had, now with `AMBIGUOUS_BINDING` naming it. A user who
  omits `@Fallback` (the overwhelmingly common case) wins outright.
- ✅ Stays out of collections (`container.py:1683-1686`) — Plan 051's `list[T]`/`Multibound`
  registries never see a shadowed default.
- ❌ One more marker on ~60 source lines. Accepted — it replaces three magic numbers and a
  helper function.

**Alternative — the `DI-3` interim (`FRAMEWORK_DEFAULT_PRIORITY = -sys.maxsize - 1` named once):**
✅ zero semantic change; ❌ keeps the order-dependent tie (`container.py:2853`), keeps the
`get_all()` pollution, and the whole point of `DI-15` was that providify would ship the real
primitive — it has. Rejected; `BACKLOG.md:76` itself calls the floor "interim, until providify
ships it".

### §D-tiers — two fallback tiers, one named constant, in `varco_core.di`

**Chosen: in-process defaults carry no priority (providify's default `0`); backend bindings
carry `priority=BACKEND_PRIORITY` (`100`), a `Final[int]` in a new `varco_core/varco_core/di.py`.
Both tiers are `@Fallback`.**

```
request get(AbstractEventBus)          candidates after activation/qualifier narrowing
  app binding (non-fallback, 0)  ───►  present → every @Fallback dropped → app wins
  KafkaEventBus (@Fallback, 100, kafka) ┐ all-fallback → max(priority) → Kafka wins
  InMemoryEventBus (@Fallback, 0, in_memory) ┘
request get(AbstractEventBus, qualifier="in_memory")  → only A2 → InMemoryEventBus, always
```

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: defaults bare, backends `BACKEND_PRIORITY=100`** | The 31 in-process sites (A1-A20) look like *nothing special* — `@Fallback` and done; the thing that must **outrank** something carries the number (B1-B13, ~20 sites). Headroom above 100 is deliberate: Plan 047's env dotted-path override must beat varco's backends *and* lose to a code binding, i.e. be a `@Fallback` at a priority `> BACKEND_PRIORITY` — 047 names it | A second number still exists (one, named, imported — vs three unnamed today) |
| **(2) defaults at a negative constant, backends bare** | Backends look ordinary | 31 sites carry the constant instead of 20; a negative "default priority" reads as the old floor under a new name, and every new Null/InMemory default a contributor adds must remember it. Rejected |
| **(3) backends as plain non-fallback bindings at 0** | No constant at all | A user's own `AbstractEventBus` at 0 **ties** with a scanned `KafkaEventBus` at 0 → first-registered wins (`:2853`) — a regression from today, where the user beats `-sys.maxsize`. The one rule (§D-rule) would be false for every backend seam. Rejected |
| **(4) no priority; defer backend-vs-default to Plan 047's `@Requires`** | Nothing to name | 045 must be mergeable alone with a scanned backend beating the in-memory default and `validate()` clean; two fallbacks tied at 0 at an injection point (`KafkaEventBus.__init__` injects `KafkaEventBusSettings`; `AsyncService` injects `AbstractEventBus`) is an `AMBIGUOUS_BINDING` **ERROR** (`:6700-6712`). Rejected |

**The redis three-level shape (B3/B4/A2)** is the one place a backend has *two* varco bindings
for one seam: the selector provider (B3, unqualified winner, `BACKEND_PRIORITY`) and the concrete
class (B4, `qualifier="redis"`, priority omitted). If B4 also carried `BACKEND_PRIORITY` the
unqualified request would see two fallbacks tied at 100 → `AMBIGUOUS_BINDING` at every
`Inject[AbstractEventBus]` point. B4 at 0 loses to B3 for the unqualified request by priority
(all-fallback set) and still wins `get(AbstractEventBus, qualifier="redis")` (only candidate).
Plan 047 may collapse this when it replaces the selector; 045 records the shape and pins it with
a test (Step 12).

**Two backends scanned for the same seam** (kafka + nats both `BACKEND_PRIORITY`) tie exactly as
they tie today at `-sys.maxsize` — first-registered, plus `AMBIGUOUS_BINDING` where injected. Not
a regression; it is `DI-4`'s problem and Plan 047 owns it. The validate-clean test (§D-validate)
therefore covers *one* backend per seam.

### §D-qualifier — the `qualifier="in_memory"` escape hatch under `@Fallback`

`get(T, qualifier="in_memory")` narrows to `in_memory`-qualified candidates *before* the fallback
drop (`container.py:2147` then `:2168`), so the in-memory default is the **only** candidate and is
returned — even when an app binding and a backend both exist for `T`. `Inject[T]` (unqualified)
sees all three and drops the two fallbacks. This is exactly what the five qualified sites
(A1-A5) were built for, and it is now a *specified* providify behaviour (`container.py:2096-2101`)
rather than a side effect of `_filter`'s `None`-matches-any rule. Step 5 pins it in a
providify-only test and Step 11 in a varco test (`InMemoryEventBus` with a user bus present).

**What changes for a user**: nothing — except that `get_all(T)` no longer includes the in-memory
default beside their own binding (§Migration).

### §D-fastapi — `setup_varco_defaults()` disappears; what replaces each thing it did

| `setup_varco_defaults()` / its surroundings did | Replacement |
|---|---|
| `container.bind(TaskSerializer, DefaultTaskSerializer)` (`di.py:545`) | `VarcoFastAPIModule.task_serializer(self) -> TaskSerializer: return DefaultTaskSerializer()` — `@Fallback @Provider(singleton=True)`, same shape as `message_catalog` (`:400-408`). Registered by `container.scan("varco_fastapi", …)` / `bootstrap()`, i.e. exactly where every other varco_fastapi default already lives |
| module-level `Singleton(priority=…)(DefaultTaskSerializer)` stamp (`:104`) | gone — the provider above owns the binding; `DefaultTaskSerializer` stays undecorated in `varco_core` (a plain class; the DI-token is the provider's return type) |
| module-level `Singleton(priority=…)(TaskRegistry)` stamp (`:105`) | gone — `task_registry` provider (`:371`) already exists |
| `bootstrap(setup_defaults=True)` kwarg (`:647,683-685,723-724`) | **removed** (breaking, §Migration); `bootstrap()` scans, which registers the module |
| "If `VarcoFastAPIModule` was not installed … `VarcoCRUDRouter._task_serializer.is_resolvable()` falls back to `DEFAULT_SERIALIZER`" (`:532-535`) | unchanged — that runtime fallback is in the router, not the helper |
| `__all__` entry `"setup_varco_defaults"` (`:632`) | removed; `scripts/api_surface.py --check` run in Step 16 (not a top-level `varco_fastapi.__all__` name — expected no snapshot change; regenerate if it reports one) |

⚠️ The imperative stamp form `Singleton(priority=…)(cls)` is the one shape where **order matters
for `@Fallback`**: `ClassBinding.fallback` is read at binding construction (`binding.py:207`), so
a `Fallback(cls)` call must precede any `register()`/`bind()`/`scan()` that sees `cls`. This plan
removes all three imperative stamps (A17, A19), so no varco site depends on it; the rule is
recorded in `dependency-injection.md` for app authors.

### §D-dupes — the four duplicate settings providers and the redis Null-store copy

`KafkaDLQConfiguration.kafka_dlq_settings` (E1) exists so `ainstall(KafkaDLQConfiguration)` on a
bare container works without a scan ("DLQ only, no bus" — `kafka/dlq.py:762-764`, tested at
`varco_kafka/tests/test_kafka_dlq_disposes.py:38`). Under `@Fallback` it and
`kafka_event_bus_settings` (A20) would be two fallbacks tied at 0 → `AMBIGUOUS_BINDING` ERROR at
`KafkaEventBus.__init__`'s `Inject[KafkaEventBusSettings]` whenever both are registered — the
*normal* `bootstrap()` + `ainstall(DLQ)` path.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: delete the copy; `settings: Annotated[XSettings \| None, InjectMeta(optional=True)] = None`, `settings = settings or XSettings.from_env()` in the body** | Standalone install keeps working; scan path injects the package's single provider; `validate()` is silent for an optional param with no binding (`container.py:6653-6655`); the idiom already exists two lines away for `serializer` (`kafka/dlq.py:791`) | The env fallback is decided in a provider body — the "hidden selection" smell P25 was filed about. Accepted: it is a fall-*back to env*, not a choice between implementations, and Plan 048's settings recipe may replace it |
| (2) keep the copy at a third, lower priority | Standalone install unchanged | A third named number for one case; the exact "fallback beneath a fallback" the P27 report §2 called evidence of a stretched heuristic. Rejected |
| (3) delete the copy, require a scan first | Simplest | Breaks the documented and tested standalone path; a `MISSING_BINDING` ERROR is the only signal. Rejected |

`RedisScanNullTokenRevocationStoreDefault` (E2) is the same problem in class form (two Null-store
fallbacks tied at 0 when `varco_core` and `varco_redis` are both scanned). Its reason for existing
— providify's scanner skips re-exported symbols — is answered by scanning the owner:
`varco_redis.di.bootstrap()` adds `container.scan("varco_core.revocation", recursive=True)`.
✅ one class, one binding, one owner; ❌ `bootstrap()` now touches a `varco_core` subpackage —
acceptable, it is the sanctioned entry point and Plan 046 makes it the only one.

### §D-proof — what "proof" means, and the scanned-class blind spot

The rule's proof is behavioural (a user binding registered **after** the default wins — Steps
11-12 test both shapes: scanned `@Singleton` default and `@Provider`-in-`@Configuration` default)
**plus** the `FALLBACK_SHADOWED` INFO — but the INFO is only emitted where the fallback's
*natural request* is the one the app shadows:

- `@Provider(...) -> ABC` defaults (A13, A15, A16, A18, A20, B3, B7, B8, B10, B11): `b.interface`
  is the ABC → shadowed by an app `ABC` binding → INFO with `shadowed_by` ✅.
- Scanned `@Singleton class Impl(ABC)` defaults (A1-A12, A14, B1, B2, B4-B6, B9, B12, B13, D1-2):
  `b.interface is Impl` (`container.py:1248`) → the natural request is `(Impl, qualifier)` → the
  app's `ABC` binding is not a candidate for *that* request → **no INFO**, although `get(ABC)`
  correctly returns the app's binding.

This is a real diagnostic gap for Plan 052's report ("which defaults are live?"). Per CLAUDE.md
§"When you hit a providify limitation": Step 1 verifies it against the installed 2.1.0 (a
scanned-class default + an app `bind(ABC, X)` → `validate()` has zero `FALLBACK_SHADOWED`), and
if confirmed, Step 4 files **`P28-FALLBACK-SHADOWED-SCANNED-CLASS`** — report, ledger row, strict
xfail (`raises=AssertionError`) in the contracts test module — with the ask *"evaluate the
natural request against each base class in `Impl.__mro__` that has at least one other active
binding, or record the ABC at `register()` time"*. It does **not** block any step here: the
validate-clean test and the behavioural tests do not depend on it, and 052 is told in its own
plan to read this section. ⚠️ ASSUMPTION until Step 1 measures it.

### §D-validate — "`validate()` is clean for a default varco container"

**Definition** (`varco_fastapi/tests/test_validate_clean_default_container.py`, Step 13): for
`container = DIContainer(); container.scan("varco_core", recursive=True); bootstrap(container)`
(varco_fastapi's), `report = container.validate(raise_on_error=False)` must satisfy
`report.errors == ()` **and** `report.warnings == ()`; `report.infos` may contain only
`FALLBACK_SHADOWED` and `CONDITION_INACTIVE` kinds (none expected in this container; asserted as
a set-subset so 047's `@Requires` additions do not break it). Three more legs, one per
Docker-free backend scan (`varco_kafka`, `varco_nats`, `varco_sa`), assert the same in those
packages' own test dirs — scanning registers bindings without opening sockets; `validate()`
instantiates nothing. `varco_redis`/`varco_memcached`/`varco_beanie` legs are added only if
Step 1's preflight shows their scan is import-clean without a server; otherwise they are left
to 046's suite with a note.

⚠️ ASSUMPTION — a default container has **no** pre-existing `WARNING`s at 2.1.0. Step 1 measures
the baseline at 2.0.1 and again after the bump; a pre-existing warning unrelated to this plan
(e.g. `MISSING_BINDING_DEFAULTED` on a defaulted, non-optional parameter, or
`UNREACHABLE_PRE_DESTROY`) is recorded in the test as an explicit `_KNOWN_WARNINGS` allowlist of
`(kind, owner)` pairs with a comment naming the owning plan (046/050), never as a blanket
"warnings allowed". The test then asserts `set(report.warnings) - _KNOWN == set()`.

**Double registration**: `varco_fastapi/tests/test_di_binding_health_i18n_tz.py:23-26` scans
`varco_fastapi` (which auto-installs `VarcoFastAPIModule`) **and** calls
`container.install(VarcoFastAPIModule)` — two provider bindings per default. At 2.1.0 that is two
fallbacks tied at 0 → `AMBIGUOUS_BINDING` at every injection point that injects one of them. The
redundant `install()` is removed from that test and from `test_di_binding_health.py:174`'s
`setup_varco_defaults` call site; ⚠️ ASSUMPTION that providify does not already dedupe a second
`install()` of the same module — Step 1 checks.

### §D-tests — the test shape (seeds for 046's suite, not the suite)

| Test module | Asserts | Docker |
|---|---|---|
| `varco_core/tests/test_providify_contracts.py` (renamed from `test_providify_upstream_gaps.py`, Step 5) | The converted P25/P26/P27 bodies as **passing contract pins** of what varco relies on from 2.1.0: `@Requires` flips with the predicate; open-generic closed request + `type[T]` delivery; `@Fallback` loses to a later non-fallback, `get_all` excludes it, lone fallback resolves, `FALLBACK_SHADOWED` INFO names `B`; **plus** three new pins — qualified fallback wins its own qualified request; two fallbacks tie by priority (`100` beats `0`); `get_all()` of an all-fallback set returns both. Module docstring becomes the *upgrade checklist*: "re-run at every providify bump; a failure here means the semantics varco's defaults rely on moved" | no |
| `varco_redis/tests/test_redis_cache_disposes.py` (Step 6) | marker removed; both configurations installed → both `stop()` called; `validate()` has zero `UNMATCHED_DISPOSER`/`DISPOSER_OVERWRITTEN`; `provide()`'s return is a `ProviderBinding` (the P24 ask's "alternative, larger" fix, shipped) | no |
| `varco_core/tests/test_fallback_defaults.py` (Step 11) | For each of A1-A12 (parametrised by `(ABC, default_cls, qualifier)`): scan `varco_core` alone → `get_binding(ABC).implementation is default_cls`; then `container.bind(ABC, UserImpl)` **after** the scan → `get_binding(ABC).implementation is UserImpl`, `[type(b.implementation) …] == [UserImpl]` for `get_all_bindings(ABC)`; for the qualified five → `get_binding(ABC, qualifier="in_memory").implementation is default_cls` still; `getattr(default_cls, …)` carries `FallbackMarker` (via `providify.metadata._is_fallback`); **and** for A13 (`OtelConfiguration.otel_config`, the provider shape) → after `install()`, then `provide(user_otel_config)` → user wins and `report.infos` has one `FALLBACK_SHADOWED` whose `shadowed_by` names `user_otel_config`. Also the §D-proof blind spot pinned as the P28 strict xfail | no |
| `varco_fastapi/tests/test_fallback_defaults.py` (Step 12, **replaces** `test_di_priority_direction.py`) | For `TrustStore`/`CORSConfig`/`ClientProfile`/`TaskSerializer`/`MessageCatalog`/`AbstractJobStore`: user `@Provider` registered after `bootstrap()` wins; `get_all(T)` returns **only** the user's (the old test's `len == 2` / "default sorts first" assertions are the exact behaviour this plan removes); `validate()` has one `FALLBACK_SHADOWED` INFO per shadowed provider default, `shadowed_by` naming the user's provider; `bootstrap()` no longer accepts `setup_defaults=` (`TypeError`) | no |
| `varco_kafka/tests/test_kafka_fallback_tier.py`, `varco_nats/tests/test_nats_fallback_tier.py`, `varco_redis/tests/test_redis_fallback_tier.py`, `varco_sa/tests/test_sa_fallback_tier.py` (Step 12) | scan `varco_core` + `bootstrap()` the backend → `get_binding(AbstractEventBus)` is the backend's (kafka/nats class; redis: the selector `ProviderBinding`, `binding.fallback is True`, `binding.priority == BACKEND_PRIORITY`); `get_binding(AbstractEventBus, qualifier="redis").implementation is RedisEventBus`; `get_binding(AbstractEventBus, qualifier="in_memory")` is `InMemoryEventBus`; a user `bind(AbstractEventBus, UserBus)` registered **after** bootstrap wins; sa: `get_binding(AbstractDistributedLock)` is `sa_advisory_lock`'s binding, user lock provider after scan wins; `HealthCheck` bindings are **not** fallbacks (`binding.fallback is False`) and are all present in `get_all_bindings(HealthCheck)` after a user `HealthCheck` is bound | no (`get_binding` never instantiates) |
| `varco_fastapi/tests/test_validate_clean_default_container.py` + one leg each in `varco_kafka`/`varco_nats`/`varco_sa` (Step 13) | §D-validate | no |

`get_binding()`/`get_all_bindings()` are the workhorses: pure reads (`container.py:7429-7433`),
so a `KafkaEventBus` binding is asserted without a broker. The existing per-package DI tests
that scan and then `get()` a bus (`varco_kafka/tests/test_kafka_di.py:166` etc.) are untouched.

### §D-closure — closing four ledger rows, the P22 shape

`design/upstream-gaps/providify-provider-predestroy.md` is the precedent (`:7` status line; `§8.
Resolution (2026-09-02)` at `:202-241`): header `**Status**` flips to `✅ Resolved (2026-09-14,
Plan 045 / DI-15 close-out) — see §N`; a new final `## N. Resolution (2026-09-14)` section cites
the providify `CHANGELOG.md` lines that shipped it, names what varco adopted and in which step,
and states that the guard is now a passing contract test (file::test). `UPSTREAM-GAPS.md` loses
the four "Open gaps" rows and gains four "Recently closed" bullets in the P22 bullet's register;
the header table keeps its shape so the next filer has a template. ⚠️ If Step 4 files P28, the
"Open gaps" table has exactly one row afterwards.

### §D-docs — where each sentence goes (CLAUDE.md stays a routing table)

- `technical_docs/features/dependency-injection.md` (**new**, first version): title + a banner
  "*Plan 045 wrote the Default bindings section; Plan 058 completes this document (contract,
  selection, multi-binding, lifecycle, testing)*"; then **§Default bindings** only — the rule
  (§D-rule verbatim), the two tiers + `BACKEND_PRIORITY` (§D-tiers diagram), the `in_memory`
  escape hatch (§D-qualifier), how to override each shape (scanned class → `bind()`/`register()`
  your own; provider → `provide()` yours; both "in any order"), the `FALLBACK_SHADOWED` INFO and
  how to read `report.infos`, the scanned-class blind spot (§D-proof) with its P28 pointer, the
  imperative-stamp ordering rule (§D-fastapi ⚠️), and a **Pitfalls** table (marking a collection
  contribution `@Fallback` empties it; a subclass does not inherit `@Fallback`; a fallback
  singleton resolved before its shadower is not evicted; two backends for one seam tie until 047).
- `CLAUDE.md` §"DI wiring (providify)": **three lines** after the code block — *Rule: every
  varco-registered binding of a varco seam is `@Fallback` (in-process defaults bare, backends at
  `varco_core.di.BACKEND_PRIORITY`); a user binding always wins regardless of order; never mark
  a collection contribution (`HealthCheck`) `@Fallback`. Full model + pitfalls:
  `technical_docs/features/dependency-injection.md`.* Nothing else — the verb-taxonomy table is
  046's and the `setup_varco_defaults` mention in the code block (`CLAUDE.md` "Typical app
  bootstrap") does not exist; only the pitfall-catalogue pointer text changes.
- `README.md`: the "DI wiring" snippet (`:709-736`) gains a second block showing an override
  (`@Singleton class MyAuthorizer(AbstractAuthorizer)` registered *after* `bootstrap()` and
  winning) and one sentence on `@Fallback`; `:879` ("Registered at the lowest priority so any
  application authorizer automatically takes precedence") → "declared `@Fallback`, so …";
  `:3734-3735` (OtelConfig "before install()") → any order; `:4457` (NullFeatureFlags "lowest
  priority") → `@Fallback`; `:3497` unchanged (`setup_producer=` is 046's).
- `ARCHITECTURE.md:223`, `:1309` (priority prose) and `:824` (advisory-lock "provide() before
  install()" recipe) → the rule; no new tables (058 owns the entry-point table).
- `technical_docs/common-pitfalls.md:43` (the `Serializer[Event]` row's fix column: "use
  `@Singleton(priority=-sys.maxsize - 1)`") → "`@Fallback @Singleton`"; **new row**: "Marked a
  `HealthCheck` (or any `get_all()`-consumed contribution) `@Fallback` → it vanishes from the
  collection the moment the app adds its own".
- `technical_docs/features/feature-flags.md:68`, `cloudevents-envelope.md:14-15`,
  `distributed-locks.md:103-106`, `opa-design.md:51` (the parked OPA design's sketch decorator)
  → the new decorations.
- `CHANGELOG.md` `[Unreleased]`: one `### BREAKING — @Fallback framework defaults, providify
  >= 2.1.0 (Plan 045, DI-15 close-out / DI-3)` block (§Migration's bullets) and one
  `### Fixed / Tests` block closing P24/P25/P26/P27 (each: what shipped upstream, what varco
  adopted, the guard's new name).

## Steps

All commands run from `/home/edoardo/projects/varco`. Order matters through Step 6; Steps 7-9
are independent of each other; 10-13 need 6-9; 14-17 last.

### Preflight and the pin

1. [x] **Preflight — record the 2.0.1 baseline, then confirm 2.1.0's semantics on the installed copy.**
       (a) Baseline at 2.0.1: `uv run pytest varco_core/tests/test_providify_upstream_gaps.py varco_redis/tests/test_redis_cache_disposes.py -q -rxX` → `6 xfailed`; then the §D-validate
       container's `validate(raise_on_error=False)` — paste `len(report.errors)`,
       `len(report.warnings)` and every `(kind, owner)` into
       `/tmp/claude-1000/…/scratchpad/045-preflight.txt` (the allowlist source, §D-validate ⚠️).
       Also `rg -n 'HealthCheck' varco_fastapi/tests varco_*/tests | rg -i 'order|index|\[0\]'` →
       record whether any test asserts health-check ordering (Class C ⚠️).
       (b) `git -C /home/edoardo/projects/providify rev-parse --short HEAD` → `3cb207f`;
       `rg -n '^version' /home/edoardo/projects/providify/pyproject.toml` → `2.1.0`.
       (c) Bump: in **all ten** `pyproject.toml`s (`rg -n 'providify>=2.0.1' */pyproject.toml` →
       10 hits: `varco_core:28`, `varco_redis:28`, `varco_ws:25`, `varco_fastapi:31`,
       `varco_memcached:26`, `varco_sa:25`, `varco_kafka:30`, `varco_nats:32`, `varco_beanie:34`,
       `varco_casbin:24`) change to `providify>=2.1.0`; `uv lock`; `uv sync --all-packages
       --all-extras`; `uv run python -c "import importlib.metadata as m; print(m.version('providify'))"`
       → `2.1.0`. `scripts/bump.py` is **not** used (it writes varco versions, not third-party pins).
       (d) On the installed 2.1.0, re-read and confirm every `file:line` in §Context's table
       (`.venv/lib/python3.12/site-packages/providify/…` — same tree as `3cb207f`; if any line
       differs, fix the citation in this plan before continuing).
       (e) Measure the three ⚠️s: the scanned-class blind spot (§D-proof: `@Fallback @Singleton
       class Impl(Base)`, `container.register(Impl)`, `container.bind(Base, Other)`,
       `validate()` → count `FALLBACK_SHADOWED`); double `install()` of one `@Configuration`
       (§D-validate: count bindings for `TrustStore` after `scan("varco_fastapi") +
       install(VarcoFastAPIModule)`); whether `container.scan("varco_redis"/"varco_memcached"/
       "varco_beanie", recursive=True)` on a bare container imports cleanly with no server
       (§D-validate legs). Paste all three outcomes into the preflight file.
       (f) `uv run pytest varco_core/tests/test_providify_upstream_gaps.py varco_redis/tests/test_redis_cache_disposes.py -q -rxX`
       at 2.1.0 → expect `6 xpassed` reported as **failures** (strict) — the loud signal this plan
       exists to answer. `make test` is expected **red** from here until Step 6.
       **Verify**: the preflight file lists ≥ 20 confirmed citations, the baseline issue list, and
       the three measurements; `uv.lock` shows `providify` `2.1.0`.

2. [x] `varco_core/varco_core/di.py` (**new**) — `BACKEND_PRIORITY: Final[int] = 100` with a
       module docstring that *is* the two-tier model (§D-tiers diagram, the "why not negative /
       why not plain / why 100 and headroom for 047" ✅/❌ block, the `in_memory` rule, the
       imperative-stamp ordering caveat) and `__all__ = ["BACKEND_PRIORITY"]`. No providify
       import, no side effects, nothing scanned (a bare `Final` module — the standing "nothing
       scanned in `varco_core` activates anything" rule is trivially met). Not added to
       `varco_core/__init__.py`'s lazy map (Non-goals).
       Test: `varco_core/tests/test_di_constants.py` — `BACKEND_PRIORITY > 0`, `isinstance(…, int)`,
       and `import varco_core.di` has no `providify` in `sys.modules` delta (keeps the import
       budget untouched). **Verify**: `uv run python scripts/import_budget.py --check --warn-only`
       unchanged.

### Convert the guards and close the ledger (P24-P27)

3. [x] **Report resolutions (§D-closure)** — `design/upstream-gaps/providify-disposes-first-match.md`
       (status `:7` → resolved; new `## 7. Resolution (2026-09-14)` after `:156`: cite providify
       `CHANGELOG.md:14-31` — wiring scoped to the installing module's own `own` list
       (`container.py:7321,7359`), `UNMATCHED_DISPOSER`/`DISPOSER_OVERWRITTEN` shipped, `provide()`
       returns the binding; varco adopted: nothing to change in `RedisCacheConfiguration`/
       `RedisLayeredCacheConfiguration`, guard converted in Step 6; the **behaviour change**
       `CHANGELOG.md:22-24` — a `@Disposes(X)` on a module with no `X` provider no longer attaches
       anywhere — audited by `rg -n '@Disposes' varco_*/varco_*` against each module's own
       providers, result pasted). `providify-conditional-registration.md` (status `:7`; new
       `## 9. Resolution`: `CHANGELOG.md:32-47`; varco adopts `@Requires` in Plan 047, not here;
       guard converted Step 5; §5's "DI-4 not blocked via `activate_profile()`" is superseded —
       047 uses `@Requires` directly). `providify-open-generic-binding.md` (status; new `## 8.
       Resolution`: `CHANGELOG.md:69-129` incl. the two narrow behaviour changes; the eight rules
       vs the report's §4 table — note rule 3's positional idiom and rule 8's `TypeError`; adoption
       by 046/051; guard converted Step 5). `providify-fallback-binding.md` (status; new `## 9.
       Resolution`: `CHANGELOG.md:48-68`; every §5 edge row confirmed at `container.py:2096-2112`
       and `scope.py:1039-1053`; `Severity.INFO` granted (`validation.py:60`); adoption is *this
       plan* — cite the site table; §6/§7's `FRAMEWORK_DEFAULT_PRIORITY` interim never built).
       **Verify**: `rg -n '^\*\*Status\*\*: ✅ Resolved' design/upstream-gaps/providify-*.md` → 5
       hits (P22 + four); each new section cites `providify/CHANGELOG.md:` at least once.

4. [x] **Ledger + the conditional P28 filing.** `UPSTREAM-GAPS.md`: delete rows `:59-62`; under
       "Recently closed" (`:64-75`) add four bullets in the P22 bullet's register, each naming
       the resolving providify version (2.1.0), the plan (045), the report §, and the converted
       test id; footer `:79` → `**Last updated:** 2026-09-14 · Plan 045 closed P24, P25, P26, P27
       (providify 2.1.0)`. **If Step 1(e) confirmed the blind spot** (§D-proof): write
       `design/upstream-gaps/providify-fallback-shadowed-scanned-class.md` in the P27 file's
       eight-section shape (§1 cite `container.py:1248` and `:6360-6370`; §2 the 22 scanned-class
       defaults in the site table that 052's report cannot see; §3 the Step 11 xfail body; §4 the
       MRO-walk and record-ABC-at-register candidates with ✅/❌; §5 semantics — one INFO per
       `(base, qualifier)` request actually shadowed; §6 partly ours? **No** — nothing in public
       API lets varco declare "my interface is `AbstractEventBus`" on a scanned class except
       switching every default to `bind()`/`@Provider`, a shape change 045 rejects for 22 sites;
       §7 interim: none, 052 renders the provider-shaped INFOs and lists scanned-class defaults
       from 047's catalogue instead; §8 guard), one "Open gaps" row, and a CHANGELOG line. If
       **not** confirmed (2.1.0 already reports it), record that in the preflight file and skip.
       **Verify**: `rg -c '^\| \*\*P2' UPSTREAM-GAPS.md` → `0` or `1`; every "Recently closed"
       report path resolves.

5. [x] `varco_core/tests/test_providify_upstream_gaps.py` → **`git mv`** to
       `varco_core/tests/test_providify_contracts.py`; rewrite the module docstring (§D-tests row
       1 — the upgrade-checklist framing; keep the gap-ID map as *history* with "closed by
       providify 2.1.0, Plan 045"); for each of the five xfail tests: remove the marker, run, and
       fix only what 2.1.0's shipped shape requires (P25: `@Requires(condition=…)` on undecorated
       classes + `bind()` — if `bind()` needs `@Singleton` metadata, add `Singleton(OnImpl)`
       first and say so in the docstring; P26: the `# type: ignore[valid-type]` on
       `returns=Repo[T]` may now be unneeded — drop it if mypy passes; P27: unchanged bodies, drop
       both `# type: ignore[attr-defined]`). Add the three new pins (§D-tests row 1) plus
       `test_priority_narrowing_returns_shadowed_fallback` (E14, `container.py:2108-2112`) and
       `test_subclass_of_fallback_is_not_a_fallback` (`metadata.py:858-861`). Every test docstring
       cites the 2.1.0 line it pins. Keep the three 2.0.1 controls (still true).
       **Verify**: `uv run pytest varco_core/tests/test_providify_contracts.py -q -rxX` → all
       passed, `0 xfailed`, `0 xpassed` (or exactly `1 xfailed` if P28 was filed, with
       `GAP: P28-` in its reason); `rg -n 'GAP: P2[4-7]-' varco_*/tests/` → 0 hits.

6. [x] `varco_redis/tests/test_redis_cache_disposes.py` — remove the `xfail` marker
       (`:91-103`); rewrite the module docstring `:1-20` ("RED MODE" is stale — describe the
       three passing contracts); in `test_both_cache_configurations_installed_together_both_get_stopped`
       add `report = container.validate(raise_on_error=False)` before `ashutdown()` and assert no
       issue of kind `UNMATCHED_DISPOSER`/`DISPOSER_OVERWRITTEN`; add
       `test_provide_returns_the_provider_binding` (`isinstance(container.provide(fn), ProviderBinding)`).
       **Verify**: `uv run pytest varco_redis/tests/test_redis_cache_disposes.py -q -rxX` → all
       passed, `0 xpassed`.

### Declare the defaults

7. [x] **Class A sites A1-A13** (`varco_core`) — apply the table's "New" column; delete
       `import sys` where it becomes unused; `from providify import Fallback` beside the existing
       `Singleton`/`Provider` import; rewrite each site's priority prose (E3) to one sentence:
       *"`@Fallback` — the in-process default; any application binding of `<ABC>` wins regardless
       of registration order (`technical_docs/features/dependency-injection.md`)."* A12: delete
       `_FALLBACK_PRIORITY` (`authorizer.py:59-62`), rewrite the module docstring `:8-45` (the
       "why not `-1`" paragraph is obsolete; the "no qualifier is intentional" DESIGN block stays
       — it is still true under `@Fallback`, `container.py:2096-2101`), keep the class docstring's
       "When to replace" but as "register any `AbstractAuthorizer` binding". A13: docstring
       `:190-208` → "register yours before or after `install()`".
       Tests updated in the same step: `varco_core/tests/test_base_authorizer.py:36,108-138,245`
       (drop the constant tests; assert `_is_fallback(BaseAuthorizer)` and the after-scan override);
       `test_event_serializer_alias.py:23,131` (`{-sys.maxsize - 1}` → `binding.fallback is True`,
       priority `0`); `test_cloudevents_serializer.py:289-297` (the "wins by default (priority)"
       comment and `priority != -sys.maxsize - 1` → the CloudEvents provider is a non-fallback that
       shadows A11 — assert `get_binding(Serializer[Event]).fallback is False`).
       **Verify**: `make test PKG=varco_core`; `rg -n 'sys\.maxsize|2\*\*31' varco_core/varco_core`
       → 0 hits.

8. [x] **Class A sites A14-A20 + §D-fastapi** — `varco_fastapi/varco_fastapi/di.py`: delete
       `:98-105` (stamps + stale comment), add `task_serializer` provider after `:416` (A18),
       decorate `:280-432` per table (A15/A16), delete `setup_varco_defaults` (`:509-545`) and
       its `__all__` entry (`:632`), delete the A19 stamp `:609-615`, drop `setup_defaults` from
       `bootstrap()` (`:647,683-685,723-724`), rewrite the module docstring `:16-37` (the
       "Registered defaults" list gains `TaskSerializer`; the `setup_varco_defaults` paragraph
       goes; the override example becomes "register yours in any order"), the class docstring
       `:266` ("before scanning" → any order), `:406`. `job/store.py:39` (A14).
       `varco_fastapi/varco_fastapi/posture.py:764-767`: message → "register any
       `AbstractAuthorizer` binding — varco's `BaseAuthorizer` is `@Fallback` and yields to it".
       Backend settings providers (A20): the nine sites + their docstrings/examples. E1
       (§D-dupes): the four DLQ-configuration copies deleted, the optional-inject + `from_env()`
       shape applied, each configuration's class docstring "Reuses … if already registered"
       paragraph rewritten to state the optional-injection contract. E2: delete
       `RedisScanNullTokenRevocationStoreDefault` (`varco_redis/revocation.py:256-274+`, and the
       `sys` import if unused), add `container.scan("varco_core.revocation", recursive=True)` to
       `varco_redis.di.bootstrap()` before `:165`, fix `di.py:161,252` prose.
       Tests updated: `test_di_binding_health.py:54,174` and `test_di_binding_health_i18n_tz.py:18,25-26`
       (drop `setup_varco_defaults` import/call **and** the redundant `install(VarcoFastAPIModule)`
       after a scan — §D-validate); `varco_kafka/tests/test_kafka_dlq_disposes.py`,
       `varco_redis/tests/test_redis_dlq_disposes.py`, `test_redis_stream_dlq_disposes.py`,
       `varco_nats` equivalent — unchanged in intent (bare `ainstall(DLQ)` still works via
       `from_env()`), re-run to prove it; `varco_kafka/tests/test_kafka_di.py:176`,
       `varco_nats/tests/test_nats_di.py:159`, `varco_redis/tests/test_redis_di.py:145`
       (`@Singleton(priority=-sys.maxsize)` on a test settings subclass → plain `@Singleton`; the
       characterization they prove is about pydantic `**values`, not priority).
       **Verify**: `make test PKG=varco_fastapi`, `PKG=varco_kafka`, `PKG=varco_nats`,
       `PKG=varco_redis`, `PKG=varco_memcached` green; `rg -n 'setup_varco_defaults|setup_defaults'
       varco_* examples README.md` → 0 hits.

9. [x] **Class B, C, D sites** — B1-B13 per table (`from varco_core.di import BACKEND_PRIORITY`
       beside each backend's providify import; B4 and D1-2 without priority); C1-7 → plain
       `@Singleton(qualifier=…)` with a one-line comment *"collection contribution — never
       `@Fallback` (it would vanish from `get_all(HealthCheck)` once an app adds its own check)"*;
       B8's recipe `varco_sa/di.py:225-241` → "register yours in any order; `SAXactAdvisoryLock`
       remains injectable directly". Rewrite every remaining E3 prose site.
       **Verify**: `rg -n 'sys\.maxsize' varco_*/varco_*` → **0 hits**;
       `rg -n 'first-registered|before install\(\)|before scan\(\)|order matters' varco_*/varco_*`
       → 0 hits (excluding `lifespan.py`/`router/*` lines about *component/route* order, which
       are unrelated — list them in the preflight file); `rg -c '@Fallback' varco_*/varco_*` ≥ 55;
       `make test PKG=varco_sa`, `PKG=varco_beanie`, `PKG=varco_casbin`, `PKG=varco_ws` green.

### Prove the rule

10. [x] `varco_core/tests/test_di_constants.py` (Step 2) and the **contract pins** are in place;
        now `varco_core/tests/test_fallback_defaults.py` (**new**, §D-tests row 3) — the
        A1-A12 parametrised table, the A13 provider-shape INFO test, the `in_memory` escape-hatch
        test, and (if P28 filed) the strict-xfail blind-spot guard with `raises=AssertionError`
        and `reason="GAP: P28-FALLBACK-SHADOWED-SCANNED-CLASS — …"`. Each user override is a
        local class/provider defined **inside the test**, registered **after**
        `container.scan("varco_core", recursive=True)`.
        **Verify**: `uv run pytest varco_core/tests/test_fallback_defaults.py -q -rxX` → passed
        (+ `1 xfailed` iff P28).

11. [x] `varco_fastapi/tests/test_fallback_defaults.py` (**new**) **replaces**
        `test_di_priority_direction.py` (`git rm` — its `len(all_instances) == 2` and "default
        sorts first" assertions are the removed behaviour): §D-tests row 4, including
        `test_bootstrap_rejects_setup_defaults_kwarg`.
        **Verify**: `uv run pytest varco_fastapi/tests/test_fallback_defaults.py -q` → passed;
        `ls varco_fastapi/tests/test_di_priority_direction.py` → absent.

12. [x] The four backend tier tests (§D-tests row 5): `varco_kafka/tests/test_kafka_fallback_tier.py`,
        `varco_nats/tests/test_nats_fallback_tier.py`, `varco_redis/tests/test_redis_fallback_tier.py`
        (the three-level B3/B4/A2 shape, incl. `get_binding(AbstractEventBus).priority == BACKEND_PRIORITY`
        and `.fallback is True`), `varco_sa/tests/test_sa_fallback_tier.py` (B7/B8 + the
        `HealthCheck` non-fallback assertion for C1-2). Each ends with the "user binding registered
        AFTER bootstrap wins" assertion for both shapes present in that package. A one-line
        docstring in each: *"Seed for Plan 046's `di_wiring` conformance suite — do not grow this
        file; move it there."*
        **Verify**: the four files pass with no Docker (`-m 'not integration'` is the default).

13. [x] **Validate-clean** (§D-validate): `varco_fastapi/tests/test_validate_clean_default_container.py`
        + `varco_kafka/tests/test_validate_clean_scan.py`, `varco_nats/…`, `varco_sa/…` (+ redis/
        memcached/beanie only if Step 1(e) allowed). The `_KNOWN_WARNINGS` allowlist, if
        non-empty, names the owning plan per entry.
        **Verify**: each passes; `report.infos` non-empty **only** if a user binding was added
        (it is not, in these tests — assert `report.infos == ()` for the default container, which
        doubles as the proof that no varco default shadows another varco default at equal tier).

### Docs, changelog, gates

14. [x] `technical_docs/features/dependency-injection.md` (**new**) per §D-docs; `CLAUDE.md`
        three lines in §"DI wiring (providify)"; `README.md:709-736`, `:879`, `:3734-3735`, `:4457`;
        `ARCHITECTURE.md:223`, `:824`, `:1309`; `technical_docs/common-pitfalls.md:43` + the new
        row; `feature-flags.md:68`; `cloudevents-envelope.md:14-15`; `distributed-locks.md:103-106`;
        `opa-design.md:51`. Also `docs/` mkdocs nav gains the new feature page if
        `technical_docs/features/*.md` are listed there (Step 1 checks `mkdocs.yml`).
        **Verify**: `rg -n 'sys\.maxsize|2\*\*31|lowest priority|lowest possible priority' README.md ARCHITECTURE.md CLAUDE.md technical_docs docs` → 0 hits; `make docs` builds.

15. [x] `CHANGELOG.md` `[Unreleased]` — above the Plan 043/044 blocks: `### BREAKING — @Fallback
        framework defaults, providify >= 2.1.0 (Plan 045, DI-15 close-out / DI-3)` with §Migration's
        bullets verbatim; `### Fixed / Tests (Plan 045)` — four closure bullets (P24 "wiring fix
        shipped upstream; guard is now a passing test", P25/P26 "shipped; contract pinned; adopted
        by 047/046/051", P27 "shipped; adopted at N sites") each naming the report § and the new
        test id; the P28 filing bullet if applicable. Update `BACKLOG.md:67` (`DI-15` status →
        `done (Plan 043/044 filed; Plan 045 closed all four against providify 2.1.0)`) and `:76`
        (`DI-3` → `done (Plan 045)`; strike the interim `FRAMEWORK_DEFAULT_PRIORITY` clause with
        "never built — providify 2.1.0 shipped `@Fallback` first").
        **Verify**: `rg -n 'Plan 045' CHANGELOG.md BACKLOG.md` ≥ 3 hits.

16. [x] **Gates.** `make lint` (no-`PKG` path → `api-check`: expected *no change* since no
        top-level `__all__` moved; if it reports `setup_varco_defaults`, regenerate with
        `uv run python scripts/api_surface.py` and commit the snapshot in this step;
        `asyncapi-check` unchanged; `import-budget --warn-only` unchanged — `varco_core.di` is
        not imported by any `__init__`); `make type-check` (M1 drops by ≥ 3: the removed
        `attr-defined` ignores); `make test` (all eleven suites green, `0 xpassed`; `xfailed`
        count = 1 iff P28).

17. [x] **Handoff notes for the neighbours** (no file of their own — one paragraph each appended
        to `plans/000-index-4-0-di-standardization.md`'s 045 row, or to the neighbour plan's
        "Depends on" line if it already exists): 046 — the four `*_fallback_tier.py` seeds and
        `test_validate_clean_*` legs are to be folded into `di_wiring.py`; the redundant
        `install()`-after-scan pattern is now an `AMBIGUOUS_BINDING`. 047 — `BACKEND_PRIORITY`
        headroom; B3/B4's three-level shape; the two-backends tie. 050 — P24 is closed;
        `provide()` returns the binding. 051 — `get_all()` exclusion pinned at
        `test_providify_contracts.py::…get_all_excludes_shadowed_fallback`. 052 — §D-proof/P28.
        053 — `BaseAuthorizer` is `@Fallback @Singleton` with no constant; the posture message
        at `posture.py:764-767` was reworded. 058 — `dependency-injection.md` §Default bindings
        is final; §Migration below is the 4.0 guide's DI-3 entry.

## Edge cases

- **providify installed version ≠ 2.1.0 after `uv lock`** (a newer release appeared) → cite the
  installed tree, re-verify every line; if `_filter`'s post-step or `fallback_shadowed_issue`
  moved semantically, stop and update §Context before Step 7.
- **A converted guard fails for a reason other than the marker** (Step 5/6) → the shipped shape
  differs from the ask; fix the *test* to the shipped contract and record the delta in that
  report's Resolution §; never `xfail` it again.
- **`bind(Base, Impl)` on an undecorated `Impl`** (P25 body) raises `TypeError` → add
  `Singleton(Impl)`; providify's `register()` requires metadata (`container.py:1244-1245`),
  `bind()` reads it if present (`binding.py:112-152` per the P27 report §1).
- **Scanning `varco_redis` alone** (no `varco_core`) now finds no `AbstractTokenRevocationStore`
  default unless through `bootstrap()` (E2) → documented in §Migration; 046 makes `bootstrap()`
  the only entry point anyway.
- **A user registered their override at `priority=-sys.maxsize`** to sit "between" varco's tiers
  → it is a non-fallback and now simply wins (§Migration).
- **A user marked their own binding `@Fallback`** (copied a varco line) and also has varco's →
  both fallbacks; priority then order decides; `validate()` reports `AMBIGUOUS_BINDING` if
  injected at equal priority — the message names both. Documented in the Pitfalls table.
- **`get_all(AbstractEventBus)` in a core+kafka container** → `[InMemoryEventBus, KafkaEventBus]`
  (all-fallback set, ascending priority). With a user bus → `[UserBus]` only.
- **`RedisCacheConfiguration` + `RedisLayeredCacheConfiguration` both installed** (the P24 test)
  → two `CacheBackend` fallbacks tied at `BACKEND_PRIORITY`; `aget_all` returns both (the test's
  need); `get(CacheBackend)` first-installed; `validate()` → `AMBIGUOUS_BINDING` only where
  `CacheBackend` is injected unqualified — the P24 test asserts only the two disposer kinds.
- **Health-check order in `/health` changes** (Class C ⚠️) → registration order; if Step 1(a)
  found a test asserting order, sort by name in the test, not by re-adding a priority.
- **`FALLBACK_SHADOWED` count is zero for a scanned-class default** → §D-proof, expected; the
  P28 guard, not a plan failure.
- **`api-check` reports a change** → regenerate in Step 16 only if the diff is exactly the
  `setup_varco_defaults` removal; anything else is another branch's dirt.

## Verification

```bash
# Pin
rg -n 'providify>=' */pyproject.toml                        # 10 × >=2.1.0
uv run python -c "import importlib.metadata as m; print(m.version('providify'))"   # 2.1.0

# Floors gone
rg -n 'sys\.maxsize|2\*\*31' varco_*/varco_*                # 0
rg -n 'setup_varco_defaults|setup_defaults' varco_* examples README.md CLAUDE.md   # 0
rg -c '@Fallback' varco_*/varco_*                           # >= 55
rg -n 'GAP: P2[4-7]-' varco_*/tests/                        # 0
rg -c '^\| \*\*P2' UPSTREAM-GAPS.md                         # 0 (1 iff P28)

# Contracts + rule
uv run pytest varco_core/tests/test_providify_contracts.py -q -rxX
uv run pytest varco_core/tests/test_fallback_defaults.py varco_fastapi/tests/test_fallback_defaults.py -q -rxX
uv run pytest varco_kafka/tests/test_kafka_fallback_tier.py varco_nats/tests/test_nats_fallback_tier.py \
              varco_redis/tests/test_redis_fallback_tier.py varco_sa/tests/test_sa_fallback_tier.py -q
uv run pytest varco_fastapi/tests/test_validate_clean_default_container.py -q
uv run pytest varco_redis/tests/test_redis_cache_disposes.py -q -rxX

# Gates
make lint            # api-check / asyncapi-check / import-budget unchanged
make type-check
make test            # eleven suites, 0 xpassed
make docs
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — the installed 2.1.0 wheel matches the `3cb207f` tree line-for-line | Every `file:line` in §Context is true of what runs | Step 1(d) re-reads on the installed copy |
| ⚠️ ASSUMPTION — a default varco container has zero `WARNING`s at 2.1.0 (§D-validate) | The validate-clean test is an honest gate, not an allowlist | Step 1(a) baseline; any residue is an explicit `(kind, owner)` allowlist naming its owning plan |
| ⚠️ ASSUMPTION — `FALLBACK_SHADOWED` is *not* emitted for a scanned-class default shadowed at its ABC (§D-proof, read from `container.py:1248` + `:6360-6370`, not executed) | 052 knows which defaults its report can see | Step 1(e) executes it; Step 4 files P28 with a strict xfail iff confirmed |
| ⚠️ ASSUMPTION — a second `install()` of an already-scanned `@Configuration` registers a second binding set (§D-validate) | The two health tests do not trip `AMBIGUOUS_BINDING` | Step 1(e) counts bindings; the redundant `install()` is removed either way |
| ⚠️ ASSUMPTION — `container.scan("varco_kafka"/"varco_nats"/"varco_sa", recursive=True)` on a bare container is import-clean without a broker/DB | The Docker-free validate-clean legs exist for three backends | Step 1(e); a leg that needs a server is dropped with a note for 046 |
| ⚠️ ASSUMPTION — no test asserts `/health` entry order (Class C) | Removing the health-check floor changes only ordering, never membership | Step 1(a) `rg`; Edge cases row |
| ⚠️ ASSUMPTION — `bind(ABC, Impl)` honours `@Fallback` on `Impl` the same way `register()` does (`binding.py:207` reads the implementation; the P27 report §1 confirms `bind()` reads `@Singleton` metadata off the implementation) | Step 10's `container.bind(ABC, UserImpl)` overrides behave as specified | Step 5's converted P27 body uses exactly `bind()` and passes first |
| Two backends for one seam still tie (kafka + nats) | Not a regression; `AMBIGUOUS_BINDING` names it | Out of scope → 047; validate-clean covers one backend per seam |
| `MemcachedCacheConfiguration.memcached_cache` moves from the floor to `BACKEND_PRIORITY` — a user who relied on their own `CacheBackend` provider at a *negative* priority beating it | A non-fallback user binding wins at any priority | §Migration; the only way to lose now is to have written `@Fallback` |
| E1's optional-inject + `from_env()` differs from a *bound* settings object if an app binds settings **after** the DLQ was resolved | Singleton caching: first resolution wins, same as today | Docstring states "bind settings before the first `aget(AbstractDeadLetterQueue)`" — the not-evicted caveat, `scope.py:1048-1053` |
| E2's `bootstrap()` scanning a `varco_core` subpackage surprises a reader | One owner per default; nothing else in `varco_core.revocation` is scan-activated (`revocation/di.py` is `@Provider`-module-level, not scanned) | Docstring on `bootstrap()`; `test_redis_di.py` asserts the Null store resolves after `bootstrap()` alone |
| `make test` is red between Step 1(c) and Step 6 (six strict XPASSes) | Steps 1-6 land in one commit | Commit boundary stated in Step 1(f) |
| M1 (`type: ignore` count) moves | Directional only, never a gate | Step 16 records the delta (expected −3) |
| The DI guide's first version is read as complete | 058 completes it | The banner in its first paragraph; CLAUDE.md pointer says "default bindings" |

## Migration (3.x → 4.0, DI-3 / DI-15 entry — Plan 058 assembles)

**BREAKING — `setup_varco_defaults()` and `bootstrap(setup_defaults=)` are removed.** They bound
`TaskSerializer → DefaultTaskSerializer`; `VarcoFastAPIModule` now declares that default itself
(`task_serializer`, `@Fallback`), registered by `container.scan("varco_fastapi", …)` /
`varco_fastapi.di.bootstrap()`. Delete the call and the kwarg; nothing replaces them. Override as
before: `container.bind(TaskSerializer, MySerializer)` — now in any order.

**BREAKING — framework defaults are `@Fallback`, not priority floors.** `priority=-sys.maxsize - 1`,
`-sys.maxsize`, and `varco_core.auth.authorizer._FALLBACK_PRIORITY` (`-(2**31)`, removed) are gone
from every varco binding. Consequences:
- If you registered your own binding at a **negative priority** to sit between varco's tiers, or
  copied a varco decorator line including its priority: drop the priority. Any binding without
  `@Fallback` wins over every varco default regardless of order or priority.
- If you copied a varco line **including `@Fallback`**: your binding is now a fallback too and
  ties with varco's by priority, then registration order — `validate()` reports
  `AMBIGUOUS_BINDING`. Remove `@Fallback` from your own bindings.
- If you relied on `container.get_all(T)` / `InjectInstances[T]` **including** the varco default
  beside your own (e.g. to iterate "every cache"): a shadowed fallback is now excluded
  (`providify/container.py:1683-1686`). Resolve the default explicitly with
  `get(T, qualifier="in_memory")` if you need it.
- If you relied on "provide your override **before** `install()`/`scan()`" (`OtelConfig`,
  `AbstractDistributedLock`, the backend settings): still works; order no longer matters.
- If you read `container.validate()` reports: there is a new `Severity.INFO` tier
  (`FALLBACK_SHADOWED`, `CONDITION_INACTIVE`); `report.errors`/`.warnings` are unchanged, and
  `report.ok` still means "no errors". Code that treated every `report.issues` entry as a problem
  must filter by `severity`.
- Backend bindings (`KafkaEventBus`, `RedisEventBusSelectorConfiguration.bus`, the DLQ and cache
  providers, `SQLAlchemyRepositoryProvider`, `CasbinPolicyEngine`, …) are `@Fallback` at
  `varco_core.di.BACKEND_PRIORITY` (100): a scanned backend beats the in-process default, and
  your binding beats both. Two backends scanned for one seam still tie (unchanged; Plan 047).
- `varco_redis.revocation.RedisScanNullTokenRevocationStoreDefault` is removed;
  `varco_redis.di.bootstrap()` registers `varco_core`'s `NullTokenRevocationStore` instead. An
  app that scanned `varco_redis` **without** calling `bootstrap()` must now also scan
  `varco_core.revocation` (or call `bootstrap()`).
- `/health` lists varco's own probes in registration order instead of first (they are no longer
  at a priority floor). Membership is unchanged.
- `KafkaDLQConfiguration` / `RedisDLQConfiguration` / `RedisStreamDLQConfiguration` /
  `NatsDLQConfiguration` no longer register a second `*EventBusSettings` provider; they inject the
  package's settings binding when present and fall back to `from_env()` otherwise. Standalone
  `ainstall(<DLQ>)` on a bare container works as before.

**Dependency**: `providify>=2.1.0` (all ten packages).

## Docs / backlog updates

- `technical_docs/features/dependency-injection.md` — new (Step 14; §Default bindings only).
- `CLAUDE.md` — three lines in §"DI wiring (providify)" (Step 14).
- `README.md`, `ARCHITECTURE.md`, `technical_docs/common-pitfalls.md`, four feature docs — Step 14.
- `CHANGELOG.md`, `BACKLOG.md:67,76` — Step 15.
- `UPSTREAM-GAPS.md` + four report Resolution sections (+ P28 report if filed) — Steps 3-4.
- `design/api-freeze-and-standards/measurements/api-surface.*` — regenerated only if Step 16's
  `--check` reports the `setup_varco_defaults` removal.

## Done criteria

- [x] All ten `pyproject.toml`s pin `providify>=2.1.0`; `uv.lock` resolves 2.1.0.
- [x] `rg -n 'sys\.maxsize|2\*\*31' varco_*/varco_*` → 0; every Class A/B/D site carries
      `@Fallback`; every Class C site is a plain qualified `@Singleton`; `BACKEND_PRIORITY` is the
      only priority literal on a varco binding (`rg -n 'priority=' varco_*/varco_* | rg -v BACKEND_PRIORITY | rg -v 'tests/'` → only `@listen`/subscription priorities).
- [x] `setup_varco_defaults`, `bootstrap(setup_defaults=)`, `_FALLBACK_PRIORITY`,
      `RedisScanNullTokenRevocationStoreDefault`, the four DLQ settings copies, and the three
      imperative `Singleton(...)(cls)` stamps are gone.
- [x] `test_providify_contracts.py` and `test_redis_cache_disposes.py` pass with `0 xfailed`
      (`1 xfailed` iff P28) and `0 xpassed`; `rg -n 'GAP: P2[4-7]-' varco_*/tests/` → 0.
- [x] Four report files carry a Resolution section and `✅ Resolved` status; `UPSTREAM-GAPS.md`
      "Open gaps" has 0 (or exactly the P28) rows and four new "Recently closed" bullets.
- [x] The rule is proven both ways: "user binding registered AFTER the default wins" for the
      scanned-`@Singleton` and `@Provider`-in-`@Configuration` shapes (`varco_core`,
      `varco_fastapi`, and four backend tier tests), `FALLBACK_SHADOWED` INFO with `shadowed_by`
      for the provider shape, `qualifier="in_memory"` still resolving the in-process default.
- [x] `validate()` of the default varco container: `errors == ()`, `warnings == ()` (or an
      explicit, plan-attributed allowlist), `infos == ()`.
- [x] `technical_docs/features/dependency-injection.md` exists with the default-binding section
      and the 058 banner; CLAUDE.md has the three-line pointer; README/ARCHITECTURE/pitfall docs
      carry no floor prose.
- [x] CHANGELOG `[Unreleased]` has the BREAKING block (= §Migration) and the four closure bullets;
      BACKLOG `DI-15` and `DI-3` are `done (Plan 045)`.
- [x] `make lint`, `make type-check`, `make test`, `make docs` green; no file under
      `/home/edoardo/projects/providify` modified.
