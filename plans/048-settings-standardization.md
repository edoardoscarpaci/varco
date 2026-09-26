# Plan 048 — Settings standardization: one `bind_settings()` recipe, fail-fast at bootstrap, `VARCO_<FEATURE>_` everywhere, a generated `VARCO_*` reference (DI-6, DI-16)

Covers BACKLOG 4.0 rows **`DI-6`** (`BACKLOG.md:77`): *"every settings class a pydantic
`BaseSettings` with `VARCO_<FEATURE>_`, registered by one `@Provider` recipe, validated fail-fast
at bootstrap, with a documented code > env > default precedence; the `VARCO_*` reference table
regenerated from the classes"* — and **`DI-16`** (`BACKLOG.md:100`): *"Named multiple instances of
one integration (two Redis URLs, two Kafka clusters) via qualifiers + per-instance settings"*,
scoped to *one worked example and test, no general mechanism beyond what the recipe needs*.

**Neighbours (reference by number, never absorb):** 045 (pin `providify>=2.1.0` + `@Fallback` on
every framework default — **assumed landed**; this plan uses `@Fallback` and cites the 2.1.0 tree);
046 (one `bootstrap(container) -> DIContainer` contract, `bootstrap(streams=True)` stops mutating
`os.environ` — this plan supplies the mechanism 046 uses for that: an explicit
`RedisEventBusSettings(use_streams=True)` instance); 047 (`VARCO_<SEAM>_BACKEND` — **not** this
plan; a settings class never decides which backend is live); 049 (`VarcoModule.register()` calls
this plan's `bind_settings()`/`bind_core_settings()` — the recipe is a plain function of a
container, callable from anywhere); 052 (`varco diagnose` reads the provenance
`bind_settings()` records — this plan exposes it, 052 renders it); 058 (final docs; assembles this
plan's §Migration and §Docs).

**Research briefs backing this plan:**
[`015-starters-modules-and-configuration-binding.md`](../design/research/015-starters-modules-and-configuration-binding.md)
§2 (ASP.NET `AddOptions<T>().Bind().ValidateOnStart()` — fail-fast at startup, not first access;
named options via `.Configure<T>(name, …)`; precedence code > env > file; Micronaut
`@ConfigurationProperties(prefix=)` + `@EachProperty` — one bean per named config entry; the
"Convergence" paragraph: *"Validation at startup (fail-fast) is table stakes"*), §Evidence Gaps 4
(ASP.NET `ValidateOnStart` timing bug #56453 — validation must be *synchronous with registration*,
not "somewhere before the first request") and 6 (one precedence order vs per-setting — this plan
picks one), §Librarian's Note 5 (*"every backend's settings class is a pydantic `BaseSettings`
with `env_prefix="VARCO_<FEATURE>_"` … validated in `bootstrap()`, and documents all `VARCO_*` env
vars in README"*) and 6 (a disabled feature registers a Null impl, never raises);
[`014-multi-binding-qualifiers-and-conditional-di.md`](../design/research/014-multi-binding-qualifiers-and-conditional-di.md)
§3 (the `<prefix>.<feature>.enabled` convention — Quarkus `quarkus.<ext>.enabled`, Micronaut
`@Requires(property=)`; consumed here only as the naming rule for `*_ENABLED` fields, the
enable-precedence decision itself is 047's DI-12).

**providify source of record:** `/home/edoardo/projects/providify` at **2.1.0**
(`CHANGELOG.md` top section dated 2026-09-13). Every `providify/…` citation below was read in that
tree. The varco venv is still 2.0.1 until 045's bump; Step 0 re-confirms.

> ⚠️ **Three premises corrected from source before anything else was decided.**
> (1) CLAUDE.md's rule *"`CasbinSettings` is registered via `@Provider`, NOT `@Singleton` —
> providify cannot inject pydantic's `**values` constructor"* is **right in conclusion, wrong in
> reason** at 2.1.0: `resolve_params()` skips `VAR_POSITIONAL`/`VAR_KEYWORD` outright
> (`providify/_annotations.py:574-578`), so `**values` is not the trap. The real trap is that
> pydantic-settings' `BaseSettings.__init__` declares **29 annotated, defaulted keyword
> parameters** (`_case_sensitive: bool | None = None` … `_build_sources: … | None = None`,
> `.venv/…/pydantic_settings/main.py:180-210`) which providify treats as injection points: each is
> passed to `_resolve_hint_sync()` (`providify/container.py:3749`) and **silently left to its
> default only if unresolved** (`:3753-3759`). A class binding's configuration surface would then
> depend on whatever else the container happens to bind (`str`, `bool`, …). §D-provider restates
> the rule with the correct reason; CLAUDE.md's sentence is rewritten (Step 22).
> (2) providify's `@ConfigProperties`/`bind_config()` **does** accept a pydantic class
> (duck-typed `hasattr(cls, "model_validate")`, `providify/config.py:816-824`) — but its
> `EnvSource` nests on `"__"` and lower-cases every segment (`config.py:160,171`), and
> `_select_prefix()` picks a *subtree* (`:451`), so `@ConfigProperties(prefix="varco_redis")`
> reads `VARCO_REDIS__URL`, never `VARCO_REDIS_URL`; and the pydantic hand-off calls
> `cls.model_validate(subtree)` (`:824`), which **bypasses pydantic-settings' own source chain**
> (env/dotenv/secrets/aliases/`env_nested_delimiter`). It is also lazy — *"raises
> `ConfigBindingError` at first resolution, not at `bind_config()` call time … `validate()` does
> not instantiate anything"* (`container.py:1429-1435`). §D-not-configproperties rejects it with
> ✅/❌.
> (3) The scout's "19 settings classes, three shapes" undercounts. From source: **29 pydantic
> `BaseSettings` subclasses** (27 via `VarcoSettings`, 2 direct) plus **8 frozen dataclasses with
> a hand-rolled `from_env()`** and **11 loose `os.environ.get("VARCO_…")` reads** outside any
> class — **four** registration shapes, not three (§Context inventory).

> **Handoff from Plan 046 (landed):** the casbin `is_user_bound(CasbinSettings)` guard (`varco_casbin/varco_casbin/di.py`) and the redis `bind_instance(container, RedisEventBusSettings, RedisEventBusSettings(use_streams=True))`
> docs (`varco_redis/varco_redis/di.py`, README.md, `technical_docs/features/dependency-injection.md`) are the two call sites 048's `bind_settings()` replaces. `varco_core.di.AlreadyBoundError` is available for a `SettingsAlreadyBoundError` to subclass.

## Goal

After this plan: every varco settings object that reads the environment is a pydantic
`VarcoSettings` subclass with a `VARCO_<FEATURE>_` prefix, registered into a container by exactly
one function — `varco_core.settings.bind_settings(container, Cls, *, instance=None,
qualifier=None, env_prefix=None)` — which constructs it **once, immediately, at the call**
(fail-fast with an error that names the offending env var), registers it as a `@Fallback`
singleton (or a non-fallback one when `instance=` is given), and records its provenance
(`code`/`env`/`default`, per field) for 052. Precedence is **per field**: explicit `instance=`
kwargs > process environment > class default; `.env` files are never read by varco. Two instances
of one settings class coexist under qualifiers (`qualifier="replica",
env_prefix="VARCO_REDIS_REPLICA_"`), demonstrated once. `scripts/settings_reference.py`
introspects every registered class (package list executed from `scripts/packages.sh`), writes
`technical_docs/settings-reference.md` + a JSON snapshot, and `--check` gates drift in `make
lint`'s no-`PKG` path and CI.

## Non-goals

- **Which backend is selected** (`VARCO_<SEAM>_BACKEND`, `@Requires(env=…)`, dotted-path
  overrides) — 047. `RedisEventBusSettings.use_streams` stays a *field*; who reads it is 047's.
- **The `enable_*` vs `*_ENABLED` precedence decision and the on/off matrix** — 047 / DI-12. This
  plan only fixes the *name shape* (`VARCO_<FEATURE>_ENABLED`) where a class already has such a
  field.
- **`VarcoModule` / `create_varco_app(modules=[…])`** — 049. This plan ships
  `bind_core_settings(container)` and per-package `bind_settings()` calls inside today's
  `bootstrap()` bodies; 049 moves those calls into `register()` unchanged.
- **Rendering provenance** — 052. This plan stores `SettingsProvenance` on the container-side
  record; 052 prints it.
- **The `ConnectionSettings` family's prefixes** (`POSTGRES_`, `REDIS_`, `KAFKA_`, `NATS_`,
  `HttpConnectionSettings.from_env(prefix=)`) — deliberately **not** renamed and **not**
  registered (§D-connection).
- **`SAConfig` / `BeanieSettings`** — wiring bundles holding a live engine/client
  (`varco_sa/varco_sa/config.py:78`, `varco_beanie/varco_beanie/config.py:75`), not
  env-representable; 049's modules take them as constructor arguments.
- **`OtelConfig`** — code-only by design (`observability/config.py:17` *"No automatic env-var
  reading"*); the `VARCO_OTEL_*` loose reads stay allowlisted (§D-loose).
- **Removing the nine `removed_in="4.0.0"` sites** — 058.
- **Hot reload of settings** (ASP.NET `IOptionsMonitor`) — brief 015 §Evidence Gap 1; parked.
- **Flip-track default changes** on converted classes (`enforce_rls` — 056; `on_missing_claim` —
  055; `include_detail` — 054). This plan converts *class shape only*; every default literal is
  byte-identical (§Risks row 3).

## Context

### Inventory — every settings-like class, from source (2026-09-14)

Shape legend: **P-mod** = module-level scanned `@Provider` at a priority floor; **P-cfg** =
`@Provider` method inside a scanned `@Configuration`; **P-boot** = `@Provider` registered by
`bootstrap()`; **hand** = `settings or Cls()` inside a consumer's `__init__` (never in DI);
**explicit** = always passed by the app. "Validated" = when a bad env value first raises today.

| # | Class | Defined | Prefix | Registered today | Validated today |
|---|---|---|---|---|---|
| 1 | `VarcoSettings` (base) | `varco_core/varco_core/config.py:63` | — | — | — |
| 2 | `EventBusSettings` (base) | `varco_core/varco_core/event/config.py:31` | *(none, `:62`)* | never alone | — |
| 3 | `CacheSettings` | `varco_core/varco_core/cache/config.py:39` | `VARCO_CACHE_` | hand (`cache/memory.py:205`) | first `InMemoryCache()` |
| 4 | `IdempotencySettings` | `varco_core/varco_core/idempotency/settings.py:21` | `VARCO_IDEMPOTENCY_` | hand (middleware) | first middleware construction |
| 5 | `WebhookSettings` | `varco_core/varco_core/webhook/settings.py:24` | `VARCO_WEBHOOK_` | hand (`webhook/dispatcher.py:198`, `inbound/verifiers.py:590`) | first dispatcher/verifier |
| 6 | `CloudEventsSettings` | `varco_core/varco_core/event/cloudevents.py:157` (**direct `BaseSettings`**) | `VARCO_CLOUDEVENTS_` | explicit (`bind_cloudevents_serializer`) — `source` is **required** (`:183`) | at the explicit call |
| 7 | `ErrorEnvelopeSettings` | `varco_core/varco_core/exception/settings.py:24` (`extra="ignore"`) | `VARCO_ERROR_` | hand ×6 (`exception/http.py:296`, `varco_fastapi/exceptions.py:119`, `middleware/introspect.py:150,355`, `redaction/posture.py:146`) | first error rendered |
| 8 | `JwtVerificationSettings` | `varco_core/varco_core/jwt/config.py:27` | `VARCO_JWT_` | hand, **re-read per call** (`authority/registry.py:906,943,965,1016`, `jwt/parser.py:185`, `varco_fastapi/auth/server_auth.py:203`) | first `verify()` |
| 9 | `JwtTransformSettings` | `varco_core/varco_core/jwt/transform/config.py:89` | `VARCO_JWT_TRANSFORM_` (`_FLAT_PREFIX`, `:46`) | P-cfg (`varco_fastapi/di.py:348`) | first resolve |
| 10 | `I18nSettings` | `varco_core/varco_core/i18n/settings.py:16` | `VARCO_I18N_` | P-cfg (`varco_fastapi/di.py:393`) + hand (`app.py:453`, `middleware/localization.py:99`) | first resolve |
| 11 | `TimezoneSettings` | `varco_core/varco_core/tz/settings.py:18` | `VARCO_TZ_` | P-cfg (`di.py:398`) + hand (`app.py:454`) | first resolve |
| 12 | `ConnectionSettings` (base) + `PostgresConnectionSettings` / `RedisConnectionSettings` / `KafkaConnectionSettings` / `NatsConnectionSettings` / `HttpConnectionSettings` | `varco_core/varco_core/connection/base.py:69`; `varco_sa/varco_sa/connection.py:81`; `varco_redis/varco_redis/connection.py:68`; `varco_kafka/varco_kafka/connection.py:71`; `varco_nats/varco_nats/connection.py:71`; `varco_fastapi/varco_fastapi/connection.py:89` | `POSTGRES_` / `REDIS_` / `KAFKA_` / `NATS_` / caller-supplied (`:161-199`) | explicit, never DI-bound (grep: no `di.py`/`app.py` reference) | at the app's call — **excluded**, §D-connection |
| 13 | `RedisEventBusSettings` | `varco_redis/varco_redis/config.py:59` | `VARCO_REDIS_` | P-mod (`:140`, `-sys.maxsize`) **and** P-cfg duplicates (`dlq.py:740`, `stream_dlq.py:766`, both `-sys.maxsize-1`) + hand ×6 (`lock.py:186`, `bulkhead.py:220`, `rate_limit.py:231`, `deduplication.py:146`, `dlq.py:209`, `stream_dlq.py:236`) | first resolve |
| 14 | `RedisCacheSettings` | `varco_redis/varco_redis/cache.py:63` | `VARCO_REDIS_CACHE_` | P-cfg (`:576`) + hand (`:183`) | first resolve |
| 15 | `LayeredCacheSettings` | `varco_redis/varco_redis/cache.py:634` | `VARCO_LAYERED_CACHE_` | P-cfg (`:796`) | first resolve |
| 16 | `RedisBackplaneSettings` | `varco_redis/varco_redis/backplane.py:106` (**direct `BaseSettings`**) | `VARCO_REDIS_CACHE_BACKPLANE_` | P-mod (`:134`) + hand (`:185`) | first resolve |
| 17 | `KafkaEventBusSettings` | `varco_kafka/varco_kafka/config.py:111` | `VARCO_KAFKA_` | P-mod (`:260`) + P-cfg duplicate (`dlq.py:785`) + hand (`dlq.py:225`) | first resolve |
| 18 | `KafkaChannelManagerSettings` | `varco_kafka/varco_kafka/channel.py:73` | `VARCO_KAFKA_ADMIN_` | P-mod (`:154`) | first resolve |
| 19 | `NatsEventBusSettings` | `varco_nats/varco_nats/config.py:111` | `VARCO_NATS_` | P-mod (`:341`) + P-cfg duplicate (`dlq.py:772`) + hand (`dlq.py:206`) | first resolve |
| 20 | `NatsChannelManagerSettings` | `varco_nats/varco_nats/channel.py:84` | `VARCO_NATS_ADMIN_` | P-mod (`:192`) | first resolve |
| 21 | `MemcachedCacheSettings` | `varco_memcached/varco_memcached/cache.py:73` | `VARCO_MEMCACHED_CACHE_` | P-cfg (`:645`) + hand (`:205`) | first resolve |
| 22 | `CasbinSettings` | `varco_casbin/varco_casbin/config.py:44` | `VARCO_CASBIN_` | P-boot (`varco_casbin/di.py:64`) | first resolve |
| 23 | `SecurityPostureSettings` | `varco_fastapi/varco_fastapi/posture.py:154` | `VARCO_SECURITY_` (+ alias `VARCO_SECURITY_ENV`, `:189`) | hand (`:291`) | lifecycle construction |
| 24 | `SecurityHeadersSettings` | `varco_fastapi/varco_fastapi/middleware/security_headers.py:109` | `VARCO_SECURITY_HEADERS_` | hand (`:204`, `app.py:710`, `introspect.py:231`) | app construction |
| 25 | `BodyLimitSettings` | `varco_fastapi/varco_fastapi/middleware/body_limit.py:93` | `VARCO_BODY_LIMIT_` | hand (`:190`, `app.py:662`, `introspect.py:260`) | app construction |
| 26 | `RateLimitSettings` | `varco_fastapi/varco_fastapi/middleware/rate_limit.py:236` | `VARCO_RATE_LIMIT_` | hand (`:352`, `introspect.py:294`) | middleware construction |
| 27 | `ProfilingSettings` | `varco_fastapi/varco_fastapi/middleware/profiling.py:63` | `VARCO_PROFILER_` ⚠️ vs the `VARCO_PROFILING_ENABLED` kill switch (`varco_core/profiling/config.py:33`) | P-cfg (`di.py:332`) + hand (`:125`, `app.py:1166`) | first resolve |
| **Dataclasses with hand-rolled `from_env()`** | | | | | |
| 28 | `MigrationSettings` | `varco_core/varco_core/migration/settings.py:35` | `VARCO_MIGRATE_` (`:85-105`) | hand (`varco_fastapi/app.py:380`, `migrate.py:61`, `varco_sa/migration/migrator.py:155`, `varco_beanie/migration/migrator.py:107`) | `ValueError` at first `from_env()` |
| 29 | `TenancySettings` | `varco_core/varco_core/tenancy/settings.py:95` | `VARCO_TENANCY_` (`:191-225`) | hand (`TenancyLifecycle`); `app.py:412` reads `VARCO_TENANCY_ISOLATION` **directly** | first `from_env()` |
| 30 | `TenantProvenanceSettings` | `varco_core/varco_core/tenancy/settings.py:237` | `VARCO_TENANT_` (`:315-367`) | hand (`:401`) | first `from_env()` |
| 31 | `TenantMembershipSettings` | `varco_core/varco_core/tenancy/membership.py:93` | `VARCO_TENANT_MEMBERSHIP_` (`:149,157`) | hand (`tenancy/di.py:103`) | first `from_env()` |
| 32 | `AuthorizationConfig` (+ `IssuerConfig`) | `varco_core/varco_core/authority/config.py:95` | **`FASTREST_AUTHORIZATION__<LABEL>__{URL,ISS}`** (`:63`) — a non-`VARCO_` legacy prefix, mirrored at `jwt/transform/config.py:56` | hand (`authority/registry.py:1200`, `authority/__init__.py:47`) | first `from_env()` |
| 33 | `CORSConfig` | `varco_fastapi/varco_fastapi/middleware/cors.py:40` | `VARCO_CORS_` (`:140-174`) | P-cfg (`di.py:299`) + hand (`app.py:715,1190`, `middleware/__init__.py:162`) | first resolve |
| 34 | `ClientProfile` | `varco_fastapi/varco_fastapi/client/base.py:65` | `VARCO_CLIENT_` (`:211-214`) | P-cfg (`di.py:309`) | first resolve |
| 35 | `TrustStore.from_env()` | `varco_core/varco_core/tls/store.py:174` (+ deprecated shim `varco_fastapi/auth/trust_store.py:140`) | **`VARCO_CA_CERT`, `VARCO_CLIENT_CERT`, `VARCO_CLIENT_KEY`, `VARCO_TRUST_STORE_DIR`** (`:205-210`) — no feature prefix; `VARCO_CLIENT_CERT`/`_KEY` **collide with #34's `VARCO_CLIENT_` prefix** | P-cfg (`di.py:288`) | first resolve |
| **Label-keyed families (kept, documented as patterns)** | | | | | |
| 36 | `PeerConfig.from_env(name)` | `varco_fastapi/varco_fastapi/client/peer.py:130` | `VARCO_PEER_<NAME>_*` (`:43`) | explicit (`bind_peers`) | at the call |
| 37 | `TokenProfileRegistry.from_env()` | `varco_core/varco_core/jwt/profile.py:275` | `VARCO_JWT_PROFILE__<NAME>__*` (`:47`) | explicit | at the call |
| 38 | `JwtTransformConfig.from_env()` | `varco_core/varco_core/jwt/transform/config.py:243` | `VARCO_JWT_TRANSFORM__<LABEL>__*` (`:51`) + #9 | P-cfg (`di.py:369`) | first resolve |

**Loose `VARCO_*` reads outside any class (11):** `observability/params.py:669,676`
(`VARCO_OTEL_CAPTURE_PARAMS[_EXCLUDE]`), `observability/attributes.py:420+`
(`VARCO_OTEL_GLOBAL_ATTRS*`), `profiling/config.py:33` (`VARCO_PROFILING_ENABLED`, **import-time**),
`authority/registry.py:227,234` (`VARCO_JWKS_MIN_REFRESH_SECONDS`/`VARCO_JWKS_TTL_SECONDS`),
`tenancy/control/service.py:145,148` (`VARCO_TENANCY_NODE_ID`/`STORE_ID`),
`varco_fastapi/app.py:412` (`VARCO_TENANCY_ISOLATION`), `app.py:865`
(`VARCO_LIFECYCLE_DISCOVERY_WARN`), `auth/server_auth.py:435` (`VARCO_API_KEY_PEPPER`),
`cli/tenant.py:71` + `cli/retention.py:66-76` (CLI defaults). §D-loose decides each.

**What is wrong, concretely:** four shapes; three packages register the *same* settings class
twice at two different floors (rows 13/17/19 — with 045's `@Fallback` on both, an equal-priority
pair reports `AMBIGUOUS_BINDING`, `providify/decorator/scope.py:1042-1047`); every "hand" row
validates at first use, and row 8 re-parses the environment on every token verification; two
prefixes collide outright (rows 34/35) and two name one feature two ways (row 27); one prefix is
not `VARCO_` at all (row 32); no file lists every `VARCO_*` name (README has 67 hand-written rows
across per-feature tables that already disagree with source in places).

### providify 2.1.0 facts this plan rides on

| Primitive | Where | Used for |
|---|---|---|
| `resolve_params()` skips `*args`/`**kwargs` | `providify/_annotations.py:574-578` | §D-provider (the corrected reason) |
| Unresolved annotated param **with a default** is silently left to the default | `providify/container.py:3753-3759` | §D-provider — why a class binding on `BaseSettings` is fragile |
| `provide(fn, *, returns=…) -> ProviderBinding`; `returns=` overrides the annotation, which is *not read* | `container.py:1250-1266` | The recipe's closure is `-> Any` + `returns=cls` (no quoted-annotation pitfall) |
| `@Provider(qualifier=, priority=, singleton=, returns=)` | `providify/decorator/scope.py:497-511` | Qualified settings (DI-16) |
| `@Fallback` marker on classes **and** provider functions; resolve-time; shadowed → `FALLBACK_SHADOWED` at `Severity.INFO` | `scope.py:976-1053`; marker read `metadata.py:867-879` (`__dict__`-only) | The env-backed provider is a fallback; an `instance=` provider is not |
| Two fallbacks tied at equal priority → `AMBIGUOUS_BINDING` | `scope.py:1042-1047` | Why one registration per `(cls, qualifier)` per container (idempotency guard) |
| Not-evicted caveat: a fallback singleton cached before its shadower arrives stays cached | `scope.py:1048-1053` | §D-precedence ordering rule |
| `get_all_bindings(interface, *, qualifier=None)` — pure read, `[]` when none, registration order | `container.py:7464-7491` | Idempotency guard |
| `get()` on first call runs `validate_bindings()` — **scope-leak checks only**, no resolvability pass | `container.py:5876-5900` | Calling `container.get(cls)` mid-bootstrap is safe |
| `warm_up(qualifier=, priority=)` — all singletons, all-or-nothing on async providers | `container.py:1498-1548` | Rejected for eager settings (§D-eager) |
| `bind_config()` lazy; `validate()` never instantiates | `container.py:1408-1466` | §D-not-configproperties |
| `EnvSource` `"__"` nesting, lower-casing; `_select_prefix` subtree | `providify/config.py:129-190, 451` | §D-not-configproperties |
| pydantic-settings 2.14.2 (`uv.lock:2198`): `__init__(_env_prefix=…)` overrides `model_config["env_prefix"]` | `.venv/…/pydantic_settings/main.py:184, :307` | DI-16's per-instance prefix, one kwarg |

## Design

### §D-provider — a `@Provider` closure, never a class binding; the rule stays, the reason changes

**Chosen:** `bind_settings()` builds a zero-argument provider closure per call and registers it
with `container.provide(fn, returns=cls)`. No settings class is ever `@Singleton`-decorated.

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) `@Provider` closure via `provide(fn, returns=cls)`** | The closure has *no* parameters, so providify's parameter resolver never sees pydantic-settings' 29 underscore kwargs (`main.py:180-210`) — construction is fully under pydantic-settings' control. `returns=cls` means the annotation is never evaluated (`container.py:1265-1266`) — immune to the quoted-annotation failure documented at `varco_fastapi/di.py:316-327`. Works for a `@Fallback`-marked *and* an unmarked closure alike (`scope.py:998-1005`). Same shape 049's `register()` can call | A closure per `(cls, qualifier)`; `describe()` shows a generated name — mitigated by setting `fn.__qualname__ = f"varco.settings:{cls.__name__}[{qualifier}]"` |
| **(2) `@Singleton` on the class** | One decorator, scan-discoverable | `_resolve_hint_sync()` runs for every annotated defaulted kwarg (`container.py:3749`); an app that binds `str` or `bool` (or `Path`, or any union member) silently injects into `_env_prefix`/`_env_file`/`_secrets_dir` (`:3753-3761`). Also violates the "no scanned `@Singleton` in `varco_core`" locked decision for the eleven core classes. **Rejected** — CLAUDE.md's conclusion stands |
| **(3) providify `@ConfigProperties` + `bind_config()`** | Upstream-native, visible to `describe()`/`validate()` | §D-not-configproperties |

The CLAUDE.md sentence becomes: *"`CasbinSettings` (and every settings class) is registered via
`bind_settings()`'s `@Provider` closure, never `@Singleton` — pydantic-settings' `__init__` carries
29 annotated defaulted keyword parameters that providify treats as injection points
(`_annotations.py:574-578` skips only `**values`; `container.py:3753-3759` fills the rest from
whatever is bound)"*.

### §D-not-configproperties — providify's `ConfigProperties` is usable for pydantic, and still the wrong tool here

| | ✅ | ❌ |
|---|---|---|
| `@ConfigProperties(prefix="varco_redis") class RedisEventBusSettings(BaseSettings)` + `container.bind_config()` | Duck-typed pydantic hand-off exists (`config.py:816-824`); scan-discoverable (`scanner.py:158`); one upstream primitive instead of a varco helper; YAML/TOML/JSON sources for free | **Env-var shape changes for every variable**: `EnvSource` nests on `"__"` (`config.py:160,171`) and `_select_prefix()` selects the `varco_redis` *subtree* (`:451`) → `VARCO_REDIS__URL`, not `VARCO_REDIS_URL`; `EnvSource(delimiter="_")` would split `VARCO_REDIS_CHANNEL_PREFIX` into four levels. **~120 documented names would all move** for no user-visible gain. The pydantic path calls `model_validate(subtree)` (`:824`), which **skips pydantic-settings' own source chain** — `validation_alias` (`SecurityPostureSettings.environment`, `posture.py:189`), `env_nested_delimiter="__"` on `ConnectionSettings` (`connection/base.py:111`), `_env_prefix` per-instance override (DI-16) all stop working; varco would run two config engines. Lazy by design (`container.py:1429-1435`) — fail-fast still needs a separate eager step. Its `ConfigBindingError` wraps the pydantic error as one `<root>` string (`:826-828`) — the env-var-naming message §D-eager requires would have to be re-derived. **Rejected** |

Recorded as *not* an upstream gap: nothing is missing in providify; the two tools solve different
problems (providify: "bind a config tree to a plain class"; varco: "keep pydantic-settings as the
engine and make registration + validation uniform"). No `design/upstream-gaps/` report.

### §D-eager — fail-fast is `container.get()` inside `bind_settings()`, not `warm_up()`

**Chosen:** after registering the provider, `bind_settings()` immediately calls
`container.get(cls, qualifier=qualifier)` and returns the instance. If pydantic raises
`ValidationError`, it is re-raised as `SettingsValidationError` whose message lists, per error,
the **env var name** (`prefix + FIELD.upper()`, or the alias, or `PREFIX + "OUTER__INNER"` for a
nested model — derived by `varco_core.settings.inventory.env_vars_for(cls)`), the pydantic message,
and the raw value's *presence* (never its value — a secret must not land in a traceback).

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) `get()` at bind time** | Synchronous with registration — the exact property brief 015 §Evidence Gap 4 says ASP.NET's `ValidateOnStart()` lacks (#56453). Goes *through* the container: if a non-fallback user binding for `(cls, qualifier)` exists, providify's post-filter drops our fallback (`scope.py:979-986`) and `get()` returns the user's instance — **code > env holds with no marker inspection**, and the environment is not even read. `get()`'s implicit `validate_bindings()` is scope-leak-only (`container.py:5876-5900`), safe mid-bootstrap. Returns the instance so `bootstrap()` bodies can use it (`bus = RedisEventBus(settings)`) | Constructs at registration; a user who registers a code override *after* `bootstrap()` finds the env-built fallback already cached and consumed by anything built during bootstrap (the not-evicted caveat, `scope.py:1048-1053`). Rule stated in §D-precedence: **register overrides before `bootstrap()`** — 046's contract makes that the natural order |
| **(2) `container.warm_up()` at the end of bootstrap** | No per-class code | Instantiates *every* singleton (buses, caches, engines) and raises on any async provider (`container.py:1519-1522`); no interface filter (`:1498-1502`) — a settings-only warm-up is impossible without a fake qualifier/priority convention. **Rejected** |
| **(3) Lazy providers + a `validate_settings(container, classes)` list** | Matches providify's lazy model | Two shapes (a provider *and* a list) — the exact drift DI-6 removes; the list rots. **Rejected** |

### §D-precedence — per field: `instance=` kwargs > environment > class default; overrides before bootstrap

pydantic-settings already merges *per field*: init kwargs > env > (dotenv, secrets — disabled
here) > field default. The recipe adds exactly one layer above that — the container's own
binding precedence — and states the whole chain once:

```
container.get(Cls)                     resolved by providify
  ├─ a non-fallback binding of Cls     ← code: bind_settings(c, Cls, instance=obj)
  │                                      or any user @Provider/@Singleton of Cls
  └─ else the @Fallback binding         ← bind_settings(c, Cls)  → Cls(_env_prefix=…)
       └─ per field, inside pydantic-settings:
            instance kwargs (only on the code path) > VARCO_X_FIELD > class default
```

- `instance=Cls(url="…")` is **still env-aware for the other fields** (pydantic reads
  `VARCO_X_CHANNEL_PREFIX` while constructing it). That is deliberate and documented: "code wins
  per field, not per object". An app that wants a fully env-independent object uses
  `Cls.from_dict({...})` (`config.py:143-168`, `model_validate` bypasses env) as the `instance=`.
- **Ordering rule** (from the not-evicted caveat): register code overrides *before* the
  `bootstrap()` that consumes them. `bind_settings()` on the code path raises
  `SettingsAlreadyBoundError` if a non-fallback binding of the same `(cls, qualifier)` already
  exists — a second code override is a bug, not a precedence question; tests replace a binding
  with providify's `di_overrides` (CLAUDE.md's RL-3 rule), never a second `bind_settings()`.
- **Provenance**, per field, recorded on the returned record for 052: `"code"` if the field was
  set in `instance.model_fields_set`, else `"env"` if its env var name was present in `os.environ`
  at construction, else `"default"`. Exposed as `varco_core.settings.provenance_of(container, Cls,
  qualifier=None) -> SettingsProvenance` (frozen dataclass: `cls`, `qualifier`, `env_prefix`,
  `fields: Mapping[str, Literal["code","env","default"]]`, `source: Literal["fallback","instance","external"]`).
  `"external"` = the live binding is a user-registered one this recipe never saw; fields then
  report `"code"` uniformly.

### §D-dotenv — varco never reads a `.env` file; the application loads one into `os.environ` first

- No `VarcoSettings` subclass sets `env_file`/`secrets_dir` (grep today: zero hits) — made a
  **tested invariant** (Step 3's `test_no_settings_class_declares_env_file`).
- Rule (README + guide): *"varco reads the process environment only. To use a `.env` file, call
  `dotenv.load_dotenv()` (or your platform's equivalent) before `bootstrap()`. One source of
  truth means `varco diagnose` (052) can say `VARCO_REDIS_URL: env` and be right."*
- ✅ A stray `.env` in a test's cwd can never change a test; per-class `env_file` drift is
  impossible; provenance stays two-valued. ❌ Users who expect pydantic-settings' `env_file` must
  add one line. Rejected alternative — `VarcoSettings.model_config = SettingsConfigDict(env_file=".env")`
  base-wide: silently honours a file the operator did not mention, and 052 could no longer
  distinguish env from file.

### §D-prefix — one prefix per class, `VARCO_<FEATURE>_`, full-name uniqueness across the whole tree

Rules, all mechanically checked by `scripts/settings_reference.py --check` and by
`varco_core/tests/test_settings_inventory.py`:

1. Every registered class's `env_prefix` starts with `VARCO_` and ends with `_`.
2. **Full env-var names are unique across all classes** (`VARCO_SECURITY_` and
   `VARCO_SECURITY_HEADERS_` may both exist because no field of `SecurityPostureSettings` is
   named `headers_*`; the check is on the *expanded* names, not on prefix-disjointness — so no
   rename of `VARCO_SECURITY_*`, which 053/054 reference).
3. A boolean opt-in field is named `enabled` → `VARCO_<FEATURE>_ENABLED` (brief 014 §3).
4. Label-keyed families (`VARCO_PEER_<NAME>_*`, `VARCO_JWT_PROFILE__<NAME>__*`,
   `VARCO_JWT_TRANSFORM__<LABEL>__*`, `VARCO_AUTHORIZATION__<LABEL>__*`) are documented as
   **patterns** in a separate table section; they are not `bind_settings()` targets.
5. Classes excluded from registration must appear in `_EXCLUDED_SETTINGS` (`inventory.py`) with a
   one-line reason; the test asserts *every* `VarcoSettings`/`BaseSettings` subclass importable
   from the ten packages is either registered by some `bind_*_settings()` list or excluded — an
   unlisted class fails the build.

**Renames (breaking, §Migration):**

| Today | 4.0.0 | Why |
|---|---|---|
| `FASTREST_AUTHORIZATION__<LABEL>__URL` / `__ISS` | `VARCO_AUTHORIZATION__<LABEL>__URL` / `__ISS` | Rule 1; the only non-`VARCO_` prefix (`authority/config.py:63`, `jwt/transform/config.py:56`) |
| `VARCO_PROFILER_*` | `VARCO_PROFILING_*` | Rule 2's spirit — one feature, one prefix; matches the package `varco_core.profiling` and the existing `VARCO_PROFILING_ENABLED` kill switch |
| `VARCO_CA_CERT`, `VARCO_CLIENT_CERT`, `VARCO_CLIENT_KEY`, `VARCO_TRUST_STORE_DIR` | `VARCO_TLS_CA_CERT`, `VARCO_TLS_CLIENT_CERT`, `VARCO_TLS_CLIENT_KEY`, `VARCO_TLS_TRUST_STORE_DIR` (new `TlsSettings`) | Rule 2 — `VARCO_CLIENT_CERT` sits inside `ClientProfile`'s `VARCO_CLIENT_` prefix today (a real collision) |
| `VARCO_JWKS_MIN_REFRESH_SECONDS`, `VARCO_JWKS_TTL_SECONDS` | `VARCO_JWT_JWKS_MIN_REFRESH_SECONDS`, `VARCO_JWT_JWKS_TTL_SECONDS` (fields on `JwtVerificationSettings`) | Rule 1 (feature = JWT); removes two loose reads (`registry.py:227,234`) |
| *(none)* `VARCO_TENANCY_NODE_ID`, `VARCO_TENANCY_STORE_ID` | same names, now fields of `TenancySettings` | Not a rename — removes two loose reads (`control/service.py:145-148`) |

**Kept as-is, with reason:** `VARCO_MIGRATE_` (a feature name; churn without benefit),
`VARCO_TENANCY_`/`VARCO_TENANT_`/`VARCO_TENANT_MEMBERSHIP_` (055 owns their semantics; full-name
uniqueness holds), `VARCO_SECURITY_*` (053/054 reference them), `VARCO_CORS_`, `VARCO_CLIENT_`,
`VARCO_REDIS_`/`VARCO_REDIS_CACHE_`/`VARCO_REDIS_CACHE_BACKPLANE_` (nested prefixes, unique names).

### §D-convert — which dataclasses become `VarcoSettings`, and which do not

Convert (rows 28-35): `MigrationSettings`, `TenancySettings`, `TenantProvenanceSettings`,
`TenantMembershipSettings`, `AuthorizationConfig` (prefix rename), `CORSConfig`, `ClientProfile`,
and a **new** `TlsSettings` (`varco_core/tls/settings.py`, `VARCO_TLS_`) that `TrustStore.from_env()`
delegates to. Each keeps its class name, field names, and every default literal; loses the
`from_env(env: Mapping | None)` parameter (used only by tests — grep: `varco_core/tests/
test_tenancy_settings.py:39-143`, `test_tenant_membership_di.py:103,126`,
`test_migration_contracts.py:19`; tests move to `monkeypatch.setenv` or `Cls(field=…)`). Frozen via
`SettingsConfigDict(frozen=True)`. Comma-separated fields use a shared
`CommaSeparated = Annotated[tuple[str, ...], NoDecode, BeforeValidator(_split_csv)]`
(`varco_core/settings/types.py`) so pydantic-settings' JSON-decoding of `list`/`tuple` never
fires (`VARCO_CORS_ORIGINS=https://a,https://b` must keep working).

Not converted: `ConnectionSettings` family (§D-connection), `SAConfig`/`BeanieSettings`/`OtelConfig`
(Non-goals), `TrustStore` itself (a value object with non-env fields — it gains `from_settings()`),
`TokenProfileRegistry`/`JwtTransformConfig`/`PeerConfig` (label-keyed families; their flat halves —
`JwtTransformSettings`, `ClientProfile` — are registered).

### §D-connection — the `ConnectionSettings` family stays outside the recipe

`POSTGRES_HOST`, `REDIS_HOST`, `KAFKA_BOOTSTRAP_SERVERS`, `NATS_SERVERS` describe a **third-party
service**, in the names that service's own tooling, docker-compose files and Kubernetes secrets
already emit; `HttpConnectionSettings` *requires* a caller prefix (`connection.py:198`). None is
DI-bound; each is constructed by the app to build a DSN. Renaming them to `VARCO_POSTGRES_HOST`
buys prefix consistency at the price of every deployment's secret names, for objects the recipe
never touches. ✅ Zero churn; documented as "connection fragments" in their own table section of
the generated reference. ❌ The hazard CLAUDE.md's test-fixture rule names — a bare `REDIS_URL`
in a dev shell silently honoured — remains for these four; recorded in the reference page's
header and in `technical_docs/common-pitfalls.md` as a new row.

### §D-named — DI-16 is one kwarg on the recipe, demonstrated once

`bind_settings(container, RedisEventBusSettings, qualifier="replica",
env_prefix="VARCO_REDIS_REPLICA_")` → the closure calls `cls(_env_prefix="VARCO_REDIS_REPLICA_")`
(`main.py:184`, honoured at `:307`) and registers under `@Provider(qualifier="replica")`. A
consumer injects `Annotated[RedisEventBusSettings, InjectMeta(qualifier="replica")]` (the in-repo
idiom, `varco_core/cache/mixin.py:61`) or calls `container.get(RedisEventBusSettings,
qualifier="replica")`. Micronaut's `@EachProperty` (brief 015 §2) *enumerates* names from the
config tree; this plan deliberately does **not** — a name is declared in code, once, greppable.
Guards: `env_prefix` without `qualifier` → `ValueError` (two unqualified bindings would tie);
`qualifier` without `env_prefix` is allowed (same env, different identity — rarely useful, but
harmless). The generated reference documents the pattern in one paragraph, never enumerates
instances. The worked example (Step 7) is a **settings-only** test plus a README snippet building
a second `RedisCache` from a `"sessions"`-qualified `RedisCacheSettings`; no container is started.

### §D-loose — the eleven loose reads

| Read | Decision |
|---|---|
| `VARCO_JWKS_*` (`registry.py:227,234`) | Fold into `JwtVerificationSettings` (rename above) |
| `VARCO_TENANCY_NODE_ID`/`STORE_ID` (`control/service.py:145,148`) | Fold into `TenancySettings` (same names) |
| `VARCO_TENANCY_ISOLATION` (`app.py:412`) | Replace with `container.get(TenancySettings).isolation` |
| `VARCO_OTEL_CAPTURE_PARAMS[_EXCLUDE]`, `VARCO_OTEL_GLOBAL_ATTRS*` | **Allowlisted** — `OtelConfig` is code-only by design; folding is an observability redesign (parked, §Out of scope) |
| `VARCO_PROFILING_ENABLED` (`profiling/config.py:33`) | **Allowlisted** — a process-global kill switch read once at import, documented as such |
| `VARCO_LIFECYCLE_DISCOVERY_WARN` (`app.py:865`) | **Allowlisted** — 049 replaces lifecycle discovery |
| `VARCO_API_KEY_PEPPER` (`server_auth.py:435`) | **Allowlisted** — 053's surface |
| CLI defaults (`cli/tenant.py:71`, `cli/retention.py:66-76`) | **Allowlisted** — CLI argument defaults, not settings |

`varco_core/tests/test_settings_no_loose_env_reads.py` walks the ten source trees for
`environ.get("VARCO_` / `getenv("VARCO_` / `environ["VARCO_` and asserts the hit set equals the
frozen allowlist — a new loose read fails the build until it is either folded or allowlisted with
a reason.

### §D-core-list — where `varco_core`'s eleven classes get bound

`varco_core` has no `bootstrap()` and, by locked decision, nothing in it is scanned into
activation. `varco_core.settings.bind_core_settings(container) -> DIContainer` calls
`bind_settings()` for: `CacheSettings`, `IdempotencySettings`, `WebhookSettings`,
`ErrorEnvelopeSettings`, `JwtVerificationSettings`, `JwtTransformSettings`, `I18nSettings`,
`TimezoneSettings`, `MigrationSettings`, `TenancySettings`, `TenantProvenanceSettings`,
`TenantMembershipSettings`, `AuthorizationConfig`, `TlsSettings`. Every listed class **must
construct from an empty environment** (tested: Step 4 clears `VARCO_*` and constructs each).
`CloudEventsSettings` is excluded (required `source`; stays on the explicit
`bind_cloudevents_serializer()` path, which internally becomes
`bind_settings(container, CloudEventsSettings, instance=settings)`). Today's callers:
`varco_fastapi.di.create_varco_container()`/`bootstrap()` call it first; 049's core module calls
it from `register()`.

### §D-script — `scripts/settings_reference.py`

Same shape as `scripts/api_surface.py`: executes `scripts/packages.sh` (RL-18), imports each
package, walks it with `pkgutil.walk_packages`, collects every `pydantic_settings.BaseSettings`
subclass, and delegates to `varco_core.settings.inventory.env_vars_for(cls)` (the *tested*
introspection: prefix, `validation_alias`/`AliasChoices`, nested `BaseModel` fields via
`env_nested_delimiter`, `CommaSeparated`, defaults rendered via `repr`, secret-typed fields
rendered as `***`). Emits:

- `design/api-freeze-and-standards/measurements/settings-reference.json` — the thing `--check`
  diffs (sorted; class → prefix → `[ {env, field, type, default, description} ]`, plus the
  exclusion list and the loose-read allowlist).
- `technical_docs/settings-reference.md` — sections: *Registered settings* (one table per class,
  grouped by package, with the class's docstring first paragraph), *Named instances* (the DI-16
  paragraph), *Label-keyed families*, *Connection fragments (non-`VARCO_` by design)*, *Loose
  reads*. Added to `mkdocs.yml` nav as `- Settings Reference: settings-reference.md`
  (`docs_dir: technical_docs`, `mkdocs.yml:3`; kept out of the literate-nav `reference/` tree).

`--check` fails on **any** difference (there is no "additive is fine" tier: a new env var without
a regenerated reference is exactly the drift the gate exists for). Wired as `make
settings-check` inside `make lint`'s no-`PKG` block (`Makefile:163-167`), a `make
settings-reference` regenerator, and one step in `.github/workflows/test.yml`'s `lint` job after
`api surface --check` (`:64-65`).

## Steps

All commands from `/home/edoardo/projects/varco`. Steps 1-3 are the core and are TDD-ordered;
4-6 convert; 7 is DI-16; 8-15 rewire the packages (independent of each other after 3); 16-18 are
the script + gates; 19-23 docs/ledger. `make test PKG=<pkg>` after every step that touches
`<pkg>`.

0. [x] **Preflight.** `uv run python -c "import importlib.metadata as m; print(m.version('providify'))"`
       → `2.1.0` (045 landed; if `2.0.1`, stop — 045 first). Re-read and record in the scratchpad
       every `providify/…` and `pydantic_settings/main.py` line cited in §Context's table; re-run
       the inventory greps (`rg -n 'class \w+\((VarcoSettings|BaseSettings|ConnectionSettings|EventBusSettings|CacheSettings)\b' varco_*/varco_*`
       → 29 + bases; `rg -n 'def from_env\(' varco_*/varco_*` → 16;
       `rg -n 'environ(\.get\(|\[)"VARCO_' varco_*/varco_*` → the eleven loose reads). Any drift
       → fix §Context before continuing. **Verify:** scratchpad file lists ≥ 20 confirmations.

1. [x] `varco_core/tests/test_settings_bind.py` (**new, failing**) — the recipe's contract, using
       providify's `di_container` fixture and `monkeypatch.setenv`, with a local
       `class _Demo(VarcoSettings)` (`env_prefix="VARCO_DEMO_"`, `url: str = "x"`, `port: int = 1`,
       `enabled: bool = False`, `origins: CommaSeparated = ()`):
       (a) `bind_settings(c, _Demo)` returns a `_Demo` and `c.get(_Demo) is` it;
       (b) env wins over default (`VARCO_DEMO_PORT=7` → `port == 7`);
       (c) `instance=_Demo(port=9)` with `VARCO_DEMO_PORT=7` → `port == 9`, `url` still from env if set (per-field);
       (d) code override registered **before** `bind_settings(c, _Demo)` (plain `c.provide(...)`) → `bind_settings` returns the user's instance and **never reads env** (assert via a `VARCO_DEMO_PORT=notanint` that would have raised);
       (e) `VARCO_DEMO_PORT=abc` → `SettingsValidationError`, `str(exc)` contains `VARCO_DEMO_PORT` and `_Demo` and **not** the string `abc`;
       (f) `bind_settings` twice → same instance, exactly one binding (`len(c.get_all_bindings(_Demo)) == 1`);
       (g) `instance=` twice → `SettingsAlreadyBoundError`;
       (h) `env_prefix=` without `qualifier=` → `ValueError`;
       (i) `qualifier="b", env_prefix="VARCO_DEMO_B_"` + `VARCO_DEMO_B_PORT=3` → `c.get(_Demo, qualifier="b").port == 3` while `c.get(_Demo).port` is unaffected;
       (j) `c.validate()` on a container with one bound `_Demo` and no user override reports **no** `FALLBACK_SHADOWED`; with a user override it reports exactly one, `Severity.INFO`;
       (k) `provenance_of(c, _Demo).fields == {"url": "default", "port": "env", ...}` for (b), `"code"` for the field set in (c), `source == "external"` for (d);
       (l) `origins` from `VARCO_DEMO_ORIGINS=a, b ,c` → `("a","b","c")`.
       **Verify:** `uv run pytest varco_core/tests/test_settings_bind.py -q` → 12 failing with `ImportError`.

2. [x] `varco_core/varco_core/settings/__init__.py`, `bind.py`, `errors.py`, `types.py` (**new
       package**) — `bind_settings()` per §D-provider/§D-eager/§D-precedence/§D-named
       (`Provider(singleton=True, qualifier=qualifier)` closure; `@Fallback` applied only on the
       env path; `container.provide(fn, returns=cls)`; idempotency guard via
       `[b for b in container.get_all_bindings(cls) if b.qualifier == qualifier]`; eager
       `container.get()`; `SettingsValidationError` built from `ValidationError.errors()` `loc`
       → env name via `inventory.env_vars_for`); `provenance_of()` backed by a module-level
       `weakref.WeakKeyDictionary[DIContainer, dict[tuple[type, str | None], SettingsProvenance]]`
       (no container attribute is monkey-patched); `SettingsError(ValueError)` base,
       `SettingsValidationError`, `SettingsAlreadyBoundError`; `CommaSeparated` +
       `_split_csv`. `__init__` re-exports `VarcoSettings` from `varco_core.config` (the old
       import path stays valid). **No module-level `@Provider`/`@Singleton`/`@Configuration`
       anywhere in the package** (the `varco_core` scan rule) — add
       `test_settings_package_has_no_scanned_markers` to Step 1's file. Every function: full
       docstring (Args/Returns/Raises/Edge cases/Async safety) + a `DESIGN:` block citing the
       providify lines above. Add the new public names to `varco_core/__init__.py`'s `__all__` +
       `_LAZY` map (`:511`) — `varco_core.settings` must not be imported eagerly;
       `uv run python scripts/import_budget.py --check --warn-only` before/after.
       **Verify:** Step 1's file green; `make type-check`; import budget unchanged.

3. [x] `varco_core/tests/test_settings_inventory.py` (**new, failing first**) +
       `varco_core/varco_core/settings/inventory.py` — `env_vars_for(cls) -> tuple[EnvVarSpec, ...]`
       (frozen dataclass: `env`, `field_path`, `type_repr`, `default_repr`, `description`,
       `secret: bool`) covering: plain prefix; `validation_alias=AliasChoices(...)`
       (`SecurityPostureSettings.environment` → `VARCO_SECURITY_ENV`); nested `BaseModel` with
       `env_nested_delimiter="__"` (`PostgresConnectionSettings.ssl.ca_cert` →
       `POSTGRES_SSL__CA_CERT`); `CommaSeparated`; `SecretStr`/field names matching
       `varco_core.redaction`'s default key patterns → `secret=True`, default rendered `***`;
       `dict[str, Any]` fields flagged `env=None` ("not env-readable", as
       `RedisEventBusSettings.redis_kwargs` documents). Also `discover_settings_classes(packages)
       -> list[type[BaseSettings]]` (walk_packages + `__subclasses__` closure),
       `_EXCLUDED_SETTINGS: Mapping[type, str]` (reason per class), `check_unique_env_names(classes)`
       → list of collisions (must be empty after Step 6's renames; **before** them it must
       report the `VARCO_CLIENT_CERT` pair — write that as a temporary xfail(strict) that Step 6
       flips to a plain pass), and `test_no_settings_class_declares_env_file`,
       `test_every_registered_class_constructs_from_empty_env` (runs after Step 4/5 lists exist;
       clears `VARCO_*` via `monkeypatch.delenv` over `os.environ` keys).
       **Verify:** file green except the one strict xfail.

4. [x] `varco_core/varco_core/settings/core.py` — `bind_core_settings(container) -> DIContainer`
       per §D-core-list (initially the eleven pydantic classes; Step 5 adds the converted ones).
       Test in `varco_core/tests/test_settings_core.py`: every class resolvable afterwards,
       idempotent, `container.validate()` has no ERROR/WARNING. Rewire `varco_core` internal
       "hand" sites to *accept* an instance and *not* re-read env: `authority/registry.py` gains
       `settings: JwtVerificationSettings | None = None` on `TrustedIssuerRegistry.__init__`,
       stored once, replacing the four `from_env()` calls (`:906,943,965,1016`) and the two
       `VARCO_JWKS_*` reads (`:227,234`) with the two new fields; `jwt/parser.py:185` reads a
       module-level default built once (`_default_settings()` lru-cached, invalidated by
       `reset_default_settings()` for tests); `webhook/dispatcher.py:198`,
       `inbound/verifiers.py:590`, `exception/http.py:296`, `redaction/posture.py:146`,
       `cache/memory.py:205` keep their `settings or Cls()` shape (pure-function/core callers
       without a container) — unchanged. Tests: `test_trusted_issuer_registry.py` gains
       `test_settings_read_once` (patch env after construction → no effect).
       **Verify:** `make test PKG=varco_core`.

5. [x] **Convert the seven dataclasses + add `TlsSettings`** (§D-convert), one commit each:
       `migration/settings.py:35` (`VARCO_MIGRATE_`; `Literal` fields validate natively — delete
       `_LEGAL_*` loops), `tenancy/settings.py:95` and `:237` (`VARCO_TENANCY_` incl. new
       `node_id`/`store_id`; `VARCO_TENANT_` with `CommaSeparated` for `sources`/`base_domains`/
       `reserved_labels`), `tenancy/membership.py:93` (`VARCO_TENANT_MEMBERSHIP_`; default
       `on_missing_claim` **unchanged** — 055 flips it), `authority/config.py:95`
       (`AuthorizationConfig` keeps its label-scanning `from_env()` but the prefix constant and
       `jwt/transform/config.py:56` move to `VARCO_AUTHORIZATION__`; it becomes a `VarcoSettings`
       with `issuers: tuple[IssuerConfig, ...]` built by a `model_validator(mode="before")` that
       scans `os.environ` — so `bind_settings()` and provenance work unchanged),
       `varco_fastapi/middleware/cors.py:40` (`VARCO_CORS_`, `CommaSeparated`),
       `varco_fastapi/client/base.py:65` (`VARCO_CLIENT_`), **new**
       `varco_core/tls/settings.py` `TlsSettings` (`VARCO_TLS_`: `ca_cert`, `client_cert`,
       `client_key`, `trust_store_dir`, `Path | None`) with `TrustStore.from_settings(s)`;
       `TrustStore.from_env()` (`tls/store.py:174`) becomes `from_settings(TlsSettings())`; the
       deprecated `varco_fastapi/auth/trust_store.py:140` shim delegates the same way. Update
       `control/service.py:145-148` and `app.py:412` to read from the settings object. Add the
       converted classes to `bind_core_settings()`. Tests: rewrite the affected `from_env({...})`
       tests to `monkeypatch.setenv`; add a `test_defaults_byte_identical` per converted class
       asserting the exact default values listed in today's source (so 055/056 diffs are
       one-literal changes). **Verify:** `make test PKG=varco_core`, `make test PKG=varco_fastapi`;
       `uv run python scripts/api_surface.py` regenerated (class `__init__` changes are invisible
       to `--check`; `from_env` *signature* changes are not — the snapshot must move in this step).
       **REPAIR ROUND 1 (2026-09-24) note:** `ClientProfile` (`varco_fastapi/varco_fastapi/
       client/base.py:65`) was checked off as converted but is **still a plain `@dataclass(frozen=
       True)`**, not a `VarcoSettings`. Per the repair instructions this is escalated to the user
       as a design decision (not fixed in this repair round) — every other class this step lists
       (`MigrationSettings`, `TenancySettings`, `TenantProvenanceSettings`,
       `TenantMembershipSettings`, `AuthorizationConfig`, `CORSConfig`, `TlsSettings`) is correctly
       converted; only `ClientProfile` is outstanding.
       **REPAIR ROUND 2 (Decision 1) note:** the owner ruled — split, not a mechanical rename.
       `varco_fastapi/varco_fastapi/client/settings.py` now holds `ClientProfileSettings
       (VarcoSettings)` with the four scalar knobs (`timeout`, `retry_max`, `retry_delay`,
       `otel_enabled`) under `VARCO_CLIENT_`; `ClientProfile` itself stays a frozen dataclass
       holding `middleware`/`trust_store`/`timeout` (unchanged), gains
       `ClientProfile.from_settings(settings)`, and `from_env()` is now a thin
       `from_settings(ClientProfileSettings())` wrapper — the exact `TlsSettings`/`TrustStore`
       shape. Registered via `bind_settings()` in `varco_fastapi.di.bootstrap()`. The four loose
       `os.environ.get()` reads (`client/base.py:211-214`) and their allowlist rows in
       `varco_core/varco_core/settings/inventory.py` are deleted. Regression test:
       `test_client_profile_settings_defaults_byte_identical` in
       `varco_fastapi/tests/milestone_c/test_client.py`.

6. [x] **Renames** (§D-prefix table): `varco_fastapi/middleware/profiling.py:94`
       `VARCO_PROFILER_` → `VARCO_PROFILING_` (+ every docstring/README mention:
       `rg -n 'VARCO_PROFILER_' --type md --type py`); `VARCO_JWKS_*` folded (Step 4);
       `FASTREST_AUTHORIZATION__` (Step 5); TLS names (Step 5). Flip Step 3's collision xfail to a
       pass. `rg -n 'FASTREST_|VARCO_PROFILER_|VARCO_JWKS_|VARCO_CA_CERT|VARCO_CLIENT_CERT|VARCO_CLIENT_KEY|VARCO_TRUST_STORE_DIR' --type py --type md`
       → hits only in `CHANGELOG.md`, this plan, and the migration guide.
       **Verify:** the grep; `make test`.
       **REPAIR ROUND 1 (2026-09-24):** originally checked `[x]` with the `VARCO_PROFILER_` →
       `VARCO_PROFILING_` rename and the `VARCO_JWKS_*` fold **not actually done** in live code
       (`ProfilingSettings.model_config` still said `env_prefix="VARCO_PROFILER_"`; no legacy-name
       startup warning existed anywhere in `bind.py` despite the box being checked and §Risks
       claiming it). Fixed in the repair: `ProfilingSettings` now uses `VARCO_PROFILING_` (+ every
       docstring/README/ARCHITECTURE.md mention updated); `posture.py`'s docstring and
       `technical_docs/features/jwt-claim-transformer.md`/README.md/`varco_core/README.md`/
       CLAUDE.md's `VARCO_JWKS_*` mentions renamed to `VARCO_JWT_JWKS_*` (the fields themselves,
       `JwtVerificationSettings.jwks_min_refresh_seconds`/`jwks_ttl_seconds`, were already correct
       from Step 4 — only the stale doc prose lagged); `bind_settings()` now logs a one-release
       WARNING (naming old → new) for every §Migration-table rename when the *old* env name is
       still set, deduplicated per container (`_warn_legacy_env_names()`, `varco_core/varco_core/
       settings/bind.py`). Regression tests added: `TestLegacyEnvNameWarns` in
       `varco_core/tests/test_settings_bind.py` (exact-name, prefixed, JWKS, no-warning, and
       once-per-container cases).

7. [x] **DI-16 worked example** — `varco_redis/tests/test_redis_named_settings.py` (unit, no
       container started): `bind_settings(c, RedisCacheSettings)` +
       `bind_settings(c, RedisCacheSettings, qualifier="sessions", env_prefix="VARCO_REDIS_SESSIONS_CACHE_")`
       with `VARCO_REDIS_CACHE_URL`/`VARCO_REDIS_SESSIONS_CACHE_URL` set → two distinct instances;
       a local `@Singleton class _TwoCaches` with `Inject[RedisCacheSettings]` and
       `Annotated[RedisCacheSettings, InjectMeta(qualifier="sessions")]` resolves both;
       `c.validate()` clean; `provenance_of(c, RedisCacheSettings, qualifier="sessions").env_prefix`
       is the override. README snippet (Step 20) constructs `RedisCache(c.get(RedisCacheSettings,
       qualifier="sessions"))`. Nothing more (the DI-16 scope line).
       **Verify:** `make test PKG=varco_redis`.

8. [x] `varco_redis/varco_redis/{config.py:140-172, backplane.py:130-137, cache.py:576,796,
       dlq.py:740-753, stream_dlq.py:766}` — **delete** the six settings `@Provider`s;
       `varco_redis/di.py bootstrap()` calls `bind_settings()` for `RedisEventBusSettings`,
       `RedisCacheSettings`, `LayeredCacheSettings`, `RedisBackplaneSettings` (once each) before
       `container.scan(...)`; `RedisBackplaneSettings` becomes `VarcoSettings`-derived. The six
       "hand" sites keep `settings or Cls()` (standalone use). `bootstrap(streams=True)` → 046
       owns the signature; this step leaves a `# 046:` comment and passes
       `instance=RedisEventBusSettings(use_streams=True)` only if 046 has landed, else untouched.
       Tests: `test_redis_di.py` gains `test_settings_bound_once` (one binding per class after
       two `bootstrap()` calls) and `test_bad_env_fails_bootstrap` (`VARCO_REDIS_SOCKET_TIMEOUT=x`
       → `SettingsValidationError` naming it). **Verify:** `make test PKG=varco_redis`.
       **REPAIR ROUND 2 (Decision 2) note:** the owner ruled — finish the deletion. Prior repair
       rounds had converted `RedisEventBusSettings` but left `RedisCacheSettings`
       (`cache.py:568-569`), `LayeredCacheSettings` (`cache.py:791-792`), and
       `RedisBackplaneSettings` (`backplane.py:133-134`) as `@Fallback @Provider` factories
       (two per-`@Configuration` methods + one module-level function) because deleting them makes
       `container.ainstall(RedisCacheConfiguration)`/`ainstall(RedisLayeredCacheConfiguration)`/a
       scanned `RedisPubSubBackplane` unable to resolve their settings without a prior
       `bootstrap(container)` call — an owner-approved **BREAKING** change, now made. All three
       are deleted; `varco_redis.di.bootstrap()` binds all four settings classes
       (`RedisEventBusSettings`, `RedisCacheSettings`, `LayeredCacheSettings`,
       `RedisBackplaneSettings`) via `bind_settings()` before `scan()`. In-tree call sites fixed:
       `test_redis_cache.py`, `test_layered_cache_di.py`, `test_redis_cache_disposes.py`
       (add `bind_settings(container, ...)` before `ainstall()`), and
       `examples/00-full-stack-post-api/example/di.py`'s illustrative docstring. Conformance:
       `TestVarcoRedisDIWiringConformance.settings_classes`/`expected_defaults` in
       `varco_redis/tests/test_redis_di_wiring.py` extended with all three. `rg -n '@Provider'
       varco_*/varco_*/**/{config,settings,backplane}.py` → 0, and `varco_redis/varco_redis/
       cache.py` verified by eye (grep's filename blind spot) — the only `@Provider`s remaining
       there build the `CacheBackend` singletons themselves, not settings.

9. [x] `varco_kafka/varco_kafka/{config.py:260, channel.py:154, dlq.py:785}` — same as Step 8
       for `KafkaEventBusSettings`, `KafkaChannelManagerSettings`; tests in `test_kafka_di.py`.
10. [x] `varco_nats/varco_nats/{config.py:341, channel.py:192, dlq.py:772}` — same for
        `NatsEventBusSettings`, `NatsChannelManagerSettings`; tests in `test_nats_di.py`.
11. [x] `varco_memcached/varco_memcached/cache.py:645` — same for `MemcachedCacheSettings`; tests
        in `test_di.py`.
12. [x] `varco_casbin/varco_casbin/di.py:64` — the P-boot provider becomes
        `bind_settings(container, CasbinSettings)`; **new** `varco_casbin/tests/test_casbin_di.py`
        (the package has no DI test today — scout §10) with the two tests of Step 8.
13. [x] `varco_fastapi/varco_fastapi/di.py:280-398` — delete the settings providers
        (`cors_config`, `client_profile`, `profiling_settings`, `jwt_transform_settings`,
        `i18n_settings`, `timezone_settings`, `trust_store`'s env half); `create_varco_container()`
        (`:500`) / `bootstrap()` (`:712`) call `bind_core_settings(container)` then
        `bind_settings()` for `SecurityPostureSettings`, `SecurityHeadersSettings`,
        `BodyLimitSettings`, `RateLimitSettings`, `ProfilingSettings`, `CORSConfig`,
        `ClientProfile`; `trust_store` provider becomes `TrustStore.from_settings(Inject[TlsSettings])`.
        `app.py:380,453,454,662,710,715,1166,1190` and `middleware/introspect.py:150,231,260,294,355`,
        `exceptions.py:119` resolve from the container when one is present (the existing
        `container is None` branches keep the hand construction). **New**
        `varco_fastapi/tests/test_fastapi_settings_di.py`: every class resolvable after
        `bootstrap()`, one binding each, `validate()` clean, an explicit
        `create_varco_app(body_limit=BodyLimitSettings(max_bytes=1))` wins over
        `VARCO_BODY_LIMIT_MAX_BYTES=2`. **Verify:** `make test PKG=varco_fastapi`.
14. [x] `varco_sa/varco_sa/migration/migrator.py:155`, `varco_beanie/varco_beanie/migration/migrator.py:107`,
        `varco_fastapi/migrate.py:61` — `settings or MigrationSettings()` (the converted class);
        no other change. `varco_core/varco_core/event/cloudevents.py` —
        `bind_cloudevents_serializer()` routes its settings through
        `bind_settings(container, CloudEventsSettings, instance=settings)` (non-fallback, so a
        second call raises `SettingsAlreadyBoundError` instead of silently rebinding). Tests in the
        existing `test_cloudevents_di`-style files. **Verify:** `make test PKG=varco_sa`,
        `PKG=varco_beanie`, `PKG=varco_core`.
15. [x] `varco_core/tests/test_settings_no_loose_env_reads.py` (**new**) — §D-loose's allowlist
        test over the ten source trees. **Verify:** green with exactly the allowlisted hits.
        **REPAIR ROUND 2**: the first implementation was line-oriented regex and its
        "hit set equals the allowlist" promise was narrower than advertised — it could not see
        an *aliased* read (`env = environ if environ is not None else os.environ` then
        `env.get("VARCO_…")`, which is exactly what `observability/attributes.py` does), so four
        §D-loose-sanctioned reads were invisible and a new one could have been added unnoticed;
        and it counted a docstring `Example::` block as executable code, forcing a bogus
        `di/select.py` allowlist row. Replaced with an AST walk that resolves `os.environ`
        aliases (including through an `IfExp`) and never sees string literals. Allowlist now
        carries `attributes.py:450,461,477,478` (real, sanctioned) and no longer needs the
        docstring row. Four regression tests pin both defects closed.
16. [x] `scripts/settings_reference.py` (**new**) per §D-script — `discover_packages()` copied
        verbatim from `api_surface.py:96-139` (same DESIGN block), `--check`, `--snapshot PATH`,
        `--packages`, `--markdown PATH`. Unit test `varco_core/tests/test_settings_reference_script.py`
        (runs the script's functions in-process against `varco_core` only: deterministic output,
        `--check` exit codes, secret defaults rendered `***`). Then generate and commit
        `design/api-freeze-and-standards/measurements/settings-reference.json` and
        `technical_docs/settings-reference.md`. **Verify:** `uv run python scripts/settings_reference.py --check` → 0.
17. [x] `Makefile` — `settings-check` / `settings-reference` targets in the `api-check` shape
        (`:176-178`), `settings-check` added to `lint`'s `ifeq ($(strip $(PKG)),)` block
        (`:163-167`), `make help` lines (`:102-106`). `.github/workflows/test.yml` — one step
        `settings reference --check` after `:64-65`. `mkdocs.yml:103` nav entry.
        **Verify:** `make lint` (no `PKG`) runs it; `make lint PKG=varco_core` does not.
18. [x] `testkit/varco_conformance/COVERAGE.md` — no new ABC, no row; **but** 046's
        `di_wiring.py` suite (if landed) gains one assertion this plan owns: *"every settings
        class the package binds has exactly one binding after two `bootstrap()` calls"* — add it
        to the suite if present, otherwise note in §Risks that 046 must add it.
19. [x] `technical_docs/features/dependency-injection.md` — **new "Settings" section** (045 owns
        the file's first version; append, do not restructure): the recipe, the precedence
        diagram, the `.env` rule, the ordering rule, DI-16's paragraph, the `@Provider`-never-
        `@Singleton` reason with the corrected citation, and a Pitfalls table (code override
        after bootstrap → not-evicted; `instance=` is per-field; `env_prefix` needs `qualifier`;
        `list` fields are comma-separated, never JSON; a bare `REDIS_URL` is honoured by
        `RedisConnectionSettings` only). 058 assembles.
20. [x] `README.md` — one new "Settings and environment variables" section (recipe snippet,
        precedence, `.env` rule, the DI-16 `RedisCache` snippet, a link to
        `technical_docs/settings-reference.md`); **delete the 67 hand-written `VARCO_*` rows**
        across the per-feature tables (`rg -n '^\| `?VARCO_' README.md`) and leave one pointer
        line per section. `ARCHITECTURE.md` — `varco_core.settings` in the package map + the
        `SettingsProvenance`/`EnvVarSpec` types. `technical_docs/common-pitfalls.md` — two rows
        (`@Singleton` on `BaseSettings`, corrected reason; bare `REDIS_URL`/`POSTGRES_*` honoured
        by connection fragments).
        **REPAIR ROUND 2 (2026-09-24): completed by the doc-writer pass.** All 70 hand-written
        `VARCO_*` table rows across README.md's per-feature sections (error envelope, tenant
        provenance, JWT verification hardening, TLS trust store, OTEL, redaction, JWT revocation,
        API key pepper, CloudEvents, inbound webhooks, security headers, body limits, rate
        limiting, security posture) were replaced with one pointer paragraph per section aimed at
        the generated `technical_docs/settings-reference.md` (or, for the four loose-read/allowlisted
        vars with no `bind_settings()` home — `VARCO_OTEL_*`, `VARCO_API_KEY_PEPPER` — the
        existing full table in that feature's own doc, or inline prose noting the loose-read
        status). `rg -n '^\| `?VARCO_' README.md` → 0 hits, confirmed. Also fixed in passing: a
        stale code comment and table at README.md's TLS trust-store section still named the
        pre-4.0 `VARCO_CA_CERT`/`VARCO_CLIENT_CERT`/`VARCO_CLIENT_KEY`/`VARCO_TRUST_STORE_DIR`
        names Step 6 renamed to `VARCO_TLS_*` — real drift, now corrected to the current names.
        `ARCHITECTURE.md`'s package map gained a `varco_core/settings/` subtree entry
        (`bind.py`/`errors.py`/`types.py`/`inventory.py`/`core.py`, citing `SettingsProvenance`
        and `EnvVarSpec`) right after the `di/` entry it sits beside functionally.
        `technical_docs/common-pitfalls.md`'s two rows were already present from an earlier pass
        and were verified accurate, not duplicated.
21. [x] `CHANGELOG.md` `[Unreleased]` — `### Changed (Plan 048, DI-6/DI-16)` with the §Migration
        table verbatim and the new gate; `### Added` for `varco_core.settings`,
        `scripts/settings_reference.py`, `TlsSettings`.
22. [x] `CLAUDE.md` — rewrite the one `CasbinSettings` sentence (§D-provider's wording); add a
        four-line "Settings" pointer under "DI wiring (providify)" (rule: *every settings class
        is bound by `bind_settings()`; never `@Singleton`, never a scanned `@Provider`; the
        `VARCO_*` reference is generated — regenerate with `make settings-reference` or
        `make lint` fails*) and the `settings-check` line in the Commands section beside
        `api-check`. Nothing else — it is a routing table.
23. [x] `BACKLOG.md:77,100` — `DI-6`/`DI-16` status → `done (Plan 048)`; §Full verification below.

## Edge cases

- **A user binds `Cls` *after* `bootstrap()`** → their binding wins for every *later* `get()`;
  objects built during bootstrap keep the env instance (`scope.py:1048-1053`). Documented rule;
  `provenance_of()` reports `source="external"` so 052 can flag it.
- **A user binds a *qualified* `Cls` only** → `bind_settings(c, Cls)` (unqualified) still registers
  its fallback (exact-qualifier idempotency check); an unqualified `get()` then sees both
  candidates and providify's post-filter picks the non-fallback (the `:1729` any-qualifier rule)
  — acceptable and identical to providify's own semantics; test (d) variant records it.
- **Empty environment + class with a required field** (`CloudEventsSettings`) → never in a bind
  list (Step 3's test); explicit-instance path only.
- **`instance=` of the wrong type** → `TypeError` before any registration.
- **`SettingsValidationError` for a nested field** (`ssl.ca_cert`) → env name rendered with the
  class's `env_nested_delimiter`; for an alias-only field the alias; for a `dict` field the
  message says "not env-readable — pass it via `instance=`".
- **Two packages bind the same class** (e.g. `RedisEventBusSettings` by `varco_redis` and a
  hypothetical plugin) → second call is a no-op returning the first instance (idempotency guard
  is per container, not per package).
- **`monkeypatch.setenv` after `bind_settings`** in a test → no effect (constructed once); tests
  that need a different value construct a fresh container — stated in the guide.
- **`--check` run on a tree where a package fails to import** → non-zero with the package named
  (same as `api_surface.py`'s `ApiSurfaceError`), never a silently shorter snapshot.

## Verification

```bash
uv run python -c "import importlib.metadata as m; print(m.version('providify'))"   # 2.1.0
uv run pytest varco_core/tests/test_settings_bind.py varco_core/tests/test_settings_inventory.py \
    varco_core/tests/test_settings_core.py varco_core/tests/test_settings_no_loose_env_reads.py \
    varco_core/tests/test_settings_reference_script.py -q
uv run pytest varco_redis/tests/test_redis_named_settings.py varco_casbin/tests/test_casbin_di.py \
    varco_fastapi/tests/test_fastapi_settings_di.py -q
make test                                   # all eleven suites
make lint                                   # ruff + api-check + import-budget + asyncapi-check + settings-check
make lint PKG=varco_core                    # must NOT run settings-check
make type-check
uv run python scripts/settings_reference.py --check
uv run python scripts/api_surface.py --check
uv run python scripts/import_budget.py --check --warn-only
rg -n 'FASTREST_|VARCO_PROFILER_|VARCO_JWKS_|VARCO_CA_CERT|VARCO_CLIENT_CERT|VARCO_CLIENT_KEY|VARCO_TRUST_STORE_DIR' --type py   # 0 hits
rg -n '@Provider' varco_*/varco_*/**/config.py varco_*/varco_*/**/settings.py                                                     # 0 hits
rg -n '^\| `?VARCO_' README.md                                                                                                    # 0 hits
```

## Risks

| Risk | Invariant | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — 045 has landed (`providify>=2.1.0`, `@Fallback` importable) | Step 2 imports `Fallback`; Step 1(j) asserts `FALLBACK_SHADOWED` | Step 0 stops on 2.0.1 |
| ⚠️ ASSUMPTION — `get_all_bindings()` excludes a *shadowed* fallback (shared context lists it among the honouring methods; `container.py:7464-7491` docstring not re-read at 2.1.0 for the fallback post-filter) | Idempotency guard and the code-override skip both rely on it | Step 0 reads `get_all_bindings` → `_filter` path; if it returns shadowed fallbacks too, the guard checks `_is_fallback`-free by testing `b.fn` for `varco`'s own marker attribute instead — still no private providify access |
| ⚠️ ASSUMPTION — `AnyBinding` exposes `.qualifier` (plan 044 cites `b.qualifier == qualifier` at `_filter :1729`) | Exact-qualifier idempotency | Step 0 confirms on `providify/binding.py` |
| Converted classes' default literals drift (055/056/054 flip some of them) | Every default byte-identical in this plan (`test_defaults_byte_identical`) | Flip plans change one literal + that test's expected value; merge either order |
| 045 decorates the six duplicate settings `@Provider`s with `@Fallback`; this plan deletes them | No duplicate binding per `(cls, qualifier)` | Order 045 → 048; if reversed, 045 finds them gone — no conflict beyond merge churn |
| `container.scan("varco_redis")` **without** `bootstrap()` no longer yields settings → `LookupError` on `Inject[RedisEventBusSettings]` | 046 makes `bootstrap()` the contract | §Migration row; the error message is providify's clear "Cannot resolve …; bind it" |
| `_env_prefix` override is a pydantic-settings init kwarg, not a public `model_config` route — a future pydantic-settings could rename it | DI-16 test (i) fails loudly | Pin note in `pyproject` comment; single call site in `bind.py` |
| Renames break deployments silently (an unset new name falls back to the default) | 052's diagnose shows provenance `default` for a renamed var; this plan adds a **one-release startup warning** in `bind_settings()`: if any *old* name from the §Migration table is present in `os.environ`, log `WARNING` naming old → new (removed in 5.0.0) | Test: `test_legacy_env_name_warns` |
| README table deletion loses per-feature context | Each section keeps a pointer + the generated page groups by package/class with docstrings | 058 polishes |
| `walk_packages` imports every module of every package in the script (side effects, import time) | Same cost `api_surface.py` already pays; the script never runs in `make lint PKG=` | — |
| 046's `di_wiring.py` conformance suite may not exist yet | Step 18 is conditional | Note carried to 046 if absent |

## Migration (3.x → 4.0.0; 058 assembles)

| Old | New | Notes |
|---|---|---|
| `FASTREST_AUTHORIZATION__<LABEL>__URL` / `__ISS` | `VARCO_AUTHORIZATION__<LABEL>__URL` / `__ISS` | rename; warning if the old name is set |
| `VARCO_PROFILER_ENABLED`, `VARCO_PROFILER_SLOW_THRESHOLD_MS`, … | `VARCO_PROFILING_ENABLED`, `VARCO_PROFILING_SLOW_THRESHOLD_MS`, … | rename |
| `VARCO_CA_CERT`, `VARCO_CLIENT_CERT`, `VARCO_CLIENT_KEY`, `VARCO_TRUST_STORE_DIR` | `VARCO_TLS_CA_CERT`, `VARCO_TLS_CLIENT_CERT`, `VARCO_TLS_CLIENT_KEY`, `VARCO_TLS_TRUST_STORE_DIR` | rename; `TrustStore.from_env()` now reads `TlsSettings` |
| `VARCO_JWKS_MIN_REFRESH_SECONDS`, `VARCO_JWKS_TTL_SECONDS` | `VARCO_JWT_JWKS_MIN_REFRESH_SECONDS`, `VARCO_JWT_JWKS_TTL_SECONDS` | rename; `TrustedIssuerRegistry(min_refresh_interval=, ttl_seconds=)` kwargs unchanged |
| `MigrationSettings.from_env(env)`, `TenancySettings.from_env(env)`, `TenantProvenanceSettings.from_env(env)`, `TenantMembershipSettings.from_env(env)` | `Cls.from_env()` (no argument) or `Cls(field=…)` / `Cls.from_dict({...})` | the `env` mapping parameter is gone; the classes are pydantic `VarcoSettings` |
| `CORSConfig`, `ClientProfile` (frozen dataclasses) | same names, pydantic `VarcoSettings`; positional construction → keyword | `CORSConfig("a")` → `CORSConfig(allow_origins=("a",))` |
| A module-level `@Provider` returning a settings class (six in-tree; any downstream copy) | `bind_settings(container, Cls)` in `bootstrap()`/`register()` | scanned settings providers are gone; `container.scan("varco_x")` alone no longer registers settings — call `varco_x.bootstrap(container)` (046) |
| `container.bind(Cls, MyCls)` / hand-registered settings before scan ("provide before scan to win") | `bind_settings(container, Cls, instance=MyCls(...))` **before** `bootstrap()`; or any non-fallback binding | order still matters only for objects built during bootstrap |
| README per-feature `VARCO_*` tables | `technical_docs/settings-reference.md` (generated) | `make settings-reference` |
| `ClientProfile` (frozen dataclass with loose `os.environ.get()` reads in `from_env()`) | `ClientProfileSettings(VarcoSettings)` (`VARCO_CLIENT_TIMEOUT`/`_RETRY_MAX`/`_RETRY_DELAY`/`_OTEL_ENABLED`) + `ClientProfile.from_settings(settings)` | Decision 1 (repair round): same split shape as `TlsSettings`/`TrustStore`; `ClientProfile.from_env()` is now a thin `from_settings(ClientProfileSettings())` wrapper — no call-site change |
| `container.ainstall(RedisCacheConfiguration)` / `ainstall(RedisLayeredCacheConfiguration)` / `RedisPubSubBackplane` resolved via `container.scan("varco_redis")` alone | Requires `varco_redis.di.bootstrap(container)` first | **BREAKING** (Decision 2, repair round): the last three per-`@Configuration`/module-level `@Fallback @Provider` settings factories in `varco_redis` (`RedisCacheSettings`, `LayeredCacheSettings`, `RedisBackplaneSettings`) are deleted; `bootstrap()` now binds all three via `bind_settings()`, closing Step 8's "delete the six settings providers" for real |

## Docs

- `technical_docs/features/dependency-injection.md` — "Settings" section (Step 19; 058 assembles).
- `technical_docs/settings-reference.md` — **generated**, never hand-edited (header says so).
- `README.md` — new section + pointers (Step 20). `ARCHITECTURE.md` — package map + types.
- `CLAUDE.md` — corrected `CasbinSettings` sentence, four-line pointer, `settings-check` command.
- `technical_docs/common-pitfalls.md` — two rows. `CHANGELOG.md` — Step 21.

## Done criteria

- `rg -n '@Provider' varco_*/varco_*/**/{config,settings,backplane}.py` → 0; every settings
  binding in the tree goes through `bind_settings()`; `make test` green on 3.12/3.13.
- `make lint` (no `PKG`) runs `settings-check` and it passes on the committed snapshot; CI's
  `lint` job has the step.
- `test_every_registered_class_constructs_from_empty_env`, `test_no_settings_class_declares_env_file`,
  `check_unique_env_names == []`, and the loose-read allowlist test are all green.
- `VARCO_DEMO_PORT=abc`-style misconfiguration fails at `bootstrap()` with the env var named.
- The DI-16 test resolves two `RedisCacheSettings` under different prefixes with `validate()` clean.
- `BACKLOG.md` rows `DI-6`, `DI-16` → done; CHANGELOG + §Migration written.

## Out of scope / Parked

- Folding `VARCO_OTEL_*` into an `ObservabilitySettings` (would redesign `OtelConfig`, which is
  code-only by design) — a future row.
- YAML/TOML settings sources (providify's `ConfigProperties` sources) — nothing asks for them;
  re-evaluate only with a consumer.
- Enumerating named instances from the environment (`@EachProperty`) — DI-16's "no general
  mechanism" line.
- A `SecretStr` sweep across every existing password-shaped field — redaction is 057/040's seam;
  the inventory only *renders* secrets as `***`.
