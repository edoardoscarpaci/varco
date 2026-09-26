# Plan 047 — Config-driven backend selection (`VARCO_<SEAM>_BACKEND`), one feature-enable convention, and the generated default-implementation catalogue (DI-4, DI-12, DI-17)

Covers BACKLOG 4.0 rows **`DI-4`** (`BACKLOG.md:93` — *"Config-driven backend selection —
`VARCO_<SEAM>_BACKEND=` (cache, DLQ, idempotency store, job store, event bus, revocation store)
resolved through providify `@Alternative`/`@Profile`, replacing 'first-scanned wins' and the
Redis-only `_redis_active_bus` selector"*), **`DI-12`** (`BACKLOG.md:90` — *"One feature-enable
convention — decide `enable_*(container)` vs `VARCO_<FEATURE>_ENABLED` (or both, with a stated
precedence) and publish the default on/off matrix by category: optional integration whose library
is present / security feature / dev-only convenience"*), **`DI-17`** (`BACKLOG.md:94` —
*"Default-implementation catalogue + bring-your-own override — one generated table (ABC → shipped
default → Null/InMemory/production-safe → override path) sourced from the default declarations
`DI-3` introduces, published in the DI guide (`DI-14`) and checked by `DI-13`; plus a Django-style
dotted-path override (`VARCO_<SEAM>_BACKEND=pkg.mod:Class`) resolved into a providify binding with
a stated precedence: code binding > env dotted path > varco default. `DI-7`'s `varco diagnose`
reads the same table at runtime"*), and **Open Question 2** (`BACKLOG.md:130-134`), re-decided
below against providify 2.1.0 (§D-mechanism).

Third plan of the 4.0 DI track (`plans/000-index-4-0-di-standardization.md`, row 047; depends on
045 and 046). **Landed decisions this plan builds on and never re-opens:** Plan 045 — `providify
>= 2.1.0`, every varco-owned binding of a varco seam is `@Fallback`, in-process defaults at
priority 0 and backend bindings at `varco_core.di.BACKEND_PRIORITY` (`100`), the per-site table
A1–A20/B1–B13, `validate()` clean for a default container, the `qualifier="in_memory"` escape
hatch. Plan 046 — `bootstrap(container: DIContainer) -> DIContainer` required-container contract
for all ten packages, `varco_core.di.bind_instance()`/`is_user_bound()`/`AlreadyBoundError`,
`enable_*` raise on repeat, `bootstrap(streams=True)` gone (an explicit `RedisEventBusSettings
(use_streams=True)` instance replaces it), the `testkit/varco_conformance/di_wiring.py` suite and
its `fallback_bindings()` shim. Plan 048 (parallel, may land before or after) — `varco_core.
settings.bind_settings()` and `scripts/settings_reference.py --check`; this plan's catalogue
script copies that script's shape and coordinates the env-var namespace with it (§D-script).

Neighbours referenced by number, never absorbed: **049** (moves `create_varco_app`'s tri-state
kwargs into modules — this plan defines the on/off convention and env-var names those modules
honour), **050** (adds lifecycle/`@Disposes` to the seven providers this plan introduces only as
*selectable bindings*), **052** (`varco diagnose` renders `catalogue(container)` and
`inspect_backend_selection()` — the schema is defined here), **053** (FLIP-5, `BaseAuthorizer`),
**054** (owns whether the production in-process-backend finding this plan emits at `WARN`
becomes a refusal), **058** (assembles §Migration; completes the DI guide).

**Research briefs:** 014 §3 (`quarkus.<ext>.enabled`, Micronaut `@Requires(property=…,
value=…)`/`@Requires(env=…)`; `014:84-114`), 014 §Librarian #1 and #4 (config-selected
alternatives; `varco.<feature>.enabled` for optional features, `varco.<seam>.selected=Impl` for
swappable seams; `014:248,254`), 010 §3 (fail-closed for security seams, no-op default acceptable
for non-security seams; `010:43-55`), 010 §4 ("what implementation am I running" is table stakes;
`010:59-73`), 010 §6 (Python users expect settings strings + env vars, not DI; `010:97-115`), 010
§Librarian #1/#3/#4 (`010:173-184`), 015 §2 (precedence code > env > file is the cross-framework
convergence; `015:94-95`), 015 §Librarian #1 (explicit over discovered).

**providify source of record:** `/home/edoardo/projects/providify` at **2.1.0** (`pyproject.toml:3`).
Every `providify/…` line below was read in that tree on 2026-09-15; Step 0 re-confirms each on
the installed copy.

> ⚠️ **Four premises corrected from source before anything was decided.**
> (1) **`_redis_active_bus` does not exist as code.** It survives only in three docstring
> mentions (`varco_redis/varco_redis/di.py:25,123,162`); the selector is
> `RedisEventBusSelectorConfiguration.bus()` (`varco_redis/varco_redis/bus.py:516-592`, a
> `@Configuration` provider reading `settings.use_streams`). "Replacing `_redis_active_bus`" is a
> prose deletion plus a gate on the real selector (§D-vocab).
> (2) **Three of the six seams have no DI binding for any backend today.** Idempotency stores
> (`RedisIdempotencyStore` `varco_redis/idempotency.py:60`, `SAIdempotencyStore`
> `varco_sa/idempotency.py:95`, `BeanieIdempotencyStore` `varco_beanie/idempotency.py:107`, and
> even `InMemoryIdempotencyStore` `varco_core/idempotency/memory.py:37`) and job stores
> (`RedisJobStore` `varco_redis/job_store.py:174`, `SAJobStore` `varco_sa/job_store.py:284`,
> `BeanieJobStore` `varco_beanie/job_store.py:380`) carry no `@Singleton`/`@Provider` anywhere
> (`rg -n '^@(Singleton|Provider|Configuration)' varco_*/varco_*/{idempotency,job_store}.py` →
> 0 hits); `SADeadLetterQueue` (`varco_sa/dlq.py:143`) is hand-constructed (CLAUDE.md ⚠️). The
> BACKLOG's "which idempotency store wins depends on scan order" is false — none is ever
> scanned. `DI-4` therefore requires **seven new selectable bindings** (§D-new-providers), not
> only gates on existing ones. The in-process job-store default lives in `varco_fastapi`
> (`varco_fastapi/job/store.py:39`, 045 A14), not `varco_core`.
> (3) **providify 2.1.0 ships `@Requires(env=, value=)` natively** (`providify/decorator/scope.py:818-967`,
> `providify/metadata.py:230-367`) — the P25 report's §5 answer (profile translation via
> `activate_profile()`) is superseded; 045 Step 3 already records that. Open Question 2 is
> re-answered in §D-mechanism.
> (4) **The premise "an env-selected backend binding is non-fallback, so it collides with a code
> binding at priority 0" is the rejected alternative, not the design.** Under 045, backend
> bindings are `@Fallback` at `BACKEND_PRIORITY`; this plan *keeps them fallbacks* and adds the
> gate. "Code beats env" is enforced by `_filter()`'s post-step (`providify/container.py:2168-2169`),
> which drops every fallback when any non-fallback survives — `_get_best_candidate()`'s
> `max(priority)` (`container.py:2853`) only ranks the survivors. **045's `BACKEND_PRIORITY =
> 100` does not break the precedence rule and is not amended** (§D-precedence).

> **Handoff from Plan 046 (landed):** `bootstrap()` is the registration point for 047's `@Requires`-gated providers; each package's `expected_defaults`
> (`testkit/varco_conformance/di_wiring.py` subclasses) is the seed of the catalogue and should be *generated* from it once the catalogue exists (then
> `test_declared_defaults_are_complete` reads the catalogue, and the P29 shim's only remaining consumer is the completeness cross-check); `enable_*` now
> raises `AlreadyBoundError` on a second non-fallback — 047's precedence decision must not reintroduce a silent no-op.

## Goal

After this plan: each of the six seams — `CacheBackend`, `AbstractDeadLetterQueue`,
`AbstractIdempotencyStore`, `AbstractJobStore`, `AbstractEventBus`, `AbstractTokenRevocationStore`
— is selected by **one** environment variable `VARCO_<SEAM>_BACKEND` whose value is a named
backend (`kafka`, `redis`, `sa`, …), the seam's in-process default name (`memory`/`null`), or a
dotted path `pkg.mod:Name`; every varco backend binding of those seams carries
`@Requires(env=…, value=…)` beside its `@Fallback`, so an unselected backend is *inactive*
(`CONDITION_INACTIVE` INFO) and the in-process `@Fallback` default serves; a dotted path is
resolved at `varco_core.di.bootstrap()` into a `@Fallback` provider at
`varco_core.di.ENV_OVERRIDE_PRIORITY` (`200`); the precedence **code binding > env dotted path >
env named backend > varco default** holds mechanically and is proven per package by the
`di_wiring` suite; a typo in the value fails `check_backend_selection()` with a message naming the
seam and the known values. Every binding-shaped feature (`feature_flags`, `tenant_membership`,
`policy_authorizer`) is switchable by `enable_*(container)` **or** `VARCO_<FEATURE>_ENABLED`,
code > env, through one helper (`feature_default`); every settings-shaped feature keeps its
`enabled` field under the same name rule; the default on/off matrix is published.
`scripts/di_catalogue.py` generates `technical_docs/di-catalogue.md` + a JSON snapshot from the
bootstrapped containers of all ten packages, `--check` gates drift in `make lint`'s no-`PKG` path
and CI; `varco_core.di.catalogue(container)` is the runtime function 052 renders. A production
app running a seam on its in-process default is a `SecurityPosture` finding.

## Non-goals

- **Any change to providify** — 2.1.0 consumed as shipped. No upstream ask is filed by this plan
  (the P29 descriptor gap 046 filed is *used*, via the relocated shim — §D-introspect).
- **Lifecycle (`start()`/`stop()`/`@Disposes`) for the seven new providers** — Plan 050 (DI-9/
  DI-10). This plan registers them as *selectable* bindings that construct the object exactly as
  today's hand-construction does; where a class needs `await store.start()` the provider docstring
  says so and 050 wires it. `SADeadLetterQueue` and `OutboxRelay` stay hand-constructed for every
  purpose other than selection.
- **Settings registration** — Plan 048. The gated providers *inject* the package's settings
  object (`Inject[RedisEventBusSettings]`, `Inject[SAConfig]`) and do not care whether 045's
  `@Fallback` provider or 048's `bind_settings()` bound it.
- **`VarcoModule` / moving `create_varco_app`'s eleven kwargs** — Plan 049. This plan defines the
  tri-state semantics and env names; 049 moves the kwargs.
- **`varco diagnose`** — Plan 052 renders; this plan ships the data functions and the schema.
- **Flipping any default** — the in-process defaults stay the defaults (`memory`/`null`); the
  posture finding is `WARN`, and whether production *refuses* is 054's decision.
- **A code-side "select the shipped kafka bus" verb.** A shipped backend is selected by the env
  var only; the code tier is for *your own* implementation (`bind_instance`) — §D-precedence
  states why there is deliberately no `select_backend(container, "kafka")`.
- **Transport selection inside Redis** — `VARCO_REDIS_USE_STREAMS` stays a `RedisEventBusSettings`
  field read by the selector (046 §D-streams / 048); the DLQ gets two named values instead
  (§D-vocab, asymmetry recorded).
- **`enable_authorization_audit`'s env form** — a wrapper over the live authorizer has no
  declarative gate shape (§D-features); it stays code-only and is listed as the one exception.
- **`ChannelManager`, `RepositoryProvider`, `IUoWProvider`, `AbstractDistributedLock`, `PolicyEngine`,
  query compilers** (045 B5/B6/B7/B8/B12/B13) — not among the six seams; untouched.
- **A top-level `varco_core` re-export** of the new `varco_core.di` names — same 045/046
  non-goal (import budget).

## Context

### providify 2.1.0 facts this plan rides on (verified in the tree)

| Primitive | Where | Semantic |
|---|---|---|
| `@Requires(*, condition=None, env=None, value=None)` | `providify/decorator/scope.py:818-967` | Keyword-only; `env=` is a plain `os.environ` name (`:832-836`, verbatim/case-sensitive — `metadata.py:252-255`); `value=` exact, case-sensitive string (`:843-847`); evaluated **lazily on every `_filter()` call, never cached** (`:849-854`) — a `monkeypatch.setenv` after import takes effect on the next `get()`; AND with `@Profile`/`@Alternative`, evaluated **last** (`:859-864`); stackable = AND (`:866-867`); `__dict__`-only, not inherited (`:868-870`); **no singleton eviction** when the condition flips (`:872-876`); **goes on the `@Provider` method, not the `@Configuration` class** (`:878-883`); `ValueError` at decoration for no clause / `value` without `env` / blank `env` (`:945-955`) |
| `RequiresMarker.is_satisfied()` | `providify/metadata.py:265-334` | `actual = os.environ.get(env)`; **`env=` + `value=` given → unset OR any other string → `False`** (`:320-331`); `env=` alone → set-to-non-empty (`:326-327`); `value=""` = "set and exactly empty" (`:292-294`); `condition()` truthiness (`:332-333`) |
| `RequiresMarker.describe()` | `metadata.py:336-367` | Renders `@Requires(env='X', value='y')` / `condition=<qualname>` for messages — the reason this plan's predicate callables carry a readable `__qualname__` |
| `ClassBinding.conditions` / `ProviderBinding.conditions` | `providify/binding.py:212` / `:727` | `_get_requires_markers(impl or fn)` read **once at binding construction** — same rule as `fallback` (`:207`/`:723`); a marker stamped after `register()`/`provide()`/`scan()` is invisible |
| `_binding_is_active()` third conjunct | `providify/container.py:1904-2005`; `:2000-2005` | `profile_ok AND alternative_ok AND (not b.conditions or _conditions_hold(b))`; `_conditions_hold` (`:549-596`) wraps a raising predicate in `ConditionEvaluationError` (`:589-593`) |
| `_filter()` order | `container.py:2129-2170` | interface → `exact_only` → qualifier (`:2147`) → priority (`:2148`) → **activation** (`:2150`) → **fallback post-step** (`:2168-2169`): `if any(not b.fallback …): candidates = [non-fallbacks]`. An inactive gated backend never reaches the post-step; an active gated `@Fallback` competes with the in-process `@Fallback` by priority only when no non-fallback exists |
| `_get_best_candidate()` | `container.py:2814-2853` | `max(candidates, key=priority)` over the **survivors** of `_filter()` (`:2846,2853`) — never sees a dropped fallback; exact tie → first registered |
| Qualifier request-relativity | `container.py:2096-2101` | Unqualified request matches qualified candidates; qualified request only its own — `get(AbstractEventBus, qualifier="in_memory")` keeps working under gating |
| `IssueKind.CONDITION_INACTIVE` | `providify/validation.py:110-114`; emitter `container.py:6296-6347` (pass 1c, `:6508-6517`) | `Severity.INFO`, one per binding with an unsatisfied marker; message renders each failed clause with `(VARCO_X is unset)` / `('actual')` (`:6285-6294`); skipped for the synthetic `exact_only` self-binding (`:6302-6316`); **pass 1c runs unfiltered** — a raising predicate surfaces from `validate()` itself (`:6145-6152`) |
| `ConditionEvaluationError` | `providify/exceptions.py:369-409` | Subclasses `providifyError`, **not** `BindingError` (`:378-394`) — a broken predicate is a resolution-time bug; carries `owner` and `marker` |
| Inactive fallbacks | `container.py:6153-6155` (E27) | Never reported as `FALLBACK_SHADOWED` — an unselected backend produces exactly one INFO (`CONDITION_INACTIVE`), never two |
| `@Fallback` composes with `@Requires` | `scope.py:979-986` | "a `@Fallback` binding that is itself inactive … is simply invisible" — the gate and the fallback marker are independent |
| `describe().bindings` | `container.py:7399-7418` | Walks `self._bindings` **unfiltered** (every binding, active or not) but `BindingDescriptor` carries no `fallback`/`conditions` (046 §D-suite, P29) — why `inspect_bindings()` needs the one private read (§D-introspect) |
| `get_binding()` / `get_all_bindings()` | `container.py:7422-7491` | Pure reads honouring activation + fallback rule — the "who is live" tools; never instantiate |
| No "construct without registering" API | `container.py` method list (`rg -n '^    (async )?def [a-z]'`) | There is no `container.build(cls)` — a dotted-path class cannot be injected into without a binding; hence §D-dotted's zero-arg rule |

### The six seams — every binding today, per package (2026-09-15; 045's site ids in brackets)

| Seam (interface) | In-process default | Backend bindings that exist | Backend classes with **no** binding |
|---|---|---|---|
| `cache` — `varco_core.cache.base.CacheBackend` (`AsyncCache` is the Protocol; bindings are the ABC) | `InMemoryCache` `varco_core/cache/memory.py:145` [A1] `qualifier="in_memory"` | `RedisCacheConfiguration.redis_cache` `varco_redis/cache.py:578-582` [B11]; `RedisLayeredCacheConfiguration.layered_cache` `cache.py:798-802` [B11]; `MemcachedCacheConfiguration.memcached_cache` `varco_memcached/cache.py:647-651` [B11] — **redis + layered tie at `BACKEND_PRIORITY` whenever `varco_redis` is scanned** (045 Edge cases; 046 §D-suite "B11 ×2") | — |
| `dlq` — `varco_core.event.dlq.AbstractDeadLetterQueue` | `InMemoryDeadLetterQueue` `varco_core/event/dlq.py:524` [A3] `in_memory` | `KafkaDLQConfiguration.dlq` `varco_kafka/dlq.py:787-792` [B10]; `NatsDLQConfiguration.dlq` `varco_nats/dlq.py:773-779` [B10]; `RedisDLQConfiguration.dlq` `varco_redis/dlq.py:755-760` and `RedisStreamDLQConfiguration.dlq` `stream_dlq.py:766-772` [B10 ×2 — **tie**]; `BeanieDeadLetterQueue` `varco_beanie/dlq.py:122` [B9] `qualifier="beanie"` | `SADeadLetterQueue` `varco_sa/dlq.py:143,169` (`engine`, `serializer=`) |
| `idempotency` — `varco_core.idempotency.base.AbstractIdempotencyStore` | **none** — `InMemoryIdempotencyStore` `varco_core/idempotency/memory.py:37` is undecorated; the middleware takes `store=` explicitly (`varco_fastapi/middleware/idempotency.py:210-214`) | **none** | `RedisIdempotencyStore` (`url`, `key_prefix`, `**redis_kwargs`; `redis/idempotency.py:76-82`); `SAIdempotencyStore` (`url`, `**engine_kwargs`; `start()` builds the engine, `sa/idempotency.py:117-134`); `BeanieIdempotencyStore` (`session=None`; `beanie/idempotency.py:120`) |
| `job_store` — `varco_core.job.base.AbstractJobStore` | `InMemoryJobStore` **in `varco_fastapi`** `varco_fastapi/job/store.py:39-40` [A14], imports only `varco_core` (`:26-36`) | **none** | `RedisJobStore(client, *, key_prefix, claim_ttl)` `redis/job_store.py:217-223`; `SAJobStore(engine)` `sa/job_store.py:328`; `BeanieJobStore()` `beanie/job_store.py:380,415-417` (needs `JobDocument` in `init_beanie()`, `:419-421`) |
| `event_bus` — `varco_core.event.base.AbstractEventBus` | `InMemoryEventBus` `varco_core/event/memory.py:87` [A2] `in_memory` | `KafkaEventBus` `varco_kafka/bus.py:95` [B1] `kafka`; `NatsEventBus` `varco_nats/bus.py:129` [B2] `nats`; `RedisEventBusSelectorConfiguration.bus` `varco_redis/bus.py:532-537` [B3, unqualified, `BACKEND_PRIORITY`] + `RedisEventBus` `bus.py:102` [B4, `redis`, priority 0] — **kafka + nats tie when both scanned** (045 §D-tiers) | `RedisStreamEventBus` (`streams.py:176`, produced only by B3 — by design) |
| `revocation` — `varco_core.revocation.base.AbstractTokenRevocationStore` | `NullTokenRevocationStore` `varco_core/revocation/null.py:35` [A8] | opt-in only: `enable_token_revocation()` → `_provide_in_memory_revocation_store` (`revocation/di.py:54-67,70-97`); `enable_redis_token_revocation(container, **redis_kwargs)` → closure provider (`varco_redis/di.py:248-289`) | `InMemoryTokenRevocationStore` `revocation/memory.py:32` (no scanned binding — correct, it is an opt-in) |

No varco module injects a **concrete** backend class (`rg -n 'Inject\[(KafkaEventBus|RedisEventBus|…)\]' varco_*/varco_*` → 0), so gating a scanned class's `ClassBinding` (which also serves `get(KafkaEventBus)`) breaks nothing in-tree. `varco_ws`'s `WebSocketEventBus`/`SSEEventBus` (`varco_ws/websocket.py:353`, `sse.py:175`) do **not** subclass `AbstractEventBus` — no tie with the bus seam.

### Feature switches today — three shapes

| Shape | Sites | Switch today |
|---|---|---|
| **Binding-shaped** (`enable_*` swaps a Null default for a real implementation) | `enable_feature_flags` `varco_core/flags/di.py:58-83` (`NullFeatureFlags` → `InMemoryFeatureFlags`); `enable_tenant_membership(container, settings=None)` `tenancy/di.py:77-105` (`NullTenantMembership` → `ClaimTenantMembership`, settings closed over); `enable_policy_authorizer` `varco_casbin/di.py:127-160` (`BaseAuthorizer` → `PolicyEngineAuthorizer`, injects `PolicyEngine`); `enable_token_revocation` / `enable_redis_token_revocation` (**a backend choice, not a feature** — folded into the `revocation` seam above) | code only; no env form |
| **Wrapper-shaped** | `enable_authorization_audit` `varco_core/auth/di.py:49-99` — resolves the live `AbstractAuthorizer` eagerly and rebinds `AuditingAuthorizer(delegate)` | code only |
| **Settings-shaped** (`enabled: bool` consumed by a constructor) | `I18nSettings.enabled=False` (`i18n/settings.py:39`), `TimezoneSettings.enabled=False` (`tz/settings.py:35`), `IdempotencySettings.enabled=False` (`idempotency/settings.py:69`), `ProfilingSettings.enabled=False` (`varco_fastapi/middleware/profiling.py:96`), `SecurityHeadersSettings.enabled=True` (`security_headers.py:154`), `BodyLimitSettings.enabled=True` (`body_limit.py:119`), `RateLimitSettings.enabled=True` (`rate_limit.py:271`); `MigrationSettings.mode="off"` (`migration/settings.py:57`, tri-valued); the import-time `VARCO_PROFILING_ENABLED` kill switch (`profiling/config.py:33`, 048-allowlisted) | env via pydantic; code via an instance (`create_varco_app(i18n=I18nSettings(enabled=True))`, `security_headers: SecurityHeadersSettings \| bool \| None` `app.py:141-143`) |

`create_varco_app` tri-state kwargs (`app.py:104-144`): `security_headers`/`body_limit` are
`Settings | bool | None` (`None` = from env, `False` = force off, instance = on with these);
`migrations`/`tenancy`/`reliability`/`retention`/`jwks_refresh`/`i18n`/`timezone`/`rate_limit` are
`object | None` with no `False` form and (except `i18n`/`timezone`) no env form — 049's modules.

### Script shape to copy (RL-18)

`scripts/api_surface.py:96-139` `discover_packages()` executes `scripts/packages.sh`; `--check`
(`:370-396`) diffs a committed JSON; `make lint` no-`PKG` block `Makefile:163-166` runs
`api-check`/`import-budget`/`asyncapi-check`; CI `.github/workflows/test.yml:65,73`. 048's
`settings_reference.py` (its Step 16-17) is the same shape plus a Markdown page under
`technical_docs/` and a `mkdocs.yml` nav entry — this plan's `di_catalogue.py` is its sibling.

## Design

### §D-mechanism — `@Requires(env=…, value=…)` directly; Open Question 2 re-answered

**Chosen: every varco backend binding of a selectable seam carries `@Requires(env="VARCO_<SEAM>_BACKEND",
value="<name>")` beside its 045 `@Fallback` — written through one varco decorator
`varco_core.di.selected_by(seam, value)` so the env-var name is derived from the seam registry
(§D-vocab) and the value is recorded for `check_backend_selection()`.**

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: `@Requires(env=, value=)` on each backend binding** | One decorator, no container state, no translation function, no profile vocabulary to document as public (the P25 §5 answer's conditions (a)–(c) all disappear); lazy at resolve time (`scope.py:849-854`) so tests `monkeypatch.setenv` and see it on the next `get()`; `validate()` explains an unselected backend as `CONDITION_INACTIVE` with the env var's current state in the message (`container.py:6285-6294`) — the exact "why is this not the live one" 052 needs; composes with `@Fallback` by AND (`scope.py:982-986`) so 045's tiers are untouched; the name mirrors Micronaut's `@Requires(env=)` and Spring's `@ConditionalOnProperty(havingValue=)` (brief 014 §3) | Exact, case-sensitive match (`metadata.py:328-331`): `VARCO_CACHE_BACKEND=Redis` is silently "unset" — closed by `check_backend_selection()`'s normalisation check (§D-check); env-only — cannot see a code-bound settings object (the P25 report's own caveat, `providify-conditional-registration.md:161`). Accepted: a seam *selection* is not a settings field (048 Non-goals say so), and the code tier is a binding, not a value |
| (2) `activate_profile(f"{seam}-{value}")` translation (P25 §5's interim) | Public API at 2.0.1 | Needs a varco-written translation function, a public profile-name vocabulary, `@Profile` on every backend binding, and the two documented edge cases (`container.py:1865-1868`, `:5327-5333` at 2.0.1); `@Profile` names are lower-cased/normalised while env values are user data; profiles are process-wide per container and there is no `describe()` explanation of *why* a profile is off. Everything it offered, (1) offers without the translation layer. **Rejected — superseded by 2.1.0**, which is exactly why 045 Step 3 marks P25 §5 as superseded |
| (3) `@Alternative` + `enable_alternative(cls)` from a bootstrap-time env read | No new decorator | Imperative, evaluated once at bootstrap (not lazy), no `validate()` explanation, and `enable_alternative` is per-class so the env→class mapping is a second registry. Rejected |
| (4) A varco settings field per seam (`CacheSettings.backend`) + provider-body `if` | Code>env via 048 | Selection hidden inside a provider body — the "hidden selection" smell the P25 report was filed about; no `CONDITION_INACTIVE`; every seam needs a settings class it does not otherwise have. Rejected |

**What happens when the env var is unset — precisely.** `RequiresMarker(env="VARCO_CACHE_BACKEND",
value="redis").is_satisfied()` reads `os.environ.get("VARCO_CACHE_BACKEND")` → `None`; because
`value` is not `None` the `elif actual != self.value` branch returns `False` (`metadata.py:320-331`);
`_conditions_hold()` returns `False` (`container.py:586-596`); `_binding_is_active()` returns `False`
(`:2005`); `_filter()` excludes the binding at the activation row (`:2150`) **before** the fallback
post-step; the surviving candidate set for `get(CacheBackend)` is `[InMemoryCache]` (an ungated
`@Fallback` at priority 0, `b.conditions == ()`) → `_get_best_candidate()` returns it (`:2846-2853`).
`validate()` emits one `CONDITION_INACTIVE` INFO for the redis provider whose message ends
`(VARCO_CACHE_BACKEND is unset)` (`:6290,6332-6347`) and **no** `FALLBACK_SHADOWED` (E27,
`:6153-6155`). A set-but-empty value (`VARCO_CACHE_BACKEND=`) behaves identically (`"" != "redis"`).
So: **unset → the seam's in-process `@Fallback` default serves.** Whether that is safe in
production is §D-unset.

### §D-vocab — the value vocabulary and the seam registry

`varco_core/varco_core/di/seams.py` holds the **only** table (frozen dataclasses; interfaces as
lazy dotted strings so `import varco_core.di` still imports no providify and no event/cache module):

| `seam` | env | default value → binding | named values (package → binding) |
|---|---|---|---|
| `cache` | `VARCO_CACHE_BACKEND` | `memory` → `InMemoryCache` (`in_memory`) | `redis` → `RedisCacheConfiguration.redis_cache`; `layered` → `RedisLayeredCacheConfiguration.layered_cache`; `memcached` → `MemcachedCacheConfiguration.memcached_cache` |
| `dlq` | `VARCO_DLQ_BACKEND` | `memory` → `InMemoryDeadLetterQueue` (`in_memory`) | `kafka`; `nats`; `redis` → `RedisDLQConfiguration.dlq`; `redis-streams` → `RedisStreamDLQConfiguration.dlq`; `sa` → **new** `SAModule.dead_letter_queue`; `beanie` → `BeanieDeadLetterQueue` |
| `idempotency` | `VARCO_IDEMPOTENCY_BACKEND` | `memory` → `InMemoryIdempotencyStore` (**newly** `@Fallback @Singleton(qualifier="in_memory")`) | `redis` / `sa` / `beanie` → **new** providers (§D-new-providers) |
| `job_store` | `VARCO_JOB_STORE_BACKEND` | `memory` → `InMemoryJobStore` (**moved** to `varco_core.job.memory`, re-exported from `varco_fastapi.job.store`) | `redis` / `sa` / `beanie` → **new** providers |
| `event_bus` | `VARCO_EVENT_BUS_BACKEND` | `memory` → `InMemoryEventBus` (`in_memory`) | `kafka`; `nats`; `redis` → `RedisEventBusSelectorConfiguration.bus` (+ `RedisEventBus` [B4], same gate; `VARCO_REDIS_USE_STREAMS` still picks Pub/Sub vs Streams *inside* `redis`) |
| `revocation` | `VARCO_REVOCATION_BACKEND` | `null` → `NullTokenRevocationStore` | `memory` → **new** gated provider in `varco_core.revocation.di` (registered by `varco_core.di.bootstrap`); `redis` → **new** gated provider in `varco_redis` (`Inject[RedisEventBusSettings]` → `.url`) |

Plus, for every seam, the dotted form `pkg.mod:Name` (§D-dotted). Rules: values are
`[a-z][a-z0-9-]*`, exact; a value containing `:` is a dotted path; the default value (`memory`/
`null`) is *always known* and gates nothing — setting it explicitly forces the in-process default
even when a backend package is bootstrapped (provenance `env`, not `default`). `selected_by()`
rejects a value equal to the seam's default name at decoration time (`ValueError`) — nothing may
shadow the default's name.

**Two asymmetries, recorded:** (a) the bus's Streams transport stays a settings field
(`VARCO_REDIS_USE_STREAMS`) because 046 §D-streams designed the code path around an explicit
`RedisEventBusSettings(use_streams=True)` instance and one selector provider already exists; the
DLQ gets `redis`/`redis-streams` because two independent `@Configuration`s with separate
`@Disposes` exist and would otherwise tie — merging them into a settings-reading selector is a
refactor with no user-visible gain. ✅ zero refactor, explicit and greppable; ❌ the two Redis
surfaces spell "streams" differently — one Pitfalls row. A future `redis-streams` *bus* value is
purely additive. (b) `revocation`'s default is `null`, not `memory` — `memory` there is an opt-in
(the in-memory store *turns revocation on*), so the seam's vocabulary is `null | memory | redis`.

**The redis three-level shape (045 B3/B4/A2) is kept, not collapsed.** Both B3 (the selector
provider, `BACKEND_PRIORITY`) and B4 (`RedisEventBus`, `qualifier="redis"`, priority 0) get the
same `selected_by("event_bus", "redis")`. If B4 were left ungated, `bootstrap(varco_redis)` with
the var unset would leave B4 (fallback, 0) tied with `InMemoryEventBus` (fallback, 0) for every
unqualified `Inject[AbstractEventBus]` → `AMBIGUOUS_BINDING` ERROR — the tie 045 avoided only
because B3 was always active at 100. ✅ two decorator lines, 045's tests keep their shape; ❌ two
bindings gated on one value (documented). `get(AbstractEventBus, qualifier="redis")` therefore
resolves only when redis is selected — the qualifier is no longer an "even if unselected" escape
hatch for backends (only `qualifier="in_memory"` keeps that role, because the default is ungated).

### §D-precedence — code > env dotted > env named > default, and why 045's `100` stands

```
get(Seam)  →  _filter(): interface/qualifier/priority narrowing, then activation, then fallback post-step
   any non-fallback present?  ──yes──►  every @Fallback dropped  ──►  the code binding wins   (container.py:2168-2169)
            │ no
            ▼  all survivors are @Fallback; max(priority) wins (container.py:2853)
   dotted-path provider   @Fallback, ENV_OVERRIDE_PRIORITY = 200   (active iff VARCO_<SEAM>_BACKEND contains ':')
   named backend          @Fallback, BACKEND_PRIORITY      = 100   (active iff VARCO_<SEAM>_BACKEND == its value)
   in-process default     @Fallback, priority 0                    (always active; qualifier="in_memory"/none)
```

| Tier | Mechanism | Beats the tier below because |
|---|---|---|
| **code** — any non-fallback binding: `bind_instance()`, `enable_*()`, an app `@Singleton`/`provide()` | providify's fallback post-step | `:2168-2169` runs after activation; a user binding at *any* priority (default 0) removes every fallback from the candidate set. Priority is never consulted between a code binding and a varco binding |
| **env dotted** — `VARCO_<SEAM>_BACKEND=pkg.mod:Name` → `@Fallback @Provider(priority=ENV_OVERRIDE_PRIORITY)` | priority among fallbacks | `200 > 100 > 0`; the 045 §D-tiers "headroom above 100 … 047 names it" constant. Mutually exclusive with a named value in practice (one variable) — the tier exists so an ungated third-party `@Fallback` backend at `BACKEND_PRIORITY` still loses to an explicit dotted override |
| **env named** — `@Fallback @Requires(...) @…(priority=BACKEND_PRIORITY)` | priority among fallbacks | `100 > 0`; only one named binding per seam is ever active (one variable, exact match), so two backends never tie |
| **varco default** — `@Fallback` at 0, ungated | sole survivor | — |

**Does 045's `BACKEND_PRIORITY = 100` break "code beats env"? No.** The task's premise that an
env-selected binding is "ALSO non-fallback" describes the rejected alternative:

| Alternative: env-selected backends become **non-fallback** when selected | ✅ | ❌ |
|---|---|---|
| | "selected = a real binding" reads naturally | Two non-fallbacks (user's at 0, backend's at 100) survive the post-step and `max(priority)` picks the **backend** — "code beats env" is false unless `BACKEND_PRIORITY` is pushed below 0, which reintroduces the negative-floor shape 045 §D-tiers option (2) rejected; the user's binding no longer produces a `FALLBACK_SHADOWED` INFO for the backend, so 052 cannot say "your env var is being ignored because code binds this seam"; a binding cannot be fallback-when-unselected and non-fallback-when-selected anyway — `ClassBinding.fallback` is read once at construction (`binding.py:207`). **Rejected; no amendment to 045's constant.** |

**Why no code-side `select_backend(container, "kafka")` verb.** A gated binding is invisible to
every lookup while inactive, including `get(KafkaEventBus)` and `get(T, priority=100)` (E14
narrows *before* activation? — no: `:2148` and `:2150` are in the same comprehension, so an
inactive binding is excluded regardless of priority narrowing). The only non-env signal a
zero-arg predicate could read is process-global state — an in-process copy of the environment,
which 046 forbade `bootstrap()` from mutating. The honest rule: **a shipped backend is selected by
`VARCO_<SEAM>_BACKEND` (set before bootstrap, or `monkeypatch.setenv` in a test); the code tier
is for an implementation you construct yourself** (`bind_instance(container, AbstractEventBus,
KafkaEventBus(settings))` is legal and wins outright). ✅ one mechanism, no hidden global; ❌ a
test that wants kafka live sets one env var (§D-tests' conftest fixture).

### §D-unset — what "unset" means, and production

Unset → the seam's in-process default (`memory`/`null`) is live. This is the correct **development
and test** default (brief 010 §3: a no-op/in-memory default is acceptable for non-security seams;
010 §Librarian #4) and the same zero-config behaviour a bare `varco_core.di.bootstrap()` has
today. It is **not** a safe production posture for five of the six seams — an in-memory cache/
DLQ/job store/idempotency store/bus loses data on restart and has no cross-process visibility —
and for `revocation` it means revocation is off (already a posture finding via
`inspect_revocation_posture()`, 034).

**BREAKING consequence, stated plainly:** `bootstrap(varco_kafka)` alone no longer makes Kafka
the bus. Every deployment that relied on "scanned = live" adds `VARCO_<SEAM>_BACKEND=<name>` for
each seam that package serves (§Migration lists them per package).

| Candidate for "unset" | ✅ | ❌ |
|---|---|---|
| **(1) chosen: strict — unset means the in-process default, always** | One rule, no tie is ever possible (one variable, exact match); zero-config dev/test is in-memory; matches Quarkus `@Alternative` "does not auto-activate without configuration" (brief 010 §3 `010:53`) and brief 015 §Librarian #1 (explicit over discovered); the redis `redis`/`layered` and `redis`/`redis-streams` ties disappear without a per-package "default candidate" rule | A forgotten env var in production silently runs in-memory. Mitigated three ways: the `SecurityPosture` finding `di.in_process_backend` (WARN in production, 054 decides refusal), a `logger.warning` from `check_backend_selection()` when a package has registered values for a seam nobody selected, and 052's report |
| (2) unset means "the sole bootstrapped backend", error if two | No migration for single-backend apps | Needs a "one default value per (package, seam)" rule for redis's two DLQs/two caches; `varco_redis` serves seven seam values, so a redis-cache-plus-kafka-bus app has *two* bus candidates and errors anyway; "sole" is a process-wide import-time fact, so a test process importing a second backend flips behaviour by side effect. Rejected |
| (3) unset means an error whenever any backend package is bootstrapped | Loudest | Breaks every unit test that bootstraps a backend to exercise something else (health checks, channel managers); the in-memory default is the point of the `@Fallback` model. Rejected |

### §D-dotted — the dotted-path override

`VARCO_<SEAM>_BACKEND=pkg.mod:Name` (colon, entry-point style; a value without `:` is a name).
`varco_core.di.bind_env_overrides(container) -> DIContainer`, called by `varco_core.di.bootstrap()`
(046's core entry point — every app runs it first, and all six interfaces live in `varco_core`):

1. For each seam whose variable contains `:` → `importlib.import_module(mod)`, `getattr(mod, name)`.
   Any `ImportError`/`AttributeError` → `BackendSelectionError` naming the env var, the value, the
   underlying error, and the expected shape — **at bootstrap**, never at first `get()`.
2. If the target is a class: `issubclass(target, seam.interface)` else `BackendSelectionError`. If
   it is any other callable: accepted; the instance is `isinstance`-checked at first resolution.
3. Register `container.provide(fn, returns=seam.interface)` where `fn` is a zero-parameter closure
   `def _factory() -> Any: return target()`, stamped `Fallback(Provider(singleton=True,
   priority=ENV_OVERRIDE_PRIORITY)(_factory))` (decorator order irrelevant, `scope.py:1001-1005`;
   both markers are read at `provide()` time — `binding.py:723,727`), `__qualname__ =
   f"varco.di:env_override[{seam}]"` so `describe()`/`validate()` name it.
4. Idempotent per container (a `weakref.WeakKeyDictionary[DIContainer, set[str]]` of registered
   seams, the same shape 048's provenance registry uses); a second `bootstrap()` registers nothing.

**Constructor arguments: the target must be a zero-argument class or a zero-argument callable
(`pkg.mod:make_cache`).** ✅ Django's `SESSION_ENGINE`/`EMAIL_BACKEND` precedent (brief 010 §Librarian
— settings strings for users who do not know DI); a factory function can read its own settings;
no DI knowledge needed. ❌ no injection into a dotted-path class — providify offers no
construct-without-registering API (§Context), and `container.register(cls)` would create a
non-fallback `ClassBinding` at the class's own metadata (a code-tier binding that would beat the
user's *real* code binding by registration order) or require stamping `Fallback`/`Singleton`
onto a user's class at bootstrap (the 045 imperative-stamp ordering hazard, on someone else's
code). Anything that needs injected dependencies is, by definition, the code tier: put
`@Singleton class MyCache(CacheBackend)` in your own scanned package or call `bind_instance()`.
Documented as the rule, with the factory form as the escape hatch.

### §D-check — `check_backend_selection()`: typos and "bootstrapped but unselected"

`varco_core.di.check_backend_selection(container, *, environ=os.environ) -> BackendSelectionReport`
(frozen; per seam: `env`, `raw_value`, `kind: "unset"|"default"|"named"|"dotted"`, `known_values`,
`registered_values` — those `selected_by()` recorded *in this process*, i.e. which backend
modules were imported). It **raises `BackendSelectionError`** when a named value is not the
seam's default name and not in `registered_values` (a typo, a wrong case, or a backend package
that was never bootstrapped — the message lists the known values and says "values are exact and
lower-case"), and **logs `WARNING`** (never raises) when a seam has `registered_values` but the
variable is unset ("varco_kafka is bootstrapped but VARCO_EVENT_BUS_BACKEND is unset — the
in-memory bus is live; set it, or set it to `memory` to silence this"). Called by
`create_varco_app()` when a container is passed (Step 12), by 052's `varco diagnose`, and
documented for bare-container apps. `selected_by()` records values in a module-level
`dict[str, set[str]]` at decoration (import) time — the same lifetime as the marker itself;
`check_backend_selection` is therefore correct only after the backend packages are imported,
which `bootstrap()` guarantees and the docstring states.

### §D-new-providers — the seven bindings DI-4 needs that do not exist

Each is a `@Fallback @Requires-gated @Provider(singleton=True, priority=BACKEND_PRIORITY)` that
constructs the object exactly as today's documented hand-construction does, injecting the
package's settings object; lifecycle is 050's (each docstring says "call `await x.start()`
yourself until Plan 050 wires it"). No new `@Configuration` in `varco_sa`/`varco_beanie` — the
providers go on the existing `SAModule` (`varco_sa/di.py:80`) / `BeanieModule`
(`varco_beanie/di.py:70`); `varco_redis` has no module, so a new scan-discovered
`RedisStoresConfiguration` in `varco_redis/varco_redis/stores.py` holds its three.

| # | Seam/value | Provider | Constructs |
|---|---|---|---|
| N1 | `dlq`/`sa` | `SAModule.dead_letter_queue(config: Inject[SAConfig], serializer: Annotated[Serializer[Event] \| None, InjectMeta(optional=True)] = None)` | `SADeadLetterQueue(config.engine, serializer=serializer)` — the CLAUDE.md "pass `serializer=` explicitly" pitfall closed for the DI path |
| N2 | `idempotency`/`sa` | `SAModule.idempotency_store(config: Inject[SAConfig])` | `SAIdempotencyStore(engine=config.engine)` — **additive kwarg** `engine: AsyncEngine \| None = None` on `SAIdempotencyStore.__init__` (`url` becomes optional; exactly one of the two; `start()` skips `create_async_engine` when an engine was given). ✅ reuses the app's pool; ❌ one signature change (additive, `api_surface` notes it) |
| N3 | `job_store`/`sa` | `SAModule.job_store(config: Inject[SAConfig])` | `SAJobStore(config.engine)` |
| N4 | `idempotency`/`beanie` | `BeanieModule.idempotency_store()` | `BeanieIdempotencyStore()` — docstring: `IdempotencyDocument` must be in the app's `init_beanie()` |
| N5 | `job_store`/`beanie` | `BeanieModule.job_store()` | `BeanieJobStore()` — same note for `JobDocument` |
| N6 | `idempotency`/`redis` | `RedisStoresConfiguration.idempotency_store(settings: Inject[RedisEventBusSettings])` | `RedisIdempotencyStore(url=settings.url)` |
| N7 | `job_store`/`redis` | `RedisStoresConfiguration.job_store(settings: Inject[RedisEventBusSettings])` | `RedisJobStore(aioredis.from_url(settings.url))` |
| N8 | `revocation`/`redis` | `RedisStoresConfiguration.revocation_store(settings: Inject[RedisEventBusSettings])` | `RedisTokenRevocationStore(url=settings.url)` — the env twin of `enable_redis_token_revocation()`, which stays the code path (046's `bind_instance` shape, `**redis_kwargs` kept) |
| N9 | `revocation`/`memory` | `varco_core.revocation.di.in_memory_revocation_store_when_selected()` — registered by `varco_core.di.bootstrap()` via `container.provide()` (not scanned: the "`varco_core` scan activates nothing" rule keeps its literal form even though a gated provider is inert) | `InMemoryTokenRevocationStore()` — the env twin of `enable_token_revocation()` |

`RedisEventBusSettings.url` (`VARCO_REDIS_URL`) is the package's canonical connection URL
(`RedisCacheSettings` has its own `VARCO_REDIS_CACHE_URL` for the cache only); using it for the
three stores means one variable configures "where Redis is" — the `RedisConnectionSettings`
fragment is deliberately not DI-bound (048 §D-connection). A user who needs a different URL per
store uses the code tier.

### §D-features — one convention, two shapes, one helper

**The convention (published verbatim in the guide):**

> A feature is switchable two ways with one name: in code, `enable_<feature>(container)` (binding-
> shaped) or `XSettings(enabled=True)` passed as an instance (settings-shaped); in the environment,
> `VARCO_<FEATURE>_ENABLED=true` (`1`/`true`/`yes`/`on`, case-insensitive, via
> `varco_core.di.env_truthy`). **Code beats env beats default**, for both shapes, by the same
> mechanisms that make backend selection work: a binding-shaped feature's env path is a
> `@Fallback` provider at `BACKEND_PRIORITY` gated by `feature_default("<feature>")`, and its
> code path is the non-fallback binding `enable_*` registers (046's `bind_instance` shape, raises
> `AlreadyBoundError` on repeat); a settings-shaped feature's precedence is 048's per-field rule
> (`instance=` kwargs > env > class default). A multi-state feature uses `VARCO_<FEATURE>_MODE`
> with `off` as one of its values (`MigrationSettings.mode`).

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: gated `@Fallback` twin provider per binding-shaped feature (env) + `enable_*` (code)** | Same tiers, same diagnostics as seams — one mental model; `enable_*` after env-enable is *not* an error and not a no-op (code shadows the twin, `FALLBACK_SHADOWED` INFO says so — the 046 handoff's requirement); lazy, testable with `monkeypatch.setenv`; `validate()` shows `CONDITION_INACTIVE` for an off feature | One provider function per feature written twice in shape (a gated twin beside the code provider) — three sites, each two lines, sharing one factory body |
| (2) `bootstrap()` reads `VARCO_X_ENABLED` and calls `enable_x()` imperatively | Nothing new | A later code `enable_x()` hits 046's `AlreadyBoundError` — code>env is *violated* unless `enable_*` becomes idempotent-for-same-impl, which silently hides the app's-own-binding case 046 §D-enable rejected; evaluated once; no `CONDITION_INACTIVE`. Rejected |
| (3) fold every feature into a settings class with `enabled` and gate on `condition=lambda: settings.enabled` | One shape | The predicate cannot inject the container-bound settings (zero-arg); a module-level settings read reintroduces 048's "hand" shape. Rejected for binding-shaped features |

`varco_core.di.feature_default(feature: str)` = `Fallback` ∘ `Requires(condition=EnvFlag(env))` ∘
`Provider(singleton=True, priority=BACKEND_PRIORITY)` where `EnvFlag` is a small callable class
with `__qualname__ = f"env_truthy({env!r})"` so `CONDITION_INACTIVE` renders
`@Requires(condition=env_truthy('VARCO_FEATURE_FLAGS_ENABLED')) returned False`. Registry
`varco_core/di/features.py` `FEATURES` (name, env, interface path, category, default_on, shape,
code verb) feeds the matrix and the catalogue.

**Sites:** `feature_flags` (`VARCO_FEATURE_FLAGS_ENABLED`; twin in `flags/di.py`, registered by
core bootstrap); `tenant_membership` (`VARCO_TENANT_MEMBERSHIP_ENABLED`; twin injects
`Inject[TenantMembershipSettings]` — bound by 045 A15/048 — instead of closing over it;
`TenantMembershipSettings` gets **no** `enabled` field: settings carry a feature's *parameters*,
the gate is the switch); `policy_authorizer` (`VARCO_POLICY_AUTHORIZER_ENABLED`; twin in
`varco_casbin/di.py`, `Inject[PolicyEngine]`, registered by `varco_casbin.di.bootstrap()` —
the CLAUDE.md rule "never a scanned `@Configuration` that shadows the app's authorizer" is
honoured: it is a gated *fallback*, inert unless the operator sets the variable, and it never
shadows an app binding). `authorization_audit` — **code-only exception**: a wrapper that must
resolve the live `AbstractAuthorizer` cannot be a provider *of* `AbstractAuthorizer` (a self-cycle);
listed in the matrix with "no env form; call last"; 049's module ordering is where an env form
could land.

**Default on/off matrix (published in the guide and README):**

| Category | Default | Rule | Members |
|---|---|---|---|
| **Optional integration** (a library/backend is installed) | **off** | Never on merely because it is importable; on by `VARCO_<SEAM>_BACKEND` / `VARCO_<FEATURE>_ENABLED` / code | every backend of the six seams; `policy_authorizer` (casbin); `feature_flags` (InMemory); `tenant_membership` (claim); `revocation=memory\|redis` |
| **Security feature** | **on** where a safe in-process default exists; **off + posture finding** where it needs configuration | Fail-closed (brief 010 §3) | on: `SecurityHeadersSettings.enabled`, `BodyLimitSettings.enabled`, `RateLimitSettings.enabled` (when a bundle is passed), `MigrationSettings.mode` (`off` — 006's "recommended production posture" is `check`; a finding, 054/052); off + finding: revocation (`null`), `BaseAuthorizer` live (053), `authorization_audit` |
| **Dev-only convenience** | **off in production** | A posture finding when on in production | `ProfilingSettings.enabled`, the in-process backend of any seam (this plan's `di.in_process_backend`), `ErrorEnvelopeSettings.include_detail` (054), `I18nSettings`/`TimezoneSettings` are *not* dev-only — they are optional features, off by default, no finding |

`create_varco_app` tri-state kwargs, defined here and moved by 049: `None` → resolve the
feature's settings from the container (048) and honour `.enabled`; `False` → off regardless of
env (code > env); instance → on with those settings. 049 gives every module the same three states.

### §D-introspect — `inspect_bindings()`, `catalogue()`, and the one private read

`varco_core.di.introspect.inspect_bindings(container) -> tuple[BindingRecord, ...]` walks
`container._bindings` — **the** private read, relocated from 046's testkit shim
(`varco_conformance.di_wiring.fallback_bindings()` becomes a one-line alias over it, so the
count of private-read sites stays one and the P29 guard still names the fix). `BindingRecord`
(frozen): `interface` (qualname), `implementation`, `owner`, `qualifier`, `priority`, `fallback`,
`exact_only`, `conditions: tuple[str, ...]` (`marker.describe()`), `active: bool` (evaluated now
via the marker, never via the container). Only `_bindings` is private; `b.fallback`/`b.conditions`
are plain public attributes (`binding.py:207,212,723,727`).

`varco_core.di.catalogue(container) -> Catalogue` classifies every **fallback** record:
seam default / seam backend (via `SEAMS` + the values `selected_by()` recorded) / feature default
/ feature twin (via `FEATURES`) / settings (implementation is a `BaseSettings` subclass → counted,
not listed; 048's reference owns them) / **other default** (045's A10–A13, A15–A18, B5–B8, B12,
B13, D1–2) with `category` inferred `null` (`Null*`), `in-memory` (`InMemory*`), else
`production-safe`, and an override snippet. A fallback that fits no bucket **raises
`CatalogueError`** — the conformance suite and the script turn a new undeclared default into a
failure, never a silent omission. Non-fallback varco bindings (health checks, C1–7) are ignored.

**JSON schema (`schema_version: 1`; the committed snapshot and 052's runtime report are the same
`Catalogue.to_dict()`):**

```json
{
  "schema_version": 1,
  "providify": "2.1.0",
  "packages": ["varco_core", "..."],
  "seams": [
    {"seam": "event_bus", "env": "VARCO_EVENT_BUS_BACKEND",
     "interface": "varco_core.event.base:AbstractEventBus",
     "default": {"value": "memory", "implementation": "varco_core.event.memory:InMemoryEventBus",
                 "package": "varco_core", "qualifier": "in_memory", "category": "in-memory"},
     "backends": [
       {"value": "kafka", "implementation": "varco_kafka.bus:KafkaEventBus", "package": "varco_kafka",
        "owner": "KafkaEventBus", "qualifier": "kafka", "priority": 100, "shape": "class"}],
     "override": {"env_named": "VARCO_EVENT_BUS_BACKEND=kafka",
                  "env_dotted": "VARCO_EVENT_BUS_BACKEND=pkg.mod:Name",
                  "code": "bind_instance(container, AbstractEventBus, my_bus)"}}],
  "features": [
    {"feature": "feature_flags", "env": "VARCO_FEATURE_FLAGS_ENABLED", "shape": "binding",
     "interface": "varco_core.flags.base:AbstractFeatureFlags", "category": "integration", "default_on": false,
     "default": {"implementation": "varco_core.flags.null:NullFeatureFlags", "category": "null"},
     "enabled": {"implementation": "varco_core.flags.memory:InMemoryFeatureFlags", "owner": "@Provider(feature_flags_when_enabled)"},
     "code": "enable_feature_flags(container)"}],
  "other_defaults": [
    {"interface": "varco_core.observability.config:OtelConfig", "implementation": "...", "package": "varco_core",
     "qualifier": null, "priority": 0, "shape": "provider", "category": "production-safe",
     "override": "container.provide(my_otel_config)"}],
  "settings_defaults_count": 27
}
```

052 adds, at runtime, per seam: `live` (`get_binding()`'s owner), `source`
(`inspect_backend_selection()`), and the two-step revocation flag. It **never reads the committed
file** — the JSON under `design/` is a dev-tree snapshot, not packaged; the function is the source.

`varco_core.di.inspect_backend_selection(container) -> tuple[SeamSelection, ...]` (public reads
only): per seam `value`, `source: "code" | "env-dotted" | "env" | "default"` — `code` iff
`is_user_bound()`; **otherwise by the raw env-string's own shape, not by
`get_binding().priority`**: unset/empty → `default` (`seam.default_value`); `":" in raw` →
`env-dotted`; anything else → `env`. This is a deliberate, owner-ratified deviation from an
earlier draft of this section that specified reading `get_binding().priority` instead
(`200` → dotted, `100` → env, `0` → `default`) — **the shipped shape stands** (repair round,
Decision 3): a priority read cannot distinguish "the app named a backend whose package was never
bootstrapped" from "nothing was set" — both leave the *default's* binding at priority `0`, since
the requested backend's own `@Fallback` binding was simply never registered. The raw env-string
shape reports the *intent* (`source="env"`, naming what the operator asked for) even when that
intent could not be satisfied; that inconsistent state — an env var naming a backend with no
live binding — is exactly what `check_backend_selection()` already raises
`BackendSelectionError` for, so `inspect_backend_selection()` does not need to duplicate that
detection to stay safe. `live_owner` (from `get_binding()`) is the field that *does* report what
actually ended up serving the seam, precisely so `source` and `live_owner` can disagree on this
one edge case without either being wrong. Consumed by `SecurityPosture._collect_di`
(§D-posture) and 052.

### §D-script — `scripts/di_catalogue.py`

Copies `api_surface.py`'s `discover_packages()` verbatim (RL-18: executes `scripts/packages.sh`),
builds **one container per package** exactly as 046's `di_wiring` subclasses do —
`DIContainer()`, `varco_core.di.bootstrap(c)`, `<pkg>.di.bootstrap(c)`; `varco_sa` gets
`bind_instance(c, SAConfig, SAConfig(engine=create_async_engine("sqlite+aiosqlite://"), base=Base,
entity_classes=()))`, `varco_beanie` a lazy `AsyncMongoClient` `BeanieSettings` — nothing connects,
`catalogue()` instantiates nothing — merges the ten `Catalogue`s (a seam's `backends` list is the
union; a duplicate `(seam, value)` across packages is an error), and writes:

- `design/api-freeze-and-standards/measurements/di-catalogue.json` (sorted; what `--check` diffs).
- `technical_docs/di-catalogue.md` — header "generated by `make di-catalogue`, never hand-edited";
  §Seams (one table per seam: value → implementation → package → lifecycle note), §Features (the
  matrix rows with their live members), §Other defaults, §Override cheat-sheet (the three tiers).
  `mkdocs.yml` nav entry beside 048's `settings-reference.md`.

`--check` fails on any difference (same "no additive tier" rule as 048). Wired as `make
di-catalogue-check` inside `make lint`'s no-`PKG` block (`Makefile:163-166`), `make di-catalogue`
regenerator, one step in `test.yml`'s `lint` job after `api_surface.py --check` (`:65`).
**Env-var namespace coordination with 048:** the six `VARCO_<SEAM>_BACKEND` and three
`VARCO_<FEATURE>_ENABLED` names are added to 048's `check_unique_env_names()` input (as
"selection variables", not settings fields) so a future settings field cannot collide with them;
if 048 has not landed, a note is left in 048's Step 3 and the check lives here temporarily
(`test_selection_env_names_are_unique`). ⚠️ ASSUMPTION: pydantic-settings' env source ignores
undeclared prefixed variables (`VARCO_CACHE_BACKEND` beside `CacheSettings`'s `VARCO_CACHE_`
prefix, `VARCO_IDEMPOTENCY_BACKEND` beside `IdempotencySettings`) — only dotenv sources trigger
`extra="forbid"`, and 048 §D-dotenv forbids dotenv; Step 0 measures it.

### §D-posture — the production finding

`varco_fastapi/posture.py` gains `_collect_di(container)` (the `_collect_local` shape,
`:671-700`): for each `SeamSelection` with `value in {"memory"}` (never `null` — revocation is
034's finding) emit `PostureFinding(check="di.in_process_backend", severity=WARN, detail="<seam>
is served by <impl> (<source>) — in-process, lost on restart, single-process only",
remediation="set VARCO_<SEAM>_BACKEND=<one of registered values> or bind_instance(...)")`. The
existing environment demotion makes it INFO outside `production`; `VARCO_SECURITY_SUPPRESS`
applies. Whether production **refuses** on it is 054's call (its FLIP-2 owns `enforce`); this
plan only registers the id, which is never renamed.

### §D-tests — shape, and the conftest fixture

- **Unit, `varco_core`**: `test_di_selection.py` — decorators, `EnvFlag`, `check_backend_selection`
  (typo → error naming known values; unset with registered values → one WARNING log via
  `caplog`; `memory` explicit → no warning), `bind_env_overrides` (dotted resolves via a module
  written to `tmp_path` + `monkeypatch.syspath_prepend`; unknown module / unknown attribute /
  wrong base class → `BackendSelectionError` **at bootstrap**; callable form; wrong instance type →
  error at first `get()`; idempotent; code `bind_instance` beats it; `get_binding().priority ==
  ENV_OVERRIDE_PRIORITY` and `.fallback is True`); `test_di_introspect.py` (`inspect_bindings`,
  `catalogue` buckets, `CatalogueError` on an unclassified fallback, `inspect_backend_selection`
  sources ×4, JSON round-trip against the schema); `test_di_features.py` (twin active with env,
  inactive without; `enable_*` after env → code wins + one `FALLBACK_SHADOWED`; `enable_*` twice →
  `AlreadyBoundError`); the seam-default tests for `InMemoryIdempotencyStore`/`InMemoryJobStore`
  in 046's `test_di_wiring_conformance.py` `expected_defaults` (+2 rows).
- **Per package** (folded into 046's `di_wiring` subclass via the suite's new `selectable`
  attribute, §D-suite): `varco_kafka`, `varco_nats`, `varco_redis`, `varco_memcached`, `varco_sa`,
  `varco_beanie`, `varco_casbin` (feature twin), `varco_fastapi` (posture + `create_varco_app`
  check). All `get_binding()`-based — **no Docker**.
- **Integration (`-m integration`)**: **none new**. `get_binding()` is a pure read
  (`container.py:7429-7433`); the seven new providers construct objects the same way existing
  integration tests already construct them by hand. Existing broker-backed suites gain only the
  conftest fixture below and are re-run once locally (Verification, optional).
- **Conftest fixture (the migration cost of §D-unset in tests):** each backend package's
  `tests/conftest.py` gains an **autouse** `_select_<pkg>_backends(monkeypatch)` fixture setting
  the package's seam variables to its primary value (`varco_redis`: `event_bus=redis`, `dlq=redis`,
  `cache=redis`, `idempotency=redis`, `job_store=redis`, `revocation=redis`; kafka/nats:
  `event_bus`, `dlq`; memcached: `cache`; sa/beanie: `dlq`, `idempotency`, `job_store`). Tests
  that exercise the unset path or a secondary value (`layered`, `redis-streams`) `monkeypatch.
  delenv`/`setenv` explicitly. ✅ one edit per package instead of one per test (Step 0 counts the
  sites; expected > 40); ❌ hides mis-selection inside those packages' own tests — closed by the
  `di_wiring` `selectable` tests, which start from `delenv`. The P24 disposes test
  (`varco_redis/tests/test_redis_cache_disposes.py`) is reworked: resolve under `redis`, flip the
  variable to `layered`, resolve again (lazy predicate; the first singleton is **not evicted**,
  `scope.py:1048-1053`) → both instantiated → `ashutdown()` stops both — the same P24 contract.

### §D-suite — the `di_wiring` conformance addition

`DIWiringConformance` gains `selectable: ClassVar[tuple[Selectable, ...]] = ()` where
`Selectable = (seam: str, value: str, interface: Any, qualifier: str | None, owner_substr: str)`,
and three tests (skipped when the tuple is empty):

| Test | Asserts |
|---|---|
| `test_unselected_backend_is_inactive_and_default_serves` | with the seam's variable **deleted**: `get_binding(interface)` is the in-process default (`.fallback is True`, `.priority == 0`); `validate()` has a `CONDITION_INACTIVE` INFO whose `requested` names the interface and whose message contains `is unset`; no `AMBIGUOUS_BINDING` |
| `test_selected_backend_is_a_fallback_at_backend_priority` | with `setenv(env, value)`: `get_binding(interface, qualifier=q)` has `.fallback is True`, `.priority == BACKEND_PRIORITY`, and `owner_substr` in its owner; `get_binding(interface, qualifier="in_memory")` (where the seam has one) is still the default; `validate()` has **no** `CONDITION_INACTIVE` for this owner and no `AMBIGUOUS_BINDING` |
| `test_code_binding_beats_env_selection` | with the variable set: `bind_instance(c, interface, MagicMock(spec=interface))` registered **after** bootstrap → `get_binding(interface).fallback is False`; for provider-shaped backends, one `FALLBACK_SHADOWED` INFO names the user's binding |

Plus `test_unknown_value_fails_check` (`setenv(env, "nope")` → `check_backend_selection` raises,
message lists `value`) and `test_catalogue_classifies_every_default` (`catalogue(container)` does
not raise; every `(interface, qualifier)` in `expected_defaults` appears in some bucket). The
task's phrasing "env-selected backend is non-fallback" is **deliberately inverted** here —
§D-precedence is why.

### §D-docs — where each sentence goes

- `technical_docs/features/dependency-injection.md` — new **§Selecting a backend** (the vocabulary
  table, the three-tier diagram, "unset means", the dotted-path rule and its zero-arg contract,
  `check_backend_selection`, the redis `use_streams` vs `redis-streams` asymmetry, the
  `qualifier="kafka"`-only-when-selected note) and **§Enabling a feature** (the convention verbatim,
  the matrix, the `authorization_audit` exception, the tri-state kwargs), a pointer to the generated
  `di-catalogue.md`, and **Pitfalls** rows: bootstrapped-but-unselected; `Redis` ≠ `redis`; a
  dotted class with constructor args; `enable_*` vs env "which won?" (read `report.infos`); the
  P24-style not-evicted singleton when a test flips the variable; `get(T, qualifier="kafka")`
  `LookupError` when unselected.
- `README.md` — two new subsections under the DI section: **"Choosing a backend"** (the six
  variables, one compose-file snippet, the dotted form, `bind_instance` for your own) and
  **"Enabling a feature"** (the matrix in short form, one `enable_feature_flags` + one
  `VARCO_FEATURE_FLAGS_ENABLED=true` example); every existing `bootstrap(varco_kafka)` snippet gains
  its env line; `VARCO_REDIS_USE_STREAMS` prose says it picks the transport *within* `redis`.
- `CLAUDE.md` — under "DI wiring (providify)": three lines (*Rule: a backend of one of the six
  seams is live only when `VARCO_<SEAM>_BACKEND` names it (or a dotted path) — bootstrapping a
  package never makes it live; code binding > env dotted > env named > `@Fallback` default; every
  backend binding is `@Fallback` **and** `selected_by(...)`, never a plain non-fallback. Rule: a
  binding-shaped feature is switched by `enable_*(container)` or `VARCO_<FEATURE>_ENABLED`, through
  `feature_default(...)`; never an imperative env read in `bootstrap()`. Regenerate
  `make di-catalogue` or `make lint` fails.*); the Commands section gets `di-catalogue-check`
  beside `api-check`; the decision tree gets one branch (*"Making a backend/feature selectable or
  adding a default?" → `varco_core.di` (`selected_by`/`feature_default`/`SEAMS`/`FEATURES`), never
  a provider-body `if`, never `activate_profile`*); the `enable_policy_authorizer` bullet gains
  "or `VARCO_POLICY_AUTHORIZER_ENABLED=true`"; the revocation two-step pitfall gains "the env form
  `VARCO_REVOCATION_BACKEND=redis` has the same second step".
- `ARCHITECTURE.md` — `varco_core.di` package listing; the catalogue in the docs map.
- `technical_docs/common-pitfalls.md` — two rows (bootstrapped-but-unselected; exact-case value).
- `technical_docs/features/{dead-letter-queues,cache-hardening,idempotency-key,job-scheduling-and-leases,credential-and-token-lifecycle,casbin-authorization,feature-flags,tenant-provenance}.md`
  — one "Selecting/enabling via env" paragraph each, pointing at the guide.
- `varco_redis/varco_redis/di.py:21-63` module docstring rewritten (bus selection = the env var;
  `_redis_active_bus` ghosts at `:25,123,162` deleted).
- `CHANGELOG.md` `[Unreleased]`: `### BREAKING — config-driven backend selection, one feature-enable
  convention (Plan 047, DI-4 / DI-12 / DI-17)` = §Migration; `### Added` (`varco_core.di` package
  names, the seven providers, `scripts/di_catalogue.py`, the posture finding, the `di_wiring`
  addition). `BACKLOG.md:90,93,94` → `done (Plan 047)`; `:130-134` Open Question 2 gains
  "**Re-answered (Plan 047 §D-mechanism):** providify 2.1.0 ships `@Requires(env=, value=)`; used
  directly, no profile translation".

## Steps

All commands from `/home/edoardo/projects/varco`. Steps 0–4 are the core and TDD-ordered; 5–9
the packages (independent of each other after 4); 10–12 introspection/script/posture; 13–16
docs, changelog, gates, handoff. `make test PKG=<pkg>` after every step touching `<pkg>`.

### Preflight and the core primitives

0. [x] **Preflight.** (a) 045 + 046 landed: `rg -c '@Fallback' varco_*/varco_*` ≥ 55;
       `rg -n 'def bootstrap\(container: DIContainer\) -> DIContainer' varco_*/varco_*/di.py | wc -l`
       → 10; `ls testkit/varco_conformance/di_wiring.py`; providify `2.1.0` installed. Whether 048
       landed (`ls varco_core/varco_core/settings/bind.py`) — record; it changes only Steps 8/13's
       "how settings are bound" wording, never a decision. (b) Re-read every `providify/…` line in
       §Context on the installed copy; fix drift in this plan first. (c) Measure the ⚠️s: a
       `CacheSettings()` / `IdempotencySettings()` construction with `VARCO_CACHE_BACKEND=redis` /
       `VARCO_IDEMPOTENCY_BACKEND=redis` exported (expect: no error, §D-script ⚠️); `rg -n
       'a?get(_binding)?\((AbstractEventBus|CacheBackend|AbstractDeadLetterQueue|AbstractJobStore|AbstractTokenRevocationStore)' varco_*/tests examples | wc -l`
       (the §D-tests fixture justification); `rg -n '_redis_active_bus' -t py -t md` (expect 3 +
       docs); `rg -n 'aiosqlite|asyncpg' varco_sa/pyproject.toml pyproject.toml` (the script's lazy
       engine, §D-script). (d) Paste everything into
       `/tmp/claude-1000/…/scratchpad/047-preflight.txt`. **Verify**: the file has (a)–(c).

1. [x] **`varco_core/varco_core/di.py` → package `varco_core/varco_core/di/`** — `__init__.py`
       re-exports 045/046's names unchanged (`BACKEND_PRIORITY`, `AlreadyBoundError`,
       `bind_instance`, `is_user_bound`, `bootstrap`) plus the new ones; new constant
       `ENV_OVERRIDE_PRIORITY: Final[int] = 200` with the §D-precedence diagram as its docstring;
       `seams.py` (`Seam` frozen dataclass, `SEAMS` per §D-vocab, `seam_by_name()`,
       `interface_of(seam)` resolving the dotted interface string lazily); `errors.py`
       (`BackendSelectionError(RuntimeError)`, `CatalogueError(RuntimeError)`). providify imported
       only inside function bodies (046's import-budget rule; `# noqa: PLC0415` with the reason).
       Tests first: `varco_core/tests/test_di_constants.py` gains `ENV_OVERRIDE_PRIORITY >
       BACKEND_PRIORITY > 0` and keeps the `sys.modules` no-providify delta; new
       `varco_core/tests/test_di_seams.py` — six seams, env names `VARCO_<SEAM>_BACKEND`, default
       names, `interface_of()` resolves each ABC, names are unique, `value` regex.
       **Verify**: both files green; `uv run python scripts/import_budget.py --check --warn-only` unchanged.

2. [x] `varco_core/tests/test_di_selection.py` (**new, failing first**) per §D-tests row 1, then
       `varco_core/varco_core/di/select.py`: `selected_by(seam, value)` (validates name/value,
       records into `_REGISTERED_VALUES: dict[str, set[str]]`, returns `Requires(env=…, value=…)`
       applied to the target — the value must not equal the seam's default name), `EnvFlag`,
       `env_truthy(name)`, `BackendSelectionReport`/`SeamSelection` (frozen),
       `check_backend_selection(container, *, environ=None)`, `bind_env_overrides(container)`
       (§D-dotted, with the per-container `WeakKeyDictionary`), `inspect_backend_selection(container)`.
       Full docstrings (Args/Returns/Raises/Edge cases/Thread safety — the registry is import-time,
       single-threaded; Async safety — no awaits) and a `DESIGN:` block per §D-mechanism/§D-dotted/
       §D-check with the ✅/❌ rows. **Verify**: the test file green.
       **REPAIR ROUND 2 (Decision 3) note:** the owner ruled that
       `inspect_backend_selection()`'s shipped raw-env-string-shape `source` derivation stands as
       the intended contract — §D-introspect's prose (above) is corrected to describe it that way
       instead of the `get_binding().priority` reading it originally specified. The function's own
       docstring now carries the same `Edge cases:` explanation (the not-bootstrapped case, and
       why `source`/`live_owner` may disagree there). Pinned by
       `test_inspect_backend_selection_sources_env_when_backend_package_not_bootstrapped`
       (`varco_core/tests/test_di_introspect.py`).

3. [x] `varco_core/tests/test_di_features.py` (**new, failing first**) per §D-tests, then
       `varco_core/varco_core/di/features.py`: `Feature` frozen dataclass, `FEATURES` (the three
       binding-shaped + the settings-shaped rows + `authorization_audit` marked `shape="wrapper",
       env=None`), `feature_default(feature)` decorator (§D-features). **Verify**: green.

4. [x] **Core bootstrap and the two seam defaults.** `varco_core/varco_core/di/__init__.py`
       `bootstrap()` = 046's scan + `bind_env_overrides(container)` + `container.provide(in_memory_revocation_store_when_selected)`
       and `provide(feature_flags_when_enabled)` guarded by the per-container registry (idempotent).
       `varco_core/varco_core/revocation/di.py` — add the N9 twin (`@selected_by("revocation","memory")
       @Fallback @Provider(singleton=True, priority=BACKEND_PRIORITY)`, public name so it is
       *not* skipped by scan — but it is registered by `bootstrap()`, and `revocation/di.py` is
       not scan-activated per 045 E2's note); `flags/di.py` — the `feature_flags_when_enabled` twin;
       `tenancy/di.py` — `tenant_membership_when_enabled(settings: Inject[TenantMembershipSettings])`
       twin (registered by core bootstrap). `varco_core/varco_core/idempotency/memory.py:37` →
       `@Fallback @Singleton(qualifier="in_memory")` (docstring: the in-process default; A1-shape).
       **Move** `InMemoryJobStore` to `varco_core/varco_core/job/memory.py` (`@Fallback
       @Singleton(qualifier="in_memory")`, drop the `sys` import); `varco_fastapi/varco_fastapi/job/store.py`
       becomes a re-export module (`from varco_core.job.memory import InMemoryJobStore`, `__all__`
       kept; module docstring says "moved in 4.0.0"). Update 046's
       `varco_core/tests/test_di_wiring_conformance.py` `expected_defaults` (+ `(AbstractIdempotencyStore,"in_memory")`,
       `(AbstractJobStore,"in_memory")`) and `varco_fastapi/tests/test_fastapi_di_wiring.py`
       (− `(AbstractJobStore,None)`). Tests: `test_revocation_di.py` (env `memory` → twin live at
       100; `enable_token_revocation()` after → code wins, one `FALLBACK_SHADOWED`),
       `test_flags_di.py`, `test_tenant_membership_di.py` (same shape), `test_idempotency_memory.py`
       + `varco_core/tests/test_job_store_memory.py` (resolvable by default, `in_memory` qualifier).
       **Verify**: `make test PKG=varco_core`, `PKG=varco_fastapi`;
       `uv run python scripts/api_surface.py --check` reports the `InMemoryJobStore` move as a
       **note** only (regenerate the snapshot in Step 15 regardless).

### The backends

5. [x] **`varco_kafka`, `varco_nats`** — `@selected_by("event_bus","kafka")` on `KafkaEventBus`
       (`bus.py:95`, beside 045's `@Fallback @Singleton(priority=BACKEND_PRIORITY, qualifier="kafka")`),
       `@selected_by("dlq","kafka")` on `KafkaDLQConfiguration.dlq` (`dlq.py:787` — the **method**,
       `scope.py:878-883`); nats likewise (`bus.py:129`, `dlq.py:773`). Docstrings: one sentence
       each ("live only when `VARCO_EVENT_BUS_BACKEND=kafka`"). `tests/conftest.py` autouse
       fixture (§D-tests). `test_kafka_di_wiring.py` / `test_nats_di_wiring.py`: `selectable =
       (("event_bus","kafka",AbstractEventBus,"kafka","KafkaEventBus"), ("dlq","kafka",AbstractDeadLetterQueue,None,"dlq"))`.
       Existing `test_kafka_di.py:166`-style resolutions pass via the fixture.
       **Verify**: `make test PKG=varco_kafka`, `PKG=varco_nats`.

6. [x] **`varco_redis`** — gates: `RedisEventBusSelectorConfiguration.bus` (`bus.py:532`) and
       `RedisEventBus` (`bus.py:102`) both `@selected_by("event_bus","redis")`;
       `RedisDLQConfiguration.dlq` `("dlq","redis")`; `RedisStreamDLQConfiguration.dlq`
       `("dlq","redis-streams")`; `RedisCacheConfiguration.redis_cache` `("cache","redis")`;
       `RedisLayeredCacheConfiguration.layered_cache` `("cache","layered")`. **New**
       `varco_redis/varco_redis/stores.py` `@Configuration class RedisStoresConfiguration` with
       N6/N7/N8 (each `@Fallback @selected_by(...) @Provider(singleton=True, priority=BACKEND_PRIORITY)`,
       full docstrings incl. the 050 lifecycle note and the `VARCO_REDIS_URL` sharing rule).
       `enable_redis_token_revocation` unchanged (046 shape) — docstring gains "env form:
       `VARCO_REVOCATION_BACKEND=redis`; same second step". `di.py:21-63,123,162` docstrings per
       §D-docs. Tests: conftest fixture; `test_redis_di_wiring.py` `selectable` (event_bus/redis
       ×2 owners — the selector *and* `RedisEventBus` under `qualifier="redis"` —, dlq/redis,
       dlq/redis-streams, cache/redis, cache/layered, idempotency/redis, job_store/redis,
       revocation/redis); the P24 rework of `test_redis_cache_disposes.py` (§D-tests); 046's
       streams-replacement test keeps `bind_instance(RedisEventBusSettings(use_streams=True))` and
       now also `setenv("VARCO_EVENT_BUS_BACKEND","redis")`; `test_redis_stores_di.py` (`get_binding`
       for N6–N8, no server). **Verify**: `make test PKG=varco_redis`; `rg -n '_redis_active_bus'
       varco_redis` → 0.

7. [x] **`varco_memcached`** — `@selected_by("cache","memcached")` on
       `MemcachedCacheConfiguration.memcached_cache` (`cache.py:647`); conftest fixture;
       `test_memcached_di_wiring.py` `selectable`. **Verify**: `make test PKG=varco_memcached`.

8. [x] **`varco_sa`, `varco_beanie`** — `SAIdempotencyStore.__init__(*, url: str | None = None,
       engine: AsyncEngine | None = None, **engine_kwargs)` (exactly one; `start()` honours a given
       engine; `stop()` disposes only an engine it created — docstring `DESIGN:` ✅/❌); `SAModule`
       gains N1/N2/N3; `BeanieModule` gains N4/N5; `@selected_by("dlq","beanie")` on
       `BeanieDeadLetterQueue` (`dlq.py:122`). Tests: `test_sa_idempotency.py` gains the
       `engine=` construction + "neither/both → `ValueError`"; conftest fixtures;
       `test_sa_di_wiring.py`/`test_beanie_di_wiring.py` `selectable` rows (sa: dlq/idempotency/
       job_store; beanie: dlq/idempotency/job_store); `get_binding` only. **Verify**: `make test
       PKG=varco_sa`, `PKG=varco_beanie` (no Docker); `api_surface.py --check` shows the
       `SAIdempotencyStore` signature change as a *function* note only if it is exported top-level
       (it is a class — invisible; recorded in §Migration regardless).

9. [x] **`varco_casbin`** — `policy_authorizer_when_enabled(engine: Inject[PolicyEngine]) ->
       AbstractAuthorizer` twin (`@feature_default("policy_authorizer")`), registered by
       `bootstrap()` via `provide()` (idempotent guard); `enable_policy_authorizer` unchanged
       (046). CLAUDE.md's "never a scanned `@Configuration`" rule stays literally true. Tests in
       `test_casbin_di_wiring.py`: env → twin live, `BaseAuthorizer` shadowed by priority;
       `enable_policy_authorizer()` after env → code wins; app-bound `AbstractAuthorizer` + env →
       app wins (no error — the env form never raises where a code form would be `AlreadyBoundError`,
       stated in the docstring). **Verify**: `make test PKG=varco_casbin`.

### Introspection, the suite, the script, posture

10. [x] `varco_core/tests/test_di_introspect.py` (**failing first**) then
        `varco_core/varco_core/di/introspect.py`: `BindingRecord`, `inspect_bindings()` (the one
        `_bindings` read, docstring naming P29 and the testkit alias), `Catalogue` +
        `catalogue(container)` + `to_dict()`/`from_dict()` (§D-introspect schema), `CatalogueError`.
        `testkit/varco_conformance/di_wiring.py`: `fallback_bindings()` → alias over
        `inspect_bindings()` (module docstring updated: "the private read now lives in
        `varco_core.di.introspect`; P29 still names it"); add `selectable` + the five tests of
        §D-suite; `COVERAGE.md` `di_wiring` row text mentions `selectable`. Update every subclass
        (Steps 4–9 already added rows). **Verify**: `for p in core kafka nats redis memcached ws
        casbin sa beanie fastapi; do uv run pytest varco_$p/tests/test_${p}_di_wiring.py -q; done`
        (ws has an empty `selectable` → skipped tests, not failures).

11. [x] `scripts/di_catalogue.py` (**new**, §D-script) + `varco_core/tests/test_di_catalogue_script.py`
        (in-process against `varco_core` only: deterministic, `--check` exit codes, a package that
        fails to import → non-zero with the name, duplicate `(seam, value)` → error). Generate and
        commit `design/api-freeze-and-standards/measurements/di-catalogue.json` and
        `technical_docs/di-catalogue.md`; `Makefile` `di-catalogue` / `di-catalogue-check` (the
        `api-check` shape `:176-178`, added to `lint`'s no-`PKG` block `:163-166`, `make help`);
        `.github/workflows/test.yml` one step after `:65`; `mkdocs.yml` nav. Coordinate with 048's
        `check_unique_env_names()` (§D-script). **Verify**: `uv run python scripts/di_catalogue.py
        --check` → 0; `make lint` runs it; `make lint PKG=varco_core` does not; `make docs`.

12. [x] **Posture + app check.** `varco_fastapi/varco_fastapi/posture.py` `_collect_di(container)`
        (§D-posture) wired into the collector list beside `_collect_local`; `create_varco_app()`
        calls `check_backend_selection(container)` when `container is not None` (before the
        lifespan is built; a `BackendSelectionError` therefore fails app construction — the
        fail-fast 048 also chose). Tests: `varco_fastapi/tests/test_posture_di.py` (production +
        memory bus → one WARN finding with the id; `VARCO_EVENT_BUS_BACKEND=memory` explicit → still
        a finding, `source="env"` in the detail; a code-bound bus → no finding; development →
        demoted) and `test_app_backend_check.py` (`VARCO_CACHE_BACKEND=nope` → `create_varco_app`
        raises `BackendSelectionError`). **Verify**: `make test PKG=varco_fastapi`.

### Docs, changelog, gates, handoff

13. [x] **Docs** per §D-docs (guide, README, CLAUDE.md three lines + commands line + decision-tree
        branch + the two bullet amendments, ARCHITECTURE.md, common-pitfalls ×2, the eight feature
        docs, the redis module docstring, `examples/00-full-stack-post-api`'s compose/`.env`/README
        gaining `VARCO_EVENT_BUS_BACKEND=redis` etc. for the backends it bootstraps — and its test
        conftest the same fixture shape). **Verify**: `rg -n '_redis_active_bus|first-scanned|scan order'
        README.md ARCHITECTURE.md CLAUDE.md technical_docs varco_*/varco_*` → 0; `make docs`;
        `make asyncapi-check` unchanged (consumers, not the bus, feed it).

14. [x] `CHANGELOG.md` `[Unreleased]` (§D-docs; BREAKING block = §Migration verbatim);
        `BACKLOG.md:90,93,94` → `done (Plan 047)`; `:130-134` Open Question 2 re-answer;
        `design/upstream-gaps/providify-conditional-registration.md` §5 — confirm 045's Resolution
        section already says "047 uses `@Requires` directly"; if not, one line. **Verify**:
        `rg -n 'Plan 047' CHANGELOG.md BACKLOG.md` ≥ 4.

15. [x] **Gates + snapshots.** `uv run python scripts/api_surface.py` (expected: the
        `InMemoryJobStore` module-move note if it is a top-level `varco_fastapi` export; no
        removals) — commit `api-surface.{json,md}` here; `make lint` (`api-check`,
        `asyncapi-check`, `import-budget --warn-only`, `di-catalogue-check`, and 048's
        `settings-check` if landed — all green); `make type-check`; `make test` (eleven suites,
        `0 xpassed`; `xfailed` unchanged from 046's count — this plan files no gap).

16. [x] **Handoff notes** (one paragraph each, appended to the neighbour's header/"Depends on"
        line or the index row): **049** — the tri-state semantics (§D-features last paragraph) and
        the `FEATURES` registry are what each module's `enabled` state must honour; `VARCO_
        <FEATURE>_ENABLED` names are final. **050** — N1–N9's docstrings carry the "lifecycle is
        050's" line; `SAIdempotencyStore(engine=)` exists for it; the redis stores share one
        `aioredis` client per provider today. **052** — `catalogue(container).to_dict()` is the
        report body (schema v1); `inspect_backend_selection()` gives `source`; `check_backend_
        selection()`'s report gives the "bootstrapped but unselected" warning; render
        `CONDITION_INACTIVE` beside `FALLBACK_SHADOWED`. **054** — the `di.in_process_backend`
        finding id exists at WARN; refusing on it in production is yours. **058** — §Selecting a
        backend / §Enabling a feature are final; §Migration below is the DI-4/DI-12/DI-17 entry;
        `di-catalogue.md` is generated, link it, never edit it.

## Edge cases

- **Variable unset** → in-process default; one `CONDITION_INACTIVE` INFO per gated binding; no
  `FALLBACK_SHADOWED` (E27). **Set but empty** → identical (`"" != value`; `check_backend_selection`
  treats it as unset).
- **`VARCO_CACHE_BACKEND=Redis`** → gate false (exact match); `check_backend_selection` raises
  ("values are exact and lower-case; known: memory, redis, layered, memcached").
- **Value names a backend whose package was not bootstrapped** (`kafka` in a redis-only app) →
  not in `registered_values` → `BackendSelectionError` — never a silent in-memory fallback.
- **Both a dotted path and code** → code wins (post-step); `validate()` has `FALLBACK_SHADOWED`
  for `varco.di:env_override[cache]` naming the user's binding — 052 shows "env ignored".
- **Dotted path to a class with required constructor args** → `TypeError` inside the provider at
  first `get()`, wrapped as `BackendSelectionError("… takes no arguments; use a zero-arg factory
  `pkg.mod:make_x` or bind it in code")`; the bootstrap-time check cannot see it (signature
  inspection is deliberately not attempted — a `__init__(self, *, url=...)` with defaults is fine).
- **Dotted path resolves to something that is neither a class nor callable** → error at bootstrap.
- **`selected_by` value equals the seam's default name** (`"memory"`) → `ValueError` at import.
- **Two packages register the same `(seam, value)`** (a plugin re-using `redis`) → `selected_by`
  records it silently (a set), but `catalogue()` raises `CatalogueError` on the duplicate and the
  script fails — the plugin must pick a distinct value.
- **`get(AbstractEventBus, qualifier="kafka")` while unselected** → providify `LookupError`
  ("No binding found … qualifier='kafka'"); documented; `qualifier="in_memory"` never fails.
- **A test flips the variable after a singleton resolved** → not evicted (`scope.py:1048-1053`,
  `:872-876`); the P24 rework relies on exactly this; a test wanting a fresh backend uses a fresh
  container.
- **`enable_feature_flags()` with `VARCO_FEATURE_FLAGS_ENABLED=true` also set** → code wins, one
  INFO; **twice** → `AlreadyBoundError` (046). **Env set, app binds its own `AbstractFeatureFlags`**
  → app wins, twin shadowed, no error.
- **`VARCO_REVOCATION_BACKEND=memory` and `enable_redis_token_revocation()`** → code (redis) wins;
  the registry still needs `TrustedIssuerRegistry(revocation_store=…)` — the two-step pitfall is
  unchanged and 052's `registry_wired` reports it.
- **`varco_redis` bootstrapped, `VARCO_DLQ_BACKEND=redis`, `VARCO_REDIS_USE_STREAMS=true`** →
  the Streams *bus* with the list-based `RedisDLQ` — legal, works, and a Pitfalls row says
  `redis-streams` is the matching DLQ.
- **`ConditionEvaluationError`** cannot arise from `env=`/`value=` markers (no user code); it can
  from a `feature_default` `EnvFlag` only if `os.environ` itself misbehaves — not guarded.
- **`check_backend_selection` called before the backend packages are imported** → false
  "unknown value" — the docstring says "call after every `bootstrap()`"; `create_varco_app` is
  such a point.
- **`--check` on a tree where a package fails to bootstrap** → non-zero naming the package,
  never a shorter snapshot (same as `api_surface.py`).

## Verification

```bash
uv run python -c "import importlib.metadata as m; print(m.version('providify'))"      # 2.1.0
uv run pytest varco_core/tests/test_di_constants.py varco_core/tests/test_di_seams.py \
    varco_core/tests/test_di_selection.py varco_core/tests/test_di_features.py \
    varco_core/tests/test_di_introspect.py varco_core/tests/test_di_catalogue_script.py -q
for p in core kafka nats redis memcached ws casbin sa beanie fastapi; do uv run pytest varco_$p/tests/test_${p}_di_wiring.py -q; done
uv run pytest varco_redis/tests/test_redis_cache_disposes.py varco_redis/tests/test_redis_stores_di.py \
    varco_fastapi/tests/test_posture_di.py varco_fastapi/tests/test_app_backend_check.py -q
rg -n 'selected_by\(' varco_*/varco_* | wc -l                    # >= 20 (B1,B2,B3,B4,B9,B10x4,B11x3,N1-N9)
rg -n '_redis_active_bus' -t py -t md                             # 0 outside CHANGELOG/plans
rg -n 'activate_profile' varco_*/varco_*                          # 0
uv run python scripts/di_catalogue.py --check                     # 0
uv run python scripts/api_surface.py --check                      # 0 (after Step 15's regen)
make lint            # ruff + api-check + import-budget + asyncapi-check + di-catalogue-check (+ settings-check)
make lint PKG=varco_core                                          # must NOT run di-catalogue-check
make type-check && make test && make docs
# optional, local, Docker: confirm the conftest fixtures keep the broker suites green
make integration-test PKG=varco_redis
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — the installed providify 2.1.0 matches the tree at every cited line (`scope.py:818-967`, `metadata.py:230-367`, `container.py:549-596,1904-2005,2129-2170,2814-2853,6296-6347,6508-6524`, `binding.py:207,212,723,727`) | Gate + fallback + priority semantics are what runs | Step 0(b) |
| ⚠️ ASSUMPTION — pydantic-settings ignores undeclared `VARCO_CACHE_BACKEND`/`VARCO_IDEMPOTENCY_BACKEND` beside `CacheSettings`/`IdempotencySettings` (env source, no dotenv) | Setting a seam variable never breaks a settings class | Step 0(c) measures; if it raises, the two seams' variables become `VARCO_CACHE_SELECT_BACKEND`-style *before* any code is written and the vocabulary table is updated — a naming change, not a design change |
| ⚠️ ASSUMPTION — `@Requires` on a `@Configuration` **method** is honoured for scan-installed configurations the same way as for `install()` (`scope.py:878-883` says the marker is read off the provider function; `binding.py:727` reads `fn`) | B3/B10/B11/N1–N8 gates work | Step 0(b) executes a two-line check; 046's redis subclass is the first consumer |
| ⚠️ ASSUMPTION — `scan("varco_sa")`/`scan("varco_beanie")` with the lazy engine/client is import-clean (046 measured) and the catalogue script can construct `SAConfig` with a sqlite or asyncpg URL without connecting | `di_catalogue.py` runs in CI without Docker | Step 0(c); fallback: the script accepts `--skip PKG` with a loud note in the JSON, never silently |
| ⚠️ ASSUMPTION — more than ~40 test sites resolve a backend after bootstrap (§D-tests) | The autouse conftest fixture is the smaller edit | Step 0(c) counts; below 25, explicit `monkeypatch.setenv` per test instead and no autouse fixture |
| Silent in-memory in production after a forgotten variable (§D-unset) | An operator is told three ways | `di.in_process_backend` posture finding (WARN; 054 may refuse), `check_backend_selection()` WARNING log, 052's report; §Migration lists the exact variable per package |
| A user copies a backend decorator line including `selected_by(...)` onto their own class | Their binding is gated on *our* variable | Pitfalls row; `selected_by` rejects the default name and the docstring says "for varco-shipped backends; your own implementation is a plain binding" |
| `InMemoryJobStore` move breaks `from varco_fastapi.job.store import InMemoryJobStore` | Import path keeps working | Re-export module kept; `api_surface` move note only |
| `SAIdempotencyStore.__init__` signature change | `url=` callers unchanged | `url` stays first-keyword-optional; both-given/neither-given raise `ValueError`; test |
| `check_backend_selection()` false positive when called early | Only after every `bootstrap()` | Docstring + the two sanctioned call sites (`create_varco_app`, 052) |
| The catalogue's `other_defaults` bucket rots as plans add defaults | Every fallback is classified or the build fails | `CatalogueError` + `test_catalogue_classifies_every_default` in the suite |
| 048 lands after this plan | Settings still bound (045's `@Fallback` providers) | The gated providers inject settings by type; nothing here reads a `bind_settings` API |
| The redis `use_streams` / `redis-streams` asymmetry confuses | One Pitfalls row; `catalogue.md` shows both under `redis` | §D-vocab records it; a bus value is additive later |

## Migration (3.x → 4.0, DI-4 / DI-12 / DI-17 entry — Plan 058 assembles)

**BREAKING — bootstrapping a backend package no longer makes it live.** Set one variable per seam
the package serves (or bind your own implementation in code, which wins over any variable):

| Package you bootstrap | Add to the environment |
|---|---|
| `varco_kafka` | `VARCO_EVENT_BUS_BACKEND=kafka`, `VARCO_DLQ_BACKEND=kafka` |
| `varco_nats` | `VARCO_EVENT_BUS_BACKEND=nats`, `VARCO_DLQ_BACKEND=nats` |
| `varco_redis` | `VARCO_EVENT_BUS_BACKEND=redis` (+ `VARCO_REDIS_USE_STREAMS=true` for Streams, unchanged), `VARCO_DLQ_BACKEND=redis` or `redis-streams`, `VARCO_CACHE_BACKEND=redis` or `layered`, and — new capabilities — `VARCO_IDEMPOTENCY_BACKEND=redis`, `VARCO_JOB_STORE_BACKEND=redis`, `VARCO_REVOCATION_BACKEND=redis` |
| `varco_memcached` | `VARCO_CACHE_BACKEND=memcached` |
| `varco_sa` | `VARCO_DLQ_BACKEND=sa`, `VARCO_IDEMPOTENCY_BACKEND=sa`, `VARCO_JOB_STORE_BACKEND=sa` (all three are new DI bindings; the classes were hand-constructed before) |
| `varco_beanie` | `VARCO_DLQ_BACKEND=beanie`, `VARCO_IDEMPOTENCY_BACKEND=beanie`, `VARCO_JOB_STORE_BACKEND=beanie` |

Unset means the in-process default (`memory`; `null` for revocation) — in production this is
reported by `SecurityPosture` as `di.in_process_backend`. `VARCO_<SEAM>_BACKEND=memory` forces the
default explicitly. Values are exact and lower-case; an unknown value fails `create_varco_app()`
with `BackendSelectionError`.

| Old | New |
|---|---|
| `VARCO_REDIS_USE_STREAMS=true` alone selected the Redis Streams bus | `VARCO_EVENT_BUS_BACKEND=redis` selects Redis; `VARCO_REDIS_USE_STREAMS` picks Pub/Sub vs Streams within it |
| `varco_redis` scanned → *both* `RedisDLQ` and `RedisStreamDLQ` (and both `RedisCache`/`LayeredCache`) registered, first-installed won | one value: `VARCO_DLQ_BACKEND=redis` \| `redis-streams`; `VARCO_CACHE_BACKEND=redis` \| `layered` |
| `varco_kafka` + `varco_nats` both scanned → first-registered bus won (`AMBIGUOUS_BINDING`) | `VARCO_EVENT_BUS_BACKEND` decides; no tie is possible |
| `container.get(AbstractEventBus, qualifier="kafka")` regardless of selection | resolves only when `VARCO_EVENT_BUS_BACKEND=kafka`; `qualifier="in_memory"` always resolves |
| `enable_token_revocation(container)` / `enable_redis_token_revocation(container, url=…)` | unchanged (code tier); **or** `VARCO_REVOCATION_BACKEND=memory` \| `redis` (uses `VARCO_REDIS_URL`) — the `TrustedIssuerRegistry(revocation_store=…)` second step is unchanged either way |
| `enable_feature_flags(container)` / `enable_tenant_membership(container)` / `enable_policy_authorizer(container)` | unchanged; **or** `VARCO_FEATURE_FLAGS_ENABLED=true` / `VARCO_TENANT_MEMBERSHIP_ENABLED=true` / `VARCO_POLICY_AUTHORIZER_ENABLED=true`. Code beats env; calling `enable_*` with the variable set is fine (one `FALLBACK_SHADOWED` INFO) |
| `enable_authorization_audit(container)` | unchanged; no env form (call it last) |
| `AbstractIdempotencyStore` unresolvable without an app binding | `InMemoryIdempotencyStore` is the `@Fallback` default (`qualifier="in_memory"`); pass `container.get(AbstractIdempotencyStore)` to `IdempotencyMiddleware(store=…)` |
| `from varco_fastapi.job.store import InMemoryJobStore` | still works (re-export); canonical home is `varco_core.job.memory` |
| `SAIdempotencyStore(url=…)` | still works; `SAIdempotencyStore(engine=…)` is new |
| A per-seam "my own backend" via a scanned `@Singleton` or `provide()` | unchanged — any non-fallback binding wins over every variable; **new** zero-DI form: `VARCO_<SEAM>_BACKEND=pkg.mod:Name` (zero-arg class or factory) |
| The `_redis_active_bus` docstring references | gone; the selector is `RedisEventBusSelectorConfiguration.bus`, gated like every other backend |

**New gate:** `make lint` runs `scripts/di_catalogue.py --check`; regenerate with `make
di-catalogue` whenever a default, backend, or feature binding changes.

## Docs / backlog updates

- `technical_docs/features/dependency-injection.md` — §Selecting a backend, §Enabling a feature,
  Pitfalls rows (Step 13). `technical_docs/di-catalogue.md` — **generated** (Step 11).
- `README.md` "Choosing a backend" / "Enabling a feature" (Step 13); `CLAUDE.md` three rule lines +
  commands line + decision-tree branch + two bullet amendments (Step 13); `ARCHITECTURE.md`;
  `technical_docs/common-pitfalls.md` ×2; eight feature docs; `examples/00-full-stack-post-api`.
- `CHANGELOG.md`, `BACKLOG.md:90,93,94,130-134` (Step 14).
- `design/api-freeze-and-standards/measurements/{di-catalogue.json,api-surface.*}` (Steps 11, 15).
- `testkit/varco_conformance/COVERAGE.md` `di_wiring` row (Step 10).

## Done criteria

- [ ] `varco_core.di` is a package exporting `ENV_OVERRIDE_PRIORITY`, `SEAMS`, `FEATURES`,
      `selected_by`, `feature_default`, `env_truthy`, `bind_env_overrides`, `check_backend_selection`,
      `inspect_backend_selection`, `inspect_bindings`, `catalogue`, `BackendSelectionError`,
      `CatalogueError` (+ 045/046's names); `import varco_core.di` still imports no providify.
- [ ] Every backend binding of the six seams carries `selected_by(...)` beside `@Fallback` (≥ 20
      sites); the seven new providers N1–N9 exist; `InMemoryIdempotencyStore` and `InMemoryJobStore`
      (now in `varco_core.job.memory`) are `@Fallback @Singleton(qualifier="in_memory")`.
- [ ] Per package, `di_wiring`'s `selectable` tests prove: unset → default + `CONDITION_INACTIVE`;
      set → the backend at `BACKEND_PRIORITY`, `fallback is True`; code binding wins in both states.
- [ ] Dotted path: resolves, wins over named backends, loses to code, fails at bootstrap with a
      clear message on import/type errors.
- [ ] `check_backend_selection()` raises on unknown values and warns on bootstrapped-but-unselected;
      `create_varco_app()` calls it.
- [ ] Three feature twins live (`feature_flags`, `tenant_membership`, `policy_authorizer`); the
      matrix is published; `enable_*` semantics from 046 unchanged.
- [ ] `scripts/di_catalogue.py --check` green on the committed snapshot; wired into `make lint`
      (no-`PKG`) and CI's `lint` job; `technical_docs/di-catalogue.md` generated and in the nav.
- [ ] `di.in_process_backend` posture finding exists with tests.
- [ ] `rg -n '_redis_active_bus|activate_profile' varco_*/varco_*` → 0; no file under
      `/home/edoardo/projects/providify` modified.
- [ ] `make lint`, `make type-check`, `make test`, `make docs` green; CHANGELOG BREAKING block =
      §Migration; BACKLOG `DI-4`/`DI-12`/`DI-17` → done, Open Question 2 re-answered.
