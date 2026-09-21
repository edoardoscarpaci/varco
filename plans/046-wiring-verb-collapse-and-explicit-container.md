# Plan 046 — Wiring-verb collapse, explicit container everywhere, `di_wiring` conformance suite (DI-2, DI-8, DI-13)

Covers BACKLOG 4.0 rows **`DI-2`** (`BACKLOG.md:75` — *"Collapse the six-verb taxonomy — one return
contract for every `bootstrap()` (container, never `None`; providify is a hard dependency),
`enable_authorization_audit` returns like its four siblings, the eight `bind_<hand-built-object>`
helpers … fold into one shape; `bootstrap(streams=True)` stops mutating `os.environ`. Old names
removed outright (breaking, migration guide)"*), **`DI-8`** (`BACKLOG.md:89` — *"Remove
`DIContainer.current()` service-locator use — 32 sites in 14 files … take an explicit container"*)
and **`DI-13`** (`BACKLOG.md:91` — *"Wiring contract tests per package — a
`testkit/varco_conformance/di_wiring.py` suite: `bootstrap()` idempotent, returns the container,
`validate()` clean, every default overridable by a user binding, every `@Disposes` reachable; fills
the `varco_casbin`, CloudEvents-per-backend, and tenant-membership gaps"*).

Second plan of the 4.0 DI track (`plans/000-index-4-0-di-standardization.md`, row 046). **Builds on
Plan 045 as landed** — `providify>=2.1.0`, every framework default `@Fallback`,
`varco_core.di.BACKEND_PRIORITY`, `setup_varco_defaults()`/`bootstrap(setup_defaults=)` gone,
`technical_docs/features/dependency-injection.md` started. None of those decisions is re-opened
here. Neighbours, referenced by number and never absorbed: **047** (backend selection / catalogue /
`enable_*`-vs-env precedence — its `@Requires`-gated providers register through this plan's
`bootstrap()` contract; its catalogue will supersede §D-suite's `expected_defaults` lists),
**048** (`varco_core.settings.bind_settings()` — the recipe that *replaces* the interim
`bind_instance(...)` form this plan gives `bootstrap(streams=True)`'s users; 048 §D-precedence's
"register overrides before `bootstrap()`" rule is the same one §D-streams states), **049**
(`VarcoModule.register(container)` calls this plan's `bootstrap()` — every `bootstrap()` stays a
plain function of one container so a module can wrap it), **050** (`@Disposes` reachability for
provider-produced singletons — this plan only *asserts* it via `validate()`), **052** (provenance
report — reads the same `expected_defaults`/P29 facts), **058** (assembles §Migration; polishes the
CLAUDE.md table this plan rewrites).

**providify source of record**: `/home/edoardo/projects/providify` at **2.1.0** (`pyproject.toml:3`;
`CHANGELOG.md` `[2.1.0] — 2026-09-13`). Every `providify/…` citation below was read in that tree on
2026-09-15; Step 0 re-confirms each against the installed copy after 045's pin bump.

> ⚠️ **Six premises corrected from source before anything was decided.**
> (1) **`enable_authorization_audit` already returns the container** (`varco_core/varco_core/auth/di.py:49-53,99`,
> typed `-> DIContainer`). The BACKLOG row's "returns `None`" is stale. The real inconsistency is the
> *other five* `enable_*` being typed `-> Any` and none of the six agreeing on what a **second call**
> does (§D-enable). (2) **providify is already a hard dependency of all ten packages**
> (`rg -n 'providify>=' varco_*/pyproject.toml` → 10 hits) — every `try: from providify import …
> except ImportError: return None` guard (16 sites, §Context) is dead code, and the docstrings
> claiming "returns `None` if providify is absent" describe a state that cannot occur. Making
> providify "a hard dependency" is therefore a *deletion*, not an addition. (3) **`DIContainer.current()`
> has 13 code sites in 10 files, not 32 in 14** — the 32 is `rg`'s hit count including 19 docstring
> mentions, four of which are *negative* ("never touches `DIContainer.current()`" —
> `retention/di.py:81`, `revocation/di.py:20`, `authority/registry.py:214`, `lifespan.py:158`).
> `VarcoLifespan`, `TrustedIssuerRegistry`, `revocation/di.py` and `retention/di.py` already take
> explicit containers or none at all; nothing is threaded through them (§D-locator). (4) A middleware
> receives its container through `install_middleware_stack`'s **`(cls, kwargs)` entry**
> (`varco_fastapi/varco_fastapi/middleware/__init__.py:120-126,169-174`), not through a
> `install_middleware_stack(app, container)` signature — that function takes no container and is not
> changed. (5) **`varco_casbin` has a DI test** (`varco_casbin/tests/test_di.py`, four tests: both
> interfaces, settings provider, opt-in authorizer, `@PostConstruct`); what it lacks is the
> conformance-suite membership (and the `pythonpath = ["../testkit"]` line — it is the one package
> without it, `COVERAGE.md:52-55`). (6) **Every "calling twice replaces the previous binding"
> docstring is false** (`varco_ws/di.py:203-204,330-331`, `router/mcp.py:969`, `router/skill.py:1059`,
> `event/cloudevents.py:534-535`, `retention/di.py:78-80`): `container.provide()` appends
> (`providify/container.py:1250`), and a tie at equal priority resolves **first-registered**
> (`_get_best_candidate`, `container.py:2814-2853`). `bind_retention_registry` even carries a
> module-global `_bind_call_count` (`retention/di.py:45-59,93-95`) that bumps `priority=` per call to
> fake "replace". §D-bind replaces the lie with one rule.

## Goal

After this plan: **every varco package exposes exactly one `bootstrap(container: DIContainer) ->
DIContainer`** (ten, including a new `varco_core.di.bootstrap`), idempotent, container required, never
touching `DIContainer.current()` or `os.environ`; **every `enable_*` and `bind_*` returns the
container**, and a second registration of the same `(interface, qualifier)` raises
`varco_core.di.AlreadyBoundError` instead of silently losing; the three hand-built-object helpers are
one-liners over a new public primitive **`varco_core.di.bind_instance()`**; `bootstrap(streams=True)`
is gone and its users register an explicit `RedisEventBusSettings(use_streams=True)` instance;
**`rg -n 'DIContainer\.current\(\)|\.acurrent\(\)' varco_*/varco_*` → 0 hits**, mechanically
guarded; **`testkit/varco_conformance/di_wiring.py`** exists (the ninth conformance module) and is
subclassed by **all ten packages** with a `COVERAGE.md` row each; the 16 dead `ImportError` guards
and their tests are deleted; the CLAUDE.md verb-taxonomy table is rewritten to the new shape and
`dependency-injection.md` gains its **§Wiring contract** section; `scripts/api_surface.py`'s
snapshot is regenerated in the same step as the signature changes.

## Non-goals

- **Any change to providify** — 2.1.0 is consumed as shipped. One upstream ask is *filed* (P29,
  §D-suite) with a strict xfail; nothing waits on it.
- **Backend selection** (`VARCO_<SEAM>_BACKEND`, `@Requires`, the redis selector's replacement,
  the two-backends tie, the `enable_*`-vs-`*_ENABLED` precedence) — Plan 047. This plan changes
  `enable_*`'s *return and double-call contract* only, never *when* a feature is on.
- **Settings registration** — Plan 048. §D-streams uses `bind_instance()` as the 046-era form and
  says explicitly that 048's `bind_settings(container, Cls, instance=…)` supersedes it (adds
  fail-fast + provenance). The casbin `_provide_casbin_settings` provider gets an *idempotency
  guard* here (§D-contract) and is *replaced* by 048.
- **`VarcoModule` / `create_varco_app(modules=[…])`** — Plan 049. `create_varco_app(container, …)`
  is untouched (it already takes an explicit container, `app.py:105`).
- **Phased lifecycle / `@Disposes` for every provider-produced singleton** — Plan 050. This plan
  asserts reachability through `validate()`'s three `WARNING` kinds (§D-suite) and files what it
  finds as `known_warnings` entries naming 050, never fixes them.
- **Open-generic `bind_repositories` → one `returns=AsyncRepository[T]` provider** — Plan 051.
  `bind_repositories` keeps its per-class loop here and only gains the uniform shape.
- **`varco diagnose`** — Plan 052.
- **The nine `removed_in="4.0.0"` deprecation sites** — Plan 058 re-labels them; untouched.
- **`mount_*` and `install_*`** — not container verbs; their taxonomy rows are carried over
  verbatim (§D-docs). `install_middleware_stack` is not changed (premise 4).
- **`DISessionMiddleware`'s per-request scoping semantics** — only its container parameter changes.
- **A top-level `varco_core` re-export of `varco_core.di`** — same 045 non-goal (import budget);
  `from varco_core.di import bootstrap, bind_instance` is the import.

## Context

### Every container-taking entry point, from source (recounted 2026-09-15)

Method: `rg -n '^(async )?def (bootstrap|async_bootstrap|bind_\w+|enable_\w+|setup_\w+|create_varco_container|create_tables)\(' varco_*/varco_*` → **36 module-level functions** (the
scout's 52 also counted the 3 `mount_*` and 10 `install_*` functions, which take no container and
are out of scope, and 3 non-wiring functions named `enable_subscription`/`install_*` inside
routers — excluded). ⚠️ ASSUMPTION until Step 0 re-runs the command after 045 (which deletes
`setup_varco_defaults`, → 35).

| # | Function | File:line | Today | After (§D) |
|---|---|---|---|---|
| 1 | `bootstrap` | `varco_kafka/varco_kafka/di.py:64-137` | `(container: Any = None) -> Any`; `current()` at `:130`; guard `:122-125` | `(container: DIContainer) -> DIContainer` |
| 2 | `bootstrap` | `varco_nats/varco_nats/di.py:68-137` | same shape; `current()` `:133`; guard `:125-127` | same |
| 3 | `bootstrap` | `varco_redis/varco_redis/di.py:74-167` | `(container=None, *, streams=False)`; **`os.environ.setdefault("VARCO_REDIS_USE_STREAMS", "true")` at `:154`**; `current()` `:157`; guard `:145-148` | `(container) -> DIContainer`; `streams=` **removed** (§D-streams) |
| 4 | `async_bootstrap` | `varco_redis/varco_redis/di.py:173-242` | `(container=None, *, streams=False, setup_cache=False)`; RIDER-1 `None` passthrough `:234-235` | `(container) -> DIContainer` = `bootstrap()` + `ainstall(RedisCacheConfiguration)`; both kwargs removed |
| 5 | `enable_redis_token_revocation` | `varco_redis/varco_redis/di.py:248-289` | `(container: Any, **redis_kwargs) -> Any`; second call → two providers → `AMBIGUOUS_BINDING` | `-> DIContainer`; `AlreadyBoundError` on a second non-fallback (§D-enable) |
| 6 | `bootstrap` | `varco_memcached/varco_memcached/di.py:68-134` | `(container=None) -> Any`; `current()` `:125`; guard `:109-120` | `(container) -> DIContainer` |
| 7 | `async_bootstrap` | `varco_memcached/varco_memcached/di.py:140-…` | `(container=None, *, setup_cache=True) -> Any` | `(container) -> DIContainer`; `setup_cache=` removed |
| 8 | `bootstrap` | `varco_ws/varco_ws/di.py:81-143` | `(container=None) -> Any`; `current()` `:137`; guard `:129-132` | `(container) -> DIContainer` |
| 9 | `bind_websocket_adapter` | `varco_ws/varco_ws/di.py:149-273` | `-> None`; guard `:213-227`; "replaces" lie `:203-204`; closure resolves `AbstractEventBus` lazily `:264` | `(container, *, qualifier=None, …) -> DIContainer`; `AlreadyBoundError` on repeat (§D-bind) |
| 10 | `bind_sse_adapter` | `varco_ws/varco_ws/di.py:279-386` | same shape; guard `:340-352` | same |
| 11 | `bootstrap` | `varco_casbin/varco_casbin/di.py:89-124` | `(container=None) -> Any`; `current()` `:118`; guard `:112-115`; **`container.provide(_provide_casbin_settings)` at `:123` is not idempotent** (docstring `:107-108` claims it is) | `(container) -> DIContainer`; provider guarded by `is_user_bound()` until 048 |
| 12 | `enable_policy_authorizer` | `varco_casbin/varco_casbin/di.py:127-160` | `(container: Any) -> Any` | `-> DIContainer` + `AlreadyBoundError` |
| 13 | `bind_repositories` | `varco_sa/varco_sa/di.py:292-349` | `-> None`; "second binding … avoid duplicates" `:328-329` | `-> DIContainer`; `AlreadyBoundError` per alias |
| 14 | `bootstrap` | `varco_sa/varco_sa/di.py:398-484` | `(container=None, *entity_classes)`; `current()` `:472`; guard `:464-467` | `(container) -> DIContainer`; `*entity_classes` **removed** (call `bind_repositories`) |
| 15 | `create_tables` | `varco_sa/varco_sa/di.py:490-…` | `(container=None)`; `current()` `:537`; guard `:531-534` | `(container: DIContainer) -> None` |
| 16 | `bind_repositories` | `varco_beanie/varco_beanie/di.py:103-…` | as #13 | as #13 |
| 17 | `bootstrap` | `varco_beanie/varco_beanie/di.py:207-295` | as #14; `current()` `:284`; guard `:276-279` | as #14 |
| 18 | `bind_clients` | `varco_fastapi/varco_fastapi/di.py:108-196` | `-> None`; docstring `:142-147` already states first-registered-wins honestly | `-> DIContainer`; `AlreadyBoundError` per alias |
| 19 | `bind_clients_from` | `varco_fastapi/varco_fastapi/di.py:199-252` | `-> None` | `-> DIContainer` (delegates to #18) |
| 20 | `create_varco_container` | `varco_fastapi/varco_fastapi/di.py:445-506` | `(*packages) -> Any`; **is** `DIContainer.current()` + a scan loop (`:500-504`); guard `:495-498` | **deleted** (§D-locator) |
| 21 | `setup_event_producer` | `varco_fastapi/varco_fastapi/di.py:548-624` | binds a non-fallback `AbstractEventProducer → BusEventProducer` provider; 045 deleted its A19 stamp and left the verb to this plan | **deleted** — `varco_core.di.bootstrap()` registers `BusEventProducer` (045 A10, `@Fallback @Singleton`, `producer.py:146-172` injects `AbstractEventBus`) |
| 22 | `bootstrap` | `varco_fastapi/varco_fastapi/di.py:644-729` | `(container=None, *packages, setup_producer=False)`; `current()` `:712`; guard `:704-707`; ⚠️ the documented `bootstrap("myapp")` (`:664`) passes `"myapp"` **as the container** — a latent bug the `*packages` signature invites | `(container) -> DIContainer`; `*packages` and `setup_producer=` removed |
| 23 | `bind_mcp_adapter` (wrapper) | `varco_fastapi/varco_fastapi/di.py:735-752` | `(*args, **kwargs) -> None` re-export wrapper — erases the signature | replaced by a true re-export (`from varco_fastapi.router.mcp import bind_mcp_adapter`) |
| 24 | `bind_skill_adapter` (wrapper) | `varco_fastapi/varco_fastapi/di.py:755-765` | same | same |
| 25 | `bind_mcp_adapter` | `varco_fastapi/varco_fastapi/router/mcp.py:939-1022` | `-> None`; guard `:977-987`; docstring `:955-956` **promises a `qualifier=` kwarg the signature does not have**; "replaces" lie `:969` | `(container, router_cls, *, qualifier=None, …) -> DIContainer` |
| 26 | `bind_skill_adapter` | `varco_fastapi/varco_fastapi/router/skill.py:1020-…` | `-> None`; guard `:1067-1076`; "replaces" lie `:1059` | same shape as #25 |
| 27 | `bind_peers` | `varco_fastapi/varco_fastapi/client/peer.py:320-342` | `-> None`; delegates to #18 | `-> DIContainer` |
| 28 | `bind_trust_store` | `varco_core/varco_core/tls/di.py:44-78` | `-> None`; two `Provider(singleton=True)(closure), returns=` registrations | `-> DIContainer`; two `bind_instance()` calls |
| 29 | `bind_retention_registry` | `varco_core/varco_core/retention/di.py:62-102` | `-> None`; the `_bind_call_count` priority hack | `-> DIContainer`; one `bind_instance()`; global deleted |
| 30 | `bind_cloudevents_serializer` | `varco_core/varco_core/event/cloudevents.py:501-562` | `-> None`; two bare `lambda` provides `:561-562` | `-> DIContainer`; two `bind_instance()` calls |
| 31 | `enable_authorization_audit` | `varco_core/varco_core/auth/di.py:49-99` | `-> DIContainer` ✅; raises `RuntimeError` on double-wrap `:83-88` | unchanged shape; the `RuntimeError` becomes `AlreadyBoundError` (a `RuntimeError` subclass) |
| 32 | `enable_token_revocation` | `varco_core/varco_core/revocation/di.py:70-97` | `(container: Any) -> Any` | `-> DIContainer` + `AlreadyBoundError` |
| 33 | `enable_tenant_membership` | `varco_core/varco_core/tenancy/di.py:77-105` | `(container: Any, settings=None) -> Any` | same |
| 34 | `enable_feature_flags` | `varco_core/varco_core/flags/di.py:58-83` | `(container: Any) -> Any` | same |
| 35 | *(new)* `bootstrap` | `varco_core/varco_core/di.py` | — | `(container) -> DIContainer` = `container.scan("varco_core", recursive=True)` (§D-contract) |
| 36 | *(new)* `bind_instance` | `varco_core/varco_core/di.py` | — | the primitive (§D-bind) |

Ten `bootstrap()`s after this plan: `varco_core.di`, `varco_kafka.di`, `varco_nats.di`,
`varco_redis.di`, `varco_memcached.di`, `varco_ws.di`, `varco_casbin.di`, `varco_sa.di`,
`varco_beanie.di`, `varco_fastapi.di`.

### The 13 `DIContainer.current()` code sites (DI-8)

`rg -n 'DIContainer\.current\(\)|\.acurrent\(\)' varco_*/varco_*` → 32 hits; **13 are code**:
`varco_kafka/di.py:130`, `varco_nats/di.py:133`, `varco_redis/di.py:157`, `varco_memcached/di.py:125`,
`varco_ws/di.py:137`, `varco_casbin/di.py:118`, `varco_sa/di.py:472` (bootstrap), `varco_sa/di.py:537`
(create_tables), `varco_beanie/di.py:284`, `varco_fastapi/di.py:500` (create_varco_container),
`varco_fastapi/di.py:712` (bootstrap), `varco_fastapi/middleware/session.py:69` (`get_container()` —
a dead helper, not in `__all__`, returns the global under a docstring that says "or `None` …
ContextVar read", neither of which is true) and `session.py:136`
(`DISessionMiddleware.__init__`'s `container or DIContainer.current()`). The other 19 are
docstring prose (15 "When `None`, `DIContainer.current()` is used" + the 4 negative mentions).
All 32 hits go to zero (Step 12 rewrites the four negative mentions to "never reaches for the
process-global container" so the guard grep is literal).

### The 16 dead providify guards

`try: from providify import DIContainer/Provider … except ImportError: return None/…`:
`varco_kafka/di.py:122-125`, `varco_nats/di.py:125-127`, `varco_redis/di.py:145-148`,
`varco_memcached/di.py:109-120`, `varco_ws/di.py:129-132`, `:213-227`, `:340-352`,
`varco_casbin/di.py:112-115`, `varco_sa/di.py:464-467`, `:531-534`, `varco_sa/bootstrap.py:145-150`
(raises a friendlier `ImportError` — equally dead), `varco_beanie/di.py:276-279`,
`varco_fastapi/di.py:495-498`, `:704-707`, `router/mcp.py:977-987`, `router/skill.py:1067-1076`.
Tests that exist only to exercise them: `varco_redis/tests/test_async_bootstrap_no_providify.py`
(whole file), `varco_memcached/tests/test_di.py:73-145,237-250,272-279,298-322` (five tests that
patch `sys.modules["providify"]`), `varco_ws/tests/test_di.py:346-…` and `:454-…`
(`*_importerror_guard`), `varco_fastapi/tests/milestone_f/test_mcp_adapter.py:442-455`
(`test_bind_mcp_adapter_noop_without_providify`). ⚠️ ASSUMPTION the skill adapter has no
equivalent guard test (Step 0 greps `sys.modules\["providify"\]` across `varco_*/tests`).

### providify 2.1.0 facts this plan rides on (verified in the tree)

| Primitive | Where | Semantic |
|---|---|---|
| `DIContainer.current()` | `providify/container.py:1043-1056` | Lazily creates a **process-global** container under a `threading.Lock`; `acurrent()` `:1058-1080`; `reset()` `:1082-1091`; `scoped(container)` `:1093-1121` swaps the global for a block — the mechanism behind `di_global` |
| `di_global` fixture | `providify/pytest_plugin.py:107-129` | Opt-in, **not autouse**; `with DIContainer.scoped(di_container)`; exists "for code that resolves via `DIContainer.current()` rather than receiving a container by dependency injection" — after this plan, no varco code does; the fixture remains for an *application's* own locator use (§D-locator) |
| `scan()` idempotency — classes | `providify/scanner.py:204-206` | `if any(isinstance(b, ClassBinding) and b.implementation is cls …): return` — a second scan of the same package registers nothing |
| `scan()` idempotency — module-level `@Provider` | `scanner.py:249-251` | `if any(isinstance(b, ProviderBinding) and b.fn is fn …): return`; names starting `_` are skipped outright (`:143-145`) — which is why `_provide_casbin_settings` and the six `_provide_*` `enable_*` providers are invisible to scan and must be `provide()`d by hand |
| `scan()` idempotency — `@Configuration` | `scanner.py:255-…` + `container.py:1019` (`_installed_modules`), checks at `:7158`, `:7194` | Container-owned dedup: a second `install()`/scan of the same module is a no-op. **So `bootstrap()` idempotency is free for everything a scan registers, and NOT free for anything registered by a direct `container.provide()` inside `bootstrap()`** (casbin #11, the removed sa/beanie `*entity_classes` path) — the guard is §D-contract's |
| `provide()` | `container.py:1250-1299` | Appends; returns `ProviderBinding` (2.1.0); `returns=` overrides the annotation, which is then never read (`:1265-1266`) |
| Tie resolution | `container.py:2814-2853` | `max(candidates, key=priority)`; exact tie → first registered — the "replaces" docstrings are wrong |
| `_filter()` fallback post-step | `container.py:2168-2169` | Reaches `get_binding`/`get_all_bindings` (`:2159-2162`) — so after a user binding exists, the shadowed `@Fallback` is *invisible to every public read*; the only enumeration of all fallbacks is `container._bindings` (private) or `describe()` — and `BindingDescriptor` **has no `fallback` field** (`providify/descriptor.py:49-54`: `interface`, `implementation`, `scope`, `qualifier`, `priority`, `dependencies`). This is the P29 ask (§D-suite) |
| `get_all_bindings(interface, *, qualifier=None)` | `container.py:7464-7491` | Pure read, registration order, `[]` when none; **exact** qualifier match when given; unqualified request also matches qualified candidates (`:2096-2101`, E3/E4) — `is_user_bound()` is built on it |
| `get_binding()` | `container.py:7422-7462` | Pure read; the "who wins" tool |
| `describe().bindings` | `container.py:7415-7418` | `tuple(b.describe(self) for b in self._bindings)` — `len()` is a **public** total-binding count (the idempotency assertion uses it) |
| `ClassBinding.fallback` / `ProviderBinding.fallback` | `providify/binding.py:207` / `:723` | Read once at construction |
| `ValidationReport` | `providify/validation.py:227-287` | `issues`, `checked_bindings`; `errors`/`warnings`/`infos` properties; `ok == not errors` |
| `UNREACHABLE_PRE_DESTROY` | `validation.py:97-101`; emitter `container.py:6255-6280` | **`WARNING`** — a `SINGLETON` `ProviderBinding` whose produced type carries `@PreDestroy` and has no `@Disposes` |
| `UNMATCHED_DISPOSER` / `DISPOSER_OVERWRITTEN` | `validation.py:102-109`; `container.py:6407-6427` | Both **`WARNING`** |
| `AMBIGUOUS_BINDING` | `validation.py:82-84`; `container.py:6709-6710` | `ERROR`, per injection point |
| `@Provider(qualifier=, priority=, singleton=, returns=)` | `providify/decorator/scope.py:497-511` | `bind_instance(qualifier=)` rides `qualifier=` |

## Design

### §D-contract — ten `bootstrap(container: DIContainer) -> DIContainer`, container required

**Chosen: the container is a required positional parameter; `bootstrap()` never creates one and
never consults `DIContainer.current()`; it returns the same object it was given; calling it twice
is a no-op the second time.**

```
container = DIContainer()
varco_core.di.bootstrap(container)      # scan("varco_core", recursive=True) — every @Fallback default
varco_redis.di.bootstrap(container)     # scan("varco_redis", …) + scan("varco_core.revocation", …) [045 E2]
varco_sa.di.bootstrap(container)        # scan("varco_sa", …)
varco_fastapi.di.bootstrap(container)   # scan("varco_fastapi", …)
container.scan("myapp", recursive=True) # the app's own — plain providify, no varco verb
bind_repositories(container, User, Post)
app = create_varco_app(container)
```

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: `container` required, `-> DIContainer`** | Explicit over discovered (brief 015 §Librarian #1); the return value is always usable, so `async_bootstrap`'s RIDER-1 `None` branch and every `if container is None` disappear; a test constructs `DIContainer()` and passes it — no `di_global`, no `reset()`; 049's `register(container)` is a one-line wrapper; the `bootstrap("myapp")`-is-the-container bug class (#22) cannot be written | Every `bootstrap()` call site without an argument breaks (README `:3495-3497`, package docstrings, `varco_memcached/__init__.py:23`; **zero** in `examples/` — all 23 already pass an explicit container). Accepted: 4.0 is the breaking window |
| (2) `container=None` creates a **fresh** `DIContainer()` and returns it | No break for `bootstrap()` callers | Two packages bootstrapped without an argument produce two unrelated containers — the exact silent-wrong-wiring failure DI-8 exists to remove; `async_bootstrap()` would still need the `None` dance |
| (3) `container=None` keeps `DIContainer.current()` | No break at all | Is DI-8's problem restated; untestable without `di_global`; 052 cannot answer "where did this come from" |

**`varco_core.di.bootstrap(container)`** (new, #35) is `container.scan("varco_core",
recursive=True)` — the call every example already makes by hand (`examples/01-minimal-crud-api/app.py:96`,
`06/app.py:97`) and the one 045 §D-validate's canonical container starts with. It becomes the
*only* documented way to get the Class-A defaults, which is what lets `setup_event_producer` (#21)
go: `BusEventProducer` is A10. ✅ ten packages, ten identical verbs; ❌ an app that scanned only
`varco_fastapi` and relied on `setup_event_producer` must now call `varco_core.di.bootstrap()`
(§Migration). The module keeps 045's "no providify import at module scope" property by importing
providify **inside** the function bodies (`varco_core/tests/test_di_constants.py`'s
`sys.modules` delta assertion stays true; the import budget is untouched).

**Idempotency** is free for scanned registrations (`scanner.py:204-206`, `:249-251`, `:7158`) and
must be *guarded* for a direct `provide()` inside a `bootstrap()`. Exactly one such site survives
this plan (casbin #11's `_provide_casbin_settings`, until 048 replaces it with `bind_settings()`,
which has its own guard); it becomes `if not is_user_bound(container, CasbinSettings):
container.provide(_provide_casbin_settings)`. The sa/beanie `*entity_classes` convenience is
removed rather than guarded — `bind_repositories()` is the verb for that and its own rule (§D-bind)
is "raise on repeat", which is the wrong behaviour for a re-run `bootstrap()`.

**`async_bootstrap(container) -> DIContainer`** (redis #4, memcached #7) keeps its name and becomes
exactly "`bootstrap(container)` then `await container.ainstall(<the package's connection-opening
@Configuration>)`" with **no keyword arguments**: `setup_cache=False` was byte-identical to
`bootstrap()` (pointless), `streams=` is §D-streams's, and the two packages' *different* defaults
(`False` vs `True`) were the inconsistency DI-2 names. An app that wants scan-only calls
`bootstrap()`. Plan 050 may fold `async_bootstrap` into the lifecycle; not this plan's call.

### §D-hard-dep — providify is a hard dependency; the guards are deleted, not kept

Premise (2): all ten `pyproject.toml`s already declare `providify>=2.0.1` (→ `>=2.1.0` after 045).
The 16 guards can never fire in an installed varco. **Chosen: delete every guard, import providify
at module scope in each `di.py` (they already do in `varco_casbin/di.py:40`, `tls/di.py:35`,
`retention/di.py:36`, `revocation/di.py:46`, `auth/di.py`), delete the five guard-only test
modules/functions, and rewrite every "returns `None` if providify is not installed" sentence.**
✅ `-> DIContainer` becomes an honest annotation and mypy stops seeing `Any`; ❌ none — there is no
supported configuration in which the guards did anything. (The `varco_core.di` module is the one
exception to "import at module scope", for the import-budget reason in §D-contract.)

### §D-enable — `enable_*` all return `DIContainer`, and a second non-fallback is an error

Premise (1): the six already return the container; five are typed `Any`. **Chosen: all six typed
`-> DIContainer`; each raises `varco_core.di.AlreadyBoundError` when a non-fallback binding of the
seam it flips already exists** — whether from a previous `enable_*` call or from the app's own
binding (both mean "a second non-fallback would tie at 0 → `AMBIGUOUS_BINDING` at every injection
point", `scope.py:1042-1047`).

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: raise `AlreadyBoundError`** | Same rule as `bind_*` (§D-bind) — one sentence in the guide; the tie is caught at the call that causes it, with a message naming the seam; `enable_authorization_audit` already had this shape (`RuntimeError`, `auth/di.py:83-88`) — it just changes exception class | An app that called `enable_feature_flags()` twice by accident now fails at startup. Correct — it was `AMBIGUOUS_BINDING` before |
| (2) silently no-op on repeat | Idempotent | Hides the app's-own-binding case: `enable_policy_authorizer()` after the app bound `AbstractAuthorizer` would do nothing and log nothing — the "think it is on when it is not" pitfall CLAUDE.md already documents for revocation |
| (3) replace via `container.override()` | "Last wins" | `override()` only replaces `ClassBinding` entries (`container.py:7619`; the retention docstring `:49-51` already records this); every `enable_*` registers a `ProviderBinding` |

`enable_authorization_audit` (#31) keeps its own `isinstance(delegate, AuditingAuthorizer)` guard —
its job is precisely to wrap a *user* binding, so `is_user_bound()` is the wrong test for it — and
raises `AlreadyBoundError` (a `RuntimeError` subclass, so any `except RuntimeError` keeps working).
`enable_redis_token_revocation` (#5) and `enable_token_revocation` (#32) flip the same seam; calling
both is the same error and says so.

### §D-bind — one primitive, eight names kept, one rule

**Chosen: a public `varco_core.di.bind_instance(container, interface, instance, *, qualifier=None)
-> ProviderBinding`** registers an already-constructed object as a non-fallback singleton, raising
`AlreadyBoundError` if a non-fallback binding of `(interface, qualifier)` already exists. **The
three hand-built-object helpers become one-liners over it; the four adapter helpers and the three
client/repository helpers keep their lazy-factory bodies but adopt the same signature shape
`(container, …, *, qualifier=None) -> DIContainer` and the same rule.** Every old name stays.

```python
# varco_core/varco_core/di.py (additions; BACKEND_PRIORITY from 045 stays)
class AlreadyBoundError(RuntimeError): ...        # (interface, qualifier, owner) in the message

def is_user_bound(container, interface, *, qualifier=None) -> bool:
    return any(not b.fallback for b in container.get_all_bindings(interface, qualifier=qualifier))

def bind_instance(container, interface, instance, *, qualifier=None) -> ProviderBinding:
    if is_user_bound(container, interface, qualifier=qualifier): raise AlreadyBoundError(...)
    def _factory() -> Any: return instance
    _factory.__qualname__ = f"varco.di:bind_instance[{_name(interface)}{'@'+qualifier if qualifier else ''}]"
    return container.provide(Provider(singleton=True, qualifier=qualifier)(_factory), returns=interface)

def bootstrap(container) -> DIContainer:
    container.scan("varco_core", recursive=True); return container
```

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) chosen: `bind_instance` primitive + eight names on one shape** | The names say *what* is bound (a trust store, a retention registry, an MCP adapter) and carry the construction logic that a generic call cannot: the four adapters resolve `AbstractEventBus`/the client class/the job runner **lazily at first `get()`** inside their closures (`varco_ws/di.py:250-271`, `router/mcp.py:999-1020`, `skill.py:1092-…`) so the bus is constructed by the lifecycle, not at wiring time — 050's phased startup depends on that staying lazy; `bind_cloudevents_serializer` keeps the `Serializer[Event]` alias out of app code; `AlreadyBoundError` gives every helper the same honest double-call story and deletes the `_bind_call_count` hack; `bind_instance` is what an app uses for *its own* objects instead of copying the `Provider(singleton=True)(closure), returns=` dance (README shows it once) | Eight names remain — DI-2 says "fold into one shape", and one *shape* is what lands; the guide's table (§D-docs) is the one place they are listed |
| (2) only `bind_instance`; delete the eight | One function | Callers of the four adapters must `container.get(AbstractEventBus)` **at wiring time** to build the adapter — constructing the bus before the lifespan owns it (and `RedisEventBus.__init__` with no server is fine, but `KafkaEventBus` and the MCP client fallback are not free) and moving the client/job-runner fallback logic (`mcp.py:1006-1013`, `skill.py:1099-1117`) into every app; `bind_trust_store`'s *two* bindings (`ReloadingTrustStore` + its `TrustStore` spec, `tls/di.py:77-78`) become two lines every app must remember |
| (3) keep everything, just return the container | Smallest diff | Leaves six false "replaces" docstrings and the `_bind_call_count` global in place; `bind_websocket_adapter` called twice for two channels still silently serves the first — the bug the `qualifier=` kwarg fixes |

Rule, stated once in the guide: **a `bind_*`/`enable_*` call registers a non-fallback binding; a
second registration of the same `(interface, qualifier)` raises `AlreadyBoundError`. To replace a
binding in a test use providify's `di_overrides`/`ContainerOverrides` (CLAUDE.md's RL-3 rule); to
have two, qualify them.** `bind_repositories`/`bind_clients` apply it per generic alias
(`AsyncRepository[User]`, `VarcoClient[OrderRouter]`); `bind_peers` inherits it from `bind_clients`.

### §D-streams — `bootstrap(streams=True)` goes; an explicit settings instance replaces it

`os.environ.setdefault("VARCO_REDIS_USE_STREAMS", "true")` (`varco_redis/di.py:154`) leaks into
every later `RedisEventBusSettings()` construction in the process (the docstring `:136-138` admits
it), including the five "hand" sites 048's inventory row 13 lists and any test that constructs
settings afterwards. **Chosen: the kwarg is removed; the selector (`RedisEventBusSelectorConfiguration.bus`,
`bus.py:532-590`, reads `settings.use_streams`) is untouched; a user who wants Streams registers a
non-fallback `RedisEventBusSettings` before `bootstrap()`:**

```python
from varco_core.di import bind_instance
bind_instance(container, RedisEventBusSettings, RedisEventBusSettings(use_streams=True))  # 046 form
varco_redis.di.bootstrap(container)
# Plan 048 replaces the first line with:
#   bind_settings(container, RedisEventBusSettings, instance=RedisEventBusSettings(use_streams=True))
```

✅ no process-global mutation; the app's binding is a non-fallback and wins over 045 A20's
`@Fallback` provider in any order (the not-evicted caveat, `scope.py:1048-1053`, only bites if
something *resolved* the settings before the override — `bootstrap()` resolves nothing, so "before
`bootstrap()`" is the documented, not the required, order; 048 §D-precedence states the same rule).
❌ one more line than `streams=True`. Rejected alternative — `bootstrap(container,
settings=RedisEventBusSettings(...))`: ✅ one call; ❌ a per-package kwarg for one field on one
class is the exact "six verbs for three ideas" drift DI-2 removes, and 048 would have to keep it in
sync with `bind_settings()`. `VARCO_REDIS_USE_STREAMS=true` in the environment keeps working
unchanged (it is a field on the settings class, read by pydantic — 048's concern).

### §D-locator — `DIContainer.current()` reaches zero; what remains, and for whom

Per site: #1-#3, #6, #8, #11, #14, #15, #17, #22 → the `container` parameter is required (§D-contract).
#20 `create_varco_container` → **deleted**: it *was* the locator plus a scan loop; its replacement
is `DIContainer()` + the ten `bootstrap()`s + `container.scan("myapp", recursive=True)`. `session.py:69`
`get_container()` → **deleted** (dead, undocumented, lies twice). `session.py:136` →
`DISessionMiddleware(app, *, container: DIContainer)` — keyword-only, required; the app installs it
with `install_middleware_stack(app, [(DISessionMiddleware, {"container": container}), …])` or
`app.add_middleware(DISessionMiddleware, container=container)` (premise 4). `VarcoLifespan`
(`lifespan.py:150-159` — refused a `container=` kwarg by design), `TrustedIssuerRegistry`
(`registry.py:206-214`), `revocation/di.py` and `retention/di.py` already have no locator use;
their four negative docstring mentions are reworded so the literal token disappears.

**What `DIContainer.current()`/`di_global` remain for**: an *application's* own code may still use
providify's global, and providify's `di_global` fixture exists for *that* code's tests
(`pytest_plugin.py:110-114`). varco itself never reads or writes it; the guarantee is mechanical —
`varco_core/tests/test_no_service_locator.py` walks every `varco_*/varco_*/**/*.py` and asserts
zero occurrences of `DIContainer.current(`, `DIContainer.acurrent(`, `.scoped(` and
`DIContainer.reset(` outside `varco_*/tests/`. The suite's `test_bootstrap_never_touches_the_global`
(§D-suite) proves it behaviourally per package: bootstrap under `DIContainer.scoped(sentinel)`
leaves `sentinel.describe().bindings` empty.

### §D-suite — `testkit/varco_conformance/di_wiring.py`, the ninth conformance module

Same shape as `token_revocation.py` (`testkit/varco_conformance/token_revocation.py:25-33`): a base
class **not** named `Test*`, one abstract fixture raising `NotImplementedError`, subclassed per
package. Two class attributes replace hand-written assertions:

```python
class DIWiringConformance:
    expected_defaults: ClassVar[tuple[tuple[Any, str | None], ...]]   # (interface, qualifier) this bootstrap registers as @Fallback
    known_warnings:    ClassVar[frozenset[tuple[IssueKind, str]]] = frozenset()  # (kind, owner), each naming its owning plan in a comment

    @pytest.fixture
    def bootstrap(self) -> Callable[[DIContainer], DIContainer]:
        raise NotImplementedError("… override the `bootstrap` fixture")

    @pytest.fixture
    async def container(self, bootstrap):
        c = DIContainer(); yield bootstrap(c); await c.ashutdown()
```

| Test | Asserts | How (public API unless stated) |
|---|---|---|
| `test_bootstrap_returns_the_same_container` | `bootstrap(c) is c` | identity |
| `test_bootstrap_is_idempotent` | second `bootstrap(c)` adds **no** binding and no `AMBIGUOUS_BINDING` | `len(c.describe().bindings)` before/after; `validate(raise_on_error=False)` kinds |
| `test_bootstrap_never_touches_the_global` | no binding lands in the process-global container | `with DIContainer.scoped(sentinel): bootstrap(DIContainer())`; `len(sentinel.describe().bindings) == 0` |
| `test_validate_is_clean` | `report.errors == ()`; `{(i.kind, i.owner) for i in report.warnings} - known_warnings == set()`; `{i.kind for i in report.infos} <= {FALLBACK_SHADOWED, CONDITION_INACTIVE}` | 045 §D-validate's definition, generalised; `MISSING_BINDING` is **not** tolerated here (unlike `assert_no_structural_di_issues`, `providify_health.py:80`) — the subclass fixture must supply what the package needs (an `SAConfig`, a `BeanieSettings`, `varco_core` for `AbstractEventBus`) |
| `test_every_provider_singleton_has_a_reachable_disposer` | no `UNREACHABLE_PRE_DESTROY` / `UNMATCHED_DISPOSER` / `DISPOSER_OVERWRITTEN` warning outside `known_warnings` | a named subset of the previous test — all three are `WARNING` (`container.py:6265,6408,6427`), so "validate clean" already covers it; the separate test exists for the failure message, and its `known_warnings` entries are the list 050 works from |
| `test_declared_defaults_are_fallbacks` | for each `(I, q)` in `expected_defaults`: `c.get_binding(I, qualifier=q).fallback is True` | `get_binding` |
| `test_every_default_is_overridable_after_bootstrap` | for each `(I, q)`: after `bind_instance(c, I, MagicMock(spec=I), qualifier=q)` (or `Provider(qualifier=q)` where `I` is a generic alias) registered **after** bootstrap, `get_binding(I, qualifier=q).fallback is False` and every binding in `get_all_bindings(I, qualifier=q)` is non-fallback | 045's rule proven per package, from the suite |
| `test_declared_defaults_are_complete` | `{(b.interface, b.qualifier) for b in fallback_bindings(c) if not b.exact_only} == set(expected_defaults)` | ⚠️ **the one private read**: `varco_conformance.di_wiring.fallback_bindings(container)` reads `container._bindings` — centralised in exactly one function, named as a shim for deletion, because `BindingDescriptor` carries no `fallback` field (`descriptor.py:49-54`) and every public read drops shadowed fallbacks (`container.py:2159-2169`). Same private read `varco_redis/tests/test_redis_di.py:62-66` already makes |
| `test_redundant_install_after_scan_is_ambiguous` *(varco_fastapi subclass only)* | `container.install(VarcoFastAPIModule)` after `bootstrap()` → `AMBIGUOUS_BINDING` | pins 045 §D-validate's "double registration" finding as a documented pitfall, not a defect |

**P29 filing** (CLAUDE.md §"When you hit a providify limitation"): `design/upstream-gaps/providify-descriptor-fallback-field.md`
— §1 cite `descriptor.py:49-54` (no field) and `container.py:7415-7418` (`describe()` walks
`_bindings`); §2 what varco pays (the shim); §3 reproduction; §4 the ask — `BindingDescriptor.fallback:
bool` (+ `exact_only`) and `DIContainerDescriptor.fallback_bindings` (✅ additive, ❌ none) vs a
`get_all_bindings(include_shadowed=True)` kwarg (✅ smaller; ❌ changes a pure-read's filter contract);
§5 partly ours? **No** — 047's catalogue will list the *declared* defaults, but "are the declared ones
all there are" is a container question only the container can answer; §6 interim: the shim; §7 guard:
`varco_core/tests/test_providify_contracts.py::test_binding_descriptor_carries_fallback` — `strict=True`
xfail, `raises=AssertionError`, reason `"GAP: P29-DESCRIPTOR-FALLBACK — …"`. One `UPSTREAM-GAPS.md`
row. The number is P29 if 045 filed P28, else P28 — Step 2 reads the ledger.

**Subclasses — one per package, all Docker-free** (bootstrap must never need a live server; a
package whose bootstrap did would be a bug in that package, not a suite exemption):

| Package | Test module | `bootstrap` fixture composes | `expected_defaults` (interface, qualifier) — derived from 045's site table | Notes |
|---|---|---|---|---|
| `varco_core` | `varco_core/tests/test_di_wiring_conformance.py` | `varco_core.di.bootstrap` | A1-A12: `(CacheBackend,"in_memory")`, `(AbstractEventBus,"in_memory")`, `(AbstractDeadLetterQueue,"in_memory")`, A4's deduplicator ABC `"in_memory"`, `(AbstractDistributedLock,"in_memory")`, `(AbstractConversationStore,"in_memory")`, `(AbstractFeatureFlags,None)`, `(AbstractTokenRevocationStore,None)`, `(AbstractTenantMembership,None)`, `(AbstractEventProducer,None)`, `(Serializer[Event],None)`, `(AbstractAuthorizer,None)`; A13 `(OtelConfig,None)` | the in-process feedback loop; 045's `test_fallback_defaults.py` A1-A12 table is the source of the exact ABC names (Step 0 copies them) |
| `varco_kafka` | `varco_kafka/tests/test_kafka_di_wiring.py` | core + `varco_kafka.di.bootstrap` | core's + B1 `(AbstractEventBus,"kafka")`, B5 `(ChannelManager,"kafka")`, A20 `(KafkaEventBusSettings,None)`, `(KafkaChannelManagerSettings,None)`, B10 `(AbstractDeadLetterQueue,None)` if `KafkaDLQConfiguration` is scan-installed | absorbs 045's `test_kafka_fallback_tier.py` + `test_validate_clean_scan.py` (`git rm` both) |
| `varco_nats` | `varco_nats/tests/test_nats_di_wiring.py` | core + nats | B2, B5, A20 ×2, B10 | absorbs the nats seed + leg |
| `varco_redis` | `varco_redis/tests/test_redis_di_wiring.py` | core + redis | B3 `(AbstractEventBus,None)` (the selector, priority `BACKEND_PRIORITY`), B4 `(AbstractEventBus,"redis")`, B5, A20 ×4 (`RedisEventBusSettings`, `RedisBackplaneSettings`, `RedisCacheSettings`, `LayeredCacheSettings`), B10 ×2 if scan-installed, B11 ×2 (`CacheBackend`, if scan-installed) | scan is server-free (`test_redis_di.py:19-20`); absorbs the redis seed + leg; **also** the `streams` replacement test (§D-streams): `bind_instance(RedisEventBusSettings(use_streams=True))` then `get(AbstractEventBus)` is a `RedisStreamEventBus` and `"VARCO_REDIS_USE_STREAMS" not in os.environ` |
| `varco_memcached` | `varco_memcached/tests/test_memcached_di_wiring.py` | core + memcached | A20 `(MemcachedCacheSettings,None)`, B11 `(CacheBackend,None)` if scan-installed | `async_bootstrap` is **not** exercised (opens a pool) |
| `varco_ws` | `varco_ws/tests/test_ws_di_wiring.py` | core + ws | D1-2 `(WebSocketEventBus,None)`, `(SSEEventBus,None)` — self-bound fallbacks | core supplies `AbstractEventBus` (`InMemoryEventBus`) so `validate()` is clean; plus `bind_websocket_adapter` twice unqualified → `AlreadyBoundError`, twice with `qualifier=` → two bindings |
| `varco_casbin` | `varco_casbin/tests/test_casbin_di_wiring.py` | core + casbin | B13 `(PolicyEngine,"casbin")`, `(PolicyManagement,"casbin")` | **adds `pythonpath = ["../testkit"]`** to `varco_casbin/pyproject.toml`; `enable_policy_authorizer` twice → `AlreadyBoundError` |
| `varco_sa` | `varco_sa/tests/test_sa_di_wiring.py` | `bind_instance(c, SAConfig, SAConfig(engine=create_async_engine("sqlite+aiosqlite:///:memory:"), base=Base, entity_classes=()))` then core + sa | B6 `(RepositoryProvider,"sa")`, B7 `(IUoWProvider,None)`, B8 `(AbstractDistributedLock,None)` | `aiosqlite` is already a `varco_sa` test dep (`varco_sa/pyproject.toml:46-50`); `create_async_engine` opens nothing until first use; **`bootstrap()` needs no live DB** — `SAConfig` is injected lazily. Absorbs 045's sa seed + leg |
| `varco_beanie` | `varco_beanie/tests/test_beanie_di_wiring.py` | `bind_instance(c, BeanieSettings, BeanieSettings(mongo_client=AsyncMongoClient("mongodb://127.0.0.1:1/?connectTimeoutMS=1&serverSelectionTimeoutMS=1"), db_name="x"))` then core + beanie | B6 `(RepositoryProvider,"beanie")`, B9 `(AbstractDeadLetterQueue,"beanie")`, B12 ×2 | `AsyncMongoClient()` connects lazily (pymongo); `validate()` instantiates nothing; `BeanieRepositoryProvider`'s async `@PostConstruct` never runs. ⚠️ ASSUMPTION `scan("varco_beanie")` is import-clean without a server — 045 Step 1(e) measures it; if not, the subclass is written with `pytest.mark.integration` and a `COVERAGE.md` note |
| `varco_fastapi` | `varco_fastapi/tests/test_fastapi_di_wiring.py` | core + fastapi | A14 `(AbstractJobStore,None)`, A15 ×11, A16 ×2, A18 `(TaskSerializer,None)` | absorbs 045's `test_validate_clean_default_container.py`; adds the redundant-install pitfall test |

`COVERAGE.md`: the matrix gains a `di_wiring` row listing all ten subclass paths; "eight
conformance modules" → "nine" everywhere it is counted (`COVERAGE.md:3-6`, CLAUDE.md §Test
Conventions, `testkit/varco_conformance/__init__.py`); the `varco_casbin` stated-absence bullet
(`COVERAGE.md:52-55`) is rewritten — it now has a `pythonpath` and subclasses one suite.

### §D-removals — what goes, outright

`create_varco_container`, `setup_event_producer`, `get_container()`, the `streams=`/`setup_cache=`/
`setup_producer=`/`*packages`/`*entity_classes` parameters, the two `*args, **kwargs` wrappers
(#23-24, replaced by true re-exports), the 16 guards, `_bind_call_count`, the five guard tests. No
deprecation shims: `BACKLOG.md:53` allows breaking wiring changes outright, and each removal has a
one-line replacement in §Migration. `scripts/api_surface.py --check` **will** fail on this plan
(return annotations of ten top-level names change, `varco_memcached.bootstrap`'s signature changes)
— the snapshot is regenerated **in the same step** as the signatures (Step 13), never later.

### §D-docs — where each sentence goes

- **CLAUDE.md §"DI wiring verb taxonomy"** — the whole table is replaced (058 only polishes):

  | Verb | Signature | Meaning | Example |
  |---|---|---|---|
  | `bootstrap` | `(container: DIContainer) -> DIContainer` | one per package (ten, incl. `varco_core.di`); `container.scan(pkg, recursive=True)` (+ the package's own hand `provide()`s, idempotently); container **required**; never creates one, never reads `DIContainer.current()`, never touches `os.environ`; returns the same object; safe to call twice | `varco_kafka.di.bootstrap(container)` |
  | `async_bootstrap` | `async (container) -> DIContainer` | `bootstrap()` + `await container.ainstall(<the package's connection-opening @Configuration>)`; no kwargs | `varco_redis.di.async_bootstrap`, `varco_memcached.di.async_bootstrap` |
  | `bind_*` | `(container, …, *, qualifier=None) -> DIContainer` | registers **non-fallback** bindings the app decides at wiring time; the primitive is `varco_core.di.bind_instance(container, Interface, obj, *, qualifier=None)`; a second registration of the same `(interface, qualifier)` raises `AlreadyBoundError` — qualify to have two, use `di_overrides` to replace in a test | `bind_instance`, `bind_repositories`, `bind_clients`/`bind_clients_from`/`bind_peers`, `bind_trust_store`, `bind_retention_registry`, `bind_cloudevents_serializer`, `bind_websocket_adapter`/`bind_sse_adapter`/`bind_mcp_adapter`/`bind_skill_adapter` |
  | `enable_*` | `(container, …) -> DIContainer` | flips on an opt-in binding that shadows a `@Fallback` default; raises `AlreadyBoundError` if a non-fallback binding of that seam already exists (yours or a previous `enable_*`) | `enable_policy_authorizer`, `enable_token_revocation`, `enable_redis_token_revocation`, `enable_feature_flags`, `enable_tenant_membership`, `enable_authorization_audit` (wraps the current binding — call it last) |
  | `mount_*` | `(app, …)` | *(unchanged row)* | |
  | `install_*` | *(unchanged three-shape row)* | | |

  Removed rows/notes: the "returns `None` if providify is absent" clause; the `bind_*` "N typed
  per-item bindings" phrasing (subsumed). The AB-1/AB-3 name-collision bullets stay verbatim. Three
  new lines beneath the table: *Rule: varco never reads `DIContainer.current()` — every helper takes
  the container (guarded by `test_no_service_locator.py`); a middleware gets it via
  `install_middleware_stack(app, [(DISessionMiddleware, {"container": container})])`. Rule: the
  ten packages subclass `varco_conformance.di_wiring.DIWiringConformance`; a new package or a new
  `@Fallback` default adds a row to its `expected_defaults` or the suite fails.* §Test Conventions:
  "eight conformance modules" → "nine", `di_wiring.py` named, and the `varco_casbin`-has-no-testkit
  sentence deleted.
- **`technical_docs/features/dependency-injection.md`** — new **§Wiring contract** section after
  045's §Default bindings (058's banner updated to list it): the ten-line bootstrap recipe
  (§D-contract block), the `bind_instance` signature and the one rule (§D-bind), the `enable_*`
  rule (§D-enable), the streams replacement (§D-streams, with the 048 pointer), the locator section
  (§D-locator — what `di_global` is for), the `di_wiring` suite's contract and how a package opts in
  (§D-suite table, the `expected_defaults` convention, the P29 shim), and **Pitfalls**: "called
  `bootstrap()` with no argument" (`TypeError`, replacement); "expected `bind_x` twice to replace"
  (`AlreadyBoundError`, use `di_overrides`); "`install(VarcoFastAPIModule)` after `bootstrap()`"
  (`AMBIGUOUS_BINDING`); "scanned only `varco_fastapi`, `AbstractEventProducer` missing" (call
  `varco_core.di.bootstrap`); "set `VARCO_REDIS_USE_STREAMS` by mutating `os.environ` in a test"
  (use `bind_instance`/`monkeypatch.setenv`).
- **README.md**: the "DI wiring" snippet (`:709-736`) and the full-stack bootstrap (`:3495-3497`)
  rewritten to the recipe; one `bind_instance` example; `redis_bootstrap(container, streams=True)`
  → the two-line form; every `bootstrap()`-without-argument → `bootstrap(container)`.
- **ARCHITECTURE.md** `:79,88,100,165` ("bootstrap() scan helper") → "`bootstrap(container)`";
  `:1469,1849` (`varco_ws` DI module) → returns the container / `qualifier=`.
- **Feature docs**: `tls-trust-and-hot-reload.md` (4 `bind_trust_store` mentions → returns
  container), `retention-and-purge.md` (3), `cloudevents-envelope.md` (4; the "twice … last one
  wins" sentence → `AlreadyBoundError`), `reliability-preset.md` (1), `generic-router.md:221`
  (`DISessionMiddleware(container=)`), `docs/peer-service-integration.md` (5 `bind_peers`),
  `technical_docs/common-pitfalls.md:52`.
- **Package docstrings**: every `di.py` module docstring's "Usage" block; `varco_memcached/__init__.py:23`;
  `varco_ws/di.py:21-22,96-97` (`redis_bootstrap()` → `(container)`).
- **CHANGELOG.md `[Unreleased]`**: `### BREAKING — one wiring contract: explicit container, no
  service locator, `AlreadyBoundError` (Plan 046, DI-2 / DI-8 / DI-13)` = §Migration verbatim; `###
  Tests (Plan 046)` — the ninth suite, ten subclasses, the P29 filing, the five guard tests removed.
- **BACKLOG.md** `:75,89,91` → `done (Plan 046)`; `:91`'s "casbin has no DI test at all" → "casbin
  had a DI test but no suite membership".

## Steps

All commands from `/home/edoardo/projects/varco`. Steps 0-3 first, in order; 4-11 are the
package/helper rewrites (each independently verifiable; the suite is red for a package until its
step lands); 12-16 last.

### Preflight and primitives

0. [ ] **Preflight — confirm 045 landed, recount, confirm providify lines, measure the ⚠️s.**
       (a) `rg -c '@Fallback' varco_*/varco_*` ≥ 55; `rg -n 'setup_varco_defaults|setup_defaults' varco_*` → 0;
       `ls varco_core/varco_core/di.py` exists with `BACKEND_PRIORITY`; `uv run python -c "import importlib.metadata as m; print(m.version('providify'))"` → `2.1.0`.
       (b) Re-run §Context's entry-point `rg` → paste the count (expect 35); re-run the `current()`
       `rg` → paste the 13 code sites; `rg -n 'sys\.modules\["providify"\]|import providify  # noqa: F401' varco_*/tests varco_*/varco_*` → the guard-test inventory (the skill-adapter ⚠️).
       (c) On the installed copy (`.venv/lib/python3.12/site-packages/providify/`), re-read every
       `file:line` in §Context's providify table; fix any drifted citation in this plan first.
       (d) Measure: `container.scan("varco_beanie", recursive=True)` on a bare container with no
       server (import-clean? — if 045's preflight file already says so, cite it); the same for
       `varco_memcached`; `DIContainer().scan("varco_core", recursive=True)` then
       `varco_sa.di.bootstrap` with the sqlite `SAConfig` from §D-suite → `validate()` `errors`
       count (expect 0) and the `(kind, owner)` list of warnings (the `known_warnings` seed for
       `varco_sa`); the same for each of the ten §D-suite compositions — paste every warning
       into `/tmp/claude-1000/…/scratchpad/046-preflight.txt` with its owning plan (050 for the
       three disposer kinds, 048 for a settings `MISSING_BINDING_DEFAULTED`, else "046 fixes").
       (e) Copy 045's `varco_core/tests/test_fallback_defaults.py` A1-A13 `(ABC, default_cls,
       qualifier)` table into the preflight file — the `expected_defaults` source for Step 3.
       (f) `rg -n 'Plan 045' UPSTREAM-GAPS.md; rg -c '^\| \*\*P2' UPSTREAM-GAPS.md` → decides P28 vs
       P29 numbering for Step 2.
       **Verify**: the preflight file has all six sections; every ⚠️ in §Risks has a measured value.

1. [ ] `varco_core/tests/test_di_wiring_primitives.py` (**new, failing first**) — for
       `varco_core.di`: `AlreadyBoundError` is a `RuntimeError` and its message names interface +
       qualifier; `is_user_bound()` is `False` on an empty container, `False` when only a `@Fallback`
       binding exists (scan `varco_core.flags`), `True` after `bind_instance()`, exact-qualifier when
       `qualifier=` is given and inclusive of qualified bindings when it is `None`
       (`container.py:2096-2101`); `bind_instance()` returns a `ProviderBinding` with
       `.fallback is False`, `.qualifier == qualifier`; `container.get(I) is instance`; second
       `bind_instance` same `(I, q)` → `AlreadyBoundError`, different `q` → two bindings and
       `get(I, qualifier=q2)` resolves each; `bind_instance` over a `@Fallback` default → the
       instance wins and `validate()` has one `FALLBACK_SHADOWED` INFO for a provider-shaped default
       (A13 `OtelConfig`); `bootstrap(c) is c`; `bootstrap` twice → `len(c.describe().bindings)`
       unchanged; `import varco_core.di` alone leaves `"providify" not in sys.modules`-delta
       (extends 045's `test_di_constants.py`, which stays). Then `varco_core/varco_core/di.py` —
       add `AlreadyBoundError`, `is_user_bound`, `bind_instance`, `bootstrap` per §D-bind/§D-contract;
       providify imported **inside** function bodies (`# noqa: PLC0415` with the import-budget
       reason); `DIContainer`/`ProviderBinding` under `TYPE_CHECKING`; full docstrings (Args/
       Returns/Raises/Edge cases/Async safety) and a `DESIGN:` block per §D-bind's table;
       `__all__ = ["BACKEND_PRIORITY", "AlreadyBoundError", "bind_instance", "bootstrap", "is_user_bound"]`.
       **Verify**: `uv run pytest varco_core/tests/test_di_wiring_primitives.py varco_core/tests/test_di_constants.py -q` green; `uv run python scripts/import_budget.py --check --warn-only` unchanged.

2. [ ] **P29 filing (§D-suite).** `design/upstream-gaps/providify-descriptor-fallback-field.md` in
       the P27 report's eight-section shape; `UPSTREAM-GAPS.md` one "Open gaps" row (recreate the
       ledger from its header template if absent — CLAUDE.md's rule); strict xfail
       `test_binding_descriptor_carries_fallback` appended to
       `varco_core/tests/test_providify_contracts.py` (body: `@Fallback @Singleton class F: …`;
       `c.register(F)`; `d = c.describe().bindings[0]`; `assert getattr(d, "fallback", None) is True`).
       **Verify**: `uv run pytest varco_core/tests/test_providify_contracts.py -q -rxX` → exactly
       one more `xfailed` than before, reason contains `GAP: P29-` (or `P28-`).

3. [ ] `testkit/varco_conformance/di_wiring.py` (**new**) — `DIWiringConformance` per §D-suite
       (the eight tests; the varco_fastapi-only ninth lives in that subclass), the
       `fallback_bindings(container)` shim (module docstring: "reads `container._bindings` —
       the one private read in the testkit; delete when P29 ships, guard:
       `test_providify_contracts.py::test_binding_descriptor_carries_fallback`"), full docstrings.
       `testkit/varco_conformance/__init__.py` docstring: nine modules. Then the **core subclass**
       `varco_core/tests/test_di_wiring_conformance.py` with `expected_defaults` from Step 0(e)
       and an empty `known_warnings` (or the measured allowlist, each entry commented with its
       plan). **Verify**: `uv run pytest varco_core/tests/test_di_wiring_conformance.py -q` green
       (core needs nothing from later steps — `varco_core.di.bootstrap` landed in Step 1).

### The ten bootstraps

4. [ ] **`varco_kafka`, `varco_nats`, `varco_ws`** — `di.py` `bootstrap(container: DIContainer) ->
       DIContainer`: delete the guard and the `if container is None` branch, `from providify import
       DIContainer` at module scope, rewrite the module + function docstrings (Usage blocks pass a
       container; "returns `None`…" sentences gone; `varco_ws/di.py:21-22,96-97`). `varco_ws`
       `bind_websocket_adapter`/`bind_sse_adapter`: delete guards `:213-227`/`:340-352`, add
       `qualifier: str | None = None` (forwarded to `Provider(singleton=True, qualifier=…)`),
       `is_user_bound()` check → `AlreadyBoundError`, `-> DIContainer`, docstrings' "replaces"
       bullet → the rule. Tests: `varco_kafka/tests/test_kafka_di.py`, `varco_nats/tests/test_nats_di.py`,
       `varco_ws/tests/test_di.py` — every `bootstrap()`/`bootstrap(None)` call → `bootstrap(DIContainer())`;
       `git rm`-equivalent deletion of the two `*_importerror_guard` tests and the `pytest.skip("providify
       not installed")` at `test_di.py:36`; new `test_bind_websocket_adapter_twice_raises` /
       `_twice_qualified_binds_two`. New subclasses `test_kafka_di_wiring.py`, `test_nats_di_wiring.py`,
       `test_ws_di_wiring.py` (§D-suite rows; `git rm` 045's `test_kafka_fallback_tier.py`,
       `test_nats_fallback_tier.py`, and their `test_validate_clean_scan.py` legs after moving their
       assertions in). **Verify**: `make test PKG=varco_kafka`, `PKG=varco_nats`, `PKG=varco_ws`.

5. [ ] **`varco_redis`** — `di.py`: `bootstrap(container) -> DIContainer` (delete `streams=`, the
       `import os`, `:150-154`, the guard, the `None` branch; keep 045 E2's
       `scan("varco_core.revocation")`); `async_bootstrap(container) -> DIContainer` = `bootstrap`
       + `ainstall(RedisCacheConfiguration)`, RIDER-1 block `:228-235` deleted;
       `enable_redis_token_revocation(container, **redis_kwargs) -> DIContainer` with the
       `is_user_bound(AbstractTokenRevocationStore)` → `AlreadyBoundError` check; module docstring
       `:21-62` rewritten (Streams: the §D-streams two-liner + the env var; "before scan() — order
       matters" was 045's E3 — confirm gone). Tests: `git rm varco_redis/tests/test_async_bootstrap_no_providify.py`;
       `test_redis_async_bootstrap.py` → no kwargs, container required; `test_redis_revocation_di.py`
       → `-> DIContainer` + twice-raises; `test_redis_cloudevents_di.py:44-65` keeps `monkeypatch.setenv`
       (it tests the env path, legitimately); new `test_redis_di_wiring.py` (§D-suite row, incl. the
       `streams` replacement test and `"VARCO_REDIS_USE_STREAMS" not in os.environ` after
       `bootstrap()`); `git rm` 045's `test_redis_fallback_tier.py` after folding.
       **Verify**: `make test PKG=varco_redis`; `rg -n 'os\.environ' varco_redis/varco_redis/di.py` → 0.

6. [ ] **`varco_memcached`** — `di.py`: `bootstrap(container) -> DIContainer`; `async_bootstrap(container)
       -> DIContainer` (no `setup_cache`); guards `:109-120` gone; `_logger.debug("providify not
       installed…")` gone; `__init__.py:23` docstring. Tests: `varco_memcached/tests/test_di.py` —
       delete the five `sys.modules["providify"]` tests and the `setup_cache=False` tests
       (`:255-280`); the `_make_mock_container()` tests keep working with a required arg;
       `test_stop_idempotency_regression.py:12` docstring. New `test_memcached_di_wiring.py`.
       **Verify**: `make test PKG=varco_memcached`.

7. [ ] **`varco_casbin`** — `di.py`: `bootstrap(container) -> DIContainer` with the
       `is_user_bound(CasbinSettings)` guard around `provide(_provide_casbin_settings)` (comment:
       "048 replaces this with `bind_settings()`"); `enable_policy_authorizer(container) ->
       DIContainer` + `is_user_bound(AbstractAuthorizer)` → `AlreadyBoundError` (its Edge-cases
       bullet `:150-152` "bind your own at a higher priority" is obsolete under `@Fallback` —
       rewrite); guard `:112-115` gone. `varco_casbin/pyproject.toml` `[tool.pytest.ini_options]`
       gains `pythonpath = ["../testkit"]`. Tests: `test_di.py` — `bootstrap` twice → one
       `CasbinSettings` binding; `enable_policy_authorizer` twice → `AlreadyBoundError`; after the
       app binds its own `AbstractAuthorizer`, `enable_policy_authorizer` → `AlreadyBoundError`.
       New `test_casbin_di_wiring.py`. **Verify**: `make test PKG=varco_casbin`.

8. [ ] **`varco_sa`, `varco_beanie`** — `di.py`: `bootstrap(container) -> DIContainer`
       (`*entity_classes` removed; docstring example → `bootstrap(container);
       bind_repositories(container, User, Post)`); `bind_repositories(container, *classes) ->
       DIContainer` — per class, `is_user_bound(AsyncRepository[cls])` → `AlreadyBoundError`
       (the `# type: ignore[valid-type, misc]` stays as-is); `varco_sa` `create_tables(container:
       DIContainer)`; `varco_sa/bootstrap.py:145-150` guard → plain import. Tests: `test_sa_di.py`,
       `test_beanie_di.py` (the mock-container `bind_repositories` tests: the mock must answer
       `get_all_bindings` → `[]`; `bootstrap(c, _User)` call sites split); new `test_sa_di_wiring.py`
       (sqlite `SAConfig` via `bind_instance`; absorbs 045's `test_sa_fallback_tier.py` +
       `test_validate_clean_scan.py` → `git rm`), `test_beanie_di_wiring.py` (lazy `AsyncMongoClient`;
       `pytest.mark.integration` only if Step 0(d) found the scan needs a server — then also a
       `COVERAGE.md` note). **Verify**: `make test PKG=varco_sa`, `PKG=varco_beanie` (no Docker).

9. [ ] **`varco_fastapi`** — `di.py`: delete `create_varco_container` (`:445-506`),
       `setup_event_producer` (`:548-624`), the two wrappers (`:735-765`) → `from varco_fastapi.router.mcp
       import bind_mcp_adapter` / `from varco_fastapi.router.skill import bind_skill_adapter` re-exports
       at module scope (keep both in `__all__`); `bootstrap(container) -> DIContainer` (`*packages`,
       `setup_producer=` gone; docstring `:658-675` rewritten to the §D-contract recipe with
       `varco_core.di.bootstrap` first); `bind_clients`/`bind_clients_from` `-> DIContainer` +
       `AlreadyBoundError` per alias (`:142-147` prose → the rule); `__all__` loses
       `create_varco_container`, `setup_event_producer`. `client/peer.py:320-342` `bind_peers ->
       DIContainer`. `router/mcp.py` / `router/skill.py`: guards gone, `qualifier: str | None =
       None` (forwarded), `is_user_bound(MCPAdapter/SkillAdapter, qualifier=…)` → `AlreadyBoundError`,
       `-> DIContainer`, the "replaces" bullets → the rule, mcp's `:955-956` example now true.
       `middleware/session.py`: delete `get_container()` (`:59-69`) and the ContextVar prose
       (`:9-18`, `:61-63` — it was never a ContextVar); `DISessionMiddleware.__init__(self, app, *,
       container: DIContainer)` required; class docstring Args. Tests: `test_di_binding_health.py`,
       `test_di_binding_health_i18n_tz.py`, `test_lifecycle_component_discovery.py`, any
       `bootstrap(` call site (`rg -n 'bootstrap\(' varco_fastapi/tests`) → explicit container and
       `varco_core.di.bootstrap` where `AbstractEventProducer` is resolved; `milestone_f/test_mcp_adapter.py:442-467`
       guard test deleted, `bind_mcp_adapter` twice → raises / qualified → two; a
       `DISessionMiddleware` construction test (`TypeError` without `container=`; installed via
       `install_middleware_stack(app, [(DISessionMiddleware, {"container": c})])` and a request
       passes through `asession`/`arequest`). New `test_fastapi_di_wiring.py` (absorbs 045's
       `test_validate_clean_default_container.py` → `git rm`; adds the redundant-install pitfall
       test). **Verify**: `make test PKG=varco_fastapi`; `rg -n 'create_varco_container|setup_event_producer|get_container' varco_fastapi` → 0.

### The core helpers

10. [ ] **`varco_core` `bind_*` trio + `enable_*` quartet** — `tls/di.py:44-78` → two
        `bind_instance()` calls, `-> DIContainer`; `retention/di.py` → one `bind_instance()`,
        delete `_bind_call_count` and its comment block `:45-59`, docstring `:77-82` → the rule
        (twice → `AlreadyBoundError`; "use `di_overrides` in tests"); `event/cloudevents.py:501-562`
        → `bind_instance(container, CloudEventsSettings, settings)` + `bind_instance(container,
        Serializer[Event], CloudEventsJsonSerializer(settings))`, `-> DIContainer`, Edge-cases
        `:534-535` → the rule (a second call is the "changed the wire format twice" bug and now
        fails loudly); `revocation/di.py:70`, `tenancy/di.py:77`, `flags/di.py:58` → `->
        DIContainer` + `is_user_bound(<seam>)` → `AlreadyBoundError`; `auth/di.py:83-88`
        `RuntimeError` → `AlreadyBoundError`. Docstring rewrites of the four negative locator
        mentions (`retention/di.py:81`, `revocation/di.py:20`, `authority/registry.py:214`,
        `varco_fastapi/lifespan.py:158`) → "never reaches for the process-global container".
        Tests **first**: `test_tls_di.py` (returns container; twice → raises; the no-scan-side-effect
        guard stays), `test_retention_di.py` (its "twice replaces" test → "twice raises"; the
        `retention_no_di_side_effect` guard stays), `test_cloudevents_serializer.py` (twice →
        raises; the serializer still wins over A11), `test_revocation_di.py`, `test_tenant_membership_di.py`,
        `test_flags_di.py`, `test_authorizing_audit.py` (`pytest.raises(AlreadyBoundError)` — also
        satisfies `RuntimeError`). **Verify**: `make test PKG=varco_core`.

11. [ ] `varco_core/tests/test_no_service_locator.py` (**new**, §D-locator) — walks
        `Path(__file__).parents[2].glob("varco_*/varco_*/**/*.py")`, asserts no line contains
        `DIContainer.current(`, `DIContainer.acurrent(`, `DIContainer.scoped(`, or
        `DIContainer.reset(`; failure message lists `file:line`. Docstring states what the global
        remains for (applications; `di_global`). **Verify**: passes only after Steps 4-10 — it is
        the DI-8 done gate; `rg -n 'DIContainer\.current\(\)|acurrent\(\)' varco_*/varco_*` → 0.

### Docs, changelog, gates

12. [ ] **Docs (§D-docs)** — `CLAUDE.md` (the table + three lines + §Test Conventions counts);
        `technical_docs/features/dependency-injection.md` §Wiring contract + Pitfalls + banner;
        `README.md:709-736`, `:3495-3497`, every `bootstrap()` without argument (`rg -n 'bootstrap\(\)' README.md`);
        `ARCHITECTURE.md:79,88,100,165,1469,1849`; the seven feature/doc files listed in §D-docs;
        `technical_docs/common-pitfalls.md:52` + **one new row** ("expected a second `bind_*`/`enable_*`
        to replace → `AlreadyBoundError`; qualify or `di_overrides`"); `testkit/varco_conformance/COVERAGE.md`
        (`di_wiring` matrix row, "nine", the casbin bullet). **Verify**: `rg -n 'DIContainer\.current|returns None if providify|providify is not installed|streams=True|setup_producer|create_varco_container|setup_event_producer' README.md ARCHITECTURE.md CLAUDE.md technical_docs docs examples varco_*/varco_*` → 0; `make docs`.

13. [ ] **Gates + snapshot.** `uv run python scripts/api_surface.py` (regenerate — expected diff:
        return annotations on `bind_clients`, `bind_clients_from`, `bind_mcp_adapter`,
        `bind_skill_adapter`, `bind_websocket_adapter`, `bind_sse_adapter`, `bind_repositories` ×2,
        `enable_tenant_membership`, `bootstrap`/`async_bootstrap` (`varco_memcached`), new
        `qualifier=` params; **no removals of a top-level name** — `create_varco_container` and
        `setup_event_producer` were `varco_fastapi.di`-level, confirm with `rg -n '"create_varco_container"\|"setup_event_producer"' varco_fastapi/varco_fastapi/__init__.py` → 0);
        commit `design/api-freeze-and-standards/measurements/api-surface.{json,md}` in this step.
        `make lint` (`api-check` green against the regenerated snapshot; `asyncapi-check`,
        `import-budget --warn-only` unchanged); `make type-check` (M1: expect −0 or better — the
        `Any` → `DIContainer` annotations remove no ignores but may surface new errors in tests that
        passed `None`; fix the tests, never the annotation); `make test` (eleven suites; `xfailed`
        = 045's count + 1 for P29; `0 xpassed`).

14. [ ] `CHANGELOG.md` `[Unreleased]` above the 045 block (§D-docs); `BACKLOG.md:75,89,91` →
        `done (Plan 046)`; `plans/000-index-4-0-di-standardization.md` 046 row unchanged.
        **Verify**: `rg -n 'Plan 046' CHANGELOG.md BACKLOG.md` ≥ 3.

15. [ ] **Handoff notes** (one paragraph each, appended to the neighbour plan's "Depends on"/header
        line, or to the index's row if the plan file does not yet exist): **047** — `bootstrap()`
        is the registration point for its `@Requires`-gated providers; `expected_defaults` per
        package is the seed of the catalogue and should be *generated* from it once the catalogue
        exists (then `test_declared_defaults_are_complete` reads the catalogue, and the P29 shim's
        only remaining consumer is the completeness cross-check); `enable_*` now raises on a
        second non-fallback — its precedence decision must not reintroduce a silent no-op.
        **048** — the casbin `is_user_bound` guard and the redis `bind_instance(...use_streams=True)`
        docs are the two lines it replaces; `AlreadyBoundError` is available for
        `SettingsAlreadyBoundError` to subclass. **049** — `register(container)` = the package's
        `bootstrap(container)`; nothing in any `bootstrap()` needs an event loop. **050** — the
        `known_warnings` entries naming 050 across the ten subclasses are its work list; `async_bootstrap`
        is the only place a `@Configuration` is `ainstall`ed by a varco verb. **052** — the P29
        report and the shim; `is_user_bound()` is the "is a user override live for this seam"
        primitive the provenance report can call. **058** — §Migration below is the DI-2/DI-8/DI-13
        entry; the CLAUDE.md table is final in shape.

## Edge cases

- **`bootstrap()` called with no argument** → `TypeError` (missing positional) — the intended
  loud failure; §Migration names the replacement.
- **`bootstrap()` called twice, with an app binding registered in between** → second call
  registers nothing (scan dedup); the app binding still wins (`@Fallback`); `validate()` clean.
- **Two packages' `bootstrap()` in either order** (e.g. `varco_ws` before `varco_core`) → both
  fine; nothing is resolved at bootstrap time, so `varco_ws`'s `Inject[AbstractEventBus]` binds
  lazily. The suite's ws subclass composes core **first** only for readability.
- **`bind_instance(c, I, obj)` when a `@Fallback` default of `I` exists** → registers, wins,
  `FALLBACK_SHADOWED` INFO for provider-shaped defaults (045 §D-proof for scanned ones).
- **`bind_instance` unqualified when a *qualified* non-fallback exists** → `AlreadyBoundError`
  (an unqualified request would tie the two — `container.py:2096-2101`). Message says "qualify
  this binding or the existing one".
- **`bind_instance` with `qualifier="x"` when only unqualified non-fallback exists** → registers;
  `get(I)` now sees two non-fallbacks → `AMBIGUOUS_BINDING` at unqualified injection points. This
  is providify's documented semantics, not a varco bug; the guide's Pitfalls row says "qualify
  *both*".
- **`enable_policy_authorizer()` after the app bound its own `AbstractAuthorizer`** →
  `AlreadyBoundError` naming the seam — the "on when it is not" pitfall made loud.
- **`enable_authorization_audit()` after `enable_policy_authorizer()`** → wraps the policy
  authorizer (intended); after itself → `AlreadyBoundError`.
- **`bind_repositories(c, User)` then `bootstrap(c)` again** → fine (bootstrap no longer binds
  repositories); `bind_repositories(c, User)` again → `AlreadyBoundError` for `AsyncRepository[User]`.
- **`async_bootstrap(container)` with no reachable Redis/Memcached** → `ConnectionError` from
  `ainstall` — unchanged; `bootstrap()` is the scan-only path.
- **A subclass forgets a new `@Fallback` default in `expected_defaults`** →
  `test_declared_defaults_are_complete` fails naming the `(interface, qualifier)` — the
  contributor's signal to add the row (and, post-047, the catalogue entry).
- **A subclass's container has a `WARNING` not in `known_warnings`** → `test_validate_is_clean`
  fails listing `(kind, owner)`; the fix is an allowlist entry *with a plan number*, never a
  blanket filter.
- **`DISessionMiddleware` constructed positionally with a container** → `TypeError`
  (keyword-only) — deliberate; Starlette's `add_middleware(cls, **kwargs)` only passes kwargs.
- **`varco_beanie` scan turns out to need a server** (⚠️) → the subclass is `integration`-marked;
  `COVERAGE.md` records it; the beanie `bootstrap()` itself is then a bug for 050/056 to fix
  (a bootstrap must be import-clean), noted in the handoff.

## Verification

```bash
# Contract
rg -n 'DIContainer\.current\(\)|\.acurrent\(\)|DIContainer\.scoped\(|DIContainer\.reset\(' varco_*/varco_*   # 0
rg -n 'except ImportError' varco_*/varco_*/di.py varco_fastapi/varco_fastapi/router/{mcp,skill}.py varco_sa/varco_sa/bootstrap.py   # 0
rg -n 'os\.environ' varco_redis/varco_redis/di.py                                              # 0
rg -n 'create_varco_container|setup_event_producer|get_container\(|_bind_call_count' varco_*/varco_*   # 0
rg -n 'def bootstrap\(container: DIContainer\) -> DIContainer' varco_*/varco_*/di.py | wc -l   # 10
rg -n '\-> Any:' varco_*/varco_*/di.py varco_core/varco_core/{auth,revocation,tenancy,flags,tls,retention}/di.py   # 0 on wiring verbs
rg -n 'pythonpath' varco_casbin/pyproject.toml                                                 # 1

# Suite
uv run pytest varco_core/tests/test_di_wiring_primitives.py varco_core/tests/test_di_wiring_conformance.py \
              varco_core/tests/test_no_service_locator.py -q
for p in kafka nats redis memcached ws casbin sa beanie fastapi; do uv run pytest varco_$p/tests/test_${p}_di_wiring.py -q; done
uv run pytest varco_core/tests/test_providify_contracts.py -q -rxX     # +1 xfailed (P29), 0 xpassed

# Gates
uv run python scripts/api_surface.py --check   # green against the regenerated snapshot
make lint && make type-check && make test && make docs
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — the installed providify 2.1.0 matches the tree line-for-line (`scanner.py:204-206,249-251`, `container.py:1019,2168-2169,7415-7418,7464`, `descriptor.py:49-54`, `pytest_plugin.py:107-129`) | Every cited semantic (scan dedup, `provide()` append, no `fallback` on the descriptor) is what runs | Step 0(c) re-reads on the installed copy |
| ⚠️ ASSUMPTION — `scan("varco_beanie")`/`scan("varco_memcached")` are import-clean without a server (045 Step 1(e) may already have measured it) | Every `di_wiring` subclass is Docker-free | Step 0(d); fallback is an `integration`-marked subclass + `COVERAGE.md` note + a handoff bug for the package |
| ⚠️ ASSUMPTION — the ten composed containers have **no `ERROR`** and only disposer-kind/settings-kind `WARNING`s at 2.1.0 (045 §D-validate covered core+fastapi+kafka+nats+sa; redis/memcached/beanie/ws/casbin are new here) | `test_validate_is_clean` is honest per package | Step 0(d) measures each; residue → `known_warnings` with a plan number; an `ERROR` that is not a missing app-supplied binding stops the plan (it is a real wiring bug to fix first) |
| ⚠️ ASSUMPTION — no test outside the deleted five depends on `bootstrap()`/`bootstrap(None)` resolving the global (`rg -n 'bootstrap\((None)?\)' varco_*/tests`) | `make test` green after Step 9 | Step 0(b) inventory; each hit becomes `bootstrap(DIContainer())` |
| ⚠️ ASSUMPTION — 045's `expected_defaults` names (A1-A13) are exact ABC names | The completeness test matches the real bindings | Step 0(e) copies from 045's landed test, not from this plan's table |
| `AlreadyBoundError` on `enable_*` breaks an app that called one twice by accident | It was `AMBIGUOUS_BINDING` at 2.1.0 already; the new failure is earlier and named | §Migration bullet; the message names the seam and the fix |
| Removing `setup_event_producer` leaves an app that scanned only `varco_fastapi` without an `AbstractEventProducer` | `AsyncService`'s `NoopEventProducer` constructor fallback is unchanged; `validate()` reports `MISSING_BINDING_DEFAULTED` where it is optional | §Migration: call `varco_core.di.bootstrap(container)` |
| `api_surface.py --check` red between the signature edits and the snapshot regen | The snapshot is regenerated in the same step (13) and committed with the code | Step 13's commit boundary |
| The testkit's private `_bindings` read rots when providify renames it | One shim, one guard | P29 strict xfail; `fallback_bindings()` has a single call site |
| Health-check/other collection contributions accidentally listed in `expected_defaults` | Only `@Fallback` bindings appear there — Class C is never `@Fallback` (045) | `test_declared_defaults_are_fallbacks` fails on a non-fallback row |
| `varco_core.di` importing providify inside function bodies reads as a lint smell | Import budget unchanged (045's `test_di_constants.py` assertion) | `# noqa: PLC0415` with the reason on each |

## Migration (3.x → 4.0, DI-2 / DI-8 / DI-13 entry — Plan 058 assembles)

**BREAKING — every `bootstrap()` requires a container and returns it.** All ten
(`varco_core.di` is new; `varco_kafka`, `varco_nats`, `varco_redis`, `varco_memcached`, `varco_ws`,
`varco_casbin`, `varco_sa`, `varco_beanie`, `varco_fastapi`) are `bootstrap(container: DIContainer)
-> DIContainer`. None consults `DIContainer.current()`; none returns `None`.

| Old | New |
|---|---|
| `container = varco_x.di.bootstrap()` | `container = DIContainer(); varco_x.di.bootstrap(container)` |
| `container.scan("varco_core", recursive=True)` (by hand) | `varco_core.di.bootstrap(container)` (identical effect; the documented entry point) |
| `varco_fastapi.di.bootstrap(container, "myapp")` / `bootstrap("myapp")` | `varco_fastapi.di.bootstrap(container); container.scan("myapp", recursive=True)` |
| `varco_fastapi.di.bootstrap(container, setup_producer=True)` / `setup_event_producer(container)` | `varco_core.di.bootstrap(container)` — `BusEventProducer` is the `@Fallback` `AbstractEventProducer` (045 A10) |
| `create_varco_container("varco_core", "varco_redis", "myapp")` | `c = DIContainer(); varco_core.di.bootstrap(c); varco_redis.di.bootstrap(c); c.scan("myapp", recursive=True)` |
| `varco_sa.di.bootstrap(container, User, Post)` / `varco_beanie.di.bootstrap(container, User, Post)` | `bootstrap(container); bind_repositories(container, User, Post)` |
| `varco_sa.di.create_tables()` | `create_tables(container)` |
| `varco_redis.di.bootstrap(container, streams=True)` | `from varco_core.di import bind_instance; bind_instance(container, RedisEventBusSettings, RedisEventBusSettings(use_streams=True)); bootstrap(container)` — or `VARCO_REDIS_USE_STREAMS=true` in the environment (unchanged). Plan 048 replaces the first line with `bind_settings(container, RedisEventBusSettings, instance=…)` |
| `await varco_redis.di.async_bootstrap(container, setup_cache=True)` / `(…, streams=True)` | `await async_bootstrap(container)` — always installs `RedisCacheConfiguration`; use `bootstrap()` for scan-only; streams as above |
| `await varco_memcached.di.async_bootstrap(container, setup_cache=False)` | `bootstrap(container)` |
| `await varco_memcached.di.async_bootstrap()` | `await async_bootstrap(container)` |
| `DISessionMiddleware(app)` / `app.add_middleware(DISessionMiddleware)` | `app.add_middleware(DISessionMiddleware, container=container)` or `install_middleware_stack(app, [(DISessionMiddleware, {"container": container}), …])` |
| `varco_fastapi.middleware.session.get_container()` | removed — pass the container you built |
| `from varco_fastapi.di import bind_mcp_adapter, bind_skill_adapter` | still works — now true re-exports with their real signatures |

**BREAKING — `bind_*` and `enable_*` return the container and never silently lose a binding.**
`bind_trust_store`, `bind_retention_registry`, `bind_cloudevents_serializer`, `bind_websocket_adapter`,
`bind_sse_adapter`, `bind_mcp_adapter`, `bind_skill_adapter`, `bind_repositories` (both),
`bind_clients`, `bind_clients_from`, `bind_peers` now return `DIContainer` (was `None`). A second
registration of the same `(interface, qualifier)` raises `varco_core.di.AlreadyBoundError`
(previously: appended, first-registered silently won — the "replaces" docstrings were wrong).
`bind_websocket_adapter`/`bind_sse_adapter`/`bind_mcp_adapter`/`bind_skill_adapter` gain
`qualifier=` for multiple adapters. `enable_token_revocation`, `enable_redis_token_revocation`,
`enable_feature_flags`, `enable_tenant_membership`, `enable_policy_authorizer` are typed
`-> DIContainer` and raise `AlreadyBoundError` when a non-fallback binding of their seam already
exists (yours, or a previous call); `enable_authorization_audit`'s double-wrap `RuntimeError` is now
`AlreadyBoundError` (a `RuntimeError` subclass). In tests, replace a binding with providify's
`di_overrides`/`ContainerOverrides`, not a second call.

**New — `varco_core.di.bind_instance(container, Interface, obj, *, qualifier=None)`** registers any
already-constructed object as a non-fallback singleton; `varco_core.di.is_user_bound(container,
Interface, qualifier=None)` answers "does a non-fallback binding exist".

**Removed — "returns `None` when providify is not installed".** providify has been a declared
dependency of every varco package since 3.0; the guards and their documented `None` path are gone.

**Removed — `DIContainer.current()` inside varco.** varco never reads the process-global
container. Applications may; providify's `di_global` fixture exists for their tests.

**New — `testkit/varco_conformance/di_wiring.py`.** Out-of-tree backends are unaffected (the
testkit is never packaged). In-tree: every package subclasses it.

## Docs / backlog updates

- `CLAUDE.md` — taxonomy table rewritten, three rule lines, §Test Conventions counts (Step 12).
- `technical_docs/features/dependency-injection.md` — §Wiring contract + Pitfalls (Step 12).
- `README.md`, `ARCHITECTURE.md`, seven feature/doc files, `common-pitfalls.md` (+1 row) (Step 12).
- `testkit/varco_conformance/COVERAGE.md` — `di_wiring` row, "nine", casbin bullet (Step 12).
- `design/upstream-gaps/providify-descriptor-fallback-field.md` + `UPSTREAM-GAPS.md` row (Step 2).
- `design/api-freeze-and-standards/measurements/api-surface.{json,md}` — regenerated (Step 13).
- `CHANGELOG.md`, `BACKLOG.md:75,89,91` (Step 14); neighbour handoff notes (Step 15).

## Done criteria

- [ ] Ten `def bootstrap(container: DIContainer) -> DIContainer` in `varco_*/varco_*/di.py`
      (incl. `varco_core/varco_core/di.py`); `rg -n 'DIContainer\.current\(\)|\.acurrent\(\)' varco_*/varco_*` → 0;
      `test_no_service_locator.py` green.
- [ ] `rg -n 'os\.environ' varco_redis/varco_redis/di.py` → 0; the streams replacement test passes
      with `"VARCO_REDIS_USE_STREAMS" not in os.environ`.
- [ ] Zero `except ImportError` providify guards in wiring modules; the five guard tests are gone;
      no docstring says "returns `None` if providify is not installed".
- [ ] `create_varco_container`, `setup_event_producer`, `get_container`, `_bind_call_count`,
      `streams=`, `setup_cache=`, `setup_producer=`, `*packages`, `*entity_classes` are gone.
- [ ] `varco_core.di` exports `AlreadyBoundError`, `bind_instance`, `is_user_bound`, `bootstrap`
      (+ 045's `BACKEND_PRIORITY`); `import varco_core.di` still imports no providify.
- [ ] Every `bind_*`/`enable_*` is typed `-> DIContainer` and raises `AlreadyBoundError` on a
      repeat; the four adapters accept `qualifier=`.
- [ ] `testkit/varco_conformance/di_wiring.py` exists; ten subclasses pass without Docker (or one
      beanie exception recorded in `COVERAGE.md`); `COVERAGE.md` has the row; `varco_casbin` has
      `pythonpath = ["../testkit"]`.
- [ ] 045's four `*_fallback_tier.py` seeds and its `test_validate_clean_*` legs are folded into
      the subclasses and deleted.
- [ ] P29 report + ledger row + strict xfail exist; `test_providify_contracts.py` has exactly one
      more `xfailed`.
- [ ] `api-surface` snapshot regenerated and committed with the signature changes; `make lint`,
      `make type-check`, `make test`, `make docs` green; no file under
      `/home/edoardo/projects/providify` modified.
- [ ] CLAUDE.md table is the new shape; `dependency-injection.md` has §Wiring contract; CHANGELOG
      BREAKING block = §Migration; BACKLOG `DI-2`/`DI-8`/`DI-13` → `done (Plan 046)`.
