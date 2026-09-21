# Plan 049 — `VarcoModule` and the composable app: `create_varco_app(container, modules=[…])` (DI-1, FLIP-1, RO-4)

Covers BACKLOG 4.0 rows **`DI-1`** (`BACKLOG.md:80` — *"One `VarcoModule` unit per package —
`register(container)` → `async_init()` → `validate()` → `start()`/`stop()` — and
`create_varco_app(modules=[...])` replacing the eleven hand-passed keyword objects (`migrations`,
`tenancy`, `reliability`, `retention`, `jwks_refresh`, `i18n`, `timezone`, `security_headers`,
`body_limit`, `rate_limit`, `extra_lifespan_components`) with declared ordering (`depends_on=`)"*),
**`FLIP-1`** (`BACKLOG.md:81` — *"`SecurityPostureLifecycle` auto-registered by `create_varco_app`
… becomes trivial once `DI-1` lands (it is a module with a default-on flag)"*), **`RO-4`**
(`BACKLOG.md:112` — *"`create_varco_app` gains a `log_redactor=` keyword … folds into `DI-1`'s
module shape"*), and **answers Open Question 1** (`BACKLOG.md:125-129` — *"a providify
`@Configuration` subclass with `depends_on=`, or a varco ABC that owns a `@Configuration`?"*) —
answered in §D-shape: **a varco ABC, never a `@Configuration`**, with providify source cited for
every claim about what `@Configuration`/`depends_on=` do and do not give us.

Slice row 049 of `plans/000-index-4-0-di-standardization.md`. **Fixed inputs from landed plans**,
referenced by number and never re-argued: 045 (`@Fallback` defaults, `varco_core.di.BACKEND_PRIORITY`),
046 (`bootstrap(container: DIContainer) -> DIContainer` required-container contract; `bind_instance`
/ `is_user_bound` / `AlreadyBoundError`; `DIContainer.current()` at zero; the `di_wiring` conformance
suite), 047 (`@Requires`-gated backends; `feature_default("<feature>")` + `VARCO_<FEATURE>_ENABLED`;
the tri-state kwarg semantics §D-features "defined here and moved by 049"), 048 (`bind_settings()`
recipe, `bind_core_settings()`, per-field precedence, `VARCO_LIFECYCLE_DISCOVERY_WARN` allowlisted
"until 049 replaces lifecycle discovery"). **Neighbours, by number, never absorbed**: **050** owns
the runtime *phase* model, the strict reverse-order shutdown guarantee across phases, `@Disposes`
reachability for every provider-produced singleton, and DI-wiring the hand-constructed infra — 049
ships only the module unit, `depends_on`-declared *install* ordering, and the `start()`/`stop()`
hook slots 050 sequences; **051** owns multi-binding (the posture collectors become `Multibound`
there; here they are the current hand list); **052** owns `validate()` *into the lifespan* and
`varco diagnose` — 049 defines the `validate()` slot and the `ModuleRegistry` 052 reads, and never
invokes `validate()` from the lifespan; **054** owns FLIP-2 (`enforce="refuse"`); **057** added the
redaction posture collector; **058** finishes the docs and assembles §Migration.

**Research briefs**: 015 §1 (every surveyed framework registers modules *explicitly, never by
scanning* — Spring's imports file, NestJS `imports`, Django `INSTALLED_APPS`, Laravel
`providers.php`), §3 (*"No framework uses a 15-keyword constructor"*; ASP.NET's two-stage
`AddX`/`UseX`; Laravel's `register()`-then-`boot()`), §Librarian #2 (*"a `varco.Module` ABC …
wrapping `register` + `async_init` + `validate` + `middleware_install` … then
`create_varco_app(modules=[...])`"*); 014 §Librarian #3 (Spring `SmartLifecycle` phases with a
reverse-shutdown guarantee — **050's**, cited here only to mark the boundary).

**providify source of record**: `/home/edoardo/projects/providify` at 2.1.0 (`pyproject.toml:3`),
the tree 045 pinned. Every `providify/…:line` below was read in that tree on 2026-09-15.

## Goal

After this plan: `varco_core.di.VarcoModule` is the one unit of wiring — a plain varco ABC with
`register(container)` / `async_init(container)` / `validate(container)` / `start(container)` /
`stop(container)` slots, a `depends_on` class attribute, and the 047 tri-state `enabled` kwarg;
**every package ships exactly one** (`CoreModule`, `FastAPIModule`, `SAModule`, `BeanieModule`,
`RedisModule`, `KafkaModule`, `NatsModule`, `MemcachedModule`, `CasbinModule`, `WsModule`) and
every subsystem that was a `create_varco_app` keyword is one too; `create_varco_app(container, *,
modules=None, routers=None, …)` takes a module list instead of eleven keyword objects (+ six
`enable_*` booleans + `cors`) and installs middleware from the modules' declared slots in the
**unchanged, characterised** §D-order; `SecurityPostureModule` is in the default set (FLIP-1) with
`VARCO_SECURITY_POSTURE_ENABLED=false` as the escape hatch; `LoggingModule(redactor=…)` is where
RO-4's redactor goes; a test proves `container.scan("varco_core", recursive=True)` activates no
module; the `di_wiring` conformance suite checks every package's module registers cleanly; the
example app boots through `create_varco_app(modules=[…])` and `make asyncapi-check` still passes.

## Non-goals

- **Phases, cross-module start/stop invariants, `@Disposes` reachability, DI-wiring
  `SADeadLetterQueue`/`OutboxRelay`/`WebhookDispatcher`/…** — 050. This plan reuses
  `VarcoLifespan`'s existing "start in order, stop LIFO" (`lifespan.py:239-260`) at *module*
  granularity and states in §D-order exactly which ordering it does and does not promise.
- **Calling `validate()` from the lifespan; `varco diagnose`** — 052. The slot exists; nothing
  calls it except tests.
- **Multi-binding the posture collectors / `TenantSource`s / retention targets** — 051.
- **`enforce="refuse"` default** — 054 (FLIP-2). `SecurityPostureSettings.enforce` is untouched.
- **Any change to providify.** No `@Configuration` subclassing, no new upstream ask (§D-shape
  needs nothing from providify it does not already ship).
- **Backend selection** (`VARCO_<SEAM>_BACKEND`) — 047; a backend module's `register()` is its
  package's `bootstrap()`, which 047 already gates.
- **A worker/CLI host that runs modules without FastAPI** — the core ABC is transport-free on
  purpose (§D-http) so 052/RO-9 can build one; none is built here.
- **The nine `removed_in="4.0.0"` sites** — 058.
- **A `varco_core` top-level re-export of `varco_core.di.module`** — same 045/046 non-goal
  (PEP 562 lazy map, import budget); `from varco_core.di import VarcoModule` is the import.

## Context

### What providify 2.1.0's `@Configuration` / `depends_on=` / `install()` actually guarantee (read 2026-09-15)

| Claim | Where | What it means for §D-shape |
|---|---|---|
| `@Configuration` is a **marker** stamped on the class (`ConfigurationMetadata(depends_on=…)` in `cls.__dict__`), nothing else | `providify/decorator/module.py:84-189` (`:176-182` `_decorate`); `providify/metadata.py:536-547` | Own-`__dict__` lookup only — `_has_configuration_module()` *"does not walk MRO — so subclasses of a `@Configuration` class are not treated as modules themselves"* (`metadata.py:539-540`; `module.py:130-132`). A `VarcoModule` base decorated `@Configuration` would confer **nothing** on its subclasses |
| `depends_on=` orders **installation only**: `install(M)` runs `resolve_install_order([M])` — deterministic DFS post-order, deps first in declaration order, transitive, each class once; a cycle raises `ModuleCycleError` **before** any instantiation; a non-`@Configuration` dep raises `TypeError` | `providify/modules.py:52-141` (`:109-136` `visit`); `providify/container.py:7099-7160` (`:7153-7160`); `ainstall` mirror `:7162-7196` | Exactly the ordering semantics varco wants — but only for *install*. providify has no vocabulary for "start after X" or "stop before X"; `@PreDestroy` on a module runs at `ashutdown()` **after every singleton has been torn down**, in reverse install order (`container.py:5380-5402`; providify `CHANGELOG.md:581-584`) — the wrong moment for "consumers stop before the bus" (050) |
| `install()` is **idempotent** (dedup by `_installed_modules`); `install()` **instantiates the class itself** with constructor injection (`_resolve_constructor`), runs `@PostConstruct`, registers only `@Provider`-marked callables found in `vars(module_cls)`, records the instance | `container.py:7111-7116`, `:7198-7234` (`_install_one`), `:7260-7345` (`_register_module_providers`: `vars(module_cls)` `:7322`, `_get_provider_metadata` gate `:7327`) | (a) A `@Configuration` cannot be handed a **user-constructed instance** with arguments (`MigrationsModule(migrator)`, `RetentionModule(registry)`, `RateLimitModule(bundle)`) — the container builds it. (b) `vars()` is own-attributes only, so inherited `@Provider` methods on a base class are never registered. (c) **Non-`@Provider` methods are ignored** — a `@Configuration` *can* carry `start()`/`stop()` without providify interpreting them; that is not what decides §D-shape, (a) and the scan rule are |
| `ainstall()` awaits async constructor deps and an `async def @PostConstruct`; nothing else is awaited | `container.py:7236-7258`; `module.py:127-129` | Async setup *can* live in a module `@PostConstruct` — but the scanner installs with **sync** `install()` (`scanner.py:282`), so a scanned module with an async `@PostConstruct` raises `RuntimeError` (`container.py:7219`). Varco's `async_init()` therefore stays a varco slot, awaited by the lifespan's `setup=` hook, and calls `container.ainstall(<a real @Configuration>)` where a package has one (`RedisCacheConfiguration`) |
| **The scanner auto-installs every public, non-re-exported `@Configuration` class it meets** (`container.install(cls)`) | `providify/scanner.py:142-162` (`_scan_module`; the `@Configuration` branch `:161-162`), `:276-282` (`_autoregister_configurator`); skip rules: attribute name starts with `_` (`:144`), or `inspect.getmodule(obj) is not module` (`:148`) | This is the locked T3 decision's mechanism (`BACKLOG.md:249-252`: *"a varco module in `varco_core` is registered, never scanned into activation"*). A `@Configuration`-shaped `VarcoModule` in `varco_core` would be activated by `container.scan("varco_core", recursive=True)` — a documented, in-use pattern (CLAUDE.md, `varco_core.di.bootstrap` is literally that call) — unless hidden behind an `_`-prefixed name and re-exported, which is a trick, not a design |
| `@Disposes` wiring is scoped to the bindings *this* `install()`/`ainstall()` registered (`own`); an unmatched disposer is a `WARNING` | `container.py:7321`, `:7347-7384`; providify `CHANGELOG.md:16-24` | Unaffected by this plan: the `@Configuration`s that carry `@Disposes` (`RedisCacheConfiguration`, the four DLQ configurations, `MemcachedCacheConfiguration`) stay providify modules, installed by scan/`async_bootstrap` exactly as 045/046 left them. A `VarcoModule` never carries `@Disposes` — its teardown is `stop()` (050 extends) |
| Module `@PostConstruct` runs once at install; `@PreDestroy` at shutdown phase 2 | providify `CHANGELOG.md:579-591`; `container.py:5380-5402` | Not used by varco modules (plain classes); noted so nobody adds one expecting lifespan timing |

### Everything `create_varco_app` takes today, and where each goes (`varco_fastapi/varco_fastapi/app.py:104-144`)

| Today | Kind | After 049 |
|---|---|---|
| `container: Any \| None = None` | positional | **required** `DIContainer` (§D-signature) |
| `routers`, `scan_packages`, `title`, `version`, `description`, `openapi_url`, `docs_url`, `validate`, `strict_validation`, `mcp_adapter`, `mcp_path`, `skill_adapter`, `skill_base_url`, `skill_agent_card_path`, `skill_tasks_prefix` | the app's *own* surface / `create_varco_app`'s build-time checks | **stay** (§D-signature) |
| `cors` | edge | `CorsModule(config=None)` — default-on |
| `enable_error_middleware` | edge | `ErrorEnvelopeModule()` — default-on |
| `enable_tracing` | edge | `TracingModule()` — default-on |
| `enable_logging` | edge | `LoggingModule(redactor=None)` — default-on; **RO-4 lands here** |
| `enable_metrics` | edge + mount | `MetricsModule()` — opt-in |
| `enable_profiling` | edge | `ProfilingModule()` — opt-in (`VARCO_PROFILING_ENABLED` unchanged) |
| `security_headers: Settings \| bool \| None` | edge, tri-state | `SecurityHeadersModule(settings=None, *, enabled=None)` — default-on |
| `body_limit: Settings \| bool \| None` | edge, tri-state | `BodyLimitModule(settings=None, *, enabled=None)` — default-on |
| `rate_limit: RateLimitBundle \| None` | edge, two slots | `RateLimitModule(bundle)` — opt-in |
| `i18n`, `timezone` | edge + lifecycle (one `LocalizationMiddleware`, RD-3) | `LocalizationModule(i18n=None, timezone=None)` — in the default set, each toggle `enabled` from its settings (env) exactly as today |
| `migrations`, `migration_settings` | lifecycle, prepended | `MigrationsModule(*migrators, settings=None)` — opt-in |
| `tenancy` | lifecycle, prepended | `TenancyModule(pool=…, supervisor=None, catalog_subscription=None)` — opt-in |
| `reliability` | lifecycle, appended | `ReliabilityModule(preset)` — opt-in |
| `retention` | lifecycle, appended | `RetentionModule(registry, interval=0.0)` — opt-in |
| `jwks_refresh` | lifecycle, appended | `JwksRefreshModule(registry, interval=None)` — opt-in |
| `extra_lifespan_components` | lifecycle | `LifecycleModule(component, *, name=…, depends_on=())` adapter (§D-adapters) |
| `extra_middleware` | edge (OUTSIDE `ErrorMiddleware`) | `MiddlewareModule(entries, slot=EdgeSlot.EXTRA)` adapter — same position (§D-adapters) |
| `configure_jwt` | process-global | `FastAPIModule(configure_jwt=True)` |
| `global_attributes`, `capture_params` | process-global | `ObservabilityModule(global_attributes=None, capture_params=None)` — default-on, both `None` = no-op |
| `_collect_lifecycle_components` (`app.py:871-918`), `_try_resolve_component` (`:921-1087`), `_lifecycle_discovery_warns` + `VARCO_LIFECYCLE_DISCOVERY_WARN` (`:838-868`) | hidden discovery | **deleted**: `CoreModule.start()` starts the resolved `AbstractEventBus`/`AbstractJobRunner`; `WsModule.start()` the two push adapters (§D-core) |
| the `VARCO_MIGRATE_MODE`-set-but-no-`migrations=` and `VARCO_TENANCY_ISOLATION`-set-but-no-`tenancy=` warnings (`:392-401`, `:409-418`) | build-time warnings | kept in `create_varco_app` as `_warn_unowned_settings()` reading the 048-bound settings, keyed on module *absence* (052 may fold into `validate()`) |
| `SecurityPostureLifecycle` via `extra_lifespan_components` (`posture.py:263-266`) | opt-in lifecycle | `SecurityPostureModule()` — **default-on (FLIP-1)** |

### Name collision that must be resolved first

Three providify `@Configuration` grouping classes are already called `*Module`: `SAModule`
(`varco_sa/varco_sa/di.py:80-81`), `BeanieModule` (`varco_beanie/varco_beanie/di.py:71`),
`VarcoFastAPIModule` (`varco_fastapi/varco_fastapi/di.py:259`). Ten other providify modules in the
tree are named `*Configuration` (`RedisCacheConfiguration`, `KafkaDLQConfiguration`,
`OtelConfiguration`, `RedisEventBusSelectorConfiguration`, …). §D-names renames the three.

## Design decisions

### §D-shape — Open Question 1 answered: a varco ABC, never a `@Configuration`

**Chosen: `varco_core.di.VarcoModule` is a plain abstract class. Instances (not classes) are
passed to `create_varco_app(modules=[…])`. `depends_on` is a varco `ClassVar` ordered by a varco
topological sort with the same post-order semantics as `providify.modules.resolve_install_order`.
A module that needs a providify `@Configuration` (`RedisCacheConfiguration`) *owns* it by calling
`container.ainstall(...)` from `async_init()`. No `VarcoModule` subclass is ever decorated with
`@Configuration`, `@Singleton`, `@Component` or `@Provider` — enforced by a test (§D-scan).**

```python
# varco_core/varco_core/di/module.py
class VarcoModule(ABC):
    name: ClassVar[str]                                   # "core", "security_posture", … — unique, greppable
    depends_on: ClassVar[tuple[type[VarcoModule], ...]] = ()

    def __init__(self, *, enabled: bool | None = None) -> None: ...   # 047 tri-state (§D-enabled)

    # ── slots (all sync-or-async as marked; every default is a no-op) ──
    def register(self, container: DIContainer) -> None: ...          # template: idempotent via ModuleRegistry (§D-registry)
    def _register(self, container: DIContainer) -> None: ...         # ABSTRACT — the package's bootstrap() + own bindings
    async def async_init(self, container: DIContainer) -> None: ...  # ainstall()/awaitable setup; before any start()
    def validate(self, container: DIContainer) -> Sequence[str]: ... # 052 sequences; default ()
    async def start(self, container: DIContainer) -> None: ...
    async def stop(self, container: DIContainer) -> None: ...

    def is_enabled(self, container: DIContainer) -> bool: ...        # tri-state resolution (§D-enabled)
    def _enabled_from_settings(self, container: DIContainer) -> bool: return True   # hook
```

| Candidate | ✅ | ❌ |
|---|---|---|
| **(A) `VarcoModule` = a `@Configuration` subclass with `depends_on=`** (OQ1's first option) | Reuses `resolve_install_order` (`modules.py:52-141`), `ModuleCycleError`, module `@PreDestroy` reverse order (`container.py:5380-5402`), `@Disposes` scoping | **Scan activates it**: `scanner.py:161-162,282` installs every public `@Configuration` in a scanned package — `container.scan("varco_core", recursive=True)` is the documented pattern and *is* `varco_core.di.bootstrap()` (046); hiding modules behind `_`-prefixed names + re-exports (`scanner.py:144,148`) is a trick the T3 decision (`BACKLOG.md:249-252`) forbids relying on. **Container-constructed only** (`container.py:7221`): `MigrationsModule(alembic_migrator)`, `RetentionModule(registry)`, `RateLimitModule(bundle)`, `TenancyModule(pool=…)`, `JwksRefreshModule(registry)` all take user-built objects — they would have to be bound into the container first, i.e. the `bind_<hand-built-object>` shape 046 just collapsed. **Marker not inherited** (`metadata.py:539-540`): every subclass must be decorated anyway, so "subclass" buys nothing. **Wrong lifecycle moments**: `@PostConstruct` runs at install (sync from scan), `@PreDestroy` after every singleton is dead — neither is `start()`/`stop()`; 050's phases would have to fight providify's vocabulary. Rejected |
| **(B) chosen: a varco ABC; `@Configuration` owned via `async_init()` where a package has one** | Scan-proof by construction (a plain class carries no providify marker, `scanner.py:151-162` never sees it); user-constructed with arguments (NestJS `forRoot(options)`, brief 015 §1); `start()`/`stop()` are varco words that 050 sequences without touching providify; `@Disposes` stays exactly where 045/046 left it (the provider-carrying `@Configuration`s, which remain scanned/`ainstall`ed); a module is testable with a bare `DIContainer()` and no event loop for `register()` (046's "nothing in any `bootstrap()` needs an event loop") | A second ~40-line topological sort (§D-order) mirroring `resolve_install_order` — cannot be reused because it raises `TypeError` for any non-`@Configuration` node (`modules.py:120-125`); `depends_on` is a varco attribute a reader might confuse with providify's — the name is kept **on purpose** so the concept maps 1:1, and the docstring says which one it is |
| (C) a varco ABC whose *class* is also `@Configuration` but defined in a `di.py` the scanner is told to skip | Reuses providify ordering | providify's scanner has no exclude list (`scanner.py:129-162` — only the `_`-name and re-export rules); "told to skip" means "named `_X`", i.e. (A)'s trick. Rejected |

### §D-registry — `ModuleRegistry`: idempotent `register()`, and the thing 052 reads

`VarcoModule.register(container)` is a template method: it fetches (or, on the first call,
`bind_instance`s — 046) a `ModuleRegistry` singleton on the container; if `type(self)` is already
recorded it returns; else it calls `self._register(container)` and records `(cls → instance,
enabled=is_enabled(container))`. ✅ Idempotency by construction — independent of whether every
`bootstrap()`/`bind_settings()` underneath is idempotent (046/048 say they are; ⚠️ ASSUMPTION for
048's `bind_settings` fallback path, measured in Step 1 — either way the template holds). ✅ No
process-global state (a `WeakKeyDictionary` keyed by container was the alternative; rejected —
`bind_instance` is the sanctioned "already-constructed object" verb and the registry is then
visible to `container.describe()`). ✅ `ModuleRegistry.modules` (ordered, frozen view) is what
052's `varco diagnose` prints under "Modules: name · enabled · package"; 049 gives it no CLI.
❌ One non-fallback binding of a varco-owned type in every app container — acceptable, it is the
composition root's own record. A second **different instance** of the same module class →
`ValueError` naming the class (§D-signature's duplicate rule, one place).

### §D-enabled — the 047 tri-state, one kwarg on the base class

047 §D-features last paragraph, moved here verbatim in effect: `enabled=None` → resolve the
feature's settings from the container (048) and honour its `.enabled`/mode via
`_enabled_from_settings()`; `enabled=False` → off regardless of env (code > env);
`enabled=True` → on regardless of env; a **settings instance** passed to a settings-shaped module
(`SecurityHeadersModule(settings=SecurityHeadersSettings(preset=STRICT))`) is bound via
`bind_settings(container, Cls, instance=…)` in `_register()` (048's code path, non-fallback) and
then `enabled=None` reads *that* instance's `.enabled` — so "instance ⇒ on" holds exactly when the
instance says so, which is 048's per-field rule rather than a third mechanism.

**`register()` always runs, even for a disabled module.** It binds settings and `@Fallback`
providers, which is what makes a disabled module still *visible* (to 052 as "disabled", to
`validate()` as bound-but-inert). Every other slot — `async_init`, `validate`, `middleware`,
`configure_app`, `start`, `stop` — runs only when `is_enabled(container)` was `True` at
registration time (recorded in the registry; a later env change does not flip it — same
not-evicted caveat as 045). For a **binding-shaped** feature (`CasbinModule(policy_authorizer=True)`)
`_register()` may consult the constructor tri-state to call `enable_*` (code path); it never reads
env itself — the env path is 047's `@Requires` twin, already registered by the package
`bootstrap()`. For a backend module with no `.enabled` settings (`SAModule`, `KafkaModule`, …)
`enabled=None` resolves `True`; `enabled=False` skips only the lifecycle slots — backend
*selection* is 047's `VARCO_<SEAM>_BACKEND`, and the docstring says so.

✅ One kwarg, one rule, three states — 047's table row becomes the base-class docstring.
❌ `enabled` on a backend module is nearly meaningless. Accepted over a two-class hierarchy
(`FeatureModule(VarcoModule)`) — minimal abstraction; the wart is documented, not modelled.

### §D-order — what 049 orders, what it inherits, what it leaves to 050

**`resolve_module_order(modules: Sequence[VarcoModule]) -> list[VarcoModule]`**
(`varco_core/di/module.py`): DFS post-order over `type(m).depends_on`, visiting roots in **list
order**, deps first in declaration order, each module once — the same shape as
`providify/modules.py:109-136` so a reader who knows one knows both. Errors, all raised before
anything is registered: `ModuleCycleError(cycle)` (a varco exception; providify's is not reused
because it is raised only from providify's own walk); `ModuleDependencyError` when a `depends_on`
class has **no instance in the list** — explicit over implicit (brief 015 §Librarian #1): the
message names the missing class and suggests `default_modules()`; `ValueError` on two instances
of one class.

What the resolved order **is used for** in 049, in this sequence inside `create_varco_app`:

```
for m in order:  m.register(container)                      # sync, at create_varco_app() time
active = [m for m in order if registry.enabled(m)]
_warn_unowned_settings(container, active)                   # the two kept warnings
specs  = [spec for m in active if HttpContributor ∋ m for spec in m.middleware(container)]
install_middleware_stack(app, sorted-by-slot(specs))        # §D-http — order is the slot table, never the module order
for m in active:  m.configure_app(app, container)           # mounts (metrics router, posture captures app)
VarcoLifespan(*[_ModuleComponent(m, container) for m in active],
              setup=_async_init_all(active, container),     # awaits m.async_init() in order, before any start()
              shutdown=container.ashutdown)
```

- **Start order** = resolved module order; **stop order** = exact reverse of *started* — inherited
  from `VarcoLifespan` (`lifespan.py:239-260`: "Start in registration order … Stop in reverse order
  (LIFO)"). 049 adds no second orchestrator; `_ModuleComponent` is a 12-line adapter binding
  `container` so `AbstractLifecycle`'s `start()`/`stop()` shape is met (`lifespan.py:72-90`).
- **Unconstrained modules keep list order** (stable sort). This is what makes the default set's
  documented order hold: `default_modules()` lists `SecurityPostureModule` last and an app puts
  `MigrationsModule`/`TenancyModule` first. ⚠️ **This is the registration-order model 014
  §Librarian #3 calls fragile, and it is deliberately temporary**: 050 ships phases. Until then
  `create_varco_app` logs a WARNING if a `MigrationsModule`/`TenancyModule` is present but resolves
  *after* `CoreModule` ("bus/job runner will start before migrations ran — list it first; Plan 050
  replaces this with phases"). Test-pinned.
- **Not promised by 049**: any ordering *inside* a module's `start()` (bus-before-consumer is
  `CoreModule`'s own body today, as it was `_collect_lifecycle_components`'s), timeouts per phase,
  "stop consumers before the bus" across two modules, `@Disposes` reachability. 050.

Rejected — **defer even topological order to 050, ship list order only**: ✅ smaller; ❌ every
backend module already *needs* `CoreModule` first (its `bootstrap()` scans `varco_core`
subpackages, 045 E2) and `depends_on` is the one word DI-1 names; shipping the attribute with no
consumer would be a lie. Rejected — **phases now**: 050's row, with its own research (014 §Librarian
#3) and the DI-10 infra it must sequence.

### §D-http — the HTTP contribution is a separate Protocol in `varco_fastapi`, not on the core ABC

`varco_fastapi.modules.base.HttpContributor` (`@runtime_checkable Protocol`):
`middleware(container) -> Sequence[MiddlewareSpec]` and `configure_app(app, container) -> None`.
`MiddlewareSpec(slot: EdgeSlot, target: type | Callable[..., None], kwargs: Mapping[str, Any])`
(frozen) lives in `varco_fastapi/middleware/slots.py` beside **`EdgeSlot`, an `IntEnum` that *is*
the normative §D-order table** (`varco_fastapi/middleware/__init__.py:14-37`), outermost first:

```
CORS=10  SECURITY_HEADERS=20  EXTRA=30  ERROR=40  BODY_LIMIT=50  LOGGING=60  TRACING=70  METRICS=80
RATE_LIMIT_PRE_AUTH=90  REQUEST_CONTEXT=100  RATE_LIMIT_POST_AUTH=110  LOCALIZATION=120
IDEMPOTENCY=130  PROFILING=140
```

`create_varco_app` sorts every collected spec by `slot` (stable — two modules in one slot keep
module order) and calls `install_middleware_stack(app, …)` **once**, outermost-first. The
callable-installer form (`install_cors`) is already a `MiddlewareEntry` shape (`middleware/__init__.py:95-103`).

✅ The core ABC stays transport-free — a worker host can run `[CoreModule(), KafkaModule(),
RetentionModule(r)]` with no FastAPI import (a stated non-goal to *build*, a design goal to
*allow*). ✅ A module cannot invent a position — it names a slot, and the slot table is the same
one `test_middleware_order.py` characterises, so the byte-identical-order proof is that test
staying green with only its construction call migrated. ✅ `RateLimitModule` yields two specs
(PRE/POST) — the "one middleware, two positions" design (035) survives untouched. ✅
`IdempotencyMiddleware` gets a slot too, so an app's `MiddlewareModule([IdempotencyMiddleware…],
slot=EdgeSlot.IDEMPOTENCY)` finally lands *inside* `ErrorMiddleware` — the position CLAUDE.md's
Idempotency section demands and `extra_middleware=` could never give. ❌ Two `isinstance` checks
in `create_varco_app`. Rejected — `middleware()`/`configure_app()` on the core ABC returning
`Sequence[Any]`: ❌ `varco_core` would grow HTTP vocabulary it has no consumer for.

### §D-signature — `create_varco_app(container, *, modules=None, routers=None, …)`

```python
def create_varco_app(
    container: DIContainer,                       # required — modules register into it (046's rule, extended)
    *,
    modules: Sequence[VarcoModule] | None = None,  # None → default_modules(); a list → exactly those
    routers: list[type] | None = None, scan_packages: list[str] | None = None,
    title=..., version=..., description=..., openapi_url=..., docs_url=...,
    validate: bool = True, strict_validation: bool = False,
    mcp_adapter=None, mcp_path="/mcp", skill_adapter=None, skill_base_url="", skill_agent_card_path=..., skill_tasks_prefix=...,
) -> FastAPI
```

| Decision | ✅ | ❌ |
|---|---|---|
| **`container` required** | Modules must register somewhere; 046 made every varco verb container-required for the same reason (two implicit containers = silently wrong wiring). A container-free app is `create_varco_app(DIContainer(), routers=[X])` — one more token | Every container-less call site breaks: 3 examples (`03`, `23`×2, `24`), ~40 test sites (`milestone_f/test_app_factory.py`, `test_custom_route_params.py`, …). Accepted: 4.0 window; §Migration row |
| **`modules=None` → `default_modules()`; explicit list → exactly those** | NestJS `imports`/Django `INSTALLED_APPS` semantics (brief 015 §1): what you list is what you get; a missing `depends_on` fails loudly with a message that names `default_modules()`; no hidden kernel insertion that would make "exactly those" false | `modules=[SAModule()]` gives an app with no error envelope, no CORS, no posture. Correct — it says so; `[*default_modules(), SAModule()]` is the idiom and the README's first snippet |
| **`default_modules(*, exclude=()) -> list[VarcoModule]` returns fresh instances** | A module holds state (`SecurityPostureModule.report`); a module-level tuple of instances shared by two apps in one process (every test file) would be a bug | A function, not a constant |
| **What stays a kwarg**: `routers`/`scan_packages` (the app's own routing surface), `title`/`version`/`description`/`openapi_url`/`docs_url` (the `FastAPI(...)` constructor), `validate`/`strict_validation` (`create_varco_app`'s *own* build-time router/container checks, `app.py:341-367` — not a subsystem, not lifecycle; 052 owns the runtime `validate()`), `mcp_*`/`skill_*` (mounts of the app's subject, kept beside `routers`) | One rule: *a kwarg configures the FastAPI object or names what the app exposes; everything that binds, starts, stops, or wraps a request is a module* | Six mount kwargs remain — they were never in DI-1's eleven |
| **Two modules of one class → `ValueError`** (`modules=[*default_modules(), SecurityPostureModule(enforce…)]`) | The alternative "last wins" hides a copy-paste; `default_modules(exclude=(SecurityPostureModule,))` is the explicit path | — |

### §D-core — `CoreModule` absorbs lifecycle discovery; `VARCO_LIFECYCLE_DISCOVERY_WARN` goes

`CoreModule._register()` = `varco_core.di.bootstrap(container)` (046) + `bind_core_settings(container)`
(048 — ⚠️ ASSUMPTION `varco_fastapi.di.bootstrap()` no longer calls it itself, or does so
idempotently; Step 1 measures; if it does, `FastAPIModule` simply does not repeat it).
`CoreModule.start(container)`: `if container.is_resolvable(AbstractEventBus): await (await
container.aget(AbstractEventBus)).start()`; same for `AbstractJobRunner`; `stop()` the reverse —
byte-for-byte what `_collect_lifecycle_components` + `VarcoLifespan` did for the two core seams
(`app.py:902-903`), minus the discovery logging: with explicit modules there is no "did you forget
`bootstrap()`?" to warn about — the module *is* the bootstrap, so the WARNING/DEBUG tiering, the
kill switch and its 048 allowlist row are deleted. `WsModule.start()` does the same for
`WebSocketEventBus`/`SSEEventBus` (`app.py:907-916`). ✅ Zero hidden `container.scan()` calls
inside `create_varco_app` (`_try_resolve_component` scanned `varco_core.event.base` etc. on every
build, `app.py:997`). ❌ A bus bound by a backend module is started by `CoreModule` — "the seam's
owner starts it", which 050 may re-home into a phase; recorded as the handoff.

### §D-names — three providify `@Configuration`s renamed `*Configuration`

`SAModule` → `SAConfiguration`, `BeanieModule` → `BeanieConfiguration`, `VarcoFastAPIModule` →
`VarcoFastAPIConfiguration`. Rule, stated once in the guide: **`*Configuration` = a providify
`@Configuration` grouping `@Provider` methods, scan-installed, owned by its package's
`bootstrap()`; `*Module` = a `VarcoModule`, constructed by the app, never scanned.** ✅ Matches the
ten existing `*Configuration` names; ✅ the new `SAModule`/`BeanieModule`/`FastAPIModule` names
are DI-1's "one per package" names with no qualifier; ❌ three breaking renames (`api_surface`
regenerated in the same step; §Migration rows). No alias: `BACKLOG.md:53` allows wiring breaks
outright.

### §D-adapters — `LifecycleModule` and `MiddlewareModule` replace the two `extra_*` kwargs

`LifecycleModule(component: AbstractLifecycle, *, name: str | None = None, depends_on=())` wraps
any `start()`/`stop()` object (the `extra_lifespan_components` shape, `app.py:373-374`) — `name`
defaults to `type(component).__name__`. `MiddlewareModule(entries: Sequence[MiddlewareEntry],
slot: EdgeSlot = EdgeSlot.EXTRA)` wraps the `extra_middleware` shape at the **same** position it
had (outside `ErrorMiddleware`, `app.py:686-692`; the §D-order-bugs note stays true) unless the app
names a better slot. ✅ No user is forced to subclass `VarcoModule` to keep today's escape hatches;
✅ the README's first "write your own module" example is the *example app's* `AppModule`, not an
adapter (§D-example). Per-instance `depends_on` on `LifecycleModule` is an `__init__` kwarg that
sets an instance attribute shadowing the `ClassVar` — `resolve_module_order` reads
`getattr(m, "depends_on")`, so both shapes work; documented.

### §D-posture — FLIP-1 concretely

`SecurityPostureModule(settings: SecurityPostureSettings | None = None, *, enabled=None,
collectors: Sequence[Callable[[], list[PostureFinding]]] | None = None)`, `name="security_posture"`,
`depends_on=(FastAPIModule,)`, **in `default_modules()`, listed last.**

- `_register()`: `bind_settings(container, SecurityPostureSettings, instance=settings)` when
  given (048 already binds the fallback in `varco_fastapi.di.bootstrap()` — ⚠️ ASSUMPTION per 048
  Step 579; Step 1 verifies). `_enabled_from_settings()` → `container.get(SecurityPostureSettings).enabled`.
- **New field** `SecurityPostureSettings.enabled: bool = True`, env **`VARCO_SECURITY_POSTURE_ENABLED`**
  (the name `BACKLOG.md:81`/036 §D-S9-flip #1 hand us). ⚠️ The class's prefix after 048 is either
  still `VARCO_SECURITY_` (then the field carries an explicit `validation_alias=AliasChoices(
  "VARCO_SECURITY_POSTURE_ENABLED")`, the same shape `environment` uses at `posture.py:187-190`)
  or was renamed to `VARCO_SECURITY_POSTURE_` (then it is a plain field). Step 1 reads the
  class; either way the env name is `VARCO_SECURITY_POSTURE_ENABLED` and 048's
  `settings_reference.py --check` snapshot is regenerated in the same step.
- `configure_app(app, container)`: captures `app` (for `_collect_http(app=app)` —
  `posture.py:585`).
- `start(container)`: builds `SecurityPostureLifecycle(collectors=self._collectors or
  _default_collectors(app, container), settings=container.get(SecurityPostureSettings))` and
  awaits its `start()`; `stop()` its `stop()`; `.report` is exposed as `SecurityPostureModule.report`
  (the `RuntimeError` on `enforce="refuse"`, `posture.py:311-316`, propagates and aborts startup
  exactly as before — `lifespan.py:245-254`). `SecurityPostureLifecycle` itself is **unchanged**
  and still usable by hand (`LifecycleModule(SecurityPostureLifecycle(...))`).
- **`_default_collectors(app, container)`** — the current five, wired from what a composition
  root has on hand and never from a static list of bindings: `partial(_collect_http, app=app)`;
  `partial(_collect_local, container=container)`; `partial(_collect_auth, auth=<AbstractServerAuth>,
  registry=<TrustedIssuerRegistry>, store=<AbstractTokenRevocationStore>)` each resolved **only
  if `container.is_resolvable(...)`**, else `None` (the collector degrades per §D-S9-degrade);
  `partial(_collect_tenant, chain=<TenantSourceChain>, membership=<AbstractTenantMembership>,
  delegation=<…>)` same rule; `partial(_collect_data, rls_posture=None)` (unchanged —
  `data.rls_not_checked` at `NOT_ASSESSED`, `security-posture.md:67-70`; a live RLS check needs
  an `AsyncConnection`, 050/056's call). 057's redaction collector is added to this list in the
  same shape (`inspect_redaction_posture` → `partial(_collect_redaction, …)`) — ⚠️ Step 1 reads
  what 057 shipped and where. **051 replaces this function with a `Multibound` collector set;
  the function is named `_default_collectors` and documented as the seam 051 removes.**
- The `posture.base_authorizer_bound` remediation text at `posture.py:766-767` ("priority higher
  than `-(2**31)`") is stale since 045 — rewritten to "register your own `AbstractAuthorizer`
  binding; varco's default is `@Fallback`" while the file is open.

✅ Every app gets the preflight with zero code (FLIP-1's intent) and can silence it with one env
var or `exclude=(SecurityPostureModule,)`; ✅ `enforce` stays `"warn"` so a default 4.0 app cannot
*fail* to start because of this plan (054 flips it); ❌ one more `start()` in every default app —
measured in Step 1 as sub-millisecond (five sync collectors over already-built objects,
`posture.py:23-25`).

### §D-logging — RO-4: `LoggingModule(redactor=…)`

`LoggingModule(redactor: Redactor | None = None, *, enabled=None)`, `name="logging"`, default-on,
`middleware()` → `MiddlewareSpec(EdgeSlot.LOGGING, RequestLoggingMiddleware, {"redactor": redactor})`.
`redactor=None` means **no redaction** — 040 §D-S21-logging's "does not fall back to
`default_redactor()`" (hot-path cost for zero benefit) stands; 057 owns any flip. `enabled=False`
≡ today's `enable_logging=False`. ✅ The keyword 040 refused to add to `app.py` lands as one
constructor argument on the module that owns the middleware, which is exactly the "folds into
DI-1's module shape" the row predicted. A `RedactionModule` was considered and rejected: ❌ redaction
is a *setting of four consumers* (spans, `error_params()`, audit, logging), not a subsystem with a
lifecycle; a module that only holds a `Redactor` would be a bind-object wrapper — `bind_instance(
container, Redactor, …)` already exists for that (046).

### §D-scan — the module-vs-scan rule, enforced

`varco_core/tests/test_modules_never_scanned.py`: (1) a fresh `DIContainer()` after
`varco_core.di.bootstrap(container)` has **no** binding whose implementation is a `VarcoModule`
subclass and `container.is_resolvable(ModuleRegistry)` is `False` (public reads only:
`describe().bindings` + `is_resolvable`); (2) after importing every package's module file (the ten
`<pkg>.modules`/`varco_core.di.module` + `varco_fastapi.modules.*`), for every class in
`VarcoModule.__subclasses__()` (transitively) `cls.__dict__` carries none of providify's markers
(`_DI_CONFIGURATION_ATTR`, and the `@Singleton`/`@Component` metadata attribute — read via the
same `providify.metadata` helpers 045's fallback tests already import) and no `@Provider`-marked
callable in `vars(cls)`; (3) the same for `container.scan("varco_fastapi", recursive=True)` in
`varco_fastapi/tests/test_modules_never_scanned.py`. This is the T3 decision made mechanical.

### §D-example — the example app migrates to `create_varco_app(modules=…)` and shows the app-module idiom

`examples/00-full-stack-post-api/example/app.py` today builds `FastAPI` + `VarcoLifespan` by hand
(`app.py:237-378`) — it never calls `create_varco_app`. Its `_bootstrap` does five things: DDL,
Redis cache install, `registry.load_all()`, resolve+register lifecycle singletons, build+include
routers. After 049 it is:

```python
class PostAppModule(VarcoModule):                 # examples/00-full-stack-post-api/example/module.py
    name = "post_app"; depends_on = (CoreModule, SAModule, RedisModule, WsModule)
    def _register(self, c): c.scan("example", recursive=True); c.bind(PostService, PostService); …
    async def async_init(self, c): await create_tables(c); await self._registry.load_all()
    async def start(self, c): consumer = await c.aget(PostEventConsumer); await consumer.start()   # register_to(bus) — bus started by CoreModule
    async def stop(self, c): …

app = create_varco_app(container, modules=[MigrationsModule(...)?, *default_modules(), SAModule(...),
                       RedisModule(cache=True), WsModule(), PostAppModule(registry, authority)],
                       routers=[...])
```

Routers: `PostRouter(service=…, auth=server_auth)` is hand-constructed today because it needs
`await container.aget(PostService)`; `create_varco_app`'s `_mount_router` builds routers from the
container synchronously (`app.py:1193`). The step migrates what it can (`AuthRouter`,
`StreamsRouter` are `@Singleton`/plain) and keeps `PostRouter` included from `PostAppModule.
configure_app()` if `_mount_router` cannot construct it — ⚠️ Step 19 decides after reading
`_mount_router` (`app.py:1193-1241`). `make asyncapi-check` is unaffected by construction: the
exporter imports `example.consumer:PostEventConsumer` and registers it to a throwaway bus
(`varco_core/cli/asyncapi.py:244-276`) — the consumer class does not move.

## Steps

All commands from `/home/edoardo/projects/varco`. Steps 1-5 first, in order; 6-10 are independent
of each other; 11-13 need 6-10; 14-15 need 11; 16-17 after all modules exist; 18-22 last. `make
test` is expected **red** from Step 11 until Step 18 lands — Steps 11-18 are one commit.

### Preflight and the core primitive

1. [ ] **Preflight — measure every ⚠️ ASSUMPTION before writing code.** Record in
       `/tmp/claude-1000/…/scratchpad/049-preflight.txt`: (a) `ls varco_core/varco_core/di*` — is
       `varco_core.di` a package (047's `features.py`) or still `di.py`? (module file goes to
       `varco_core/varco_core/di/module.py`; if flat, convert to a package first, keeping
       `varco_core/tests/test_di_constants.py`'s "no providify in `sys.modules`" property by
       importing providify only inside function bodies in `__init__.py`). (b) `rg -n
       'bind_core_settings' varco_*/varco_*` — who calls it today; whether a second call is
       idempotent (`bind_settings(c, Cls)` twice → binding count unchanged?). (c) `rg -n 'class
       SecurityPostureSettings' -A 12 varco_fastapi/varco_fastapi/posture.py` — the env prefix 048
       left; `rg -n 'SecurityPostureSettings' varco_fastapi/varco_fastapi/di.py` — is it
       `bind_settings`-bound by `varco_fastapi.di.bootstrap()`? (d) `rg -n 'redaction' varco_fastapi/varco_fastapi/posture.py`
       — what 057 added and how it is called. (e) `uv run pytest varco_fastapi/tests/test_middleware_order.py -q`
       — green, and read its assertions: the slot table in §D-http must reproduce them exactly.
       (f) `rg -n 'create_varco_app\(' varco_*/tests examples --count` and `rg -n
       'create_varco_app\(\s*$|create_varco_app\(\s*routers=|create_varco_app\(\s*title=|create_varco_app\(\s*enable' varco_*/tests examples`
       — the container-less sites Step 18/19 migrate (expected ~40 + 4). (g) Time
       `SecurityPostureLifecycle.start()` with the five default collectors on a default container
       (§D-posture ❌). (h) Confirm every providify citation in §Context against the installed
       2.1.0 (`.venv/lib/python3.12/site-packages/providify/{scanner,container,modules,metadata}.py`,
       `decorator/module.py`). **Verify**: the file lists (a)-(h) with outcomes; any citation that
       moved is corrected in this plan before Step 2.

2. [ ] `varco_core/tests/test_di_module.py` (**new, failing first**) — the `VarcoModule` contract
       from §D-shape/§D-registry/§D-enabled/§D-order, all Docker-free, no FastAPI import:
       - a minimal `class _M(VarcoModule): name="m"; def _register(self, c): c.bind(_Tok, _Tok)`;
         `register(c)` twice → `len(c.describe().bindings)` unchanged after the second call and
         `c.get(ModuleRegistry).modules == (_M-instance,)`; `is_resolvable(ModuleRegistry)` is
         `True` only after the first `register()`.
       - tri-state: `_M()` → `is_enabled` `True`; `_M(enabled=False)` → `False` and still
         registered (the `_Tok` binding exists); `_M(enabled=True)` with `_enabled_from_settings`
         returning `False` → `True` (code beats settings); `enabled=None` → the hook's answer.
       - `resolve_module_order`: unconstrained modules keep list order; `B.depends_on=(A,)`
         listed `[B, A]` → `[A, B]`; diamond `D→(B,C)`, `B→A`, `C→A` → `A` once, `B` before `C`;
         `A→B→A` → `ModuleCycleError` whose `.cycle` names both; `B.depends_on=(A,)` with no `A`
         instance → `ModuleDependencyError` naming `A`; two instances of `_M` → `ValueError`;
         an instance-level `depends_on` (the `LifecycleModule` shape) is honoured.
       - the base class exposes exactly the six slots + `is_enabled` (a `dir()`-based guard so a
         seventh slot is a deliberate diff); `register` is not overridable without going through
         `_register` (asserted via a subclass overriding `register` → the registry still records it,
         i.e. the template calls `super()` — or simpler: `register` is marked `@final` and mypy
         enforces it; choose `@final`).
       - `import varco_core.di.module` adds no `providify` **or** `fastapi` to `sys.modules`
         beyond what `varco_core.di` already imports (the 045/046 import-budget property).
       **Verify**: `uv run pytest varco_core/tests/test_di_module.py -q` → red, `ImportError`.

3. [ ] `varco_core/varco_core/di/module.py` (**new**) — `VarcoModule` (§D-shape, `@final
       register`, template calling `_register`, providify imported inside function bodies),
       `ModuleRegistry` (frozen-view `modules` property; `enabled(module) -> bool`; `record()`),
       `resolve_module_order`, `ModuleCycleError(RuntimeError)` with `.cycle`,
       `ModuleDependencyError(LookupError)`, `CoreModule` (§D-core — `name="core"`,
       `_register` = `bootstrap(container)` + `bind_core_settings(container)` per Step 1(b);
       `start`/`stop` for `AbstractEventBus`/`AbstractJobRunner`), `ObservabilityModule`
       (`name="observability"`, `global_attributes`/`capture_params` → the two `set_*` calls from
       `app.py:332-339` in `_register()`), `LifecycleModule` (§D-adapters; **core-side** because it
       has no HTTP shape). Module docstring = §D-shape's table condensed + the ✅/❌ block + the
       "`depends_on` here is varco's, mirrors `providify.modules.resolve_install_order`" note.
       Every slot docstring carries `Args/Returns/Raises/Edge cases/Async safety`. `varco_core/di/__init__.py`
       re-exports `VarcoModule, ModuleRegistry, CoreModule, ObservabilityModule, LifecycleModule,
       resolve_module_order, ModuleCycleError, ModuleDependencyError` (lazy, PEP 562, to keep the
       `sys.modules` property). **Verify**: Step 2 green; `uv run python scripts/import_budget.py
       --check --warn-only` unchanged.

4. [ ] `varco_fastapi/tests/test_middleware_slots.py` (**new, failing first**) — `EdgeSlot` values
       are strictly increasing in the §D-order table's order and the enum's member list, read in
       order, equals the outermost-first sequence in `varco_fastapi/middleware/__init__.py:18-37`
       (assert against a literal tuple of names so the docstring and the enum cannot drift
       silently); `MiddlewareSpec` is frozen and hashable; `sort_specs([...])` is stable within a
       slot; `HttpContributor` is `runtime_checkable` and a `VarcoModule` without the two methods
       is not one.

5. [ ] `varco_fastapi/varco_fastapi/middleware/slots.py` (**new**) — `EdgeSlot`, `MiddlewareSpec`,
       `sort_specs`; `varco_fastapi/varco_fastapi/modules/__init__.py` + `modules/base.py` (**new
       package**) — `HttpContributor` Protocol; `MiddlewareModule` (§D-adapters). `middleware/__init__.py`'s
       docstring gains one sentence: "`EdgeSlot` (`middleware/slots.py`) is this table as code;
       `test_middleware_slots.py` pins the two together." **Verify**: Step 4 green.

### The modules (each: failing test first, then the class; all Docker-free)

6. [ ] **Edge modules** — `varco_fastapi/varco_fastapi/modules/edge.py`: `CorsModule(config=None)`
       (absorbs `_resolve_cors`, `app.py:1175`), `SecurityHeadersModule(settings=None, *, enabled=None)`,
       `BodyLimitModule(settings=None, *, enabled=None)`, `ErrorEnvelopeModule()` (resolves
       `I18nSettings` + `MessageCatalog` from the container for `message_catalog=`/
       `set_content_language=` exactly as `app.py:453-471`, `:670-676`; also owns
       `add_exception_handlers()` in `configure_app` — `app.py:529-533`), `TracingModule()`,
       `LoggingModule(redactor=None)` (§D-logging), `MetricsModule()` (middleware + `_mount_metrics_router`
       in `configure_app`, `app.py:1263`), `ProfilingModule()` (absorbs `_try_add_profiling_middleware`,
       `app.py:1149`), `RateLimitModule(bundle)` (absorbs `_partition_rate_limit_rules`, `app.py:1090`;
       two specs; `has_error_middleware=` is read from the registry: `ErrorEnvelopeModule` present
       and enabled), `LocalizationModule(i18n=None, timezone=None)` (one `LocalizationMiddleware`
       when either is enabled, `app.py:555-562`; `I18nLifecycle(catalog)` started from `start()`
       when i18n is enabled and a `MessageCatalog` resolves, `app.py:461-471`), `FastAPIModule(*,
       configure_jwt=True)` (`_register` = `varco_fastapi.di.bootstrap(container)` +
       `configure_jwt_from_env()`; `middleware()` = `RequestContextMiddleware` at `REQUEST_CONTEXT`
       iff `AbstractServerAuth` is resolvable, `app.py:1120-1137`; `depends_on=(CoreModule,)`).
       Tests `varco_fastapi/tests/test_edge_modules.py`: each module's `middleware()` returns the
       expected `(slot, cls)` pairs for `enabled` `None/True/False` and for a passed settings
       instance (the instance is what `container.get(Cls)` returns afterwards — 048's code path);
       `RateLimitModule` yields PRE and POST specs partitioned by scope; `LocalizationModule` with
       both disabled yields nothing.

7. [ ] **Subsystem lifecycle modules** — `varco_fastapi/varco_fastapi/modules/lifecycle.py`:
       `MigrationsModule(*migrators, settings=None)` (`_enabled_from_settings` → `mode != "off"`;
       wraps `MigrationLifecycle`), `TenancyModule(*, pool, supervisor=None, catalog_subscription=None)`
       (wraps `TenancyLifecycle`), `ReliabilityModule(preset)` (wraps `ReliabilityLifecycle(preset,
       container=…)` — built in `start()` so the container is the lifespan's), `RetentionModule(
       registry, *, interval=0.0)`, `JwksRefreshModule(registry, *, interval=None)`,
       `SecurityPostureModule` (§D-posture, incl. the `SecurityPostureSettings.enabled` field and
       the stale remediation text fix). The seven `*Lifecycle` classes are **unchanged** — a module
       delegates. Tests `varco_fastapi/tests/test_lifecycle_modules.py`: each `start()`/`stop()`
       delegates to a `MagicMock(spec=…Lifecycle)`; `MigrationsModule` with `VARCO_MIGRATE_MODE`
       unset is disabled and with `settings=MigrationSettings(mode="check")` enabled;
       `SecurityPostureModule()` default `is_enabled` `True`, `monkeypatch.setenv(
       "VARCO_SECURITY_POSTURE_ENABLED", "false")` → `False`, `enabled=True` beats the env; after
       `start()` on a default container `module.report` has one finding per default collector
       family (the existing `test_security_posture*.py` shape); a `collectors=` list replaces the
       default set entirely.

8. [ ] **Backend modules, one file per package** — `varco_sa/varco_sa/modules.py::SAModule(config:
       SAConfig | None = None, *, entities: Sequence[type[DomainModel]] = ())` (`_register`:
       `bind_instance(SAConfig, config)` when given; `varco_sa.di.bootstrap`; `bind_repositories(
       container, *entities)`), `varco_beanie/varco_beanie/modules.py::BeanieModule(settings:
       BeanieSettings | None = None, *, entities=())`, `varco_redis/varco_redis/modules.py::RedisModule(*,
       cache: bool = False)` (`async_init`: `await varco_redis.di.async_bootstrap(container)` when
       `cache`; `_register`: `bootstrap`), `varco_memcached/varco_memcached/modules.py::MemcachedModule()`
       (same shape, `async_bootstrap`), `varco_kafka/varco_kafka/modules.py::KafkaModule()`,
       `varco_nats/varco_nats/modules.py::NatsModule()`, `varco_casbin/varco_casbin/modules.py::
       CasbinModule(*, policy_authorizer: bool | None = None)` (`True` → `enable_policy_authorizer`;
       `None` → 047's env twin; `False` → nothing), `varco_ws/varco_ws/modules.py::WsModule()`
       (`start`/`stop` for the two push adapters, §D-core). Every one: `depends_on=(CoreModule,)`,
       `name` = the package suffix, `_register` = the package's 046 `bootstrap(container)` first.
       Tests: per package `tests/test_<pkg>_module.py` — `register()` on `DIContainer()` +
       `CoreModule().register()` → `validate()` clean under the package's 046 `known_warnings`;
       `SAModule(config=…)` makes `container.get(SAConfig)` return the instance; `CasbinModule(
       policy_authorizer=True)` binds `PolicyEngineAuthorizer`; `RedisModule(cache=True).async_init`
       is awaited on a bare container without a server (install only — the provider is lazy,
       `cache.py:578-602`).

9. [ ] `varco_fastapi/varco_fastapi/modules/defaults.py` — `default_modules(*, exclude:
       tuple[type[VarcoModule], ...] = ()) -> list[VarcoModule]` returning fresh instances in
       this order: `CoreModule, ObservabilityModule, FastAPIModule, CorsModule,
       SecurityHeadersModule, ErrorEnvelopeModule, BodyLimitModule, LoggingModule, TracingModule,
       LocalizationModule, SecurityPostureModule`. Test `varco_fastapi/tests/test_default_modules.py`:
       two calls return distinct instances; `exclude=(SecurityPostureModule,)` drops exactly it;
       the list resolves without `ModuleDependencyError`; every default module's `is_enabled` on
       a default container is `True` except `LocalizationModule`'s two toggles (env-off) — asserted
       as a literal name list so a default flip is a visible diff (FLIP-1's row lives here).

10. [ ] **§D-names renames** — `SAModule`→`SAConfiguration` (`varco_sa/di.py:81` + every
        reference: `rg -n 'SAModule' varco_* examples README.md ARCHITECTURE.md technical_docs CLAUDE.md`),
        `BeanieModule`→`BeanieConfiguration`, `VarcoFastAPIModule`→`VarcoFastAPIConfiguration`
        (`rg -n 'VarcoFastAPIModule'` — includes CLAUDE.md's "Typical app bootstrap" block and the
        `feedback_di_defaults` memory's wording, README `:709-736`). No alias. `scripts/api_surface.py`
        regenerated **in this step** (the three names move in `__all__`s). **Verify**: `rg -n
        'SAModule|BeanieModule|VarcoFastAPIModule' --glob '!plans/**' --glob '!CHANGELOG.md'` → only
        the new `VarcoModule` subclasses from Step 8 (`class SAModule(VarcoModule)`) and Step 6.

### `create_varco_app`

11. [ ] `varco_fastapi/tests/test_create_varco_app_modules.py` (**new, failing first**) — the
        §D-signature/§D-order contract: `create_varco_app()` with no container → `TypeError`;
        `modules=None` → `container.get(ModuleRegistry).modules` names equal `default_modules()`'s
        names in order; explicit `modules=[CoreModule(), FastAPIModule()]` → exactly two recorded,
        no `SecurityHeadersMiddleware` in `app.user_middleware`; `modules=[SecurityPostureModule()]`
        alone → `ModuleDependencyError` naming `FastAPIModule`; duplicate class → `ValueError`;
        `[*default_modules(), MigrationsModule(mock)]` listed after core → the §D-order WARNING is
        logged (caplog) and `[MigrationsModule(mock), *default_modules()]` is not; a spy module
        records the call sequence `register → (async_init) → middleware → configure_app → start
        → stop` with `start` order = resolved order and `stop` = its reverse (two spy modules `B
        depends_on A`: start `A,B`, stop `B,A`); `async_init` of every module completes before any
        `start` (the `setup=` hook); a disabled module's `register` runs and nothing else does;
        `container.ashutdown()` is awaited after the last `stop` (the existing shutdown-hook test
        shape, `test_lifespan_shutdown.py`); `LifecycleModule(component)` starts/stops the
        component; `MiddlewareModule([(X, {})])` lands **outside** `ErrorMiddleware` (the
        §D-order-bugs position, asserted by index in `app.user_middleware`) and with
        `slot=EdgeSlot.IDEMPOTENCY` inside it.

12. [ ] `varco_fastapi/varco_fastapi/app.py` — rewrite `create_varco_app` per §D-signature and
        §D-order's sequence; `_ModuleComponent` adapter + `_async_init_all` + `_collect_specs` +
        `_warn_unowned_settings` (reads `container.get(MigrationSettings).mode` / `container.get(
        TenancySettings).isolation`, warns when no `MigrationsModule`/`TenancyModule` is
        registered); keep `_scan_routers`, `_mount_router`, `_mount_health_router`; **delete**
        `_collect_lifecycle_components`, `_try_resolve_component`, `_lifecycle_discovery_warns`,
        `_resolve_cors`, `_try_add_request_context_middleware`, `_try_add_profiling_middleware`,
        `_partition_rate_limit_rules`, `_mount_metrics_router` (each moved into its module in
        Steps 6-7). Module docstring rewritten: the numbered step list, the "Minimal usage" block
        (`container = DIContainer(); app = create_varco_app(container)` — modules default), the
        "one rule" for what is a kwarg. `varco_fastapi/__init__.py` exports `default_modules`,
        `VarcoModule` (re-export), `EdgeSlot`, `MiddlewareSpec`; the module classes are imported
        from `varco_fastapi.modules`. **Verify**: Step 11 green; `test_middleware_order.py` green
        after **only** its construction call is migrated (Step 18) — the byte-identical-order proof.

13. [ ] Remove `VARCO_LIFECYCLE_DISCOVERY_WARN` from 048's allowlist in
        `varco_core/tests/test_settings_no_loose_env_reads.py` and from `README.md`'s env tables
        (`rg -n 'VARCO_LIFECYCLE_DISCOVERY_WARN'` → 0 outside CHANGELOG/plans). **Verify**: that
        test green.

### FLIP-1, RO-4, the scan rule, the suite

14. [ ] `varco_fastapi/tests/test_flip1_posture_default_on.py` (**new**) — `create_varco_app(
        DIContainer())` then lifespan enter → `container.get(ModuleRegistry)` has an enabled
        `SecurityPostureModule` whose `.report` is a `SecurityPosture`; `VARCO_SECURITY_POSTURE_ENABLED=false`
        → registered, `enabled False`, `.report` raises `RuntimeError("not started")`; `modules=
        default_modules(exclude=(SecurityPostureModule,))` → absent; `enforce="refuse"` +
        a forced `HIGH` collector → lifespan enter raises (unchanged semantics). Then
        `settings_reference.py` snapshot regenerated for the new field (048's gate).

15. [ ] `varco_fastapi/tests/test_logging_redaction.py` (040's, **extended**) — `LoggingModule(
        redactor=PolicyRedactor())` yields a `RequestLoggingMiddleware` spec with that redactor;
        an app built with it redacts a secret-named field in the log entry (040's existing
        assertion, driven through `create_varco_app`); `LoggingModule()` passes `redactor=None`
        (no `default_redactor()` fallback — 040's rule, asserted).

16. [ ] `varco_core/tests/test_modules_never_scanned.py` + `varco_fastapi/tests/test_modules_never_scanned.py`
        — §D-scan (1)-(3).

17. [ ] `testkit/varco_conformance/di_wiring.py` (046's suite, **extended**) — class attribute
        `module_cls: ClassVar[type[VarcoModule] | None] = None` and four tests, each `pytest.skip`
        when `None` (never silently pass): `test_package_module_registers_cleanly` (fresh
        `DIContainer()`, `CoreModule().register(c)` unless `module_cls is CoreModule`, then
        `module_cls().register(c)` → 046's `test_validate_is_clean` predicate under
        `known_warnings`); `test_package_module_register_is_idempotent` (binding count unchanged
        on the second `register()`); `test_package_module_carries_no_providify_marker` (§D-scan
        (2) for this class); `test_package_module_name_matches_package` (`module_cls.name ==
        package suffix`, `depends_on` contains `CoreModule` unless it *is* `CoreModule`). Set
        `module_cls` in all ten 046 subclasses; `COVERAGE.md`'s `di_wiring` row gains a "module"
        column. Delete the per-package `test_<pkg>_module.py` assertions this now covers (keep the
        constructor-argument ones from Step 8).

### Migration of callers, gates, docs

18. [ ] **Migrate every `create_varco_app` call in `varco_*/tests/`** (Step 1(f)'s list): each old
        kwarg → its §Migration row; container-less calls get `DIContainer()`; tests that asserted
        `enable_logging=False`-style behaviour pass `modules=default_modules(exclude=(LoggingModule,))`
        or the module's `enabled=False`. `test_middleware_order.py` changes **only** its
        construction call (Step 12's proof). `test_lifespan_shutdown*.py` keep their assertions.
        **Verify**: `make test PKG=varco_fastapi` green; `git diff --stat varco_fastapi/tests/test_middleware_order.py`
        shows ≤ 5 changed lines.

19. [ ] **Migrate the examples** — `examples/00-full-stack-post-api` per §D-example (new
        `example/module.py::PostAppModule`; `create_app()` becomes one `create_varco_app(...)`
        call; `README.md:170-260` wiring narrative rewritten; its tests green); `03`, `23`×2, `24`
        gain `DIContainer()`; `01`/`08`/`09`/`10` unchanged except any old kwarg. **Verify**:
        `uv run pytest examples/00-full-stack-post-api -q`; `make asyncapi-check` green (the
        consumer class did not move); `uv run pytest examples/03-observability-metrics -q`.

20. [ ] **Gates** — `uv run python scripts/api_surface.py` (the `create_varco_app` signature,
        the three renames, new exports) and `git diff design/api-freeze-and-standards/measurements/`
        reviewed line by line; `uv run python scripts/import_budget.py --check --warn-only`
        (`varco_fastapi.modules` must not add eager middleware imports — every middleware import
        lives inside a hook body, the pattern `app.py` uses today); `uv run python
        scripts/settings_reference.py --check`; `make lint`; `make type-check` (M1 delta recorded).

21. [ ] **Docs** (same commit — `feedback_keep_docs_updated`):
        - `technical_docs/features/dependency-injection.md` — **§Modules** (new section; 058
          banner unchanged): OQ1's answer with the providify citations table from §Context, the
          `VarcoModule` slots and when each runs, the tri-state, `depends_on` (varco's, with the
          "not providify's" sentence), `default_modules()` + the "exactly those" rule, the
          `*Configuration`/`*Module` naming rule, `HttpContributor`/`EdgeSlot`, `LifecycleModule`/
          `MiddlewareModule`, writing an app module (the example's `PostAppModule`), a **Pitfalls**
          table (a `@Configuration`-decorated module is scan-activated; listing `MigrationsModule`
          after the defaults; two instances of one class; `enabled=False` on a backend module does
          not deselect the backend; a module that `container.get()`s in `_register()` freezes a
          fallback before the app's override — 045's not-evicted caveat).
        - `README.md` — **"Wiring an app"** (first version, before the per-subsystem sections):
          the 8-line default app, the `[*default_modules(), SAModule(...), MigrationsModule(...)]`
          idiom, one `enabled=`/env example, the FLIP-1 line with its env escape hatch,
          `LoggingModule(redactor=…)`; every per-subsystem snippet that passed a `create_varco_app`
          keyword (`rg -n 'create_varco_app\(' README.md` → 18) rewritten to its module.
        - `CLAUDE.md` — §"DI wiring (providify)": the "Typical app bootstrap" block becomes the
          8-line default app; **one new paragraph** "Modules (Plan 049)": the rule (*a varco module is
          a plain `VarcoModule`, constructed by the app, never a `@Configuration`, never scanned —
          `test_modules_never_scanned.py`*), the naming rule, a pointer to the guide's §Modules.
          Decision tree: one new branch under "Am I adding a new capability?" — *"A subsystem the
          app should switch on/off or that needs startup/shutdown? → a `VarcoModule` in the owning
          package (`<pkg>/modules.py`), never a `create_varco_app` keyword; HTTP position → an
          `EdgeSlot`, never `add_middleware` in a module"*. The §HTTP edge hardening rule "never
          register … via `extra_middleware=`" is rewritten to name `MiddlewareModule`'s slot rule.
          `VARCO_LIFECYCLE_DISCOVERY_WARN` mention (if any) removed.
        - `ARCHITECTURE.md` — a "Modules" entry under the DI section: the ABC, the registry, the
          eleven default + ten package + eleven subsystem modules as one table (name · package ·
          default-on · depends_on).
        - `technical_docs/features/security-posture.md` — the "Wiring a collector" snippet becomes
          "Default-on since 4.0 (`SecurityPostureModule`); the hand form still works via
          `LifecycleModule`"; `http-edge-hardening.md`'s `create_varco_app(security_headers=…)`
          rows → modules; `schema-migrations.md`, `multitenancy.md`, `reliability-preset.md`,
          `retention-and-purge.md`, `jwt-claim-transformer.md` (JWKS lifecycle), `i18n-and-localization.md`,
          `timezone-handling.md`, `observability-attributes.md`, `error-taxonomy-and-i18n.md`,
          `composite-deployment.md`, `generic-router.md` — each `create_varco_app(` snippet migrated
          (`rg -n 'create_varco_app\(' technical_docs` → 33 sites, Step 1(f)).
        - `technical_docs/common-pitfalls.md` — one new row: "Decorated a `VarcoModule` subclass
          with `@Configuration`/`@Singleton` → activated by every `container.scan()` of its package;
          modules are constructed by the app, never scanned (`test_modules_never_scanned.py`)".

22. [ ] `CHANGELOG.md` `[Unreleased]` — `### BREAKING — VarcoModule and create_varco_app(modules=…)
        (Plan 049, DI-1 / FLIP-1 / RO-4)` = §Migration; `BACKLOG.md:80,81,112` → `done (Plan 049)`;
        `BACKLOG.md:125-129` Open Question 1 → **Answered (Plan 049 §D-shape)** one-liner;
        `BACKLOG.md:249-252` T3 gains "(enforced by `test_modules_never_scanned.py` since Plan
        049)". **Handoff notes** appended to the index row or the neighbours' plan files if they
        exist: **050** — `_ModuleComponent`/`_async_init_all` in `app.py` are the seams to replace
        with phases; the §D-order WARNING is the placeholder for "migrations phase before core
        phase"; `CoreModule.start()` starting a backend's bus is the re-homing candidate;
        `RedisModule(cache=True)` does not eagerly `aget(CacheBackend)` — fail-fast-at-startup is
        050's call. **051** — `_default_collectors()` in `modules/lifecycle.py` is the function to
        replace with `Multibound`. **052** — `ModuleRegistry.modules` (name · enabled · class) is the
        "Modules" section of the report; `VarcoModule.validate()` is defined, returns `Sequence[str]`,
        and nothing calls it; `_warn_unowned_settings` is a candidate to fold into it. **054** —
        `SecurityPostureModule` is where `enforce` is read. **058** — §Migration below.

## Edge cases

- **`create_varco_app(container)` on an empty `DIContainer()`** → default modules register core +
  fastapi; the app boots with `InMemoryEventBus`/`InMemoryJobStore` defaults and a `HealthRouter`
  — the "Minimal usage" block, and the shape ~40 tests use.
- **`modules=[]`** → exactly nothing registered; no middleware at all, no lifecycle; a bare
  `FastAPI` with `HealthRouter`. Allowed (a test may want it); documented.
- **A module's `_register()` raises** → propagates from `create_varco_app()` (sync, before any
  FastAPI object exists); the registry has recorded nothing for it (recording happens after
  `_register` returns) — a retry with a fresh container is clean.
- **`async_init()` raises** → `VarcoLifespan.setup` raises inside the shutdown-guarded region
  (`lifespan.py:231-237`); `container.ashutdown()` still runs; no `start()` ran.
- **`start()` of module N raises** → modules 1..N-1 are stopped in reverse, then the exception
  propagates and FastAPI fails startup (`lifespan.py:245-254`, unchanged).
- **`SecurityPostureModule` with `enforce="refuse"` and a `HIGH` finding** → its `start()` raises
  → the above path; every earlier module stopped. Byte-identical to the 3.2 hand-wired case.
- **Same module class in `default_modules()` and the app's list** → `ValueError` at
  `create_varco_app()`; message shows `default_modules(exclude=(X,))`.
- **`LifecycleModule(x)` twice for two different components** → two distinct classes? No — same
  class, two instances → `ValueError`. `name=` does not change the class. Documented: subclass
  `VarcoModule` for two lifecycle objects, or wrap both in one `LifecycleModule` via a composite
  component. (⚠️ if Step 18 finds a test needing two, the duplicate rule becomes "same class **and**
  same `name`" — decide there, record in the guide.)
- **`enabled=None` and the settings class is not bound** (an app that skipped `CoreModule`) →
  `_enabled_from_settings` raises `LookupError` from `container.get()`; not caught — the
  `ModuleDependencyError` on `depends_on=(CoreModule,)` fires first for every varco module, so this
  is reachable only from a user module, whose docstring says to depend on `CoreModule`.
- **`VARCO_SECURITY_POSTURE_ENABLED=maybe`** → pydantic `ValidationError` at `bind_settings()`
  (048's fail-fast), naming the variable.
- **`MiddlewareModule([...], slot=EdgeSlot.CORS)`** → sorted beside `CorsModule`'s spec, stable by
  module order; nothing prevents an app from placing a middleware outside CORS — the slot is the
  app's explicit statement.
- **Two backend modules for one seam** (`KafkaModule()` + `NatsModule()`) → both register; the
  seam ties exactly as 045/047 describe; not this plan's concern; `validate()` (052) names it.
- **`container.scan("varco_fastapi", recursive=True)` by an app that never calls
  `create_varco_app`** → installs `VarcoFastAPIConfiguration` (renamed, still a providify
  module) and **no** `VarcoModule` (§D-scan (3)).

## Verification

```bash
# Contract + rule
uv run pytest varco_core/tests/test_di_module.py varco_core/tests/test_modules_never_scanned.py -q
uv run pytest varco_fastapi/tests/test_middleware_slots.py varco_fastapi/tests/test_edge_modules.py \
              varco_fastapi/tests/test_lifecycle_modules.py varco_fastapi/tests/test_default_modules.py \
              varco_fastapi/tests/test_create_varco_app_modules.py varco_fastapi/tests/test_flip1_posture_default_on.py \
              varco_fastapi/tests/test_logging_redaction.py varco_fastapi/tests/test_modules_never_scanned.py -q
uv run pytest varco_fastapi/tests/test_middleware_order.py -q          # byte-identical order proof
uv run pytest varco_fastapi/tests/test_lifespan_shutdown.py varco_fastapi/tests/test_lifespan_shutdown_characterization.py -q

# Suite (all ten packages carry module_cls)
rg -n 'module_cls = ' varco_*/tests/test_*_di_wiring*.py | wc -l      # 10
uv run pytest varco_core/tests/test_di_wiring_conformance.py varco_fastapi/tests/test_fastapi_di_wiring.py -q

# Removals
rg -n 'extra_lifespan_components|extra_middleware=|enable_logging=|enable_tracing=|enable_error_middleware=|migration_settings=' varco_*/varco_* examples README.md technical_docs CLAUDE.md ARCHITECTURE.md   # 0
rg -n 'VARCO_LIFECYCLE_DISCOVERY_WARN|_collect_lifecycle_components|_try_resolve_component' varco_* README.md   # 0
rg -n 'SAModule|BeanieModule|VarcoFastAPIModule' varco_*/varco_* | rg -v 'class (SA|Beanie)Module\(VarcoModule\)'   # 0

# Example + snapshot gates
uv run pytest examples/00-full-stack-post-api -q
make asyncapi-check
uv run python scripts/api_surface.py --check
uv run python scripts/settings_reference.py --check
uv run python scripts/import_budget.py --check --warn-only

# Gates
make lint && make type-check && make test && make docs
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — `varco_core.di` is a package after 047 (`features.py`) | `varco_core/di/module.py` has a home; the `sys.modules` no-providify property of `test_di_constants.py` survives | Step 1(a); convert if flat |
| ⚠️ ASSUMPTION — 048's `bind_core_settings()`/`bind_settings()` fallback path is idempotent, and `varco_fastapi.di.bootstrap()` may already call `bind_core_settings()` | `CoreModule` + `FastAPIModule` registering on one container produce no `AMBIGUOUS_BINDING` | Step 1(b); the `ModuleRegistry` template guarantees module-level idempotency regardless; if `bind_settings` is not idempotent, `CoreModule` guards with `is_user_bound` per class and files the finding to 048's owner |
| ⚠️ ASSUMPTION — `SecurityPostureSettings` is `bind_settings`-bound by `varco_fastapi.di.bootstrap()` and its prefix is `VARCO_SECURITY_` | `VARCO_SECURITY_POSTURE_ENABLED` is the env name either way | Step 1(c); alias or plain field (§D-posture) |
| ⚠️ ASSUMPTION — `test_middleware_order.py` characterises every position in the §D-order table | Green with only its construction call changed proves byte-identical order | Step 1(e); if it characterises fewer positions, `test_middleware_slots.py`'s literal tuple is the second pin |
| ⚠️ ASSUMPTION — 057 shipped `inspect_redaction_posture()` as a collector callable in `posture.py` | `_default_collectors()` includes it in the same `partial` shape | Step 1(d) |
| ⚠️ ASSUMPTION — `_mount_router` can build the example's `PostRouter` from the container synchronously | The example boots through `create_varco_app(routers=[PostRouter])` | §D-example fallback: include it from `PostAppModule.configure_app()`; Step 19 decides |
| ⚠️ ASSUMPTION — ~40 container-less `create_varco_app()` test sites, none relying on `container=None` *semantics* beyond "no routers" | Step 18 is mechanical | Step 1(f) count; a site that needs true container-free behaviour gets `modules=[]` |
| `container.scan("varco_core", recursive=True)` starts activating a module because someone decorates one | The T3 decision | `test_modules_never_scanned.py` (§D-scan), the pitfall row, the CLAUDE.md rule |
| List-order sensitivity for `MigrationsModule`/`TenancyModule` (§D-order ⚠️) | A migration runs before the bus/job runner start when the app lists it first — and the app is told when it did not | The WARNING + its test; 050 replaces with phases |
| `CoreModule.start()` starts a backend's bus — a reader expects `KafkaModule` to | The seam's owner starts what resolves; the backend module only binds | Docstrings on both; handoff to 050 |
| `default_modules()` grows into a hidden "kernel" | Explicit list = exactly those, always | `test_create_varco_app_modules.py`'s explicit-list test; no insertion code path exists |
| The `SecurityPostureModule` `start()` adds latency to every default app | Sub-millisecond | Step 1(g) measurement recorded in the CHANGELOG entry |
| `varco_fastapi.modules` eagerly imports every middleware and breaks the import budget | Middleware imports stay inside hook bodies | Step 20's `--check --warn-only`; the pattern is `app.py`'s today |
| The three `*Configuration` renames break downstream imports | 4.0 window; one-line replacements | §Migration rows; `api_surface` regenerated in Step 10 |
| M1 (`type: ignore` count) moves | Directional only | Step 20 records the delta |

## Migration (3.x → 4.0, DI-1 / FLIP-1 / RO-4 entry — Plan 058 assembles)

**BREAKING — `create_varco_app` takes a container and a module list.**
`create_varco_app(container, *, modules=None, routers=None, scan_packages=None, title=…, version=…,
description=…, openapi_url=…, docs_url=…, validate=True, strict_validation=False, mcp_adapter=None,
mcp_path=…, skill_adapter=None, skill_base_url=…, skill_agent_card_path=…, skill_tasks_prefix=…)`.
`container` is required (`create_varco_app(DIContainer(), routers=[…])` for a container-free app).
`modules=None` installs `varco_fastapi.default_modules()`; a list installs **exactly** those —
start from `[*default_modules(), …]`.

| Old kwarg / shape | New |
|---|---|
| `cors=CORSConfig(...)` | `CorsModule(config=CORSConfig(...))` (default set has `CorsModule()`) |
| `enable_error_middleware=False` | `default_modules(exclude=(ErrorEnvelopeModule,))` |
| `enable_tracing=False` | `default_modules(exclude=(TracingModule,))` or `TracingModule(enabled=False)` |
| `enable_logging=False` | `LoggingModule(enabled=False)` / exclude |
| **RO-4** (no keyword existed) | `LoggingModule(redactor=PolicyRedactor())` |
| `enable_metrics=True` | `modules=[*default_modules(), MetricsModule()]` |
| `enable_profiling=True` | `+ ProfilingModule()` |
| `security_headers=False` / `SecurityHeadersSettings(...)` | `SecurityHeadersModule(enabled=False)` / `SecurityHeadersModule(settings=SecurityHeadersSettings(...))` |
| `body_limit=False` / `BodyLimitSettings(...)` | `BodyLimitModule(enabled=False)` / `BodyLimitModule(settings=...)` |
| `rate_limit=RateLimitBundle(...)` | `+ RateLimitModule(bundle)` |
| `i18n=I18nSettings(enabled=True)`, `timezone=TimezoneSettings(...)` | `LocalizationModule(i18n=..., timezone=...)` (replace the default instance via `exclude`); env toggles unchanged |
| `migrations=m, migration_settings=s` | `+ MigrationsModule(m, settings=s)` — **list it before `*default_modules()`** |
| `tenancy=TenancyLifecycle(pool=p, supervisor=v)` | `+ TenancyModule(pool=p, supervisor=v)` — list it before the defaults |
| `reliability=ReliabilityPreset(...)` | `+ ReliabilityModule(preset)` |
| `retention=RetentionLifecycle(r, container=c, interval=i)` | `+ RetentionModule(r, interval=i)` |
| `jwks_refresh=JwksRefreshLifecycle(reg, interval=i)` | `+ JwksRefreshModule(reg, interval=i)` |
| `extra_lifespan_components=[x, y]` | `+ LifecycleModule(x), LifecycleModule(y)` (or one `VarcoModule` subclass) |
| `extra_middleware=[(M, kw)]` | `+ MiddlewareModule([(M, kw)])` (same position — outside `ErrorMiddleware`); `slot=EdgeSlot.IDEMPOTENCY` to land inside it |
| `configure_jwt=False` | `FastAPIModule(configure_jwt=False)` via `exclude` + re-add |
| `global_attributes=…, capture_params=…` | `ObservabilityModule(global_attributes=…, capture_params=…)` |
| `SecurityPostureLifecycle(...)` in `extra_lifespan_components` | **nothing** — `SecurityPostureModule` is default-on (FLIP-1); `VARCO_SECURITY_POSTURE_ENABLED=false` or `exclude=(SecurityPostureModule,)` turns it off; `SecurityPostureModule(collectors=[…])` keeps a hand list |
| `VARCO_LIFECYCLE_DISCOVERY_WARN` | removed — there is no discovery; a missing bus is a missing module |
| `from varco_sa.di import SAModule` (the `@Configuration`) | `SAConfiguration` — `SAModule` now names the `VarcoModule` in `varco_sa.modules` |
| `from varco_beanie.di import BeanieModule` | `BeanieConfiguration` (same split) |
| `from varco_fastapi.di import VarcoFastAPIModule` | `VarcoFastAPIConfiguration` |
| hand-built `FastAPI()` + `VarcoLifespan()` + `lifespan._setup = …` (the example's shape) | `create_varco_app(container, modules=[…, MyAppModule()])` with `async_init()` as the setup hook — `VarcoLifespan` is unchanged for anyone who keeps the hand shape |

**New public names**: `varco_core.di.{VarcoModule, ModuleRegistry, CoreModule, ObservabilityModule,
LifecycleModule, resolve_module_order, ModuleCycleError, ModuleDependencyError}`;
`varco_fastapi.{default_modules, VarcoModule, EdgeSlot, MiddlewareSpec}`;
`varco_fastapi.modules.{FastAPIModule, CorsModule, SecurityHeadersModule, BodyLimitModule,
ErrorEnvelopeModule, TracingModule, LoggingModule, MetricsModule, ProfilingModule,
RateLimitModule, LocalizationModule, MigrationsModule, TenancyModule, ReliabilityModule,
RetentionModule, JwksRefreshModule, SecurityPostureModule, MiddlewareModule, HttpContributor}`;
`varco_{sa,beanie,redis,kafka,nats,memcached,casbin,ws}.modules.<Pkg>Module`;
`SecurityPostureSettings.enabled` (`VARCO_SECURITY_POSTURE_ENABLED`, default `true`).

## Docs / backlog updates

- `technical_docs/features/dependency-injection.md` §Modules; `README.md` "Wiring an app";
  `CLAUDE.md` paragraph + decision-tree branch; `ARCHITECTURE.md` Modules table; 13 feature docs'
  snippets; `common-pitfalls.md` row — Step 21.
- `CHANGELOG.md`, `BACKLOG.md:80,81,112,125-129,249-252` — Step 22.
- `design/api-freeze-and-standards/measurements/api-surface.*` — Steps 10 and 20;
  `technical_docs/settings-reference.md` + JSON — Step 14; `asyncapi-example.json` — unchanged
  (verified, Step 19).
- `testkit/varco_conformance/COVERAGE.md` — Step 17.

## Done criteria

- [ ] `varco_core.di.VarcoModule` exists with exactly the six slots + `is_enabled`; `register` is
      `@final` and idempotent through `ModuleRegistry`; `resolve_module_order` has the six
      ordering tests green.
- [ ] Ten package modules (`<pkg>/modules.py`, `CoreModule` in `varco_core.di.module`) and the
      subsystem/edge modules exist; every one is Docker-free to `register()`.
- [ ] `create_varco_app` has the §D-signature; `rg` shows zero old kwargs anywhere outside
      `CHANGELOG.md`/`plans/`; `_collect_lifecycle_components` and its kill switch are gone.
- [ ] `test_middleware_order.py` green with ≤ 5 changed lines — order byte-identical.
- [ ] FLIP-1: `SecurityPostureModule` in `default_modules()`, `VARCO_SECURITY_POSTURE_ENABLED=false`
      disables it, `enforce` untouched; RO-4: `LoggingModule(redactor=…)` tested end-to-end.
- [ ] `test_modules_never_scanned.py` (core + fastapi) green; `di_wiring` suite has `module_cls`
      set in all ten subclasses; `COVERAGE.md` updated.
- [ ] Example `00` boots via `create_varco_app(modules=[…])`; `make asyncapi-check` green.
- [ ] `api_surface`, `settings_reference`, `import_budget --warn-only` snapshots regenerated/green
      in the steps that change them; OQ1 answered in `BACKLOG.md`; three rows `done (Plan 049)`.
- [ ] `make lint`, `make type-check`, `make test`, `make docs` green on 3.12/3.13; no file under
      `/home/edoardo/projects/providify` modified.
