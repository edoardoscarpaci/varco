# Research 014 — Multi-Binding, Qualifiers, and Conditional DI in Mature Frameworks

> ⚠️ **Correction (Plan 043 / DI-15):** every mention below of providify shipping `@Requires`
> (`:187-188,204,238`) is wrong. Verified against providify 2.0.1 @ `1402f0a`'s own source
> (`rg -in 'Requires|ConditionalOn|condition=' providify/` — prose hits of the English word
> "requires" only, zero decorator/marker/method definitions): providify ships `@Profile` and
> `@Alternative` only, no predicate-over-configuration primitive under any name. See
> [`design/upstream-gaps/providify-conditional-registration.md`](../upstream-gaps/providify-conditional-registration.md)
> for the filed ask this gap produced. This is a correction to this record, not a rewrite — the
> claims below are left as originally written.

Date: 2026-09-12 · Freshness matters: **YES** — DI patterns, framework versions, and async support conventions are active areas of framework development.

## Question

How do mature frameworks (Spring Boot, Quarkus, Micronaut, .NET, NestJS, and Python DI libraries) handle multi-binding (injecting all implementations of an interface), qualified/keyed selection (choosing one implementation by name/key), conditional registration (turning features on/off at startup), factory methods (async construction, generic types at runtime), startup/shutdown lifecycle ordering, and ordering guarantees for collections? What conventions should varco adopt?

## Findings

### 1. Multi-Binding and Collection Injection Semantics

**Spring Framework:**
- `@Autowired` + `Set<T>` or `List<T>` injects all beans matching a type; `@Qualifier("name")` filters the collection by qualifier value — [Fine-tuning Annotation-based Autowiring with Qualifiers :: Spring Framework](https://docs.spring.io/spring-framework/reference/core/beans/annotation-config/autowired-qualifiers.html).
- Multiple beans may share the same qualifier value (no uniqueness requirement); qualifiers are **filtering criteria, not unique identifiers**.
- `@Order` annotation orders beans in an ordered collection (lower values appear first).
- **No built-in dedup**: if the same bean is registered twice, it appears twice in the injected collection.

**Quarkus / CDI:**
- `@Inject @All List<T>` injects all disambiguated beans as an **immutable list** — [Contexts and Dependency Injection - Quarkus](https://quarkus.io/guides/cdi-reference/).
- `@Priority` annotation sorts beans by priority value (higher values first).
- `@Any` is implicitly applied if no other qualifiers are declared (matches all eligible implementations).
- Collections are **immutable and type-safe** — wildcards and type variables are disallowed.

**Micronaut:**
- `Collection<T>` injection collects all implementations; `@Order` defines precedence (lowest value wins if injecting a single instance).
- `@Primary` marks a default; `@Named("name")` selects by qualifier.
- **Known issue (GitHub #10327)**: `BeanRegistration` collections may ignore `@Named` values intermittently depending on bean registration order.

**Guice:**
- `Multibinder<T>` API binds multiple values separately, later injected as an unmodifiable `Set<T>` — [Multibinder (Google Guice API)](https://google.github.io/guice/api-docs/latest/javadoc/com/google/inject/multibindings/Multibinder.html).
- Iteration order is consistent with binding order.
- Guice 4.0+ adds `@ProvidesIntoSet` and `@ProvidesIntoMap` annotations for simplified multi-binding.
- `@ElementsIntoSet` is used for nested set contributions.

**NestJS:**
- `APP_GUARD`, `APP_INTERCEPTOR`, `APP_FILTER` tokens allow registering multiple implementations globally.
- No native ordering control — multi-providers are accumulated in registration order.
- When multiple global guards/interceptors are registered in different modules, all are applied (accumulation, not replacement).
- **Known limitation**: dependency injection with global providers has historically been problematic ([How to inject component into global guards · Issue #244](https://github.com/nestjs/nest/issues/244)).

**.NET / ASP.NET Core:**
- `IEnumerable<T>` injection collects all registrations; `AddKeyedSingleton<T>(key, ...)` added in .NET 8 for keyed selection.
- `[FromKeyedServices("key")]` attribute selects a specific keyed registration.
- **Registration order determines precedence**: when injecting a single `T`, the last-registered implementation wins.
- Graceful degradation: if injecting `IEnumerable<T>` and no registrations exist, an empty collection is returned (fail-open).

### 2. Qualified/Keyed Selection: Name, Priority, Profile, or Settings

**Spring Framework:**
- `@Qualifier("name")` matches beans with `<qualifier value="name"/>` in XML or `@Bean` + `@Qualifier("name")` in code.
- **No precedence** between multiple qualifiers on the same injection point; all matching beans are injected.
- `@Primary` marks one bean as the default when no qualifier is specified.
- If both `@Primary` and `@Qualifier` are present, `@Qualifier` wins (explicit over implicit).

**Quarkus / CDI:**
- `@Alternative` + `@Priority` / `@AlternativePriority` for priority-based selection.
- **Configuration-driven selection**: `quarkus.arc.selected-alternatives=org.acme.Foo,org.acme.*` (comma-separated list, supports wildcard packages) — [ArC - select alternatives · GitHub PR #8296](https://github.com/quarkusio/quarkus/pull/8296/files).
- When `quarkus.arc.selected-alternatives` is set, matching `@Alternative` beans receive implicit priority of `Integer.MAX_VALUE`, overriding any declared `@Priority`.
- Pattern matching: fully qualified class name, simple class name, or package name with `.*` or `.**` suffix.

**Micronaut:**
- `@Named("name")` for string-based selection; `@Primary` for default.
- `@Requires(property=..., value=...)` allows property-driven bean selection — a bean is only registered if a configuration property matches.

**.NET / ASP.NET Core:**
- Keyed services via `AddKeyedSingleton<T>(object key, ...)` (.NET 8+).
- Retrieval via `[FromKeyedServices("key")]` or `KeyedService.GetKeyedService<T>(key)`.
- No built-in profile or property-driven key selection (unlike Quarkus's `selected-alternatives`).

**Django (Python, no DI):**
- Settings strings: `AUTHENTICATION_BACKENDS = ['path.to.Backend1', 'path.to.Backend2']` — purely string-based, resolved at runtime via `importlib.import_module()`.
- **No precedence rules** — all backends are tried in order until one succeeds.

### 3. Conditional Registration and Feature Activation

**Spring Boot:**
- `@ConditionalOnProperty(name="feature.enabled", havingValue="true")` registers a bean only if a property is set — [Creating Your Own Auto-configuration](https://docs.spring.io/spring-boot/reference/features/developing-auto-configuration.html).
- `@ConditionalOnClass(SomeClass.class)` registers only if a class is on the classpath (library present).
- `@ConditionalOnBean(SomeBean.class)` registers only if another bean exists.
- `@ConditionalOnMissingBean(SomeBean.class)` registers only if no bean of that type exists (used for defaults).
- `@Profile("profile-name")` activates beans for named profiles.

**Quarkus:**
- `@IfBuildProperty(name="...", stringValue="...")` evaluates at build time; `@IfBuildProfile("profile")` for profile-based activation.
- `quarkus.<ext>.enabled` convention: each extension can define a feature flag (e.g., `quarkus.security.enabled`, `quarkus.cache.enabled`).
- `@UnlessBuildProperty` for negation.

**Micronaut:**
- `@Requires(property="feature.enabled", value="true")` — runtime property check.
- `@Requires(beans=SomeBean.class)` and `@Requires(classes=SomeClass.class)` for conditional registration.
- `@Requires(env=...)` checks environment variables.

**NestJS:**
- Dynamic modules with `forRoot({ enabled: true/false })` pattern.
- No declarative conditional annotations; feature enablement is imperative inside a provider factory.

**.NET / ASP.NET Core:**
- `IOptions<T>` pattern: check `options.Value.IsEnabled` inside a factory or configuration method.
- .NET 8+: `HostOptions.ServicesStartConcurrently`/`ServicesStopConcurrently` to toggle concurrent startup/shutdown.
- No standardized `@ConditionalOn*` annotations; enablement is typically hand-written in `ConfigureServices()`.

**Python entry points (ecosystem convention):**
- Libraries register implementations under a `pyproject.toml` entry point group; apps discover via `importlib.metadata.entry_points(group='mygroup')`.
- No conditional mechanism in entry points themselves — filtering is done at discovery time in application code.

### 4. Factory Method Conventions: Async Construction and Generic Types

**Spring Framework:**
- `@Bean` method can inject dependencies as parameters; Spring resolves them at bean-creation time.
- Async construction via `ObjectProvider<T>` (lazy evaluation) or `SmartLifecycle` for post-construction setup.
- `@Bean` method return type is used as the bean type; cannot be parameterized generically at runtime.

**Quarkus / CDI:**
- `@Produces` method (equivalent to Spring's `@Bean`) can inject dependencies and declare return type.
- Async construction via startup events (`@Observes StartupEvent`) or `@Context` scoped beans with `@PostConstruct`.
- No native support for "construct a `Repository<User>` at runtime without a separate method per type."

**NestJS:**
- `useFactory: async (dep: Dep) => { ... }` in `@Module.providers` allows async factories.
- Injection points declared via `inject: [ServiceA, ServiceB]`.
- Generic types resolved by wrapper factory, not native generic resolution.

**Python:**
- `providify` (varco's DI): `@Provider` decorated methods, async support via `@Provider async def factory(...): ...`, return type from method signature.
- `dependency-injector`: `@inject` decorator on factory methods; async via `Callable` return type.
- `Dishka`: `@provide` decorator, async def factories fully supported, resource cleanup via generator pattern (code after `yield` runs on scope exit).

### 5. Startup and Shutdown Lifecycle: Ordering and Phases

**Spring Boot SmartLifecycle:**
- `SmartLifecycle.getPhase()` returns an integer determining startup order: lower values start first, higher values start last.
- **Shutdown is reverse order**: highest phase stops first, lowest phase stops last.
- Default phase for non-`SmartLifecycle` objects is `0`; `Integer.MIN_VALUE` starts first and stops last; `Integer.MAX_VALUE` starts last and stops first.
- `stop(Runnable callback)` is mandatory: implementation **must invoke** the callback's `run()` after shutdown completes.
- **Per-phase timeout**: default 30 seconds, configurable via `DefaultLifecycleProcessor.timeoutPerShutdownPhase` — [Customizing the Nature of a Bean :: Spring Framework](https://docs.spring.io/spring-framework/reference/core/beans/factory-nature.html).
- Explicit "depends-on" relationships override phase order.

**Quarkus:**
- `@Observes StartupEvent` runs after `@Initialized(ApplicationScoped.class)`.
- `@Startup` annotation on bean classes enables eager initialization; priority via method declaration order or `@Priority`.
- `@Observes ShutdownEvent` for cleanup.
- No explicit phase-based ordering of startup tasks; order depends on bean registration.

**.NET / ASP.NET Core IHostedService:**
- `IHostedService.StartAsync()` is called in **registration order** (first-registered starts first).
- `IHostedService.StopAsync()` is called in **reverse registration order** (last-registered stops first).
- Sequential by default; .NET 8+ adds `HostOptions.ServicesStartConcurrently=true` and `ServicesStopConcurrently=true` for concurrent execution.
- `StopAsync()` has a **graceful shutdown timeout** (default 5 seconds, configurable) — [Background tasks with hosted services in ASP.NET Core](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/host/hosted-services?view=aspnetcore-8.0).
- Shutdown is always reverse of startup order (guaranteed bidirectional).

**NestJS Lifecycle:**
- No explicit phase/priority mechanism for multiple services.
- `OnModuleInit()` runs after module creation; `OnModuleDestroy()` runs on shutdown.
- Order follows module import/dependency order, not declarative phases.

**Python:**
- `Dishka` (Python DI): async context-manager lifecycle (`async with container: ...`); cleanup via generator pattern.
- `providify`: `@PreDestroy` methods run on container shutdown (no phase/priority mechanism yet).
- `FastAPI` lifespan events: `@app.lifespan` async generator (setup code before `yield`, cleanup after).

### 6. Framework Lifecycle Guarantees and Reverse Shutdown

**Spring Boot:**
- Guaranteed: startup order = `[phase_low -> phase_high]`, shutdown order = `[phase_high -> phase_low]` (exact reverse).
- Explicit "depends-on" relationships override phase order for both startup and shutdown.

**Quarkus / CDI:**
- Startup order follows bean registration and `@Priority` annotation.
- Shutdown order is **not explicitly documented** as reverse of startup.
- Prefire events fire at startup; `@Observes ShutdownEvent` fires at shutdown (no phase/priority).

**.NET IHostedService:**
- Guaranteed: startup order = `[registered_1st -> registered_Nth]`, shutdown order = `[registered_Nth -> registered_1st]` (reverse).
- Ordered, synchronized (sequential by default unless `ServicesStartConcurrently=true`).

**Python:**
- No framework-wide convention. `asyncio` context managers guarantee cleanup on context exit (reverse of entry), but app-specific `@PreDestroy` ordering is ad hoc.

---

## Options Compared

| Mechanism | Spring Boot | Quarkus / CDI | .NET Core 8+ | Micronaut | NestJS | Python (Dishka/providify) |
|---|---|---|---|---|---|---|
| **Collection Injection** | `Set<T>`, `List<T>` with `@Qualifier` | `@All List<T>` (immutable) | `IEnumerable<T>` or keyed-lookup | `Collection<T>` + `@Order` | `APP_*` tokens (unreliable) | Manual collection via factory |
| **Qualifier / Key Selection** | `@Qualifier("name")` + `@Primary` | `@Alternative` + config-driven `selected-alternatives` | `AddKeyedSingleton<T>(key, ...)` | `@Named("name")` + `@Requires(property)` | No native mechanism | `@Named` (providify 2.0+) |
| **Conditional Registration** | `@ConditionalOn{Property,Bean,Class}` + `@Profile` | `@IfBuildProperty` / `@IfBuildProfile` | `IOptions<T>` checks + manual conditionals | `@Requires(property/beans/classes)` | `forRoot({ enabled: bool })` | `@Requires` (providify); `@Provides(when=...)` (proposed) |
| **Async Factory** | `ObjectProvider<T>` or `SmartLifecycle` | `@Observes StartupEvent` + `@PostConstruct` | `ServiceProvider` + `IAsyncDisposable` | `@Context` + `@PostConstruct` | `useFactory: async (...)` | `@Provider async def`, Dishka generator cleanup |
| **Generic Type at Runtime** | Not native; workaround: separate `@Bean` per type | Not native; workaround: producer method per type | Not native; workaround: factory per type | Not native; workaround: per-type provider | `useFactory` returns instance, never the type | `@Provider` method return type; no native parametric binding |
| **Startup/Shutdown Phase** | `SmartLifecycle.getPhase()` (integer phases, reverse guaranteed) | Bean registration order (reverse not guaranteed) | `IHostedService` registration order (reverse guaranteed) | Bean order (no phases) | Module import order (no phases) | Context manager cleanup (reverse of entry guaranteed) |
| **Ordering Guarantee** | Lowest phase first; highest phase stops first | Registration/priority order; reverse at shutdown not explicit | Registration order first; reverse at shutdown | Lowest `@Order` first | Accumulation; no ordering | Context manager semantic (reverse guaranteed) |
| **Dedup / Uniqueness** | No dedup; duplicate beans = duplicate entries in collection | `@All` requires all beans be disambiguated (immutable prevents duplicates) | No dedup; `IEnumerable<T>` may contain duplicates | No dedup | No dedup | Manual dedup via set/dict |

---

## Version/Compatibility Notes

- **Spring Framework**: `@Qualifier` stable since 2.0 (2006); `SmartLifecycle` since 3.0 (2009); `@Order` since 4.0 (2013). Tested through Spring Boot 4.1 (2025).
- **Quarkus**: `@DefaultBean` added in 2.0+ (2021); `@All` available in 2.15+ (2022); `quarkus.arc.selected-alternatives` added in 2.7+ (2021).
- **ASP.NET Core**: `IHostedService` in .NET Core 2.0+ (2017); keyed services in .NET 8 (2023) via `AddKeyedSingleton<T>`.
- **Micronaut**: `@Order` and collection injection since 1.0 (2018); `@Requires(property)` available throughout.
- **NestJS**: `APP_GUARD`/`APP_INTERCEPTOR` since early 2.x (2017); dependency injection improvements ongoing (known issues through 2024).
- **Python (providify)**: `@Named` qualifiers in 2.0+ (2024); `@Requires` and conditional registration not yet standardized across libraries.

---

## Evidence Gaps

1. **Python DI multi-binding convention**: Dishka, providify, dependency-injector, and others lack a consensus on collection injection (like Spring's `List<T>` or Quarkus's `@All`). Each library uses different patterns (manual factory-returned collections, `Instance<T>` iteration, or none). No PEP or ecosystem standard yet.

2. **NestJS multi-provider reliability**: Documentation acknowledges dependency injection with global providers (`APP_GUARD`, `APP_INTERCEPTOR`) has had issues ([GitHub #4240](https://github.com/nestjs/nest/issues/4240)), but no comprehensive test matrix or fixed version boundary is published. Third-party packages like `nestjs-multi-provider` exist as workarounds, indicating the core support is weak.

3. **Micronaut BeanRegistration + @Named ordering**: GitHub issue #10327 reports intermittent failures when injecting `BeanRegistration<T>` collections with `@Named`. Root cause and fix status unclear; no version cutoff provided.

4. **Python entry-point conditional activation**: Entry points (`importlib.metadata`) support discovery only, not conditions. Each Python framework (Litestar, Quart, FastAPI) implements conditional loading independently — no shared standard.

5. **Startup/shutdown phase reversal in Quarkus**: Quarkus documentation does not explicitly guarantee that shutdown order is the reverse of startup order. Empirical testing needed to confirm or deny this contract.

> ⚠️ **Superseded in part (Plan 043):** the claim below that "None of the 6 frameworks natively
> support" open-generic binding is wrong for *implementation-type* registration — brief
> [`016-open-generic-binding-in-di-containers.md`](016-open-generic-binding-in-di-containers.md)
> §1 documents .NET support since Core 1.0, and §5 documents Castle Windsor support. What is
> absent everywhere except Autofac (016 §2) is specifically *factory-delegate* open generics —
> the shape varco actually needs, since Python has no runtime generic-class instantiation with
> substituted parameters (016 §7). The "unsolved across the ecosystem" framing below holds for
> **Python** (016 §6), which is the part that motivated this BACKLOG row; it does not hold for
> the wider survey. See [`design/upstream-gaps/providify-open-generic-binding.md`](../upstream-gaps/providify-open-generic-binding.md)
> for the filed ask.

6. **Generic-type runtime binding across frameworks**: None of the 6 frameworks natively support "register a `Repository<User>` and `Repository<Post>` at runtime from a single parametric factory" — all require separate methods/calls per type. This is an unsolved problem across the entire DI ecosystem.

---

## Librarian's Note

**Five concrete conventions emerge that varco should adopt:**

1. **Config-driven alternative selection (Quarkus model)** — Adopt `varco.di.selected-alternatives` property (comma-separated, wildcard-supported class names) as the canonical way to swap implementations without code changes. This is more powerful than Spring Boot's registration-order approach and explicitly acknowledges operator intent. Evidence: Quarkus's pattern is proven at scale across microservices deployments.

2. **Immutable collection injection** — If varco adds multi-binding (all implementations of an ABC), injected collections must be immutable (per Quarkus's `@All` contract). This prevents accidental mutations and forces explicit dedup at registration time, not resolution time.

3. **Explicit phase-based startup/shutdown with reverse-shutdown guarantee** — Adopt Spring Boot's `SmartLifecycle` phase model (integer phases; lowest starts first; highest stops first; shutdown = reverse of startup) rather than NestJS/Micronaut's registration-order models. Phases are discoverable and composable; registration order is implicit and fragile. Evidence: Spring Boot's model has survived 15+ years of production use.

4. **Conditional registration via `@Requires(property=...)` and configuration properties** — Use Micronaut's `@Requires` semantic with property name precedence (`varco.<feature>.enabled` for optional features, `varco.<seam>.selected=ImplementationName` for swappable seams). Quarkus's config-selected alternatives handles the swapping; `@Requires` handles the conditional registration. Evidence: Both are production-proven.

5. **Async factory pattern via providify's native `@Provider async def` (not new mechanism)** — varco already has this; document clearly that async factories run inside providify's `@Configuration` installation phase. For generic types at runtime (e.g., `Repository<User>`), document the workaround: a `RepositoryFactory` that builds and registers repositories, rather than a parametric factory — this pattern is universal across all frameworks surveyed.

**Regarding Python DI ecosystem gaps**: varco's use of providify (with `@Named`, `@Requires`, async support) positions it as a Python-DI pioneer. No off-the-shelf precedent exists; varco's own CLAUDE.md patterns are the standard varco will set. The contrast with Java/Spring (30-year maturity) and .NET (multi-key keyed services in .NET 8) is stark; Python has no settled convention yet, making varco's explicit design choices table-stakes for adoption.

