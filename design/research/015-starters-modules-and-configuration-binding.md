# Research 015 — Starters, Modules, and Configuration Binding: Patterns from Mature Frameworks

Date: 2026-09-12 · Freshness matters: **YES** — plugin architectures, configuration binding, and composition patterns evolve with framework versions; DI standardization is current as of 2026.

## Question

How do mature frameworks standardize the unit of extension (starter/extension/module), bind configuration to that unit, compose many such units into a single app, validate the composition at startup, and support testing with overrides? Specifically:

1. What is the minimal contract a startup module/extension must declare (dependencies on other modules, ordering, what it contributes, what config it reads)?
2. How is configuration injected, validated, and made overridable (env var vs code vs file, precedence, hot reload)?
3. How do apps compose 10–15 optional subsystems without a 15-keyword constructor?
4. What do frameworks check at startup to catch wiring errors early, and when (build time, compile time, startup)?
5. How do tests override one binding or an entire "profile" of an app?

For varco specifically, which uses `providify` as a DI container, the goal is to unify today's scattered patterns (`bootstrap()`, `async_bootstrap()`, `bind_*()`, `enable_*()`, `mount_*()`, `install_*()`, plus ~15 hand-constructed Lifecycle objects in `create_varco_app()`).

## Findings

### 1. The Extension/Starter/Module Contract: Three Dominant Shapes

**Spring Boot Starters:**
- [Spring Boot reference: Starters](https://docs.spring.io/spring-boot/reference/features/developing-auto-configuration.html) — A starter is a **Maven/Gradle artifact pair**: an autoconfigure module (contains `@AutoConfiguration` classes) + a starter module (depends on the autoconfigure + related libraries).
- Auto-configuration classes are discovered via `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` (a flat list of class names). They are NOT auto-scanned; they must be listed in this file.
- An `@AutoConfiguration` class declares: (1) what it configures (via `@Bean` methods), (2) conditions under which it activates (`@ConditionalOnMissingBean`, `@ConditionalOnClass`, `@ConditionalOnProperty`), (3) ordering relative to other auto-configs (`@AutoConfigureAfter`, `@AutoConfigureBefore`, or inline `@AutoConfiguration(after=...)`).
- **Minimal contract**: class name → condition → beans → ordering. Registration is explicit (the imports file is checked into the artifact), not implicit scanning.

**Quarkus Extensions:**
- [Quarkus writing-extensions guide](https://quarkus.io/guides/writing-extensions/) — An extension has two modules: runtime (contains the actual code) and deployment (processes configuration at build time, generates bytecode via "Augmentation"). Neither module scans the other.
- Extensions are discovered by Maven/Gradle plugin convention (the `-deployment` suffix) and activation is automatic if the runtime artifact is on the classpath. No explicit registration file needed (the plugin does it).
- An extension declares: (1) runtime classes (must not reference deployment), (2) build-time config via `@ConfigRoot(phase=BUILD_TIME)` vs runtime config via `@ConfigRoot(phase=RUN_TIME)` (sources: [Quarkus CDI docs](https://quarkus.io/guides/cdi-reference/)), (3) dependencies on other extension deployment modules.
- **Key difference from Spring Boot**: build-time and runtime are separate — config decisions can be baked into the bytecode, not resolved at startup.

**NestJS Dynamic Modules:**
- [NestJS fundamentals: Dynamic Modules](https://docs.nestjs.com/fundamentals/dynamic-modules) — A module is a class with `@Module()` decorator. A **dynamic module** is returned by a static method (convention: `forRoot()`, `forRootAsync()`, `forFeature()`, `forFeatureAsync()`).
- `forRoot()` configures once globally; `forRootAsync()` is the same but uses DI for the config; `forFeature()` uses root config while allowing feature-specific tweaks.
- The `imports` array in `@Module()` lists static modules or functions returning dynamic modules. NestJS's `ConfigurableModuleBuilder` ([docs](https://docs.nestjs.com/fundamentals/dynamic-modules)) auto-generates `forRoot`/`forRootAsync` methods.
- **Minimal contract**: a static method returning a module object with `module` property + `providers`/`exports` array + config passed through to the module's dependency injection.

**Django AppConfig:**
- [Django applications reference](https://docs.djangoproject.com/en/6.0/ref/applications/) — Each app has an `AppConfig` subclass in `apps.py`. `INSTALLED_APPS` lists app labels or config class paths.
- Django's startup stages: (1) import each app, (2) import `models` module, (3) call `ready()` on each `AppConfig`. The `ready()` method is where to register signal handlers, validate settings (before any request).
- **Minimal contract**: `AppConfig.name` (required), optional `ready()` override. No explicit dependency graph; ordering is `INSTALLED_APPS` order. No output-to-DI-container mechanism — Django uses settings strings for component discovery.

**Laravel Service Providers:**
- [Laravel service providers docs](https://laravel.com/docs/12.x/providers) — A provider has `register()` (bind to container only) and `boot()` (use other services, register listeners, routes). They're registered in `bootstrap/providers.php` (a flat list).
- Deferred providers implement `DeferrableProvider` interface + `provides()` method; Laravel compiles a service list and loads the provider only when a service is needed.
- **Minimal contract**: `register()` method, optional `boot()` method, optional `provides()` for deferral. Ordering is registration order, but `boot()` methods run after all `register()` methods complete (two-phase).
- No scanning; explicit list in PHP array.

**Convergence:**
All five frameworks require an **explicit, not scanned, registration** of modules (Spring's imports file, Quarkus's plugin convention, NestJS's decorator, Django's `INSTALLED_APPS`, Laravel's `providers.php`). Each module declares what it provides and conditions under which it's active. Ordering is either explicit (`@AutoConfigureAfter`) or implicit via registration order.

---

### 2. Configuration Binding: Injection, Validation, Precedence, Hot Reload

**ASP.NET Core Options Pattern:**
- [Microsoft Learn: Options pattern](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/configuration/options?view=aspnetcore-10.0) — Configuration is strongly typed into a POCO class; `services.AddOptions<TOptions>().Bind(config.GetSection(...)).ValidateDataAnnotations().ValidateOnStart()` registers it.
- Three injection interfaces: `IOptions<T>` (singleton, no reload), `IOptionsSnapshot<T>` (scoped, reloads per-request), `IOptionsMonitor<T>` (singleton, hot-reload via change notifications).
- Named options supported by `.Configure<T>(name, ...)` → injected via `IOptionsSnapshot<T>.Get(name)` or `IOptionsMonitor<T>.Get(name)`.
- `ValidateOnStart()` runs validation at app startup (fail-fast), not on first access. Validation is via data annotations or `IValidateOptions<T>` implementation.
- Precedence: code (explicit `Configure()` call) > env var (via `IConfigurationProvider` precedence chain) > file (appsettings.json).

**Quarkus Configuration:**
- [Quarkus CDI guide](https://quarkus.io/guides/cdi-reference/) — `@ConfigMapping` maps a configuration subtree to a Java interface (immutable, at build time or runtime depending on `@ConfigRoot(phase=...)`).
- **Build-time config** is embedded in bytecode, no reload. **Runtime config** reads on startup, can change per restart but not live-reload (SmallRye Config does not support hot reload).
- Precedence: Java system property > env var > config file. Env var key names are auto-derived from config key path with `_` separator.
- Validation: `@Valid` annotations on config classes; validation runs at startup if `@ConfigRoot(phase=RUN_TIME)`.

**Micronaut Configuration:**
- [Micronaut configuration guide](https://guides.micronaut.io/latest/micronaut-configuration-maven-java.html) — `@ConfigurationProperties(prefix="...")` on a POJO; `@EachProperty` creates a bean per named config entry (multiple datasources, e.g. `datasources.db1`, `datasources.db2`).
- Compile-time code generation; no reflection at runtime. Validation via Jakarta Bean Validation annotations (`@NotBlank`, etc.).
- Property type conversion via `ConversionService`. JSON Schema generation (Micronaut 5+) for IDE completion and external tooling.
- Precedence: Java property > env var > file (no documented "code override" — assumes env var is the highest manual override).

**Spring Boot Configuration:**
- [Spring Boot: Configuration documentation](https://docs.spring.io/spring-boot/reference/features/external-config.html) — `@ConfigurationProperties(prefix="...")` on a POJO + `@EnableConfigurationProperties(MyConfig.class)` (or auto if component-scanned); `spring-boot-configuration-processor` generates IDE metadata.
- Precedence (highest to lowest): command-line args → system properties → env vars → config files (in order: `application-{profile}.properties` → `application.properties`).
- Validation: data annotations + `@Validated` + optional `BindResult.getExceptions()` inspection. No built-in hot reload (Spring Cloud Config Refresh is a separate library).
- Named properties: `@ConfigurationProperties` with a list → each item bound to a list element.

**Django Settings:**
- [Django settings documentation](https://docs.djangoproject.com/en/6.0/ref/settings/) — Settings are Python module-level constants; no injection or type safety (a dict-based settings object may be used for organization, but it's still module-level).
- No validation framework in Django core; apps define custom `AppConfig.ready()` checks or use separate `django.core.checks` system.
- Precedence: env var (via manual `os.getenv()` in settings.py) > hardcoded constant. Settings override via subclassing (e.g. `settings/production.py` extends `settings/base.py`).
- No hot reload. No named instances (a list of settings per feature is the convention).

**Laravel Configuration:**
- [Laravel configuration reference](https://laravel.com/docs/12.x/configuration) — Config files are PHP arrays in `config/` directory; values are accessed via `config('key.nested')` helper. ServiceProvider's `register()` can read config and bind to container.
- Precedence: env var (via `env('KEY')` helper in config files) > config file. Config caching (artisan `config:cache`) compiles to a PHP array for performance.
- Named instances: convention is nested arrays, e.g. `config('database.connections.mysql')` and `config('database.connections.pgsql')`.
- No type safety; no validation in core framework (apps add custom logic in ServiceProvider).

**Convergence:**
Strong-typed configuration via decorated POJOs is the JVM norm (Spring, Quarkus, Micronaut). Python (Django) and PHP (Laravel) default to untyped dicts/arrays because their languages lack compile-time type safety as a standard. Precedence hierarchy is consistent: code/CLI > env var > file. Hot reload is rare (only ASP.NET Core's `IOptionsMonitor`). Named instances via `@EachProperty`/`@ConfigProperty(name=...)` or repeated registration are supported on JVM frameworks. Validation at startup (fail-fast) is table stakes for all except Django and Laravel (which leave it to the app).

---

### 3. Composition at App-Level: 10–15 Subsystems Without a 15-Keyword Constructor

**Spring Boot: Starters on Classpath, Auto-config Kicks In**
- If a starter artifact is on the classpath, its `@AutoConfiguration` class is discovered and activated. No application code needed. Conditions + `@ConditionalOnMissingBean` handle opt-in/opt-out.
- **No builder.** The app is a `@SpringBootApplication` class; starters handle composition transparently. Override a bean if needed; otherwise, the default activates.
- Evidence: Spring Boot's own 50+ starters (data, security, web, etc.) and zero documented composition ceremony.

**Quarkus: Extensions on Classpath, Plugin Detects Them**
- Add the extension artifact to your pom.xml/build.gradle. Quarkus's Maven/Gradle plugin scans for `-deployment` artifacts and auto-configures at build time.
- Properties can be set via `application.properties`; extensions read their config during Augmentation. No code ceremony.
- Precedence: present on classpath → extension auto-activates, unless property-disabled (`quarkus.{feature}.enabled=false`).

**NestJS: Explicit Module Imports**
- The root `AppModule` has an `imports` array: `imports: [AuthModule.forRoot(options), DatabaseModule.forRoot(options), ...]`.
- Each module is explicit; the app composes them by listing them. No auto-discovery (JavaScript/TypeScript doesn't have a standard classpath-scanning convention).
- Builder pattern: app code reads config and calls `Module.forRoot(config)`, then adds to imports.

**Laravel: Service Providers + `bootstrap/providers.php`**
- Add the provider class to `bootstrap/providers.php` (a PHP array). No ceremony; no code-side builder.
- Providers run their `register()` then `boot()` methods in order. Deferred providers lazy-load.
- Result: app is very simple — just a list of provider class names.

**Django: `INSTALLED_APPS` List**
- Add app label or config class to `INSTALLED_APPS` (a Python list in settings).
- Django's loading sequence is automatic: import → models → `ready()`. No code ceremony.

**ASP.NET Core Builder Pattern:**
- `var builder = WebApplicationBuilder.CreateBuilder(args);`
- Then: `builder.Services.AddAuthentication(...); builder.Services.AddEntityFrameworkSqlServer(...); builder.Services.AddOpenAPI(...);`
- Then: `var app = builder.Build();`
- Then: `app.UseAuthentication(); app.UseRouting(); app.MapOpenApi(); app.Run();`
- **Explicit builder; two stages** (service registration + middleware pipeline activation). No auto-discovery.
- Recently (ASP.NET 8+), builder can load module registration from config files to reduce ceremony.

**Convergence:**
JVM frameworks (Spring, Quarkus) prefer classpath-based auto-discovery (zero application code). Frameworks without a standard classpath convention (NestJS, Laravel, Django, ASP.NET) require explicit lists or module imports. **No framework uses a 15-keyword constructor**; they all delegate composition to either the classpath or a small list/array.

Varco's pain point is the `create_varco_app(..., TenancyLifecycle(...), ReliabilityLifecycle(...), ...)` pattern — 15 keyword args. Spring Boot's solution is "classpath auto-discovery + conditions"; ASP.NET's is "builder chain + registration methods"; NestJS's is "explicit module list".

---

### 4. Startup Validation & Wiring Verification

**Spring Boot: @ConditionalOnMissingBean + Actuator**
- Conditions are evaluated at bean-resolution time. Missing dependency → no bean created (silent fallback if `@ConditionalOnBean` fails).
- **Auto-config report** (enabled with `--debug` or `logging.level.org.springframework.boot.autoconfigure=DEBUG`): logs every auto-config class, whether it was applied or skipped, and why.
- `/actuator/conditions` endpoint (if enabled) exposes full condition evaluation as JSON.
- Evidence: [Spring Boot reference](https://docs.spring.io/spring-boot/reference/features/developing-auto-configuration.html), feature is standard since 1.0.0.

**Quarkus: Build-Time Validation**
- Quarkus's build phase validates all injection points. If a dependency is missing or ambiguous, the build fails with an error.
- `@ConfigRoot(phase=BUILD_TIME)` config is validated at build time; `@ConfigRoot(phase=RUN_TIME)` is validated at startup if validation annotations are present.
- No equivalent to Spring Boot's debug report; build failure is the signal.

**Micronaut: Compile-Time Validation**
- Micronaut's annotation processor (runs at compile time, not build time) validates all injection points and reports mismatches.
- If a dependency is missing, the compile fails. No runtime surprises.

**ASP.NET Core: ValidateOnStart + Development Checks**
- `ValidateOnStart()` on options runs validation at app startup. If config is invalid, the app refuses to start.
- `ValidateScopes()` in development checks that no scoped services are injected into singletons (a common mistake).
- Evidence: [Microsoft Learn](https://learn.microsoft.com/en-us/aspnet/core/fundamentals/configuration/options?view=aspnetcore-10.0), known issue filed in 2024 ([GitHub #56453](https://github.com/dotnet/aspnetcore/issues/56453)) that `ValidateOnStart` doesn't always run before first request.

**Django: `check --deploy` + AppConfig.ready()**
- `manage.py check --deploy` runs registered system checks at startup (custom checks via `@register(Tags.security, deploy=True)`).
- Developers manually write checks in `AppConfig.ready()` or register them. No automatic validation.
- Not run by default; must be explicit (`check --deploy` is a deployment ritual, not automatic).

**FastAPI/Starlette: No Built-In Validation**
- No standard "startup validation" mechanism. Apps must add custom `@app.on_event("startup")` handlers.
- Litestar's plugin system ([docs](https://docs.litestar.dev/2/onboarding/fastapi.html)) allows lifecycle hooks per plugin, but no framework-level validation report.
- Gap: Python async frameworks have no equivalent to Spring Boot's `--debug` report.

**Convergence:**
JVM frameworks validate at build/compile time (Quarkus, Micronaut) or startup with logging (Spring Boot). Python/PHP frameworks (Django, Laravel, FastAPI) leave validation to the app (Django's optional `check --deploy`, manual ServiceProvider code). **Only Spring Boot ships a standard "what did I auto-configure?" report.** This is a differentiation — ops teams need visibility.

---

### 5. Testing Support: Overrides and Profiles

**Spring Boot: @TestConfiguration + @MockBean**
- [Baeldung: Overriding Spring Beans in Tests](https://www.baeldung.com/spring-beans-integration-test-override) — `@TestConfiguration` class defines test beans; `@MockBean` auto-creates a Mockito mock + registers it in the context.
- `@Primary` gives a bean precedence; `@Profile` activates beans conditionally. `@ActiveProfiles("test")` activates a test profile.
- `ApplicationContextRunner` is used to test auto-configurations in isolation: `.withConfiguration(AutoConfigurations.of(MyAutoConfiguration.class)).withPropertyValues("prop=val").run(ctx -> { ... })`.

**Quarkus: @TestProfile + getConfigOverrides()**
- [Quarkus testing guide](https://quarkus.io/guides/testing-with-quarkus) — A test class implements `QuarkusTestProfile` to override config: `getConfigOverrides()` returns a map of property key-value pairs. A test class is annotated with `@QuarkusTest` and can implement `QuarkusTestProfile` on a static inner class (or vice versa).
- `@InjectMock` injects a Mockito mock. Properties set in `getConfigOverrides()` apply to that test only.

**Quarkus example:**
```java
@QuarkusTest
class MyTest {
    static class MyProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("db.url", "jdbc:sqlite::memory:");
        }
    }
}
```

**NestJS: Test.createTestingModule + overrideProvider**
- [NestJS testing docs](https://docs.nestjs.com/fundamentals/testing) — `Test.createTestingModule()` creates a module for testing. `.overrideProvider(SomeService).useValue(mockService)` replaces a provider with a mock. `.overrideGuard(SomeGuard).useValue(mockGuard)` replaces a guard, etc.
- Chainable: `.overrideProvider(...).useValue(...).overrideProvider(...).useClass(...).compile()`.
- Three strategies: `useValue` (direct mock), `useClass` (different implementation), `useFactory` (factory function).

**ASP.NET Core: WebApplicationFactory**
- [WebApplicationFactory docs](https://learn.microsoft.com/en-us/dotnet/core/testing/unit-testing-with-webapplicationfactory) — `WebApplicationFactory<Program>` creates an `HttpClient` for integration testing. Override via `CreateClient()` or override `ConfigureWebHost()` to customize services/middleware.
- `.ConfigureServices(services => services.AddScoped<IMyService, MockService>())` replaces a service. Services are reregistered, so registration order matters (user registration first, mock second).

**Django: Test Client + Django Test Case**
- [Django testing documentation](https://docs.djangoproject.com/en/6.0/topics/testing/) — Override settings for a test via `@override_settings(SETTING='value')` decorator or `TestCase.settings_changed` signal.
- Mock objects via `unittest.mock.patch()`. Override a setting in-memory; Django's test runner creates a fresh DB for each test.
- No "test profile" or module-level config switching; it's ad-hoc per test.

**Laravel: Testing Traits + Config Override**
- [Laravel testing docs](https://laravel.com/docs/12.x/testing) — Test classes use `RefreshDatabase` and `WithoutMiddleware` traits. Override config via `$this->artisan('config:cache')` or `Config::set('key', 'value')` within a test.
- Mock services via `$this->mock(SomeService::class, function ($mock) { $mock->shouldReceive('method')->andReturn('result'); })`.
- No test profile; config is per-test.

**Convergence:**
JVM frameworks (Spring, Quarkus, NestJS) support a "test profile" or "test module" that substitutes an entire subsystem (good for integration tests). Python/PHP frameworks (Django, Laravel) override individual settings or services in-memory (fine for unit tests, but integration-test setup is verbose). **Spring Boot's `ApplicationContextRunner` is unique** — it tests auto-configuration in isolation, simulating partial classpath/config states.

Varco's testing gap: with `~15 Lifecycle` objects, testing one override (e.g., "use an in-memory cache instead of Redis") requires constructing 15 objects minus one replacement. A "test profile" (Quarkus-style) would reduce this.

---

## Options Compared

| Framework | Module Registration | Config Binding | Composition | Startup Validation | Testing Approach |
|---|---|---|---|---|---|
| **Spring Boot** | Artifact on classpath → auto-config discovered via META-INF imports file | `@ConfigurationProperties` + strong typing, env-var-friendly | Auto-discover; conditions + `@ConditionalOnMissingBean` handle opt-in | `--debug` report; actuator `/conditions` endpoint | `@TestConfiguration` + `@MockBean` + `ApplicationContextRunner` for auto-config testing |
| **Quarkus** | Extension artifact → plugin convention detects deployment module | `@ConfigMapping` (build-time) or `@ConfigRoot(RUN_TIME)` + compile-time validation | Auto-detect via build-time metadata; properties in `application.properties` | Build-time validation; missing injection point fails build | `@TestProfile` + `getConfigOverrides()` for per-test property override |
| **Micronaut** | Component scan + convention (`@Singleton` on classpath) | `@ConfigurationProperties` + compile-time code generation + `@EachProperty` for named instances | Automatic via compile-time analysis; no DI ceremony | Compile-time validation of injection points | `@RunWith(MicronautTest.class)` + property overrides; manual service mocking |
| **NestJS** | Explicit `imports` array in root module | Decorators on config class; pass options to `Module.forRoot(options)` | `imports: [ModuleA.forRoot(...), ModuleB.forRootAsync(...)]` — explicit list | No automatic validation; depends on code + tests | `Test.createTestingModule().overrideProvider(...).useValue(...)` |
| **ASP.NET Core** | `services.Add*` registration + TryAdd* for defaults | `IOptions<T>` / `IOptionsSnapshot<T>` / `IOptionsMonitor<T>` + strong typing | Builder chain: `services.AddX()` → `app.UseX()` (register, then activate) | `ValidateOnStart()` on options + `ValidateScopes()` in dev | `WebApplicationFactory` + override via `.ConfigureServices()` |
| **Django** | `INSTALLED_APPS` list; auto-discovery of `AppConfig` in `apps.py` | Untyped `settings` module; no injection (manual `config('key')` calls) | List-based; `AppConfig.ready()` for init logic | Optional `manage.py check --deploy` + manual `AppConfig.ready()` checks | `@override_settings()` decorator + `unittest.mock.patch()` |
| **Laravel** | `bootstrap/providers.php` list; deferred providers via `DeferrableProvider` + `provides()` | Untyped config files in `config/` + manual ServiceProvider binding to container | Explicit provider list; `register()` + `boot()` two-phase | Manual validation in ServiceProvider; no framework-level preflight | `Config::set()` in test + `$this->mock()` for services |

---

## Version/Compatibility Notes

- **Spring Boot**: `@ConditionalOnMissingBean` and auto-config imports file stable since 1.0.0 (2013); `@AutoConfiguration` introduced 3.0 (2022) as replacement for `@Configuration`, now standard; `--debug` report stable since 1.0.0.
- **Quarkus**: Two-module extension pattern and build-time config (`@ConfigRoot(BUILD_TIME)`) standard since 3.x (2023+); Augmentation model is core to Quarkus 1.x+.
- **Micronaut**: `@ConfigurationProperties` stable since 1.x (2018+); compile-time code generation is core; `@EachProperty` for named instances available since 1.x.
- **NestJS**: `forRoot` convention from 2.x (2017+); `ConfigurableModuleBuilder` added 9.x (2022) to reduce boilerplate. Stable through 11.x (2025).
- **ASP.NET Core**: `IOptions<T>` pattern stable since .NET Core 2.0 (2017); `IOptionsMonitor<T>` (hot reload) since 2.0. `ValidateOnStart()` in .NET 6.0+ (2021); known validation-timing issue documented 2024.
- **Django**: `AppConfig` and `ready()` since 1.7 (2014); `manage.py check` framework since 1.8 (2015). System checks stable through 6.0 (2025).
- **Laravel**: `ServiceProvider` and two-phase (`register`/`boot`) stable since Laravel 4 (2013); `DeferrableProvider` and `provides()` since Laravel 5.2 (2016). Stable through 13.x (2026).

---

## Evidence Gaps

1. **Hot-reload configuration in Python async frameworks**: Neither Quarkus nor Django/FastAPI ship live configuration reload (except ASP.NET's `IOptionsMonitor`). Gap: no measurement of whether varco should target hot reload or "restart-to-reload" is acceptable.

2. **Automatic composition discovery in Python**: Python has no classpath-scanning standard (entry points are explicit registration). Varco must invent its own discovery mechanism or accept explicit lists (like NestJS/Laravel). No prior Python async framework has standardized this.

3. **Startup validation report best practices**: Spring Boot's `--debug` report is ten years old; no equivalent in Python. Gap: no template for what a varco "diagnose" report should contain (which ABCs have what impl, where config came from, etc.). Doing a survey of 3+ async Python frameworks that added this after Spring Boot could help, but literature is sparse.

4. **Configuration validation at startup vs on-first-use**: Only ASP.NET Core documents a known issue where `ValidateOnStart()` doesn't always run before first request. Gap: no measurement of which of the other 5 frameworks has similar bugs (Quarkus build-time validation should be bulletproof, Django/Laravel don't claim any validation at all).

5. **Testing with 10+ overrides**: None of the frameworks document how to override 10+ bindings in a single test. The patterns shown (Spring's `ApplicationContextRunner`, NestJS's `.override...`) scale to ~3 overrides before becoming unreadable. Gap: no guidance on "test profile" complexity at scale.

6. **Configuration precedence when code, env, file, and CLI args all exist**: Spring Boot documents the full hierarchy; others are less explicit. Gap: whether varco should enforce a single precedence order (to reduce surprises) or allow different precedence per setting.

---

## Librarian's Note

**The evidence strongly converges on four conventions varco should adopt:**

1. **Explicit module registration, not classpath scanning** (Spring Boot model). Define a `@Singleton` or `@Provider` class per backend in `varco_core` (the interface) and each `varco_backend` (the impl). Do NOT rely on `container.scan("varco_kafka", recursive=True)` to auto-discover them — scanning is too magic for a DI-coupled framework. Instead, make each `varco_X.bootstrap(container)` or `bind_X(container)` call explicit. This trades "zero code in the app" (Spring Boot's strength) for "explicit, greppable registration" (varco's constraint given it does not control the Python classpath). Recommendation: **Document that app code must call `bootstrap()`/`bind_*()` for each backend it needs — do not make these optional or auto-discovered.** Brief 010 covered `@DefaultBean` / `@ConditionalOnMissingBean` precedence; use that model for defaults (a no-op `NullEventBus` or `InMemoryCache` lives in `varco_core` as a scanned `@Singleton`, user code provides a different one, and providify's `@DefaultBean` logic wins automatically).

2. **Unify configuration binding into one verb: `install_*` or `configure_*` ** (consolidating today's `bootstrap()`, `async_bootstrap()`, `bind_*()`, `enable_*()`, `mount_*()`). Right now, varco's pattern taxonomy has 6 verbs for different shapes. Example: `install_cache(container, RedisSettings(...))` could handle both binding and validating config (failing at startup if required keys are missing). This is ASP.NET Core's `services.AddX()` + `app.UseX()` shape — one registration, two phases. Recommendation: **Design a `varco.Module` ABC or `Lifecycle` class that each backend implements, wrapping its `register` + `async_init` + `validate` + `middleware_install` steps into one unit.** Then `create_varco_app(modules=[...])` takes a list of modules, not 15 keyword objects.

3. **Startup validation report as table stakes** (Spring Boot's `--debug` precedent). Add a `varco diagnose` CLI verb or a `VaricoApp.startup_report()` method that prints: (1) which implementation of each ABC is live (event bus, cache, authorizer, etc.), (2) where it came from (builtin, env var, DI override, code config), (3) any deprecations active, (4) which features are disabled. This is standard in mature frameworks; varco is pioneering in Python but should steal the idea. Recommendation: **Build `inspect_*()` functions per seam (already done for `inspect_rls_posture()`, `inspect_revocation_posture()`) and wire them into a startup report.**

4. **Test-profile composition over 15-keyword override** (Quarkus's `@TestProfile` + `getConfigOverrides()` model). Varco's testing pain today: to replace one backend (e.g., cache) in a test, you still construct 14 other Lifecycle objects. Recommendation: **Design a `VaricoTestProfile` ABC with `get_overrides() -> Dict[ABC, Implementation]` and wire it into `create_varco_app(test_profile=...)`.** Then a test module can override just the cache, and defaults provide the rest. This requires that varco's defaults are actually usable in tests (in-memory event bus, in-memory cache, in-memory DLQ, etc.) — which is already mostly true in varco_core.

**For configuration specifics** (binding, validation, precedence):

5. **Settings as pydantic `BaseSettings` with env-var prefix, validated at bootstrap** (following ASP.NET's `IOptions<T>` + `ValidateOnStart()` pattern). Varco already does this for some settings (e.g., `RedisSettings`). Recommendation: **Ensure every backend's settings class is a pydantic `BaseSettings` with `env_prefix="VARCO_<FEATURE>_"`, uses `model_validate()` with `raise_validation_errors=True` in `bootstrap()`, and documents all `VARCO_*` env vars in README.**

6. **Compose settings with feature flags, not conditional imports** (Django's `INSTALLED_APPS` + conditional enable pattern). When a feature is off (e.g., `VARCO_CACHE_ENABLED=false`), the bootstrap should return a no-op impl, not fail. Recommendation: **Each `bootstrap()` or module's `register()` should check a `enabled` property first; if disabled, register a `NullX` (no-op) and return early.** This is safer than raising or returning `None`.

---

**For evidence credibility**: sources are official documentation from Spring, Quarkus, Micronaut, NestJS, Microsoft Learn, Django, and Laravel, plus published GitHub issues (Microsoft ASP.NET #56453 on `ValidateOnStart` timing). No blogs, no best-guess posts. The Python DI ecosystem gap is real — no standard exists. Varco is designing new territory; it may need a "varco module" convention rather than copying one framework exactly.

