# Research 017 — Fallback Binding Evaluation Timing and Diagnostics

Date: 2026-09-12 · Freshness matters: **YES** — DI framework features, timing semantics, and diagnostic tooling evolve with versions.

## Question

When a fallback binding (Quarkus `@DefaultBean`, Spring `@ConditionalOnMissingBean`, ASP.NET `TryAdd*`, Micronaut `@Requires(missingBeans=...)`) is evaluated, at what exact moment does the framework check whether a binding already exists? Does the answer change if a binding arrives later at runtime? What happens when the fallback is also shadowed by an inactive (profile-deactivated) bean, or when multiple fallbacks exist for the same type? How do frameworks tell operators "your default X was not used because Y exists"?

---

## Findings

### 1. Micronaut @Requires(missingBeans=...) Semantics

**Evaluation target (definitions vs instances):**
- [Conditional Beans guide, micronaut-projects/micronaut-core](https://github.com/micronaut-projects/micronaut-core/blob/4.10.x/src/main/docs/guide/ioc/conditionalBeans.adoc) specifies: `@Requires(missingBeans=javax.sql.DataSource)` prevents bean instantiation when the specified bean type exists.
- Micronaut operates at **compile time** — beans are discovered and conditions evaluated during AOT compilation, not runtime. The framework checks against **bean definitions** available in the classpath at build time, not instantiated bean instances.

**Evaluation moment:**
- Conditions are evaluated during the annotation-processing phase (compile time). Once build artifacts are generated, the evaluation cannot change — Micronaut has no runtime bean registration.
- A `@Requires` condition can be inspected at runtime via `BeanContext.findBeanDefinition()` and debug logging (`io.micronaut.context.condition`), but the decision was final at build time.

**Circular dependency hazard:**
- When two beans each declare `@Requires(missingBeans=` the other, Micronaut's compile-time evaluator can encounter a StackOverflowException if the beans also carry preprocessed method annotations (e.g., `@async`, `@NotNull`) — [Issue #9315, micronaut-projects/micronaut-core](https://github.com/micronaut-projects/micronaut-core/issues/9315). The framework does not ship a formal "both conditions fail" tie-breaker; the evaluator can deadlock on preprocessing order.

**varco implication:** If varco targets AOT compilation (GraalVM native), Micronaut's build-time evaluation is the model to follow; fallback bindings must be immutable post-build. If varco stays runtime-only, Micronaut's constraints do not apply. Circular fallback hazards are real and require explicit ordering or a tie-breaker rule (e.g., `@Priority`).

---

### 2. Evaluation Timing: Comparison Table

| Framework | Predicate target | Evaluation moment | Can answer change after later registrations? | Behavior when in a collection (`List<T>`) | Qualifier/name matching |
|---|---|---|---|---|---|
| **Spring @ConditionalOnMissingBean** | Bean definitions only (must be in BeanFactory) | Configuration-class parsing time (early, before app start); auto-configs run last for determinism | NO — the answer was frozen when configuration was parsed; later beans do not retroactively disable the fallback. If a bean *of that type* arrives during parsing (e.g., user `@Configuration` runs first), the fallback never registers. | If fallback is registered, it appears in `InjectionPoint` resolution as if it were a normal bean; if not registered, it is absent from `List<T>`. | `@ConditionalOnMissingBean(name="myBeanName")` checks by name; `@ConditionalOnMissingBean(value=Foo.class)` checks by type. Qualifier-aware only via `name=` (GitHub issue #2834: no built-in `@Qualifier` support). |
| **Quarkus @DefaultBean** | Bean definitions only (build-time artifact) | Build time (ArC code generation phase) | NO — the answer is baked into bytecode. Quarkus does not support runtime bean registration; all beans must be known at build time. | Default bean is excluded from `@All` and `List<T>` resolution if a non-default bean of the same type exists; only appears when truly alone. | Qualifier-aware: `@DefaultBean` is matched by type+qualifier; two `@DefaultBean` beans with different qualifiers are distinct. `@Priority(value)` disambiguates multiple `@DefaultBean` of same type+qualifier. |
| **ASP.NET Core TryAdd*** | ServiceDescriptor objects in the current `IServiceCollection` | Add-call evaluation time (registration order dependent); purely synchronous check against the collection's current state | NO — the check happens at the moment `services.TryAddSingleton<T, Impl>()` is called; if a binding for T is already present, the method silently does nothing. A binding added *after* this call does not retroactively affect the outcome. | If fallback was added (check passed), it appears in `IEnumerable<T>` resolution in insertion order; if check failed, it is absent. | Type-based only; no built-in qualifier support. Multiple registrations of same type are allowed and are collected in `IEnumerable<T>`. |
| **Micronaut @Requires(missingBeans=...)** | Bean definitions at compile time | Compile-time / AOT phase (annotation processing) | NO — Micronaut has no runtime bean registration; the answer is final after build. Attempting runtime registration of a bean that a `@Requires` already evaluated against will produce no effect or errors. | Runtime lookup (`Instance<T>`, `getBean(T.class)`) excludes the bean if its condition was false at build time; no collection operation exists. | Type-based only; no qualifier support in `missingBeans=`. Can check multiple missing types: `@Requires(missingBeans={DataSource.class, EntityManager.class})`. |

**Key finding:** Spring's timing (configuration parsing, auto-config last) and ASP.NET's timing (registration-order at add-call) both allow the answer to freeze early, but Spring at least documents auto-config ordering to ensure determinism. Quarkus and Micronaut both freeze at build time, making the answer completely immutable.

**varco implication:** For a runtime DI container (providify), the fallback must be **evaluated at resolution time** (not registration time), the same moment `@Profile` conditions are checked. This means later registrations can retroactively affect the fallback's visibility, which is intentional — it allows app code to override framework defaults after container setup.

---

### 3. Shadowing Diagnostics: How Frameworks Report "Your Default Was Not Used"

**Spring Boot:**
- [ConditionEvaluationReport and --debug flag](https://docs.spring.io/spring-boot/reference/features/developing-auto-configuration.html): Run with `--debug` or set `logging.level.org.springframework.boot.autoconfigure=DEBUG` to see all auto-configurations evaluated, with their matching status (positive/negative).
- [Spring Boot Actuator `/actuator/conditions` endpoint](https://docs.spring.io/spring-boot/api/rest/actuator/conditions.html): Exposes a `negativeMatches` JSON array listing every condition that failed to match, with the reason (`condition`, `notMatched` details, and `message`).
- **Example field in negative match:** `"notMatched": [ { "condition": "OnMissingBeanCondition", "message": "@ConditionalOnMissingBean (types: com.example.MyService) found beans of type 'com.example.MyService' - class com.example.MyServiceImpl defined in URL [jar:...MyServiceImpl.class]" } ]`.
- This is **on by default** when Actuator is enabled, and the `--debug` output is human-readable console log.

**Quarkus:**
- [ArC development console (`/q/arc`)](https://quarkus.io/guides/cdi-reference/): In dev mode, lists all discovered beans with their status (scanned, produced, synthetic, removed). An `@DefaultBean` that is shadowed by a regular bean is not listed under "removed beans" but appears in the bean list with a note (implementation detail varies by Quarkus version).
- [Removed beans report (`quarkus.arc.unremovable-types`)](https://docs.micronaut.io/latest/api/io/micronaut/context/annotation/Requires.html): If a bean is dynamically removed (not used), you can preserve it with `@Unremovable` or the config property. The `dev UI` shows removals; no automatic "default shadowed" message.
- **No structured condition-evaluation report exists** in Quarkus like Spring's `/actuator/conditions`. The dev UI shows bean discovery results, not the reasons why a fallback was not used.

**Micronaut:**
- [Debug logging for `io.micronaut.context.condition` package](https://github.com/micronaut-projects/micronaut-core/blob/4.10.x/src/main/docs/guide/ioc/conditionalBeans.adoc): Enable `logger.level.io.micronaut.context.condition = DEBUG` to see why beans were not loaded.
- **No built-in REST diagnostic endpoint** (unlike Spring Actuator). No programmatic API like Spring's `ConditionEvaluationReport` to query condition results at runtime.
- **Compile-time diagnostics:** Micronaut can be configured with `micronaut.debug` environment variable to enable extra logging during AOT. The build log will show condition-evaluation decisions, but the format is not standardized and is implementation-specific.
- **No "conditions" admin surface** — Micronaut does not ship a diagnostic page or file listing all conditions and their outcomes.

**ASP.NET Core:**
- **No built-in diagnostics for shadowed `TryAdd*` registrations.** The framework does not log or report when a `TryAdd*` call silently fails (the binding already exists). Operators must inspect `IServiceCollection` programmatically or use third-party tools.
- A custom service diagnostic can be built by iterating `IServiceProvider.GetServices()` and comparing against expected registrations, but this is manual and application-specific.
- **Why:** ASP.NET's DI is minimal by design; the assumption is that developers control the setup code directly (no auto-configuration), so shadowing is visible in source review.

**Python (providify/dependency-injector/etc):**
- No established diagnostic convention yet. Most Python DI libraries either log binding registration (raw output) or provide no diagnostics at all.
- providify's current DI error messages are minimal (typically `ResolutionError: no binding for X`), with no equivalent to Spring's report or Quarkus's dev UI.

**varco implication:** A `varco diagnose` command (or logging at startup) should report: for each fallback binding, whether it was shadowed, by what (the active binding name + type + source), and when it was shadowed (at container-build time or later). This mirrors Spring Boot's `--debug` output but is simpler because varco's runtime resolution is not yet bound to the full condition-evaluation semantics. Quarkus and Micronaut's build-time diagnostics are harder to replicate in varco's runtime model, but the principle applies: the default behavior must be transparent.

---

### 4. Edge Semantics

#### a) Two Fallback Bindings for the Same T, No Non-Fallback Binding

**Spring:**
- `@ConditionalOnMissingBean` does not define tiebreaker behavior for multiple fallbacks. If two auto-config classes each declare `@ConditionalOnMissingBean @Bean public T t()`, both conditions are true (no T exists), and both register. The last one processed wins (takes precedence in `@Autowired` resolution).
- Spring relies on configuration-class ordering (auto-configs run last, and within auto-configs, the file/package order is deterministic) to avoid ambiguity, but the contract does not explicitly forbid double-registration.

**Quarkus:**
- [Multiple `@DefaultBean` with no non-default bean](https://github.com/quarkusio/quarkus/issues/37042): If two `@DefaultBean` methods produce the same type, the one with the highest `@Priority(value)` wins. If both have the same priority, ArC build-time resolution fails with an ambiguity error: `Ambiguous dependencies for type ... and qualifiers [@Default]`.
- The `@Priority` rule is explicit and deterministic; no double-registration is allowed.

**ASP.NET:**
- If `services.TryAddSingleton<T, Impl1>()` and `services.TryAddSingleton<T, Impl2>()` are both called in that order, the first call succeeds and the second silently fails (no error). Only Impl1 is registered.
- No tiebreaker or ambiguity error; registration order is the sole decider.

**Micronaut:**
- If two `@Requires(missingBeans=T)` beans exist and no T is present, the compile-time evaluator can process both (no duplicate-registration error is mandated). The order in which `BeanContext.getBean(T)` resolves one vs. the other is unspecified and may be implementation-dependent.
- A `@Priority` equivalent does not exist; the behavior is under-specified.

**varco implication:** Providify should reject ambiguous fallback-binding resolution with a clear error at container-build time (like Quarkus). If two fallbacks exist, an explicit `@Priority` or disambiguating qualifier must resolve it. Silently picking the first (ASP.NET) or the last (Spring) is confusing and error-prone.

#### b) Fallback Shadowed by an Inactive Bean (Profile-Deactivated)

**Spring:**
- [Conditional and Profile interaction](https://github.com/spring-projects/spring-boot/issues/2897): When a bean has both `@ConditionalOnMissingBean(MyService)` and is in an auto-config, and separately a user defines `@Bean @Profile("dev") MyService`, the behavior depends on whether "dev" is active:
  - If "dev" is inactive, the profile bean does not register, so the fallback sees no conflict and registers.
  - If "dev" is active, the profile bean registers, and the fallback does not.
- **Key finding:** `@ConditionalOnMissingBean` checks bean definitions, including profile-deactivated ones. An inactive bean's definition is still in the BeanFactory; the condition sees it as "bean exists" and the fallback backs off.

**Quarkus:**
- A bean with `@IfBuildProfile("dev")` is **not** included in the build if the profile is not active. Therefore, an `@DefaultBean` evaluates against beans that *will actually run*, not all syntactically defined beans.
- If `@DefaultBean Service` and `@IfBuildProfile("dev") Service` both exist, and "dev" is not active, the default bean registers because the profile bean was excluded at build time.

**Micronaut:**
- [Profile conditions and compile-time evaluation](https://github.com/micronaut-projects/micronaut-core/blob/4.10.x/src/main/docs/guide/ioc/conditionalBeans.adoc): A bean with `@Requires(env="dev")` is omitted from the build if that environment is not the current build environment. An `@Requires(missingBeans=T)` then evaluates against beans *for the current environment*, not all possible beans.
- If `@Requires(missingBeans=T) T fallback` and `@Requires(env="dev") T devImpl` both exist, and env is "prod", the fallback registers (the dev impl is not in the current build).

**varco implication:** At resolution time, a fallback must be shadowed only by bindings that are **actually active** (not by inactive profiles). This is the Quarkus/Micronaut model and is cleaner than Spring's: an inactive binding does not count as "present." Varco's runtime resolution can check active bindings dynamically, making this simpler than Spring's static configuration-class model.

#### c) Interaction with Qualifiers/Named Bindings

**Spring:**
- `@ConditionalOnMissingBean(name="myName")` checks for a bean with that exact bean name.
- `@ConditionalOnMissingBean(value=MyInterface.class)` checks for any bean of that type, ignoring qualifiers/names.
- **No built-in support for qualifier-aware checking** ([GitHub issue #2834](https://github.com/spring-projects/spring-boot/issues/2834)): you cannot write `@ConditionalOnMissingBean(value=MyInterface.class, qualifier=@SomeQualifier)`. This is a documented limitation.

**Quarkus:**
- `@DefaultBean` is **qualifier-aware**: `@DefaultBean @Named("primary") T bean1()` and `@DefaultBean @Named("secondary") T bean2()` are separate bindings. A user bean with `@Named("primary")` shadows only the first, not the second.
- The type+qualifier combination is the identity; two defaults with different qualifiers do not conflict.

**ASP.NET:**
- Qualifiers do not exist in the standard DI model. Keyed services (available in .NET 6+) are registered as `services.TryAddKeyedSingleton<T>(key, impl)` and checked by both type and key, but the semantics are the same as the unkeyed case: the first registration wins, period.

**Micronaut:**
- `@Requires(missingBeans=T)` checks for the type T across all qualifiers. You cannot write a qualifier-specific fallback (e.g., "use this default only if `@Named("foo") T` is missing").
- If any bean of type T exists with any qualifier, the fallback does not register.

**varco implication:** Varco should be **qualifier-aware** for fallback bindings (like Quarkus). A fallback for `AsyncRepository[User]` should not be shadowed by a binding for `AsyncRepository[Post]` (different type parameters). A fallback with a specific qualifier should not be shadowed by a binding with a different qualifier. This is more nuanced than the JVM frameworks because generic type parameters complicate the identity.

---

### 5. Python Precedent: Fallback/Default Provider Primitives in Other DI Libraries

**dependency-injector:**
- [GitHub](https://github.com/ets-labs/python-dependency-injector), mature (~9 years), 4.x stable.
- **No fallback binding primitive.** You can define a factory that returns a default, but there is no declarative "use this only if nothing else is registered" mechanism.
- Default behavior: if you define two bindings for the same type, the second overwrites the first (registration order).

**dishka:**
- [GitHub](https://github.com/reagento/dishka), newer, active 2024-2026.
- [Alternatives feature](https://dishka.readthedocs.io/en/stable/alternatives.html) allows conditional binding based on runtime context, but this is not a fallback pattern; it is a strategy pattern (pick Impl A or B at resolution time based on a predicate).
- **No fallback binding.**

**lagom, punq, injector, wireup, rodi, that-depends:**
- Searched multiple documentation pages and GitHub repos; **none ship a "register only if missing" or "@DefaultBean-style" primitive.**
- All assume registration-order semantics (last wins, or first wins depending on the library).

**FastAPI's Depends:**
- Not a DI container in the formal sense; it is a routing-level dependency injector for request parameters.
- No fallback semantics.

**Conclusion:** **No Python DI library has a mature fallback/default-bean primitive as of September 2026.** This is a feature gap in the Python ecosystem, which is why varco (using providify) would be a pioneer in bringing this pattern to Python's runtime DI story.

**varco implication:** Varco's adoption of a fallback binding feature would be distinctive in Python. The design should borrow from Quarkus (build-time explicit, `@Priority` for tiebreaker, qualifier-aware) but adapt it to runtime (lazily evaluated at resolution time). Providify should define the primitive (e.g., `@DefaultProvider`, `@Fallback`); varco's `bootstrap()` and `enable_*()` helpers wire the seams.

---

## Version/Compatibility Notes

- **Spring Boot:** `@ConditionalOnMissingBean` stable since 1.0.0 (2013); behavior unchanged through 4.1 (2025). Actuator `/actuator/conditions` present since 1.5.0 (2015).
- **Quarkus:** `@DefaultBean` stable from 2.13.0 LTS (2021); `@Priority` support documented in 3.x. No planned deprecation.
- **ASP.NET Core:** `TryAdd*` methods stable since 2.0 (2017); behavior unchanged through .NET 10 (2026).
- **Micronaut:** `@Requires(missingBeans=...)` present from early versions (1.x); known issue #9315 (circular fallback with method annotations) unresolved as of 4.10.x (2024).
- **providify:** No fallback binding feature as of 2.0.0 (2024); this would be a new feature request.

---

## Evidence Gaps

1. **Spring's handling of profile-deactivated beans in `@ConditionalOnMissingBean` resolution**: The GitHub issue #2897 shows the observed behavior (inactive bean counts as "present"), but Spring's documentation does not formally specify this; it is inferred from test cases. Clarification needed: does Spring's BeanFactory include or exclude profile-deactivated bean definitions?

2. **Quarkus `@DefaultBean` interaction with `@All` and `Instance<T>` (multi-bean injection patterns)**: The CDI spec defines how alternatives behave in list/all injections, but Quarkus's ArC may have deviations. Unclear whether a shadowed default bean is completely absent from `Instance` lookups or is still discoverable via iteration.

3. **Micronaut circular fallback prevention**: The known StackOverflowException (#9315) suggests the framework does not detect or prevent cycles in `@Requires(missingBeans=...)` conditions. No documented mitigation or error message exists.

4. **ASP.NET Core's DI diagnostics roadmap**: No public issue or design proposal found for adding shadowed-registration diagnostics. Unclear if this is intentional (out-of-scope for minimal DI) or a gap.

5. **Python DI library fallback patterns in active use**: The absence of a fallback primitive in Python libraries may be deliberate (no known use case) or accidental (gap no one has filed). Requires a survey of real-world Python dependency injection deployments to confirm whether this pattern is needed.

---

## Librarian's Note

**The core tradeoff: static (build-time) vs dynamic (runtime) fallback evaluation:**

1. **Build-time (Quarkus, Micronaut)** gives determinism — the answer never changes after compilation. Drawback: no runtime configuration of defaults; all choices are final at build time.

2. **Runtime (Spring, ASP.NET, varco's proposed model)** allows later configuration to override defaults (more flexible). Drawback: ordering hazards (Spring's `@ConditionalOnMissingBean` is order-dependent; ASP.NET's `TryAdd*` freezes at add-call time, not resolution).

**For varco:** The evidence favors **resolution-time evaluation** (like `@Profile` conditions in providify). When a binding is resolved, check whether any non-fallback binding of that type (same qualifier, active, not a fallback) exists. If yes, skip the fallback. If no, use it. This gives Spring's flexibility + Quarkus's explicitness (fallback is declared `@Fallback`, not hidden in a `@Provider` default) + qualifier awareness + profile awareness. The diagnostics (shadowing report) would be built into a `varco diagnose` command, mirroring Spring Boot's `--debug`.

**Edge cases to specify in the ask:**
1. Two fallbacks for the same T, no non-fallback: require `@Priority` to disambiguate, fail at container-build time if ambiguous (Quarkus model).
2. Fallback shadowed by inactive binding: count as "present" only if active (Quarkus/Micronaut, not Spring).
3. Qualifier-aware shadowing (distinct fallbacks for different qualifiers, like Quarkus).
4. Fallback in a collection (`List<T>`, `Set<T>`): if shadowed, exclude from the collection entirely (not included at all).

**Sources reviewed:** official Javadocs (Spring, ASP.NET), GitHub repositories (Quarkus, Micronaut), documentation (Quarkus, Spring Boot, Django for comparison), and GitHub issues/discussions showing real pain points (circular fallbacks, ambiguity with profiles, qualifier mismatch). No published case studies exist on Python DI fallback patterns, making this research an evidence gap.
