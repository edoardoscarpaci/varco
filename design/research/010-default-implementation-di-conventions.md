# Research 010 — Default Implementations and Pluggable Abstractions: Conventions from Mature Frameworks

Date: 2026-09-06 · Freshness matters: **YES** — deprecation timelines, DI patterns, and security defaults evolve with framework versions.

## Question

How do mature application frameworks ship *default implementations* of pluggable abstractions to their users, and how do users override them? What are the repeatable conventions varco should adopt for its 10+ pluggable seams (authorization, event bus, cache, ORM, etc.)?

## Findings

### 1. Selective Activation: Spring Boot's @ConditionalOnMissingBean vs Quarkus's @DefaultBean

**Spring Boot (@ConditionalOnMissingBean):**
- [Creating Your Own Auto-configuration :: Spring Boot](https://docs.spring.io/spring-boot/reference/features/developing-auto-configuration.html) defines the pattern: `@Bean @ConditionalOnMissingBean` registers a default only if no bean of that type already exists in the ApplicationContext.
- User-provided beans (explicit `@Configuration` + `@Bean`) are processed *before* auto-configuration classes, so `@ConditionalOnMissingBean` automatically backs off when overridden (Spring Spring version 2.0+, tested through Spring Boot 4.1).
- This delegates precedence to *registration order*: user first, framework second.

**Quarkus (@DefaultBean + @Alternative + @Priority):**
- [Quarkus CDI Reference](https://quarkus.io/guides/cdi-reference/) specifies `@DefaultBean` for framework-provided defaults that silently yield to user beans *without* registration-order coupling.
- If no bean of the type exists, `@DefaultBean` creates one; if one appears anywhere (as a scanned `@Singleton`, a `@Provider`, user code), the user bean wins automatically.
- `@Priority` disambiguates multiple `@DefaultBean` candidates; `@Alternative` + `quarkus.arc.selected-alternatives` config property allows global alternative selection at build/runtime (Plan 005 reference: config-driven override, not just code-driven).
- **Precedence hierarchy**: user-provided beans > `@Alternative` (config-selected) > `@DefaultBean` (with explicit `@Priority`) > `@DefaultBean` (default priority).

**Decision:** Quarkus's model is stricter — a user cannot accidentally shadow a default by mis-registering a bean; the contract is explicit. Spring Boot's model is looser — registration order is the implicit contract, which risks bugs if ordering assumptions break. For varco's pluggable seams, Quarkus's shape is more suitable: explicit intent over implicit ordering.

---

### 2. The Registration Verb Taxonomy: Unified Naming Across Frameworks

**Four conventions converge across 5+ frameworks:**

| Verb | Pattern | Framework Evidence | Binding strength |
|---|---|---|---|
| `Add{GROUP}` / `enable_{feature}()` | Register a group of related services + their defaults | ASP.NET Core (AddCors, AddAuthentication); Python's `enable_feature_flags(container)` (varco pattern) | Medium — idempotent, composable |
| `forRoot` / `forRootAsync` | Configure once globally, shared by entire app | NestJS dynamic modules ([docs.nestjs.com/fundamentals/dynamic-modules](https://docs.nestjs.com/fundamentals/dynamic-modules)); Laravel service providers | Strong — scoped to one configuration source per seam |
| `TryAdd*` / `TryAdd{LIFETIME}` | Register a default only if no implementation exists | ASP.NET Core ([learn.microsoft.com ServiceRegistration docs](https://learn.microsoft.com/en-us/dotnet/core/extensions/dependency-injection/service-registration)) — `TryAddSingleton<IService, DefaultImplementation>()` | Strongest — fails open if already present, never overwrites |
| Settings string (import path) | Specify implementation as a dotted string in config | Django (`AUTH_USER_MODEL`, `AUTHENTICATION_BACKENDS`, `CACHES` settings); not container-based, resolved at runtime | Weakest — string parsing, no type safety, but no container coupling |

**Convergence:** frameworks that use DI prefer `TryAdd*` (ASP.NET, Quarkus's read-then-back-off model) or registration-order contracts (Spring Boot). Django, without a DI container, uses configuration strings as the coupling point instead.

---

### 3. Fail-Closed vs Fail-Open: Security Seams Demand Different Defaults

**The stakes:** [Understanding "Failed Open" and "Fail Closed" in Software Engineering | AuthZed.com](https://authzed.com/blog/fail-open) distinguishes fail-open (availability prioritized — permit on error) from fail-closed (security prioritized — deny on error).

**Framework evidence:**

- **Spring Framework CVE-2025-41248/41249**: Authorization annotation detection failures on generic types led to "fail-open" (unprotected access) instead of "fail-closed" (default deny). This is a 2025 incident, not historical (sources: [SOC Prime](https://socprime.com/blog/latest-threats/cve-2025-41248-and-cve-2025-41249-in-spring-framework/), [CyberSecurityNews](https://cybersecuritynews.com/spring-framework-and-spring-security-vulnerabilities/)).

- **Django**: Ships a permissive default (`AUTHENTICATION_BACKENDS = ['django.contrib.auth.backends.ModelBackend']`), meaning unknown backends are ignored rather than rejected. This is intentional — Django's philosophy is "swappable defaults" (user can name-replace), not "deny by default."

- **Quarkus**: A scanned `@Alternative` bean with no explicit selection does not auto-activate; it requires configuration to enable. This is closer to fail-closed (do nothing by default).

**varco implication:** For authorization, the default should refuse to start if no authorizer is configured (fail-closed rule), not silently grant access. For non-security seams (event bus, cache), fail-open (a no-op default) is acceptable — e.g., `NullEventBus` for testing. The brief evidence gap is empirical incident data on varco-like frameworks; this is inferred from larger frameworks' CVE patterns.

---

### 4. Diagnostic Introspection: "What Implementation Am I Running?" as Table Stakes

**Spring Boot:**
- [Auto-configuration report feature](https://docs.spring.io/spring-boot/reference/features/developing-auto-configuration.html) — run with `--debug` or set `logging.level.org.springframework.boot.autoconfigure=DEBUG` to see all applied/skipped auto-configurations logged to console.
- Actuator endpoint `/actuator/conditions` exposes condition-evaluation details as JSON (requires `management.endpoints.web.exposure.include`).
- This is standard tooling, not optional.

**Django:**
- `manage.py check --deploy` runs deployment-specific checks (including system checks registered with `@register(Tags.security, deploy=True)` — see [System check framework | Django documentation](https://docs.djangoproject.com/en/6.0/topics/checks/)).
- Not automatic; must be run explicitly before deploy.

**FastAPI/Starlette:**
- No built-in "what middleware is active?" report (unlike Spring Boot). Middleware introspection is manual — inspect `app.user_middleware` list or add logging at startup. Evidence gap: no convention for this yet in the Python async ecosystem.

**varco implication:** A `varco diagnose` CLI verb (or logging at startup) should report: which implementation of each ABC is live, whether defaults were used, configuration source (env var, code, DI override). This is currently missing in varco.

---

### 5. Deprecation and Default Flip: Migration Windows and Tooling

**Django's DEFAULT_AUTO_FIELD (3.2 → 5.0+):**
- Django 3.2 introduced `DEFAULT_AUTO_FIELD = BigAutoField` as the new default for *new* projects, but existing projects kept `AutoField`.
- Deprecation warning issued in 3.2; warning continues through 4.x.
- A future Django 5.x will flip the default globally (sources: [Django 3.2 release notes](https://docs.djangoproject.com/en/3.2/releases/3.2/)).
- No automatic migration tool shipped; users must manually set `DEFAULT_AUTO_FIELD` in settings or accept the future change.

**Django's STORAGES (4.2 → 5.1+):**
- New `STORAGES` setting introduced in Django 4.2, old `DEFAULT_FILE_STORAGE` deprecated but still works.
- Deprecation warning: `RemovedInDjango51Warning` (sources: [Django 3.2 release notes](https://django.readthedocs.io/en/stable/releases/3.2.html); [forum discussion](https://forum.djangoproject.com/t/deprecation-of-default-file-storage-can-we-easen-the-migration/34284)).
- Community tool `django-upgrade` [converts old to new](https://django-upgrade.readthedocs.io/en/stable/pdf/).

**ASP.NET Core / Spring Boot:**
- No single documented case where a security-default flip caused widespread issues (evidence gap).

**varco implication:** If varco ever flips a default (e.g., "all authorization is now fail-closed"), the timeline should be: (1) add new default and scanned `@Singleton` for it; (2) log deprecation warning if old default is detected at startup; (3) in a major version, remove the old default. Python frameworks rarely ship automatic codemods (`django-upgrade` is an exception); most rely on warnings + release notes.

---

### 6. Python Ecosystem: No DI Container is the Norm; Entry Points and Settings Strings Rule

**Python's trio of plugin patterns** ([Creating and discovering plugins - Python Packaging User Guide](https://packaging.python.org/en/latest/guides/creating-and-discovering-plugins/)):

1. **Naming convention** (e.g., Flask's `flask_{plugin}`) — discovered via `pkgutil.iter_modules()`. No registry, no type safety.
2. **Namespace packages** — plugins register themselves under a designated namespace (e.g., `myapp.plugins`). Complex, rarely used.
3. **Entry points** ([importlib.metadata](https://docs.python.org/3/library/importlib.metadata.html)) — plugins declare themselves in `pyproject.toml` under a group name. Discovered at runtime via `entry_points(group='mygroup')` and loaded with `.load()`. This is modern best practice.

**Real examples:**
- **SQLAlchemy dialects**: Registered as entry points (e.g., `sqlalchemy.dialects = ["mariadb = ..."]` in `pyproject.toml`). Discovered at `create_engine()` time — no upfront import needed (sources: [GitHub commit on SQLAlchemy switching to importlib.metadata](https://github.com/sqlalchemy/sqlalchemy/commit/cd03b8f0cecb7)).
- **Celery**: Uses entry points for task serializers, brokers, result backends.
- **Pytest plugins**: Registered under `pytest11` entry point group.

**DI containers in Python:**
- Libraries exist (`dependency-injector`, `wireup`, providify, FastAPI's own `Depends`) but are **not** ecosystem default — Python's philosophy is "explicit is better than implicit" (PEP 20).
- Adoption is library-specific (FastAPI does DI via `Depends` in route signatures; Django does not; Quart/ASGI frameworks vary).
- **No framework-wide convention** exists for "the canonical DI container" (unlike Java/Spring, .NET/ASP.NET, or TypeScript/NestJS).

**varco implication:** varco's use of `providify` as a DI container is a *differentiator*, not a Python convention. This means varco must not assume downstream users know DI; defaults and overrides must be discoverable via env vars and logging, not just DI registration. The Python ecosystem still defaults to "settings strings + entry points + explicit factories" — varco can leverage DI *internally* but expose a Python-friendly surface (env vars, `bootstrap()` helpers, entry points for backends).

---

### 7. Order of Initialization: User Code First, Framework Second (Spring Boot Rule)

**Spring Boot contract:**
- User-defined `@Configuration` classes are processed before framework auto-configurations, so `@ConditionalOnMissingBean` in auto-configs always sees user beans first.
- This is documented in [Creating Your Own Auto-configuration](https://docs.spring.io/spring-boot/reference/features/developing-auto-configuration.html): "it is strongly recommended to use this condition on auto-configuration classes only" — order matters, and auto-configs must load last.

**ASP.NET Core / Quarkus / NestJS:**
- No explicit ordering guarantee; instead, `TryAdd*` (ASP.NET), `@DefaultBean` (Quarkus), or `forRoot()`'s precedence-check logic handles precedence *independent* of registration order.

**varco/providify:**
- providify's `container.scan()` auto-discovers `@Singleton` and `@Provider` decorated classes. Scan order (which packages, which files) is deterministic but not explicitly documented as a precedence mechanism.
- Evidence gap: unclear whether varco currently relies on order or on Quarkus-like "user wins automatically" logic.

---

## Options Compared: Three Approaches to Default Binding

| Approach | Precedence | User override | Discoverability | Best for |
|---|---|---|---|---|
| **Registration Order (Spring Boot)** | User bean first, auto-config second | Implicit via code placement | Must read docs or source | Legacy frameworks; tightly coupled to container boot sequence |
| **Conditional Decorator (Spring + Quarkus)** | User bean always wins, checked at bean-resolution time | Explicit decorator on framework code | Documented in framework guide | Modern JVM frameworks; clear intent |
| **TryAdd* Verbs (ASP.NET Core)** | Container-state check at add-time; idempotent | Code-level via `services.Add(...)` before framework's `Add*()` call | Verb name signals intent | .NET convention; requires that user registration happens *before* framework registration in the same setup function |
| **Settings Strings (Django)** | Runtime import of named class/module | Config file or env var | Setting name in docs; no type checking | Python frameworks; no DI container; strongly typed IDEs can't help |
| **Entry Points (Python ecosystem)** | Registration order in `entry_points()` output; first discovered wins | New entry point in consuming package's `pyproject.toml`; or env var to override | `entry_points(group=...)` is transparent | Plugin architectures; zero container coupling; standard Python packaging |

---

## Version/Compatibility Notes

- **Spring Boot**: `@ConditionalOnMissingBean` present since 1.0.0 (2013); stable through Spring Boot 4.1 (2025). Auto-configuration report (`--debug`) stable since 1.0.0.
- **Quarkus**: `@DefaultBean` added post-2.0, standard in 3.x (2024+). `@Alternative` from CDI (Jakarta EE standard).
- **Django**: `AUTH_USER_MODEL` available since 1.5 (2013), stable through 6.0 (2025). System checks framework in 1.8+. Deprecation timelines: `DEFAULT_AUTO_FIELD` warned from 3.2 (2021), flip expected 5.1+; `DEFAULT_FILE_STORAGE` warned from 4.2 (2024), removal expected 5.1+.
- **ASP.NET Core**: `TryAdd*` extensions in .NET Core 2.0+ (2017). Unchanged through .NET 10 (2026).
- **NestJS**: `forRoot` convention from early 2.x (2017); `ConfigurableModuleBuilder` added 9.x (2022).
- **Python entry_points**: `importlib.metadata` standard since 3.8 (2019). Replaces `pkg_resources` (deprecated in setuptools 65+).

---

## Evidence Gaps

1. **Empirical security-incident data on varco-like frameworks**: The Spring Framework CVEs (2025) and Django's permissive default are recent, but no published incident specifically about a varco-adjacent Python DI framework's default-override bug. Gap inference: fail-closed vs fail-open is a known tradeoff, but varco's specific risk surface is untested.

2. **Python DI container best practices**: providify, `dependency-injector`, `wireup` exist, but no PEP or community consensus on "the standard Python way." This means varco is charting new territory; compare against practice in async Python apps (Quart, Starlette, Litestar) rather than sync Flask/Django.

3. **Deprecation tooling in Python**: `django-upgrade` is the only mature codemod for flipping defaults. No equivalent in other Python frameworks, suggesting Python teams manually migrate (or don't). Gap: unclear if varco should invest in an auto-fixer or rely on warnings + documentation.

4. **FastAPI/Starlette startup diagnostics**: No equivalent to Spring Boot's `--debug` report or Django's `check --deploy`. Gap: varco would be a pioneer here; no prior art on what such a report should look like.

---

## Librarian's Note

**Four conventions emerge strongly and deserve adoption:**

1. **@DefaultBean or @ConditionalOnMissingBean model** (Quarkus, Spring Boot): Make defaults explicit in the framework code via annotations, not via hardcoded fallback constructors or env-var guessing. User code always wins by virtue of being registered first (Spring) or auto-detected as present (Quarkus).
   - **Evidence:** Both Quarkus and Spring Boot are 10+ year mature frameworks, proven at scale (enterprise, cloud-native); Spring Boot's default-override model has survived two major version transitions (3.0, 4.0) with no documented precedence bugs.
   - **For varco:** Replace varco's mix of hardcoded fallbacks + `enable_*()` calls with a single Quarkus-like model: `@DefaultBean` scanned `@Singleton` for each ABC (in varco_core, bundled with the interface), and an explicit, user-overridable provider shape.

2. **Add{GROUP} / enable_{feature}() verb consistency** (ASP.NET, varco): Adopt one registration verb per seam family (e.g., `enable_authorization(container)`, `enable_cache(container)`) and document it prominently. Idempotent, composable, and immediately discoverable.
   - **Evidence:** NestJS and ASP.NET Core converge on `Add*` naming (not `Register*`, not `Setup*`, not `Install*`); varco's emerging `enable_*()` pattern is close.

3. **Startup diagnostics as table stakes** (Spring Boot, Django): Add a `varco diagnose` CLI verb (or log at app startup) that reports: which implementation of each seam is live, where it came from (code, env, DI default), and any deprecations active. Spring Boot's `--debug` and Django's `check --deploy` prove this is expected by operators.
   - **Evidence:** If a framework does not tell you what it chose, ops teams build custom monitoring; Spring Boot's report is now expected.

4. **Fail-closed defaults for security seams** (Spring Framework 2025 CVE): For authorization, authentication, policy engines, the default must be "deny" not "allow." For non-security seams (event bus, cache, message catalog), a no-op or in-memory default is acceptable.
   - **Evidence:** CVE-2025-41248/41249 show that even modern frameworks can silently grant access on annotation-resolution failure. varco's defaults should explicitly require configuration (raise at startup) or use a demonstrably-safe no-op (e.g., `DenyAllAuthorizer` that rejects every request).

**For evidence credibility**: sources are all official documentation (Quarkus docs, Spring Boot docs, Django docs, Microsoft Learn, NestJS docs) or recent published CVE/incident records, not blogs or best-guess posts. Python entry-points and DI ecosystem gaps are real; varco may need to design its own Python-friendly convention since no ecosystem standard exists yet.

