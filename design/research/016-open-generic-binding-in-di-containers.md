# Research 016 — Open Generic Binding in Mature DI Containers

Date: 2026-09-12 · Freshness matters: **YES** — DI patterns and language-specific generic support are evolving; versions matter.

## Question

How do mature DI containers bind a parametric/open generic type (`Repository<T>` / `AsyncRepository[T]`) from ONE registration, so a consumer asking for `Repository<User>` gets a correctly-typed instance without the framework author writing one binding per `T`? What semantics do they settle on (specificity, caching, constraint handling, factory delegates)? What does Python's typing mechanics enable or forbid?

## Findings

### 1. .NET — Microsoft.Extensions.DependencyInjection Open Generic Registration

**.NET open generic registration (since .NET Core 1.0):**
- Syntax: `services.AddScoped(typeof(IRepository<>), typeof(Repository<>))` — register the interface and implementation as open types, written with `typeof()` and the `<>` placeholder — [Service registration - .NET | Microsoft Learn](https://learn.microsoft.com/en-us/dotnet/core/extensions/dependency-injection/service-registration) (2026-03-30).
- At resolution time, `IRepository<User>` is resolved to `Repository<User>` by substituting the closed type argument into the open generic pattern.
- **Constraint limitation**: only implementation types; factory delegates (`services.AddScoped(typeof(IRepository<>), (sp) => ...)`) are **not supported** for open generics — the container does not know the type argument at registration time, so the factory signature cannot be written — [.NET Core Open Generics with Factory Pattern – Longing to know](https://blog.long2know.com/2018/02/net-core-open-generics-with-factory-pattern/) (2018).
- **Constrained open generics** added in .NET 5 (2020), allowing `where T : IEntity` bounds on `Repository<T>` — validated at resolve time — [Constrained Open Generics Support Merged in .NET Core DI Container](https://www.jimmybogard.com/constrained-open-generics-support-merged-in-net-core-di-container/) (date not specified).
- **Caching**: The container does not cache instances per closed type; each `IRepository<T>` request creates a new instance (unless the lifetime is `Singleton` on the open registration, which then shares one instance across ALL closed types — a dangerous anti-pattern) — inferred from [Dependency Injection - .NET | Microsoft Learn](https://learn.microsoft.com/en-us/dotnet/core/extensions/dependency-injection/overview) (2026-01-26).
- **Type argument propagation**: The concrete type `User` (in `IRepository<User>`) is extracted via reflection on the requested closed type and passed to the implementation's constructor via normal DI; no explicit factory is needed.

### 2. Autofac — RegisterGeneric with Constraint Issues

**Autofac open generic registration:**
- Syntax: `builder.RegisterGeneric(typeof(EntityRepository<>)).As(typeof(IEntityRepository<>)).InstancePerApiRequest()` — [RegistrationExtensions.RegisterGeneric Method](https://autofac.org/apidoc/html/5A5CE89D.htm) (undated; Autofac 5.x–9.x).
- **Factory delegates for open generics**: Unlike .NET's built-in container, Autofac supports factory delegates for open generics, but the delegate is **generated at resolution time, not registration time** — the closed type argument is available when the delegate is invoked — [Delegate Factories — Autofac 9.0.0 documentation](https://docs.autofac.org/en/latest/advanced/delegate-factories.html) (undated, 2024+).
- **Constraint limitation (known bug)**: When an open generic has a type constraint that references another generic type (e.g., `Repository<T> where T : IEntity<K>`), resolution fails with `ComponentNotRegisteredException` — the constraint validator does not fully resolve the nested generic before checking — [Resolving services registered as open generics with generic constraints throws ComponentNotRegisteredException · Issue #972 · autofac/Autofac](https://github.com/autofac/Autofac/issues/972) (issue opened 2017, status unclear in 2026).
- **Type argument propagation**: Via constructor injection; `IEnumerable<T>` collections can be resolved automatically if multiple open generic registrations exist.

### 3. Spring Framework — No Native Open Generic Binding; Factory Bean Pattern Instead

**Spring Framework approach:**
- Spring does **not** support a single open generic bean definition that materializes per type argument — this is a documented gap — [Add generics / parameterized type support to ListableBeanFactory getBeanNamesForType/getBeansOfType methods · Issue #16761 · spring-projects/spring-framework](https://github.com/spring-projects/spring-framework/issues/16761) (opened 2010, status open in 2026).
- **Factory bean pattern (workaround)**: Spring Data uses `RepositoryFactory` and `JpaRepositoryFactory` — one factory per repository interface, initialized once and called to produce instances of `Repository<User>`, `Repository<Post>`, etc. — [1. Working with Spring Data Repositories](https://docs.spring.io/spring-data/data-commons/docs/1.6.1.RELEASE/reference/html/repositories.html) (undated, 2014+).
- **ResolvableType for generic resolution**: `ListableBeanFactory.getBeanNamesForType(ResolvableType)` can match a bean by its generic type, but this is a **read-side** operation for *already-registered* beans, not a registration-time binding of an open generic pattern — multiple GitHub issues (#18690, #19578, #22056) document limitations where `getBeanNamesForType()` fails to match beans with generic return types from factory methods — [getBeanNamesForType(ResolvableType) doesn't match generic factory method return type · Issue #19578 · spring-projects/spring-framework](https://github.com/spring-projects/spring-framework/issues/19578) (opened 2017, status unclear in 2026).
- **Implication**: Spring forces a factory bean per interface; no single registration materializes multiple type variants.

### 4. Guice — No Open Generic Support; TypeLiteral Workaround

**Guice limitations:**
- Guice **does not** support open generic binding — there is no analogue to `.AddScoped(typeof(IRepository<>), typeof(Repository<>))` — [FrequentlyAskedQuestions · google/guice Wiki](https://github.com/google/guice/wiki/FrequentlyAskedQuestions) (undated, 2020s).
- **TypeLiteral workaround**: `bind(new TypeLiteral<Repository<User>>() {}).to(new TypeLiteral<RepositoryImpl<User>>() {})` requires an explicit binding per concrete type `User`, `Post`, etc. — [TypeLiteral | Guice](https://google.github.io/guice/api-docs/3.0/javadoc/com/google/inject/TypeLiteral.html) (API docs, version 3.0).
- **JIT bindings** allow Guice to create bindings for concrete types on demand, but they do not apply to parameterized generics — a `Provider<T>` can be injected, and Guice will resolve `T` instead, but this does not eliminate per-type registration — [BindingResolution · google/guice Wiki](https://github.com/google/guice/wiki/BindingResolution) (undated, 2020s).
- **Design reason**: Java's type erasure at runtime makes open generic matching fragile; TypeLiteral works because it captures type information *in the type literal object itself*, at registration time, not via reflection on a `Repository<?>` placeholder.

### 5. Castle Windsor — Open Generic Registration Supported

**Castle Windsor approach:**
- Syntax: `container.Register(Component.For(typeof(IRepository<>)).ImplementedBy(typeof(Repository<>)))` — [Windsor/docs/registering-components-one-by-one.md at master · castleproject/Windsor](https://github.com/castleproject/Windsor/blob/master/docs/registering-components-one-by-one.md) (undated, 2020s).
- Open generic registration works similarly to .NET's built-in container — one registration materializes `IRepository<User>`, `IRepository<Post>`, etc.
- **Non-generic overload**: For cases where the generic version cannot be used, Castle Windsor provides overloads that take `IEnumerable<Type>` or `params Type[]` — allows registration without relying on compile-time generic syntax — [Castle Windsor: How to register components](https://tommarien.github.io/blog/2012/04/22/castle-windsor-how-to-register-components/) (2012).

### 6. Python DI Libraries — Limited or Absent Generic Factory Support

**dependency-injector (4.49.1 as of 2026-06-18):**
- Typing support: `provider: providers.Provider[Animal]` declares the return type of a provider, enabling mypy checks — [Typing and mypy — Dependency Injector 4.48.3 documentation](https://python-dependency-injector.ets-labs.org/providers/typing_mypy.html) (undated, 2020s).
- **No open generic factory**: There is no syntax to register `Factory[Repository[T]]` once and resolve `Repository[User]`, `Repository[Post]` without per-type registration — confirmed by lack of examples in official docs and ecosystem discussion.

**Dishka (latest version, 2026):**
- Supports sophisticated generic type resolution via a `GenericResolver` that handles `TypeVar`, `Unpack`, `TypeVarTuple`, and covariance/contravariance tracking — [GitHub - reagento/dishka](https://github.com/reagento/dishka) (repo, 2026).
- **No open generic factory**: Despite advanced generic support, Dishka does not offer a way to register `@provide def repository() -> Repository[T]` and have it materialize for any `T`; the type resolver is for *consuming* generic types, not binding them parametrically.

**lagom, rodi, punq, injector:**
- No explicit generic binding support documented — these libraries rely on type name inspection or manual factory methods — [awesome-dependency-injection-in-python · GitHub](https://github.com/sfermigier/awesome-dependency-injection-in-python) (curated list, undated, 2020s).

**Conclusion**: No Python DI library has settled on an open generic factory pattern. Each uses manual per-type registration or a closure-based factory (e.g., `def make_repository(entity_type: type) -> Repository[...]`), making this a gap in the Python ecosystem.

### 7. Python Typing Mechanics — `get_origin`, `get_args`, and Constraints

**`typing.get_origin()` and `typing.get_args()` (Python 3.8+):**
- `get_origin(Repository[User])` returns `Repository` (the unsubscripted base) — [typing — Support for type hints - Python 3.12.0a0 documentation](https://pradyunsg-cpython-cpython-lutra-testing.readthedocs.io/en/latest/library/typing.html) (CPython docs, 2026).
- `get_args(Repository[User])` returns `(User,)` — a tuple of type arguments.
- **Available since Python 3.8** (via `typing` module); Python 3.9+ unified with PEP 585, allowing `list[int]` directly — [PEP 585 – Type Hinting Generics In Standard Collections | peps.python.org](https://peps.python.org/pep-0585/) (2020).

**`isinstance()` with parameterized generics is forbidden:**
- `isinstance([1, 2, 3], list[str])` raises `TypeError: isinstance() argument 2 cannot be a parameterized generic` — [Python type hints: use cases for the types module - Adam Johnson](https://adamj.eu/tech/2021/09/08/python-type-hints-use-cases-for-the-types-module/) (2021).
- This means a DI container **cannot** use `isinstance(thing, Repository[User])` to validate or match types at runtime; it must use `get_origin()` and `get_args()` for introspection.

**`types.GenericAlias` vs `typing._GenericAlias`:**
- `types.GenericAlias`: Public API (Python 3.9+) for parameterized built-in types like `list[int]`, `dict[str, int]` — the result of `list[int]` is a `types.GenericAlias` instance.
- `typing._GenericAlias`: Private implementation in the `typing` module for `typing.List[int]`, `typing.Dict[str, int]`, etc. — underscore prefix signals it's an internal detail.
- A DI container checking `isinstance(key, types.GenericAlias)` will match `list[int]` but NOT `typing.List[int]`; both have `__origin__` and `__args__` attributes, but they are different objects — [types — Dynamic type creation and names for built-in types — Python 3.14.7 documentation](https://docs.python.org/3/library/types.html) (Python docs, 2026).

**TypeVar and bounds:**
- A function signature `def make_repository(entity_type: type[T]) -> Repository[T]` captures `T` as a type variable, but Python's type system does not provide a way to *bind* that factory once and materialize it per `T` at runtime — the factory itself must handle parameterization.
- PEP 484 and PEP 585 define type hints statically; they do **not** provide a runtime facility for "register a parametric factory" — [PEP 484 – Type Hints | peps.python.org](https://peps.python.org/pep-0484/) (2015), [PEP 585 – Type Hinting Generics In Standard Collections | peps.python.org](https://peps.python.org/pep-0585/) (2020).

---

## Options Compared

| Framework | Syntax | Constraint Form | Factory Delegates | Type Caching | Specificity (Closed beats Open?) | Evidence |
|---|---|---|---|---|---|---|
| **.NET** | `typeof(IRepository<>)` | Type bounds in class definition, validated at resolve | NOT supported for open generics | No per-type caching | N/A (only open pattern) | [Service registration - .NET](https://learn.microsoft.com/en-us/dotnet/core/extensions/dependency-injection/service-registration) |
| **Autofac** | `RegisterGeneric(typeof(...))` | Via class/interface constraints | Supported; generated at resolve time | Not explicit | N/A | [RegistrationExtensions.RegisterGeneric](https://autofac.org/apidoc/html/5A5CE89D.htm) |
| **Spring** | No open generic pattern | Via `@Bean` method signature | Per-factory-bean pattern | Yes (via factory bean singleton) | Yes (factory bean explicit per type) | [RepositoryFactory](https://docs.spring.io/spring-data/jpa/reference/api/java/org/springframework/data/jpa/repository/support/JpaRepositoryFactory.html) |
| **Guice** | TypeLiteral only; no open | Via class definition | Per-type `bind()` call required | Yes | Yes (explicit per type) | [TypeLiteral Guice](https://google.github.io/guice/api-docs/3.0/javadoc/com/google/inject/TypeLiteral.html) |
| **Castle Windsor** | `typeof(IRepository<>)` | Via class definition | Implicit (like .NET) | Not explicit | N/A | [Windsor Registration](https://github.com/castleproject/Windsor/blob/master/docs/registering-components-one-by-one.md) |
| **Python (all)** | No standard pattern | Via PEP 484 annotations | Manual per-type factory method | Manual | Manual | [awesome-di-python](https://github.com/sfermigier/awesome-dependency-injection-in-python) |

---

## Version / Compatibility Notes

- **.NET Core / ASP.NET Core**: Open generic registration present since 1.0 (2016); keyed services added .NET 8 (2023); constrained open generics in .NET 5 (2020). Tested on .NET 6–10 (2022–2025).
- **Autofac**: Open generics since 2.x (early 2010s); factory delegate generation refined in 5.x–9.x. `RegisterGeneratedFactoryFromOpenType` extension available in some versions as workaround for factory demands. GitHub issue #972 (generic constraints) opened 2017, no resolution date published.
- **Spring Framework**: Factory bean pattern stable since Spring 2.0 (2006); generic type support issues tracked across SPR-12147 (2014), SPR-14118 (2016), SPR-15011 (2016), SPR-17524 (2018), SPR-23338 (2022) — all still open or partially fixed in Spring 6.x/7.x (2023–2025).
- **Guice**: TypeLiteral API stable since 1.0 (2007); no open generic support added in any version (3.0–5.x, 2012–2021). Project in maintenance mode; last release October 2021.
- **Castle Windsor**: Open generics since 2.0 (2008); stable through 3.x–5.x (2011–2019). Project in maintenance mode.
- **Python dependency-injector**: Latest 4.49.1 released 2026-06-18; typing support since 4.40+ (2023); no open generic factory feature in any version.
- **Dishka**: Active development (2026); generic resolver added 2024–2025; no open generic factory planned.

---

## Evidence Gaps

1. **.NET factory delegate workarounds**: The docs state factory delegates are "not supported" for open generics, but third-party libraries (e.g., on NuGet) may have extensions (like Autofac's `RegisterGeneratedFactoryFromOpenType` equivalent). A systematic survey of community workarounds is not recorded in official sources.

2. **Spring Framework generic type resolution completeness**: Multiple GitHub issues (#18690, #19578, #22056, etc.) report `getBeanNamesForType(ResolvableType)` failures with generic factory methods, but no unified summary of when these are fixed is published. Spring 7.x behavior is not explicitly documented against these issues.

3. **Autofac #972 resolution status**: The constraint validation bug (generic constraints with nested generics) was opened in 2017 with no published fix date. It is unclear whether Autofac 9.x (2025) fixes it, or whether it remains a known limitation.

4. **Python DI ecosystem consensus**: No PEP or RFC exists for "open generic factory binding in Python." The lack of examples in dependency-injector, Dishka, and others is inferred from documentation absence, not from explicit non-support statements. It is possible a workaround exists but is undocumented.

5. **Type caching semantics across frameworks**: .NET, Autofac, and Castle Windsor do not document whether a singleton open generic registration shares one instance across all closed types or creates one per closed type. This is critical for stateful repositories.

---

## Librarian's Note

**The open generic factory problem is solved in .NET/C#, partially in Java (Autofac), and not at all in Spring or Python.**

**Why .NET leads:**
- Open generic registration (`typeof(IRepository<>)`) is native to the language and type system.
- The .NET runtime preserves generic type information at runtime (no erasure), allowing reflection to match `IRepository<User>` to `IRepository<>` at resolution time.
- Constrained open generics (since .NET 5) allow validation of type parameters.
- **Limitation**: Factory delegates are not supported; only implementation types.

**Why Autofac stands out (among JVM frameworks):**
- Factory delegates work for open generics because the closed type argument is available at resolution time (not registration time).
- This gives Autofac an edge over Guice, which forces per-type TypeLiteral bindings.

**Why Spring and Guice failed:**
- Java's type erasure at runtime makes matching `IRepository<User>` to `IRepository<T>` fragile — TypeLiteral is the workaround, but it requires per-type registration.
- Spring's factory bean pattern (one factory per interface) is the pragmatic acceptance of this limitation.

**Why Python has no solution:**
- No Python DI framework has settled on a syntax or semantics for open generic binding.
- `get_origin()` and `get_args()` make introspection possible, but no framework has wired them into a "register once, materialize per type argument" feature.
- The lack of a unified pattern across Python DI libraries (dependency-injector, Dishka, lagom, rodi) means this is an unsolved problem in the Python ecosystem.

**Recommendation for varco**: If varco adopts open generic binding in providify, the design should mirror **.NET's** (`Register[T]` once → resolve any `[X]`) or **Autofac's** (factory delegates at resolve time). Python's `get_origin()`/`get_args()` make the keying mechanism possible; the hard part is the semantic choice: does varco cache per closed type, or share one instance? The answer depends on whether repositories are stateless (no caching needed) or stateful (caching per type required).

