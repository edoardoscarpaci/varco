# Dependency injection — default bindings

> Plan 045 wrote the **Default bindings** section below. Plan 047 wrote
> **Selecting a backend** and **Enabling a feature**. Plan 058 completes
> this document (multi-binding, lifecycle, testing).

## Default bindings

**The rule** (§D-rule, verbatim): *Every binding varco itself registers for
a varco seam is `@Fallback`. A binding the application registers — any
`@Singleton`, `container.bind()`, `container.provide()`, or `install()`ed
`@Configuration` provider without `@Fallback` — always wins, regardless of
registration order, priority, or qualifier. `container.validate()` reports
each default that lost as `IssueKind.FALLBACK_SHADOWED` at `Severity.INFO`
— expected, never an error.*

This replaced three unnamed magic negative-integer priority floors (Python's
platform integer ceiling negated, negated again by one more, and a removed
`varco_core.auth.authorizer._FALLBACK_PRIORITY` constant equal to the
negation of two to the 31st power) with providify 2.1.0's `@Fallback`
marker — a resolve-time activation rule ("yield if a
non-fallback competitor exists"), not a priority comparison.

### Two tiers, one named constant

Every varco default is `@Fallback`. Within that population there are two
kinds, both defined in `varco_core.di`:

1. **In-process defaults** (`InMemoryCache`, `InMemoryEventBus`,
   `NullFeatureFlags`, `BaseAuthorizer`, ...) — carry **no** explicit priority
   (providify's own default, `0`).
2. **Backend implementations of a core seam** (`KafkaEventBus`,
   `RedisEventBusSelectorConfiguration.bus`, `SQLAlchemyRepositoryProvider`,
   `CasbinPolicyEngine`, ...) — carry `priority=varco_core.di.BACKEND_PRIORITY`
   (`100`) so they outrank the in-process default once scanned/installed.

Both tiers remain `@Fallback` — an application binding beats either one, at
any priority, because `@Fallback` is an activation rule, not a priority
comparison. Priority only resolves ties *among* fallbacks (e.g. an
in-process default vs. a scanned backend for the same seam, both
`@Fallback`, no application binding present):

```
request get(AbstractEventBus)          candidates after activation/
                                        qualifier narrowing
  app binding (non-fallback, 0)  ───►  present → every @Fallback
                                        dropped → app wins
  KafkaEventBus (@Fallback, 100, kafka) ┐ all-fallback → max(priority)
  InMemoryEventBus (@Fallback, 0, in_memory) ┘ → Kafka wins
request get(AbstractEventBus, qualifier="in_memory")
  → only the in_memory-qualified candidate → InMemoryEventBus, always
```

### The `in_memory` escape hatch

`get(T, qualifier="in_memory")` (and the analogous backend qualifiers —
`"kafka"`, `"redis"`, `"nats"`, `"sa"`, `"beanie"`, `"casbin"`, ...) narrows
the candidate set to only that qualifier's bindings *before* the
fallback-drop runs (providify's `container.py` `_filter()`), so the
in-process default (or a specific backend) is returned even when an
application binding and/or another backend both exist for the *unqualified*
request. This is a specified providify behaviour, not a side effect, and it
is why every qualified in-process default (cache, event bus, DLQ,
deduplicator, distributed lock) keeps working exactly as before under
`@Fallback`.

### Overriding a default

- **A scanned `@Singleton` class default** (e.g. `InMemoryEventBus`,
  `KafkaEventBus`): register your own binding of the same interface —
  `container.bind(AbstractEventBus, MyBus)` or a plain `@Singleton class
  MyBus(AbstractEventBus)` — **in any order**, before or after the scan.
- **A `@Provider`-in-`@Configuration` default** (e.g.
  `OtelConfiguration.otel_config`, `VarcoFastAPIModule.task_serializer`):
  register your own `@Provider` — `container.provide(my_provider)` — **in
  any order**, before or after `install()`/`scan()`.

Neither shape requires a priority, a specific registration order, or
`@Fallback` on your own binding — omitting `@Fallback` is what makes a
binding win.

### Reading `FALLBACK_SHADOWED`

`container.validate()` reports one `IssueKind.FALLBACK_SHADOWED` issue at
`Severity.INFO` per active `@Fallback` binding whose *own natural request*
is currently won by an active non-fallback binding. `report.infos` is a
separate partition from `.errors`/`.warnings` — it never raises and never
affects `report.ok`. `shadowed_by` names the winning binding's owner.

**The scanned-class blind spot** (§D-proof): a scanned `@Singleton class
Impl(ABC)` registers with `interface = Impl` (the concrete class), never the
ABC — so its "natural request" is `(Impl, qualifier, None)`, which an
application's `bind(ABC, UserImpl)` binding never serves. The INFO therefore
fires correctly for `@Provider`-shaped defaults (whose interface **is** the
ABC) but is **never emitted** for a scanned-class default shadowed at its
ABC — even though `get(ABC)` correctly returns the override. This is a
confirmed providify limitation, filed as `P28-FALLBACK-SHADOWED-SCANNED-CLASS`
(`design/upstream-gaps/providify-fallback-shadowed-scanned-class.md` in the
repository — outside this site's docs tree, so not directly linkable here).
Plan 052's `varco diagnose` report is told to read this section: for
scanned-class defaults it must list them from a static catalogue (Plan 047),
not from `validate()`'s `FALLBACK_SHADOWED` output.

### Imperative-stamp ordering caveat

For a class decorated *imperatively* (e.g. `Fallback(Singleton(cls))` rather
than `@Fallback`/`@Singleton` at class-definition time), the marker must be
stamped **before** any `register()`/`bind()`/`scan()` call sees that class —
`ClassBinding.fallback` (and `ProviderBinding.fallback`) is resolved once, at
binding construction. varco itself has no such imperative stamp site as of
3.2.0 (Plan 045 removed the last three); an application author replicating
this pattern must respect the same ordering.

## Wiring contract

Plan 046 (DI-2 / DI-8 / DI-13) collapses the wiring-verb taxonomy to one shape per verb and
removes every service-locator use from varco's own source.

**Every package's `bootstrap(container: DIContainer) -> DIContainer`** — ten of them, including
the new `varco_core.di.bootstrap` — is idempotent, requires an explicit container (never creates
one, never reads the process-global container), never touches `os.environ`, and returns the same
object it was given:

```python
container = DIContainer()
varco_core.di.bootstrap(container)      # scan("varco_core", recursive=True) — every @Fallback default
varco_redis.di.bootstrap(container)     # scan("varco_redis", …) + scan("varco_core.revocation", …)
varco_sa.di.bootstrap(container)        # scan("varco_sa", …)
varco_fastapi.di.bootstrap(container)   # scan("varco_fastapi", …)
container.scan("myapp", recursive=True) # the app's own — plain providify, no varco verb
bind_repositories(container, User, Post)
app = create_varco_app(container)
```

`varco_core.di.bootstrap(container)` is what makes the Class-A `varco_core` defaults (including
`AbstractEventProducer → BusEventProducer`) available — it replaces the deleted
`varco_fastapi.di.setup_event_producer()`.

**`varco_core.di.bind_instance(container, Interface, obj, *, qualifier=None) -> ProviderBinding`**
is the one primitive every hand-built-object `bind_*` helper (`bind_trust_store`,
`bind_retention_registry`, `bind_cloudevents_serializer`) now reduces to. The lazy-factory
`bind_*` helpers (`bind_websocket_adapter`, `bind_mcp_adapter`, `bind_clients`, …) keep their own
factory bodies but adopt the same contract: **a `bind_*`/`enable_*` call registers a non-fallback
binding; a second registration of the same `(interface, qualifier)` raises
`varco_core.di.AlreadyBoundError`.** To replace a binding in a test use providify's
`di_overrides`/`ContainerOverrides` fixture (RL-3's rule); to have two, qualify them.
`varco_core.di.is_user_bound(container, Interface, qualifier=None) -> bool` is the primitive
`bind_instance`/every `enable_*` uses to detect "a non-fallback binding already exists" —
available directly for an app's own idempotency guards.

**`enable_*`** (`enable_policy_authorizer`, `enable_token_revocation`,
`enable_redis_token_revocation`, `enable_feature_flags`, `enable_tenant_membership`,
`enable_authorization_audit`) all raise `AlreadyBoundError` when a non-fallback binding of the
seam they flip already exists — whether from a previous `enable_*` call or the app's own binding.
`enable_authorization_audit` is the one exception to the `is_user_bound()` check: its job is to
*wrap* whatever `AbstractAuthorizer` is currently bound, so it instead checks
`isinstance(delegate, AuditingAuthorizer)` — call it last, after the real authorizer is wired.

**Streams replacement** (`varco_redis`) — `bootstrap(streams=True)` is gone; it used to mutate
`os.environ["VARCO_REDIS_USE_STREAMS"]` for the whole process. Register an explicit settings
instance before `bootstrap()` instead:

```python
from varco_core.di import bind_instance
from varco_redis.config import RedisEventBusSettings

bind_instance(container, RedisEventBusSettings, RedisEventBusSettings(use_streams=True))
varco_redis.di.bootstrap(container)
# Plan 048 replaces the first line with:
#   bind_settings(container, RedisEventBusSettings, instance=RedisEventBusSettings(use_streams=True))
```

`VARCO_REDIS_USE_STREAMS=true` in the environment keeps working unchanged — it is just a field
on the settings class, read by pydantic.

**The process-global container** — varco itself never reads or writes
`DIContainer.current()`/`.acurrent()`/`.scoped()`/`.reset()` (mechanically guarded by
`varco_core/tests/test_no_service_locator.py`, which walks every `varco_*/varco_*/**/*.py`).
An *application*'s own code may still use providify's global, and providify's `di_global` pytest
fixture exists for that code's tests — it is not something varco re-exports or wraps.

**The `di_wiring` conformance suite** (`testkit/varco_conformance/di_wiring.py`, the ninth
conformance module) is what all ten packages subclass to prove their `bootstrap()` satisfies this
contract: returns the same container, is idempotent, never touches the process-global container,
leaves `validate()` clean, every declared `@Fallback` default is overridable after `bootstrap()`,
and the declared `expected_defaults` set is complete (no undeclared fallback). A new package, or a
new `@Fallback` default in an existing one, adds a row to that package's `expected_defaults` or
the suite fails naming the missing `(interface, qualifier)` pair.

## Selecting a backend

Plan 047 (DI-4) replaces "bootstrapping a package's `bootstrap()` makes its backend live" with
**one environment variable per seam**: `VARCO_<SEAM>_BACKEND=<name>`. Six seams are selectable
today:

| Seam | Env var | In-process default | Named backends |
|---|---|---|---|
| `cache` | `VARCO_CACHE_BACKEND` | `memory` → `InMemoryCache` | `redis`, `layered`, `memcached` |
| `dlq` | `VARCO_DLQ_BACKEND` | `memory` → `InMemoryDeadLetterQueue` | `kafka`, `nats`, `redis`, `redis-streams`, `sa`, `beanie` |
| `idempotency` | `VARCO_IDEMPOTENCY_BACKEND` | `memory` → `InMemoryIdempotencyStore` | `redis`, `sa`, `beanie` |
| `job_store` | `VARCO_JOB_STORE_BACKEND` | `memory` → `InMemoryJobStore` | `redis`, `sa`, `beanie` |
| `event_bus` | `VARCO_EVENT_BUS_BACKEND` | `memory` → `InMemoryEventBus` | `kafka`, `nats`, `redis` |
| `revocation` | `VARCO_REVOCATION_BACKEND` | `null` → `NullTokenRevocationStore` | `memory`, `redis` |

`varco_core.di.SEAMS` (`varco_core/varco_core/di/seams.py`) is the one registry these rows come
from — `seam_by_name()`/`interface_of()` resolve a seam's ABC lazily, without importing providify
or the seam's own module at `import varco_core.di` time.

### Three tiers, and why priority `100` never breaks "code beats env"

Every varco backend binding of a selectable seam carries `@Fallback` (Plan 045) **and**
`selected_by(seam, value)` (Plan 047) — `@Requires(env="VARCO_<SEAM>_BACKEND",
value="<name>")` under the hood. A binding gated this way is *inactive* (invisible to every
lookup, including a qualified one) unless the variable is set to exactly that value:

```
get(Seam)  →  narrowing (interface/qualifier)  →  activation  →  fallback post-step
   a non-fallback (code) binding present?  ──yes──►  every @Fallback dropped  ──►  code wins
            │ no
            ▼  all survivors are @Fallback; max(priority) wins
   env dotted path        @Fallback, ENV_OVERRIDE_PRIORITY = 200  (VARCO_<SEAM>_BACKEND contains ':')
   env named backend      @Fallback, BACKEND_PRIORITY      = 100  (VARCO_<SEAM>_BACKEND == "<name>")
   in-process default     @Fallback, priority 0                   (always active)
```

**Code always wins outright, regardless of `BACKEND_PRIORITY`.** A non-fallback binding (your own
`@Singleton`, `bind_instance()`, `enable_*()`) is removed from no candidate set — it *removes*
every `@Fallback` competitor at the post-step, before priority is ever compared. `100 > 0` only
decides which `@Fallback` wins when no non-fallback binding exists — it can never make a backend
outrank your own code. See `varco_core/varco_core/di/select.py`'s module docstring for the
`providify/container.py` line numbers this rests on.

### Unset means the in-process default — always

Unsetting (or never setting) `VARCO_<SEAM>_BACKEND` means the seam's in-process default serves,
even if the backend package is bootstrapped/scanned. This is a **BREAKING** change from pre-4.0:
`varco_redis.di.bootstrap(container)` alone no longer makes Redis the live event bus/cache/DLQ —
you must also set `VARCO_EVENT_BUS_BACKEND=redis` (etc.) for each seam you want it to serve. An
explicit `VARCO_<SEAM>_BACKEND=memory` (or `null` for revocation) forces the default and silences
the "bootstrapped but unselected" warning below. See the plan's Migration table
(`plans/047-config-driven-backend-selection-and-catalogue.md`) for the exact variable per package.

### The dotted-path override

`VARCO_<SEAM>_BACKEND=pkg.mod:Name` resolves, at `varco_core.di.bootstrap()`, into a `@Fallback`
provider at `ENV_OVERRIDE_PRIORITY` (`200` — above every named backend, still below any code
binding). The target must be a **zero-argument** class (`issubclass()`-checked against the
seam's interface at bootstrap time) or a zero-argument callable (`pkg.mod:make_cache`,
`isinstance`-checked lazily at first `get()`). There is deliberately no way to inject
dependencies into a dotted-path target — providify has no "construct without registering" API;
anything that needs injection is, by definition, the code tier (`@Singleton class MyCache(...)`
in your own scanned package, or `bind_instance()`).

### `check_backend_selection()` — catching typos and forgotten variables

`varco_core.di.check_backend_selection(container)` — called by `create_varco_app()` whenever a
container is passed, and by `varco diagnose` (Plan 052) — raises `BackendSelectionError` when a
named value is neither the seam's default nor a value any bootstrapped package registered (a
typo, wrong case, or an unbootstrapped package), and logs one `WARNING` (never raises) when a
package registered values for a seam whose variable is unset — "bootstrapped but unselected; the
in-process default is live". `varco_core.di.inspect_backend_selection(container)` is the runtime
read: per seam, `value`, `source` (`"code"`/`"env-dotted"`/`"env"`/`"default"`), and `live_owner`.

### The redis `use_streams` / `redis-streams` asymmetry

`VARCO_EVENT_BUS_BACKEND=redis` selects the Redis event bus; **within** that selection,
`VARCO_REDIS_USE_STREAMS=true` still picks `RedisStreamEventBus` (Streams, at-least-once) over
the Pub/Sub default — a `RedisEventBusSettings` field, not a second named value, because 046
already designed the transport switch around an explicit settings instance and there is only one
selector provider to gate either way. The DLQ has no equivalent settings field, so it gets two
named values instead — `redis` and `redis-streams` — because two independent `@Configuration`s
(`RedisDLQConfiguration`, `RedisStreamDLQConfiguration`) already exist as separate providers with
separate `@Disposes`. Two Redis surfaces, two different spellings for "use Streams" — recorded
here so it is never "fixed" by merging the DLQ shape into a `use_streams` field or vice versa.

### `qualifier="kafka"` (and friends) only resolves when selected

`get(AbstractEventBus, qualifier="kafka")` raises `LookupError` while `VARCO_EVENT_BUS_BACKEND`
is not `"kafka"` — a gated binding is invisible to *every* lookup while inactive, including one
that asks for its own qualifier. Only `qualifier="in_memory"` (the ungated in-process default)
always resolves — it is the one qualifier that survives regardless of selection, which is why it
remains the escape hatch documented above under "The `in_memory` escape hatch".

## Enabling a feature

Three switchable, binding-shaped features follow one convention (Plan 047 / DI-12): in code,
`enable_<feature>(container)`; in the environment, `VARCO_<FEATURE>_ENABLED` (`1`/`true`/`yes`/
`on`, case-insensitive, via `varco_core.di.env_truthy`). **Code beats env beats default**, by the
same fallback/priority mechanism backend selection uses — the env form is a `@Fallback` provider
at `BACKEND_PRIORITY`, gated by `varco_core.di.feature_default("<feature>")`
(`@Requires(condition=env_truthy(env))`); the code form is the non-fallback binding `enable_*`
registers (raises `AlreadyBoundError` on a second non-fallback registration, Plan 046).

| Feature | Env var | Code verb | Category |
|---|---|---|---|
| `feature_flags` | `VARCO_FEATURE_FLAGS_ENABLED` | `enable_feature_flags(container)` | optional integration |
| `tenant_membership` | `VARCO_TENANT_MEMBERSHIP_ENABLED` | `enable_tenant_membership(container, settings=None)` | optional integration |
| `policy_authorizer` | `VARCO_POLICY_AUTHORIZER_ENABLED` | `enable_policy_authorizer(container)` | optional integration |
| `authorization_audit` | **none — code-only exception** | `enable_authorization_audit(container)` (call it last) | security |

`varco_core.di.FEATURES` is the registry these rows come from. `enable_authorization_audit` has
no env form: it wraps whatever `AbstractAuthorizer` is *currently* live, which is a self-cycle if
expressed as a provider *of* `AbstractAuthorizer` — it stays code-only, and it must be the last
`enable_*`/env-selection call in your bootstrap.

```python
from varco_core.flags.di import enable_feature_flags

container = DIContainer()
varco_core.di.bootstrap(container)
enable_feature_flags(container)                 # code form — always wins
# equivalently, before bootstrap: os.environ["VARCO_FEATURE_FLAGS_ENABLED"] = "true"
```

Calling `enable_*(container)` while the matching env var is also set is not an error and not a
no-op — code shadows the env twin, and `container.validate()` reports one `FALLBACK_SHADOWED`
INFO naming your binding. Calling `enable_*(container)` **twice** still raises `AlreadyBoundError`
(Plan 046, unchanged).

**Default on/off matrix** (published verbatim — §D-features):

| Category | Default | Members |
|---|---|---|
| Optional integration | off | every backend of the six seams; `feature_flags`, `tenant_membership`, `policy_authorizer`; `revocation=memory\|redis` |
| Security feature | on where a safe in-process default exists; off + posture finding otherwise | on: `SecurityHeadersSettings`/`BodyLimitSettings`/`RateLimitSettings` (when a bundle is passed), `MigrationSettings.mode` (`off`, a finding); off + finding: revocation (`null`), `BaseAuthorizer` live, `authorization_audit` |
| Dev-only convenience | off in production (a posture finding when on) | `ProfilingSettings.enabled`, the in-process backend of any seam (`di.in_process_backend`), `ErrorEnvelopeSettings.include_detail` |

## Pitfalls

| Pitfall | Fix |
|---|---|
| Marked a collection contribution (e.g. `HealthCheck`) `@Fallback` | Never do this — a fallback is excluded from `get_all()`/`aget_all()`/`list[T]` the instant any non-fallback sibling exists, which would silently empty `/health` the moment an app adds its own check. Collection contributions stay a plain qualified `@Singleton`. |
| Subclassed a `@Fallback` class and expected the subclass to also be a fallback | It is not — `FallbackMarker`/`_is_fallback` never walks the MRO (`__dict__`-only lookup, matching `@Profile`'s rule). Re-decorate the subclass explicitly if it should also be a fallback. |
| Resolved a `@Fallback` singleton *before* registering its shadowing binding, then wondered why the shadow "didn't take" | Not evicted — a fallback singleton already instantiated and cached stays cached even after a shadowing binding arrives later (the same documented `activate_profile()` caveat). `override()`/`reset_binding()` are the eviction tools; varco's own bootstrap order (scan defaults → install app → first `get()`) never hits this in practice. |
| Two backends scanned for the same seam (e.g. both `varco_kafka` and `varco_nats`) and expected one to deterministically win | Both are `@Fallback` at the same `BACKEND_PRIORITY` — they tie exactly as they did before this plan (first-registered wins), plus `validate()` now names the tie as `AMBIGUOUS_BINDING`. Not a regression; Plan 047 (config-driven backend selection) owns disambiguating this. |
| Copied a varco default's decorator line verbatim, including `@Fallback` | Your binding is now a fallback too — it ties with varco's own default by priority, then registration order, instead of winning outright. Drop `@Fallback` from your own bindings; omitting it is what makes a binding win. |
| Called `bootstrap()` with no argument | `TypeError` (missing positional) — every `bootstrap()` requires an explicit container (Plan 046). `container = DIContainer(); varco_x.di.bootstrap(container)`. |
| Expected a second `bind_x`/`enable_x` call to replace the first | It raises `AlreadyBoundError` instead (Plan 046 — providify's `provide()` appends, it never replaces). Qualify the second binding, or use `di_overrides`/`ContainerOverrides` in a test. |
| `container.install(VarcoFastAPIModule)` right after `varco_fastapi.di.bootstrap(container)` | A documented no-op, not a bug — `bootstrap()`'s own scan already installs the `@Configuration`, and providify's container-owned install dedup (`scanner.py:255-…`, `container.py:1019/7158/7194`) means a second `install()` of the same module adds no binding and raises nothing (measured against providify 2.1.0; see `test_redundant_install_after_scan_is_a_documented_noop`). Redundant, not harmful — no need to guard against it. |
| Scanned only `varco_fastapi`, and `AbstractEventProducer` never resolves to `BusEventProducer` | Call `varco_core.di.bootstrap(container)` too — `BusEventProducer` is a `varco_core` Class-A default; the old `setup_event_producer()` helper that used to paper over a missing `varco_core` scan is gone. |
| Set `VARCO_REDIS_USE_STREAMS` by mutating `os.environ` directly in a test | Use `bind_instance(container, RedisEventBusSettings, RedisEventBusSettings(use_streams=True))` before `bootstrap()`, or `monkeypatch.setenv(...)` if the test genuinely exercises the env-var path. `bootstrap(streams=True)` no longer exists. |
| Bootstrapped `varco_kafka`/`varco_redis`/… and expected its backend to be live | Bootstrapping a package no longer selects it (Plan 047, BREAKING) — set `VARCO_<SEAM>_BACKEND=<name>` too. `check_backend_selection()` logs a `WARNING` ("bootstrapped but unselected") exactly for this case; `varco diagnose`/the `di.in_process_backend` posture finding also surface it. |
| `VARCO_CACHE_BACKEND=Redis` (or any other case) | Silently "unset" — `@Requires(value=...)` is exact and case-sensitive. `check_backend_selection()` raises `BackendSelectionError` naming the known, lower-case values. |
| `VARCO_<SEAM>_BACKEND=pkg.mod:MyClass` where `MyClass.__init__` takes required constructor arguments | `TypeError` at first `get()`, wrapped as `BackendSelectionError` telling you to use a zero-arg factory (`pkg.mod:make_x`) or bind it in code instead — the dotted-path tier deliberately does no dependency injection. |
| Called `enable_feature_flags(container)` and also set `VARCO_FEATURE_FLAGS_ENABLED=true`, and are unsure which one "won" | Code always wins (Plan 047 §D-precedence) — confirm via `container.validate(raise_on_error=False).infos`, which names one `FALLBACK_SHADOWED` for the env twin whenever code shadows it. |
| Flipped `VARCO_CACHE_BACKEND` after a `CacheBackend` singleton already resolved, expecting the old backend to disappear | It is not evicted — `@Requires` is evaluated lazily on the *next* `get()`, and providify never evicts an already-cached fallback singleton (the same P24-style caveat as elsewhere in this doc). A second `get()` after the flip constructs and caches the *new* backend alongside the old one; `ashutdown()` stops both. Use a fresh container if a test wants a single, fresh backend. |
| `get(AbstractEventBus, qualifier="kafka")` while `VARCO_EVENT_BUS_BACKEND` is not `"kafka"` | `LookupError` — a gated binding is invisible to every lookup while inactive, including its own qualifier. Only `qualifier="in_memory"` (the ungated default) always resolves. |

## Settings (Plan 048 / DI-6, DI-16)

Every varco settings class — every `pydantic_settings.BaseSettings` subclass with a
`VARCO_<FEATURE>_` prefix — is registered through exactly one recipe:
`varco_core.settings.bind_settings(container, Cls, *, instance=None, qualifier=None,
env_prefix=None)`. It replaces every module-level `@Fallback @Provider` settings factory and
every hand-rolled `is_user_bound()` idempotency guard that used to exist per package.

```python
from providify import DIContainer
from varco_core.settings import bind_settings
from varco_redis.config import RedisEventBusSettings

container = DIContainer()
bind_settings(container, RedisEventBusSettings)   # fail-fast, right here, right now
```

**Precedence, per field**: `instance=` kwargs (code) > process environment (`VARCO_X_FIELD`) >
class default. `bind_settings()` constructs the class **immediately**, at the call — a bad env
value raises `SettingsValidationError` naming the offending env var, never a lazy failure the
first time something happens to resolve the class. Register a code override (`instance=Cls(...)`
or any non-fallback binding) **before** calling a package's `bootstrap()` — providify never
evicts an already-cached fallback singleton, so an override registered after something already
resolved the env-backed default has no effect on that already-built object.

**varco never reads a `.env` file.** Only the real process environment. Call
`dotenv.load_dotenv()` (or your platform's equivalent) before `bootstrap()` if you want one.

**Named instances (DI-16)** — one extra kwarg, no general enumeration mechanism:

```python
bind_settings(container, RedisCacheSettings)  # the default, VARCO_REDIS_CACHE_*
bind_settings(
    container, RedisCacheSettings, qualifier="sessions", env_prefix="VARCO_REDIS_SESSIONS_CACHE_"
)
sessions_cache = RedisCache(container.get(RedisCacheSettings, qualifier="sessions"))
```

`env_prefix=` requires `qualifier=` (two unqualified bindings of the same class would tie).

**`bind_core_settings(container)`** binds every `varco_core`-owned settings class that has no
package-specific home (§D-core-list) — called first by every package's `bootstrap()`.

**`CasbinSettings` (and every settings class) is registered via `bind_settings()`'s `@Provider`
closure, never `@Singleton`** — pydantic-settings' `BaseSettings.__init__` carries 29 annotated,
defaulted keyword parameters (`_env_prefix`, `_env_file`, …) that providify's per-parameter
resolver treats as injection points and silently fills from whatever else is bound
(`providify/container.py:3753-3759`); `resolve_params()` skipping `**values`
(`_annotations.py:574-578`) is not what makes a class binding unsafe here — the 29 *named*
kwargs are. A zero-argument `@Provider` closure has no parameters for the resolver to see at all.

The generated `VARCO_*` reference — every registered class, its prefix, every field's env var,
type, and default (secrets rendered `***`) — lives at
[technical_docs/settings-reference.md](../settings-reference.md), regenerated by `make
settings-reference` and gated by `make settings-check` (part of `make lint`'s no-`PKG` path).

### Pitfalls (Settings)

| Pitfall | Fix |
|---|---|
| Registered a code override for a settings class *after* calling `bootstrap()` | Too late for anything already built during that `bootstrap()` call — register overrides before `bootstrap()`. |
| `instance=MySettings(url="...")` and expected every other field to also be fixed | `instance=` is env-aware *per field*, not per object — pydantic still reads `VARCO_X_OTHER_FIELD` while constructing it. Use `MySettings.from_dict({...})` as the `instance=` for a fully env-independent object. |
| `bind_settings(container, Cls, env_prefix="VARCO_X_REPLICA_")` with no `qualifier=` | `ValueError` — two unqualified bindings of the same class would tie; always pass `qualifier=` alongside `env_prefix=`. |
| A `list`/`tuple`-typed settings field read from `VARCO_X_ORIGINS=a,b,c` | Type it `varco_core.settings.CommaSeparated`, not a bare `tuple[str, ...]` — pydantic-settings' default env decoding expects JSON for a `list`/`tuple` annotation. |
| A bare `REDIS_URL`/`POSTGRES_HOST`/… honoured in a dev shell | The `ConnectionSettings` family (§D-connection) is deliberately **not** part of this recipe — those names belong to the underlying service's own tooling, not `bind_settings()`. |

See [technical_docs/di-catalogue.md](../di-catalogue.md) for the **generated** table of every
seam's live backends, every feature's env var, and every other `@Fallback` default in the
workspace — regenerated by `make di-catalogue`, never hand-edited.
