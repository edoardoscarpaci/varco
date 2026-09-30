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
  `OtelConfiguration.otel_config`, `VarcoFastAPIConfiguration.task_serializer`):
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
| `container.install(VarcoFastAPIConfiguration)` right after `varco_fastapi.di.bootstrap(container)` | A documented no-op, not a bug — `bootstrap()`'s own scan already installs the `@Configuration`, and providify's container-owned install dedup (`scanner.py:255-…`, `container.py:1019/7158/7194`) means a second `install()` of the same module adds no binding and raises nothing (measured against providify 2.1.0; see `test_redundant_install_after_scan_is_a_documented_noop`). Redundant, not harmful — no need to guard against it. |
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

## Modules (Plan 049 / DI-1, FLIP-1, RO-4)

`create_varco_app(container, *, modules=None, ...)` replaces the eleven hand-passed keyword
objects (`migrations`, `tenancy`, `reliability`, `retention`, `jwks_refresh`, `i18n`, `timezone`,
`security_headers`, `body_limit`, `rate_limit`, `extra_lifespan_components`) — plus six
`enable_*` booleans and `cors` — with a list of `varco_core.di.VarcoModule` instances.
`modules=None` installs `varco_fastapi.default_modules()`; an explicit list installs **exactly**
those (NestJS `imports`/Django `INSTALLED_APPS` semantics — nothing is inserted implicitly).

```python
from providify import DIContainer
from varco_fastapi.app import create_varco_app
from varco_fastapi.modules import default_modules
from varco_sa.modules import SAModule
from varco_fastapi.modules import MigrationsModule

container = DIContainer()
app = create_varco_app(container)  # modules=None -> default_modules()

# Extending the default set:
app = create_varco_app(
    container,
    modules=[MigrationsModule(my_migrator), *default_modules(), SAModule(config=my_sa_config)],
)
```

**Open Question 1 answered**: `VarcoModule` is a plain varco ABC — **never** a providify
`@Configuration` subclass. `container.scan(pkg, recursive=True)` auto-installs every public
`@Configuration` it meets, and `container.scan("varco_core", recursive=True)` is a documented,
in-use pattern (it *is* `varco_core.di.bootstrap()`) — a `@Configuration`-shaped module would
therefore be silently activated by any package's own bootstrap. A plain class carries no
providify marker at all, so the scanner never sees it — mechanically enforced by
`test_modules_never_scanned.py` (core + fastapi). Rejected candidates and the full providify
citation table (`@Configuration`/`depends_on=`/`install()` semantics, read against providify
2.1.0 source): `plans/049-varco-module-and-composable-app.md` §D-shape/§Context.

**`*Configuration` vs. `*Module`, the naming rule**: `*Configuration` = a providify
`@Configuration` grouping `@Provider` methods, scan-installed, owned by its package's
`bootstrap()` (`SAConfiguration`, `BeanieConfiguration`, `VarcoFastAPIConfiguration`,
`RedisCacheConfiguration`, …). `*Module` = a `VarcoModule`, constructed by the app, **never**
scanned (`SAModule`, `BeanieModule`, `KafkaModule`, `CasbinModule`, …). Every package ships
exactly one `VarcoModule` (in `<pkg>/modules.py`, or `varco_core.di.module` for `CoreModule`);
its `_register()` calls that package's own `bootstrap(container)` first.

**Slots, and when each runs** (base class: `varco_core.di.VarcoModule`):

| Slot | Runs | Notes |
|---|---|---|
| `register(container)` | Synchronously, at `create_varco_app()` time, for **every** module (even a disabled one) | `@final` — a template method backed by `ModuleRegistry`; idempotent; calls `_register()` then records `(self, enabled=is_enabled(container))` |
| `_register(container)` | From `register()` | Abstract — the package's own `bootstrap()` + any constructor-argument bindings |
| `async_init(container)` | Awaited once per module, before any module's `start()` | The seam for `await container.ainstall(<a real @Configuration>)` (e.g. `RedisCacheConfiguration`) |
| `validate(container)` | Nothing calls it yet | Defined here; Plan 052 folds it into the lifespan (`varco diagnose`) |
| `start(container)` / `stop(container)` | Only for a module recorded `enabled=True` at registration | Start order = `depends_on`-resolved order; stop order = exact reverse of started (`VarcoLifespan`'s existing LIFO contract, at module granularity) |
| `is_enabled(container)` | Called from `register()` | The Plan 047 tri-state (below) |

**The tri-state** (Plan 047 §D-features, moved here verbatim): `enabled=None` (constructor
default) defers to `self._enabled_from_settings(container)`; `enabled=False`/`True` (a code
decision) always wins over whatever the settings hook would answer. `register()` always runs —
even a disabled module binds its settings/`@Fallback` defaults, so it stays visible to 052's
`varco diagnose` as "disabled" rather than silently absent. A backend module with no
`.enabled` settings (`SAModule`, `KafkaModule`, …) has `enabled=None` always resolve `True`;
`enabled=False` there only skips the module's lifecycle slots — backend *selection* is still
047's `VARCO_<SEAM>_BACKEND`.

**`depends_on`** is a **varco** `ClassVar` (`tuple[type[VarcoModule], ...]`), walked by
`varco_core.di.resolve_module_order` — a DFS post-order matching the shape of providify's own
`resolve_install_order`, but a small, independent implementation (providify's raises `TypeError`
for any non-`@Configuration` node, so it cannot be reused). It is **not** providify's
`depends_on=` and confers no providify behaviour — the name is kept identical on purpose because
the concept maps 1:1. Every package module `depends_on = (CoreModule,)` except `CoreModule`
itself. An instance-level `depends_on` (set in `__init__`) shadows the class attribute and is
honoured — this is how `LifecycleModule(component, depends_on=(...))` works.

**`default_modules(*, exclude=())`** returns fresh instances, in order: `CoreModule,
ObservabilityModule, FastAPIModule, CorsModule, SecurityHeadersModule, ErrorEnvelopeModule,
BodyLimitModule, LoggingModule, TracingModule, LocalizationModule, SecurityPostureModule`.
**FLIP-1** (`BACKLOG.md:81`): `SecurityPostureModule` is in this default set —
`VARCO_SECURITY_POSTURE_ENABLED=false` or `default_modules(exclude=(SecurityPostureModule,))`
turns it off; `enforce` stays `"warn"` (Plan 054 owns the flip to `"refuse"`).

**RO-4** (`BACKLOG.md:112`) lands as `LoggingModule(redactor=PolicyRedactor())` — the redactor
keyword Plan 040 refused to add directly to `create_varco_app`. `LoggingModule()` (no redactor)
still means "no redaction" — it never falls back to `default_redactor()`.

**`FastAPIModule` unconditionally contributes `RequestContextMiddleware`** — unlike every other
default module, this one is not itself switchable by env var/`exclude=`; the only thing that
changes is *which* auth strategy it runs. With `AbstractServerAuth` bound, it runs the app's real
one; with nothing bound, it still runs `RequestContextMiddleware` in **anonymous mode**
(`server_auth or AnonymousAuth()`, `RequestContextMiddleware`'s own default) — every request gets
a populated, unauthenticated `AuthContext`/`correlation_id`/`current_tenant()` rather than none
at all. This restores the exact pre-049 contract (the deleted `_try_add_request_context_middleware`
had the same anonymous-mode fallback branch) after a brief regression earlier in this plan's own
review cycle where the middleware was gated on `is_resolvable(AbstractServerAuth)` and silently
omitted otherwise — see `varco_fastapi/varco_fastapi/modules/edge.py`'s `FastAPIModule` DESIGN
block for the full argument. The operator-facing risk this trades for parity — an app that forgot
to bind `AbstractServerAuth` gets a *working*, fully anonymous app instead of a loud failure — is
⚠️ **not currently caught by anything**: neither `inspect_http_edge()` nor the security posture
preflight emits a finding for a missing `AbstractServerAuth` binding as of this writing (checked
directly against both modules' `check=` id sets) — the risk is real and, today, silent.

**`HttpContributor`** (`varco_fastapi.modules.base`, a `runtime_checkable` Protocol) is how an
HTTP-shaped module contributes: `middleware(container) -> Sequence[MiddlewareSpec]` and
`configure_app(app, container) -> None`. It is deliberately **not** on the core `VarcoModule`
ABC — `varco_core` stays transport-free, so a non-FastAPI host can run a plain module list
(`[CoreModule(), KafkaModule(), RetentionModule(r)]`) with no FastAPI import at all.
`MiddlewareSpec(slot: EdgeSlot, target, kwargs)` names a slot rather than an implicit position;
`varco_fastapi.middleware.slots.EdgeSlot` **is** the normative, outermost-first middleware order
table, as code — pinned to `varco_fastapi.middleware`'s prose table by `test_middleware_slots.py`
so the two can never silently drift apart.

**`LifecycleModule(component, *, name=None, depends_on=(), enabled=None)`** and
**`MiddlewareModule(entries, *, slot=EdgeSlot.EXTRA, enabled=None)`** replace the pre-049
`extra_lifespan_components=`/`extra_middleware=` escape hatches — no subclassing required to
keep a hand-built `start()`/`stop()` object or a raw middleware entry working.
`MiddlewareModule` defaults to `EdgeSlot.EXTRA` (the exact position `extra_middleware=` used to
occupy — outside `ErrorMiddleware`); pass `slot=EdgeSlot.IDEMPOTENCY` to finally land a custom
middleware *inside* `ErrorMiddleware`.

**Writing an app's own module** — subclass `VarcoModule` directly rather than reaching for
`LifecycleModule`/`MiddlewareModule` when the app has its own scan/bind/start/stop story (the
worked example is `examples/00-full-stack-post-api/example/module.py`'s `PostAppModule`):

```python
from varco_core.di.module import CoreModule, VarcoModule


class MyAppModule(VarcoModule):
    name = "my_app"
    depends_on = (CoreModule,)

    def _register(self, container):
        container.scan("myapp", recursive=True)

    async def async_init(self, container):
        await create_tables(container)

    async def start(self, container):
        ...

app = create_varco_app(container, modules=[*default_modules(), MyAppModule()])
```

### Pitfalls (Modules)

| Pitfall | Fix |
|---|---|
| Decorated a `VarcoModule` subclass with `@Configuration`/`@Singleton`/`@Provider` | Activated by every `container.scan()` of its package — a `VarcoModule` is constructed by the app, never scanned (`test_modules_never_scanned.py` enforces this mechanically). |
| Listed `MigrationsModule`/`TenancyModule` *after* `CoreModule` (or after `*default_modules()`) | No longer a problem (Plan 050) — both declare `phase = Phase.INFRA`, so they start before `core.bus`/`core.consumers` regardless of list position; the pre-050 "list it first or get a WARNING" caveat and its WARNING are both gone. §Lifecycle phases below. |
| Two instances of one `VarcoModule` subclass in one `modules=` list | `ValueError` — almost always a copy-paste error. Use `default_modules(exclude=(X,))` to drop the default and add your own. |
| `enabled=False` on a backend module (e.g. `KafkaModule(enabled=False)`) expecting it to deselect the backend | It only skips that module's lifecycle slots — backend *selection* is still Plan 047's `VARCO_<SEAM>_BACKEND`. |
| A module that `container.get()`s a settings class in `_register()`, then the app tries to override it afterward | Same not-evicted caveat as everywhere else in this doc — a fallback already resolved and cached does not un-resolve. Pass the override as a constructor argument to the module instead (e.g. `SecurityHeadersModule(settings=...)`), which binds it via `bind_instance` before anything else can resolve the fallback. |
| "Fixed" a settings-shaped module (`SecurityHeadersModule`/`BodyLimitModule`/`SecurityPostureModule`) to call `bind_settings(container, Cls, instance=...)` instead of `bind_instance`, because that is the Settings section's documented recipe | `SettingsAlreadyBoundError` — `FastAPIModule` (or `CoreModule`) always registers a plain `@Fallback` `bind_settings(container, Cls)` binding first (`depends_on=` guarantees the order), and `bind_settings(..., instance=...)` raises against **any** prior binding of that class, fallback or not. `bind_instance` (Plan 046's "already-constructed object" verb) uses separate bookkeeping and only raises against a non-fallback binding — exactly what "a code decision beats the module's own default" needs here. This is a **resolved deviation from the plan's original recipe** (`plans/049-varco-module-and-composable-app.md` §D-enabled), not an oversight — do not revert it. |
| Assumed `RateLimitModule` always installs two `RateLimitMiddleware` instances (PRE_AUTH + POST_AUTH) | It installs **up to** two — one `MiddlewareSpec` per rule-scope partition that is non-empty. A bundle with only a `TENANT` rule yields exactly one `RateLimitMiddleware` at `POST_AUTH`, not one real instance plus a `rules=()` pass-through at `PRE_AUTH` — an existing byte-identical-order characterization test pins this. Also a **resolved deviation** from the plan's literal "two specs" text (`plans/049` §D-http), not a bug. |
| Bound no `AbstractServerAuth` and expected the app to refuse to start, or expected no `RequestContextMiddleware` at all | `FastAPIModule` contributes `RequestContextMiddleware` **unconditionally** (pre-049 parity) — with nothing bound it runs in anonymous mode, so every request still gets a populated (but unauthenticated) `AuthContext`/`current_tenant()`. The app boots and serves traffic; it is not fail-closed. ⚠️ Neither `inspect_http_edge()` nor the security posture preflight currently reports this as a finding — verified absent from both `check=` id sets; nothing today surfaces a forgotten `AbstractServerAuth` binding except reading the response yourself. |

## Multi-binding for "all implementations of X" (Plan 051 / DI-5)

**The one convention every collection seam follows**: a collection seam is an ABC (or a frozen
value type) stamped `@Multibound` (`providify.Multibound`). A contribution is any ordinary
non-`@Fallback` binding of that type — a scanned `@Singleton` subclass, a `@Provider` returning
it, or `varco_core.di.collect.contribute(container, Seam, obj)`. A consumer reads it as a bare
`list[Seam]` constructor hint or via `varco_core.di.collect.collect()`/`acollect()`, which return
an immutable `tuple`. Zero contributions is a legal, empty tuple — never a `LookupError`.
providify orders a collection **ascending** by priority, ties in registration order — **no varco
contribution ever sets `priority=`**, and no consumer reads the order as meaning (the tenant chain
is the one exception: it sorts by its own `TenantTrust` domain key, never by providify priority).

| Seam | Module | varco's own contributors | How an app/plugin contributes | Reader | Empty means |
|---|---|---|---|---|---|
| `EventConsumer` | `varco_core.event.consumer` | none by default | `@Singleton` subclass | `CoreModule.start()` auto-registers every contribution to the started bus | no consumers — legal, silent |
| `RetentionPolicy` | `varco_core.retention.policy` | none by default | `@Singleton`/`@Provider`, or `contribute()` | `RetentionModule.async_init()` builds a `RetentionRegistry` from `acollect()` | nothing is ever purged — `RetentionModule.validate()` reports it |
| `TenantSource` | `varco_core.tenancy.source` | the four built-ins, constructed from settings, not the pool | `@Singleton` subclass, named in `VARCO_TENANT_SOURCES` to become active | `resolve_tenant_source_chain(container)` | `VARCO_TENANT_SOURCES` empty → no chain (unchanged) |
| `PostureCollector` | `varco_core.posture` | the five default collectors, contributed by `SecurityPostureModule.configure_app()` | `@Singleton` subclass, or `collectors=` replaces the default set | `SecurityPostureModule.start()` → `acollect()` | `SecurityPostureModule.validate()` reports it (cannot happen with the default set) |
| `HealthCheck` | `varco_core.health` | seven backend probes | `@Singleton(qualifier=...)` | `HealthRouter`'s `Instance[HealthCheck].aget_all()` (unchanged — lazy, zero-tolerant) — `collect(container, HealthCheck)` is an equally valid eager read | no probes — legal |

`WebhookVerifier` is **not** a collection seam (§D-verifier) — a `WebhookVerifier` instance is
constructed *per route*, with that route's secrets; there is no shared instance to multi-bind.
It uses the same explicit-registration verb `varco_core.profiling`'s CPU/memory backends use:
`register_webhook_verifier(name, cls, *, replace=False)`.

### `priority=` means opposite things for `get()` vs. a collection

providify's `priority` picks the **highest**-priority winner for `get(T)`, but sorts a
`list[T]`/`collect()` result **ascending** — the same number means "the `get()` winner" and "last
in the list" simultaneously. This is why no varco contribution ever sets `priority=`, and why
every consumer treats the tuple as a set with a stable, documented-as-incidental order.

### The tuple is made once, at the consumer

`collect()`/`acollect()` convert providify's fresh `list` per resolution into a `tuple` — every
consumer stores that tuple. providify itself has no de-dup mechanism; `contribute()` refuses the
exact **same object** twice (identity) — a seam whose contributions have their own notion of
uniqueness (a policy `name`, a chain source `name`, a posture collector `name`) enforces it at its
own consumption point.

### `contribute()` vs. `bind_instance()`

| | `bind_instance()` (Plan 046) | `contribute()` (Plan 051) |
|---|---|---|
| Says | "the one implementation of *interface*" | "one of many contributions to *seam*" |
| Second call, same object | `AlreadyBoundError` | `AlreadyBoundError` (identity) |
| Second call, a distinct object | `AlreadyBoundError` | Registers a second, distinct contribution |

### An empty *required* collection is varco's own finding, never providify's

providify's `validate()` never reports a missing collection — `()` is always a legal answer. A
module that *requires* at least one contribution says so itself, from its own `validate(container)`
slot (a pure read over `container.get_all_bindings(Seam)`) — see `RetentionModule`/
`SecurityPostureModule` above.

### Contributing from your own package

```python
from providify import Singleton
from varco_core.posture import PostureCollector, PostureFinding, PostureSeverity
from varco_core.tenancy.source import TenantSource, TenantTrust


@Singleton
class PartnerHeaderTenantSource(TenantSource):
    name = "partner"
    trust = TenantTrust.MEDIUM

    def resolve(self, request):
        ...


@Singleton
class MyPostureCollector(PostureCollector):
    name = "my_check"

    def collect(self) -> list[PostureFinding]:
        return [PostureFinding(check="my.check", severity=PostureSeverity.INFO, ...)]
```

`container.scan("my_package")` is all it takes — no `bind_*` call, no framework edit. See
`testkit/varco_plugin_example` for the full four-seam worked example (never packaged).

### Pitfalls (Multi-binding)

| Pitfall | Fix |
|---|---|
| A contribution sets `priority=` "to run first" | It sorts **last** among ascending-priority ties, and simultaneously becomes what `get(Seam)` returns as "the winner" — never set `priority=` on a collection contribution. |
| Used `InjectInstances[T]` instead of a bare `list[T]`/`collect()` | Raises `LookupError` on zero contributions — every consumer would need a try/except. `@Multibound` + `list[T]`/`collect()` resolves to `()` instead. |
| A contribution marked `@Fallback` | Disappears the moment any non-fallback sibling exists (045's structural rule) — a *default set* is contributed as ordinary bindings and replaced by *construction* (`collectors=`), never by shadowing. |
| A literal `list[Seam]` binding registered alongside the collection point | The literal binding wins outright and the collection is silently ignored — never register a literal `list[T]` binding for a `@Multibound` seam. |
| A `@Component`-scoped `EventConsumer` | `CoreModule.validate()` lists it; `start()` raises naming the class — make it `@Singleton` or set `auto_register = False`. |
| Kept a `@PostConstruct def _setup(self): self.register_to(self._bus)` on a DI-bound consumer | Harmless (the auto path's `is_registered_to()` guard skips an already-registered consumer, silently) but no longer the documented way — delete it; `CoreModule.start()` registers it after the bus has started. |
| A `TenantSource` bound (scanned) but not named in `VARCO_TENANT_SOURCES` | Inert — bound ≠ active. `resolve_tenant_source_chain()` logs every such name at INFO. |
| `register_webhook_verifier(name, cls, replace=True)` on a built-in name | Silently shadows the built-in — documented, greppable; never done by varco itself. |

## Lifecycle phases (Plan 050 / DI-9, DI-10)

`varco_core.di.Phase` (an `IntEnum`, room left between values) names five coarse, anonymous
start-order buckets every `VarcoModule` declares itself into via `phase: int` (default
`CONSUMERS`):

| Phase | Owns |
|---|---|
| `INFRA` (100) | Migrations, tenancy pools, trust stores, catalogs, stores (DLQ/idempotency/job/revocation), repositories, caches |
| `BUS` (200) | The event bus |
| `CONSUMERS` (300) | `EventConsumer`s, `OutboxRelay`, `WebhookDispatcher`, WS/SSE adapters, `AuditConsumer` — the default for app modules |
| `SCHEDULERS` (400) | `AbstractJobRunner`, `RetentionScheduler`, JWKS background refresh |
| `EDGE` (500) | `SecurityPosture` preflight — observes the fully-started world |

`phase` is the coarse, anonymous order across packages that do not know each other (why
`MigrationsModule` starts before `CoreModule`'s bus no matter which one is listed first);
`depends_on` (049) remains the fine, *named* order within or across phases — Spring's
`SmartLifecycle` split (an integer phase + `depends-on`), for the same reason: phase alone
cannot express "a relay starts after whichever package provides the store", and `depends_on`
alone cannot order two modules that do not name each other.

A module contributes phased lifecycle units via `components(container) ->
Sequence[LifecycleComponent]` — the default wraps the module's own `start`/`stop` into one
component named/phased like the module; `CoreModule` overrides it to contribute four
(`core.stores`/INFRA, `core.bus`/BUS, `core.consumers`/CONSUMERS, `core.job_runner`/SCHEDULERS)
and `WebhookModule` two (`webhook.repository`/INFRA, `webhook.dispatcher`/CONSUMERS).

`create_varco_app` orders modules via `resolve_lifecycle_order()` (049's `depends_on` DFS, then
a **stable** sort by phase) and flattens every active module's `components()` via
`phased_components()` (same stable sort, over components). `varco_core.di.lifecycle.
LifecycleRunner` — the engine `VarcoLispan` now delegates to — is the six-invariant primitive:

1. Start in the given (phase, dependency) order.
2. Stop in the **exact reverse of what actually started** — the normal-shutdown path.
3. A partial-start failure rolls back already-started components in exact reverse too (the
   pre-050 bug: `VarcoLifespan`'s rollback used to stop in **forward** order).
4. A rollback `stop()` failure never replaces the original `start()` exception — it is attached
   via `BaseException.add_note()` and rollback continues.
5. Normal-shutdown `stop()` failures are collected (`runner.last_failures`), never raised — an
   `asynccontextmanager`'s `finally` is the wrong place to raise during ASGI shutdown.
6. The `shutdown=` hook (`container.ashutdown()`) always runs last, even after a `setup()`/
   `start()` failure, and never raises.

`ModulePhaseError` is raised by `resolve_lifecycle_order()` the moment a `depends_on` edge would
require a module to start after something scheduled for a **later** phase — a wiring mistake
named at `create_varco_app()` time, never silently rescheduled.

**The `@Disposes`-or-owner rule**: every provider-produced singleton with a `stop()`/`aclose()`/
`close()`/`shutdown()` method needs either a `@Disposes` in the `@Configuration` that declares
its provider (one per produced type, idempotent teardown — disposal is the *backstop* for a
half-built app or a container used without a lifespan, never the primary ordering mechanism), or
— when a `VarcoModule`'s own `start()`/`stop()` owns that object's lifecycle directly (the two
`varco_ws` push adapters, `ReloadingTrustStore`) — a `module_owned: ClassVar[frozenset[type]]`
entry on that package's `di_wiring` conformance subclass. Both are enforced by
`testkit/varco_conformance/di_wiring.py`'s
`test_every_lifecycle_bearing_provider_has_a_disposer_or_owner`.

`bind_instance(container, Interface, obj, *, disposer=...)` is the P-DISP shim
(`design/upstream-gaps/providify-provide-disposer.md`) for a caller-constructed object that
needs a teardown hook and has no `@Configuration` of its own (`enable_redis_token_revocation`'s
`RedisTokenRevocationStore` is the motivating case) — delete it once providify ships
`provide(disposer=)` natively.

### Pitfalls (Lifecycle phases)

| Pitfall | Fix |
|---|---|
| A `depends_on` pointing at a module scheduled for a later phase | `ModulePhaseError` at `create_varco_app()` time — move the dependency to an earlier/equal phase, or drop the edge. |
| A user module left at the default `CONSUMERS` phase that opens a store other modules depend on | Works (the bus is up by then) but is fragile — declare `INFRA` explicitly if anything depends on it. |
| `enable_*` constructing a connection-holding object without `disposer=` | Leaks at every `container.ashutdown()` — pass `bind_instance(..., disposer=obj.aclose)`. |
| A Kafka/NATS bus resolved by hand and never started | `publish()` raises `RuntimeError` mentioning `start()` — no more silent hang or first-resolution auto-start (`@PostConstruct` removed in 4.0). |
| `SAModule(ensure_tables=True)` under an Alembic-managed schema | Harmless (`checkfirst`) but redundant — set `ensure_tables=False` when Alembic owns every framework table this deployment uses. |
