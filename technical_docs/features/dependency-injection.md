# Dependency injection — default bindings

> Plan 045 wrote the **Default bindings** section below. Plan 058 completes
> this document (contract, backend selection, multi-binding, lifecycle,
> testing).

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

## Pitfalls

| Pitfall | Fix |
|---|---|
| Marked a collection contribution (e.g. `HealthCheck`) `@Fallback` | Never do this — a fallback is excluded from `get_all()`/`aget_all()`/`list[T]` the instant any non-fallback sibling exists, which would silently empty `/health` the moment an app adds its own check. Collection contributions stay a plain qualified `@Singleton`. |
| Subclassed a `@Fallback` class and expected the subclass to also be a fallback | It is not — `FallbackMarker`/`_is_fallback` never walks the MRO (`__dict__`-only lookup, matching `@Profile`'s rule). Re-decorate the subclass explicitly if it should also be a fallback. |
| Resolved a `@Fallback` singleton *before* registering its shadowing binding, then wondered why the shadow "didn't take" | Not evicted — a fallback singleton already instantiated and cached stays cached even after a shadowing binding arrives later (the same documented `activate_profile()` caveat). `override()`/`reset_binding()` are the eviction tools; varco's own bootstrap order (scan defaults → install app → first `get()`) never hits this in practice. |
| Two backends scanned for the same seam (e.g. both `varco_kafka` and `varco_nats`) and expected one to deterministically win | Both are `@Fallback` at the same `BACKEND_PRIORITY` — they tie exactly as they did before this plan (first-registered wins), plus `validate()` now names the tie as `AMBIGUOUS_BINDING`. Not a regression; Plan 047 (config-driven backend selection) owns disambiguating this. |
| Copied a varco default's decorator line verbatim, including `@Fallback` | Your binding is now a fallback too — it ties with varco's own default by priority, then registration order, instead of winning outright. Drop `@Fallback` from your own bindings; omitting it is what makes a binding win. |
