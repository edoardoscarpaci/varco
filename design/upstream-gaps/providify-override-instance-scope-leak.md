# P33 — `ContainerOverrides.instance()` always binds `Scope.DEPENDENT`, which trips `_check_scope_violation` for any `SINGLETON` consumer using plain `Inject[T]`

**Filed**: 2026-09-30 (Plan 052 / DI-11, Step 11 — the single-override test recipe)
**Library**: `providify` 2.1.0 (installed copy, `/home/edoardo/projects/providify`)
**Severity**: Medium — blocks the officially documented "swap one binding" test recipe
(`ContainerOverrides.instance()`, `container.py`'s own worked example, `testing.py:119-150`)
whenever the overridden interface's *real* production binding is `@Singleton` (the common
case — every varco seam default is), and the consumer injects it with a plain `Inject[T]`
(also the common, recommended shape for a `SINGLETON`→`SINGLETON` dependency).

## 1. What upstream does today (verified in source, 2026-09-30)

`ContainerOverrides.instance()` (`testing.py:119-150`) always registers the replacement at
`Scope.DEPENDENT` — its own docstring says so explicitly: *"The provider has no `@Provider`
decoration, so `ProviderMetadata.default()` applies: `singleton=False` → `Scope.DEPENDENT`"*
(`testing.py:128-130`). There is no scope override parameter.

`DIContainer._check_scope_violation()` (`container.py:5653-5757`) — run automatically on every
container's *first* `get()`/`aget()` call via `validate_bindings()` (`container.py:5876-5900`),
and again (non-raising, folded into `ValidationReport.errors`) by `container.validate()`
(`container.py:6006+`, "Scope tier — `binding.validate(self)` is reused as-is; its structured
exceptions ... are unpacked into one `ValidationIssue` per underlying violation") — treats any
`SINGLETON` binding holding a direct (`Inject[T]`/bare-type) reference to a `DEPENDENT`-scoped
dependency as a scope leak (`Scope` ranking, `metadata.py:28-33`: `SINGLETON(1) < SESSION(2) <
REQUEST(3) < DEPENDENT(4)`), *unless* the consumer's parameter is wrapped in `Instance[T]`
(`container.py:5749-5757`: *"Instance[T] is exempt: the proxy defers resolution to `.get()` call
time — the SINGLETON stores the proxy, never a resolved instance"*).

**The conflict**: for any seam whose real production default is `@Singleton` (every one in
`varco_core` — `InMemoryCache`, `InMemoryEventBus`, ... — all `@Fallback @Singleton`), a
consumer correctly written as `def __init__(self, cache: Inject[CacheBackend])` (the ordinary,
recommended shape for a `SINGLETON`-to-`SINGLETON` dependency, and the shape every existing
varco service uses) becomes a **false-positive scope leak** the instant a test overrides that
interface with `ContainerOverrides.instance()` — not because the test double behaves unsafely
(the same object is returned on every resolution, functionally indistinguishable from a real
singleton), but purely because `instance()` hard-codes `Scope.DEPENDENT` with no way to say
"this one object is a stable substitute for a `SINGLETON`".

## 2. Minimal reproduction (no Docker, `varco_core` only)

```python
import asyncio
from providify import DIContainer, Singleton, Inject


@Singleton
class Dep:
    pass


@Singleton
class Consumer:
    def __init__(self, d: Inject[Dep]) -> None:
        self.d = d


async def main() -> None:
    c = DIContainer()
    c.register(Dep)
    c.register(Consumer)

    from providify.testing import ContainerOverrides

    overrides = ContainerOverrides(c)
    overrides.instance(Dep, Dep())  # the officially documented override verb

    await c.aget(Consumer)
    # providify.exceptions.ScopeViolationDetectedError: Scope leak detected from
    # binding Consumer with scope SINGLETON to reference Dep with scope DEPENDENT


asyncio.run(main())
```

`Dep` is genuinely `@Singleton` in production; the *only* thing that changed its effective scope
to `DEPENDENT` is the override mechanism itself.

## 3. Why this is a gap, not a varco misuse

- `instance()` is the verb `ContainerOverrides`' own docstring and `testing.py`'s worked example
  present as the primary override API, and it is what Plan 052's own §D-testprofile premise (1)
  documents as *the* recipe verb (chosen specifically because, unlike `.bind()`, it guarantees
  "the fake never ties with a `@Fallback` default").
- The check exists to prevent a genuinely narrower-lived value (request/session state, or a
  freshly-constructed-per-call value) from being captured stale inside a longer-lived singleton.
  A `.instance()` double returns the *exact same object* on every resolution (a plain closure
  over one captured value) — there is no staleness risk to protect against, yet it is scored
  identically to a truly `DEPENDENT` (per-call-fresh) dependency.
- No existing varco test (`rg -rln 'di_overrides\.' varco_*/tests/` — zero hits before this plan)
  had exercised `ContainerOverrides.instance()` against a `SINGLETON` consumer before Plan 052
  Step 11 needed it for the first time, which is why this had not surfaced earlier.

## 4. Blocks

Plan 052 (DI-11)'s single-override recipe test
(`varco_core/tests/test_single_override_recipe.py::test_uses_fake_cache`) exactly as originally
written — `OrderService` (a `@Singleton`) injecting `Inject[CacheBackend]` (production default
`@Fallback @Singleton(qualifier="in_memory") class InMemoryCache`) and asserting
`validation.errors == ()` after `di_overrides.instance(CacheBackend, FakeCache())`.

## 5. The ask — candidate fixes

- ✅ **(a) — an explicit scope parameter on `instance()`**, e.g.
  `instance(interface, obj, *, scope=Scope.SINGLETON)` (default unchanged, so existing callers
  see no behaviour change), letting a caller declare "this double is stable for the container's
  lifetime" and register it accordingly.
- ✅ **(b) — exempt a `.instance()`-registered binding from `_check_scope_violation()` by
  construction** — e.g. a private marker on the `ProviderBinding` recording "this was a
  closure-captured, identical-every-time double", checked alongside the existing
  `isinstance(inject_marker, InstanceMeta)` exemption at `container.py:5749`. No new public API;
  purely an internal refinement of what counts as "safe to hold".
- ❌ **(c) — do nothing, document "wrap every consumer parameter in `Instance[T]`".** Rejected
  as the general answer: it works (§7's workaround), but it means *every* service in an app must
  be written defensively against a scope mismatch that only exists because of how tests override
  bindings — the ordinary, recommended `Inject[T]` shape becomes untestable via the documented
  recipe for any `SINGLETON` seam, which is most of them.

## 6. Is this partly ours?

No. `OrderService`'s `Inject[CacheBackend]` shape matches every other `@Singleton`-consuming
`@Singleton` in this codebase (e.g. `CoreModule`'s own internals resolve `AbstractEventBus`,
also `@Fallback @Singleton`, the same way) — it is not a varco-side misuse, it is the ordinary
shape the check's own false positive appears for.

## 7. Interim workaround (until fixed upstream)

`varco_core/tests/test_single_override_recipe.py`'s `OrderService` fixture class uses
`Instance[CacheBackend]` instead of `Inject[CacheBackend]` for the one parameter under test,
resolving it once in `__init__` via `.get()` — providify's own documented exemption path
(§1's citation). This is a **deviation from the plan's own prose** ("a local `@Singleton`
injecting `Inject[CacheBackend]`"), reported here rather than silently applied; the recipe shown
in the README/`dependency-injection.md` §Testing should note the same substitution for any
consumer of a seam whose production default is `@Singleton`, until fix (a) or (b) lands.

## 8. Guard

`varco_core/tests/test_providify_contracts.py::TestP33OverrideInstanceScopeLeak::
test_instance_override_trips_scope_leak_on_singleton_consumer` — `strict=True`,
`raises=ScopeViolationDetectedError`, `reason="GAP: P33 — …"`. The moment
`ContainerOverrides.instance()` stops binding `Scope.DEPENDENT` (or `_check_scope_violation()`
exempts it), pytest reports `XPASS`, which `strict=True` turns into a hard failure. Status at
4.0.0 (Plan 058 audit): open; named in `CHANGELOG.md` `[4.0.0]` → Upstream.
