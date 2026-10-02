# P32 — `DIContainer.copy()` never copies `_collection_points`, breaking every `get()`/`aget()` call on the copy once any `@Multibound` type exists

**Filed**: 2026-09-30 (Plan 052 / DI-11, Step 11 — the single-override test recipe)
**Library**: `providify` 2.1.0 (installed copy, `/home/edoardo/projects/providify`)
**Severity**: High — `copy()` is providify's own documented "suitable for test overrides"
primitive (its own docstring, `container.py:7801`), and it is unusable on any container that
has registered at least one `@Multibound` collection point (varco ships five as of Plan 051:
`EventConsumer`, `RetentionPolicy`, `TenantSource`, `PostureCollector`, `HealthCheck` —
`varco_core.di.collect.collection_seams()`), which is every bootstrapped `varco_core`
container.

## 1. What upstream does today (verified in source, 2026-09-30)

`DIContainer.__init__` initialises the instance-level bookkeeping set that backs
`multibind()`/`@Multibound` collection-point tracking:

```python
# container.py:875
self._collection_points: set[Any] = set()
```

populated by `multibind()`:

```python
# container.py:1403
self._collection_points.add(cls)
```

and read, unconditionally, on **every** `get()`/`aget()`/`get_all()`/`aget_all()` call, as the
very first branch inside `_multibound_inner()` -> `_is_collection_point()`:

```python
# container.py:1725
return cls in self._collection_points or _is_multibound(cls)
```

`DIContainer.copy()` (`container.py:7800-7900+`) deliberately bypasses `__init__` via
`DIContainer.__new__(DIContainer)` — its own docstring says so ("use `__new__` to bypass
`__init__` entirely... we only want to clone the binding list") — and then manually sets every
instance attribute `__init__` would have set: `_bindings`, `_singleton_cache`,
`_singleton_order`, `_singleton_locks`, `_singleton_lock_guard`, `_async_singleton_locks`,
`scope_context`, `_scanner`, `_validated`, `_localns_cache`, `_hints_cache`,
`_enabled_alternatives`, `_active_profiles`, `_interceptor_classes`, `_observers`,
`_tracked_dependents`, and the hook dict — **but never `_collection_points`**. The copy's own
docstring names this risk explicitly and accepts it as a known trade-off ("❌ Fragile if
`__init__` adds new attributes in the future — this method must be kept in sync"), but
`_collection_points` is not new: it has existed since `__init__` line 875, in the same file,
the same class, for the lifetime of the multibinding feature — this is not a recent regression,
it is an omission in the original `copy()` implementation.

## 2. Minimal reproduction (no Docker, `varco_core` only)

```python
from providify import DIContainer, Multibound, Singleton


@Multibound
class Seam: ...


@Singleton
class Contribution(Seam): ...


base = DIContainer()
base.register(Contribution)
base.multibind(Seam)
clone = base.copy()

clone.get(list[Seam])
# AttributeError: 'DIContainer' object has no attribute '_collection_points'.
# Did you mean: '_is_collection_point'?
```

`_multibound_inner()` (`container.py:1730-1749`) only reaches `_is_collection_point()` for a
`list[T]`-shaped request — a *plain* `clone.get(Contribution)` does not touch
`_collection_points` at all and is unaffected. But this is the exact shape varco's own
`varco_core.di.collect.collect()`/`acollect()` use (`container.get(list[seam])`,
`collect.py:149,165`) for every one of the five `@Multibound` seams (`EventConsumer`,
`RetentionPolicy`, `TenantSource`, `PostureCollector`, `HealthCheck`), and it is also what
providify's own `AbstractEventBus`/event-dispatch path requests internally
(`list[EventMiddleware]`) — which is how this was actually discovered (Plan 052 Step 11's
single-override recipe test failed with this exact `AttributeError` while resolving an
ordinary, unrelated `@Singleton` service that merely depended on `AbstractEventBus`).

## 3. Why this is a gap, not a varco misuse

- `copy()` is providify's own documented mechanism for "test overrides" (its docstring's own
  words and worked example, `container.py:7833-7836`), and its very next line after that example
  builds exactly the pattern Plan 052's `varco diagnose` recipe uses: `base =
  build_app_container(); test_c = base.copy()`.
- varco never reaches into `_collection_points` itself, does not monkeypatch `copy()`, and does
  not construct `DIContainer` via `__new__` anywhere in this repo (`rg -n '__new__'
  varco_*/varco_*` has zero hits in DI code) — this is purely an upstream omission.
- The bug is latent until a container has **any** `@Multibound` type registered — which is why
  it was not caught earlier in this repo's own test suite: Plan 045/046/047/048's conformance
  work predates Plan 051's five collection seams, and `copy()` itself had no direct varco test
  before Plan 052 needed it for the first time.

## 4. Blocks

Plan 052 (DI-11, §D-testprofile)'s single-override recipe — the one the README and
`dependency-injection.md` §Testing document verbatim:

```python
@pytest.fixture(scope="session")
def app_container():
    c = DIContainer()
    register_modules(c, in_memory_modules())   # registers varco_core, which has 5 @Multibound seams
    return c

@pytest.fixture
def di_container(app_container):
    c = app_container.copy()                   # <- raises on the very next get()/aget()
    yield c
```

## 5. The ask — candidate fixes

- ✅ **(a) — copy `_collection_points` in `copy()`.** One line:
  `new._collection_points = set(self._collection_points)`, in the same place every other
  instance-local set/dict is copied (`_enabled_alternatives`, `_interceptor_classes`). Mirrors
  the existing pattern exactly; zero new API surface; zero behaviour change for any container
  that has no `@Multibound` types (the set stays empty either way).
- ✅ **(b) — a dedicated regression test in providify's own suite** asserting
  `copy().get(AnyRegisteredMultiboundContribution)` does not raise, guarding against this
  specific omission recurring if `__init__`'s attribute list grows again.
- ❌ **(c) — do nothing, document "call `scan()` again on the copy instead of `copy()`".**
  Rejected: re-scanning defeats `copy()`'s entire stated purpose ("No module re-scan, no
  double-registration from the auto-scan path" is listed as one of `copy()`'s own `✅` benefits
  in its docstring) and would mean every fresh-per-test container pays a full package scan,
  exactly what `copy()` exists to avoid (Plan 052's own §D-testprofile: "`copy()` is what makes
  'bootstrap once per session, fresh per test' cheap").

## 6. Is this partly ours?

No. This is a pure omission inside `copy()`'s own attribute list, in a code path varco never
touches directly (varco only ever calls the public `container.copy()` method).

## 7. Interim workaround (until fixed upstream)

Centralized in exactly one place — `varco_core/tests/test_single_override_recipe.py`'s
`di_container` fixture manually re-attaches the copy's `_collection_points` set immediately
after calling `.copy()`, citing this report. This is a **test-only** workaround (the fixture the
README/`dependency-injection.md` §Testing recipe shows verbatim); it is not a production shim
and is deleted the same day providify ships fix (a).

```python
c = app_container.copy()
c._collection_points = set(app_container._collection_points)  # P32 workaround — see report
```

## 8. Guard

`varco_core/tests/test_providify_contracts.py::TestP32CopyCollectionPoints::test_copy_preserves_collection_points`
— a `strict=True`, `raises=AttributeError` xfail reproducing §2's minimal case. Flips to a
passing test (and the guard is deleted, per the standing convention) the day providify ships fix
(a); a regression in this specific behaviour is then caught immediately by `make test`, no
Docker required.
