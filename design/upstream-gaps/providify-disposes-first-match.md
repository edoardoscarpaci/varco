# `@Disposes` wiring attaches to the first matching binding, not the caller's own

**ID**: P24-DISPOSES-FIRSTMATCH
**Raised by**: varco, Plan 024 Phase 1 (C2 — `@Disposes` adoption), Step 16
**Date**: 2026-09-02
**providify version**: 2.0.1 (as resolved in `uv.lock`)
**Status**: ✅ Resolved (2026-09-14, Plan 045 / DI-15 close-out) — see §7
**Priority**: 🟡 should. Not a crash; a silent resource leak when two
`@Configuration`s in the same container both bind the same interface via
`@Provider`, each with its own `@Disposes` for that interface.
**Re-verified**: 2026-09-12 against providify 2.0.1 @ `1402f0a` (Plan 043 / DI-15) — loop
unchanged.
**Gates**: BACKLOG `DI-9`.

---

## 1. What providify does today

`DIContainer.install()`'s disposer-wiring loop (`providify/container.py:6201-6214`):

```python
# Wire @Disposes teardown methods to their ProviderBinding
for name, fn in vars(module_cls).items():
    if not callable(fn):
        continue
    disposes_marker = _get_disposes_marker(fn)
    if disposes_marker is None:
        continue
    disposed_type = disposes_marker.disposed_type
    for binding in self._bindings:
        if isinstance(binding, ProviderBinding) and _interface_matches(
            binding.interface, disposed_type
        ):
            binding.disposer = getattr(instance, name)
            break
```

For every `@Disposes(X)` method found on the module being installed, this walks
**all** `self._bindings` (every binding in the *entire container*, not just the
ones this `install()` call is registering) and attaches the disposer to the
**first** `ProviderBinding` whose interface matches `X`, then `break`s.

The loop lives inside `_register_module_providers(module_cls, instance)`
(`providify/container.py:6160`), the shared tail both the sync and async install paths funnel
through. Every caller, found by
`rg -n '_register_module_providers' /home/edoardo/projects/providify/providify/container.py`:

- `providify/container.py:6135` — `_install_one()`, the sync path (`DIContainer.install()`'s
  per-class loop tail).
- `providify/container.py:6157` — `_ainstall_one()`, the async path (`DIContainer.ainstall()`'s
  per-class loop tail).

Both callers register providers and wire `@Disposes` through the identical
`_register_module_providers` body — the defect below applies to `install()` and `ainstall()`
equally, not only the async path this report originally cited.

## 2. Why this is a gap

When two independent `@Configuration`s both produce the same interface via
`@Provider` and each carries its own `@Disposes` for that interface — a real
and reasonable shape (varco has exactly this: `RedisCacheConfiguration` and
`RedisLayeredCacheConfiguration` both bind `CacheBackend`) — installing the
second configuration's disposer-wiring loop finds the **first** matching
`CacheBackend` binding in the container (which is the *first* configuration's
own binding, already correctly wired to its own disposer) and **overwrites**
it with the second configuration's disposer. The second configuration's own
binding — the one the loop was ostensibly wiring for — is left with
`disposer = None` and leaks.

Concretely, with `RedisCacheConfiguration` installed first and
`RedisLayeredCacheConfiguration` second:

1. `install(RedisCacheConfiguration)` registers binding **A** (`CacheBackend`
   → `RedisCache`), then wires `A.disposer = RedisCacheConfiguration.close_cache`
   (correct — the only `CacheBackend` binding so far is A itself).
2. `install(RedisLayeredCacheConfiguration)` registers binding **B**
   (`CacheBackend` → `LayeredCache`), then wires its own `close_cache`: the
   loop scans `self._bindings`, finds **A** first (not B), and sets
   `A.disposer = RedisLayeredCacheConfiguration.close_cache`. **B never
   receives a disposer.**

At `ashutdown()`, `A`'s disposer — now `RedisLayeredCacheConfiguration.close_cache`
— is invoked with **A's own produced instance** (`binding.disposer(instance)`,
`providify/container.py:4541-4546`, where `instance` is looked up from the
binding's own singleton cache entry). Since `close_cache(self, cache)` just
does `await cache.stop()` on whatever `cache` it is handed, this *happens* to
call `RedisCache.stop()` on binding A's redis instance — so A is torn down,
coincidentally, by the wrong method. But **B (the `LayeredCache` instance) is
never stopped at all** — its L1 and L2 backends leak silently.

Reproduced in `varco_redis/tests/test_redis_cache_disposes.py::test_both_cache_configurations_installed_together_both_get_stopped`:
only `"redis"` appears in the observed stop-order list; `"layered"` never
does.

## 3. What the report §D-C2-firstmatch predicted vs. what happened

The design note that anticipated this edge case (`plans/024-3-0-1-cleanup.md`
§D-C2-firstmatch) predicted the outcome would be "benign" because both
disposers happen to call `await backend.stop()` on *a* `CacheBackend`. That
prediction is **wrong**: the disposer that gets attached to the winning
binding calls `stop()` on that binding's own instance (not the other
configuration's), so the winning binding *is* torn down correctly — but the
**losing binding's disposer slot is simply never populated**, leaving its
instance with no teardown path whatsoever. This is a genuine leak, not a
redundant-but-harmless double-stop.

## 4. The ask

`_register_module_providers` already has the information it needs to scope the search correctly,
because `self.provide()` is called for each of *this module's own* providers
(`providify/container.py:6196`, `:6199`) inside the **same function**, appending to
`self._bindings` — the provider-registration loop starts at `providify/container.py:6176`.

- ✅ **Preferred, minimal**: record `start = len(self._bindings)` before the provider-registration
  loop (`providify/container.py:6176`), and scope the disposer search
  (`providify/container.py:6209`, `for binding in self._bindings:`) to
  `self._bindings[start:]` instead of the whole list. No signature change, no new state, ~3
  lines. This is possible only because `provide()` currently returns `None`
  (`providify/container.py:1048`) — nothing else identifies which bindings this call produced,
  so the slice is the cheapest correct fix available today.
- ✅ **Alternative, larger**: change `provide()` to return the `ProviderBinding` it created
  (`providify/container.py:1048` currently `-> None`). This also serves `DI-13`'s "every
  `@Disposes` reachable" test directly — the disposer-wiring loop could then match against the
  small set of bindings `_register_module_providers` itself just created, holding direct
  references rather than re-deriving a slice.
- ✅ **Detect, not just fix**: add an `IssueKind.DISPOSER_OVERWRITTEN` warning, sibling of the
  shipped `IssueKind.UNREACHABLE_PRE_DESTROY` (`providify/container.py:5327`), raised when a
  disposer-wiring pass would replace a non-`None` `disposer` on a binding **not** registered by
  this `install()`/`ainstall()` call — this is precisely the class of finding `DI-7`'s wiring
  report should surface, independent of whether the "Preferred" fix above also lands.
- ❌ **Not requested**: any change to `@PreDestroy`/producer-method semantics —
  that gap (P22-PROVIDER-PREDESTROY) is settled as intentional and closed
  separately; this is a distinct wiring-loop defect in `@Disposes` itself.

## 5. Interim workaround — none applied in varco

There is no supported, non-hacky way for application code to control which
binding a `@Disposes` attaches to — the loop is purely internal to
`install()`. varco does **not** work around this (per this repo's norm): the
affected test is marked `xfail(strict=True)` so the fix cannot land
unnoticed, and both `RedisCacheConfiguration` and
`RedisLayeredCacheConfiguration` continue to ship their own `@Disposes`
methods (correct in isolation — the bug only manifests when **both**
configurations are installed into the **same** container, which is an
unusual, not-default composition). A future consumer that installs both
should be aware `LayeredCache`'s pool may leak on `ashutdown()` until this is
fixed upstream.

## 6. Guard

`varco_redis/tests/test_redis_cache_disposes.py::test_both_cache_configurations_installed_together_both_get_stopped`
— `xfail(strict=True, reason="BUG: providify @Disposes wiring attaches to the
first matching binding across the whole container, not the installing
module's own — see design/upstream-gaps/providify-disposes-first-match.md")`.
No Docker required (monkeypatched I/O), runs in every `make test`.

## 7. Resolution (2026-09-14)

providify 2.1.0's `CHANGELOG.md:14-31` ships the "Alternative, larger" fix from §4 above:
`_register_module_providers`'s disposer-wiring loop is now scoped to the bindings *this*
`install()`/`ainstall()` call itself registered (the `own` list, `providify/container.py:7321`,
consulted by the scoped loop at `:7359-7360`) rather than every binding in the container — the
exact "Preferred, minimal" shape asked for, generalized. Two new `IssueKind`s ship alongside it:
`UNMATCHED_DISPOSER` and `DISPOSER_OVERWRITTEN` (`providify/validation.py:102-109`, both
`Severity.WARNING`) — the "Detect, not just fix" ask. `DIContainer.provide()` also now returns the
`ProviderBinding` it created (`providify/container.py:1250`, was `None`) — the "Alternative,
larger" fix's own prerequisite, adopted independently of this report (Plan 045 §Context row
"`provide()` returns the binding").

**Behaviour change, audited**: `CHANGELOG.md:22-24` notes a `@Disposes(X)` on a module with no `X`
provider of its own no longer attaches to any binding elsewhere in the container (it used to
silently attach to the first matching binding anywhere — the very defect this report is about).
`rg -n '@Disposes' varco_*/varco_*` (site list above, §"Why this is a gap" audit) shows every
`@Disposes(X)` in varco is declared on the *same* `@Configuration` class that also provides an `X`
binding of its own — `RedisCacheConfiguration`/`RedisLayeredCacheConfiguration` (`CacheBackend`),
the four DLQ configurations (`AbstractDeadLetterQueue`), `RedisEventBusSelectorConfiguration`
(`AbstractEventBus`), `RedisBulkheadConfiguration` (`RedisBulkhead`),
`MemcachedCacheConfiguration` (`CacheBackend`) — so no varco site loses its disposer under the new
scoped-search behaviour; nothing changed in application code.

**varco adopted**: nothing to change in `RedisCacheConfiguration`/`RedisLayeredCacheConfiguration`
— both configurations' own `@Disposes(CacheBackend)` methods were already correct in isolation; the
bug was entirely in providify's cross-container search. The guard is converted in Plan 045 Step 6
(`varco_redis/tests/test_redis_cache_disposes.py`): the `xfail` marker is removed, the test now
asserts both `"redis"` and `"layered"` appear in the stop-order list, plus a new
`test_provide_returns_the_provider_binding` pinning the `provide()` return-value change, and a
`validate()` assertion that zero `UNMATCHED_DISPOSER`/`DISPOSER_OVERWRITTEN` issues are reported for
this configuration pair.
