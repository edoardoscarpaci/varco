# P30-PROVIDE-DISPOSER — `provide()`/`@Provider` accept no `disposer=` kwarg

**Library**: `providify` 2.1.0 (installed at `/home/edoardo/projects/providify`, read 2026-09-27)
**Filed by**: Plan 050 (Phased lifecycle and DI-wired infrastructure), §D-asks / Step 8

## 1. What upstream does today (verified in providify's own source)

`DIContainer.provide(fn, *, returns=None)` — `providify/container.py:1250` — takes exactly
two parameters: the factory and an optional `returns=` interface override. There is no
`disposer=` keyword:

```python
# providify/container.py:1250
def provide(self, fn: Callable[..., Any], *, returns: Any = None) -> ProviderBinding:
```

`ProviderBinding.disposer` (`providify/binding.py:749`) is a plain, public attribute —
`self.disposer: Callable[..., Any] | None = None` — set to `None` at construction and never
exposed through any constructor keyword. The **only** code in providify that ever assigns a
non-`None` value to it is the `@Configuration` install loop that matches a `@Disposes`-marked
method against the binding it declares:

```python
# providify/container.py:7379
binding.disposer = getattr(instance, name)
```

That assignment only fires for a method carrying `@Disposes(SomeInterface)` **inside a
`@Configuration` class that is `install()`ed** — there is no equivalent path for a binding
created via the bare `container.provide(fn)`/`@Provider` function-level API, which is exactly
the shape `varco_core.di.bind_instance()` (Plan 046) uses to register an already-constructed,
caller-owned object.

`_adispose` (`providify/container.py:5280-5304`) is the consumer of `binding.disposer` — it
dispatches on `inspect.iscoroutinefunction(binding.disposer)` and calls
`await binding.disposer(instance)` or `binding.disposer(instance)` accordingly. This part of
the mechanism is generic and would work perfectly with a caller-supplied disposer if one could
be attached at registration time.

## 2. Why this is a gap

`bind_instance(container, Interface, obj)` is the sanctioned way to register an
already-constructed object (Plan 046). Several varco objects registered this way hold a live
connection that must be closed at `container.ashutdown()`:

- `varco_redis.di.enable_redis_token_revocation()` constructs a `RedisTokenRevocationStore`
  (which opens an `aioredis.Redis` client eagerly in `__init__`) and had no way to attach a
  disposer — before Plan 050, this client leaked at every shutdown.
- Any future `enable_*`/`bind_*` helper that constructs a connection-holding object by hand hits
  the identical wall.

Without `disposer=`, the only two options are: (a) route the object through a full
`@Configuration`/`@Provider`+`@Disposes` pair instead of a plain caller-constructed instance
(defeats the entire purpose of `bind_instance` — "an already-constructed object", Plan 046 §D-bind),
or (b) leak the resource.

## 3. Minimal reproduction

```python
from providify import DIContainer

container = DIContainer()
calls = []

def factory() -> str:
    return "value"

# TypeError: provide() got an unexpected keyword argument 'disposer'
container.provide(factory, returns=str, disposer=lambda v: calls.append(v))
```

Guarded by `varco_core/tests/test_providify_contracts.py::
TestP050UpstreamAsks::test_provide_accepts_disposer_kwarg` (`strict=True`, `raises=TypeError`).

## 4. The ask

Add a `disposer=` keyword to `provide()` (and, symmetrically, to `@Provider(disposer=...)` for
the decorator-attached case), assigning `binding.disposer` exactly as the `@Configuration`
install loop already does:

```python
def provide(self, fn, *, returns=None, disposer=None) -> ProviderBinding:
    binding = ProviderBinding(fn, returns=returns)
    ...
    if disposer is not None:
        binding.disposer = disposer
    ...
```

| Candidate | ✅ | ❌ |
|---|---|---|
| **`provide(fn, *, returns=, disposer=)`** | Additive, symmetric with the existing `returns=` override; works uniformly for `bind_instance`-shaped registration; no change to `_adispose`'s dispatch, which already handles any callable | One more optional kwarg on an already-two-kwarg method |
| `@Provider(disposer=...)` (decoration-time) | Consistent with `@Provider(returns=...)` | Needs the disposer to be an *instance-bound* method reachable at decoration time, which is exactly the shape `bind_instance`'s already-constructed object does *not* have (the disposer is `obj.aclose`, a bound method of the object being registered, known only at `provide()` call time, not at decoration time) — doesn't cover the motivating case |

Recommendation: **`provide(fn, *, returns=, disposer=)`** — it directly covers the `bind_instance`
motivating case; the decorator form can be added later as a convenience for the
`@Configuration` shape without conflicting.

## 5. Is this partly ours?

**Partly.** Two of the three disposer gaps this plan's inventory found (rows 17-18 — the
`varco_ws` push adapters, `ReloadingTrustStore`) are solved with **no** upstream change at all —
module ownership (a `VarcoModule`'s own `start()`/`stop()`) is the correct home for an object
whose lifecycle a *module*, not providify's container, drives. Only row 16
(`enable_redis_token_revocation`'s caller-constructed `RedisTokenRevocationStore`) genuinely
needs a disposer attachable to a `bind_instance`-registered object with no other sanctioned way to
reach it.

## 6. Interim workaround (shim, named for deletion)

`varco_core/varco_core/di/__init__.py::bind_instance(container, interface, instance, *,
qualifier=None, disposer=None)` — the parameter is added directly on varco's own
`bind_instance` wrapper, and a private `_attach_disposer(binding, disposer)` assigns
`binding.disposer = disposer` on the `ProviderBinding` `container.provide()` returned, exactly
mirroring `container.py:7379`'s own assignment. Both are explicitly documented as a **SHIM for
P30** — delete `disposer=` from `bind_instance`'s signature and `_attach_disposer` itself once
`provide(disposer=)` ships natively, and route `bind_instance` through it instead.

## 7. Guard

`varco_core/tests/test_providify_contracts.py::TestP050UpstreamAsks::
test_provide_accepts_disposer_kwarg` — `strict=True`, `raises=TypeError`. The moment providify
adds `disposer=`, this test starts passing and pytest reports it as `XPASS`, which `strict=True`
turns into a hard failure — impossible to silently miss the fix landing.

## 8. Status at 4.0.0 (Plan 058 audit)

Open. Filed by Plan 050 under a working name ("P-DISP"/"P-INJ"); the ledger id (P30/P31) is now
also in the guard's `reason=` (`GAP: P3x-…`) so `rg -n 'GAP: P'` counts it. Named in
`CHANGELOG.md` `[4.0.0]` → Upstream; the guard above is still `xfailed`.
