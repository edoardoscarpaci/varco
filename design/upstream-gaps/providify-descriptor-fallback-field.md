# P29-DESCRIPTOR-FALLBACK — `BindingDescriptor` carries no `fallback` field

**Library**: `providify` 2.1.0 (`/home/edoardo/projects/providify`)
**Filed**: Plan 046 (DI-13, §D-suite), 2026-09-21

## 1. What upstream does today

`providify/descriptor.py:49-54` defines `BindingDescriptor` with exactly these
fields: `interface`, `implementation`, `scope`, `qualifier`, `priority`,
`dependencies`. No `fallback` field.

`DIContainer.describe()` (`providify/container.py:7415-7418`) returns
`tuple(b.describe(self) for b in self._bindings)` — i.e. every public,
documented way to enumerate a container's bindings goes through
`BindingDescriptor`, which drops whether a binding is `@Fallback`.

`ClassBinding.fallback` / `ProviderBinding.fallback`
(`providify/binding.py:207`, `:723`) exist on the *binding* object itself and
are readable if you hold a direct reference to it (e.g. the return value of
`container.provide(...)`, or `container.get_binding(interface)`), but there is
no way to ask "list every fallback binding in this container" without reading
the private `container._bindings` list directly.

## 2. Why this is a gap

Plan 046's `di_wiring` conformance suite needs a `test_declared_defaults_are_complete`
assertion: "the set of `(interface, qualifier)` pairs this package registers
as `@Fallback` defaults is *exactly* `expected_defaults` — no more, no fewer."
That requires enumerating every fallback binding in a container. Every public
API (`describe()`, `get_binding()`, `get_all_bindings()`) either drops the
`fallback` bit entirely (`describe()`) or actively filters out shadowed
fallbacks once a user binding exists (`container.py:2159-2169`'s `_filter()`
fallback post-step), so none of them can answer the question.

The one working answer is `container._bindings` — a private, undocumented
attribute — centralised in exactly one function,
`testkit/varco_conformance/di_wiring.py::fallback_bindings()`, named as a shim
intended for deletion once this gap closes.

## 3. Minimal reproduction

```python
from providify import DIContainer, Fallback, Singleton

class Base: ...

@Fallback
@Singleton
class F(Base): ...

container = DIContainer()
container.bind(Base, F)

descriptor = container.describe().bindings[0]
assert descriptor.fallback is True  # AttributeError / AssertionError today
```

## 4. The ask

- ✅ **(a) Add `BindingDescriptor.fallback: bool` (+ `exact_only: bool`)** and
  a `DIContainerDescriptor.fallback_bindings` convenience property. Additive,
  no existing consumer breaks.
- ❌ **(b) `get_all_bindings(include_shadowed=True)` kwarg.** Smaller surface
  area, but changes a pure-read's filter contract and invites confusion about
  what the *default* call now means.

Preferred: (a).

## 5. Is this gap partly ours?

No. Plan 047's backend-selection catalogue will list the *declared* set of
defaults a package intends to register, but "are the declared ones all there
are" — i.e. did a scanned class or provider register an *undeclared* fallback
— is a question only the container itself can answer authoritatively.

## 6. Interim workaround

`testkit/varco_conformance/di_wiring.py::fallback_bindings(container)` reads
`container._bindings` directly. Single call site, documented as a shim,
guarded below.

## 7. Guard

`varco_core/tests/test_providify_contracts.py::TestP29DescriptorFallbackField::test_binding_descriptor_carries_fallback`
— `strict=True` xfail, `raises=AssertionError`.
