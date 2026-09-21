# `FALLBACK_SHADOWED` is never emitted for a scanned-class default shadowed at its ABC

**ID**: P28-FALLBACK-SHADOWED-SCANNED-CLASS
**Raised by**: varco, Plan 045 / DI-15 close-out (§D-proof)
**Date**: 2026-09-14
**providify version**: 2.1.0 (`3cb207f`)
**Status**: ⛔ Open — filed, not worked around
**Priority**: 🟡 should — gates Plan 052's `varco diagnose` "which defaults are live?" report for
22 of varco's own default sites (every scanned `@Singleton class Impl(ABC)` default)
**Gates**: BACKLOG `DI-15` (interim close-out), feeds Plan 052.

---

## 1. What providify does today

A scanned `@Singleton` class is registered with `interface = cls` itself, never the ABC it
implements: `DIContainer.register(cls)` (`providify/container.py:1248` in the 2.1.0 tree —
confirmed on the installed copy, `.venv/lib/python3.12/site-packages/providify/container.py`)
constructs `ClassBinding(cls, cls)` — both the interface and implementation slots are the
concrete class. `get(ABC)` still resolves such a binding correctly, but only because
`_binding_serves()`'s `issubclass` match (`container.py:2138` in this plan's citation, confirmed
present) treats `cls` as serving any base class request — the binding's own *recorded* interface
is never the ABC.

`FALLBACK_SHADOWED` is computed per-binding, for the binding's own "natural request"
`(b.interface, b.qualifier, None)` (`container.py:6360-6370`, the Pass 1d emitter). For a
`@Provider`-shaped default (`b.interface` is literally the ABC, because `provide(fn, returns=ABC)`
or a `@Provider -> ABC` annotation records the ABC directly), this natural request is exactly what
an application's own `ABC` binding shadows — the INFO fires correctly. For a scanned-class
default, the natural request is `(Impl, qualifier, None)` — the concrete class — which an
application's `bind(ABC, UserImpl)` binding never serves (an `ABC` binding does not serve a
concrete-`Impl` request), so no non-fallback binding is ever a candidate for `Impl`'s own natural
request, and `FALLBACK_SHADOWED` is never emitted for it — even though `get(ABC)` correctly
returns `UserImpl`.

## 2. Why it is a gap

Plan 045 converts every one of varco's ~31 in-process/backend default bindings to `@Fallback`.
22 of them are scanned-class defaults (per Plan 045's §D-proof site enumeration: A1-A12, A14, B1,
B2, B4-B6, B9, B12, B13, D1-2) — the majority. Plan 052's planned `varco diagnose` report renders
`FALLBACK_SHADOWED` to answer "which framework defaults are actually live, and what shadowed
them?" For these 22 sites, that report will show **nothing** even when an application has in fact
overridden the default — a false "everything is default" read for the majority of varco's own
seams, not a minority edge case.

**Measured, not assumed** (per CLAUDE.md's "verify the claim in providify's own source" rule):

```python
from providify import DIContainer, Fallback, Singleton

class Base: ...

@Fallback
@Singleton
class Impl(Base): ...

class Other(Base): ...

c = DIContainer()
c.register(Impl)
Singleton(Other)
c.bind(Base, Other)

report = c.validate(raise_on_error=False)
shadowed = [i for i in report.infos if i.kind.value == "fallback_shadowed"]
print(len(shadowed))   # -> 0
```

Run against the installed providify 2.1.0 (Plan 045 Step 1(e)): **0** — confirming the blind spot
exists exactly as predicted from source, not merely inferred from it.

## 3. The Step 11 xfail body

`varco_core/tests/test_fallback_defaults.py` (Plan 045 Step 10/11) pins the gap as a `strict=True`
xfail with `raises=AssertionError`, reusing the reproduction above verbatim (scanned `@Fallback
@Singleton class Impl(Base)`, `register(Impl)`, `bind(Base, Other)`, then asserting
`len(shadowed) == 1` — which is currently `0`, so the assertion fails and the xfail is honoured).
This is deliberately the *opposite* assertion of the reproduction script above (which prints the
count); the test encodes what *should* be true once fixed, not what is true today.

## 4. The ask

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) Evaluate the natural request against every base class in `Impl.__mro__` that has at least one other active binding** — for a scanned `ClassBinding(cls, cls)`, also check `(base, qualifier, None)` for each `base` in `cls.__mro__[1:]` that appears as another binding's interface anywhere in the container, and report shadowed if any of those requests is won by a non-fallback | Catches exactly the varco shape (an ABC with a fallback impl and a user impl bound at the ABC) without requiring a registration-shape change at any of the 22 sites | `__mro__` walk is `O(depth)` per binding per `validate()` call — bounded and cheap, but a departure from the current single-request model; needs a decision on which bases qualify ("has at least one other binding" vs. "every base unconditionally," which would report noise for `object`/mixins) |
| **(2) Record the ABC at `register()` time** — when scanning a class, if it has exactly one non-`object`, non-mixin base that is itself referenced as an interface elsewhere (or is abstract), store that as a secondary "reportable interface" on the `ClassBinding`, consulted only by `validate()`'s `FALLBACK_SHADOWED` pass, never by lookup matching (`_binding_serves` stays `issubclass`-based, unchanged) | No per-lookup cost; purely a `validate()`-time concern | Requires provify to guess "which base is the seam" — ambiguous for multiple inheritance sqrt cannot always be right; varco could get this wrong for a class with two ABC bases |
| **(3) Do nothing; `describe()`/reflection can already enumerate `Impl.__bases__` for a human to correlate manually** | Zero implementation cost | Defeats the entire purpose of an automated wiring report — the exact failure mode Plan 052 exists to prevent |

**Recommendation for the maintainer**: (1) as the primitive, scoped to bases that have at least one
other *active* binding in the container (avoids reporting noise for `object`/pure mixins); (2) as
an alternative if the MRO walk's cost is judged too high; (3) rejected — it reintroduces the manual
correlation problem `varco diagnose` exists to remove.

## 5. Semantics this ask assumes

One `FALLBACK_SHADOWED` INFO per `(base, qualifier)` request that is *actually* shadowed — not
one per binding regardless of whether any base has a competing binding. A scanned `Impl` with no
ABC, or whose ABC has no other binding anywhere in the container, must not emit spurious INFOs; the
rule stays "expected for a default — no action needed" only when there is something to compare
against.

## 6. Partly ours? No.

Nothing in providify's current public API lets varco declare "my interface, for reporting
purposes, is `AbstractEventBus`" on a scanned class — the only way to make `b.interface` literally
the ABC is to switch every scanned-class default to a `bind()`/`@Provider` shape instead, which
Plan 045 explicitly rejected for these 22 sites (a shape change with no other benefit, made purely
to satisfy a diagnostic that the ABC itself is the one providify already reads for `get()`).
There is no interim varco-side fix that does not change how these 22 bindings are registered.

## 7. Interim workaround — none applied

Plan 052 (`varco diagnose`) is told, in its own plan, to read this report: it renders the
provider-shaped `FALLBACK_SHADOWED` INFOs directly, and for scanned-class defaults it lists them
from Plan 047's backend/default catalogue instead (a static, hand-maintained enumeration of "these
22 classes are varco's own scanned defaults") rather than from `validate()`'s output. No varco
shim is added to providify's resolution or validation path.

## 8. Guard

`varco_core/tests/test_fallback_defaults.py` — a `strict=True` xfail, `raises=AssertionError`,
`reason="GAP: P28-FALLBACK-SHADOWED-SCANNED-CLASS — providify never emits FALLBACK_SHADOWED for a
scanned-class default shadowed at its ABC — see design/upstream-gaps/
providify-fallback-shadowed-scanned-class.md"`. No Docker, no backend — pure providify, runs in
every `make test PKG=varco_core`.
