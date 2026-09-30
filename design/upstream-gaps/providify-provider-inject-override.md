# P31-PROVIDER-INJECT-OVERRIDE — no parameter-injection override for a runtime-only type

**Library**: `providify` 2.1.0 (installed at `/home/edoardo/projects/providify`, read 2026-09-27)
**Filed by**: Plan 050 (Phased lifecycle and DI-wired infrastructure), §D-asks / Step 8

## 1. What upstream does today (verified in providify's own source)

Under PEP 563 (`from __future__ import annotations`, which every varco module carries per
CLAUDE.md's coding standard), a parameter's type annotation is a lazy string that providify
must evaluate before it can decide whether the parameter is an injection point. That
evaluation happens against two namespaces only:

```python
# providify/_annotations.py:180-235 (_annotation_namespaces / _eval_annotation)
globalns, localns = _annotation_namespaces(fn, {})
...
value = _eval_annotation(raw, globalns, localns)
```

`globalns` is `fn.__globals__` — the **module** the function was *defined in*, not the module
it happens to be called from. `localns` is seeded only with the names of already-registered
interfaces (`container.py:3453-3483`), never with anything the caller of `provide()` supplies
ad hoc. There is no keyword on either `provide()` or `@Provider` that lets a caller say "resolve
parameter X as type Y regardless of what its annotation says":

```python
# providify/decorator/scope.py:497-511 — @Provider's own signature
class Provider:
    def __init__(self, *, singleton: bool = False, priority: int = 0,
                 qualifier: str | None = None, returns: Any = None) -> None:
        ...
```

`returns=` exists (the return-type override this repo already relies on — see CLAUDE.md's
"quoted `@Provider` return annotations" pitfall) but has no parameter-side twin.

## 2. Why this is a gap

A factory function defined as a **closure**, whose annotated parameter type is a class known
only at the call site (not importable at the defining module's top level), cannot declare an
`Inject[SomeRuntimeClass]` parameter — `SomeRuntimeClass` is not a name in the closure's
`__globals__` (which is the *defining module's* globals, e.g. `varco_ws/di.py`'s own, not the
caller's locals), so evaluation raises `NameError`/`AnnotationResolutionError`.

Concretely, this plan found two call sites:

- `varco_fastapi.router.mcp.MCPAdapter`/`varco_fastapi.router.skill.SkillAdapter`'s four
  runtime-configurable dependencies (`client_cls`, `runner_cls`, `store_cls`,
  `conv_store_cls`, `mcp.py:1010-1022`, `skill.py:1102-1153`) — each is a *class chosen by the
  application at construction time* (not a fixed varco type), so there is no module-level
  name to import and annotate a provider parameter with.
- `varco_ws/di.py`'s `bind_websocket_adapter`/`bind_sse_adapter` factories — **this one turned
  out to be entirely fixable without upstream** (see §5): the DESIGN note at
  `varco_ws/di.py:253-258` claimed `Inject[AbstractEventBus]` could never be declared because
  the type was "closure-local"; in fact `AbstractEventBus` is an ordinary, stable,
  module-importable type. Moving the import to `varco_ws/di.py`'s own module scope (so it is
  present in `fn.__globals__`) resolves it with **no** upstream change (Plan 050 / Step 13).

The residual, genuine gap is the *first* bullet: a type that is a **runtime value** (chosen per
application, per call), which by construction cannot be a module-level import anywhere ahead of
time.

## 3. Minimal reproduction

```python
from providify import DIContainer

container = DIContainer()

class _RuntimeOnlyType:
    pass

def factory(client: object) -> str:
    return "value"

# TypeError: provide() got an unexpected keyword argument 'inject'
container.provide(factory, returns=str, inject={"client": _RuntimeOnlyType})
```

Guarded by `varco_core/tests/test_providify_contracts.py::
TestP050UpstreamAsks::test_provider_accepts_inject_override` (`strict=True`, `raises=TypeError`).

## 4. The ask

Add an `inject={"param_name": Type, ...}` keyword to `provide()` (and/or `@Provider`) that
overrides the annotation-derived injection point for named parameters, the same way `returns=`
overrides the return-type-derived interface:

```python
container.provide(factory, returns=WebhookDispatcher, inject={"client_cls": SomeClient})
```

| Candidate | ✅ | ❌ |
|---|---|---|
| **`provide(fn, *, returns=, inject={...})`** | Symmetric with `returns=`; sidesteps PEP-563 entirely for the overridden parameters — no annotation evaluation needed for those names | One more optional kwarg; providify's per-parameter resolver (`_resolve_params`) needs a lookup-by-name short-circuit before annotation evaluation |
| `@Provider(inject={...})` (decoration-time) | Consistent with a hypothetical `@Provider(returns=...)`-shaped twin | Decoration time is *before* the caller knows the runtime type in the motivating case (`client_cls` is a constructor argument to `bind_mcp_adapter`, known only when that function runs) — does not cover the motivating case at all |

Recommendation: **`provide(fn, *, returns=, inject=...)`** — decoration-time cannot work for
this gap's motivating case by construction (the type is not known until the wrapping function
runs), so only the call-site form is useful here.

## 5. Is this partly ours?

**Yes, partly — and the bigger part.** Of the two call sites this plan audited:

- `varco_ws`'s adapters: **entirely ours to fix**, and fixed in this plan (Step 13) — the
  `:253-258` DESIGN note's claim that the dependency was "closure-local" was simply wrong;
  `AbstractEventBus` is a stable module-level type, and importing it at `varco_ws/di.py`'s own
  module scope (rather than inside the factory's function body) makes `Inject[AbstractEventBus]`
  resolvable with zero upstream involvement. This is now documented directly in that module's
  rewritten DESIGN block.
- `MCPAdapter`/`SkillAdapter`'s four runtime-class dependencies: **not fixable without either
  this ask or a design change**, because the type genuinely does not exist as an importable name
  until the application constructs it. This is the part actually filed here.

## 6. Interim workaround (shim, named for deletion)

**Proposed, not yet landed** (out of scope for the slice of Plan 050 that filed this report —
tracked as follow-up work, not done here): a single helper,
`varco_fastapi/varco_fastapi/router/_di.py::_resolve_optional(container, cls)`, doing the
equivalent of `container.get(cls)` with `None`-on-`LookupError` handling, to replace the four
near-identical inline `container.get(_x_cls)` blocks in `mcp.py:1010-1022`/`skill.py:1102-1153`
with one named, documented call site — so a future `inject=` addition has one place to convert,
not four. Until that lands, those four call sites keep their current pre-Plan-050 shape
unchanged; this report's ask is unaffected either way (the gap is about `provide()`/`@Provider`,
not about how many call sites use the workaround).

## 7. Guard

`varco_core/tests/test_providify_contracts.py::TestP050UpstreamAsks::
test_provider_accepts_inject_override` — `strict=True`, `raises=TypeError`. The moment
providify adds `inject=`, this test starts passing and pytest reports `XPASS`, which
`strict=True` turns into a hard failure.
