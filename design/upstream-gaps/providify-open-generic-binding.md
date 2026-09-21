# Open-generic binding: no wildcard match for a TypeVar-parameterised binding

**ID**: P26-OPEN-GENERIC-BINDING
**Raised by**: varco, Plan 043 / DI-15
**Date**: 2026-09-12
**providify version**: 2.0.1 (`1402f0a`)
**Status**: ✅ Resolved (2026-09-14, Plan 045 / DI-15 close-out) — see §8
**Priority**: 🟡 should — gates `DI-5`'s shape question; three per-type registration loops
today (`bind_repositories`, `bind_repositories` (beanie), `bind_clients`)

---

## 1. What providify does today

**Registration accepts a `TypeVar`-parameterised alias as a binding interface, unconditionally.**
`container.provide(fn, *, returns=)` (`providify/container.py:1048-1077`) hands `returns` to
`_normalize_explicit_interface` (`providify/binding.py:432-491`); the generic-alias branch —
"a parameterised generic alias (`get_origin(v) is not None`), e.g. `Repository[User]` → used
directly" (`binding.py:460-461`) — makes no distinction between a concrete argument
(`Repository[User]`) and a `TypeVar` argument (`Repository[T]`). Both are stored verbatim on
`ProviderBinding.interface`. `returns=Repository[User]` is documented as the supported spelling
for a per-type registration (`container.py:1062-1068`'s own example loops `for model in (User,
Order): ... returns=Repository[model]`) — nothing in that docstring says `model` must be
concrete, and nothing rejects a `TypeVar` in its place.

**Matching, however, treats a binding-side `TypeVar` as a literal, not a wildcard.**
`_interface_matches()` (`providify/utils.py:130-196`) has four structural branches; the
both-generic branch (`:174-180`) is:

```python
if req_origin is not None and bind_origin is not None:
    # Both parameterised generics.
    # Origins must be compatible AND type args must match exactly.
    # DESIGN: exact args match only — no covariance.
    if not issubclass(bind_origin, req_origin):
        return False
    return get_args(binding_interface) == get_args(requested)
```

Its own docstring already names the gap as a documented non-feature (`utils.py:157-158`):

> `- requested is generic with TypeVar args → literal comparison; will not match concrete parameterisations ⚠️`

(That docstring is phrased from the *requested*-side case; the code path a caller actually hits
when *registering* an open generic — a `TypeVar` on the *binding* side, matched against a
*concrete* request — is the identical `get_args(binding_interface) == get_args(requested)` line,
which compares `(T,)` to `(User,)` and returns `False` just the same. Both directions fail for
the same reason: `get_args` equality is `==`, not "is one side a `TypeVar`".)

`_filter()` (`providify/container.py:1691-1733`) calls `_interface_matches` for every binding on
every `get()`/`aget()`; a registration whose args never equal a request's args is filtered out of
every future resolution — `container.get(Repository[User])` raises `LookupError` (observed at
`providify/container.py:2377`, inside `_get_best_candidate()`, once `_filter()` returns an empty
candidate list) even though `container.provide(factory, returns=Repository[T])` succeeded
silently at registration time.

## 2. Why it is a gap

Three independent, structurally identical per-type registration loops exist in varco solely to
work around this — one `container.provide()` call per concrete entity, because a single
`returns=Repository[T]` binding is unreachable:

- `varco_sa/varco_sa/di.py:292-393` — `bind_repositories()` calls `_bind_repo_provider()` once
  per `entity_cls` passed in; each call registers its own closure and its own
  `container.provide(Provider(singleton=False)(_repo_factory), returns=AsyncRepository[entity_cls])`
  (`:389-392`). The function's own DESIGN block records the trade-off explicitly
  (`:307-312`):

  ```
  DESIGN: per-entity @Provider functions over a single generic factory
    ✅ Each binding has a concrete generic alias (AsyncRepository[User])
       that the container can match exactly via _is_generic_subtype()
    ✅ Works with container.aget(AsyncRepository[User]) type resolution
    ❌ N @Provider registrations for N entity classes — scales linearly
       with the model count, but that is typically small (<50)
  ```

- `varco_beanie/varco_beanie/di.py:103-202` — the identical shape, async, one
  `Provider(returns=AsyncRepository[entity_cls])(_repo_factory)` per entity (`:201`).
- `varco_fastapi/varco_fastapi/di.py:108-196` — `bind_clients()`, one
  `container.provide(Provider(singleton=True)(_factory), returns=client_alias)` per router class
  (`:196`), the same per-type loop for `VarcoClient[RouterType]`.

Every one of the three loops exists because `container.provide(factory, returns=AsyncRepository[T])`
(a single, open registration) is not resolvable today — `container.get(AsyncRepository[User])`
would raise `LookupError` for the same reason the guard in §3 does.

## 3. Minimal reproduction

Providify-only — no varco import, pasteable verbatim into providify's own test suite. Identical
to `varco_core/tests/test_providify_upstream_gaps.py::TestP26OpenGenericBinding`:

```python
from typing import Generic, TypeVar

from providify import DIContainer

T = TypeVar("T")


class Repo(Generic[T]):
    def __init__(self, entity: type[object] | None = None) -> None:
        self.entity = entity


class User:
    pass


def test_open_alias_registration_resolves_closed_request() -> None:
    def factory() -> Repo[T]:
        return Repo()

    container = DIContainer()
    container.provide(factory, returns=Repo[T])  # registers without error

    result: Repo[User] = container.get(Repo[User])  # raises LookupError
    assert isinstance(result, Repo)


def test_open_alias_factory_receives_closed_type_argument() -> None:
    def factory(entity: type[T]) -> Repo[T]:
        return Repo(entity=entity)

    container = DIContainer()
    container.provide(factory, returns=Repo[T])

    result: Repo[User] = container.get(Repo[User])  # same LookupError — the
    # factory is never even called, because _filter() runs before instantiation
    assert result.entity is User
```

Observed exception on both: `LookupError`, raised at `providify/container.py:2377` inside
`_get_best_candidate()`, message shape `"No binding found for '<module>.Repo[<module>.User]'.
Did you forget container.bind() or container.provide()?'"`. Registration itself never raises —
confirmed by running the reproduction with the marker removed first (plan §D-failmode
procedure).

A control proves the two failures are specific to the `TypeVar`, not to generic-alias matching
in general: `container.provide(factory, returns=Repo[User])` then `container.get(Repo[User])`
resolves correctly (`test_closed_alias_still_matches_exactly`), because both sides are concrete
and `get_args() == get_args()` is `(User,) == (User,)`.

## 4. The ask

**Model proposed: Autofac's resolve-time factory, not .NET's implementation-type substitution**
(research brief [`016-open-generic-binding-in-di-containers.md`](../research/016-open-generic-binding-in-di-containers.md)):

- Brief 016 §1 (.NET): registers `typeof(IRepository<>)` → `typeof(Repository<>)` and does
  **not** support factory delegates for open generics — the type argument is unknown at
  registration, and .NET's DI container instantiates the closed generic type directly via
  reflection. Python has no equivalent runtime generic-class instantiation with substituted
  parameters (brief 016 §7); varco's factories genuinely need `entity_cls` as a *value*
  (`varco_sa/varco_sa/di.py:376-381` calls `provider.get_repository(entity_cls)`), which rules
  out implementation-type substitution as a model here.
- Brief 016 §2 (Autofac): `RegisterGeneric` delegates are generated **at resolution time**, with
  the closed type argument available to the delegate body — exactly the shape a Python factory
  needs.
- Brief 016 §3/§4/§6: Spring's factory-bean-per-interface (what `bind_repositories` already does
  today), Guice's deliberate absence of open-generic support, and every surveyed Python DI
  library (dependency-injector, Dishka, lagom, rodi, punq, injector) — all absent. **providify
  would be the first Python DI container to support this**, adapting Autofac's model rather than
  copying an existing Python precedent, because none exists.

**Proposed spelling** (primary candidate):

```python
T = TypeVar("T")

def repo_factory(provider: RepositoryProvider, entity: type[T]) -> AsyncRepository[T]:
    return provider.get_repository(entity)

container.provide(repo_factory, returns=AsyncRepository[T])   # open registration
container.get(AsyncRepository[User])                          # closed resolution
```

`entity: type[T]` is filled from the requested alias's `get_args()`, matched by position of `T`
in the open alias's own `get_args()`. Alternative spelling to offer if positional `TypeVar`
matching is judged too implicit: a reserved marker type (`TypeArg[T]`/`Inject[TypeArgs]`)
requiring an explicit parameter annotation.

**The semantics table this ask asks the maintainer to settle:**

| # | Semantic | Proposed | Grounding |
|---|---|---|---|
| 1 | **Key/match rule** — a `TypeVar` in a *binding's* alias args matches any concrete type at that position; a *requested* alias with a `TypeVar` keeps today's literal rule (unaffected, out of scope) | ✅ wildcard on the binding side only; ❌ risk: a truly bare `Repo` request must not accidentally match an open `Repo[T]` binding via this new rule — needs `req_origin is not None` guarded explicitly | `providify/utils.py:157-158, 174-180`; brief 016 §7 (`get_origin`/`get_args`; `isinstance` on a parameterised generic is forbidden by Python itself) |
| 2 | **Specificity** — a closed binding (`AsyncRepository[User]`) beats an open one (`AsyncRepository[T]`) regardless of registration order or priority | ✅ closed > open, then existing priority rules; ❌ needs a new tie-break step before/alongside the existing priority sort | brief 016 §Options table, "Specificity" column — no surveyed framework documents this; varco needs it so a per-entity `bind_repositories(User)` override can coexist with a generic default |
| 3 | **Type-arg delivery** — how the factory receives the closed type | ✅ positional by `TypeVar` name, a `type[T]` parameter; ❌ implicit — a factory that forgets the parameter silently gets no type info rather than an error | brief 016 §2 (Autofac's resolve-time delegate) |
| 4 | **Singleton caching** — `singleton=True` on an open binding caches per closed alias, never one instance shared across every `X` | ✅ cache key `(binding, closed_alias)`; ❌ a naive "cache by binding" implementation is the dangerous default to avoid | brief 016 §1 (".NET shares one instance across ALL closed types — a dangerous anti-pattern" if done naively); brief 016 Evidence Gap 5 (undocumented across surveyed frameworks — providify should document its choice explicitly rather than leave it implicit) |
| 5 | **Enumeration** — `InjectInstances[AsyncRepository[User]]` / `get_all(AsyncRepository)` (`providify/decorator/multibinding.py:20`) | ✅ an open binding contributes to a **closed** request's enumeration once; ❌ a **bare** `AsyncRepository` request must not expand the open binding into every conceivable `X` (unbounded — there is no registry of "every X that has been requested") | research 014 §Librarian #2 (immutable collections — the same "no surprise expansion" property) |
| 6 | **`validate()`** — an open binding's `type[T]` parameter must not be flagged as a missing dependency; a closed request's resolvability should still be checked against open bindings | ✅ new resolvability branch, no new `IssueKind` unless the `bound=` check (row 7) fails; ❌ requires `validate()`'s pass 1 (`providify/container.py:5327-5333`, unfiltered by `@Profile`) to also treat `type[T]` specially | `providify/container.py:5249`, `validate_bindings` `providify/container.py:5119`, `validate_all` `providify/container.py:5171` |
| 7 | **`TypeVar(bound=)`/constraints** — a request whose `X` violates `bound=` is a non-match (a second binding may still serve it), not a resolve-time error | ✅ non-match, optionally an informational `IssueKind` in `validate()`; ❌ under-specifying this invites exactly Autofac's own bug class | brief 016 §1 (.NET 5 validates constrained open generics at resolve time); brief 016 §2 (Autofac #972 — a nested-constraint bug, cited as a cautionary tale for the implementation, not a reason to skip constraints) |
| 8 | **Multiple `TypeVar`s / partial closure** (e.g. `Mapper[S, T]` with only `S` closed) | ✅ out of scope for a first cut — specify that only all-`TypeVar` or all-concrete aliases are accepted at registration, `TypeError` otherwise; ❌ none — this is a deliberate scope cut, not a trade-off | — |

## 5. Partly ours? No.

The per-type loop is the Spring factory-bean pattern (brief 016 §3; research 014 §Librarian #5)
and it works correctly today — it is not a bug, only a scaling cost the DESIGN block at
`varco_sa/varco_sa/di.py:307-312` already names and accepts (`<50` models). What varco can do on
its own side is fold the three structurally-identical loops (`varco_sa`, `varco_beanie`,
`varco_fastapi`) into one shared helper — that is `DI-2`'s territory, an abstraction over the
existing per-type pattern, not a fix for the underlying absence of open-generic binding, and it
is explicitly not what this report or `DI-5` asks for.

## 6. Interim workaround — none applied

The three loops stay exactly as they are; there is no supported way to register a single open
binding today. No varco shim, helper, or translation layer is added for this (per `BACKLOG.md:55`
and this repo's standing rule).

## 7. Guard

`varco_core/tests/test_providify_upstream_gaps.py::TestP26OpenGenericBinding` — two
`xfail(strict=True, raises=LookupError)` tests
(`test_open_alias_registration_resolves_closed_request`,
`test_open_alias_factory_receives_closed_type_argument`) plus one always-passing control
(`test_closed_alias_still_matches_exactly`). No Docker, no backend — pure providify, runs in
every `make test PKG=varco_core`.

## 8. Resolution (2026-09-14)

providify 2.1.0 ships open-generic provider bindings (`providify/CHANGELOG.md:69-98`), adopting
the Autofac resolve-time-factory model this report's §4 recommended, with all eight semantics from
the table settled:

1. binding-side `TypeVar` is a wildcard; request-side `TypeVar` and a bare request are not.
2. a closed binding always beats an open one, regardless of priority/order.
3. `type[T]` delivery by TypeVar name — with one addition beyond the ask: a factory with exactly
   one un-delivered closing value and a fully unannotated, no-default parameter gets it
   positionally (`lambda entity: Repo(entity)`), the "positional by TypeVar" idiom this report's
   §4 offered only as an *alternative* spelling to the primary — providify shipped both, choosing
   positional-when-unambiguous over requiring the explicit annotation in that one case.
4. `singleton=True` caches one instance **per closed alias** — the "dangerous anti-pattern" this
   report warned against (one shared instance across every closed type) was avoided.
5. `get_all`/`InjectInstances` include the open binding once per closed request, never expand a
   bare request.
6. `validate()` skips the `type[T]` parameter as a graph edge and checks closed requests against
   open bindings.
7. a `bound=`/constraint violation is a non-match, not a resolve-time error.
8. a mixed alias (`Repo[list[T]]`, `Pair[str, T]`) raises `TypeError` at registration — row 8's
   "out of scope, `TypeError` otherwise" ask, adopted verbatim.

**Two behaviour changes** ship alongside, both narrow and both on registrations that were never
resolvable before (`CHANGELOG.md:119-129`): a bare `get(Repo)` against a now-open `returns=Repo[T]`
binding raises `LookupError` (previously an uninformed factory call with no type information at
all — strictly safer, not a regression for any working call site), and a partially-open alias
raises `TypeError` at registration instead of silently registering an unreachable binding.

**varco does not adopt open-generic bindings in this plan** — the feature is *verified* here (Step
5 converts the two `xfail` reproductions in §3 into passing pins) but *used* by **Plan 046**
(verb collapse) and **Plan 051** (multi-binding registries), per Plan 045's own Non-goals. The
three per-type registration loops (`varco_sa.di.bind_repositories`,
`varco_beanie.di.bind_repositories`, `varco_fastapi.di.bind_clients`) are unchanged by this plan;
folding them into a single `returns=AsyncRepository[T]` open binding is `DI-2`'s and `DI-5`'s
territory (§5 above), now unblocked.

Guard: converted in Plan 045 Step 5 — both `xfail(strict=True, raises=LookupError)` tests now
assert the resolution succeeds and (for the second) that the factory receives the closed type
argument; the `# type: ignore[valid-type]` annotations on `returns=Repo[T]` were re-checked against
mypy and dropped where no longer needed. The always-passing control
(`test_closed_alias_still_matches_exactly`) is kept, still true.
