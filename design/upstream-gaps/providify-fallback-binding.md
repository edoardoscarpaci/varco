# Fallback binding: no `@DefaultBean`-style "yield if a real binding exists" primitive

**ID**: P27-FALLBACK-BINDING
**Raised by**: varco, Plan 044 / DI-15 (third ask)
**Date**: 2026-09-12
**providify version**: 2.0.1 (`1402f0a`)
**Status**: ✅ Resolved (2026-09-14, Plan 045 / DI-15 close-out) — see §9
**Priority**: 🔴 must — gates `DI-3`'s default-binding contract; `DI-7`'s "default shadowed by X"
report

---

## 1. What providify does today

**Activation is a single-binding predicate — it never sees the rest of the candidate set.**
`_binding_is_active(b)` (`providify/container.py:1606-1676`) evaluates `profile_ok(b) AND
alternative_ok(b)` from `b` alone: `profile_ok` is assigned at `:1663`
(`not b.profiles or _profile_matches(b.profiles, self._active_profiles)`); `alternative_ok`
spans `:1669-1676` (reads the marker off `b.implementation` for a `ClassBinding` or `b.fn` for a
`ProviderBinding`, exactly the lookup a fallback marker would need). A "no other active binding
of `T`" predicate cannot live inside this signature unchanged — it needs the sibling list, which
`_binding_is_active` structurally does not have.

**`_filter()` is the one place the full candidate list exists.** `providify/container.py:1678-1733`;
its own docstring truth table (`:1691-1698`) enumerates six activation combinations. The
comprehension (`:1714-1733`) applies, in order: interface match via `_interface_matches`
(`:1720`, so generic aliases like `Repository[User]` are matched structurally, not by
`issubclass`); the `exact_only` self-binding guard (`:1728`); the qualifier rule (`:1729`:
`qualifier is None or b.qualifier == qualifier` — **an unqualified request matches ANY
qualifier**, confirmed by the identical clause recurring at `:2350` and `:6557`); the priority
rule (`:1730`); and `self._binding_is_active(b)` (`:1732`). `get()`/`aget()`/`get_all()`
(`:1379`)/`aget_all()` (`:1570-1602`, which additionally sorts the result by ascending priority
at its final comprehension)/`is_resolvable()` (`:1735-1780`) all obtain their candidates from
this one function.

**Multibinding (`list[T]` collection points) is also fully `_filter()`-routed, in two hops.**
`_multibound_inner(hint)` (`:1438-1459`) is a pure type-shape gate — it detects a `list[T]`
generic alias and returns `T`, but does **not** itself call `_filter()`. The actual candidate
collection happens one level down, in `_collect_sync`/`_collect_async` (`:1462-1523`), both of
which call `self._filter(inner, qualifier=qualifier)` directly. So a post-filter step placed
inside `_filter()` itself (§4) reaches `get()`, `get_all()`, `aget_all()`, `is_resolvable()`,
**and** every `list[T]` collection point in one place — no second change site, confirmed by
reading `:1438-1523` (this corrects an initial worry in the plan that `_multibound_inner` might
bypass `_filter()`; it does not, because the actual collection is one call away, in
`_collect_sync`/`_collect_async`).

**Highest priority wins; ties break by registration order, silently.** `_get_best_candidate()`
(`:2354-2382`) raises `LookupError` (`:2377`) when `_filter()` returns nothing, otherwise returns
`max(candidates, key=lambda b: b.priority)` (`:2382`). Python's `max()` returns the *first*
maximal element on a tie — providify's own `IssueKind.AMBIGUOUS_BINDING` docstring comment says
so verbatim (`providify/validation.py:76`, directly above the member at `:77`):

> `#: >=2 candidates tied at the max priority for a single-valued point — `max()` picks the first
> in registration order, silently and arbitrarily.`

**providify's own model is mutable-after-registration and lazily consulted — it already
promises this.** `is_resolvable()`'s Edge cases block states, verbatim (`:1770-1771`):

> "Results are not cached — re-evaluated on every call, so a new binding added between two calls
> will be reflected immediately. ✅"

`enable_alternative()` (`:1784`) and `activate_profile()` (`:1836`) both mutate activation state
*after* registration and both reset `self._validated = False`, precisely so the next `_filter()`
call honours the mutation. `activate_profile()`'s Edge cases block documents one caveat this ask
inherits verbatim (`:1865-1868`): a `@Profile`d singleton already resolved and cached under the
old profile set is **not** evicted — "profiles are a startup-time concern;
`override()`/`reset_binding()` remain the eviction tools."

**`validate()`'s pass 1 is unfiltered.** `providify/container.py:5249`; the Edge cases block
(read at the region documented near `:5327-5333`) states `UNREACHABLE_PRE_DESTROY` "runs on
registered bindings regardless of `@Profile` activation — pass 1 iterates `self._bindings`
unfiltered." `AMBIGUOUS_BINDING` itself, however, is emitted per-injection-point from a
`candidates` list computed for that specific request (`container.py:5648-5662`), at
**`severity=Severity.ERROR`** (`:5657`, confirmed by direct read) — today, two equal-priority
candidates at a single-valued injection point already make `validate(raise_on_error=True)`
raise. This is load-bearing for §4/§5 below.

**`Severity` has only two tiers.** `providify/validation.py:41-55`: `ERROR = "error"`,
`WARNING = "warning"`. No `INFO` tier exists to report a shadowed fallback as "expected
behaviour" rather than "legal but notable" (`WARNING`'s own docstring definition, `:46-48`).

**`IssueKind` (`validation.py:58-96`) has no fallback-related member.** `AMBIGUOUS_BINDING`
(`:77`) already exists and already covers "≥2 candidates tied at the max priority" — no
*second* kind is needed for that case (§4).

**`@Profile` is the marker-decorator precedent this ask copies.** `decorator/scope.py:694-798`;
usable on **both** a class and a `@Provider` function/`@Configuration` method (docstring note,
`:726-729`) with zero change to `Singleton()`/`Provider()` signatures — because
`_binding_is_active` already reads the marker off `b.implementation`/`b.fn` (`:1669`), whichever
binding shape it is. `@Alternative` (`:669-686`) is the inverse direction: opt-in-by-default-off,
activated imperatively. `@Default` (`:646-654`) is a **qualifier** — "Explicit default qualifier
— semantically equivalent to `qualifier=None`. Equivalent to Jakarta CDI's `@Default`." — and is
**not** this feature (`BACKLOG.md:263-267` records that this exact name already misled one
reader once).

**`bind()` honours `@Singleton(priority=...)` metadata**, confirmed for the guard's spelling:
`container.py:991-1021` `bind(interface, implementation)` appends
`ClassBinding(interface, implementation)`; `ClassBinding.__init__`
(`providify/binding.py:112-152`) docstring states it "Reads DI metadata from the
`@Component`/`@Singleton` decorator on *implementation*" — so `@Singleton(priority=N) class A`
followed by `container.bind(Base, A)` produces a binding at priority `N`, exactly as the guard
tests assume.

**Absence, confirmed by source grep, not by documentation** (this repo's U-8 lesson):

```bash
rg -in 'fallback|DefaultBean|missing_beans|missingBeans|TryAdd|ConditionalOnMissing' \
   /home/edoardo/projects/varco/.venv/lib/python3.12/site-packages/providify/
```

**Zero hits — not even prose.** Every one of the six surveyed frameworks' names for this
pattern is entirely absent from the installed 2.0.1 tree; there is no partial implementation, no
docstring mention, no deprecated attempt.

## 2. Why it is a gap

**Every framework default in varco is a `priority=-sys.maxsize - 1` heuristic** whose only
contract is "anything else outranks me." Re-counting `rg -n 'priority=-sys\.maxsize'
varco_*/varco_*` for this report: **72 raw hits**, classified into four buckets (the raw count
mixes shapes the report must keep apart):

1. **Unqualified `-sys.maxsize - 1` sites — the true fallbacks.** In `varco_core`:
   `flags/null.py:27` (`NullFeatureFlags`), `revocation/null.py:35`
   (`NullTokenRevocationStore`), `tenancy/membership.py:237`, `event/producer.py:146`,
   `event/serializer.py:116`. In `varco_fastapi/varco_fastapi/di.py`: 13
   `@Provider(..., priority=-sys.maxsize - 1)` methods (`:280,290,301,311,334,350,371,384,395,
   400,410,418,432`) plus 3 imperative `Singleton(priority=-sys.maxsize - 1)(X)` calls
   (`:104,105,615`) — 16 sites in one file. Backends: e.g. `varco_redis/varco_redis/
   revocation.py:256`, `varco_redis/varco_redis/cache.py:567,787`,
   `varco_kafka/varco_kafka/dlq.py:772`, `varco_sa/varco_sa/di.py:176`,
   `varco_memcached/varco_memcached/cache.py:636,647`.
2. **Qualified `-sys.maxsize - 1` sites** — `event/memory.py:87` (`InMemoryEventBus`,
   `qualifier="in_memory"`), `cache/memory.py:145` (`InMemoryCache`, same qualifier),
   `event/dlq.py:524` (`InMemoryDeadLetterQueue`, same qualifier), plus `lock.py:373` and
   `event/deduplication.py:157`. These are *both* a named escape hatch
   (`get(T, qualifier="in_memory")`) *and* the unqualified default — which works only because
   `_filter()` (`:1729`) lets an unqualified request see a qualified binding.
3. **`-sys.maxsize` (one above the floor) sites** — a hand-built **second tier**: backend
   singletons registered so they beat the in-memory core default but lose to anything the app
   registers, e.g. `varco_redis/varco_redis/bus.py:102` (`qualifier="redis"`),
   `varco_kafka/varco_kafka/bus.py:95` (`qualifier="kafka"`),
   `varco_ws/varco_ws/sse.py:174`, `varco_ws/varco_ws/websocket.py:352`, plus the `health.py`
   probes in `varco_kafka`/`varco_redis`/`varco_beanie`. Two magic numbers encoding "fallback
   beneath a fallback" is the clearest evidence the heuristic is already being stretched past
   what a bare number can express.
4. **Prose mentions** — docstrings/comments explaining the pattern rather than applying it, e.g.
   `varco_kafka/varco_kafka/config.py:253`, `varco_kafka/varco_kafka/channel.py:147`,
   `varco_redis/varco_redis/config.py:165`.

**Distinguishing this ask from P25 (`@Requires(condition=...)`)**: P25 is a predicate over
**application configuration** (a settings object, an env var, whether a package is importable) —
its answer does not change unless the operator changes a setting. This ask is a predicate over
**the binding set itself** — its answer changes the moment a *sibling binding* is registered or
activated, with no configuration involved at all. Brief 010 §1's Quarkus-vs-Spring framing
applies here, not there: `@DefaultBean` "silently yield[s] to user beans *without*
registration-order coupling," where P25's `@Requires` is entirely about the app's own settings,
never about what else is bound.

**What the heuristic cannot do, and a fallback binding can:**

- **State intent.** `priority=-sys.maxsize - 1` is indistinguishable from "a very low-priority
  real binding" — `describe()`/`validate()` cannot tell an operator "this is a default and it
  was shadowed by X," which is exactly the report `DI-7`'s `varco diagnose` needs
  (`BACKLOG.md:78`).
- **Survive an equal-priority collision.** A user who copies a varco default's decorator line
  verbatim (including its priority) registers at the floor too and loses by registration order
  (`:2382`) — silently. Brief 010 §1: Quarkus's model is "explicit intent over implicit
  ordering"; Spring's registration-order coupling is what `DI-3` already names as the
  undocumented "provide before scan to win" folk rule (`BACKLOG.md:76`).
- **Stay out of collections.** `get_all(T)`/`InjectInstances[T]` includes the default beside the
  real implementation today (`:1599-1602`'s sort has no exclusion step) — an app fanning out to
  "every event bus" also gets the in-memory one.

## 3. Minimal reproduction

The two `xfail` bodies below are verbatim from
`varco_core/tests/test_providify_upstream_gaps.py::TestP27FallbackBinding` — providify-only,
pasteable into providify's own test suite unchanged.

```python
from providify import DIContainer, IssueKind, Profile, Singleton


class Base:
    pass


def test_fallback_decorator_is_exported() -> None:
    from providify import Fallback  # raises ImportError today

    @Fallback
    @Singleton
    class A(Base):
        pass

    @Singleton
    class B(Base):
        pass

    container = DIContainer()
    container.bind(Base, A)
    container.bind(Base, B)

    assert isinstance(container.get(Base), B)
    assert [type(x) for x in container.get_all(Base)] == [B]

    lone_container = DIContainer()
    lone_container.bind(Base, A)

    assert isinstance(lone_container.get(Base), A)


def test_validate_reports_shadowed_fallback() -> None:
    shadowed_kind = IssueKind.FALLBACK_SHADOWED  # raises AttributeError today

    from providify import Fallback

    @Fallback
    @Singleton
    class A(Base):
        pass

    @Singleton
    class B(Base):
        pass

    container = DIContainer()
    container.bind(Base, A)
    container.bind(Base, B)

    report = container.validate(raise_on_error=False)
    shadow_issues = [issue for issue in report.issues if issue.kind == shadowed_kind]

    assert len(shadow_issues) == 1
    assert "B" in str(shadow_issues[0])
```

**Observed today** (marker verified without it present first, in both directions):

```
>>> from providify import Fallback
ImportError: cannot import name 'Fallback' from 'providify'
(/home/edoardo/projects/varco/.venv/lib/python3.12/site-packages/providify/__init__.py)

>>> from providify import IssueKind
>>> IssueKind.FALLBACK_SHADOWED
AttributeError: type object 'IssueKind' has no attribute 'FALLBACK_SHADOWED'
```

The three passing controls in the same class pin exactly the behaviour a fallback must change,
none of it:

```python
def test_equal_priority_tie_breaks_to_first_registered_today() -> None:
    """bind(Base, A) then bind(Base, B), both priority 0 -> get(Base) is A."""
    ...

def test_higher_priority_already_wins_regardless_of_order_today() -> None:
    """A at priority 0 first, B at priority=1 second -> get(Base) is B."""
    ...

def test_inactive_sibling_is_not_a_candidate_today() -> None:
    """A plain, B @Profile("x")-gated; DIContainer(profiles=()) -> get_all(Base) is [A].
    After activate_profile("x") -> {A, B}."""
    ...
```

## 4. The ask

### 4a. Name — `@Fallback`, a `@Profile`-shaped marker decorator

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) `@Fallback` marker decorator** (copy `@Profile`'s shape, `scope.py:694-798`) | Works on **both** binding shapes varco has — scanned `@Singleton` classes and `@Provider` functions/`@Configuration` methods — with **zero** change to `Singleton()`/`Provider()` signatures, because `_binding_is_active` already reads markers off `b.implementation`/`b.fn` (`:1669`). Symmetric with `@Alternative` (the inverse direction). One greppable token. Composes with `@Profile`/`@Alternative` by the same AND | A third marker on the source object; inherits `@Profile`'s "`__dict__`-only, subclass does not inherit" rule (`:754-755` region) — must be documented the same way |
| **(2) `Singleton(fallback=True)` / `Provider(fallback=True)` kwarg** | Reads as a binding attribute, next to `priority=`; one line per site | Two (really four — `Singleton`/`Component`/`Provider`/`Configuration`-method) signature changes plus the imperative `Singleton(...)(cls)` call form; `Alternative`/`Profile` are *not* kwargs, so this breaks providify's own convention that activation markers are decorators. Offer as **sugar over (1)** only |
| **(3) `@DefaultBean`** (the Quarkus name) | Operators from the JVM recognise it | providify already exports `Default` — a **qualifier** (`scope.py:646-654`, CDI `@Default`), and `BACKLOG.md:263-267` records that this very name collision already misled one reader. `DefaultBean` beside `Default` in the same namespace invites the confusion permanently. Rejected |
| **(4) `@Requires(missing_beans=T)`** (the Micronaut name) | Names the predicate's target | Couples this ask to the unshipped P25 `@Requires` decorator; forces the caller to **repeat the interface** (`missing_beans=AbstractEventBus`) that the binding already declares via `returns=`/base class, a redundancy that rots; Micronaut's own `missingBeans` is qualifier-blind (brief 017 §4c) and compile-time (brief 017 §1) — the wrong model to borrow a name from. Rejected |

**Recommendation**: (1) as the primitive, (2) as optional sugar over it. Any name is acceptable
if the §5 semantics below match — the guard is name-based (§8) and this report is the contract,
the same P25 precedent (Plan 043 §D-failmode).

### 4b. Evaluation timing — lazy, at resolve time, the same point `@Profile` is checked

Brief 017 §2's comparison table: **no surveyed framework re-evaluates after a later
registration.** Spring freezes at configuration-parse time; Quarkus and Micronaut freeze at
build time; ASP.NET's `TryAdd*` freezes at the add-call. providify cannot copy any of them,
because its binding set and activation state are **mutable after registration by design**:

- `bind()`/`register()`/`provide()` (`:991`/`:1026`/`:1048`) and `scan()` may be called at any
  time, in any order — `DI-3`'s whole complaint (`BACKLOG.md:76`) is that the *current* floor
  heuristic makes that order matter.
- `enable_alternative()` and `activate_profile()` mutate activation **after** registration, and
  `_binding_is_active` is evaluated on every `_filter()` call precisely so those mutations are
  honoured.
- `is_resolvable()`'s own docstring guarantees "a new binding added between two calls will be
  reflected immediately" (`:1770-1771`, quoted in full above).

A registration-time check (ASP.NET `TryAdd`, brief 017 §2 row 3) would evaluate "no other binding
of `T`" the moment the fallback is registered — so `container.scan("varco_core")` **before**
`container.install(AppModule)` would keep the fallback and *then* have the app's real binding
arrive, producing two bindings and reintroducing the floor-priority race this ask exists to
remove. Brief 017's Librarian's Note states this plainly: *"the evidence favors resolution-time
evaluation."*

**Cost**: `O(len(candidates))` extra work per lookup (§4c) — not `O(len(self._bindings))`, since
`_filter()` already walks every binding once regardless. No per-interface cache is asked for; the
report notes it as a future option only if providify ever memoizes `_filter()` itself.

**Caveat, inherited verbatim from `@Profile`**: a fallback singleton already instantiated and
cached before the shadowing binding arrives is **not evicted** — the same documented behaviour
as `activate_profile()`'s `:1865-1868` caveat. varco's own bootstrap order (scan defaults →
install app → first `get()`) never hits this in practice, but the report states it because
`@Profile` already documents the identical trap.

### 4c. Placement — a post-filter step inside `_filter()`

**Recommended**: append one step to the existing comprehension (`:1714-1733`):

```python
candidates = [ …today's comprehension… ]
if any(not is_fallback(b) for b in candidates):
    candidates = [b for b in candidates if not is_fallback(b)]
return candidates
```

- ✅ Zero new container state; no signature change to `_binding_is_active`;
  `O(len(candidates))` extra cost, already argued in §4b.
- ❌ The rule is request-relative rather than a property of the binding in isolation — "is this
  fallback shadowed?" has no answer without a request. For `validate()` (§4d), the natural
  request to evaluate is `(b.interface, b.qualifier, None)` — "would the most natural request
  for this binding be shadowed?"

**Alternative — extend `_binding_is_active(b)` to `_binding_is_active(b, siblings)`**:
✅ keeps the "single activation predicate" docstring promise (`:1609-1612`); ❌ needs the sibling
list computed *before* activation filtering, so inactive siblings would have to be re-filtered
inside the predicate — duplicating `_filter()`'s own work and turning the inactive-shadower rule
(§5 row 1) into a special case instead of something free. Rejected; mentioned as the
less-preferred placement.

### 4d. Diagnostic — a new `Severity.INFO` + `IssueKind.FALLBACK_SHADOWED`; `WARNING` accepted as fallback

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) New `Severity.INFO` + `IssueKind.FALLBACK_SHADOWED`** | Precise: a shadowed default is **expected behaviour**, not "legal-but-notable" (`WARNING`'s own definition, `validation.py:46-48`). `DI-7`'s wiring report can render it as "default / overridden by X," the Spring `/actuator/conditions` `negativeMatches` shape (brief 017 §3) | A new `Severity` member every consumer's `match` must handle; providify has lived with two tiers so far |
| **(2) `FALLBACK_SHADOWED` at `WARNING`** | No `Severity` change | A varco app with ~20 shadowed defaults emits ~20 warnings on every `validate()` — a CI gate on zero warnings would reject correct wiring. Acceptable **only** if (1) is declined |
| **(3) No issue kind; `describe()` only** | Smallest change | Brief 017 §3: Micronaut (debug log only) and ASP.NET (nothing) are the *cautionary* examples here, not the model — the whole point of this ask is that a shadowed default must be visible to the operator |
| **(4) A second kind for "two fallbacks tie"** | — | **Not asked** — `AMBIGUOUS_BINDING` (`validation.py:77-79`) already is that kind (§5 row 4). A duplicate kind for the identical shape would be a U-8-class error |

**Recommendation**: (1) as the primitive, evaluated for the request `(b.interface, b.qualifier,
None)`, with a `shadowed_by` detail naming the winning binding (owner + interface + qualifier +
priority); (2) accepted only if (1) is declined; (3) rejected; (4) explicitly **not** asked.

## 5. Specified edge semantics

**Chosen rule**: a fallback is shadowed, for a given request `(T, qualifier, priority)`, iff at
least one active non-fallback binding is a candidate for that same request. This one rule
settles every edge below **by construction**, no special case required.

| Edge | Behaviour under the rule | Grounding |
|---|---|---|
| **Inactive shadower** — a `@Profile`-off or un-enabled `@Alternative` binding of `T` | Does **not** shadow: it never enters `candidates` because `_binding_is_active` (`:1732`) already dropped it. When `activate_profile()`/`enable_alternative()` later activates it, the **next** `get()` sees it (§4b) | Brief 017 §4b: the Quarkus/Micronaut model ("count as present only if active"), explicitly **not** Spring's (`spring-projects/spring-boot#2897`, where a profile-deactivated definition still shadows). Librarian's Note edge 2. Pinned by the passing control `test_inactive_sibling_is_not_a_candidate_today` |
| **Qualifiers** — `@Singleton(qualifier="x")` non-fallback `X`, unqualified `@Fallback` `F` | `get(T)` → both are candidates (`:1729`, unqualified matches any) → `F` shadowed → `X` wins. `get(T, qualifier="in_memory")` → only a binding with that qualifier is a candidate → an `"in_memory"`-qualified fallback is **not** shadowed → returned. `get(T, qualifier="y")` → neither matches → `LookupError`, unchanged | Brief 017 §4c: Spring cannot be qualifier-aware at all (`#2834`); Quarkus keys on type+qualifier identity. providify's rule is **request-relative**, strictly more useful for varco than CDI identity: the `qualifier="in_memory"` escape hatch at the five qualified sites (§2 bucket 2) keeps working *and* the unqualified default is still shadowed. This is a deliberate deviation from CDI, grounded in `_filter()`'s own `:1729` |
| **Generic aliases** — fallback for `Repo[User]`, non-fallback for `Repo[Post]` | Not the same candidate set (`_interface_matches`, `:1720`) → no shadowing | Brief 017 §4c's varco implication; consistent with P26's generic-alias matching |
| **Two fallbacks, no non-fallback** | `candidates` is all-fallback → unchanged → today's `max()` rule applies: higher `priority` wins, exact ties → first registered (`:2382`), and **`IssueKind.AMBIGUOUS_BINDING` already reports the tie** (`validation.py:77-79`) — at `Severity.ERROR` today (`container.py:5657`). **No second kind is asked for.** The ask is only that the ambiguity detector run on the *post-shadowing* candidate set, so `{F1, F2, X}` reports nothing (X wins cleanly, no false ambiguity against the shadowed fallbacks) while `{F1, F2}` alone at equal priority still does | Brief 017 §4a: Quarkus `quarkusio/quarkus#37042` (`@Priority`, then a hard build-time ambiguity error); ASP.NET silently takes the first; Spring the last. providify already has the Quarkus shape minus the hard build-time error — keep it, because a hard error at `get()` time would make every varco two-tier site (§2 bucket 3) fail if both tiers were ever marked `@Fallback` |
| **Circularity** (Micronaut `micronaut-projects/micronaut-core#9315`) | **Impossible.** `is_fallback(b)` is a marker read, not a resolution; the rule consults only *non-fallback* siblings; a fallback never shadows a fallback; no binding's activity depends on another's activity | Brief 017 §1, Evidence Gap 3 — Micronaut ships no mitigation for its compile-time cycle; providify's runtime rule needs none by construction |
| **`exact_only` self-bindings** (`:1721-1728`) | Unchanged — they are never fallbacks and only match their own type | — |
| **`priority=` narrowing** (`get(T, priority=p)`) | The rule runs after priority narrowing, so `get(T, priority=FLOOR)` returns the fallback even when shadowed — an explicit "give me the default" request | Mirrors the qualifier row; named so the maintainer decides whether that is desired |
| **Fallback in a collection** — `get_all(T)`/`aget_all(T)`/`list[T]` collection points | A shadowed fallback is excluded from **every** consumer at once, because all four (`get_all`, `aget_all`, `is_resolvable`, `_collect_sync`/`_collect_async`) obtain their candidates from `_filter()` (confirmed §1's multibinding paragraph) | Quarkus's `@All`/`List<T>` behaviour (brief 017 §2 row 2; Librarian's Note edge 4), for free — specified here as a **required property**, not an implementation accident |

**No Python DI library ships this today.** Brief 017 §5 surveyed `dependency-injector`,
`dishka`, `lagom`, `punq`, `injector`, `wireup`, `rodi`, `that-depends`: "No Python DI library
has a mature fallback/default-bean primitive as of September 2026." providify would be first in
the ecosystem to ship it.

## 6. Partly ours? No.

**No.** P25 had a "partly ours" answer — a supported `activate_profile()` translation that fixes
one site's symptom today. This gap does not. The floor priority is a *heuristic over a number*,
not a translation of the feature through any supported providify API: there is no public call
that says "hide this binding if another exists," and emulating one would mean varco inspecting
`container._bindings` (private) or wrapping `get()` — both are shims `BACKLOG.md:55` forbids.
`DI-3`'s interim (`BACKLOG.md:76`) is to **name** the floor once
(`FRAMEWORK_DEFAULT_PRIORITY`) and document the tie rule — that is honesty about the heuristic,
not a workaround of the gap, and it lives in `DI-3`'s own plan.

## 7. Interim workaround — none applied

No varco shim, helper, or translation layer is added by this plan. `DI-3` will name the floor
`FRAMEWORK_DEFAULT_PRIORITY` and document "ties resolve by registration order"
(`BACKLOG.md:76`) — a statement of the heuristic varco already lives with, not a workaround of
this gap. That work happens in `DI-3`'s own plan, not here.

## 8. Guard

`varco_core/tests/test_providify_upstream_gaps.py::TestP27FallbackBinding` —
`test_fallback_decorator_is_exported` (`xfail(strict=True, raises=ImportError)`) and
`test_validate_reports_shadowed_fallback` (`xfail(strict=True, raises=AttributeError)`), each an
**independent flip condition**: the maintainer may ship `@Fallback` without the diagnostic (the
second guard stays correctly xfail) or the diagnostic without the decorator (the second guard
then fails with `ImportError` instead of `AttributeError` — loud, a signal to revisit). Three
always-passing controls in the same class pin exactly the behaviour a fallback must change:
`test_equal_priority_tie_breaks_to_first_registered_today` (`container.py:2382`,
`validation.py:77-79`), `test_higher_priority_already_wins_regardless_of_order_today` (proves the
floor heuristic *works* when a real priority gap exists — a higher-priority `B` would be no
guard at all), and `test_inactive_sibling_is_not_a_candidate_today` (`container.py:1732`,
grounding §5 row 1).

No Docker, no backend — pure providify, runs in every `make test PKG=varco_core` and CI's `unit`
job. ⚠️ **Name-based, like P25**: if providify ships an equivalent feature under a different
name, both guards stay silently xfail. Mitigation: this report names the full contract (§4, §5),
and §1's absence `rg` should be re-run at every providify version bump (the same discipline as
Plan 016's upgrade checklist).

## 9. Resolution (2026-09-14)

providify 2.1.0 ships `@Fallback`/`FallbackMarker` exactly under the recommended name (§4a
candidate 1), evaluated lazily at resolve time inside `_filter()` (§4b/§4c, `container.py`'s
fallback-drop step: `if any(not b.fallback for b in candidates): candidates = [b for b in
candidates if not b.fallback]`, confirmed running after interface/`exact_only`/qualifier/priority/
activation narrowing exactly as §4c's placement asked) and `IssueKind.FALLBACK_SHADOWED` at the new
`Severity.INFO` tier (§4d candidate 1, the recommended one — not the `WARNING`-fallback candidate
2). Every edge row in §5's table is confirmed against the shipped source:

- Inactive shadower does not shadow (`container.py`'s activation narrowing runs before the
  fallback-drop) — as specified.
- Qualifier request-relativity — an unqualified `@Fallback` yields to a qualified non-fallback
  sibling only for the unqualified request; a qualified fallback still wins its own qualified
  request (the `qualifier="in_memory"` escape hatch) — exactly the deliberate CDI deviation this
  report argued for, confirmed at `container.py:2096-2112`.
- Two fallbacks, no non-fallback sibling: neither is dropped, `max()`'s existing first-registered
  tie-break applies, and `AMBIGUOUS_BINDING` (pre-existing `IssueKind`, unchanged — §5 row 4's "no
  second kind is asked for") reports the tie. No new kind was added for this case, matching the
  ask precisely.
- `get_all()`/`aget_all()`/`list[T]` multibinding all exclude a shadowed fallback via the shared
  `_filter()` choke point — the "for free, specified as a required property" row, confirmed.
- Not evicted: a fallback singleton resolved before its shadower is registered stays cached — the
  `@Profile` caveat this report said would be inherited verbatim, confirmed unchanged.

`Severity.INFO` never raises and never affects `report.ok` (`validation.py`) — this report's
recommendation (1) was adopted in full, not the `WARNING`-tier fallback (2).

**Adoption is this plan** (Plan 045) — every varco-owned binding of a varco seam across all ten
packages is declared `@Fallback`, replacing the three priority-floor magic numbers this report's
§2 catalogued (72 raw `-sys.maxsize`/`-sys.maxsize - 1` hits plus `_FALLBACK_PRIORITY`) with the
two-tier `@Fallback` + `varco_core.di.BACKEND_PRIORITY` model — see Plan 045's §Context site table
(A1-A20, B1-B13, D1-2) for every converted site. §6/§7's `FRAMEWORK_DEFAULT_PRIORITY` interim
(naming the floor once, documenting "ties resolve by registration order") was **never built** —
providify shipped the real primitive first, so Plan 045 went straight to `@Fallback` rather than
building and then immediately discarding the interim.

Guard: converted in Plan 045 Step 5 — `test_fallback_decorator_is_exported` and
`test_validate_reports_shadowed_fallback` (formerly `xfail`) now pass as written, plus three new
pins added per Plan 045 (§D-tests): qualified fallback wins its own qualified request, two
fallbacks tie by priority (`100` beats `0`), and `get_all()` of an all-fallback set returns both.
The three always-passing controls are kept, still true.
