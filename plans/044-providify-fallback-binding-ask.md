# Plan 044 — Upstream providify ask: `@DefaultBean`-style fallback binding (DI-15, third ask)

Covers the **remaining `todo` half** of BACKLOG 4.0 row **`DI-15`** (`BACKLOG.md:67`): *"a
`@DefaultBean`-style fallback binding … evaluated at the same lazy `_binding_is_active` point as
`@Profile`, with a `validate()` issue kind for 'default shadowed by X'"*. Plan 043 shipped the first
three asks (P24/P25/P26); this plan ships the fourth as a **new report + guard + ledger row**, not a
Plan 043 rewrite. Per `BACKLOG.md:55` (*missing providify features are filed upstream, never worked
around*) this plan ships **no production code** in any `varco_*` package.

**Research briefs backing this plan:**
[`017-fallback-binding-evaluation-timing-and-diagnostics.md`](../design/research/017-fallback-binding-evaluation-timing-and-diagnostics.md)
(evaluation-timing table §2; shadowing diagnostics §3; edge semantics §4a/§4b/§4c; Python
precedent §5; Librarian's Note) and
[`010-default-implementation-di-conventions.md`](../design/research/010-default-implementation-di-conventions.md)
(§1 Quarkus `@DefaultBean` intent-over-order vs Spring order coupling; §7 order of initialization;
§"Options Compared" precedence table).

**providify source of record**: installed copy at
`/home/edoardo/projects/varco/.venv/lib/python3.12/site-packages/providify/`, version **2.0.1**,
HEAD **`1402f0a`** (the same revision Plan 043 cited; Step 1 re-confirms both). Every `providify/…`
citation below was read at that revision while writing this plan.

> ⚠️ **Five premises in the task brief are corrected here from source, and each reshapes a §D block.**
> (1) The priority tie-break is **not** at `container.py:1759-1761` — that range is
> `is_resolvable()`'s docstring. The tie-break is `max(candidates, key=lambda b: b.priority)` at
> **`container.py:2382`**, inside `_get_best_candidate()` (**`:2354-2382`**, not `:1735-1774`);
> Python's `max()` returns the *first* maximal element, i.e. first-registered wins. providify's own
> `IssueKind.AMBIGUOUS_BINDING` docstring says exactly this (`validation.py:77-79`: *"`max()` picks
> the first in registration order, silently and arbitrarily"*). (2) `_filter()` spans
> **`:1678-1733`** (the truth table the task cites is at `:1691-1698` inside it). (3)
> `Severity` has **only `ERROR` and `WARNING`** (`validation.py:41-55`) — there is no `INFO` tier to
> report a shadowed fallback at; §D-diagnostic must ask for one or settle for `WARNING`. (4)
> `IssueKind.AMBIGUOUS_BINDING` **already exists** (`validation.py:77-79`) and already covers
> "≥2 candidates tied at max priority" — the "two fallbacks, no winner" case needs **no second
> kind** (§D-predicate). (5) Three of the five varco_core floor sites the task lists are
> **qualified** (`event/memory.py:87`, `cache/memory.py:145`, `event/dlq.py:524` all carry
> `qualifier="in_memory"`); only `flags/null.py:27` and `revocation/null.py:35` are unqualified.
> Because `_filter()` lets an **unqualified request match any qualifier** (`:1729`,
> `qualifier is None or b.qualifier == qualifier`), the qualifier semantics in §D-predicate must be
> specified against providify's actual matching rule, not against CDI's.

## Goal

A fourth providify gap — the absence of a fallback binding primitive — exists as a durable,
source-cited, maintainer-actionable report at `design/upstream-gaps/providify-fallback-binding.md`,
with two Docker-free `strict=True` xfail guards (plus three passing controls) in the existing
`varco_core/tests/test_providify_upstream_gaps.py`, one `UPSTREAM-GAPS.md` row (`P27`), and a
CHANGELOG entry — so that `DI-3` can be planned against a written statement of what providify does
today, what was asked for, and exactly which resolve-time semantics the ask specifies.

## Non-goals

- **Any change to providify itself.** The user owns providify; this plan produces the *ask*.
- **Any varco shim, helper, or translation layer.** `BACKLOG.md:55` forbids it, and §D-not-ours
  records that — unlike P25 — there is **no** supported public-API interim path for this gap at all.
- **The named `FRAMEWORK_DEFAULT_PRIORITY` floor.** That is `DI-3`'s interim (`BACKLOG.md:76`),
  lives in `DI-3`'s plan, and is only *referenced* here as the workaround in the report's §7.
- **Touching P24/P25/P26 reports or tests** beyond adding the new class and its docstring-map
  entry to the shared test module. Brief 014's wrong-`@Requires` banner is Plan 043 Step 8 — done,
  not here.
- **Re-deciding the P25 `@Requires(condition=)` ask.** The report's §2 states the distinction
  (predicate over *settings* vs predicate over the *binding set*) in one paragraph and moves on.
- **README.md / CLAUDE.md / technical_docs content.** No user-facing behaviour changes.
- **The `BACKLOG.md` `DI-15` status flip** is listed as Step 6 for completeness; the launching
  thread has said it will perform it. The implementer does it only if it has not already happened.

## Context

### What providify 2.0.1 offers today (verified at `1402f0a`)

| Primitive | Where | What it does / why it matters to this ask |
|---|---|---|
| `_binding_is_active(b)` | `container.py:1606-1676`; `profile_ok` at `:1665`, `alternative_ok` at `:1669-1676` | **Single-binding signature** — sees `b` only, never the rest of the binding set. A "no other binding of `T`" predicate cannot live inside it unchanged (§D-lazy) |
| `_filter(cls, qualifier, priority)` | `container.py:1678-1733`; comprehension `:1714-1733`; truth table `:1691-1698` | The one place the full candidate list exists. Interface match `:1720` (`_interface_matches`), `exact_only` guard `:1728`, qualifier rule `:1729` (**`None` matches any qualifier**), priority rule `:1730`, activation `:1732`. Called by `get`/`aget`/`get_all`/`aget_all`/`is_resolvable` — a post-filter step here reaches every consumer (§D-collection) |
| `_get_best_candidate()` | `container.py:2354-2382`; `LookupError` at `:2377`; `max(candidates, key=priority)` at `:2382` | Highest priority wins; **ties → first in registration order** (`max()` semantics). This is the rule a fallback must *not* rely on (§D-guard) |
| `get_all()` / `aget_all()` | `container.py:1379` / `:1570-1602`; `aget_all` sorts `_filter()`'s result by priority `:1599-1602` | Collection injection sees *every* active candidate — today a floor-priority default is always in the list (§D-collection) |
| `is_resolvable()` | `container.py:1735-1780`; docstring `:1770-1771` | *"Results are not cached — re-evaluated on every call, so a new binding added between two calls will be reflected immediately. ✅"* — providify's own statement that the binding set is **mutable after registration and consulted lazily** (§D-lazy) |
| `enable_alternative`/`disable_alternative` | `container.py:1784` / `:1796` | Post-registration mutation of activation state |
| `activate_profile`/`deactivate_profile` | `container.py:1836` / `:1877`; the not-evicted caveat `:1865-1868` | Post-registration mutation of activation state; an already-cached singleton is **not** evicted when its profile is deactivated — the same caveat the fallback ask inherits |
| `@Profile(*exprs)` | `decorator/scope.py:694-798`; usable on classes **and** `@Provider` functions `:726-729`; stamped on `__dict__` `:786-796`; read off `b.implementation`/`b.fn` at `container.py:1669` | **The marker-decorator precedent** the ask copies: one decorator, both binding shapes, no `Singleton`/`Provider` signature change (§D-name) |
| `@Alternative` | `decorator/scope.py:669-686` | The *inverse* direction — the replacement must be decorated and enabled; not a fallback |
| `@Default` | `decorator/scope.py:646-654` | A **qualifier**, alias of "no qualifier" (`_filter` `:1708-1712`). **Not** this feature — `BACKLOG.md:263-267` "Answered decisions". Drives the naming choice in §D-name |
| `validate()` / `validate_bindings()` / `validate_all()` | `container.py:5249` / `:5119` / `:5171` | Pass 1 is unfiltered over `self._bindings` (`:5327-5333`, per Plan 043's reading — ⚠️ re-verify) |
| `Severity` | `validation.py:41-55` — `ERROR`, `WARNING` **only** | No `INFO` tier (§D-diagnostic) |
| `IssueKind(StrEnum)` | `validation.py:58-96`; `AMBIGUOUS_BINDING` `:77-79`; `UNREACHABLE_PRE_DESTROY` `:92-96` | The enum a `FALLBACK_SHADOWED` member would join; `AMBIGUOUS_BINDING` already covers equal-priority ties |
| Fallback primitive | — | **Absent.** `rg -in 'fallback|DefaultBean|missing_beans|missingBeans|TryAdd|ConditionalOnMissing' providify/` must return only prose hits (Step 1 records the count) |

### Where varco pays today (evidence for the report's §2)

Every framework default is a **priority-floor heuristic** — `priority=-sys.maxsize - 1` — whose
only contract is "anything else outranks me". `rg -n 'priority=-sys\.maxsize' varco_*/varco_*` at
plan time: **72 raw hits** (Plan 043's P25 report already cited this number). Step 3 re-counts and
**classifies** them, because the raw count mixes four things the report must keep apart:

1. **Unqualified `-sys.maxsize - 1` sites — the true fallbacks.** varco_core scanned `@Singleton`s:
   `flags/null.py:27` (`NullFeatureFlags`), `revocation/null.py:35` (`NullTokenRevocationStore`),
   `tenancy/membership.py:237`, `event/producer.py:146`, `event/serializer.py:116`. varco_fastapi:
   the **13** `@Provider(... priority=-sys.maxsize - 1)` methods in `varco_fastapi/varco_fastapi/di.py`
   (`:280,290,301,311,334,350,371,384,395,400,410,418,432`) plus the three imperative
   `Singleton(priority=-sys.maxsize - 1)(X)` calls (`:104,105,615`). Backends: e.g.
   `varco_redis/varco_redis/revocation.py:256`, `varco_redis/cache.py:567,787`,
   `varco_kafka/dlq.py:772`, `varco_sa/di.py:176`, `varco_memcached/cache.py:636,647`.
2. **Qualified `-sys.maxsize - 1` sites** — `event/memory.py:87`, `cache/memory.py:145`,
   `event/dlq.py:524`, `lock.py:373`, `event/deduplication.py:157` (all `qualifier="in_memory"`).
   These are *both* a named escape hatch (`get(T, qualifier="in_memory")`) *and* the unqualified
   default — which only works because `_filter` `:1729` lets an unqualified request see them.
3. **`-sys.maxsize` (one above the floor) sites** — a hand-built **second tier**: backend
   singletons (`varco_redis/bus.py:102`, `varco_kafka/bus.py:95`, `varco_nats/bus.py:129`, the
   `health.py` probes, …) placed so they beat the core in-memory default but lose to anything the
   app registers. Two magic numbers encoding "fallback beneath a fallback" is the clearest
   evidence that the heuristic is already being stretched past what a number can express.
4. **Prose mentions** (docstrings/comments explaining the pattern, e.g.
   `varco_redis/config.py:165`, `varco_core/event/cloudevents.py:81,516`).

What the heuristic cannot do, and a fallback binding can (each is a §2 bullet in the report):
- **State intent.** `priority=-sys.maxsize - 1` is indistinguishable from "a very low-priority real
  binding"; `describe()`/`validate()` cannot tell an operator "this is a default and it was
  shadowed by X" (`DI-7`'s report needs exactly that — `BACKLOG.md:78`).
- **Survive an equal-priority collision.** A user who copies a varco default's decorator line
  (including its priority) registers at the floor too and loses by registration order
  (`:2382`) — silently. brief 010 §1: Quarkus `@DefaultBean` yields *"without registration-order
  coupling"*; Spring's order coupling is what `DI-3` names as the undocumented "provide before
  scan to win" folk rule (`BACKLOG.md:76`).
- **Stay out of collections.** `get_all(T)`/`InjectInstances[T]` includes the default beside the
  real implementation (`:1599-1602`) — an app fanning out to "every event bus" also gets the
  in-memory one.

## Design

### §D-shape — one report, one guard class, one ledger row; nothing else moves

```
 report   design/upstream-gaps/providify-fallback-binding.md        (new, 8 sections)
 guard    varco_core/tests/test_providify_upstream_gaps.py::TestP27FallbackBinding
          (new class in the EXISTING module; module docstring map gains one entry)
 ledger   UPSTREAM-GAPS.md  row  P27-FALLBACK-BINDING  (+ "Last updated" footer)
 log      CHANGELOG.md [Unreleased]  ### Documentation / Tests (Plan 044, DI-15 third ask)
 backlog  BACKLOG.md:67  DI-15 status  → done (Plan 043 + Plan 044)   [launching thread]
```

- ✅ Same file, same docstring-map index Plan 043 §D-guards chose, so
  `rg -n 'GAP: P2[0-9]-' varco_*/tests/` lists every live providify guard.
- ❌ A separate `test_providify_fallback_binding.py` would die whole when the gap closes but breaks
  the single-index property. Rejected, as in Plan 043.

⚠️ **`P27` is an ASSUMPTION** — the ledger shows P24/P25/P26 at `UPSTREAM-GAPS.md:59-61`. Step 1
confirms with `rg -n 'P27' UPSTREAM-GAPS.md CHANGELOG.md design/upstream-gaps/` returning nothing;
if taken, use the next free number everywhere (report header, `reason=` strings, docstring map,
ledger, CHANGELOG).

### §D-name — ask for a `@Profile`-shaped marker decorator, `@Fallback`

**Chosen: `providify.Fallback` — a bare marker decorator, usable on a class or a `@Provider`
function, stamped on `__dict__` and read off `b.implementation`/`b.fn` exactly as `@Profile` is.**

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) `@Fallback` marker decorator** (copy `@Profile`'s shape, `scope.py:694-798`) | Works on **both** binding shapes varco has — scanned `@Singleton` classes and `@Provider` functions/`@Configuration` methods — with **zero** change to `Singleton()`/`Provider()` signatures, because `_binding_is_active` already reads markers off `b.implementation if ClassBinding else b.fn` (`container.py:1669`). Symmetric with `@Alternative` (the inverse direction). One greppable token. Composes with `@Profile`/`@Alternative` by the same AND | A third marker on the source object; inherits `@Profile`'s "`__dict__`-only, subclass does not inherit" rule (`scope.py:754-755`) — must be documented the same way |
| **(2) `Singleton(fallback=True)` / `Provider(fallback=True)` kwarg** | Reads as a binding attribute, next to `priority=`; one line per site | Two (really four — `Singleton`/`Component`/`Provider`/`Configuration`-method) signature changes plus the imperative `Singleton(...)(cls)` call form; `Alternative`/`Profile` are *not* kwargs, so this breaks providify's own convention that activation markers are decorators. Offer as **sugar over (1)** only |
| **(3) `@DefaultBean`** (the Quarkus name) | Operators from the JVM recognise it | providify already exports `Default` — a **qualifier** (`scope.py:646-654`, CDI `@Default`), and `BACKLOG.md:263-267` records that this very name collision already misled one reader. `DefaultBean` beside `Default` in the same namespace invites the confusion permanently. Rejected |
| **(4) `@Requires(missing_beans=T)`** (the Micronaut name) | Names the predicate's target | Couples this ask to the unshipped P25 `@Requires` decorator; forces the user to **repeat the interface** (`missing_beans=AbstractEventBus`) that the binding already declares via `returns=`/base class, a redundancy that rots; Micronaut's own `missingBeans` is qualifier-blind (brief 017 §4c) and compile-time (§1) — the wrong model to borrow a name from. Rejected |

The report's §4 presents (1) as the primitive, (2) as optional sugar, and states plainly that any
name is acceptable if the §5 semantics match — the guard is name-based (§D-guard) and the report is
the contract (the P25 precedent, Plan 043 §D-failmode).

### §D-predicate — "shadowed" is request-relative, computed over the active candidate set

**Chosen: a fallback is shadowed, for a given request `(T, qualifier, priority)`, iff at least one
active non-fallback binding is a candidate for that same request.** Formally, as a post-filter
step at the end of `_filter()` (`:1714-1733`):

```
candidates = [ …today's comprehension… ]
if any(not is_fallback(b) for b in candidates):
    candidates = [b for b in candidates if not is_fallback(b)]
return candidates
```

This one rule settles every edge the task lists, each **by construction** rather than by a
special case:

| Edge | Behaviour under the rule | Grounding |
|---|---|---|
| **Inactive shadower** — a `@Profile`-off or un-enabled `@Alternative` binding of `T` | Does **not** shadow: it never enters `candidates` because `_binding_is_active` (`:1732`) already dropped it. When `activate_profile()`/`enable_alternative()` later activates it, the **next** `get()` sees it (lazy, §D-lazy) | brief 017 §4b: the Quarkus/Micronaut model (*"count as present only if active"*), explicitly **not** Spring's (#2897, where a profile-deactivated definition still shadows) — Librarian's Note edge 2 |
| **Qualifiers** — `@Singleton(qualifier="x")` non-fallback `X`, unqualified `@Fallback` `F` | `get(T)` → both are candidates (`:1729`, `None` matches any) → `F` shadowed → `X`. `get(T, qualifier="in_memory")` → only a binding with that qualifier is a candidate → an `"in_memory"`-qualified fallback is **not** shadowed → returned. `get(T, qualifier="y")` → neither matches → `LookupError` as today | brief 017 §4c: Spring cannot be qualifier-aware at all (#2834); Quarkus keys on type+qualifier. providify's rule is *request*-relative, which is **strictly more useful for varco**: the `qualifier="in_memory"` escape hatch at the five qualified sites (§Context item 2) keeps working, *and* the unqualified default is shadowed. The report must state that this differs from CDI identity semantics because providify's `_filter` already does |
| **Generic aliases** — fallback for `Repo[User]`, non-fallback for `Repo[Post]` | Not the same candidate set (`_interface_matches`, `:1720`) → no shadowing | brief 017 §4c's varco implication; consistent with P26 |
| **Two fallbacks, no non-fallback** | `candidates` is all-fallback → unchanged → today's `max()` rule: higher `priority` wins, exact ties → first registered (`:2382`) and **`IssueKind.AMBIGUOUS_BINDING` already reports the tie** (`validation.py:77-79`). **No second kind is asked for.** The report asks only that the ambiguity detector run on the *post-shadowing* candidate set, so that `{F1, F2, X}` reports nothing (X wins cleanly) while `{F1, F2}` at equal priority still does | brief 017 §4a: Quarkus #37042 (`@Priority`, then hard ambiguity error); ASP.NET silently takes the first; Spring the last. providify already has the Quarkus shape minus the hard error — keep it, because a hard error at `get()` time would make every varco two-tier site (§Context item 3) fail if both tiers were ever marked `@Fallback` |
| **Circularity** (Micronaut #9315) | **Impossible.** `is_fallback(b)` is a marker read, not a resolution; the rule consults only *non-fallback* siblings; a fallback never shadows a fallback; no binding's activity depends on another's activity | brief 017 §1, §Evidence Gap 3 — Micronaut ships no mitigation; providify's rule needs none |
| **`exact_only` self-bindings** (`:1721-1728`) | Unchanged — they are never fallbacks and only match their own type | — |
| **`priority=` narrowing** (`get(T, priority=p)`) | The rule runs after priority narrowing, so `get(T, priority=FLOOR)` returns the fallback even when shadowed — an explicit "give me the default" request | Mirrors the qualifier row; name it in the report so the maintainer decides whether that is desired |

- ✅ Zero new container state; no signature change to `_binding_is_active`; `O(len(candidates))`
  extra work per lookup (not `O(len(self._bindings))` — the task's "every `get(T)` checks siblings
  → add a cache" concern is about a *second scan*, which this design does not perform; `_filter`
  already walks every binding once). A per-interface cache is therefore **not** asked for; the
  report notes it as a future option *if* providify ever memoizes `_filter` itself.
- ❌ The rule is request-relative rather than a property of the binding: "is this fallback
  shadowed?" has no answer without a request. For `validate()` (§D-diagnostic) the report
  specifies the request to evaluate: `(b.interface, b.qualifier, None)` — i.e. "would the most
  natural request for this binding be shadowed?"

**Alternative — extend `_binding_is_active(b)` to `_binding_is_active(b, siblings)`**: ✅ keeps the
"single activation predicate" docstring promise (`:1609-1612`); ❌ needs the sibling list computed
*before* activation filtering, so inactive siblings would have to be re-filtered inside the
predicate — duplicating `_filter`'s work and making the inactive-shadower rule a special case
instead of free. Rejected; the report mentions it as the less-preferred placement.

### §D-lazy — resolve-time evaluation is novel across the surveyed frameworks, and providify's own model requires it

brief 017 §2's table: Spring (config-parse time, frozen), Quarkus (build time), ASP.NET (`TryAdd`
call time, frozen), Micronaut (compile time) — **none re-evaluates after a later registration**.
providify must, because its binding set and activation state are **mutable after registration by
design**, and it already promises lazy visibility:

- `bind()`/`register()`/`provide()` (`container.py:991`/`:1026`/`:1048`) and `scan()` may be called
  at any time, in any order — `DI-3`'s whole complaint (`BACKLOG.md:76`) is that the *current*
  floor heuristic makes that order matter.
- `enable_alternative()` (`:1784`) and `activate_profile()` (`:1836`) mutate activation **after**
  registration, and `_binding_is_active` is evaluated on every `_filter()` call precisely so
  those mutations are honoured (`:1609-1612`).
- `is_resolvable()`'s own docstring guarantees *"a new binding added between two calls will be
  reflected immediately"* (`:1770-1771`).

A registration-time check (ASP.NET `TryAdd`, brief 017 §2 row 3) would evaluate "no other binding
of `T`" at the moment the fallback is registered — so `container.scan("varco_core")` **before**
`container.install(AppModule)` would keep the fallback and *then* have the app's real binding
arrive, producing two bindings and the floor-priority race this ask exists to remove. The report's
§4 argues this as the reason providify's ask is deliberately the one design no surveyed framework
ships (brief 017 Librarian's Note: *"the evidence favors resolution-time evaluation"*).

**Cost and caveat to state in the report:**
- Cost: `O(len(candidates))` per lookup (§D-predicate); no cache asked for.
- Caveat, inherited verbatim from `@Profile`: a fallback **singleton already instantiated and
  cached** before the shadowing binding arrived is **not evicted** — the same documented
  behaviour as `activate_profile()` at `:1865-1868`. The report says so and notes that varco's
  bootstrap order (scan defaults → install app → first `get()`) never hits it in practice.

### §D-collection — a shadowed fallback is absent from `get_all()`/`aget_all()`/`InjectInstances`/`Multibound`

Because `get_all()` (`:1379`), `aget_all()` (`:1593`), `is_resolvable()` (`:1780`) and
`_get_best_candidate()` (`:2375`) all obtain their candidates from `_filter()`, the §D-predicate
post-filter step excludes a shadowed fallback from **every** consumer at once — Quarkus's
`@All`/`List<T>` behaviour (brief 017 §2 row 2; Librarian's Note edge 4) for free. The report
specifies this explicitly as a required property, not an implementation accident, and asks that
the multibinding paths (`_multibound_inner`, `:1438`; `_collect_async`, `:1563`) be confirmed to
route through `_filter()` — ⚠️ Step 1 verifies by reading `:1438-1470` and recording the result.

### §D-diagnostic — `IssueKind.FALLBACK_SHADOWED`, asked at a new `Severity.INFO`, `WARNING` accepted as the fallback

**Chosen ask: `validate()` reports one `ValidationIssue(kind=FALLBACK_SHADOWED, severity=INFO)`
per shadowed fallback, with a `shadowed_by` detail naming the winning binding (owner + interface
+ qualifier + priority), evaluated for the request `(b.interface, b.qualifier, None)`.**

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) New `Severity.INFO` + `IssueKind.FALLBACK_SHADOWED`** | Precise: a shadowed default is **expected behaviour**, not "legal-but-notable" (`WARNING`'s definition, `validation.py:46-48`). `DI-7`'s wiring report (`BACKLOG.md:78`) can render it as "default / overridden by X" — Spring `/actuator/conditions` `negativeMatches` shape, brief 017 §3 | A new `Severity` member every consumer's `match` must handle; providify has lived with two tiers so far |
| **(2) `FALLBACK_SHADOWED` at `WARNING`** | No `Severity` change | A varco app with ~20 shadowed defaults emits ~20 warnings on every `validate()` — a CI that gates on zero warnings would reject correct wiring. Acceptable **only** if (1) is declined; the report says so |
| **(3) No issue kind; `describe()` only** | Smallest change | Brief 017 §3: Micronaut (debug log only) and ASP.NET (nothing) are the *cautionary* examples; the report's whole point is that a shadowed default must be visible to the operator |
| **(4) A second kind for "two fallbacks tie"** | — | **Not asked** — `AMBIGUOUS_BINDING` (`validation.py:77-79`) already is that kind (§D-predicate). Asking for a duplicate would be a U-8-class error |

### §D-guard — two `strict=True` xfails with distinct flip conditions, three passing controls

| Test | Marker | Today fails with | Flips when |
|---|---|---|---|
| `test_fallback_decorator_is_exported` | `xfail(strict=True, raises=ImportError)` | `from providify import Fallback` → `ImportError` (`providify/__init__.py` exports no such name) | providify exports `Fallback`. Body then runs the **full behavioural contract**: `@Fallback @Singleton class A(Base)`, plain `@Singleton class B(Base)` at **equal priority (0)**, `bind(Base, A)` **first**, `bind(Base, B)` second → `assert isinstance(get(Base), B)`; then `assert [type(x) for x in get_all(Base)] == [B]` (§D-collection); then a fresh container with `A` alone → `assert isinstance(get(Base), A)`. If the body fails after the import succeeds, the failure is **not** `ImportError` → loud, "semantics differ, revisit" |
| `test_validate_reports_shadowed_fallback` | `xfail(strict=True, raises=AttributeError)` | **First statement** `IssueKind.FALLBACK_SHADOWED` → `AttributeError` (StrEnum member missing, `validation.py:58-96`) — evaluated *before* the `Fallback` import so the observed failure is the diagnostic half, not the decorator half | providify adds the member. Body then builds the `{A (fallback), B}` container, calls `validate(raise_on_error=False)`, asserts one issue of that kind whose details name `B`. Independent flip from the first guard: the maintainer may ship the decorator without the diagnostic (then this stays xfail, correctly) or the diagnostic without the decorator (then this fails with `ImportError` — loud, revisit) |
| `test_equal_priority_tie_breaks_to_first_registered_today` | none (control) | passes | Never flips for unmarked bindings: `bind(Base, A)` then `bind(Base, B)` at priority 0 → `get(Base)` is `A`. Cites `container.py:2382` and `validation.py:77-79`. **This is why the xfail uses equal priority**: it pins the exact tie-break a fallback must beat |
| `test_higher_priority_already_wins_regardless_of_order_today` | none (control) | passes | `A` at priority 0 first, `B` at `priority=1` second → `get(Base)` is `B`. Proves the floor heuristic *works* — and therefore that a behavioural xfail using a **higher**-priority `B` would pass today and be no guard at all (the task's warning, made mechanical) |
| `test_inactive_sibling_is_not_a_candidate_today` | none (control) | passes | `A` plain, `B` under `@Profile("x")` with `DIContainer(profiles=())` → `get_all(Base)` is `[A]`; after `activate_profile("x")` → two. Grounds §D-predicate's inactive-shadower row on the shipped `_filter` (`:1732`) |

- ✅ Every xfail names `raises=`, so a wrong-reason failure is a real red (Plan 043 §D-failmode).
- ✅ Providify-only: a local `class Base`, `Singleton`, `Profile`, `DIContainer`, `IssueKind` —
  pasteable into providify's test suite verbatim; the report's §3 **is** these bodies.
- ✅ Docker-free, runs in `make test PKG=varco_core` and CI's `unit` job.
- ❌ Name-based, like P25: a feature shipped under another name stays silently xfail. Mitigation:
  the report names the contract, and Step 1's absence `rg` is re-run at every providify bump.

**Alternative — a behavioural xfail with a no-op stand-in** (`try: from providify import Fallback
except ImportError: Fallback = lambda c: c`) so the body runs today and fails with
`AssertionError` (`A` wins by order), flipping to XPASS when the real decorator arrives:
✅ the xfail is about behaviour, not a name; ❌ one test then fails for two conflated reasons,
needs a `# type: ignore` on the redefinition, and a stand-in decorator inside a guard *is* a
shim-in-miniature of the very kind `BACKLOG.md:55` forbids in spirit. Rejected; the first
guard's body already carries the behavioural contract and runs the moment the import succeeds.

**Alternative — only the `ImportError` guard**: ✅ minimal; ❌ loses the independent flip for the
diagnostic half, which is the part `DI-7` depends on. Rejected; P26's two-guard precedent
(matching vs delivery) applies.

### §D-not-ours — no varco-side fix exists; say so plainly

P25 had a "partly ours" answer (a public-API `activate_profile()` translation). **This gap does
not.** The floor priority is a *heuristic over a number*, not a translation of the feature through
any supported providify API: there is no public call that says "hide this binding if another
exists", and emulating one would mean varco inspecting `container._bindings` (private) or
wrapping `get()` — both shims. `DI-3`'s interim (`BACKLOG.md:76`) is to **name** the floor once
(`FRAMEWORK_DEFAULT_PRIORITY`) and document the tie rule; that is honesty about the heuristic, not
a workaround of the gap, and it lives in `DI-3`'s plan. The report's §6 says "No" in its first
sentence, and §7 points at `DI-3` without restating it.

### §D-changelog — one `[Unreleased]` block, same register as Plan 043's

Plan 043 §D-changelog's reasoning holds (the P22 closure precedent, `CHANGELOG.md:758-763`): the
filing must be findable from the file where its resolution will land. One
`### Documentation / Tests (Plan 044, DI-15 third ask)` block under `## [Unreleased]`, directly
below Plan 043's block (`CHANGELOG.md:12-34`), ending *"No runtime behaviour changes; no public API
changes."*

## Steps

All commands run from `/home/edoardo/projects/varco`. Steps 2 and 3 are independent; 4-6 depend
on both; 7 is last.

1. [x] **Preflight — re-verify every citation and the free ID.**
       `uv run python -c "import importlib.metadata as m; print(m.version('providify'))"` → `2.0.1`;
       `git -C /home/edoardo/projects/providify rev-parse --short HEAD` → `1402f0a` (if the sibling
       checkout is absent, record "installed copy only" and cite the `.venv` path). Then **re-read and
       record** in `/tmp/claude-1000/…/scratchpad/044-preflight.txt` the exact lines for:
       `_binding_is_active` (`container.py:1606-1676`), `_filter` (`:1678-1733`, qualifier rule
       `:1729`, activation `:1732`), `_get_best_candidate` (`:2354-2382`, `max()` at `:2382`),
       `get_all` (`:1379`), `aget_all` (`:1570-1602`), `_multibound_inner` (`:1438` — ⚠️ read to
       `:1470` and note whether it calls `_filter`), `is_resolvable` docstring (`:1770-1771`),
       `enable_alternative` (`:1784`), `activate_profile` (`:1836`, caveat `:1865-1868`),
       `validate` (`:5249`) and the unfiltered pass-1 docstring (`:5327-5333`),
       `Severity` (`validation.py:41-55`), `IssueKind` (`:58-96`, `AMBIGUOUS_BINDING` `:77-79`),
       `@Profile` (`decorator/scope.py:694-798`, both-shapes note `:726-729`), `@Alternative`
       (`:669-686`), `@Default` (`:646-654`), `bind()` (`container.py:991` — ⚠️ confirm it reads
       `priority` off the implementation's `@Singleton(...)` metadata, which the guards rely on).
       Absence: `rg -in 'fallback|DefaultBean|missing_beans|missingBeans|TryAdd|ConditionalOnMissing' /home/edoardo/projects/varco/.venv/lib/python3.12/site-packages/providify/`
       → record the hit count and confirm every hit is prose. Free ID:
       `rg -n 'P27' UPSTREAM-GAPS.md CHANGELOG.md design/upstream-gaps/` → no hits. If the version,
       HEAD, or **any** line number differs, stop and fix every citation in this plan's §Context
       before continuing (CLAUDE.md U-8: cite source, not docs; quote both when they disagree).
       **Verify**: the scratchpad file exists and lists ≥ 18 `file:line` confirmations.

2. [x] `varco_core/tests/test_providify_upstream_gaps.py` — (a) extend the module docstring map
       (`:5-20`) with a **P27-FALLBACK-BINDING** entry (report path → `TestP27FallbackBinding`,
       one sentence: "no fallback-binding primitive; `_filter()` `:1714-1733` has no sibling check
       and `_get_best_candidate()` `:2382` breaks priority ties by registration order"); update
       the title line (`:2-3`) to list P27. (b) Append `class TestP27FallbackBinding` with the
       five tests of §D-guard, **in this order**: the two xfails first, then the three controls.
       Write each xfail **without** its marker first, run it, paste the observed exception type,
       message, and providify `file:line` into its docstring, *then* add
       `@pytest.mark.xfail(strict=True, raises=ImportError, reason="GAP: P27-FALLBACK-BINDING — providify has no fallback-binding primitive; priority ties break by registration order (providify/container.py:2382); see design/upstream-gaps/providify-fallback-binding.md")`
       (and `raises=AttributeError` for the `validate()` guard, same `reason=` prefix). The
       `from providify import Fallback` line needs `# type: ignore[attr-defined]`; the
       `IssueKind.FALLBACK_SHADOWED` access needs the same. Class docstring: cite the unqualified
       floor sites `varco_core/varco_core/flags/null.py:27`, `revocation/null.py:35`, the
       qualified trio `event/memory.py:87`/`cache/memory.py:145`/`event/dlq.py:524`, and
       `varco_fastapi/varco_fastapi/di.py:280-432` (13 `@Provider`s), and state that the report is
       the contract. Controls cite `container.py:2382`, `validation.py:77-79`, `container.py:1732`.
       **Verify**: `uv run pytest varco_core/tests/test_providify_upstream_gaps.py -q -rxX -k P27`
       → `2 xfailed, 3 passed, 0 xpassed`; full file → `5 xfailed, 5 passed`. Temporarily remove
       each marker in turn and re-run → `ImportError` / `AttributeError` respectively (paste the
       providify frame into the docstring); restore. `make type-check` clean on the file.

3. [x] `design/upstream-gaps/providify-fallback-binding.md` (**new**) — header in the P25 file's
       shape: `**ID**: P27-FALLBACK-BINDING`, `**Raised by**: varco, Plan 044 / DI-15 (third ask)`,
       `**Date**: 2026-09-12`, `**providify version**: 2.0.1 (\`1402f0a\`)`, `**Status**: ⛔ Open —
       filed, not worked around`, `**Priority**: 🔴 must — gates DI-3's default-binding contract;
       DI-7's "default shadowed by X" report`. Then exactly eight sections:
       **§1 What providify does today** — the §Context table with every `file:line` from Step 1;
       quote `is_resolvable`'s `:1770-1771` docstring verbatim (providify's own lazy-visibility
       promise); quote `AMBIGUOUS_BINDING`'s `validation.py:77-79` docstring verbatim (providify's
       own statement of the tie-break); state `Severity` has only `ERROR`/`WARNING`; the Step 1
       absence `rg` command and hit count. **§2 Why it is a gap** — the four-way classification of
       the re-counted `rg -n 'priority=-sys\.maxsize' varco_*/varco_*` hits (§Context "Where varco
       pays"), with the two-tier `-sys.maxsize-1`/`-sys.maxsize` observation and the three "cannot
       do" bullets; one paragraph distinguishing this ask from P25 (predicate over the binding set,
       answer changes with later registrations — vs predicate over settings). Cite brief 010 §1
       (intent over order), §7, §"Options Compared" row 2. **§3 Minimal reproduction** — the Step 2
       xfail bodies verbatim, plus the observed-today output from the marker-off runs.
       **§4 The ask** — §D-name's four-row table (recommend `@Fallback`, `fallback=True` as sugar),
       §D-lazy's argument (brief 017 §2 table: no surveyed framework evaluates lazily; why
       providify's mutable container requires it; cost; the `:1865-1868` not-evicted caveat),
       §D-predicate's placement (post-filter in `_filter()`, with the rejected
       `_binding_is_active(b, siblings)` alternative), §D-collection, §D-diagnostic's four-row
       table (recommend `Severity.INFO` + `FALLBACK_SHADOWED`; `WARNING` accepted; no second kind).
       **§5 Specified edge semantics** — §D-predicate's eight-row table verbatim, each row citing
       brief 017 (§4a Quarkus #37042, §4b #2897 vs Quarkus/Micronaut, §4c Spring #2834, §1
       Micronaut #9315) and the providify line it rides on; state that qualifier semantics are
       *request-relative* and differ from CDI identity because `_filter` `:1729` already does.
       Add brief 017 §5: no Python DI library ships this — providify would be first. **§6 Partly
       ours? No.** — §D-not-ours verbatim; first sentence is "No". **§7 Interim workaround** — none
       applied by this plan; `DI-3` will name the floor `FRAMEWORK_DEFAULT_PRIORITY` and document
       "ties resolve by registration order" (`BACKLOG.md:76`) — a statement of the heuristic, not a
       workaround of the gap. **§8 Guard** — the two xfail IDs with `raises=`, the three controls,
       the name-based caveat and the "re-run §1's `rg` at every providify bump" instruction.
       **Verify**: `rg -c 'container.py:|validation.py:|scope.py:' design/upstream-gaps/providify-fallback-binding.md` ≥ 14;
       `rg -n '017 §|017 Librarian|010 §' design/upstream-gaps/providify-fallback-binding.md` ≥ 8
       and every referenced § exists in the brief; §6's first word is "No".

4. [x] `UPSTREAM-GAPS.md` — one new row after `:61` in the P25/P26 shape:
       `| **P27-FALLBACK-BINDING** | \`providify\` 2.0.1 (\`1402f0a\`) | No fallback-binding primitive — \`_filter()\` (\`providify/container.py:1714-1733\`) has no "drop fallbacks when a non-fallback candidate exists" step and \`_get_best_candidate()\` breaks priority ties by registration order (\`:2382\`), so varco's framework defaults can only be a \`priority=-sys.maxsize-1\` heuristic that loses an equal-priority collision silently and appears in every \`get_all()\` | [providify-fallback-binding.md](design/upstream-gaps/providify-fallback-binding.md) | \`varco_core/tests/test_providify_upstream_gaps.py::TestP27FallbackBinding::test_fallback_decorator_is_exported\` (strict xfail, \`raises=ImportError\`) + \`::test_validate_reports_shadowed_fallback\` (strict xfail, \`raises=AttributeError\`), no Docker, every \`make test\` |`.
       Update `:78` → `**Last updated:** 2026-09-12 · Plan 044 / DI-15 opened P27 (fallback binding); Plan 043 opened P25, P26; P24 re-verified.`
       **Verify**: `rg -c '^\| \*\*P2' UPSTREAM-GAPS.md` → `4`; `rg -n 'P27' UPSTREAM-GAPS.md design/upstream-gaps/` → ≥ 2 hits; `ls design/upstream-gaps/providify-fallback-binding.md`.

5. [x] `CHANGELOG.md` — under `## [Unreleased]`, after Plan 043's block (after `:34`), add
       `### Documentation / Tests (Plan 044, DI-15 third ask)` with one bullet:
       **Filed `P27-FALLBACK-BINDING`** — providify 2.0.1 has no `@DefaultBean`-style fallback
       binding; framework defaults are a `priority=-sys.maxsize-1` heuristic that loses an
       equal-priority collision by registration order (`providify/container.py:2382`) and is never
       excluded from `get_all()`. The ask: a `@Fallback` marker evaluated lazily in `_filter()`
       (request-relative, active-candidates-only shadowing) plus `IssueKind.FALLBACK_SHADOWED`. See
       `design/upstream-gaps/providify-fallback-binding.md`, guarded by
       `varco_core/tests/test_providify_upstream_gaps.py::TestP27FallbackBinding` (two strict
       xfails, `raises=ImportError` / `raises=AttributeError`). End with *"No runtime behaviour
       changes; no public API changes."*
       **Verify**: `rg -n 'P27-FALLBACK-BINDING' CHANGELOG.md` → 1 hit.

6. [x] `BACKLOG.md:67` (`DI-15` row) — **only if the launching thread has not already done it**:
       status cell → `done (Plan 043: P24/P25/P26; Plan 044: P27 fallback binding)`; evidence cell
       append `; brief 017 (fallback evaluation timing)`. Do **not** edit `DI-3` (`:76`) — it
       already names `DI-15`'s third ask as its target mechanism and the named floor as interim.
       **Verify**: `rg -n 'Plan 044' BACKLOG.md` → 1 hit; `rg -n 'fallback-binding ask \*\*todo' BACKLOG.md` → 0 hits.

7. [x] **Full verification** — `make test PKG=varco_core` green (`5 xfailed` in the gaps module,
       zero `xpassed`); `make lint PKG=varco_core`; `make lint` (no-`PKG` path: `api-check` must
       report **no change** — this plan touches no `__all__`); `make type-check` clean (two new
       `# type: ignore[attr-defined]`, M1 rises by exactly two, directional only);
       `rg -n 'GAP: P2[0-9]-' varco_*/tests/` lists exactly four providify guards (P25 ×1, P26 ×2,
       P27 ×2 → 5 markers) each naming a ledger row; `git status` shows **no** change under
       `varco_*/varco_*` and no file under `/home/edoardo/projects/providify` modified.

## Edge cases

- **providify version/HEAD differs from 2.0.1/`1402f0a`** → Step 1 stops the plan; every
  `file:line` is re-read. Do not proceed on "the lines moved a little".
- **`bind(Base, A)` does not honour `@Singleton(priority=…)` on `A`** (Step 1 ⚠️) → use
  `container.register(A)` / `container.provide(...)` instead and record which; the equal-priority
  control must still show first-registered wins.
- **`IssueKind.FALLBACK_SHADOWED` raises something other than `AttributeError`** (unlikely for a
  `StrEnum`) → `raises=` names the observed class; docstring cites where.
- **The absence `rg` finds a real `fallback`-named primitive** → the gap is narrower or gone; stop,
  read it, and rewrite §1/§4 before filing. Do not file a report against a feature that exists.
- **`_multibound_inner`/`_collect_async` do not route through `_filter()`** (Step 1 ⚠️) → §D-collection
  must then *ask* for the exclusion explicitly as a second change site, with the line cited.
- **`P27` already taken** → substitute the next free number in every file, including `reason=`
  strings and the docstring map.
- **A control test fails at 2.0.1** → a §D premise is wrong; stop, re-read the cited lines, fix
  the §D block and the report before Step 4.
- **`make lint`'s `api-check` reports a change** → not caused by this plan; something else is dirty
  on the branch. Do not regenerate the snapshot from this plan.

## Verification

```bash
# Preflight
uv run python -c "import importlib.metadata as m; print(m.version('providify'))"   # 2.0.1
rg -in 'fallback|DefaultBean|missing_beans|missingBeans|TryAdd|ConditionalOnMissing' \
   /home/edoardo/projects/varco/.venv/lib/python3.12/site-packages/providify/         # prose only
rg -n 'P27' UPSTREAM-GAPS.md CHANGELOG.md design/upstream-gaps/                      # empty before, ≥3 after

# Guards
uv run pytest varco_core/tests/test_providify_upstream_gaps.py -q -rxX              # 5 xfailed, 5 passed, 0 xpassed
uv run pytest varco_core/tests/test_providify_upstream_gaps.py -q -rxX -k P27       # 2 xfailed, 3 passed

# Package + gates
make test PKG=varco_core
make lint PKG=varco_core
make lint            # api-check / asyncapi-check / import-budget all unchanged
make type-check

# Liveness cross-check
rg -n 'GAP: P2[0-9]-' varco_*/tests/            # 5 markers: P25×1, P26×2, P27×2
rg -c '^\| \*\*P2' UPSTREAM-GAPS.md             # 4
rg -n 'P27' UPSTREAM-GAPS.md design/upstream-gaps/
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — `P27` is the next free gap ID | IDs unique across ledger, reports, CHANGELOG, test `reason=` | Step 1 `rg`; substitute if taken |
| ⚠️ ASSUMPTION — `container.bind(iface, impl)` reads `priority` from `impl`'s `@Singleton(priority=…)` metadata (`container.py:991-1025` not read by this plan; the P25 control's `@Profile @Singleton` + `bind()` suggests markers are honoured) | The equal-priority and higher-priority controls exercise the intended priorities | Step 1 reads `:991-1025`; Edge cases row 2 gives the fallback spelling |
| ⚠️ ASSUMPTION — the multibinding paths (`_multibound_inner` `:1438`, `_collect_async` `:1563`) obtain candidates via `_filter()` | §D-collection's "free by construction" claim | Step 1 reads `:1438-1470`; if false, the report asks for a second change site explicitly |
| ⚠️ ASSUMPTION — `validate()`'s resolvability checks go through `_filter()`/`is_resolvable()` so a shadowed fallback is not reported as a spurious `AMBIGUOUS_BINDING` alongside its shadower | The ask's "run the ambiguity detector on the post-shadowing set" is a one-line request, not a redesign | Step 1 reads `:5249-5340`; the report phrases it as an ask either way |
| ⚠️ ASSUMPTION — `AMBIGUOUS_BINDING`'s current `Severity` (ERROR or WARNING) is acceptable for the all-fallback tie case | Two equal-priority fallbacks with no real binding must not make `validate()` raise in a varco app that never hits that shape | Step 1 records the severity from `container.py`'s emitter; if `ERROR`, the report notes it |
| Both guards are name-based (`Fallback`, `FALLBACK_SHADOWED`); a feature shipped under other names stays silently xfail | Every providify bump re-runs Step 1's absence `rg` (Plan 016 upgrade-checklist precedent) | Report §4 says any name is acceptable if §5 semantics match; guard docstrings say the report is the contract |
| The request-relative qualifier rule (§D-predicate) is **more permissive** than Quarkus's type+qualifier identity: an unqualified fallback *is* shadowed by a `@Named` non-fallback for an unqualified request | This is the behaviour varco's five qualified sites need (the `in_memory` escape hatch survives); the report must say it is a deliberate deviation from CDI, grounded in `_filter` `:1729` | §5 of the report states it explicitly; the maintainer decides |
| Scout/task counts drift (5 + 13 sites; 72 raw hits) | The report cites numbers the implementer measured and **classified** | Step 3 re-runs `rg` and classifies into the four buckets |
| `make type-check` under `strict = true` on two non-existent names | Exactly two targeted `# type: ignore[attr-defined]`; M1 +2, directional only | Step 7 |
| The report reads as a demand | Every §4 row carries ✅ **and** ❌; §4 recommends, never insists | Review §4 against §D-name / §D-diagnostic before Step 4 |
| The `BACKLOG.md` flip is done twice (launching thread + implementer) | One coherent `DI-15` status cell | Step 6 is conditional — check before editing |

## Out of scope / Parked

- **`DI-3` — the varco default-binding contract and `FRAMEWORK_DEFAULT_PRIORITY`.** Referenced in
  the report's §7 only. Its plan decides when/how the 15+ sites migrate once `@Fallback` ships.
- **`DI-7` — `varco diagnose`.** The `FALLBACK_SHADOWED` issue kind is asked *for* it; rendering
  it is `DI-7`'s work.
- **A varco-side `Fallback` stand-in, `container.get()` wrapper, or `_bindings` inspection** —
  forbidden shims (§D-not-ours).
- **Reconciling the two-tier `-sys.maxsize-1` / `-sys.maxsize` scheme** — evidence in the report's
  §2; fixing it is `DI-3`.
- **Qualifier-aware fallback identity beyond the request-relative rule** (e.g. "this fallback may
  only be shadowed by a binding with the same qualifier") — a stricter Quarkus-style option the
  maintainer may prefer; named in §5 as an alternative, not asked for.
- **Brief 014's wrong-`@Requires` banner** — Plan 043 Step 8, done.
- **README.md / CLAUDE.md / technical_docs** — no change; CLAUDE.md §"When you hit a `providify`
  limitation" already describes this workflow.
