# Plan 043 — Upstream providify asks: conditional registration, open-generic binding, and the P24 fix (DI-15)

Covers BACKLOG 4.0 row **`DI-15`** (🔴 must, S — *Upstream providify asks*). Gates `DI-3`/`DI-4`/
`DI-5`/`DI-9`. The BACKLOG's locked decision (`BACKLOG.md:55`): *missing providify features are
filed upstream, never worked around* — so this plan ships **no production code** in any `varco_*`
package. Its deliverables are two new upstream-gap reports, one sharpened existing report, three
`strict=True` xfail guards (two new, one re-confirmed), three ledger rows, and the documentation
that records the answer to BACKLOG open question 2.

**Research briefs backing this plan:**
[`016-open-generic-binding-in-di-containers.md`](../design/research/016-open-generic-binding-in-di-containers.md)
(the parametric-generic ask's external grounding — cited by § below) and
[`014-multi-binding-qualifiers-and-conditional-di.md`](../design/research/014-multi-binding-qualifiers-and-conditional-di.md)
(conditional-registration prior art: Micronaut `@Requires` `014:64,90-92`, Spring
`@ConditionalOnProperty` `014:78`, Quarkus `selected-alternatives` `014:58-59`). ⚠️ 014's claim
that providify has `@Requires` (`014:187-188,204,238`) is **wrong** and already flagged at
`BACKLOG.md:29-31`; this plan adds an in-file banner (Step 8) so the brief cannot mislead a future
reader who does not open the BACKLOG.

**providify source of record**: `/home/edoardo/projects/providify`, version **2.0.1**, HEAD
**`1402f0a`**. Every `providify/...` citation below was read at that revision while writing this
plan. The installed copy resolved by `uv.lock` is also 2.0.1 (Step 1 re-confirms both).

> ⚠️ **Two premises in the row/brief are corrected here, and both reshape a step.**
> (1) Research 014 **does** have an Evidence Gap 6 (`014:220`) — the task premise that it stops at
> Gap 5 is wrong. What is wrong is Gap 6's *content*: it says "None of the 6 frameworks natively
> support" open-generic binding, which brief 016 §1 and §5 contradict (.NET and Castle Windsor
> do, for implementation types). See §D-gap6. (2) The P24 wiring loop lives in
> `_register_module_providers` (`providify/container.py:6160`), not literally in `install()` —
> the existing report's citation `6201-6214` is still exact; only the enclosing-function name in
> its prose needs sharpening. See §D-C.

## Goal

Three providify gaps exist as durable, source-cited, maintainer-actionable reports under
`design/upstream-gaps/`, each with a Docker-free `strict=True` xfail that turns red the moment
providify ships the feature, each indexed by one `UPSTREAM-GAPS.md` row — and the four downstream
DI rows (`DI-3`/`DI-4`/`DI-5`/`DI-9`) can each be planned against a written statement of what
providify does today, what was asked for, and (for `DI-4`) whether it may proceed before the ask
lands.

## Non-goals

- **Any change to providify itself.** The user owns providify; this plan produces the *asks*. The
  implementer must not edit `/home/edoardo/projects/providify`.
- **Any varco shim, helper, or translation layer** for the two missing features — `BACKLOG.md:55`
  forbids it. The one thing this plan *does* decide is whether `DI-4` may use a **shipped**
  providify API (`activate_profile()`) as an interim path (§D-OQ2); that is a use of public API,
  not a shim, and the decision is recorded for the `DI-4` planner to confirm, not implemented here.
- **Collapsing `bind_repositories`/`bind_clients`/the beanie loop into one helper.** That is `DI-2`'s
  territory and, per brief 016 §3, the per-type factory loop *is* the Spring Data factory-bean
  precedent — a legitimate pattern, not a defect to hide.
- **A second P24 report.** `design/upstream-gaps/providify-disposes-first-match.md` is amended in
  place (§D-C).
- **Fixing the two missing features' *symptoms* in varco** (e.g. rewriting
  `RedisEventBusSelectorConfiguration` onto `@Profile`). Named in the reports' "partly ours"
  sections as options; scheduled by `DI-4`, not here.
- **README.md / CLAUDE.md content changes.** CLAUDE.md §"When you hit a `providify` limitation"
  already states the process this plan follows, and no user-facing behaviour changes. If the
  implementer finds a needed change, it is a one-line pointer at most (§Docs).

## Context

### What providify 2.0.1 offers today (verified)

| Primitive | Where | What it does |
|---|---|---|
| `@Profile(*exprs)` | `providify/decorator/scope.py:773-789` (validation + marker stamping); `providify/profiles.py:59-` (`matches()`, OR across expressions, `"!"` negation) | Declarative activation gate on a binding |
| Active profile set | `DIContainer(profiles=...)` → `resolve_active_profiles()` (`profiles.py:147`; env var `PROVIDIFY_PROFILES`, `profiles.py:24`) stored at `container.py:767`; mutable later via `activate_profile()`/`deactivate_profile()` (`container.py:1836-1875`, `:1877`) | The only configuration-shaped input to binding activation |
| `@Alternative` / `@Default` | `providify/decorator/scope.py:647` (`Default`), `:669` (`Alternative`); `providify/metadata.py:100` (`AlternativeMarker`) | Excluded by default; activated per container by `enable_alternative(cls)` (`container.py:1784`) / `disable_alternative` (`:1796`), **or** by a matching `@Profile` (`:1672-1675`) |
| Activation evaluation | `_binding_is_active()` `container.py:1606-1676`, called from `_filter()` `:1714-1733` on **every** lookup | **Lazy, resolve-time** — not scan-time. `profile_ok AND alternative_ok`, nothing else |
| `validate()` | `container.py:5249`; `validate_bindings` `:5119`; `validate_all` `:5171` | Pass 1 iterates `self._bindings` **unfiltered**, "regardless of `@Profile` activation" (`:5327-5333`, its own docstring) |
| Generic-alias binding | `container.provide(fn, *, returns=)` `container.py:1048-1077`; `_normalize_explicit_interface` `providify/binding.py:432-491` — a parameterised alias (`get_origin(v) is not None`, `:460-461`) is stored **verbatim** as `binding.interface` | `returns=Repository[User]` is the documented, supported spelling (`container.py:1062-1068`) |
| Generic-alias matching | `_interface_matches()` `providify/utils.py:130-196`; both-generic case `:174-180` — `issubclass(bind_origin, req_origin)` **and** `get_args(binding) == get_args(requested)` exactly | Docstring `utils.py:157-158`: *"requested is generic with TypeVar args → literal comparison; will not match concrete parameterisations ⚠️"* — the open-generic gap is **documented as a known non-feature** |
| Conditional predicate | — | **Absent.** `rg -in 'Requires|ConditionalOn|condition=' providify/` returns only prose hits (docstrings using the English word "requires") — zero decorators, zero markers, zero container methods |
| `@Disposes` wiring | `container.py:6201-6214` inside `_register_module_providers` (`:6160`), called from the async install path at `:6157` | Walks **all** `self._bindings`, attaches to the **first** `ProviderBinding` whose interface matches, `break`s — the P24 defect, unchanged at `1402f0a` |

### Where varco pays for each gap today (evidence for the reports' "why it is a gap")

**Conditional registration** — selection is done *inside provider bodies* or by *imperative
`enable_*` calls*, never at the binding level:
- `varco_redis/varco_redis/bus.py:516-537` — `RedisEventBusSelectorConfiguration.bus()` reads
  `settings.use_streams` (`VARCO_REDIS_USE_STREAMS`, `varco_redis/varco_redis/di.py:21-26`) and
  returns one of two classes from one provider. Both implementations are reachable only through
  this `if`; neither is a binding in its own right, so `describe()`/`validate()` cannot see the
  choice.
- `varco_core/varco_core/auth/di.py:49-100` `enable_authorization_audit()`;
  `varco_core/varco_core/tenancy/di.py:77-99` `enable_tenant_membership()`; `enable_feature_flags`,
  `enable_policy_authorizer`, `enable_token_revocation` — five imperative opt-ins that a
  property-driven condition would replace with one declaration each.
- `priority=-sys.maxsize-1` framework defaults — scout counted **15**; ⚠️ Step 5 re-counts with
  `rg -n 'priority=-sys.maxsize' varco_*/varco_*` and cites the number it finds, not the scout's.

**Open-generic binding** — three per-type registration loops of identical shape:
- `varco_sa/varco_sa/di.py:292-393` `bind_repositories()` → `_bind_repo_provider()`, per-entity
  closure + `container.provide(Provider(singleton=False)(_repo_factory), returns=AsyncRepository[entity_cls])`
  (`:389-392`); its own DESIGN block (`:307-312`) records the linear-scaling trade-off.
- `varco_beanie/varco_beanie/di.py:103-202` — same shape, async, `Provider(returns=AsyncRepository[entity_cls])` at `:201`.
- `varco_fastapi/varco_fastapi/di.py:108-196` `bind_clients()` — `returns=VarcoClient[router_type]` at `:196`.

**P24** — `RedisCacheConfiguration` + `RedisLayeredCacheConfiguration` both bind `CacheBackend`;
guard at `varco_redis/tests/test_redis_cache_disposes.py:91-103` (strict xfail, monkeypatched
I/O, no Docker).

## Design

### §D-shape — three deliverable groups, two of them independent

```
 (A) conditional registration        (B) open-generic binding         (C) P24 sharpening
 ─────────────────────────────       ─────────────────────────        ──────────────────
 report  providify-conditional-      report  providify-open-          amend  providify-disposes-
         registration.md                     generic-binding.md              first-match.md §1/§4
 guard   test_providify_upstream_    guard   test_providify_upstream_ guard  (existing) re-run,
         gaps.py::TestP25*                   gaps.py::TestP26*               confirm still XFAIL
 ledger  row P25-CONDITIONAL-        ledger  row P26-OPEN-GENERIC-    ledger row P24 amended
         REGISTRATION                        BINDING                        (HEAD hash, "gates DI-9")
 answers BACKLOG OQ2 (→ DI-4)        unblocks DI-5's shape question   gates DI-9
```

**A and B are independent and may be done in either order.** C is independent of both. Only the
ledger/BACKLOG/CHANGELOG steps (7-9) depend on all three.

⚠️ **Gap IDs `P25`/`P26` are an ASSUMPTION** — the ledger shows P22 (closed) and P24 (open); P23's
fate is unknown to this plan. Step 1 confirms with `ls design/upstream-gaps/` and
`rg -n 'P2[0-9]-' design/upstream-gaps/ UPSTREAM-GAPS.md CHANGELOG.md`; if P25/P26 are taken, take
the next two free numbers and substitute throughout.

### §D-guards — one file, `varco_core/tests/test_providify_upstream_gaps.py`, one class per gap

**Chosen: a single new file in `varco_core/tests/`, one `TestP25ConditionalRegistration` /
`TestP26OpenGenericBinding` class each, using a local minimal generic — not `AsyncRepository`.**

- ✅ `varco_core/tests/` runs in `make test PKG=varco_core` and in CI's `unit` job with **no
  backend, no Docker**, meeting UPSTREAM-GAPS.md:47-49's "fast, dependency-free" rule. Neither
  gap needs anything beyond `providify` itself.
- ✅ One file whose module docstring is the map: gap ID → report file → guard class. It mirrors the
  ledger's index shape, so `rg -n 'P2[0-9]-' varco_*/tests/` lists every live providify guard, the
  same liveness discipline Plan 042 §D-register adopted for `KI-N`.
- ✅ A local `class Repo(Generic[T])` in the test (not `varco_core.repository.AsyncRepository`)
  keeps the reproduction **pasteable into providify's own test suite unchanged** — the report's
  repro and the guard are the same code. The docstring cross-references the three real varco
  sites so the maintainer sees the production shape.
- ❌ The P24 guard stays in `varco_redis/tests/test_redis_cache_disposes.py` — it needs
  `RedisCache`/`LayeredCache` (monkeypatched) to be a faithful repro. The new file's docstring
  says so. Accepted: moving it would weaken the repro to gain uniformity.
- ❌ Two gaps in one file means the file is edited twice as upstream fixes land. Accepted — each
  fix deletes one class; the file goes with the last one.

**Alternative — one file per gap** (`test_providify_conditional_registration.py`,
`test_providify_open_generic.py`): ✅ each file dies whole when its gap closes; ❌ two more
top-level test files for two ~40-line classes, and no single place lists the providify guards.
Rejected for the index property.

**Alternative — use `AsyncRepository[T]` directly**: ✅ byte-identical to production; ❌
`AsyncRepository`'s parameter list is not one `T` (`varco_sa/di.py:376` annotates it
`AsyncRepository[Any, Any]`, `:391` needs `# type: ignore[valid-type, misc]`), which drags mypy
noise and a varco import into what must be a providify-only repro. Rejected.

### §D-failmode — each guard must fail today *for the right reason*, and say so

A `strict=True` xfail is only a guard if the thing it is waiting for is the thing it asserts.
Both new guards therefore carry `raises=` so that a **different** failure is reported as a real
red, not silently absorbed as "still xfail":

| Guard | Spelling used today | Expected failure today | `raises=` | What flips it |
|---|---|---|---|---|
| P26 matching | `container.provide(factory, returns=Repo[T])` then `container.get(Repo[User])` | `LookupError` from `get()` — registration **succeeds** (`binding.py:460-461` accepts any parameterised alias, a TypeVar arg included), lookup **misses** because `utils.py:180` compares `(T,) == (User,)` | `LookupError` | providify treating a TypeVar type-arg as a wildcard in `_interface_matches` |
| P26 type-arg delivery | same, factory declares `entity: type[T]` and the test asserts `repo.entity is User` | same `LookupError` (filter runs before any instantiation — `_filter()` `:1714`) | `LookupError` | matching **and** the factory receiving the closed type. If upstream ships matching only, this test fails with something other than `LookupError` → loud, "revisit" |
| P25 | `from providify import Requires` (module scope inside the test body, not at import time) | `ImportError` | `ImportError` | providify exporting `Requires`. ⚠️ If it ships the feature under a **different name**, this guard stays silently xfail — the report names the contract (§D-A), and Step 1's `rg` is re-run at every providify bump (Plan 016's upgrade-checklist precedent) |

The task's condition for an `ImportError`-based xfail is met explicitly: the P25 test docstring
states the expected API name (`providify.Requires`), the expected evaluation semantics, and that
`design/upstream-gaps/providify-conditional-registration.md` **is the contract** — the guard only
detects that *something* named `Requires` arrived; the report says what it must do.

**Procedure the implementer must follow for each guard (and record in its docstring):** write the
test **without** the marker first, run it, paste the observed exception type and the providify
`file:line` that raised it into the docstring, *then* add the marker. A guard whose docstring
says "fails today with X at Y" is verifiable; one that says "fails today" is not.

### §D-A — the conditional-registration ask

**What is missing, precisely.** providify's only activation inputs are a *profile name set*
(`_active_profiles`, `container.py:767`) and an *enabled-alternatives set* (`:762`). Neither is a
predicate over configuration. A profile is a free-form token whose only sources are
`PROVIDIFY_PROFILES` (`profiles.py:24`) — providify's env namespace, not varco's — and code.
Micronaut `@Requires(property="varco.cache.backend", value="redis")` (`014:64,90`) and Spring
`@ConditionalOnProperty` (`014:78`) evaluate the application's own property directly.

**Is a profile the same thing as a config property? No.** Mapping `VARCO_CACHE_BACKEND=redis` onto
`@Profile("cache-redis")` requires varco code that reads the setting and calls
`container.activate_profile("cache-redis")` (`:1836`) — a translation whose profile names are a
second, undocumented vocabulary. That translation *is* a use of supported API (§D-OQ2), but it
cannot express *predicates*: "active when `redis` is importable **and** `VARCO_CACHE_BACKEND=redis`",
or "active when `settings.enabled` is true" for any of the five `enable_*` sites. The report's
repro shows the `bus.py:516-537` shape — two implementations hidden behind one `if` inside a
provider — and states that `describe()`/`validate()` cannot see either branch.

**The ask, with candidate fixes for the maintainer** (the report's §4; each ✅/❌ must be
reproduced there):

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) `@Requires(condition=Callable[[], bool])` as a third conjunct in `_binding_is_active()`** (`:1665-1676`), evaluated **lazily at resolve time** like `@Profile` today | Consistent with the existing evaluation point; zero new container state; a callable can close over any settings object; composes with `@Profile`/`@Alternative` by AND | Evaluated on every `_filter()` call — needs a per-binding memo (a `functools.cache` on the marker, or a `frozenset` of settled bindings) or a cheap predicate contract; `validate()` pass 1 is unfiltered (`:5327-5333`) so an inactive conditional binding is still validated — decide whether that is desired (⚠️ it *is* desirable for `DI-7`'s "what would be active under other settings" report) |
| **(2) `@Requires(env="VARCO_CACHE_BACKEND", value="redis")` sugar** over (1) | The Micronaut/Spring spelling operators recognise; greppable; the 80% case in varco is a single env var | Only env — varco settings are pydantic `BaseSettings` with code>env>default precedence (`DI-6`), so an env-only check can disagree with the settings object. **Ask for it only as sugar over (1), never as the sole form** |
| **(3) Scan-time / install-time evaluation** — drop the binding entirely if the condition is false | The container's `describe()` shows only what is real; no per-lookup cost | Breaks the ordering-independence `activate_profile()` was designed for (`:1840-1846` — the graph may change after registration); a condition over a settings object that is itself DI-provided cannot run before that provider exists. ❌ **Not recommended as the primary**; acceptable as an opt-in `eager=True` |
| **(4) Interaction with `@Alternative` priority and `validate()`** — specify that a false condition removes the binding from candidate selection *before* priority/qualifier tie-break, exactly where `_binding_is_active` sits today (`:1732`), and that `validate()` reports a conditional binding's state as an informational issue kind (e.g. `IssueKind.CONDITION_INACTIVE`) rather than skipping it | `DI-7`'s wiring report gets its "why is this the live one" answer for free | One more `IssueKind`; the report should ask, not insist |

**Partly ours? Yes, partly — and the report must say so in its own section.** varco *could* today
split `RedisEventBusSelectorConfiguration` into two `@Profile`-gated bindings and translate
`VARCO_REDIS_USE_STREAMS` into `activate_profile("redis-streams")` — a supported mechanism. That
fixes the *selection* symptom for one site; it does not give `enable_*`-shaped opt-ins a
declarative form, and it introduces the profile-name vocabulary problem above. Filed as an option
for `DI-4`, not applied here.

### §D-OQ2 — BACKLOG open question 2, answered from source

> *Can providify's `@Alternative` be selected from configuration, or only from code?*

**Answer (for the implementer to verify and record in the P25 report and BACKLOG):**
`@Alternative` is selected by **code** (`enable_alternative(cls)`, `container.py:1784`) **or** by
a **`@Profile` on the alternative** (`:1672-1675` — once a profile expression is present it alone
governs, and `_enabled_alternatives` is not consulted). Profiles come from `PROVIDIFY_PROFILES`
(`profiles.py:24`, read once at construction `container.py:767`) or `activate_profile()` (`:1836`).
So: **configuration-selected, yes — but only via providify's own profile env var or a
varco-written translation into `activate_profile()`.** There is no equivalent of Quarkus
`quarkus.arc.selected-alternatives` (`014:58-59`) keyed on a class name, and no property predicate.

**Consequence for `DI-4`: not blocked.** A `VARCO_<SEAM>_BACKEND=<value>` → `activate_profile(f"<seam>-<value>")`
mapping at bootstrap uses only public API and is therefore a legitimate interim path — **provided**
`DI-4`'s plan (a) centralises it in exactly one function, (b) documents the profile vocabulary as
public, and (c) notes the two edge cases providify itself documents: a `@Profile`d singleton already
resolved under the old set is **not evicted** (`:1865-1868`), and `validate()` pass 1 ignores
activation (`:5327-5333`). The P25 ask is filed regardless, because the predicate cases
(§D-A, the five `enable_*` sites) have no profile spelling. ⚠️ ASSUMPTION: that a
public-API translation is not a "shim" under `BACKLOG.md:55`. Recorded as this plan's reading;
the `DI-4` planner confirms.

### §D-B — the open-generic-binding ask

**How a generic alias is keyed today (the compatibility constraint).** `ProviderBinding.interface`
holds the alias object verbatim (`binding.py:460-461, 471-473`). Lookup is `_filter()` →
`_interface_matches(b.interface, requested)` (`container.py:1714-1720`, `utils.py:130-196`).
For two parameterised aliases the rule is origin-`issubclass` **and** `get_args ==` exactly
(`utils.py:174-180`); the docstring at `:157-158` already names the TypeVar case as a literal
non-match. `_localns_cache` maps aliases by origin name (`container.py:2904-2918`). Nothing in
this path is hostile to a wildcard rule — the change is local to `_interface_matches` plus a
type-argument hand-off to the factory.

**Model to propose: Autofac's resolve-time factory, not .NET's implementation-type substitution.**
- Brief 016 §1: .NET registers `typeof(IRepository<>)` → `typeof(Repository<>)` and **does not
  support factory delegates** for open generics because the type argument is unknown at
  registration. Python cannot do implementation-type substitution at all — there is no runtime
  generic class instantiation with substituted parameters (016 §7, Librarian's note), and varco's
  factories genuinely need `entity_cls` as a *value* (`varco_sa/di.py:376-381`
  `provider.get_repository(entity_cls)`).
- Brief 016 §2: Autofac's delegate is *generated at resolution time* with the closed type argument
  available — exactly the shape a Python factory needs.
- Brief 016 §3/§4/§6: Spring (factory bean per interface — today's `bind_repositories`), Guice
  (deliberate absence), and every Python container (dependency-injector, Dishka, lagom, rodi,
  punq, injector — absent) — so providify would be **first in Python** and the report should say
  so; there is no precedent to copy, only .NET/Autofac to adapt.

**Proposed spelling** (the report's §4, primary candidate):

```python
T = TypeVar("T")

def repo_factory(provider: RepositoryProvider, entity: type[T]) -> AsyncRepository[T]:
    return provider.get_repository(entity)

container.provide(repo_factory, returns=AsyncRepository[T])   # open registration
container.get(AsyncRepository[User])                          # closed resolution
```

with `entity: type[T]` filled from the requested alias's `get_args()` by position of `T` in the
open alias's args. Alternative spelling to offer: a reserved marker `TypeArg[T]`/`Inject[TypeArgs]`
if positional TypeVar matching is judged too implicit.

**The semantics table the report must settle** (each row ✅/❌; cite brief 016 where marked):

| # | Semantic | Proposed | Grounding |
|---|---|---|---|
| 1 | **Key/match rule** — a TypeVar in a *binding's* alias args matches any concrete type at that position (a *requested* alias with a TypeVar keeps today's literal rule) | wildcard on the binding side only | `utils.py:157-158, 174-180`; 016 §7 (`get_origin`/`get_args`; `isinstance` on a parameterised generic is forbidden) |
| 2 | **Specificity** — a closed binding (`AsyncRepository[User]`) beats an open one (`AsyncRepository[T]`) regardless of registration order or priority | closed > open, then existing priority rules | 016 §Options table "Specificity" column — no surveyed framework documents it; varco needs it so `bind_repositories(User)` can override a generic default |
| 3 | **Type-arg delivery** — how the factory receives `X` | positional by TypeVar name, `type[T]` parameter | 016 §2 (Autofac resolve-time delegate) |
| 4 | **Singleton caching** — `singleton=True` on an open binding caches **per closed alias**, never one instance shared across all `X` | per-closed-type cache key `(binding, closed_alias)` | 016 §1 ("shares one instance across ALL closed types — a dangerous anti-pattern"); 016 Evidence Gap 5 (undocumented across frameworks — providify should document it explicitly) |
| 5 | **Enumeration** — `InjectInstances[AsyncRepository[User]]` / `get_all(AsyncRepository)` (`providify/decorator/multibinding.py:20`) | an open binding contributes to a **closed** request's enumeration once; a **bare** `AsyncRepository` request does not expand the open binding into every conceivable `X` (unbounded) | 014 §Librarian #2 (immutable collections) |
| 6 | **`validate()`** — an open binding's `type[T]` parameter is not a missing dependency; a closed request's resolvability is checked against open bindings too | new resolvability branch, no new `IssueKind` unless the bound check below fails | `container.py:5249`, `:5327-5333` |
| 7 | **`TypeVar(bound=)`/constraints** — a request whose `X` violates `bound=` is a non-match (so a second binding may serve it), not an error at resolve time; optionally an `IssueKind` in `validate()` | non-match | 016 §1 (.NET 5 constrained open generics validated at resolve); 016 §2 (Autofac #972 — nested-constraint bug, a cautionary tale for the implementation) |
| 8 | **Multiple TypeVars / partial closure** (`Mapper[S, T]` bound with `S` closed) | out of scope for the first cut; specify that only all-TypeVar or all-concrete aliases are accepted, `TypeError` otherwise | — |

**Partly ours? No.** The per-type loop is the Spring factory-bean pattern (016 §3, §Librarian #5 of
014) and works. What varco can do today is fold three loops into one helper — that is `DI-2`, an
abstraction, not a fix, and it is explicitly *not* what `DI-5`/`DI-15` asks for. The report says so
in one paragraph.

### §D-C — P24: re-verify, sharpen, do not re-file

At `1402f0a` the loop is byte-identical to the report's quote (`container.py:6201-6214`). Two
sharpenings, both in place:

1. **§1 prose**: the loop is in `_register_module_providers(module_cls, instance)` (`:6160`),
   reached from the async install path at `:6157` (⚠️ the sync `install()` path's call site must
   be cited too — the implementer finds it with `rg -n '_register_module_providers' providify/container.py`
   and records every caller). The `6201-6214` citation stays.
2. **§4 ask, made concrete**: the loop already has the information it needs, because
   `self.provide()` is called for each of this module's providers at `:6196`/`:6199` **in the
   same function**, appending to `self._bindings`. So —
   - ✅ **Preferred, minimal**: record `start = len(self._bindings)` before the provider loop
     (`:6176`) and scope the disposer search to `self._bindings[start:]`. No signature change, no
     new state, ~3 lines. `provide()` returns `None` (`:1048`), so returning the binding is a
     second, larger option (`provide() -> ProviderBinding`) that also serves `DI-13`'s
     "every `@Disposes` reachable" test — offer it as ✅ alternative.
   - ✅ **Detect, not just fix**: an `IssueKind.DISPOSER_OVERWRITTEN` warning when the wiring pass
     would replace a non-`None` `disposer` on a binding not registered by this call — sibling of
     the shipped `UNREACHABLE_PRE_DESTROY` (`:5327`) and precisely the class of finding `DI-7`'s
     report should surface.
   - ❌ Unchanged: no `@PreDestroy` semantics change (P22 is closed as intentional).
3. **Guard**: re-run `varco_redis/tests/test_redis_cache_disposes.py` and confirm the strict
   xfail is still XFAIL (not XPASS, not a different failure). No edit unless the observed reason
   drifted from the marker's text.
4. **Ledger row**: add the HEAD hash and "gates `DI-9`" to the P24 row; report header gains
   `Re-verified: 2026-09-12 against 1402f0a (Plan 043 / DI-15)`.

### §D-gap6 — research 014's Evidence Gap 6 exists and overclaims; brief 016 is the deeper source

The BACKLOG row cites "014 Evidence Gap 6". It exists (`014:220`) and the citation is **correct** —
do not "fix" the BACKLOG to point elsewhere. What needs correcting is Gap 6's claim that *"None of
the 6 frameworks natively support"* parametric binding: brief 016 §1 (.NET, since Core 1.0) and
§5 (Castle Windsor) do, for implementation types; what is absent everywhere except Autofac (016 §2)
is *factory-delegate* open generics, and what is absent across **Python** entirely is any form
(016 §6). Research briefs are records, so the fix is an **in-tree banner**, not a rewrite — the
precedent is research 002's superseded-claim banner (CLAUDE.md §Test Conventions, chaos
`restart()` paragraph). Same treatment, same step, for 014's providify-`@Requires` error
(`014:187-188,204,238`). The BACKLOG evidence cell gains "brief 016" beside "014 Evidence Gap 6".

- ✅ One step, two banners, BACKLOG citation preserved and enriched.
- ❌ Alternative — retarget the BACKLOG citation to 016 only: loses the "unsolved across the
  ecosystem" framing that is *true for Python* and is the row's actual motivation. Rejected.

### §D-changelog — an Unreleased entry, "Documentation / Tests" only

Plan 042 §D-changelog argued *no* entry for a docs-integrity plan. This plan differs in one
respect: it files upstream asks whose eventual resolution will be recorded in CHANGELOG (the P22
precedent, `CHANGELOG.md:758-763`), so the *filing* should be findable from the same file. One
short entry under `## [Unreleased]`, no behaviour change stated explicitly.

## Steps

All commands run from `/home/edoardo/projects/varco`. Steps 2-3 (B), 4-5 (A), and 6 (C) are
independent; do them in any order. Steps 7-10 come last.

### Preflight

1. [x] **Confirm the source of record and the free IDs.**
       `git -C /home/edoardo/projects/providify rev-parse --short HEAD` → expect `1402f0a`;
       `uv run python -c "import providify, importlib.metadata as m; print(m.version('providify'))"`
       → expect `2.0.1`. Confirm absence: `rg -in 'Requires|ConditionalOn|condition=' /home/edoardo/projects/providify/providify/`
       must show **no** decorator/marker/method definitions (prose hits only — paste the command
       and the hit count into both reports' §1). Confirm free IDs:
       `ls design/upstream-gaps/ && rg -n 'P2[0-9]-' UPSTREAM-GAPS.md CHANGELOG.md design/upstream-gaps/`
       — take the two lowest unused numbers ≥ 25 (§D-shape ⚠️). If HEAD or the version differ,
       **stop and re-read every cited line** before continuing — the citations below are only
       valid at `1402f0a`.
       **Verify**: the three commands' output pasted into the scratchpad
       (`/tmp/claude-1000/.../scratchpad/043-preflight.txt`) for later citation.

### Group B — open-generic binding (independent of A)

2. [x] `varco_core/tests/test_providify_upstream_gaps.py` (**new**) — module docstring = the
       gap-ID → report → class map (§D-guards), stating the P24 guard's location and why it is
       not here. `class TestP26OpenGenericBinding` with two tests, written **first without the
       marker**, run, failure recorded, *then* marked
       `@pytest.mark.xfail(strict=True, raises=LookupError, reason="GAP: P26-OPEN-GENERIC-BINDING — ... see design/upstream-gaps/providify-open-generic-binding.md")`:
       - `test_open_alias_registration_resolves_closed_request` — local `T = TypeVar("T")`,
         `class Repo(Generic[T])`, `class User`, a zero-dependency factory returning `Repo()`,
         `container.provide(factory, returns=Repo[T])` then `container.get(Repo[User])`, asserting
         `isinstance(result, Repo)`. Docstring records: registration succeeds
         (`providify/binding.py:460-461`), lookup misses at `providify/utils.py:180`
         (`get_args` exact compare), observed exception `LookupError` from `container.get()`.
       - `test_open_alias_factory_receives_closed_type_argument` — factory signature
         `def factory(entity: type[T]) -> Repo[T]`, `Repo` stores `entity`; assert
         `container.get(Repo[User]).entity is User`. Same today-failure; docstring states the
         distinct flip condition (§D-failmode row 2).
       Also add an **always-passing control** (no marker) `test_closed_alias_still_matches_exactly`
       — `returns=Repo[User]` then `get(Repo[User])` works — so the file proves the failing tests
       fail for the TypeVar and not for generics in general.
       **Verify**: `uv run pytest varco_core/tests/test_providify_upstream_gaps.py -q -k P26` →
       `2 xfailed, 1 passed`. Then temporarily remove one marker and re-run → the test fails with
       `LookupError` (paste the traceback's providify frame into the docstring); restore the marker.

3. [x] `design/upstream-gaps/providify-open-generic-binding.md` (**new**) — header block in the
       P24 file's shape (`ID`, `Raised by: varco, Plan 043 / DI-15`, `Date`, `providify version:
       2.0.1 (1402f0a)`, `Status: ⛔ Open — filed, not worked around`, `Priority: 🟡 should —
       gates DI-5's shape; three per-type loops today`). Sections: **§1 What providify does today**
       (the keying/matching table from §Context with every `file:line`, and the `utils.py:157-158`
       docstring quoted verbatim — it is upstream's own acknowledgement); **§2 Why it is a gap**
       (the three varco loops with line ranges, `varco_sa/di.py:307-312`'s DESIGN block quoted);
       **§3 Minimal reproduction** (the Step 2 test bodies, verbatim, providify-only); **§4 The
       ask** (§D-B's spelling, the Autofac-not-.NET argument citing 016 §1/§2/§7, and the
       8-row semantics table with ✅/❌ per row); **§5 Partly ours?** (§D-B's "No" paragraph, naming
       `DI-2` as where the loops fold); **§6 Interim workaround** (none applied; the loops stay);
       **§7 Guard** (the two test IDs). Cite 016 §3/§4/§6 for "no Python precedent; providify would
       be first".
       **Verify**: `rg -c 'providify/' design/upstream-gaps/providify-open-generic-binding.md` ≥ 8
       (every claim carries a citation); every `016 §N` referenced exists in the brief.

### Group A — conditional registration (independent of B)

4. [x] `varco_core/tests/test_providify_upstream_gaps.py` — add `class TestP25ConditionalRegistration`
       with `test_requires_decorator_is_exported`, marked
       `@pytest.mark.xfail(strict=True, raises=ImportError, reason="GAP: P25-CONDITIONAL-REGISTRATION — providify has @Profile/@Alternative only; see design/upstream-gaps/providify-conditional-registration.md")`.
       Body: `from providify import Requires  # noqa: F401` inside the function, then a
       **semantic assertion that documents the contract** even though it is unreachable today:
       decorate two `@Singleton` implementations of a local ABC with
       `@Requires(condition=lambda: flag["on"])` / `@Requires(condition=lambda: not flag["on"])`,
       `container.scan`-free (`container.register` / `provide` them directly), assert
       `container.get(Base)` returns the one whose condition is true, flip `flag`, assert the
       other. Docstring: expected API name `providify.Requires`, expected evaluation point (a third
       conjunct in `_binding_is_active`, `container.py:1665-1676`), and *"the report is the
       contract — this guard detects only that the name arrived"*. Add an **always-passing
       control** (no marker) `test_profile_is_the_only_declarative_switch_today` that proves the
       shipped mechanism: `@Profile("x")` binding invisible under `DIContainer(profiles=())`,
       visible after `container.activate_profile("x")` — citing `:1836-1875`. This control is
       what §D-OQ2 rests on; it must pass at 2.0.1.
       **Verify**: `uv run pytest varco_core/tests/test_providify_upstream_gaps.py -q -k P25` →
       `1 xfailed, 1 passed`. Remove the marker, re-run → `ImportError` (record it); restore.

5. [x] `design/upstream-gaps/providify-conditional-registration.md` (**new**) — same header
       shape; `Priority: 🔴 must — gates DI-4's design and DI-3's default model`. **§1 What
       providify does today**: the `@Profile`/`@Alternative`/`_binding_is_active`/`activate_profile`/
       `validate()` rows from §Context with `file:line`, the Step 1 `rg` command and its
       hit-count as the absence proof, and the `_filter()` activation truth table quoted from
       `container.py:1691-1698`. **§2 Why it is a gap**: "profile ≠ property" (§D-A), the
       `bus.py:516-537` selector, the five `enable_*` sites (`auth/di.py:49-100`,
       `tenancy/di.py:77-99`, plus the three others by `rg -n 'def enable_' varco_*/varco_*`), and
       the re-counted `priority=-sys.maxsize-1` sites. Cite 014 §Micronaut (`:64,90-92`), §Spring
       (`:78`), §Quarkus (`:58-59`) for prior art, and note 014's providify-`@Requires` error
       (`:187-188,204,238`) is flagged at `BACKLOG.md:29-31` and by Step 8's banner. **§3 Minimal
       reproduction**: the Step 4 test body plus a 15-line providify-only version of the
       `bus.py` selector. **§4 The ask**: §D-A's four-row candidate table with ✅/❌, recommending
       (1) with (2) as sugar, (3) as opt-in only, and (4) specified. **§5 Answer to BACKLOG open
       question 2**: §D-OQ2 verbatim, with the Step 4 control test as its proof. **§6 Partly
       ours?**: yes, partly — the `@Profile` split of the Redis selector is available today and is
       `DI-4`'s call. **§7 Interim workaround**: none applied. **§8 Guard**.
       **Verify**: `rg -c 'container.py:|profiles.py:|scope.py:' design/upstream-gaps/providify-conditional-registration.md` ≥ 10;
       §5 states "not blocked" or "blocked" for `DI-4` in its first sentence.

### Group C — P24 re-verification and sharpening

6. [x] `design/upstream-gaps/providify-disposes-first-match.md` — (a) header: add
       `**Re-verified**: 2026-09-12 against providify 2.0.1 @ 1402f0a (Plan 043 / DI-15) — loop
       unchanged` and `**Gates**: BACKLOG DI-9`; (b) §1: name the enclosing function
       `_register_module_providers` (`container.py:6160`) and list every caller found by
       `rg -n '_register_module_providers' /home/edoardo/projects/providify/providify/container.py`
       (the async path at `:6157` is one; cite the sync one); (c) §4: replace the "Preferred"
       bullet with §D-C's concrete `start = len(self._bindings)` slice proposal (cite `:6176`,
       `:6196`, `:6199`, `provide()` returning `None` at `:1048`), add the `provide() -> ProviderBinding`
       alternative and the `IssueKind.DISPOSER_OVERWRITTEN` detector beside the shipped
       `UNREACHABLE_PRE_DESTROY` (`:5327-5333`). Do **not** touch §2/§3/§5. Then run the guard.
       **Verify**: `uv run pytest varco_redis/tests/test_redis_cache_disposes.py -q` → the
       first-match test reports `xfailed` (not `xpassed`, not `failed`); if anything else, stop —
       the gap changed and the report needs a §3-style update before this plan proceeds.

### Index, backlog, changelog

7. [x] `UPSTREAM-GAPS.md` — two new rows in "Open gaps" (`P25-CONDITIONAL-REGISTRATION`,
       `P26-OPEN-GENERIC-BINDING`), each in the P24 row's shape: ID, `providify 2.0.1 (1402f0a)`,
       a one-sentence gap with the single most load-bearing `file:line`, the report link, the
       guard's `file::Class::test` ID. Amend the P24 row: append `(1402f0a)` to the version and
       `— gates DI-9` to the gap text. Update the trailing `**Last updated:**` line
       (`2026-09-12 · Plan 043 / DI-15 opened P25, P26; P24 re-verified`).
       **Verify**: `rg -c '^\| \*\*P2' UPSTREAM-GAPS.md` → `3`; every report path in the table
       resolves (`ls` each).

8. [x] `design/research/014-multi-binding-qualifiers-and-conditional-di.md` — two in-tree
       banners, no rewrite (§D-gap6): (a) directly above `## Evidence Gaps` item 6 (`:220`), a
       `> ⚠️ **Superseded in part (Plan 043):** …` block stating that .NET (016 §1) and Castle
       Windsor (016 §5) do support open-generic *implementation-type* registration, that
       factory-delegate open generics exist only in Autofac (016 §2), and that the "unsolved"
       verdict holds for **Python** (016 §6) — with a link to brief 016; (b) at the top of the
       file, a `> ⚠️` banner that every mention of providify `@Requires` (`:187-188,204,238`) is
       wrong — providify 2.0.1 ships `@Profile`/`@Alternative` only, see
       `design/upstream-gaps/providify-conditional-registration.md`. Then `BACKLOG.md:67`
       (`DI-15` row): status `todo` → `done (Plan 043)`, evidence cell `014 Evidence Gap 6
       ("unsolved across the ecosystem"); brief 016; UPSTREAM-GAPS.md`. `BACKLOG.md:129-131`
       (open question 2): append `**Answered (Plan 043 / P25 §5):** code or @Profile only;
       DI-4 not blocked via a public-API activate_profile() translation — see the report.`
       `BACKLOG.md:93` (`DI-4` row): change "Blocked on `DI-15` if …" to "`DI-15` answered:
       not blocked (profile translation is public API); the `@Requires` ask is filed as P25".
       **Verify**: `rg -n 'Plan 043' BACKLOG.md design/research/014-*.md` → ≥ 4 hits.

9. [x] `CHANGELOG.md` `## [Unreleased]` — one `### Documentation / Tests (Plan 043, DI-15)`
       block: three bullets (P25 filed, P26 filed, P24 re-verified and sharpened), each naming
       the report file and the guard, ending with *"No runtime behaviour changes; no public API
       changes."* Mirror the P22 closure bullet's register (`CHANGELOG.md:758-763`).
       **Verify**: `rg -n 'P25-CONDITIONAL-REGISTRATION|P26-OPEN-GENERIC-BINDING' CHANGELOG.md` → 2 hits.

10. [x] **Full verification** — `make test PKG=varco_core` and `make test PKG=varco_redis` green
        (three `xfailed`, zero `xpassed`); `make lint` (no-`PKG` path, so `api-check` runs — it
        must report **no change**: this plan touches no `__all__`); `make type-check` clean (the
        test file is under `strict = true` — `Repo(Generic[T])` and the `type[T]` parameter must
        type-check; the `from providify import Requires` line needs
        `# type: ignore[attr-defined]` with a comment pointing at the gap ID);
        `rg -n 'GAP: P2[0-9]-|BUG: ' varco_*/tests/` lists exactly the three providify guards
        plus Plan 042's `KI-N` markers, each naming a ledger/register row.

## Edge cases

- **providify at a different HEAD/version than `1402f0a`/2.0.1** → Step 1 stops the plan; every
  `file:line` must be re-read. Do not proceed on the assumption that "the lines moved a little".
- **`container.get(Repo[T-alias])` raises something other than `LookupError`** (e.g. providify's
  own subclass) → `raises=` must name the observed class; the docstring cites where it is raised.
- **`provide(..., returns=Repo[T])` is rejected at registration** (a `TypeError` from
  `_normalize_explicit_interface` at `binding.py:492+`, not read by this plan) → the guard then
  fails for the *wrong* reason. Switch the spelling to `Provider(returns=lambda: Repo[T])` or
  `returns=Repo` + a `type[T]` parameter, record which, and make the report's §4 propose the
  spelling that *does* register today. ⚠️ ASSUMPTION that registration succeeds — Step 2 verifies.
- **The P24 guard XPASSes** → providify fixed it between 2.0.1 and now, or varco's redis
  configurations changed. Do not delete the marker blindly: confirm the loop at `:6201-6214`
  changed, then follow the P22 closure procedure (report §8, ledger "Recently closed", CHANGELOG).
- **P25/P26 IDs already taken** → substitute the next free numbers in every file, including the
  test `reason=` strings and the module docstring map.
- **`make lint`'s `api-check` reports a change** → this plan cannot cause one; something else is
  dirty on the branch. Do not regenerate the snapshot from this plan.
- **A `@Profile` control test (Step 4) fails at 2.0.1** → §D-OQ2's answer is wrong; stop, re-read
  `container.py:1606-1676` and `:1836-1875`, and rewrite §5 of the P25 report before Step 8.

## Verification

```bash
# Preflight
git -C /home/edoardo/projects/providify rev-parse --short HEAD              # 1402f0a
uv run python -c "import importlib.metadata as m; print(m.version('providify'))"  # 2.0.1
rg -in 'Requires|ConditionalOn|condition=' /home/edoardo/projects/providify/providify/  # prose only

# Guards
uv run pytest varco_core/tests/test_providify_upstream_gaps.py -q -rxX     # 3 xfailed, 2 passed, 0 xpassed
uv run pytest varco_redis/tests/test_redis_cache_disposes.py -q -rxX       # P24 still xfailed

# Whole packages + gates
make test PKG=varco_core
make test PKG=varco_redis
make lint            # includes api-check + asyncapi-check + import-budget (warn-only); all unchanged
make type-check

# Liveness cross-check (every marker names a row)
rg -n 'GAP: P2[0-9]-' varco_*/tests/
rg -n '^\| \*\*P2' UPSTREAM-GAPS.md
```

## Risks

| Risk | Invariant that must hold | Mitigation |
|---|---|---|
| ⚠️ ASSUMPTION — `P25`/`P26` are the next free gap IDs | Every ID is unique across `UPSTREAM-GAPS.md`, `design/upstream-gaps/`, `CHANGELOG.md` | Step 1 `ls`/`rg`; substitute if taken |
| ⚠️ ASSUMPTION — `provide(fn, returns=Repo[T])` registers without error at 2.0.1 (`binding.py:460-461` read; `:492+` not read) | The P26 guard fails at `container.get()` with `LookupError`, not earlier | Step 2's marker-off run; Edge cases row 3 gives the fallback spelling |
| ⚠️ ASSUMPTION — `container.get()` raises plain `LookupError` for a no-match (docstring `container.py:1757-1760`) | `raises=LookupError` accepts the observed exception | Step 2 records the observed class; adjust `raises=` if it is a subclass with another name |
| ⚠️ ASSUMPTION — the sync `install()` path also reaches `_register_module_providers` (only the async caller at `:6157` was read) | P24's report names every caller | Step 6's `rg` |
| The P25 guard is name-based: providify could ship the feature under another name and the guard would stay xfail | A providify version bump re-runs Step 1's `rg` (Plan 016 upgrade-checklist precedent) | Report §4 names `Requires` as the *proposed* name and says any name is acceptable if semantics match; the guard docstring says the report is the contract |
| ⚠️ ASSUMPTION — a public-API `activate_profile()` translation is not a "shim" under `BACKLOG.md:55` | `DI-4` is planned against a recorded reading, not an implicit one | §D-OQ2 recorded in the P25 report §5 and BACKLOG OQ2; the `DI-4` planner confirms or overturns it in writing |
| Research-brief edits: 014 is a record and must not be rewritten | Only prepended `> ⚠️` banners; original text untouched | Step 8; precedent research 002's banner |
| `make type-check` under `strict = true` on a test that imports a non-existent name | The file type-checks with one targeted `# type: ignore[attr-defined]` | Step 10; M1 (`type: ignore` count) rises by exactly one and is directional only, not a gate |
| The reports over-specify and read as demands rather than options | Every ask row carries ✅ **and** ❌; §4 recommends, never insists | Review each report's §4 against §D-A / §D-B tables before Step 7 |
| Scout counts (15 default sites, three loops) drift | Reports cite numbers the implementer measured | Steps 3 and 5 re-run the `rg` and cite their own output |

## Docs / backlog updates

- `UPSTREAM-GAPS.md` — Step 7 (two rows added, one amended, footer date).
- `BACKLOG.md` — Step 8 (`DI-15` → done; `DI-4` rationale; OQ2 answered).
- `design/research/014-…md` — Step 8 (two banners).
- `CHANGELOG.md` — Step 9 (Unreleased, docs/tests only).
- `README.md` — **no change**: no user-facing behaviour or API changes.
- `CLAUDE.md` — **no change**: §"When you hit a `providify` limitation or bug" already describes
  this exact workflow, and the file's own rules forbid restating what UPSTREAM-GAPS.md carries.
  If the implementer believes a pointer is warranted, it is at most one clause in that section's
  existing "Worked example" parenthetical — never a new paragraph.
- `technical_docs/` — **no change**.
- `design/api-freeze-and-standards/measurements/api-surface.json` — **must not change** (Step 10).

## Done criteria

- [x] Two new report files exist under `design/upstream-gaps/`, each with §1 (source-cited,
      `file:line` at `1402f0a`), §2, §3 (providify-only repro identical to the guard), §4 (✅/❌
      per candidate), a "partly ours" section, an interim-workaround section, and a guard section.
- [x] The P25 report answers BACKLOG open question 2 in its own section, with the Step 4
      `@Profile` control test as proof, and states plainly whether `DI-4` is blocked.
- [x] The P26 report cites brief 016 for the Autofac-over-.NET choice and settles all eight
      semantics rows.
- [x] `providify-disposes-first-match.md` carries the re-verification header, the enclosing
      function and all callers, and the concrete slice-based fix proposal.
- [x] `varco_core/tests/test_providify_upstream_gaps.py` exists: three `strict=True` xfails with
      `raises=`, two passing controls, every docstring recording the observed failure and its
      providify `file:line`; runs in `make test PKG=varco_core` with no Docker.
- [x] The P24 guard still reports `xfailed`.
- [x] `UPSTREAM-GAPS.md` has exactly three open rows; each guard `reason=` names its row's ID.
- [x] BACKLOG `DI-15` is `done`, `DI-4`'s "blocked on" text is resolved, OQ2 is answered; 014
      carries both banners; CHANGELOG has the Unreleased entry.
- [x] `make test PKG=varco_core`, `make test PKG=varco_redis`, `make lint`, `make type-check`
      all green; `api-surface.json` unchanged.
- [x] No file under `/home/edoardo/projects/providify` was modified, and no `varco_*/varco_*`
      production file was modified.
