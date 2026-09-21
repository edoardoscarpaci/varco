# Conditional registration: no predicate-over-configuration primitive

**ID**: P25-CONDITIONAL-REGISTRATION
**Raised by**: varco, Plan 043 / DI-15
**Date**: 2026-09-12
**providify version**: 2.0.1 (`1402f0a`)
**Status**: ✅ Resolved (2026-09-14, Plan 045 / DI-15 close-out) — see §9
**Priority**: 🔴 must — gates `DI-4`'s design and `DI-3`'s default model

---

## 1. What providify does today

providify's only two activation inputs, both consulted from `_binding_is_active()`
(`providify/container.py:1606-1676`, called from `_filter()` on **every** lookup —
`providify/container.py:1691-1698`'s own truth table, quoted verbatim):

```
| condition                                           | included? |
|------------------------------------------------------|-----------|
| unprofiled, not @Alternative                          | always    |
| unprofiled @Alternative, not enabled                  | no        |
| unprofiled @Alternative, enabled                      | yes       |
| @Profile matches active set                           | yes       |
| @Profile does not match active set                    | no        |
| @Profile matches + @Alternative (any enable state)    | yes       |
```

- **`@Profile(*exprs)`** (`providify/decorator/scope.py:773-789` for validation and marker
  stamping) is a free-form token set, OR-across-expressions, `"!"`-negatable
  (`providify/profiles.py:59-` `matches()`). The active set comes from
  `PROVIDIFY_PROFILES` (`providify/profiles.py:24`, read once at `DIContainer.__init__`
  and stored at `providify/container.py:767`) or mutated later with
  `activate_profile()`/`deactivate_profile()` (`providify/container.py:1836-1875`,
  `:1877`).
- **`@Alternative`** (`providify/decorator/scope.py:669` decorator,
  `providify/metadata.py:100` `AlternativeMarker`) is excluded by default; activated
  per-container via `enable_alternative(cls)`/`disable_alternative(cls)`
  (`providify/container.py:1784`/`:1796`), **or** overridden by a matching `@Profile` on the
  same class — once `b.profiles` is non-empty, `_enabled_alternatives` is not consulted at all
  (`providify/container.py:1672-1675`).
- **`validate()`** (`providify/container.py:5249`; `validate_bindings`
  `providify/container.py:5119`; `validate_all` `providify/container.py:5171`) runs its first
  pass over `self._bindings` **unfiltered**, "regardless of `@Profile` activation" — its own
  docstring, `providify/container.py:5327-5333`.

**Absence, confirmed by source grep, not by documentation** (per this repo's U-8 lesson):

```bash
rg -in 'Requires|ConditionalOn|condition=' /home/edoardo/projects/providify/providify/
```

22 hits, all prose — the English word "requires" inside docstrings/error messages/comments
(e.g. `decorator/scope.py:774`'s `ValueError("@Profile requires at least one profile
expression.")`, several `# ... requires a running event loop` comments). **Zero** decorator
definitions, zero markers, zero container methods named `Requires`/`ConditionalOn`/anything
accepting a `condition=` keyword. There is no predicate-over-configuration primitive in
providify 2.0.1 under any name.

## 2. Why it is a gap

**A profile is not a config property.** Micronaut's `@Requires(property="...", value="...")`
(research brief [`014-multi-binding-qualifiers-and-conditional-di.md`](../research/014-multi-binding-qualifiers-and-conditional-di.md)
`:64,90-92`) and Spring's `@ConditionalOnProperty` (`014:78`) evaluate the application's own
property directly. providify's `@Profile` evaluates a free-form token from providify's own env
namespace (`PROVIDIFY_PROFILES`) or an explicit `activate_profile()` call — mapping
`VARCO_CACHE_BACKEND=redis` onto `@Profile("cache-redis")` requires varco code that reads the
setting and translates it, which is a second, undocumented vocabulary, and — more importantly —
cannot express a **predicate**: "active when `redis` is importable **and**
`VARCO_CACHE_BACKEND=redis`", or "active when `settings.enabled` is true" for any of the five
`enable_*` opt-in sites below.

Concrete varco sites paying for this today — selection happens *inside a provider body* or by an
*imperative `enable_*` call*, and neither shape is visible to `describe()`/`validate()`:

- `varco_redis/varco_redis/bus.py:516-537` — `RedisEventBusSelectorConfiguration.bus()` reads
  `settings.use_streams` (`VARCO_REDIS_USE_STREAMS`, `varco_redis/varco_redis/di.py:21-26`) and
  returns one of `RedisStreamEventBus`/`RedisEventBus` from a single `@Provider`. Both
  implementations are reachable only through the `if` inside the function body; neither is a
  binding in its own right.
- Five imperative opt-ins, each an `if`-shaped predicate a caller must remember to invoke rather
  than a declaration the container can see: `varco_core/varco_core/auth/di.py:49-100`
  `enable_authorization_audit()`, `varco_core/varco_core/tenancy/di.py:77-99`
  `enable_tenant_membership()`, and (per `rg -n 'def enable_' varco_*/varco_*`, re-run for this
  report, 8 raw hits) `varco_casbin/varco_casbin/di.py:127` `enable_policy_authorizer`,
  `varco_core/varco_core/flags/di.py:58` `enable_feature_flags`,
  `varco_core/varco_core/revocation/di.py:70` `enable_token_revocation`,
  `varco_redis/varco_redis/di.py:248` `enable_redis_token_revocation`. (The remaining two of the
  8 raw `def enable_` hits are not this pattern:
  `varco_fastapi/varco_fastapi/webhook/router.py:265` `enable_subscription` is an HTTP route
  handler, unrelated to DI; `varco_core/varco_core/deprecation.py:149` is a docstring example
  of the deprecated `enable_rls_ddl` name (renamed to `render_rls_ddl`, per `CLAUDE.md`'s
  DI-wiring-verb-taxonomy section) — neither is a live `enable_*` DI opt-in site.)
- **`priority=-sys.maxsize-1`/`priority=-sys.maxsize` framework-default sites** — re-counted for
  this report: `rg -n 'priority=-sys.maxsize' varco_*/varco_*` → **72 raw hits** (this plan's own
  re-count, not the scout's earlier count of 15; the difference is that this `rg` invocation, run
  without `-w` or a line-content filter, also matches the handful of docstring/comment lines that
  *mention* the pattern in prose rather than apply it as a decorator argument — e.g.
  `varco_redis/varco_redis/config.py:165`'s "``priority=-sys.maxsize`` keeps this the
  lowest-priority binding" explanatory comment). None of these sites are conditional on
  configuration either — they are unconditional low-priority defaults, overridable only by a
  higher-priority binding, never by a property predicate.

## 3. Minimal reproduction

The guard body (`varco_core/tests/test_providify_upstream_gaps.py::TestP25ConditionalRegistration::test_requires_decorator_is_exported`),
providify-only, documenting the expected contract even though unreachable while the import
fails:

```python
from providify import Requires  # raises ImportError today

flag = {"on": True}

class Base:
    pass

@Requires(condition=lambda: flag["on"])
class OnImpl(Base):
    pass

@Requires(condition=lambda: not flag["on"])
class OffImpl(Base):
    pass

container = DIContainer()
container.bind(Base, OnImpl)
container.bind(Base, OffImpl)

assert isinstance(container.get(Base), OnImpl)

flag["on"] = False
assert isinstance(container.get(Base), OffImpl)
```

Observed today (marker verified without it present first): `from providify import Requires`
raises `ImportError: cannot import name 'Requires' from 'providify'` —
`providify/__init__.py` exports no such name at 2.0.1.

A 15-line providify-only version of the real `bus.py:516-537` selector, showing exactly what a
declarative form would replace:

```python
@Configuration
class SelectorConfiguration:
    @Provider(singleton=True)
    def bus(self, settings: Inject[Settings]) -> AbstractEventBus:
        # The only "declaration" of which implementation is active is this
        # runtime `if` — describe()/validate() see one opaque provider, never
        # the two candidate implementations or the condition that chose one.
        if settings.use_streams:
            return RedisStreamEventBus(settings)
        return RedisEventBus(settings)
```

## 4. The ask

| Candidate | ✅ | ❌ |
|---|---|---|
| **(1) `@Requires(condition=Callable[[], bool])` as a third conjunct in `_binding_is_active()`** (`providify/container.py:1665-1676`), evaluated **lazily at resolve time**, exactly where `@Profile` is evaluated today | Consistent with the existing evaluation point; zero new container state; a callable can close over any settings object; composes with `@Profile`/`@Alternative` by AND | Evaluated on every `_filter()` call — needs a per-binding memo (a `functools.cache` on the marker, or a settled-bindings cache) or a documented "predicates must be cheap" contract; `validate()` pass 1 is unfiltered (`providify/container.py:5327-5333`), so an inactive conditional binding is still validated today — whether that stays true for `@Requires` needs a decision (⚠️ **desirable, not a defect** — `DI-7`'s planned "what would be active under other settings" report needs exactly this) |
| **(2) `@Requires(env="VARCO_CACHE_BACKEND", value="redis")` sugar over (1)** | The Micronaut/Spring spelling operators already recognise; greppable; the 80% case in varco is a single env var | Env-only — varco settings are pydantic `BaseSettings` with code>env>default precedence (`DI-6`), so an env-only check can disagree with the resolved settings object. **Ask for it only as sugar over (1), never as the sole form** |
| **(3) Scan-time / install-time evaluation** — drop the binding entirely if the condition is false | `describe()` shows only what is real; no per-lookup cost | Breaks the ordering-independence `activate_profile()` was designed to preserve (`providify/container.py:1840-1846` — the reachable graph may change after registration); a condition over a settings object that is itself DI-provided cannot run before that provider exists. ❌ **Not recommended as the primary**; acceptable only as an opt-in `eager=True` |
| **(4) Interaction with `@Alternative` priority and `validate()`** — a false condition removes the binding from candidate selection *before* priority/qualifier tie-break, at exactly the point `_binding_is_active` sits today (`providify/container.py:1732`); `validate()` reports a conditional binding's state via an informational issue kind (e.g. `IssueKind.CONDITION_INACTIVE`), sibling of the shipped `UNREACHABLE_PRE_DESTROY` (`providify/container.py:5327`), rather than skipping it silently | `DI-7`'s wiring-report work gets its "why is this the live one" answer for free | One more `IssueKind` to maintain; this row should be asked, not insisted on |

**Recommendation for the maintainer**: (1) as the primitive, (2) as optional sugar over it never
as a replacement, (3) rejected as the primary evaluation point (opt-in only), (4) specified
alongside (1).

## 5. Answer to BACKLOG open question 2

**Not blocked.** *"Can providify's `@Alternative` be selected from configuration, or only from
code?"* — **configuration-selected, yes, but only via providify's own profile vocabulary or a
varco-written translation into `activate_profile()`; never via a property predicate.**

`@Alternative` is selected by **code** (`enable_alternative(cls)`,
`providify/container.py:1784`) **or** by a **`@Profile` on the alternative**
(`providify/container.py:1672-1675` — once a profile expression is present it alone governs
eligibility, and `_enabled_alternatives` membership is not consulted). Profiles come from
`PROVIDIFY_PROFILES` (`providify/profiles.py:24`, read once at construction,
`providify/container.py:767`) or `activate_profile()` (`providify/container.py:1836`). There is
no equivalent of Quarkus `quarkus.arc.selected-alternatives` (`014:58-59`) keyed on a class name,
and no property predicate of any kind.

Proof, this report's control test
(`TestP25ConditionalRegistration::test_profile_is_the_only_declarative_switch_today`, always
passing at 2.0.1): a `@Profile("x")`-gated singleton is invisible under
`DIContainer(profiles=())` (`container.get(Base)` raises `LookupError`) and becomes visible
immediately after `container.activate_profile("x")`.

**Consequence for `DI-4`: not blocked.** A `VARCO_<SEAM>_BACKEND=<value>` →
`activate_profile(f"<seam>-<value>")` mapping at bootstrap uses only public API and is a
legitimate interim path — **provided** `DI-4`'s plan (a) centralises the translation in exactly
one function, (b) documents the profile vocabulary as public, and (c) records the two edge cases
providify itself documents: a `@Profile`d singleton already resolved under the old profile set is
**not** evicted (`providify/container.py:1865-1868`), and `validate()` pass 1 ignores activation
entirely (`providify/container.py:5327-5333`). The P25 ask (this report) is filed regardless,
because the predicate cases (the five `enable_*` sites above) have no profile spelling at all —
`activate_profile()` selects among named alternatives, it cannot express "on if `settings.enabled`
and `redis` is importable". ⚠️ ASSUMPTION, per the plan: that a public-API `activate_profile()`
translation is not a "shim" under `BACKLOG.md:55` — recorded as this report's reading; the `DI-4`
planner confirms or overturns it in writing.

⚠️ **Correction to research brief 014**: every mention of providify shipping `@Requires`
(`014:187-188,204,238`) is wrong at providify 2.0.1 — it ships `@Profile`/`@Alternative` only,
confirmed by the §1 absence grep above. Flagged at `BACKLOG.md:29-31` and banner-corrected in the
brief itself (Plan 043 Step 8).

## 6. Partly ours? Yes, partly.

varco *could* today split `RedisEventBusSelectorConfiguration` into two `@Profile`-gated
bindings and translate `VARCO_REDIS_USE_STREAMS` into
`activate_profile("redis-streams")` — a fully supported mechanism, available now. That fixes the
*selection* symptom for one site; it does not give the five `enable_*`-shaped opt-ins a
declarative form, and it introduces the profile-name-vocabulary problem described in §2. Filed
as an option for `DI-4` to weigh, not applied in this plan.

## 7. Interim workaround — none applied

No varco shim, helper, or translation layer is added by this plan. The `activate_profile()`
translation described in §5 is a legitimate *use* of public API that `DI-4` may adopt — it is not
implemented here.

## 8. Guard

`varco_core/tests/test_providify_upstream_gaps.py::TestP25ConditionalRegistration` —
`test_requires_decorator_is_exported` (`xfail(strict=True, raises=ImportError)`) plus
`test_profile_is_the_only_declarative_switch_today` (always-passing control, proves §5's answer).
No Docker, no backend — pure providify, runs in every `make test PKG=varco_core`. ⚠️ Name-based:
if providify ships an equivalent feature under a different name, this guard stays silently xfail
— re-run the §1 absence grep at every providify version bump (same discipline as Plan 016's
upgrade checklist).

## 9. Resolution (2026-09-14)

providify 2.1.0 ships `@Requires(condition=..., env=..., value=...)` exactly as candidate (1) of
§4 asked, with (2)'s `env=`/`value=` sugar included and (4)'s `IssueKind.CONDITION_INACTIVE`
(`Severity.INFO`) shipped alongside it (`providify/CHANGELOG.md:32-47`; `RequiresMarker` and
`ConditionEvaluationError` are the new public names; `providify/decorator/scope.py:818-823`).
Candidate (3) — scan-time/install-time evaluation — was correctly rejected upstream too: `@Requires`
is evaluated lazily at resolve time, the same moment as `@Profile`/`@Alternative`, per this
report's recommendation.

**varco does not adopt `@Requires` in this plan.** Plan 045 verifies the shipped primitive as a
passing contract pin (Step 5, `test_providify_contracts.py`) so the guard converts from `xfail` to
green, but the five `enable_*` opt-in sites and `RedisEventBusSelectorConfiguration`'s
`settings.use_streams` branch are left untouched — **Plan 047** (config-driven backend selection
and catalogue) is where `@Requires` gets adopted at those sites, per this plan's own Non-goals
("Backend selection ... this plan leaves `RedisEventBusSelectorConfiguration` in place and only
re-declares its priority tier").

§5's "DI-4 not blocked via `activate_profile()`" answer is **superseded**, not merely academic: with
`@Requires` shipped, Plan 047 uses it directly rather than the `activate_profile()` translation
layer this report proposed as an interim — the profile-vocabulary problem §2 raised (a second,
undocumented naming scheme) never needs to be built.

Guard: converted in Plan 045 Step 5 — `test_requires_decorator_is_exported` (formerly `xfail`) now
imports `Requires` and asserts a `@Requires(condition=...)`-gated pair flips as predicted, plus a
new `CONDITION_INACTIVE` INFO pin. `test_profile_is_the_only_declarative_switch_today` is kept as a
still-true control (`@Profile`/`@Alternative` semantics are unchanged by this release).
