# P34 — `validate()` reports `MISSING_BINDING` ERRORs for the *own* dependencies of an inactive (`@Requires`/`@Profile`-excluded) binding

**Filed**: 2026-10-02 (Plan 052 / DI-11, amendment 4 — `varco diagnose --check` on the example app)
**Status**: ✅ Resolved (2026-10-02, providify 2.1.1) — see §Resolution
**Library**: `providify` 2.1.0 (installed copy `.venv/lib/python3.12/site-packages/providify/`,
identical to `/home/edoardo/projects/providify` at `3cb207f`)
**Severity**: High for varco — the false positive is ERROR-severity, so it fails
`varco diagnose --check` and, through Plan 052 §D-startup's default
`create_varco_app(validate=True)`, aborts the lifespan of any app that installs a module whose
inactive conditional provider has an unbound dependency. Today that is
`SAConfiguration.webhook_repository` (`varco_sa/varco_sa/di.py:411-418`) for every `SAModule`
app without a bound `FieldEncryptor`.

## 1. What upstream does today (verified in source, 2026-10-02)

`DIContainer.validate()` walks every registered binding in one loop —
`for idx, binding in enumerate(self._bindings):` (`container.py:6442`). Inside that loop:

- **Pass 1c** (`container.py:6508-6517`, closure `condition_inactive_issue` at `:6296-6350`)
  evaluates the binding's `@Requires` markers and, when any is unsatisfied, emits a
  `CONDITION_INACTIVE` **INFO** whose message (`:6337-6343`) says the binding *"is excluded from
  get()/get_all()/is_resolvable() and from candidate resolution in this report"*.
- **Pass 2 — the graph tier** (`container.py:6529-6548+`) then runs **for the same binding
  anyway**. The only skip is `if unresolved: continue` (`:6526`), which is set by an
  `AnnotationResolutionError`, never by inactivity. `self._iter_injection_points(binding)`
  (`:6531`) yields the binding's own injection points, each is resolved through `memo_filter` →
  `self._filter()` (`:2051`, active-only via `self._binding_is_active(b)` at `:2150`), and a
  point with no candidate emits `IssueKind.MISSING_BINDING` at **`Severity.ERROR`**
  (`:6604`, `:6676`).

So the **candidate** side of pass 2 is activation-aware (that is plan 015's documented design,
docstring `:6086-6097`, and the subject of providify's own
`tests/test_requires.py:651::test_inactive_condition_plus_dependent_reports_missing_binding_error`),
but the **owner** side is not: the dependencies of a binding that `get()` can never construct
are checked against the active graph and reported as ERRORs.

`_binding_is_active()` (`container.py:1904-2005`) — the very predicate `get()` uses — is never
consulted for the owner in pass 2. The same holds for `@Profile`-excluded and
not-enabled-`@Alternative` owners (same predicate, `:1915`): reproduced for `@Profile` below.

**Not decided upstream, just unspecified.** Neither plan 015's edge-case table (E1-E23,
`plans/015-requires-conditional-registration.md:695-723`) nor plan 005's validation step
(`plans/005-profile-based-activation.md:356-360`) mentions an inactive binding's *own*
dependencies; both only discuss the dependents of an inactive binding. The `validate()`
docstring's advice to *"satisfy the condition (env var / flag) before validating each
deployment configuration you care about"* (`:6095-6097`) is the right model, and it is exactly
what pass 2 contradicts: validating the deployment where the condition is **off** still reports
the deployment where it is **on**.

## 2. Minimal reproduction (no Docker, providify only)

```python
import os
from providify import DIContainer, Inject, Requires, Singleton

os.environ.pop("GATE", None)          # condition is OFF


class Missing: ...                     # never bound
class Port: ...


@Singleton
@Requires(env="GATE", value="on")
class Gated(Port):
    def __init__(self, m: Inject[Missing]) -> None: ...


c = DIContainer()
c.bind(Port, Gated)
report = c.validate(raise_on_error=False)

print(c.is_resolvable(Port))           # False — get() treats Gated as absent
print([(i.kind.name, i.owner) for i in report.issues])
# [('CONDITION_INACTIVE', 'Gated'),
#  ('MISSING_BINDING', 'Gated.__init__'),     <-- ERROR for a binding that cannot be built
#  ('MISSING_BINDING', 'Gated.__init__')]     <-- (doubled by bind()'s exact_only self-binding)
print(report.ok)                       # False
```

Identical result for a `@Configuration` `@Provider` gated by `@Requires` (one `MISSING_BINDING`
owned by `@Provider(port)`) and for a `@Profile("prod")` class with `"prod"` inactive. No false
*cycle* results: an inactive owner records an adjacency edge, but no active binding can point
back at it (candidates are filtered), so `_find_cycles` stays correct.

The varco symptom, end to end:

```text
$ uv run varco diagnose --path examples/00-full-stack-post-api \
      --modules example.app:modules --static --check
... ERROR missing_binding @Provider(webhook_repository): 'encryptor' requests FieldEncryptor
    but no binding is registered ...
... INFO  condition_inactive @Provider(webhook_repository) is registered but currently inactive:
    @Requires(env='VARCO_WEBHOOK_BACKEND', value='sa') (VARCO_WEBHOOK_BACKEND is unset) ...
exit 1
```

providify reports, in the same report, that the owner is inactive *and* that its dependency is
a wiring defect.

## 3. Why this is a gap, not a varco misuse

- `webhook_repository` declares `encryptor: Inject[FieldEncryptor]` **required, on purpose**
  (`varco_sa/varco_sa/di.py:420-427`): a durable webhook backend selected without an encryptor
  must fail loudly. That is precisely what should happen when `VARCO_WEBHOOK_BACKEND=sa` — and
  with the condition **on**, the ERROR is correct and wanted. With it **off**, the provider can
  never run, so no injection can ever fail; the ERROR describes a deployment other than the one
  being validated.
- varco registers it through the ordinary `@Configuration` install path; nothing bypasses the
  condition — providify's own `CONDITION_INACTIVE` for the same owner proves the marker was
  read.
- The ERROR cannot be silenced without changing what the binding means: making the parameter
  optional (`Inject[FieldEncryptor] | None` or a default) would downgrade the condition-**on**
  case to a `None` at runtime, removing the loud failure the parameter exists for.
- Conditional registration's whole purpose (plan 015, varco's P25) is to ship backends whose
  dependencies only exist when selected. Every such backend with a dependency the default
  deployment does not bind hits this.

## 4. Blocks

- Plan 052 Verification: `varco diagnose ... --check` on the example app (amendment 4 removed
  it from Verification and pinned this one ERROR in the example test instead).
- Plan 052 §D-startup: the default lifespan refuses to start on any providify ERROR, so any
  `SAModule` app without a bound `FieldEncryptor` is at risk of a startup refusal caused by a
  backend it did not select. (Not exercised against a live Postgres in this filing.)

## 5. The ask — candidate fixes

- ✅ **(a) — recommended: skip pass 2's injection-point walk for an owner where
  `not self._binding_is_active(binding)`.** Pass 1/1b/1c stay unfiltered exactly as plan 005/015
  designed (scope leaks, unreachable `@PreDestroy`, `CONDITION_INACTIVE` still reported, raising
  predicates still propagate per E14). This makes pass 2 mirror `get()` on *both* sides of an
  edge, which is what the docstring already promises (`:6086-6097`: "validates the graph as it
  will actually be wired"). One-line guard beside `if unresolved: continue`. ❌ an inactive
  binding's broken dependency is no longer reported until someone validates with the condition
  on — which is the documented per-deployment workflow anyway; `CONDITION_INACTIVE` already says
  the binding was not checked as live.
- ✅ **(b) — keep the walk, downgrade the result for inactive owners** — e.g. report
  `MISSING_BINDING` at `INFO` (or a new `MISSING_BINDING_INACTIVE_OWNER` kind) and fold the
  owner's inactive clause into the message. Preserves the "your off-configuration is broken"
  signal. ❌ a new `IssueKind` (or a severity that varies by context for one kind) is more API
  surface; consumers keying on `kind` must learn it.
- ✅ **(c) — opt-in flag**: `validate(*, include_inactive_owners: bool = False)` selecting
  between (a) (default) and today's behaviour. ❌ a knob most callers will never turn; only worth
  it if someone already depends on today's behaviour.
- ❌ **(d) — do nothing; document "bind every dependency of every conditional backend, or make
  them optional".** Rejected: it forces each library shipping conditional backends to either
  bind placeholders for unselected backends or weaken required parameters (§3, third bullet).

Separately (minor, same walk): a `bind(Iface, Impl)` pair reports each `MISSING_BINDING` twice
(the `exact_only` self-binding), which `condition_inactive_issue` already dedupes for its own
kind (`:6302-6316`). Fix (a) removes the doubled ERRORs for the inactive case for free.

## 6. Is this partly ours?

Not the root cause. One varco-side mitigation exists and was considered:

- **Post-filter in `varco_core.di.report.build_wiring_report()`** — drop any `MISSING_BINDING`
  whose owner also has a `CONDITION_INACTIVE`. ❌ Rejected for now: it must join on the
  `owner` *display* string, and those differ by binding shape (`CONDITION_INACTIVE` owner
  `Gated` vs `MISSING_BINDING` owner `Gated.__init__` for a class; equal only for providers).
  Plan 052 premise (5) keys report rows on `(interface, qualifier)`, never on `owner`; a
  string join on display labels is the fragile private-shape dependency CLAUDE.md asks us not
  to paper over upstream gaps with. It also would not cover `@Profile`/`@Alternative`-excluded
  owners, which emit no INFO to join on.

## 7. Interim workaround (until fixed upstream)

- `varco diagnose --check` is **excluded** from Plan 052's Verification (amendment 4); the
  example-app commands run without `--check`.
- `examples/00-full-stack-post-api/example/tests/test_diagnose_static.py` pins
  `webhook_repository` as the **only** tolerated ERROR (`_KNOWN_ERRORS`), so a new ERROR still
  fails, and the entry is removed when this gap closes.
- Guard: `varco_core/tests/test_providify_contracts.py::TestP34ValidateInactiveOwnerDeps`
  (strict xfail, no Docker) — XPASSes the moment upstream fixes (a) or (c)-default, and is then
  converted into a passing contract pin.

## Resolution (2026-10-02)

Fixed upstream in **providify 2.1.1** with candidate fix (a): `validate()` now
`continue`s past pass 2 for any binding where `_binding_is_active(binding)` is
false, placed after passes 1–1d so `CONDITION_INACTIVE`, scope-leak and
unreachable-`@PreDestroy` reporting still run for every binding, and a raising
condition still propagates (E14). providify added nine regression tests
(`tests/test_requires.py::TestInactiveOwnerDependencies`), including the
must-still-error cases for an *active* gated binding and for dependents of an
inactive binding.

varco side: every package pins `providify>=2.1.1`; the strict-xfail guard
`TestP34ValidateInactiveOwnerDeps` is converted to a regular regression test;
the example app's `test_diagnose_static.py` now asserts **zero** validation
errors (the `webhook_repository` pin is gone). The interim workaround is retired.

Not addressed by this fix (separate, unfiled): `bind(Iface, Impl)` reports each
pass-2 issue twice because of its implicit self-binding.
