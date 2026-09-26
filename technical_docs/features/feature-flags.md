# Feature flags — `varco_core.flags`

Plan 032 (BACKLOG 3.1, row **D7** nice/S). Closes the re-parked OpenFeature row from Plan 022's
`§D-OF`: *"If it is still pre-1.0, D7 ships the seam and defers the provider."*

## §D-D7-trigger — the trigger has NOT fired; ship the seam, defer the provider

The backlog's OpenFeature Parked row carried an undated, confused evidence trail — briefs 001 and
002 evidenced the OpenFeature **specification** at v0.9, while the recorded un-park condition was
*"the `openfeature-sdk` Python SDK reaches 1.0"* — a different artifact. This plan re-checked the
SDK itself, not the spec, and recorded the date the check was made.

**Verified (research brief 004 §1, fetched 2026-09-04):**

| Artifact | Version | Date | 1.0? |
|---|---|---|---|
| `openfeature-sdk` (PyPI) | **0.10.0** | 2026-06-01 | ❌ no |
| OpenFeature specification | 0.9.0 | 2026-07-29 | ❌ no |

The trigger has **not** fired. Worse, brief 004 §1 records that v0.10.0 itself shipped a
breaking change inside a *minor* release — `set_provider()` no longer blocks; callers must use
`set_provider_and_wait()` instead — and the spec explicitly warns breaking changes will continue
while the SDK sits below 1.0.

Plan 022's `§D-OF` rejected building against OpenFeature "inside a version freeze". The freeze is
over, but the underlying objection is unchanged: an ABC shaped to a still-churning pre-1.0 spec,
shipped under lockstep versioning and gated by `scripts/api_surface.py`, is a liability — a
breaking upstream change would force either a varco breaking change of our own or a permanently
awkward compatibility shim.

**Decision — build the seam, do not build the provider.** `varco_core/varco_core/flags/`
(`base.py`, `memory.py`, `null.py`, `di.py`) ships `AbstractFeatureFlags` (a varco-shaped ABC,
not a transcription of OpenFeature's `AbstractProvider`), `FlagEvaluationContext`,
`InMemoryFeatureFlags`, and `NullFeatureFlags` (the always-off DI default). No
`OpenFeatureFlags` adapter exists yet.

DESIGN: a varco-shaped ABC now, an adapter later
  ✅ The seam is the valuable half — a tenant/user-aware evaluation context is useful with no
     provider at all.
  ✅ varco's ABC is stable under *our* SemVer because we own its shape; an OpenFeature-shaped ABC
     would inherit a pre-1.0 spec's churn into a frozen public surface.
  ✅ When the SDK reaches 1.0, an `OpenFeatureFlags` adapter is additive — a new implementation
     of an existing ABC, the cheapest possible change.
  ✅ Brief 004 §1 notes the SDK ships **no** in-memory/no-op test provider — `InMemoryFeatureFlags`
     is that, and it is useful immediately, independent of the OpenFeature question.
  ❌ Our ABC will not match OpenFeature's method names, so the future adapter does real
     translation rather than pass-through. Accepted: four typed resolution methods
     (bool/string/numeric/object) is a small translation surface — brief 004 §1 enumerates
     OpenFeature's own resolution methods as the same four shapes.
  ❌ Someone will ask why we did not just adopt OpenFeature outright. Answered here, with the
     version evidence and the fetch date, so the question is answered from a record rather than
     re-researched next cycle.

**Un-park trigger, going forward:** `openfeature-sdk` (the SDK, not the spec) reaches **1.0.0**.
At that point an `OpenFeatureFlags` adapter is purely additive against this ABC — no change to
`AbstractFeatureFlags`, `InMemoryFeatureFlags`, or `NullFeatureFlags` is anticipated.

## Why `tenant_id` comes from `current_tenant()`, never `RequestContext`

`FlagEvaluationContext.current()` builds its `tenant_id` from
`varco_core.service.tenant.current_tenant()` and its `attributes` from the active
`RequestContext`'s `extras`. This is the same rule CLAUDE.md states for every other subsystem
that touches tenancy (RLS, the DLQ tenant stamp, the audit trail): tenant has exactly one source
of truth, and `RequestContext` never holds it.

## Why `NullFeatureFlags` is a scanned `@Singleton`, not a module-level default

`NullFeatureFlags` is registered `@Fallback @Singleton` (Plan 045) so
`container.scan("varco_core.flags", recursive=True)` binds it automatically — mirroring
CLAUDE.md's "DI defaults" rule (default ABC implementations register through DI, never as a
hardcoded module-level singleton). `enable_feature_flags(container)` then registers
`InMemoryFeatureFlags` via a module-level `@Provider` (never scanned itself, so importing the
module does not activate it) which unconditionally outranks the low-priority default. This is
the same `enable_*` verb shape as `varco_casbin.di.enable_policy_authorizer` (CLAUDE.md's DI
wiring verb taxonomy).

## Enabling via env (Plan 047)

`VARCO_FEATURE_FLAGS_ENABLED=true` is the env twin of `enable_feature_flags(container)` — a
`@Requires`-gated `@Fallback` provider (`feature_flags_when_enabled`, `varco_core.flags.di`),
discovered by the same `container.scan("varco_core", recursive=True)` that discovers
`NullFeatureFlags`; it is inert (never activates `InMemoryFeatureFlags`) unless the variable is
truthy. Code always wins over the env form. See
`technical_docs/features/dependency-injection.md`'s "Enabling a feature" section.

## Pitfalls

| Pitfall | Why it happens | Fix |
|---|---|---|
| Expecting `varco_core.flags` to be reachable from top-level `varco_core` | Deliberately not re-exported — same PEP 562 import-budget reasoning as `varco_core.webhook`/`varco_core.idempotency` | `from varco_core.flags import AbstractFeatureFlags` (or the individual submodule) |
| A flag override "leaking" across tenants in tests | `InMemoryFeatureFlags` only consults `context.tenant_id` if a `FlagEvaluationContext` was actually passed | Always pass `context=FlagEvaluationContext.current()` (or an explicit one) at the call site — omitting it evaluates with no targeting at all |
| Assuming `enable_feature_flags()` alone is enough | It only registers the provider — `container.scan("varco_core.flags", recursive=True)` must also run so `NullFeatureFlags` exists as the pre-opt-in default and providify resolves the binding at all | Always pair `scan` + `enable_feature_flags()`, in that order, as shown in the README |
| Shaping a resolver to match OpenFeature's method names in advance | Tempting, since an `OpenFeatureFlags` adapter is anticipated | Don't — `§D-D7-trigger` above explicitly accepts that the future adapter does real translation; matching a pre-1.0 shape now buys nothing and risks copying its churn |

## Un-park trigger record

See BACKLOG.md's OpenFeature Parked row for the amended entry (checked 2026-09-04, cites this
document and research brief 004 §1).
