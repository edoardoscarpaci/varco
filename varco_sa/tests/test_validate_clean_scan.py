"""
Plan 045 / Step 13 — "`validate()` is clean" leg for `varco_sa` (§D-validate)
— DROPPED, per the plan's own escape hatch, with this note for Plan 046.

`container.scan("varco_sa", recursive=True)` alone (no `SAConfig` bound)
produces two genuine `Severity.ERROR` `MISSING_BINDING` issues —
`@Provider(sa_advisory_lock)` and `@Provider(sa_xact_advisory_lock)` both
require `Inject[SAConfig]`, and `SAConfig` has no sensible env-settings-style
default: it holds a hand-constructed `AsyncEngine` (a real resource, not a
value pydantic `BaseSettings.from_env()` could synthesize). This is
structurally different from every other package's validate-clean leg — the
other legs' warnings are `Severity.WARNING` `MISSING_BINDING_DEFAULTED`
issues, safely allowlisted; an `ERROR` cannot be allowlisted under
§D-validate's `report.errors == ()` requirement, and there is no
plan-045-scoped way to give `SAConfig` a default without changing what it
means (Plan 048's settings-standardization work is the more natural owner
of that decision, if it is ever made at all).

Measured at Plan 045 Step 1(e) preflight (see the scratchpad preflight
file cited in the plan's final report) and confirmed again here — this is
not a regression from any Plan 045 change, it is the shape `SAConfig` has
always had.

DECISION (§D-validate): this leg is dropped rather than written as a
false-green test. Plan 046's `testkit/varco_conformance/di_wiring.py` suite
is the natural home for a `varco_sa` validate-clean check that either (a)
requires the caller to supply a real `SAConfig`/engine fixture first, or
(b) narrows the assertion to "no *additional* errors beyond the two
already-known `SAConfig` ones" rather than a blanket `errors == ()`.
"""

from __future__ import annotations

import pytest


@pytest.mark.skip(
    reason=(
        "GAP: varco_sa's `SAConfig` requires a real AsyncEngine — a bare "
        "container.scan('varco_sa') without SAConfig bound produces two "
        "genuine ERROR-severity MISSING_BINDING issues that cannot be "
        "allowlisted under §D-validate's report.errors == () rule. Dropped "
        "for Plan 046's di_wiring suite to own with a real SAConfig fixture "
        "— see this module's docstring."
    )
)
def test_validate_is_clean_for_a_bare_varco_sa_scan() -> None:
    pass
