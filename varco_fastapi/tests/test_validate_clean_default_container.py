"""
Plan 045 / Step 13 — "`validate()` is clean for a default varco container" (§D-validate).

For `container = DIContainer(); container.scan("varco_core", recursive=True);
bootstrap(container)` (varco_fastapi's `bootstrap()`),
`report = container.validate(raise_on_error=False)` must satisfy
`report.errors == ()` and `report.warnings == ()`; `report.infos` must be
`()` for a *default* container with no user binding added — proof that no
varco default shadows another varco default at equal tier.

`_KNOWN_WARNINGS` is an explicit `(kind, owner)` allowlist populated only if
Step 1's preflight measures a pre-existing warning unrelated to this plan;
empty today.

Two pre-existing `MISSING_BINDING_DEFAULTED` warnings, unrelated to this
plan, are allowlisted below: `InMemoryEventBus.__init__`'s `error_policy`
(`ErrorPolicy`) and `dispatch_mode` (`DispatchMode`) parameters both carry a
Python default with no DI binding — a pre-existing, correct-as-is shape
(the constructor default is the intended value; no binding was ever meant
to exist), not a Plan 045 default-binding defect. Left to whichever future
plan wants to either bind these types explicitly or teach
`InMemoryEventBus` to declare them `Annotated[..., InjectMeta(optional=True)]`
(tracked informally for 046/050's `di_wiring` follow-on work — no BACKLOG
row exists yet for this specific pair).
"""

from __future__ import annotations

from providify import DIContainer, IssueKind
from varco_fastapi.di import bootstrap

_KNOWN_WARNINGS: set[tuple[IssueKind, str]] = {
    (IssueKind.MISSING_BINDING_DEFAULTED, "InMemoryEventBus.__init__"),
}


def test_validate_is_clean_for_a_default_varco_fastapi_container() -> None:
    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bootstrap(container)

    report = container.validate(raise_on_error=False)

    assert report.errors == ()
    known = {kind for kind, _owner in _KNOWN_WARNINGS}
    unexpected_warnings = [w for w in report.warnings if getattr(w, "kind", None) not in known]
    assert unexpected_warnings == []
    assert report.infos == ()
