"""
Plan 045 / Step 13 — "`validate()` is clean" leg for `varco_kafka` (§D-validate).

`container.scan("varco_kafka", recursive=True)` registers bindings without
opening a socket; `validate()` instantiates nothing, so this leg needs no
Docker/broker.

Pre-existing `MISSING_BINDING_DEFAULTED` warnings, unrelated to this plan,
are allowlisted below: `InMemoryEventBus.__init__`'s `error_policy`/
`dispatch_mode` and `KafkaEventBus.__init__`'s own `error_policy`/
`dispatch_mode`-shaped parameters all carry a Python default with no DI
binding — a pre-existing, correct-as-is shape, not a Plan 045 default-
binding defect (see `varco_fastapi/tests/test_validate_clean_default_container.py`
for the identical `InMemoryEventBus` pair).
"""

from __future__ import annotations

from providify import DIContainer, IssueKind

_KNOWN_WARNINGS: set[tuple[IssueKind, str]] = {
    (IssueKind.MISSING_BINDING_DEFAULTED, "InMemoryEventBus.__init__"),
    (IssueKind.MISSING_BINDING_DEFAULTED, "KafkaEventBus.__init__"),
}


def test_validate_is_clean_for_a_bare_varco_kafka_scan() -> None:
    container = DIContainer()
    container.scan("varco_core", recursive=True)
    container.scan("varco_kafka", recursive=True)

    report = container.validate(raise_on_error=False)

    assert report.errors == ()
    known = {kind for kind, _owner in _KNOWN_WARNINGS}
    unexpected_warnings = [w for w in report.warnings if getattr(w, "kind", None) not in known]
    assert unexpected_warnings == []
    assert report.infos == ()
