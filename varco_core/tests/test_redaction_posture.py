"""
tests.test_redaction_posture
==============================
Plan 040 / S21, Phase 5, Step 17 — ``inspect_redaction_posture()``
(§D-S21-posture): a pure, never-raising read with a fixed ``check``-id table.

⛔ This does not wire into ``varco_fastapi.posture.SecurityPosture`` — that
is explicitly out of scope this cycle (§D-S21-posture, Non-goals).
"""

from __future__ import annotations

from typing import Any

_EXPECTED_CHECK_IDS = {
    "redaction.audit.disabled",
    "redaction.error_params.disabled",
    "redaction.default.custom",
    "redaction.patterns.substring_mode",
}


def test_check_id_set_matches_the_literal_table() -> None:
    from varco_core.redaction.posture import inspect_redaction_posture

    posture = inspect_redaction_posture()
    check_ids = {f.check for f in posture.findings}
    assert check_ids <= _EXPECTED_CHECK_IDS


def test_service_class_without_audit_redactor_emits_disabled_finding() -> None:
    from varco_core.redaction.posture import inspect_redaction_posture
    from varco_core.service.audit import AuditLogMixin

    class _NoRedactorService(AuditLogMixin):
        pass

    posture = inspect_redaction_posture(service_classes=(_NoRedactorService,))
    checks = {f.check for f in posture.findings}
    assert "redaction.audit.disabled" in checks


def test_service_class_with_audit_redactor_does_not_emit_disabled_finding() -> None:
    from varco_core.redaction import PolicyRedactor
    from varco_core.redaction.posture import inspect_redaction_posture
    from varco_core.service.audit import AuditLogMixin

    class _WithRedactorService(AuditLogMixin):
        _audit_redactor = PolicyRedactor()

    posture = inspect_redaction_posture(service_classes=(_WithRedactorService,))
    for finding in posture.findings:
        if finding.check == "redaction.audit.disabled":
            assert False, "must not fire when _audit_redactor is set"


def test_redact_params_false_emits_error_params_disabled_finding() -> None:
    from varco_core.exception.settings import ErrorEnvelopeSettings
    from varco_core.redaction.posture import inspect_redaction_posture

    posture = inspect_redaction_posture(
        envelope_settings=ErrorEnvelopeSettings(redact_params=False)
    )
    checks = {f.check for f in posture.findings}
    assert "redaction.error_params.disabled" in checks


def test_redact_params_true_does_not_emit_error_params_disabled_finding() -> None:
    from varco_core.exception.settings import ErrorEnvelopeSettings
    from varco_core.redaction.posture import inspect_redaction_posture

    posture = inspect_redaction_posture(envelope_settings=ErrorEnvelopeSettings(redact_params=True))
    checks = {f.check for f in posture.findings}
    assert "redaction.error_params.disabled" not in checks


def test_custom_default_redactor_emits_info_finding() -> None:
    from varco_core.redaction import (
        reset_redaction_state,
        set_default_redactor,
    )
    from varco_core.redaction.posture import inspect_redaction_posture

    class _CustomRedactor:
        def redact(self, key: str, value: Any) -> Any:
            return value

    reset_redaction_state()
    set_default_redactor(_CustomRedactor())  # type: ignore[arg-type]
    try:
        posture = inspect_redaction_posture()
        checks = {f.check for f in posture.findings}
        assert "redaction.default.custom" in checks
    finally:
        reset_redaction_state()


def test_default_policy_redactor_does_not_emit_custom_finding() -> None:
    from varco_core.redaction import reset_redaction_state
    from varco_core.redaction.posture import inspect_redaction_posture

    reset_redaction_state()
    posture = inspect_redaction_posture()
    checks = {f.check for f in posture.findings}
    assert "redaction.default.custom" not in checks


def test_never_raises_on_nonsense_input() -> None:
    from varco_core.redaction.posture import inspect_redaction_posture

    # A nonsense service class (not even an AuditLogMixin subclass) must
    # never crash the inspector — it must degrade to a finding, not raise.
    class _NotAService:
        pass

    posture = inspect_redaction_posture(service_classes=(_NotAService,))  # type: ignore[arg-type]
    assert posture is not None
