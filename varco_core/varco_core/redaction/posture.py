"""
varco_core.redaction.posture
==============================
``inspect_redaction_posture()`` — a pure, never-raising posture read over
the redaction seam (Plan 040 / S21, §D-S21-posture).

⛔ **Not** wired into ``varco_fastapi.posture.SecurityPosture`` (Plan 036's
harness) — see the Non-goals section of the plan. It is defined and
exported so the 4.0 audit-default flip has somewhere to land, and a future
collector can be written against the stable ``check``-id table below without
reading this module's implementation.

Thread safety:  ✅ Pure function, no shared state beyond the module-level
                   default-redactor cell it reads (``redaction.redactor``).
Async safety:   ✅ Synchronous, no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from varco_core.redaction.redactor import PolicyRedactor, default_redactor

if TYPE_CHECKING:
    from varco_core.exception.settings import ErrorEnvelopeSettings
    from varco_core.service.audit import AuditLogMixin

__all__ = ["RedactionFinding", "RedactionPosture", "inspect_redaction_posture"]

Severity = Literal["info", "warn"]


@dataclass(frozen=True)
class RedactionFinding:
    """
    One reported fact about the current redaction configuration.

    Args:
        check: Stable id from the fixed table this plan commits to
            (§D-S21-posture) — never renamed once shipped.
        severity: ``"info"`` or ``"warn"``.
        detail: What was observed, human-readable.
    """

    check: str
    severity: Severity
    detail: str


@dataclass(frozen=True)
class RedactionPosture:
    """The full point-in-time report — every finding, unfiltered."""

    findings: tuple[RedactionFinding, ...]


def inspect_redaction_posture(
    *,
    service_classes: tuple[type[AuditLogMixin], ...] = (),
    envelope_settings: ErrorEnvelopeSettings | None = None,
) -> RedactionPosture:
    """
    Build a point-in-time ``RedactionPosture`` report.

    Never raises — a nonsense ``service_classes`` entry (not even an
    ``AuditLogMixin`` subclass) degrades to "no finding for that class",
    never a crash (§D-S21-posture: "the function never raises on a
    nonsense input and returns an all-``False`` posture instead").

    Args:
        service_classes: ``AuditLogMixin`` subclasses to inspect for
            ``redaction.audit.disabled``. Empty tuple (default) — no
            service-level findings.
        envelope_settings: The ``ErrorEnvelopeSettings`` in force. ``None``
            (default) constructs a fresh ``ErrorEnvelopeSettings()`` — the
            byte-identical, on-by-default posture.

    Returns:
        A ``RedactionPosture`` whose ``findings`` use only the four stable
        ``check`` ids:

        - ``redaction.audit.disabled`` (``warn``): an inspected
          ``AuditLogMixin`` subclass has ``_audit_redactor is None`` and
          has not overridden ``_audit_diff``.
        - ``redaction.error_params.disabled`` (``warn``):
          ``ErrorEnvelopeSettings.redact_params is False``.
        - ``redaction.default.custom`` (``info``): ``default_redactor()``
          is not a ``PolicyRedactor``.
        - ``redaction.patterns.substring_mode`` (``info``): the effective
          policy uses ``match_mode="substring"``.
    """
    findings: list[RedactionFinding] = []

    for service_class in service_classes:
        try:
            _inspect_service_class(service_class, findings)
        except Exception:  # noqa: BLE001 - a posture read must never crash
            continue

    try:
        _inspect_error_params(envelope_settings, findings)
    except Exception:  # noqa: BLE001
        pass

    try:
        _inspect_default_redactor(findings)
    except Exception:  # noqa: BLE001
        pass

    return RedactionPosture(findings=tuple(findings))


def _has_own_audit_diff_override(service_class: type) -> bool:
    """``True`` when ``service_class`` (or an ancestor short of the mixin
    itself) overrides ``AuditLogMixin._audit_diff``."""
    from varco_core.service.audit import AuditLogMixin

    audit_diff = getattr(service_class, "_audit_diff", None)
    base_audit_diff = AuditLogMixin.__dict__.get("_audit_diff")
    return audit_diff is not None and audit_diff is not base_audit_diff


def _inspect_service_class(service_class: type, findings: list[RedactionFinding]) -> None:
    redactor = getattr(service_class, "_audit_redactor", None)
    overridden = _has_own_audit_diff_override(service_class)
    if redactor is None and not overridden:
        findings.append(
            RedactionFinding(
                check="redaction.audit.disabled",
                severity="warn",
                detail=(
                    f"{service_class.__name__}._audit_redactor is None and "
                    f"_audit_diff is not overridden — audit diffs are "
                    f"written unredacted."
                ),
            )
        )


def _inspect_error_params(
    envelope_settings: ErrorEnvelopeSettings | None, findings: list[RedactionFinding]
) -> None:
    from varco_core.exception.settings import ErrorEnvelopeSettings as _Settings

    settings = envelope_settings if envelope_settings is not None else _Settings()
    if not settings.redact_params:
        findings.append(
            RedactionFinding(
                check="redaction.error_params.disabled",
                severity="warn",
                detail="ErrorEnvelopeSettings.redact_params is False — "
                "error_params() is emitted unredacted.",
            )
        )


def _inspect_default_redactor(findings: list[RedactionFinding]) -> None:
    redactor = default_redactor()
    if not isinstance(redactor, PolicyRedactor):
        findings.append(
            RedactionFinding(
                check="redaction.default.custom",
                severity="info",
                detail=f"default_redactor() is {type(redactor).__name__}, not PolicyRedactor.",
            )
        )
        return
    if redactor.policy.match_mode == "substring":
        findings.append(
            RedactionFinding(
                check="redaction.patterns.substring_mode",
                severity="info",
                detail="The effective policy uses match_mode='substring' "
                "— known false positives apply (e.g. 'pin' in "
                "'shipping_address').",
            )
        )
