"""
Plan 036 (S9a) / Phase 2, Step 17 — red-mode tests for the
``varco_fastapi.posture`` core (§D-S9-shape, §D-S9-oq1, §D-S9-suppress,
§D-S9-enforce).

``varco_fastapi/varco_fastapi/posture.py`` does not exist yet — every test
below is expected to fail with ``ModuleNotFoundError`` on first import.
"""

from __future__ import annotations

import pytest


def test_posture_severity_has_four_values() -> None:
    from varco_fastapi.posture import PostureSeverity

    assert {s.value for s in PostureSeverity} == {"info", "warn", "high", "not_assessed"}


def test_settings_default_environment_is_production(monkeypatch) -> None:
    from varco_fastapi.posture import SecurityPostureSettings

    monkeypatch.delenv("VARCO_SECURITY_ENV", raising=False)
    settings = SecurityPostureSettings()

    assert settings.environment == "production"


def test_settings_default_enforce_is_warn(monkeypatch) -> None:
    from varco_fastapi.posture import SecurityPostureSettings

    monkeypatch.delenv("VARCO_SECURITY_ENFORCE", raising=False)
    settings = SecurityPostureSettings()

    assert settings.enforce == "warn"


async def test_development_demotes_warn_findings_to_info() -> None:
    """§D-S9-oq1: development changes presentation only. A WARN finding
    must be demoted to INFO in development, and the check must still have
    run (the finding is still present)."""
    from varco_fastapi.posture import (
        PostureFinding,
        PostureSeverity,
        SecurityPosture,
        SecurityPostureLifecycle,
        SecurityPostureSettings,
    )

    def _collector() -> list[PostureFinding]:
        return [
            PostureFinding(
                check="test.some_warn",
                severity=PostureSeverity.WARN,
                detail="observed",
                remediation="fix it",
            )
        ]

    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector],
        settings=SecurityPostureSettings(environment="development"),
    )
    await lifecycle.start()

    report: SecurityPosture = lifecycle.report
    finding = next(f for f in report.findings if f.check == "test.some_warn")
    assert finding.severity == PostureSeverity.INFO


async def test_suppression_demotes_but_never_removes_the_finding() -> None:
    """§D-S9-suppress: a suppressed finding is still produced, with
    suppressed=True — never dropped from the report."""
    from varco_fastapi.posture import (
        PostureFinding,
        PostureSeverity,
        SecurityPostureLifecycle,
        SecurityPostureSettings,
    )

    def _collector() -> list[PostureFinding]:
        return [
            PostureFinding(
                check="test.suppress_me",
                severity=PostureSeverity.HIGH,
                detail="observed",
                remediation="fix it",
            )
        ]

    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector],
        settings=SecurityPostureSettings(suppress="test.suppress_me"),
    )
    await lifecycle.start()

    finding = next(f for f in lifecycle.report.findings if f.check == "test.suppress_me")
    assert finding.suppressed is True
    assert finding.severity == PostureSeverity.INFO


async def test_unknown_suppression_id_yields_info_finding() -> None:
    """§D-S9-suppress: an unknown id in VARCO_SECURITY_SUPPRESS is a typo
    that silently suppresses nothing — surfaced as
    posture.unknown_suppression at INFO."""
    from varco_fastapi.posture import (
        PostureSeverity,
        SecurityPostureLifecycle,
        SecurityPostureSettings,
    )

    lifecycle = SecurityPostureLifecycle(
        collectors=[],
        settings=SecurityPostureSettings(suppress="no.such.check.id"),
    )
    await lifecycle.start()

    finding = next(f for f in lifecycle.report.findings if f.check == "posture.unknown_suppression")
    assert finding.severity == PostureSeverity.INFO
    assert "no.such.check.id" in finding.detail


async def test_enforce_refuse_raises_on_high_finding() -> None:
    from varco_fastapi.posture import (
        PostureFinding,
        PostureSeverity,
        SecurityPostureLifecycle,
        SecurityPostureSettings,
    )

    def _collector() -> list[PostureFinding]:
        return [
            PostureFinding(
                check="test.dangerous",
                severity=PostureSeverity.HIGH,
                detail="observed",
                remediation="fix it",
            )
        ]

    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector],
        settings=SecurityPostureSettings(enforce="refuse"),
    )

    with pytest.raises(Exception):  # noqa: B017 — exact exception type is an impl detail here
        await lifecycle.start()


async def test_enforce_refuse_never_raises_on_not_assessed_only() -> None:
    """§D-S9-enforce: NOT_ASSESSED never blocks startup, even under
    enforce=refuse — "could not check" is not evidence of a problem."""
    from varco_fastapi.posture import (
        PostureFinding,
        PostureSeverity,
        SecurityPostureLifecycle,
        SecurityPostureSettings,
    )

    def _collector() -> list[PostureFinding]:
        return [
            PostureFinding(
                check="test.not_assessed",
                severity=PostureSeverity.NOT_ASSESSED,
                detail="could not check",
                remediation="install the sibling",
            )
        ]

    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector],
        settings=SecurityPostureSettings(enforce="refuse"),
    )

    # Must return normally — no exception.
    await lifecycle.start()
    assert lifecycle.report.worst() == PostureSeverity.NOT_ASSESSED
