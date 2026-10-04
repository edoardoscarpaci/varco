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


def test_settings_default_enforce_is_none_and_derived(monkeypatch) -> None:
    """Plan 054 / FLIP-2 (§D-flip2-default) — replaces the 3.x
    ``test_settings_default_enforce_is_warn``: with both variables unset the
    raw field is the ``None`` sentinel and the effective value derives
    ``"refuse"``."""
    from varco_fastapi.posture import SecurityPostureSettings

    monkeypatch.delenv("VARCO_SECURITY_ENFORCE", raising=False)
    monkeypatch.delenv("VARCO_SECURITY_ENV", raising=False)
    settings = SecurityPostureSettings()

    assert settings.enforce is None
    assert settings.effective_enforce == "refuse"
    assert settings.enforce_is_derived is True


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
        settings=SecurityPostureSettings(environment="development", enforce="warn"),
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
        settings=SecurityPostureSettings(suppress="test.suppress_me", enforce="warn"),
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
        settings=SecurityPostureSettings(suppress="no.such.check.id", enforce="warn"),
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


# ── Plan 054 / FLIP-2 — derived `enforce` default (§D-flip2-default/-env/-message/-report) ──
#
# Every test below deletes both variables first (§D-flip2-tests): the
# package conftest will gain an autouse `VARCO_SECURITY_ENFORCE=warn`
# (`_posture_warn_only`), and these are the tests that prove the flip.


def _clear_posture_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VARCO_SECURITY_ENFORCE", raising=False)
    monkeypatch.delenv("VARCO_SECURITY_ENV", raising=False)
    monkeypatch.delenv("VARCO_SECURITY_SUPPRESS", raising=False)


def _collector_of(check: str, severity_name: str):
    from varco_fastapi.posture import PostureFinding, PostureSeverity

    severity = PostureSeverity[severity_name]

    def _collector() -> list[PostureFinding]:
        return [
            PostureFinding(
                check=check,
                severity=severity,
                detail="observed",
                remediation="fix it",
            )
        ]

    _collector.__name__ = f"collector_{check.replace('.', '_')}"
    return _collector


def test_development_derives_warn(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_fastapi.posture import SecurityPostureSettings

    _clear_posture_env(monkeypatch)
    monkeypatch.setenv("VARCO_SECURITY_ENV", "development")
    settings = SecurityPostureSettings()

    assert settings.effective_enforce == "warn"
    assert settings.enforce_is_derived is True


def test_explicit_env_enforce_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_fastapi.posture import SecurityPostureSettings

    _clear_posture_env(monkeypatch)
    monkeypatch.setenv("VARCO_SECURITY_ENV", "production")
    monkeypatch.setenv("VARCO_SECURITY_ENFORCE", "warn")
    settings = SecurityPostureSettings()

    assert settings.effective_enforce == "warn"
    assert settings.enforce_is_derived is False


def test_code_enforce_beats_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # 048 precedence: an explicit code value beats the derivation even in development.
    from varco_fastapi.posture import SecurityPostureSettings

    _clear_posture_env(monkeypatch)
    settings = SecurityPostureSettings(enforce="refuse", environment="development")

    assert settings.effective_enforce == "refuse"


def test_unrecognised_environment_derives_refuse(monkeypatch: pytest.MonkeyPatch) -> None:
    # The strict direction is `!= "development"` — a typo is never more lenient.
    from varco_fastapi.posture import SecurityPostureSettings

    _clear_posture_env(monkeypatch)
    monkeypatch.setenv("VARCO_SECURITY_ENV", "staging")

    assert SecurityPostureSettings().effective_enforce == "refuse"


def test_enforce_field_default_is_none_signature_guard() -> None:
    # api_surface --check cannot see class signatures; this pins the default.
    from varco_fastapi.posture import SecurityPostureSettings

    assert SecurityPostureSettings.model_fields["enforce"].default is None


async def test_warn_finding_refuses_in_production_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from varco_fastapi.posture import SecurityPostureLifecycle, SecurityPostureSettings

    _clear_posture_env(monkeypatch)
    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector_of("test.some_warn", "WARN")],
        settings=SecurityPostureSettings(),
    )

    with pytest.raises(
        RuntimeError,
        match=r"enforce='refuse', derived from VARCO_SECURITY_ENV='production'",
    ) as exc:
        await lifecycle.start()
    assert "test.some_warn" in str(exc.value)
    assert "VARCO_SECURITY_ENFORCE=warn" in str(exc.value)


async def test_same_warn_finding_starts_in_development(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_fastapi.posture import (
        PostureSeverity,
        SecurityPostureLifecycle,
        SecurityPostureSettings,
    )

    _clear_posture_env(monkeypatch)
    monkeypatch.setenv("VARCO_SECURITY_ENV", "development")
    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector_of("test.some_warn", "WARN")],
        settings=SecurityPostureSettings(),
    )
    await lifecycle.start()

    finding = next(f for f in lifecycle.report.findings if f.check == "test.some_warn")
    assert finding.severity == PostureSeverity.INFO
    assert lifecycle.report.enforce == "warn"


async def test_env_warn_escape_hatch_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_fastapi.posture import (
        PostureSeverity,
        SecurityPostureLifecycle,
        SecurityPostureSettings,
    )

    _clear_posture_env(monkeypatch)
    monkeypatch.setenv("VARCO_SECURITY_ENFORCE", "warn")
    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector_of("test.some_warn", "WARN")],
        settings=SecurityPostureSettings(),
    )
    await lifecycle.start()

    finding = next(f for f in lifecycle.report.findings if f.check == "test.some_warn")
    assert finding.severity == PostureSeverity.WARN
    assert lifecycle.report.enforce == "warn"


async def test_suppressed_warn_starts(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_fastapi.posture import SecurityPostureLifecycle, SecurityPostureSettings

    _clear_posture_env(monkeypatch)
    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector_of("test.some_warn", "WARN")],
        settings=SecurityPostureSettings(suppress="test.some_warn"),
    )
    await lifecycle.start()

    # Derived refuse must be in force, or this test proves nothing.
    assert lifecycle.report.enforce == "refuse"


@pytest.mark.parametrize(
    ("environment", "refuses"),
    [("production", True), ("development", False), ("staging", True)],
)
async def test_high_refuses_except_in_derived_development(
    monkeypatch: pytest.MonkeyPatch, environment: str, refuses: bool
) -> None:
    # development derives `warn`, so even HIGH starts there (the dev setup stays startable).
    from varco_fastapi.posture import SecurityPostureLifecycle, SecurityPostureSettings

    _clear_posture_env(monkeypatch)
    monkeypatch.setenv("VARCO_SECURITY_ENV", environment)
    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector_of("test.dangerous", "HIGH")],
        settings=SecurityPostureSettings(),
    )

    if refuses:
        with pytest.raises(RuntimeError, match="test.dangerous"):
            await lifecycle.start()
    else:
        await lifecycle.start()
        assert lifecycle.report.enforce == "warn"


@pytest.mark.parametrize("environment", ["staging", "production", "development"])
async def test_staging_warn_does_not_block_but_is_not_demoted_and_is_reported(
    monkeypatch: pytest.MonkeyPatch, environment: str
) -> None:
    from varco_fastapi.posture import (
        PostureSeverity,
        SecurityPostureLifecycle,
        SecurityPostureSettings,
    )

    _clear_posture_env(monkeypatch)
    monkeypatch.setenv("VARCO_SECURITY_ENV", environment)
    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector_of("test.some_warn", "WARN")],
        # Explicit warn for production so the start itself succeeds and the
        # `unrecognised_environment` presence/absence is what is under test.
        settings=SecurityPostureSettings(enforce="warn")
        if environment == "production"
        else SecurityPostureSettings(),
    )
    await lifecycle.start()

    ids = {f.check: f for f in lifecycle.report.findings}
    if environment == "staging":
        assert lifecycle.report.enforce == "refuse"
        assert ids["test.some_warn"].severity == PostureSeverity.WARN
        unrecognised = ids["posture.unrecognised_environment"]
        assert unrecognised.severity == PostureSeverity.INFO
        assert "'staging'" in unrecognised.detail
    else:
        assert "posture.unrecognised_environment" not in ids


async def test_unrecognised_enforce_value_is_reported_and_treated_as_warn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # `Refuse` is never normalised; it is reported and treated as warn.
    from varco_fastapi.posture import (
        PostureSeverity,
        SecurityPostureLifecycle,
        SecurityPostureSettings,
    )

    _clear_posture_env(monkeypatch)
    monkeypatch.setenv("VARCO_SECURITY_ENFORCE", "Refuse")
    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector_of("test.dangerous", "HIGH")],
        settings=SecurityPostureSettings(),
    )
    await lifecycle.start()

    finding = next(
        (f for f in lifecycle.report.findings if f.check == "posture.unrecognised_enforce"),
        None,
    )
    assert finding is not None, "posture.unrecognised_enforce finding missing"
    assert finding.severity == PostureSeverity.INFO


async def test_not_assessed_never_blocks_under_derived_refuse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from varco_fastapi.posture import (
        PostureSeverity,
        SecurityPostureLifecycle,
        SecurityPostureSettings,
    )

    _clear_posture_env(monkeypatch)
    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector_of("test.not_assessed", "NOT_ASSESSED")],
        settings=SecurityPostureSettings(),
    )
    await lifecycle.start()

    assert lifecycle.report.worst() == PostureSeverity.NOT_ASSESSED
    # Must be the derived refuse that was in force — not a vacuous pass under warn.
    assert lifecycle.report.enforce == "refuse"


async def test_summary_line_names_enforce(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_fastapi.posture import SecurityPostureLifecycle, SecurityPostureSettings

    _clear_posture_env(monkeypatch)
    lifecycle = SecurityPostureLifecycle(collectors=[], settings=SecurityPostureSettings())
    await lifecycle.start()

    assert lifecycle.report.summary().endswith("enforce=refuse")


async def test_refuse_message_keeps_3_2_fragment(monkeypatch: pytest.MonkeyPatch) -> None:
    # Operator alert rules (and 049's module test) match on this fragment verbatim.
    from varco_fastapi.posture import SecurityPostureLifecycle, SecurityPostureSettings

    _clear_posture_env(monkeypatch)
    lifecycle = SecurityPostureLifecycle(
        collectors=[_collector_of("test.some_warn", "WARN")],
        settings=SecurityPostureSettings(),
    )

    with pytest.raises(RuntimeError) as exc:
        await lifecycle.start()
    assert "refusing to start — unsuppressed blocking findings:" in str(exc.value)
