"""
Plan 047 / Step 12 — `varco_fastapi.posture._collect_di(container)` (§D-posture).

For each `SeamSelection` with `value in {"memory"}` (never `null` — that's
034's revocation finding), emits `PostureFinding(check="di.in_process_backend",
severity=WARN, ...)`. `_collect_di` itself never demotes — like every other
collector in this module, severity demotion for a non-production
environment is applied centrally by `SecurityPostureLifecycle.start()`,
which reads `SecurityPostureSettings.environment` (`VARCO_SECURITY_ENV`).

REPAIR (round 1): the collector previously read a second, undocumented,
never-introduced environment variable directly, pre-demoting its own
finding independently of the central `VARCO_SECURITY_ENV` mechanism. That
let a deployment with that stray variable set to a development-like value
silently demote this one finding to INFO while `VARCO_SECURITY_ENV` (and
therefore every other finding) stayed at the strict `"production"` default
— defeating `enforce="refuse"` for this finding alone. Fixed: `_collect_di`
always emits raw `WARN`; only `SecurityPostureLifecycle` demotes, and only
via `VARCO_SECURITY_ENV`.
"""

from __future__ import annotations

import pytest
from providify import DIContainer


@pytest.fixture
def bootstrapped_container(monkeypatch: pytest.MonkeyPatch) -> DIContainer:
    from varco_core.di import bootstrap

    monkeypatch.delenv("VARCO_EVENT_BUS_BACKEND", raising=False)
    container = DIContainer()
    bootstrap(container)
    return container


def test_in_memory_bus_produces_one_warn_finding(
    bootstrapped_container: DIContainer,
) -> None:
    """`_collect_di` always emits raw WARN — it never reads any
    environment variable itself."""
    from varco_fastapi.posture import PostureSeverity, _collect_di

    findings = _collect_di(bootstrapped_container)

    matching = [f for f in findings if f.check == "di.in_process_backend"]
    assert matching, "expected a di.in_process_backend finding"
    assert all(f.severity == PostureSeverity.WARN for f in matching)


def test_explicit_memory_value_still_produces_a_finding_with_env_source(
    bootstrapped_container: DIContainer, monkeypatch: pytest.MonkeyPatch
) -> None:
    from varco_fastapi.posture import _collect_di

    monkeypatch.setenv("VARCO_EVENT_BUS_BACKEND", "memory")
    findings = _collect_di(bootstrapped_container)

    matching = [f for f in findings if f.check == "di.in_process_backend"]
    assert matching
    assert any("env" in f.detail for f in matching)


def test_code_bound_bus_produces_no_finding(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.di import bind_instance
    from varco_core.event.base import AbstractEventBus
    from varco_core.event.memory import InMemoryEventBus
    from varco_fastapi.posture import _collect_di

    bind_instance(bootstrapped_container, AbstractEventBus, InMemoryEventBus())

    findings = _collect_di(bootstrapped_container)
    matching = [
        f for f in findings if f.check == "di.in_process_backend" and "event_bus" in f.detail
    ]
    assert matching == []


def test_revocation_null_is_never_reported_by_this_collector(
    bootstrapped_container: DIContainer,
) -> None:
    # revocation's default is "null", not "memory" — that finding belongs
    # to 034's inspect_revocation_posture(), never duplicated here.
    from varco_fastapi.posture import _collect_di

    findings = _collect_di(bootstrapped_container)

    matching = [
        f for f in findings if f.check == "di.in_process_backend" and "revocation" in f.detail
    ]
    assert matching == []


def test_collector_is_wired_into_security_posture_lifecycle(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_fastapi.posture import SecurityPostureLifecycle, _collect_di

    lifecycle = SecurityPostureLifecycle(
        collectors=[lambda: _collect_di(bootstrapped_container)],
    )
    assert lifecycle is not None


async def test_lifecycle_demotes_to_info_in_development_via_security_env(
    bootstrapped_container: DIContainer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The demotion path is the central one, keyed on `VARCO_SECURITY_ENV`
    (`SecurityPostureSettings.environment`) — never on any second env var
    read by the collector itself."""
    from varco_fastapi.posture import PostureSeverity, SecurityPostureLifecycle, _collect_di

    monkeypatch.setenv("VARCO_SECURITY_ENV", "development")
    lifecycle = SecurityPostureLifecycle(
        collectors=[lambda: _collect_di(bootstrapped_container)],
    )
    await lifecycle.start()

    matching = [f for f in lifecycle.report.findings if f.check == "di.in_process_backend"]
    assert matching
    assert all(f.severity != PostureSeverity.WARN for f in matching)


async def test_lifecycle_stays_warn_in_production_via_security_env(
    bootstrapped_container: DIContainer, monkeypatch: pytest.MonkeyPatch
) -> None:
    from varco_fastapi.posture import PostureSeverity, SecurityPostureLifecycle, _collect_di

    monkeypatch.setenv("VARCO_SECURITY_ENV", "production")
    lifecycle = SecurityPostureLifecycle(
        collectors=[lambda: _collect_di(bootstrapped_container)],
    )
    await lifecycle.start()

    matching = [f for f in lifecycle.report.findings if f.check == "di.in_process_backend"]
    assert matching
    assert any(f.severity == PostureSeverity.WARN for f in matching)


async def test_no_env_var_other_than_varco_security_env_can_demote_the_finding(
    bootstrapped_container: DIContainer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression guard for a prior bug where `_collect_di` read a second,
    undocumented environment variable directly and pre-demoted its own
    finding independently of `SecurityPostureSettings.environment`. With
    `VARCO_SECURITY_ENV` left at its strict `"production"` default, no
    other environment variable — however development-sounding — may
    demote the finding; only `SecurityPostureLifecycle`, reading
    `VARCO_SECURITY_ENV`, is ever allowed to."""
    from varco_fastapi.posture import PostureSeverity, SecurityPostureLifecycle, _collect_di

    monkeypatch.delenv("VARCO_SECURITY_ENV", raising=False)
    monkeypatch.setenv("VARCO_APP_STAGE", "development")
    monkeypatch.setenv("ENVIRONMENT", "development")
    lifecycle = SecurityPostureLifecycle(
        collectors=[lambda: _collect_di(bootstrapped_container)],
    )
    await lifecycle.start()

    matching = [f for f in lifecycle.report.findings if f.check == "di.in_process_backend"]
    assert matching
    assert any(f.severity == PostureSeverity.WARN for f in matching)
