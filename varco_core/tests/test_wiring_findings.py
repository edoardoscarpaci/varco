"""
Red-mode tests for Plan 052 Step 5 — the "bound but not wired" finding
producers in ``varco_core.di.report`` (§D-wired).

``build_wiring_report`` does not exist yet (Step 4), so every test here is
expected to fail with an ``ImportError`` until Plan 052 lands the module.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_core.authority.registry import TrustedIssuerRegistry
from varco_core.di import CoreModule, bind_instance, bootstrap
from varco_core.di.report import build_wiring_report  # noqa: F401
from varco_core.revocation.base import AbstractTokenRevocationStore
from varco_core.revocation.memory import InMemoryTokenRevocationStore


@pytest.fixture
def bootstrapped_container() -> DIContainer:
    c = DIContainer()
    bootstrap(c)
    CoreModule().register(c)
    return c


def test_a_store_bound_without_registry_wiring_yields_warning_finding(
    bootstrapped_container: DIContainer,
) -> None:
    bind_instance(
        bootstrapped_container,
        AbstractTokenRevocationStore,
        InMemoryTokenRevocationStore(),
    )
    # A registry that never received revocation_store=...
    bind_instance(bootstrapped_container, TrustedIssuerRegistry, TrustedIssuerRegistry())
    validation = bootstrapped_container.validate(raise_on_error=False)

    report = build_wiring_report(bootstrapped_container, validation=validation)

    finding = next(f for f in report.findings if f.check == "di.revocation_store_not_wired")
    assert "TrustedIssuerRegistry(revocation_store=" in finding.remediation


def test_a_store_wired_into_registry_yields_no_finding(
    bootstrapped_container: DIContainer,
) -> None:
    store = InMemoryTokenRevocationStore()
    bind_instance(bootstrapped_container, AbstractTokenRevocationStore, store)
    bind_instance(
        bootstrapped_container,
        TrustedIssuerRegistry,
        TrustedIssuerRegistry(revocation_store=store),
    )
    validation = bootstrapped_container.validate(raise_on_error=False)

    report = build_wiring_report(bootstrapped_container, validation=validation)

    assert not any(f.check == "di.revocation_store_not_wired" for f in report.findings)


def test_a_no_registry_bound_yields_no_finding(bootstrapped_container: DIContainer) -> None:
    bind_instance(
        bootstrapped_container,
        AbstractTokenRevocationStore,
        InMemoryTokenRevocationStore(),
    )
    validation = bootstrapped_container.validate(raise_on_error=False)

    report = build_wiring_report(bootstrapped_container, validation=validation)

    assert not any(f.check == "di.revocation_store_not_wired" for f in report.findings)


def test_c_module_validate_message_becomes_warning_finding() -> None:
    from varco_core.di.module import VarcoModule

    class _NoisyModule(VarcoModule):
        name = "noisy"

        def _register(self, container: DIContainer) -> None:
            pass

        def validate(self, container: DIContainer) -> list[str]:
            return ["something is misconfigured"]

    c = DIContainer()
    bootstrap(c)
    CoreModule().register(c)
    module = _NoisyModule()
    module.register(c)
    validation = c.validate(raise_on_error=False)

    report = build_wiring_report(
        c, validation=validation, module_validate_messages={module: ["something is misconfigured"]}
    )

    finding = next(f for f in report.findings if f.check == "module.noisy.validate")
    assert finding.detail == "something is misconfigured"


# ── Drift repair: settings.unowned, di.bootstrapped_unselected, deprecation.fired ──


def _modules_named(*names: str) -> list:
    from varco_core.di.module import VarcoModule

    mods = []
    for n in names:

        class _M(VarcoModule):
            name = n

            def _register(self, container: DIContainer) -> None:
                pass

        mods.append(_M())
    return mods


def test_b_unowned_migrate_mode_yields_warning(
    bootstrapped_container: DIContainer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VARCO_MIGRATE_MODE", "check")

    report = build_wiring_report(bootstrapped_container, validation=None, modules=[])

    finding = next(f for f in report.findings if f.check == "settings.unowned")
    assert "VARCO_MIGRATE_MODE" in finding.detail
    assert str(finding.severity) == "warning"


def test_b_owned_migrate_mode_and_tenancy_yield_no_finding(
    bootstrapped_container: DIContainer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VARCO_MIGRATE_MODE", "check")
    monkeypatch.setenv("VARCO_TENANCY_ISOLATION", "schema")
    mods = _modules_named("migrations", "tenancy")
    for m in mods:
        m.register(bootstrapped_container)

    report = build_wiring_report(bootstrapped_container, validation=None, modules=mods)

    assert not any(f.check == "settings.unowned" for f in report.findings)


def test_b_unowned_tenancy_isolation_yields_warning(
    bootstrapped_container: DIContainer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("VARCO_MIGRATE_MODE", raising=False)
    monkeypatch.setenv("VARCO_TENANCY_ISOLATION", "schema")

    report = build_wiring_report(bootstrapped_container, validation=None, modules=[])

    finding = next(f for f in report.findings if f.check == "settings.unowned")
    assert "VARCO_TENANCY_ISOLATION" in finding.detail


def test_d_bootstrapped_but_unselected_is_a_finding_and_logs_once(
    bootstrapped_container: DIContainer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    from varco_core.di import select

    monkeypatch.setattr(select, "_REGISTERED_VALUES", {"job_store": {"kafka"}})
    monkeypatch.delenv("VARCO_JOB_STORE_BACKEND", raising=False)

    with caplog.at_level(logging.WARNING, logger="varco_core.di.select"):
        report = build_wiring_report(bootstrapped_container, validation=None)

    finding = next(f for f in report.findings if f.check == "di.bootstrapped_unselected")
    assert "VARCO_JOB_STORE_BACKEND" in finding.remediation
    # The report path uses the non-logging evaluator — no duplicate WARNING.
    assert not [r for r in caplog.records if "VARCO_JOB_STORE_BACKEND" in r.message]


def test_d_backend_selection_typo_propagates_out_of_the_builder(
    bootstrapped_container: DIContainer, monkeypatch: pytest.MonkeyPatch
) -> None:
    from varco_core.di.errors import BackendSelectionError

    monkeypatch.setenv("VARCO_EVENT_BUS_BACKEND", "nope")

    with pytest.raises(BackendSelectionError):
        build_wiring_report(bootstrapped_container, validation=None)


def test_deprecation_fired_info_finding_per_record(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.deprecation import DeprecationRecord, reset_fired_deprecations

    reset_fired_deprecations()
    extra = DeprecationRecord(
        subject="old_thing is deprecated",
        since="3.0",
        removed_in="4.0",
        replacement="new_thing",
        count=1,
        first_location="x.py:1",
    )

    report = build_wiring_report(
        bootstrapped_container, validation=None, extra_deprecations=[extra]
    )

    finding = next(f for f in report.findings if f.check == "deprecation.fired")
    assert str(finding.severity) == "info"
    assert "old_thing" in finding.detail
    assert finding.remediation == "new_thing"
    assert extra in report.deprecations


def test_header_fields_are_supplied_by_the_caller(bootstrapped_container: DIContainer) -> None:
    default = build_wiring_report(bootstrapped_container, validation=None)
    assert (default.environment, default.enforce, default.enforce_source) == (
        "unknown",
        "unknown",
        "default",
    )

    report = build_wiring_report(
        bootstrapped_container,
        validation=None,
        environment="production",
        enforce="refuse",
        enforce_source="env",
    )

    assert (report.environment, report.enforce, report.enforce_source) == (
        "production",
        "refuse",
        "env",
    )
    assert "environment=production" in report.render()
    assert report.to_dict()["enforce"] == "refuse"
