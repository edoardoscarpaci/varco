"""
Red-mode tests for Plan 052 Step 4 — ``varco_core.di.report.build_wiring_report``
and ``WiringReport`` (§D-model/§D-provenance).

``varco_core.di.report`` does not exist yet, so every test here is expected
to fail with an ``ImportError`` until Plan 052 Step 4 lands the module.
"""

from __future__ import annotations

import json

import pytest
from providify import DIContainer
from varco_core.di import CoreModule, bind_instance, bootstrap

# Primary red-mode probe: this module does not exist yet.
from varco_core.di.report import (  # noqa: F401
    Severity,
    WiringFinding,
    WiringReport,
    build_wiring_report,
)
from varco_core.event import AbstractEventBus, InMemoryEventBus


class _FakeBus(InMemoryEventBus):
    """A distinguishable non-fallback event bus for the shadowing case."""


@pytest.fixture
def bootstrapped_container() -> DIContainer:
    c = DIContainer()
    bootstrap(c)
    CoreModule().register(c)
    return c


def test_a_default_container_every_seam_is_default_with_no_shadowing(
    bootstrapped_container: DIContainer,
) -> None:
    validation = bootstrapped_container.validate(raise_on_error=False)
    report = build_wiring_report(bootstrapped_container, validation=validation)

    assert isinstance(report, WiringReport)
    assert len(report.seams) >= 1
    event_bus_row = next(row for row in report.seams if row.name == "event_bus")
    assert event_bus_row.source == "default"
    assert event_bus_row.live is not None
    assert event_bus_row.live.fallback is True
    assert event_bus_row.shadowed == ()


def test_b_code_bound_bus_shadows_the_fallback_default(
    bootstrapped_container: DIContainer,
) -> None:
    bind_instance(bootstrapped_container, AbstractEventBus, _FakeBus())
    validation = bootstrapped_container.validate(raise_on_error=False)

    report = build_wiring_report(bootstrapped_container, validation=validation)

    event_bus_row = next(row for row in report.seams if row.name == "event_bus")
    assert event_bus_row.source == "code"
    assert any("InMemoryEventBus" in b.implementation for b in event_bus_row.shadowed)


def test_c_env_selects_backend_and_source_is_env(
    bootstrapped_container: DIContainer, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("VARCO_EVENT_BUS_BACKEND", "memory")
    validation = bootstrapped_container.validate(raise_on_error=False)

    report = build_wiring_report(bootstrapped_container, validation=validation)

    event_bus_row = next(row for row in report.seams if row.name == "event_bus")
    assert event_bus_row.source == "env"
    assert event_bus_row.env_value == "memory"


def test_e_feature_flags_default_disabled(bootstrapped_container: DIContainer) -> None:
    validation = bootstrapped_container.validate(raise_on_error=False)
    report = build_wiring_report(bootstrapped_container, validation=validation)

    flags_row = next(row for row in report.features if row.name == "feature_flags")
    assert flags_row.enabled is False
    assert flags_row.source == "default"


def test_e_enable_feature_flags_flips_source_to_code(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.flags.di import enable_feature_flags

    enable_feature_flags(bootstrapped_container)
    validation = bootstrapped_container.validate(raise_on_error=False)

    report = build_wiring_report(bootstrapped_container, validation=validation)

    flags_row = next(row for row in report.features if row.name == "feature_flags")
    assert flags_row.source == "code"
    assert flags_row.enabled is True


def test_f_other_defaults_covers_every_catalogue_entry(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.di import catalogue

    validation = bootstrapped_container.validate(raise_on_error=False)
    report = build_wiring_report(bootstrapped_container, validation=validation)

    cat = catalogue(bootstrapped_container)
    assert len(report.other_defaults) == len(cat.other_defaults)


def test_h_core_module_status_present_with_default_enabled_source(
    bootstrapped_container: DIContainer,
) -> None:
    validation = bootstrapped_container.validate(raise_on_error=False)
    report = build_wiring_report(bootstrapped_container, validation=validation)

    core_row = next(m for m in report.modules if m.name == "core")
    assert core_row.enabled_source == "default"


def test_h_explicitly_disabled_module_reports_enabled_source_code() -> None:
    c = DIContainer()
    bootstrap(c)
    CoreModule(enabled=False).register(c)
    validation = c.validate(raise_on_error=False)

    report = build_wiring_report(c, validation=validation)

    core_row = next(m for m in report.modules if m.name == "core")
    assert core_row.enabled_source == "code"
    assert core_row.enabled is False


def test_j_validation_ran_true_with_no_errors_on_clean_container(
    bootstrapped_container: DIContainer,
) -> None:
    validation = bootstrapped_container.validate(raise_on_error=False)
    report = build_wiring_report(bootstrapped_container, validation=validation)

    assert report.validation.ran is True
    assert report.validation.errors == ()


def test_j_validation_none_means_ran_false(bootstrapped_container: DIContainer) -> None:
    report = build_wiring_report(bootstrapped_container, validation=None)

    assert report.validation.ran is False


def test_l_to_dict_round_trips_through_json_and_embeds_catalogue(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.di import catalogue

    validation = bootstrapped_container.validate(raise_on_error=False)
    report = build_wiring_report(bootstrapped_container, validation=validation)

    as_dict = report.to_dict()
    json.dumps(as_dict)  # must not raise

    assert as_dict["schema_version"] == 1
    assert as_dict["catalogue"] == catalogue(bootstrapped_container).to_dict()


def test_m_render_contains_every_section_header_and_event_bus_line(
    bootstrapped_container: DIContainer,
) -> None:
    validation = bootstrapped_container.validate(raise_on_error=False)
    report = build_wiring_report(bootstrapped_container, validation=validation)

    rendered = report.render()

    for header in (
        "Modules",
        "Seams",
        "Features",
        "Settings",
        "Validation",
    ):
        assert header in rendered
    assert "event_bus" in rendered


def test_n_worst_is_none_for_a_clean_container(
    bootstrapped_container: DIContainer, monkeypatch: pytest.MonkeyPatch
) -> None:
    from varco_core.deprecation import reset_fired_deprecations
    from varco_core.di import select

    # Process-global state other tests leave behind: backend values registered
    # at import time (bootstrapped-but-unselected finding) and the deprecation
    # ledger (INFO findings). Isolate both so "clean" means this container.
    monkeypatch.setattr(select, "_REGISTERED_VALUES", {})
    reset_fired_deprecations()
    validation = bootstrapped_container.validate(raise_on_error=False)
    report = build_wiring_report(bootstrapped_container, validation=validation)

    assert report.worst() is None


def test_n_worst_reflects_an_injected_warning_finding(
    bootstrapped_container: DIContainer,
) -> None:
    validation = bootstrapped_container.validate(raise_on_error=False)
    finding = WiringFinding(
        check="test.injected",
        severity=Severity.WARNING,
        detail="injected for the test",
        remediation="n/a",
    )

    report = build_wiring_report(
        bootstrapped_container, validation=validation, extra_findings=(finding,)
    )

    assert report.worst() == Severity.WARNING


def test_d_profile_bound_bus_reports_source_profile() -> None:
    from providify import Profile, Singleton

    @Profile("test")
    @Singleton
    class _ProfileBus(InMemoryEventBus):
        pass

    c = DIContainer()
    bootstrap(c)
    CoreModule().register(c)
    c.provide(_ProfileBus, returns=AbstractEventBus)
    c.activate_profile("test")
    validation = c.validate(raise_on_error=False)

    report = build_wiring_report(c, validation=validation)

    event_bus_row = next(row for row in report.seams if row.name == "event_bus")
    assert event_bus_row.source == "profile"


def test_g_collections_has_one_row_per_collection_seam_with_contributors() -> None:
    from varco_core.di.collect import collection_seams, contribute
    from varco_core.posture import PostureCollector

    class _StubCollector(PostureCollector):
        name = "stub"

        def collect(self) -> list:
            return []

    c = DIContainer()
    bootstrap(c)
    CoreModule().register(c)
    contribute(c, PostureCollector, _StubCollector())
    validation = c.validate(raise_on_error=False)

    report = build_wiring_report(c, validation=validation)

    assert len(report.collections) == len(collection_seams())
    posture_row = next(row for row in report.collections if "PostureCollector" in row.interface)
    assert len(posture_row.contributors) >= 1


def test_k_deprecations_contains_a_record_after_a_deprecated_call() -> None:
    import warnings

    from varco_core.deprecation import deprecated, reset_fired_deprecations

    reset_fired_deprecations()

    @deprecated(since="3.0.0", removed_in="5.0.0")
    def _old() -> None:
        return None

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        _old()

    c = DIContainer()
    bootstrap(c)
    CoreModule().register(c)
    validation = c.validate(raise_on_error=False)

    report = build_wiring_report(c, validation=validation)

    assert len(report.deprecations) >= 1
    reset_fired_deprecations()
