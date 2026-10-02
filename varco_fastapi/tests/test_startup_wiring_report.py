"""
Red-mode tests for Plan 052 Step 8 — the lifespan runs
``run_wiring_preflight()`` after ``async_init``, before the first
``start()`` (§D-startup). Docker-free.

None of ``app.state.varco_wiring_report``/``varco_container``/``varco_modules``
and none of ``varco_core.di.diagnose.run_wiring_preflight`` exist yet — every
test here is expected to fail with an ``AttributeError``/``ImportError`` or
an assertion on the current (pre-052) behaviour until Plan 052 Step 8 lands.
"""

from __future__ import annotations

import logging

import pytest
from providify import DIContainer, Inject, Singleton
from varco_core.di import CoreModule, bind_instance
from varco_core.di.module import VarcoModule
from varco_fastapi.app import create_varco_app
from varco_fastapi.auth.server_auth import AbstractServerAuth, AnonymousAuth
from varco_fastapi.di import bootstrap as fastapi_bootstrap
from varco_fastapi.validation import ConfigurationError


def _container_with_auth() -> DIContainer:
    container = DIContainer()
    fastapi_bootstrap(container)
    bind_instance(container, AbstractServerAuth, AnonymousAuth())
    return container


class _UnboundDep:
    """A deliberately never-bound interface, module scope so providify's
    annotation resolver (which needs a runtime-resolvable name, not a
    locally-scoped class, under `from __future__ import annotations`) can
    see it."""


@Singleton
class _NeedsUnboundDep:
    def __init__(self, dep: Inject[_UnboundDep]) -> None:
        self.dep = dep


class _UnboundDep2:
    """Ditto, for the validate=False case (a distinct interface so the two
    tests' containers cannot accidentally satisfy each other)."""


@Singleton
class _NeedsUnboundDep2:
    def __init__(self, dep: Inject[_UnboundDep2]) -> None:
        self.dep = dep


def _app(modules: list[VarcoModule], **kwargs: object):
    container = _container_with_auth()
    return create_varco_app(container, modules=modules, **kwargs)


async def test_a_default_app_logs_wiring_report_at_info(caplog: pytest.LogCaptureFixture) -> None:
    app = _app([CoreModule()], routers=[])

    with caplog.at_level(logging.INFO, logger="varco_core.di.diagnose"):
        async with app.router.lifespan_context(app):
            pass

    records = [r for r in caplog.records if r.name == "varco_core.di.diagnose"]
    assert any("varco wiring report" in r.getMessage() for r in records)
    assert any("errors" in r.getMessage().lower() for r in records)


async def test_b_startup_report_opt_out_suppresses_the_block_but_not_problem_lines(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The opt-out silences the rendered block, but a WARNING finding is
    # *still* emitted as its own record (§D-startup) — this is the half of
    # the behaviour that cannot pass trivially before Step 8 lands, since it
    # requires the driver to actually compute and log a WARNING finding.
    class _WarningModule(VarcoModule):
        name = "warns_for_opt_out_test"

        def _register(self, container: DIContainer) -> None:
            pass

        def validate(self, container: DIContainer) -> list[str]:
            return ["a soft warning for the opt-out test"]

    monkeypatch.setenv("VARCO_DIAGNOSE_STARTUP_REPORT", "false")
    app = _app([CoreModule(), _WarningModule()], routers=[])

    with caplog.at_level(logging.INFO, logger="varco_core.di.diagnose"):
        async with app.router.lifespan_context(app):
            pass

    records = [r for r in caplog.records if r.name == "varco_core.di.diagnose"]
    assert not any("varco wiring report" in r.getMessage() for r in records)
    assert any(
        r.levelno == logging.WARNING and "a soft warning for the opt-out test" in r.getMessage()
        for r in records
    )


async def test_d_container_with_missing_binding_raises_configuration_error() -> None:
    container = _container_with_auth()
    container.bind(_NeedsUnboundDep, _NeedsUnboundDep)
    app = create_varco_app(container, modules=[CoreModule()], routers=[])

    with pytest.raises(ConfigurationError):
        async with app.router.lifespan_context(app):
            pass


async def test_d_validate_false_boots_despite_missing_binding_but_still_builds_report() -> None:
    container = _container_with_auth()
    container.bind(_NeedsUnboundDep2, _NeedsUnboundDep2)
    app = create_varco_app(container, modules=[CoreModule()], routers=[], validate=False)

    async with app.router.lifespan_context(app):
        pass  # must not raise despite the missing binding

    # "validation.ran is False; the report is still built and logged" (§D-startup).
    assert app.state.varco_wiring_report.validation.ran is False


async def test_e_warning_only_boots_by_default_but_refuses_with_strict_validation() -> None:
    class _WarningModule(VarcoModule):
        name = "warns"

        def _register(self, container: DIContainer) -> None:
            pass

        def validate(self, container: DIContainer) -> list[str]:
            return ["a soft warning"]

    lenient_app = _app([CoreModule(), _WarningModule()], routers=[])
    async with lenient_app.router.lifespan_context(lenient_app):
        pass  # must not raise — WARNING tier does not block by default

    strict_container = _container_with_auth()
    strict_app = create_varco_app(
        strict_container,
        modules=[CoreModule(), _WarningModule()],
        routers=[],
        strict_validation=True,
    )
    with pytest.raises(ConfigurationError):
        async with strict_app.router.lifespan_context(strict_app):
            pass


async def test_g_report_is_set_on_app_state_after_lifespan_entered() -> None:
    # The report is built before the first start() (§D-startup) — checking
    # it is non-None once the lifespan has entered is the externally
    # observable half of that ordering guarantee from outside the module.
    app = _app([CoreModule()], routers=[])

    async with app.router.lifespan_context(app):
        assert app.state.varco_wiring_report is not None


async def test_app_state_varco_container_and_modules_are_set() -> None:
    container = _container_with_auth()
    app = create_varco_app(container, modules=[CoreModule()], routers=[])

    async with app.router.lifespan_context(app):
        pass

    assert app.state.varco_container is container
    assert app.state.varco_modules is not None


async def test_startup_report_header_carries_real_posture_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("VARCO_SECURITY_ENV", "staging")
    monkeypatch.setenv("VARCO_SECURITY_ENFORCE", "refuse")
    app = _app([CoreModule()], routers=[])

    async with app.router.lifespan_context(app):
        report = app.state.varco_wiring_report

    assert (report.environment, report.enforce, report.enforce_source) == (
        "staging",
        "refuse",
        "env",
    )


async def test_startup_deprecation_capture_window_around_async_init(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import warnings

    class _WarnsInAsyncInit(VarcoModule):
        name = "warns_in_async_init"

        def _register(self, container: DIContainer) -> None:
            pass

        async def async_init(self, container: DIContainer) -> None:
            warnings.warn("startup-only old api", DeprecationWarning, stacklevel=2)

    app = _app([CoreModule(), _WarnsInAsyncInit()], routers=[])

    async with app.router.lifespan_context(app):
        report = app.state.varco_wiring_report

    assert any("startup-only old api" in d.subject for d in report.deprecations)
    assert any(
        f.check == "deprecation.fired" and "startup-only old api" in f.detail
        for f in report.findings
    )


async def test_startup_unowned_settings_is_a_logged_finding(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("VARCO_MIGRATE_MODE", "check")
    app = _app([CoreModule()], routers=[])

    with caplog.at_level(logging.WARNING, logger="varco_core.di.diagnose"):
        async with app.router.lifespan_context(app):
            report = app.state.varco_wiring_report

    assert any(f.check == "settings.unowned" for f in report.findings)
    assert any("VARCO_MIGRATE_MODE" in r.getMessage() for r in caplog.records)
