"""
Plan 054 / Phase A, Step 4 — 052's startup preflight composed with FLIP-2's
posture refusal (§D-compose). Docker-free.

The two refusals are disjoint by construction: the wiring preflight runs
after ``async_init`` and before any ``start()``; the posture module is the
last ``start()``. One error, one log block — never both. The wiring report
header shows the *effective* ``enforce`` and its provenance, including the
new ``"derived"`` source.
"""

from __future__ import annotations

import logging

import pytest
from providify import DIContainer, Inject, Singleton
from varco_core.di import CoreModule, bind_instance
from varco_fastapi.app import create_varco_app
from varco_fastapi.auth.server_auth import AbstractServerAuth, AnonymousAuth
from varco_fastapi.di import bootstrap as fastapi_bootstrap
from varco_fastapi.modules import SecurityPostureModule
from varco_fastapi.validation import ConfigurationError

_HIGH_ID = "test.compose_high"


class _ComposeUnbound:
    """Never bound — module scope so providify can resolve the annotation."""


@Singleton
class _NeedsComposeUnbound:
    def __init__(self, dep: Inject[_ComposeUnbound]) -> None:
        self.dep = dep


def _high_collector():
    from varco_fastapi.posture import PostureFinding, PostureSeverity

    return [
        PostureFinding(
            check=_HIGH_ID,
            severity=PostureSeverity.HIGH,
            detail="forced for test",
            remediation="n/a",
        )
    ]


def _container() -> DIContainer:
    container = DIContainer()
    fastapi_bootstrap(container)
    bind_instance(container, AbstractServerAuth, AnonymousAuth())
    return container


@pytest.fixture(autouse=True)
def _clean_posture_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # §D-flip2-tests: opt out of the package-wide `_posture_warn_only` fixture.
    monkeypatch.delenv("VARCO_SECURITY_ENFORCE", raising=False)
    monkeypatch.delenv("VARCO_SECURITY_ENV", raising=False)
    monkeypatch.delenv("VARCO_SECURITY_SUPPRESS", raising=False)


def _summary_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.name == "varco_fastapi.posture" and r.getMessage().startswith("SecurityPosture:")
    ]


async def test_wiring_error_refuses_before_posture_runs(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Control for §D-compose: the preflight's ConfigurationError wins; posture never runs.
    container = _container()
    container.bind(_NeedsComposeUnbound, _NeedsComposeUnbound)
    posture = SecurityPostureModule(collectors=[_high_collector])
    app = create_varco_app(container, modules=[CoreModule(), posture], routers=[])

    with caplog.at_level(logging.WARNING), pytest.raises(ConfigurationError):
        async with app.router.lifespan_context(app):
            pass

    with pytest.raises(RuntimeError, match="has not started"):
        _ = posture.report
    assert _summary_lines(caplog) == []


async def test_clean_wiring_then_posture_refuses_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # Derived refuse (no env, no code value) must block the HIGH finding exactly once.
    container = _container()
    app = create_varco_app(
        container,
        modules=[CoreModule(), SecurityPostureModule(collectors=[_high_collector])],
        routers=[],
    )

    with caplog.at_level(logging.WARNING), pytest.raises(RuntimeError) as exc:
        async with app.router.lifespan_context(app):
            pass

    assert str(exc.value).count(_HIGH_ID) == 1
    assert len(_summary_lines(caplog)) == 1
    report = app.state.varco_wiring_report
    assert not any(getattr(f, "check", None) == _HIGH_ID for f in report.findings)


@pytest.mark.parametrize(
    ("env", "code_enforce", "expected"),
    [
        ({}, None, ("refuse", "derived")),
        (
            {"VARCO_SECURITY_ENV": "production", "VARCO_SECURITY_ENFORCE": "warn"},
            None,
            ("warn", "env"),
        ),
        ({}, "warn", ("warn", "code")),
        ({"VARCO_SECURITY_ENV": "development"}, None, ("warn", "derived")),
    ],
    ids=["unset-derived", "env-warn", "code-bind_settings", "development-derived"],
)
def test_wiring_report_shows_effective_enforce_and_provenance(
    monkeypatch: pytest.MonkeyPatch,
    env: dict[str, str],
    code_enforce: str | None,
    expected: tuple[str, str],
) -> None:
    from varco_core.settings import bind_settings
    from varco_fastapi.diagnose import posture_header
    from varco_fastapi.posture import SecurityPostureSettings

    for key, value in env.items():
        monkeypatch.setenv(key, value)
    if code_enforce is None:
        container = _container()
    else:
        # A bare container: fastapi's bootstrap already binds the settings class,
        # and bind_settings() refuses a second binding (048).
        container = DIContainer()
        bind_settings(
            container,
            SecurityPostureSettings,
            instance=SecurityPostureSettings(enforce=code_enforce),
        )

    header = posture_header(container, ())

    assert (header["enforce"], header["enforce_source"]) == expected


async def test_wiring_report_header_renders_the_derivation() -> None:
    container = _container()
    app = create_varco_app(container, modules=[CoreModule()], routers=[])

    async with app.router.lifespan_context(app):
        pass

    rendered = app.state.varco_wiring_report.render()
    assert "enforce=refuse (derived from VARCO_SECURITY_ENV=production)" in rendered


# ── `varco diagnose --check` never starts a module (§D-compose) ─────────────
#
# Module-scope factories so `--app tests.test_posture_wiring_compose:<name>`
# resolves. The absolute exit code depends on which backends the workspace
# happens to bootstrap (an unrelated `di.bootstrapped_unselected` WARNING can
# fail --check on its own), so the deterministic claim is a *difference*: the
# HIGH posture collector changes nothing about --check.


def _diagnose_app(*, with_high_collector: bool):
    from varco_core.auth.authorizer import BaseAuthorizer
    from varco_core.auth.base import AbstractAuthorizer

    container = _container()
    bind_instance(container, AbstractAuthorizer, BaseAuthorizer())
    collectors = [_high_collector] if with_high_collector else []
    return create_varco_app(
        container,
        modules=[CoreModule(), SecurityPostureModule(collectors=collectors)],
        routers=[],
        validate=False,
    )


def diagnose_app_with_high_collector():
    """``--app`` target: a HIGH posture finding that would refuse at start()."""
    return _diagnose_app(with_high_collector=True)


def diagnose_app_without_collector():
    """``--app`` target: the same app with no posture finding at all."""
    return _diagnose_app(with_high_collector=False)


def test_diagnose_check_ignores_posture_findings(
    capsys: pytest.CaptureFixture[str], caplog: pytest.LogCaptureFixture
) -> None:
    from varco_core.cli.main import main

    target = "tests.test_posture_wiring_compose:{}"
    with caplog.at_level(logging.WARNING):
        with_high = main(
            ["diagnose", "--app", target.format("diagnose_app_with_high_collector"), "--check"]
        )
        with_high_out = capsys.readouterr().out
        without = main(
            ["diagnose", "--app", target.format("diagnose_app_without_collector"), "--check"]
        )
        capsys.readouterr()

    # Derived refuse is in force, yet the HIGH finding never reaches --check:
    # diagnose never starts a module, so the posture preflight never runs.
    assert with_high == without
    assert _HIGH_ID not in with_high_out
    assert "enforce=refuse (derived from VARCO_SECURITY_ENV=production)" in with_high_out
    assert _summary_lines(caplog) == []
