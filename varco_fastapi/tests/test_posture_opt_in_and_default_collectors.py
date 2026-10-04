"""
Drift decisions on Plans 054 / 057 — which posture collectors a stock app
gets.

* Plan 054: ``di.in_process_backend`` is **opt-in**. ``DIPostureCollector``
  is never a default collector; an app that contributes it in production
  with in-process seams refuses to start.
* Plan 057 (via 051 §D-posture): ``RedactionPostureCollector`` **is** a
  default collector; ``SecurityPostureModule(audited_services=...)`` feeds
  it, and a listed opted-out service refuses a production start with
  ``redaction.audit.disabled``.

Production legs delete the package conftest's ``VARCO_SECURITY_ENFORCE=warn``
pin (054 §D-flip2-tests) and inspect only the id under test — unrelated
default findings (``posture.authorizer_unbound``) may also block a bare app.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from providify import DIContainer
from varco_core.redaction import reset_redaction_state
from varco_core.service.audit import AuditLogMixin

_SECURITY_VARS = ("VARCO_SECURITY_ENFORCE", "VARCO_SECURITY_ENV", "VARCO_SECURITY_SUPPRESS")


class _OptedOut(AuditLogMixin):
    _audit_redactor = None


class _Audited(AuditLogMixin):
    """Keeps the 4.0 default redactor — must produce no finding."""


@pytest.fixture(autouse=True)
def _clean_redaction_state() -> Iterator[None]:
    reset_redaction_state()
    yield
    reset_redaction_state()


@pytest.fixture
def production(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    for name in _SECURITY_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("VARCO_EVENT_BUS_BACKEND", raising=False)
    monkeypatch.delenv("VARCO_ERROR_REDACT_PARAMS", raising=False)
    return monkeypatch


def _posture_app(container: DIContainer, posture_module: object) -> object:
    from varco_fastapi.app import create_varco_app
    from varco_fastapi.modules import SecurityPostureModule
    from varco_fastapi.modules.defaults import default_modules

    return create_varco_app(
        container,
        modules=[*default_modules(exclude=(SecurityPostureModule,)), posture_module],
        routers=[],
        validate=False,
    )


async def _refusal_text(app: object) -> str:
    """Enter the lifespan; return the refusal message, or ``""`` if it started."""
    try:
        async with app.router.lifespan_context(app):  # type: ignore[attr-defined]
            return ""
    except RuntimeError as exc:
        return str(exc)


# ── Plan 054: di.in_process_backend is opt-in ────────────────────────────────


async def test_default_collectors_exclude_di_collector() -> None:
    from varco_fastapi.modules.lifecycle import _build_default_posture_collectors
    from varco_fastapi.posture import DIPostureCollector

    defaults = _build_default_posture_collectors(None, DIContainer())

    assert not [c for c in defaults if isinstance(c, DIPostureCollector)]


async def test_default_app_reports_no_in_process_backend(production: pytest.MonkeyPatch) -> None:
    # In-process seams everywhere, development so the report is reachable.
    from varco_fastapi.modules import SecurityPostureModule

    production.setenv("VARCO_SECURITY_ENV", "development")
    posture = SecurityPostureModule()
    app = _posture_app(DIContainer(), posture)

    async with app.router.lifespan_context(app):  # type: ignore[attr-defined]
        ids = {f.check for f in posture.report.findings}

    assert "di.in_process_backend" not in ids


async def test_default_app_in_production_never_refuses_on_in_process_backend(
    production: pytest.MonkeyPatch,
) -> None:
    from varco_fastapi.modules import SecurityPostureModule

    message = await _refusal_text(_posture_app(DIContainer(), SecurityPostureModule()))

    assert "di.in_process_backend" not in message


async def test_opted_in_app_in_production_refuses_with_in_process_backend(
    production: pytest.MonkeyPatch,
) -> None:
    from varco_core.di import contribute
    from varco_core.posture import PostureCollector
    from varco_fastapi.modules import SecurityPostureModule
    from varco_fastapi.posture import DIPostureCollector

    container = DIContainer()
    contribute(container, PostureCollector, DIPostureCollector(container))  # type: ignore[type-abstract]

    message = await _refusal_text(_posture_app(container, SecurityPostureModule()))

    assert "di.in_process_backend" in message


async def test_di_collector_name_and_findings_match_collect_di() -> None:
    from varco_core.di import bootstrap
    from varco_fastapi.posture import DIPostureCollector, _collect_di

    container = DIContainer()
    bootstrap(container)
    collector = DIPostureCollector(container)

    assert collector.name == "di"
    assert collector.collect() == _collect_di(container)


# ── Plan 057 / 051 §D-posture: RedactionPostureCollector is a default ────────


async def test_default_collectors_include_redaction_collector() -> None:
    from varco_fastapi.modules.lifecycle import _build_default_posture_collectors
    from varco_fastapi.posture import RedactionPostureCollector

    defaults = _build_default_posture_collectors(None, DIContainer())

    assert [type(c) for c in defaults if c.name == "redaction"] == [RedactionPostureCollector]


async def test_default_app_contributes_redaction_collector() -> None:
    from fastapi import FastAPI
    from varco_core.di.collect import collect
    from varco_core.posture import PostureCollector
    from varco_fastapi.modules import SecurityPostureModule
    from varco_fastapi.posture import RedactionPostureCollector

    container = DIContainer()
    SecurityPostureModule().configure_app(FastAPI(), container)

    collectors = collect(container, PostureCollector)  # type: ignore[type-abstract]

    assert any(isinstance(c, RedactionPostureCollector) for c in collectors)


async def test_default_app_emits_no_redaction_finding(production: pytest.MonkeyPatch) -> None:
    from varco_fastapi.modules import SecurityPostureModule

    production.setenv("VARCO_SECURITY_ENV", "development")
    posture = SecurityPostureModule(audited_services=(_Audited,))
    app = _posture_app(DIContainer(), posture)

    async with app.router.lifespan_context(app):  # type: ignore[attr-defined]
        ids = {f.check for f in posture.report.findings}

    assert not {i for i in ids if i.startswith("redaction.")}, ids


async def test_audited_opt_out_refuses_production_start(production: pytest.MonkeyPatch) -> None:
    from varco_fastapi.modules import SecurityPostureModule

    posture = SecurityPostureModule(audited_services=(_OptedOut,))

    message = await _refusal_text(_posture_app(DIContainer(), posture))

    assert "redaction.audit.disabled" in message


async def test_audited_opt_out_is_warn_in_the_report(production: pytest.MonkeyPatch) -> None:
    from varco_fastapi.modules import SecurityPostureModule
    from varco_fastapi.posture import PostureSeverity, RedactionPostureCollector

    findings = RedactionPostureCollector(service_classes=(_OptedOut,)).collect()

    assert [(f.check, f.severity) for f in findings] == [
        ("redaction.audit.disabled", PostureSeverity.WARN)
    ]
    # And an unlisted opt-out is never discovered (explicit input only).
    assert RedactionPostureCollector().collect() == []
    assert SecurityPostureModule()._audited_services == ()


async def test_redact_params_disabled_fires_only_when_opted_out(
    production: pytest.MonkeyPatch,
) -> None:
    from varco_fastapi.posture import RedactionPostureCollector

    assert RedactionPostureCollector().collect() == []
    production.setenv("VARCO_ERROR_REDACT_PARAMS", "false")
    assert [f.check for f in RedactionPostureCollector().collect()] == [
        "redaction.error_params.disabled"
    ]
