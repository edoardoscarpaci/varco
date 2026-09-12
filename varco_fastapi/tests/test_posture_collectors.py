"""
Plan 036 (S9b) / Phase 3, Step 22 — red-mode tests for the five posture
collectors (§D-S9-checks, §D-S9-degrade).

Depends on ``varco_fastapi.posture`` (Phase 2) plus the four sibling
inspectors, all of which already ship: ``varco_core.tenancy.
inspect_tenant_provenance``, ``varco_core.revocation.
inspect_revocation_posture`` + ``varco_fastapi.auth.posture.
inspect_auth_posture``, ``varco_fastapi.middleware.introspect.
inspect_http_edge``, ``varco_sa.tenancy.rls_check.inspect_rls_posture``.

Every test imports ``varco_fastapi.posture``'s collector functions, which
do not exist yet — expected failure mode is ``ModuleNotFoundError`` /
``ImportError`` / ``AttributeError`` on the first missing name.
"""

from __future__ import annotations

import builtins

import pytest


def _block_import(monkeypatch: pytest.MonkeyPatch, *module_prefixes: str) -> None:
    """Monkeypatch builtins.__import__ so importing any of the given module
    prefixes raises ImportError — simulates the sibling package being
    absent, per §D-S9-degrade."""
    real_import = builtins.__import__

    def _fake_import(name, globals=None, locals=None, fromlist=(), level=0):  # noqa: A002
        if any(name == p or name.startswith(p + ".") for p in module_prefixes):
            raise ImportError(f"simulated absence of {name!r}")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _fake_import)


async def test_report_has_four_not_assessed_when_every_sibling_absent(monkeypatch) -> None:
    """§D-S9-degrade: with varco_core.tenancy / varco_core.revocation /
    varco_fastapi.auth.posture / varco_fastapi.middleware.introspect /
    varco_sa all unimportable, the report must contain NOT_ASSESSED
    entries for each collector — never a silently clean report — and
    start() must still return normally."""
    from varco_fastapi.posture import (
        PostureSeverity,
        SecurityPostureLifecycle,
        _collect_auth,
        _collect_data,
        _collect_http,
        _collect_tenant,
    )

    _block_import(
        monkeypatch,
        "varco_core.tenancy",
        "varco_core.revocation",
        "varco_fastapi.auth.posture",
        "varco_fastapi.middleware.introspect",
        "varco_sa",
    )

    lifecycle = SecurityPostureLifecycle(
        collectors=[_collect_tenant, _collect_auth, _collect_http, _collect_data]
    )

    # Must not raise even though every collector's import fails.
    await lifecycle.start()

    report = lifecycle.report
    not_assessed = [f for f in report.findings if f.severity == PostureSeverity.NOT_ASSESSED]
    assert len(not_assessed) >= 4
    assert len(report.not_assessed) >= 4


async def test_collector_raising_non_import_exception_yields_type_name_only() -> None:
    """A collector raising something other than ImportError must yield
    exactly one NOT_ASSESSED finding whose detail carries ONLY the
    exception's type name — never str(exc), per §D-S9-degrade and
    CLAUDE.md's error_params() rule."""
    from varco_fastapi.posture import (
        PostureSeverity,
        _wrap_collector,
    )

    secret_detail = "super secret internal path /etc/shadow leaked here"

    def _broken_collector():
        raise RuntimeError(secret_detail)

    wrapped = _wrap_collector(_broken_collector, name="broken")
    findings = wrapped()

    assert len(findings) == 1
    finding = findings[0]
    assert finding.severity == PostureSeverity.NOT_ASSESSED
    assert "RuntimeError" in finding.detail
    assert secret_detail not in finding.detail


async def test_full_report_against_worst_case_app_has_expected_local_ids() -> None:
    """The local collector must report the two admin-mount findings and
    the base-authorizer finding for a deliberately worst-case app: no
    posture lifecycle configured, BaseAuthorizer bound, nothing mounted
    with auth."""
    from providify import DIContainer
    from varco_core.auth.authorizer import BaseAuthorizer
    from varco_core.auth.base import AbstractAuthorizer
    from varco_fastapi.posture import _collect_local

    container = DIContainer()
    container.bind(AbstractAuthorizer, BaseAuthorizer)

    findings = _collect_local(container=container)
    ids = {f.check for f in findings}

    assert "posture.base_authorizer_bound" in ids
