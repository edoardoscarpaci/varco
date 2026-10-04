"""
tests.test_posture_redaction_collector
========================================
Plan 057 / RO-5, Step 10 — ``_collect_redaction``: 040's
``inspect_redaction_posture()`` wired into ``SecurityPosture`` the 3.2 way
(a private collector function + a ``partial(...)`` for ``service_classes``).

After FLIP-040-2 a default app has no redaction finding at all, so every
finding now means "opted out" — the property RO-5 was parked on.

Red-mode: every test imports ``_collect_redaction`` from
``varco_fastapi.posture``, which does not exist yet (``ImportError``).
"""

from __future__ import annotations

import builtins
from collections.abc import Iterator
from functools import partial

import pytest
from varco_core.exception.settings import ErrorEnvelopeSettings
from varco_core.redaction import (
    PolicyRedactor,
    RedactionPolicy,
    reset_redaction_state,
    set_default_redactor,
)
from varco_core.service.audit import AuditLogMixin

_FOUR_IDS = {
    "redaction.audit.disabled",
    "redaction.error_params.disabled",
    "redaction.default.custom",
    "redaction.patterns.substring_mode",
}


class _OptedOut(AuditLogMixin):
    _audit_redactor = None


@pytest.fixture(autouse=True)
def _clean_redaction_state() -> Iterator[None]:
    # The default redactor is process-global; never leak it across tests.
    reset_redaction_state()
    yield
    reset_redaction_state()


def _block_import(monkeypatch: pytest.MonkeyPatch, *module_prefixes: str) -> None:
    # Same shape as test_posture_collectors.py::_block_import (§D-S9-degrade).
    real_import = builtins.__import__

    def _fake_import(name, globals=None, locals=None, fromlist=(), level=0):  # noqa: A002
        if any(name == p or name.startswith(p + ".") for p in module_prefixes):
            raise ImportError(f"simulated absence of {name!r}")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _fake_import)


async def test_zero_arg_collector_is_clean_on_a_default_process() -> None:
    from varco_fastapi.posture import _collect_redaction

    assert _collect_redaction() == []


async def test_opted_out_service_class_is_warn_with_remediation() -> None:
    from varco_fastapi.posture import PostureSeverity, _collect_redaction

    findings = _collect_redaction(service_classes=(_OptedOut,))

    assert len(findings) == 1
    finding = findings[0]
    assert finding.check == "redaction.audit.disabled"
    assert finding.severity is PostureSeverity.WARN
    assert "_audit_redactor = None" in finding.remediation
    assert "VARCO_SECURITY_SUPPRESS" in finding.remediation


async def test_redact_params_false_is_warn() -> None:
    from varco_fastapi.posture import PostureSeverity, _collect_redaction

    findings = _collect_redaction(envelope_settings=ErrorEnvelopeSettings(redact_params=False))

    assert [(f.check, f.severity) for f in findings] == [
        ("redaction.error_params.disabled", PostureSeverity.WARN)
    ]
    assert findings[0].remediation


async def test_substring_process_default_is_info() -> None:
    # Opting into substring redacts MORE — a data-quality concern, INFO only.
    from varco_fastapi.posture import PostureSeverity, _collect_redaction

    set_default_redactor(PolicyRedactor(RedactionPolicy(match_mode="substring")))

    findings = _collect_redaction()

    assert [(f.check, f.severity) for f in findings] == [
        ("redaction.patterns.substring_mode", PostureSeverity.INFO)
    ]


async def test_absent_core_module_degrades_to_not_assessed(monkeypatch) -> None:
    from varco_fastapi.posture import (
        PostureSeverity,
        SecurityPostureLifecycle,
        _collect_redaction,
    )

    _block_import(monkeypatch, "varco_core.redaction")
    lifecycle = SecurityPostureLifecycle(collectors=[_collect_redaction])

    await lifecycle.start()

    not_assessed = [
        f for f in lifecycle.report.findings if f.severity == PostureSeverity.NOT_ASSESSED
    ]
    assert len(not_assessed) == 1
    assert not_assessed[0].check == "posture.not_assessed._collect_redaction"


async def test_refuse_in_production_refuses_an_opted_out_service() -> None:
    # An explicit opt-out of a security default in production needs an
    # explicit suppression — WARN blocks under refuse.
    from varco_fastapi.posture import (
        SecurityPostureLifecycle,
        SecurityPostureSettings,
        _collect_redaction,
    )

    lifecycle = SecurityPostureLifecycle(
        collectors=[partial(_collect_redaction, service_classes=(_OptedOut,))],
        settings=SecurityPostureSettings(enforce="refuse", environment="production"),
    )

    with pytest.raises(RuntimeError, match="redaction.audit.disabled"):
        await lifecycle.start()


async def test_ids_are_the_four_stable_literals() -> None:
    # 040 committed to these ids as stable — an operator's
    # VARCO_SECURITY_SUPPRESS list must keep matching.
    from varco_fastapi.posture import _REDACTION_REMEDIATION

    assert set(_REDACTION_REMEDIATION) == _FOUR_IDS
