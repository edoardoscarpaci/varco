"""
Plan 054 / Phase A, Step 5 — red-mode tests for the posture severity table
(§D-severity-table) and the ``http.rate_limit.absent`` re-tier
(§D-ratelimit).

* ``di.in_process_backend`` (WARN, Plan 047) becomes a production
  start-blocker under FLIP-2's derived ``enforce="refuse"`` — for an app
  that opted in by contributing ``DIPostureCollector``.
* ``http.rate_limit.absent`` moves WARN → INFO so a recorded non-flip (036
  row 11) never blocks a production start by the back door.
* Every shipped posture ``check`` id has a row in
  ``technical_docs/features/security-posture.md``'s severity table.

Every test deletes ``VARCO_SECURITY_ENFORCE``/``VARCO_SECURITY_ENV`` first
(§D-flip2-tests) — the package conftest's autouse ``_posture_warn_only``
would otherwise hide the flip.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from providify import DIContainer

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SEVERITY_DOC = _REPO_ROOT / "technical_docs" / "features" / "security-posture.md"


@pytest.fixture
def clean_posture_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    monkeypatch.delenv("VARCO_SECURITY_ENFORCE", raising=False)
    monkeypatch.delenv("VARCO_SECURITY_ENV", raising=False)
    monkeypatch.delenv("VARCO_SECURITY_SUPPRESS", raising=False)
    return monkeypatch


@pytest.fixture
def in_memory_container(clean_posture_env: pytest.MonkeyPatch) -> DIContainer:
    # The event-bus seam resolves to its in-process default (no VARCO_EVENT_BUS_BACKEND).
    from varco_core.di import bootstrap

    clean_posture_env.delenv("VARCO_EVENT_BUS_BACKEND", raising=False)
    container = DIContainer()
    bootstrap(container)
    return container


def _di_collector(container: DIContainer):
    # The opt-in collector (never a default — see
    # test_posture_opt_in_and_default_collectors.py): the consequence below is
    # what an app that contributes it gets.
    from varco_fastapi.posture import DIPostureCollector

    return DIPostureCollector(container)


async def test_in_process_backend_blocks_production_start(
    in_memory_container: DIContainer,
) -> None:
    from varco_fastapi.posture import SecurityPostureLifecycle, SecurityPostureSettings

    lifecycle = SecurityPostureLifecycle(
        collectors=[_di_collector(in_memory_container)],
        settings=SecurityPostureSettings(),
    )

    with pytest.raises(RuntimeError, match=r"di\.in_process_backend"):
        await lifecycle.start()


async def test_in_process_backend_is_info_in_development(
    in_memory_container: DIContainer, clean_posture_env: pytest.MonkeyPatch
) -> None:
    from varco_fastapi.posture import (
        PostureSeverity,
        SecurityPostureLifecycle,
        SecurityPostureSettings,
    )

    clean_posture_env.setenv("VARCO_SECURITY_ENV", "development")
    lifecycle = SecurityPostureLifecycle(
        collectors=[_di_collector(in_memory_container)],
        settings=SecurityPostureSettings(),
    )
    await lifecycle.start()

    matching = [f for f in lifecycle.report.findings if f.check == "di.in_process_backend"]
    assert matching
    assert all(f.severity == PostureSeverity.INFO for f in matching)
    # Development derives warn — the start succeeded because of the derivation.
    assert lifecycle.report.enforce == "warn"


async def test_in_process_backend_suppressed_starts(
    in_memory_container: DIContainer,
) -> None:
    from varco_fastapi.posture import SecurityPostureLifecycle, SecurityPostureSettings

    lifecycle = SecurityPostureLifecycle(
        collectors=[_di_collector(in_memory_container)],
        settings=SecurityPostureSettings(suppress="di.in_process_backend"),
    )
    await lifecycle.start()

    # Must be the derived refuse in force — otherwise the start proves nothing.
    assert lifecycle.report.enforce == "refuse"
    assert all(
        f.suppressed for f in lifecycle.report.findings if f.check == "di.in_process_backend"
    )


def test_rate_limit_absent_is_info() -> None:
    # §D-ratelimit: an opt-in feature's absence can never be a production start-blocker.
    from fastapi import FastAPI
    from varco_fastapi.middleware.introspect import inspect_http_edge

    posture = inspect_http_edge(FastAPI())

    finding = next(f for f in posture.findings if f.check == "http.rate_limit.absent")
    assert finding.severity == "info"


def test_default_app_edge_findings(clean_posture_env: pytest.MonkeyPatch) -> None:
    # After FLIP-10 detail_exposed is gone from a default app; rate_limit.absent is INFO.
    from varco_fastapi.app import create_varco_app
    from varco_fastapi.middleware.introspect import inspect_http_edge

    clean_posture_env.delenv("VARCO_ERROR_INCLUDE_DETAIL", raising=False)
    app = create_varco_app(container=DIContainer(), routers=[], validate=False)
    posture = inspect_http_edge(app)

    assert {f.check for f in posture.findings} == {"http.rate_limit.absent"}
    assert {f.severity for f in posture.findings} == {"info"}


# Files that emit *posture* findings. `varco_core/di/report.py` is excluded on
# purpose: its `check=` ids are 052 `WiringFinding`s ("never a posture finding").
_POSTURE_SOURCES = (
    "varco_fastapi/varco_fastapi/posture.py",
    "varco_fastapi/varco_fastapi/middleware/introspect.py",
    "varco_core/varco_core/authority/posture.py",
    "varco_core/varco_core/redaction/posture.py",
    "varco_core/varco_core/retention/posture.py",
)


def _shipped_check_ids() -> set[str]:
    ids: set[str] = set()
    for rel in _POSTURE_SOURCES:
        text = (_REPO_ROOT / rel).read_text(encoding="utf-8")
        ids.update(re.findall(r'check="([a-z_.]+)"', text))
    # introspect.py's docstring table lists the http.* ids.
    introspect = (_REPO_ROOT / _POSTURE_SOURCES[1]).read_text(encoding="utf-8")
    ids.update(re.findall(r"^\s+(http\.[a-z_.]+)\s+(?:info|warn|high)\b", introspect, re.M))
    # Plan 054's two new INFO ids must ship (and therefore need rows).
    ids.update({"posture.unrecognised_environment", "posture.unrecognised_enforce"})
    return ids


def test_every_shipped_check_id_has_a_severity_table_row() -> None:
    """The doc is the register: a new id without a severity-table row fails here
    (the mechanical form of §D-severity-table's "add a row" rule)."""
    doc = _SEVERITY_DOC.read_text(encoding="utf-8")
    table_rows = [line for line in doc.splitlines() if line.startswith("|")]
    # Only rows of the severity table count — they carry a "Blocks"-column verdict.
    severity_rows = "\n".join(
        line for line in table_rows if re.search(r"\|\s*(HIGH|WARN|INFO|NOT_ASSESSED)\b", line)
    )

    missing = []
    for check in sorted(_shipped_check_ids()):
        if check.startswith("posture.not_assessed."):
            if "posture.not_assessed.*" not in severity_rows:
                missing.append(check)
            continue
        if f"`{check}`" not in severity_rows:
            missing.append(check)

    assert not missing, f"check ids with no severity-table row: {missing}"
