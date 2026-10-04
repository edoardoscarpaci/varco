"""
Plan 056 / FLIP-12, Step 6 — ``varco_sa.tenancy.rls_posture.rls_findings()``
over hand-built ``RlsPosture`` objects (red until Step 7).

Docker-free: premises 2 (field names), 3 (non-Postgres dialect) and 4
(owned-and-forced is not a bypass) are pinned here without a database.
"""

from __future__ import annotations

import dataclasses
import re
from pathlib import Path
from typing import Any

from varco_core.posture import PostureFinding, PostureSeverity
from varco_sa.tenancy.rls_check import RlsPosture, TablePosture

_SEVERITY_DOC = (
    Path(__file__).resolve().parents[2] / "technical_docs" / "features" / "security-posture.md"
)

_OFF = TablePosture(rls_enabled=False, rls_forced=False, has_policy=False)
_ENABLED_NO_POLICY = TablePosture(rls_enabled=True, rls_forced=True, has_policy=False)
_ENABLED_NOT_FORCED = TablePosture(rls_enabled=True, rls_forced=False, has_policy=True)
_GOOD = TablePosture(rls_enabled=True, rls_forced=True, has_policy=True)


def _posture(
    *,
    role: str = "app",
    superuser: bool = False,
    bypass: bool = False,
    owned: frozenset[str] = frozenset(),
    dialect: str | None = None,
    **tables: TablePosture,
) -> RlsPosture:
    kwargs: dict[str, Any] = {}
    if dialect is not None:
        kwargs["dialect"] = dialect
    return RlsPosture(
        current_role=role,
        is_superuser=superuser,
        rolbypassrls=bypass,
        owned_tables=owned,
        tables=tables,
        **kwargs,
    )


def _by_id(findings: list[PostureFinding]) -> dict[str, PostureFinding]:
    return {f.check: f for f in findings}


def _findings(posture: RlsPosture | None, **kw: Any) -> list[PostureFinding]:
    from varco_sa.tenancy.rls_posture import rls_findings

    return rls_findings(posture, **kw)


def test_rls_posture_dialect_is_trailing_field_defaulting_to_postgresql() -> None:
    # Class-field guard api_surface.py --check cannot provide; positional construction must keep working.
    # `missing_tables` (drift decision on Plan 056) was appended after it, also defaulted.
    fields = dataclasses.fields(RlsPosture)

    assert [f.name for f in fields[-2:]] == ["dialect", "missing_tables"]
    assert fields[-2].default == "postgresql"
    assert fields[-1].default == frozenset()


def test_none_is_not_checked() -> None:
    result = _findings(None, not_checked_reason="the RLS posture refresh has not run")

    assert len(result) == 1
    assert result[0].check == "data.rls_not_checked"
    assert result[0].severity is PostureSeverity.NOT_ASSESSED
    assert "the RLS posture refresh has not run" in result[0].detail


def test_non_postgres_is_info_only() -> None:
    # Premise 3: SQLite's all-False degenerate posture must not read as "RLS disabled".
    result = _findings(_posture(dialect="sqlite", t_a=_OFF, t_b=_OFF))

    assert {f.check for f in result} == {"data.rls_not_applicable"}
    assert result[0].severity is PostureSeverity.INFO
    assert "sqlite" in result[0].detail


def test_disabled_is_high_and_names_tables() -> None:
    found = _by_id(_findings(_posture(t_a=_OFF, t_b=_OFF, t_ok=_GOOD)))

    f = found["data.rls_disabled"]
    assert f.severity is PostureSeverity.HIGH
    assert "t_a" in f.detail and "t_b" in f.detail
    assert "t_ok" not in f.detail


def test_enabled_without_policy_is_warn() -> None:
    found = _by_id(_findings(_posture(t_a=_ENABLED_NO_POLICY)))

    assert found["data.rls_enabled_without_policy"].severity is PostureSeverity.WARN
    assert "t_a" in found["data.rls_enabled_without_policy"].detail


def test_enabled_not_forced_is_warn() -> None:
    found = _by_id(_findings(_posture(t_a=_ENABLED_NOT_FORCED)))

    assert found["data.rls_not_forced"].severity is PostureSeverity.WARN
    assert "t_a" in found["data.rls_not_forced"].detail
    assert "data.role_bypasses_rls" not in found  # not owned → no bypass


def test_owned_and_forced_is_clean() -> None:
    # Premise 4: the textbook-correct owner shape (FORCE set) is not a bypass.
    assert _findings(_posture(owned=frozenset({"t_a"}), t_a=_GOOD)) == []


def test_owned_and_unforced_is_high_and_warn() -> None:
    found = _by_id(_findings(_posture(owned=frozenset({"t_a"}), t_a=_ENABLED_NOT_FORCED)))

    assert set(found) == {"data.rls_not_forced", "data.role_bypasses_rls"}
    assert found["data.role_bypasses_rls"].severity is PostureSeverity.HIGH
    assert found["data.rls_not_forced"].severity is PostureSeverity.WARN


def test_superuser_is_high() -> None:
    found = _by_id(_findings(_posture(superuser=True, t_a=_GOOD)))

    assert set(found) == {"data.role_bypasses_rls"}
    assert found["data.role_bypasses_rls"].severity is PostureSeverity.HIGH


def test_bypassrls_is_high() -> None:
    found = _by_id(_findings(_posture(bypass=True, t_a=_GOOD)))

    assert set(found) == {"data.role_bypasses_rls"}
    assert found["data.role_bypasses_rls"].severity is PostureSeverity.HIGH


def test_guc_setter_absent_fires_only_with_live_rls() -> None:
    live = _by_id(_findings(_posture(t_a=_GOOD), guc_setter_present=False))
    unknown = _findings(_posture(t_a=_GOOD), guc_setter_present=None)
    all_off = _by_id(_findings(_posture(t_a=_OFF), guc_setter_present=False))

    assert set(live) == {"data.rls_guc_setter_absent"}
    assert live["data.rls_guc_setter_absent"].severity is PostureSeverity.WARN
    assert unknown == []
    assert "data.rls_guc_setter_absent" not in all_off


def test_guc_setter_present_is_clean() -> None:
    assert _findings(_posture(t_a=_GOOD), guc_setter_present=True) == []


def test_clean_posture_yields_nothing() -> None:
    assert _findings(_posture(t_a=_GOOD, t_b=_GOOD)) == []


def test_every_id_is_in_the_severity_register() -> None:
    # Local twin of 054's register test: a red here points at plan 056's doc rows.
    emitted = {
        "data.rls_not_checked",
        "data.rls_not_applicable",
        "data.rls_disabled",
        "data.rls_enabled_without_policy",
        "data.rls_not_forced",
        "data.role_bypasses_rls",
        "data.rls_guc_setter_absent",
        "data.rls_not_enforced",
        "data.rls_table_absent",
    }
    rows = "\n".join(
        line
        for line in _SEVERITY_DOC.read_text(encoding="utf-8").splitlines()
        if line.startswith("|") and re.search(r"\|\s*(HIGH|WARN|INFO|NOT_ASSESSED)\b", line)
    )

    missing = sorted(c for c in emitted if f"`{c}`" not in rows)

    assert not missing, f"plan 056 ids with no severity-table row: {missing}"


# ── drift decision on Plan 056: a candidate table that does not exist yet ────
# (fresh database, first deploy) is "not yet created", never data.rls_disabled.


def test_rls_posture_missing_tables_is_trailing_field_defaulting_to_empty() -> None:
    posture = _posture(t_a=_GOOD)

    assert posture.missing_tables == frozenset()


def test_missing_table_is_not_rls_disabled() -> None:
    posture = dataclasses.replace(_posture(t_new=_OFF), missing_tables=frozenset({"t_new"}))

    findings = _findings(posture, guc_setter_present=True)

    assert [(f.check, f.severity) for f in findings] == [
        ("data.rls_table_absent", PostureSeverity.INFO)
    ]
    assert "t_new" in findings[0].detail


def test_missing_table_beside_a_disabled_one_reports_only_the_existing() -> None:
    posture = dataclasses.replace(
        _posture(t_new=_OFF, t_old=_OFF), missing_tables=frozenset({"t_new"})
    )

    by_id = _by_id(_findings(posture, guc_setter_present=True))

    assert set(by_id) == {"data.rls_disabled", "data.rls_table_absent"}
    assert "t_old" in by_id["data.rls_disabled"].detail
    assert "t_new" not in by_id["data.rls_disabled"].detail


def test_role_findings_still_fire_with_only_missing_tables() -> None:
    # A superuser bypasses RLS whether or not the tables exist yet.
    posture = dataclasses.replace(
        _posture(superuser=True, t_new=_OFF), missing_tables=frozenset({"t_new"})
    )

    assert set(_by_id(_findings(posture))) == {"data.rls_table_absent", "data.role_bypasses_rls"}
