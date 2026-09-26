"""
Red-mode tests for ``scripts/settings_reference.py`` (Plan 048 / Step 16).

Runs the script's functions in-process against ``varco_core`` only, per the
plan's own scope for this file. ``scripts/settings_reference.py`` does not
exist yet — every test fails with ``ImportError``/``ModuleNotFoundError``
until Step 16 lands it. This file is test-only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_SCRIPTS_DIR = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import settings_reference  # noqa: E402


class TestSettingsReferenceScript:
    def test_build_snapshot_is_deterministic(self) -> None:
        first = settings_reference.build_snapshot(["varco_core"])
        second = settings_reference.build_snapshot(["varco_core"])

        assert first == second

    def test_check_returns_zero_when_snapshot_matches(self, tmp_path: Path) -> None:
        snapshot_path = tmp_path / "settings-reference.json"
        settings_reference.write_snapshot(["varco_core"], snapshot_path)

        exit_code = settings_reference.check(["varco_core"], snapshot_path)

        assert exit_code == 0

    def test_check_returns_nonzero_when_snapshot_is_stale(self, tmp_path: Path) -> None:
        snapshot_path = tmp_path / "settings-reference.json"
        snapshot_path.write_text("{}")

        exit_code = settings_reference.check(["varco_core"], snapshot_path)

        assert exit_code != 0

    def test_secret_field_default_rendered_as_stars_in_snapshot(self) -> None:
        snapshot = settings_reference.build_snapshot(["varco_core"])

        rendered = str(snapshot)
        assert "***" in rendered

    def test_import_failure_reports_nonzero_never_a_shorter_snapshot(self) -> None:
        with pytest.raises(settings_reference.SettingsReferenceError) as exc:
            settings_reference.build_snapshot(["varco_definitely_missing_package"])

        assert "varco_definitely_missing_package" in str(exc.value)
