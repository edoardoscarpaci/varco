"""
Plan 047 / Step 11 — `scripts/di_catalogue.py` (§D-script).

In-process, `varco_core`-only exercise: deterministic output,
`--check` exit codes, a package that fails to import surfaces non-zero
naming the package, and a duplicate `(seam, value)` across packages is an
error.

RED MODE: `scripts/di_catalogue.py` does not exist yet — every test here
is expected to fail with `ModuleNotFoundError`/`ImportError`.
"""

from __future__ import annotations

import subprocess
import sys

import pytest

REPO_ROOT = __file__.rsplit("/varco_core/", 1)[0]


def _run_script(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "scripts/di_catalogue.py", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def test_script_module_is_importable() -> None:
    import scripts.di_catalogue as di_catalogue_script

    assert hasattr(di_catalogue_script, "discover_packages")
    assert hasattr(di_catalogue_script, "build_catalogue")


def test_discover_packages_executes_packages_sh() -> None:
    # RL-18 discipline — same shape as api_surface.py's discover_packages().
    import scripts.di_catalogue as di_catalogue_script

    packages = di_catalogue_script.discover_packages()
    assert "varco_core" in packages


def test_check_flag_exits_zero_on_a_clean_snapshot() -> None:
    result = _run_script("--check")
    assert result.returncode == 0, result.stderr


def test_check_flag_is_deterministic_across_two_successful_runs() -> None:
    first = _run_script("--check")
    second = _run_script("--check")

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert first.stdout == second.stdout


def test_package_import_failure_reports_package_name_and_nonzero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import scripts.di_catalogue as di_catalogue_script

    def _boom(*_args: object, **_kwargs: object) -> None:
        raise ImportError("varco_totally_broken has no module named this")

    monkeypatch.setattr(di_catalogue_script, "build_catalogue", _boom)

    with pytest.raises(ImportError, match="varco_totally_broken"):
        di_catalogue_script.build_catalogue()


def test_duplicate_seam_value_across_packages_raises() -> None:
    from varco_core.di.errors import CatalogueError
    from varco_core.di.introspect import Catalogue

    seam_a = {"seam": "cache", "value": "redis", "package": "varco_redis"}
    seam_b = {"seam": "cache", "value": "redis", "package": "varco_memcached"}

    with pytest.raises(CatalogueError):
        Catalogue.merge_backend_entries([seam_a, seam_b])
