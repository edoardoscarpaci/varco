"""Tests for ``scripts/unit_tests.sh`` — the two-lane unit runner (Plan 059 Step 7).

Precedent: ``test_bump_script.py`` (drive a repo script as a subprocess).

Every test except the last runs only ``--plan`` (prints the lane plan and
exits before any ``uv sync``), so the file is fast. The last one runs the
full scheduler against a fake pytest (``VARCO_UNIT_PYTEST``) with the sync
skipped (``VARCO_UNIT_SYNC=0``) — it never executes a real suite.

``--plan`` contract (§D-lanes / §D-script): one line per suite,
``suite=<label> lane=<xdist|pool> workers=<n> files=<n>``, plus ``seed=<n>``,
``workers=<n>`` and ``jobs=<n>`` lines. The example suite's label is its
directory, ``examples/00-full-stack-post-api``.
"""

from __future__ import annotations

import os
import re
import subprocess
import textwrap
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts" / "unit_tests.sh"
PACKAGES_SH = REPO_ROOT / "scripts" / "packages.sh"
EXAMPLE_LABEL = "examples/00-full-stack-post-api"

_SUITE_RE = re.compile(
    r"^suite=(?P<label>\S+)\s+lane=(?P<lane>xdist|pool)\s+"
    r"workers=(?P<workers>\d+)\s+files=(?P<files>\d+)\s*$"
)


def _run(
    *args: str, env: dict[str, str] | None = None, timeout: float = 120
) -> subprocess.CompletedProcess[str]:
    """Run ``bash scripts/unit_tests.sh *args`` with ``env`` layered on os.environ.

    Args:
        *args: Arguments passed to the script.
        env: Extra environment variables (override the inherited ones).
        timeout: Seconds before the subprocess is killed.

    Returns:
        The completed process, text mode, stdout/stderr captured.
    """
    full_env = {**os.environ, **(env or {})}
    return subprocess.run(
        ["bash", str(SCRIPT), *args],
        cwd=REPO_ROOT,
        env=full_env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )


def _plan(*args: str, env: dict[str, str] | None = None) -> list[dict[str, str]]:
    """Run ``--plan`` and parse its ``suite=`` lines, asserting exit 0.

    Returns:
        One dict per suite line, in printed order.
    """
    result = _run("--plan", *args, env=env)
    assert result.returncode == 0, (
        f"--plan exited {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    rows = [
        m.groupdict() for line in result.stdout.splitlines() if (m := _SUITE_RE.match(line.strip()))
    ]
    assert rows, f"--plan printed no suite= lines:\n{result.stdout}"
    return rows


def _expected_labels() -> list[str]:
    """Every ``packages.sh`` name plus the example (RL-18 derivation)."""
    out = subprocess.run(
        ["bash", str(PACKAGES_SH)], capture_output=True, text=True, check=True
    ).stdout
    return [*out.split(), EXAMPLE_LABEL]


def test_plan_lists_every_package_and_example_exactly_once() -> None:
    # RL-18: a hand list that drops a suite is the failure mode this guards.
    labels = [row["label"] for row in _plan()]
    assert sorted(labels) == sorted(_expected_labels())


def test_plan_prints_seed_workers_and_jobs_lines() -> None:
    result = _run("--plan", env={"VARCO_UNIT_WORKERS": "3", "VARCO_UNIT_JOBS": "2"})
    assert result.returncode == 0, result.stderr
    lines = [line.strip() for line in result.stdout.splitlines()]
    assert any(re.fullmatch(r"seed=\d+", line) for line in lines)
    assert "workers=3" in lines
    assert "jobs=2" in lines


def test_ws_and_example_are_pool_even_with_min_files_one() -> None:
    # The loop-scope rule (async session fixtures), not suite size, decides.
    rows = {
        r["label"]: r
        for r in _plan(env={"VARCO_UNIT_XDIST_MIN_FILES": "1", "VARCO_UNIT_WORKERS": "4"})
    }
    assert rows["varco_ws"]["lane"] == "pool"
    assert rows[EXAMPLE_LABEL]["lane"] == "pool"


@pytest.mark.parametrize("workers", ["0", "1"])
def test_workers_zero_or_one_disables_xdist_everywhere(workers: str) -> None:
    # W <= 1 means "no xdist": one worker only adds start-up overhead.
    rows = _plan(env={"VARCO_UNIT_WORKERS": workers, "VARCO_UNIT_XDIST_MIN_FILES": "1"})
    assert all(r["lane"] == "pool" for r in rows), rows


def test_plan_with_one_package_lists_only_it() -> None:
    rows = _plan("varco_redis")
    assert [r["label"] for r in rows] == ["varco_redis"]


def test_pool_order_is_file_count_descending() -> None:
    # Longest-first start keeps the pool's tail short.
    pool = [r for r in _plan() if r["lane"] == "pool"]
    counts = [int(r["files"]) for r in pool]
    assert counts == sorted(counts, reverse=True), pool


def test_plan_with_unknown_package_exits_one() -> None:
    # Path validation must survive the rewrite.
    result = _run("--plan", "bogus_pkg")
    assert result.returncode == 1
    assert "bogus_pkg" in (result.stdout + result.stderr)
    # Must be rejected as an unknown package, not as an unknown "--plan" suite.
    assert "--plan" not in result.stderr


def test_seed_env_is_honoured() -> None:
    result = _run("--plan", env={"VARCO_UNIT_SEED": "42"})
    assert result.returncode == 0, result.stderr
    assert "seed=42" in [line.strip() for line in result.stdout.splitlines()]


_FAKE_PYTEST = textwrap.dedent(
    """\
    #!/usr/bin/env python3
    # Fake pytest for Plan 059 Step 7(h): records start/end, picks exit by suite.
    import os, sys, time
    label = None
    for arg in sys.argv[1:]:
        if arg.startswith("examples/00-full-stack-post-api"):
            label = "examples/00-full-stack-post-api"
    if label is None:
        label = os.path.basename(os.getcwd())
    events = os.environ["FAKE_PYTEST_EVENTS"]
    with open(events, "a") as fh:
        fh.write(f"start {label} {time.monotonic_ns()}\\n")
    time.sleep(0.2)
    code = {"varco_casbin": 1, "varco_ws": 5}.get(label, 0)
    print(f"FAKE-LOG-MARKER {label}")
    if code == 1:
        print("========== 1 failed, 2 passed in 0.20s ==========")
    elif code == 5:
        print("========== no tests ran in 0.20s ==========")
    else:
        print("========== 3 passed in 0.20s ==========")
    with open(events, "a") as fh:
        fh.write(f"end {label} {time.monotonic_ns()}\\n")
    sys.exit(code)
    """
)


def test_fake_pytest_run_accumulates_statuses_and_caps_pool_width(tmp_path: Path) -> None:
    # Run --plan first: it fails fast (and cheaply) if --plan support ever
    # regresses, before the full run below, and yields the pool-lane labels
    # the concurrency check needs.
    env = {"VARCO_UNIT_WORKERS": "0", "VARCO_UNIT_JOBS": "2"}
    plan_rows = _plan(env=env)
    pool_labels = {r["label"] for r in plan_rows if r["lane"] == "pool"}

    fake = tmp_path / "fake_pytest"
    fake.write_text(_FAKE_PYTEST)
    fake.chmod(0o755)
    events = tmp_path / "events.txt"
    result = _run(
        env={
            **env,
            "VARCO_UNIT_SYNC": "0",
            "VARCO_UNIT_PYTEST": str(fake),
            "VARCO_UNIT_LOG_DIR": str(tmp_path / "logs"),
            "FAKE_PYTEST_EVENTS": str(events),
        },
        timeout=300,
    )
    out = result.stdout + result.stderr

    assert result.returncode == 1, out

    labels = _expected_labels()
    summary = out[out.rfind("Summary") :]
    for label in labels:
        lines = [
            ln for ln in summary.splitlines() if re.search(rf"(^|\s){re.escape(label)}(\s|$)", ln)
        ]
        assert len(lines) == 1, f"{label} appears {len(lines)}x in summary:\n{summary}"
        line = lines[0].lower()
        if label == "varco_casbin":
            assert "fail" in line or "✘" in line, line
        elif label == "varco_ws":
            assert "no tests" in line or "○" in line, line
        else:
            assert "pass" in line or "✔" in line, line

    # Buffered pool logs must reach stdout for a failed suite.
    assert "FAKE-LOG-MARKER varco_casbin" in out

    # Oversubscription invariant: never more than J pool suites at once.
    timeline: list[tuple[int, int]] = []
    for raw in events.read_text().splitlines():
        kind, label, ns = raw.split()
        if label in pool_labels:
            timeline.append((int(ns), 1 if kind == "start" else -1))
    timeline.sort(key=lambda e: (e[0], e[1]))  # ends before starts on ties
    running = peak = 0
    for _, delta in timeline:
        running += delta
        peak = max(peak, running)
    assert 1 <= peak <= 2, f"peak concurrent pool suites = {peak}"
