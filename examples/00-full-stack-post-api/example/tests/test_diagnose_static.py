"""
``varco diagnose --static`` against the example app's real module list.

Docker-free: ``example.app:modules`` builds the same ``VarcoModule`` list
``create_app()`` boots without touching Postgres/Redis, and ``--static`` never
calls ``async_init``/``start``. Plan 052 Verification runs this exact target.
"""

from __future__ import annotations

import json

import pytest
from varco_core.cli.main import main

from example.backends import BACKEND_ENV

_TARGET = "example.app:modules"


def _run(capsys: pytest.CaptureFixture[str], *flags: str) -> tuple[int, str]:
    code = main(["diagnose", "--modules", _TARGET, "--static", *flags])
    return code, capsys.readouterr().out


def test_diagnose_static_runs_without_env_or_docker(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for name in ("DATABASE_URL", "VARCO_REDIS_URL", "VARCO_REDIS_CACHE_URL"):
        monkeypatch.delenv(name, raising=False)

    code, out = _run(capsys, "--json")

    assert code == 0
    report = json.loads(out)
    assert report["static"] is True
    names = {m["name"] for m in report["modules"]}
    assert {"sa", "redis", "ws", "post_app"} <= names


def test_diagnose_static_reports_no_errors(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)

    _, out = _run(capsys, "--json")

    # providify >= 2.1.1 (P34 closed): the @Requires-gated SA webhook_repository
    # is inactive here and no longer reports its own FieldEncryptor dependency.
    errors = [str(e) for e in json.loads(out)["validation"]["errors"]]
    assert errors == [], errors


def test_diagnose_static_check_passes_with_the_example_backends(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # The deployed configuration (docker-compose.yml / README) names every
    # selectable seam, so --check sees no ERROR and no WARNING finding.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    for name, value in BACKEND_ENV.items():
        monkeypatch.setenv(name, value)

    code, out = _run(capsys, "--check")

    assert code == 0, out


def test_diagnose_static_check_flags_an_unselected_backend(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    # Dropping one seam's selection brings back di.bootstrapped_unselected,
    # proving the passing test above is not vacuous.
    monkeypatch.delenv("DATABASE_URL", raising=False)
    for name, value in BACKEND_ENV.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("VARCO_DLQ_BACKEND")

    code, out = _run(capsys, "--check")

    assert code == 1
    assert "di.bootstrapped_unselected" in out
