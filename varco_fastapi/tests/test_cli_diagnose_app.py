"""
Red-mode tests for Plan 052 Step 7/§D-driver's ``--app`` input form —
``varco diagnose --app module:attr`` reads ``app.state.varco_container``/
``app.state.varco_modules`` off an already-built ``create_varco_app()``
object (§D-driver).

``varco diagnose`` is not registered yet and ``app.state.varco_container``/
``varco_modules`` are not set yet — expected to fail for either reason until
Plan 052 Step 7/8 land.
"""

from __future__ import annotations

from providify import DIContainer
from varco_core.di import CoreModule
from varco_fastapi.app import create_varco_app

# Module-scope target so "tests.test_cli_diagnose_app:app" resolves.
app = create_varco_app(DIContainer(), modules=[CoreModule()], routers=[], validate=False)


async def test_app_source_exits_0_and_shows_seams(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(["diagnose", "--app", "tests.test_cli_diagnose_app:app", "--static"])

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "Seams" in out


def make_app():
    """Zero-arg app factory — ``--app`` accepts it as well as an app object."""
    return create_varco_app(DIContainer(), modules=[CoreModule()], routers=[], validate=False)


def broken_factory():
    raise RuntimeError("boom")


async def test_app_factory_is_called(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(["diagnose", "--app", "tests.test_cli_diagnose_app:make_app", "--static"])

    assert exit_code == 0
    assert "Seams" in capsys.readouterr().out


async def test_raising_app_factory_exits_1_one_line(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(
        ["diagnose", "--app", "tests.test_cli_diagnose_app:broken_factory", "--static"]
    )

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "boom" in captured.err
    assert "Traceback" not in captured.err


def posture_app():
    """An app whose SecurityPostureModule carries explicit (code) settings."""
    from varco_fastapi.modules import SecurityPostureModule
    from varco_fastapi.posture import SecurityPostureSettings

    return create_varco_app(
        DIContainer(),
        modules=[
            CoreModule(),
            SecurityPostureModule(
                settings=SecurityPostureSettings(environment="staging", enforce="refuse")
            ),
        ],
        routers=[],
        validate=False,
    )


async def test_app_source_populates_report_header_from_posture_settings(capsys) -> None:
    import json

    from varco_core.cli.main import main

    exit_code = main(
        ["diagnose", "--app", "tests.test_cli_diagnose_app:posture_app", "--static", "--json"]
    )

    parsed = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert parsed["environment"] == "staging"
    assert parsed["enforce"] == "refuse"
    assert parsed["enforce_source"] == "code"
