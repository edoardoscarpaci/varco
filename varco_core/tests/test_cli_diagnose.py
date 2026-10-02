"""
Red-mode tests for Plan 052 Step 7 — the ``varco diagnose`` CLI
(``varco_core/varco_core/cli/diagnose.py`` + one registration line in
``cli/main.py:_build_parser()``, §D-cli).

Calls ``main(argv)`` directly — no subprocess — same convention as
``test_cli_migrate.py``.

``varco diagnose`` is not registered yet, so every test here is expected to
fail: either the subcommand is unrecognised (argparse usage error via
``main()``'s ``SystemExit`` handling) or, once ``varco_core.di.diagnose``
exists but the CLI module doesn't, an ``ImportError``. At the current state
of the tree (neither exists) every test fails at the `main(["diagnose", ...])`
call with a non-2 "unrecognised command" style mismatch against the assertions
below — i.e. for the right reason: the subcommand does not exist.
"""

from __future__ import annotations

import json

from providify import DIContainer
from varco_core.di import CoreModule

# Module-scope targets so "tests.test_cli_diagnose:modules" resolves.


def modules() -> list:
    return [CoreModule()]


def container() -> DIContainer:
    c = DIContainer()
    CoreModule().register(c)
    return c


def raising_modules() -> list:
    from varco_core.di.module import VarcoModule

    class _Raises(VarcoModule):
        name = "raises"

        def _register(self, c: DIContainer) -> None:
            pass

        async def async_init(self, c: DIContainer) -> None:
            raise RuntimeError("boom")

    return [CoreModule(), _Raises()]


def deprecating_modules() -> list:
    import warnings

    from varco_core.di.module import VarcoModule

    class _Dep(VarcoModule):
        name = "dep"

        def _register(self, c: DIContainer) -> None:
            warnings.warn("cli deprecated thing", DeprecationWarning, stacklevel=2)

    return [CoreModule(), _Dep()]


def not_a_list() -> object:
    return object()


def complaining_modules() -> list:
    from varco_core.di.module import VarcoModule

    class _Complains(VarcoModule):
        name = "complains"

        def _register(self, c: DIContainer) -> None:
            pass

        def validate(self, c: DIContainer) -> list[str]:
            return ["this needs fixing"]

    return [CoreModule(), _Complains()]


def test_a_modules_source_exits_0_and_prints_seams_section(
    capsys,
) -> None:
    from varco_core.cli.main import main

    exit_code = main(["diagnose", "--modules", "tests.test_cli_diagnose:modules", "--static"])

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "Seams" in out
    assert "event_bus" in out


def test_b_json_flag_prints_parseable_document_with_schema_version_1(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(
        ["diagnose", "--modules", "tests.test_cli_diagnose:modules", "--static", "--json"]
    )

    out = capsys.readouterr().out
    assert exit_code == 0
    parsed = json.loads(out)
    assert parsed["schema_version"] == 1


def test_c_check_exits_0_on_a_clean_profile(capsys, monkeypatch) -> None:
    from varco_core.di import select

    monkeypatch.setattr(select, "_REGISTERED_VALUES", {})
    from varco_core.cli.main import main

    exit_code = main(
        ["diagnose", "--modules", "tests.test_cli_diagnose:modules", "--static", "--check"]
    )

    assert exit_code == 0


def test_c_check_exits_1_when_a_module_validate_warns(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(
        [
            "diagnose",
            "--modules",
            "tests.test_cli_diagnose:complaining_modules",
            "--static",
            "--check",
        ]
    )

    out_err = capsys.readouterr()
    assert exit_code == 1
    assert "this needs fixing" in out_err.out


def test_c_without_check_same_input_exits_0(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(
        ["diagnose", "--modules", "tests.test_cli_diagnose:complaining_modules", "--static"]
    )

    assert exit_code == 0


def test_d_static_flag_prints_banner(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(["diagnose", "--modules", "tests.test_cli_diagnose:modules", "--static"])

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "static" in out.lower()


def test_e_no_source_exits_2_with_stderr_message(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(["diagnose"])

    err = capsys.readouterr().err
    assert exit_code == 2
    assert err != ""


def test_e_modules_target_without_colon_exits_2(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(["diagnose", "--modules", "no_colon_here", "--static"])

    assert exit_code == 2


def test_e_modules_target_not_a_list_exits_2(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(["diagnose", "--modules", "tests.test_cli_diagnose:not_a_list", "--static"])

    assert exit_code == 2


def test_f_async_init_raising_exits_1_one_line_stderr_no_traceback(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(["diagnose", "--modules", "tests.test_cli_diagnose:raising_modules"])

    err = capsys.readouterr().err
    assert exit_code == 1
    assert "Traceback" not in err
    assert err.strip() == "_Raises.async_init failed: boom"


def test_check_ignores_info_only_findings(capsys, monkeypatch) -> None:
    """A fired deprecation is an INFO finding — it must never fail ``--check``."""
    from varco_core.cli.main import main
    from varco_core.di import select

    monkeypatch.setattr(select, "_REGISTERED_VALUES", {})
    exit_code = main(
        ["diagnose", "--modules", "tests.test_cli_diagnose:deprecating_modules", "--check"]
    )

    assert exit_code == 0
    assert "deprecation.fired" in capsys.readouterr().out


def test_g_container_source_works(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(["diagnose", "--container", "tests.test_cli_diagnose:container", "--static"])

    assert exit_code == 0


def test_i_help_lists_diagnose(capsys) -> None:
    from varco_core.cli.main import main

    exit_code = main(["--help"])

    out = capsys.readouterr().out
    assert exit_code == 0
    assert "diagnose" in out
