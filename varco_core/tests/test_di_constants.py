"""
Plan 045 / Step 2 — `varco_core.di.BACKEND_PRIORITY`.

`varco_core/varco_core/di.py` is a new, bare module: a single `Final[int]`
constant used to declare the fallback priority tier every backend
implementation of a varco seam carries (§D-tiers). It must import nothing
from `providify` and register nothing — the standing "nothing scanned in
`varco_core` activates anything" rule (CLAUDE.md) is trivially met by a
module with no decorators at all, and the import-time budget
(`scripts/import_budget.py`) must not move.

RED MODE: `varco_core/varco_core/di.py` does not exist yet — every test here
is expected to fail with `ModuleNotFoundError`.
"""

from __future__ import annotations

import subprocess
import sys


def test_backend_priority_is_a_positive_int() -> None:
    from varco_core.di import BACKEND_PRIORITY

    assert isinstance(BACKEND_PRIORITY, int)
    assert BACKEND_PRIORITY > 0


def test_backend_priority_value_is_100() -> None:
    # Pinned exact value (§D-tiers candidate 1) — headroom above it is
    # deliberately left for Plan 047's env-override tier.
    from varco_core.di import BACKEND_PRIORITY

    assert BACKEND_PRIORITY == 100


def test_di_module_exports_backend_priority() -> None:
    # Plan 046 adds AlreadyBoundError/bind_instance/bootstrap/is_user_bound
    # to this module (test_di_wiring_primitives.py covers those) — this
    # test only pins that BACKEND_PRIORITY is still exported.
    import varco_core.di as di_module

    assert "BACKEND_PRIORITY" in di_module.__all__


def test_importing_di_module_does_not_import_providify() -> None:
    # Keeps varco_core.di free of providify — a bare Final constant module,
    # so importing it never pulls providify into sys.modules as a side
    # effect (the import-time budget must stay unaffected, Step 2 §Verify).
    #
    # DESIGN: a fresh subprocess, not an in-process sys.modules purge
    #   Plan 045's test-writer draft deleted every `providify`/`providify.*`
    #   entry from `sys.modules` in-process, then re-imported `providify`
    #   afterwards. That corrupts identity for the REST of the test session:
    #   providify's own classes (e.g. `ClassBinding`) get reconstructed as
    #   NEW class objects on re-import, so any `isinstance(binding,
    #   ClassBinding)` check elsewhere in the suite — comparing a binding
    #   built with the OLD class object against the NEW one — silently
    #   fails with `AttributeError`/`isinstance` mismatches for every test
    #   that runs afterward in the same process. Observed exactly this way
    #   while running the full `varco_core` suite (Plan 045 Step 7).
    #   ✅ A subprocess import is hermetic — nothing in THIS process's
    #      module graph is disturbed, so no later test can be corrupted.
    #   ❌ Slower than an in-process check (subprocess startup cost) —
    #      accepted; this is one test, not a hot loop.
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import varco_core.di; import sys; "
            "print('providify' in sys.modules or "
            "any(m.startswith('providify.') for m in sys.modules))",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "False", "varco_core.di must not import providify"


def test_env_override_priority_outranks_backend_priority() -> None:
    # Plan 047 §D-precedence: the env-dotted-path tier must outrank the
    # named-backend tier so a dotted override wins among fallbacks even
    # when a shipped backend is also active.
    from varco_core.di import BACKEND_PRIORITY, ENV_OVERRIDE_PRIORITY

    assert isinstance(ENV_OVERRIDE_PRIORITY, int)
    assert ENV_OVERRIDE_PRIORITY > BACKEND_PRIORITY > 0


def test_env_override_priority_value_is_200() -> None:
    # Pinned exact value (§D-precedence: "200").
    from varco_core.di import ENV_OVERRIDE_PRIORITY

    assert ENV_OVERRIDE_PRIORITY == 200


def test_di_package_exports_plan_047_names() -> None:
    # Plan 047 turns varco_core.di into a package; __init__.py must
    # re-export 045/046's names unchanged plus the new selection/feature/
    # introspection surface (Done criteria list).
    import varco_core.di as di_module

    for name in (
        "BACKEND_PRIORITY",
        "ENV_OVERRIDE_PRIORITY",
        "AlreadyBoundError",
        "bind_instance",
        "bootstrap",
        "is_user_bound",
        "SEAMS",
        "FEATURES",
        "selected_by",
        "feature_default",
        "env_truthy",
        "bind_env_overrides",
        "check_backend_selection",
        "inspect_backend_selection",
        "inspect_bindings",
        "catalogue",
        "BackendSelectionError",
        "CatalogueError",
    ):
        assert name in di_module.__all__, f"{name} missing from varco_core.di.__all__"
        assert hasattr(di_module, name), f"{name} missing from varco_core.di"
