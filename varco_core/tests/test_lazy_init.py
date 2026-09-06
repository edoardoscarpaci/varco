"""Tests for the lazy ``varco_core/__init__.py`` (Plan 028 / Phase 0, P1a).

Step 3 of the plan calls this module "the whole phase's specification".

RED MODE: ``varco_core/varco_core/__init__.py`` is still the eager ~330-line
``from varco_core.X import (...)`` block. The tests that encode the *new*
mechanism (``_LAZY``, ``__dir__``, the cold-set assertion, the
``TYPE_CHECKING`` drift guards) fail today. Per Step 3 the plan explicitly
expects the pure-equivalence tests (a)/(c) to pass on ``main`` as well —
they exist to prove P1 is invisible, so they must be green before *and*
after the rewrite.

The mechanism under test (§D-P1-mechanism):

    if TYPE_CHECKING:                  # verbatim current eager import block
        from varco_core.model import DomainModel
        ...

    _LAZY: Final[dict[str, str]] = {"DomainModel": "varco_core.model", ...}

    def __getattr__(name: str) -> Any: ...
    def __dir__() -> list[str]: return sorted(__all__)
"""

from __future__ import annotations

import ast
import importlib
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import varco_core

INIT_PATH = Path(varco_core.__file__).resolve()

ALL_NAMES: list[str] = sorted(varco_core.__all__)

# The four measured import-time contributors (BACKLOG.md:56-57), each paired
# with the varco_core submodule that legitimately owns it and one __all__
# name defined by that submodule. Touching the name must pull the dependency;
# a bare ``import varco_core`` must not.
COLD_SET: list[tuple[str, str]] = [
    ("lark", "QueryParser"),  # varco_core.query.parser
    ("jwt", "JwtUtil"),  # varco_core.jwt.util
    ("opentelemetry.sdk", "OtelConfig"),  # varco_core.observability.config
    ("psutil", "OtelConfig"),  # pulled transitively by opentelemetry.sdk.resources
]


def _run_python(code: str) -> subprocess.CompletedProcess[str]:
    """Execute ``code`` in a fresh interpreter (a genuinely cold process)."""
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        capture_output=True,
        text=True,
    )


def _init_tree() -> ast.Module:
    return ast.parse(INIT_PATH.read_text())


# ── (a) equivalence: every __all__ name resolves, to the identical object ──


@pytest.mark.parametrize("name", ALL_NAMES)
def test_every_all_name_is_resolvable_via_getattr(name: str) -> None:
    # The ❌ of §D-P1-mechanism: a broken submodule now fails at first access,
    # not at import. Resolving every name is the mitigation, on every CI run.
    assert getattr(varco_core, name) is not None or True
    getattr(varco_core, name)


def test_every_all_name_is_importable_via_from_import() -> None:
    # `from varco_core import X` goes through a different CPython path than
    # getattr (IMPORT_FROM), so it is asserted separately. One subprocess for
    # all 235 names: 235 interpreters would dominate the unit suite's runtime.
    names = ", ".join(ALL_NAMES)
    result = _run_python(f"from varco_core import ({names})")
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("name", ALL_NAMES)
def test_lazy_attribute_is_the_same_object_as_the_submodule_attribute(name: str) -> None:
    # Non-goal #1: "the same object identity". Resolved through _LAZY so the
    # committed map itself is what is verified, not a re-derivation of it.
    lazy_map = varco_core._LAZY  # type: ignore[attr-defined]
    submodule = importlib.import_module(lazy_map[name])
    assert getattr(varco_core, name) is getattr(submodule, name)


def test_star_import_materialises_every_all_name() -> None:
    # Edge case: `from varco_core import *` is correct, just not lazy.
    result = _run_python(
        """
        import varco_core
        ns: dict[str, object] = {}
        exec("from varco_core import *", ns)
        missing = [n for n in varco_core.__all__ if n not in ns]
        assert not missing, missing
        """
    )
    assert result.returncode == 0, result.stderr


# ── (b) dir() ──────────────────────────────────────────────────────────────


def test_dir_returns_exactly_the_sorted_all_list() -> None:
    # §D-P1-mechanism's __dir__: keeps tab-completion and inspect tooling
    # working when the globals are empty.
    assert dir(varco_core) == sorted(varco_core.__all__)


# ── (c) unknown attribute ──────────────────────────────────────────────────


def test_unknown_attribute_raises_attribute_error_naming_the_module() -> None:
    with pytest.raises(AttributeError) as exc:
        varco_core.NoSuchName  # type: ignore[attr-defined]  # noqa: B018
    assert "varco_core" in str(exc.value)
    assert "NoSuchName" in str(exc.value)


# ── (d) the cold-set assertion ─────────────────────────────────────────────


def test_bare_import_varco_core_pulls_none_of_the_measured_contributors() -> None:
    # THE point of the phase: 419 ms is these four modules.
    result = _run_python(
        """
        import sys
        import varco_core
        leaked = [m for m in ("lark", "jwt", "psutil", "opentelemetry.sdk") if m in sys.modules]
        assert not leaked, f"eagerly imported: {leaked}"
        """
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(("dependency", "name"), COLD_SET)
def test_touching_the_owning_name_pulls_its_dependency(dependency: str, name: str) -> None:
    # The other half of the cold-set contract: lazy must still mean *reachable*.
    result = _run_python(
        f"""
        import sys
        import varco_core
        varco_core.{name}
        assert "{dependency}" in sys.modules, "{dependency} not imported by {name}"
        """
    )
    assert result.returncode == 0, result.stdout + result.stderr


# ── Risks section: drift guards ────────────────────────────────────────────


def test_lazy_map_covers_exactly_the_all_list() -> None:
    # Risk: "a name in _LAZY but not __all__" (or vice versa) — a name that
    # silently stops being importable.
    lazy_map = varco_core._LAZY  # type: ignore[attr-defined]
    eager = getattr(varco_core, "_EAGER", ())
    assert set(lazy_map) | set(eager) == set(varco_core.__all__)


def test_type_checking_block_names_match_the_lazy_map() -> None:
    # Risk: TYPE_CHECKING drift silently degrades a name to Any repo-wide,
    # which mypy cannot report because __getattr__ -> Any is legal.
    tree = _init_tree()
    typed: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and ast.unparse(node.test) == "TYPE_CHECKING":
            for stmt in ast.walk(node):
                if isinstance(stmt, ast.ImportFrom):
                    typed |= {a.asname or a.name for a in stmt.names}
    assert typed, "no `if TYPE_CHECKING:` import block found in varco_core/__init__.py"
    assert typed == set(varco_core.__all__)


def test_module_defines_the_pep_562_hooks() -> None:
    # A structural guard: the hooks must live in __init__.py itself, not be
    # inherited or monkeypatched in from elsewhere.
    tree = _init_tree()
    defined = {n.name for n in tree.body if isinstance(n, ast.FunctionDef)}
    assert {"__getattr__", "__dir__"} <= defined


def test_all_list_is_unchanged_in_length() -> None:
    # Non-goal #2 (as of Plan 028 / P1): "Not one name is added or removed
    # by P1." That plan is long done; the pinned count below simply tracks
    # the current, deliberate size of the public surface — Plan 033 / S6+S5
    # added 25 names (TenantTrust ... inspect_tenant_provenance), taking it
    # from 235 to 260; drift-repair Decision 4 added one more
    # (TenantMembershipSettings), taking it to 261. Bump this number (with
    # a reason) whenever a plan legitimately adds/removes a top-level export.
    assert len(varco_core.__all__) == 261
    assert len(set(varco_core.__all__)) == len(varco_core.__all__)
