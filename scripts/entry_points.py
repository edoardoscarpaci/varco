#!/usr/bin/env python3
"""Generate (and diff) the DI wiring entry-point table (Plan 058 / DI-14, §D-entrypoints).

A sibling of ``scripts/api_surface.py``: the same ``scripts/packages.sh``
derivation (RL-18), but instead of walking each package's top-level
``__all__`` it walks **every module** of every package with
``pkgutil.walk_packages`` (the ``settings_reference.py`` precedent) and
classifies every public *wiring* entry point into one of eight families:

=====================  ==========================================================
Family                 Selector (all: public name, defined in the walked module)
=====================  ==========================================================
``bootstrap``          module-level function named exactly ``bootstrap``
``async_bootstrap``    module-level function named exactly ``async_bootstrap``
``bind``               module-level function named ``bind_*``
``enable``             module-level function named ``enable_*``
``mount``              module-level function named ``mount_*``
``install``            module-level function named ``install_*``
``module``             class, a ``varco_core.di.VarcoModule`` subclass (not the base)
``configuration``      class carrying providify's ``@Configuration`` marker
=====================  ==========================================================

Three outputs, all sorted and byte-stable:

* ``design/api-freeze-and-standards/measurements/entry-points.json`` — what
  ``--check`` diffs (exact; no "additive is fine" tier — the 047/048 rule).
* ``technical_docs/entry-points.md`` — the generated reference page.
* the same tables inside ``ARCHITECTURE.md``, between
  ``<!-- entry-points:begin -->`` / ``<!-- entry-points:end -->`` — ``--check``
  diffs that block too, so the hand-edited file cannot drift.

DESIGN: a separate script, ``walk_packages``, three outputs, a marked block
  ✅ Same package derivation as ``api_surface.py`` (DI-14's literal ask)
     without overloading a gate whose documented scope is "removals and
     function signature changes only".
  ✅ ``walk_packages`` sees ``<pkg>.di`` / ``<pkg>.modules`` /
     ``varco_core.webhook.module`` / ``varco_core.testing`` — names outside any
     top-level ``__all__``, invisible to an ``api_surface.py`` mode.
  ✅ ARCHITECTURE.md carries the generated table, and the docs site gets it too.
  ❌ A third verbatim copy of ``discover_packages()`` (a shared
     ``scripts/_packages.py`` is parked until a fifth script needs it).
  ❌ A generator writing into a hand-edited file needs markers — a new pattern
     here; the ``--check`` diff of the block is what makes it safe.

Alternatives considered:
  * ``api_surface.py --entry-points`` — rejected: a second walker and a second
    snapshot inside a gate named "api surface --check"; a failure would be
    ambiguous.
  * A hand-written table — rejected outright: the BACKLOG row exists because
    the hand-written taxonomy table rotted.
  * ``technical_docs/entry-points.md`` only, ARCHITECTURE.md linking to it —
    rejected: DI-14 asks for the ARCHITECTURE.md table itself to be generated.

Edge cases:
  * Classes get **no** signature — synthesised ``__init__`` rendering is not
    stable across the 3.12/3.13 matrix (``api_surface.py``'s rule). Module
    rows carry plain class attributes instead (``name``/``phase``/
    ``depends_on``), which are interpreter-stable.
  * ``examples/`` is not a distribution package (``packages.sh``), so a
    ``VarcoModule`` there is neither walked nor a completeness error.
  * A package that fails to import raises :class:`EntryPointsError` naming it,
    never a shorter snapshot.

Usage:
    uv run python scripts/entry_points.py                       # regenerate all three
    uv run python scripts/entry_points.py --check               # diff, gate
    uv run python scripts/entry_points.py --packages varco_core # print rows, write nothing
"""

from __future__ import annotations

import argparse
import importlib
import inspect
import json
import pkgutil
import re
import subprocess
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path
from types import MappingProxyType, ModuleType
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKAGES_SCRIPT = REPO_ROOT / "scripts" / "packages.sh"
DEFAULT_SNAPSHOT = (
    REPO_ROOT / "design" / "api-freeze-and-standards" / "measurements" / "entry-points.json"
)
DEFAULT_MARKDOWN = REPO_ROOT / "technical_docs" / "entry-points.md"
DEFAULT_ARCHITECTURE = REPO_ROOT / "ARCHITECTURE.md"

BEGIN_MARKER = "<!-- entry-points:begin -->"
END_MARKER = "<!-- entry-points:end -->"

#: Same heap-address stripping as ``api_surface.py`` — sentinel defaults render
#: ``<... object at 0x7f...>``, which would differ on every run.
_ADDRESS_RE = re.compile(r" at 0x[0-9a-fA-F]+>")

#: Function-name selectors, checked in order. ``bootstrap``/``async_bootstrap``
#: are exact names; the four verbs are prefixes.
_EXACT_FUNCTIONS = ("bootstrap", "async_bootstrap")
_PREFIX_FAMILIES = (
    ("bind_", "bind"),
    ("enable_", "enable"),
    ("mount_", "mount"),
    ("install_", "install"),
)

#: Rendering order of the families (the snapshot itself is sorted by key).
FAMILY_ORDER = (
    "bootstrap",
    "async_bootstrap",
    "bind",
    "enable",
    "mount",
    "install",
    "module",
    "configuration",
)

#: Names the selectors match that are NOT wiring entry points, keyed
#: ``"module:name"`` → reason (mirrors ``settings_reference``'s exclusion
#: table). A stale key is an error — ``varco_core/tests/test_entry_points_script.py``
#: resolves every one. Router-internal ``enable_subscription``/``install_*``
#: *methods* (046 §Context) are never module-level functions, so they need no
#: entry here; the walk does not see methods at all. The ``varco_plugin_example``
#: testkit package is never packaged, so ``packages.sh`` never lists it and it
#: needs no entry either.
_NOT_ENTRY_POINTS: Mapping[str, str] = MappingProxyType(
    {
        "varco_sa.rls:enable_rls_ddl": (
            "deprecated alias of render_rls_ddl (AB-1) — a pure DDL-string generator, "
            "never an enable_* wiring verb; removed in 5.0.0"
        ),
    }
)


class EntryPointsError(RuntimeError):
    """The walk or the ``--check`` could not produce a trustworthy answer.

    Raised for: a package that fails to import (a silently shorter snapshot
    would hide every one of its entry points), a ``VarcoModule`` subclass the
    walk could not reach through any package module (the completeness guard),
    and an ``ARCHITECTURE.md`` with no marker pair.
    """


def discover_packages() -> list[str]:
    """Return the distribution-package names, in ``[tool.uv.workspace]`` order.

    Identical derivation to ``scripts/api_surface.py::discover_packages`` —
    executes ``scripts/packages.sh`` rather than re-deriving the member list.

    DESIGN: shell out to ``packages.sh`` over re-deriving from ``pyproject.toml``
      ✅ One derivation, not two (Plan 020 / RL-18).
      ✅ The Makefile, ``unit_tests.sh`` and ``integration_tests.sh`` already
         consume it the same way, so there is one observable list.
      ❌ Requires ``bash`` on PATH and one subprocess per run. Accepted.

    Returns:
        Package names, one per line of the script's stdout, order preserved.

    Raises:
        EntryPointsError: if ``scripts/packages.sh`` is missing or exits non-zero.

    Example:
        >>> "varco_core" in discover_packages()
        True
    """
    if not PACKAGES_SCRIPT.is_file():
        raise EntryPointsError(f"missing package-list script: {PACKAGES_SCRIPT}")
    try:
        completed = subprocess.run(
            ["bash", str(PACKAGES_SCRIPT)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - defensive
        raise EntryPointsError(
            f"{PACKAGES_SCRIPT.name} exited {exc.returncode}: {exc.stderr.strip()}"
        ) from exc
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _iter_modules(package: str) -> list[ModuleType]:
    """Import *package* and every submodule beneath it.

    Args:
        package: A top-level importable package name.

    Returns:
        The imported modules, the package root first, then in
        ``walk_packages`` order.

    Raises:
        EntryPointsError: If the package or any submodule fails to import.

    Edge cases:
        * ``__main__`` modules are skipped — importing one would *run* a CLI.
    """
    try:
        root = importlib.import_module(package)
    except ImportError as exc:
        raise EntryPointsError(
            f"could not import package {package!r}: {exc}. "
            "Run `uv sync --all-packages --all-extras`."
        ) from exc
    modules = [root]
    path = getattr(root, "__path__", None)
    if path is None:
        return modules

    def _onerror(name: str) -> None:
        raise EntryPointsError(f"could not import {name!r} while walking {package!r}")

    for info in pkgutil.walk_packages(path, prefix=f"{package}.", onerror=_onerror):
        if info.name.rsplit(".", 1)[-1] == "__main__":
            continue
        try:
            modules.append(importlib.import_module(info.name))
        except ImportError as exc:
            raise EntryPointsError(f"could not import {info.name!r}: {exc}") from exc
    return modules


def _signature(fn: Any) -> str:
    """Render ``inspect.signature(fn)`` with heap addresses stripped.

    Args:
        fn: A function object.

    Returns:
        The signature string, or ``""`` when ``inspect`` cannot render one.
    """
    try:
        return _ADDRESS_RE.sub(">", str(inspect.signature(fn)))
    except (TypeError, ValueError):  # pragma: no cover - C-level callables
        return ""


def _function_family(name: str) -> str | None:
    """Return the family a module-level function name selects, or ``None``."""
    if name in _EXACT_FUNCTIONS:
        return name
    for prefix, family in _PREFIX_FAMILIES:
        if name.startswith(prefix):
            return family
    return None


def _phase_label(value: object) -> str | int:
    """Render a module's ``phase`` as ``Phase.NAME`` when it is a member, else the int."""
    from varco_core.di import Phase

    if isinstance(value, Phase):
        return f"Phase.{value.name}"
    if isinstance(value, int):
        try:
            return f"Phase.{Phase(value).name}"
        except ValueError:
            return int(value)
    return str(value)


def _class_names(types: Iterable[object]) -> list[str]:
    """Return the ``__name__`` of each element (``repr`` for a non-class)."""
    return [t.__name__ if isinstance(t, type) else repr(t) for t in types]


def classify(module: ModuleType, package: str) -> list[dict[str, Any]]:
    """Classify every public entry point *defined in* ``module``.

    Args:
        module: An imported module of ``package``.
        package: The distribution package the module belongs to.

    Returns:
        One row per entry point (unsorted). Every row has ``family``,
        ``package``, ``module``, ``name``; function rows add ``signature``;
        ``module`` rows add ``module_name`` (the class's ``name`` attribute),
        ``phase`` and ``depends_on``; ``configuration`` rows add ``depends_on``.

    Edge cases:
        * Re-exports are skipped (``obj.__module__`` must equal the module
          being walked), so each entry point appears exactly once.
        * Names in :data:`_NOT_ENTRY_POINTS` are skipped.
    """
    from providify import metadata as providify_metadata
    from varco_core.di import VarcoModule

    rows: list[dict[str, Any]] = []
    for name, obj in sorted(vars(module).items()):
        if name.startswith("_") or getattr(obj, "__module__", None) != module.__name__:
            continue
        if f"{module.__name__}:{name}" in _NOT_ENTRY_POINTS:
            continue
        base = {"package": package, "module": module.__name__, "name": name}
        if inspect.isfunction(obj):
            family = _function_family(name)
            if family is not None:
                rows.append({"family": family, **base, "signature": _signature(obj)})
            continue
        if not inspect.isclass(obj):
            continue
        if issubclass(obj, VarcoModule) and obj is not VarcoModule:
            rows.append(
                {
                    "family": "module",
                    **base,
                    "module_name": str(getattr(obj, "name", "")),
                    "phase": _phase_label(getattr(obj, "phase", None)),
                    "depends_on": _class_names(getattr(obj, "depends_on", ())),
                }
            )
        elif providify_metadata._has_configuration_module(obj):
            meta = providify_metadata._get_configuration_module(obj)
            depends_on = meta.depends_on if meta is not None else ()
            rows.append({"family": "configuration", **base, "depends_on": _class_names(depends_on)})
    return rows


def _all_subclasses(cls: type) -> list[type]:
    """Return every transitive subclass of ``cls`` (excluding ``cls`` itself)."""
    out: list[type] = []
    for sub in cls.__subclasses__():
        out.append(sub)
        out.extend(_all_subclasses(sub))
    return out


def walk_entry_points(packages: Iterable[str]) -> list[dict[str, Any]]:
    """Walk every module of *packages* and return the sorted entry-point rows.

    Args:
        packages: Distribution-package names, typically :func:`discover_packages`.

    Returns:
        Rows sorted by ``(family, package, module, name)`` — deterministic
        across runs, so the JSON and Markdown outputs are byte-stable.

    Raises:
        EntryPointsError: If a package fails to import, or if a
            ``VarcoModule`` subclass whose ``__module__`` lies inside one of
            *packages*' namespaces was not reached by the walk (the
            completeness guard — a module defined somewhere the packages do
            not import, or inside a function, is a bug, not a note).

    Example:
        >>> rows = walk_entry_points(["varco_core"])
        >>> any(r["name"] == "CoreModule" for r in rows)
        True
    """
    from varco_core.di import VarcoModule

    packages = list(packages)
    rows: list[dict[str, Any]] = []
    for package in packages:
        for module in _iter_modules(package):
            rows.extend(classify(module, package))

    # Completeness guard, scoped to the walked packages' own namespaces so a
    # test-local subclass elsewhere in a test session never trips it.
    reached = {(r["module"], r["name"]) for r in rows if r["family"] == "module"}
    unreachable = sorted(
        f"{cls.__module__}.{cls.__qualname__}"
        for cls in _all_subclasses(VarcoModule)
        if any(cls.__module__ == p or cls.__module__.startswith(f"{p}.") for p in packages)
        and (cls.__module__, cls.__qualname__) not in reached
        and f"{cls.__module__}:{cls.__qualname__}" not in _NOT_ENTRY_POINTS
    )
    if unreachable:
        raise EntryPointsError(
            "VarcoModule subclass(es) not reachable as a module-level class of any "
            f"walked module: {', '.join(unreachable)}"
        )
    return sorted(rows, key=lambda r: (r["family"], r["package"], r["module"], r["name"]))


def build_snapshot(packages: Iterable[str]) -> dict[str, Any]:
    """Build the JSON-serialisable snapshot ``{"entry_points": [row, ...]}``.

    Args:
        packages: Packages to walk.

    Returns:
        The snapshot mapping.

    Raises:
        EntryPointsError: Propagated from :func:`walk_entry_points`.
    """
    return {"entry_points": walk_entry_points(packages)}


def write_snapshot(packages: Iterable[str], path: Path) -> dict[str, Any]:
    """Build the snapshot and write it to *path* (parents created).

    Args:
        packages: Packages to walk.
        path: Destination JSON file.

    Returns:
        The snapshot that was written.

    Raises:
        EntryPointsError: Propagated from :func:`walk_entry_points`.
    """
    snapshot = build_snapshot(packages)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return snapshot


def _cell(text: object) -> str:
    """Escape a Markdown table cell (a ``|`` in a ``X | Y`` union breaks the row)."""
    return str(text).replace("|", "\\|")


def _render_tables(rows: list[dict[str, Any]], heading: str) -> list[str]:
    """Render one Markdown table per family, in :data:`FAMILY_ORDER`.

    Args:
        rows: Entry-point rows (any order; re-sorted per family).
        heading: The Markdown heading prefix for each family (``"##"`` …).

    Returns:
        The rendered lines.
    """
    lines: list[str] = []
    for family in FAMILY_ORDER:
        fam = sorted(
            (r for r in rows if r["family"] == family),
            key=lambda r: (r["package"], r["module"], r["name"]),
        )
        if not fam:
            continue
        lines += [f"{heading} `{family}` ({len(fam)})", ""]
        if family == "module":
            lines += [
                "| Name | Package | Module | `name` | `phase` | `depends_on` |",
                "|---|---|---|---|---|---|",
            ]
            for r in fam:
                deps = ", ".join(r["depends_on"]) or "—"
                lines.append(
                    f"| `{r['name']}` | {r['package']} | `{r['module']}` | "
                    f"`{_cell(r['module_name'])}` | `{r['phase']}` | {_cell(deps)} |"
                )
        elif family == "configuration":
            lines += ["| Name | Package | Module | `depends_on` |", "|---|---|---|---|"]
            for r in fam:
                deps = ", ".join(r["depends_on"]) or "—"
                lines.append(
                    f"| `{r['name']}` | {r['package']} | `{r['module']}` | {_cell(deps)} |"
                )
        else:
            lines += ["| Name | Package | Module | Signature |", "|---|---|---|---|"]
            for r in fam:
                lines.append(
                    f"| `{r['name']}` | {r['package']} | `{r['module']}` | "
                    f"`{_cell(r['signature'])}` |"
                )
        lines.append("")
    return lines


def render_markdown(rows: list[dict[str, Any]]) -> str:
    """Render the standalone ``technical_docs/entry-points.md`` page.

    Args:
        rows: Entry-point rows from :func:`walk_entry_points`.

    Returns:
        The page text, ending in a newline.
    """
    lines = [
        "# Entry Points",
        "",
        "**Generated by `make entry-points` (`scripts/entry_points.py`) — never hand-edited.**",
        "`make lint` fails when this page, its JSON snapshot, or the matching block in",
        "`ARCHITECTURE.md` drifts from the live tree (`make entry-points-check`).",
        "",
        "Every DI wiring entry point of the ten packages: the `bootstrap()`s, every",
        "`bind_*`/`enable_*`/`mount_*`/`install_*` function, every `VarcoModule` and every",
        "providify `@Configuration`. What each verb *means*: "
        "[the DI guide's Wiring contract](features/dependency-injection.md#wiring-contract).",
        "",
        *_render_tables(rows, "##"),
    ]
    return "\n".join(lines).rstrip("\n") + "\n"


def render_block(rows: list[dict[str, Any]]) -> str:
    """Render the text that sits between the ``ARCHITECTURE.md`` markers.

    Args:
        rows: Entry-point rows from :func:`walk_entry_points`.

    Returns:
        The block text, without the marker lines and without a trailing newline.
    """
    lines = [
        "<!-- generated — `make entry-points`; never hand-edit between these markers -->",
        "",
        *_render_tables(rows, "####"),
    ]
    return "\n".join(lines).rstrip("\n")


def _split_architecture(text: str, path: Path) -> tuple[str, str, str]:
    """Split *text* into (before, inside, after) the marker pair.

    Raises:
        EntryPointsError: If either marker is missing or they are out of order.
    """
    begin = text.find(BEGIN_MARKER)
    end = text.find(END_MARKER)
    if begin == -1 or end == -1 or end < begin:
        raise EntryPointsError(
            f"{path} has no `{BEGIN_MARKER}` … `{END_MARKER}` marker pair; "
            "add both where the generated entry-point tables belong."
        )
    inner_start = begin + len(BEGIN_MARKER)
    return text[:inner_start], text[inner_start:end], text[end:]


def write_architecture(rows: list[dict[str, Any]], path: Path) -> None:
    """Rewrite the marked block of *path* with :func:`render_block`.

    Args:
        rows: Entry-point rows.
        path: The ``ARCHITECTURE.md`` to update in place.

    Raises:
        EntryPointsError: If the marker pair is missing.
    """
    before, _inside, after = _split_architecture(path.read_text(encoding="utf-8"), path)
    path.write_text(f"{before}\n{render_block(rows)}\n{after}", encoding="utf-8")


def _row_key(row: Mapping[str, Any]) -> str:
    """A human-readable identity for a row in diff output."""
    return f"{row.get('family')} {row.get('module')}:{row.get('name')}"


def check(packages: Iterable[str], snapshot: Path, *, architecture: Path | None = None) -> int:
    """Diff the live tree against the committed snapshot (and ARCHITECTURE.md block).

    Args:
        packages: Packages to walk.
        snapshot: The committed JSON snapshot.
        architecture: When given, the marked block of this file must equal
            :func:`render_block` of the live rows.

    Returns:
        ``0`` on an exact match; ``1`` on any difference (each differing row
        is named, as ``family module:name``, on stdout); ``2`` if the snapshot
        file is missing or unreadable.

    Raises:
        EntryPointsError: If *architecture* has no marker pair, or the walk fails.
    """
    rows = walk_entry_points(packages)
    try:
        committed = json.loads(snapshot.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"entry-points snapshot unreadable: {snapshot} ({exc})", file=sys.stderr)
        return 2

    status = 0
    committed_rows = committed.get("entry_points", []) if isinstance(committed, dict) else []
    live_by_key = {_row_key(r): r for r in rows}
    old_by_key = {_row_key(r): r for r in committed_rows}
    for key in sorted(old_by_key.keys() - live_by_key.keys()):
        print(f"REMOVED  {key}")
        status = 1
    for key in sorted(live_by_key.keys() - old_by_key.keys()):
        print(f"ADDED    {key}")
        status = 1
    for key in sorted(live_by_key.keys() & old_by_key.keys()):
        if live_by_key[key] != old_by_key[key]:
            print(f"CHANGED  {key}")
            status = 1

    if architecture is not None:
        _before, inside, _after = _split_architecture(
            architecture.read_text(encoding="utf-8"), architecture
        )
        if inside.strip("\n") != render_block(rows):
            print(f"DRIFT    {architecture}: the {BEGIN_MARKER} block is stale")
            status = 1

    if status:
        print("entry points drifted — run `make entry-points` and commit the result.")
    else:
        print(f"entry points match {snapshot.name} ({len(rows)} rows).")
    return status


def main(argv: list[str] | None = None) -> int:
    """CLI entry point.

    Args:
        argv: Arguments (defaults to ``sys.argv[1:]``).

    Returns:
        The process exit code.

    Edge cases:
        * ``--packages`` narrows the walk; without an explicit
          ``--snapshot``/``--markdown``/``--architecture`` it then *prints* the
          rows and writes nothing — a subset must never overwrite the
          committed, all-package outputs.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="diff against the committed outputs")
    parser.add_argument("--snapshot", type=Path, default=None)
    parser.add_argument("--markdown", type=Path, default=None)
    parser.add_argument("--architecture", type=Path, default=None)
    parser.add_argument("--packages", nargs="+", default=None)
    args = parser.parse_args(argv)

    subset = args.packages is not None
    packages = args.packages if subset else discover_packages()
    explicit = any(p is not None for p in (args.snapshot, args.markdown, args.architecture))
    snapshot = args.snapshot or DEFAULT_SNAPSHOT
    markdown = args.markdown or (None if subset else DEFAULT_MARKDOWN)
    architecture = args.architecture or (None if subset else DEFAULT_ARCHITECTURE)

    try:
        if args.check:
            return check(packages, snapshot, architecture=architecture)
        if subset and not explicit:
            rows = walk_entry_points(packages)
            for r in rows:
                print(f"{r['family']}\t{r['module']}\t{r['name']}")
            print(f"{len(rows)} rows", file=sys.stderr)
            return 0
        rows = write_snapshot(packages, snapshot)["entry_points"]
        written = [str(snapshot)]
        if markdown is not None:
            markdown.parent.mkdir(parents=True, exist_ok=True)
            markdown.write_text(render_markdown(rows), encoding="utf-8")
            written.append(str(markdown))
        if architecture is not None:
            write_architecture(rows, architecture)
            written.append(str(architecture))
    except EntryPointsError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"wrote {len(rows)} rows to {', '.join(written)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
