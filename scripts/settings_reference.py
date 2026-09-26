#!/usr/bin/env python3
"""Generate (and diff) the ``VARCO_*`` settings reference (Plan 048 / DI-6, §D-script).

Same shape as ``scripts/api_surface.py``: for the given packages, imports
every module (via ``varco_core.settings.inventory.discover_settings_classes``),
walks every discovered ``pydantic_settings.BaseSettings`` subclass with
``env_vars_for()``, and emits a sorted, deterministic snapshot — one entry
per class, one row per env var.

Two outputs:

* ``design/api-freeze-and-standards/measurements/settings-reference.json`` —
  machine-readable, the thing ``--check`` diffs against.
* ``technical_docs/settings-reference.md`` — the sibling, human-diffable page.

``--check`` fails on ANY difference — unlike ``api_surface.py --check``,
there is no "additive is fine" tier here: a new env var without a
regenerated reference is exactly the drift this gate exists to catch.

DESIGN: reuse ``varco_core.settings.inventory``, never a second field walk
    ✅ One place decides "is this env-readable", "is this secret", "what is
       the rendered default" — the same question ``bind_settings()``'s own
       error messages answer via the same module.
    ❌ A `varco_core` import cost is paid by this script — acceptable, the
       same cost ``api_surface.py`` already pays for every package.

Usage:
    uv run python scripts/settings_reference.py            # regenerate
    uv run python scripts/settings_reference.py --check    # diff, gate
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKAGES_SCRIPT = REPO_ROOT / "scripts" / "packages.sh"
DEFAULT_SNAPSHOT = (
    REPO_ROOT / "design" / "api-freeze-and-standards" / "measurements" / "settings-reference.json"
)
DEFAULT_MARKDOWN = REPO_ROOT / "technical_docs" / "settings-reference.md"


class SettingsReferenceError(RuntimeError):
    """A named package could not be imported/introspected.

    Raised instead of skipping the package — a silently shorter snapshot
    would hide every one of that package's settings classes rather than
    naming the actual import failure.
    """


def discover_packages() -> list[str]:
    """Return the distribution-package names, in ``[tool.uv.workspace]`` order.

    Identical derivation to ``scripts/api_surface.py::discover_packages`` —
    executes ``scripts/packages.sh`` rather than re-deriving the member
    list (Plan 020 / RL-18).

    Returns:
        Package names, order preserved.

    Raises:
        SettingsReferenceError: if ``scripts/packages.sh`` is missing or
            exits non-zero.
    """
    if not PACKAGES_SCRIPT.is_file():
        raise SettingsReferenceError(f"missing package-list script: {PACKAGES_SCRIPT}")
    try:
        completed = subprocess.run(
            ["bash", str(PACKAGES_SCRIPT)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - defensive
        raise SettingsReferenceError(
            f"{PACKAGES_SCRIPT.name} exited {exc.returncode}: {exc.stderr.strip()}"
        ) from exc
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def build_snapshot(packages: list[str]) -> dict[str, object]:
    """
    Build the deterministic, sorted settings snapshot for *packages*.

    Args:
        packages: Top-level importable package names.

    Returns:
        A JSON-serializable dict: ``{"classes": {class_qualname: [{env,
        field, type, default, secret, description}, ...]}}``, sorted by
        class qualname and, within a class, by field path.

    Raises:
        SettingsReferenceError: If any package fails to import — named,
            never silently absorbed into a shorter snapshot.
    """
    from varco_core.settings.inventory import discover_settings_classes, env_vars_for

    try:
        classes = discover_settings_classes(packages)
    except ImportError as exc:
        raise SettingsReferenceError(f"failed to import package(s) {packages}: {exc}") from exc

    result: dict[str, list[dict[str, object]]] = {}
    for cls in classes:
        qualname = f"{cls.__module__}.{cls.__qualname__}"
        rows = [
            {
                "env": spec.env,
                "field": spec.field_path,
                "type": spec.type_repr,
                "default": spec.default_repr,
                "secret": spec.secret,
                "description": spec.description,
            }
            for spec in sorted(env_vars_for(cls), key=lambda s: s.field_path)
        ]
        result[qualname] = rows

    return {"classes": dict(sorted(result.items()))}


def write_snapshot(packages: list[str], path: Path) -> dict[str, object]:
    """Build and write the snapshot to *path* (parents created as needed)."""
    snapshot = build_snapshot(packages)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n")
    return snapshot


def render_markdown(snapshot: dict[str, object]) -> str:
    """Render *snapshot* as the human-diffable reference page."""
    lines = ["# Settings Reference", "", "**Generated — never hand-edited.**", ""]
    classes = snapshot.get("classes", {})
    assert isinstance(classes, dict)
    for qualname, rows in classes.items():
        lines.append(f"## `{qualname}`")
        lines.append("")
        lines.append("| Env var | Field | Type | Default | Secret |")
        lines.append("|---|---|---|---|---|")
        for row in rows:
            lines.append(
                f"| `{row['env']}` | `{row['field']}` | `{row['type']}` | "
                f"`{row['default']}` | {row['secret']} |"
            )
        lines.append("")
    return "\n".join(lines)


def check(packages: list[str], snapshot_path: Path) -> int:
    """
    Diff the live tree's snapshot against the committed one.

    Args:
        packages: Packages to introspect.
        snapshot_path: Path to the committed JSON snapshot.

    Returns:
        ``0`` if identical, ``1`` otherwise (including when *snapshot_path*
        does not exist or does not parse as the expected shape).
    """
    live = build_snapshot(packages)
    try:
        committed = json.loads(snapshot_path.read_text())
    except (OSError, json.JSONDecodeError):
        committed = None

    if committed != live:
        print(f"settings-reference drift detected against {snapshot_path}", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="diff against the committed snapshot")
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--markdown", type=Path, default=DEFAULT_MARKDOWN)
    parser.add_argument("--packages", nargs="+", default=None)
    args = parser.parse_args(argv)

    packages = args.packages if args.packages is not None else discover_packages()

    if args.check:
        return check(packages, args.snapshot)

    snapshot = write_snapshot(packages, args.snapshot)
    args.markdown.parent.mkdir(parents=True, exist_ok=True)
    args.markdown.write_text(render_markdown(snapshot))
    print(f"wrote {args.snapshot} and {args.markdown}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
