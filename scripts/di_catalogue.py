#!/usr/bin/env python3
"""Generate (and verify) the default-implementation catalogue (DI-17).

Plan 047 §D-script. For every workspace package (derived from
``scripts/packages.sh``, RL-18 — the same discipline
``scripts/api_surface.py``/``scripts/bump.py`` follow), builds one
``DIContainer``, bootstraps ``varco_core`` plus that package (binding the
minimal settings a package needs to scan cleanly — ``SAConfig`` with a
sqlite engine, ``BeanieSettings`` with a lazy, never-connected
``AsyncMongoClient`` — nothing actually connects), classifies every
``@Fallback`` binding via ``varco_core.di.catalogue()``, and merges the
per-package results into one document.

Two outputs, the same shape as ``api_surface.py``:

* ``design/api-freeze-and-standards/measurements/di-catalogue.json`` — the
  machine-readable snapshot ``--check`` diffs.
* ``technical_docs/di-catalogue.md`` — sorted, human-diffable, generated —
  never hand-edited.

Usage:
    uv run python scripts/di_catalogue.py            # regenerate .json + .md
    uv run python scripts/di_catalogue.py --check    # diff vs committed, gate
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
PACKAGES_SCRIPT = REPO_ROOT / "scripts" / "packages.sh"
DEFAULT_SNAPSHOT = (
    REPO_ROOT / "design" / "api-freeze-and-standards" / "measurements" / "di-catalogue.json"
)
DEFAULT_DOC = REPO_ROOT / "technical_docs" / "di-catalogue.md"


class DiCatalogueError(RuntimeError):
    """A package could not be bootstrapped/classified — never silently skipped."""


def discover_packages() -> list[str]:
    """Return the distribution-package names (same shape as `api_surface.py`).

    Returns:
        Package names, in `scripts/packages.sh`'s order.

    Raises:
        DiCatalogueError: `scripts/packages.sh` is missing or exits non-zero.
    """
    if not PACKAGES_SCRIPT.is_file():
        raise DiCatalogueError(f"missing package-list script: {PACKAGES_SCRIPT}")
    try:
        completed = subprocess.run(
            ["bash", str(PACKAGES_SCRIPT)],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - defensive
        raise DiCatalogueError(
            f"{PACKAGES_SCRIPT.name} exited {exc.returncode}: {exc.stderr.strip()}"
        ) from exc
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _bootstrap_package(package: str) -> Any:
    """Build one bootstrapped `DIContainer` for `package` (+ `varco_core`).

    Args:
        package: A workspace package name (e.g. ``"varco_redis"``).

    Returns:
        A `DIContainer` with `varco_core.di.bootstrap()` and the package's
        own `bootstrap()` (or equivalent scan) applied.

    Raises:
        DiCatalogueError: The package could not be imported/bootstrapped —
            named explicitly, never a silently smaller snapshot.
    """
    from providify import DIContainer, Provider
    from varco_core.di import bootstrap as core_bootstrap

    container = DIContainer()
    try:
        core_bootstrap(container)

        if package == "varco_core":
            pass
        elif package == "varco_sa":
            from sqlalchemy.ext.asyncio import create_async_engine
            from sqlalchemy.orm import DeclarativeBase
            from varco_core.di import bind_instance
            from varco_sa.bootstrap import SAConfig
            from varco_sa.di import bootstrap as sa_bootstrap

            class _Base(DeclarativeBase):
                pass

            bind_instance(
                container,
                SAConfig,
                SAConfig(
                    engine=create_async_engine("sqlite+aiosqlite:///:memory:"),
                    base=_Base,
                    entity_classes=(),
                ),
            )
            sa_bootstrap(container)
        elif package == "varco_beanie":
            from pymongo import AsyncMongoClient
            from varco_beanie.config import BeanieSettings
            from varco_beanie.di import bootstrap as beanie_bootstrap

            @Provider(singleton=True)
            def _beanie_settings() -> Any:
                return BeanieSettings(
                    mongo_client=AsyncMongoClient(
                        "mongodb://127.0.0.1:1/?connectTimeoutMS=1&serverSelectionTimeoutMS=1"
                    ),
                    db_name="di_catalogue",
                )

            # returns= override — the return annotation above is `Any`
            # (never eval()'d as a real interface) precisely so this
            # function-local provider cannot trigger the annotation-eval
            # defect a function-local class name would (see
            # varco_fastapi/tests/test_di_binding_health.py).
            container.provide(_beanie_settings, returns=BeanieSettings)
            beanie_bootstrap(container)
        else:
            module = __import__(f"{package}.di", fromlist=["bootstrap"])
            bootstrap_fn = getattr(module, "bootstrap", None)
            if bootstrap_fn is not None:
                bootstrap_fn(container)
    except Exception as exc:  # noqa: BLE001 — re-raised, named, below
        raise DiCatalogueError(f"{package}: failed to bootstrap — {exc}") from exc

    return container


def build_catalogue(packages: list[str] | None = None) -> dict[str, Any]:
    """Build the merged catalogue dict across every package.

    Args:
        packages: Explicit package list, or `None` to use
            `discover_packages()`.

    Returns:
        A JSON-safe dict (`Catalogue.to_dict()` shape, merged).

    Raises:
        DiCatalogueError: A package failed to bootstrap/classify, or two
            packages registered the same `(seam, value)` pair.
    """
    from varco_core.di import catalogue as build_container_catalogue
    from varco_core.di.errors import CatalogueError
    from varco_core.di.introspect import Catalogue

    pkgs = packages if packages is not None else discover_packages()

    merged_seams: dict[str, dict[str, Any]] = {}
    merged_features: dict[str, dict[str, Any]] = {}
    other_defaults: list[dict[str, Any]] = []
    settings_count = 0
    all_packages: set[str] = set()
    backend_entries: list[dict[str, Any]] = []

    for package in pkgs:
        container = _bootstrap_package(package)
        try:
            result = build_container_catalogue(container)
        except CatalogueError as exc:
            raise DiCatalogueError(f"{package}: {exc}") from exc

        payload = result.to_dict()
        all_packages.update(payload["packages"])
        settings_count += payload["settings_defaults_count"]

        for seam_row in payload["seams"]:
            entry = merged_seams.setdefault(
                seam_row["seam"],
                {
                    "seam": seam_row["seam"],
                    "env": seam_row["env"],
                    "interface": seam_row["interface"],
                    "default": seam_row["default"],
                    "backends": [],
                },
            )
            if seam_row["default"] is not None:
                entry["default"] = seam_row["default"]
            for backend in seam_row["backends"]:
                if backend not in entry["backends"]:
                    entry["backends"].append(backend)

        for feature_row in payload["features"]:
            entry = merged_features.setdefault(feature_row["feature"], feature_row)
            if feature_row["default"] is not None:
                entry["default"] = feature_row["default"]
            if feature_row["enabled"] is not None:
                entry["enabled"] = feature_row["enabled"]

        for other in payload["other_defaults"]:
            if other not in other_defaults:
                other_defaults.append(other)

    for seam, entry in merged_seams.items():
        for backend in entry["backends"]:
            backend_entries.append({**backend, "seam": seam})
    Catalogue.merge_backend_entries(backend_entries)

    return {
        "schema_version": 1,
        "providify": _providify_version(),
        "packages": sorted(all_packages),
        "seams": [merged_seams[k] for k in sorted(merged_seams)],
        "features": [merged_features[k] for k in sorted(merged_features)],
        "other_defaults": sorted(other_defaults, key=lambda d: (d["package"], d["interface"])),
        "settings_defaults_count": settings_count,
    }


def _providify_version() -> str:
    import importlib.metadata as metadata

    return metadata.version("providify")


def _render_markdown(payload: dict[str, Any]) -> str:
    """Render the human-readable `di-catalogue.md` (§D-script)."""
    lines = [
        "# DI catalogue",
        "",
        "**Generated by `make di-catalogue` — never hand-edited.**",
        "",
        f"providify: `{payload['providify']}` · packages: {', '.join(payload['packages'])}",
        "",
        "## Seams",
        "",
    ]
    for seam in payload["seams"]:
        lines.append(f"### `{seam['seam']}` (`{seam['env']}`)")
        lines.append("")
        lines.append("| value | implementation | package | lifecycle |")
        lines.append("|---|---|---|---|")
        if seam["default"] is not None:
            d = seam["default"]
            lines.append(
                f"| `{d['value']}` (default) | `{d['implementation']}` | {d['package']} | in-process |"
            )
        for b in seam["backends"]:
            lines.append(f"| `{b['value']}` | `{b['implementation']}` | {b['package']} | see 050 |")
        lines.append("")

    lines.append("## Features")
    lines.append("")
    lines.append("| feature | env | default | enabled |")
    lines.append("|---|---|---|---|")
    for f in payload["features"]:
        default_impl = f["default"]["implementation"] if f["default"] else "—"
        enabled_impl = f["enabled"]["implementation"] if f["enabled"] else "—"
        lines.append(f"| {f['feature']} | `{f['env']}` | `{default_impl}` | `{enabled_impl}` |")
    lines.append("")

    lines.append("## Other defaults")
    lines.append("")
    lines.append("| interface | implementation | package | category |")
    lines.append("|---|---|---|---|")
    for d in payload["other_defaults"]:
        lines.append(
            f"| `{d['interface']}` | `{d['implementation']}` | {d['package']} | {d['category']} |"
        )
    lines.append("")

    lines.append("## Override cheat-sheet")
    lines.append("")
    lines.append("1. **Code** — `bind_instance(container, Interface, my_impl)` always wins.")
    lines.append("2. **Env dotted** — `VARCO_<SEAM>_BACKEND=pkg.mod:Name`.")
    lines.append("3. **Env named** — `VARCO_<SEAM>_BACKEND=<value>`.")
    lines.append("")

    return "\n".join(lines)


def _diff(committed: dict[str, Any], live: dict[str, Any]) -> list[str]:
    """Return a list of human-readable differences (empty = clean)."""
    diffs: list[str] = []
    if json.dumps(committed, sort_keys=True) != json.dumps(live, sort_keys=True):
        diffs.append(
            "di-catalogue.json differs from the live tree — regenerate with `make di-catalogue`."
        )
    return diffs


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="di_catalogue.py",
        description="Generate or verify the default-implementation catalogue.",
    )
    parser.add_argument(
        "--check", action="store_true", help="diff vs the committed snapshot; exit 1 on drift"
    )
    parser.add_argument("--snapshot", type=Path, default=DEFAULT_SNAPSHOT)
    parser.add_argument("--doc", type=Path, default=DEFAULT_DOC)
    parser.add_argument("--packages", nargs="+", metavar="PKG")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        live = build_catalogue(args.packages)
    except DiCatalogueError as exc:
        print(f"di_catalogue.py: {exc}", file=sys.stderr)
        return 1

    if args.check:
        if not args.snapshot.is_file():
            print(
                f"di_catalogue.py --check: no committed snapshot at {args.snapshot}",
                file=sys.stderr,
            )
            return 1
        committed = json.loads(args.snapshot.read_text(encoding="utf-8"))
        diffs = _diff(committed, live)
        if diffs:
            for d in diffs:
                print(d, file=sys.stderr)
            return 1
        print("di_catalogue.py --check: clean")
        return 0

    args.snapshot.parent.mkdir(parents=True, exist_ok=True)
    args.snapshot.write_text(json.dumps(live, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    args.doc.parent.mkdir(parents=True, exist_ok=True)
    args.doc.write_text(_render_markdown(live), encoding="utf-8")
    print(f"di_catalogue.py: wrote {args.snapshot} and {args.doc}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
