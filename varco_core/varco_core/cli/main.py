"""
varco_core.cli.main
====================
``varco`` — the workspace's single console script entry point
(``[project.scripts] varco = "varco_core.cli.main:main"``).

Subcommands are discovered via the ``varco.commands`` entry-point group —
``varco_core`` itself only ships ``migrate`` and ``diagnose`` (built in, not plugins, since
it depends only on ``varco_core.migration``). Backend packages
(``varco_sa``, ``varco_beanie``) may contribute additional top-level
subcommands the same way, keeping ``varco_core`` free of sibling
dependencies (the same rule as the event-bus/cache backend split).

Thread safety:  N/A — a one-shot CLI process.
Async safety:   N/A — argument parsing is synchronous; individual
                   subcommands manage their own ``asyncio.run()``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from importlib.metadata import entry_points

from varco_core.cli import diagnose as _diagnose_module
from varco_core.cli import dlq as _dlq_module
from varco_core.cli import migrate as _migrate_module
from varco_core.cli import retention as _retention_module
from varco_core.cli import tenant as _tenant_module


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="varco", description="varco CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Built in — depends only on varco_core.migration / varco_core.tenancy.
    _migrate_module.register(subparsers)
    _tenant_module.register(subparsers)
    _dlq_module.register(subparsers)
    _retention_module.register(subparsers)
    _diagnose_module.register(subparsers)

    # Backend-contributed subcommands, discovered lazily so a package that
    # is not installed simply does not appear — never a hard ImportError at
    # `varco --help` time.
    for ep in entry_points(group="varco.commands"):
        try:
            register_fn = ep.load()
        except ImportError:
            continue
        try:
            register_fn(subparsers)
        except Exception:
            logging.getLogger(__name__).warning(
                "varco CLI: subcommand plugin %r failed to register",
                ep.name,
                exc_info=True,
            )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """
    Entry point for the ``varco`` console script.

    Args:
        argv: Argument list (excluding the program name). ``None`` reads
              ``sys.argv[1:]``.

    Returns:
        Process exit code: ``0`` ok, ``1`` migration/drift error,
        ``2`` usage error.
    """
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        # argparse calls sys.exit() directly on a usage error (or --help).
        # main()'s contract is "always return an int, never raise" — tests
        # call main(argv) directly and inspect the return value.
        return int(exc.code) if isinstance(exc.code, int) else 2
    run_fn = getattr(args, "_run", None)
    if run_fn is None:
        parser.print_help(sys.stderr)
        return 2
    return int(run_fn(args))


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())


__all__ = ["main"]
