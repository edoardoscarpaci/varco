"""
varco_core.cli.diagnose
==========================
The ``varco diagnose`` subcommand — print (or ``--check`` gate on) a
:class:`~varco_core.di.report.WiringReport` for an app's module list,
without ever calling ``start()`` (Plan 052 / §D-cli). Registered as a
**built-in** (beside ``migrate``/``tenant``/``dlq``/``retention``) — it
depends only on ``varco_core.di``, never a sibling package.

```
varco diagnose [--modules module:callable | --container module:callable | --app module:attr]
               [--path DIR]... [--static] [--json] [--verbose] [--check]
```

Exit codes: ``0`` ok (report printed; ``--check`` also clean), ``1``
``--check`` found an ERROR/WARNING or the driver itself failed
(``async_init`` raised, a backend-selection typo, ...), ``2`` usage error.

Thread safety:  N/A — a one-shot CLI process.
Async safety:   The driver (``varco_core.di.diagnose.diagnose``) is
                   ``async def``; this module is the one place that calls
                   ``asyncio.run()`` for it.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import sys
import traceback
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover — typing only
    from collections.abc import Sequence

    from providify import DIContainer

    from varco_core.di.module import VarcoModule

__all__ = ["register"]


class _UsageError(Exception):
    """Raised for any ``--modules``/``--container``/``--app`` target shape error — mapped to exit 2."""


def _run_async(coro: Any) -> Any:
    """
    Run *coro* to completion, tolerating an already-running event loop.

    ``varco diagnose`` is normally a one-shot CLI process (``asyncio.run()``
    is the whole story), but ``main()`` is also called directly from inside
    an ``async def`` test (pytest-asyncio's auto mode already has a loop
    running) — e.g. Plan 052's ``--app`` test, which awaits a real
    ``create_varco_app()`` lifespan elsewhere in the same test session.
    ``asyncio.run()`` raises ``RuntimeError`` in that case, so a running
    loop is detected and the coroutine is instead driven to completion on a
    dedicated thread with its own fresh loop.

    Args:
        coro: The coroutine to run.

    Returns:
        Whatever *coro* returns.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    import concurrent.futures  # noqa: PLC0415

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """
    Register the ``diagnose`` subcommand parser.

    Args:
        subparsers: The ``varco`` root parser's subparser action.
    """
    parser = subparsers.add_parser(
        "diagnose",
        help="Report what this app's DI wiring resolves to, and why",
    )
    parser.set_defaults(_run=_run)

    parser.add_argument(
        "--modules",
        default=None,
        metavar="module:callable",
        help="A zero-arg callable returning Sequence[VarcoModule] — a fresh DIContainer() is created",
    )
    parser.add_argument(
        "--container",
        default=None,
        metavar="module:callable",
        help="A zero-arg callable returning an already-bootstrapped DIContainer",
    )
    parser.add_argument(
        "--app",
        default=None,
        metavar="module:attr",
        help="A FastAPI object built by create_varco_app() — reads app.state.varco_container/varco_modules",
    )
    parser.add_argument(
        "--path",
        action="append",
        default=[],
        metavar="DIR",
        help="Directory to prepend to sys.path before importing (repeatable)",
    )
    parser.add_argument(
        "--static",
        action="store_true",
        help="Skip async_init() — Docker-free, at the cost of possibly-spurious MISSING_BINDINGs",
    )
    parser.add_argument("--json", action="store_true", help="Print report.to_dict() as JSON")
    parser.add_argument("--verbose", action="store_true", help="Print a traceback on failure")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit 1 if the report's worst() severity is ERROR or a WARNING finding",
    )


def _run(args: argparse.Namespace) -> int:
    """
    Execute ``varco diagnose``.

    Args:
        args: The parsed namespace produced by :func:`register`'s parser.

    Returns:
        ``0`` ok, ``1`` on a driver failure or (with ``--check``) a
        non-clean report, ``2`` on a usage error.
    """
    for path in args.path:
        sys.path.insert(0, str(Path(path).resolve()))

    if not args.modules and not args.container and not args.app:
        print(
            "Nothing to diagnose — pass one of --modules/--container/--app.",
            file=sys.stderr,
        )
        return 2

    try:
        container, modules, header = _resolve_source(args)
    except _UsageError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001 — a user factory raising is a diagnose failure, not a crash
        print(f"varco diagnose failed while building the target: {exc}", file=sys.stderr)
        if args.verbose:
            traceback.print_exc()
        return 1

    from varco_core.di.diagnose import ModuleAsyncInitError, diagnose  # noqa: PLC0415

    try:
        report = _run_async(diagnose(container, modules, async_init=not args.static, **header))
    except ModuleAsyncInitError as exc:
        # The message is already "<ModuleName>.async_init failed: <exc>".
        print(str(exc), file=sys.stderr)
        if args.verbose:
            traceback.print_exc()
        return 1
    except Exception as exc:  # noqa: BLE001 — a driver failure is a diagnose failure, not a crash
        print(f"varco diagnose failed: {exc}", file=sys.stderr)
        if args.verbose:
            traceback.print_exc()
        return 1

    if args.json:
        print(json.dumps(report.to_dict(), indent=2, sort_keys=True))
    else:
        print(report.render())

    # INFO findings (e.g. deprecation.fired) never fail --check.
    from providify.validation import Severity  # noqa: PLC0415

    if args.check and report.worst() in (Severity.ERROR, Severity.WARNING):
        return 1
    return 0


def _resolve_source(
    args: argparse.Namespace,
) -> tuple[DIContainer, Sequence[VarcoModule], dict[str, Any]]:
    """
    Resolve ``(container, modules)`` from ``--modules``/``--container``/``--app``
    (§D-driver's three input forms).

    Args:
        args: The parsed CLI namespace.

    Returns:
        A ``(DIContainer, Sequence[VarcoModule], header)`` triple.
        ``modules`` may be empty (a ``--container``-only invocation with no
        ``--modules``). ``header`` carries ``environment``/``enforce``/
        ``enforce_source`` read from ``app.state.varco_posture_header`` on
        the ``--app`` path (``varco_core`` cannot import ``varco_fastapi``'s
        settings); empty — i.e. ``"unknown"`` — on the other two paths.

    Raises:
        _UsageError: A target has no ``:`` separator, does not resolve, or
            returns the wrong shape (a container where a module list was
            expected, or vice versa).
    """
    from providify import DIContainer  # noqa: PLC0415

    if args.app:
        try:
            app = _import_attr(args.app)
        except (ValueError, ImportError, AttributeError) as exc:
            raise _UsageError(f"--app target {args.app!r}: {exc}") from exc
        # A zero-arg app factory (``create_app``) is accepted as well as an app
        # object — the example app only exposes a factory. ✅ no import-time
        # side effects needed; ❌ a callable ASGI app without ``.state`` is
        # indistinguishable, but such an object has no varco state anyway.
        if not hasattr(app, "state") and callable(app):
            app = app()
        state = getattr(app, "state", None)
        container = getattr(state, "varco_container", None) if state is not None else None
        if container is None:
            raise _UsageError(
                f"--app target {args.app!r} has no app.state.varco_container — "
                "was it built by create_varco_app()?"
            )
        modules = getattr(state, "varco_modules", None) or ()
        header = dict(getattr(state, "varco_posture_header", None) or {})
        return container, list(modules), header

    if args.container:
        try:
            target = _import_attr(args.container)
        except (ValueError, ImportError, AttributeError) as exc:
            raise _UsageError(f"--container target {args.container!r}: {exc}") from exc
        result = target() if callable(target) else target
        if not hasattr(result, "get_all_bindings"):
            raise _UsageError(
                f"--container target {args.container!r} did not return a DIContainer."
            )
        extra: Sequence[VarcoModule] = ()
        if args.modules:
            extra = _resolve_module_list(args.modules)
        return result, extra, {}

    modules = _resolve_module_list(args.modules)
    return DIContainer(), modules, {}


def _resolve_module_list(target_str: str) -> Sequence[VarcoModule]:
    """
    Resolve a ``--modules module:callable`` target into ``Sequence[VarcoModule]``.

    Args:
        target_str: ``"module:callable"``.

    Returns:
        The list the callable returned.

    Raises:
        _UsageError: No ``:`` separator, import/attribute failure, the
            callable returned a ``DIContainer`` (use ``--container``
            instead), or the result is not a list/tuple.
    """
    try:
        target = _import_attr(target_str)
    except (ValueError, ImportError, AttributeError) as exc:
        raise _UsageError(f"--modules target {target_str!r}: {exc}") from exc
    result = target() if callable(target) else target
    if hasattr(result, "get_all_bindings"):
        raise _UsageError(
            f"--modules target {target_str!r} returned a DIContainer — use --container for that."
        )
    if not isinstance(result, (list, tuple)):
        raise _UsageError(
            f"--modules target {target_str!r} did not return a list of VarcoModule instances."
        )
    return list(result)


def _import_attr(target: str) -> Any:
    """
    Resolve a ``module:dotted.attr`` string (same shape as
    ``cli/asyncapi.py``'s ``_import_attr``).

    Args:
        target: e.g. ``"example.app:modules"``.

    Returns:
        The named object.

    Raises:
        ValueError: If *target* has no ``:`` separator.
        ImportError: If the module cannot be imported.
        AttributeError: If any attribute in the dotted path is missing.
    """
    module_name, sep, attr_path = target.partition(":")
    if not sep:
        raise ValueError(f"target must be 'module:attribute' (got {target!r})")
    obj: Any = importlib.import_module(module_name)
    for part in attr_path.split("."):
        obj = getattr(obj, part)
    return obj
