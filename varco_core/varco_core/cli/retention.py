"""
varco_core.cli.retention
==========================
The ``retention`` subcommand — chunked-sweep pruning for a DLQ or an audit
log, resolved via ``module:callable`` targets (Plan 009, Phase 2 / R3),
mirroring ``varco migrate``'s own resolution convention.

```
varco retention prune --type {dlq,audit} --before <ISO8601> [--limit N]
                      [--chunk 1000] [--dry-run] --target module:factory
```

``--target`` names an importable zero-arg factory returning the
``AbstractDeadLetterQueue`` / ``AuditRepository`` — the CLI cannot know the
app's DI container (same reasoning as ``varco migrate``'s ``-t``).
``--dry-run`` requires ``count()``/``list()`` support and prints the count
without deleting. Default behaviour is the chunked sweep: loop
``delete_where(..., limit=chunk)`` until it returns ``0``.

Exit codes: ``0`` ok, ``1`` the backend refuses (e.g. Kafka/NATS naming their
own retention mechanism), ``2`` usage error.

Thread safety:  N/A — a one-shot CLI process.
Async safety:   ✅ Resolves the target synchronously, then ``asyncio.run()``s
                   the chunked sweep.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import os
import sys
from datetime import datetime
from typing import Any


def register(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    """Register the ``retention`` subcommand parser and its verbs."""
    parser = subparsers.add_parser(
        "retention", help="Prune DLQ / audit-log entries (retention sweep)"
    )
    parser.set_defaults(_run=_run)

    verb_parsers = parser.add_subparsers(dest="verb", required=True)

    prune_p = verb_parsers.add_parser("prune")
    # Plan 039 (S20) / Step 24: --type is no longer required=True on its own
    # — --policy is a THIRD, mutually exclusive resolution mode
    # (§D-S20-cli). Validated in _run_prune (not argparse's own
    # mutually-exclusive-group machinery, because --before is required for
    # --type mode but NOT for --policy mode — the policy already carries its
    # own older_than). Existing --type/--target invocations are BYTE-
    # IDENTICAL: this argument's choices/default are unchanged, only
    # `required=True` was dropped.
    prune_p.add_argument("--type", choices=["dlq", "audit"], default=None)
    prune_p.add_argument("--policy", default=None, help="Run one registered RetentionPolicy once")
    prune_p.add_argument("--before", default=None, help="ISO8601 cutoff (required with --type)")
    prune_p.add_argument("--limit", type=int, default=None)
    prune_p.add_argument("--chunk", type=int, default=1000)
    prune_p.add_argument("--dry-run", action="store_true", dest="dry_run")
    prune_p.add_argument(
        "-t",
        "--target",
        default=os.environ.get("VARCO_RETENTION_TARGET"),
        required=os.environ.get("VARCO_RETENTION_TARGET") is None,
        help="module:callable target (env fallback: VARCO_RETENTION_TARGET)",
    )

    list_p = verb_parsers.add_parser("list", help="Print every policy in a RetentionRegistry")
    list_p.add_argument(
        "-t",
        "--target",
        default=os.environ.get("VARCO_RETENTION_TARGET"),
        required=os.environ.get("VARCO_RETENTION_TARGET") is None,
        help="module:callable target resolving to a RetentionRegistry "
        "(env fallback: VARCO_RETENTION_TARGET)",
    )


def _resolve(target: str) -> Any:
    """Resolve ``module:callable`` → an instance, calling zero-arg callables."""
    if ":" not in target:
        return None
    module_name, _, attr_name = target.partition(":")
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        return None
    obj: Any = module
    for part in attr_name.split("."):
        obj = getattr(obj, part, None)
        if obj is None:
            return None
    if callable(obj):
        obj = obj()
    return obj


def _validate_prune_args(args: argparse.Namespace) -> int | None:
    """
    Usage-level validation for ``prune`` shared by both resolution modes —
    returns an exit code (printing a message) on a usage error, or ``None``
    to proceed. §D-S20-cli: ``--type``/``--target`` behaviour is unchanged;
    ``--policy`` is a third, mutually exclusive mode.
    """
    if args.type is not None and args.policy is not None:
        print("--policy and --type are mutually exclusive.", file=sys.stderr)
        return 2
    if args.type is None and args.policy is None:
        print("One of --type or --policy is required.", file=sys.stderr)
        return 2
    if args.type is not None and args.before is None:
        print("--before is required with --type.", file=sys.stderr)
        return 2
    return None


async def _run_prune_policy(args: argparse.Namespace, registry: Any) -> int:
    """
    ``--policy <name>`` mode — resolve one policy from ``registry`` and run
    it once, through ``varco_core.retention.scheduler.execute_policy()`` —
    the identical code path a scheduled dispatch takes (§D-S20-cli).
    """
    from varco_core.retention.scheduler import execute_policy  # noqa: PLC0415

    policy = registry.get(args.policy)
    if policy is None:
        print(f"No retention policy named {args.policy!r} is registered.", file=sys.stderr)
        return 2

    result = await execute_policy(policy)
    if result.error is not None:
        print(result.error, file=sys.stderr)
        return 1
    if result.dry_run:
        print(
            f"Would prune (dry-run) policy {result.policy!r} "
            f"({result.kind}) — would_delete={result.would_delete}. Nothing deleted."
        )
    else:
        print(f"Pruned policy {result.policy!r} ({result.kind}) — deleted={result.deleted}.")
    return 0


async def _run_list(args: argparse.Namespace, registry: Any) -> int:
    """``list`` verb — print one line per policy in ``registry``."""
    names = list(registry)
    if not names:
        print("No retention policies registered.")
        return 0
    for name in names:
        policy = registry.get(name)
        print(
            f"{name}\tkind={policy.target.kind}\tcron={policy.cron_expr!r}\t"
            f"tz={policy.timezone!r}\tolder_than={policy.older_than}\t"
            f"dry_run={policy.dry_run}\ttenant_ids={policy.tenant_ids}"
        )
    return 0


async def _run_prune(args: argparse.Namespace, target: Any) -> int:
    cutoff = datetime.fromisoformat(args.before)

    if args.dry_run:
        try:
            count = await target.count()
        except (NotImplementedError, AttributeError):
            print("dry-run requires count() support on this target.", file=sys.stderr)
            return 1
        print(f"Would prune (dry-run) — current total count: {count}. Nothing deleted.")
        return 0

    total = 0
    try:
        while True:
            deleted = await target.delete_where(older_than=cutoff, limit=args.limit or args.chunk)
            total += deleted
            if deleted == 0 or args.limit is not None:
                break
    except NotImplementedError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    print(f"Pruned {total} entr{'y' if total == 1 else 'ies'}.")
    return 0


def _run(args: argparse.Namespace) -> int:
    """
    Resolve the ``-t module:factory`` target synchronously first, then
    dispatch the async body — see ``varco_core.cli.dlq._run``'s DESIGN block
    for why resolution must not happen inside our own coroutine.
    """
    try:
        asyncio.get_running_loop()
        loop_running = True
    except RuntimeError:
        loop_running = False

    if not loop_running:
        return _resolve_and_dispatch(args)

    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_resolve_and_dispatch, args)
        return future.result()


def _resolve_and_dispatch(args: argparse.Namespace) -> int:
    """Runs on a thread with no event loop yet — safe to resolve the target."""
    if args.verb == "list":
        registry = _resolve(args.target)
        if registry is None:
            print(f"Could not resolve retention target {args.target!r}.", file=sys.stderr)
            return 2
        return asyncio.run(_run_list(args, registry))

    # verb == "prune"
    usage_error = _validate_prune_args(args)
    if usage_error is not None:
        return usage_error

    if args.policy is not None:
        registry = _resolve(args.target)
        if registry is None:
            print(f"Could not resolve retention target {args.target!r}.", file=sys.stderr)
            return 2
        return asyncio.run(_run_prune_policy(args, registry))

    target = _resolve(args.target)
    if target is None:
        print(f"Could not resolve retention target {args.target!r}.", file=sys.stderr)
        return 2
    return asyncio.run(_run_prune(args, target))


__all__ = ["register"]
