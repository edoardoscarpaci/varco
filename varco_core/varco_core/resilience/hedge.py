"""
varco_core.resilience.hedge
=============================
Hedged-requests pattern for async callables — reduce tail latency by issuing
a speculative duplicate request after a configurable delay.

The hedged-requests pattern was popularised by Google's "The Tail at Scale"
paper (Dean & Barroso, 2013).  The idea is simple:

    1. Issue the first call.
    2. If it has not returned within ``delay`` seconds, issue a second
       (hedged) call in parallel.
    3. Accept whichever call returns first; cancel the other(s).

This drives p99 latency toward p50 at the cost of extra load (at most 2×
requests for slow calls — fast calls complete before the hedge fires).

⚠️  IMPORTANT — hedging is safe ONLY for idempotent operations:
    - Read-only calls (database reads, cache lookups, external GET requests).
    - Upsert / idempotent PUT operations.
    - NEVER hedge non-idempotent writes (INSERT, financial transactions,
      email sends) — both requests may execute concurrently.

This module provides:

* ``HedgeConfig``  — immutable configuration (delay, max_hedges).
* ``@hedge``       — decorator that applies hedging to any async callable.

Usage::

    from varco_core.resilience.hedge import HedgeConfig, hedge

    # Hedge after 100 ms if the first read hasn't returned
    @hedge(HedgeConfig(delay=0.1))
    async def read_from_replica(user_id: int) -> UserData:
        ...

    # Allow up to 2 hedges (3 total attempts in-flight at once)
    @hedge(HedgeConfig(delay=0.05, max_hedges=2))
    async def fetch_config(key: str) -> str:
        ...

DESIGN: asyncio.wait(FIRST_COMPLETED) over asyncio.gather
    ✅ ``asyncio.wait`` returns the moment ANY task completes — no need to
       wait for the rest.  ``asyncio.gather`` would block on all tasks.
    ✅ Returned ``pending`` set makes cancellation explicit and exhaustive.
    ❌ The ``pending`` set must be cancelled and awaited manually — more
       boilerplate than ``gather``, but correct resource management.

DESIGN: asyncio.sleep as the hedge trigger
    ✅ Pure async — does not block the event loop.
    ✅ Simple: wrap original call + delayed hedge as a pair of tasks in
       ``asyncio.wait``.
    ❌ Clock resolution: on some platforms ``asyncio.sleep`` has ~1 ms
       granularity — sub-millisecond hedge delays are unreliable.

Thread safety:  ❌  Not thread-safe.  Async-only by design.
Async safety:   ✅  Tasks cancelled in ``finally`` to prevent leaks.
                    Cancellation is awaited to ensure tasks actually stop.

📚 Docs
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.wait
  asyncio.wait — wait for the first completed task
- 🔍 https://research.google/pubs/pub40801/
  Dean & Barroso, "The Tail at Scale" — original paper on hedged requests
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

_logger = logging.getLogger(__name__)

_R = TypeVar("_R")


# ── HedgeConfig ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class HedgeConfig:
    """
    Immutable configuration for hedged requests.

    Thread safety:  ✅ Frozen dataclass — stateless, safe to share globally.

    Attributes:
        delay:       Seconds to wait for the first call before issuing a
                     hedged (duplicate) call.  Should be set near the p95
                     latency of the protected call so most fast calls complete
                     before the hedge fires.  Must be > 0.
        max_hedges:  Maximum number of extra (hedged) calls to issue.  The
                     total in-flight count is ``1 + max_hedges``.  Default 1.
                     Must be ≥ 1.

    Edge cases:
        - Very small ``delay`` values (< 0.001 s) risk clock resolution issues
          on some platforms — prefer ≥ 0.01 s in production.
        - ``max_hedges=0`` is rejected — use no decorator at all if you don't
          want hedging.
        - A ``delay`` larger than the typical response time means the hedge
          almost never fires — equivalent to no hedging.

    Example::

        # Hedge after 50 ms (typical for a sub-10 ms median, high p99)
        HedgeConfig(delay=0.05)

        # Two hedges after 100 ms gaps: [call_0, call_1@+100ms, call_2@+200ms]
        HedgeConfig(delay=0.1, max_hedges=2)
    """

    delay: float
    """Seconds before issuing a hedged call.  Must be > 0."""

    max_hedges: int = 1
    """Maximum number of hedged (extra) calls.  Must be ≥ 1."""

    def __post_init__(self) -> None:
        """
        Validate invariants at construction time.

        Raises:
            ValueError: ``delay`` ≤ 0 or ``max_hedges`` < 1.
        """
        if self.delay <= 0:
            raise ValueError(f"HedgeConfig.delay must be > 0, got {self.delay}.")
        if self.max_hedges < 1:
            raise ValueError(
                f"HedgeConfig.max_hedges must be ≥ 1, got {self.max_hedges}."
            )

    def __repr__(self) -> str:
        return f"HedgeConfig(delay={self.delay}, max_hedges={self.max_hedges})"


# ── _hedged_call — core implementation ───────────────────────────────────────


async def _hedged_call(
    config: HedgeConfig,
    func: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> Any:
    """
    Execute ``func`` with hedging: issue up to ``1 + max_hedges`` copies.

    Schedules the original call immediately.  After each ``delay`` interval,
    if no call has returned yet, issues another copy.  Returns the result of
    whichever call finishes first and cancels the remaining tasks.

    Args:
        config: Hedge configuration.
        func:   The async callable to hedge.
        args:   Positional arguments for ``func``.
        kwargs: Keyword arguments for ``func``.

    Returns:
        The first successful return value from any of the hedged calls.

    Raises:
        Exception: If all hedged calls raise, re-raises the exception from
                   the last completed (failed) call.

    Async safety: ✅  All pending tasks are cancelled and awaited in
                      ``finally`` to ensure no leaked background tasks.

    Edge cases:
        - If the first call returns before any hedge fires, no hedge is
          ever created — zero extra load for fast calls.
        - If all ``1 + max_hedges`` calls fail, the last exception wins.
          Callers should combine hedging with ``@retry`` for resilience.
        - Task cancellation (asyncio.CancelledError from the outer scope)
          cancels all in-flight hedged tasks via the ``finally`` block.
    """
    # ── Build the initial task set ────────────────────────────────────────────

    # pending_tasks tracks all outstanding hedge tasks so we can cancel them.
    pending_tasks: list[asyncio.Task[Any]] = []

    async def _attempt() -> Any:
        """Single async invocation of func — wraps to allow task creation."""
        return await func(*args, **kwargs)

    # Start the first call immediately.
    first_task: asyncio.Task[Any] = asyncio.create_task(_attempt())
    pending_tasks.append(first_task)

    hedges_issued = 0

    try:
        while True:
            # Wait for either the delay to elapse or any task to complete.
            done, _ = await asyncio.wait(
                pending_tasks,
                timeout=config.delay if hedges_issued < config.max_hedges else None,
                return_when=asyncio.FIRST_COMPLETED,
            )

            if done:
                # At least one task finished — return its result.
                completed_task = next(iter(done))
                return completed_task.result()  # raises if task raised

            # No task finished within the delay window — issue a hedge.
            hedges_issued += 1
            hedge_task: asyncio.Task[Any] = asyncio.create_task(_attempt())
            pending_tasks.append(hedge_task)
            _logger.debug(
                "Hedge %d/%d issued for %s (delay=%.3f s).",
                hedges_issued,
                config.max_hedges,
                func.__qualname__,
                config.delay,
            )

    finally:
        # Cancel ALL still-running tasks (winners have already returned,
        # so only losers remain in pending_tasks).
        # Awaiting the cancelled tasks ensures they stop cleanly and do not
        # leak coroutines or I/O handles.
        for task in pending_tasks:
            if not task.done():
                task.cancel()
        # Suppress CancelledError from tasks we just cancelled — they are
        # expected.  asyncio.gather(return_exceptions=True) absorbs them.
        await asyncio.gather(*pending_tasks, return_exceptions=True)


# ── @hedge decorator ──────────────────────────────────────────────────────────


def hedge(config: HedgeConfig) -> Callable:
    """
    Decorator factory that wraps an async callable with hedged-request logic.

    ⚠️  Only apply to IDEMPOTENT operations (reads, upserts).  Non-idempotent
    operations (writes, financial transactions, emails) may execute multiple
    times concurrently — the decorator has no way to detect or prevent this.

    DESIGN: decorator over an explicit ``HedgeManager`` class
        ✅ Simple one-liner usage — no instance management required.
        ✅ Stateless decorator — ``HedgeConfig`` holds all configuration.
        ❌ All calls to the decorated function are hedged; there is no way to
           opt out per-call.  For selective hedging, call ``_hedged_call``
           directly.

    Args:
        config: ``HedgeConfig`` with delay and max_hedges.

    Returns:
        A decorator applicable to ``async def`` functions.

    Raises:
        TypeError: Applied to a non-async function.

    Example::

        @hedge(HedgeConfig(delay=0.1))
        async def read_replica(user_id: int) -> UserData:
            ...  # safe — idempotent read

        # ❌ WRONG — never hedge non-idempotent writes
        @hedge(HedgeConfig(delay=0.1))
        async def send_email(to: str, body: str) -> None:
            ...  # may send the email twice!
    """

    def decorator(func: Callable) -> Callable:
        if not asyncio.iscoroutinefunction(func):
            raise TypeError(
                f"@hedge can only decorate async functions; "
                f"'{func.__qualname__}' is synchronous.  "
                f"Hedging a sync function would block the event loop."
            )

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await _hedged_call(config, func, args, kwargs)

        return wrapper

    return decorator


__all__ = [
    "HedgeConfig",
    "hedge",
]
