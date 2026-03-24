"""
varco_core.resilience.timeout
==============================
Timeout decorator for async callables.

Wraps an ``async def`` function so that if it does not complete within the
configured number of seconds, it is cancelled and ``CallTimeoutError`` is raised.

Usage::

    from varco_core.resilience.timeout import timeout, CallTimeoutError

    @timeout(5.0)
    async def fetch_user(user_id: int) -> User:
        return await http_client.get(f"/users/{user_id}")

    try:
        user = await fetch_user(42)
    except CallTimeoutError as e:
        logger.warning("fetch_user timed out after %s s", e.limit)

DESIGN: async-only limitation
    The decorator only supports ``async def`` functions.  Sync timeouts require
    OS-level signals (Unix-only, not safe in multi-threaded programs) or blocking
    a thread via ``concurrent.futures.Future.result(timeout=)``.  Both approaches
    are out of scope for this module.

    If you need to time out a sync function, run it in an executor::

        try:
            result = await asyncio.wait_for(
                asyncio.get_event_loop().run_in_executor(None, sync_fn, *args),
                timeout=5.0,
            )
        except asyncio.TimeoutError:
            ...

Thread safety:  ✅ The wrapper is stateless — no shared mutable state.
Async safety:   ✅ Uses ``asyncio.wait_for`` which cancels the inner coroutine
                    correctly and does not block the event loop.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

_R = TypeVar("_R")


# ── CallTimeoutError ──────────────────────────────────────────────────────────


class CallTimeoutError(Exception):
    """
    Raised when a decorated async function does not complete within the limit.

    Chains ``asyncio.TimeoutError`` as ``__cause__`` so the full traceback is
    preserved for debugging.

    Attributes:
        func_name: Qualified name of the function that timed out.
        limit:     Configured timeout in seconds.

    Example::

        try:
            await fetch_user(42)
        except CallTimeoutError as e:
            logger.warning("%r timed out after %.1f s", e.func_name, e.limit)
    """

    def __init__(self, func_name: str, limit: float) -> None:
        """
        Args:
            func_name: ``__qualname__`` of the timed-out function.
            limit:     The timeout threshold in seconds.
        """
        self.func_name = func_name
        self.limit = limit
        super().__init__(f"{func_name!r} did not complete within {limit:.1f} s.")


# ── @timeout decorator ────────────────────────────────────────────────────────


def timeout(seconds: float) -> Callable:
    """
    Decorator factory that enforces a maximum execution time on an async function.

    If the decorated coroutine does not finish within ``seconds``, it is
    cancelled via the standard ``asyncio.wait_for`` mechanism and
    ``CallTimeoutError`` is raised to the caller.

    DESIGN: ``asyncio.wait_for`` over manual ``asyncio.Task`` + ``cancel()``
        ✅ ``wait_for`` handles task cancellation and cleanup correctly,
           including shield interactions (``asyncio.shield``).
        ✅ Less boilerplate — no manual task management.
        ❌ ``wait_for`` cancels the *entire* coroutine tree, including any
           inner tasks it has spawned.  If the inner coroutine catches
           ``asyncio.CancelledError`` and suppresses it, the timeout will not
           work.  Document this known limitation.

    Args:
        seconds: Maximum allowed execution time in seconds.
                 Must be > 0.

    Returns:
        A decorator that wraps ``async def`` functions only.

    Raises:
        TypeError: The decorated function is not ``async def``.
        ValueError: ``seconds`` ≤ 0.

    Example::

        @timeout(10.0)
        async def send_email(to: str, body: str) -> None:
            await smtp_client.send(to, body)

    Known limitations:
        - Only works on ``async def`` — raises ``TypeError`` on sync functions.
        - If the inner coroutine suppresses ``asyncio.CancelledError``, the
          timeout signal is silently ignored and the coroutine continues running.
        - Does not compose with ``asyncio.shield`` as expected — shielded tasks
          keep running after the timeout, consuming resources silently.

    Edge cases:
        - ``seconds=0`` raises ``ValueError`` — a zero timeout would immediately
          cancel every call.
        - Very small values (< 0.001 s) are accepted but are unlikely to be
          meaningful in practice.
    """
    if seconds <= 0:
        raise ValueError(
            f"@timeout requires seconds > 0, got {seconds!r}. "
            "Use a positive float (e.g. timeout(5.0))."
        )

    def decorator(func: Callable[..., Awaitable[_R]]) -> Callable[..., Awaitable[_R]]:
        if not asyncio.iscoroutinefunction(func):
            raise TypeError(
                f"@timeout can only decorate async functions. "
                f"{func.__qualname__!r} is a sync function. "
                "Run sync functions in an executor if you need timeouts."
            )

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> _R:
            try:
                return await asyncio.wait_for(func(*args, **kwargs), timeout=seconds)
            except asyncio.TimeoutError as exc:
                raise CallTimeoutError(func.__qualname__, seconds) from exc

        return wrapper

    return decorator
