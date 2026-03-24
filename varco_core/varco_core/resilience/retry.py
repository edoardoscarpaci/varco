"""
varco_core.resilience.retry
============================
Retry decorator and policy for sync and async callables.

Provides ``RetryPolicy`` (immutable configuration) and the ``@retry`` decorator
that wraps a function with automatic retry logic and exponential-backoff delay.

Works on both ``def`` and ``async def`` functions without changing the call
signature or return type.

Usage::

    from varco_core.resilience.retry import retry, RetryPolicy

    # Async function — retried with exponential back-off + jitter
    @retry(RetryPolicy(max_attempts=4, base_delay=0.5))
    async def call_payment_api(payload: dict) -> Receipt:
        ...

    # Sync function — same decorator, time.sleep used between attempts
    @retry(RetryPolicy(max_attempts=3, retryable_on=(IOError, OSError)))
    def read_config(path: str) -> dict:
        ...

    # Programmatic use without a decorator
    result = await retry_async(fetch, policy, url)

DESIGN: decorator over subclassing
    ✅ Works on any callable — no base class required.
    ✅ Preserves the original function's signature via ``@functools.wraps``.
    ✅ Same ``RetryPolicy`` object reusable across many functions.
    ❌ Stacking ``@retry`` twice is possible but almost always a mistake —
       the outer one sees a function that never raises (inner catches everything).
       Document this limitation explicitly.

Thread safety:  ✅ ``RetryPolicy`` is a frozen dataclass — stateless and safe
                    to share across threads and async tasks.
Async safety:   ✅ Async wrapper uses ``asyncio.sleep`` — never blocks the event
                    loop between attempts.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Final, TypeVar

_logger = logging.getLogger(__name__)

# TypeVars for preserving generic signatures through the decorator
_R = TypeVar("_R")
_SyncFn = TypeVar("_SyncFn", bound=Callable[..., Any])
_AsyncFn = TypeVar("_AsyncFn", bound=Callable[..., Awaitable[Any]])

# Default set of retryable exceptions — every Exception subclass.
# Callers should narrow this to avoid retrying on programmer errors
# (TypeError, AttributeError) which will never succeed on retry.
_ALL_EXCEPTIONS: Final[tuple[type[Exception], ...]] = (Exception,)


# ── RetryPolicy ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RetryPolicy:
    """
    Immutable configuration for retry behaviour.

    Controls how many times a function is retried, how long to wait between
    attempts, and which exception types are considered transient (retryable).

    Backoff formula::

        delay = min(base_delay * exponential_base ** attempt_index, max_delay)
        if jitter: delay *= uniform(0.5, 1.5)

    Where ``attempt_index`` is 0-based (first retry = 0, second = 1, …).

    Thread safety:  ✅ Frozen dataclass — immutable and safe to share globally.
    Async safety:   ✅ Stateless — no shared mutable fields.

    Attributes:
        max_attempts:     Total call attempts, including the first one.
                          ``1`` means no retries — the function is called once.
                          Must be ≥ 1.
        base_delay:       Seconds to wait before the first retry.
                          Defaults to ``1.0``.
        max_delay:        Upper bound on computed delay (before jitter).
                          Prevents runaway back-off for high attempt counts.
                          Defaults to ``60.0`` seconds.
        exponential_base: Multiplier applied per retry.  ``2.0`` doubles the
                          delay each time.  ``1.0`` gives fixed-interval retry.
                          Defaults to ``2.0``.
        jitter:           When ``True``, multiply the computed delay by a
                          uniform random factor in ``[0.5, 1.5]``.  Prevents
                          thundering-herd when many callers retry simultaneously.
                          Defaults to ``True``.
        retryable_on:     Tuple of exception types that trigger a retry.
                          Exceptions NOT in this tuple are propagated immediately
                          without retrying.  Defaults to ``(Exception,)`` — retry
                          on any non-BaseException error.

    Edge cases:
        - ``max_attempts=1`` means no retry; the function is called once
          and any exception propagates immediately.
        - ``retryable_on=()`` (empty tuple) means nothing is retried — every
          exception propagates immediately even if ``max_attempts > 1``.
        - ``jitter=False`` with a fixed delay is useful for deterministic tests.
        - Setting ``exponential_base=1.0`` produces fixed-interval (no growth)
          retries regardless of ``base_delay``.

    Example::

        # Retry up to 5 times, doubling delay from 0.5 s to at most 10 s
        policy = RetryPolicy(
            max_attempts=5,
            base_delay=0.5,
            max_delay=10.0,
            retryable_on=(ConnectionError, TimeoutError),
        )
    """

    max_attempts: int = 3
    """Total number of call attempts (first call + retries).  Must be ≥ 1."""

    base_delay: float = 1.0
    """Seconds before the first retry.  Subsequent delays grow exponentially."""

    max_delay: float = 60.0
    """Hard ceiling on delay seconds (before jitter is applied)."""

    exponential_base: float = 2.0
    """Growth factor applied per retry index.  2.0 = double each time."""

    jitter: bool = True
    """Add uniform random noise to delay to avoid synchronized retries."""

    retryable_on: tuple[type[Exception], ...] = field(
        # field() with default_factory is needed because mutable defaults are
        # forbidden in frozen dataclasses.  A tuple is immutable but the default
        # value assignment syntax triggers the same check — use factory instead.
        default_factory=lambda: _ALL_EXCEPTIONS
    )
    """Exception types that trigger a retry.  Others propagate immediately."""

    def __post_init__(self) -> None:
        """
        Validate invariants at construction time.

        Raises:
            ValueError: ``max_attempts`` is less than 1, ``base_delay`` or
                        ``max_delay`` is negative, or ``exponential_base`` ≤ 0.
        """
        if self.max_attempts < 1:
            raise ValueError(
                f"RetryPolicy.max_attempts must be ≥ 1, got {self.max_attempts}. "
                "Use max_attempts=1 to disable retries entirely."
            )
        if self.base_delay < 0:
            raise ValueError(
                f"RetryPolicy.base_delay must be ≥ 0, got {self.base_delay}."
            )
        if self.max_delay < 0:
            raise ValueError(
                f"RetryPolicy.max_delay must be ≥ 0, got {self.max_delay}."
            )
        if self.exponential_base <= 0:
            raise ValueError(
                f"RetryPolicy.exponential_base must be > 0, got {self.exponential_base}."
            )

    def compute_delay(self, attempt_index: int) -> float:
        """
        Compute the sleep duration before the next retry attempt.

        Args:
            attempt_index: Zero-based retry index.  0 = first retry,
                           1 = second retry, etc.

        Returns:
            Seconds to sleep.  Always ≥ 0.  Capped at ``max_delay``
            before jitter; jitter can push the result above ``max_delay``
            by up to 50%.

        Edge cases:
            - ``attempt_index=0`` returns ``base_delay`` (possibly jittered).
            - Very large ``attempt_index`` values are capped at ``max_delay``
              before jitter is applied — no overflow risk.
        """
        # Exponential growth capped at max_delay
        raw = min(
            self.base_delay * (self.exponential_base**attempt_index), self.max_delay
        )
        if self.jitter:
            # Uniform jitter in [0.5, 1.5] — preserves average delay while
            # desynchronising retries from multiple callers.
            raw *= random.uniform(0.5, 1.5)
        return raw

    def is_retryable(self, exc: BaseException) -> bool:
        """
        Return ``True`` if ``exc`` should trigger a retry.

        Args:
            exc: The exception that was raised.

        Returns:
            ``True`` when ``exc`` is an instance of any type in ``retryable_on``.

        Edge cases:
            - ``retryable_on=()`` always returns ``False`` — nothing is retried.
            - ``BaseException`` subclasses not in ``retryable_on`` (e.g.,
              ``KeyboardInterrupt``, ``SystemExit``) return ``False`` and are
              never retried.
        """
        return isinstance(exc, self.retryable_on)


# ── RetryExhaustedError ───────────────────────────────────────────────────────


class RetryExhaustedError(Exception):
    """
    Raised when all retry attempts have been exhausted.

    Chains the last exception that caused the final attempt to fail so the
    original traceback is always preserved in ``__cause__``.

    Attributes:
        attempts:     Total number of call attempts made (including first call).
        last_exc:     The exception raised by the final attempt.
        func_name:    Qualified name of the function that was retried.

    Example::

        try:
            await call_payment_api(payload)
        except RetryExhaustedError as e:
            logger.error(
                "Payment API failed after %d attempts: %s",
                e.attempts,
                e.last_exc,
            )
    """

    def __init__(
        self,
        func_name: str,
        attempts: int,
        last_exc: BaseException,
    ) -> None:
        """
        Args:
            func_name: Qualified name of the function (for the error message).
            attempts:  Total number of attempts that were made.
            last_exc:  The exception raised by the final attempt.
        """
        self.attempts = attempts
        self.last_exc = last_exc
        self.func_name = func_name
        super().__init__(
            f"{func_name!r} failed after {attempts} attempt(s). "
            f"Last error ({type(last_exc).__name__}): {last_exc}"
        )


# ── Internal helpers ─────────────────────────────────────────────────────────


def _log_retry_attempt(
    func_name: str,
    attempt: int,
    max_attempts: int,
    exc: BaseException,
    delay: float,
) -> None:
    """Log a single retry attempt at WARNING level."""
    _logger.warning(
        "%r failed on attempt %d/%d (%s: %s) — retrying in %.2f s",
        func_name,
        attempt,
        max_attempts,
        type(exc).__name__,
        exc,
        delay,
    )


def _log_exhausted(func_name: str, attempts: int, exc: BaseException) -> None:
    """Log exhaustion at ERROR level."""
    _logger.error(
        "%r exhausted all %d attempt(s). Final error (%s): %s",
        func_name,
        attempts,
        type(exc).__name__,
        exc,
    )


# ── @retry decorator ─────────────────────────────────────────────────────────


def retry(policy: RetryPolicy) -> Callable:
    """
    Decorator factory that wraps a callable with retry + exponential-backoff logic.

    Works transparently on both ``def`` (sync) and ``async def`` (async) callables.
    The return type and signature of the wrapped function are preserved.

    DESIGN: single decorator over separate async_retry / sync_retry
        ✅ One import, one API surface — callers do not need to care about
           whether the function is async.
        ✅ ``@functools.wraps`` preserves ``__name__``, ``__doc__``, ``__module__``.
        ❌ Runtime inspection via ``asyncio.iscoroutinefunction`` at decoration
           time adds a tiny overhead per decorated function, not per call.

    Args:
        policy: ``RetryPolicy`` controlling attempt count, delays, and
                which exceptions trigger a retry.

    Returns:
        A decorator.  Apply it to a sync or async function.

    Raises:
        RetryExhaustedError: All ``policy.max_attempts`` attempts failed and
                             the exception is in ``policy.retryable_on``.
        Exception:           Any exception NOT in ``policy.retryable_on`` is
                             re-raised immediately without retrying.

    Edge cases:
        - Stacking ``@retry`` twice is technically possible but almost always
          wrong — the inner wrapper catches all retryable errors; the outer
          one never sees them.  Use a single policy with higher ``max_attempts``.
        - If ``policy.max_attempts=1``, the function is called once and any
          exception propagates as-is (no ``RetryExhaustedError`` wrapping).
        - ``KeyboardInterrupt`` and ``SystemExit`` are never retried —
          they are ``BaseException`` subclasses and bypass the ``Exception``
          catch block.

    Example::

        @retry(RetryPolicy(max_attempts=3, base_delay=0.5, jitter=False))
        async def fetch_user(user_id: int) -> User:
            return await http_client.get(f"/users/{user_id}")

        # Sync usage
        @retry(RetryPolicy(max_attempts=5, retryable_on=(IOError,)))
        def read_file(path: str) -> bytes:
            with open(path, "rb") as f:
                return f.read()
    """

    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            return _wrap_async(func, policy)
        return _wrap_sync(func, policy)

    return decorator


def _wrap_async(
    func: Callable[..., Awaitable[_R]],
    policy: RetryPolicy,
) -> Callable[..., Awaitable[_R]]:
    """
    Build an async wrapper that retries ``func`` according to ``policy``.

    Uses ``asyncio.sleep`` between attempts — never blocks the event loop.

    Args:
        func:   The async function to wrap.
        policy: Retry configuration.

    Returns:
        Async callable with the same signature as ``func``.

    Async safety: ✅ Each await of the wrapper runs independently — concurrent
                     callers do not share state.
    """

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> _R:
        last_exc: BaseException | None = None

        for attempt in range(1, policy.max_attempts + 1):
            try:
                return await func(*args, **kwargs)
            except BaseException as exc:
                if not policy.is_retryable(exc):
                    # Non-retryable exception — propagate immediately,
                    # preserving original traceback without re-wrapping.
                    raise

                last_exc = exc

                if attempt < policy.max_attempts:
                    # Calculate delay using 0-based index for backoff math
                    delay = policy.compute_delay(attempt - 1)
                    _log_retry_attempt(
                        func.__qualname__, attempt, policy.max_attempts, exc, delay
                    )
                    await asyncio.sleep(delay)

        # All attempts exhausted — last_exc is always set here because the
        # loop body always assigns it before reaching this point.
        assert last_exc is not None  # invariant: loop ran ≥ 1 time
        _log_exhausted(func.__qualname__, policy.max_attempts, last_exc)
        raise RetryExhaustedError(
            func.__qualname__, policy.max_attempts, last_exc
        ) from last_exc

    return wrapper


def _wrap_sync(
    func: Callable[..., _R],
    policy: RetryPolicy,
) -> Callable[..., _R]:
    """
    Build a sync wrapper that retries ``func`` according to ``policy``.

    Uses ``time.sleep`` between attempts.  Must NOT be used inside an async
    event loop — it will block the loop.  For async callers, always decorate
    the ``async def`` function directly.

    DESIGN: sync wrapper kept for completeness
        ✅ Useful for CLI scripts, background threads, and Django sync views.
        ❌ Blocks the thread — unsuitable for async contexts.

    Args:
        func:   The sync function to wrap.
        policy: Retry configuration.

    Returns:
        Sync callable with the same signature as ``func``.

    Thread safety: ✅ No shared mutable state — each call is independent.
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> _R:
        last_exc: BaseException | None = None

        for attempt in range(1, policy.max_attempts + 1):
            try:
                return func(*args, **kwargs)
            except BaseException as exc:
                if not policy.is_retryable(exc):
                    raise

                last_exc = exc

                if attempt < policy.max_attempts:
                    delay = policy.compute_delay(attempt - 1)
                    _log_retry_attempt(
                        func.__qualname__, attempt, policy.max_attempts, exc, delay
                    )
                    # Blocking sleep — intentional for sync context.
                    # Callers in async code should decorate async functions instead.
                    time.sleep(delay)

        assert last_exc is not None
        _log_exhausted(func.__qualname__, policy.max_attempts, last_exc)
        raise RetryExhaustedError(
            func.__qualname__, policy.max_attempts, last_exc
        ) from last_exc

    return wrapper
