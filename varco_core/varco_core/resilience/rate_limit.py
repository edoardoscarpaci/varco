"""
varco_core.resilience.rate_limit
=================================
Rate-limiting ABC and in-memory sliding-window implementation.

Rate limiting caps the number of calls to a resource over a rolling time
window.  This module provides:

* ``RateLimitConfig``       — immutable configuration (rate, period).
* ``RateLimiter``           — ABC that backend implementations subclass.
* ``InMemoryRateLimiter``   — per-process sliding-window implementation using
                               ``collections.deque``.  Not suitable for
                               multi-process or multi-pod deployments — use
                               ``varco_redis.RedisRateLimiter`` for those.
* ``RateLimitExceededError``— raised by the ``@rate_limit`` decorator when
                               the limiter denies a call.
* ``@rate_limit``           — decorator that gates any async callable.

Usage::

    from varco_core.resilience.rate_limit import (
        RateLimitConfig,
        InMemoryRateLimiter,
        RateLimitExceededError,
        rate_limit,
    )

    limiter = InMemoryRateLimiter(RateLimitConfig(rate=5, period=1.0))

    @rate_limit(limiter=limiter)
    async def call_external_api(payload: dict) -> Response:
        ...

    # Per-caller key — each user gets their own 5-req/s budget
    @rate_limit(limiter=limiter, key_fn=lambda user_id, _: f"user:{user_id}")
    async def upload_file(user_id: str, data: bytes) -> None:
        ...

DESIGN: sliding window over fixed window
    ✅ No boundary burst — a fixed window allows 2× rate at the turn of a
       window (N calls at end of window N + N calls at start of window N+1).
       The sliding window distributes calls evenly over a rolling period.
    ✅ Simple implementation — only timestamps are stored, no counters to reset.
    ❌ Slightly more memory than a fixed window — up to ``rate`` timestamps per
       key rather than a single integer counter.

DESIGN: async-only interface
    Rate limiters are almost always paired with async external calls.
    Providing a sync variant would require ``threading.Lock`` and ``time.sleep``,
    adding complexity for a rare use-case.  Callers that truly need sync rate
    limiting should use ``asyncio.run()`` or a different tool.

Thread safety:  ⚠️  ``InMemoryRateLimiter`` is safe within a single event loop.
                     Do NOT share it across threads with separate event loops.
                     For multi-thread use, one limiter per thread is safe.
Async safety:   ✅  Protected by a per-key ``asyncio.Lock`` created lazily
                    (never at module level — locks must be created inside a
                    running event loop).

📚 Docs
- 🐍 https://docs.python.org/3/library/collections.html#collections.deque
  collections.deque with maxlen — O(1) append/popleft, auto-evicts oldest
- 🐍 https://docs.python.org/3/library/asyncio-sync.html#asyncio.Lock
  asyncio.Lock — coroutine-safe mutual exclusion
"""

from __future__ import annotations

import asyncio
import functools
import logging
import time
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

_logger = logging.getLogger(__name__)


# ── RateLimitConfig ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class RateLimitConfig:
    """
    Immutable configuration for a rate limiter.

    Thread safety:  ✅ Frozen dataclass — stateless, safe to share globally.

    Attributes:
        rate:    Maximum number of calls allowed within ``period`` seconds.
                 Must be ≥ 1.
        period:  Rolling window length in seconds.  Must be > 0.

    Edge cases:
        - ``rate=1, period=1.0`` allows exactly one call per second.
        - Very small ``period`` values (< 0.001) may interact with system clock
          resolution — in practice, use at least 0.01 seconds.
        - ``rate`` is the total across all callers sharing the same ``key`` —
          use distinct keys per user/tenant to give each its own budget.

    Example::

        # 100 calls per minute
        cfg = RateLimitConfig(rate=100, period=60.0)
    """

    rate: int
    """Maximum calls allowed within ``period`` seconds.  Must be ≥ 1."""

    period: float
    """Rolling window length in seconds.  Must be > 0."""

    def __post_init__(self) -> None:
        """
        Validate invariants at construction time.

        Raises:
            ValueError: ``rate`` < 1 or ``period`` ≤ 0.
        """
        if self.rate < 1:
            raise ValueError(f"RateLimitConfig.rate must be ≥ 1, got {self.rate}.")
        if self.period <= 0:
            raise ValueError(f"RateLimitConfig.period must be > 0, got {self.period}.")

    def __repr__(self) -> str:
        return f"RateLimitConfig(rate={self.rate}, period={self.period})"


# ── RateLimitExceededError ────────────────────────────────────────────────────


class RateLimitExceededError(Exception):
    """
    Raised by ``@rate_limit`` when the limiter denies a call.

    Attributes:
        key:         The rate-limit key that exceeded its budget.
        retry_after: Seconds until the oldest call in the window expires and
                     the caller can try again.  May be 0.0 if the window
                     cleared by the time the error was constructed.

    Example::

        try:
            await upload_file(user_id, data)
        except RateLimitExceededError as e:
            return {"error": "too many requests", "retry_after": e.retry_after}
    """

    def __init__(self, key: str, retry_after: float) -> None:
        """
        Args:
            key:         Rate-limit key that hit its budget.
            retry_after: Seconds to wait before retrying.
        """
        self.key = key
        self.retry_after = max(0.0, retry_after)
        super().__init__(
            f"Rate limit exceeded for key '{key}'. "
            f"Retry after {self.retry_after:.3f} s."
        )


# ── RateLimiter ABC ───────────────────────────────────────────────────────────


class RateLimiter(ABC):
    """
    Abstract base class for rate-limiter backends.

    Subclasses implement the per-key call budget.  A ``key`` is an arbitrary
    string that namespaces the budget — use ``"default"`` for a single global
    budget, or a user/tenant ID for per-entity limits.

    Async safety:   ✅  All methods are ``async def`` — implementations MUST
                         use async primitives (asyncio.Lock / Redis pipeline)
                         and never block the event loop.

    Edge cases:
        - Implementations MUST be safe to call concurrently for the same key.
        - ``reset()`` is primarily for tests — production code rarely needs it.

    Example (custom backend)::

        class MyRateLimiter(RateLimiter):
            async def acquire(self, key: str = "default") -> bool:
                ...  # return True if allowed, False if denied

            async def reset(self, key: str = "default") -> None:
                ...  # clear the window for this key
    """

    @abstractmethod
    async def acquire(self, key: str = "default") -> bool:
        """
        Try to acquire a call slot for ``key``.

        If the budget for ``key`` is not exhausted, record the call and
        return ``True``.  Otherwise return ``False`` without recording.

        Args:
            key: Namespacing key for the rate budget.
                 Defaults to ``"default"`` for a single global budget.

        Returns:
            ``True``  — call is within budget; proceed.
            ``False`` — call exceeds the rate limit; caller should back off.

        Async safety: ✅  Implementations MUST use async locking to prevent
                          TOCTOU races when multiple coroutines share a key.
        """

    @abstractmethod
    async def reset(self, key: str = "default") -> None:
        """
        Clear the call history for ``key``, resetting its budget to full.

        Intended for test teardowns and administrative resets.

        Args:
            key: The key whose window to clear.
        """

    @abstractmethod
    async def retry_after(self, key: str = "default") -> float:
        """
        Return how many seconds until the oldest call in the window expires.

        This is the minimum wait time before the next ``acquire()`` for
        ``key`` would succeed.  Returns ``0.0`` if the budget is not
        currently exhausted.

        Args:
            key: Rate-limit key to inspect.

        Returns:
            Seconds to wait.  Always ≥ 0.
        """


# ── InMemoryRateLimiter ───────────────────────────────────────────────────────


class InMemoryRateLimiter(RateLimiter):
    """
    Per-process sliding-window rate limiter backed by ``collections.deque``.

    Each ``key`` gets its own ``deque(maxlen=rate)`` of call timestamps.
    ``acquire()`` inspects whether the oldest timestamp in the full deque falls
    inside the rolling window — if yes, the budget is exhausted.

    DESIGN: deque(maxlen=rate) as the sliding window
        ✅ O(1) append and popleft — no periodic cleanup needed.
        ✅ ``maxlen`` auto-evicts the oldest entry once full, so storage is
           bounded at ``rate`` timestamps per key regardless of call volume.
        ✅ No background task required — eviction is implicit on append.
        ❌ Not distributed — each process has its own counter.  For multi-pod
           deployments use ``varco_redis.RedisRateLimiter``.

    DESIGN: per-key asyncio.Lock
        ✅ Prevents TOCTOU races between concurrent coroutines on the same key.
        ✅ Created lazily — safe to instantiate before the event loop starts.
        ❌ One lock per key; with a very large number of distinct keys the lock
           dict grows unboundedly.  Callers should use bounded key spaces
           (e.g. user IDs from a fixed pool) rather than arbitrary strings.

    Thread safety:  ⚠️  Safe within a single asyncio event loop.
                         Do NOT share across threads with different event loops.
    Async safety:   ✅  Per-key asyncio.Lock prevents concurrent window reads
                         from racing and double-counting.

    Attributes:
        config: Immutable configuration (rate + period).

    Edge cases:
        - An empty deque (no prior calls) always grants the call.
        - If all ``rate`` prior calls are outside the window, the deque is
          effectively full of stale entries — the new call replaces the oldest
          (auto-eviction) and is granted.
        - Concurrent ``acquire()`` calls for the same key are serialised by the
          per-key lock — no double-counting.
        - ``reset()`` removes the key's deque and lock so they are re-created
          fresh on the next ``acquire()``.
    """

    def __init__(self, config: RateLimitConfig) -> None:
        """
        Args:
            config: Immutable rate-limit configuration (rate + period).
        """
        self.config = config

        # _windows: per-key deque of monotonic call timestamps.
        # deque maxlen=rate ensures the deque never exceeds rate entries —
        # appending beyond capacity silently drops the oldest entry.
        self._windows: dict[str, deque[float]] = {}

        # _locks: per-key asyncio.Lock created lazily inside the event loop.
        # Using a single global lock would serialise ALL keys — wasteful.
        # Per-key locks only serialise concurrent callers on the same key.
        self._locks: dict[str, asyncio.Lock] = {}

    def _get_lock(self, key: str) -> asyncio.Lock:
        """Return the lock for ``key``, creating it lazily if absent."""
        if key not in self._locks:
            # Lock created here — guaranteed inside a running event loop.
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    def _get_window(self, key: str) -> deque[float]:
        """Return the timestamp deque for ``key``, creating it if absent."""
        if key not in self._windows:
            # maxlen=rate bounds storage and makes the eviction logic trivial:
            # when the deque is full AND the oldest entry is inside the window,
            # the budget is exhausted.
            self._windows[key] = deque(maxlen=self.config.rate)
        return self._windows[key]

    async def acquire(self, key: str = "default") -> bool:
        """
        Try to acquire a slot for ``key`` within the sliding window.

        Records the current timestamp and returns ``True`` if the window is
        not yet full (or if old entries have expired).  Returns ``False``
        without recording if the budget is exhausted.

        Args:
            key: Rate-limit namespace.  Default ``"default"``.

        Returns:
            ``True`` if the call is allowed; ``False`` if rate-limited.

        Async safety: ✅  Serialised by a per-key asyncio.Lock.

        Edge cases:
            - First call for a new key is always allowed (empty deque).
            - A call immediately after ``reset()`` is always allowed.
        """
        async with self._get_lock(key):
            now = time.monotonic()
            window = self._get_window(key)

            if len(window) < self.config.rate:
                # Window not yet full — call is within budget.
                window.append(now)
                _logger.debug(
                    "RateLimiter key=%r allowed (%d/%d)",
                    key,
                    len(window),
                    self.config.rate,
                )
                return True

            # Window is full.  Check if the oldest call has expired.
            # If the oldest timestamp is outside the rolling period, the slot
            # it occupied is now free — overwrite it with this new call.
            oldest = window[0]
            if now - oldest >= self.config.period:
                # The oldest call has "scrolled out" of the window — allow.
                window.append(now)  # auto-evicts oldest via maxlen
                _logger.debug("RateLimiter key=%r allowed (oldest expired)", key)
                return True

            # All rate timestamps are within the rolling window — deny.
            _logger.debug(
                "RateLimiter key=%r denied (rate=%d, period=%.3f)",
                key,
                self.config.rate,
                self.config.period,
            )
            return False

    async def reset(self, key: str = "default") -> None:
        """
        Clear the call history for ``key``, resetting its budget to full.

        Removes the key's deque and lock so the next ``acquire()`` starts
        with a clean slate.  Primarily intended for test teardowns.

        Args:
            key: Rate-limit key to reset.

        Async safety: ✅  Safe to call concurrently — lock protects removal.
        """
        async with self._get_lock(key):
            self._windows.pop(key, None)
        # Remove the lock AFTER releasing it so no waiter finds a stale lock.
        self._locks.pop(key, None)
        _logger.debug("RateLimiter key=%r reset.", key)

    async def retry_after(self, key: str = "default") -> float:
        """
        Return seconds until the oldest call in the full window expires.

        If the budget is not currently exhausted (window has free slots or
        the oldest entry has already expired), returns ``0.0``.

        Args:
            key: Rate-limit key to inspect.

        Returns:
            Seconds to wait before the next ``acquire()`` would succeed.
            Always ≥ 0.

        Async safety: ✅  Read serialised by per-key lock.
        """
        async with self._get_lock(key):
            window = self._windows.get(key)
            if window is None or len(window) < self.config.rate:
                return 0.0

            now = time.monotonic()
            oldest = window[0]
            remaining = self.config.period - (now - oldest)
            return max(0.0, remaining)

    def __repr__(self) -> str:
        return (
            f"InMemoryRateLimiter("
            f"rate={self.config.rate}, "
            f"period={self.config.period}, "
            f"keys={list(self._windows.keys())})"
        )


# ── @rate_limit decorator ─────────────────────────────────────────────────────


def rate_limit(
    limiter: RateLimiter,
    *,
    key_fn: Callable[..., str] | None = None,
) -> Callable:
    """
    Decorator that gates calls to an async function through a ``RateLimiter``.

    If the limiter denies the call, ``RateLimitExceededError`` is raised with
    the computed key and the estimated wait time.

    DESIGN: key_fn callable instead of a static key string
        ✅ Allows per-request keys (e.g. user ID, IP address, tenant).
        ✅ Called with the same ``*args, **kwargs`` as the decorated function —
           no separate context object required.
        ❌ Caller must ensure the key function is deterministic and cheap.
           A slow key function adds latency to every call.

    Args:
        limiter: A ``RateLimiter`` instance that holds the call budget.
        key_fn:  Optional callable ``(*args, **kwargs) -> str`` that derives
                 the rate-limit key from the function's arguments.
                 If ``None``, the key ``"default"`` is used — all callers
                 share a single budget.

    Returns:
        Decorator applicable to ``async def`` functions.

    Raises:
        TypeError: If applied to a non-async function (sync rate limiting is
                   out of scope — see module docstring for rationale).

    Example::

        limiter = InMemoryRateLimiter(RateLimitConfig(rate=10, period=1.0))

        # All callers share one 10-req/s budget
        @rate_limit(limiter=limiter)
        async def search(query: str) -> list[Result]: ...

        # Per-user 10-req/s budget — key derived from first positional arg
        @rate_limit(limiter=limiter, key_fn=lambda user_id, *_, **__: user_id)
        async def upload(user_id: str, data: bytes) -> None: ...
    """

    def decorator(func: Callable) -> Callable:
        # Reject sync functions early — async-only by design.
        if not asyncio.iscoroutinefunction(func):
            raise TypeError(
                f"@rate_limit can only decorate async functions; "
                f"'{func.__qualname__}' is synchronous.  "
                f"See varco_core.resilience.rate_limit module docstring for rationale."
            )

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Derive the key from the call arguments, or use the default.
            key = key_fn(*args, **kwargs) if key_fn is not None else "default"

            allowed = await limiter.acquire(key)
            if not allowed:
                wait = await limiter.retry_after(key)
                raise RateLimitExceededError(key, wait)

            return await func(*args, **kwargs)

        return wrapper

    return decorator


__all__ = [
    "RateLimitConfig",
    "RateLimitExceededError",
    "RateLimiter",
    "InMemoryRateLimiter",
    "rate_limit",
]
