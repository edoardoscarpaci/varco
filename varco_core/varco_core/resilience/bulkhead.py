"""
varco_core.resilience.bulkhead
================================
Bulkhead pattern for async callables — limits maximum concurrency per
external dependency.

Named after the watertight compartments in a ship hull: if one section
floods, the bulkheads prevent it from sinking the whole vessel.  In
software, a bulkhead ensures that one slow dependency can not starve
all other async tasks by consuming unbounded concurrency.

This module provides:

* ``BulkheadConfig``    — immutable configuration (max_concurrent, max_wait).
* ``Bulkhead``          — stateful semaphore-based concurrency limiter.
* ``BulkheadFullError`` — raised when no slot is available within ``max_wait``.
* ``@bulkhead``         — decorator that wraps any async callable with a new
                           ``Bulkhead`` instance.

Usage::

    from varco_core.resilience.bulkhead import (
        BulkheadConfig,
        Bulkhead,
        BulkheadFullError,
        bulkhead,
    )

    # Shared bulkhead — one per external dependency (same rule as CircuitBreaker)
    db_bulkhead = Bulkhead(BulkheadConfig(max_concurrent=10, max_wait=0.5))

    result = await db_bulkhead.call(fetch_user, user_id)

    # Decorator form
    @bulkhead(BulkheadConfig(max_concurrent=5, max_wait=0.2))
    async def call_payment_api(payload: dict) -> Receipt:
        ...

DESIGN: asyncio.Semaphore over manual counter
    ✅ Semaphore is the canonical async concurrency primitive — no manual
       counter or lock required.
    ✅ ``asyncio.wait_for(semaphore.acquire(), timeout)`` gives precise
       max-wait semantics without busy-waiting.
    ❌ ``asyncio.Semaphore`` is not thread-safe — safe within a single event
       loop only.  Cross-thread use requires a threading.Semaphore instead.

DESIGN: fail-fast (max_wait=0.0) vs. bounded queue (max_wait > 0)
    ✅ max_wait=0.0 is the safest default — denies immediately if all slots
       are busy, preventing caller goroutine / task accumulation.
    ✅ max_wait > 0 allows short bursts to queue briefly, trading memory for
       lower error rate at the cost of slightly higher latency variance.
    ❌ A very large max_wait is equivalent to no bulkhead — callers pile up.

Thread safety:  ⚠️  Safe within a single asyncio event loop.
                     Do NOT share across threads with separate event loops.
Async safety:   ✅  asyncio.Semaphore provides coroutine-safe exclusion.
                    The semaphore is created lazily to avoid
                    "no running event loop" errors at construction time.

📚 Docs
- 🐍 https://docs.python.org/3/library/asyncio-sync.html#asyncio.Semaphore
  asyncio.Semaphore — counts available concurrent slots
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for
  asyncio.wait_for — async timeout with cancellation
"""

from __future__ import annotations

import asyncio
import functools
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

_logger = logging.getLogger(__name__)

_R = TypeVar("_R")


# ── BulkheadConfig ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class BulkheadConfig:
    """
    Immutable configuration for a ``Bulkhead``.

    Thread safety:  ✅ Frozen dataclass — stateless, safe to share globally.

    Attributes:
        max_concurrent: Maximum number of calls that may execute concurrently.
                        Once this many calls are in-flight, new calls either
                        wait (``max_wait > 0``) or fail fast (``max_wait == 0``).
                        Must be ≥ 1.
        max_wait:       Seconds a new call waits for a free slot before raising
                        ``BulkheadFullError``.  ``0.0`` (default) means fail
                        immediately if no slot is available — recommended for
                        most production use to avoid task pile-up.
                        Must be ≥ 0.

    Edge cases:
        - ``max_concurrent=1`` serialises all calls — equivalent to a mutex.
        - ``max_wait=0.0`` with a busy bulkhead raises ``BulkheadFullError``
          on every new call until a slot frees — callers should combine this
          with ``@retry`` for short back-off.
        - Very large ``max_wait`` values can cause async tasks to pile up when
          the dependency is slow — prefer small values (< 1 s in most cases).

    Example::

        # Up to 10 concurrent DB calls; fail fast if all busy
        BulkheadConfig(max_concurrent=10)

        # Up to 5 concurrent API calls; wait up to 500 ms for a slot
        BulkheadConfig(max_concurrent=5, max_wait=0.5)
    """

    max_concurrent: int
    """Max in-flight calls.  Must be ≥ 1."""

    max_wait: float = 0.0
    """Seconds to wait for a free slot.  0.0 = fail-fast."""

    def __post_init__(self) -> None:
        """
        Validate invariants at construction time.

        Raises:
            ValueError: ``max_concurrent`` < 1 or ``max_wait`` < 0.
        """
        if self.max_concurrent < 1:
            raise ValueError(
                f"BulkheadConfig.max_concurrent must be ≥ 1, "
                f"got {self.max_concurrent}."
            )
        if self.max_wait < 0:
            raise ValueError(
                f"BulkheadConfig.max_wait must be ≥ 0, " f"got {self.max_wait}."
            )

    def __repr__(self) -> str:
        return (
            f"BulkheadConfig("
            f"max_concurrent={self.max_concurrent}, "
            f"max_wait={self.max_wait})"
        )


# ── BulkheadFullError ─────────────────────────────────────────────────────────


class BulkheadFullError(Exception):
    """
    Raised when a ``Bulkhead`` has no available slot within ``max_wait``.

    Attributes:
        bulkhead_name:  Name of the ``Bulkhead`` that rejected the call.
        max_concurrent: The configured concurrency limit.
        max_wait:       The configured wait timeout (seconds).

    Example::

        try:
            result = await db_bulkhead.call(fetch_user, user_id)
        except BulkheadFullError as e:
            return {"error": f"Service '{e.bulkhead_name}' is saturated"}
    """

    def __init__(
        self,
        bulkhead_name: str,
        max_concurrent: int,
        max_wait: float,
    ) -> None:
        """
        Args:
            bulkhead_name:  Human-readable name for the saturated bulkhead.
            max_concurrent: Configured concurrency limit.
            max_wait:       Configured wait timeout in seconds.
        """
        self.bulkhead_name = bulkhead_name
        self.max_concurrent = max_concurrent
        self.max_wait = max_wait
        super().__init__(
            f"Bulkhead '{bulkhead_name}' is full "
            f"({max_concurrent} concurrent calls in-flight). "
            f"No slot available after {max_wait:.3f} s."
        )


# ── Bulkhead ──────────────────────────────────────────────────────────────────


class Bulkhead:
    """
    Stateful concurrency limiter for a single external dependency.

    Like ``CircuitBreaker``, a ``Bulkhead`` should be instantiated ONCE per
    external dependency and shared across all callers — a per-call instance
    provides no isolation.

    Usage::

        db_bulkhead = Bulkhead(BulkheadConfig(max_concurrent=10, max_wait=0.5))

        # Call through the bulkhead
        result = await db_bulkhead.call(fetch_user, user_id)

        # Or use .protect() as a decorator
        @db_bulkhead.protect
        async def fetch_user(user_id: int) -> User: ...

    DESIGN: shared instance over per-call instance
        ✅ Shared semaphore correctly limits concurrency across ALL callers.
        ❌ Requires dependency injection to pass the shared instance around.

    Thread safety:  ⚠️  Safe within a single event loop.  Do NOT share across
                         threads with separate event loops.
    Async safety:   ✅  asyncio.Semaphore serialises slot acquisition.
                         Created lazily — safe to construct before the loop.

    Attributes:
        name:   Human-readable name for logging and error messages.
        config: Immutable configuration.

    Edge cases:
        - Construction before the event loop starts is safe — semaphore is
          created lazily on first ``call()`` invocation.
        - ``available_slots`` reflects a point-in-time snapshot — it may change
          immediately after reading in a concurrent context.
    """

    __slots__ = ("name", "config", "_semaphore")

    def __init__(self, config: BulkheadConfig, *, name: str = "unnamed") -> None:
        """
        Args:
            config: Immutable bulkhead configuration.
            name:   Human-readable name used in log messages and error strings.
        """
        self.name: str = name
        self.config: BulkheadConfig = config

        # asyncio.Semaphore must be created inside a running event loop.
        # DESIGN: lazy semaphore creation
        #   ✅ Safe to construct Bulkhead before the event loop exists.
        #   ❌ Tiny overhead on the very first call — negligible in practice.
        self._semaphore: asyncio.Semaphore | None = None

    # ── Public properties ─────────────────────────────────────────────────────

    @property
    def available_slots(self) -> int:
        """
        Number of concurrency slots currently available.

        A snapshot value — may change concurrently.  Use for metrics/logging
        only, not for admission control (call ``call()`` for that).
        """
        if self._semaphore is None:
            # Semaphore not yet created — all slots are free.
            return self.config.max_concurrent
        return self._semaphore._value  # type: ignore[attr-defined]

    # ── Public methods ────────────────────────────────────────────────────────

    async def call(
        self,
        func: Callable[..., Awaitable[_R]],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> _R:
        """
        Call ``func`` through the bulkhead.

        Acquires a concurrency slot before calling and releases it afterward.
        If no slot is available within ``config.max_wait``, raises
        ``BulkheadFullError`` immediately.

        Args:
            func:     The async callable to execute within the bulkhead.
            *args:    Positional arguments forwarded to ``func``.
            **kwargs: Keyword arguments forwarded to ``func``.

        Returns:
            The return value of ``func(*args, **kwargs)``.

        Raises:
            BulkheadFullError: No concurrency slot available within
                               ``config.max_wait`` seconds.
            Exception:         Any exception raised by ``func`` — propagated
                               unchanged after releasing the slot.

        Async safety: ✅  Slot is always released in a ``finally`` block —
                          exceptions and cancellation cannot leak slots.

        Edge cases:
            - If ``func`` raises, the slot is released before re-raising.
            - Task cancellation (asyncio.CancelledError) also releases the slot
              because ``CancelledError`` propagates through the ``finally``.
        """
        semaphore = self._get_semaphore()

        if self.config.max_wait == 0.0:
            # Fail-fast path — try to acquire without blocking.
            # ``asyncio.Semaphore.acquire()`` is not truly non-blocking so we
            # use the internal locked() check: if _value is 0 there is no slot.
            # This is a best-effort check; a tiny TOCTOU window exists but is
            # acceptable for fail-fast semantics.
            if semaphore.locked():
                _logger.warning(
                    "Bulkhead '%s' full (0 slots available, fail-fast).", self.name
                )
                raise BulkheadFullError(
                    self.name,
                    self.config.max_concurrent,
                    self.config.max_wait,
                )
            await semaphore.acquire()
        else:
            # Bounded-wait path — wait up to max_wait seconds for a slot.
            try:
                await asyncio.wait_for(
                    semaphore.acquire(),
                    timeout=self.config.max_wait,
                )
            except asyncio.TimeoutError:
                _logger.warning(
                    "Bulkhead '%s' full after %.3f s wait.",
                    self.name,
                    self.config.max_wait,
                )
                raise BulkheadFullError(
                    self.name,
                    self.config.max_concurrent,
                    self.config.max_wait,
                ) from None

        # Slot acquired — always release in finally, even on cancellation.
        try:
            _logger.debug(
                "Bulkhead '%s' slot acquired (%d remaining).",
                self.name,
                self.available_slots,
            )
            result = await func(*args, **kwargs)
            return result
        finally:
            semaphore.release()
            _logger.debug(
                "Bulkhead '%s' slot released (%d remaining).",
                self.name,
                self.available_slots,
            )

    def protect(self, func: Callable) -> Callable:
        """
        Decorator that routes all calls to ``func`` through this bulkhead.

        Async-only — applying to a sync function raises ``TypeError``.

        Args:
            func: The async callable to protect.

        Returns:
            A wrapped version of ``func`` with identical signature.

        Raises:
            TypeError: ``func`` is not an async function.

        Example::

            @db_bulkhead.protect
            async def fetch_user(user_id: int) -> User: ...
        """
        if not asyncio.iscoroutinefunction(func):
            raise TypeError(
                f"Bulkhead.protect() only supports async functions; "
                f"'{func.__qualname__}' is synchronous."
            )

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self.call(func, *args, **kwargs)

        return wrapper

    # ── Private helpers ───────────────────────────────────────────────────────

    def _get_semaphore(self) -> asyncio.Semaphore:
        """Return the semaphore, creating it lazily on first access."""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.config.max_concurrent)
        return self._semaphore

    def __repr__(self) -> str:
        return (
            f"Bulkhead("
            f"name={self.name!r}, "
            f"max_concurrent={self.config.max_concurrent}, "
            f"available={self.available_slots})"
        )


# ── @bulkhead decorator ───────────────────────────────────────────────────────


def bulkhead(
    config: BulkheadConfig,
    *,
    name: str | None = None,
) -> Callable:
    """
    Decorator factory that wraps an async function with a new ``Bulkhead``.

    A NEW ``Bulkhead`` instance is created per decorated function.  If you
    need to share a bulkhead across multiple functions that access the same
    dependency, create a shared ``Bulkhead`` and use its ``.protect()`` method
    instead.

    DESIGN: new instance per decorated function vs. shared instance
        ✅ Simple one-liner usage — no manual instance management.
        ❌ Each decorated function has its own slot pool — concurrent calls to
           ``fetch_user`` and ``fetch_order`` each have their own ``max_concurrent``
           slots even if they hit the same DB.  For shared isolation use a
           shared ``Bulkhead`` instance.

    Args:
        config: ``BulkheadConfig`` applied to the new bulkhead instance.
        name:   Optional name for the bulkhead.  Defaults to the decorated
                function's ``__qualname__``.

    Returns:
        A decorator applicable to ``async def`` functions.

    Raises:
        TypeError: Applied to a non-async function.

    Example::

        @bulkhead(BulkheadConfig(max_concurrent=5, max_wait=0.2))
        async def call_payment_api(payload: dict) -> Receipt:
            ...
    """

    def decorator(func: Callable) -> Callable:
        bulkhead_name = name or func.__qualname__
        bh = Bulkhead(config, name=bulkhead_name)
        return bh.protect(func)

    return decorator


__all__ = [
    "BulkheadConfig",
    "BulkheadFullError",
    "Bulkhead",
    "bulkhead",
]
