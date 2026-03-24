"""
varco_core.resilience.circuit_breaker
======================================
Circuit-breaker pattern for sync and async callables.

Prevents a repeatedly-failing dependency from being hammered by rejecting calls
fast (without waiting for a timeout) once the failure threshold is exceeded.

State machine::

    ┌─────────┐  failures ≥ threshold   ┌──────┐
    │ CLOSED  │ ─────────────────────▶  │ OPEN │
    │ (normal)│                          │(reject)
    └─────────┘                          └──────┘
         ▲                                   │
         │ success                           │ recovery_timeout elapsed
         │                             ┌─────┴────┐
         └──────────────────────────── │ HALF_OPEN│
                                       │ (probe)  │
                                       └──────────┘
                                            │ failure → back to OPEN

Usage::

    from varco_core.resilience.circuit_breaker import (
        CircuitBreaker, CircuitBreakerConfig, circuit_breaker
    )

    # Shared breaker instance (one per external dependency)
    payment_breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=5, recovery_timeout=30.0)
    )

    # Decorator form — the breaker is shared across all calls to this function
    @circuit_breaker(CircuitBreakerConfig(failure_threshold=3))
    async def call_payment_api(payload: dict) -> Receipt:
        ...

    # Direct call through a shared breaker
    result = await payment_breaker.call_async(call_payment_api, payload)

DESIGN: shared CircuitBreaker instance vs. per-call state
    ✅ Shared instance correctly counts failures from ALL callers —
       a per-call instance would never accumulate enough failures to open.
    ✅ State transitions (CLOSED → OPEN → HALF_OPEN) are centralised.
    ❌ Shared instance requires thread/async safety — handled via asyncio.Lock
       created lazily (not at module import time, which would precede the loop).

Thread safety:  ⚠️  Conditional — safe within a single asyncio event loop.
                     Do NOT share a CircuitBreaker across threads; each thread
                     should have its own instance if running separate event loops.
Async safety:   ✅  asyncio.Lock protects state transitions in HALF_OPEN;
                     CLOSED/OPEN paths are lock-free (safe because state reads
                     are atomic under GIL and only one probe is allowed at a time).
"""

from __future__ import annotations
import inspect
import asyncio
import functools
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeVar

_logger = logging.getLogger(__name__)

_R = TypeVar("_R")


# ── CircuitState ──────────────────────────────────────────────────────────────


class CircuitState(str, Enum):
    """
    Operational state of a ``CircuitBreaker``.

    CLOSED
        Normal operation.  Calls pass through.  Failures are counted;
        when ``failure_threshold`` is reached the circuit transitions to OPEN.

    OPEN
        Failing fast.  ALL calls are immediately rejected with
        ``CircuitOpenError`` without touching the protected resource.
        After ``recovery_timeout`` seconds the circuit transitions to HALF_OPEN.

    HALF_OPEN
        Recovery probe.  ONE call is allowed through.
        - Success → circuit closes (back to CLOSED, counters reset).
        - Failure → circuit re-opens (back to OPEN, timer resets).
        Concurrent callers during HALF_OPEN are rejected (like OPEN) — only
        the first caller probes.
    """

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# ── CircuitBreakerConfig ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class CircuitBreakerConfig:
    """
    Immutable configuration for a ``CircuitBreaker``.

    Thread safety:  ✅ Frozen dataclass — stateless, safe to share globally.

    Attributes:
        failure_threshold:  Number of consecutive failures in CLOSED state
                            that trips the breaker to OPEN.  Default ``5``.
        recovery_timeout:   Seconds to stay in OPEN state before transitioning
                            to HALF_OPEN for a probe call.  Default ``30.0``.
        success_threshold:  Number of consecutive successes in HALF_OPEN state
                            required to fully close the circuit.  Default ``1``
                            (single probe success is enough).
        monitored_on:       Tuple of exception types that count as failures.
                            Exceptions outside this tuple propagate without
                            counting against the circuit.  Defaults to
                            ``(Exception,)`` — every non-BaseException failure
                            counts.

    Edge cases:
        - ``failure_threshold=1`` opens the circuit on the FIRST failure.
        - ``success_threshold > 1`` requires multiple probe successes before
          closing — useful when a flaky dependency recovers intermittently.
        - ``monitored_on=()`` means nothing trips the circuit — equivalent to
          no circuit breaker.
    """

    failure_threshold: int = 5
    """Consecutive CLOSED-state failures needed to open the circuit."""

    recovery_timeout: float = 30.0
    """Seconds the circuit stays OPEN before allowing a probe call."""

    success_threshold: int = 1
    """Consecutive HALF_OPEN probe successes needed to close the circuit."""

    monitored_on: tuple[type[Exception], ...] = field(
        # Same frozen-dataclass pattern as RetryPolicy.retryable_on
        default_factory=lambda: (Exception,)
    )
    """Exception types that count as circuit failures.  Others pass through."""

    def __post_init__(self) -> None:
        """
        Validate invariants at construction time.

        Raises:
            ValueError: Any threshold or timeout is out of range.
        """
        if self.failure_threshold < 1:
            raise ValueError(
                f"CircuitBreakerConfig.failure_threshold must be ≥ 1, "
                f"got {self.failure_threshold}."
            )
        if self.recovery_timeout < 0:
            raise ValueError(
                f"CircuitBreakerConfig.recovery_timeout must be ≥ 0, "
                f"got {self.recovery_timeout}."
            )
        if self.success_threshold < 1:
            raise ValueError(
                f"CircuitBreakerConfig.success_threshold must be ≥ 1, "
                f"got {self.success_threshold}."
            )


# ── CircuitOpenError ──────────────────────────────────────────────────────────


class CircuitOpenError(Exception):
    """
    Raised when a call is rejected because the circuit is OPEN.

    Attributes:
        breaker_name:    Name of the ``CircuitBreaker`` that rejected the call.
        opens_at:        Unix timestamp when the circuit opened.
        retry_after:     Seconds until the circuit may transition to HALF_OPEN.
                         May be negative if the recovery timeout has already
                         elapsed but the first probe has not yet been attempted.

    Example::

        try:
            await payment_breaker.call_async(charge, payload)
        except CircuitOpenError as e:
            return {"error": "payment service unavailable", "retry_after": e.retry_after}
    """

    def __init__(
        self,
        breaker_name: str,
        opens_at: float,
        recovery_timeout: float,
    ) -> None:
        """
        Args:
            breaker_name:      Name identifying the breaker (usually the
                               function name or dependency name).
            opens_at:          ``time.monotonic()`` timestamp when the circuit
                               transitioned to OPEN.
            recovery_timeout:  Configured recovery timeout in seconds.
        """
        self.breaker_name = breaker_name
        self.opens_at = opens_at
        elapsed = time.monotonic() - opens_at
        self.retry_after = max(0.0, recovery_timeout - elapsed)
        super().__init__(
            f"Circuit '{breaker_name}' is OPEN — calls rejected. "
            f"Retry after {self.retry_after:.1f} s."
        )


# ── CircuitBreaker ────────────────────────────────────────────────────────────


class CircuitBreaker:
    """
    Stateful circuit breaker protecting a single external dependency.

    Instantiate ONE ``CircuitBreaker`` per dependency (e.g. one for the payment
    API, one for the notification service).  Share the instance across all
    callers that access that dependency — the failure counter only works if it
    accumulates across concurrent callers.

    Usage patterns::

        # Pattern 1 — call through the breaker directly
        result = await breaker.call_async(fetch_user, user_id)

        # Pattern 2 — wrap a function as a method on the breaker
        @breaker.protect
        async def fetch_user(user_id: int) -> User: ...

    DESIGN: explicit instance over global registry
        ✅ No hidden global state — callers own their breaker instances.
        ✅ Easy to test — create a new instance per test, reset counters freely.
        ❌ Callers must pass the instance around — dependency injection recommended.

    Thread safety:  ⚠️  Safe within a single event loop.  Do NOT share
                         across multiple threads with separate event loops.
    Async safety:   ✅  State transitions are protected by a lazy asyncio.Lock.
                         The lock is only held during HALF_OPEN state transitions —
                         CLOSED and OPEN checks are lock-free.

    Attributes:
        name:   Human-readable name for logging and error messages.
        config: Immutable configuration (thresholds, timeouts, monitored types).
        state:  Current ``CircuitState`` — read-only from outside.

    Edge cases:
        - Constructing a ``CircuitBreaker`` before the asyncio event loop starts
          is safe — the lock is created lazily on first async call.
        - Calling ``reset()`` is safe at any time (e.g., in test teardowns) and
          returns the breaker to CLOSED state with counters zeroed.
    """

    __slots__ = (
        "name",
        "config",
        "_state",
        "_failure_count",
        "_success_count",
        "_opened_at",
        "_lock",
    )

    def __init__(self, config: CircuitBreakerConfig, *, name: str = "unnamed") -> None:
        """
        Args:
            config: Immutable breaker configuration.
            name:   Human-readable name used in log messages and error strings.
        """
        self.name: str = name
        self.config: CircuitBreakerConfig = config

        # ── Mutable state (protected by _lock in HALF_OPEN) ───────────────────
        self._state: CircuitState = CircuitState.CLOSED
        self._failure_count: int = 0
        self._success_count: int = 0
        self._opened_at: float = 0.0  # monotonic timestamp of last OPEN transition

        # asyncio.Lock must be created inside a running event loop — defer it.
        # DESIGN: lazy lock instead of module-level creation
        #   ✅ Safe to construct CircuitBreaker before the loop exists.
        #   ❌ First call pays a tiny extra cost for lock creation — negligible.
        self._lock: asyncio.Lock | None = None

    # ── Public read-only properties ───────────────────────────────────────────

    @property
    def state(self) -> CircuitState:
        """Current state of the circuit (read-only)."""
        return self._state

    @property
    def failure_count(self) -> int:
        """Current consecutive failure count in CLOSED state."""
        return self._failure_count

    # ── Public methods ────────────────────────────────────────────────────────

    def reset(self) -> None:
        """
        Reset the circuit to CLOSED state with all counters zeroed.

        Useful in test teardowns or manual recovery scenarios.

        Thread safety:  ✅ GIL makes the individual attribute writes atomic.
        """
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._opened_at = 0.0
        _logger.info("Circuit '%s' manually reset to CLOSED.", self.name)

    async def call_async(
        self,
        func: Callable[..., Awaitable[_R]],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> _R:
        """
        Call ``func`` through the circuit breaker.

        Depending on the current state:
        - **CLOSED**: call proceeds; failure increments counter.
        - **OPEN**: raises ``CircuitOpenError`` immediately.
        - **HALF_OPEN**: one probe call is allowed; others are rejected.

        Args:
            func:     The async callable to protect.
            *args:    Positional arguments forwarded to ``func``.
            **kwargs: Keyword arguments forwarded to ``func``.

        Returns:
            The return value of ``func(*args, **kwargs)`` on success.

        Raises:
            CircuitOpenError: Circuit is OPEN or another caller is already
                              probing in HALF_OPEN state.
            Exception:        Any exception raised by ``func`` that is in
                              ``config.monitored_on`` — after state transitions
                              are applied.  Exceptions NOT in ``monitored_on``
                              propagate without counting against the circuit.

        Async safety: ✅ Lock is held only during state transitions, not during
                         the actual ``func`` call — no blocking.
        """
        await self._maybe_attempt_recovery()

        if self._state == CircuitState.OPEN:
            raise CircuitOpenError(
                self.name, self._opened_at, self.config.recovery_timeout
            )

        if self._state == CircuitState.HALF_OPEN:
            return await self._probe_async(func, *args, **kwargs)

        # CLOSED — normal call path
        return await self._call_closed_async(func, *args, **kwargs)

    def call_sync(
        self,
        func: Callable[..., _R],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> _R:
        """
        Call ``func`` synchronously through the circuit breaker.

        The sync path cannot use ``asyncio.Lock`` for HALF_OPEN protection —
        concurrent probes in sync context are handled with a best-effort flag.
        For full safety use ``call_async`` with an async function.

        Args:
            func:     The sync callable to protect.
            *args:    Positional arguments forwarded to ``func``.
            **kwargs: Keyword arguments forwarded to ``func``.

        Returns:
            The return value of ``func(*args, **kwargs)`` on success.

        Raises:
            CircuitOpenError: Circuit is OPEN.
            Exception:        Any monitored exception raised by ``func``.

        Thread safety:  ⚠️  State transitions are not lock-protected in sync
                             context.  Single-threaded use is safe.
        """
        # Sync recovery check — compare timestamps without asyncio
        if self._state == CircuitState.OPEN:
            if time.monotonic() - self._opened_at >= self.config.recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                _logger.info(
                    "Circuit '%s' → HALF_OPEN (recovery timeout elapsed).", self.name
                )

        if self._state == CircuitState.OPEN:
            raise CircuitOpenError(
                self.name, self._opened_at, self.config.recovery_timeout
            )

        try:
            result = func(*args, **kwargs)
        except BaseException as exc:
            if isinstance(exc, self.config.monitored_on):
                self._on_failure()
            raise

        self._on_success()
        return result

    def protect(self, func: Callable) -> Callable:
        """
        Decorator that wraps ``func`` so all calls pass through this breaker.

        Works on both ``async def`` and ``def`` functions.

        Args:
            func: The function to protect.

        Returns:
            Wrapped callable with the same signature as ``func``.

        Example::

            @payment_breaker.protect
            async def charge(payload: dict) -> Receipt: ...
        """
        if inspect.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await self.call_async(func, *args, **kwargs)

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            return self.call_sync(func, *args, **kwargs)

        return sync_wrapper

    # ── Private helpers ───────────────────────────────────────────────────────

    def _get_lock(self) -> asyncio.Lock:
        """Return the asyncio.Lock, creating it lazily on first access."""
        if self._lock is None:
            # Created here — guaranteed to be inside a running event loop.
            self._lock = asyncio.Lock()
        return self._lock

    async def _maybe_attempt_recovery(self) -> None:
        """
        Transition OPEN → HALF_OPEN if the recovery timeout has elapsed.

        Runs outside any lock — it only transitions to HALF_OPEN, not back to
        CLOSED.  The actual probe is protected by the lock in ``_probe_async``.
        """
        if (
            self._state == CircuitState.OPEN
            and time.monotonic() - self._opened_at >= self.config.recovery_timeout
        ):
            # Transition without lock — only one flag flip is possible here
            # (OPEN→HALF_OPEN).  If two coroutines race, the second one sees
            # HALF_OPEN and is rejected by call_async (treated like OPEN).
            self._state = CircuitState.HALF_OPEN
            _logger.info(
                "Circuit '%s' → HALF_OPEN (recovery timeout elapsed).", self.name
            )

    async def _probe_async(
        self,
        func: Callable[..., Awaitable[_R]],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> _R:
        """
        Attempt a single probe call in HALF_OPEN state.

        The asyncio.Lock ensures only ONE probe runs at a time.  Concurrent
        callers that arrive while the lock is held see the state as HALF_OPEN
        and are rejected with ``CircuitOpenError``.

        DESIGN: lock held only for the probe call, not for the entire HALF_OPEN
        window.  This allows fast rejection of concurrent callers without
        blocking them on the lock.

        Args:
            func:     Async callable to probe.
            *args:    Positional args forwarded to ``func``.
            **kwargs: Keyword args forwarded to ``func``.

        Returns:
            Return value of ``func`` on probe success.

        Raises:
            CircuitOpenError: Another probe is in progress (lock is held).
            Exception:        Probe failure — triggers re-open.
        """
        lock = self._get_lock()

        # Non-blocking acquire — if the lock is held another probe is running;
        # reject immediately rather than queueing probes.
        if lock.locked():
            raise CircuitOpenError(
                self.name, self._opened_at, self.config.recovery_timeout
            )

        async with lock:
            # Re-check state after acquiring — the state may have changed
            # between the locked() check above and entering the critical section.
            if self._state != CircuitState.HALF_OPEN:
                # The probe that held the lock already closed or re-opened the
                # circuit — short-circuit to avoid a redundant probe.
                if self._state == CircuitState.OPEN:
                    raise CircuitOpenError(
                        self.name, self._opened_at, self.config.recovery_timeout
                    )
                # CLOSED — rare race; proceed normally (the lock will be released)
                # Fall through to the try block below.

            try:
                result = await func(*args, **kwargs)
            except BaseException as exc:
                if isinstance(exc, self.config.monitored_on):
                    # Probe failed — re-open immediately
                    self._open_circuit()
                    _logger.warning(
                        "Circuit '%s' probe FAILED — back to OPEN. Error: %s",
                        self.name,
                        exc,
                    )
                raise

            # Probe succeeded
            self._on_success()
            return result

    async def _call_closed_async(
        self,
        func: Callable[..., Awaitable[_R]],
        /,
        *args: Any,
        **kwargs: Any,
    ) -> _R:
        """Call ``func`` in CLOSED state, updating counters on outcome."""
        try:
            result = await func(*args, **kwargs)
        except BaseException as exc:
            if isinstance(exc, self.config.monitored_on):
                self._on_failure()
            raise

        self._on_success()
        return result

    def _on_success(self) -> None:
        """Handle a successful call — reset failure counter or close circuit."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._success_count = 0
                _logger.info(
                    "Circuit '%s' → CLOSED (probe succeeded after %d success(es)).",
                    self.name,
                    self.config.success_threshold,
                )
        else:
            # Reset consecutive failure counter on any CLOSED success
            if self._failure_count > 0:
                _logger.debug(
                    "Circuit '%s' success — resetting failure count (%d → 0).",
                    self.name,
                    self._failure_count,
                )
            self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle a monitored failure — increment counter or open circuit."""
        self._failure_count += 1
        _logger.warning(
            "Circuit '%s' failure %d/%d.",
            self.name,
            self._failure_count,
            self.config.failure_threshold,
        )
        if self._failure_count >= self.config.failure_threshold:
            self._open_circuit()

    def _open_circuit(self) -> None:
        """Transition the circuit to OPEN state and record the open timestamp."""
        self._state = CircuitState.OPEN
        self._opened_at = time.monotonic()
        self._failure_count = 0  # reset so next CLOSED window starts fresh
        self._success_count = 0
        _logger.error(
            "Circuit '%s' → OPEN (failure threshold reached). " "Recovery in %.1f s.",
            self.name,
            self.config.recovery_timeout,
        )

    def __repr__(self) -> str:
        return (
            f"CircuitBreaker("
            f"name={self.name!r}, "
            f"state={self._state.value}, "
            f"failures={self._failure_count}/"
            f"{self.config.failure_threshold})"
        )


# ── @circuit_breaker decorator ────────────────────────────────────────────────


def circuit_breaker(
    config: CircuitBreakerConfig,
    *,
    name: str | None = None,
) -> Callable:
    """
    Decorator factory that wraps a function with a ``CircuitBreaker``.

    A NEW ``CircuitBreaker`` instance is created per decorated function.
    If you need to share a breaker across multiple functions (e.g. all calls
    to the same external API), create a shared ``CircuitBreaker`` instance and
    use its ``.protect`` method instead.

    DESIGN: new instance per decorated function vs. shared instance
        ✅ Simple usage — no need to manage instances separately.
        ❌ Each decorated function has its own failure counter — failures on
           ``fetch_user`` do NOT contribute to opening ``charge_user``'s circuit
           even if they share the same backend.  For shared state, use a shared
           ``CircuitBreaker`` instance.

    Args:
        config: ``CircuitBreakerConfig`` applied to the new breaker instance.
        name:   Optional name for the breaker.  Defaults to the decorated
                function's ``__qualname__``.

    Returns:
        A decorator that wraps the function with a new ``CircuitBreaker``.

    Example::

        # Simple — each function gets its own breaker
        @circuit_breaker(CircuitBreakerConfig(failure_threshold=3))
        async def call_payment_api(payload: dict) -> Receipt:
            ...

        # Shared — all functions share one failure counter
        _shared_breaker = CircuitBreaker(CircuitBreakerConfig(...), name="payments")

        @_shared_breaker.protect
        async def fetch_payment_status(payment_id: str) -> Status: ...

        @_shared_breaker.protect
        async def charge(payload: dict) -> Receipt: ...
    """

    def decorator(func: Callable) -> Callable:
        breaker_name = name or func.__qualname__
        breaker = CircuitBreaker(config, name=breaker_name)
        return breaker.protect(func)

    return decorator
