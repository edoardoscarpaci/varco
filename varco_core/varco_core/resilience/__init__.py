"""
varco_core.resilience
======================
General-purpose resilience patterns for sync and async callables.

All public symbols are importable directly from ``varco_core.resilience``::

    # Retry with exponential back-off
    from varco_core.resilience import retry, RetryPolicy, RetryExhaustedError

    # Circuit breaker
    from varco_core.resilience import (
        circuit_breaker,
        CircuitBreaker,
        CircuitBreakerConfig,
        CircuitState,
        CircuitOpenError,
    )

    # Timeout (async only)
    from varco_core.resilience import timeout, CallTimeoutError

Patterns overview
-----------------
``@retry`` / ``RetryPolicy``
    Retries a failing function up to ``max_attempts`` times with exponential
    back-off and optional jitter.  Works on both ``def`` and ``async def``.

``CircuitBreaker`` / ``@circuit_breaker``
    Prevents repeated calls to a failing dependency.  CLOSED → OPEN → HALF_OPEN
    state machine.  Use a SHARED ``CircuitBreaker`` instance per external
    dependency to accumulate failures across all callers correctly.

``@timeout``
    Cancels an async function if it doesn't complete within a time limit.
    Async-only — sync timeouts are out of scope (require OS signals or threads).

Composing patterns::

    @timeout(10.0)
    @retry(RetryPolicy(max_attempts=3, base_delay=0.5))
    @circuit_breaker(CircuitBreakerConfig(failure_threshold=5))
    async def call_external_api(payload: dict) -> Response:
        ...

    # Execution order (outermost first):
    # timeout → retry loop → circuit_breaker → actual call
"""

from __future__ import annotations

from varco_core.resilience.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    circuit_breaker,
)
from varco_core.resilience.retry import (
    RetryExhaustedError,
    RetryPolicy,
    retry,
)
from varco_core.resilience.timeout import (
    CallTimeoutError,
    timeout,
)

__all__ = [
    # ── Retry ────────────────────────────────────────────────────────────────
    "retry",
    "RetryPolicy",
    "RetryExhaustedError",
    # ── Circuit breaker ──────────────────────────────────────────────────────
    "circuit_breaker",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "CircuitOpenError",
    # ── Timeout ──────────────────────────────────────────────────────────────
    "timeout",
    "CallTimeoutError",
]
