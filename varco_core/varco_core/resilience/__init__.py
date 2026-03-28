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

    # Rate limiting (async only)
    from varco_core.resilience import (
        rate_limit,
        RateLimiter,
        RateLimitConfig,
        InMemoryRateLimiter,
        RateLimitExceededError,
    )

    # Bulkhead — concurrency cap per dependency (async only)
    from varco_core.resilience import (
        bulkhead,
        Bulkhead,
        BulkheadConfig,
        BulkheadFullError,
    )

    # Hedged requests — speculative duplicate for tail-latency reduction
    from varco_core.resilience import hedge, HedgeConfig

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

``RateLimiter`` / ``@rate_limit``
    Caps the number of calls within a rolling time window.  ``InMemoryRateLimiter``
    is per-process (single-node); use ``varco_redis.RedisRateLimiter`` for
    distributed (multi-pod) rate limiting.  Async-only.

``Bulkhead`` / ``@bulkhead``
    Limits maximum concurrent calls to a single external dependency.  Prevents
    one slow dependency from starving all async tasks.  Use a SHARED ``Bulkhead``
    instance per dependency.  Async-only.

``@hedge`` / ``HedgeConfig``
    Issues a speculative duplicate call after a delay to reduce tail latency.
    ⚠️  ONLY safe for idempotent operations (reads, upserts).  Async-only.

Composing patterns::

    @timeout(10.0)
    @retry(RetryPolicy(max_attempts=3, base_delay=0.5))
    @circuit_breaker(CircuitBreakerConfig(failure_threshold=5))
    async def call_external_api(payload: dict) -> Response:
        ...

    # Execution order (outermost first):
    # timeout → retry loop → circuit_breaker → actual call

    # Rate limiting + bulkhead — cap concurrent AND per-second calls
    limiter = InMemoryRateLimiter(RateLimitConfig(rate=100, period=1.0))
    db_bh   = Bulkhead(BulkheadConfig(max_concurrent=10))

    @rate_limit(limiter=limiter)
    @db_bh.protect
    async def fetch_user(user_id: int) -> User: ...
"""

from __future__ import annotations

from varco_core.resilience.bulkhead import (
    Bulkhead,
    BulkheadConfig,
    BulkheadFullError,
    bulkhead,
)
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
from varco_core.resilience.hedge import (
    HedgeConfig,
    hedge,
)
from varco_core.resilience.rate_limit import (
    InMemoryRateLimiter,
    RateLimitConfig,
    RateLimitExceededError,
    RateLimiter,
    rate_limit,
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
    # ── Rate limiting ─────────────────────────────────────────────────────────
    "rate_limit",
    "RateLimiter",
    "RateLimitConfig",
    "InMemoryRateLimiter",
    "RateLimitExceededError",
    # ── Bulkhead ──────────────────────────────────────────────────────────────
    "bulkhead",
    "Bulkhead",
    "BulkheadConfig",
    "BulkheadFullError",
    # ── Hedged requests ───────────────────────────────────────────────────────
    "hedge",
    "HedgeConfig",
]
