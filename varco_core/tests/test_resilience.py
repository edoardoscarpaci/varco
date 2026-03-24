"""
Unit tests for varco_core.resilience
=====================================
Covers all three resilience primitives without any external dependencies.

Sections
--------
- ``RetryPolicy``          — construction validation, compute_delay, is_retryable
- ``@retry`` (async)       — success, retry on error, exhaust, non-retryable errors,
                             max_attempts=1, retryable_on=()
- ``@retry`` (sync)        — success, retry, exhaust
- ``RetryExhaustedError``  — attributes, chaining
- ``@timeout``             — success within limit, timeout exceeded, sync function
                             raises TypeError, seconds<=0 raises ValueError
- ``CallTimeoutError``     — attributes
- ``CircuitBreakerConfig`` — defaults, frozen, validation
- ``CircuitBreaker``       — CLOSED normal flow, opens on threshold, OPEN rejects,
                             HALF_OPEN probe success (closes), HALF_OPEN probe failure (re-opens),
                             reset(), call_sync(), protect()
- ``@circuit_breaker``     — decorator creates per-function breaker
- ``CircuitOpenError``     — attributes

Test strategy
-------------
- ``jitter=False`` and ``base_delay=0`` in all retry tests — no real sleeping.
- ``recovery_timeout=0.0`` in circuit-breaker tests — instant HALF_OPEN transition.
- Plain Python no-argument lambdas as test callables — no mocking library.
"""

from __future__ import annotations

import asyncio

import pytest

from varco_core.resilience import (
    CallTimeoutError,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    RetryExhaustedError,
    RetryPolicy,
    circuit_breaker,
    retry,
    timeout,
)


# ── RetryPolicy ────────────────────────────────────────────────────────────────


class TestRetryPolicy:
    def test_defaults(self) -> None:
        p = RetryPolicy()
        assert p.max_attempts == 3
        assert p.base_delay == 1.0
        assert p.max_delay == 60.0
        assert p.exponential_base == 2.0
        assert p.jitter is True

    def test_max_attempts_below_one_raises(self) -> None:
        with pytest.raises(ValueError, match="max_attempts"):
            RetryPolicy(max_attempts=0)

    def test_negative_base_delay_raises(self) -> None:
        with pytest.raises(ValueError, match="base_delay"):
            RetryPolicy(base_delay=-1.0)

    def test_negative_max_delay_raises(self) -> None:
        with pytest.raises(ValueError, match="max_delay"):
            RetryPolicy(max_delay=-1.0)

    def test_zero_exponential_base_raises(self) -> None:
        with pytest.raises(ValueError, match="exponential_base"):
            RetryPolicy(exponential_base=0.0)

    def test_compute_delay_no_jitter(self) -> None:
        # base_delay=1, exp=2, no jitter → 1.0 * 2^0 = 1.0
        p = RetryPolicy(base_delay=1.0, exponential_base=2.0, jitter=False)
        assert p.compute_delay(0) == pytest.approx(1.0)
        assert p.compute_delay(1) == pytest.approx(2.0)
        assert p.compute_delay(2) == pytest.approx(4.0)

    def test_compute_delay_capped_at_max_delay(self) -> None:
        # max_delay=3, so delay is capped before reaching 8.0
        p = RetryPolicy(
            base_delay=1.0, exponential_base=2.0, max_delay=3.0, jitter=False
        )
        assert p.compute_delay(3) == pytest.approx(3.0)

    def test_compute_delay_with_jitter_in_range(self) -> None:
        # With jitter the delay should be within [0.5×raw, 1.5×raw]
        p = RetryPolicy(base_delay=4.0, exponential_base=2.0, jitter=True)
        for _ in range(20):
            d = p.compute_delay(0)
            assert 2.0 <= d <= 6.0

    def test_is_retryable_default_includes_exception(self) -> None:
        p = RetryPolicy()
        assert p.is_retryable(ValueError("boom")) is True

    def test_is_retryable_specific_type_matches(self) -> None:
        p = RetryPolicy(retryable_on=(ConnectionError,))
        assert p.is_retryable(ConnectionError("down")) is True
        assert p.is_retryable(ValueError("type error")) is False

    def test_is_retryable_empty_tuple_never_retries(self) -> None:
        p = RetryPolicy(retryable_on=())
        assert p.is_retryable(Exception("x")) is False

    def test_frozen(self) -> None:
        p = RetryPolicy()
        with pytest.raises(Exception):
            p.max_attempts = 99  # type: ignore[misc]


# ── @retry (async) ────────────────────────────────────────────────────────────


class TestRetryAsync:
    async def test_success_on_first_attempt(self) -> None:
        # Policy that would retry but function succeeds immediately
        call_count = 0

        @retry(RetryPolicy(max_attempts=3, base_delay=0, jitter=False))
        async def fn() -> str:
            nonlocal call_count
            call_count += 1
            return "ok"

        result = await fn()
        assert result == "ok"
        assert call_count == 1

    async def test_retries_on_failure_then_succeeds(self) -> None:
        attempts = 0

        @retry(RetryPolicy(max_attempts=3, base_delay=0, jitter=False))
        async def fn() -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                raise ConnectionError("down")
            return "ok"

        result = await fn()
        assert result == "ok"
        assert attempts == 2

    async def test_exhausts_all_attempts_raises_retry_exhausted(self) -> None:
        attempts = 0

        @retry(RetryPolicy(max_attempts=3, base_delay=0, jitter=False))
        async def fn() -> None:
            nonlocal attempts
            attempts += 1
            raise ConnectionError("always fails")

        with pytest.raises(RetryExhaustedError) as exc_info:
            await fn()

        assert exc_info.value.attempts == 3
        assert attempts == 3
        # Original exception is chained
        assert isinstance(exc_info.value.last_exc, ConnectionError)
        assert exc_info.value.__cause__ is exc_info.value.last_exc

    async def test_non_retryable_exception_propagates_immediately(self) -> None:
        attempts = 0

        @retry(
            RetryPolicy(
                max_attempts=3,
                base_delay=0,
                jitter=False,
                retryable_on=(ConnectionError,),
            )
        )
        async def fn() -> None:
            nonlocal attempts
            attempts += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError, match="not retryable"):
            await fn()

        # Must not retry — only one attempt
        assert attempts == 1

    async def test_max_attempts_one_no_retry(self) -> None:
        attempts = 0

        @retry(RetryPolicy(max_attempts=1, base_delay=0, jitter=False))
        async def fn() -> None:
            nonlocal attempts
            attempts += 1
            raise RuntimeError("fail")

        with pytest.raises(RetryExhaustedError):
            await fn()

        # Exactly one attempt made
        assert attempts == 1

    async def test_empty_retryable_on_propagates_immediately(self) -> None:
        attempts = 0

        @retry(RetryPolicy(max_attempts=5, base_delay=0, jitter=False, retryable_on=()))
        async def fn() -> None:
            nonlocal attempts
            attempts += 1
            raise Exception("err")

        with pytest.raises(Exception, match="err"):
            await fn()

        assert attempts == 1

    async def test_preserves_return_value_type(self) -> None:
        @retry(RetryPolicy(max_attempts=2, base_delay=0, jitter=False))
        async def fn() -> list[int]:
            return [1, 2, 3]

        assert await fn() == [1, 2, 3]


# ── @retry (sync) ─────────────────────────────────────────────────────────────


class TestRetrySync:
    def test_success_on_first_attempt(self) -> None:
        @retry(RetryPolicy(max_attempts=3, base_delay=0, jitter=False))
        def fn() -> int:
            return 42

        assert fn() == 42

    def test_retries_then_succeeds(self) -> None:
        attempts = 0

        @retry(RetryPolicy(max_attempts=3, base_delay=0, jitter=False))
        def fn() -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise OSError("retry me")
            return "done"

        assert fn() == "done"
        assert attempts == 3

    def test_exhaust_raises_retry_exhausted(self) -> None:
        @retry(RetryPolicy(max_attempts=2, base_delay=0, jitter=False))
        def fn() -> None:
            raise IOError("fail")

        with pytest.raises(RetryExhaustedError) as exc_info:
            fn()

        assert exc_info.value.attempts == 2


# ── RetryExhaustedError ───────────────────────────────────────────────────────


class TestRetryExhaustedError:
    def test_attributes(self) -> None:
        cause = ValueError("root")
        err = RetryExhaustedError("my_fn", 3, cause)
        assert err.attempts == 3
        assert err.last_exc is cause
        assert err.func_name == "my_fn"

    def test_message_includes_func_name_and_attempts(self) -> None:
        err = RetryExhaustedError("payment_api", 5, RuntimeError("boom"))
        assert "payment_api" in str(err)
        assert "5" in str(err)


# ── @timeout ──────────────────────────────────────────────────────────────────


class TestTimeout:
    async def test_success_within_limit(self) -> None:
        @timeout(5.0)
        async def fast() -> str:
            return "done"

        assert await fast() == "done"

    async def test_raises_call_timeout_error_on_exceed(self) -> None:
        @timeout(0.01)  # 10ms
        async def slow() -> None:
            await asyncio.sleep(10.0)

        with pytest.raises(CallTimeoutError) as exc_info:
            await slow()

        assert exc_info.value.limit == pytest.approx(0.01)
        assert "slow" in exc_info.value.func_name
        # Chains asyncio.TimeoutError
        assert isinstance(exc_info.value.__cause__, asyncio.TimeoutError)

    def test_decorating_sync_function_raises_type_error(self) -> None:
        with pytest.raises(TypeError, match="async"):

            @timeout(5.0)
            def sync_fn() -> None:
                pass

    def test_zero_seconds_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="seconds > 0"):
            timeout(0)

    def test_negative_seconds_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="seconds > 0"):
            timeout(-1.0)

    async def test_preserves_return_value(self) -> None:
        @timeout(5.0)
        async def fn(x: int) -> int:
            return x * 2

        assert await fn(21) == 42


# ── CallTimeoutError ──────────────────────────────────────────────────────────


class TestCallTimeoutError:
    def test_attributes(self) -> None:
        err = CallTimeoutError("some_fn", 3.5)
        assert err.func_name == "some_fn"
        assert err.limit == pytest.approx(3.5)

    def test_message_includes_func_and_limit(self) -> None:
        err = CallTimeoutError("fetch_user", 10.0)
        assert "fetch_user" in str(err)
        assert "10.0" in str(err)


# ── CircuitBreakerConfig ──────────────────────────────────────────────────────


class TestCircuitBreakerConfig:
    def test_defaults(self) -> None:
        c = CircuitBreakerConfig()
        assert c.failure_threshold == 5
        assert c.recovery_timeout == 30.0
        assert c.success_threshold == 1

    def test_frozen(self) -> None:
        c = CircuitBreakerConfig()
        with pytest.raises(Exception):
            c.failure_threshold = 99  # type: ignore[misc]

    def test_failure_threshold_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="failure_threshold"):
            CircuitBreakerConfig(failure_threshold=0)

    def test_negative_recovery_timeout_raises(self) -> None:
        with pytest.raises(ValueError, match="recovery_timeout"):
            CircuitBreakerConfig(recovery_timeout=-1.0)

    def test_success_threshold_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="success_threshold"):
            CircuitBreakerConfig(success_threshold=0)


# ── CircuitBreaker — CLOSED state ─────────────────────────────────────────────


class TestCircuitBreakerClosed:
    async def test_initial_state_is_closed(self) -> None:
        b = CircuitBreaker(CircuitBreakerConfig())
        assert b.state == CircuitState.CLOSED
        assert b.failure_count == 0

    async def test_successful_call_returns_value(self) -> None:
        b = CircuitBreaker(CircuitBreakerConfig())
        await b.call_async(lambda: asyncio.sleep(0) or True)

        async def succeeding() -> str:
            return "value"

        assert await b.call_async(succeeding) == "value"

    async def test_failure_increments_count(self) -> None:
        b = CircuitBreaker(CircuitBreakerConfig(failure_threshold=5))

        async def failing() -> None:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError):
            await b.call_async(failing)

        assert b.failure_count == 1

    async def test_success_resets_failure_count(self) -> None:
        b = CircuitBreaker(CircuitBreakerConfig(failure_threshold=5))

        async def failing() -> None:
            raise RuntimeError("boom")

        async def succeeding() -> str:
            return "ok"

        # Cause a failure first
        with pytest.raises(RuntimeError):
            await b.call_async(failing)

        assert b.failure_count == 1

        # Then succeed — counter should reset
        await b.call_async(succeeding)
        assert b.failure_count == 0

    async def test_opens_after_threshold(self) -> None:
        b = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))

        async def failing() -> None:
            raise RuntimeError("fail")

        for _ in range(3):
            with pytest.raises(RuntimeError):
                await b.call_async(failing)

        assert b.state == CircuitState.OPEN

    async def test_non_monitored_exception_does_not_count(self) -> None:
        # Only ConnectionError is monitored; ValueError must not count
        b = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=2, monitored_on=(ConnectionError,))
        )

        async def raises_value_error() -> None:
            raise ValueError("not monitored")

        with pytest.raises(ValueError):
            await b.call_async(raises_value_error)

        # Counter must stay at 0
        assert b.failure_count == 0
        assert b.state == CircuitState.CLOSED


# ── CircuitBreaker — OPEN state ───────────────────────────────────────────────


class TestCircuitBreakerOpen:
    async def _trip_breaker(self, b: CircuitBreaker) -> None:
        """Helper: trip the breaker to OPEN by hitting the threshold."""
        threshold = b.config.failure_threshold

        async def failing() -> None:
            raise RuntimeError("fail")

        for _ in range(threshold):
            with pytest.raises(RuntimeError):
                await b.call_async(failing)

    async def test_open_rejects_calls_immediately(self) -> None:
        b = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=2, recovery_timeout=60.0)
        )
        await self._trip_breaker(b)
        assert b.state == CircuitState.OPEN

        async def should_not_run() -> None:
            raise AssertionError("must not be called")

        with pytest.raises(CircuitOpenError):
            await b.call_async(should_not_run)

    async def test_circuit_open_error_attributes(self) -> None:
        b = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=1, recovery_timeout=30.0),
            name="payments",
        )
        await self._trip_breaker(b)

        with pytest.raises(CircuitOpenError) as exc_info:

            async def fn() -> None:
                pass

            await b.call_async(fn)

        err = exc_info.value
        assert err.breaker_name == "payments"
        assert err.retry_after >= 0

    async def test_repr_shows_open_state(self) -> None:
        b = CircuitBreaker(CircuitBreakerConfig(failure_threshold=1), name="test-b")
        await self._trip_breaker(b)
        assert "open" in repr(b).lower()


# ── CircuitBreaker — HALF_OPEN state ──────────────────────────────────────────


class TestCircuitBreakerHalfOpen:
    async def _trip_and_wait(self, b: CircuitBreaker) -> None:
        """Trip the breaker, then force recovery_timeout to 0 to allow probe."""
        threshold = b.config.failure_threshold

        async def failing() -> None:
            raise RuntimeError("fail")

        for _ in range(threshold):
            with pytest.raises(RuntimeError):
                await b.call_async(failing)

        # recovery_timeout=0 means the next call transitions to HALF_OPEN
        # We must force time to pass; since recovery_timeout=0, it's already elapsed.

    async def test_probe_success_closes_circuit(self) -> None:
        # recovery_timeout=0 means instant recovery
        b = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.0)
        )
        await self._trip_and_wait(b)
        assert b.state == CircuitState.OPEN

        async def probe() -> str:
            return "recovered"

        # With timeout=0, calling transitions to HALF_OPEN and the probe succeeds
        result = await b.call_async(probe)
        assert result == "recovered"
        assert b.state == CircuitState.CLOSED

    async def test_probe_failure_reopens_circuit(self) -> None:
        b = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.0)
        )
        await self._trip_and_wait(b)

        async def bad_probe() -> None:
            raise ConnectionError("still broken")

        with pytest.raises(ConnectionError):
            await b.call_async(bad_probe)

        assert b.state == CircuitState.OPEN

    async def test_reset_returns_to_closed(self) -> None:
        b = CircuitBreaker(
            CircuitBreakerConfig(failure_threshold=2, recovery_timeout=0.0)
        )
        await self._trip_and_wait(b)
        b.reset()
        assert b.state == CircuitState.CLOSED
        assert b.failure_count == 0


# ── CircuitBreaker — sync path ────────────────────────────────────────────────


class TestCircuitBreakerSync:
    def test_sync_success_returns_value(self) -> None:
        b = CircuitBreaker(CircuitBreakerConfig())
        assert b.call_sync(lambda: 42) == 42

    def test_sync_failure_counts(self) -> None:
        b = CircuitBreaker(CircuitBreakerConfig(failure_threshold=3))

        def failing() -> None:
            raise RuntimeError("err")

        with pytest.raises(RuntimeError):
            b.call_sync(failing)

        assert b.failure_count == 1

    def test_sync_protect_decorator(self) -> None:
        b = CircuitBreaker(CircuitBreakerConfig())

        @b.protect
        def fn(x: int) -> int:
            return x + 1

        assert fn(10) == 11

    async def test_async_protect_decorator(self) -> None:
        b = CircuitBreaker(CircuitBreakerConfig())

        @b.protect
        async def fn(x: int) -> int:
            return x + 1

        assert await fn(10) == 11


# ── @circuit_breaker decorator ────────────────────────────────────────────────


class TestCircuitBreakerDecorator:
    async def test_decorator_creates_breaker_per_function(self) -> None:
        cfg = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=60.0)

        @circuit_breaker(cfg)
        async def fn() -> str:
            return "hello"

        assert await fn() == "hello"

    async def test_decorator_opens_after_failures(self) -> None:
        cfg = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=60.0)
        attempts = 0

        @circuit_breaker(cfg)
        async def fragile() -> None:
            nonlocal attempts
            attempts += 1
            raise RuntimeError("down")

        with pytest.raises(RuntimeError):
            await fragile()
        with pytest.raises(RuntimeError):
            await fragile()

        # Now the circuit should be open — next call must be rejected
        with pytest.raises(CircuitOpenError):
            await fragile()

        # Only 2 actual calls were made (not 3)
        assert attempts == 2


# ── CircuitOpenError ──────────────────────────────────────────────────────────


class TestCircuitOpenError:
    def test_attributes(self) -> None:
        import time

        opens_at = time.monotonic()
        err = CircuitOpenError("my-breaker", opens_at, 30.0)
        assert err.breaker_name == "my-breaker"
        assert err.retry_after >= 0.0

    def test_message_contains_name(self) -> None:
        import time

        err = CircuitOpenError("payments", time.monotonic(), 10.0)
        assert "payments" in str(err)
        assert "OPEN" in str(err)
