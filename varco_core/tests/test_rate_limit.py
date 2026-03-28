"""
Tests for varco_core.resilience.rate_limit
==========================================
Covers ``RateLimitConfig``, ``InMemoryRateLimiter``, ``RateLimitExceededError``,
and the ``@rate_limit`` decorator.

All tests are async (asyncio_mode = "auto" is set project-wide).
Time-dependent tests use ``unittest.mock.patch`` on ``time.monotonic`` to
ensure determinism — no real sleeps.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from varco_core.resilience.rate_limit import (
    InMemoryRateLimiter,
    RateLimitConfig,
    RateLimitExceededError,
    RateLimiter,
    rate_limit,
)


# ── RateLimitConfig ───────────────────────────────────────────────────────────


class TestRateLimitConfig:
    def test_defaults_applied(self) -> None:
        cfg = RateLimitConfig(rate=5, period=1.0)
        assert cfg.rate == 5
        assert cfg.period == 1.0

    def test_frozen(self) -> None:
        cfg = RateLimitConfig(rate=5, period=1.0)
        with pytest.raises((AttributeError, TypeError)):
            cfg.rate = 10  # type: ignore[misc]

    def test_rate_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="rate must be ≥ 1"):
            RateLimitConfig(rate=0, period=1.0)

    def test_period_must_be_positive(self) -> None:
        with pytest.raises(ValueError, match="period must be > 0"):
            RateLimitConfig(rate=5, period=0.0)

    def test_negative_period_rejected(self) -> None:
        with pytest.raises(ValueError, match="period must be > 0"):
            RateLimitConfig(rate=5, period=-1.0)

    def test_repr(self) -> None:
        cfg = RateLimitConfig(rate=3, period=2.0)
        assert "3" in repr(cfg)
        assert "2.0" in repr(cfg)


# ── RateLimitExceededError ────────────────────────────────────────────────────


class TestRateLimitExceededError:
    def test_attributes(self) -> None:
        err = RateLimitExceededError("user:42", 0.5)
        assert err.key == "user:42"
        assert err.retry_after == pytest.approx(0.5)

    def test_negative_retry_after_clamped_to_zero(self) -> None:
        err = RateLimitExceededError("k", -1.0)
        assert err.retry_after == 0.0

    def test_message_contains_key_and_wait(self) -> None:
        err = RateLimitExceededError("mykey", 1.23)
        assert "mykey" in str(err)
        assert "1.230" in str(err)


# ── InMemoryRateLimiter ───────────────────────────────────────────────────────


class TestInMemoryRateLimiterBasic:
    async def test_first_call_always_allowed(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=3, period=1.0))
        assert await limiter.acquire("k") is True

    async def test_calls_within_rate_all_allowed(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=3, period=1.0))
        results = [await limiter.acquire("k") for _ in range(3)]
        assert all(results)

    async def test_call_beyond_rate_denied(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=3, period=10.0))
        for _ in range(3):
            await limiter.acquire("k")
        # 4th call within the same window should be denied.
        assert await limiter.acquire("k") is False

    async def test_different_keys_independent(self) -> None:
        # rate=1 — each key has its own budget of 1 call.
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=1, period=10.0))
        assert await limiter.acquire("a") is True
        assert await limiter.acquire("b") is True
        # Both keys are now full.
        assert await limiter.acquire("a") is False
        assert await limiter.acquire("b") is False

    async def test_reset_clears_window(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=1, period=10.0))
        await limiter.acquire("k")
        assert await limiter.acquire("k") is False  # full
        await limiter.reset("k")
        # After reset, the key starts with a clean slate.
        assert await limiter.acquire("k") is True

    async def test_reset_unknown_key_is_noop(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=5, period=1.0))
        # Should not raise even if the key was never seen.
        await limiter.reset("never-seen")

    async def test_repr_contains_rate_and_period(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=5, period=2.0))
        r = repr(limiter)
        assert "5" in r
        assert "2.0" in r


class TestInMemoryRateLimiterSlidingWindow:
    """Time-sensitive tests using a mocked monotonic clock."""

    async def test_expired_call_frees_slot(self) -> None:
        """
        If the oldest call has scrolled past the window boundary, a new call
        should be allowed even though the deque is at max capacity.
        """
        cfg = RateLimitConfig(rate=2, period=1.0)
        limiter = InMemoryRateLimiter(cfg)

        # t=0: fill the window
        with patch("varco_core.resilience.rate_limit.time.monotonic", return_value=0.0):
            assert await limiter.acquire("k") is True
            assert await limiter.acquire("k") is True
            # Window full — denied at t=0.
            assert await limiter.acquire("k") is False

        # t=1.001: the first call at t=0 is now outside the window (period=1.0).
        # The slot it occupied is free; new call should be allowed.
        with patch(
            "varco_core.resilience.rate_limit.time.monotonic", return_value=1.001
        ):
            assert await limiter.acquire("k") is True

    async def test_call_at_window_boundary_allowed(self) -> None:
        """
        A call at exactly t=period (now - oldest == period) IS allowed.

        The implementation uses ``>=`` — the slot is considered free as soon
        as the elapsed time reaches the full period.  This is the more
        user-friendly interpretation: if you configured rate=1 per second,
        you should be allowed to call again after exactly 1 second.
        """
        cfg = RateLimitConfig(rate=1, period=1.0)
        limiter = InMemoryRateLimiter(cfg)

        with patch("varco_core.resilience.rate_limit.time.monotonic", return_value=0.0):
            await limiter.acquire("k")  # t=0 recorded

        # At t=1.0 exactly, now - oldest = 1.0 >= period (1.0) — allowed.
        with patch("varco_core.resilience.rate_limit.time.monotonic", return_value=1.0):
            assert await limiter.acquire("k") is True

        # At t=0.999, now - oldest = 0.999 < period (1.0) — still denied.
        limiter2 = InMemoryRateLimiter(cfg)
        with patch("varco_core.resilience.rate_limit.time.monotonic", return_value=0.0):
            await limiter2.acquire("k")
        with patch(
            "varco_core.resilience.rate_limit.time.monotonic", return_value=0.999
        ):
            assert await limiter2.acquire("k") is False


class TestInMemoryRateLimiterRetryAfter:
    async def test_retry_after_zero_when_not_full(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=5, period=1.0))
        assert await limiter.retry_after("k") == 0.0

    async def test_retry_after_zero_for_unknown_key(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=5, period=1.0))
        assert await limiter.retry_after("unknown") == 0.0

    async def test_retry_after_positive_when_full(self) -> None:
        cfg = RateLimitConfig(rate=1, period=2.0)
        limiter = InMemoryRateLimiter(cfg)

        with patch("varco_core.resilience.rate_limit.time.monotonic", return_value=0.0):
            await limiter.acquire("k")  # recorded at t=0

        # At t=0.5, the slot won't free until t=2.0 → retry_after ≈ 1.5 s.
        with patch("varco_core.resilience.rate_limit.time.monotonic", return_value=0.5):
            wait = await limiter.retry_after("k")
        assert wait == pytest.approx(1.5, abs=0.01)

    async def test_retry_after_clamped_to_zero(self) -> None:
        """If the window has already expired, retry_after returns 0 not negative."""
        cfg = RateLimitConfig(rate=1, period=1.0)
        limiter = InMemoryRateLimiter(cfg)

        with patch("varco_core.resilience.rate_limit.time.monotonic", return_value=0.0):
            await limiter.acquire("k")

        # At t=5.0 the entry has long expired — retry_after should be 0.
        with patch("varco_core.resilience.rate_limit.time.monotonic", return_value=5.0):
            wait = await limiter.retry_after("k")
        assert wait == 0.0


# ── @rate_limit decorator ─────────────────────────────────────────────────────


class TestRateLimitDecorator:
    async def test_allowed_call_passes_through(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=5, period=1.0))

        @rate_limit(limiter=limiter)
        async def handler(x: int) -> int:
            return x * 2

        result = await handler(3)
        assert result == 6

    async def test_denied_call_raises_rate_limit_exceeded_error(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=1, period=10.0))

        @rate_limit(limiter=limiter)
        async def handler() -> str:
            return "ok"

        await handler()  # consumes the single slot
        with pytest.raises(RateLimitExceededError) as exc_info:
            await handler()
        assert exc_info.value.key == "default"

    async def test_key_fn_used_for_per_caller_budget(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=1, period=10.0))

        @rate_limit(limiter=limiter, key_fn=lambda user_id: user_id)
        async def handler(user_id: str) -> str:
            return user_id

        # Each user gets their own budget.
        assert await handler("alice") == "alice"
        assert await handler("bob") == "bob"
        # alice's slot is full — denied.
        with pytest.raises(RateLimitExceededError) as exc_info:
            await handler("alice")
        assert exc_info.value.key == "alice"
        # bob's slot is also full.
        with pytest.raises(RateLimitExceededError) as exc_info:
            await handler("bob")
        assert exc_info.value.key == "bob"

    def test_sync_function_raises_type_error(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=5, period=1.0))
        with pytest.raises(TypeError, match="async functions"):

            @rate_limit(limiter=limiter)
            def sync_handler() -> None:  # type: ignore[return]
                pass

    async def test_functools_wraps_preserves_name(self) -> None:
        limiter = InMemoryRateLimiter(RateLimitConfig(rate=5, period=1.0))

        @rate_limit(limiter=limiter)
        async def my_named_handler() -> None:
            pass

        assert my_named_handler.__name__ == "my_named_handler"

    async def test_error_contains_retry_after(self) -> None:
        """RateLimitExceededError should carry a non-negative retry_after."""
        cfg = RateLimitConfig(rate=1, period=2.0)
        limiter = InMemoryRateLimiter(cfg)

        @rate_limit(limiter=limiter)
        async def handler() -> None:
            pass

        with patch("varco_core.resilience.rate_limit.time.monotonic", return_value=0.0):
            await handler()

        with (
            patch("varco_core.resilience.rate_limit.time.monotonic", return_value=0.5),
            pytest.raises(RateLimitExceededError) as exc_info,
        ):
            await handler()

        assert exc_info.value.retry_after > 0.0


# ── RateLimiter ABC ───────────────────────────────────────────────────────────


class TestRateLimiterABC:
    def test_cannot_instantiate_abstract_class(self) -> None:
        with pytest.raises(TypeError):
            RateLimiter()  # type: ignore[abstract]

    def test_concrete_subclass_must_implement_all_methods(self) -> None:
        """Partial implementation should raise TypeError at instantiation."""

        class Partial(RateLimiter):
            async def acquire(self, key: str = "default") -> bool:
                return True

            # Missing reset() and retry_after()

        with pytest.raises(TypeError):
            Partial()  # type: ignore[abstract]

    async def test_custom_implementation_works_with_decorator(self) -> None:
        """Any RateLimiter subclass should be usable with @rate_limit."""

        class AlwaysAllowLimiter(RateLimiter):
            async def acquire(self, key: str = "default") -> bool:
                return True

            async def reset(self, key: str = "default") -> None:
                pass

            async def retry_after(self, key: str = "default") -> float:
                return 0.0

        limiter = AlwaysAllowLimiter()

        @rate_limit(limiter=limiter)
        async def handler() -> str:
            return "ok"

        assert await handler() == "ok"
