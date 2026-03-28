"""
Integration tests for varco_redis.rate_limit
=============================================
Spins up a real Redis instance via testcontainers and verifies end-to-end
sliding-window behaviour of ``RedisRateLimiter``.

DISABLED BY DEFAULT — requires Docker.  Run with::

    pytest -m integration tests/test_redis_rate_limit_integration.py

Or set the ``VARCO_RUN_INTEGRATION`` env var::

    VARCO_RUN_INTEGRATION=1 pytest tests/test_redis_rate_limit_integration.py

Prerequisites:
    - Docker daemon running
    - testcontainers[redis] installed (see pyproject.toml dev dependencies)
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

pytestmark = pytest.mark.integration

if not os.environ.get("VARCO_RUN_INTEGRATION"):
    pytest.skip(
        "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration",
        allow_module_level=True,
    )


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def redis_container():
    """Start a Redis instance for the integration test module."""
    from testcontainers.redis import RedisContainer

    with RedisContainer() as redis:
        yield redis


@pytest.fixture
async def limiter(redis_container):
    """
    Connected ``RedisRateLimiter`` backed by the testcontainers Redis instance.

    Uses a unique key prefix per test to prevent cross-test interference.
    """
    from varco_core.resilience.rate_limit import RateLimitConfig
    from varco_redis.config import RedisEventBusSettings
    from varco_redis.rate_limit import RedisRateLimiter

    prefix = f"test:{uuid.uuid4().hex[:8]}:"
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    url = f"redis://{host}:{port}/0"

    cfg = RateLimitConfig(rate=5, period=2.0)
    settings = RedisEventBusSettings(url=url, channel_prefix=prefix)
    async with RedisRateLimiter(cfg, settings=settings) as lim:
        yield lim


# ── Tests ─────────────────────────────────────────────────────────────────────


async def test_calls_within_rate_allowed(limiter) -> None:
    """All 5 calls in the window should be allowed."""
    for _ in range(5):
        allowed = await limiter.acquire("k")
        assert allowed is True


async def test_call_beyond_rate_denied(limiter) -> None:
    """The 6th call within the same window should be denied."""
    for _ in range(5):
        await limiter.acquire("k")

    result = await limiter.acquire("k")
    assert result is False


async def test_different_keys_are_independent(limiter) -> None:
    """Separate keys must have independent budgets."""
    # Exhaust key "a"
    for _ in range(5):
        await limiter.acquire("a")
    assert await limiter.acquire("a") is False

    # Key "b" should still have a full budget.
    assert await limiter.acquire("b") is True


async def test_reset_clears_key(limiter) -> None:
    """After reset, the key should accept calls again."""
    for _ in range(5):
        await limiter.acquire("k")

    await limiter.reset("k")
    assert await limiter.acquire("k") is True


async def test_retry_after_positive_when_full(limiter) -> None:
    """retry_after() should return a positive value when the window is full."""
    for _ in range(5):
        await limiter.acquire("k")

    wait = await limiter.retry_after("k")
    assert wait > 0.0


async def test_retry_after_zero_when_not_full(limiter) -> None:
    """retry_after() should return 0.0 when the budget is not exhausted."""
    # Fresh key — no calls recorded yet.
    wait = await limiter.retry_after("fresh-key")
    assert wait == 0.0


async def test_window_expires_after_period(limiter) -> None:
    """
    After the configured period (2 s), the window should reset and new calls
    should be allowed.  Uses a short-lived limiter with period=0.2 s to keep
    the test fast.
    """
    from varco_core.resilience.rate_limit import RateLimitConfig
    from varco_redis.config import RedisEventBusSettings
    from varco_redis.rate_limit import RedisRateLimiter

    # Build a fresh limiter with a very short period.
    host = limiter.settings.url.split("//")[1].split(":")[0]
    port = limiter.settings.url.split(":")[-1].split("/")[0]
    url = f"redis://{host}:{port}/0"

    cfg = RateLimitConfig(rate=2, period=0.3)
    settings = RedisEventBusSettings(
        url=url, channel_prefix=f"test:{uuid.uuid4().hex[:8]}:"
    )
    async with RedisRateLimiter(cfg, settings=settings) as short_limiter:
        # Fill the window.
        await short_limiter.acquire("k")
        await short_limiter.acquire("k")
        assert await short_limiter.acquire("k") is False  # full

        # Wait for the window to expire.
        await asyncio.sleep(0.35)

        # Window should have expired — new calls allowed.
        assert await short_limiter.acquire("k") is True


async def test_concurrent_acquires_respect_rate(limiter) -> None:
    """
    Many concurrent callers should collectively not exceed the rate limit.

    With rate=5 and 10 concurrent acquire() calls, exactly 5 should succeed
    and 5 should be denied.  This validates that the Lua script is atomic.
    """
    # Use a fresh key for this test.
    key = f"concurrent:{uuid.uuid4().hex[:8]}"
    results = await asyncio.gather(*[limiter.acquire(key) for _ in range(10)])
    allowed = sum(1 for r in results if r)
    denied = sum(1 for r in results if not r)

    assert allowed == 5
    assert denied == 5


async def test_rate_limit_decorator_with_redis_limiter(limiter) -> None:
    """The @rate_limit decorator should work with RedisRateLimiter."""
    from varco_core.resilience.rate_limit import RateLimitExceededError, rate_limit

    call_count = 0

    @rate_limit(
        limiter=limiter,
        key_fn=lambda: "decorated-key",
    )
    async def handler() -> int:
        nonlocal call_count
        call_count += 1
        return call_count

    # 5 calls should succeed (rate=5).
    for _ in range(5):
        await handler()

    # 6th call should raise.
    with pytest.raises(RateLimitExceededError) as exc_info:
        await handler()

    assert exc_info.value.key == "decorated-key"


async def test_repr_shows_connection_state(redis_container) -> None:
    from varco_core.resilience.rate_limit import RateLimitConfig
    from varco_redis.config import RedisEventBusSettings
    from varco_redis.rate_limit import RedisRateLimiter

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    url = f"redis://{host}:{port}/0"

    lim = RedisRateLimiter(
        RateLimitConfig(rate=5, period=1.0), settings=RedisEventBusSettings(url=url)
    )
    assert "connected=False" in repr(lim)
    await lim.connect()
    assert "connected=True" in repr(lim)
    await lim.disconnect()
    assert "connected=False" in repr(lim)
