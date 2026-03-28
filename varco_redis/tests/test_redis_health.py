"""
Unit tests for varco_redis.health.RedisHealthCheck
====================================================
All tests use ``monkeypatch`` / ``AsyncMock`` to replace the Redis PING call —
no real Redis connection is required.

Sections
--------
- Healthy probe     — PING succeeds → HEALTHY with latency
- Timeout           — asyncio.wait_for raises TimeoutError → UNHEALTHY
- Connection error  — redis raises on ping → UNHEALTHY with detail
- Never-raise       — exceptions never propagate to caller
- Cleanup           — client.aclose() always called, even on failure
- Repr              — human-readable string for logging
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from varco_core.health import HealthStatus
from varco_redis.health import RedisHealthCheck


# ── Healthy probe ─────────────────────────────────────────────────────────────


async def test_healthy_returns_healthy_status() -> None:
    # The import happens inside check() as `import redis.asyncio as aioredis`.
    # We patch the from_url at the redis.asyncio module level directly.
    mock_client = AsyncMock()
    mock_client.ping = AsyncMock(return_value=True)
    mock_client.aclose = AsyncMock()

    import redis.asyncio as _aioredis

    with patch.object(_aioredis, "from_url", return_value=mock_client):
        check = RedisHealthCheck(url="redis://localhost:6379/0")
        result = await check.check()

    assert result.status is HealthStatus.HEALTHY
    assert result.component == "redis"
    assert result.latency_ms is not None
    assert result.latency_ms >= 0.0


async def test_healthy_calls_ping() -> None:
    mock_client = AsyncMock()
    mock_client.ping = AsyncMock(return_value=True)
    mock_client.aclose = AsyncMock()

    import redis.asyncio as _aioredis

    with patch.object(_aioredis, "from_url", return_value=mock_client):
        check = RedisHealthCheck(url="redis://localhost:6379/0")
        await check.check()

    mock_client.ping.assert_awaited_once()


# ── Timeout ───────────────────────────────────────────────────────────────────


async def test_timeout_returns_unhealthy() -> None:
    # Use a real coroutine that hangs; set timeout=0.001 so wait_for fires.
    async def _hang() -> None:
        await asyncio.sleep(999)

    mock_client = AsyncMock()
    mock_client.ping = _hang
    mock_client.aclose = AsyncMock()

    import redis.asyncio as _aioredis

    with patch.object(_aioredis, "from_url", return_value=mock_client):
        check = RedisHealthCheck(url="redis://localhost:6379/0", timeout=0.001)
        result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY
    assert result.latency_ms is None
    assert "timed out" in (result.detail or "")


# ── Connection error ──────────────────────────────────────────────────────────


async def test_connection_error_returns_unhealthy() -> None:
    mock_client = AsyncMock()
    mock_client.ping = AsyncMock(side_effect=ConnectionRefusedError("refused"))
    mock_client.aclose = AsyncMock()

    import redis.asyncio as _aioredis

    with patch.object(_aioredis, "from_url", return_value=mock_client):
        check = RedisHealthCheck(url="redis://localhost:6379/0")
        result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY
    assert "refused" in (result.detail or "")


async def test_generic_exception_returns_unhealthy() -> None:
    mock_client = AsyncMock()
    mock_client.ping = AsyncMock(side_effect=RuntimeError("unexpected"))
    mock_client.aclose = AsyncMock()

    import redis.asyncio as _aioredis

    with patch.object(_aioredis, "from_url", return_value=mock_client):
        check = RedisHealthCheck(url="redis://localhost:6379/0")
        result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY


# ── Never-raise contract ──────────────────────────────────────────────────────


async def test_check_never_raises_on_error() -> None:
    # Even if from_url itself raises, check() must not propagate it.
    import redis.asyncio as _aioredis

    with patch.object(_aioredis, "from_url", side_effect=OSError("no route")):
        check = RedisHealthCheck(url="redis://localhost:6379/0")
        result = await check.check()  # must not raise

    assert result.status is HealthStatus.UNHEALTHY


# ── Cleanup ───────────────────────────────────────────────────────────────────


async def test_aclose_called_on_success() -> None:
    mock_client = AsyncMock()
    mock_client.ping = AsyncMock(return_value=True)
    mock_client.aclose = AsyncMock()

    import redis.asyncio as _aioredis

    with patch.object(_aioredis, "from_url", return_value=mock_client):
        check = RedisHealthCheck(url="redis://localhost:6379/0")
        await check.check()

    mock_client.aclose.assert_awaited_once()


async def test_aclose_called_on_error() -> None:
    mock_client = AsyncMock()
    mock_client.ping = AsyncMock(side_effect=ConnectionRefusedError())
    mock_client.aclose = AsyncMock()

    import redis.asyncio as _aioredis

    with patch.object(_aioredis, "from_url", return_value=mock_client):
        check = RedisHealthCheck(url="redis://localhost:6379/0")
        await check.check()

    mock_client.aclose.assert_awaited_once()


# ── Repr ──────────────────────────────────────────────────────────────────────


def test_repr_contains_url() -> None:
    check = RedisHealthCheck(url="redis://myhost:6379/1", timeout=3.0)
    text = repr(check)
    assert "myhost" in text
    assert "3.0" in text


# ── Integration: real Redis PING ──────────────────────────────────────────────


@pytest.mark.integration
async def test_integration_healthy_against_real_redis() -> None:
    """
    Requires a running Redis on localhost:6379.
    Run with: VARCO_RUN_INTEGRATION=1 pytest -m integration
    """
    import os

    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip("Set VARCO_RUN_INTEGRATION=1 to run integration tests")

    check = RedisHealthCheck(url="redis://localhost:6379/0", timeout=5.0)
    result = await check.check()
    assert result.status is HealthStatus.HEALTHY
    assert result.latency_ms is not None and result.latency_ms >= 0.0
