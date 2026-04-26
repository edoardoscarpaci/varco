"""
Unit tests for varco_memcached.health.MemcachedHealthCheck
===========================================================
All tests mock ``aiomcache.Client`` — no real Memcached instance required.

The probe issues a ``stats()`` command (Memcached's canonical health-check
command, since Memcached has no PING equivalent).

Sections
--------
- Healthy probe     — stats() succeeds → HEALTHY with latency
- Timeout           — asyncio.wait_for raises TimeoutError → UNHEALTHY
- Connection error  — aiomcache raises on stats() → UNHEALTHY with detail
- Never-raise       — exceptions never propagate to caller
- Cleanup           — client.close() always called, even on failure
- Repr              — human-readable string for logging
- Integration       — real Memcached via testcontainers (skip by default)
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from varco_core.health import HealthStatus
from varco_memcached.cache import MemcachedCacheSettings
from varco_memcached.health import MemcachedHealthCheck


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_settings(
    host: str = "localhost", port: int = 11211
) -> MemcachedCacheSettings:
    return MemcachedCacheSettings(host=host, port=port)


def _make_mock_client(
    stats_return=None,
    stats_side_effect=None,
) -> MagicMock:
    """Build a mock ``aiomcache.Client`` for unit tests."""
    client = MagicMock()
    if stats_side_effect is not None:
        client.stats = AsyncMock(side_effect=stats_side_effect)
    else:
        client.stats = AsyncMock(return_value=stats_return or {b"version": b"1.6.0"})
    client.close = AsyncMock()
    return client


# ── Healthy probe ──────────────────────────────────────────────────────────────


async def test_healthy_returns_healthy_status() -> None:
    mock_client = _make_mock_client()

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(_make_settings())
        result = await check.check()

    assert result.status is HealthStatus.HEALTHY
    assert result.component == "memcached"
    assert result.latency_ms is not None
    assert result.latency_ms >= 0.0


async def test_healthy_calls_stats() -> None:
    mock_client = _make_mock_client()

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(_make_settings())
        await check.check()

    mock_client.stats.assert_awaited_once()


async def test_healthy_empty_stats_dict_is_still_healthy() -> None:
    """stats() returning {} (idle server) is still HEALTHY — it responded."""
    mock_client = _make_mock_client(stats_return={})

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(_make_settings())
        result = await check.check()

    assert result.status is HealthStatus.HEALTHY


async def test_client_created_with_correct_host_port() -> None:
    mock_client = _make_mock_client()

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client) as mock_cls:
        check = MemcachedHealthCheck(_make_settings(host="mc-host", port=11311))
        await check.check()

    call_kwargs = mock_cls.call_args
    assert call_kwargs[0][0] == "mc-host"
    assert call_kwargs[0][1] == 11311


# ── Timeout ────────────────────────────────────────────────────────────────────


async def test_timeout_returns_unhealthy() -> None:
    async def _hang():
        await asyncio.sleep(999)

    mock_client = MagicMock()
    mock_client.stats = _hang
    mock_client.close = AsyncMock()

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(_make_settings(), timeout=0.001)
        result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY
    assert result.latency_ms is None
    assert "timed out" in (result.detail or "")


async def test_timeout_detail_includes_host_and_port() -> None:
    async def _hang():
        await asyncio.sleep(999)

    mock_client = MagicMock()
    mock_client.stats = _hang
    mock_client.close = AsyncMock()

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(
            _make_settings(host="mc-prod", port=11211), timeout=0.001
        )
        result = await check.check()

    assert "mc-prod" in (result.detail or "")
    assert "11211" in (result.detail or "")


# ── Connection error ───────────────────────────────────────────────────────────


async def test_connection_error_returns_unhealthy() -> None:
    mock_client = _make_mock_client(stats_side_effect=ConnectionRefusedError("refused"))

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(_make_settings())
        result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY
    assert "refused" in (result.detail or "")


async def test_generic_exception_returns_unhealthy() -> None:
    mock_client = _make_mock_client(stats_side_effect=RuntimeError("unexpected error"))

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(_make_settings())
        result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY
    assert "unexpected error" in (result.detail or "")


async def test_component_name_is_memcached_on_error() -> None:
    mock_client = _make_mock_client(stats_side_effect=OSError("no route"))

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(_make_settings())
        result = await check.check()

    assert result.component == "memcached"


# ── Never-raise contract ───────────────────────────────────────────────────────


async def test_check_never_raises_when_client_constructor_raises() -> None:
    """If aiomcache.Client() itself raises, check() must not propagate."""
    import aiomcache

    with patch.object(aiomcache, "Client", side_effect=OSError("no route")):
        check = MemcachedHealthCheck(_make_settings())
        result = await check.check()  # must not raise

    assert result.status is HealthStatus.UNHEALTHY


async def test_check_never_raises_on_generic_error() -> None:
    mock_client = _make_mock_client(stats_side_effect=Exception("boom"))

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(_make_settings())
        result = await check.check()  # must not raise

    assert result.status is HealthStatus.UNHEALTHY


# ── Cleanup ────────────────────────────────────────────────────────────────────


async def test_close_called_on_success() -> None:
    mock_client = _make_mock_client()

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(_make_settings())
        await check.check()

    mock_client.close.assert_awaited_once()


async def test_close_called_on_stats_error() -> None:
    mock_client = _make_mock_client(stats_side_effect=ConnectionRefusedError())

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(_make_settings())
        await check.check()

    mock_client.close.assert_awaited_once()


async def test_close_called_on_timeout() -> None:
    async def _hang():
        await asyncio.sleep(999)

    mock_client = MagicMock()
    mock_client.stats = _hang
    mock_client.close = AsyncMock()

    import aiomcache

    with patch.object(aiomcache, "Client", return_value=mock_client):
        check = MemcachedHealthCheck(_make_settings(), timeout=0.001)
        await check.check()

    mock_client.close.assert_awaited_once()


async def test_close_not_called_if_client_constructor_raises() -> None:
    """If aiomcache.Client() raises, no client object exists — close() cannot run."""
    import aiomcache

    with patch.object(aiomcache, "Client", side_effect=OSError("no route")):
        check = MemcachedHealthCheck(_make_settings())
        # Must not raise even though close() cannot be called.
        result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY


# ── Repr ───────────────────────────────────────────────────────────────────────


def test_repr_contains_host_and_port() -> None:
    check = MemcachedHealthCheck(
        _make_settings(host="mc-server", port=11311), timeout=3.0
    )
    text = repr(check)
    assert "mc-server" in text
    assert "11311" in text
    assert "3.0" in text


def test_repr_contains_class_name() -> None:
    check = MemcachedHealthCheck(_make_settings())
    assert "MemcachedHealthCheck" in repr(check)


def test_name_property() -> None:
    check = MemcachedHealthCheck(_make_settings())
    assert check.name == "memcached"


# ── Integration: real Memcached stats ─────────────────────────────────────────


@pytest.mark.integration
async def test_integration_healthy_against_real_memcached() -> None:
    """
    Spins up a real Memcached via testcontainers and verifies the health check
    reports HEALTHY with a non-negative latency.
    Run with: VARCO_RUN_INTEGRATION=1 pytest -m integration
    """
    import os

    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip("Set VARCO_RUN_INTEGRATION=1 to run integration tests")

    from testcontainers.memcached import MemcachedContainer

    with MemcachedContainer() as mc:
        host = mc.get_container_host_ip()
        port = int(mc.get_exposed_port(11211))
        settings = MemcachedCacheSettings(host=host, port=port)
        check = MemcachedHealthCheck(settings, timeout=5.0)
        result = await check.check()

    assert result.status is HealthStatus.HEALTHY
    assert result.latency_ms is not None and result.latency_ms >= 0.0
