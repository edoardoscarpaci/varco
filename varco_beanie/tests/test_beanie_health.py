"""
Unit tests for varco_beanie.health.BeanieHealthCheck
======================================================
Uses ``AsyncMock`` to replace ``client.server_info()`` — no real MongoDB
connection required.

Sections
--------
- Healthy probe     — server_info succeeds → HEALTHY with latency
- Timeout           — wait_for fires → UNHEALTHY with detail
- Connection error  — server_info raises → UNHEALTHY with detail
- Never-raise       — exceptions never propagate to caller
- Repr              — human-readable string for logging
- Integration       — real MongoDB via testcontainers (marked integration)
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from varco_core.health import HealthStatus
from varco_beanie.health import BeanieHealthCheck


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_client(*, side_effect=None, return_value=None) -> MagicMock:
    """
    Return a mock AsyncMongoClient whose server_info() is an AsyncMock.

    Args:
        side_effect:  If set, server_info() raises this exception.
        return_value: Return value of server_info() on success.
    """
    client = MagicMock()
    if side_effect is not None:
        client.server_info = AsyncMock(side_effect=side_effect)
    else:
        client.server_info = AsyncMock(return_value=return_value or {"version": "6.0"})
    return client


# ── Healthy probe ─────────────────────────────────────────────────────────────


async def test_healthy_returns_healthy_status() -> None:
    check = BeanieHealthCheck(client=_make_client())
    result = await check.check()
    assert result.status is HealthStatus.HEALTHY


async def test_healthy_component_name() -> None:
    check = BeanieHealthCheck(client=_make_client())
    result = await check.check()
    assert result.component == "mongodb"


async def test_healthy_has_latency() -> None:
    check = BeanieHealthCheck(client=_make_client())
    result = await check.check()
    assert result.latency_ms is not None
    assert result.latency_ms >= 0.0


async def test_healthy_no_detail() -> None:
    check = BeanieHealthCheck(client=_make_client())
    result = await check.check()
    assert result.detail is None


# ── Timeout ───────────────────────────────────────────────────────────────────


async def test_timeout_returns_unhealthy() -> None:
    async def _hang():
        await asyncio.sleep(999)

    client = MagicMock()
    client.server_info = _hang
    check = BeanieHealthCheck(client=client, timeout=0.001)
    result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY
    assert result.latency_ms is None
    assert "timed out" in (result.detail or "")


# ── Connection error ──────────────────────────────────────────────────────────


async def test_connection_error_returns_unhealthy() -> None:
    check = BeanieHealthCheck(
        client=_make_client(side_effect=ConnectionRefusedError("no server"))
    )
    result = await check.check()
    assert result.status is HealthStatus.UNHEALTHY
    assert "no server" in (result.detail or "")


async def test_server_selection_timeout_returns_unhealthy() -> None:
    # pymongo raises ServerSelectionTimeoutError when no primary is available.
    # Simulate with a generic timeout-like exception.
    check = BeanieHealthCheck(
        client=_make_client(side_effect=RuntimeError("no primary available"))
    )
    result = await check.check()
    assert result.status is HealthStatus.UNHEALTHY


# ── Never-raise contract ──────────────────────────────────────────────────────


async def test_check_never_raises() -> None:
    check = BeanieHealthCheck(client=_make_client(side_effect=Exception("crash")))
    result = await check.check()  # must not raise
    assert result.status is HealthStatus.UNHEALTHY


# ── Repr ──────────────────────────────────────────────────────────────────────


def test_repr_contains_timeout() -> None:
    check = BeanieHealthCheck(client=_make_client(), timeout=2.5)
    assert "2.5" in repr(check)


# ── Integration: real MongoDB ─────────────────────────────────────────────────


@pytest.mark.integration
async def test_integration_healthy_against_real_mongodb() -> None:
    """
    Spins up a real MongoDB via testcontainers and verifies the health check
    reports HEALTHY with a non-negative latency.
    Run with: VARCO_RUN_INTEGRATION=1 pytest -m integration
    """
    import os

    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip("Set VARCO_RUN_INTEGRATION=1 to run integration tests")

    from pymongo import AsyncMongoClient
    from testcontainers.mongodb import MongoDbContainer

    # Use testcontainers so the test is self-contained — no pre-running MongoDB needed.
    with MongoDbContainer() as mongo:
        url = mongo.get_connection_url()
        client: AsyncMongoClient = AsyncMongoClient(url)
        try:
            check = BeanieHealthCheck(client=client, timeout=5.0)
            result = await check.check()
        finally:
            await client.aclose()
    assert result.status is HealthStatus.HEALTHY
    assert result.latency_ms is not None
