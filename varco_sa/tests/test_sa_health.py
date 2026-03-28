"""
Unit tests for varco_sa.health.SAHealthCheck
=============================================
Uses an in-memory SQLite engine (no mock) for the "healthy" path, and
monkeypatching for error/timeout scenarios.  No external database required
for unit tests.

Sections
--------
- Healthy probe     — SELECT 1 on real SQLite → HEALTHY with latency
- Timeout           — _probe hangs → wait_for fires → UNHEALTHY
- DB error          — engine raises OperationalError → UNHEALTHY with detail
- Never-raise       — exceptions never propagate to the caller
- Repr              — human-readable string for logging
- Integration       — real PostgreSQL via testcontainers (marked integration)
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.ext.asyncio import create_async_engine

from varco_core.health import HealthStatus
from varco_sa.health import SAHealthCheck


# ── Helpers ───────────────────────────────────────────────────────────────────


@pytest.fixture
async def sqlite_engine():
    """
    Yield a real in-memory SQLite async engine and dispose it afterwards.

    Using a real SQLite engine for the healthy-path tests avoids mocking
    SQLAlchemy internals — the test validates the actual SELECT 1 path.
    """
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    yield engine
    await engine.dispose()


# ── Healthy probe ─────────────────────────────────────────────────────────────


async def test_healthy_returns_healthy_status(sqlite_engine) -> None:
    check = SAHealthCheck(engine=sqlite_engine)
    result = await check.check()
    assert result.status is HealthStatus.HEALTHY


async def test_healthy_returns_latency(sqlite_engine) -> None:
    check = SAHealthCheck(engine=sqlite_engine)
    result = await check.check()
    assert result.latency_ms is not None
    assert result.latency_ms >= 0.0


async def test_healthy_component_name(sqlite_engine) -> None:
    check = SAHealthCheck(engine=sqlite_engine)
    result = await check.check()
    assert result.component == "sqlalchemy"


async def test_healthy_no_detail(sqlite_engine) -> None:
    # A healthy result should not carry a detail string — cleaner logs.
    check = SAHealthCheck(engine=sqlite_engine)
    result = await check.check()
    assert result.detail is None


# ── Timeout ───────────────────────────────────────────────────────────────────


async def test_timeout_returns_unhealthy(sqlite_engine) -> None:
    # Replace _probe with a coroutine that hangs, then set timeout=0.001 so
    # wait_for fires immediately.
    async def _hang() -> None:
        await asyncio.sleep(999)

    check = SAHealthCheck(engine=sqlite_engine, timeout=0.001)
    with patch.object(check, "_probe", new=_hang):
        result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY
    assert result.latency_ms is None
    assert "timed out" in (result.detail or "")


# ── DB error ──────────────────────────────────────────────────────────────────


async def test_db_error_returns_unhealthy(sqlite_engine) -> None:
    from sqlalchemy.exc import OperationalError

    check = SAHealthCheck(engine=sqlite_engine)
    with patch.object(
        check,
        "_probe",
        new=AsyncMock(side_effect=OperationalError("no such table", {}, None)),
    ):
        result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY
    assert result.detail is not None


async def test_generic_exception_returns_unhealthy(sqlite_engine) -> None:
    check = SAHealthCheck(engine=sqlite_engine)
    with patch.object(check, "_probe", new=AsyncMock(side_effect=RuntimeError("bang"))):
        result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY


# ── Never-raise contract ──────────────────────────────────────────────────────


async def test_check_never_raises(sqlite_engine) -> None:
    check = SAHealthCheck(engine=sqlite_engine)
    with patch.object(check, "_probe", new=AsyncMock(side_effect=Exception("boom"))):
        # Must not propagate
        result = await check.check()

    assert result.status is HealthStatus.UNHEALTHY


# ── Repr ──────────────────────────────────────────────────────────────────────


async def test_repr_contains_timeout(sqlite_engine) -> None:
    check = SAHealthCheck(engine=sqlite_engine, timeout=3.5)
    assert "3.5" in repr(check)


# ── Integration: real PostgreSQL ──────────────────────────────────────────────


@pytest.mark.integration
async def test_integration_healthy_against_real_db() -> None:
    """
    Requires a running PostgreSQL accessible at DATABASE_URL env var.
    Run with: VARCO_RUN_INTEGRATION=1 pytest -m integration
    """
    import os

    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip("Set VARCO_RUN_INTEGRATION=1 to run integration tests")

    url = os.environ.get(
        "DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost/test"
    )
    engine = create_async_engine(url)
    try:
        check = SAHealthCheck(engine=engine, timeout=5.0)
        result = await check.check()
        assert result.status is HealthStatus.HEALTHY
        assert result.latency_ms is not None
    finally:
        await engine.dispose()
