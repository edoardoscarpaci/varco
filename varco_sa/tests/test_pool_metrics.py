"""
tests.test_pool_metrics
========================
Unit tests for varco_sa.pool_metrics.

Covers:
    SAPoolMetrics       — construction, is_saturated, utilisation, repr
    pool_metrics()      — reads engine pool stats (StaticPool for tests)
    SAFastrestApp       — pool_metrics() delegate method

Uses SQLite in-memory with StaticPool — no real DB connection required.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase

from varco_sa.pool_metrics import SAPoolMetrics, pool_metrics


# ── SAPoolMetrics ──────────────────────────────────────────────────────────────


def _make_metrics(**kwargs) -> SAPoolMetrics:
    """Build a SAPoolMetrics with sensible defaults for testing."""
    defaults = dict(
        size=10,
        checked_out=0,
        checked_in=10,
        overflow=0,
        max_overflow=5,
        invalid=0,
        captured_at=datetime.now(timezone.utc),
        pool_type="QueuePool",
    )
    defaults.update(kwargs)
    return SAPoolMetrics(**defaults)


def test_pool_metrics_construction() -> None:
    """SAPoolMetrics can be constructed with all fields."""
    m = _make_metrics()
    assert m.size == 10
    assert m.pool_type == "QueuePool"


def test_pool_metrics_is_frozen() -> None:
    """SAPoolMetrics is immutable — mutation raises."""
    m = _make_metrics()
    with pytest.raises(Exception):
        m.size = 20  # type: ignore[misc]


def test_is_saturated_false_when_idle() -> None:
    """Not saturated when no connections are checked out."""
    m = _make_metrics(checked_out=0)
    assert m.is_saturated is False


def test_is_saturated_false_when_partial() -> None:
    """Not saturated when some connections are in use but capacity remains."""
    m = _make_metrics(size=10, checked_out=8, overflow=0, max_overflow=5)
    assert m.is_saturated is False


def test_is_saturated_true_when_all_slots_exhausted() -> None:
    """Saturated when checked_out == size + max_overflow."""
    m = _make_metrics(size=10, checked_out=15, overflow=5, max_overflow=5)
    assert m.is_saturated is True


def test_is_saturated_false_for_unlimited_overflow() -> None:
    """When max_overflow=-1 (unlimited), is_saturated is always False."""
    m = _make_metrics(size=10, checked_out=100, overflow=90, max_overflow=-1)
    assert m.is_saturated is False


def test_utilisation_zero_when_idle() -> None:
    """Utilisation is 0.0 when nothing is checked out."""
    m = _make_metrics(size=10, checked_out=0, max_overflow=5)
    assert m.utilisation == pytest.approx(0.0)


def test_utilisation_one_when_saturated() -> None:
    """Utilisation is 1.0 when the pool is fully saturated."""
    m = _make_metrics(size=10, checked_out=15, overflow=5, max_overflow=5)
    assert m.utilisation == pytest.approx(1.0)


def test_utilisation_partial() -> None:
    """Utilisation is proportional to checked_out / (size + max_overflow)."""
    # 5 checked out of 15 total (10 base + 5 overflow) = 1/3
    m = _make_metrics(size=10, checked_out=5, overflow=0, max_overflow=5)
    assert m.utilisation == pytest.approx(5 / 15)


def test_utilisation_zero_capacity() -> None:
    """Zero-capacity pool (NullPool) returns 0.0 utilisation."""
    m = _make_metrics(size=0, checked_out=0, max_overflow=0)
    assert m.utilisation == pytest.approx(0.0)


def test_repr_includes_key_fields() -> None:
    """repr includes utilisation and saturation for quick inspection."""
    m = _make_metrics(size=10, checked_out=5, overflow=0, max_overflow=5)
    r = repr(m)
    assert "SAPoolMetrics" in r
    assert "saturated" in r


# ── pool_metrics() with StaticPool ────────────────────────────────────────────


def test_pool_metrics_null_pool() -> None:
    """
    pool_metrics() returns zeroed SAPoolMetrics for NullPool — all counters 0,
    is_saturated always False.
    """
    from sqlalchemy.pool import NullPool

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        poolclass=NullPool,
    )
    m = pool_metrics(engine)
    assert m.size == 0
    assert m.checked_out == 0
    assert m.checked_in == 0
    assert m.overflow == 0
    assert m.is_saturated is False
    assert isinstance(m.captured_at, datetime)


def test_pool_metrics_static_pool() -> None:
    """
    pool_metrics() returns zeroed SAPoolMetrics for StaticPool (used in tests).
    """
    from sqlalchemy.pool import StaticPool

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    m = pool_metrics(engine)
    assert m.size == 0
    assert m.checked_out == 0
    assert m.is_saturated is False


def test_pool_metrics_queue_pool() -> None:
    """
    pool_metrics() returns an SAPoolMetrics snapshot for a standard engine.
    SQLite always uses SingletonThreadPool internally — we just verify the
    counters are numeric and a timestamp is captured.
    """
    # SQLite does not accept pool_size/max_overflow — use default pool.
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    m = pool_metrics(engine)
    # No connections have been checked out — all idle.
    assert isinstance(m.size, int)
    assert isinstance(m.checked_out, int)
    assert m.size >= 0
    assert m.checked_out >= 0
    assert isinstance(m.captured_at, datetime)
    # SQLite always uses SingletonThreadPool — just check it's a non-empty string.
    assert isinstance(m.pool_type, str) and len(m.pool_type) > 0


# ── SAFastrestApp.pool_metrics() ──────────────────────────────────────────────


def test_sa_fastrest_app_pool_metrics_delegate() -> None:
    """
    SAFastrestApp.pool_metrics() delegates to pool_metrics() and returns
    an SAPoolMetrics snapshot.
    """
    from sqlalchemy.pool import StaticPool

    from varco_sa.bootstrap import SAConfig, SAFastrestApp

    class Base(DeclarativeBase):
        pass

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    config = SAConfig(engine=engine, base=Base, entity_classes=())
    app = SAFastrestApp(config)

    metrics = app.pool_metrics()
    assert isinstance(metrics, SAPoolMetrics)
    assert isinstance(metrics.captured_at, datetime)
