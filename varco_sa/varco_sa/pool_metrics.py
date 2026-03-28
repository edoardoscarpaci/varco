"""
varco_sa.pool_metrics
=====================
SQLAlchemy connection pool metrics for observability and health checking.

Problem
-------
A saturated connection pool silently queues requests instead of failing fast.
Without surfacing pool stats, operators cannot distinguish a slow query from a
pool saturation event — the symptom looks the same (high P99 latency) until
the queue overflows and requests start timing out.

Solution
--------
``SAPoolMetrics`` is a frozen snapshot of the pool's current counters.
``pool_metrics(engine)`` reads those counters from the engine at call time.
``SAFastrestApp.pool_metrics()`` delegates to ``pool_metrics(self.config.engine)``.

DESIGN: frozen dataclass snapshot over a live property
    ✅ Snapshots are serializable and loggable — pass them to health checks,
       Prometheus exporters, or log aggregators without capturing a reference
       to the engine.
    ✅ Callers know the snapshot age — ``captured_at`` timestamps when the
       stats were read.
    ✅ Frozen — cannot accidentally mutate a stats object after collection.
    ❌ Snapshot is stale by the time the caller reads it — acceptable for
       monitoring (1–5 s scrape intervals) but not for real-time admission
       control.

Saturation check
----------------
``SAPoolMetrics.is_saturated`` returns ``True`` when all pool connections are
checked-out AND the overflow limit is reached.  This is the point at which new
``connect()`` calls will block (or fail if ``pool_timeout`` is set).

Usage::

    from varco_sa.pool_metrics import pool_metrics

    metrics = pool_metrics(engine)
    print(metrics.checked_out, metrics.size, metrics.is_saturated)

    # Or via SAFastrestApp:
    app = SAFastrestApp(config)
    metrics = app.pool_metrics()

Thread safety:  ✅ Reads pool stats in a single synchronous read — no locking.
Async safety:   ✅ ``pool_metrics()`` is synchronous; safe to call from async.

📚 Docs
- 🔍 https://docs.sqlalchemy.org/en/20/core/pooling.html#connection-pool-events
  SQLAlchemy pool — status() and pool statistics documentation.
- 🔍 https://docs.sqlalchemy.org/en/20/core/connections.html#sqlalchemy.engine.Engine.pool
  AsyncEngine.sync_engine.pool — how to access pool stats.
- 📐 https://docs.sqlalchemy.org/en/20/dialects/postgresql.html
  PostgreSQL asyncpg driver — pool configuration options.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import NullPool, StaticPool

_logger = logging.getLogger(__name__)


# ── SAPoolMetrics ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SAPoolMetrics:
    """
    Immutable snapshot of SQLAlchemy connection pool statistics.

    All counters are read at ``captured_at`` — the snapshot is immediately
    stale.  Use for monitoring and alerting, not for admission control.

    DESIGN: frozen dataclass snapshot over a live view
        ✅ Serializable — can be logged, sent to Prometheus, stored in DB.
        ✅ Timestamped — callers know how old the data is.
        ✅ Immutable — safe to pass across async boundaries without copying.
        ❌ Stale by construction — pool state changes between reads.

    Thread safety:  ✅ Frozen — immutable value object.
    Async safety:   ✅ No I/O; pure value object.

    Attributes:
        size:         Pool size configured at engine creation (``pool_size``).
                      This is the number of persistent connections.
        checked_out:  Number of connections currently held by active sessions.
        checked_in:   Number of connections currently idle in the pool.
        overflow:     Number of overflow connections (beyond ``pool_size``)
                      currently checked out.  Non-zero means the pool is under
                      pressure.
        max_overflow: Maximum allowed overflow connections (``max_overflow``).
        invalid:      Number of connections marked invalid (checked back in as
                      broken and awaiting reconnection).
        captured_at:  UTC timestamp when the metrics were read.
        pool_type:    String name of the pool implementation class (e.g.
                      ``"QueuePool"``, ``"NullPool"``, ``"StaticPool"``).

    Edge cases:
        - For ``NullPool`` (no persistent connections) and ``StaticPool``
          (tests), all numeric fields are ``0`` and ``is_saturated`` is
          always ``False``.
        - ``checked_in + checked_out`` may exceed ``size`` when overflow
          connections exist.

    Example::

        metrics = pool_metrics(engine)
        if metrics.is_saturated:
            alert("Pool saturated — new connections will block")
    """

    size: int
    checked_out: int
    checked_in: int
    overflow: int
    max_overflow: int
    invalid: int
    captured_at: datetime
    pool_type: str

    @property
    def is_saturated(self) -> bool:
        """
        Return ``True`` when all connections (including overflow) are in use.

        A saturated pool blocks new ``connect()`` calls (or raises if
        ``pool_timeout=0``).  This is the early-warning signal before
        request queuing starts.

        Returns:
            ``True`` if ``checked_out >= size + max_overflow`` AND
            ``overflow >= max_overflow``.

        Edge cases:
            - ``NullPool`` / ``StaticPool``: always returns ``False`` (size=0,
              max_overflow=0 — the formula evaluates as 0 >= 0, but those pools
              are never "saturated" in the queue-blocking sense).
            - Negative ``max_overflow`` means unlimited overflow — can never
              be saturated.  Returns ``False`` in that case.
        """
        if self.max_overflow < 0:
            # Unlimited overflow — pool can never saturate via overflow.
            return False
        if self.size == 0 and self.max_overflow == 0:
            # Zero-capacity pool (NullPool / StaticPool) — never "saturated"
            # in the queue-blocking sense; these pools have no connection queue.
            return False
        # Saturated when all base + overflow slots are exhausted.
        return self.checked_out >= self.size + self.max_overflow

    @property
    def utilisation(self) -> float:
        """
        Return pool utilisation as a fraction in [0.0, 1.0].

        Computed as ``checked_out / (size + max_overflow)`` where the denominator
        is the total available slots (including overflow).  ``0.0`` means idle;
        ``1.0`` means saturated.

        Returns:
            Utilisation fraction, or ``0.0`` for pools with no finite capacity.

        Edge cases:
            - ``size + max_overflow == 0`` (e.g. ``NullPool``) → returns 0.0.
            - ``max_overflow < 0`` (unlimited) → returns ``checked_out / size``
              capped at 1.0.
        """
        if self.max_overflow < 0:
            # Unlimited overflow — measure against base size only.
            total = self.size
        else:
            total = self.size + self.max_overflow

        if total == 0:
            return 0.0
        return min(1.0, self.checked_out / total)

    def __repr__(self) -> str:
        return (
            f"SAPoolMetrics("
            f"size={self.size}, "
            f"checked_out={self.checked_out}, "
            f"overflow={self.overflow}, "
            f"utilisation={self.utilisation:.0%}, "
            f"saturated={self.is_saturated})"
        )


# ── pool_metrics() ────────────────────────────────────────────────────────────


def pool_metrics(engine: AsyncEngine) -> SAPoolMetrics:
    """
    Read the current connection pool statistics from an ``AsyncEngine``.

    This function is synchronous — it reads pool attributes directly without
    I/O.  Safe to call from async code.

    Args:
        engine: The ``AsyncEngine`` whose pool to inspect.  Created by
                ``create_async_engine(url, pool_size=N, max_overflow=M)``.

    Returns:
        An ``SAPoolMetrics`` snapshot with all counters and a ``captured_at``
        UTC timestamp.

    Thread safety:  ✅ Single-pass attribute reads — no mutations.
    Async safety:   ✅ Synchronous; safe to call from any async context.

    Edge cases:
        - ``NullPool`` (``pool_size=0``, no persistent connections):
          all counters return 0.  ``is_saturated`` is always False.
        - ``StaticPool`` (used in tests with ``check_same_thread=False``):
          counters return 0.
        - Pool attributes are read without locking — there is a theoretical
          race condition between reads, but the window is sub-microsecond and
          monitoring tools tolerate slight inaccuracies.

    Example::

        engine = create_async_engine("postgresql+asyncpg://...", pool_size=10)
        metrics = pool_metrics(engine)
        print(f"Connections in use: {metrics.checked_out}/{metrics.size}")
    """
    # AsyncEngine wraps a synchronous Engine — pool is on the sync engine.
    # This is the correct way to access pool stats in SQLAlchemy 2.x async.
    sync_engine = engine.sync_engine
    pool = sync_engine.pool

    pool_type = type(pool).__name__

    # NullPool and StaticPool do not expose the standard pool counters.
    # Return zeroed metrics rather than AttributeError.
    if isinstance(pool, (NullPool, StaticPool)):
        _logger.debug(
            "pool_metrics: pool_type=%s has no counters — returning zeroed metrics.",
            pool_type,
        )
        return SAPoolMetrics(
            size=0,
            checked_out=0,
            checked_in=0,
            overflow=0,
            max_overflow=0,
            invalid=0,
            captured_at=datetime.now(timezone.utc),
            pool_type=pool_type,
        )

    # QueuePool (the default) exposes these properties directly.
    # DESIGN: access private attributes via public status API where possible.
    # Pool.status() returns a human-readable string — not useful for metrics.
    # The numeric attributes below are the actual counters.
    try:
        size: int = pool.size()
        checked_in: int = pool.checkedin()
        checked_out: int = pool.checkedout()
        overflow: int = pool.overflow()
        # max_overflow is a constructor argument stored on the pool instance.
        # Access via the private _max_overflow — it is the canonical source.
        max_overflow: int = getattr(pool, "_max_overflow", 0)
        invalid: int = getattr(pool, "_invalidated_count", 0)
    except AttributeError as exc:
        # Unexpected pool type — log and return zeroed metrics rather than crash.
        _logger.warning(
            "pool_metrics: unexpected pool type %r — cannot read counters: %s. "
            "Returning zeroed metrics.",
            pool_type,
            exc,
        )
        return SAPoolMetrics(
            size=0,
            checked_out=0,
            checked_in=0,
            overflow=0,
            max_overflow=0,
            invalid=0,
            captured_at=datetime.now(timezone.utc),
            pool_type=pool_type,
        )

    return SAPoolMetrics(
        size=size,
        checked_out=checked_out,
        checked_in=checked_in,
        overflow=overflow,
        max_overflow=max_overflow,
        invalid=invalid,
        captured_at=datetime.now(timezone.utc),
        pool_type=pool_type,
    )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SAPoolMetrics",
    "pool_metrics",
]
