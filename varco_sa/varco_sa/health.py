"""
varco_sa.health
===============
Liveness probe for the SQLAlchemy async backend.

``SAHealthCheck`` borrows the injected ``AsyncEngine``, acquires a connection
from the pool, executes ``SELECT 1``, and releases the connection.  It does NOT
create a throw-away engine — unlike the Kafka/Redis probes which create
independent short-lived connections — because ``AsyncEngine`` creation requires
sync imports and configuration that is already owned by the caller.  Instead,
the probe borrows one connection from the existing pool for the duration of the
check, which is the standard SQLAlchemy health-check pattern.

DESIGN: borrow engine connection over throw-away engine
    ✅ No duplicate engine/pool creation — reuses the one already configured
       with SSL, pool size, auth, etc.  Throwing it away would recreate all that.
    ✅ ``SELECT 1`` via ``conn.execute`` is the canonical SQLAlchemy aliveness test —
       used by Alembic's ``env.py`` template and all major frameworks.
    ❌ A pool exhaustion scenario could cause ``check()`` to hang waiting for a
       connection slot — mitigated by ``asyncio.wait_for`` with the configured
       timeout.
    Alternative: ``engine.connect()`` with ``pool_timeout`` — rejected because
    pool_timeout is not available on all dialect/engine variants; wait_for is
    more universal.

Thread safety:  ✅ Engine is thread-safe; connections are per-coroutine.
Async safety:   ✅ check() is async def; all I/O uses await.

📚 Docs
- 🔍 https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
  SQLAlchemy Async — AsyncEngine.connect(), conn.execute()
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for
  asyncio.wait_for — timeout wrapper
- 🐍 https://docs.python.org/3/library/time.html#time.monotonic
  time.monotonic — monotonic clock for latency measurement
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from sqlalchemy import text

from varco_core.health import HealthCheck, HealthResult, HealthStatus

if TYPE_CHECKING:
    # AsyncEngine is only needed for type hints — avoid the import in test
    # environments that mock the engine.
    from sqlalchemy.ext.asyncio import AsyncEngine


# ── SAHealthCheck ─────────────────────────────────────────────────────────────


class SAHealthCheck(HealthCheck):
    """
    Liveness probe for a SQLAlchemy-backed database.

    Borrows one connection from the engine's pool, executes ``SELECT 1``,
    and releases the connection.  Tests that the database is reachable and
    the connection pool is functional.

    Attributes:
        engine:  The shared ``AsyncEngine`` to probe.
        timeout: Seconds before the probe is abandoned.  Default 5 s.

    Thread safety:  ✅ Engine is thread-safe; each check() call uses its
                       own borrowed connection.
    Async safety:   ✅ check() is async def; all I/O uses await.

    Edge cases:
        - check() NEVER raises — exceptions are returned as UNHEALTHY results.
        - If the engine pool is exhausted, wait_for will cancel the acquire
          attempt after the configured timeout and return UNHEALTHY.
        - ``SELECT 1`` works on PostgreSQL, MySQL, SQLite, and most others.
          For databases that don't support it (e.g. Oracle uses ``SELECT 1
          FROM DUAL``), subclass and override check() directly.
    """

    def __init__(
        self,
        engine: AsyncEngine,
        *,
        timeout: float = 5.0,
    ) -> None:
        """
        Initialise the SQLAlchemy health probe.

        Args:
            engine:  The ``AsyncEngine`` to probe — typically the same engine
                     registered in ``SAConfig`` and injected into ``SAModule``.
            timeout: Probe timeout in seconds.
        """
        self._engine = engine
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "sqlalchemy"

    async def _probe(self) -> None:
        """
        Execute ``SELECT 1`` against the engine.

        Isolated into its own method so ``asyncio.wait_for`` can cancel it
        cleanly — the connection context manager's ``__aexit__`` runs even
        when the coroutine is cancelled.

        Raises:
            Any exception from the DB driver propagates directly to check().
        """
        # async with engine.connect() borrows one connection from the pool
        # and releases it on exit — does NOT create a new connection if the
        # pool has an idle one available.
        async with self._engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    async def check(self) -> HealthResult:
        """
        Probe database connectivity via ``SELECT 1``.

        Returns:
            HealthResult(HEALTHY, latency_ms) on success.
            HealthResult(UNHEALTHY, detail) on timeout or connection error.
            Never raises.
        """
        start = time.monotonic()

        try:
            await asyncio.wait_for(self._probe(), timeout=self._timeout)
            latency_ms = (time.monotonic() - start) * 1000
            return HealthResult(
                status=HealthStatus.HEALTHY,
                component=self.name,
                latency_ms=latency_ms,
            )
        except asyncio.TimeoutError:
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                component=self.name,
                detail=f"timed out after {self._timeout}s waiting for database connection",
            )
        except Exception as exc:  # noqa: BLE001 — intentionally broad: never raise
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                component=self.name,
                detail=str(exc),
            )

    def __repr__(self) -> str:
        return (
            f"SAHealthCheck(" f"engine={self._engine!r}, " f"timeout={self._timeout})"
        )


__all__ = ["SAHealthCheck"]
