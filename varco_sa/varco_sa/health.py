"""
varco_sa.health
===============
Health probes for the SQLAlchemy async backend.

Two probes are provided:

``SAHealthCheck``
    Liveness probe — borrows one connection from the engine pool, executes
    ``SELECT 1``, and releases the connection.  Tests that the database is
    reachable.  It does NOT create a throw-away engine — unlike the Kafka/Redis
    probes which create independent short-lived connections — because
    ``AsyncEngine`` creation requires sync imports and configuration that is
    already owned by the caller.  The probe borrows one connection from the
    existing pool for the duration of the check, which is the standard
    SQLAlchemy health-check pattern.

``SAPoolSaturationCheck``
    Pool-saturation readiness probe — takes a *synchronous* snapshot of the
    pool's counters via ``pool_metrics(engine)`` and returns ``DEGRADED`` when
    all connections are checked out.  This does NOT test database connectivity;
    it is purely about pool utilisation.  ``DEGRADED`` means the database is
    reachable but new requests may queue or fail waiting for a free slot.

DESIGN: borrow engine connection over throw-away engine (SAHealthCheck)
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

DESIGN: sync pool_metrics snapshot in async check() (SAPoolSaturationCheck)
    ✅ ``pool_metrics()`` reads Pool attributes that are updated synchronously
       by SQLAlchemy's pool implementation — no I/O needed, always available.
    ✅ Taking the snapshot inside an async method does NOT block the event loop
       because no I/O or slow Python is involved; it is a handful of attribute reads.
    ❌ The snapshot is a point-in-time view — the pool state may change between
       the read and any action taken on the result.  Acceptable: health checks
       are advisory and sampled.
    Alternative: ``asyncio.get_event_loop().run_in_executor`` — rejected as
    unnecessary overhead for attribute reads that complete in nanoseconds.

Thread safety:  ✅ Engine is thread-safe; connections are per-coroutine.
Async safety:   ✅ check() is async def; all I/O uses await; pool snapshot is
                   a read-only attribute access with no blocking I/O.

📚 Docs
- 🔍 https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html
  SQLAlchemy Async — AsyncEngine.connect(), conn.execute()
- 🔍 https://docs.sqlalchemy.org/en/20/core/pooling.html#pool-events
  SQLAlchemy Pool — checkedout(), checkedin(), overflow(), size()
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for
  asyncio.wait_for — timeout wrapper
- 🐍 https://docs.python.org/3/library/time.html#time.monotonic
  time.monotonic — monotonic clock for latency measurement
"""

from __future__ import annotations

import asyncio
import time
from typing import Annotated

from providify import InjectMeta, Singleton
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from varco_core.health import HealthCheck, HealthResult, HealthStatus

from varco_sa.config import SAConfig
from varco_sa.pool_metrics import pool_metrics

# ── SAHealthCheck ─────────────────────────────────────────────────────────────


# collection contribution — never @Fallback (it would vanish from
# get_all(HealthCheck) once an app adds its own check)
@Singleton(qualifier="sa")
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
        config: Annotated[SAConfig | None, InjectMeta(optional=True)] = None,
        *,
        engine: AsyncEngine | None = None,
        timeout: float = 5.0,
    ) -> None:
        """
        Initialise the SQLAlchemy health probe.

        Args:
            config:  Injected ``SAConfig`` — the probe uses ``config.engine``
                     to target the same database as the repository provider.
                     Used by the DI container.
            engine:  Legacy keyword arg — explicit ``AsyncEngine`` for direct
                     construction (tests, non-DI usage).
            timeout: Probe timeout in seconds.

        DESIGN: dual-path constructor matches ``SQLAlchemyRepositoryProvider``
            ✅ Backward-compatible — tests that pass ``engine=...`` keep working.
            ✅ DI path uses ``Inject[SAConfig]`` — single clean injection point.
            ❌ Two code paths — accepted to avoid breaking the public API.

        Raises:
            TypeError: Neither ``config`` nor ``engine`` is provided.
        """
        if config is not None:
            self._engine: AsyncEngine = config.engine
        elif engine is not None:
            self._engine = engine
        else:
            raise TypeError(
                "SAHealthCheck requires either a ``SAConfig`` injected via DI "
                "or an explicit ``engine`` keyword argument for direct construction."
            )
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
        except TimeoutError:
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
        return f"SAHealthCheck(engine={self._engine!r}, timeout={self._timeout})"


# ── SAPoolSaturationCheck ─────────────────────────────────────────────────────


# collection contribution — never @Fallback (it would vanish from
# get_all(HealthCheck) once an app adds its own check)
@Singleton(qualifier="sa-pool")
class SAPoolSaturationCheck(HealthCheck):
    """
    Pool-saturation readiness probe for a SQLAlchemy async backend.

    Takes a synchronous point-in-time snapshot of the pool's connection
    counters and returns ``DEGRADED`` when all connections (including overflow)
    are checked out.  It does NOT test database connectivity — that is
    ``SAHealthCheck``'s responsibility.

    A ``DEGRADED`` result means:
    - The database itself is reachable.
    - The connection pool is fully exhausted.
    - New requests will queue (if ``pool_timeout > 0``) or fail immediately
      (if ``pool_timeout = 0``) waiting for a free connection slot.

    Attributes:
        engine: The shared ``AsyncEngine`` whose pool is inspected.

    Thread safety:  ✅ Pool attribute reads are thread-safe per SQLAlchemy's
                       pool implementation; no locking required in this probe.
    Async safety:   ✅ check() is async def.  The pool snapshot is a set of
                       synchronous attribute reads — no I/O, no event-loop
                       blocking.

    Edge cases:
        - check() NEVER raises — all exceptions are returned as UNHEALTHY.
        - On ``NullPool`` or ``StaticPool`` (common in tests), ``pool_metrics``
          returns an all-zero snapshot; ``is_saturated`` is always ``False``
          and the result is ``HEALTHY``.
        - The snapshot is point-in-time: pool state may change between the
          read and the caller's decision.  This is acceptable for advisory
          health checks.
        - ``detail`` is always populated (even on HEALTHY) to surface pool
          utilisation to operators in health dashboards.
    """

    def __init__(
        self,
        config: Annotated[SAConfig | None, InjectMeta(optional=True)] = None,
        *,
        engine: AsyncEngine | None = None,
    ) -> None:
        """
        Initialise the pool-saturation probe.

        Args:
            config:  Injected ``SAConfig`` — the probe uses ``config.engine``
                     to target the same pool as the repository provider.
                     Used by the DI container.
            engine:  Explicit ``AsyncEngine`` for direct construction (tests,
                     non-DI usage).

        DESIGN: dual-path constructor mirrors ``SAHealthCheck``
            ✅ Backward-compatible — tests pass ``engine=...`` directly.
            ✅ DI path uses injected ``SAConfig`` — single injection point.
            ❌ Two code paths — accepted to keep the public API consistent
               with the sibling ``SAHealthCheck`` class.

        Raises:
            TypeError: Neither ``config`` nor ``engine`` is provided.

        Edge cases:
            - If both ``config`` and ``engine`` are supplied, ``config`` wins —
              it represents the DI-managed engine and is preferred.
        """
        if config is not None:
            # DI path: config carries the engine already set up with the
            # correct dialect, pool settings, SSL config, etc.
            self._engine: AsyncEngine = config.engine
        elif engine is not None:
            # Direct-construction path: used in tests or non-DI bootstrapping.
            self._engine = engine
        else:
            raise TypeError(
                "SAPoolSaturationCheck requires either a ``SAConfig`` injected via DI "
                "or an explicit ``engine`` keyword argument for direct construction."
            )

    @property
    def name(self) -> str:
        """Human-readable component name used in HealthResult.component.

        Returns:
            The fixed string ``"sqlalchemy-pool"``.
        """
        # Use a distinct name from SAHealthCheck ("sqlalchemy") so that
        # aggregated /health endpoints can distinguish connectivity failures
        # from pool-saturation degradations without parsing detail strings.
        return "sqlalchemy-pool"

    async def check(self) -> HealthResult:
        """
        Snapshot the pool's counters and return DEGRADED if fully saturated.

        The pool snapshot is a synchronous attribute read — no I/O is
        performed and the event loop is never blocked.

        Returns:
            HealthResult(HEALTHY, detail=...) when checked_out < total capacity.
            HealthResult(DEGRADED, detail=...) when checked_out >= total capacity
                (all connections are in active use).
            HealthResult(UNHEALTHY, detail=...) if an unexpected exception occurs
                while reading pool attributes — this should never happen in
                practice but the never-raise contract must be honoured.

        Edge cases:
            - Never raises — all exceptions are caught and returned as
              UNHEALTHY results.
            - NullPool / StaticPool (tests): all counters are 0, result is
              HEALTHY because ``is_saturated`` is False when checked_out == 0.
            - ``detail`` is always set so operators can see utilisation even
              when the status is HEALTHY.
        """
        try:
            # pool_metrics() is synchronous — reads Pool.checkedout(), .size(),
            # .overflow() etc. directly.  Safe to call from an async context.
            m = pool_metrics(self._engine)
            # is_saturated is a proper bool: checked_out >= size + max_overflow.
            # Never use the float saturation_ratio here — a pool at 99.9%
            # utilisation is still not saturated; only >= 100% is DEGRADED.
            status = HealthStatus.DEGRADED if m.is_saturated else HealthStatus.HEALTHY
            total = m.size + m.max_overflow
            return HealthResult(
                status=status,
                component=self.name,
                # Include both checked_out and total so operators can see
                # absolute utilisation numbers, not just the boolean threshold.
                detail=f"checked_out={m.checked_out}/{total}",
            )
        except Exception as exc:  # noqa: BLE001 — never-raise contract
            # Attribute reads on the pool should never fail, but guard anyway
            # so a misconfigured pool never crashes the aggregated health endpoint.
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                component=self.name,
                detail=str(exc),
            )

    def __repr__(self) -> str:
        return f"SAPoolSaturationCheck(engine={self._engine!r})"


__all__ = ["SAHealthCheck", "SAPoolSaturationCheck"]
