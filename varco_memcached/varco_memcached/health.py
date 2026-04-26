"""
varco_memcached.health
======================
Liveness probe for the Memcached backend.

``MemcachedHealthCheck`` opens a throw-away ``aiomcache`` connection, issues a
``stats()`` command to the server, and closes the connection immediately.

Memcached has no ``PING`` command — ``stats()`` is the canonical liveness
probe.  It sends a single ``stats`` request over the memcached text protocol
and returns server statistics (version, uptime, hit/miss counters, etc.)
only when the server is alive and responding.

DESIGN: throw-away connection per check() call
    ✅ Tests real broker connectivity at call-time.
    ✅ No shared mutable state — safe to call concurrently from multiple tasks.
    ✅ Does not depend on ``MemcachedCache`` internals — the health probe works
       even if the cache is not started (useful for readiness checks before
       the application has initialised its cache).
    ❌ Slightly higher overhead vs. reusing the ``MemcachedCache._client`` pool.
       Acceptable — health checks are infrequent and ``stats()`` is lightweight.
    Alternative: borrow ``MemcachedCache._client`` — rejected because it couples
    the health probe to the cache's private state and returns stale connectivity
    if the pool was created before the server restarted.

DESIGN: stats() over set/get/delete round-trip
    ✅ ``stats()`` is a single-round-trip command with no side effects —
       no keys are written or deleted.
    ✅ It is the canonical Memcached health-check command (used by AWS
       ElastiCache monitors, memcached-tool, and most client libraries).
    ❌ A successful ``stats()`` call does not prove that read/write operations
       will succeed (e.g. memory full or slab allocation failure).  For a
       deeper probe, use an integration-level readiness check instead.

Thread safety:  ✅ No shared state between check() calls.
Async safety:   ✅ check() is async def; all I/O uses await.

📚 Docs
- 🔍 https://github.com/aio-libs/aiomcache    — aiomcache API reference
- 🔍 https://github.com/memcached/memcached/wiki/Commands#stats
  Memcached ``stats`` command — canonical server health probe
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for
  asyncio.wait_for — timeout wrapper
- 🐍 https://docs.python.org/3/library/time.html#time.monotonic
  time.monotonic — monotonic clock for latency measurement
"""

from __future__ import annotations

import asyncio
import sys
import time

from providify import Inject, Singleton

from varco_core.health import HealthCheck, HealthResult, HealthStatus
from varco_memcached.cache import MemcachedCacheSettings


# ── MemcachedHealthCheck ──────────────────────────────────────────────────────


@Singleton(priority=-sys.maxsize, qualifier="memcached")
class MemcachedHealthCheck(HealthCheck):
    """
    Liveness probe for a Memcached instance.

    Opens a throw-away ``aiomcache.Client`` on each ``check()`` call,
    sends a ``stats()`` command, and closes the connection.

    Attributes:
        host:    Memcached server hostname.
        port:    Memcached server port.
        timeout: Seconds before the probe is abandoned.  Default 5 s.

    Thread safety:  ✅ No shared mutable state.
    Async safety:   ✅ check() is fully async; connections are task-local.

    Edge cases:
        - check() NEVER raises — exceptions are returned as UNHEALTHY results.
        - The connection is always closed in a finally block to avoid leaks.
        - ``stats()`` returns an empty dict on a healthy but idle server — this
          is treated as HEALTHY (it means the server responded without error).
        - If Memcached is at memory capacity, ``stats()`` still succeeds —
          the probe confirms reachability, not write availability.
    """

    def __init__(
        self,
        settings: Inject[MemcachedCacheSettings],
        *,
        timeout: float = 5.0,
    ) -> None:
        """
        Initialise the Memcached health probe.

        Args:
            settings: Memcached connection settings injected from the container.
                      The probe uses ``settings.host`` and ``settings.port`` to
                      target the same Memcached instance as the cache backend.
            timeout:  Probe timeout in seconds.
        """
        self._host = settings.host
        self._port = settings.port
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "memcached"

    async def check(self) -> HealthResult:
        """
        Probe Memcached connectivity via the ``stats()`` command.

        Returns:
            HealthResult(HEALTHY, latency_ms) on success.
            HealthResult(UNHEALTHY, detail) on timeout or connection error.
            Never raises.
        """
        # Import here to avoid importing aiomcache in environments where
        # the Memcached backend is not installed.
        import aiomcache  # type: ignore[import-untyped]

        start = time.monotonic()
        client: aiomcache.Client | None = None

        try:
            client = aiomcache.Client(self._host, self._port, pool_size=1)
            # stats() returns {b"stat_name": b"stat_value", ...} — any non-error
            # response (including an empty dict) confirms the server is alive.
            await asyncio.wait_for(client.stats(), timeout=self._timeout)
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
                detail=f"timed out after {self._timeout}s connecting to {self._host}:{self._port}",
            )
        except Exception as exc:  # noqa: BLE001 — intentionally broad: never raise
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                component=self.name,
                detail=str(exc),
            )
        finally:
            # Close the throw-away connection regardless of outcome to avoid
            # leaking TCP sockets.
            if client is not None:
                try:
                    await client.close()
                except Exception:  # noqa: BLE001 — best-effort cleanup
                    pass

    def __repr__(self) -> str:
        return (
            f"MemcachedHealthCheck("
            f"host={self._host!r}, "
            f"port={self._port}, "
            f"timeout={self._timeout})"
        )


__all__ = ["MemcachedHealthCheck"]
