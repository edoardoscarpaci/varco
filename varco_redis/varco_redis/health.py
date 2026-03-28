"""
varco_redis.health
==================
Liveness probe for the Redis backend.

``RedisHealthCheck`` opens a throw-away Redis connection, issues a ``PING``
command, and closes the connection immediately.  This tests real network
connectivity to the Redis instance, not just the existence of a connection
pool object.

DESIGN: throw-away connection per check() call
    ✅ Tests real broker connectivity at call-time.
    ✅ No shared mutable state — safe to call concurrently.
    ❌ Slightly higher overhead vs. reusing an existing pool.  Acceptable —
       health checks are infrequent and PING is the lightest Redis command.
    Alternative: reuse the ``RedisEventBus`` or ``RedisCache`` internal
    connection — rejected because it would couple this probe to their internals
    and would return stale state if the pool was created before a broker restart.

Thread safety:  ✅ No shared state between check() calls.
Async safety:   ✅ check() is async def; all I/O uses await.

📚 Docs
- 🔍 https://redis-py.readthedocs.io/en/stable/connections.html
  redis.asyncio connection options — from_url, PING
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for
  asyncio.wait_for — timeout wrapper
- 🐍 https://docs.python.org/3/library/time.html#time.monotonic
  time.monotonic — monotonic clock for latency measurement
"""

from __future__ import annotations

import asyncio
import time

from varco_core.health import HealthCheck, HealthResult, HealthStatus


# ── RedisHealthCheck ──────────────────────────────────────────────────────────


class RedisHealthCheck(HealthCheck):
    """
    Liveness probe for a Redis instance.

    Opens a throw-away ``redis.asyncio`` connection on each ``check()`` call,
    sends a ``PING`` command, and closes the connection.

    Attributes:
        url:     Redis connection URL (e.g. ``"redis://localhost:6379/0"``).
        timeout: Seconds before the probe is abandoned.  Default 5 s.

    Thread safety:  ✅ No shared mutable state.
    Async safety:   ✅ check() is fully async; connections are task-local.

    Edge cases:
        - check() NEVER raises — exceptions are returned as UNHEALTHY results.
        - The connection is always closed in a finally block to avoid leaks.
        - A PONG response other than ``True`` is still considered HEALTHY —
          redis.asyncio returns ``True`` on success but the contract only
          requires a non-exception response.
    """

    def __init__(
        self,
        url: str,
        *,
        timeout: float = 5.0,
    ) -> None:
        """
        Initialise the Redis health probe.

        Args:
            url:     Redis connection URL (e.g. ``"redis://localhost:6379/0"``).
            timeout: Probe timeout in seconds.
        """
        self._url = url
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "redis"

    async def check(self) -> HealthResult:
        """
        Probe Redis connectivity via a PING command.

        Returns:
            HealthResult(HEALTHY, latency_ms) on success.
            HealthResult(UNHEALTHY, detail) on timeout or connection error.
            Never raises.
        """
        # Import here to avoid importing redis.asyncio in environments where
        # the Redis backend is not installed.
        import redis.asyncio as aioredis  # type: ignore[import-untyped]

        start = time.monotonic()
        client: aioredis.Redis | None = None

        try:
            # from_url creates the connection object but does NOT open a socket
            # yet — the socket is opened on the first command.
            client = aioredis.from_url(
                self._url,
                # decode_responses=False keeps parity with the event bus settings
                # contract; PING response is handled as bytes.
                decode_responses=False,
            )
            # wait_for bounds the total PING round-trip including connection setup.
            await asyncio.wait_for(client.ping(), timeout=self._timeout)
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
                detail=f"timed out after {self._timeout}s connecting to {self._url}",
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
                    await client.aclose()
                except Exception:  # noqa: BLE001 — best-effort cleanup
                    pass

    def __repr__(self) -> str:
        return f"RedisHealthCheck(url={self._url!r}, timeout={self._timeout})"


__all__ = ["RedisHealthCheck"]
