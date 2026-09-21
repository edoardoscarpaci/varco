"""
varco_nats.health
=================
Liveness probe for the NATS JetStream backend.

``NatsHealthCheck`` opens a short-lived NATS connection, confirms the server is
reachable and JetStream is enabled (via ``account_info()``), then closes the
connection immediately.

DESIGN: throw-away connection per check() call
    ✅ Tests real connectivity at call-time — a cached connection created at
       startup could appear alive while the server is currently unreachable
       (e.g. a NATS restart between health-check intervals).
    ✅ ``account_info()`` confirms JetStream specifically — a NATS server
       without JetStream enabled would pass a plain connect but fail the bus.
    ✅ No shared mutable state — safe to call concurrently from multiple tasks.
    ❌ Slightly higher overhead vs. reusing the bus connection.  Acceptable —
       health checks are infrequent and the connection is torn down at once.
    Alternative considered: expose the bus's internal connection — rejected
    because it would couple NatsHealthCheck to NatsEventBus internals.

Thread safety:  ✅ No shared state between check() calls.
Async safety:   ✅ check() is async def; all I/O uses await.

📚 Docs
- 🔍 https://nats-io.github.io/nats.py/ — nats-py connect() / JetStream account_info
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for
  asyncio.wait_for — timeout wrapper for coroutines
- 🐍 https://docs.python.org/3/library/time.html#time.monotonic
  time.monotonic — monotonic clock for latency measurement
"""

from __future__ import annotations

import asyncio
import time

from providify import Inject, Singleton
from varco_core.health import HealthCheck, HealthResult, HealthStatus

from varco_nats.config import NatsEventBusSettings

# ── NatsHealthCheck ───────────────────────────────────────────────────────────


# collection contribution — never @Fallback (it would vanish from
# get_all(HealthCheck) once an app adds its own check)
@Singleton(qualifier="nats")
class NatsHealthCheck(HealthCheck):
    """
    Liveness probe for a NATS server with JetStream.

    Opens a throw-away connection on each ``check()`` call, queries JetStream
    account info to confirm real connectivity, then tears the connection down.

    Attributes:
        servers: Comma-separated NATS server URLs to probe.
        timeout: Seconds before the probe is abandoned and ``UNHEALTHY`` is
                 returned.  Default 5 s.

    Thread safety:  ✅ No shared mutable state.
    Async safety:   ✅ check() is fully async; connections are task-local.

    Edge cases:
        - check() NEVER raises — exceptions are caught and returned as
          ``HealthResult(UNHEALTHY, ...)``.
        - If the connection opens but JetStream is not enabled,
          ``account_info()`` raises and the probe reports UNHEALTHY.
        - The connection is always closed, even on failure, to avoid a
          dangling socket.
    """

    def __init__(
        self,
        settings: Inject[NatsEventBusSettings],
        *,
        timeout: float = 5.0,
    ) -> None:
        """
        Initialise the NATS health probe.

        Args:
            settings: NATS connection settings injected from the container.
                      The probe targets the same servers as the event bus.
            timeout:  Probe timeout in seconds.
        """
        self._servers = settings.to_servers_list()
        self._servers_repr = settings.servers
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "nats"

    async def check(self) -> HealthResult:
        """
        Probe NATS connectivity and JetStream availability.

        Opens a throw-away connection, calls ``account_info()`` on the
        JetStream context, and tears the connection down regardless of outcome.

        Returns:
            HealthResult with HEALTHY and latency on success.
            HealthResult with UNHEALTHY and error detail on any failure.
            Never raises.

        Edge cases:
            - asyncio.TimeoutError → UNHEALTHY, latency_ms=None.
            - Any other exception (incl. JetStream-not-enabled) → UNHEALTHY
              with the exception text as detail.
        """
        # Import here to keep the top-level import fast — nats-py is a hard
        # dependency, but deferring keeps health.py importable in a stripped
        # environment that only needs the settings class.
        import nats  # noqa: PLC0415

        start = time.monotonic()
        nc = None

        try:
            # wait_for bounds the connect — connection setup is the slow path.
            nc = await asyncio.wait_for(
                nats.connect(servers=self._servers),
                timeout=self._timeout,
            )
            # account_info() is a real round-trip to the JetStream API — it
            # confirms both connectivity AND that JetStream is enabled.
            js = nc.jetstream()
            await asyncio.wait_for(js.account_info(), timeout=self._timeout)

            latency_ms = (time.monotonic() - start) * 1000
            return HealthResult(
                status=HealthStatus.HEALTHY,
                component=self.name,
                latency_ms=latency_ms,
            )
        except TimeoutError:
            # The probe exceeded its budget — server may be alive but slow.
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                component=self.name,
                detail=f"timed out after {self._timeout}s connecting to {self._servers_repr}",
            )
        except Exception as exc:  # noqa: BLE001 — intentionally broad: never raise
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                component=self.name,
                detail=str(exc),
            )
        finally:
            # Always close the throw-away connection — even on failure — to
            # release the socket immediately.
            if nc is not None:
                try:
                    await nc.close()
                except Exception:  # noqa: BLE001 — best-effort cleanup
                    pass

    def __repr__(self) -> str:
        return f"NatsHealthCheck(servers={self._servers_repr!r}, timeout={self._timeout})"


__all__ = ["NatsHealthCheck"]
