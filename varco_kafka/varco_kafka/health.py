"""
varco_kafka.health
==================
Liveness probe for the Kafka backend.

``KafkaHealthCheck`` creates a short-lived ``AIOKafkaProducer``, calls
``fetch_all_metadata()`` to confirm the broker is reachable, then closes
the connection immediately.

DESIGN: throw-away producer per check() call
    ✅ Tests real broker connectivity at call-time — a cached connection object
       that was created at startup could be alive while the broker is currently
       unreachable (e.g. broker restart between health-check intervals).
    ✅ No shared mutable state — safe to call concurrently from multiple tasks.
    ❌ Slightly higher overhead vs. reusing the bus connection.  Acceptable —
       health checks are infrequent (seconds between calls) and the producer
       creates one TCP connection that is immediately torn down.
    Alternative considered: expose the bus's internal producer — rejected because
    it would require coupling KafkaHealthCheck to KafkaEventBus internals.

Thread safety:  ✅ No shared state between check() calls.
Async safety:   ✅ check() is async def; all I/O uses await.

📚 Docs
- 🔍 https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.AIOKafkaProducer
  AIOKafkaProducer — start(), stop(), client
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for
  asyncio.wait_for — timeout wrapper for coroutines
- 🐍 https://docs.python.org/3/library/time.html#time.monotonic
  time.monotonic — monotonic clock for latency measurement
"""

from __future__ import annotations

import asyncio
import time

from varco_core.health import HealthCheck, HealthResult, HealthStatus


# ── KafkaHealthCheck ──────────────────────────────────────────────────────────


class KafkaHealthCheck(HealthCheck):
    """
    Liveness probe for a Kafka cluster.

    Creates a throw-away ``AIOKafkaProducer`` on each ``check()`` call,
    fetches broker metadata to confirm real connectivity, then tears the
    producer down.

    Attributes:
        bootstrap_servers: Comma-separated broker addresses to probe.
        timeout:           Seconds before the probe is abandoned and
                           ``UNHEALTHY`` is returned.  Default 5 s.

    Thread safety:  ✅ No shared mutable state.
    Async safety:   ✅ check() is fully async; producers are task-local.

    Edge cases:
        - check() NEVER raises — exceptions are caught and returned as
          HealthResult(UNHEALTHY, ...).
        - If the producer starts but metadata fetch times out, the producer
          is still properly stopped to avoid a dangling connection.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        *,
        timeout: float = 5.0,
    ) -> None:
        """
        Initialise the Kafka health probe.

        Args:
            bootstrap_servers: Comma-separated broker addresses
                               (e.g. ``"kafka:9092"`` or ``"b1:9092,b2:9092"``).
            timeout:           Probe timeout in seconds.  The probe attempts
                               to connect and fetch metadata within this budget.
        """
        self._bootstrap_servers = bootstrap_servers
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "kafka"

    async def check(self) -> HealthResult:
        """
        Probe Kafka broker connectivity by fetching cluster metadata.

        Creates a throw-away producer, calls ``fetch_all_metadata()``, and
        tears the producer down regardless of the outcome.

        Returns:
            HealthResult with HEALTHY and latency on success.
            HealthResult with UNHEALTHY and error detail on any failure.
            Never raises.

        Edge cases:
            - asyncio.TimeoutError → UNHEALTHY, latency_ms=None.
            - Any other exception → UNHEALTHY with exception repr as detail.
        """
        # Import here to keep the top-level import fast and avoid importing
        # aiokafka in environments where the Kafka backend is not installed.
        from aiokafka import AIOKafkaProducer  # type: ignore[import-untyped]

        start = time.monotonic()
        producer: AIOKafkaProducer | None = None

        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
            )
            # wait_for enforces the probe budget — wrap both start() and the
            # metadata fetch together so the total round-trip is bounded.
            await asyncio.wait_for(producer.start(), timeout=self._timeout)
            # fetch_all_metadata() triggers a real round-trip to the broker —
            # start() alone only establishes the connection object.
            await asyncio.wait_for(
                producer.client.fetch_all_metadata(),
                timeout=self._timeout,
            )
            latency_ms = (time.monotonic() - start) * 1000
            return HealthResult(
                status=HealthStatus.HEALTHY,
                component=self.name,
                latency_ms=latency_ms,
            )
        except asyncio.TimeoutError:
            # The probe exceeded its budget — broker may be alive but slow.
            # Report UNHEALTHY; latency_ms is omitted because the round-trip
            # never completed.
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                component=self.name,
                detail=f"timed out after {self._timeout}s connecting to {self._bootstrap_servers}",
            )
        except Exception as exc:  # noqa: BLE001 — intentionally broad: never raise
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                component=self.name,
                detail=str(exc),
            )
        finally:
            # Always stop the throw-away producer — even if an exception
            # occurred — to release the TCP connection immediately.
            if producer is not None:
                try:
                    await producer.stop()
                except (
                    Exception
                ):  # noqa: BLE001 — best-effort cleanup; ignore teardown errors
                    pass

    def __repr__(self) -> str:
        return (
            f"KafkaHealthCheck("
            f"bootstrap_servers={self._bootstrap_servers!r}, "
            f"timeout={self._timeout})"
        )


__all__ = ["KafkaHealthCheck"]
