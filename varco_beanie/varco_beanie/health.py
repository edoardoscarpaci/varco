"""
varco_beanie.health
===================
Liveness probe for the Beanie / MongoDB backend.

``BeanieHealthCheck`` calls ``client.server_info()`` on the injected
``AsyncMongoClient`` to confirm the MongoDB server is reachable and
responding to commands.  Like the SA probe, it borrows the existing client
rather than creating a throw-away connection — motor/pymongo manage their
own connection pools, and ``server_info()`` is the canonical aliveness test.

DESIGN: borrow existing client over throw-away client
    ✅ No duplicate connection pool creation — the caller's client already
       has auth, TLS, and replica-set config baked in.
    ✅ ``server_info()`` is the standard MongoDB health-check command — it
       returns the server version and capabilities, confirming real connectivity.
    ❌ A client with no available connections could cause check() to hang —
       mitigated by ``asyncio.wait_for`` with the configured timeout.
    Alternative: ``client.admin.command("ping")`` — functionally identical;
    ``server_info()`` was chosen because it is the pymongo-recommended probe.

Thread safety:  ✅ AsyncMongoClient is thread-safe; server_info() is safe
                   to call concurrently from multiple tasks.
Async safety:   ✅ check() is async def; all I/O uses await.

📚 Docs
- 🔍 https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html
  AsyncMongoClient.server_info() — canonical connectivity probe
- 🐍 https://docs.python.org/3/library/asyncio-task.html#asyncio.wait_for
  asyncio.wait_for — timeout wrapper
- 🐍 https://docs.python.org/3/library/time.html#time.monotonic
  time.monotonic — monotonic clock for latency measurement
"""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from varco_core.health import HealthCheck, HealthResult, HealthStatus

if TYPE_CHECKING:
    # AsyncMongoClient only needed for type hints — avoids importing pymongo
    # in environments that mock the client.
    from pymongo import AsyncMongoClient


# ── BeanieHealthCheck ─────────────────────────────────────────────────────────


class BeanieHealthCheck(HealthCheck):
    """
    Liveness probe for a MongoDB server.

    Calls ``client.server_info()`` on the injected ``AsyncMongoClient``
    to verify the server is reachable and responding.

    Attributes:
        client:  The shared ``AsyncMongoClient`` to probe.
        timeout: Seconds before the probe is abandoned.  Default 5 s.

    Thread safety:  ✅ AsyncMongoClient is internally thread-safe.
    Async safety:   ✅ check() is async def; server_info() is awaited.

    Edge cases:
        - check() NEVER raises — exceptions are returned as UNHEALTHY results.
        - If the server is a replica set with no primary, ``server_info()``
          may raise ``ServerSelectionTimeoutError`` before the wait_for
          deadline — this is caught and returned as UNHEALTHY with detail.
        - Authentication failures also raise here and are returned as UNHEALTHY.
    """

    def __init__(
        self,
        client: AsyncMongoClient,
        *,
        timeout: float = 5.0,
    ) -> None:
        """
        Initialise the MongoDB health probe.

        Args:
            client:  The ``AsyncMongoClient`` to probe — typically the one
                     registered in ``BeanieSettings`` and injected into
                     ``BeanieModule``.
            timeout: Probe timeout in seconds.
        """
        self._client = client
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "mongodb"

    async def check(self) -> HealthResult:
        """
        Probe MongoDB connectivity via ``server_info()``.

        Returns:
            HealthResult(HEALTHY, latency_ms) on success.
            HealthResult(UNHEALTHY, detail) on timeout or connection error.
            Never raises.
        """
        start = time.monotonic()

        try:
            # server_info() sends a ``buildInfo`` command to the server —
            # lightweight, requires no authentication on most configurations,
            # and confirms the server is responsive.
            await asyncio.wait_for(
                self._client.server_info(),
                timeout=self._timeout,
            )
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
                detail=f"timed out after {self._timeout}s connecting to MongoDB",
            )
        except Exception as exc:  # noqa: BLE001 — intentionally broad: never raise
            return HealthResult(
                status=HealthStatus.UNHEALTHY,
                component=self.name,
                detail=str(exc),
            )

    def __repr__(self) -> str:
        return f"BeanieHealthCheck(client={self._client!r}, timeout={self._timeout})"


__all__ = ["BeanieHealthCheck"]
