"""
varco_fastapi.router.health
============================
Auto-generated health check endpoints for ``VarcoRouter``.

``HealthRouter`` exposes three endpoints:

    GET /health         — Aggregated health of all registered components
    GET /health/ready   — Readiness probe (503 if any component is UNHEALTHY)
    GET /health/live    — Liveness probe (always 200 — proves process is running)

Uses ``CompositeHealthCheck`` from ``varco_core.health`` to aggregate multiple
``HealthCheck`` instances concurrently via ``asyncio.gather``.

The ``HealthCheck`` instances can be passed explicitly at construction time or
auto-discovered from a DI container.

DESIGN: standalone build_router() over inheriting VarcoRouter
    ✅ HealthRouter has no generic type params (no D/PK/C/R/U)
    ✅ Health endpoints don't need a service or auth (they're public by default)
    ✅ build_router() is the same interface — easy to include alongside other routers
    ❌ Slightly different construction than other VarcoRouter subclasses

DESIGN: 503 on UNHEALTHY vs. always 200
    ✅ Kubernetes readiness probes check HTTP status — 503 stops traffic routing
    ✅ Liveness probe always 200 — process is alive even when components are down
    ✅ Aggregated /health returns 200 but body contains full status details
    ❌ Callers must check both status code and body for full health picture

Thread safety:  ✅ HealthRouter is stateless after construction.
Async safety:   ✅ check() runs all probes concurrently via asyncio.gather.
"""

from __future__ import annotations

import asyncio
from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from varco_core.health import (
    CompositeHealthCheck,
    HealthCheck,
    HealthResult,
    HealthStatus,
)
from providify import Instance, InstanceProxy


class HealthRouter:
    """
    Auto-generated health check router.

    Exposes three endpoints:
    - ``GET /health``       → aggregated status (always 200, body has details)
    - ``GET /health/ready`` → readiness probe (503 if any UNHEALTHY)
    - ``GET /health/live``  → liveness probe (always 200)

    Args:
        checks:  List of ``HealthCheck`` instances to aggregate.
                 If empty, all probes return ``HEALTHY`` (trivially passing).
        prefix:  URL prefix (default: ``"/health"``).
        tags:    OpenAPI tags (default: ``["health"]``).
        timeout: Max seconds to wait for all checks (default: 5.0).
                 Probes that exceed this are marked UNHEALTHY.

    Usage::

        health_router = HealthRouter(
            checks=[
                RedisHealthCheck(redis_client),
                SAHealthCheck(engine),
            ],
            prefix="/health",
        )
        app.include_router(health_router.build_router())

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ check() uses asyncio.gather; safe for concurrent probes.

    Edge cases:
        - No checks provided → all endpoints return HEALTHY
        - A check raises → wrapped in try/except, recorded as UNHEALTHY
        - Timeout exceeded → remaining checks recorded as UNHEALTHY
    """

    def __init__(
        self,
        checks: Instance[HealthCheck] | list[HealthCheck] | None = None,
        *,
        prefix: str = "/health",
        tags: list[str] | None = None,
        timeout: float = 5.0,
    ) -> None:
        self._checks = checks or []
        self._prefix = prefix
        self._tags = tags or ["health"]
        self._timeout = timeout
        # Build composite once — it owns concurrent execution logic.
        # CompositeHealthCheck takes *checks (variadic), not a list.
        self._composite = None  # Lazy construction
        self._resolved = False

    def __repr__(self) -> str:
        checks_len = len(self._checks) if isinstance(self._checks, list) else 0
        return f"HealthRouter(" f"checks={checks_len}, " f"prefix={self._prefix!r})"

    async def _resolve_checks(self):
        if self._checks and isinstance(self._checks, InstanceProxy):
            self._checks = await self._checks.aget_all()
        self._composite = CompositeHealthCheck(*self._checks) if self._checks else None
        self._resolved = True

    async def _run_checks(self) -> list[HealthResult]:
        """
        Run all health checks with a timeout.

        Returns:
            List of ``HealthResult`` objects (one per check).
            Empty list if no checks are registered.

        Async safety:   ✅ Uses asyncio.gather; concurrent probe execution.
        """
        if not self._resolved:
            await self._resolve_checks()

        if self._composite is None:
            return []

        try:
            # asyncio.wait_for cancels the composite if it exceeds timeout
            return await asyncio.wait_for(
                self._composite.check_all(),
                timeout=self._timeout,
            )
        except asyncio.TimeoutError:
            # All checks timed out — return a single UNHEALTHY result
            return [
                HealthResult(
                    component="composite",
                    status=HealthStatus.UNHEALTHY,
                    detail=f"Health checks timed out after {self._timeout}s",
                )
            ]

    def build_router(self) -> APIRouter:
        """
        Materialize the three health endpoints into a FastAPI ``APIRouter``.

        Returns:
            An ``APIRouter`` with GET /, GET /ready, GET /live.
        """
        router = APIRouter(prefix=self._prefix, tags=self._tags)

        # Capture instance in closure to avoid late-binding issues
        health_router = self

        @router.get(
            "/",
            summary="Aggregate health status",
            description=(
                "Returns the aggregated health of all registered components.\n\n"
                "Always returns HTTP 200 — check the ``status`` field in the response body.\n"
                "Use ``/health/ready`` for Kubernetes readiness probes."
            ),
            response_class=JSONResponse,
            include_in_schema=True,
        )
        async def health_aggregate() -> dict[str, Any]:
            """Return aggregated health status (always 200)."""
            results = await health_router._run_checks()
            overall = _aggregate_status(results)
            return {
                "status": overall.value,
                "components": [_result_to_dict(r) for r in results],
            }

        @router.get(
            "/ready",
            summary="Readiness probe",
            description=(
                "Returns ``200 OK`` if all components are HEALTHY or DEGRADED.\n\n"
                "Returns ``503 Service Unavailable`` if any component is UNHEALTHY.\n\n"
                "Use this endpoint for Kubernetes readiness probes — "
                "a 503 stops traffic from being routed to this pod."
            ),
            response_class=JSONResponse,
            include_in_schema=True,
        )
        async def health_ready() -> JSONResponse:
            """Return 200 if ready, 503 if not."""
            results = await health_router._run_checks()
            overall = _aggregate_status(results)
            status_code = 503 if overall == HealthStatus.UNHEALTHY else 200
            return JSONResponse(
                content={
                    "status": overall.value,
                    "components": [_result_to_dict(r) for r in results],
                },
                status_code=status_code,
            )

        @router.get(
            "/live",
            summary="Liveness probe",
            description=(
                "Always returns ``200 OK`` — proves the process is running.\n\n"
                "Does NOT check component health.  Use ``/health/ready`` to check\n"
                "whether the service is ready to receive traffic."
            ),
            include_in_schema=True,
        )
        async def health_live() -> dict[str, str]:
            """Always 200 — process is alive."""
            return {"status": HealthStatus.HEALTHY.value}

        return router


# ── Helpers ───────────────────────────────────────────────────────────────────


def _aggregate_status(results: list[HealthResult]) -> HealthStatus:
    """
    Reduce a list of health results to the worst-case status.

    UNHEALTHY > DEGRADED > HEALTHY.

    Args:
        results: Health results to aggregate.

    Returns:
        Worst-case ``HealthStatus`` across all results.
        Returns ``HEALTHY`` if ``results`` is empty (no checks registered).
    """
    if not results:
        return HealthStatus.HEALTHY
    # Severity order: HEALTHY=0, DEGRADED=1, UNHEALTHY=2
    severity: dict[HealthStatus, int] = {
        HealthStatus.HEALTHY: 0,
        HealthStatus.DEGRADED: 1,
        HealthStatus.UNHEALTHY: 2,
    }
    return max(results, key=lambda r: severity.get(r.status, 0)).status


def _result_to_dict(result: HealthResult) -> dict[str, Any]:
    """
    Serialize a ``HealthResult`` to a JSON-safe dict.

    Args:
        result: The ``HealthResult`` to serialize.

    Returns:
        Dict with ``component``, ``status``, and optional ``latency_ms``/``detail``.
    """
    d: dict[str, Any] = {
        "component": result.component,
        "status": result.status.value,
    }
    if result.latency_ms is not None:
        d["latency_ms"] = result.latency_ms
    if result.detail:
        d["detail"] = result.detail
    return d


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "HealthRouter",
]
