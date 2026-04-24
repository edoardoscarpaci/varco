"""
varco_fastapi.router.metrics
=============================
Router that exposes ``GET /metrics`` in Prometheus text or OpenMetrics format.

How it works
------------
``MetricsRouter`` delegates entirely to ``prometheus_client.generate_latest()``.
It does NOT drive the OTel collection itself — that is handled by
``PrometheusMetricReader`` created inside ``OtelConfiguration.meter_provider()``
when ``OtelConfig.prometheus_enabled=True``.  The reader registers itself with
``prometheus_client.REGISTRY`` at construction time, so any call to
``generate_latest()`` automatically includes all OTel metrics.

This separation means ``MetricsRouter`` is stateless: it holds no reference to
the reader, the provider, or the registry — it just calls the library function.

Format negotiation
------------------
When the scraper sends ``Accept: application/openmetrics-text``, the endpoint
returns OpenMetrics format (``prometheus_client.openmetrics.exposition``).
OpenMetrics is preferred by Prometheus ≥ 2.26 and the OTel Collector because
it supports **exemplars** — links from a metric observation to the trace span
that caused it.  Falls back to classic Prometheus text if OpenMetrics is
unavailable.

DESIGN: 503 on ImportError instead of startup crash
    ✅ Consistent with ``MCPAdapter`` and other optional-extra patterns in
       varco_fastapi — errors surface at request time with a log.ERROR, not
       at app startup.
    ✅ Apps that do not use ``MetricsRouter`` pay zero import cost.
    ❌ Error is visible at scrape time, not startup — mitigated by the
       ERROR log that fires on the first scrape attempt.

DESIGN: standalone class (not VarcoRouter subclass)
    ``MetricsRouter`` has no domain model, no service, and no CRUD operations.
    Making it a ``VarcoRouter`` would pull in all the CRUD plumbing for no
    benefit.  Modelled after ``HealthRouter`` — a plain class with
    ``build_router()`` returning a FastAPI ``APIRouter``.
    ✅ Lightweight — no DI requirements.
    ✅ Independently mountable — callers can add it without ``create_varco_app``.
    ❌ Not auto-scanned by ``container.get_all(VarcoRouter)`` — must be
       explicitly mounted (``create_varco_app(enable_metrics=True)`` does this).

DESIGN: Accept header negotiation for OpenMetrics vs Prometheus text
    ✅ Prometheus ≥ 2.26 sends ``Accept: application/openmetrics-text``
       automatically when it detects exemplar support — no config required.
    ✅ OpenMetrics exemplars link metrics to traces — Grafana can show the
       trace for any latency spike directly from a dashboard panel.
    ❌ Adding a second import path (openmetrics.exposition) increases
       complexity slightly; graceful fallback to classic format mitigates this.

Thread safety:  ✅ Stateless — no mutable instance state.
Async safety:   ✅ Handler is ``async def``.

📚 Docs:
    - 🔍 prometheus_client: https://github.com/prometheus/client_python
    - 🔍 OpenMetrics spec: https://openmetrics.io/
    - 🔍 OTel exemplars: https://opentelemetry.io/docs/specs/otel/metrics/data-model/#exemplars
"""

from __future__ import annotations

import logging

from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response

_logger = logging.getLogger(__name__)

# ── MetricsRouter ─────────────────────────────────────────────────────────────


class MetricsRouter:
    """
    Standalone router that exposes ``GET /metrics`` in Prometheus/OpenMetrics format.

    Works because ``PrometheusMetricReader`` (created by ``OtelConfiguration``
    when ``OtelConfig.prometheus_enabled=True``) auto-registers with
    ``prometheus_client.REGISTRY`` at construction time.  ``generate_latest()``
    collects from that registry — no polling, no background thread needed here.

    If ``OtelConfig.prometheus_enabled`` was ``False`` (or ``OtelConfiguration``
    was never installed), the REGISTRY still exists but contains only default
    Python process metrics (GC counts, memory usage, etc.) — not OTel metrics.
    The endpoint still returns 200 in that case; callers see process metrics only.

    Args:
        prefix: URL path for the metrics endpoint.  Default: ``"/metrics"``.
        tags:   OpenAPI tags shown in ``/docs``.  Default: ``["observability"]``.

    Edge cases:
        - ``opentelemetry-exporter-prometheus`` (``prometheus_client``) not
          installed → 503 response with install instructions; logs ERROR.
        - ``Accept: application/openmetrics-text`` requested but
          ``prometheus_client.openmetrics`` unavailable → silently falls back
          to classic Prometheus text format.
        - Multiple ``MetricsRouter`` instances with the same ``prefix`` mounted
          on the same app → FastAPI raises a duplicate route error at mount time.

    Thread safety:  ✅ Stateless — no mutable instance attributes after ``__init__``.
    Async safety:   ✅ Handler is ``async def``.

    Example::

        from varco_fastapi.router.metrics import MetricsRouter
        from fastapi import FastAPI

        app = FastAPI()
        app.include_router(MetricsRouter().build_router())

        # With custom prefix:
        app.include_router(MetricsRouter(prefix="/observability/metrics").build_router())
    """

    def __init__(
        self,
        *,
        prefix: str = "/metrics",
        tags: list[str] | None = None,
    ) -> None:
        # Store as private attributes — accessed by the closure inside build_router().
        self._prefix = prefix
        self._tags = tags or ["observability"]

    def build_router(self) -> APIRouter:
        """
        Build and return the FastAPI ``APIRouter`` with the metrics endpoint.

        Returns:
            An ``APIRouter`` with a single ``GET {prefix}`` route.

        Edge cases:
            - Called multiple times → returns a new ``APIRouter`` each time
              (no shared state).  Mount only once per app.
        """
        router = APIRouter(tags=self._tags)
        # Capture self in a local variable — closures in Python capture by
        # reference, so we need a stable reference to the router instance.
        metrics_router = self

        @router.get(
            self._prefix,
            # FastAPI needs an explicit response_class so it does not try to
            # JSON-serialise the raw bytes from generate_latest().
            response_class=Response,
            summary="Prometheus metrics scrape endpoint",
            description=(
                "Returns current metrics in Prometheus text format (default) "
                "or OpenMetrics format when ``Accept: application/openmetrics-text`` "
                "is sent.  Powered by ``prometheus_client.generate_latest()``."
            ),
        )
        async def metrics_endpoint(request: Request) -> Response:
            return await metrics_router._handle(request)

        return router

    async def _handle(self, request: Request) -> Response:
        """
        Produce the metrics scrape response.

        Negotiates the response format from the ``Accept`` header:
        - ``application/openmetrics-text`` → OpenMetrics format (exemplars).
        - Anything else → classic Prometheus text format.

        Args:
            request: Incoming HTTP request.

        Returns:
            A ``Response`` with the appropriate ``Content-Type`` and body, or
            a 503 ``PlainTextResponse`` if ``prometheus_client`` is not
            installed.

        Raises:
            Nothing — all errors are returned as HTTP responses.

        Edge cases:
            - ``prometheus_client`` not installed → 503.
            - ``prometheus_client.openmetrics`` unavailable → falls back to
              classic format silently.
        """
        try:
            from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
        except ImportError:
            _logger.error(
                "MetricsRouter: prometheus_client is not installed — "
                "GET %s returns 503.  "
                "Fix: pip install 'varco-fastapi[prometheus]'",
                self._prefix,
            )
            return PlainTextResponse(
                "Prometheus exporter not available. "
                "Install: pip install 'varco-fastapi[prometheus]'",
                status_code=503,
            )

        accept = request.headers.get("accept", "")

        if "application/openmetrics-text" in accept:
            # Prometheus ≥ 2.26 and the OTel Collector prefer OpenMetrics
            # because it supports exemplars — links from metric observations
            # to the trace spans that caused them.
            try:
                from prometheus_client import REGISTRY
                from prometheus_client.openmetrics.exposition import (
                    CONTENT_TYPE_LATEST as OPENMETRICS_CONTENT_TYPE,
                    generate_latest as openmetrics_generate,
                )

                # prometheus_client ≥ 0.20 requires REGISTRY as first arg
                return Response(
                    content=openmetrics_generate(REGISTRY),
                    media_type=OPENMETRICS_CONTENT_TYPE,
                )
            except ImportError:
                # OpenMetrics module missing in older prometheus_client versions —
                # fall through to classic format below.
                pass

        # Default: classic Prometheus text format (v0.0.4)
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST,
        )

    def __repr__(self) -> str:
        return f"MetricsRouter(prefix={self._prefix!r}, tags={self._tags!r})"


__all__ = ["MetricsRouter"]
