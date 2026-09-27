"""
app.py
======
Application factory for the ``03-observability-metrics`` example.

Demonstrates the full varco observability / metrics stack:

1. ``create_varco_app(container, modules=[*default_modules(), MetricsModule()])``
   — adds ``MetricsMiddleware`` to the ASGI stack AND mounts ``GET /metrics``
   (Prometheus scrape endpoint).
2. ``MetricsMiddleware`` — records three OTel instruments per request:
   ``http.server.request.duration`` (Histogram), ``http.server.active_requests``
   (UpDownCounter), ``http.server.request.body.size`` (Histogram).
3. ``PrometheusMetricReader`` — bridges OTel SDK metrics into
   ``prometheus_client.REGISTRY`` so ``generate_latest()`` includes them.
4. ``MeterProvider`` — the OTel SDK meter provider that drives collection.

No database, no message broker, no Docker required.

Run locally::

    cd examples/03-observability-metrics
    uv run uvicorn app:app --reload

Then scrape::

    curl http://localhost:8000/metrics

DESIGN: explicit MeterProvider + PrometheusMetricReader setup
    varco_fastapi's ``MetricsMiddleware`` creates instruments lazily on the
    first request, obtaining them from the OTel global ``MeterProvider``.  If
    no real provider is registered (the default is a no-op), instruments are
    discarded silently.

    This example wires a real ``MeterProvider`` backed by
    ``PrometheusMetricReader`` before the first request arrives.  The reader
    auto-registers with ``prometheus_client.REGISTRY`` at construction time,
    so every subsequent ``generate_latest()`` call includes OTel metrics.

    ✅ Demonstrates the full OTel → Prometheus pipeline end-to-end.
    ✅ No external collector required — ``/metrics`` is the scrape endpoint.
    ✅ PrometheusMetricReader is push-pull: it collects on demand (each
       scrape) rather than on a fixed interval — ideal for a demo.
    ❌ ``prometheus_client`` must be installed (``pip install prometheus-client``
       or ``uv add prometheus-client``).

DESIGN: ``create_varco_app`` for full middleware stack
    Unlike the profiling example which hand-assembles the ASGI stack,
    ``create_varco_app`` is used here because:
    ✅ ``enable_metrics=True`` cleanly wires both the middleware and the router.
    ✅ The full standard middleware stack (CORS, errors, tracing) is shown
       working together with metrics — a realistic production configuration.
    ❌ Slightly more implicit than hand-assembly — readers must know that
       ``create_varco_app`` calls ``_mount_metrics_router`` internally.

Thread safety:  ✅ Called once at startup.
Async safety:   ✅ Synchronous factory — no event loop required at call time.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI

_logger = logging.getLogger(__name__)


def _setup_prometheus_meter_provider() -> bool:
    """
    Install a real OTel ``MeterProvider`` backed by ``PrometheusMetricReader``.

    Must be called BEFORE the first HTTP request so that ``MetricsMiddleware``'s
    lazy instrument creation picks up the real provider (not the no-op default).

    The ``PrometheusMetricReader`` auto-registers with
    ``prometheus_client.REGISTRY`` at construction time.  All metrics recorded
    through any OTel meter obtained from this provider will appear in
    ``generate_latest()`` output — i.e. at ``GET /metrics``.

    Returns:
        ``True`` if the provider was installed; ``False`` if
        ``opentelemetry-exporter-prometheus`` is not installed (graceful
        degradation — the app still starts, but ``/metrics`` will return 503
        and OTel metrics will not flow into Prometheus).

    Edge cases:
        - Called multiple times → the global provider is replaced.  In normal
          operation this function is called exactly once at app startup.
        - ``opentelemetry-sdk`` not installed → ``ImportError`` propagates
          (it is a required dep of ``varco_core``; should never happen).
        - ``prometheus-client`` not installed → logs a warning and returns
          ``False`` — ``/metrics`` serves a 503 with an install hint.

    Thread safety:  ✅ Intended to be called once at startup before requests begin.
    Async safety:   ✅ Synchronous — no event loop required.
    """
    try:
        from opentelemetry.exporter.prometheus import (
            PrometheusMetricReader,
        )  # noqa: PLC0415
    except ImportError:
        # opentelemetry-exporter-prometheus is an optional extra.
        # The app still starts without it; GET /metrics returns 503.
        # Install with: uv add opentelemetry-exporter-prometheus
        _logger.warning(
            "observability-metrics: opentelemetry-exporter-prometheus not installed — "
            "OTel metrics will not flow into Prometheus.  "
            "GET /metrics will return 503.  "
            "Fix: uv add opentelemetry-exporter-prometheus"
        )
        return False

    from opentelemetry import metrics as otel_metrics  # noqa: PLC0415
    from opentelemetry.sdk.metrics import MeterProvider  # noqa: PLC0415

    # PrometheusMetricReader registers itself with prometheus_client.REGISTRY
    # at construction time — no explicit registration call needed.
    reader = PrometheusMetricReader()

    # Set as the global OTel provider so MetricsMiddleware's lazy
    # ``get_meter(meter_name)`` calls resolve to this provider.
    provider = MeterProvider(metric_readers=[reader])
    otel_metrics.set_meter_provider(provider)

    _logger.info("observability-metrics: OTel MeterProvider with PrometheusMetricReader installed.")
    return True


def create_app() -> FastAPI:
    """
    Build and return the configured FastAPI application with metrics enabled.

    Steps:
    1. Install a ``MeterProvider`` backed by ``PrometheusMetricReader`` so OTel
       metrics flow into ``prometheus_client.REGISTRY``.
    2. Call ``create_varco_app(modules=[*default_modules(), MetricsModule()])`` which:
       a. Adds ``MetricsMiddleware`` to the ASGI stack (records per-request
          duration, active-requests count, and body size instruments).
       b. Mounts ``GET /metrics`` via ``MetricsRouter`` (Prometheus scrape
          endpoint backed by ``prometheus_client.generate_latest()``).
    3. Register a minimal ``/posts`` endpoint so the middleware has real
       requests to measure (otherwise ``/metrics`` only shows the scrape itself,
       which is excluded from measurements by default).

    Returns:
        A configured ``FastAPI`` instance ready for an ASGI server.

    Edge cases:
        - ``opentelemetry-exporter-prometheus`` not installed → ``ImportError``
          from ``_setup_prometheus_meter_provider()`` — app fails at startup
          with a helpful message rather than silently discarding metrics.
        - ``MetricsMiddleware`` skips ``/metrics`` by default (``skip_paths``
          frozenset) to avoid Prometheus scrapes inflating request counts.
        - The ``/posts`` endpoint uses an in-memory list — not thread-safe for
          concurrent writes.  This is acceptable for a single-process demo.

    Thread safety:  ✅ Intended to be called once per process.
    Async safety:   ✅ Synchronous; no event loop required at call time.
    """
    # ── 1. Wire OTel → Prometheus pipeline BEFORE first request ──────────────
    # MetricsMiddleware creates instruments lazily on first request using the
    # global OTel MeterProvider.  The provider must be set before that happens.
    # Returns False when the prometheus extra is absent — app starts anyway;
    # GET /metrics will return 503 until the extra is installed.
    _setup_prometheus_meter_provider()

    # ── 2. Build app with metrics enabled ────────────────────────────────────
    from providify import DIContainer
    from varco_fastapi.modules import MetricsModule
    from varco_fastapi.modules.defaults import default_modules

    from varco_fastapi import create_varco_app

    app: FastAPI = create_varco_app(
        DIContainer(),
        title="Observability Metrics Example",
        version="0.1.0",
        description=(
            "Demonstrates varco's OTel metrics + Prometheus scraping.\n\n"
            "**Endpoints**:\n"
            "- ``POST /posts`` — create a post (increments request counter)\n"
            "- ``GET /posts`` — list all posts (measured by middleware)\n"
            "- ``GET /metrics`` — Prometheus scrape endpoint\n\n"
            "After making a few requests to ``/posts``, the ``/metrics`` "
            "output will include ``http_server_request_duration_seconds`` "
            "and related OTel instruments."
        ),
        # Plan 049 / DI-1 migration: enable_metrics=True is now MetricsModule
        # (adds MetricsMiddleware + mounts GET /metrics).
        modules=[*default_modules(), MetricsModule()],
        validate=False,  # No routers to validate in this example
    )

    # ── 3. Minimal in-memory posts API for traffic generation ─────────────────
    # A simple list store — safe for the single-process demo; not for prod.
    _store: list[dict] = []

    @app.post("/posts", status_code=201)
    async def create_post(body: dict) -> dict:
        """
        Create a post and return it.

        Intentionally minimal — accepts any JSON dict — so the example stays
        focused on the observability layer rather than schema validation.

        Args:
            body: Arbitrary JSON dict representing the new post.

        Returns:
            The stored post dict with an auto-assigned ``id`` field.
        """
        # Assign a sequential id for human-readable scrape output
        post = {"id": len(_store) + 1, **body}
        _store.append(post)
        return post

    @app.get("/posts")
    async def list_posts() -> list[dict]:
        """
        Return all posts.

        Returns:
            List of all stored post dicts (may be empty).
        """
        return list(_store)

    return app


# Module-level app — lets uvicorn use ``uvicorn app:app`` without ``--factory``.
# ``create_app()`` no longer raises ImportError for the prometheus extra —
# it degrades gracefully (GET /metrics returns 503).
app = create_app()

__all__ = ["app", "create_app"]
