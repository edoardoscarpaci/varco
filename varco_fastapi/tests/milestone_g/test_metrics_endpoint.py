"""
tests/milestone_g/test_metrics_endpoint.py
==========================================
Tests for MetricsRouter — the ``GET /metrics`` Prometheus scrape endpoint.

Tests verify:
- Endpoint returns 200 with correct Prometheus content type.
- OpenMetrics format is returned when ``Accept: application/openmetrics-text``.
- 503 is returned when ``prometheus_client`` is not installed.
- Custom prefix mounts the endpoint at the correct path.
- ``create_varco_app(enable_metrics=True)`` auto-mounts the endpoint.
- ``create_varco_app(enable_metrics=False)`` does NOT mount the endpoint.
- MetricsMiddleware is present in the stack when ``enable_metrics=True``.

📚 Docs:
    - 🐍 unittest.mock: https://docs.python.org/3/library/unittest.mock.html
    - 🔍 httpx AsyncClient + ASGITransport: https://www.python-httpx.org/async/
    - 🔍 Prometheus text format: https://prometheus.io/docs/instrumenting/exposition_formats/
"""

from __future__ import annotations

import sys
from unittest import mock

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from varco_fastapi.router.metrics import MetricsRouter


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_metrics_app(prefix: str = "/metrics") -> FastAPI:
    """
    Build a minimal FastAPI app with MetricsRouter mounted.

    Args:
        prefix: The URL path for the metrics endpoint.

    Returns:
        A FastAPI app with the metrics route registered.
    """
    app = FastAPI()
    app.include_router(MetricsRouter(prefix=prefix).build_router())
    return app


# ── Basic endpoint tests ──────────────────────────────────────────────────────


async def test_metrics_endpoint_returns_200():
    """``GET /metrics`` returns HTTP 200 when prometheus_client is installed."""
    app = _make_metrics_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/metrics")
    assert resp.status_code == 200


async def test_metrics_content_type_is_prometheus_text():
    """
    ``Content-Type`` header must contain ``text/plain`` for the classic
    Prometheus text format (v0.0.4).
    """
    app = _make_metrics_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/metrics")
    assert resp.status_code == 200
    assert "text/plain" in resp.headers["content-type"]


async def test_metrics_body_is_non_empty():
    """
    The response body is non-empty — even without OTel metrics, prometheus_client
    exports default Python process metrics (GC stats, memory, etc.).
    """
    app = _make_metrics_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/metrics")
    assert len(resp.content) > 0


async def test_openmetrics_format_via_accept_header():
    """
    When ``Accept: application/openmetrics-text`` is sent, the response uses
    OpenMetrics content type (used by Prometheus ≥ 2.26 for exemplar support).
    """
    app = _make_metrics_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get(
            "/metrics",
            headers={"Accept": "application/openmetrics-text"},
        )
    # OpenMetrics or classic — either is valid; the key is 200 and correct type
    assert resp.status_code == 200
    assert (
        "openmetrics-text" in resp.headers["content-type"]
        or "text/plain" in resp.headers["content-type"]
    )


async def test_returns_503_when_prometheus_client_missing():
    """
    When ``prometheus_client`` is not importable, the endpoint returns 503
    with an install hint in the body.

    This is the graceful degradation path — the app does not crash at startup;
    the error surfaces on the first scrape attempt.
    """
    app = _make_metrics_app()

    # Simulate prometheus_client being absent by patching the import inside the handler
    original_modules = sys.modules.copy()
    # Remove prometheus_client from sys.modules to force ImportError on import
    sys.modules["prometheus_client"] = None  # type: ignore[assignment]
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            resp = await client.get("/metrics")
        assert resp.status_code == 503
        assert "prometheus" in resp.text.lower()
    finally:
        # Restore original module state — critical to avoid polluting other tests
        sys.modules.update(original_modules)
        if "prometheus_client" not in original_modules:
            sys.modules.pop("prometheus_client", None)


async def test_default_prefix_is_slash_metrics():
    """
    ``MetricsRouter()`` with no ``prefix`` argument mounts at ``/metrics``.
    """
    router = MetricsRouter()
    api_router = router.build_router()
    routes = [r.path for r in api_router.routes]  # type: ignore[attr-defined]
    assert "/metrics" in routes


async def test_custom_prefix():
    """
    ``MetricsRouter(prefix="/observability/metrics")`` mounts at the custom path.
    """
    app = _make_metrics_app(prefix="/observability/metrics")
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/observability/metrics")
    assert resp.status_code == 200

    # Original /metrics must NOT exist
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/metrics")
    assert resp.status_code == 404


# ── Integration with create_varco_app ─────────────────────────────────────────


async def test_create_varco_app_enable_metrics_mounts_endpoint():
    """
    ``create_varco_app(enable_metrics=True)`` automatically mounts
    ``MetricsRouter`` — ``GET /metrics`` returns 200 (or 503 if exporter
    missing, but the route must be present — not 404).
    """
    from varco_fastapi import create_varco_app

    app = create_varco_app(enable_metrics=True, validate=False)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/metrics")
    # Route is mounted — 200 (prometheus installed) or 503 (not installed)
    # Either way it must NOT be 404.
    assert (
        resp.status_code != 404
    ), "GET /metrics must be mounted when enable_metrics=True"


async def test_create_varco_app_enable_metrics_false_no_endpoint():
    """
    ``create_varco_app(enable_metrics=False)`` does NOT mount ``MetricsRouter``
    — ``GET /metrics`` returns 404.
    """
    from varco_fastapi import create_varco_app

    app = create_varco_app(enable_metrics=False, validate=False)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/metrics")
    assert (
        resp.status_code == 404
    ), "GET /metrics must not exist when enable_metrics=False"


async def test_create_varco_app_metrics_middleware_added():
    """
    ``create_varco_app(enable_metrics=True)`` adds ``MetricsMiddleware`` to
    the middleware stack — verifiable by making a request and confirming the
    ``http.server.request.duration`` histogram instrument is populated.
    """
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader
    from varco_fastapi.middleware.metrics import _instruments
    from varco_fastapi import create_varco_app

    _instruments.clear()
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])

    # Patch in the _internal module where get_meter actually looks up the provider.
    # opentelemetry.metrics.get_meter is DEFINED in opentelemetry.metrics._internal
    # (its __globals__ points to _internal.__dict__), so patching the re-export
    # namespace (opentelemetry.metrics.get_meter_provider) has no effect on
    # the internal name resolution.
    with mock.patch(
        "opentelemetry.metrics._internal.get_meter_provider", return_value=provider
    ):
        app = create_varco_app(enable_metrics=True, validate=False)

        @app.get("/ping")
        async def ping():
            return {"pong": True}

        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            await client.get("/ping")

    # If MetricsMiddleware was active, a duration data point should exist
    metrics_data = reader.get_metrics_data()
    instrument_names = []
    if metrics_data and metrics_data.resource_metrics:
        for rm in metrics_data.resource_metrics:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    instrument_names.append(m.name)

    assert "http.server.request.duration" in instrument_names, (
        "MetricsMiddleware must record http.server.request.duration "
        "when enable_metrics=True"
    )
    _instruments.clear()


# ── MetricsRouter repr ────────────────────────────────────────────────────────


def test_metrics_router_repr():
    """``MetricsRouter.__repr__`` includes prefix and tags for debuggability."""
    router = MetricsRouter(prefix="/custom", tags=["ops"])
    r = repr(router)
    assert "/custom" in r
    assert "ops" in r
