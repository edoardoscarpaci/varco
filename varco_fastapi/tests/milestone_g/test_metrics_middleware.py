"""
tests/milestone_g/test_metrics_middleware.py
=============================================
Unit tests for MetricsMiddleware.

Tests verify:
- OTel instruments are recorded with the correct names and attributes.
- Route template (low-cardinality) is used, not the raw URL.
- Paths matching skip_paths are not instrumented.
- Active requests counter is balanced (sum = 0 after request completes).
- Body size is recorded when Content-Length is present.
- Exception paths record status 500 and still decrement the active counter.

Each test gets a fresh InMemoryMetricReader + MeterProvider so instruments
from one test do not bleed into the next.  The module-level ``_instruments``
cache is cleared in the autouse fixture.

📚 Docs:
    - 🔍 OTel InMemoryMetricReader: https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.export.html
    - 🔍 httpx AsyncClient + ASGITransport: https://www.python-httpx.org/async/
"""

from __future__ import annotations

from unittest import mock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from varco_fastapi.middleware.metrics import MetricsMiddleware, _instruments


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def clear_instrument_cache():
    """
    Clear the lazy instrument cache before and after every test.

    The cache is module-level so instruments created in one test would be
    bound to that test's MeterProvider.  Clearing it guarantees each test
    gets fresh instruments wired to its own reader.
    """
    _instruments.clear()
    yield
    _instruments.clear()


@pytest.fixture()
def metric_reader():
    """
    Create a fresh ``InMemoryMetricReader`` + ``MeterProvider`` for each test
    without touching the OTel global provider.

    DESIGN: mock.patch on opentelemetry.metrics._internal.get_meter_provider
        The OTel SDK locks the global MeterProvider after first set — calling
        ``set_meter_provider()`` a second time logs a warning and is a no-op.
        We patch ``opentelemetry.metrics._internal.get_meter_provider`` (NOT
        ``opentelemetry.metrics.get_meter_provider``) because ``get_meter`` is
        *defined* in ``_internal`` — its ``__globals__`` points to
        ``_internal.__dict__``, so it looks up ``get_meter_provider`` there.
        Patching the re-export namespace (``opentelemetry.metrics``) has no
        effect on the internal name resolution.
        ✅ Each test gets a truly isolated provider — no cross-test contamination.
        ✅ No warning noise from the OTel SDK.
        ❌ Relies on a private module name (``_internal``) that could change
           across OTel SDK versions.  Accepted: the alternative (global provider
           mutation) is far more fragile in a parallel test environment.

    Returns:
        The ``InMemoryMetricReader`` that can be queried after requests.
    """
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    # Patch the module-level function that all get_meter() calls delegate to.
    # The autouse clear_instrument_cache fixture ensures each test gets fresh
    # instruments that call get_meter() — and thus see our patched provider.
    with mock.patch(
        # get_meter is defined in opentelemetry.metrics._internal (its __globals__
        # points to _internal.__dict__), so we must patch there — NOT in
        # opentelemetry.metrics, which is just the re-export namespace.
        "opentelemetry.metrics._internal.get_meter_provider",
        return_value=provider,
    ):
        yield reader


def _make_app(**middleware_kwargs) -> FastAPI:
    """
    Build a minimal FastAPI app with MetricsMiddleware for testing.

    Args:
        **middleware_kwargs: Forwarded to ``MetricsMiddleware.__init__``.

    Returns:
        A FastAPI app with test routes and the middleware installed.
    """
    app = FastAPI()
    app.add_middleware(MetricsMiddleware, **middleware_kwargs)

    @app.get("/test")
    async def test_route():
        return {"ok": True}

    @app.get("/orders/{order_id}")
    async def get_order(order_id: str):
        return {"id": order_id}

    @app.post("/items")
    async def create_item():
        return {"created": True}

    @app.get("/fail")
    async def failing_route():
        raise ValueError("intentional error")

    return app


def _get_data_points(reader: InMemoryMetricReader, metric_name: str) -> list:
    """
    Extract data points for a named metric from the reader.

    Args:
        reader:      The ``InMemoryMetricReader`` to query.
        metric_name: OTel metric name (e.g. ``"http.server.request.duration"``).

    Returns:
        List of data point objects (may be empty if metric not recorded yet).
    """
    metrics = reader.get_metrics_data()
    if not metrics or not metrics.resource_metrics:
        return []
    for rm in metrics.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                if m.name == metric_name:
                    # Metrics can have multiple data types; gather all points
                    data = m.data
                    # HistogramDataPoint or NumberDataPoint
                    if hasattr(data, "data_points"):
                        return list(data.data_points)
    return []


# ── Tests ─────────────────────────────────────────────────────────────────────


async def test_records_request_duration(metric_reader):
    """
    ``http.server.request.duration`` histogram is recorded with count=1
    after one request, with correct method, route, and status_code attributes.
    """
    app = _make_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/test")
    assert resp.status_code == 200

    points = _get_data_points(metric_reader, "http.server.request.duration")
    assert len(points) == 1, f"Expected 1 data point, got {len(points)}"

    dp = points[0]
    assert dp.count == 1
    attrs = dict(dp.attributes)
    assert attrs["http.request.method"] == "GET"
    assert attrs["http.route"] == "/test"
    assert attrs["http.response.status_code"] == "200"
    # Duration must be positive (wall clock)
    assert dp.sum > 0


async def test_route_template_not_raw_url(metric_reader):
    """
    ``http.route`` attribute uses the template ``/orders/{order_id}``,
    NOT the concrete URL ``/orders/123``.

    This is the cardinality guard — without this check, each unique order ID
    would create a separate time-series in the TSDB.
    """
    app = _make_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/orders/123")
        await client.get("/orders/456")

    points = _get_data_points(metric_reader, "http.server.request.duration")
    # Both requests should collapse into ONE histogram bucket (same route template)
    assert len(points) == 1, "Both /orders/* requests must share the same route label"
    attrs = dict(points[0].attributes)
    assert (
        attrs["http.route"] == "/orders/{order_id}"
    ), "Route must be the template, not the concrete URL"
    assert attrs["http.route"] != "/orders/123"
    assert attrs["http.route"] != "/orders/456"
    # count=2 because both requests hit the same route template
    assert points[0].count == 2


async def test_unknown_route_for_404(metric_reader):
    """
    Unmatched paths (404s) produce ``http.route="unknown"``.

    Grouping all 404s under ``"unknown"`` prevents cardinality explosion
    from path-scanning bots that probe thousands of distinct URLs.
    """
    app = _make_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/does/not/exist")

    assert resp.status_code == 404
    points = _get_data_points(metric_reader, "http.server.request.duration")
    assert len(points) == 1
    assert dict(points[0].attributes)["http.route"] == "unknown"


async def test_active_requests_decremented_after_request(metric_reader):
    """
    ``http.server.active_requests`` UpDownCounter net value is 0 after a
    request completes — the +1 before the call is balanced by the -1 in finally.
    """
    app = _make_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/test")

    points = _get_data_points(metric_reader, "http.server.active_requests")
    # UpDownCounter may report one or two data points depending on the SDK version
    # (one per attribute set).  The key invariant is that the total sum is 0.
    total = sum(dp.value for dp in points)
    assert total == 0, f"Net active requests must be 0 after completion, got {total}"


async def test_skips_metrics_path(metric_reader):
    """
    Requests to ``/metrics`` are not instrumented (skip_paths default).

    The scrape endpoint itself should not appear in the latency histogram —
    it is infrastructure, not business traffic.
    """
    app = _make_app()

    # Add a /metrics route so the app doesn't 404
    from fastapi.responses import PlainTextResponse

    @app.get("/metrics")
    async def mock_metrics():
        return PlainTextResponse("# empty")

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/metrics")

    points = _get_data_points(metric_reader, "http.server.request.duration")
    assert len(points) == 0, "GET /metrics must not be instrumented"


async def test_skips_health_path(metric_reader):
    """
    Requests to ``/health`` are not instrumented (skip_paths default).

    Kubernetes liveness/readiness probes hit this endpoint every few seconds —
    recording them would inflate request counts and distort latency percentiles.
    """
    app = _make_app()

    from fastapi.responses import JSONResponse

    @app.get("/health")
    async def mock_health():
        return JSONResponse({"status": "ok"})

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/health")

    points = _get_data_points(metric_reader, "http.server.request.duration")
    assert len(points) == 0, "GET /health must not be instrumented"


async def test_custom_skip_paths(metric_reader):
    """
    Custom ``skip_paths`` overrides the default set.
    """
    app = _make_app(skip_paths=frozenset({"/custom-skip"}))

    from fastapi.responses import PlainTextResponse

    @app.get("/custom-skip")
    async def custom():
        return PlainTextResponse("skipped")

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        # /custom-skip is in skip_paths — should not be instrumented
        await client.get("/custom-skip")
        # /test is NOT in skip_paths — should be instrumented
        await client.get("/test")

    points = _get_data_points(metric_reader, "http.server.request.duration")
    routes = {dict(dp.attributes)["http.route"] for dp in points}
    assert "/test" in routes, "/test should be instrumented"
    assert "/custom-skip" not in routes, "/custom-skip should be skipped"


async def test_records_body_size_when_content_length(metric_reader):
    """
    ``http.server.request.body.size`` histogram records the ``Content-Length``
    value when the header is present.
    """
    app = _make_app()
    body = b'{"name": "widget"}'
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.post(
            "/items",
            content=body,
            headers={"Content-Length": str(len(body))},
        )

    points = _get_data_points(metric_reader, "http.server.request.body.size")
    assert len(points) == 1
    # sum should equal the Content-Length value
    assert points[0].sum == len(body)


async def test_no_body_size_without_content_length(metric_reader):
    """
    ``http.server.request.body.size`` is NOT recorded when ``Content-Length``
    is absent — common for GET requests with no body.
    """
    app = _make_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/test")

    points = _get_data_points(metric_reader, "http.server.request.body.size")
    assert (
        len(points) == 0
    ), "Body size must not be recorded when Content-Length is absent"


async def test_5xx_recorded_with_correct_status_code(metric_reader):
    """
    When the route handler returns a 5xx response, the duration histogram records
    ``status_code="500"`` and the active requests counter is still balanced.

    This ensures error-rate queries (filtering by 5xx status) work correctly.

    DESIGN: use HTTPException(500) instead of raise RuntimeError
        Starlette 1.0 ServerErrorMiddleware calls the error handler AND then
        re-raises the exception, causing it to propagate to the test client even
        when a handler is registered.  HTTPException is handled by FastAPI's
        built-in exception handler which returns a proper 500 JSON response
        without re-raising — so the test client receives a 500 status code
        cleanly rather than a RuntimeError.
    """
    from fastapi import HTTPException

    app = FastAPI()
    app.add_middleware(MetricsMiddleware)

    @app.get("/boom")
    async def boom():
        # HTTPException(500) → FastAPI converts to 500 JSON response internally
        # without re-raising at the ASGI level.
        raise HTTPException(status_code=500, detail="intentional server error")

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.get("/boom")

    # FastAPI returns 500 for unhandled exceptions
    assert resp.status_code == 500

    points = _get_data_points(metric_reader, "http.server.request.duration")
    assert len(points) == 1
    assert dict(points[0].attributes)["http.response.status_code"] == "500"

    # Active requests must still be balanced
    active_points = _get_data_points(metric_reader, "http.server.active_requests")
    assert sum(dp.value for dp in active_points) == 0


async def test_multiple_routes_separate_histogram_buckets(metric_reader):
    """
    Requests to different routes produce separate histogram data points,
    each with distinct ``http.route`` attributes.
    """
    app = _make_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/test")
        await client.get("/orders/999")

    points = _get_data_points(metric_reader, "http.server.request.duration")
    routes = {dict(dp.attributes)["http.route"] for dp in points}
    assert "/test" in routes
    assert "/orders/{order_id}" in routes
    assert len(routes) == 2


async def test_different_status_codes_separate_buckets(metric_reader):
    """
    Requests resulting in different status codes produce separate histogram
    data points, enabling per-status-code rate queries.
    """
    app = _make_app()
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        await client.get("/test")  # 200
        await client.get("/missing")  # 404 (no such route)

    points = _get_data_points(metric_reader, "http.server.request.duration")
    status_codes = {dict(dp.attributes)["http.response.status_code"] for dp in points}
    assert "200" in status_codes
    assert "404" in status_codes
