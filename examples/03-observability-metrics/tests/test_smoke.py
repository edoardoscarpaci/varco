"""
test_smoke.py
=============
Smoke tests for the ``03-observability-metrics`` example.

Coverage
--------
Happy paths:
  - ``GET /metrics`` returns 200 (or 503 when prometheus_client absent — route
    is always present when ``enable_metrics=True``).
  - ``GET /metrics`` body is non-empty (process metrics present even without
    OTel wiring).
  - ``POST /posts`` creates a post; ``GET /posts`` returns it.
  - After a request to ``/posts``, ``http.server.request.duration`` instrument
    is populated in the OTel ``InMemoryMetricReader`` — proves
    ``MetricsMiddleware`` is active in the stack.

Unhappy paths:
  - ``GET /metrics`` is NOT present when ``enable_metrics=False`` (returns 404).
  - Unknown route returns 404.

DESIGN: InMemoryMetricReader for instrument-level assertions
    ``prometheus_client`` is an optional extra (``pip install prometheus-client``
    or ``uv add opentelemetry-exporter-prometheus``).  These tests must pass
    without it.  For instrument-level assertions we use OTel's own
    ``InMemoryMetricReader`` (always available via ``opentelemetry-sdk``) and
    ``mock.patch`` the ``_setup_prometheus_meter_provider`` function so
    ``create_app()`` installs our in-memory reader instead of the Prometheus one.

    Tests that are only meaningful with ``prometheus_client`` installed are
    guarded by ``_requires_prometheus`` and skipped otherwise.

    ✅ Core middleware behaviour is testable without any optional extra.
    ✅ Prometheus-specific tests still run in environments with the extra.
    ❌ Two code paths for ``create_app()`` in tests vs. production — mitigated
       by the mock being narrow (only ``_setup_prometheus_meter_provider``).

DESIGN: session-scoped client with pre-wired InMemoryMetricReader
    ``MetricsMiddleware`` creates instruments lazily on first request.  We need
    a reader attached BEFORE the first request so the reader captures data.
    The ``inmemory_client`` fixture patches the provider setup before calling
    ``create_app()`` — guaranteeing the reader is wired into the same provider
    that the middleware will use.

    ✅ Reader and middleware share the same provider — data actually flows.
    ❌ Reader is global OTel state — tests must clear it between runs to avoid
       data bleed.  The ``inmemory_client`` fixture clears ``_instruments`` on
       setup to prevent stale lazy-init from a previous test session.

Thread safety:  ✅ asyncio_mode=auto; single-threaded event loop.
Async safety:   ✅ All tests are ``async def``.

📚 Docs:
    - 🔍 OTel InMemoryMetricReader: https://opentelemetry-python.readthedocs.io/
    - 🔍 httpx AsyncClient: https://www.python-httpx.org/async/
    - 🐍 unittest.mock: https://docs.python.org/3/library/unittest.mock.html
"""

from __future__ import annotations

import importlib.util
from unittest import mock

import httpx
import pytest
from httpx import ASGITransport
from opentelemetry import metrics as otel_metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

# Guard for prometheus_client-dependent tests.
# ``prometheus_client`` is optional — skip rather than fail when absent.
_prometheus_installed = importlib.util.find_spec("prometheus_client") is not None
_requires_prometheus = pytest.mark.skipif(
    not _prometheus_installed,
    reason=("prometheus_client not installed — run: uv add opentelemetry-exporter-prometheus"),
)


# ── Fixtures ──────────────────────────────────────────────────────────────────


def _make_in_memory_provider() -> tuple[MeterProvider, InMemoryMetricReader]:
    """
    Build an OTel ``MeterProvider`` backed by ``InMemoryMetricReader``.

    Returns:
        Tuple of ``(MeterProvider, InMemoryMetricReader)`` sharing the same
        reader so callers can both record metrics and inspect them.
    """
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    return provider, reader


@pytest.fixture()
async def inmemory_client() -> httpx.AsyncClient:
    """
    ASGI client wired to the example app with ``InMemoryMetricReader``.

    Uses two patches to ensure ``MetricsMiddleware`` instruments flow into the
    ``InMemoryMetricReader`` without requiring ``prometheus_client``:

    1. ``app._setup_prometheus_meter_provider`` → no-op (skips Prometheus import).
    2. ``opentelemetry.metrics._internal.get_meter_provider`` → returns our
       ``MeterProvider(metric_readers=[reader])``.

    Patch (2) is required because the OTel SDK forbids replacing the global
    ``MeterProvider`` once it has been set (raises a warning and ignores the
    call).  Patching ``get_meter_provider`` at the internal level bypasses the
    guard — every call to ``get_meter(name)`` returns a meter from OUR provider,
    regardless of what the global state is.

    Also clears ``varco_fastapi.middleware.metrics._instruments`` before each
    test so lazy-initialised instruments from a prior test session (bound to a
    different provider) are not reused.

    Yields:
        A configured ``httpx.AsyncClient`` backed by the example app.
        ``ac._reader`` is set to the ``InMemoryMetricReader`` for assertions.

    Edge cases:
        - Clearing ``_instruments`` is critical: if stale instruments exist
          they reference the OLD provider's meter, so new data goes to the old
          reader (which the test never inspects).  Fresh instruments bind to
          OUR provider.
        - ``get_meter_provider`` is patched at ``_internal`` not at
          ``opentelemetry.metrics`` because ``get_meter`` resolves the provider
          via its own module's ``__globals__`` — patching the re-export has no
          effect.  This matches the pattern in the existing
          ``test_metrics_endpoint.py``.

    Thread safety:  ✅ Single-threaded asyncio test runner.
    Async safety:   ✅ Yields inside an ``async with`` block.
    """
    from varco_fastapi.middleware.metrics import _instruments  # noqa: PLC0415

    # Force fresh lazy instrument init on first request so they bind to our
    # provider rather than any previously registered one.
    _instruments.clear()

    provider, reader = _make_in_memory_provider()

    # Patch 1: skip the Prometheus setup so create_app() doesn't try to import
    # opentelemetry-exporter-prometheus (which is not installed in the test env).
    def _noop_setup() -> bool:
        return True

    # Patch 2: intercept every get_meter() call at the OTel internal level so
    # instruments are created from OUR provider.  Using the internal path because
    # that is where get_meter() actually looks up the provider at call time.
    with (
        mock.patch("app._setup_prometheus_meter_provider", _noop_setup),
        mock.patch(
            "opentelemetry.metrics._internal.get_meter_provider",
            return_value=provider,
        ),
    ):
        from app import create_app  # noqa: PLC0415

        test_app = create_app()

        async with httpx.AsyncClient(
            transport=ASGITransport(app=test_app),
            base_url="http://testserver",
        ) as ac:
            # Expose the reader on the client so test assertions can inspect it.
            ac._reader = reader  # type: ignore[attr-defined]
            yield ac

    # Clean up stale instruments after the test — each fixture invocation
    # should start with a clean slate.
    _instruments.clear()


@pytest.fixture()
async def plain_client() -> httpx.AsyncClient:
    """
    ASGI client for the example app using the real Prometheus metric reader.

    Used only in tests decorated with ``_requires_prometheus``.  Does NOT patch
    ``_setup_prometheus_meter_provider`` — the real function runs and installs
    a ``PrometheusMetricReader``.

    Yields:
        A configured ``httpx.AsyncClient`` backed by the example app.
    """
    from varco_fastapi.middleware.metrics import _instruments

    _instruments.clear()
    _previous_provider = otel_metrics.get_meter_provider()

    from app import create_app  # noqa: PLC0415

    test_app = create_app()
    try:
        async with httpx.AsyncClient(
            transport=ASGITransport(app=test_app),
            base_url="http://testserver",
        ) as ac:
            yield ac
    finally:
        otel_metrics.set_meter_provider(_previous_provider)
        _instruments.clear()


# ── Helper: extract instrument names from InMemoryMetricReader ────────────────


def _collect_instrument_names(reader: InMemoryMetricReader) -> list[str]:
    """
    Collect all metric instrument names from an ``InMemoryMetricReader``.

    Args:
        reader: The ``InMemoryMetricReader`` to query.

    Returns:
        List of instrument name strings recorded since the last flush.
    """
    data = reader.get_metrics_data()
    names: list[str] = []
    if not data or not data.resource_metrics:
        return names
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            for m in sm.metrics:
                names.append(m.name)
    return names


# ── Happy path — /metrics endpoint is mounted ─────────────────────────────────


async def test_metrics_endpoint_is_mounted(inmemory_client: httpx.AsyncClient) -> None:
    """
    ``GET /metrics`` must be reachable (not 404) when ``enable_metrics=True``.

    May return 200 (prometheus_client installed) or 503 (not installed) but
    must never return 404 — the route is always registered.
    """
    response = await inmemory_client.get("/metrics")
    assert response.status_code != 404, (
        f"GET /metrics must be mounted when enable_metrics=True. Got {response.status_code}."
    )


async def test_metrics_endpoint_body_is_non_empty(
    inmemory_client: httpx.AsyncClient,
) -> None:
    """
    ``GET /metrics`` body is non-empty.

    Even without OTel metrics, when prometheus_client IS installed the
    endpoint serves default Python process metrics.  When prometheus_client
    is absent the 503 body contains an install hint.
    Either way the body must not be empty.
    """
    response = await inmemory_client.get("/metrics")
    # Body must have content regardless of status code.
    assert len(response.content) > 0, "GET /metrics must return a non-empty body."


@_requires_prometheus
async def test_metrics_endpoint_returns_200_with_prometheus(
    plain_client: httpx.AsyncClient,
) -> None:
    """
    When ``prometheus_client`` is installed, ``GET /metrics`` returns 200
    with Prometheus text content type.
    """
    response = await plain_client.get("/metrics")
    assert response.status_code == 200
    assert "text/plain" in response.headers.get("content-type", ""), (
        "Expected Prometheus text/plain content-type for GET /metrics."
    )


@_requires_prometheus
async def test_metrics_body_contains_prometheus_text(
    plain_client: httpx.AsyncClient,
) -> None:
    """
    Prometheus text format starts with ``# HELP`` or ``# TYPE`` comment lines.

    After at least one ``/posts`` request the body should include OTel metrics
    alongside the default Python process metrics.
    """
    # Generate traffic so MetricsMiddleware has something to record.
    await plain_client.post("/posts", json={"title": "hello"})

    response = await plain_client.get("/metrics")
    assert response.status_code == 200
    body = response.text
    # Classic Prometheus text format always starts with # HELP / # TYPE blocks.
    assert "# HELP" in body or "# TYPE" in body, (
        "Prometheus text format must contain # HELP or # TYPE comment lines."
    )


# ── Happy path — MetricsMiddleware records instruments ────────────────────────


async def test_middleware_records_duration_after_request(
    inmemory_client: httpx.AsyncClient,
) -> None:
    """
    After a request to ``/posts``, the ``InMemoryMetricReader`` must contain
    ``http.server.request.duration`` — proving ``MetricsMiddleware`` is active.

    This test does NOT require ``prometheus_client``; it uses OTel's own
    ``InMemoryMetricReader`` to inspect what the middleware recorded.
    """
    # Make a request so the middleware records at least one data point.
    await inmemory_client.post("/posts", json={"title": "test post"})

    reader: InMemoryMetricReader = inmemory_client._reader  # type: ignore[attr-defined]
    instrument_names = _collect_instrument_names(reader)

    assert "http.server.request.duration" in instrument_names, (
        "MetricsMiddleware must record http.server.request.duration "
        "histogram after handling a request.  "
        f"Instruments found: {instrument_names}"
    )


async def test_middleware_records_active_requests_counter(
    inmemory_client: httpx.AsyncClient,
) -> None:
    """
    After a request, the ``http.server.active_requests`` UpDownCounter must be
    in the reader — proving all three standard instruments are wired.
    """
    await inmemory_client.get("/posts")

    reader: InMemoryMetricReader = inmemory_client._reader  # type: ignore[attr-defined]
    instrument_names = _collect_instrument_names(reader)

    assert "http.server.active_requests" in instrument_names, (
        "MetricsMiddleware must record http.server.active_requests counter.  "
        f"Instruments found: {instrument_names}"
    )


# ── Happy path — /posts CRUD endpoints ───────────────────────────────────────


async def test_create_post_returns_201(inmemory_client: httpx.AsyncClient) -> None:
    """``POST /posts`` with a JSON body returns 201 with the created post."""
    response = await inmemory_client.post(
        "/posts", json={"title": "Hello World", "body": "First post"}
    )
    assert response.status_code == 201
    body = response.json()
    assert body["title"] == "Hello World"
    assert body["body"] == "First post"
    # In-memory store assigns sequential id
    assert "id" in body


async def test_list_posts_returns_200(inmemory_client: httpx.AsyncClient) -> None:
    """``GET /posts`` returns 200 with a list (may be empty)."""
    response = await inmemory_client.get("/posts")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


async def test_created_post_appears_in_list(inmemory_client: httpx.AsyncClient) -> None:
    """A post created via ``POST /posts`` appears in ``GET /posts``."""
    create_resp = await inmemory_client.post("/posts", json={"title": "Visible post"})
    assert create_resp.status_code == 201
    post_id = create_resp.json()["id"]

    list_resp = await inmemory_client.get("/posts")
    assert list_resp.status_code == 200
    ids = [p["id"] for p in list_resp.json()]
    assert post_id in ids, f"Post with id={post_id} must appear in GET /posts response."


# ── Unhappy path — routing ────────────────────────────────────────────────────


async def test_unknown_route_returns_404(inmemory_client: httpx.AsyncClient) -> None:
    """Requests to undefined paths return 404."""
    response = await inmemory_client.get("/nonexistent")
    assert response.status_code == 404


async def test_metrics_not_mounted_when_disabled() -> None:
    """
    A default ``create_varco_app()`` (no ``MetricsModule``) must NOT mount
    ``GET /metrics``.

    Verified by checking that a request to ``/metrics`` returns 404.
    """
    from providify import DIContainer  # noqa: PLC0415

    from varco_fastapi import create_varco_app  # noqa: PLC0415

    # Build a minimal app WITHOUT metrics — no prometheus setup needed.
    disabled_app = create_varco_app(
        DIContainer(),
        validate=False,
    )

    async with httpx.AsyncClient(
        transport=ASGITransport(app=disabled_app),
        base_url="http://testserver",
    ) as client:
        response = await client.get("/metrics")

    assert response.status_code == 404, (
        f"GET /metrics must not be mounted when enable_metrics=False. Got {response.status_code}."
    )
