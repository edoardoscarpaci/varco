"""
varco_fastapi.middleware.metrics
=================================
ASGI middleware that records OTel HTTP server metrics per request.

Records three OTel instruments following the `OpenTelemetry HTTP semantic
conventions <https://opentelemetry.io/docs/specs/semconv/http/http-metrics/>`_:

``http.server.request.duration`` (Histogram, seconds)
    Duration of each request.  Attributes: ``http.request.method``,
    ``http.route``, ``http.response.status_code``.  From this single
    instrument, Prometheus/Grafana can derive RPS, latency percentiles,
    and error rates — no separate counter needed.

``http.server.active_requests`` (UpDownCounter, ``{request}``)
    Number of in-flight requests.  Attribute: ``http.request.method`` only.
    Route is intentionally omitted to avoid the pre/post-routing cardinality
    mismatch (see DESIGN note below).

``http.server.request.body.size`` (Histogram, bytes)
    Request body size when ``Content-Length`` header is present.  Attributes:
    ``http.request.method``, ``http.route``.

Usage::

    app = FastAPI()
    app.add_middleware(MetricsMiddleware)

    # Custom meter name and skip paths:
    app.add_middleware(
        MetricsMiddleware,
        meter_name="myapp.http",
        skip_paths=frozenset({"/metrics", "/health", "/readyz"}),
    )

DESIGN: lazy instrument creation (first request, not module import)
    OTel instruments must be obtained from the live ``MeterProvider`` set by
    ``OtelConfiguration``.  At import time only the no-op provider is
    registered.  Creating instruments eagerly captures the no-op provider —
    metrics are silently discarded.  Lazy creation (first request) guarantees
    the instrument comes from the real provider.
    ✅ Instruments are wired to the live exporter — data actually flows out.
    ✅ No ordering constraint between module import and DI bootstrap.
    ❌ A dict lookup on every request to check whether the instrument exists.
       This is a single dict.get() — negligible overhead.

DESIGN: ``http.server.active_requests`` without ``http.route`` attribute
    The middleware increments before routing (route is unknown) and decrements
    after routing (route is known).  Using route on both sides would require
    storing the pre-routing state so the decrement uses the same attribute set
    as the increment — extra complexity for marginal benefit.  Omitting route
    from the UpDownCounter keeps the implementation simple and correct.
    ✅ Increment and decrement always use the same attribute set → no orphan
       rows in the time-series database.
    ❌ Cannot filter active_requests by route — use the histogram for that.

DESIGN: skip_paths as frozenset of path prefixes
    ``/metrics`` and ``/health`` are excluded by default to avoid recording
    the scrape endpoint itself as high-frequency phantom traffic (Prometheus
    scrapes once every 15–60 s; health checks even more often on Kubernetes).
    ✅ Simple prefix matching avoids complex regex.
    ❌ No wildcard support — callers must add each path explicitly.

DESIGN: BaseHTTPMiddleware over pure ASGI
    Consistent with every other varco_fastapi middleware.  Simpler dispatch
    logic at the cost of one minor known limitation: timing includes body
    streaming time for streaming responses (SSE, large downloads).  This is
    acceptable for standard REST endpoint latency histograms.
    ✅ Same dispatch model as TracingMiddleware and RequestLoggingMiddleware.
    ❌ Streaming body timing may inflate p99 for endpoints with large payloads.

Thread safety:  ✅ Stateless per-request — no shared mutable state beyond the
                   module-level ``_instruments`` cache (GIL-protected writes).
Async safety:   ✅ ``dispatch`` is ``async def``; instruments are re-entrant.

📚 Docs:
    - 🔍 OTel HTTP semconv: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/
    - 🔍 Starlette BaseHTTPMiddleware: https://www.starlette.io/middleware/
    - 🐍 time.perf_counter: https://docs.python.org/3/library/time.html#time.perf_counter
"""

from __future__ import annotations

import logging
import time
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

_logger = logging.getLogger(__name__)

# ── Lazy instrument cache ──────────────────────────────────────────────────────
#
# Instruments are created on first request and reused forever.  The same
# lazy-cache pattern used by varco_core.observability.metrics._instrument_cache.
# Key: str (instrument name) — Value: OTel instrument object.
#
# CPython's GIL makes concurrent dict writes safe: two tasks racing to create
# the same instrument produce identical objects; last writer wins harmlessly.
_instruments: dict[str, Any] = {}


# ── OTel availability check ───────────────────────────────────────────────────


def _otel_available() -> bool:
    """
    Return ``True`` if the ``opentelemetry`` SDK is installed.

    Checked once at ``__init__`` time — not on every request — to avoid
    per-request import overhead.

    Returns:
        ``True`` if ``opentelemetry.metrics`` can be imported; ``False`` otherwise.
    """
    try:
        import opentelemetry.metrics  # noqa: F401

        return True
    except ImportError:
        return False


# ── Route template extraction ─────────────────────────────────────────────────


def _get_route_template(request: Request) -> str:
    """
    Extract the low-cardinality route template from the matched Starlette route.

    After routing, Starlette/FastAPI populates ``request.scope["route"]`` with
    the matched ``Route`` object.  Its ``.path`` attribute is the template
    string (e.g. ``"/orders/{id}"``), not the concrete URL (``"/orders/123"``).

    This is CRITICAL for metric cardinality — recording ``/orders/123`` as an
    attribute would produce a unique time-series per order ID, quickly
    exhausting TSDB memory.  The template ``/orders/{id}`` is bounded by the
    number of defined routes.

    Args:
        request: The current Starlette ``Request`` object.

    Returns:
        Route path template string, e.g. ``"/orders/{id}"``.
        Falls back to ``"unknown"`` for unmatched routes (404s) or if the
        route object lacks a ``.path`` attribute.

    Edge cases:
        - Called BEFORE routing (before ``call_next``) → always returns
          ``"unknown"`` because routing hasn't matched yet.  Always call this
          in the ``finally`` block, after ``call_next`` completes.
        - 404 Not Found → route is never populated → returns ``"unknown"``.
          All 404s are grouped under ``"unknown"`` intentionally — prevents
          cardinality explosion from path-scanning attackers.
        - WebSocket upgrade requests → same logic applies; route may or may
          not be populated depending on the Starlette version.
    """
    route = request.scope.get("route")
    if route is not None:
        # FastAPI/Starlette Route objects expose their template as .path
        path: str | None = getattr(route, "path", None)
        if path is not None:
            return path
    return "unknown"


# ── Lazy instrument helpers ───────────────────────────────────────────────────


def _get_duration_histogram(meter_name: str) -> Any:
    """
    Return (and lazily create) the ``http.server.request.duration`` Histogram.

    Args:
        meter_name: OTel meter name used to scope the instrument.

    Returns:
        An ``opentelemetry.metrics.Histogram`` instance.
    """
    key = f"{meter_name}:http.server.request.duration"
    instrument = _instruments.get(key)
    if instrument is None:
        from opentelemetry import metrics as otel_metrics

        meter = otel_metrics.get_meter(meter_name)
        instrument = meter.create_histogram(
            name="http.server.request.duration",
            description="Duration of HTTP server requests",
            unit="s",
        )
        _instruments[key] = instrument
    return instrument


def _get_active_requests(meter_name: str) -> Any:
    """
    Return (and lazily create) the ``http.server.active_requests`` UpDownCounter.

    Args:
        meter_name: OTel meter name used to scope the instrument.

    Returns:
        An ``opentelemetry.metrics.UpDownCounter`` instance.
    """
    key = f"{meter_name}:http.server.active_requests"
    instrument = _instruments.get(key)
    if instrument is None:
        from opentelemetry import metrics as otel_metrics

        meter = otel_metrics.get_meter(meter_name)
        instrument = meter.create_up_down_counter(
            name="http.server.active_requests",
            description="Number of active in-flight HTTP server requests",
            unit="{request}",
        )
        _instruments[key] = instrument
    return instrument


def _get_body_size_histogram(meter_name: str) -> Any:
    """
    Return (and lazily create) the ``http.server.request.body.size`` Histogram.

    Args:
        meter_name: OTel meter name used to scope the instrument.

    Returns:
        An ``opentelemetry.metrics.Histogram`` instance.
    """
    key = f"{meter_name}:http.server.request.body.size"
    instrument = _instruments.get(key)
    if instrument is None:
        from opentelemetry import metrics as otel_metrics

        meter = otel_metrics.get_meter(meter_name)
        instrument = meter.create_histogram(
            name="http.server.request.body.size",
            description="Size of HTTP request bodies",
            unit="By",
        )
        _instruments[key] = instrument
    return instrument


# ── MetricsMiddleware ─────────────────────────────────────────────────────────


class MetricsMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware that records per-request OTel HTTP server metrics.

    Records three instruments following the OTel HTTP semantic conventions:
    ``http.server.request.duration`` (Histogram), ``http.server.active_requests``
    (UpDownCounter), and ``http.server.request.body.size`` (Histogram).

    All instruments are lazily created on the first request — not at import or
    ``__init__`` time — so they are obtained from the live ``MeterProvider``
    set by ``OtelConfiguration``.

    Recommended position in the middleware stack (inside ``create_varco_app``):

    ``CORS → Error → Tracing → MetricsMiddleware → Logging → RequestContext``

    This ensures:
    - The OTel tracing context is active (set by ``TracingMiddleware``) when
      metrics are recorded.
    - ``/metrics`` and ``/health`` paths are skipped before reaching
      ``RequestLoggingMiddleware``, so access logs don't spam the log.

    Args:
        app:        The ASGI application to wrap.
        meter_name: OTel meter name (instrumentation scope).
                    Default: ``"varco_fastapi.http"``.
        skip_paths: Path prefixes to skip entirely (no metrics recorded).
                    Default: ``frozenset({"/metrics", "/health"})``.

    Thread safety:  ✅ Stateless — each request is independent.  The
                       module-level ``_instruments`` cache is GIL-protected.
    Async safety:   ✅ ``dispatch`` is ``async def``.

    Edge cases:
        - OTel SDK not installed → ``_otel_enabled=False``; middleware is a
          transparent pass-through with zero overhead.
        - No active ``MeterProvider`` → OTel returns a no-op meter; requests
          run normally; metrics are silently discarded.
        - Exceptions from ``call_next`` → ``status_code`` defaults to 500;
          ``request.duration`` is still recorded; exception is re-raised.
        - Streaming responses → timing includes entire body streaming time,
          not just time-to-first-byte.  Acceptable for REST endpoints.

    Example::

        from varco_fastapi.middleware.metrics import MetricsMiddleware
        from fastapi import FastAPI

        app = FastAPI()
        app.add_middleware(
            MetricsMiddleware,
            skip_paths=frozenset({"/metrics", "/health", "/readyz"}),
        )

    📚 Docs:
        - 🔍 OTel HTTP semconv: https://opentelemetry.io/docs/specs/semconv/http/http-metrics/
        - 🔍 Starlette middleware: https://www.starlette.io/middleware/
    """

    def __init__(
        self,
        app: Any,
        *,
        meter_name: str = "varco_fastapi.http",
        skip_paths: frozenset[str] = frozenset({"/metrics", "/health"}),
    ) -> None:
        super().__init__(app)
        self._meter_name = meter_name
        self._skip_paths = skip_paths
        # Check OTel availability once at init — avoids per-request ImportError
        # cost.  Since varco_core mandates opentelemetry-api/sdk as hard deps,
        # this will almost always be True in practice.  The check exists for
        # defensive consistency with TracingMiddleware's pattern.
        self._otel_enabled = _otel_available()
        if not self._otel_enabled:
            _logger.debug(
                "MetricsMiddleware: opentelemetry SDK not installed — metrics disabled."
            )

    async def dispatch(self, request: Request, call_next: Any) -> Response:
        """
        Handle the request, recording OTel metrics around the call.

        Args:
            request:   Incoming HTTP request.
            call_next: Next middleware / route handler callable.

        Returns:
            Response from the downstream handler (unchanged).

        Edge cases:
            - Path matches ``skip_paths`` → pass-through, no metrics.
            - Exception from ``call_next`` → duration recorded with
              ``status_code="500"``, exception re-raised.
            - ``Content-Length`` absent or non-integer → body size skipped.
        """
        if not self._otel_enabled:
            return await call_next(request)

        # Skip paths like /metrics and /health to avoid self-recording
        # phantom traffic from Prometheus scrapers and Kubernetes probes.
        for prefix in self._skip_paths:
            if request.url.path.startswith(prefix):
                return await call_next(request)

        method = request.method

        # Increment active requests counter BEFORE the call.
        # Route attribute is intentionally omitted here — the route is only
        # known after call_next completes (post-routing).  Using "unknown"
        # for the increment and the real route for the decrement would create
        # a permanent dimension mismatch in the TSDB.  Omitting route avoids
        # that mismatch entirely — the counter remains balanced.
        _get_active_requests(self._meter_name).add(1, {"http.request.method": method})

        start = time.perf_counter()
        # Default to 500 so that exceptions (which bypass the response path)
        # still produce a data point with a meaningful status code.
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            raise
        finally:
            elapsed = time.perf_counter() - start

            # Route template is resolved post-routing (in finally), after
            # call_next has given the router a chance to match the path.
            route = _get_route_template(request)

            duration_attrs = {
                "http.request.method": method,
                "http.route": route,
                # Status code as string per OTel HTTP semconv
                "http.response.status_code": str(status_code),
            }
            _get_duration_histogram(self._meter_name).record(
                elapsed, attributes=duration_attrs
            )

            # Decrement active counter — must balance the pre-call increment.
            # Same attribute set as the increment (method only, no route).
            _get_active_requests(self._meter_name).add(
                -1, {"http.request.method": method}
            )

            # Record request body size if Content-Length is present.
            # Many GET requests have no body — skip gracefully if absent.
            content_length = request.headers.get("content-length")
            if content_length is not None:
                try:
                    _get_body_size_histogram(self._meter_name).record(
                        int(content_length),
                        attributes={
                            "http.request.method": method,
                            "http.route": route,
                        },
                    )
                except ValueError:
                    # Non-integer Content-Length header — skip silently.
                    pass


__all__ = ["MetricsMiddleware"]
