"""
varco_fastapi.middleware.tracing
================================
ASGI middleware for distributed tracing and correlation ID propagation.

Reads the ``X-Correlation-ID`` header (or generates a fresh UUID), enters
``correlation_context()`` from ``varco_core.tracing``, and writes the ID to
the response ``X-Correlation-ID`` header.

If the ``opentelemetry`` SDK is installed, also creates a server span per
request with standard HTTP attributes and propagates W3C trace context
(``traceparent`` / ``tracestate`` headers).  Gracefully no-ops when OTel
is not installed — no ``ImportError``, no overhead.

DESIGN: optional OTel over mandatory dependency
    ✅ Works without opentelemetry SDK installed
    ✅ Zero overhead when OTel is disabled (checked once at init, not per-request)
    ✅ Integrates with any OTel-compatible backend (Jaeger, OTLP, Datadog, ...)
    ❌ Custom propagators (Zipkin B3, AWS X-Ray) require manual setup via
       ``opentelemetry.propagate.set_global_textmap()`` before app startup

NOTE: ``TracingMiddleware`` does NOT call ``correlation_context()`` itself —
that is done by ``RequestContextMiddleware`` (which uses the same request ID
as the correlation ID).  ``TracingMiddleware`` only manages the
``X-Correlation-ID`` header and optional OTel spans.

Thread safety:  ✅ Stateless per-request.
Async safety:   ✅ ``dispatch`` is ``async def``.
"""

from __future__ import annotations

import logging

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

from varco_core.tracing import current_correlation_id

_logger = logging.getLogger(__name__)

_CORRELATION_ID_HEADER = "X-Correlation-ID"
_TRACEPARENT_HEADER = "traceparent"
_TRACESTATE_HEADER = "tracestate"


def _otel_available() -> bool:
    """Return True if the opentelemetry SDK is installed."""
    try:
        import opentelemetry.trace  # noqa: F401

        return True
    except ImportError:
        return False


class TracingMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware for distributed tracing.

    Responsibilities:
    - Set the ``X-Correlation-ID`` response header.
    - Optionally create an OpenTelemetry server span per request.
    - Propagate W3C trace context from/to ``traceparent`` headers.

    Args:
        app:                The ASGI application to wrap.
        enable_otel:        Enable OpenTelemetry integration.  Default: ``True``
                            (auto-detects SDK availability; silently skips if absent).
        tracer_name:        OTel tracer name.  Default: ``"varco_fastapi"``.
        record_request_body: Include request body as span attribute.
                             Default: ``False`` — avoid PII in traces.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ ``dispatch`` is ``async def``.

    Edge cases:
        - If OTel SDK is not installed, this middleware is a no-op beyond
          copying the ``X-Correlation-ID`` header.
        - 5xx responses set the OTel span status to ERROR.
        - Exceptions are recorded as span events and then re-raised.
    """

    def __init__(
        self,
        app,
        *,
        enable_otel: bool = True,
        tracer_name: str = "varco_fastapi",
        record_request_body: bool = False,
    ) -> None:
        super().__init__(app)
        self._tracer_name = tracer_name
        self._record_request_body = record_request_body
        # Check OTel availability once at init — avoids per-request ImportError cost
        self._otel_enabled = enable_otel and _otel_available()
        if enable_otel and not self._otel_enabled:
            _logger.debug(
                "TracingMiddleware: opentelemetry SDK not installed — OTel disabled."
            )

    async def dispatch(self, request: Request, call_next) -> Response:
        """
        Handle the request, optionally wrapping it in an OTel span.

        Args:
            request:   Incoming HTTP request.
            call_next: Next middleware / route handler callable.

        Returns:
            Response with ``X-Correlation-ID`` header added.
        """
        if self._otel_enabled:
            return await self._dispatch_with_otel(request, call_next)
        return await self._dispatch_plain(request, call_next)

    async def _dispatch_plain(self, request: Request, call_next) -> Response:
        """Dispatch without OTel — just pass through."""
        response = await call_next(request)
        cid = current_correlation_id()
        if cid:
            response.headers[_CORRELATION_ID_HEADER] = cid
        return response

    async def _dispatch_with_otel(self, request: Request, call_next) -> Response:
        """Dispatch with OTel span wrapping."""
        import opentelemetry.trace as otel_trace
        from opentelemetry.propagate import extract
        from opentelemetry.trace import StatusCode

        tracer = otel_trace.get_tracer(self._tracer_name)

        # Extract W3C trace context from incoming headers
        carrier = dict(request.headers)
        context = extract(carrier)

        span_name = f"{request.method} {request.url.path}"
        with tracer.start_as_current_span(
            span_name,
            context=context,
            kind=otel_trace.SpanKind.SERVER,
        ) as span:
            # Standard HTTP semantic conventions
            span.set_attribute("http.method", request.method)
            span.set_attribute("http.url", str(request.url))
            span.set_attribute("http.scheme", request.url.scheme)
            span.set_attribute("http.host", request.url.hostname or "")
            span.set_attribute("http.route", request.url.path)

            cid = current_correlation_id()
            if cid:
                span.set_attribute("varco.correlation_id", cid)

            # Read user_id from auth context if available
            try:
                from varco_fastapi.context import get_auth_context_or_none

                ctx = get_auth_context_or_none()
                if ctx and ctx.user_id:
                    span.set_attribute("varco.user_id", ctx.user_id)
                    tenant_id = ctx.metadata.get("tenant_id")
                    if tenant_id:
                        span.set_attribute("varco.tenant_id", str(tenant_id))
            except Exception:  # noqa: BLE001
                pass  # Auth context not set yet — this is before RequestContextMiddleware

            try:
                response = await call_next(request)
            except Exception as exc:
                span.record_exception(exc)
                span.set_status(StatusCode.ERROR, str(exc))
                raise

            span.set_attribute("http.status_code", response.status_code)
            if response.status_code >= 500:
                span.set_status(StatusCode.ERROR, f"HTTP {response.status_code}")

        if cid:
            response.headers[_CORRELATION_ID_HEADER] = cid
        return response


__all__ = ["TracingMiddleware"]
