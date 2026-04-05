"""
varco_fastapi.client.middleware
================================
Client-side HTTP middleware pipeline.

Each middleware in the stack is an async callable that receives a
``PreparedRequest`` and a ``next`` callable, and returns an ``httpx.Response``.
This is the "chain of responsibility" pattern — identical to ASGI middleware
but for outbound requests.

Middleware order (applied left-to-right, response right-to-left):
    OTelClientMiddleware → CorrelationIdMiddleware → JwtMiddleware
    → AuthForwardMiddleware → HeadersMiddleware → RetryMiddleware
    → TimeoutMiddleware → [httpx sends the request]

DESIGN: middleware pipeline over httpx event hooks
    ✅ Composable — multiple middleware combine cleanly without hooks interference
    ✅ Testable — each middleware can be tested in isolation with a mock next()
    ✅ Consistent — same pattern as server-side ASGI middleware
    ✅ Cancellable — RetryMiddleware wraps the entire pipeline including next()
    ❌ More explicit setup than auto-instrumentation (httpx event hooks)

Thread safety:  ✅ Middleware instances are stateless after construction.
Async safety:   ✅ All __call__ methods are async-safe.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import httpx

logger = logging.getLogger(__name__)

# Type alias for the "next" callable in the pipeline
NextMiddleware = Callable[["PreparedRequest"], Awaitable[httpx.Response]]


# ── PreparedRequest ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PreparedRequest:
    """
    Immutable snapshot of an outbound HTTP request before it is sent.

    Each middleware in the pipeline receives a ``PreparedRequest`` and may return
    a new one (via ``with_*`` methods) before passing it to ``next``.  This ensures
    the pipeline is fully composable without any shared mutable state.

    Attributes:
        method:       HTTP method (uppercase).
        url:          Full URL string.
        headers:      Dict of request headers.
        body:         Request body bytes, or ``None`` for GET/DELETE.
        query_params: Query string parameters as a dict.

    Thread safety:  ✅ Frozen dataclass — immutable.
    Async safety:   ✅ Pure value object.
    """

    method: str
    url: str
    headers: dict[str, str] = field(default_factory=dict, compare=False, hash=False)
    body: bytes | None = None
    query_params: dict[str, str] = field(
        default_factory=dict, compare=False, hash=False
    )

    def with_header(self, key: str, value: str) -> PreparedRequest:
        """
        Return a new ``PreparedRequest`` with the given header added or replaced.

        Args:
            key:   Header name (case-sensitive as stored, case-insensitive in HTTP).
            value: Header value.

        Returns:
            New ``PreparedRequest`` with the updated headers dict.
        """
        return PreparedRequest(
            method=self.method,
            url=self.url,
            headers={**self.headers, key: value},
            body=self.body,
            query_params=self.query_params,
        )

    def with_query(self, key: str, value: str) -> PreparedRequest:
        """
        Return a new ``PreparedRequest`` with the given query parameter added.

        Args:
            key:   Query parameter name.
            value: Query parameter value.

        Returns:
            New ``PreparedRequest`` with the updated query_params dict.
        """
        return PreparedRequest(
            method=self.method,
            url=self.url,
            headers=self.headers,
            body=self.body,
            query_params={**self.query_params, key: value},
        )

    def with_url(self, url: str) -> PreparedRequest:
        """
        Return a new ``PreparedRequest`` with the URL replaced.

        Args:
            url: New full URL string.

        Returns:
            New ``PreparedRequest`` with the updated URL.
        """
        return PreparedRequest(
            method=self.method,
            url=url,
            headers=self.headers,
            body=self.body,
            query_params=self.query_params,
        )


# ── AbstractClientMiddleware ───────────────────────────────────────────────────


class AbstractClientMiddleware(ABC):
    """
    Abstract base for outbound HTTP request middleware.

    Implementations intercept the request, optionally modify it, call ``next``,
    and optionally modify the response.  The pipeline is driven by the client's
    ``_request()`` method.

    Thread safety:  ✅ Subclasses must document their own safety contract.
    Async safety:   ✅ __call__ is always async.
    """

    @abstractmethod
    async def __call__(
        self,
        request: PreparedRequest,
        next: NextMiddleware,
    ) -> httpx.Response:
        """
        Process a request and delegate to the next middleware.

        Args:
            request: The outbound request to process.
            next:    The next middleware in the pipeline (or the actual sender).

        Returns:
            The HTTP response.
        """
        ...  # pragma: no cover


# ── HeadersMiddleware ─────────────────────────────────────────────────────────


class HeadersMiddleware(AbstractClientMiddleware):
    """
    Inject static or dynamic headers into every outbound request.

    Supports both static strings and dynamic callables (sync or async).

    Args:
        headers: Dict mapping header name to a string or a callable returning
                 a string (sync) or awaitable string (async).

    Usage::

        # Static headers
        HeadersMiddleware({"X-Service-Name": "order-svc"})

        # Dynamic header (e.g. fresh request ID per call)
        HeadersMiddleware({"X-Request-ID": lambda: str(uuid4())})

    Thread safety:  ✅ Static headers are immutable.
    Async safety:   ✅ Dynamic callables are awaited if they return a coroutine.
    """

    def __init__(
        self,
        headers: dict[str, str | Callable[[], str | Any]],
    ) -> None:
        self._headers = headers

    async def __call__(
        self,
        request: PreparedRequest,
        next: NextMiddleware,
    ) -> httpx.Response:
        """Resolve all headers (sync or async) and inject them."""
        import asyncio

        extra: dict[str, str] = {}
        for key, value in self._headers.items():
            if callable(value):
                result = value()
                if asyncio.iscoroutine(result):
                    result = await result
                extra[key] = str(result)
            else:
                extra[key] = str(value)
        # Inject: extra headers override existing, others preserved
        for key, value in extra.items():
            request = request.with_header(key, value)
        return await next(request)


# ── CorrelationIdMiddleware ───────────────────────────────────────────────────


class CorrelationIdMiddleware(AbstractClientMiddleware):
    """
    Propagate the X-Correlation-ID from the current ContextVar to outbound requests.

    Reads ``current_correlation_id()`` from ``varco_core.tracing`` and injects
    it as a header.  No-ops if no correlation ID is set in the current context.

    Thread safety:  ✅ ContextVar read is task-local.
    Async safety:   ✅ No I/O; pure ContextVar read.
    """

    async def __call__(
        self,
        request: PreparedRequest,
        next: NextMiddleware,
    ) -> httpx.Response:
        """Inject X-Correlation-ID if one is set in the current context."""
        try:
            from varco_core.tracing import current_correlation_id

            cid = current_correlation_id()
            if cid:
                request = request.with_header("X-Correlation-ID", cid)
        except ImportError:
            pass
        return await next(request)


# ── AuthForwardMiddleware ─────────────────────────────────────────────────────


class AuthForwardMiddleware(AbstractClientMiddleware):
    """
    Forward the incoming request's Bearer token to outbound requests.

    Reads ``request_token_var`` from ``varco_fastapi.context`` and injects the
    raw JWT as ``Authorization: Bearer <token>``.  Useful for service-to-service
    calls where the downstream service should receive the same identity as the
    original request.

    DESIGN: ContextVar over explicit token passing
        ✅ No extra plumbing — token flows through ContextVar automatically
        ✅ Works in any code path (service layer, job callbacks, etc.)
        ❌ Requires the request context to be set up (i.e., inside a web request)
           — outside a web request, the ContextVar is None and this is a no-op

    Thread safety:  ✅ ContextVar read is task-local.
    Async safety:   ✅ No I/O.
    """

    async def __call__(
        self,
        request: PreparedRequest,
        next: NextMiddleware,
    ) -> httpx.Response:
        """Inject Authorization header from the current request token."""
        try:
            from varco_fastapi.context import request_token_var

            token = request_token_var.get(None)
            if token:
                request = request.with_header("Authorization", f"Bearer {token}")
        except ImportError:
            pass
        return await next(request)


# ── JwtMiddleware ─────────────────────────────────────────────────────────────


class JwtMiddleware(AbstractClientMiddleware):
    """
    Sign and inject a Bearer JWT; caches the token until (expiry - 30s).

    Uses ``JwtAuthority.token()`` from ``varco_core.authority`` to sign a
    short-lived JWT for service-to-service authentication.

    Args:
        authority:   ``JwtAuthority`` that signs tokens.
        audience:    JWT ``aud`` claim (default: ``"api"``).
        expires_in:  Token lifetime (default: 5 minutes).
        subject:     JWT ``sub`` claim (default: ``"svc"``).
        extra_claims: Additional claims to add to the token payload.

    Thread safety:  ⚠️ Conditional — the token cache (_cached_token) is
                       only mutated inside an asyncio.Lock (lazy).
    Async safety:   ✅ Lock is created lazily inside the event loop.

    Edge cases:
        - Token near expiry → refreshed 30s early to avoid races
        - ``authority.sign()`` raises → propagates (caller sees the error)
    """

    # Margin before expiry to refresh the token — avoids serving an expired token
    _REFRESH_MARGIN_SECONDS: int = 30

    def __init__(
        self,
        authority: Any,
        *,
        audience: str = "api",
        expires_in: Any | None = None,
        subject: str = "svc",
        extra_claims: dict[str, Any] | None = None,
    ) -> None:
        from datetime import timedelta

        self._authority = authority
        self._audience = audience
        self._expires_in = expires_in or timedelta(minutes=5)
        self._subject = subject
        self._extra_claims = extra_claims or {}
        self._cached_token: str | None = None
        self._token_expiry: float | None = None
        # Lazy lock — created inside the running event loop
        self._lock: Any | None = None

    def _get_lock(self) -> Any:
        """Lazily create the asyncio.Lock inside the running event loop."""
        import asyncio

        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def __call__(
        self,
        request: PreparedRequest,
        next: NextMiddleware,
    ) -> httpx.Response:
        """Sign or reuse a cached JWT and inject as Authorization: Bearer."""
        import time

        async with self._get_lock():
            now = time.monotonic()
            if (
                self._cached_token is None
                or self._token_expiry is None
                or now >= self._token_expiry - self._REFRESH_MARGIN_SECONDS
            ):
                self._cached_token = await self._mint_token()
                # Compute expiry from expires_in
                from datetime import timedelta

                if isinstance(self._expires_in, timedelta):
                    self._token_expiry = now + self._expires_in.total_seconds()
                else:
                    self._token_expiry = now + float(self._expires_in)
            token = self._cached_token

        request = request.with_header("Authorization", f"Bearer {token}")
        return await next(request)

    async def _mint_token(self) -> str:
        """Sign a new JWT using the authority."""
        builder = (
            self._authority.token().subject(self._subject).expires_in(self._expires_in)
        )
        if self._audience:
            builder = builder.audience(self._audience)
        for key, value in self._extra_claims.items():
            builder = builder.claim(key, value)
        return self._authority.sign(builder)


# ── RetryMiddleware ───────────────────────────────────────────────────────────


class RetryMiddleware(AbstractClientMiddleware):
    """
    Retry failed HTTP requests using ``RetryPolicy`` from ``varco_core.resilience``.

    Retries on network errors (httpx.TransportError) and on configurable
    HTTP status codes (default: 502, 503, 504).

    Args:
        policy:            ``RetryPolicy`` from varco_core (max_attempts, delays).
        retryable_statuses: HTTP status codes that trigger a retry.

    DESIGN: client middleware retry over varco_core @retry decorator
        ✅ HTTP-aware — can retry on specific status codes (not just exceptions)
        ✅ Retries the full middleware pipeline (re-applies auth headers, etc.)
        ❌ RetryPolicy from varco_core used for configuration only — we drive
           the loop manually to also handle HTTP status code-based retries

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ asyncio.sleep used for backoff.
    """

    def __init__(
        self,
        policy: Any,
        *,
        retryable_statuses: frozenset[int] | None = None,
    ) -> None:
        self._policy = policy
        self._retryable_statuses = retryable_statuses or frozenset({502, 503, 504})

    async def __call__(
        self,
        request: PreparedRequest,
        next: NextMiddleware,
    ) -> httpx.Response:
        """Retry the request on network errors or retryable status codes."""
        import asyncio
        import random

        max_attempts = getattr(self._policy, "max_attempts", 3)
        base_delay = getattr(self._policy, "base_delay", 0.5)
        max_delay = getattr(self._policy, "max_delay", 30.0)

        last_exc: Exception | None = None
        for attempt in range(max_attempts):
            try:
                response = await next(request)
                if response.status_code not in self._retryable_statuses:
                    return response
                # Retryable status code — treat as an error
                if attempt == max_attempts - 1:
                    return response  # Final attempt — return the response
            except httpx.TransportError as exc:
                last_exc = exc
                if attempt == max_attempts - 1:
                    raise

            # Exponential backoff with jitter
            delay = min(base_delay * (2**attempt) + random.uniform(0, 0.1), max_delay)
            await asyncio.sleep(delay)

        if last_exc is not None:
            raise last_exc
        # Unreachable — but type checker needs it
        raise RuntimeError("RetryMiddleware: max_attempts reached without returning")


# ── LoggingMiddleware ─────────────────────────────────────────────────────────


class LoggingMiddleware(AbstractClientMiddleware):
    """
    Log outbound request and inbound response at DEBUG level.

    Logs method, URL, status code, and duration.  Does NOT log request/response
    bodies to avoid leaking PII.

    Thread safety:  ✅ Stateless — uses module-level logger.
    Async safety:   ✅ time.monotonic() is safe from async context.
    """

    async def __call__(
        self,
        request: PreparedRequest,
        next: NextMiddleware,
    ) -> httpx.Response:
        """Log the request, call next, and log the response."""
        import time

        start = time.monotonic()
        logger.debug("→ %s %s", request.method, request.url)
        try:
            response = await next(request)
            duration_ms = (time.monotonic() - start) * 1000
            logger.debug(
                "← %s %s %d (%.1fms)",
                request.method,
                request.url,
                response.status_code,
                duration_ms,
            )
            return response
        except Exception as exc:
            duration_ms = (time.monotonic() - start) * 1000
            logger.debug(
                "✗ %s %s raised %s (%.1fms)",
                request.method,
                request.url,
                type(exc).__name__,
                duration_ms,
            )
            raise


# ── TimeoutMiddleware ─────────────────────────────────────────────────────────


class TimeoutMiddleware(AbstractClientMiddleware):
    """
    Apply a per-request timeout that overrides the client-level timeout.

    Args:
        timeout: Timeout in seconds.  Raises ``asyncio.TimeoutError`` if exceeded.

    DESIGN: middleware timeout over httpx.Timeout parameter
        ✅ Per-endpoint overrides without creating a new client
        ✅ Works uniformly with the retry middleware (retry sees TimeoutError)
        ❌ asyncio.wait_for cancels the coroutine — httpx may not close the connection
           immediately.  Acceptable for short-lived clients.

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ Uses asyncio.wait_for.
    """

    def __init__(self, timeout: float) -> None:
        self._timeout = timeout

    async def __call__(
        self,
        request: PreparedRequest,
        next: NextMiddleware,
    ) -> httpx.Response:
        """Apply timeout to the request."""
        import asyncio

        return await asyncio.wait_for(next(request), timeout=self._timeout)


# ── OTelClientMiddleware ──────────────────────────────────────────────────────


class OTelClientMiddleware(AbstractClientMiddleware):
    """
    OpenTelemetry instrumentation for outbound HTTP requests.

    Creates a client span per request with HTTP semantic attributes and propagates
    W3C trace context (``traceparent`` / ``tracestate``) to downstream services.

    Gracefully no-ops if the ``opentelemetry`` SDK is not installed.

    Args:
        tracer_name:         OpenTelemetry tracer name (default: ``"varco_fastapi.client"``).
        record_request_body: If ``True``, add the request body as a span attribute.
                             Careful with PII!
        record_response_body: If ``True``, add the response body as a span attribute.

    DESIGN: span-per-request over event hooks
        ✅ Full attribute control — semantic conventions applied explicitly
        ✅ Propagation to downstream services via header injection
        ✅ Composable — sits in the pipeline with other middleware
        ❌ Manual setup required — not auto-instrumented like ``opentelemetry-instrument``

    Thread safety:  ✅ OTel tracer is thread/task-safe by design.
    Async safety:   ✅ Spans are safe across async boundaries.
    """

    def __init__(
        self,
        *,
        tracer_name: str = "varco_fastapi.client",
        record_request_body: bool = False,
        record_response_body: bool = False,
    ) -> None:
        self._tracer_name = tracer_name
        self._record_request_body = record_request_body
        self._record_response_body = record_response_body
        # Check once at init — avoids per-request import overhead
        self._otel_available = self._check_otel()

    @staticmethod
    def _check_otel() -> bool:
        """Return True if the opentelemetry SDK is importable."""
        try:
            import opentelemetry.trace  # noqa: F401

            return True
        except ImportError:
            return False

    async def __call__(
        self,
        request: PreparedRequest,
        next: NextMiddleware,
    ) -> httpx.Response:
        """Create a client span and propagate trace context to the request."""
        if not self._otel_available:
            return await next(request)

        try:
            from opentelemetry import propagate, trace
            from opentelemetry.trace import Status, StatusCode

            tracer = trace.get_tracer(self._tracer_name)
            # Use URL path as span name — avoids high-cardinality from IDs in path
            from urllib.parse import urlparse

            parsed = urlparse(request.url)
            span_name = f"{request.method} {parsed.path}"

            with tracer.start_as_current_span(span_name) as span:
                # Inject trace context into outbound headers for propagation
                carrier: dict[str, str] = {}
                propagate.inject(carrier)
                for key, value in carrier.items():
                    request = request.with_header(key, value)

                span.set_attribute("http.method", request.method)
                span.set_attribute("http.url", request.url)

                try:
                    response = await next(request)
                    span.set_attribute("http.status_code", response.status_code)
                    if response.status_code >= 500:
                        span.set_status(Status(StatusCode.ERROR))
                    return response
                except Exception as exc:
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR))
                    raise
        except Exception:
            # OTel must never crash the request
            return await next(request)


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "PreparedRequest",
    "AbstractClientMiddleware",
    "HeadersMiddleware",
    "CorrelationIdMiddleware",
    "AuthForwardMiddleware",
    "JwtMiddleware",
    "RetryMiddleware",
    "LoggingMiddleware",
    "TimeoutMiddleware",
    "OTelClientMiddleware",
]
