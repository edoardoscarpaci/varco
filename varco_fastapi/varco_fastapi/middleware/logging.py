"""
varco_fastapi.middleware.logging
================================
ASGI middleware for structured request/response logging.

Logs one line per request at INFO level with a structured dict that is
easy to parse by log aggregators (Loki, CloudWatch, Datadog, etc.):

    {
        "method": "POST", "path": "/orders/", "status": 201,
        "duration_ms": 42.3, "request_id": "...", "user_id": "usr_123",
        "tenant_id": "t1"
    }

Reads ``request_id`` and auth context from ContextVars set by
``RequestContextMiddleware``.  If those vars are not set (middleware not
installed), falls back gracefully.

DESIGN: single-line structured log over two lines (request + response)
    ✅ One log line per request — easy to grep and correlate
    ✅ ``duration_ms`` in the same entry — no join query needed
    ✅ Uses stdlib ``logging`` — integrates with any handler (file, JSON, OTel)
    ❌ No request/response body logging (PII risk) — add a subclass if needed

Thread safety:  ✅ Stateless — each request is logged independently.
Async safety:   ✅ ``dispatch`` is ``async def``.
"""

from __future__ import annotations

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

_logger = logging.getLogger("varco_fastapi.access")


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware that logs request/response at INFO level.

    Args:
        app:            The ASGI application to wrap.
        logger:         Custom ``logging.Logger`` to write to.
                        Default: ``logging.getLogger("varco_fastapi.access")``.
        log_level:      Logging level for successful requests.  Default: ``INFO``.
        error_level:    Logging level for 5xx responses.  Default: ``ERROR``.
        skip_paths:     Set of path prefixes to skip (e.g. ``{"/health", "/metrics"}``).

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ ``dispatch`` is ``async def``.

    Edge cases:
        - Exceptions in ``call_next()`` are caught, logged, and re-raised so
          the log entry still records the failure.
        - Paths matching any prefix in ``skip_paths`` are not logged (useful
          for health check spam).
    """

    def __init__(
        self,
        app,
        *,
        logger: logging.Logger | None = None,
        log_level: int = logging.INFO,
        error_level: int = logging.ERROR,
        skip_paths: set[str] | None = None,
    ) -> None:
        super().__init__(app)
        self._log = logger or _logger
        self._log_level = log_level
        self._error_level = error_level
        self._skip_paths = skip_paths or set()

    async def dispatch(self, request: Request, call_next) -> Response:
        """
        Log the request and response.

        Args:
            request:   Incoming HTTP request.
            call_next: Next middleware / route handler callable.

        Returns:
            Response from the downstream handler (unchanged).
        """
        # Skip logging for health checks / metrics
        for prefix in self._skip_paths:
            if request.url.path.startswith(prefix):
                return await call_next(request)

        start = time.perf_counter()
        status_code = 500
        try:
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception:
            raise
        finally:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)

            # Read from ContextVars (set by RequestContextMiddleware)
            try:
                from varco_fastapi.context import (
                    get_request_id,
                    get_auth_context_or_none,
                )

                request_id = get_request_id()
                ctx = get_auth_context_or_none()
                user_id = ctx.user_id if ctx else None
                tenant_id = ctx.metadata.get("tenant_id") if ctx else None
            except Exception:  # noqa: BLE001
                request_id = None
                user_id = None
                tenant_id = None

            log_entry = {
                "method": request.method,
                "path": request.url.path,
                "status": status_code,
                "duration_ms": duration_ms,
            }
            if request_id:
                log_entry["request_id"] = request_id
            if user_id:
                log_entry["user_id"] = user_id
            if tenant_id:
                log_entry["tenant_id"] = str(tenant_id)

            level = self._error_level if status_code >= 500 else self._log_level
            self._log.log(level, "%s", log_entry)


__all__ = ["RequestLoggingMiddleware"]
