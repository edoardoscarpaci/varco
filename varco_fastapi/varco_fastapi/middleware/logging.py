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
    ❌ No request/response body logging (PII risk) — a subclass that adds
       one now has something to call: ``redactor=`` (Plan 040 / S21,
       §D-S21-logging) — see ``RequestLoggingMiddleware``'s docstring.

Thread safety:  ✅ Stateless — each request is logged independently.
Async safety:   ✅ ``dispatch`` is ``async def``.
"""

from __future__ import annotations

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp
from varco_core.redaction import Redactor, redact_mapping

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
        redactor:       Plan 040 / S21, §D-S21-logging. A ``Redactor`` (e.g.
                        ``PolicyRedactor()``) applied to the assembled
                        ``log_entry`` before it is logged. Default ``None``
                        — the log entry is byte-identical to pre-3.2. Today's
                        entry carries no user data (``method``/``path``
                        excludes the query string, no headers, no body), so
                        this is sized as a prophylactic for the subclass this
                        middleware's own docstring invites — a body/header/
                        full-URL-logging subclass finally has something to
                        call. Does NOT fall back to ``default_redactor()``
                        when omitted — a per-request dict walk that can
                        never find anything sensitive is pure hot-path cost
                        for an explicit opt-in feature.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ ``dispatch`` is ``async def``.

    Edge cases:
        - Exceptions in ``call_next()`` are caught, logged, and re-raised so
          the log entry still records the failure.
        - Paths matching any prefix in ``skip_paths`` are not logged (useful
          for health check spam).
        - ``redact_query_string()`` (``varco_core.redaction``) is the
          companion helper for a subclass that logs a full URL rather than
          just ``request.url.path``.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        logger: logging.Logger | None = None,
        log_level: int = logging.INFO,
        error_level: int = logging.ERROR,
        skip_paths: set[str] | None = None,
        redactor: Redactor | None = None,
    ) -> None:
        super().__init__(app)
        self._log = logger or _logger
        self._log_level = log_level
        self._error_level = error_level
        self._skip_paths = skip_paths or set()
        self._redactor = redactor

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
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
                    get_auth_context_or_none,
                    get_request_id,
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

            if self._redactor is not None:
                log_entry = redact_mapping(log_entry, self._redactor)

            level = self._error_level if status_code >= 500 else self._log_level
            # DESIGN: pass log_entry as the `msg` itself, not as a "%s" arg.
            #   ✅ stdlib logging special-cases a SINGLE dict positional arg
            #      by collapsing LogRecord.args to that dict (for %(key)s
            #      style formatting) rather than keeping it as a `(dict,)`
            #      tuple — `"%s" % log_entry` would still render correctly,
            #      but `record.args` would then be the dict, not a tuple, an
            #      easy-to-miss surprise for any handler/test introspecting
            #      `record.args`. Passing the dict as `msg` sidesteps this
            #      entirely: `record.msg` is the dict object itself (for a
            #      JSON-aware handler), and `record.getMessage()`/`str()`
            #      still renders identically for any plain-text handler.
            self._log.log(level, log_entry)


__all__ = ["RequestLoggingMiddleware"]
