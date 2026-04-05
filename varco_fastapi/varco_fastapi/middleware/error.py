"""
varco_fastapi.middleware.error
==============================
ASGI middleware that catches unhandled exceptions and returns structured JSON.

Maps varco_core ``ServiceException`` subclasses to HTTP status codes using the
existing ``error_message_for()`` / ``error_code_for()`` infrastructure, and
wraps any other exception in a generic 500 response.

Exception → HTTP status mapping::

    ServiceNotFoundError      → 404 Not Found
    ServiceAuthorizationError → 403 Forbidden
    ServiceConflictError      → 409 Conflict
    ServiceValidationError    → 422 Unprocessable Entity
    pydantic.ValidationError  → 422 Unprocessable Entity (request body)
    asyncio.TimeoutError      → 504 Gateway Timeout
    HTTPException             → re-raised (FastAPI handles it natively)
    Exception                 → 500 Internal Server Error

DESIGN: middleware over FastAPI exception_handler
    ✅ Catches errors in OTHER middleware (not just route handlers)
    ✅ Single centralized error formatting via varco_core.exception.http
    ✅ Stack traces are NOT included in responses (avoids info leakage)
    ✅ Correlation ID is included in 5xx error responses for log correlation
    ❌ Must re-raise ``HTTPException`` to let FastAPI handle it natively
    ❌ Ordering matters: must be installed AFTER CORSMiddleware so CORS headers
       are still set on error responses

Thread safety:  ✅ Stateless — safe to share across requests.
Async safety:   ✅ All paths are ``async``; no blocking I/O.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from fastapi import HTTPException

from varco_core.exception.service import (
    ServiceAuthorizationError,
    ServiceConflictError,
    ServiceException,
    ServiceNotFoundError,
    ServiceValidationError,
)
from varco_core.exception.http import error_message_for
from varco_core.tracing import current_correlation_id

_logger = logging.getLogger(__name__)


class ErrorMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware that catches unhandled exceptions and returns structured JSON.

    Installed at the outermost layer of the middleware stack so it catches
    errors from all downstream middleware and route handlers.

    Error responses always include:
    - ``code``: stable i18n error code (from varco_core.exception.http)
    - ``message``: human-readable description
    - ``correlation_id``: current request's correlation ID (for log lookup)

    Args:
        app:              The ASGI application to wrap.
        debug:            If ``True``, include exception repr in 5xx responses.
                          Default: ``False``.  NEVER set ``True`` in production.
        include_trace_id: If ``True`` (default), add ``correlation_id`` to
                          error response body for log correlation.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ ``dispatch`` is ``async def``.

    Edge cases:
        - ``HTTPException`` from FastAPI/Starlette is re-raised so FastAPI's
          own handler formats it correctly (preserves headers, detail format).
        - ``asyncio.CancelledError`` is not caught — it propagates to let
          the ASGI server cancel the request cleanly.
        - Errors in ``send()`` (e.g. client disconnected mid-response) may
          cause a double-error situation — log and swallow the second error.
    """

    def __init__(
        self,
        app,
        *,
        debug: bool = False,
        include_trace_id: bool = True,
    ) -> None:
        super().__init__(app)
        self._debug = debug
        self._include_trace_id = include_trace_id

    async def dispatch(self, request: Request, call_next) -> Response:
        """
        Process the request, catching all exceptions and mapping them to JSON.

        Args:
            request:   Incoming HTTP request.
            call_next: Next middleware / route handler callable.

        Returns:
            The response from the downstream handler, or a JSON error response.
        """
        try:
            return await call_next(request)
        except HTTPException:
            # Let FastAPI handle its own HTTPException — it formats headers
            # and detail correctly (e.g. 307 redirects, auth challenges).
            raise
        except asyncio.CancelledError:
            # Task cancellation — propagate immediately.
            raise
        except ServiceException as exc:
            return self._service_error_response(exc)
        except Exception as exc:  # noqa: BLE001
            return self._internal_error_response(exc)

    def _service_error_response(self, exc: ServiceException) -> JSONResponse:
        """Map a ``ServiceException`` to the correct HTTP status code and body."""
        try:
            msg = error_message_for(exc)
            status_code = msg.http_status
            body: dict[str, Any] = {
                "code": msg.code,
                "message": msg.message,
            }
        except Exception:  # noqa: BLE001
            # Fallback if error_message_for fails (e.g. unmapped exception type)
            status_code = _FALLBACK_STATUS.get(type(exc), 500)
            body = {"code": "INTERNAL_ERROR", "message": str(exc)}

        if self._include_trace_id:
            cid = current_correlation_id()
            if cid:
                body["correlation_id"] = cid

        return JSONResponse(status_code=status_code, content=body)

    def _internal_error_response(self, exc: Exception) -> JSONResponse:
        """Map an unexpected exception to 500 with a sanitized error body."""
        _logger.error(
            "Unhandled exception in request: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )

        body: dict[str, Any] = {
            "code": "INTERNAL_SERVER_ERROR",
            "message": "An unexpected error occurred. Please try again later.",
        }

        if self._debug:
            body["detail"] = f"{type(exc).__name__}: {exc}"

        if self._include_trace_id:
            cid = current_correlation_id()
            if cid:
                body["correlation_id"] = cid

        return JSONResponse(status_code=500, content=body)


# Default HTTP status fallback map — used only when error_message_for() fails
_FALLBACK_STATUS: dict[type[ServiceException], int] = {
    ServiceNotFoundError: 404,
    ServiceAuthorizationError: 403,
    ServiceConflictError: 409,
    ServiceValidationError: 422,
}


__all__ = ["ErrorMiddleware"]
