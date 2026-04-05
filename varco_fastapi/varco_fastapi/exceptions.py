"""
varco_fastapi.exceptions
========================
Exception registry and FastAPI exception handler helpers.

Provides ``add_exception_handlers(app)`` which registers all varco_core
``ServiceException`` subclasses as FastAPI exception handlers with the correct
HTTP status codes and structured JSON bodies.

Use this instead of (or in addition to) ``ErrorMiddleware`` when you want FastAPI
to manage exception → response mapping via its native ``exception_handler``
mechanism (useful for OpenAPI schema generation and HTTP/2 streaming).

Comparison with ErrorMiddleware:
- ``ErrorMiddleware`` catches errors from ALL middleware (not just route handlers).
- ``add_exception_handlers`` only catches errors in route handlers and dependencies.
- Use BOTH for comprehensive coverage — handlers for known exceptions in routes,
  middleware as the catch-all.

Usage::

    from fastapi import FastAPI
    from varco_fastapi.exceptions import add_exception_handlers

    app = FastAPI()
    add_exception_handlers(app)
    # Now ServiceNotFoundError → 404, ServiceAuthorizationError → 403, etc.

Thread safety:  ✅ Registration happens at startup; handlers are stateless.
Async safety:   ✅ All exception handlers are ``async def``.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from varco_core.exception.http import error_message_for
from varco_core.exception.service import (
    ServiceAuthorizationError,
    ServiceConflictError,
    ServiceException,
    ServiceNotFoundError,
    ServiceValidationError,
)
from varco_core.tracing import current_correlation_id

_logger = logging.getLogger(__name__)


def _make_error_response(exc: ServiceException) -> JSONResponse:
    """
    Build a structured JSON error response from a ``ServiceException``.

    Args:
        exc: The service exception to format.

    Returns:
        A ``JSONResponse`` with the correct HTTP status and body.
    """
    try:
        msg = error_message_for(exc)
        status_code = msg.http_status
        body: dict[str, Any] = {
            "code": msg.code,
            "message": msg.message,
        }
    except Exception:  # noqa: BLE001
        # Fallback for exceptions not registered with error_code_for
        status_code = _FALLBACK_STATUS.get(type(exc).__mro__[0], 500)
        body = {"code": "SERVICE_ERROR", "message": str(exc)}

    cid = current_correlation_id()
    if cid:
        body["correlation_id"] = cid

    return JSONResponse(status_code=status_code, content=body)


_FALLBACK_STATUS: dict[type, int] = {
    ServiceNotFoundError: 404,
    ServiceAuthorizationError: 403,
    ServiceConflictError: 409,
    ServiceValidationError: 422,
    ServiceException: 500,
}


def add_exception_handlers(app: FastAPI) -> None:
    """
    Register varco exception handlers on a FastAPI application.

    Registers handlers for:
    - ``ServiceNotFoundError``      → 404
    - ``ServiceAuthorizationError`` → 403
    - ``ServiceConflictError``      → 409
    - ``ServiceValidationError``    → 422
    - ``ServiceException``          → 500 (catch-all for unknown service errors)

    All responses use the structured body format from ``error_message_for()``:
    ``{"code": "VARCO_XXXX", "message": "...", "correlation_id": "..."}``.

    Args:
        app: The ``FastAPI`` application to register handlers on.

    Edge cases:
        - Handlers are registered in order from most-specific to least-specific
          (``ServiceNotFoundError`` before ``ServiceException``) so FastAPI
          dispatches to the most specific handler first.
        - Does NOT register a handler for ``HTTPException`` — FastAPI's built-in
          handler already handles those correctly.

    Thread safety:  ✅ Safe to call at startup before requests begin.
    Async safety:   ✅ Handlers are ``async def``.
    """

    @app.exception_handler(ServiceNotFoundError)
    async def not_found_handler(request: Request, exc: ServiceNotFoundError):
        return _make_error_response(exc)

    @app.exception_handler(ServiceAuthorizationError)
    async def auth_error_handler(request: Request, exc: ServiceAuthorizationError):
        return _make_error_response(exc)

    @app.exception_handler(ServiceConflictError)
    async def conflict_handler(request: Request, exc: ServiceConflictError):
        return _make_error_response(exc)

    @app.exception_handler(ServiceValidationError)
    async def validation_handler(request: Request, exc: ServiceValidationError):
        return _make_error_response(exc)

    @app.exception_handler(ServiceException)
    async def service_exception_handler(request: Request, exc: ServiceException):
        _logger.error(
            "Unhandled ServiceException: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        return _make_error_response(exc)


__all__ = [
    "add_exception_handlers",
]
