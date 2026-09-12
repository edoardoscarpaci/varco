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
from collections.abc import Mapping
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
from varco_core.exception.settings import ErrorEnvelopeSettings
from varco_core.i18n.catalog import MessageCatalog
from varco_core.tracing import current_correlation_id, generate_correlation_id

_logger = logging.getLogger(__name__)


def _locale_from_request(request: Request | None) -> str | None:
    """
    Read the resolved locale from ``request.state.varco_request_context``.

    RD-3: ``LocalizationMiddleware`` sets this ``request.state`` mirror
    specifically because, for a raised exception, the ambient ``ContextVar``
    is already reset (via that middleware's own ``finally``) by the time an
    outer error-rendering path runs — ``request.state`` is the only place
    the resolved ``RequestContext`` is still reachable here.

    Returns:
        The resolved locale, or ``None`` when no ``LocalizationMiddleware``
        ran (i18n disabled) or it resolved no locale (T1-only / i18n off).
    """
    if request is None:
        return None
    ctx = getattr(request.state, "varco_request_context", None)
    if ctx is None:
        return None
    return getattr(ctx, "locale", None)


def _make_error_response(
    exc: ServiceException,
    *,
    request: Request | None = None,
    message_catalog: MessageCatalog | None = None,
    set_content_language: bool = True,
) -> JSONResponse:
    """
    Build a structured JSON error response from a ``ServiceException``.

    Honours ``ErrorEnvelopeSettings`` (``VARCO_ERROR_*`` env vars, read fresh
    on every call — never cached — so a test/deployment can flip the D-4
    kill switch or the D-3 ``problem_details`` flag without a process
    restart): ``message_key``/``params`` on every built-in exception body
    (D-4), and the RFC 9457 ``type``/``title``/``detail``/``instance``
    members + ``application/problem+json`` media type when
    ``problem_details=True`` (D-3).

    Plan 011 / RD-3: when ``request`` carries a resolved locale
    (``request.state.varco_request_context.locale``, set by
    ``LocalizationMiddleware``) and a ``message_catalog`` is supplied, the
    ``message`` is rendered via ``message_catalog.format_message(key,
    locale, params)`` and the response gets a ``Content-Language`` header.
    With no locale resolved (i18n disabled or unresolved) or no catalog
    wired, this is a no-op — ``error_message_for()`` falls back to
    ``default_message`` exactly as before this fix.

    Args:
        exc:                  The service exception to format.
        request:               The originating request, if available — used
                               only to read the RD-3 locale mirror.
        message_catalog:       Catalog to render ``message_key`` through, if
                               any. ``None`` (default) reproduces pre-fix
                               behaviour exactly.
        set_content_language: Whether to set the ``Content-Language``
                               response header when a locale was resolved —
                               mirrors ``I18nSettings.set_content_language``.

    Returns:
        A ``JSONResponse`` with the correct HTTP status and body.
    """
    settings = ErrorEnvelopeSettings()
    media_type = "application/json"
    locale = _locale_from_request(request)
    message_resolver = None
    if message_catalog is not None and locale is not None:

        def message_resolver(key: str, params: Mapping[str, Any]) -> str | None:
            return message_catalog.format_message(key, locale, params)

    try:
        msg = error_message_for(exc, envelope_settings=settings, message_resolver=message_resolver)
        status_code = msg.http_status
        body: dict[str, Any] = {
            "code": msg.code,
            "message": msg.message,
        }
        # DEVIATION (Plan 002, guard.py step 29): error_message_for() already
        # populates ErrorMessage.detail with str(exc) "for dynamic context"
        # (see its docstring) but this response builder was silently dropping
        # it. RouteGuard denial messages (require_roles/scopes/token_profile/…)
        # are only useful to API clients if the actionable str(exc) reaches
        # the response body — include it whenever present. Non-breaking: no
        # existing test asserts the absence of a "detail" key here.
        if msg.detail:
            body["detail"] = msg.detail
        if msg.message_key is not None:
            body["message_key"] = msg.message_key
        if msg.params:
            body["params"] = msg.params

        if settings.problem_details:
            media_type = "application/problem+json"
            base = settings.problem_type_base or "about:blank"
            body["type"] = f"{base}{msg.message_key}" if msg.message_key else base
            body["title"] = msg.message
            body["status"] = status_code
            body["instance"] = None
    except Exception:  # noqa: BLE001
        # Fallback if error_message_for() itself raises — e.g. exc.error_params()
        # or a translator/message_resolver raises. Plan 035 / §D-S3a: this used
        # to echo str(exc) straight into the body — the identical leak fixed in
        # ErrorMiddleware._service_error_response (error.py). Same fix here:
        # opaque message, keep the "SERVICE_ERROR" code (no code string
        # changes), ERROR-level log with exc_info=True so the operator can
        # recover the detail via the correlation_id already in the response.
        status_code = _FALLBACK_STATUS.get(type(exc).__mro__[0], 500)
        _logger.error(
            "error_message_for() raised while rendering %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        body = {"code": "SERVICE_ERROR", "message": "An internal error occurred."}

    # DESIGN (Plan 035 / §D-S3a): correlation_id is the supported correlation
    # key for every error response, including one rendered before
    # RequestContextMiddleware ever ran — generate a fresh id rather than
    # silently omitting the key when no ambient one is set.
    body["correlation_id"] = current_correlation_id() or generate_correlation_id()

    response = JSONResponse(status_code=status_code, content=body, media_type=media_type)
    if set_content_language and locale:
        response.headers["Content-Language"] = locale
    return response


_FALLBACK_STATUS: dict[type, int] = {
    ServiceNotFoundError: 404,
    ServiceAuthorizationError: 403,
    ServiceConflictError: 409,
    ServiceValidationError: 422,
    ServiceException: 500,
}


def add_exception_handlers(
    app: FastAPI,
    *,
    message_catalog: MessageCatalog | None = None,
    set_content_language: bool = True,
) -> None:
    """
    Register varco exception handlers on a FastAPI application.

    Registers handlers for:
    - ``ServiceNotFoundError``      → 404
    - ``ServiceAuthorizationError`` → 403
    - ``ServiceConflictError``      → 409
    - ``ServiceValidationError``    → 422
    - ``ServiceException``          → 500 (catch-all for unknown service errors)

    All responses use the structured body format from ``error_message_for()``,
    e.g. ``{"code": "FASTREST_001", "message": "The requested resource was
    not found.", "message_key": "varco.error.not_found", "correlation_id":
    "..."}`` (``code`` is the stable machine identifier — see D-5; it is not
    "VARCO_XXXX", and it is never renamed after release).

    Args:
        app:                   The ``FastAPI`` application to register handlers on.
        message_catalog:       Plan 011 / RD-3 — optional ``MessageCatalog``.
                               When supplied, each handler reads
                               ``request.state.varco_request_context``
                               (set by ``LocalizationMiddleware``) and, if a
                               locale was resolved, renders the error
                               ``message`` via
                               ``message_catalog.format_message(message_key,
                               locale, params)`` and sets a
                               ``Content-Language`` response header. ``None``
                               (default) reproduces pre-fix behaviour exactly
                               — no localization, no header.
        set_content_language:  Mirrors ``I18nSettings.set_content_language``
                               for the error path specifically.

    Edge cases:
        - Handlers are registered in order from most-specific to least-specific
          (``ServiceNotFoundError`` before ``ServiceException``) so FastAPI
          dispatches to the most specific handler first.
        - Does NOT register a handler for ``HTTPException`` — FastAPI's built-in
          handler already handles those correctly.
        - No ``LocalizationMiddleware`` in the stack (i18n disabled) →
          ``request.state.varco_request_context`` is absent →
          ``_locale_from_request()`` returns ``None`` → identical to
          ``message_catalog=None``.

    Thread safety:  ✅ Safe to call at startup before requests begin.
    Async safety:   ✅ Handlers are ``async def``.
    """

    def _respond(exc: ServiceException, request: Request) -> JSONResponse:
        return _make_error_response(
            exc,
            request=request,
            message_catalog=message_catalog,
            set_content_language=set_content_language,
        )

    @app.exception_handler(ServiceNotFoundError)
    async def not_found_handler(request: Request, exc: ServiceNotFoundError) -> JSONResponse:
        return _respond(exc, request)

    @app.exception_handler(ServiceAuthorizationError)
    async def auth_error_handler(request: Request, exc: ServiceAuthorizationError) -> JSONResponse:
        return _respond(exc, request)

    @app.exception_handler(ServiceConflictError)
    async def conflict_handler(request: Request, exc: ServiceConflictError) -> JSONResponse:
        return _respond(exc, request)

    @app.exception_handler(ServiceValidationError)
    async def validation_handler(request: Request, exc: ServiceValidationError) -> JSONResponse:
        return _respond(exc, request)

    @app.exception_handler(ServiceException)
    async def service_exception_handler(request: Request, exc: ServiceException) -> JSONResponse:
        _logger.error(
            "Unhandled ServiceException: %s: %s",
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        return _respond(exc, request)


__all__ = [
    "add_exception_handlers",
]
