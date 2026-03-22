"""
fastrest_core.exception.http
==============================
HTTP error response model and helpers for mapping service exceptions.

``ErrorMessage``
    Pydantic model representing the structured JSON body returned to API
    clients when a ``ServiceException`` is raised.  ``code`` is the stable
    i18n key; ``message`` is the resolved (possibly translated) text.

``error_code_for(exc)``
    Resolve the ``FastrestErrorCodes`` member (or raw ``ErrorCode``) for any
    ``ServiceException`` instance.  Returns a ``FastrestErrorCodes`` enum
    member for built-in exceptions; returns an ``ErrorCode`` dataclass for
    exceptions registered via ``register_error_code()``.

``error_message_for(exc, *, translator)``
    Build an ``ErrorMessage`` from any ``ServiceException``.  Accepts an
    optional ``translator`` callable so responses are ready for i18n.

``AnyErrorCode``
    Type alias: ``FastrestErrorCodes | ErrorCode`` — the union of the two
    types that ``error_code_for`` can return.  Use it when annotating helpers
    that accept either form.

FastAPI integration::

    from fastapi import FastAPI, Request
    from fastapi.responses import JSONResponse
    from fastrest_core.exception.service import ServiceException
    from fastrest_core.exception.http import error_message_for

    app = FastAPI()

    @app.exception_handler(ServiceException)
    async def service_error_handler(request: Request, exc: ServiceException):
        msg = error_message_for(exc)
        return JSONResponse(status_code=msg.http_status, content=msg.model_dump())

DESIGN: two separate maps for built-in vs custom codes
    Built-in codes: ``_EXCEPTION_CODE_MAP`` → ``FastrestErrorCodes`` members.
    Custom codes:   ``_CUSTOM_CODE_MAP``    → raw ``ErrorCode`` instances.
    ``error_code_for()`` checks ``_CUSTOM_CODE_MAP`` first (app codes are
    more specific by intent), then falls back to ``_EXCEPTION_CODE_MAP``.

    ✅ Built-in codes remain enum members — iterable, identity-comparable.
    ✅ Custom codes are plain ``ErrorCode`` values — app owns them.
    ✅ MRO walk means subclasses inherit from the parent's registered code.
    ❌ Callers of ``error_code_for`` receive either type — ``AnyErrorCode``
       union annotation makes this explicit.

DESIGN: ``AnyErrorCode`` Protocol over a common base class
    Both ``FastrestErrorCodes`` enum members and ``ErrorCode`` dataclasses
    expose ``.code``, ``.http_status``, and ``.default_message``.
    Using a TypeAlias union instead of a shared Protocol keeps the code
    simple — structural compatibility is enough at this call volume.

Thread safety:  ⚠️ ``_CUSTOM_CODE_MAP`` is mutable — call ``register_error_code``
                at startup only, before request handling begins.
Async safety:   ✅ Synchronous — safe to call from async exception handlers.
"""

from __future__ import annotations

from typing import Callable, TypeAlias

from pydantic import BaseModel

from fastrest_core.exception.codes import ErrorCode, FastrestErrorCodes
from fastrest_core.exception.service import (
    ServiceAuthorizationError,
    ServiceConflictError,
    ServiceException,
    ServiceNotFoundError,
    ServiceValidationError,
)

# ── AnyErrorCode type alias ───────────────────────────────────────────────────

# Both types expose .code, .http_status, .default_message.
# FastrestErrorCodes = built-in; ErrorCode = app-registered custom code.
AnyErrorCode: TypeAlias = FastrestErrorCodes | ErrorCode


# ── Error response model ──────────────────────────────────────────────────────


class ErrorMessage(BaseModel):
    """
    Structured JSON error response body.

    ``code`` is the stable i18n key — use it in translation catalogs.
    ``message`` is the resolved human-readable text (English by default).
    ``detail`` carries dynamic context (entity ID, field name, etc.) that
    should NOT go into translation strings because it varies per request.

    Attributes:
        code:        Stable error code string, e.g. ``"FASTREST_001"``.
                     This is the translation catalog key — never changes.
        http_status: HTTP status code.  Mirrors ``AnyErrorCode.http_status``.
        message:     Human-readable message, possibly translated.
                     English fallback when no translator is provided.
        detail:      Optional extra context — exception-specific data such
                     as the missing entity's ID or the failing field name.
                     ``None`` when no extra context is available.

    Thread safety:  ✅ Pydantic model — immutable after validation.
    Async safety:   ✅ Pure value object.

    Edge cases:
        - ``detail`` may contain internal information (stack traces, IDs) —
          filter it before including in production responses if sensitivity
          is a concern.
        - ``model_dump()`` produces a plain dict suitable for ``JSONResponse``.
        - OpenAPI schema is auto-generated when used as a FastAPI response model.

    Example JSON::

        {
            "code": "FASTREST_001",
            "http_status": 404,
            "message": "The requested resource was not found.",
            "detail": "Post with id='42' not found."
        }
    """

    # Stable translation key — never change after release
    code: str

    # HTTP status code — mirrors AnyErrorCode.http_status
    http_status: int

    # Human-readable message (translated when translator is provided)
    message: str

    # Optional dynamic context — not part of the translation string
    detail: str | None = None


# ── Exception → ErrorCode mappings ────────────────────────────────────────────

# Built-in codes: exception class → FastrestErrorCodes enum member.
# MRO walk handles subclasses automatically — ServiceNotFoundError subclasses
# get NOT_FOUND without explicit registration.
_EXCEPTION_CODE_MAP: dict[type[ServiceException], FastrestErrorCodes] = {
    ServiceNotFoundError: FastrestErrorCodes.NOT_FOUND,
    ServiceAuthorizationError: FastrestErrorCodes.UNAUTHORIZED,
    ServiceConflictError: FastrestErrorCodes.CONFLICT,
    ServiceValidationError: FastrestErrorCodes.VALIDATION_ERROR,
}

# Custom codes: populated at startup via register_error_code().
# Checked BEFORE _EXCEPTION_CODE_MAP so app-defined codes take precedence
# over inherited built-in codes.
_CUSTOM_CODE_MAP: dict[type[ServiceException], ErrorCode] = {}


# ── Public helpers ────────────────────────────────────────────────────────────


def error_code_for(exc: ServiceException) -> AnyErrorCode:
    """
    Resolve the error code for a ``ServiceException`` instance.

    Checks ``_CUSTOM_CODE_MAP`` first (app-registered codes), then
    ``_EXCEPTION_CODE_MAP`` (built-in codes), walking the MRO of the
    exception's type so subclasses are handled automatically.

    Falls back to ``FastrestErrorCodes.INTERNAL_ERROR`` for unregistered
    exception types.

    Args:
        exc: The service exception to look up.

    Returns:
        ``FastrestErrorCodes`` enum member for built-in codes, or a raw
        ``ErrorCode`` for app-registered codes.  Both expose ``.code``,
        ``.http_status``, and ``.default_message``.

    Edge cases:
        - A subclass registered in ``_CUSTOM_CODE_MAP`` takes precedence over
          its parent in ``_EXCEPTION_CODE_MAP`` because custom map is checked
          first in the MRO walk.
        - MRO includes ``object`` — it is never in either map, so the fallback
          is always returned for truly unknown exception types.
    """
    # Walk MRO from most-specific to least-specific.
    # Custom map takes precedence — checked first on every class in the MRO.
    for cls in type(exc).__mro__:
        if cls in _CUSTOM_CODE_MAP:
            return _CUSTOM_CODE_MAP[cls]  # type: ignore[index]
        if cls in _EXCEPTION_CODE_MAP:
            return _EXCEPTION_CODE_MAP[cls]  # type: ignore[index]

    # No match found — internal server error as catch-all
    return FastrestErrorCodes.INTERNAL_ERROR


def error_message_for(
    exc: ServiceException,
    *,
    translator: Callable[[str], str] | None = None,
) -> ErrorMessage:
    """
    Build an ``ErrorMessage`` from a ``ServiceException``.

    Resolves the correct error code via MRO walk, optionally translates the
    message using the code string as the translation key, and populates
    ``detail`` with ``str(exc)`` for dynamic context.

    Args:
        exc:        The service exception to convert.
        translator: Optional callable mapping a code string (e.g.
                    ``"FASTREST_001"``) to a locale-specific message.
                    When ``None``, ``default_message`` is used.

    Returns:
        A fully populated ``ErrorMessage`` ready for serialization.

    Edge cases:
        - ``str(exc)`` may contain internal detail (entity IDs, field names).
          Filter ``detail`` before including it in external API responses
          if the data could leak internal state.
        - If ``translator`` raises, the exception propagates — callers should
          handle translation failures before calling this function.
        - Empty ``str(exc)`` is normalized to ``detail=None`` for cleaner JSON.

    Example::

        msg = error_message_for(exc)
        return JSONResponse(status_code=msg.http_status, content=msg.model_dump())

        # With i18n:
        msg = error_message_for(exc, translator=request.state.translate)
        return JSONResponse(status_code=msg.http_status, content=msg.model_dump())
    """
    error_code = error_code_for(exc)

    # Both FastrestErrorCodes members and ErrorCode dataclasses expose these
    # properties — no type narrowing needed.
    if translator is not None:
        # translator receives the stable code string (e.g. "FASTREST_001")
        # so the translation catalog is keyed on the code, not the message.
        message = translator(error_code.code)
    else:
        message = error_code.default_message

    # Normalize empty str(exc) to None — avoids {"detail": ""} in JSON output
    raw_detail = str(exc)
    detail: str | None = raw_detail if raw_detail else None

    return ErrorMessage(
        code=error_code.code,
        http_status=error_code.http_status,
        message=message,
        detail=detail,
    )


def register_error_code(
    exception_cls: type[ServiceException],
    error_code: ErrorCode,
) -> None:
    """
    Register a custom ``ErrorCode`` for an application-defined exception.

    Populates ``_CUSTOM_CODE_MAP`` which is checked before the built-in
    ``_EXCEPTION_CODE_MAP`` during ``error_code_for()`` resolution.

    Args:
        exception_cls: The exception class to register.  Must be a subclass
                       of ``ServiceException``.
        error_code:    The ``ErrorCode`` to associate with it.

    Raises:
        Nothing — silently replaces an existing mapping.

    Edge cases:
        - Not thread-safe after request handling begins — call at startup only.
        - Registering a ``FastrestErrorCodes`` enum member's base class with a
          custom code will shadow the built-in for that class and all its
          subclasses.

    Example::

        class QuotaExceededError(ServiceException): ...

        register_error_code(
            QuotaExceededError,
            ErrorCode("APP_001", 429, "Request quota exceeded."),
        )
    """
    # Write to custom map — checked before built-ins in error_code_for()
    _CUSTOM_CODE_MAP[exception_cls] = error_code


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "AnyErrorCode",
    "ErrorMessage",
    "error_code_for",
    "error_message_for",
    "register_error_code",
]
