"""
varco_core.exception.body_limit
================================
``RequestBodyTooLargeError`` — Plan 035 / Phase 4, Step 15 (S8, §D-S8-default).

Raised by ``varco_fastapi.middleware.body_limit.BodyLimitMiddleware`` when a
request body exceeds ``BodyLimitSettings.max_bytes``. Lives in
``varco_core`` because the exception taxonomy does — the ASGI middleware
that raises it is HTTP-specific and lives in ``varco_fastapi``, but the
exception itself is a plain ``ServiceException`` with no ASGI dependency,
same layering as every other exception in this package.

No existing built-in ``ServiceException`` maps to HTTP 413, so this
registers its own ``ErrorCode`` via ``register_error_code()`` at import
time — the same pattern ``IdempotencyKeyInvalidError`` uses for its 400
mapping (``varco_core/exception/idempotency.py``).
"""

from __future__ import annotations

from typing import Any

from varco_core.exception.codes import ErrorCode
from varco_core.exception.http import register_error_code
from varco_core.exception.service import ServiceException

__all__ = ["RequestBodyTooLargeError"]


class RequestBodyTooLargeError(ServiceException):
    """
    Raised when a request body exceeds the configured byte ceiling.

    Maps to HTTP 413 Payload Too Large (RFC 9110 §15.4.14 / RFC 6585 §4 —
    brief 008 §2) via a registered ``ErrorCode`` (no existing built-in
    ``ServiceException`` maps to 413).

    Attributes:
        max_bytes: The configured ceiling that was exceeded, named in the
            message so the response is self-diagnosing (§D-S8-default) —
            an operator sees both the ceiling and the env var that raises
            it without consulting docs.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Safe to raise in async contexts.
    """

    message_key = "varco.error.request_body_too_large"

    def __init__(self, *, max_bytes: int, **kwargs: Any) -> None:
        """
        Args:
            max_bytes: The configured byte ceiling that was exceeded.
            kwargs:    Forwarded to ``Exception.__init__``.
        """
        self.max_bytes = max_bytes
        super().__init__(
            f"Request body exceeds the {max_bytes}-byte limit. "
            "Raise the ceiling with VARCO_BODY_LIMIT_MAX_BYTES, or exempt "
            "this path via VARCO_BODY_LIMIT_EXEMPT_PATHS.",
            **kwargs,
        )

    def error_params(self) -> dict[str, Any]:
        return {"max_bytes": self.max_bytes}


# Register the 413 mapping — module import time, before request handling
# begins (register_error_code's documented "call at startup only" contract).
register_error_code(
    RequestBodyTooLargeError,
    ErrorCode(
        code="VARCO_BODY_LIMIT_001",
        http_status=413,
        default_message=(
            "The request body exceeds the configured size limit. Raise the "
            "ceiling with VARCO_BODY_LIMIT_MAX_BYTES, or exempt this path "
            "via VARCO_BODY_LIMIT_EXEMPT_PATHS."
        ),
        message_key="varco.error.request_body_too_large",
    ),
)
