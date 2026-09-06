"""
varco_core.exception.rate_limit
================================
``RateLimitExceededError`` — Plan 035 drift fix (§D-order position 11 /
§D-S10-headers).

Raised by ``varco_fastapi.middleware.rate_limit.RateLimitMiddleware`` when a
rule denies a request and an ``ErrorMiddleware`` is present further out in
the stack to render it. Lives in ``varco_core`` because the exception
taxonomy does — the ASGI middleware that raises it is HTTP-specific and
lives in ``varco_fastapi``, but the exception itself is a plain
``ServiceException`` with no ASGI dependency, same layering as
``RequestBodyTooLargeError`` (``varco_core/exception/body_limit.py``), whose
shape this mirrors exactly.

No existing built-in ``ServiceException`` maps to HTTP 429, so this
registers its own ``ErrorCode`` via ``register_error_code()`` at import
time — the same pattern ``RequestBodyTooLargeError``/
``IdempotencyKeyInvalidError`` use for their own status codes.

DESIGN: ``retry_after_seconds``/``rate_limit_policy`` as duck-typed
    attributes, not a generic ``ServiceException.headers`` API
    (§D-S10-headers)
    ✅ ``ErrorMiddleware._service_error_response`` already has exactly this
       precedent: ``IdempotencyKeyConflictError.retry_after_seconds`` is
       read via ``getattr(exc, "retry_after_seconds", None)`` and, if
       present, becomes the ``Retry-After`` response header — a narrow,
       already-shipped hook, not a new public API surface on the
       ``ServiceException`` base class (the base class is untouched).
       Adding one more attribute of the same shape (``rate_limit_policy``)
       for the draft ``RateLimit-Policy`` header reuses that exact
       precedent rather than inventing a generic headers dict.
    ✅ The alternative the drift report also named — wrapping ``send`` in
       ``RateLimitMiddleware`` to inject headers onto the outgoing
       response — is not actually reachable here: ``ErrorMiddleware``
       subclasses Starlette's ``BaseHTTPMiddleware``, which calls the
       wrapped app (``RateLimitMiddleware``, in this stack) with a
       *synthetic*, in-memory-stream-backed ``send``
       (``starlette/middleware/base.py``'s ``send_no_error`` passed into
       ``self.app(scope, receive_or_disconnect, send_no_error)``), not the
       real ASGI ``send``. When ``RateLimitMiddleware`` raises instead of
       calling ``self.app()``, that synthetic ``send`` is never invoked by
       anyone — ``BaseHTTPMiddleware.dispatch()`` catches the exception and
       renders its own ``Response``, then sends *that* through the real,
       outer ``send`` obtained separately. Wrapping the synthetic ``send``
       therefore could never influence the final response headers in this
       exact ordering (``RateLimitMiddleware`` strictly inside
       ``ErrorMiddleware``, §D-order). The duck-typed-attribute route is
       the only one of the two options the drift report offered that is
       reachable in practice.
    ❌ Two attributes to keep in sync with ``ErrorMiddleware`` rather than
       one generic mechanism. Accepted: both are narrow, additive,
       ``getattr``-guarded reads — an out-of-tree ``ServiceException``
       subclass that never sets either renders byte-identically to before.

Thread safety:  ✅ Immutable after construction.
Async safety:   ✅ Safe to raise in async contexts.
"""

from __future__ import annotations

from typing import Any

from varco_core.exception.codes import ErrorCode
from varco_core.exception.http import register_error_code
from varco_core.exception.service import ServiceException

__all__ = ["RateLimitExceededError"]


class RateLimitExceededError(ServiceException):
    """
    Raised when a ``RateLimitRule`` denies a request and ``ErrorMiddleware``
    is present to render the 429 through the standard error envelope.

    Maps to HTTP 429 Too Many Requests (RFC 6585 §4) via a registered
    ``ErrorCode`` (no existing built-in ``ServiceException`` maps to 429).

    Attributes:
        rule_name:          The name of the ``RateLimitRule`` that denied
            the request — named in the message so the response is
            self-diagnosing, mirroring ``RequestBodyTooLargeError``.
        retry_after_seconds: Read by ``ErrorMiddleware._service_error_response``
            via ``getattr`` (the same mechanism ``IdempotencyKeyConflictError``
            uses) to set the ``Retry-After`` response header — §D-S10-headers
            requires this on every 429, always.
        rate_limit_policy:   Optional draft-11 ``RateLimit-Policy`` structured-
            field value, read the same way to set the ``RateLimit-Policy``
            header when ``RateLimitSettings.emit_draft_headers=True``.
            ``None`` (default) means no header is emitted.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Safe to raise in async contexts.
    """

    message_key = "varco.error.rate_limit_exceeded"

    def __init__(
        self,
        *,
        rule_name: str,
        retry_after_seconds: int,
        rate_limit_policy: str | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            rule_name:           Name of the denying ``RateLimitRule``.
            retry_after_seconds: Seconds until the caller may retry.
            rate_limit_policy:   Optional draft-11 ``RateLimit-Policy`` value.
            kwargs:               Forwarded to ``Exception.__init__``.
        """
        self.rule_name = rule_name
        self.retry_after_seconds = retry_after_seconds
        self.rate_limit_policy = rate_limit_policy
        super().__init__(
            f"Rate limit exceeded for rule {rule_name!r}. "
            f"Retry after {retry_after_seconds} second(s).",
            **kwargs,
        )

    def error_params(self) -> dict[str, Any]:
        return {"rule_name": self.rule_name, "retry_after_seconds": self.retry_after_seconds}


# Register the 429 mapping — module import time, before request handling
# begins (register_error_code's documented "call at startup only" contract).
register_error_code(
    RateLimitExceededError,
    ErrorCode(
        code="VARCO_RATE_LIMIT_001",
        http_status=429,
        default_message="Rate limit exceeded. Retry after the indicated interval.",
        message_key="varco.error.rate_limit_exceeded",
    ),
)
