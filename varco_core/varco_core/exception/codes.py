"""
varco_core.exception.codes
================================
Predefined error codes for the varco service layer.

``ErrorCode``
    Immutable value object pairing a stable string code with an HTTP status
    and an English fallback message.  Used as the value type inside the
    ``FastrestErrorCodes`` enum so the enum carries the full descriptor.

``FastrestErrorCodes``
    ``Enum`` of well-known ``ErrorCode`` constants, one per ``ServiceException``
    subclass.  The enum form means:

    - You can iterate all built-in codes: ``list(FastrestErrorCodes)``
    - You can compare by identity: ``code is FastrestErrorCodes.NOT_FOUND``
    - You cannot accidentally create an unknown code as a plain string

    Each member's ``.value`` is an ``ErrorCode`` dataclass; convenience
    properties (``.code``, ``.http_status``, ``.default_message``) forward
    to ``.value`` so callers rarely need to write ``.value.code``.

``FastrestErrorCodes.NOT_FOUND.code``         → ``"FASTREST_001"``
``FastrestErrorCodes.NOT_FOUND.http_status``  → ``404``
``FastrestErrorCodes.NOT_FOUND.value``        → ``ErrorCode(...)``

Extending with app-specific codes
----------------------------------
Application codes live outside this enum — define an ``ErrorCode`` directly
and pass it to ``register_error_code()`` in ``varco_core.exception.http``::

    from varco_core.exception.codes import ErrorCode

    QUOTA_EXCEEDED = ErrorCode("APP_001", 429, "Request quota exceeded.")
    register_error_code(QuotaExceededError, QUOTA_EXCEEDED)

DESIGN: ``Enum`` over a plain namespace class
    ✅ Members are iterable: ``list(FastrestErrorCodes)`` gives all codes.
    ✅ Identity check: ``code is FastrestErrorCodes.NOT_FOUND`` is safe.
    ✅ Name resolution: ``FastrestErrorCodes["NOT_FOUND"]`` works.
    ✅ Can't accidentally create new members at runtime — prevents typo codes.
    ❌ Adding a new code requires editing this file; app codes use
       ``register_error_code()`` with a raw ``ErrorCode`` instead.

Thread safety:  ✅ ``Enum`` members are module-level singletons — immutable.
Async safety:   ✅ Pure value objects — no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


# ── ErrorCode value object ────────────────────────────────────────────────────


@dataclass(frozen=True)
class ErrorCode:
    """
    Immutable descriptor for a single error condition.

    ``code`` is the stable i18n translation key — never change it after release.
    ``http_status`` maps directly to an HTTP response code.
    ``default_message`` is the English fallback used when no translator is
    provided.

    Attributes:
        code:            Stable string identifier, e.g. ``"FASTREST_001"``.
                         Used as the i18n translation key.
        http_status:     HTTP status code to return (e.g. 404, 403, 500).
        default_message: English fallback message returned when no translator
                         is configured.

    Thread safety:  ✅ frozen=True — immutable after construction.
    Async safety:   ✅ Pure value object.

    Edge cases:
        - ``code`` should be globally unique within the application.
          Collisions between ``FastrestErrorCodes`` and application-defined
          codes are silently allowed — use a distinct prefix (e.g. ``"APP_"``).
        - ``http_status`` is not validated — callers must supply a valid HTTP
          status integer.  Use standard values (2xx, 4xx, 5xx).

    Example::

        MY_ERROR = ErrorCode("APP_001", 400, "Something went wrong with the request.")
    """

    # Stable string identifier — the i18n translation key
    code: str

    # HTTP status to return when this error is mapped to a response
    http_status: int

    # English fallback — used when no translator callable is provided
    default_message: str

    def __repr__(self) -> str:
        return f"ErrorCode(code={self.code!r}, http_status={self.http_status})"


# ── FastrestErrorCodes Enum ───────────────────────────────────────────────────


class FastrestErrorCodes(Enum):
    """
    Enum of predefined ``ErrorCode`` constants for ``ServiceException`` subtypes.

    Each member's ``.value`` is a ``ErrorCode`` dataclass.  Convenience
    properties (``.code``, ``.http_status``, ``.default_message``) expose the
    same attributes without needing ``.value`` every time.

    Codes are stable identifiers — do NOT change a code after it has been
    released.  Add new members for new conditions; never recycle old ones.

    Naming convention:
        ``FASTREST_0xx`` — client errors (4xx HTTP)
        ``FASTREST_5xx`` — server errors (5xx HTTP)

    Usage::

        FastrestErrorCodes.NOT_FOUND.code         # "FASTREST_001"
        FastrestErrorCodes.NOT_FOUND.http_status   # 404
        FastrestErrorCodes.NOT_FOUND.value         # ErrorCode(...)
        list(FastrestErrorCodes)                   # all built-in codes

    Thread safety:  ✅ Enum members are module-level singletons — immutable.
    Async safety:   ✅ Pure value object — no I/O.

    Edge cases:
        - ``FastrestErrorCodes["NOT_FOUND"]`` resolves by member name.
        - ``FastrestErrorCodes("FASTREST_001")`` would NOT work because
          the value is an ``ErrorCode`` object, not a string.  Use
          ``FastrestErrorCodes.NOT_FOUND`` directly.
    """

    # 4xx client errors ────────────────────────────────────────────────────────

    # Maps to ServiceNotFoundError → HTTP 404
    NOT_FOUND = ErrorCode(
        code="FASTREST_001",
        http_status=404,
        default_message="The requested resource was not found.",
    )

    # Maps to ServiceAuthorizationError → HTTP 403
    UNAUTHORIZED = ErrorCode(
        code="FASTREST_002",
        http_status=403,
        default_message="You are not authorised to perform this action.",
    )

    # Maps to ServiceConflictError → HTTP 409
    CONFLICT = ErrorCode(
        code="FASTREST_003",
        http_status=409,
        default_message="The operation conflicts with existing data.",
    )

    # Maps to ServiceValidationError → HTTP 422
    VALIDATION_ERROR = ErrorCode(
        code="FASTREST_004",
        http_status=422,
        default_message="The request data failed validation.",
    )

    # 5xx server errors ────────────────────────────────────────────────────────

    # Catch-all for unhandled ServiceException subtypes → HTTP 500
    INTERNAL_ERROR = ErrorCode(
        code="FASTREST_500",
        http_status=500,
        default_message="An unexpected error occurred.",
    )

    # ── Convenience properties ────────────────────────────────────────────────
    # Forward to self.value so callers can write FastrestErrorCodes.NOT_FOUND.code
    # instead of FastrestErrorCodes.NOT_FOUND.value.code.

    @property
    def code(self) -> str:
        """Stable i18n translation key, e.g. ``"FASTREST_001"``."""
        return self.value.code

    @property
    def http_status(self) -> int:
        """HTTP status code, e.g. ``404``."""
        return self.value.http_status

    @property
    def default_message(self) -> str:
        """English fallback message when no translator is provided."""
        return self.value.default_message


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "ErrorCode",
    "FastrestErrorCodes",
]
