"""
varco_core.exception.service
=================================
Exceptions raised by the service layer.

All exceptions inherit from ``ServiceException`` so HTTP adapters can catch
the entire family with a single ``except ServiceException`` clause and then
dispatch on subtype to produce the correct HTTP status code.

Suggested HTTP status code mapping::

    ServiceNotFoundError      → 404 Not Found
    ServiceAuthorizationError → 403 Forbidden
    ServiceConflictError      → 409 Conflict
    ServiceValidationError    → 422 Unprocessable Entity

Thread safety:  ✅ Exception objects are immutable after construction.
Async safety:   ✅ Safe to raise and catch in async contexts.
"""

from __future__ import annotations

from typing import Any, ClassVar


class ServiceException(Exception):
    """
    Base class for all service-layer exceptions.

    Plan 011 / I1: ``message_key`` is a ``ClassVar`` (not a constructor
    parameter) and ``error_params()`` is a method returning ``{}`` on the
    base — deliberately, so **no ``__init__`` signature changes anywhere**.
    Subclasses forward ``*args``/``**kwargs`` positionally to
    ``Exception.__init__``, which accepts no keywords; a new constructor
    keyword would be a real breaking change for every out-of-tree subclass.
    An out-of-tree ``ServiceException`` subclass that never sets
    ``message_key``/overrides ``error_params()`` compiles, runs, and
    serializes byte-identically to before this plan (D-4).
    """

    #: i18n key resolved by varco_core.i18n — None means "no server-side
    #: catalog lookup for this exception type" (byte-identical body).
    message_key: ClassVar[str | None] = None

    def error_params(self) -> dict[str, Any]:
        """
        Structured params for message interpolation and client-side i18n.

        Deliberately excludes anything sensitive — see subclass overrides.
        A params dict is exactly the kind of thing someone later fills with
        ``vars(exc)``; don't.

        Plan 040 / S21, §D-S21-errparams: as of 3.2, ``error_message_for()``
        routes this return value through ``varco_core.redaction.redact_mapping()``
        before emitting it (``ErrorEnvelopeSettings.redact_params``, default
        ``True``), which makes **two of the three** leak shapes this
        docstring warns about mechanical rather than merely advisory:

        - **Mechanical now:** a secret-*named* key (matching one of
          ``DEFAULT_REDACT_PATTERNS``) is replaced with ``"[REDACTED]"``.
        - **Mechanical now:** a non-JSON value (e.g. a live object from a
          ``vars(exc)`` dump) is replaced with ``"<TypeName>"``.
        - **Still advisory — no mechanism catches this:** a secret *value*
          under a key that does not match any pattern (e.g.
          ``{"internal_reason": "postgres://user:pw@host/db"}``) is still
          emitted verbatim. Redaction in 3.2 is key-name-based only
          (§D-S21-shape) — it does not scan values. Do not return a secret
          value under a benign key and rely on this mechanism to catch it.
        """
        return {}


class ServiceNotFoundError(ServiceException):
    """
    Raised when a requested entity does not exist.

    Raised by the service after ``find_by_id()`` returns ``None``, giving
    the caller a single exception type that maps cleanly to HTTP 404.

    Attributes:
        entity_id:  String-encoded primary key that was not found.
        entity_cls: The domain class that was searched.

    DESIGN: NotFound is raised BEFORE the authorization check on
    read/update/delete.  A missing entity raises this regardless of the
    caller's identity — this avoids an existence oracle where a 403 would
    leak the fact that the entity exists.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Safe to raise in async contexts.

    Edge cases:
        - ``entity_id`` is always coerced to ``str`` so the HTTP adapter
          does not need to know the concrete PK type.
    """

    def __init__(
        self,
        entity_id: Any,
        entity_cls: type,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            entity_id:  The primary key that was not found.  Stored as ``str``.
            entity_cls: The domain class that was queried.
            args:       Forwarded to ``Exception.__init__``.
            kwargs:     Forwarded to ``Exception.__init__``.
        """
        # Coerce to str so callers never need to know the concrete PK type
        self.entity_id: str = str(entity_id)
        self.entity_cls = entity_cls
        super().__init__(
            f"{entity_cls.__name__} with id={self.entity_id!r} not found. "
            "The entity may have been deleted or the id may be wrong.",
            *args,
            **kwargs,
        )

    message_key = "varco.error.not_found"

    def error_params(self) -> dict[str, Any]:
        return {"entity": self.entity_cls.__name__, "entity_id": self.entity_id}


class ServiceAuthorizationError(ServiceException):
    """
    Raised when a caller lacks permission to perform the requested operation.

    Maps to HTTP 403 Forbidden.  The public message intentionally avoids
    revealing *why* the check failed (ownership vs. role vs. scope) to
    prevent information leakage.

    Attributes:
        operation:  Human-readable name of the denied operation (e.g. ``"delete"``).
        entity_cls: Domain class involved, or ``None`` for collection-level
                    operations such as ``list``.
        reason:     Optional internal description — log it server-side only;
                    never surface to API clients.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Safe to raise in async contexts.

    Edge cases:
        - ``reason`` is stored on the exception but deliberately excluded from
          ``str(exc)`` — log it separately at DEBUG/INFO level.
        - ``entity_cls`` may be ``None`` for collection-level denials such as
          a caller that is not allowed to list a resource at all.
    """

    def __init__(
        self,
        operation: str,
        entity_cls: type | None = None,
        *,
        reason: str | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            operation:  Name of the denied operation (e.g. ``"create"``,
                        ``"delete"``).
            entity_cls: Domain class involved.  ``None`` for collection-ops.
            reason:     Internal description of why access was denied.
                        Never included in the public message.
            kwargs:     Forwarded to ``Exception.__init__``.
        """
        self.operation = operation
        self.entity_cls = entity_cls
        # Internal detail — must NOT appear in str(exc) to prevent
        # information leakage through API responses.
        self.reason: str | None = reason

        entity_part = f" on {entity_cls.__name__}" if entity_cls else ""
        super().__init__(
            f"Permission denied: {operation!r}{entity_part}.",
            **kwargs,
        )

    message_key = "varco.error.unauthorized"

    def error_params(self) -> dict[str, Any]:
        # `reason` is deliberately excluded — it is documented above as
        # server-side-only and must never reach a client (Plan 011, the
        # "params dict is a new exfiltration surface" risk).
        params: dict[str, Any] = {"operation": self.operation}
        if self.entity_cls is not None:
            params["entity"] = self.entity_cls.__name__
        return params


class ServiceConflictError(ServiceException):
    """
    Raised when an operation violates a business rule or uniqueness constraint.

    Examples: duplicate email, invalid state transition, optimistic-lock
    conflict (``StaleEntityError`` wrapped at the service boundary).

    Maps to HTTP 409 Conflict.

    Attributes:
        detail: Human-readable description of the conflict.

    DESIGN: use this for business-layer conflicts only.  Low-level DB
    integrity errors should be caught and re-raised as
    ``ServiceConflictError`` in the concrete service implementation.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Safe to raise in async contexts.

    Edge cases:
        - Wrap ``StaleEntityError`` here so the HTTP adapter sees a
          consistent exception type regardless of the storage backend.
    """

    def __init__(self, detail: str, *args: Any, **kwargs: Any) -> None:
        """
        Args:
            detail: Human-readable description of what conflicted and why.
            args:   Forwarded to ``Exception.__init__``.
            kwargs: Forwarded to ``Exception.__init__``.

        Example::

            raise ServiceConflictError(
                "A user with email 'foo@example.com' already exists."
            )
        """
        self.detail = detail
        super().__init__(detail, *args, **kwargs)

    message_key = "varco.error.conflict"

    def error_params(self) -> dict[str, Any]:
        return {"detail": self.detail}


class ServiceValidationError(ServiceException):
    """
    Raised when a DTO passes Pydantic validation but fails a business rule.

    Examples: start_date > end_date, negative budget, reserved slug.

    Maps to HTTP 422 Unprocessable Entity.

    DESIGN: DTO-level validation (type, format) belongs in Pydantic;
    business rule validation (cross-field, domain invariants) belongs here.

    Attributes:
        detail: Human-readable description of the violated rule.
        field:  Optional field name that caused the violation.
                ``None`` when the rule spans multiple fields.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ Safe to raise in async contexts.
    """

    def __init__(
        self,
        detail: str,
        field: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            detail: Description of the violated business rule.
            field:  Optional field name that caused the violation.
            args:   Forwarded to ``Exception.__init__``.
            kwargs: Forwarded to ``Exception.__init__``.

        Example::

            raise ServiceValidationError(
                "start_date must be before end_date",
                field="start_date",
            )
        """
        self.detail = detail
        self.field: str | None = field
        field_part = f" (field: {field!r})" if field else ""
        super().__init__(f"Validation error{field_part}: {detail}", *args, **kwargs)

    message_key = "varco.error.validation_failed"

    def error_params(self) -> dict[str, Any]:
        params: dict[str, Any] = {"detail": self.detail}
        if self.field is not None:
            params["field"] = self.field
        return params
