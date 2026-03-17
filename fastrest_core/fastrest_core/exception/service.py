"""
fastrest_core.exception.service
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

from typing import Any


class ServiceException(Exception):
    """Base class for all service-layer exceptions."""


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
