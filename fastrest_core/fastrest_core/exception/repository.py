"""
fastrest_core.exception.repository
=====================================
Exceptions raised by the repository layer.

All exceptions inherit from ``RepositoryException`` so callers can catch
the entire family with a single ``except RepositoryException`` clause.

Thread safety:  ✅ Exception objects are immutable after construction.
Async safety:   ✅ Safe to raise and catch in async contexts.
"""

from __future__ import annotations

from typing import Any


class RepositoryException(Exception):
    """Base class for all repository exceptions."""


class RepositoryClassCreationFailed(RepositoryException):
    """
    Raised when dynamic ORM / Document class generation fails.

    Attributes:
        repository_cls: The domain class for which generation was attempted.
    """

    def __init__(
        self, message: str, repository_cls: type, *args: Any, **kwargs: Any
    ) -> None:
        """
        Args:
            message:        Human-readable description of the failure.
            repository_cls: The domain class that failed to generate.
            args:           Forwarded to ``Exception.__init__``.
            kwargs:         Forwarded to ``Exception.__init__``.
        """
        self.repository_cls = repository_cls
        super().__init__(message, *args, **kwargs)


class FieldNotFound(RepositoryException):
    """
    Raised when a query or sort references a field that does not exist on
    the model / ORM class.

    Attributes:
        field: The field name that was not found.
        table: The table / collection name that was searched.
    """

    def __init__(self, field: str, table: str, *args: Any, **kwargs: Any) -> None:
        """
        Args:
            field: Field name that is missing.
            table: Table or collection name.
            args:  Forwarded to ``Exception.__init__``.
            kwargs: Forwarded to ``Exception.__init__``.
        """
        self.field = field
        self.table = table
        super().__init__(
            f"Field {field!r} not found on model {table!r}. "
            "Check spelling and that the field is mapped as a column.",
            *args,
            **kwargs,
        )


class StaleEntityError(RepositoryException):
    """
    Raised when an optimistic lock conflict is detected during an UPDATE.

    The entity was modified by another process between when it was loaded
    and when the current ``save()`` was called.  The caller should reload
    the entity and retry (or surface a 409 Conflict to the client).

    Attributes:
        entity_cls:       Domain class involved in the conflict.
        expected_version: The ``row_version`` the caller held at load time.
        actual_version:   The ``row_version`` currently stored in the DB.
    """

    def __init__(
        self,
        entity_cls: type,
        expected_version: int,
        actual_version: int,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.entity_cls = entity_cls
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            f"Stale {entity_cls.__name__}: expected row_version={expected_version}, "
            f"found {actual_version}. Reload the entity and retry.",
            *args,
            **kwargs,
        )


class EntityNotFound(RepositoryException):
    """
    Raised when a required entity is not found during a lookup.

    Distinct from a repository returning ``None`` — use this when the
    caller's contract demands the entity must exist (e.g. GET by ID endpoint).

    Attributes:
        entity_id: The PK value that was not found.
        table:     The table / collection name that was searched.
    """

    def __init__(self, entity_id: str, table: str, *args: Any, **kwargs: Any) -> None:
        """
        Args:
            entity_id: The PK value (string-coerced) that was not found.
            table:     Table or collection name.
            args:      Forwarded to ``Exception.__init__``.
            kwargs:    Forwarded to ``Exception.__init__``.
        """
        self.entity_id = entity_id
        self.table = table
        super().__init__(
            f"Entity with id={entity_id!r} not found in {table!r}.",
            *args,
            **kwargs,
        )
