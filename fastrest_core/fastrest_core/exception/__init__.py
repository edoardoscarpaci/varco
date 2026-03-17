"""
fastrest_core.exception
========================
Exception hierarchy for the fastrest_core domain, query, and service layers.

    QueryException      — base for all query-system errors
    ├── OperationNotFound
    ├── OperationNotSupported
    ├── WrongNodeVisited
    └── CoercionError

    RepositoryException — base for all repository errors
    ├── RepositoryClassCreationFailed
    ├── FieldNotFound
    ├── EntityNotFound
    └── StaleEntityError

    ServiceException    — base for all service-layer errors
    ├── ServiceNotFoundError      → HTTP 404
    ├── ServiceAuthorizationError → HTTP 403
    ├── ServiceConflictError      → HTTP 409
    └── ServiceValidationError    → HTTP 422
"""

from fastrest_core.exception.query import (
    CoercionError,
    OperationNotFound,
    OperationNotSupported,
    QueryException,
    WrongNodeVisited,
)
from fastrest_core.exception.repository import (
    EntityNotFound,
    FieldNotFound,
    RepositoryClassCreationFailed,
    RepositoryException,
    StaleEntityError,
)
from fastrest_core.exception.service import (
    ServiceAuthorizationError,
    ServiceConflictError,
    ServiceException,
    ServiceNotFoundError,
    ServiceValidationError,
)

__all__ = [
    # Query exceptions
    "QueryException",
    "OperationNotFound",
    "OperationNotSupported",
    "WrongNodeVisited",
    "CoercionError",
    # Repository exceptions
    "RepositoryException",
    "RepositoryClassCreationFailed",
    "FieldNotFound",
    "EntityNotFound",
    "StaleEntityError",
    # Service exceptions
    "ServiceException",
    "ServiceNotFoundError",
    "ServiceAuthorizationError",
    "ServiceConflictError",
    "ServiceValidationError",
]
