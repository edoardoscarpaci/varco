"""
fastrest_core.exception
========================
Exception hierarchy for the fastrest_core domain and query layers.

    QueryException      — base for all query-system errors
    ├── OperationNotFound
    ├── OperationNotSupported
    ├── WrongNodeVisited
    └── CoercionError

    RepositoryException — base for all repository errors
    ├── RepositoryClassCreationFailed
    ├── FieldNotFound
    └── EntityNotFound
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
]
