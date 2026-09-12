"""
varco_core.exception
========================
Exception hierarchy for the varco_core domain, query, and service layers.

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
    ├── ServiceNotFoundError                  → HTTP 404
    ├── ServiceAuthorizationError              → HTTP 403
    ├── ServiceConflictError                   → HTTP 409
    │   └── IdempotencyKeyConflictError        → HTTP 409 (Plan 029 / D1)
    ├── ServiceValidationError                 → HTTP 422
    │   └── IdempotencyFingerprintMismatchError → HTTP 422 (Plan 029 / D1)
    ├── IdempotencyKeyInvalidError              → HTTP 400 (Plan 029 / D1)
    ├── RequestBodyTooLargeError                → HTTP 413 (Plan 035 / S8)
    └── RateLimitExceededError                  → HTTP 429 (Plan 035 drift fix)
"""

from varco_core.exception.body_limit import RequestBodyTooLargeError
from varco_core.exception.idempotency import (
    IdempotencyFingerprintMismatchError,
    IdempotencyKeyConflictError,
    IdempotencyKeyInvalidError,
)
from varco_core.exception.query import (
    CoercionError,
    OperationNotFound,
    OperationNotSupported,
    QueryException,
    WrongNodeVisited,
)
from varco_core.exception.rate_limit import RateLimitExceededError
from varco_core.exception.repository import (
    EntityNotFound,
    FieldNotFound,
    RepositoryClassCreationFailed,
    RepositoryException,
    StaleEntityError,
)
from varco_core.exception.service import (
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
    # Idempotency exceptions (Plan 029 / D1a)
    "IdempotencyKeyConflictError",
    "IdempotencyFingerprintMismatchError",
    "IdempotencyKeyInvalidError",
    # Body limit exceptions (Plan 035 / S8)
    "RequestBodyTooLargeError",
    # Rate limit exceptions (Plan 035 drift fix — §D-order position 11)
    "RateLimitExceededError",
]
