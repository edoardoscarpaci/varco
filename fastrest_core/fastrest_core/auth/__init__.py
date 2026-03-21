"""
fastrest_core.auth
==================
Authorization primitives and default permissive authorizer.
"""

from fastrest_core.auth.base import (
    AbstractAuthorizer,
    Action,
    AuthContext,
    Resource,
    ResourceGrant,
)
from fastrest_core.auth.authorizer import BaseAuthorizer

__all__ = [
    "AbstractAuthorizer",
    "Action",
    "AuthContext",
    "Resource",
    "ResourceGrant",
    "BaseAuthorizer",
]
