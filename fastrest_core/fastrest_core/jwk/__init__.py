"""
fastrest_core.jwk
=================

RFC 7517 JSON Web Key (JWK) and JSON Web Key Set (JWKS) support.

Public surface — importable directly from ``fastrest_core.jwk``::

    from fastrest_core.jwk import JsonWebKey, JsonWebKeySet, JwkBuilder

Or from the top-level package::

    from fastrest_core import JsonWebKey, JsonWebKeySet, JwkBuilder

Sub-module layout
-----------------
    fastrest_core/jwk/
    ├── model.py    — JsonWebKey dataclass + JsonWebKeySet + base64url helpers
    └── builder.py  — JwkBuilder (RSA/EC/PEM factories + keyset constructor)
"""

from fastrest_core.jwk.model import JsonWebKey, JsonWebKeySet
from fastrest_core.jwk.builder import JwkBuilder

__all__ = [
    "JsonWebKey",
    "JsonWebKeySet",
    "JwkBuilder",
]
