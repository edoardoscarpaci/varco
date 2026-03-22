"""
varco_core.jwk
=================

RFC 7517 JSON Web Key (JWK) and JSON Web Key Set (JWKS) support.

Public surface — importable directly from ``varco_core.jwk``::

    from varco_core.jwk import JsonWebKey, JsonWebKeySet, JwkBuilder

Or from the top-level package::

    from varco_core import JsonWebKey, JsonWebKeySet, JwkBuilder

Sub-module layout
-----------------
    varco_core/jwk/
    ├── model.py    — JsonWebKey dataclass + JsonWebKeySet + base64url helpers
    └── builder.py  — JwkBuilder (RSA/EC/PEM factories + keyset constructor)
"""

from varco_core.jwk.model import JsonWebKey, JsonWebKeySet
from varco_core.jwk.builder import JwkBuilder

__all__ = [
    "JsonWebKey",
    "JsonWebKeySet",
    "JwkBuilder",
]
