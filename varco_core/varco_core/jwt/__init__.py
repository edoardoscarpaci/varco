"""
varco_core.jwt
=================

JWT model, builder, parser, and utility helpers for the varco service layer.

Public surface — importable directly from ``varco_core.jwt``::

    from varco_core.jwt import JsonWebToken, JwtBuilder, JwtParser, JwtUtil
    from varco_core.jwt import SYSTEM_ISSUER

Or from the top-level package::

    from varco_core import JsonWebToken, JwtBuilder, JwtParser, JwtUtil

Sub-module layout
-----------------
    varco_core/jwt/
    ├── model.py    — JsonWebToken dataclass + timestamp helpers + reserved-key set
    ├── builder.py  — JwtBuilder (fluent construction + signing)
    ├── parser.py   — JwtParser (decoding + AuthContext reconstruction)
    └── util.py     — JwtUtil (predicate helpers) + SYSTEM_ISSUER constant
"""

from varco_core.jwt.model import JsonWebToken
from varco_core.jwt.builder import JwtBuilder
from varco_core.jwt.parser import JwtParser
from varco_core.jwt.util import SYSTEM_ISSUER, JwtUtil

__all__ = [
    "SYSTEM_ISSUER",
    "JsonWebToken",
    "JwtBuilder",
    "JwtParser",
    "JwtUtil",
]
