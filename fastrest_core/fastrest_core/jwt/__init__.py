"""
fastrest_core.jwt
=================

JWT model, builder, parser, and utility helpers for the fastrest service layer.

Public surface — importable directly from ``fastrest_core.jwt``::

    from fastrest_core.jwt import JsonWebToken, JwtBuilder, JwtParser, JwtUtil
    from fastrest_core.jwt import SYSTEM_ISSUER

Or from the top-level package::

    from fastrest_core import JsonWebToken, JwtBuilder, JwtParser, JwtUtil

Sub-module layout
-----------------
    fastrest_core/jwt/
    ├── model.py    — JsonWebToken dataclass + timestamp helpers + reserved-key set
    ├── builder.py  — JwtBuilder (fluent construction + signing)
    ├── parser.py   — JwtParser (decoding + AuthContext reconstruction)
    └── util.py     — JwtUtil (predicate helpers) + SYSTEM_ISSUER constant
"""

from fastrest_core.jwt.model import JsonWebToken
from fastrest_core.jwt.builder import JwtBuilder
from fastrest_core.jwt.parser import JwtParser
from fastrest_core.jwt.util import SYSTEM_ISSUER, JwtUtil

__all__ = [
    "SYSTEM_ISSUER",
    "JsonWebToken",
    "JwtBuilder",
    "JwtParser",
    "JwtUtil",
]
