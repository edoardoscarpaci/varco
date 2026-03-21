"""
fastrest_core.authority
========================

JWT authority system — sign tokens with private keys, verify tokens against
configured trusted issuers.

Public surface
--------------

**Core authority classes:**

    from fastrest_core.authority import JwtAuthority, MultiKeyAuthority

    # Sign tokens
    authority = JwtAuthority.from_pem(pem_bytes, kid="svc:A", issuer="my-svc", algorithm="RS256")
    token_str = authority.sign(authority.token().subject("usr_1").expires_in(timedelta(hours=1)))

    # Rotate keys (zero-downtime)
    multi = MultiKeyAuthority(authority)
    multi.rotate(JwtAuthority.from_pem(new_pem, kid="svc:B", ...))
    multi.retire("svc:A")  # after old tokens expire

**Trusted issuer registry:**

    from fastrest_core.authority import TrustedIssuerRegistry

    registry = TrustedIssuerRegistry.from_env()
    await registry.load_all()
    token = await registry.verify(raw_token_string)

**Key sources (used internally or for custom config):**

    from fastrest_core.authority.sources import (
        IssuerSource,
        PemFileSource,
        PemFolderSource,
        JwksUrlSource,
        OidcDiscoverySource,
        IssuerSourceFactory,
    )

**Configuration:**

    from fastrest_core.authority import AuthorizationConfig

    config = AuthorizationConfig.from_env()
    registry = config.to_registry()

**Exceptions:**

    from fastrest_core.authority import (
        AuthorityError,
        UnknownKidError,
        IssuerNotFoundError,
        KeyLoadError,
    )

Sub-module layout
-----------------
    fastrest_core/authority/
    ├── jwt_authority.py      — JwtAuthority (single key pair)
    ├── multi_key_authority.py— MultiKeyAuthority (rotation support)
    ├── registry.py           — TrustedIssuerRegistry + TrustedIssuerEntry
    ├── config.py             — AuthorizationConfig (env var parsing)
    ├── exceptions.py         — AuthorityError, UnknownKidError, ...
    └── sources/
        ├── protocol.py       — IssuerSource Protocol
        ├── pem_file.py       — PemFileSource
        ├── pem_folder.py     — PemFolderSource
        ├── jwks_url.py       — JwksUrlSource
        ├── oidc.py           — OidcDiscoverySource
        └── factory.py        — IssuerSourceFactory
"""

from fastrest_core.authority.config import AuthorizationConfig, IssuerConfig
from fastrest_core.authority.exceptions import (
    AuthorityError,
    IssuerNotFoundError,
    KeyLoadError,
    UnknownKidError,
)
from fastrest_core.authority.jwt_authority import JwtAuthority
from fastrest_core.authority.multi_key_authority import MultiKeyAuthority
from fastrest_core.authority.registry import TrustedIssuerEntry, TrustedIssuerRegistry
from fastrest_core.authority.sources.authority import AuthoritySource

__all__ = [
    # ── Authority classes ───────────────────────────────────────────────────
    "JwtAuthority",
    "MultiKeyAuthority",
    # ── Registry ────────────────────────────────────────────────────────────
    "TrustedIssuerRegistry",
    "TrustedIssuerEntry",
    # ── Sources ─────────────────────────────────────────────────────────────
    "AuthoritySource",
    # ── Configuration ────────────────────────────────────────────────────────
    "AuthorizationConfig",
    "IssuerConfig",
    # ── Exceptions ──────────────────────────────────────────────────────────
    "AuthorityError",
    "UnknownKidError",
    "IssuerNotFoundError",
    "KeyLoadError",
]
