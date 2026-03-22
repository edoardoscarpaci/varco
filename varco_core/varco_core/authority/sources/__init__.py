"""
varco_core.authority.sources
================================

Pluggable key sources for the ``TrustedIssuerRegistry``.

Each source implements the ``IssuerSource`` Protocol and knows how to load /
refresh a ``JsonWebKeySet`` from a specific backend:

    ``PemFileSource``       — single PEM file on disk
    ``PemFolderSource``     — directory of PEM files (supports hot-add)
    ``JwksUrlSource``       — remote JWKS endpoint (with caching + rate-limit)
    ``OidcDiscoverySource`` — OIDC issuer URL (auto-discovers JWKS URI)
    ``AuthoritySource``     — in-memory JwtAuthority / MultiKeyAuthority adapter
    ``IssuerSourceFactory`` — parses a string value into the right source

Source descriptor syntax
-------------------------
    ``pem::<path>``           — PemFileSource
    ``pem-folder::<path>``    — PemFolderSource
    ``jwks::<url>``           — JwksUrlSource
    ``oidc::<url>``           — OidcDiscoverySource
    heuristic fallback        — see IssuerSourceFactory.from_string()
    (no string form)          — AuthoritySource is always constructed directly
"""

from varco_core.authority.sources.authority import AuthoritySource
from varco_core.authority.sources.factory import IssuerSourceFactory
from varco_core.authority.sources.jwks_url import JwksUrlSource
from varco_core.authority.sources.oidc import OidcDiscoverySource
from varco_core.authority.sources.pem_file import PemFileSource
from varco_core.authority.sources.pem_folder import PemFolderSource
from varco_core.authority.sources.protocol import IssuerSource

__all__ = [
    "IssuerSource",
    "PemFileSource",
    "PemFolderSource",
    "JwksUrlSource",
    "OidcDiscoverySource",
    "AuthoritySource",
    "IssuerSourceFactory",
]
