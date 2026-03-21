"""
fastrest_core.authority.sources.factory
=========================================

``IssuerSourceFactory`` — converts a raw string value (from an env var or
config file) into the appropriate ``IssuerSource`` implementation.

Prefix dispatch table
---------------------
    ``pem::<path>``           → ``PemFileSource`` — single PEM file
    ``pem-folder::<path>``    → ``PemFolderSource`` — directory of PEM files
    ``jwks::<url>``           → ``JwksUrlSource`` — remote JWKS endpoint
    ``oidc::<url>``           → ``OidcDiscoverySource`` — OIDC discovery

Heuristic fallback (no prefix)
-------------------------------
    Starts with ``"/"`` or ``"./"``        → ``PemFileSource`` (local path)
    Starts with ``"file://"``              → ``PemFileSource`` (file URI)
    Ends with ``"jwks.json"``              → ``JwksUrlSource``
    Any other ``https://`` / ``http://``   → ``OidcDiscoverySource``

DESIGN: explicit prefix over pure heuristics
    ✅ Unambiguous — ``pem-folder::`` vs ``pem::`` is crystal clear.
    ✅ Heuristic fallback handles common cases without requiring a prefix,
       improving DX for simple setups.
    ❌ Heuristic is opinionated — an https:// URL to a non-OIDC JWKS that
       doesn't end in ``jwks.json`` requires the ``jwks::`` prefix.
    Alternative considered: separate config fields (URL + type).  Rejected
    because the prefix approach keeps it to one env var per issuer.

Thread safety:  ✅ Stateless factory — all methods are classmethods.
Async safety:   ✅ Pure transformation — no I/O.
"""

from __future__ import annotations

from pathlib import Path

from fastrest_core.authority.sources.jwks_url import JwksUrlSource
from fastrest_core.authority.sources.oidc import OidcDiscoverySource
from fastrest_core.authority.sources.pem_file import PemFileSource
from fastrest_core.authority.sources.pem_folder import PemFolderSource
from fastrest_core.authority.sources.protocol import IssuerSource


# ── IssuerSourceFactory ───────────────────────────────────────────────────────


class IssuerSourceFactory:
    """
    Stateless factory that parses a source string into an ``IssuerSource``.

    All methods are classmethods — no instance is needed.

    Thread safety:  ✅ Stateless — safe to call from any context.
    Async safety:   ✅ Pure transformation — no I/O.

    Example::

        source = IssuerSourceFactory.from_string(
            "pem::/etc/certs/my-service.pem",
            algorithm="RS256",
        )
        # → PemFileSource(path=Path('/etc/certs/my-service.pem'), kid='my-service', ...)

        source = IssuerSourceFactory.from_string(
            "https://accounts.google.com",
        )
        # → OidcDiscoverySource(issuer='https://accounts.google.com')
    """

    @classmethod
    def from_string(
        cls,
        value: str,
        *,
        algorithm: str = "RS256",
        use: str = "sig",
    ) -> IssuerSource:
        """
        Parse a source descriptor string and return the matching ``IssuerSource``.

        The ``algorithm`` and ``use`` parameters only apply to PEM-based sources
        (``PemFileSource``, ``PemFolderSource``).  URL-based sources
        (``JwksUrlSource``, ``OidcDiscoverySource``) derive algorithm from
        the ``alg`` field in the remote JWKS.

        Args:
            value:     Source descriptor.  See module docstring for prefix
                       dispatch table and heuristic fallback rules.
            algorithm: Default JWT algorithm for PEM sources.  Defaults to
                       ``"RS256"``.  Ignored for URL-based sources.
            use:       JWK public key use for PEM sources.  Defaults to
                       ``"sig"``.  Ignored for URL-based sources.

        Returns:
            The appropriate ``IssuerSource`` for the given descriptor.

        Raises:
            ValueError: The value is empty or cannot be classified.

        Edge cases:
            - ``value`` is stripped of leading/trailing whitespace before
              parsing — env vars sometimes have accidental spaces.
            - ``pem::`` with a relative path → ``Path`` object; the source
              will resolve it relative to the process working directory at
              load() time.
            - ``pem-folder::`` with a relative path → same behaviour.
            - An https:// URL ending in ``/jwks.json`` triggers ``JwksUrlSource``
              via heuristic, not ``OidcDiscoverySource`` — be explicit with
              ``jwks::`` when in doubt.

        Example::

            # Explicit prefix — always unambiguous
            IssuerSourceFactory.from_string("pem::/etc/certs/service.pem")
            IssuerSourceFactory.from_string("pem-folder::/etc/certs/user-keys/")
            IssuerSourceFactory.from_string("jwks::https://auth.example.com/.well-known/jwks.json")
            IssuerSourceFactory.from_string("oidc::https://accounts.google.com")

            # Heuristic fallback
            IssuerSourceFactory.from_string("/etc/certs/service.pem")  # → PemFileSource
            IssuerSourceFactory.from_string("https://accounts.google.com")  # → OidcDiscoverySource
        """
        value = value.strip()

        if not value:
            raise ValueError(
                "Source descriptor cannot be empty. "
                "Provide a path (e.g. 'pem::/etc/certs/key.pem') or URL "
                "(e.g. 'oidc::https://accounts.google.com')."
            )

        # ── Explicit prefix dispatch ───────────────────────────────────────────

        if value.startswith("pem-folder::"):
            # pem-folder:: must be checked BEFORE pem:: — startswith("pem::")
            # would also match "pem-folder::" on a naive check.
            path = Path(value[len("pem-folder::") :])
            return PemFolderSource(path=path, algorithm=algorithm, use=use)

        if value.startswith("pem::"):
            path = Path(value[len("pem::") :])
            # Use filename stem as kid — e.g. "auth-2025-A.pem" → "auth-2025-A"
            kid = path.stem
            return PemFileSource(path=path, kid=kid, algorithm=algorithm, use=use)

        if value.startswith("jwks::"):
            return JwksUrlSource(value[len("jwks::") :])

        if value.startswith("oidc::"):
            return OidcDiscoverySource(value[len("oidc::") :])

        # ── Heuristic fallback ─────────────────────────────────────────────────

        # Local filesystem paths
        if (
            value.startswith("/")
            or value.startswith("./")
            or value.startswith("../")
            or value.startswith("file://")
        ):
            # Strip file:// URI scheme if present
            path_str = value[len("file://") :] if value.startswith("file://") else value
            path = Path(path_str)

            # If it looks like a directory (no extension), treat as folder
            if path.suffix == "":
                return PemFolderSource(path=path, algorithm=algorithm, use=use)

            kid = path.stem
            return PemFileSource(path=path, kid=kid, algorithm=algorithm, use=use)

        # Remote URLs
        if value.startswith("https://") or value.startswith("http://"):
            # Explicit JWKS URL pattern: ends with jwks.json
            if value.endswith("jwks.json") or "jwks.json?" in value:
                return JwksUrlSource(value)
            # Everything else assumed to be an OIDC issuer base URL
            return OidcDiscoverySource(value)

        raise ValueError(
            f"Cannot determine source type for {value!r}. "
            f"Use an explicit prefix: "
            f"'pem::<path>', 'pem-folder::<path>', "
            f"'jwks::<url>', or 'oidc::<url>'."
        )
