"""
varco_core.authority.sources.protocol
==========================================

``IssuerSource`` — structural Protocol that all key source implementations
must satisfy.

DESIGN: Protocol over ABC
    ✅ Structural typing — any class with load()/refresh() methods qualifies,
       no explicit inheritance required.  Makes third-party sources easy to
       integrate without modifying their class hierarchy.
    ✅ @runtime_checkable — isinstance() works, enabling registry validation.
    ❌ No default method implementations — each source must implement both
       methods fully.  A base class would allow shared retry/logging logic,
       but adds coupling.  Use composition (e.g. a caching wrapper) instead.

Thread safety:  ✅ Protocol definition only — no shared state.
Async safety:   ✅ Both methods are async — implementations must not block.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from varco_core.jwk.model import JsonWebKeySet


# ── IssuerSource Protocol ─────────────────────────────────────────────────────


@runtime_checkable
class IssuerSource(Protocol):
    """
    Structural protocol for pluggable JWT key sources.

    Any class that exposes ``source_id``, ``load()``, and ``refresh()`` with
    matching signatures satisfies this protocol — no explicit inheritance
    needed.  This allows first-party sources (``PemFileSource``,
    ``JwksUrlSource``, etc.) and user-provided custom sources to coexist
    without a shared base class.

    Lifecycle
    ---------
    1. ``TrustedIssuerRegistry.register()`` stores the source.
    2. ``TrustedIssuerRegistry.load_all()`` calls ``load()`` on every source
       at startup.
    3. On a ``kid``-not-found signal during verification, the registry calls
       ``refresh()`` on all sources — remote sources re-fetch, file sources
       re-read, folder sources re-scan for new files.

    Thread safety:  ⚠️ Depends on implementation.  Sources that cache keysets
                    internally must document their own safety contract.
    Async safety:   ✅ Both methods are async — implementations must use async
                    I/O or wrap sync I/O with ``asyncio.to_thread()``.

    Edge cases:
        - ``load()`` may be called multiple times — implementations must be
          idempotent (return a fresh keyset each time).
        - ``refresh()`` should be rate-limited internally to prevent DoS via
          repeated kid-not-found signals from forged tokens.
        - An empty ``JsonWebKeySet`` is a valid return — the registry treats
          it as "no keys available from this source right now".
    """

    @property
    def source_id(self) -> str:
        """
        Stable human-readable identifier for this source.

        Used in log messages and error output.  Should encode the source type
        and location, e.g. ``"pem::/etc/certs/service.pem"``,
        ``"jwks::https://auth.example.com/.well-known/jwks.json"``.

        Returns:
            Non-empty string identifying this source.
        """
        ...

    async def load(self) -> JsonWebKeySet:
        """
        Load the keyset from this source for the first time (or force-reload).

        Called by ``TrustedIssuerRegistry.load_all()`` at startup.
        Implementations should perform the full load operation — read file,
        fetch URL, etc. — regardless of any cached state.

        Returns:
            ``JsonWebKeySet`` containing all currently valid public keys from
            this source.  May be empty if the source has no keys yet.

        Raises:
            KeyLoadError: The source could not be loaded (file missing,
                          network error, malformed data, etc.).
        """
        ...

    async def refresh(self) -> JsonWebKeySet:
        """
        Refresh the keyset, returning the current set of valid public keys.

        Called when a ``kid``-not-found signal is received during token
        verification — this is the hint that a remote source has rotated keys.

        Implementations should:
        - Rate-limit re-fetches to prevent DoS.
        - For static sources (PEM file), re-read the file in case it was
          updated on disk.
        - For remote sources (JWKS URL), re-fetch only if the minimum
          inter-refresh interval has elapsed.
        - Return the previously cached keyset when the rate limit blocks a
          re-fetch — never return an empty set just because the limit was hit.

        Returns:
            ``JsonWebKeySet`` — the latest known keyset, possibly cached.

        Raises:
            KeyLoadError: The refresh failed AND no cached keyset is available
                          to fall back to.
        """
        ...
