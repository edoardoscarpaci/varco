"""
varco_core.authority.sources.oidc
======================================

``OidcDiscoverySource`` — discovers a JWKS URL via OpenID Connect Discovery
and delegates all key-fetching to ``JwksUrlSource``.

The OIDC Discovery specification (OpenID Connect Discovery 1.0 §4) defines a
``/.well-known/openid-configuration`` document at a known URL under each
issuer.  That document contains a ``jwks_uri`` field pointing to the JWKS
endpoint.  This source automates the two-step fetch:

    1. GET ``{issuer_url}/.well-known/openid-configuration``
    2. Extract ``jwks_uri``
    3. Delegate to ``JwksUrlSource(jwks_uri)`` for all further fetches.

DESIGN: delegation to JwksUrlSource
    ✅ Reuses all of JwksUrlSource's caching, rate-limiting, and error
       handling — no duplication.
    ✅ Discovery is done once on first load() — the result is cached.
    ❌ If the OIDC discovery document changes ``jwks_uri`` after startup,
       the change is NOT detected.  Re-discovery requires restarting the
       process or calling load() again.  In practice, issuer jwks_uri values
       are stable — Google's has never changed.

Thread safety:  ⚠️ _delegate is set lazily on first load() — concurrent
                    first calls may race to discover and both set _delegate.
                    This is benign (both get the same jwks_uri) but wastes a
                    network request.  The registry serialises calls.
Async safety:   ✅ Discovery fetch runs in asyncio.to_thread().
"""

from __future__ import annotations

import asyncio
import json
import urllib.request
import urllib.error
from typing import Any

from varco_core.jwk.model import JsonWebKeySet

from varco_core.authority.exceptions import KeyLoadError
from varco_core.authority.sources.jwks_url import JwksUrlSource


# ── OidcDiscoverySource ───────────────────────────────────────────────────────


class OidcDiscoverySource:
    """
    Key source that auto-discovers a JWKS endpoint via OIDC Discovery.

    Pass the issuer base URL (e.g. ``"https://accounts.google.com"``).
    The source appends ``/.well-known/openid-configuration`` automatically,
    fetches the metadata document, extracts ``jwks_uri``, and delegates to
    a ``JwksUrlSource`` for all key-fetching.

    Thread safety:  ⚠️ See module docstring.
    Async safety:   ✅ Discovery and JWKS fetches run via asyncio.to_thread().

    Args:
        issuer_url:           Issuer base URL.  May or may not have a trailing
                              slash — it is stripped before appending the
                              discovery path.
        cache_ttl:            Forwarded to the internal ``JwksUrlSource``.
        min_refresh_interval: Forwarded to the internal ``JwksUrlSource``.
        timeout:              HTTP timeout in seconds for both the discovery
                              fetch and all subsequent JWKS fetches.

    Edge cases:
        - If ``issuer_url`` already ends with
          ``/.well-known/openid-configuration``, it is used directly as the
          discovery URL without appending the path again.
        - Discovery document missing ``jwks_uri`` → ``KeyLoadError``.
        - Discovery endpoint returns HTTP error → ``KeyLoadError``.
        - After first successful ``load()``, the delegate is cached — the
          discovery document is NOT re-fetched on ``refresh()``.

    Example::

        source = OidcDiscoverySource("https://accounts.google.com")
        keyset = await source.load()
        # Fetched: https://accounts.google.com/.well-known/openid-configuration
        # Extracted jwks_uri, then fetched it to build the keyset
    """

    _DISCOVERY_SUFFIX: str = "/.well-known/openid-configuration"

    __slots__ = (
        "_issuer_url",
        "_discovery_url",
        "_cache_ttl",
        "_min_refresh_interval",
        "_timeout",
        "_delegate",
    )

    def __init__(
        self,
        issuer_url: str,
        *,
        cache_ttl: float = 3600.0,
        min_refresh_interval: float = 10.0,
        timeout: float = 10.0,
    ) -> None:
        # Strip trailing slash for consistent URL construction
        self._issuer_url: str = issuer_url.rstrip("/")

        # If the caller already included the full discovery path, use as-is.
        # Otherwise append the standard OIDC discovery suffix.
        if issuer_url.endswith(self._DISCOVERY_SUFFIX):
            self._discovery_url: str = self._issuer_url
        else:
            self._discovery_url = self._issuer_url + self._DISCOVERY_SUFFIX

        self._cache_ttl: float = cache_ttl
        self._min_refresh_interval: float = min_refresh_interval
        self._timeout: float = timeout

        # Delegate is None until first load() — discovery is lazy so startup
        # does not block on a remote request when the issuer is offline.
        self._delegate: JwksUrlSource | None = None

    @property
    def source_id(self) -> str:
        """Stable identifier: ``oidc::<issuer_url>``."""
        return f"oidc::{self._issuer_url}"

    async def load(self) -> JsonWebKeySet:
        """
        Discover the JWKS URL (if not already known) and fetch the keyset.

        On the first call, fetches ``{issuer_url}/.well-known/openid-configuration``,
        extracts ``jwks_uri``, and delegates to ``JwksUrlSource.load()``.
        On subsequent calls, re-uses the cached delegate and calls
        ``JwksUrlSource.load()`` (which always re-fetches the JWKS URL).

        Returns:
            ``JsonWebKeySet`` from the issuer's JWKS endpoint.

        Raises:
            KeyLoadError: Discovery document is unreachable, missing
                          ``jwks_uri``, or the JWKS fetch fails.
        """
        if self._delegate is None:
            jwks_uri = await asyncio.to_thread(self._sync_discover)
            self._delegate = JwksUrlSource(
                jwks_uri,
                cache_ttl=self._cache_ttl,
                min_refresh_interval=self._min_refresh_interval,
                timeout=self._timeout,
            )
        return await self._delegate.load()

    async def refresh(self) -> JsonWebKeySet:
        """
        Refresh the underlying JWKS source.

        If discovery has not run yet, triggers a full ``load()``.  Otherwise,
        delegates to the cached ``JwksUrlSource.refresh()``, which applies
        rate-limiting internally.

        Returns:
            Up-to-date ``JsonWebKeySet`` — cached or freshly fetched.

        Raises:
            KeyLoadError: Same conditions as ``load()``.
        """
        if self._delegate is None:
            # Discovery hasn't run yet — do a full load
            return await self.load()
        return await self._delegate.refresh()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _sync_discover(self) -> str:
        """
        Blocking fetch of the OIDC discovery document.

        Runs in a thread via asyncio.to_thread() in load().

        Returns:
            The ``jwks_uri`` value from the discovery document.

        Raises:
            KeyLoadError: HTTP error, network failure, malformed JSON, or
                          ``jwks_uri`` is absent from the document.
        """
        try:
            with urllib.request.urlopen(
                self._discovery_url, timeout=self._timeout
            ) as resp:
                body = resp.read()
        except urllib.error.HTTPError as e:
            raise KeyLoadError(
                f"OIDC discovery endpoint {self._discovery_url!r} returned "
                f"HTTP {e.code}: {e.reason}. "
                f"Verify the issuer URL is correct (e.g. 'https://accounts.google.com' "
                f"not 'https://accounts.google.com/auth')."
            ) from e
        except urllib.error.URLError as e:
            raise KeyLoadError(
                f"Cannot reach OIDC discovery endpoint {self._discovery_url!r}: "
                f"{e.reason}."
            ) from e

        try:
            config: dict[str, Any] = json.loads(body)
        except json.JSONDecodeError as e:
            raise KeyLoadError(
                f"OIDC discovery endpoint {self._discovery_url!r} returned "
                f"non-JSON body: {e}."
            ) from e

        jwks_uri: str | None = config.get("jwks_uri")
        if jwks_uri is None:
            raise KeyLoadError(
                f"OIDC discovery document at {self._discovery_url!r} is missing "
                f"the 'jwks_uri' field. "
                f"This field is required by OpenID Connect Discovery 1.0 §3. "
                f"The document contained these keys: {sorted(config.keys())!r}."
            )

        return jwks_uri

    def __repr__(self) -> str:
        return (
            f"OidcDiscoverySource("
            f"issuer={self._issuer_url!r}, "
            f"discovered={self._delegate is not None})"
        )
