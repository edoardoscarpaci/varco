"""
fastrest_core.authority.sources.jwks_url
==========================================

``JwksUrlSource`` — fetches a remote JWKS endpoint and caches the result.

DESIGN: urllib.request over httpx
    ✅ No extra dependency — urllib.request is stdlib.
    ✅ asyncio.to_thread() wraps the blocking call cleanly.
    ❌ No connection pooling — each fetch opens a new TCP connection.
    ❌ No streaming — the full response body is buffered in memory.
    Alternative considered: httpx — provides async HTTP natively and
    connection pooling.  Rejected to avoid adding a hard dependency;
    users can subclass JwksUrlSource and override _sync_fetch() to use
    httpx or requests when they need production-grade HTTP behaviour.

DESIGN: rate-limited refresh
    ✅ Prevents DoS via repeated kid-not-found signals from forged tokens.
    ✅ Returns the cached keyset immediately when the rate limit blocks —
       never returns an empty set just because the limit was hit.
    ❌ Rate limit is per-instance — if you create multiple JwksUrlSource
       instances for the same URL, each has its own counter.  Not an
       issue in normal usage (one instance per URL in the registry).

Thread safety:  ⚠️ _keyset, _loaded_at, _last_refresh are mutable.
                    Concurrent refresh() calls may race on the cache.
                    The registry serialises calls through its own lock, so
                    this is safe in the expected usage pattern.
Async safety:   ✅ HTTP fetch runs in asyncio.to_thread() — event loop safe.
"""

from __future__ import annotations

import asyncio
import json
import time
import urllib.request
import urllib.error
from typing import Any

from fastrest_core.jwk.model import JsonWebKey, JsonWebKeySet

from fastrest_core.authority.exceptions import KeyLoadError


# ── JwksUrlSource ─────────────────────────────────────────────────────────────


class JwksUrlSource:
    """
    Key source that fetches a remote JWKS endpoint.

    On ``load()``, fetches the URL and parses the ``{"keys": [...]}`` JSON
    response.  Caches the result for ``cache_ttl`` seconds.  On
    ``refresh()``, only re-fetches if ``min_refresh_interval`` seconds have
    elapsed since the last refresh — this rate-limits re-fetches triggered
    by kid-not-found signals from forged tokens.

    Thread safety:  ⚠️ See module docstring.
    Async safety:   ✅ Fetch runs via asyncio.to_thread().

    Args:
        url:                  Full URL of the JWKS endpoint
                              (e.g. ``"https://auth.example.com/.well-known/jwks.json"``).
        cache_ttl:            Seconds before the cached keyset is considered
                              stale.  Defaults to 3600 (1 hour).
        min_refresh_interval: Minimum seconds between consecutive refresh()
                              calls.  Defaults to 10 seconds.
        timeout:              HTTP request timeout in seconds.  Defaults to 10.

    Edge cases:
        - HTTP error (4xx, 5xx) → ``KeyLoadError`` (chained from
          ``urllib.error.HTTPError``).
        - Network error / timeout → ``KeyLoadError`` (chained from
          ``urllib.error.URLError``).
        - Response body is not valid JSON → ``KeyLoadError``.
        - ``{"keys": []}`` (empty JWKS) → empty ``JsonWebKeySet``.
          This is valid — the issuer may have no active signing keys yet.
        - A key entry in ``keys`` is missing ``kty`` → ``KeyLoadError``
          (``JsonWebKey.from_dict()`` raises ``KeyError`` on missing ``kty``).

    Example::

        source = JwksUrlSource("https://auth.example.com/.well-known/jwks.json")
        keyset = await source.load()
    """

    __slots__ = (
        "_url",
        "_cache_ttl",
        "_min_refresh_interval",
        "_timeout",
        "_keyset",
        "_loaded_at",
        "_last_refresh",
    )

    def __init__(
        self,
        url: str,
        *,
        cache_ttl: float = 3600.0,
        min_refresh_interval: float = 10.0,
        timeout: float = 10.0,
    ) -> None:
        self._url = url
        self._cache_ttl = cache_ttl
        self._min_refresh_interval = min_refresh_interval
        self._timeout = timeout

        # None until first successful load() call
        self._keyset: JsonWebKeySet | None = None

        # monotonic timestamps for cache/rate-limit decisions
        self._loaded_at: float = 0.0
        self._last_refresh: float = 0.0

    @property
    def source_id(self) -> str:
        """Stable identifier: ``jwks::<url>``."""
        return f"jwks::{self._url}"

    @property
    def is_stale(self) -> bool:
        """
        Return ``True`` when the cached keyset has exceeded ``cache_ttl``.

        Returns:
            ``True`` when no keyset is loaded or the TTL has elapsed.
        """
        return time.monotonic() - self._loaded_at >= self._cache_ttl

    async def load(self) -> JsonWebKeySet:
        """
        Fetch the JWKS endpoint and return the parsed keyset.

        Always makes a live HTTP request regardless of cache state.
        Updates ``_loaded_at`` and ``_last_refresh`` on success.

        Returns:
            ``JsonWebKeySet`` parsed from the remote JWKS response.

        Raises:
            KeyLoadError: HTTP error, network failure, timeout, or malformed
                          JSON / JWK entry.
        """
        keyset = await asyncio.to_thread(self._sync_load)
        now = time.monotonic()
        self._keyset = keyset
        self._loaded_at = now
        self._last_refresh = now
        return keyset

    async def refresh(self) -> JsonWebKeySet:
        """
        Re-fetch the JWKS endpoint, subject to rate-limiting.

        If ``min_refresh_interval`` seconds have not elapsed since the last
        refresh, returns the cached keyset immediately without hitting the
        network.  This prevents a flood of remote requests when many tokens
        with unknown kids arrive in quick succession (e.g. an attacker
        probing the service).

        Returns:
            Up-to-date ``JsonWebKeySet`` — either freshly fetched or cached.

        Raises:
            KeyLoadError: Network/parse failure AND no cached keyset exists
                          to fall back to.
        """
        now = time.monotonic()

        # Rate-limit: only re-fetch if enough time has passed
        if now - self._last_refresh < self._min_refresh_interval:
            # Return cached keyset — do not hit the network
            return self._keyset or JsonWebKeySet(keys=())

        try:
            return await self.load()
        except KeyLoadError:
            # Refresh failed — return stale cache rather than propagating
            # the error.  An expired/stale keyset is better than a hard 500.
            if self._keyset is not None:
                return self._keyset
            raise  # No cache at all — must propagate

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _sync_load(self) -> JsonWebKeySet:
        """
        Blocking HTTP fetch + parse.  Runs in a thread via asyncio.to_thread().

        Returns:
            Parsed ``JsonWebKeySet``.

        Raises:
            KeyLoadError: HTTP error, network error, JSON parse error, or
                          malformed JWK entry.
        """
        try:
            with urllib.request.urlopen(self._url, timeout=self._timeout) as resp:
                body = resp.read()
        except urllib.error.HTTPError as e:
            raise KeyLoadError(
                f"JWKS endpoint {self._url!r} returned HTTP {e.code}: {e.reason}. "
                f"Check that the URL is correct and the server is healthy."
            ) from e
        except urllib.error.URLError as e:
            raise KeyLoadError(
                f"Cannot reach JWKS endpoint {self._url!r}: {e.reason}. "
                f"Check network connectivity and that the URL is reachable."
            ) from e

        try:
            data: dict[str, Any] = json.loads(body)
        except json.JSONDecodeError as e:
            raise KeyLoadError(
                f"JWKS endpoint {self._url!r} returned non-JSON body: {e}. "
                f"Expected a JSON object with a 'keys' array."
            ) from e

        raw_keys: list[dict[str, Any]] = data.get("keys", [])

        try:
            keys = tuple(JsonWebKey.from_dict(k) for k in raw_keys)
        except KeyError as e:
            raise KeyLoadError(
                f"JWKS response from {self._url!r} contains a malformed key entry: "
                f"missing required field {e}. "
                f"Each key must have at least a 'kty' field."
            ) from e

        return JsonWebKeySet(keys=keys)

    def __repr__(self) -> str:
        loaded = self._keyset is not None
        key_count = len(self._keyset.keys) if loaded else 0
        return (
            f"JwksUrlSource("
            f"url={self._url!r}, "
            f"loaded={loaded}, "
            f"keys={key_count}, "
            f"stale={self.is_stale})"
        )
