"""
varco_fastapi.auth.client_auth
==============================
Client-side authentication helpers for outbound HTTP requests.

Each ``AbstractClientAuth`` subclass generates the HTTP headers that authenticate
``AsyncVarcoClient`` / ``SyncVarcoClient`` requests to downstream services.

Hierarchy::

    AbstractClientAuth (ABC)
      ├── BearerTokenAuth     — static Bearer token header
      ├── JwtClientAuth       — signs a fresh JWT from JwtAuthority; caches until near expiry
      ├── ApiKeyClientAuth    — injects X-API-Key (or any header) with a static key
      ├── ForwardingClientAuth— forwards the originating request's Bearer token
      └── CompositeClientAuth — merge multiple ClientAuth instances

Usage (in client middleware)::

    auth = JwtClientAuth(authority=my_authority)
    headers = await auth.inject_headers()
    # {'Authorization': 'Bearer eyJhbGciOiJSUzI1NiJ9...'}

DESIGN: async inject_headers() → dict over request mutation
    ✅ Simple interface — auth strategies don't need Request access
    ✅ Composable via CompositeClientAuth (merge dicts in order)
    ✅ Async-ready — JwtClientAuth can refresh the token asynchronously
    ❌ No access to the specific request being made — use
       AbstractClientMiddleware for request-aware auth

Thread safety:  ✅ Implementations must be safe to call concurrently from
                   multiple coroutines (important for connection pools).
Async safety:   ✅ All ``inject_headers()`` methods are ``async def``.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import timedelta
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from varco_core.authority import JwtAuthority

_logger = logging.getLogger(__name__)

# How many seconds before a cached JWT expires to refresh it proactively.
_JWT_REFRESH_BUFFER_SECONDS: int = 30


# ── AbstractClientAuth ────────────────────────────────────────────────────────


class AbstractClientAuth(ABC):
    """
    Produces authentication headers for outbound HTTP requests.

    Implementations cache tokens where applicable (e.g. ``JwtClientAuth``
    caches the signed JWT until it nears expiry).

    Thread safety:  ✅ Implementations must not store per-request mutable state.
    Async safety:   ✅ ``inject_headers()`` is ``async def``.
    """

    @abstractmethod
    async def inject_headers(self) -> dict[str, str]:
        """
        Return a dict of HTTP headers to add to outbound requests.

        Returns:
            A dict of header name → value pairs.  Keys are canonical header
            names (e.g. ``"Authorization"``, ``"X-API-Key"``).

        Raises:
            Any error from the underlying credential source (key loading,
            network errors for token refresh, etc.).

        Edge cases:
            - Implementations should cache tokens and refresh only when near
              expiry — do NOT make a new request to the auth service per call.
            - Return an empty dict (``{}``) if no auth headers are needed for
              this particular auth strategy.

        Thread safety:  ✅ Must be safe for concurrent coroutine calls.
        Async safety:   ✅ Always ``async def`` for uniformity.
        """


# ── BearerTokenAuth ───────────────────────────────────────────────────────────


class BearerTokenAuth(AbstractClientAuth):
    """
    Inject a static Bearer token into the ``Authorization`` header.

    Use for:
    - Internal service accounts with a long-lived token.
    - Development / testing with a fixed test token.

    Args:
        token: The raw JWT or opaque token string (without ``"Bearer "`` prefix).

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ ``inject_headers()`` is ``async def``; no I/O.
    """

    def __init__(self, token: str) -> None:
        self._token = token

    async def inject_headers(self) -> dict[str, str]:
        """Returns ``{"Authorization": "Bearer <token>"}``."""
        return {"Authorization": f"Bearer {self._token}"}


# ── JwtClientAuth ─────────────────────────────────────────────────────────────


class JwtClientAuth(AbstractClientAuth):
    """
    Sign a fresh JWT from ``JwtAuthority`` and cache it until near expiry.

    On first call, signs a new JWT with the configured TTL.  On subsequent
    calls, returns the cached token unless it expires within
    ``_JWT_REFRESH_BUFFER_SECONDS`` (default 30 s).

    Args:
        authority:          ``JwtAuthority`` instance for signing tokens.
        ttl:                Token lifetime.  Default: 15 minutes.
        subject:            ``sub`` claim for the signed token.
                            Identifies this service in downstream audit logs.
                            Default: ``authority.issuer``.
        extra_claims:       Additional claims to include in the token body.
        refresh_buffer_secs: How many seconds before expiry to proactively
                             refresh.  Default: 30.

    DESIGN: proactive refresh over on-demand refresh
        ✅ Avoids the thundering-herd problem when many requests hit expiry
           simultaneously — token is refreshed before it expires
        ✅ Thread-safe via asyncio.Lock (lazy-created inside sign())
        ❌ Adds a small constant overhead per cache check (~microseconds)

    Thread safety:  ✅ ``asyncio.Lock`` prevents concurrent sign() calls.
    Async safety:   ✅ Lock is lazily created on first call to stay within
                       the running event loop.

    Edge cases:
        - The lock is created lazily in ``inject_headers()`` — NOT in
          ``__init__`` — to avoid the "no running event loop" error.
        - ``JwtAuthority.sign()`` is synchronous in varco_core — the lock is
          still useful to serialize cache writes from concurrent coroutines.
    """

    def __init__(
        self,
        authority: JwtAuthority,
        *,
        ttl: timedelta = timedelta(minutes=15),
        subject: str | None = None,
        extra_claims: dict[str, object] | None = None,
        refresh_buffer_secs: int = _JWT_REFRESH_BUFFER_SECONDS,
    ) -> None:
        self._authority = authority
        self._ttl = ttl
        self._subject = subject
        self._extra_claims = extra_claims or {}
        self._refresh_buffer = refresh_buffer_secs

        # Lazy-created lock — must NOT be created in __init__ to avoid
        # "no running event loop" error on module import or test setup.
        self._lock: asyncio.Lock | None = None

        # Cached token and its expiry timestamp (seconds since epoch, UTC).
        self._cached_token: str | None = None
        self._cached_expiry: float = 0.0

    async def inject_headers(self) -> dict[str, str]:
        """
        Returns ``{"Authorization": "Bearer <signed_jwt>"}``.

        Signs a new JWT if the cache is empty or the token will expire within
        ``refresh_buffer_secs``.
        """
        import time

        if self._lock is None:
            self._lock = asyncio.Lock()

        now = time.time()
        if self._cached_token and (self._cached_expiry - now) > self._refresh_buffer:
            return {"Authorization": f"Bearer {self._cached_token}"}

        async with self._lock:
            # Double-check under the lock — another coroutine may have refreshed
            now = time.time()
            if (
                self._cached_token
                and (self._cached_expiry - now) > self._refresh_buffer
            ):
                return {"Authorization": f"Bearer {self._cached_token}"}

            # Sign a fresh token
            builder = self._authority.token().expires_in(self._ttl)
            subject = self._subject or self._authority.issuer
            builder = builder.subject(subject)
            for key, value in self._extra_claims.items():
                builder = builder.claim(key, value)

            signed = self._authority.sign(builder)
            self._cached_token = signed.raw if hasattr(signed, "raw") else str(signed)
            # Record when this token expires
            self._cached_expiry = now + self._ttl.total_seconds()

            _logger.debug(
                "JwtClientAuth: signed new service token (sub=%r, ttl=%s).",
                subject,
                self._ttl,
            )

        return {"Authorization": f"Bearer {self._cached_token}"}


# ── ApiKeyClientAuth ──────────────────────────────────────────────────────────


class ApiKeyClientAuth(AbstractClientAuth):
    """
    Inject a static API key as an HTTP header.

    Args:
        api_key: The API key value to inject.
        header:  Header name.  Default: ``"X-API-Key"``.

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ No I/O.
    """

    def __init__(self, api_key: str, *, header: str = "X-API-Key") -> None:
        self._api_key = api_key
        self._header = header

    async def inject_headers(self) -> dict[str, str]:
        """Returns ``{header: api_key}``."""
        return {self._header: self._api_key}


# ── ForwardingClientAuth ──────────────────────────────────────────────────────


class ForwardingClientAuth(AbstractClientAuth):
    """
    Forward the original request's Bearer token to downstream services.

    Reads the current request's raw JWT from ``request_token_var`` (set by
    ``RequestContextMiddleware``) and injects it as-is into outbound requests.

    Use for service-to-service calls where the downstream service should see
    the end-user's token (e.g. for audit logging or fine-grained authorization).

    WARNING: Only forward tokens to services you trust with the user's identity.

    Thread safety:  ✅ Reads ContextVar — task-local, no shared state.
    Async safety:   ✅ ``inject_headers()`` is ``async def``; no I/O.

    Edge cases:
        - Returns ``{}`` if no token is in the current context (anonymous request,
          or called outside a request context).
        - The forwarded token may be near expiry — the downstream service must
          handle ``401 Unauthorized`` and the client must retry with a fresh token
          if needed.
    """

    async def inject_headers(self) -> dict[str, str]:
        """
        Returns ``{"Authorization": "Bearer <forwarded_token>"}`` or ``{}``.
        """
        from varco_fastapi.context import get_request_token

        token = get_request_token()
        if token:
            return {"Authorization": f"Bearer {token}"}
        return {}


# ── CompositeClientAuth ───────────────────────────────────────────────────────


class CompositeClientAuth(AbstractClientAuth):
    """
    Merge headers from multiple ``AbstractClientAuth`` instances.

    Later strategies in the list override earlier ones for the same header key.
    Use this to combine a service identity token with tenant headers or
    correlation IDs.

    Args:
        strategies: List of ``AbstractClientAuth`` instances.  Evaluated in order.

    Thread safety:  ✅ Delegates to each strategy; no mutable state.
    Async safety:   ✅ Awaits each strategy's ``inject_headers()`` in sequence.

    Edge cases:
        - Empty ``strategies`` list produces ``{}``.
        - If two strategies produce the same header key, the LAST one wins.

    Example::

        auth = CompositeClientAuth([
            JwtClientAuth(authority=svc_authority),
            ApiKeyClientAuth(api_key="internal-secret", header="X-Service-Key"),
        ])
        # Produces: {"Authorization": "Bearer ...", "X-Service-Key": "internal-secret"}
    """

    def __init__(self, strategies: list[AbstractClientAuth]) -> None:
        self._strategies = strategies

    async def inject_headers(self) -> dict[str, str]:
        """
        Returns merged headers from all strategies.
        Later strategies override earlier ones for duplicate keys.
        """
        merged: dict[str, str] = {}
        for strategy in self._strategies:
            headers = await strategy.inject_headers()
            merged.update(headers)
        return merged


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "AbstractClientAuth",
    "BearerTokenAuth",
    "JwtClientAuth",
    "ApiKeyClientAuth",
    "ForwardingClientAuth",
    "CompositeClientAuth",
]
