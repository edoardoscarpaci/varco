"""
varco_fastapi.connection
========================
``HttpConnectionSettings`` — structured, env-var loadable configuration for
an HTTP/REST client connection via httpx.

Environment variables (prefix ``HTTP_``)
----------------------------------------
::

    HTTP_HOST=api.example.com
    HTTP_PORT=443
    HTTP_BASE_URL=https://api.example.com/v1   # overrides host/port
    HTTP_TIMEOUT=30.0

    # TLS (optional)
    HTTP_SSL__CA_CERT=/etc/ssl/api-ca.pem
    HTTP_SSL__VERIFY=true

    # Basic auth (optional)
    HTTP_AUTH__TYPE=basic
    HTTP_AUTH__USERNAME=svc-user
    HTTP_AUTH__PASSWORD=secret

    # OAuth2 static token (optional)
    HTTP_AUTH__TYPE=oauth2
    HTTP_AUTH__TOKEN=eyJhbGciOiJSUzI1NiJ9...

Construction patterns::

    # From env (production)
    conn = HttpConnectionSettings.from_env()

    # Explicit (tests / DI)
    conn = HttpConnectionSettings(base_url="https://api.example.com/v1")

    # With SSL
    conn = HttpConnectionSettings.with_ssl(
        SSLConfig(ca_cert=Path("/etc/ssl/ca.pem")),
        base_url="https://secure-api.example.com",
    )

    # Build httpx client
    async with httpx.AsyncClient(**conn.to_httpx_kwargs()) as client:
        response = await client.get("/users")

DESIGN: HttpConnectionSettings over ClientProfile for structured config
    ✅ Env-var loadable — ClientProfile has no pydantic-settings-based env loading.
    ✅ Composable with ClientProfile via to_trust_store() bridge.
    ✅ OAuth2 static tokens and Basic auth both expressible.
    ❌ OAuth2 token refresh is NOT managed here — a pure config object.
       Use HTTP middleware (JwtMiddleware / OAuth2Middleware) for dynamic tokens.
    ❌ Middleware (retry, correlation ID, OTel tracing) is not configurable here —
       use ClientProfile for those concerns.

Thread safety:  ✅ frozen=True — immutable after construction.
Async safety:   ✅ No I/O; all methods are synchronous.

📚 Docs
- 🔍 https://www.python-httpx.org/api/#asyncclient
  httpx.AsyncClient — base_url, timeout, verify, auth
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from varco_core.connection.auth import BasicAuthConfig, OAuth2Config
from varco_core.connection.base import ConnectionSettings

if TYPE_CHECKING:
    from varco_fastapi.auth.trust_store import TrustStore


# ── HttpConnectionSettings ────────────────────────────────────────────────────


class HttpConnectionSettings(ConnectionSettings):
    """
    Immutable HTTP/REST connection configuration for httpx clients.

    Reads from environment variables with the ``HTTP_`` prefix.
    Nested SSL and auth config use the ``__`` delimiter::

        HTTP_BASE_URL=https://api.example.com
        HTTP_SSL__CA_CERT=/etc/ssl/ca.pem
        HTTP_AUTH__TYPE=basic
        HTTP_AUTH__USERNAME=user
        HTTP_AUTH__PASSWORD=pass

    Attributes:
        host:     API hostname (used when ``base_url`` is empty).
                  Env: ``HTTP_HOST``.
        port:     API port (used when ``base_url`` is empty).
                  Env: ``HTTP_PORT``.
        base_url: Full base URL.  When set, overrides ``host``/``port``.
                  Env: ``HTTP_BASE_URL``.
        timeout:  Default request timeout in seconds.  Env: ``HTTP_TIMEOUT``.
        ssl:      Optional ``SSLConfig`` for TLS verification/mTLS.
                  Populated from ``HTTP_SSL__*`` env vars.
        auth:     Optional ``BasicAuthConfig`` or ``OAuth2Config``.
                  Discriminated by the ``type`` field.
                  Populated from ``HTTP_AUTH__TYPE``, etc.

    Thread safety:  ✅ frozen=True — immutable.
    Async safety:   ✅ No I/O.

    Edge cases:
        - ``base_url`` takes precedence over ``host``/``port`` in all
          ``to_*()`` methods.
        - ``auth=OAuth2Config(token_url=...)`` (client-credentials mode):
          ``to_httpx_kwargs()`` does NOT include auth — token exchange is
          the caller's responsibility (e.g. a middleware or pre-request hook).
        - ``ssl.verify=False`` → ``verify=False`` in httpx kwargs (disables
          all TLS verification).  Use only in dev/testing.

    Example::

        conn = HttpConnectionSettings(
            base_url="https://api.example.com/v1",
            timeout=10.0,
            auth=BasicAuthConfig(username="svc", password="secret"),
        )
        async with httpx.AsyncClient(**conn.to_httpx_kwargs()) as client:
            resp = await client.get("/health")
    """

    model_config = SettingsConfigDict(
        env_prefix="HTTP_",
        env_nested_delimiter="__",
        frozen=True,
    )

    port: int = 443
    """API port (used when ``base_url`` is empty).  Env: ``HTTP_PORT``."""

    base_url: str = ""
    """
    Full base URL string.  Overrides ``host``/``port`` when set.
    Env: ``HTTP_BASE_URL``.
    """

    timeout: float = 30.0
    """Request timeout in seconds.  Env: ``HTTP_TIMEOUT``."""

    auth: Annotated[
        BasicAuthConfig | OAuth2Config | None,
        Field(discriminator="type"),
    ] = None
    """
    Optional auth configuration.  Discriminated by ``type``:
    ``"basic"`` → ``BasicAuthConfig``, ``"oauth2"`` → ``OAuth2Config``.
    Populated from ``HTTP_AUTH__TYPE``, ``HTTP_AUTH__USERNAME``, etc.
    """

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _effective_base_url(self) -> str:
        """
        Return the resolved base URL.

        If ``base_url`` is set, returns it directly.  Otherwise synthesises
        from ``host``/``port``/``ssl``: ``https://`` when ssl is configured,
        ``http://`` otherwise.

        Returns:
            Base URL string (always non-empty after resolution).
        """
        if self.base_url:
            return self.base_url
        scheme = "https" if self.ssl is not None else "http"
        return f"{scheme}://{self.host}:{self.port}"

    # ── Conversion methods ────────────────────────────────────────────────────

    def to_httpx_kwargs(self) -> dict[str, Any]:
        """
        Build kwargs for ``httpx.AsyncClient`` (or ``httpx.Client``).

        The returned dict is ready to unpack into the httpx constructor::

            async with httpx.AsyncClient(**conn.to_httpx_kwargs()) as client:
                ...

        Keys:
        - ``base_url`` — resolved from ``base_url`` or ``host``/``port``/ssl.
        - ``timeout`` — always included.
        - ``verify`` — ``ssl.SSLContext`` (when verify=True), ``False`` (when
          verify=False), or absent (system CAs; no custom ssl config).
        - ``auth`` — ``(username, password)`` tuple for ``BasicAuthConfig``;
          absent for ``OAuth2Config`` (token exchange is the caller's concern).

        Returns:
            Dict of httpx-compatible kwargs.

        Edge cases:
            - OAuth2 client-credentials auth is NOT included — add a middleware
              or pre-request hook to exchange credentials for a token.
            - No ``ssl`` configured → no ``verify`` key → httpx defaults to
              system CAs (verify=True).
        """
        kwargs: dict[str, Any] = {
            "base_url": self._effective_base_url(),
            "timeout": self.timeout,
        }

        if self.ssl is not None:
            if not self.ssl.verify:
                kwargs["verify"] = False
            else:
                kwargs["verify"] = self.ssl.build_ssl_context()

        if isinstance(self.auth, BasicAuthConfig):
            kwargs["auth"] = self.auth.to_tuple()

        return kwargs

    def to_trust_store(self) -> TrustStore | None:
        """
        Convert the SSL config to a ``TrustStore`` for use with ``ClientProfile``.

        Returns ``None`` when no ``ssl`` is configured.

        Useful for bridging to older ``ClientProfile``-based HTTP clients::

            profile = ClientProfile.production(
                trust_store=conn.to_trust_store()
            )

        Returns:
            ``TrustStore`` instance or ``None``.

        Edge cases:
            - ``ssl.verify=False`` is NOT reflected in the returned ``TrustStore``
              (``TrustStore`` always verifies).  If you need ``verify=False``, use
              ``to_httpx_kwargs()["verify"]`` directly.
        """
        if self.ssl is None:
            return None

        from varco_fastapi.auth.trust_store import TrustStore  # noqa: PLC0415

        return TrustStore(
            ca_cert=self.ssl.ca_cert,
            ca_folder=self.ssl.ca_folder,
            client_cert=self.ssl.client_cert,
            client_key=self.ssl.client_key,
        )


__all__ = ["HttpConnectionSettings"]
