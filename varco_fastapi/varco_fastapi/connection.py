"""
varco_fastapi.connection
========================
``HttpConnectionSettings`` — structured, env-var loadable configuration for
an HTTP/REST client connection via httpx.

Unlike the other backend connection settings (Postgres, Redis, Kafka) this
class has **no fixed env-var prefix**.  A service typically talks to many
different HTTP APIs (payment, notification, inventory …) and a single ``HTTP_``
prefix would only allow one of them to be configured from env vars.

Instead, supply a prefix when loading from env:

Environment variables — caller-supplied prefix
----------------------------------------------
::

    # Two independent HTTP client configs in the same process:
    PAYMENT_API_BASE_URL=https://pay.example.com
    PAYMENT_API_TIMEOUT=5.0
    PAYMENT_API_AUTH__TYPE=basic
    PAYMENT_API_AUTH__USERNAME=svc
    PAYMENT_API_AUTH__PASSWORD=secret

    NOTIF_API_BASE_URL=https://notify.example.com
    NOTIF_API_SSL__CA_CERT=/etc/ssl/notify-ca.pem

Construction patterns::

    # From env — prefix is REQUIRED (one per client)
    payment = HttpConnectionSettings.from_env(prefix="PAYMENT_API_")
    notify  = HttpConnectionSettings.from_env(prefix="NOTIF_API_")

    # Explicit (tests / DI — no env reading)
    conn = HttpConnectionSettings(base_url="https://api.example.com/v1")

    # With SSL (no env reading)
    conn = HttpConnectionSettings.with_ssl(
        SSLConfig(ca_cert=Path("/etc/ssl/ca.pem")),
        base_url="https://secure-api.example.com",
    )

    # Build httpx client
    async with httpx.AsyncClient(**conn.to_httpx_kwargs()) as client:
        response = await client.get("/users")

DESIGN: no fixed env_prefix — caller must supply one via from_env(prefix=...)
    ✅ Multiple HTTP clients can be configured from env vars in the same process.
    ✅ Explicit prefix at the call site documents which service is being configured.
    ❌ from_env() now requires a prefix argument — slightly more verbose than the
       other connection settings.  This is intentional: HTTP is the only backend
       where multiple independent clients per process is the common case.

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
- 🔍 https://docs.pydantic.dev/latest/concepts/pydantic_settings/#changing-priority
  pydantic-settings — model_config, env_prefix, how env sources are built
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

    Unlike Postgres/Redis/Kafka settings, this class has **no fixed env-var
    prefix**.  A single service often calls multiple HTTP APIs, so a hardcoded
    ``HTTP_`` prefix would only allow one client to be configured from env vars.
    Use ``from_env(prefix=...)`` to supply the prefix at the call site::

        payment = HttpConnectionSettings.from_env(prefix="PAYMENT_API_")
        notify  = HttpConnectionSettings.from_env(prefix="NOTIF_API_")

    Nested SSL and auth config use the ``__`` delimiter::

        PAYMENT_API_BASE_URL=https://pay.example.com
        PAYMENT_API_SSL__CA_CERT=/etc/ssl/ca.pem
        PAYMENT_API_AUTH__TYPE=basic
        PAYMENT_API_AUTH__USERNAME=user
        PAYMENT_API_AUTH__PASSWORD=pass

    Attributes:
        host:     API hostname (used when ``base_url`` is empty).
                  Env: ``{PREFIX}HOST``.
        port:     API port (used when ``base_url`` is empty).
                  Env: ``{PREFIX}PORT``.
        base_url: Full base URL.  When set, overrides ``host``/``port``.
                  Env: ``{PREFIX}BASE_URL``.
        timeout:  Default request timeout in seconds.  Env: ``{PREFIX}TIMEOUT``.
        ssl:      Optional ``SSLConfig`` for TLS verification/mTLS.
                  Populated from ``{PREFIX}SSL__*`` env vars.
        auth:     Optional ``BasicAuthConfig`` or ``OAuth2Config``.
                  Discriminated by the ``type`` field.
                  Populated from ``{PREFIX}AUTH__TYPE``, etc.

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
        - Calling ``HttpConnectionSettings()`` directly (without ``from_env()``)
          does NOT read any env vars — all values come from the arguments you pass.
          Only ``from_env(prefix=...)`` triggers env-var reading.

    Example::

        conn = HttpConnectionSettings(
            base_url="https://api.example.com/v1",
            timeout=10.0,
            auth=BasicAuthConfig(username="svc", password="secret"),
        )
        async with httpx.AsyncClient(**conn.to_httpx_kwargs()) as client:
            resp = await client.get("/health")
    """

    # DESIGN: no env_prefix — subclasses or from_env(prefix=...) supply it.
    # A fixed "HTTP_" prefix would only allow one HTTP client per process to
    # be configured from env vars, which is the common case for all other
    # backends but NOT for HTTP (you typically talk to many external services).
    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        frozen=True,
    )

    # ── from_env override ─────────────────────────────────────────────────────

    @classmethod
    def from_env(cls, prefix: str) -> "HttpConnectionSettings":  # type: ignore[override]
        """
        Load HTTP connection settings from environment variables.

        Unlike other ``ConnectionSettings`` subclasses, ``HttpConnectionSettings``
        has no fixed env-var prefix.  The ``prefix`` argument is **required** so
        that multiple HTTP clients can coexist in the same process with different
        env-var namespaces::

            payment = HttpConnectionSettings.from_env(prefix="PAYMENT_API_")
            # reads PAYMENT_API_BASE_URL, PAYMENT_API_TIMEOUT, etc.

            notify = HttpConnectionSettings.from_env(prefix="NOTIF_API_")
            # reads NOTIF_API_BASE_URL, NOTIF_API_SSL__CA_CERT, etc.

        Args:
            prefix: Env-var prefix including the trailing underscore, e.g.
                    ``"PAYMENT_API_"``.  Must be non-empty.

        Returns:
            A fully populated ``HttpConnectionSettings`` instance.

        Raises:
            ValueError:       If ``prefix`` is empty.
            ValidationError:  If a required field is missing or invalid.

        Edge cases:
            - The returned object's type is technically an anonymous subclass
              (created at call time to carry the prefix in ``model_config``).
              It is fully interchangeable with ``HttpConnectionSettings`` for all
              practical purposes — ``isinstance(result, HttpConnectionSettings)``
              returns ``True``.
            - Nested fields use ``__`` as the delimiter:
              ``PAYMENT_API_SSL__CA_CERT`` → ``ssl.ca_cert``.
        """
        if not prefix:
            raise ValueError(
                "HttpConnectionSettings.from_env() requires a non-empty prefix. "
                "Example: HttpConnectionSettings.from_env(prefix='PAYMENT_API_'). "
                "A prefix is required because a service can talk to many HTTP APIs "
                "simultaneously — each needs its own env-var namespace."
            )

        # DESIGN: create a temporary subclass that carries the caller-supplied
        # prefix in its model_config.  We use type() so pydantic-settings'
        # metaclass processes the new model_config exactly as it would for a
        # hand-written class, reading {prefix}* env vars on construction.
        #
        # ✅ Zero extra state — the subclass is discarded after construction.
        # ✅ isinstance(result, HttpConnectionSettings) == True.
        # ❌ The dynamic class has an auto-generated __name__; this is only
        #    visible in repr() — not a practical problem.
        prefixed_cls: type[HttpConnectionSettings] = type(
            cls.__name__,
            (cls,),
            {
                "model_config": SettingsConfigDict(
                    env_prefix=prefix,
                    env_nested_delimiter="__",
                    frozen=True,
                ),
            },
        )
        return prefixed_cls()

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
