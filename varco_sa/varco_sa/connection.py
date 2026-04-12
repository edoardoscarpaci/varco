"""
varco_sa.connection
===================
``PostgresConnectionSettings`` — structured, env-var loadable configuration for
a PostgreSQL connection via asyncpg / SQLAlchemy async.

Environment variables (prefix ``POSTGRES_``)
---------------------------------------------
::

    POSTGRES_HOST=my-db-host
    POSTGRES_PORT=5432
    POSTGRES_DATABASE=orders
    POSTGRES_SCHEMA_NAME=app
    POSTGRES_USERNAME=svc_user
    POSTGRES_PASSWORD=s3cret
    POSTGRES_POOL_SIZE=10
    POSTGRES_MAX_OVERFLOW=20
    POSTGRES_POOL_TIMEOUT=30.0

    # TLS (optional — nested via __)
    POSTGRES_SSL__CA_CERT=/etc/ssl/postgres-ca.pem
    POSTGRES_SSL__CLIENT_CERT=/etc/ssl/client.crt
    POSTGRES_SSL__CLIENT_KEY=/etc/ssl/client.key
    POSTGRES_SSL__VERIFY=true

    # Credentials object (alternative to inline username/password)
    POSTGRES_AUTH__TYPE=basic
    POSTGRES_AUTH__USERNAME=admin
    POSTGRES_AUTH__PASSWORD=s3cret

Construction patterns::

    # From env (production)
    conn = PostgresConnectionSettings.from_env()

    # Explicit (tests / DI)
    conn = PostgresConnectionSettings(host="localhost", database="testdb")

    # With a pre-built SSLConfig (does NOT read env vars)
    conn = PostgresConnectionSettings.with_ssl(
        SSLConfig(ca_cert=Path("/etc/ssl/ca.pem")),
        host="prod-db",
        database="orders",
    )

    # Build DSN / asyncpg kwargs
    dsn = conn.to_dsn()                         # "postgresql+asyncpg://..."
    kwargs = conn.to_asyncpg_kwargs()           # {"dsn": "...", "ssl": ctx}
    create_async_engine(conn.to_sqlalchemy_url(), **conn.to_engine_kwargs())

DESIGN: PostgresConnectionSettings over SAConfig for connection config
    ✅ Env-var loadable with ``POSTGRES_`` prefix + SSL/auth nesting.
    ✅ to_dsn() / to_asyncpg_kwargs() produce driver-ready output.
    ✅ Composable with ``SAConfig`` — pass to_sqlalchemy_url() to create_async_engine.
    ❌ Parallel to ``SAConfig`` — both exist; ``PostgresConnectionSettings`` is the
       *connection* concern, ``SAConfig`` is the *ORM bootstrap* concern.

Thread safety:  ✅ frozen=True — immutable after construction.
Async safety:   ✅ No I/O; all methods are synchronous.

📚 Docs
- 🔍 https://magicstack.github.io/asyncpg/current/api/index.html#connection
  asyncpg — connect() kwargs, ssl parameter
- 🔍 https://docs.sqlalchemy.org/en/20/dialects/postgresql.html#connect-args
  SQLAlchemy + asyncpg — create_async_engine connect_args
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus

from pydantic_settings import SettingsConfigDict

from varco_core.connection.auth import BasicAuthConfig
from varco_core.connection.base import ConnectionSettings


# ── PostgresConnectionSettings ────────────────────────────────────────────────


class PostgresConnectionSettings(ConnectionSettings):
    """
    Immutable PostgreSQL connection configuration.

    Reads from environment variables with the ``POSTGRES_`` prefix.
    Nested SSL and auth config use the ``__`` delimiter::

        POSTGRES_HOST=my-db
        POSTGRES_SSL__CA_CERT=/etc/ssl/ca.pem

    Attributes:
        host:         Database server hostname.  Env: ``POSTGRES_HOST``.
        port:         Database server port.  Env: ``POSTGRES_PORT``.
        database:     Database name.  Env: ``POSTGRES_DATABASE``.
        schema_name:  Default search path schema.  Named ``schema_name`` (not
                      ``schema``) to avoid clashing with pydantic's internal
                      ``.model_json_schema()`` method.  Env: ``POSTGRES_SCHEMA_NAME``.
        username:     Authentication username (inline).  Overridden by ``auth``
                      when both are set.  Env: ``POSTGRES_USERNAME``.
        password:     Authentication password (inline).  Env: ``POSTGRES_PASSWORD``.
        pool_size:    SQLAlchemy pool minimum connections.  Env: ``POSTGRES_POOL_SIZE``.
        max_overflow: SQLAlchemy pool max additional connections.
                      Env: ``POSTGRES_MAX_OVERFLOW``.
        pool_timeout: Seconds to wait for a pool connection.
                      Env: ``POSTGRES_POOL_TIMEOUT``.
        ssl:          Optional ``SSLConfig`` for TLS.  Populated from
                      ``POSTGRES_SSL__*`` env vars.
        auth:         Optional ``BasicAuthConfig`` — overrides inline username/password
                      when set.  Populated from ``POSTGRES_AUTH__*`` env vars.

    Thread safety:  ✅ frozen=True — immutable.
    Async safety:   ✅ No I/O.

    Edge cases:
        - ``auth`` takes precedence over inline ``username``/``password`` in all
          ``to_*()`` methods.
        - ``schema_name="public"`` → no ``server_settings`` added to asyncpg kwargs
          (public is the default; adding it is harmless but noisy).
        - Passwords with special URL characters are percent-encoded in ``to_dsn()``.
    """

    model_config = SettingsConfigDict(
        env_prefix="POSTGRES_",
        env_nested_delimiter="__",
        frozen=True,
    )

    port: int = 5432
    """Database server port.  Env: ``POSTGRES_PORT``."""

    database: str = "postgres"
    """Database name.  Env: ``POSTGRES_DATABASE``."""

    schema_name: str = "public"
    """Default schema / search path.  Env: ``POSTGRES_SCHEMA_NAME``."""

    username: str = "postgres"
    """Inline authentication username.  Env: ``POSTGRES_USERNAME``."""

    password: str = ""
    """Inline authentication password.  Env: ``POSTGRES_PASSWORD``."""

    pool_size: int = 5
    """SQLAlchemy pool minimum connections.  Env: ``POSTGRES_POOL_SIZE``."""

    max_overflow: int = 10
    """SQLAlchemy pool max additional connections.  Env: ``POSTGRES_MAX_OVERFLOW``."""

    pool_timeout: float = 30.0
    """Seconds to wait for a pool connection.  Env: ``POSTGRES_POOL_TIMEOUT``."""

    auth: BasicAuthConfig | None = None
    """
    Optional structured auth config.  Overrides inline ``username``/``password``.
    Populated from ``POSTGRES_AUTH__TYPE``, ``POSTGRES_AUTH__USERNAME``,
    ``POSTGRES_AUTH__PASSWORD`` env vars.
    """

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _effective_username(self) -> str:
        """Return the resolved username (auth object takes precedence)."""
        return self.auth.username if self.auth else self.username

    def _effective_password(self) -> str:
        """Return the resolved password (auth object takes precedence)."""
        return self.auth.password if self.auth else self.password

    # ── Conversion methods ────────────────────────────────────────────────────

    def to_dsn(self) -> str:
        """
        Build the PostgreSQL asyncpg DSN string.

        Format: ``postgresql+asyncpg://{user}:{password}@{host}:{port}/{database}``

        The username and password are percent-encoded to handle special characters
        (``@``, ``:``, ``/``, etc.).

        Returns:
            Asyncpg-compatible DSN string.

        Edge cases:
            - Empty password → ``user:@host:port/db`` (colon with no password).
            - ``auth`` object overrides inline ``username``/``password``.
        """
        user = quote_plus(self._effective_username())
        pw = quote_plus(self._effective_password())
        return (
            f"postgresql+asyncpg://{user}:{pw}@{self.host}:{self.port}/{self.database}"
        )

    def to_sqlalchemy_url(self) -> str:
        """
        Return the SQLAlchemy async engine URL.

        Alias for ``to_dsn()`` — both produce the same ``postgresql+asyncpg://``
        connection string.  Use whichever name reads more clearly at the call site::

            engine = create_async_engine(conn.to_sqlalchemy_url())

        Returns:
            SQLAlchemy-compatible database URL string.
        """
        return self.to_dsn()

    def to_asyncpg_kwargs(self) -> dict[str, Any]:
        """
        Build asyncpg-compatible connection kwargs.

        Returns a dict suitable for ``asyncpg.connect(...)`` or as
        ``connect_args`` in ``create_async_engine(..., connect_args=...)``.

        Keys:
        - ``dsn`` — full connection string from ``to_dsn()``.
        - ``ssl`` — ``ssl.SSLContext`` when ``ssl`` is configured; absent otherwise.
        - ``server_settings`` — ``{"search_path": schema_name}`` when schema is
          not the default ``"public"``.

        Returns:
            Dict of asyncpg kwargs.

        Edge cases:
            - The ``dsn`` key already encodes credentials — do NOT also pass
              ``user`` and ``password`` separately to asyncpg; that would conflict.
            - asyncpg accepts an ``ssl.SSLContext`` directly on the ``ssl`` kwarg.
        """
        kwargs: dict[str, Any] = {"dsn": self.to_dsn()}

        if self.ssl is not None:
            kwargs["ssl"] = self.ssl.build_ssl_context()

        if self.schema_name != "public":
            kwargs["server_settings"] = {"search_path": self.schema_name}

        return kwargs

    def to_engine_kwargs(self) -> dict[str, Any]:
        """
        Build kwargs for ``sqlalchemy.ext.asyncio.create_async_engine``.

        Separates pool settings (top-level kwargs) from asyncpg connect args
        (nested under ``connect_args``)::

            engine = create_async_engine(
                conn.to_sqlalchemy_url(),
                **conn.to_engine_kwargs(),
            )

        Returns:
            Dict containing ``pool_size``, ``max_overflow``, ``pool_timeout``,
            and ``connect_args`` (asyncpg SSL/schema kwargs).

        Edge cases:
            - ``connect_args`` excludes ``dsn`` — SQLAlchemy receives the URL
              separately as the first positional argument to ``create_async_engine``.
        """
        connect_args = self.to_asyncpg_kwargs()
        connect_args.pop("dsn", None)  # URL is the first arg to create_async_engine

        return {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "pool_timeout": self.pool_timeout,
            "connect_args": connect_args,
        }


__all__ = ["PostgresConnectionSettings"]
