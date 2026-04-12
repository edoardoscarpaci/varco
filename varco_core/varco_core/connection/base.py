"""
varco_core.connection.base
==========================
``ConnectionSettings`` — the base class for all structured backend connection
configurations (Postgres, Redis, Kafka, HTTP).

Subclasses add:
- Their own ``env_prefix`` via ``model_config = SettingsConfigDict(env_prefix="POSTGRES_", ...)``
- Backend-specific fields (``database``, ``db``, ``bootstrap_servers``, etc.)
- Conversion methods (``to_dsn()``, ``to_url()``, ``to_aiokafka_kwargs()``, etc.)

Environment variable loading (via pydantic-settings)
-----------------------------------------------------
Nested models (``SSLConfig``, ``BasicAuthConfig``) are populated using the
``env_nested_delimiter="__"`` inherited from ``ConnectionSettings``::

    # For PostgresConnectionSettings (env_prefix="POSTGRES_"):
    POSTGRES_HOST=my-db          → host = "my-db"
    POSTGRES_PORT=5432           → port = 5432
    POSTGRES_SSL__CA_CERT=/path  → ssl.ca_cert = Path("/path")
    POSTGRES_SSL__VERIFY=true    → ssl.verify = True
    POSTGRES_AUTH__USERNAME=u    → auth.username = "u"
    POSTGRES_AUTH__PASSWORD=pw   → auth.password = "pw"
    POSTGRES_AUTH__TYPE=basic    → auth.type = "basic"

Construction patterns
---------------------
::

    # From env vars (production)
    conn = PostgresConnectionSettings.from_env()

    # Explicit (tests / DI override)
    conn = PostgresConnectionSettings(host="localhost", port=5432)

    # From dict
    conn = PostgresConnectionSettings.from_dict({"host": "prod-db", "port": 5432})

    # With a pre-built SSLConfig (does NOT read env vars — useful in DI wiring)
    conn = PostgresConnectionSettings.with_ssl(
        SSLConfig(ca_cert=Path("/etc/ssl/ca.pem")),
        host="prod-db",
        database="orders",
    )

DESIGN: ConnectionSettings inherits VarcoSettings (BaseSettings) directly
    ✅ Env-var loading is inherited — subclasses just set env_prefix.
    ✅ env_nested_delimiter="__" set once here — all subclasses get it for free.
    ✅ with_ssl() classmethod uses model_validate() to bypass env scanning.
    ❌ Pydantic-settings' nested model support requires the nested model
       (SSLConfig) to be a pydantic BaseModel — a frozen dataclass would not work.

Thread safety:  ✅ Subclasses should set frozen=True — immutable after construction.
Async safety:   ✅ No mutable state; all methods are synchronous.
"""

from __future__ import annotations

from typing import Any, Self

from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings
from varco_core.connection.ssl import SSLConfig


# ── ConnectionSettings ────────────────────────────────────────────────────────


class ConnectionSettings(VarcoSettings):
    """
    Base class for all backend connection configurations.

    Provides:
    - ``host`` / ``port`` — common to every connection type.
    - ``ssl`` — optional ``SSLConfig`` for TLS (populated via ``{PREFIX}SSL__*`` env vars).
    - ``with_ssl()`` — factory classmethod that bypasses env-var reading.
    - ``env_prefix()`` — inherited; returns the subclass prefix string.

    Subclasses must set their own ``model_config`` to add ``env_prefix``::

        class PostgresConnectionSettings(ConnectionSettings):
            model_config = SettingsConfigDict(
                env_prefix="POSTGRES_",
                env_nested_delimiter="__",
                frozen=True,
            )
            port: int = 5432
            database: str = "postgres"

    All ``env_nested_delimiter="__"`` is already set here and inherited.
    Subclasses that also set ``env_nested_delimiter`` explicitly will override it
    (same value — no conflict).

    Attributes:
        host: Hostname or IP address of the service.  Env: ``{PREFIX}HOST``.
        port: Port number.  Backends set their own default.  Env: ``{PREFIX}PORT``.
        ssl:  Optional ``SSLConfig``.  Env: ``{PREFIX}SSL__CA_CERT``, etc.

    Thread safety:  ✅ Frozen in subclasses — safe to share across threads.
    Async safety:   ✅ No I/O; all methods are synchronous.

    Edge cases:
        - ``port=0`` is the default here.  Backend subclasses should override it
          with a sensible default (e.g. 5432, 6379, 9092).
        - ``ssl=None`` means no TLS is configured; the driver connects in plaintext.
          This is intentionally ``None`` (not a default ``SSLConfig()``) so that
          callers can detect whether TLS was explicitly configured.
    """

    # Inherited by all subclasses — they add env_prefix on top.
    model_config = SettingsConfigDict(env_nested_delimiter="__")

    host: str = "localhost"
    """Service hostname.  Env: ``{PREFIX}HOST``."""

    port: int = 0
    """Service port.  Backend subclasses override the default.  Env: ``{PREFIX}PORT``."""

    ssl: SSLConfig | None = None
    """
    Optional TLS/SSL configuration.  ``None`` = no TLS (plaintext connection).
    Populated from ``{PREFIX}SSL__*`` env vars when a parent settings class
    uses ``env_nested_delimiter="__"``.
    """

    # ── Factory methods ───────────────────────────────────────────────────────

    @classmethod
    def with_ssl(cls, ssl_config: SSLConfig, **kwargs: Any) -> Self:
        """
        Construct a connection settings instance with a pre-built ``SSLConfig``.

        Unlike calling ``cls(ssl=ssl_config, **kwargs)`` directly (which would
        trigger pydantic-settings' env-var sources on top of the kwargs), this
        method uses ``cls.model_validate()`` to bypass env scanning entirely.
        The result is fully populated from the provided arguments only.

        Args:
            ssl_config: Pre-built ``SSLConfig`` instance.
            **kwargs:   Additional field values (``host``, ``port``, ``database``,
                        etc.) forwarded to the model.

        Returns:
            A new instance of the calling class with ``ssl`` set to ``ssl_config``.

        Raises:
            ValidationError: If required fields are missing or values are invalid.

        Edge cases:
            - ``kwargs`` overrides field defaults but does NOT trigger env-var
              reading — this is the primary reason to use this factory.
            - Nested ``auth`` config can be passed as a dict:
              ``with_ssl(ssl, auth={"type": "basic", "username": "u", "password": "p"})``.

        Example::

            ssl = SSLConfig(ca_cert=Path("/etc/ssl/ca.pem"))
            conn = PostgresConnectionSettings.with_ssl(
                ssl,
                host="prod-db",
                database="orders",
            )
            # Equivalent to model_validate({"ssl": {...}, "host": "prod-db", ...})
            # Does NOT read POSTGRES_* env vars.
        """
        data: dict[str, Any] = {"ssl": ssl_config.model_dump(), **kwargs}
        return cls.model_validate(data)


__all__ = ["ConnectionSettings"]
