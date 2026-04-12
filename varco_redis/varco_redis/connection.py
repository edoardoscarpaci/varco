"""
varco_redis.connection
======================
``RedisConnectionSettings`` — structured, env-var loadable configuration for a
Redis connection via redis.asyncio.

Environment variables (prefix ``REDIS_``)
-----------------------------------------
::

    REDIS_HOST=my-redis
    REDIS_PORT=6379
    REDIS_DB=0
    REDIS_PASSWORD=s3cret
    REDIS_USERNAME=default
    REDIS_SOCKET_TIMEOUT=5.0
    REDIS_DECODE_RESPONSES=false

    # TLS (optional)
    REDIS_SSL__CA_CERT=/etc/ssl/redis-ca.pem
    REDIS_SSL__CLIENT_CERT=/etc/ssl/client.crt
    REDIS_SSL__CLIENT_KEY=/etc/ssl/client.key
    REDIS_SSL__VERIFY=true

Construction patterns::

    # From env (production)
    conn = RedisConnectionSettings.from_env()

    # Explicit (tests / DI)
    conn = RedisConnectionSettings(host="localhost", db=1)

    # With SSL
    conn = RedisConnectionSettings.with_ssl(
        SSLConfig(ca_cert=Path("/etc/ssl/ca.pem")),
        host="prod-redis",
    )

    # Build redis.asyncio connection
    client = redis.asyncio.from_url(conn.to_url(), **conn.to_redis_kwargs())

DESIGN: Separate from RedisEventBusSettings
    ✅ RedisConnectionSettings is a general-purpose Redis connection config.
    ✅ RedisEventBusSettings remains unchanged — it owns channel_prefix,
       use_streams, and other event-bus-specific fields.
    ✅ Existing code that uses RedisEventBusSettings is NOT affected.
    ❌ Two Redis config classes exist — users choose the appropriate one.

Thread safety:  ✅ frozen=True — immutable after construction.
Async safety:   ✅ No I/O; all methods are synchronous.

📚 Docs
- 🔍 https://redis-py.readthedocs.io/en/stable/connections.html
  redis.asyncio — from_url(), Redis() kwargs, ssl parameter
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote_plus

from pydantic_settings import SettingsConfigDict

from varco_core.connection.base import ConnectionSettings


# ── RedisConnectionSettings ───────────────────────────────────────────────────


class RedisConnectionSettings(ConnectionSettings):
    """
    Immutable Redis connection configuration.

    Reads from environment variables with the ``REDIS_`` prefix.
    Nested SSL config uses the ``__`` delimiter::

        REDIS_HOST=my-redis
        REDIS_SSL__CA_CERT=/etc/ssl/ca.pem

    Attributes:
        host:             Redis server hostname.  Env: ``REDIS_HOST``.
        port:             Redis server port.  Env: ``REDIS_PORT``.
        db:               Redis database index (0–15).  Env: ``REDIS_DB``.
        password:         Optional Redis AUTH password.  Env: ``REDIS_PASSWORD``.
        username:         Optional Redis ACL username.  Env: ``REDIS_USERNAME``.
        decode_responses: Whether redis.asyncio decodes bytes to strings.
                          Keep ``False`` when working with binary data or the
                          event bus (which uses raw bytes serialization).
                          Env: ``REDIS_DECODE_RESPONSES``.
        socket_timeout:   Seconds before a socket operation times out.
                          ``None`` = no timeout.  Env: ``REDIS_SOCKET_TIMEOUT``.
        ssl:              Optional ``SSLConfig`` for TLS.  Populated from
                          ``REDIS_SSL__*`` env vars.

    Thread safety:  ✅ frozen=True — immutable.
    Async safety:   ✅ No I/O.

    Edge cases:
        - When ``ssl`` is configured, ``to_url()`` uses the ``rediss://`` scheme.
        - ``decode_responses=True`` may cause ``TypeError`` when binary data is
          expected — the default is ``False`` to match ``RedisEventBusSettings``.
        - ACL authentication (Redis 6+) requires both ``username`` and ``password``.
          AUTH-only (Redis <6) requires only ``password``.
    """

    model_config = SettingsConfigDict(
        env_prefix="REDIS_",
        env_nested_delimiter="__",
        frozen=True,
    )

    port: int = 6379
    """Redis server port.  Env: ``REDIS_PORT``."""

    db: int = 0
    """Redis database index (0–15).  Env: ``REDIS_DB``."""

    password: str | None = None
    """Redis AUTH password.  Env: ``REDIS_PASSWORD``."""

    username: str | None = None
    """Redis ACL username (Redis 6+).  Env: ``REDIS_USERNAME``."""

    decode_responses: bool = False
    """Return strings instead of bytes.  Keep False for binary/event-bus usage."""

    socket_timeout: float | None = None
    """Socket operation timeout in seconds.  None = no timeout."""

    # ── Conversion methods ────────────────────────────────────────────────────

    def to_url(self) -> str:
        """
        Build the Redis connection URL string.

        Scheme is ``rediss://`` (TLS) when ``ssl`` is configured, ``redis://``
        otherwise.  Credentials (username/password) are percent-encoded.

        Format: ``redis[s]://[user:password@]host:port/db``

        Returns:
            Redis connection URL string.

        Edge cases:
            - No ``username`` and no ``password`` → ``redis://host:port/db``
            - ``password`` only → ``redis://:password@host:port/db``
            - Both → ``redis://user:password@host:port/db``
        """
        scheme = "rediss" if self.ssl is not None else "redis"
        auth_part = ""
        if self.username or self.password:
            user = quote_plus(self.username or "")
            pw = quote_plus(self.password or "")
            auth_part = f"{user}:{pw}@"
        return f"{scheme}://{auth_part}{self.host}:{self.port}/{self.db}"

    def to_redis_kwargs(self) -> dict[str, Any]:
        """
        Build kwargs for ``redis.asyncio.from_url()``.

        Returns a dict that, when unpacked alongside ``from_url(conn.to_url())``,
        produces a fully configured Redis client::

            client = redis.asyncio.from_url(
                conn.to_url(),
                **conn.to_redis_kwargs(),
            )

        Keys:
        - ``decode_responses`` — always included.
        - ``socket_timeout`` — included when not ``None``.
        - ``ssl`` — ``ssl.SSLContext`` when ``ssl`` is configured.

        Returns:
            Dict of redis.asyncio kwargs (excludes the URL itself).

        Edge cases:
            - The ``url`` is NOT included — pass ``conn.to_url()`` as the first
              positional argument to ``from_url()`` separately.
            - ``ssl=ctx`` and ``rediss://`` in the URL are compatible; redis.asyncio
              accepts the ssl context on the kwarg (the URL scheme enables TLS mode).
        """
        kwargs: dict[str, Any] = {"decode_responses": self.decode_responses}

        if self.socket_timeout is not None:
            kwargs["socket_timeout"] = self.socket_timeout

        if self.ssl is not None:
            kwargs["ssl"] = self.ssl.build_ssl_context()

        return kwargs


__all__ = ["RedisConnectionSettings"]
