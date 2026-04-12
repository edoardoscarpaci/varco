"""
varco_redis.config
==================
Configuration for the Redis event bus and cache backends.

``RedisEventBusSettings`` is the configuration object for ``RedisEventBus``.
It extends ``EventBusSettings`` so all Redis connection settings are read from
environment variables automatically.

Environment variables (prefix ``VARCO_REDIS_``)
-------------------------------------------------
::

    VARCO_REDIS_URL=redis://my-redis:6379/0
    VARCO_REDIS_CHANNEL_PREFIX=prod:
    VARCO_REDIS_SOCKET_TIMEOUT=5.0

Construction patterns::

    # From env (production)
    config = RedisEventBusSettings.from_env()

    # Explicit (tests, DI override)
    config = RedisEventBusSettings(url="redis://localhost:6379/0")

    # From dict (DI wiring)
    config = RedisEventBusSettings.from_dict({"url": os.environ["REDIS_URL"]})

DESIGN: Pydantic BaseSettings over frozen dataclass
    ✅ Env var reading is automatic — no ``os.environ`` boilerplate.
    ✅ Validates types at load time — ``VARCO_REDIS_SOCKET_TIMEOUT=abc`` fails fast.
    ✅ Immutable after construction — prevents accidental mutation.
    ❌ ``redis_kwargs`` cannot be set from a plain env var (would need JSON string).
       Use keyword arguments or ``from_dict()`` for complex extra kwargs.

Thread safety:  ✅ Immutable after construction (frozen=True).
Async safety:   ✅ No mutable state.

📚 Docs
- 🔍 https://redis-py.readthedocs.io/en/stable/connections.html
  redis.asyncio connection options — url format, SSL, auth
- 🔍 https://docs.pydantic.dev/latest/concepts/pydantic_settings/
  Pydantic Settings — env_prefix, SettingsConfigDict
"""

from __future__ import annotations

import sys
from typing import Any

from pydantic import Field
from pydantic_settings import SettingsConfigDict
from providify import Singleton

from varco_core.event.config import EventBusSettings


# ── RedisEventBusSettings ─────────────────────────────────────────────────────


@Singleton(priority=-sys.maxsize)
class RedisEventBusSettings(EventBusSettings):
    """
    Immutable configuration for ``RedisEventBus``.

    All fields are read from environment variables with the ``VARCO_REDIS_``
    prefix.  Every field has a sensible default so a local Redis instance
    can be used with no configuration::

        bus = RedisEventBus(RedisEventBusSettings())  # redis://localhost:6379/0

    Attributes:
        url:             Redis connection URL.
                         Env var: ``VARCO_REDIS_URL``.
        channel_prefix:  Optional prefix for every Pub/Sub channel name.
                         Env var: ``VARCO_REDIS_CHANNEL_PREFIX``.
                         (Inherited from ``EventBusSettings``.)
        decode_responses: Whether redis.asyncio returns strings instead of bytes.
                          Must be ``False`` (default) — the bus uses
                          ``JsonEventSerializer`` which expects raw bytes.
                          Env var: ``VARCO_REDIS_DECODE_RESPONSES``.
        socket_timeout:  Seconds before a socket operation times out.
                         ``None`` (default) means no timeout.
                         Env var: ``VARCO_REDIS_SOCKET_TIMEOUT``.
        redis_kwargs:    Extra keyword arguments forwarded verbatim to
                         ``redis.asyncio.from_url()``.  Use for SSL, auth,
                         connection pool settings.
                         **Cannot be set from a plain env var** — use
                         keyword args or ``from_dict()`` instead.

    Thread safety:  ✅ Immutable — frozen=True.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``decode_responses`` must remain ``False`` — setting it to ``True``
          causes ``TypeError`` at deserialization time (str vs bytes).
        - ``channel_prefix`` is NOT applied retroactively — orphaned channels
          remain in Redis if the prefix is changed after first use.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_REDIS_", frozen=True)

    url: str = "redis://localhost:6379/0"
    """Redis connection URL.  Env var: ``VARCO_REDIS_URL``."""

    use_streams: bool = False
    """
    When ``True``, the container resolves ``RedisStreamEventBus`` (at-least-once,
    Redis Streams) instead of ``RedisEventBus`` (at-most-once, Pub/Sub).
    Env var: ``VARCO_REDIS_USE_STREAMS=true``.
    """

    decode_responses: bool = False
    """Must stay False — bus uses raw bytes for serialization."""

    socket_timeout: float | None = None
    """Socket operation timeout in seconds.  None = no timeout."""

    # Extra kwargs forwarded verbatim to redis.asyncio.from_url().
    # Cannot be set from an env var — set via keyword args or from_dict().
    redis_kwargs: dict[str, Any] = Field(default_factory=dict)
    """Extra kwargs for ``redis.asyncio.from_url()``.  Not env-readable."""

    def channel_name(self, channel: str) -> str:
        """
        Return the full Redis Pub/Sub channel name for a logical event channel.

        Prepends ``channel_prefix`` if set.

        Args:
            channel: The logical event channel (e.g. ``"orders"``).

        Returns:
            The full Redis channel name (e.g. ``"prod:orders"``).

        Edge cases:
            - Empty ``channel_prefix`` → returns ``channel`` unchanged.
            - Redis channel names can be arbitrary strings — no validation here.
        """
        return f"{self.channel_prefix}{channel}"


__all__ = [
    "RedisEventBusSettings",
]
