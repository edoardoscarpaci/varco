"""
varco_redis.config
==================
Configuration value objects for ``RedisEventBus``.

``RedisConfig`` carries the minimal Redis connection settings.  All fields
have sensible defaults so a local Redis instance can be used with no
configuration::

    bus = RedisEventBus(RedisConfig())  # connects to redis://localhost:6379

DESIGN: dataclass over raw kwargs
    ✅ Type-checked at construction.
    ✅ Single import for all bus configuration.
    ✅ Defaults cover the common local-dev case.

Thread safety:  ✅ Frozen dataclass — immutable after construction.
Async safety:   ✅ No mutable state.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class RedisConfig:
    """
    Immutable configuration for ``RedisEventBus``.

    Attributes:
        url:             Redis connection URL.  Defaults to
                         ``"redis://localhost:6379/0"``.
        channel_prefix:  Optional prefix prepended to every Pub/Sub channel
                         name.  Useful for namespace isolation across
                         environments (e.g. ``"dev:"`` vs ``"prod:"``).
        decode_responses: Whether redis.asyncio should return strings instead
                          of bytes.  ``False`` (default) means raw bytes —
                          required because ``JsonEventSerializer`` expects bytes.
        socket_timeout:  Seconds to wait for a socket operation before timing
                         out.  ``None`` (default) means no timeout.
        redis_kwargs:    Extra keyword arguments forwarded to
                         ``redis.asyncio.from_url()``.  Use for SSL, auth,
                         connection pool settings, etc.

    Edge cases:
        - ``decode_responses`` must be ``False`` — the bus uses
          ``JsonEventSerializer`` which expects raw bytes, not strings.
          Setting it to ``True`` will cause ``TypeError`` at deserialization.
        - ``channel_prefix`` is NOT applied retroactively — changing it
          leaves orphaned channels in Redis.
    """

    url: str = "redis://localhost:6379/0"
    channel_prefix: str = ""
    decode_responses: bool = False
    socket_timeout: float | None = None
    # Extra kwargs forwarded verbatim to redis.asyncio.from_url()
    redis_kwargs: dict = field(default_factory=dict)

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
