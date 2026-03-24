"""
varco_core.event.config
=======================
Base settings for event bus backends.

``EventBusSettings`` is the shared base class for all backend-specific event
bus configuration objects (``RedisEventBusSettings``, ``KafkaEventBusSettings``).
It adds fields that are meaningful for all bus implementations on top of the
env-var loading provided by ``VarcoSettings``.

DESIGN: separate from CacheSettings
    ``EventBusSettings`` and ``CacheSettings`` share the ``VarcoSettings`` base
    but are otherwise independent.  This means a package like ``varco_redis``
    can ship ``RedisEventBusSettings`` (for the bus) and ``RedisCacheSettings``
    (for the cache) as distinct objects — users who only want caching do not
    need to think about bus configuration at all.

Thread safety:  ✅ Immutable after construction (subclasses use frozen=True).
Async safety:   ✅ No mutable state.
"""

from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings


# ── EventBusSettings ──────────────────────────────────────────────────────────


class EventBusSettings(VarcoSettings):
    """
    Base configuration for all event bus backend implementations.

    Provides settings that are common to every bus:

    Attributes:
        channel_prefix: Optional prefix prepended to every channel or topic
                        name.  Use this for environment isolation —
                        e.g. ``"dev:"`` vs ``"prod:"`` so dev and prod buses
                        don't interfere on a shared broker.

    Subclasses add backend-specific fields::

        class RedisEventBusSettings(EventBusSettings):
            model_config = SettingsConfigDict(env_prefix="VARCO_REDIS_", frozen=True)
            url: str = "redis://localhost:6379/0"

    Thread safety:  ✅ Immutable after construction.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``channel_prefix`` is NOT applied retroactively — changing it after
          messages have been produced leaves orphaned channels on the backend.
        - Empty string ``""`` (default) means no prefix — channel names are
          used as-is.
    """

    # No env_prefix at this level — subclasses define their own.
    # frozen=True is intentionally set here so all event bus configs are
    # immutable by default.  Subclasses may override, but should not.
    model_config = SettingsConfigDict(frozen=True)

    channel_prefix: str = ""
    """
    Optional prefix for all channel/topic names.

    Applied by backend config helpers like ``channel_name()`` / ``topic_name()``.
    Empty string (default) means no prefix — channels are used verbatim.
    """


__all__ = [
    "EventBusSettings",
]
