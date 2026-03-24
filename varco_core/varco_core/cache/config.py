"""
varco_core.cache.config
========================
Base configuration for all varco cache backends.

``CacheSettings`` is the base class for backend-specific settings.
It inherits from ``VarcoSettings`` (``pydantic-settings`` ``BaseSettings``)
so values can be overridden via environment variables.

Backend packages (``varco_redis``, ``varco_memcached``) extend this with
their own ``env_prefix`` and transport-specific fields.

Usage (standalone default settings)::

    settings = CacheSettings()         # all defaults
    settings = CacheSettings.from_env()  # same — reads VARCO_CACHE_* env vars

Usage (in a backend)::

    class RedisCacheSettings(CacheSettings):
        model_config = SettingsConfigDict(env_prefix="VARCO_REDIS_CACHE_", frozen=True)
        url: str = "redis://localhost:6379/0"

DESIGN: CacheSettings shares the same VarcoSettings base as EventBusSettings
    ✅ from_env() / from_dict() on every config class — uniform API.
    ✅ pydantic-settings handles env var coercion and validation.
    ✅ frozen=True prevents accidental mutation after construction.
    ❌ Backends must subclass and set their own env_prefix — minor boilerplate.
       Acceptable — explicit is better than implicit namespacing.
"""

from __future__ import annotations

from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings


class CacheSettings(VarcoSettings):
    """
    Base configuration for cache backends.

    Inherits ``from_env()`` / ``from_dict()`` from ``VarcoSettings``.
    Backend subclasses should override ``model_config`` to set a proper
    ``env_prefix`` (e.g. ``VARCO_REDIS_CACHE_``).

    Attributes:
        default_ttl: Default time-to-live in seconds for cache entries.
                     ``None`` means entries never expire (unless the backend
                     imposes its own TTL or an invalidation strategy fires).
                     Env var: ``VARCO_CACHE_DEFAULT_TTL`` (on this base class).

    Thread safety:  ✅ Immutable — frozen=True.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``default_ttl=None`` with ``TTLStrategy`` → the strategy is a
          no-op for entries that have no explicit TTL.
        - Backend subclasses that override ``model_config`` must keep
          ``frozen=True`` to preserve immutability.
    """

    model_config = SettingsConfigDict(
        # Base prefix — backends should override with a more specific prefix.
        env_prefix="VARCO_CACHE_",
        frozen=True,
    )

    default_ttl: float | None = None
    """Default TTL in seconds.  ``None`` = no expiry.  Env: ``VARCO_CACHE_DEFAULT_TTL``."""


__all__ = ["CacheSettings"]
