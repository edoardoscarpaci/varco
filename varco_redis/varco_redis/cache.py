"""
varco_redis.cache
==================
Redis-backed ``CacheBackend`` implementation.

``RedisCache`` stores values as serialized bytes in Redis, with TTL support
via Redis native EXPIRE / SETEX commands.  Metadata (``stored_at``, ``tags``)
is not persisted in Redis — it lives only in ``InMemoryCache`` (L1 of a
``LayeredCache``) or is reconstructed at read time via the serializer.

Because Redis enforces its own TTL at the storage level, ``TTLStrategy`` is
not required for Redis-level expiry — simply pass ``ttl=`` to ``set()`` and
Redis handles eviction.  However, an ``InvalidationStrategy`` can still be
used for explicit or event-driven invalidation on top of the TTL.

Architecture
------------
::

    RedisCacheSettings   →  channel_prefix, url, serializer config
    RedisCache           →  CacheBackend backed by redis.asyncio
    RedisCacheConfiguration  →  Providify @Configuration

DESIGN: bytes-only storage over JSON strings
    ✅ Backend-agnostic — works with any ``Serializer[Any]`` implementation.
    ✅ ``JsonSerializer`` (default) handles arbitrary Pydantic models and
       dicts without requiring explicit type hints on ``get()``.
    ✅ ``NoOpSerializer`` works for callers that store pre-serialized bytes.
    ❌ Without type hints on ``get()``, deserialized values are plain dicts,
       not typed models.  Pass ``type_hint`` to ``get()`` for type safety.

Thread safety:  ❌  Not thread-safe.  Use from a single event loop.
Async safety:   ✅  All public methods are ``async def``.

📚 Docs
- 🔍 https://redis.io/commands/setex/   — Redis SETEX (set with TTL)
- 🔍 https://redis.io/commands/set/     — Redis SET (set without TTL)
- 🔍 https://redis-py.readthedocs.io/   — redis-py async docs
"""

from __future__ import annotations

import logging
from typing import Any

import redis.asyncio as aioredis
from pydantic import Field
from pydantic_settings import SettingsConfigDict

from providify import Configuration, Inject, PreDestroy, Provider

from varco_core.cache.base import CacheBackend, InvalidationStrategy
from varco_core.cache.config import CacheSettings
from varco_core.cache.layered import LayeredCache
from varco_core.cache.memory import InMemoryCache
from varco_core.serialization import JsonSerializer, Serializer

_logger = logging.getLogger(__name__)


# ── RedisCacheSettings ────────────────────────────────────────────────────────


class RedisCacheSettings(CacheSettings):
    """
    Configuration for ``RedisCache``.

    Extends ``CacheSettings`` with Redis connection settings.  Uses the
    ``VARCO_REDIS_CACHE_`` env prefix to keep cache settings separate from
    the event bus (``VARCO_REDIS_``).

    Attributes:
        url:              Redis connection URL.
                          Env: ``VARCO_REDIS_CACHE_URL``.
        key_prefix:       Prefix applied to all Redis keys — useful for
                          namespace isolation in a shared Redis instance.
                          Env: ``VARCO_REDIS_CACHE_KEY_PREFIX``.
        decode_responses: Must be ``False`` — cache values are raw bytes.
                          Env: ``VARCO_REDIS_CACHE_DECODE_RESPONSES``.
        socket_timeout:   Optional socket timeout in seconds.
                          Env: ``VARCO_REDIS_CACHE_SOCKET_TIMEOUT``.
        redis_kwargs:     Extra kwargs forwarded to ``aioredis.from_url()``.
                          Not env-readable — set via keyword args.

    Thread safety:  ✅ Immutable — frozen=True.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``key_prefix`` must end with a separator character (e.g. ``":"`` or
          ``"::"``).  A prefix without a separator blurs key boundaries.
        - ``decode_responses=True`` will cause ``get()`` to return strings
          instead of bytes — the serializer will fail to deserialize.
    """

    model_config = SettingsConfigDict(
        # Separate from the event bus VARCO_REDIS_ prefix.
        env_prefix="VARCO_REDIS_CACHE_",
        frozen=True,
    )

    url: str = "redis://localhost:6379/0"
    """Redis connection URL.  Env: ``VARCO_REDIS_CACHE_URL``."""

    key_prefix: str = ""
    """Redis key prefix for namespace isolation.  Env: ``VARCO_REDIS_CACHE_KEY_PREFIX``."""

    # Must stay False — cache values are bytes; decode_responses=True would
    # return strings and break the serializer.
    decode_responses: bool = False
    """Must be False (bytes mode).  Env: ``VARCO_REDIS_CACHE_DECODE_RESPONSES``."""

    socket_timeout: float | None = None
    """Socket timeout in seconds.  Env: ``VARCO_REDIS_CACHE_SOCKET_TIMEOUT``."""

    redis_kwargs: dict[str, Any] = Field(default_factory=dict)
    """Extra kwargs forwarded to ``aioredis.from_url()``.  Not env-readable."""

    def redis_key(self, key: Any) -> str:
        """
        Return the full Redis key for a logical cache key.

        Args:
            key: Logical cache key (any stringable value).

        Returns:
            ``f"{self.key_prefix}{key}"``.
        """
        return f"{self.key_prefix}{key}"


# ── RedisCache ────────────────────────────────────────────────────────────────


class RedisCache(CacheBackend):
    """
    Redis-backed ``CacheBackend``.

    Stores values as serialized bytes in Redis.  TTL is enforced natively by
    Redis via SETEX / EXPIRE — no in-memory TTL tracking is needed.

    An optional ``InvalidationStrategy`` can be layered on top for explicit
    or event-driven invalidation (e.g. ``ExplicitStrategy``,
    ``EventDrivenStrategy``).

    Lifecycle::

        async with RedisCache(RedisCacheSettings()) as cache:
            await cache.set("user:42", user_obj, ttl=300)
            result = await cache.get("user:42")

    Args:
        settings:   Redis connection and namespace settings.
        strategy:   Optional invalidation strategy for explicit/event-driven
                    eviction on top of TTL.  ``None`` = TTL-only (Redis-native).
        serializer: Pluggable serializer.  Defaults to ``JsonSerializer()``.

    Thread safety:  ❌  Not thread-safe.  Use from a single event loop.
    Async safety:   ✅  All public methods are ``async def``.

    Edge cases:
        - ``get()`` may return ``None`` for two reasons: cache miss OR
          serialization failure (logged as a warning).
        - ``delete()`` on a non-existent key is a silent no-op (Redis DEL
          returns 0 for missing keys — we don't raise).
        - ``clear()`` uses ``SCAN`` + ``DEL`` to remove only keys matching
          the configured ``key_prefix``.  Without a prefix it removes ALL keys
          in the Redis database — use with caution.
    """

    def __init__(
        self,
        settings: RedisCacheSettings | None = None,
        *,
        strategy: InvalidationStrategy | None = None,
        serializer: Serializer[Any] | None = None,
    ) -> None:
        """
        Args:
            settings:   Cache settings.  Defaults to ``RedisCacheSettings()``
                        (localhost, reads from env).
            strategy:   Optional invalidation strategy.  ``None`` = Redis TTL only.
            serializer: Value serializer.  Defaults to ``JsonSerializer()``.
        """
        self._settings = settings or RedisCacheSettings()
        self._strategy = strategy
        # Default to JSON serialization — handles dicts, Pydantic models, primitives.
        self._serializer: Serializer[Any] = serializer or JsonSerializer()
        self._redis: Any | None = None

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Connect to Redis and start the invalidation strategy.

        Raises:
            RuntimeError:    If already started.
            ConnectionError: If Redis is unreachable.
        """
        if self._redis is not None:
            raise RuntimeError(
                "RedisCache.start() called on an already-started cache. "
                "Call stop() first."
            )
        self._redis = aioredis.from_url(
            self._settings.url,
            decode_responses=self._settings.decode_responses,
            socket_timeout=self._settings.socket_timeout,
            **self._settings.redis_kwargs,
        )
        if self._strategy is not None:
            await self._strategy.start()
        _logger.info("RedisCache started (url=%s).", self._settings.url)

    @PreDestroy
    async def stop(self) -> None:
        """Close the Redis connection and stop the strategy.  Idempotent."""
        if self._redis is None:
            return
        if self._strategy is not None:
            await self._strategy.stop()
        await self._redis.aclose()
        self._redis = None
        _logger.info("RedisCache stopped.")

    # ── Cache operations ───────────────────────────────────────────────────────

    async def get(self, key: Any, *, type_hint: type | None = None) -> Any | None:
        """
        Return the cached value for ``key``, or ``None`` on miss.

        Args:
            key:       Logical cache key.
            type_hint: Optional type hint passed to the serializer's
                       ``deserialize()`` for typed deserialization.

        Returns:
            Deserialized value, or ``None`` if absent / strategy-invalidated.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        redis_key = self._settings.redis_key(key)

        # Check invalidation strategy before querying Redis.
        # Metadata for Redis entries is minimal — TTL is Redis-native.
        if self._strategy is not None and self._strategy.should_invalidate(key, {}):
            # Proactively delete from Redis so the TTL doesn't fool callers.
            await self._redis.delete(redis_key)  # type: ignore[union-attr]
            _logger.debug("RedisCache: strategy-evicted key %r.", key)
            return None

        raw = await self._redis.get(redis_key)  # type: ignore[union-attr]
        if raw is None:
            return None

        try:
            return self._serializer.deserialize(raw, type_hint)
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "RedisCache: failed to deserialize key %r: %s", key, exc, exc_info=True
            )
            return None

    async def set(self, key: Any, value: Any, *, ttl: float | None = None) -> None:
        """
        Serialize and store ``value`` in Redis under ``key``.

        Uses Redis SETEX when ``ttl`` is provided, otherwise SET (no expiry).

        Args:
            key:   Logical cache key.
            value: Value to serialize and store.
            ttl:   TTL in seconds.  ``None`` falls back to
                   ``settings.default_ttl``.  ``None`` with no default = no expiry.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        redis_key = self._settings.redis_key(key)
        effective_ttl = ttl if ttl is not None else self._settings.default_ttl
        data = self._serializer.serialize(value)

        if effective_ttl is not None:
            # SETEX — Redis natively expires the key after effective_ttl seconds.
            await self._redis.setex(  # type: ignore[union-attr]
                redis_key,
                int(effective_ttl),
                data,
            )
        else:
            # SET — no TTL; key lives until deleted or Redis is flushed.
            await self._redis.set(redis_key, data)  # type: ignore[union-attr]

        _logger.debug("RedisCache: stored key %r (ttl=%s).", key, effective_ttl)

    async def delete(self, key: Any) -> None:
        """
        Remove ``key`` from Redis.  Idempotent.

        Args:
            key: Logical cache key to remove.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        await self._redis.delete(self._settings.redis_key(key))  # type: ignore[union-attr]

    async def exists(self, key: Any) -> bool:
        """
        Return ``True`` if ``key`` exists in Redis (and strategy allows it).

        Args:
            key: Logical cache key to check.

        Returns:
            ``True`` if the key exists in Redis and is not strategy-invalidated.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        if self._strategy is not None and self._strategy.should_invalidate(key, {}):
            return False
        result = await self._redis.exists(self._settings.redis_key(key))  # type: ignore[union-attr]
        return bool(result)

    async def clear(self) -> None:
        """
        Delete all keys matching ``settings.key_prefix + "*"`` from Redis.

        ⚠️ Without a ``key_prefix``, this flushes the ENTIRE Redis database.
        Always configure a meaningful ``key_prefix`` in production.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        pattern = f"{self._settings.key_prefix}*"
        # SCAN is non-blocking — preferred over KEYS for production safety.
        cursor = 0
        deleted = 0
        while True:
            cursor, keys = await self._redis.scan(cursor, match=pattern, count=100)  # type: ignore[union-attr]
            if keys:
                await self._redis.delete(*keys)  # type: ignore[union-attr]
                deleted += len(keys)
            if cursor == 0:
                break
        _logger.debug("RedisCache: cleared %d key(s) matching %r.", deleted, pattern)

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _require_started(self) -> None:
        """
        Raise ``RuntimeError`` if the cache has not been started.

        Raises:
            RuntimeError: If ``start()`` has not been called.
        """
        if self._redis is None:
            raise RuntimeError(
                f"{type(self).__name__} is not started. "
                f"Call 'await cache.start()' or use 'async with cache' first."
            )

    def __repr__(self) -> str:
        return (
            f"RedisCache("
            f"url={self._settings.url!r}, "
            f"key_prefix={self._settings.key_prefix!r}, "
            f"strategy={self._strategy!r}, "
            f"started={self._redis is not None})"
        )


# ── RedisCacheConfiguration ───────────────────────────────────────────────────


@Configuration
class RedisCacheConfiguration:
    """
    Providify ``@Configuration`` that wires ``RedisCache`` into the container.

    Provides:
        ``RedisCacheSettings`` — default localhost settings; override before
                                  installing this configuration.
        ``CacheBackend``        — started ``RedisCache`` singleton.

    Lifecycle:
        The cache is started inside the provider and stopped automatically by
        ``@PreDestroy`` on ``RedisCache.stop()`` when
        ``await container.ashutdown()`` is called.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def`` — safe to ``await``.

    Example::

        container = DIContainer()
        await container.ainstall(RedisCacheConfiguration)
        cache = await container.aget(CacheBackend)
        await cache.set("key", value, ttl=300)
        result = await cache.get("key")
        await container.ashutdown()

    Overriding settings::

        container.provide(
            lambda: RedisCacheSettings(url=os.environ["REDIS_URL"], key_prefix="myapp:"),
            RedisCacheSettings,
        )
        await container.ainstall(RedisCacheConfiguration)
    """

    @Provider(singleton=True)
    def redis_cache_settings(self) -> RedisCacheSettings:
        """
        Default ``RedisCacheSettings`` pointing at ``redis://localhost:6379/0``.

        Returns:
            A ``RedisCacheSettings`` with development-friendly defaults.
        """
        # Reads from VARCO_REDIS_CACHE_* env vars if set.
        return RedisCacheSettings.from_env()

    @Provider(singleton=True)
    async def redis_cache(
        self,
        settings: Inject[RedisCacheSettings],
    ) -> CacheBackend:
        """
        Create, start, and return the ``RedisCache`` singleton.

        Args:
            settings: ``RedisCacheSettings`` — injected from the container.

        Returns:
            A started ``RedisCache`` bound to ``CacheBackend``.

        Raises:
            ConnectionError: If Redis is unreachable at startup.
        """
        _logger.info(
            "RedisCacheConfiguration: starting RedisCache (url=%s, prefix=%r).",
            settings.url,
            settings.key_prefix,
        )
        cache = RedisCache(settings)
        await cache.start()
        return cache


# ── LayeredCacheSettings ──────────────────────────────────────────────────────


class LayeredCacheSettings(CacheSettings):
    """
    Configuration for the ``RedisLayeredCacheConfiguration`` (L1 InMemory + L2 Redis).

    Uses a dedicated ``VARCO_LAYERED_CACHE_`` prefix so this configuration can
    coexist with ``RedisCacheConfiguration`` (``VARCO_REDIS_CACHE_``) in the
    same container without env-var collision.

    DESIGN: flat settings over nested RedisCacheSettings
        ✅ Pydantic Settings does not support cleanly nested BaseSettings with
           different prefixes — a flat layout avoids model_validator hacks.
        ✅ All fields are directly env-readable with a single prefix.
        ❌ Duplicates a few field names from RedisCacheSettings.  Acceptable
           because the env prefixes are intentionally different.

    Attributes:
        url:            L2 Redis connection URL.
                        Env: ``VARCO_LAYERED_CACHE_URL``.
        key_prefix:     L2 Redis key prefix for namespace isolation.
                        Env: ``VARCO_LAYERED_CACHE_KEY_PREFIX``.
        decode_responses: Must be ``False`` (bytes mode) for L2.
                          Env: ``VARCO_LAYERED_CACHE_DECODE_RESPONSES``.
        socket_timeout: L2 Redis socket timeout in seconds.
                        Env: ``VARCO_LAYERED_CACHE_SOCKET_TIMEOUT``.
        l1_max_size:    Maximum number of entries held in L1 (InMemoryCache).
                        ``None`` = unbounded.
                        Env: ``VARCO_LAYERED_CACHE_L1_MAX_SIZE``.
        l1_default_ttl: Default TTL for L1 entries in seconds.
                        ``None`` = use the promoted L2 TTL if available.
                        Env: ``VARCO_LAYERED_CACHE_L1_DEFAULT_TTL``.
        write_mode:     ``"write-through"`` (default) — all writes go to both
                        L1 and L2.  ``"write-around"`` — writes go to L2 only;
                        L1 is populated only on the next read (lazy promotion).
                        Env: ``VARCO_LAYERED_CACHE_WRITE_MODE``.
        promote_ttl:    TTL applied to values promoted from L2 → L1.
                        ``None`` = use the L1 default TTL.
                        Env: ``VARCO_LAYERED_CACHE_PROMOTE_TTL``.

    Thread safety:  ✅ Immutable — frozen=True.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``decode_responses`` must remain ``False`` — the cache uses bytes-
          mode serialization; setting it to True causes deserialization errors.
        - Setting ``write_mode="write-around"`` means L1 will be cold until the
          first read after a write — suitable for write-heavy, read-light loads.
        - ``l1_max_size=None`` is unbounded — in long-running services with many
          unique keys this can grow large.  Set a reasonable cap in production.
    """

    model_config = SettingsConfigDict(
        # Distinct from VARCO_REDIS_CACHE_ — avoids conflict if both
        # RedisCacheConfiguration and RedisLayeredCacheConfiguration are installed.
        env_prefix="VARCO_LAYERED_CACHE_",
        frozen=True,
    )

    # ── L2 (Redis) settings ───────────────────────────────────────────────────

    url: str = "redis://localhost:6379/0"
    """L2 Redis URL.  Env: ``VARCO_LAYERED_CACHE_URL``."""

    key_prefix: str = ""
    """L2 key prefix.  Env: ``VARCO_LAYERED_CACHE_KEY_PREFIX``."""

    decode_responses: bool = False
    """Must be False.  Env: ``VARCO_LAYERED_CACHE_DECODE_RESPONSES``."""

    socket_timeout: float | None = None
    """L2 socket timeout in seconds.  Env: ``VARCO_LAYERED_CACHE_SOCKET_TIMEOUT``."""

    # ── L1 (InMemoryCache) settings ───────────────────────────────────────────

    l1_max_size: int | None = None
    """L1 maximum entry count.  None = unbounded.  Env: ``VARCO_LAYERED_CACHE_L1_MAX_SIZE``."""

    l1_default_ttl: float | None = None
    """L1 default TTL in seconds.  Env: ``VARCO_LAYERED_CACHE_L1_DEFAULT_TTL``."""

    # ── LayeredCache settings ─────────────────────────────────────────────────

    write_mode: str = "write-through"
    """Write propagation mode.  Env: ``VARCO_LAYERED_CACHE_WRITE_MODE``."""

    promote_ttl: float | None = None
    """TTL applied when promoting from L2 → L1.  Env: ``VARCO_LAYERED_CACHE_PROMOTE_TTL``."""


# ── RedisLayeredCacheConfiguration ────────────────────────────────────────────


@Configuration
class RedisLayeredCacheConfiguration:
    """
    Providify ``@Configuration`` that wires a two-layer ``LayeredCache``
    (L1 ``InMemoryCache`` + L2 ``RedisCache``) into the container.

    Provides:
        ``LayeredCacheSettings`` — all L1/L2 settings; override before
                                    installing this configuration.
        ``CacheBackend``          — started ``LayeredCache`` singleton.

    Use this instead of ``RedisCacheConfiguration`` when you want a fast
    in-process L1 cache in front of Redis.  Common for read-heavy workloads
    where the same keys are accessed repeatedly in a short window.

    Lifecycle:
        The ``LayeredCache.start()`` starts both the L1 and L2 backends.
        ``@PreDestroy`` on each backend's ``stop()`` is called automatically
        when ``await container.ashutdown()`` is called.

    DESIGN: builds both L1 and L2 internally over exposing them as separate
    bindings
        ✅ The composite is the useful artifact — callers need CacheBackend,
           not the individual layers.
        ✅ Keeps the DI graph simple — one install, one resolved interface.
        ❌ Callers cannot independently inject the L1 or L2 backend.
           If that's needed, install RedisCacheConfiguration separately and
           compose manually.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def`` — safe to ``await``.

    Example::

        container = DIContainer()
        await container.ainstall(RedisLayeredCacheConfiguration)
        cache = await container.aget(CacheBackend)
        await cache.set("user:42", user_data, ttl=300)
        result = await cache.get("user:42")   # served from L1 on second call
        await container.ashutdown()

    Overriding settings::

        container.provide(
            lambda: LayeredCacheSettings(
                url=os.environ["REDIS_URL"],
                key_prefix="myapp:",
                l1_max_size=1000,
                l1_default_ttl=60.0,
            ),
            LayeredCacheSettings,
        )
        await container.ainstall(RedisLayeredCacheConfiguration)
    """

    @Provider(singleton=True)
    def layered_cache_settings(self) -> LayeredCacheSettings:
        """
        Default ``LayeredCacheSettings`` with localhost Redis and unbounded L1.

        Returns:
            A ``LayeredCacheSettings`` with development-friendly defaults.
        """
        # Reads from VARCO_LAYERED_CACHE_* env vars if set.
        return LayeredCacheSettings.from_env()

    @Provider(singleton=True)
    async def layered_cache(
        self,
        settings: Inject[LayeredCacheSettings],
    ) -> CacheBackend:
        """
        Build, start, and return the ``LayeredCache`` singleton.

        Constructs an ``InMemoryCache`` (L1) and a ``RedisCache`` (L2) from
        the injected settings, then wraps them in a ``LayeredCache``.

        Args:
            settings: ``LayeredCacheSettings`` — injected from the container.

        Returns:
            A started ``LayeredCache`` bound to ``CacheBackend``.

        Raises:
            ConnectionError: If L2 Redis is unreachable at startup.

        Edge cases:
            - ``LayeredCache.start()`` starts L1 first, then L2.  If L2 fails
              to start, L1 is already started — this is handled by LayeredCache
              (it stops in reverse order on error).
        """
        _logger.info(
            "RedisLayeredCacheConfiguration: starting LayeredCache "
            "(l2_url=%s, l2_prefix=%r, l1_max_size=%s, write_mode=%r).",
            settings.url,
            settings.key_prefix,
            settings.l1_max_size,
            settings.write_mode,
        )

        # Build L1 — in-process InMemoryCache with optional TTL and size cap.
        # L1 settings use CacheSettings with the l1_default_ttl from LayeredCacheSettings.
        l1_settings = CacheSettings(default_ttl=settings.l1_default_ttl)
        l1 = InMemoryCache(l1_settings, max_size=settings.l1_max_size)

        # Build L2 — Redis-backed cache.  Construct RedisCacheSettings from the
        # flat LayeredCacheSettings fields rather than reading a second env prefix.
        l2_settings = RedisCacheSettings(
            url=settings.url,
            key_prefix=settings.key_prefix,
            decode_responses=settings.decode_responses,
            socket_timeout=settings.socket_timeout,
            # default_ttl comes from the base CacheSettings field.
            default_ttl=settings.default_ttl,
        )
        l2 = RedisCache(l2_settings)

        # Wrap both layers in LayeredCache.
        # start() is called here — not deferred to __aenter__ — because providify
        # @PostConstruct is not called on provider-returned instances.
        cache = LayeredCache(
            l1,
            l2,
            write_mode=settings.write_mode,  # type: ignore[arg-type]
            promote_ttl=settings.promote_ttl,
        )
        await cache.start()
        return cache


__all__ = [
    "RedisCacheSettings",
    "RedisCache",
    "RedisCacheConfiguration",
    "LayeredCacheSettings",
    "RedisLayeredCacheConfiguration",
]
