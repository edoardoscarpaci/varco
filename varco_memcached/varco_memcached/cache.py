"""
varco_memcached.cache
======================
Memcached-backed ``CacheBackend`` implementation.

``MemcachedCache`` stores serialized bytes in Memcached via ``aiomcache``.
TTL is enforced natively by Memcached (the ``exptime`` parameter on ``set``).

Architecture
------------
::

    MemcachedCacheSettings    →  host, port, pool_size, key_prefix, serializer config
    MemcachedCache            →  CacheBackend backed by aiomcache.Client
    MemcachedCacheConfiguration  →  Providify @Configuration

Key differences from RedisCache
--------------------------------
- **Keys must be bytes**: ``aiomcache`` requires keys as ``bytes``; all logical
  keys are encoded after the prefix is applied.
- **No native key enumeration**: Memcached has no ``SCAN``-equivalent command.
  ``clear()`` maintains an in-process key registry (``set[str]``) tracking every
  key written via this instance.  Keys written by other processes (or before a
  restart) will NOT appear in the registry and will not be cleared — they expire
  naturally via their TTL.  Use ``flush_all()`` on the server if a full flush is
  needed (it affects ALL clients sharing that server).
- **No native ``exists()``**: Memcached has no ``EXISTS`` command; ``exists()``
  performs a ``get`` and checks for ``None``.  This incurs a full network round
  trip and deserialisation is skipped (raw bytes are returned by ``get``).
- **TTL is ``exptime``** (int, seconds).  ``0`` means no expiry (different from
  Redis ``SET`` without TTL — ``0`` is Memcached's equivalent of "never expire").

DESIGN: in-process key registry for clear()
    ✅ Safe for single-process apps — ``clear()`` only removes our own keys.
    ✅ No external coordination needed — no Redis-style SCAN or server-side scripts.
    ❌ Registry is lost on process restart — stale entries expire naturally via TTL.
    ❌ Multi-process deployments each maintain their own registry — ``clear()`` on
       one process will not clear keys set by another.
    Alternative considered: ``flush_all()`` — rejected because it affects all
    clients sharing the server, potentially evicting data owned by other services.

Thread safety:  ❌  Not thread-safe.  Use from a single event loop.
Async safety:   ✅  All public methods are ``async def``.

📚 Docs
- 🔍 https://github.com/aio-libs/aiomcache              — aiomcache API reference
- 🔍 https://github.com/memcached/memcached/wiki/Commands — Memcached protocol
"""

from __future__ import annotations

import logging
from typing import Any

import aiomcache
from pydantic_settings import SettingsConfigDict

from providify import Configuration, Inject, PreDestroy, Provider

from varco_core.cache.base import CacheBackend, InvalidationStrategy
from varco_core.cache.config import CacheSettings
from varco_core.serialization import JsonSerializer, Serializer

_logger = logging.getLogger(__name__)


# ── MemcachedCacheSettings ────────────────────────────────────────────────────


class MemcachedCacheSettings(CacheSettings):
    """
    Configuration for ``MemcachedCache``.

    Extends ``CacheSettings`` with Memcached connection settings.  Uses the
    ``VARCO_MEMCACHED_CACHE_`` env prefix so it can coexist with any Redis
    settings in the same environment.

    Attributes:
        host:       Memcached server hostname.
                    Env: ``VARCO_MEMCACHED_CACHE_HOST``.
        port:       Memcached server port.
                    Env: ``VARCO_MEMCACHED_CACHE_PORT``.
        pool_size:  Size of the ``aiomcache`` connection pool.
                    Env: ``VARCO_MEMCACHED_CACHE_POOL_SIZE``.
        key_prefix: String prepended to every key — provides namespace isolation
                    on a shared Memcached server.
                    Env: ``VARCO_MEMCACHED_CACHE_KEY_PREFIX``.

    Thread safety:  ✅ Immutable — frozen=True.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``key_prefix`` should end with a separator (e.g. ``":"``).  A prefix
          without one blurs key boundaries between namespaces.
        - ``pool_size=1`` disables connection pooling — acceptable for dev/test;
          increase to 4–8 for concurrent production workloads.
        - Memcached key restrictions: max 250 bytes, no whitespace or control
          characters.  The ``memcached_key()`` helper encodes the key as bytes
          but does NOT validate the length.  Callers must ensure keys are short.
    """

    model_config = SettingsConfigDict(
        # Separate from VARCO_CACHE_ base prefix and any Redis settings.
        env_prefix="VARCO_MEMCACHED_CACHE_",
        frozen=True,
    )

    host: str = "localhost"
    """Memcached server hostname.  Env: ``VARCO_MEMCACHED_CACHE_HOST``."""

    port: int = 11211
    """Memcached server port.  Env: ``VARCO_MEMCACHED_CACHE_PORT``."""

    pool_size: int = 2
    """aiomcache connection pool size.  Env: ``VARCO_MEMCACHED_CACHE_POOL_SIZE``."""

    key_prefix: str = ""
    """Key prefix for namespace isolation.  Env: ``VARCO_MEMCACHED_CACHE_KEY_PREFIX``."""

    def memcached_key(self, key: Any) -> bytes:
        """
        Return the full Memcached key for a logical cache key as ``bytes``.

        ``aiomcache`` requires keys as ``bytes`` — this helper encodes the
        prefixed key using UTF-8.

        Args:
            key: Logical cache key (any stringable value).

        Returns:
            UTF-8 encoded bytes of ``f"{self.key_prefix}{key}"``.

        Edge cases:
            - The resulting key must be ≤ 250 bytes (Memcached protocol limit).
              This helper does NOT enforce that limit — callers are responsible.
            - Whitespace or control characters in ``key`` will raise a
              ``ValueError`` from ``aiomcache`` at runtime.
        """
        return f"{self.key_prefix}{key}".encode()


# ── MemcachedCache ────────────────────────────────────────────────────────────


class MemcachedCache(CacheBackend):
    """
    Memcached-backed ``CacheBackend``.

    Stores serialized bytes in Memcached via ``aiomcache``.  TTL is enforced
    natively by Memcached via the ``exptime`` argument on each ``set`` call.

    An optional ``InvalidationStrategy`` can be layered on top for explicit
    or event-driven invalidation.

    ``clear()`` removes only the keys written by this cache instance in this
    process (tracked in an in-process registry).  Keys set by other processes
    or surviving from before a restart are unaffected and expire via TTL.
    See the module docstring for a full discussion of this limitation.

    Lifecycle::

        async with MemcachedCache(MemcachedCacheSettings()) as cache:
            await cache.set("user:42", user_obj, ttl=300)
            result = await cache.get("user:42")

    Args:
        settings:   Memcached connection and namespace settings.
        strategy:   Optional invalidation strategy for explicit/event-driven
                    eviction on top of TTL.  ``None`` = TTL-only (Memcached-native).
        serializer: Pluggable serializer.  Defaults to ``JsonSerializer()``.

    Thread safety:  ❌  Not thread-safe.  Use from a single event loop.
    Async safety:   ✅  All public methods are ``async def``.

    Edge cases:
        - ``get()`` may return ``None`` for two reasons: cache miss OR
          deserialisation failure (logged as a warning, key is returned as miss).
        - ``delete()`` on a non-existent key is a silent no-op.
        - ``exists()`` performs a full ``get`` round-trip (Memcached has no
          native ``EXISTS`` command) — avoid calling it in hot paths; prefer
          ``get()`` directly and branch on ``None``.
        - ``clear()`` only removes keys tracked in-process — see DESIGN note
          above for multi-process limitations.
        - Memcached ``exptime=0`` means "never expire" — a ``ttl`` of ``0``
          is treated as no-expiry, matching Redis behaviour.
    """

    def __init__(
        self,
        settings: MemcachedCacheSettings | None = None,
        *,
        strategy: InvalidationStrategy | None = None,
        serializer: Serializer[Any] | None = None,
    ) -> None:
        """
        Args:
            settings:   Cache settings.  Defaults to ``MemcachedCacheSettings()``
                        (localhost:11211, reads from env).
            strategy:   Optional invalidation strategy.  ``None`` = Memcached TTL only.
            serializer: Value serializer.  Defaults to ``JsonSerializer()``.
        """
        self._settings = settings or MemcachedCacheSettings()
        self._strategy = strategy
        # Default to JSON serialisation — handles dicts, Pydantic models, primitives.
        self._serializer: Serializer[Any] = serializer or JsonSerializer()
        self._client: aiomcache.Client | None = None

        # In-process registry of every logical key written via this instance.
        # Needed because Memcached has no SCAN command — clear() uses this set
        # to delete only the keys we own, leaving other keys untouched.
        self._key_registry: set[str] = set()

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Connect to Memcached and start the invalidation strategy.

        Creates an ``aiomcache.Client`` connection pool.  The pool does not
        eagerly connect — the first operation will establish the connection.

        Args:
            (none)

        Raises:
            RuntimeError: If already started.
        """
        if self._client is not None:
            raise RuntimeError(
                "MemcachedCache.start() called on an already-started cache. "
                "Call stop() first."
            )
        # pool_size controls the aiomcache connection pool size — higher values
        # allow more concurrent requests without blocking on a single connection.
        self._client = aiomcache.Client(
            self._settings.host,
            self._settings.port,
            pool_size=self._settings.pool_size,
        )
        if self._strategy is not None:
            await self._strategy.start()
        _logger.info(
            "MemcachedCache started (host=%s, port=%s, pool_size=%s).",
            self._settings.host,
            self._settings.port,
            self._settings.pool_size,
        )

    @PreDestroy
    async def stop(self) -> None:
        """
        Close the Memcached connection pool and stop the strategy.  Idempotent.

        Decorated with ``@PreDestroy`` so the Providify container calls this
        automatically on ``await container.ashutdown()``.
        """
        if self._client is None:
            # Already stopped or never started — safe no-op.
            return
        if self._strategy is not None:
            await self._strategy.stop()
        await self._client.close()
        self._client = None
        # Clear the registry on stop — any keys from a previous run are stale
        # from this instance's perspective.
        self._key_registry.clear()
        _logger.info("MemcachedCache stopped.")

    # ── Cache operations ───────────────────────────────────────────────────────

    async def get(self, key: Any, *, type_hint: type | None = None) -> Any | None:
        """
        Return the cached value for ``key``, or ``None`` on miss.

        Args:
            key:       Logical cache key.
            type_hint: Optional type hint passed to the serialiser's
                       ``deserialize()`` for typed deserialisation.

        Returns:
            Deserialised value, or ``None`` if absent or strategy-invalidated.

        Raises:
            RuntimeError: If the cache has not been started.

        Edge cases:
            - Returns ``None`` on deserialisation failure (logged as warning).
            - The invalidation strategy is checked first; if it signals
              invalidation the key is deleted from Memcached proactively.
        """
        self._require_started()
        mc_key = self._settings.memcached_key(key)

        # Check invalidation strategy before hitting Memcached.
        if self._strategy is not None and self._strategy.should_invalidate(key, {}):
            # Proactively evict the key so stale data is not returned on the
            # next call (e.g. after strategy state is reset).
            await self._client.delete(mc_key)  # type: ignore[union-attr]
            _logger.debug("MemcachedCache: strategy-evicted key %r.", key)
            return None

        raw: bytes | None = await self._client.get(mc_key)  # type: ignore[union-attr]
        if raw is None:
            return None

        try:
            return self._serializer.deserialize(raw, type_hint)
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "MemcachedCache: failed to deserialise key %r: %s",
                key,
                exc,
                exc_info=True,
            )
            return None

    async def set(
        self,
        key: Any,
        value: Any,
        *,
        ttl: float | None = None,
    ) -> None:
        """
        Serialise and store ``value`` in Memcached under ``key``.

        Args:
            key:   Logical cache key.
            value: Value to serialise and store.
            ttl:   TTL in seconds.  ``None`` falls back to
                   ``settings.default_ttl``.  ``None`` with no default = no expiry
                   (``exptime=0`` in Memcached protocol).

        Raises:
            RuntimeError: If the cache has not been started.

        Edge cases:
            - ``exptime=0`` in Memcached means "no expiry", not "expire immediately"
              (unlike some other caches).  Passing ``ttl=0`` here is treated
              as no-expiry — it will NOT evict immediately.
        """
        self._require_started()
        mc_key = self._settings.memcached_key(key)
        effective_ttl = ttl if ttl is not None else self._settings.default_ttl

        # Memcached exptime is an int in seconds; 0 = no expiry.
        # Cast to int — float TTLs are truncated, matching Redis behaviour.
        exptime = int(effective_ttl) if effective_ttl is not None else 0

        data = self._serializer.serialize(value)
        await self._client.set(mc_key, data, exptime=exptime)  # type: ignore[union-attr]

        # Track this key so clear() can find it later.
        # We record the logical (unprefixed) key — memcached_key() re-applies
        # the prefix when constructing the delete call in clear().
        self._key_registry.add(str(key))
        _logger.debug("MemcachedCache: stored key %r (exptime=%s).", key, exptime)

    async def delete(self, key: Any) -> None:
        """
        Remove ``key`` from Memcached.  Idempotent.

        Args:
            key: Logical cache key to remove.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        await self._client.delete(self._settings.memcached_key(key))  # type: ignore[union-attr]
        # Remove from the local registry — if not present, discard is a no-op.
        self._key_registry.discard(str(key))

    async def exists(self, key: Any) -> bool:
        """
        Return ``True`` if ``key`` exists in Memcached (and strategy allows it).

        DESIGN: get-based check over a native EXISTS
            Memcached has no ``EXISTS`` command — a ``get`` is the canonical
            way to check presence.  The raw bytes are returned and discarded
            without deserialisation overhead.
            ✅ Correct — accurately reflects whether the key is still alive.
            ❌ Full network round-trip per check — avoid in hot paths; use
               ``get()`` directly instead.

        Args:
            key: Logical cache key to check.

        Returns:
            ``True`` if the key exists in Memcached and is not strategy-invalidated.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        if self._strategy is not None and self._strategy.should_invalidate(key, {}):
            return False
        # aiomcache.get returns None on miss; non-None bytes on hit.
        raw: bytes | None = await self._client.get(  # type: ignore[union-attr]
            self._settings.memcached_key(key)
        )
        return raw is not None

    async def clear(self) -> None:
        """
        Delete all keys tracked by this cache instance from Memcached.

        Only removes keys written via ``set()`` on this instance in the current
        process lifetime.  Keys written by other processes or before a restart
        are unaffected and will expire naturally via their TTL.

        ⚠️ If a full server flush is needed across all clients, call
        ``flush_all()`` directly on the ``aiomcache.Client`` — but be aware
        that this evicts ALL data on the server, affecting every client.

        Raises:
            RuntimeError: If the cache has not been started.

        Edge cases:
            - If the key registry is empty (e.g. right after ``start()``),
              this is a no-op.
            - The registry is cleared atomically after all deletes succeed.
              A partial failure leaves the registry in a consistent state
              (only successfully-deleted keys are removed).
        """
        self._require_started()
        if not self._key_registry:
            return

        # Snapshot the current registry — new keys may be written concurrently
        # and should not be deleted by this clear() call.
        keys_to_clear = list(self._key_registry)
        deleted = 0
        for logical_key in keys_to_clear:
            await self._client.delete(  # type: ignore[union-attr]
                self._settings.memcached_key(logical_key)
            )
            self._key_registry.discard(logical_key)
            deleted += 1

        _logger.debug("MemcachedCache: cleared %d key(s).", deleted)

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _require_started(self) -> None:
        """
        Raise ``RuntimeError`` if the cache has not been started.

        Raises:
            RuntimeError: If ``start()`` has not been called.
        """
        if self._client is None:
            raise RuntimeError(
                f"{type(self).__name__} is not started. "
                f"Call 'await cache.start()' or use 'async with cache' first."
            )

    def __repr__(self) -> str:
        return (
            f"MemcachedCache("
            f"host={self._settings.host!r}, "
            f"port={self._settings.port}, "
            f"key_prefix={self._settings.key_prefix!r}, "
            f"strategy={self._strategy!r}, "
            f"started={self._client is not None}, "
            f"tracked_keys={len(self._key_registry)})"
        )


# ── MemcachedCacheConfiguration ───────────────────────────────────────────────


@Configuration
class MemcachedCacheConfiguration:
    """
    Providify ``@Configuration`` that wires ``MemcachedCache`` into the container.

    Provides:
        ``MemcachedCacheSettings`` — default localhost settings; override before
                                      installing this configuration.
        ``CacheBackend``            — started ``MemcachedCache`` singleton.

    Lifecycle:
        The cache is started inside the provider and stopped automatically by
        ``@PreDestroy`` on ``MemcachedCache.stop()`` when
        ``await container.ashutdown()`` is called.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def`` — safe to ``await``.

    Example::

        container = DIContainer()
        await container.ainstall(MemcachedCacheConfiguration)
        cache = await container.aget(CacheBackend)
        await cache.set("key", value, ttl=300)
        result = await cache.get("key")
        await container.ashutdown()

    Overriding settings::

        container.provide(
            lambda: MemcachedCacheSettings(
                host=os.environ["MEMCACHED_HOST"],
                key_prefix="myapp:",
            ),
            MemcachedCacheSettings,
        )
        await container.ainstall(MemcachedCacheConfiguration)
    """

    @Provider(singleton=True)
    def memcached_cache_settings(self) -> MemcachedCacheSettings:
        """
        Default ``MemcachedCacheSettings`` pointing at ``localhost:11211``.

        Returns:
            A ``MemcachedCacheSettings`` with development-friendly defaults.
        """
        # Reads from VARCO_MEMCACHED_CACHE_* env vars if set.
        return MemcachedCacheSettings.from_env()

    @Provider(singleton=True)
    async def memcached_cache(
        self,
        settings: Inject[MemcachedCacheSettings],
    ) -> CacheBackend:
        """
        Create, start, and return the ``MemcachedCache`` singleton.

        Args:
            settings: ``MemcachedCacheSettings`` — injected from the container.

        Returns:
            A started ``MemcachedCache`` bound to ``CacheBackend``.

        Raises:
            ConnectionError: If Memcached is unreachable at first use (aiomcache
                             connects lazily — errors surface on the first operation,
                             not on ``start()``).
        """
        _logger.info(
            "MemcachedCacheConfiguration: starting MemcachedCache "
            "(host=%s, port=%s, prefix=%r).",
            settings.host,
            settings.port,
            settings.key_prefix,
        )
        cache = MemcachedCache(settings)
        await cache.start()
        return cache


__all__ = [
    "MemcachedCacheSettings",
    "MemcachedCache",
    "MemcachedCacheConfiguration",
]
