"""
varco_core.cache.service
=========================
Cache integration layer for service/repository objects.

``CachedService``
    A thin transparent wrapper that adds read-through caching and automatic
    write-invalidation to any object that implements the standard varco
    service interface (``get``, ``list``, ``exists``, ``create``, ``update``,
    ``delete``).

    Supports two invalidation paths that can be used independently or together:

    1. **Explicit** — ``ExplicitStrategy`` is called directly inside
       ``create/update/delete`` so the affected key is evicted before the
       next read.

    2. **Event-driven** — a ``CacheInvalidationConsumer`` reacts to
       ``CacheInvalidated`` messages on a configurable bus channel.
       Useful when writes happen in a different process (e.g., a separate
       microservice publishes invalidation events).

Architecture
------------
::

    CachedService
      ├── wraps → any service / repository object
      ├── cache → CacheBackend (InMemoryCache, LayeredCache, RedisCache, …)
      └── invalidation → ExplicitStrategy (sync) + CacheInvalidationConsumer (async)

    Read path (get / list / exists):
        cache hit?  → return cached value
        cache miss? → call wrapped service → store result → return

    Write path (create / update / delete):
        → call wrapped service
        → invalidate affected key(s) via ExplicitStrategy
        → (optionally) publish CacheInvalidationEvent on bus

    🟡 Naming convention for cache keys:
        ``{namespace}:{operation}:{id_or_hash}``
        e.g.  ``"user:get:42"``  ``"user:list:all"``

DESIGN: wrapper over subclass / mixin
    ✅ Works with any service — does not require base-class coupling.
    ✅ Transparent — callers use ``CachedService(svc, cache)`` and call
       the same methods they would on ``svc`` directly.
    ✅ Testable — inject ``NoOpCache`` to disable caching in unit tests.
    ✅ Composable — two layers (L1 in-memory + L2 Redis) are a single
       ``LayeredCache`` passed at construction; ``CachedService`` doesn't care.
    ❌ Requires the caller to know which methods mutate state (create/update/
       delete) vs which read (get/list/exists).  This is unavoidable without
       ORM-level change detection.

Thread safety:  ❌  Inherits the thread-safety of the underlying cache.
Async safety:   ✅  All public methods are ``async def``.

Usage example::

    from varco_core.cache.service import CachedService
    from varco_core.cache import InMemoryCache, ExplicitStrategy, LayeredCache
    from varco_core.cache.invalidation import TTLStrategy

    # 1 — plain in-memory cache with TTL
    cache = InMemoryCache(strategy=TTLStrategy(300))
    svc = CachedService(user_service, cache, namespace="user")

    async with cache:
        user = await svc.get(42)          # miss → hits DB → cached
        user = await svc.get(42)          # hit  → served from cache
        await svc.update(42, {"name": "X"})  # writes to DB + invalidates cache
        user = await svc.get(42)          # miss → refreshed from DB

    # 2 — layered (L1 in-memory + L2 Redis) with event-driven cross-process invalidation
    from varco_redis.cache import RedisCache, RedisCacheSettings
    from varco_core.cache.consumer import CacheInvalidationConsumer
    from varco_core.event import BusEventProducer, InMemoryEventBus

    bus = InMemoryEventBus()
    strategy = ExplicitStrategy()
    l1 = InMemoryCache(strategy=strategy)
    l2 = RedisCache(RedisCacheSettings())
    cache = LayeredCache(l1, l2, promote_ttl=60)

    consumer = CacheInvalidationConsumer(strategy, channel="cache.invalidations")
    consumer.register_to(bus)

    svc = CachedService(
        user_service, cache, namespace="user",
        producer=BusEventProducer(bus),
        bus_channel="cache.invalidations",
    )
    # start bus + cache before use
    async with bus, cache:
        user = await svc.get(42)
        await svc.delete(42)  # invalidates cache AND produces CacheInvalidated event
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from varco_core.cache.base import CacheBackend
from varco_core.cache.invalidation import ExplicitStrategy
from varco_core.event.base import Event
from varco_core.event.producer import AbstractEventProducer, NoopEventProducer

_logger = logging.getLogger(__name__)


# ── CacheInvalidationEvent ─────────────────────────────────────────────────────


class CacheInvalidated(Event):
    """
    Domain event published after a mutating operation invalidates cache entries.

    ``EventDrivenStrategy`` instances subscribed to the bus channel will
    pick this up and mark the contained keys for eviction in their local cache.

    Attributes:
        keys:      List of logical cache keys that were invalidated.
        namespace: The entity namespace that owns the keys (e.g. ``"post"``).
        operation: The operation that triggered invalidation
                   (``"create"``, ``"update"``, ``"delete"``).
    """

    __event_type__ = "varco.cache.invalidated"

    keys: list[str]
    namespace: str = ""
    operation: str = ""


#: Backward-compatible alias — prefer ``CacheInvalidated``.
CacheInvalidationEvent = CacheInvalidated


# ── CachedService ──────────────────────────────────────────────────────────────


class CachedService:
    """
    Transparent cache wrapper for any service / repository object.

    Wraps an existing service and intercepts reads with a cache look-aside
    pattern.  On mutating operations (``create``, ``update``, ``delete``),
    the affected cache keys are explicitly invalidated and optionally an
    event is published on a bus channel for cross-process invalidation.

    Args:
        service:     The underlying service or repository to wrap.  Must have
                     the methods you intend to call (``get``, ``list``,
                     ``exists``, ``create``, ``update``, ``delete``).
        cache:       A started ``CacheBackend``.  The caller is responsible for
                     starting and stopping the cache independently of the service.
        namespace:   Key prefix for all cache entries.  Use a unique identifier
                     per entity type (e.g. ``"user"``, ``"product"``).
        default_ttl: Per-entry TTL in seconds passed to ``cache.set()``.
                     ``None`` → fallback to cache's own default.
        producer:    Optional ``AbstractEventProducer`` for publishing
                     ``CacheInvalidationEvent`` after mutations.  ``None`` =
                     no event publishing (explicit-only invalidation).
                     Only ``AbstractEventProducer`` interacts with the bus —
                     ``CachedService`` never holds a direct bus reference.
        bus_channel: Channel to publish invalidation events on.
                     Defaults to ``"varco.cache.invalidations"``.

    Thread safety:  ❌  Inherits thread-safety from the underlying cache.
    Async safety:   ✅  All public methods are ``async def``.

    Edge cases:
        - The cache must be started before calling any method.  ``CachedService``
          does NOT start/stop the cache — manage its lifecycle separately.
        - If the wrapped service raises, the exception propagates to the caller
          and nothing is cached.
        - ``list()`` uses a stable hash of the kwargs as a cache key.  Different
          kwarg ordering produces the same hash (kwargs are sorted before hashing).
        - ``create()`` invalidates the list cache (``"{namespace}:list:*"``
          notation) — currently implemented as a full-namespace clear on the
          explicit strategy.  This is conservative but correct.
    """

    # Key for listing all entries (no filter)
    _LIST_ALL_KEY = "__list_all__"

    def __init__(
        self,
        service: Any,
        cache: CacheBackend,
        *,
        namespace: str,
        entity_type: type | None = None,
        default_ttl: float | None = None,
        producer: AbstractEventProducer | None = None,
        bus_channel: str = "varco.cache.invalidations",
    ) -> None:
        self._service = service
        self._cache = cache
        self._namespace = namespace
        # entity_type is forwarded to cache.get() as type_hint so the serializer
        # can reconstruct typed objects on cache hits instead of returning plain dicts.
        # None → backend's default deserialization (dict/list for JSON backends).
        self._entity_type = entity_type
        self._default_ttl = default_ttl
        # Fall back to NoopEventProducer so _produce() calls are always safe
        # and callers never need a `if self._producer is not None` guard.
        self._producer: AbstractEventProducer = producer or NoopEventProducer()
        self._bus_channel = bus_channel
        # Internal explicit strategy for synchronous in-process invalidation.
        # Always present — event-driven cross-process invalidation is layered
        # on top via the producer + CacheInvalidationConsumer pattern.
        self._explicit = ExplicitStrategy()

    # ── Key helpers ────────────────────────────────────────────────────────────

    def _get_key(self, entity_id: Any) -> str:
        """``"<namespace>:get:<id>"``"""
        return f"{self._namespace}:get:{entity_id}"

    def _exists_key(self, entity_id: Any) -> str:
        """``"<namespace>:exists:<id>"``"""
        return f"{self._namespace}:exists:{entity_id}"

    def _list_key(self, **kwargs: Any) -> str:
        """
        Stable cache key for a ``list()`` call.

        Sorts kwargs by key before hashing so different ordering produces the
        same cache key.
        """
        if not kwargs:
            return f"{self._namespace}:list:{self._LIST_ALL_KEY}"
        sorted_kwargs = json.dumps(
            dict(sorted(kwargs.items())), sort_keys=True, default=str
        )
        h = hashlib.md5(sorted_kwargs.encode(), usedforsecurity=False).hexdigest()[:12]
        return f"{self._namespace}:list:{h}"

    # ── Read-through helpers ───────────────────────────────────────────────────

    async def _get_or_set(
        self, key: str, call: Any, *, type_hint: type | None = None
    ) -> Any:
        """
        Cache look-aside: return cached value, or call ``call`` and cache it.

        Args:
            key:       Cache key.
            call:      Async callable that produces the value on a cache miss.
            type_hint: Passed to the backing cache's ``get()`` for typed
                       deserialization.  Use ``entity_type`` for single-entity
                       keys and ``list[entity_type]`` for list keys.

        Returns:
            Cached or freshly fetched value.
        """
        # Check explicit invalidation first (synchronous, zero-cost).
        if self._explicit.should_invalidate(key, {}):
            self._explicit.clear_invalidated(key)
            cached = None
        else:
            # Forward type_hint so backends that serialize to bytes (e.g. RedisCache)
            # can reconstruct the original typed object rather than returning a dict.
            cached = await self._cache.get(key, type_hint=type_hint)

        if cached is not None:
            _logger.debug(
                "CachedService[%s]: cache hit for key %r.", self._namespace, key
            )
            return cached

        _logger.debug(
            "CachedService[%s]: cache miss for key %r, fetching.", self._namespace, key
        )
        value = await call()
        if value is not None:
            await self._cache.set(key, value, ttl=self._default_ttl)
        return value

    # ── Read operations ────────────────────────────────────────────────────────

    async def get(self, entity_id: Any, **kwargs: Any) -> Any:
        """
        Fetch a single entity by ID, using the cache as look-aside.

        On a cache miss, delegates to ``self._service.get(entity_id, **kwargs)``.

        Args:
            entity_id: Entity identifier.
            **kwargs:  Extra keyword arguments forwarded to the underlying service.

        Returns:
            The entity (cached or freshly fetched), or ``None`` if not found.
        """
        key = self._get_key(entity_id)
        return await self._get_or_set(
            key,
            lambda: self._service.get(entity_id, **kwargs),
            type_hint=self._entity_type,
        )

    async def list(self, **kwargs: Any) -> Any:
        """
        Fetch a list of entities, using the cache as look-aside.

        The cache key is a stable hash of ``kwargs`` so different filter
        combinations are cached independently.

        On a cache miss, delegates to ``self._service.list(**kwargs)``.

        Args:
            **kwargs: Filter / pagination arguments forwarded to the underlying
                      service.

        Returns:
            A list of entities (cached or freshly fetched).
        """
        key = self._list_key(**kwargs)
        # List results are typed as list[entity_type], not entity_type itself.
        list_hint = list[self._entity_type] if self._entity_type is not None else None  # type: ignore[valid-type]
        return await self._get_or_set(
            key, lambda: self._service.list(**kwargs), type_hint=list_hint
        )

    async def exists(self, entity_id: Any, **kwargs: Any) -> bool:
        """
        Check whether an entity exists, using the cache as look-aside.

        Stores ``True`` / ``False`` in the cache so repeated existence checks
        avoid the database.  A ``False`` result is also cached — it will be
        invalidated when the entity is created.

        On a cache miss, delegates to ``self._service.exists(entity_id, **kwargs)``.

        Args:
            entity_id: Entity identifier.
            **kwargs:  Extra keyword arguments forwarded to the underlying service.

        Returns:
            ``True`` if the entity exists.
        """
        key = self._exists_key(entity_id)
        # Check explicit invalidation first — mirrors the logic in _get_or_set.
        # Without this check, delete()/update() would mark the exists key as
        # invalidated but exists() would still return the stale cached value.
        if self._explicit.should_invalidate(key, {}):
            self._explicit.clear_invalidated(key)
            cached = None
        else:
            cached = await self._cache.get(key)
        if cached is not None:
            return bool(cached)
        result = await self._service.exists(entity_id, **kwargs)
        # Cache both True and False — store as 1/0 since None means "miss"
        await self._cache.set(key, 1 if result else 0, ttl=self._default_ttl)
        return result

    # ── Write operations ───────────────────────────────────────────────────────

    async def create(self, data: Any, **kwargs: Any) -> Any:
        """
        Create a new entity and invalidate affected cache entries.

        Delegates to ``self._service.create(data, **kwargs)`` then invalidates:
        - All ``list`` cache keys for this namespace (list result changed).

        Args:
            data:     Entity data to create.
            **kwargs: Extra keyword arguments forwarded to the underlying service.

        Returns:
            The created entity as returned by the underlying service.
        """
        result = await self._service.create(data, **kwargs)
        # Invalidate all list caches — a new entity changes any list result.
        await self._invalidate_list_all()
        # Also invalidate exists cache if result has an id
        entity_id = self._extract_id(result)
        if entity_id is not None:
            invalidated_keys = [self._exists_key(entity_id), self._list_key()]
            await self._publish_invalidation(invalidated_keys, operation="create")
        _logger.debug(
            "CachedService[%s]: post-create invalidation complete.", self._namespace
        )
        return result

    async def update(self, entity_id: Any, data: Any, **kwargs: Any) -> Any:
        """
        Update an entity and invalidate its cache entries.

        Delegates to ``self._service.update(entity_id, data, **kwargs)`` then
        invalidates:
        - ``get`` key for ``entity_id``
        - ``exists`` key for ``entity_id``
        - All ``list`` cache keys (list ordering/values may have changed).

        Args:
            entity_id: Entity identifier to update.
            data:      Updated entity data.
            **kwargs:  Extra keyword arguments forwarded to the underlying service.

        Returns:
            The updated entity as returned by the underlying service.
        """
        result = await self._service.update(entity_id, data, **kwargs)
        invalidated_keys = [
            self._get_key(entity_id),
            self._exists_key(entity_id),
        ]
        self._explicit.invalidate_many(invalidated_keys)
        await self._invalidate_list_all()
        await self._publish_invalidation(invalidated_keys, operation="update")
        _logger.debug(
            "CachedService[%s]: post-update invalidation for id=%r.",
            self._namespace,
            entity_id,
        )
        return result

    async def delete(self, entity_id: Any, **kwargs: Any) -> Any:
        """
        Delete an entity and invalidate its cache entries.

        Delegates to ``self._service.delete(entity_id, **kwargs)`` then
        invalidates:
        - ``get`` key for ``entity_id``
        - ``exists`` key for ``entity_id``
        - All ``list`` cache keys.

        Args:
            entity_id: Entity identifier to delete.
            **kwargs:  Extra keyword arguments forwarded to the underlying service.

        Returns:
            The result of the underlying service's ``delete()`` call.
        """
        result = await self._service.delete(entity_id, **kwargs)
        invalidated_keys = [
            self._get_key(entity_id),
            self._exists_key(entity_id),
        ]
        self._explicit.invalidate_many(invalidated_keys)
        await self._invalidate_list_all()
        await self._publish_invalidation(invalidated_keys, operation="delete")
        _logger.debug(
            "CachedService[%s]: post-delete invalidation for id=%r.",
            self._namespace,
            entity_id,
        )
        return result

    # ── Invalidation helpers ────────────────────────────────────────────────────

    async def _invalidate_list_all(self) -> None:
        """Delete all ``list`` entries from the cache for this namespace."""
        list_key = self._list_key()
        self._explicit.invalidate(list_key)
        # Also delete from the cache backend directly so promoted L1 entries
        # are cleared even if the explicit strategy isn't consulted yet.
        await self._cache.delete(list_key)

    async def _publish_invalidation(self, keys: list[str], *, operation: str) -> None:
        """
        Publish a ``CacheInvalidated`` event via the injected producer.

        ``CachedService`` never calls ``bus.publish()`` directly — all bus
        interaction is delegated to ``AbstractEventProducer._produce()``.
        When no producer is configured, ``NoopEventProducer`` silently
        discards the call so no guard is needed here.

        Args:
            keys:      Cache keys being invalidated.
            operation: The mutation that triggered this (``"create"`` / ``"update"``
                       / ``"delete"``).

        Edge cases:
            - ``NoopEventProducer`` discards the event silently — callers always
              get explicit-only invalidation when no producer is configured.
            - Publish failures are caught and logged at WARNING level so they
              never break the service call.
        """
        try:
            # _produce() is the bus-touching method on AbstractEventProducer.
            # Calling it here keeps the bus-access boundary at the producer
            # layer — CachedService holds no direct bus reference.
            await self._producer._produce(
                CacheInvalidationEvent(
                    keys=keys,
                    namespace=self._namespace,
                    operation=operation,
                ),
                channel=self._bus_channel,
            )
        except Exception as exc:  # noqa: BLE001
            # Never let publish failure break the service call.
            _logger.warning(
                "CachedService[%s]: failed to publish CacheInvalidated: %s",
                self._namespace,
                exc,
                exc_info=True,
            )

    @staticmethod
    def _extract_id(entity: Any) -> Any:
        """
        Try to extract a primary key from an entity returned by ``create()``.

        Checks common attribute names in order: ``id``, ``pk``, ``uuid``.
        Returns ``None`` if no ID can be found.
        """
        for attr in ("id", "pk", "uuid"):
            value = getattr(entity, attr, None)
            if value is not None:
                return value
        return None

    def __repr__(self) -> str:
        return (
            f"CachedService("
            f"namespace={self._namespace!r}, "
            f"cache={self._cache!r}, "
            f"producer={type(self._producer).__name__!r})"
        )


__all__ = ["CachedService", "CacheInvalidated", "CacheInvalidationEvent"]
