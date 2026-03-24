"""
varco_core.cache
================
Backend-agnostic async cache system for varco applications.

All public symbols are importable directly from ``varco_core.cache``::

    # Core protocol and ABC
    from varco_core.cache import AsyncCache, CacheBackend

    # In-process backends
    from varco_core.cache import NoOpCache, InMemoryCache

    # Multi-layer cache
    from varco_core.cache import LayeredCache

    # Invalidation strategies
    from varco_core.cache import (
        InvalidationStrategy,
        TTLStrategy,
        ExplicitStrategy,
        TaggedStrategy,
        EventDrivenStrategy,
        CompositeStrategy,
    )

    # Configuration
    from varco_core.cache import CacheSettings

Layer map::

    AsyncCache (Protocol) / CacheBackend (ABC)
        ↑ implemented by
    NoOpCache       — no-op placeholder
    InMemoryCache   — in-process dict backend
    LayeredCache    — L1/L2/… composite
    RedisCache      — (varco_redis) Redis-backed

    InvalidationStrategy (ABC)
        ↑ implemented by
    TTLStrategy         — time-based expiry
    ExplicitStrategy    — manual key invalidation
    TaggedStrategy      — tag-based bulk invalidation
    EventDrivenStrategy — event-bus-triggered invalidation
    CompositeStrategy   — aggregates multiple strategies (logical OR)

Example — simple in-memory cache with TTL::

    from varco_core.cache import InMemoryCache, TTLStrategy

    async with InMemoryCache(strategy=TTLStrategy(300)) as cache:
        await cache.set("user:42", user_obj)
        result = await cache.get("user:42")   # None after 300s

Example — layered cache (L1 in-memory, L2 Redis)::

    from varco_core.cache import InMemoryCache, LayeredCache, TTLStrategy
    from varco_redis.cache import RedisCache, RedisCacheSettings

    l1 = InMemoryCache(strategy=TTLStrategy(60))
    l2 = RedisCache(RedisCacheSettings())

    async with LayeredCache(l1, l2, promote_ttl=60) as cache:
        await cache.set("key", value)
        result = await cache.get("key")   # served from L1 on second read

Example — composable invalidation::

    from varco_core.cache import (
        InMemoryCache, CompositeStrategy, TTLStrategy, EventDrivenStrategy
    )

    strategy = CompositeStrategy(
        TTLStrategy(300),
        EventDrivenStrategy(bus, channel="cache-invalidations"),
    )
    async with InMemoryCache(strategy=strategy) as cache:
        ...
"""

from __future__ import annotations

from varco_core.cache.base import AsyncCache, CacheBackend, InvalidationStrategy
from varco_core.cache.config import CacheSettings
from varco_core.cache.consumer import CacheInvalidationConsumer
from varco_core.cache.decorator import cached
from varco_core.cache.mixin import CacheServiceMixin
from varco_core.cache.service import (
    CacheInvalidated,
    CacheInvalidationEvent,
    CachedService,
)
from varco_core.cache.invalidation import (
    CompositeStrategy,
    ExplicitStrategy,
    TaggedStrategy,
    TTLStrategy,
)
from varco_core.cache.layered import LayeredCache
from varco_core.cache.memory import InMemoryCache, NoOpCache

__all__ = [
    # ── Core abstractions ──────────────────────────────────────────────────────
    "AsyncCache",
    "CacheBackend",
    "InvalidationStrategy",
    # ── Configuration ─────────────────────────────────────────────────────────
    "CacheSettings",
    # ── In-process backends ────────────────────────────────────────────────────
    "NoOpCache",
    "InMemoryCache",
    # ── Multi-layer ────────────────────────────────────────────────────────────
    "LayeredCache",
    # ── Service mixin ──────────────────────────────────────────────────────────
    "CacheServiceMixin",
    # ── Decorator ──────────────────────────────────────────────────────────────
    "cached",
    # ── Service wrapper ────────────────────────────────────────────────────────
    "CachedService",
    # ── Domain events ──────────────────────────────────────────────────────────
    "CacheInvalidated",
    "CacheInvalidationEvent",  # backward-compat alias
    # ── Invalidation strategies ────────────────────────────────────────────────
    "TTLStrategy",
    "ExplicitStrategy",
    "TaggedStrategy",
    "CompositeStrategy",
    # ── Event-driven invalidation consumer ─────────────────────────────────────
    "CacheInvalidationConsumer",
]
