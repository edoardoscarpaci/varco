"""
Regression guard — ``CoreModule``'s ``core.stores`` (INFRA) component vs. a
provider-started ``CacheBackend``.

Symptom (examples/00-full-stack-post-api integration tests): uvicorn never
started, because the INFRA phase raised
``RuntimeError: RedisCache.start() called on an already-started cache``.

Every shipped cache backend provider (``RedisCacheConfiguration.redis_cache``,
``RedisLayeredCacheConfiguration.layered_cache``,
``MemcachedCacheConfiguration.memcached_cache``) returns an *already-started*
instance, and ``CacheBackend.start()``'s documented contract is to raise
``RuntimeError`` when already started. ``core.stores`` must therefore treat
that ``RuntimeError`` as "already started", while still starting a cache
nothing has started yet (the ``@Singleton`` ``InMemoryCache`` default) and
still failing fast on a real start failure.

Docker-free: ``InMemoryCache`` stands in for any provider-produced backend.
"""

from __future__ import annotations

import pytest
from providify import Configuration, DIContainer, Provider
from varco_core.cache.base import CacheBackend
from varco_core.cache.memory import InMemoryCache
from varco_core.di.module import CoreModule


def _core_stores(container: DIContainer):  # type: ignore[no-untyped-def]
    """Return the ``core.stores`` LifecycleComponent CoreModule contributes."""
    (component,) = [c for c in CoreModule().components(container) if c.name == "core.stores"]
    return component


async def test_regression_core_stores_double_starts_provider_started_cache() -> None:
    # User reports: create_varco_app startup dies with "already-started cache"
    # when VARCO_CACHE_BACKEND=redis. Correct behaviour: core.stores starts
    # cleanly, because a provider that returns a started cache already owns
    # that start (the established provider shape, Plan 050 §D-owner rows 2/3/7)
    # and CacheBackend.start() is contractually non-idempotent.
    @Configuration
    class _StartedCacheConfiguration:
        @Provider(singleton=True)
        async def cache(self) -> CacheBackend:
            cache = InMemoryCache()
            await cache.start()  # same shape as RedisCacheConfiguration.redis_cache
            return cache

    container = DIContainer()
    await container.ainstall(_StartedCacheConfiguration)
    component = _core_stores(container)

    await component.start()  # raised RuntimeError("...already-started cache...")

    cache = await container.aget(CacheBackend)
    await cache.set("k", "v")
    assert await cache.get("k") == "v"
    await component.stop()


async def test_core_stores_starts_a_cache_nothing_started_yet() -> None:
    # The over-fix guard: dropping CacheBackend from core.stores (or skipping
    # it entirely) would leave the scanned @Singleton InMemoryCache default
    # unstarted, and the first cache call would raise "not started".
    @Configuration
    class _UnstartedCacheConfiguration:
        @Provider(singleton=True)
        def cache(self) -> CacheBackend:
            return InMemoryCache()

    container = DIContainer()
    container.install(_UnstartedCacheConfiguration)
    component = _core_stores(container)

    await component.start()

    cache = await container.aget(CacheBackend)
    await cache.set("k", "v")  # would raise RuntimeError if never started
    assert await cache.get("k") == "v"
    await component.stop()


async def test_core_stores_propagates_a_non_runtime_error_start_failure() -> None:
    # Fail-fast guard: only the contract's "already started" RuntimeError is
    # tolerated — a genuine start failure (unreachable broker) must still
    # abort startup rather than leave a dead cache behind a live app.
    class _UnreachableCache(InMemoryCache):
        async def start(self) -> None:
            raise ConnectionError("cache backend unreachable")

    @Configuration
    class _BrokenCacheConfiguration:
        @Provider(singleton=True)
        def cache(self) -> CacheBackend:
            return _UnreachableCache()

    container = DIContainer()
    container.install(_BrokenCacheConfiguration)

    with pytest.raises(ConnectionError, match="unreachable"):
        await _core_stores(container).start()


async def test_core_stores_start_stop_start_cycle_on_provider_started_cache() -> None:
    # Boundary: after core.stores' stop() the cache is genuinely stopped, so
    # the next start() must really start it again (not be mistaken for the
    # tolerated already-started case and leave it dead).
    @Configuration
    class _StartedCacheConfiguration:
        @Provider(singleton=True)
        async def cache(self) -> CacheBackend:
            cache = InMemoryCache()
            await cache.start()
            return cache

    container = DIContainer()
    await container.ainstall(_StartedCacheConfiguration)
    component = _core_stores(container)

    await component.start()
    await component.stop()
    await component.start()

    cache = await container.aget(CacheBackend)
    await cache.set("k", "v")
    assert await cache.get("k") == "v"
    await component.stop()
    await component.stop()  # stop() stays idempotent (Plan 050 §D-owner)
