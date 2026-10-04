"""
Regression guard — the exact examples/00-full-stack-post-api failure shape:
``RedisCacheConfiguration.redis_cache`` returns an already-started
``RedisCache``, and ``CoreModule``'s ``core.stores`` (INFRA) component used to
call ``start()`` on it again ("RedisCache.start() called on an already-started
cache"), so uvicorn never started.

No Docker: ``RedisCache.start()`` only builds a lazy ``redis.asyncio`` client
(``from_url`` opens no socket until the first command), and nothing below
issues a command.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_core.cache.base import CacheBackend
from varco_core.cache.layered import LayeredCache
from varco_core.di.module import CoreModule
from varco_core.settings import bind_settings
from varco_redis.cache import (
    LayeredCacheSettings,
    RedisCache,
    RedisCacheConfiguration,
    RedisCacheSettings,
    RedisLayeredCacheConfiguration,
)


def _core_stores(container: DIContainer):  # type: ignore[no-untyped-def]
    (component,) = [c for c in CoreModule().components(container) if c.name == "core.stores"]
    return component


async def test_core_stores_start_does_not_restart_the_provider_started_redis_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Select the backend exactly as the example's conftest does.
    monkeypatch.setenv("VARCO_CACHE_BACKEND", "redis")
    container = DIContainer()
    bind_settings(
        container, RedisCacheSettings, instance=RedisCacheSettings(url="redis://127.0.0.1:1/0")
    )
    await container.ainstall(RedisCacheConfiguration)
    component = _core_stores(container)

    await component.start()  # raised "already-started cache" before the fix

    cache = await container.aget(CacheBackend)
    assert isinstance(cache, RedisCache)
    await component.stop()
    await container.ashutdown()  # @Disposes backstop after stop(): idempotent


async def test_core_stores_start_does_not_restart_the_provider_started_layered_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Sibling provider with the same "start inside the provider" shape.
    # selected_by("cache", "layered"): without this the provider is never
    # chosen and the test would be vacuously green.
    monkeypatch.setenv("VARCO_CACHE_BACKEND", "layered")
    container = DIContainer()
    bind_settings(
        container, LayeredCacheSettings, instance=LayeredCacheSettings(url="redis://127.0.0.1:1/0")
    )
    await container.ainstall(RedisLayeredCacheConfiguration)
    component = _core_stores(container)

    await component.start()

    assert isinstance(await container.aget(CacheBackend), LayeredCache)
    await component.stop()
    await container.ashutdown()
