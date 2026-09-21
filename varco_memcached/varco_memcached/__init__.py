"""
varco_memcached
===============
Memcached-backed ``CacheBackend`` implementation for the varco cache system.

Usage::

    from varco_memcached.cache import (
        MemcachedCache,
        MemcachedCacheSettings,
        MemcachedCacheConfiguration,
    )

    async with MemcachedCache() as cache:
        await cache.set("user:42", user_obj, ttl=300)
        result = await cache.get("user:42")

DI wiring (recommended — one call)::

    from providify import DIContainer
    from varco_memcached.di import async_bootstrap
    from varco_core.cache import CacheBackend

    container = DIContainer()
    await async_bootstrap(container)
    cache = await container.aget(CacheBackend)

DI wiring (manual)::

    from providify import DIContainer
    from varco_memcached.cache import MemcachedCacheConfiguration

    container = DIContainer()
    await container.ainstall(MemcachedCacheConfiguration)
    cache = await container.aget(CacheBackend)

📚 Docs
- 🔍 https://github.com/aio-libs/aiomcache  — aiomcache asyncio Memcached client
"""

from varco_memcached.cache import (
    MemcachedCache,
    MemcachedCacheConfiguration,
    MemcachedCacheSettings,
)
from varco_memcached.di import async_bootstrap, bootstrap
from varco_memcached.health import MemcachedHealthCheck

__all__ = [
    "MemcachedCacheSettings",
    "MemcachedCache",
    "MemcachedCacheConfiguration",
    "MemcachedHealthCheck",
    "bootstrap",
    "async_bootstrap",
]
