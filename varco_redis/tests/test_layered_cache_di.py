"""
Unit tests for varco_redis.cache.RedisLayeredCacheConfiguration
================================================================
Tests that the DI configuration correctly wires a ``LayeredCache`` (L1
``InMemoryCache`` + L2 ``RedisCache``) as the ``CacheBackend`` singleton.

All tests use ``FakeRedis`` (in-memory dict) to avoid a real Redis connection.

Sections
--------
- ``LayeredCacheSettings``          — defaults, env prefix, frozen
- ``RedisLayeredCacheConfiguration`` — DI wiring, singleton, L1/L2 types,
                                       write_mode, promote_ttl forwarding
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from varco_core.cache import CacheBackend, InMemoryCache
from varco_redis.cache import (
    LayeredCacheSettings,
    RedisCache,
    RedisLayeredCacheConfiguration,
)


# ── FakeRedis (reused from test_redis_cache.py conventions) ──────────────────


class FakeRedis:
    """Minimal in-memory Redis fake for DI wiring tests."""

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}

    async def get(self, key: str) -> bytes | None:
        return self._store.get(key)

    async def set(self, key: str, value: bytes) -> None:
        self._store[key] = value

    async def setex(self, key: str, ttl: int, value: bytes) -> None:
        self._store[key] = value

    async def delete(self, *keys: str) -> int:
        removed = sum(1 for k in keys if k in self._store)
        for k in keys:
            self._store.pop(k, None)
        return removed

    async def exists(self, key: str) -> int:
        return 1 if key in self._store else 0

    async def scan(self, cursor: int, match: str = "*", count: int = 100):
        prefix = match.rstrip("*")
        return 0, [k for k in self._store if k.startswith(prefix)]

    async def aclose(self) -> None:
        pass


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


# ── LayeredCacheSettings ─────────────────────────────────────────────────────


class TestLayeredCacheSettings:
    def test_defaults(self) -> None:
        # Default settings should point at a local Redis and use write-through.
        s = LayeredCacheSettings()
        assert "localhost" in s.url or "redis" in s.url
        assert s.write_mode == "write-through"
        assert s.l1_max_size is None
        assert s.l1_default_ttl is None
        assert s.promote_ttl is None

    def test_frozen(self) -> None:
        s = LayeredCacheSettings()
        with pytest.raises(Exception):
            s.write_mode = "write-around"  # type: ignore[misc]

    def test_custom_values(self) -> None:
        s = LayeredCacheSettings(
            url="redis://cache:6379/2",
            l1_max_size=500,
            l1_default_ttl=60.0,
            write_mode="write-around",
            promote_ttl=30.0,
        )
        assert s.url == "redis://cache:6379/2"
        assert s.l1_max_size == 500
        assert s.l1_default_ttl == 60.0
        assert s.write_mode == "write-around"
        assert s.promote_ttl == 30.0

    def test_env_prefix(self) -> None:
        # Confirm the configuration uses the right prefix (distinct from VARCO_REDIS_CACHE_)
        # so both configurations can coexist in the same container.

        assert hasattr(LayeredCacheSettings, "model_config")
        prefix = LayeredCacheSettings.model_config.get("env_prefix", "")
        assert "LAYERED" in prefix.upper()


# ── RedisLayeredCacheConfiguration DI wiring ─────────────────────────────────


class TestRedisLayeredCacheConfiguration:
    async def test_provides_cache_backend(self, fake_redis: FakeRedis) -> None:
        """
        Installing RedisLayeredCacheConfiguration should let us resolve CacheBackend.
        """
        from providify import DIContainer

        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis

            container = DIContainer()
            await container.ainstall(RedisLayeredCacheConfiguration)
            cache = await container.aget(CacheBackend)

            assert cache is not None

            await container.ashutdown()

    async def test_resolved_cache_is_layered_cache(self, fake_redis: FakeRedis) -> None:
        """The resolved CacheBackend must be a LayeredCache."""
        from providify import DIContainer
        from varco_core.cache import LayeredCache

        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis

            container = DIContainer()
            await container.ainstall(RedisLayeredCacheConfiguration)
            cache = await container.aget(CacheBackend)

            assert isinstance(cache, LayeredCache)

            await container.ashutdown()

    async def test_l1_is_in_memory_cache(self, fake_redis: FakeRedis) -> None:
        """First layer must be an InMemoryCache — the fast in-process L1."""
        from providify import DIContainer
        from varco_core.cache import LayeredCache

        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis

            container = DIContainer()
            await container.ainstall(RedisLayeredCacheConfiguration)
            cache = await container.aget(CacheBackend)

            assert isinstance(cache, LayeredCache)
            # LayeredCache exposes its layers — L1 is index 0.
            assert isinstance(cache._layers[0], InMemoryCache)

            await container.ashutdown()

    async def test_l2_is_redis_cache(self, fake_redis: FakeRedis) -> None:
        """Second layer must be a RedisCache — the durable L2."""
        from providify import DIContainer
        from varco_core.cache import LayeredCache

        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis

            container = DIContainer()
            await container.ainstall(RedisLayeredCacheConfiguration)
            cache = await container.aget(CacheBackend)

            assert isinstance(cache, LayeredCache)
            assert isinstance(cache._layers[1], RedisCache)

            await container.ashutdown()

    async def test_singleton_returns_same_instance(self, fake_redis: FakeRedis) -> None:
        """Repeated resolves of CacheBackend return the same started instance."""
        from providify import DIContainer

        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis

            container = DIContainer()
            await container.ainstall(RedisLayeredCacheConfiguration)

            c1 = await container.aget(CacheBackend)
            c2 = await container.aget(CacheBackend)
            assert c1 is c2

            await container.ashutdown()

    async def test_write_mode_forwarded(self, fake_redis: FakeRedis) -> None:
        """The write_mode from settings is applied to the LayeredCache."""
        from providify import DIContainer, Provider
        from varco_core.cache import LayeredCache

        # Override settings to use write-around mode.
        custom_settings = LayeredCacheSettings(write_mode="write-around")

        # providify.provide() requires a @Provider-decorated function whose
        # return annotation determines the resolved interface.
        @Provider(singleton=True)
        def _settings() -> LayeredCacheSettings:
            return custom_settings

        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis

            container = DIContainer()
            container.provide(_settings)
            await container.ainstall(RedisLayeredCacheConfiguration)

            cache = await container.aget(CacheBackend)
            assert isinstance(cache, LayeredCache)
            # LayeredCache stores the write mode — verify it was forwarded.
            assert cache._write_mode == "write-around"

            await container.ashutdown()

    async def test_l1_max_size_forwarded(self, fake_redis: FakeRedis) -> None:
        """l1_max_size from settings is applied to the InMemoryCache layer."""
        from providify import DIContainer, Provider
        from varco_core.cache import LayeredCache

        custom_settings = LayeredCacheSettings(l1_max_size=42)

        @Provider(singleton=True)
        def _settings() -> LayeredCacheSettings:
            return custom_settings

        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis

            container = DIContainer()
            container.provide(_settings)
            await container.ainstall(RedisLayeredCacheConfiguration)

            cache = await container.aget(CacheBackend)
            assert isinstance(cache, LayeredCache)
            l1 = cache._layers[0]
            assert isinstance(l1, InMemoryCache)
            # InMemoryCache exposes max_size — verify it was set correctly.
            assert l1._max_size == 42

            await container.ashutdown()

    async def test_cache_started_before_return(self, fake_redis: FakeRedis) -> None:
        """The LayeredCache must be started (start() called) before the provider returns it."""
        from providify import DIContainer
        from varco_core.cache import LayeredCache

        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis

            container = DIContainer()
            await container.ainstall(RedisLayeredCacheConfiguration)

            cache = await container.aget(CacheBackend)
            assert isinstance(cache, LayeredCache)
            # A started cache allows operations; an unstarted cache raises.
            # Attempt a set — this will raise if start() was not called.
            await cache.set("probe_key", "probe_value")
            result = await cache.get("probe_key")
            assert result == "probe_value"

            await container.ashutdown()
