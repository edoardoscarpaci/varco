"""
Unit tests for varco_redis.cache
==================================
All tests mock ``redis.asyncio`` — no real Redis instance required.

A ``FakeRedis`` test double replaces ``aioredis.from_url()`` via
``unittest.mock.patch``, providing an in-memory dict-backed implementation
that mirrors the Redis commands used by ``RedisCache`` (``get``, ``set``,
``setex``, ``delete``, ``exists``, ``scan``).

Sections
--------
- ``RedisCacheSettings``   — defaults, redis_key(), frozen, env prefix
- ``RedisCache`` lifecycle — start/stop, double-start guard, context manager
- ``RedisCache`` set/get   — round-trip, TTL (setex), no-TTL (set), prefix
- ``RedisCache`` delete    — removes key; idempotent
- ``RedisCache`` exists    — true/false; strategy gate
- ``RedisCache`` clear     — SCAN + DEL pattern matching key_prefix
- ``RedisCache`` strategy  — ExplicitStrategy gates get/exists
- ``RedisCacheConfiguration`` — wires settings + cache via Providify
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from varco_core.cache import ExplicitStrategy
from varco_redis.cache import RedisCache, RedisCacheConfiguration, RedisCacheSettings


# ── FakeRedis ───────────────────────────────────────────────────────────────────


class FakeRedis:
    """
    In-memory Redis fake.

    Supports: get, set, setex, delete, exists, scan, aclose.
    Does NOT implement real TTL expiry — tests that need TTL expiry use
    real Redis via the integration test suite.
    """

    def __init__(self) -> None:
        self._store: dict[str, bytes] = {}
        self._ttls: dict[str, int] = {}

    async def get(self, key: str) -> bytes | None:
        return self._store.get(key)

    async def set(self, key: str, value: bytes) -> None:
        self._store[key] = value

    async def setex(self, key: str, ttl: int, value: bytes) -> None:
        self._store[key] = value
        self._ttls[key] = ttl

    async def delete(self, *keys: str) -> int:
        count = 0
        for key in keys:
            if key in self._store:
                del self._store[key]
                count += 1
        return count

    async def exists(self, key: str) -> int:
        return 1 if key in self._store else 0

    async def scan(
        self,
        cursor: int,
        match: str = "*",
        count: int = 100,
    ) -> tuple[int, list[str]]:
        """Simple non-paginating scan — returns all matching keys at once."""
        pattern = match.rstrip("*")
        matching = [k for k in self._store if k.startswith(pattern)]
        return 0, matching  # cursor=0 signals end of scan

    async def aclose(self) -> None:
        pass


# ── Fixtures ────────────────────────────────────────────────────────────────────


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def settings() -> RedisCacheSettings:
    return RedisCacheSettings(url="redis://fake:6379/0")


@pytest.fixture
async def cache(settings: RedisCacheSettings, fake_redis: FakeRedis) -> RedisCache:
    with patch("varco_redis.cache.aioredis") as mock_aioredis:
        mock_aioredis.from_url.return_value = fake_redis
        async with RedisCache(settings) as c:
            yield c


# ── RedisCacheSettings ──────────────────────────────────────────────────────────


class TestRedisCacheSettings:
    def test_defaults(self) -> None:
        s = RedisCacheSettings()
        assert s.url == "redis://localhost:6379/0"
        assert s.key_prefix == ""
        assert s.decode_responses is False
        assert s.default_ttl is None

    def test_frozen(self) -> None:
        s = RedisCacheSettings()
        with pytest.raises(Exception):
            s.url = "redis://other"  # type: ignore[misc]

    def test_redis_key_no_prefix(self) -> None:
        s = RedisCacheSettings()
        assert s.redis_key("user:1") == "user:1"

    def test_redis_key_with_prefix(self) -> None:
        s = RedisCacheSettings(key_prefix="myapp:")
        assert s.redis_key("user:1") == "myapp:user:1"

    def test_from_dict(self) -> None:
        s = RedisCacheSettings.from_dict(
            {"url": "redis://other:6379/1", "key_prefix": "prod:"}
        )
        assert s.url == "redis://other:6379/1"
        assert s.key_prefix == "prod:"


# ── RedisCache lifecycle ─────────────────────────────────────────────────────────


class TestRedisCacheLifecycle:
    async def test_not_started_raises_on_get(
        self, settings: RedisCacheSettings
    ) -> None:
        c = RedisCache(settings)
        with pytest.raises(RuntimeError, match="not started"):
            await c.get("k")

    async def test_not_started_raises_on_set(
        self, settings: RedisCacheSettings
    ) -> None:
        c = RedisCache(settings)
        with pytest.raises(RuntimeError, match="not started"):
            await c.set("k", "v")

    async def test_double_start_raises(
        self, settings: RedisCacheSettings, fake_redis: FakeRedis
    ) -> None:
        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            c = RedisCache(settings)
            await c.start()
            with pytest.raises(RuntimeError, match="already-started"):
                await c.start()
            await c.stop()

    async def test_stop_before_start_noop(self, settings: RedisCacheSettings) -> None:
        c = RedisCache(settings)
        await c.stop()  # must not raise

    async def test_context_manager(
        self, settings: RedisCacheSettings, fake_redis: FakeRedis
    ) -> None:
        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisCache(settings) as c:
                assert c._redis is not None
            assert c._redis is None

    async def test_repr(self, cache: RedisCache) -> None:
        assert "RedisCache" in repr(cache)
        assert "started=True" in repr(cache)


# ── RedisCache set / get ─────────────────────────────────────────────────────────


class TestRedisCacheSetGet:
    async def test_set_and_get_roundtrip(
        self, cache: RedisCache, fake_redis: FakeRedis
    ) -> None:
        await cache.set("user:1", {"name": "Alice", "age": 30})
        result = await cache.get("user:1")
        assert result == {"name": "Alice", "age": 30}

    async def test_get_missing_returns_none(self, cache: RedisCache) -> None:
        assert await cache.get("missing") is None

    async def test_set_with_ttl_uses_setex(
        self, cache: RedisCache, fake_redis: FakeRedis
    ) -> None:
        await cache.set("k", "v", ttl=120)
        assert "k" in fake_redis._ttls
        assert fake_redis._ttls["k"] == 120

    async def test_set_without_ttl_uses_set_not_setex(
        self, cache: RedisCache, fake_redis: FakeRedis
    ) -> None:
        await cache.set("k", "v")
        assert "k" not in fake_redis._ttls

    async def test_key_prefix_applied_in_storage(self, fake_redis: FakeRedis) -> None:
        settings = RedisCacheSettings(url="redis://fake:6379/0", key_prefix="myapp:")
        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisCache(settings) as cache:
                await cache.set("user:1", "Alice")
                assert "myapp:user:1" in fake_redis._store

    async def test_default_ttl_from_settings(self, fake_redis: FakeRedis) -> None:
        settings = RedisCacheSettings(url="redis://fake:6379/0", default_ttl=60.0)
        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisCache(settings) as cache:
                await cache.set("k", "v")
                assert fake_redis._ttls.get("k") == 60


# ── RedisCache delete ────────────────────────────────────────────────────────────


class TestRedisCacheDelete:
    async def test_delete_removes_key(
        self, cache: RedisCache, fake_redis: FakeRedis
    ) -> None:
        await cache.set("k", "v")
        await cache.delete("k")
        assert await cache.get("k") is None

    async def test_delete_nonexistent_noop(self, cache: RedisCache) -> None:
        await cache.delete("missing")  # must not raise


# ── RedisCache exists ────────────────────────────────────────────────────────────


class TestRedisCacheExists:
    async def test_exists_true(self, cache: RedisCache) -> None:
        await cache.set("k", "v")
        assert await cache.exists("k") is True

    async def test_exists_false_for_missing(self, cache: RedisCache) -> None:
        assert await cache.exists("missing") is False

    async def test_exists_false_when_strategy_invalidates(
        self, fake_redis: FakeRedis, settings: RedisCacheSettings
    ) -> None:
        strategy = ExplicitStrategy()
        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisCache(settings, strategy=strategy) as cache:
                await cache.set("k", "v")
                strategy.invalidate("k")
                assert await cache.exists("k") is False


# ── RedisCache clear ─────────────────────────────────────────────────────────────


class TestRedisCacheClear:
    async def test_clear_removes_all_prefixed_keys(self, fake_redis: FakeRedis) -> None:
        settings = RedisCacheSettings(url="redis://fake:6379/0", key_prefix="ns:")
        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisCache(settings) as cache:
                await cache.set("a", 1)
                await cache.set("b", 2)
                await cache.clear()
                assert await cache.get("a") is None
                assert await cache.get("b") is None

    async def test_clear_does_not_remove_unrelated_keys(
        self, fake_redis: FakeRedis
    ) -> None:
        settings = RedisCacheSettings(url="redis://fake:6379/0", key_prefix="ns:")
        # Manually put an unrelated key directly in the fake store
        fake_redis._store["other:key"] = b'"unrelated"'
        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisCache(settings) as cache:
                await cache.set("a", 1)
                await cache.clear()
                # "other:key" must survive (different prefix)
                assert "other:key" in fake_redis._store


# ── RedisCache + ExplicitStrategy ────────────────────────────────────────────────


class TestRedisCacheWithStrategy:
    async def test_strategy_invalidates_get(
        self, fake_redis: FakeRedis, settings: RedisCacheSettings
    ) -> None:
        strategy = ExplicitStrategy()
        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisCache(settings, strategy=strategy) as cache:
                await cache.set("k", "v")
                strategy.invalidate("k")
                result = await cache.get("k")
                assert result is None
                # Key must be deleted from Redis too
                assert await fake_redis.get("k") is None


# ── RedisCacheConfiguration ──────────────────────────────────────────────────────


class TestRedisCacheConfiguration:
    async def test_provides_cache_backend(self, fake_redis: FakeRedis) -> None:
        from providify import DIContainer
        from varco_core.cache import CacheBackend

        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis

            container = DIContainer()
            await container.ainstall(RedisCacheConfiguration)

            cache = await container.aget(CacheBackend)
            assert isinstance(cache, RedisCache)

            await container.ashutdown()

    async def test_singleton_returns_same_instance(self, fake_redis: FakeRedis) -> None:
        from providify import DIContainer
        from varco_core.cache import CacheBackend

        with patch("varco_redis.cache.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis

            container = DIContainer()
            await container.ainstall(RedisCacheConfiguration)

            c1 = await container.aget(CacheBackend)
            c2 = await container.aget(CacheBackend)
            assert c1 is c2

            await container.ashutdown()
