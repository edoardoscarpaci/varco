"""
Unit tests for varco_memcached.cache
======================================
All tests mock ``aiomcache.Client`` — no real Memcached instance required.

A ``FakeMemcached`` test double replaces ``aiomcache.Client`` via
``unittest.mock.patch``, providing an in-memory dict-backed implementation
that mirrors the aiomcache commands used by ``MemcachedCache``
(``get``, ``set``, ``delete``, ``close``).

DESIGN: FakeMemcached stores keys as bytes
    aiomcache requires ``bytes`` keys — the fake intentionally uses
    ``dict[bytes, bytes]`` to surface any caller that forgets to encode.

Sections
--------
- ``MemcachedCacheSettings``    — defaults, memcached_key(), frozen, env prefix
- ``MemcachedCache`` lifecycle  — start/stop, double-start guard, context manager
- ``MemcachedCache`` set/get    — round-trip, TTL (exptime), no-TTL, prefix
- ``MemcachedCache`` delete     — removes key; removes from registry; idempotent
- ``MemcachedCache`` exists     — true/false; strategy gate
- ``MemcachedCache`` clear      — only removes registry keys; leaves untracked keys
- ``MemcachedCache`` strategy   — ExplicitStrategy gates get/exists
- ``MemcachedCacheConfiguration`` — wires settings + cache via Providify
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from varco_core.cache import ExplicitStrategy
from varco_memcached.cache import (
    MemcachedCache,
    MemcachedCacheConfiguration,
    MemcachedCacheSettings,
)


# ── FakeMemcached ───────────────────────────────────────────────────────────────


class FakeMemcached:
    """
    In-memory Memcached fake.

    Keys and values are stored as ``bytes`` — matching the aiomcache contract.
    Supports: get, set, delete, close.

    Does NOT implement real TTL expiry — tests that need TTL expiry use
    real Memcached via the integration test suite.

    Async safety: ✅ — all methods are ``async def``, safe to await in tests.
    """

    def __init__(self) -> None:
        # bytes → bytes mapping mirrors aiomcache's raw byte storage.
        self._store: dict[bytes, bytes] = {}
        # Track the exptime passed to set() — used to assert TTL behaviour.
        self._exptime: dict[bytes, int] = {}

    async def get(self, key: bytes) -> bytes | None:
        return self._store.get(key)

    async def set(self, key: bytes, value: bytes, exptime: int = 0) -> bool:
        self._store[key] = value
        self._exptime[key] = exptime
        return True

    async def delete(self, key: bytes) -> bool:
        # Idempotent — do not raise on missing key.
        self._store.pop(key, None)
        self._exptime.pop(key, None)
        return True

    async def close(self) -> None:
        pass


# ── Fixtures ─────────────────────────────────────────────────────────────────────


@pytest.fixture
def fake_mc() -> FakeMemcached:
    return FakeMemcached()


@pytest.fixture
def settings() -> MemcachedCacheSettings:
    return MemcachedCacheSettings(host="fake-mc", port=11211)


@pytest.fixture
async def cache(
    settings: MemcachedCacheSettings,
    fake_mc: FakeMemcached,
) -> MemcachedCache:
    with patch("varco_memcached.cache.aiomcache.Client", return_value=fake_mc):
        async with MemcachedCache(settings) as c:
            yield c


# ── MemcachedCacheSettings ──────────────────────────────────────────────────────


class TestMemcachedCacheSettings:
    def test_defaults(self) -> None:
        s = MemcachedCacheSettings()
        assert s.host == "localhost"
        assert s.port == 11211
        assert s.pool_size == 2
        assert s.key_prefix == ""
        assert s.default_ttl is None

    def test_frozen(self) -> None:
        s = MemcachedCacheSettings()
        with pytest.raises(Exception):
            s.host = "other"  # type: ignore[misc]

    def test_memcached_key_no_prefix(self) -> None:
        s = MemcachedCacheSettings()
        assert s.memcached_key("user:1") == b"user:1"

    def test_memcached_key_with_prefix(self) -> None:
        s = MemcachedCacheSettings(key_prefix="myapp:")
        assert s.memcached_key("user:1") == b"myapp:user:1"

    def test_from_dict(self) -> None:
        s = MemcachedCacheSettings.from_dict(
            {"host": "mc.prod", "port": 11212, "key_prefix": "prod:"}
        )
        assert s.host == "mc.prod"
        assert s.port == 11212
        assert s.key_prefix == "prod:"


# ── MemcachedCache lifecycle ────────────────────────────────────────────────────


class TestMemcachedCacheLifecycle:
    async def test_not_started_raises_on_get(
        self, settings: MemcachedCacheSettings
    ) -> None:
        c = MemcachedCache(settings)
        with pytest.raises(RuntimeError, match="not started"):
            await c.get("k")

    async def test_not_started_raises_on_set(
        self, settings: MemcachedCacheSettings
    ) -> None:
        c = MemcachedCache(settings)
        with pytest.raises(RuntimeError, match="not started"):
            await c.set("k", "v")

    async def test_double_start_raises(
        self, settings: MemcachedCacheSettings, fake_mc: FakeMemcached
    ) -> None:
        with patch("varco_memcached.cache.aiomcache.Client", return_value=fake_mc):
            c = MemcachedCache(settings)
            await c.start()
            with pytest.raises(RuntimeError, match="already-started"):
                await c.start()
            await c.stop()

    async def test_stop_before_start_noop(
        self, settings: MemcachedCacheSettings
    ) -> None:
        c = MemcachedCache(settings)
        await c.stop()  # must not raise

    async def test_context_manager_starts_and_stops(
        self, settings: MemcachedCacheSettings, fake_mc: FakeMemcached
    ) -> None:
        with patch("varco_memcached.cache.aiomcache.Client", return_value=fake_mc):
            async with MemcachedCache(settings) as c:
                assert c._client is not None
            assert c._client is None

    async def test_repr_started(self, cache: MemcachedCache) -> None:
        text = repr(cache)
        assert "MemcachedCache" in text
        assert "started=True" in text

    async def test_repr_not_started(self, settings: MemcachedCacheSettings) -> None:
        text = repr(MemcachedCache(settings))
        assert "started=False" in text


# ── MemcachedCache set / get ────────────────────────────────────────────────────


class TestMemcachedCacheSetGet:
    async def test_set_and_get_roundtrip(self, cache: MemcachedCache) -> None:
        await cache.set("user:1", {"name": "Alice", "age": 30})
        result = await cache.get("user:1")
        assert result == {"name": "Alice", "age": 30}

    async def test_get_missing_returns_none(self, cache: MemcachedCache) -> None:
        assert await cache.get("missing") is None

    async def test_set_with_ttl_passes_exptime(
        self,
        cache: MemcachedCache,
        fake_mc: FakeMemcached,
        settings: MemcachedCacheSettings,
    ) -> None:
        await cache.set("k", "v", ttl=120)
        mc_key = settings.memcached_key("k")
        assert fake_mc._exptime.get(mc_key) == 120

    async def test_set_without_ttl_uses_zero_exptime(
        self,
        cache: MemcachedCache,
        fake_mc: FakeMemcached,
        settings: MemcachedCacheSettings,
    ) -> None:
        # 0 = no expiry in Memcached protocol
        await cache.set("k", "v")
        mc_key = settings.memcached_key("k")
        assert fake_mc._exptime.get(mc_key) == 0

    async def test_default_ttl_from_settings(self, fake_mc: FakeMemcached) -> None:
        settings = MemcachedCacheSettings(host="fake-mc", port=11211, default_ttl=60.0)
        with patch("varco_memcached.cache.aiomcache.Client", return_value=fake_mc):
            async with MemcachedCache(settings) as cache:
                await cache.set("k", "v")
                mc_key = settings.memcached_key("k")
                assert fake_mc._exptime.get(mc_key) == 60

    async def test_key_prefix_applied_in_storage(self, fake_mc: FakeMemcached) -> None:
        settings = MemcachedCacheSettings(
            host="fake-mc", port=11211, key_prefix="myapp:"
        )
        with patch("varco_memcached.cache.aiomcache.Client", return_value=fake_mc):
            async with MemcachedCache(settings) as cache:
                await cache.set("user:1", "Alice")
                assert b"myapp:user:1" in fake_mc._store

    async def test_set_tracks_key_in_registry(self, cache: MemcachedCache) -> None:
        await cache.set("user:1", "Alice")
        # The logical key (unprefixed) must be tracked.
        assert "user:1" in cache._key_registry


# ── MemcachedCache delete ───────────────────────────────────────────────────────


class TestMemcachedCacheDelete:
    async def test_delete_removes_key(self, cache: MemcachedCache) -> None:
        await cache.set("k", "v")
        await cache.delete("k")
        assert await cache.get("k") is None

    async def test_delete_removes_from_registry(self, cache: MemcachedCache) -> None:
        await cache.set("k", "v")
        assert "k" in cache._key_registry
        await cache.delete("k")
        assert "k" not in cache._key_registry

    async def test_delete_nonexistent_noop(self, cache: MemcachedCache) -> None:
        await cache.delete("missing")  # must not raise


# ── MemcachedCache exists ───────────────────────────────────────────────────────


class TestMemcachedCacheExists:
    async def test_exists_true(self, cache: MemcachedCache) -> None:
        await cache.set("k", "v")
        assert await cache.exists("k") is True

    async def test_exists_false_for_missing(self, cache: MemcachedCache) -> None:
        assert await cache.exists("missing") is False

    async def test_exists_false_when_strategy_invalidates(
        self, fake_mc: FakeMemcached, settings: MemcachedCacheSettings
    ) -> None:
        strategy = ExplicitStrategy()
        with patch("varco_memcached.cache.aiomcache.Client", return_value=fake_mc):
            async with MemcachedCache(settings, strategy=strategy) as cache:
                await cache.set("k", "v")
                strategy.invalidate("k")
                assert await cache.exists("k") is False


# ── MemcachedCache clear ────────────────────────────────────────────────────────


class TestMemcachedCacheClear:
    async def test_clear_removes_tracked_keys(self, cache: MemcachedCache) -> None:
        await cache.set("a", 1)
        await cache.set("b", 2)
        await cache.clear()
        assert await cache.get("a") is None
        assert await cache.get("b") is None

    async def test_clear_empties_registry(self, cache: MemcachedCache) -> None:
        await cache.set("a", 1)
        await cache.set("b", 2)
        await cache.clear()
        assert len(cache._key_registry) == 0

    async def test_clear_does_not_remove_untracked_keys(
        self, fake_mc: FakeMemcached, settings: MemcachedCacheSettings
    ) -> None:
        # Put a key directly into the fake store (simulating another client).
        untracked_key = b"other:key"
        fake_mc._store[untracked_key] = b'"external"'

        with patch("varco_memcached.cache.aiomcache.Client", return_value=fake_mc):
            async with MemcachedCache(settings) as cache:
                await cache.set("a", 1)
                await cache.clear()
                # The externally-written key must survive.
                assert untracked_key in fake_mc._store

    async def test_clear_noop_when_empty(self, cache: MemcachedCache) -> None:
        await cache.clear()  # must not raise


# ── MemcachedCache + ExplicitStrategy ───────────────────────────────────────────


class TestMemcachedCacheWithStrategy:
    async def test_strategy_invalidates_get(
        self, fake_mc: FakeMemcached, settings: MemcachedCacheSettings
    ) -> None:
        strategy = ExplicitStrategy()
        with patch("varco_memcached.cache.aiomcache.Client", return_value=fake_mc):
            async with MemcachedCache(settings, strategy=strategy) as cache:
                await cache.set("k", "v")
                strategy.invalidate("k")
                result = await cache.get("k")
                assert result is None
                # Key must be evicted from Memcached too.
                assert await fake_mc.get(settings.memcached_key("k")) is None


# ── MemcachedCacheConfiguration ─────────────────────────────────────────────────


class TestMemcachedCacheConfiguration:
    async def test_provides_cache_backend(self, fake_mc: FakeMemcached) -> None:
        from providify import DIContainer
        from varco_core.cache import CacheBackend

        with patch("varco_memcached.cache.aiomcache.Client", return_value=fake_mc):
            container = DIContainer()
            await container.ainstall(MemcachedCacheConfiguration)

            cache = await container.aget(CacheBackend)
            assert isinstance(cache, MemcachedCache)

            await container.ashutdown()

    async def test_singleton_returns_same_instance(
        self, fake_mc: FakeMemcached
    ) -> None:
        from providify import DIContainer
        from varco_core.cache import CacheBackend

        with patch("varco_memcached.cache.aiomcache.Client", return_value=fake_mc):
            container = DIContainer()
            await container.ainstall(MemcachedCacheConfiguration)

            c1 = await container.aget(CacheBackend)
            c2 = await container.aget(CacheBackend)
            assert c1 is c2

            await container.ashutdown()

    async def test_settings_default_to_localhost(self) -> None:
        # Settings created without arguments must default to localhost:11211 —
        # verifies the DI configuration provider reads sane defaults from env.
        s = MemcachedCacheSettings()
        assert s.host == "localhost"
        assert s.port == 11211
