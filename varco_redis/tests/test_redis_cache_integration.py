"""
Integration tests for varco_redis.cache
==========================================
Spin up a real Redis instance via testcontainers and exercise the full
``RedisCache`` + ``LayeredCache`` + ``CachedService`` stack.

DISABLED BY DEFAULT — requires Docker.  Run with::

    pytest -m integration tests/test_redis_cache_integration.py

Or set the ``VARCO_RUN_INTEGRATION`` env var::

    VARCO_RUN_INTEGRATION=1 pytest tests/test_redis_cache_integration.py

Test sections
-------------
1. ``RedisCache`` basics       — get/set/delete/exists/clear with real Redis
2. TTL enforcement             — Redis natively expires keys; verified via sleep
3. ``LayeredCache`` (L1+Redis) — promote, write-through, delete propagation
4. ``CachedService``           — get/list/exists with explicit invalidation
5. ``CachedService`` + producer — event-driven invalidation via producer/consumer
"""

from __future__ import annotations

import asyncio
import os

import pytest

from varco_core.cache import (
    CachedService,
    CacheInvalidationConsumer,
    CacheInvalidationEvent,
    ExplicitStrategy,
    InMemoryCache,
    LayeredCache,
    TTLStrategy,
)
from varco_core.event import BusEventProducer, InMemoryEventBus
from varco_redis.cache import RedisCache, RedisCacheSettings

pytestmark = pytest.mark.integration

if not os.environ.get("VARCO_RUN_INTEGRATION"):
    pytest.skip(
        "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration",
        allow_module_level=True,
    )


# ── Redis container fixture ─────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def redis_container():
    from testcontainers.redis import RedisContainer  # noqa: PLC0415

    with RedisContainer() as r:
        yield r


@pytest.fixture
def redis_url(redis_container) -> str:
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}/0"


# ── Helper: tiny fake service ───────────────────────────────────────────────────


class _User:
    """Lightweight stand-in for a domain entity."""

    def __init__(self, id: int, name: str) -> None:
        self.id = id
        self.name = name

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, _User) and self.id == other.id and self.name == other.name
        )

    def __repr__(self) -> str:
        return f"_User(id={self.id}, name={self.name!r})"


class _UserService:
    """Minimal in-memory service used as the ``CachedService`` backing store."""

    def __init__(self) -> None:
        self._store: dict[int, _User] = {}
        self.get_calls = 0
        self.list_calls = 0
        self.exists_calls = 0

    async def get(self, entity_id: int) -> _User | None:
        self.get_calls += 1
        return self._store.get(entity_id)

    async def list(self) -> list[_User]:
        self.list_calls += 1
        return list(self._store.values())

    async def exists(self, entity_id: int) -> bool:
        self.exists_calls += 1
        return entity_id in self._store

    async def create(self, data: dict) -> _User:
        user = _User(id=data["id"], name=data["name"])
        self._store[user.id] = user
        return user

    async def update(self, entity_id: int, data: dict) -> _User:
        user = self._store[entity_id]
        self._store[entity_id] = _User(id=user.id, name=data.get("name", user.name))
        return self._store[entity_id]

    async def delete(self, entity_id: int) -> None:
        self._store.pop(entity_id, None)


# ── 1 — RedisCache basics ───────────────────────────────────────────────────────


class TestRedisCacheBasics:
    async def test_set_and_get(self, redis_url: str) -> None:
        settings = RedisCacheSettings(url=redis_url, key_prefix="test:basics:")
        async with RedisCache(settings) as cache:
            await cache.set("user:1", {"name": "Alice"})
            result = await cache.get("user:1")
            assert result == {"name": "Alice"}

    async def test_get_missing_returns_none(self, redis_url: str) -> None:
        settings = RedisCacheSettings(url=redis_url, key_prefix="test:basics:")
        async with RedisCache(settings) as cache:
            assert await cache.get("nonexistent") is None

    async def test_delete(self, redis_url: str) -> None:
        settings = RedisCacheSettings(url=redis_url, key_prefix="test:delete:")
        async with RedisCache(settings) as cache:
            await cache.set("k", "v")
            await cache.delete("k")
            assert await cache.get("k") is None

    async def test_exists(self, redis_url: str) -> None:
        settings = RedisCacheSettings(url=redis_url, key_prefix="test:exists:")
        async with RedisCache(settings) as cache:
            assert await cache.exists("k") is False
            await cache.set("k", "v")
            assert await cache.exists("k") is True

    async def test_clear(self, redis_url: str) -> None:
        settings = RedisCacheSettings(url=redis_url, key_prefix="test:clear:")
        async with RedisCache(settings) as cache:
            await cache.set("a", 1)
            await cache.set("b", 2)
            await cache.clear()
            assert await cache.get("a") is None
            assert await cache.get("b") is None

    async def test_overwrite_value(self, redis_url: str) -> None:
        settings = RedisCacheSettings(url=redis_url, key_prefix="test:overwrite:")
        async with RedisCache(settings) as cache:
            await cache.set("k", "first")
            await cache.set("k", "second")
            assert await cache.get("k") == "second"


# ── 2 — TTL enforcement (real Redis native expiry) ──────────────────────────────


class TestRedisCacheTTL:
    async def test_key_expires_after_ttl(self, redis_url: str) -> None:
        settings = RedisCacheSettings(url=redis_url, key_prefix="test:ttl:")
        async with RedisCache(settings) as cache:
            await cache.set("expiring", "value", ttl=1)  # 1-second TTL
            assert await cache.get("expiring") == "value"
            await asyncio.sleep(1.5)
            assert await cache.get("expiring") is None

    async def test_key_without_ttl_persists(self, redis_url: str) -> None:
        settings = RedisCacheSettings(url=redis_url, key_prefix="test:nttl:")
        async with RedisCache(settings) as cache:
            await cache.set("persistent", "value")
            await asyncio.sleep(0.5)
            assert await cache.get("persistent") == "value"
            await cache.delete("persistent")  # cleanup

    async def test_default_ttl_from_settings(self, redis_url: str) -> None:
        settings = RedisCacheSettings(
            url=redis_url, key_prefix="test:dttl:", default_ttl=1.0
        )
        async with RedisCache(settings) as cache:
            await cache.set("k", "v")
            assert await cache.get("k") == "v"
            await asyncio.sleep(1.5)
            assert await cache.get("k") is None


# ── 3 — LayeredCache (L1 InMemoryCache + L2 RedisCache) ─────────────────────────


class TestLayeredCacheIntegration:
    async def test_write_through_populates_both_layers(self, redis_url: str) -> None:
        l1 = InMemoryCache()
        l2 = RedisCache(
            RedisCacheSettings(url=redis_url, key_prefix="test:layered:wt:")
        )
        async with LayeredCache(l1, l2) as cache:
            await cache.set("k", "v")
            assert await l1.get("k") == "v"
            assert await l2.get("k") == "v"
            await l2.clear()

    async def test_l2_hit_promotes_to_l1(self, redis_url: str) -> None:
        l1 = InMemoryCache()
        l2 = RedisCache(
            RedisCacheSettings(url=redis_url, key_prefix="test:layered:promo:")
        )
        async with LayeredCache(l1, l2) as cache:
            # Write only to L2
            await l2.set("k", "from-redis")
            # Read through layered cache → L1 miss → L2 hit → promotes to L1
            result = await cache.get("k")
            assert result == "from-redis"
            # Now L1 has it
            assert await l1.get("k") == "from-redis"
            await l2.clear()

    async def test_l1_hit_does_not_go_to_l2(self, redis_url: str) -> None:
        """L2 value differs — layered cache must return L1 value."""
        l1 = InMemoryCache()
        l2 = RedisCache(
            RedisCacheSettings(url=redis_url, key_prefix="test:layered:l1hit:")
        )
        async with LayeredCache(l1, l2) as cache:
            await cache.set("k", "shared")
            # Modify L2 directly — simulates stale L2
            await l2.set("k", "stale-l2-value")
            # L1 hit — should return L1 value ("shared")
            assert await cache.get("k") == "shared"
            await l2.clear()

    async def test_delete_propagates_to_both_layers(self, redis_url: str) -> None:
        l1 = InMemoryCache()
        l2 = RedisCache(
            RedisCacheSettings(url=redis_url, key_prefix="test:layered:del:")
        )
        async with LayeredCache(l1, l2) as cache:
            await cache.set("k", "v")
            await cache.delete("k")
            assert await l1.get("k") is None
            assert await l2.get("k") is None

    async def test_l1_ttl_stale_window(self, redis_url: str) -> None:
        """L1 expires (stale window), L2 still has authoritative value."""
        l1 = InMemoryCache(strategy=TTLStrategy(default_ttl=0.3))  # 300ms stale window
        l2 = RedisCache(
            RedisCacheSettings(url=redis_url, key_prefix="test:layered:stale:")
        )
        async with LayeredCache(l1, l2, promote_ttl=0.3) as cache:
            await cache.set("k", "v")
            # Immediately available
            assert await cache.get("k") == "v"
            # Wait for L1 TTL to expire
            await asyncio.sleep(0.5)
            # L1 expired → falls through to L2 → re-promotes to L1
            assert await cache.get("k") == "v"
            await l2.clear()


# ── 4 — CachedService with explicit invalidation ────────────────────────────────


class TestCachedServiceWithRedis:
    async def test_get_cache_miss_then_hit(self, redis_url: str) -> None:
        svc = _UserService()
        await svc.create({"id": 1, "name": "Alice"})

        settings = RedisCacheSettings(url=redis_url, key_prefix="test:cs:get:")
        async with RedisCache(settings) as cache:
            cached_svc = CachedService(svc, cache, namespace="user")

            # First call: cache miss → hits service
            user = await cached_svc.get(1)
            assert user.name == "Alice"
            assert svc.get_calls == 1

            # Second call: cache hit → does NOT call service
            user2 = await cached_svc.get(1)
            assert user2.name == "Alice"
            assert svc.get_calls == 1  # still 1

            await cache.clear()

    async def test_list_cache_miss_then_hit(self, redis_url: str) -> None:
        svc = _UserService()
        await svc.create({"id": 1, "name": "Alice"})
        await svc.create({"id": 2, "name": "Bob"})

        settings = RedisCacheSettings(url=redis_url, key_prefix="test:cs:list:")
        async with RedisCache(settings) as cache:
            cached_svc = CachedService(svc, cache, namespace="user")

            users = await cached_svc.list()
            assert len(users) == 2
            assert svc.list_calls == 1

            users2 = await cached_svc.list()
            assert len(users2) == 2
            assert svc.list_calls == 1  # still 1

            await cache.clear()

    async def test_update_invalidates_get_cache(self, redis_url: str) -> None:
        svc = _UserService()
        await svc.create({"id": 1, "name": "Alice"})

        settings = RedisCacheSettings(url=redis_url, key_prefix="test:cs:upd:")
        async with RedisCache(settings) as cache:
            cached_svc = CachedService(svc, cache, namespace="user")

            # Cache the get result
            await cached_svc.get(1)
            assert svc.get_calls == 1

            # Update → invalidates cache
            await cached_svc.update(1, {"name": "Alicia"})

            # Next get must hit the service again (stale cache evicted)
            user = await cached_svc.get(1)
            assert user.name == "Alicia"
            assert svc.get_calls == 2

            await cache.clear()

    async def test_delete_invalidates_get_cache(self, redis_url: str) -> None:
        svc = _UserService()
        await svc.create({"id": 1, "name": "Alice"})

        settings = RedisCacheSettings(url=redis_url, key_prefix="test:cs:del:")
        async with RedisCache(settings) as cache:
            cached_svc = CachedService(svc, cache, namespace="user")

            await cached_svc.get(1)
            assert svc.get_calls == 1

            await cached_svc.delete(1)

            result = await cached_svc.get(1)
            assert result is None  # entity gone
            assert svc.get_calls == 2  # service was called again

            await cache.clear()

    async def test_create_invalidates_list_cache(self, redis_url: str) -> None:
        svc = _UserService()
        await svc.create({"id": 1, "name": "Alice"})

        settings = RedisCacheSettings(url=redis_url, key_prefix="test:cs:create:")
        async with RedisCache(settings) as cache:
            cached_svc = CachedService(svc, cache, namespace="user")

            # Cache the list
            users = await cached_svc.list()
            assert len(users) == 1
            assert svc.list_calls == 1

            # Create a new user → list cache invalidated
            await cached_svc.create({"id": 2, "name": "Bob"})

            # Next list must hit the service again
            users2 = await cached_svc.list()
            assert len(users2) == 2
            assert svc.list_calls == 2

            await cache.clear()

    async def test_exists_cached_then_invalidated(self, redis_url: str) -> None:
        svc = _UserService()
        await svc.create({"id": 1, "name": "Alice"})

        settings = RedisCacheSettings(url=redis_url, key_prefix="test:cs:ex:")
        async with RedisCache(settings) as cache:
            cached_svc = CachedService(svc, cache, namespace="user")

            assert await cached_svc.exists(1) is True
            assert svc.exists_calls == 1

            # Second call: cache hit
            assert await cached_svc.exists(1) is True
            assert svc.exists_calls == 1

            # Delete → invalidates
            await cached_svc.delete(1)

            assert await cached_svc.exists(1) is False
            assert svc.exists_calls == 2

            await cache.clear()


# ── 5 — CachedService + producer/consumer event-driven invalidation ──────────────


class TestCachedServiceEventDriven:
    async def test_event_driven_invalidation_via_producer(self, redis_url: str) -> None:
        """
        Simulate a write in another process publishing a CacheInvalidated event.
        The CacheInvalidationConsumer reacts and evicts the stale key via
        ExplicitStrategy — no class holds a direct bus reference except
        BusEventProducer (producer side) and CacheInvalidationConsumer (consumer side).

        In a real multi-process setup:
            - Process A writes to the DB; CachedService uses BusEventProducer to
              publish CacheInvalidated.
            - Process B has CacheInvalidationConsumer subscribed to the bus.
            - Process B's ExplicitStrategy marks the key; next read evicts it.

        Here we simulate with an InMemoryEventBus shared between both sides.
        """
        bus = InMemoryEventBus()
        svc = _UserService()
        await svc.create({"id": 1, "name": "Alice"})

        settings = RedisCacheSettings(url=redis_url, key_prefix="test:cs:bus:")
        # ExplicitStrategy is shared — consumer writes to it, cache reads from it
        event_strategy = ExplicitStrategy()

        consumer = CacheInvalidationConsumer(
            event_strategy, channel="varco.cache.invalidations"
        )
        consumer.register_to(bus)

        async with RedisCache(settings, strategy=event_strategy) as cache:
            cached_svc = CachedService(
                svc,
                cache,
                namespace="user",
                producer=BusEventProducer(bus),
                bus_channel="varco.cache.invalidations",
            )

            # Warm the cache
            user = await cached_svc.get(1)
            assert user.name == "Alice"
            assert svc.get_calls == 1

            # Simulate an external process updating the entity and publishing
            # the invalidation event directly (bypassing CachedService.update)
            svc._store[1] = _User(id=1, name="Alicia")  # DB updated externally
            await bus.publish(
                CacheInvalidationEvent(
                    keys=["user:get:1"],
                    namespace="user",
                    operation="update",
                ),
                channel="varco.cache.invalidations",
            )
            # Yield for the async subscription handler to run
            await asyncio.sleep(0.05)

            # Next read must go to the service (cache evicted by consumer)
            user2 = await cached_svc.get(1)
            assert user2.name == "Alicia"
            assert svc.get_calls == 2

            await cache.clear()

    async def test_layered_cache_with_event_driven_invalidation(
        self, redis_url: str
    ) -> None:
        """
        Full stack: InMemoryEventBus + L1(InMemory, ExplicitStrategy) + L2(Redis).
        CacheInvalidationConsumer on L1 reacts to bus events via ExplicitStrategy
        to clear promoted entries — only EventConsumer touches the bus subscription.
        """
        bus = InMemoryEventBus()
        svc = _UserService()
        await svc.create({"id": 10, "name": "Carol"})

        # Shared ExplicitStrategy — wired to both L1 and the consumer
        event_strategy = ExplicitStrategy()
        consumer = CacheInvalidationConsumer(event_strategy, channel="cache.inv")
        consumer.register_to(bus)

        l1 = InMemoryCache(strategy=event_strategy)
        l2 = RedisCache(RedisCacheSettings(url=redis_url, key_prefix="test:cs:full:"))

        async with LayeredCache(l1, l2, promote_ttl=60) as cache:
            cached_svc = CachedService(
                svc,
                cache,
                namespace="u",
                producer=BusEventProducer(bus),
                bus_channel="cache.inv",
            )

            # Warm both layers
            user = await cached_svc.get(10)
            assert user.name == "Carol"
            assert svc.get_calls == 1
            assert await l1.get("u:get:10") is not None  # promoted to L1

            # External update + bus event
            svc._store[10] = _User(id=10, name="Caroline")
            await bus.publish(
                CacheInvalidationEvent(
                    keys=["u:get:10"], namespace="u", operation="update"
                ),
                channel="cache.inv",
            )
            await asyncio.sleep(0.05)

            # L1 evicted; L2 still has stale value so the next read will
            # hit L2 first. Then the service itself has the fresh value.
            # The test validates that get() goes to service (not stale L1/L2)
            # after explicit also clearing L2:
            await l2.delete("u:get:10")  # also clear L2 to force service call

            user2 = await cached_svc.get(10)
            assert user2.name == "Caroline"

            await l2.clear()
