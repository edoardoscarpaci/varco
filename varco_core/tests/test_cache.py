"""
Unit tests for varco_core.cache
================================
Covers all cache components without any external dependencies.

Sections
--------
- ``NoOpCache``               — all reads return None; writes are no-ops
- ``InMemoryCache``           — get/set/delete/exists/clear; start/stop lifecycle
- ``InMemoryCache`` + TTL     — TTLStrategy eviction on read
- ``InMemoryCache`` + Explicit— ExplicitStrategy key-level invalidation
- ``InMemoryCache`` + Tagged  — TaggedStrategy bulk-tag invalidation
- ``InMemoryCache`` + Composite— CompositeStrategy (TTL OR Explicit)
- ``InMemoryCache`` max_size  — FIFO eviction when capacity exceeded
- ``CacheInvalidationConsumer`` — EventConsumer drives ExplicitStrategy from bus events
- ``LayeredCache``            — L1/L2 read-promote, write-through, write-around
- ``LayeredCache`` lifecycle  — start/stop propagates; not-started guard
- ``CacheSettings``           — defaults, frozen, from_dict
"""

from __future__ import annotations

import asyncio
import time

import pytest

from varco_core.cache import (
    CacheInvalidationConsumer,
    CacheSettings,
    CompositeStrategy,
    ExplicitStrategy,
    InMemoryCache,
    LayeredCache,
    NoOpCache,
    TaggedStrategy,
    TTLStrategy,
)
from varco_core.cache.service import CacheInvalidated
from varco_core.event import InMemoryEventBus


# ── CacheSettings ───────────────────────────────────────────────────────────────


class TestCacheSettings:
    def test_defaults(self) -> None:
        s = CacheSettings()
        assert s.default_ttl is None

    def test_frozen(self) -> None:
        s = CacheSettings()
        with pytest.raises(Exception):
            s.default_ttl = 10  # type: ignore[misc]

    def test_from_dict(self) -> None:
        s = CacheSettings.from_dict({"default_ttl": 30.0})
        assert s.default_ttl == 30.0


# ── NoOpCache ───────────────────────────────────────────────────────────────────


class TestNoOpCache:
    async def test_get_returns_none(self) -> None:
        cache = NoOpCache()
        async with cache:
            assert await cache.get("k") is None

    async def test_set_is_discarded(self) -> None:
        cache = NoOpCache()
        async with cache:
            await cache.set("k", "v")
            assert await cache.get("k") is None

    async def test_exists_always_false(self) -> None:
        cache = NoOpCache()
        async with cache:
            await cache.set("k", "v")
            assert await cache.exists("k") is False

    async def test_delete_noop(self) -> None:
        cache = NoOpCache()
        async with cache:
            await cache.delete("k")  # must not raise

    async def test_clear_noop(self) -> None:
        cache = NoOpCache()
        async with cache:
            await cache.clear()  # must not raise

    async def test_start_stop_idempotent(self) -> None:
        cache = NoOpCache()
        await cache.start()
        await cache.stop()
        await cache.stop()  # idempotent


# ── InMemoryCache — basic operations ───────────────────────────────────────────


class TestInMemoryCacheBasic:
    async def test_set_and_get(self) -> None:
        async with InMemoryCache() as cache:
            await cache.set("user:1", {"name": "Alice"})
            result = await cache.get("user:1")
            assert result == {"name": "Alice"}

    async def test_get_missing_returns_none(self) -> None:
        async with InMemoryCache() as cache:
            assert await cache.get("missing") is None

    async def test_delete_removes_key(self) -> None:
        async with InMemoryCache() as cache:
            await cache.set("k", "v")
            await cache.delete("k")
            assert await cache.get("k") is None

    async def test_delete_nonexistent_noop(self) -> None:
        async with InMemoryCache() as cache:
            await cache.delete("missing")  # must not raise

    async def test_exists_true(self) -> None:
        async with InMemoryCache() as cache:
            await cache.set("k", "v")
            assert await cache.exists("k") is True

    async def test_exists_false_for_missing(self) -> None:
        async with InMemoryCache() as cache:
            assert await cache.exists("missing") is False

    async def test_clear_removes_all(self) -> None:
        async with InMemoryCache() as cache:
            await cache.set("a", 1)
            await cache.set("b", 2)
            await cache.clear()
            assert await cache.get("a") is None
            assert await cache.get("b") is None
            assert cache.size == 0

    async def test_overwrite_value(self) -> None:
        async with InMemoryCache() as cache:
            await cache.set("k", "first")
            await cache.set("k", "second")
            assert await cache.get("k") == "second"

    async def test_none_value_stored(self) -> None:
        async with InMemoryCache() as cache:
            await cache.set("k", None)
            # get() returns None for both miss and stored-None —
            # use exists() to distinguish
            assert await cache.exists("k") is True

    async def test_size_property(self) -> None:
        async with InMemoryCache() as cache:
            assert cache.size == 0
            await cache.set("a", 1)
            await cache.set("b", 2)
            assert cache.size == 2

    async def test_repr_contains_class_name(self) -> None:
        async with InMemoryCache() as cache:
            assert "InMemoryCache" in repr(cache)
            assert "started=True" in repr(cache)


# ── InMemoryCache — lifecycle ───────────────────────────────────────────────────


class TestInMemoryCacheLifecycle:
    async def test_not_started_raises_on_get(self) -> None:
        cache = InMemoryCache()
        with pytest.raises(RuntimeError, match="not started"):
            await cache.get("k")

    async def test_not_started_raises_on_set(self) -> None:
        cache = InMemoryCache()
        with pytest.raises(RuntimeError, match="not started"):
            await cache.set("k", "v")

    async def test_stop_before_start_is_noop(self) -> None:
        cache = InMemoryCache()
        await cache.stop()  # must not raise

    async def test_context_manager_starts_and_stops(self) -> None:
        cache = InMemoryCache()
        async with cache:
            assert cache._started
        assert not cache._started

    async def test_double_start_raises(self) -> None:
        cache = InMemoryCache()
        await cache.start()
        with pytest.raises(RuntimeError, match="already-started"):
            await cache.start()
        await cache.stop()


# ── TTLStrategy ─────────────────────────────────────────────────────────────────


class TestTTLStrategy:
    def test_no_ttl_never_invalidates(self) -> None:
        s = TTLStrategy(default_ttl=None)
        assert (
            s.should_invalidate("k", {"stored_at": time.time() - 10000, "ttl": None})
            is False
        )

    def test_expired_entry_invalidated(self) -> None:
        s = TTLStrategy(default_ttl=1)
        # Omit "ttl" key entirely so the strategy falls back to default_ttl=1
        metadata = {"stored_at": time.time() - 10}
        assert s.should_invalidate("k", metadata) is True

    def test_fresh_entry_not_invalidated(self) -> None:
        s = TTLStrategy(default_ttl=300)
        metadata = {"stored_at": time.time(), "ttl": None}
        assert s.should_invalidate("k", metadata) is False

    def test_per_entry_ttl_overrides_default(self) -> None:
        s = TTLStrategy(default_ttl=300)
        # Per-entry TTL of 1s, entry is 10s old → invalid
        metadata = {"stored_at": time.time() - 10, "ttl": 1.0}
        assert s.should_invalidate("k", metadata) is True

    def test_missing_stored_at_treated_as_now(self) -> None:
        s = TTLStrategy(default_ttl=1)
        # No stored_at → treated as brand-new → not expired
        assert s.should_invalidate("k", {}) is False

    async def test_start_stop_noop(self) -> None:
        s = TTLStrategy(default_ttl=60)
        await s.start()
        await s.stop()

    async def test_cache_evicts_expired_on_read(self) -> None:
        s = TTLStrategy(default_ttl=0.05)  # 50 ms default TTL
        async with InMemoryCache(strategy=s) as cache:
            # set() stores the entry with ttl=None → strategy uses default_ttl
            await cache.set("k", "v")
            assert await cache.get("k") == "v"
            await asyncio.sleep(0.15)  # wait well past 50ms
            assert await cache.get("k") is None
            assert cache.size == 0  # evicted from store


# ── ExplicitStrategy ────────────────────────────────────────────────────────────


class TestExplicitStrategy:
    def test_not_invalidated_by_default(self) -> None:
        s = ExplicitStrategy()
        assert s.should_invalidate("k", {}) is False

    def test_invalidate_marks_key(self) -> None:
        s = ExplicitStrategy()
        s.invalidate("k")
        assert s.should_invalidate("k", {}) is True

    def test_invalidate_many(self) -> None:
        s = ExplicitStrategy()
        s.invalidate_many(["a", "b", "c"])
        assert s.should_invalidate("a", {}) is True
        assert s.should_invalidate("b", {}) is True
        assert s.should_invalidate("c", {}) is True

    def test_clear_invalidated_removes_key(self) -> None:
        s = ExplicitStrategy()
        s.invalidate("k")
        s.clear_invalidated("k")
        assert s.should_invalidate("k", {}) is False

    def test_reset_clears_all(self) -> None:
        s = ExplicitStrategy()
        s.invalidate_many(["a", "b"])
        s.reset()
        assert s.should_invalidate("a", {}) is False

    async def test_cache_evicts_explicitly_invalidated_on_read(self) -> None:
        s = ExplicitStrategy()
        async with InMemoryCache(strategy=s) as cache:
            await cache.set("user:1", "Alice")
            s.invalidate("user:1")
            assert await cache.get("user:1") is None


# ── TaggedStrategy ───────────────────────────────────────────────────────────────


class TestTaggedStrategy:
    async def test_tagged_entry_invalidated_on_tag_eviction(self) -> None:
        s = TaggedStrategy()
        async with InMemoryCache(strategy=s) as cache:
            await cache.set("user:1:profile", "Alice", tags={"user:1"})
            await cache.set("user:1:orders", ["o1"], tags={"user:1"})
            await cache.set("user:2:profile", "Bob", tags={"user:2"})

            s.invalidate_tag("user:1")

            assert await cache.get("user:1:profile") is None
            assert await cache.get("user:1:orders") is None
            # user:2 is not affected
            assert await cache.get("user:2:profile") == "Bob"

    def test_no_tags_never_invalidated(self) -> None:
        s = TaggedStrategy()
        s.invalidate_tag("user:1")
        assert s.should_invalidate("k", {"tags": set()}) is False

    def test_multiple_tags_any_match_invalidates(self) -> None:
        s = TaggedStrategy()
        s.invalidate_tag("products")
        assert s.should_invalidate("k", {"tags": {"user:1", "products"}}) is True


# ── CompositeStrategy ───────────────────────────────────────────────────────────


class TestCompositeStrategy:
    def test_requires_at_least_one_strategy(self) -> None:
        with pytest.raises(ValueError, match="at least one"):
            CompositeStrategy()

    def test_any_child_true_returns_true(self) -> None:
        explicit = ExplicitStrategy()
        ttl = TTLStrategy(default_ttl=None)
        composite = CompositeStrategy(ttl, explicit)
        explicit.invalidate("k")
        assert composite.should_invalidate("k", {}) is True

    def test_all_false_returns_false(self) -> None:
        ttl = TTLStrategy(default_ttl=300)
        explicit = ExplicitStrategy()
        composite = CompositeStrategy(ttl, explicit)
        assert (
            composite.should_invalidate("k", {"stored_at": time.time(), "ttl": None})
            is False
        )

    async def test_start_stop_propagates_to_children(self) -> None:
        started = []
        stopped = []

        class TrackingStrategy(ExplicitStrategy):
            async def start(self) -> None:
                started.append(1)

            async def stop(self) -> None:
                stopped.append(1)

        s1 = TrackingStrategy()
        s2 = TrackingStrategy()
        composite = CompositeStrategy(s1, s2)
        await composite.start()
        assert len(started) == 2
        await composite.stop()
        assert len(stopped) == 2

    async def test_ttl_or_explicit_eviction(self) -> None:
        explicit = ExplicitStrategy()
        ttl = TTLStrategy(default_ttl=300)
        composite = CompositeStrategy(ttl, explicit)

        async with InMemoryCache(strategy=composite) as cache:
            await cache.set("a", "val-a")
            await cache.set("b", "val-b")

            explicit.invalidate("a")
            assert await cache.get("a") is None  # evicted by explicit
            assert await cache.get("b") == "val-b"  # TTL is 300s, not expired


# ── InMemoryCache max_size ──────────────────────────────────────────────────────


class TestInMemoryCacheMaxSize:
    async def test_oldest_entry_evicted_when_full(self) -> None:
        async with InMemoryCache(max_size=3) as cache:
            await cache.set("a", 1)
            await cache.set("b", 2)
            await cache.set("c", 3)
            await cache.set("d", 4)  # triggers FIFO eviction of "a"

            assert await cache.get("a") is None
            assert await cache.get("b") == 2
            assert await cache.get("c") == 3
            assert await cache.get("d") == 4
            assert cache.size == 3

    async def test_size_never_exceeds_max(self) -> None:
        async with InMemoryCache(max_size=2) as cache:
            for i in range(10):
                await cache.set(f"k{i}", i)
            assert cache.size == 2


# ── CacheInvalidationConsumer ────────────────────────────────────────────────────


class TestCacheInvalidationConsumer:
    async def test_key_invalidated_after_bus_event(self) -> None:
        """
        Publishing a CacheInvalidated event marks the key in ExplicitStrategy,
        so the next cache.get() misses and returns None.
        """
        bus = InMemoryEventBus()
        # strategy is shared between consumer and cache backend — same object
        strategy = ExplicitStrategy()
        consumer = CacheInvalidationConsumer(strategy, channel="cache-invalidations")
        consumer.register_to(bus)

        async with InMemoryCache(strategy=strategy) as cache:
            await cache.set("user:1", "Alice")

            # Publish the real CacheInvalidated domain event
            await bus.publish(
                CacheInvalidated(keys=["user:1"], namespace="user", operation="update"),
                channel="cache-invalidations",
            )
            # Yield to let the async subscription handler run
            await asyncio.sleep(0)
            await asyncio.sleep(0)

            # ExplicitStrategy marked the key — cache backend evicts on next get
            assert await cache.get("user:1") is None

    async def test_unrelated_key_not_affected(self) -> None:
        """Keys not in the event are not marked — unrelated entries survive."""
        bus = InMemoryEventBus()
        strategy = ExplicitStrategy()
        consumer = CacheInvalidationConsumer(strategy, channel="cache-invalidations")
        consumer.register_to(bus)

        async with InMemoryCache(strategy=strategy) as cache:
            await cache.set("user:1", "Alice")
            await cache.set("user:2", "Bob")

            await bus.publish(
                CacheInvalidated(keys=["user:1"], namespace="user", operation="delete"),
                channel="cache-invalidations",
            )
            await asyncio.sleep(0)
            await asyncio.sleep(0)

            assert await cache.get("user:1") is None
            assert await cache.get("user:2") == "Bob"

    async def test_register_returns_one_subscription(self) -> None:
        """register_to() returns exactly one Subscription handle per consumer."""
        bus = InMemoryEventBus()
        strategy = ExplicitStrategy()
        consumer = CacheInvalidationConsumer(strategy, channel="cache-invalidations")
        subs = consumer.register_to(bus)

        assert len(subs) == 1
        assert not subs[0].is_cancelled

    async def test_cancel_subscription_stops_invalidation(self) -> None:
        """After cancel(), subsequent events no longer mark keys."""
        bus = InMemoryEventBus()
        strategy = ExplicitStrategy()
        consumer = CacheInvalidationConsumer(strategy, channel="cache-invalidations")
        (sub,) = consumer.register_to(bus)

        # Cancel before publishing — handler must NOT be called
        sub.cancel()
        assert sub.is_cancelled

        await bus.publish(
            CacheInvalidated(keys=["user:99"], namespace="user", operation="delete"),
            channel="cache-invalidations",
        )
        await asyncio.sleep(0)

        # strategy must not have received the key
        assert not strategy.should_invalidate("user:99", {})

    async def test_callable_channel_resolves_from_instance(self) -> None:
        """
        The channel lambda is resolved at register_to() time — confirms the
        callable-channel pattern in @listen works as expected.
        """
        bus = InMemoryEventBus()
        strategy = ExplicitStrategy()
        # Use a custom channel — the lambda lambda self: self._channel should pick it up
        consumer = CacheInvalidationConsumer(strategy, channel="custom.channel")
        consumer.register_to(bus)

        await bus.publish(
            CacheInvalidated(keys=["x:1"], namespace="x", operation="create"),
            channel="custom.channel",
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)

        assert strategy.should_invalidate("x:1", {})


# ── LayeredCache ────────────────────────────────────────────────────────────────


class TestLayeredCacheBasic:
    async def test_requires_at_least_two_layers(self) -> None:
        with pytest.raises(ValueError, match="at least 2"):
            LayeredCache(InMemoryCache())

    async def test_write_through_populates_all_layers(self) -> None:
        l1 = InMemoryCache()
        l2 = InMemoryCache()
        async with LayeredCache(l1, l2) as cache:
            await cache.set("k", "v")
            assert await l1.get("k") == "v"
            assert await l2.get("k") == "v"

    async def test_read_from_l1_hit(self) -> None:
        l1 = InMemoryCache()
        l2 = InMemoryCache()
        async with LayeredCache(l1, l2) as cache:
            await cache.set("k", "l1-only")
            # Only L1 has it — set l2 to something different to prove L1 wins
            await l2.delete("k")
            await l2.set("k", "l2-value")
            # L1 hit — should return L1 value
            assert await cache.get("k") == "l1-only"

    async def test_l2_hit_promotes_to_l1(self) -> None:
        l1 = InMemoryCache()
        l2 = InMemoryCache()
        async with LayeredCache(l1, l2) as cache:
            # Only write to L2 directly
            await l2.set("k", "from-l2")
            # L1 miss → L2 hit → promotes to L1
            result = await cache.get("k")
            assert result == "from-l2"
            assert await l1.get("k") == "from-l2"  # promoted

    async def test_miss_in_all_layers_returns_none(self) -> None:
        l1 = InMemoryCache()
        l2 = InMemoryCache()
        async with LayeredCache(l1, l2) as cache:
            assert await cache.get("missing") is None

    async def test_delete_removes_from_all_layers(self) -> None:
        l1 = InMemoryCache()
        l2 = InMemoryCache()
        async with LayeredCache(l1, l2) as cache:
            await cache.set("k", "v")
            await cache.delete("k")
            assert await l1.get("k") is None
            assert await l2.get("k") is None

    async def test_exists_true_if_any_layer_has_key(self) -> None:
        l1 = InMemoryCache()
        l2 = InMemoryCache()
        async with LayeredCache(l1, l2) as cache:
            await l2.set("k", "v")
            assert await cache.exists("k") is True

    async def test_exists_false_if_no_layer_has_key(self) -> None:
        l1 = InMemoryCache()
        l2 = InMemoryCache()
        async with LayeredCache(l1, l2) as cache:
            assert await cache.exists("gone") is False

    async def test_clear_removes_from_all_layers(self) -> None:
        l1 = InMemoryCache()
        l2 = InMemoryCache()
        async with LayeredCache(l1, l2) as cache:
            await cache.set("a", 1)
            await cache.set("b", 2)
            await cache.clear()
            assert l1.size == 0
            assert l2.size == 0


class TestLayeredCacheWriteAround:
    async def test_write_around_only_writes_last_layer(self) -> None:
        l1 = InMemoryCache()
        l2 = InMemoryCache()
        async with LayeredCache(l1, l2, write_mode="write-around") as cache:
            await cache.set("k", "v")
            # L1 not written in write-around mode
            assert await l1.get("k") is None
            assert await l2.get("k") == "v"

    async def test_write_around_promotes_on_read(self) -> None:
        l1 = InMemoryCache()
        l2 = InMemoryCache()
        async with LayeredCache(l1, l2, write_mode="write-around") as cache:
            await cache.set("k", "v")
            # First read: L1 miss → L2 hit → promotes to L1
            assert await cache.get("k") == "v"
            assert await l1.get("k") == "v"  # promoted


class TestLayeredCachePromoteTTL:
    async def test_promote_ttl_applied_to_l1(self) -> None:
        l1 = InMemoryCache(strategy=TTLStrategy(default_ttl=None))
        l2 = InMemoryCache()
        async with LayeredCache(l1, l2, promote_ttl=0.01) as cache:
            await l2.set("k", "v")
            # Trigger promotion
            await cache.get("k")
            assert await l1.get("k") == "v"  # promoted with 10ms TTL
            await asyncio.sleep(0.05)
            assert await l1.get("k") is None  # TTL expired in L1


class TestLayeredCacheLifecycle:
    async def test_not_started_raises(self) -> None:
        cache = LayeredCache(InMemoryCache(), InMemoryCache())
        with pytest.raises(RuntimeError, match="not started"):
            await cache.get("k")

    async def test_double_start_raises(self) -> None:
        cache = LayeredCache(InMemoryCache(), InMemoryCache())
        await cache.start()
        with pytest.raises(RuntimeError, match="already-started"):
            await cache.start()
        await cache.stop()

    async def test_stop_before_start_noop(self) -> None:
        cache = LayeredCache(InMemoryCache(), InMemoryCache())
        await cache.stop()  # must not raise

    async def test_repr_contains_class_and_mode(self) -> None:
        cache = LayeredCache(
            InMemoryCache(), InMemoryCache(), write_mode="write-around"
        )
        assert "LayeredCache" in repr(cache)
        assert "write-around" in repr(cache)
