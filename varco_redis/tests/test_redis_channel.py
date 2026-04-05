"""
Unit tests for varco_redis.channel
=====================================
Covers ``RedisChannelManager`` — the local-registry Pub/Sub channel manager.

No Redis connection is required: all state is in-memory.

Sections
--------
- ``RedisChannelManagerSettings``  — alias check
- ``RedisChannelManager`` lifecycle — start/stop guards, double-start, context manager
- ``declare_channel``               — adds to registry, idempotent, config update
- ``delete_channel``                — removes from registry; noop if missing
- ``channel_exists``                — reflects local registry only
- ``list_channels``                 — sorted; requires started
- repr
"""

from __future__ import annotations

import pytest

from varco_redis.channel import RedisChannelManager, RedisChannelManagerSettings
from varco_redis.config import RedisEventBusSettings
from varco_core.event.base import ChannelConfig


# ── Settings alias ─────────────────────────────────────────────────────────────


class TestRedisChannelManagerSettings:
    def test_is_alias_of_redis_event_bus_settings(self) -> None:
        # RedisChannelManagerSettings is the same class as RedisEventBusSettings —
        # confirmed by identity check.  No separate settings class is needed
        # because Pub/Sub channels require no admin credentials.
        assert RedisChannelManagerSettings is RedisEventBusSettings

    def test_creates_with_url(self) -> None:
        settings = RedisChannelManagerSettings(url="redis://custom:6379/1")
        assert settings.url == "redis://custom:6379/1"


# ── Lifecycle ──────────────────────────────────────────────────────────────────


class TestRedisChannelManagerLifecycle:
    async def test_start_sets_started(self) -> None:
        manager = RedisChannelManager(RedisEventBusSettings())
        await manager.start()
        assert manager._started is True
        await manager.stop()

    async def test_stop_clears_started(self) -> None:
        manager = RedisChannelManager(RedisEventBusSettings())
        await manager.start()
        await manager.stop()
        assert manager._started is False

    async def test_stop_before_start_is_noop(self) -> None:
        # Stop on an unstarted manager must not raise — idempotent.
        manager = RedisChannelManager(RedisEventBusSettings())
        await manager.stop()  # must not raise

    async def test_double_start_raises(self) -> None:
        manager = RedisChannelManager(RedisEventBusSettings())
        await manager.start()
        with pytest.raises(RuntimeError, match="already-started"):
            await manager.start()
        await manager.stop()

    async def test_context_manager_starts_and_stops(self) -> None:
        manager = RedisChannelManager(RedisEventBusSettings())
        async with manager:
            assert manager._started is True
        assert manager._started is False

    async def test_operations_before_start_raise(self) -> None:
        manager = RedisChannelManager(RedisEventBusSettings())
        with pytest.raises(RuntimeError):
            await manager.declare_channel("orders")

    async def test_list_channels_before_start_raises(self) -> None:
        manager = RedisChannelManager(RedisEventBusSettings())
        with pytest.raises(RuntimeError):
            await manager.list_channels()

    async def test_channel_exists_before_start_raises(self) -> None:
        manager = RedisChannelManager(RedisEventBusSettings())
        with pytest.raises(RuntimeError):
            await manager.channel_exists("orders")

    async def test_delete_channel_before_start_raises(self) -> None:
        manager = RedisChannelManager(RedisEventBusSettings())
        with pytest.raises(RuntimeError):
            await manager.delete_channel("orders")


# ── declare_channel ────────────────────────────────────────────────────────────


class TestRedisChannelManagerDeclare:
    async def test_declare_adds_to_registry(self) -> None:
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.declare_channel("orders")
            assert await m.channel_exists("orders") is True

    async def test_declare_multiple_channels(self) -> None:
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.declare_channel("orders")
            await m.declare_channel("payments")
            assert await m.channel_exists("orders") is True
            assert await m.channel_exists("payments") is True

    async def test_declare_with_config_stores_config(self) -> None:
        config = ChannelConfig(num_partitions=3, replication_factor=1)
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.declare_channel("orders", config=config)
            # Config is stored locally — verify via the internal registry.
            stored = m._registry.get("orders")
            assert stored is config

    async def test_declare_again_with_none_config_preserves_existing(self) -> None:
        # Redeclaring with config=None must NOT overwrite an existing config.
        config = ChannelConfig(num_partitions=6)
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.declare_channel("orders", config=config)
            await m.declare_channel("orders", config=None)  # must not overwrite
            assert m._registry["orders"] is config

    async def test_declare_again_with_new_config_updates(self) -> None:
        # Redeclaring with a new non-None config DOES update the stored config.
        original = ChannelConfig(num_partitions=1)
        updated = ChannelConfig(num_partitions=6)
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.declare_channel("orders", config=original)
            await m.declare_channel("orders", config=updated)
            assert m._registry["orders"] is updated


# ── delete_channel ─────────────────────────────────────────────────────────────


class TestRedisChannelManagerDelete:
    async def test_delete_removes_from_registry(self) -> None:
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.declare_channel("orders")
            await m.delete_channel("orders")
            assert await m.channel_exists("orders") is False

    async def test_delete_nonexistent_is_noop(self) -> None:
        # Deleting a channel never declared must not raise — silent no-op.
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.delete_channel("undeclared")  # must not raise


# ── channel_exists ─────────────────────────────────────────────────────────────


class TestRedisChannelManagerExists:
    async def test_exists_returns_false_for_undeclared(self) -> None:
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            assert await m.channel_exists("nonexistent") is False

    async def test_exists_returns_true_after_declare(self) -> None:
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.declare_channel("payments")
            assert await m.channel_exists("payments") is True

    async def test_exists_returns_false_after_delete(self) -> None:
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.declare_channel("payments")
            await m.delete_channel("payments")
            assert await m.channel_exists("payments") is False


# ── list_channels ─────────────────────────────────────────────────────────────


class TestRedisChannelManagerList:
    async def test_empty_registry_returns_empty_list(self) -> None:
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            assert await m.list_channels() == []

    async def test_returns_sorted_channels(self) -> None:
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.declare_channel("zzz")
            await m.declare_channel("aaa")
            await m.declare_channel("mmm")
            channels = await m.list_channels()
            assert channels == ["aaa", "mmm", "zzz"]

    async def test_deleted_channels_not_in_list(self) -> None:
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.declare_channel("orders")
            await m.declare_channel("payments")
            await m.delete_channel("orders")
            assert await m.list_channels() == ["payments"]


# ── repr ───────────────────────────────────────────────────────────────────────


class TestRedisChannelManagerRepr:
    async def test_repr_contains_class_name(self) -> None:
        m = RedisChannelManager(RedisEventBusSettings())
        assert "RedisChannelManager" in repr(m)

    async def test_repr_contains_started_state(self) -> None:
        m = RedisChannelManager(RedisEventBusSettings())
        assert "started=False" in repr(m)
        await m.start()
        assert "started=True" in repr(m)
        await m.stop()

    async def test_repr_contains_channel_count(self) -> None:
        async with RedisChannelManager(RedisEventBusSettings()) as m:
            await m.declare_channel("orders")
            await m.declare_channel("payments")
            assert "2" in repr(m)
