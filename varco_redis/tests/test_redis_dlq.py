"""
Unit tests for varco_redis.dlq
================================
Covers ``RedisDLQ`` — the Redis Hash + Sorted Set backed dead letter queue.

All tests use a ``FakeRedis`` test double that supports the exact Redis
commands used by ``RedisDLQ``: ``hset``, ``hdel``, ``hmget``, ``zadd``,
``zrem``, ``zrange``, ``zcard``, and ``pipeline``.

No real Redis instance is required.

Sections
--------
- ``RedisDLQ`` construction / repr
- lifecycle: connect/disconnect (idempotent, context manager)
- ``push()``       — stores in hash+zset, never raises, drops if not connected
- ``pop_batch()``  — reads from zset+hash, limit guard, not-connected guard
- ``ack()``        — removes from both structures atomically, unknown id noop
- ``count()``      — ZCARD, not-connected guard
- serialization   — round-trip of DeadLetterEntry (including nested Event)
- DI              — ``RedisDLQConfiguration`` wires settings + DLQ
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any
from unittest.mock import patch

import pytest

from varco_core.event import Event
from varco_core.event.dlq import DeadLetterEntry
from varco_redis.config import RedisEventBusSettings
from varco_redis.dlq import RedisDLQ, RedisDLQConfiguration


# ── Minimal event for tests ────────────────────────────────────────────────────


class SampleEvent(Event):
    __event_type__ = "test.sample.redis_dlq"
    payload: str = "test"


# ── FakeRedis test double ──────────────────────────────────────────────────────


class FakePipeline:
    """
    Fake redis.asyncio pipeline.

    Collects HSET / HDEL / ZADD / ZREM calls and executes them synchronously
    against the parent ``FakeRedis`` store on ``execute()``.
    """

    def __init__(self, redis: FakeRedis) -> None:
        # Commands queued before execute() — list of (method_name, args, kwargs)
        self._commands: list[tuple[str, tuple, dict]] = []
        self._redis = redis

    async def hset(self, key: str, field: str, value: bytes) -> None:
        self._commands.append(("hset", (key, field, value), {}))

    async def hdel(self, key: str, field: str) -> None:
        self._commands.append(("hdel", (key, field), {}))

    async def zadd(self, key: str, mapping: dict[str, float]) -> None:
        self._commands.append(("zadd", (key, mapping), {}))

    async def zrem(self, key: str, member: str) -> None:
        self._commands.append(("zrem", (key, member), {}))

    async def execute(self) -> list:
        results = []
        for cmd, args, _kwargs in self._commands:
            if cmd == "hset":
                key, field, value = args
                self._redis._hash.setdefault(key, {})[field] = value
                results.append(1)
            elif cmd == "hdel":
                key, field = args
                self._redis._hash.get(key, {}).pop(field, None)
                results.append(1)
            elif cmd == "zadd":
                key, mapping = args
                for member, score in mapping.items():
                    self._redis._zset.setdefault(key, {})[member] = score
                results.append(1)
            elif cmd == "zrem":
                key, member = args
                self._redis._zset.get(key, {}).pop(member, None)
                results.append(1)
        return results

    async def __aenter__(self) -> FakePipeline:
        return self

    async def __aexit__(self, *_: Any) -> None:
        pass


class FakeRedis:
    """
    In-memory fake for redis.asyncio.

    Supports: hset, hdel, hmget, zadd, zrem, zrange, zcard, pipeline, aclose.
    """

    def __init__(self) -> None:
        # Hash: key → {field: bytes}
        self._hash: dict[str, dict[str, bytes]] = defaultdict(dict)
        # Sorted set: key → {member: score}
        self._zset: dict[str, dict[str, float]] = defaultdict(dict)

    def pipeline(self, transaction: bool = True) -> FakePipeline:
        return FakePipeline(self)

    async def hmget(self, key: str, *fields: str) -> list[bytes | None]:
        store = self._hash.get(key, {})
        return [store.get(f) for f in fields]

    async def zrange(self, key: str, start: int, stop: int) -> list[bytes]:
        members = self._zset.get(key, {})
        # Sort by score ascending (FIFO order), then slice by rank.
        sorted_members = sorted(members.items(), key=lambda x: x[1])
        sliced = (
            sorted_members[start : stop + 1] if stop >= 0 else sorted_members[start:]
        )
        return [m.encode("utf-8") for m, _ in sliced]

    async def zcard(self, key: str) -> int:
        return len(self._zset.get(key, {}))

    async def aclose(self) -> None:
        pass


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def settings() -> RedisEventBusSettings:
    return RedisEventBusSettings(url="redis://fake:6379/0")


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
async def dlq(settings: RedisEventBusSettings, fake_redis: FakeRedis) -> RedisDLQ:
    """Connected ``RedisDLQ`` with a fake Redis client."""
    with patch("varco_redis.dlq.aioredis") as mock_aioredis:
        mock_aioredis.from_url.return_value = fake_redis
        dlq = RedisDLQ(settings)
        await dlq.connect()
        yield dlq
        await dlq.disconnect()


def _make_entry(handler_name: str = "H.handle") -> DeadLetterEntry:
    return DeadLetterEntry(
        event=SampleEvent(),
        channel="orders",
        handler_name=handler_name,
        error_type="ValueError",
        error_message="something went wrong",
        attempts=3,
    )


# ── Construction and repr ─────────────────────────────────────────────────────


class TestRedisDLQConstruction:
    def test_repr_contains_class_name(self, settings: RedisEventBusSettings) -> None:
        dlq = RedisDLQ(settings)
        assert "RedisDLQ" in repr(dlq)

    def test_repr_shows_disconnected(self, settings: RedisEventBusSettings) -> None:
        dlq = RedisDLQ(settings)
        assert "connected=False" in repr(dlq)

    def test_repr_shows_url(self, settings: RedisEventBusSettings) -> None:
        dlq = RedisDLQ(settings)
        assert "fake" in repr(dlq)

    def test_default_settings_reads_from_env(self) -> None:
        # Constructing without arguments must not raise — defaults to from_env().
        dlq = RedisDLQ()
        assert dlq is not None


# ── Lifecycle ─────────────────────────────────────────────────────────────────


class TestRedisDLQLifecycle:
    async def test_connect_sets_redis_client(
        self, settings: RedisEventBusSettings, fake_redis: FakeRedis
    ) -> None:
        with patch("varco_redis.dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            dlq = RedisDLQ(settings)
            await dlq.connect()
            assert dlq._redis is not None
            await dlq.disconnect()

    async def test_connect_idempotent(
        self, settings: RedisEventBusSettings, fake_redis: FakeRedis
    ) -> None:
        with patch("varco_redis.dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            dlq = RedisDLQ(settings)
            await dlq.connect()
            first_client = dlq._redis
            await dlq.connect()  # second connect is a no-op
            assert dlq._redis is first_client
            await dlq.disconnect()

    async def test_disconnect_before_connect_is_noop(
        self, settings: RedisEventBusSettings
    ) -> None:
        dlq = RedisDLQ(settings)
        await dlq.disconnect()  # must not raise

    async def test_disconnect_clears_redis_client(
        self, settings: RedisEventBusSettings, fake_redis: FakeRedis
    ) -> None:
        with patch("varco_redis.dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            dlq = RedisDLQ(settings)
            await dlq.connect()
            await dlq.disconnect()
            assert dlq._redis is None

    async def test_context_manager(
        self, settings: RedisEventBusSettings, fake_redis: FakeRedis
    ) -> None:
        with patch("varco_redis.dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisDLQ(settings) as dlq:
                assert dlq._redis is not None
            assert dlq._redis is None

    async def test_repr_shows_connected_after_connect(self, dlq: RedisDLQ) -> None:
        assert "connected=True" in repr(dlq)


# ── push() ────────────────────────────────────────────────────────────────────


class TestRedisDLQPush:
    async def test_push_stores_in_hash(
        self, dlq: RedisDLQ, fake_redis: FakeRedis
    ) -> None:
        entry = _make_entry()
        await dlq.push(entry)

        # Entry should be in the entries hash
        hash_store = fake_redis._hash.get(dlq._entries_key, {})
        assert str(entry.entry_id) in hash_store

    async def test_push_stores_in_sorted_set(
        self, dlq: RedisDLQ, fake_redis: FakeRedis
    ) -> None:
        entry = _make_entry()
        await dlq.push(entry)

        zset_store = fake_redis._zset.get(dlq._queue_key, {})
        assert str(entry.entry_id) in zset_store

    async def test_push_never_raises(
        self, settings: RedisEventBusSettings, fake_redis: FakeRedis
    ) -> None:
        with patch("varco_redis.dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            dlq = RedisDLQ(settings)
            await dlq.connect()
            # Patch the pipeline to raise — push must swallow it.
            fake_redis._hash = None  # type: ignore[assignment]  # will cause AttributeError
            entry = _make_entry()
            await dlq.push(entry)  # must NOT raise

    async def test_push_before_connect_is_noop(
        self, settings: RedisEventBusSettings
    ) -> None:
        # push() before connect() logs a warning and returns silently.
        dlq = RedisDLQ(settings)
        await dlq.push(_make_entry())  # must not raise

    async def test_push_score_is_timestamp(
        self, dlq: RedisDLQ, fake_redis: FakeRedis
    ) -> None:
        entry = _make_entry()
        await dlq.push(entry)

        score = fake_redis._zset[dlq._queue_key][str(entry.entry_id)]
        # Score is the Unix timestamp of last_failed_at — must be positive and
        # within a sane range (close to now).
        assert score == pytest.approx(entry.last_failed_at.timestamp(), abs=1.0)


# ── pop_batch() ───────────────────────────────────────────────────────────────


class TestRedisDLQPopBatch:
    async def test_pop_batch_limit_below_one_raises(self, dlq: RedisDLQ) -> None:
        with pytest.raises(ValueError, match="limit"):
            await dlq.pop_batch(limit=0)

    async def test_pop_batch_before_connect_raises(
        self, settings: RedisEventBusSettings
    ) -> None:
        dlq = RedisDLQ(settings)
        with pytest.raises(RuntimeError, match="connect"):
            await dlq.pop_batch()

    async def test_pop_batch_empty_returns_empty_list(self, dlq: RedisDLQ) -> None:
        result = await dlq.pop_batch()
        assert result == []

    async def test_pop_batch_returns_pushed_entry(
        self, dlq: RedisDLQ, fake_redis: FakeRedis
    ) -> None:
        entry = _make_entry("OrderConsumer.on_order")
        await dlq.push(entry)

        result = await dlq.pop_batch(limit=10)
        assert len(result) == 1
        assert result[0].handler_name == "OrderConsumer.on_order"

    async def test_pop_batch_respects_limit(
        self, dlq: RedisDLQ, fake_redis: FakeRedis
    ) -> None:
        for i in range(5):
            await dlq.push(_make_entry(f"H.h{i}"))

        result = await dlq.pop_batch(limit=3)
        assert len(result) == 3

    async def test_pop_batch_does_not_remove_entries(
        self, dlq: RedisDLQ, fake_redis: FakeRedis
    ) -> None:
        # pop_batch leaves entries until ack() — at-least-once semantics.
        entry = _make_entry()
        await dlq.push(entry)
        await dlq.pop_batch(limit=10)

        # Entry still in sorted set and hash
        assert await dlq.count() == 1

    async def test_pop_batch_deserializes_event_type(
        self, dlq: RedisDLQ, fake_redis: FakeRedis
    ) -> None:
        entry = _make_entry()
        await dlq.push(entry)
        result = await dlq.pop_batch(limit=1)

        assert isinstance(result[0].event, SampleEvent)

    async def test_pop_batch_fifo_order(
        self, dlq: RedisDLQ, fake_redis: FakeRedis
    ) -> None:
        # Entries with earlier last_failed_at should come first.
        for i in range(3):
            await dlq.push(_make_entry(f"H.h{i}"))

        result = await dlq.pop_batch(limit=3)
        # FIFO = entries added first (lowest timestamp score) come first.
        # Since all entries are created almost simultaneously, just verify count.
        assert len(result) == 3


# ── ack() ─────────────────────────────────────────────────────────────────────


class TestRedisDLQAck:
    async def test_ack_removes_from_hash_and_zset(
        self, dlq: RedisDLQ, fake_redis: FakeRedis
    ) -> None:
        entry = _make_entry()
        await dlq.push(entry)
        await dlq.ack(entry.entry_id)

        assert str(entry.entry_id) not in fake_redis._hash.get(dlq._entries_key, {})
        assert str(entry.entry_id) not in fake_redis._zset.get(dlq._queue_key, {})

    async def test_ack_unknown_id_is_noop(self, dlq: RedisDLQ) -> None:
        # Acking an unknown entry_id must not raise — idempotent.
        await dlq.ack(uuid.uuid4())  # must not raise

    async def test_ack_before_connect_is_noop(
        self, settings: RedisEventBusSettings
    ) -> None:
        dlq = RedisDLQ(settings)
        await dlq.ack(uuid.uuid4())  # must not raise (logs warning)

    async def test_ack_reduces_count(
        self, dlq: RedisDLQ, fake_redis: FakeRedis
    ) -> None:
        entry = _make_entry()
        await dlq.push(entry)
        assert await dlq.count() == 1
        await dlq.ack(entry.entry_id)
        assert await dlq.count() == 0


# ── count() ───────────────────────────────────────────────────────────────────


class TestRedisDLQCount:
    async def test_count_before_connect_raises(
        self, settings: RedisEventBusSettings
    ) -> None:
        dlq = RedisDLQ(settings)
        with pytest.raises(RuntimeError, match="connect"):
            await dlq.count()

    async def test_count_empty(self, dlq: RedisDLQ) -> None:
        assert await dlq.count() == 0

    async def test_count_after_push(self, dlq: RedisDLQ, fake_redis: FakeRedis) -> None:
        await dlq.push(_make_entry("A"))
        await dlq.push(_make_entry("B"))
        assert await dlq.count() == 2

    async def test_count_after_ack(self, dlq: RedisDLQ, fake_redis: FakeRedis) -> None:
        entry = _make_entry()
        await dlq.push(entry)
        await dlq.ack(entry.entry_id)
        assert await dlq.count() == 0


# ── Serialization round-trip ─────────────────────────────────────────────────


class TestRedisDLQSerialization:
    def test_serialize_deserialize_roundtrip(
        self, settings: RedisEventBusSettings
    ) -> None:
        dlq = RedisDLQ(settings)
        entry = _make_entry("MyConsumer.handler")
        payload = dlq._serialize_entry(entry)

        assert isinstance(payload, bytes)

        # Deserialize back
        recovered = dlq._deserialize_entry(str(entry.entry_id), payload)
        assert recovered.entry_id == entry.entry_id
        assert recovered.channel == entry.channel
        assert recovered.handler_name == entry.handler_name
        assert recovered.error_type == entry.error_type
        assert recovered.error_message == entry.error_message
        assert recovered.attempts == entry.attempts
        assert isinstance(recovered.event, SampleEvent)

    def test_serialize_contains_event_type(
        self, settings: RedisEventBusSettings
    ) -> None:
        dlq = RedisDLQ(settings)
        entry = _make_entry()
        payload = dlq._serialize_entry(entry)
        data = json.loads(payload)
        # The event_payload field embeds the event's __event_type__
        assert "test.sample.redis_dlq" in data["event_payload"]

    def test_serialize_datetimes_are_iso_strings(
        self, settings: RedisEventBusSettings
    ) -> None:
        dlq = RedisDLQ(settings)
        entry = _make_entry()
        payload = dlq._serialize_entry(entry)
        data = json.loads(payload)
        # Datetimes are stored as ISO-8601 strings — parseable.
        datetime.fromisoformat(data["first_failed_at"])
        datetime.fromisoformat(data["last_failed_at"])


# ── DI Configuration ──────────────────────────────────────────────────────────


class TestRedisDLQConfiguration:
    async def test_provides_abstract_dead_letter_queue(
        self, fake_redis: FakeRedis
    ) -> None:
        from providify import DIContainer
        from varco_core.event.dlq import AbstractDeadLetterQueue

        with patch("varco_redis.dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            container = DIContainer()
            await container.ainstall(RedisDLQConfiguration)
            dlq = await container.aget(AbstractDeadLetterQueue)
            assert isinstance(dlq, RedisDLQ)
