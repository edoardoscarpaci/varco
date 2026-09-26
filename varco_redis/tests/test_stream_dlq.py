"""
tests.test_stream_dlq
======================
Unit and integration tests for ``varco_redis.stream_dlq.RedisStreamDLQ``.

Unit tests use a ``FakeRedis`` test double — no real Redis required.
The integration test spins up a real Redis via testcontainers and tests the
full push → pop_batch → ack flow.

Sections
--------
- ``RedisStreamDLQ`` construction / repr
- lifecycle: connect / disconnect (idempotent, context manager)
- ``push()``         — XADD, never raises, drops if not connected
- ``pop_batch()``    — XREADGROUP, creates group on first call, maps to entries
- ``ack()``          — XACK + XDEL, idempotent on unknown entry_id
- ``count()``        — XLEN, not-connected guard
- serialization      — round-trip of ``DeadLetterEntry`` (including nested Event)
- DI                 — ``RedisStreamDLQConfiguration`` wires settings + DLQ
- integration        — real Redis: push N → pop_batch → ack → pop_batch empty

Integration tests are disabled by default.  Run with::

    pytest -m integration tests/test_stream_dlq.py
    # or
    VARCO_RUN_INTEGRATION=1 pytest tests/test_stream_dlq.py
"""

from __future__ import annotations

import json
import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from varco_core.event import Event
from varco_core.event.dlq import DeadLetterEntry
from varco_redis.config import RedisEventBusSettings
from varco_redis.stream_dlq import RedisStreamDLQ, RedisStreamDLQConfiguration

# ── Minimal event for tests ────────────────────────────────────────────────────


class SampleEvent(Event):
    """Minimal event used only in tests — unique __event_type__ avoids registry clash."""

    __event_type__ = "test.sample.stream_dlq"
    payload: str = "test"


# ── FakeRedis test double ──────────────────────────────────────────────────────


class FakeRedis:
    """
    In-memory fake for redis.asyncio.

    Supports the exact commands used by ``RedisStreamDLQ``:
    ``xadd``, ``xreadgroup``, ``xgroup_create``, ``xack``, ``xdel``,
    ``xlen``, and ``aclose``.

    DESIGN: explicit fake over MagicMock
        ✅ Tests verify actual Redis semantics (e.g. XADD auto-ID ordering).
        ✅ Easier to reason about in assertions.
        ✅ No surprise coroutine wrapping issues from AsyncMock.
        ❌ More code than a simple MagicMock; but the clarity is worth it.
    """

    def __init__(self) -> None:
        # Stream storage: stream_key → [(msg_id_bytes, {field: value}), ...]
        # msg_id is generated as a monotonic string "N-0" where N increments.
        self._streams: dict[str, list[tuple[bytes, dict[bytes, bytes]]]] = {}
        # Consumer group PEL: (stream_key, group) → {msg_id: fields}
        # Tracks messages delivered but not yet XACK'd.
        self._pel: dict[tuple[str, str], dict[bytes, dict[bytes, bytes]]] = {}
        # Counter for auto-generated stream IDs.
        self._id_counter: int = 1
        # Track whether XGROUP CREATE has been called (for test assertions).
        self.xgroup_create_calls: list[dict[str, Any]] = []
        self.xack_calls: list[tuple[Any, ...]] = []
        self.xdel_calls: list[tuple[Any, ...]] = []

    async def xadd(
        self,
        key: str,
        fields: dict[Any, Any],
        id: str = "*",  # noqa: A002 — matches redis-py API
    ) -> bytes:
        """Append a new entry to the stream.  Returns the auto-assigned msg ID."""
        # Generate a monotonic ID — real Redis uses millisecond timestamps.
        # Simple counter is sufficient for unit tests.
        msg_id = f"{self._id_counter}-0".encode()
        self._id_counter += 1

        # Normalize field keys/values to bytes — redis-py returns bytes when
        # decode_responses=False.
        normalized: dict[bytes, bytes] = {
            (k.encode() if isinstance(k, str) else k): (
                v if isinstance(v, bytes) else str(v).encode()
            )
            for k, v in fields.items()
        }

        self._streams.setdefault(key, []).append((msg_id, normalized))
        return msg_id

    async def xgroup_create(
        self,
        key: str,
        group: str,
        id: str = "$",  # noqa: A002
        mkstream: bool = False,
    ) -> bool:
        """Create a consumer group.  Raises ResponseError('BUSYGROUP') if exists."""
        self.xgroup_create_calls.append(
            {"key": key, "group": group, "id": id, "mkstream": mkstream}
        )
        group_key = (key, group)
        if group_key in self._pel:
            # Group already exists — raise BUSYGROUP as real Redis does.
            import redis.asyncio as aioredis  # noqa: PLC0415

            raise aioredis.ResponseError("BUSYGROUP Consumer Group name already exists")
        self._pel[group_key] = {}
        # If mkstream, ensure the stream key exists.
        if mkstream:
            self._streams.setdefault(key, [])
        return True

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        count: int = 10,
        block: int | None = None,
    ) -> list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]]:
        """
        Read new messages for the consumer group.

        Only supports the ``">"`` special ID (read new messages not yet delivered).
        Returns [] if no undelivered messages.
        """
        results = []
        for stream_key, last_id in streams.items():
            if last_id != ">":
                # Only support ">" in these tests — pending re-delivery not tested.
                continue
            group_key = (stream_key, group)
            if group_key not in self._pel:
                # Group doesn't exist yet — real Redis would error, but in tests
                # we return empty (the group is created before xreadgroup is called).
                continue

            stream_entries = self._streams.get(stream_key, [])
            pel = self._pel[group_key]

            # Collect messages not yet in the PEL for this group.
            delivered: list[tuple[bytes, dict[bytes, bytes]]] = []
            for msg_id, fields in stream_entries:
                if msg_id not in pel and len(delivered) < count:
                    pel[msg_id] = fields
                    delivered.append((msg_id, fields))

            if delivered:
                results.append((stream_key.encode(), delivered))

        return results

    async def xack(self, key: str, group: str, *msg_ids: bytes) -> int:
        """Remove messages from the PEL."""
        self.xack_calls.append((key, group, *msg_ids))
        group_key = (key, group)
        pel = self._pel.get(group_key, {})
        removed = 0
        for msg_id in msg_ids:
            if msg_id in pel:
                del pel[msg_id]
                removed += 1
        return removed

    async def xdel(self, key: str, *msg_ids: bytes) -> int:
        """Remove messages from the stream."""
        self.xdel_calls.append((key, *msg_ids))
        stream = self._streams.get(key, [])
        before = len(stream)
        self._streams[key] = [(mid, f) for mid, f in stream if mid not in msg_ids]
        return before - len(self._streams[key])

    async def xlen(self, key: str) -> int:
        """Return the number of entries in the stream."""
        return len(self._streams.get(key, []))

    async def aclose(self) -> None:
        """No-op for the fake."""


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def settings() -> RedisEventBusSettings:
    return RedisEventBusSettings(url="redis://fake:6379/0")


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
async def dlq(settings: RedisEventBusSettings, fake_redis: FakeRedis) -> RedisStreamDLQ:
    """Connected ``RedisStreamDLQ`` with a fake Redis client."""
    with patch("varco_redis.stream_dlq.aioredis") as mock_aioredis:
        mock_aioredis.from_url.return_value = fake_redis
        d = RedisStreamDLQ(settings)
        await d.connect()
        yield d
        await d.disconnect()


def _make_entry(handler_name: str = "H.handle") -> DeadLetterEntry:
    """Build a minimal ``DeadLetterEntry`` for tests."""
    return DeadLetterEntry(
        event=SampleEvent(),
        channel="orders",
        handler_name=handler_name,
        error_type="ValueError",
        error_message="something went wrong",
        attempts=3,
    )


# ── Construction and repr ─────────────────────────────────────────────────────


class TestRedisStreamDLQConstruction:
    def test_repr_contains_class_name(self, settings: RedisEventBusSettings) -> None:
        dlq = RedisStreamDLQ(settings)
        assert "RedisStreamDLQ" in repr(dlq)

    def test_repr_shows_disconnected(self, settings: RedisEventBusSettings) -> None:
        dlq = RedisStreamDLQ(settings)
        assert "connected=False" in repr(dlq)

    def test_repr_shows_url(self, settings: RedisEventBusSettings) -> None:
        dlq = RedisStreamDLQ(settings)
        assert "fake" in repr(dlq)

    def test_repr_shows_stream_key(self, settings: RedisEventBusSettings) -> None:
        dlq = RedisStreamDLQ(settings)
        assert "dlq:stream" in repr(dlq)

    def test_default_settings_reads_from_env(self) -> None:
        """Constructing without arguments must not raise — defaults to from_env()."""
        dlq = RedisStreamDLQ()
        assert dlq is not None

    def test_custom_group(self, settings: RedisEventBusSettings) -> None:
        dlq = RedisStreamDLQ(settings, group="my-relay")
        assert dlq._group == "my-relay"

    def test_custom_consumer(self, settings: RedisEventBusSettings) -> None:
        dlq = RedisStreamDLQ(settings, consumer="replica-1")
        assert dlq._consumer == "replica-1"


# ── Lifecycle ─────────────────────────────────────────────────────────────────


class TestRedisStreamDLQLifecycle:
    async def test_connect_creates_redis_client(
        self, settings: RedisEventBusSettings, fake_redis: FakeRedis
    ) -> None:
        """connect() instantiates the Redis client via from_url."""
        with patch("varco_redis.stream_dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            dlq = RedisStreamDLQ(settings)
            assert dlq._redis is None
            await dlq.connect()
            assert dlq._redis is not None
            mock_aioredis.from_url.assert_called_once()
            await dlq.disconnect()

    async def test_connect_is_idempotent(
        self, settings: RedisEventBusSettings, fake_redis: FakeRedis
    ) -> None:
        """Calling connect() twice only creates one Redis client."""
        with patch("varco_redis.stream_dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            dlq = RedisStreamDLQ(settings)
            await dlq.connect()
            await dlq.connect()  # second call should be a no-op
            mock_aioredis.from_url.assert_called_once()
            await dlq.disconnect()

    async def test_disconnect_clears_client(self, dlq: RedisStreamDLQ) -> None:
        """disconnect() sets _redis back to None."""
        assert dlq._redis is not None
        await dlq.disconnect()
        assert dlq._redis is None

    async def test_disconnect_before_connect_is_noop(self, settings: RedisEventBusSettings) -> None:
        """disconnect() before connect() must not raise."""
        dlq = RedisStreamDLQ(settings)
        # Should not raise
        await dlq.disconnect()

    async def test_context_manager(
        self, settings: RedisEventBusSettings, fake_redis: FakeRedis
    ) -> None:
        """async with RedisStreamDLQ(...) as dlq: connects and disconnects."""
        with patch("varco_redis.stream_dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisStreamDLQ(settings) as dlq:
                assert dlq._redis is not None
            assert dlq._redis is None

    async def test_disconnect_clears_pending_map(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """disconnect() clears the in-memory _pending map."""
        # Push and pop to populate the pending map.
        await dlq.push(_make_entry())
        await dlq.pop_batch(limit=1)
        assert len(dlq._pending) == 1
        await dlq.disconnect()
        assert len(dlq._pending) == 0


# ── push() ────────────────────────────────────────────────────────────────────


class TestRedisStreamDLQPush:
    async def test_push_adds_entry_to_stream(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """push() calls XADD with the correct stream key."""
        entry = _make_entry()
        await dlq.push(entry)
        # One entry should be in the stream.
        assert len(fake_redis._streams.get(dlq._stream_key, [])) == 1

    async def test_push_serializes_entry_id(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """The payload stored in the stream contains the entry_id as a UUID string."""
        entry = _make_entry()
        await dlq.push(entry)

        stream_entries = fake_redis._streams.get(dlq._stream_key, [])
        assert len(stream_entries) == 1
        _msg_id, fields = stream_entries[0]
        payload = fields[b"payload"]
        data = json.loads(payload)
        assert data["entry_id"] == str(entry.entry_id)
        assert data["handler_name"] == "H.handle"
        assert data["channel"] == "orders"

    async def test_push_multiple_entries_appends(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """Each push() appends a new stream entry."""
        await dlq.push(_make_entry("H.a"))
        await dlq.push(_make_entry("H.b"))
        await dlq.push(_make_entry("H.c"))
        assert len(fake_redis._streams.get(dlq._stream_key, [])) == 3

    async def test_push_never_raises_on_redis_error(self, settings: RedisEventBusSettings) -> None:
        """push() swallows all exceptions — even Redis errors."""
        broken_redis = MagicMock()
        broken_redis.xadd = AsyncMock(side_effect=OSError("connection reset"))

        with patch("varco_redis.stream_dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = broken_redis
            dlq = RedisStreamDLQ(settings)
            await dlq.connect()
            # Must not raise
            await dlq.push(_make_entry())

    async def test_push_never_raises_on_serialization_error(
        self, settings: RedisEventBusSettings, fake_redis: FakeRedis
    ) -> None:
        """push() swallows serialization errors too."""
        with patch("varco_redis.stream_dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            dlq = RedisStreamDLQ(settings)
            await dlq.connect()

            # Patch serializer to raise
            dlq._serializer.serialize = MagicMock(side_effect=ValueError("bad event"))
            # Must not raise
            await dlq.push(_make_entry())

    async def test_push_drops_when_not_connected(self, settings: RedisEventBusSettings) -> None:
        """push() before connect() logs a warning and drops the entry silently."""
        dlq = RedisStreamDLQ(settings)
        # Must not raise even though _redis is None
        await dlq.push(_make_entry())


# ── pop_batch() ───────────────────────────────────────────────────────────────


class TestRedisStreamDLQPopBatch:
    async def test_pop_batch_creates_consumer_group_on_first_call(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """pop_batch() calls XGROUP CREATE on first invocation."""
        assert not dlq._group_created
        await dlq.pop_batch(limit=1)
        assert dlq._group_created
        # xgroup_create_calls should contain one call.
        assert len(fake_redis.xgroup_create_calls) == 1
        call = fake_redis.xgroup_create_calls[0]
        assert call["key"] == dlq._stream_key
        assert call["group"] == dlq._group
        assert call["mkstream"] is True

    async def test_pop_batch_creates_group_only_once(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """Subsequent pop_batch() calls do NOT re-create the consumer group."""
        await dlq.pop_batch(limit=1)
        await dlq.pop_batch(limit=1)
        await dlq.pop_batch(limit=1)
        # xgroup_create should only be called once.
        assert len(fake_redis.xgroup_create_calls) == 1

    async def test_pop_batch_returns_mapped_entries(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """pop_batch() deserializes stream entries into DeadLetterEntry objects."""
        entry_a = _make_entry("H.a")
        entry_b = _make_entry("H.b")
        await dlq.push(entry_a)
        await dlq.push(entry_b)

        results = await dlq.pop_batch(limit=10)
        assert len(results) == 2
        handler_names = {e.handler_name for e in results}
        assert handler_names == {"H.a", "H.b"}

    async def test_pop_batch_returns_correct_entry_fields(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """Deserialized entries have correct field values."""
        entry = _make_entry()
        await dlq.push(entry)

        results = await dlq.pop_batch(limit=1)
        assert len(results) == 1
        deserialized = results[0]
        assert deserialized.handler_name == entry.handler_name
        assert deserialized.channel == entry.channel
        assert deserialized.error_type == entry.error_type
        assert deserialized.error_message == entry.error_message
        assert deserialized.attempts == entry.attempts

    async def test_pop_batch_respects_limit(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """pop_batch() returns at most ``limit`` entries."""
        for i in range(5):
            await dlq.push(_make_entry(f"H.{i}"))

        results = await dlq.pop_batch(limit=3)
        assert len(results) <= 3

    async def test_pop_batch_raises_on_invalid_limit(self, dlq: RedisStreamDLQ) -> None:
        """pop_batch() raises ValueError when limit < 1."""
        with pytest.raises(ValueError, match="limit must be"):
            await dlq.pop_batch(limit=0)

    async def test_pop_batch_raises_when_not_connected(
        self, settings: RedisEventBusSettings
    ) -> None:
        """pop_batch() raises RuntimeError when called before connect()."""
        dlq = RedisStreamDLQ(settings)
        with pytest.raises(RuntimeError, match="connect()"):
            await dlq.pop_batch(limit=1)

    async def test_pop_batch_returns_empty_on_empty_stream(self, dlq: RedisStreamDLQ) -> None:
        """pop_batch() returns empty list when no messages are available."""
        results = await dlq.pop_batch(limit=10)
        assert results == []

    async def test_pop_batch_populates_pending_map(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """After pop_batch(), entry_id → stream msg_id mapping is in _pending."""
        entry = _make_entry()
        await dlq.push(entry)
        results = await dlq.pop_batch(limit=1)
        assert len(results) == 1
        # The entry's UUID must be in the pending map.
        assert results[0].entry_id in dlq._pending

    async def test_pop_batch_handles_busygroup_on_group_exists(
        self, settings: RedisEventBusSettings, fake_redis: FakeRedis
    ) -> None:
        """pop_batch() suppresses BUSYGROUP ResponseError (group already exists)."""
        with patch("varco_redis.stream_dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            # Pre-create the group so FakeRedis raises BUSYGROUP.
            import redis.asyncio as aioredis  # noqa: PLC0415

            mock_aioredis.ResponseError = aioredis.ResponseError
            dlq = RedisStreamDLQ(settings)
            await dlq.connect()
            # Create group manually first — next call should get BUSYGROUP.
            await fake_redis.xgroup_create(dlq._stream_key, dlq._group, mkstream=True)
            # Reset the flag so pop_batch tries to create again.
            dlq._group_created = False
            # Must not raise.
            await dlq.pop_batch(limit=1)
            assert dlq._group_created


# ── ack() ─────────────────────────────────────────────────────────────────────


class TestRedisStreamDLQAck:
    async def test_ack_calls_xack_and_xdel(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """ack() calls both XACK and XDEL with the correct stream message ID."""
        entry = _make_entry()
        await dlq.push(entry)
        results = await dlq.pop_batch(limit=1)
        assert len(results) == 1

        entry_id = results[0].entry_id
        stream_msg_id = dlq._pending[entry_id]

        await dlq.ack(entry_id)

        # XACK must have been called with the stream msg ID.
        assert len(fake_redis.xack_calls) == 1
        ack_args = fake_redis.xack_calls[0]
        assert ack_args[0] == dlq._stream_key
        assert ack_args[1] == dlq._group
        assert stream_msg_id in ack_args

        # XDEL must have been called with the stream msg ID.
        assert len(fake_redis.xdel_calls) == 1
        del_args = fake_redis.xdel_calls[0]
        assert del_args[0] == dlq._stream_key
        assert stream_msg_id in del_args

    async def test_ack_removes_entry_from_pending_map(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """After ack(), the entry_id is removed from _pending."""
        await dlq.push(_make_entry())
        results = await dlq.pop_batch(limit=1)
        entry_id = results[0].entry_id

        await dlq.ack(entry_id)
        assert entry_id not in dlq._pending

    async def test_ack_removes_entry_from_stream(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """After ack(), the stream is empty (XDEL removed the entry)."""
        await dlq.push(_make_entry())
        results = await dlq.pop_batch(limit=1)
        await dlq.ack(results[0].entry_id)

        # Stream should be empty after XDEL.
        assert len(fake_redis._streams.get(dlq._stream_key, [])) == 0

    async def test_ack_is_idempotent_on_unknown_entry_id(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """ack() with an unknown entry_id is a silent no-op — does NOT raise."""
        unknown_id = uuid.uuid4()
        # Must not raise.
        await dlq.ack(unknown_id)
        # No XACK or XDEL should have been called.
        assert len(fake_redis.xack_calls) == 0
        assert len(fake_redis.xdel_calls) == 0

    async def test_ack_when_not_connected_is_noop(self, settings: RedisEventBusSettings) -> None:
        """ack() before connect() logs a warning and does not raise."""
        dlq = RedisStreamDLQ(settings)
        # Must not raise.
        await dlq.ack(uuid.uuid4())


# ── count() ───────────────────────────────────────────────────────────────────


class TestRedisStreamDLQCount:
    async def test_count_returns_zero_on_empty_stream(self, dlq: RedisStreamDLQ) -> None:
        assert await dlq.count() == 0

    async def test_count_returns_correct_count_after_push(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        await dlq.push(_make_entry())
        await dlq.push(_make_entry())
        assert await dlq.count() == 2

    async def test_count_decreases_after_ack(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """count() decreases after ack() (XDEL removes entries from stream)."""
        await dlq.push(_make_entry())
        await dlq.push(_make_entry())
        results = await dlq.pop_batch(limit=2)
        for entry in results:
            await dlq.ack(entry.entry_id)
        assert await dlq.count() == 0

    async def test_count_raises_when_not_connected(self, settings: RedisEventBusSettings) -> None:
        dlq = RedisStreamDLQ(settings)
        with pytest.raises(RuntimeError, match="connect()"):
            await dlq.count()


# ── Serialization round-trip ──────────────────────────────────────────────────


class TestRedisStreamDLQSerialization:
    async def test_roundtrip_preserves_all_fields(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """Serialize + deserialize yields an entry identical to the original."""
        original = _make_entry("MyConsumer.on_event")
        await dlq.push(original)
        results = await dlq.pop_batch(limit=1)
        assert len(results) == 1
        restored = results[0]

        # All metadata fields must be preserved.
        assert restored.handler_name == original.handler_name
        assert restored.channel == original.channel
        assert restored.error_type == original.error_type
        assert restored.error_message == original.error_message
        assert restored.attempts == original.attempts

        # Datetime fields — compare to second precision to avoid float drift.
        assert (
            restored.first_failed_at.isoformat()[:19] == original.first_failed_at.isoformat()[:19]
        )
        assert restored.last_failed_at.isoformat()[:19] == original.last_failed_at.isoformat()[:19]

    async def test_roundtrip_preserves_nested_event(
        self, dlq: RedisStreamDLQ, fake_redis: FakeRedis
    ) -> None:
        """The nested Event is deserialized to the correct subclass."""
        entry = DeadLetterEntry(
            event=SampleEvent(payload="hello-world"),
            channel="ch",
            handler_name="X.y",
        )
        await dlq.push(entry)
        results = await dlq.pop_batch(limit=1)
        restored_event = results[0].event
        assert isinstance(restored_event, SampleEvent)
        assert restored_event.payload == "hello-world"  # type: ignore[union-attr]


# ── DI Configuration ──────────────────────────────────────────────────────────


class TestRedisStreamDLQConfiguration:
    async def test_configuration_provides_abstract_dlq(
        self,
        settings: RedisEventBusSettings,
        fake_redis: FakeRedis,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """RedisStreamDLQConfiguration wires a connected RedisStreamDLQ."""
        from providify import DIContainer  # noqa: PLC0415
        from varco_core.event.dlq import AbstractDeadLetterQueue  # noqa: PLC0415

        # Plan 047 §D-tests: "redis-streams" is a secondary DLQ value.
        monkeypatch.setenv("VARCO_DLQ_BACKEND", "redis-streams")
        with patch("varco_redis.stream_dlq.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            container = DIContainer()
            await container.ainstall(RedisStreamDLQConfiguration)

            dlq = await container.aget(AbstractDeadLetterQueue)
            assert isinstance(dlq, RedisStreamDLQ)
            assert dlq._redis is not None  # connected


# ── Integration test ──────────────────────────────────────────────────────────

# Disabled by default; only runs when VARCO_RUN_INTEGRATION=1 or -m integration.
import os as _os  # noqa: E402

if not _os.environ.get("VARCO_RUN_INTEGRATION"):
    pytestmark_integration = pytest.mark.skip(
        reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1"
    )
else:
    pytestmark_integration = pytest.mark.integration


@pytest.mark.integration
class TestRedisStreamDLQIntegration:
    """
    End-to-end integration tests using a real Redis instance via testcontainers.

    Prerequisites:
        - Docker daemon running
        - testcontainers[redis] installed (dev dependency)

    Run with::

        pytest -m integration tests/test_stream_dlq.py
    """

    # The local, class-scoped redis_container fixture was replaced by the
    # session-scoped redis_url fixture in tests/conftest.py (Plan 012 / RT1,
    # Step 6).

    @pytest.fixture
    async def real_dlq(self, redis_url: str) -> RedisStreamDLQ:
        """Connected ``RedisStreamDLQ`` backed by the shared session-scoped
        Redis container."""
        # Unique prefix per test run to prevent cross-test interference.
        prefix = f"inttest:{uuid.uuid4().hex[:8]}:"

        settings = RedisEventBusSettings(url=redis_url, channel_prefix=prefix)
        async with RedisStreamDLQ(settings) as dlq:
            yield dlq

    async def test_push_pop_ack_round_trip(self, real_dlq: RedisStreamDLQ) -> None:
        """
        Full round-trip: push N entries → pop_batch → ack all → pop_batch empty.
        """
        n = 5
        pushed: list[DeadLetterEntry] = []
        for i in range(n):
            entry = DeadLetterEntry(
                event=SampleEvent(payload=f"payload-{i}"),
                channel="orders",
                handler_name=f"Handler.method_{i}",
                error_type="RuntimeError",
                error_message=f"error {i}",
                attempts=i + 1,
            )
            await real_dlq.push(entry)
            pushed.append(entry)

        assert await real_dlq.count() == n

        # Pop all entries.
        results = await real_dlq.pop_batch(limit=n + 1)
        assert len(results) == n

        # Verify each entry round-trips correctly.
        handler_names = {e.handler_name for e in results}
        expected_names = {f"Handler.method_{i}" for i in range(n)}
        assert handler_names == expected_names

        # Ack all entries.
        for entry in results:
            await real_dlq.ack(entry.entry_id)

        # After ack, count should be 0 and pop_batch should return empty.
        assert await real_dlq.count() == 0
        final_results = await real_dlq.pop_batch(limit=10)
        assert final_results == []

    async def test_push_never_raises_on_real_redis(self, real_dlq: RedisStreamDLQ) -> None:
        """push() against a real Redis instance must not raise."""
        entry = _make_entry()
        # Must not raise.
        await real_dlq.push(entry)

    async def test_ack_idempotent_on_real_redis(self, real_dlq: RedisStreamDLQ) -> None:
        """ack() with an unknown entry_id is a no-op even against a real Redis."""
        # Must not raise.
        await real_dlq.ack(uuid.uuid4())
