"""
Unit tests for varco_kafka.dlq
================================
Covers ``KafkaDLQ`` — the Kafka-backed dead letter queue.

All tests mock ``AIOKafkaProducer`` and ``AIOKafkaConsumer`` — no real Kafka
broker required.

Sections
--------
- ``KafkaDLQ`` construction / repr
- lifecycle: start/stop (idempotent, context manager)
- ``count()``       — always returns -1
- ``push()``        — never raises, drops if not started, sends to DLQ topic
- ``pop_batch()``   — limit guard, not-started guard, deserializes entries
- ``ack()``         — commits offset via consumer, noop for unknown id
- serialization    — round-trip of DeadLetterEntry (including nested Event)
- DI               — ``KafkaDLQConfiguration`` wires settings + DLQ
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime
from typing import Any
from unittest.mock import patch

import pytest
from aiokafka.structs import TopicPartition

from varco_core.event import Event
from varco_core.event.dlq import DeadLetterEntry
from varco_kafka.config import KafkaEventBusSettings
from varco_kafka.dlq import KafkaDLQ, KafkaDLQConfiguration


# ── Minimal event for tests ────────────────────────────────────────────────────


class SampleEvent(Event):
    __event_type__ = "test.sample.kafka_dlq"
    value: str = "test"


# ── FakeProducer ─────────────────────────────────────────────────────────────


class FakeProducer:
    """
    Fake AIOKafkaProducer that records send_and_wait calls.

    Attributes:
        sent: List of (topic, value, key) tuples in send order.
        fail: If True, send_and_wait raises RuntimeError.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.sent: list[tuple[str, bytes, bytes]] = []
        self.fail: bool = False
        self._started = False

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def send_and_wait(self, topic: str, *, value: bytes, key: bytes) -> None:
        if self.fail:
            raise RuntimeError("producer error")
        self.sent.append((topic, value, key))


# ── FakeConsumer ─────────────────────────────────────────────────────────────


class FakeRecord:
    """Simulates an aiokafka ConsumerRecord."""

    def __init__(self, topic: str, partition: int, offset: int, value: bytes) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.value = value


class FakeConsumer:
    """
    Fake AIOKafkaConsumer for getmany() and commit().

    Attributes:
        queued_records: Records to return from the first getmany() call.
        committed:      List of offset dicts passed to commit().
    """

    def __init__(self, **kwargs: Any) -> None:
        self._started = False
        self._queued: list[FakeRecord] = []
        self.committed: list[dict] = []

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def getmany(
        self, timeout_ms: int = 500, max_records: int = 10
    ) -> dict[TopicPartition, list[FakeRecord]]:
        if not self._queued:
            return {}
        # Return all queued records under a single TopicPartition.
        tp = TopicPartition(topic="__dlq__", partition=0)
        records = self._queued[:max_records]
        self._queued = self._queued[max_records:]
        return {tp: records}

    async def commit(self, offsets: dict) -> None:
        self.committed.append(offsets)


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def settings() -> KafkaEventBusSettings:
    return KafkaEventBusSettings(bootstrap_servers="fake:9092", group_id="test-group")


@pytest.fixture
def fake_producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture
def fake_consumer() -> FakeConsumer:
    return FakeConsumer()


@pytest.fixture
async def dlq(
    settings: KafkaEventBusSettings,
    fake_producer: FakeProducer,
    fake_consumer: FakeConsumer,
) -> KafkaDLQ:
    """Started ``KafkaDLQ`` with fake producer and consumer."""
    with (
        patch("varco_kafka.dlq.AIOKafkaProducer", return_value=fake_producer),
        patch("varco_kafka.dlq.AIOKafkaConsumer", return_value=fake_consumer),
    ):
        dlq = KafkaDLQ(settings)
        await dlq.start()
        yield dlq
        await dlq.stop()


def _make_entry(handler_name: str = "H.handle") -> DeadLetterEntry:
    return DeadLetterEntry(
        event=SampleEvent(),
        channel="orders",
        handler_name=handler_name,
        error_type="ValueError",
        error_message="test error",
        attempts=2,
    )


# ── Construction and repr ─────────────────────────────────────────────────────


class TestKafkaDLQConstruction:
    def test_repr_contains_class_name(self, settings: KafkaEventBusSettings) -> None:
        dlq = KafkaDLQ(settings)
        assert "KafkaDLQ" in repr(dlq)

    def test_repr_shows_not_started(self, settings: KafkaEventBusSettings) -> None:
        dlq = KafkaDLQ(settings)
        assert "started=False" in repr(dlq)

    def test_default_dlq_topic_uses_prefix(
        self, settings: KafkaEventBusSettings
    ) -> None:
        # Default DLQ topic = "{prefix}__dlq__"
        dlq = KafkaDLQ(settings)
        assert dlq._dlq_topic == "__dlq__"

    def test_custom_dlq_topic_not_prefixed(
        self, settings: KafkaEventBusSettings
    ) -> None:
        # When dlq_topic is provided explicitly, no prefix is prepended.
        dlq = KafkaDLQ(settings, dlq_topic="custom-dlq")
        assert dlq._dlq_topic == "custom-dlq"

    def test_custom_consumer_group(self, settings: KafkaEventBusSettings) -> None:
        dlq = KafkaDLQ(settings, dlq_consumer_group="my-relay-group")
        assert dlq._dlq_consumer_group == "my-relay-group"

    def test_prefix_applied_to_default_topic(self) -> None:
        settings = KafkaEventBusSettings(
            bootstrap_servers="fake:9092", channel_prefix="prod."
        )
        dlq = KafkaDLQ(settings)
        assert dlq._dlq_topic == "prod.__dlq__"


# ── Lifecycle ─────────────────────────────────────────────────────────────────


class TestKafkaDLQLifecycle:
    async def test_start_sets_started(
        self, settings: KafkaEventBusSettings, fake_producer: FakeProducer
    ) -> None:
        with patch("varco_kafka.dlq.AIOKafkaProducer", return_value=fake_producer):
            d = KafkaDLQ(settings)
            await d.start()
            assert d._started is True
            await d.stop()

    async def test_start_idempotent(
        self, settings: KafkaEventBusSettings, fake_producer: FakeProducer
    ) -> None:
        with patch("varco_kafka.dlq.AIOKafkaProducer", return_value=fake_producer):
            d = KafkaDLQ(settings)
            await d.start()
            await d.start()  # second call is no-op
            assert d._started is True
            await d.stop()

    async def test_stop_before_start_is_noop(
        self, settings: KafkaEventBusSettings
    ) -> None:
        d = KafkaDLQ(settings)
        await d.stop()  # must not raise

    async def test_stop_clears_started(self, dlq: KafkaDLQ) -> None:
        await dlq.stop()
        assert dlq._started is False

    async def test_context_manager(
        self, settings: KafkaEventBusSettings, fake_producer: FakeProducer
    ) -> None:
        with patch("varco_kafka.dlq.AIOKafkaProducer", return_value=fake_producer):
            async with KafkaDLQ(settings) as d:
                assert d._started is True
            assert d._started is False

    async def test_repr_shows_started(self, dlq: KafkaDLQ) -> None:
        assert "started=True" in repr(dlq)


# ── count() ───────────────────────────────────────────────────────────────────


class TestKafkaDLQCount:
    async def test_count_always_returns_minus_one(self, dlq: KafkaDLQ) -> None:
        # Kafka cannot compute consumer lag without AdminClient — documented -1.
        assert await dlq.count() == -1


# ── push() ────────────────────────────────────────────────────────────────────


class TestKafkaDLQPush:
    async def test_push_sends_to_dlq_topic(
        self, dlq: KafkaDLQ, fake_producer: FakeProducer
    ) -> None:
        entry = _make_entry()
        await dlq.push(entry)

        assert len(fake_producer.sent) == 1
        topic, value, key = fake_producer.sent[0]
        assert topic == dlq._dlq_topic

    async def test_push_key_is_entry_id(
        self, dlq: KafkaDLQ, fake_producer: FakeProducer
    ) -> None:
        entry = _make_entry()
        await dlq.push(entry)

        _, _, key = fake_producer.sent[0]
        assert key == str(entry.entry_id).encode("utf-8")

    async def test_push_never_raises_even_on_producer_failure(
        self, settings: KafkaEventBusSettings
    ) -> None:
        failing_producer = FakeProducer()
        failing_producer.fail = True
        with patch("varco_kafka.dlq.AIOKafkaProducer", return_value=failing_producer):
            d = KafkaDLQ(settings)
            await d.start()
            await d.push(_make_entry())  # must NOT raise
            await d.stop()

    async def test_push_before_start_is_noop(
        self, settings: KafkaEventBusSettings, fake_producer: FakeProducer
    ) -> None:
        d = KafkaDLQ(settings)
        await d.push(_make_entry())  # must not raise (producer is None)
        assert len(fake_producer.sent) == 0

    async def test_push_value_is_json_bytes(
        self, dlq: KafkaDLQ, fake_producer: FakeProducer
    ) -> None:
        entry = _make_entry("OrderConsumer.on_order")
        await dlq.push(entry)

        _, value, _ = fake_producer.sent[0]
        data = json.loads(value.decode("utf-8"))
        assert data["handler_name"] == "OrderConsumer.on_order"
        assert data["error_type"] == "ValueError"


# ── pop_batch() ───────────────────────────────────────────────────────────────


class TestKafkaDLQPopBatch:
    async def test_pop_batch_limit_below_one_raises(self, dlq: KafkaDLQ) -> None:
        with pytest.raises(ValueError, match="limit"):
            await dlq.pop_batch(limit=0)

    async def test_pop_batch_before_start_raises(
        self, settings: KafkaEventBusSettings
    ) -> None:
        d = KafkaDLQ(settings)
        with pytest.raises(RuntimeError, match="start"):
            await d.pop_batch()

    async def test_pop_batch_empty_returns_empty_list(
        self, dlq: KafkaDLQ, fake_consumer: FakeConsumer
    ) -> None:
        # Consumer is lazily created — inject it before pop_batch.
        dlq._consumer = fake_consumer
        result = await dlq.pop_batch()
        assert result == []

    async def test_pop_batch_deserializes_entry(
        self, settings: KafkaEventBusSettings, fake_producer: FakeProducer
    ) -> None:
        """Push to a DLQ, then manually feed the serialized bytes to a fake consumer."""
        consumer = FakeConsumer()
        with (
            patch("varco_kafka.dlq.AIOKafkaProducer", return_value=fake_producer),
            patch("varco_kafka.dlq.AIOKafkaConsumer", return_value=consumer),
        ):
            d = KafkaDLQ(settings)
            await d.start()

            entry = _make_entry("MyConsumer.handle")
            payload = d._serialize_entry(entry)

            # Inject the serialized payload as a fake consumer record.
            consumer._queued.append(
                FakeRecord(topic="__dlq__", partition=0, offset=0, value=payload)
            )
            d._consumer = consumer

            result = await d.pop_batch(limit=10)
            assert len(result) == 1
            assert result[0].handler_name == "MyConsumer.handle"
            assert isinstance(result[0].event, SampleEvent)
            await d.stop()

    async def test_pop_batch_tracks_inflight(
        self, settings: KafkaEventBusSettings, fake_producer: FakeProducer
    ) -> None:
        consumer = FakeConsumer()
        with (
            patch("varco_kafka.dlq.AIOKafkaProducer", return_value=fake_producer),
            patch("varco_kafka.dlq.AIOKafkaConsumer", return_value=consumer),
        ):
            d = KafkaDLQ(settings)
            await d.start()

            entry = _make_entry()
            payload = d._serialize_entry(entry)
            consumer._queued.append(
                FakeRecord(topic="__dlq__", partition=0, offset=5, value=payload)
            )
            d._consumer = consumer

            result = await d.pop_batch(limit=10)
            # Entry must be tracked in _in_flight for subsequent ack().
            assert str(result[0].entry_id) in d._in_flight
            await d.stop()


# ── ack() ─────────────────────────────────────────────────────────────────────


class TestKafkaDLQAck:
    async def test_ack_unknown_id_is_noop(self, dlq: KafkaDLQ) -> None:
        # Acking an entry_id not returned by pop_batch must be silent.
        await dlq.ack(uuid.uuid4())  # must not raise

    async def test_ack_commits_offset(
        self, settings: KafkaEventBusSettings, fake_producer: FakeProducer
    ) -> None:
        consumer = FakeConsumer()
        with (
            patch("varco_kafka.dlq.AIOKafkaProducer", return_value=fake_producer),
            patch("varco_kafka.dlq.AIOKafkaConsumer", return_value=consumer),
        ):
            d = KafkaDLQ(settings)
            await d.start()

            entry = _make_entry()
            payload = d._serialize_entry(entry)
            consumer._queued.append(
                FakeRecord(topic="__dlq__", partition=0, offset=7, value=payload)
            )
            d._consumer = consumer

            result = await d.pop_batch(limit=1)
            await d.ack(result[0].entry_id)

            # Offset committed = original_offset + 1
            assert len(consumer.committed) == 1
            committed_offset = list(consumer.committed[0].values())[0]
            assert committed_offset == 8  # offset 7 + 1

            await d.stop()

    async def test_ack_removes_from_inflight(
        self, settings: KafkaEventBusSettings, fake_producer: FakeProducer
    ) -> None:
        consumer = FakeConsumer()
        with (
            patch("varco_kafka.dlq.AIOKafkaProducer", return_value=fake_producer),
            patch("varco_kafka.dlq.AIOKafkaConsumer", return_value=consumer),
        ):
            d = KafkaDLQ(settings)
            await d.start()

            entry = _make_entry()
            payload = d._serialize_entry(entry)
            consumer._queued.append(
                FakeRecord(topic="__dlq__", partition=0, offset=0, value=payload)
            )
            d._consumer = consumer

            result = await d.pop_batch(limit=1)
            await d.ack(result[0].entry_id)

            assert str(result[0].entry_id) not in d._in_flight
            await d.stop()


# ── Serialization round-trip ─────────────────────────────────────────────────


class TestKafkaDLQSerialization:
    def test_serialize_deserialize_roundtrip(
        self, settings: KafkaEventBusSettings
    ) -> None:
        dlq = KafkaDLQ(settings)
        entry = _make_entry("MyConsumer.handler")
        payload = dlq._serialize_entry(entry)

        assert isinstance(payload, bytes)

        recovered = dlq._deserialize_entry(payload)
        assert recovered.entry_id == entry.entry_id
        assert recovered.channel == entry.channel
        assert recovered.handler_name == entry.handler_name
        assert recovered.error_type == entry.error_type
        assert recovered.error_message == entry.error_message
        assert recovered.attempts == entry.attempts
        assert isinstance(recovered.event, SampleEvent)

    def test_serialize_contains_event_payload(
        self, settings: KafkaEventBusSettings
    ) -> None:
        dlq = KafkaDLQ(settings)
        entry = _make_entry()
        payload = dlq._serialize_entry(entry)
        data = json.loads(payload)
        assert "event_payload" in data
        assert "test.sample.kafka_dlq" in data["event_payload"]

    def test_serialize_datetimes_are_iso(self, settings: KafkaEventBusSettings) -> None:
        dlq = KafkaDLQ(settings)
        entry = _make_entry()
        payload = dlq._serialize_entry(entry)
        data = json.loads(payload)
        datetime.fromisoformat(data["first_failed_at"])
        datetime.fromisoformat(data["last_failed_at"])


# ── DI Configuration ──────────────────────────────────────────────────────────


class TestKafkaDLQConfiguration:
    async def test_provides_abstract_dead_letter_queue(
        self, fake_producer: FakeProducer
    ) -> None:
        from providify import DIContainer
        from varco_core.event.dlq import AbstractDeadLetterQueue

        with patch("varco_kafka.dlq.AIOKafkaProducer", return_value=fake_producer):
            container = DIContainer()
            await container.ainstall(KafkaDLQConfiguration)
            dlq = await container.aget(AbstractDeadLetterQueue)
            assert isinstance(dlq, KafkaDLQ)
