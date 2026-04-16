"""
tests.test_kafka_eos
=====================
Unit tests for ``KafkaEventBus`` delivery semantics.

Covers all three ``KafkaDeliverySemantics`` modes:
  AT_MOST_ONCE   — offset committed before dispatch
  AT_LEAST_ONCE  — auto-commit after dispatch (default)
  EXACTLY_ONCE   — dispatch + offset commit inside a Kafka transaction

Uses the same ``FakeProducer`` / ``FakeConsumer`` pattern as ``test_kafka_bus.py``,
extended with transaction / commit capabilities.

No real Kafka broker required.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any

import pytest

from varco_core.event import Event
from varco_core.event.serializer import JsonEventSerializer
from varco_kafka import KafkaDeliverySemantics, KafkaEventBus, KafkaEventBusSettings


# ── Test event ─────────────────────────────────────────────────────────────────


class PaymentEvent(Event):
    __event_type__ = "test.eos.payment"
    amount: float = 0.0


# ── Extended fakes ─────────────────────────────────────────────────────────────


class FakeMessage:
    def __init__(
        self, topic: str, value: bytes, partition: int = 0, offset: int = 0
    ) -> None:
        self.topic = topic
        self.value = value
        self.partition = partition
        self.offset = offset


class FakeTransaction:
    """Context manager returned by FakeProducer.transaction()."""

    def __init__(self, producer: "FakeProducer") -> None:
        self._producer = producer

    async def __aenter__(self) -> FakeTransaction:
        self._producer.transaction_count += 1
        self._producer._in_transaction = True
        return self

    async def __aexit__(self, exc_type: Any, *_: Any) -> None:
        if exc_type is not None:
            self._producer.aborted_count += 1
        else:
            self._producer.committed_count += 1
        self._producer._in_transaction = False


class FakeProducer:
    """
    Fake AIOKafkaProducer with transaction support.

    Tracks transaction open/commit/abort counts and sent-offset calls
    so tests can assert on exactly-once semantics.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.sent: list[tuple[str, bytes]] = []
        self.offsets_committed: list[dict] = []  # records of (offsets, group_id)
        self.transaction_count = 0
        self.committed_count = 0
        self.aborted_count = 0
        self._in_transaction = False

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def transaction(self) -> FakeTransaction:
        return FakeTransaction(self)

    async def send_and_wait(self, topic: str, *, value: bytes) -> None:
        self.sent.append((topic, value))

    async def send_offsets_to_transaction(self, offsets: dict, group_id: str) -> None:
        self.offsets_committed.append({"offsets": offsets, "group_id": group_id})


class FakeConsumer:
    """
    Fake AIOKafkaConsumer with commit and isolation_level tracking.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self._messages: list[FakeMessage] = []
        self.commits: int = 0

    def queue(
        self, topic: str, event: Event, partition: int = 0, offset: int = 0
    ) -> None:
        self._messages.append(
            FakeMessage(
                topic=topic,
                value=JsonEventSerializer().serialize(event),
                partition=partition,
                offset=offset,
            )
        )

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def subscribe(self, topics: list[str]) -> None:
        pass

    async def commit(self) -> None:
        self.commits += 1

    def __aiter__(self) -> AsyncIterator[FakeMessage]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[FakeMessage]:
        for msg in self._messages:
            yield msg
        await asyncio.sleep(1_000_000)  # Block after all messages


# ── Helpers ────────────────────────────────────────────────────────────────────


def _settings(
    semantics: KafkaDeliverySemantics, txn_id: str | None = None
) -> KafkaEventBusSettings:
    return KafkaEventBusSettings(
        bootstrap_servers="localhost:9092",
        group_id="eos-test-group",
        delivery_semantics=semantics,
        transactional_id=txn_id,
    )


# ── AT_LEAST_ONCE (default) ────────────────────────────────────────────────────


async def test_at_least_once_producer_has_no_transactional_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AT_LEAST_ONCE producer must NOT have transactional_id in kwargs."""
    created: list[FakeProducer] = []

    def _make_producer(**kw: Any) -> FakeProducer:
        p = FakeProducer(**kw)
        created.append(p)
        return p

    monkeypatch.setattr("varco_kafka.bus.AIOKafkaProducer", _make_producer)
    monkeypatch.setattr("varco_kafka.bus.AIOKafkaConsumer", FakeConsumer)

    bus = KafkaEventBus(_settings(KafkaDeliverySemantics.AT_LEAST_ONCE))
    await bus.start()

    assert "transactional_id" not in created[0].kwargs
    await bus.stop()


async def test_at_least_once_consumer_uses_auto_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AT_LEAST_ONCE consumer must have enable_auto_commit=True (default)."""
    created: list[FakeConsumer] = []

    def _make_consumer(**kw: Any) -> FakeConsumer:
        c = FakeConsumer(**kw)
        created.append(c)
        return c

    monkeypatch.setattr("varco_kafka.bus.AIOKafkaProducer", FakeProducer)
    monkeypatch.setattr("varco_kafka.bus.AIOKafkaConsumer", _make_consumer)

    bus = KafkaEventBus(_settings(KafkaDeliverySemantics.AT_LEAST_ONCE))
    await bus.start()

    assert created[0].kwargs.get("enable_auto_commit") is True
    assert created[0].kwargs.get("isolation_level") is None
    await bus.stop()


async def test_at_least_once_publish_no_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AT_LEAST_ONCE publish must NOT wrap in a transaction."""
    fake_producer = FakeProducer()
    monkeypatch.setattr("varco_kafka.bus.AIOKafkaProducer", lambda **kw: fake_producer)
    monkeypatch.setattr("varco_kafka.bus.AIOKafkaConsumer", FakeConsumer)

    bus = KafkaEventBus(_settings(KafkaDeliverySemantics.AT_LEAST_ONCE))
    await bus.start()
    await bus.publish(PaymentEvent(amount=100.0), channel="payments")

    assert len(fake_producer.sent) == 1
    assert fake_producer.transaction_count == 0
    await bus.stop()


# ── AT_MOST_ONCE ──────────────────────────────────────────────────────────────


async def test_at_most_once_consumer_no_auto_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AT_MOST_ONCE consumer must have enable_auto_commit=False."""
    created: list[FakeConsumer] = []

    def _make_consumer(**kw: Any) -> FakeConsumer:
        c = FakeConsumer(**kw)
        created.append(c)
        return c

    monkeypatch.setattr("varco_kafka.bus.AIOKafkaProducer", FakeProducer)
    monkeypatch.setattr("varco_kafka.bus.AIOKafkaConsumer", _make_consumer)

    bus = KafkaEventBus(_settings(KafkaDeliverySemantics.AT_MOST_ONCE))
    await bus.start()

    assert created[0].kwargs.get("enable_auto_commit") is False
    await bus.stop()


async def test_at_most_once_commits_before_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AT_MOST_ONCE consumer must commit BEFORE calling the handler."""
    order: list[str] = []
    fake_consumer = FakeConsumer()
    fake_consumer.queue("payments", PaymentEvent(amount=50.0))

    original_commit = fake_consumer.commit

    async def _tracking_commit() -> None:
        order.append("commit")
        await original_commit()

    fake_consumer.commit = _tracking_commit  # type: ignore[method-assign]

    received: list[Event] = []

    async def _handler(event: Event) -> None:
        order.append("dispatch")
        received.append(event)

    monkeypatch.setattr("varco_kafka.bus.AIOKafkaProducer", FakeProducer)
    monkeypatch.setattr("varco_kafka.bus.AIOKafkaConsumer", lambda **kw: fake_consumer)

    bus = KafkaEventBus(_settings(KafkaDeliverySemantics.AT_MOST_ONCE))
    await bus.start()
    bus.subscribe(PaymentEvent, _handler, channel="payments")

    # Cancel after one message is delivered.
    try:
        await asyncio.wait_for(asyncio.shield(bus._consumer_task), timeout=0.1)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass

    await bus.stop()

    assert order[:2] == [
        "commit",
        "dispatch",
    ], "commit must happen before dispatch in AT_MOST_ONCE mode"


# ── EXACTLY_ONCE ──────────────────────────────────────────────────────────────


async def test_exactly_once_requires_transactional_id() -> None:
    """start() must raise ValueError when EXACTLY_ONCE is set but transactional_id is missing."""
    bus = KafkaEventBus(_settings(KafkaDeliverySemantics.EXACTLY_ONCE, txn_id=None))
    with pytest.raises(ValueError, match="transactional_id"):
        await bus.start()


async def test_exactly_once_producer_has_transactional_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EXACTLY_ONCE producer must have transactional_id and enable_idempotence=True."""
    created: list[FakeProducer] = []

    def _make_producer(**kw: Any) -> FakeProducer:
        p = FakeProducer(**kw)
        created.append(p)
        return p

    monkeypatch.setattr("varco_kafka.bus.AIOKafkaProducer", _make_producer)
    monkeypatch.setattr("varco_kafka.bus.AIOKafkaConsumer", FakeConsumer)

    bus = KafkaEventBus(
        _settings(KafkaDeliverySemantics.EXACTLY_ONCE, txn_id="svc-txn-1")
    )
    await bus.start()

    assert created[0].kwargs.get("transactional_id") == "svc-txn-1"
    assert created[0].kwargs.get("enable_idempotence") is True
    await bus.stop()


async def test_exactly_once_consumer_read_committed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EXACTLY_ONCE consumer must use isolation_level=read_committed and no auto-commit."""
    created: list[FakeConsumer] = []

    def _make_consumer(**kw: Any) -> FakeConsumer:
        c = FakeConsumer(**kw)
        created.append(c)
        return c

    monkeypatch.setattr("varco_kafka.bus.AIOKafkaProducer", FakeProducer)
    monkeypatch.setattr("varco_kafka.bus.AIOKafkaConsumer", _make_consumer)

    bus = KafkaEventBus(
        _settings(KafkaDeliverySemantics.EXACTLY_ONCE, txn_id="svc-txn-1")
    )
    await bus.start()

    assert created[0].kwargs.get("isolation_level") == "read_committed"
    assert created[0].kwargs.get("enable_auto_commit") is False
    await bus.stop()


async def test_exactly_once_publish_wraps_in_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EXACTLY_ONCE publish must open exactly one transaction per publish call."""
    fake_producer = FakeProducer()
    monkeypatch.setattr("varco_kafka.bus.AIOKafkaProducer", lambda **kw: fake_producer)
    monkeypatch.setattr("varco_kafka.bus.AIOKafkaConsumer", FakeConsumer)

    bus = KafkaEventBus(
        _settings(KafkaDeliverySemantics.EXACTLY_ONCE, txn_id="svc-txn-1")
    )
    await bus.start()
    await bus.publish(PaymentEvent(amount=200.0), channel="payments")

    assert len(fake_producer.sent) == 1
    assert fake_producer.transaction_count == 1
    assert fake_producer.committed_count == 1
    await bus.stop()


async def test_exactly_once_consume_commits_offset_in_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    EXACTLY_ONCE consume loop must commit the consumer offset inside the
    producer transaction via send_offsets_to_transaction.
    """
    fake_producer = FakeProducer()
    fake_consumer = FakeConsumer()
    fake_consumer.queue("payments", PaymentEvent(amount=99.0), partition=0, offset=7)

    received: list[Event] = []

    async def _handler(event: Event) -> None:
        received.append(event)

    monkeypatch.setattr("varco_kafka.bus.AIOKafkaProducer", lambda **kw: fake_producer)
    monkeypatch.setattr("varco_kafka.bus.AIOKafkaConsumer", lambda **kw: fake_consumer)

    bus = KafkaEventBus(
        _settings(KafkaDeliverySemantics.EXACTLY_ONCE, txn_id="svc-txn-1")
    )
    await bus.start()
    bus.subscribe(PaymentEvent, _handler, channel="payments")

    try:
        await asyncio.wait_for(asyncio.shield(bus._consumer_task), timeout=0.1)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass

    await bus.stop()

    # Handler must have been called.
    assert len(received) == 1

    # Exactly one transaction must have been opened and committed.
    assert fake_producer.transaction_count == 1
    assert fake_producer.committed_count == 1

    # send_offsets_to_transaction must have been called with offset+1.
    assert len(fake_producer.offsets_committed) == 1
    committed = fake_producer.offsets_committed[0]
    assert committed["group_id"] == "eos-test-group"
    # OffsetAndMetadata(offset+1)
    from aiokafka import TopicPartition

    tp = TopicPartition("payments", 0)
    assert tp in committed["offsets"]
    assert committed["offsets"][tp].offset == 8  # offset 7 + 1


# ── repr ───────────────────────────────────────────────────────────────────────


def test_repr_includes_semantics() -> None:
    """repr must include the delivery_semantics value."""
    bus = KafkaEventBus(_settings(KafkaDeliverySemantics.EXACTLY_ONCE, txn_id="t1"))
    r = repr(bus)
    assert "exactly_once" in r
