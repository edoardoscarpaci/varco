"""
Unit tests for varco_kafka.KafkaEventBus
=========================================
All tests mock ``aiokafka`` — no real Kafka broker required.

Integration tests that spin up a real Kafka broker via testcontainers are
in ``test_kafka_integration.py`` and are disabled by default::

    pytest -m integration   # run integration tests

Test doubles
------------
``FakeProducer`` and ``FakeConsumer`` replace ``AIOKafkaProducer`` and
``AIOKafkaConsumer`` at import time using ``monkeypatch``.  This keeps tests
fast and dependency-free.

DESIGN: monkeypatch over unittest.mock
    ✅ Explicit fake objects — behaviour is fully visible in this file.
    ✅ No magic attribute access on mocks — test failures point to real bugs.
    ❌ More verbose than ``MagicMock()`` — justified because aiokafka's async
       interface is non-trivial and mock auto-spec would be fragile.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import patch

import pytest

from varco_core.event import (
    Event,
    Subscription,
)
from varco_core.event.serializer import JsonEventSerializer
from varco_kafka import KafkaEventBus, KafkaEventBusSettings


# ── Test event types ────────────────────────────────────────────────────────────


class OrderPlacedEvent(Event):
    __event_type__ = "order.placed.kafka_test"
    order_id: str


class OrderCancelledEvent(Event):
    __event_type__ = "order.cancelled.kafka_test"
    reason: str = ""


# ── Fake aiokafka doubles ──────────────────────────────────────────────────────


class FakeMessage:
    """Fake aiokafka consumer message."""

    def __init__(self, topic: str, value: bytes) -> None:
        self.topic = topic
        self.value = value


class FakeProducer:
    """
    Fake AIOKafkaProducer — records calls to send_and_wait.

    Attributes:
        sent: List of ``(topic, value)`` tuples in send order.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.sent: list[tuple[str, bytes]] = []
        self._started = False

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    async def send_and_wait(self, topic: str, *, value: bytes) -> None:
        self.sent.append((topic, value))


class FakeConsumer:
    """
    Fake AIOKafkaConsumer — yields pre-loaded messages on iteration.

    Attributes:
        messages: Queue messages into this list before starting — the async
                  iterator will yield them in order then block forever.
    """

    def __init__(self, **kwargs: Any) -> None:
        self._subscriptions: set[str] = set()
        self._messages: list[FakeMessage] = []
        self._started = False

    def queue(self, topic: str, event: Event) -> None:
        """Enqueue an event to be delivered on the next iteration."""
        self._messages.append(
            # Instance method — JsonEventSerializer is stateless so a throwaway
            # instance is fine here.
            FakeMessage(topic=topic, value=JsonEventSerializer().serialize(event))
        )

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False

    def subscribe(self, topics: list[str]) -> None:
        self._subscriptions = set(topics)

    def __aiter__(self) -> AsyncIterator[FakeMessage]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[FakeMessage]:
        for msg in self._messages:
            yield msg
        # Block forever after all pre-loaded messages — mimics a real consumer
        await asyncio.sleep(1_000_000)


# ── Fixture: bus with fakes ────────────────────────────────────────────────────


@pytest.fixture
def fake_producer() -> FakeProducer:
    return FakeProducer()


@pytest.fixture
def fake_consumer() -> FakeConsumer:
    return FakeConsumer()


@pytest.fixture
def config() -> KafkaEventBusSettings:
    return KafkaEventBusSettings(bootstrap_servers="fake:9092", group_id="test-group")


@pytest.fixture
async def bus(
    config: KafkaEventBusSettings,
    fake_producer: FakeProducer,
    fake_consumer: FakeConsumer,
) -> AsyncIterator[KafkaEventBus]:
    """
    ``KafkaEventBus`` with aiokafka fakes injected via monkeypatch.

    Patches ``AIOKafkaProducer`` and ``AIOKafkaConsumer`` so no real Kafka
    connection is made.  ``AIOKafkaAdminClient`` is no longer part of the bus
    — it lives in ``KafkaChannelManager``.
    """
    with (
        patch("varco_kafka.bus.AIOKafkaProducer", return_value=fake_producer),
        patch("varco_kafka.bus.AIOKafkaConsumer", return_value=fake_consumer),
    ):
        async with KafkaEventBus(config) as b:
            yield b


# ── KafkaEventBusSettings ──────────────────────────────────────────────────────


class TestKafkaConfig:
    def test_defaults(self) -> None:
        cfg = KafkaEventBusSettings()
        assert cfg.bootstrap_servers == "localhost:9092"
        assert cfg.group_id == "varco-default"
        assert cfg.channel_prefix == ""

    def test_topic_name_no_prefix(self) -> None:
        cfg = KafkaEventBusSettings()
        assert cfg.topic_name("orders") == "orders"

    def test_topic_name_with_prefix(self) -> None:
        cfg = KafkaEventBusSettings(channel_prefix="prod.")
        assert cfg.topic_name("orders") == "prod.orders"

    def test_frozen(self) -> None:
        cfg = KafkaEventBusSettings()
        with pytest.raises(Exception):
            cfg.group_id = "other"  # type: ignore[misc]


# ── KafkaEventBus — lifecycle ──────────────────────────────────────────────────


class TestKafkaEventBusLifecycle:
    async def test_publish_before_start_raises(self) -> None:
        bus = KafkaEventBus()
        with pytest.raises(RuntimeError, match="start()"):
            await bus.publish(OrderPlacedEvent(order_id="1"))

    async def test_start_idempotent(
        self,
        config: KafkaEventBusSettings,
        fake_producer: FakeProducer,
        fake_consumer: FakeConsumer,
    ) -> None:
        with (
            patch("varco_kafka.bus.AIOKafkaProducer", return_value=fake_producer),
            patch("varco_kafka.bus.AIOKafkaConsumer", return_value=fake_consumer),
        ):
            bus = KafkaEventBus(config)
            await bus.start()
            await bus.start()  # second call is no-op
            assert bus._started
            await bus.stop()

    async def test_stop_before_start_is_noop(self) -> None:
        bus = KafkaEventBus()
        await bus.stop()  # must not raise

    async def test_context_manager_starts_and_stops(
        self,
        config: KafkaEventBusSettings,
        fake_producer: FakeProducer,
        fake_consumer: FakeConsumer,
    ) -> None:
        with (
            patch("varco_kafka.bus.AIOKafkaProducer", return_value=fake_producer),
            patch("varco_kafka.bus.AIOKafkaConsumer", return_value=fake_consumer),
        ):
            async with KafkaEventBus(config) as bus:
                assert bus._started
            assert not bus._started


# ── KafkaEventBus — publishing ─────────────────────────────────────────────────


class TestKafkaEventBusPublish:
    async def test_publish_sends_to_correct_topic(
        self,
        bus: KafkaEventBus,
        fake_producer: FakeProducer,
    ) -> None:
        event = OrderPlacedEvent(order_id="abc")
        await bus.publish(event, channel="orders")

        assert len(fake_producer.sent) == 1
        topic, value = fake_producer.sent[0]
        assert topic == "orders"

    async def test_published_bytes_contain_event_type(
        self,
        bus: KafkaEventBus,
        fake_producer: FakeProducer,
    ) -> None:
        await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")
        _, value = fake_producer.sent[0]
        raw = json.loads(value.decode("utf-8"))
        assert raw["__event_type__"] == "order.placed.kafka_test"

    async def test_publish_returns_none(
        self,
        bus: KafkaEventBus,
    ) -> None:
        result = await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")
        assert result is None

    async def test_topic_prefix_applied(
        self,
        fake_producer: FakeProducer,
        fake_consumer: FakeConsumer,
    ) -> None:
        config = KafkaEventBusSettings(channel_prefix="prod.")
        with (
            patch("varco_kafka.bus.AIOKafkaProducer", return_value=fake_producer),
            patch("varco_kafka.bus.AIOKafkaConsumer", return_value=fake_consumer),
        ):
            async with KafkaEventBus(config) as bus:
                await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")

        _, value = fake_producer.sent[0]
        assert fake_producer.sent[0][0] == "prod.orders"

    async def test_publish_many(
        self,
        bus: KafkaEventBus,
        fake_producer: FakeProducer,
    ) -> None:
        e1 = OrderPlacedEvent(order_id="1")
        e2 = OrderCancelledEvent(order_id="2")
        await bus.publish_many([(e1, "orders"), (e2, "orders")])

        assert len(fake_producer.sent) == 2


# ── KafkaEventBus — subscribe and local dispatch ──────────────────────────────


class TestKafkaEventBusSubscribe:
    async def test_subscribe_returns_subscription(self, bus: KafkaEventBus) -> None:
        received: list[Event] = []

        async def handler(e: Event) -> None:
            received.append(e)

        sub = bus.subscribe(OrderPlacedEvent, handler, channel="orders")
        assert isinstance(sub, Subscription)
        assert not sub.is_cancelled

    async def test_cancel_subscription(self, bus: KafkaEventBus) -> None:
        received: list[Event] = []

        async def handler(e: Event) -> None:
            received.append(e)

        sub = bus.subscribe(OrderPlacedEvent, handler, channel="orders")
        sub.cancel()
        assert sub.is_cancelled

    async def test_repr(self, bus: KafkaEventBus) -> None:
        r = repr(bus)
        assert "KafkaEventBus" in r
        assert "started=True" in r


# ── KafkaEventBus — consumer loop dispatch ────────────────────────────────────


class TestKafkaEventBusConsumerDispatch:
    async def test_consumer_dispatches_incoming_messages(
        self,
        config: KafkaEventBusSettings,
        fake_producer: FakeProducer,
        fake_consumer: FakeConsumer,
    ) -> None:
        received: list[Event] = []
        event = OrderPlacedEvent(order_id="x")
        fake_consumer.queue("orders", event)

        async def handler(e: Event) -> None:
            received.append(e)

        with (
            patch("varco_kafka.bus.AIOKafkaProducer", return_value=fake_producer),
            patch("varco_kafka.bus.AIOKafkaConsumer", return_value=fake_consumer),
        ):
            async with KafkaEventBus(config) as bus:
                bus.subscribe(OrderPlacedEvent, handler, channel="orders")
                # Give the consumer loop a tick to process the pre-loaded message
                await asyncio.sleep(0)
                await asyncio.sleep(0)

        assert len(received) == 1
        assert isinstance(received[0], OrderPlacedEvent)

    async def test_consumer_strips_topic_prefix_for_channel(
        self,
        fake_producer: FakeProducer,
        fake_consumer: FakeConsumer,
    ) -> None:
        config = KafkaEventBusSettings(channel_prefix="prod.")
        event = OrderPlacedEvent(order_id="1")
        # The message arrives on topic "prod.orders"
        fake_consumer.queue("prod.orders", event)

        async def handler(e: Event) -> None:
            pass

        with (
            patch("varco_kafka.bus.AIOKafkaProducer", return_value=fake_producer),
            patch("varco_kafka.bus.AIOKafkaConsumer", return_value=fake_consumer),
        ):
            async with KafkaEventBus(config) as bus:
                # Subscribe to "orders" — should match after prefix stripping
                bus.subscribe(OrderPlacedEvent, handler, channel="orders")
                await asyncio.sleep(0)
                await asyncio.sleep(0)
