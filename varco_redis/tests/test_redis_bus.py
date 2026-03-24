"""
Unit tests for varco_redis.RedisEventBus
=========================================
All tests mock ``redis.asyncio`` — no real Redis instance required.

Integration tests that spin up a real Redis instance via testcontainers are
in ``test_redis_integration.py`` and are disabled by default::

    pytest -m integration   # run integration tests

Test doubles
------------
``FakeRedis`` and ``FakePubSub`` replace the real redis client.  Both are
synchronous enough to exercise the bus without I/O.

DESIGN: explicit fake over MagicMock
    ✅ Behaviour is fully visible and controllable in this file.
    ✅ Async methods are real ``async def`` — no mock magic required.
    ❌ More verbose — justified by the complexity of the async Pub/Sub API.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from unittest.mock import patch

import pytest

from varco_core.event import (
    Event,
    Subscription,
)
from varco_core.event.serializer import JsonEventSerializer
from varco_redis import RedisEventBus, RedisEventBusSettings


# ── Test event types ────────────────────────────────────────────────────────────


class OrderPlacedEvent(Event):
    __event_type__ = "order.placed.redis_test"
    order_id: str


class OrderCancelledEvent(Event):
    __event_type__ = "order.cancelled.redis_test"
    reason: str = ""


# ── Fake redis doubles ─────────────────────────────────────────────────────────


class FakePubSub:
    """
    Fake redis.asyncio PubSub client.

    Attributes:
        subscribed_channels: Channels passed to ``subscribe()``.
        pattern_subscribed:  True if ``psubscribe("*")`` was called.
        queued_messages:     Messages to return on successive ``get_message()`` calls.
    """

    def __init__(self) -> None:
        self.subscribed_channels: list[str] = []
        self.pattern_subscribed: bool = False
        self._messages: list[dict] = []
        self._message_idx: int = 0

    def queue_message(self, channel: str, event: Event) -> None:
        """Enqueue a message to be returned by the next ``get_message()`` call."""
        self._messages.append(
            {
                "type": "message",
                "channel": channel.encode("utf-8"),
                # Instance method — JsonEventSerializer is stateless so a
                # throwaway instance is fine here.
                "data": JsonEventSerializer().serialize(event),
            }
        )

    async def subscribe(self, *channels: str) -> None:
        self.subscribed_channels.extend(channels)

    async def psubscribe(self, *patterns: str) -> None:
        self.pattern_subscribed = True

    async def get_message(
        self, ignore_subscribe_messages: bool = True, timeout: float = 0.0
    ) -> dict | None:
        if self._message_idx >= len(self._messages):
            return None
        msg = self._messages[self._message_idx]
        self._message_idx += 1
        return msg

    async def close(self) -> None:
        pass


class FakeRedis:
    """
    Fake redis.asyncio.Redis client.

    Attributes:
        published: List of ``(channel, value)`` pairs from ``publish()`` calls.
    """

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []
        self._pubsub = FakePubSub()

    def pubsub(self) -> FakePubSub:
        return self._pubsub

    async def publish(self, channel: str, value: bytes) -> None:
        self.published.append((channel, value))

    async def aclose(self) -> None:
        pass


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture
def fake_redis() -> FakeRedis:
    return FakeRedis()


@pytest.fixture
def config() -> RedisEventBusSettings:
    return RedisEventBusSettings(url="redis://fake:6379/0")


@pytest.fixture
async def bus(
    config: RedisEventBusSettings, fake_redis: FakeRedis
) -> AsyncIterator[RedisEventBus]:
    """
    ``RedisEventBus`` with redis.asyncio fakes injected via monkeypatch.
    """
    with patch("varco_redis.bus.aioredis") as mock_aioredis:
        mock_aioredis.from_url.return_value = fake_redis
        async with RedisEventBus(config) as b:
            # Inject our fake pubsub so get_message() calls work
            b._pubsub = fake_redis.pubsub()
            yield b


# ── RedisEventBusSettings ────────────────────────────────────────────────────────────────


class TestRedisEventBusSettings:
    def test_defaults(self) -> None:
        cfg = RedisEventBusSettings()
        assert cfg.url == "redis://localhost:6379/0"
        assert cfg.channel_prefix == ""
        assert cfg.decode_responses is False

    def test_channel_name_no_prefix(self) -> None:
        cfg = RedisEventBusSettings()
        assert cfg.channel_name("orders") == "orders"

    def test_channel_name_with_prefix(self) -> None:
        cfg = RedisEventBusSettings(channel_prefix="prod:")
        assert cfg.channel_name("orders") == "prod:orders"

    def test_frozen(self) -> None:
        cfg = RedisEventBusSettings()
        with pytest.raises(Exception):
            cfg.url = "redis://other"  # type: ignore[misc]


# ── RedisEventBus — lifecycle ──────────────────────────────────────────────────


class TestRedisEventBusLifecycle:
    async def test_publish_before_start_raises(self) -> None:
        bus = RedisEventBus()
        with pytest.raises(RuntimeError, match="start()"):
            await bus.publish(OrderPlacedEvent(order_id="1"))

    async def test_start_idempotent(
        self,
        config: RedisEventBusSettings,
        fake_redis: FakeRedis,
    ) -> None:
        with patch("varco_redis.bus.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            bus = RedisEventBus(config)
            await bus.start()
            await bus.start()  # second call is no-op
            assert bus._started
            await bus.stop()

    async def test_stop_before_start_is_noop(self) -> None:
        bus = RedisEventBus()
        await bus.stop()  # must not raise

    async def test_context_manager(
        self,
        config: RedisEventBusSettings,
        fake_redis: FakeRedis,
    ) -> None:
        with patch("varco_redis.bus.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisEventBus(config) as bus:
                assert bus._started
            assert not bus._started


# ── RedisEventBus — publishing ─────────────────────────────────────────────────


class TestRedisEventBusPublish:
    async def test_publish_sends_to_correct_channel(
        self,
        bus: RedisEventBus,
        fake_redis: FakeRedis,
    ) -> None:
        event = OrderPlacedEvent(order_id="abc")
        await bus.publish(event, channel="orders")

        assert len(fake_redis.published) == 1
        channel, value = fake_redis.published[0]
        assert channel == "orders"

    async def test_published_bytes_contain_event_type(
        self,
        bus: RedisEventBus,
        fake_redis: FakeRedis,
    ) -> None:
        await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")
        _, value = fake_redis.published[0]
        raw = json.loads(value.decode("utf-8"))
        assert raw["__event_type__"] == "order.placed.redis_test"

    async def test_publish_returns_none(
        self,
        bus: RedisEventBus,
    ) -> None:
        result = await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")
        assert result is None

    async def test_channel_prefix_applied(
        self,
        fake_redis: FakeRedis,
    ) -> None:
        config = RedisEventBusSettings(
            url="redis://fake:6379/0", channel_prefix="prod:"
        )
        with patch("varco_redis.bus.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisEventBus(config) as bus:
                await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")

        channel, _ = fake_redis.published[0]
        assert channel == "prod:orders"


# ── RedisEventBus — subscribe ──────────────────────────────────────────────────


class TestRedisEventBusSubscribe:
    async def test_subscribe_returns_subscription(self, bus: RedisEventBus) -> None:
        sub = bus.subscribe(OrderPlacedEvent, lambda e: None, channel="orders")
        assert isinstance(sub, Subscription)
        assert not sub.is_cancelled

    async def test_cancel_subscription(self, bus: RedisEventBus) -> None:
        sub = bus.subscribe(OrderPlacedEvent, lambda e: None, channel="orders")
        sub.cancel()
        assert sub.is_cancelled

    async def test_repr(self, bus: RedisEventBus) -> None:
        r = repr(bus)
        assert "RedisEventBus" in r
        assert "started=True" in r


# ── RedisEventBus — listener dispatch ─────────────────────────────────────────


class TestRedisEventBusListenerDispatch:
    async def test_incoming_message_dispatched_to_handler(
        self,
        config: RedisEventBusSettings,
        fake_redis: FakeRedis,
    ) -> None:
        received: list[Event] = []
        event = OrderPlacedEvent(order_id="r1")

        with patch("varco_redis.bus.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisEventBus(config) as bus:
                # Inject our controlled pubsub BEFORE starting the listener loop
                fake_pubsub = fake_redis.pubsub()
                fake_pubsub.queue_message("orders", event)
                bus._pubsub = fake_pubsub

                async def handler(e: Event) -> None:
                    received.append(e)

                bus.subscribe(OrderPlacedEvent, handler, channel="orders")

                # Yield to the listener task to process the queued message
                await asyncio.sleep(0.05)

        assert len(received) == 1
        assert isinstance(received[0], OrderPlacedEvent)
        assert received[0].order_id == "r1"

    async def test_channel_prefix_stripped_on_receive(
        self,
        fake_redis: FakeRedis,
    ) -> None:
        received: list[Event] = []
        event = OrderPlacedEvent(order_id="prefix-test")
        config = RedisEventBusSettings(
            url="redis://fake:6379/0", channel_prefix="prod:"
        )

        with patch("varco_redis.bus.aioredis") as mock_aioredis:
            mock_aioredis.from_url.return_value = fake_redis
            async with RedisEventBus(config) as bus:
                fake_pubsub = fake_redis.pubsub()
                # Message arrives on "prod:orders" — the listener strips "prod:" prefix
                fake_pubsub._messages.append(
                    {
                        "type": "message",
                        "channel": b"prod:orders",
                        "data": JsonEventSerializer().serialize(event),
                    }
                )
                bus._pubsub = fake_pubsub

                async def handler(e: Event) -> None:
                    received.append(e)

                # subscribe with logical channel "orders" — listener maps "prod:orders" → "orders"
                bus.subscribe(OrderPlacedEvent, handler, channel="orders")
                await asyncio.sleep(0.05)

        assert len(received) == 1
