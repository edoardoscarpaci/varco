"""
Integration tests for varco_redis.RedisEventBus
================================================
These tests spin up a real Redis instance via testcontainers and verify
end-to-end publish/subscribe behaviour.

DISABLED BY DEFAULT — requires Docker.  Run with::

    pytest -m integration tests/test_redis_integration.py

Or set the ``VARCO_RUN_INTEGRATION`` env var::

    VARCO_RUN_INTEGRATION=1 pytest tests/test_redis_integration.py

Prerequisites:
    - Docker daemon running
    - testcontainers[redis] installed (see pyproject.toml dev dependencies)
"""

from __future__ import annotations

import asyncio
import os

import pytest

from varco_core.event import Event

pytestmark = pytest.mark.integration

# Skip the entire module if not explicitly requested.
if not os.environ.get("VARCO_RUN_INTEGRATION"):
    pytest.skip(
        "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration",
        allow_module_level=True,
    )


# ── Test event types ────────────────────────────────────────────────────────────


class IntegrationRedisEvent(Event):
    __event_type__ = "order.integration.redis"
    order_id: str
    amount: float = 0.0


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def redis_container():
    """
    Start a Redis instance for the integration test module.

    Uses ``testcontainers.redis.RedisContainer`` to spin up a real server.
    Shared across all tests in the module (``scope="module"``) to avoid
    per-test startup overhead (~2s per container).
    """
    from testcontainers.redis import RedisContainer  # noqa: PLC0415

    with RedisContainer() as redis:
        yield redis


@pytest.fixture
async def bus(redis_container):
    """
    ``RedisEventBus`` connected to the testcontainers Redis instance.
    """
    from varco_redis import RedisEventBus, RedisEventBusSettings  # noqa: PLC0415

    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    config = RedisEventBusSettings(url=f"redis://{host}:{port}/0")

    async with RedisEventBus(config) as b:
        yield b


# ── Integration tests ──────────────────────────────────────────────────────────


class TestRedisIntegration:
    async def test_publish_and_consume(self, bus) -> None:
        """Publish an event and verify it arrives at a subscribed local handler."""
        received: list[Event] = []
        delivered = asyncio.Event()

        async def handler(event: Event) -> None:
            received.append(event)
            delivered.set()

        bus.subscribe(IntegrationRedisEvent, handler, channel="integration-orders")

        # Give the Pub/Sub subscription a moment to establish before publishing
        await asyncio.sleep(0.1)

        event = IntegrationRedisEvent(order_id="redis-int-1", amount=55.0)
        await bus.publish(event, channel="integration-orders")

        await asyncio.wait_for(delivered.wait(), timeout=5.0)

        assert len(received) == 1
        assert isinstance(received[0], IntegrationRedisEvent)
        assert received[0].order_id == "redis-int-1"

    async def test_publish_multiple_events(self, bus) -> None:
        """Publish three events and verify all three are consumed."""
        received: list[Event] = []
        count = asyncio.Event()

        async def handler(event: Event) -> None:
            received.append(event)
            if len(received) >= 3:
                count.set()

        bus.subscribe(IntegrationRedisEvent, handler, channel="integration-multi")
        await asyncio.sleep(0.1)

        for i in range(3):
            await bus.publish(
                IntegrationRedisEvent(order_id=f"redis-multi-{i}"),
                channel="integration-multi",
            )

        await asyncio.wait_for(count.wait(), timeout=5.0)
        assert len(received) == 3

    async def test_channel_isolation(self, bus) -> None:
        """Events published to channel A must not reach a subscriber on channel B."""
        received_b: list[Event] = []

        async def handler_b(event: Event) -> None:
            received_b.append(event)

        bus.subscribe(IntegrationRedisEvent, handler_b, channel="redis-channel-b")
        await asyncio.sleep(0.1)

        await bus.publish(
            IntegrationRedisEvent(order_id="wrong"),
            channel="redis-channel-a",
        )

        await asyncio.sleep(0.5)
        assert received_b == []

    async def test_wildcard_subscription_receives_all_channels(self, bus) -> None:
        """A CHANNEL_ALL subscriber must receive events from any channel."""
        received: list[Event] = []
        got_two = asyncio.Event()

        async def handler(event: Event) -> None:
            received.append(event)
            if len(received) >= 2:
                got_two.set()

        bus.subscribe(IntegrationRedisEvent, handler)  # CHANNEL_ALL
        await asyncio.sleep(0.1)

        await bus.publish(IntegrationRedisEvent(order_id="wc-1"), channel="ch-x")
        await bus.publish(IntegrationRedisEvent(order_id="wc-2"), channel="ch-y")

        await asyncio.wait_for(got_two.wait(), timeout=5.0)
        assert len(received) == 2
