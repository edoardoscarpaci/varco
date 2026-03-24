"""
Integration tests for varco_kafka.KafkaEventBus
=================================================
These tests spin up a real Kafka broker via testcontainers and verify
end-to-end publish/subscribe behaviour.

DISABLED BY DEFAULT — requires Docker.  Run with::

    pytest -m integration tests/test_kafka_integration.py

Or set the ``VARCO_RUN_INTEGRATION`` env var::

    VARCO_RUN_INTEGRATION=1 pytest tests/test_kafka_integration.py

Performance note: the first run downloads the Kafka Docker image (~500 MB).
Subsequent runs reuse the cached image.

Prerequisites:
    - Docker daemon running
    - testcontainers[kafka] installed (see pyproject.toml dev dependencies)
"""

from __future__ import annotations

import asyncio
import os

import pytest

from varco_core.event import Event

pytestmark = pytest.mark.integration

# Skip the entire module if not explicitly requested.
# This prevents accidental slow test runs in CI where Docker is unavailable.
if not os.environ.get("VARCO_RUN_INTEGRATION"):
    pytest.skip(
        "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration",
        allow_module_level=True,
    )


# ── Test event types ────────────────────────────────────────────────────────────


class IntegrationOrderEvent(Event):
    __event_type__ = "order.integration.kafka"
    order_id: str
    amount: float = 0.0


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def kafka_container():
    """
    Start a Kafka broker for the integration test module.

    Uses ``testcontainers.kafka.KafkaContainer`` to spin up a real broker.
    Shared across all tests in the module (``scope="module"``) to avoid
    per-test startup overhead (~5s per container).
    """
    from testcontainers.kafka import KafkaContainer  # noqa: PLC0415

    with KafkaContainer() as kafka:
        yield kafka


@pytest.fixture
async def bus(kafka_container):
    """
    ``KafkaEventBus`` connected to the testcontainers Kafka broker.

    Creates a fresh consumer group per test to avoid offset interference
    between tests.
    """
    import uuid  # noqa: PLC0415

    from varco_kafka import KafkaEventBus, KafkaEventBusSettings  # noqa: PLC0415

    config = KafkaEventBusSettings(
        bootstrap_servers=kafka_container.get_bootstrap_server(),
        # Unique group ID per test — avoids cross-test offset sharing.
        group_id=f"test-{uuid.uuid4().hex[:8]}",
        # earliest — so consumers see messages published before they subscribe.
        auto_offset_reset="earliest",
    )
    async with KafkaEventBus(config) as b:
        yield b


@pytest.fixture
async def channel_manager(kafka_container):
    """
    ``KafkaChannelManager`` connected to the testcontainers Kafka broker.

    Used by integration tests that need to pre-create topics before subscribing.
    Admin operations live here — not on the bus — so credentials stay separate.
    """
    from varco_kafka import (
        KafkaChannelManager,
        KafkaChannelManagerSettings,
    )  # noqa: PLC0415

    settings = KafkaChannelManagerSettings(
        bootstrap_servers=kafka_container.get_bootstrap_server(),
    )
    async with KafkaChannelManager(settings) as m:
        yield m


# ── Integration tests ──────────────────────────────────────────────────────────


class TestKafkaIntegration:
    async def test_publish_and_consume(self, bus, channel_manager) -> None:
        """
        Publish an event and verify it arrives at a subscribed local handler.

        DESIGN: We declare the topic first so it exists before the consumer
        subscribes.  Without pre-creation, Kafka's auto-create can race with
        consumer group coordinator formation, causing GroupCoordinatorNotAvailableError
        and missed messages.  ``auto_offset_reset="earliest"`` means the consumer
        will still receive messages published before it fully joined.

        Topic management uses ``channel_manager`` (``KafkaChannelManager``) — a
        separate fixture with admin credentials — so the bus stays credential-free.
        """
        received: list[Event] = []
        delivered = asyncio.Event()

        async def handler(event: Event) -> None:
            received.append(event)
            delivered.set()

        # Pre-create the topic before subscribing so the consumer group
        # coordinator can form without racing against Kafka auto-create.
        await channel_manager.declare_channel("integration-orders")
        bus.subscribe(IntegrationOrderEvent, handler, channel="integration-orders")

        # Wait for the consumer group coordinator to accept this group.
        # Kafka needs a brief window after topic creation and subscribe() for
        # the broker to assign partitions to the consumer.
        await asyncio.sleep(5.0)

        event = IntegrationOrderEvent(order_id="int-1", amount=99.0)
        await bus.publish(event, channel="integration-orders")

        # Give up to 20 s for the consumer loop to receive and dispatch — a
        # testcontainers Kafka broker can be slow on first message delivery.
        await asyncio.wait_for(delivered.wait(), timeout=20.0)

        assert len(received) == 1
        assert isinstance(received[0], IntegrationOrderEvent)
        assert received[0].order_id == "int-1"
        assert received[0].amount == 99.0

    async def test_publish_multiple_events(self, bus, channel_manager) -> None:
        """Publish three events and verify all three are consumed."""
        received: list[Event] = []
        count = asyncio.Event()

        async def handler(event: Event) -> None:
            received.append(event)
            if len(received) >= 3:
                count.set()

        # Pre-create topic — same rationale as test_publish_and_consume.
        await channel_manager.declare_channel("integration-multi")
        bus.subscribe(IntegrationOrderEvent, handler, channel="integration-multi")
        await asyncio.sleep(5.0)

        for i in range(3):
            await bus.publish(
                IntegrationOrderEvent(order_id=f"multi-{i}"),
                channel="integration-multi",
            )

        await asyncio.wait_for(count.wait(), timeout=20.0)
        assert len(received) == 3

    async def test_channel_isolation(self, bus) -> None:
        """Events published to channel A must not reach a subscriber on channel B."""
        received_b: list[Event] = []

        async def handler_b(event: Event) -> None:
            received_b.append(event)

        bus.subscribe(IntegrationOrderEvent, handler_b, channel="channel-b")

        # Publish to channel-a — handler_b should NOT receive this
        await bus.publish(
            IntegrationOrderEvent(order_id="wrong-channel"),
            channel="channel-a",
        )

        # Give the consumer a moment to process any mis-routed messages
        await asyncio.sleep(2.0)

        assert received_b == []
