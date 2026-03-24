"""
Integration tests for varco_kafka.channel
==========================================
These tests spin up a real Kafka broker via testcontainers and verify
end-to-end topic management behaviour via ``KafkaChannelManager``.

DISABLED BY DEFAULT — requires Docker.  Run with::

    pytest -m integration tests/test_kafka_channel_integration.py

Or set the ``VARCO_RUN_INTEGRATION`` env var::

    VARCO_RUN_INTEGRATION=1 pytest tests/test_kafka_channel_integration.py

Prerequisites:
    - Docker daemon running
    - testcontainers[kafka] installed (see pyproject.toml dev dependencies)
"""

from __future__ import annotations

import os
import uuid

import pytest

pytestmark = pytest.mark.integration

# Skip the entire module if not explicitly requested.
if not os.environ.get("VARCO_RUN_INTEGRATION"):
    pytest.skip(
        "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration",
        allow_module_level=True,
    )


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def kafka_container():
    """
    Start a Kafka broker for the integration test module.

    Shared across all tests in the module (``scope="module"``) to avoid
    per-test startup overhead (~5s per container).
    """
    from testcontainers.kafka import KafkaContainer

    with KafkaContainer() as kafka:
        yield kafka


@pytest.fixture
async def manager(kafka_container):
    """
    ``KafkaChannelManager`` connected to the testcontainers Kafka broker.

    Yields a started manager; stops on teardown.
    """
    from varco_kafka.channel import KafkaChannelManager, KafkaChannelManagerSettings

    bootstrap = kafka_container.get_bootstrap_server()
    settings = KafkaChannelManagerSettings(bootstrap_servers=bootstrap)
    async with KafkaChannelManager(settings) as m:
        yield m


# ── Tests ──────────────────────────────────────────────────────────────────────


class TestKafkaChannelManagerIntegration:
    async def test_declare_channel_creates_topic(self, manager) -> None:
        """Declaring a channel creates the Kafka topic on the broker."""
        channel = f"test-declare-{uuid.uuid4().hex[:8]}"
        await manager.declare_channel(channel)
        assert await manager.channel_exists(channel) is True

    async def test_declare_channel_idempotent(self, manager) -> None:
        """Declaring the same channel twice must not raise."""
        channel = f"test-idempotent-{uuid.uuid4().hex[:8]}"
        await manager.declare_channel(channel)
        await manager.declare_channel(channel)  # must not raise
        assert await manager.channel_exists(channel) is True

    async def test_channel_does_not_exist_before_declare(self, manager) -> None:
        """A channel that was never declared must not exist on the broker."""
        channel = f"test-missing-{uuid.uuid4().hex[:8]}"
        assert await manager.channel_exists(channel) is False

    async def test_list_channels_includes_declared(self, manager) -> None:
        """Declared channels must appear in list_channels()."""
        channel = f"test-list-{uuid.uuid4().hex[:8]}"
        await manager.declare_channel(channel)
        channels = await manager.list_channels()
        assert channel in channels

    async def test_delete_channel_removes_topic(self, manager) -> None:
        """Deleting a channel removes the topic from the broker."""
        channel = f"test-delete-{uuid.uuid4().hex[:8]}"
        await manager.declare_channel(channel)
        await manager.delete_channel(channel)
        assert await manager.channel_exists(channel) is False

    async def test_declare_with_custom_partitions(self, manager) -> None:
        """Declaring with ChannelConfig sets the partition count."""
        from varco_core.event.base import ChannelConfig

        channel = f"test-partitions-{uuid.uuid4().hex[:8]}"
        config = ChannelConfig(num_partitions=3, replication_factor=1)
        await manager.declare_channel(channel, config=config)
        assert await manager.channel_exists(channel) is True
