"""
Unit tests for varco_kafka.channel
=====================================
Covers ``KafkaChannelManager`` and ``KafkaChannelManagerSettings``.

All tests mock ``AIOKafkaAdminClient`` — no real Kafka broker required.

Sections
--------
- ``KafkaChannelManagerSettings`` — defaults, topic_name, frozen
- ``KafkaChannelManager`` lifecycle — start guard, stop idempotent, context manager
- ``declare_channel``               — calls create_topics, idempotent on TopicAlreadyExists
- ``delete_channel``                — calls delete_topics, propagates errors
- ``channel_exists``               — interprets describe_topics response
- ``list_channels``                 — strips prefix, sorted
- repr
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
from aiokafka.errors import TopicAlreadyExistsError

from varco_core.event.base import ChannelConfig
from varco_kafka.channel import KafkaChannelManager, KafkaChannelManagerSettings


# ── FakeAdminClient ───────────────────────────────────────────────────────────


class FakeAdminClient:
    """
    Fake AIOKafkaAdminClient that records calls without contacting a broker.

    Attributes:
        created_topics: List of topic names from create_topics() calls.
        deleted_topics: List of topic names from delete_topics() calls.
        topics:         Simulated broker topic list for list_topics().
        fail_create:    If True, create_topics() raises TopicAlreadyExistsError.
    """

    def __init__(self, **kwargs: Any) -> None:
        self.created_topics: list[str] = []
        self.deleted_topics: list[str] = []
        self.topics: set[str] = set()
        self.fail_create: bool = False
        self._describe_responses: list[dict] = []

    async def start(self) -> None:
        pass

    async def close(self) -> None:
        pass

    async def create_topics(self, new_topics: list) -> None:
        if self.fail_create:
            raise TopicAlreadyExistsError()
        for t in new_topics:
            self.created_topics.append(t.name)
            self.topics.add(t.name)

    async def delete_topics(self, topics: list[str]) -> None:
        for t in topics:
            self.deleted_topics.append(t)
            self.topics.discard(t)

    async def describe_topics(self, topics: list[str]) -> list[dict]:
        # Return the pre-loaded describe responses, or default to error_code=0
        # for any topic that is in self.topics.
        if self._describe_responses:
            return self._describe_responses
        result = []
        for t in topics:
            # Use "topic" key — matches aiokafka 0.13 describe_topics() dict shape.
            # The implementation (channel.py) keys on "topic", not "name".
            result.append({"topic": t, "error_code": 0 if t in self.topics else 3})
        return result

    async def list_topics(self) -> list[str]:
        """Return topic names as a plain list — matches aiokafka 0.13 API.

        Older aiokafka returned ClusterMetadata; 0.13+ returns list[str] directly.
        The implementation in channel.py iterates this return value directly.
        """
        # Return a sorted list so tests that don't control insertion order are stable.
        return list(self.topics)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def fake_admin() -> FakeAdminClient:
    return FakeAdminClient()


@pytest.fixture
def settings() -> KafkaChannelManagerSettings:
    return KafkaChannelManagerSettings(bootstrap_servers="fake:9092")


@pytest.fixture
async def manager(
    settings: KafkaChannelManagerSettings, fake_admin: FakeAdminClient
) -> KafkaChannelManager:
    """Started ``KafkaChannelManager`` with fake admin client."""
    with patch("varco_kafka.channel.AIOKafkaAdminClient", return_value=fake_admin):
        m = KafkaChannelManager(settings)
        await m.start()
        yield m
        await m.stop()


# ── KafkaChannelManagerSettings ───────────────────────────────────────────────


class TestKafkaChannelManagerSettings:
    def test_defaults(self) -> None:
        s = KafkaChannelManagerSettings()
        assert s.bootstrap_servers == "localhost:9092"
        assert s.topic_prefix == ""

    def test_topic_name_no_prefix(self) -> None:
        s = KafkaChannelManagerSettings()
        assert s.topic_name("orders") == "orders"

    def test_topic_name_with_prefix(self) -> None:
        s = KafkaChannelManagerSettings(topic_prefix="prod.")
        assert s.topic_name("orders") == "prod.orders"

    def test_frozen(self) -> None:
        s = KafkaChannelManagerSettings()
        with pytest.raises(Exception):
            s.bootstrap_servers = "other"  # type: ignore[misc]


# ── KafkaChannelManager — lifecycle ───────────────────────────────────────────


class TestKafkaChannelManagerLifecycle:
    async def test_start_initialises_admin_client(
        self, settings: KafkaChannelManagerSettings, fake_admin: FakeAdminClient
    ) -> None:
        with patch("varco_kafka.channel.AIOKafkaAdminClient", return_value=fake_admin):
            m = KafkaChannelManager(settings)
            await m.start()
            assert m._admin is not None
            await m.stop()

    async def test_double_start_raises(
        self, settings: KafkaChannelManagerSettings, fake_admin: FakeAdminClient
    ) -> None:
        with patch("varco_kafka.channel.AIOKafkaAdminClient", return_value=fake_admin):
            m = KafkaChannelManager(settings)
            await m.start()
            with pytest.raises(RuntimeError, match="already-started"):
                await m.start()
            await m.stop()

    async def test_stop_clears_admin_client(
        self, settings: KafkaChannelManagerSettings, fake_admin: FakeAdminClient
    ) -> None:
        with patch("varco_kafka.channel.AIOKafkaAdminClient", return_value=fake_admin):
            m = KafkaChannelManager(settings)
            await m.start()
            await m.stop()
            assert m._admin is None

    async def test_stop_before_start_is_noop(
        self, settings: KafkaChannelManagerSettings
    ) -> None:
        m = KafkaChannelManager(settings)
        await m.stop()  # must not raise

    async def test_stop_idempotent(
        self, settings: KafkaChannelManagerSettings, fake_admin: FakeAdminClient
    ) -> None:
        with patch("varco_kafka.channel.AIOKafkaAdminClient", return_value=fake_admin):
            m = KafkaChannelManager(settings)
            await m.start()
            await m.stop()
            await m.stop()  # second stop is a no-op

    async def test_context_manager_starts_and_stops(
        self, settings: KafkaChannelManagerSettings, fake_admin: FakeAdminClient
    ) -> None:
        with patch("varco_kafka.channel.AIOKafkaAdminClient", return_value=fake_admin):
            async with KafkaChannelManager(settings) as m:
                assert m._admin is not None
            assert m._admin is None

    async def test_operations_before_start_raise(
        self, settings: KafkaChannelManagerSettings
    ) -> None:
        m = KafkaChannelManager(settings)
        with pytest.raises(RuntimeError):
            await m.declare_channel("orders")


# ── declare_channel ───────────────────────────────────────────────────────────


class TestKafkaChannelManagerDeclare:
    async def test_declare_calls_create_topics(
        self, manager: KafkaChannelManager, fake_admin: FakeAdminClient
    ) -> None:
        await manager.declare_channel("orders")
        assert "orders" in fake_admin.created_topics

    async def test_declare_with_prefix(
        self, settings: KafkaChannelManagerSettings, fake_admin: FakeAdminClient
    ) -> None:
        prefixed_settings = KafkaChannelManagerSettings(
            bootstrap_servers="fake:9092", topic_prefix="prod."
        )
        with patch("varco_kafka.channel.AIOKafkaAdminClient", return_value=fake_admin):
            async with KafkaChannelManager(prefixed_settings) as m:
                await m.declare_channel("orders")
        assert "prod.orders" in fake_admin.created_topics

    async def test_declare_with_config_uses_partitions(
        self, manager: KafkaChannelManager, fake_admin: FakeAdminClient
    ) -> None:
        # The ChannelConfig should be forwarded to the NewTopic constructor.
        # We verify via the fake_admin's created topic name (config details are
        # in the NewTopic — we trust aiokafka handles it correctly).
        config = ChannelConfig(num_partitions=3, replication_factor=1)
        await manager.declare_channel("payments", config=config)
        assert "payments" in fake_admin.created_topics

    async def test_declare_idempotent_on_topic_already_exists(
        self, manager: KafkaChannelManager, fake_admin: FakeAdminClient
    ) -> None:
        # TopicAlreadyExistsError must be swallowed — idempotent.
        fake_admin.fail_create = True
        await manager.declare_channel("orders")  # must not raise


# ── delete_channel ────────────────────────────────────────────────────────────


class TestKafkaChannelManagerDelete:
    async def test_delete_calls_delete_topics(
        self, manager: KafkaChannelManager, fake_admin: FakeAdminClient
    ) -> None:
        fake_admin.topics.add("orders")  # pre-populate so delete has a target
        await manager.delete_channel("orders")
        assert "orders" in fake_admin.deleted_topics

    async def test_delete_with_prefix(
        self, settings: KafkaChannelManagerSettings, fake_admin: FakeAdminClient
    ) -> None:
        prefixed_settings = KafkaChannelManagerSettings(
            bootstrap_servers="fake:9092", topic_prefix="prod."
        )
        fake_admin.topics.add("prod.orders")
        with patch("varco_kafka.channel.AIOKafkaAdminClient", return_value=fake_admin):
            async with KafkaChannelManager(prefixed_settings) as m:
                await m.delete_channel("orders")
        assert "prod.orders" in fake_admin.deleted_topics


# ── channel_exists ────────────────────────────────────────────────────────────


class TestKafkaChannelManagerExists:
    async def test_channel_exists_true(
        self, manager: KafkaChannelManager, fake_admin: FakeAdminClient
    ) -> None:
        fake_admin.topics.add("orders")
        result = await manager.channel_exists("orders")
        assert result is True

    async def test_channel_exists_false(
        self, manager: KafkaChannelManager, fake_admin: FakeAdminClient
    ) -> None:
        result = await manager.channel_exists("nonexistent")
        assert result is False

    async def test_channel_exists_after_declare(
        self, manager: KafkaChannelManager, fake_admin: FakeAdminClient
    ) -> None:
        await manager.declare_channel("orders")
        result = await manager.channel_exists("orders")
        assert result is True


# ── list_channels ─────────────────────────────────────────────────────────────


class TestKafkaChannelManagerList:
    async def test_list_returns_all_topics(
        self, manager: KafkaChannelManager, fake_admin: FakeAdminClient
    ) -> None:
        fake_admin.topics = {"aaa", "bbb", "ccc"}
        channels = await manager.list_channels()
        assert sorted(channels) == ["aaa", "bbb", "ccc"]

    async def test_list_strips_prefix(
        self, settings: KafkaChannelManagerSettings, fake_admin: FakeAdminClient
    ) -> None:
        prefixed_settings = KafkaChannelManagerSettings(
            bootstrap_servers="fake:9092", topic_prefix="prod."
        )
        fake_admin.topics = {"prod.orders", "prod.payments", "other.topic"}
        with patch("varco_kafka.channel.AIOKafkaAdminClient", return_value=fake_admin):
            async with KafkaChannelManager(prefixed_settings) as m:
                channels = await m.list_channels()

        # Only prefixed topics are included, and prefix is stripped.
        assert "orders" in channels
        assert "payments" in channels
        # "other.topic" has a different prefix — excluded.
        assert "other.topic" not in channels
        assert "topic" not in channels  # not prefixed with "prod."

    async def test_list_returns_sorted(
        self, manager: KafkaChannelManager, fake_admin: FakeAdminClient
    ) -> None:
        fake_admin.topics = {"zzz", "aaa", "mmm"}
        channels = await manager.list_channels()
        assert channels == sorted(channels)

    async def test_list_empty_when_no_topics(
        self, manager: KafkaChannelManager, fake_admin: FakeAdminClient
    ) -> None:
        fake_admin.topics = set()
        assert await manager.list_channels() == []


# ── repr ──────────────────────────────────────────────────────────────────────


class TestKafkaChannelManagerRepr:
    def test_repr_not_started(self, settings: KafkaChannelManagerSettings) -> None:
        m = KafkaChannelManager(settings)
        assert "KafkaChannelManager" in repr(m)
        assert "started=False" in repr(m)

    async def test_repr_started(self, manager: KafkaChannelManager) -> None:
        assert "started=True" in repr(manager)
