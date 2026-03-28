"""
varco_kafka.channel
====================
Kafka-backed ``ChannelManager`` implementation.

``KafkaChannelManager`` manages Kafka topic lifecycle — creation, deletion, and
listing — using an ``AIOKafkaAdminClient``.  It is intentionally separate from
``KafkaEventBus`` because topic management requires admin credentials that most
application services should NOT have.

Configuration
-------------
``KafkaChannelManagerSettings`` carries the admin client's bootstrap servers and
optional extra kwargs (SSL / SASL).  These settings are separate from
``KafkaEventBusSettings`` so you can configure different credentials per concern::

    # Application-level broker access (produce/consume only)
    bus_config = KafkaEventBusSettings(
        bootstrap_servers="kafka.internal:9092",
        group_id="my-service",
    )

    # Admin-level broker access (topic management — typically ops/infra only)
    manager_config = KafkaChannelManagerSettings(
        bootstrap_servers="kafka.internal:9092",
        admin_kwargs={"security_protocol": "SASL_SSL", ...},
    )

Usage::

    async with KafkaChannelManager(manager_config) as manager:
        await manager.declare_channel(
            "orders",
            ChannelConfig(num_partitions=6, replication_factor=3),
        )

DESIGN: separate from KafkaEventBus
    ✅ Admin credentials never bleed into the bus.
    ✅ Services that only produce/consume events have no admin dependency.
    ✅ Infrastructure scripts / migration tools can use KafkaChannelManager
       independently without instantiating a full event bus.
    ❌ Two objects instead of one — justified by the privilege separation.

Thread safety:  ❌  Not thread-safe.  Use from a single event loop.
Async safety:   ✅  All public methods are ``async def``.

📚 Docs
- 🔍 https://aiokafka.readthedocs.io/en/stable/api.html#aiokafka.admin.AIOKafkaAdminClient
  AIOKafkaAdminClient — topic management API
"""

from __future__ import annotations

import logging
from typing import Any

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from pydantic import Field
from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings
from varco_core.event.base import ChannelConfig
from varco_core.event.channel import ChannelManager

_logger = logging.getLogger(__name__)


# ── KafkaChannelManagerSettings ───────────────────────────────────────────────


class KafkaChannelManagerSettings(VarcoSettings):
    """
    Configuration for ``KafkaChannelManager``.

    Uses the ``VARCO_KAFKA_ADMIN_`` prefix to separate admin settings from
    the bus's ``VARCO_KAFKA_`` namespace.

    Attributes:
        bootstrap_servers: Comma-separated broker addresses to connect the
                           admin client to.  Usually the same as the bus
                           but may use dedicated admin credentials.
                           Env var: ``VARCO_KAFKA_ADMIN_BOOTSTRAP_SERVERS``.
        topic_prefix:      Prefix applied to all topic names — must match
                           the bus's ``channel_prefix`` so channels align.
                           Env var: ``VARCO_KAFKA_ADMIN_TOPIC_PREFIX``.
        admin_kwargs:      Extra kwargs forwarded to ``AIOKafkaAdminClient``.
                           Use for SSL, SASL, or other security settings.
                           **Not env-readable** — set via keyword args or
                           ``from_dict()``.

    Thread safety:  ✅ Immutable — frozen=True.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``topic_prefix`` must match the bus's ``channel_prefix`` exactly.
          A mismatch means the manager creates topics under different names
          than the bus expects.
    """

    model_config = SettingsConfigDict(
        env_prefix="VARCO_KAFKA_ADMIN_",
        frozen=True,
    )

    bootstrap_servers: str = "localhost:9092"
    """Admin client broker addresses.  Env: ``VARCO_KAFKA_ADMIN_BOOTSTRAP_SERVERS``."""

    topic_prefix: str = ""
    """Must match the bus's ``channel_prefix``.  Env: ``VARCO_KAFKA_ADMIN_TOPIC_PREFIX``."""

    admin_kwargs: dict[str, Any] = Field(default_factory=dict)
    """Extra kwargs for ``AIOKafkaAdminClient``.  Not env-readable."""

    def topic_name(self, channel: str) -> str:
        """
        Return the full Kafka topic name for a logical channel.

        Args:
            channel: Logical event channel name.

        Returns:
            Full topic name with prefix applied.
        """
        return f"{self.topic_prefix}{channel}"


# ── KafkaChannelManager ───────────────────────────────────────────────────────


class KafkaChannelManager(ChannelManager):
    """
    Kafka topic management via ``AIOKafkaAdminClient``.

    Implements ``ChannelManager`` for Kafka.  Requires admin-level broker access
    to create and delete topics.  All operations are idempotent.

    Lifecycle:
        Must be started before any channel operations::

            manager = KafkaChannelManager(settings)
            await manager.start()
            await manager.declare_channel("orders")
            await manager.stop()

        Or as an async context manager (preferred)::

            async with KafkaChannelManager(settings) as manager:
                await manager.declare_channel("orders")

    Args:
        settings: Admin client configuration.  Defaults to
                  ``KafkaChannelManagerSettings()`` (localhost, reads from env).

    Thread safety:  ❌  Not thread-safe.
    Async safety:   ✅  All public methods are ``async def``.

    Edge cases:
        - ``declare_channel`` is idempotent — creating an existing topic is
          silently ignored (Kafka's ``TopicAlreadyExistsError`` is swallowed).
        - ``delete_channel`` on a non-existent topic raises
          ``UnknownTopicOrPartitionError`` from aiokafka — let it propagate.
        - ``topic_prefix`` in settings MUST match the bus's ``channel_prefix``
          or topic names will diverge between the manager and the bus.
    """

    def __init__(self, settings: KafkaChannelManagerSettings | None = None) -> None:
        """
        Args:
            settings: Admin configuration.  Defaults to
                      ``KafkaChannelManagerSettings()`` (reads from env vars).
        """
        self._settings = settings or KafkaChannelManagerSettings()
        # Admin client is created in start() — aiokafka objects must be created
        # inside a running event loop.
        self._admin: AIOKafkaAdminClient | None = None

    # ── ChannelManager implementation ─────────────────────────────────────────

    async def start(self) -> None:
        """
        Connect the AIOKafkaAdminClient.

        Raises:
            RuntimeError:       If already started.
            NoBrokersAvailable: If the configured brokers are unreachable.
        """
        if self._admin is not None:
            raise RuntimeError(
                "KafkaChannelManager.start() called on an already-started manager. "
                "Call stop() first."
            )
        self._admin = AIOKafkaAdminClient(
            bootstrap_servers=self._settings.bootstrap_servers,
            **self._settings.admin_kwargs,
        )
        await self._admin.start()
        _logger.info(
            "KafkaChannelManager started (brokers=%s)",
            self._settings.bootstrap_servers,
        )

    async def stop(self) -> None:
        """Close the admin client.  Idempotent — safe to call multiple times."""
        if self._admin is None:
            return
        await self._admin.close()
        self._admin = None
        _logger.info("KafkaChannelManager stopped.")

    async def declare_channel(
        self,
        channel: str,
        config: ChannelConfig | None = None,
    ) -> None:
        """
        Create a Kafka topic for ``channel`` if it does not already exist.

        Args:
            channel: Logical channel name (e.g. ``"orders"``).
            config:  Optional channel configuration.  Defaults to
                     ``ChannelConfig()`` (1 partition, replication factor 1).

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Idempotent — ``TopicAlreadyExistsError`` is silently swallowed.
            - Does NOT modify an existing topic's partition count or replication.
        """
        self._require_started()
        cfg = config or ChannelConfig()
        topic = self._settings.topic_name(channel)
        new_topic = NewTopic(
            name=topic,
            num_partitions=cfg.num_partitions,
            replication_factor=cfg.replication_factor,
        )
        try:
            await self._admin.create_topics([new_topic])  # type: ignore[union-attr]
            _logger.info(
                "Declared Kafka topic %r (channel=%r, partitions=%d)",
                topic,
                channel,
                cfg.num_partitions,
            )
        except TopicAlreadyExistsError:
            # Idempotent — topic pre-exists, treat as success.
            _logger.debug("Topic %r already exists in Kafka — skipping create.", topic)

    async def delete_channel(self, channel: str) -> None:
        """
        Delete the Kafka topic for ``channel``.

        Args:
            channel: Logical channel name to delete.

        Raises:
            RuntimeError:                    If called before ``start()``.
            UnknownTopicOrPartitionError:    (aiokafka) If the topic does not exist.

        Edge cases:
            - Irreversible — all unread messages on the topic are permanently lost.
            - Active consumers on the topic will encounter errors on the next poll.
        """
        self._require_started()
        topic = self._settings.topic_name(channel)
        await self._admin.delete_topics([topic])  # type: ignore[union-attr]
        _logger.info("Deleted Kafka topic %r (channel=%r)", topic, channel)

    async def channel_exists(self, channel: str) -> bool:
        """
        Return ``True`` if the Kafka topic for ``channel`` exists on the broker.

        Queries the broker directly — not a local cache.

        Args:
            channel: Logical channel name to check.

        Returns:
            ``True`` if the corresponding Kafka topic exists.

        Raises:
            RuntimeError: If called before ``start()``.
        """
        self._require_started()
        topic = self._settings.topic_name(channel)
        # describe_topics() returns a list of dicts with shape:
        #   {"topic": str, "error_code": int, "partitions": [...], ...}
        # Key is "topic" (not "name") — matches aiokafka 0.13 to_object() output.
        # error_code 0 means NO_ERROR (topic exists and is healthy).
        metadata = await self._admin.describe_topics([topic])  # type: ignore[union-attr]
        for meta in metadata:
            if meta.get("topic") == topic and meta.get("error_code") == 0:
                return True
        return False

    async def list_channels(self) -> list[str]:
        """
        Return all Kafka topics visible to the admin client, stripped of prefix.

        Returns:
            Sorted list of logical channel names.

        Raises:
            RuntimeError: If called before ``start()``.

        Edge cases:
            - Returns ALL visible topics, including those from other services.
              Use a meaningful ``topic_prefix`` to scope to your service.
        """
        self._require_started()
        # list_topics() returns list[str] in aiokafka 0.13 — plain topic name strings.
        # Older aiokafka returned ClusterMetadata with a .topics dict; that API
        # was removed.  Sorting here keeps the output stable for callers.
        topics: list[str] = await self._admin.list_topics()  # type: ignore[union-attr]
        prefix = self._settings.topic_prefix
        channels = [
            name.removeprefix(prefix)
            for name in sorted(topics)
            if not prefix or name.startswith(prefix)
        ]
        return channels

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _require_started(self) -> None:
        """
        Raise ``RuntimeError`` if the admin client has not been started.

        Args: (none)

        Raises:
            RuntimeError: If ``start()`` has not been called.
        """
        if self._admin is None:
            raise RuntimeError(
                f"{type(self).__name__} is not started. "
                f"Call 'await manager.start()' or use 'async with manager' first."
            )

    def __repr__(self) -> str:
        started = self._admin is not None
        return (
            f"KafkaChannelManager("
            f"brokers={self._settings.bootstrap_servers!r}, "
            f"started={started})"
        )


__all__ = [
    "KafkaChannelManager",
    "KafkaChannelManagerSettings",
]
