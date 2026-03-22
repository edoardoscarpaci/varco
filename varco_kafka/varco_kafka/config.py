"""
varco_kafka.config
==================
Configuration value objects for ``KafkaEventBus``.

``KafkaConfig`` carries the minimal Kafka connection settings needed by the
bus.  All fields have sensible defaults so a local Kafka broker can be used
with no configuration::

    bus = KafkaEventBus(KafkaConfig())  # connects to localhost:9092

For production, override ``bootstrap_servers``, ``group_id``, and any
security settings::

    config = KafkaConfig(
        bootstrap_servers="kafka.internal:9092",
        group_id="my-service",
        topic_prefix="prod.",
    )

DESIGN: dataclass over raw dict / kwargs
    ✅ Type-checked at construction — no stringly-typed key lookups.
    ✅ Single import for all bus configuration — IDE autocomplete works.
    ✅ Defaults cover the common local-dev case.
    ❌ Changing a field name is a breaking API change — justified because
       configuration should be stable.

Thread safety:  ✅ Frozen dataclass — immutable after construction.
Async safety:   ✅ No mutable state.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class KafkaConfig:
    """
    Immutable configuration for ``KafkaEventBus``.

    Attributes:
        bootstrap_servers:  Comma-separated Kafka broker addresses.
                            Defaults to ``"localhost:9092"``.
        group_id:           Kafka consumer group ID.  All instances sharing
                            the same ``group_id`` form a consumer group —
                            each message is delivered to exactly ONE instance.
                            Use a unique ``group_id`` per service to avoid
                            competing consumers.
        topic_prefix:       Optional prefix prepended to every topic name.
                            Useful for namespace isolation across environments
                            (e.g. ``"dev."`` vs ``"prod."``).
        auto_offset_reset:  Offset policy for new consumer groups.
                            ``"earliest"`` — replay from the beginning of the
                            topic.  ``"latest"`` — skip past messages already
                            in the topic (default for production).
        enable_auto_commit: Whether the consumer auto-commits offsets.
                            ``True`` (default) means ``at-least-once`` delivery.
                            Set to ``False`` for manual offset management.
        producer_kwargs:    Extra keyword arguments forwarded to
                            ``AIOKafkaProducer``.  Use for SSL config, SASL,
                            compression, etc.
        consumer_kwargs:    Extra keyword arguments forwarded to
                            ``AIOKafkaConsumer``.  Same use cases as
                            ``producer_kwargs``.

    Edge cases:
        - ``topic_prefix`` is NOT applied retroactively — changing it after
          messages have been produced leaves orphaned topics.
        - ``group_id`` defaults to ``"varco-default"`` — override in
          production to avoid cross-service interference.
    """

    bootstrap_servers: str = "localhost:9092"
    group_id: str = "varco-default"
    topic_prefix: str = ""
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    # Extra kwargs forwarded verbatim to aiokafka — use for SSL / SASL settings.
    producer_kwargs: dict = field(default_factory=dict)
    consumer_kwargs: dict = field(default_factory=dict)

    def topic_name(self, channel: str) -> str:
        """
        Return the full Kafka topic name for a given event channel.

        Prepends ``topic_prefix`` if set.

        Args:
            channel: The logical event channel name (e.g. ``"orders"``).

        Returns:
            The full Kafka topic name (e.g. ``"prod.orders"``).

        Edge cases:
            - Empty ``topic_prefix`` → returns ``channel`` unchanged.
            - ``channel`` itself must be a valid Kafka topic name (no spaces,
              max 249 chars, allowed chars: letters, digits, dots, underscores,
              hyphens).  This is NOT validated here — Kafka will reject invalid
              names at produce time.
        """
        return f"{self.topic_prefix}{channel}"
