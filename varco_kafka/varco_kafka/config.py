"""
varco_kafka.config
==================
Configuration for the Kafka event bus backend.

``KafkaEventBusSettings`` is the configuration object for ``KafkaEventBus``.
It extends ``EventBusSettings`` so all Kafka connection settings are read from
environment variables automatically.

Environment variables (prefix ``VARCO_KAFKA_``)
-------------------------------------------------
::

    VARCO_KAFKA_BOOTSTRAP_SERVERS=kafka.internal:9092
    VARCO_KAFKA_GROUP_ID=my-service
    VARCO_KAFKA_TOPIC_PREFIX=prod.

Construction patterns::

    # From env (production)
    config = KafkaEventBusSettings.from_env()

    # Explicit (tests, DI override)
    config = KafkaEventBusSettings(
        bootstrap_servers="localhost:9092",
        group_id="my-service",
    )

    # From dict (DI wiring)
    config = KafkaEventBusSettings.from_dict({
        "bootstrap_servers": os.environ["KAFKA_BROKERS"],
        "group_id": os.environ["SERVICE_NAME"],
    })

DESIGN: Pydantic BaseSettings over frozen dataclass
    ✅ Env var reading is automatic — no ``os.environ`` boilerplate.
    ✅ Validates types at load time — invalid values fail at startup.
    ✅ Immutable after construction — prevents accidental mutation.
    ❌ ``producer_kwargs`` / ``consumer_kwargs`` cannot be set from plain
       env vars (would need JSON string).  Use keyword args or ``from_dict()``.

Thread safety:  ✅ Immutable after construction (frozen=True).
Async safety:   ✅ No mutable state.

📚 Docs
- 🔍 https://aiokafka.readthedocs.io/en/stable/api.html
  aiokafka — AIOKafkaProducer / AIOKafkaConsumer constructor options
- 🔍 https://docs.pydantic.dev/latest/concepts/pydantic_settings/
  Pydantic Settings — env_prefix, SettingsConfigDict
"""

from __future__ import annotations

import enum
from typing import Any

from pydantic import Field
from pydantic_settings import SettingsConfigDict
from varco_core.event.config import EventBusSettings

# ── KafkaDeliverySemantics ────────────────────────────────────────────────────


class KafkaDeliverySemantics(enum.StrEnum):
    """
    Delivery guarantee level for ``KafkaEventBus``.

    AT_MOST_ONCE  — Offsets committed before dispatch.  A consumer crash after
                    the commit but before the handler runs loses the message.
                    Lowest overhead; no duplicates, possible message loss.

    AT_LEAST_ONCE — Offsets committed manually after successful dispatch
                    (default).  A crash — or a raised handler exception —
                    between dispatch and commit leaves the offset
                    uncommitted, causing re-delivery to a fresh consumer in
                    the same ``group_id``.  Standard Kafka behavior;
                    duplicates possible.  NOTE: aiokafka's own periodic
                    auto-commit is never used for this mode (see
                    ``enable_auto_commit`` below) — it advances the
                    committed offset on a fixed timer independent of
                    handler success, which silently violates this
                    contract.

    EXACTLY_ONCE  — Dispatch and offset commit are wrapped in a Kafka
                    transaction (``send_offsets_to_transaction``).  Requires a
                    ``transactional_id`` in the settings.  The consumer uses
                    ``isolation_level=read_committed`` so it never sees
                    uncommitted (in-flight) messages from other transactional
                    producers.  Highest overhead; no duplicates, no message loss.

    DESIGN: str + enum.Enum
        ✅ Values are plain strings — JSON-serialisable for config / logging.
        ✅ env var readable: ``VARCO_KAFKA_DELIVERY_SEMANTICS=exactly_once``.

    Trade-offs documented in ARCHITECTURE.md:
        AT_MOST_ONCE  → throughput ↑↑, correctness ↓
        AT_LEAST_ONCE → throughput ↑,  correctness ~ (default)
        EXACTLY_ONCE  → throughput ↓,  correctness ↑↑
    """

    AT_MOST_ONCE = "at_most_once"
    AT_LEAST_ONCE = "at_least_once"
    EXACTLY_ONCE = "exactly_once"


# ── KafkaEventBusSettings ────────────────────────────────────────────────────


class KafkaEventBusSettings(EventBusSettings):
    """
    Immutable configuration for ``KafkaEventBus``.

    All fields are read from environment variables with the ``VARCO_KAFKA_``
    prefix.  Every field has a sensible default so a local Kafka broker can
    be used with no configuration::

        bus = KafkaEventBus(KafkaEventBusSettings())  # connects to localhost:9092

    Attributes:
        bootstrap_servers:    Comma-separated Kafka broker addresses.
                              Env var: ``VARCO_KAFKA_BOOTSTRAP_SERVERS``.
        group_id:             Consumer group ID.  All instances sharing the same
                              ``group_id`` form a consumer group — each message is
                              delivered to exactly ONE instance.
                              Env var: ``VARCO_KAFKA_GROUP_ID``.
        topic_prefix:         Prefix prepended to every topic name.
                              Alias for ``channel_prefix`` (inherited) but exposed
                              under the Kafka-idiomatic name via ``topic_name()``.
                              Env var: ``VARCO_KAFKA_TOPIC_PREFIX``.
        auto_offset_reset:    Offset policy for new consumer groups.
                              ``"earliest"`` replays from the start; ``"latest"``
                              (default) skips past messages already in the topic.
                              Env var: ``VARCO_KAFKA_AUTO_OFFSET_RESET``.
        enable_auto_commit:   Retained for backward compatibility only —
                              ``KafkaEventBus`` always disables aiokafka's
                              built-in periodic auto-commit and commits
                              offsets manually itself (before dispatch for
                              ``AT_MOST_ONCE``, after successful dispatch for
                              ``AT_LEAST_ONCE``, inside the transaction for
                              ``EXACTLY_ONCE``).  aiokafka's auto-commit
                              fires on a fixed timer regardless of handler
                              success, which would silently violate the
                              "committed after successful dispatch" contract
                              documented on ``KafkaDeliverySemantics``.
                              This field no longer changes bus behaviour;
                              it is deprecated and may be removed in a
                              future release.
                              Env var: ``VARCO_KAFKA_ENABLE_AUTO_COMMIT``.
        delivery_semantics:   Delivery guarantee level.  Default
                              ``AT_LEAST_ONCE``.  Set to ``EXACTLY_ONCE`` for
                              transactional producer semantics (requires
                              ``transactional_id``).
                              Env var: ``VARCO_KAFKA_DELIVERY_SEMANTICS``.
        transactional_id:     Unique producer identity for Kafka transactions.
                              **Required** when ``delivery_semantics`` is
                              ``EXACTLY_ONCE``.  Must be stable across restarts
                              (same value every time) so the broker can recover
                              incomplete transactions.
                              Env var: ``VARCO_KAFKA_TRANSACTIONAL_ID``.
        producer_kwargs:      Extra kwargs forwarded to ``AIOKafkaProducer``.
                              **Not env-readable** — use kwargs or ``from_dict()``.
        consumer_kwargs:      Extra kwargs forwarded to ``AIOKafkaConsumer``.
                              **Not env-readable** — use kwargs or ``from_dict()``.

    Thread safety:  ✅ Immutable — frozen=True.
    Async safety:   ✅ No mutable state.

    Edge cases:
        - ``topic_prefix`` (via ``channel_prefix``) is NOT applied retroactively.
        - ``group_id`` defaults to ``"varco-default"`` — always override in
          production to avoid cross-service interference.
        - Changing ``auto_offset_reset`` only affects NEW consumer groups —
          existing groups with committed offsets are unaffected.
    """

    model_config = SettingsConfigDict(env_prefix="VARCO_KAFKA_", frozen=True)

    bootstrap_servers: str = "localhost:9092"
    """Comma-separated broker addresses.  Env var: ``VARCO_KAFKA_BOOTSTRAP_SERVERS``."""

    group_id: str = "varco-default"
    """Consumer group ID.  Env var: ``VARCO_KAFKA_GROUP_ID``."""

    auto_offset_reset: str = "latest"
    """New consumer group offset policy: ``"earliest"`` or ``"latest"``."""

    enable_auto_commit: bool = True
    """Auto-commit offsets.  True = at-least-once delivery.  Ignored for AT_MOST_ONCE / EXACTLY_ONCE."""

    delivery_semantics: KafkaDeliverySemantics = KafkaDeliverySemantics.AT_LEAST_ONCE
    """Delivery guarantee level.  Env var: ``VARCO_KAFKA_DELIVERY_SEMANTICS``."""

    transactional_id: str | None = None
    """Unique producer transactional ID.  Required for EXACTLY_ONCE.  Env var: ``VARCO_KAFKA_TRANSACTIONAL_ID``."""

    # Extra kwargs forwarded verbatim to aiokafka — use for SSL / SASL.
    # Cannot be set from a plain env var — set via keyword args or from_dict().
    producer_kwargs: dict[str, Any] = Field(default_factory=dict)
    """Extra kwargs for ``AIOKafkaProducer``.  Not env-readable."""

    consumer_kwargs: dict[str, Any] = Field(default_factory=dict)
    """Extra kwargs for ``AIOKafkaConsumer``.  Not env-readable."""

    def topic_name(self, channel: str) -> str:
        """
        Return the full Kafka topic name for a logical event channel.

        Prepends ``channel_prefix`` (the topic prefix) if set.

        Args:
            channel: The logical event channel name (e.g. ``"orders"``).

        Returns:
            The full Kafka topic name (e.g. ``"prod.orders"``).

        Edge cases:
            - Empty ``channel_prefix`` → returns ``channel`` unchanged.
            - ``channel`` must be a valid Kafka topic name (no spaces,
              max 249 chars, allowed chars: letters, digits, dots, underscores,
              hyphens).  NOT validated here — Kafka rejects invalid names at
              produce time.
        """
        return f"{self.channel_prefix}{channel}"


# Plan 048 / DI-6, Step 9 — the `kafka_event_bus_settings` @Fallback
# @Provider that used to live here is gone. `KafkaEventBusSettings` is now
# registered eagerly, fail-fast, by `bind_settings()` inside
# `varco_kafka.di.bootstrap()` (before `scan()`) — a bad env value now
# fails a plain `bootstrap()` call instead of only failing lazily the
# first time something resolved the settings.


__all__ = [
    "KafkaDeliverySemantics",
    "KafkaEventBusSettings",
]
