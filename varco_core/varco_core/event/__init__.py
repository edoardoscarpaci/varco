"""
varco_core.event
================
General-purpose event system for varco applications.

All public symbols are importable directly from ``varco_core.event``::

    # Infrastructure
    from varco_core.event import AbstractEventBus, InMemoryEventBus
    from varco_core.event import ErrorPolicy, Subscription, EventMiddleware
    from varco_core.event import CHANNEL_ALL, CHANNEL_DEFAULT

    # Channel management (separate from the bus — admin credentials not needed by bus)
    from varco_core.event import ChannelManager

    # Configuration
    from varco_core.event import EventBusSettings

    # Serializer
    from varco_core.event import EventSerializer, JsonEventSerializer

    # Producer side
    from varco_core.event import AbstractEventProducer
    from varco_core.event import BusEventProducer, NoopEventProducer

    # Consumer side
    from varco_core.event import EventConsumer, listen

    # Base event class
    from varco_core.event import Event

    # Domain events (entity lifecycle)
    from varco_core.event import EntityEvent
    from varco_core.event import EntityCreatedEvent, EntityUpdatedEvent, EntityDeletedEvent

    # Dead Letter Queue
    from varco_core.event import (
        AbstractDeadLetterQueue,
        DeadLetterEntry,
        InMemoryDeadLetterQueue,
    )

Layer map::

    User code
      ↓ depends on
    AbstractEventProducer  /  EventConsumer + @listen
      ↓ hides
    AbstractEventBus
      ↓ implemented by
    InMemoryEventBus  (this package)
    KafkaEventBus     (varco_kafka)
    RedisEventBus     (varco_redis)

    ChannelManager (channel lifecycle — separate from bus)
      ↓ implemented by
    KafkaChannelManager  (varco_kafka)
    RedisChannelManager  (varco_redis)

    AbstractDeadLetterQueue (DLQ — per-handler failure routing)
      ↓ implemented by
    InMemoryDeadLetterQueue  (this package, for tests)
    KafkaDLQ    (varco_kafka — publishes to a dedicated DLQ topic)
    RedisDLQ    (varco_redis — publishes to a dedicated DLQ channel/stream)
"""

from __future__ import annotations

from varco_core.event.base import (
    CHANNEL_ALL,
    CHANNEL_DEFAULT,
    AbstractEventBus,
    ChannelConfig,
    DispatchMode,
    ErrorPolicy,
    Event,
    EventMiddleware,
    Subscription,
)
from varco_core.event.channel import ChannelManager
from varco_core.event.config import EventBusSettings
from varco_core.event.consumer import EventConsumer, listen
from varco_core.event.domain import (
    EntityCreatedEvent,
    EntityDeletedEvent,
    EntityEvent,
    EntityUpdatedEvent,
)
from varco_core.event.memory import InMemoryEventBus, NoopEventBus
from varco_core.event.serializer import EventSerializer, JsonEventSerializer
from varco_core.event.producer import (
    AbstractEventProducer,
    BusEventProducer,
    NoopEventProducer,
)
from varco_core.event.dlq import (
    AbstractDeadLetterQueue,
    DeadLetterEntry,
    InMemoryDeadLetterQueue,
)
from varco_core.event.middleware import (
    CorrelationMiddleware,
    LoggingMiddleware,
    RetryMiddleware,
)

__all__ = [
    # ── Channel constants ───────────────────────────────────────────────────
    "CHANNEL_ALL",
    "CHANNEL_DEFAULT",
    # ── Infrastructure ──────────────────────────────────────────────────────
    "AbstractEventBus",
    "ChannelConfig",
    "ErrorPolicy",
    "Event",
    "EventMiddleware",
    "Subscription",
    # ── Dispatch mode ───────────────────────────────────────────────────────
    "DispatchMode",
    # ── Channel management ──────────────────────────────────────────────────
    "ChannelManager",
    # ── Configuration ───────────────────────────────────────────────────────
    "EventBusSettings",
    # ── In-memory bus ───────────────────────────────────────────────────────
    "InMemoryEventBus",
    "NoopEventBus",
    # ── Serializer ──────────────────────────────────────────────────────────
    "EventSerializer",
    "JsonEventSerializer",
    # ── Producer ────────────────────────────────────────────────────────────
    "AbstractEventProducer",
    "BusEventProducer",
    "NoopEventProducer",
    # ── Consumer ────────────────────────────────────────────────────────────
    "EventConsumer",
    "listen",
    # ── Domain events ───────────────────────────────────────────────────────
    "EntityEvent",
    "EntityCreatedEvent",
    "EntityUpdatedEvent",
    "EntityDeletedEvent",
    # ── Dead Letter Queue ────────────────────────────────────────────────────
    "AbstractDeadLetterQueue",
    "DeadLetterEntry",
    "InMemoryDeadLetterQueue",
    # ── Built-in middleware ──────────────────────────────────────────────────
    "LoggingMiddleware",
    "CorrelationMiddleware",
    "RetryMiddleware",
]
