"""
varco_core.event
================
General-purpose event system for varco applications.

All public symbols are importable directly from ``varco_core.event``::

    # Infrastructure
    from varco_core.event import AbstractEventBus, InMemoryEventBus
    from varco_core.event import ErrorPolicy, Subscription, EventMiddleware
    from varco_core.event import CHANNEL_ALL, CHANNEL_DEFAULT

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
from varco_core.event.consumer import EventConsumer, listen
from varco_core.event.domain import (
    EntityCreatedEvent,
    EntityDeletedEvent,
    EntityEvent,
    EntityUpdatedEvent,
)
from varco_core.event.memory import InMemoryEventBus, NoopEventBus
from varco_core.event.serializer import JsonEventSerializer
from varco_core.event.producer import (
    AbstractEventProducer,
    BusEventProducer,
    NoopEventProducer,
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
    # ── In-memory bus ───────────────────────────────────────────────────────
    "InMemoryEventBus",
    "NoopEventBus",
    # ── Serializer ──────────────────────────────────────────────────────────
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
]
