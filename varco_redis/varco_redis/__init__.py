"""
varco_redis
===========
Redis event bus backends (Pub/Sub and Streams) for varco.

All public symbols are importable directly from ``varco_redis``::

    from varco_redis import RedisEventBus, RedisEventBusSettings
    from varco_redis import RedisStreamEventBus              # at-least-once
    from varco_redis import RedisChannelManager, RedisChannelManagerSettings
    from varco_redis import RedisEventBusConfiguration        # Providify DI (Pub/Sub)
    from varco_redis import RedisStreamConfiguration          # Providify DI (Streams)
    from varco_redis import RedisChannelManagerConfiguration  # Providify DI

Layer map::

    varco_core.event.AbstractEventBus
        ↑ implemented by
    varco_redis.RedisEventBus   ← THIS PACKAGE
        ↑ configured by
    varco_redis.RedisEventBusSettings
        ↑ wired by (optional)
    varco_redis.RedisEventBusConfiguration  ← Providify @Configuration

    varco_core.event.channel.ChannelManager
        ↑ implemented by
    varco_redis.RedisChannelManager
        ↑ configured by
    varco_redis.RedisChannelManagerSettings (alias for RedisEventBusSettings)
        ↑ wired by (optional)
    varco_redis.RedisChannelManagerConfiguration

Usage (standalone bus)::

    from varco_redis import RedisEventBus, RedisEventBusSettings
    from varco_core.event import BusEventProducer, listen, EventConsumer

    config = RedisEventBusSettings(url="redis://localhost:6379/0")

    async with RedisEventBus(config) as bus:
        # producer side
        producer = BusEventProducer(bus)
        await producer._produce(MyEvent(...), channel="my-channel")

        # consumer side
        class MyConsumer(EventConsumer):
            @listen(MyEvent, channel="my-channel")
            async def on_event(self, event: MyEvent) -> None:
                ...

        consumer = MyConsumer()
        consumer.register_to(bus)

Usage (Providify DI)::

    from providify import DIContainer
    from varco_redis import RedisEventBusConfiguration
    from varco_core.event import AbstractEventBus

    container = DIContainer()
    await container.ainstall(RedisEventBusConfiguration)

    bus = await container.aget(AbstractEventBus)  # RedisEventBus singleton
"""

from __future__ import annotations

from varco_redis.bus import RedisEventBus
from varco_redis.cache import RedisCache, RedisCacheConfiguration, RedisCacheSettings
from varco_redis.channel import RedisChannelManager, RedisChannelManagerSettings
from varco_redis.config import RedisEventBusSettings
from varco_redis.di import (
    RedisChannelManagerConfiguration,
    RedisEventBusConfiguration,
    RedisStreamConfiguration,
)
from varco_redis.dlq import RedisDLQ, RedisDLQConfiguration
from varco_redis.rate_limit import RedisRateLimiter
from varco_redis.streams import RedisStreamEventBus

__all__ = [
    # ── Pub/Sub bus ────────────────────────────────────────────────────────────
    "RedisEventBus",
    "RedisEventBusSettings",
    # ── Streams bus (at-least-once) ────────────────────────────────────────────
    "RedisStreamEventBus",
    # ── Channel management ─────────────────────────────────────────────────────
    "RedisChannelManager",
    "RedisChannelManagerSettings",
    # ── Cache ──────────────────────────────────────────────────────────────────
    "RedisCache",
    "RedisCacheSettings",
    "RedisCacheConfiguration",
    # ── Dead Letter Queue ──────────────────────────────────────────────────────
    "RedisDLQ",
    "RedisDLQConfiguration",
    # ── Rate limiting ──────────────────────────────────────────────────────────
    "RedisRateLimiter",
    # ── DI configurations ──────────────────────────────────────────────────────
    "RedisEventBusConfiguration",
    "RedisStreamConfiguration",
    "RedisChannelManagerConfiguration",
]
