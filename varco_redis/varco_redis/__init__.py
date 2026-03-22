"""
varco_redis
===========
Redis Pub/Sub event bus backend for varco.

All public symbols are importable directly from ``varco_redis``::

    from varco_redis import RedisEventBus, RedisConfig
    from varco_redis import RedisBusModule   # Providify DI wiring

Layer map::

    varco_core.event.AbstractEventBus
        ↑ implemented by
    varco_redis.RedisEventBus   ← THIS PACKAGE
        ↑ configured by
    varco_redis.RedisConfig
        ↑ wired by (optional)
    varco_redis.RedisBusModule  ← Providify @Configuration module

Usage (standalone)::

    from varco_redis import RedisEventBus, RedisConfig
    from varco_core.event import BusEventProducer, listen, EventConsumer

    config = RedisConfig(url="redis://localhost:6379/0")

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
    from varco_redis import RedisBusModule
    from varco_core.event import AbstractEventBus

    container = DIContainer()
    await container.ainstall(RedisBusModule)

    bus = await container.aget(AbstractEventBus)  # RedisEventBus singleton
"""

from __future__ import annotations

from varco_redis.bus import RedisEventBus
from varco_redis.config import RedisConfig
from varco_redis.di import RedisBusModule

__all__ = [
    "RedisEventBus",
    "RedisConfig",
    "RedisBusModule",
]
