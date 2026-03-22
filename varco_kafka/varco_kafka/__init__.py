"""
varco_kafka
===========
Apache Kafka event bus backend for varco.

All public symbols are importable directly from ``varco_kafka``::

    from varco_kafka import KafkaEventBus, KafkaConfig
    from varco_kafka import KafkaBusModule   # Providify DI wiring

Layer map::

    varco_core.event.AbstractEventBus
        ↑ implemented by
    varco_kafka.KafkaEventBus   ← THIS PACKAGE
        ↑ configured by
    varco_kafka.KafkaConfig
        ↑ wired by (optional)
    varco_kafka.KafkaBusModule  ← Providify @Configuration module

Usage (standalone)::

    from varco_kafka import KafkaEventBus, KafkaConfig
    from varco_core.event import BusEventProducer, listen, EventConsumer

    config = KafkaConfig(
        bootstrap_servers="localhost:9092",
        group_id="my-service",
    )

    async with KafkaEventBus(config) as bus:
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
    from varco_kafka import KafkaBusModule
    from varco_core.event import AbstractEventBus

    container = DIContainer()
    await container.ainstall(KafkaBusModule)

    bus = await container.aget(AbstractEventBus)  # KafkaEventBus singleton
"""

from __future__ import annotations

from varco_kafka.bus import KafkaEventBus
from varco_kafka.config import KafkaConfig
from varco_kafka.di import KafkaBusModule

__all__ = [
    "KafkaEventBus",
    "KafkaConfig",
    "KafkaBusModule",
]
