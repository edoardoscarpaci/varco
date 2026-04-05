"""
varco_kafka.di
==============
Providify DI configuration for ``varco_kafka``.

This module ships two independent ``@Configuration`` classes so users only
import what they need.  Each configuration provides **only** the settings
object — the concrete class singletons (``KafkaEventBus``,
``KafkaChannelManager``, ``KafkaHealthCheck``) are decorated with
``@Singleton`` and discovered automatically by
``container.scan("varco_kafka")``.

``KafkaEventBusConfiguration``
    Provides ``KafkaEventBusSettings`` for the Kafka event bus.

``KafkaChannelManagerConfiguration``
    Provides ``KafkaChannelManagerSettings`` for admin topic management.
    Install only when you need to create/delete topics at runtime.
    Requires admin-level broker credentials — most services should NOT install this.

Usage
-----
Event bus only (most common)::

    from varco_kafka.di import KafkaEventBusConfiguration
    from varco_core.event import AbstractEventBus

    container = DIContainer()
    await container.ainstall(KafkaEventBusConfiguration)
    container.scan("varco_kafka", recursive=True)

    bus = await container.aget(AbstractEventBus)
    await bus.publish(MyEvent(...), channel="my-channel")
    await container.ashutdown()   # calls bus.stop() via @PreDestroy

Bus + channel management::

    await container.ainstall(KafkaEventBusConfiguration)
    await container.ainstall(KafkaChannelManagerConfiguration)
    container.scan("varco_kafka", recursive=True)

    manager = await container.aget(ChannelManager)
    await manager.declare_channel("orders", ChannelConfig(num_partitions=6))

Overriding the default configs::

    container.provide(lambda: KafkaEventBusSettings(
        bootstrap_servers=os.environ["KAFKA_BROKERS"],
        group_id=os.environ["SERVICE_NAME"],
    ), KafkaEventBusSettings)
    await container.ainstall(KafkaEventBusConfiguration)
"""

from __future__ import annotations

from providify import Configuration, Provider

from varco_kafka.channel import KafkaChannelManagerSettings
from varco_kafka.config import KafkaEventBusSettings


# ── KafkaEventBusConfiguration ────────────────────────────────────────────────


@Configuration
class KafkaEventBusConfiguration:
    """
    Providify ``@Configuration`` providing ``KafkaEventBusSettings`` for the
    Kafka event bus.

    The ``KafkaEventBus`` and ``KafkaHealthCheck`` singletons are registered
    automatically via their ``@Singleton`` decorators when
    ``container.scan("varco_kafka")`` is called.

    Example::

        container = DIContainer()
        await container.ainstall(KafkaEventBusConfiguration)
        container.scan("varco_kafka", recursive=True)
        bus = await container.aget(AbstractEventBus)
    """

    @Provider(singleton=True)
    def kafka_event_bus_settings(self) -> KafkaEventBusSettings:
        """
        Default ``KafkaEventBusSettings`` pointing at ``localhost:9092``.

        Override by registering your own ``KafkaEventBusSettings`` provider
        before installing this configuration::

            container.provide(lambda: KafkaEventBusSettings(
                bootstrap_servers=os.environ["KAFKA_BROKERS"],
                group_id=os.environ["SERVICE_NAME"],
            ), KafkaEventBusSettings)
            await container.ainstall(KafkaEventBusConfiguration)
        """
        return KafkaEventBusSettings.from_env()


# ── KafkaChannelManagerConfiguration ─────────────────────────────────────────


@Configuration
class KafkaChannelManagerConfiguration:
    """
    Providify ``@Configuration`` providing ``KafkaChannelManagerSettings``
    for admin topic management.

    The ``KafkaChannelManager`` singleton is registered automatically via its
    ``@Singleton`` decorator when ``container.scan("varco_kafka")`` is called.

    Uses ``KafkaChannelManagerSettings`` (env prefix ``VARCO_KAFKA_ADMIN_``),
    intentionally separate from ``KafkaEventBusSettings`` so admin credentials
    never bleed into the bus.

    Example::

        await container.ainstall(KafkaEventBusConfiguration)
        await container.ainstall(KafkaChannelManagerConfiguration)
        container.scan("varco_kafka", recursive=True)

        manager = await container.aget(ChannelManager)
        await manager.declare_channel("orders",
            ChannelConfig(num_partitions=6, replication_factor=3))
    """

    @Provider(singleton=True)
    def kafka_channel_manager_settings(self) -> KafkaChannelManagerSettings:
        """Default ``KafkaChannelManagerSettings`` (reads from ``VARCO_KAFKA_ADMIN_*`` env)."""
        return KafkaChannelManagerSettings.from_env()
