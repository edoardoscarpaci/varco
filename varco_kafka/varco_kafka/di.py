"""
varco_kafka.di
==============
Providify DI wiring for ``KafkaEventBus`` and ``KafkaChannelManager``.

This module ships two independent ``@Configuration`` classes so users only
import what they need:

``KafkaEventBusConfiguration``
    Wires ``KafkaEventBus`` → ``AbstractEventBus``.  Install this when you need
    to publish or consume events over Kafka.

``KafkaChannelManagerConfiguration``
    Wires ``KafkaChannelManager`` → ``ChannelManager``.  Install this only when
    you need to manage topic declarations at runtime (create/delete topics).
    Requires admin-level broker credentials — most application services should
    NOT install this.

Usage
-----
Event bus only (most common)::

    from varco_kafka.di import KafkaEventBusConfiguration
    from varco_core.event import AbstractEventBus

    container = DIContainer()
    await container.ainstall(KafkaEventBusConfiguration)

    bus = await container.aget(AbstractEventBus)
    await bus.publish(MyEvent(...), channel="my-channel")
    await container.ashutdown()   # calls bus.stop() via @PreDestroy

Bus + channel management::

    from varco_kafka.di import KafkaEventBusConfiguration, KafkaChannelManagerConfiguration
    from varco_core.event import AbstractEventBus
    from varco_core.event.channel import ChannelManager

    container = DIContainer()
    await container.ainstall(KafkaEventBusConfiguration)
    await container.ainstall(KafkaChannelManagerConfiguration)

    manager = await container.aget(ChannelManager)
    await manager.declare_channel("orders", ChannelConfig(num_partitions=6))

    bus = await container.aget(AbstractEventBus)
    await bus.publish(...)

Overriding the default configs::

    from varco_kafka.config import KafkaEventBusSettings
    from varco_kafka.channel import KafkaChannelManagerSettings

    bus_settings = KafkaEventBusSettings(
        bootstrap_servers=os.environ["KAFKA_BROKERS"],
        group_id=os.environ["SERVICE_NAME"],
    )
    admin_settings = KafkaChannelManagerSettings(
        bootstrap_servers=os.environ["KAFKA_BROKERS"],
        admin_kwargs={"security_protocol": "SASL_SSL", ...},
    )

    container.provide(lambda: bus_settings, KafkaEventBusSettings)
    container.provide(lambda: admin_settings, KafkaChannelManagerSettings)
    await container.ainstall(KafkaEventBusConfiguration)
    await container.ainstall(KafkaChannelManagerConfiguration)

DESIGN: two separate @Configuration classes over one combined class
    ✅ Admin credentials (KafkaChannelManagerSettings) never bleed into bus.
    ✅ Most services install only the bus — no admin client is ever created.
    ✅ DI graph is explicit — no hidden conditional providers.
    ✅ Both configs can be overridden independently before installing.
    ❌ Two installs instead of one for the "bus + channel manager" case.
       Acceptable — privilege separation is worth the small extra verbosity.
"""

from __future__ import annotations

import logging

from providify import Configuration, Inject, Provider

from varco_core.event.base import AbstractEventBus
from varco_core.event.channel import ChannelManager

from varco_kafka.bus import KafkaEventBus
from varco_kafka.channel import KafkaChannelManager, KafkaChannelManagerSettings
from varco_kafka.config import KafkaEventBusSettings

_logger = logging.getLogger(__name__)


# ── KafkaEventBusConfiguration ────────────────────────────────────────────────


@Configuration
class KafkaEventBusConfiguration:
    """
    Providify ``@Configuration`` that wires ``KafkaEventBus`` into the container.

    Provides:
        ``KafkaEventBusSettings`` — default localhost settings; override before
                                    installing this configuration.
        ``AbstractEventBus``       — started ``KafkaEventBus`` singleton.

    Lifecycle:
        The bus is started inside the provider and stopped automatically by
        ``@PreDestroy`` on ``KafkaEventBus.stop()`` when
        ``await container.ashutdown()`` is called.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def`` — safe to ``await``.

    Example::

        container = DIContainer()
        await container.ainstall(KafkaEventBusConfiguration)
        bus = await container.aget(AbstractEventBus)
        await bus.publish(MyEvent(...), channel="orders")
        await container.ashutdown()
    """

    @Provider(singleton=True)
    def kafka_event_bus_settings(self) -> KafkaEventBusSettings:
        """
        Default ``KafkaEventBusSettings`` pointing at ``localhost:9092``.

        Override by registering your own ``KafkaEventBusSettings`` provider in
        the container BEFORE installing this configuration::

            container.provide(lambda: KafkaEventBusSettings(
                bootstrap_servers=os.environ["KAFKA_BROKERS"],
                group_id=os.environ["SERVICE_NAME"],
            ))
            await container.ainstall(KafkaEventBusConfiguration)

        Returns:
            A ``KafkaEventBusSettings`` with development-friendly defaults.
        """
        # Default settings — reads from VARCO_KAFKA_* env vars if set,
        # otherwise falls back to localhost:9092.
        return KafkaEventBusSettings.from_env()

    @Provider(singleton=True)
    async def kafka_event_bus(
        self,
        settings: Inject[KafkaEventBusSettings],
    ) -> AbstractEventBus:
        """
        Create, start, and return the ``KafkaEventBus`` singleton.

        The bus is started inside this provider so it is immediately ready
        to publish and consume events.  Shutdown is handled by ``@PreDestroy``
        on ``KafkaEventBus.stop()``.

        Args:
            settings: ``KafkaEventBusSettings`` — injected from the container.

        Returns:
            A started ``KafkaEventBus`` bound to ``AbstractEventBus``.

        Raises:
            NoBrokersAvailable: (aiokafka) If the configured brokers are
                                unreachable at startup time.
        """
        _logger.info(
            "KafkaEventBusConfiguration: starting KafkaEventBus "
            "(brokers=%s, group=%s)",
            settings.bootstrap_servers,
            settings.group_id,
        )
        # Plain class — no @Singleton on the class itself.
        # Singleton scope is enforced here via @Provider(singleton=True).
        bus = KafkaEventBus(settings)
        # @PostConstruct is NOT called on provider-returned instances — start explicitly.
        await bus.start()
        return bus


# ── KafkaChannelManagerConfiguration ─────────────────────────────────────────


@Configuration
class KafkaChannelManagerConfiguration:
    """
    Providify ``@Configuration`` that wires ``KafkaChannelManager`` into the
    container.

    Provides:
        ``KafkaChannelManagerSettings`` — default localhost admin settings;
                                          override before installing.
        ``ChannelManager``               — started ``KafkaChannelManager`` singleton.

    This configuration uses ``KafkaChannelManagerSettings`` (env prefix
    ``VARCO_KAFKA_ADMIN_``), which is intentionally separate from
    ``KafkaEventBusSettings`` so admin credentials never bleed into the bus.

    Install AFTER ``KafkaEventBusConfiguration`` if both are needed.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def``.

    Example (with bus)::

        await container.ainstall(KafkaEventBusConfiguration)
        await container.ainstall(KafkaChannelManagerConfiguration)

        manager = await container.aget(ChannelManager)
        await manager.declare_channel(
            "orders", ChannelConfig(num_partitions=6, replication_factor=3)
        )
    """

    @Provider(singleton=True)
    def kafka_channel_manager_settings(self) -> KafkaChannelManagerSettings:
        """
        Default ``KafkaChannelManagerSettings`` pointing at ``localhost:9092``
        with no extra admin kwargs.

        Override by registering your own ``KafkaChannelManagerSettings`` provider
        before installing this configuration::

            container.provide(lambda: KafkaChannelManagerSettings(
                bootstrap_servers=os.environ["KAFKA_BROKERS"],
                admin_kwargs={"security_protocol": "SASL_SSL"},
            ))
            await container.ainstall(KafkaChannelManagerConfiguration)

        Returns:
            A ``KafkaChannelManagerSettings`` with development-friendly defaults.
        """
        # Reads from VARCO_KAFKA_ADMIN_* env vars if set, otherwise localhost.
        return KafkaChannelManagerSettings.from_env()

    @Provider(singleton=True)
    async def kafka_channel_manager(
        self,
        settings: Inject[KafkaChannelManagerSettings],
    ) -> ChannelManager:
        """
        Create and start the ``KafkaChannelManager`` singleton.

        Args:
            settings: ``KafkaChannelManagerSettings`` — injected from the container.

        Returns:
            A started ``KafkaChannelManager`` bound to ``ChannelManager``.

        Raises:
            NoBrokersAvailable: (aiokafka) If the configured brokers are
                                unreachable at startup time.
            RuntimeError:       If the admin client fails to connect.
        """
        _logger.info(
            "KafkaChannelManagerConfiguration: starting KafkaChannelManager "
            "(brokers=%s)",
            settings.bootstrap_servers,
        )
        manager = KafkaChannelManager(settings)
        await manager.start()
        return manager
