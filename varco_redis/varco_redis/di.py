"""
varco_redis.di
==============
Providify DI wiring for ``RedisEventBus``, ``RedisStreamEventBus``, and
``RedisChannelManager``.

This module ships three independent ``@Configuration`` classes so users only
import what they need:

``RedisEventBusConfiguration``
    Wires ``RedisEventBus`` (Pub/Sub) → ``AbstractEventBus``.
    At-most-once delivery — fast and simple for ephemeral events.

``RedisStreamConfiguration``
    Wires ``RedisStreamEventBus`` (Streams) → ``AbstractEventBus``.
    At-least-once delivery — use when events must not be lost.

``RedisChannelManagerConfiguration``
    Wires ``RedisChannelManager`` → ``ChannelManager``.  Install this only when
    you need to manage channel declarations at runtime.  Most services don't need
    this — Redis Pub/Sub channels are created implicitly by subscribers.

Usage
-----
Event bus only (most common, at-most-once)::

    from varco_redis.di import RedisEventBusConfiguration
    from varco_core.event import AbstractEventBus

    container = DIContainer()
    await container.ainstall(RedisEventBusConfiguration)

    bus = await container.aget(AbstractEventBus)
    await bus.publish(MyEvent(...), channel="my-channel")
    await container.ashutdown()   # calls bus.stop() via @PreDestroy

At-least-once (Streams)::

    from varco_redis.di import RedisStreamConfiguration
    from varco_core.event import AbstractEventBus

    container = DIContainer()
    await container.ainstall(RedisStreamConfiguration)

    bus = await container.aget(AbstractEventBus)
    await bus.publish(MyEvent(...), channel="orders")
    await container.ashutdown()

Bus + channel management::

    from varco_redis.di import RedisEventBusConfiguration, RedisChannelManagerConfiguration
    from varco_core.event import AbstractEventBus
    from varco_core.event.channel import ChannelManager

    container = DIContainer()
    await container.ainstall(RedisEventBusConfiguration)
    await container.ainstall(RedisChannelManagerConfiguration)

    manager = await container.aget(ChannelManager)
    await manager.declare_channel("orders")

    bus = await container.aget(AbstractEventBus)
    await bus.publish(...)

Overriding the default config::

    from varco_redis.config import RedisEventBusSettings
    from providify import DIContainer, Provider

    custom_settings = RedisEventBusSettings(url="redis://my-host:6379/1")

    container = DIContainer()
    container.provide(lambda: custom_settings, RedisEventBusSettings)
    await container.ainstall(RedisEventBusConfiguration)

DESIGN: three separate @Configuration classes over one combined class
    ✅ Users who only need caching (varco_redis.cache) don't need bus DI.
    ✅ Pub/Sub and Streams users install exactly what they need — no wasted
       connections.
    ✅ DI graph is explicit — no hidden conditional providers.
    ❌ Two installs instead of one for the "bus + channel manager" case.
       Acceptable — composability is worth the small extra verbosity.
"""

from __future__ import annotations

import logging

from providify import Configuration, Inject, Provider

from varco_core.event.base import AbstractEventBus
from varco_core.event.channel import ChannelManager

from varco_redis.bus import RedisEventBus
from varco_redis.channel import RedisChannelManager
from varco_redis.config import RedisEventBusSettings
from varco_redis.streams import RedisStreamEventBus

_logger = logging.getLogger(__name__)


# ── RedisEventBusConfiguration ────────────────────────────────────────────────


@Configuration
class RedisEventBusConfiguration:
    """
    Providify ``@Configuration`` that wires ``RedisEventBus`` into the container.

    Provides:
        ``RedisEventBusSettings`` — default localhost settings; override before
                                    installing this configuration.
        ``AbstractEventBus``       — started ``RedisEventBus`` singleton.

    Lifecycle:
        The bus is started inside the provider and stopped automatically by
        ``@PreDestroy`` on ``RedisEventBus.stop()`` when
        ``await container.ashutdown()`` is called.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def`` — safe to ``await``.

    Example::

        container = DIContainer()
        await container.ainstall(RedisEventBusConfiguration)
        bus = await container.aget(AbstractEventBus)
        await bus.publish(MyEvent(...), channel="my-channel")
        await container.ashutdown()
    """

    @Provider(singleton=True)
    def redis_event_bus_settings(self) -> RedisEventBusSettings:
        """
        Default ``RedisEventBusSettings`` pointing at ``redis://localhost:6379/0``.

        Override by registering your own ``RedisEventBusSettings`` provider in
        the container BEFORE installing this configuration::

            container.provide(lambda: RedisEventBusSettings(url=os.environ["REDIS_URL"]))
            await container.ainstall(RedisEventBusConfiguration)

        Returns:
            A ``RedisEventBusSettings`` with development-friendly defaults.
        """
        # Default settings — reads from VARCO_REDIS_* env vars if set,
        # otherwise falls back to redis://localhost:6379/0.
        return RedisEventBusSettings.from_env()

    @Provider(singleton=True)
    async def redis_event_bus(
        self,
        settings: Inject[RedisEventBusSettings],
    ) -> AbstractEventBus:
        """
        Create, start, and return the ``RedisEventBus`` singleton.

        The bus is started inside this provider so it is immediately ready
        to publish and consume events.  Shutdown is handled by ``@PreDestroy``
        on ``RedisEventBus.stop()``.

        Args:
            settings: ``RedisEventBusSettings`` — injected from the container.

        Returns:
            A started ``RedisEventBus`` bound to ``AbstractEventBus``.

        Raises:
            ConnectionError: (redis.asyncio) If Redis is unreachable at startup.
        """
        _logger.info(
            "RedisEventBusConfiguration: starting RedisEventBus (url=%s)",
            settings.url,
        )
        # Plain class now — no @Singleton on the class itself.
        # The singleton scope is enforced here via @Provider(singleton=True).
        bus = RedisEventBus(settings)
        # @PostConstruct is NOT called on provider-returned instances — start explicitly.
        await bus.start()
        return bus


# ── RedisChannelManagerConfiguration ─────────────────────────────────────────


@Configuration
class RedisChannelManagerConfiguration:
    """
    Providify ``@Configuration`` that wires ``RedisChannelManager`` into the
    container.

    Provides:
        ``ChannelManager`` — started ``RedisChannelManager`` singleton.

    This configuration reuses the ``RedisEventBusSettings`` already registered
    by ``RedisEventBusConfiguration`` (or its own default).  Install AFTER
    ``RedisEventBusConfiguration`` if you want them to share the same settings.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def``.

    Example (with bus)::

        await container.ainstall(RedisEventBusConfiguration)
        await container.ainstall(RedisChannelManagerConfiguration)

        manager = await container.aget(ChannelManager)
        await manager.declare_channel("orders")
    """

    @Provider(singleton=True)
    async def redis_channel_manager(
        self,
        settings: Inject[RedisEventBusSettings],
    ) -> ChannelManager:
        """
        Create and start the ``RedisChannelManager`` singleton.

        Args:
            settings: ``RedisEventBusSettings`` injected from the container.
                      If ``RedisEventBusConfiguration`` was installed first,
                      the same settings instance is reused.

        Returns:
            A started ``RedisChannelManager`` bound to ``ChannelManager``.
        """
        _logger.info(
            "RedisChannelManagerConfiguration: starting RedisChannelManager (url=%s)",
            settings.url,
        )
        manager = RedisChannelManager(settings)
        await manager.start()
        return manager


# ── RedisStreamConfiguration ──────────────────────────────────────────────────


@Configuration
class RedisStreamConfiguration:
    """
    Providify ``@Configuration`` that wires ``RedisStreamEventBus`` into the
    container.

    Use this instead of ``RedisEventBusConfiguration`` when you need
    **at-least-once** delivery — messages published while a consumer is down
    are retained in the stream and redelivered on reconnect.

    Provides:
        ``RedisEventBusSettings`` — default localhost settings; override before
                                    installing this configuration.
        ``AbstractEventBus``       — started ``RedisStreamEventBus`` singleton.

    The bus is bound to the same ``AbstractEventBus`` interface as the Pub/Sub
    variant — application code does not need to change when switching backends.

    Lifecycle:
        The bus is started inside the provider and stopped automatically by
        ``@PreDestroy`` on ``RedisStreamEventBus.stop()`` when
        ``await container.ashutdown()`` is called.

    DESIGN: separate @Configuration from RedisEventBusConfiguration
        ✅ Users pick exactly one delivery semantic — no ambiguity.
        ✅ Both bind to ``AbstractEventBus`` — application code is backend-agnostic.
        ❌ Cannot install both at the same time — both bind the same interface.
           Install only one per container.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def`` — safe to ``await``.

    Example::

        container = DIContainer()
        await container.ainstall(RedisStreamConfiguration)
        bus = await container.aget(AbstractEventBus)
        bus.subscribe(OrderPlacedEvent, my_handler, channel="orders")
        await bus.publish(OrderPlacedEvent(...), channel="orders")
        await container.ashutdown()
    """

    @Provider(singleton=True)
    def redis_event_bus_settings(self) -> RedisEventBusSettings:
        """
        Default ``RedisEventBusSettings`` pointing at ``redis://localhost:6379/0``.

        Override by registering your own ``RedisEventBusSettings`` provider in
        the container BEFORE installing this configuration::

            container.provide(lambda: RedisEventBusSettings(url=os.environ["REDIS_URL"]))
            await container.ainstall(RedisStreamConfiguration)

        Returns:
            A ``RedisEventBusSettings`` with development-friendly defaults.
        """
        # Default settings — reads from VARCO_REDIS_* env vars if set.
        return RedisEventBusSettings.from_env()

    @Provider(singleton=True)
    async def redis_stream_event_bus(
        self,
        settings: Inject[RedisEventBusSettings],
    ) -> AbstractEventBus:
        """
        Create, start, and return the ``RedisStreamEventBus`` singleton.

        The bus is started inside this provider so it is immediately ready
        to publish and consume events.  Shutdown is handled by ``@PreDestroy``
        on ``RedisStreamEventBus.stop()``.

        Args:
            settings: ``RedisEventBusSettings`` — injected from the container.

        Returns:
            A started ``RedisStreamEventBus`` bound to ``AbstractEventBus``.

        Raises:
            ConnectionError: (redis.asyncio) If Redis is unreachable at startup.
        """
        _logger.info(
            "RedisStreamConfiguration: starting RedisStreamEventBus (url=%s)",
            settings.url,
        )
        bus = RedisStreamEventBus(settings)
        # @PostConstruct is NOT called on provider-returned instances — start explicitly.
        await bus.start()
        return bus
