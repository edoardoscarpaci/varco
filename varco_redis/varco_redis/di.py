"""
varco_redis.di
==============
Providify DI configuration for ``varco_redis``.

This module ships three independent ``@Configuration`` classes so users only
import what they need.  Each configuration provides **only** the settings
object — the concrete class singletons (``RedisEventBus``,
``RedisStreamEventBus``, ``RedisChannelManager``, ``RedisHealthCheck``) are
decorated with ``@Singleton`` and discovered automatically by
``container.scan("varco_redis")``.

``RedisEventBusConfiguration``
    Provides ``RedisEventBusSettings`` for ``RedisEventBus`` (Pub/Sub).
    At-most-once delivery — fast and simple for ephemeral events.

``RedisStreamConfiguration``
    Provides ``RedisEventBusSettings`` for ``RedisStreamEventBus`` (Streams).
    At-least-once delivery — use when events must not be lost.

``RedisChannelManagerConfiguration``
    Provides ``RedisEventBusSettings`` for ``RedisChannelManager``.
    Install only when you need runtime channel declarations.

Usage
-----
Event bus only (most common, at-most-once)::

    from varco_redis.di import RedisEventBusConfiguration
    from varco_core.event import AbstractEventBus

    container = DIContainer()
    await container.ainstall(RedisEventBusConfiguration)
    container.scan("varco_redis", recursive=True)

    bus = await container.aget(AbstractEventBus)
    await bus.publish(MyEvent(...), channel="my-channel")
    await container.ashutdown()   # calls bus.stop() via @PreDestroy

At-least-once (Streams)::

    from varco_redis.di import RedisStreamConfiguration

    container = DIContainer()
    await container.ainstall(RedisStreamConfiguration)
    container.scan("varco_redis", recursive=True)

Overriding the default config::

    from varco_redis.config import RedisEventBusSettings

    container = DIContainer()
    container.provide(lambda: RedisEventBusSettings(url=os.environ["REDIS_URL"]),
                      RedisEventBusSettings)
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

from providify import Configuration, Provider

from varco_redis.config import RedisEventBusSettings


# ── RedisEventBusConfiguration ────────────────────────────────────────────────


@Configuration
class RedisEventBusConfiguration:
    """
    Providify ``@Configuration`` providing ``RedisEventBusSettings`` for the
    Redis Pub/Sub event bus.

    The ``RedisEventBus`` singleton is registered automatically via its
    ``@Singleton`` decorator when ``container.scan("varco_redis")`` is called.

    Example::

        container = DIContainer()
        await container.ainstall(RedisEventBusConfiguration)
        container.scan("varco_redis", recursive=True)
        bus = await container.aget(AbstractEventBus)
    """

    @Provider(singleton=True)
    def redis_event_bus_settings(self) -> RedisEventBusSettings:
        """
        Default ``RedisEventBusSettings`` pointing at ``redis://localhost:6379/0``.

        Override by registering your own ``RedisEventBusSettings`` provider in
        the container BEFORE installing this configuration::

            container.provide(lambda: RedisEventBusSettings(url=os.environ["REDIS_URL"]),
                              RedisEventBusSettings)
            await container.ainstall(RedisEventBusConfiguration)

        Returns:
            A ``RedisEventBusSettings`` with development-friendly defaults.
        """
        return RedisEventBusSettings.from_env()


# ── RedisChannelManagerConfiguration ─────────────────────────────────────────


@Configuration
class RedisChannelManagerConfiguration:
    """
    Providify ``@Configuration`` providing ``RedisEventBusSettings`` for the
    Redis channel manager.

    The ``RedisChannelManager`` singleton is registered automatically via its
    ``@Singleton`` decorator when ``container.scan("varco_redis")`` is called.

    This configuration reuses ``RedisEventBusSettings`` — install after
    ``RedisEventBusConfiguration`` if you want them to share the same settings.

    Example::

        await container.ainstall(RedisEventBusConfiguration)
        await container.ainstall(RedisChannelManagerConfiguration)
        container.scan("varco_redis", recursive=True)

        manager = await container.aget(ChannelManager)
        await manager.declare_channel("orders")
    """

    @Provider(singleton=True)
    def redis_event_bus_settings(self) -> RedisEventBusSettings:
        """Default Redis settings for the channel manager."""
        return RedisEventBusSettings.from_env()


# ── RedisStreamConfiguration ──────────────────────────────────────────────────


@Configuration
class RedisStreamConfiguration:
    """
    Providify ``@Configuration`` providing ``RedisEventBusSettings`` for the
    Redis Streams event bus.

    Use this instead of ``RedisEventBusConfiguration`` when you need
    **at-least-once** delivery.  The ``RedisStreamEventBus`` singleton is
    registered automatically via its ``@Singleton`` decorator when
    ``container.scan("varco_redis")`` is called.

    Example::

        container = DIContainer()
        await container.ainstall(RedisStreamConfiguration)
        container.scan("varco_redis", recursive=True)
        bus = await container.aget(AbstractEventBus)
    """

    @Provider(singleton=True)
    def redis_event_bus_settings(self) -> RedisEventBusSettings:
        """Default Redis settings for the Streams bus."""
        return RedisEventBusSettings.from_env()
