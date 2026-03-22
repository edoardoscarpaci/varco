"""
varco_redis.di
==============
Providify DI wiring for ``RedisEventBus``.

Install ``RedisBusModule`` into your container to automatically bind
``AbstractEventBus`` → ``RedisEventBus`` with a default ``RedisConfig``::

    from varco_redis.di import RedisBusModule
    from varco_core.event import AbstractEventBus

    container = DIContainer()
    await container.ainstall(RedisBusModule)

    bus = await container.aget(AbstractEventBus)  # → RedisEventBus singleton

Overriding the default config::

    @Provider(singleton=True)
    def redis_config() -> RedisConfig:
        return RedisConfig(
            url=os.environ["REDIS_URL"],
            channel_prefix=os.environ.get("REDIS_CHANNEL_PREFIX", ""),
        )

    container.provide(redis_config)           # register BEFORE install
    await container.ainstall(RedisBusModule)  # module's RedisConfig won't replace yours

DESIGN: mirrors ``KafkaBusModule`` — see ``varco_kafka.di`` for full rationale.
"""

from __future__ import annotations

import logging

from providify import Configuration, Inject, Provider

from varco_core.event.base import AbstractEventBus

from varco_redis.bus import RedisEventBus
from varco_redis.config import RedisConfig

_logger = logging.getLogger(__name__)


@Configuration
class RedisBusModule:
    """
    Providify ``@Configuration`` module that wires ``RedisEventBus`` into
    any ``DIContainer`` with a single ``container.install(RedisBusModule)`` call.

    Provides:
        ``RedisConfig``      — default localhost config; override with your own provider.
        ``AbstractEventBus`` — ``RedisEventBus`` singleton, started on creation.

    Lifecycle:
        ``AbstractEventBus`` singleton is started inside the provider.
        Shutdown is handled by ``@PreDestroy`` on ``RedisEventBus.stop()``
        when the container is torn down via ``await container.ashutdown()``.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def`` — safe to ``await``.

    Example::

        container = DIContainer()
        await container.ainstall(RedisBusModule)
        bus = await container.aget(AbstractEventBus)
        await bus.publish(MyEvent(...), channel="my-channel")
        await container.ashutdown()  # calls bus.stop() via @PreDestroy
    """

    @Provider(singleton=True)
    def redis_config(self) -> RedisConfig:
        """
        Default ``RedisConfig`` pointing at ``redis://localhost:6379/0``.

        Override by registering your own ``RedisConfig`` provider in the
        container BEFORE installing this module.

        Returns:
            A ``RedisConfig`` with development-friendly defaults.
        """
        # Default config — suitable for local dev with a Docker Redis instance.
        # Production code should override by providing a custom RedisConfig
        # that reads url / channel_prefix from env vars or settings.
        return RedisConfig()

    @Provider(singleton=True)
    async def redis_event_bus(self, config: Inject[RedisConfig]) -> AbstractEventBus:
        """
        Create, start, and return the ``RedisEventBus`` singleton.

        The bus is started inside this provider so it is immediately ready to
        publish and consume events after ``container.aget(AbstractEventBus)``.
        Shutdown is handled by ``@PreDestroy`` on ``RedisEventBus.stop()``.

        Args:
            config: ``RedisConfig`` — injected from the container (either the
                    default provided by this module or a user-supplied override).

        Returns:
            A started ``RedisEventBus`` bound to ``AbstractEventBus``.

        Raises:
            ConnectionError: (redis.asyncio) If Redis is unreachable at startup.
        """
        _logger.info(
            "RedisBusModule: starting RedisEventBus (url=%s)",
            config.url,
        )
        bus = RedisEventBus(config)
        # Start the bus here — @PostConstruct is NOT called on provider-returned
        # instances, so we must initialise the bus explicitly.
        await bus.start()
        return bus
