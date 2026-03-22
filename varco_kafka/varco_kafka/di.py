"""
varco_kafka.di
==============
Providify DI wiring for ``KafkaEventBus``.

Install ``KafkaBusModule`` into your container to automatically bind
``AbstractEventBus`` → ``KafkaEventBus`` with a default ``KafkaConfig``::

    from varco_kafka.di import KafkaBusModule
    from varco_core.event import AbstractEventBus

    container = DIContainer()
    await container.ainstall(KafkaBusModule)

    bus = await container.aget(AbstractEventBus)  # → KafkaEventBus singleton

Overriding the default config::

    @Provider(singleton=True)
    def kafka_config() -> KafkaConfig:
        return KafkaConfig(
            bootstrap_servers=os.environ["KAFKA_BROKERS"],
            group_id=os.environ["SERVICE_NAME"],
            topic_prefix=os.environ.get("KAFKA_TOPIC_PREFIX", ""),
        )

    container.provide(kafka_config)       # register BEFORE install
    await container.ainstall(KafkaBusModule)  # module's KafkaConfig won't replace yours

DESIGN: provider-based wiring over class-binding
    Using ``@Provider(singleton=True) async def`` for ``AbstractEventBus``
    lets the provider call ``await bus.start()`` before the instance is
    returned to the caller.  Class bindings + ``@PostConstruct`` would also
    work but require the bus constructor to be DI-friendly (all params
    must be injectable types).

    Shutdown is handled by ``@PreDestroy`` on ``KafkaEventBus.stop()`` —
    Providify inspects every cached singleton's class on ``container.shutdown()``
    and calls any ``@PreDestroy`` method it finds, regardless of whether the
    instance was created by a class or provider binding.

    ✅ Single bus instance — ``@Singleton`` on ``KafkaEventBus`` guarantees
       at most one per container even if the module is installed multiple times.
    ✅ Async lifecycle — provider is ``async def``, safe to ``await bus.start()``.
    ✅ Config override — users register their own ``KafkaConfig`` provider
       BEFORE installing this module; Providify will use that instead.
    ❌ ``@PostConstruct`` is NOT called on provider-returned instances —
       the provider must handle initialisation explicitly.
"""

from __future__ import annotations

import logging

from providify import Configuration, Inject, Provider

from varco_core.event.base import AbstractEventBus

from varco_kafka.bus import KafkaEventBus
from varco_kafka.config import KafkaConfig

_logger = logging.getLogger(__name__)


@Configuration
class KafkaBusModule:
    """
    Providify ``@Configuration`` module that wires ``KafkaEventBus`` into
    any ``DIContainer`` with a single ``container.install(KafkaBusModule)`` call.

    Provides:
        ``KafkaConfig``     — default localhost config; override with your own provider.
        ``AbstractEventBus`` — ``KafkaEventBus`` singleton, started on creation.

    Lifecycle:
        ``AbstractEventBus`` singleton is started inside the provider.
        Shutdown is handled by ``@PreDestroy`` on ``KafkaEventBus.stop()``
        when the container is torn down via ``await container.ashutdown()``.

    Thread safety:  ✅  Providify singletons are created once and cached.
    Async safety:   ✅  Provider is ``async def`` — safe to ``await``.

    Example::

        container = DIContainer()
        await container.ainstall(KafkaBusModule)
        bus = await container.aget(AbstractEventBus)
        await bus.publish(MyEvent(...), channel="my-channel")
        await container.ashutdown()  # calls bus.stop() via @PreDestroy
    """

    @Provider(singleton=True)
    def kafka_config(self) -> KafkaConfig:
        """
        Default ``KafkaConfig`` pointing at ``localhost:9092``.

        Override by registering your own ``KafkaConfig`` provider in the
        container BEFORE installing this module.  Providify uses the first
        registered binding so the user-supplied provider takes precedence.

        Returns:
            A ``KafkaConfig`` with development-friendly defaults.
        """
        # Default config — suitable for local dev with a Docker Kafka broker.
        # Production code should override by providing a custom KafkaConfig
        # that reads bootstrap_servers / group_id from env vars or settings.
        return KafkaConfig()

    @Provider(singleton=True)
    async def kafka_event_bus(self, config: Inject[KafkaConfig]) -> AbstractEventBus:
        """
        Create, start, and return the ``KafkaEventBus`` singleton.

        The bus is started inside this provider so it is immediately ready to
        publish and consume events after ``container.aget(AbstractEventBus)``.
        Shutdown is handled by ``@PreDestroy`` on ``KafkaEventBus.stop()``.

        Args:
            config: ``KafkaConfig`` — injected from the container (either the
                    default provided by this module or a user-supplied override).

        Returns:
            A started ``KafkaEventBus`` bound to ``AbstractEventBus``.

        Raises:
            NoBrokersAvailable: (aiokafka) If the configured brokers are
                                unreachable at startup time.
        """
        _logger.info(
            "KafkaBusModule: starting KafkaEventBus (brokers=%s, group=%s)",
            config.bootstrap_servers,
            config.group_id,
        )
        bus = KafkaEventBus(config)
        # Start the bus here — @PostConstruct is NOT called on provider-returned
        # instances, so we must initialise the bus explicitly.
        await bus.start()
        return bus
