"""
varco_kafka.di
==============
Providify DI integration for ``varco_kafka``.

All singletons (``KafkaEventBus``, ``KafkaHealthCheck``, ``KafkaChannelManager``)
carry ``@Singleton`` on their class definitions and are discovered automatically
by ``container.scan("varco_kafka", recursive=True)``.

Settings classes are the exception: pydantic ``BaseSettings`` declares
``__init__(self, **values)``, a shape ``@Singleton`` must not be applied to
(see CLAUDE.md's pitfall table — on providify < 1.1.0 this raised
``LookupError: Cannot resolve 'values: typing.Any'``; current providify
skips ``**values`` outright, but the sanctioned shape must not depend on
that implementation detail).  They are therefore registered by
lowest-priority ``@Provider`` factories — ``kafka_event_bus_settings`` in
``varco_kafka.config``, ``kafka_channel_manager_settings`` in
``varco_kafka.channel``, and ``KafkaDLQConfiguration.kafka_dlq_settings`` in
``varco_kafka.dlq`` — which the same ``scan()`` discovers.

No ``@Configuration`` class or ``ainstall()`` call is required.

Usage
-----
Event bus only (most common)::

    from providify import DIContainer
    from varco_kafka.di import bootstrap

    container = bootstrap()
    bus = await container.aget(AbstractEventBus)
    await container.ashutdown()   # calls KafkaEventBus.stop() via @PreDestroy

Or manually::

    container = DIContainer()
    container.scan("varco_kafka", recursive=True)
    bus = await container.aget(AbstractEventBus)

Overriding the default settings::

    # ⚠️ @Provider-decorated module-level function: provide() rejects bare
    #    lambdas and takes no second "interface" argument (the return
    #    annotation is the interface).
    @Provider(singleton=True)
    def kafka_settings() -> KafkaEventBusSettings:
        return KafkaEventBusSettings(
            bootstrap_servers=os.environ["KAFKA_BROKERS"],
            group_id=os.environ["SERVICE_NAME"],
        )

    container = DIContainer()
    container.provide(kafka_settings)          # any order — the default is @Fallback
    container.scan("varco_kafka", recursive=True)
"""

from __future__ import annotations

from providify import DIContainer

# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(container: DIContainer) -> DIContainer:
    """
    Bootstrap ``varco_kafka`` into a ``DIContainer``.

    Calls ``container.scan("varco_kafka", recursive=True)`` to discover the
    ``@Singleton``-annotated classes (``KafkaEventBus``, ``KafkaHealthCheck``,
    ``KafkaChannelManager``) plus the ``@Provider`` factories that register
    ``KafkaEventBusSettings`` and ``KafkaChannelManagerSettings``.

    No ``ainstall()`` call is required.  Settings come from ``@Provider``
    factories rather than class-level ``@Singleton`` — a pydantic
    ``BaseSettings`` cannot be constructor-injected.

    Call this **once** at application startup, before resolving any singletons::

        from providify import DIContainer
        from varco_kafka.di import bootstrap

        container = DIContainer()
        bootstrap(container)
        bus = await container.aget(AbstractEventBus)
        await container.ashutdown()

    Override defaults before calling bootstrap::

        from varco_kafka.config import KafkaEventBusSettings
        from providify import DIContainer

        # Registered in any order — the package default is @Fallback.
        @Provider(singleton=True)
        def kafka_settings() -> KafkaEventBusSettings:
            return KafkaEventBusSettings(
                bootstrap_servers=os.environ["KAFKA_BROKERS"],
                group_id=os.environ["SERVICE_NAME"],
            )

        container = DIContainer()
        container.provide(kafka_settings)
        bootstrap(container)

    Args:
        container: The ``DIContainer`` to scan into. Required — this
            function never creates one and never reads
            the process-global container (Plan 046 §D-locator).

    Returns:
        The same ``container`` passed in, after scanning.

    Edge cases:
        - Calling twice is safe — scanning is idempotent; already-registered
          classes are not re-registered.
        - ``container.ashutdown()`` must be awaited at process exit to call
          ``KafkaEventBus.stop()`` via its ``@PreDestroy`` hook.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ Scanning is synchronous.  The function itself is sync.
    """
    # Discover all @Singleton/@Component classes in varco_kafka recursively.
    # KafkaEventBusSettings, KafkaChannelManagerSettings, KafkaEventBus,
    # KafkaHealthCheck, and KafkaChannelManager are all registered here.
    container.scan("varco_kafka", recursive=True)

    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "bootstrap",
]
