"""
varco_nats.di
=============
Providify DI integration for ``varco_nats``.

All singletons (``NatsEventBus``, ``NatsHealthCheck``, ``NatsStreamManager``)
carry ``@Singleton`` on their class definitions and are discovered automatically
by ``container.scan("varco_nats", recursive=True)``.

Settings classes are the exception: pydantic ``BaseSettings`` declares
``__init__(self, **values)``, a shape ``@Singleton`` must not be applied to
(see CLAUDE.md's pitfall table — on providify < 1.1.0 this raised
``LookupError: Cannot resolve 'values: typing.Any'``; current providify
skips ``**values`` outright, but the sanctioned shape must not depend on
that implementation detail).  They are therefore registered by
lowest-priority ``@Provider`` factories — ``nats_event_bus_settings`` in
``varco_nats.config``, ``nats_channel_manager_settings`` in
``varco_nats.channel``, and ``NatsDLQConfiguration.nats_dlq_settings`` in
``varco_nats.dlq`` — which the same ``scan()`` discovers.

No ``@Configuration`` class or ``ainstall()`` call is required for the bus.
The DLQ is the one exception — ``NatsDLQConfiguration`` (in ``varco_nats.dlq``)
must be installed explicitly when a DLQ is needed.

Usage
-----
Event bus only (most common)::

    from varco_nats.di import bootstrap
    from varco_core.event import AbstractEventBus

    container = bootstrap()
    bus = await container.aget(AbstractEventBus)
    await container.ashutdown()   # calls NatsEventBus.stop() via @PreDestroy

Or manually::

    from providify import DIContainer

    container = DIContainer()
    container.scan("varco_nats", recursive=True)
    bus = await container.aget(AbstractEventBus)

Overriding the default settings::

    # ⚠️ @Provider-decorated module-level function: provide() rejects bare
    #    lambdas and takes no second "interface" argument (the return
    #    annotation is the interface).
    @Provider(singleton=True)
    def nats_settings() -> NatsEventBusSettings:
        return NatsEventBusSettings(
            servers=os.environ["NATS_SERVERS"],
            durable_name=os.environ["SERVICE_NAME"],
        )

    container = DIContainer()
    container.provide(nats_settings)           # any order — the default is @Fallback
    container.scan("varco_nats", recursive=True)
"""

from __future__ import annotations

from providify import DIContainer

# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(container: DIContainer) -> DIContainer:
    """
    Bootstrap ``varco_nats`` into a ``DIContainer``.

    Calls ``container.scan("varco_nats", recursive=True)`` to discover the
    ``@Singleton``-annotated classes (``NatsEventBus``, ``NatsHealthCheck``,
    ``NatsStreamManager``) plus the ``@Provider`` factories that register
    ``NatsEventBusSettings`` and ``NatsChannelManagerSettings``.

    No ``ainstall()`` call is required for the bus.  Settings come from
    ``@Provider`` factories rather than class-level ``@Singleton`` — a pydantic
    ``BaseSettings`` cannot be constructor-injected.  Install
    ``NatsDLQConfiguration`` separately if a dead-letter queue is needed.

    Call this **once** at application startup, before resolving any singletons::

        from providify import DIContainer
        from varco_nats.di import bootstrap

        container = DIContainer()
        bootstrap(container)
        bus = await container.aget(AbstractEventBus)
        await container.ashutdown()

    Override defaults before calling bootstrap::

        from varco_nats.config import NatsEventBusSettings
        from providify import DIContainer

        # Registered in any order — the package default is @Fallback.
        @Provider(singleton=True)
        def nats_settings() -> NatsEventBusSettings:
            return NatsEventBusSettings(
                servers=os.environ["NATS_SERVERS"],
                durable_name=os.environ["SERVICE_NAME"],
            )

        container = DIContainer()
        container.provide(nats_settings)
        bootstrap(container)

    Args:
        container: The ``DIContainer`` to scan into. Required — this
            function never creates one and never reads
            the process-global container (Plan 046 §D-locator).

    Returns:
        The same ``container`` passed in, after scanning.

    Edge cases:
        - Calling twice is safe — scanning is idempotent.
        - ``container.ashutdown()`` must be awaited at process exit to call
          ``NatsEventBus.stop()`` via its ``@PreDestroy`` hook.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ Scanning is synchronous; the function itself is sync.
    """
    # Plan 048 / DI-6, Step 10 — NatsEventBusSettings/
    # NatsChannelManagerSettings are bound eagerly, fail-fast, via
    # bind_settings() BEFORE scan(), rather than the two module-level
    # @Fallback @Provider functions that used to live in
    # varco_nats.config/varco_nats.channel.
    from varco_core.settings import bind_settings

    from varco_nats.channel import NatsChannelManagerSettings
    from varco_nats.config import NatsEventBusSettings

    bind_settings(container, NatsEventBusSettings)
    bind_settings(container, NatsChannelManagerSettings)

    # Discover all @Singleton/@Component classes in varco_nats recursively.
    container.scan("varco_nats", recursive=True)

    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "bootstrap",
]
