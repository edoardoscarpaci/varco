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

from typing import Any

# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(container: Any = None) -> Any:
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

        from varco_nats.di import bootstrap

        container = bootstrap()
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
        container: An existing ``DIContainer`` to scan into.  When ``None``,
                   ``DIContainer.current()`` is used — the process-level
                   singleton.

    Returns:
        The ``DIContainer`` after scanning, or ``None`` if providify is not
        installed.

    Edge cases:
        - Calling twice is safe — scanning is idempotent.
        - ``container.ashutdown()`` must be awaited at process exit to call
          ``NatsEventBus.stop()`` via its ``@PreDestroy`` hook.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ Scanning is synchronous; the function itself is sync.
    """
    try:
        from providify import DIContainer  # noqa: PLC0415
    except ImportError:
        return None

    if container is None:
        # Use the process-level singleton container so callers don't need to
        # pass it around — consistent with varco_kafka.di.bootstrap().
        container = DIContainer.current()

    # Discover all @Singleton/@Component classes in varco_nats recursively.
    container.scan("varco_nats", recursive=True)

    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "bootstrap",
]
