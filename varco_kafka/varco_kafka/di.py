"""
varco_kafka.di
==============
Providify DI integration for ``varco_kafka``.

All singletons (``KafkaEventBus``, ``KafkaHealthCheck``, ``KafkaChannelManager``,
``KafkaEventBusSettings``, ``KafkaChannelManagerSettings``) carry ``@Singleton``
on their class definitions and are discovered automatically by
``container.scan("varco_kafka", recursive=True)``.

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

    container = DIContainer()
    container.provide(
        lambda: KafkaEventBusSettings(
            bootstrap_servers=os.environ["KAFKA_BROKERS"],
            group_id=os.environ["SERVICE_NAME"],
        ),
        KafkaEventBusSettings,
    )
    container.scan("varco_kafka", recursive=True)
"""

from __future__ import annotations

from typing import Any


# ── Backward-compatibility aliases ────────────────────────────────────────────
# ``KafkaEventBusConfiguration`` and ``KafkaChannelManagerConfiguration`` are
# kept as no-op ``@Configuration`` classes so existing code that calls
# ``container.ainstall(KafkaEventBusConfiguration)`` does not break.
# They no longer register any providers — scan handles everything.

try:
    from providify import Configuration  # noqa: PLC0415

    @Configuration
    class KafkaEventBusConfiguration:
        """
        Backward-compatibility alias — no longer registers any providers.

        ``KafkaEventBusSettings`` is now ``@Singleton``-decorated and
        discovered automatically by ``container.scan("varco_kafka", recursive=True)``.
        Calling ``container.ainstall(KafkaEventBusConfiguration)`` is a no-op.
        """

    @Configuration
    class KafkaChannelManagerConfiguration:
        """
        Backward-compatibility alias — no longer registers any providers.

        ``KafkaChannelManagerSettings`` is now ``@Singleton``-decorated and
        discovered automatically by ``container.scan("varco_kafka", recursive=True)``.
        Calling ``container.ainstall(KafkaChannelManagerConfiguration)`` is a no-op.
        """

except ImportError:
    KafkaEventBusConfiguration = None  # type: ignore[assignment,misc]
    KafkaChannelManagerConfiguration = None  # type: ignore[assignment,misc]


# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(
    container: Any = None,
) -> Any:
    """
    Bootstrap ``varco_kafka`` into a ``DIContainer``.

    Calls ``container.scan("varco_kafka", recursive=True)`` to discover all
    ``@Singleton``-annotated classes — ``KafkaEventBusSettings``,
    ``KafkaEventBus``, ``KafkaHealthCheck``, ``KafkaChannelManagerSettings``,
    and ``KafkaChannelManager``.

    No ``ainstall()`` call is required — settings are self-registering via
    ``@Singleton`` on the Pydantic ``BaseSettings`` subclasses.

    Call this **once** at application startup, before resolving any singletons::

        from varco_kafka.di import bootstrap

        container = bootstrap()
        bus = await container.aget(AbstractEventBus)
        await container.ashutdown()

    Override defaults before calling bootstrap::

        from varco_kafka.config import KafkaEventBusSettings
        from providify import DIContainer

        container = DIContainer()
        # Register a higher-priority provider — wins over the @Singleton default.
        container.provide(
            lambda: KafkaEventBusSettings(
                bootstrap_servers=os.environ["KAFKA_BROKERS"],
                group_id=os.environ["SERVICE_NAME"],
            ),
            KafkaEventBusSettings,
        )
        bootstrap(container)

    Args:
        container: An existing ``DIContainer`` to scan into.
                   When ``None``, ``DIContainer.current()`` is used —
                   the process-level singleton.

    Returns:
        The ``DIContainer`` after scanning.

    Edge cases:
        - Calling twice is safe — scanning is idempotent; already-registered
          classes are not re-registered.
        - ``container.ashutdown()`` must be awaited at process exit to call
          ``KafkaEventBus.stop()`` via its ``@PreDestroy`` hook.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ Scanning is synchronous.  The function itself is sync.
    """
    try:
        from providify import DIContainer  # noqa: PLC0415
    except ImportError:
        return None

    if container is None:
        # Use the process-level singleton container so callers don't need
        # to pass it around — consistent with create_varco_container().
        container = DIContainer.current()

    # Discover all @Singleton/@Component classes in varco_kafka recursively.
    # KafkaEventBusSettings, KafkaChannelManagerSettings, KafkaEventBus,
    # KafkaHealthCheck, and KafkaChannelManager are all registered here.
    container.scan("varco_kafka", recursive=True)

    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    # Backward-compat aliases — kept so existing code doesn't break.
    "KafkaEventBusConfiguration",
    "KafkaChannelManagerConfiguration",
    "bootstrap",
]
