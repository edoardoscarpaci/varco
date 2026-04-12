"""
varco_redis.di
==============
Providify DI integration for ``varco_redis``.

All singletons (``RedisEventBusSettings``, ``RedisEventBus``,
``RedisStreamEventBus`` via selector, ``RedisChannelManager``,
``RedisHealthCheck``, ``RedisCache``) carry ``@Singleton`` or module-level
``@Provider`` annotations and are discovered automatically by
``container.scan("varco_redis", recursive=True)``.

No ``@Configuration`` class or ``ainstall()`` call is required.

Bus type selection
------------------
Set ``VARCO_REDIS_USE_STREAMS=true`` to activate ``RedisStreamEventBus``
(at-least-once, Redis Streams).  Default is ``RedisEventBus`` (at-most-once,
Pub/Sub).  The selector lives in ``varco_redis.bus._redis_active_bus`` and is
discovered automatically by scan.

Usage
-----
Pub/Sub (default)::

    from providify import DIContainer
    from varco_redis.di import bootstrap

    container = bootstrap()
    bus = await container.aget(AbstractEventBus)
    await container.ashutdown()

Or manually::

    container = DIContainer()
    container.scan("varco_redis", recursive=True)
    bus = await container.aget(AbstractEventBus)

Redis Streams (at-least-once)::

    # Set in environment before starting the process:
    # VARCO_REDIS_USE_STREAMS=true
    container.scan("varco_redis", recursive=True)

Overriding the default settings::

    container = DIContainer()
    container.provide(
        lambda: RedisEventBusSettings(url=os.environ["REDIS_URL"]),
        RedisEventBusSettings,
    )
    container.scan("varco_redis", recursive=True)
"""

from __future__ import annotations

from typing import Any


# ── Backward-compatibility aliases ────────────────────────────────────────────
# The old ``@Configuration`` classes are kept as no-op aliases so existing code
# that calls ``container.ainstall(RedisEventBusConfiguration)`` does not break.
# They no longer register any providers — scan handles everything.

try:
    from providify import Configuration  # noqa: PLC0415

    @Configuration
    class RedisEventBusConfiguration:
        """
        Backward-compatibility alias — no longer registers any providers.

        ``RedisEventBusSettings`` is now ``@Singleton``-decorated and
        discovered automatically by ``container.scan("varco_redis", recursive=True)``.
        The active bus (Pub/Sub or Streams) is selected by the ``_redis_active_bus``
        ``@Provider`` in ``varco_redis.bus``, also discovered by scan.
        """

    @Configuration
    class RedisChannelManagerConfiguration:
        """
        Backward-compatibility alias — no longer registers any providers.

        ``RedisChannelManager`` is ``@Singleton``-decorated and discovered
        automatically by scan.
        """

    @Configuration
    class RedisStreamConfiguration:
        """
        Backward-compatibility alias — no longer registers any providers.

        Set ``VARCO_REDIS_USE_STREAMS=true`` to activate ``RedisStreamEventBus``
        instead of installing this configuration class.
        """

except ImportError:
    RedisEventBusConfiguration = None  # type: ignore[assignment,misc]
    RedisChannelManagerConfiguration = None  # type: ignore[assignment,misc]
    RedisStreamConfiguration = None  # type: ignore[assignment,misc]


# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(
    container: Any = None,
    *,
    streams: bool = False,
) -> Any:
    """
    Bootstrap ``varco_redis`` into a ``DIContainer``.

    Calls ``container.scan("varco_redis", recursive=True)`` to discover all
    ``@Singleton``-annotated classes — ``RedisEventBusSettings``,
    ``RedisEventBus`` (or ``RedisStreamEventBus`` when
    ``VARCO_REDIS_USE_STREAMS=true``), ``RedisHealthCheck``,
    ``RedisChannelManager``, etc.

    No ``ainstall()`` call is required — settings are self-registering via
    ``@Singleton`` on the Pydantic ``BaseSettings`` subclass.

    Call this **once** at application startup::

        from varco_redis.di import bootstrap

        container = bootstrap()
        bus = await container.aget(AbstractEventBus)
        await container.ashutdown()

    To use Redis Streams, either pass ``streams=True`` or set the env var::

        container = bootstrap(streams=True)
        # Equivalent to: VARCO_REDIS_USE_STREAMS=true  (env var)

    Override settings before calling bootstrap::

        from varco_redis.config import RedisEventBusSettings
        from providify import DIContainer

        container = DIContainer()
        container.provide(
            lambda: RedisEventBusSettings(url=os.environ["REDIS_URL"]),
            RedisEventBusSettings,
        )
        bootstrap(container)

    Args:
        container: An existing ``DIContainer`` to scan into.
                   When ``None``, ``DIContainer.current()`` is used —
                   the process-level singleton.
        streams:   When ``True``, sets ``VARCO_REDIS_USE_STREAMS=true`` in
                   the process environment before scanning so the
                   ``_redis_active_bus`` selector returns ``RedisStreamEventBus``.
                   Prefer setting the env var directly in production.

    Returns:
        The ``DIContainer`` after scanning.

    Edge cases:
        - Calling twice is safe — scanning is idempotent.
        - ``container.ashutdown()`` must be awaited at process exit to call
          the bus ``stop()`` via its ``@PreDestroy`` hook.
        - ``streams=True`` mutates ``os.environ["VARCO_REDIS_USE_STREAMS"]``.
          This affects all subsequent ``RedisEventBusSettings()`` constructions
          in the same process.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ Scanning is synchronous.
    """
    import os  # noqa: PLC0415

    try:
        from providify import DIContainer  # noqa: PLC0415
    except ImportError:
        return None

    if streams:
        # Set env var before scan so RedisEventBusSettings() picks it up.
        # The @Singleton is constructed once during warm-up; changing the env
        # var afterwards has no effect.
        os.environ.setdefault("VARCO_REDIS_USE_STREAMS", "true")

    if container is None:
        container = DIContainer.current()

    # Discover all @Singleton/@Component classes and module-level @Provider
    # functions in varco_redis recursively.  This includes:
    #   - RedisEventBusSettings (@Singleton, reads VARCO_REDIS_* env vars)
    #   - _redis_active_bus (@Provider, selects Pub/Sub or Streams)
    #   - RedisHealthCheck, RedisChannelManager (@Singleton)
    #   - RedisCache and related classes (@Singleton)
    container.scan("varco_redis", recursive=True)

    return container


# ── async_bootstrap ───────────────────────────────────────────────────────────


async def async_bootstrap(
    container: Any = None,
    *,
    streams: bool = False,
    setup_cache: bool = False,
) -> Any:
    """
    Async version of :func:`bootstrap` that optionally starts the Redis cache.

    Combines the synchronous scan step with an optional async cache installation
    so the app's ``_bootstrap`` function doesn't need a separate
    ``await container.ainstall(RedisCacheConfiguration)`` call::

        async def _bootstrap() -> None:
            await redis_async_bootstrap(container, streams=True, setup_cache=True)
            # ↑ one call instead of two

        # Equivalent to:
        async def _bootstrap() -> None:
            redis_bootstrap(container, streams=True)
            await container.ainstall(RedisCacheConfiguration)

    Args:
        container:   An existing ``DIContainer`` to scan into.
                     When ``None``, ``DIContainer.current()`` is used.
        streams:     When ``True``, activates ``RedisStreamEventBus`` — same
                     as passing ``streams=True`` to :func:`bootstrap`.
        setup_cache: When ``True``, installs ``RedisCacheConfiguration`` after
                     scanning, which constructs and starts ``RedisCache``.
                     The cache is bound as ``CacheBackend`` in the container.
                     When ``False`` (default), no cache is installed.

    Returns:
        The ``DIContainer`` after scanning and optional cache installation.

    Raises:
        ConnectionError: If ``setup_cache=True`` and Redis is unreachable.

    Edge cases:
        - ``setup_cache=False`` is the same as calling :func:`bootstrap` — no
          async work is done.
        - The cache URL is read from ``VARCO_REDIS_CACHE_URL`` (via
          ``RedisCacheSettings.from_env()``), not from ``VARCO_REDIS_URL``.
          Both can point at the same Redis instance.

    Thread safety:  ✅ Intended for single-threaded startup only.
    Async safety:   ✅ ``async def`` — safe to ``await``.
    """
    container = bootstrap(container, streams=streams)

    if setup_cache:
        from varco_redis.cache import RedisCacheConfiguration  # noqa: PLC0415

        await container.ainstall(RedisCacheConfiguration)

    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    # Backward-compat aliases — kept so existing code doesn't break.
    "RedisEventBusConfiguration",
    "RedisChannelManagerConfiguration",
    "RedisStreamConfiguration",
    "bootstrap",
    "async_bootstrap",
]
