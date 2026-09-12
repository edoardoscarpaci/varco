"""
varco_redis.di
==============
Providify DI integration for ``varco_redis``.

All singletons (``RedisEventBus``, ``RedisStreamEventBus`` via selector,
``RedisChannelManager``, ``RedisHealthCheck``, ``RedisCache``) carry
``@Singleton`` on the class itself and are discovered automatically by
``container.scan("varco_redis", recursive=True)``.

``RedisEventBusSettings`` is the exception: pydantic ``BaseSettings``
declares ``__init__(self, **values)``, which is not a shape ``@Singleton``
should be applied to (see CLAUDE.md's pitfall table). It is registered
instead by a lowest-priority ``@Provider`` factory
(``redis_event_bus_settings`` in ``varco_redis.config``), same as
``RedisBackplaneSettings`` in ``varco_redis.backplane`` — both are
discovered by the same ``scan()``.

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

    # ⚠️ @Provider-decorated module-level function: provide() rejects bare
    #    lambdas and takes no second "interface" argument (the return
    #    annotation is the interface).
    @Provider(singleton=True)
    def redis_settings() -> RedisEventBusSettings:
        return RedisEventBusSettings(url=os.environ["REDIS_URL"])

    container = DIContainer()
    container.provide(redis_settings)          # before scan() — order matters
    container.scan("varco_redis", recursive=True)
"""

from __future__ import annotations

from typing import Any

from varco_core.revocation.base import AbstractTokenRevocationStore

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

        @Provider(singleton=True)
        def redis_settings() -> RedisEventBusSettings:
            return RedisEventBusSettings(url=os.environ["REDIS_URL"])

        container = DIContainer()
        container.provide(redis_settings)      # before bootstrap() — order matters
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
          the bus ``stop()`` via ``RedisEventBusSelectorConfiguration``'s
          ``@Disposes(AbstractEventBus)`` method — not ``@PreDestroy``, which
          providify never invokes on a ``@Provider``-produced instance
          (Plan 024 / C2).
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
        The ``DIContainer`` after scanning and optional cache installation, or
        ``None`` when providify is not installed — mirroring
        :func:`bootstrap`'s own contract. Prior to 3.0.0 this path raised
        ``AttributeError`` instead (Plan 022 / RIDER-1).

    Raises:
        ConnectionError: If ``setup_cache=True`` and Redis is unreachable.

    Edge cases:
        - ``setup_cache=False`` is the same as calling :func:`bootstrap` — no
          async work is done.
        - When providify is absent, ``None`` is returned and no cache is
          installed, regardless of ``setup_cache``.
        - The cache URL is read from ``VARCO_REDIS_CACHE_URL`` (via
          ``RedisCacheSettings.from_env()``), not from ``VARCO_REDIS_URL``.
          Both can point at the same Redis instance.

    Thread safety:  ✅ Intended for single-threaded startup only.
    Async safety:   ✅ ``async def`` — safe to ``await``.
    """
    container = bootstrap(container, streams=streams)

    # RIDER-1 (Plan 022 / Phase 3). ``bootstrap()`` returns ``None`` when
    # providify is absent (its own ``except ImportError: return None`` path),
    # so without this guard the next line raised
    # ``AttributeError: 'NoneType' object has no attribute 'ainstall'`` —
    # a crash on exactly the path documented as a graceful no-op. Returning
    # ``None`` here makes this function's contract match ``bootstrap()``'s.
    if container is None:
        return None

    if setup_cache:
        from varco_redis.cache import RedisCacheConfiguration  # noqa: PLC0415

        await container.ainstall(RedisCacheConfiguration)

    return container


# ── Token revocation (Plan 034 / S13b, Step 34) ──────────────────────────────


def enable_redis_token_revocation(container: Any, **redis_kwargs: Any) -> Any:
    """
    Opt in to ``RedisTokenRevocationStore`` as the application's
    ``AbstractTokenRevocationStore``, shadowing the always-off Null default
    (``varco_redis.revocation.RedisScanNullTokenRevocationStoreDefault``,
    a local subclass of ``varco_core.revocation.null.NullTokenRevocationStore`` —
    see that class's docstring for why it lives here rather than being
    picked up cross-package).

    Mirrors ``varco_core.revocation.di.enable_token_revocation``'s shape —
    same ``enable_*`` verb, same "binding a store here does not by itself
    wire ``TrustedIssuerRegistry``" caveat (§D-S13-di's two-step).

    Args:
        container:     The ``DIContainer`` already scanned via
                       ``container.scan("varco_redis", recursive=True)``.
        **redis_kwargs: Forwarded to ``RedisTokenRevocationStore()`` —
                       ``url``, ``namespace``, or any ``redis.asyncio.from_url()``
                       keyword.

    Returns:
        The same container, for chaining.

    Example::

        container = DIContainer()
        container.scan("varco_redis", recursive=True)
        enable_redis_token_revocation(container, url="redis://localhost:6379/0")

        store = await container.aget(AbstractTokenRevocationStore)
        registry = TrustedIssuerRegistry(revocation_store=store)  # the second step
    """
    from providify import Provider  # noqa: PLC0415

    from varco_redis.revocation import RedisTokenRevocationStore  # noqa: PLC0415

    @Provider(singleton=True)
    def _provide_redis_revocation_store() -> AbstractTokenRevocationStore:
        return RedisTokenRevocationStore(**redis_kwargs)

    container.provide(_provide_redis_revocation_store)
    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "bootstrap",
    "async_bootstrap",
    "enable_redis_token_revocation",
]
