"""
varco_memcached.di
==================
Providify DI integration for ``varco_memcached``.

Unlike event-bus backends where ``@Singleton`` classes are discovered
by ``container.scan()`` alone, ``MemcachedCache`` requires an async
``start()`` call to open its connection pool.  The async lifecycle is
managed by ``MemcachedCacheConfiguration`` (a ``@Configuration`` class),
which must be installed via ``await container.ainstall(...)`` — a sync
scan is not sufficient.

``bootstrap()`` performs the synchronous scan (discovers any ``@Singleton``
classes in ``varco_memcached``).  ``async_bootstrap()`` adds the async
``ainstall`` step that actually constructs and starts the cache.

Usage
-----
Typical app startup::

    from providify import DIContainer
    from varco_memcached.di import async_bootstrap
    from varco_core.cache import CacheBackend

    container = DIContainer()
    await async_bootstrap(container)
    cache = await container.aget(CacheBackend)
    await container.ashutdown()

Sync-only (settings discovery, no connection)::

    from providify import DIContainer
    from varco_memcached.di import bootstrap

    container = DIContainer()
    bootstrap(container)
    # Cache is NOT started — call async_bootstrap() or
    # ``await container.ainstall(MemcachedCacheConfiguration)`` separately.

Overriding settings (any order — the default is @Fallback)::

    from providify import DIContainer
    from varco_memcached.cache import MemcachedCacheSettings
    from varco_memcached.di import async_bootstrap

    # ⚠️ @Provider-decorated module-level function: provide() rejects bare
    #    lambdas and takes no second "interface" argument (the return
    #    annotation is the interface).
    @Provider(singleton=True)
    def memcached_settings() -> MemcachedCacheSettings:
        return MemcachedCacheSettings(host="memcached.internal", port=11211)

    container = DIContainer()
    container.provide(memcached_settings)   # any order — the default is @Fallback
    await async_bootstrap(container)

📚 Docs
- 🔍 https://github.com/aio-libs/aiomcache — aiomcache asyncio Memcached client
"""

from __future__ import annotations

from providify import DIContainer

# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(container: DIContainer) -> DIContainer:
    """
    Bootstrap ``varco_memcached`` into a ``DIContainer`` (sync scan only).

    Calls ``container.scan("varco_memcached", recursive=True)`` to discover
    any ``@Singleton``-annotated classes.  Does **not** install
    ``MemcachedCacheConfiguration`` — the cache connection pool is not
    opened until :func:`async_bootstrap` (or a manual
    ``await container.ainstall(MemcachedCacheConfiguration)``) is called.

    Call this at application startup if you need the sync scan step
    separate from the async activation::

        from providify import DIContainer
        from varco_memcached.di import bootstrap, async_bootstrap

        container = DIContainer()
        bootstrap(container)
        # ... other sync wiring ...
        await async_bootstrap(container)

    Args:
        container: The ``DIContainer`` to scan into. Required — this
            function never creates one and never reads
            the process-global container (Plan 046 §D-locator).

    Returns:
        The same ``container`` passed in, after scanning.

    Edge cases:
        - Calling twice is safe — scanning is idempotent.
        - The cache is NOT started after this call; use
          :func:`async_bootstrap` or call
          ``await container.ainstall(MemcachedCacheConfiguration)``
          to open the connection pool.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ Scanning is synchronous.
    """
    # Discover all @Singleton/@Component classes and module-level @Provider
    # functions in varco_memcached recursively.  Currently the package
    # exposes MemcachedCacheSettings (@Singleton) and MemcachedHealthCheck
    # (@Singleton).  MemcachedCache itself is registered by
    # MemcachedCacheConfiguration during ainstall().
    container.scan("varco_memcached", recursive=True)

    return container


# ── async_bootstrap ───────────────────────────────────────────────────────────


async def async_bootstrap(container: DIContainer) -> DIContainer:
    """
    Bootstrap ``varco_memcached`` and activate the Memcached cache (async).

    Combines the synchronous scan step with the async
    ``MemcachedCacheConfiguration`` installation so the app's startup
    function needs only one ``await`` call::

        async def _startup() -> None:
            await async_bootstrap(container)
            # ↑ equivalent to:
            #   bootstrap(container)
            #   await container.ainstall(MemcachedCacheConfiguration)

    After this call ``CacheBackend`` is resolvable from the container and
    the connection pool to Memcached has been opened. Plan 046 §D-contract
    removes the ``setup_cache=`` kwarg — call plain :func:`bootstrap` for a
    scan-only container.

    Args:
        container: The ``DIContainer`` to scan into. Required — see
            :func:`bootstrap`.

    Returns:
        The same ``container`` passed in, after scanning and cache
        installation.

    Raises:
        ConnectionRefusedError: If the Memcached server is unreachable
            when ``MemcachedCacheConfiguration.setup()`` runs.

    Edge cases:
        - Connection settings are read from ``VARCO_MEMCACHED_HOST``,
          ``VARCO_MEMCACHED_PORT``, and related env vars via
          ``MemcachedCacheSettings``.  Override by providing a custom
          ``MemcachedCacheSettings`` binding before calling this function.
        - ``await container.ashutdown()`` must be called at process exit to
          close the aiomcache connection pool via
          ``MemcachedCacheConfiguration``'s ``@Disposes(CacheBackend)``
          method. Not ``@PreDestroy`` — providify never invokes
          ``@PreDestroy`` on a ``@Provider``-produced instance; ``@Disposes``
          is the supported teardown path for provider output (Plan 024 / C2).

    Thread safety:  ✅ Intended for single-threaded startup only.
    Async safety:   ✅ ``async def`` — safe to ``await``.
    """
    bootstrap(container)

    # ainstall runs MemcachedCacheConfiguration.setup(), which constructs
    # MemcachedCache, calls cache.start() to open the aiomcache pool, and
    # binds the instance as CacheBackend in the container.
    from varco_memcached.cache import MemcachedCacheConfiguration  # noqa: PLC0415

    await container.ainstall(MemcachedCacheConfiguration)

    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "bootstrap",
    "async_bootstrap",
]
