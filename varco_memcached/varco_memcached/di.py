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

    from varco_memcached.di import async_bootstrap
    from varco_core.cache import CacheBackend

    container = await async_bootstrap()
    cache = await container.aget(CacheBackend)
    await container.ashutdown()

Sync-only (settings discovery, no connection)::

    from varco_memcached.di import bootstrap

    container = bootstrap()
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

import logging
from typing import Any

_logger = logging.getLogger(__name__)


# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(container: Any = None) -> Any:
    """
    Bootstrap ``varco_memcached`` into a ``DIContainer`` (sync scan only).

    Calls ``container.scan("varco_memcached", recursive=True)`` to discover
    any ``@Singleton``-annotated classes.  Does **not** install
    ``MemcachedCacheConfiguration`` — the cache connection pool is not
    opened until :func:`async_bootstrap` (or a manual
    ``await container.ainstall(MemcachedCacheConfiguration)``) is called.

    Call this at application startup if you need the sync scan step
    separate from the async activation::

        from varco_memcached.di import bootstrap, async_bootstrap

        container = bootstrap()
        # ... other sync wiring ...
        await async_bootstrap(container)

    Args:
        container: An existing ``DIContainer`` to scan into.
                   When ``None``, ``DIContainer.current()`` is used —
                   the process-level singleton.

    Returns:
        The ``DIContainer`` after scanning, or ``None`` if ``providify``
        is not installed.

    Edge cases:
        - Returns ``None`` (not a container) when ``providify`` is absent so
          callers can safely ignore the return value in environments where
          the DI framework is optional.
        - Calling twice is safe — scanning is idempotent.
        - The cache is NOT started after this call; use
          :func:`async_bootstrap` or call
          ``await container.ainstall(MemcachedCacheConfiguration)``
          to open the connection pool.

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ Scanning is synchronous.
    """
    try:
        # Guard: providify is an optional dependency — importing it here
        # (rather than at module level) means this module loads cleanly
        # even when providify is absent (e.g. in test environments that
        # only use InMemoryCache without the DI container).
        from providify import DIContainer  # noqa: PLC0415
    except ImportError:
        # Providify not installed — return None so callers that check
        # the return value can branch accordingly, and callers that
        # ignore the return value continue without error.
        _logger.debug("providify not installed; varco_memcached DI helpers are no-ops")
        return None

    if container is None:
        # Fall back to the process-level singleton so the most common usage
        # (a single-container app) requires no explicit container argument.
        container = DIContainer.current()

    # Discover all @Singleton/@Component classes and module-level @Provider
    # functions in varco_memcached recursively.  Currently the package
    # exposes MemcachedCacheSettings (@Singleton) and MemcachedHealthCheck
    # (@Singleton).  MemcachedCache itself is registered by
    # MemcachedCacheConfiguration during ainstall().
    container.scan("varco_memcached", recursive=True)

    return container


# ── async_bootstrap ───────────────────────────────────────────────────────────


async def async_bootstrap(container: Any = None, *, setup_cache: bool = True) -> Any:
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
    the connection pool to Memcached has been opened.

    Args:
        container: An existing ``DIContainer`` to scan into.
                   When ``None``, ``DIContainer.current()`` is used.
        setup_cache: When ``True`` (default — unchanged from before this
                   parameter existed), installs ``MemcachedCacheConfiguration``
                   after scanning, which constructs and starts
                   ``MemcachedCache``. The cache is bound as ``CacheBackend``
                   in the container. When ``False``, this call is exactly
                   :func:`bootstrap` — no connection pool is opened. Mirrors
                   ``varco_redis.di.async_bootstrap(..., setup_cache=...)``
                   in *shape* — see the cross-reference paragraph below for
                   why the *defaults* still differ.

    Returns:
        The ``DIContainer`` after scanning and (when ``setup_cache=True``)
        cache installation, or ``None`` if ``providify`` is not installed
        (same as :func:`bootstrap`).

    Raises:
        ConnectionRefusedError: If ``setup_cache=True`` and the Memcached
                                server is unreachable when
                                ``MemcachedCacheConfiguration.setup()`` runs.

    Edge cases:
        - Connection settings are read from ``VARCO_MEMCACHED_HOST``,
          ``VARCO_MEMCACHED_PORT``, and related env vars via
          ``MemcachedCacheSettings``.  Override by providing a custom
          ``MemcachedCacheSettings`` binding before calling this function.
        - ``setup_cache=False`` is equivalent to :func:`bootstrap` — no
          connection pool is opened, and ``CacheBackend`` is not resolvable
          from the container until a later ``async_bootstrap(setup_cache=True)``
          or manual ``await container.ainstall(MemcachedCacheConfiguration)``.
        - ``setup_cache=False`` with providify absent still returns ``None``
          — the ``container is None`` guard runs *before* the
          ``setup_cache`` branch, so no ``AttributeError`` is possible
          either way.
        - ``await container.ashutdown()`` must be called at process exit to
          close the aiomcache connection pool via
          ``MemcachedCacheConfiguration``'s ``@Disposes(CacheBackend)``
          method (only relevant when ``setup_cache=True`` was used at some
          point). Not ``@PreDestroy`` — providify never invokes
          ``@PreDestroy`` on a ``@Provider``-produced instance; ``@Disposes``
          is the supported teardown path for provider output (Plan 024 / C2).

    Cross-reference — why the *defaults* differ from ``varco_redis``:
        ``varco_redis.di.async_bootstrap()`` defaults ``setup_cache=False``
        because it also serves a streams/event-bus bootstrap path where no
        cache is wanted at all — defaulting to install one there would
        silently open a Redis connection pool for callers who only use
        Redis as an event bus. ``varco_memcached`` has no such second use
        case: a Memcached connection is its *only* reason to exist, so
        ``setup_cache=True`` here reproduces this function's
        pre-existing, unconditional-install behaviour exactly — no
        existing ``await async_bootstrap(container)`` call site changes
        behaviour.

    Thread safety:  ✅ Intended for single-threaded startup only.
    Async safety:   ✅ ``async def`` — safe to ``await``.
    """
    container = bootstrap(container)
    if container is None:
        # providify is absent — bootstrap() returned None; nothing to install
        _logger.debug("providify not installed; varco_memcached DI helpers are no-ops")
        return None

    if setup_cache:
        # ainstall runs MemcachedCacheConfiguration.setup(), which constructs
        # MemcachedCache, calls cache.start() to open the aiomcache pool, and
        # binds the instance as CacheBackend in the container.
        # This is intentionally separate from bootstrap() — the async step cannot
        # run in a sync context, and some apps need the sync scan before the event
        # loop is started (e.g. framework introspection at import time).
        from varco_memcached.cache import MemcachedCacheConfiguration  # noqa: PLC0415

        await container.ainstall(MemcachedCacheConfiguration)

    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "bootstrap",
    "async_bootstrap",
]
