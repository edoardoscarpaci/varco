"""
varco_redis.di
==============
Providify DI integration for ``varco_redis``.

All singletons (``RedisEventBus``, ``RedisStreamEventBus`` via selector,
``RedisChannelManager``, ``RedisHealthCheck``, ``RedisCache``) carry
``@Singleton`` on the class itself and are discovered automatically by
``container.scan("varco_redis", recursive=True)``.

``RedisEventBusSettings``/``RedisCacheSettings``/``LayeredCacheSettings``/
``RedisBackplaneSettings`` are the exception: pydantic ``BaseSettings``
declares ``__init__(self, **values)``, which is not a shape ``@Singleton``
should be applied to (see CLAUDE.md's pitfall table). All four are
registered by ``bootstrap()`` via ``bind_settings()`` (Plan 048 / DI-6,
Step 8, Decision 2) — never a module-level or per-``@Configuration``
``@Fallback @Provider`` factory any more.

No ``@Configuration`` class or ``ainstall()`` call is required for the
event bus. ``RedisCacheConfiguration``/``RedisLayeredCacheConfiguration``
still need an explicit ``await container.ainstall(...)`` to *start* the
cache, but — as of Decision 2 — ``bootstrap(container)`` must run first so
their settings are resolvable at all.

Bus type selection
------------------
The bus is live only when ``VARCO_EVENT_BUS_BACKEND=redis`` (Plan 047
§D-vocab) — bootstrapping this package alone no longer makes it the live
bus. Once selected, set ``VARCO_REDIS_USE_STREAMS=true`` to activate
``RedisStreamEventBus`` (at-least-once, Redis Streams) *within* that
selection; the default is ``RedisEventBus`` (at-most-once, Pub/Sub). The
selector is ``RedisEventBusSelectorConfiguration.bus()`` (a gated
``@Configuration`` provider), discovered automatically by scan.

Usage
-----
Pub/Sub (default)::

    from providify import DIContainer
    from varco_redis.di import bootstrap

    container = DIContainer()
    bootstrap(container)
    bus = await container.aget(AbstractEventBus)
    await container.ashutdown()

Or manually::

    container = DIContainer()
    container.scan("varco_redis", recursive=True)
    bus = await container.aget(AbstractEventBus)

Redis Streams (at-least-once) — either set the env var, or register an
explicit non-fallback settings instance before ``bootstrap()`` (Plan 046
§D-streams; ``bootstrap(streams=True)`` is removed — it used to mutate
``os.environ`` for the whole process, which is gone)::

    # Set in environment before starting the process:
    # VARCO_REDIS_USE_STREAMS=true
    container.scan("varco_redis", recursive=True)

    # Or, code-side (Plan 048's recipe; the 046-era bind_instance form is equivalent):
    from varco_core.settings import bind_settings
    from varco_redis.config import RedisEventBusSettings

    bind_settings(container, RedisEventBusSettings, instance=RedisEventBusSettings(use_streams=True))
    bootstrap(container)

Overriding the default settings::

    # ⚠️ @Provider-decorated module-level function: provide() rejects bare
    #    lambdas and takes no second "interface" argument (the return
    #    annotation is the interface).
    @Provider(singleton=True)
    def redis_settings() -> RedisEventBusSettings:
        return RedisEventBusSettings(url=os.environ["REDIS_URL"])

    container = DIContainer()
    container.provide(redis_settings)          # any order — the default is @Fallback
    container.scan("varco_redis", recursive=True)
"""

from __future__ import annotations

from typing import Any

from providify import DIContainer
from varco_core.revocation.base import AbstractTokenRevocationStore

# ── bootstrap ─────────────────────────────────────────────────────────────────


def bootstrap(container: DIContainer) -> DIContainer:
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

        from providify import DIContainer
        from varco_redis.di import bootstrap

        container = DIContainer()
        bootstrap(container)
        bus = await container.aget(AbstractEventBus)
        await container.ashutdown()

    To use Redis Streams, either set ``VARCO_REDIS_USE_STREAMS=true`` in the
    environment, or register an explicit settings instance before calling
    this function (Plan 046 §D-streams — ``bootstrap(streams=True)`` is
    removed; it used to mutate ``os.environ`` for the whole process)::

        from varco_core.settings import bind_settings
        from varco_redis.config import RedisEventBusSettings

        bind_settings(
            container, RedisEventBusSettings, instance=RedisEventBusSettings(use_streams=True)
        )
        bootstrap(container)

    Override settings before calling bootstrap::

        from varco_redis.config import RedisEventBusSettings
        from providify import DIContainer

        @Provider(singleton=True)
        def redis_settings() -> RedisEventBusSettings:
            return RedisEventBusSettings(url=os.environ["REDIS_URL"])

        container = DIContainer()
        container.provide(redis_settings)      # any order — the default is @Fallback
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
          the bus ``stop()`` via ``RedisEventBusSelectorConfiguration``'s
          ``@Disposes(AbstractEventBus)`` method — not ``@PreDestroy``, which
          providify never invokes on a ``@Provider``-produced instance
          (Plan 024 / C2).

    Thread safety:  ✅ Bootstrap is intended for single-threaded startup only.
    Async safety:   ✅ Scanning is synchronous.
    """
    # Plan 045 / E2: scan varco_core.revocation FIRST so
    # NullTokenRevocationStore (the real @Fallback @Singleton, defined in
    # varco_core.revocation.null) is registered as the AbstractTokenRevocationStore
    # default — replaces the deleted varco_redis-local
    # RedisScanNullTokenRevocationStoreDefault subclass, which existed only
    # because providify's scanner skips re-exported symbols (only a class's
    # OWN defining module registers it). An app that scans varco_redis
    # WITHOUT calling bootstrap() must scan varco_core.revocation itself.
    container.scan("varco_core.revocation", recursive=True)

    # Plan 048 / DI-6, Step 8 — every varco_redis settings class goes
    # through bind_settings() (fail-fast, one recipe) BEFORE scan(), rather
    # than a module-level/per-@Configuration @Fallback @Provider. Safe to
    # call before an app's own code override too — bind_settings() never
    # reads the environment when a non-fallback binding already exists
    # (§D-precedence).
    #
    # ⚠️ BREAKING (Decision 2): RedisCacheSettings/LayeredCacheSettings/
    # RedisBackplaneSettings used to be registered lazily, only when their
    # owning @Configuration was installed (RedisCacheConfiguration /
    # RedisLayeredCacheConfiguration) or scanned (backplane.py's
    # module-level provider). They are now registered here, unconditionally
    # — `container.ainstall(RedisCacheConfiguration)` /
    # `ainstall(RedisLayeredCacheConfiguration)` / the backplane no longer
    # resolve their settings without a prior `bootstrap(container)` call.
    from varco_core.settings import bind_settings

    from varco_redis.backplane import RedisBackplaneSettings
    from varco_redis.cache import LayeredCacheSettings, RedisCacheSettings
    from varco_redis.config import RedisEventBusSettings

    for settings_cls in (
        RedisEventBusSettings,
        RedisCacheSettings,
        LayeredCacheSettings,
        RedisBackplaneSettings,
    ):
        bind_settings(container, settings_cls)

    # Discover all @Singleton/@Component classes and module-level @Provider
    # functions in varco_redis recursively.  This includes:
    #   - RedisEventBusSettings (@Singleton, reads VARCO_REDIS_* env vars)
    #   - RedisEventBusSelectorConfiguration.bus (@Provider, gated on
    #     VARCO_EVENT_BUS_BACKEND=redis; selects Pub/Sub or Streams)
    #   - RedisHealthCheck, RedisChannelManager (@Singleton)
    #   - RedisCache and related classes (@Singleton)
    container.scan("varco_redis", recursive=True)

    return container


# ── async_bootstrap ───────────────────────────────────────────────────────────


async def async_bootstrap(container: DIContainer) -> DIContainer:
    """
    Async version of :func:`bootstrap` that also starts the Redis cache.

    Combines the synchronous scan step with an async cache installation so
    the app's own bootstrap function doesn't need a separate
    ``await container.ainstall(RedisCacheConfiguration)`` call::

        async def _bootstrap() -> None:
            await async_bootstrap(container)
            # ↑ one call instead of two

        # Equivalent to:
        async def _bootstrap() -> None:
            bootstrap(container)
            await container.ainstall(RedisCacheConfiguration)

    Plan 046 §D-contract removes the ``setup_cache=`` kwarg — the two
    packages' differing defaults (``varco_redis`` was ``False``,
    ``varco_memcached`` was ``True``) were exactly the inconsistency DI-2
    names. Call plain :func:`bootstrap` for a scan-only container.

    Args:
        container: The ``DIContainer`` to scan into. Required — see
            :func:`bootstrap`.

    Returns:
        The same ``container`` passed in, after scanning and cache
        installation.

    Raises:
        ConnectionError: If Redis is unreachable.

    Thread safety:  ✅ Intended for single-threaded startup only.
    Async safety:   ✅ ``async def`` — safe to ``await``.
    """
    bootstrap(container)

    from varco_redis.cache import RedisCacheConfiguration  # noqa: PLC0415

    await container.ainstall(RedisCacheConfiguration)

    return container


# ── Token revocation (Plan 034 / S13b, Step 34) ──────────────────────────────


def enable_redis_token_revocation(container: DIContainer, **redis_kwargs: Any) -> DIContainer:
    """
    Opt in to ``RedisTokenRevocationStore`` as the application's
    ``AbstractTokenRevocationStore``, shadowing the always-off
    ``varco_core.revocation.null.NullTokenRevocationStore`` default (a
    ``@Fallback`` binding — this registers a plain, non-fallback provider,
    which wins regardless of registration order). ``bootstrap()`` scans
    ``varco_core.revocation`` itself so this default is present even when
    an app only scans ``varco_redis`` (Plan 045 / E2).

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

    Raises:
        AlreadyBoundError: A non-fallback ``AbstractTokenRevocationStore``
            binding already exists (yours, or a previous call to this or
            ``varco_core.revocation.di.enable_token_revocation`` — both flip
            the same seam, §D-enable).

    Example::

        container = DIContainer()
        container.scan("varco_redis", recursive=True)
        enable_redis_token_revocation(container, url="redis://localhost:6379/0")

        store = await container.aget(AbstractTokenRevocationStore)
        registry = TrustedIssuerRegistry(revocation_store=store)  # the second step
    """
    from varco_core.di import AlreadyBoundError, bind_instance, is_user_bound  # noqa: PLC0415

    from varco_redis.revocation import RedisTokenRevocationStore  # noqa: PLC0415

    if is_user_bound(container, AbstractTokenRevocationStore):
        raise AlreadyBoundError(
            "A non-fallback AbstractTokenRevocationStore binding already exists — "
            "enable_redis_token_revocation()/enable_token_revocation() flip the same "
            "seam and only one may win."
        )

    # Plan 050 / §D-disposes row 16, P-DISP shim: bind_instance(disposer=)
    # closes the client at container.ashutdown() — before this, the client
    # this function constructs leaked at every shutdown. providify's
    # _adispose calls `binding.disposer(instance)` (container.py:5301-5303)
    # — store.aclose takes no arguments, so a thin async wrapper absorbs
    # the positional instance argument.
    store = RedisTokenRevocationStore(**redis_kwargs)

    async def _dispose_store(_instance: object) -> None:
        await store.aclose()

    bind_instance(container, AbstractTokenRevocationStore, store, disposer=_dispose_store)
    return container


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "bootstrap",
    "async_bootstrap",
    "enable_redis_token_revocation",
]
