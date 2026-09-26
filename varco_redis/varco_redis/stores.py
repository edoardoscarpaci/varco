"""
varco_redis.stores
=====================
``RedisStoresConfiguration`` — Plan 047 §D-new-providers N6/N7/N8: the
three gated Redis-backed seam bindings that previously had no DI binding
at all (``RedisIdempotencyStore``, ``RedisJobStore``,
``RedisTokenRevocationStore``). ``varco_redis`` has no pre-existing
``@Configuration`` for these, so this is a new, scan-discovered class.

Each provider shares ``RedisEventBusSettings.url`` (``VARCO_REDIS_URL``) —
the package's canonical connection URL — rather than a per-store URL, so
one variable configures "where Redis is" for every store this package
serves (048 §D-connection keeps ``RedisConnectionSettings`` un-bound; a
caller needing a different URL per store uses the code tier —
``bind_instance()``).

⚠️ **Lifecycle is Plan 050's** — none of these providers call
``await store.start()``/hold a background task; ``RedisJobStore`` needs no
start, ``RedisIdempotencyStore``/``RedisTokenRevocationStore`` connect
lazily on first use (matching their own hand-construction contract
today). No ``@Disposes`` is registered here — 050 wires it.
"""

from __future__ import annotations

from providify import Configuration, Fallback, Inject, Provider
from varco_core.di import BACKEND_PRIORITY
from varco_core.di.select import selected_by
from varco_core.idempotency.base import AbstractIdempotencyStore
from varco_core.job.base import AbstractJobStore
from varco_core.revocation.base import AbstractTokenRevocationStore

from varco_redis.config import RedisEventBusSettings
from varco_redis.idempotency import RedisIdempotencyStore
from varco_redis.job_store import RedisJobStore
from varco_redis.revocation import RedisTokenRevocationStore

__all__ = ["RedisStoresConfiguration"]


@Configuration
class RedisStoresConfiguration:
    """
    Gated Redis-backed bindings for the ``idempotency``, ``job_store``, and
    ``revocation`` seams (Plan 047 §D-new-providers N6/N7/N8).

    Discovered and auto-installed by
    ``container.scan("varco_redis", recursive=True)`` — each provider is
    inert (``@Requires``-gated via ``selected_by()``) unless the matching
    ``VARCO_<SEAM>_BACKEND=redis`` variable is set.

    Thread safety:  ✅ Installed once at startup.
    Async safety:   ✅ No I/O at construction time — each store connects
                       lazily on first use (unchanged from hand
                       construction).
    """

    @Fallback
    @selected_by("idempotency", "redis")
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    def idempotency_store(
        self, settings: Inject[RedisEventBusSettings]
    ) -> AbstractIdempotencyStore:
        """
        N6 — live only when ``VARCO_IDEMPOTENCY_BACKEND=redis``.

        Args:
            settings: The scanned ``RedisEventBusSettings`` singleton —
                its ``.url`` is this store's connection URL.

        Returns:
            A ``RedisIdempotencyStore`` sharing ``settings.url``.
        """
        return RedisIdempotencyStore(url=settings.url)

    @Fallback
    @selected_by("job_store", "redis")
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    def job_store(self, settings: Inject[RedisEventBusSettings]) -> AbstractJobStore:
        """
        N7 — live only when ``VARCO_JOB_STORE_BACKEND=redis``.

        Args:
            settings: The scanned ``RedisEventBusSettings`` singleton —
                its ``.url`` is this store's connection URL.

        Returns:
            A ``RedisJobStore`` over a fresh ``aioredis.Redis`` client
            connected to ``settings.url``.
        """
        import redis.asyncio as aioredis  # noqa: PLC0415 — import-budget rule

        return RedisJobStore(aioredis.from_url(settings.url))

    @Fallback
    @selected_by("revocation", "redis")
    @Provider(singleton=True, priority=BACKEND_PRIORITY)
    def revocation_store(
        self, settings: Inject[RedisEventBusSettings]
    ) -> AbstractTokenRevocationStore:
        """
        N8 — the env twin of ``enable_redis_token_revocation()``, live
        only when ``VARCO_REVOCATION_BACKEND=redis``.

        ⚠️ Same two-step caveat as ``enable_redis_token_revocation()`` —
        binding this store does not, by itself, wire it into
        ``TrustedIssuerRegistry(revocation_store=...)``.

        Args:
            settings: The scanned ``RedisEventBusSettings`` singleton —
                its ``.url`` is this store's connection URL.

        Returns:
            A ``RedisTokenRevocationStore`` sharing ``settings.url``.
        """
        return RedisTokenRevocationStore(url=settings.url)
