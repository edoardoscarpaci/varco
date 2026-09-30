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

Lifecycle (Plan 050 / §D-disposes rows 13-15): none of these providers call
``await store.start()``/hold a background task; ``RedisJobStore`` needs no
start, ``RedisIdempotencyStore``/``RedisTokenRevocationStore`` connect
lazily/eagerly on their own construction contract. Each provider's client is
now reachable at shutdown: ``idempotency_store``/``revocation_store``
close through the store's own ``aclose()``; ``job_store`` keeps the
``aioredis`` client it creates on ``self`` (the provider instance, cached
by providify) so ``close_job_store`` can close it without changing
``RedisJobStore``'s constructor signature.
"""

from __future__ import annotations

from typing import Any

from providify import Configuration, Disposes, Fallback, Inject, Provider
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

    @Disposes(AbstractIdempotencyStore)
    async def close_idempotency_store(self, store: RedisIdempotencyStore) -> None:
        """
        Close ``idempotency_store``'s client on ``container.ashutdown()``
        (Plan 050 / §D-disposes row 13) — before this, a client created by
        the provider above leaked at shutdown.

        Args:
            store: The ``RedisIdempotencyStore`` this configuration
                produced. Typed concretely, not ``AbstractIdempotencyStore``
                — ``aclose()`` is not part of the ABC (same dispatch-key
                note as ``varco_redis/dlq.py``'s ``close_dlq``).
        """
        await store.aclose()

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

        Edge cases:
            The created client is kept on ``self._job_store_client`` (the
            provider *instance* is cached as a providify singleton) so
            ``close_job_store`` can close it without ``RedisJobStore``
            itself growing a close method (Plan 050 / §D-disposes row 14).
        """
        import redis.asyncio as aioredis  # noqa: PLC0415 — import-budget rule

        client = aioredis.from_url(settings.url)
        self._job_store_client: Any = client
        return RedisJobStore(client)

    @Disposes(AbstractJobStore)
    async def close_job_store(self, store: AbstractJobStore) -> None:
        """
        Close the ``aioredis`` client ``job_store`` created, held on
        ``self._job_store_client`` (Plan 050 / §D-disposes row 14).

        Args:
            store: Unused — the dispatch key is ``AbstractJobStore``; the
                client to close lives on this configuration instance, not
                on the store (``RedisJobStore`` never owned it directly).
        """
        client = getattr(self, "_job_store_client", None)
        if client is not None:
            await client.aclose()
            self._job_store_client = None

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

    @Disposes(AbstractTokenRevocationStore)
    async def close_revocation_store(self, store: RedisTokenRevocationStore) -> None:
        """
        Close ``revocation_store``'s client on ``container.ashutdown()``
        (Plan 050 / §D-disposes row 15) — before this, the eagerly-created
        client leaked at shutdown.

        Args:
            store: The ``RedisTokenRevocationStore`` this configuration
                produced.
        """
        await store.aclose()
