"""
Plan 022 / Phase 4 (RL-8a), Step 25 — the orphan is *actually* torn down.

Steps 22–23 proved with unit tests that ``VarcoLifespan`` awaits its new
``shutdown=`` hook.  That is not the claim RL-8a makes.  The claim is that a
``@PreDestroy``-bearing singleton which is **not** a registered lifecycle
component — and which therefore leaked before this plan — now really releases
its resources.  Asserting "a hook was called" would restate Step 22; this file
asserts the *effect*, against a real Redis container.

``RedisCache`` is the worst confirmed orphan of the six measured in
``design/api-freeze-and-standards/measurements/predestroy-vs-lifespan.md``:
``RedisCacheConfiguration.redis_cache()`` (``varco_redis/cache.py:573-597``)
**awaits ``cache.start()``** inside the provider, so
``async_bootstrap(setup_cache=True)`` leaves a *started* connection pool bound
as ``CacheBackend`` that no lifespan path ever closed.

✅ **Fixed (Plan 024 / C2)** — resolved by adopting ``@Disposes``, not by an
upstream change. providify 2.0.1 (2026-09-01) settles the underlying dispatch
as **intentional** (Jakarta CDI producer-method rule): a ``ProviderBinding``
runs its ``@Disposes`` disposer and returns — the ``@PreDestroy`` on the
*instance a provider returned* is never reached, by design, and providify
2.0.1 documents this explicitly (`providify/README.md:945-949`) rather than
changing it. ``RedisCacheConfiguration`` now carries its own
``@Disposes(CacheBackend)`` method (``close_cache``, `varco_redis/cache.py`)
that calls ``RedisCache.stop()`` on ``ashutdown()`` — the assertions below
are now real, passing proof that the pool is actually closed. Same fix
shipped for ``MemcachedCache``
(`varco_memcached/tests/test_memcached_cache_disposes.py`).

See BACKLOG.md's C2 row and `plans/024-3-0-1-cleanup.md` §D-C2 for the full
resolution. The *positive* proof for a ``@Singleton``-bound (``ClassBinding``)
orphan — a structurally different shape — lives in
``varco_nats/tests/test_nats_lifespan_shutdown_integration.py``, against
``NatsStreamManager``, holding a real connection.

⚠️ Cross-package direction.  ``varco_redis`` does not (and must not) depend on
``varco_fastapi``; the import below is guarded by ``importorskip`` and the test
lives here only because the session-scoped ``redis_url`` fixture and the real
container do (CLAUDE.md's Test Conventions).  Nothing in ``varco_redis``'s
shipped code is involved in that import.

Per-test namespacing: the shared session container means this test confines
itself to a ``uuid4().hex[:8]`` key prefix it owns exclusively.

Thread safety:  N/A (integration test)
Async safety:   ✅ the lifespan is driven as an async context manager.
"""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

pytestmark = pytest.mark.integration


async def test_container_shutdown_closes_orphaned_redis_cache_pool(
    redis_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    A started-but-unregistered ``RedisCache`` has its socket closed at shutdown.

    Edge cases covered here on purpose:
        - The cache is an *orphan* by construction: the ``VarcoLifespan``
          built below is given no positional components at all (only the
          ``shutdown=`` hook), so the test cannot silently degrade into "a
          registered component was stopped". (Plan 049 / DI-1 deleted
          ``_collect_lifecycle_components`` — this precondition used to be
          asserted by checking the cache was absent from its return value;
          that helper's replacement, ``CoreModule.start()``, only manages
          ``AbstractEventBus``/``AbstractJobRunner``, never ``CacheBackend``,
          so the same "never a managed lifecycle component" invariant holds
          without needing an equivalent check here.)
        - The assertion is on the live ``redis.asyncio`` connection object, not
          on ``RedisCache._redis`` alone: a hook that nulled the attribute but
          leaked the socket would still pass a weaker check.
    """
    providify = pytest.importorskip("providify")
    pytest.importorskip("varco_fastapi")

    from varco_core.cache.base import CacheBackend
    from varco_fastapi.lifespan import VarcoLifespan
    from varco_redis.di import async_bootstrap

    run_id = uuid4().hex[:8]
    monkeypatch.setenv("VARCO_REDIS_CACHE_URL", redis_url)
    monkeypatch.setenv("VARCO_REDIS_CACHE_KEY_PREFIX", f"test:{run_id}:")

    container = providify.DIContainer()
    await async_bootstrap(container)

    cache: Any = await container.aget(CacheBackend)
    # Force a real round-trip so the pool holds a genuinely connected socket.
    await cache.set(f"{run_id}:probe", "v", ttl=30)
    assert await cache.get(f"{run_id}:probe") == "v"

    client = cache._redis
    assert client is not None
    pool = client.connection_pool
    connections = list(pool._available_connections)
    assert connections, "expected the pool to hold a released connection after a round-trip"
    assert any(conn.is_connected for conn in connections)

    # Pre-condition: this cache is exactly the orphan RL-8a is about — see
    # the docstring's Edge cases note above.
    async def _shutdown() -> None:
        await container.ashutdown()

    async with VarcoLifespan(shutdown=_shutdown)(object()):
        pass

    assert cache._redis is None
    assert not any(conn.is_connected for conn in connections)
