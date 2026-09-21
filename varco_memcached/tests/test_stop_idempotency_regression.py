"""
Plan 022 / Phase 4 (RL-8a), Step 21 — repurposed as regression tests.

Step 6's measurement (``design/api-freeze-and-standards/measurements/
predestroy-vs-lifespan.md`` Part 2) found all ten ``stop()`` implementations
idempotent by reading them, and asked for that read to be replaced by a test
before §D-8a2(b) makes the property load-bearing — under the new ``shutdown=``
hook, a component reachable by both ``_stop_all()`` and ``container.ashutdown()``
has ``stop()`` called twice.

``MemcachedCache`` is table row #9 and the worst-affected orphan alongside
``RedisCache``: ``varco_memcached.di.async_bootstrap()`` always installs the
cache configuration (Plan 046 removed the ``setup_cache=`` kwarg — it is
unconditional now), so the default path leaves an eagerly-started client
that nothing tears down today.  No server is needed here — ``aiomcache``
connects lazily, so start→stop→stop completes offline.

NOTE: may legitimately pass on arrival — a regression guard, not a red test.

Thread safety:  N/A (unit test)
Async safety:   ✅ every stop() is awaited.
"""

from __future__ import annotations

from varco_memcached.cache import MemcachedCache


async def test_stop_twice_on_never_started_cache_is_a_noop() -> None:
    # A container sweep can reach a singleton the lifespan never started.
    cache = MemcachedCache()

    await cache.stop()
    await cache.stop()  # must not raise


async def test_stop_twice_after_start_is_a_noop() -> None:
    # The §D-8a2(b) double-stop path itself: _stop_all() then container.ashutdown().
    cache = MemcachedCache()
    await cache.start()

    await cache.stop()
    await cache.stop()  # must not raise


async def test_second_stop_does_not_reclose_the_client() -> None:
    """
    ``if self._client is None: return`` (cache.py:256) + ``self._client = None``
    (:262) is the line that makes #9 idempotent.  Assert the sentinel directly:
    a refactor that closes without clearing would double-close under the new
    double-stop model.
    """
    cache = MemcachedCache()
    await cache.start()

    await cache.stop()

    assert cache._client is None  # noqa: SLF001 — the sentinel IS the contract

    await cache.stop()

    assert cache._client is None  # noqa: SLF001
