"""
Fast, no-Docker conformance run for the in-process ``varco_core``
implementations (Plan 012 / RT6, Step 26).

Deliberately unmarked — no ``@pytest.mark.integration`` — this is the fast
feedback loop for the shared conformance suites and is also where the
indirect-parametrization recipe from the CI-patterns brief is used
literally, since one process holds every in-memory implementation.

⚠️ Depends on ``pythonpath = ["../testkit"]`` being added to
``varco_core/pyproject.toml``'s ``[tool.pytest.ini_options]`` (Step 26) —
until that line exists, every import below fails with
``ModuleNotFoundError: No module named 'varco_conformance'``. That failure
is the RED state this file is meant to produce.

⚠️ ``InMemoryJobStore`` lives in ``varco_fastapi.job.store``, not
``varco_core`` (verified against the tree — the plan text is imprecise
here). Importing it from ``varco_core/tests/`` requires adding
``varco-fastapi`` to ``varco_core``'s dev dependency group — an inversion
of the normal dependency direction (every OTHER package depends on
``varco_core``, never the reverse), justified the same way Step 35
justifies a dev-group-only edge (`varco_fastapi` -> `varco_sa`): a
dev-group entry is not a package dependency and never reaches the wheel.
This must be called out explicitly to the implementer, since it is easy to
mistake for a layer-rule violation.

⚠️ ``NoopEventBus`` (``varco_core.event.memory.py:639``) does **not** get its
own conformance test class here, deliberately (Plan 024 / C7 coverage audit —
full matrix in ``testkit/varco_conformance/COVERAGE.md``). It is a Null
Object: ``publish()`` discards and ``subscribe()`` returns a
**pre-cancelled** ``Subscription`` (``memory.py:665-691``), so it
intentionally violates ``EventBusConformance``'s deliver-what-you-publish
contract by design — subclassing the suite for it would mean xfail-ing most
of it, which teaches nothing and rots. See ``COVERAGE.md``'s "Stated
absences" section for the full reasoning and for every other package's
absence (`varco_ws`, `varco_memcached`, `varco_casbin`).
"""

from __future__ import annotations

import pytest
from varco_conformance.cache import CacheBackendConformance
from varco_conformance.dlq import DeadLetterQueueConformance
from varco_conformance.event_bus import EventBusConformance
from varco_conformance.job_store import JobStoreConformance
from varco_conformance.token_revocation import TokenRevocationStoreConformance
from varco_core.cache.invalidation import TTLStrategy
from varco_core.cache.memory import InMemoryCache, NoOpCache
from varco_core.event.dlq import InMemoryDeadLetterQueue
from varco_core.event.memory import InMemoryEventBus

# ── Event bus ────────────────────────────────────────────────────────────────


class TestInMemoryEventBusConformance(EventBusConformance):
    # InMemoryEventBus has no start()/stop()/__aenter__/__aexit__ lifecycle
    # at all (verified: varco_core/varco_core/event/memory.py has none of
    # these methods) — unlike KafkaEventBus/RedisEventBus, which do. The
    # base suite's lifecycle tests self-skip via hasattr() guards, but this
    # flag documents the reason explicitly for this backend.
    supports_lifecycle = False

    @pytest.fixture
    async def bus(self) -> InMemoryEventBus:
        return InMemoryEventBus()


# ── Cache ────────────────────────────────────────────────────────────────────


class TestInMemoryCacheConformance(CacheBackendConformance):
    @pytest.fixture
    async def cache(self) -> InMemoryCache:
        # A bare InMemoryCache() with no InvalidationStrategy never actively
        # expires entries (documented: "None = entries never expire unless
        # explicitly deleted or the cache is cleared") — wire TTLStrategy
        # so the shared suite's test_ttl_expiry exercises real behaviour.
        async with InMemoryCache(strategy=TTLStrategy()) as cache:
            yield cache


class TestNoOpCacheConformance(CacheBackendConformance):
    """
    ``NoOpCache`` legitimately cannot satisfy set->get — every write is
    discarded by design. Override the storage-semantics tests with the
    no-op expectation instead of weakening the shared suite (Step 26 /
    the plan's edge-case table).
    """

    @pytest.fixture
    async def cache(self) -> NoOpCache:
        async with NoOpCache() as cache:
            yield cache

    async def test_set_get_round_trip(self, cache: NoOpCache) -> None:
        key = self._key("roundtrip")
        await cache.set(key, "value-1")
        # NoOpCache: every write is discarded — a "round trip" is a miss.
        assert await cache.get(key) is None

    async def test_ttl_expiry(self, cache: NoOpCache) -> None:
        key = self._key("ttl")
        await cache.set(key, "expires-soon", ttl=0.05)
        # Never stored in the first place.
        assert await cache.get(key) is None

    async def test_overwrite(self, cache: NoOpCache) -> None:
        key = self._key("overwrite")
        await cache.set(key, "first")
        await cache.set(key, "second")
        assert await cache.get(key) is None

    async def test_exists(self, cache: NoOpCache) -> None:
        key = self._key("exists")
        await cache.set(key, "value")
        # NoOpCache.exists() is documented to always return False.
        assert await cache.exists(key) is False

    async def test_get_many_partial_hit(self, cache: NoOpCache) -> None:
        prefix = self._key("many")
        key_hit = f"{prefix}:hit"
        key_miss = f"{prefix}:miss"
        await cache.set(key_hit, "value")

        result = await cache.get_many([key_hit, key_miss])

        # Nothing was ever stored — every key misses.
        assert result == {}

    async def test_set_many_round_trip(self, cache: NoOpCache) -> None:
        prefix = self._key("setmany")
        items = {f"{prefix}:a": "1", f"{prefix}:b": "2"}
        await cache.set_many(items)

        assert await cache.get(f"{prefix}:a") is None
        assert await cache.get(f"{prefix}:b") is None


# ── Job store ────────────────────────────────────────────────────────────────


class TestInMemoryJobStoreConformance(JobStoreConformance):
    @pytest.fixture
    async def store(self):
        # See module docstring — this import crosses the normal dependency
        # direction (varco_core/tests importing from varco_fastapi) and
        # requires a dev-group-only addition to varco_core/pyproject.toml.
        from varco_fastapi.job.store import InMemoryJobStore

        return InMemoryJobStore()


# ── Dead letter queue ────────────────────────────────────────────────────────


class TestInMemoryDeadLetterQueueConformance(DeadLetterQueueConformance):
    @pytest.fixture
    async def dlq(self) -> InMemoryDeadLetterQueue:
        return InMemoryDeadLetterQueue()


# ── Token revocation store ───────────────────────────────────────────────────


class TestInMemoryTokenRevocationStoreConformance(TokenRevocationStoreConformance):
    @pytest.fixture
    async def store(self):
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        return InMemoryTokenRevocationStore()
