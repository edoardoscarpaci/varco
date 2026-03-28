"""
tests.test_cache_warming
=========================
Unit tests for varco_core.cache.warming.

Covers:
    CacheWarmer         — ABC, single warm() method
    QueryCacheWarmer    — runs query_fn, stores (key, value) pairs
    SnapshotCacheWarmer — runs snapshot_fn, stores {key: value} dict
    CompositeWarmer     — runs multiple warmers sequentially
    CacheBackend        — add_warmer() and _run_warmers() integration

All tests use InMemoryCache — no external dependencies required.
"""

from __future__ import annotations

import pytest

from varco_core.cache.warming import (
    CacheWarmer,
    CompositeWarmer,
    QueryCacheWarmer,
    SnapshotCacheWarmer,
)


# ── Minimal in-process cache stub ─────────────────────────────────────────────


class StubCache:
    """
    Minimal in-process cache stub for testing warmers in isolation.
    Records all set() calls for assertion.
    """

    def __init__(self) -> None:
        self.store: dict[str, object] = {}
        self.ttls: dict[str, float | None] = {}

    async def set(self, key: str, value: object, *, ttl: float | None = None) -> None:
        self.store[key] = value
        self.ttls[key] = ttl

    async def get(self, key: str, *, type_hint: type | None = None) -> object | None:
        return self.store.get(key)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass


# ── CacheWarmer (ABC) ─────────────────────────────────────────────────────────


def test_cache_warmer_is_abstract() -> None:
    """CacheWarmer cannot be instantiated directly — it is an ABC."""
    with pytest.raises(TypeError):
        CacheWarmer()  # type: ignore[abstract]


# ── QueryCacheWarmer ───────────────────────────────────────────────────────────


async def test_query_cache_warmer_populates_cache() -> None:
    """
    QueryCacheWarmer must call query_fn and store each (key, value) pair
    in the cache.
    """

    async def my_query() -> list[tuple[str, str]]:
        return [("user:1", "alice"), ("user:2", "bob")]

    warmer = QueryCacheWarmer(my_query)
    cache = StubCache()

    await warmer.warm(cache)

    assert cache.store == {"user:1": "alice", "user:2": "bob"}


async def test_query_cache_warmer_passes_ttl() -> None:
    """
    TTL passed to QueryCacheWarmer must be forwarded to every cache.set() call.
    """

    async def my_query() -> list[tuple[str, str]]:
        return [("k:1", "v")]

    warmer = QueryCacheWarmer(my_query, ttl=300.0)
    cache = StubCache()
    await warmer.warm(cache)

    assert cache.ttls == {"k:1": 300.0}


async def test_query_cache_warmer_empty_result() -> None:
    """Empty query result must not raise — the cache just remains empty."""

    async def empty() -> list[tuple[str, str]]:
        return []

    warmer = QueryCacheWarmer(empty)
    cache = StubCache()
    await warmer.warm(cache)

    assert cache.store == {}


def test_query_cache_warmer_repr() -> None:
    """repr should include the query function name for debuggability."""

    async def load_top_products():
        return []

    warmer = QueryCacheWarmer(load_top_products)
    assert "load_top_products" in repr(warmer)


# ── SnapshotCacheWarmer ───────────────────────────────────────────────────────


async def test_snapshot_cache_warmer_populates_cache() -> None:
    """
    SnapshotCacheWarmer must call snapshot_fn and store every {key: value} entry.
    """

    async def config_snapshot() -> dict[str, str]:
        return {"feature:dark_mode": "enabled", "feature:beta": "disabled"}

    warmer = SnapshotCacheWarmer(config_snapshot)
    cache = StubCache()
    await warmer.warm(cache)

    assert cache.store == {
        "feature:dark_mode": "enabled",
        "feature:beta": "disabled",
    }


async def test_snapshot_cache_warmer_passes_ttl() -> None:
    """TTL must be forwarded to each cache.set() call."""

    async def snap() -> dict[str, str]:
        return {"key": "value"}

    warmer = SnapshotCacheWarmer(snap, ttl=60.0)
    cache = StubCache()
    await warmer.warm(cache)

    assert cache.ttls == {"key": 60.0}


async def test_snapshot_cache_warmer_empty_snapshot() -> None:
    """Empty snapshot must not raise."""

    async def empty_snap() -> dict[str, str]:
        return {}

    warmer = SnapshotCacheWarmer(empty_snap)
    cache = StubCache()
    await warmer.warm(cache)

    assert cache.store == {}


def test_snapshot_cache_warmer_repr() -> None:
    """repr should include the snapshot function name."""

    async def my_snapshot():
        return {}

    warmer = SnapshotCacheWarmer(my_snapshot)
    assert "my_snapshot" in repr(warmer)


# ── CompositeWarmer ────────────────────────────────────────────────────────────


async def test_composite_warmer_runs_all_warmers() -> None:
    """
    CompositeWarmer must run every constituent warmer and merge their entries
    into the cache.
    """

    async def q1() -> list[tuple[str, str]]:
        return [("a", "1")]

    async def q2() -> list[tuple[str, str]]:
        return [("b", "2")]

    composite = CompositeWarmer(QueryCacheWarmer(q1), QueryCacheWarmer(q2))
    cache = StubCache()
    await composite.warm(cache)

    assert cache.store == {"a": "1", "b": "2"}


async def test_composite_warmer_runs_in_order() -> None:
    """
    Warmers must run in registration order — later warmers can overwrite
    earlier ones (last-write wins).
    """
    order: list[int] = []

    class SequencedWarmer(CacheWarmer):
        def __init__(self, n: int) -> None:
            self._n = n

        async def warm(self, cache: object) -> None:
            order.append(self._n)

    composite = CompositeWarmer(
        SequencedWarmer(1), SequencedWarmer(2), SequencedWarmer(3)
    )
    cache = StubCache()
    await composite.warm(cache)

    assert order == [1, 2, 3]


async def test_composite_warmer_stops_on_first_failure() -> None:
    """
    A failing warmer in CompositeWarmer must propagate immediately — subsequent
    warmers must NOT be called.
    """
    called: list[str] = []

    class FailingWarmer(CacheWarmer):
        async def warm(self, cache: object) -> None:
            raise RuntimeError("warmer failed")

    class SuccessWarmer(CacheWarmer):
        async def warm(self, cache: object) -> None:
            called.append("success")

    composite = CompositeWarmer(FailingWarmer(), SuccessWarmer())
    cache = StubCache()

    with pytest.raises(RuntimeError, match="warmer failed"):
        await composite.warm(cache)

    # SuccessWarmer must NOT have been called.
    assert called == []


async def test_composite_warmer_empty_list() -> None:
    """Empty CompositeWarmer must be a no-op."""
    composite = CompositeWarmer()
    cache = StubCache()
    await composite.warm(cache)
    assert cache.store == {}


# ── CacheBackend integration: add_warmer / _run_warmers ───────────────────────


async def test_cache_backend_add_warmer_runs_on_aenter() -> None:
    """
    add_warmer() must cause the warmer to run when the backend is used as an
    async context manager (inside __aenter__).
    """
    from varco_core.cache.memory import InMemoryCache

    warmed: list[str] = []

    class RecordingWarmer(CacheWarmer):
        async def warm(self, cache: object) -> None:
            warmed.append("ran")

    cache = InMemoryCache()
    cache.add_warmer(RecordingWarmer())

    async with cache:
        assert warmed == ["ran"]


async def test_cache_backend_warmer_populates_cache_entries() -> None:
    """
    Entries set by a warmer must be retrievable via get() immediately after
    the backend starts.
    """
    from varco_core.cache.memory import InMemoryCache

    async def products() -> list[tuple[str, str]]:
        return [("product:1", "widget"), ("product:2", "gadget")]

    cache = InMemoryCache()
    cache.add_warmer(QueryCacheWarmer(products))

    async with cache:
        assert await cache.get("product:1") == "widget"
        assert await cache.get("product:2") == "gadget"


async def test_cache_backend_failing_warmer_does_not_abort_start() -> None:
    """
    A warmer that raises must NOT prevent the backend from starting.
    _run_warmers() catches exceptions and logs them.
    """

    from varco_core.cache.memory import InMemoryCache

    class BrokenWarmer(CacheWarmer):
        async def warm(self, cache: object) -> None:
            raise ValueError("simulated warmer failure")

    cache = InMemoryCache()
    cache.add_warmer(BrokenWarmer())

    # Backend must start despite the warmer failing.
    async with cache:
        # Cache is operational — just not warm.
        await cache.set("manual", "ok")
        assert await cache.get("manual") == "ok"


async def test_cache_backend_multiple_warmers_all_run() -> None:
    """
    All registered warmers must run, not just the first one.
    """
    from varco_core.cache.memory import InMemoryCache

    ran: list[str] = []

    class NamedWarmer(CacheWarmer):
        def __init__(self, name: str) -> None:
            self._name = name

        async def warm(self, cache: object) -> None:
            ran.append(self._name)

    cache = InMemoryCache()
    cache.add_warmer(NamedWarmer("first"))
    cache.add_warmer(NamedWarmer("second"))
    cache.add_warmer(NamedWarmer("third"))

    async with cache:
        assert ran == ["first", "second", "third"]
