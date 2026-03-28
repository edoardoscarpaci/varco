"""
varco_core.cache.warming
========================
Cache warming / preloading for the varco cache system.

Problem
-------
After a service restarts or a new replica starts, the cache is cold.  The
first N requests all miss the cache and hit the database simultaneously —
the "thundering herd" problem.  Cold-start latency spikes and the database
receives a sudden burst of queries it would normally answer from cache.

Solution
--------
``CacheWarmer`` defines a simple lifecycle: ``warm(cache)`` is called by the
backend *after* ``start()`` (inside ``__aenter__``).  Warmers load common
data into the cache so the first real requests are likely cache hits.

DESIGN: CacheWarmer as an ABC over a plain callable
    ✅ ABCs can be subclassed, documented, and type-checked — a plain callable
       loses the contract and is harder to introspect in DI containers.
    ✅ Multiple warmer implementations can compose via the ``CompositeWarmer``.
    ✅ The ``__repr__`` is used in logs so operators can see which warmers ran.
    ❌ Slightly more ceremony than a simple ``async def warm(cache): ...`` lambda.

Alternative considered: pass warmers directly to ``CacheBackend.__init__``
    Rejected — ``CacheBackend`` is an ABC with no ``__init__`` — adding one
    would force every existing subclass to call ``super().__init__(warmers=...)``.
    Using ``add_warmer()`` / ``_warmers`` keeps the change non-breaking.

Components
----------
``CacheWarmer``
    ABC — single abstract method: ``warm(cache: AsyncCache) -> None``.

``QueryCacheWarmer``
    Runs a caller-supplied async query function at warm time and stores each
    ``(key, value)`` pair in the cache.  The query is lazy — it runs at
    warm time, not at construction time, so it is safe to construct before
    the DB session is available.

``SnapshotCacheWarmer``
    Loads a pre-computed snapshot dict ``{key: value}`` by calling a supplier
    function.  Useful for loading from a serialized checkpoint or a static
    in-memory seed.

``CompositeWarmer``
    Runs multiple ``CacheWarmer`` instances in sequence.

Hooking into the lifecycle::

    from varco_core.cache.warming import QueryCacheWarmer
    from varco_core.cache import InMemoryCache

    async def load_top_users() -> list[tuple[str, dict]]:
        users = await db.query("SELECT * FROM users LIMIT 100")
        return [(f"user:{u.id}", u.to_dict()) for u in users]

    cache = InMemoryCache()
    cache.add_warmer(QueryCacheWarmer(load_top_users))

    async with cache:   # start() called, then all warmers run
        ...

Thread safety:  ⚠️ ``add_warmer()`` is not thread-safe — call before ``start()``.
Async safety:   ✅ ``warm()`` is ``async def`` — safe to await I/O inside.

📚 Docs
- 🐍 https://docs.python.org/3/library/abc.html
  abc.ABC — abstract base class used for ``CacheWarmer``.
- 📐 https://en.wikipedia.org/wiki/Cache_warming
  Cache warming concept — pre-populating cache before traffic arrives.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable

_logger = logging.getLogger(__name__)


# ── CacheWarmer ABC ────────────────────────────────────────────────────────────


class CacheWarmer(ABC):
    """
    Abstract base class for cache warmers.

    A warmer is called once by the hosting ``CacheBackend`` after ``start()``
    completes (inside ``__aenter__``).  It should populate the cache with
    commonly accessed data so the first real requests are cache hits.

    DESIGN: single ``warm(cache)`` method
        ✅ Simple contract — warmers are focused on one job.
        ✅ Receives the live cache object — can call ``set()``, ``exists()``,
           and ``get()`` freely.
        ✅ Can perform async I/O (DB queries, HTTP fetches) during warm.
        ❌ Warmers run synchronously in sequence — a slow warmer delays startup.
           For startup-speed-critical services, consider running warmers as a
           background task after startup completes.

    Thread safety:  ⚠️ Defined by each subclass.
    Async safety:   ✅ ``warm()`` is ``async def``.

    Edge cases:
        - If ``warm()`` raises, ``add_warmer()`` callers should catch and log
          the exception — one failed warmer should not prevent the backend from
          starting.  See ``CacheBackend._run_warmers()``.
        - Warmers run AFTER ``start()`` — the cache is fully operational when
          ``warm()`` is called.  Calling ``set()`` inside ``warm()`` is always safe.
    """

    @abstractmethod
    async def warm(self, cache: Any) -> None:
        """
        Populate the cache with pre-loaded data.

        Called once by the hosting backend during ``__aenter__``.

        Args:
            cache: The live cache backend (satisfies ``AsyncCache`` protocol).
                   Implementations call ``await cache.set(key, value)`` to
                   populate entries.

        Raises:
            Exception: Any exception is caught and logged by the caller
                       (``CacheBackend._run_warmers()``).  Warmers must NOT
                       silence their own exceptions — let them propagate so
                       the host can decide whether to abort startup or log
                       and continue.

        Async safety:   ✅ Fully async — DB queries, HTTP calls, etc. are safe.

        Edge cases:
            - Setting a key that already exists overwrites the existing value.
            - The cache is fully started — calling ``get()``, ``exists()``,
              ``set()``, ``delete()`` are all safe.
        """

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


# ── QueryCacheWarmer ───────────────────────────────────────────────────────────


class QueryCacheWarmer(CacheWarmer):
    """
    Warmer that runs an async query function and stores results in the cache.

    The query callable returns a list of ``(key, value)`` pairs (or any iterable
    of 2-tuples).  Each pair is stored with ``await cache.set(key, value)``.

    DESIGN: callable-based query over injected repository
        ✅ The query callable is completely flexible — it can call a DB,
           read a file, or call an HTTP API.
        ✅ Lazy execution — the callable runs at warm time, not at construction
           time.  This makes it safe to construct the warmer before the DB
           connection exists.
        ✅ No coupling to a specific repository or ORM.
        ❌ Requires the caller to manage the callable's dependencies (DB session,
           etc.) — typically handled by a DI provider or closure.

    Thread safety:  ⚠️ The query callable may not be thread-safe — use from
                        the same event loop that owns the DB session.
    Async safety:   ✅ ``warm()`` awaits the query callable.

    Args:
        query_fn: Async callable that returns an iterable of ``(key, value)``
                  tuples.  Called once per ``warm()``.
        ttl:      Optional TTL in seconds passed to each ``cache.set()`` call.
                  ``None`` uses the backend default TTL.

    Edge cases:
        - If ``query_fn()`` returns an empty iterable, no entries are loaded
          and no error is raised.
        - If ``query_fn()`` raises, the exception propagates to ``_run_warmers()``
          which logs and continues (does not abort startup).
        - Individual ``cache.set()`` failures are NOT caught here — they propagate
          to ``_run_warmers()``.

    Example::

        async def top_products() -> list[tuple[str, dict]]:
            rows = await db.execute("SELECT id, name FROM products ORDER BY views DESC LIMIT 200")
            return [(f"product:{r.id}", {"id": r.id, "name": r.name}) for r in rows]

        warmer = QueryCacheWarmer(top_products, ttl=300)
        cache.add_warmer(warmer)
    """

    def __init__(
        self,
        query_fn: Callable[[], Awaitable[list[tuple[Any, Any]]]],
        *,
        ttl: float | None = None,
    ) -> None:
        """
        Args:
            query_fn: Async callable returning ``[(key, value), ...]``.
            ttl:      Per-entry TTL in seconds.  ``None`` = backend default.
        """
        self._query_fn = query_fn
        self._ttl = ttl

    async def warm(self, cache: Any) -> None:
        """
        Execute the query callable and populate the cache.

        Args:
            cache: The live cache backend.

        Edge cases:
            - Empty result from query_fn → no entries loaded, no error.
            - query_fn raises → propagates to caller (not caught here).
        """
        entries = await self._query_fn()
        count = 0
        for key, value in entries:
            await cache.set(key, value, ttl=self._ttl)
            count += 1
        _logger.info(
            "QueryCacheWarmer: loaded %d entries (ttl=%s)",
            count,
            self._ttl,
        )

    def __repr__(self) -> str:
        return (
            f"QueryCacheWarmer("
            f"query_fn={self._query_fn.__name__!r}, "
            f"ttl={self._ttl})"
        )


# ── SnapshotCacheWarmer ───────────────────────────────────────────────────────


class SnapshotCacheWarmer(CacheWarmer):
    """
    Warmer that loads a pre-computed snapshot dict into the cache.

    The snapshot supplier is an async callable that returns a ``dict[key, value]``.
    Each entry is stored with ``await cache.set(key, value)``.  Useful for loading
    from a serialized checkpoint, a static in-memory seed, or a config-file snapshot.

    DESIGN: supplier callable over direct dict injection
        ✅ Lazy — the supplier runs at warm time (inside ``start()``), not at
           construction time.  Prevents stale snapshots from being injected.
        ✅ Same pattern as ``QueryCacheWarmer`` — caller manages all data sources.
        ❌ Snapshots can be large — loading the full dict into memory may cause
           a spike.  Use ``QueryCacheWarmer`` with pagination for large datasets.

    Thread safety:  ⚠️ Depends on the supplier callable.
    Async safety:   ✅ Supplier is awaited inside ``warm()``.

    Args:
        snapshot_fn: Async callable returning ``dict[key, value]``.
        ttl:         Per-entry TTL in seconds.  ``None`` = backend default.

    Edge cases:
        - Empty snapshot → no entries loaded, no error.
        - snapshot_fn raises → propagates to ``_run_warmers()``.

    Example::

        async def load_config_snapshot() -> dict[str, str]:
            return {"feature:dark_mode": "enabled", "feature:beta": "disabled"}

        warmer = SnapshotCacheWarmer(load_config_snapshot, ttl=None)
        cache.add_warmer(warmer)
    """

    def __init__(
        self,
        snapshot_fn: Callable[[], Awaitable[dict[Any, Any]]],
        *,
        ttl: float | None = None,
    ) -> None:
        """
        Args:
            snapshot_fn: Async callable returning ``{key: value, ...}``.
            ttl:         Per-entry TTL.  ``None`` = backend default.
        """
        self._snapshot_fn = snapshot_fn
        self._ttl = ttl

    async def warm(self, cache: Any) -> None:
        """
        Load the snapshot dict and populate the cache.

        Args:
            cache: The live cache backend.

        Edge cases:
            - Empty snapshot → no entries loaded, no error.
            - snapshot_fn raises → propagates to caller.
        """
        snapshot = await self._snapshot_fn()
        count = 0
        for key, value in snapshot.items():
            await cache.set(key, value, ttl=self._ttl)
            count += 1
        _logger.info(
            "SnapshotCacheWarmer: loaded %d entries from snapshot (ttl=%s)",
            count,
            self._ttl,
        )

    def __repr__(self) -> str:
        return (
            f"SnapshotCacheWarmer("
            f"snapshot_fn={self._snapshot_fn.__name__!r}, "
            f"ttl={self._ttl})"
        )


# ── CompositeWarmer ────────────────────────────────────────────────────────────


class CompositeWarmer(CacheWarmer):
    """
    Runs multiple ``CacheWarmer`` instances sequentially.

    Warmers are executed in the order they are provided.  If any warmer raises,
    the exception propagates immediately — subsequent warmers are not executed.
    Catch-and-continue behaviour is handled by the caller (``_run_warmers()``).

    DESIGN: sequential execution over concurrent
        ✅ Simpler reasoning — no inter-warmer dependencies or race conditions.
        ✅ One failed warmer visibly stops the chain — no silent partial warm.
        ❌ Slow warmers delay startup proportionally.  For speed-critical
           services, run warmers as background tasks after ``start()`` returns.

    Thread safety:  ⚠️ Depends on constituent warmers.
    Async safety:   ✅ Sequential ``await`` on each warmer.

    Args:
        warmers: The constituent warmers to execute in order.

    Edge cases:
        - Empty warmer list → ``warm()`` is a no-op.
        - If warmer N raises, warmers N+1 … M are not called.

    Example::

        composite = CompositeWarmer(
            SnapshotCacheWarmer(config_snapshot),
            QueryCacheWarmer(top_users, ttl=300),
        )
        cache.add_warmer(composite)
    """

    def __init__(self, *warmers: CacheWarmer) -> None:
        """
        Args:
            *warmers: One or more ``CacheWarmer`` instances to execute in order.
        """
        self._warmers = list(warmers)

    async def warm(self, cache: Any) -> None:
        """
        Run each constituent warmer sequentially.

        Args:
            cache: The live cache backend.

        Raises:
            Exception: From the first constituent warmer that raises.
        """
        for warmer in self._warmers:
            await warmer.warm(cache)

    def __repr__(self) -> str:
        return f"CompositeWarmer(warmers={self._warmers!r})"


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "CacheWarmer",
    "QueryCacheWarmer",
    "SnapshotCacheWarmer",
    "CompositeWarmer",
]
