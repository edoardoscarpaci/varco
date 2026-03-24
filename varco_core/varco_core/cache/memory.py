"""
varco_core.cache.memory
========================
In-process cache backends — no external dependencies required.

``NoOpCache``
    Does nothing.  All writes are discarded; all reads return ``None``.
    Useful in tests or as a disabled-cache placeholder.

``InMemoryCache``
    Full-featured in-process LRU/unbounded cache backed by a plain dict.
    Supports TTL enforcement via ``TTLStrategy`` (or any composite).
    Appropriate for L1 in a ``LayeredCache`` or standalone dev/test use.

DESIGN: dict-backed cache over lru_cache / functools
    ✅ Explicit key/value/TTL/metadata control — ``functools.lru_cache``
       doesn't expose per-entry TTL or manual invalidation.
    ✅ Supports arbitrary hashable keys, not just function arguments.
    ✅ Pluggable ``InvalidationStrategy`` — TTL is not hard-coded.
    ❌ No automatic size cap — set ``max_size`` to protect heap memory.

Thread safety:  ❌  Not thread-safe.  Both backends use a plain dict.
Async safety:   ✅  All public methods are ``async def``.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from varco_core.cache.base import CacheBackend, InvalidationStrategy
from varco_core.cache.config import CacheSettings

_logger = logging.getLogger(__name__)


# ── _Entry dataclass ──────────────────────────────────────────────────────────


class _Entry:
    """
    Internal storage entry for ``InMemoryCache``.

    Attributes:
        value:     The cached value.
        ttl:       Per-entry TTL in seconds (``None`` = fallback to strategy).
        stored_at: UNIX timestamp when this entry was stored.
        tags:      Optional set of string tags for ``TaggedStrategy``.

    Thread safety:  ❌  Plain Python object — not thread-safe.
    """

    __slots__ = ("value", "ttl", "stored_at", "tags")

    def __init__(
        self,
        value: Any,
        ttl: float | None,
        tags: set[str] | None = None,
    ) -> None:
        self.value = value
        self.ttl = ttl
        # Capture wall clock at write time so TTLStrategy can compute age.
        self.stored_at: float = time.time()
        self.tags: set[str] = tags or set()

    def to_metadata(self) -> dict[str, Any]:
        """
        Build the metadata dict passed to ``InvalidationStrategy.should_invalidate()``.

        Returns:
            Dict with ``"stored_at"``, ``"ttl"``, and ``"tags"`` keys.
        """
        return {
            "stored_at": self.stored_at,
            "ttl": self.ttl,
            "tags": self.tags,
        }


# ── NoOpCache ─────────────────────────────────────────────────────────────────


class NoOpCache(CacheBackend):
    """
    No-operation cache backend.

    All writes are silently discarded.  All reads return ``None`` / ``False``.
    Useful as a disabled-cache placeholder or in unit tests where caching
    should have no effect.

    Lifecycle:
        ``start()`` and ``stop()`` are no-ops — nothing to initialise or tear down.

    Thread safety:  ✅ Fully stateless — no shared mutable state.
    Async safety:   ✅ All methods are ``async def``.

    Edge cases:
        - ``exists()`` always returns ``False``.
        - ``clear()`` is always a no-op.
        - ``set()`` accepts but silently discards all arguments.
    """

    async def start(self) -> None:
        """No-op."""

    async def stop(self) -> None:
        """No-op."""

    async def get(self, key: Any) -> Any | None:
        """Always returns ``None`` — no entry is ever cached."""
        return None

    async def set(self, key: Any, value: Any, *, ttl: float | None = None) -> None:
        """No-op — value is silently discarded."""

    async def delete(self, key: Any) -> None:
        """No-op."""

    async def exists(self, key: Any) -> bool:
        """Always returns ``False``."""
        return False

    async def clear(self) -> None:
        """No-op."""

    def __repr__(self) -> str:
        return "NoOpCache()"


# ── InMemoryCache ─────────────────────────────────────────────────────────────


class InMemoryCache(CacheBackend):
    """
    In-process dict-backed cache with optional TTL and invalidation strategies.

    Stores all entries in a plain Python dict.  TTL enforcement and custom
    eviction are handled by an ``InvalidationStrategy`` (default: no strategy
    = entries never expire unless explicitly deleted or the cache is cleared).

    Suitable for:
        - Standalone dev/test caching (no external broker needed).
        - L1 layer in a ``LayeredCache`` (fast in-process hot cache).

    Lifecycle::

        async with InMemoryCache(settings, strategy=TTLStrategy(300)) as cache:
            await cache.set("key", value)
            result = await cache.get("key")

    Args:
        settings:   Cache configuration.  ``settings.default_ttl`` is used as
                    the per-entry fallback when no explicit ``ttl`` is given.
        strategy:   Pluggable ``InvalidationStrategy``.  ``None`` means entries
                    are kept until explicitly deleted or the cache is cleared.
        max_size:   Optional maximum number of entries.  When the dict exceeds
                    ``max_size``, the oldest-inserted key is evicted (FIFO).
                    ``None`` = unbounded.

    Thread safety:  ❌  Not thread-safe — the entry dict is a plain Python dict.
    Async safety:   ✅  All public methods are ``async def``.

    Edge cases:
        - ``max_size=0`` → every ``set()`` is immediately evicted (effectively
          a ``NoOpCache``).  Prefer ``NoOpCache`` explicitly.
        - A ``get()`` that triggers invalidation returns ``None`` and removes
          the entry from the dict (eager eviction on read).
        - If ``strategy`` is ``None``, ``should_invalidate()`` is never called —
          entries persist until ``delete()`` or ``clear()``.
    """

    def __init__(
        self,
        settings: CacheSettings | None = None,
        *,
        strategy: InvalidationStrategy | None = None,
        max_size: int | None = None,
    ) -> None:
        """
        Args:
            settings: Cache settings.  Defaults to ``CacheSettings()``
                      (no default TTL, reads from env).
            strategy: Invalidation strategy.  ``None`` = no expiry.
            max_size: Maximum number of cached entries.  ``None`` = unbounded.
        """
        self._settings = settings or CacheSettings()
        self._strategy = strategy
        self._max_size = max_size

        # Dict maintains insertion order (CPython 3.7+) — used for FIFO eviction.
        self._store: dict[Any, _Entry] = {}
        self._started = False

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Activate the cache and its strategy.

        Raises:
            RuntimeError: If already started.
        """
        if self._started:
            raise RuntimeError(
                "InMemoryCache.start() called on an already-started cache. "
                "Call stop() first."
            )
        if self._strategy is not None:
            await self._strategy.start()
        self._started = True
        _logger.debug(
            "InMemoryCache started (max_size=%s, strategy=%r).",
            self._max_size,
            self._strategy,
        )

    async def stop(self) -> None:
        """Stop the strategy and mark the cache as stopped.  Idempotent."""
        if not self._started:
            return
        if self._strategy is not None:
            await self._strategy.stop()
        self._started = False
        _logger.debug("InMemoryCache stopped.")

    # ── Cache operations ───────────────────────────────────────────────────────

    async def get(self, key: Any) -> Any | None:
        """
        Return the cached value for ``key``, or ``None`` on miss / expiry.

        If the strategy marks the entry as invalid, it is evicted eagerly
        before returning ``None``.

        Args:
            key: Cache key to look up.

        Returns:
            Cached value, or ``None`` if absent or invalidated.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        entry = self._store.get(key)
        if entry is None:
            return None

        # Ask the strategy whether this entry should be evicted.
        if self._strategy is not None and self._strategy.should_invalidate(
            key, entry.to_metadata()
        ):
            # Evict eagerly — return None as if the key never existed.
            del self._store[key]
            _logger.debug("InMemoryCache: evicted stale key %r.", key)
            return None

        return entry.value

    async def set(
        self,
        key: Any,
        value: Any,
        *,
        ttl: float | None = None,
        tags: set[str] | None = None,
    ) -> None:
        """
        Store ``value`` under ``key``.

        Args:
            key:   Cache key (must be hashable).
            value: Value to store.
            ttl:   Per-entry TTL in seconds.  ``None`` falls back to
                   ``settings.default_ttl``.
            tags:  Optional set of string tags for ``TaggedStrategy``.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        # Resolve effective TTL — explicit > settings default.
        effective_ttl = ttl if ttl is not None else self._settings.default_ttl
        self._store[key] = _Entry(value=value, ttl=effective_ttl, tags=tags)

        # Enforce max_size via FIFO eviction — pop the oldest entry.
        if self._max_size is not None and len(self._store) > self._max_size:
            oldest_key = next(iter(self._store))
            del self._store[oldest_key]
            _logger.debug(
                "InMemoryCache: FIFO evicted oldest key %r (max_size=%d reached).",
                oldest_key,
                self._max_size,
            )

    async def delete(self, key: Any) -> None:
        """
        Remove ``key`` from the cache.  Idempotent.

        Args:
            key: Cache key to remove.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        self._store.pop(key, None)

    async def exists(self, key: Any) -> bool:
        """
        Return ``True`` if ``key`` is present and not invalidated.

        Performs the same invalidation check as ``get()`` but does not
        return the value.

        Args:
            key: Cache key to check.

        Returns:
            ``True`` if the key exists and passes invalidation.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        # Reuse get() — it handles invalidation + eviction.
        # Sentinel approach: if get() returns None, we don't know if the
        # stored value was None or if it was a miss.  Check _store directly.
        self._require_started()
        entry = self._store.get(key)
        if entry is None:
            return False
        if self._strategy is not None and self._strategy.should_invalidate(
            key, entry.to_metadata()
        ):
            del self._store[key]
            return False
        return True

    async def clear(self) -> None:
        """
        Remove ALL entries from the cache.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        self._store.clear()
        _logger.debug("InMemoryCache: cleared.")

    # ── Metrics / introspection ────────────────────────────────────────────────

    @property
    def size(self) -> int:
        """Current number of entries (including potentially-stale ones)."""
        return len(self._store)

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _require_started(self) -> None:
        """
        Raise ``RuntimeError`` if the cache has not been started.

        Raises:
            RuntimeError: If ``start()`` has not been called.
        """
        if not self._started:
            raise RuntimeError(
                f"{type(self).__name__} is not started. "
                f"Call 'await cache.start()' or use 'async with cache' first."
            )

    def __repr__(self) -> str:
        return (
            f"InMemoryCache("
            f"size={len(self._store)}, "
            f"max_size={self._max_size!r}, "
            f"strategy={self._strategy!r}, "
            f"started={self._started})"
        )


__all__ = [
    "NoOpCache",
    "InMemoryCache",
]
