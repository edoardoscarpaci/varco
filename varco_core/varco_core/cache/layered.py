"""
varco_core.cache.layered
=========================
Multi-layer (L1/L2/…) cache implementation.

``LayeredCache`` composes two or more ``CacheBackend`` instances into a
single coherent cache.  Reads walk the layers from fastest (L1) to slowest
(Ln) and, on a miss, promote the found value back up the chain.  Writes
propagate to all layers simultaneously (write-through).

Architecture
------------
::

    Read path:
        get(key)
          ↓ L1 hit?  → return value
          ↓ L1 miss  → try L2
          ↓ L2 hit?  → promote to L1 → return value
          ↓ L2 miss  → try L3 … → None

    Write path (write-through — default):
        set(key, value)
          → set on L1
          → set on L2
          → set on L3 … (all layers updated atomically from the caller's view)

    Delete path:
        delete(key)
          → delete on all layers

Write modes
-----------
``"write-through"`` (default)
    Every ``set()`` writes to all layers.  Guarantees that all layers are
    consistent immediately.  Higher write latency (blocks on all layers).

``"write-around"``
    ``set()`` writes only to the LAST layer (e.g. Redis).  L1 is populated
    lazily on the next read-promote cycle.  Lower write latency but L1 may
    serve stale data until the next read.

    Useful when writes are infrequent and L1 hit rate is high.

DESIGN: write-through as default over write-around
    ✅ Consistency by default — no stale L1 data after a write.
    ✅ Simpler reasoning — caller doesn't need to know about layer topology.
    ❌ Higher write latency for deep stacks (e.g. Redis as L2).
       Mitigated by ``asyncio.gather()`` — all layers are written concurrently.

Read-promote TTL
----------------
When a value is promoted from Ln to L1 on a read, the L1 entry gets a
TTL equal to ``promote_ttl`` (default: ``None`` → uses L1's own default TTL).
If ``promote_ttl`` is set, L1 will serve a slightly stale window while the
authoritative value lives in L2.  This is a deliberate trade-off:

    promote_ttl=60 → L1 cache has at most 60s of staleness.
    promote_ttl=None → L1 uses its configured TTL (same freshness window).

Thread safety:  ❌  Not thread-safe.  Use from a single event loop.
Async safety:   ✅  All public methods are ``async def``.

📚 Docs
- 🐍 asyncio.gather — https://docs.python.org/3/library/asyncio-task.html#asyncio.gather
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Literal

from varco_core.cache.base import CacheBackend

_logger = logging.getLogger(__name__)

WriteMode = Literal["write-through", "write-around"]


class LayeredCache(CacheBackend):
    """
    Multi-layer composite cache backend.

    Reads walk from L1 (fastest) to Ln (slowest) and promote found values
    back to faster layers.  Writes propagate to all layers (``write-through``)
    or only to the last layer (``write-around``).

    Lifecycle:
        ``start()`` / ``stop()`` propagate to all layers in order.  Layers are
        stopped in reverse order (LIFO) — symmetric with start.

    Args:
        *layers:      Two or more ``CacheBackend`` instances ordered from
                      fastest (L1) to slowest (Ln).
        write_mode:   ``"write-through"`` (default) or ``"write-around"``.
        promote_ttl:  TTL in seconds applied to L1 entries on read-promote.
                      ``None`` → use L1's own default TTL.

    Raises:
        ValueError: If fewer than 2 layers are provided.

    Thread safety:  ❌  Not thread-safe — delegates to child backends.
    Async safety:   ✅  All operations use ``asyncio.gather()`` for concurrency.

    Edge cases:
        - ``write-around`` + a fast L1 write-miss results in the L1 serving
          stale data until the next promote cycle.  Set ``promote_ttl`` to
          bound staleness.
        - If a layer's ``set()`` raises, the exception propagates to the caller.
          Remaining layers may or may not have been written — callers should
          treat the write as best-effort in error scenarios.
        - ``clear()`` clears ALL layers — this is intentional.  To clear a
          single layer, access it directly.

    Example::

        from varco_core.cache.memory import InMemoryCache
        from varco_core.cache.invalidation import TTLStrategy
        from varco_redis.cache import RedisCache, RedisCacheSettings

        l1 = InMemoryCache(strategy=TTLStrategy(60))    # 60s stale window
        l2 = RedisCache(RedisCacheSettings())            # authoritative store

        async with LayeredCache(l1, l2, promote_ttl=60) as cache:
            await cache.set("user:42", user_obj)
            result = await cache.get("user:42")   # served from L1 on second read
    """

    def __init__(
        self,
        *layers: CacheBackend,
        write_mode: WriteMode = "write-through",
        promote_ttl: float | None = None,
    ) -> None:
        """
        Args:
            *layers:    Two or more cache backends, fastest first.
            write_mode: ``"write-through"`` writes to all layers; ``"write-around"``
                        writes only to the last layer.
            promote_ttl: TTL applied when promoting a value from Ln to L1.
                         ``None`` = use L1's own default.

        Raises:
            ValueError: If fewer than 2 layers are given.
        """
        if len(layers) < 2:
            raise ValueError(
                f"LayeredCache requires at least 2 layers; got {len(layers)}. "
                f"Use a single CacheBackend directly if you only have one."
            )
        self._layers: tuple[CacheBackend, ...] = layers
        self._write_mode: WriteMode = write_mode
        self._promote_ttl = promote_ttl
        self._started = False

    # ── Properties ─────────────────────────────────────────────────────────────

    @property
    def layers(self) -> tuple[CacheBackend, ...]:
        """The composed backend layers (L1 first)."""
        return self._layers

    @property
    def write_mode(self) -> WriteMode:
        """Active write mode."""
        return self._write_mode

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start all layers in declaration order.

        Raises:
            RuntimeError: If already started.
        """
        if self._started:
            raise RuntimeError(
                "LayeredCache.start() called on an already-started cache. "
                "Call stop() first."
            )
        for layer in self._layers:
            await layer.start()
        self._started = True
        _logger.debug(
            "LayeredCache started (%d layers, mode=%s).",
            len(self._layers),
            self._write_mode,
        )

    async def stop(self) -> None:
        """Stop all layers in reverse order (LIFO).  Idempotent."""
        if not self._started:
            return
        for layer in reversed(self._layers):
            await layer.stop()
        self._started = False
        _logger.debug("LayeredCache stopped.")

    # ── Cache operations ───────────────────────────────────────────────────────

    async def get(self, key: Any, *, type_hint: type | None = None) -> Any | None:
        """
        Walk layers from L1 to Ln.  On a miss, promote to faster layers.

        On a hit in layer Ln (n > 1), the value is written back to all faster
        layers (L1 … L(n-1)) with ``promote_ttl``.

        Args:
            key:       Cache key to look up.
            type_hint: Forwarded to each layer's ``get()`` so serializing backends
                       (e.g. Redis as L2) can reconstruct typed objects.

        Returns:
            Cached value, or ``None`` if absent in all layers.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        for i, layer in enumerate(self._layers):
            value = await layer.get(key, type_hint=type_hint)
            if value is not None:
                # Promote to faster layers if the hit was not in L1.
                if i > 0:
                    # Write-back to all layers faster than the one we hit.
                    # asyncio.gather allows concurrent promotion for deep stacks.
                    await asyncio.gather(
                        *(
                            faster.set(key, value, ttl=self._promote_ttl)
                            for faster in self._layers[:i]
                        )
                    )
                    _logger.debug(
                        "LayeredCache: promoted key %r from L%d to L1..L%d.",
                        key,
                        i + 1,
                        i,
                    )
                return value
        return None

    async def set(self, key: Any, value: Any, *, ttl: float | None = None) -> None:
        """
        Store ``value`` under ``key`` according to the active write mode.

        ``write-through``: Writes to all layers concurrently via
        ``asyncio.gather()``.

        ``write-around``: Writes only to the LAST layer.  L1 is populated
        lazily on the next read-promote cycle.

        Args:
            key:   Cache key.
            value: Value to store.
            ttl:   Per-entry TTL forwarded to each layer.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        if self._write_mode == "write-through":
            # All layers updated concurrently.
            await asyncio.gather(
                *(layer.set(key, value, ttl=ttl) for layer in self._layers)
            )
        else:
            # write-around — only the slowest (authoritative) layer is written.
            await self._layers[-1].set(key, value, ttl=ttl)

    async def delete(self, key: Any) -> None:
        """
        Remove ``key`` from ALL layers.

        Args:
            key: Cache key to remove.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        await asyncio.gather(*(layer.delete(key) for layer in self._layers))

    async def exists(self, key: Any) -> bool:
        """
        Return ``True`` if ``key`` is present (and not invalidated) in ANY layer.

        Checks layers in order — returns immediately on the first ``True``.

        Args:
            key: Cache key to check.

        Returns:
            ``True`` if the key exists in at least one layer.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        for layer in self._layers:
            if await layer.exists(key):
                return True
        return False

    async def clear(self) -> None:
        """
        Clear ALL entries from ALL layers.

        Raises:
            RuntimeError: If the cache has not been started.
        """
        self._require_started()
        await asyncio.gather(*(layer.clear() for layer in self._layers))
        _logger.debug("LayeredCache: cleared all %d layers.", len(self._layers))

    async def delete_prefix(self, prefix: str) -> None:
        """
        Remove all entries matching ``prefix`` from ALL layers concurrently.

        Delegates to each layer's ``delete_prefix()`` via ``asyncio.gather()``
        — same concurrency model as ``delete()`` and ``clear()``.

        Args:
            prefix: Key prefix to match and remove.

        Returns:
            None.

        Raises:
            RuntimeError: If the cache has not been started.

        Edge cases:
            - If a layer raises, the exception propagates to the caller.
              Remaining layers may or may not have completed their deletion.
        """
        self._require_started()
        # All layers are updated concurrently — consistent with delete() / clear().
        await asyncio.gather(*(layer.delete_prefix(prefix) for layer in self._layers))
        _logger.debug(
            "LayeredCache: delete_prefix(%r) across %d layers.",
            prefix,
            len(self._layers),
        )

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
        layers_repr = ", ".join(repr(layer) for layer in self._layers)
        return (
            f"LayeredCache("
            f"layers=[{layers_repr}], "
            f"write_mode={self._write_mode!r}, "
            f"promote_ttl={self._promote_ttl!r}, "
            f"started={self._started})"
        )


__all__ = ["LayeredCache"]
