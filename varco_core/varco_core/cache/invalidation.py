"""
varco_core.cache.invalidation
==============================
Built-in ``InvalidationStrategy`` implementations.

``TTLStrategy``
    Time-based expiry.  ``should_invalidate()`` returns ``True`` when
    ``now - stored_at > ttl``.  Pure in-memory check — no I/O.

``ExplicitStrategy``
    Tracks a set of manually invalidated keys.  Call
    ``strategy.invalidate(key)`` / ``strategy.invalidate_many(keys)`` to
    mark keys for eviction on the next read.

    For cross-process event-driven invalidation, pair this strategy with
    ``CacheInvalidationConsumer`` (from ``varco_core.cache.consumer``).
    The consumer subscribes to the bus and calls
    ``strategy.invalidate_many(event.keys)`` — keeping all bus interaction
    inside ``EventConsumer`` where it belongs.

``TaggedStrategy``
    Associates keys with string tags at write-time.  Call
    ``strategy.invalidate_tag(tag)`` to mark every key with that tag for
    eviction.  Useful for entity-level cache busting (e.g. invalidate all
    keys tagged ``"user:42"`` when user 42 is updated).

``CompositeStrategy``
    Aggregates an ordered list of strategies.  ``should_invalidate()``
    returns ``True`` if ANY child strategy says yes.  ``start()``/``stop()``
    propagate to all children.

DESIGN: start()/stop() on every strategy vs conditional lifecycle
    ✅ Uniform contract — cache backends call start()/stop() unconditionally.
    ✅ No hasattr inspection — eliminates an entire category of runtime errors.
    ✅ CompositeStrategy can delegate lifecycle to children without branching.
    ❌ Pure-TTL strategies implement no-op start()/stop() — minor boilerplate.
       Acceptable — consistency beats saving two lines per class.

Thread safety:  ❌  Not thread-safe.  Use from a single event loop.
Async safety:   ✅  start()/stop() are async def.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from varco_core.cache.base import InvalidationStrategy

_logger = logging.getLogger(__name__)


# ── TTLStrategy ────────────────────────────────────────────────────────────────


class TTLStrategy(InvalidationStrategy):
    """
    Time-to-live invalidation strategy.

    Evicts entries whose age exceeds their TTL.  ``should_invalidate()``
    computes ``now - stored_at > effective_ttl`` synchronously — no I/O.

    The effective TTL per entry is resolved in this order:
        1. ``metadata["ttl"]`` — per-entry override set at write time.
        2. ``self.default_ttl``   — fallback configured on this strategy.
        3. If both are ``None``, the entry is never expired by this strategy.

    Lifecycle:
        ``start()`` and ``stop()`` are no-ops — TTL is a pure computation.

    Args:
        default_ttl: Fallback TTL in seconds.  ``None`` = never expire by default.

    Thread safety:  ✅ Stateless — ``should_invalidate()`` reads ``time.time()`` only.
    Async safety:   ✅ No async state.

    Edge cases:
        - ``default_ttl=0`` → every entry is immediately expired.
        - ``stored_at`` missing from metadata → treated as ``time.time()``
          (entry is considered brand-new and is NOT invalidated).
        - Negative TTL values are treated as 0 (immediate expiry).
    """

    def __init__(self, default_ttl: float | None = None) -> None:
        """
        Args:
            default_ttl: Default TTL in seconds for entries without a per-entry
                         TTL.  ``None`` means entries without a per-entry TTL
                         are kept indefinitely by this strategy.
        """
        self._default_ttl = default_ttl

    @property
    def default_ttl(self) -> float | None:
        """Default TTL configured on this strategy."""
        return self._default_ttl

    async def start(self) -> None:
        """No-op — TTL is a pure computation, no resources to acquire."""

    async def stop(self) -> None:
        """No-op — nothing to release."""

    def should_invalidate(self, key: Any, metadata: dict[str, Any]) -> bool:
        """
        Return ``True`` if the entry is older than its effective TTL.

        Args:
            key:      Cache key (not used — TTL is time-based, not key-based).
            metadata: Must contain ``"stored_at"`` (float UNIX timestamp) and
                      optionally ``"ttl"`` (float seconds).

        Returns:
            ``True`` if the entry should be evicted due to TTL expiry.
        """
        # Resolve effective TTL — per-entry override takes precedence.
        # Use explicit None check rather than dict.get() default: the key is
        # always present in metadata (set to None when no per-entry TTL was
        # given), so .get("ttl", fallback) would never reach the fallback.
        entry_ttl: float | None = metadata.get("ttl")
        effective_ttl: float | None = (
            entry_ttl if entry_ttl is not None else self._default_ttl
        )
        if effective_ttl is None:
            # No TTL configured — entry never expires via this strategy.
            return False

        stored_at: float = metadata.get("stored_at", time.time())
        age = time.time() - stored_at
        # Negative TTL is treated as 0 — any age triggers expiry.
        return age > max(0.0, effective_ttl)

    def __repr__(self) -> str:
        return f"TTLStrategy(default_ttl={self._default_ttl!r})"


# ── ExplicitStrategy ──────────────────────────────────────────────────────────


class ExplicitStrategy(InvalidationStrategy):
    """
    Manual key-based invalidation strategy.

    Maintains an in-memory set of explicitly invalidated keys.  Callers
    mark keys for eviction by calling ``invalidate()`` or ``invalidate_many()``.
    On the next read, ``should_invalidate()`` returns ``True`` for those keys
    and the cache backend evicts them.

    After eviction the key is automatically cleared from the invalidated set
    by the cache backend (which calls ``clear_invalidated(key)``).  If the
    backend does NOT call ``clear_invalidated()``, keys accumulate until
    ``reset()`` is called manually.

    Lifecycle:
        ``start()`` and ``stop()`` are no-ops — the invalidated set is
        maintained in memory regardless of lifecycle state.

    Thread safety:  ❌  The invalidated set is a plain Python set.
    Async safety:   ✅  ``invalidate()`` / ``invalidate_many()`` are sync —
                    safe to call from sync or async context.

    Edge cases:
        - Calling ``invalidate(key)`` on a key that does not exist in the
          cache is a silent no-op — the key is added to the invalidated set
          but the backend will never call ``should_invalidate()`` for it.
        - Memory grows without bound if the backend never calls
          ``clear_invalidated()`` — call ``reset()`` periodically in that case.
    """

    def __init__(self) -> None:
        # Plain set — not thread-safe, but the cache system is single-loop.
        self._invalidated: set[Any] = set()

    async def start(self) -> None:
        """No-op — the invalidated set lives in memory regardless."""

    async def stop(self) -> None:
        """No-op."""

    def invalidate(self, key: Any) -> None:
        """
        Mark ``key`` for eviction on the next read.

        Args:
            key: Cache key to invalidate.
        """
        self._invalidated.add(key)
        _logger.debug("ExplicitStrategy: marked key %r for invalidation.", key)

    def invalidate_many(self, keys: Any) -> None:
        """
        Mark multiple keys for eviction on the next read.

        Args:
            keys: Iterable of cache keys to invalidate.
        """
        for key in keys:
            self._invalidated.add(key)
        _logger.debug(
            "ExplicitStrategy: marked %d keys for invalidation.", len(self._invalidated)
        )

    def clear_invalidated(self, key: Any) -> None:
        """
        Remove ``key`` from the invalidated set after the backend evicts it.

        Called by the cache backend after eviction to prevent memory growth.

        Args:
            key: Cache key that has been evicted.
        """
        self._invalidated.discard(key)

    def reset(self) -> None:
        """Clear the entire invalidated set.  Use with caution."""
        self._invalidated.clear()
        _logger.debug("ExplicitStrategy: invalidated set cleared.")

    def should_invalidate(self, key: Any, metadata: dict[str, Any]) -> bool:
        """
        Return ``True`` if ``key`` was explicitly marked for invalidation.

        Args:
            key:      Cache key to check.
            metadata: Not used — explicit invalidation is key-based.

        Returns:
            ``True`` if the key is in the invalidated set.
        """
        return key in self._invalidated

    def __repr__(self) -> str:
        return f"ExplicitStrategy(invalidated={len(self._invalidated)})"


# ── TaggedStrategy ────────────────────────────────────────────────────────────


class TaggedStrategy(InvalidationStrategy):
    """
    Tag-based invalidation strategy.

    Associates cache keys with string tags at write time (via ``metadata``).
    Calling ``invalidate_tag(tag)`` marks every key with that tag for eviction.

    Tags are set by the cache backend at write time when it calls
    ``should_invalidate()`` with ``metadata["tags"]``.  The backend is
    responsible for passing the correct tag metadata.

    Typical usage::

        # Write with tags
        await cache.set("user:42:profile", profile, ttl=60, tags={"user:42"})

        # Invalidate all user:42 entries on update
        strategy.invalidate_tag("user:42")

    Lifecycle:
        ``start()`` and ``stop()`` are no-ops.

    Thread safety:  ❌  Plain dict/set — not thread-safe.
    Async safety:   ✅  ``invalidate_tag()`` is sync — safe from any context.

    Edge cases:
        - A key with no tags is never invalidated by this strategy alone.
        - Calling ``invalidate_tag()`` for a tag that has no associated keys
          is a silent no-op.
        - Tags accumulate until ``clear_tag()`` or ``reset()`` is called.
    """

    def __init__(self) -> None:
        # tag → set of invalidated keys for that tag
        self._invalidated_tags: set[str] = set()
        # key → set of tags associated with that key (populated at read time)
        self._key_tags: dict[Any, set[str]] = {}

    async def start(self) -> None:
        """No-op."""

    async def stop(self) -> None:
        """No-op."""

    def invalidate_tag(self, tag: str) -> None:
        """
        Mark all keys associated with ``tag`` for eviction on the next read.

        Args:
            tag: Tag string to invalidate (e.g. ``"user:42"``).
        """
        self._invalidated_tags.add(tag)
        _logger.debug("TaggedStrategy: invalidated tag %r.", tag)

    def register_tags(self, key: Any, tags: set[str]) -> None:
        """
        Register the tags for a cache key.

        Called by the cache backend at write time so the strategy can map
        tags to keys.  The backend should pass ``tags`` from the ``set()``
        call kwargs.

        Args:
            key:  Cache key being written.
            tags: Set of tag strings to associate with this key.
        """
        if tags:
            self._key_tags[key] = tags

    def clear_tag(self, tag: str) -> None:
        """
        Remove ``tag`` from the invalidated set.

        Args:
            tag: Tag to un-invalidate.
        """
        self._invalidated_tags.discard(tag)

    def reset(self) -> None:
        """Clear all invalidated tags and key→tag mappings."""
        self._invalidated_tags.clear()
        self._key_tags.clear()

    def should_invalidate(self, key: Any, metadata: dict[str, Any]) -> bool:
        """
        Return ``True`` if any of ``key``'s tags are in the invalidated set.

        Also updates the internal key→tag mapping from ``metadata["tags"]``
        so the strategy knows which tags a key carries.

        Args:
            key:      Cache key.
            metadata: Expected to contain ``"tags"`` (set[str]).

        Returns:
            ``True`` if the key carries at least one invalidated tag.
        """
        tags: set[str] = metadata.get("tags", set())
        # Keep the key→tag map up to date for future tag invalidations.
        if tags:
            self._key_tags[key] = tags
        return bool(tags & self._invalidated_tags)

    def __repr__(self) -> str:
        return (
            f"TaggedStrategy("
            f"invalidated_tags={len(self._invalidated_tags)}, "
            f"tracked_keys={len(self._key_tags)})"
        )


# ── CompositeStrategy ──────────────────────────────────────────────────────────


class CompositeStrategy(InvalidationStrategy):
    """
    Aggregate multiple ``InvalidationStrategy`` instances.

    ``should_invalidate()`` returns ``True`` if ANY child strategy returns
    ``True`` (logical OR).  ``start()`` and ``stop()`` propagate to all
    children in order.

    This enables combining orthogonal invalidation axes::

        explicit = ExplicitStrategy()
        strategy = CompositeStrategy(
            TTLStrategy(default_ttl=300),
            explicit,
        )
        # Wire cross-process invalidation via EventConsumer — not here:
        consumer = CacheInvalidationConsumer(explicit, channel="cache-invalidations")
        consumer.register_to(bus)

    Lifecycle:
        Children are started in declaration order and stopped in reverse order
        (LIFO) — mirrors typical resource acquisition/release patterns.

    Args:
        *strategies: One or more ``InvalidationStrategy`` instances.

    Raises:
        ValueError: If no strategies are provided.

    Thread safety:  ❌  Delegates to children — see each child's docstring.
    Async safety:   ✅  start()/stop() are ``async def`` and await each child.

    Edge cases:
        - An empty ``CompositeStrategy`` raises ``ValueError`` at construction —
          use a single no-op strategy instead.
        - If one child's ``start()`` raises, subsequent children are NOT started
          and the partially-started state must be cleaned up by the caller.
    """

    def __init__(self, *strategies: InvalidationStrategy) -> None:
        """
        Args:
            *strategies: One or more ``InvalidationStrategy`` instances to compose.

        Raises:
            ValueError: If no strategies are passed.
        """
        if not strategies:
            raise ValueError(
                "CompositeStrategy requires at least one InvalidationStrategy. "
                "Pass one or more strategy instances."
            )
        # Tuple — immutable after construction; order is deterministic.
        self._strategies: tuple[InvalidationStrategy, ...] = strategies

    @property
    def strategies(self) -> tuple[InvalidationStrategy, ...]:
        """The composed strategy instances (read-only)."""
        return self._strategies

    async def start(self) -> None:
        """
        Start all child strategies in declaration order.

        Raises:
            RuntimeError: If a child raises on start (partial state — caller
                          is responsible for cleanup).
        """
        for strategy in self._strategies:
            await strategy.start()

    async def stop(self) -> None:
        """
        Stop all child strategies in reverse order (LIFO).  Idempotent.
        """
        for strategy in reversed(self._strategies):
            await strategy.stop()

    def should_invalidate(self, key: Any, metadata: dict[str, Any]) -> bool:
        """
        Return ``True`` if ANY child strategy says the entry should be evicted.

        Short-circuits on the first ``True`` — remaining children are not
        consulted.

        Args:
            key:      Cache key.
            metadata: Forwarded unchanged to each child strategy.

        Returns:
            ``True`` if at least one child strategy returns ``True``.
        """
        return any(s.should_invalidate(key, metadata) for s in self._strategies)

    def __repr__(self) -> str:
        children = ", ".join(repr(s) for s in self._strategies)
        return f"CompositeStrategy({children})"


__all__ = [
    "TTLStrategy",
    "ExplicitStrategy",
    "TaggedStrategy",
    "CompositeStrategy",
]
