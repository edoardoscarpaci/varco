"""
varco_redis.deduplication
=========================
Redis-backed message deduplication for the varco event system.

``RedisDeduplicator`` implements ``AbstractDeduplicator`` using a Redis key
per event ID with a configurable TTL.  The TTL defines the deduplication
window: any re-delivery of the same event within the TTL window is suppressed.

Key format::

    {channel_prefix}dedup:{event_id}

Strategy
--------
``is_duplicate``:  ``EXISTS {key}``      — O(1), single round-trip.
``mark_seen``:     ``SET {key} 1 NX PX {ttl_ms}``  — atomic set-if-not-exists.
                   NX means the key is only set if it does NOT already exist —
                   idempotent by design.

DESIGN: two separate commands (EXISTS + SET NX PX) over a single Lua script
    ✅ No Lua scripting needed — simpler ops story, works with Redis Cluster.
    ✅ SET NX PX in mark_seen is atomic check-and-set on its own — avoids
       double-marking in concurrent scenarios.
    ❌ A race window exists between is_duplicate (EXISTS) and mark_seen (SET NX):
       two concurrent consumers could both see is_duplicate=False, both call
       mark_seen, and both process the event.  This window is sub-millisecond
       on a local Redis — acceptable for at-least-once deduplication.
       For strict exactly-once, use a transactional inbox pattern instead.

Alternative considered: single Lua script (EVAL) for atomic check-and-set
    Rejected — SET NX already covers the idempotency requirement on mark_seen.
    The only race is between concurrent is_duplicate calls, which are rare in
    single-consumer deployments.  Lua adds operational complexity without
    meaningfully improving deduplication guarantees.

Alternative considered: Redis Bloom Filter (RedisBloom module)
    Rejected — not available in all Redis deployments.  Simple key-per-event
    is universally supported and sufficient for typical event volumes.

TTL considerations
------------------
The ``ttl_seconds`` parameter controls the deduplication window.  It should be
set to at least 2× the maximum expected broker re-delivery delay.  Default is
86400 seconds (24 h) — covers most at-most-once delivery windows.

For short-lived events (e.g. real-time sensor data), a smaller TTL reduces
Redis memory usage.  For long-lived audit events, a larger TTL may be needed.

Usage::

    from varco_redis.deduplication import RedisDeduplicator

    dedup = RedisDeduplicator(ttl_seconds=3600)   # 1-hour window
    await dedup.connect()

    class OrderConsumer(EventConsumer):
        @listen(OrderPlacedEvent, channel="orders", deduplicator=dedup)
        async def on_order(self, event: OrderPlacedEvent) -> None:
            await self._process(event)  # called at most once per event_id

Thread safety:  ❌ Not thread-safe — use from a single event loop.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🔍 https://redis.io/commands/set/
  Redis SET command — NX and PX options for atomic set-if-not-exists with TTL.
- 🔍 https://redis.io/commands/exists/
  Redis EXISTS command — O(1) key existence check.
- 🔍 https://redis-py.readthedocs.io/en/stable/
  redis.asyncio — async Python Redis client.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

import redis.asyncio as aioredis

from varco_core.event.deduplication import AbstractDeduplicator
from varco_redis.config import RedisEventBusSettings

_logger = logging.getLogger(__name__)

# Default deduplication window: 24 hours.
# Covers most at-least-once broker re-delivery windows.
_DEFAULT_TTL_SECONDS: int = 86_400


class RedisDeduplicator(AbstractDeduplicator):
    """
    Redis-backed message deduplicator using SET NX PX.

    Each processed event is recorded as a Redis key with a TTL.  Re-deliveries
    within the TTL window are detected via ``EXISTS`` and suppressed.

    DESIGN: per-event Redis key with TTL over a persistent set
        ✅ TTL provides automatic cleanup — no external eviction logic needed.
        ✅ Scales to millions of events — Redis handles key expiry efficiently.
        ✅ Atomically handles concurrent mark_seen calls via SET NX (only the
           first caller sets the key; subsequent callers are no-ops).
        ❌ TTL granularity is per-event — can't cheaply "clear all seen events
           for a given channel".  Use a key prefix convention if needed.
        ❌ Redis restart without persistence clears all deduplication state —
           events may be re-processed after a Redis restart if broker holds them.

    Thread safety:  ❌ redis.asyncio client is not thread-safe.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        settings:     ``RedisEventBusSettings`` providing the connection URL and
                      key prefix.  Defaults to ``RedisEventBusSettings.from_env()``.
        ttl_seconds:  Deduplication window in seconds.  Keys expire after this
                      duration.  Default: 86 400 (24 hours).

    Edge cases:
        - Call ``connect()`` before ``is_duplicate()`` / ``mark_seen()`` — both
          methods assert the Redis client is connected.
        - Evicted TTL keys cause previously-seen events to appear new on
          re-delivery.  Size the TTL to match the longest expected re-delivery
          window.
        - ``is_duplicate`` on a network-error may return ``False`` (safe default)
          rather than raising — "process the event" is safer than "drop the event"
          on transient Redis failures.

    Example::

        dedup = RedisDeduplicator(ttl_seconds=3600)
        async with dedup:           # connects on enter, disconnects on exit
            consumer.register_to(bus)   # wires dedup into each @listen handler
    """

    def __init__(
        self,
        *,
        settings: RedisEventBusSettings | None = None,
        ttl_seconds: int = _DEFAULT_TTL_SECONDS,
    ) -> None:
        """
        Args:
            settings:    Redis connection settings.  ``None`` → reads from env.
            ttl_seconds: Deduplication TTL in seconds.  Default: 86 400.
        """
        self._settings = settings or RedisEventBusSettings()
        # Convert to milliseconds for Redis PX option (SET key value PX ms).
        self._ttl_ms: int = ttl_seconds * 1000
        # Key prefix: {channel_prefix}dedup:{event_id}
        # Using the channel_prefix from settings so different environments
        # (dev/prod) do not collide if they share a Redis instance.
        self._key_prefix: str = f"{self._settings.channel_prefix}dedup:"
        # Redis client is lazily created in connect() — not here, because
        # redis.asyncio requires a running event loop at construction time.
        self._redis: Any | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """
        Open the Redis connection.  Idempotent.

        Must be called before ``is_duplicate()`` or ``mark_seen()``.

        Raises:
            redis.asyncio.ConnectionError: If Redis is unreachable at connect time.

        Edge cases:
            - Calling a second time is a no-op — existing connection is reused.
        """
        if self._redis is not None:
            return
        self._redis = aioredis.from_url(
            self._settings.url,
            # decode_responses=False — keys are strings, values are "1" bytes.
            # We keep it False for consistency with the rest of varco_redis.
            decode_responses=False,
            socket_timeout=self._settings.socket_timeout,
            **self._settings.redis_kwargs,
        )
        _logger.info(
            "RedisDeduplicator connected (url=%s, key_prefix=%r, ttl_ms=%d)",
            self._settings.url,
            self._key_prefix,
            self._ttl_ms,
        )

    async def disconnect(self) -> None:
        """
        Close the Redis connection.  Idempotent.

        Edge cases:
            - Calling before ``connect()`` is a no-op.
        """
        if self._redis is None:
            return
        await self._redis.aclose()
        self._redis = None
        _logger.info("RedisDeduplicator disconnected.")

    async def __aenter__(self) -> RedisDeduplicator:
        """Connect on context-manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Disconnect on context-manager exit."""
        await self.disconnect()

    # ── AbstractDeduplicator interface ─────────────────────────────────────────

    async def is_duplicate(self, event_id: UUID) -> bool:
        """
        Return ``True`` if the Redis key for ``event_id`` exists (i.e. TTL not expired).

        Issues a single ``EXISTS {key}`` command — O(1).

        Args:
            event_id: Event UUID to check.

        Returns:
            ``True`` if the key exists (event was seen); ``False`` if new or TTL
            expired.  On transient Redis errors returns ``False`` (safe default —
            prefer processing the event over silently dropping it).

        Thread safety:  ❌ Not thread-safe across OS threads.
        Async safety:   ✅ Single ``await`` on the EXISTS command.

        Edge cases:
            - If Redis is unreachable: logs a warning and returns ``False`` —
              the event will be processed (possibly again after reconnection).
            - If the key's TTL has expired: returns ``False`` — the event will be
              re-processed (this is correct if the TTL covers the re-delivery window).
        """
        assert self._redis is not None, (
            "RedisDeduplicator.is_duplicate called before connect(). "
            "Call await dedup.connect() or use 'async with dedup:' first."
        )
        try:
            result: int = await self._redis.exists(self._key(event_id))
            return result > 0
        except Exception as exc:
            # Safe default: do NOT suppress the event on Redis errors.
            # Processing the event twice is safer than silently dropping it.
            _logger.warning(
                "RedisDeduplicator.is_duplicate failed for event_id=%s: %s. "
                "Returning False (will process event).",
                event_id,
                exc,
            )
            return False

    async def mark_seen(self, event_id: UUID) -> None:
        """
        Set the Redis key for ``event_id`` with TTL, if not already set.

        Uses ``SET key 1 NX PX {ttl_ms}`` — atomic: only sets if the key does
        NOT already exist (NX).  This makes the operation idempotent: calling
        ``mark_seen`` on an already-seen event_id is a no-op.

        Does NOT raise — any exception is logged and swallowed, per the
        ``AbstractDeduplicator.mark_seen`` contract.

        Args:
            event_id: Event UUID to mark as processed.

        Thread safety:  ❌ Not thread-safe across OS threads.
        Async safety:   ✅ Single ``await`` on the SET NX PX command.

        Edge cases:
            - Already-seen key: SET NX does nothing (returns None) — idempotent.
            - Redis unavailable: logs error and returns without raising.
            - TTL of 0 ms would expire immediately — use ``ttl_seconds >= 1``.
        """
        try:
            assert self._redis is not None, (
                "RedisDeduplicator.mark_seen called before connect(). "
                "Call await dedup.connect() or use 'async with dedup:' first."
            )
            # SET key value NX PX ttl_ms
            # NX   → only set if NOT exists (idempotent re-delivery handling)
            # PX   → TTL in milliseconds (automatic cleanup after window expires)
            # Value is b"1" — the presence of the key is all that matters.
            await self._redis.set(
                self._key(event_id),
                b"1",
                nx=True,
                px=self._ttl_ms,
            )
        except Exception as exc:
            # mark_seen MUST NOT raise — contract from AbstractDeduplicator.
            _logger.error(
                "RedisDeduplicator.mark_seen failed for event_id=%s: %s",
                event_id,
                exc,
            )

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _key(self, event_id: UUID) -> str:
        """
        Build the Redis key for a given event ID.

        Args:
            event_id: Event UUID to build the key for.

        Returns:
            Full Redis key string: ``{channel_prefix}dedup:{event_id}``.

        Edge cases:
            - UUID is converted to its canonical string form (lowercase hex
              with dashes) — consistent across all Python versions.
        """
        return f"{self._key_prefix}{event_id}"

    def __repr__(self) -> str:
        return (
            f"RedisDeduplicator("
            f"url={self._settings.url!r}, "
            f"ttl_ms={self._ttl_ms}, "
            f"connected={self._redis is not None})"
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = ["RedisDeduplicator"]
