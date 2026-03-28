"""
varco_redis.rate_limit
=======================
Redis-backed distributed rate limiter implementing ``RateLimiter`` from
``varco_core.resilience.rate_limit``.

``RedisRateLimiter`` uses a **sorted set** per key where each member is a
unique call ID and the score is the call's Unix timestamp.  The sliding
window is maintained by:

1. Removing all members with scores older than ``now - period``
   (``ZREMRANGEBYSCORE key 0 expiry_boundary``).
2. Counting remaining members (``ZCARD key``).
3. If count < rate: adding a new member with score ``now`` (``ZADD``).
4. Setting an expiry on the key so Redis auto-cleans idle keys (``EXPIRE``).

All four commands execute inside a **Lua script** (``EVAL``) so they are
atomic from Redis's perspective — no TOCTOU window between step 2 and step 3.

Architecture::

    varco_core.resilience.RateLimiter  (ABC)
        ↑ implemented by
    RedisRateLimiter                    (THIS CLASS)
        ↑ configured by
    RedisEventBusSettings               (reuses existing Redis connection config)

Usage (standalone)::

    from varco_redis.rate_limit import RedisRateLimiter
    from varco_redis.config import RedisEventBusSettings
    from varco_core.resilience.rate_limit import RateLimitConfig, rate_limit

    config = RateLimitConfig(rate=10, period=1.0)
    limiter = RedisRateLimiter(config, settings=RedisEventBusSettings())
    await limiter.connect()

    @rate_limit(limiter=limiter, key_fn=lambda user_id: user_id)
    async def call_api(user_id: str, payload: dict) -> Response:
        ...

    await limiter.disconnect()

Or use as an async context manager::

    async with RedisRateLimiter(config, settings=settings) as limiter:
        @rate_limit(limiter=limiter)
        async def handler(...): ...

DESIGN: Lua script (EVAL) over pipeline + conditional
    ✅ Atomic — ZREMRANGEBYSCORE + ZCARD + conditional ZADD happen as one
       Redis operation.  Under high concurrency, two clients can not both
       read count=9 < 10 and both add a 10th entry.
    ✅ No optimistic-lock retries needed — the script either adds or not.
    ❌ Lua scripts are executed synchronously on the Redis server — long-
       running scripts block other Redis clients.  This script is O(log N)
       per call (ZREMRANGEBYSCORE range is bounded by rate), so it is fast.

DESIGN: one sorted set per key
    ✅ Isolated counters per rate-limit namespace (per-user, per-IP, etc.).
    ✅ Redis EXPIRE auto-cleans idle keys after the window expires.
    ❌ Key count grows with the number of distinct keys — bounded in practice
       by the tenant/user population.  Use Redis ``maxmemory-policy allkeys-lru``
       to evict if needed.

DESIGN: member = unique call ID (UUID hex)
    ✅ Sorted set members must be unique — if two calls land at the same
       millisecond with the same score, using ``time.time()`` as member would
       collapse them into one entry.  A unique member avoids score collisions.
    ✅ ZADD NX (not used here — Lua handles the conditional) ensures no
       duplicate members.

Thread safety:  ❌  Not thread-safe — use from a single asyncio event loop.
Async safety:   ✅  All methods are ``async def``.
                    Redis commands are non-blocking within the event loop.

📚 Docs
- 🔍 https://redis.io/docs/manual/programmability/eval-intro/
  EVAL — atomic Lua scripting in Redis
- 🔍 https://redis.io/commands/zadd/
  ZADD — Sorted Set add with score
- 🔍 https://redis.io/commands/zremrangebyscore/
  ZREMRANGEBYSCORE — Remove sorted set members by score range
- 🔍 https://redis.io/commands/zcard/
  ZCARD — Count members of a sorted set
"""

from __future__ import annotations

import logging
import time
import uuid

import redis.asyncio as aioredis

from varco_core.resilience.rate_limit import RateLimitConfig, RateLimiter
from varco_redis.config import RedisEventBusSettings

_logger = logging.getLogger(__name__)

# ── Lua script ────────────────────────────────────────────────────────────────
# Atomic sliding-window check + record.
#
# KEYS[1]  — Redis sorted set key for this rate-limit namespace.
# ARGV[1]  — current Unix timestamp (float as string, microsecond precision).
# ARGV[2]  — window start boundary = now - period (float as string).
# ARGV[3]  — rate (max calls allowed).
# ARGV[4]  — unique call member ID (prevents score-collision collapsing).
# ARGV[5]  — key TTL in seconds (ceil(period) + 1, to auto-clean idle keys).
#
# Returns: 1 if the call is allowed (added), 0 if rate-limited (denied).
_SLIDING_WINDOW_SCRIPT = """
local key        = KEYS[1]
local now        = tonumber(ARGV[1])
local window_low = tonumber(ARGV[2])
local rate       = tonumber(ARGV[3])
local member     = ARGV[4]
local ttl        = tonumber(ARGV[5])

-- Remove all calls that have scrolled outside the rolling window.
redis.call('ZREMRANGEBYSCORE', key, 0, window_low)

-- Count calls still inside the window.
local count = redis.call('ZCARD', key)

if count < rate then
    -- Within budget — record this call and allow it.
    redis.call('ZADD', key, now, member)
    -- Reset TTL so the key lives at least one full window after the last call.
    redis.call('EXPIRE', key, ttl)
    return 1
end

-- Over budget — do not record, deny the call.
return 0
"""

# Retry-after script: returns the score (timestamp) of the oldest call
# in the window, or 0 if the window is not full.
_RETRY_AFTER_SCRIPT = """
local key        = KEYS[1]
local now        = tonumber(ARGV[1])
local window_low = tonumber(ARGV[2])
local rate       = tonumber(ARGV[3])
local period     = tonumber(ARGV[4])

redis.call('ZREMRANGEBYSCORE', key, 0, window_low)
local count = redis.call('ZCARD', key)

if count < rate then
    return '0'
end

-- The oldest member's score is the timestamp of the earliest call in the window.
-- The caller must wait until (oldest + period - now) seconds for that slot to free.
local oldest_members = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
if #oldest_members == 0 then
    return '0'
end
local oldest = tonumber(oldest_members[2])
local wait = (oldest + period) - now
if wait < 0 then
    return '0'
end
return tostring(wait)
"""


class RedisRateLimiter(RateLimiter):
    """
    Distributed sliding-window rate limiter backed by Redis sorted sets.

    Each rate-limit key maps to a Redis sorted set.  Set members are unique
    call IDs; scores are call timestamps.  Window membership is enforced
    atomically via a Lua script so concurrent callers in different processes
    or pods share an accurate counter.

    Lifecycle:
        Call ``await limiter.connect()`` before first use.  Call
        ``await limiter.disconnect()`` when done.  Or use as an async context
        manager (``async with RedisRateLimiter(...) as limiter: ...``).

    Args:
        config:   Rate-limit configuration (rate, period).
        settings: Redis connection settings.  Defaults to env-based
                  ``RedisEventBusSettings()``.

    DESIGN: reusing RedisEventBusSettings instead of a dedicated settings class
        ✅ No new settings class needed — same URL/prefix/timeout fields apply.
        ❌ The ``channel_prefix`` field is re-purposed as a rate-limit key
           prefix, which may be surprising.  Callers can pass a settings
           instance with a distinct prefix if namespace separation is needed.

    Thread safety:  ❌  Not thread-safe.  Use from one asyncio event loop.
    Async safety:   ✅  All methods are ``async def``.

    Attributes:
        config:   Immutable rate-limit configuration.
        settings: Immutable Redis connection settings.

    Edge cases:
        - ``acquire()`` before ``connect()`` raises ``RuntimeError``.
        - ``reset()`` deletes the sorted set key entirely — the next call
          starts a fresh window.
        - If Redis is unavailable, ``acquire()`` raises
          ``redis.asyncio.RedisError`` — callers should decide whether to
          fail open (allow the call) or fail closed (deny it).
          Wrapping with ``@circuit_breaker`` is recommended for production use.

    Example::

        config = RateLimitConfig(rate=100, period=60.0)
        async with RedisRateLimiter(config) as limiter:
            @rate_limit(limiter=limiter, key_fn=lambda uid, *_, **__: uid)
            async def upload(uid: str, data: bytes) -> None: ...
    """

    def __init__(
        self,
        config: RateLimitConfig,
        *,
        settings: RedisEventBusSettings | None = None,
    ) -> None:
        """
        Args:
            config:   Immutable rate-limit configuration.
            settings: Redis connection settings.  If omitted, reads from env
                      (``RedisEventBusSettings()``).
        """
        self.config = config
        self.settings = settings or RedisEventBusSettings()
        self._redis: aioredis.Redis | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """
        Open the Redis connection.

        Must be called before ``acquire()`` / ``reset()`` / ``retry_after()``.

        Raises:
            redis.asyncio.RedisError: Connection refused or auth failure.
        """
        self._redis = aioredis.from_url(
            self.settings.url,
            decode_responses=False,  # scores come back as bytes — decoded in-code
            socket_timeout=self.settings.socket_timeout,
            **self.settings.redis_kwargs,
        )
        _logger.debug("RedisRateLimiter connected to %s.", self.settings.url)

    async def disconnect(self) -> None:
        """
        Close the Redis connection.

        Safe to call even if not connected.
        """
        if self._redis is not None:
            await self._redis.aclose()
            self._redis = None
            _logger.debug("RedisRateLimiter disconnected.")

    async def __aenter__(self) -> RedisRateLimiter:
        """Connect on context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        """Disconnect on context manager exit."""
        await self.disconnect()

    # ── RateLimiter interface ─────────────────────────────────────────────────

    async def acquire(self, key: str = "default") -> bool:
        """
        Try to acquire a call slot for ``key`` using a Redis atomic Lua script.

        Args:
            key: Rate-limit namespace.

        Returns:
            ``True`` if the call is within budget; ``False`` if denied.

        Raises:
            RuntimeError:             ``connect()`` has not been called.
            redis.asyncio.RedisError: Redis I/O error.

        Async safety: ✅  Lua script is atomic — no TOCTOU window.

        Edge cases:
            - Clock drift across nodes: Redis uses its own server-side time
              (``time.time()`` on the client is only used as the ARGV score here).
              In a multi-node Redis cluster, use ``TIME`` command instead of
              client-side timestamps for strict fairness.  For single Redis
              nodes, client timestamps are acceptable.
        """
        redis = self._require_redis()
        redis_key = self._key(key)
        now = time.time()
        window_low = now - self.config.period
        # Unique member prevents score-collision collapsing when two calls
        # arrive within the same floating-point timestamp.
        member = uuid.uuid4().hex
        # Key TTL: one full period after the last call is the earliest Redis
        # can safely evict this key.  Add 1 second margin.
        ttl = int(self.config.period) + 1

        result = await redis.eval(  # type: ignore[attr-defined]
            _SLIDING_WINDOW_SCRIPT,
            1,  # number of KEYS
            redis_key,  # KEYS[1]
            str(now),  # ARGV[1] — current timestamp
            str(window_low),  # ARGV[2] — window lower boundary
            str(self.config.rate),  # ARGV[3] — rate
            member,  # ARGV[4] — unique member
            str(ttl),  # ARGV[5] — key TTL
        )
        allowed = bool(result)
        _logger.debug(
            "RedisRateLimiter key=%r %s (rate=%d, period=%.3f).",
            key,
            "allowed" if allowed else "denied",
            self.config.rate,
            self.config.period,
        )
        return allowed

    async def reset(self, key: str = "default") -> None:
        """
        Delete the Redis sorted set for ``key``, resetting its budget.

        Args:
            key: Rate-limit key to reset.

        Raises:
            RuntimeError:             ``connect()`` has not been called.
            redis.asyncio.RedisError: Redis I/O error.
        """
        redis = self._require_redis()
        await redis.delete(self._key(key))
        _logger.debug("RedisRateLimiter key=%r reset (key deleted).", key)

    async def retry_after(self, key: str = "default") -> float:
        """
        Return seconds until the oldest call in the full window expires.

        Args:
            key: Rate-limit key to inspect.

        Returns:
            Seconds to wait.  ``0.0`` if budget is not currently exhausted.

        Raises:
            RuntimeError:             ``connect()`` has not been called.
            redis.asyncio.RedisError: Redis I/O error.
        """
        redis = self._require_redis()
        redis_key = self._key(key)
        now = time.time()
        window_low = now - self.config.period

        raw = await redis.eval(  # type: ignore[attr-defined]
            _RETRY_AFTER_SCRIPT,
            1,
            redis_key,
            str(now),
            str(window_low),
            str(self.config.rate),
            str(self.config.period),
        )
        # Redis EVAL returns bytes when decode_responses=False.
        wait = float(raw.decode() if isinstance(raw, bytes) else raw)
        return max(0.0, wait)

    # ── Private helpers ───────────────────────────────────────────────────────

    def _key(self, key: str) -> str:
        """Build the full Redis sorted set key, applying the channel prefix."""
        return f"{self.settings.channel_prefix}rl:{key}"

    def _require_redis(self) -> aioredis.Redis:
        """Return the Redis client, raising if not yet connected."""
        if self._redis is None:
            raise RuntimeError(
                "RedisRateLimiter is not connected.  "
                "Call await limiter.connect() or use it as an async context manager."
            )
        return self._redis

    def __repr__(self) -> str:
        connected = self._redis is not None
        return (
            f"RedisRateLimiter("
            f"rate={self.config.rate}, "
            f"period={self.config.period}, "
            f"connected={connected})"
        )


__all__ = [
    "RedisRateLimiter",
]
