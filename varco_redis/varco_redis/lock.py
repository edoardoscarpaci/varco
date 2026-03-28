"""
varco_redis.lock
================
Redis-backed distributed lock for cross-process coordination.

``RedisLock`` implements ``AbstractDistributedLock`` using Redis SET NX PX for
acquisition and a Lua script for atomic token-guarded release.

Key format::

    {key_prefix}lock:{key}

Acquire strategy
----------------
``try_acquire``:
    ``SET {redis_key} {token} NX PX {ttl_ms}``
    NX   — only set if NOT exists (atomically acquires if free).
    PX   — TTL in milliseconds (auto-expiry prevents forever-held locks).
    Returns a ``LockHandle`` if SET succeeded (returned "OK"), else ``None``.

Release strategy
----------------
``release``:
    Executes a Lua script in a single round-trip::

        if redis.call("GET", KEYS[1]) == ARGV[1]
        then return redis.call("DEL", KEYS[1])
        else return 0
        end

    This check-and-delete is **atomic** on the Redis server — no other command
    can execute between GET and DEL.  The token check prevents a holder whose
    TTL expired from accidentally deleting a new holder's key.

DESIGN: SET NX PX + Lua release over Redlock multi-node algorithm
    ✅ Single-node SET NX PX is simpler, has lower latency, and sufficient
       for the vast majority of deployments where Redis is a single primary
       or a replicated primary (Redis Sentinel / ElastiCache).
    ✅ Lua script for release is atomic — eliminates the GET→check→DEL race.
    ✅ PX TTL guarantees liveness — a crashed holder eventually releases the lock.
    ❌ Single-node Redis: if Redis crashes between acquire and release, the lock
       TTL ensures eventual release but there is no quorum guarantee.
       For strict multi-node split-brain safety, implement Redlock across N nodes.
    ❌ Redis replication lag: in async replica setups, a failover may promote a
       replica that does not yet have the latest key.  Mitigate with
       WAIT command or Redis Sentinel with quorum acks.

Alternative considered: Redlock (acquire on N/2+1 nodes)
    Rejected — significantly more complex, requires N Redis nodes per lock,
    and the correctness guarantees under network partitions are debated
    (see Kleppmann's analysis).  Single-node is correct for typical services.

Alternative considered: SETNX + EXPIRE (two commands)
    Rejected — non-atomic: a crash between SETNX and EXPIRE leaves the key
    with no TTL, holding the lock forever.  SET NX PX is atomic.

Usage::

    from varco_redis.lock import RedisLock

    lock = RedisLock(ttl_seconds=30)
    await lock.connect()

    # Non-blocking
    handle = await lock.try_acquire("order:42", ttl=30)
    if handle:
        async with handle:
            await process_order(42)

    # Blocking with timeout
    async with await lock.acquire("order:42", ttl=30, timeout=5.0):
        await process_order(42)

Thread safety:  ❌ Not thread-safe — use from a single event loop.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🔍 https://redis.io/commands/set/
  Redis SET command — NX and PX options for atomic conditional set with TTL.
- 🔍 https://redis.io/commands/eval/
  Redis EVAL — Lua scripting for atomic multi-command sequences.
- 📐 https://redis.io/docs/manual/patterns/distributed-locks/
  Official Redis distributed locks pattern documentation.
- 📐 https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
  Kleppmann's analysis — important caveats about fencing tokens under GC pauses.
- 🔍 https://redis-py.readthedocs.io/en/stable/
  redis.asyncio — async Python client used here.
"""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID, uuid4

import redis.asyncio as aioredis

from varco_core.lock import AbstractDistributedLock, LockHandle

from varco_redis.config import RedisEventBusSettings

_logger = logging.getLogger(__name__)

# ── Lua release script ─────────────────────────────────────────────────────────
#
# Atomically checks the stored token and deletes the key only if it matches.
# Running as a Lua script means Redis executes GET + conditional DEL as a single
# atomic operation — no other command can interleave between the check and the
# delete.
#
# KEYS[1]  — the Redis key (e.g. "varco:lock:order:42")
# ARGV[1]  — the token string that must match for release to proceed
#
# Returns: 1 if the key was deleted (released), 0 if token mismatch or key gone.
_RELEASE_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1]
then return redis.call("DEL", KEYS[1])
else return 0
end
"""

# Prefix for all lock keys — distinguishes lock keys from other Redis keys.
_DEFAULT_KEY_PREFIX = "lock:"


class RedisLock(AbstractDistributedLock):
    """
    Redis-backed distributed lock using SET NX PX + Lua atomic release.

    Each acquire attempt sets a Redis key with a UUID token and a TTL.  Release
    uses a Lua script to atomically verify the token before deleting the key.

    DESIGN: per-key Redis key with TTL over a single global lock key
        ✅ Independent keys never contend with each other.
        ✅ TTL provides automatic liveness — crashed holders eventually release.
        ✅ Lua release script eliminates the GET→check→DEL race window.
        ❌ Redis restart without persistence clears all lock state — holders
           lose their locks but do not know it (their LockHandle.release()
           silently no-ops on the next call).
        ❌ Single-node — not split-brain safe under Redis primary failover.

    Thread safety:  ❌ redis.asyncio client is not thread-safe.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        settings:   ``RedisEventBusSettings`` providing the connection URL and
                    key prefix.  Defaults to ``RedisEventBusSettings()``.
        key_prefix: Additional prefix for all lock keys, appended after the
                    settings channel_prefix.  Default: ``"lock:"``.

    Edge cases:
        - Call ``connect()`` before ``try_acquire()`` / ``acquire()`` / ``release()``.
        - Keys survive Redis connection drops — the TTL is enforced server-side.
        - A LockHandle returned after a Redis crash may have an expired token —
          ``release()`` will be a silent no-op (correct behaviour).
        - ``key_prefix`` combined with ``settings.channel_prefix`` produces the
          full Redis key prefix.  Use different prefixes in dev/staging/prod if
          they share a Redis instance.

    Example::

        lock = RedisLock()
        await lock.connect()

        try:
            async with await lock.acquire("inventory:item_1", ttl=30, timeout=5.0):
                await reserve_inventory(item_id=1)
        except LockNotAcquiredError:
            raise HTTPException(429, "Service busy — please retry")
        finally:
            await lock.disconnect()
    """

    def __init__(
        self,
        *,
        settings: RedisEventBusSettings | None = None,
        key_prefix: str = _DEFAULT_KEY_PREFIX,
    ) -> None:
        """
        Args:
            settings:   Redis connection settings.  ``None`` → defaults to
                        ``RedisEventBusSettings()`` (reads env vars).
            key_prefix: Prefix appended to every lock key after the settings
                        channel prefix.  Default: ``"lock:"``.
        """
        self._settings = settings or RedisEventBusSettings()
        # Full Redis key prefix: {channel_prefix}{key_prefix}
        # e.g. "prod:lock:" for channel_prefix="prod:", key_prefix="lock:"
        self._key_prefix = f"{self._settings.channel_prefix}{key_prefix}"
        # Redis client is set in connect() — lazy to avoid event loop requirement.
        self._redis: Any | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def connect(self) -> None:
        """
        Open the Redis connection.  Idempotent.

        Must be called before any lock operations.

        Raises:
            redis.asyncio.ConnectionError: If Redis is unreachable.

        Edge cases:
            - Calling a second time is a no-op — existing connection is reused.
        """
        if self._redis is not None:
            return
        self._redis = aioredis.from_url(
            self._settings.url,
            decode_responses=False,  # tokens are bytes — keep raw
            socket_timeout=self._settings.socket_timeout,
            **self._settings.redis_kwargs,
        )
        _logger.info(
            "RedisLock connected (url=%s, key_prefix=%r)",
            self._settings.url,
            self._key_prefix,
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
        _logger.info("RedisLock disconnected.")

    async def __aenter__(self) -> RedisLock:
        """Connect on context-manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Disconnect on context-manager exit."""
        await self.disconnect()

    # ── AbstractDistributedLock interface ─────────────────────────────────────

    async def try_acquire(
        self,
        key: str,
        *,
        ttl: float,
    ) -> LockHandle | None:
        """
        Attempt to acquire the Redis lock for ``key`` without blocking.

        Issues a single ``SET {redis_key} {token} NX PX {ttl_ms}`` command.
        Returns a ``LockHandle`` if SET returned "OK", else ``None``.

        Args:
            key: The logical lock key (e.g. ``"order:42"``).
                 The Redis key is ``{key_prefix}{key}``.
            ttl: Lock TTL in seconds.  The Redis key expires automatically
                 after this many seconds if ``release()`` is not called.
                 Must be positive.

        Returns:
            A ``LockHandle`` if the lock was acquired (SET NX succeeded).
            ``None`` if the lock is currently held by another process.

        Raises:
            ValueError:                     If ``key`` is empty or ``ttl <= 0``.
            AssertionError:                 If called before ``connect()``.
            redis.asyncio.RedisError:       On unexpected Redis errors.

        Thread safety:  ❌ Not thread-safe across OS threads.
        Async safety:   ✅ Single ``await`` on the SET NX PX command.

        Edge cases:
            - If Redis is unavailable, the redis.asyncio client raises — callers
              should catch ``redis.asyncio.RedisError`` and decide whether to
              fail open or closed.
            - Concurrent calls from different processes: only one SET NX wins;
              others receive None.  This is the fundamental mutual-exclusion
              guarantee.
        """
        if not key:
            raise ValueError("Lock key must be a non-empty string.")
        if ttl <= 0:
            raise ValueError(f"Lock TTL must be positive; got ttl={ttl}.")
        assert self._redis is not None, (
            "RedisLock.try_acquire called before connect(). "
            "Call await lock.connect() or use 'async with lock:' first."
        )

        token = uuid4()
        # Convert TTL to milliseconds for PX option (Redis uses integers).
        ttl_ms = int(ttl * 1000)

        result = await self._redis.set(
            self._redis_key(key),
            str(token).encode(),  # store token as bytes (decode_responses=False)
            nx=True,  # NX: only set if NOT exists — atomic acquire
            px=ttl_ms,  # PX: TTL in milliseconds — liveness guarantee
        )

        if result is None:
            # SET NX returned None — lock is already held.
            return None

        # SET returned "OK" (as bytes b"OK" or the string "OK" depending on
        # decode_responses) — lock acquired.
        _logger.debug(
            "RedisLock acquired key=%r token=%s ttl_ms=%d",
            key,
            token,
            ttl_ms,
        )
        return LockHandle(key=key, token=token, lock=self)

    async def release(self, key: str, token: UUID) -> None:
        """
        Release the Redis lock for ``key`` if the stored token matches.

        Executes the atomic Lua release script — checks token and deletes in
        a single Redis command, preventing phantom releases.

        Does NOT raise — any exception is logged and swallowed, per the
        ``AbstractDistributedLock`` contract.

        Args:
            key:   The logical lock key.
            token: The UUID token from the ``LockHandle``.

        Thread safety:  ❌ Not thread-safe across OS threads.
        Async safety:   ✅ Single ``await`` on the EVAL command.

        Edge cases:
            - Token mismatch (TTL expired, new holder) → Lua returns 0, no DEL.
            - Key does not exist (already expired) → Lua returns 0, no-op.
            - Redis unavailable → logs error, returns without raising.
        """
        try:
            assert self._redis is not None, (
                "RedisLock.release called before connect(). "
                "The lock handle's release() should only be called after connect()."
            )
            result = await self._redis.eval(
                _RELEASE_SCRIPT,
                1,  # numkeys=1 — KEYS[1] is the lock key
                self._redis_key(key),  # KEYS[1]
                str(token).encode(),  # ARGV[1] — token to check
            )
            if result == 0:
                _logger.debug(
                    "RedisLock.release: key=%r token=%s — "
                    "token mismatch or key expired (no-op).",
                    key,
                    token,
                )
            else:
                _logger.debug("RedisLock released key=%r token=%s", key, token)
        except Exception as exc:
            # release must NOT raise — log and swallow per contract.
            _logger.error(
                "RedisLock.release failed for key=%r token=%s: %s",
                key,
                token,
                exc,
            )

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _redis_key(self, key: str) -> str:
        """
        Build the full Redis key for a logical lock key.

        Args:
            key: Logical lock key (e.g. ``"order:42"``).

        Returns:
            Full Redis key: ``{key_prefix}{key}`` (e.g. ``"prod:lock:order:42"``).

        Edge cases:
            - No validation of ``key`` content — the caller is responsible for
              ensuring keys are safe strings.  This avoids double-validation
              overhead in the hot path.
        """
        return f"{self._key_prefix}{key}"

    def __repr__(self) -> str:
        return (
            f"RedisLock("
            f"url={self._settings.url!r}, "
            f"key_prefix={self._key_prefix!r}, "
            f"connected={self._redis is not None})"
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = ["RedisLock"]
