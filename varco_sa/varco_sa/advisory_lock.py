"""
varco_sa.advisory_lock
=======================
PostgreSQL advisory lock implementation of ``AbstractDistributedLock``.

Uses PostgreSQL's session-level advisory lock functions::

    SELECT pg_try_advisory_lock(int8)  — non-blocking acquire
    SELECT pg_advisory_unlock(int8)    — release

Each held lock pins one connection from the ``AsyncEngine`` pool for the
duration of the lock.  The connection is returned to the pool on ``release()``
or when the lock's ``LockHandle`` is used as an async context manager.

Key hashing
-----------
PostgreSQL advisory locks use an ``int8`` (64-bit signed integer) key.  String
lock keys are hashed deterministically with ``hashlib.md5`` (truncated to 63
bits to stay within signed int64 range)::

    hash = int.from_bytes(md5(key.encode()).digest()[:8], "big") & 0x7FFFFFFFFFFFFFFF

DESIGN: advisory lock over application-level locking (Redis SET NX)
    ✅ Native to PostgreSQL — no additional infrastructure (no Redis).
    ✅ Session-level lock is automatically released if the process crashes
       (PostgreSQL detects the broken connection and releases the lock).
    ✅ Fair on most PostgreSQL versions — other waiters are unblocked in order.
    ❌ Requires a pinned connection for the lock lifetime — may starve the pool
       in high-concurrency scenarios.  Tune pool size accordingly.
    ❌ TTL is not natively enforced — a process holding the lock indefinitely
       is only released when its connection closes.  Pair with ``timeout``
       in ``acquire()`` and ensure critical sections are time-bounded.
    ❌ Lock granularity is int64 — collisions across different string keys
       are astronomically unlikely (2^63 key space) but theoretically possible.

DESIGN: one pinned connection per held lock
    ✅ Advisory locks are session-scoped — the lock is tied to the connection.
       Releasing the connection releases the lock.
    ✅ Allows concurrent locks on different keys from the same process.
    ❌ Each held lock consumes one DB connection.  Avoid holding many locks
       simultaneously; release promptly.

DESIGN: in-process dict for token → connection tracking
    ✅ Token check matches the ``AbstractDistributedLock`` contract — the
       original holder cannot release a lock another process has re-acquired.
    ❌ State is process-local — tokens are not replicated across replicas.
       This is correct: each process holds its own advisory lock via its own
       session; the token just prevents double-release within one process.

Thread safety:  ⚠️ asyncio.Lock used for the token dict.  Not safe across OS threads.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 📐 https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS
  pg_try_advisory_lock / pg_advisory_unlock reference
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from varco_core.lock import AbstractDistributedLock, LockHandle

_logger = logging.getLogger(__name__)

# Maximum value of a signed int64 (PostgreSQL bigint).
_MAX_INT64 = 0x7FFFFFFFFFFFFFFF


def _key_to_int64(key: str) -> int:
    """
    Hash a string lock key to a signed int64 for PostgreSQL advisory locking.

    Uses the first 8 bytes of the MD5 digest, interpreted as a big-endian
    unsigned integer, then masked to 63 bits to stay within signed int64 range.

    Args:
        key: The string lock key (e.g. ``"inventory:item_42"``).

    Returns:
        A signed int64 in the range [0, 2^63 - 1].

    Edge cases:
        - Two keys may hash to the same int64 (collision probability ~2^-63).
          This is negligible for typical key spaces.
        - Empty string produces a valid hash — callers should validate keys
          before passing to ``try_acquire()`` (validated by the base class).
    """
    digest = hashlib.md5(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") & _MAX_INT64


# ── SAAdvisoryLock ────────────────────────────────────────────────────────────


class SAAdvisoryLock(AbstractDistributedLock):
    """
    PostgreSQL advisory lock implementation of ``AbstractDistributedLock``.

    Each call to ``try_acquire()`` borrows a connection from the engine's pool
    and holds it for the duration of the lock.  The connection is returned to
    the pool when ``LockHandle.release()`` is called (or the context manager
    exits).

    Thread safety:  ⚠️ asyncio.Lock guards the internal token dict — safe for
                        concurrent coroutines; not safe across OS threads.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        engine: ``AsyncEngine`` — connection pool used to borrow connections.

    Edge cases:
        - ``ttl`` is accepted by ``try_acquire`` but NOT enforced at the
          database level.  PostgreSQL session-level advisory locks last until
          the connection is closed.  Use the ``timeout`` parameter of
          ``acquire()`` to bound the waiting time, and keep critical sections
          short.
        - Calling ``try_acquire`` with the same key from the same process
          will succeed (PostgreSQL advisory locks are NOT re-entrant at the
          session level — a second lock on the same session and key is counted
          separately and requires a matching unlock).  The ``InMemoryLock``
          pattern avoids re-entrant use; so does keeping lock scopes narrow.
        - On connection failure during acquire, the connection is closed and
          ``None`` is returned (as if the lock were contended).
        - This implementation is NOT compatible with SQLite (advisory lock
          functions are PostgreSQL-specific).  Use ``InMemoryLock`` for tests
          that do not need cross-process coordination.

    Example::

        engine = create_async_engine("postgresql+asyncpg://...")
        lock = SAAdvisoryLock(engine)

        handle = await lock.try_acquire("inventory:item_42", ttl=30)
        if handle is not None:
            async with handle:
                await reserve_item(42)
        else:
            # Lock contended — skip or retry
            ...
    """

    def __init__(self, engine: AsyncEngine) -> None:
        """
        Args:
            engine: Async SQLAlchemy engine used to borrow connections.
        """
        self._engine = engine
        # Maps token UUID → (key_int64, pinned_connection).
        self._held: dict[UUID, tuple[int, AsyncConnection]] = {}
        # Protects mutations to _held across concurrent coroutines.
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        """Return the dict guard lock, creating it lazily inside the event loop."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def try_acquire(
        self,
        key: str,
        *,
        ttl: float,
    ) -> LockHandle | None:
        """
        Attempt to acquire the PostgreSQL advisory lock for ``key``.

        Executes ``SELECT pg_try_advisory_lock(:key_int)`` on a borrowed
        connection.  Returns a ``LockHandle`` if the lock is free, or ``None``
        if it is already held by another session.

        The ``ttl`` parameter is accepted for API compatibility but is NOT
        enforced at the database level.  Session-level advisory locks persist
        until the connection is explicitly closed or the PostgreSQL session ends.

        Args:
            key: The string lock key — hashed to int64 internally.
            ttl: Accepted for interface compatibility; not enforced by PostgreSQL
                 advisory locks.  The lock is held until ``release()`` is called.

        Returns:
            A ``LockHandle`` if the lock was acquired; ``None`` if contended.

        Raises:
            ValueError: If ``key`` is empty.

        Edge cases:
            - On database connection failure, ``None`` is returned — the caller
              treats it the same as contention.
            - Each successful ``try_acquire`` pins one connection from the pool.
              Release promptly to avoid pool exhaustion.

        Async safety: ✅ Acquires asyncio.Lock before mutating ``_held``.
        """
        if not key:
            raise ValueError("Lock key must be a non-empty string.")

        key_int = _key_to_int64(key)
        conn: AsyncConnection | None = None

        try:
            conn = await self._engine.connect()
            result = await conn.execute(
                sa.text("SELECT pg_try_advisory_lock(:key)").bindparams(key=key_int)
            )
            acquired: bool = result.scalar()
        except Exception as exc:
            _logger.warning(
                "SAAdvisoryLock.try_acquire: connection error for key=%r: %s",
                key,
                exc,
            )
            if conn is not None:
                await conn.close()
            return None

        if not acquired:
            # Lock is held by another session — return connection to pool.
            await conn.close()
            _logger.debug(
                "SAAdvisoryLock.try_acquire: contended for key=%r (int=%d)",
                key,
                key_int,
            )
            return None

        token = uuid4()
        async with self._get_lock():
            self._held[token] = (key_int, conn)

        _logger.debug(
            "SAAdvisoryLock.try_acquire: acquired key=%r (int=%d, token=%s)",
            key,
            key_int,
            token,
        )
        return LockHandle(key=key, token=token, lock=self)

    async def release(self, key: str, token: UUID) -> None:
        """
        Release the advisory lock identified by ``token``.

        Executes ``SELECT pg_advisory_unlock(:key_int)`` on the pinned
        connection, then closes the connection (returning it to the pool).

        Token mismatch (e.g. double-release or stale handle) → silent no-op.

        Args:
            key:   The original string lock key (used for logging only;
                   the stored int is looked up via ``token``).
            token: The token issued by ``try_acquire`` — must match to release.

        Edge cases:
            - Token not found → silent no-op (already released or never acquired
              by this process).
            - ``pg_advisory_unlock`` returns ``false`` if the lock was not held
              by this session (e.g. the connection was recycled) — logged as
              a warning but not raised.
            - The connection is always closed in the ``finally`` block, even if
              unlock raises.

        Async safety: ✅ Acquires asyncio.Lock before mutating ``_held``.
        """
        async with self._get_lock():
            entry = self._held.pop(token, None)

        if entry is None:
            # Token not in our dict — already released or from another process.
            _logger.debug(
                "SAAdvisoryLock.release: token %s not found (already released?)",
                token,
            )
            return

        key_int, conn = entry

        try:
            result = await conn.execute(
                sa.text("SELECT pg_advisory_unlock(:key)").bindparams(key=key_int)
            )
            unlocked: bool = result.scalar()
            if not unlocked:
                _logger.warning(
                    "SAAdvisoryLock.release: pg_advisory_unlock returned false "
                    "for key=%r (int=%d, token=%s) — session may have been recycled.",
                    key,
                    key_int,
                    token,
                )
        except Exception as exc:
            _logger.error(
                "SAAdvisoryLock.release: error unlocking key=%r: %s", key, exc
            )
        finally:
            try:
                await conn.close()
            except Exception:
                pass

        _logger.debug(
            "SAAdvisoryLock.release: released key=%r (int=%d, token=%s)",
            key,
            key_int,
            token,
        )

    def __repr__(self) -> str:
        held = len(self._held)
        return f"SAAdvisoryLock(engine={self._engine!r}, held={held})"


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SAAdvisoryLock",
]
