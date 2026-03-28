"""
varco_core.lock
===============
Distributed locking abstractions for cross-process coordination.

Problem
-------
``asyncio.Lock`` is process-local — multiple replicas of the same service can
race on shared resources (e.g. inventory reservation, idempotency slot
allocation, leader election).  Without cross-process coordination, exclusive
operations execute concurrently on different replicas, corrupting shared state.

Solution
--------
``AbstractDistributedLock`` defines a two-shape API::

    try_acquire(key, ttl) → bool        — non-blocking; False if already held
    acquire(key, ttl, timeout)          — blocking; raises LockNotAcquiredError

Each acquire call returns a ``LockHandle`` that the caller uses to release.
The handle carries an opaque token that the backend checks before releasing —
preventing a holder whose TTL expired from accidentally releasing a new holder's
lock (the "phantom release" bug).

DESIGN: separate try_acquire / acquire shapes over a single acquire(blocking=)
    ✅ The two shapes have different return types — a boolean vs None.
       A single acquire(blocking=bool) would need Union return types or
       overloads, making the API harder to typecheck and use.
    ✅ try_acquire is useful in "skip if busy" patterns where the caller simply
       skips work if the lock is contended — no exception needed.
    ✅ acquire is useful in "must proceed exclusively" patterns where giving up
       is not acceptable — raising LockNotAcquiredError on timeout is cleaner
       than returning False and leaving the caller to decide what to do.
    ❌ Two methods instead of one — minimal API surface increase.

DESIGN: LockHandle over returning a raw token
    ✅ LockHandle is a context manager — ``async with handle:`` auto-releases.
    ✅ LockHandle carries the key alongside the token — backend doesn't need
       a separate argument on release.
    ✅ LockHandle is typed — callers can't accidentally pass the wrong token to
       the wrong lock backend.
    ❌ Allocates one extra object per acquire — negligible overhead.

Components
----------
``LockNotAcquiredError``
    Raised by ``acquire()`` when the lock is not acquired within ``timeout``.

``LockHandle``
    Returned by ``try_acquire()`` and ``acquire()``.  Context manager that
    calls ``release()`` on exit.

``AbstractDistributedLock``
    ABC — two abstract methods: ``try_acquire`` and ``release``.

``InMemoryLock``
    asyncio-based implementation backed by a dict of asyncio.Lock per key.
    For single-process use and tests only.

Usage::

    lock = InMemoryLock()

    handle = await lock.acquire("inventory:item_42", ttl=30)
    try:
        await reserve_item(item_id=42)
    finally:
        await handle.release()

    # Or with context manager:
    async with await lock.acquire("inventory:item_42", ttl=30):
        await reserve_item(item_id=42)

Thread safety:  ⚠️ InMemoryLock uses asyncio.Lock (lazy-created per key).
                    Not safe for multi-process setups — use RedisLock.
Async safety:   ✅ All abstract methods are ``async def``.

📚 Docs
- 🐍 https://docs.python.org/3/library/asyncio-sync.html
  asyncio.Lock — coroutine-safe primitive used by InMemoryLock.
- 📐 https://redis.io/docs/manual/patterns/distributed-locks/
  Redlock algorithm — original reference for distributed lock design.
- 📐 https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html
  Martin Kleppmann's analysis of distributed locking correctness constraints.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any
from uuid import UUID, uuid4

_logger = logging.getLogger(__name__)


# ── LockNotAcquiredError ──────────────────────────────────────────────────────


class LockNotAcquiredError(Exception):
    """
    Raised by ``AbstractDistributedLock.acquire()`` when the lock cannot be
    obtained within the caller-specified timeout.

    Args:
        key:     The lock key that was not acquired.
        timeout: The timeout (in seconds) that elapsed before giving up.

    Example::

        try:
            handle = await lock.acquire("order:42", ttl=10, timeout=5.0)
        except LockNotAcquiredError as exc:
            logger.warning("Could not acquire lock for key=%r", exc.key)
    """

    def __init__(self, key: str, timeout: float) -> None:
        self.key = key
        self.timeout = timeout
        super().__init__(
            f"Could not acquire distributed lock for key={key!r} "
            f"within timeout={timeout}s. "
            f"Another process may be holding the lock — retry later or "
            f"increase the timeout."
        )


# ── LockHandle ────────────────────────────────────────────────────────────────


class LockHandle:
    """
    Represents a successfully acquired distributed lock.

    Returned by ``AbstractDistributedLock.try_acquire()`` and ``acquire()``.
    The handle carries the opaque token that the backend checks before
    releasing — preventing a holder whose TTL expired from releasing a new
    holder's lock (the "phantom release" bug).

    DESIGN: LockHandle as a context manager
        ✅ ``async with handle:`` guarantees release even on exception.
        ✅ Carries both key and token — backend doesn't need separate args.
        ✅ Explicitly typed — callers cannot accidentally pass the wrong token.
        ❌ Allocates one extra object per acquire — negligible.

    Thread safety:  ⚠️ Not thread-safe — use from a single asyncio Task.
    Async safety:   ✅ ``release()`` is ``async def``.

    Args:
        key:    The lock key this handle corresponds to.
        token:  Opaque UUID issued by the backend at acquire time.
        lock:   Reference to the parent ``AbstractDistributedLock`` for release.

    Edge cases:
        - Calling ``release()`` more than once is safe — the second call is a
          no-op (the backend checks the token, which is already gone).
        - The handle does NOT auto-extend the TTL — if the critical section
          takes longer than ``ttl``, another process may acquire the same lock.
          Use a shorter critical section or extend TTL explicitly via the backend.
    """

    __slots__ = ("key", "token", "_lock", "_released")

    def __init__(
        self,
        key: str,
        token: UUID,
        lock: AbstractDistributedLock,
    ) -> None:
        self.key = key
        self.token = token
        self._lock = lock
        # Track whether release has already been called — idempotent release.
        self._released = False

    async def release(self) -> None:
        """
        Release the distributed lock represented by this handle.

        Idempotent — calling more than once is safe.

        Async safety:   ✅ Delegates to the parent lock's ``release()`` method.

        Edge cases:
            - If ``release()`` is called after the TTL has expired and another
              process has acquired the lock, the backend's token check ensures
              this call is a no-op (the token no longer matches).
        """
        if self._released:
            return
        self._released = True
        await self._lock.release(self.key, self.token)

    async def __aenter__(self) -> LockHandle:
        """Return self — the handle is already acquired."""
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Release the lock on context exit — runs even on exceptions."""
        await self.release()

    def __repr__(self) -> str:
        return (
            f"LockHandle("
            f"key={self.key!r}, "
            f"token={self.token}, "
            f"released={self._released})"
        )


# ── AbstractDistributedLock ───────────────────────────────────────────────────


class AbstractDistributedLock(ABC):
    """
    Abstract interface for cross-process distributed locking.

    Implementations must guarantee that at most one ``LockHandle`` is live for
    a given key at any point in time across all processes that share the
    backend.

    DESIGN: two-shape API (try_acquire returning bool, acquire raising on timeout)
        ✅ try_acquire is used in "skip if busy" patterns — no exception needed.
        ✅ acquire is used in "must proceed exclusively" patterns — raising on
           timeout is cleaner than returning False and leaving the caller to
           decide.
        ❌ Two methods — minor API surface increase vs a single method with a
           keyword argument.

    Thread safety:  ⚠️ Subclass-defined.
    Async safety:   ✅ All methods are ``async def``.
    """

    @abstractmethod
    async def try_acquire(
        self,
        key: str,
        *,
        ttl: float,
    ) -> LockHandle | None:
        """
        Attempt to acquire the lock for ``key`` without blocking.

        Returns immediately with a ``LockHandle`` if the lock is free, or
        ``None`` if it is currently held by another process.

        Args:
            key: The lock key (e.g. ``"inventory:item_42"``).
                 Must be a non-empty string.
            ttl: Lock time-to-live in **seconds**.  The lock is automatically
                 released by the backend after this duration even if ``release()``
                 is never called — prevents a crashed holder from holding the
                 lock forever.  Must be positive.

        Returns:
            A ``LockHandle`` if the lock was acquired; ``None`` if contended.

        Raises:
            ValueError: If ``key`` is empty or ``ttl`` is not positive.

        Edge cases:
            - A ``None`` return does NOT raise — the caller decides how to
              handle contention (retry, skip, or raise their own error).
            - If ``ttl`` expires before ``release()`` is called, another process
              may acquire the lock.  The handle's token check prevents a phantom
              release from the original holder.
            - Passing the same ``key`` from the same process is NOT re-entrant
              by default — ``try_acquire`` returns ``None`` if the current
              process already holds the lock (no nested locking).

        Async safety:   ✅ Must be ``async def``.
        """

    @abstractmethod
    async def release(self, key: str, token: UUID) -> None:
        """
        Release the lock for ``key`` if and only if the stored token matches.

        The token check is the critical correctness guarantee: if the TTL has
        expired and another process acquired the lock (with a new token), this
        call is a no-op rather than accidentally releasing the new holder.

        Called by ``LockHandle.release()`` — do not call directly.

        Args:
            key:   The lock key to release.
            token: The token issued when the lock was acquired.  Release is
                   only performed if the backend's stored token equals this value.

        Edge cases:
            - Token mismatch (TTL expired, new holder) → silent no-op.
            - Key does not exist (already expired) → silent no-op.
            - ``release()`` must NOT raise — a holder that crashes during
              release should not block the caller's exception handler.

        Async safety:   ✅ Must be ``async def``.
        """

    async def acquire(
        self,
        key: str,
        *,
        ttl: float,
        timeout: float = 10.0,
        retry_interval: float = 0.05,
    ) -> LockHandle:
        """
        Blocking acquire — polls ``try_acquire`` until the lock is free or
        ``timeout`` is exceeded.

        Args:
            key:            The lock key.
            ttl:            Lock TTL in seconds — passed to each ``try_acquire``.
            timeout:        Maximum wait time in seconds before raising.
                            Default: 10 s.
            retry_interval: Sleep between retries in seconds.  Default: 50 ms.
                            Keep small to reduce lock contention latency, but
                            not so small that it spins the event loop.

        Returns:
            A ``LockHandle`` when the lock is acquired.

        Raises:
            LockNotAcquiredError: If the lock was not acquired within ``timeout``.
            ValueError:           If ``key`` is empty or ``ttl``/``timeout`` are
                                  not positive.

        DESIGN: poll-based acquire over a condition variable
            ✅ Works across processes — a cross-process condition variable would
               require a Pub/Sub subscription (e.g. Redis keyspace notifications),
               adding complexity and ops overhead.
            ✅ Simple to reason about — retry_interval caps wasted CPU.
            ❌ Under high contention, polling adds latency up to retry_interval.
               For most lock TTLs (seconds), 50 ms polling overhead is negligible.

        Edge cases:
            - If ``timeout=0``, the method behaves like a single ``try_acquire``
              but raises ``LockNotAcquiredError`` on contention instead of
              returning ``None``.
            - Concurrent calls with the same key from the same process both poll
              independently — no internal coordination between concurrent callers.

        Async safety:   ✅ Uses ``asyncio.sleep`` — yields control between polls.
        """
        if not key:
            raise ValueError("Lock key must be a non-empty string.")
        if ttl <= 0:
            raise ValueError(f"Lock TTL must be positive; got ttl={ttl}.")
        if timeout < 0:
            raise ValueError(
                f"Lock timeout must be non-negative; got timeout={timeout}."
            )

        # Track wall-clock time via asyncio's event-loop clock to avoid
        # issues with system clock adjustments during long waits.
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while True:
            handle = await self.try_acquire(key, ttl=ttl)
            if handle is not None:
                return handle

            remaining = deadline - loop.time()
            if remaining <= 0:
                raise LockNotAcquiredError(key, timeout)

            # Sleep for retry_interval but cap at remaining time so we don't
            # overshoot the deadline by a full interval.
            await asyncio.sleep(min(retry_interval, remaining))


# ── InMemoryLock ──────────────────────────────────────────────────────────────


class InMemoryLock(AbstractDistributedLock):
    """
    In-process distributed lock backed by a dict of ``asyncio.Lock`` per key.

    Suitable for tests and single-process deployments.  State is lost when the
    process exits.  Has NO cross-process coordination — use ``RedisLock`` in
    multi-replica deployments.

    DESIGN: asyncio.Lock per key over a single global Lock with a waiting dict
        ✅ Independent keys never block each other — fine-grained concurrency.
        ✅ No busy-waiting for keys with different names.
        ✅ Simple implementation — one asyncio.Lock per active key.
        ❌ Leaked handles (process crashes without releasing) hold the asyncio.Lock
           forever — the per-key dict grows unboundedly.  Acceptable for tests;
           real-world use should use RedisLock with TTL.

    DESIGN: asyncio.Lock per key created lazily
        ✅ Locks created inside the running event loop — no RuntimeError.
        ✅ Keys with no current holder have no Lock object allocated.
        ❌ Lock dict must be protected by a global asyncio.Lock to avoid a race
           where two concurrent try_acquire calls for the same new key both create
           separate Lock objects and both succeed.

    Thread safety:  ⚠️ asyncio.Lock — safe for concurrent coroutines in the
                        same event loop.  Not safe across OS threads.
    Async safety:   ✅ All methods acquire the global dict lock before mutating
                       the key → (asyncio.Lock, token) dict.

    Edge cases:
        - TTL is NOT enforced by InMemoryLock — a held lock is only released
          when ``release()`` is called.  In tests this is fine; for production
          use RedisLock (TTL is enforced server-side).
        - Re-entrant acquire from the same coroutine will block — asyncio.Lock
          is not re-entrant.  Wrap re-entrant critical sections in a single
          outer acquire.

    Example::

        lock = InMemoryLock()
        async with await lock.acquire("order:42", ttl=30):
            await process_order(42)
    """

    def __init__(self) -> None:
        # Maps key → (asyncio.Lock, current_token | None).
        # None token means no holder currently (lock is free but cached).
        self._locks: dict[str, tuple[asyncio.Lock, UUID | None]] = {}
        # Global dict guard — prevents concurrent key-creation races.
        # Lazy to avoid creating the Lock before the event loop starts.
        self._dict_lock: asyncio.Lock | None = None

    def _get_dict_lock(self) -> asyncio.Lock:
        """
        Return the global dict guard lock, creating it lazily on first call.

        DESIGN: lazy dict lock
            asyncio.Lock() must be created inside a running event loop.
            Creating at __init__ raises RuntimeError if no loop is running yet.

        Returns:
            The shared asyncio.Lock protecting ``self._locks`` mutations.
        """
        if self._dict_lock is None:
            self._dict_lock = asyncio.Lock()
        return self._dict_lock

    async def try_acquire(
        self,
        key: str,
        *,
        ttl: float,
    ) -> LockHandle | None:
        """
        Attempt to acquire the in-memory lock for ``key`` without blocking.

        Note: ``ttl`` is accepted but NOT enforced by InMemoryLock — there is
        no background timer.  A held lock is only released on ``release()``.
        For TTL enforcement use ``RedisLock``.

        Args:
            key: Lock key.
            ttl: Accepted for API compatibility; not enforced.

        Returns:
            A ``LockHandle`` if the lock is free; ``None`` if held.

        Thread safety:  ⚠️ asyncio.Lock — safe for concurrent coroutines.
        Async safety:   ✅ Acquires dict lock, then per-key lock (non-blocking).

        Edge cases:
            - If the same coroutine holds the lock and calls try_acquire again,
              the per-key asyncio.Lock is NOT re-entrant — returns None.
        """
        if not key:
            raise ValueError("Lock key must be a non-empty string.")
        if ttl <= 0:
            raise ValueError(f"Lock TTL must be positive; got ttl={ttl}.")

        async with self._get_dict_lock():
            if key not in self._locks:
                # First acquirer for this key — create the per-key lock.
                self._locks[key] = (asyncio.Lock(), None)

            per_key_lock, current_token = self._locks[key]

            # Try to acquire the per-key lock without blocking.
            # asyncio.Lock.acquire() with immediate cancellation emulates try.
            acquired = per_key_lock.locked() is False and await per_key_lock.acquire()
            if not acquired:
                return None

            # Lock is now held — assign a fresh token for this acquisition.
            token = uuid4()
            self._locks[key] = (per_key_lock, token)

        return LockHandle(key=key, token=token, lock=self)

    async def release(self, key: str, token: UUID) -> None:
        """
        Release the in-memory lock for ``key`` if the stored token matches.

        Does NOT raise — logs a warning on token mismatch and returns.

        Args:
            key:   The lock key to release.
            token: The token from the ``LockHandle`` — must match to release.

        Edge cases:
            - Token mismatch → warning log, no-op (prevents phantom release).
            - Key not in dict → no-op (already released or never acquired).
            - Releasing an unlocked lock → no-op (asyncio.Lock.release()
              raises RuntimeError — we guard against this with the token check).

        Thread safety:  ⚠️ asyncio.Lock — safe for concurrent coroutines.
        Async safety:   ✅ Acquires dict lock before mutating state.
        """
        async with self._get_dict_lock():
            if key not in self._locks:
                # Key was never acquired — silent no-op.
                return

            per_key_lock, current_token = self._locks[key]

            if current_token != token:
                # Token mismatch — either TTL expired and another holder acquired
                # the lock, or this is a duplicate release.  Do NOT release.
                _logger.warning(
                    "InMemoryLock.release: token mismatch for key=%r "
                    "(expected %s, got %s) — ignoring release.",
                    key,
                    current_token,
                    token,
                )
                return

            # Clear the token and release the asyncio.Lock.
            self._locks[key] = (per_key_lock, None)
            per_key_lock.release()

    def __repr__(self) -> str:
        held = sum(1 for _, (lock, token) in self._locks.items() if token is not None)
        return f"InMemoryLock(keys={len(self._locks)}, held={held})"


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "LockNotAcquiredError",
    "LockHandle",
    "AbstractDistributedLock",
    "InMemoryLock",
]
