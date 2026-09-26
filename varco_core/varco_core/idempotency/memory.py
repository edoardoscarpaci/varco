"""
varco_core.idempotency.memory
==============================
``InMemoryIdempotencyStore`` — the default, single-process
``AbstractIdempotencyStore`` implementation (Plan 029 / D1a, Step 3).

⚠️ **Single-process only.** Exactly the same warning
``InMemoryRateLimiter`` already carries: behind a load balancer with more
than one process/pod, each process sees its own reservations, so a retry
routed to a different instance would not observe the first instance's
in-flight reservation or completed record. Use ``RedisIdempotencyStore``,
``SAIdempotencyStore``, or ``BeanieIdempotencyStore`` for any
multi-process deployment. This store exists for local development, tests,
and single-process deployments where it is genuinely correct.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field

from providify import Fallback, Singleton

from varco_core.idempotency.base import AbstractIdempotencyStore, ReserveOutcome
from varco_core.idempotency.record import IdempotencyRecord


@dataclass
class _Entry:
    """Internal bookkeeping for one reserved/completed key. Not part of the
    public API — never returned to a caller."""

    fingerprint: str
    expires_at: float
    record: IdempotencyRecord | None = field(default=None)


@Fallback
@Singleton(qualifier="in_memory")
class InMemoryIdempotencyStore(AbstractIdempotencyStore):
    """
    Single-process ``AbstractIdempotencyStore`` backed by a plain ``dict``.

    Plan 047 §D-vocab — the ``idempotency`` seam's in-process default,
    now a scanned ``@Fallback @Singleton(qualifier="in_memory")`` (it
    previously carried no DI binding at all — the middleware took
    ``store=`` explicitly). Same escape hatch as every other in-process
    default: ``get(AbstractIdempotencyStore, qualifier="in_memory")``
    always resolves this class, even when a backend is selected via
    ``VARCO_IDEMPOTENCY_BACKEND`` or a code binding exists.

    Atomicity (§D-D1-atomic) is provided by a **lazily-created**
    ``asyncio.Lock`` guarding the whole dict — CLAUDE.md's rule that a lock
    must never be constructed at module scope or in ``__init__`` (it must be
    created inside a running event loop) applies here exactly as everywhere
    else in this codebase.

    DESIGN: one process-wide lock over a per-key lock
        ✅ Simple, and every operation here is O(1) dict access with no I/O
           — the critical section is microseconds long regardless of how
           many distinct keys are in flight, so a per-key lock would add
           complexity (a lock registry, its own cleanup) for no measurable
           benefit.
        ❌ A store handling many distinct concurrent keys serializes all of
           them through one lock. Acceptable: this store is explicitly for
           single-process/dev/test use, not high-throughput production.

    Thread safety:  ❌ Not thread-safe across OS threads — use from a single
                    event loop, same as every other in-memory varco_core
                    primitive.
    Async safety:   ✅ Coroutine-safe within one event loop via the lazily-
                    created lock.
    """

    def __init__(self) -> None:
        self._entries: dict[str, _Entry] = {}
        # DESIGN: created lazily in _get_lock(), never here — asyncio.Lock()
        # requires a running event loop to bind correctly (CLAUDE.md).
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        """Lazily create the guarding lock on first use inside a running loop."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _is_expired(self, entry: _Entry, *, now: float) -> bool:
        return entry.expires_at <= now

    async def reserve(self, key: str, fingerprint: str, *, ttl: float) -> ReserveOutcome:
        """See ``AbstractIdempotencyStore.reserve()``."""
        if ttl <= 0:
            raise ValueError(f"reserve() ttl must be > 0, got {ttl!r}.")

        async with self._get_lock():
            now = time.monotonic()
            existing = self._entries.get(key)

            if existing is not None and self._is_expired(existing, now=now):
                # TTL elapsed — treat as if it never existed.
                del self._entries[key]
                existing = None

            if existing is None:
                self._entries[key] = _Entry(fingerprint=fingerprint, expires_at=now + ttl)
                return ReserveOutcome.ACQUIRED

            if existing.record is not None:
                return ReserveOutcome.REPLAY

            return ReserveOutcome.IN_FLIGHT

    async def complete(self, key: str, record: IdempotencyRecord) -> None:
        """See ``AbstractIdempotencyStore.complete()``."""
        async with self._get_lock():
            entry = self._entries.get(key)
            if entry is None:
                # No prior reserve() — accept it anyway (implementation-
                # defined per the ABC docstring); the caller always reserves
                # first in practice.
                entry = _Entry(fingerprint=record.fingerprint, expires_at=time.monotonic())
                self._entries[key] = entry
            entry.record = record

    async def get(self, key: str) -> IdempotencyRecord | None:
        """See ``AbstractIdempotencyStore.get()``."""
        async with self._get_lock():
            entry = self._entries.get(key)
            if entry is None or self._is_expired(entry, now=time.monotonic()):
                return None
            return entry.record

    async def release(self, key: str) -> None:
        """See ``AbstractIdempotencyStore.release()``."""
        async with self._get_lock():
            self._entries.pop(key, None)

    async def delete_expired(self) -> int:
        """See ``AbstractIdempotencyStore.delete_expired()``."""
        async with self._get_lock():
            now = time.monotonic()
            expired = [k for k, entry in self._entries.items() if self._is_expired(entry, now=now)]
            for k in expired:
                del self._entries[k]
            return len(expired)


__all__ = ["InMemoryIdempotencyStore"]
