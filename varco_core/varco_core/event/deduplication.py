"""
varco_core.event.deduplication
==============================
Message deduplication for the varco event system.

Problem
-------
When a broker restarts or a consumer crashes, messages may be re-delivered
to handlers that already processed them.  Without deduplication, non-idempotent
handlers silently corrupt data (e.g. charging a card twice, sending an email
twice).

Solution
--------
``AbstractDeduplicator`` defines a two-step API::

    is_duplicate(event_id)  →  True if already seen
    mark_seen(event_id)     →  record as processed

The deduplicator is wired into the event consumer at handler registration time
via ``@listen(..., deduplicator=my_deduplicator)``.  The retry wrapper checks
``is_duplicate`` BEFORE calling the handler, and calls ``mark_seen`` AFTER
successful handler completion.

DESIGN: separate is_duplicate / mark_seen over a single atomic check-and-mark
    ✅ mark_seen fires AFTER handler success — a crashed handler (exception
       raised) leaves the event un-seen so it can be retried or re-delivered.
    ✅ Implementations can make the pair atomic (e.g. Lua script in Redis)
       without changing the API contract.
    ✅ Callers can decide when to mark_seen — useful for inbox-style patterns
       where the mark happens after DB commit.
    ❌ Non-atomic (in InMemory): a crash between is_duplicate=False and
       mark_seen leaves the event un-deduplicated on re-delivery.  Acceptable
       for tests; real-world deployments use RedisDeduplicator.

Components
----------
``AbstractDeduplicator``
    ABC — two abstract methods: ``is_duplicate`` and ``mark_seen``.

``InMemoryDeduplicator``
    Set-based implementation backed by an insertion-ordered dict.  Optional
    ``max_size`` eviction (FIFO — removes the oldest entry) prevents unbounded
    memory growth.  For tests only — state is lost on process restart.

Wire via @listen::

    dedup = InMemoryDeduplicator()

    class OrderConsumer(EventConsumer):
        @listen(OrderPlacedEvent, channel="orders", deduplicator=dedup)
        async def on_order(self, event: OrderPlacedEvent) -> None:
            await self._process(event)  # called at most once per event_id

Thread safety:  ⚠️ InMemoryDeduplicator uses asyncio.Lock (lazy-created).
                    Not safe for multi-process setups — use RedisDeduplicator.
Async safety:   ✅ All abstract methods are ``async def``.

📚 Docs
- 🐍 https://docs.python.org/3/library/asyncio-sync.html
  asyncio.Lock — coroutine-safe lock used for InMemoryDeduplicator.
- 📐 https://microservices.io/patterns/communication-style/idempotent-consumer.html
  Idempotent Consumer pattern — original pattern reference.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from abc import ABC, abstractmethod
from uuid import UUID

from providify import Singleton

_logger = logging.getLogger(__name__)


# ── AbstractDeduplicator ──────────────────────────────────────────────────────


class AbstractDeduplicator(ABC):
    """
    Abstract interface for message deduplication.

    Implementations track which ``event_id`` values have been processed and
    answer whether a given event is a duplicate re-delivery.

    DESIGN: two-step API (is_duplicate + mark_seen) over single atomic op
        ✅ mark_seen is called AFTER handler success — a crashed handler leaves
           the event un-seen so the retry path can re-process it.
        ✅ Implementations are free to make the two steps atomic internally
           (e.g. a Redis Lua script that checks NX and sets atomically).
        ❌ A crash between is_duplicate=False and mark_seen leaves a window
           where the same event can be processed twice on re-delivery.
           For true exactly-once semantics, use an idempotency key in the
           domain logic, not just the deduplicator.

    Thread safety:  ⚠️ Subclass-defined.
    Async safety:   ✅ All methods are ``async def``.
    """

    @abstractmethod
    async def is_duplicate(self, event_id: UUID) -> bool:
        """
        Return ``True`` if ``event_id`` has been seen and successfully processed.

        Called by the consumer retry wrapper BEFORE invoking the handler.  If
        this returns ``True``, the handler is skipped and the event is silently
        acknowledged — no retry, no DLQ.

        Args:
            event_id: The ``Event.event_id`` UUID to check.

        Returns:
            ``True`` if the event was previously processed; ``False`` if it is
            new or was never successfully completed.

        Edge cases:
            - A ``True`` result from ``is_duplicate`` causes the handler to be
              skipped entirely — no exception is raised, no DLQ entry created.
            - A handler that failed (raised) on a previous attempt is NOT
              considered seen — ``mark_seen`` is only called on success.
            - An implementation may return ``False`` on transient errors (e.g.
              Redis timeout) rather than raising — "process the event anyway"
              is safer than silently dropping it.
        """

    @abstractmethod
    async def mark_seen(self, event_id: UUID) -> None:
        """
        Record that ``event_id`` has been successfully processed.

        Called by the consumer retry wrapper AFTER the handler returns without
        raising.  Must NOT raise — if the underlying store is unavailable the
        exception must be logged and swallowed.  Raising here would cause the
        caller to see an exception from an otherwise-successful handler
        invocation.

        Args:
            event_id: The ``Event.event_id`` UUID to mark as seen.

        Edge cases:
            - Calling ``mark_seen`` with an already-seen ``event_id`` is
              idempotent — no error, no state change.
            - If the store is unavailable (e.g. Redis down), implementations
              must log a warning and return normally.  This means the event may
              be re-processed on re-delivery — "at-least-once" semantics.

        Async safety:   ✅ Must be ``async def``.
        """


# ── InMemoryDeduplicator ──────────────────────────────────────────────────────


@Singleton(priority=-sys.maxsize - 1, qualifier="in_memory")
class InMemoryDeduplicator(AbstractDeduplicator):
    """
    In-memory deduplication backed by an insertion-ordered dict.

    Suitable for tests and single-process deployments where durability across
    restarts is not required.  State is lost when the process exits.

    Eviction
    --------
    When the internal dict reaches ``max_size`` entries, the oldest-inserted
    entry is evicted before adding the new one.  This bounds memory usage at
    the cost of potentially re-processing very old events if the deduplication
    window is shorter than the message re-delivery window.

    DESIGN: insertion-ordered dict over set + deque
        ✅ ``dict`` (Python 3.7+) preserves insertion order — oldest key can
           be evicted via ``next(iter(self._seen))`` in O(1).
        ✅ Membership check ``event_id in self._seen`` is O(1) — same as set.
        ✅ Single data structure — no synchronization between set and deque.
        ❌ Slightly higher per-entry memory than a plain set (dict stores a
           None value) — negligible for ``max_size`` up to millions.

    DESIGN: asyncio.Lock created lazily (never at __init__ or module level)
        ✅ Locks must be created inside a running event loop — creating at
           __init__ raises RuntimeError if there is no loop yet.
        ✅ The lazy pattern (``self._lock = self._lock or asyncio.Lock()``)
           is the standard varco pattern for mutable async state.
        ❌ Lock is never None after first access — zero overhead thereafter.

    Thread safety:  ⚠️ asyncio.Lock protects concurrent coroutines but NOT
                        OS threads — do not share across threads.
    Async safety:   ✅ All methods acquire the lock before modifying state.

    Args:
        max_size: Maximum number of event IDs to track.  When full, the oldest
                  entry is evicted before adding the new one.
                  Default: 10 000.

    Edge cases:
        - Two concurrent ``mark_seen`` calls for the same ``event_id`` are safe
          — the lock ensures one completes before the other runs; the second
          call is a no-op (idempotent).
        - ``max_size=0`` is equivalent to a no-op deduplicator — every event is
          seen as a duplicate immediately after ``mark_seen`` evicts itself.
          Use ``max_size >= 1`` for meaningful deduplication.
        - Eviction removes the oldest-inserted ID — if a very old event is
          re-delivered after eviction it will be processed again.  Use a larger
          ``max_size`` or a durable backend (``RedisDeduplicator``) to prevent
          this.

    Example::

        dedup = InMemoryDeduplicator(max_size=50_000)

        class OrderConsumer(EventConsumer):
            @listen(OrderPlacedEvent, deduplicator=dedup)
            async def on_order(self, event: OrderPlacedEvent) -> None:
                await process(event)
    """

    def __init__(self, *, max_size: int = 10_000) -> None:
        """
        Args:
            max_size: Maximum number of event IDs to remember.
                      Must be a positive integer.
        """
        # dict[UUID, None] preserves insertion order — enables FIFO eviction.
        # Values are always None; only keys matter for membership tests.
        self._seen: dict[UUID, None] = {}
        self._max_size = max_size
        # Lazy lock — created on first access inside the running event loop.
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        """
        Return the asyncio.Lock, creating it lazily on first call.

        DESIGN: lazy lock creation
            asyncio.Lock() must be created inside a running event loop.
            Creating at __init__ time can raise RuntimeError if no loop exists.
            The lazy pattern defers creation to the first in-loop access.

        Returns:
            The shared asyncio.Lock for this instance.
        """
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def is_duplicate(self, event_id: UUID) -> bool:
        """
        Return ``True`` if ``event_id`` is present in the seen-set.

        Args:
            event_id: Event UUID to check.

        Returns:
            ``True`` if seen before; ``False`` if new.

        Thread safety:  ⚠️ asyncio.Lock — safe for concurrent coroutines.
        Async safety:   ✅ Lock acquired before dict read.
        """
        async with self._get_lock():
            return event_id in self._seen

    async def mark_seen(self, event_id: UUID) -> None:
        """
        Record ``event_id`` as processed.  Evicts oldest entry if at capacity.

        Does NOT raise — any internal error is logged and swallowed.

        Args:
            event_id: Event UUID to mark.

        Edge cases:
            - Already-seen event_id → idempotent no-op.
            - At capacity → evicts oldest entry, then inserts new one.

        Thread safety:  ⚠️ asyncio.Lock — safe for concurrent coroutines.
        Async safety:   ✅ Lock acquired for the full mutating section.
        """
        try:
            async with self._get_lock():
                if event_id in self._seen:
                    # Already marked — idempotent no-op.
                    return

                if len(self._seen) >= self._max_size:
                    # Evict the oldest-inserted entry (first key in dict).
                    # O(1) thanks to Python dict's insertion-order guarantee.
                    oldest = next(iter(self._seen))
                    del self._seen[oldest]
                    _logger.warning(
                        "InMemoryDeduplicator: max_size=%d reached, "
                        "evicted oldest event_id=%s.  "
                        "Consider increasing max_size or using RedisDeduplicator.",
                        self._max_size,
                        oldest,
                    )

                self._seen[event_id] = None

        except Exception as exc:
            # mark_seen contract: MUST NOT raise.  Log and swallow.
            _logger.error(
                "InMemoryDeduplicator.mark_seen failed for event_id=%s: %s",
                event_id,
                exc,
            )

    def __repr__(self) -> str:
        return (
            f"InMemoryDeduplicator("
            f"seen={len(self._seen)}, max_size={self._max_size})"
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "AbstractDeduplicator",
    "InMemoryDeduplicator",
]
