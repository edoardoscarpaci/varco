"""
varco_core.event.dlq
=====================
Dead Letter Queue (DLQ) abstractions for the varco event system.

A DLQ receives events that a handler could not process after exhausting all
retry attempts.  Instead of silently dropping the event or crashing the
consumer, the event is persisted in the DLQ for later inspection, replay, or
manual intervention.

Architecture
------------
``AbstractDeadLetterQueue`` is the interface that backend packages implement:

    varco_core       AbstractDeadLetterQueue   ← THIS MODULE
                           ↑ implemented by
    varco_kafka      KafkaDLQ    (publishes to a dedicated DLQ topic)
    varco_redis      RedisDLQ    (publishes to a dedicated DLQ channel/stream)
    (in-process)     InMemoryDeadLetterQueue   (for tests)

Responsibility split
--------------------
- ``AbstractDeadLetterQueue.push()`` — called by the event consumer when a
  handler exhausts all retry attempts.  Stores the failed event entry.
- ``pop_batch()`` / ``ack()`` — used by a *DLQ relay* process (similar to
  ``OutboxRelay``) that replays DLQ entries or forwards them to monitoring.
  Backend implementations may choose not to support replay (e.g. a Kafka
  DLQ topic is self-contained — consumers read from it directly).

Usage in consumer (via ``@listen`` with ``dlq=``)::

    class OrderConsumer(EventConsumer):
        @listen(
            OrderPlacedEvent,
            retry_policy=RetryPolicy(max_attempts=3),
            dlq=my_dlq,
        )
        async def on_order_placed(self, event: OrderPlacedEvent) -> None:
            await self._fulfillment_service.process(event)
            # If this raises after 3 attempts, event lands in my_dlq.

DESIGN: separate DLQ from the bus
    ✅ DLQ is a policy concern (per-handler), not an infrastructure concern
       (per-bus).  Different handlers on the same bus can route to different
       DLQ backends.
    ✅ ``AbstractDeadLetterQueue`` is independent of ``AbstractEventBus`` —
       no circular imports.
    ❌ Backend implementations (Kafka, Redis) must be instantiated and injected
       separately — slightly more wiring than a fully automatic approach.

Thread safety:  ⚠️ Implementations must document their own thread safety.
Async safety:   ✅ All interface methods are ``async def``.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import ClassVar, Final
from uuid import UUID, uuid4

from providify import Fallback, Singleton

from varco_core.event.base import Event

_logger = logging.getLogger(__name__)

# Maximum number of entries the InMemoryDeadLetterQueue holds before it starts
# dropping the oldest entries.  This prevents unbounded memory growth in tests
# or long-running in-process scenarios.
_IN_MEMORY_MAX_SIZE: Final[int] = 10_000


# ── DeadLetterSource ──────────────────────────────────────────────────────────


class DeadLetterSource(StrEnum):
    """
    Which varco subsystem produced a ``DeadLetterEntry`` (Plan 005 Phase 3,
    U-6). One DLQ concept serving all three producers — U-17 §4's own words —
    rather than a second, job-specific DLQ abstraction.
    """

    CONSUMER = "consumer"
    """An ``EventConsumer`` handler exhausted its ``retry_policy`` (existing
    behaviour, unchanged — this is simply the new explicit name for it)."""

    OUTBOX_RELAY = "outbox_relay"
    """``OutboxRelay`` could not deserialize or publish an ``OutboxEntry``."""

    JOB = "job"
    """A job exhausted its ``max_attempts`` in ``JobRunner`` (Phase 4)."""


# ── DeadLetterEntry ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DeadLetterEntry:
    """
    Immutable record of an event that failed all handler retry attempts.

    Carries both the original event (for replay) and metadata about the failure
    (for monitoring and debugging).

    DESIGN: frozen dataclass over Pydantic model
        ✅ No JSON serialization overhead for in-memory and test scenarios.
        ✅ Hashable — safe to use in sets for deduplication.
        ❌ Not directly JSON-serializable — backend implementations must convert
           to their own wire format (e.g. Kafka message, Redis hash).

    Thread safety:  ✅ Immutable — safe to share across threads and async tasks.

    Attributes:
        entry_id:       Unique identifier for this DLQ entry.  Generated
                        automatically.  Used by ``ack()`` and ``reject()``.
        event:          The original event that could not be processed.
        channel:        The channel the event was published to.
        handler_name:   Qualified name of the handler that failed
                        (e.g. ``"OrderConsumer.on_order_placed"``).
        error_type:     ``type(last_exception).__name__``.
        error_message:  ``str(last_exception)``.
        attempts:       Total number of call attempts made (including first).
        first_failed_at: UTC datetime of the first failure for this event.
        last_failed_at:  UTC datetime of the final failed attempt.
        source:          Which subsystem produced this entry (Plan 005 Phase 3).
                         Defaults to ``CONSUMER`` — every pre-Phase-3 call site
                         (positional or keyword, without ``source=``) keeps
                         working unchanged.
        source_ref:      Opaque string identifying the source record — an
                         outbox ``entry_id`` or a job id, as ``str``. ``None``
                         for consumer-sourced entries (the ``event`` itself
                         carries identity).
        payload:         Raw bytes when ``event`` could not be deserialized at
                         all (``OutboxRelay``'s failed-to-deserialize path) —
                         the whole reason ``event`` had to become optional.

    Edge cases:
        - ``first_failed_at`` and ``last_failed_at`` are equal when
          ``attempts=1`` (no retries configured).
        - ``error_type`` and ``error_message`` are strings — the original
          exception object is NOT stored (not picklable/serializable in general).
        - ``event=None`` is only expected when ``source != CONSUMER`` — a
          consumer-sourced entry always carries the original event.
    """

    entry_id: UUID = field(default_factory=uuid4)
    """Auto-generated unique ID for this DLQ entry."""

    event: Event | None = field(default=None)
    """Original event that could not be processed. ``None`` for a
    relay-sourced entry whose payload could not even be deserialized —
    see ``payload`` below."""

    channel: str = ""
    """Channel the event was published to."""

    handler_name: str = ""
    """Qualified name of the handler method that exhausted its retries."""

    error_type: str = ""
    """``type(last_exception).__name__`` — class name of the terminal error."""

    error_message: str = ""
    """``str(last_exception)`` — human-readable error description."""

    attempts: int = 0
    """Total number of call attempts (first call + retries)."""

    first_failed_at: datetime = field(
        # Timezone-aware UTC — avoids datetime.utcnow() deprecation
        default_factory=lambda: datetime.now(tz=UTC)
    )
    """UTC datetime of the first failed attempt."""

    last_failed_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    """UTC datetime of the most recent (final) failed attempt."""

    source: DeadLetterSource = DeadLetterSource.CONSUMER
    """Which subsystem produced this entry (Plan 005 Phase 3) — defaults to
    the pre-Phase-3 behaviour (a consumer handler's retry exhaustion)."""

    source_ref: str | None = None
    """Opaque source-record identifier (outbox ``entry_id`` / job id, as
    ``str``) — ``None`` for consumer-sourced entries."""

    payload: bytes | None = None
    """Raw bytes when ``event`` could not be deserialized — ``None`` unless
    a relay-sourced entry's payload was unparseable."""

    tenant_id: str | None = None
    """Owning tenant, sourced from the ambient ``varco_core.tenancy.tenant_context()``
    at push time (Plan 009, Phase 6 / R4) — never a constructor parameter a
    caller is expected to fill in by hand. ``None`` for a framework-level
    entry produced outside any tenant context (e.g. an outbox deserialize
    failure at boot) — that is the correct, documented value, not a bug:
    a framework-level failure is not any one tenant's data."""

    @classmethod
    def from_failure(
        cls,
        *,
        event: Event,
        channel: str,
        handler_name: str,
        last_exc: BaseException,
        attempts: int,
        first_failed_at: datetime,
    ) -> DeadLetterEntry:
        """
        Convenience constructor from a handler failure.

        Args:
            event:            The event that could not be processed.
            channel:          The channel the event was published to.
            handler_name:     Qualified name of the failed handler.
            last_exc:         The exception raised by the final attempt.
            attempts:         Total call attempts made.
            first_failed_at:  When the first failure occurred.

        Returns:
            A fully populated ``DeadLetterEntry``.

        Example::

            entry = DeadLetterEntry.from_failure(
                event=order_event,
                channel="orders",
                handler_name="OrderConsumer.on_order_placed",
                last_exc=ConnectionError("DB unavailable"),
                attempts=3,
                first_failed_at=first_failure_time,
            )
            await dlq.push(entry)
        """
        now = datetime.now(tz=UTC)
        return cls(
            event=event,
            channel=channel,
            handler_name=handler_name,
            error_type=type(last_exc).__name__,
            error_message=str(last_exc),
            attempts=attempts,
            first_failed_at=first_failed_at,
            last_failed_at=now,
        )


# ── AbstractDeadLetterQueue ───────────────────────────────────────────────────


class AbstractDeadLetterQueue(ABC):
    """
    Abstract interface for Dead Letter Queues.

    Backend packages (varco_kafka, varco_redis) implement this to persist
    failed events in their native storage format.  The in-process
    ``InMemoryDeadLetterQueue`` is provided for tests.

    Implementors must guarantee that ``push()`` is durable — a pushed entry
    must survive process restarts (for persistent backends) and must be
    retrievable via ``pop_batch()``.

    Thread safety:  ⚠️ Implementations must document their own thread safety.
    Async safety:   ✅ All methods are ``async def``.

    Edge cases:
        - ``push()`` must never raise — callers (event consumer wrapper) cannot
          handle DLQ failures meaningfully.  Implementations should log errors
          and swallow them rather than propagating.
        - ``pop_batch()`` returning an empty list means the DLQ is empty;
          this is not an error.
        - ``ack()`` on an unknown ``entry_id`` should be a no-op, not an error,
          to handle at-least-once delivery scenarios where ``ack`` is retried.
    """

    @abstractmethod
    async def push(self, entry: DeadLetterEntry) -> None:
        """
        Persist a failed event entry in the DLQ.

        Called by the event consumer after all retry attempts are exhausted.
        Must NOT raise — callers cannot meaningfully recover from DLQ failures.
        Implementations should log errors and swallow them.

        Args:
            entry: The ``DeadLetterEntry`` to persist.

        Async safety: ✅ Each ``push`` call is independent.
        """

    @abstractmethod
    async def pop_batch(self, *, limit: int = 10) -> list[DeadLetterEntry]:
        """
        Retrieve up to ``limit`` unacknowledged entries from the DLQ.

        Used by a DLQ relay process for replay or monitoring.  Entries returned
        here are NOT automatically deleted — call ``ack()`` after successful
        processing to remove them.

        Args:
            limit: Maximum number of entries to return.  Must be ≥ 1.

        Returns:
            List of ``DeadLetterEntry`` objects.  Empty list if the DLQ is empty.

        Edge cases:
            - May return fewer than ``limit`` entries even when the DLQ is not
              empty (e.g. due to visibility windows in Kafka/Redis).
            - Calling ``pop_batch`` while another caller has not yet ``ack``'d
              the previous batch behaviour is implementation-defined.
        """

    @abstractmethod
    async def ack(self, entry_id: UUID) -> None:
        """
        Acknowledge that a DLQ entry has been successfully processed.

        Removes the entry from the DLQ so it is not returned by future
        ``pop_batch`` calls.  Idempotent — safe to call multiple times.

        Args:
            entry_id: The ``DeadLetterEntry.entry_id`` to acknowledge.

        Edge cases:
            - Calling with an unknown ``entry_id`` is a no-op (not an error).
        """

    @abstractmethod
    async def count(self) -> int:
        """
        Return the current number of unacknowledged entries in the DLQ.

        Returns:
            Non-negative integer.  ``0`` means the DLQ is empty.

        Edge cases:
            - For persistent backends, this may be an approximation (e.g.
              Kafka consumer lag) rather than an exact count.
        """

    # ── Plan 009 additions — retention (R3), redrive (R1), tenancy (R4) ────────
    #
    # See the ABC table in plans/009-reliability-and-service-integration.md:
    # `supports_random_access` and `delete()` have portable defaults;
    # `get`/`list_entries`/`delete_where`/`count_by_channel` are
    # concrete-but-raising — no correct portable fallback exists for any of
    # them (RD-4: a `pop_batch`-based default would be destructive).

    supports_random_access: ClassVar[bool] = False
    """Capability flag (RD-4). ``False`` (the conservative default) means the
    backend cannot address a single entry by ``entry_id`` — a stream-shaped
    store (Kafka/NATS) where only ``pop_batch()`` is meaningful. Backends
    that DO support it (SA, Redis, Beanie) override this to ``True``."""

    async def get(self, entry_id: UUID) -> DeadLetterEntry | None:
        """
        Fetch a single entry by ``entry_id`` without removing it.

        Concrete-but-raising: no portable random-access implementation exists
        — a ``pop_batch()``-based scan would be destructive on
        ``InMemoryDeadLetterQueue`` (consume-on-pop) and would advance
        Kafka's in-flight tracking. See ``supports_random_access``.

        Returns:
            The entry, or ``None`` if no entry with that id exists.

        Raises:
            NotImplementedError: unless overridden by a random-access backend.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support get(entry_id) — "
            f"supports_random_access={self.supports_random_access}. "
            "Use redrive_batch()/pop_batch() instead."
        )

    async def list_entries(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        channel: str | None = None,
        source: DeadLetterSource | None = None,
        tenant_id: str | None = None,
        older_than: datetime | None = None,
        newer_than: datetime | None = None,
    ) -> list[DeadLetterEntry]:
        """
        Non-destructive, filtered read of entries — unlike ``pop_batch()``.

        A REST/CLI browse surface must be able to *look* without consuming;
        ``pop_batch()`` cannot serve that purpose on every backend (it is
        consume-on-pop in-memory). Concrete-but-raising for the same random-
        access reason as ``get()``.

        Args:
            limit:      Maximum entries to return.
            offset:     Pagination offset.
            channel:    Filter to one channel.
            source:     Filter to one ``DeadLetterSource``.
            tenant_id:  Filter to one tenant. ``None`` means "no tenant
                        filter" (an operator/global view) — this is
                        deliberately asymmetric with a ``None``-tenant entry
                        never matching an explicit ``tenant_id=`` filter.
            older_than: Only entries with ``last_failed_at`` before this time.
            newer_than: Only entries with ``last_failed_at`` after this time.

        Raises:
            NotImplementedError: unless overridden by a random-access backend.

        Edge cases:
            - A BSON-backed implementation (``BeanieDeadLetterQueue``) stores
              ``last_failed_at`` at millisecond precision and widens
              ``older_than`` to the next whole millisecond before querying
              (``varco_beanie._bson_time.ceil_to_bson_millisecond``) so that
              every entry the store itself reports as strictly older than
              the cutoff is still matched — see
              ``technical_docs/features/dead-letter-queues.md``'s Retention
              section. ``newer_than`` needs no such adjustment. SA/in-memory
              implementations keep full microsecond precision and this
              caveat does not apply to them.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support list_entries() — "
            f"supports_random_access={self.supports_random_access}."
        )

    async def delete(self, entry_id: UUID) -> None:
        """
        Delete one entry by id.

        Portable default: delegates to ``ack(entry_id)`` — every backend's
        ``ack()`` already means "never return this entry again", which is
        exactly the storage-semantics meaning of "delete". ``ack`` is the
        message-semantics name for the same operation; ``delete`` is the
        storage-semantics name a retention/admin caller reaches for.

        Edge cases:
            - Unknown ``entry_id`` → no-op (inherits ``ack()``'s contract).
        """
        await self.ack(entry_id)

    async def delete_where(
        self,
        *,
        older_than: datetime | None = None,
        source: DeadLetterSource | Sequence[DeadLetterSource] | None = None,
        channel: str | None = None,
        tenant_id: str | None = None,
        limit: int | None = None,
    ) -> int:
        """
        Bulk-delete entries matching every given predicate (retention sweep).

        Concrete-but-raising: any ``pop_batch()``-based default would have to
        ``ack()`` non-matching entries just to reach matching ones — silent
        data loss. Refusing is strictly safer than a wrong portable default.

        Args:
            older_than: Only entries with ``last_failed_at`` before this time.
            source:     One or more ``DeadLetterSource`` values to match.
            channel:    Match one channel.
            tenant_id:  Match one tenant.
            limit:      Cap on rows deleted in this call — chunk a large sweep
                        with repeated calls (``AbstractJobStore.delete_where``'s
                        precedent) rather than one unbounded DELETE.

        Returns:
            Number of entries deleted.

        Raises:
            NotImplementedError: unless overridden by a backend that supports
                bulk deletion (SA, Redis, Beanie). Kafka/NATS raise naming
                their own retention mechanism (``retention.ms`` / JetStream
                ``MaxAge``) instead — deleting a topic/subject entry-by-entry
                is not how those stores work.
            ValueError: no predicate at all was given — refuses to silently
                delete every entry (mirrors ``AbstractJobStore.delete_where``).

        Edge cases:
            - Same BSON-millisecond caveat as ``list_entries()``'s
              ``older_than``: ``BeanieDeadLetterQueue`` widens the operand to
              the next whole millisecond before querying, so a chunked sweep
              re-passing a fixed cutoff cannot strand an entry the store
              reports as strictly older than it.
        """
        if older_than is None and source is None and channel is None and tenant_id is None:
            raise ValueError(
                "delete_where() requires at least one predicate "
                "(older_than/source/channel/tenant_id) — refusing to delete "
                "every entry."
            )
        raise NotImplementedError(
            f"{type(self).__name__} does not support delete_where() — "
            "implement it in a durable backend, or use that backend's own "
            "retention mechanism."
        )

    async def count_by_channel(self) -> dict[str, int]:
        """
        Return ``{channel: unacknowledged_count}`` for every channel.

        Concrete-but-raising: ``count()`` itself is already ``-1`` (an
        approximation) on Kafka — a per-channel breakdown is not portable
        even in principle there. Opt-in via
        ``ReliabilityMetricsConfig(depth_by_channel=True)``.

        Raises:
            NotImplementedError: unless overridden.
        """
        raise NotImplementedError(f"{type(self).__name__} does not support count_by_channel().")


# ── InMemoryDeadLetterQueue ───────────────────────────────────────────────────


@Fallback
@Singleton(qualifier="in_memory")
class InMemoryDeadLetterQueue(AbstractDeadLetterQueue):
    """
    In-memory ``AbstractDeadLetterQueue`` implementation for tests.

    Stores entries in a ``deque`` with a configurable maximum size.  The oldest
    entries are evicted when the deque is full to prevent unbounded memory use.

    NOT suitable for production — entries are lost on process restart.

    DESIGN: deque over list
        ✅ O(1) append to right and pop from left — efficient for a queue.
        ✅ ``maxlen`` parameter provides automatic eviction of oldest entries.
        ❌ No persistence — tests only.

    Thread safety:  ✅ ``asyncio.Lock`` protects all mutations.
    Async safety:   ✅ All methods use the lock — safe for concurrent coroutines.

    Attributes:
        max_size: Maximum number of entries before oldest are evicted.
                  Defaults to ``10_000``.

    Edge cases:
        - When ``max_size`` is reached, the oldest entry is silently dropped
          on the next ``push()``.  A WARNING is logged when eviction occurs.
        - ``pop_batch()`` returns and removes entries from oldest-first order.
        - ``ack()`` is a no-op since ``pop_batch()`` already removes entries —
          the in-memory implementation uses a simple consume-on-pop model.
    """

    # In-memory is fully addressable — a plain scan over the deque, non-
    # destructive except pop_batch() (which is documented as consume-on-pop).
    supports_random_access = True

    def __init__(self, *, max_size: int = _IN_MEMORY_MAX_SIZE) -> None:
        """
        Args:
            max_size: Maximum number of DLQ entries before oldest are evicted.
                      Must be ≥ 1.

        Raises:
            ValueError: ``max_size`` < 1.
        """
        if max_size < 1:
            raise ValueError(f"InMemoryDeadLetterQueue.max_size must be ≥ 1, got {max_size}.")
        # deque with maxlen auto-evicts oldest entries when full — no manual
        # size management needed.
        self._entries: deque[DeadLetterEntry] = deque(maxlen=max_size)
        self._max_size = max_size
        # Lock guards all mutations — prevents race conditions under concurrent
        # push/pop calls in the same event loop.
        self._lock: asyncio.Lock | None = None

    def _get_lock(self) -> asyncio.Lock:
        """Return the lock, creating it lazily inside the running event loop."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    @property
    def entries(self) -> list[DeadLetterEntry]:
        """
        Snapshot of currently stored entries, oldest first.

        Test-only convenience accessor (no lock — reads a ``deque``
        snapshot). Production code should use ``pop_batch()``.
        """
        return list(self._entries)

    async def push(self, entry: DeadLetterEntry) -> None:
        """
        Append ``entry`` to the DLQ.

        If the deque is full, the oldest entry is evicted and a WARNING is
        logged.  Never raises — callers (retry wrapper) must not be interrupted
        by DLQ failures.

        Args:
            entry: ``DeadLetterEntry`` to store.

        Async safety: ✅ Protected by asyncio.Lock.

        Edge cases:
            - Eviction is silent except for the WARNING log — the evicted entry
              is permanently lost.
        """
        try:
            async with self._get_lock():
                if len(self._entries) == self._max_size:
                    # deque.maxlen would silently drop — log explicitly so the
                    # operator knows the DLQ is filling up.
                    _logger.warning(
                        "InMemoryDeadLetterQueue reached max_size=%d — "
                        "evicting oldest entry to make room for new one.",
                        self._max_size,
                    )
                self._entries.append(entry)
                _logger.debug(
                    "DLQ push: handler=%r event_type=%r entry_id=%s",
                    entry.handler_name,
                    type(entry.event).__name__,
                    entry.entry_id,
                )
        except Exception as exc:  # noqa: BLE001 — DLQ must not propagate
            # push() must never raise — log and swallow any unexpected error.
            _logger.error("InMemoryDeadLetterQueue.push() failed unexpectedly: %s", exc)

    async def pop_batch(self, *, limit: int = 10) -> list[DeadLetterEntry]:
        """
        Remove and return up to ``limit`` entries from the front of the DLQ.

        DESIGN: consume-on-pop (no separate ack step for in-memory)
            The in-memory implementation removes entries immediately — there is
            no persistence, so a separate ``ack`` step would add complexity
            without value.  ``ack()`` is a no-op.

        Args:
            limit: Maximum number of entries to return.  Must be ≥ 1.

        Returns:
            List of ``DeadLetterEntry`` objects removed from the DLQ.
            Empty list if the DLQ is empty.

        Raises:
            ValueError: ``limit`` < 1.

        Async safety: ✅ Protected by asyncio.Lock.
        """
        if limit < 1:
            raise ValueError(f"pop_batch limit must be ≥ 1, got {limit}.")
        async with self._get_lock():
            batch: list[DeadLetterEntry] = []
            for _ in range(min(limit, len(self._entries))):
                batch.append(self._entries.popleft())
            return batch

    async def ack(self, entry_id: UUID) -> None:
        """
        No-op for ``InMemoryDeadLetterQueue`` — entries are removed by ``pop_batch``.

        Args:
            entry_id: Ignored.

        Edge cases:
            - Always succeeds regardless of whether ``entry_id`` exists.
        """
        # In-memory implementation consumes entries at pop_batch time —
        # no separate ack step is needed.

    async def count(self) -> int:
        """
        Return the number of entries currently in the DLQ.

        Returns:
            Exact count — never an approximation for the in-memory implementation.

        Async safety: ✅ Protected by asyncio.Lock.
        """
        async with self._get_lock():
            return len(self._entries)

    async def get(self, entry_id: UUID) -> DeadLetterEntry | None:
        """Non-destructive scan of the deque for ``entry_id``."""
        async with self._get_lock():
            for entry in self._entries:
                if entry.entry_id == entry_id:
                    return entry
        return None

    async def list_entries(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        channel: str | None = None,
        source: DeadLetterSource | None = None,
        tenant_id: str | None = None,
        older_than: datetime | None = None,
        newer_than: datetime | None = None,
    ) -> list[DeadLetterEntry]:
        """Non-destructive, filtered, paginated read — unlike ``pop_batch()``."""
        async with self._get_lock():
            snapshot = list(self._entries)
        filtered = [
            e
            for e in snapshot
            if (channel is None or e.channel == channel)
            and (source is None or e.source == source)
            and (tenant_id is None or e.tenant_id == tenant_id)
            and (older_than is None or e.last_failed_at < older_than)
            and (newer_than is None or e.last_failed_at > newer_than)
        ]
        return filtered[offset : offset + limit]

    async def delete_where(
        self,
        *,
        older_than: datetime | None = None,
        source: DeadLetterSource | Sequence[DeadLetterSource] | None = None,
        channel: str | None = None,
        tenant_id: str | None = None,
        limit: int | None = None,
    ) -> int:
        """Bulk delete matching entries directly from the deque."""
        if older_than is None and source is None and channel is None and tenant_id is None:
            raise ValueError(
                "delete_where() requires at least one predicate "
                "(older_than/source/channel/tenant_id) — refusing to delete "
                "every entry."
            )
        sources = None
        if source is not None:
            sources = source if isinstance(source, (list, tuple, set)) else [source]
        async with self._get_lock():
            to_delete: list[UUID] = []
            for entry in self._entries:
                if older_than is not None and entry.last_failed_at >= older_than:
                    continue
                if channel is not None and entry.channel != channel:
                    continue
                if tenant_id is not None and entry.tenant_id != tenant_id:
                    continue
                if sources is not None and entry.source not in sources:
                    continue
                to_delete.append(entry.entry_id)
                if limit is not None and len(to_delete) >= limit:
                    break
            if to_delete:
                ids = set(to_delete)
                remaining = [e for e in self._entries if e.entry_id not in ids]
                self._entries.clear()
                self._entries.extend(remaining)
        return len(to_delete)

    async def count_by_channel(self) -> dict[str, int]:
        """Return ``{channel: count}`` across all currently stored entries."""
        async with self._get_lock():
            snapshot = list(self._entries)
        counts: dict[str, int] = {}
        for entry in snapshot:
            counts[entry.channel] = counts.get(entry.channel, 0) + 1
        return counts

    def __repr__(self) -> str:
        return f"InMemoryDeadLetterQueue(entries={len(self._entries)}, max_size={self._max_size})"
