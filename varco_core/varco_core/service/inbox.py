"""
varco_core.service.inbox
========================
Inbox pattern — guarantees at-least-once consumption (bus → DB → handler).

Problem
-------
The event consumer flow is:

    1. Bus delivers event to handler.
    2. Handler processes event and writes to DB.
    3. Bus ACKs the delivery.

If step 2 fails (handler crash, DB unavailable), the bus has already ACKed
the delivery.  The event is permanently lost — no retry will occur because
the bus believes it was processed.

This is the reverse of the Outbox problem:
    - Outbox: DB committed but bus publish failed → event lost (DB → bus gap).
    - Inbox:  Bus ACKed but handler failed → event lost (bus → handler gap).

Solution
--------
Before calling the handler, save the incoming event as an ``InboxEntry`` in
the DB.  After successful handler completion, mark the entry as processed.

A background ``InboxPoller`` re-delivers unprocessed entries to the bus so
they reach the handler again, even after a crash.

::

    ┌───────────────────────────────────────────────────────────────────────┐
    │  Event arrives from bus                                               │
    │                                                                       │
    │  inbox.save(InboxEntry)   ←── saved BEFORE handler runs              │
    │  handler(event)            ←── handler executes                       │
    │  inbox.mark_processed()   ←── marked AFTER success                   │
    │                                                                       │
    │  If handler crashes: entry stays unprocessed → InboxPoller replays   │
    └───────────────────────────────────────────────────────────────────────┘

               InboxPoller (background loop)
                  ↓
    get_unprocessed() → bus.publish(event, channel) → mark_processed()

Components
----------
``InboxEntry``
    Immutable value object.  ``processed_at`` is ``None`` until the handler
    completes successfully.

``InboxRepository``
    ABC.  Implement against your storage backend (SA, Beanie, etc.).
    ``mark_processed`` must use optimistic locking (``WHERE processed_at IS NULL``)
    to be safe under concurrent InboxPoller + live handler execution.

``InboxPoller``
    Background asyncio task.  Polls unprocessed entries and re-publishes them
    to the bus so the full consumer dispatch path (retry wrappers, deduplication)
    handles re-delivery transparently.

Integration
-----------
Wire via ``@listen(..., inbox=my_inbox_repo)`` on the handler method.  The
``_make_inbox_wrapper`` function wraps the retry wrapper so that
``inbox.save`` fires before any retry attempt, and ``mark_processed`` fires
only after the outermost wrapper returns successfully.

Usage::

    repo = SAInboxRepository(uow.session)
    poller = InboxPoller(inbox=repo, bus=bus)
    await poller.start()

    class OrderConsumer(EventConsumer):
        @listen(OrderPlacedEvent, channel="orders", inbox=repo)
        async def on_order(self, event: OrderPlacedEvent) -> None:
            await self._process(event)

DESIGN: InboxPoller re-publishes to bus (not directly to handler)
    ✅ Re-uses the full consumer dispatch path — retry, DLQ, deduplication
       wrappers are all applied on re-delivery.
    ✅ InboxPoller is stateless w.r.t. handler knowledge — no coupling to
       specific consumer classes.
    ❌ Re-published events reach ALL subscribers of that channel, not just the
       consumer that originally saved the InboxEntry.  Use a dedicated inbox
       channel per consumer to scope re-delivery.

DESIGN: inbox wrapper is outermost (wraps the retry wrapper)
    ✅ inbox.save fires once before any retry attempt — the entry captures the
       intent to process, not the result.  Retries all see the same entry.
    ✅ mark_processed fires after the outermost wrapper returns — only when
       the handler (and all retries) have completed successfully.
    ❌ If inbox.save fails, the handler is NOT called — the event may be lost.
       A DB outage is a more severe problem than a single lost event.

Thread safety:  ⚠️ InboxPoller must be started inside the running event loop.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 🐍 https://docs.python.org/3/library/asyncio-task.html
  asyncio.create_task — background task creation used by InboxPoller.
- 📐 https://microservices.io/patterns/reliability/transactional-outbox.html
  Transactional Outbox / Inbox pattern — original pattern reference.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from varco_core.event.base import CHANNEL_DEFAULT, AbstractEventBus, Event
from varco_core.event.serializer import JsonEventSerializer

if TYPE_CHECKING:
    from varco_core.event.serializer import EventSerializer

_logger = logging.getLogger(__name__)

# Default batch size for get_unprocessed() — balance between DB round trips and
# memory usage.  100 entries per poll is conservative; tune per workload.
_DEFAULT_BATCH_SIZE: int = 100

# Default polling interval in seconds.
_DEFAULT_POLL_INTERVAL: float = 1.0


# ── InboxEntry ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class InboxEntry:
    """
    Immutable value object representing a single in-flight inbox event.

    Saved by the consumer wrapper BEFORE the handler runs.  Marked processed
    AFTER the handler completes successfully.  Unprocessed entries are
    replayed by ``InboxPoller`` after a crash.

    DESIGN: frozen dataclass — mirrors OutboxEntry
        ✅ No Pydantic dependency in the service layer.
        ✅ Hashable — safe to use in sets.
        ✅ Consistent with the Outbox pattern; same lifecycle contract.
        ❌ ``processed_at`` is part of the frozen value but is semantically
           mutable.  The repository updates the DB row directly; a fresh
           domain model object reflects the updated state if needed.

    Thread safety:  ✅ Frozen — immutable after construction.
    Async safety:   ✅ Pure value object; no I/O.

    Attributes:
        entry_id:     Unique identifier for this inbox row.
        event_type:   ``Event.event_type_name()`` — for logging and tracing.
        channel:      The bus channel the event arrived on.  Stored so
                      ``InboxPoller`` can re-publish to the same channel.
        payload:      UTF-8 JSON bytes produced by ``JsonEventSerializer``.
                      Stored so re-delivery requires no knowledge of the
                      original event class at replay time.
        received_at:  UTC timestamp when the event first arrived.
        processed_at: ``None`` until the handler completes.  Set by
                      ``InboxRepository.mark_processed()``.

    Edge cases:
        - ``processed_at`` on a frozen dataclass can only be updated by
          constructing a new ``InboxEntry``; the repository stores the update
          in the backing table.  ``InboxPoller`` only reads the ``received_at``
          ordering — it does not compare ``processed_at`` in Python.
        - Two entries for the same ``event_id`` (re-saved before mark_processed)
          are independent rows — the handler sees the event twice.  Pair with a
          deduplicator on the handler to prevent double-processing.
        - ``received_at`` is always UTC.

    Example::

        entry = InboxEntry.from_event(event, channel="orders")
        await inbox_repo.save(entry)
        handler(event)
        await inbox_repo.mark_processed(entry.entry_id)
    """

    entry_id: UUID = field(default_factory=uuid4)

    # Human-readable event type — for logging; not used in routing.
    event_type: str = ""

    # Bus channel the event arrived on — used by InboxPoller to re-publish.
    channel: str = CHANNEL_DEFAULT

    # Pre-serialized event bytes — allows re-delivery without importing the
    # original event class at replay time.
    payload: bytes = b""

    # UTC time when the event first arrived at this consumer.
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    # None until successfully processed — sentinel for InboxPoller queries.
    processed_at: datetime | None = None

    @classmethod
    def from_event(
        cls,
        event: Event,
        *,
        channel: str = CHANNEL_DEFAULT,
        serializer: EventSerializer | None = None,
    ) -> InboxEntry:
        """
        Construct an ``InboxEntry`` from an incoming ``Event``.

        Serializes the event to bytes so the entry is self-describing and can
        be re-delivered without importing the original event class.

        Args:
            event:      The incoming ``Event`` instance.
            channel:    The bus channel the event arrived on.
            serializer: Custom serializer.  Defaults to ``JsonEventSerializer``.

        Returns:
            A fully populated ``InboxEntry`` with ``processed_at=None``.

        Edge cases:
            - ``received_at`` is set to ``datetime.now(UTC)`` at call time —
              the consumer's arrival timestamp, not the event's original timestamp.
            - ``entry_id`` is a fresh UUID independent of ``event.event_id``.
        """
        _serializer = serializer or JsonEventSerializer()
        return cls(
            event_type=event.event_type_name(),
            channel=channel,
            payload=_serializer.serialize(event),
        )


# ── InboxRepository ───────────────────────────────────────────────────────────


class InboxRepository(ABC):
    """
    Abstract persistence contract for the inbox store.

    Implement this against your DB backend:
    - ``varco_sa``: ``SAInboxRepository`` using ``AsyncSession``.
    - ``varco_beanie``: ``BeanieInboxRepository`` using a Beanie document.

    Thread safety:  ⚠️ Implementations must not share state between concurrent
                       callers — use one repository instance per task/request.
    Async safety:   ✅ All methods are ``async def``.
    """

    @abstractmethod
    async def save(self, entry: InboxEntry) -> None:
        """
        Persist a new inbox entry before the handler runs.

        Should be called INSIDE the handler dispatch path — before the handler
        is invoked — so the entry is durably stored even if the handler crashes.

        Args:
            entry: The ``InboxEntry`` to persist.

        Raises:
            Any DB-level exception from the underlying driver.

        Edge cases:
            - Calling outside a transaction context is implementation-defined;
              most async drivers will auto-commit in that case.
            - If ``save`` fails, the handler must NOT be called — the consumer
              wrapper skips the handler and lets the exception propagate.
        """

    @abstractmethod
    async def mark_processed(self, entry_id: UUID) -> None:
        """
        Mark the inbox entry as processed after successful handler completion.

        Implementations MUST use optimistic locking:
        ``UPDATE SET processed_at = now() WHERE entry_id = ? AND processed_at IS NULL``

        This ensures that concurrent ``InboxPoller`` and live handler paths
        cannot both claim the same entry — only the first one succeeds; the
        other call is a no-op.

        Args:
            entry_id: The ``InboxEntry.entry_id`` to mark.

        Edge cases:
            - Unknown ``entry_id`` → silent no-op (idempotent).
            - Already-processed entry → silent no-op (idempotent — see above).
            - Must NOT raise — if the backing store is unavailable, log the
              error and return.  The InboxPoller will re-deliver the event;
              pair with a deduplicator on the handler to prevent re-processing.
        """

    @abstractmethod
    async def get_unprocessed(
        self, *, limit: int = _DEFAULT_BATCH_SIZE
    ) -> list[InboxEntry]:
        """
        Return up to ``limit`` unprocessed inbox entries, ordered by ``received_at``.

        ``InboxPoller`` calls this in a polling loop.  Oldest-first ordering
        ensures events are replayed in approximate FIFO order.

        Args:
            limit: Maximum number of entries to return.  Default ``100``.

        Returns:
            List of ``InboxEntry`` objects with ``processed_at=None``.
            Empty list if no unprocessed entries exist.

        Edge cases:
            - Concurrent callers may receive overlapping results — handlers and
              the poller may both attempt to process the same entry.  Use
              optimistic locking in ``mark_processed`` to resolve races.
            - Sort without a DB index on ``received_at`` is O(N) — acceptable
              for small inboxes; add an index in production.
        """


# ── InboxPoller ───────────────────────────────────────────────────────────────


class InboxPoller:
    """
    Background asyncio task that replays unprocessed inbox entries to the bus.

    Mirrors ``OutboxRelay`` in structure:
    - ``start()`` / ``stop()`` lifecycle.
    - ``_relay_loop()`` / ``_relay_once()`` / ``_relay_entry()`` for testability.
    - Async context manager support (``async with InboxPoller(...) as poller:``).

    On each tick:
    1. Calls ``inbox.get_unprocessed(limit=batch_size)``.
    2. For each entry, deserializes the payload and calls ``bus.publish()``.
    3. On successful publish, calls ``inbox.mark_processed(entry.entry_id)``.
    4. On failure, logs the error and leaves the entry unprocessed for retry.
    5. Sleeps for ``poll_interval`` seconds before the next tick.

    DESIGN: re-publish to bus (not directly to handler)
        ✅ Re-uses the full consumer dispatch path — retry wrappers, DLQ, and
           deduplication are all applied transparently on re-delivery.
        ✅ InboxPoller is decoupled from handler code — no handler references.
        ❌ Re-published events reach ALL subscribers of that channel.  Use a
           dedicated inbox channel per consumer to scope re-delivery.

    DESIGN: mark_processed AFTER publish (not before)
        ✅ If the process crashes between publish and mark_processed, the entry
           is replayed on the next tick (at-least-once delivery).
        ✅ If mark_processed fails (DB down), the entry is replayed — pair with
           a deduplicator on the handler to prevent double-processing.
        ❌ At-least-once semantics — handlers must be idempotent.

    DESIGN: InboxPoller holds AbstractEventBus directly
        The poller is infrastructure, not domain code.  The accepted exception
        for infrastructure components — same rationale as OutboxRelay.
        ✅ See project feedback: bus_access_pattern.

    Thread safety:  ❌ Not thread-safe.  Run one poller per event loop.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        inbox:          Implementation of ``InboxRepository``.
        bus:            ``AbstractEventBus`` to re-publish events to.
        serializer:     Serializer used to decode ``InboxEntry.payload`` back
                        to a typed ``Event``.  Defaults to ``JsonEventSerializer``.
        poll_interval:  Seconds to sleep between polls.  Default ``1.0``.
        batch_size:     Max entries fetched per poll.  Default ``100``.

    Edge cases:
        - ``start()`` must be called from within a running event loop.
        - Calling ``start()`` a second time without ``stop()`` raises.
        - ``stop()`` cancels the background task and waits for it to exit.
        - Deserialization errors on an entry are logged and the entry is left
          unprocessed — it will be retried next tick.  Manual intervention may
          be needed if the payload is permanently corrupt.
    """

    def __init__(
        self,
        *,
        inbox: InboxRepository,
        bus: AbstractEventBus,
        serializer: EventSerializer | None = None,
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
        batch_size: int = _DEFAULT_BATCH_SIZE,
    ) -> None:
        """
        Args:
            inbox:         Inbox repository for fetching and marking entries.
            bus:           Event bus to re-publish unprocessed events to.
            serializer:    Deserializer for ``InboxEntry.payload`` bytes.
                           Defaults to ``JsonEventSerializer``.
            poll_interval: Seconds between polls.  Default ``1.0``.
            batch_size:    Max entries per poll batch.  Default ``100``.
        """
        self._inbox = inbox
        self._bus = bus
        self._serializer: EventSerializer = serializer or JsonEventSerializer()
        self._poll_interval = poll_interval
        self._batch_size = batch_size
        # Background asyncio Task — created in start(), cancelled in stop().
        self._task: asyncio.Task | None = None  # type: ignore[type-arg]

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start the background polling task.

        Must be called from within a running event loop.  The task runs until
        ``stop()`` is called.

        Raises:
            RuntimeError: If already started (task is not None).

        Edge cases:
            - Calling ``start()`` a second time without ``stop()`` raises —
              prevents ghost tasks that poll concurrently and cause duplicates.
        """
        if self._task is not None:
            raise RuntimeError(
                f"{type(self).__name__}.start() called twice without stop(). "
                "Call stop() first to cancel the running task."
            )
        self._task = asyncio.create_task(
            self._relay_loop(), name=f"InboxPoller[{id(self)}]"
        )
        _logger.info(
            "InboxPoller started (poll_interval=%.1f s, batch_size=%d)",
            self._poll_interval,
            self._batch_size,
        )

    async def stop(self) -> None:
        """
        Cancel the background task and wait for it to finish.

        Idempotent — calling ``stop()`` when not started is a no-op.

        Edge cases:
            - If the task is sleeping between polls, cancellation wakes it
              immediately — the current poll batch is abandoned.
        """
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            # CancelledError is expected — task was cancelled by stop().
            # Other exceptions are already logged inside _relay_loop.
            pass
        self._task = None
        _logger.info("InboxPoller stopped.")

    async def __aenter__(self) -> InboxPoller:
        """Start on context-manager entry."""
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        """Stop on context-manager exit."""
        await self.stop()

    # ── Polling loop ──────────────────────────────────────────────────────────

    async def _relay_loop(self) -> None:
        """
        Main polling loop — runs until cancelled.

        Calls ``_relay_once()`` on each tick, then sleeps for ``poll_interval``
        seconds.  Exceptions from ``_relay_once`` are caught and logged so a
        single failure does not stop the entire loop.

        Async safety: ✅ asyncio.sleep yields control between ticks.
        """
        while True:
            try:
                await self._relay_once()
            except asyncio.CancelledError:
                raise  # Re-raise so the task exits cleanly on stop()
            except Exception as exc:
                _logger.error(
                    "InboxPoller._relay_once raised an unexpected exception: %s",
                    exc,
                    exc_info=True,
                )
            await asyncio.sleep(self._poll_interval)

    async def _relay_once(self) -> None:
        """
        Fetch one batch of unprocessed entries and attempt to re-publish each.

        Called by ``_relay_loop`` on each tick.  Also callable directly in
        tests to trigger a single relay pass without starting the background task.

        Edge cases:
            - If ``get_unprocessed`` raises, the exception propagates to
              ``_relay_loop`` which logs it and continues on the next tick.
            - Individual entry failures are caught inside ``_relay_entry`` and
              do not abort the rest of the batch.
        """
        entries = await self._inbox.get_unprocessed(limit=self._batch_size)
        if not entries:
            return

        _logger.debug("InboxPoller: replaying %d unprocessed entries", len(entries))
        for entry in entries:
            await self._relay_entry(entry)

    async def _relay_entry(self, entry: InboxEntry) -> None:
        """
        Deserialize and re-publish a single unprocessed inbox entry to the bus.

        On successful ``bus.publish()``, calls ``inbox.mark_processed()``.
        On failure, logs the error and leaves the entry unprocessed for retry.

        Args:
            entry: The ``InboxEntry`` to relay.

        Async safety: ✅ Awaits both publish and mark_processed.

        Edge cases:
            - Deserialization failure: the entry is left unprocessed.  Manual
              inspection of the payload bytes may be needed.
            - ``mark_processed`` failure: the entry will be replayed next tick.
              Pair the handler with a deduplicator to prevent double-processing.
        """
        try:
            # Deserialize the stored payload back to a typed Event.
            event: Event = self._serializer.deserialize(entry.payload)
        except Exception as exc:
            _logger.error(
                "InboxPoller: failed to deserialize entry_id=%s event_type=%r: %s",
                entry.entry_id,
                entry.event_type,
                exc,
            )
            return  # Leave entry unprocessed — retry next tick

        try:
            # Re-publish to the bus on the original channel.
            # The full consumer dispatch path (retry, DLQ, deduplication) applies.
            await self._bus.publish(event, channel=entry.channel)
        except Exception as exc:
            _logger.error(
                "InboxPoller: failed to publish entry_id=%s event_type=%r channel=%r: %s",
                entry.entry_id,
                entry.event_type,
                entry.channel,
                exc,
            )
            return  # Leave entry unprocessed — retry next tick

        # Mark processed AFTER publish — if this call fails, the entry will be
        # replayed on the next tick (at-least-once delivery).
        try:
            await self._inbox.mark_processed(entry.entry_id)
            _logger.debug(
                "InboxPoller: marked entry_id=%s as processed",
                entry.entry_id,
            )
        except Exception as exc:
            _logger.warning(
                "InboxPoller: mark_processed failed for entry_id=%s: %s. "
                "Entry will be replayed next tick.",
                entry.entry_id,
                exc,
            )

    def __repr__(self) -> str:
        return (
            f"InboxPoller("
            f"running={self._task is not None}, "
            f"poll_interval={self._poll_interval}, "
            f"batch_size={self._batch_size})"
        )


# ── _make_inbox_wrapper ───────────────────────────────────────────────────────


def _make_inbox_wrapper(
    handler: object,
    inbox: InboxRepository,
    channel: str,
    *,
    serializer: EventSerializer | None = None,
) -> object:
    """
    Wrap a handler (or retry wrapper) with inbox save-before / mark-after logic.

    The returned wrapper:
    1. Constructs an ``InboxEntry`` from the incoming event.
    2. Calls ``inbox.save(entry)`` — durably records intent to process.
    3. Calls the inner ``handler(event)`` — the original business logic.
    4. On success: calls ``inbox.mark_processed(entry.entry_id)``.
    5. On failure: does NOT mark processed — ``InboxPoller`` will replay.

    DESIGN: inbox wrapper is outermost (wraps the retry wrapper)
        ✅ inbox.save fires once — not once per retry attempt.  The entry
           captures the intent to process the event; retries are internal.
        ✅ mark_processed fires only after all retries succeed — the entry
           stays unprocessed until the handler truly completes.
        ❌ If inbox.save fails, the handler is skipped — a DB outage means
           the event is unprocessed AND unsaved.  This is the rarest failure
           mode; an OutboxRelay restart on the producer side will re-deliver.

    Args:
        handler:    The inner handler (may be the raw method or a retry wrapper).
        inbox:      ``InboxRepository`` instance to save / mark entries.
        channel:    Resolved channel string — stored in the ``InboxEntry``.
        serializer: Event serializer for ``InboxEntry.from_event()``.

    Returns:
        An async callable ``(event: Event) -> None``.

    Async safety: ✅ All I/O is awaited.

    Edge cases:
        - ``inbox.save`` failure → handler NOT called; exception propagates.
        - Handler failure → ``mark_processed`` NOT called; entry replayed.
        - ``mark_processed`` failure → logs warning; entry replayed (at-least-once).
    """
    import asyncio as _asyncio

    _serializer = serializer or JsonEventSerializer()

    async def wrapper(event: Event) -> None:
        """
        Inbox-aware handler wrapper.

        Async safety: ✅ All calls are awaited.
        """
        # Save BEFORE calling the handler — ensures the entry exists in the
        # DB even if the handler or the process crashes.
        entry = InboxEntry.from_event(event, channel=channel, serializer=_serializer)
        await inbox.save(entry)

        try:
            # Call the inner handler (may be a retry wrapper or raw method).
            if _asyncio.iscoroutinefunction(handler):
                await handler(event)  # type: ignore[operator]
            else:
                handler(event)  # type: ignore[operator]

            # Mark as processed AFTER success — not marked on failure so
            # InboxPoller can replay the event.
            try:
                await inbox.mark_processed(entry.entry_id)
            except Exception as exc:
                # mark_processed failure must not mask a successful handler.
                # Log and continue — InboxPoller will replay and re-mark.
                _logger.warning(
                    "_make_inbox_wrapper: mark_processed failed for entry_id=%s: %s. "
                    "Entry will be replayed by InboxPoller.",
                    entry.entry_id,
                    exc,
                )

        except BaseException:
            # Handler or retry wrapper raised — do NOT mark processed.
            # The InboxPoller will re-deliver the event.
            raise

    return wrapper


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "InboxEntry",
    "InboxPoller",
    "InboxRepository",
    "_make_inbox_wrapper",
]
