"""
varco_core.service.outbox
=========================
Transactional outbox pattern — guarantees at-least-once event delivery.

Problem
-------
``AsyncService`` publishes events directly to the bus **after** a DB commit::

    async with uow:
        saved = await repo.save(entity)
    # ← DB committed here
    await self._produce(EntityCreatedEvent(...))   # ← can fail!

If ``_produce`` fails (network hiccup, broker down), the DB change is
committed but the event is permanently lost.  There is no recovery path.

Solution
--------
The transactional outbox pattern writes the event to a DB table in the
**same transaction** as the domain entity.  A separate background relay
task reads unsent entries from the outbox table and publishes them,
deleting each entry only after the bus confirms it was accepted.

::

    ┌────────────────────────────────────────────────────────────────────┐
    │  DB transaction                                                    │
    │  ┌──────────┐   save()    ┌───────────┐   save_outbox()           │
    │  │  service │ ──────────► │   repo    │ ──────────────► outbox tbl│
    │  └──────────┘             └───────────┘                           │
    └────────────────────────────────────────────────────────────────────┘
               ▲                                            │
               │  (separate process / periodic task)        │
               │                                            ▼
    ┌──────────┴────────────────────────────────────────────────────────┐
    │  OutboxRelay (background loop)                                    │
    │                                                                   │
    │  get_pending()  →  bus.publish()  →  delete(entry_id)  ✓         │
    │                                                                   │
    │  If publish fails → entry stays in outbox → retried next tick     │
    └───────────────────────────────────────────────────────────────────┘

Components
----------
``OutboxEntry``
    Immutable value object representing a single pending event.  Stored in
    the outbox table by the service layer within the domain transaction.

``OutboxRepository``
    ABC defining persistence operations for outbox entries.  Implement this
    against your storage backend (SQLAlchemy, Beanie, etc.).

``OutboxRelay``
    Background asyncio task that polls ``OutboxRepository`` and forwards
    pending entries to an ``AbstractEventBus``.  Deletes entries on success.

Usage (wiring at startup)::

    # 1.  Implement OutboxRepository for your backend (e.g. varco_sa)
    class SAOutboxRepository(OutboxRepository):
        async def save(self, entry: OutboxEntry) -> None: ...
        async def get_pending(self, *, limit: int = 100) -> list[OutboxEntry]: ...
        async def delete(self, entry_id: UUID) -> None: ...

    # 2.  Save events inside the domain transaction (in your service / UoW)
    outbox_entry = OutboxEntry.from_event(EntityCreatedEvent(...), channel="orders")
    await outbox_repo.save(outbox_entry)   # same session as domain entity save

    # 3.  Start the relay (once at app startup)
    relay = OutboxRelay(outbox=outbox_repo, bus=bus)
    await relay.start()
    # ... app runs ...
    await relay.stop()

DESIGN: OutboxRelay holds AbstractEventBus directly (not AbstractEventProducer)
    The relay IS infrastructure — its sole job is bridging the persistent
    outbox store to the event bus.  It is not domain or service code.
    ✅ Direct bus access is the accepted exception for infrastructure-level
       components (see project feedback: bus_access_pattern).
    ✅ Keeps ``AbstractEventProducer`` as the domain-layer abstraction only.
    ❌ If the relay is extended to handle domain concerns, re-evaluate.

Thread safety:  ⚠️ ``OutboxRelay.start()`` must be called from within the
                    running event loop.  ``stop()`` cancels the background task.
Async safety:   ✅ All relay methods are ``async def``.

📚 Docs
- 🐍 https://docs.python.org/3/library/asyncio-task.html
  asyncio.create_task — background task creation
- 📐 https://microservices.io/patterns/data/transactional-outbox.html
  Transactional Outbox pattern — original pattern reference
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

# Default batch size for get_pending() — balance between DB round trips and
# memory usage.  100 entries per poll is conservative; tune per workload.
_DEFAULT_BATCH_SIZE: int = 100

# Default polling interval in seconds.  1 second provides a reasonable
# trade-off between event delivery latency and DB poll pressure.
_DEFAULT_POLL_INTERVAL: float = 1.0


# ── OutboxEntry ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class OutboxEntry:
    """
    Immutable value object representing a single pending outbox event.

    Persisted by the service layer inside the domain transaction.  Consumed
    and deleted by ``OutboxRelay`` after the event is published to the bus.

    DESIGN: frozen dataclass over Pydantic BaseModel
      ✅ No Pydantic dependency in the service layer — ``OutboxEntry`` is
         pure Python; DB mappers in varco_sa/varco_beanie handle persistence.
      ✅ Hashable — can be stored in sets for deduplication.
      ❌ No built-in JSON serialization — callers must use the ``payload``
         field (pre-serialized bytes) rather than reconstructing from JSON.

    Thread safety:  ✅ Frozen — immutable after construction.
    Async safety:   ✅ Pure value object; no I/O.

    Attributes:
        entry_id:   Unique identifier for this outbox entry.  Used as the
                    idempotency key when acknowledging successful delivery.
        event_type: ``Event.__event_type__`` class name — stored so the relay
                    can log meaningful information without deserializing.
        channel:    Target event channel.  Passed verbatim to ``bus.publish()``.
        payload:    UTF-8 JSON bytes produced by ``JsonEventSerializer.serialize()``.
                    Stored as bytes so the relay can forward them without
                    round-tripping through the domain model.
        created_at: UTC timestamp of when this entry was created.  Useful for
                    alerting on stale entries that were never relayed.

    Edge cases:
        - ``payload`` is opaque bytes — the relay does not inspect the contents.
          Only the ``EventSerializer.deserialize()`` call in the relay
          reconstructs the typed ``Event`` instance for bus dispatch.
        - Two entries for the same event (e.g. duplicate saves) will both be
          relayed — idempotency must be enforced by the event handler, not here.
        - ``created_at`` uses ``datetime.now(timezone.utc)`` — always UTC.

    Example::

        entry = OutboxEntry.from_event(EntityCreatedEvent(...), channel="orders")
        await outbox_repo.save(entry)
    """

    # Unique ID for this outbox row — used to delete the row after publish.
    entry_id: UUID = field(default_factory=uuid4)

    # Human-readable event type name — for logging; relay does not use it
    # to route or deserialize (payload is self-describing via __event_type__).
    event_type: str = ""

    # Logical event channel — forwarded verbatim to bus.publish(channel=...).
    channel: str = CHANNEL_DEFAULT

    # JSON-encoded event bytes ready for bus transport.
    # Pre-serialized in the service layer so the outbox table has no schema
    # dependency on the event domain model — just opaque bytes.
    payload: bytes = b""

    # UTC creation time — useful for monitoring stale entries.
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def from_event(
        cls,
        event: Event,
        *,
        channel: str = CHANNEL_DEFAULT,
        serializer: EventSerializer | None = None,
    ) -> OutboxEntry:
        """
        Construct an ``OutboxEntry`` from a domain ``Event``.

        Serializes the event to bytes using ``JsonEventSerializer`` (or the
        provided ``serializer``) so the entry is self-describing and can be
        relayed without importing the specific event class at relay time.

        Args:
            event:      The ``Event`` instance to persist in the outbox.
            channel:    Target event channel.  Defaults to ``CHANNEL_DEFAULT``.
            serializer: Custom serializer.  Defaults to ``JsonEventSerializer()``.

        Returns:
            A fully populated ``OutboxEntry`` ready for persistence.

        Edge cases:
            - If the event class uses a custom ``__event_type__``, that value
              is stored in ``event_type`` — useful for log filtering.
            - ``created_at`` is set to ``datetime.now(timezone.utc)`` at call
              time — the service layer's transaction timestamp, not the relay's.
        """
        _serializer = serializer or JsonEventSerializer()
        return cls(
            # Generate a fresh UUID for this entry — independent of event_id
            # so that retried events still have unique outbox row identifiers.
            entry_id=uuid4(),
            event_type=event.event_type_name(),
            channel=channel,
            payload=_serializer.serialize(event),
        )


# ── OutboxRepository ──────────────────────────────────────────────────────────


class OutboxRepository(ABC):
    """
    Abstract persistence contract for the outbox store.

    Implement this against your DB backend:

    - ``varco_sa``: write an ``SAOutboxRepository`` using ``AsyncSession``.
    - ``varco_beanie``: write a ``BeanieOutboxRepository`` using a Beanie doc.

    The service layer saves entries by calling ``save()`` inside the **same
    DB transaction** as the domain entity write — that is the whole point of
    the pattern.  ``get_pending()`` and ``delete()`` are called only by
    ``OutboxRelay``, not by services.

    Thread safety:  ⚠️ Implementations must not share state between concurrent
                       ``get_pending()`` callers — each relay instance should
                       have its own repository instance.
    Async safety:   ✅ All methods are ``async def``.
    """

    @abstractmethod
    async def save(self, entry: OutboxEntry) -> None:
        """
        Persist an ``OutboxEntry`` inside the caller's current DB transaction.

        Must be called within the same ``AsyncSession`` / ``AsyncUnitOfWork``
        context as the domain entity write so that both succeed or fail together.

        Args:
            entry: The outbox entry to persist.

        Raises:
            Any DB-level exception raised by the underlying driver (e.g.
            ``sqlalchemy.exc.IntegrityError`` on a duplicate ``entry_id``).

        Edge cases:
            - If the transaction is rolled back after ``save()``, the entry is
              never written — events are only published for committed changes.
            - Calling outside a transaction context is implementation-defined;
              most async drivers will auto-commit in that case.
        """

    @abstractmethod
    async def get_pending(
        self, *, limit: int = _DEFAULT_BATCH_SIZE
    ) -> list[OutboxEntry]:
        """
        Return up to ``limit`` unsent outbox entries, ordered by ``created_at``.

        The relay calls this in a polling loop.  Returning entries in creation
        order (oldest first) ensures FIFO delivery within a channel — not
        globally guaranteed, but a reasonable best-effort.

        Args:
            limit: Maximum number of entries to return per call.
                   Default is ``100`` — tune per workload.

        Returns:
            A list of ``OutboxEntry`` objects awaiting relay.  Empty list
            if no pending entries exist.

        Edge cases:
            - Implementations may return the same entry more than once if
              the relay crashes between ``get_pending()`` and ``delete()``.
              Event handlers must be idempotent.
            - Setting ``limit`` too high increases memory pressure; too low
              increases DB round trips.  Default 100 is a safe starting point.
        """

    @abstractmethod
    async def delete(self, entry_id: UUID) -> None:
        """
        Delete the outbox entry identified by ``entry_id`` after successful relay.

        Called by the relay only after the bus has accepted the event (i.e.
        ``bus.publish()`` returned without raising).  If ``publish()`` raises,
        this method is NOT called — the entry stays in the outbox for retry.

        Args:
            entry_id: The ``OutboxEntry.entry_id`` to delete.

        Edge cases:
            - Calling with an unknown ``entry_id`` should be a silent no-op
              (the entry was already deleted by a concurrent relay instance).
            - Implementations must NOT raise if the entry is not found —
              at-least-once delivery means a concurrent relay may have
              deleted it first.
        """


# ── OutboxRelay ───────────────────────────────────────────────────────────────


class OutboxRelay:
    """
    Background task that polls the outbox and publishes pending events to the bus.

    The relay runs as an ``asyncio.Task``.  On each tick it:

    1. Calls ``outbox.get_pending(limit=batch_size)``.
    2. For each entry, calls ``bus.publish(event, channel=entry.channel)``.
    3. On success, calls ``outbox.delete(entry.entry_id)``.
    4. On failure, logs the error and leaves the entry for the next tick.
    5. Sleeps for ``poll_interval`` seconds before the next tick.

    DESIGN: OutboxRelay holds AbstractEventBus directly
        The relay is infrastructure — its job is to bridge the DB to the bus.
        See module docstring for the full rationale.
        ✅ Accepted exception per project feedback (bus_access_pattern).
        ❌ If relay grows domain-level logic, move it to a producer.

    DESIGN: delete-after-publish over status flag
        Deleting the row after successful publish keeps the outbox table
        small and avoids a separate "mark as sent" UPDATE step.
        ✅ Simple — no "sent" / "failed" status column needed.
        ✅ Outbox table stays small in steady state.
        ❌ If the relay crashes between publish and delete, the event is
           published again on the next poll (at-least-once, not exactly-once).
           Event handlers must be idempotent.

    Thread safety:  ❌ Not thread-safe.  Run one relay per event loop.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        outbox:         Implementation of ``OutboxRepository``.
        bus:            ``AbstractEventBus`` to publish events to.
        serializer:     Serializer used to decode ``OutboxEntry.payload`` back
                        to a typed ``Event``.  Defaults to ``JsonEventSerializer``.
        poll_interval:  Seconds to sleep between polls.  Default ``1.0``.
        batch_size:     Max entries fetched per poll.  Default ``100``.

    Edge cases:
        - ``start()`` is idempotent — calling it twice has no effect.
        - ``stop()`` is idempotent — calling it before ``start()`` is a no-op.
        - If the bus is unavailable for an extended period, entries accumulate
          in the outbox table until the bus recovers.
        - A crash between ``publish()`` success and ``delete()`` causes the
          event to be published again on restart (at-least-once).  Design
          handlers to be idempotent (e.g. deduplicate on ``event.event_id``).

    Example::

        relay = OutboxRelay(outbox=my_outbox_repo, bus=my_bus)
        await relay.start()

        # ...app serves traffic...

        await relay.stop()
    """

    def __init__(
        self,
        *,
        outbox: OutboxRepository,
        bus: AbstractEventBus,
        serializer: EventSerializer | None = None,
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
        batch_size: int = _DEFAULT_BATCH_SIZE,
    ) -> None:
        """
        Args:
            outbox:         Outbox repository implementation.
            bus:            Event bus to publish relayed events to.
            serializer:     Event serializer for decoding payload bytes.
                            Defaults to ``JsonEventSerializer()``.
            poll_interval:  Seconds between polling cycles.  Tune this to
                            balance event delivery latency vs DB load.
            batch_size:     Max entries to fetch per polling cycle.  Tune to
                            balance memory usage vs throughput.

        Raises:
            ValueError: If ``poll_interval`` is not positive.
            ValueError: If ``batch_size`` is not positive.

        Edge cases:
            - Very small ``poll_interval`` (e.g. 0.01) increases DB load
              significantly for low-volume systems.  Use at least 0.1 in
              production; lower values are only useful for integration tests.
        """
        if poll_interval <= 0:
            raise ValueError(
                f"poll_interval must be positive, got {poll_interval!r}. "
                "Use a value >= 0.1 for production and >= 0.001 for tests."
            )
        if batch_size <= 0:
            raise ValueError(f"batch_size must be positive, got {batch_size!r}.")

        self._outbox = outbox
        self._bus = bus
        # Use provided serializer or fall back to JSON (matches the default
        # in OutboxEntry.from_event so round-trip is consistent).
        self._serializer: EventSerializer = serializer or JsonEventSerializer()
        self._poll_interval = poll_interval
        self._batch_size = batch_size

        # Lazy-created inside start() so the Lock is always bound to the
        # running event loop.  Avoids "got Future attached to a different loop".
        self._task: asyncio.Task[None] | None = None
        self._started: bool = False

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        """
        Start the background polling task.  Idempotent.

        Must be called from within a running event loop (i.e. inside an
        ``async`` function or at application startup after the loop is started).

        Edge cases:
            - Calling ``start()`` twice is safe — the second call is a no-op.
            - The task is named ``"outbox-relay"`` for visibility in
              asyncio debug logs and task monitors.
        """
        if self._started:
            return

        self._task = asyncio.create_task(
            self._relay_loop(),
            name="outbox-relay",
        )
        self._started = True
        _logger.info(
            "OutboxRelay started (poll_interval=%.2fs, batch_size=%d).",
            self._poll_interval,
            self._batch_size,
        )

    async def stop(self) -> None:
        """
        Cancel the background polling task and wait for it to finish.  Idempotent.

        Edge cases:
            - Calling ``stop()`` before ``start()`` is a silent no-op.
            - If the task is mid-publish when cancelled, the entry is NOT
              deleted (``delete()`` has not been called yet) — it will be
              retried on the next relay startup.
        """
        if not self._started:
            return

        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                # Expected cancellation — not an error.
                pass

        self._started = False
        _logger.info("OutboxRelay stopped.")

    async def __aenter__(self) -> OutboxRelay:
        """Support ``async with OutboxRelay(...) as relay:`` usage."""
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        """Stop the relay on context manager exit."""
        await self.stop()

    # ── Polling loop ──────────────────────────────────────────────────────────

    async def _relay_loop(self) -> None:
        """
        Background polling loop.  Runs until cancelled by ``stop()``.

        Each cycle:
        1. Fetches up to ``batch_size`` pending entries.
        2. Forwards each entry to the bus — failures are logged and skipped.
        3. Sleeps for ``poll_interval`` seconds before the next cycle.

        Edge cases:
            - ``asyncio.CancelledError`` propagates immediately — expected on shutdown.
            - Individual entry publish failures are logged at WARNING level
              and do NOT abort the batch — other entries in the same batch
              are still processed.
            - If ``get_pending()`` raises, the entire cycle is skipped
              and the error is logged at ERROR level — the relay keeps running.
        """
        _logger.debug("OutboxRelay polling loop started.")
        try:
            while True:
                await self._relay_once()
                # Sleep between ticks — yields control to other coroutines.
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            _logger.debug("OutboxRelay polling loop cancelled.")
            raise

    async def _relay_once(self) -> None:
        """
        Execute a single relay cycle: fetch → publish → delete.

        Separated from ``_relay_loop`` so integration tests can call it
        directly without waiting for the poll interval.

        Edge cases:
            - If ``get_pending()`` raises, the error is logged and the cycle
              is aborted.  The relay resumes on the next scheduled tick.
            - If ``bus.publish()`` raises, ``delete()`` is NOT called — the
              entry stays in the outbox for retry.
            - ``asyncio.CancelledError`` from ``bus.publish()`` propagates
              immediately — it is not treated as a retryable failure.
        """
        try:
            entries = await self._outbox.get_pending(limit=self._batch_size)
        except Exception as exc:  # noqa: BLE001
            _logger.error(
                "OutboxRelay: get_pending() failed — skipping this tick: %s",
                exc,
                exc_info=True,
            )
            return

        if not entries:
            # Nothing to do this tick — common case in low-traffic systems.
            return

        _logger.debug("OutboxRelay: relaying %d pending entries.", len(entries))

        for entry in entries:
            await self._relay_entry(entry)

    async def _relay_entry(self, entry: OutboxEntry) -> None:
        """
        Relay a single ``OutboxEntry`` to the bus and delete it on success.

        Args:
            entry: The outbox entry to relay.

        Edge cases:
            - Deserialization failure (bad payload bytes) is logged at ERROR
              level.  The entry is deleted anyway — a bad payload will never
              succeed on retry, so leaving it causes infinite retry loops.
            - If ``bus.publish()`` raises, the entry is NOT deleted and will
              be retried on the next tick.
            - ``asyncio.CancelledError`` from ``bus.publish()`` propagates.
        """
        # Deserialize the payload back to a typed Event so the bus can
        # dispatch it to typed handlers via isinstance() matching.
        try:
            event = self._serializer.deserialize(entry.payload)
        except Exception as exc:  # noqa: BLE001
            # Unrecoverable bad payload — delete and move on.  Keeping this
            # entry would cause an infinite retry loop.
            _logger.error(
                "OutboxRelay: failed to deserialize entry %s (type=%s): %s — "
                "deleting unrecoverable entry.",
                entry.entry_id,
                entry.event_type,
                exc,
                exc_info=True,
            )
            try:
                await self._outbox.delete(entry.entry_id)
            except Exception as del_exc:  # noqa: BLE001
                _logger.warning(
                    "OutboxRelay: could not delete unrecoverable entry %s: %s",
                    entry.entry_id,
                    del_exc,
                )
            return

        # Publish to the bus.  If this raises, we intentionally do NOT delete
        # the entry — the next relay tick will retry it.
        try:
            await self._bus.publish(event, channel=entry.channel)
        except asyncio.CancelledError:
            # Task cancellation — propagate immediately; do not delete entry.
            raise
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "OutboxRelay: publish failed for entry %s (type=%s, channel=%s): %s — "
                "will retry on next tick.",
                entry.entry_id,
                entry.event_type,
                entry.channel,
                exc,
                exc_info=True,
            )
            return

        # Delete AFTER successful publish.  If this delete fails, the event
        # will be re-published on the next tick — at-least-once, not
        # exactly-once.  Handlers must be idempotent.
        try:
            await self._outbox.delete(entry.entry_id)
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "OutboxRelay: delete failed for entry %s after successful publish: %s — "
                "event will be re-published on next tick (at-least-once delivery).",
                entry.entry_id,
                exc,
                exc_info=True,
            )

    def __repr__(self) -> str:
        return (
            f"OutboxRelay("
            f"started={self._started}, "
            f"poll_interval={self._poll_interval}s, "
            f"batch_size={self._batch_size})"
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "OutboxEntry",
    "OutboxRepository",
    "OutboxRelay",
]
