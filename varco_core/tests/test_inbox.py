"""
tests.test_inbox
================
Unit tests for Feature 3: Inbox Pattern.

Covers:
    InboxEntry            — construction, from_event classmethod
    InboxRepository ABC   — cannot instantiate directly
    InboxPoller           — relay_once, relay_entry, mark_processed on success,
                            entry left unprocessed on failure
    _make_inbox_wrapper   — save-before / mark-after integration via @listen
    @listen(inbox=...)    — wrapper stacking: inbox(retry(handler))

All tests use InMemoryEventBus — no broker required.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from varco_core.event.base import Event
from varco_core.event.consumer import EventConsumer, listen
from varco_core.event.memory import InMemoryEventBus
from varco_core.service.inbox import (
    InboxEntry,
    InboxPoller,
    InboxRepository,
    _make_inbox_wrapper,
)


# ── Test event ─────────────────────────────────────────────────────────────────


class InboxTestEvent(Event):
    """Minimal event used only in inbox tests — name avoids registry collision."""

    value: int = 0


# ── InMemoryInboxRepository — test double ─────────────────────────────────────


class InMemoryInboxRepository(InboxRepository):
    """
    Fully in-memory ``InboxRepository`` for unit tests.

    Stores entries in a plain dict; ``mark_processed`` sets ``processed_at``
    by replacing the entry with a new frozen dataclass.  No DB required.

    Thread safety:  ❌ Not thread-safe — single-loop test use only.
    Async safety:   ✅ Trivially async — no I/O.
    """

    def __init__(self) -> None:
        # Maps entry_id → InboxEntry (frozen dataclass).
        self._entries: dict[UUID, InboxEntry] = {}

        # Tracks calls so tests can inspect interaction counts.
        self.save_calls: list[InboxEntry] = []
        self.mark_processed_calls: list[UUID] = []

        # When truthy, mark_processed raises — used to test failure path.
        self.mark_processed_raises: bool = False

    async def save(self, entry: InboxEntry) -> None:
        """
        Persist the entry in memory.

        Args:
            entry: The ``InboxEntry`` to persist.
        """
        self._entries[entry.entry_id] = entry
        self.save_calls.append(entry)

    async def mark_processed(self, entry_id: UUID) -> None:
        """
        Mark the entry processed.  No-op if unknown.  Optionally raises.

        Args:
            entry_id: The entry to mark.

        Raises:
            RuntimeError: If ``mark_processed_raises`` is set — for failure tests.
        """
        if self.mark_processed_raises:
            raise RuntimeError("mark_processed intentionally failed")

        self.mark_processed_calls.append(entry_id)

        if entry_id not in self._entries:
            return  # Unknown entry — idempotent no-op

        original = self._entries[entry_id]
        from datetime import datetime, timezone

        # Replace with a new frozen instance with processed_at set.
        # dataclasses.replace() is used here because InboxEntry is frozen.
        from dataclasses import replace

        self._entries[entry_id] = replace(
            original, processed_at=datetime.now(timezone.utc)
        )

    async def get_unprocessed(self, *, limit: int = 100) -> list[InboxEntry]:
        """
        Return all unprocessed entries (processed_at is None).

        Args:
            limit: Maximum number of entries to return.

        Returns:
            List of entries with ``processed_at=None``, oldest-first.
        """
        unprocessed = [e for e in self._entries.values() if e.processed_at is None]
        # Sort by received_at — mirrors production behaviour (FIFO replay).
        unprocessed.sort(key=lambda e: e.received_at)
        return unprocessed[:limit]

    def is_processed(self, entry_id: UUID) -> bool:
        """
        Helper for assertions: True if the entry was marked processed.

        Args:
            entry_id: The entry to check.

        Returns:
            ``True`` if ``processed_at`` is set; ``False`` if still pending.
        """
        entry = self._entries.get(entry_id)
        return entry is not None and entry.processed_at is not None


# ── Tests: InboxEntry ──────────────────────────────────────────────────────────


async def test_inbox_entry_from_event_sets_fields() -> None:
    """
    ``from_event`` constructs an entry with the correct event_type and channel.

    Verifies: event_type = Event.event_type_name(), channel stored, processed_at=None.
    """
    event = InboxTestEvent(value=7)
    entry = InboxEntry.from_event(event, channel="orders")

    assert entry.event_type == event.event_type_name()
    assert entry.channel == "orders"
    assert entry.processed_at is None
    assert isinstance(entry.entry_id, UUID)
    assert isinstance(entry.payload, bytes)
    assert len(entry.payload) > 0


async def test_inbox_entry_has_unique_entry_id() -> None:
    """
    Two entries from the same event have different entry_ids.

    Verifies: entry_id is independent of event.event_id.
    """
    event = InboxTestEvent(value=1)
    entry_a = InboxEntry.from_event(event, channel="c")
    entry_b = InboxEntry.from_event(event, channel="c")

    assert entry_a.entry_id != entry_b.entry_id
    # Both entries reference the same logical event but are independent rows.
    assert entry_a.event_type == entry_b.event_type


async def test_inbox_entry_is_frozen() -> None:
    """
    ``InboxEntry`` is frozen — mutation raises ``FrozenInstanceError``.

    Verifies: immutability contract; safe to cache and hash.
    """
    import dataclasses

    entry = InboxEntry.from_event(InboxTestEvent(), channel="x")

    with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
        entry.channel = "mutated"  # type: ignore[misc]


# ── Tests: InMemoryInboxRepository ────────────────────────────────────────────


async def test_inbox_repo_save_then_get_unprocessed() -> None:
    """
    Saved entries appear in get_unprocessed when processed_at is None.
    """
    repo = InMemoryInboxRepository()
    event = InboxTestEvent(value=10)
    entry = InboxEntry.from_event(event, channel="orders")

    await repo.save(entry)

    unprocessed = await repo.get_unprocessed()
    assert len(unprocessed) == 1
    assert unprocessed[0].entry_id == entry.entry_id


async def test_inbox_repo_mark_processed_hides_entry() -> None:
    """
    After mark_processed, the entry no longer appears in get_unprocessed.
    """
    repo = InMemoryInboxRepository()
    event = InboxTestEvent(value=5)
    entry = InboxEntry.from_event(event, channel="c")

    await repo.save(entry)
    await repo.mark_processed(entry.entry_id)

    unprocessed = await repo.get_unprocessed()
    assert unprocessed == []
    assert repo.is_processed(entry.entry_id)


async def test_inbox_repo_mark_processed_unknown_is_noop() -> None:
    """
    mark_processed on an unknown entry_id is idempotent — no exception.
    """
    repo = InMemoryInboxRepository()
    await repo.mark_processed(uuid4())  # Must not raise


# ── Tests: _make_inbox_wrapper ─────────────────────────────────────────────────


async def test_inbox_wrapper_saves_before_handler() -> None:
    """
    The inbox wrapper calls ``inbox.save`` BEFORE the handler runs.

    Verifies: save is called even if the handler fails — entry always recorded.
    """
    repo = InMemoryInboxRepository()
    event = InboxTestEvent(value=1)
    handler_called = False

    async def handler(e: Event) -> None:
        nonlocal handler_called
        # At this point, the entry must already be saved.
        assert len(repo.save_calls) == 1
        handler_called = True

    wrapped = _make_inbox_wrapper(handler, repo, "ch")
    await wrapped(event)  # type: ignore[operator]

    assert handler_called
    assert len(repo.save_calls) == 1


async def test_inbox_wrapper_marks_processed_after_success() -> None:
    """
    After the handler returns without raising, ``mark_processed`` is called.
    """
    repo = InMemoryInboxRepository()
    event = InboxTestEvent(value=2)

    async def handler(e: Event) -> None:
        pass  # Succeeds

    wrapped = _make_inbox_wrapper(handler, repo, "ch")
    await wrapped(event)  # type: ignore[operator]

    assert len(repo.mark_processed_calls) == 1
    saved_entry_id = repo.save_calls[0].entry_id
    assert repo.mark_processed_calls[0] == saved_entry_id


async def test_inbox_wrapper_does_not_mark_processed_on_handler_failure() -> None:
    """
    When the handler raises, ``mark_processed`` is NOT called.

    The entry stays unprocessed so ``InboxPoller`` can replay the event.
    """
    repo = InMemoryInboxRepository()
    event = InboxTestEvent(value=3)

    async def failing_handler(e: Event) -> None:
        raise RuntimeError("handler failed")

    wrapped = _make_inbox_wrapper(failing_handler, repo, "ch")

    with pytest.raises(RuntimeError, match="handler failed"):
        await wrapped(event)  # type: ignore[operator]

    assert len(repo.save_calls) == 1  # Save DID happen before handler
    assert len(repo.mark_processed_calls) == 0  # NOT marked

    # Entry is still unprocessed — InboxPoller will replay it.
    unprocessed = await repo.get_unprocessed()
    assert len(unprocessed) == 1


async def test_inbox_wrapper_continues_on_mark_processed_failure() -> None:
    """
    If ``mark_processed`` raises, the wrapper does NOT re-raise.

    The handler already succeeded — its side-effects are committed.
    The warning is logged; InboxPoller replays and re-marks on the next tick.
    """
    repo = InMemoryInboxRepository()
    repo.mark_processed_raises = True  # Simulate DB outage on mark step
    event = InboxTestEvent(value=4)
    handler_ran = False

    async def handler(e: Event) -> None:
        nonlocal handler_ran
        handler_ran = True

    wrapped = _make_inbox_wrapper(handler, repo, "ch")

    # Must NOT raise — mark_processed failure is swallowed.
    await wrapped(event)  # type: ignore[operator]

    assert handler_ran
    # mark_processed raised — entry stays unprocessed.
    assert len(repo.mark_processed_calls) == 0


# ── Tests: InboxPoller ────────────────────────────────────────────────────────


async def test_inbox_poller_relay_once_marks_processed() -> None:
    """
    ``_relay_once`` fetches unprocessed entries, publishes to bus, and marks them.

    Verifies: the full relay cycle under nominal conditions.
    """
    repo = InMemoryInboxRepository()
    bus = InMemoryEventBus()
    poller = InboxPoller(inbox=repo, bus=bus)

    # Pre-seed an unprocessed entry.
    event = InboxTestEvent(value=99)
    entry = InboxEntry.from_event(event, channel="orders")
    await repo.save(entry)

    # Run one relay tick.
    await poller._relay_once()

    # Entry must be marked processed.
    assert repo.is_processed(entry.entry_id)


async def test_inbox_poller_relay_once_publish_failure_leaves_unprocessed() -> None:
    """
    When bus.publish fails, the entry is NOT marked processed.

    Verifies: at-least-once semantics — failed relay entries are retried next tick.
    """
    from unittest.mock import AsyncMock, patch

    repo = InMemoryInboxRepository()
    bus = InMemoryEventBus()
    poller = InboxPoller(inbox=repo, bus=bus)

    event = InboxTestEvent(value=7)
    entry = InboxEntry.from_event(event, channel="orders")
    await repo.save(entry)

    # Patch bus.publish to raise — simulates broker outage.
    with patch.object(
        bus, "publish", new=AsyncMock(side_effect=RuntimeError("broker down"))
    ):
        await poller._relay_once()

    # Entry must remain unprocessed.
    assert not repo.is_processed(entry.entry_id)
    unprocessed = await repo.get_unprocessed()
    assert len(unprocessed) == 1


async def test_inbox_poller_relay_once_empty_inbox_is_noop() -> None:
    """
    An empty inbox causes ``_relay_once`` to return immediately (no-op).

    Verifies: no unnecessary DB or bus calls when the inbox is empty.
    """
    repo = InMemoryInboxRepository()
    bus = InMemoryEventBus()
    poller = InboxPoller(inbox=repo, bus=bus)

    # Should not raise and should make no calls.
    await poller._relay_once()
    assert repo.save_calls == []


async def test_inbox_poller_start_stop() -> None:
    """
    ``start()`` creates a background task; ``stop()`` cancels it cleanly.

    Verifies: lifecycle contract — no RuntimeError or unhandled CancelledError.
    """
    repo = InMemoryInboxRepository()
    bus = InMemoryEventBus()
    poller = InboxPoller(inbox=repo, bus=bus, poll_interval=10.0)

    await poller.start()
    assert poller._task is not None

    await poller.stop()
    assert poller._task is None


async def test_inbox_poller_start_twice_raises() -> None:
    """
    Calling ``start()`` a second time without ``stop()`` raises ``RuntimeError``.

    Verifies: double-start guard prevents ghost polling tasks.
    """
    repo = InMemoryInboxRepository()
    bus = InMemoryEventBus()
    poller = InboxPoller(inbox=repo, bus=bus, poll_interval=10.0)

    await poller.start()
    try:
        with pytest.raises(RuntimeError, match="start\\(\\) called twice"):
            await poller.start()
    finally:
        await poller.stop()


async def test_inbox_poller_context_manager() -> None:
    """
    ``async with InboxPoller(...)`` starts on entry and stops on exit.
    """
    repo = InMemoryInboxRepository()
    bus = InMemoryEventBus()

    async with InboxPoller(inbox=repo, bus=bus, poll_interval=10.0) as poller:
        assert poller._task is not None

    assert poller._task is None


# ── Tests: @listen(inbox=...) integration ─────────────────────────────────────


async def test_listen_with_inbox_saves_and_marks_on_success() -> None:
    """
    With ``inbox=`` wired, publishing an event saves and marks it processed.

    Verifies: the wrapper is built at ``register_to()`` time and the full
    save-before / mark-after cycle runs through the bus dispatch path.
    """
    bus = InMemoryEventBus()
    repo = InMemoryInboxRepository()

    class _Consumer(EventConsumer):
        def __init__(self) -> None:
            self.call_count = 0

        @listen(InboxTestEvent, channel="orders", inbox=repo)
        async def on_order(self, event: InboxTestEvent) -> None:
            self.call_count += 1

    consumer = _Consumer()
    consumer.register_to(bus)

    event = InboxTestEvent(value=42)
    await bus.publish(event, channel="orders")

    assert consumer.call_count == 1
    assert len(repo.save_calls) == 1
    assert len(repo.mark_processed_calls) == 1
    assert repo.is_processed(repo.save_calls[0].entry_id)


async def test_listen_with_inbox_does_not_mark_on_failure() -> None:
    """
    When the handler raises, the inbox entry is NOT marked processed.

    Verifies: InboxPoller can replay the event on next tick.
    """
    bus = InMemoryEventBus()
    repo = InMemoryInboxRepository()

    class _FailingConsumer(EventConsumer):
        def __init__(self) -> None:
            self.call_count = 0

        @listen(InboxTestEvent, channel="orders", inbox=repo)
        async def on_order(self, event: InboxTestEvent) -> None:
            self.call_count += 1
            raise RuntimeError("handler failed")

    consumer = _FailingConsumer()
    consumer.register_to(bus)

    event = InboxTestEvent(value=5)

    try:
        await bus.publish(event, channel="orders")
    except Exception:
        pass  # Bus may re-raise depending on error policy

    assert consumer.call_count == 1
    assert len(repo.save_calls) == 1  # Save happened
    assert len(repo.mark_processed_calls) == 0  # NOT marked
    unprocessed = await repo.get_unprocessed()
    assert len(unprocessed) == 1


async def test_listen_inbox_stacks_with_deduplicator() -> None:
    """
    ``inbox=`` and ``deduplicator=`` can be combined — dedup check is inside the inbox wrapper.

    First delivery: saved, handler runs, marked processed, marked seen in dedup.
    Second delivery: saved again (new entry), but handler is SKIPPED (dedup hit).
    """
    from varco_core.event.deduplication import InMemoryDeduplicator

    bus = InMemoryEventBus()
    repo = InMemoryInboxRepository()
    dedup = InMemoryDeduplicator()

    class _Consumer(EventConsumer):
        def __init__(self) -> None:
            self.call_count = 0

        @listen(InboxTestEvent, channel="orders", inbox=repo, deduplicator=dedup)
        async def on_order(self, event: InboxTestEvent) -> None:
            self.call_count += 1

    consumer = _Consumer()
    consumer.register_to(bus)

    event = InboxTestEvent(value=1)

    # First delivery — handler runs.
    await bus.publish(event, channel="orders")
    assert consumer.call_count == 1

    # Second delivery — dedup suppresses the handler, but inbox.save is still called
    # (inbox is outermost — it records intent before dedup check runs inside).
    await bus.publish(event, channel="orders")

    # Handler ran exactly once despite two deliveries.
    assert consumer.call_count == 1

    # Two inbox.save calls (one per delivery) — inbox always records receipt.
    assert len(repo.save_calls) == 2

    # mark_processed called once — only the first delivery completed the handler.
    # The second inbox entry was saved but the handler was skipped — mark_processed
    # is called by _make_inbox_wrapper AFTER the inner handler (retry_wrapper)
    # returns.  The retry_wrapper returns without calling the handler for duplicates,
    # so mark_processed IS called for the second entry too (inner returns None = success).
    # Both entries are marked processed because the dedup check counts as success.
    assert len(repo.mark_processed_calls) == 2
