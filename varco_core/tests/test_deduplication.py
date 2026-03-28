"""
tests.test_deduplication
========================
Unit tests for Feature 2: Message Deduplication.

Covers:
    InMemoryDeduplicator  — set-based, FIFO eviction, asyncio.Lock
    @listen(deduplicator=...)  — handler skipped for duplicates, marked after success,
                                  NOT marked after failure

All tests use InMemoryEventBus — no broker required.
"""

from __future__ import annotations

from uuid import uuid4


from varco_core.event.base import Event
from varco_core.event.consumer import EventConsumer, listen
from varco_core.event.deduplication import InMemoryDeduplicator
from varco_core.event.memory import InMemoryEventBus


# ── Test event ─────────────────────────────────────────────────────────────────


class SampleEvent(Event):
    """Minimal event used only in deduplication tests."""

    value: int = 0


# ── Tests: InMemoryDeduplicator ────────────────────────────────────────────────


async def test_is_duplicate_returns_false_for_new_event() -> None:
    """
    A freshly constructed deduplicator has no seen events — every event_id
    is new and is_duplicate must return False.
    """
    dedup = InMemoryDeduplicator()
    event_id = uuid4()

    assert await dedup.is_duplicate(event_id) is False


async def test_is_duplicate_returns_true_after_mark_seen() -> None:
    """
    After mark_seen is called, the same event_id is recognised as a duplicate.
    """
    dedup = InMemoryDeduplicator()
    event_id = uuid4()

    await dedup.mark_seen(event_id)

    assert await dedup.is_duplicate(event_id) is True


async def test_is_duplicate_different_ids_are_independent() -> None:
    """
    Marking one event_id as seen does not affect other event_ids.
    """
    dedup = InMemoryDeduplicator()
    seen_id = uuid4()
    other_id = uuid4()

    await dedup.mark_seen(seen_id)

    assert await dedup.is_duplicate(seen_id) is True
    assert await dedup.is_duplicate(other_id) is False


async def test_mark_seen_is_idempotent() -> None:
    """
    Calling mark_seen twice on the same event_id does not raise and leaves
    the deduplicator in a consistent state.
    """
    dedup = InMemoryDeduplicator()
    event_id = uuid4()

    await dedup.mark_seen(event_id)
    await dedup.mark_seen(event_id)  # second call — must be a no-op

    assert await dedup.is_duplicate(event_id) is True
    # Only one entry should exist for the same id
    assert len(dedup._seen) == 1


async def test_max_size_eviction_removes_oldest() -> None:
    """
    When max_size is reached, the oldest-inserted entry is evicted FIFO.

    After eviction, the evicted event_id no longer appears as a duplicate,
    and the new event_id is stored instead.
    """
    dedup = InMemoryDeduplicator(max_size=3)
    ids = [uuid4() for _ in range(3)]
    for event_id in ids:
        await dedup.mark_seen(event_id)

    # All three are present
    for i in ids:
        assert await dedup.is_duplicate(i) is True

    # Adding one more triggers eviction of the first (oldest) entry
    new_id = uuid4()
    await dedup.mark_seen(new_id)

    assert await dedup.is_duplicate(ids[0]) is False  # evicted
    assert await dedup.is_duplicate(ids[1]) is True  # still present
    assert await dedup.is_duplicate(ids[2]) is True  # still present
    assert await dedup.is_duplicate(new_id) is True  # newly added


async def test_max_size_one_evicts_on_every_add() -> None:
    """
    With max_size=1, every mark_seen evicts the previous entry.
    Only the most-recently-seen event_id is retained.
    """
    dedup = InMemoryDeduplicator(max_size=1)
    first = uuid4()
    second = uuid4()

    await dedup.mark_seen(first)
    assert await dedup.is_duplicate(first) is True

    await dedup.mark_seen(second)
    assert await dedup.is_duplicate(first) is False  # evicted
    assert await dedup.is_duplicate(second) is True


async def test_repr_shows_count_and_max_size() -> None:
    """
    __repr__ shows the current size and max_size for debugging.
    """
    dedup = InMemoryDeduplicator(max_size=100)
    await dedup.mark_seen(uuid4())

    r = repr(dedup)

    assert "seen=1" in r
    assert "max_size=100" in r


# ── Tests: @listen deduplication integration ───────────────────────────────────


class _CountingConsumer(EventConsumer):
    """
    EventConsumer that counts how many times the handler was called.

    Used to verify that duplicates are skipped (call_count stays at 1 on
    re-delivery) and that failures do not mark the event as seen.
    """

    def __init__(self, bus: InMemoryEventBus, dedup: InMemoryDeduplicator) -> None:
        self._bus = bus
        self._dedup = dedup
        self.call_count: int = 0
        self.last_event: SampleEvent | None = None

    def wire(self) -> None:
        """Wire @listen methods to the bus."""
        self.register_to(self._bus)

    @listen(SampleEvent, deduplicator=None)  # overridden per test via _dedup
    async def on_event(self, event: SampleEvent) -> None:
        self.call_count += 1
        self.last_event = event


class _DeduplicatingConsumer(EventConsumer):
    """
    EventConsumer with a deduplicator wired at class-definition time.

    Separate from _CountingConsumer so the @listen entry has a deduplicator
    captured at decoration time (mimicking real-world usage).
    """

    # Class-level shared deduplicator — shared across all instances.
    # In production, the deduplicator would be injected; here it is a module-
    # level singleton for test clarity.
    _dedup: InMemoryDeduplicator  # set per-test via _build()

    def __init__(self, bus: InMemoryEventBus) -> None:
        self._bus = bus
        self.call_count: int = 0
        self.raises_on_call: bool = False

    def wire(self) -> None:
        """Wire @listen methods to the bus."""
        self.register_to(self._bus)

    @listen(SampleEvent, channel="test")
    async def on_event(self, event: SampleEvent) -> None:
        if self.raises_on_call:
            raise RuntimeError("handler intentionally failed")
        self.call_count += 1


def _build_consumer_with_dedup(
    bus: InMemoryEventBus,
    dedup: InMemoryDeduplicator,
) -> _DeduplicatingConsumer:
    """
    Build a _DeduplicatingConsumer whose @listen entry carries a deduplicator.

    We need a fresh class per-test so the deduplicator captured in the
    _ListenEntry is the test-specific instance.

    Args:
        bus:   Event bus to register to.
        dedup: Deduplicator to wire into the @listen handler.

    Returns:
        Consumer instance with the deduplicator wired.
    """

    class _Consumer(EventConsumer):
        def __init__(self) -> None:
            self.call_count = 0
            self.raises_on_call = False

        @listen(SampleEvent, channel="test", deduplicator=dedup)
        async def on_event(self, event: SampleEvent) -> None:  # type: ignore[override]
            if self.raises_on_call:
                raise RuntimeError("handler intentionally failed")
            self.call_count += 1

    consumer = _Consumer()
    consumer.register_to(bus)
    return consumer  # type: ignore[return-value]


async def test_deduplicator_skips_second_delivery() -> None:
    """
    When the same event is published twice (re-delivery scenario), the handler
    is called only once — the second delivery is suppressed.

    Verifies: deduplication check fires before handler, call_count stays at 1.
    """
    bus = InMemoryEventBus()
    dedup = InMemoryDeduplicator()
    consumer = _build_consumer_with_dedup(bus, dedup)

    event = SampleEvent(value=42)

    # First delivery — handler should run
    await bus.publish(event, channel="test")
    assert consumer.call_count == 1  # type: ignore[attr-defined]

    # Second delivery of the SAME event (same event_id) — should be suppressed
    await bus.publish(event, channel="test")
    assert consumer.call_count == 1  # type: ignore[attr-defined]  # unchanged


async def test_deduplicator_allows_different_events() -> None:
    """
    Two different events (distinct event_ids) are both processed.

    Verifies: deduplication is per event_id, not per event_type or channel.
    """
    bus = InMemoryEventBus()
    dedup = InMemoryDeduplicator()
    consumer = _build_consumer_with_dedup(bus, dedup)

    event_a = SampleEvent(value=1)
    event_b = SampleEvent(value=2)

    await bus.publish(event_a, channel="test")
    await bus.publish(event_b, channel="test")

    assert consumer.call_count == 2  # type: ignore[attr-defined]


async def test_deduplicator_does_not_mark_seen_on_handler_failure() -> None:
    """
    When the handler raises, the event is NOT marked as seen — a subsequent
    re-delivery of the same event will be processed again (not skipped).

    Verifies: mark_seen is only called on successful completion.
    """
    bus = InMemoryEventBus()
    dedup = InMemoryDeduplicator()

    class _FailingConsumer(EventConsumer):
        def __init__(self) -> None:
            self.call_count = 0

        @listen(SampleEvent, channel="test", deduplicator=dedup)
        async def on_event(self, event: SampleEvent) -> None:
            self.call_count += 1
            raise RuntimeError("intentional failure")

    consumer = _FailingConsumer()
    consumer.register_to(bus)

    event = SampleEvent(value=99)

    # First delivery — handler runs and raises (bus swallows the error)
    try:
        await bus.publish(event, channel="test")
    except Exception:
        pass

    # The event must NOT be marked as seen after a failure
    assert await dedup.is_duplicate(event.event_id) is False

    # Second delivery — handler should run again (not skipped as duplicate)
    try:
        await bus.publish(event, channel="test")
    except Exception:
        pass

    assert consumer.call_count == 2  # ran both times, not deduplicated


async def test_deduplicator_marks_seen_after_success() -> None:
    """
    After the handler completes successfully, the event is marked as seen.

    Verifies: mark_seen is called on successful handler completion.
    """
    bus = InMemoryEventBus()
    dedup = InMemoryDeduplicator()
    consumer = _build_consumer_with_dedup(bus, dedup)

    event = SampleEvent(value=7)

    await bus.publish(event, channel="test")

    # After success the event must be in the seen set
    assert await dedup.is_duplicate(event.event_id) is True
    assert consumer.call_count == 1  # type: ignore[attr-defined]


async def test_no_deduplicator_always_calls_handler() -> None:
    """
    Without a deduplicator, the same event published twice calls the handler
    twice — confirming the default (no deduplication) behaviour is unchanged.
    """
    bus = InMemoryEventBus()

    class _NoDedupConsumer(EventConsumer):
        def __init__(self) -> None:
            self.call_count = 0

        @listen(SampleEvent, channel="test")
        async def on_event(self, event: SampleEvent) -> None:
            self.call_count += 1

    consumer = _NoDedupConsumer()
    consumer.register_to(bus)

    event = SampleEvent(value=5)

    await bus.publish(event, channel="test")
    await bus.publish(event, channel="test")

    # Without dedup, both deliveries are processed
    assert consumer.call_count == 2
