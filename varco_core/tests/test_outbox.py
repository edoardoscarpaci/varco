"""
Unit tests for varco_core.service.outbox
==========================================
Covers the transactional outbox pattern components without any external dependencies.

Sections
--------
- ``OutboxEntry``       — construction, from_event factory, frozen, serialization
- ``OutboxRelay``       — construction validation, start/stop idempotent, relay_once
                          publishes and deletes, publish failure leaves entry,
                          bad payload deletes entry, context manager
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from varco_core.event import Event, InMemoryEventBus
from varco_core.event.base import CHANNEL_DEFAULT
from varco_core.service.outbox import OutboxEntry, OutboxRepository, OutboxRelay

if TYPE_CHECKING:
    pass


# ── Minimal event fixture ─────────────────────────────────────────────────────


class OrderPlacedEvent(Event):
    __event_type__ = "test.order.placed"
    order_id: str = "ord-1"


# ── Minimal in-memory OutboxRepository ───────────────────────────────────────


class InMemoryOutboxRepository(OutboxRepository):
    """
    Pure in-memory implementation for testing.

    Not thread-safe — tests are single-threaded.
    """

    def __init__(self) -> None:
        self._entries: list[OutboxEntry] = []

    async def save(self, entry: OutboxEntry) -> None:
        self._entries.append(entry)

    async def get_pending(self, *, limit: int = 100) -> list[OutboxEntry]:
        return list(self._entries[:limit])

    async def delete(self, entry_id: UUID) -> None:
        self._entries = [e for e in self._entries if e.entry_id != entry_id]


# ── OutboxEntry ───────────────────────────────────────────────────────────────


class TestOutboxEntry:
    def test_has_uuid_entry_id(self) -> None:
        entry = OutboxEntry(
            event_type="test.event",
            channel="orders",
            payload=b'{"test": 1}',
        )
        assert isinstance(entry.entry_id, uuid.UUID)

    def test_from_event_sets_event_type(self) -> None:
        event = OrderPlacedEvent()
        entry = OutboxEntry.from_event(event, channel="orders")
        assert entry.event_type == "test.order.placed"

    def test_from_event_sets_channel(self) -> None:
        event = OrderPlacedEvent()
        entry = OutboxEntry.from_event(event, channel="custom-channel")
        assert entry.channel == "custom-channel"

    def test_from_event_default_channel(self) -> None:
        event = OrderPlacedEvent()
        entry = OutboxEntry.from_event(event)
        assert entry.channel == CHANNEL_DEFAULT

    def test_from_event_payload_is_bytes(self) -> None:
        event = OrderPlacedEvent()
        entry = OutboxEntry.from_event(event, channel="orders")
        assert isinstance(entry.payload, bytes)
        assert len(entry.payload) > 0

    def test_from_event_payload_is_non_empty_json(self) -> None:
        import json

        event = OrderPlacedEvent()
        entry = OutboxEntry.from_event(event, channel="orders")
        data = json.loads(entry.payload)
        # The JSON envelope must contain the event type
        assert "__event_type__" in data
        assert data["__event_type__"] == "test.order.placed"

    def test_from_event_entry_id_is_unique(self) -> None:
        event = OrderPlacedEvent()
        a = OutboxEntry.from_event(event, channel="orders")
        b = OutboxEntry.from_event(event, channel="orders")
        assert a.entry_id != b.entry_id

    def test_frozen(self) -> None:
        entry = OutboxEntry(event_type="x", channel="y", payload=b"z")
        with pytest.raises(Exception):
            entry.channel = "other"  # type: ignore[misc]

    def test_created_at_is_utc(self) -> None:
        before = datetime.now(tz=timezone.utc)
        entry = OutboxEntry.from_event(OrderPlacedEvent(), channel="orders")
        after = datetime.now(tz=timezone.utc)
        assert before <= entry.created_at <= after


# ── OutboxRelay — construction ─────────────────────────────────────────────────


class TestOutboxRelayConstruction:
    def test_invalid_poll_interval_raises(self) -> None:
        repo = InMemoryOutboxRepository()
        bus = InMemoryEventBus()
        with pytest.raises(ValueError, match="poll_interval"):
            OutboxRelay(outbox=repo, bus=bus, poll_interval=0.0)

    def test_negative_poll_interval_raises(self) -> None:
        repo = InMemoryOutboxRepository()
        bus = InMemoryEventBus()
        with pytest.raises(ValueError, match="poll_interval"):
            OutboxRelay(outbox=repo, bus=bus, poll_interval=-1.0)

    def test_invalid_batch_size_raises(self) -> None:
        repo = InMemoryOutboxRepository()
        bus = InMemoryEventBus()
        with pytest.raises(ValueError, match="batch_size"):
            OutboxRelay(outbox=repo, bus=bus, batch_size=0)

    def test_repr_contains_class_name(self) -> None:
        relay = OutboxRelay(outbox=InMemoryOutboxRepository(), bus=InMemoryEventBus())
        assert "OutboxRelay" in repr(relay)


# ── OutboxRelay — lifecycle ────────────────────────────────────────────────────


class TestOutboxRelayLifecycle:
    async def test_start_stop_idempotent(self) -> None:
        relay = OutboxRelay(
            outbox=InMemoryOutboxRepository(),
            bus=InMemoryEventBus(),
            poll_interval=0.01,
        )
        await relay.start()
        await relay.start()  # second start must be a no-op
        await relay.stop()
        await relay.stop()  # second stop must be a no-op

    async def test_stop_before_start_is_noop(self) -> None:
        relay = OutboxRelay(
            outbox=InMemoryOutboxRepository(),
            bus=InMemoryEventBus(),
        )
        await relay.stop()  # must not raise

    async def test_context_manager_starts_and_stops(self) -> None:
        relay = OutboxRelay(
            outbox=InMemoryOutboxRepository(),
            bus=InMemoryEventBus(),
            poll_interval=0.01,
        )
        async with relay:
            assert relay._started
        assert not relay._started


# ── OutboxRelay — relay_once ───────────────────────────────────────────────────


class TestOutboxRelayOnce:
    async def _setup(
        self,
    ) -> tuple[InMemoryOutboxRepository, InMemoryEventBus, OutboxRelay]:
        repo = InMemoryOutboxRepository()
        bus = InMemoryEventBus()
        relay = OutboxRelay(outbox=repo, bus=bus, poll_interval=0.01)
        return repo, bus, relay

    async def test_relay_once_publishes_pending_entry(self) -> None:
        repo, bus, relay = await self._setup()
        event = OrderPlacedEvent()
        await repo.save(OutboxEntry.from_event(event, channel="orders"))

        received: list[Event] = []

        async def handler(e: Event) -> None:
            received.append(e)

        bus.subscribe(OrderPlacedEvent, handler, channel="orders")

        await relay._relay_once()
        await asyncio.sleep(0)  # let SYNC bus tasks settle

        # Entry must be deleted after publish
        assert len(repo._entries) == 0

    async def test_relay_once_deletes_entry_after_success(self) -> None:
        repo, bus, relay = await self._setup()
        await repo.save(OutboxEntry.from_event(OrderPlacedEvent(), channel="orders"))
        await relay._relay_once()
        assert len(repo._entries) == 0

    async def test_relay_once_with_empty_outbox_does_nothing(self) -> None:
        repo, bus, relay = await self._setup()
        # No entries — must not raise
        await relay._relay_once()

    async def test_relay_once_keeps_entry_on_publish_failure(self) -> None:
        """If bus.publish() raises, the entry must NOT be deleted (for retry)."""
        repo = InMemoryOutboxRepository()

        class FailingBus(InMemoryEventBus):
            async def publish(
                self, event: Event, *, channel: str = CHANNEL_DEFAULT
            ) -> None:
                raise RuntimeError("broker down")

        relay = OutboxRelay(outbox=repo, bus=FailingBus(), poll_interval=0.01)
        await repo.save(OutboxEntry.from_event(OrderPlacedEvent(), channel="orders"))

        await relay._relay_once()

        # Entry must remain — will be retried on next tick
        assert len(repo._entries) == 1

    async def test_relay_once_deletes_undeserializable_entry(self) -> None:
        """A bad payload causes an unrecoverable error — entry must be deleted to prevent infinite retry."""
        repo = InMemoryOutboxRepository()
        bus = InMemoryEventBus()
        relay = OutboxRelay(outbox=repo, bus=bus, poll_interval=0.01)

        # Manually add an entry with garbage payload
        bad_entry = OutboxEntry(
            event_type="test.broken",
            channel="orders",
            payload=b"NOT_VALID_JSON{{{",
        )
        await repo.save(bad_entry)

        await relay._relay_once()

        # Must be deleted even though it could not be deserialized
        assert len(repo._entries) == 0

    async def test_relay_once_processes_multiple_entries(self) -> None:
        repo, bus, relay = await self._setup()
        for _ in range(5):
            await repo.save(
                OutboxEntry.from_event(OrderPlacedEvent(), channel="orders")
            )

        await relay._relay_once()
        assert len(repo._entries) == 0
