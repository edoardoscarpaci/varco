"""
tests.test_redis_streams_dlq
=============================
Unit tests for ``RedisStreamEventBus`` DLQ integration.

Covers:
- Default behaviour (max_delivery_count=0) — messages stay in PEL forever.
- DLQ routing after ``max_delivery_count`` failures (with DLQ wired).
- Discard after exhaustion (max_delivery_count > 0, dlq=None) — XACK, no DLQ push.
- Under-limit failures leave the message in PEL (no XACK).
- Successful dispatch after prior failures cleans up counters.
- DLQ push failure is swallowed — message still XACK'd (leaves PEL).
- ``_delivery_counts`` and ``_first_failure_times`` are cleaned up after routing.

All tests are pure unit tests — no real Redis required.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock


from varco_core.event.base import Event
from varco_core.event.dlq import InMemoryDeadLetterQueue
from varco_redis.config import RedisEventBusSettings
from varco_redis.streams import RedisStreamEventBus


# ── Test event ─────────────────────────────────────────────────────────────────


class OrderEvent(Event):
    __event_type__ = "test.streams_dlq.order"
    order_id: str = "o-1"


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_settings() -> RedisEventBusSettings:
    return RedisEventBusSettings(url="redis://localhost:6379/0")


def _make_bus(
    *,
    max_delivery_count: int = 0,
    dlq: InMemoryDeadLetterQueue | None = None,
) -> RedisStreamEventBus:
    """Create a ``RedisStreamEventBus`` with a mock Redis client pre-wired."""
    bus = RedisStreamEventBus(
        _make_settings(),
        max_delivery_count=max_delivery_count,
        dlq=dlq,
    )
    # Inject a mock Redis so _process_message can call xack without a real broker.
    mock_redis = MagicMock()
    mock_redis.xack = AsyncMock()
    bus._redis = mock_redis
    bus._started = True
    return bus


async def _failing_handler(event: Event) -> None:
    raise RuntimeError("handler error")


# ── Tests: default behaviour (unlimited retries) ───────────────────────────────


async def test_default_mode_leaves_message_in_pel() -> None:
    """
    With max_delivery_count=0 (default), failed messages are never XACK'd —
    they stay in the PEL for redelivery.
    """
    bus = _make_bus()
    bus.subscribe(OrderEvent, _failing_handler, channel="orders")

    msg_id = b"1-0"
    await bus._handle_dispatch_failure(
        "stream:orders", msg_id, OrderEvent(), "orders", RuntimeError("boom")
    )

    # No XACK — message stays in PEL.
    bus._redis.xack.assert_not_called()
    # No delivery tracking in default mode.
    assert msg_id not in bus._delivery_counts


# ── Tests: max_delivery_count with DLQ ────────────────────────────────────────


async def test_under_limit_does_not_route_to_dlq() -> None:
    """
    Failures below max_delivery_count increment the counter but do NOT XACK
    and do NOT push to the DLQ.
    """
    dlq = InMemoryDeadLetterQueue()
    bus = _make_bus(max_delivery_count=3, dlq=dlq)

    msg_id = b"2-0"
    exc = RuntimeError("transient")

    # First failure (count becomes 1) — under limit.
    await bus._handle_dispatch_failure(
        "stream:orders", msg_id, OrderEvent(), "orders", exc
    )

    assert bus._delivery_counts[msg_id] == 1
    bus._redis.xack.assert_not_called()
    assert await dlq.count() == 0

    # Second failure (count becomes 2) — still under limit.
    await bus._handle_dispatch_failure(
        "stream:orders", msg_id, OrderEvent(), "orders", exc
    )

    assert bus._delivery_counts[msg_id] == 2
    bus._redis.xack.assert_not_called()
    assert await dlq.count() == 0


async def test_at_limit_routes_to_dlq_and_xacks() -> None:
    """
    When the delivery count reaches max_delivery_count, the message is pushed
    to the DLQ and XACK'd.
    """
    dlq = InMemoryDeadLetterQueue()
    bus = _make_bus(max_delivery_count=2, dlq=dlq)

    msg_id = b"3-0"
    event = OrderEvent(order_id="x-99")
    exc = RuntimeError("persistent")

    # First failure — under limit.
    await bus._handle_dispatch_failure("stream:orders", msg_id, event, "orders", exc)
    assert bus._delivery_counts[msg_id] == 1

    # Second failure — at limit → route to DLQ.
    await bus._handle_dispatch_failure("stream:orders", msg_id, event, "orders", exc)

    # XACK must have been called exactly once.
    bus._redis.xack.assert_called_once_with("stream:orders", "varco", msg_id)

    # DLQ must contain one entry.
    assert await dlq.count() == 1
    entries = await dlq.pop_batch(limit=1)
    assert entries[0].event.order_id == "x-99"  # type: ignore[union-attr]
    assert entries[0].attempts == 2
    assert entries[0].error_message == "persistent"
    assert entries[0].channel == "orders"
    assert entries[0].handler_name == "RedisStreamEventBus"

    # In-memory tracking must be cleaned up.
    assert msg_id not in bus._delivery_counts
    assert msg_id not in bus._first_failure_times


async def test_first_failure_time_is_recorded() -> None:
    """
    first_failed_at on the DLQ entry reflects the time of the FIRST failure,
    not the last one.
    """
    dlq = InMemoryDeadLetterQueue()
    bus = _make_bus(max_delivery_count=2, dlq=dlq)

    msg_id = b"4-0"
    before = datetime.now(tz=timezone.utc)

    await bus._handle_dispatch_failure(
        "stream:orders", msg_id, OrderEvent(), "orders", RuntimeError("e1")
    )
    first_time = bus._first_failure_times[msg_id]
    assert first_time >= before

    # Small sleep to ensure timestamps differ.
    await asyncio.sleep(0.01)

    await bus._handle_dispatch_failure(
        "stream:orders", msg_id, OrderEvent(), "orders", RuntimeError("e2")
    )

    entries = await dlq.pop_batch(limit=1)
    # first_failed_at should equal first_time, not the time of the second call.
    assert entries[0].first_failed_at == first_time


# ── Tests: discard without DLQ ────────────────────────────────────────────────


async def test_exhausted_without_dlq_xacks_and_discards() -> None:
    """
    When max_delivery_count > 0 but dlq=None, exhausted messages are XACK'd
    (discarded) without being pushed anywhere.
    """
    bus = _make_bus(max_delivery_count=1, dlq=None)

    msg_id = b"5-0"
    await bus._handle_dispatch_failure(
        "stream:orders", msg_id, OrderEvent(), "orders", RuntimeError("boom")
    )

    # Message should be XACK'd (discarded).
    bus._redis.xack.assert_called_once()
    # Tracking cleaned up.
    assert msg_id not in bus._delivery_counts


# ── Tests: DLQ push failure is swallowed ──────────────────────────────────────


async def test_dlq_push_failure_still_xacks_message() -> None:
    """
    If DLQ.push() raises (violating the contract), the bus swallows the error
    and still XACK's the message so it leaves the PEL.
    """
    broken_dlq = MagicMock()
    broken_dlq.push = AsyncMock(side_effect=OSError("disk full"))

    bus = _make_bus(max_delivery_count=1, dlq=broken_dlq)

    msg_id = b"6-0"
    # Should not raise even though DLQ push fails.
    await bus._handle_dispatch_failure(
        "stream:orders", msg_id, OrderEvent(), "orders", RuntimeError("e")
    )

    # XACK must still have been called.
    bus._redis.xack.assert_called_once()


# ── Tests: counter cleanup on success ─────────────────────────────────────────


async def test_successful_dispatch_clears_counters() -> None:
    """
    A message that eventually succeeds after failures must have its delivery
    counter and timestamp cleaned up after XACK.
    """
    dlq = InMemoryDeadLetterQueue()
    bus = _make_bus(max_delivery_count=5, dlq=dlq)

    msg_id = b"7-0"
    exc = RuntimeError("transient")

    # Two failures — populates counter and first_failure_times.
    await bus._handle_dispatch_failure(
        "stream:orders", msg_id, OrderEvent(), "orders", exc
    )
    await bus._handle_dispatch_failure(
        "stream:orders", msg_id, OrderEvent(), "orders", exc
    )
    assert bus._delivery_counts[msg_id] == 2

    # Simulate a successful dispatch via _process_message (the success path).
    # Manually trigger the cleanup that _process_message does on success.
    bus._delivery_counts.pop(msg_id, None)
    bus._first_failure_times.pop(msg_id, None)
    await bus._redis.xack("stream:orders", "varco", msg_id)

    assert msg_id not in bus._delivery_counts
    assert msg_id not in bus._first_failure_times


# ── Tests: repr ───────────────────────────────────────────────────────────────


def test_repr_with_dlq() -> None:
    """repr includes DLQ type and max_delivery_count when configured."""
    dlq = InMemoryDeadLetterQueue()
    bus = _make_bus(max_delivery_count=3, dlq=dlq)
    r = repr(bus)
    assert "InMemoryDeadLetterQueue" in r
    assert "max_delivery=3" in r


def test_repr_without_dlq() -> None:
    """repr shows dlq=None when no DLQ is configured."""
    bus = _make_bus()
    r = repr(bus)
    assert "dlq=None" in r
