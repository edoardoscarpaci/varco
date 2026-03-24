"""
Integration tests for varco_redis.dlq
=======================================
These tests spin up a real Redis instance via testcontainers and verify
end-to-end push/pop/ack/count behaviour of ``RedisDLQ``.

DISABLED BY DEFAULT — requires Docker.  Run with::

    pytest -m integration tests/test_redis_dlq_integration.py

Or set the ``VARCO_RUN_INTEGRATION`` env var::

    VARCO_RUN_INTEGRATION=1 pytest tests/test_redis_dlq_integration.py

Prerequisites:
    - Docker daemon running
    - testcontainers[redis] installed (see pyproject.toml dev dependencies)
"""

from __future__ import annotations

import os
import uuid

import pytest

from varco_core.event import Event
from varco_core.event.dlq import DeadLetterEntry

pytestmark = pytest.mark.integration

if not os.environ.get("VARCO_RUN_INTEGRATION"):
    pytest.skip(
        "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration",
        allow_module_level=True,
    )


# ── Test event types ────────────────────────────────────────────────────────────


class OrderPlacedEvent(Event):
    __event_type__ = "test.order.placed.redis_dlq_integration"
    order_id: str = "ord-1"


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def redis_container():
    """Start a Redis instance for the integration test module."""
    from testcontainers.redis import RedisContainer

    with RedisContainer() as redis:
        yield redis


@pytest.fixture
async def dlq(redis_container):
    """Connected ``RedisDLQ`` backed by the testcontainers Redis instance."""
    from varco_redis.config import RedisEventBusSettings
    from varco_redis.dlq import RedisDLQ

    # Use a unique key prefix per test run to avoid cross-test interference.
    prefix = f"test:{uuid.uuid4().hex[:8]}:"
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    url = f"redis://{host}:{port}/0"

    settings = RedisEventBusSettings(url=url, channel_prefix=prefix)
    async with RedisDLQ(settings) as d:
        yield d


def _make_entry(handler_name: str = "H.handle") -> DeadLetterEntry:
    return DeadLetterEntry(
        event=OrderPlacedEvent(),
        channel="orders",
        handler_name=handler_name,
        error_type="ValueError",
        error_message="integration test error",
        attempts=1,
    )


# ── Tests ──────────────────────────────────────────────────────────────────────


class TestRedisDLQIntegration:
    async def test_push_increases_count(self, dlq) -> None:
        assert await dlq.count() == 0
        await dlq.push(_make_entry())
        assert await dlq.count() == 1

    async def test_push_multiple_increases_count(self, dlq) -> None:
        for _ in range(3):
            await dlq.push(_make_entry())
        assert await dlq.count() == 3

    async def test_pop_batch_returns_pushed_entry(self, dlq) -> None:
        entry = _make_entry("OrderConsumer.on_order")
        await dlq.push(entry)

        result = await dlq.pop_batch(limit=10)
        assert any(e.handler_name == "OrderConsumer.on_order" for e in result)

    async def test_pop_batch_does_not_remove_entries(self, dlq) -> None:
        await dlq.push(_make_entry())
        count_before = await dlq.count()
        await dlq.pop_batch(limit=10)
        count_after = await dlq.count()
        assert count_after == count_before  # entries remain until ack()

    async def test_ack_removes_entry(self, dlq) -> None:
        entry = _make_entry()
        await dlq.push(entry)
        assert await dlq.count() == 1

        await dlq.ack(entry.entry_id)
        assert await dlq.count() == 0

    async def test_ack_unknown_id_is_noop(self, dlq) -> None:
        await dlq.ack(uuid.uuid4())  # must not raise

    async def test_roundtrip_event_type_preserved(self, dlq) -> None:
        """The event nested in DeadLetterEntry must survive Redis serialization."""
        entry = _make_entry()
        await dlq.push(entry)

        result = await dlq.pop_batch(limit=1)
        assert len(result) == 1
        assert isinstance(result[0].event, OrderPlacedEvent)

    async def test_fifo_order(self, dlq) -> None:
        """Oldest entries (lowest score) must be returned first."""
        import asyncio

        e1 = _make_entry("first")
        await asyncio.sleep(0.01)
        e2 = _make_entry("second")

        await dlq.push(e1)
        await dlq.push(e2)

        result = await dlq.pop_batch(limit=10)
        handler_names = [e.handler_name for e in result]
        assert handler_names.index("first") < handler_names.index("second")
