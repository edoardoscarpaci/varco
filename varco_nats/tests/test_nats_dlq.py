"""
Unit tests for varco_nats.NatsDLQ
==================================
All tests fake ``nats-py`` — no real NATS broker required.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
from varco_core.event.dlq import DeadLetterEntry
from varco_nats.dlq import NatsDLQConfiguration

from tests.fakes import FakeJetStream, FakeNatsClient, OrderPlacedEvent
from varco_nats import NatsDLQ, NatsEventBusSettings

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_entry(handler_name: str = "on_order", order_id: str = "1") -> DeadLetterEntry:
    """Build a ``DeadLetterEntry`` from a simulated handler failure."""
    return DeadLetterEntry.from_failure(
        event=OrderPlacedEvent(order_id=order_id),
        channel="orders",
        handler_name=handler_name,
        last_exc=RuntimeError("boom"),
        attempts=3,
        first_failed_at=datetime.now(UTC),
    )


@pytest.fixture
def fake_js() -> FakeJetStream:
    return FakeJetStream()


@pytest.fixture
def fake_nc(fake_js: FakeJetStream) -> FakeNatsClient:
    return FakeNatsClient(fake_js)


@asynccontextmanager
async def _started_dlq(nc: FakeNatsClient) -> AsyncIterator[NatsDLQ]:
    """Build and start a ``NatsDLQ`` wired to ``nc``."""

    async def _fake_connect(**_: object) -> FakeNatsClient:
        return nc

    with patch("varco_nats.dlq.connect", new=_fake_connect):
        settings = NatsEventBusSettings(servers="nats://fake:4222")
        async with NatsDLQ(settings) as dlq:
            yield dlq


# ── Lifecycle ─────────────────────────────────────────────────────────────────


class TestLifecycle:
    async def test_stop_before_start_is_noop(self) -> None:
        dlq = NatsDLQ(NatsEventBusSettings())
        await dlq.stop()  # must not raise

    async def test_start_creates_dlq_stream(
        self, fake_nc: FakeNatsClient, fake_js: FakeJetStream
    ) -> None:
        async with _started_dlq(fake_nc):
            # The DLQ stream uses WorkQueue retention so ack deletes messages.
            assert "varco-events-dlq" in fake_js.streams
            assert fake_js.streams["varco-events-dlq"].retention == "workqueue"

    async def test_pop_batch_before_start_raises(self) -> None:
        dlq = NatsDLQ(NatsEventBusSettings())
        with pytest.raises(RuntimeError, match="start()"):
            await dlq.pop_batch()

    async def test_count_before_start_raises(self) -> None:
        dlq = NatsDLQ(NatsEventBusSettings())
        with pytest.raises(RuntimeError, match="start()"):
            await dlq.count()


# ── push ──────────────────────────────────────────────────────────────────────


class TestPush:
    async def test_push_before_start_does_not_raise(self) -> None:
        # push() NEVER raises — a not-started DLQ logs and drops the entry.
        dlq = NatsDLQ(NatsEventBusSettings())
        await dlq.push(_make_entry())  # must not raise

    async def test_push_increases_count(self, fake_nc: FakeNatsClient) -> None:
        async with _started_dlq(fake_nc) as dlq:
            assert await dlq.count() == 0
            await dlq.push(_make_entry())
            assert await dlq.count() == 1

    async def test_push_never_raises_on_publish_failure(self, fake_nc: FakeNatsClient) -> None:
        async with _started_dlq(fake_nc) as dlq:
            # Simulate a broker error mid-publish — push() must swallow it.
            async def _boom(*_: object, **__: object) -> None:
                raise RuntimeError("NATS down")

            dlq._js.publish = _boom  # type: ignore[method-assign,union-attr]
            await dlq.push(_make_entry())  # must not raise


# ── pop_batch / ack ───────────────────────────────────────────────────────────


class TestPopBatchAndAck:
    async def test_pop_batch_empty_returns_empty_list(self, fake_nc: FakeNatsClient) -> None:
        async with _started_dlq(fake_nc) as dlq:
            assert await dlq.pop_batch(limit=5) == []

    async def test_pop_batch_invalid_limit_raises(self, fake_nc: FakeNatsClient) -> None:
        async with _started_dlq(fake_nc) as dlq:
            with pytest.raises(ValueError, match="limit"):
                await dlq.pop_batch(limit=0)

    async def test_pop_batch_returns_pushed_entry(self, fake_nc: FakeNatsClient) -> None:
        async with _started_dlq(fake_nc) as dlq:
            await dlq.push(_make_entry(handler_name="on_order", order_id="42"))
            entries = await dlq.pop_batch(limit=10)
            assert len(entries) == 1
            assert entries[0].handler_name == "on_order"
            assert isinstance(entries[0].event, OrderPlacedEvent)
            assert entries[0].event.order_id == "42"

    async def test_pop_batch_does_not_remove_until_acked(self, fake_nc: FakeNatsClient) -> None:
        async with _started_dlq(fake_nc) as dlq:
            await dlq.push(_make_entry())
            await dlq.pop_batch(limit=10)
            # Fetched but not acked — still counted as pending.
            assert await dlq.count() == 1

    async def test_ack_removes_entry(self, fake_nc: FakeNatsClient) -> None:
        async with _started_dlq(fake_nc) as dlq:
            await dlq.push(_make_entry())
            entries = await dlq.pop_batch(limit=10)
            await dlq.ack(entries[0].entry_id)
            # WorkQueue retention → ack deletes the message from the stream.
            assert await dlq.count() == 0

    async def test_ack_unknown_entry_is_noop(self, fake_nc: FakeNatsClient) -> None:
        async with _started_dlq(fake_nc) as dlq:
            await dlq.ack(uuid4())  # must not raise

    async def test_multiple_entries_round_trip(self, fake_nc: FakeNatsClient) -> None:
        async with _started_dlq(fake_nc) as dlq:
            for i in range(3):
                await dlq.push(_make_entry(order_id=str(i)))
            assert await dlq.count() == 3

            entries = await dlq.pop_batch(limit=10)
            assert len(entries) == 3
            for entry in entries:
                await dlq.ack(entry.entry_id)
            assert await dlq.count() == 0


# ── DI configuration ──────────────────────────────────────────────────────────


class TestNatsDLQConfiguration:
    def test_settings_provider_returns_settings(self) -> None:
        # The sync provider is testable without a container.
        settings = NatsDLQConfiguration().nats_dlq_settings()
        assert isinstance(settings, NatsEventBusSettings)


# ── repr ──────────────────────────────────────────────────────────────────────


class TestRepr:
    async def test_repr(self, fake_nc: FakeNatsClient) -> None:
        async with _started_dlq(fake_nc) as dlq:
            r = repr(dlq)
            assert "NatsDLQ" in r
            assert "started=True" in r


# ════════════════════════════════════════════════════════════════════════════
# Plan 009, Phase 2 (R3 retention) / Phase 4 (R1 redrive) — RD-4
# ════════════════════════════════════════════════════════════════════════════
#
# RD-4: stream-backed stores (NATS JetStream) leave get/list_entries/
# delete_where raising, with a message naming JetStream's own retention
# mechanism (MaxAge) rather than a silent no-op.


class TestNatsDLQRandomAccessCapabilityFlag:
    def test_supports_random_access_is_false(self) -> None:
        dlq = NatsDLQ(NatsEventBusSettings())
        assert dlq.supports_random_access is False


class TestNatsDLQDeleteWhereRaises:
    async def test_delete_where_raises_not_implemented_naming_maxage(self) -> None:
        dlq = NatsDLQ(NatsEventBusSettings())
        with pytest.raises(NotImplementedError, match="MaxAge"):
            await dlq.delete_where(older_than=datetime.now(UTC))

    async def test_delete_where_with_no_predicate_raises_value_error(self) -> None:
        # KI-7 regression, Docker-free — the ABC's no-predicate refusal must be
        # reached BEFORE the backend-support NotImplementedError
        # (varco_nats/varco_nats/dlq.py:558-563). The inherited conformance
        # guard for this ordering is -m integration only.
        dlq = NatsDLQ(NatsEventBusSettings())
        with pytest.raises(ValueError):
            await dlq.delete_where()


class TestNatsDLQGetRaises:
    async def test_get_raises_not_implemented(self) -> None:
        dlq = NatsDLQ(NatsEventBusSettings())
        with pytest.raises(NotImplementedError):
            await dlq.get(uuid4())


class TestNatsDLQListEntriesRaises:
    async def test_list_entries_raises_not_implemented(self) -> None:
        dlq = NatsDLQ(NatsEventBusSettings())
        with pytest.raises(NotImplementedError):
            await dlq.list_entries()


class TestNatsDLQCountByChannelRaises:
    async def test_count_by_channel_raises_not_implemented(self) -> None:
        dlq = NatsDLQ(NatsEventBusSettings())
        with pytest.raises(NotImplementedError):
            await dlq.count_by_channel()
