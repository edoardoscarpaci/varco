"""
EventBusConformance — shared contract tests for ``AbstractEventBus``
implementations (Plan 012 / RT6, Step 22).

Subclass and override the ``bus`` fixture to opt a backend in::

    from varco_conformance.event_bus import EventBusConformance

    class TestRedisEventBusConformance(EventBusConformance):
        @pytest.fixture
        async def bus(self, redis_url):
            async with RedisEventBus(RedisEventBusSettings(url=redis_url)) as bus:
                yield bus

Not named ``Test*`` — never collected standalone (see package docstring).
"""

from __future__ import annotations

import asyncio
from typing import ClassVar
from uuid import uuid4

import pytest
from varco_core.event.base import AbstractEventBus, Event

# Some brokers (Kafka in particular) make subscribe() -> publish() delivery
# genuinely asynchronous — the consumer group join / partition assignment
# has not necessarily completed by the time subscribe() returns, so a
# publish() issued immediately afterward can race the consumer's readiness.
# This is not a contract violation (AbstractEventBus.subscribe() makes no
# synchronous-delivery guarantee for any backend); it is a real
# characteristic of message-broker-backed buses. Poll for delivery instead
# of a single-shot assertion — zero added latency for backends where
# delivery is already synchronous (the loop exits on the first check).
_DELIVERY_TIMEOUT = 30.0
_DELIVERY_POLL_INTERVAL = 0.1
# The subscribe() -> publish() settle is NOT free for synchronous backends:
# `asyncio.sleep(2.0)` always costs 2 s of wall clock (3 × 2 s per unit run
# before Plan 059). It is therefore a per-class knob,
# `EventBusConformance.subscribe_settle_seconds`, which in-process buses set
# to 0.0.


async def _wait_until(predicate, timeout: float = _DELIVERY_TIMEOUT) -> None:
    """Poll ``predicate()`` until truthy or ``timeout`` elapses (no-op if
    already true on the first check)."""
    deadline = asyncio.get_event_loop().time() + timeout
    while not predicate():
        if asyncio.get_event_loop().time() >= deadline:
            return
        await asyncio.sleep(_DELIVERY_POLL_INTERVAL)


class _ConformanceEventA(Event):
    __event_type__ = "conformance.a"
    value: str = ""


class _ConformanceEventB(Event):
    __event_type__ = "conformance.b"
    value: str = ""


class EventBusConformance:
    """
    Shared behavioural contract for ``AbstractEventBus``.

    Every test below namespaces its channel with a fresh ``uuid4()`` suffix
    so that backends whose ``bus`` fixture is backed by a session-scoped
    shared broker container (Plan 012 Step 8's isolation rule) never
    collide across tests or across concurrent CI shards.

    Edge cases:
        ``subscribe_settle_seconds`` (default ``2.0``, inherited by every
        broker subclass) is the wait between ``subscribe()`` and
        ``publish()``. An in-process bus that delivers synchronously sets it
        to ``0.0`` (Plan 059) — the wait is then skipped entirely.
    """

    #: Subclasses set this False if the concrete bus genuinely has no
    #: start()/stop()/async-context-manager lifecycle (e.g. InMemoryEventBus).
    supports_lifecycle: ClassVar[bool] = True

    #: Seconds to wait between subscribe() and publish() so a broker-backed
    #: consumer can join its group / auto-create its topic (observed necessary
    #: for Kafka). The 2.0 default is what every broker subclass (Kafka, NATS,
    #: Redis) inherits — integration behaviour unchanged by Plan 059.
    #: Edge cases: an in-process bus that delivers synchronously sets 0.0 —
    #: `_settle()` then returns without yielding to the loop at all. Delivery
    #: is still proven by `_wait_until` polling, so no assertion is weakened.
    subscribe_settle_seconds: ClassVar[float] = 2.0

    @pytest.fixture
    async def bus(self) -> AbstractEventBus:
        """Abstract — must be overridden by every subclass."""
        raise NotImplementedError(
            "EventBusConformance subclasses must override the `bus` fixture "
            "with a concrete AbstractEventBus implementation."
        )

    def _channel(self) -> str:
        return f"conformance-{uuid4().hex[:8]}"

    async def _settle(self) -> None:
        """Wait ``subscribe_settle_seconds`` between subscribe and publish.

        Returns:
            None — immediately when the knob is ``0`` (in-process buses).
        """
        if self.subscribe_settle_seconds > 0:
            await asyncio.sleep(self.subscribe_settle_seconds)

    async def test_publish_subscribe_round_trip(self, bus: AbstractEventBus) -> None:
        channel = self._channel()
        received: list[Event] = []

        async def handler(event: Event) -> None:
            received.append(event)

        bus.subscribe(_ConformanceEventA, handler, channel=channel)
        # Give a broker-backed consumer a moment to join/settle before
        # publishing — see `subscribe_settle_seconds`.
        await self._settle()
        await bus.publish(_ConformanceEventA(value="hello"), channel=channel)
        if hasattr(bus, "drain"):
            await bus.drain()
        await _wait_until(lambda: len(received) >= 1)

        assert len(received) == 1
        assert received[0].value == "hello"  # type: ignore[attr-defined]

    async def test_multiple_subscribers_on_one_channel(self, bus: AbstractEventBus) -> None:
        channel = self._channel()
        received_1: list[Event] = []
        received_2: list[Event] = []

        async def handler_1(event: Event) -> None:
            received_1.append(event)

        async def handler_2(event: Event) -> None:
            received_2.append(event)

        bus.subscribe(_ConformanceEventA, handler_1, channel=channel)
        bus.subscribe(_ConformanceEventA, handler_2, channel=channel)
        await self._settle()
        await bus.publish(_ConformanceEventA(value="x"), channel=channel)
        if hasattr(bus, "drain"):
            await bus.drain()
        await _wait_until(lambda: len(received_1) >= 1 and len(received_2) >= 1)

        assert len(received_1) == 1
        assert len(received_2) == 1

    async def test_unsubscribe_stops_delivery(self, bus: AbstractEventBus) -> None:
        channel = self._channel()
        received: list[Event] = []

        async def handler(event: Event) -> None:
            received.append(event)

        sub = bus.subscribe(_ConformanceEventA, handler, channel=channel)
        sub.cancel()
        await bus.publish(_ConformanceEventA(value="x"), channel=channel)
        if hasattr(bus, "drain"):
            await bus.drain()

        assert received == []

    async def test_channel_isolation(self, bus: AbstractEventBus) -> None:
        channel_a = self._channel()
        channel_b = self._channel()
        received: list[Event] = []

        async def handler(event: Event) -> None:
            received.append(event)

        # Subscribe only to channel A — an event on channel B must never arrive.
        bus.subscribe(_ConformanceEventA, handler, channel=channel_a)
        await bus.publish(_ConformanceEventA(value="x"), channel=channel_b)
        if hasattr(bus, "drain"):
            await bus.drain()

        assert received == []

    async def test_publish_with_no_subscriber_does_not_raise(self, bus: AbstractEventBus) -> None:
        channel = self._channel()
        # Must not raise.
        await bus.publish(_ConformanceEventA(value="nobody-listening"), channel=channel)
        if hasattr(bus, "drain"):
            await bus.drain()

    async def test_event_payload_fidelity_round_trip(self, bus: AbstractEventBus) -> None:
        channel = self._channel()
        received: list[Event] = []

        async def handler(event: Event) -> None:
            received.append(event)

        bus.subscribe(_ConformanceEventA, handler, channel=channel)
        await self._settle()
        sent = _ConformanceEventA(value="round-trip-value")
        await bus.publish(sent, channel=channel)
        if hasattr(bus, "drain"):
            await bus.drain()
        await _wait_until(lambda: len(received) >= 1)

        assert len(received) == 1
        got = received[0]
        assert isinstance(got, _ConformanceEventA)
        assert got.value == "round-trip-value"  # type: ignore[attr-defined]

    async def test_lifecycle_start_stop_idempotent(self, bus: AbstractEventBus) -> None:
        if not self.supports_lifecycle:
            pytest.skip("bus does not support start()/stop() lifecycle")
        if not (hasattr(bus, "start") and hasattr(bus, "stop")):
            pytest.skip("bus has no start()/stop() methods")
        await bus.start()
        await bus.start()  # idempotent — must not raise
        await bus.stop()
        await bus.stop()  # idempotent — must not raise

    async def test_async_context_manager_entry_exit(self, bus: AbstractEventBus) -> None:
        if not self.supports_lifecycle:
            pytest.skip("bus does not support the async context manager protocol")
        if not (hasattr(bus, "__aenter__") and hasattr(bus, "__aexit__")):
            pytest.skip("bus has no __aenter__/__aexit__")
        async with bus as entered:
            assert entered is bus
