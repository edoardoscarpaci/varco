"""
Unit tests for varco_core.event
================================
Covers every component of the event system:

- ``Event`` base class (immutability, auto-ID, timestamp, auto-registry,
  ``event_type_name``)
- ``Subscription`` handle (cancel, is_cancelled)
- ``InMemoryEventBus`` — SYNC mode:
    - basic publish / subscribe
    - channel routing (wildcard, specific, no-match)
    - event type routing (class, string, inheritance)
    - filter predicate
    - multiple handlers
    - priority ordering (higher → runs first, equal → FIFO)
    - error policies: COLLECT_ALL, FAIL_FAST, FIRE_FORGET
    - cancelled entries are skipped
    - ``emitted`` list pre-populated before dispatch
    - ``clear_emitted()``
- ``InMemoryEventBus`` — BACKGROUND mode:
    - task returned from ``publish()``
    - ``drain()`` waits for handlers
    - handler still called after drain
    - errors in background are NOT raised to caller
- ``NoopEventBus``:
    - ``publish()`` returns ``None``
    - ``subscribe()`` returns pre-cancelled subscription
- ``EventConsumer`` + ``@listen``:
    - single event type
    - multiple event types on one method
    - stacked decorators
    - channel filter
    - filter predicate
    - priority pass-through
    - ``register_to`` returns one handle per entry
    - MRO walk discovers parent methods
- ``AbstractEventProducer``:
    - ``BusEventProducer._produce`` delegates to bus
    - ``BusEventProducer._produce_many`` delegates to bus
    - ``NoopEventProducer`` silently discards
- Middleware:
    - pre/post hooks fire around dispatch
    - middleware chain order (first is outermost)
    - middleware can suppress dispatch
- Domain events (``EntityCreatedEvent``, ``EntityUpdatedEvent``,
  ``EntityDeletedEvent``):
    - ``isinstance(EntityCreatedEvent(), EntityEvent)`` is True
    - ``event_type_name()`` returns declared ``__event_type__``
- ``JsonEventSerializer``:
    - serialize round-trips to bytes and back
    - ``__event_type__`` injected into JSON
    - deserialize reconstructs the correct subclass
    - missing ``__event_type__`` raises ``ValueError``
    - unknown type raises ``KeyError``

Test doubles
------------
All test doubles are plain Python — no mocking library.
``CapturingHandler`` records calls for assertion.
``ErrorHandler`` raises on every call.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from varco_core.event import (
    CHANNEL_ALL,
    AbstractEventProducer,
    BusEventProducer,
    DispatchMode,
    EntityCreatedEvent,
    EntityDeletedEvent,
    EntityEvent,
    EntityUpdatedEvent,
    ErrorPolicy,
    Event,
    EventConsumer,
    EventMiddleware,
    InMemoryEventBus,
    JsonEventSerializer,
    NoopEventBus,
    NoopEventProducer,
    Subscription,
    listen,
)


# ── Test event types ───────────────────────────────────────────────────────────


class OrderEvent(Event):
    """Base test event."""

    order_id: str


class OrderPlacedEvent(OrderEvent):
    """Concrete subclass — inherits from OrderEvent."""

    __event_type__ = "order.placed"
    total: float = 0.0


class OrderCancelledEvent(OrderEvent):
    """Another concrete subclass."""

    __event_type__ = "order.cancelled"
    reason: str = ""


class AnotherEvent(Event):
    """Unrelated event type for negative routing tests."""

    data: str = ""


# ── Helpers ────────────────────────────────────────────────────────────────────


class CapturingHandler:
    """
    Records every call to ``__call__``.

    Attributes:
        calls: List of events received, in call order.
    """

    def __init__(self) -> None:
        self.calls: list[Event] = []

    async def __call__(self, event: Event) -> None:
        self.calls.append(event)

    def __repr__(self) -> str:
        return f"CapturingHandler(calls={len(self.calls)})"


class SyncCapturingHandler:
    """Sync variant — the bus must call sync handlers directly."""

    def __init__(self) -> None:
        self.calls: list[Event] = []

    def __call__(self, event: Event) -> None:
        self.calls.append(event)


class ErrorHandler:
    """Always raises ``RuntimeError`` with a configurable message."""

    def __init__(self, msg: str = "handler error") -> None:
        self.msg = msg
        self.called = False

    async def __call__(self, event: Event) -> None:
        self.called = True
        raise RuntimeError(self.msg)


# ── Event base class ───────────────────────────────────────────────────────────


class TestEvent:
    def test_auto_id_and_timestamp(self) -> None:
        e = OrderPlacedEvent(order_id="1")
        assert e.event_id is not None
        assert e.timestamp is not None

    def test_immutable(self) -> None:
        e = OrderPlacedEvent(order_id="1")
        with pytest.raises(Exception):
            # frozen=True — any attribute assignment must raise
            e.order_id = "2"  # type: ignore[misc]

    def test_event_type_name_from_classvar(self) -> None:
        assert OrderPlacedEvent.event_type_name() == "order.placed"

    def test_event_type_name_fallback_to_class_name(self) -> None:
        # OrderEvent has no __event_type__ — falls back to class name
        assert OrderEvent.event_type_name() == "OrderEvent"

    def test_auto_registry(self) -> None:
        # OrderPlacedEvent declares __event_type__ — registered under that key
        assert Event._registry["order.placed"] is OrderPlacedEvent
        assert Event._registry["order.cancelled"] is OrderCancelledEvent
        # OrderEvent has no __event_type__ — registered under class name
        assert Event._registry["OrderEvent"] is OrderEvent

    def test_two_events_are_not_equal(self) -> None:
        # Separate instances have different event_ids — not equal
        a = OrderPlacedEvent(order_id="x")
        b = OrderPlacedEvent(order_id="x")
        assert a.event_id != b.event_id


# ── Subscription ───────────────────────────────────────────────────────────────


class TestSubscription:
    async def test_cancel_sets_flag(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        sub = bus.subscribe(OrderPlacedEvent, handler)

        assert not sub.is_cancelled
        sub.cancel()
        assert sub.is_cancelled

    async def test_cancel_idempotent(self) -> None:
        bus = InMemoryEventBus()
        sub = bus.subscribe(OrderPlacedEvent, CapturingHandler())
        sub.cancel()
        sub.cancel()  # second call — no error
        assert sub.is_cancelled

    async def test_cancelled_handler_not_called(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        sub = bus.subscribe(OrderPlacedEvent, handler)
        sub.cancel()

        await bus.publish(OrderPlacedEvent(order_id="x"))
        assert handler.calls == []


# ── InMemoryEventBus — SYNC mode ──────────────────────────────────────────────


class TestInMemoryEventBusSyncMode:
    async def test_basic_publish_subscribe(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler)

        event = OrderPlacedEvent(order_id="1")
        await bus.publish(event)

        assert handler.calls == [event]

    async def test_sync_publish_returns_none(self) -> None:
        bus = InMemoryEventBus()
        result = await bus.publish(OrderPlacedEvent(order_id="1"))
        assert result is None

    async def test_emitted_list_populated(self) -> None:
        bus = InMemoryEventBus()
        event = OrderPlacedEvent(order_id="1")
        await bus.publish(event, channel="orders")

        assert len(bus.emitted) == 1
        assert bus.emitted[0] == (event, "orders")

    async def test_emitted_populated_even_when_handler_raises(self) -> None:
        bus = InMemoryEventBus(error_policy=ErrorPolicy.FIRE_FORGET)
        bus.subscribe(OrderPlacedEvent, ErrorHandler())
        event = OrderPlacedEvent(order_id="1")

        await bus.publish(event)

        # emitted is appended before dispatch fires — always recorded
        assert bus.emitted[0][0] is event

    async def test_clear_emitted(self) -> None:
        bus = InMemoryEventBus()
        await bus.publish(OrderPlacedEvent(order_id="1"))
        bus.clear_emitted()
        assert bus.emitted == []

    async def test_channel_wildcard_matches_any(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler, channel=CHANNEL_ALL)

        await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")
        await bus.publish(OrderPlacedEvent(order_id="2"), channel="payments")

        assert len(handler.calls) == 2

    async def test_channel_specific_matches_only_matching(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler, channel="orders")

        await bus.publish(OrderPlacedEvent(order_id="1"), channel="orders")
        await bus.publish(OrderPlacedEvent(order_id="2"), channel="payments")

        assert len(handler.calls) == 1

    async def test_channel_no_match_handler_not_called(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler, channel="orders")

        await bus.publish(OrderPlacedEvent(order_id="1"), channel="payments")

        assert handler.calls == []

    async def test_class_based_dispatch_exact_type(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler)

        await bus.publish(OrderPlacedEvent(order_id="1"))

        assert len(handler.calls) == 1

    async def test_class_based_dispatch_parent_receives_subclass(self) -> None:
        # Subscribing to OrderEvent must receive OrderPlacedEvent (subclass)
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe(OrderEvent, handler)

        await bus.publish(OrderPlacedEvent(order_id="1"))

        assert len(handler.calls) == 1

    async def test_class_based_dispatch_unrelated_type_not_received(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler)

        await bus.publish(AnotherEvent())

        assert handler.calls == []

    async def test_string_based_dispatch_exact_match(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe("order.placed", handler)

        await bus.publish(OrderPlacedEvent(order_id="1"))

        assert len(handler.calls) == 1

    async def test_string_based_dispatch_no_match(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe("order.placed", handler)

        await bus.publish(OrderCancelledEvent(order_id="1"))

        assert handler.calls == []

    async def test_filter_passes_matching_event(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe(
            OrderPlacedEvent,
            handler,
            filter=lambda e: e.total > 100,  # type: ignore[attr-defined]
        )

        await bus.publish(OrderPlacedEvent(order_id="1", total=200.0))

        assert len(handler.calls) == 1

    async def test_filter_blocks_non_matching_event(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe(
            OrderPlacedEvent,
            handler,
            filter=lambda e: e.total > 100,  # type: ignore[attr-defined]
        )

        await bus.publish(OrderPlacedEvent(order_id="1", total=50.0))

        assert handler.calls == []

    async def test_multiple_handlers_all_called(self) -> None:
        bus = InMemoryEventBus()
        h1 = CapturingHandler()
        h2 = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, h1)
        bus.subscribe(OrderPlacedEvent, h2)

        await bus.publish(OrderPlacedEvent(order_id="1"))

        assert len(h1.calls) == 1
        assert len(h2.calls) == 1

    async def test_sync_handler_called(self) -> None:
        # Sync handlers (not async def) are supported — bus detects via
        # asyncio.iscoroutine on the return value.
        bus = InMemoryEventBus()
        handler = SyncCapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler)

        await bus.publish(OrderPlacedEvent(order_id="1"))

        assert len(handler.calls) == 1

    async def test_priority_higher_runs_first(self) -> None:
        bus = InMemoryEventBus()
        call_order: list[int] = []

        async def low(e: Event) -> None:
            call_order.append(0)

        async def high(e: Event) -> None:
            call_order.append(10)

        bus.subscribe(OrderPlacedEvent, low, priority=0)
        bus.subscribe(OrderPlacedEvent, high, priority=10)

        await bus.publish(OrderPlacedEvent(order_id="1"))

        # high priority must run before low priority
        assert call_order == [10, 0]

    async def test_priority_equal_fifo(self) -> None:
        bus = InMemoryEventBus()
        call_order: list[str] = []

        async def first(e: Event) -> None:
            call_order.append("first")

        async def second(e: Event) -> None:
            call_order.append("second")

        # Same priority — subscription order wins (FIFO)
        bus.subscribe(OrderPlacedEvent, first, priority=5)
        bus.subscribe(OrderPlacedEvent, second, priority=5)

        await bus.publish(OrderPlacedEvent(order_id="1"))

        assert call_order == ["first", "second"]

    async def test_error_policy_collect_all_raises_exception_group(self) -> None:
        bus = InMemoryEventBus(error_policy=ErrorPolicy.COLLECT_ALL)
        bus.subscribe(OrderPlacedEvent, ErrorHandler("e1"))
        bus.subscribe(OrderPlacedEvent, ErrorHandler("e2"))

        with pytest.raises(ExceptionGroup) as exc_info:
            await bus.publish(OrderPlacedEvent(order_id="1"))

        # Both errors collected
        assert len(exc_info.value.exceptions) == 2

    async def test_error_policy_collect_all_single_error_unwrapped(self) -> None:
        # Single-error COLLECT_ALL raises the bare exception, not ExceptionGroup
        bus = InMemoryEventBus(error_policy=ErrorPolicy.COLLECT_ALL)
        bus.subscribe(OrderPlacedEvent, ErrorHandler("solo"))

        with pytest.raises(RuntimeError, match="solo"):
            await bus.publish(OrderPlacedEvent(order_id="1"))

    async def test_error_policy_fail_fast_stops_at_first_error(self) -> None:
        bus = InMemoryEventBus(error_policy=ErrorPolicy.FAIL_FAST)
        h1 = ErrorHandler("first")
        h2 = CapturingHandler()  # should NOT be called
        bus.subscribe(OrderPlacedEvent, h1, priority=10)
        bus.subscribe(OrderPlacedEvent, h2, priority=0)

        with pytest.raises(RuntimeError, match="first"):
            await bus.publish(OrderPlacedEvent(order_id="1"))

        # h2 was never reached because h1 raised under FAIL_FAST
        assert h2.calls == []

    async def test_error_policy_fire_forget_no_exception_raised(self) -> None:
        bus = InMemoryEventBus(error_policy=ErrorPolicy.FIRE_FORGET)
        bus.subscribe(OrderPlacedEvent, ErrorHandler())

        # Must not raise
        await bus.publish(OrderPlacedEvent(order_id="1"))

    async def test_error_policy_fire_forget_subsequent_handlers_still_run(self) -> None:
        bus = InMemoryEventBus(error_policy=ErrorPolicy.FIRE_FORGET)
        bus.subscribe(OrderPlacedEvent, ErrorHandler(), priority=10)
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler, priority=0)

        await bus.publish(OrderPlacedEvent(order_id="1"))

        # Second handler still received the event despite first raising
        assert len(handler.calls) == 1

    async def test_no_subscribers_is_noop(self) -> None:
        bus = InMemoryEventBus()
        # Should not raise
        await bus.publish(OrderPlacedEvent(order_id="1"))

    async def test_repr(self) -> None:
        bus = InMemoryEventBus()
        bus.subscribe(OrderPlacedEvent, CapturingHandler())
        r = repr(bus)
        assert "InMemoryEventBus" in r
        assert "subscriptions=1" in r

    async def test_publish_many(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe(Event, handler)

        e1 = OrderPlacedEvent(order_id="1")
        e2 = OrderCancelledEvent(order_id="2")
        await bus.publish_many([(e1, "orders"), (e2, "orders")])

        assert len(handler.calls) == 2


# ── InMemoryEventBus — BACKGROUND mode ────────────────────────────────────────


class TestInMemoryEventBusBackgroundMode:
    async def test_publish_returns_task_in_background_mode(self) -> None:
        bus = InMemoryEventBus(dispatch_mode=DispatchMode.BACKGROUND)
        task = await bus.publish(OrderPlacedEvent(order_id="1"))

        assert isinstance(task, asyncio.Task)
        await bus.drain()

    async def test_handler_called_after_drain(self) -> None:
        bus = InMemoryEventBus(dispatch_mode=DispatchMode.BACKGROUND)
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler)

        await bus.publish(OrderPlacedEvent(order_id="1"))
        # Handler not necessarily called yet — drain() waits for the task
        await bus.drain()

        assert len(handler.calls) == 1

    async def test_drain_noop_when_no_pending_tasks(self) -> None:
        bus = InMemoryEventBus(dispatch_mode=DispatchMode.BACKGROUND)
        # Should not raise or hang
        await bus.drain()

    async def test_background_error_does_not_raise_to_caller(self) -> None:
        bus = InMemoryEventBus(
            dispatch_mode=DispatchMode.BACKGROUND,
            error_policy=ErrorPolicy.COLLECT_ALL,
        )
        bus.subscribe(OrderPlacedEvent, ErrorHandler())

        # publish must NOT raise even though the handler will fail
        task = await bus.publish(OrderPlacedEvent(order_id="1"))
        await bus.drain()

        # The task finished with an exception — but the caller never sees it
        assert task is not None
        # task.exception() is NOT None (the handler raised)
        assert task.done()

    async def test_caller_can_await_returned_task(self) -> None:
        bus = InMemoryEventBus(dispatch_mode=DispatchMode.BACKGROUND)
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler)

        task = await bus.publish(OrderPlacedEvent(order_id="1"))
        assert task is not None
        await task  # caller explicitly waits

        assert len(handler.calls) == 1

    async def test_emitted_populated_before_handler_runs(self) -> None:
        # Even in BACKGROUND mode, emitted is populated synchronously in publish
        bus = InMemoryEventBus(dispatch_mode=DispatchMode.BACKGROUND)
        event = OrderPlacedEvent(order_id="1")

        await bus.publish(event)
        # emitted is populated before the background task runs
        assert len(bus.emitted) == 1
        await bus.drain()


# ── NoopEventBus ──────────────────────────────────────────────────────────────


class TestNoopEventBus:
    async def test_publish_returns_none(self) -> None:
        bus = NoopEventBus()
        result = await bus.publish(OrderPlacedEvent(order_id="1"))
        assert result is None

    async def test_subscribe_returns_pre_cancelled_subscription(self) -> None:
        bus = NoopEventBus()
        sub = bus.subscribe(OrderPlacedEvent, CapturingHandler())

        assert sub.is_cancelled

    async def test_repr(self) -> None:
        assert repr(NoopEventBus()) == "NoopEventBus()"


# ── EventConsumer + @listen ────────────────────────────────────────────────────


class TestEventConsumerAndListen:
    async def test_single_event_type(self) -> None:
        bus = InMemoryEventBus()

        class MyConsumer(EventConsumer):
            def __init__(self) -> None:
                self.received: list[Event] = []

            @listen(OrderPlacedEvent)
            async def on_placed(self, event: OrderPlacedEvent) -> None:
                self.received.append(event)

        consumer = MyConsumer()
        consumer.register_to(bus)

        event = OrderPlacedEvent(order_id="1")
        await bus.publish(event)

        assert consumer.received == [event]

    async def test_multiple_event_types_on_one_method(self) -> None:
        bus = InMemoryEventBus()

        class MyConsumer(EventConsumer):
            def __init__(self) -> None:
                self.received: list[Event] = []

            @listen(OrderPlacedEvent, OrderCancelledEvent)
            async def on_any(self, event: Event) -> None:
                self.received.append(event)

        consumer = MyConsumer()
        consumer.register_to(bus)

        e1 = OrderPlacedEvent(order_id="1")
        e2 = OrderCancelledEvent(order_id="2")
        await bus.publish(e1)
        await bus.publish(e2)

        assert len(consumer.received) == 2

    async def test_stacked_listen_decorators(self) -> None:
        bus = InMemoryEventBus()

        class MyConsumer(EventConsumer):
            def __init__(self) -> None:
                self.received: list[Event] = []

            @listen(OrderPlacedEvent, channel="orders")
            @listen(OrderPlacedEvent, channel="audit")
            async def on_placed(self, event: Event) -> None:
                self.received.append(event)

        consumer = MyConsumer()
        consumer.register_to(bus)

        event = OrderPlacedEvent(order_id="1")
        await bus.publish(event, channel="orders")
        await bus.publish(event, channel="audit")

        # Both subscriptions trigger — event received twice
        assert len(consumer.received) == 2

    async def test_channel_filter(self) -> None:
        bus = InMemoryEventBus()

        class MyConsumer(EventConsumer):
            def __init__(self) -> None:
                self.received: list[Event] = []

            @listen(OrderPlacedEvent, channel="orders")
            async def on_placed(self, event: Event) -> None:
                self.received.append(event)

        consumer = MyConsumer()
        consumer.register_to(bus)

        await bus.publish(OrderPlacedEvent(order_id="1"), channel="payments")
        await bus.publish(OrderPlacedEvent(order_id="2"), channel="orders")

        assert len(consumer.received) == 1

    async def test_filter_predicate(self) -> None:
        bus = InMemoryEventBus()

        class MyConsumer(EventConsumer):
            def __init__(self) -> None:
                self.received: list[Event] = []

            @listen(OrderPlacedEvent, filter=lambda e: e.total > 50)  # type: ignore[attr-defined]
            async def on_large(self, event: Event) -> None:
                self.received.append(event)

        consumer = MyConsumer()
        consumer.register_to(bus)

        await bus.publish(OrderPlacedEvent(order_id="1", total=10.0))
        await bus.publish(OrderPlacedEvent(order_id="2", total=100.0))

        assert len(consumer.received) == 1

    async def test_priority_passed_through(self) -> None:
        bus = InMemoryEventBus()
        call_order: list[str] = []

        class HighConsumer(EventConsumer):
            @listen(OrderPlacedEvent, priority=10)
            async def on_placed(self, event: Event) -> None:
                call_order.append("high")

        class LowConsumer(EventConsumer):
            @listen(OrderPlacedEvent, priority=0)
            async def on_placed(self, event: Event) -> None:
                call_order.append("low")

        LowConsumer().register_to(bus)  # subscribed first
        HighConsumer().register_to(bus)  # subscribed second

        await bus.publish(OrderPlacedEvent(order_id="1"))

        # high priority must win despite being subscribed after low
        assert call_order == ["high", "low"]

    async def test_register_to_returns_one_handle_per_entry(self) -> None:
        bus = InMemoryEventBus()

        class MyConsumer(EventConsumer):
            @listen(OrderPlacedEvent, OrderCancelledEvent)
            async def on_any(self, event: Event) -> None:
                pass

        consumer = MyConsumer()
        subs = consumer.register_to(bus)

        # Two event types → two separate subscriptions
        assert len(subs) == 2
        assert all(isinstance(s, Subscription) for s in subs)

    async def test_mro_walk_discovers_parent_methods(self) -> None:
        bus = InMemoryEventBus()

        class BaseConsumer(EventConsumer):
            def __init__(self) -> None:
                self.parent_calls: list[Event] = []

            @listen(OrderPlacedEvent)
            async def on_placed(self, event: Event) -> None:
                self.parent_calls.append(event)

        class ChildConsumer(BaseConsumer):
            pass

        consumer = ChildConsumer()
        consumer.register_to(bus)

        await bus.publish(OrderPlacedEvent(order_id="1"))

        # Parent method discovered via MRO walk
        assert len(consumer.parent_calls) == 1

    async def test_listen_no_event_types_raises(self) -> None:
        with pytest.raises(TypeError, match="@listen requires at least one event_type"):
            listen()  # type: ignore[call-overload]

    async def test_repr(self) -> None:
        class MyConsumer(EventConsumer):
            @listen(OrderPlacedEvent)
            async def on_placed(self, event: Event) -> None:
                pass

        r = repr(MyConsumer())
        assert "MyConsumer" in r
        assert "listen_entries=1" in r


# ── AbstractEventProducer ─────────────────────────────────────────────────────


class TestEventProducers:
    async def test_bus_event_producer_delegates_to_bus(self) -> None:
        bus = InMemoryEventBus()
        producer = BusEventProducer(bus)

        event = OrderPlacedEvent(order_id="1")
        await producer._produce(event, channel="orders")

        assert bus.emitted == [(event, "orders")]

    async def test_bus_event_producer_produce_many(self) -> None:
        bus = InMemoryEventBus()
        producer = BusEventProducer(bus)

        e1 = OrderPlacedEvent(order_id="1")
        e2 = OrderCancelledEvent(order_id="2")
        await producer._produce_many([(e1, "orders"), (e2, "orders")])

        assert len(bus.emitted) == 2

    async def test_noop_event_producer_discards_events(self) -> None:
        producer = NoopEventProducer()
        # Must not raise and must have no observable side-effect
        await producer._produce(OrderPlacedEvent(order_id="1"))
        await producer._produce_many([(OrderPlacedEvent(order_id="2"), "orders")])

    async def test_abstract_event_producer_is_abstract(self) -> None:
        # AbstractEventProducer cannot be instantiated directly
        with pytest.raises(TypeError):
            AbstractEventProducer()  # type: ignore[abstract]


# ── Middleware ─────────────────────────────────────────────────────────────────


class TestMiddleware:
    async def test_middleware_pre_hook_fires(self) -> None:
        pre_events: list[Event] = []

        class LogMiddleware(EventMiddleware):
            async def __call__(self, event, channel, next):  # type: ignore[override]
                pre_events.append(event)
                await next(event, channel)

        bus = InMemoryEventBus(middleware=[LogMiddleware()])
        await bus.publish(OrderPlacedEvent(order_id="1"))

        assert len(pre_events) == 1

    async def test_middleware_post_hook_fires_after_handlers(self) -> None:
        order: list[str] = []

        class TrackingMiddleware(EventMiddleware):
            async def __call__(self, event, channel, next):  # type: ignore[override]
                order.append("before")
                await next(event, channel)
                order.append("after")

        bus = InMemoryEventBus(middleware=[TrackingMiddleware()])
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler)

        await bus.publish(OrderPlacedEvent(order_id="1"))

        assert order == ["before", "after"]

    async def test_middleware_can_suppress_dispatch(self) -> None:
        class SuppressionMiddleware(EventMiddleware):
            async def __call__(self, event, channel, next):  # type: ignore[override]
                # Intentionally NOT calling next — suppresses all handlers
                pass

        bus = InMemoryEventBus(middleware=[SuppressionMiddleware()])
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler)

        await bus.publish(OrderPlacedEvent(order_id="1"))

        assert handler.calls == []

    async def test_middleware_chain_order_first_is_outermost(self) -> None:
        # First middleware in list wraps the second; the second wraps dispatch.
        order: list[int] = []

        class MW(EventMiddleware):
            def __init__(self, n: int) -> None:
                self.n = n

            async def __call__(self, event, channel, next):  # type: ignore[override]
                order.append(self.n)
                await next(event, channel)

        bus = InMemoryEventBus(middleware=[MW(1), MW(2)])
        await bus.publish(OrderPlacedEvent(order_id="1"))

        # MW(1) is outermost — fires first
        assert order == [1, 2]

    async def test_middleware_can_modify_channel(self) -> None:
        class RewriteChannelMiddleware(EventMiddleware):
            async def __call__(self, event, channel, next):  # type: ignore[override]
                await next(event, "rewritten")

        bus = InMemoryEventBus(middleware=[RewriteChannelMiddleware()])
        handler = CapturingHandler()
        bus.subscribe(OrderPlacedEvent, handler, channel="rewritten")

        await bus.publish(OrderPlacedEvent(order_id="1"), channel="original")

        assert len(handler.calls) == 1


# ── Domain events ─────────────────────────────────────────────────────────────


class TestDomainEvents:
    def test_entity_created_event_is_entity_event(self) -> None:
        e = EntityCreatedEvent(entity_type="Post", pk="abc", payload={"title": "hi"})
        assert isinstance(e, EntityEvent)
        assert isinstance(e, Event)

    def test_entity_updated_event_type_name(self) -> None:
        assert EntityUpdatedEvent.event_type_name() == "entity.updated"

    def test_entity_deleted_event_has_no_payload(self) -> None:
        e = EntityDeletedEvent(entity_type="Post", pk="1")
        # EntityDeletedEvent has no payload field
        assert not hasattr(e, "payload")

    async def test_subscribe_to_entity_event_receives_all_subclasses(self) -> None:
        bus = InMemoryEventBus()
        handler = CapturingHandler()
        bus.subscribe(EntityEvent, handler)

        await bus.publish(EntityCreatedEvent(entity_type="Post", pk="1", payload={}))
        await bus.publish(EntityUpdatedEvent(entity_type="Post", pk="1", payload={}))
        await bus.publish(EntityDeletedEvent(entity_type="Post", pk="1"))

        assert len(handler.calls) == 3

    def test_correlation_id_defaults_to_none(self) -> None:
        e = EntityCreatedEvent(entity_type="Post", pk="1", payload={})
        assert e.correlation_id is None


# ── JsonEventSerializer ────────────────────────────────────────────────────────


class TestJsonEventSerializer:
    def test_serialize_produces_bytes(self) -> None:
        event = OrderPlacedEvent(order_id="1", total=99.0)
        data = JsonEventSerializer.serialize(event)

        assert isinstance(data, bytes)

    def test_type_key_injected(self) -> None:
        event = OrderPlacedEvent(order_id="1")
        data = JsonEventSerializer.serialize(event)
        raw = json.loads(data.decode("utf-8"))

        assert raw["__event_type__"] == "order.placed"

    def test_round_trip_preserves_fields(self) -> None:
        original = OrderPlacedEvent(order_id="abc", total=42.5)
        data = JsonEventSerializer.serialize(original)
        restored = JsonEventSerializer.deserialize(data)

        assert isinstance(restored, OrderPlacedEvent)
        assert restored.order_id == "abc"
        assert restored.total == 42.5
        assert str(restored.event_id) == str(original.event_id)

    def test_deserialize_reconstructs_correct_subclass(self) -> None:
        event = OrderCancelledEvent(order_id="2", reason="out of stock")
        data = JsonEventSerializer.serialize(event)
        restored = JsonEventSerializer.deserialize(data)

        assert type(restored) is OrderCancelledEvent

    def test_deserialize_missing_type_key_raises_value_error(self) -> None:
        raw = json.dumps({"order_id": "1", "total": 0.0}).encode("utf-8")

        with pytest.raises(ValueError, match="__event_type__"):
            JsonEventSerializer.deserialize(raw)

    def test_deserialize_unknown_type_raises_key_error(self) -> None:
        raw = json.dumps({"__event_type__": "completely.unknown"}).encode("utf-8")

        with pytest.raises(KeyError, match="completely.unknown"):
            JsonEventSerializer.deserialize(raw)

    def test_entity_created_event_round_trip(self) -> None:
        original = EntityCreatedEvent(
            entity_type="Post",
            pk="post-1",
            correlation_id="req-xyz",
            payload={"title": "Hello"},
        )
        data = JsonEventSerializer.serialize(original)
        restored = JsonEventSerializer.deserialize(data)

        assert isinstance(restored, EntityCreatedEvent)
        assert restored.entity_type == "Post"
        assert restored.payload == {"title": "Hello"}
        assert restored.correlation_id == "req-xyz"
