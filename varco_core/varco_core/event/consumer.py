"""
varco_core.event.consumer
=========================
Event consumer base class and the ``@listen`` method decorator.

``EventConsumer`` can be used in two ways:

**Standalone consumer class**::

    @Component
    class NotificationConsumer(EventConsumer):
        def __init__(self, bus: Inject[AbstractEventBus]) -> None:
            self._bus = bus

        @PostConstruct
        def _setup(self) -> None:
            # Explicit wiring — called once after DI construction.
            self.register_to(self._bus)

        @listen(OrderPlacedEvent)
        async def send_email(self, event: OrderPlacedEvent) -> None:
            ...

        @listen(OrderPlacedEvent, filter=lambda e: e.total > 1000)
        async def alert_large_order(self, event: OrderPlacedEvent) -> None:
            ...

**Mixin with AsyncService** (consume AND produce)::

    class OrderService(
        AsyncService[Order, UUID, CreateOrderDTO, OrderReadDTO, UpdateOrderDTO],
        EventConsumer,
    ):
        def __init__(self, ..., event_bus: AbstractEventBus) -> None:
            super().__init__(..., producer=BusEventProducer(event_bus))
            # Register @listen methods from EventConsumer side
            self.register_to(event_bus)

        @listen(PaymentCompletedEvent, channel="payments")
        async def on_payment(self, event: PaymentCompletedEvent) -> None:
            # React to payments and produce order events
            await self._produce(OrderUpdatedEvent(...))

The ``@listen`` decorator is **bus-agnostic** — it stores metadata on the
method at class-definition time.  The actual subscription to a bus only
happens when ``register_to(bus)`` is called.  This separation means the
same consumer class can be registered to different buses in tests vs.
production without any code changes.

DESIGN: ``@listen`` metadata on ``__listen_entries__`` vs. a class registry
    ✅ No global state — each class is independent.
    ✅ Inheritance works naturally — MRO walk discovers parent entries.
    ✅ Stacking multiple ``@listen`` decorators on one method is supported.
    ❌ Metadata lives on the function object — accessing it requires either
       iterating ``inspect.getmembers`` (done once at ``register_to`` time)
       or scanning ``__dict__`` manually.  The cost is paid once at wiring
       time, not per event.

Thread safety:  ✅ ``register_to`` is called once during setup — not
                    during concurrent request handling.
Async safety:   ✅ Handlers are invoked by the bus — sync or async both
                    work (the bus detects via ``asyncio.iscoroutinefunction``).
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from varco_core.event.base import CHANNEL_ALL, AbstractEventBus, Event, Subscription

if TYPE_CHECKING:
    pass


# ── _ListenEntry ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _ListenEntry:
    """
    Immutable metadata for a single ``@listen`` registration on a method.

    Stored in a list on ``func.__listen_entries__`` by the ``@listen``
    decorator.  Each entry corresponds to one ``bus.subscribe()`` call
    issued by ``EventConsumer.register_to()``.

    DESIGN: frozen dataclass over a plain tuple
        ✅ Named fields — no positional-index bugs when the structure grows.
        ✅ Hashable — safe to use in sets if deduplication is ever needed.
        ❌ Slightly more verbose than a tuple — justified for maintainability.
    """

    event_type: type[Event] | str
    """Event class (class-based dispatch) or ``__event_type__`` string."""

    channel: str
    """Channel filter.  ``CHANNEL_ALL`` matches any publish channel."""

    filter: Callable[[Event], bool] | None  # noqa: A003
    """Optional predicate applied after event_type and channel match."""

    priority: int = 0
    """Dispatch priority.  Higher values run first.  Defaults to ``0``."""


# ── @listen decorator ─────────────────────────────────────────────────────────


def listen(
    *event_types: type[Event] | str,
    channel: str = CHANNEL_ALL,
    filter: Callable[[Event], bool] | None = None,  # noqa: A002
    priority: int = 0,
) -> Callable:
    """
    Method decorator for ``EventConsumer`` subclasses.

    Marks a method to be automatically subscribed to one or more event types
    when ``EventConsumer.register_to(bus)`` is called.  The decorator is
    **bus-agnostic** — it stores metadata on the function; no subscription
    is created until ``register_to`` is called.

    Supports stacking to subscribe the same method to different event types
    or channels::

        @listen(OrderPlacedEvent, channel="orders")
        @listen(OrderPlacedEvent, channel="audit")    # same method, two channels
        async def on_order(self, event: OrderPlacedEvent) -> None: ...

        @listen(OrderPlacedEvent, OrderUpdatedEvent)  # multiple types, one channel
        async def on_any_order(self, event: Event) -> None: ...

    Args:
        *event_types: One or more event types to subscribe to.  Each type
                      can be a concrete ``Event`` subclass (class-based
                      dispatch — supports inheritance) or an
                      ``__event_type__`` string (exact match only).
        channel:      Channel filter.  Defaults to ``CHANNEL_ALL`` (``"*"``)
                      so the method receives events from any channel unless
                      narrowed explicitly.
        filter:       Optional predicate.  Called with the event after
                      ``event_type`` and ``channel`` have already matched.
                      Return ``True`` to process the event, ``False`` to skip.
        priority:     Dispatch order within a publish call.  Higher values run
                      first.  Equal priorities run in subscription order (FIFO).
                      Defaults to ``0``.

    Returns:
        The unmodified function with ``__listen_entries__`` attribute added.

    Raises:
        TypeError: If ``event_types`` is empty (no event type to subscribe to).

    Edge cases:
        - Stacking ``@listen`` appends to the existing ``__listen_entries__``
          list — entries from earlier decorators are preserved.
        - Works on both ``async def`` and plain ``def`` methods.  The bus
          decides how to call the handler.
        - Does NOT work on ``staticmethod`` or ``classmethod`` — the
          descriptor protocol wraps them before ``__listen_entries__`` can
          be set.  Use instance methods only.
        - Applying to a ``lambda`` is possible but the lambda cannot be
          discovered by ``inspect.getmembers`` — use ``@listen`` on proper
          ``def`` methods only.

    Example::

        class OrderConsumer(EventConsumer):
            @listen(OrderPlacedEvent, channel="orders")
            async def on_order_placed(self, event: OrderPlacedEvent) -> None:
                logger.info("Order placed: %s", event.order_id)

            @listen(OrderPlacedEvent,
                    filter=lambda e: e.total > 1000,
                    channel="orders")
            async def on_large_order(self, event: OrderPlacedEvent) -> None:
                await self._alert_team(event)
    """
    if not event_types:
        raise TypeError(
            "@listen requires at least one event_type argument. "
            "Usage: @listen(MyEvent) or @listen(MyEvent, OtherEvent)"
        )

    def decorator(func: Callable) -> Callable:
        # Initialise the entries list on first decoration.
        # Using setdefault-style check so stacking @listen on the same method
        # appends to the existing list rather than overwriting it.
        if not hasattr(func, "__listen_entries__"):
            # Attached directly to the function object — bound method lookup
            # proxies attribute access to __func__, so this is visible via
            # `getattr(bound_method, "__listen_entries__")` at register time.
            func.__listen_entries__: list[_ListenEntry] = []

        for et in event_types:
            func.__listen_entries__.append(
                _ListenEntry(
                    event_type=et, channel=channel, filter=filter, priority=priority
                )
            )

        return func  # Return the original function — no wrapping

    return decorator


# ── EventConsumer ─────────────────────────────────────────────────────────────


class EventConsumer:
    """
    Base class for event consumers.

    Can be used standalone (subclass it, decorate methods with ``@listen``,
    call ``register_to(bus)``) or as a mixin alongside ``AsyncService`` for
    classes that both produce and consume events.

    ``EventConsumer`` is bus-agnostic until ``register_to`` is called.  The
    same instance can be registered to multiple buses (e.g. different
    channels on separate buses), though this is unusual.

    ``register_to`` returns a list of ``Subscription`` handles — retain them
    if you need to deregister later (e.g. during teardown or in tests).

    DESIGN: explicit ``register_to`` over auto-registration in ``__init__``
        ✅ MRO-safe for multiple inheritance — no cooperative ``__init__``
           fragility.
        ✅ The bus is not required at construction time — useful when the
           bus itself is constructed lazily.
        ✅ Works with providify ``@PostConstruct`` for DI-native wiring.
        ❌ Callers must remember to call ``register_to`` — not automatic.
           Mitigated by ``@PostConstruct`` convention documented above.

    Thread safety:  ✅ ``register_to`` is called once during setup.
    Async safety:   ✅ Handlers are invoked by the bus — may be async or sync.

    Edge cases:
        - If the same consumer is registered to the same bus twice, all
          ``@listen`` methods will be subscribed twice — events are
          dispatched to handlers once per subscription.  Avoid double
          registration or cancel the first set of subscriptions first.
        - ``register_to`` walks the MRO via ``inspect.getmembers`` — methods
          defined on parent classes are discovered automatically.
        - Methods without ``__listen_entries__`` are silently skipped.
    """

    def register_to(self, bus: AbstractEventBus) -> list[Subscription]:
        """
        Subscribe all ``@listen``-decorated methods of this instance to ``bus``.

        Scans the instance's MRO for methods with ``__listen_entries__``
        metadata (set by ``@listen``) and calls ``bus.subscribe()`` for
        each entry.

        Args:
            bus: The ``AbstractEventBus`` to register handlers against.

        Returns:
            List of ``Subscription`` handles, one per ``@listen`` entry found.
            Retain these if you need to deregister the consumer later.

        Edge cases:
            - Returns an empty list if no ``@listen`` methods are found.
            - Does NOT deduplicate — stacked ``@listen`` entries on the same
              method each produce their own ``Subscription`` handle.

        Thread safety:  ✅ Called once at setup — not during concurrent dispatch.
        Async safety:   ✅ ``bus.subscribe`` is synchronous — safe to call
                           from any context (sync or async).

        Example::

            consumer = NotificationConsumer()
            subscriptions = consumer.register_to(bus)

            # Later, in teardown:
            for sub in subscriptions:
                sub.cancel()
        """
        subscriptions: list[Subscription] = []

        # inspect.getmembers walks the full MRO — discovers methods from
        # parent classes automatically.  predicate=inspect.ismethod filters
        # out non-callable attributes and unbound functions.
        for _name, method in inspect.getmembers(self, predicate=inspect.ismethod):
            # Bound method attribute lookup proxies to __func__ — entries
            # set by @listen on the raw function are visible here.
            entries: list[_ListenEntry] = getattr(method, "__listen_entries__", [])
            for entry in entries:
                sub = bus.subscribe(
                    entry.event_type,
                    method,
                    channel=entry.channel,
                    filter=entry.filter,
                    priority=entry.priority,
                )
                subscriptions.append(sub)

        return subscriptions

    def __repr__(self) -> str:
        # Count @listen entries across all methods for a useful repr
        count = sum(
            len(getattr(m, "__listen_entries__", []))
            for _, m in inspect.getmembers(self, predicate=inspect.ismethod)
        )
        return f"{type(self).__name__}(listen_entries={count})"
