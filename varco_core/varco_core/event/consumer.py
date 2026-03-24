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

**With retry and DLQ**::

    dlq = InMemoryDeadLetterQueue()

    class OrderConsumer(EventConsumer):
        @listen(
            OrderPlacedEvent,
            channel="orders",
            retry_policy=RetryPolicy(max_attempts=3, base_delay=0.5),
            dlq=dlq,
        )
        async def on_order_placed(self, event: OrderPlacedEvent) -> None:
            # If this raises, it will be retried up to 3 times.
            # If all retries fail, the event lands in dlq.
            await self._fulfillment_service.process(event)

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

Retry and DLQ integration
--------------------------
When ``retry_policy`` is set on ``@listen``, the handler is automatically
wrapped with retry logic at ``register_to()`` time.  The wrapper:

1. Calls the handler.
2. On failure: waits for the configured back-off delay and retries.
3. On exhaustion: if ``dlq`` is set, pushes to the DLQ; otherwise re-raises
   ``RetryExhaustedError``.

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
                    Retry wrappers always use ``asyncio.sleep`` — never block.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from varco_core.event.base import CHANNEL_ALL, AbstractEventBus, Event, Subscription

if TYPE_CHECKING:
    from varco_core.event.dlq import AbstractDeadLetterQueue, DeadLetterEntry
    from varco_core.resilience.retry import RetryPolicy

_logger = logging.getLogger(__name__)


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

    ``channel`` can be a plain string *or* a one-argument callable that
    receives the consumer instance and returns a string.  The callable
    form lets a subclass configure the channel from instance state
    (e.g. ``lambda self: self._channel``) instead of a compile-time
    constant.  ``register_to`` resolves the callable at wiring time,
    when ``self`` is available.

    DESIGN: callable channel over overriding ``register_to``
        ✅ Keeps all wiring logic in one place (``register_to``).
        ✅ ``@listen`` stays declarative — no boilerplate override needed.
        ✅ Backward-compatible — existing string channels work unchanged.
        ❌ ``Callable`` channel is slightly less obvious than a literal string;
           mitigated by clear documentation and type annotations.

    ``retry_policy`` and ``dlq`` are optional.  When ``retry_policy`` is set,
    ``register_to()`` wraps the handler in a retry loop.  When ``dlq`` is also
    set, exhausted events are pushed to the DLQ instead of re-raising.
    Both are stored as ``TYPE_CHECKING``-only imports to avoid circular
    dependency at runtime — the actual types are checked via ``is not None``
    guards, not ``isinstance``.
    """

    event_type: type[Event] | str
    """Event class (class-based dispatch) or ``__event_type__`` string."""

    channel: str | Callable[[Any], str]
    """
    Channel filter.  ``CHANNEL_ALL`` matches any publish channel.

    May be a plain ``str`` (resolved at decoration time) or a one-argument
    callable ``(consumer_instance) -> str`` (resolved at ``register_to``
    time when ``self`` is available).  Use the callable form to reference
    instance attributes:

        @listen(MyEvent, channel=lambda self: self._channel)
        async def handler(self, event: MyEvent) -> None: ...
    """

    filter: Callable[[Event], bool] | None  # noqa: A003
    """Optional predicate applied after event_type and channel match."""

    priority: int = 0
    """Dispatch priority.  Higher values run first.  Defaults to ``0``."""

    retry_policy: RetryPolicy | None = None
    """
    Optional retry policy for this handler.

    When set, ``register_to()`` wraps the handler so that on failure it is
    retried up to ``retry_policy.max_attempts`` times with the configured
    back-off delay.

    If ``dlq`` is also set, events that exhaust all retries are pushed to
    the DLQ.  If only ``retry_policy`` is set (no ``dlq``), exhaustion raises
    ``RetryExhaustedError`` — which propagates to the bus's error policy.
    """

    dlq: AbstractDeadLetterQueue | None = None
    """
    Optional Dead Letter Queue for this handler.

    When set alongside ``retry_policy``, events that exhaust all retry
    attempts are pushed here instead of raising ``RetryExhaustedError``.

    Can be set WITHOUT ``retry_policy`` to capture first-attempt failures
    directly without any retry loop (``max_attempts`` effectively = 1).
    """


# ── @listen decorator ─────────────────────────────────────────────────────────


def listen(
    *event_types: type[Event] | str,
    channel: str | Callable[[Any], str] = CHANNEL_ALL,
    filter: Callable[[Event], bool] | None = None,  # noqa: A002
    priority: int = 0,
    retry_policy: RetryPolicy | None = None,
    dlq: AbstractDeadLetterQueue | None = None,
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

                      Can be a plain ``str`` or a one-argument callable
                      ``(consumer_instance) -> str``.  The callable form
                      lets you reference instance state that is not yet
                      available at class-definition time::

                          @listen(MyEvent, channel=lambda self: self._channel)
                          async def handler(self, event: MyEvent) -> None: ...

                      ``register_to`` resolves the callable with ``self``
                      at wiring time — safe to use any instance attribute.
        filter:       Optional predicate.  Called with the event after
                      ``event_type`` and ``channel`` have already matched.
                      Return ``True`` to process the event, ``False`` to skip.
        priority:     Dispatch order within a publish call.  Higher values run
                      first.  Equal priorities run in subscription order (FIFO).
                      Defaults to ``0``.
        retry_policy: Optional ``RetryPolicy`` controlling retry behaviour.
                      When set, the handler is automatically wrapped with a
                      retry loop at ``register_to()`` time.  Failures matching
                      ``retry_policy.retryable_on`` are retried with back-off.
                      Defaults to ``None`` (no retries — single attempt).
        dlq:          Optional ``AbstractDeadLetterQueue``.  When set alongside
                      ``retry_policy``, events that exhaust all retry attempts
                      are pushed here instead of raising ``RetryExhaustedError``.
                      Can also be set without ``retry_policy`` to capture
                      first-attempt failures directly.  Defaults to ``None``.

    Returns:
        The unmodified function with ``__listen_entries__`` attribute added.

    Raises:
        TypeError: If ``event_types`` is empty (no event type to subscribe to).

    Edge cases:
        - Stacking ``@listen`` appends to the existing ``__listen_entries__``
          list — entries from earlier decorators are preserved.
        - Works on both ``async def`` and plain ``def`` methods.  The bus
          decides how to call the handler.  Retry wrappers always produce
          ``async def`` wrappers, so after wrapping the handler will always
          be async regardless of the original method type.
        - Does NOT work on ``staticmethod`` or ``classmethod`` — the
          descriptor protocol wraps them before ``__listen_entries__`` can
          be set.  Use instance methods only.
        - Applying to a ``lambda`` is possible but the lambda cannot be
          discovered by ``inspect.getmembers`` — use ``@listen`` on proper
          ``def`` methods only.
        - ``retry_policy`` and ``dlq`` are stored as-is at decoration time —
          they are resolved and the wrapper is built at ``register_to()`` time
          when ``self`` is available.

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

            # With retry + DLQ
            @listen(
                PaymentEvent,
                channel="payments",
                retry_policy=RetryPolicy(max_attempts=4, base_delay=1.0),
                dlq=payment_dlq,
            )
            async def on_payment(self, event: PaymentEvent) -> None:
                await self._payment_service.process(event)
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
                    event_type=et,
                    channel=channel,
                    filter=filter,
                    priority=priority,
                    retry_policy=retry_policy,
                    dlq=dlq,
                )
            )

        return func  # Return the original function — no wrapping

    return decorator


# ── _make_retry_wrapper ────────────────────────────────────────────────────────


def _make_retry_wrapper(
    handler: Callable[[Event], Awaitable[None] | None],
    policy: RetryPolicy | None,
    dlq: AbstractDeadLetterQueue | None,
    channel: str,
) -> Callable[[Event], Awaitable[None]]:
    """
    Build an async wrapper that retries ``handler`` and routes to ``dlq`` on exhaustion.

    Called by ``EventConsumer.register_to()`` when an entry has a non-None
    ``retry_policy`` or ``dlq``.  The wrapper is registered with the bus in
    place of the raw handler.

    DESIGN: build wrapper in register_to, not in @listen
        ✅ The resolved channel string is available at wiring time — needed for
           ``DeadLetterEntry.channel``.
        ✅ The bound method (self already captured) is wrapped — no need to pass
           the consumer instance into the closure explicitly.
        ✅ @listen stays purely declarative — no asyncio imports in the decorator.
        ❌ The wrapper is created once per ``register_to()`` call, not once per
           class definition — for typical usage (one registration per app startup)
           this overhead is negligible.

    Args:
        handler:  Bound method to wrap.  May be sync or async — the wrapper
                  always produces an ``async def``.
        policy:   ``RetryPolicy`` controlling attempt count and back-off.
                  ``None`` means a single attempt (no retry loop).
        dlq:      ``AbstractDeadLetterQueue`` for exhausted events.
                  ``None`` means ``RetryExhaustedError`` is re-raised.
        channel:  Resolved channel string captured in the DLQ entry.

    Returns:
        An async callable ``(event: Event) -> None`` suitable for bus.subscribe().

    Async safety: ✅ Uses ``asyncio.sleep`` — never blocks the event loop.

    Edge cases:
        - If ``policy`` is ``None`` but ``dlq`` is set, the handler is called
          once; on failure the event goes directly to the DLQ without retrying.
        - If the handler raises a non-retryable exception (not in
          ``policy.retryable_on``), the exception propagates immediately without
          DLQ routing — these are programmer errors, not transient failures.
        - ``dlq.push()`` errors are swallowed (``AbstractDeadLetterQueue``
          contract) — the wrapper will not crash the event loop on DLQ failure.
    """
    # Import at call time (not module level) to avoid circular imports.
    # consumer.py is imported by event/__init__.py which is imported by dlq.py
    # (dlq.py imports event.base, not event.__init__, so this is safe — but
    # the lazy import makes the dependency explicit and avoidable at parse time).
    from varco_core.event.dlq import DeadLetterEntry
    from varco_core.resilience.retry import RetryExhaustedError

    # Determine the effective attempt count:
    # - policy given → use max_attempts from policy
    # - no policy, just dlq → single attempt (treat as max_attempts=1)
    max_attempts: int = policy.max_attempts if policy is not None else 1
    handler_name: str = getattr(handler, "__qualname__", repr(handler))

    async def wrapper(event: Event) -> None:
        """
        Retry wrapper injected by ``@listen(retry_policy=..., dlq=...)``.

        Runs the original handler up to ``max_attempts`` times.  On exhaustion,
        routes to the DLQ (if configured) or re-raises ``RetryExhaustedError``.

        Async safety: ✅ asyncio.sleep used for back-off — event loop is never
                         blocked between attempts.
        """
        last_exc: BaseException | None = None
        first_failed_at: datetime | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                # Support both sync and async handlers — match the bus dispatch
                # behaviour where the bus itself calls sync or awaits async.
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
                return  # Success — exit immediately

            except BaseException as exc:
                # Non-retryable exceptions (programmer errors like TypeError,
                # AttributeError) bypass the retry loop entirely — they will
                # never succeed on retry and should surface immediately.
                if policy is not None and not policy.is_retryable(exc):
                    raise

                now = datetime.now(tz=timezone.utc)
                if first_failed_at is None:
                    first_failed_at = now

                last_exc = exc

                if attempt < max_attempts:
                    # Compute back-off delay.  When policy is None (dlq-only
                    # mode with max_attempts=1) this branch is never reached.
                    delay = (
                        policy.compute_delay(attempt - 1) if policy is not None else 0.0
                    )
                    _logger.warning(
                        "Handler %r failed (attempt %d/%d) on channel %r "
                        "— event_type=%r, error=%s: %s — retrying in %.2f s",
                        handler_name,
                        attempt,
                        max_attempts,
                        channel,
                        type(event).__name__,
                        type(exc).__name__,
                        exc,
                        delay,
                    )
                    await asyncio.sleep(delay)

        # ── All attempts exhausted ────────────────────────────────────────────
        assert last_exc is not None  # invariant: at least one attempt was made
        assert first_failed_at is not None

        _logger.error(
            "Handler %r exhausted %d attempt(s) on channel %r — "
            "event_type=%r, last_error=%s: %s",
            handler_name,
            max_attempts,
            channel,
            type(event).__name__,
            type(last_exc).__name__,
            last_exc,
        )

        if dlq is not None:
            # Push to DLQ — dlq.push() contract says it never raises.
            entry: DeadLetterEntry = DeadLetterEntry.from_failure(
                event=event,
                channel=channel,
                handler_name=handler_name,
                last_exc=last_exc,
                attempts=max_attempts,
                first_failed_at=first_failed_at,
            )
            await dlq.push(entry)
            _logger.info(
                "Event routed to DLQ: entry_id=%s handler=%r channel=%r",
                entry.entry_id,
                handler_name,
                channel,
            )
        else:
            # No DLQ — re-raise so the bus's ErrorPolicy can handle it.
            raise RetryExhaustedError(
                handler_name, max_attempts, last_exc
            ) from last_exc

    return wrapper


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
                # Resolve callable channels at wiring time — self is available
                # here so instance attributes (e.g. self._channel) can be used
                # as the channel value via: @listen(Event, channel=lambda self: self._channel)
                resolved_channel: str = (
                    entry.channel(self)  # callable → call with instance
                    if callable(entry.channel)
                    else entry.channel  # plain string → use directly
                )

                # Wrap the handler with retry + DLQ logic if configured.
                # The wrapper is built here (not in @listen) so that:
                #   1. The resolved channel string is captured in the closure —
                #      needed for DeadLetterEntry.channel.
                #   2. The bound method (with self already captured) is wrapped
                #      rather than the unbound function.
                actual_handler: Callable[[Event], Awaitable[None] | None] = (
                    _make_retry_wrapper(
                        method,
                        entry.retry_policy,
                        entry.dlq,
                        resolved_channel,
                    )
                    if entry.retry_policy is not None or entry.dlq is not None
                    else method
                )

                sub = bus.subscribe(
                    entry.event_type,
                    actual_handler,
                    channel=resolved_channel,
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
