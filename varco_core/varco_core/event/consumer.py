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
import warnings
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, cast

from providify import Multibound

from varco_core.event.base import CHANNEL_ALL, AbstractEventBus, Event, Subscription

if TYPE_CHECKING:
    from varco_core.event.deduplication import AbstractDeduplicator
    from varco_core.event.dlq import AbstractDeadLetterQueue, DeadLetterEntry
    from varco_core.resilience.retry import RetryPolicy
    from varco_core.service.inbox import InboxRepository

_logger = logging.getLogger(__name__)


# ── _ListenEntry ──────────────────────────────────────────────────────────────


class _Unset:
    """
    Private sentinel type for ``@listen``'s ``retry_policy=``/``dlq=``
    (Plan 009, Phase 9 / R5, RD-7).

    DESIGN: sentinel over ``None`` as the "not passed" default
        ✅ An explicitly passed ``retry_policy=None`` (or ``dlq=None``) must
           still mean "no retry / no DLQ, and do NOT inherit the process-wide
           default preset" — otherwise there is no way to opt a single
           handler *out* of a global default once one is set.
        ✅ Omission (`_UNSET`) resolves through `register_to()`'s own
           fallback params, then the global default preset, then nothing —
           see `EventConsumer.register_to()`.
        ❌ A private sentinel in a public-facing decorator signature — every
           doc/IDE renders it as `...`; this docstring is the explanation.

    Never compare with ``==`` — always ``is _UNSET`` (there is exactly one
    instance, constructed once at module import).
    """


_UNSET = _Unset()


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

    retry_policy: RetryPolicy | None | _Unset = _UNSET
    """
    Optional retry policy for this handler.

    Defaults to the private ``_UNSET`` sentinel, NOT ``None`` (Plan 009,
    Phase 9 / R5, RD-7) — omission is distinguishable from an explicitly
    passed ``retry_policy=None``, which must NOT inherit the process-wide
    default ``ReliabilityPreset``. See ``_Unset``'s docstring and
    ``EventConsumer.register_to()``'s resolution order.

    Annotated ``RetryPolicy | None | _Unset`` (Plan 021 §D4) — the prior
    ``RetryPolicy | None`` annotation was simply wrong: it excluded the
    sentinel the field is actually assigned by default, which is why the
    ``is not _UNSET`` identity check at ``register_to()`` used to need a
    ``# type: ignore[comparison-overlap]``. Widening the annotation to match
    reality removes the need for any suppression; the runtime behaviour is
    unchanged.

    When set, ``register_to()`` wraps the handler so that on failure it is
    retried up to ``retry_policy.max_attempts`` times with the configured
    back-off delay.

    If ``dlq`` is also set, events that exhaust all retries are pushed to
    the DLQ.  If only ``retry_policy`` is set (no ``dlq``), exhaustion raises
    ``RetryExhaustedError`` — which propagates to the bus's error policy.
    """

    dlq: AbstractDeadLetterQueue | None | _Unset = _UNSET
    """
    Optional Dead Letter Queue for this handler.

    Same ``_UNSET``-sentinel default and resolution rule as ``retry_policy``
    above (Plan 009, Phase 9 / R5, RD-7).

    Annotated ``AbstractDeadLetterQueue | None | _Unset`` (Plan 021 §D4) —
    same annotation-was-wrong fix as ``retry_policy`` above.

    When set alongside ``retry_policy``, events that exhaust all retry
    attempts are pushed here instead of raising ``RetryExhaustedError``.

    Can be set WITHOUT ``retry_policy`` to capture first-attempt failures
    directly without any retry loop (``max_attempts`` effectively = 1).
    """

    deduplicator: AbstractDeduplicator | None = None
    """
    Optional deduplicator for this handler.

    When set, the retry wrapper calls ``is_duplicate(event.event_id)`` before
    invoking the handler.  If the event is a duplicate, the handler is skipped
    silently — no retry loop, no DLQ entry.

    After a successful handler invocation, ``mark_seen(event.event_id)`` is
    called so subsequent re-deliveries of the same event are suppressed.

    DESIGN: per-handler deduplicator over per-consumer
        ✅ Fine-grained control — different handlers may require different
           deduplication windows or different backends.
        ✅ Idempotent handlers can opt out; non-idempotent handlers opt in.
        ❌ Sharing the same deduplicator instance across multiple @listen
           decorations is the recommended pattern — less setup boilerplate.

    Edge cases:
        - A handler that raises is NOT marked as seen — the deduplicator
          leaves the event eligible for re-delivery and retry.
        - ``deduplicator`` can be set without ``retry_policy`` — in that case
          first-attempt failures are NOT retried but the event is also not
          marked as seen (re-deliveries will be re-processed).
    """

    inbox: InboxRepository | None = None
    """
    Optional inbox repository for this handler.

    When set, ``register_to()`` wraps the handler with ``_make_inbox_wrapper``
    as the outermost layer.  Before the handler (and any retry loop) runs, the
    incoming event is persisted as an ``InboxEntry``.  After successful
    completion, the entry is marked processed.

    If the handler raises (including after all retry attempts), the entry is
    NOT marked processed — ``InboxPoller`` will replay the event on the next
    poll tick.

    DESIGN: inbox wrapper is outermost (wraps retry + dedup wrappers)
        ✅ ``inbox.save`` fires once per delivery — not once per retry attempt.
        ✅ ``mark_processed`` fires only after the outermost wrapper returns
           without exception — the entry stays unprocessed until truly done.
        ❌ If ``inbox.save`` fails, the handler is NOT called — the event is
           neither processed nor saved.  This is the rarest failure mode; the
           broker may re-deliver if not yet ACKed.

    Stacking order in ``register_to``::

        inbox_wrapper(
            retry_wrapper(     # deduplication check inside
                handler
            )
        )

    Edge cases:
        - ``inbox`` can be combined with ``deduplicator``, ``retry_policy``,
          and ``dlq`` — each layer is independent and composable.
        - If ``inbox`` is set without a deduplicator, re-delivery by
          ``InboxPoller`` will cause the handler to run again.  Combine with
          a deduplicator to achieve at-most-once semantics.
        - The ``InboxRepository`` instance is captured at ``@listen``
          decoration time — use a thread-safe / async-safe implementation
          (e.g. ``SAInboxRepository`` with a session-per-call pattern).
    """


# ── @listen decorator ─────────────────────────────────────────────────────────


def listen(
    *event_types: type[Event] | str,
    channel: str | Callable[[Any], str] = CHANNEL_ALL,
    filter: Callable[[Event], bool] | None = None,  # noqa: A002
    priority: int = 0,
    retry_policy: RetryPolicy | None = _UNSET,  # type: ignore[assignment]
    dlq: AbstractDeadLetterQueue | None = _UNSET,  # type: ignore[assignment]
    deduplicator: AbstractDeduplicator | None = None,
    inbox: InboxRepository | None = None,
) -> Callable[..., Any]:
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
        deduplicator: Optional ``AbstractDeduplicator``.  When set, the retry
                      wrapper checks ``is_duplicate(event.event_id)`` before
                      calling the handler.  Duplicate events are silently skipped.
                      After successful handler invocation, ``mark_seen`` is called.
                      Defaults to ``None`` (no deduplication).
        inbox:        Optional ``InboxRepository``.  When set, the handler is
                      wrapped with inbox save-before / mark-after logic.  The
                      incoming event is persisted as an ``InboxEntry`` before the
                      handler runs; the entry is marked processed on success.
                      On handler failure the entry is left unprocessed so
                      ``InboxPoller`` can replay it.  Defaults to ``None``.

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

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        # Initialise the entries list on first decoration.
        # Using setdefault-style check so stacking @listen on the same method
        # appends to the existing list rather than overwriting it.
        if not hasattr(func, "__listen_entries__"):
            # Attached directly to the function object — bound method lookup
            # proxies attribute access to __func__, so this is visible via
            # `getattr(bound_method, "__listen_entries__")` at register time.
            func.__listen_entries__: list[_ListenEntry] = []  # type: ignore[misc,attr-defined]

        for et in event_types:
            func.__listen_entries__.append(  # type: ignore[attr-defined]
                _ListenEntry(
                    event_type=et,
                    channel=channel,
                    filter=filter,
                    priority=priority,
                    retry_policy=retry_policy,
                    dlq=dlq,
                    deduplicator=deduplicator,
                    inbox=inbox,
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
    *,
    deduplicator: AbstractDeduplicator | None = None,
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
        handler:       Bound method to wrap.  May be sync or async — the wrapper
                       always produces an ``async def``.
        policy:        ``RetryPolicy`` controlling attempt count and back-off.
                       ``None`` means a single attempt (no retry loop).
        dlq:           ``AbstractDeadLetterQueue`` for exhausted events.
                       ``None`` means ``RetryExhaustedError`` is re-raised.
        channel:       Resolved channel string captured in the DLQ entry.
        deduplicator:  Optional ``AbstractDeduplicator``.  When set, the wrapper
                       checks ``is_duplicate`` before the retry loop; duplicates
                       are silently skipped without entering the retry path.

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
        - Deduplication check fires BEFORE the retry loop — a duplicate event
          is skipped on the first delivery check, not after N failed retries.
        - ``mark_seen`` is called after the first successful completion only —
          a failed handler is NOT marked as seen, so re-deliveries are retried.
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
        Retry + deduplication wrapper injected by ``@listen(...)``.

        Flow:
          1. If ``deduplicator`` is set and ``is_duplicate(event.event_id)``
             returns True → skip silently (no retry, no DLQ).
          2. Run the handler up to ``max_attempts`` times.
          3. On success → call ``mark_seen(event.event_id)`` if deduplicator set.
          4. On exhaustion → route to DLQ or raise ``RetryExhaustedError``.

        Async safety: ✅ asyncio.sleep used for back-off — event loop is never
                         blocked between attempts.
        """
        # ── Deduplication check ───────────────────────────────────────────────
        # Fires BEFORE the retry loop — a duplicate is skipped immediately
        # without entering any retry path and without touching the DLQ.
        if deduplicator is not None:
            if await deduplicator.is_duplicate(event.event_id):
                _logger.debug(
                    "Skipping duplicate event event_id=%s handler=%r channel=%r",
                    event.event_id,
                    handler_name,
                    channel,
                )
                return  # Silently skip — not an error

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

                # ── Handler succeeded ─────────────────────────────────────────
                # mark_seen AFTER success (not before) — a crashed handler must
                # not be treated as "done" so re-deliveries can retry it.
                if deduplicator is not None:
                    await deduplicator.mark_seen(event.event_id)

                return  # Success — exit immediately

            except BaseException as exc:
                # Non-retryable exceptions (programmer errors like TypeError,
                # AttributeError) bypass the retry loop entirely — they will
                # never succeed on retry and should surface immediately.
                if policy is not None and not policy.is_retryable(exc):
                    raise

                now = datetime.now(tz=UTC)
                if first_failed_at is None:
                    first_failed_at = now

                last_exc = exc

                if attempt < max_attempts:
                    # Compute back-off delay.  When policy is None (dlq-only
                    # mode with max_attempts=1) this branch is never reached.
                    delay = policy.compute_delay(attempt - 1) if policy is not None else 0.0
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
            "Handler %r exhausted %d attempt(s) on channel %r — event_type=%r, last_error=%s: %s",
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
            # Plan 009, Phase 6 (R4) — stamp the ambient tenant, sourced from
            # context rather than a parameter so every producer (this
            # wrapper, OutboxRelay, the job runner) gets it for free. A
            # framework-level failure outside any tenant_context() correctly
            # gets tenant_id=None — it is not any one tenant's data.
            from varco_core.service.tenant import current_tenant

            tenant_id = current_tenant()
            if tenant_id is not None:
                import dataclasses

                entry = dataclasses.replace(entry, tenant_id=tenant_id)
            await dlq.push(entry)
            _logger.info(
                "Event routed to DLQ: entry_id=%s handler=%r channel=%r",
                entry.entry_id,
                handler_name,
                channel,
            )
        else:
            # No DLQ — re-raise so the bus's ErrorPolicy can handle it.
            raise RetryExhaustedError(handler_name, max_attempts, last_exc) from last_exc

    return wrapper


# ── EventConsumer ─────────────────────────────────────────────────────────────


@Multibound
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

    Plan 051 / DI-5, §D-consumer: a ``@Multibound`` collection seam. A
    ``@Singleton`` ``EventConsumer`` bound in the container is registered to
    the app's ``AbstractEventBus`` by ``CoreModule.start()``, once the bus
    has started — you do **not** need to call ``register_to()`` from
    ``@PostConstruct`` any more::

        @Singleton
        class PostEventConsumer(EventConsumer):
            def __init__(self, bus: Inject[AbstractEventBus]) -> None:
                self._bus = bus  # optional — only needed by hand-passed
                                 # lifecycle wiring (see below), not by
                                 # CoreModule's own discovery.

            @listen(OrderPlacedEvent, channel="orders")
            async def on_order(self, event: OrderPlacedEvent) -> None:
                ...

    ``auto_register: ClassVar[bool] = True`` is the opt-out — set it
    ``False`` on a consumer that must not be auto-registered (e.g. one
    wanting a specific *qualified* bus, or fan-out to a second bus by
    hand). ``is_registered_to(bus)``/``registered_bus_count`` are public
    reads of the registration state ``CoreModule`` (and the asyncapi
    generator) use to avoid double-registering an already-``@PostConstruct``
    -wired consumer.

    ``register_to()`` by hand is still the right tool for: non-DI consumers
    (tests, scripts), ``auto_register = False`` consumers, and fan-out to a
    second bus. The ``_bus`` attribute convention below remains optional —
    needed only by a consumer that is *also* passed to
    ``LifecycleModule``/``VarcoLifespan.register()`` directly rather than
    discovered by ``CoreModule``.

    REQUIRED PATTERN — ``_bus`` attribute for hand-passed lifecycle wiring
    -----------------------------------------------------------------------
    When passing an ``EventConsumer`` directly to
    ``LifecycleModule``/``VarcoLifespan.register()`` (rather than relying on
    ``CoreModule``'s auto-discovery above), the subclass **must** declare
    and assign ``self._bus`` in its ``__init__``. ``start()`` reads
    ``self._bus`` and calls ``self.register_to(self._bus)``. Missing this
    assignment causes ``AttributeError`` at startup.

    If you are NOT using ``VarcoLifespan``/``CoreModule`` (e.g. tests or
    scripts), call ``register_to(bus)`` directly and skip ``_bus`` entirely.

    DESIGN: explicit ``register_to`` over auto-registration in ``__init__``
        ✅ MRO-safe for multiple inheritance — no cooperative ``__init__``
           fragility.
        ✅ The bus is not required at construction time — useful when the
           bus itself is constructed lazily.
        ✅ Works with providify ``@PostConstruct`` for DI-native wiring.
        ❌ Callers must remember to call ``register_to`` — not automatic.
           Mitigated by the ``_bus`` convention for lifecycle-managed consumers.

    Thread safety:  ✅ ``register_to`` is called once during setup.
    Async safety:   ✅ Handlers are invoked by the bus — may be async or sync.

    Edge cases:
        - If the same consumer is registered to the same bus twice,
          ``register_to`` emits a ``RuntimeWarning`` and returns an empty
          list — no duplicate subscriptions are created.  To re-register
          (e.g. after a restart), call ``stop()`` first to cancel the
          existing subscriptions.
        - Registering to different bus instances accumulates subscriptions
          across both — useful for fan-out but uncommon.
        - ``register_to`` walks the MRO via ``inspect.getmembers`` — methods
          defined on parent classes are discovered automatically.
        - Methods without ``__listen_entries__`` are silently skipped.
    """

    # Declared (not initialised) so IDEs and type checkers surface the
    # attribute.  Subclasses must assign it in __init__ when using the
    # VarcoLifespan lifecycle API (start() / stop()).  Marked as AbstractEventBus
    # via TYPE_CHECKING to avoid a runtime import cycle.
    if TYPE_CHECKING:
        _bus: AbstractEventBus

    #: Opt-out for ``CoreModule``'s auto-registration (§D-consumer). A
    #: consumer that must not be registered to the container's unqualified
    #: default bus (e.g. one wanting a specific qualified bus, or wiring
    #: itself by hand) sets this ``False``.
    auto_register: ClassVar[bool] = True

    def is_registered_to(self, bus: AbstractEventBus) -> bool:
        """
        Report whether this consumer has already called ``register_to(bus)``
        for this exact ``bus`` instance.

        Args:
            bus: The bus to check.

        Returns:
            ``True`` iff ``register_to(bus)`` has been called (and
            ``stop()`` has not since cleared it) for this exact instance.

        Edge cases:
            ``False`` before any ``register_to()`` call — ``
            _registered_buses`` is lazily created (§D-consumer).
        """
        registered = getattr(self, "_registered_buses", None)
        return registered is not None and id(bus) in registered

    @property
    def registered_bus_count(self) -> int:
        """The number of distinct bus instances this consumer is registered to."""
        return len(getattr(self, "_registered_buses", ()))

    def register_to(
        self,
        bus: AbstractEventBus,
        *,
        retry_policy: RetryPolicy | None = None,
        dlq: AbstractDeadLetterQueue | None = None,
    ) -> list[Subscription]:
        """
        Subscribe all ``@listen``-decorated methods of this instance to ``bus``.

        Scans the instance's MRO for methods with ``__listen_entries__``
        metadata (set by ``@listen``) and calls ``bus.subscribe()`` for
        each entry.

        Also stores returned subscriptions in ``self._subscriptions`` (lazily
        initialised on first call) so that ``stop()`` can cancel them without
        requiring callers to retain the return value.

        Args:
            bus:          The ``AbstractEventBus`` to register handlers against.
            retry_policy: Applied ONLY to ``@listen`` entries that declared
                          neither ``retry_policy=`` nor ``dlq=`` themselves
                          (Plan 005 Phase 3, U-6). ``None`` (default) — no
                          instance-level override; entries keep whatever
                          ``@listen`` declared (today's behaviour, unchanged).
                          Rationale: ``@listen`` binds at class-definition
                          time, so an instance cannot otherwise supply a
                          policy — ``register_to`` is already the imperative
                          wiring seam (CLAUDE.md layer rule).
            dlq:          Same fallback rule as ``retry_policy``, independently.

        Returns:
            List of ``Subscription`` handles, one per ``@listen`` entry found.
            Retain these if you need to deregister the consumer later; the
            handles are also accessible via ``self._subscriptions``.

        Edge cases:
            - Returns an empty list if no ``@listen`` methods are found.
            - Does NOT deduplicate — stacked ``@listen`` entries on the same
              method each produce their own ``Subscription`` handle.
            - Calling ``register_to`` twice on the **same bus** emits a
              ``RuntimeWarning`` and returns an empty list — no duplicate
              subscriptions are created.  Cancel the first set via
              ``stop()`` if you need to re-register.
            - Calling ``register_to`` on **different buses** is allowed and
              accumulates subscriptions in ``_subscriptions`` — useful for
              fan-out scenarios where the same consumer listens to two buses.

        Thread safety:  ✅ Called once at setup — not during concurrent dispatch.
        Async safety:   ✅ ``bus.subscribe`` is synchronous — safe to call
                           from any context (sync or async).

        Example::

            consumer = NotificationConsumer()
            subscriptions = consumer.register_to(bus)

            # Later, in teardown:
            for sub in subscriptions:
                sub.cancel()
            # — or just call consumer.stop() if using the lifecycle API.
        """
        # ── Double-registration guard ─────────────────────────────────────────
        # Calling register_to(bus) twice on the SAME bus creates duplicate
        # subscriptions — every event is then delivered twice to each handler.
        # This is almost always a bug (e.g. @PostConstruct + start() both called,
        # or register_to() called in __init__ AND in @PostConstruct).
        #
        # We warn instead of raising because:
        #   ✅ The call IS recoverable — the extra subscriptions are redundant
        #      but they do not corrupt state (they just cause double delivery).
        #   ✅ Existing code that relies on double-registration (rare but valid
        #      for fan-out) is not broken.
        #   ❌ Silent double-delivery is very hard to diagnose — the warning
        #      surfaces the bug immediately at the call site.
        #
        # DESIGN: id(bus) instead of `bus is bus` comparison
        #   Using the object identity (id()) as the set key avoids importing
        #   or calling any bus methods at guard time — the guard is zero-cost
        #   for the common (non-duplicate) path.
        if not hasattr(self, "_registered_buses"):
            # Lazy initialisation — same pattern as _warmers in CacheBackend.
            # EventConsumer has no __init__ of its own; an __init__ would force
            # all subclasses to call super().__init__() which is fragile in
            # multiple-inheritance chains.
            object.__setattr__(self, "_registered_buses", set())
        bus_id = id(bus)
        if bus_id in self._registered_buses:  # type: ignore[attr-defined]
            warnings.warn(
                f"{type(self).__name__}.register_to() called twice on the same "
                "bus instance. "
                "This creates duplicate subscriptions — each event will be "
                "delivered twice to every @listen handler. "
                "Use one registration path only: either @PostConstruct or "
                "start(), but not both.",
                RuntimeWarning,
                stacklevel=2,
            )
            return []
        self._registered_buses.add(bus_id)  # type: ignore[attr-defined]

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

                # Resolution order (Plan 009, Phase 9 / R5, RD-7 extends the
                # pre-existing Plan 005 Phase 3 fallback with one more tier):
                #   1. @listen(...)'s own retry_policy=/dlq= — an entry that
                #      passed EITHER explicitly (including `=None`) is never
                #      overridden by anything below, by design: an explicit
                #      `retry_policy=None` must opt a handler OUT of a global
                #      default, not silently inherit it.
                #   2. register_to()'s own retry_policy=/dlq= params (Plan 005
                #      Phase 3, U-6 — an instance-level fallback, unchanged).
                #   3. The process-wide default ReliabilityPreset (Plan 009 —
                #      defaults to ReliabilityPreset.off(), so step 3 is a
                #      no-op unless set_default_reliability_preset() was
                #      called; resolved HERE, at register_to() time, so a
                #      preset set after class definition still applies).
                entry_declared = entry.retry_policy is not _UNSET or entry.dlq is not _UNSET
                # DESIGN: explicit local annotations here, not narrowing.
                # mypy does not narrow an attribute read (`entry.retry_policy`)
                # across a ternary the way it narrows a local variable, so
                # without these the inferred type stays
                # `RetryPolicy | None | _Unset` and every later `is not None`
                # comparison against `effective_retry_policy` mismatches the
                # `_make_retry_wrapper` signature below. Annotating states the
                # already-true post-condition (the `_UNSET` branch was
                # excluded) rather than fighting mypy's narrowing limits.
                effective_retry_policy: RetryPolicy | None
                effective_dlq: AbstractDeadLetterQueue | None
                if entry_declared:
                    # cast(): mypy does not narrow `x is SENTINEL` for a plain
                    # (non-Literal, non-enum) singleton instance — only
                    # `isinstance`/`is None`/enum-member comparisons narrow.
                    # `entry_declared` above already proves neither field is
                    # `_UNSET` reaches this branch as `_UNSET` on its own; the
                    # `None if ... is _UNSET else ...` ternary re-derives the
                    # same fact mypy can't carry into the ternary's type.
                    effective_retry_policy = cast(
                        "RetryPolicy | None",
                        None if entry.retry_policy is _UNSET else entry.retry_policy,
                    )
                    effective_dlq = cast(
                        "AbstractDeadLetterQueue | None",
                        None if entry.dlq is _UNSET else entry.dlq,
                    )
                elif retry_policy is not None or dlq is not None:
                    effective_retry_policy = retry_policy
                    effective_dlq = dlq
                else:
                    from varco_core.reliability.wiring import (
                        get_default_reliability_preset,
                    )

                    _preset = get_default_reliability_preset()
                    effective_retry_policy = _preset.retry_policy
                    effective_dlq = _preset.dlq

                # Build the wrapper stack at wiring time (not at @listen time)
                # so the resolved channel string and bound self are available.
                #
                # Stacking order (outermost first):
                #   inbox_wrapper(         ← save-before / mark-after
                #       retry_wrapper(     ← retry loop + DLQ + dedup check
                #           handler        ← raw bound method
                #       )
                #   )
                #
                # inbox wrapper is outermost so inbox.save fires once before
                # any retry attempt; mark_processed fires only when all retries
                # (and the dedup check) have succeeded.

                # ── Layer 1: retry + DLQ + deduplication ─────────────────────
                # Build the retry wrapper when any of the three inner wrapping
                # concerns are active.  When all are None, register the raw
                # method to avoid unnecessary indirection.
                inner_handler: Callable[[Event], Awaitable[None] | None] = (
                    _make_retry_wrapper(
                        method,
                        effective_retry_policy,
                        effective_dlq,
                        resolved_channel,
                        deduplicator=entry.deduplicator,
                    )
                    if (
                        effective_retry_policy is not None
                        or effective_dlq is not None
                        or entry.deduplicator is not None
                    )
                    else method
                )

                # ── Layer 2 (outermost): inbox save-before / mark-after ───────
                # Imported lazily to avoid a circular import:
                #   consumer → inbox (service layer) → event.base → consumer
                # The import is inside register_to (called at setup time, not
                # per-event) so the one-time cost is negligible.
                if entry.inbox is not None:
                    from varco_core.service.inbox import _make_inbox_wrapper

                    actual_handler: Callable[[Event], Awaitable[None] | None] = _make_inbox_wrapper(  # type: ignore[assignment]
                        inner_handler,
                        entry.inbox,
                        resolved_channel,
                    )
                else:
                    actual_handler = inner_handler

                sub = bus.subscribe(
                    entry.event_type,
                    actual_handler,
                    channel=resolved_channel,
                    filter=entry.filter,
                    priority=entry.priority,
                )
                subscriptions.append(sub)

        # ── Store subscriptions for lifecycle management ──────────────────────
        # Accumulated on self so stop() can cancel without the caller needing
        # to retain the return value.  Lazy-initialised (not in __init__) so
        # EventConsumer remains safe as a mixin — no cooperative __init__
        # fragility introduced.  Extending with a list (not replacing) so
        # multiple register_to() calls accumulate correctly even though
        # double-registration is discouraged.
        if not hasattr(self, "_subscriptions"):
            # First registration — initialise the list now.
            self._subscriptions: list[Subscription] = []
        self._subscriptions.extend(subscriptions)

        return subscriptions

    async def start(self) -> None:
        """
        Lifecycle hook — register all ``@listen`` handlers to ``self._bus``.

        Satisfies the ``AbstractLifecycle`` protocol expected by
        ``VarcoLifespan.register()``.  When called by ``VarcoLifespan``,
        subscriptions are created at the right point in the startup sequence
        (after the bus is running) and cancelled in reverse order on shutdown
        via ``stop()``.

        **Contract**: the subclass must set ``self._bus`` before
        ``start()`` is called — typically in ``__init__`` via DI::

            class PostEventConsumer(EventConsumer):
                def __init__(self, bus: Inject[AbstractEventBus]) -> None:
                    self._bus = bus   # ← required for start()

        If you are NOT using ``VarcoLifespan`` (e.g. a standalone script),
        call ``register_to(bus)`` directly instead.

        DESIGN: start() delegates to register_to() instead of duplicating logic
            ✅ A single implementation path — both ``@PostConstruct`` and the
               lifecycle API call the same ``register_to()`` code.
            ✅ ``register_to()`` remains callable independently for tests and
               non-lifecycle setups.
            ❌ ``self._bus`` is a convention, not enforced by the base class —
               an ``AttributeError`` surfaces immediately if it is missing,
               with a helpful message from the ``getattr`` check below.

        Raises:
            AttributeError: ``self._bus`` is not set on the subclass instance.

        Thread safety:  ✅ Called once at startup by VarcoLifespan.
        Async safety:   ✅ ``register_to`` is synchronous; the coroutine shell
                           is required to satisfy ``AbstractLifecycle``.

        Edge cases:
            - Calling ``start()`` after ``stop()`` re-registers all handlers
              (``register_to`` appends to ``_subscriptions`` again) — correct
              for restart scenarios.
            - Plan 051 / DI-5, §D-consumer: ``start()`` is now **idempotent
              per bus** — if this instance is already registered to
              ``self._bus`` (via a prior ``start()`` or a manual
              ``register_to()``, e.g. from ``@PostConstruct``), it returns
              immediately and does **not** re-subscribe or emit
              ``register_to()``'s double-registration ``RuntimeWarning``.
              Calling ``register_to()`` a second time directly (bypassing
              ``start()``) still warns and no-ops, as documented there.
        """
        bus = getattr(self, "_bus", None)
        if bus is None:
            raise AttributeError(
                f"{type(self).__name__}.start() requires self._bus to be set. "
                "Assign it in __init__: self._bus = bus. "
                "This is required for VarcoLifespan lifecycle management."
            )
        # Plan 051 / DI-5, §D-consumer: idempotent per bus — a consumer
        # already registered to this exact bus (e.g. via @PostConstruct, or
        # a prior start()) is skipped silently rather than emitting
        # register_to()'s double-registration RuntimeWarning.
        if self.is_registered_to(bus):
            return
        self.register_to(bus)

    async def stop(self) -> None:
        """
        Lifecycle hook — cancel all active subscriptions created by this consumer.

        Satisfies the ``AbstractLifecycle`` protocol expected by
        ``VarcoLifespan.register()``.  Called in LIFO order during shutdown,
        so consumers stop before the bus they subscribed to.

        Cancels every ``Subscription`` stored in ``self._subscriptions``
        (accumulated by all prior ``register_to()`` / ``start()`` calls) and
        resets the list to empty.

        DESIGN: cancel all stored subscriptions on stop
            ✅ Symmetrical with start() — every handler registered by this
               consumer is removed on shutdown, even across multiple
               register_to() calls (e.g. if registered to two buses).
            ✅ Idempotent — calling stop() twice is safe; the second call
               finds an empty list and is a no-op.
            ❌ Subscription.cancel() errors are NOT swallowed here —
               they propagate to VarcoLifespan._stop_all(), which logs
               them but continues the shutdown sequence.

        Thread safety:  ✅ Called once at shutdown by VarcoLifespan.
        Async safety:   ✅ Coroutine shell required by AbstractLifecycle;
                           Subscription.cancel() is synchronous.

        Edge cases:
            - If ``register_to()`` was never called, ``_subscriptions`` does
              not exist and ``getattr(..., [])`` returns an empty list — no-op.
            - ``Subscription.cancel()`` is idempotent on most bus implementations.
              Cancelling an already-cancelled subscription is safe.
        """
        # Lazily initialised in register_to — getattr default is correct here:
        # if start() was never called, there is nothing to cancel.
        for sub in getattr(self, "_subscriptions", []):
            sub.cancel()
        # Reset both the subscriptions list and the registered-buses set so
        # a subsequent start() / register_to() can register to the same bus
        # again without triggering the double-registration warning.
        self._subscriptions = []
        # Clear the bus-id set — after stop() all subscriptions are gone so
        # the "same bus" guard must allow re-registration on the next start().
        if hasattr(self, "_registered_buses"):
            self._registered_buses.clear()

    def __repr__(self) -> str:
        # Count @listen entries across all methods for a useful repr
        count = sum(
            len(getattr(m, "__listen_entries__", []))
            for _, m in inspect.getmembers(self, predicate=inspect.ismethod)
        )
        return f"{type(self).__name__}(listen_entries={count})"
