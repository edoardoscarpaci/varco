"""
consumer.py
===========
Order consumer for the Kafka order-events example.

``OrderConsumer`` subscribes to ``OrderPlacedEvent`` messages on the
``"orders"`` channel via the ``@listen`` decorator.  Received events are
appended to an in-memory list that the HTTP handler exposes as notifications.

Pattern
-------
The ``@listen`` decorator is **declarative** — it stores metadata on the
method at class-definition time.  No subscription is created until
``register_to(bus)`` is called.  The consumer holds no bus reference at all,
which keeps it bus-agnostic and easy to test.

Registration (Plan 051 / DI-5)
------------------------------
In a DI-wired app, declare the consumer a ``@Singleton`` and bind it in the
container: ``CoreModule.start()`` registers every bound ``EventConsumer``
after the bus has started — no ``@PostConstruct`` hook and no hand-written
``register_to()`` call.  This example is deliberately **not** DI-wired (see
``app.py``), so ``create_app`` calls ``consumer.register_to(bus)`` itself —
the documented path for non-DI consumers (tests, scripts, focused examples).

``retry_policy`` is wired into the ``@listen`` decorator.  The retry wrapper
is built at ``register_to()`` time (not decoration time) so the resolved
channel string and bound ``self`` are available.  The handler will be retried
up to 3 times with exponential back-off before raising.

DESIGN: retry_policy on @listen, no DLQ for this example
    ✅ Shows the retry_policy parameter in a realistic context.
    ✅ Keeps the example self-contained — no DLQ topic needed.
    ✅ After 3 failures the exception propagates to the bus's consume loop,
       which logs it and continues — the broker moves to the next message.
    ❌ Without a DLQ, failed messages are lost after retries are exhausted.
       In production, always pair retry_policy with a KafkaDLQ.

DESIGN: in-memory notification list over a database
    ✅ Keeps the example laser-focused on the event bus mechanics.
    ✅ No persistence dependency — tests run without a DB.
    ❌ State is lost on restart and invisible across multiple processes.
       For production, persist notifications to a database.

Thread safety:  ❌  Not thread-safe — single asyncio event loop assumed.
Async safety:   ✅  ``on_order`` is ``async def``; list.append() is GIL-safe.
"""

from __future__ import annotations

from events import OrderPlacedEvent
from varco_core.event import EventConsumer, listen
from varco_core.resilience.retry import RetryPolicy


class OrderConsumer(EventConsumer):
    """
    Listens for ``OrderPlacedEvent`` messages on the ``"orders"`` Kafka topic.

    Published events are appended to ``received`` — an ordered list of all
    events seen since the consumer was created.  The HTTP layer reads this
    list to serve the ``GET /v1/notifications`` endpoint.

    The handler is wrapped with a ``RetryPolicy(max_attempts=3, base_delay=0.5)``
    — if ``on_order`` raises, the bus retries it up to 3 times before moving on.

    Lifecycle:
        Construction creates no subscription.  ``register_to(bus)`` wires
        every ``@listen`` method — ``create_app`` calls it explicitly (this
        example is not DI-wired); a container-bound ``@Singleton`` consumer
        is instead registered by ``CoreModule.start()`` (Plan 051 / DI-5).

    Attributes:
        received: Ordered list of ``OrderPlacedEvent`` objects seen so far.

    Thread safety:  ❌  Single event loop; ``received`` is not thread-safe.
    Async safety:   ✅  Handler is ``async def``; list.append() is atomic.

    Edge cases:
        - Events published before ``register_to(bus)`` is called are
          silently lost (no subscription exists yet).
        - Kafka AT_LEAST_ONCE delivery may redeliver messages on crash/reconnect.
          The ``received`` list may contain duplicates in that scenario.
        - There is no maximum list size — in production, add eviction or
          pagination to avoid unbounded memory growth.
    """

    def __init__(self) -> None:
        """
        Create the consumer with an empty ``received`` list.

        DESIGN: no bus constructor argument
            ✅ The consumer never holds the bus — only ``register_to()``
               touches it, so the "only EventConsumer.register_to() may call
               the bus" rule holds by construction.
            ✅ The same class works unchanged under DI auto-registration
               (``CoreModule.start()``) and under an explicit
               ``register_to()`` call.
            ❌ The caller must remember to call ``register_to(bus)`` when
               not using a container — ``create_app`` does.
            Rejected: subscribing from a providify post-construct hook —
            superseded by DI auto-registration in 4.0 (Plan 051 / DI-5).
        """
        # In-memory store — accumulates every OrderPlacedEvent received.
        self.received: list[OrderPlacedEvent] = []

    @listen(
        OrderPlacedEvent,
        channel="orders",
        # Retry up to 3 times with exponential back-off before giving up.
        # The wrapper is built lazily at register_to() time so ``self`` and
        # the resolved channel string are available.  base_delay=0.5 means:
        #   attempt 1 → immediate
        #   attempt 2 → wait ~0.5 s
        #   attempt 3 → wait ~1.0 s
        #   → raises after 3 failures (no DLQ wired in this example)
        retry_policy=RetryPolicy(max_attempts=3, base_delay=0.5),
    )
    async def on_order(self, event: OrderPlacedEvent) -> None:
        """
        Handle an incoming ``OrderPlacedEvent``.

        Appends the event to ``self.received`` so the HTTP layer can expose it.

        Args:
            event: The deserialized ``OrderPlacedEvent`` from Kafka.

        Edge cases:
            - If the bus delivers duplicate events (Kafka AT_LEAST_ONCE
              redelivery on crash), duplicates appear in ``received``.
              De-duplication is out of scope for this example.
            - If this method raises, the retry wrapper retries up to 3 times
              before logging the error and continuing to the next message.
        """
        self.received.append(event)


__all__ = ["OrderConsumer"]
