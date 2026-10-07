"""
consumer.py
===========
Order consumer for the NATS JetStream notification-hub example.

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


class OrderConsumer(EventConsumer):
    """
    Listens for ``OrderPlacedEvent`` messages on the ``"orders"`` channel.

    Published events are appended to ``received`` — an ordered list of all
    events seen since the consumer was created.  The HTTP layer reads this
    list to serve the ``GET /v1/notifications`` endpoint.

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
        - NATS JetStream redelivers unacknowledged messages — duplicates may
          appear in ``received``.  De-duplication is out of scope for this
          example.
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

    @listen(OrderPlacedEvent, channel="orders")
    async def on_order(self, event: OrderPlacedEvent) -> None:
        """
        Handle an incoming ``OrderPlacedEvent``.

        Appends the event to ``self.received`` so the HTTP layer can expose it.

        Args:
            event: The deserialized ``OrderPlacedEvent`` from NATS JetStream.

        Edge cases:
            - JetStream delivers at-least-once — duplicates appear in
              ``received`` if the message is redelivered after a crash.
              For exactly-once behaviour, enable ``EXACTLY_ONCE`` semantics
              in ``NatsEventBusSettings`` and make handlers idempotent.
        """
        # Append to the in-memory list — the HTTP endpoint reads this list.
        self.received.append(event)


__all__ = ["OrderConsumer"]
