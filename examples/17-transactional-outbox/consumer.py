"""
consumer.py
===========
Event consumer for ``OrderCreatedEvent`` in the transactional-outbox example.

``OrderConsumer`` subscribes to ``OrderCreatedEvent`` on the ``"orders"``
channel.  Received events are appended to ``received`` — an in-memory list
that the HTTP handler exposes for inspection.

Registration (Plan 051 / DI-5)
------------------------------
The consumer holds no bus reference.  In a DI-wired app, declare it a
``@Singleton`` bound in the container and ``CoreModule.start()`` registers it
after the bus has started — no ``@PostConstruct`` hook and no hand-written
``register_to()`` call.  This example is deliberately **not** DI-wired (see
``app.py``), so ``create_app`` calls ``consumer.register_to(bus)`` itself — the
documented path for non-DI consumers (tests, scripts, focused examples).

Deduplication
-------------
``SADeduplicator`` is wired into the ``@listen`` decorator via ``deduplicator=``.
The relay may deliver the same outbox entry more than once (e.g., if the process
crashes between ``bus.publish()`` and the ``delete()`` call).  The deduplicator
checks whether the event's ``event_id`` was seen recently and skips duplicates
within the configured TTL window.

DESIGN: SADeduplicator on @listen vs in the handler body
    ✅ The decorator wiring is declarative — the deduplication window is
       visible at class-definition time, not buried in handler logic.
    ✅ Duplicate events never reach ``on_order`` — cleaner handler code.
    ❌ The deduplicator needs a DB connection (``AsyncEngine``), which means
       the consumer is no longer fully in-memory.  Acceptable for this example.

DESIGN: in-memory notification list over a database
    ✅ Keeps the consumer simple — tests can assert on ``received`` directly.
    ❌ State is lost on restart.  In production, persist notifications.

Thread safety:  ❌ Not thread-safe — single asyncio event loop assumed.
Async safety:   ✅ Handler is ``async def``; list.append() is GIL-safe.
"""

from __future__ import annotations

from events import OrderCreatedEvent
from varco_core.event import EventConsumer, listen


class OrderConsumer(EventConsumer):
    """
    Listens for ``OrderCreatedEvent`` messages on the ``"orders"`` channel.

    Published events are appended to ``received`` — an ordered list of all
    ``OrderCreatedEvent`` instances received since this consumer was started.

    Attributes:
        received: Ordered list of ``OrderCreatedEvent`` objects seen so far.

    Args:
        deduplicator: An ``AbstractDeduplicator`` instance (e.g. ``SADeduplicator``).
                      When ``None``, no deduplication is applied.

    Lifecycle:
        Construction creates no subscription.  ``register_to(bus)`` wires
        every ``@listen`` method — ``create_app`` calls it explicitly (this
        example is not DI-wired); a container-bound ``@Singleton`` consumer
        is instead registered by ``CoreModule.start()`` (Plan 051 / DI-5).

    Thread safety:  ❌ Single event loop; ``received`` is not thread-safe.
    Async safety:   ✅ Handler is ``async def``; list.append() is atomic.
    """

    def __init__(
        self,
        *,
        deduplicator=None,
    ) -> None:
        """
        Args:
            deduplicator: Optional ``AbstractDeduplicator`` — wraps the handler
                          so duplicate ``event_id`` values are skipped.
        """
        self._deduplicator = deduplicator
        self.received: list[OrderCreatedEvent] = []

    @listen(OrderCreatedEvent, channel="orders")
    async def on_order(self, event: OrderCreatedEvent) -> None:
        """
        Handle an incoming ``OrderCreatedEvent``.

        Checks the deduplicator (if configured) before processing.  If the
        event's ``event_id`` was already seen within the TTL window, the event
        is silently skipped.

        Args:
            event: The deserialized ``OrderCreatedEvent`` from the bus.

        Edge cases:
            - If the bus delivers duplicate events (relay crash-before-delete),
              the deduplicator prevents double-counting in ``received``.
            - If ``deduplicator`` is ``None``, every event is processed — safe
              for tests that use ``InMemoryEventBus`` (exactly-once delivery).
        """
        if self._deduplicator is not None:
            key = str(event.event_id)
            if await self._deduplicator.is_duplicate(key):
                return
            await self._deduplicator.mark_seen(key)

        self.received.append(event)


__all__ = ["OrderConsumer"]
