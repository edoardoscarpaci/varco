"""
test_registration.py
====================
Broker-free regression tests for how the 14-kafka-order-events example registers its
OrderConsumer (Plan 058 repair — R4: no ``@PostConstruct``-driven
registration; Plan 051 / DI-5 auto-registration).

Every test runs on InMemoryEventBus — no Docker, no broker — so the
registration contract is checked on every run rather than only under
``-m integration``.

DESIGN: in-memory bus instead of the example's real broker
    ✅ ``create_app(..., bus=...)`` already accepts any ``AbstractEventBus``
       — the in-memory bus exercises the exact wiring the broker path uses.
    ✅ Fast and hermetic; the broker round-trip stays in ``test_smoke.py``.
    ❌ Says nothing about broker delivery semantics — out of scope here.

Async safety:   ✅  Each test owns its own bus and container.
"""

from __future__ import annotations

import httpx
from consumer import OrderConsumer
from events import OrderPlacedEvent
from httpx import ASGITransport
from varco_core.event import InMemoryEventBus


def test_consumer_has_no_post_construct_registration_hook() -> None:
    """The consumer takes no bus and creates no subscription on construction."""
    consumer = OrderConsumer()

    # The pre-4.0 shape held the bus and subscribed from a providify
    # post-construct hook named ``_setup``; it must not come back.
    assert "_setup" not in vars(OrderConsumer)
    assert consumer.registered_bus_count == 0


async def test_explicit_register_to_delivers_events() -> None:
    """The non-DI path: ``register_to(bus)`` alone wires ``on_order``."""
    bus = InMemoryEventBus()
    consumer = OrderConsumer()
    consumer.register_to(bus)

    await bus.publish(OrderPlacedEvent(order_id="o-1", amount=1.0), channel="orders")
    await bus.drain()

    assert consumer.is_registered_to(bus) is True
    assert [e.order_id for e in consumer.received] == ["o-1"]


async def test_core_module_start_auto_registers_a_singleton_consumer() -> None:
    """The DI path: a ``@Singleton`` consumer is registered by ``CoreModule.start()``."""
    from providify import DIContainer, Singleton
    from varco_core.di.module import CoreModule
    from varco_core.event import AbstractEventBus

    # Subclass rather than decorate OrderConsumer itself, so the module-level
    # class the non-DI app uses is never mutated by this test.
    @Singleton
    class _BoundOrderConsumer(OrderConsumer):
        pass

    container = DIContainer()
    core = CoreModule(job_runner=False)
    core.register(container)
    container.register(_BoundOrderConsumer)

    await core.start(container)
    try:
        consumer = container.get(_BoundOrderConsumer)
        bus = container.get(AbstractEventBus)
        assert consumer.is_registered_to(bus) is True

        await bus.publish(OrderPlacedEvent(order_id="o-2", amount=2.0), channel="orders")
        if isinstance(bus, InMemoryEventBus):
            await bus.drain()
        assert [e.order_id for e in consumer.received] == ["o-2"]
    finally:
        await core.stop(container)


async def test_create_app_registers_its_consumer() -> None:
    """``create_app`` registers its consumer — a POSTed order shows up as a notification."""
    from app import create_app  # noqa: PLC0415

    bus = InMemoryEventBus()
    app = create_app("unused://in-memory", bus=bus)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        resp = await client.post("/v1/orders", json={"order_id": "o-3", "amount": 3.0})
        assert resp.status_code == 202
        await bus.drain()
        notes = (await client.get("/v1/notifications")).json()

    assert [n["order_id"] for n in notes] == ["o-3"]
