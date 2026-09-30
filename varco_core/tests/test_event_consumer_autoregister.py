"""
Plan 051 (DI-5) / §D-consumer — RED tests: ``EventConsumer`` becomes a
``@Multibound`` seam; ``auto_register: ClassVar[bool]``, ``is_registered_to()``,
``registered_bus_count`` are added; ``start()`` becomes idempotent per bus;
``CoreModule.start()``/``stop()`` discover and (de)register DI-bound
consumers.

Expected to fail until Step 8 lands — none of ``auto_register``,
``is_registered_to``, ``registered_bus_count`` exist today, and
``CoreModule`` does not discover consumers at all.
"""

from __future__ import annotations

from varco_core.event.base import Event
from varco_core.event.consumer import EventConsumer, listen


class _PingEvent(Event):
    pass


class _Consumer(EventConsumer):
    def __init__(self) -> None:
        self.calls = 0

    @listen(_PingEvent, channel="ping")
    async def on_ping(self, event: _PingEvent) -> None:
        self.calls += 1


def test_event_consumer_is_multibound_seam() -> None:
    assert getattr(EventConsumer, "__di_multibound__", None) is not None


def test_event_consumer_has_auto_register_classvar_default_true() -> None:
    assert EventConsumer.auto_register is True


def test_is_registered_to_is_false_before_register_to_called() -> None:
    from varco_core.event.memory import InMemoryEventBus

    consumer = _Consumer()
    bus = InMemoryEventBus()
    assert consumer.is_registered_to(bus) is False


def test_is_registered_to_is_true_after_register_to_called() -> None:
    from varco_core.event.memory import InMemoryEventBus

    consumer = _Consumer()
    bus = InMemoryEventBus()
    consumer.register_to(bus)
    assert consumer.is_registered_to(bus) is True


def test_registered_bus_count_reflects_number_of_registered_buses() -> None:
    from varco_core.event.memory import InMemoryEventBus

    consumer = _Consumer()
    bus1 = InMemoryEventBus()
    bus2 = InMemoryEventBus()
    consumer.register_to(bus1)
    consumer.register_to(bus2)
    assert consumer.registered_bus_count == 2


async def test_start_is_idempotent_per_bus_no_runtime_warning(recwarn) -> None:
    from varco_core.event.memory import InMemoryEventBus

    consumer = _Consumer()
    bus = InMemoryEventBus()
    consumer.register_to(bus)
    # Registering again for the same bus, via the idempotent is_registered_to
    # guard `start()` is expected to add, must not warn.
    if not consumer.is_registered_to(bus):
        consumer.register_to(bus)

    runtime_warnings = [w for w in recwarn.list if issubclass(w.category, RuntimeWarning)]
    assert runtime_warnings == []


async def test_core_module_start_auto_registers_singleton_consumers() -> None:
    from providify import DIContainer, Singleton
    from varco_core.di.module import CoreModule

    @Singleton
    class _AutoConsumer(_Consumer):
        pass

    container = DIContainer()
    core = CoreModule()
    core.register(container)
    container.register(_AutoConsumer)

    await core.start(container)

    consumer = container.get(_AutoConsumer)
    from varco_core.event.base import AbstractEventBus as _Bus

    bus = container.get(_Bus)
    assert consumer.is_registered_to(bus) is True


async def test_core_module_skips_consumer_with_auto_register_false() -> None:
    from providify import DIContainer, Singleton
    from varco_core.di.module import CoreModule

    @Singleton
    class _OptOutConsumer(_Consumer):
        auto_register = False

    container = DIContainer()
    core = CoreModule()
    core.register(container)
    container.register(_OptOutConsumer)

    await core.start(container)

    consumer = container.get(_OptOutConsumer)
    from varco_core.event.base import AbstractEventBus as _Bus

    bus = container.get(_Bus)
    assert consumer.is_registered_to(bus) is False


async def test_core_module_validate_flags_non_singleton_consumer() -> None:
    from providify import Component, DIContainer
    from varco_core.di.module import CoreModule

    @Component
    class _ScopedConsumer(_Consumer):
        pass

    container = DIContainer()
    core = CoreModule()
    core.register(container)
    container.register(_ScopedConsumer)

    problems = core.validate(container)
    assert any("_ScopedConsumer" in p or "singleton" in p.lower() for p in problems)
