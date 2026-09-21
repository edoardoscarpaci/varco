"""
Plan 045 / Step 12 — the varco_kafka backend-tier fallback rule.

Seed for Plan 046's `di_wiring` conformance suite — do not grow this file;
move it there.

`KafkaEventBus` is `@Fallback @Singleton(priority=BACKEND_PRIORITY,
qualifier="kafka")` (B1): it outranks the in-process `InMemoryEventBus`
default (A2, priority 0) once both `varco_core` and `varco_kafka` are
scanned, and it still yields to any application binding of
`AbstractEventBus` registered after `bootstrap()`. No Docker/broker
required — `get_binding()` is a pure read.

RED MODE: `KafkaEventBus` is not `@Fallback` yet and `varco_core.di` does
not exist — every assertion below is expected to fail
(`ModuleNotFoundError`/`AttributeError`/`AssertionError`).
"""

from __future__ import annotations

from providify import DIContainer, Singleton
from varco_core.di import BACKEND_PRIORITY
from varco_core.event import AbstractEventBus
from varco_core.event.memory import InMemoryEventBus
from varco_kafka.bus import KafkaEventBus
from varco_kafka.di import bootstrap


def test_kafka_bus_outranks_in_memory_default() -> None:
    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bootstrap(container)

    binding = container.get_binding(AbstractEventBus)

    assert binding.implementation is KafkaEventBus
    assert binding.fallback is True
    assert binding.priority == BACKEND_PRIORITY


def test_in_memory_default_still_resolves_via_qualifier() -> None:
    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bootstrap(container)

    binding = container.get_binding(AbstractEventBus, qualifier="in_memory")

    assert binding.implementation is InMemoryEventBus


def test_user_binding_registered_after_bootstrap_wins() -> None:
    class UserBus(AbstractEventBus):
        pass

    Singleton(UserBus)

    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bootstrap(container)
    container.bind(AbstractEventBus, UserBus)

    binding = container.get_binding(AbstractEventBus)

    assert binding.implementation is UserBus
