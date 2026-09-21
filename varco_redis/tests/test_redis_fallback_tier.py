"""
Plan 045 / Step 12 — the varco_redis three-level fallback shape (B3/B4/A2).

Seed for Plan 046's `di_wiring` conformance suite — do not grow this file;
move it there.

`RedisEventBusSelectorConfiguration.bus()` (B3) is
`@Fallback @Provider(singleton=True, priority=BACKEND_PRIORITY)` — the
unqualified winner for `AbstractEventBus` once `varco_redis` is bootstrapped.
`RedisEventBus` (B4) stays `@Fallback @Singleton(qualifier="redis")` with NO
priority — deliberately, so it never ties with B3 for an unqualified request
(§D-tiers) — and remains the `qualifier="redis"` escape hatch.
`InMemoryEventBus` (A2) is still reachable via `qualifier="in_memory"`. No
Docker/broker required — `get_binding()` is a pure read.

RED MODE: none of these three bindings carry `@Fallback` yet and
`varco_core.di` does not exist — every assertion below is expected to fail
(`ModuleNotFoundError`/`AttributeError`/`AssertionError`).
"""

from __future__ import annotations

from providify import DIContainer, Singleton
from varco_core.di import BACKEND_PRIORITY
from varco_core.event import AbstractEventBus
from varco_core.event.memory import InMemoryEventBus
from varco_redis.bus import RedisEventBus
from varco_redis.di import bootstrap


def test_redis_selector_is_the_unqualified_winner() -> None:
    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bootstrap(container)

    binding = container.get_binding(AbstractEventBus)

    assert binding.fallback is True
    assert binding.priority == BACKEND_PRIORITY


def test_redis_qualifier_still_resolves_the_concrete_class() -> None:
    container = DIContainer()
    container.scan("varco_core", recursive=True)
    bootstrap(container)

    binding = container.get_binding(AbstractEventBus, qualifier="redis")

    assert binding.implementation is RedisEventBus


def test_in_memory_qualifier_still_resolves_the_in_process_default() -> None:
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
