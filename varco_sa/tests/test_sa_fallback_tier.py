"""
Plan 045 / Step 12 — the varco_sa backend-tier fallback rule + the
`HealthCheck` collection-contribution non-fallback rule (Class C).

Seed for Plan 046's `di_wiring` conformance suite — do not grow this file;
move it there.

`SAModule.sa_advisory_lock` (B8) is `@Fallback @Provider(singleton=True,
priority=BACKEND_PRIORITY)` — it outranks the in-process `InMemoryLock`
default (A5) once `varco_sa` is installed, and yields to any application
`AbstractDistributedLock` binding registered after. `HealthCheck`
contributions (C1-2) are plain `@Singleton(qualifier=...)` — NEVER
`@Fallback`, because a fallback is excluded from `get_all()` the moment any
non-fallback sibling exists, which would silently empty `/health` the
instant an app added its own check. No Docker/DB required — `get_binding()`/
`get_all_bindings()` are pure reads.

These are passing contract pins. Note the accompanying fix on
`SAModule.sa_xact_advisory_lock` (also declared `@Fallback`, Plan 045) —
without it, that binding's incidental `issubclass`-based match against the
bare `AbstractDistributedLock` request would win over `sa_advisory_lock`
the moment the latter became a fallback (see its own docstring).
"""

from __future__ import annotations

from providify import DIContainer, Singleton
from varco_core.di import BACKEND_PRIORITY
from varco_core.health import HealthCheck
from varco_core.lock import AbstractDistributedLock
from varco_sa.di import SAModule


async def test_sa_advisory_lock_outranks_in_memory_default() -> None:
    container = DIContainer()
    container.scan("varco_core", recursive=True)
    await container.ainstall(SAModule)

    binding = container.get_binding(AbstractDistributedLock)

    assert binding.fallback is True
    assert binding.priority == BACKEND_PRIORITY
    # ProviderBinding (the @Provider shape SAModule uses) has no
    # `.implementation` attribute — that's a ClassBinding-only field.
    # `.fn.__name__` is the provider-shape equivalent identity check.
    assert binding.fn.__name__ == "sa_advisory_lock"


async def test_user_lock_provider_registered_after_install_wins() -> None:
    from providify import Provider

    class UserLock(AbstractDistributedLock):
        async def try_acquire(self, key: str, *, ttl: float = 30.0):  # pragma: no cover - stub
            raise NotImplementedError

        async def release(self, key: str, token) -> None:  # pragma: no cover - stub
            return None

    @Provider(singleton=True)
    def user_lock() -> AbstractDistributedLock:
        return UserLock()

    container = DIContainer()
    container.scan("varco_core", recursive=True)
    await container.ainstall(SAModule)
    container.provide(user_lock)

    binding = container.get_binding(AbstractDistributedLock)

    assert isinstance(container.get(AbstractDistributedLock), UserLock)
    assert binding.fallback is False


def test_health_check_contributions_are_never_fallback() -> None:
    container = DIContainer()
    container.scan("varco_sa", recursive=True)

    bindings = container.get_all_bindings(HealthCheck)
    assert bindings, "expected at least one varco_sa HealthCheck binding"
    assert all(b.fallback is False for b in bindings)


def test_health_check_stays_present_after_a_user_adds_their_own() -> None:
    class UserHealthCheck(HealthCheck):
        async def check(self):  # pragma: no cover - stub
            raise NotImplementedError

    Singleton(UserHealthCheck)

    container = DIContainer()
    container.scan("varco_sa", recursive=True)
    container.bind(HealthCheck, UserHealthCheck)

    bindings = container.get_all_bindings(HealthCheck)
    implementations = {b.implementation for b in bindings}

    assert UserHealthCheck in implementations
    assert len(implementations) >= 2, (
        "a varco HealthCheck must never vanish from get_all() just because "
        "a user added their own — it must never be @Fallback"
    )
