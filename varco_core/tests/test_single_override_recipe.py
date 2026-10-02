"""
Plan 052 Step 11 — the "one override, fourteen defaults" recipe
(§D-testprofile), using providify's own ``di_container``/``di_overrides``
pytest fixtures unwrapped (RL-3), never a varco fixture.

This test is the README recipe plus two interim providify-2.1.0
workarounds (P32 ``_collection_points`` copy, P33 ``Instance[T]`` consumer —
see the comments below); keep README/``dependency-injection.md`` §Testing in
step with it.
"""

from __future__ import annotations

import pytest
from providify import ContainerOverrides, DIContainer, Inject, Instance, Singleton
from varco_core.cache.base import CacheBackend
from varco_core.cache.memory import InMemoryCache
from varco_core.event import AbstractEventBus

# Primary red-mode probe.
from varco_core.testing import in_memory_modules, register_modules  # noqa: F401


class FakeCache(InMemoryCache):
    """A distinguishable double for the "swapped one binding" assertion."""


@Singleton
class OrderService:
    # Deviation (Plan 052 Step 11, reported — see
    # design/upstream-gaps/providify-override-instance-scope-leak.md, P33):
    # `di_overrides.instance()` always registers its double at Scope.DEPENDENT
    # (providify 2.1.0), which providify's own scope-leak checker then flags
    # as unsafe for a SINGLETON holding it via plain `Inject[T]` — even
    # though CacheBackend's real production default is itself @Singleton.
    # `Instance[T]` is providify's own documented exemption for exactly this
    # shape (container.py:5749-5757); resolving it once in __init__ keeps
    # `self.cache` a plain, directly-usable object for every other purpose.
    def __init__(self, cache: Instance[CacheBackend], bus: Inject[AbstractEventBus]) -> None:
        self.cache = cache.get()
        self.bus = bus


@pytest.fixture(scope="session")
def app_container() -> DIContainer:
    c = DIContainer()
    register_modules(c, in_memory_modules())
    # Deviation (Plan 052 Step 11, reported): OrderService is a plain
    # @Singleton local to this test module — providify has no implicit
    # scan-on-use, so it needs an explicit container.register() the same
    # way an app's own top-level service would need container.scan("myapp").
    # The original test omitted this and would LookupError on aget().
    c.register(OrderService)
    return c


@pytest.fixture
def di_container(app_container: DIContainer):
    # providify's own documented override shape (pytest_plugin.py:46-50) —
    # redefining di_container is the recipe, not a varco wrapper.
    c = app_container.copy()
    # P32 workaround (design/upstream-gaps/providify-copy-collection-points.md):
    # DIContainer.copy() never copies _collection_points on providify 2.1.0,
    # so any list[T] collection-seam resolution on the copy (here, reached
    # transitively via AbstractEventBus's own event-middleware collection)
    # raises AttributeError. Test-only; deleted once providify ships the fix.
    c._collection_points = set(app_container._collection_points)  # noqa: SLF001
    yield c


async def test_uses_fake_cache(di_container: DIContainer, di_overrides: ContainerOverrides) -> None:
    di_overrides.instance(CacheBackend, FakeCache())

    svc = await di_container.aget(OrderService)

    assert isinstance(svc.cache, FakeCache)
    # instance() drops every binding of the interface — only the fake remains.
    assert len(di_container.get_all_bindings(CacheBackend)) == 1
    validation = di_container.validate(raise_on_error=False)
    assert validation.errors == ()


async def test_second_test_sees_the_real_default_cache_again(di_container: DIContainer) -> None:
    # Overrides from the previous test must be restored at teardown.
    svc = await di_container.aget(OrderService)

    assert isinstance(svc.cache, InMemoryCache)
    assert not isinstance(svc.cache, FakeCache)
