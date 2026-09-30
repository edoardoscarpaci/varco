"""
Plan 051 (DI-5) — RED tests for the new ``varco_core.di.collect`` primitives:
``contribute()``, ``collect()``, ``acollect()``, ``collection_seams()``.

None of these exist yet (Step 2 of the plan creates
``varco_core/varco_core/di/collect.py``). Every test here is expected to
fail with ``ImportError`` until that module lands.

No Docker, no backend — pure providify + varco_core.di.
"""

from __future__ import annotations

import pytest
from providify import Configuration, DIContainer, Multibound, Provider
from varco_core.di import AlreadyBoundError


@Multibound
class _Seam:
    """A throwaway collection-point ABC-ish type for these tests."""


class _ContribA(_Seam):
    pass


class _ContribB(_Seam):
    pass


@pytest.fixture
def container() -> DIContainer:
    return DIContainer()


async def test_contribute_registers_non_fallback_singleton_binding(container: DIContainer) -> None:
    # contribute() must exist and register a real, non-fallback binding.
    from varco_core.di.collect import contribute

    instance = _ContribA()
    binding = contribute(container, _Seam, instance)

    assert binding.fallback is False
    # providify's ProviderBinding names the resolved interface `.interface`,
    # not `.returns` (the latter is only the `container.provide(...,
    # returns=...)` *keyword* name, verified against the installed 2.1.0
    # wheel — providify/binding.py's ProviderBinding.__init__).
    assert binding.interface is _Seam


async def test_contribute_same_object_twice_raises_already_bound(container: DIContainer) -> None:
    # Identity-based dedup guard (§D-immutable): the exact same object
    # contributed twice is almost certainly a wiring bug (double bootstrap).
    from varco_core.di.collect import contribute

    instance = _ContribA()
    contribute(container, _Seam, instance)

    with pytest.raises(AlreadyBoundError):
        contribute(container, _Seam, instance)


async def test_contribute_two_distinct_equal_objects_both_collected(container: DIContainer) -> None:
    # Two distinct objects (even if "equal") are both legitimate contributions.
    from varco_core.di.collect import collect, contribute

    a = _ContribA()
    b = _ContribA()
    contribute(container, _Seam, a)
    contribute(container, _Seam, b)

    result = collect(container, _Seam)
    assert len(result) == 2


async def test_collect_returns_empty_tuple_on_zero_contributions(container: DIContainer) -> None:
    # Zero-tolerant terminal: an unpopulated @Multibound seam collects to
    # an empty tuple, never a LookupError.
    from varco_core.di.collect import collect

    result = collect(container, _Seam)
    assert result == ()
    assert isinstance(result, tuple)


async def test_collect_returns_tuple_in_registration_order(container: DIContainer) -> None:
    from varco_core.di.collect import collect, contribute

    b = _ContribB()
    a = _ContribA()
    contribute(container, _Seam, b)
    contribute(container, _Seam, a)

    result = collect(container, _Seam)
    assert isinstance(result, tuple)
    assert result == (b, a)


async def test_acollect_awaits_async_provider_contribution(container: DIContainer) -> None:
    # providify's sync _collect_sync refuses async providers; acollect()
    # is required for a seam whose contribution comes from an async factory.
    from varco_core.di.collect import acollect

    @Configuration
    class _AsyncConfig:
        @Provider(singleton=True, returns=_Seam)
        async def make(self) -> _Seam:
            return _ContribA()

    container.install(_AsyncConfig)

    result = await acollect(container, _Seam)
    assert isinstance(result, tuple)
    assert len(result) == 1
    assert isinstance(result[0], _ContribA)


async def test_collection_seams_returns_five_seam_types() -> None:
    from varco_core.di.collect import collection_seams

    seams = collection_seams()
    assert isinstance(seams, tuple)
    assert len(seams) == 5


def test_importing_collect_module_does_not_eagerly_import_seam_modules() -> None:
    # Import-budget guard (§D-verbs): collection_seams() imports the five
    # seam modules lazily inside its own body.
    #
    # NOTE: popping sys.modules entries and never restoring them corrupts
    # every OTHER test in the same process that relies on `varco_core`'s
    # PEP 562 lazy attributes being the *same* object as the freshly
    # re-imported submodule (test_lazy_init.py's identity test) — a stale
    # cached attribute on `varco_core` would then point at a class from a
    # now-orphaned module instance. Snapshot and restore explicitly, in a
    # `finally`, so this test's process-global mutation never leaks.
    import sys

    seam_modules = (
        "varco_core.retention.policy",
        "varco_core.tenancy.source",
        "varco_core.posture",
        "varco_core.event.consumer",
        "varco_core.health",
    )
    saved = {mod: sys.modules.get(mod) for mod in seam_modules}
    for mod in seam_modules:
        sys.modules.pop(mod, None)

    try:
        import varco_core.di.collect  # noqa: F401

        for mod in seam_modules:
            assert mod not in sys.modules, f"{mod} was eagerly imported by varco_core.di.collect"
    finally:
        for mod, module in saved.items():
            if module is not None:
                sys.modules[mod] = module
