"""
varco_core.testing
=====================
The whole-app in-memory test profile (Plan 052 / DI-11, §D-testprofile) —
``InMemoryProfile`` + ``in_memory_modules()`` give a downstream test a
whole varco app on in-process defaults with no scheduler, no JWKS refresh,
and a clean ``validate()``. A real, **packaged** module (unlike
``testkit/``, which is never packaged and therefore unreachable from a
downstream project's own tests).

The documented override recipe (README "Testing a varco app",
``dependency-injection.md`` §Testing) swaps **one** binding through
providify's own ``di_container``/``di_overrides`` pytest fixtures —
never a varco wrapper (CLAUDE.md's RL-3 rule: "Verified as already
sound" — document, never wrap):

    # conftest.py
    @pytest.fixture(scope="session")
    def app_container():
        c = DIContainer()
        register_modules(c, in_memory_modules())
        return c

    @pytest.fixture
    def di_container(app_container):
        c = app_container.copy()
        yield c
        c.shutdown()

    # test_orders.py
    async def test_uses_fake_cache(di_container, di_overrides):
        di_overrides.instance(CacheBackend, FakeCache())
        svc = await di_container.aget(OrderService)

Thread safety:  ⚠️ Single-threaded test setup only, same contract as every
                    other ``varco_core.di`` composition-root verb.
Async safety:   ✅ ``InMemoryProfile``'s own slots do no I/O.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.di.lifecycle import Phase
from varco_core.di.module import CoreModule, VarcoModule

if TYPE_CHECKING:
    from collections.abc import Sequence

    import pytest
    from providify import DIContainer

    from varco_core.di.lifecycle import LifecycleComponent

__all__ = [
    "InMemoryProfile",
    "clear_backend_selection",
    "in_memory_modules",
    "register_modules",
]


class InMemoryProfile(VarcoModule):
    """
    The whole-app in-memory profile (brief 015 §5, Quarkus ``@TestProfile``).

    Binds nothing new — the in-process defaults every seam already ships
    *are* the profile. ``_register()`` only closes one pre-existing,
    cosmetic wart: ``InMemoryEventBus.__init__``'s ``error_policy``/
    ``dispatch_mode`` parameters have no DI binding, so providify reports a
    ``MISSING_BINDING_DEFAULTED`` **warning** for both on every bare
    container (harmless — it falls back to the correct Python default —
    but it means ``validate()`` is never warning-clean without an explicit
    binding, which is exactly what this profile promises). ``validate()``
    is the profile's real job: assert every seam/feature is still on its
    in-process default, never fix anything.
    """

    name = "in_memory_profile"
    phase = Phase.EDGE
    depends_on = (CoreModule,)

    def _register(self, container: DIContainer) -> None:
        from varco_core.di import bind_instance, is_user_bound  # noqa: PLC0415
        from varco_core.event.base import DispatchMode, ErrorPolicy  # noqa: PLC0415

        # See the class docstring — silences a cosmetic MISSING_BINDING_DEFAULTED
        # warning that would otherwise make every in-memory-profile container
        # validate non-clean, for a value that was already correct.
        if not is_user_bound(container, ErrorPolicy):
            bind_instance(container, ErrorPolicy, ErrorPolicy.COLLECT_ALL)
        if not is_user_bound(container, DispatchMode):
            bind_instance(container, DispatchMode, DispatchMode.SYNC)

    def validate(self, container: DIContainer) -> Sequence[str]:
        """
        Assert the in-memory shape — every 047 seam is still on its
        ``default``/``env`` (never a real backend) — and return one
        message per seam that is not.
        """
        from varco_core.di import inspect_backend_selection  # noqa: PLC0415

        problems: list[str] = []
        for selection in inspect_backend_selection(container):
            if selection.source not in ("default", "env"):
                problems.append(
                    f"{selection.seam} seam resolved via source={selection.source!r} "
                    f"({selection.value!r}) — the in-memory test profile expects every "
                    f"seam to stay on its in-process default."
                )
        return tuple(problems)

    def components(self, container: DIContainer) -> Sequence[LifecycleComponent]:
        """No lifecycle of its own — a pure assertion module."""
        return ()


def in_memory_modules(*extra: VarcoModule) -> list[VarcoModule]:
    """
    The module list for a whole app on in-process defaults, no scheduler.

    Args:
        extra: Additional ``VarcoModule`` instances the caller wants in the
            mix (e.g. an app's own module, or a backend module under test
            for a deliberate seam-selection assertion via
            ``InMemoryProfile.validate()``).

    Returns:
        ``[CoreModule(job_runner=False), *extra, InMemoryProfile()]`` —
        ``CoreModule``'s ``SCHEDULERS``-phase job-runner component is
        omitted (the store stays bound; nothing polls it).
    """
    return [CoreModule(job_runner=False), *extra, InMemoryProfile()]


def register_modules(container: DIContainer, modules: Sequence[VarcoModule]) -> DIContainer:
    """
    Register every module in dependency order — ``register()`` only
    (never ``async_init``/``start``), the composition-root half every
    in-memory test needs and nothing more.

    Args:
        container: The ``DIContainer`` to register onto.
        modules: The modules to register, in the order passed.

    Returns:
        The same *container*, for chaining.
    """
    from varco_core.di.module import resolve_module_order  # noqa: PLC0415

    for module in resolve_module_order(list(modules)):
        module.register(container)
    return container


def clear_backend_selection(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Delete every ``VARCO_<SEAM>_BACKEND``/``VARCO_<FEATURE>_ENABLED``
    environment variable (``raising=False`` — safe whether or not it was
    set) — for a test that adds a real backend module on purpose and wants
    a clean slate first.

    Args:
        monkeypatch: pytest's ``MonkeyPatch`` fixture.
    """
    from varco_core.di.features import FEATURES  # noqa: PLC0415
    from varco_core.di.seams import SEAMS  # noqa: PLC0415

    for seam in SEAMS:
        monkeypatch.delenv(seam.env, raising=False)
    for feat in FEATURES:
        if feat.env:
            monkeypatch.delenv(feat.env, raising=False)
