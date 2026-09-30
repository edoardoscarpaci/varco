"""
Plan 050 / Step 4 — the phase model on `VarcoModule` (§D-phase, §D-components):
`Phase`, `VarcoModule.phase`/`components()`, `resolve_lifecycle_order`,
`phased_components`. Docker-free, no FastAPI.

RED MODE: none of these names exist on `varco_core.di.module`/`varco_core.di.lifecycle`
yet — every test below is expected to fail with `ImportError`/`AttributeError`.
"""

from __future__ import annotations

import pytest
from providify import DIContainer

# Expected to fail today: Phase/LifecycleComponent/resolve_lifecycle_order/
# phased_components/ModulePhaseError do not exist until Plan 050.
from varco_core.di.lifecycle import (  # type: ignore[import-not-found]
    ModulePhaseError,
    Phase,
    phased_components,
    resolve_lifecycle_order,
)
from varco_core.di.module import VarcoModule


class _Bare(VarcoModule):
    name = "bare"

    def _register(self, container: DIContainer) -> None:
        pass


class _Infra(VarcoModule):
    name = "infra"
    phase = Phase.INFRA  # type: ignore[misc]

    def _register(self, container: DIContainer) -> None:
        pass


class _Schedulers(VarcoModule):
    name = "schedulers"
    phase = Phase.SCHEDULERS  # type: ignore[misc]

    def _register(self, container: DIContainer) -> None:
        pass


def test_module_phase_defaults_to_consumers() -> None:
    # The plan's stated default for an app module with no declared phase.
    assert _Bare.phase == Phase.CONSUMERS  # type: ignore[attr-defined]


def test_components_default_yields_one_component_named_and_phased_like_the_module() -> None:
    container = DIContainer()
    m = _Infra()
    components = m.components(container)  # type: ignore[attr-defined]
    assert len(components) == 1
    component = components[0]
    assert component.name == "infra"
    assert component.phase == Phase.INFRA


async def test_default_component_start_stop_delegate_to_the_module_slots() -> None:
    calls: list[str] = []

    class _Recording(VarcoModule):
        name = "recording"

        def _register(self, container: DIContainer) -> None:
            pass

        async def start(self, container: DIContainer) -> None:
            calls.append("start")

        async def stop(self, container: DIContainer) -> None:
            calls.append("stop")

    container = DIContainer()
    m = _Recording()
    component = m.components(container)[0]  # type: ignore[attr-defined]
    await component.start()
    await component.stop()
    assert calls == ["start", "stop"]


def test_resolve_lifecycle_order_sorts_by_phase_with_no_edges() -> None:
    b = _Bare()  # CONSUMERS
    a = _Infra()  # INFRA
    ordered = resolve_lifecycle_order([b, a])
    assert ordered == [a, b]


def test_resolve_lifecycle_order_preserves_dependency_order_within_one_phase() -> None:
    class _First(VarcoModule):
        name = "first"
        phase = Phase.INFRA  # type: ignore[misc]

        def _register(self, container: DIContainer) -> None:
            pass

    class _Second(VarcoModule):
        name = "second"
        phase = Phase.INFRA  # type: ignore[misc]
        depends_on = (_First,)

        def _register(self, container: DIContainer) -> None:
            pass

    first = _First()
    second = _Second()
    ordered = resolve_lifecycle_order([second, first])
    assert ordered == [first, second]


def test_depends_on_pointing_to_a_later_phase_raises_module_phase_error() -> None:
    class _EarlyPhaseDependsOnLatePhase(VarcoModule):
        name = "early"
        phase = Phase.SCHEDULERS  # type: ignore[misc]
        depends_on = (_Infra,)

        def _register(self, container: DIContainer) -> None:
            pass

    # _Infra has phase INFRA < SCHEDULERS, so this is fine — construct a
    # genuinely backwards case instead: a module in an earlier phase that
    # depends_on a module scheduled to start later.
    class _WantsLateModule(VarcoModule):
        name = "wants_late"
        phase = Phase.INFRA  # type: ignore[misc]
        depends_on = (_Schedulers,)

        def _register(self, container: DIContainer) -> None:
            pass

    schedulers = _Schedulers()
    wants_late = _WantsLateModule()

    with pytest.raises(ModulePhaseError) as exc_info:
        resolve_lifecycle_order([wants_late, schedulers])

    assert exc_info.value.module is _WantsLateModule or exc_info.value.module is wants_late
    assert exc_info.value.dependency is _Schedulers or exc_info.value.dependency is schedulers


def test_phased_components_interleaves_two_modules_multi_phase_components() -> None:
    container = DIContainer()

    class _Core(VarcoModule):
        name = "core"
        phase = Phase.INFRA  # type: ignore[misc]

        def _register(self, container: DIContainer) -> None:
            pass

        def components(self, container: DIContainer):  # type: ignore[override]
            from varco_core.di.lifecycle import LifecycleComponent  # noqa: PLC0415

            async def noop() -> None:
                return None

            return [
                LifecycleComponent(name="core.stores", phase=Phase.INFRA, start=noop, stop=noop),
                LifecycleComponent(name="core.bus", phase=Phase.BUS, start=noop, stop=noop),
                LifecycleComponent(
                    name="core.consumers", phase=Phase.CONSUMERS, start=noop, stop=noop
                ),
                LifecycleComponent(
                    name="core.job_runner", phase=Phase.SCHEDULERS, start=noop, stop=noop
                ),
            ]

    class _Other(VarcoModule):
        name = "other"
        phase = Phase.CONSUMERS  # type: ignore[misc]
        depends_on = (_Core,)

        def _register(self, container: DIContainer) -> None:
            pass

        def components(self, container: DIContainer):  # type: ignore[override]
            from varco_core.di.lifecycle import LifecycleComponent  # noqa: PLC0415

            async def noop() -> None:
                return None

            return [
                LifecycleComponent(name="other.infra", phase=Phase.INFRA, start=noop, stop=noop),
                LifecycleComponent(
                    name="other.consumers", phase=Phase.CONSUMERS, start=noop, stop=noop
                ),
            ]

    core = _Core()
    other = _Other()
    components = phased_components([core, other], container)
    names = [c.name for c in components]
    assert names == [
        "core.stores",
        "other.infra",
        "core.bus",
        "core.consumers",
        "other.consumers",
        "core.job_runner",
    ]


def test_dir_guard_lists_seven_slots_plus_is_enabled() -> None:
    # Plan 050 extends 049's dir()-guard test: `phase`/`components` are new
    # public slots on the base class.
    expected = {
        "name",
        "depends_on",
        "phase",
        "register",
        "_register",
        "async_init",
        "validate",
        "start",
        "stop",
        "components",
        "is_enabled",
        "_enabled_from_settings",
    }
    public_api = {
        n
        for n in dir(VarcoModule)
        if not n.startswith("__")
        and (callable(getattr(VarcoModule, n, None)) or n in ("name", "depends_on", "phase"))
    }
    assert expected.issubset(public_api)
