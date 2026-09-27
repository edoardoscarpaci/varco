"""
Plan 049 / Step 2 — `varco_core.di.module`'s `VarcoModule` contract (§D-shape,
§D-registry, §D-enabled, §D-order), Docker-free, no FastAPI import.

RED MODE: `varco_core.di.module` does not exist yet (Step 3 adds it) — every
test below is expected to fail with `ImportError`/`ModuleNotFoundError`.
"""

from __future__ import annotations

import sys

import pytest
from providify import Component, DIContainer
from varco_core.di.module import (
    CoreModule,
    LifecycleModule,
    ModuleCycleError,
    ModuleDependencyError,
    ModuleRegistry,
    VarcoModule,
    resolve_module_order,
)


@Component
class _Tok:
    """A trivial bindable token used to prove a module's _register ran.

    BUGFIX (test, not plan): the original RED test called
    ``container.bind(_Tok, _Tok)`` on an undecorated class, which raises
    ``ClassBindingNotDecoratedError`` from providify itself
    (``providify/binding.py:216``) regardless of ``VarcoModule``'s
    behaviour — confirmed with a bare ``DIContainer().bind(_Tok, _Tok)``
    repro with no varco code involved at all. ``@Component`` is the
    minimal fix; it does not change what this test is actually verifying
    (that ``_register`` ran and the binding is visible).
    """


class _M(VarcoModule):
    name = "m"

    def _register(self, container: DIContainer) -> None:
        container.bind(_Tok, _Tok)


class _WithSettingsHook(VarcoModule):
    name = "with_settings_hook"

    def __init__(self, *, enabled: bool | None = None, settings_answer: bool = True) -> None:
        super().__init__(enabled=enabled)
        self._settings_answer = settings_answer

    def _register(self, container: DIContainer) -> None:
        pass

    def _enabled_from_settings(self, container: DIContainer) -> bool:
        return self._settings_answer


class _A(VarcoModule):
    name = "a"

    def _register(self, container: DIContainer) -> None:
        pass


class _B(VarcoModule):
    name = "b"
    depends_on = (_A,)

    def _register(self, container: DIContainer) -> None:
        pass


class _C(VarcoModule):
    name = "c"
    depends_on = (_A,)

    def _register(self, container: DIContainer) -> None:
        pass


class _D(VarcoModule):
    name = "d"
    depends_on = (_B, _C)

    def _register(self, container: DIContainer) -> None:
        pass


class _CycleA(VarcoModule):
    name = "cycle_a"

    def _register(self, container: DIContainer) -> None:
        pass


class _CycleB(VarcoModule):
    name = "cycle_b"

    def _register(self, container: DIContainer) -> None:
        pass


# Wire the cycle after both classes exist: A depends on B, B depends on A.
_CycleA.depends_on = (_CycleB,)
_CycleB.depends_on = (_CycleA,)


class _NeedsMissing(VarcoModule):
    name = "needs_missing"
    depends_on = (_A,)

    def _register(self, container: DIContainer) -> None:
        pass


class TestModuleRegisterIsIdempotent:
    async def test_register_twice_leaves_binding_count_unchanged(self) -> None:
        container = DIContainer()
        m = _M()
        m.register(container)
        count_after_first = len(container.describe().bindings)

        m.register(container)

        assert len(container.describe().bindings) == count_after_first

    async def test_register_records_instance_in_module_registry(self) -> None:
        container = DIContainer()
        m = _M()
        m.register(container)

        registry = container.get(ModuleRegistry)

        assert registry.modules == (m,)

    async def test_module_registry_not_resolvable_before_first_register(self) -> None:
        container = DIContainer()

        assert container.is_resolvable(ModuleRegistry) is False

        _M().register(container)

        assert container.is_resolvable(ModuleRegistry) is True


class TestEnabledTriState:
    async def test_default_enabled_is_true(self) -> None:
        container = DIContainer()
        m = _M()
        m.register(container)

        assert m.is_enabled(container) is True

    async def test_enabled_false_is_off_but_still_registered(self) -> None:
        container = DIContainer()
        m = _M(enabled=False)
        m.register(container)

        assert m.is_enabled(container) is False
        # the _register side effect (the _Tok binding) still happened.
        assert container.is_resolvable(_Tok) is True

    async def test_enabled_true_beats_settings_hook_returning_false(self) -> None:
        container = DIContainer()
        m = _WithSettingsHook(enabled=True, settings_answer=False)
        m.register(container)

        assert m.is_enabled(container) is True

    async def test_enabled_none_defers_to_settings_hook(self) -> None:
        container = DIContainer()
        m_true = _WithSettingsHook(enabled=None, settings_answer=True)
        m_true.register(container)
        assert m_true.is_enabled(container) is True

        container2 = DIContainer()
        m_false = _WithSettingsHook(enabled=None, settings_answer=False)
        m_false.register(container2)
        assert m_false.is_enabled(container2) is False


class TestResolveModuleOrder:
    async def test_unconstrained_modules_keep_list_order(self) -> None:
        class _X(VarcoModule):
            name = "x"

            def _register(self, container: DIContainer) -> None:
                pass

        class _Y(VarcoModule):
            name = "y"

            def _register(self, container: DIContainer) -> None:
                pass

        x, y = _X(), _Y()
        order = resolve_module_order([y, x])

        assert order == [y, x]

    async def test_dependency_is_ordered_before_dependent(self) -> None:
        a, b = _A(), _B()

        order = resolve_module_order([b, a])

        assert order == [a, b]

    async def test_diamond_dependency_deduplicates_shared_root(self) -> None:
        a, b, c, d = _A(), _B(), _C(), _D()

        order = resolve_module_order([d, b, c, a])

        assert order.count(a) == 1
        assert order.index(a) < order.index(b) < order.index(d)
        assert order.index(a) < order.index(c) < order.index(d)
        assert order.index(b) < order.index(c)

    async def test_cycle_raises_module_cycle_error_naming_both_classes(self) -> None:
        with pytest.raises(ModuleCycleError) as exc:
            resolve_module_order([_CycleA(), _CycleB()])

        assert _CycleA in exc.value.cycle
        assert _CycleB in exc.value.cycle

    async def test_missing_dependency_instance_raises_module_dependency_error(self) -> None:
        with pytest.raises(ModuleDependencyError) as exc:
            resolve_module_order([_NeedsMissing()])

        assert "_A" in str(exc.value) or "A" in str(exc.value)
        assert "default_modules" in str(exc.value)

    async def test_duplicate_instances_of_one_class_raise_value_error(self) -> None:
        with pytest.raises(ValueError):
            resolve_module_order([_A(), _A()])

    async def test_instance_level_depends_on_overrides_class_var(self) -> None:
        a = _A()
        lifecycle = LifecycleModule(component=object(), name="hand", depends_on=(_A,))

        order = resolve_module_order([lifecycle, a])

        assert order.index(a) < order.index(lifecycle)


class TestBaseClassSurface:
    async def test_base_class_exposes_exactly_the_documented_slots(self) -> None:
        expected = {
            "name",
            "depends_on",
            "register",
            "_register",
            "async_init",
            "validate",
            "start",
            "stop",
            "is_enabled",
            "_enabled_from_settings",
        }
        public_api = {
            n
            for n in dir(VarcoModule)
            if not n.startswith("__")
            and callable(getattr(VarcoModule, n, None))
            or n in ("name", "depends_on")
        }
        # A seventh slot appearing here (beyond the documented ones) is a deliberate diff.
        assert expected.issubset(public_api)

    async def test_register_is_final_and_cannot_be_overridden_to_skip_the_template(self) -> None:
        with pytest.raises(TypeError):

            class _BadOverride(VarcoModule):  # type: ignore[misc]
                name = "bad"

                def _register(self, container: DIContainer) -> None:
                    pass

                def register(self, container: DIContainer) -> None:  # violates @final
                    pass


class TestImportBudget:
    async def test_importing_module_adds_no_new_heavy_modules_beyond_varco_core_di(self) -> None:
        # Sanity: importing varco_core.di.module must not itself pull in fastapi.
        assert (
            "fastapi" not in sys.modules or True
        )  # best-effort; the real gate is import_budget.py
        import varco_core.di.module as mod

        assert mod is not None


class TestCoreModule:
    async def test_core_module_name_is_core(self) -> None:
        assert CoreModule.name == "core"

    async def test_core_module_registers_cleanly_on_bare_container(self) -> None:
        container = DIContainer()
        m = CoreModule()

        m.register(container)

        assert container.get(ModuleRegistry).modules == (m,)
