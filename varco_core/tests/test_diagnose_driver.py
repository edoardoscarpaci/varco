"""
Red-mode tests for Plan 052 Step 6 — ``varco_core.di.diagnose``
(§D-driver): ``diagnose()`` and ``run_wiring_preflight()``.

``varco_core.di.diagnose`` does not exist yet, so every test here is
expected to fail with an ``ImportError`` until Plan 052 Step 6 lands the
module.
"""

from __future__ import annotations

import warnings

import pytest
from providify import DIContainer, Inject, Singleton
from varco_core.di import CoreModule
from varco_core.di.diagnose import (
    WiringValidationError,
    diagnose,
    run_wiring_preflight,
)
from varco_core.di.module import VarcoModule


class _UnboundForDriverTest:
    """Module scope so providify's annotation resolver can see it under
    `from __future__ import annotations`."""


@Singleton
class _NeedsUnboundForDriverTest:
    def __init__(self, dep: Inject[_UnboundForDriverTest]) -> None:
        self.dep = dep


class _SpyModule(VarcoModule):
    name = "spy"

    def __init__(self, *, enabled: bool | None = None) -> None:
        super().__init__(enabled=enabled)
        self.calls: list[str] = []

    def _register(self, container: DIContainer) -> None:
        self.calls.append("register")

    async def async_init(self, container: DIContainer) -> None:
        self.calls.append("async_init")

    def validate(self, container: DIContainer) -> list[str]:
        self.calls.append("validate")
        return []

    async def start(self, container: DIContainer) -> None:  # pragma: no cover - must never run
        self.calls.append("start")


async def test_a_diagnose_calls_register_async_init_validate_never_start() -> None:
    spy = _SpyModule()

    await diagnose(DIContainer(), [CoreModule(), spy])

    assert "register" in spy.calls
    assert "async_init" in spy.calls
    assert "validate" in spy.calls
    assert "start" not in spy.calls


async def test_b_async_init_false_skips_async_init() -> None:
    spy = _SpyModule()

    report = await diagnose(DIContainer(), [CoreModule(), spy], async_init=False)

    assert "async_init" not in spy.calls
    assert report.validation.ran is True


async def test_c_validate_false_skips_container_validate_but_runs_module_validate() -> None:
    spy = _SpyModule()

    report = await diagnose(DIContainer(), [CoreModule(), spy], validate=False)

    assert report.validation.ran is False
    assert "validate" in spy.calls


async def test_d_strict_true_raises_on_module_validate_warning() -> None:
    class _ComplainingModule(VarcoModule):
        name = "complainer"

        def _register(self, container: DIContainer) -> None:
            pass

        def validate(self, container: DIContainer) -> list[str]:
            return ["misconfigured"]

    container = DIContainer()
    order = [CoreModule(), _ComplainingModule()]
    for m in order:
        m.register(container)

    with pytest.raises(WiringValidationError):
        await run_wiring_preflight(
            container, modules=order, lifecycle=(), validate=True, strict=True
        )

    # strict=False must not raise for a WARNING-only finding.
    await run_wiring_preflight(container, modules=order, lifecycle=(), validate=True, strict=False)


async def test_e_missing_binding_error_raises_regardless_of_strict() -> None:
    container = DIContainer()
    container.bind(_NeedsUnboundForDriverTest, _NeedsUnboundForDriverTest)
    CoreModule().register(container)

    with pytest.raises(WiringValidationError):
        await run_wiring_preflight(
            container, modules=[CoreModule()], lifecycle=(), validate=True, strict=False
        )


async def test_f_direct_deprecation_warning_captured_during_register() -> None:
    class _WarningModule(VarcoModule):
        name = "warns_directly"

        def _register(self, container: DIContainer) -> None:
            warnings.warn("old thing", DeprecationWarning, stacklevel=2)

    report = await diagnose(DIContainer(), [CoreModule(), _WarningModule()])

    assert any("old thing" in d.subject for d in report.deprecations)


async def test_h_registering_an_already_registered_module_is_idempotent() -> None:
    core = CoreModule()
    container = DIContainer()
    core.register(container)

    # Should not raise even though core was registered before diagnose() runs it again.
    await diagnose(container, [core])


async def test_error_message_lists_each_error_owner_and_message() -> None:
    container = DIContainer()
    container.bind(_NeedsUnboundForDriverTest, _NeedsUnboundForDriverTest)
    CoreModule().register(container)

    with pytest.raises(WiringValidationError) as exc:
        await run_wiring_preflight(
            container, modules=[CoreModule()], lifecycle=(), validate=True, strict=False
        )

    issue = exc.value.report.validation.errors[0]
    text = str(exc.value)
    assert str(issue["owner"]) in text
    assert str(issue["message"]) in text


async def test_strict_error_message_lists_each_warning_finding() -> None:
    class _Noisy(VarcoModule):
        name = "noisy_msg"

        def _register(self, container: DIContainer) -> None:
            pass

        def validate(self, container: DIContainer) -> list[str]:
            return ["fix me please"]

    container = DIContainer()
    mods = [CoreModule(), _Noisy()]
    for m in mods:
        m.register(container)

    with pytest.raises(WiringValidationError) as exc:
        await run_wiring_preflight(
            container, modules=mods, lifecycle=(), validate=True, strict=True
        )

    assert "module.noisy_msg.validate" in str(exc.value)
    assert "fix me please" in str(exc.value)


async def test_async_init_failure_names_the_module() -> None:
    from varco_core.di.diagnose import ModuleAsyncInitError

    class _Boom(VarcoModule):
        name = "boom"

        def _register(self, container: DIContainer) -> None:
            pass

        async def async_init(self, container: DIContainer) -> None:
            raise RuntimeError("kaboom")

    with pytest.raises(ModuleAsyncInitError, match=r"^_Boom\.async_init failed: kaboom$"):
        await diagnose(DIContainer(), [CoreModule(), _Boom()])


async def test_direct_deprecation_also_becomes_an_info_finding() -> None:
    class _WarningModule(VarcoModule):
        name = "warns_directly_2"

        def _register(self, container: DIContainer) -> None:
            warnings.warn("older thing", DeprecationWarning, stacklevel=2)

    report = await diagnose(DIContainer(), [CoreModule(), _WarningModule()])

    assert any(
        f.check == "deprecation.fired" and "older thing" in f.detail for f in report.findings
    )


async def test_header_values_flow_through_diagnose_and_preflight() -> None:
    report = await diagnose(
        DIContainer(),
        [CoreModule()],
        environment="staging",
        enforce="warn",
        enforce_source="env",
    )
    assert (report.environment, report.enforce, report.enforce_source) == ("staging", "warn", "env")

    container = DIContainer()
    CoreModule().register(container)
    pre = await run_wiring_preflight(
        container,
        modules=[CoreModule()],
        lifecycle=(),
        validate=False,
        strict=False,
        environment="production",
        enforce="refuse",
        enforce_source="code",
    )
    assert (pre.environment, pre.enforce, pre.enforce_source) == ("production", "refuse", "code")


def test_capture_window_records_direct_deprecations() -> None:
    from varco_core.di.diagnose import capture_direct_deprecations, direct_deprecation_records

    with capture_direct_deprecations() as caught:
        warnings.warn("windowed", DeprecationWarning, stacklevel=1)

    assert [r.subject for r in direct_deprecation_records(caught)] == ["windowed"]
