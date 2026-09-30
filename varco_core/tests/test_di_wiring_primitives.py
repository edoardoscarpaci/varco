"""
Plan 046 / Step 1 — `varco_core.di`'s wiring primitives (`AlreadyBoundError`,
`is_user_bound`, `bind_instance`, `bootstrap`), §D-bind / §D-contract.

RED MODE: none of `AlreadyBoundError`, `is_user_bound`, `bind_instance`, or
`bootstrap` exist yet on `varco_core.di` (only `BACKEND_PRIORITY` does) —
every test here is expected to fail with `ImportError`/`AttributeError`.
"""

from __future__ import annotations

import sys

import pytest
from providify import DIContainer, IssueKind


def test_already_bound_error_is_a_runtime_error() -> None:
    # AlreadyBoundError must subclass RuntimeError so existing
    # `except RuntimeError` call sites (e.g. enable_authorization_audit's
    # old double-wrap guard) keep working after the swap (§D-enable).
    from varco_core.di import AlreadyBoundError

    assert issubclass(AlreadyBoundError, RuntimeError)


def test_already_bound_error_message_names_interface_and_qualifier() -> None:
    from varco_core.di import AlreadyBoundError, bind_instance
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()
    bind_instance(container, AbstractFeatureFlags, object(), qualifier="q1")

    with pytest.raises(AlreadyBoundError) as exc:
        bind_instance(container, AbstractFeatureFlags, object(), qualifier="q1")

    message = str(exc.value)
    assert "AbstractFeatureFlags" in message
    assert "q1" in message


def test_is_user_bound_false_on_empty_container() -> None:
    from varco_core.di import is_user_bound
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()

    assert is_user_bound(container, AbstractFeatureFlags) is False


def test_is_user_bound_false_when_only_a_fallback_binding_exists() -> None:
    # A scanned varco default is @Fallback — it must never register as
    # "user bound", or every enable_*/bind_* would spuriously refuse to
    # register alongside varco's own defaults.
    from varco_core.di import is_user_bound
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()
    container.scan("varco_core.flags", recursive=True)

    assert is_user_bound(container, AbstractFeatureFlags) is False


def test_is_user_bound_true_after_bind_instance() -> None:
    from varco_core.di import bind_instance, is_user_bound
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()
    bind_instance(container, AbstractFeatureFlags, object())

    assert is_user_bound(container, AbstractFeatureFlags) is True


def test_is_user_bound_exact_qualifier_when_qualifier_given() -> None:
    from varco_core.di import bind_instance, is_user_bound
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()
    bind_instance(container, AbstractFeatureFlags, object(), qualifier="q1")

    # A different qualifier must not see the "q1" binding as user-bound.
    assert is_user_bound(container, AbstractFeatureFlags, qualifier="q2") is False
    assert is_user_bound(container, AbstractFeatureFlags, qualifier="q1") is True


def test_is_user_bound_unqualified_request_is_inclusive_of_qualified_bindings() -> None:
    # container.py:2096-2101 — an unqualified request also matches qualified
    # candidates, so is_user_bound(None) must see a qualified user binding.
    from varco_core.di import bind_instance, is_user_bound
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()
    bind_instance(container, AbstractFeatureFlags, object(), qualifier="q1")

    assert is_user_bound(container, AbstractFeatureFlags) is True


def test_bind_instance_returns_a_non_fallback_provider_binding() -> None:
    from varco_core.di import bind_instance
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()
    instance = object()

    binding = bind_instance(container, AbstractFeatureFlags, instance, qualifier="q1")

    assert binding.fallback is False
    assert binding.qualifier == "q1"


def test_bind_instance_registers_the_exact_object() -> None:
    from varco_core.di import bind_instance
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()
    instance = object()
    bind_instance(container, AbstractFeatureFlags, instance)

    assert container.get(AbstractFeatureFlags) is instance


def test_bind_instance_same_interface_and_qualifier_twice_raises() -> None:
    from varco_core.di import AlreadyBoundError, bind_instance
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()
    bind_instance(container, AbstractFeatureFlags, object())

    with pytest.raises(AlreadyBoundError):
        bind_instance(container, AbstractFeatureFlags, object())


def test_bind_instance_different_qualifiers_registers_two_bindings() -> None:
    from varco_core.di import bind_instance
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()
    instance1 = object()
    instance2 = object()
    bind_instance(container, AbstractFeatureFlags, instance1, qualifier="q1")
    bind_instance(container, AbstractFeatureFlags, instance2, qualifier="q2")

    assert container.get(AbstractFeatureFlags, qualifier="q1") is instance1
    assert container.get(AbstractFeatureFlags, qualifier="q2") is instance2


def test_bind_instance_over_a_fallback_default_wins_and_shadows_it() -> None:
    from varco_core.di import bind_instance
    from varco_core.flags import AbstractFeatureFlags

    container = DIContainer()
    container.scan("varco_core.flags", recursive=True)
    instance = object()

    bind_instance(container, AbstractFeatureFlags, instance)

    assert container.get(AbstractFeatureFlags) is instance


def test_bind_instance_over_a_provider_shaped_fallback_default_yields_fallback_shadowed_info() -> (
    None
):
    # A13's shape: a @Provider-in-@Configuration default (OtelConfig) whose
    # natural request IS the ABC — bind_instance over it fires
    # FALLBACK_SHADOWED, same as 045 §D-proof pinned for the scanned case.
    from varco_core.di import bind_instance
    from varco_core.observability import OtelConfig
    from varco_core.observability.di import OtelConfiguration

    container = DIContainer()
    container.install(OtelConfiguration)
    bind_instance(container, OtelConfig, OtelConfig(service_name="user-service"))

    report = container.validate(raise_on_error=False)
    assert any(i.kind is IssueKind.FALLBACK_SHADOWED for i in report.infos)


def test_bootstrap_returns_the_same_container() -> None:
    from varco_core.di import bootstrap

    container = DIContainer()

    assert bootstrap(container) is container


def test_bootstrap_twice_adds_no_new_bindings() -> None:
    from varco_core.di import bootstrap

    container = DIContainer()
    bootstrap(container)
    before = len(container.describe().bindings)

    bootstrap(container)

    assert len(container.describe().bindings) == before


async def test_bind_instance_disposer_kwarg_is_called_on_ashutdown() -> None:
    # Plan 050 / §D-asks P-DISP shim — bind_instance(disposer=) attaches a
    # teardown callable providify's own provide()/@Provider have no kwarg
    # for yet (container.py:1250 takes no disposer=).
    from varco_core.di import bind_instance

    class _Thing:
        pass

    container = DIContainer()
    thing = _Thing()
    calls: list[object] = []

    def _dispose(instance: object) -> None:
        calls.append(instance)

    bind_instance(container, _Thing, thing, disposer=_dispose)
    container.get(_Thing)  # force resolution so the singleton cache holds it

    await container.ashutdown()

    assert calls == [thing]


async def test_bind_instance_async_disposer_is_awaited() -> None:
    from varco_core.di import bind_instance

    class _Thing:
        pass

    container = DIContainer()
    thing = _Thing()
    calls: list[object] = []

    async def _dispose(instance: object) -> None:
        calls.append(instance)

    bind_instance(container, _Thing, thing, disposer=_dispose)
    container.get(_Thing)

    await container.ashutdown()

    assert calls == [thing]


def test_importing_varco_core_di_does_not_import_providify() -> None:
    # varco_core.di must import providify inside function bodies only
    # (import-budget reason, §D-contract) — extends 045's
    # test_di_constants.py assertion to the new symbols.
    import subprocess

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import varco_core.di; import sys; "
            "print('providify' in sys.modules or "
            "any(m.startswith('providify.') for m in sys.modules))",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip() == "False", "varco_core.di must not import providify"
