"""
Plan 049 / Step 9 — `default_modules()`
(`varco_fastapi/varco_fastapi/modules/defaults.py`), Docker-free.

RED MODE: `varco_fastapi.modules.defaults` does not exist yet — every test
below is expected to fail with `ImportError`.
"""

from __future__ import annotations

from providify import DIContainer
from varco_core.di.module import CoreModule, ObservabilityModule, resolve_module_order
from varco_fastapi.modules import (
    BodyLimitModule,
    CorsModule,
    ErrorEnvelopeModule,
    FastAPIModule,
    LocalizationModule,
    LoggingModule,
    SecurityHeadersModule,
    SecurityPostureModule,
    TracingModule,
)
from varco_fastapi.modules.defaults import default_modules

EXPECTED_ORDER = (
    CoreModule,
    ObservabilityModule,
    FastAPIModule,
    CorsModule,
    SecurityHeadersModule,
    ErrorEnvelopeModule,
    BodyLimitModule,
    LoggingModule,
    TracingModule,
    LocalizationModule,
    SecurityPostureModule,
)


class TestDefaultModules:
    async def test_two_calls_return_distinct_instances(self) -> None:
        first = default_modules()
        second = default_modules()

        assert first is not second
        for a, b in zip(first, second):
            assert a is not b

    async def test_order_matches_the_literal_documented_list(self) -> None:
        modules = default_modules()

        assert tuple(type(m) for m in modules) == EXPECTED_ORDER

    async def test_exclude_drops_exactly_the_named_class(self) -> None:
        modules = default_modules(exclude=(SecurityPostureModule,))

        assert not any(isinstance(m, SecurityPostureModule) for m in modules)
        assert len(modules) == len(EXPECTED_ORDER) - 1

    async def test_resolves_without_module_dependency_error(self) -> None:
        modules = default_modules()

        # Must not raise.
        resolve_module_order(modules)

    async def test_every_default_module_is_enabled_except_localization_toggles(self) -> None:
        container = DIContainer()
        modules = default_modules()
        for m in modules:
            m.register(container)

        disabled = {type(m).__name__ for m in modules if not m.is_enabled(container)}

        # LocalizationModule's own i18n/timezone toggles are env-off by
        # default (RD-3) — the module itself still registers "enabled" as a
        # container (its middleware() legitimately yields nothing), so per
        # §D-order/FLIP-1 the literal named-list this test pins is empty
        # unless a reader can show LocalizationModule itself resolves
        # is_enabled() False on a bare container.
        # PLAN-AMBIGUITY: the plan says "every default module's is_enabled
        # on a default container is True except LocalizationModule's two
        # toggles (env-off)" — asserted as a literal name list "so a
        # default flip is a visible diff". The literal list of module
        # CLASS names (not toggle names) is assumed empty here because
        # LocalizationModule.is_enabled() is not itself gated by the two
        # toggles per §D-shape/§D-enabled (is_enabled is a per-module
        # tri-state, not per-toggle) — the toggles instead determine
        # whether middleware()/start() yield anything. If the real
        # implementation makes is_enabled() False when both toggles are
        # off, this literal set becomes {"LocalizationModule"}.
        assert disabled == set()
