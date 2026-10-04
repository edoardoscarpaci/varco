"""
Plan 049 / Step 8 — `varco_casbin.modules.CasbinModule`, Docker-free.

RED MODE: `varco_casbin.modules` does not exist yet — every test below is
expected to fail with `ImportError`.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_casbin.modules import CasbinModule
from varco_core.di.module import CoreModule


class TestCasbinModule:
    async def test_policy_authorizer_true_binds_policy_engine_authorizer(self) -> None:
        # BUGFIX (test, not plan): CasbinPolicyEngine's @PostConstruct
        # ("start") is async — resolving anything that injects it (here,
        # PolicyEngineAuthorizer -> AbstractAuthorizer) requires
        # `await container.aget(...)`; a sync `container.get(...)` raises
        # providify's own RuntimeError regardless of CasbinModule's
        # behaviour (confirmed with a bare bootstrap()+enable_
        # policy_authorizer() repro, no varco_casbin.modules involved).
        # CoreModule().register() first — CasbinModule.depends_on names it,
        # and AbstractAuthorizer's BaseAuthorizer @Fallback default (which
        # PolicyEngineAuthorizer must outrank) comes from varco_core's own
        # bootstrap(), never varco_casbin's.
        from varco_core.auth.authorizer import AbstractAuthorizer

        container = DIContainer()
        CoreModule().register(container)
        m = CasbinModule(policy_authorizer=True)
        m.register(container)

        binding = await container.aget(AbstractAuthorizer)
        assert type(binding).__name__ == "PolicyEngineAuthorizer"

    async def test_policy_authorizer_false_binds_nothing_extra(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from varco_core.auth.authorizer import AbstractAuthorizer

        # FLIP-5 (Plan 053): the BaseAuthorizer fallback binds only in development.
        monkeypatch.setenv("VARCO_SECURITY_ENV", "development")

        container = DIContainer()
        CoreModule().register(container)
        m = CasbinModule(policy_authorizer=False)
        m.register(container)

        binding = await container.aget(AbstractAuthorizer)
        assert type(binding).__name__ != "PolicyEngineAuthorizer"

    async def test_depends_on_core_module(self) -> None:
        from varco_core.di.module import CoreModule

        assert CoreModule in CasbinModule.depends_on

    async def test_name_is_casbin(self) -> None:
        assert CasbinModule.name == "casbin"
