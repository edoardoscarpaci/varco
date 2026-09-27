"""
Plan 049 / Step 8 — `varco_nats.modules.NatsModule`, Docker-free.

RED MODE: `varco_nats.modules` does not exist yet — every test below is
expected to fail with `ImportError`.
"""

from __future__ import annotations

from providify import DIContainer
from varco_nats.modules import NatsModule


class TestNatsModule:
    async def test_register_does_not_raise_on_bare_container(self) -> None:
        container = DIContainer()
        m = NatsModule()
        m.register(container)

    async def test_depends_on_core_module(self) -> None:
        from varco_core.di.module import CoreModule

        assert CoreModule in NatsModule.depends_on

    async def test_name_is_nats(self) -> None:
        assert NatsModule.name == "nats"
