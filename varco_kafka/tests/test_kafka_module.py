"""
Plan 049 / Step 8 — `varco_kafka.modules.KafkaModule`, Docker-free.

RED MODE: `varco_kafka.modules` does not exist yet — every test below is
expected to fail with `ImportError`.
"""

from __future__ import annotations

from providify import DIContainer
from varco_kafka.modules import KafkaModule


class TestKafkaModule:
    async def test_register_does_not_raise_on_bare_container(self) -> None:
        container = DIContainer()
        m = KafkaModule()
        m.register(container)

    async def test_depends_on_core_module(self) -> None:
        from varco_core.di.module import CoreModule

        assert CoreModule in KafkaModule.depends_on

    async def test_name_is_kafka(self) -> None:
        assert KafkaModule.name == "kafka"
