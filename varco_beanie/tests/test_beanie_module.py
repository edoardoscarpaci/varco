"""
Plan 049 / Step 8 — `varco_beanie.modules.BeanieModule`, Docker-free.

RED MODE: `varco_beanie.modules` does not exist yet — every test below is
expected to fail with `ImportError`.
"""

from __future__ import annotations

from providify import DIContainer
from varco_beanie.modules import BeanieModule


class TestBeanieModule:
    async def test_passed_settings_is_what_container_resolves_afterward(self) -> None:
        # BUGFIX (test, not plan): BeanieSettings takes `mongo_client=`
        # (an already-constructed AsyncMongoClient, `Any`-typed to avoid a
        # hard pymongo import) + `db_name=`, not a `url=` kwarg — see
        # `varco_beanie/varco_beanie/config.py`'s BeanieSettings. No
        # connection is opened here — this test only proves the passed
        # instance is what the container resolves afterward.
        from varco_beanie.config import BeanieSettings

        container = DIContainer()
        settings = BeanieSettings(mongo_client=object(), db_name="test")
        m = BeanieModule(settings=settings)
        m.register(container)

        assert container.get(BeanieSettings) is settings

    async def test_depends_on_core_module(self) -> None:
        from varco_core.di.module import CoreModule

        assert CoreModule in BeanieModule.depends_on

    async def test_name_is_beanie(self) -> None:
        assert BeanieModule.name == "beanie"
