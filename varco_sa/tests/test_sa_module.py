"""
Plan 049 / Step 8 — `varco_sa.modules.SAModule`, Docker-free.

RED MODE: `varco_sa.modules` does not exist yet — every test below is
expected to fail with `ImportError`.
"""

from __future__ import annotations

from providify import DIContainer
from varco_sa.modules import SAModule


class TestSAModule:
    async def test_passed_config_is_what_container_resolves_afterward(self) -> None:
        # BUGFIX (test, not plan): SAConfig has no `url=` kwarg — it takes
        # a constructed `engine`/`base`/`entity_classes` (see
        # `varco_sa/varco_sa/config.py`'s SAConfig, and the identical
        # construction `test_sa_di_wiring.py::_make_sqlite_sa_config` uses).
        from sqlalchemy.ext.asyncio import create_async_engine
        from sqlalchemy.orm import DeclarativeBase
        from varco_sa.config import SAConfig

        class _Base(DeclarativeBase):
            pass

        container = DIContainer()
        config = SAConfig(
            engine=create_async_engine("sqlite+aiosqlite:///:memory:"),
            base=_Base,
            entity_classes=(),
        )
        m = SAModule(config=config)
        m.register(container)

        assert container.get(SAConfig) is config

    async def test_entities_are_bound_via_bind_repositories(self) -> None:
        # BUGFIX (test, not plan): `RepositoryProvider` (varco_core.providers)
        # is not a `Generic` and is not subscriptable — `bind_repositories()`
        # registers a per-entity `AsyncRepository[D]` binding (its own
        # docstring says so verbatim), the same generic alias
        # `test_sa_di.py`'s own regression tests resolve through.
        from varco_core.model import DomainModel
        from varco_core.repository import AsyncRepository

        class _Entity(DomainModel):
            pass

        container = DIContainer()
        m = SAModule(entities=(_Entity,))
        m.register(container)

        assert container.is_resolvable(AsyncRepository[_Entity])

    async def test_depends_on_core_module(self) -> None:
        from varco_core.di.module import CoreModule

        assert CoreModule in SAModule.depends_on

    async def test_name_is_sa(self) -> None:
        assert SAModule.name == "sa"
