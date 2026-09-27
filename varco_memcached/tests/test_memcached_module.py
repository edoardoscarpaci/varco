"""
Plan 049 / Step 8 — `varco_memcached.modules.MemcachedModule`, Docker-free.

RED MODE: `varco_memcached.modules` does not exist yet — every test below is
expected to fail with `ImportError`.
"""

from __future__ import annotations

from providify import DIContainer
from varco_memcached.modules import MemcachedModule


class TestMemcachedModule:
    async def test_register_does_not_raise_on_bare_container(self) -> None:
        container = DIContainer()
        m = MemcachedModule()
        m.register(container)

    async def test_async_init_is_install_only_on_bare_container(self) -> None:
        container = DIContainer()
        m = MemcachedModule()
        m.register(container)

        await m.async_init(container)  # must not raise/connect

    async def test_depends_on_core_module(self) -> None:
        from varco_core.di.module import CoreModule

        assert CoreModule in MemcachedModule.depends_on

    async def test_name_is_memcached(self) -> None:
        assert MemcachedModule.name == "memcached"
