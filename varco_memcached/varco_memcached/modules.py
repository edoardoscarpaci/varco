"""
varco_memcached.modules
==========================
``MemcachedModule`` — ``varco_memcached``'s one ``VarcoModule``
(Plan 049 / DI-1, Step 8).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.di.module import CoreModule, VarcoModule

if TYPE_CHECKING:
    from providify import DIContainer


class MemcachedModule(VarcoModule):
    """
    Registers ``varco_memcached``'s ``@Fallback`` defaults
    (``bootstrap()``); ``async_init()`` installs the connection-opening
    ``MemcachedCacheConfiguration`` (install-only — lazy provider, never
    connects on a bare container without a server).
    """

    name = "memcached"
    depends_on = (CoreModule,)

    def _register(self, container: DIContainer) -> None:
        from varco_memcached.di import bootstrap  # noqa: PLC0415

        bootstrap(container)

    async def async_init(self, container: DIContainer) -> None:
        from varco_memcached.di import async_bootstrap  # noqa: PLC0415

        await async_bootstrap(container)


__all__ = ["MemcachedModule"]
