"""
varco_redis.modules
======================
``RedisModule`` — ``varco_redis``'s one ``VarcoModule`` (Plan 049 / DI-1,
Step 8).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from varco_core.di.lifecycle import Phase
from varco_core.di.module import CoreModule, VarcoModule

if TYPE_CHECKING:
    from providify import DIContainer


class RedisModule(VarcoModule):
    """
    Registers ``varco_redis``'s ``@Fallback`` defaults (``bootstrap()``);
    ``async_init()`` additionally installs the connection-opening
    ``RedisCacheConfiguration`` when ``cache=True``.

    Args:
        cache: When ``True``, ``async_init()`` calls
            ``varco_redis.di.async_bootstrap(container)`` (install-only —
            the cache provider itself is lazy, so this never opens a
            connection on a bare container without a server).
    """

    name = "redis"
    phase = Phase.INFRA
    depends_on = (CoreModule,)

    def __init__(self, *, cache: bool = False, enabled: bool | None = None) -> None:
        super().__init__(enabled=enabled)
        self._cache = cache

    def _register(self, container: DIContainer) -> None:
        from varco_redis.di import bootstrap  # noqa: PLC0415

        bootstrap(container)

    async def async_init(self, container: DIContainer) -> None:
        if not self._cache:
            return
        from varco_redis.di import async_bootstrap  # noqa: PLC0415

        await async_bootstrap(container)


__all__ = ["RedisModule"]
