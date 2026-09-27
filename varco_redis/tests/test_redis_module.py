"""
Plan 049 / Step 8 — `varco_redis.modules.RedisModule`, Docker-free.

RED MODE: `varco_redis.modules` does not exist yet — every test below is
expected to fail with `ImportError`.
"""

from __future__ import annotations

from providify import DIContainer
from varco_redis.modules import RedisModule


class TestRedisModule:
    async def test_cache_false_by_default(self) -> None:
        container = DIContainer()
        m = RedisModule()
        m.register(container)

        # async_init is a no-op install-only path when cache=False;
        # nothing to assert beyond "does not raise" without a server.
        await m.async_init(container)

    async def test_async_init_with_cache_true_is_install_only_on_bare_container(self) -> None:
        # §D-suite / Step 8: RedisModule(cache=True).async_init is awaited
        # on a bare container without a server (install only — the
        # provider is lazy, cache.py:578-602) — must not raise / connect.
        container = DIContainer()
        m = RedisModule(cache=True)
        m.register(container)

        await m.async_init(container)  # must not raise even with no Redis server

    async def test_depends_on_core_module(self) -> None:
        from varco_core.di.module import CoreModule

        assert CoreModule in RedisModule.depends_on

    async def test_name_is_redis(self) -> None:
        assert RedisModule.name == "redis"
