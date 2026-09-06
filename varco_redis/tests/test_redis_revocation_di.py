"""
Unit tests for varco_redis.di.enable_redis_token_revocation (Plan 034 / S13b,
Step 34/35). Mirrors test_tls_di.py's "nothing is registered by default"
assertion shape.
"""

from __future__ import annotations

from providify import DIContainer


async def test_default_scan_binds_null_store_not_redis() -> None:
    from varco_core.revocation import AbstractTokenRevocationStore
    from varco_core.revocation.null import NullTokenRevocationStore

    container = DIContainer()
    container.scan("varco_redis", recursive=True)
    store = await container.aget(AbstractTokenRevocationStore)
    assert isinstance(store, NullTokenRevocationStore)
    await container.ashutdown()


async def test_enable_redis_token_revocation_binds_exactly_one_redis_store() -> None:
    from varco_core.revocation import AbstractTokenRevocationStore
    from varco_redis.di import enable_redis_token_revocation
    from varco_redis.revocation import RedisTokenRevocationStore

    container = DIContainer()
    container.scan("varco_redis", recursive=True)
    enable_redis_token_revocation(container)

    store = await container.aget(AbstractTokenRevocationStore)
    assert isinstance(store, RedisTokenRevocationStore)
    await container.ashutdown()
