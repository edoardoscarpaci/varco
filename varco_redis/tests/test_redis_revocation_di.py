"""
Unit tests for varco_redis.di.enable_redis_token_revocation (Plan 034 / S13b,
Step 34/35). Mirrors test_tls_di.py's "nothing is registered by default"
assertion shape.
"""

from __future__ import annotations

import pytest
from providify import DIContainer


async def test_bootstrap_binds_null_store_not_redis() -> None:
    # Plan 045 / E2: RedisScanNullTokenRevocationStoreDefault is removed —
    # `bootstrap()` now scans varco_core.revocation itself (the real
    # `@Fallback` NullTokenRevocationStore, owned by varco_core), so the
    # sanctioned entry point still resolves the Null default with zero
    # behaviour change for a caller that uses bootstrap().
    from varco_core.revocation import AbstractTokenRevocationStore
    from varco_core.revocation.null import NullTokenRevocationStore
    from varco_redis.di import bootstrap

    container = bootstrap(DIContainer())
    store = await container.aget(AbstractTokenRevocationStore)
    assert isinstance(store, NullTokenRevocationStore)
    await container.ashutdown()


async def test_bare_scan_without_bootstrap_has_no_default() -> None:
    # BREAKING (§Migration): scanning "varco_redis" alone, without bootstrap()
    # or an explicit scan of varco_core.revocation, no longer finds a default
    # AbstractTokenRevocationStore — the old varco_redis-local subclass this
    # used to rely on is deleted (Plan 045 / E2).
    from varco_core.revocation import AbstractTokenRevocationStore

    container = DIContainer()
    container.scan("varco_redis", recursive=True)
    assert not container.is_resolvable(AbstractTokenRevocationStore)
    await container.ashutdown()


async def test_enable_redis_token_revocation_binds_exactly_one_redis_store() -> None:
    from varco_core.revocation import AbstractTokenRevocationStore
    from varco_redis.di import enable_redis_token_revocation
    from varco_redis.revocation import RedisTokenRevocationStore

    container = DIContainer()
    container.scan("varco_redis", recursive=True)
    result = enable_redis_token_revocation(container)

    assert result is container
    store = await container.aget(AbstractTokenRevocationStore)
    assert isinstance(store, RedisTokenRevocationStore)
    await container.ashutdown()


async def test_enable_redis_token_revocation_twice_raises() -> None:
    from varco_core.di import AlreadyBoundError
    from varco_redis.di import enable_redis_token_revocation

    container = DIContainer()
    container.scan("varco_redis", recursive=True)
    enable_redis_token_revocation(container)

    with pytest.raises(AlreadyBoundError):
        enable_redis_token_revocation(container)

    await container.ashutdown()
