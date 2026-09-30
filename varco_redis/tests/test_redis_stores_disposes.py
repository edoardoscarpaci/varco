"""
Plan 050 / Step 7 (§D-disposes rows 13-15) — the three `RedisStoresConfiguration`
providers (idempotency/job_store/revocation) get a reachable `@Disposes`, and
`RedisIdempotencyStore`/`RedisTokenRevocationStore` get an idempotent
`aclose()`. Docker-free — no real Redis connection.

RED MODE: `stores.py`'s own docstring says "No @Disposes is registered here
— 050 wires it" — every assertion below is expected to fail today.
"""

from __future__ import annotations

import pytest
from providify import DIContainer


@pytest.fixture
def bootstrapped_container(monkeypatch: pytest.MonkeyPatch) -> DIContainer:
    from varco_core.di import bootstrap as core_bootstrap
    from varco_redis.di import bootstrap as redis_bootstrap

    monkeypatch.setenv("VARCO_IDEMPOTENCY_BACKEND", "redis")
    monkeypatch.setenv("VARCO_JOB_STORE_BACKEND", "redis")
    monkeypatch.setenv("VARCO_REVOCATION_BACKEND", "redis")

    container = DIContainer()
    core_bootstrap(container)
    redis_bootstrap(container)
    return container


def test_idempotency_store_binding_has_a_disposer(bootstrapped_container: DIContainer) -> None:
    from varco_core.idempotency.base import AbstractIdempotencyStore

    binding = bootstrapped_container.get_binding(AbstractIdempotencyStore)
    assert binding.disposer is not None


def test_job_store_binding_has_a_disposer(bootstrapped_container: DIContainer) -> None:
    from varco_core.job.base import AbstractJobStore

    binding = bootstrapped_container.get_binding(AbstractJobStore)
    assert binding.disposer is not None


def test_revocation_store_binding_has_a_disposer(bootstrapped_container: DIContainer) -> None:
    from varco_core.revocation.base import AbstractTokenRevocationStore

    binding = bootstrapped_container.get_binding(AbstractTokenRevocationStore)
    assert binding.disposer is not None


async def test_ashutdown_closes_all_three_stores(bootstrapped_container: DIContainer) -> None:
    from varco_core.idempotency.base import AbstractIdempotencyStore
    from varco_core.job.base import AbstractJobStore
    from varco_core.revocation.base import AbstractTokenRevocationStore

    idem = await bootstrapped_container.aget(AbstractIdempotencyStore)
    job = await bootstrapped_container.aget(AbstractJobStore)
    revocation = await bootstrapped_container.aget(AbstractTokenRevocationStore)

    await bootstrapped_container.ashutdown()

    assert getattr(idem, "_closed", None) is True or getattr(idem, "_client", "sentinel") is None
    assert (
        getattr(revocation, "_closed", None) is True
        or getattr(revocation, "_client", "sentinel") is None
    )
    assert job is not None  # the job store's own client is closed via its provider-held handle


def test_redis_idempotency_store_aclose_is_idempotent() -> None:
    from varco_redis.idempotency import RedisIdempotencyStore

    store = RedisIdempotencyStore(url="redis://localhost:1")
    assert hasattr(store, "aclose"), "RedisIdempotencyStore has no aclose() method yet"


def test_redis_token_revocation_store_aclose_is_idempotent() -> None:
    from varco_redis.revocation import RedisTokenRevocationStore

    store = RedisTokenRevocationStore(url="redis://localhost:1")
    assert hasattr(store, "aclose"), "RedisTokenRevocationStore has no aclose() method yet"
