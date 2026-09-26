"""
Plan 047 / Step 6 — N6-N8: the three new `RedisStoresConfiguration` providers
(idempotency/job_store/revocation), each `@Fallback @selected_by(...)
@Provider(singleton=True, priority=BACKEND_PRIORITY)`, injecting
`Inject[RedisEventBusSettings]` for `.url`.

`get_binding()`-only — no server, no real Redis connection (§D-tests: "All
get_binding()-based — no Docker").

RED MODE: `varco_redis/varco_redis/stores.py` (`RedisStoresConfiguration`)
does not exist yet — every test here is expected to fail with
`ModuleNotFoundError`/`ImportError`, or a `LookupError` from providify when
the binding is looked up.
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


def test_redis_stores_configuration_is_importable() -> None:
    from varco_redis.stores import RedisStoresConfiguration

    assert RedisStoresConfiguration is not None


def test_idempotency_store_binding_is_gated_and_resolvable(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.di import BACKEND_PRIORITY
    from varco_core.idempotency.base import AbstractIdempotencyStore

    binding = bootstrapped_container.get_binding(AbstractIdempotencyStore)
    assert binding.fallback is True
    assert binding.priority == BACKEND_PRIORITY


def test_job_store_binding_is_gated_and_resolvable(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.di import BACKEND_PRIORITY
    from varco_core.job.base import AbstractJobStore

    binding = bootstrapped_container.get_binding(AbstractJobStore)
    assert binding.fallback is True
    assert binding.priority == BACKEND_PRIORITY


def test_revocation_store_binding_is_gated_and_resolvable(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.di import BACKEND_PRIORITY
    from varco_core.revocation import AbstractTokenRevocationStore

    binding = bootstrapped_container.get_binding(AbstractTokenRevocationStore)
    assert binding.fallback is True
    assert binding.priority == BACKEND_PRIORITY


def test_unselected_stores_leave_the_in_process_defaults_live(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from varco_core.di import bootstrap as core_bootstrap
    from varco_core.idempotency.base import AbstractIdempotencyStore
    from varco_core.job.base import AbstractJobStore
    from varco_redis.di import bootstrap as redis_bootstrap

    monkeypatch.delenv("VARCO_IDEMPOTENCY_BACKEND", raising=False)
    monkeypatch.delenv("VARCO_JOB_STORE_BACKEND", raising=False)

    container = DIContainer()
    core_bootstrap(container)
    redis_bootstrap(container)

    idempotency_binding = container.get_binding(AbstractIdempotencyStore, qualifier="in_memory")
    assert idempotency_binding.priority == 0

    job_store_binding = container.get_binding(AbstractJobStore, qualifier="in_memory")
    assert job_store_binding.priority == 0
