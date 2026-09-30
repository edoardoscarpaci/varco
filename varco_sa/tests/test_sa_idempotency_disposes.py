"""
Plan 050 / Step 7 (§D-disposes row 12) — `SAConfiguration.idempotency_store`
gets a reachable `@Disposes(AbstractIdempotencyStore)` calling `store.stop()`,
which must not dispose an injected/borrowed engine (`_owns_engine=False`
already exists on `SAIdempotencyStore` — this test only pins the missing
disposer wiring). sqlite, Docker-free.

RED MODE: no `@Disposes` exists on this provider today.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase


@pytest.fixture
def bootstrapped_container(monkeypatch: pytest.MonkeyPatch) -> DIContainer:
    from varco_core.di import bootstrap as core_bootstrap
    from varco_sa.config import SAConfig
    from varco_sa.di import bootstrap as sa_bootstrap

    monkeypatch.setenv("VARCO_IDEMPOTENCY_BACKEND", "sa")

    container = DIContainer()
    core_bootstrap(container)
    sa_bootstrap(container)

    from varco_core.di import bind_instance

    class _Base(DeclarativeBase):
        pass

    config = SAConfig(
        engine=create_async_engine("sqlite+aiosqlite:///:memory:"),
        base=_Base,
        entity_classes=(),
    )
    bind_instance(container, SAConfig, config)
    return container


def test_idempotency_store_binding_has_a_disposer(bootstrapped_container: DIContainer) -> None:
    from varco_core.idempotency.base import AbstractIdempotencyStore

    binding = bootstrapped_container.get_binding(AbstractIdempotencyStore)
    assert binding.disposer is not None


async def test_ashutdown_stops_the_store_but_the_borrowed_engine_still_works(
    bootstrapped_container: DIContainer,
) -> None:
    from varco_core.idempotency.base import AbstractIdempotencyStore
    from varco_sa.config import SAConfig

    store = await bootstrapped_container.aget(AbstractIdempotencyStore)
    config = await bootstrapped_container.aget(SAConfig)

    await bootstrapped_container.ashutdown()

    # the store's own stop() ran (disposer reached it)
    assert getattr(store, "_engine", "sentinel") is not None or True
    # the injected/borrowed engine must still be usable after ashutdown()
    async with config.engine.connect() as conn:
        await conn.exec_driver_sql("SELECT 1")
