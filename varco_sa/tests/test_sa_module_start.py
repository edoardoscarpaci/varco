"""
Plan 050 / Step 13 (§D-stores) — `SAModule.start()` owns SA framework-table
readiness: for each of `AbstractIdempotencyStore`/`AbstractDeadLetterQueue`/
`AbstractJobStore`/`WebhookSubscriptionRepository` that is resolvable and
resolves to a `varco_sa` class, `start()`/`ensure_table()` is called.
sqlite, Docker-free.

RED MODE: `SAModule` has no `start()` method (nor `ensure_tables` kwarg)
today — it inherits `VarcoModule`'s no-op default.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase


@pytest.fixture
def sa_container(monkeypatch: pytest.MonkeyPatch) -> tuple[DIContainer, object]:
    from varco_core.di.module import CoreModule
    from varco_sa.config import SAConfig
    from varco_sa.modules import SAModule

    monkeypatch.setenv("VARCO_IDEMPOTENCY_BACKEND", "sa")

    class _Base(DeclarativeBase):
        pass

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    config = SAConfig(engine=engine, base=_Base, entity_classes=())

    container = DIContainer()
    CoreModule().register(container)
    module = SAModule(config, ensure_tables=True)
    module.register(container)
    return container, module


def test_sa_module_accepts_ensure_tables_kwarg() -> None:
    from varco_sa.config import SAConfig
    from varco_sa.modules import SAModule

    class _Base(DeclarativeBase):
        pass

    config = SAConfig(
        engine=create_async_engine("sqlite+aiosqlite:///:memory:"),
        base=_Base,
        entity_classes=(),
    )
    # Expected to fail today: no ensure_tables kwarg on SAModule.__init__.
    SAModule(config, ensure_tables=True)


async def test_sa_module_start_ensures_the_idempotency_table(
    sa_container: tuple[DIContainer, object],
) -> None:
    container, module = sa_container
    from varco_core.idempotency.base import AbstractIdempotencyStore

    await module.start(container)  # type: ignore[attr-defined]

    store = await container.aget(AbstractIdempotencyStore)
    # If ensure_table() ran, a reserve() call must not raise "no such table".
    # NOTE (Plan 050 repair): AbstractIdempotencyStore.reserve()'s keyword is
    # `ttl`, not `ttl_seconds` — fixed here per the implementation-agent
    # contract ("follow the plan, fix the test").
    await store.reserve("k1", "fp1", ttl=60)
