"""
Plan 047 / Step 8 — `SAIdempotencyStore` gains an additive `engine=` kwarg
(N2: `SAModule.idempotency_store(config)` constructs `SAIdempotencyStore(
engine=config.engine)` to reuse the app's own pool). `url` stays
first-keyword-optional; exactly one of `url=`/`engine=` must be given.

RED MODE: `SAIdempotencyStore.__init__` does not accept `engine=` yet —
every test here is expected to fail with `TypeError` (unexpected keyword
argument) or the neither/both-`ValueError` guard simply not existing yet
(no exception raised where one is expected).
"""

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import create_async_engine
from varco_sa.idempotency import SAIdempotencyStore


def test_construction_with_url_still_works() -> None:
    store = SAIdempotencyStore(url="sqlite+aiosqlite:///:memory:")
    assert store is not None


def test_construction_with_engine_is_accepted() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    store = SAIdempotencyStore(engine=engine)
    assert store is not None


def test_construction_with_neither_url_nor_engine_raises_value_error() -> None:
    with pytest.raises(ValueError, match="engine"):
        SAIdempotencyStore()


def test_construction_with_both_url_and_engine_raises_value_error() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    with pytest.raises(ValueError, match="engine"):
        SAIdempotencyStore(url="sqlite+aiosqlite:///:memory:", engine=engine)


async def test_start_with_given_engine_does_not_create_a_new_one() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    store = SAIdempotencyStore(engine=engine)

    await store.start()

    assert store._engine is engine  # noqa: SLF001 - white-box: the given engine is honoured, not replaced


async def test_stop_does_not_dispose_an_engine_it_did_not_create() -> None:
    # DESIGN (N2): "stop() disposes only an engine it created" — an
    # app-supplied engine must survive this store's stop().
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    store = SAIdempotencyStore(engine=engine)
    await store.start()

    await store.stop()

    # The app-owned engine must still be usable after the store's stop().
    async with engine.connect() as conn:
        assert conn is not None
    await engine.dispose()
