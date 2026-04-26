"""
Unit tests for varco_sa.saga
==============================
Covers ``SASagaRepository`` using an in-memory SQLite database (aiosqlite).

No external PostgreSQL instance is required — all tests run against SQLite
via ``create_async_engine("sqlite+aiosqlite:///:memory:")``.

Sections
--------
- ``SASagaRepository`` schema     — ensure_table idempotency, sagas_metadata export
- ``SASagaRepository.save``       — insert, upsert (status update), context preservation
- ``SASagaRepository.load``       — found, not found, field round-trip
- Full saga lifecycle             — PENDING → RUNNING → COMPLETED / COMPENSATED
"""

from __future__ import annotations

from uuid import uuid4

import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from varco_core.service.saga import SagaState, SagaStatus
from varco_sa.saga import SASagaRepository, sagas_metadata


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def engine():
    """In-memory SQLite engine with the varco_sagas table created."""
    eng = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(sagas_metadata.create_all)
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(sagas_metadata.drop_all)
    await eng.dispose()


@pytest_asyncio.fixture
async def repo(engine) -> SASagaRepository:
    """``SASagaRepository`` backed by the in-memory SQLite engine."""
    return SASagaRepository(engine)


def _pending_state(**kwargs) -> SagaState:
    """Build a ``SagaState`` in PENDING status with a fresh UUID."""
    return SagaState(saga_id=uuid4(), **kwargs)


# ── Schema ────────────────────────────────────────────────────────────────────


class TestSASagaRepositorySchema:
    def test_sagas_metadata_exported(self) -> None:
        assert sagas_metadata is not None
        assert "varco_sagas" in sagas_metadata.tables

    async def test_ensure_table_idempotent(self, engine) -> None:
        """Calling ensure_table() twice must not raise."""
        repo = SASagaRepository(engine)
        await repo.ensure_table()
        await repo.ensure_table()  # must not raise

    def test_repr(self, repo: SASagaRepository) -> None:
        assert "SASagaRepository" in repr(repo)


# ── save ──────────────────────────────────────────────────────────────────────


class TestSASagaRepositorySave:
    async def test_save_and_load(self, repo: SASagaRepository) -> None:
        """save() persists the state; load() retrieves it with all fields intact."""
        state = _pending_state()
        await repo.save(state)

        loaded = await repo.load(state.saga_id)
        assert loaded is not None
        assert loaded.saga_id == state.saga_id
        assert loaded.status == SagaStatus.PENDING

    async def test_save_upserts_on_status_change(self, repo: SASagaRepository) -> None:
        """Saving again with an updated status replaces the old row."""
        state = _pending_state()
        await repo.save(state)

        running = SagaState(
            saga_id=state.saga_id,
            status=SagaStatus.RUNNING,
            completed_steps=0,
        )
        await repo.save(running)

        loaded = await repo.load(state.saga_id)
        assert loaded is not None
        assert loaded.status == SagaStatus.RUNNING

    async def test_save_preserves_context(self, repo: SASagaRepository) -> None:
        """``context`` dict round-trips correctly."""
        ctx = {"order_id": "ord-1", "amount": 99.99, "tags": ["a", "b"]}
        state = _pending_state(context=ctx)
        await repo.save(state)

        loaded = await repo.load(state.saga_id)
        assert loaded is not None
        assert loaded.context == ctx

    async def test_save_preserves_completed_steps(self, repo: SASagaRepository) -> None:
        """``completed_steps`` is persisted and restored correctly."""
        state = SagaState(
            saga_id=uuid4(),
            status=SagaStatus.RUNNING,
            completed_steps=3,
        )
        await repo.save(state)

        loaded = await repo.load(state.saga_id)
        assert loaded is not None
        assert loaded.completed_steps == 3

    async def test_save_preserves_error(self, repo: SASagaRepository) -> None:
        """``error`` string round-trips correctly for FAILED sagas."""
        state = SagaState(
            saga_id=uuid4(),
            status=SagaStatus.FAILED,
            completed_steps=2,
            error="Step [2] 'charge_card' failed: timeout",
        )
        await repo.save(state)

        loaded = await repo.load(state.saga_id)
        assert loaded is not None
        assert loaded.error == "Step [2] 'charge_card' failed: timeout"

    async def test_save_none_error_round_trip(self, repo: SASagaRepository) -> None:
        """``error=None`` round-trips as ``None``."""
        state = _pending_state()
        await repo.save(state)

        loaded = await repo.load(state.saga_id)
        assert loaded is not None
        assert loaded.error is None

    async def test_save_empty_context_round_trip(self, repo: SASagaRepository) -> None:
        """``context={}`` round-trips as an empty dict."""
        state = _pending_state()
        await repo.save(state)

        loaded = await repo.load(state.saga_id)
        assert loaded is not None
        assert loaded.context == {}


# ── load ──────────────────────────────────────────────────────────────────────


class TestSASagaRepositoryLoad:
    async def test_load_unknown_id_returns_none(self, repo: SASagaRepository) -> None:
        """load() must return None for an unknown saga_id."""
        result = await repo.load(uuid4())
        assert result is None

    async def test_load_returns_correct_saga(self, repo: SASagaRepository) -> None:
        """load() returns the exact saga that was saved, not a neighbour."""
        s1 = _pending_state()
        s2 = _pending_state()
        await repo.save(s1)
        await repo.save(s2)

        loaded = await repo.load(s1.saga_id)
        assert loaded is not None
        assert loaded.saga_id == s1.saga_id

    async def test_load_all_statuses(self, repo: SASagaRepository) -> None:
        """All SagaStatus values survive the save → load round-trip."""
        for status in SagaStatus:
            state = SagaState(saga_id=uuid4(), status=status)
            await repo.save(state)
            loaded = await repo.load(state.saga_id)
            assert loaded is not None
            assert loaded.status == status


# ── Full lifecycle ─────────────────────────────────────────────────────────────


class TestSASagaRepositoryLifecycle:
    async def test_pending_to_running_to_completed(
        self, repo: SASagaRepository
    ) -> None:
        """PENDING → RUNNING → COMPLETED lifecycle persists correctly."""
        saga_id = uuid4()

        pending = SagaState(saga_id=saga_id, status=SagaStatus.PENDING)
        await repo.save(pending)
        assert (await repo.load(saga_id)).status == SagaStatus.PENDING  # type: ignore[union-attr]

        running = SagaState(
            saga_id=saga_id, status=SagaStatus.RUNNING, completed_steps=0
        )
        await repo.save(running)
        assert (await repo.load(saga_id)).status == SagaStatus.RUNNING  # type: ignore[union-attr]

        completed = SagaState(
            saga_id=saga_id, status=SagaStatus.COMPLETED, completed_steps=3
        )
        await repo.save(completed)

        loaded = await repo.load(saga_id)
        assert loaded is not None
        assert loaded.status == SagaStatus.COMPLETED
        assert loaded.completed_steps == 3

    async def test_compensation_lifecycle(self, repo: SASagaRepository) -> None:
        """RUNNING → COMPENSATING → COMPENSATED lifecycle with error preserved."""
        saga_id = uuid4()
        ctx = {"payment_id": "pay-123"}

        running = SagaState(
            saga_id=saga_id,
            status=SagaStatus.RUNNING,
            completed_steps=2,
            context=ctx,
        )
        await repo.save(running)

        compensating = SagaState(
            saga_id=saga_id,
            status=SagaStatus.COMPENSATING,
            completed_steps=2,
            context=ctx,
            error="Step [2] 'reserve_inventory' failed: out of stock",
        )
        await repo.save(compensating)

        compensated = SagaState(
            saga_id=saga_id,
            status=SagaStatus.COMPENSATED,
            completed_steps=2,
            context=ctx,
            error="Step [2] 'reserve_inventory' failed: out of stock",
        )
        await repo.save(compensated)

        loaded = await repo.load(saga_id)
        assert loaded is not None
        assert loaded.status == SagaStatus.COMPENSATED
        assert loaded.error is not None
        assert "reserve_inventory" in loaded.error
        assert loaded.context == ctx

    async def test_failed_saga_lifecycle(self, repo: SASagaRepository) -> None:
        """A FAILED saga (compensation error) is stored with FAILED status."""
        saga_id = uuid4()

        failed = SagaState(
            saga_id=saga_id,
            status=SagaStatus.FAILED,
            completed_steps=1,
            error="Compensation for step [0] failed: refund API down",
        )
        await repo.save(failed)

        loaded = await repo.load(saga_id)
        assert loaded is not None
        assert loaded.status == SagaStatus.FAILED
        assert "Compensation" in loaded.error  # type: ignore[operator]
