"""
varco_sa.saga
=============
SQLAlchemy async implementation of ``AbstractSagaRepository``.

Uses raw SQLAlchemy Core (not ORM / SAModelFactory) so it has no circular
dependency on the application's ``DeclarativeBase`` or ``DomainModel``.

Table
-----
``varco_sagas`` — created idempotently via ``ensure_table()`` or Alembic::

    from varco_sa.saga import sagas_metadata
    target_metadata = [Base.metadata, sagas_metadata]

Usage::

    from sqlalchemy.ext.asyncio import create_async_engine
    from varco_sa.saga import SASagaRepository

    engine = create_async_engine("postgresql+asyncpg://...")
    repo = SASagaRepository(engine)
    await repo.ensure_table()

    state = SagaState(saga_id=uuid4())
    await repo.save(state)
    loaded = await repo.load(state.saga_id)

DESIGN: raw Core over ORM / SAModelFactory
    ✅ No dependency on application ``DeclarativeBase`` — infrastructure table.
    ✅ ``ensure_table()`` for zero-migration startup convenience.
    ✅ Works with any async SQLAlchemy engine (PostgreSQL, SQLite for tests).
    ❌ No Alembic auto-detection unless ``sagas_metadata`` is added to target.

DESIGN: upsert via DELETE + INSERT (dialect-agnostic)
    ✅ Works on SQLite (unit tests) and PostgreSQL without dialect-specific ON CONFLICT.
    ✅ Simple to reason about — the latest save always wins.
    ❌ Not atomic on PostgreSQL under extreme concurrency — acceptable for sagas
       (the orchestrator owns a saga exclusively during execution).

Thread safety:  ✅ AsyncEngine connection pool is coroutine-safe.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import Column, DateTime, Integer, MetaData, String, Table, Text
from sqlalchemy.ext.asyncio import AsyncEngine

from varco_core.service.saga import AbstractSagaRepository, SagaState, SagaStatus

_logger = logging.getLogger(__name__)

# ── Table schema ──────────────────────────────────────────────────────────────

# Separate MetaData so varco_sagas never pollutes the application's Base.metadata.
sagas_metadata = MetaData()

_sagas_table = Table(
    "varco_sagas",
    sagas_metadata,
    # ── Identity ───────────────────────────────────────────────────────────────
    Column(
        "saga_id",
        sa.Uuid(as_uuid=True),
        primary_key=True,
        nullable=False,
    ),
    # ── Status ─────────────────────────────────────────────────────────────────
    Column("status", String(32), nullable=False),
    # ── Progress ───────────────────────────────────────────────────────────────
    Column("completed_steps", Integer, nullable=False, default=0),
    # ── Shared step context (JSON dict) ────────────────────────────────────────
    Column("context", Text, nullable=False, default="{}"),
    # ── Failure info ───────────────────────────────────────────────────────────
    Column("error", Text, nullable=True),
    # ── Audit ──────────────────────────────────────────────────────────────────
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    ),
)


# ── Serialization helpers ─────────────────────────────────────────────────────


def _state_to_row(state: SagaState) -> dict[str, Any]:
    """Convert a ``SagaState`` to a column-value dict for INSERT."""
    return {
        "saga_id": state.saga_id,
        "status": state.status.value,
        "completed_steps": state.completed_steps,
        "context": json.dumps(state.context),
        "error": state.error,
        "updated_at": datetime.now(timezone.utc),
    }


def _row_to_state(row: Any) -> SagaState:
    """Convert a SA Core row back to a ``SagaState``."""
    context: dict[str, Any] = json.loads(row.context) if row.context else {}
    return SagaState(
        saga_id=UUID(str(row.saga_id)),
        status=SagaStatus(row.status),
        completed_steps=row.completed_steps,
        context=context,
        error=row.error,
    )


# ── SASagaRepository ──────────────────────────────────────────────────────────


class SASagaRepository(AbstractSagaRepository):
    """
    SQLAlchemy Core implementation of ``AbstractSagaRepository``.

    Stores each ``SagaState`` as a single row in the ``varco_sagas`` table.
    Uses DELETE + INSERT for upsert semantics (dialect-agnostic).

    Thread safety:  ✅ ``AsyncEngine`` connection pool is coroutine-safe.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        engine: An ``AsyncEngine`` instance (shared across all operations).

    Edge cases:
        - Calling ``save()`` twice with the same ``saga_id`` replaces the row —
          last-write-wins, as required by ``AbstractSagaRepository``.
        - ``load()`` returns ``None`` for unknown saga IDs — not an error.
        - ``ensure_table()`` is idempotent — safe to call on every startup.
          If you use Alembic, include ``sagas_metadata`` in ``target_metadata``
          instead of calling ``ensure_table()``.

    Example::

        engine = create_async_engine("postgresql+asyncpg://...")
        repo = SASagaRepository(engine)
        await repo.ensure_table()

        state = SagaState(saga_id=uuid4())
        await repo.save(state)
        loaded = await repo.load(state.saga_id)
        assert loaded.status == SagaStatus.PENDING
    """

    def __init__(self, engine: AsyncEngine) -> None:
        """
        Args:
            engine: Async SQLAlchemy engine — shared connection pool.
        """
        self._engine = engine

    async def ensure_table(self) -> None:
        """
        Create the ``varco_sagas`` table if it does not exist.

        Idempotent — safe to call multiple times.

        Raises:
            sqlalchemy.exc.SQLAlchemyError: On database errors.

        Edge cases:
            - Uses ``checkfirst=True`` — does not raise if the table already
              exists.  Works on SQLite and PostgreSQL.
        """
        async with self._engine.begin() as conn:
            await conn.run_sync(sagas_metadata.create_all, checkfirst=True)
        _logger.debug("SASagaRepository: varco_sagas table ensured.")

    async def save(self, state: SagaState) -> None:
        """
        Persist a ``SagaState`` snapshot (upsert semantics).

        Replaces any existing row for the same ``saga_id``.

        DESIGN: DELETE + INSERT over ON CONFLICT DO UPDATE
            ✅ Dialect-agnostic — works on both SQLite (unit tests) and
               PostgreSQL (production) without conditional compilation.
            ❌ Not atomic — a crash between DELETE and INSERT leaves the row
               absent.  Acceptable: the orchestrator owns a saga exclusively.

        Args:
            state: The ``SagaState`` to persist.

        Raises:
            sqlalchemy.exc.SQLAlchemyError: On database errors.

        Async safety: ✅ Wrapped in a single ``begin()`` transaction.
        """
        row = _state_to_row(state)
        async with self._engine.begin() as conn:
            # Delete the existing row (no-op if absent).
            await conn.execute(
                _sagas_table.delete().where(_sagas_table.c.saga_id == state.saga_id)
            )
            await conn.execute(_sagas_table.insert().values(**row))

        _logger.debug(
            "SASagaRepository.save: saga_id=%s status=%s",
            state.saga_id,
            state.status,
        )

    async def load(self, saga_id: UUID) -> SagaState | None:
        """
        Load the current state for a saga.

        Args:
            saga_id: The saga ID to look up.

        Returns:
            The persisted ``SagaState``, or ``None`` if not found.

        Raises:
            sqlalchemy.exc.SQLAlchemyError: On database errors.

        Async safety: ✅ Single ``SELECT`` — no transaction needed.
        """
        async with self._engine.connect() as conn:
            row = await conn.execute(
                sa.select(_sagas_table).where(_sagas_table.c.saga_id == saga_id)
            )
            result = row.first()

        if result is None:
            return None

        return _row_to_state(result)

    def __repr__(self) -> str:
        return f"SASagaRepository(engine={self._engine!r})"


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SASagaRepository",
    "sagas_metadata",
]
