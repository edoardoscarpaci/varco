"""
varco_sa.conversation
=====================
SQLAlchemy async implementation of ``AbstractConversationStore``.

Uses raw SQLAlchemy Core (not ORM / SAModelFactory) so it has no circular
dependency on the application's ``DeclarativeBase`` or ``DomainModel``.

Table
-----
``varco_conversation_turns`` — one row per turn, ordered by ``turn_ts``::

    from varco_sa.conversation import conversation_metadata
    target_metadata = [Base.metadata, conversation_metadata]

Usage::

    from sqlalchemy.ext.asyncio import create_async_engine
    from varco_sa.conversation import SAConversationStore

    engine = create_async_engine("postgresql+asyncpg://...")
    store = SAConversationStore(engine)
    await store.ensure_table()

    turn = ConversationTurn(role="user", content="Hello!")
    await store.append("task-123", turn)

DESIGN: raw Core over ORM / SAModelFactory
    ✅ No dependency on application ``DeclarativeBase`` — infrastructure table.
    ✅ ``ensure_table()`` for zero-migration startup convenience.
    ✅ Works with any async SQLAlchemy engine (PostgreSQL, SQLite for tests).
    ❌ No Alembic auto-detection unless ``conversation_metadata`` added to target.

DESIGN: one row per turn over a single JSON column
    ✅ Allows SELECT COUNT(*) for turn_count — O(1) via index.
    ✅ DELETE WHERE task_id = :id removes all turns in one query.
    ✅ ORDER BY turn_ts ASC gives deterministic oldest-first ordering.
    ❌ More rows in the table — add an index on (task_id, turn_ts) for
       production workloads with many active conversations.

Thread safety:  ✅ AsyncEngine connection pool is coroutine-safe.
Async safety:   ✅ All methods are ``async def``.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import timezone
from typing import Any

import sqlalchemy as sa
from sqlalchemy import Column, DateTime, MetaData, String, Table, Text
from sqlalchemy.ext.asyncio import AsyncEngine

from varco_core.service.conversation import AbstractConversationStore, ConversationTurn

_logger = logging.getLogger(__name__)

# ── Table schema ──────────────────────────────────────────────────────────────

# Separate MetaData so varco_conversation_turns never pollutes Base.metadata.
conversation_metadata = MetaData()

_turns_table = Table(
    "varco_conversation_turns",
    conversation_metadata,
    # ── Identity ───────────────────────────────────────────────────────────────
    Column(
        "turn_id",
        sa.Uuid(as_uuid=True),
        primary_key=True,
        nullable=False,
        default=uuid.uuid4,
    ),
    # ── Conversation key ───────────────────────────────────────────────────────
    Column("task_id", String(255), nullable=False, index=True),
    # ── Turn data ──────────────────────────────────────────────────────────────
    Column("role", String(32), nullable=False),
    Column("content", Text, nullable=False),
    # ── Ordering ───────────────────────────────────────────────────────────────
    Column(
        "turn_ts",
        DateTime(timezone=True),
        nullable=False,
    ),
)


# ── Serialization helpers ─────────────────────────────────────────────────────


def _turn_to_row(task_id: str, turn: ConversationTurn) -> dict[str, Any]:
    """Convert a ``ConversationTurn`` to a column-value dict for INSERT."""
    return {
        "turn_id": uuid.uuid4(),
        "task_id": task_id,
        "role": turn.role,
        "content": json.dumps(turn.content),
        "turn_ts": turn.timestamp,
    }


def _row_to_turn(row: Any) -> ConversationTurn:
    """Convert a SA Core row back to a ``ConversationTurn``."""
    ts = row.turn_ts
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ConversationTurn(
        role=row.role,
        content=json.loads(row.content),
        timestamp=ts,
    )


# ── SAConversationStore ───────────────────────────────────────────────────────


class SAConversationStore(AbstractConversationStore):
    """
    SQLAlchemy Core implementation of ``AbstractConversationStore``.

    Persists each ``ConversationTurn`` as a row in ``varco_conversation_turns``.
    Turns are retrieved in ``turn_ts ASC`` order — oldest first.

    Thread safety:  ✅ ``AsyncEngine`` connection pool is coroutine-safe.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        engine: An ``AsyncEngine`` instance (shared across all operations).

    Edge cases:
        - ``get()`` on an unknown ``task_id`` returns ``[]`` — not an error.
        - ``delete()`` on an unknown ``task_id`` is a no-op.
        - ``turn_count()`` uses ``SELECT COUNT(*)`` — O(1) with the index.
        - ``ensure_table()`` is idempotent — safe to call on every startup.
          Include ``conversation_metadata`` in Alembic's ``target_metadata``
          if you prefer migration-based schema management.
        - For production workloads with many concurrent conversations, add a
          composite index on ``(task_id, turn_ts)`` via Alembic.

    Example::

        engine = create_async_engine("postgresql+asyncpg://...")
        store = SAConversationStore(engine)
        await store.ensure_table()

        await store.append("task-1", ConversationTurn(role="user", content="Hi!"))
        turns = await store.get("task-1")
        assert len(turns) == 1
    """

    def __init__(self, engine: AsyncEngine) -> None:
        """
        Args:
            engine: Async SQLAlchemy engine — shared connection pool.
        """
        self._engine = engine

    async def ensure_table(self) -> None:
        """
        Create the ``varco_conversation_turns`` table if it does not exist.

        Idempotent — safe to call multiple times.

        Raises:
            sqlalchemy.exc.SQLAlchemyError: On database errors.

        Edge cases:
            - Uses ``checkfirst=True`` — does not raise if the table already
              exists.  Works on SQLite and PostgreSQL.
        """
        async with self._engine.begin() as conn:
            await conn.run_sync(conversation_metadata.create_all, checkfirst=True)
        _logger.debug("SAConversationStore: varco_conversation_turns table ensured.")

    async def append(self, task_id: str, turn: ConversationTurn) -> None:
        """
        Append a turn to the conversation for ``task_id``.

        Inserts a new row with the turn data and the turn's timestamp.

        Args:
            task_id: A2A task identifier.
            turn:    The ``ConversationTurn`` to append.

        Raises:
            sqlalchemy.exc.SQLAlchemyError: On database errors.
            TypeError: If ``turn.content`` is not JSON-serialisable.

        Async safety: ✅ Single INSERT in a ``begin()`` transaction.
        """
        row = _turn_to_row(task_id, turn)
        async with self._engine.begin() as conn:
            await conn.execute(_turns_table.insert().values(**row))
        _logger.debug(
            "SAConversationStore.append: task_id=%s role=%s", task_id, turn.role
        )

    async def get(self, task_id: str) -> list[ConversationTurn]:
        """
        Return all turns for ``task_id`` in insertion order (oldest first).

        Uses ``ORDER BY turn_ts ASC``.

        Args:
            task_id: A2A task identifier.

        Returns:
            List of ``ConversationTurn`` instances.  Empty if task is unknown.

        Raises:
            sqlalchemy.exc.SQLAlchemyError: On database errors.

        Async safety: ✅ Single SELECT query.
        """
        stmt = (
            sa.select(_turns_table)
            .where(_turns_table.c.task_id == task_id)
            .order_by(_turns_table.c.turn_ts.asc())
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(stmt)
            rows = result.fetchall()

        turns = [_row_to_turn(r) for r in rows]
        _logger.debug(
            "SAConversationStore.get: task_id=%s returned %d turns",
            task_id,
            len(turns),
        )
        return turns

    async def delete(self, task_id: str) -> None:
        """
        Delete all turns for ``task_id``.

        No-op if the conversation does not exist.

        Args:
            task_id: A2A task identifier.

        Raises:
            sqlalchemy.exc.SQLAlchemyError: On database errors.

        Async safety: ✅ Single DELETE in a ``begin()`` transaction.
        """
        async with self._engine.begin() as conn:
            await conn.execute(
                _turns_table.delete().where(_turns_table.c.task_id == task_id)
            )
        _logger.debug("SAConversationStore.delete: task_id=%s", task_id)

    async def turn_count(self, task_id: str) -> int:
        """
        Return the number of turns for ``task_id``.

        Uses ``SELECT COUNT(*)`` — avoids fetching full rows.

        Args:
            task_id: A2A task identifier.

        Returns:
            Number of turns.  ``0`` if the task is unknown.

        Async safety: ✅ Single COUNT query.
        """
        stmt = sa.select(sa.func.count()).select_from(
            _turns_table.select().where(_turns_table.c.task_id == task_id).subquery()
        )
        async with self._engine.connect() as conn:
            result = await conn.execute(stmt)
            count = result.scalar()
        return count if count is not None else 0

    def __repr__(self) -> str:
        return f"SAConversationStore(engine={self._engine!r})"


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "SAConversationStore",
    "conversation_metadata",
]
