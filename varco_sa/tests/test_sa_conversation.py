"""
Unit tests for varco_sa.conversation
=======================================
Covers ``SAConversationStore`` using an in-memory SQLite database (aiosqlite).

No external PostgreSQL instance is required — all tests run against SQLite
via ``create_async_engine("sqlite+aiosqlite:///:memory:")``.

Sections
--------
- ``SAConversationStore`` schema      — ensure_table idempotency, metadata export
- ``SAConversationStore.append/get``  — round-trip, ordering, JSON content
- ``SAConversationStore.delete``      — removes all turns; idempotent
- ``SAConversationStore.turn_count``  — correct count; 0 for unknown tasks
- Full lifecycle                      — append → get → delete cycle
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine

from varco_core.service.conversation import ConversationTurn
from varco_sa.conversation import SAConversationStore, conversation_metadata


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def engine():
    """In-memory SQLite engine with the varco_conversation_turns table created."""
    eng = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with eng.begin() as conn:
        await conn.run_sync(conversation_metadata.create_all)
    yield eng
    async with eng.begin() as conn:
        await conn.run_sync(conversation_metadata.drop_all)
    await eng.dispose()


@pytest_asyncio.fixture
async def store(engine) -> SAConversationStore:
    """``SAConversationStore`` backed by the in-memory SQLite engine."""
    return SAConversationStore(engine)


def _turn(role: str = "user", content: Any = "Hello") -> ConversationTurn:
    return ConversationTurn(role=role, content=content)


# ── Schema ────────────────────────────────────────────────────────────────────


class TestSAConversationStoreSchema:
    def test_conversation_metadata_exported(self) -> None:
        assert conversation_metadata is not None
        assert "varco_conversation_turns" in conversation_metadata.tables

    async def test_ensure_table_idempotent(self, engine) -> None:
        """Calling ensure_table() twice must not raise."""
        store = SAConversationStore(engine)
        await store.ensure_table()
        await store.ensure_table()  # must not raise

    def test_repr(self, store: SAConversationStore) -> None:
        assert "SAConversationStore" in repr(store)


# ── append / get ──────────────────────────────────────────────────────────────


class TestSAConversationStoreAppendGet:
    async def test_append_and_get_single_turn(self, store: SAConversationStore) -> None:
        """append() then get() returns the turn with correct fields."""
        turn = _turn(role="user", content="Hello!")
        await store.append("task-1", turn)

        turns = await store.get("task-1")
        assert len(turns) == 1
        assert turns[0].role == "user"
        assert turns[0].content == "Hello!"

    async def test_get_unknown_task_returns_empty(
        self, store: SAConversationStore
    ) -> None:
        assert await store.get("no-such-task") == []

    async def test_append_multiple_turns_preserves_order(
        self, store: SAConversationStore
    ) -> None:
        """Turns are returned in oldest-first order via turn_ts ASC."""
        turns_in = [
            ConversationTurn(
                role="user",
                content="Hello",
                timestamp=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            ),
            ConversationTurn(
                role="agent",
                content="Hi there",
                timestamp=datetime(2024, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
            ),
            ConversationTurn(
                role="user",
                content="How are you?",
                timestamp=datetime(2024, 1, 1, 12, 0, 2, tzinfo=timezone.utc),
            ),
        ]
        for t in turns_in:
            await store.append("task-1", t)

        turns_out = await store.get("task-1")
        assert len(turns_out) == 3
        assert turns_out[0].content == "Hello"
        assert turns_out[1].content == "Hi there"
        assert turns_out[2].content == "How are you?"

    async def test_different_tasks_are_isolated(
        self, store: SAConversationStore
    ) -> None:
        """Turns from different task_ids do not interfere."""
        await store.append("task-A", _turn(content="A1"))
        await store.append("task-B", _turn(content="B1"))
        await store.append("task-A", _turn(content="A2"))

        a_turns = await store.get("task-A")
        b_turns = await store.get("task-B")

        assert len(a_turns) == 2
        assert a_turns[0].content == "A1"
        assert a_turns[1].content == "A2"
        assert len(b_turns) == 1
        assert b_turns[0].content == "B1"

    async def test_json_dict_content_round_trips(
        self, store: SAConversationStore
    ) -> None:
        """dict content (A2A message format) survives JSON serialization."""
        content = {"parts": [{"text": "Hello"}, {"type": "data", "value": 42}]}
        await store.append("task-1", _turn(content=content))

        turns = await store.get("task-1")
        assert turns[0].content == content

    async def test_list_content_round_trips(self, store: SAConversationStore) -> None:
        """list content also round-trips correctly."""
        content = ["item-1", "item-2", 3]
        await store.append("task-1", _turn(content=content))

        turns = await store.get("task-1")
        assert turns[0].content == content

    async def test_string_content_round_trips(self, store: SAConversationStore) -> None:
        await store.append("task-1", _turn(content="plain string"))
        turns = await store.get("task-1")
        assert turns[0].content == "plain string"

    async def test_role_preserved(self, store: SAConversationStore) -> None:
        await store.append("task-1", _turn(role="agent", content="ok"))
        turns = await store.get("task-1")
        assert turns[0].role == "agent"


# ── delete ────────────────────────────────────────────────────────────────────


class TestSAConversationStoreDelete:
    async def test_delete_removes_all_turns(self, store: SAConversationStore) -> None:
        await store.append("task-1", _turn(role="user"))
        await store.append("task-1", _turn(role="agent"))
        await store.delete("task-1")

        assert await store.get("task-1") == []

    async def test_delete_unknown_task_is_noop(
        self, store: SAConversationStore
    ) -> None:
        await store.delete("no-such-task")  # must not raise

    async def test_delete_does_not_affect_other_tasks(
        self, store: SAConversationStore
    ) -> None:
        await store.append("task-A", _turn(content="A"))
        await store.append("task-B", _turn(content="B"))

        await store.delete("task-A")

        assert await store.get("task-A") == []
        assert len(await store.get("task-B")) == 1


# ── turn_count ────────────────────────────────────────────────────────────────


class TestSAConversationStoreTurnCount:
    async def test_turn_count_zero_for_unknown_task(
        self, store: SAConversationStore
    ) -> None:
        assert await store.turn_count("unknown") == 0

    async def test_turn_count_matches_appended_turns(
        self, store: SAConversationStore
    ) -> None:
        for i in range(5):
            await store.append("task-1", _turn(content=str(i)))

        assert await store.turn_count("task-1") == 5

    async def test_turn_count_after_delete(self, store: SAConversationStore) -> None:
        await store.append("task-1", _turn())
        await store.delete("task-1")
        assert await store.turn_count("task-1") == 0


# ── Full lifecycle ─────────────────────────────────────────────────────────────


class TestSAConversationStoreLifecycle:
    async def test_full_append_get_delete(self, store: SAConversationStore) -> None:
        """End-to-end: append → get → turn_count → delete."""
        await store.append(
            "task-1",
            ConversationTurn(role="user", content="Order pizza"),
        )
        await store.append(
            "task-1",
            ConversationTurn(role="agent", content="Which size?"),
        )
        await store.append(
            "task-1",
            ConversationTurn(role="user", content="Large"),
        )

        turns = await store.get("task-1")
        assert len(turns) == 3
        assert turns[0].content == "Order pizza"
        assert turns[1].content == "Which size?"
        assert turns[2].content == "Large"

        assert await store.turn_count("task-1") == 3

        await store.delete("task-1")
        assert await store.get("task-1") == []
        assert await store.turn_count("task-1") == 0
