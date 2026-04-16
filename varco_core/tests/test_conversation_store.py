"""
tests.test_conversation_store
==============================
Unit tests for ``ConversationTurn``, ``AbstractConversationStore``, and
``InMemoryConversationStore`` — the A2A multi-turn conversation infrastructure.

Covers:
- ``ConversationTurn`` immutability and default timestamp
- ``InMemoryConversationStore.append()`` creates new conversation on first turn
- ``InMemoryConversationStore.get()`` returns ordered copy, empty on unknown task
- ``InMemoryConversationStore.delete()`` removes conversation, no-op for unknown
- ``InMemoryConversationStore.turn_count()`` O(1) shortcut
- ``InMemoryConversationStore.conversation_count`` property
- Isolation: ``get()`` returns a copy (not a reference to internal list)
- ``__repr__`` includes conversation count
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from varco_core.service.conversation import (
    AbstractConversationStore,
    ConversationTurn,
    InMemoryConversationStore,
)


# ── ConversationTurn ──────────────────────────────────────────────────────────


def test_turn_is_frozen():
    """ConversationTurn must be immutable (frozen dataclass)."""
    turn = ConversationTurn(role="user", content="hello")
    with pytest.raises((AttributeError, TypeError)):
        turn.role = "agent"  # type: ignore[misc]


def test_turn_auto_timestamp():
    """Timestamp defaults to a UTC datetime when not provided."""
    before = datetime.now(tz=timezone.utc)
    turn = ConversationTurn(role="user", content="hi")
    after = datetime.now(tz=timezone.utc)
    assert before <= turn.timestamp <= after
    assert turn.timestamp.tzinfo is not None


def test_turn_explicit_timestamp_preserved():
    """Explicit timestamp is stored unchanged."""
    ts = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    turn = ConversationTurn(role="user", content="ping", timestamp=ts)
    assert turn.timestamp == ts


def test_turn_content_any_type():
    """content may be a dict, list, or any JSON-serialisable value."""
    dict_turn = ConversationTurn(role="user", content={"parts": [{"text": "hello"}]})
    list_turn = ConversationTurn(role="agent", content=["a", "b"])
    assert isinstance(dict_turn.content, dict)
    assert isinstance(list_turn.content, list)


# ── InMemoryConversationStore — append / get ───────────────────────────────────


async def test_append_creates_conversation():
    store = InMemoryConversationStore()
    turn = ConversationTurn(role="user", content="hello")
    await store.append("task-1", turn)
    turns = await store.get("task-1")
    assert len(turns) == 1
    assert turns[0] == turn


async def test_append_multiple_turns_preserves_order():
    store = InMemoryConversationStore()
    t1 = ConversationTurn(role="user", content="first")
    t2 = ConversationTurn(role="agent", content="second")
    t3 = ConversationTurn(role="user", content="third")
    await store.append("task-1", t1)
    await store.append("task-1", t2)
    await store.append("task-1", t3)
    turns = await store.get("task-1")
    assert turns == [t1, t2, t3]


async def test_get_unknown_task_returns_empty_list():
    store = InMemoryConversationStore()
    turns = await store.get("nonexistent-task")
    assert turns == []


async def test_get_returns_copy_not_reference():
    """Mutating the returned list must not affect the store."""
    store = InMemoryConversationStore()
    turn = ConversationTurn(role="user", content="x")
    await store.append("task-1", turn)
    turns = await store.get("task-1")
    turns.clear()
    # Store must still hold the original turn
    assert await store.turn_count("task-1") == 1


async def test_conversations_are_isolated_by_task_id():
    """Turns for different task_ids must not leak into each other."""
    store = InMemoryConversationStore()
    t1 = ConversationTurn(role="user", content="task A")
    t2 = ConversationTurn(role="user", content="task B")
    await store.append("task-A", t1)
    await store.append("task-B", t2)
    assert len(await store.get("task-A")) == 1
    assert len(await store.get("task-B")) == 1
    assert (await store.get("task-A"))[0].content == "task A"


# ── InMemoryConversationStore — delete ────────────────────────────────────────


async def test_delete_removes_conversation():
    store = InMemoryConversationStore()
    await store.append("task-1", ConversationTurn(role="user", content="hi"))
    await store.delete("task-1")
    assert await store.get("task-1") == []


async def test_delete_unknown_task_is_noop():
    """Deleting a task that doesn't exist must not raise."""
    store = InMemoryConversationStore()
    await store.delete("ghost-task")  # should not raise


async def test_delete_does_not_affect_other_tasks():
    store = InMemoryConversationStore()
    await store.append("task-A", ConversationTurn(role="user", content="a"))
    await store.append("task-B", ConversationTurn(role="user", content="b"))
    await store.delete("task-A")
    assert await store.get("task-A") == []
    assert len(await store.get("task-B")) == 1


# ── InMemoryConversationStore — turn_count ─────────────────────────────────────


async def test_turn_count_zero_for_unknown():
    store = InMemoryConversationStore()
    assert await store.turn_count("no-such-task") == 0


async def test_turn_count_matches_appended_turns():
    store = InMemoryConversationStore()
    for i in range(5):
        await store.append("task-1", ConversationTurn(role="user", content=str(i)))
    assert await store.turn_count("task-1") == 5


async def test_turn_count_drops_after_delete():
    store = InMemoryConversationStore()
    await store.append("task-1", ConversationTurn(role="user", content="x"))
    await store.delete("task-1")
    assert await store.turn_count("task-1") == 0


# ── InMemoryConversationStore — conversation_count ────────────────────────────


async def test_conversation_count_zero_initially():
    store = InMemoryConversationStore()
    assert store.conversation_count == 0


async def test_conversation_count_increments():
    store = InMemoryConversationStore()
    await store.append("task-1", ConversationTurn(role="user", content="hi"))
    assert store.conversation_count == 1
    await store.append("task-2", ConversationTurn(role="user", content="hey"))
    assert store.conversation_count == 2


async def test_conversation_count_decrements_on_delete():
    store = InMemoryConversationStore()
    await store.append("task-1", ConversationTurn(role="user", content="x"))
    await store.append("task-2", ConversationTurn(role="user", content="y"))
    await store.delete("task-1")
    assert store.conversation_count == 1


# ── AbstractConversationStore — base turn_count via get() ─────────────────────


async def test_base_class_turn_count_uses_get():
    """
    The default ``turn_count()`` on the ABC calls ``get()`` under the hood.
    Verify the delegation works correctly for a custom implementation.
    """

    class MinimalStore(AbstractConversationStore):
        def __init__(self) -> None:
            self._data: dict[str, list[ConversationTurn]] = {}

        async def append(self, task_id: str, turn: ConversationTurn) -> None:
            self._data.setdefault(task_id, []).append(turn)

        async def get(self, task_id: str) -> list[ConversationTurn]:
            return list(self._data.get(task_id, []))

        async def delete(self, task_id: str) -> None:
            self._data.pop(task_id, None)

    store = MinimalStore()
    await store.append("t1", ConversationTurn(role="user", content="hi"))
    await store.append("t1", ConversationTurn(role="agent", content="hello"))
    assert await store.turn_count("t1") == 2
    assert await store.turn_count("unknown") == 0


# ── __repr__ ──────────────────────────────────────────────────────────────────


def test_repr_zero_conversations():
    store = InMemoryConversationStore()
    assert "conversations=0" in repr(store)


async def test_repr_reflects_current_count():
    store = InMemoryConversationStore()
    await store.append("t1", ConversationTurn(role="user", content="hi"))
    await store.append("t2", ConversationTurn(role="user", content="hey"))
    assert "conversations=2" in repr(store)
