"""
Unit tests for varco_redis.conversation
==========================================
All tests use an in-memory ``FakeRedis`` — no real Redis instance required.

``FakeRedis`` implements the subset of ``redis.asyncio`` commands used by
``RedisConversationStore``: ``rpush``, ``lrange``, ``llen``, ``delete``,
``expire``.

Sections
--------
- ``RedisConversationStore`` append/get   — round-trip, ordering, JSON content
- ``RedisConversationStore`` delete       — removes all turns; idempotent
- ``RedisConversationStore`` turn_count   — O(1) LLEN; 0 for unknown tasks
- TTL                                     — expire called on append when set
- Full lifecycle                          — append → get → delete cycle
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any


from varco_core.service.conversation import ConversationTurn
from varco_redis.conversation import RedisConversationStore


# ── FakeRedis ─────────────────────────────────────────────────────────────────


class FakeRedis:
    """
    In-memory Redis double for unit tests.

    Implements: rpush, lrange, llen, delete, expire.
    Does NOT implement real TTL expiry.
    """

    def __init__(self) -> None:
        self._lists: dict[str, list[str]] = {}
        self._expire_calls: list[tuple[str, int]] = []

    async def rpush(self, key: str, *values: str) -> int:
        lst = self._lists.setdefault(key, [])
        for v in values:
            lst.append(
                v
                if isinstance(v, str)
                else v.decode() if isinstance(v, bytes) else str(v)
            )
        return len(lst)

    async def lrange(self, key: str, start: int, stop: int) -> list[bytes]:
        lst = self._lists.get(key, [])
        end = stop + 1 if stop != -1 else None
        return [item.encode() for item in lst[start:end]]

    async def llen(self, key: str) -> int:
        return len(self._lists.get(key, []))

    async def delete(self, *keys: str) -> int:
        count = 0
        for k in keys:
            if k in self._lists:
                del self._lists[k]
                count += 1
        return count

    async def expire(self, key: str, seconds: int) -> int:
        self._expire_calls.append((key, seconds))
        return 1

    async def aclose(self) -> None:
        pass


# ── Fixtures ───────────────────────────────────────────────────────────────────


def _make_store(*, ttl: int | None = None) -> tuple[FakeRedis, RedisConversationStore]:
    fake = FakeRedis()
    store = RedisConversationStore(fake, key_prefix="varco:conv:", ttl_seconds=ttl)  # type: ignore[arg-type]
    return fake, store


def _turn(role: str = "user", content: Any = "Hello") -> ConversationTurn:
    return ConversationTurn(role=role, content=content)


# ── append / get ──────────────────────────────────────────────────────────────


class TestRedisConversationStoreAppendGet:
    async def test_append_and_get_single_turn(self) -> None:
        """append() then get() returns the turn with correct fields."""
        _, store = _make_store()
        turn = _turn(role="user", content="Hello!")
        await store.append("task-1", turn)

        turns = await store.get("task-1")
        assert len(turns) == 1
        assert turns[0].role == "user"
        assert turns[0].content == "Hello!"

    async def test_get_unknown_task_returns_empty(self) -> None:
        _, store = _make_store()
        assert await store.get("no-such-task") == []

    async def test_append_multiple_turns_preserves_order(self) -> None:
        """Turns are returned in insertion order (oldest first)."""
        _, store = _make_store()
        t0 = datetime.now(tz=timezone.utc)
        turns_in = [
            ConversationTurn(role="user", content="Hello", timestamp=t0),
            ConversationTurn(
                role="agent", content="Hi", timestamp=t0 + timedelta(seconds=1)
            ),
            ConversationTurn(
                role="user", content="How are you?", timestamp=t0 + timedelta(seconds=2)
            ),
        ]
        for t in turns_in:
            await store.append("task-1", t)

        turns_out = await store.get("task-1")
        assert len(turns_out) == 3
        assert turns_out[0].role == "user"
        assert turns_out[0].content == "Hello"
        assert turns_out[1].role == "agent"
        assert turns_out[2].content == "How are you?"

    async def test_different_tasks_are_isolated(self) -> None:
        """Turns from different task_ids do not interfere."""
        _, store = _make_store()
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

    async def test_json_dict_content_round_trips(self) -> None:
        """dict content (A2A message format) survives JSON serialization."""
        _, store = _make_store()
        content = {"parts": [{"text": "Hello"}, {"type": "data", "value": 42}]}
        await store.append("task-1", _turn(content=content))

        turns = await store.get("task-1")
        assert turns[0].content == content

    async def test_timestamp_preserved(self) -> None:
        """The turn's timestamp is preserved through serialization."""
        _, store = _make_store()
        ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        turn = ConversationTurn(role="agent", content="response", timestamp=ts)
        await store.append("task-1", turn)

        turns = await store.get("task-1")
        assert turns[0].timestamp == ts

    async def test_repr(self) -> None:
        _, store = _make_store()
        assert "RedisConversationStore" in repr(store)


# ── delete ────────────────────────────────────────────────────────────────────


class TestRedisConversationStoreDelete:
    async def test_delete_removes_all_turns(self) -> None:
        _, store = _make_store()
        await store.append("task-1", _turn())
        await store.append("task-1", _turn(role="agent"))
        await store.delete("task-1")

        assert await store.get("task-1") == []

    async def test_delete_unknown_task_is_noop(self) -> None:
        _, store = _make_store()
        await store.delete("no-such-task")  # must not raise

    async def test_delete_does_not_affect_other_tasks(self) -> None:
        _, store = _make_store()
        await store.append("task-A", _turn(content="A"))
        await store.append("task-B", _turn(content="B"))

        await store.delete("task-A")

        assert await store.get("task-A") == []
        assert len(await store.get("task-B")) == 1


# ── turn_count ────────────────────────────────────────────────────────────────


class TestRedisConversationStoreTurnCount:
    async def test_turn_count_zero_for_unknown_task(self) -> None:
        _, store = _make_store()
        assert await store.turn_count("unknown") == 0

    async def test_turn_count_matches_appended_turns(self) -> None:
        _, store = _make_store()
        for i in range(5):
            await store.append("task-1", _turn(content=str(i)))

        assert await store.turn_count("task-1") == 5

    async def test_turn_count_after_delete(self) -> None:
        _, store = _make_store()
        await store.append("task-1", _turn())
        await store.delete("task-1")
        assert await store.turn_count("task-1") == 0


# ── TTL ───────────────────────────────────────────────────────────────────────


class TestRedisConversationStoreTTL:
    async def test_no_ttl_does_not_call_expire(self) -> None:
        """When ttl_seconds=None, EXPIRE is never called."""
        fake, store = _make_store(ttl=None)
        await store.append("task-1", _turn())
        assert fake._expire_calls == []

    async def test_ttl_set_calls_expire_on_append(self) -> None:
        """When ttl_seconds is set, EXPIRE is called on each RPUSH."""
        fake, store = _make_store(ttl=3600)
        await store.append("task-1", _turn())
        assert len(fake._expire_calls) == 1
        key, seconds = fake._expire_calls[0]
        assert "task-1" in key
        assert seconds == 3600

    async def test_ttl_refreshed_on_every_append(self) -> None:
        """EXPIRE is called for every append, not just the first."""
        fake, store = _make_store(ttl=300)
        for _ in range(3):
            await store.append("task-1", _turn())
        assert len(fake._expire_calls) == 3


# ── Full lifecycle ─────────────────────────────────────────────────────────────


class TestRedisConversationStoreLifecycle:
    async def test_full_append_get_delete(self) -> None:
        """End-to-end: append → get → turn_count → delete."""
        _, store = _make_store()

        await store.append(
            "task-1", ConversationTurn(role="user", content="Order pizza")
        )
        await store.append(
            "task-1", ConversationTurn(role="agent", content="Which size?")
        )
        await store.append("task-1", ConversationTurn(role="user", content="Large"))

        turns = await store.get("task-1")
        assert len(turns) == 3
        assert turns[0].content == "Order pizza"
        assert turns[2].content == "Large"

        count = await store.turn_count("task-1")
        assert count == 3

        await store.delete("task-1")
        assert await store.get("task-1") == []
        assert await store.turn_count("task-1") == 0
