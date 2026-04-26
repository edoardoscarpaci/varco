"""
varco_redis.conversation
========================
Redis-backed implementation of ``AbstractConversationStore``.

Each A2A conversation is stored as a Redis List::

    {prefix}conv:{task_id}  →  List [ JSON-turn, JSON-turn, ... ]

Turns are appended with ``RPUSH`` (right-push), retrieved in insertion order
with ``LRANGE 0 -1``, counted in O(1) with ``LLEN``, and deleted with ``DEL``.

DESIGN: Redis List over Redis JSON or Hash
    ✅ Native ordered list — RPUSH preserves insertion order.
    ✅ O(1) append and count (RPUSH / LLEN); O(n) read where n = turn count.
    ✅ Single DEL removes the entire conversation.
    ❌ No random-access to a single turn without fetching the whole list.
       For typical A2A conversations (< 50 turns), full reads are cheap.

DESIGN: JSON-serialised turns over raw bytes
    ✅ Human-readable in redis-cli.
    ✅ Self-describing — no schema versioning for turn fields.
    ❌ JSON parse overhead on read — negligible for conversational payloads.

DESIGN: optional TTL on RPUSH
    ✅ Conversations that are never explicitly deleted expire automatically —
       prevents unbounded memory growth.
    ✅ TTL is refreshed on every append — active conversations stay alive.
    ❌ A very long conversation with many hours between turns may expire
       mid-conversation if ttl_seconds is too small.  Choose conservatively.

Thread safety:  ✅ redis.asyncio.Redis is coroutine-safe.
Async safety:   ✅ All methods are ``async def``.

📚 Docs
- 📐 https://redis.io/docs/data-types/lists/
  Redis Lists — RPUSH / LRANGE / LLEN / DEL
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING

from varco_core.service.conversation import AbstractConversationStore, ConversationTurn

if TYPE_CHECKING:
    import redis.asyncio as aioredis

_logger = logging.getLogger(__name__)

# ── Serialization helpers ─────────────────────────────────────────────────────


def _turn_to_json(turn: ConversationTurn) -> str:
    """Serialize a ``ConversationTurn`` to a JSON string for Redis storage."""
    return json.dumps(
        {
            "role": turn.role,
            "content": turn.content,
            "timestamp": turn.timestamp.isoformat(),
        }
    )


def _json_to_turn(raw: str | bytes) -> ConversationTurn:
    """Deserialize a JSON string from Redis storage back to a ``ConversationTurn``."""
    data: dict[str, Any] = json.loads(raw)
    ts_str: str = data.get("timestamp", "")
    try:
        ts = datetime.fromisoformat(ts_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        ts = datetime.now(tz=timezone.utc)
    return ConversationTurn(
        role=data["role"],
        content=data["content"],
        timestamp=ts,
    )


# ── RedisConversationStore ────────────────────────────────────────────────────


class RedisConversationStore(AbstractConversationStore):
    """
    Redis-backed implementation of ``AbstractConversationStore``.

    Stores each conversation as a Redis List of JSON-serialised
    ``ConversationTurn`` objects.  Supports an optional TTL so inactive
    conversations expire automatically.

    Thread safety:  ✅ ``redis.asyncio.Redis`` is coroutine-safe.
    Async safety:   ✅ All methods are ``async def``.

    Args:
        client:      An ``aioredis.Redis`` client instance.
        key_prefix:  Namespace prefix for all Redis keys.
                     Default: ``"varco:conv:"``.
        ttl_seconds: Optional TTL (seconds) for each conversation list.
                     The TTL is refreshed on every ``append()``.  Set to
                     ``None`` to disable expiry (conversations persist until
                     ``delete()`` is called).  Default: ``None``.

    Edge cases:
        - ``get()`` on an unknown ``task_id`` returns ``[]`` — Redis LRANGE
          on a non-existent key returns an empty list.
        - ``delete()`` on an unknown ``task_id`` is a no-op (DEL is
          idempotent).
        - If a conversation key expires (TTL) while turns are still being
          appended, subsequent appends create a fresh list — the expired turns
          are silently lost.  Choose ``ttl_seconds`` generously.
        - ``content`` inside each ``ConversationTurn`` must be
          JSON-serialisable.  Bytes, sets, or custom objects will raise
          ``TypeError`` on ``append()``.

    Example::

        import redis.asyncio as aioredis
        from varco_redis.conversation import RedisConversationStore
        from varco_core.service.conversation import ConversationTurn

        client = aioredis.from_url("redis://localhost:6379/0")
        store = RedisConversationStore(client, ttl_seconds=3600)

        turn = ConversationTurn(role="user", content={"text": "Hello!"})
        await store.append("task-123", turn)

        history = await store.get("task-123")
        print(len(history))  # 1
    """

    def __init__(
        self,
        client: aioredis.Redis,
        *,
        key_prefix: str = "varco:conv:",
        ttl_seconds: int | None = None,
    ) -> None:
        """
        Args:
            client:      Async Redis client — shared across all operations.
            key_prefix:  Prefix for all conversation list keys.
            ttl_seconds: Optional TTL; refreshed on every ``append()``.
        """
        self._client = client
        self._prefix = key_prefix
        self._ttl = ttl_seconds

    # ── Key helper ────────────────────────────────────────────────────────────

    def _conv_key(self, task_id: str) -> str:
        """Redis key for a conversation's turn list."""
        return f"{self._prefix}{task_id}"

    # ── AbstractConversationStore implementation ──────────────────────────────

    async def append(self, task_id: str, turn: ConversationTurn) -> None:
        """
        Append a turn to the conversation for ``task_id``.

        Uses ``RPUSH`` to append the JSON-serialised turn and optionally
        refreshes the TTL with ``EXPIRE``.

        Args:
            task_id: A2A task identifier.
            turn:    The ``ConversationTurn`` to append.

        Raises:
            TypeError: If ``turn.content`` is not JSON-serialisable.

        Async safety: ✅ RPUSH + optional EXPIRE (two commands, not atomic).
        """
        key = self._conv_key(task_id)
        await self._client.rpush(key, _turn_to_json(turn))
        if self._ttl is not None:
            await self._client.expire(key, self._ttl)
        _logger.debug(
            "RedisConversationStore.append: task_id=%s role=%s", task_id, turn.role
        )

    async def get(self, task_id: str) -> list[ConversationTurn]:
        """
        Return all turns for ``task_id`` in insertion order.

        Uses ``LRANGE 0 -1`` to retrieve the full list.

        Args:
            task_id: A2A task identifier.

        Returns:
            List of ``ConversationTurn`` instances, oldest first.
            Empty list if no conversation exists.

        Async safety: ✅ Single LRANGE command.
        """
        key = self._conv_key(task_id)
        raw_turns: list[bytes] = await self._client.lrange(key, 0, -1)
        turns: list[ConversationTurn] = []
        for raw in raw_turns:
            try:
                turns.append(_json_to_turn(raw))
            except Exception as exc:
                _logger.warning(
                    "RedisConversationStore.get: failed to deserialise turn for "
                    "task_id=%s: %s",
                    task_id,
                    exc,
                )
        _logger.debug(
            "RedisConversationStore.get: task_id=%s returned %d turns",
            task_id,
            len(turns),
        )
        return turns

    async def delete(self, task_id: str) -> None:
        """
        Delete the conversation for ``task_id``.

        No-op if the key does not exist.

        Args:
            task_id: A2A task identifier.

        Async safety: ✅ Single DEL command.
        """
        await self._client.delete(self._conv_key(task_id))
        _logger.debug("RedisConversationStore.delete: task_id=%s", task_id)

    async def turn_count(self, task_id: str) -> int:
        """
        Return the number of turns in the conversation.

        O(1) via ``LLEN`` — does not fetch the full list.

        Args:
            task_id: A2A task identifier.

        Returns:
            Number of turns.  ``0`` if the conversation is unknown.

        Async safety: ✅ Single LLEN command.
        """
        result = await self._client.llen(self._conv_key(task_id))
        return result if result is not None else 0

    def __repr__(self) -> str:
        return (
            f"RedisConversationStore("
            f"prefix={self._prefix!r}, "
            f"ttl={self._ttl!r})"
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "RedisConversationStore",
]
