"""
varco_core.service.conversation
================================
Multi-turn conversation store for the A2A (Agent-to-Agent) protocol.

``ConversationStore`` persists the message history for a long-running A2A
task so that a stateless ``SkillAdapter`` can reconstruct context on each
turn.  Each logical conversation is identified by a ``task_id`` string (the
same ID used by the A2A protocol for task tracking).

Wire-up pattern::

    store = InMemoryConversationStore()

    adapter = SkillAdapter(
        MyRouter,
        agent_name="MyAgent",
        conversation_store=store,
    )

Turn model
----------
Each user or agent message is recorded as a ``ConversationTurn``:

- ``role``    — ``"user"`` or ``"agent"``
- ``content`` — raw message content (A2A message dict or any string)
- ``timestamp`` — UTC datetime of the turn

``ConversationStore``
---------------------
ABC with three methods:

- ``append(task_id, turn)`` — record a new turn for this task
- ``get(task_id)``          — retrieve all turns in order
- ``delete(task_id)``       — drop the conversation (cleanup after completion)

``InMemoryConversationStore``
-----------------------------
In-process store for tests and single-process deployments.  State is NOT
shared across processes and is lost on restart.  For durable storage, use a
``SAConversationStore`` (``varco_sa``) or ``RedisConversationStore`` (``varco_redis``).

Thread safety:  ❌  Not thread-safe (asyncio-only, no threading primitives).
Async safety:   ✅  All methods are ``async def``.

📚 Docs
- 📐 https://google.github.io/A2A/specification/
  Google A2A specification — task_id and multi-turn message model.
"""

from __future__ import annotations

import sys
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from providify import Singleton


# ── ConversationTurn ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ConversationTurn:
    """
    Immutable record of a single turn in an A2A conversation.

    DESIGN: frozen dataclass over dict
        ✅ Hashable — safe to store in sets, use as dict keys.
        ✅ Immutable — prevents accidental in-place mutation of history.
        ❌ Must use ``dataclasses.replace()`` to create a modified copy.

    Attributes:
        role:      ``"user"`` (incoming) or ``"agent"`` (outgoing).
        content:   The message content.  For A2A, this is typically the raw
                   message dict (``{"parts": [...]}``).  Any JSON-serialisable
                   value is accepted.
        timestamp: UTC datetime of the turn.  Auto-generated if not provided.

    Thread safety:  ✅ Immutable.
    """

    role: str
    """Turn author: ``"user"`` or ``"agent"``."""

    content: Any
    """Message content.  Typically an A2A message dict or plain string."""

    timestamp: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    """UTC datetime when this turn was recorded."""


# ── AbstractConversationStore ─────────────────────────────────────────────────


class AbstractConversationStore(ABC):
    """
    Abstract interface for persisting A2A multi-turn conversation history.

    Implementations must support three operations:

    - ``append``  — add a turn to an existing or new conversation.
    - ``get``     — retrieve all turns for a task in insertion order.
    - ``delete``  — remove a conversation (cleanup after task completion).

    Thread safety:  ❌  Not thread-safe (design for single-event-loop access).
    Async safety:   ✅  All methods are ``async def``.

    Edge cases:
        - ``get()`` on an unknown ``task_id`` returns ``[]`` (not an error).
        - ``delete()`` on an unknown ``task_id`` is a no-op.
        - Implementations should handle concurrent ``append()`` calls for the
          same ``task_id`` gracefully (ordering is best-effort unless the
          implementation uses database-level serialization).
    """

    @abstractmethod
    async def append(self, task_id: str, turn: ConversationTurn) -> None:
        """
        Append ``turn`` to the conversation for ``task_id``.

        Creates the conversation if it does not exist yet.

        Args:
            task_id: A2A task identifier (string — may be a UUID string or
                     any opaque ID the A2A client provides).
            turn:    The new turn to append.
        """

    @abstractmethod
    async def get(self, task_id: str) -> list[ConversationTurn]:
        """
        Return all turns for ``task_id`` in insertion order.

        Args:
            task_id: A2A task identifier.

        Returns:
            List of ``ConversationTurn`` instances, oldest first.
            Empty list if no conversation exists for this task.
        """

    @abstractmethod
    async def delete(self, task_id: str) -> None:
        """
        Delete the conversation for ``task_id``.

        No-op if the conversation does not exist.

        Args:
            task_id: A2A task identifier.
        """

    async def turn_count(self, task_id: str) -> int:
        """
        Return the number of turns in the conversation for ``task_id``.

        Default implementation calls ``get()`` — backends may override with
        a cheaper count query.

        Args:
            task_id: A2A task identifier.

        Returns:
            Number of recorded turns.  ``0`` if the conversation is unknown.
        """
        return len(await self.get(task_id))


# ── InMemoryConversationStore ─────────────────────────────────────────────────


@Singleton(priority=-sys.maxsize, qualifier="in_memory")
class InMemoryConversationStore(AbstractConversationStore):
    """
    In-process conversation store backed by a plain ``dict[str, list]``.

    Suitable for:
        - Unit and integration tests (no external dependency).
        - Single-process deployments where state loss on restart is acceptable.

    NOT suitable for:
        - Multi-process or multi-replica deployments (state is not shared).
        - Production deployments that require durable history.

    DESIGN: dict[task_id, list[ConversationTurn]] over any external backend
        ✅ Zero dependencies — works in any test environment.
        ✅ O(1) append; O(n) get — efficient for typical conversation lengths.
        ❌ State is lost on process restart.
        ❌ Not safe for concurrent access from multiple OS threads.

    Thread safety:  ❌  Not thread-safe across OS threads.
    Async safety:   ✅  Coroutine-safe (all mutations on the event loop).

    Edge cases:
        - ``get()`` returns a copy of the turn list so callers cannot mutate
          the store's internal state.
        - ``delete()`` on an unknown task_id is silently ignored.
    """

    def __init__(self) -> None:
        # Dict mapping task_id → ordered list of turns.
        self._store: dict[str, list[ConversationTurn]] = {}

    async def append(self, task_id: str, turn: ConversationTurn) -> None:
        """
        Append ``turn`` to the conversation.  Creates the list if absent.

        Args:
            task_id: A2A task identifier.
            turn:    Turn to append.
        """
        if task_id not in self._store:
            self._store[task_id] = []
        self._store[task_id].append(turn)

    async def get(self, task_id: str) -> list[ConversationTurn]:
        """
        Return a shallow copy of all turns for ``task_id``.

        Args:
            task_id: A2A task identifier.

        Returns:
            List of turns in insertion order.  Empty if task is unknown.
        """
        return list(self._store.get(task_id, []))

    async def delete(self, task_id: str) -> None:
        """
        Remove the conversation for ``task_id``.  No-op if absent.

        Args:
            task_id: A2A task identifier.
        """
        self._store.pop(task_id, None)

    async def turn_count(self, task_id: str) -> int:
        """
        O(1) count via len() — no full list copy needed.

        Args:
            task_id: A2A task identifier.

        Returns:
            Number of turns.  ``0`` if task is unknown.
        """
        return len(self._store.get(task_id, []))

    @property
    def conversation_count(self) -> int:
        """Number of active conversations in the store."""
        return len(self._store)

    def __repr__(self) -> str:
        return f"InMemoryConversationStore(" f"conversations={self.conversation_count})"


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "AbstractConversationStore",
    "ConversationTurn",
    "InMemoryConversationStore",
]
