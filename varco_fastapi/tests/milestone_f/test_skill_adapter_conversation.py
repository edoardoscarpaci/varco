"""
milestone_f / test_skill_adapter_conversation.py
==================================================
Tests for ``SkillAdapter`` multi-turn conversation support — P2-6 feature.

Covers:
- ``handle_task()`` records user turn before dispatch
- ``handle_task()`` records agent turn after successful sync dispatch
- ``handle_task()`` does NOT record agent turn on failure
- ``_record_turn()`` errors are suppressed — never propagate to caller
- ``agent_card()`` advertises ``multiTurnConversation: True`` when store is set
- ``agent_card()`` has ``multiTurnConversation: False`` without a store
- ``GET /tasks/{task_id}/history`` returns 404 without a store
- ``GET /tasks/{task_id}/history`` returns ordered turns with a store
- Turn content serialises Pydantic models to dicts
- Multiple tasks are isolated in the store

All tests are pure unit tests — no HTTP server, no real broker.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from varco_core.service.conversation import (
    AbstractConversationStore,
    ConversationTurn,
    InMemoryConversationStore,
)
from varco_fastapi.router.base import VarcoRouter
from varco_fastapi.router.mixins import CreateMixin, ReadMixin
from varco_fastapi.router.skill import SkillAdapter


# ── Test models ───────────────────────────────────────────────────────────────


class NoteCreate(BaseModel):
    text: str


class NoteRead(BaseModel):
    id: UUID
    text: str


# ── Fixture router ────────────────────────────────────────────────────────────


class NoteRouter(CreateMixin, ReadMixin, VarcoRouter):
    _prefix = "/notes"
    _create_skill = True
    _read_skill = True


# ── Helper: build SkillAdapter with optional conversation store ───────────────


def _make_adapter(
    *,
    conversation_store: AbstractConversationStore | None = None,
) -> tuple[SkillAdapter, MagicMock]:
    """Return ``(adapter, mock_client)``."""
    mock_client = MagicMock()
    mock_client.create = AsyncMock(return_value={"id": str(uuid4()), "text": "hi"})
    mock_client.read = AsyncMock(return_value={"id": str(uuid4()), "text": "hi"})
    return (
        SkillAdapter(
            NoteRouter,
            agent_name="NoteAgent",
            agent_description="Manages notes",
            client=mock_client,
            conversation_store=conversation_store,
        ),
        mock_client,
    )


def _task(skill_id: str, data: dict, task_id: str | None = None) -> dict:
    return {
        "id": task_id or str(uuid4()),
        "skill_id": skill_id,
        "message": {
            "role": "user",
            "parts": [{"type": "data", "data": data}],
        },
    }


# ── agent_card() multiTurnConversation ─────────────────────────────────────────


def test_agent_card_multi_turn_false_without_store():
    adapter, _ = _make_adapter()
    card = adapter.agent_card(base_url="https://example.com")
    assert card["capabilities"]["multiTurnConversation"] is False


def test_agent_card_multi_turn_true_with_store():
    store = InMemoryConversationStore()
    adapter, _ = _make_adapter(conversation_store=store)
    card = adapter.agent_card(base_url="https://example.com")
    assert card["capabilities"]["multiTurnConversation"] is True


# ── handle_task() turn recording ──────────────────────────────────────────────


async def test_handle_task_records_user_turn_before_dispatch():
    """User turn must be recorded even before the client is called."""
    store = InMemoryConversationStore()
    call_order: list[str] = []

    # Wrap append to track ordering
    original_append = store.append

    async def _tracking_append(task_id: str, turn: ConversationTurn) -> None:
        call_order.append(f"store:{turn.role}")
        await original_append(task_id, turn)

    store.append = _tracking_append  # type: ignore[method-assign]

    adapter, mock_client = _make_adapter(conversation_store=store)
    task_id = str(uuid4())

    # Make dispatch register *after* we've captured call order
    original_create = mock_client.create

    async def _tracking_create(body: dict) -> dict:
        call_order.append("dispatch")
        return await original_create(body)

    mock_client.create = _tracking_create

    await adapter.handle_task(_task("create_note", {"text": "hello"}, task_id))

    # user turn must appear before dispatch
    assert call_order.index("store:user") < call_order.index("dispatch")


async def test_handle_task_records_agent_turn_after_success():
    """Agent turn is recorded after a successful dispatch."""
    store = InMemoryConversationStore()
    adapter, _ = _make_adapter(conversation_store=store)
    task_id = str(uuid4())

    await adapter.handle_task(_task("create_note", {"text": "world"}, task_id))

    turns = await store.get(task_id)
    assert len(turns) == 2
    assert turns[0].role == "user"
    assert turns[1].role == "agent"


async def test_handle_task_no_agent_turn_on_failure():
    """Agent turn must NOT be recorded when the dispatch raises."""
    store = InMemoryConversationStore()
    adapter, mock_client = _make_adapter(conversation_store=store)
    mock_client.create = AsyncMock(side_effect=RuntimeError("service down"))
    task_id = str(uuid4())

    result = await adapter.handle_task(_task("create_note", {"text": "x"}, task_id))

    assert result["status"]["state"] == "failed"
    turns = await store.get(task_id)
    # Only the user turn should have been recorded
    assert len(turns) == 1
    assert turns[0].role == "user"


async def test_handle_task_no_turns_without_store():
    """Without a conversation_store, no turns are stored anywhere."""
    adapter, mock_client = _make_adapter()  # no store
    task_id = str(uuid4())
    result = await adapter.handle_task(_task("create_note", {"text": "y"}, task_id))
    assert result["status"]["state"] == "completed"
    # Adapter has no store attribute exposed, but we can verify the response is clean


async def test_user_turn_content_is_raw_message():
    """User turn content should be the A2A message dict."""
    store = InMemoryConversationStore()
    adapter, _ = _make_adapter(conversation_store=store)
    task_id = str(uuid4())
    message = {"role": "user", "parts": [{"type": "data", "data": {"text": "hello"}}]}

    await adapter.handle_task(
        {"id": task_id, "skill_id": "create_note", "message": message}
    )

    turns = await store.get(task_id)
    assert turns[0].role == "user"
    assert turns[0].content == message


async def test_agent_turn_pydantic_result_serialised():
    """Pydantic model results are serialised to dict before storing."""
    store = InMemoryConversationStore()
    adapter, mock_client = _make_adapter(conversation_store=store)
    note_id = uuid4()
    mock_client.create = AsyncMock(return_value=NoteRead(id=note_id, text="hi"))

    task_id = str(uuid4())
    await adapter.handle_task(_task("create_note", {"text": "hi"}, task_id))

    turns = await store.get(task_id)
    agent_turn = turns[1]
    assert agent_turn.role == "agent"
    # Content should be a dict, not a Pydantic model
    assert isinstance(agent_turn.content, dict)
    assert agent_turn.content["text"] == "hi"


# ── _record_turn() error suppression ──────────────────────────────────────────


async def test_record_turn_error_does_not_propagate():
    """A broken store must never interrupt task execution."""
    store = MagicMock(spec=InMemoryConversationStore)
    store.append = AsyncMock(side_effect=RuntimeError("store is broken"))

    adapter, mock_client = _make_adapter(conversation_store=store)
    # Task should complete successfully despite the store error
    result = await adapter.handle_task(_task("create_note", {"text": "z"}))
    assert result["status"]["state"] == "completed"


async def test_record_turn_agent_error_does_not_propagate():
    """Broken store on agent turn recording must not surface as task failure."""
    call_count = 0
    store = MagicMock(spec=InMemoryConversationStore)

    async def _side_effect(task_id: str, turn: ConversationTurn) -> None:
        nonlocal call_count
        call_count += 1
        # user turn OK, agent turn fails
        if turn.role == "agent":
            raise RuntimeError("write failed")

    store.append = _side_effect

    adapter, _ = _make_adapter(conversation_store=store)
    result = await adapter.handle_task(_task("create_note", {"text": "a"}))
    # Task must still be "completed" even though agent turn write failed
    assert result["status"]["state"] == "completed"
    assert call_count == 2  # both turns attempted


# ── Multiple tasks isolated ───────────────────────────────────────────────────


async def test_turns_isolated_per_task_id():
    """Turns for different task_ids must not bleed into each other."""
    store = InMemoryConversationStore()
    adapter, mock_client = _make_adapter(conversation_store=store)

    task_a = str(uuid4())
    task_b = str(uuid4())

    await adapter.handle_task(_task("create_note", {"text": "A"}, task_a))
    await adapter.handle_task(_task("create_note", {"text": "B"}, task_b))

    turns_a = await store.get(task_a)
    turns_b = await store.get(task_b)
    assert len(turns_a) == 2
    assert len(turns_b) == 2
    # User message content should differ
    assert turns_a[0].content != turns_b[0].content


# ── GET /tasks/{task_id}/history endpoint ─────────────────────────────────────


def _mount_client(adapter: SkillAdapter) -> TestClient:
    app = FastAPI()
    adapter.mount(app)
    return TestClient(app, raise_server_exceptions=False)


def test_history_endpoint_404_without_store():
    """Without a conversation_store, the history endpoint must return 404."""
    adapter, _ = _make_adapter()  # no store
    client = _mount_client(adapter)
    resp = client.get(f"/tasks/{uuid4()}/history")
    assert resp.status_code == 404
    body = resp.json()
    assert "error" in body


async def test_history_endpoint_returns_turns():
    """With a store, /history returns the ordered turn list."""
    store = InMemoryConversationStore()
    adapter, _ = _make_adapter(conversation_store=store)
    http_client = _mount_client(adapter)

    task_id = str(uuid4())
    # Populate turns directly
    await store.append(
        task_id, ConversationTurn(role="user", content={"text": "hello"})
    )
    await store.append(
        task_id, ConversationTurn(role="agent", content={"result": "ok"})
    )

    resp = http_client.get(f"/tasks/{task_id}/history")
    assert resp.status_code == 200
    body = resp.json()
    assert body["task_id"] == task_id
    assert len(body["turns"]) == 2
    assert body["turns"][0]["role"] == "user"
    assert body["turns"][1]["role"] == "agent"


async def test_history_endpoint_unknown_task_returns_empty_list():
    """Unknown task_id with a store should return an empty turn list."""
    store = InMemoryConversationStore()
    adapter, _ = _make_adapter(conversation_store=store)
    http_client = _mount_client(adapter)

    resp = http_client.get(f"/tasks/{uuid4()}/history")
    assert resp.status_code == 200
    body = resp.json()
    assert body["turns"] == []


async def test_history_endpoint_turn_has_timestamp():
    """Each turn in the history response must include an ISO 8601 timestamp."""
    store = InMemoryConversationStore()
    adapter, _ = _make_adapter(conversation_store=store)
    http_client = _mount_client(adapter)

    task_id = str(uuid4())
    await store.append(task_id, ConversationTurn(role="user", content="hello"))

    resp = http_client.get(f"/tasks/{task_id}/history")
    assert resp.status_code == 200
    turn = resp.json()["turns"][0]
    assert "timestamp" in turn
    # Should be a parseable ISO 8601 string
    from datetime import datetime

    datetime.fromisoformat(turn["timestamp"])  # raises ValueError if malformed
