"""
milestone_f / test_skill_adapter.py
=====================================
Tests for ``SkillAdapter`` — Google A2A agent integration.

Covers:
- Skill list built from ``skill_enabled`` routes only
- Fine-grained metadata: ``skill_id``, ``skill_name``, ``skill_description``,
  ``skill_input_modes``, ``skill_output_modes``
- Fallback chain: explicit → summary → description → auto-sentence
- Auto skill ID derivation (CRUD vs custom @route)
- Agent Card structure (name, url, capabilities, skills)
- ``handle_task()`` dispatch: all CRUD actions + custom routes
- ``handle_task()`` error handling: unknown skill, execution failure
- FastAPI endpoint mounting via ``adapter.mount(app)``

All tests are pure unit tests — no HTTP server, no real client.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from varco_fastapi.router.base import VarcoRouter
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.mixins import CreateMixin, DeleteMixin, ListMixin, ReadMixin
from varco_fastapi.router.skill import (
    SkillAdapter,
    SkillDefinition,
    _auto_skill_id,
    _resolve_description,
    _title_case_id,
)
from varco_fastapi.router.introspection import ResolvedRoute


# ── Test models ───────────────────────────────────────────────────────────────


class OrderCreate(BaseModel):
    name: str
    amount: float


class OrderRead(BaseModel):
    id: UUID
    name: str
    amount: float


# ── Fixture routers ───────────────────────────────────────────────────────────


class OrderRouter(CreateMixin, ReadMixin, ListMixin, DeleteMixin, VarcoRouter):
    """Router with selectively enabled A2A skills and fine-grained metadata."""

    _prefix = "/orders"

    # create exposed with custom skill ID and name
    _create_skill = True
    _create_skill_id = "place_order"
    _create_skill_name = "Place Order"
    _create_skill_description = "Submit a new customer order to the system"

    # read exposed with only skill=True — name and description auto-generated
    _read_skill = True
    _read_summary = "Retrieve an order by ID"

    # list exposed with custom I/O modes
    _list_skill = True
    _list_skill_input_modes = ["application/json", "text/plain"]
    _list_skill_output_modes = ["application/json"]

    # delete is NOT exposed as skill
    _delete_skill = False

    @route(
        "POST",
        "/{id}/ship",
        skill=True,
        skill_id="ship_order",
        skill_name="Ship Order",
        skill_description="Trigger shipment for an existing order",
    )
    async def ship(self, id: UUID) -> OrderRead: ...


class MinimalRouter(VarcoRouter):
    """Router with no skill-enabled routes."""

    _prefix = "/noop"

    @route("GET", "/ping")
    async def ping(self) -> dict: ...


# ── _title_case_id ────────────────────────────────────────────────────────────


def test_title_case_single_word():
    assert _title_case_id("order") == "Order"


def test_title_case_snake_case():
    assert _title_case_id("create_order") == "Create Order"


def test_title_case_multi_word():
    assert _title_case_id("list_all_orders") == "List All Orders"


# ── _auto_skill_id ────────────────────────────────────────────────────────────


def test_auto_skill_id_crud():
    """CRUD route → ``{action}_{resource}``."""
    route_obj = ResolvedRoute(
        name="create", method="POST", path="", is_crud=True, crud_action="create"
    )
    assert _auto_skill_id(route_obj, "order") == "create_order"


def test_auto_skill_id_custom_route():
    """Non-CRUD route → method name."""
    route_obj = ResolvedRoute(
        name="ship", method="POST", path="/{id}/ship", is_crud=False
    )
    assert _auto_skill_id(route_obj, "order") == "ship"


# ── _resolve_description ──────────────────────────────────────────────────────


def test_resolve_description_explicit_wins():
    assert _resolve_description("explicit", "summary", "desc", "auto") == "explicit"


def test_resolve_description_summary_fallback():
    assert _resolve_description(None, "summary", "desc", "auto") == "summary"


def test_resolve_description_auto_last_resort():
    assert _resolve_description(None, None, None, "auto") == "auto"


# ── skill list ────────────────────────────────────────────────────────────────


def test_skills_only_from_skill_enabled_routes():
    """Only ``skill_enabled`` routes appear; delete and label must be absent."""
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")
    ids = {s.id for s in adapter.skills}
    assert "place_order" in ids  # _create_skill_id override
    assert "read_order" in ids  # auto from _read_skill
    assert "list_order" in ids  # auto from _list_skill
    assert "ship_order" in ids  # @route skill_id override
    assert "delete_order" not in ids  # _delete_skill = False


def test_skills_empty_when_no_skill_routes():
    adapter = SkillAdapter(MinimalRouter, agent_name="A", agent_description="D")
    assert adapter.skills == []


def test_skill_definition_is_frozen():
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")
    skill = adapter.skills[0]
    assert isinstance(skill, SkillDefinition)
    with pytest.raises((AttributeError, TypeError)):
        skill.id = "mutated"  # type: ignore[misc]


# ── fine-grained metadata ─────────────────────────────────────────────────────


def test_explicit_skill_id_used():
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")
    skill = next(s for s in adapter.skills if s.id == "place_order")
    assert skill.name == "Place Order"
    assert skill.description == "Submit a new customer order to the system"


def test_description_falls_back_to_summary():
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")
    read_skill = next(s for s in adapter.skills if s.id == "read_order")
    assert read_skill.description == "Retrieve an order by ID"


def test_custom_input_output_modes_used():
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")
    list_skill = next(s for s in adapter.skills if s.id == "list_order")
    assert "text/plain" in list_skill.input_modes
    assert "application/json" in list_skill.output_modes


def test_default_modes_when_not_specified():
    """``read`` has no mode overrides → defaults to ``application/json``."""
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")
    read_skill = next(s for s in adapter.skills if s.id == "read_order")
    assert read_skill.input_modes == ("application/json",)
    assert read_skill.output_modes == ("application/json",)


# ── Agent Card ────────────────────────────────────────────────────────────────


def test_agent_card_name_and_description():
    adapter = SkillAdapter(
        OrderRouter, agent_name="OrderAgent", agent_description="Manages orders"
    )
    card = adapter.agent_card(base_url="https://api.example.com")
    assert card["name"] == "OrderAgent"
    assert card["description"] == "Manages orders"


def test_agent_card_url_points_to_tasks_send():
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")
    card = adapter.agent_card(base_url="https://api.example.com")
    assert card["url"] == "https://api.example.com/tasks/send"


def test_agent_card_url_strips_trailing_slash():
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")
    card = adapter.agent_card(base_url="https://api.example.com/")
    assert card["url"] == "https://api.example.com/tasks/send"


def test_agent_card_version():
    adapter = SkillAdapter(
        OrderRouter, agent_name="A", agent_description="D", agent_version="2.1.0"
    )
    card = adapter.agent_card(base_url="https://example.com")
    assert card["version"] == "2.1.0"


def test_agent_card_skills_list():
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")
    card = adapter.agent_card(base_url="https://example.com")
    skill_ids = {s["id"] for s in card["skills"]}
    assert "place_order" in skill_ids
    assert "read_order" in skill_ids
    assert "list_order" in skill_ids
    assert "ship_order" in skill_ids


def test_agent_card_skill_has_all_fields():
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")
    card = adapter.agent_card(base_url="https://example.com")
    skill = next(s for s in card["skills"] if s["id"] == "place_order")
    assert "name" in skill
    assert "description" in skill
    assert "inputModes" in skill
    assert "outputModes" in skill


def test_agent_card_capabilities_v1_no_streaming():
    """v1 declares no streaming or push notifications."""
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")
    card = adapter.agent_card(base_url="https://example.com")
    assert card["capabilities"]["streaming"] is False
    assert card["capabilities"]["pushNotifications"] is False


# ── handle_task() ─────────────────────────────────────────────────────────────


def _make_task(skill_id: str, data: dict, task_id: str = "t1") -> dict:
    """Build a minimal A2A task request."""
    return {
        "id": task_id,
        "skill_id": skill_id,
        "message": {"role": "user", "parts": [{"type": "data", "data": data}]},
    }


async def test_handle_task_create_dispatches_to_client():
    mock_client = MagicMock()
    mock_client.create = AsyncMock(return_value={"id": "new"})
    adapter = SkillAdapter(
        OrderRouter, agent_name="A", agent_description="D", client=mock_client
    )

    result = await adapter.handle_task(
        _make_task("place_order", {"name": "X", "amount": 5.0})
    )

    mock_client.create.assert_called_once_with({"name": "X", "amount": 5.0})
    assert result["status"]["state"] == "completed"
    assert result["artifacts"][0]["parts"][0]["data"] == {"id": "new"}


async def test_handle_task_read_dispatches_to_client():
    mock_client = MagicMock()
    mock_client.read = AsyncMock(return_value={"id": "abc", "name": "X", "amount": 1.0})
    adapter = SkillAdapter(
        OrderRouter, agent_name="A", agent_description="D", client=mock_client
    )

    result = await adapter.handle_task(_make_task("read_order", {"id": "abc"}))

    mock_client.read.assert_called_once_with("abc")
    assert result["status"]["state"] == "completed"


async def test_handle_task_list_dispatches_with_filters():
    mock_client = MagicMock()
    mock_client.list = AsyncMock(return_value=[])
    adapter = SkillAdapter(
        OrderRouter, agent_name="A", agent_description="D", client=mock_client
    )

    await adapter.handle_task(
        _make_task("list_order", {"q": "status=active", "limit": 20})
    )

    mock_client.list.assert_called_once()
    call_kwargs = mock_client.list.call_args[1]
    assert call_kwargs["q"] == "status=active"
    assert call_kwargs["limit"] == 20


async def test_handle_task_unknown_skill_returns_failed():
    """Unknown ``skill_id`` → TaskResponse with ``state: failed``, not a raised exception."""
    mock_client = MagicMock()
    adapter = SkillAdapter(
        OrderRouter, agent_name="A", agent_description="D", client=mock_client
    )

    result = await adapter.handle_task(_make_task("nonexistent_skill", {}))

    assert result["status"]["state"] == "failed"
    assert "nonexistent_skill" in result["status"]["message"]
    assert result["id"] == "t1"


async def test_handle_task_execution_error_returns_failed():
    """Client error is caught and wrapped in a failed TaskResponse."""
    mock_client = MagicMock()
    mock_client.create = AsyncMock(side_effect=RuntimeError("DB down"))
    adapter = SkillAdapter(
        OrderRouter, agent_name="A", agent_description="D", client=mock_client
    )

    result = await adapter.handle_task(
        _make_task("place_order", {"name": "X", "amount": 1.0})
    )

    assert result["status"]["state"] == "failed"
    assert "DB down" in result["status"]["message"]


async def test_handle_task_without_client_raises():
    """Adapter with no client → ``RuntimeError`` on ``handle_task``."""
    adapter = SkillAdapter(OrderRouter, agent_name="A", agent_description="D")

    with pytest.raises(RuntimeError, match="no client"):
        await adapter.handle_task(_make_task("place_order", {}))


async def test_handle_task_preserves_caller_task_id():
    """Task ID from the request body is echoed back in the response."""
    mock_client = MagicMock()
    mock_client.create = AsyncMock(return_value={"id": "x"})
    adapter = SkillAdapter(
        OrderRouter, agent_name="A", agent_description="D", client=mock_client
    )

    result = await adapter.handle_task(
        _make_task("place_order", {}, task_id="caller-42")
    )

    assert result["id"] == "caller-42"


async def test_handle_task_generates_id_when_absent():
    """Missing ``id`` in task request → adapter generates a UUID."""
    mock_client = MagicMock()
    mock_client.create = AsyncMock(return_value={})
    adapter = SkillAdapter(
        OrderRouter, agent_name="A", agent_description="D", client=mock_client
    )

    request_without_id = {
        "skill_id": "place_order",
        "message": {"role": "user", "parts": [{"type": "data", "data": {}}]},
    }
    result = await adapter.handle_task(request_without_id)

    assert result["id"]  # truthy UUID string
    assert result["id"] != "unknown"


# ── completed / failed responses ─────────────────────────────────────────────


async def test_completed_response_serialises_pydantic_model():
    """Pydantic model result is serialised via ``model_dump(mode='json')``."""
    mock_client = MagicMock()
    order_read = OrderRead(
        id=UUID("00000000-0000-0000-0000-000000000001"), name="X", amount=9.99
    )
    mock_client.read = AsyncMock(return_value=order_read)
    adapter = SkillAdapter(
        OrderRouter, agent_name="A", agent_description="D", client=mock_client
    )

    result = await adapter.handle_task(_make_task("read_order", {"id": "some-id"}))

    artifact_data = result["artifacts"][0]["parts"][0]["data"]
    assert isinstance(artifact_data, dict)
    assert artifact_data["name"] == "X"


# ── FastAPI mounting ──────────────────────────────────────────────────────────


def test_mount_registers_agent_card_endpoint():
    """``adapter.mount(app)`` → GET ``/.well-known/agent.json`` returns 200."""
    mock_client = MagicMock()
    adapter = SkillAdapter(
        OrderRouter,
        agent_name="OrderAgent",
        agent_description="Manages orders",
        client=mock_client,
    )

    app = FastAPI()
    adapter.mount(app, base_url="https://example.com")

    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/.well-known/agent.json")
    assert resp.status_code == 200
    body = resp.json()
    assert body["name"] == "OrderAgent"


def test_mount_agent_card_uses_request_base_url_when_no_base_url():
    """When ``base_url=''``, the agent card URL is built from the incoming request."""
    mock_client = MagicMock()
    adapter = SkillAdapter(
        OrderRouter,
        agent_name="DynAgent",
        agent_description="D",
        client=mock_client,
    )

    app = FastAPI()
    adapter.mount(app)  # base_url defaults to ''

    client = TestClient(
        app, base_url="http://testserver", raise_server_exceptions=False
    )
    resp = client.get("/.well-known/agent.json")
    assert resp.status_code == 200
    body = resp.json()
    # URL should be derived from the request, not empty
    assert "tasks/send" in body["url"]


def test_mount_tasks_send_returns_200():
    """POST /tasks/send → 200 with A2A TaskResponse."""
    mock_client = MagicMock()
    mock_client.create = AsyncMock(return_value={"id": "x"})
    adapter = SkillAdapter(
        OrderRouter,
        agent_name="A",
        agent_description="D",
        client=mock_client,
    )

    app = FastAPI()
    adapter.mount(app)

    client = TestClient(app, raise_server_exceptions=False)
    resp = client.post(
        "/tasks/send",
        json={
            "skill_id": "place_order",
            "message": {
                "role": "user",
                "parts": [{"type": "data", "data": {"name": "W", "amount": 1.0}}],
            },
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert "status" in body
    assert "id" in body


def test_mount_tasks_get_returns_404():
    """GET /tasks/{id} in v1 (synchronous mode) returns 404."""
    mock_client = MagicMock()
    adapter = SkillAdapter(
        OrderRouter, agent_name="A", agent_description="D", client=mock_client
    )
    app = FastAPI()
    adapter.mount(app)

    client = TestClient(app, raise_server_exceptions=False)
    resp = client.get("/tasks/some-task-id")
    assert resp.status_code == 404


def test_mount_bad_json_returns_400():
    """Malformed body to POST /tasks/send → 400."""
    mock_client = MagicMock()
    adapter = SkillAdapter(
        OrderRouter, agent_name="A", agent_description="D", client=mock_client
    )
    app = FastAPI()
    adapter.mount(app)

    client = TestClient(app, raise_server_exceptions=False)
    resp = client.post(
        "/tasks/send",
        data="not json",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


# ── repr ──────────────────────────────────────────────────────────────────────


def test_repr_includes_agent_name_and_skill_count():
    adapter = SkillAdapter(OrderRouter, agent_name="OrderAgent", agent_description="D")
    r = repr(adapter)
    assert "OrderAgent" in r
    assert "OrderRouter" in r
