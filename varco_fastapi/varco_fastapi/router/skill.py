"""
varco_fastapi.router.skill
===========================
``SkillAdapter`` — convert any ``VarcoRouter`` into a Google A2A agent.

The adapter reads ``ResolvedRoute`` metadata via ``introspect_routes()`` and exposes
every route flagged with ``skill_enabled=True`` as a Google A2A skill.  Execution is
delegated to ``AsyncVarcoClient`` — no handler logic is duplicated.

Google A2A protocol surfaces:

    GET  ``/.well-known/agent.json``  → Agent Card (skill discovery)
    POST ``/tasks/send``              → execute a skill synchronously
    GET  ``/tasks/{task_id}``         → poll task status

Full A2A spec: https://google.github.io/A2A/

Typical usage::

    from varco_fastapi.router.skill import SkillAdapter
    from myapp.routers import OrderRouter
    from myapp.clients import OrderClient

    adapter = SkillAdapter(
        OrderRouter,
        agent_name="OrderAgent",
        agent_description="Manages customer orders",
        client=OrderClient(base_url="http://localhost:8080"),
    )
    adapter.mount(app)   # registers /.well-known/agent.json + /tasks/*

DI-friendly usage::

    from varco_fastapi.router.skill import bind_skill_adapter
    bind_skill_adapter(container, OrderRouter, agent_name="OrderAgent",
                       agent_description="Manages orders", client_cls=OrderClient)
    # Now injectable: Inject[SkillAdapter]

DESIGN: adapter delegates execution to AsyncVarcoClient
    ✅ Same rationale as MCPAdapter — auth, rate-limiting, serialisation handled once
    ✅ Skill protocol translation is pure I/O transformation (JSON in → JSON out)
    ❌ One extra HTTP hop vs. direct service call — negligible in agentic workflows

DESIGN: tasks are handled synchronously in v1 (no background polling)
    ✅ Simpler — no task store or background job needed for synchronous ops
    ✅ Works for all CRUD operations that complete in < 30s
    ❌ Long-running operations (file processing, ML inference) need async tasks
       — extend by integrating AbstractJobRunner in a future version

Thread safety:  ✅ SkillAdapter is constructed once and read-only after construction.
Async safety:   ✅ handle_task() is async; agent_card() has no I/O.
"""

from __future__ import annotations

import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from varco_fastapi.router.introspection import ResolvedRoute, introspect_routes

from fastapi import Request
from fastapi.responses import JSONResponse

if TYPE_CHECKING:
    from fastapi import FastAPI
    from varco_fastapi.client.base import AsyncVarcoClient

_logger = logging.getLogger(__name__)

# ── Default MIME modes ─────────────────────────────────────────────────────────

_DEFAULT_INPUT_MODES: tuple[str, ...] = ("application/json",)
_DEFAULT_OUTPUT_MODES: tuple[str, ...] = ("application/json",)

# ── A2A response shape constants ───────────────────────────────────────────────
# Task states from the A2A spec
_STATE_COMPLETED = "completed"
_STATE_FAILED = "failed"

# ── Helpers ───────────────────────────────────────────────────────────────────


def _resource_name(router_cls: type) -> str:
    """
    Derive a snake_case resource name from a router class name.

    Strips common suffixes (``Router``, ``Controller``, ``View``) and converts
    CamelCase to snake_case.

    Args:
        router_cls: The ``VarcoRouter`` subclass.

    Returns:
        Lower-case snake_case resource name (e.g. ``"order"`` for ``OrderRouter``).

    Edge cases:
        - Class named exactly ``"Router"`` → returns ``"resource"`` as fallback.
        - No recognised suffix → whole class name is snake_cased.
    """
    name = router_cls.__name__
    for suffix in ("Router", "Controller", "View", "Handler"):
        if name.endswith(suffix) and name != suffix:
            name = name[: -len(suffix)]
            break
    snake = re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()
    return snake or "resource"


def _auto_skill_id(route: ResolvedRoute, resource: str) -> str:
    """
    Generate a skill ID when no explicit ``skill_id`` override is set.

    Convention mirrors MCP tool naming:
        - CRUD: ``{crud_action}_{resource}``  e.g. ``create_order``
        - Custom: ``{method_name}``            e.g. ``ship_order``

    Args:
        route:    The ``ResolvedRoute`` to name.
        resource: Snake-case resource name.

    Returns:
        Skill ID string suitable for the Agent Card.
    """
    if route.is_crud and route.crud_action:
        return f"{route.crud_action}_{resource}"
    return route.name


def _title_case_id(skill_id: str) -> str:
    """
    Convert a snake_case skill ID to a Title Case display name.

    ``"create_order"``  →  ``"Create Order"``

    Args:
        skill_id: Snake-case skill ID string.

    Returns:
        Title-cased display name string.
    """
    return " ".join(word.capitalize() for word in skill_id.split("_"))


def _resolve_description(
    skill_desc: str | None,
    summary: str | None,
    description: str | None,
    auto: str,
) -> str:
    """
    Apply the skill description fallback chain.

    Priority: ``skill_description`` → ``summary`` → ``description`` → auto-sentence.

    Args:
        skill_desc:  Explicit override.
        summary:     OpenAPI summary.
        description: OpenAPI description.
        auto:        Auto-generated fallback.

    Returns:
        Resolved description string (never empty).
    """
    return skill_desc or summary or description or auto


# ── SkillDefinition ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SkillDefinition:
    """
    Immutable descriptor for a single A2A skill derived from a ``ResolvedRoute``.

    Used to build the Agent Card and dispatch task requests.

    Attributes:
        id:           Unique skill identifier within the agent.
        name:         Human-readable display name.
        description:  Natural-language description of what the skill does.
        input_modes:  MIME types the skill accepts.
        output_modes: MIME types the skill returns.
        route:        Source ``ResolvedRoute`` for traceability.

    Thread safety:  ✅ frozen=True — immutable.
    Async safety:   ✅ Pure value object.
    """

    id: str
    name: str
    description: str
    input_modes: tuple[str, ...]
    output_modes: tuple[str, ...]
    route: ResolvedRoute


# ── SkillAdapter ───────────────────────────────────────────────────────────────


class SkillAdapter:
    """
    Converts a ``VarcoRouter`` class into a Google A2A agent.

    Exposes three A2A protocol surfaces:

    - ``GET  /.well-known/agent.json``  — Agent Card (discovery)
    - ``POST /tasks/send``              — execute a skill synchronously
    - ``GET  /tasks/{task_id}``         — poll task status

    Delegates execution to ``AsyncVarcoClient`` — no handler logic duplicated.

    Usage::

        adapter = SkillAdapter(
            OrderRouter,
            agent_name="OrderAgent",
            agent_description="Manages customer orders",
            client=OrderClient(base_url="http://localhost:8080"),
        )
        adapter.mount(app)

    DI usage::

        bind_skill_adapter(container, OrderRouter, ...)
        # Inject[SkillAdapter] now resolves to the adapter

    Args:
        router_cls:         The ``VarcoRouter`` subclass to expose.
        agent_name:         Agent display name in the Agent Card.
        agent_description:  Agent description in the Agent Card.
        agent_version:      Semantic version string (default: ``"1.0.0"``).
        client:             ``AsyncVarcoClient`` instance for execution.
                            If ``None``, ``handle_task()`` raises ``RuntimeError``.
        base_url:           Shortcut — bare client constructed if ``client`` is ``None``.
        enabled_routes:     Explicit allowlist of route names (``None`` = all skill_enabled).

    Thread safety:  ✅ Read-only after construction.
    Async safety:   ✅ ``handle_task()`` is async; ``agent_card()`` has no I/O.
    """

    def __init__(
        self,
        router_cls: type,
        *,
        agent_name: str,
        agent_description: str,
        agent_version: str = "1.0.0",
        client: AsyncVarcoClient | None = None,
        base_url: str | None = None,
        enabled_routes: set[str] | None = None,
    ) -> None:
        self._router_cls = router_cls
        self._agent_name = agent_name
        self._agent_description = agent_description
        self._agent_version = agent_version
        self._resource = _resource_name(router_cls)

        # Resolve client — prefer explicit instance, then build from base_url
        self._client = client
        if self._client is None and base_url is not None:
            from varco_fastapi.client.base import (
                AsyncVarcoClient as _Client,
            )  # noqa: PLC0415

            # Bare client with no auth — for internal / development use only
            self._client = _Client(base_url=base_url)  # type: ignore[assignment]

        # Pre-compute skill list at construction time — routes are immutable
        all_routes = introspect_routes(router_cls)
        self._skills: list[SkillDefinition] = []
        for route in all_routes:
            if not route.skill_enabled:
                continue
            if enabled_routes is not None and route.name not in enabled_routes:
                continue

            skill_id = route.skill_id or _auto_skill_id(route, self._resource)
            skill_name = route.skill_name or _title_case_id(skill_id)
            auto_desc = f"Perform the '{route.crud_action or route.name}' operation on {self._resource}."
            description = _resolve_description(
                route.skill_description,
                route.summary,
                route.description,
                auto_desc,
            )
            input_modes = route.skill_input_modes or _DEFAULT_INPUT_MODES
            output_modes = route.skill_output_modes or _DEFAULT_OUTPUT_MODES

            self._skills.append(
                SkillDefinition(
                    id=skill_id,
                    name=skill_name,
                    description=description,
                    input_modes=input_modes,
                    output_modes=output_modes,
                    route=route,
                )
            )

        # O(1) lookup for dispatch in handle_task()
        self._skill_by_id: dict[str, SkillDefinition] = {s.id: s for s in self._skills}

    # ── Public read-only properties ────────────────────────────────────────────

    @property
    def skills(self) -> list[SkillDefinition]:
        """All A2A skill definitions derived from ``skill_enabled`` routes."""
        return self._skills

    @property
    def router_class(self) -> type:
        """The ``VarcoRouter`` class this adapter was built from."""
        return self._router_cls

    # ── Agent Card ─────────────────────────────────────────────────────────────

    def agent_card(self, *, base_url: str) -> dict[str, Any]:
        """
        Produce the Google A2A Agent Card JSON.

        The Agent Card is served at ``GET /.well-known/agent.json`` and describes
        the agent's identity, capabilities, and available skills.

        Args:
            base_url: The public base URL of the running service
                      (e.g. ``"https://api.example.com"``).  Used to build the
                      ``url`` field that other agents call to submit tasks.

        Returns:
            A JSON-serialisable dict conforming to the A2A Agent Card schema.

        Edge cases:
            - ``base_url`` should NOT have a trailing slash — the adapter appends
              ``/tasks/send`` itself.
            - Skills with no ``skill_enabled`` routes → ``skills`` list is empty;
              the card is still valid but the agent will reject all task requests.

        Async safety:   ✅ Pure — no I/O.
        """
        return {
            "name": self._agent_name,
            "description": self._agent_description,
            "version": self._agent_version,
            # Full URL where other agents POST tasks/send requests
            "url": f"{base_url.rstrip('/')}/tasks/send",
            "capabilities": {
                # v1 is synchronous only — no streaming push updates
                "streaming": False,
                "pushNotifications": False,
                "stateTransitionHistory": False,
            },
            "skills": [
                {
                    "id": skill.id,
                    "name": skill.name,
                    "description": skill.description,
                    "inputModes": list(skill.input_modes),
                    "outputModes": list(skill.output_modes),
                }
                for skill in self._skills
            ],
        }

    # ── Task execution ─────────────────────────────────────────────────────────

    async def handle_task(self, task_request: dict[str, Any]) -> dict[str, Any]:
        """
        Handle a ``POST /tasks/send`` request body.

        Extracts ``skill_id`` and ``message.parts[0].data`` from the A2A task
        request, dispatches to ``AsyncVarcoClient``, and wraps the result in an
        A2A ``TaskResponse`` with a completed ``artifact``.

        Args:
            task_request: Parsed JSON body of the ``POST /tasks/send`` request.
                          Expected shape::

                              {
                                "id": "optional-caller-id",
                                "skill_id": "create_order",
                                "message": {
                                    "role": "user",
                                    "parts": [{"type": "data", "data": {...}}]
                                }
                              }

        Returns:
            A2A ``TaskResponse`` dict::

                {
                    "id": "task-uuid",
                    "status": {"state": "completed"},
                    "artifacts": [{"parts": [{"type": "data", "data": {...}}]}]
                }

        Raises:
            RuntimeError: If the adapter has no ``client``.

        Edge cases:
            - Unknown ``skill_id`` → returns a ``failed`` TaskResponse (not a 4xx)
              so the calling agent can retry or escalate.
            - Missing ``message.parts`` → treated as empty body (``{}``) for the
              underlying client call.
            - Execution errors → wrapped in a ``failed`` TaskResponse with the
              error string in ``status.message``.

        Async safety:   ✅ Delegates to async client methods.
        """
        if self._client is None:
            raise RuntimeError(
                "SkillAdapter has no client — pass client= or base_url= at construction."
            )

        # Generate a stable task ID (or reuse one if the caller provided it)
        task_id = task_request.get("id") or str(uuid.uuid4())
        skill_id = task_request.get("skill_id", "")

        skill = self._skill_by_id.get(skill_id)
        if skill is None:
            known = list(self._skill_by_id.keys())
            return _failed_response(
                task_id,
                f"Unknown skill '{skill_id}'. Available: {known}. "
                "Did you forget to set skill=True on the route?",
            )

        # Extract the body from the first message part
        # A2A messages have role + parts array; data is in parts[0].data
        body: dict[str, Any] = {}
        try:
            parts = task_request.get("message", {}).get("parts", [])
            if parts:
                first = parts[0]
                # Support both {"type": "data", "data": {...}} and raw dict
                body = first.get("data", first) if isinstance(first, dict) else {}
        except Exception:  # noqa: BLE001
            body = {}

        try:
            result = await self._dispatch(skill.route, body)
            return _completed_response(task_id, result)
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "SkillAdapter task %s failed: %s", task_id, exc, exc_info=True
            )
            return _failed_response(task_id, str(exc))

    async def _dispatch(self, route: ResolvedRoute, body: dict[str, Any]) -> Any:
        """
        Route a skill task to the correct ``AsyncVarcoClient`` method.

        Args:
            route:  The matched ``ResolvedRoute``.
            body:   Arguments extracted from the A2A message parts.

        Returns:
            Client method result (Pydantic model or dict).

        Edge cases:
            - CRUD routes use ``"id"`` as the primary key path param.
            - Custom routes pass all body fields as the request body.
        """
        action = route.crud_action
        client = self._client
        # Pull entity ID from body for routes that address a specific entity
        entity_id = body.pop("id", None)

        if action == "create":
            return await client.create(body)  # type: ignore[union-attr]
        elif action == "read":
            return await client.read(entity_id)  # type: ignore[union-attr]
        elif action == "update":
            return await client.update(entity_id, body)  # type: ignore[union-attr]
        elif action == "patch":
            return await client.patch(entity_id, body)  # type: ignore[union-attr]
        elif action == "delete":
            return await client.delete(entity_id)  # type: ignore[union-attr]
        elif action == "list":
            return await client.list(  # type: ignore[union-attr]
                q=body.get("q"),
                sort=body.get("sort"),
                limit=body.get("limit", 50),
                offset=body.get("offset", 0),
            )
        else:
            # Custom @route — pass entire body; path uses stored path template
            path_params = {p: body.pop(p) for p in route.path_params if p in body}
            return await client.request(  # type: ignore[union-attr]
                method=route.method,
                path=route.path.format(**path_params),
                json=body or None,
            )

    # ── FastAPI mounting ───────────────────────────────────────────────────────

    def mount(
        self,
        app: FastAPI,
        *,
        base_url: str = "",
        agent_card_path: str = "/.well-known/agent.json",
        tasks_prefix: str = "/tasks",
    ) -> None:
        """
        Mount the A2A protocol surfaces on a FastAPI application.

        Registers three routes:

        - ``GET  {agent_card_path}``              → Agent Card JSON
        - ``POST {tasks_prefix}/send``            → task execution
        - ``GET  {tasks_prefix}/{task_id}``       → task status (v1: echo back)

        Args:
            app:               The ``FastAPI`` application to mount onto.
            base_url:          Public base URL embedded in the Agent Card's ``url``
                               field.  If empty, FastAPI's ``root_path`` is used
                               at request time.
            agent_card_path:   Path for the Agent Card endpoint.
                               Default: ``"/.well-known/agent.json"``.
            tasks_prefix:      Prefix for task endpoints.
                               Default: ``"/tasks"``.

        Thread safety:  ✅ Called once at startup.
        Async safety:   ✅ Route registration has no I/O.
        """
        # Capture self to avoid closure over changing outer scope
        _adapter = self
        _base_url = base_url

        # ── GET /.well-known/agent.json ──────────────────────────────────────
        @app.get(agent_card_path, include_in_schema=False)
        async def _agent_card_endpoint(request: Request) -> JSONResponse:
            """
            Serve the A2A Agent Card.

            Uses the request's base URL if ``base_url`` was not provided at
            mount time — enables correct URL generation behind reverse proxies.
            """
            resolved_base = _base_url or str(request.base_url).rstrip("/")
            return JSONResponse(_adapter.agent_card(base_url=resolved_base))

        # ── POST /tasks/send ─────────────────────────────────────────────────
        @app.post(f"{tasks_prefix}/send", include_in_schema=False)
        async def _tasks_send_endpoint(request: Request) -> JSONResponse:
            """
            Execute a skill synchronously and return the A2A TaskResponse.

            Body must be a valid A2A task request JSON object.
            """
            try:
                body = await request.json()
            except Exception as exc:  # noqa: BLE001
                return JSONResponse(
                    _failed_response("unknown", f"Invalid JSON body: {exc}"),
                    status_code=400,
                )
            result = await _adapter.handle_task(body)
            # Use 200 for completed, 200 for failed too (A2A spec uses task state,
            # not HTTP status, to communicate success/failure)
            return JSONResponse(result)

        # ── GET /tasks/{task_id} ─────────────────────────────────────────────
        # v1: synchronous tasks are completed immediately; this endpoint simply
        # returns a "not found" response since we don't persist task history.
        # Extend this with AbstractJobStore integration when async tasks land.
        @app.get(f"{tasks_prefix}/{{task_id}}", include_in_schema=False)
        async def _tasks_get_endpoint(task_id: str) -> JSONResponse:
            """
            Poll task status.

            v1 note: tasks are synchronous — no history is stored.  Returns
            ``404`` for all task IDs (the task result was in the ``/send`` response).
            """
            return JSONResponse(
                _failed_response(
                    task_id, "Task history not available in synchronous mode."
                ),
                status_code=404,
            )

        _logger.info(
            "SkillAdapter: mounted %d skills — Agent Card at %s, tasks at %s/send",
            len(self._skills),
            agent_card_path,
            tasks_prefix,
        )

    def __repr__(self) -> str:
        return (
            f"SkillAdapter("
            f"router={self._router_cls.__name__!r}, "
            f"agent={self._agent_name!r}, "
            f"skills={len(self._skills)})"
        )


# ── A2A response builders ──────────────────────────────────────────────────────


def _completed_response(task_id: str, data: Any) -> dict[str, Any]:
    """
    Build an A2A ``TaskResponse`` for a successfully completed task.

    The ``artifact`` wraps the result data as a single JSON part, which is the
    simplest A2A artifact shape and works for all CRUD operations.

    Args:
        task_id: The task identifier echoed back to the caller.
        data:    The result payload (Pydantic model or dict).

    Returns:
        A2A-conformant ``TaskResponse`` dict.
    """
    # Pydantic models need serialisation; plain dicts pass through
    serialised: Any
    try:
        serialised = (
            data.model_dump(mode="json") if hasattr(data, "model_dump") else data
        )
    except Exception:  # noqa: BLE001
        serialised = str(data)

    return {
        "id": task_id,
        "status": {
            "state": _STATE_COMPLETED,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        "artifacts": [
            {
                "parts": [{"type": "data", "data": serialised}],
            }
        ],
    }


def _failed_response(task_id: str, message: str) -> dict[str, Any]:
    """
    Build an A2A ``TaskResponse`` for a failed task.

    Args:
        task_id: The task identifier.
        message: Human-readable error description.

    Returns:
        A2A-conformant ``TaskResponse`` dict with ``state: failed``.
    """
    return {
        "id": task_id,
        "status": {
            "state": _STATE_FAILED,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        "artifacts": [],
    }


# ── DI helper ─────────────────────────────────────────────────────────────────


def bind_skill_adapter(
    container: Any,
    router_cls: type,
    *,
    agent_name: str,
    agent_description: str,
    agent_version: str = "1.0.0",
    client_cls: type | None = None,
    base_url: str | None = None,
    enabled_routes: set[str] | None = None,
) -> None:
    """
    Register a ``SkillAdapter`` singleton in a providify ``DIContainer``.

    After this call, ``Inject[SkillAdapter]`` resolves to the adapter for
    ``router_cls``.

    Args:
        container:          ``DIContainer`` instance.
        router_cls:         The ``VarcoRouter`` subclass to expose as A2A skills.
        agent_name:         Agent display name in the Agent Card.
        agent_description:  Agent description in the Agent Card.
        agent_version:      Semantic version string (default: ``"1.0.0"``).
        client_cls:         ``AsyncVarcoClient`` subclass for execution.
                            Resolved from the container if registered.
        base_url:           Fallback base URL if ``client_cls`` is absent.
        enabled_routes:     Explicit route allowlist (``None`` = all skill_enabled).

    Edge cases:
        - Calling twice replaces the previous binding.
        - If providify is not installed, logs a warning and returns.

    Thread safety:  ✅ Registration at bootstrap (single-threaded).
    Async safety:   ✅ No I/O during registration.
    """
    try:
        from providify import Provider  # noqa: PLC0415
    except ImportError:
        _logger.warning(
            "bind_skill_adapter: providify not installed — SkillAdapter not registered in DI."
        )
        return

    # Capture all args to avoid late-binding in the closure
    _router_cls = router_cls
    _agent_name = agent_name
    _agent_description = agent_description
    _agent_version = agent_version
    _client_cls = client_cls
    _base_url = base_url
    _enabled = enabled_routes

    @Provider(singleton=True)
    def _skill_adapter_factory() -> SkillAdapter:
        """Singleton SkillAdapter factory — built once at first injection."""
        client = None
        if _client_cls is not None:
            try:
                client = container.get(_client_cls)
            except Exception:  # noqa: BLE001
                client = _client_cls(base_url=_base_url)
        return SkillAdapter(
            _router_cls,
            agent_name=_agent_name,
            agent_description=_agent_description,
            agent_version=_agent_version,
            client=client,
            base_url=_base_url,
            enabled_routes=_enabled,
        )

    _skill_adapter_factory.__annotations__["return"] = SkillAdapter
    container.provide(_skill_adapter_factory)


# ── Public API ─────────────────────────────────────────────────────────────────

__all__ = [
    "SkillAdapter",
    "SkillDefinition",
    "bind_skill_adapter",
]
