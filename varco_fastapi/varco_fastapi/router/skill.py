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

DESIGN: synchronous v1 + optional async v2 via AbstractJobRunner
    ✅ Backward-compatible — no runner = same synchronous behaviour as v1.
    ✅ With a runner, POST /tasks/send returns immediately with "working" state;
       clients poll GET /tasks/{task_id} for completion.
    ✅ JobRunner handles crash recovery — PENDING jobs are re-submitted on restart.
    ❌ The task result is stored as opaque bytes; callers must know the schema.

Thread safety:  ✅ SkillAdapter is constructed once and read-only after construction.
Async safety:   ✅ handle_task() is async; agent_card() has no I/O.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any
from uuid import UUID

from varco_fastapi.router.introspection import ResolvedRoute, introspect_routes

from fastapi import Request
from fastapi.responses import JSONResponse

if TYPE_CHECKING:
    from fastapi import FastAPI
    from varco_fastapi.client.base import AsyncVarcoClient
    from varco_core.job.base import AbstractJobRunner, AbstractJobStore
    from varco_core.service.conversation import AbstractConversationStore

_logger = logging.getLogger(__name__)

# ── Default MIME modes ─────────────────────────────────────────────────────────

_DEFAULT_INPUT_MODES: tuple[str, ...] = ("application/json",)
_DEFAULT_OUTPUT_MODES: tuple[str, ...] = ("application/json",)

# ── A2A response shape constants ───────────────────────────────────────────────
# Task states from the A2A spec — https://google.github.io/A2A/
_STATE_COMPLETED = "completed"
_STATE_FAILED = "failed"
# "working" is the A2A state for tasks that are in-flight (PENDING or RUNNING).
# v1 never returned this — it only appears when a JobRunner is wired in.
_STATE_WORKING = "working"
_STATE_SUBMITTED = "submitted"

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
        job_runner: AbstractJobRunner | None = None,
        job_store: AbstractJobStore | None = None,
        conversation_store: AbstractConversationStore | None = None,
    ) -> None:
        """
        Args:
            router_cls:          The ``VarcoRouter`` subclass to expose.
            agent_name:          Agent display name in the Agent Card.
            agent_description:   Agent description in the Agent Card.
            agent_version:       Semantic version string (default: ``"1.0.0"``).
            client:              ``AsyncVarcoClient`` instance for execution.
                                 If ``None``, ``handle_task()`` raises ``RuntimeError``.
            base_url:            Shortcut — bare client constructed if ``client`` is ``None``.
            enabled_routes:      Explicit allowlist of route names (``None`` = all skill_enabled).
            job_runner:          Optional ``AbstractJobRunner`` for async (long-running) tasks.
                                 When provided, ``POST /tasks/send`` returns immediately with
                                 ``state: working`` and the client polls ``GET /tasks/{task_id}``.
                                 When ``None``, tasks execute synchronously (v1 behaviour).
            job_store:           Optional ``AbstractJobStore`` used to look up task status.
                                 Required when ``job_runner`` is set; otherwise polling always
                                 returns 404.
            conversation_store:  Optional ``AbstractConversationStore`` for multi-turn context.
                                 When set, every user message and agent response is recorded,
                                 the Agent Card advertises ``multiTurnConversation: True``,
                                 and ``GET /tasks/{task_id}/history`` returns the full turn list.
                                 When ``None``, single-turn mode (default, no history stored).

        Edge cases:
            - ``job_runner`` without ``job_store``: async submission works but polling
              always returns 404 (store not queryable).
            - ``job_store`` without ``job_runner``: behaves like sync mode; store is
              not written to during task execution.
            - ``conversation_store`` failures (e.g. DB write error) are logged and
              suppressed — they must never fail the primary task execution.
        """
        self._router_cls = router_cls
        self._agent_name = agent_name
        self._agent_description = agent_description
        self._agent_version = agent_version
        self._resource = _resource_name(router_cls)

        # Async job infrastructure — both are optional for backward compatibility.
        # When job_runner is set, tasks are submitted asynchronously (at-least-once
        # delivery guaranteed by JobRunner's store + recovery mechanism).
        self._job_runner = job_runner
        self._job_store = job_store

        # Multi-turn conversation history — optional.
        # When set, user messages and agent responses are stored per task_id.
        self._conversation_store = conversation_store

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
        # DESIGN: advertise stateTransitionHistory when async runner is wired in.
        # This signals to A2A clients that they can poll GET /tasks/{id} for status.
        # "streaming" stays False — we use polling (not push) for async mode.
        async_mode = self._job_runner is not None
        multi_turn = self._conversation_store is not None
        return {
            "name": self._agent_name,
            "description": self._agent_description,
            "version": self._agent_version,
            # Full URL where other agents POST tasks/send requests
            "url": f"{base_url.rstrip('/')}/tasks/send",
            "capabilities": {
                # Streaming push not implemented — clients poll GET /tasks/{task_id}
                "streaming": False,
                # Push notifications not implemented
                "pushNotifications": False,
                # True when job_runner is wired: clients can poll for state updates
                "stateTransitionHistory": async_mode,
                # Expose async flag so clients can adjust their polling strategy
                "asyncTaskExecution": async_mode,
                # True when conversation_store is wired: history endpoint available
                "multiTurnConversation": multi_turn,
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

        **Synchronous mode** (no ``job_runner``):
            Dispatches immediately via ``AsyncVarcoClient`` and returns a
            ``completed`` or ``failed`` ``TaskResponse`` in the same HTTP response.

        **Async mode** (``job_runner`` wired in):
            Submits the dispatch coroutine to the ``JobRunner`` and returns a
            ``working`` ``TaskResponse`` immediately.  The caller must poll
            ``GET /tasks/{task_id}`` to obtain the final result.

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
            A2A ``TaskResponse`` dict.  In sync mode: ``state: completed|failed``.
            In async mode: ``state: working`` (poll ``/tasks/{id}`` for final state).

        Raises:
            RuntimeError: If the adapter has no ``client``.

        Edge cases:
            - Unknown ``skill_id`` → always returns a ``failed`` TaskResponse (not 4xx).
            - Missing ``message.parts`` → treated as empty body ``{}``.
            - In async mode, a failed ``store.save()`` causes an immediate ``failed``
              response — the task was never submitted.

        Async safety:   ✅ Delegates to async client methods and JobRunner.
        """
        if self._client is None:
            raise RuntimeError(
                "SkillAdapter has no client — pass client= or base_url= at construction."
            )

        # Generate a stable task ID (or reuse one if the caller provided it).
        # In async mode, task_id doubles as the Job UUID for store lookups.
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

        # Extract the body from the first message part.
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

        # ── Record user turn ───────────────────────────────────────────────────
        # Store the incoming message before dispatch so that even failed tasks
        # have their user turns recorded.  Failures are suppressed — a store
        # write error must never prevent task execution.
        if self._conversation_store is not None:
            await self._record_turn(task_id, "user", task_request.get("message"))

        # ── Async mode: submit to JobRunner and return "working" immediately ──
        if self._job_runner is not None:
            return await self._handle_task_async(task_id, skill, body)

        # ── Sync mode (v1): dispatch inline and return final state ─────────────
        try:
            result = await self._dispatch(skill.route, body)
            # Record agent response turn on success
            if self._conversation_store is not None:
                try:
                    serialised = (
                        result.model_dump(mode="json")
                        if hasattr(result, "model_dump")
                        else result
                    )
                except Exception:  # noqa: BLE001
                    serialised = str(result)
                await self._record_turn(task_id, "agent", serialised)
            return _completed_response(task_id, result)
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "SkillAdapter task %s failed: %s", task_id, exc, exc_info=True
            )
            return _failed_response(task_id, str(exc))

    async def _handle_task_async(
        self,
        task_id: str,
        skill: SkillDefinition,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Submit a skill execution to the ``JobRunner`` for background processing.

        Creates a ``Job`` with ``job_id`` derived from ``task_id``, enqueues the
        dispatch coroutine, and returns a ``working`` TaskResponse immediately.
        The coroutine result is stored by the runner in ``job.result`` as JSON bytes.

        Args:
            task_id: The A2A task identifier (doubles as job_id).
            skill:   The resolved ``SkillDefinition`` to execute.
            body:    Extracted request body from the A2A message parts.

        Returns:
            A2A ``TaskResponse`` with ``state: working`` on success.
            A2A ``TaskResponse`` with ``state: failed`` if store.save() raises.

        Edge cases:
            - If ``task_id`` is not a valid UUID string, a new UUID is generated
              and used as the job_id.  The A2A task_id is unchanged.
            - The dispatch result is JSON-serialized by ``JobRunner._run_job()``.
              Pydantic models are automatically serialized via ``model_dump()``.

        Async safety:   ✅ Returns after scheduling; does not block on task completion.
        """
        # Import lazily — varco_core.job is not a hard dependency at module level
        from varco_core.job.base import Job  # noqa: PLC0415

        # Safely derive a UUID from task_id; fall back to a new UUID if invalid
        try:
            job_id = UUID(task_id)
        except (ValueError, AttributeError):
            job_id = uuid.uuid4()

        job = Job(job_id=job_id)

        # Build the dispatch coroutine — captures skill.route and body by closure.
        # DESIGN: capture body here, not inside JobRunner
        #   ✅ The coro holds a reference to body until execution completes.
        #   ❌ body is not serialized to the store — non-recoverable after restart.
        #      Use enqueue_task() with a VarcoTask for full crash recovery.
        coro = self._dispatch(skill.route, dict(body))  # copy to avoid mutation

        try:
            await self._job_runner.enqueue(job, coro)  # type: ignore[union-attr]
        except Exception as exc:  # noqa: BLE001
            _logger.error(
                "SkillAdapter: failed to enqueue async task %s: %s",
                task_id,
                exc,
                exc_info=True,
            )
            return _failed_response(task_id, f"Failed to submit task: {exc}")

        _logger.info(
            "SkillAdapter: task %s submitted for async execution (skill=%s)",
            task_id,
            skill.id,
        )
        return _working_response(task_id)

    async def _record_turn(
        self,
        task_id: str,
        role: str,
        content: Any,
    ) -> None:
        """
        Append a conversation turn to the store — suppresses all errors.

        The store contract requires that a write failure must never surface to
        the caller.  Errors are logged at WARNING level and swallowed so the
        primary task execution is unaffected.

        Args:
            task_id: A2A task identifier.
            role:    ``"user"`` or ``"agent"``.
            content: The message content to record.

        Async safety:   ✅ All store methods are async.
        """
        if self._conversation_store is None:
            return
        # Import lazily — keeps module-level imports clean
        from varco_core.service.conversation import ConversationTurn  # noqa: PLC0415

        try:
            turn = ConversationTurn(role=role, content=content)
            await self._conversation_store.append(task_id, turn)
        except Exception as exc:  # noqa: BLE001
            _logger.warning(
                "SkillAdapter: failed to record %s turn for task %s: %s",
                role,
                task_id,
                exc,
            )

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
        # When a job_store is wired, look up the job and map its status to the
        # A2A state machine.  In sync mode (no store), return 404 — the result
        # was already in the POST /tasks/send response.
        _store = self._job_store

        @app.get(f"{tasks_prefix}/{{task_id}}", include_in_schema=False)
        async def _tasks_get_endpoint(task_id: str) -> JSONResponse:
            """
            Poll task status.

            When a ``job_store`` is wired, returns the current A2A task state
            mapped from the ``JobStatus``:
            - PENDING / RUNNING  → ``state: working``
            - COMPLETED          → ``state: completed`` with artifact
            - FAILED / CANCELLED → ``state: failed``

            Without a store (sync mode), returns 404 — results are in the
            ``POST /tasks/send`` response.
            """
            if _store is None:
                return JSONResponse(
                    _failed_response(
                        task_id,
                        "Task history not available — no job_store configured. "
                        "Wire job_store= to enable polling.",
                    ),
                    status_code=404,
                )

            # Safely parse task_id as UUID; reject obviously invalid IDs early
            try:
                job_id = UUID(task_id)
            except (ValueError, AttributeError):
                return JSONResponse(
                    _failed_response(task_id, f"Invalid task ID format: {task_id!r}"),
                    status_code=400,
                )

            job = await _store.get(job_id)
            if job is None:
                return JSONResponse(
                    _failed_response(task_id, f"Task '{task_id}' not found."),
                    status_code=404,
                )

            return JSONResponse(_job_to_task_response(task_id, job))

        # ── GET /tasks/{task_id}/history ──────────────────────────────────────
        # Available only when a conversation_store is wired.
        # Returns the full ordered turn list for a given task_id.
        _conv_store = self._conversation_store

        @app.get(f"{tasks_prefix}/{{task_id}}/history", include_in_schema=False)
        async def _tasks_history_endpoint(task_id: str) -> JSONResponse:
            """
            Return the multi-turn conversation history for a task.

            Requires ``conversation_store`` to be wired at adapter construction.
            Returns 404 when no store is configured.

            Response shape::

                {
                    "task_id": "...",
                    "turns": [
                        {"role": "user",  "content": {...}, "timestamp": "..."},
                        {"role": "agent", "content": {...}, "timestamp": "..."},
                    ]
                }
            """
            if _conv_store is None:
                return JSONResponse(
                    {
                        "error": "Conversation history not available — no conversation_store configured."
                    },
                    status_code=404,
                )
            turns = await _conv_store.get(task_id)
            return JSONResponse(
                {
                    "task_id": task_id,
                    "turns": [
                        {
                            "role": t.role,
                            "content": t.content,
                            "timestamp": t.timestamp.isoformat(),
                        }
                        for t in turns
                    ],
                }
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


def _working_response(task_id: str) -> dict[str, Any]:
    """
    Build an A2A ``TaskResponse`` for a task that has been submitted
    to the job runner and is now executing in the background.

    Clients should poll ``GET /tasks/{task_id}`` to obtain the final result.

    Args:
        task_id: The task identifier.

    Returns:
        A2A-conformant ``TaskResponse`` dict with ``state: working``.
    """
    return {
        "id": task_id,
        "status": {
            "state": _STATE_WORKING,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        "artifacts": [],
    }


def _job_to_task_response(task_id: str, job: Any) -> dict[str, Any]:
    """
    Map a ``Job`` (from ``AbstractJobStore``) to an A2A ``TaskResponse``.

    Maps ``JobStatus`` → A2A state:

    - ``PENDING``  / ``RUNNING``   → ``working``
    - ``COMPLETED``                → ``completed`` (with decoded artifact)
    - ``FAILED``   / ``CANCELLED`` → ``failed``

    Args:
        task_id: The original A2A task identifier string.
        job:     The ``Job`` instance from the store.

    Returns:
        A2A-conformant ``TaskResponse`` dict.

    Edge cases:
        - ``job.result`` is JSON bytes encoded by ``JobRunner``.  Decoding
          failures fall back to the raw bytes decoded as UTF-8.
        - Pydantic models in the result are already serialized to dicts by
          ``JobRunner._run_job()`` before the bytes are stored.
    """
    # Import lazily to keep module-level imports clean
    from varco_core.job.base import JobStatus  # noqa: PLC0415

    status = job.status

    if status in (JobStatus.PENDING, JobStatus.RUNNING):
        return _working_response(task_id)

    if status == JobStatus.COMPLETED:
        # Decode the result stored by JobRunner as JSON bytes
        result_data: Any = None
        if job.result:
            try:
                result_data = json.loads(job.result.decode("utf-8"))
            except Exception:  # noqa: BLE001
                # Non-JSON result — return raw string
                result_data = job.result.decode("utf-8", errors="replace")
        return _completed_response(task_id, result_data)

    # FAILED or CANCELLED
    error_msg = job.error or f"Task ended with status: {status}"
    return _failed_response(task_id, error_msg)


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
    job_runner_cls: type | None = None,
    job_store_cls: type | None = None,
    conversation_store_cls: type | None = None,
) -> None:
    """
    Register a ``SkillAdapter`` singleton in a providify ``DIContainer``.

    After this call, ``Inject[SkillAdapter]`` resolves to the adapter for
    ``router_cls``.

    Args:
        container:               ``DIContainer`` instance.
        router_cls:              The ``VarcoRouter`` subclass to expose as A2A skills.
        agent_name:              Agent display name in the Agent Card.
        agent_description:       Agent description in the Agent Card.
        agent_version:           Semantic version string (default: ``"1.0.0"``).
        client_cls:              ``AsyncVarcoClient`` subclass for execution.
                                 Resolved from the container if registered.
        base_url:                Fallback base URL if ``client_cls`` is absent.
        enabled_routes:          Explicit route allowlist (``None`` = all skill_enabled).
        job_runner_cls:          Optional ``AbstractJobRunner`` subclass for async tasks.
                                 Resolved from the container.  When provided, skills are
                                 executed in the background and clients poll for results.
        job_store_cls:           Optional ``AbstractJobStore`` subclass for status polling.
                                 Resolved from the container.  Required for polling to work.
        conversation_store_cls:  Optional ``AbstractConversationStore`` subclass for
                                 multi-turn conversation history.  Resolved from the container.

    Edge cases:
        - Calling twice replaces the previous binding.
        - If providify is not installed, logs a warning and returns.
        - ``job_runner_cls`` without ``job_store_cls``: async submission works but
          ``GET /tasks/{id}`` always returns 404.

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
    _runner_cls = job_runner_cls
    _store_cls = job_store_cls
    _conv_store_cls = conversation_store_cls

    @Provider(singleton=True)
    def _skill_adapter_factory() -> SkillAdapter:
        """Singleton SkillAdapter factory — built once at first injection."""
        client = None
        if _client_cls is not None:
            try:
                client = container.get(_client_cls)
            except Exception:  # noqa: BLE001
                client = _client_cls(base_url=_base_url)

        # Resolve optional async job runner and store from the DI container
        runner = None
        if _runner_cls is not None:
            try:
                runner = container.get(_runner_cls)
            except Exception:  # noqa: BLE001
                _logger.warning(
                    "bind_skill_adapter: could not resolve job_runner_cls=%r from container "
                    "— async tasks disabled.",
                    _runner_cls,
                )

        store = None
        if _store_cls is not None:
            try:
                store = container.get(_store_cls)
            except Exception:  # noqa: BLE001
                _logger.warning(
                    "bind_skill_adapter: could not resolve job_store_cls=%r from container "
                    "— task polling disabled.",
                    _store_cls,
                )

        conv_store = None
        if _conv_store_cls is not None:
            try:
                conv_store = container.get(_conv_store_cls)
            except Exception:  # noqa: BLE001
                _logger.warning(
                    "bind_skill_adapter: could not resolve conversation_store_cls=%r from "
                    "container — multi-turn history disabled.",
                    _conv_store_cls,
                )

        return SkillAdapter(
            _router_cls,
            agent_name=_agent_name,
            agent_description=_agent_description,
            agent_version=_agent_version,
            client=client,
            base_url=_base_url,
            enabled_routes=_enabled,
            job_runner=runner,
            job_store=store,
            conversation_store=conv_store,
        )

    _skill_adapter_factory.__annotations__["return"] = SkillAdapter
    container.provide(_skill_adapter_factory)


# ── Public API ─────────────────────────────────────────────────────────────────

__all__ = [
    "SkillAdapter",
    "SkillDefinition",
    "bind_skill_adapter",
    "_working_response",
    "_job_to_task_response",
]
