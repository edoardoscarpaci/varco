"""
varco_fastapi.router.crud
==========================
``VarcoCRUDRouter`` — service-backed CRUD router with task-based async recovery.

``VarcoCRUDRouter`` extends ``VarcoRouter`` with:

1. **Service injection** — ``Inject[AsyncService[D, PK, C, R, U]]`` injected via
   providify at class-definition time.

2. **CRUD handler dispatch** — ``_make_http_handler()`` recognizes standard CRUD
   actions (``"create"``, ``"read"``, etc.) and delegates to typed factory functions.

3. **Named-task auto-registration** — ``build_router()`` registers CRUD action
   closures in ``_task_registry`` (if resolvable), enabling async mode recovery
   via ``JobRunner.recover()`` after a process restart.

4. **Recoverable async mode** — ``?with_async=true`` submissions check the task
   registry first.  If a task is registered (name = ``"ClassName.action"``), the
   job is submitted via ``enqueue_task()`` with a persisted ``TaskPayload``.  If
   no task is registered (custom ``@route`` methods), falls back to the bare
   coroutine path (non-recoverable, but still functional).

Architecture
------------
::

    VarcoRouter           — prefix, tags, @route dispatch, DI wiring (no service)
        ↑
    VarcoCRUDRouter       — service injection, CRUD handlers, task auto-registration
        ↑
    CRUDRouter / presets  — convenience pre-composed subclasses

DESIGN: VarcoCRUDRouter extends VarcoRouter (not replaces it)
    ✅ Pure routing (``VarcoRouter``) still usable without a service
    ✅ Mixins (``CreateMixin``, ``ReadMixin``, etc.) still compose via MRO
    ✅ Existing presets just need a one-line base change (VarcoRouter → VarcoCRUDRouter)
    ❌ Two classes instead of one — negligible complexity, large clarity gain

DESIGN: CRUD tasks named "ClassName.action" (e.g. "OrderRouter.create")
    ✅ Namespace-safe — multiple routers can register tasks without name collisions
    ✅ Stable across restarts as long as the class name does not change
    ❌ Class renaming breaks recovery of in-flight jobs — document this constraint

DESIGN: _task_registry / _task_serializer / _job_runner as __init__ params (not ClassVars)
    ✅ DI injects via constructor kwargs — the ClassVar injection path was silently
       broken: ``get_type_hints`` fails on forward refs (``from __future__ import
       annotations`` + TYPE_CHECKING imports), so the ``try/except`` in
       ``_inject_class_vars_sync`` swallowed the error and never injected anything.
    ✅ Each router instance owns its own proxy — PostRouter and CommentRouter
       instances are fully independent, no cross-subclass contamination.
    ✅ ``getattr(self, ...)`` reads instance state directly; class-level test
       assignments still found via MRO fallback when the param is not provided.
    ❌ Cannot be declared at class-definition time with a plain ``cls.attr = value``
       assignment anymore — use ``__init__(task_registry=...)`` instead.

Thread safety:  ✅ Instance attributes set once at construction.
                   ``build_router()`` is called once at startup (single-threaded).
Async safety:   ✅ ``build_router()`` is synchronous; closures are async-safe.

📚 Docs
- 🐍 https://docs.python.org/3/library/typing.html#typing.ClassVar
  ClassVar — type annotation for class-level attributes
- 🔍 https://github.com/edoardo-scarpaci/providify
  providify — DI framework; Inject[], Instance[], Live[]
"""

from __future__ import annotations

import logging
from typing import Any, ClassVar

from varco_core.job import AbstractJobRunner
from varco_core.job.serializer import DEFAULT_SERIALIZER, TaskSerializer
from varco_core.service.base import D, PK, C, R, U
from providify import Inject, Instance

from varco_fastapi.router.base import (
    VarcoRouter,
    _CRUD_HANDLER_FACTORIES,
    _make_noop_handler,
)

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from varco_core.job.task import TaskRegistry
    from varco_core.service import AsyncService
    from varco_fastapi import AbstractServerAuth

logger = logging.getLogger(__name__)


def _coerce_pk(pk_type: type, value: Any) -> Any:
    """
    Coerce a primary key value (possibly a string from JSON) to the declared PK type.

    During recovery, all stored args are strings/dicts/lists (JSON-safe types).
    This helper converts them back to the declared PK type so service calls work.

    Args:
        pk_type: The declared primary key type (e.g. ``UUID``, ``int``, ``str``).
        value:   The value to coerce — typically a string from JSON deserialization.

    Returns:
        The coerced value, or ``value`` unchanged if coercion is not needed.

    Edge cases:
        - ``pk_type is str`` → returns ``value`` unchanged (already a string).
        - Unknown ``pk_type`` → attempts ``pk_type(value)``; raises ``TypeError``
          or ``ValueError`` if the conversion fails.
        - ``value`` is already the correct type → ``isinstance`` check short-circuits.
    """
    if isinstance(value, pk_type):
        # Already the right type — common at submission time (before JSON roundtrip)
        return value
    if pk_type is str:
        return str(value)
    if pk_type is int:
        return int(value)
    # UUID and other types with a string constructor
    return pk_type(value)


class VarcoCRUDRouter(VarcoRouter[D, PK, C, R, U]):
    """
    Service-backed CRUD router with task-based async recovery.

    Extends ``VarcoRouter`` with service injection, CRUD handler dispatch,
    and named-task auto-registration for recoverable async mode.

    Type parameters (mirrors ``AsyncService[D, PK, C, R, U]``):
        D:   ``DomainModel`` subclass
        PK:  Primary key type (e.g. ``UUID``, ``int``)
        C:   Create DTO
        R:   Read DTO
        U:   Update / Patch DTO

    ClassVars (declarative, set at class-definition time):
        _prefix:    URL prefix for all routes (e.g. ``"/orders"``).
        _tags:      OpenAPI tags (e.g. ``["orders"]``).
        _version:   API version prefix (e.g. ``"v2"``).
        _auth:      ``AbstractServerAuth`` instance for all routes.  Can also be
                    overridden per-instance via ``__init__(auth=...)``.
        _service:   ``AsyncService`` instance — set as a ClassVar in tests;
                    normally injected per-instance by DI via ``__init__``.

    __init__ params (injected by DI or passed directly):
        job_runner:      ``AbstractJobRunner`` for ``?with_async=true`` offload.
        task_registry:   ``TaskRegistry`` for named-task recovery after restart.
                         When not resolvable, CRUD tasks are not registered.
        task_serializer: ``TaskSerializer`` for CRUD task payloads.
                         Falls back to ``DEFAULT_SERIALIZER`` when not provided.

    CRUD action tasks are registered in ``_task_registry`` under the names
    ``"ClassName.create"``, ``"ClassName.read"``, etc. at ``build_router()`` time.
    This enables ``JobRunner.recover()`` to re-submit PENDING jobs after restart.

    Usage::

        class OrderRouter(VarcoCRUDRouter[Order, UUID, OrderCreate, OrderRead, OrderUpdate]):
            _prefix = "/orders"
            _tags = ["orders"]

        # With DI:
        container.install(VarcoFastAPIModule)
        # DI injects service, job_runner, task_registry, task_serializer via __init__.
        router = container.get(OrderRouter)
        app.include_router(router.build_router())

    Thread safety:  ✅ Instance attributes set once at construction.
    Async safety:   ✅ build_router() is synchronous; handlers are async-safe.

    Edge cases:
        - ``_task_registry`` not resolvable → tasks not registered; recovery unavailable.
          Async mode still works via the bare coroutine fallback (non-recoverable).
        - ``_service`` is ``None`` (not injected) → CRUD handlers log a warning and
          return 501 Not Implemented via ``_make_noop_handler``.
        - Renaming the router class breaks in-flight job recovery — jobs submitted
          under the old class name will not find a registered task on restart.
    """

    # ── ClassVar configuration ────────────────────────────────────────────────

    # Injected service — set via Inject[] in __init__ by providify, or as a
    # ClassVar directly in tests (e.g. _service = MockService()).  The ClassVar
    # annotation without a default value acts as a documentation-only type hint;
    # the actual value is always an instance attribute after construction.
    _service: ClassVar[AsyncService[D, PK, C, R, U] | None]  # type: ignore[type-arg]

    def __init__(
        self,
        service: Inject[AsyncService[D, PK, C, R, U]] | None = None,  # type: ignore[override]
        *,
        auth: AbstractServerAuth | None = None,
        task_registry: Instance[TaskRegistry] | None = None,
        task_serializer: Instance[TaskSerializer] | None = None,
        job_runner: Instance[AbstractJobRunner] | None = None,
    ) -> None:
        """
        Args:
            service:        The ``AsyncService`` instance injected by providify.
                            When ``None`` (e.g. tests that set ``_service`` as a
                            ClassVar), the ClassVar value is used instead.
            auth:           Optional ``AbstractServerAuth`` for all routes built
                            by this router.  Shadows the class-level ``_auth``
                            ClassVar when provided.
            task_registry:  ``TaskRegistry`` proxy for named-task registration and
                            recovery.  Injected lazily — ``is_resolvable()``
                            returns ``False`` if not in the DI container, causing
                            ``build_router()`` to skip task registration gracefully.
            task_serializer: ``TaskSerializer`` proxy for CRUD task payloads.
                            Falls back to ``DEFAULT_SERIALIZER`` when not resolvable.
            job_runner:     ``AbstractJobRunner`` proxy for async route offload.
                            Passed to ``super().__init__()``; see ``VarcoRouter``.

        Edge cases:
            - ``auth=None`` → falls through to the ClassVar ``_auth`` (set at
              class definition time, or also ``None`` if never set).
            - ``task_registry=None`` → CRUD tasks not registered; async mode
              still works via the bare coroutine fallback.
            - ``task_serializer=None`` → falls back to ``DEFAULT_SERIALIZER``.

        DESIGN: _task_registry and _task_serializer as __init__ params (not ClassVars)
            ✅ DI injects via constructor kwargs — avoids the silent ClassVar
               injection failure caused by get_type_hints failing on forward refs
               (``from __future__ import annotations`` + TYPE_CHECKING imports).
            ✅ Each instance owns its own proxy — PostRouter and CommentRouter
               instances are completely independent.
            ✅ ``getattr(self, ...)`` correctly finds instance state.
            ❌ Cannot be declared at class-definition time; use __init__ params
               or assign directly to the class for test convenience.
        """
        # Pass job_runner up to VarcoRouter.__init__ so self._job_runner is set.
        super().__init__(job_runner=job_runner)

        if service is not None:
            # DI-injected: store on the instance, takes priority over ClassVar.
            self._service = service  # type: ignore[assignment]
        # If service is None, fall back to ClassVar _service (test-only pattern).

        if auth is not None:
            # Instance attribute shadows the ClassVar — each router instance can
            # carry its own auth strategy without touching the class definition.
            self._auth = auth  # type: ignore[assignment]

        # Only write instance attributes when values are provided — same rationale as
        # _job_runner in VarcoRouter.__init__: if the param is None (default, no DI),
        # leaving the class namespace untouched lets a class-level assignment
        # ``_task_registry = registry`` (test-only pattern) survive via MRO lookup.
        if task_registry is not None:
            self._task_registry = task_registry
        if task_serializer is not None:
            self._task_serializer = task_serializer

    def build_router(self) -> Any:
        """
        Build the FastAPI ``APIRouter`` and auto-register CRUD tasks.

        Extends ``VarcoRouter.build_router()`` by registering named CRUD action
        tasks in the ``TaskRegistry`` (if resolvable) before materializing routes.

        Task names follow the convention ``"ClassName.action"`` (e.g.
        ``"OrderRouter.create"``).  This ensures uniqueness across multiple routers
        without requiring manual name management.

        Returns:
            A FastAPI ``APIRouter`` with all declared CRUD and custom routes registered.

        Edge cases:
            - ``_task_registry`` not resolvable → tasks not registered; no error.
            - Task registration happens before route materialization — tasks are
              available for recovery even before the first HTTP request.
        """
        from varco_core.job.task import TaskRegistry as _TaskRegistry

        # Resolve type args early — needed for task serialization closures
        from varco_fastapi.router.base import _resolve_type_args

        type_args = _resolve_type_args(type(self))

        # ── Register CRUD tasks ───────────────────────────────────────────────
        # Try to resolve the task registry from DI.
        registry: _TaskRegistry | None = None
        # Read from instance — set by __init__ (DI-injected proxy or direct test value).
        # Falls back via MRO to a class-level assignment for backward compat.
        _tr_proxy = getattr(self, "_task_registry", None)
        if _tr_proxy is not None:
            if hasattr(_tr_proxy, "is_resolvable") and _tr_proxy.is_resolvable():
                registry = _tr_proxy.get()
            elif not hasattr(_tr_proxy, "is_resolvable") and isinstance(
                _tr_proxy, _TaskRegistry
            ):
                # Directly assigned TaskRegistry (test setup) — use as-is
                registry = _tr_proxy

        if registry is not None:
            # Resolve the DI-provided serializer, falling back to the module-level
            # DEFAULT_SERIALIZER for DI-less setups (tests, standalone scripts).
            serializer: TaskSerializer = DEFAULT_SERIALIZER
            _ts_proxy = getattr(self, "_task_serializer", None)
            if (
                _ts_proxy is not None
                and hasattr(_ts_proxy, "is_resolvable")
                and _ts_proxy.is_resolvable()
            ):
                serializer = _ts_proxy.get()

            self._register_crud_tasks(registry, type_args, serializer)
            logger.debug(
                "VarcoCRUDRouter %s registered CRUD tasks in TaskRegistry",
                type(self).__name__,
            )

        # Delegate route materialization to the base class
        return super().build_router()

    def _register_crud_tasks(
        self,
        registry: Any,  # TaskRegistry — typed as Any to avoid runtime import
        type_args: tuple[type, ...] | None,
        serializer: TaskSerializer | None = None,
    ) -> None:
        """
        Create and register named ``VarcoTask`` closures for all CRUD actions.

        Each task closes over ``self`` (the router instance, which holds the live
        service reference) so recovery always uses the current DI-resolved service,
        not a stale captured reference from a previous process lifetime.

        Task names: ``"ClassName.action"`` (e.g. ``"OrderRouter.create"``).

        Args:
            registry:   ``TaskRegistry`` to register tasks into.
            type_args:  Resolved generic type args ``(D, PK, C, R, U)``; ``None``
                        if type args are not resolvable (all handled gracefully).
            serializer: ``TaskSerializer`` to attach to every ``VarcoTask``.
                        When ``None``, falls back to ``DEFAULT_SERIALIZER``.
                        Injected from the DI container by ``build_router()``; tests
                        that don't install ``VarcoFastAPIModule`` get the default.

        Edge cases:
            - ``type_args is None`` → PK coercion in ``read``/``update``/``patch``/
              ``delete`` defaults to ``str`` (no-op coercion).
            - Service not set → tasks still registered; calls will fail at runtime.
        """
        # Resolve serializer — caller passes the DI-resolved one; fall back otherwise
        _serializer = serializer if serializer is not None else DEFAULT_SERIALIZER
        from varco_core.job.task import VarcoTask

        cls_name = type(self).__name__
        service = self._service

        # Extract PK type for coercion at recovery time
        # type_args = (D, PK, C, R, U)
        pk_type: type = str  # fallback when type args are not known
        if type_args is not None and len(type_args) >= 2:
            pk_type = type_args[1]

        # ── create task ───────────────────────────────────────────────────────
        # Serialized args: (body_dict, auth_snapshot_dict_or_None)
        # body is a plain dict (Pydantic model dumped to dict)
        # auth_snapshot is from auth_context_to_snapshot()
        _service_ref = service  # capture in closure
        _pk_type = pk_type

        async def _task_create(body_dict: dict, auth_snapshot: dict | None) -> Any:
            # Re-hydrate auth context from snapshot so the service has correct identity
            from varco_core.job.base import auth_context_from_snapshot
            from varco_fastapi.context import auth_context as _auth_ctx

            if auth_snapshot is not None:
                ctx = auth_context_from_snapshot(auth_snapshot)
                async with _auth_ctx(ctx):
                    # Determine the create DTO type to reconstruct the Pydantic model
                    _c_type = type_args[2] if type_args and len(type_args) > 2 else None
                    body = _c_type(**body_dict) if _c_type is not None else body_dict
                    return await _service_ref.create(body, ctx)
            else:
                _c_type = type_args[2] if type_args and len(type_args) > 2 else None
                body = _c_type(**body_dict) if _c_type is not None else body_dict
                return await _service_ref.create(body)

        registry.register(
            VarcoTask(
                name=f"{cls_name}.create", fn=_task_create, serializer=_serializer
            )
        )

        # ── read task ─────────────────────────────────────────────────────────
        async def _task_read(pk_str: str, auth_snapshot: dict | None) -> Any:
            from varco_core.job.base import auth_context_from_snapshot
            from varco_fastapi.context import auth_context as _auth_ctx

            pk = _coerce_pk(_pk_type, pk_str)
            if auth_snapshot is not None:
                ctx = auth_context_from_snapshot(auth_snapshot)
                async with _auth_ctx(ctx):
                    return await _service_ref.read(pk, ctx)
            return await _service_ref.read(pk)

        registry.register(
            VarcoTask(name=f"{cls_name}.read", fn=_task_read, serializer=_serializer)
        )

        # ── update task ───────────────────────────────────────────────────────
        async def _task_update(
            pk_str: str, body_dict: dict, auth_snapshot: dict | None
        ) -> Any:
            from varco_core.job.base import auth_context_from_snapshot
            from varco_fastapi.context import auth_context as _auth_ctx

            pk = _coerce_pk(_pk_type, pk_str)
            _u_type = type_args[4] if type_args and len(type_args) > 4 else None
            body = _u_type(**body_dict) if _u_type is not None else body_dict
            if auth_snapshot is not None:
                ctx = auth_context_from_snapshot(auth_snapshot)
                async with _auth_ctx(ctx):
                    return await _service_ref.update(pk, body, ctx)
            return await _service_ref.update(pk, body)

        registry.register(
            VarcoTask(
                name=f"{cls_name}.update", fn=_task_update, serializer=_serializer
            )
        )

        # ── patch task ────────────────────────────────────────────────────────
        async def _task_patch(
            pk_str: str, body_dict: dict, auth_snapshot: dict | None
        ) -> Any:
            from varco_core.job.base import auth_context_from_snapshot
            from varco_fastapi.context import auth_context as _auth_ctx

            pk = _coerce_pk(_pk_type, pk_str)
            _u_type = type_args[4] if type_args and len(type_args) > 4 else None
            body = _u_type(**body_dict) if _u_type is not None else body_dict
            if auth_snapshot is not None:
                ctx = auth_context_from_snapshot(auth_snapshot)
                async with _auth_ctx(ctx):
                    return await _service_ref.patch(pk, body, ctx)
            return await _service_ref.patch(pk, body)

        registry.register(
            VarcoTask(name=f"{cls_name}.patch", fn=_task_patch, serializer=_serializer)
        )

        # ── delete task ───────────────────────────────────────────────────────
        async def _task_delete(pk_str: str, auth_snapshot: dict | None) -> None:
            from varco_core.job.base import auth_context_from_snapshot
            from varco_fastapi.context import auth_context as _auth_ctx

            pk = _coerce_pk(_pk_type, pk_str)
            if auth_snapshot is not None:
                ctx = auth_context_from_snapshot(auth_snapshot)
                async with _auth_ctx(ctx):
                    await _service_ref.delete(pk, ctx)
            else:
                await _service_ref.delete(pk)

        registry.register(
            VarcoTask(
                name=f"{cls_name}.delete", fn=_task_delete, serializer=_serializer
            )
        )

        # ── list task — not recoverable via named task (query is complex) ─────
        # List operations carry a QueryParams object which is not trivially
        # serializable.  They are intentionally excluded from task recovery.
        # Async list is still supported via the bare-coro fallback in _submit_job.

    def _make_http_handler(
        self,
        route: Any,
        type_args: tuple[type, ...] | None,
    ) -> Any:
        """
        Create a FastAPI endpoint closure for an HTTP route.

        Extends ``VarcoRouter._make_http_handler()`` by dispatching CRUD actions
        to typed factory functions.  Custom ``@route`` methods are forwarded to
        ``super()`` (the base ``VarcoRouter`` logic).

        Args:
            route:     The ``ResolvedRoute`` describing the endpoint.
            type_args: Resolved generic type args for request/response model annotation.

        Returns:
            An async callable suitable for ``APIRouter.add_api_route()``.

        Edge cases:
            - ``crud_action`` not in ``_CRUD_HANDLER_FACTORIES`` → delegates to
              ``super()._make_http_handler()`` (custom ``@route`` logic).
            - ``_service is None`` → ``_make_noop_handler`` returns 501 Not Implemented.
        """
        crud_action = route.crud_action

        if crud_action not in _CRUD_HANDLER_FACTORIES:
            # Not a CRUD action — delegate to base class (custom @route handling)
            return super()._make_http_handler(route, type_args)

        # Resolve server auth and job runner — instance attribute (set by __init__),
        # with MRO fallback for test subclasses that assign these at class level.
        server_auth = getattr(self, "_auth", None)
        _jr_proxy = getattr(self, "_job_runner", None)
        job_runner: AbstractJobRunner | None = None
        if (
            _jr_proxy is not None
            and hasattr(_jr_proxy, "is_resolvable")
            and _jr_proxy.is_resolvable()
        ):
            job_runner = _jr_proxy.get()
        elif _jr_proxy is not None and not hasattr(_jr_proxy, "is_resolvable"):
            job_runner = _jr_proxy

        service = getattr(self, "_service", None)
        if service is None:
            logger.warning(
                "VarcoCRUDRouter %s: _service is None for CRUD action %r — returning 501",
                type(self).__name__,
                crud_action,
            )
            return _make_noop_handler(route)

        factory = _CRUD_HANDLER_FACTORIES[crud_action]
        return factory(
            router=self,
            route=route,
            service=service,
            server_auth=server_auth,
            job_runner=job_runner,
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "VarcoCRUDRouter",
]
