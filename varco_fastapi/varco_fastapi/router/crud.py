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

DESIGN: _task_registry as ClassVar[Instance[TaskRegistry]]
    ✅ Lazy resolution — the registry may not be in the DI container for tests
    ✅ ``is_resolvable()`` guard prevents crashes in DI-less setups
    ❌ Indirect — accessing the registry requires ``.get()`` not direct attribute access

Thread safety:  ✅ ClassVars are read-only after class definition.
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

    ClassVars (set at class definition time):
        _prefix:           URL prefix for all routes (e.g. ``"/orders"``).
        _tags:             OpenAPI tags (e.g. ``["orders"]``).
        _version:          API version prefix (e.g. ``"v2"``).
        _auth:             ``AbstractServerAuth`` instance for all routes.
        _job_runner:       ``Instance[AbstractJobRunner]`` for async offload.
        _task_registry:    ``Instance[TaskRegistry]`` for named-task recovery.
                           Auto-populated from DI if ``VarcoFastAPIModule`` is installed.
        _task_serializer:  ``Instance[TaskSerializer]`` injected by DI.
                           Passed to every ``VarcoTask`` created in
                           ``_register_crud_tasks()``.  Falls back to
                           ``DEFAULT_SERIALIZER`` when DI is unavailable.

    CRUD action tasks are registered in ``_task_registry`` under the names
    ``"ClassName.create"``, ``"ClassName.read"``, etc. at ``build_router()`` time.
    This enables ``JobRunner.recover()`` to re-submit PENDING jobs after restart.

    Usage::

        class OrderRouter(VarcoCRUDRouter[Order, UUID, OrderCreate, OrderRead, OrderUpdate]):
            _prefix = "/orders"
            _tags = ["orders"]

        # With DI:
        container.install(VarcoFastAPIModule)
        # VarcoCRUDRouter picks up AbstractJobRunner and TaskRegistry from the container.
        router = OrderRouter().build_router()
        app.include_router(router)

    Thread safety:  ✅ ClassVars read-only after class definition.
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

    # Injected service — set via Inject[] at class definition or by VarcoCRUDRouter.__init__
    _service: ClassVar[AsyncService | None]  # type: ignore[type-arg]

    # Task registry for named-task registration and recovery.
    # Instance[] makes this a lazy proxy — resolved from DI on first .get() call.
    # None / unresolvable → CRUD tasks not registered; async mode falls back to bare coro.
    _task_registry: ClassVar[Instance[TaskRegistry]]  # type: ignore[assignment]

    # Serializer injected from DI (bound to TaskSerializer in VarcoFastAPIModule).
    # Instance[] is used (not Inject[]) so that DI-less test setups don't crash —
    # is_resolvable() returns False when the container has no TaskSerializer binding,
    # and the code falls back to DEFAULT_SERIALIZER in that case.
    _task_serializer: ClassVar[Instance[TaskSerializer]]  # type: ignore[assignment]

    def __init__(self, service: Inject[AsyncService[D, PK, C, R, U]] | None = None) -> None:  # type: ignore[override]
        """
        Args:
            service: The ``AsyncService`` instance injected by providify.
                     When ``None`` (e.g. in tests that set ``_service`` as a ClassVar),
                     the ClassVar value is used instead.  Providify always injects
                     a concrete service via ``Inject[]``; ``None`` is only expected
                     in manual / test-only instantiation.
        """
        super().__init__()
        if service is not None:
            # DI-injected: store on the instance, takes priority over ClassVar
            self._service = service  # type: ignore[assignment]
        # If service is None, fall back to ClassVar _service (set by test subclasses)

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
        _tr_proxy = getattr(type(self), "_task_registry", None)
        if _tr_proxy is not None:
            if hasattr(_tr_proxy, "is_resolvable") and _tr_proxy.is_resolvable():
                registry = _tr_proxy.get()
            elif not hasattr(_tr_proxy, "is_resolvable") and isinstance(
                _tr_proxy, _TaskRegistry
            ):
                # Directly assigned (not a proxy) — use as-is
                registry = _tr_proxy

        if registry is not None:
            # Resolve the DI-provided serializer, falling back to the module-level
            # DEFAULT_SERIALIZER for DI-less setups (tests, standalone scripts).
            serializer: TaskSerializer = DEFAULT_SERIALIZER
            _ts_proxy = getattr(type(self), "_task_serializer", None)
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

        # Resolve server auth and job runner — same pattern as base class
        server_auth = getattr(self, "_auth", None)
        _jr_proxy = getattr(type(self), "_job_runner", None)
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
