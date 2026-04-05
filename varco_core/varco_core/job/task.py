"""
varco_core.job.task
====================
Named-task abstraction for serializable, recoverable background work.

Problem
-------
Python coroutines are not serializable — a plain ``asyncio.Task`` cannot be
persisted to a store and re-submitted after a process restart.  This module
solves that problem with a Celery-inspired naming layer:

1. Every recoverable operation is registered as a ``VarcoTask`` with a stable
   string name (e.g. ``"OrderRouter.create"``).
2. When a job is submitted, its arguments are persisted as a ``TaskPayload``
   alongside the ``Job`` record in the store.
3. On recovery, the runner looks up the function by name in the ``TaskRegistry``
   and calls it with the stored args — reconstituting the original coroutine
   without any serialization of the coroutine itself.

Components
----------
``TaskPayload``
    Frozen dataclass that stores ``task_name``, ``args``, and ``kwargs``.
    JSON-serializable — safe to persist in any backend.

``VarcoTask``
    Wrapper around an async callable with a stable ``name``.  Provides
    ``payload(*args, **kwargs)`` for building ``TaskPayload`` objects.

``TaskRegistry``
    Dict-backed registry mapping names to ``VarcoTask`` instances.
    Used by ``JobRunner.recover()`` to re-hydrate tasks on startup.

``@varco_task``
    Decorator to wrap an async callable as a ``VarcoTask``.
    Supports both bare form (``@varco_task``) and parameterized form
    (``@varco_task(name="my_task", registry=my_registry)``).

DESIGN: Registry with stable string names (Celery-inspired)
    ✅ Task functions are identified by name, not by identity — safe across
       restarts, pickle, and serialization
    ✅ Zero coupling to coroutine objects — only JSON-safe primitives are stored
    ✅ Registry is DI-injectable and swappable for testing
    ✅ VarcoTask.__call__ delegates transparently — existing code that calls the
       original function does not need to change
    ❌ Name collisions silently overwrite older registrations — callers must use
       unique names (namespacing by class: e.g. ``"OrderRouter.create"``)
    ❌ Args/kwargs must be JSON-serializable — complex objects (e.g. dataclasses,
       UUIDs as objects) must be pre-converted to primitives before storing

DESIGN: TaskPayload stores args as list, kwargs as dict (not as a single dict)
    ✅ Matches Python calling conventions exactly — *args and **kwargs
    ✅ Serializes cleanly to/from JSON without ambiguity
    ❌ Positional args lose their names — callers must document arg order

Thread safety:  ✅ TaskRegistry uses no shared mutable state after registration.
                   Registration itself is expected to happen at startup (single-threaded).
Async safety:   ✅ VarcoTask.__call__ returns a coroutine — awaited by the caller.
                   No internal async state.

📚 Docs
- 🐍 https://docs.python.org/3/library/dataclasses.html
  dataclass(frozen=True) — immutable value objects with __hash__
- 🐍 https://docs.python.org/3/library/typing.html#typing.overload
  @overload — supports both bare and parameterized decorator forms
- 🔍 https://docs.celeryq.dev/en/stable/userguide/tasks.html
  Celery task naming — inspiration for stable string-name approach
"""

from __future__ import annotations

import inspect
import json
import typing
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, overload

from varco_core.job.serializer import (
    DEFAULT_SERIALIZER,
    DefaultTaskSerializer,
    TaskSerializer,
)

# ── TaskPayload ────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class TaskPayload:
    """
    Immutable, JSON-serializable descriptor of a named task invocation.

    Stores enough information to re-invoke a ``VarcoTask`` after a process
    restart — the task name for registry lookup plus the positional and
    keyword arguments needed to call it.

    All values in ``args`` and ``kwargs`` must be JSON-serializable
    (strings, numbers, booleans, lists, dicts, ``None``).  Complex objects
    such as ``UUID``, ``datetime``, or Pydantic models must be pre-converted
    to primitives by the caller before building a ``TaskPayload``.

    Attributes:
        task_name: Stable string name used to look up the function in
                   ``TaskRegistry``.  Must match the name the ``VarcoTask``
                   was registered under.
        args:      Positional arguments to pass to the task function.
                   Default: empty list.
        kwargs:    Keyword arguments to pass to the task function.
                   Default: empty dict.

    Thread safety:  ✅ frozen=True — immutable after construction; safe to share.
    Async safety:   ✅ Pure value object; no I/O.

    Edge cases:
        - ``args=[]``, ``kwargs={}`` → valid; calls the task with no arguments.
        - ``to_dict()`` / ``from_dict()`` are round-trip safe for JSON-compatible
          values.  Non-JSON values (e.g. raw ``UUID`` objects) will raise
          ``TypeError`` from ``json.dumps``.
        - Two ``TaskPayload`` instances are equal iff all three fields match.

    Example::

        payload = TaskPayload(task_name="OrderRouter.create", args=["body_json", "auth_snap"])
        d = payload.to_dict()
        restored = TaskPayload.from_dict(d)
        assert payload == restored
    """

    # Stable name used for registry lookup — must not change across restarts
    task_name: str

    # Positional args — must be JSON-serializable primitives
    # DESIGN: list not tuple so that from_dict(to_dict()) is round-trip equal
    # (json.loads returns list, not tuple)
    args: list[Any] = field(default_factory=list, compare=True, hash=False)

    # Keyword args — must be JSON-serializable primitives
    kwargs: dict[str, Any] = field(default_factory=dict, compare=True, hash=False)

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize this payload to a plain ``dict`` suitable for ``json.dumps()``.

        Returns:
            Dict with keys ``"task_name"``, ``"args"``, ``"kwargs"``.

        Raises:
            TypeError: If ``args`` or ``kwargs`` contain non-JSON-serializable values.
                       Use ``json.dumps(payload.to_dict())`` to validate at submission time.

        Edge cases:
            - Empty ``args`` / ``kwargs`` → serialized as ``[]`` / ``{}``.
        """
        return {
            "task_name": self.task_name,
            "args": list(self.args),  # copy to avoid aliasing
            "kwargs": dict(self.kwargs),  # copy to avoid aliasing
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskPayload:
        """
        Reconstruct a ``TaskPayload`` from a plain ``dict`` (e.g. from ``json.loads()``).

        Args:
            data: Dict with keys ``"task_name"`` (str), ``"args"`` (list),
                  and ``"kwargs"`` (dict).

        Returns:
            A ``TaskPayload`` with fields populated from ``data``.

        Raises:
            KeyError:  ``"task_name"`` is missing from ``data``.
            TypeError: ``"args"`` or ``"kwargs"`` have unexpected types.

        Edge cases:
            - Missing ``"args"`` key → defaults to ``[]`` (forward-compatible).
            - Missing ``"kwargs"`` key → defaults to ``{}`` (forward-compatible).
            - Extra keys in ``data`` are silently ignored (forward-compatible).
        """
        return cls(
            task_name=data["task_name"],
            args=list(data.get("args", [])),
            kwargs=dict(data.get("kwargs", {})),
        )

    def validate_serializable(self) -> None:
        """
        Eagerly validate that ``args`` and ``kwargs`` are JSON-serializable.

        Call this at submission time (before persisting to the store) to surface
        serialization errors early rather than at recovery time.

        Raises:
            TypeError:  A value in ``args`` or ``kwargs`` is not JSON-serializable.
            ValueError: A value fails JSON serialization for structural reasons.

        Edge cases:
            - This is a best-effort validation using ``json.dumps``.
            - Custom objects with ``__json__`` or other protocols are NOT handled —
              only standard JSON types are accepted.
        """
        # Let json.dumps raise TypeError/ValueError immediately if anything fails
        json.dumps(self.to_dict())


# ── VarcoTask ──────────────────────────────────────────────────────────────────


class VarcoTask:
    """
    Named wrapper around an async callable, making it identifiable by a stable
    string name and enabling serialization of its invocations via ``TaskPayload``.

    A ``VarcoTask`` is callable — invoking it returns the underlying coroutine
    directly so existing call sites do not need to change.

    Attributes:
        name: Stable identifier for this task, used as the key in
              ``TaskRegistry`` and stored in ``TaskPayload``.

    Thread safety:  ✅ Immutable after construction; ``name`` and ``_fn`` are set once.
    Async safety:   ✅ ``__call__`` returns a coroutine but does not itself do I/O.

    Edge cases:
        - ``name`` uniqueness is the caller's responsibility.  Registering two
          ``VarcoTask`` instances with the same name in a ``TaskRegistry`` means
          the second registration silently wins.
        - The wrapped function must be async.  Calling a sync function will
          return a non-awaitable, which will fail when ``JobRunner`` tries to
          ``await`` it.

    Example::

        async def _do_work(body: dict, auth: dict) -> None:
            ...

        task = VarcoTask(name="OrderService.create", fn=_do_work)
        coro = task({"name": "Widget"}, {"user_id": "u1"})  # same as _do_work(...)
        payload = task.payload({"name": "Widget"}, {"user_id": "u1"})
        # payload.task_name == "OrderService.create"
    """

    def __init__(
        self,
        name: str,
        fn: Callable[..., Coroutine[Any, Any, Any]],
        serializer: TaskSerializer | None = None,
    ) -> None:
        """
        Args:
            name:       Stable string identifier for this task.  Should be unique
                        within the application (e.g. ``"ClassName.method_name"``).
            fn:         The async callable to wrap.  Must return a coroutine when called.
            serializer: Type-aware serializer used when building ``TaskPayload`` and
                        re-hydrating args at recovery time.  Defaults to
                        ``DefaultTaskSerializer``, which handles ``UUID``, ``datetime``,
                        Pydantic models, and generic collections automatically.
                        Pass a custom instance to override serialization behaviour.
        """
        # Store as public for inspection / debugging
        self.name = name
        # Private to discourage direct access — use __call__ or payload()
        self._fn = fn
        # Serializer used for both serialize (payload()) and deserialize (invoke())
        # Fall back to the shared module-level singleton — stateless, safe to share
        self._serializer: TaskSerializer = (
            serializer if serializer is not None else DEFAULT_SERIALIZER
        )

        # ── Inspect function signature at construction time ────────────────────
        # We do this ONCE here so that invoke() has O(1) access to type hints
        # without re-calling get_type_hints on every recovery.
        #
        # DESIGN: best-effort — if introspection fails (C extensions, lambdas,
        # partial functions) we fall back to empty hints rather than raising.
        # Deserialization will then return raw JSON values unchanged.
        try:
            # get_type_hints resolves forward references and string annotations
            # (requires from __future__ import annotations to already be evaluated)
            self._type_hints: dict[str, Any] = typing.get_type_hints(fn)
        except Exception:  # pragma: no cover — defensive fallback
            self._type_hints = {}

        try:
            self._sig: inspect.Signature | None = inspect.signature(fn)
        except (ValueError, TypeError):  # pragma: no cover — defensive fallback
            self._sig = None

    def __call__(self, *args: Any, **kwargs: Any) -> Coroutine[Any, Any, Any]:
        """
        Invoke the wrapped function, returning its coroutine.

        Transparent delegation — callers that hold a ``VarcoTask`` reference
        can call it exactly like the original function.

        Args:
            *args:    Positional arguments forwarded to the wrapped function.
            **kwargs: Keyword arguments forwarded to the wrapped function.

        Returns:
            The coroutine returned by the wrapped async function.

        Edge cases:
            - Sync function → returns a non-awaitable.  The asyncio event loop
              will raise ``TypeError: object NoneType can't be used in 'await' expression``.
        """
        return self._fn(*args, **kwargs)  # type: ignore[return-value]

    def payload(self, *args: Any, **kwargs: Any) -> TaskPayload:
        """
        Build a serialized ``TaskPayload`` for later re-invocation.

        Each positional and keyword argument is passed through the task's
        ``serializer.serialize()`` so that complex types (``UUID``, ``datetime``,
        Pydantic models) are converted to JSON-safe primitives automatically.

        Args:
            *args:    Positional arguments the task will be called with.
            **kwargs: Keyword arguments the task will be called with.

        Returns:
            A ``TaskPayload`` with ``task_name=self.name``, ``args`` and
            ``kwargs`` containing serialized (JSON-safe) values.

        Edge cases:
            - Callers can still call ``payload.validate_serializable()`` to
              confirm the result is JSON-safe.  Any type not handled by the
              serializer falls through as-is.
            - Serialization is lossy for types with no type annotation on the
              receiving end (e.g. ``*args: Any``); deserialization will then
              return the raw primitive.
        """
        return TaskPayload(
            task_name=self.name,
            # Serialize each positional arg via the configured serializer
            args=[self._serializer.serialize(a) for a in args],
            # Serialize each keyword arg via the configured serializer
            kwargs={k: self._serializer.serialize(v) for k, v in kwargs.items()},
        )

    def invoke(self, payload: TaskPayload) -> Coroutine[Any, Any, Any]:
        """
        Re-invoke this task from a serialized ``TaskPayload``, deserializing
        args back to their annotated types before calling the wrapped function.

        This is the recovery entry point — the runner persisted the payload
        at enqueue time; this call reconstructs the original coroutine using
        the stored args and the type hints inspected at construction time.

        Args:
            payload: The ``TaskPayload`` describing the task invocation,
                     as returned by ``payload()`` and stored in the job record.

        Returns:
            The coroutine returned by the wrapped async function, with all
            args deserialized to their annotated types.

        Edge cases:
            - Parameters with no type annotation → raw JSON primitive is passed.
            - If ``inspect.signature`` failed at construction time (e.g. for
              built-in or C-extension callables) → args are passed unchanged.
            - Extra positional args beyond the function's parameter count →
              deserialized as-is (no hint available for those positions).

        DESIGN: deserialization happens here, not in TaskRegistry.invoke
            ✅ VarcoTask owns its serializer — encapsulation is complete
            ✅ TaskRegistry stays thin (just a dispatch table)
            ✅ Custom serializers work without changing TaskRegistry
        """
        deserialized_args, deserialized_kwargs = self._deserialize_payload(payload)
        return self._fn(*deserialized_args, **deserialized_kwargs)  # type: ignore[return-value]

    def _deserialize_payload(
        self,
        payload: TaskPayload,
    ) -> tuple[list[Any], dict[str, Any]]:
        """
        Deserialize stored args/kwargs back to typed Python values.

        Maps each positional arg to its corresponding parameter's type hint
        (by position), and each keyword arg to its hint by name.

        Args:
            payload: The payload whose ``args`` and ``kwargs`` to deserialize.

        Returns:
            ``(deserialized_args, deserialized_kwargs)`` ready to pass to
            the wrapped function.

        Thread safety:  ✅ Reads ``_type_hints`` and ``_sig`` — immutable after init.
        Async safety:   ✅ Pure synchronous transformation.

        Edge cases:
            - ``_sig is None`` → args returned unchanged (safe degradation).
            - More stored args than parameters → extra args have no hint, passed as-is.
        """
        if self._sig is None:
            # Signature unavailable — pass raw values (defensive degradation)
            return list(payload.args), dict(payload.kwargs)

        # Build ordered list of parameter names for positional-arg → hint mapping
        param_names = list(self._sig.parameters.keys())

        deserialized_args = [
            self._serializer.deserialize(
                value=val,
                # Map positional index to parameter name, then to type hint
                type_hint=(
                    self._type_hints.get(param_names[i])
                    if i < len(param_names)
                    else None
                ),
            )
            for i, val in enumerate(payload.args)
        ]

        deserialized_kwargs = {
            k: self._serializer.deserialize(
                value=v,
                # Keyword args map directly by name to their hint
                type_hint=self._type_hints.get(k),
            )
            for k, v in payload.kwargs.items()
        }

        return deserialized_args, deserialized_kwargs

    def __repr__(self) -> str:
        return f"VarcoTask(name={self.name!r}, fn={self._fn!r})"


# ── TaskRegistry ───────────────────────────────────────────────────────────────


@dataclass
class TaskRegistry:
    """
    Dict-backed registry mapping task names to ``VarcoTask`` instances.

    ``TaskRegistry`` is the central lookup table for task recovery.  When
    ``JobRunner.recover()`` finds a PENDING job with a ``TaskPayload``, it
    calls ``registry.invoke(payload)`` to re-hydrate the coroutine from the
    stored args.

    The registry is typically registered as a singleton in the DI container
    (see ``VarcoFastAPIModule``) so all routers share one instance and can
    register their tasks at ``build_router()`` time.

    DESIGN: plain dict over WeakValueDictionary
        ✅ Tasks are static by design — registered once at startup, never GC'd
        ✅ Simple and predictable — no surprise disappearances
        ❌ Manual cleanup needed if tasks are ever unregistered (rare)

    Thread safety:  ⚠️ Registration is expected at startup (single-threaded).
                       Concurrent registration from multiple threads is unsafe
                       without an external lock.
    Async safety:   ✅ ``invoke()`` returns a coroutine and has no internal state.

    Edge cases:
        - ``get(unknown_name)`` → returns ``None``, does not raise.
        - ``invoke(payload)`` with unknown name → raises ``KeyError`` with
          a helpful message listing all registered names.
        - Registering the same name twice → second registration silently wins.
          Use unique names (namespaced by class) to avoid collisions.
    """

    # Internal storage — populated via register()
    # Not in __init__ parameters — callers always start with an empty registry
    _tasks: dict[str, VarcoTask] = field(default_factory=dict, init=False)

    def register(self, task: VarcoTask) -> None:
        """
        Add a ``VarcoTask`` to the registry.

        If a task with the same name already exists, it is silently replaced.
        Log a warning if your application registers tasks dynamically to detect
        accidental overwrites.

        Args:
            task: The ``VarcoTask`` to register.

        Edge cases:
            - Duplicate name → second registration replaces the first without error.
        """
        self._tasks[task.name] = task

    def get(self, name: str) -> VarcoTask | None:
        """
        Look up a task by name.

        Args:
            name: The stable string name to look up.

        Returns:
            The ``VarcoTask`` if found, or ``None`` if not registered.

        Edge cases:
            - Unknown name → ``None`` (never raises).
        """
        return self._tasks.get(name)

    def invoke(self, payload: TaskPayload) -> Coroutine[Any, Any, Any]:
        """
        Re-invoke a task from a serialized ``TaskPayload``.

        Looks up the task by ``payload.task_name``, then calls it with the
        stored ``args`` and ``kwargs``.  This is the core of job recovery:
        the runner persisted the payload; this call re-creates the original
        coroutine.

        Args:
            payload: The ``TaskPayload`` describing the task invocation.

        Returns:
            The coroutine returned by the task function.

        Raises:
            KeyError: If ``payload.task_name`` is not registered.  The error
                message lists all known task names to help diagnose missing
                registrations.

        Edge cases:
            - Task function is sync → returns a non-awaitable → ``await`` will
              raise ``TypeError``.  Callers should validate task asyncness at
              registration time.
            - Extra kwargs in the payload that the function does not accept →
              ``TypeError`` raised by the function, not by ``invoke``.
        """
        task = self._tasks.get(payload.task_name)
        if task is None:
            known = sorted(self._tasks.keys())
            raise KeyError(
                f"No task registered under name {payload.task_name!r}. "
                f"Known tasks: {known}. "
                "Did you forget to register the task in TaskRegistry, or was "
                "VarcoCRUDRouter.build_router() not called before recovery?"
            )
        # Delegate to task.invoke() so that type-aware deserialization is applied
        # using the serializer configured on the VarcoTask itself.
        return task.invoke(payload)

    def __repr__(self) -> str:
        return f"TaskRegistry(tasks={sorted(self._tasks.keys())})"


# ── @varco_task decorator ──────────────────────────────────────────────────────
#
# Supports two call forms:
#   @varco_task                       (bare — uses fn.__qualname__ as name)
#   @varco_task(name=..., registry=...)  (parameterized)
#
# DESIGN: overload-based dual form (same pattern as @dataclass)
#   ✅ Bare form is ergonomic for simple cases
#   ✅ Parameterized form allows stable names and auto-registration
#   ❌ Slightly complex decorator implementation — mitigated by clear overloads


@overload
def varco_task(fn: Callable[..., Coroutine[Any, Any, Any]]) -> VarcoTask:
    """Bare form: ``@varco_task`` uses ``fn.__qualname__`` as the task name."""
    ...


@overload
def varco_task(
    *,
    name: str | None = None,
    registry: TaskRegistry | None = None,
    serializer: TaskSerializer | None = None,
) -> Callable[[Callable[..., Coroutine[Any, Any, Any]]], VarcoTask]:
    """Parameterized form: ``@varco_task(name=..., registry=..., serializer=...)``."""
    ...


def varco_task(
    fn: Callable[..., Coroutine[Any, Any, Any]] | None = None,
    *,
    name: str | None = None,
    registry: TaskRegistry | None = None,
    serializer: TaskSerializer | None = None,
) -> VarcoTask | Callable[[Callable[..., Coroutine[Any, Any, Any]]], VarcoTask]:
    """
    Decorator to wrap an async callable as a ``VarcoTask``.

    Supports two forms::

        # Bare — task name defaults to fn.__qualname__
        @varco_task
        async def process_order(body: dict, auth: dict) -> None: ...

        # Parameterized — stable name + optional auto-registration
        @varco_task(name="OrderService.process", registry=my_registry)
        async def process_order(body: dict, auth: dict) -> None: ...

    Args:
        fn:       When used as a bare decorator, the function being wrapped.
                  Must be ``None`` when using the parameterized form.
        name:     Stable string name for the task.  Defaults to ``fn.__qualname__``
                  if not provided.  Using ``__qualname__`` works for module-level
                  functions but may not be stable for nested or lambda functions —
                  prefer an explicit name for tasks that will be persisted.
        registry:   If provided, the ``VarcoTask`` is automatically registered in
                    this ``TaskRegistry`` after creation.
        serializer: Type-aware serializer for this task's args.  Defaults to
                    ``DefaultTaskSerializer``.  Pass a custom instance to override
                    how args are converted to/from JSON primitives.

    Returns:
        - In bare form: a ``VarcoTask`` wrapping ``fn``.
        - In parameterized form: a decorator that returns a ``VarcoTask``.

    Raises:
        TypeError: If ``fn`` is not callable (programming error).

    Edge cases:
        - Bare form with ``name`` or ``registry`` → not possible (use parameterized form).
        - ``name=None`` in parameterized form → falls back to ``fn.__qualname__``.
        - ``registry`` is registered with at decoration time — before ``build_router()``
          is called.  This is safe as long as registration is idempotent.

    Example::

        registry = TaskRegistry()

        @varco_task(name="orders.create", registry=registry)
        async def create_order(body: dict, auth: dict) -> dict:
            ...

        # Equivalent to:
        create_order = varco_task(fn=create_order, name="orders.create", registry=registry)
    """

    def _wrap(func: Callable[..., Coroutine[Any, Any, Any]]) -> VarcoTask:
        # Derive stable name — prefer explicit, fall back to qualname
        task_name = name if name is not None else func.__qualname__
        task = VarcoTask(name=task_name, fn=func, serializer=serializer)
        if registry is not None:
            # Auto-register — caller gets the VarcoTask AND the side-effect
            registry.register(task)
        return task

    if fn is not None:
        # Bare decorator form: @varco_task applied directly to the function
        return _wrap(fn)

    # Parameterized form: @varco_task(name=...) returns the decorator
    return _wrap


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "TaskPayload",
    "VarcoTask",
    "TaskRegistry",
    "varco_task",
    # Serialization
    "TaskSerializer",
    "DefaultTaskSerializer",
    "DEFAULT_SERIALIZER",
]
