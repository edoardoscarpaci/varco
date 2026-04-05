"""
varco_core.job.serializer
==========================
Type-aware serialization for task arguments in the named-task system.

Problem
-------
``TaskPayload`` stores args/kwargs as plain JSON-safe primitives so they can
be persisted to any backend (SQL, Redis, in-memory).  But callers want to
pass typed Python objects — ``UUID``, ``datetime``, Pydantic models — and
have them automatically round-trip through the store.

This module provides:

``TaskSerializer`` (ABC)
    Nominal interface — subclasses must explicitly inherit and implement
    ``serialize`` / ``deserialize``.  Registered in the DI container so
    the default can be swapped without touching framework code.

``DefaultTaskSerializer``
    Ships with varco_core.  Handles all common Python types and Pydantic
    models out of the box.  Pydantic is an optional dependency — the
    serializer degrades gracefully when Pydantic is not installed.
    Registered as the default ``TaskSerializer`` binding in
    ``VarcoFastAPIModule``.

Supported types (``DefaultTaskSerializer``)
-------------------------------------------
Primitive passthrough
    ``None``, ``bool``, ``int``, ``float``, ``str`` — no transformation.

UUID
    Serialized to ``str``, deserialized back to ``uuid.UUID`` when the
    annotated parameter type is ``uuid.UUID``.

datetime / date
    Serialized to ISO-8601 string via ``.isoformat()``, deserialized back
    via ``datetime.fromisoformat`` / ``date.fromisoformat``.

Pydantic BaseModel
    Serialized to ``dict`` via ``model.model_dump(mode="json")`` (returns
    JSON-safe primitives recursively).  Deserialized back via
    ``ModelClass.model_validate(dict)`` when the annotated type is a known
    ``BaseModel`` subclass.

Generic collections (``list[T]``, ``tuple[T, ...]``)
    Elements are recursively serialized.  On deserialization, if the type
    hint carries an element type (e.g. ``list[UUID]``), each element is
    deserialized to that type.

``dict[K, V]``
    Values are recursively serialized.  On deserialization, if the type
    hint carries a value type, each value is deserialized to that type.

Optional / Union
    ``Optional[X]`` (``X | None``) is handled transparently — ``None``
    is returned as-is; non-``None`` values delegate to the inner type.

Custom serializers
------------------
Subclass ``TaskSerializer``, implement ``serialize`` and ``deserialize``, then
register the subclass in the DI container::

    container.bind(TaskSerializer, MyCustomSerializer)

Or inject it into ``VarcoTask`` directly::

    task = VarcoTask(name="t", fn=fn, serializer=MyCustomSerializer())

DESIGN: ABC over Protocol
    ✅ Nominal typing — implementors must explicitly declare intent; no
       accidental structural matches from third-party classes that happen
       to have ``serialize`` / ``deserialize`` methods with different semantics
    ✅ DI-compatible — providify binds and resolves by nominal type; a
       Protocol cannot be used as a DI token without explicit ``register_checkable``
    ✅ ``@abstractmethod`` enforced at class-creation time — missing implementations
       raise ``TypeError`` immediately, not at the first call site
    ✅ Optional Pydantic — guarded at module level with try/except; graceful
       degradation when Pydantic is absent
    ✅ Recursive element-type deserialization for generics (list[UUID] etc.)
    ❌ Requires inheritance — third-party serializers need a thin adapter subclass
    ❌ No schema registry — callers must keep type annotations stable across
       process restarts (e.g. renaming a Pydantic model breaks recovery)
    ❌ Deeply nested generic types (list[dict[str, UUID]]) are not fully
       introspected — only one level of element type is followed

Thread safety:  ✅ ``DefaultTaskSerializer`` is stateless; safe to share.
Async safety:   ✅ Pure synchronous value transformations; no I/O.

📚 Docs
- 🐍 https://docs.python.org/3/library/abc.html
  abc.ABC / abstractmethod — nominal ABC pattern
- 🐍 https://docs.python.org/3/library/typing.html#typing.get_type_hints
  get_type_hints — resolves string annotations at runtime
- 🐍 https://docs.python.org/3/library/typing.html#typing.get_origin
  get_origin / get_args — introspect generic aliases (list[T], Optional[T])
- 🐍 https://docs.python.org/3/library/uuid.html
  uuid.UUID — universally unique identifier
- 🐍 https://docs.python.org/3/library/datetime.html
  datetime.fromisoformat / date.fromisoformat — ISO-8601 round-trip
- 🔍 https://docs.pydantic.dev/latest/concepts/serialization/
  Pydantic model_dump / model_validate — JSON-safe round-trip serialization
"""

from __future__ import annotations

import types
import typing
import uuid
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Any

# ── Optional Pydantic support ─────────────────────────────────────────────────
#
# DESIGN: try/except at module level (not per-call)
#   ✅ Import cost paid once — per-call isinstance checks are free
#   ✅ If Pydantic is installed, full model round-trip is available
#   ❌ Module-level state — tests that mock Pydantic must reload the module
try:
    from pydantic import BaseModel as _PydanticBaseModel

    _PYDANTIC_AVAILABLE = True
except ImportError:  # pragma: no cover — optional dependency
    _PydanticBaseModel = None  # type: ignore[assignment,misc]
    _PYDANTIC_AVAILABLE = False


# ── Helper: unwrap Optional / Union ──────────────────────────────────────────


def _unwrap_optional(hint: Any) -> tuple[bool, Any]:
    """
    Detect ``Optional[X]`` / ``X | None`` and return the inner type.

    Handles both ``typing.Union`` (Python < 3.10) and ``types.UnionType``
    (``X | Y`` syntax introduced in Python 3.10).

    Args:
        hint: A type annotation (may or may not be Optional).

    Returns:
        ``(True, inner_type)`` if ``hint`` is ``Optional[X]``  /  ``X | None``,
        where ``inner_type`` is the first non-``None`` arg.
        ``(False, hint)`` otherwise.

    Edge cases:
        - ``Optional[int]`` (= ``Union[int, None]``) → ``(True, int)``
        - ``int | None`` (Python 3.10+) → ``(True, int)``
        - ``Union[int, str]`` (no None) → ``(False, Union[int, str])``
        - bare ``type`` or ``None`` → ``(False, hint)``
    """
    origin = typing.get_origin(hint)

    # typing.Union handles both Optional[X] and Union[X, Y]
    if origin is typing.Union:
        args = typing.get_args(hint)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            # True Optional — exactly one non-None arm
            return True, non_none[0]
        # Multi-arm Union with None — treat whole hint as-is
        return False, hint

    # Python 3.10+ X | Y union type (types.UnionType)
    if isinstance(hint, types.UnionType):
        args = typing.get_args(hint)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return True, non_none[0]
        return False, hint

    return False, hint


# ── TaskSerializer ABC ────────────────────────────────────────────────────────


class TaskSerializer(ABC):
    """
    Abstract base class for type-aware task argument serialization.

    Subclasses convert Python values to JSON-safe primitives (``serialize``)
    and reconstruct them from primitives using the annotated parameter type
    (``deserialize``).

    Subclass this ABC and register the implementation in the DI container::

        class MySerializer(TaskSerializer):
            def serialize(self, value: Any) -> Any: ...
            def deserialize(self, value: Any, type_hint: type | None) -> Any: ...

        # Register as the active serializer for all DI-managed tasks:
        container.bind(TaskSerializer, MySerializer)

    The framework default (``DefaultTaskSerializer``) is registered via
    ``VarcoFastAPIModule`` — installing that module is enough for most apps.

    Thread safety:  ✅ Implementations should be stateless; design for sharing.
    Async safety:   ✅ Both methods are synchronous value transformations.
    """

    @abstractmethod
    def serialize(self, value: Any) -> Any:
        """
        Convert a Python value to a JSON-safe primitive.

        Args:
            value: Any Python value that should be stored in ``TaskPayload``.

        Returns:
            A JSON-serializable primitive (``str``, ``int``, ``float``,
            ``bool``, ``None``, ``list``, or ``dict``).

        Raises:
            TypeError:  If the value cannot be converted to a JSON primitive.
        """

    @abstractmethod
    def deserialize(self, value: Any, type_hint: type | None) -> Any:
        """
        Reconstruct a Python value from a JSON primitive using the annotated type.

        Args:
            value:     The JSON primitive retrieved from ``TaskPayload``.
            type_hint: The annotated parameter type (e.g. ``uuid.UUID``,
                       ``MyModel``, ``list[int]``), or ``None`` if no
                       annotation is available.

        Returns:
            The deserialized Python value — same type as ``type_hint`` if
            reconstruction was possible, otherwise ``value`` unchanged.

        Edge cases:
            - ``type_hint=None`` → return ``value`` unchanged.
            - ``value=None`` → return ``None`` (regardless of ``type_hint``).
        """


# ── DefaultTaskSerializer ──────────────────────────────────────────────────────


class DefaultTaskSerializer(TaskSerializer):
    """
    Built-in ``TaskSerializer`` implementation supporting common Python types
    and Pydantic models.

    Handles the round-trip between typed Python objects and JSON-safe
    primitives automatically, so callers can pass ``UUID``, ``datetime``, or
    Pydantic models directly to ``enqueue_task()`` / ``task.payload()``.

    Supported types
    ---------------
    - ``None``, ``bool``, ``int``, ``float``, ``str`` — pass through unchanged.
    - ``uuid.UUID`` — serialized as ``str``, deserialized back to ``uuid.UUID``.
    - ``datetime.datetime`` — serialized as ISO-8601, deserialized back.
    - ``datetime.date`` — serialized as ISO-8601 date, deserialized back.
    - Pydantic ``BaseModel`` — serialized via ``model_dump(mode="json")``,
      deserialized via ``ModelClass.model_validate(dict)``; requires Pydantic
      to be installed.
    - ``list`` / ``tuple`` — elements are recursively serialized.  On
      deserialization, element type is extracted from the generic alias
      (e.g. ``list[UUID]``) if available.
    - ``dict`` — values are recursively serialized.  On deserialization,
      value type is extracted from the generic alias (e.g. ``dict[str, UUID]``).
    - ``Optional[X]`` / ``X | None`` — unwrapped transparently.

    Unsupported types
    -----------------
    Any value not in the above list falls through as-is.  If it is not
    JSON-serializable, ``validate_serializable()`` on the resulting
    ``TaskPayload`` will surface the error early.

    Thread safety:  ✅ Stateless — safe to use as a module-level singleton.
    Async safety:   ✅ Pure synchronous transformations; no I/O.

    Edge cases:
        - Pydantic not installed → ``BaseModel`` instances fall through to
          passthrough; ``model_dump`` will never be called.
        - ``list[list[UUID]]`` → outer list elements deserialized, but inner
          ``list[UUID]`` element type is NOT further introspected (one level).
        - Subclasses of ``datetime`` (e.g. ``pendulum.DateTime``) serialize
          correctly via ``.isoformat()`` but deserialize as plain ``datetime``.
    """

    def serialize(self, value: Any) -> Any:
        """
        Convert a Python value to a JSON-safe primitive.

        Args:
            value: The value to serialize.  See class docstring for supported types.

        Returns:
            A JSON-safe representation of ``value``.

        Edge cases:
            - ``tuple`` → serialized as ``list`` (JSON has no tuple type).
              Deserialization to ``tuple`` still works when the annotated type
              is ``tuple``.
        """
        # ── Primitive passthrough ─────────────────────────────────────────────
        # bool must be checked before int — bool IS a subclass of int
        if value is None or isinstance(value, (bool, int, float, str)):
            return value

        # ── UUID → canonical string form ─────────────────────────────────────
        if isinstance(value, uuid.UUID):
            return str(value)

        # ── datetime before date — datetime IS a subclass of date ────────────
        if isinstance(value, datetime):
            # Include timezone offset when present so round-trip is lossless
            return value.isoformat()

        if isinstance(value, date):
            return value.isoformat()

        # ── Pydantic BaseModel → JSON-safe dict ───────────────────────────────
        # DESIGN: mode="json" ensures nested types (UUID, datetime) are also
        # converted to primitives, giving us a fully flat JSON-safe dict.
        if _PYDANTIC_AVAILABLE and isinstance(value, _PydanticBaseModel):
            return value.model_dump(mode="json")

        # ── Recursive collection serialization ────────────────────────────────
        if isinstance(value, (list, tuple)):
            # Preserves list/tuple distinction at serialization; both become list
            # JSON has no tuple — deserialization uses annotated type to restore
            return [self.serialize(item) for item in value]

        if isinstance(value, dict):
            # Only values are recursively serialized — keys must already be str
            return {k: self.serialize(v) for k, v in value.items()}

        # ── Fallback: return as-is ─────────────────────────────────────────────
        # Not JSON-safe values will be caught by TaskPayload.validate_serializable()
        return value

    def deserialize(self, value: Any, type_hint: type | None) -> Any:
        """
        Reconstruct a Python value from a JSON primitive using the annotated type.

        Args:
            value:     The JSON primitive from ``TaskPayload``.
            type_hint: The annotated parameter type, or ``None`` if unavailable.

        Returns:
            The deserialized value.  Returns ``value`` unchanged when ``type_hint``
            is ``None`` or when no conversion is applicable.

        Raises:
            ValueError: If a ``UUID`` string is malformed.
            ValueError: If a datetime/date ISO-8601 string is malformed.

        Edge cases:
            - ``value is None`` → always returns ``None``.
            - Pydantic not installed → Pydantic model hints are skipped;
              the raw ``dict`` is returned.
        """
        # None is valid in any Optional context — return immediately
        if value is None:
            return None

        # No annotation available — return value unchanged
        if type_hint is None:
            return value

        # ── Unwrap Optional[X] / X | None ────────────────────────────────────
        _, inner = _unwrap_optional(type_hint)
        # _unwrap_optional returns (True, inner) for Optional; (False, hint) otherwise
        # Either way, `inner` is the type we actually want to match against
        type_hint = inner

        # ── UUID ──────────────────────────────────────────────────────────────
        if type_hint is uuid.UUID:
            return uuid.UUID(value) if isinstance(value, str) else value

        # ── datetime (must check before date — datetime IS-A date) ───────────
        if type_hint is datetime:
            return datetime.fromisoformat(value) if isinstance(value, str) else value

        # ── date ──────────────────────────────────────────────────────────────
        if type_hint is date:
            return date.fromisoformat(value) if isinstance(value, str) else value

        # ── Pydantic BaseModel subclass ───────────────────────────────────────
        if (
            _PYDANTIC_AVAILABLE
            and isinstance(type_hint, type)
            and issubclass(type_hint, _PydanticBaseModel)
        ):
            # model_validate handles dict → model, including nested types
            return type_hint.model_validate(value)

        # ── Generic list[T] / tuple[T, ...] ───────────────────────────────────
        # Extract element type from the generic alias for recursive deserialization
        origin = typing.get_origin(type_hint)
        if origin in (list, tuple):
            elem_args = typing.get_args(type_hint)
            # For list[T], elem_args = (T,); for tuple[T, ...] elem_args = (T, Ellipsis)
            # Use first arg as element type, ignoring Ellipsis sentinel
            elem_type = (
                elem_args[0] if elem_args and elem_args[0] is not Ellipsis else None
            )
            if isinstance(value, list):
                deserialized = [self.deserialize(item, elem_type) for item in value]
                # Restore to tuple when annotated as such
                return tuple(deserialized) if origin is tuple else deserialized
            return value

        # ── Generic dict[K, V] ────────────────────────────────────────────────
        if origin is dict:
            val_args = typing.get_args(type_hint)
            # dict[K, V] → val_args = (K, V); we only need V for values
            val_type = val_args[1] if len(val_args) >= 2 else None
            if isinstance(value, dict):
                return {k: self.deserialize(v, val_type) for k, v in value.items()}
            return value

        # ── Fallback: return as-is ─────────────────────────────────────────────
        return value


# ── Module-level singleton ─────────────────────────────────────────────────────
#
# Shared across all VarcoTask instances that don't specify a custom serializer.
# Stateless — safe to share between tasks and threads.
DEFAULT_SERIALIZER: DefaultTaskSerializer = DefaultTaskSerializer()

# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "TaskSerializer",
    "DefaultTaskSerializer",
    "DEFAULT_SERIALIZER",
]
