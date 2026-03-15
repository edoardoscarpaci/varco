"""
fastrest_core.query.visitor.type_coercion
==========================================
Type coercion utilities and registry for AST-based query processing.

Provides per-field and per-type coercion helpers used to convert incoming
query values (strings, JSON, iterables) into the Python types expected by
the ORM models.  Also exposes a registry that collects coercers for model
fields and a visitor that applies coercion to AST comparison nodes.

Thread safety:  ✅ ``TypeCoercionRegistry`` is safe to read concurrently.
                ⚠️ ``register_field`` mutates the registry — call at startup.
Async safety:   ✅ All methods are synchronous.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Iterable

from fastrest_core.exception.query import CoercionError
from fastrest_core.query.type import AndNode, ComparisonNode, NotNode, Operation, OrNode
from fastrest_core.query.visitor.ast_visitor import ASTVisitor


# ── Value-object holding a field's type + coercer ─────────────────────────────


@dataclass(frozen=True)
class TypeCoercionFieldInfo:
    """
    Immutable record describing how to coerce a single model field.

    Attributes:
        python_type: The target Python type (used for error messages).
        coercer:     Callable that accepts any raw value and returns the
                     coerced value of ``python_type``.

    Thread safety:  ✅ Frozen dataclass — immutable after creation.
    """

    python_type: type
    coercer: Callable[[Any], Any]


# ── Individual coercion helpers ────────────────────────────────────────────────


def coerce_boolean(value: Any) -> bool:
    """
    Coerce ``value`` to ``bool``.

    Accepts native bools, truthy/falsey strings (``"true"``, ``"1"``,
    ``"yes"`` → ``True``; anything else → ``False``), and any value
    accepted by ``bool()``.

    Args:
        value: Raw input to coerce.

    Returns:
        Coerced boolean.

    Raises:
        CoercionError: ``bool()`` itself raises (extremely rare).
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        # Case-insensitive match against common truthy strings
        return value.lower() in ("true", "1", "yes")
    try:
        return bool(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce {value!r} to bool") from exc


def coerce_datetime(value: Any) -> datetime:
    """
    Coerce ``value`` to ``datetime``.

    Accepts ISO-8601 strings (``datetime.fromisoformat``) and native
    ``datetime`` objects.

    Args:
        value: Raw input to coerce.

    Returns:
        Coerced ``datetime``.

    Raises:
        CoercionError: Value is not a string or ``datetime``, or ISO parse fails.
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise CoercionError(
                f"Failed to coerce {value!r} to datetime — expected ISO-8601 format"
            ) from exc
    raise CoercionError(
        f"Cannot coerce {value!r} (type={type(value).__name__}) to datetime"
    )


def coerce_float(value: Any) -> float:
    """
    Coerce ``value`` to ``float``.

    Args:
        value: Raw input to coerce.

    Returns:
        Coerced float.

    Raises:
        CoercionError: ``float()`` conversion fails.
    """
    try:
        return float(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce {value!r} to float") from exc


def coerce_int(value: Any) -> int:
    """
    Coerce ``value`` to ``int``.

    Args:
        value: Raw input to coerce.

    Returns:
        Coerced integer.

    Raises:
        CoercionError: ``int()`` conversion fails.
    """
    try:
        return int(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce {value!r} to int") from exc


def coerce_list(value: Any) -> list[Any]:
    """
    Coerce various inputs into a Python ``list``.

    Resolution order:
    1. Already a list → returned as-is.
    2. JSON array string (``'["a","b"]'``) → parsed.
    3. Bracket-wrapped string (``'[a,b]'``) → split on commas.
    4. Comma-separated string (``'a,b'``) → split on commas.
    5. Single non-comma string → single-element list.
    6. Any other iterable → ``list(value)``.

    Args:
        value: Raw input to coerce.

    Returns:
        Coerced list.

    Raises:
        CoercionError: Conversion is impossible.

    Edge cases:
        - Empty bracket string ``'[]'``   → ``[]``
        - Single value string ``'foo'``   → ``['foo']``
        - Non-iterable non-string scalar  → ``CoercionError``
    """
    if isinstance(value, list):
        return value

    if isinstance(value, str):
        s = value.strip()

        # Try JSON first — handles quoted strings, numbers, booleans
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass

        # Strip surrounding brackets when present
        if s.startswith("[") and s.endswith("]"):
            inner = s[1:-1].strip()
        else:
            inner = s

        if not inner:
            return []

        if "," in inner:
            parts = [p.strip() for p in inner.split(",")]
            return [
                (
                    p[1:-1]
                    if (p.startswith('"') and p.endswith('"'))
                    or (p.startswith("'") and p.endswith("'"))
                    else p
                )
                for p in parts
            ]

        # Single-value string
        return [inner]

    # Last resort: try iterating
    try:
        if isinstance(value, Iterable):
            return list(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce {value!r} to list") from exc

    raise CoercionError(
        f"Cannot coerce {value!r} (type={type(value).__name__}) to list"
    )


# ── Convenience helpers ────────────────────────────────────────────────────────


def boolean_field_info() -> TypeCoercionFieldInfo:
    """Return a ``TypeCoercionFieldInfo`` pre-configured for boolean fields."""
    return TypeCoercionFieldInfo(bool, coerce_boolean)


# ── Default type → coercer mapping ────────────────────────────────────────────

# DESIGN: module-level dict (not a class) — simple enough for a global registry.
# Callers can override with ``register_default_coercer`` without subclassing.
default_field_coercions: dict[type, Callable[[Any], Any]] = {
    bool: coerce_boolean,
    datetime: coerce_datetime,
    int: coerce_int,
    float: coerce_float,
}


def register_default_coercer(python_type: type, coercer: Callable[[Any], Any]) -> None:
    """
    Register or replace the default coercer for a Python type.

    Useful for adding coercers for custom types (e.g. ``Decimal``, ``Enum``).

    Args:
        python_type: The target type to coerce to.
        coercer:     Callable accepting any raw value, returning the coerced value.

    Edge cases:
        - Replaces an existing entry silently — no error for duplicates.
    """
    default_field_coercions[python_type] = coercer


# ── Per-field coercion registry ────────────────────────────────────────────────


@dataclass
class TypeCoercionRegistry:
    """
    Registry mapping model field names to ``TypeCoercionFieldInfo``.

    Populate via ``register_field`` for manual field registration, or use
    ``fastrest_sa.registry_from_sa_model`` for automatic SA model reflection.

    Thread safety:  ⚠️ ``register_field`` mutates the registry — call at
                    startup before serving requests.
    Async safety:   ✅ Read-only access during request handling is safe.

    Edge cases:
        - ``get()`` returns ``None`` for unregistered fields — callers should
          handle the ``None`` case gracefully (skip coercion).
    """

    fields_info: dict[str, TypeCoercionFieldInfo] = field(default_factory=dict)

    def register_field(
        self,
        field_name: str,
        python_type: type,
        coercer: Callable[[Any], Any],
    ) -> None:
        """
        Register a coercer for ``field_name``.

        Args:
            field_name:  Model column / field name.
            python_type: Target Python type for the field.
            coercer:     Callable that coerces raw input to ``python_type``.

        Edge cases:
            - Silently replaces an existing entry.
        """
        self.fields_info[field_name] = TypeCoercionFieldInfo(
            python_type=python_type,
            coercer=coercer,
        )

    def get(self, field_name: str) -> TypeCoercionFieldInfo | None:
        """
        Return the ``TypeCoercionFieldInfo`` for ``field_name``, or ``None``.

        Args:
            field_name: The field to look up.

        Returns:
            Registered info, or ``None`` if the field has no coercer.
        """
        return self.fields_info.get(field_name)


# ── Coercion visitor ───────────────────────────────────────────────────────────


class ASTTypeCoercion(ASTVisitor):
    """
    AST visitor that applies type coercion to comparison node values.

    Walks the entire AST and replaces each ``ComparisonNode`` with a new one
    whose ``value`` has been coerced to the registered Python type for that
    field.  ``IN`` operations coerce each list element individually.

    Usage::

        registry = TypeCoercionRegistry.get_default_from_sqlalchemy_model(UserORM)
        coerced_ast = ASTTypeCoercion(registry).visit(raw_ast)

    Thread safety:  ✅ Stateless after construction (registry is read-only).
    Async safety:   ✅ Synchronous.

    Edge cases:
        - Fields not in the registry are returned unchanged.
        - ``node.value is None`` (IS_NULL / IS_NOT_NULL) → returned unchanged.
        - Coercion failure raises ``CoercionError`` — not silently ignored.
    """

    def __init__(self, registry: TypeCoercionRegistry | None = None) -> None:
        """
        Initialise with an optional coercion registry.

        Args:
            registry: Pre-populated registry.  If omitted, an empty registry is
                      created (all nodes pass through unmodified).
        """
        self.registry = registry or TypeCoercionRegistry()

    def _visit_comparison(
        self, node: ComparisonNode, args: Any = None, **kwargs: Any
    ) -> ComparisonNode:
        """
        Coerce the comparison node's value using the registry.

        Args:
            node: The comparison node to process.
            args: Unused.

        Returns:
            A new ``ComparisonNode`` with a coerced ``value``, or the
            original node when no coercer is registered / value is None.

        Raises:
            CoercionError: Coercion fails for any element.
        """
        field_info = self.registry.get(node.field)
        if not field_info or node.value is None:
            return node

        if node.op == Operation.IN or isinstance(node.value, list):
            coerced: Any = [self._coerce_value(field_info, v) for v in node.value]
        else:
            coerced = self._coerce_value(field_info, node.value)

        return ComparisonNode(node.field, node.op, coerced)

    def _visit_and(self, node: AndNode, args: Any = None, **kwargs: Any) -> AndNode:
        """Recurse into AND children and return a new ``AndNode``."""
        return AndNode(self.visit(node.left), self.visit(node.right))

    def _visit_or(self, node: OrNode, args: Any = None, **kwargs: Any) -> OrNode:
        """Recurse into OR children and return a new ``OrNode``."""
        return OrNode(self.visit(node.left), self.visit(node.right))

    def _visit_not(self, node: NotNode, args: Any = None, **kwargs: Any) -> NotNode:
        """Recurse into NOT child and return a new ``NotNode``."""
        return NotNode(self.visit(node.child))

    def _coerce_value(self, field_info: TypeCoercionFieldInfo, value: Any) -> Any:
        """
        Apply ``field_info.coercer`` to ``value``, wrapping errors.

        Args:
            field_info: Registry entry with the coercer callable.
            value:      Raw value to coerce.

        Returns:
            Coerced value.

        Raises:
            CoercionError: The coercer raises for this value.
        """
        try:
            return field_info.coercer(value)
        except CoercionError:
            # Re-raise CoercionErrors as-is — they already carry a good message
            raise
        except Exception as exc:
            raise CoercionError(
                f"Failed to coerce {value!r} to {field_info.python_type.__name__}"
            ) from exc
