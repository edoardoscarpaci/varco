from __future__ import annotations
import json
from typing import Any, Callable, Optional, Type, Dict, Iterable
from fastrest.query.type import ComparisonNode, AndNode, OrNode, NotNode, Operation
from fastrest.query.visitor.ast_visitor import ASTVisitor
from dataclasses import dataclass, field
from sqlalchemy.orm import DeclarativeBase
from fastrest.exception.query import CoercionError
from datetime import datetime

"""Type coercion utilities and registry for AST-based query processing.

This module provides per-field and per-type coercion helpers used to
convert incoming query values (strings, JSON, iterables) into Python
types expected by SQLAlchemy models. It also exposes a registry that
collects coercers for model fields and a visitor that applies coercion
to AST comparison nodes.
"""


@dataclass(frozen=True)
class TypeCoercionFieldInfo:
    python_type: Type
    coercer: Callable[[Any], Any]
    """Holds the python type and coercer callable for a model field.

    Attributes:
        python_type: The target Python type for the field.
        coercer: Callable that accepts a raw value and returns the coerced value.
    """


def coerce_boolean(value: Any) -> bool:
    """Coerce a value to boolean.

    Accepts bools, common truthy/falsey strings and falls back to `bool()`.
    Raises `CoercionError` on failure.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    try:
        return bool(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce value {value} to boolean") from exc


def coerce_datetime(value: Any):
    """Coerce a value to `datetime`.

    Accepts ISO-formatted strings or `datetime` objects. Raises
    `CoercionError` if coercion is not possible.
    """
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise CoercionError(f"Failed to coerce value {value} to datetime") from exc
    raise CoercionError(f"Cannot coerce value {value} to datetime")


def coerce_float(value: Any) -> float:
    """Coerce a value to `float`. Raises `CoercionError` on failure."""
    try:
        return float(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce value {value} to float") from exc


def coerce_int(value: Any) -> int:
    """Coerce a value to `int`. Raises `CoercionError` on failure."""
    try:
        return int(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce value {value} to int") from exc


def coerce_list(value: Any) -> list:
    """Coerce various inputs into a Python list.

    Supports:
    - JSON arrays (e.g. `'["a","b"]'`)
    - bracketed lists without quotes (e.g. `'[a,b]'`)
    - comma-separated strings (`'a,b'`)
    - single values (returned as single-item list)
    - iterables

    Raises `CoercionError` if conversion fails.
    """
    if isinstance(value, list):
        return value

    # Strings may be JSON arrays, bracketed lists, or comma-separated values.
    if isinstance(value, str):
        s = value.strip()
        # Try JSON first (handles quoted strings, numbers, booleans, nested lists)
        try:
            parsed = json.loads(s)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass

        # Remove surrounding brackets if present and split on commas
        if s.startswith("[") and s.endswith("]"):
            inner = s[1:-1].strip()
        else:
            inner = s

        if inner == "":
            return []

        # Split on commas but be tolerant to single values
        if "," in inner:
            parts = [p.strip() for p in inner.split(",")]

            # strip surrounding quotes from each part
            def strip_quotes(x: str) -> str:
                if (x.startswith('"') and x.endswith('"')) or (
                    x.startswith("'") and x.endswith("'")
                ):
                    return x[1:-1]
                return x

            return [strip_quotes(p) for p in parts]

        # Single value string -> return single-element list
        return [inner]

    # Try to build a list from any iterable
    try:
        if isinstance(value, Iterable):
            return list(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce value {value} to list") from exc

    raise CoercionError(f"Cannot coerce value {value} to list")


def boolean_field_info():
    """Convenience helper returning a `TypeCoercionFieldInfo` for booleans."""
    return TypeCoercionFieldInfo(bool, coerce_boolean)


default_field_coercions: Dict[Type, Callable] = {
    bool: coerce_boolean,
    datetime: coerce_datetime,
    int: coerce_int,
    float: coerce_float,
}


def register_default_coercer(python_type: Type, coercer: Callable[[Any], Any]) -> None:
    """Register or override a default coercer for a given Python type."""
    default_field_coercions[python_type] = coercer


@dataclass
class TypeCoercionRegistry:
    fields_info: Dict[str, TypeCoercionFieldInfo] = field(default_factory=dict)

    """Registry mapping model field names to `TypeCoercionFieldInfo`.

    Use `get_default_from_sqlalchemy_model` to populate this registry from
    a SQLAlchemy declarative model. Call `register_field` to add or override
    field-specific coercers.
    """

    def register_field(
        self, field_name: str, python_type: Type, coercer: Callable[[Any], Any]
    ):
        """Register a coercer for `field_name`.

        Args:
            field_name: the model field/column name.
            python_type: the target Python type for the field.
            coercer: callable that coerces raw input to `python_type`.
        """
        self.fields_info[field_name] = TypeCoercionFieldInfo(
            python_type=python_type, coercer=coercer
        )

    def get(self, field_name: str) -> TypeCoercionFieldInfo | None:
        """Return the `TypeCoercionFieldInfo` for `field_name`, or `None`."""
        return self.fields_info.get(field_name)

    @classmethod
    def get_default_from_sqlalchemy_model(
        cls,
        model: DeclarativeBase,
        field_coercions: Dict[str, Callable[[Any], Any]] = {},
    ) -> TypeCoercionRegistry:
        """Create a registry by inspecting a SQLAlchemy model.

        It will prefer a coercer defined in `Column.info['coercer']`, then
        use any `field_coercions` passed explicitly, and finally fall back to
        per-type defaults.
        """
        registry = cls()
        for column in model.__table__.columns:
            field_name = column.name
            python_type = column.type.python_type
            # Prefer a coercer set on the Column.info when present
            coercer = None
            try:
                coercer = (
                    column.info.get("coercer") if hasattr(column, "info") else None
                )
            except Exception:
                coercer = None

            coercer = (
                coercer
                or field_coercions.get(field_name)
                or default_field_coercions.get(python_type)
            )
            if coercer is not None:
                registry.register_field(field_name, python_type, coercer)

        return registry


class ASTTypeCoercion(ASTVisitor):
    """AST visitor that applies type coercion to comparison nodes.

    The visitor walks comparison nodes and uses the provided
    `TypeCoercionRegistry` to coerce values (including element-wise coercion
    for `IN` operations).
    """

    def __init__(
        self, registry: Optional[TypeCoercionRegistry] = None, *args, **kwargs
    ):
        """Initialize with an optional `TypeCoercionRegistry`.

        If no registry is provided a new, empty registry is created.
        """
        self.registry = registry or TypeCoercionRegistry()

    def _visit_comparison(self, node: ComparisonNode, args, **kwargs) -> Any:
        """Visit a comparison node and coerce its value(s) where applicable.

        Returns a new `ComparisonNode` with coerced value(s).
        """
        field_info = self.registry.get(node.field)
        if not field_info or node.value is None:
            return node
        if node.op == Operation.IN or isinstance(node.value, list):
            coerced = [self._coerce_value(field_info, v) for v in node.value]
        else:
            coerced = self._coerce_value(field_info, node.value)
        return ComparisonNode(node.field, node.op, coerced)

    def _visit_and(self, node: AndNode, args, **kwargs) -> Any:
        """Visit an AND node and return a new `AndNode` with coerced children."""
        left = self.visit(node.left)
        right = self.visit(node.right)
        return AndNode(left, right)

    def _visit_or(self, node: OrNode, args, **kwargs) -> Any:
        """Visit an OR node and return a new `OrNode` with coerced children."""
        left = self.visit(node.left)
        right = self.visit(node.right)
        return OrNode(left, right)

    def _visit_not(self, node: NotNode, args, **kwargs) -> Any:
        """Visit a NOT node and return a new `NotNode` with the coerced child."""
        child = self.visit(node.child)
        return NotNode(child)

    def _coerce_value(self, field_info: TypeCoercionFieldInfo, value: Any) -> Any:
        """Apply the field's coercer to `value` and wrap errors in `CoercionError`."""
        try:
            return field_info.coercer(value)
        except Exception as exc:
            raise CoercionError(
                f"Failed to coerce value {value} for type {field_info.python_type}"
            ) from exc
