"""
fastrest_core.query.visitor.sqlalchemy
========================================
Translates AST query nodes into SQLAlchemy filter expressions.

This visitor is backend-specific but lives in ``fastrest_core`` because
the SQLAlchemy type system (``DeclarativeBase``, ``ColumnElement``) is the
common ground shared between ``fastrest_sa`` and any direct SA usage.

Thread safety:  ✅ Stateless after construction (``allowed_fields`` set once).
Async safety:   ✅ Synchronous; safe to call from async contexts.
"""

from __future__ import annotations

from typing import Union

from sqlalchemy import BinaryExpression, ColumnElement, and_, not_, or_
from sqlalchemy.orm import DeclarativeBase, MappedColumn

from fastrest_core.exception.query import OperationNotSupported
from fastrest_core.exception.repository import FieldNotFound
from fastrest_core.query.type import (
    ComparisonNode,
    Operation,
)
from fastrest_core.query.visitor.walking import BinaryWalkingVisitor


class SQLAlchemyQueryCompiler(BinaryWalkingVisitor):
    """
    AST visitor that compiles comparison and boolean nodes into SQLAlchemy
    filter expressions for a given mapped model class.

    DESIGN: visitor over inline SQLAlchemy expression building
      ✅ The compiler is reusable across repositories for the same model.
      ✅ ``allowed_fields`` prevents arbitrary field injection from user input.
      ❌ Does not support nested relationship traversal (dotted paths are blocked).

    Thread safety:  ✅ Read-only after construction.
    Async safety:   ✅ Synchronous.

    Args:
        model:          SQLAlchemy declarative model class used for column lookup.
        allowed_fields: Optional whitelist of column names.  When non-empty,
                        queries referencing fields outside this set raise
                        ``ValueError``.

    Edge cases:
        - ``allowed_fields`` empty set → all fields are permitted.
        - Dotted field paths (``"profile.city"``) → ``OperationNotSupported``.
        - Unknown field name → ``FieldNotFound``.
        - Unknown operation → ``ValueError``.
    """

    def __init__(
        self,
        model: type[DeclarativeBase],
        allowed_fields: set[str] | None = None,
    ) -> None:
        """
        Initialise the compiler.

        Args:
            model:          Mapped SQLAlchemy model class.
            allowed_fields: Optional column-name whitelist.  ``None`` or empty
                            set means all fields are allowed.
        """
        self.model = model
        # Use a copy to prevent external mutation of the set we depend on
        self.allowed_fields: set[str] = set(allowed_fields) if allowed_fields else set()

    # ── Visitor implementations ────────────────────────────────────────────────

    def _visit_comparison(
        self,
        node: ComparisonNode,
        args: object = None,
        **kwargs: object,
    ) -> Union[ColumnElement[bool], BinaryExpression[bool]]:
        """
        Compile a ``ComparisonNode`` to a SQLAlchemy boolean expression.

        Args:
            node: The comparison node containing field, operation, and value.
            args: Unused.

        Returns:
            A ``ColumnElement[bool]`` ready for use in ``.where()``.

        Raises:
            ValueError:             ``node.field`` is ``None``, or the field is
                                    not in ``allowed_fields`` (when set).
            OperationNotSupported:  Dotted relationship path in ``node.field``.
            FieldNotFound:          Column does not exist on the model.
            ValueError:             Unsupported ``Operation`` value.
        """
        if node.field is None:
            raise ValueError("ComparisonNode.field must not be None")

        if self.allowed_fields and node.field not in self.allowed_fields:
            raise ValueError(
                f"Field {node.field!r} is not in the allowed fields whitelist. "
                f"Allowed: {sorted(self.allowed_fields)}"
            )

        col = self._resolve_column(node.field)
        op, val = node.op, node.value

        if op == Operation.EQUAL:
            return col == val
        if op == Operation.NOT_EQUAL:
            return col != val
        if op == Operation.GREATER_THAN:
            return col > val
        if op == Operation.LESS_THAN:
            return col < val
        if op == Operation.GREATER_EQUAL:
            return col >= val
        if op == Operation.LESS_EQUAL:
            return col <= val
        if op == Operation.LIKE:
            return col.like(val)
        if op == Operation.IN:
            return col.in_(val)  # pyright: ignore[reportArgumentType]
        if op == Operation.IS_NULL:
            return col.is_(None)
        if op == Operation.IS_NOT_NULL:
            return col.isnot(None)

        raise ValueError(
            f"Unsupported operation {op!r}. "
            f"Supported: {[o.value for o in Operation]}"
        )

    # ── BinaryWalkingVisitor combine hooks ────────────────────────────────────

    def _combine_and(
        self, left: ColumnElement[bool], right: ColumnElement[bool]
    ) -> ColumnElement[bool]:
        """Combine two compiled sub-expressions with SQLAlchemy ``and_``."""
        return and_(left, right)

    def _combine_or(
        self, left: ColumnElement[bool], right: ColumnElement[bool]
    ) -> ColumnElement[bool]:
        """Combine two compiled sub-expressions with SQLAlchemy ``or_``."""
        return or_(left, right)

    def _combine_not(self, inner: ColumnElement[bool]) -> ColumnElement[bool]:
        """Negate a compiled sub-expression with SQLAlchemy ``not_``."""
        return not_(inner)

    # ── allowed_fields helpers ─────────────────────────────────────────────────

    def get_allowed_fields(self) -> set[str]:
        """Return a copy of the allowed-fields whitelist."""
        return self.allowed_fields.copy()

    def set_allowed_fields(self, allowed_fields: set[str]) -> None:
        """Replace the entire allowed-fields whitelist."""
        self.allowed_fields = set(allowed_fields)

    def add_to_allowed_fields(self, value: str) -> None:
        """Add a single field name to the whitelist."""
        self.allowed_fields.add(value)

    def is_allowed_fields_empty(self) -> bool:
        """Return ``True`` when no whitelist is configured (all fields allowed)."""
        return len(self.allowed_fields) == 0

    # ── Column resolution ──────────────────────────────────────────────────────

    def _resolve_column(self, field_path: str) -> MappedColumn:
        """
        Resolve a top-level column attribute on the mapped model class.

        Nested relationship paths (``"profile.city"``) are explicitly blocked —
        they would require join logic that belongs outside the compiler.

        Args:
            field_path: A simple column name (no dots).

        Returns:
            The mapped column attribute.

        Raises:
            OperationNotSupported: ``field_path`` contains a dot.
            FieldNotFound:         The attribute does not exist on ``self.model``.
        """
        if "." in field_path:
            raise OperationNotSupported(
                f"Nested relationship traversal is not supported ({field_path!r}). "
                "Use a JOIN-aware query builder instead."
            )
        try:
            return getattr(self.model, field_path)
        except AttributeError as exc:
            raise FieldNotFound(
                field=field_path,
                table=self.model.__tablename__,
            ) from exc
