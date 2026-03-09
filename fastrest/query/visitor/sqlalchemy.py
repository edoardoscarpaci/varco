"""Translate AST query nodes into SQLAlchemy filter expressions."""

from fastrest.query.type import ComparisonNode, AndNode, OrNode, NotNode
from fastrest.query.visitor.ast_visitor import ASTVisitor
from typing import Union
from sqlalchemy.orm import DeclarativeBase, MappedColumn
from sqlalchemy import BinaryExpression, ColumnElement, and_, or_, not_
from fastrest.query.type import Operation
from fastrest.exception.query import OperationNotSupported
from fastrest.exception.repository import FieldNotFound


class SQLAlchemyQueryCompiler(ASTVisitor):
    """AST visitor that compiles comparison and boolean nodes into SQLAlchemy
    expressions for a given mapped `model`.

    The `allowed_fields` set can be used to restrict which model fields are
    permitted in generated filters.
    """

    def __init__(
        self, model: DeclarativeBase, allowed_fields: set[str] = set(), *args, **kwargs
    ):
        """Initialize with a SQLAlchemy declarative `model` and optional allowed fields."""
        self.allowed_fields = allowed_fields or set()
        self.model = model

    def _visit_comparison(
        self, node: ComparisonNode, args, **kwargs
    ) -> Union[ColumnElement[bool], BinaryExpression[bool]]:
        """Compile a `ComparisonNode` to a SQLAlchemy boolean expression."""
        if node.field is None:
            raise ValueError("Comparison node must have a field")

        if self.allowed_fields and node.field not in self.allowed_fields:
            raise ValueError(f"Field {node.field} is not allowed")

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
        raise ValueError(f"Unsupported operation: {op}")

    def _visit_and(self, node: AndNode, args, **kwargs):
        """Compile an AND node by composing child expressions with `and_`."""
        return and_(self.visit(node.left), self.visit(node.right))

    def _visit_or(self, node: OrNode, args, **kwargs):
        """Compile an OR node by composing child expressions with `or_`."""
        return or_(self.visit(node.left), self.visit(node.right))

    def _visit_not(self, node: NotNode, args, **kwargs):
        """Compile a NOT node by negating the child expression."""
        return not_(self.visit(node.child))

    # Getters and Setters for allowed_fields
    def get_allowed_fields(self) -> set[str]:
        """Return a copy of the allowed fields set."""
        return self.allowed_fields.copy()

    def set_allowed_fields(self, allowed_fields: set[str]) -> None:
        """Replace the allowed fields set used by the compiler."""
        self.allowed_fields = allowed_fields

    def add_to_allowed_fields(self, value: str):
        """Add a single field name to the allowed fields set."""
        self.allowed_fields.add(value)

    def is_allowed_fields_empty(self) -> bool:
        """Return True when no allowed fields have been configured."""
        return len(self.allowed_fields) == 0

    # Helper Functions
    def _resolve_column(self, field_path: str) -> MappedColumn:
        """
        Resolve a top-level column attribute on the mapped model class.

        Nested relationship traversal is not supported by this helper. If a
        dotted field path is passed (e.g. ``profile.city``) a ``ValueError``
        will be raised.
        """
        if "." in field_path:
            raise OperationNotSupported(f"Nested relationship fields({field_path})")

        try:
            return getattr(self.model, field_path)
        except AttributeError as exc:
            raise FieldNotFound(
                field=field_path, table=self.model.__tablename__
            ) from exc
