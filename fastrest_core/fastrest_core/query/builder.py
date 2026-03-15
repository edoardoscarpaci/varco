"""
fastrest_core.query.builder
=============================
Fluent immutable ``QueryBuilder`` for constructing AST query trees.

``QueryBuilder`` is a frozen dataclass — every mutation method returns a new
instance.  This makes it safe to store, branch, and re-use partial queries.

Thread safety:  ✅ Immutable — all methods return new instances.
Async safety:   ✅ No I/O; pure value objects.

Example::

    from fastrest_core.query.builder import QueryBuilder

    ast = (
        QueryBuilder()
        .eq("status", "active")
        .gt("age", 18)
        .build()
    )
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from fastrest_core.query.type import (
    AndNode,
    ComparisonNode,
    NodeType,
    NotNode,
    Operation,
    OrNode,
    TransformerNode,
)


@dataclass(frozen=True)
class QueryBuilder:
    """
    Immutable fluent builder for AST query trees.

    Each method that adds a condition returns a **new** ``QueryBuilder``
    whose ``node`` is the combined expression.  Conditions added via
    ``where()``, ``eq()``, etc. are implicitly AND-ed with the existing node.

    DESIGN: frozen dataclass over mutable builder
      ✅ Builders can be stored and re-used as query templates.
      ✅ No accidental shared-state bugs in async handlers.
      ❌ Each operation allocates a new dataclass — negligible for typical queries.

    Thread safety:  ✅ Immutable.
    Async safety:   ✅ Immutable.

    Attributes:
        node: Current root AST node, or ``None`` when the builder is empty.

    Edge cases:
        - Building from an empty builder (``node is None``) → ``build()`` returns ``None``.
        - ``and_(other)`` where ``other.node is None`` → returns ``self`` unchanged.
        - ``or_(other)`` where ``other.node is None`` → returns ``self`` unchanged.
        - ``not_()`` on an empty builder → returns ``self`` unchanged.
    """

    node: TransformerNode | None = None

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _combine(self, new_node: TransformerNode, op: NodeType) -> QueryBuilder:
        """
        Combine the current node with ``new_node`` using ``op``.

        When the builder is empty (``self.node is None``) the new node
        becomes the root directly — no wrapping needed.

        Args:
            new_node: Node to combine with the current root.
            op:       ``NodeType.AND`` or ``NodeType.OR``.

        Returns:
            New ``QueryBuilder`` with the combined node as root.

        Raises:
            ValueError: ``op`` is not AND or OR.
        """
        if self.node is None:
            return QueryBuilder(new_node)

        if op == NodeType.AND:
            return QueryBuilder(AndNode(self.node, new_node))
        if op == NodeType.OR:
            return QueryBuilder(OrNode(self.node, new_node))

        raise ValueError(
            f"Unsupported combine operation {op!r}. Must be NodeType.AND or NodeType.OR."
        )

    # ── Raw WHERE ──────────────────────────────────────────────────────────────

    def where(self, field: str, op: Operation, value: Any = None) -> QueryBuilder:
        """
        Add a comparison condition (implicitly AND-ed with the existing node).

        Args:
            field: Column / field name.
            op:    Comparison ``Operation``.
            value: Comparison target.  Required for all ops except IS_NULL /
                   IS_NOT_NULL.  Must be a list for IN.

        Returns:
            New builder with the condition AND-ed in.

        Example::

            qb = QueryBuilder().where("age", Operation.GREATER_THAN, 18)
        """
        comp = ComparisonNode(field=field, op=op, value=value)
        return self._combine(comp, NodeType.AND)

    # ── Logical combinators ────────────────────────────────────────────────────

    def and_(self, other: QueryBuilder) -> QueryBuilder:
        """
        AND the current node with ``other.node``.

        Args:
            other: Another ``QueryBuilder`` whose root to AND-in.

        Returns:
            New builder.  Returns ``self`` unchanged when ``other.node is None``.
        """
        if other.node is None:
            return self
        return self._combine(other.node, NodeType.AND)

    def or_(self, other: QueryBuilder) -> QueryBuilder:
        """
        OR the current node with ``other.node``.

        Args:
            other: Another ``QueryBuilder`` whose root to OR-in.

        Returns:
            New builder.  Returns ``self`` unchanged when ``other.node is None``.
        """
        if other.node is None:
            return self
        return self._combine(other.node, NodeType.OR)

    def not_(self) -> QueryBuilder:
        """
        Negate the current node.

        Returns:
            New builder wrapping the current node in a ``NotNode``.
            Returns ``self`` unchanged when the builder is empty.
        """
        if self.node is None:
            return self
        return QueryBuilder(NotNode(self.node))

    # ── Comparison shorthands ──────────────────────────────────────────────────

    def eq(self, field: str, value: Any) -> QueryBuilder:
        """Add ``field = value`` condition.

        Args:
            field: Column name.
            value: Equality target.

        Returns:
            New builder with condition AND-ed in.
        """
        return self.where(field, Operation.EQUAL, value)

    def ne(self, field: str, value: Any) -> QueryBuilder:
        """Add ``field != value`` condition."""
        return self.where(field, Operation.NOT_EQUAL, value)

    def gt(self, field: str, value: Any) -> QueryBuilder:
        """Add ``field > value`` condition."""
        return self.where(field, Operation.GREATER_THAN, value)

    def gte(self, field: str, value: Any) -> QueryBuilder:
        """Add ``field >= value`` condition."""
        return self.where(field, Operation.GREATER_EQUAL, value)

    def lt(self, field: str, value: Any) -> QueryBuilder:
        """Add ``field < value`` condition."""
        return self.where(field, Operation.LESS_THAN, value)

    def lte(self, field: str, value: Any) -> QueryBuilder:
        """Add ``field <= value`` condition."""
        return self.where(field, Operation.LESS_EQUAL, value)

    def like(self, field: str, value: str) -> QueryBuilder:
        """Add ``field LIKE value`` condition (SQL LIKE semantics).

        Args:
            field: Column name.
            value: Pattern string (use ``%`` as wildcard).

        Returns:
            New builder with condition AND-ed in.
        """
        return self.where(field, Operation.LIKE, value)

    def in_(self, field: str, values: list[Any]) -> QueryBuilder:
        """Add ``field IN (values)`` condition.

        Args:
            field:  Column name.
            values: List of acceptable values.

        Returns:
            New builder with condition AND-ed in.
        """
        return self.where(field, Operation.IN, values)

    def is_null(self, field: str) -> QueryBuilder:
        """Add ``field IS NULL`` condition."""
        return self.where(field, Operation.IS_NULL)

    def is_not_null(self, field: str) -> QueryBuilder:
        """Add ``field IS NOT NULL`` condition."""
        return self.where(field, Operation.IS_NOT_NULL)

    # ── Terminal ───────────────────────────────────────────────────────────────

    def build(self) -> TransformerNode | None:
        """
        Return the root AST node.

        Returns:
            The root ``TransformerNode``, or ``None`` when the builder is empty.

        Example::

            ast = QueryBuilder().eq("active", True).build()
            # ast is ComparisonNode(field="active", op=EQUAL, value=True)
        """
        return self.node

    # ── Static factory methods ─────────────────────────────────────────────────

    @staticmethod
    def field(field: str, op: Operation, value: Any = None) -> QueryBuilder:
        """
        Create a single-comparison ``QueryBuilder`` without chaining.

        Args:
            field: Column name.
            op:    Comparison operation.
            value: Comparison target.

        Returns:
            New ``QueryBuilder`` with one comparison node as root.

        Example::

            qb = QueryBuilder.field("status", Operation.EQUAL, "active")
        """
        return QueryBuilder(ComparisonNode(field, op, value))

    @staticmethod
    def eq_(field: str, value: Any) -> QueryBuilder:
        """
        Shorthand factory for a single equality comparison.

        Args:
            field: Column name.
            value: Equality target.

        Returns:
            New ``QueryBuilder`` with ``field = value`` as root.

        Example::

            qb = QueryBuilder.eq_("status", "active")
        """
        return QueryBuilder(ComparisonNode(field, Operation.EQUAL, value))
