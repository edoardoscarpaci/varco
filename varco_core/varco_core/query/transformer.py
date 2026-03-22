"""
varco_core.query.transformer
================================
Lark ``Transformer`` that converts a parsed query grammar tree into the
``varco_core`` AST node types.

This is the bridge between the raw Lark parse tree (produced from the
``grammar.lark`` grammar) and the typed AST used by visitors and applicators.

Thread safety:  ✅ Stateless — a single instance can be shared.
Async safety:   ✅ Synchronous; safe to call from async contexts.
"""

from __future__ import annotations

from lark import Transformer

from varco_core.exception.query import OperationNotFound
from varco_core.query.type import (
    AndNode,
    ComparisonNode,
    NotNode,
    Operation,
    OrNode,
)


class QueryTransformer(Transformer):
    """
    Lark ``Transformer`` that rewrites a grammar parse tree into typed AST nodes.

    Each method corresponds to a grammar rule in ``grammar.lark``.  Lark calls
    these methods bottom-up as it reduces the parse tree.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ Synchronous.

    Edge cases:
        - Unknown comparison operator → raises ``OperationNotFound`` immediately
          (fail-fast at parse time rather than silently passing through).
    """

    def string(self, s: list) -> str:
        """
        Strip surrounding quotes from a grammar ``ESCAPED_STRING`` terminal.

        Args:
            s: Single-element list containing the raw quoted string token.

        Returns:
            Unquoted string value.
        """
        # s[0] is the Token — strip the outer quote characters
        return s[0][1:-1]

    def number(self, n: list) -> float:
        """
        Convert a ``SIGNED_NUMBER`` terminal to a Python ``float``.

        Args:
            n: Single-element list containing the number token.

        Returns:
            Float value.
        """
        return float(n[0])

    def field(self, f: list) -> str:
        """
        Extract a ``CNAME`` field name as a plain string.

        Args:
            f: Single-element list containing the field name token.

        Returns:
            Field name string.
        """
        return str(f[0])

    def in_list(self, items: list) -> ComparisonNode:
        """
        Build an ``IN`` comparison node from a field and value list.

        Grammar rule: ``field "IN" "(" [value ("," value)*] ")"``

        Args:
            items: First element is the field name; remaining elements are values.

        Returns:
            ``ComparisonNode`` with ``op=Operation.IN``.
        """
        field_name = items[0]
        values = items[1:]
        return ComparisonNode(field=field_name, op=Operation.IN, value=list(values))

    def is_null(self, items: list) -> ComparisonNode:
        """
        Build an ``IS NULL`` comparison node.

        Args:
            items: Single-element list with the field name.

        Returns:
            ``ComparisonNode`` with ``op=Operation.IS_NULL`` and no value.
        """
        return ComparisonNode(field=items[0], op=Operation.IS_NULL)

    def is_not_null(self, items: list) -> ComparisonNode:
        """
        Build an ``IS NOT NULL`` comparison node.

        Args:
            items: Single-element list with the field name.

        Returns:
            ``ComparisonNode`` with ``op=Operation.IS_NOT_NULL`` and no value.
        """
        return ComparisonNode(field=items[0], op=Operation.IS_NOT_NULL)

    def comparison(self, items: list) -> ComparisonNode:
        """
        Build a standard binary comparison node.

        Grammar rule: ``field OP value``

        Args:
            items: ``[field_name, operator_str, value]``

        Returns:
            ``ComparisonNode`` with the resolved ``Operation``.

        Raises:
            OperationNotFound: The operator string is not a recognised ``Operation``.
        """
        field_name, op_str, value = items
        if op_str not in Operation._value2member_map_:
            raise OperationNotFound(op=op_str)
        return ComparisonNode(field=field_name, op=Operation(op_str), value=value)

    def and_expr(self, items: list) -> AndNode:
        """
        Build an ``AND`` binary node from two child expressions.

        Args:
            items: ``[left_node, right_node]``

        Returns:
            ``AndNode``.
        """
        return AndNode(left=items[0], right=items[1])

    def or_expr(self, items: list) -> OrNode:
        """
        Build an ``OR`` binary node from two child expressions.

        Args:
            items: ``[left_node, right_node]``

        Returns:
            ``OrNode``.
        """
        return OrNode(left=items[0], right=items[1])

    def not_expr(self, items: list) -> NotNode:
        """
        Build a ``NOT`` unary node from a single child expression.

        Args:
            items: ``[child_node]``

        Returns:
            ``NotNode``.
        """
        return NotNode(child=items[0])

    def group(self, items: list) -> object:
        """
        Unwrap a parenthesised expression group.

        Args:
            items: ``[inner_node]``

        Returns:
            The inner AST node unchanged.
        """
        return items[0]
