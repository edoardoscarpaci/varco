"""
fastrest_beanie.query.compiler
================================
Translates ``fastrest_core`` AST nodes into MongoDB query filter documents.

The output is a plain Python ``dict`` compatible with Beanie's
``Document.find({...})`` API and the underlying Motor / PyMongo filter
argument.

Operator mapping
----------------
+-------------------+----------------------------------+
| AST Operation     | MongoDB equivalent               |
+===================+==================================+
| EQUAL             | ``{field: value}``               |
+-------------------+----------------------------------+
| NOT_EQUAL         | ``{field: {"$ne": value}}``      |
+-------------------+----------------------------------+
| GREATER_THAN      | ``{field: {"$gt": value}}``      |
+-------------------+----------------------------------+
| LESS_THAN         | ``{field: {"$lt": value}}``      |
+-------------------+----------------------------------+
| GREATER_EQUAL     | ``{field: {"$gte": value}}``     |
+-------------------+----------------------------------+
| LESS_EQUAL        | ``{field: {"$lte": value}}``     |
+-------------------+----------------------------------+
| LIKE              | ``{field: {"$regex": pattern}}`` |
|                   | SQL ``%`` → regex ``.*``         |
+-------------------+----------------------------------+
| IN                | ``{field: {"$in": [...]}}``      |
+-------------------+----------------------------------+
| IS_NULL           | ``{field: {"$in": [None]}}``     |
|                   | Catches explicit null AND        |
|                   | missing field documents.         |
+-------------------+----------------------------------+
| IS_NOT_NULL       | ``{field: {"$ne": None,          |
|                   |            "$exists": True}}``   |
+-------------------+----------------------------------+
| AND               | ``{"$and": [left, right]}``      |
+-------------------+----------------------------------+
| OR                | ``{"$or":  [left, right]}``      |
+-------------------+----------------------------------+
| NOT               | ``{"$nor": [child]}``            |
|                   | ``$nor`` negates any expression, |
|                   | unlike ``$not`` (single cond.)   |
+-------------------+----------------------------------+

Thread safety:  ✅ Stateless after construction.
Async safety:   ✅ Synchronous; safe to call from async contexts.
"""

from __future__ import annotations

import re
from typing import Any

from fastrest_core.exception.query import OperationNotSupported
from fastrest_core.query.type import (
    AndNode,
    ComparisonNode,
    NotNode,
    Operation,
    OrNode,
)
from fastrest_core.query.visitor.ast_visitor import ASTVisitor


def _sql_like_to_regex(pattern: str) -> str:
    """
    Convert a SQL LIKE pattern to a Python/MongoDB regex string.

    Rules:
    - ``%``  → ``.*``  (match any sequence of characters)
    - ``_``  → ``.``   (match exactly one character)
    - All other regex metacharacters are escaped.

    Args:
        pattern: SQL LIKE pattern string.

    Returns:
        Equivalent regex pattern string.

    Example::

        _sql_like_to_regex("Alice%")  → "Alice.*"
        _sql_like_to_regex("_lice")   → ".lice"
        _sql_like_to_regex("100%")    → "100.*"
    """
    # Process character-by-character: % and _ are SQL wildcards; everything
    # else is a literal that must be regex-escaped.
    # NOTE: re.escape() (Python ≥3.7) does NOT escape % or _ because they
    # are not regex metacharacters — a prior approach using re.escape then
    # str.replace was therefore broken.
    result: list[str] = []
    for ch in pattern:
        if ch == "%":
            result.append(".*")  # SQL % = any sequence of chars
        elif ch == "_":
            result.append(".")  # SQL _ = exactly one char
        else:
            result.append(re.escape(ch))  # escape regex metacharacters
    return "".join(result)


class BeanieQueryCompiler(ASTVisitor):
    """
    AST visitor that compiles query nodes into MongoDB filter documents.

    Each visit method returns a plain ``dict`` suitable for passing directly
    to ``Document.find({...})``.

    DESIGN: visitor over inline dict construction in the repository
      ✅ Compiler is isolated and independently testable.
      ✅ Consistent with the SA compiler pattern — same interface,
         different output type.
      ❌ Two allocations per node (dict + recursive visit) — fine for
         typical query depths (<20 nodes).

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ Synchronous.

    Edge cases:
        - Dotted field paths (``"profile.city"``) ARE supported — MongoDB
          handles nested document queries natively via dot notation.
        - ``allowed_fields`` is not enforced here; validate at the HTTP layer.
        - Unknown operation → ``OperationNotSupported``.
    """

    # ── Visitor implementations ────────────────────────────────────────────────

    def _visit_comparison(
        self,
        node: ComparisonNode,
        args: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Compile a comparison node to a MongoDB filter fragment.

        Args:
            node: Comparison node with field, op, and optional value.
            args: Unused.

        Returns:
            MongoDB filter dict, e.g. ``{"age": {"$gt": 18}}``.

        Raises:
            OperationNotSupported: Unknown operation encountered.
        """
        field, op, val = node.field, node.op, node.value

        if op == Operation.EQUAL:
            # Simple equality — most common case, no operator wrapper needed
            return {field: val}
        if op == Operation.NOT_EQUAL:
            return {field: {"$ne": val}}
        if op == Operation.GREATER_THAN:
            return {field: {"$gt": val}}
        if op == Operation.LESS_THAN:
            return {field: {"$lt": val}}
        if op == Operation.GREATER_EQUAL:
            return {field: {"$gte": val}}
        if op == Operation.LESS_EQUAL:
            return {field: {"$lte": val}}
        if op == Operation.LIKE:
            # Convert SQL LIKE wildcards to regex — case-sensitive by default.
            # Use $options: "i" in the filter if you need case-insensitive.
            regex_pattern = _sql_like_to_regex(str(val))
            return {field: {"$regex": regex_pattern}}
        if op == Operation.IN:
            return {field: {"$in": list(val)}}  # type: ignore[arg-type]
        if op == Operation.IS_NULL:
            # DESIGN: {"$in": [None]} catches BOTH explicit null values AND
            # documents where the field is missing entirely.
            # Using {"$eq": None} only catches explicit nulls in newer MongoDB.
            return {field: {"$in": [None]}}
        if op == Operation.IS_NOT_NULL:
            # Both conditions together: field exists AND is not null
            return {field: {"$ne": None, "$exists": True}}

        raise OperationNotSupported(
            f"Operation {op!r} is not supported by the Beanie/MongoDB compiler. "
            f"Supported: {[o.value for o in Operation]}"
        )

    def _visit_and(
        self,
        node: AndNode,
        args: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Compile AND to ``{"$and": [left, right]}``.

        DESIGN: always use ``$and`` (not merging dicts)
          ✅ Handles the case where ``left`` and ``right`` use the same field —
             merging dicts would silently drop one condition.
          ❌ Slightly more verbose output than merging for disjoint fields.

        Args:
            node: AND node.
            args: Unused.

        Returns:
            ``{"$and": [compiled_left, compiled_right]}``
        """
        return {"$and": [self.visit(node.left), self.visit(node.right)]}

    def _visit_or(
        self,
        node: OrNode,
        args: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Compile OR to ``{"$or": [left, right]}``.

        Args:
            node: OR node.
            args: Unused.

        Returns:
            ``{"$or": [compiled_left, compiled_right]}``
        """
        return {"$or": [self.visit(node.left), self.visit(node.right)]}

    def _visit_not(
        self,
        node: NotNode,
        args: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Compile NOT to ``{"$nor": [child]}``.

        DESIGN: ``$nor`` over ``$not``
          ``$not`` only applies to a single condition on a field
          (``{field: {$not: {$gt: 5}}}``).
          ``$nor`` negates an entire expression document, matching the
          semantics of ``NotNode`` which wraps arbitrary sub-trees.

        Args:
            node: NOT node.
            args: Unused.

        Returns:
            ``{"$nor": [compiled_child]}``
        """
        return {"$nor": [self.visit(node.child)]}
