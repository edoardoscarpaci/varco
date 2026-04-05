"""
varco_beanie.query.compiler
================================
Translates ``varco_core`` AST nodes into MongoDB query filter documents.

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
import sys
from typing import Any

from providify import Singleton

from varco_core.exception.query import OperationNotSupported
from varco_core.query.type import (
    ComparisonNode,
    Operation,
)
from varco_core.query.visitor.walking import BinaryWalkingVisitor

# ── Security: safe path segment regex ─────────────────────────────────────────

# DESIGN: identical pattern to the SA compiler — kept local to avoid a shared
# private module that would add an import dependency between the query visitor
# package and varco_beanie.  Two identical one-liners are cheaper than an extra
# module boundary.
#   ✅ Each backend is self-contained — varco_beanie stays independently testable.
#   ❌ Tiny duplication (one regex) — acceptable; the logic is trivially auditable.
_SAFE_SEGMENT_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


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


@Singleton(priority=-sys.maxsize, qualifier="beanie")
class BeanieQueryCompiler(BinaryWalkingVisitor):
    """
    AST visitor that compiles query nodes into MongoDB filter documents.

    Each visit method returns a plain ``dict`` suitable for passing directly
    to ``Document.find({...})``.

    Dotted paths (e.g. ``"profile.city"``) are supported natively by MongoDB
    via its dot notation — no JOIN logic is required.  Security is enforced
    via ``allowed_fields`` whitelisting and ``max_traversal_depth`` limiting.

    DESIGN: visitor over inline dict construction in the repository
      ✅ Compiler is isolated and independently testable.
      ✅ Consistent with the SA compiler pattern — same interface,
         different output type.
      ✅ ``allowed_fields`` and ``max_traversal_depth`` harden the compiler
         against user-controlled field injection.
      ❌ Two allocations per node (dict + recursive visit) — fine for
         typical query depths (<20 nodes).

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ Synchronous.

    Args:
        allowed_fields:      Optional whitelist of field paths (flat or dotted).
                             When non-empty, queries referencing paths outside
                             this set raise ``ValueError``.
        max_traversal_depth: Maximum number of dot-separated hops in a field
                             path (default 10).  MongoDB can handle deep nesting
                             natively, but unbounded depth from user input is a
                             DoS vector.

    Edge cases:
        - Dotted field paths (``"profile.city"``) ARE supported natively.
        - ``allowed_fields`` empty set → all fields are permitted.
        - Depth > ``max_traversal_depth`` → ``OperationNotSupported``.
        - Segment failing ``_SAFE_SEGMENT_RE`` → ``ValueError``.
        - Unknown operation → ``OperationNotSupported``.
    """

    def __init__(
        self,
        *,
        allowed_fields: set[str] | None = None,
        max_traversal_depth: int = 10,
    ) -> None:
        """
        Initialise the compiler.

        Args:
            allowed_fields:      Optional field-path whitelist.  ``None`` or
                                 empty set means all fields are allowed.
            max_traversal_depth: Maximum dot-notation depth.  Default 10 matches
                                 MongoDB's own 100-level nesting limit but applies
                                 much earlier to prevent abusive query strings.
        """
        # Use a copy to prevent external mutation of the set we depend on
        self.allowed_fields: set[str] = set(allowed_fields) if allowed_fields else set()
        self.max_traversal_depth = max_traversal_depth

    # ── Visitor implementations ────────────────────────────────────────────────

    def _visit_comparison(
        self,
        node: ComparisonNode,
        args: Any = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """
        Compile a comparison node to a MongoDB filter fragment.

        Validates ``node.field`` against the allowed-fields whitelist,
        depth limit, and segment safety regex before building the filter.

        Args:
            node: Comparison node with field, op, and optional value.
            args: Unused.

        Returns:
            MongoDB filter dict, e.g. ``{"age": {"$gt": 18}}``.

        Raises:
            ValueError:            Field not in ``allowed_fields`` whitelist,
                                   or a segment contains unsafe characters.
            OperationNotSupported: Traversal depth exceeds
                                   ``max_traversal_depth``, or unknown operation.
        """
        field, op, val = node.field, node.op, node.value

        # ── Security validation ────────────────────────────────────────────────

        # Guard 1: allowed_fields whitelist (full dotted path must be listed)
        if self.allowed_fields and field not in self.allowed_fields:
            raise ValueError(
                f"Field {field!r} is not in the allowed_fields whitelist. "
                f"Allowed: {sorted(self.allowed_fields)}"
            )

        if "." in field:
            parts = field.split(".")
            depth = len(parts) - 1

            # Guard 2: limit traversal depth to prevent DoS via deeply nested paths
            if depth > self.max_traversal_depth:
                raise OperationNotSupported(
                    f"Dotted path depth {depth} for {field!r} exceeds "
                    f"max_traversal_depth={self.max_traversal_depth}. "
                    "Reduce nesting depth or increase max_traversal_depth."
                )

            # Guard 3: validate each segment — prevents __dunder__ and special
            # character injection into MongoDB field names
            for segment in parts:
                if not _SAFE_SEGMENT_RE.match(segment):
                    raise ValueError(
                        f"Field path segment {segment!r} in {field!r} contains "
                        "unsafe characters. Only [a-zA-Z_][a-zA-Z0-9_]* "
                        "identifiers are accepted."
                    )

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

    # ── BinaryWalkingVisitor combine hooks ────────────────────────────────────

    def _combine_and(
        self, left: dict[str, Any], right: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Combine two compiled sub-expressions with MongoDB ``$and``.

        DESIGN: always use ``$and`` (not merging dicts)
          ✅ Handles the case where ``left`` and ``right`` use the same field —
             merging dicts would silently drop one condition.
          ❌ Slightly more verbose output than merging for disjoint fields.

        Args:
            left:  Compiled filter for the AND's left child.
            right: Compiled filter for the AND's right child.

        Returns:
            ``{"$and": [left, right]}``
        """
        return {"$and": [left, right]}

    def _combine_or(
        self, left: dict[str, Any], right: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Combine two compiled sub-expressions with MongoDB ``$or``.

        Args:
            left:  Compiled filter for the OR's left child.
            right: Compiled filter for the OR's right child.

        Returns:
            ``{"$or": [left, right]}``
        """
        return {"$or": [left, right]}

    def _combine_not(self, inner: dict[str, Any]) -> dict[str, Any]:
        """
        Negate a compiled sub-expression with MongoDB ``$nor``.

        DESIGN: ``$nor`` over ``$not``
          ``$not`` only applies to a single condition on a field
          (``{field: {$not: {$gt: 5}}}``).
          ``$nor`` negates an entire expression document, matching the
          semantics of ``NotNode`` which wraps arbitrary sub-trees.

        Args:
            inner: Compiled filter for the NOT's child.

        Returns:
            ``{"$nor": [inner]}``
        """
        return {"$nor": [inner]}
