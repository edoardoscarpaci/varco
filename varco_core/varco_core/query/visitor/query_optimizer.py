"""
varco_core.query.visitor.query_optimizer
=============================================
AST optimisation pass — runs before compilation to simplify the tree.

Current optimisations
---------------------
1. **Double-negation elimination** — ``NOT(NOT(x))`` → ``x``
2. **AND associativity flattening** — ``(a AND b) AND c`` → ``a AND (b AND c)``
   (right-leaning normal form preferred by SA's ``and_()``).

Thread safety:  ✅ Stateless — safe for concurrent use.
Async safety:   ✅ Synchronous; safe to call from async contexts.
"""

from __future__ import annotations

from typing import Any

from varco_core.query.type import AndNode, ComparisonNode, NotNode, OrNode
from varco_core.query.visitor.ast_visitor import ASTVisitor


class ASTQueryOptimizer(ASTVisitor):
    """
    AST visitor that rewrites a query tree into a simplified equivalent.

    Usage::

        optimized = ASTQueryOptimizer().visit(raw_ast)

    DESIGN: separate optimiser pass over combined compile-and-optimise
      ✅ Compiler stays pure — no optimisation logic leaks into it.
      ✅ Optimisations can be tested independently on raw AST nodes.
      ❌ Two tree walks instead of one — negligible cost for typical query depths.

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ Synchronous.

    Edge cases:
        - Comparison nodes are returned unchanged — no value inspection.
        - OR nodes recurse but are not flattened (left-assoc is fine for OR).
        - Double-NOT is eliminated recursively before re-wrapping.
    """

    def _visit_comparison(
        self, node: ComparisonNode, args: Any = None, **kwargs: Any
    ) -> ComparisonNode:
        """
        Return the comparison node unchanged — leaf nodes need no optimisation.

        Args:
            node: The comparison node.
            args: Unused.

        Returns:
            The same ``node`` (no copy).
        """
        return node

    def _visit_and(self, node: AndNode, args: Any = None, **kwargs: Any) -> AndNode:
        """
        Flatten left-recursive AND chains into right-recursive form.

        Before: ``AndNode(AndNode(a, b), c)``
        After:  ``AndNode(a, AndNode(b, c))``

        This matches SQLAlchemy's preferred operand order and avoids deeply
        nested left-biased trees from ``QueryBuilder`` chaining.

        Args:
            node: The AND node to optimise.
            args: Unused.

        Returns:
            Optimised ``AndNode``.
        """
        left = self.visit(node.left)
        right = self.visit(node.right)

        # Flatten: (a AND b) AND c → a AND (b AND c)
        if isinstance(left, AndNode):
            return AndNode(left.left, AndNode(left.right, right))
        return AndNode(left, right)

    def _visit_or(self, node: OrNode, args: Any = None, **kwargs: Any) -> OrNode:
        """
        Recurse into OR children without restructuring.

        Args:
            node: The OR node to recurse into.
            args: Unused.

        Returns:
            ``OrNode`` with optimised children.
        """
        return OrNode(self.visit(node.left), self.visit(node.right))

    def _visit_not(self, node: NotNode, args: Any = None, **kwargs: Any) -> Any:
        """
        Eliminate double negation: ``NOT(NOT(x))`` → ``x``.

        Args:
            node: The NOT node to optimise.
            args: Unused.

        Returns:
            The inner child when double-negation is detected; otherwise a
            new ``NotNode`` wrapping the optimised child.
        """
        child = self.visit(node.child)

        # NOT(NOT(x)) → x  — double negation has no semantic effect
        if isinstance(child, NotNode):
            return child.child

        return NotNode(child)
