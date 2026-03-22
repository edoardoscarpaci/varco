"""
fastrest_core.query.visitor.walking
=====================================
Template-method base for AST visitors that only need to combine children.

Most visitors (SQL compiler, Beanie compiler, type coercion) share the same
walk logic for AND / OR / NOT: visit left child, visit right child, combine
results.  Only the combination step differs across backends.

``BinaryWalkingVisitor`` captures this pattern: it provides concrete
``_visit_and``, ``_visit_or``, and ``_visit_not`` implementations that walk
the children and delegate combination to three abstract ``_combine_*`` hooks.
Subclasses implement only ``_visit_comparison`` (backend-specific) and the
three ``_combine_*`` hooks (one-liner each).

DESIGN: Template Method over per-subclass duplication
  ✅ Removes identical walk code repeated in SA compiler, Beanie compiler,
     and type-coercion visitor — DRY without sacrificing extensibility.
  ✅ Subclasses keep full control over how results are combined.
  ✅ ``ASTQueryOptimizer`` is deliberately excluded — its AND walk has
     non-trivial flattening logic that does not fit the template.
  ❌ An extra level of indirection for readers unfamiliar with
     Template Method — mitigated by this docstring.

Thread safety:  ✅ Stateless — same as ``ASTVisitor``.
Async safety:   ✅ Synchronous; safe to call from async contexts.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from fastrest_core.query.type import AndNode, NotNode, OrNode
from fastrest_core.query.visitor.ast_visitor import ASTVisitor


class BinaryWalkingVisitor(ASTVisitor, ABC):
    """
    ``ASTVisitor`` specialisation for visitors that walk AND/OR/NOT children
    and combine the results.

    Concrete subclasses must implement:
    - ``_visit_comparison()`` — the unique, backend-specific leaf handler.
    - ``_combine_and(left, right)`` — how to merge two AND sub-results.
    - ``_combine_or(left, right)`` — how to merge two OR sub-results.
    - ``_combine_not(inner)`` — how to negate a NOT sub-result.

    Example — a visitor that collects all field names::

        class FieldCollector(BinaryWalkingVisitor):
            def _visit_comparison(self, node, args=None, **kw):
                return {node.field}

            def _combine_and(self, left, right):
                return left | right

            def _combine_or(self, left, right):
                return left | right

            def _combine_not(self, inner):
                return inner

    Thread safety:  ✅ Stateless.
    Async safety:   ✅ Synchronous.
    """

    # ── Abstract combine hooks — implement in concrete subclass ───────────────

    @abstractmethod
    def _combine_and(self, left: Any, right: Any) -> Any:
        """
        Combine the compiled results of an AND node's two children.

        Called after both children have been visited.  The return value
        becomes the compiled representation of the AND node.

        Args:
            left:  Compiled result of the AND node's left child.
            right: Compiled result of the AND node's right child.

        Returns:
            Backend-specific AND expression (e.g. SQLAlchemy ``and_()``,
            MongoDB ``{"$and": [...]}``, or a reconstructed ``AndNode``).
        """

    @abstractmethod
    def _combine_or(self, left: Any, right: Any) -> Any:
        """
        Combine the compiled results of an OR node's two children.

        Args:
            left:  Compiled result of the OR node's left child.
            right: Compiled result of the OR node's right child.

        Returns:
            Backend-specific OR expression.
        """

    @abstractmethod
    def _combine_not(self, inner: Any) -> Any:
        """
        Negate the compiled result of a NOT node's single child.

        Args:
            inner: Compiled result of the NOT node's child.

        Returns:
            Backend-specific NOT expression.
        """

    # ── Concrete walk implementations — no override needed in subclasses ──────

    def _visit_and(self, node: AndNode, args: Any = None, **kwargs: Any) -> Any:
        """
        Walk both children of an AND node, then combine via ``_combine_and``.

        Args:
            node: The AND node to process.
            args: Forwarded to child visits (unused in most subclasses).

        Returns:
            ``_combine_and(visit(node.left), visit(node.right))``.
        """
        # Visit children first — results are passed to the combination hook.
        # This guarantees a post-order walk (leaves processed before parents).
        return self._combine_and(self.visit(node.left), self.visit(node.right))

    def _visit_or(self, node: OrNode, args: Any = None, **kwargs: Any) -> Any:
        """
        Walk both children of an OR node, then combine via ``_combine_or``.

        Args:
            node: The OR node to process.
            args: Forwarded to child visits.

        Returns:
            ``_combine_or(visit(node.left), visit(node.right))``.
        """
        return self._combine_or(self.visit(node.left), self.visit(node.right))

    def _visit_not(self, node: NotNode, args: Any = None, **kwargs: Any) -> Any:
        """
        Walk the single child of a NOT node, then negate via ``_combine_not``.

        Args:
            node: The NOT node to process.
            args: Forwarded to child visit.

        Returns:
            ``_combine_not(visit(node.child))``.
        """
        return self._combine_not(self.visit(node.child))
