"""
varco_core.query.visitor.ast_visitor
========================================
Abstract base class for all AST visitors.

Subclasses implement ``_visit_*`` handlers for each node type.  The base
class provides type-validated public entry points and a generic dispatcher
so visitors only need to override the four leaf handlers.

Thread safety:  ✅ Stateless — visitors carry no mutable shared state.
Async safety:   ✅ All methods are synchronous; safe to call from async code.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from varco_core.query.type import (
    AndNode,
    ComparisonNode,
    NotNode,
    OrNode,
    TransformerNode,
)
from varco_core.exception.query import WrongNodeVisited


class ASTVisitor(ABC):
    """
    Abstract base for query AST visitors.

    DESIGN: visitor pattern over isinstance chains
      ✅ Each visitor concern (compilation, type-coercion, optimisation) is
         isolated in its own class — closed for modification, open for extension.
      ✅ The dispatch method (``visit``) is derived from the node's ``type``
         attribute — no explicit isinstance in callers.
      ❌ Adding a new node type requires updating all visitor subclasses.

    Thread safety:  ✅ Stateless — safe for concurrent calls.
    Async safety:   ✅ Synchronous; safe to call from async contexts.

    Edge cases:
        - ``visit()`` called with an unknown node type → ``generic_visit()``
          raises ``NotImplementedError`` by default.
        - ``visit_comparison()`` / ``visit_and()`` etc. validate the node type
          before dispatching — raises ``WrongNodeVisited`` if wrong type passed.
    """

    # ── Abstract handlers — implement one per node type ───────────────────────

    @abstractmethod
    def _visit_comparison(
        self, node: ComparisonNode, args: Any = None, **kwargs: Any
    ) -> Any:
        """
        Handle a comparison leaf node.

        Args:
            node:   The ``ComparisonNode`` to process.
            args:   Optional positional context passed through ``visit()``.
            kwargs: Optional keyword context.

        Returns:
            Backend-specific representation (e.g. SQLAlchemy expression, dict).
        """

    @abstractmethod
    def _visit_and(self, node: AndNode, args: Any = None, **kwargs: Any) -> Any:
        """
        Handle a boolean AND node.

        Args:
            node:   The ``AndNode`` whose left/right children must be visited.
            args:   Optional positional context.
            kwargs: Optional keyword context.
        """

    @abstractmethod
    def _visit_or(self, node: OrNode, args: Any = None, **kwargs: Any) -> Any:
        """
        Handle a boolean OR node.

        Args:
            node:   The ``OrNode`` whose left/right children must be visited.
            args:   Optional positional context.
            kwargs: Optional keyword context.
        """

    @abstractmethod
    def _visit_not(self, node: NotNode, args: Any = None, **kwargs: Any) -> Any:
        """
        Handle a boolean NOT node.

        Args:
            node:   The ``NotNode`` whose single child must be visited.
            args:   Optional positional context.
            kwargs: Optional keyword context.
        """

    # ── Validated public entry points ─────────────────────────────────────────

    def visit_comparison(
        self, node: ComparisonNode, args: Any = None, **kwargs: Any
    ) -> Any:
        """
        Validate then dispatch to ``_visit_comparison``.

        Raises:
            WrongNodeVisited: ``node`` is not a ``ComparisonNode``.
        """
        self._validate_comparison_node(node)
        return self._visit_comparison(node, args, **kwargs)

    def visit_and(self, node: AndNode, args: Any = None, **kwargs: Any) -> Any:
        """
        Validate then dispatch to ``_visit_and``.

        Raises:
            WrongNodeVisited: ``node`` is not an ``AndNode``.
        """
        self._validate_and_node(node)
        return self._visit_and(node, args, **kwargs)

    def visit_or(self, node: OrNode, args: Any = None, **kwargs: Any) -> Any:
        """
        Validate then dispatch to ``_visit_or``.

        Raises:
            WrongNodeVisited: ``node`` is not an ``OrNode``.
        """
        self._validate_or_node(node)
        return self._visit_or(node, args, **kwargs)

    def visit_not(self, node: NotNode, args: Any = None, **kwargs: Any) -> Any:
        """
        Validate then dispatch to ``_visit_not``.

        Raises:
            WrongNodeVisited: ``node`` is not a ``NotNode``.
        """
        self._validate_not_node(node)
        return self._visit_not(node, args, **kwargs)

    def visit(self, node: TransformerNode) -> Any:
        """
        Generic dispatcher — routes a node to the appropriate ``visit_*`` method.

        Uses ``node.type.value.lower()`` to build the method name (e.g.
        ``visit_comparison``), falling back to ``generic_visit`` if not found.

        Args:
            node: Any AST node.

        Returns:
            Whatever the matched ``visit_*`` method returns.
        """
        # DESIGN: string-based dispatch avoids a large if/elif chain and
        # automatically handles future node types without changing this method.
        method_name = f"visit_{node.type.value.lower()}"
        visitor = getattr(self, method_name, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node: TransformerNode) -> Any:
        """
        Fallback called when no specific handler exists.

        Raises:
            NotImplementedError: Always — subclasses should override if they
                want a non-raising fallback.
        """
        raise NotImplementedError(
            f"No visitor defined for node type {node.type!r}. "
            "Implement a 'visit_{type}' method or override 'generic_visit'."
        )

    # ── Private validation helpers ────────────────────────────────────────────

    def _validate_comparison_node(self, node: ComparisonNode) -> None:
        self.__validate_node_type(node, ComparisonNode)

    def _validate_and_node(self, node: AndNode) -> None:
        self.__validate_node_type(node, AndNode)

    def _validate_or_node(self, node: OrNode) -> None:
        self.__validate_node_type(node, OrNode)

    def _validate_not_node(self, node: NotNode) -> None:
        self.__validate_node_type(node, NotNode)

    @staticmethod
    def __validate_node_type(
        node: TransformerNode,
        expected_cls: type[TransformerNode],
    ) -> None:
        """
        Raise ``WrongNodeVisited`` when ``node`` is not an instance of ``expected_cls``.

        Args:
            node:         The node to validate.
            expected_cls: The expected concrete node class.

        Raises:
            WrongNodeVisited: Type mismatch detected.
        """
        if not isinstance(node, expected_cls):
            raise WrongNodeVisited(
                received_node_cls=type(node),
                expected_node_cls=expected_cls,
            )
