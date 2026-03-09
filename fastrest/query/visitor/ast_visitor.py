"""Abstract AST visitor base class.

Subclasses implement `_visit_*` handlers for specific AST node types. The
base class provides validation wrappers and a `visit` dispatcher.
"""

from abc import ABC, abstractmethod
from typing import Any
from fastrest.query.type import (
    ComparisonNode,
    AndNode,
    OrNode,
    NotNode,
    TransformerNode,
)
from fastrest.exception.query import WrongNodeVisited


class ASTVisitor(ABC):
    """Base class for AST visitors.

    Implement concrete behavior by overriding the `_visit_comparison`,
    `_visit_and`, `_visit_or`, and `_visit_not` methods.
    """

    # Abstract visit methods to be implemented by subclasses
    @abstractmethod
    def _visit_comparison(
        self, node: ComparisonNode, args: Any = None, **kwargs
    ) -> Any:
        """Handle a comparison node. Implement in subclasses."""
        pass

    @abstractmethod
    def _visit_and(self, node: AndNode, args: Any = None, **kwargs) -> Any:
        """Handle an AND node. Implement in subclasses."""
        pass

    @abstractmethod
    def _visit_or(self, node: OrNode, args: Any = None, **kwargs) -> Any:
        """Handle an OR node. Implement in subclasses."""
        pass

    @abstractmethod
    def _visit_not(self, node: NotNode, args: Any = None, **kwargs) -> Any:
        """Handle a NOT node. Implement in subclasses."""
        pass

    # Public visit methods with validation
    def visit_comparison(self, node: ComparisonNode, args: Any = None, **kwargs) -> Any:
        """Public entry to visit a comparison node with validation."""
        self._validate_comparison_node(node)
        return self._visit_comparison(node, args, **kwargs)

    def visit_and(self, node: AndNode, args: Any = None, **kwargs) -> Any:
        """Public entry to visit an AND node with validation."""
        self._validate_and_node(node)
        return self._visit_and(node, args, **kwargs)

    def visit_or(self, node: OrNode, args: Any = None, **kwargs) -> Any:
        """Public entry to visit an OR node with validation."""
        self._validate_or_node(node)
        return self._visit_or(node, args, **kwargs)

    def visit_not(self, node: NotNode, args: Any = None, **kwargs) -> Any:
        """Public entry to visit a NOT node with validation."""
        self._validate_not_node(node)
        return self._visit_not(node, args, **kwargs)

    def visit(self, node: TransformerNode):
        """Generic dispatch: call `visit_<node_type>` if available or fallback."""
        method_name = f"visit_{node.type.value.lower()}"
        visitor = getattr(self, method_name, self.generic_visit)
        return visitor(node)

    def generic_visit(self, node: TransformerNode):
        """Default visitor when no specific handler exists; raises by default."""
        raise NotImplementedError(f"No visitor for {node.type}")

    # Helper validation functions
    def _validate_comparison_node(self, node: ComparisonNode):
        self.__validate_node_type(node, ComparisonNode)

    def _validate_and_node(self, node: AndNode):
        self.__validate_node_type(node, AndNode)

    def _validate_or_node(self, node: OrNode):
        self.__validate_node_type(node, OrNode)

    def _validate_not_node(self, node: NotNode):
        self.__validate_node_type(node, NotNode)

    def __validate_node_type(
        self, node: TransformerNode, expected_cls: type[TransformerNode]
    ):
        if not isinstance(node, expected_cls):
            raise WrongNodeVisited(
                received_node_cls=type(node), expected_node_cls=expected_cls
            )
