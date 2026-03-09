from typing import Any
from fastrest.query.type import ComparisonNode, AndNode, OrNode, NotNode
from fastrest.query.visitor.ast_visitor import ASTVisitor


class ASTQueryOptimizer(ASTVisitor):
    def _visit_comparison(
        self, node: ComparisonNode, args: Any = None, **kwargs
    ) -> Any:
        return node

    def _visit_and(self, node: AndNode, args: Any = None, **kwargs) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)

        if isinstance(left, AndNode):
            return AndNode(left.left, AndNode(left.right, right))
        return node

    def _visit_or(self, node: OrNode, args: Any = None, **kwargs) -> Any:
        return OrNode(self.visit(node.left), self.visit(node.right))

    def _visit_not(self, node: NotNode, args: Any = None, **kwargs) -> Any:
        child = self.visit(node.child)
        if isinstance(child, NotNode):
            return child.child

        return NotNode(child)
