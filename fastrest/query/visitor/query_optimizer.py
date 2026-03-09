from typing import Any
from fastrest.query.type import ComparisonNode, AndNode, OrNode, NotNode
from fastrest.query.visitor.ast_visitor import ASTVisitor


class ASTQueryOptimizer(ASTVisitor):
    def _visit_comparison(self, node: ComparisonNode, args, **kwargs) -> Any:
        return node

    def _visit_and(self, node: AndNode, args, **kwargs) -> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)

        if isinstance(left, AndNode):
            return AndNode(left.left, AndNode(left.right, right))
        return node

    def _visit_or(self, node: OrNode, args, **kwargs) -> Any:
        return OrNode(self.visit(node.left), self.visit(node.right))

    def _visit_not(self, node: NotNode, args, **kwargs) -> Any:
        child = self.visit(node.child)
        if isinstance(child, NotNode):
            return child.child

        return NotNode(child)
