from dataclasses import dataclass
from typing import Optional, Any
from fastrest.query.type import ComparisonNode,AndNode,OrNode,Operation,TransformerNode,NodeType,NotNode

@dataclass(frozen=True)
class QueryBuilder:
    node: Optional[TransformerNode] = None

    # ---------- INTERNAL ----------
    def _combine(self, new_node: TransformerNode, op: NodeType):
        if self.node is None:
            return QueryBuilder(new_node)

        if op == NodeType.AND:
            return QueryBuilder(AndNode(self.node, new_node))
        elif op == NodeType.OR:
            return QueryBuilder(OrNode(self.node, new_node))

        raise ValueError(f"Unsupported combine op {op}")

    # ---------- WHERE ----------
    def where(self, field: str, op: Operation, value: Any = None):
        comp = ComparisonNode(field=field, op=op, value=value)
        return self._combine(comp, NodeType.AND)

    # ---------- LOGICAL ----------
    def and_(self, other: "QueryBuilder"):
        if other.node is None:
            return self
        return self._combine(other.node, NodeType.AND)

    def or_(self, other: "QueryBuilder"):
        if other.node is None:
            return self
        return self._combine(other.node, NodeType.OR)

    def not_(self):
        if self.node is None:
            return self
        return QueryBuilder(NotNode(self.node))

    # ---------- COMPARISON HELPERS ----------
    def eq(self, field: str, value: Any):
        return self.where(field, Operation.EQUAL, value)

    def ne(self, field: str, value: Any):
        return self.where(field, Operation.NOT_EQUAL, value)

    def gt(self, field: str, value: Any):
        return self.where(field, Operation.GREATER_THAN, value)

    def gte(self, field: str, value: Any):
        return self.where(field, Operation.GREATER_EQUAL, value)

    def lt(self, field: str, value: Any):
        return self.where(field, Operation.LESS_THAN, value)

    def lte(self, field: str, value: Any):
        return self.where(field, Operation.LESS_EQUAL, value)

    def like(self, field: str, value: str):
        return self.where(field, Operation.LIKE, value)

    def in_(self, field: str, values):
        return self.where(field, Operation.IN, values)

    def is_null(self, field: str):
        return self.where(field, Operation.IS_NULL)

    def is_not_null(self, field: str):
        return self.where(field, Operation.IS_NOT_NULL)

    # ---------- BUILD ----------
    def build(self) -> Optional[TransformerNode]:
        return self.node
    
    @staticmethod
    def field(field: str, op: Operation, value=None):
        return QueryBuilder(ComparisonNode(field, op, value))

    @staticmethod
    def eq_(field, value):
        return QueryBuilder().eq(field, value)