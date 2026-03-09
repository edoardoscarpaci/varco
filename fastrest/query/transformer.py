from lark import Transformer
from fastrest.query.type import ComparisonNode, AndNode, OrNode, Operation, NotNode
from fastrest.exception.query import OperationNotFound


class QueryTransformer(Transformer):
    def string(self, s):
        return s[0][1:-1]

    def number(self, n):
        return float(n[0])

    def field(self, f):
        return str(f[0])

    def in_list(self, items):
        field = items[0]
        values = items[1:]
        return ComparisonNode(field=field, op=Operation.IN, value=list(values))

    def is_null(self, items):
        return ComparisonNode(field=items[0], op=Operation.IS_NULL)

    def is_not_null(self, items):
        return ComparisonNode(field=items[0], op=Operation.IS_NOT_NULL)

    def comparison(self, items):
        field, op, value = items
        if op not in Operation._value2member_map_:
            raise OperationNotFound(op=op)
        return ComparisonNode(field=field, op=Operation(op), value=value)

    def and_expr(self, items):
        return AndNode(left=items[0], right=items[1])

    def or_expr(self, items):
        return OrNode(left=items[0], right=items[1])

    def not_expr(self, items):
        return NotNode(child=items[0])

    def group(self, items):
        return items[0]
