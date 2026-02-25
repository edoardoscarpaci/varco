from typing import TYPE_CHECKING,Any,List,Optional
from sqlalchemy.orm import Query,DeclarativeBase,MappedColumn
from sqlalchemy import and_, or_, not_, asc, desc
from fastrest.query.applicator import QueryApplicator
from fastrest.query.type import TransformerNode,SortField,ComparisonNode,AndNode,OrNode,NotNode,Operation
from fastrest.exception.query import OperationNotSupported
from fastrest.exception.repository import FieldNotFound
class SQLAlchemyQueryApplicator(QueryApplicator):
    def __init__(self,model_cls : DeclarativeBase,*args,**kwargs):
        self.model_cls = model_cls
        super().__init__(*args,**kwargs)

    def apply_query(self, query: Query, node: TransformerNode, *args, **kwargs) -> Query:
        """
        Recursively apply AST filters to the query object.
        Returns the modified Query.
        """
        filter_expr = self._build_filter(node)
        return query.filter(filter_expr)
        
    def _build_filter(self, node: TransformerNode):
        """Recursively build SQLAlchemy filter expressions"""
        if isinstance(node, ComparisonNode):
            return self._convert_comparison(node)
        elif isinstance(node, AndNode):
            return and_(self._build_filter(node.left), self._build_filter(node.right))
        elif isinstance(node, OrNode):
            return or_(self._build_filter(node.left), self._build_filter(node.right))
        elif isinstance(node, NotNode):
            return not_(self._build_filter(node.child))
        else:
            raise TypeError(f"Unsupported AST node: {type(node)}")
        
    def _convert_comparison(self, node: ComparisonNode):
        if node.field is None:
            raise ValueError("Comparison node must have a field unless IS NULL")
        if self.allowed_fields and node.field not in self.allowed_fields:
            raise ValueError(f"Field {node.field} is not allowed")

        col = self._resolve_column(node.field)
        op, val = node.op, node.value

        if op == Operation.EQUAL: return col == val
        if op == Operation.NOT_EQUAL: return col != val
        if op == Operation.GREATER_THAN: return col > val
        if op == Operation.LESS_THAN: return col < val
        if op == Operation.GREATER_EQUAL: return col >= val
        if op == Operation.LESS_EQUAL: return col <= val
        if op == Operation.LIKE: return col.like(val)
        if op == Operation.IN: return col.in_(val) # pyright: ignore[reportArgumentType]
        if op == Operation.IS_NULL: return col.is_(None)
        if op == Operation.IS_NOT_NULL: return col.isnot(None)
        raise ValueError(f"Unsupported operation: {op}")
    
    def _resolve_column(self, field_path: str) -> MappedColumn:
        """
        Resolve a top-level column attribute on the mapped model class.

        Nested relationship traversal is not supported by this helper. If a
        dotted field path is passed (e.g. ``profile.city``) a ``ValueError``
        will be raised.
        """
        if "." in field_path:
            raise OperationNotSupported(f"Nested relationship fields({field_path})")

        try:
            return getattr(self.model_cls, field_path)
        except AttributeError as exc:
            raise FieldNotFound(field=field_path,table=self.model_cls.__tablename__) from exc

    def apply_pagination(self, query : Query, limit : Optional[int] , offset : Optional[int], *args,**kwargs):
        """Apply limit and offset to the query object"""
        if limit is not None:
            query = query.limit(limit)
        if offset is not None:
            query = query.offset(offset)
        return query

    def apply_sort(self, query : Query, sort_fields : List[SortField] = list(), *args,**kwargs):
        for sort in sort_fields:
            field_name = sort.field
            order = sort.order.value.lower()
            if self.allowed_fields and field_name not in self.allowed_fields:
                raise ValueError(f"Cannot sort by field {field_name}")

            col = self._resolve_column(field_name)
            query = query.order_by(desc(col) if order == "desc" else asc(col))
