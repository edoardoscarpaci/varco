from fastrest.query.type import ComparisonNode,AndNode,OrNode,NotNode
from fastrest.query.visitor.ast_visitor import ASTVisitor
from typing import Union
from sqlalchemy.orm import DeclarativeBase,MappedColumn
from sqlalchemy import BinaryExpression, ColumnElement, and_, or_, not_, asc, desc
from fastrest.query.type import ComparisonNode,AndNode,OrNode,NotNode,Operation
from fastrest.exception.query import OperationNotSupported
from fastrest.exception.repository import FieldNotFound
 
class SQLAlchemyQueryCompiler(ASTVisitor):
    def __init__(self,model : DeclarativeBase,allowed_fields: set[str] = set(),*args,**kwargs):
        self.allowed_fields = allowed_fields or set()
        self.model = model

    def _visit_comparison(self,node: ComparisonNode, args,**kwargs) -> Union[ColumnElement[bool],BinaryExpression[bool]]:
        if node.field is None:
            raise ValueError("Comparison node must have a field")
        
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

    def _visit_and(self, node :AndNode,args,**kwargs):
        return and_(self.visit(node.left), self.visit(node.right))
    
    def _visit_or(self,node : OrNode,args,**kwargs):
        return or_(self.visit(node.left), self.visit(node.right))
        
    def _visit_not(self,node : NotNode,args,**kwargs):
        return not_(self.visit(node.child))
    
    # Getters and Setters for allowed_fields
    def get_allowed_fields(self) -> set[str]:
        return self.allowed_fields.copy()
    
    def set_allowed_fields(self,allowed_fields: set[str]) -> None:
        self.allowed_fields = allowed_fields

    def add_to_allowed_fields(self, value : str):
        self.allowed_fields.add(value)

    def is_allowed_fields_empty(self)->bool:
        return len(self.allowed_fields) == 0

    #Helper Functions
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
            return getattr(self.model, field_path)
        except AttributeError as exc:
            raise FieldNotFound(field=field_path,table=self.model.__tablename__) from exc



