from __future__ import annotations

from typing import Any,Callable, Optional,Type,Dict
from fastrest.query.type import ComparisonNode,AndNode,OrNode,NotNode,TransformerNode,Operation
from fastrest.query.visitor.ast_visitor import ASTVisitor
from copy import copy
from dataclasses import dataclass,field
from sqlalchemy.orm import DeclarativeBase
from fastrest.exception.query import CoercionError
from datetime import datetime

@dataclass(frozen=True)
class TypeCoercionFieldInfo:
    python_type: Type
    coercer : Callable[[Any],Any]


def coerce_boolean(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "1", "yes")
    try:
        return bool(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce value {value} to boolean") from exc

def coerce_datetime(value: Any):
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise CoercionError(f"Failed to coerce value {value} to datetime") from exc
    raise CoercionError(f"Cannot coerce value {value} to datetime")

def coerce_float(value: Any) -> float:
    try:
        return float(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce value {value} to float") from exc
    
def coerce_int(value: Any) -> int:
    try:
        return int(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce value {value} to int") from exc

def coerce_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    try:
        if isinstance(value, str):
            stripped_value : str = value.strip()
            if not stripped_value.startswith("[") or not stripped_value.endswith("]"):
                raise CoercionError(f"Failed to coerce value {value} to list. Expected a string in the format '[value1,value2,value3]'")
            return stripped_value.split(",")
    except Exception as exc:
        raise CoercionError(f"Failed to coerce value {value} to list. Expected a comma-separated string like '[value1,value2,value3]'.") from exc
    
    try:
        return list(value)
    except Exception as exc:
        raise CoercionError(f"Failed to coerce value {value} to list") from exc

def boolean_field_info():
    return TypeCoercionFieldInfo(bool, coerce_boolean)


default_field_coercions : Dict[Type,Callable] = {
    bool : coerce_boolean,
    datetime : coerce_datetime,
    int : coerce_int,
    float : coerce_float,
}

@dataclass
class TypeCoercionRegistry:
    fields_info: Dict[str, TypeCoercionFieldInfo] = field(default_factory=dict)

    def register_field(self, field_name: str, python_type: Type, coercer: Callable[[Any], Any]):
        self.fields_info[field_name] = TypeCoercionFieldInfo(python_type=python_type, coercer=coercer)

    def get(self, field_name: str) -> TypeCoercionFieldInfo | None:
        return self.fields_info.get(field_name)
    
    @classmethod
    def get_default_from_sqlalchemy_model(cls, model: DeclarativeBase, field_coercions: Dict[str, Callable[[Any], Any]] = {}) -> TypeCoercionRegistry:
        registry = cls()
        for column in model.__table__.columns:
            field_name = column.name
            python_type = column.type.python_type            
            coercer = field_coercions.get(field_name) or default_field_coercions.get(python_type)
            if coercer is not None:
                registry.register_field(field_name, python_type, coercer)

        return registry


class ASTTypeCoercion(ASTVisitor):
    def __init__(self, registry: Optional[TypeCoercionRegistry] = None,*args,**kwargs):
        self.registry = registry or TypeCoercionRegistry()

    def _visit_comparison(self,node: ComparisonNode,args,**kwargs) -> Any:
        field_info = self.registry.get(node.field)
        if not field_info or node.value is None:
            return node
        if node.op == Operation.IN or isinstance(node.value, list):
            coerced = [self._coerce_value(field_info,v) for v in node.value]
        else:
            coerced = self._coerce_value(field_info,node.value)
        return ComparisonNode(node.field, node.op, coerced)

    def _visit_and(self, node :AndNode, args,**kwargs)-> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)
        return AndNode(left, right)
    
    def _visit_or(self,node : OrNode,args,**kwargs)-> Any:
        left = self.visit(node.left)
        right = self.visit(node.right)
        return OrNode(left, right)
        
    def _visit_not(self,node : NotNode,args,**kwargs)-> Any:
        child = self.visit(node.child)
        return NotNode(child)
    
    def _coerce_value(self,field_info: TypeCoercionFieldInfo, value: Any) -> Any:
        try:
            return field_info.coercer(value)
        except Exception as exc:
            raise CoercionError(f"Failed to coerce value {value} for type {field_info.python_type}") from exc