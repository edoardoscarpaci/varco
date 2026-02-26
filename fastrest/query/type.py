from enum import StrEnum
from dataclasses import dataclass
from dataclasses import field as dfield
from typing import Union,Optional,List,Any,Protocol,TYPE_CHECKING

if TYPE_CHECKING:
    from fastrest.query.visitor.ast_visitor import ASTVisitor 



Scalar = Union[int, float, str, bool]
Value = Optional[Union[Scalar, List[Scalar]]]

class SortOrder(StrEnum):
    ASC = "ASC"
    DESC = "DESC"

class NodeType(StrEnum):
    COMPARISON = "COMPARISON"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"

class Operation(StrEnum):
    EQUAL = "="
    NOT_EQUAL = "!="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_EQUAL = ">="
    LESS_EQUAL = "<="
    LIKE = "LIKE"
    IN = "IN"
    IS_NULL = "IS_NULL"
    IS_NOT_NULL = "IS_NOT_NULL"

@dataclass(frozen=True)
class TransformerNode(Protocol):
    """Base class for all AST nodes"""
    type: NodeType = dfield(init=False)

@dataclass(frozen=True)
class ComparisonNode(TransformerNode):
    field : str
    op: Operation
    value: Value = None
    type: NodeType = dfield(init=False, default=NodeType.COMPARISON)

    def __post_init__(self):
        if self.op == Operation.IN and not isinstance(self.value, list):
            raise TypeError("IN operation requires a list as value")

        if self.op in (Operation.IS_NULL, Operation.IS_NOT_NULL) and self.value is not None:
            raise TypeError(f"{self.op} must not have a value")

@dataclass(frozen=True)
class BinaryNode(TransformerNode):
    left : TransformerNode
    right: TransformerNode

@dataclass(frozen=True)
class AndNode(BinaryNode):
    type: NodeType = dfield(init=False, default=NodeType.AND)

@dataclass(frozen=True)
class OrNode(BinaryNode):
    type: NodeType = dfield(init=False, default=NodeType.OR)

@dataclass(frozen=True)
class NotNode(TransformerNode):
    child: TransformerNode
    type: NodeType = dfield(init=False, default=NodeType.NOT)

@dataclass(frozen=True)
class SortField():
    field : str
    order : SortOrder