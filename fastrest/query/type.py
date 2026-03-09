"""AST node types and small helpers used by the query builder.

Defines node dataclasses that represent comparison and boolean
expressions, plus enums for operations and sort order.
"""

from enum import StrEnum
from dataclasses import dataclass
from dataclasses import field as dfield
from typing import Union, Optional, List, Protocol, TYPE_CHECKING

if TYPE_CHECKING:
    pass


Scalar = Union[int, float, str, bool]
Value = Optional[Union[Scalar, List[Scalar]]]


class SortOrder(StrEnum):
    """Sort order enum used for sorting directives."""

    ASC = "ASC"
    DESC = "DESC"


class NodeType(StrEnum):
    """Type of AST node."""

    COMPARISON = "COMPARISON"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


class Operation(StrEnum):
    """Comparison operations supported by the query AST."""

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
    """Protocol base for AST nodes; all nodes expose a `type` attribute."""

    type: NodeType = dfield(init=False)


@dataclass(frozen=True)
class ComparisonNode(TransformerNode):
    """Represents a field comparison operation.

    Attributes:
        field: Name of the model field being compared.
        op: The comparison `Operation`.
        value: The comparison value (or list for `IN`).
    """

    field: str
    op: Operation
    value: Value = None
    type: NodeType = dfield(init=False, default=NodeType.COMPARISON)

    def __post_init__(self):
        if self.op == Operation.IN and not isinstance(self.value, list):
            raise TypeError("IN operation requires a list as value")

        if (
            self.op in (Operation.IS_NULL, Operation.IS_NOT_NULL)
            and self.value is not None
        ):
            raise TypeError(f"{self.op} must not have a value")


@dataclass(frozen=True)
class BinaryNode(TransformerNode):
    """Base for binary boolean nodes (AND/OR)."""

    left: TransformerNode
    right: TransformerNode


@dataclass(frozen=True)
class AndNode(BinaryNode):
    type: NodeType = dfield(init=False, default=NodeType.AND)


@dataclass(frozen=True)
class OrNode(BinaryNode):
    type: NodeType = dfield(init=False, default=NodeType.OR)


@dataclass(frozen=True)
class NotNode(TransformerNode):
    type: NodeType = dfield(init=False, default=NodeType.NOT)
    child: TransformerNode


@dataclass(frozen=True)
class SortField:
    """Describes a single field sort directive."""

    field: str
    order: SortOrder
