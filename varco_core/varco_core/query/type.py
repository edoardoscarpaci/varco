"""
varco_core.query.type
=========================
AST node types used throughout the query system.

All nodes are frozen dataclasses — they are immutable value objects safe to
cache, hash, and pass across async boundaries without copying.

Thread safety:  ✅ Immutable after construction.
Async safety:   ✅ Value objects; no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field as dfield
from enum import StrEnum
from typing import Protocol

# ── Scalar and value types ─────────────────────────────────────────────────────

# DESIGN: broad Scalar covers all primitive comparison targets.
# Lists are allowed only for IN operations (validated in ComparisonNode.__post_init__).
Scalar = int | float | str | bool
Value = Scalar | list[Scalar] | None


# ── Enums ──────────────────────────────────────────────────────────────────────


class SortOrder(StrEnum):
    """
    Sort direction for ``SortField`` directives.

    Attributes:
        ASC:  Ascending order (default in SQL).
        DESC: Descending order.
    """

    ASC = "ASC"
    DESC = "DESC"


class NodeType(StrEnum):
    """
    Discriminant tag stored on every AST node.

    Used by ``ASTVisitor.visit()`` to dispatch without ``isinstance`` chains.

    Attributes:
        COMPARISON: A binary comparison (``field OP value``).
        AND:        Boolean conjunction of two sub-expressions.
        OR:         Boolean disjunction of two sub-expressions.
        NOT:        Boolean negation of a sub-expression.
    """

    COMPARISON = "COMPARISON"
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


class Operation(StrEnum):
    """
    Comparison operators supported by the query AST.

    Attributes:
        EQUAL:        ``field = value``
        NOT_EQUAL:    ``field != value``
        GREATER_THAN: ``field > value``
        LESS_THAN:    ``field < value``
        GREATER_EQUAL:``field >= value``
        LESS_EQUAL:   ``field <= value``
        LIKE:         ``field LIKE pattern``  (SQL LIKE semantics)
        IN:           ``field IN (v1, v2, ...)``
        IS_NULL:      ``field IS NULL``  (value must be None)
        IS_NOT_NULL:  ``field IS NOT NULL``  (value must be None)
    """

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


# ── Protocol base for all nodes ────────────────────────────────────────────────


@dataclass(frozen=True)
class TransformerNode(Protocol):
    """
    Structural protocol satisfied by every AST node.

    The ``type`` attribute is the discriminant used by ``ASTVisitor.visit()``
    to route dispatch without ``isinstance`` chains.

    Thread safety:  ✅ Frozen dataclass — immutable.
    """

    type: NodeType = dfield(init=False)


# ── Leaf nodes ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ComparisonNode(TransformerNode):
    """
    Represents a single field comparison.

    Attributes:
        field: Column / field name being compared.
        op:    The comparison ``Operation``.
        value: The comparison target.  Must be ``None`` for IS_NULL /
               IS_NOT_NULL, and a ``list`` for IN.
        type:  Always ``NodeType.COMPARISON`` (set automatically).

    Thread safety:  ✅ Frozen dataclass.
    Async safety:   ✅ Value object.

    Edge cases:
        - ``op=IN``       → ``value`` must be a list; raises ``TypeError`` otherwise.
        - ``op=IS_NULL``  → ``value`` must be ``None``; raises ``TypeError`` otherwise.
        - ``op=IS_NOT_NULL`` → same constraint as IS_NULL.
    """

    field: str
    op: Operation
    value: Value = None
    type: NodeType = dfield(init=False, default=NodeType.COMPARISON)

    def __post_init__(self) -> None:
        if self.op == Operation.IN and not isinstance(self.value, list):
            raise TypeError(
                f"Operation.IN requires a list value, got {type(self.value).__name__!r}"
            )
        if (
            self.op in (Operation.IS_NULL, Operation.IS_NOT_NULL)
            and self.value is not None
        ):
            raise TypeError(f"{self.op} must not have a value (got {self.value!r})")


# ── Binary tree nodes ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class BinaryNode(TransformerNode):
    """
    Base class for binary boolean nodes (AND / OR).

    Attributes:
        left:  Left child expression.
        right: Right child expression.

    Thread safety:  ✅ Frozen.
    """

    left: TransformerNode
    right: TransformerNode


@dataclass(frozen=True)
class AndNode(BinaryNode):
    """
    Boolean AND of ``left`` and ``right`` sub-expressions.

    ``type`` is always ``NodeType.AND``.
    """

    type: NodeType = dfield(init=False, default=NodeType.AND)


@dataclass(frozen=True)
class OrNode(BinaryNode):
    """
    Boolean OR of ``left`` and ``right`` sub-expressions.

    ``type`` is always ``NodeType.OR``.
    """

    type: NodeType = dfield(init=False, default=NodeType.OR)


# ── Unary node ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class NotNode(TransformerNode):
    """
    Boolean NOT of a single child expression.

    Attributes:
        child: The expression to negate.
        type:  Always ``NodeType.NOT`` (set automatically).

    Thread safety:  ✅ Frozen.
    """

    type: NodeType = dfield(init=False, default=NodeType.NOT)
    child: TransformerNode = dfield(default=None)  # type: ignore[assignment]


# ── Sort directive ─────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SortField:
    """
    A single field sort directive.

    Attributes:
        field: Column / field name to sort by.
        order: ``SortOrder.ASC`` or ``SortOrder.DESC``.

    Thread safety:  ✅ Frozen dataclass.
    Async safety:   ✅ Value object.
    """

    field: str
    order: SortOrder
