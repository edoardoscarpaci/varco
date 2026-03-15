"""
Unit tests for fastrest_core.query.type — AST node types and enums.

All nodes are frozen dataclasses, so tests also verify immutability
and the auto-set ``type`` discriminant field.
"""

from __future__ import annotations

import pytest

from fastrest_core.query.type import (
    AndNode,
    ComparisonNode,
    NodeType,
    NotNode,
    Operation,
    OrNode,
    SortField,
    SortOrder,
)


# ── Enum sanity ────────────────────────────────────────────────────────────────


def test_sort_order_values():
    assert SortOrder.ASC == "ASC"
    assert SortOrder.DESC == "DESC"


def test_node_type_values():
    assert NodeType.COMPARISON == "COMPARISON"
    assert NodeType.AND == "AND"
    assert NodeType.OR == "OR"
    assert NodeType.NOT == "NOT"


def test_operation_all_values_present():
    values = {o.value for o in Operation}
    assert "=" in values
    assert "!=" in values
    assert ">" in values
    assert "<" in values
    assert ">=" in values
    assert "<=" in values
    assert "LIKE" in values
    assert "IN" in values
    assert "IS_NULL" in values
    assert "IS_NOT_NULL" in values


# ── ComparisonNode ─────────────────────────────────────────────────────────────


def test_comparison_node_basic():
    node = ComparisonNode(field="age", op=Operation.EQUAL, value=18)
    assert node.field == "age"
    assert node.op == Operation.EQUAL
    assert node.value == 18
    assert node.type == NodeType.COMPARISON


def test_comparison_node_type_is_comparison():
    node = ComparisonNode(field="x", op=Operation.GREATER_THAN, value=0)
    assert node.type is NodeType.COMPARISON


def test_comparison_node_is_frozen():
    """Frozen dataclass — attribute assignment must raise."""
    node = ComparisonNode(field="x", op=Operation.EQUAL, value=1)
    with pytest.raises((AttributeError, TypeError)):
        node.field = "y"  # type: ignore[misc]


def test_comparison_node_in_requires_list():
    """IN operation must receive a list — any other type raises TypeError."""
    with pytest.raises(TypeError, match="list"):
        ComparisonNode(field="status", op=Operation.IN, value="active")


def test_comparison_node_in_with_list_ok():
    node = ComparisonNode(field="status", op=Operation.IN, value=["a", "b"])
    assert node.value == ["a", "b"]


def test_comparison_node_is_null_must_have_no_value():
    with pytest.raises(TypeError, match="IS_NULL"):
        ComparisonNode(field="x", op=Operation.IS_NULL, value="oops")


def test_comparison_node_is_not_null_must_have_no_value():
    with pytest.raises(TypeError, match="IS_NOT_NULL"):
        ComparisonNode(field="x", op=Operation.IS_NOT_NULL, value=0)


def test_comparison_node_is_null_ok():
    node = ComparisonNode(field="deleted_at", op=Operation.IS_NULL)
    assert node.value is None


def test_comparison_node_none_value_default():
    node = ComparisonNode(field="f", op=Operation.EQUAL)
    assert node.value is None


# ── AndNode / OrNode ───────────────────────────────────────────────────────────

_LEFT = ComparisonNode(field="a", op=Operation.EQUAL, value=1)
_RIGHT = ComparisonNode(field="b", op=Operation.EQUAL, value=2)


def test_and_node_type():
    node = AndNode(left=_LEFT, right=_RIGHT)
    assert node.type == NodeType.AND


def test_or_node_type():
    node = OrNode(left=_LEFT, right=_RIGHT)
    assert node.type == NodeType.OR


def test_and_node_children():
    node = AndNode(left=_LEFT, right=_RIGHT)
    assert node.left is _LEFT
    assert node.right is _RIGHT


def test_or_node_children():
    node = OrNode(left=_LEFT, right=_RIGHT)
    assert node.left is _LEFT
    assert node.right is _RIGHT


def test_and_node_frozen():
    node = AndNode(left=_LEFT, right=_RIGHT)
    with pytest.raises((AttributeError, TypeError)):
        node.left = _RIGHT  # type: ignore[misc]


# ── NotNode ────────────────────────────────────────────────────────────────────


def test_not_node_type():
    node = NotNode(child=_LEFT)
    assert node.type == NodeType.NOT


def test_not_node_child():
    node = NotNode(child=_LEFT)
    assert node.child is _LEFT


def test_not_node_wrapping_and():
    inner = AndNode(left=_LEFT, right=_RIGHT)
    node = NotNode(child=inner)
    assert isinstance(node.child, AndNode)


# ── SortField ──────────────────────────────────────────────────────────────────


def test_sort_field_asc():
    sf = SortField(field="created_at", order=SortOrder.ASC)
    assert sf.field == "created_at"
    assert sf.order == SortOrder.ASC


def test_sort_field_desc():
    sf = SortField(field="price", order=SortOrder.DESC)
    assert sf.order == SortOrder.DESC


def test_sort_field_frozen():
    sf = SortField(field="x", order=SortOrder.ASC)
    with pytest.raises((AttributeError, TypeError)):
        sf.field = "y"  # type: ignore[misc]
