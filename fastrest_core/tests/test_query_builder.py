"""
Unit tests for fastrest_core.query.builder — QueryBuilder fluent API.

Verifies immutability, all comparison shorthands, logical combinators,
static factory methods, and the terminal build() method.
"""

from __future__ import annotations

import pytest

from fastrest_core.query.builder import QueryBuilder
from fastrest_core.query.type import (
    AndNode,
    ComparisonNode,
    NodeType,
    NotNode,
    Operation,
    OrNode,
)


# ── Empty builder ─────────────────────────────────────────────────────────────


def test_empty_builder_build_returns_none():
    """build() on an empty builder must return None — no filter."""
    assert QueryBuilder().build() is None


def test_empty_builder_not_returns_self():
    """not_() on an empty builder returns the same (empty) builder."""
    qb = QueryBuilder()
    assert qb.not_() is qb


def test_and_with_empty_other_returns_self():
    """and_(other) when other.node is None must return self."""
    qb = QueryBuilder().eq("x", 1)
    empty = QueryBuilder()
    assert qb.and_(empty) is qb


def test_or_with_empty_other_returns_self():
    """or_(other) when other.node is None must return self."""
    qb = QueryBuilder().eq("x", 1)
    empty = QueryBuilder()
    assert qb.or_(empty) is qb


# ── Single comparison shorthands ──────────────────────────────────────────────


def test_eq_builds_comparison():
    node = QueryBuilder().eq("name", "Alice").build()
    assert isinstance(node, ComparisonNode)
    assert node.field == "name"
    assert node.op == Operation.EQUAL
    assert node.value == "Alice"


def test_ne():
    node = QueryBuilder().ne("status", "inactive").build()
    assert isinstance(node, ComparisonNode)
    assert node.op == Operation.NOT_EQUAL


def test_gt():
    node = QueryBuilder().gt("age", 18).build()
    assert node.op == Operation.GREATER_THAN
    assert node.value == 18


def test_gte():
    node = QueryBuilder().gte("age", 18).build()
    assert node.op == Operation.GREATER_EQUAL


def test_lt():
    node = QueryBuilder().lt("price", 100).build()
    assert node.op == Operation.LESS_THAN


def test_lte():
    node = QueryBuilder().lte("price", 100).build()
    assert node.op == Operation.LESS_EQUAL


def test_like():
    node = QueryBuilder().like("email", "%@example.com").build()
    assert node.op == Operation.LIKE
    assert node.value == "%@example.com"


def test_in_():
    node = QueryBuilder().in_("role", ["admin", "editor"]).build()
    assert node.op == Operation.IN
    assert node.value == ["admin", "editor"]


def test_is_null():
    node = QueryBuilder().is_null("deleted_at").build()
    assert node.op == Operation.IS_NULL
    assert node.value is None


def test_is_not_null():
    node = QueryBuilder().is_not_null("published_at").build()
    assert node.op == Operation.IS_NOT_NULL


# ── Multiple conditions are ANDed ─────────────────────────────────────────────


def test_two_conditions_produce_and_node():
    """Chaining two conditions must produce an AndNode."""
    node = QueryBuilder().eq("a", 1).eq("b", 2).build()
    assert isinstance(node, AndNode)
    assert node.type == NodeType.AND


def test_three_conditions_are_all_anded():
    """Three chained conditions must produce a nested AND tree."""
    node = QueryBuilder().eq("a", 1).eq("b", 2).eq("c", 3).build()
    assert isinstance(node, AndNode)
    # Outer AND → one side must itself be an AND
    assert isinstance(node.left, AndNode) or isinstance(node.right, AndNode)


# ── Immutability ──────────────────────────────────────────────────────────────


def test_builder_is_immutable():
    """Each method must return a NEW builder — the original must be unchanged."""
    original = QueryBuilder()
    extended = original.eq("x", 1)
    assert original.node is None
    assert extended.node is not None


def test_chained_builders_are_independent():
    base = QueryBuilder().eq("x", 1)
    branch_a = base.eq("y", 2)
    branch_b = base.eq("z", 3)
    # Both branches should differ from each other
    assert branch_a.build() != branch_b.build()


# ── Logical combinators ───────────────────────────────────────────────────────


def test_and_combines_two_builders():
    left = QueryBuilder().eq("a", 1)
    right = QueryBuilder().eq("b", 2)
    combined = left.and_(right).build()
    assert isinstance(combined, AndNode)


def test_or_combines_two_builders():
    left = QueryBuilder().eq("a", 1)
    right = QueryBuilder().eq("b", 2)
    combined = left.or_(right).build()
    assert isinstance(combined, OrNode)


def test_not_wraps_node():
    node = QueryBuilder().eq("active", True).not_().build()
    assert isinstance(node, NotNode)
    assert isinstance(node.child, ComparisonNode)


def test_double_not_produces_nested_not():
    """not_().not_() should produce NotNode(NotNode(...)) — optimizer handles elimination."""
    node = QueryBuilder().eq("x", 1).not_().not_().build()
    assert isinstance(node, NotNode)
    assert isinstance(node.child, NotNode)


# ── Static factory methods ────────────────────────────────────────────────────


def test_eq_static_factory():
    qb = QueryBuilder.eq_("active", True)
    node = qb.build()
    assert isinstance(node, ComparisonNode)
    assert node.op == Operation.EQUAL
    assert node.field == "active"
    assert node.value is True


def test_field_static_factory():
    qb = QueryBuilder.field("score", Operation.GREATER_THAN, 50)
    node = qb.build()
    assert isinstance(node, ComparisonNode)
    assert node.op == Operation.GREATER_THAN
    assert node.value == 50


# ── QueryBuilder is a frozen dataclass ───────────────────────────────────────


def test_builder_node_cannot_be_reassigned():
    qb = QueryBuilder().eq("x", 1)
    with pytest.raises((AttributeError, TypeError)):
        qb.node = None  # type: ignore[misc]
