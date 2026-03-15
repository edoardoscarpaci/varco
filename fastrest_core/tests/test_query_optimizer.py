"""
Unit tests for fastrest_core.query.visitor.query_optimizer — ASTQueryOptimizer.

Verifies double-negation elimination and AND flattening (the two transforms
the optimizer performs).
"""

from __future__ import annotations

from fastrest_core.query.type import (
    AndNode,
    ComparisonNode,
    NotNode,
    Operation,
    OrNode,
)
from fastrest_core.query.visitor.query_optimizer import ASTQueryOptimizer


# ── Helpers ────────────────────────────────────────────────────────────────────


def _opt(node):
    """Convenience: run the optimizer on a node."""
    return ASTQueryOptimizer().visit(node)


def _cmp(field: str, value: int = 1) -> ComparisonNode:
    return ComparisonNode(field=field, op=Operation.EQUAL, value=value)


_A = _cmp("a")
_B = _cmp("b")
_C = _cmp("c")


# ── Comparison nodes pass through unchanged ────────────────────────────────────


def test_comparison_returned_unchanged():
    result = _opt(_A)
    assert result is _A


# ── Double-negation elimination ────────────────────────────────────────────────


def test_double_not_eliminated():
    """NOT(NOT(x)) → x"""
    node = NotNode(child=NotNode(child=_A))
    result = _opt(node)
    # Should be the inner comparison, not a NotNode
    assert isinstance(result, ComparisonNode)
    assert result is _A


def test_single_not_preserved():
    """NOT(x) stays as NOT(x) — not a double negation."""
    node = NotNode(child=_A)
    result = _opt(node)
    assert isinstance(result, NotNode)
    assert result.child is _A


def test_triple_not_reduces_to_single_not():
    """NOT(NOT(NOT(x))) → NOT(x) — double eliminated, single remains."""
    node = NotNode(child=NotNode(child=NotNode(child=_A)))
    result = _opt(node)
    assert isinstance(result, NotNode)
    assert isinstance(result.child, ComparisonNode)


# ── AND flattening ─────────────────────────────────────────────────────────────


def test_and_left_recursive_flattened():
    """(A AND B) AND C → A AND (B AND C)"""
    # Left-recursive input
    inner = AndNode(left=_A, right=_B)  # (A AND B)
    node = AndNode(left=inner, right=_C)  # (A AND B) AND C
    result = _opt(node)

    assert isinstance(result, AndNode)
    # After flattening: result = AndNode(A, AndNode(B, C))
    assert result.left is _A
    assert isinstance(result.right, AndNode)
    right_child = result.right
    assert right_child.left is _B
    assert right_child.right is _C


def test_and_already_right_recursive_unchanged():
    """A AND (B AND C) is already right-recursive — structure preserved."""
    inner = AndNode(left=_B, right=_C)  # (B AND C)
    node = AndNode(left=_A, right=inner)  # A AND (B AND C)
    result = _opt(node)

    assert isinstance(result, AndNode)
    assert result.left is _A
    assert isinstance(result.right, AndNode)


# ── OR: recursion without flattening ──────────────────────────────────────────


def test_or_children_are_recursed():
    """OR children are visited; structure is not changed."""
    # Wrap a double-NOT inside an OR to verify children are visited
    not_not_a = NotNode(child=NotNode(child=_A))
    node = OrNode(left=not_not_a, right=_B)
    result = _opt(node)

    assert isinstance(result, OrNode)
    # The left child's double-NOT should be eliminated
    assert isinstance(result.left, ComparisonNode)
    assert result.right is _B


def test_or_does_not_flatten():
    """OR nodes are not rebalanced — left and right stay as-is."""
    inner = OrNode(left=_A, right=_B)
    node = OrNode(left=inner, right=_C)
    result = _opt(node)

    assert isinstance(result, OrNode)
    # Left side should still be an OrNode (no flattening for OR)
    assert isinstance(result.left, OrNode)
