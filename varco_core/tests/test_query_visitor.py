"""
Unit tests for varco_core.query.visitor.ast_visitor — ASTVisitor.

ASTVisitor is an ABC so we create a minimal concrete subclass that collects
visited node types.  Tests verify dispatch routing, type validation, and
the generic_visit fallback.
"""

from __future__ import annotations

from typing import Any

import pytest

from varco_core.exception.query import WrongNodeVisited
from varco_core.query.type import (
    AndNode,
    ComparisonNode,
    NotNode,
    Operation,
    OrNode,
)
from varco_core.query.visitor.ast_visitor import ASTVisitor


# ── Minimal concrete visitor ───────────────────────────────────────────────────


class _RecordingVisitor(ASTVisitor):
    """Visitor that records which handler was called and returns the node type."""

    def _visit_comparison(
        self, node: ComparisonNode, args: Any = None, **kwargs: Any
    ) -> str:
        return "comparison"

    def _visit_and(self, node: AndNode, args: Any = None, **kwargs: Any) -> str:
        return "and"

    def _visit_or(self, node: OrNode, args: Any = None, **kwargs: Any) -> str:
        return "or"

    def _visit_not(self, node: NotNode, args: Any = None, **kwargs: Any) -> str:
        return "not"


# ── visit() dispatch ───────────────────────────────────────────────────────────


_CMP = ComparisonNode(field="x", op=Operation.EQUAL, value=1)
_AND = AndNode(left=_CMP, right=_CMP)
_OR = OrNode(left=_CMP, right=_CMP)
_NOT = NotNode(child=_CMP)


def test_visit_routes_comparison():
    v = _RecordingVisitor()
    assert v.visit(_CMP) == "comparison"


def test_visit_routes_and():
    v = _RecordingVisitor()
    assert v.visit(_AND) == "and"


def test_visit_routes_or():
    v = _RecordingVisitor()
    assert v.visit(_OR) == "or"


def test_visit_routes_not():
    v = _RecordingVisitor()
    assert v.visit(_NOT) == "not"


# ── Validated public entry points ──────────────────────────────────────────────


def test_visit_comparison_explicit_call():
    v = _RecordingVisitor()
    assert v.visit_comparison(_CMP) == "comparison"


def test_visit_and_explicit_call():
    v = _RecordingVisitor()
    assert v.visit_and(_AND) == "and"


def test_visit_or_explicit_call():
    v = _RecordingVisitor()
    assert v.visit_or(_OR) == "or"


def test_visit_not_explicit_call():
    v = _RecordingVisitor()
    assert v.visit_not(_NOT) == "not"


# ── WrongNodeVisited on type mismatch ─────────────────────────────────────────


def test_visit_comparison_wrong_type_raises():
    """Passing an AndNode to visit_comparison must raise WrongNodeVisited."""
    v = _RecordingVisitor()
    with pytest.raises(WrongNodeVisited):
        v.visit_comparison(_AND)  # type: ignore[arg-type]


def test_visit_and_wrong_type_raises():
    v = _RecordingVisitor()
    with pytest.raises(WrongNodeVisited):
        v.visit_and(_CMP)  # type: ignore[arg-type]


def test_visit_or_wrong_type_raises():
    v = _RecordingVisitor()
    with pytest.raises(WrongNodeVisited):
        v.visit_or(_AND)  # type: ignore[arg-type]


def test_visit_not_wrong_type_raises():
    v = _RecordingVisitor()
    with pytest.raises(WrongNodeVisited):
        v.visit_not(_OR)  # type: ignore[arg-type]


# ── generic_visit fallback ─────────────────────────────────────────────────────


def test_generic_visit_raises_not_implemented():
    """generic_visit must raise NotImplementedError when no handler matches."""
    v = _RecordingVisitor()
    with pytest.raises(NotImplementedError):
        v.generic_visit(_CMP)


# ── WrongNodeVisited exception attributes ─────────────────────────────────────


def test_wrong_node_visited_attributes():
    exc = WrongNodeVisited(
        received_node_cls=AndNode,
        expected_node_cls=ComparisonNode,
    )
    assert exc.received_node_cls is AndNode
    assert exc.expected_node_cls is ComparisonNode
    assert "AndNode" in str(exc)
    assert "ComparisonNode" in str(exc)
