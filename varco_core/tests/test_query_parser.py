"""
Unit tests for varco_core.query.parser — QueryParser string → AST.

Exercises the full parse pipeline: grammar → Lark → QueryTransformer → AST.
No backend is involved — output is always a TransformerNode.
"""

from __future__ import annotations

import pytest
from lark import UnexpectedEOF, UnexpectedToken

from varco_core.query.parser import QueryParser
from varco_core.query.type import (
    AndNode,
    ComparisonNode,
    NotNode,
    Operation,
    OrNode,
)


# ── Helpers ────────────────────────────────────────────────────────────────────


def _parse(q: str):
    """Convenience wrapper — creates a fresh parser per call."""
    return QueryParser().parse(q)


# ── Simple comparisons ────────────────────────────────────────────────────────


def test_parse_equality_string():
    node = _parse('name = "Alice"')
    assert isinstance(node, ComparisonNode)
    assert node.field == "name"
    assert node.op == Operation.EQUAL
    assert node.value == "Alice"


def test_parse_equality_number():
    node = _parse("age = 18")
    assert isinstance(node, ComparisonNode)
    assert node.field == "age"
    assert node.value == pytest.approx(18)


def test_parse_not_equal():
    node = _parse('status != "inactive"')
    assert isinstance(node, ComparisonNode)
    assert node.op == Operation.NOT_EQUAL


def test_parse_greater_than():
    node = _parse("price > 9.99")
    assert isinstance(node, ComparisonNode)
    assert node.op == Operation.GREATER_THAN
    assert node.value == pytest.approx(9.99)


def test_parse_greater_equal():
    node = _parse("score >= 100")
    assert node.op == Operation.GREATER_EQUAL


def test_parse_less_than():
    node = _parse("age < 65")
    assert node.op == Operation.LESS_THAN


def test_parse_less_equal():
    node = _parse("rank <= 10")
    assert node.op == Operation.LESS_EQUAL


def test_parse_like():
    node = _parse('email LIKE "%@example.com"')
    assert isinstance(node, ComparisonNode)
    assert node.op == Operation.LIKE
    assert node.value == "%@example.com"


# ── NULL checks ───────────────────────────────────────────────────────────────


def test_parse_is_null():
    node = _parse("deleted_at IS NULL")
    assert isinstance(node, ComparisonNode)
    assert node.op == Operation.IS_NULL
    assert node.value is None


def test_parse_is_not_null():
    node = _parse("published_at IS NOT NULL")
    assert isinstance(node, ComparisonNode)
    assert node.op == Operation.IS_NOT_NULL
    assert node.value is None


# ── IN list ───────────────────────────────────────────────────────────────────


def test_parse_in_list_strings():
    node = _parse('role IN ("admin", "editor")')
    assert isinstance(node, ComparisonNode)
    assert node.op == Operation.IN
    assert node.value == ["admin", "editor"]


def test_parse_in_list_numbers():
    node = _parse("tier IN (1, 2, 3)")
    assert isinstance(node, ComparisonNode)
    assert node.value == pytest.approx([1.0, 2.0, 3.0])


# ── Boolean combinators ───────────────────────────────────────────────────────


def test_parse_and():
    node = _parse('name = "Alice" AND age > 18')
    assert isinstance(node, AndNode)
    assert isinstance(node.left, ComparisonNode)
    assert isinstance(node.right, ComparisonNode)
    assert node.left.field == "name"
    assert node.right.field == "age"


def test_parse_or():
    node = _parse('status = "active" OR status = "pending"')
    assert isinstance(node, OrNode)


def test_parse_not():
    node = _parse("NOT age > 18")
    assert isinstance(node, NotNode)
    assert isinstance(node.child, ComparisonNode)


def test_parse_complex_and_or():
    node = _parse("a = 1 AND b = 2 OR c = 3")
    # Grammar: AND has higher precedence via left-recursion → (a=1 AND b=2) OR c=3
    assert isinstance(node, OrNode)


def test_parse_parenthesised_group():
    node = _parse('(name = "Alice")')
    # Parentheses are unwrapped — result is just the inner ComparisonNode
    assert isinstance(node, ComparisonNode)


def test_parse_nested_parens():
    node = _parse("(a = 1 AND b = 2) OR c = 3")
    assert isinstance(node, OrNode)
    assert isinstance(node.left, AndNode)


def test_parser_reuse_across_calls():
    """Same QueryParser instance should handle multiple calls correctly."""
    parser = QueryParser()
    n1 = parser.parse("x = 1")
    n2 = parser.parse("y = 2")
    assert isinstance(n1, ComparisonNode)
    assert isinstance(n2, ComparisonNode)
    assert n1.field == "x"
    assert n2.field == "y"


# ── Error cases ───────────────────────────────────────────────────────────────


def test_parse_empty_string_raises():
    with pytest.raises((UnexpectedEOF, UnexpectedToken, Exception)):
        _parse("")


def test_parse_syntax_error_raises():
    with pytest.raises(Exception):  # Lark raises UnexpectedToken / UnexpectedEOF
        _parse('name ==== "Alice"')
