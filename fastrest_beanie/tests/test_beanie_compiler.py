"""
Unit tests for fastrest_beanie.query.compiler — BeanieQueryCompiler and helpers.

BeanieQueryCompiler translates AST nodes to MongoDB filter dicts.
No MongoDB connection required — the output is a plain Python dict.
"""

from __future__ import annotations


from fastrest_core.query.type import (
    AndNode,
    ComparisonNode,
    NotNode,
    Operation,
    OrNode,
)
from fastrest_beanie.query.compiler import BeanieQueryCompiler, _sql_like_to_regex


# ── Helpers ────────────────────────────────────────────────────────────────────


def _compile(node) -> dict:
    return BeanieQueryCompiler().visit(node)


def _cmp(field: str, op: Operation, value=None) -> ComparisonNode:
    return ComparisonNode(field=field, op=op, value=value)


# ── _sql_like_to_regex ─────────────────────────────────────────────────────────


def test_like_percent_becomes_dot_star():
    assert _sql_like_to_regex("Alice%") == "Alice.*"


def test_like_underscore_becomes_dot():
    assert _sql_like_to_regex("_lice") == ".lice"


def test_like_percent_at_start():
    assert _sql_like_to_regex("%Alice") == ".*Alice"


def test_like_no_wildcards():
    result = _sql_like_to_regex("Alice")
    assert result == "Alice"


def test_like_regex_special_chars_escaped():
    """Dots, brackets etc. in the pattern must be escaped."""
    result = _sql_like_to_regex("file.txt")
    # The dot in "file.txt" should be escaped → "file\.txt"
    assert result == r"file\.txt"


def test_like_mixed_wildcards():
    result = _sql_like_to_regex("A%B_C")
    assert result == "A.*B.C"


# ── Comparison operations ──────────────────────────────────────────────────────


def test_compile_equal():
    result = _compile(_cmp("name", Operation.EQUAL, "Alice"))
    assert result == {"name": "Alice"}


def test_compile_not_equal():
    result = _compile(_cmp("age", Operation.NOT_EQUAL, 18))
    assert result == {"age": {"$ne": 18}}


def test_compile_greater_than():
    result = _compile(_cmp("age", Operation.GREATER_THAN, 18))
    assert result == {"age": {"$gt": 18}}


def test_compile_less_than():
    result = _compile(_cmp("age", Operation.LESS_THAN, 65))
    assert result == {"age": {"$lt": 65}}


def test_compile_greater_equal():
    result = _compile(_cmp("score", Operation.GREATER_EQUAL, 100))
    assert result == {"score": {"$gte": 100}}


def test_compile_less_equal():
    result = _compile(_cmp("rank", Operation.LESS_EQUAL, 10))
    assert result == {"rank": {"$lte": 10}}


def test_compile_like():
    result = _compile(_cmp("email", Operation.LIKE, "Alice%"))
    assert result == {"email": {"$regex": "Alice.*"}}


def test_compile_in():
    result = _compile(_cmp("role", Operation.IN, ["admin", "editor"]))
    assert result == {"role": {"$in": ["admin", "editor"]}}


def test_compile_is_null():
    """IS NULL uses $in: [None] to catch both explicit null and missing field."""
    result = _compile(_cmp("deleted_at", Operation.IS_NULL))
    assert result == {"deleted_at": {"$in": [None]}}


def test_compile_is_not_null():
    """IS NOT NULL requires $ne: None AND $exists: True."""
    result = _compile(_cmp("published_at", Operation.IS_NOT_NULL))
    assert result == {"published_at": {"$ne": None, "$exists": True}}


# ── Logical combinators ────────────────────────────────────────────────────────


def test_compile_and():
    left = _cmp("a", Operation.EQUAL, 1)
    right = _cmp("b", Operation.EQUAL, 2)
    result = _compile(AndNode(left=left, right=right))
    assert "$and" in result
    assert {"a": 1} in result["$and"]
    assert {"b": 2} in result["$and"]


def test_compile_or():
    left = _cmp("status", Operation.EQUAL, "active")
    right = _cmp("status", Operation.EQUAL, "pending")
    result = _compile(OrNode(left=left, right=right))
    assert "$or" in result
    assert len(result["$or"]) == 2


def test_compile_not():
    """NOT compiles to $nor."""
    inner = _cmp("age", Operation.LESS_THAN, 18)
    result = _compile(NotNode(child=inner))
    assert "$nor" in result
    assert {"age": {"$lt": 18}} in result["$nor"]


# ── Nested expressions ─────────────────────────────────────────────────────────


def test_compile_and_inside_or():
    left = AndNode(
        left=_cmp("a", Operation.EQUAL, 1),
        right=_cmp("b", Operation.EQUAL, 2),
    )
    right = _cmp("c", Operation.EQUAL, 3)
    result = _compile(OrNode(left=left, right=right))
    assert "$or" in result
    # First element of $or is the compiled AND
    assert "$and" in result["$or"][0]


def test_compile_not_of_and():
    inner = AndNode(
        left=_cmp("x", Operation.EQUAL, 1),
        right=_cmp("y", Operation.EQUAL, 2),
    )
    result = _compile(NotNode(child=inner))
    assert "$nor" in result
    assert "$and" in result["$nor"][0]


# ── Dotted paths are allowed ───────────────────────────────────────────────────


def test_compile_dotted_field_path():
    """MongoDB supports dot notation natively — no OperationNotSupported raised."""
    result = _compile(_cmp("profile.city", Operation.EQUAL, "NYC"))
    assert result == {"profile.city": "NYC"}
