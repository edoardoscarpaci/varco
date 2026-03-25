"""
Unit tests for varco_beanie.query.compiler — BeanieQueryCompiler and helpers.

BeanieQueryCompiler translates AST nodes to MongoDB filter dicts.
No MongoDB connection required — the output is a plain Python dict.
"""

from __future__ import annotations

import pytest

from varco_core.exception.query import OperationNotSupported
from varco_core.query.type import (
    AndNode,
    ComparisonNode,
    NotNode,
    Operation,
    OrNode,
)
from varco_beanie.query.compiler import BeanieQueryCompiler, _sql_like_to_regex


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


# ── Security: allowed_fields whitelist ─────────────────────────────────────────


def _compiler_with_allowed(*fields: str) -> BeanieQueryCompiler:
    return BeanieQueryCompiler(allowed_fields=set(fields))


class TestAllowedFieldsWhitelist:
    """BeanieQueryCompiler allowed_fields enforcement."""

    def test_whitelisted_flat_field_passes(self):
        compiler = _compiler_with_allowed("name", "age")
        # Must not raise
        compiler.visit(_cmp("name", Operation.EQUAL, "Alice"))

    def test_non_whitelisted_flat_field_raises(self):
        compiler = _compiler_with_allowed("name")
        with pytest.raises(ValueError, match="allowed_fields"):
            compiler.visit(_cmp("age", Operation.EQUAL, 18))

    def test_whitelisted_dotted_field_passes(self):
        compiler = _compiler_with_allowed("profile.city")
        result = compiler.visit(_cmp("profile.city", Operation.EQUAL, "NYC"))
        assert result == {"profile.city": "NYC"}

    def test_non_whitelisted_dotted_field_raises(self):
        compiler = _compiler_with_allowed("name")
        with pytest.raises(ValueError, match="allowed_fields"):
            compiler.visit(_cmp("profile.city", Operation.EQUAL, "NYC"))

    def test_empty_allowed_fields_permits_all(self):
        compiler = BeanieQueryCompiler(allowed_fields=set())
        compiler.visit(_cmp("anything", Operation.EQUAL, "x"))  # must not raise


# ── Security: traversal depth limit ────────────────────────────────────────────


class TestTraversalDepthLimit:
    """BeanieQueryCompiler max_traversal_depth guard."""

    def test_path_within_depth_passes(self):
        compiler = BeanieQueryCompiler(max_traversal_depth=2)
        # depth = 2 hops: "a.b.c" → "a"→"b"→"c" = 2 dots = depth 2
        compiler.visit(_cmp("a.b.c", Operation.EQUAL, "x"))

    def test_path_exceeding_depth_raises(self):
        compiler = BeanieQueryCompiler(max_traversal_depth=1)
        # depth = 2 → exceeds limit of 1
        with pytest.raises(OperationNotSupported, match="max_traversal_depth"):
            compiler.visit(_cmp("a.b.c", Operation.EQUAL, "x"))

    def test_flat_path_always_within_depth(self):
        compiler = BeanieQueryCompiler(max_traversal_depth=0)
        # depth = 0 (no dots) → never exceeds any limit
        compiler.visit(_cmp("name", Operation.EQUAL, "Alice"))


# ── Security: segment safety ───────────────────────────────────────────────────


class TestSegmentSafety:
    """BeanieQueryCompiler rejects field paths with unsafe segment characters."""

    def test_sql_injection_in_segment_raises(self):
        # Must use a dotted path so the segment-validation branch is reached.
        # Flat paths with special chars are safe in MongoDB (no SQL injection
        # surface) but we validate dotted paths to prevent operator injection
        # via nested field names.
        with pytest.raises(ValueError, match="unsafe characters"):
            _compile(_cmp("profile.city; DROP--", Operation.EQUAL, "x"))

    def test_empty_segment_from_double_dot_raises(self):
        """Double dots produce an empty segment which fails the regex."""
        with pytest.raises(ValueError, match="unsafe characters"):
            _compile(_cmp("a..b", Operation.EQUAL, "x"))

    def test_dollar_prefix_segment_in_dotted_path_raises(self):
        """
        MongoDB operator injections like ``$where`` embedded in a dotted path
        (``"profile.$where"``) must be blocked by the segment regex.

        Note: a bare ``$where`` flat field is an unusual MongoDB field name
        but is not a query injection vector since it is used as a dict key,
        not as an operator.  When using allowed_fields, flat ``$where`` is
        blocked by the whitelist instead.
        """
        with pytest.raises(ValueError, match="unsafe characters"):
            _compile(_cmp("profile.$where", Operation.EQUAL, "x"))

    def test_valid_underscore_segment_passes(self):
        """Underscores are valid in field names."""
        result = _compile(_cmp("_private.field_name", Operation.EQUAL, "x"))
        assert result == {"_private.field_name": "x"}
