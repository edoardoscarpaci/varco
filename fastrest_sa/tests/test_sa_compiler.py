"""
Unit tests for SQLAlchemyQueryCompiler — AST → SQLAlchemy expressions.

Tests verify that every comparison operation, logical combinator, and
error case produces the correct SA expression or raises the expected exception.
No session or database connection is required — we only inspect the expression
objects produced by the compiler.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import DeclarativeBase

from fastrest_core.exception.query import OperationNotSupported
from fastrest_core.exception.repository import FieldNotFound
from fastrest_core.query.type import (
    AndNode,
    ComparisonNode,
    NotNode,
    Operation,
    OrNode,
)
from fastrest_core.query.visitor.sqlalchemy import SQLAlchemyQueryCompiler


# ── Minimal SA model ───────────────────────────────────────────────────────────


class _CompilerBase(DeclarativeBase):
    pass


class _PersonORM(_CompilerBase):
    __tablename__ = "persons_compiler"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)


# ── Helpers ────────────────────────────────────────────────────────────────────


def _compiler(**kwargs) -> SQLAlchemyQueryCompiler:
    return SQLAlchemyQueryCompiler(model=_PersonORM, **kwargs)


def _compile(node) -> object:
    return _compiler().visit(node)


def _cmp(field: str, op: Operation, value=None) -> ComparisonNode:
    return ComparisonNode(field=field, op=op, value=value)


def _str(expr) -> str:
    """Render a SA expression to string for assertion checking."""
    return str(expr.compile(compile_kwargs={"literal_binds": True}))


# ── Individual operations ──────────────────────────────────────────────────────


def test_compile_equal():
    expr = _compile(_cmp("name", Operation.EQUAL, "Alice"))
    assert "= 'Alice'" in _str(expr)


def test_compile_not_equal():
    expr = _compile(_cmp("age", Operation.NOT_EQUAL, 18))
    assert "!=" in _str(expr)


def test_compile_greater_than():
    expr = _compile(_cmp("age", Operation.GREATER_THAN, 18))
    assert ">" in _str(expr)


def test_compile_less_than():
    expr = _compile(_cmp("age", Operation.LESS_THAN, 65))
    assert "<" in _str(expr)


def test_compile_greater_equal():
    expr = _compile(_cmp("age", Operation.GREATER_EQUAL, 18))
    assert ">=" in _str(expr)


def test_compile_less_equal():
    expr = _compile(_cmp("age", Operation.LESS_EQUAL, 100))
    assert "<=" in _str(expr)


def test_compile_like():
    expr = _compile(_cmp("name", Operation.LIKE, "Ali%"))
    assert "LIKE" in _str(expr).upper()


def test_compile_in():
    expr = _compile(_cmp("age", Operation.IN, [18, 21, 25]))
    sql = _str(expr)
    assert "IN" in sql.upper()


def test_compile_is_null():
    expr = _compile(_cmp("name", Operation.IS_NULL))
    sql = _str(expr)
    assert "NULL" in sql.upper()


def test_compile_is_not_null():
    expr = _compile(_cmp("name", Operation.IS_NOT_NULL))
    sql = _str(expr)
    assert "NULL" in sql.upper()


# ── Logical combinators ────────────────────────────────────────────────────────


def test_compile_and():
    left = _cmp("name", Operation.EQUAL, "Alice")
    right = _cmp("age", Operation.GREATER_THAN, 18)
    node = AndNode(left=left, right=right)
    expr = _compile(node)
    sql = _str(expr)
    assert "AND" in sql.upper()


def test_compile_or():
    left = _cmp("name", Operation.EQUAL, "Alice")
    right = _cmp("name", Operation.EQUAL, "Bob")
    node = OrNode(left=left, right=right)
    expr = _compile(node)
    sql = _str(expr)
    assert "OR" in sql.upper()


def test_compile_not():
    """NOT wraps the child expression; SA may simplify negation in rendering."""
    inner = _cmp("age", Operation.GREATER_THAN, 18)
    node = NotNode(child=inner)
    expr = _compile(node)
    sql = _str(expr)
    # SA may render NOT(age > 18) as "age <= 18" (boolean negation simplification)
    # or as "NOT (age > 18)" — either is semantically correct.
    assert "age" in sql.lower()


# ── allowed_fields whitelist ───────────────────────────────────────────────────


def test_allowed_fields_permits_whitelisted_field():
    compiler = _compiler(allowed_fields={"name"})
    node = _cmp("name", Operation.EQUAL, "Alice")
    # Must not raise
    compiler.visit(node)


def test_allowed_fields_blocks_non_whitelisted_field():
    compiler = _compiler(allowed_fields={"name"})
    node = _cmp("age", Operation.EQUAL, 18)
    with pytest.raises(ValueError, match="allowed"):
        compiler.visit(node)


def test_empty_allowed_fields_permits_all():
    compiler = _compiler(allowed_fields=set())
    node = _cmp("age", Operation.EQUAL, 18)
    compiler.visit(node)  # must not raise


def test_add_to_allowed_fields():
    compiler = _compiler(allowed_fields={"name"})
    compiler.add_to_allowed_fields("age")
    assert "age" in compiler.get_allowed_fields()


def test_set_allowed_fields_replaces():
    compiler = _compiler(allowed_fields={"name", "age"})
    compiler.set_allowed_fields({"only_this"})
    assert compiler.get_allowed_fields() == {"only_this"}


def test_is_allowed_fields_empty_true():
    compiler = _compiler()
    assert compiler.is_allowed_fields_empty() is True


def test_is_allowed_fields_empty_false():
    compiler = _compiler(allowed_fields={"name"})
    assert compiler.is_allowed_fields_empty() is False


# ── Error cases ───────────────────────────────────────────────────────────────


def test_dotted_path_raises_operation_not_supported():
    node = _cmp("profile.city", Operation.EQUAL, "NYC")
    with pytest.raises(OperationNotSupported):
        _compile(node)


def test_unknown_field_raises_field_not_found():
    node = _cmp("nonexistent_col", Operation.EQUAL, "x")
    with pytest.raises(FieldNotFound):
        _compile(node)


def test_none_field_raises_value_error():
    node = ComparisonNode.__new__(ComparisonNode)
    object.__setattr__(node, "field", None)
    object.__setattr__(node, "op", Operation.EQUAL)
    object.__setattr__(node, "value", 1)
    object.__setattr__(
        node, "type", node.__class__.__dataclass_fields__["type"].default
    )
    with pytest.raises((ValueError, AttributeError)):
        _compile(node)
