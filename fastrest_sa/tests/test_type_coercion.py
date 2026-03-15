"""
Unit tests for fastrest_core.query.visitor.type_coercion.

Covers individual coercion helpers, TypeCoercionRegistry (including
SQLAlchemy model reflection), and the ASTTypeCoercion visitor.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import Boolean, Column, Integer, String
from sqlalchemy.orm import DeclarativeBase

from fastrest_core.exception.query import CoercionError
from fastrest_core.query.type import (
    AndNode,
    ComparisonNode,
    NotNode,
    Operation,
    OrNode,
)
from fastrest_core.query.visitor.type_coercion import (
    ASTTypeCoercion,
    TypeCoercionRegistry,
    coerce_boolean,
    coerce_datetime,
    coerce_float,
    coerce_int,
    coerce_list,
)
from fastrest_sa import registry_from_sa_model


# ── coerce_boolean ─────────────────────────────────────────────────────────────


def test_coerce_boolean_native_true():
    assert coerce_boolean(True) is True


def test_coerce_boolean_native_false():
    assert coerce_boolean(False) is False


def test_coerce_boolean_string_true():
    for s in ("true", "True", "TRUE", "1", "yes", "YES"):
        assert coerce_boolean(s) is True, f"Expected True for {s!r}"


def test_coerce_boolean_string_false():
    for s in ("false", "0", "no", "anything_else"):
        assert coerce_boolean(s) is False, f"Expected False for {s!r}"


def test_coerce_boolean_int_truthy():
    assert coerce_boolean(1) is True
    assert coerce_boolean(0) is False


# ── coerce_datetime ────────────────────────────────────────────────────────────


def test_coerce_datetime_passthrough():
    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert coerce_datetime(dt) is dt


def test_coerce_datetime_iso_string():
    result = coerce_datetime("2024-01-01T12:00:00")
    assert isinstance(result, datetime)
    assert result.year == 2024


def test_coerce_datetime_invalid_string_raises():
    with pytest.raises(CoercionError, match="ISO-8601"):
        coerce_datetime("not-a-date")


def test_coerce_datetime_non_string_raises():
    with pytest.raises(CoercionError):
        coerce_datetime(12345)


# ── coerce_float ───────────────────────────────────────────────────────────────


def test_coerce_float_int():
    assert coerce_float(3) == pytest.approx(3.0)


def test_coerce_float_string():
    assert coerce_float("3.14") == pytest.approx(3.14)


def test_coerce_float_invalid_raises():
    with pytest.raises(CoercionError):
        coerce_float("abc")


# ── coerce_int ─────────────────────────────────────────────────────────────────


def test_coerce_int_string():
    assert coerce_int("42") == 42


def test_coerce_int_float_truncates():
    assert coerce_int(3.7) == 3


def test_coerce_int_invalid_raises():
    with pytest.raises(CoercionError):
        coerce_int("abc")


# ── coerce_list ────────────────────────────────────────────────────────────────


def test_coerce_list_native_passthrough():
    lst = [1, 2, 3]
    assert coerce_list(lst) is lst


def test_coerce_list_json_array_string():
    result = coerce_list('["a","b","c"]')
    assert result == ["a", "b", "c"]


def test_coerce_list_bracket_wrapped():
    result = coerce_list("[foo,bar]")
    assert result == ["foo", "bar"]


def test_coerce_list_comma_separated():
    result = coerce_list("a,b,c")
    assert result == ["a", "b", "c"]


def test_coerce_list_single_value():
    result = coerce_list("only")
    assert result == ["only"]


def test_coerce_list_empty_brackets():
    result = coerce_list("[]")
    assert result == []


def test_coerce_list_from_tuple():
    result = coerce_list((1, 2))
    assert result == [1, 2]


# ── TypeCoercionRegistry ───────────────────────────────────────────────────────


def test_registry_register_and_get():
    reg = TypeCoercionRegistry()
    reg.register_field("age", int, coerce_int)
    info = reg.get("age")
    assert info is not None
    assert info.python_type is int
    assert info.coercer is coerce_int


def test_registry_get_unknown_returns_none():
    reg = TypeCoercionRegistry()
    assert reg.get("nonexistent") is None


def test_registry_register_replaces_existing():
    reg = TypeCoercionRegistry()
    reg.register_field("x", int, coerce_int)
    reg.register_field("x", float, coerce_float)  # override
    info = reg.get("x")
    assert info.python_type is float


def test_registry_from_sqlalchemy_model():
    """get_default_from_sqlalchemy_model populates registry from SA column types."""

    class _TestBase(DeclarativeBase):
        pass

    class _UserORM(_TestBase):
        __tablename__ = "users_coercion"
        id = Column(Integer, primary_key=True)
        active = Column(Boolean)
        name = Column(String)

    reg = registry_from_sa_model(_UserORM)

    # bool should have a coercer; str typically doesn't
    bool_info = reg.get("active")
    assert bool_info is not None
    assert bool_info.python_type is bool


# ── ASTTypeCoercion visitor ────────────────────────────────────────────────────


def _reg_with_int(field: str) -> TypeCoercionRegistry:
    reg = TypeCoercionRegistry()
    reg.register_field(field, int, coerce_int)
    return reg


def test_coercion_visitor_coerces_comparison_value():
    reg = _reg_with_int("age")
    node = ComparisonNode(field="age", op=Operation.EQUAL, value="25")
    result = ASTTypeCoercion(reg).visit(node)
    assert isinstance(result, ComparisonNode)
    assert result.value == 25


def test_coercion_visitor_skips_unregistered_field():
    reg = _reg_with_int("age")
    node = ComparisonNode(field="name", op=Operation.EQUAL, value="Alice")
    result = ASTTypeCoercion(reg).visit(node)
    assert result is node  # unchanged


def test_coercion_visitor_skips_none_value():
    reg = _reg_with_int("age")
    node = ComparisonNode(field="age", op=Operation.IS_NULL)
    result = ASTTypeCoercion(reg).visit(node)
    assert result is node  # value is None, nothing to coerce


def test_coercion_visitor_coerces_in_list():
    reg = _reg_with_int("tier")
    node = ComparisonNode(field="tier", op=Operation.IN, value=["1", "2", "3"])
    result = ASTTypeCoercion(reg).visit(node)
    assert result.value == [1, 2, 3]


def test_coercion_visitor_recurses_and():
    reg = _reg_with_int("age")
    left = ComparisonNode(field="age", op=Operation.EQUAL, value="18")
    right = ComparisonNode(field="name", op=Operation.EQUAL, value="Bob")
    node = AndNode(left=left, right=right)
    result = ASTTypeCoercion(reg).visit(node)
    assert isinstance(result, AndNode)
    assert result.left.value == 18  # coerced
    assert result.right.value == "Bob"  # unchanged (not in registry)


def test_coercion_visitor_recurses_or():
    reg = _reg_with_int("x")
    left = ComparisonNode(field="x", op=Operation.EQUAL, value="5")
    right = ComparisonNode(field="x", op=Operation.EQUAL, value="10")
    node = OrNode(left=left, right=right)
    result = ASTTypeCoercion(reg).visit(node)
    assert isinstance(result, OrNode)
    assert result.left.value == 5
    assert result.right.value == 10


def test_coercion_visitor_recurses_not():
    reg = _reg_with_int("score")
    inner = ComparisonNode(field="score", op=Operation.GREATER_THAN, value="100")
    node = NotNode(child=inner)
    result = ASTTypeCoercion(reg).visit(node)
    assert isinstance(result, NotNode)
    assert result.child.value == 100


def test_coercion_visitor_empty_registry_passes_through():
    """No coercers registered → all nodes pass through unchanged."""
    node = ComparisonNode(field="age", op=Operation.EQUAL, value="18")
    result = ASTTypeCoercion().visit(node)
    assert result is node
