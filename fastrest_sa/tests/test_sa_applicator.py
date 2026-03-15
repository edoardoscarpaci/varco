"""
Unit tests for SQLAlchemyQueryApplicator — apply_query, apply_sort, apply_pagination.

No session required — we inspect the generated Select statement's compiled SQL.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column, Integer, String, select
from sqlalchemy.orm import DeclarativeBase

from fastrest_core.exception.query import OperationNotSupported
from fastrest_core.exception.repository import FieldNotFound
from fastrest_core.query.type import (
    AndNode,
    ComparisonNode,
    Operation,
    SortField,
    SortOrder,
)
from fastrest_core.query.visitor.sqlalchemy import SQLAlchemyQueryCompiler
from fastrest_core.query.applicator.sqlalchemy import SQLAlchemyQueryApplicator


# ── Minimal SA model ───────────────────────────────────────────────────────────


class _AppBase(DeclarativeBase):
    pass


class _ProductORM(_AppBase):
    __tablename__ = "products_applicator"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    price = Column(Integer)


# ── Helpers ────────────────────────────────────────────────────────────────────


def _applicator(**kwargs) -> SQLAlchemyQueryApplicator:
    return SQLAlchemyQueryApplicator(_ProductORM, **kwargs)


def _base_stmt():
    return select(_ProductORM)


def _sql(stmt) -> str:
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


def _cmp(field: str, op: Operation, value=None) -> ComparisonNode:
    return ComparisonNode(field=field, op=op, value=value)


# ── apply_query ────────────────────────────────────────────────────────────────


def test_apply_query_adds_where_clause():
    node = _cmp("name", Operation.EQUAL, "Widget")
    stmt = _applicator().apply_query(_base_stmt(), node)
    sql = _sql(stmt)
    assert "WHERE" in sql.upper()
    assert "Widget" in sql


def test_apply_query_and_node():
    left = _cmp("name", Operation.EQUAL, "Widget")
    right = _cmp("price", Operation.GREATER_THAN, 10)
    node = AndNode(left=left, right=right)
    stmt = _applicator().apply_query(_base_stmt(), node)
    sql = _sql(stmt)
    assert "AND" in sql.upper()


def test_apply_query_returns_select():
    node = _cmp("name", Operation.EQUAL, "X")
    result = _applicator().apply_query(_base_stmt(), node)
    assert "SELECT" in _sql(result).upper()


# ── apply_pagination ───────────────────────────────────────────────────────────


def test_apply_pagination_limit():
    stmt = _applicator().apply_pagination(_base_stmt(), limit=10, offset=None)
    sql = _sql(stmt)
    assert "LIMIT" in sql.upper()
    assert "10" in sql


def test_apply_pagination_offset():
    stmt = _applicator().apply_pagination(_base_stmt(), limit=None, offset=5)
    sql = _sql(stmt)
    assert "OFFSET" in sql.upper()
    assert "5" in sql


def test_apply_pagination_both():
    stmt = _applicator().apply_pagination(_base_stmt(), limit=20, offset=40)
    sql = _sql(stmt)
    assert "20" in sql
    assert "40" in sql


def test_apply_pagination_none_limit_not_added():
    stmt = _applicator().apply_pagination(_base_stmt(), limit=None, offset=None)
    sql = _sql(stmt)
    assert "LIMIT" not in sql.upper()
    assert "OFFSET" not in sql.upper()


# ── apply_sort ─────────────────────────────────────────────────────────────────


def test_apply_sort_asc():
    fields = [SortField(field="name", order=SortOrder.ASC)]
    stmt = _applicator().apply_sort(_base_stmt(), fields)
    sql = _sql(stmt)
    assert "ORDER BY" in sql.upper()
    assert "ASC" in sql.upper()


def test_apply_sort_desc():
    fields = [SortField(field="price", order=SortOrder.DESC)]
    stmt = _applicator().apply_sort(_base_stmt(), fields)
    sql = _sql(stmt)
    assert "DESC" in sql.upper()


def test_apply_sort_multiple_fields():
    fields = [
        SortField(field="name", order=SortOrder.ASC),
        SortField(field="price", order=SortOrder.DESC),
    ]
    stmt = _applicator().apply_sort(_base_stmt(), fields)
    sql = _sql(stmt)
    assert "ORDER BY" in sql.upper()


def test_apply_sort_none_returns_unchanged():
    stmt = _base_stmt()
    result = _applicator().apply_sort(stmt, None)
    assert _sql(result) == _sql(stmt)


def test_apply_sort_empty_list_returns_unchanged():
    stmt = _base_stmt()
    result = _applicator().apply_sort(stmt, [])
    assert _sql(result) == _sql(stmt)


def test_apply_sort_unknown_field_raises_field_not_found():
    fields = [SortField(field="nonexistent", order=SortOrder.ASC)]
    with pytest.raises(FieldNotFound):
        _applicator().apply_sort(_base_stmt(), fields)


def test_apply_sort_dotted_path_raises_operation_not_supported():
    fields = [SortField(field="a.b", order=SortOrder.ASC)]
    with pytest.raises(OperationNotSupported):
        _applicator().apply_sort(_base_stmt(), fields)


def test_apply_sort_allowed_fields_blocks_disallowed():
    fields = [SortField(field="price", order=SortOrder.ASC)]
    applicator = _applicator(allowed_fields={"name"})
    with pytest.raises(ValueError, match="allowed"):
        applicator.apply_sort(_base_stmt(), fields)


# ── Pre-built compiler ─────────────────────────────────────────────────────────


def test_custom_compiler_accepted():
    compiler = SQLAlchemyQueryCompiler(_ProductORM)
    applicator = SQLAlchemyQueryApplicator(_ProductORM, query_compiler=compiler)
    node = _cmp("name", Operation.EQUAL, "X")
    stmt = applicator.apply_query(_base_stmt(), node)
    assert "WHERE" in _sql(stmt).upper()


def test_wrong_compiler_type_raises():
    with pytest.raises(ValueError, match="SQLAlchemyQueryCompiler"):
        SQLAlchemyQueryApplicator(_ProductORM, query_compiler="not_a_compiler")  # type: ignore[arg-type]
