"""
Unit tests for SQLAlchemyQueryApplicator — apply_query, apply_sort, apply_pagination.

No session required — we inspect the generated Select statement's compiled SQL.
"""

from __future__ import annotations

import pytest
from sqlalchemy import Column, ForeignKey, Integer, String, select
from sqlalchemy.orm import DeclarativeBase, relationship

from varco_core.exception.query import OperationNotSupported
from varco_core.exception.repository import FieldNotFound
from varco_core.query.type import (
    AndNode,
    ComparisonNode,
    Operation,
    SortField,
    SortOrder,
)
from varco_core.query.visitor.sqlalchemy import SQLAlchemyQueryCompiler
from varco_core.query.applicator.sqlalchemy import SQLAlchemyQueryApplicator


# ── Minimal SA model — flat ────────────────────────────────────────────────────


class _AppBase(DeclarativeBase):
    pass


class _ProductORM(_AppBase):
    __tablename__ = "products_applicator"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    price = Column(Integer)


# ── Minimal SA models — with relationship ─────────────────────────────────────


class _AppJoinBase(DeclarativeBase):
    pass


class _SupplierORM(_AppJoinBase):
    __tablename__ = "suppliers_applicator"
    id = Column(Integer, primary_key=True)
    country = Column(String)
    products = relationship("_OrderORM", back_populates="supplier")


class _OrderORM(_AppJoinBase):
    __tablename__ = "orders_applicator"
    id = Column(Integer, primary_key=True)
    title = Column(String)
    price = Column(Integer)
    supplier_id = Column(Integer, ForeignKey("suppliers_applicator.id"))
    supplier = relationship("_SupplierORM", back_populates="products")


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


# ── Applicator join traversal ─────────────────────────────────────────────────


def _order_applicator(**kwargs) -> SQLAlchemyQueryApplicator:
    return SQLAlchemyQueryApplicator(_OrderORM, **kwargs)


def _order_base_stmt():
    return select(_OrderORM)


def _join_cmp(field: str, op: Operation, value=None) -> ComparisonNode:
    return ComparisonNode(field=field, op=op, value=value)


class TestApplicatorJoinTraversal:
    """
    Verify that apply_query adds JOIN clauses when dotted paths are present
    and that the generated SQL references the joined table.
    """

    def test_apply_query_adds_join_for_dotted_path(self):
        """
        Filtering on ``"supplier.country"`` must produce a JOIN to the
        suppliers table in the generated SQL.
        """
        node = _join_cmp("supplier.country", Operation.EQUAL, "DE")
        stmt = _order_applicator().apply_query(_order_base_stmt(), node)
        sql = _sql(stmt)
        assert "suppliers_applicator" in sql
        assert "JOIN" in sql.upper()
        assert "DE" in sql

    def test_apply_query_deduplicates_same_join_in_and_node(self):
        """
        Two clauses on the same relationship (``supplier.country`` AND
        ``supplier.id``) must produce exactly one JOIN in the SQL.
        """
        left = _join_cmp("supplier.country", Operation.EQUAL, "DE")
        right = _join_cmp("supplier.id", Operation.EQUAL, 5)
        node = AndNode(left=left, right=right)
        stmt = _order_applicator().apply_query(_order_base_stmt(), node)
        sql = _sql(stmt)
        # Count JOIN occurrences — should be 1
        assert sql.upper().count("JOIN") == 1

    def test_apply_query_resets_joins_between_calls(self):
        """
        Two successive apply_query calls on the same applicator must not
        accumulate stale joins from the first call.
        """
        app = _order_applicator()
        node = _join_cmp("supplier.country", Operation.EQUAL, "DE")
        # First call
        stmt1 = app.apply_query(_order_base_stmt(), node)
        # Second call — must produce identical SQL, not double-joined
        stmt2 = app.apply_query(_order_base_stmt(), node)
        assert _sql(stmt1) == _sql(stmt2)

    def test_apply_query_flat_field_no_join(self):
        """A flat field filter must not produce any JOIN."""
        node = _join_cmp("title", Operation.EQUAL, "Widget")
        stmt = _order_applicator().apply_query(_order_base_stmt(), node)
        sql = _sql(stmt)
        assert "JOIN" not in sql.upper()

    def test_apply_sort_adds_join_for_dotted_sort_field(self):
        """
        Sorting by ``"supplier.country"`` must JOIN the suppliers table
        and add an ORDER BY on the country column.
        """
        sort = [SortField(field="supplier.country", order=SortOrder.ASC)]
        stmt = _order_applicator().apply_sort(_order_base_stmt(), sort)
        sql = _sql(stmt)
        assert "JOIN" in sql.upper()
        assert "ORDER BY" in sql.upper()
        assert "country" in sql
