"""
tests.test_aggregation
======================
Unit tests for varco_core.query.aggregation.

Covers:
    AggregationFunc         — enum values
    AggregationExpression   — construction, validation, immutability
    AggregationQuery        — construction, validation, immutability
    SQLAlchemyAggregationApplicator — GROUP BY, aggregates, HAVING, LIMIT/OFFSET

The applicator tests use SQLAlchemy's in-memory text inspection
(``str(stmt.compile(...))`` → SQL string) to verify the generated SQL without
requiring a live database connection.
"""

from __future__ import annotations

import pytest
from sqlalchemy import String, select, Integer
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from varco_core.exception.repository import FieldNotFound
from varco_core.query.aggregation import (
    AggregationExpression,
    AggregationFunc,
    AggregationQuery,
    SQLAlchemyAggregationApplicator,
)
from varco_core.query.type import ComparisonNode, Operation


# ── Minimal ORM model for testing ─────────────────────────────────────────────


class Base(DeclarativeBase):
    pass


class OrderModel(Base):
    """Minimal ORM model used only in aggregation tests."""

    __tablename__ = "orders"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    status: Mapped[str] = mapped_column(String)
    amount: Mapped[int] = mapped_column(Integer)
    customer_id: Mapped[int] = mapped_column(Integer)


def sql(stmt) -> str:
    """Compile a Select to a SQL string for assertion."""
    return str(stmt.compile(compile_kwargs={"literal_binds": True}))


# ── AggregationFunc ────────────────────────────────────────────────────────────


def test_aggregation_func_values() -> None:
    """All expected AggregationFunc members must exist."""
    assert AggregationFunc.COUNT == "COUNT"
    assert AggregationFunc.SUM == "SUM"
    assert AggregationFunc.AVG == "AVG"
    assert AggregationFunc.MIN == "MIN"
    assert AggregationFunc.MAX == "MAX"


# ── AggregationExpression ──────────────────────────────────────────────────────


def test_aggregation_expression_count_star() -> None:
    """COUNT(*) is valid: func=COUNT, field=None."""
    expr = AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt")
    assert expr.func == AggregationFunc.COUNT
    assert expr.field is None
    assert expr.alias == "cnt"


def test_aggregation_expression_sum_with_field() -> None:
    """SUM with a specific field is valid."""
    expr = AggregationExpression(AggregationFunc.SUM, field="amount", alias="total")
    assert expr.field == "amount"
    assert expr.alias == "total"


def test_aggregation_expression_sum_no_field_raises() -> None:
    """SUM without a field raises ValueError — only COUNT supports field=None."""
    with pytest.raises(ValueError, match="requires a non-None field"):
        AggregationExpression(AggregationFunc.SUM, field=None, alias="bad")


def test_aggregation_expression_empty_alias_raises() -> None:
    """Empty alias raises ValueError."""
    with pytest.raises(ValueError, match="alias"):
        AggregationExpression(AggregationFunc.COUNT, field=None, alias="")


def test_aggregation_expression_unsafe_alias_raises() -> None:
    """Alias with unsafe characters raises ValueError."""
    with pytest.raises(ValueError, match="unsafe characters"):
        AggregationExpression(AggregationFunc.COUNT, field=None, alias="bad-alias")


def test_aggregation_expression_unsafe_field_raises() -> None:
    """Field with unsafe characters raises ValueError."""
    with pytest.raises(ValueError, match="unsafe characters"):
        AggregationExpression(AggregationFunc.SUM, field="bad field", alias="ok")


def test_aggregation_expression_is_frozen() -> None:
    """AggregationExpression is immutable — assignment raises FrozenInstanceError."""
    expr = AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt")
    with pytest.raises(Exception):  # FrozenInstanceError
        expr.alias = "other"  # type: ignore[misc]


def test_aggregation_expression_hashable() -> None:
    """AggregationExpression is hashable — can be used in sets."""
    e1 = AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt")
    e2 = AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt")
    assert e1 == e2
    assert hash(e1) == hash(e2)


# ── AggregationQuery ──────────────────────────────────────────────────────────


def test_aggregation_query_basic_construction() -> None:
    """AggregationQuery can be constructed with minimal arguments."""
    q = AggregationQuery(
        aggregations=(
            AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt"),
        ),
    )
    assert q.group_by == ()
    assert q.having is None
    assert q.limit is None
    assert q.offset == 0


def test_aggregation_query_empty_aggregations_raises() -> None:
    """AggregationQuery with no aggregations raises ValueError."""
    with pytest.raises(ValueError, match="at least one"):
        AggregationQuery(aggregations=())


def test_aggregation_query_unsafe_group_by_field_raises() -> None:
    """Unsafe characters in group_by field name raises ValueError."""
    with pytest.raises(ValueError, match="unsafe characters"):
        AggregationQuery(
            group_by=("bad field",),
            aggregations=(
                AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt"),
            ),
        )


def test_aggregation_query_is_frozen() -> None:
    """AggregationQuery is immutable."""
    q = AggregationQuery(
        aggregations=(
            AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt"),
        ),
    )
    with pytest.raises(Exception):
        q.limit = 10  # type: ignore[misc]


# ── SQLAlchemyAggregationApplicator ───────────────────────────────────────────


def test_applicator_count_star_no_group_by() -> None:
    """
    COUNT(*) with no GROUP BY produces a single-row global count.
    SQL should contain COUNT(*) and no GROUP BY clause.
    """
    agg = AggregationQuery(
        aggregations=(
            AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt"),
        ),
    )
    stmt = select(OrderModel)
    applicator = SQLAlchemyAggregationApplicator(OrderModel)
    result = applicator.apply(stmt, agg)
    q = sql(result)

    assert "count" in q.lower()
    assert "group by" not in q.lower()


def test_applicator_count_with_group_by() -> None:
    """
    GROUP BY with COUNT produces the correct GROUP BY clause.
    """
    agg = AggregationQuery(
        group_by=("status",),
        aggregations=(
            AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt"),
        ),
    )
    stmt = select(OrderModel)
    applicator = SQLAlchemyAggregationApplicator(OrderModel)
    result = applicator.apply(stmt, agg)
    q = sql(result)

    assert "group by" in q.lower()
    assert "status" in q.lower()
    assert "count" in q.lower()


def test_applicator_sum_aggregation() -> None:
    """
    SUM aggregation produces the correct SUM expression in the SELECT list.
    """
    agg = AggregationQuery(
        group_by=("customer_id",),
        aggregations=(
            AggregationExpression(AggregationFunc.SUM, field="amount", alias="total"),
        ),
    )
    stmt = select(OrderModel)
    applicator = SQLAlchemyAggregationApplicator(OrderModel)
    result = applicator.apply(stmt, agg)
    q = sql(result)

    assert "sum" in q.lower()
    assert "amount" in q.lower()
    assert "total" in q.lower()
    assert "group by" in q.lower()


def test_applicator_multiple_aggregations() -> None:
    """
    Multiple aggregate expressions are all included in the SELECT list.
    """
    agg = AggregationQuery(
        group_by=("status",),
        aggregations=(
            AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt"),
            AggregationExpression(AggregationFunc.SUM, field="amount", alias="total"),
            AggregationExpression(AggregationFunc.AVG, field="amount", alias="avg_amt"),
        ),
    )
    stmt = select(OrderModel)
    applicator = SQLAlchemyAggregationApplicator(OrderModel)
    result = applicator.apply(stmt, agg)
    q = sql(result)

    assert "count" in q.lower()
    assert "sum" in q.lower()
    assert "avg" in q.lower()


def test_applicator_having_clause() -> None:
    """
    having= produces a HAVING clause in the SQL.
    """
    agg = AggregationQuery(
        group_by=("status",),
        aggregations=(
            AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt"),
        ),
        having=ComparisonNode(field="id", op=Operation.GREATER_THAN, value=0),
    )
    stmt = select(OrderModel)
    applicator = SQLAlchemyAggregationApplicator(OrderModel)
    result = applicator.apply(stmt, agg)
    q = sql(result)

    assert "having" in q.lower()


def test_applicator_limit_and_offset() -> None:
    """
    limit and offset are both applied to the resulting query.
    """
    agg = AggregationQuery(
        aggregations=(
            AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt"),
        ),
        limit=10,
        offset=5,
    )
    stmt = select(OrderModel)
    applicator = SQLAlchemyAggregationApplicator(OrderModel)
    result = applicator.apply(stmt, agg)
    q = sql(result)

    assert "limit" in q.lower() or "10" in q
    assert "offset" in q.lower() or "5" in q


def test_applicator_unknown_group_by_field_raises() -> None:
    """
    A group_by field that does not exist on the model raises FieldNotFound.
    """
    agg = AggregationQuery(
        group_by=("nonexistent_col",),
        aggregations=(
            AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt"),
        ),
    )
    stmt = select(OrderModel)
    applicator = SQLAlchemyAggregationApplicator(OrderModel)

    with pytest.raises(FieldNotFound):
        applicator.apply(stmt, agg)


def test_applicator_unknown_aggregate_field_raises() -> None:
    """
    An aggregate field that does not exist on the model raises FieldNotFound.
    """
    agg = AggregationQuery(
        aggregations=(
            AggregationExpression(AggregationFunc.SUM, field="nonexistent", alias="s"),
        ),
    )
    stmt = select(OrderModel)
    applicator = SQLAlchemyAggregationApplicator(OrderModel)

    with pytest.raises(FieldNotFound):
        applicator.apply(stmt, agg)


def test_applicator_all_agg_functions() -> None:
    """
    All AggregationFunc variants produce a non-empty SQL without raising.
    """
    applicator = SQLAlchemyAggregationApplicator(OrderModel)

    for fn in AggregationFunc:
        field = None if fn == AggregationFunc.COUNT else "amount"
        agg = AggregationQuery(
            aggregations=(AggregationExpression(fn, field=field, alias="result"),),
        )
        stmt = select(OrderModel)
        result = applicator.apply(stmt, agg)
        q = sql(result)
        assert fn.value.lower() in q.lower(), f"{fn} not found in SQL: {q}"


def test_applicator_does_not_mutate_original_stmt() -> None:
    """
    apply() must return a NEW Select — the original statement must be unchanged.
    """
    agg = AggregationQuery(
        aggregations=(
            AggregationExpression(AggregationFunc.COUNT, field=None, alias="cnt"),
        ),
    )
    stmt = select(OrderModel)
    original_sql = sql(stmt)

    applicator = SQLAlchemyAggregationApplicator(OrderModel)
    applicator.apply(stmt, agg)

    # Original statement is unchanged — SQLAlchemy Select is immutable.
    assert sql(stmt) == original_sql
