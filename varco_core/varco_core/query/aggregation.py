"""
varco_core.query.aggregation
=============================
Aggregation query AST for the varco query system.

Problem
-------
The existing query system handles filter / sort / pagination but not
aggregations.  Analytics endpoints need COUNT, SUM, AVG, MIN, MAX, and
GROUP BY — computations that reduce N entity rows into M group rows.

Design
------
``AggregationQuery`` is a **separate** dataclass from ``QueryParams`` — not
an extension of it.  The two have fundamentally different semantics:

    QueryParams       → returns N entity rows (one per entity)
    AggregationQuery  → returns M group rows (one per group, M ≤ N)

Merging them would force callers to inspect nullable ``group_by`` / ``agg``
fields on every query object, violating single responsibility.

The HAVING clause (filter applied after grouping) reuses the existing
``FilterNode`` type hierarchy from ``varco_core.query.type`` — this avoids
duplicating a node hierarchy just for post-group filtering.

Components
----------
``AggregationFunc``
    Enum of supported aggregate functions: COUNT, SUM, AVG, MIN, MAX.

``AggregationExpression``
    Frozen dataclass: ``(func, field, alias)``.
    ``field`` is ``None`` for COUNT(*).

``AggregationQuery``
    Top-level frozen dataclass: ``(group_by, aggregations, having, limit, offset)``.
    ``having`` is a ``FilterNode | None`` — reuses the existing filter AST.

``SQLAlchemyAggregationApplicator``
    Applies an ``AggregationQuery`` to a SQLAlchemy 2.x ``Select`` statement,
    producing a ``SELECT col1, col2, agg_fn(field) AS alias ... GROUP BY ...
    HAVING ...`` query.

Usage::

    from varco_core.query.aggregation import (
        AggregationExpression,
        AggregationFunc,
        AggregationQuery,
        SQLAlchemyAggregationApplicator,
    )
    from sqlalchemy import select

    agg_query = AggregationQuery(
        group_by=("status",),
        aggregations=(
            AggregationExpression(AggregationFunc.COUNT, field=None, alias="count"),
            AggregationExpression(AggregationFunc.SUM, field="amount", alias="total"),
        ),
    )
    stmt = select(OrderModel)
    applicator = SQLAlchemyAggregationApplicator(model_cls=OrderModel)
    stmt = applicator.apply(stmt, agg_query)
    rows = await session.execute(stmt)

Thread safety:  ✅ AST nodes are frozen dataclasses — immutable value objects.
Async safety:   ✅ All applicator methods are synchronous.

📚 Docs
- 🐍 https://docs.python.org/3/library/enum.html
  Python Enum — used for AggregationFunc.
- 🔍 https://docs.sqlalchemy.org/en/20/core/functions.html
  SQLAlchemy func — aggregate function helpers (func.count, func.sum, etc.).
- 🔍 https://docs.sqlalchemy.org/en/20/core/selectable.html#sqlalchemy.sql.expression.Select.group_by
  SQLAlchemy Select.group_by — GROUP BY clause builder.
- 🔍 https://docs.sqlalchemy.org/en/20/core/selectable.html#sqlalchemy.sql.expression.Select.having
  SQLAlchemy Select.having — HAVING clause builder.
- 📐 https://en.wikipedia.org/wiki/SQL#Queries
  SQL aggregation semantics — GROUP BY / HAVING reference.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from dataclasses import field as dfield
from enum import StrEnum
from typing import TYPE_CHECKING, Any

from sqlalchemy import ColumnElement, Select, func
from sqlalchemy.orm import DeclarativeBase

from varco_core.exception.repository import FieldNotFound

if TYPE_CHECKING:
    # FilterNode is the Union type from query.type — only needed for type hints.
    from varco_core.query.type import ComparisonNode, AndNode, OrNode, NotNode

    FilterNode = ComparisonNode | AndNode | OrNode | NotNode


# ── Security: safe field-name regex ───────────────────────────────────────────
#
# DESIGN: strict allowlist over blocklist — same pattern as the filter visitor.
#   ✅ Prevents __dunder__ access and SQL special characters in GROUP BY fields.
#   ✅ Simple to audit — one regex, one place.
#   ❌ Rejects exotic (but valid) Python identifiers with unicode — acceptable
#      for DB column names which are always ASCII.
_SAFE_FIELD_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_field(field: str, context: str) -> None:
    """
    Reject field names that could be used for attribute-injection attacks.

    Args:
        field:   The field name to validate.
        context: Human-readable context for the error message (e.g. "GROUP BY").

    Raises:
        ValueError: If ``field`` does not match the safe identifier pattern.

    Edge cases:
        - Empty string → fails regex → raises ValueError.
        - Dunder names (``__class__``) → caught here before any getattr.
    """
    if not _SAFE_FIELD_RE.match(field):
        raise ValueError(
            f"{context} field {field!r} contains unsafe characters. "
            "Only [a-zA-Z_][a-zA-Z0-9_]* identifiers are accepted."
        )


# ── AggregationFunc ────────────────────────────────────────────────────────────


class AggregationFunc(StrEnum):
    """
    Aggregate functions supported by the aggregation query AST.

    Attributes:
        COUNT: Count of rows — used with ``field=None`` for COUNT(*) or
               a specific field for COUNT(field) (skips NULLs).
        SUM:   Sum of numeric field values.
        AVG:   Average of numeric field values.
        MIN:   Minimum value in the field.
        MAX:   Maximum value in the field.

    Note: ``DISTINCT`` variants (e.g. ``COUNT DISTINCT``) are not modelled
    here — add them as a ``distinct: bool`` flag on ``AggregationExpression``
    if needed in a future iteration.
    """

    COUNT = "COUNT"
    SUM = "SUM"
    AVG = "AVG"
    MIN = "MIN"
    MAX = "MAX"


# ── AggregationExpression ──────────────────────────────────────────────────────


@dataclass(frozen=True)
class AggregationExpression:
    """
    A single aggregate column expression: ``func(field) AS alias``.

    Immutable — safe to hash, cache, and reuse across queries.

    DESIGN: frozen dataclass over named tuple
        ✅ Field names are explicit and doc-able.
        ✅ ``__post_init__`` allows validation at construction time.
        ✅ Hashable — can be used in sets / dict keys.
        ❌ Slightly more verbose than a tuple literal for inline use.

    Thread safety:  ✅ Frozen — immutable after construction.
    Async safety:   ✅ Value object; no I/O.

    Attributes:
        func:  The aggregate function to apply.
        field: The column name to aggregate.  Use ``None`` for ``COUNT(*)``.
        alias: The output column alias (e.g. ``"total_revenue"``).  Must be a
               non-empty valid identifier.

    Args:
        func:  ``AggregationFunc`` enum value.
        field: Column name, or ``None`` for COUNT(*).
        alias: Output column alias — used as the result dict key.

    Raises:
        ValueError: If ``alias`` is empty or contains unsafe characters.
        ValueError: If ``field`` is provided and contains unsafe characters.
        ValueError: If ``func`` is not COUNT and ``field`` is None
                    (SUM/AVG/MIN/MAX all require a specific field).

    Edge cases:
        - ``func=COUNT, field=None``  → COUNT(*) — valid.
        - ``func=SUM, field=None``    → raises ValueError.
        - ``alias`` is used verbatim as a Python dict key in result rows —
          keep it unique within an ``AggregationQuery``.

    Example::

        AggregationExpression(AggregationFunc.COUNT, field=None, alias="row_count")
        AggregationExpression(AggregationFunc.SUM, field="amount", alias="total")
    """

    func: AggregationFunc
    field: str | None
    alias: str

    def __post_init__(self) -> None:
        if not self.alias:
            raise ValueError("AggregationExpression.alias must be a non-empty string.")
        _validate_field(self.alias, "alias")

        if self.field is not None:
            _validate_field(self.field, "aggregate field")

        # COUNT is the only function that can operate on all rows (*).
        # SUM / AVG / MIN / MAX all require a specific column.
        if self.func != AggregationFunc.COUNT and self.field is None:
            raise ValueError(
                f"AggregationExpression: func={self.func!r} requires a non-None field "
                f"(only COUNT supports field=None for COUNT(*))."
            )


# ── AggregationQuery ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AggregationQuery:
    """
    Top-level aggregation query descriptor.

    Describes a ``GROUP BY ... HAVING ... LIMIT ... OFFSET`` query with one
    or more aggregate expressions.

    DESIGN: separate from QueryParams
        The existing ``QueryParams`` is designed for entity-row queries (one
        row per entity).  Aggregation returns one row per group — a fundamentally
        different cardinality.  A single merged class would require callers to
        check nullable ``group_by``/``aggregations`` fields on every use.

        ✅ Separate dataclass keeps concerns separate.
        ✅ ``having`` reuses the existing ``FilterNode`` type hierarchy — no
           duplicate AST for post-group filtering.
        ❌ Callers must instantiate a different class for aggregation queries.

    Thread safety:  ✅ Frozen — immutable.
    Async safety:   ✅ Value object.

    Attributes:
        group_by:      Tuple of column names to group by.  Empty tuple → no GROUP BY
                       (computes a single aggregate over all rows).
        aggregations:  Tuple of ``AggregationExpression`` values.  Must be non-empty.
        having:        Optional post-group filter using the existing ``FilterNode``
                       AST.  Translated to a SQLAlchemy HAVING clause.
        limit:         Maximum number of result groups to return.  ``None`` → no limit.
        offset:        Number of result groups to skip.  Default: 0.

    Args:
        group_by:     Column names to group by.  Default: empty tuple (no group).
        aggregations: Aggregate expressions.  Must have at least one.
        having:       Post-group filter node.  Default: ``None``.
        limit:        Result cap.  Default: ``None`` (unlimited).
        offset:       Result skip count.  Default: 0.

    Raises:
        ValueError: If ``aggregations`` is empty.

    Edge cases:
        - ``group_by=()`` → single-row aggregate (e.g. ``SELECT COUNT(*) FROM t``).
        - ``having`` is applied AFTER GROUP BY — it cannot reference raw (non-grouped,
          non-aggregated) columns.  The applicator does not validate this — SQL
          will raise at execution time if the expression is invalid.
        - ``limit=0`` is passed through to the backend unchanged — SQL behaviour
          (returns 0 rows) is the expected result.

    Example::

        AggregationQuery(
            group_by=("status",),
            aggregations=(
                AggregationExpression(AggregationFunc.COUNT, None, "count"),
                AggregationExpression(AggregationFunc.SUM, "amount", "total"),
            ),
            having=ComparisonNode("count", Operation.GREATER_THAN, 0),
            limit=100,
        )
    """

    group_by: tuple[str, ...] = dfield(default_factory=tuple)
    aggregations: tuple[AggregationExpression, ...] = dfield(default_factory=tuple)
    having: Any | None = None  # FilterNode | None — Any avoids circular import
    limit: int | None = None
    offset: int = 0

    def __post_init__(self) -> None:
        if not self.aggregations:
            raise ValueError(
                "AggregationQuery.aggregations must contain at least one expression. "
                "Use AggregationExpression to define what to compute."
            )
        for col in self.group_by:
            _validate_field(col, "GROUP BY")


# ── SQLAlchemyAggregationApplicator ───────────────────────────────────────────


class SQLAlchemyAggregationApplicator:
    """
    Applies an ``AggregationQuery`` to a SQLAlchemy 2.x ``Select`` statement.

    Translates the query's ``group_by``, ``aggregations``, ``having``,
    ``limit``, and ``offset`` into the corresponding SQLAlchemy clauses.

    DESIGN: applicator class over standalone functions
        ✅ Consistent with ``SQLAlchemyQueryApplicator`` — same usage pattern.
        ✅ Stateless after construction — safe to share across requests.
        ✅ ``model_cls`` is injected at construction — column resolution is
           done once, not per-apply call.
        ❌ Extra allocation vs. standalone function — negligible for I/O-bound DB calls.

    Thread safety:  ✅ Stateless after construction — no mutable state.
    Async safety:   ✅ Synchronous — returns a modified ``Select`` object.

    Args:
        model_cls: SQLAlchemy declarative model class for column resolution.

    Edge cases:
        - Unknown ``group_by`` field name → ``FieldNotFound``.
        - Unknown aggregate ``field`` name → ``FieldNotFound``.
        - ``having`` is translated using ``SQLAlchemyQueryCompiler`` — all
          HAVING filter constraints apply (allowed_fields, safe paths, etc.).
        - The returned ``Select`` is a new object — the input ``stmt`` is not
          mutated.

    Example::

        applicator = SQLAlchemyAggregationApplicator(model_cls=OrderModel)
        stmt = select(OrderModel)
        stmt = applicator.apply(stmt, agg_query)
        rows = await session.execute(stmt)
        result = [dict(row) for row in rows.mappings()]
    """

    def __init__(self, model_cls: type[DeclarativeBase]) -> None:
        """
        Args:
            model_cls: Mapped SQLAlchemy ORM model class (not an instance).
        """
        self._model_cls = model_cls

    def apply(self, stmt: Select, agg_query: AggregationQuery) -> Select:
        """
        Apply the aggregation query to a ``Select`` statement.

        The pipeline is:
        1. Replace the SELECT columns with group-by columns + aggregate exprs.
        2. Add GROUP BY clause.
        3. Add HAVING clause (if provided).
        4. Add LIMIT / OFFSET.

        Args:
            stmt:      Base ``Select`` statement (typically ``select(ModelClass)``).
            agg_query: The aggregation query descriptor.

        Returns:
            A new ``Select`` statement with GROUP BY, aggregates, HAVING,
            LIMIT, and OFFSET applied.

        Raises:
            FieldNotFound: If a ``group_by`` column or aggregate ``field`` does not
                           exist on ``model_cls``.

        Thread safety:  ✅ Stateless — no mutations; ``Select`` is immutable.
        Async safety:   ✅ Synchronous.

        Edge cases:
            - ``agg_query.group_by = ()`` → no GROUP BY clause — produces a
              single-row global aggregate.
            - ``agg_query.having = None`` → no HAVING clause added.
            - ``stmt`` may already have a WHERE clause — it is preserved.
        """
        # Build the list of SELECT-level column expressions.
        # Order: group-by columns first, then aggregate expressions.
        select_cols: list[ColumnElement[Any]] = []

        # ── Group-by columns ──────────────────────────────────────────────────
        group_cols: list[ColumnElement[Any]] = []
        for col_name in agg_query.group_by:
            col = self._resolve_column(col_name)
            select_cols.append(col)
            group_cols.append(col)

        # ── Aggregate expressions ─────────────────────────────────────────────
        for agg_expr in agg_query.aggregations:
            agg_col = self._build_agg_column(agg_expr)
            select_cols.append(agg_col)

        # Replace the original SELECT columns with our computed set.
        # ``with_only_columns`` returns a new Select — the original is unchanged.
        stmt = stmt.with_only_columns(*select_cols)

        # ── GROUP BY ──────────────────────────────────────────────────────────
        if group_cols:
            stmt = stmt.group_by(*group_cols)

        # ── HAVING ───────────────────────────────────────────────────────────
        if agg_query.having is not None:
            having_expr = self._compile_filter(agg_query.having)
            stmt = stmt.having(having_expr)

        # ── LIMIT / OFFSET ────────────────────────────────────────────────────
        if agg_query.limit is not None:
            stmt = stmt.limit(agg_query.limit)
        if agg_query.offset:
            stmt = stmt.offset(agg_query.offset)

        return stmt

    # ── Internal helpers ───────────────────────────────────────────────────────

    def _resolve_column(self, field: str) -> ColumnElement[Any]:
        """
        Resolve a field name to a SQLAlchemy ``MappedColumn`` on ``model_cls``.

        Args:
            field: Column name to resolve.

        Returns:
            The SQLAlchemy column element for the field.

        Raises:
            FieldNotFound: If the column does not exist on ``model_cls``.

        Edge cases:
            - Field was already validated against ``_SAFE_FIELD_RE`` in
              ``AggregationQuery.__post_init__`` and
              ``AggregationExpression.__post_init__``.  No second validation here.
        """
        col = getattr(self._model_cls, field, None)
        if col is None:
            raise FieldNotFound(
                field,
                self._model_cls.__tablename__,  # type: ignore[attr-defined]
            )
        return col

    def _build_agg_column(self, expr: AggregationExpression) -> ColumnElement[Any]:
        """
        Build a labelled SQLAlchemy aggregate expression for one ``AggregationExpression``.

        Args:
            expr: The aggregate expression descriptor.

        Returns:
            A labelled SQLAlchemy ``ColumnElement``.

        Raises:
            FieldNotFound: If ``expr.field`` is not None and the column is not found.
        """
        # Resolve the target column, or None for COUNT(*).
        if expr.field is not None:
            target = self._resolve_column(expr.field)
        else:
            # COUNT(*) — use SQLAlchemy's func.count() with no argument.
            target = None

        # Build the aggregate expression based on the func enum.
        # DESIGN: explicit if/elif over a dispatch dict — it is exhaustive,
        # readable, and the enum is small enough that a dict adds no value.
        match expr.func:
            case AggregationFunc.COUNT:
                # func.count(col) → COUNT(col); func.count() → COUNT(*)
                agg = func.count(target) if target is not None else func.count()
            case AggregationFunc.SUM:
                agg = func.sum(target)
            case AggregationFunc.AVG:
                agg = func.avg(target)
            case AggregationFunc.MIN:
                agg = func.min(target)
            case AggregationFunc.MAX:
                agg = func.max(target)
            case _:
                # Defensive branch — AggregationFunc is exhaustive, but future
                # enum additions before updating this match will hit here.
                raise ValueError(
                    f"Unsupported AggregationFunc: {expr.func!r}. "
                    "Update SQLAlchemyAggregationApplicator._build_agg_column."
                )

        # Label the expression so result rows can be accessed by alias name.
        return agg.label(expr.alias)

    def _compile_filter(self, node: Any) -> ColumnElement[Any]:
        """
        Compile a ``FilterNode`` into a SQLAlchemy ``ColumnElement`` for HAVING.

        Delegates to ``SQLAlchemyQueryCompiler`` — reuses the existing filter
        visitor to avoid duplicating comparison logic.

        Args:
            node: A ``FilterNode`` (ComparisonNode, AndNode, OrNode, NotNode).

        Returns:
            A SQLAlchemy ``ColumnElement`` for the HAVING clause.

        Edge cases:
            - The HAVING clause can only reference grouped columns or aggregate
              expressions.  The compiler does not enforce this — the DB raises at
              execution time.
            - Dotted relationship paths in HAVING are not supported — the HAVING
              clause operates on the GROUP BY output, not on joined tables.
        """
        # Import here (not at module level) to avoid circular imports with the
        # visitor module, which imports from query.type.
        from varco_core.query.visitor.sqlalchemy import SQLAlchemyQueryCompiler

        compiler = SQLAlchemyQueryCompiler(self._model_cls)
        return compiler.visit(node)

    def _column_names(self) -> list[str]:
        """
        Return a list of column names on ``model_cls`` for use in error messages.

        Returns:
            Sorted list of column attribute names (excludes relationships).
        """
        from sqlalchemy import inspect as sa_inspect

        try:
            mapper = sa_inspect(self._model_cls)
            return sorted(c.key for c in mapper.columns)
        except Exception:
            # Inspection may fail for unmapped classes in tests — return empty.
            return []


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "AggregationFunc",
    "AggregationExpression",
    "AggregationQuery",
    "SQLAlchemyAggregationApplicator",
]
