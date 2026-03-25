"""
varco_core.query.applicator.sqlalchemy
==========================================
SQLAlchemy-specific ``QueryApplicator`` that applies AST nodes, sort directives,
and pagination to a SQLAlchemy 2.x ``Select`` statement.

DESIGN: separate applicator class over inline repository logic
  ✅ Testable in isolation — no session or mapper required.
  ✅ Swappable via dependency injection in providers.
  ❌ Extra allocation per query — acceptable for I/O-bound DB calls.

Thread safety:  ✅ Stateless after construction.
Async safety:   ✅ Synchronous — the ``Select`` object is immutable; no session
                   is touched here.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import Select, asc, desc
from sqlalchemy.orm import DeclarativeBase

from varco_core.query.applicator.applicator import QueryApplicator
from varco_core.query.type import SortField, SortOrder, TransformerNode
from varco_core.query.visitor.sqlalchemy import SQLAlchemyQueryCompiler


class SQLAlchemyQueryApplicator(QueryApplicator):
    """
    Applies an AST filter, sort, and pagination to a SQLAlchemy 2.x
    ``Select`` statement.

    Uses ``SQLAlchemyQueryCompiler`` internally to translate the AST into
    ``ColumnElement`` expressions.

    DESIGN: ``@Singleton`` was intentionally removed.
      The old code used ``providify.Singleton`` which created one shared
      instance for the entire process.  That is wrong here because each
      repository uses a different ``model_cls`` — a shared singleton would
      point at the wrong table.  Each repository creates its own applicator.

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ Synchronous.

    Args:
        model_cls:       Mapped SQLAlchemy ORM model class (not an instance).
        query_compiler:  Optional pre-configured ``SQLAlchemyQueryCompiler``.
                         Provide when you want to share a compiler (e.g. to
                         pre-populate ``allowed_fields``).  Defaults to a new
                         compiler for ``model_cls``.
        allowed_fields:  Column-name whitelist.  Propagated to the compiler when
                         the compiler's own whitelist is empty.

    Edge cases:
        - Passing a ``query_compiler`` whose ``allowed_fields`` is already set
          takes precedence over the ``allowed_fields`` kwarg.
        - ``apply_sort`` with an unknown field raises ``FieldNotFound``.
        - ``apply_sort`` with a dotted path raises ``OperationNotSupported``.
    """

    def __init__(
        self,
        model_cls: type[DeclarativeBase],
        *args: Any,
        query_compiler: SQLAlchemyQueryCompiler | None = None,
        allowed_fields: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialise the applicator.

        Args:
            model_cls:      Mapped SA model class (used for column lookup).
            query_compiler: Optional pre-built compiler; a new one is created
                            when omitted.
            allowed_fields: Column-name whitelist — see class docstring.
            args:           Forwarded to ``QueryApplicator.__init__``.
            kwargs:         Forwarded to ``QueryApplicator.__init__``.

        Raises:
            ValueError: ``query_compiler`` is provided but is not a
                        ``SQLAlchemyQueryCompiler`` instance.
        """
        self.model_cls = model_cls

        if query_compiler is not None and not isinstance(
            query_compiler, SQLAlchemyQueryCompiler
        ):
            raise ValueError(
                "query_compiler must be an instance of SQLAlchemyQueryCompiler "
                f"(got {type(query_compiler).__name__!r})"
            )
        self.query_visitor: SQLAlchemyQueryCompiler = (
            query_compiler or SQLAlchemyQueryCompiler(model=model_cls)
        )

        # Propagate allowed_fields to the compiler when it has none of its own
        if self.query_visitor.is_allowed_fields_empty() and allowed_fields:
            self.query_visitor.set_allowed_fields(allowed_fields)

        super().__init__(*args, allowed_fields=allowed_fields, **kwargs)

    # ── QueryApplicator implementation ─────────────────────────────────────────

    def apply_query(
        self,
        query: Select,  # type: ignore[override]
        node: TransformerNode,
        *args: Any,
        **kwargs: Any,
    ) -> Select:
        """
        Apply an AST filter node as ``JOIN`` + ``WHERE`` clauses to ``query``.

        When the AST contains dotted relationship paths (e.g. ``"author.name"``),
        the compiler accumulates the required SA relationship attributes in
        ``_pending_joins``.  This method applies those JOINs *before* the
        ``WHERE`` clause so SQLAlchemy can resolve the joined columns correctly.

        DESIGN: JOINs applied here, not in the compiler
          ✅ Compiler returns a ``ColumnElement`` — changing that return type
             would require altering ``BinaryWalkingVisitor`` and all subclasses.
          ✅ The applicator owns the ``Select`` object and is the natural place
             to add structural query changes (JOINs, pagination, sort).
          ❌ Tighter coupling between applicator and compiler — mitigated by the
             explicit lifecycle contract (clear → visit → collect → join → where).

        Args:
            query: A SQLAlchemy 2.x ``Select`` statement.
            node:  Root AST node (``ComparisonNode``, ``AndNode``, …).
            args:  Unused.
            kwargs: Unused.

        Returns:
            A new ``Select`` statement with any required JOINs and the
            ``WHERE`` clause applied.
        """
        # Reset join tracking before compilation so stale joins from a
        # previous call on the same compiler instance do not bleed in
        self.query_visitor._clear_pending_joins()

        filter_expr = self.query_visitor.visit(node)

        # Apply any JOINs collected during the walk in traversal order.
        # Each element is a SA relationship attribute (e.g. PostORM.author).
        # SA's join(rel_attr) form resolves the ON clause from the relationship
        # definition — no manual join condition needed.
        for rel_attr in self.query_visitor.collect_pending_joins():
            query = query.join(rel_attr)

        # Use .where() — the SQLAlchemy 2.x API; .filter() is legacy ORM only
        return query.where(filter_expr)

    def apply_pagination(
        self,
        query: Select,  # type: ignore[override]
        limit: int | None,
        offset: int | None,
        *args: Any,
        **kwargs: Any,
    ) -> Select:
        """
        Apply ``LIMIT`` and ``OFFSET`` to ``query``.

        Args:
            query:  A SQLAlchemy 2.x ``Select`` statement.
            limit:  Maximum rows.  ``None`` → no LIMIT applied.
            offset: Rows to skip.  ``None`` → no OFFSET applied.
            args:   Unused.
            kwargs: Unused.

        Returns:
            Modified ``Select`` statement.
        """
        if limit is not None:
            query = query.limit(limit)
        if offset is not None:
            query = query.offset(offset)
        return query

    def apply_sort(
        self,
        query: Select,  # type: ignore[override]
        sort_fields: list[SortField] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> Select:
        """
        Apply ``ORDER BY`` clauses to ``query``, with relationship JOIN support.

        Dotted sort paths (e.g. ``"author.name"``) are resolved through the
        compiler's relationship traversal logic, and any required JOINs are
        added before the ``ORDER BY`` clause.  Joins are deduplicated against
        any joins already applied by a preceding ``apply_query`` call on the
        same compiler instance.

        Args:
            query:       A SQLAlchemy 2.x ``Select`` statement.
            sort_fields: Ordered list of ``SortField`` directives.  ``None``
                         or empty → returns ``query`` unchanged.
            args:        Unused.
            kwargs:      Unused.

        Returns:
            Modified ``Select`` statement.

        Raises:
            ValueError:            Field not in ``allowed_fields`` whitelist.
            OperationNotSupported: Traversal depth exceeded or unknown relationship.
            FieldNotFound:         Field does not exist on the model.
        """
        if not sort_fields:
            return query

        # Reset join tracking so sort-path traversal is isolated from any
        # prior apply_query pass on the same compiler instance.
        # SA deduplicates identical JOINs (same table + condition) at query
        # compilation time, so double-joining the same relationship is safe.
        self.query_visitor._clear_pending_joins()

        # Collect columns first; _resolve_column may populate _pending_joins
        # for dotted sort paths (e.g. "author.name")
        order_cols = []
        for sort in sort_fields:
            field_name = sort.field

            if self.allowed_fields and field_name not in self.allowed_fields:
                raise ValueError(
                    f"Cannot sort by {field_name!r}: not in allowed_fields whitelist. "
                    f"Allowed: {sorted(self.allowed_fields)}"
                )

            col = self._resolve_column(field_name)
            order_cols.append((col, sort.order))

        # Apply any JOINs required for the sort columns before ORDER BY
        for rel_attr in self.query_visitor.collect_pending_joins():
            query = query.join(rel_attr)

        for col, order in order_cols:
            query = query.order_by(desc(col) if order == SortOrder.DESC else asc(col))

        return query

    # ── Column resolution ──────────────────────────────────────────────────────

    def _resolve_column(self, field_path: str) -> Any:
        """
        Resolve a column attribute for sort, delegating to the compiler.

        Dotted relationship paths in sort fields (e.g. ``"author.name"``) are
        resolved via the same compiler logic used for filters, including security
        validation and join tracking.

        Note: sort-generated joins are collected separately after this call in
        ``apply_sort``; the compiler's ``_pending_joins`` state is shared, so
        this must not be called concurrently with ``apply_query`` on the same
        compiler instance.

        Args:
            field_path: Column name or dot-separated relationship path.

        Returns:
            The mapped column attribute.

        Raises:
            OperationNotSupported: Traversal depth exceeded or unknown relationship.
            FieldNotFound:         Attribute missing from the model or leaf model.
            ValueError:            Unsafe segment characters.
        """
        # Delegate to the compiler's resolver so segment validation, depth
        # checks, and join tracking are applied consistently for sort as well
        return self.query_visitor._resolve_column(field_path)
