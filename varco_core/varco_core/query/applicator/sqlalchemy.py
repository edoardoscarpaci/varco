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

from varco_core.exception.query import OperationNotSupported
from varco_core.exception.repository import FieldNotFound
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
        Apply an AST filter node as a ``WHERE`` clause to ``query``.

        Args:
            query: A SQLAlchemy 2.x ``Select`` statement.
            node:  Root AST node (``ComparisonNode``, ``AndNode``, …).
            args:  Unused.
            kwargs: Unused.

        Returns:
            A new ``Select`` statement with the filter applied.
        """
        filter_expr = self.query_visitor.visit(node)
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
        Apply ``ORDER BY`` clauses to ``query``.

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
            OperationNotSupported: Dotted path in ``sort_field.field``.
            FieldNotFound:         Field does not exist on the model.
        """
        if not sort_fields:
            return query

        for sort in sort_fields:
            field_name = sort.field

            if self.allowed_fields and field_name not in self.allowed_fields:
                raise ValueError(
                    f"Cannot sort by {field_name!r}: not in allowed_fields whitelist. "
                    f"Allowed: {sorted(self.allowed_fields)}"
                )

            col = self._resolve_column(field_name)
            query = query.order_by(
                desc(col) if sort.order == SortOrder.DESC else asc(col)
            )

        return query

    # ── Column resolution ──────────────────────────────────────────────────────

    def _resolve_column(self, field_path: str) -> Any:
        """
        Resolve a top-level column attribute on the model.

        Args:
            field_path: Simple column name (no dots).

        Returns:
            The mapped column attribute.

        Raises:
            OperationNotSupported: ``field_path`` contains a dot.
            FieldNotFound:         Attribute missing from the model.
        """
        if "." in field_path:
            raise OperationNotSupported(
                f"Nested relationship traversal is not supported in sort ({field_path!r})"
            )
        try:
            return getattr(self.model_cls, field_path)
        except AttributeError as exc:
            raise FieldNotFound(
                field=field_path,
                table=self.model_cls.__tablename__,
            ) from exc
