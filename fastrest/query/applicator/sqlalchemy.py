from typing import List, Optional
from sqlalchemy.orm import Query, DeclarativeBase, MappedColumn
from sqlalchemy import asc, desc
from fastrest.query.applicator.applicator import QueryApplicator
from fastrest.query.type import TransformerNode, SortField
from fastrest.exception.query import OperationNotSupported
from fastrest.exception.repository import FieldNotFound
from fastrest.query.visitor.sqlalchemy import SQLAlchemyQueryCompiler


class SQLAlchemyQueryApplicator(QueryApplicator):
    def __init__(
        self,
        model_cls: DeclarativeBase,
        *args,
        query_compiler: Optional[SQLAlchemyQueryCompiler] = None,
        **kwargs,
    ):
        self.model_cls = model_cls
        if query_compiler is not None and not isinstance(
            query_compiler, SQLAlchemyQueryCompiler
        ):
            raise ValueError(
                "query_compiler must be an instance of SQLAlchemyQueryCompiler or its one of its subclasses"
            )
        self.query_visitor = query_compiler or SQLAlchemyQueryCompiler(model=model_cls)
        if self.query_visitor.is_allowed_fields_empty():
            self.query_visitor.set_allowed_fields(allowed_fields=self.allowed_fields)

        super().__init__(*args, **kwargs)

    def apply_query(
        self, query: Query, node: TransformerNode, *args, **kwargs
    ) -> Query:
        filter_expr = self.query_visitor.visit(node)
        return query.filter(filter_expr)

    def apply_pagination(
        self, query: Query, limit: Optional[int], offset: Optional[int], *args, **kwargs
    ):
        """Apply limit and offset to the query object"""
        if limit is not None:
            query = query.limit(limit)
        if offset is not None:
            query = query.offset(offset)
        return query

    def apply_sort(
        self, query: Query, sort_fields: List[SortField] = list(), *args, **kwargs
    ):
        for sort in sort_fields:
            field_name = sort.field
            order = sort.order.value.lower()
            if self.allowed_fields and field_name not in self.allowed_fields:
                raise ValueError(f"Cannot sort by field {field_name}")

            col = self._resolve_column(field_name)
            query = query.order_by(desc(col) if order == "desc" else asc(col))

    # Helper function
    def _resolve_column(self, field_path: str) -> MappedColumn:
        """
        Resolve a top-level column attribute on the mapped model class.

        Nested relationship traversal is not supported by this helper. If a
        dotted field path is passed (e.g. ``profile.city``) a ``ValueError``
        will be raised.
        """
        if "." in field_path:
            raise OperationNotSupported(f"Nested relationship fields({field_path})")

        try:
            return getattr(self.model_cls, field_path)
        except AttributeError as exc:
            raise FieldNotFound(
                field=field_path, table=self.model_cls.__tablename__
            ) from exc
