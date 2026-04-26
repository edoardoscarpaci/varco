"""
varco_beanie.query.aggregation
================================
``QueryApplicator`` implementation for MongoDB aggregation pipelines.

Translates a ``varco_core`` query AST into a sequence of MongoDB aggregation
pipeline stages (``$match``, ``$sort``, ``$skip``, ``$limit``) that can be
passed to ``Document.aggregate()``.

Usage::

    from beanie import Document
    from varco_core.query import QueryParams
    from varco_beanie.query.aggregation import BeanieAggregationApplicator

    applicator = BeanieAggregationApplicator()

    params = QueryParams(
        node=QueryBuilder().eq("status", "active").build(),
        sort=[SortField("created_at", SortOrder.DESC)],
        limit=20,
        offset=40,
    )

    pipeline: list[dict] = []
    pipeline = applicator.apply_query(pipeline, params.node)
    pipeline = applicator.apply_sort(pipeline, params.sort)
    pipeline = applicator.apply_pagination(pipeline, params.limit, params.offset)

    results = await MyDocument.aggregate(pipeline).to_list()

DESIGN: aggregation pipeline over ``find()`` / ``FindMany``
    ✅ Supports all MongoDB aggregation operators (``$group``, ``$lookup``,
       ``$project``, etc.) — callers can extend the returned pipeline freely.
    ✅ Composable: callers append their own stages after applying query params.
    ✅ Testable without a real MongoDB — the pipeline is a plain ``list[dict]``.
    ❌ No automatic index hint — the applicator does not know the collection
       schema.  Callers may add a ``$hint`` stage manually.
    ❌ Aggregation bypasses Beanie's ``Link`` resolution — embedded / linked
       documents require explicit ``$lookup`` stages.

DESIGN: ``BeanieQueryCompiler`` reuse for ``$match`` filter
    ✅ Same compiler used by ``find()`` queries — consistent operator semantics.
    ✅ ``allowed_fields`` whitelist from the compiler is enforced on the filter.
    ❌ The compiler produces a flat filter dict; computed fields from upstream
       ``$project`` stages cannot be referenced without aliasing.

Thread safety:  ✅ Stateless after construction.
Async safety:   ✅ Synchronous — pipeline is a plain Python list.
"""

from __future__ import annotations

import sys
from typing import Any

from providify import Singleton

from varco_core.query.applicator.applicator import QueryApplicator
from varco_core.query.type import SortField, SortOrder, TransformerNode
from varco_beanie.query.compiler import BeanieQueryCompiler


# MongoDB sort direction constants
_ASC = 1
_DESC = -1

# Type alias for a MongoDB aggregation pipeline
Pipeline = list[dict[str, Any]]


@Singleton(priority=-sys.maxsize, qualifier="beanie_aggregation")
class BeanieAggregationApplicator(QueryApplicator):
    """
    ``QueryApplicator`` that builds a MongoDB aggregation pipeline from a
    ``varco_core`` ``QueryParams``.

    Each method appends one or more pipeline stages to the input pipeline
    and returns the extended pipeline (a new list is created on each call).

    Typical usage::

        applicator = BeanieAggregationApplicator()
        pipeline = applicator.apply_query([], params.node)
        pipeline = applicator.apply_sort(pipeline, params.sort)
        pipeline = applicator.apply_pagination(pipeline, params.limit, params.offset)
        results = await MyDocument.aggregate(pipeline).to_list()

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ Synchronous; the pipeline list is plain Python.

    Args:
        compiler:       Optional pre-configured ``BeanieQueryCompiler``.
                        Useful when you want to share an ``allowed_fields``
                        whitelist across multiple applicator instances.
        allowed_fields: Field-path whitelist propagated to the compiler when
                        the compiler's own whitelist is empty.

    Edge cases:
        - ``apply_query(pipeline, None)`` → no ``$match`` stage added; the
          pipeline is returned unchanged.
        - ``apply_sort(pipeline, None)`` or empty sort list → no ``$sort``
          stage added.
        - ``apply_pagination(pipeline, None, None)`` → no stages added.
        - The caller is responsible for appending any additional stages
          (``$group``, ``$project``, ``$lookup``, etc.) before or after
          the stages added by this applicator.
    """

    def __init__(
        self,
        *args: Any,
        compiler: BeanieQueryCompiler | None = None,
        allowed_fields: set[str] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialise the applicator.

        Args:
            compiler:       Optional pre-built ``BeanieQueryCompiler``.
                            A new singleton instance is used when omitted.
            allowed_fields: Optional field-path whitelist.  Forwarded to the
                            compiler only if the compiler's whitelist is empty.
            args:           Forwarded to ``QueryApplicator.__init__``.
            kwargs:         Forwarded to ``QueryApplicator.__init__``.
        """
        super().__init__(*args, allowed_fields=allowed_fields, **kwargs)
        if compiler is not None:
            self._compiler = compiler
        else:
            # Use a fresh compiler; propagate our allowed_fields if any.
            self._compiler = BeanieQueryCompiler(
                allowed_fields=allowed_fields or None,
            )

    def apply_query(
        self,
        query: Pipeline,
        node: TransformerNode,
        *args: Any,
        **kwargs: Any,
    ) -> Pipeline:
        """
        Append a ``$match`` stage for the AST filter node.

        If ``node`` is ``None``, the pipeline is returned unchanged (no filter).

        DESIGN: single ``$match`` stage at the start
            ✅ Placed first so MongoDB can use an index on the filter fields.
            ✅ The compiler enforces ``allowed_fields`` — no raw field names
               from user input reach the pipeline unvalidated.
            ❌ Callers who want to filter after a ``$project`` must build the
               ``$match`` stage manually and append it themselves.

        Args:
            query: Existing pipeline (may be empty).
            node:  AST filter node.  ``None`` → no filter added.
            args:  Unused.
            kwargs: Unused.

        Returns:
            New pipeline list with the ``$match`` stage appended (if any).
        """
        if node is None:
            return list(query)
        mongo_filter: dict[str, Any] = self._compiler.visit(node)
        return list(query) + [{"$match": mongo_filter}]

    def apply_sort(
        self,
        query: Pipeline,
        sort_fields: list[SortField] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> Pipeline:
        """
        Append a ``$sort`` stage for the given sort directives.

        If ``sort_fields`` is ``None`` or empty, the pipeline is returned unchanged.

        DESIGN: single ``$sort`` stage with combined key document
            ✅ MongoDB's ``$sort`` accepts a document with multiple fields in
               priority order — one stage handles all sort fields.
            ✅ Position (after ``$match`` / before ``$limit``) follows MongoDB
               optimizer recommendations for index-sort pushdown.

        Args:
            query:       Existing pipeline.
            sort_fields: Ordered list of ``SortField`` values.  ``None`` or
                         empty → no sort stage added.
            args:  Unused.
            kwargs: Unused.

        Returns:
            New pipeline list with the ``$sort`` stage appended (if any).
        """
        if not sort_fields:
            return list(query)
        sort_doc = {
            sf.field: _DESC if sf.order == SortOrder.DESC else _ASC
            for sf in sort_fields
        }
        return list(query) + [{"$sort": sort_doc}]

    def apply_pagination(
        self,
        query: Pipeline,
        limit: int | None,
        offset: int | None,
        *args: Any,
        **kwargs: Any,
    ) -> Pipeline:
        """
        Append ``$skip`` and/or ``$limit`` stages.

        Stages are only added when the corresponding value is non-``None`` and
        positive.  ``$skip`` is always placed before ``$limit`` (MongoDB best
        practice — ``$skip`` + ``$limit`` lets MongoDB short-circuit early).

        Args:
            query:  Existing pipeline.
            limit:  Maximum number of documents.  ``None`` → no ``$limit``.
            offset: Documents to skip.  ``None`` or ``0`` → no ``$skip``.
            args:   Unused.
            kwargs: Unused.

        Returns:
            New pipeline with ``$skip`` and/or ``$limit`` appended.
        """
        result = list(query)
        if offset:  # skip 0 is a no-op; treat falsy as "no skip"
            result.append({"$skip": offset})
        if limit is not None:
            result.append({"$limit": limit})
        return result

    def __repr__(self) -> str:
        return (
            f"BeanieAggregationApplicator("
            f"allowed_fields={sorted(self.allowed_fields) or 'all'!r})"
        )


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    "BeanieAggregationApplicator",
    "Pipeline",
]
