"""
fastrest_core.query.params
============================
``QueryParams`` — a single value object that bundles an optional AST filter
node, sort directives, and pagination parameters.

Passing a single ``QueryParams`` instead of four individual arguments keeps
repository call-sites clean and makes it trivial to build, cache, and log
query configurations.

Thread safety:  ✅ Frozen dataclass — immutable and hashable after construction.
Async safety:   ✅ No I/O; pure value object safe to share across coroutines.

Example::

    from fastrest_core.query.builder import QueryBuilder
    from fastrest_core.query.params import QueryParams
    from fastrest_core.query.type import SortField, SortOrder

    params = QueryParams(
        node=QueryBuilder().eq("active", True).build(),
        sort=[SortField("created_at", SortOrder.DESC)],
        limit=20,
        offset=0,
    )
    results = await repo.find_by_query(params)
"""

from __future__ import annotations

from dataclasses import dataclass, field

from fastrest_core.query.type import SortField, TransformerNode


@dataclass(frozen=True)
class QueryParams:
    """
    Immutable bundle of query parameters for ``AsyncRepository.find_by_query``.

    All fields have safe defaults so callers can construct minimal params
    without needing to import every piece of the query system.

    Attributes:
        node:   Root AST filter node, or ``None`` for "no filter" (select all).
        sort:   Ordered list of ``SortField`` directives.  Empty list → default
                database order (undefined / insertion order).
        limit:  Maximum rows/documents to return.  ``None`` → no limit.
        offset: Number of rows/documents to skip.  ``None`` → start from first.

    Thread safety:  ✅ Frozen dataclass — immutable.
    Async safety:   ✅ Value object.

    Edge cases:
        - All defaults → ``QueryParams()`` is equivalent to ``find_all()`` with
          no filter, sort, or pagination.
        - ``limit=0`` is technically valid but will return an empty list; use
          ``None`` when you want all results.
        - ``offset`` with ``limit=None`` performs a skip with no cap — useful
          for cursor-based iteration.
    """

    # Optional AST filter — None means "no WHERE clause"
    node: TransformerNode | None = field(default=None)

    # Ordered sort directives — empty list means "no ORDER BY"
    sort: list[SortField] = field(default_factory=list)

    # Pagination
    limit: int | None = field(default=None)
    offset: int | None = field(default=None)

    def __repr__(self) -> str:
        node_repr = type(self.node).__name__ if self.node is not None else "None"
        return (
            f"QueryParams("
            f"node={node_repr}, "
            f"sort={self.sort!r}, "
            f"limit={self.limit!r}, "
            f"offset={self.offset!r})"
        )
