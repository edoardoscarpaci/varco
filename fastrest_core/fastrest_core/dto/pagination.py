"""
fastrest_core.pagination
=========================
Pagination DTOs and the ``paged_response()`` builder.

These types are designed to be the standard envelope for any collection
endpoint that supports filtering, sorting, and offset-based pagination.

Response shape::

    {
        "results":     [...],        ← the page of ReadDTOs
        "count":       10,           ← items in THIS page
        "total_count": 342,          ← total matching rows across all pages
        "next": {                    ← null when on the last page
            "limit":  10,
            "offset": 110,
            "sort":   [{"field": "created_at", "order": "DESC"}],
            "query":  "status = \\"active\\""
        }
    }

Usage
-----
The service layer calls ``paged_response()`` after fetching both a page of
results and the total count::

    from fastrest_core.pagination import paged_response

    async def list_articles(params: QueryParams, raw_query: str | None, ctx: AuthContext):
        async with uow_provider.make_uow() as uow:
            repo = uow.articles
            entities, total = await asyncio.gather(
                repo.find_by_query(params),
                repo.count(params),
            )
        dtos = [assembler.to_read_dto(e) for e in entities]
        return paged_response(dtos, total_count=total, params=params, raw_query=raw_query)

DESIGN: ``next`` offset = current_offset + len(results), NOT current_offset + limit
    ✅ Correctly handles the partial last page: if limit=10 and only 3 rows
       remain, ``next`` points past them (offset >= total_count) and is
       therefore ``None``.
    ✅ Also correct when the backend returns fewer rows than ``limit`` due to
       a row-level filter applied inside the DB.
    ❌ Relies on the caller not to lie about ``total_count`` — if
       ``total_count`` is stale (e.g. from a cached count), ``next`` may
       be non-None even when the real result set is exhausted.

DESIGN: ``query`` in PageCursor carries the raw query string, not the AST
    ✅ Clients can copy the cursor directly into their next HTTP request.
    ✅ No need to serialize / deserialize a complex AST over the wire.
    ❌ The raw string is lost by the time the service layer runs — callers
       must explicitly pass ``raw_query`` to ``paged_response()``.  The
       HTTP adapter layer (FastAPI, etc.) should extract this from the
       request query param and forward it.

DESIGN: ``SortCursorField`` mirrors ``SortField`` but is a Pydantic model
    ✅ Clean JSON serialization out of the box.
    ✅ ``SortOrder`` is a ``StrEnum`` — serialized as ``"ASC"`` / ``"DESC"``.
    ✅ Clients receive a structured cursor they can POST back unchanged.
    ❌ Duplicates the ``SortField`` dataclass shape — an intentional
       trade-off to avoid ``arbitrary_types_allowed`` on the DTO layer and
       to keep ``dto.py`` free of query-system imports.

Thread safety:  ✅ All types are effectively immutable after construction.
Async safety:   ✅ No I/O; pure value objects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

from pydantic import BaseModel, Field

from fastrest_core.dto.base import ReadDTO
from fastrest_core.query.type import SortOrder

if TYPE_CHECKING:
    # QueryParams is only needed at type-check time for paged_response().
    # The runtime import happens inside the function to avoid pulling in the
    # full Lark grammar machinery just by importing this module.
    from fastrest_core.query.params import QueryParams

# Bound to ReadDTO so PagedReadDTO[T] is constrained to DTO types.
T = TypeVar("T", bound=ReadDTO)


# ── Sort cursor ────────────────────────────────────────────────────────────────


class SortCursorField(BaseModel):
    """
    Pydantic representation of a single sort directive in a pagination cursor.

    Mirrors ``fastrest_core.query.type.SortField`` but is a Pydantic model
    so it serializes cleanly to / from JSON without ``arbitrary_types_allowed``.

    Attributes:
        field: The field name to sort by (e.g. ``"created_at"``).
        order: ``"ASC"`` or ``"DESC"`` (serialized from ``SortOrder`` StrEnum).

    Thread safety:  ✅ Pydantic model — effectively immutable after validation.
    Async safety:   ✅ No I/O.

    Example::

        SortCursorField(field="created_at", order=SortOrder.DESC)
        # JSON → {"field": "created_at", "order": "DESC"}
    """

    field: str = Field(..., description="Field name to sort by.")
    order: SortOrder = Field(..., description="Sort direction: ASC or DESC.")


# ── Page cursor ────────────────────────────────────────────────────────────────


class PageCursor(BaseModel):
    """
    Mirrors the parameters of the next page request.

    Embed this in ``PagedReadDTO.next`` so clients can issue the next request
    without reconstructing the parameters themselves.

    Attributes:
        limit:  Maximum items per page — same as the original request's limit.
        offset: Starting row for the next page (= this page's offset + count).
        sort:   Sort directives carried forward from the original request.
        query:  Raw filter query string from the original request, or ``None``
                if the original request had no filter.

    Thread safety:  ✅ Pydantic model — effectively immutable after validation.
    Async safety:   ✅ No I/O.

    Edge cases:
        - A ``PageCursor`` is only produced when there are more results
          beyond the current page — ``next`` is ``None`` on the last page.
        - ``query`` is the raw query string (e.g. ``'status = "active"'``),
          NOT a serialized AST.  Pass it to ``QueryParser.parse()`` on the
          next request.
    """

    limit: int = Field(..., ge=1, description="Page size for the next request.")
    offset: int = Field(..., ge=0, description="Row offset for the next page.")
    sort: list[SortCursorField] = Field(
        default_factory=list,
        description="Sort directives to carry forward to the next request.",
    )
    query: str | None = Field(
        default=None,
        description="Raw filter query string from the original request.",
    )


# ── Paged response envelope ────────────────────────────────────────────────────


class PagedReadDTO(BaseModel, Generic[T]):
    """
    Generic paginated response envelope for any ``ReadDTO`` subtype.

    Attributes:
        results:     The items in this page.
        count:       Number of items in ``results`` (may be less than ``limit``
                     on the last page or if the backend applied row-level filters).
        total_count: Total number of matching items across ALL pages, or
                     ``None`` when the caller did not issue a count query.
                     Omitting it saves one ``COUNT(*)`` round-trip — useful
                     for infinite-scroll / "load more" UIs that only need
                     ``next``.  Include it when the UI must show "342 results"
                     or a page indicator ("page 3 of 35").
        next:        Cursor for the next page, or ``None`` if this is the last
                     page.  ``next`` is sufficient to drive cursor-style
                     pagination even when ``total_count`` is absent.

    DESIGN: ``total_count`` is optional
        ✅ Avoids a mandatory second ``COUNT(*)`` query on every list request.
        ✅ ``COUNT(*)`` with a complex ``WHERE`` clause can be slow on large
           tables — callers opt in only when the UI genuinely needs it.
        ✅ ``next`` already answers "is there another page?" for all
           cursor-style UIs (infinite scroll, "load more", mobile, etc.).
        ❌ Clients that need "X of Y" or page-number indicators must call
           ``service.count()`` separately and pass it to ``paged_response()``.

    Thread safety:  ✅ Pydantic model — effectively immutable after validation.
    Async safety:   ✅ No I/O.

    Edge cases:
        - ``count`` may be less than ``limit`` on the last page — use ``next``
          to determine whether more pages exist, not ``count < limit``.
        - ``next`` is ``None`` when there is no ``limit`` in ``QueryParams``
          (unbounded query — cannot paginate without a page size).
        - When ``total_count`` is provided it reflects a point-in-time DB
          count; concurrent inserts or deletes may cause it to drift.

    Example JSON (without total_count — "load more" UI)::

        {
            "results": [{"pk": "abc", ...}, ...],
            "count": 10,
            "total_count": null,
            "next": {"limit": 10, "offset": 110, "sort": [...], "query": "..."}
        }

    Example JSON (with total_count — "page X of Y" UI)::

        {
            "results": [{"pk": "abc", ...}, ...],
            "count": 10,
            "total_count": 342,
            "next": {"limit": 10, "offset": 110, "sort": [...], "query": "..."}
        }
    """

    results: list[T] = Field(..., description="Items in the current page.")
    count: int = Field(..., ge=0, description="Number of items in this page.")
    total_count: int | None = Field(
        default=None,
        ge=0,
        description=(
            "Total matching items across all pages, or null when the caller "
            "skipped the count query (sufficient for cursor-style pagination)."
        ),
    )
    next: PageCursor | None = Field(
        default=None,
        description="Cursor for the next page; null if this is the last page.",
    )


# ── Builder helper ─────────────────────────────────────────────────────────────


def paged_response(
    results: list[T],
    *,
    params: QueryParams,
    total_count: int | None = None,
    raw_query: str | None = None,
) -> PagedReadDTO[T]:
    """
    Build a ``PagedReadDTO`` from a fetched results page and its query context.

    ``total_count`` is optional.  Omit it for "load more" / infinite-scroll UIs
    where only the ``next`` cursor matters.  Include it when the UI must show
    "342 results found" or a page-number indicator.

    Next-cursor detection
    ---------------------
    The strategy differs based on whether ``total_count`` is provided:

    **With total_count** (precise):
        ``next`` is set when ``current_offset + len(results) < total_count``.
        Correctly handles partial last pages and row-level backend filters.

    **Without total_count** (heuristic):
        ``next`` is set when ``len(results) == limit``.  This is a heuristic —
        if the last page happens to be exactly ``limit`` items, the cursor
        will point to an empty page.  The following request will then return
        ``results=[]`` and ``next=None``, which is harmless but wastes one
        round-trip.

    Args:
        results:     The page of assembled ``ReadDTO`` instances.
        params:      The ``QueryParams`` used for this request.  Provides
                     ``limit``, ``offset``, and ``sort`` for cursor construction.
        total_count: Total matching rows across all pages, or ``None`` to skip
                     the count query.  When ``None``, next-cursor detection
                     uses the heuristic described above.
        raw_query:   The raw filter query string from the HTTP request (e.g.
                     ``'status = "active"'``).  Pass ``None`` if there was no
                     filter.  The HTTP adapter should extract this from the
                     request query params and forward it here.

    Returns:
        A ``PagedReadDTO[T]`` with ``next`` set to a ``PageCursor`` if more
        pages likely exist, or ``None`` if this is the last (or only) page.

    Raises:
        TypeError: ``params`` is not a ``QueryParams`` instance.

    Edge cases:
        - ``params.limit is None`` → ``next`` is always ``None`` (unbounded
          queries have no meaningful next-page concept).
        - Without ``total_count``: if the last page is exactly ``limit`` items
          the cursor will point to an empty page — one extra round-trip.
        - With ``total_count``: concurrent inserts between the count and fetch
          may cause drift — known TOCTOU issue of offset-based pagination.
        - Empty results with ``total_count=0`` → valid empty response.

    Thread safety:  ✅ Pure function — no shared mutable state.
    Async safety:   ✅ No I/O.

    Example — simple (no count query, "load more" UI)::

        results = await service.list(params, ctx)
        return paged_response(results, params=params, raw_query=q)

    Example — with total count ("X of Y" UI)::

        import asyncio

        results, total = await asyncio.gather(
            service.list(params, ctx),
            service.count(params, ctx),
        )
        return paged_response(results, params=params, total_count=total, raw_query=q)
    """
    # Lazy import — avoids pulling the full query machinery (Lark parser, etc.)
    # just by importing this module.  QueryParams itself is a tiny frozen
    # dataclass, but its module imports SortField and TransformerNode.
    from fastrest_core.query.params import QueryParams as _QueryParams  # noqa: PLC0415

    if not isinstance(params, _QueryParams):
        raise TypeError(
            f"paged_response() requires a QueryParams instance, got {type(params)!r}."
        )

    count = len(results)
    current_offset = params.offset or 0
    next_offset = current_offset + count

    # Determine whether a next-page cursor should be emitted.
    #
    # DESIGN: two strategies depending on total_count availability
    #   With total_count — precise: next exists iff more rows remain.
    #   Without total_count — heuristic: next exists iff this page was full
    #     (i.e. count == limit).  May produce one empty trailing request when
    #     the last page happens to be exactly limit items — acceptable cost
    #     for saving the COUNT(*) round-trip.
    if params.limit is None:
        # Unbounded queries cannot be paginated — no cursor regardless
        has_next = False
    elif total_count is not None:
        # Precise: use the authoritative count
        has_next = next_offset < total_count
    else:
        # Heuristic: a full page suggests there may be more
        has_next = count == params.limit

    if has_next:
        # Convert domain SortField objects to Pydantic-native SortCursorField
        sort_cursor = [
            SortCursorField(field=s.field, order=s.order) for s in params.sort
        ]
        next_cursor: PageCursor | None = PageCursor(
            limit=params.limit,  # type: ignore[arg-type]  # limit is not None here
            offset=next_offset,
            sort=sort_cursor,
            query=raw_query,
        )
    else:
        next_cursor = None

    return PagedReadDTO(
        results=results,
        count=count,
        total_count=total_count,
        next=next_cursor,
    )
