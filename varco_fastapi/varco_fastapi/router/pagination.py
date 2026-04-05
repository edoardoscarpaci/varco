"""
varco_fastapi.router.pagination
================================
FastAPI HTTP pagination layer on top of varco_core's pagination model.

Re-exports the core pagination types for convenience and adds HTTP-specific
concerns: ``add_pagination_headers()`` sets standard response headers.

varco_core already provides the full pagination model:
    - ``PagedReadDTO[T]``   — generic envelope with results, count, total_count, next
    - ``PageCursor``        — carries limit, offset, sort, raw_query for next-page
    - ``paged_response()``  — builder that creates PagedReadDTO from QueryParams + results

This module adds:
    - Response headers: ``X-Total-Count``, ``X-Page-Size``, ``Link`` rel=next

DESIGN: header-based pagination info on top of body-based pagination
    ✅ Clients that prefer header-based pagination (cURL, API gateways) can skip
       JSON parsing for count/next metadata
    ✅ Body still contains the full PagedReadDTO — no information is lost
    ✅ Link header follows RFC 5988 — standard for pagination in REST APIs
    ❌ Two sources of pagination truth (body and headers) — they mirror each other

Thread safety:  ✅ Stateless functions.
Async safety:   ✅ No I/O.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.responses import Response

# Re-export from varco_core for convenience — callers can import from here
from varco_core.dto.pagination import (
    PageCursor,
    PagedReadDTO,
    SortCursorField,
    paged_response,
)

__all__ = [
    # Re-exports
    "PagedReadDTO",
    "PageCursor",
    "SortCursorField",
    "paged_response",
    # HTTP additions
    "add_pagination_headers",
]


def add_pagination_headers(
    response: Response,
    page: PagedReadDTO[Any],
    request: Request,
) -> None:
    """
    Set standard pagination headers on the response.

    Added headers:
    - ``X-Total-Count``: Total matching items (only if ``page.total_count`` is not None).
    - ``X-Page-Size``: Number of items in this page (``page.count``).
    - ``Link``: RFC 5988 link header with ``rel="next"`` (only if ``page.next`` is not None).

    The response body already contains the full pagination metadata via
    ``PagedReadDTO``.  Headers are added for clients that prefer header-based
    pagination without parsing the JSON body.

    Args:
        response: The FastAPI/Starlette response to add headers to.
        page:     The ``PagedReadDTO`` containing pagination state.
        request:  The current request (used to build the next-page URL).

    Edge cases:
        - ``X-Total-Count`` is omitted when ``total_count`` is ``None`` (e.g.
          when ``ListMixin._list_include_total=False``).
        - The ``Link`` next URL preserves all existing query params, replacing
          ``limit`` and ``offset`` with the next page values.
        - ``rel="prev"`` is not added — it can be computed from ``offset - limit``.

    Thread safety:  ✅ Stateless function; mutates only the provided response.
    Async safety:   ✅ No I/O.
    """
    if page.total_count is not None:
        response.headers["X-Total-Count"] = str(page.total_count)

    response.headers["X-Page-Size"] = str(page.count)

    if page.next is not None:
        next_url = _build_next_url(request, page.next)
        response.headers["Link"] = f'<{next_url}>; rel="next"'


def _build_next_url(request: Request, cursor: PageCursor) -> str:
    """
    Build the next-page URL from the current request URL and a ``PageCursor``.

    Preserves all query params from the current request, overriding
    ``limit``, ``offset``, and ``sort`` with the next-cursor values.

    Args:
        request: The current HTTP request.
        cursor:  The next-page cursor from ``PagedReadDTO.next``.

    Returns:
        A string URL for the next page.
    """
    from urllib.parse import urlencode

    # Build new query params
    params: dict[str, str] = dict(request.query_params)
    params["limit"] = str(cursor.limit)
    params["offset"] = str(cursor.offset)

    if cursor.sort:
        sort_str = ",".join(
            f"+{s.field}" if s.order == "asc" else f"-{s.field}" for s in cursor.sort
        )
        params["sort"] = sort_str
    elif "sort" in params:
        del params["sort"]

    if cursor.query is not None:
        params["q"] = cursor.query
    elif "q" in params:
        del params["q"]

    # Reconstruct the URL
    url = request.url
    new_query = urlencode(params)
    return str(url.replace(query=new_query))
