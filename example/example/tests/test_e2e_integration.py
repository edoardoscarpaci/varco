"""
tests.test_e2e_integration
==========================
End-to-end integration tests for the Post CRUD API.

Strategy
--------
- Real PostgreSQL (testcontainers), Redis (testcontainers), uvicorn, httpx.
- Session-scoped server — started once, shared across all tests in this file.
- Tests are isolated at the DATA level: each test uses unique titles / UUIDs
  so no DB reset is needed between tests.
- ``@pytest.mark.integration`` — requires Docker; skipped by default.

Fixtures are defined in ``conftest.py`` (shared with ``test_api_auth.py``):
    - ``postgres_container``  — PostgreSQL 16 Docker container
    - ``redis_container``     — Redis 7 Docker container
    - ``running_server``      — uvicorn serving ``create_app()``; yields base URL
    - ``client``              — function-scoped ``httpx.AsyncClient``

Nothing is mocked.  The complete request path is exercised:

    httpx → uvicorn → FastAPI middleware → VarcoCRUDRouter
         → PostService (CacheServiceMixin + AsyncService)
         → SQLAlchemy (asyncpg + PostgreSQL)
         → RedisCache (look-aside)
         → RedisEventBus (Streams) → PostEventConsumer → SSEEventBus → SSE clients

DESIGN: session-scoped containers over function-scoped
    ✅ Containers start once — 3–5 s for PostgreSQL, < 1 s for Redis.
    ✅ Tests use unique post titles so isolation is data-level, not DB-reset.
    ❌ A test that corrupts shared schema (e.g. DROP TABLE) will break later
       tests.  Mitigation: tests only perform DML (INSERT/SELECT/UPDATE/DELETE).

DESIGN: SSE event collection via asyncio.create_task + asyncio.wait_for
    ✅ SSE subscriber is established before the triggering action — no race
       window where the event fires before the subscriber is ready.
    ✅ ``asyncio.wait_for`` provides a bounded timeout so tests never hang.
    ✅ The helper task is always cancelled in the finally block — no leaked tasks.
    ❌ A 0.2 s sleep is needed to let the SSE connection establish — the server
       has no "ready" signal for SSE subscription acknowledgement.

Thread safety:  ⚠️ Tests share one uvicorn server — do not run with -n (xdist).
Async safety:   ✅ All tests are ``async def``; pytest-asyncio asyncio_mode=auto.

📚 Docs
- 🐍 https://www.python-httpx.org/async/ — httpx.AsyncClient
- 🔍 https://www.uvicorn.org/ — uvicorn ASGI server
- 🔍 https://testcontainers-python.readthedocs.io/ — PostgresContainer, RedisContainer
- 📐 https://html.spec.whatwg.org/multipage/server-sent-events.html — SSE spec
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from uuid import UUID

import httpx
import pytest


# ── CRUD tests ─────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_create_post_returns_201_with_body(client: httpx.AsyncClient) -> None:
    """
    ``POST /v1/posts`` creates a post and returns 201 with all required fields.

    Exercises:
    - HTTP routing (VarcoCRUDRouter → CreateMixin).
    - PostService.create() → SQLAlchemy INSERT → PostgreSQL.
    - PostAssembler.to_domain() / to_read_dto().
    - PostCreatedEvent published to Redis Pub/Sub.

    Edge cases:
        - ``author_id`` is ``None`` — no Bearer token is sent in this test
          so the service uses the anonymous ``_ANON_CTX`` (``user_id=None``).
        - ``pk`` is a valid UUID — verifies PKStrategy.UUID_AUTO ran.
    """
    response = await client.post(
        "/v1/posts",
        json={"title": "Hello E2E", "body": "This is a real post."},
    )

    assert response.status_code == 201, response.text

    body = response.json()
    assert body["title"] == "Hello E2E"
    assert body["body"] == "This is a real post."
    assert "pk" in body, "pk missing from response"
    assert "author_id" in body, "author_id field missing"
    assert "created_at" in body, "created_at missing — ORM default did not fire"
    assert "updated_at" in body, "updated_at missing — ORM default did not fire"

    # pk must be a well-formed UUID
    UUID(body["pk"])
    # author_id is None for anonymous requests — no auth token sent
    assert body["author_id"] is None or isinstance(body["author_id"], str)


@pytest.mark.integration
async def test_get_post_returns_200_and_caches(client: httpx.AsyncClient) -> None:
    """
    ``GET /v1/posts/{id}`` returns the created post.

    The second GET must return identical data (from the Redis cache).

    Exercises:
    - SQLAlchemy SELECT on first GET (cache miss → DB → cache.set).
    - Redis cache hit on second GET (no DB round-trip).

    Edge cases:
        - Cache hit is observable by consistent body content, not Redis inspection.
        - If Redis is unavailable, fallback is the DB — no assertion failure.
    """
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Cacheable", "body": "Cache me"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    # First GET — cache miss → DB fetch.
    get_1 = await client.get(f"/v1/posts/{pk}")
    assert get_1.status_code == 200
    data_1 = get_1.json()
    assert data_1["pk"] == pk
    assert data_1["title"] == "Cacheable"

    # Second GET — must hit cache; body must be bit-for-bit identical.
    get_2 = await client.get(f"/v1/posts/{pk}")
    assert get_2.status_code == 200
    assert get_2.json() == data_1, "Cache returned different data on second GET"


@pytest.mark.integration
async def test_list_posts_returns_created_items(client: httpx.AsyncClient) -> None:
    """
    ``GET /v1/posts`` lists all posts including newly created ones.

    Exercises:
    - Bulk SELECT via SQLAlchemy / QueryApplicator.
    - Pagination envelope (``{"results": [...], "count": N, "total_count": N}``).

    Edge cases:
        - Other tests also create posts — we verify OUR titles appear somewhere
          in the list rather than asserting an exact count.
    """
    titles = {f"List-Post-A-{id(object())}", f"List-Post-B-{id(object())}"}

    for title in titles:
        resp = await client.post("/v1/posts", json={"title": title, "body": "listing"})
        assert resp.status_code == 201

    list_resp = await client.get("/v1/posts")
    assert list_resp.status_code == 200

    returned_titles = {p["title"] for p in list_resp.json()["results"]}
    for title in titles:
        assert title in returned_titles, f"Expected {title!r} in list response"


@pytest.mark.integration
async def test_patch_post_updates_title_and_invalidates_cache(
    client: httpx.AsyncClient,
) -> None:
    """
    ``PATCH /v1/posts/{id}`` updates the title and evicts the Redis cache entry.

    Cache invalidation sequence:
    1. GET  → cache miss → DB fetch → ``cache.set("post:get:<pk>")``.
    2. PATCH → DB UPDATE → ``CacheServiceMixin.update()`` → ``cache.delete(...)``.
    3. GET  → cache miss → DB fetch → returns NEW title.

    Edge cases:
        - ``body`` is not supplied in the PATCH — must remain unchanged.
        - Anonymous PATCH is blocked by PostAuthorizer (403 Forbidden) unless the
          test sends no auth — here we rely on the anonymous-create permitting a
          post with ``author_id=None``, and PostAuthorizer allows anonymous PATCH
          only if the post has no author (``author_id=None``).
          In practice, anonymous cannot PATCH — this test must use a token.
          Since we test the anonymous path in test_api_auth.py, here we use
          alice (admin) credentials from the session fixtures.

    Note: This test imports ``alice_token`` from the session scope to ensure
    the PATCH is authorized.  See ``test_api_auth.py`` for the 403 path.
    """
    # Alice is an admin — can create and patch any post.
    async with httpx.AsyncClient(
        base_url=client.base_url,
        timeout=httpx.Timeout(30.0),
    ) as auth_client:
        # Get alice's token inline (session fixture not accessible here directly).
        login_resp = await auth_client.post(
            "/auth/login",
            json={"username": "alice", "password": "alice123"},
        )
        assert login_resp.status_code == 200
        token = login_resp.json()["access_token"]
        auth_headers = {"Authorization": f"Bearer {token}"}

        # Create a post as alice.
        create_resp = await auth_client.post(
            "/v1/posts",
            json={"title": "Before Patch", "body": "Stays the same"},
            headers=auth_headers,
        )
        assert create_resp.status_code == 201
        pk = create_resp.json()["pk"]

        # Warm the cache via GET.
        await auth_client.get(f"/v1/posts/{pk}", headers=auth_headers)

        # PATCH — only title, body stays.
        patch_resp = await auth_client.patch(
            f"/v1/posts/{pk}",
            json={"title": "After Patch"},
            headers=auth_headers,
        )
        assert patch_resp.status_code == 200
        patched = patch_resp.json()
        assert patched["title"] == "After Patch"
        assert patched["body"] == "Stays the same"

        # GET after PATCH — must reflect new title (cache evicted).
        get_resp = await auth_client.get(f"/v1/posts/{pk}", headers=auth_headers)
        assert get_resp.status_code == 200
        assert get_resp.json()["title"] == "After Patch"
        assert get_resp.json()["body"] == "Stays the same"


@pytest.mark.integration
async def test_delete_post_returns_204_and_subsequent_get_404(
    client: httpx.AsyncClient,
) -> None:
    """
    ``DELETE /v1/posts/{id}`` removes the post; ``GET`` on the same pk → 404.

    Exercises:
    - SQLAlchemy DELETE.
    - Cache eviction on delete (``CacheServiceMixin._after_delete``).
    - PostDeletedEvent published to Redis Pub/Sub.
    - Error middleware mapping ``ServiceNotFoundError`` → 404.

    Edge cases:
        - GET after DELETE must NOT return a stale cache entry — verifies
          the delete path evicts the cache correctly.
        - Uses alice (admin) credentials to avoid 403 on DELETE.
    """
    async with httpx.AsyncClient(
        base_url=client.base_url,
        timeout=httpx.Timeout(30.0),
    ) as auth_client:
        login_resp = await auth_client.post(
            "/auth/login",
            json={"username": "alice", "password": "alice123"},
        )
        assert login_resp.status_code == 200
        token = login_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        create_resp = await auth_client.post(
            "/v1/posts",
            json={"title": "To Be Deleted", "body": "Goodbye"},
            headers=headers,
        )
        assert create_resp.status_code == 201
        pk = create_resp.json()["pk"]

        # Warm the cache.
        await auth_client.get(f"/v1/posts/{pk}", headers=headers)

        # DELETE.
        delete_resp = await auth_client.delete(f"/v1/posts/{pk}", headers=headers)
        assert delete_resp.status_code == 204, delete_resp.text

        # GET after DELETE — must be 404 (DB row gone, cache evicted).
        get_resp = await auth_client.get(f"/v1/posts/{pk}", headers=headers)
        assert (
            get_resp.status_code == 404
        ), f"Expected 404 after delete, got {get_resp.status_code}: {get_resp.text}"


@pytest.mark.integration
async def test_get_nonexistent_post_returns_404(client: httpx.AsyncClient) -> None:
    """
    ``GET /v1/posts/{id}`` for a non-existent UUID returns 404.

    Exercises the error middleware path:
    - ``PostService.get()`` raises ``ServiceNotFoundError``.
    - Error middleware maps it to HTTP 404 with a structured JSON body.

    Edge cases:
        - Uses a random UUID guaranteed never to exist in the test DB.
    """
    import uuid  # noqa: PLC0415

    resp = await client.get(f"/v1/posts/{uuid.uuid4()}")
    assert resp.status_code == 404


@pytest.mark.integration
async def test_summary_custom_endpoint_returns_pk_and_title(
    client: httpx.AsyncClient,
) -> None:
    """
    ``GET /v1/posts/{id}/summary`` returns the lightweight summary dict.

    Exercises the custom ``@route`` endpoint in ``PostRouter.get_summary()``.

    Returns:
        ``{"pk": "<uuid>", "title": "<title>", "author_id": ..., "caller": null}``

    Edge cases:
        - Summary does NOT include ``body`` or timestamps.
        - Internally calls ``_service.get()`` — cache is exercised.
        - ``caller`` is ``null`` for anonymous requests (no auth token).
    """
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Summary Post", "body": "Full body not in summary"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    summary_resp = await client.get(f"/v1/posts/{pk}/summary")
    assert summary_resp.status_code == 200

    summary = summary_resp.json()
    assert summary["pk"] == pk
    assert summary["title"] == "Summary Post"
    # ``body`` is NOT part of the summary endpoint contract.
    assert "body" not in summary
    # Anonymous caller — ``caller`` is null.
    assert summary["caller"] is None


# ── Pagination tests ───────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_list_pagination_limit_and_offset(client: httpx.AsyncClient) -> None:
    """
    ``GET /v1/posts?limit=N&offset=M`` returns a correctly-sized page.

    Strategy:
    1. Create 5 posts with a unique ``PAGTEST-`` prefix so the filter
       ``?q=title LIKE 'PAGTEST-<uid>%'`` isolates them from other tests.
    2. Fetch page 1 (limit=2, offset=0) and page 2 (limit=2, offset=2).
    3. Assert each page contains exactly 2 results, the two pages are disjoint,
       and ``total_count`` reports all 5 posts.

    Exercises:
    - ``HttpQueryParams.to_query_params()`` limit/offset capping.
    - ``QueryApplicator`` LIMIT + OFFSET injection into the SQLAlchemy SELECT.
    - ``PagedReadDTO`` envelope: ``results``, ``count``, ``total_count``, ``next``.

    Edge cases:
        - The LIKE filter isolates this test's data so ``total_count=5`` is
          stable regardless of how many posts other tests have created.
        - ``count`` equals ``len(results)`` — both reflect the current page size.
        - Page 2 must have at least 2 results so we create 5 (not just 4) to
          guarantee the second page is non-empty even with any ordering.
    """
    # ── 1. Create 5 posts under a unique namespace ────────────────────────────
    # Use id(object()) for a per-test-run unique suffix — avoids collisions
    # when the test suite is run multiple times against the same DB.
    uid = id(object())
    prefix = f"PAGTEST-{uid}"
    # Note: grammar uses ESCAPED_STRING (double-quoted) — see grammar.lark.
    for i in range(5):
        r = await client.post(
            "/v1/posts",
            json={"title": f"{prefix}-{i}", "body": "pagination test"},
        )
        assert r.status_code == 201

    # ── 2. Page 1 (limit=2, offset=0) ────────────────────────────────────────
    # The LIKE filter expression isolates this test's posts.
    # Grammar: field LIKE value  →  q=title LIKE "PAGTEST-<uid>%"
    # Note: the grammar's ESCAPED_STRING terminal uses double quotes (Lark
    # follows the JSON string definition, not SQL's single-quote convention).
    page1 = await client.get(
        "/v1/posts",
        params={"q": f'title LIKE "{prefix}%"', "limit": 2, "offset": 0},
    )
    assert page1.status_code == 200
    body1 = page1.json()
    assert body1["count"] == 2, f"Expected 2 results on page 1, got {body1['count']}"
    assert len(body1["results"]) == 2

    # ── 3. Page 2 (limit=2, offset=2) ────────────────────────────────────────
    page2 = await client.get(
        "/v1/posts",
        params={"q": f'title LIKE "{prefix}%"', "limit": 2, "offset": 2},
    )
    assert page2.status_code == 200
    body2 = page2.json()
    assert body2["count"] == 2, f"Expected 2 results on page 2, got {body2['count']}"

    # ── 4. Pages must be disjoint ─────────────────────────────────────────────
    # If the same pk appears in both pages, OFFSET is not working correctly.
    pks1 = {p["pk"] for p in body1["results"]}
    pks2 = {p["pk"] for p in body2["results"]}
    assert not pks1.intersection(
        pks2
    ), f"Page 1 and page 2 overlap — OFFSET did not advance: {pks1 & pks2}"

    # ── 5. total_count must reflect all 5 posts ───────────────────────────────
    # Both pages share the same filter, so total_count should be identical.
    assert (
        body1.get("total_count") == 5
    ), f"Expected total_count=5, got {body1.get('total_count')}"
    assert (
        body2.get("total_count") == 5
    ), f"Expected total_count=5 on page 2, got {body2.get('total_count')}"


# ── Filter tests ───────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_list_filter_exact_title_match(client: httpx.AsyncClient) -> None:
    """
    ``GET /v1/posts?q=title = '<title>'`` returns only posts with that exact title.

    Strategy:
    1. Create two posts: a ``TARGET`` post and a ``DECOY`` post with a different
       but similarly-prefixed title.
    2. Filter by the exact target title — only the target post must appear.
    3. The decoy must NOT appear even though it shares a common prefix.

    Exercises:
    - ``QueryParser`` `=` (equality) comparison operator.
    - ``SQLAlchemyFilterVisitor`` WHERE clause generation for exact match.
    - ``TypeCoercionVisitor`` coercing the string literal to the column type.

    Edge cases:
        - The exact-match filter must not return the decoy — verifies that
          ``=`` does not behave like LIKE.
        - If ``total_count=1`` the service correctly counts filtered rows, not all rows.
    """
    uid = id(object())
    target_title = f"FILT-EQ-TARGET-{uid}"
    decoy_title = f"FILT-EQ-TARGET-{uid}-DECOY"

    # Create both posts.
    r1 = await client.post(
        "/v1/posts", json={"title": target_title, "body": "exact match body"}
    )
    assert r1.status_code == 201
    target_pk = r1.json()["pk"]

    r2 = await client.post(
        "/v1/posts", json={"title": decoy_title, "body": "decoy body"}
    )
    assert r2.status_code == 201

    # Filter by exact title — grammar: field = "value"  (double-quoted ESCAPED_STRING)
    resp = await client.get(
        "/v1/posts",
        params={"q": f'title = "{target_title}"'},
    )
    assert resp.status_code == 200
    body = resp.json()

    returned_pks = {p["pk"] for p in body["results"]}
    assert (
        target_pk in returned_pks
    ), f"Target post {target_pk!r} missing from filtered results: {returned_pks}"
    # Decoy must NOT be present — the equality filter is strict.
    _ = returned_pks - {target_pk}
    non_target_titles = {p["title"] for p in body["results"] if p["pk"] != target_pk}
    assert (
        decoy_title not in non_target_titles
    ), f"Decoy {decoy_title!r} unexpectedly returned by exact-match filter"
    assert (
        body.get("total_count") == 1
    ), f"Expected total_count=1 for exact title filter, got {body.get('total_count')}"


@pytest.mark.integration
async def test_list_filter_like_pattern(client: httpx.AsyncClient) -> None:
    """
    ``GET /v1/posts?q=title LIKE '<prefix>%'`` returns all matching posts.

    Strategy:
    1. Create 3 posts whose titles share a unique prefix.
    2. Create 1 decoy post with an unrelated title.
    3. Filter by the prefix pattern — exactly 3 posts must be returned.

    Exercises:
    - ``QueryParser`` ``LIKE`` operator token.
    - ``SQLAlchemyFilterVisitor`` ``column.like(value)`` translation.

    Edge cases:
        - The decoy confirms the filter is genuinely restrictive.
        - ``%`` at the end is a SQL wildcard — tests that the grammar passes
          it through verbatim to SQLAlchemy without escaping.
    """
    uid = id(object())
    prefix = f"FILT-LIKE-{uid}"

    # 3 matching posts under the unique prefix.
    for i in range(3):
        r = await client.post(
            "/v1/posts",
            json={"title": f"{prefix}-Item-{i}", "body": "like test body"},
        )
        assert r.status_code == 201

    # 1 decoy that shares no prefix with the filter.
    await client.post(
        "/v1/posts",
        json={"title": f"DECOY-{uid}", "body": "should not appear"},
    )

    # Filter with LIKE — grammar: field LIKE "pattern"  (double-quoted ESCAPED_STRING)
    resp = await client.get(
        "/v1/posts",
        params={"q": f'title LIKE "{prefix}%"'},
    )
    assert resp.status_code == 200
    body = resp.json()

    assert (
        body.get("total_count") == 3
    ), f"Expected total_count=3 for LIKE filter, got {body.get('total_count')}"
    assert body["count"] == 3
    returned_titles = {p["title"] for p in body["results"]}
    for i in range(3):
        expected = f"{prefix}-Item-{i}"
        assert (
            expected in returned_titles
        ), f"{expected!r} missing from LIKE-filtered results: {returned_titles}"


# ── Sort tests ─────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_list_sort_ascending_and_descending(client: httpx.AsyncClient) -> None:
    """
    ``GET /v1/posts?sort=+title`` and ``?sort=-title`` return results in the
    correct lexicographic order.

    Strategy:
    1. Create 3 posts with titles ``B``, ``A``, and ``C`` under a unique prefix
       so we can filter only our posts.
    2. Fetch with ``sort=+title`` (ascending) → expect A, B, C order.
    3. Fetch with ``sort=-title`` (descending) → expect C, B, A order.

    Exercises:
    - ``_parse_sort_string()`` in ``varco_fastapi.router.base``.
    - ``QueryApplicator`` ORDER BY clause generation.
    - Interaction between ``?q=`` filter and ``?sort=`` — both applied together.

    Edge cases:
        - The LIKE filter isolates our 3 posts — other tests' posts do not
          pollute the sort order.
        - Titles ``A-``, ``B-``, ``C-`` are lexicographically ordered by their
          leading character so the assertion is deterministic.
    """
    uid = id(object())
    prefix = f"SORT-{uid}"

    # Insert in insertion order B, A, C — the DB's natural order is NOT guaranteed
    # to match alphabetical, so a sort directive is required to get a stable result.
    for letter in ("B", "A", "C"):
        r = await client.post(
            "/v1/posts",
            json={"title": f"{prefix}-{letter}", "body": "sort test body"},
        )
        assert r.status_code == 201

    # ── Ascending sort ────────────────────────────────────────────────────────
    asc_resp = await client.get(
        "/v1/posts",
        params={"q": f'title LIKE "{prefix}%"', "sort": "+title"},
    )
    assert asc_resp.status_code == 200
    asc_titles = [p["title"] for p in asc_resp.json()["results"]]
    assert asc_titles == [
        f"{prefix}-A",
        f"{prefix}-B",
        f"{prefix}-C",
    ], f"Ascending sort mismatch: {asc_titles}"

    # ── Descending sort ───────────────────────────────────────────────────────
    desc_resp = await client.get(
        "/v1/posts",
        params={"q": f'title LIKE "{prefix}%"', "sort": "-title"},
    )
    assert desc_resp.status_code == 200
    desc_titles = [p["title"] for p in desc_resp.json()["results"]]
    assert desc_titles == [
        f"{prefix}-C",
        f"{prefix}-B",
        f"{prefix}-A",
    ], f"Descending sort mismatch: {desc_titles}"


# ── Event collection tests (SSE) ───────────────────────────────────────────────


async def _collect_sse_events(
    base_url: str,
    target_queue: asyncio.Queue[dict],
    ready_event: asyncio.Event,
    stop_event: asyncio.Event,
) -> None:
    """
    Background coroutine: streams SSE events from ``/events/posts`` into
    ``target_queue`` until ``stop_event`` is set or the connection drops.

    Opens a fresh ``httpx.AsyncClient`` (separate from the test-scoped one)
    so the streaming connection does not block the test's own HTTP requests.

    Sets ``ready_event`` after the first SSE connection line is received so the
    caller can proceed without a fixed-length sleep.

    Args:
        base_url:     Server base URL, e.g. ``"http://127.0.0.1:54321"``.
        target_queue: Asyncio queue — each parsed JSON event dict is put here.
        ready_event:  Signalled when the SSE stream delivers its first byte
                      (meaning the connection is established and the server is
                      pushing events).
        stop_event:   Set by the caller to terminate the streaming loop cleanly.

    Edge cases:
        - If the server is not running, the connect will raise immediately —
          the task will propagate the exception.
        - If no events arrive before the test timeout, ``target_queue`` stays
          empty and the caller asserts failure via ``asyncio.wait_for``.
        - SSE lines that are not ``data:`` lines (e.g. blank lines, comments)
          are silently skipped.

    Async safety: ✅ safe to run as an asyncio Task — does not share state with
                  the test except via the queue and events (which are concurrency-safe).
    """
    async with httpx.AsyncClient(
        base_url=base_url,
        timeout=httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0),
    ) as sse_client:
        async with sse_client.stream("GET", "/events/posts") as response:
            async for line in response.aiter_lines():
                if stop_event.is_set():
                    break
                # Signal the caller that the connection is live the moment
                # any byte is received (even a blank SSE keepalive line).
                if not ready_event.is_set():
                    ready_event.set()
                # Only parse ``data:`` lines — SSE format is ``data: {json}\n\n``.
                if line.startswith("data:"):
                    raw = line[len("data:") :].strip()
                    if raw:
                        # Non-blocking put: if the queue is full the event is
                        # dropped silently rather than blocking the stream reader.
                        # Default queue maxsize=0 (unbounded) so this never drops
                        # in practice for the small number of events in tests.
                        await target_queue.put(json.loads(raw))


async def _wait_for_matching_event(
    queue: asyncio.Queue[dict],
    *,
    match_type: str,
    match_post_id: str,
    timeout: float = 8.0,
) -> dict:
    """
    Drain ``queue`` until an SSE event matching ``match_type`` and
    ``match_post_id`` is found, or ``timeout`` seconds elapse.

    Args:
        queue:         Queue populated by ``_collect_sse_events``.
        match_type:    Expected ``event_type`` string (e.g. ``"PostCreatedEvent"``).
        match_post_id: Expected ``data.post_id`` UUID string.
        timeout:       Max seconds to wait before raising ``TimeoutError``.

    Returns:
        The matching event dict.

    Raises:
        TimeoutError: No matching event arrived within ``timeout`` seconds.

    Edge cases:
        - Events from other concurrent tests may arrive in the queue — they are
          discarded (not re-queued) because the test uses unique post IDs.
        - ``asyncio.wait_for`` cancels the inner coroutine when the deadline
          passes — the outer task continues and asserts failure.
    """
    deadline = asyncio.get_event_loop().time() + timeout

    async def _drain() -> dict:
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise TimeoutError(
                    f"No {match_type!r} event for post {match_post_id!r} "
                    f"arrived within {timeout} s."
                )
            try:
                event = await asyncio.wait_for(queue.get(), timeout=remaining)
            except TimeoutError:
                raise TimeoutError(
                    f"No {match_type!r} event for post {match_post_id!r} "
                    f"arrived within {timeout} s."
                )
            if (
                event.get("event_type") == match_type
                and str(event.get("data", {}).get("post_id", "")) == match_post_id
            ):
                return event
            # Event is for a different post — discard and keep waiting.

    return await _drain()


@pytest.mark.integration
async def test_sse_receives_post_created_event(
    running_server: str,
    client: httpx.AsyncClient,
) -> None:
    """
    Creating a post via ``POST /v1/posts`` publishes a ``PostCreatedEvent`` that
    arrives on the ``GET /events/posts`` SSE stream.

    Full event pipeline exercised:
        POST /v1/posts
            → PostService._after_create()
            → AbstractEventProducer._produce()
            → RedisEventBus.publish() (Redis Streams)
            → SSEEventBus._handle_event()
            → SSEConnection queue
            → GET /events/posts StreamingResponse
            → test assertion

    DESIGN: separate SSE client over reusing the test-scoped ``client``
        ✅ The streaming connection stays open while the test makes a POST —
           a single ``httpx.AsyncClient`` handles this naturally via the
           connection pool, but a second client avoids any connection-level
           interference with the test's own requests.
        ✅ ``ready_event`` removes the fixed-length sleep — the test proceeds
           as soon as the server sends the first SSE byte, not after an
           arbitrary delay.
        ❌ One extra client object per test — negligible overhead.

    Edge cases:
        - If Redis is down, the event is never published and the test fails
          with a ``TimeoutError`` from ``_wait_for_matching_event``.
        - ``post_id`` is the canonical identifier for matching — titles are
          not unique enough to distinguish our event from concurrent tests.
        - The SSE stream may contain events from OTHER concurrent tests —
          ``_wait_for_matching_event`` discards them by matching on ``post_id``.

    Thread safety:  ✅ SSE task runs in the same event loop as the test.
    Async safety:   ✅ ``asyncio.create_task`` + ``stop_event`` for clean teardown.
    """
    unique_title = f"SSE-Create-{id(object())}"
    events: asyncio.Queue[dict] = asyncio.Queue()
    # ready_event fires when the first SSE byte arrives — eliminates the fixed
    # sleep that would be needed if we polled server readiness.
    ready_event = asyncio.Event()
    stop_event = asyncio.Event()

    # ── Start the SSE reader in the background ────────────────────────────────
    sse_task = asyncio.create_task(
        _collect_sse_events(running_server, events, ready_event, stop_event)
    )

    try:
        # Wait until the SSE connection is live before creating the post.
        # Without this, there is a race: the event is published before the
        # SSE subscriber registers and the event is missed entirely.
        await asyncio.wait_for(ready_event.wait(), timeout=10.0)

        # ── Create the post — this triggers PostCreatedEvent ──────────────────
        create_resp = await client.post(
            "/v1/posts",
            json={"title": unique_title, "body": "SSE event body"},
        )
        assert create_resp.status_code == 201
        post_id = create_resp.json()["pk"]

        # ── Wait for the matching event ───────────────────────────────────────
        event = await _wait_for_matching_event(
            events,
            match_type="PostCreatedEvent",
            match_post_id=post_id,
        )

        # ── Assert event payload ──────────────────────────────────────────────
        assert event["event_type"] == "PostCreatedEvent"
        data = event["data"]
        assert (
            str(data["post_id"]) == post_id
        ), f"Event post_id {data['post_id']!r} != created post pk {post_id!r}"
        # author_id is None for anonymous requests.
        assert "author_id" in data, "author_id field missing from PostCreatedEvent data"

    finally:
        # Always cancel the streaming task so the connection is closed and
        # the fixture can tear down cleanly.
        stop_event.set()
        sse_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sse_task


@pytest.mark.integration
async def test_sse_receives_post_deleted_event(
    running_server: str,
    client: httpx.AsyncClient,
) -> None:
    """
    Deleting a post via ``DELETE /v1/posts/{id}`` publishes a ``PostDeletedEvent``
    that arrives on the ``GET /events/posts`` SSE stream.

    Full event pipeline exercised:
        DELETE /v1/posts/{id}
            → PostService._after_delete()
            → AbstractEventProducer._produce()
            → RedisEventBus.publish()
            → SSEEventBus._handle_event()
            → GET /events/posts StreamingResponse

    Edge cases:
        - Alice's token is required for DELETE (PostAuthorizer denies anonymous).
        - The post must be CREATED by alice before the SSE reader starts so the
          ``PostCreatedEvent`` for the creation does not interfere with the
          ``PostDeletedEvent`` assertion.  Both events carry the same ``post_id``
          but different ``event_type`` strings.
        - SSE stream may already contain a ``PostCreatedEvent`` for the same
          post — ``_wait_for_matching_event`` discards it by filtering on
          ``event_type == "PostDeletedEvent"``.

    Async safety:   ✅ asyncio.create_task + stop_event for clean teardown.
    """
    # ── 1. Create the post as alice BEFORE starting the SSE reader ────────────
    # The creation event is not relevant to this test — we start the SSE
    # listener after the post exists to avoid intercepting the wrong event.
    async with httpx.AsyncClient(
        base_url=client.base_url,
        timeout=httpx.Timeout(30.0),
    ) as auth_client:
        login_resp = await auth_client.post(
            "/auth/login",
            json={"username": "alice", "password": "alice123"},
        )
        assert login_resp.status_code == 200
        token = login_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {token}"}

        create_resp = await auth_client.post(
            "/v1/posts",
            json={"title": f"SSE-Delete-{id(object())}", "body": "will be deleted"},
            headers=headers,
        )
        assert create_resp.status_code == 201
        post_id = create_resp.json()["pk"]

    # ── 2. Open the SSE stream AFTER the post exists ──────────────────────────
    events: asyncio.Queue[dict] = asyncio.Queue()
    ready_event = asyncio.Event()
    stop_event = asyncio.Event()

    sse_task = asyncio.create_task(
        _collect_sse_events(running_server, events, ready_event, stop_event)
    )

    try:
        # Wait for the SSE connection to be live before triggering the delete.
        await asyncio.wait_for(ready_event.wait(), timeout=10.0)

        # ── 3. Delete the post — triggers PostDeletedEvent ────────────────────
        async with httpx.AsyncClient(
            base_url=client.base_url,
            timeout=httpx.Timeout(30.0),
        ) as auth_client:
            login_resp = await auth_client.post(
                "/auth/login",
                json={"username": "alice", "password": "alice123"},
            )
            token = login_resp.json()["access_token"]
            headers = {"Authorization": f"Bearer {token}"}

            delete_resp = await auth_client.delete(
                f"/v1/posts/{post_id}",
                headers=headers,
            )
            assert delete_resp.status_code == 204, delete_resp.text

        # ── 4. Wait for the PostDeletedEvent ──────────────────────────────────
        event = await _wait_for_matching_event(
            events,
            match_type="PostDeletedEvent",
            match_post_id=post_id,
        )

        # ── 5. Assert event payload ───────────────────────────────────────────
        assert event["event_type"] == "PostDeletedEvent"
        data = event["data"]
        assert (
            str(data["post_id"]) == post_id
        ), f"Event post_id {data['post_id']!r} != deleted post pk {post_id!r}"
        # PostDeletedEvent carries only post_id — no author_id (post is gone).
        assert (
            "author_id" not in data or data["author_id"] is None
        ), "PostDeletedEvent should NOT carry author_id — the post is already deleted"

    finally:
        stop_event.set()
        sse_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await sse_task


__all__ = []
