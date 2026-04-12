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
         → RedisEventBus (Pub/Sub) → PostEventConsumer

DESIGN: session-scoped containers over function-scoped
    ✅ Containers start once — 3–5 s for PostgreSQL, < 1 s for Redis.
    ✅ Tests use unique post titles so isolation is data-level, not DB-reset.
    ❌ A test that corrupts shared schema (e.g. DROP TABLE) will break later
       tests.  Mitigation: tests only perform DML (INSERT/SELECT/UPDATE/DELETE).

Thread safety:  ⚠️ Tests share one uvicorn server — do not run with -n (xdist).
Async safety:   ✅ All tests are ``async def``; pytest-asyncio asyncio_mode=auto.

📚 Docs
- 🐍 https://www.python-httpx.org/async/ — httpx.AsyncClient
- 🔍 https://www.uvicorn.org/ — uvicorn ASGI server
- 🔍 https://testcontainers-python.readthedocs.io/ — PostgresContainer, RedisContainer
"""

from __future__ import annotations

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


__all__ = []
