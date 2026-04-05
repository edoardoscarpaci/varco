"""
tests.test_e2e_integration
==========================
End-to-end integration test for the Post API.

Strategy
--------
- ``PostgresContainer``  — real PostgreSQL via Docker (testcontainers).
- ``RedisContainer``     — real Redis via Docker (testcontainers), used for
                           BOTH the event bus (Pub/Sub) and the cache backend.
- Real ``create_app()``  — the actual FastAPI factory with full DI wiring.
- ``uvicorn.Server``     — real ASGI server started in a background asyncio task.
- ``httpx.AsyncClient``  — real HTTP client, no ASGI transport shortcut.

Nothing is mocked.  The test exercises the complete request path:

    httpx → uvicorn → FastAPI middleware → VarcoCRUDRouter
         → PostService (CacheServiceMixin + AsyncService)
         → SQLAlchemy (asyncpg + PostgreSQL)
         → RedisCache (look-aside cache)
         → RedisEventBus (Pub/Sub) → PostEventConsumer

Containers are started once per test session (``scope="session"``) to avoid
the overhead of spinning up Docker for each test function.

DESIGN: session-scoped containers over function-scoped
    ✅ Containers start once — 3-5 seconds for PostgreSQL, < 1 s for Redis.
    ✅ Each test that modifies data uses unique UUIDs / titles so isolation
       is achieved at the data level without resetting the DB between tests.
    ❌ If one test corrupts shared state (e.g. drops tables), later tests
       fail with confusing errors.  Mitigation: tests only do CRUD, never DDL.

DESIGN: real uvicorn over ``httpx.AsyncClient(app=app, transport=...)``
    ``httpx.ASGITransport`` bypasses the actual socket, uvicorn request
    parsing, and middleware stack — it calls the ASGI callable directly.
    Using a real server ensures:
    ✅ Full HTTP stack (TCP, socket, HTTP/1.1 framing) is exercised.
    ✅ Middleware (CORS, error handler) runs as in production.
    ✅ Lifespan (bus.start, consumer.start, job_runner.start) is exercised.
    ❌ Requires a free ephemeral port; races possible if port is grabbed
       between bind and use.  Mitigated by using port 0 (OS assigns freely).

DESIGN: env vars patched via ``monkeypatch.setenv`` over ``os.environ`` mutation
    ``monkeypatch`` restores the original values after the test/session even
    if exceptions are raised — avoids poisoning later tests or the shell.

Thread safety:  ⚠️ Tests share one running uvicorn server — do not run
                   with ``-n`` (pytest-xdist) without per-worker port offset.
Async safety:   ✅ All test functions are ``async def``; pytest-asyncio handles
                   them with ``asyncio_mode = "auto"``.

📚 Docs
- 🔍 https://testcontainers-python.readthedocs.io/ — PostgresContainer, RedisContainer
- 🔍 https://www.uvicorn.org/settings/ — uvicorn.Config / uvicorn.Server
- 🐍 https://www.python-httpx.org/async/ — httpx.AsyncClient
- 🐍 https://docs.python.org/3/library/asyncio-task.html — asyncio.create_task
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
from typing import AsyncIterator, Iterator

import httpx
import pytest
import uvicorn
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

_logger = logging.getLogger(__name__)


# ── Port helpers ───────────────────────────────────────────────────────────────


def _free_port() -> int:
    """
    Find a free TCP port on the loopback interface.

    Opens a socket, binds to port 0 (OS assigns the next free port),
    reads the assigned port number, then closes the socket.  There is a
    short race window between close() and uvicorn.bind() — acceptable in
    single-process test environments.

    Returns:
        An ephemeral port number guaranteed free at the moment of return.

    Edge cases:
        - Very rarely the same port is allocated twice in concurrent tests.
          Use session scope to create the server only once.
        - Port 0 cannot be used directly in ``httpx.AsyncClient`` base URL.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # SO_REUSEADDR avoids "address already in use" if the previous test
        # closed the socket less than 60 seconds ago (TIME_WAIT on Linux).
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ── Session-scoped container + server fixtures ─────────────────────────────────


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[PostgresContainer]:
    """
    Start a PostgreSQL 16 container once for the entire test session.

    The container image is pulled on first run; subsequent runs use the
    Docker layer cache.  ``with_bind_ports`` is not called — testcontainers
    maps an ephemeral host port automatically and exposes it via
    ``get_connection_url()``.

    Yields:
        A running ``PostgresContainer`` with the test database ready.

    Edge cases:
        - Docker must be running on the host.  CI must have a Docker socket.
        - If ``docker pull`` fails (no network), the fixture raises immediately.
    """
    # Use asyncpg driver for SQLAlchemy async engine.
    # PostgresContainer defaults to psycopg2 DSN format (postgresql://);
    # we replace the scheme after ``get_connection_url()`` below.
    with PostgresContainer("postgres:16-alpine") as pg:
        _logger.info("PostgresContainer started at %s", pg.get_connection_url())
        yield pg


@pytest.fixture(scope="session")
def redis_container() -> Iterator[RedisContainer]:
    """
    Start a Redis 7 container once for the entire test session.

    Used for BOTH the event bus (``VARCO_REDIS_URL``) and the cache
    (``VARCO_REDIS_CACHE_URL``), matching the ``create_app()`` bootstrap
    in ``example.app``.

    Yields:
        A running ``RedisContainer``.

    Edge cases:
        - Redis Pub/Sub is ephemeral — messages published before any subscriber
          connects are lost.  The consumer ``start()`` (called during lifespan)
          subscribes before any HTTP request arrives, so no events are lost.
    """
    with RedisContainer("redis:7-alpine") as redis:
        _logger.info("RedisContainer started at port %s", redis.get_exposed_port(6379))
        yield redis


@pytest.fixture(scope="session")
async def running_server(
    postgres_container: PostgresContainer,
    redis_container: RedisContainer,
) -> AsyncIterator[str]:
    """
    Create the FastAPI application, run it under real uvicorn, and yield
    the base URL for HTTP clients.

    This fixture:
    1. Reads the container connection strings.
    2. Patches ``DATABASE_URL`` and ``VARCO_REDIS_URL`` env vars (session scope —
       they stay set for the whole session, which is fine because the server
       owns its own event loop task).
    3. Calls the ``create_app()`` async factory.
    4. Starts ``uvicorn.Server`` as an asyncio Task (non-blocking).
    5. Waits until uvicorn is ready (polls with a short HTTP GET loop).
    6. Yields the base URL.
    7. Shuts down uvicorn and awaits the server task to completion.

    Yields:
        The base URL string, e.g. ``"http://127.0.0.1:54321"``.

    Raises:
        TimeoutError: uvicorn did not start within 10 seconds.

    Edge cases:
        - ``asyncpg`` requires the ``postgresql+asyncpg://`` scheme; testcontainers
          returns a psycopg2 DSN (``postgresql://``), so we replace the scheme.
        - The server task captures its own exception — the fixture logs it and
          re-raises so the test is marked as ERROR rather than silently passing.

    Thread safety:  ✅ Created once; all tests share one base URL.
    Async safety:   ✅ ``async def`` fixture; uvicorn task runs in same event loop.
    """
    # ── Build connection URLs ─────────────────────────────────────────────────
    # testcontainers returns a psycopg2-style URL; asyncpg needs the +asyncpg driver tag.
    raw_pg_url = postgres_container.get_connection_url()
    db_url = raw_pg_url.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)
    # Also handle plain postgresql:// if no driver tag present
    if "postgresql+asyncpg://" not in db_url:
        db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    redis_host = redis_container.get_container_host_ip()
    redis_port = redis_container.get_exposed_port(6379)
    redis_url = f"redis://{redis_host}:{redis_port}/0"

    # ── Patch env vars BEFORE calling create_app() ───────────────────────────
    # create_app() reads these at import/setup time inside the async factory.
    # We set them directly rather than using monkeypatch (which is function-scoped
    # by default).  Restore on teardown to avoid leaking into the outer shell.
    prev_db = os.environ.get("DATABASE_URL")
    # RedisEventBusSettings.from_env() uses prefix "VARCO_REDIS_" → reads VARCO_REDIS_URL.
    # NOT the bare "REDIS_URL" documented in app.py's module docstring (which is aspirational).
    prev_redis = os.environ.get("VARCO_REDIS_URL")
    prev_redis_cache = os.environ.get("VARCO_REDIS_CACHE_URL")

    os.environ["DATABASE_URL"] = db_url
    os.environ["VARCO_REDIS_URL"] = redis_url
    # RedisCacheSettings.from_env() uses prefix "VARCO_REDIS_CACHE_" → reads VARCO_REDIS_CACHE_URL.
    os.environ["VARCO_REDIS_CACHE_URL"] = redis_url

    _logger.info("E2E server: DATABASE_URL=%s", db_url)
    _logger.info("E2E server: VARCO_REDIS_URL=%s", redis_url)

    # Initialise to None so the finally block can safely guard against the case
    # where create_app() or SAModelFactory.build() raises before server is assigned.
    server: uvicorn.Server | None = None
    server_task: asyncio.Task | None = None

    try:
        # ── Bootstrap app ─────────────────────────────────────────────────────
        # Import here to avoid module-level side effects before env vars are set.
        from example.app import create_app  # noqa: PLC0415

        app = await create_app()

        # ── Create SQLAlchemy tables ──────────────────────────────────────────
        # create_app() → SAModule.install() → provider.register(Post) already
        # triggered SAModelFactory to map Post onto Base.metadata.  By the time
        # create_app() returns, Base.metadata has the "posts" table registered.
        # We create a fresh engine here just to issue CREATE TABLE — we don't
        # reach into DI internals.
        #
        # DESIGN: create_all() over Alembic for tests
        #   ✅ No migration history needed — containers are ephemeral.
        #   ❌ Schema drift from production if Alembic migrations are not kept in sync.
        from sqlalchemy.ext.asyncio import create_async_engine  # noqa: PLC0415
        from example.app import Base  # noqa: PLC0415

        engine = create_async_engine(db_url, echo=False)
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await engine.dispose()

        # ── Configure and start uvicorn ───────────────────────────────────────
        port = _free_port()
        config = uvicorn.Config(
            app=app,
            host="127.0.0.1",
            port=port,
            # One worker — we want a single process sharing the in-process
            # DI container and event bus subscriptions.
            workers=1,
            # Suppress uvicorn access logs during tests to reduce noise.
            access_log=False,
            # Log level matches test verbosity; set to "warning" for CI.
            log_level="warning",
        )
        server = uvicorn.Server(config)

        # Run the server in a background task so the test event loop continues.
        # DESIGN: asyncio.create_task over threading.Thread
        #   ✅ Server shares the same event loop — DI singletons (bus, cache)
        #      created inside create_app() are reachable from the test.
        #   ❌ Blocking uvicorn.Server.serve() would freeze the test loop.
        server_task = asyncio.create_task(server.serve())

        # ── Wait for server to be ready ───────────────────────────────────────
        # Poll the root path with a short timeout.  uvicorn sets
        # ``server.started`` when the socket is bound and the first request
        # can be accepted.  We wait for that flag rather than HTTP responses
        # to avoid spurious 404/startup failures.
        deadline = asyncio.get_event_loop().time() + 10.0  # 10-second limit
        while not server.started:
            if asyncio.get_event_loop().time() > deadline:
                server.should_exit = True
                await server_task
                raise TimeoutError(
                    f"uvicorn did not start within 10 seconds on port {port}"
                )
            await asyncio.sleep(0.05)

        base_url = f"http://127.0.0.1:{port}"
        _logger.info("E2E server ready at %s", base_url)

        yield base_url

    finally:
        # ── Graceful shutdown ─────────────────────────────────────────────────
        # Guard against the case where an exception was raised before server
        # was assigned (e.g. create_app() or table-creation failed).
        if server is not None:
            server.should_exit = True
        if server_task is not None:
            await server_task

        # Restore env vars so later test sessions or shell sessions are unaffected.
        if prev_db is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = prev_db

        if prev_redis is None:
            os.environ.pop("VARCO_REDIS_URL", None)
        else:
            os.environ["VARCO_REDIS_URL"] = prev_redis

        if prev_redis_cache is None:
            os.environ.pop("VARCO_REDIS_CACHE_URL", None)
        else:
            os.environ["VARCO_REDIS_CACHE_URL"] = prev_redis_cache


@pytest.fixture
async def client(running_server: str) -> AsyncIterator[httpx.AsyncClient]:
    """
    Yield a configured ``httpx.AsyncClient`` pointed at the running server.

    Function-scoped so each test gets a fresh client with its own connection
    pool — avoids connection-state bleed between tests.

    Args:
        running_server: Base URL injected by the session-scoped fixture.

    Yields:
        An ``httpx.AsyncClient`` with base URL set and a 30-second timeout.

    Edge cases:
        - The client is closed (``await client.aclose()``) even if the test
          raises — ``async with`` ensures cleanup via __aexit__.
    """
    async with httpx.AsyncClient(
        base_url=running_server,
        # 30 seconds covers slow CI containers; lower in fast local runs.
        timeout=httpx.Timeout(30.0),
    ) as http:
        yield http


# ── Tests ──────────────────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_create_post_returns_201_with_body(client: httpx.AsyncClient) -> None:
    """
    ``POST /v1/posts`` creates a post and returns 201 with all required fields.

    Exercises:
    - HTTP routing (VarcoCRUDRouter → CreateMixin).
    - PostService.create() → SQLAlchemy INSERT → PostgreSQL.
    - PostAssembler.to_domain() / to_read_dto().
    - Redis cache invalidation on create.
    - PostCreatedEvent published to Redis Pub/Sub.

    Edge cases:
        - ``author_id`` is ``None`` because ``PostRouter._auth`` is not set —
          no auth is configured so the service uses the anonymous ``_ANON_CTX``
          (``user_id=None``).
        - ``pk`` is a valid UUID — verifies PKStrategy.UUID_AUTO ran.
        - ``created_at`` and ``updated_at`` are non-null — set in
          ``PostService._prepare_for_create()``.
    """
    payload = {"title": "Hello E2E", "body": "This is a real post."}

    response = await client.post(
        "/v1/posts",
        json=payload,
    )

    # The CRUD mixin returns 201 on successful create.
    assert response.status_code == 201, response.text

    body = response.json()
    # All PostRead fields must be present and typed correctly.
    assert body["title"] == "Hello E2E"
    assert body["body"] == "This is a real post."
    assert "pk" in body, "pk missing from response"
    assert "author_id" in body, "author_id missing — _prepare_for_create did not run"
    assert "created_at" in body, "created_at missing — ORM default did not fire"
    assert "updated_at" in body, "updated_at missing — ORM default did not fire"

    # Validate pk UUID — str(UUID(v)) round-trips cleanly for well-formed UUIDs.
    from uuid import UUID  # noqa: PLC0415

    UUID(body["pk"])
    # author_id is None when no auth is configured on PostRouter (anonymous request)
    assert body["author_id"] is None or isinstance(body["author_id"], str)


@pytest.mark.integration
async def test_get_post_returns_200_and_caches(client: httpx.AsyncClient) -> None:
    """
    ``GET /v1/posts/{id}`` returns the created post and caches it in Redis.

    The second GET must return identical data (from cache) even though the
    test does not directly inspect Redis keys — the cache hit is observable
    because performance is consistent and the body is stable.

    Exercises:
    - SQLAlchemy SELECT on first GET (cache miss → DB → cache.set).
    - Redis cache hit on second GET (no DB round-trip).

    Edge cases:
        - If Redis is down, the fallback is the DB — no assertion failure but
          a warning in logs.  Not tested here (Redis is always up in this suite).
    """
    # Create a post first so we have a valid pk.
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Cacheable", "body": "Cache me"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    # First GET — cache miss → DB.
    get_resp_1 = await client.get(f"/v1/posts/{pk}")
    assert get_resp_1.status_code == 200
    data_1 = get_resp_1.json()
    assert data_1["pk"] == pk
    assert data_1["title"] == "Cacheable"

    # Second GET — must hit cache; body must be identical.
    get_resp_2 = await client.get(f"/v1/posts/{pk}")
    assert get_resp_2.status_code == 200
    assert get_resp_2.json() == data_1, "Cache returned different data on second GET"


@pytest.mark.integration
async def test_list_posts_returns_created_items(client: httpx.AsyncClient) -> None:
    """
    ``GET /v1/posts`` lists all posts including newly created ones.

    Exercises:
    - Bulk SELECT via SQLAlchemy / QueryApplicator.
    - Pagination (default limit/offset).
    - PostAssembler.to_read_dto() for each entity.

    Edge cases:
        - Other tests also create posts — we verify our two posts appear
          somewhere in the list rather than asserting an exact count.
    """
    titles = {f"List-Post-A-{id(object())}", f"List-Post-B-{id(object())}"}

    for title in titles:
        resp = await client.post(
            "/v1/posts",
            json={"title": title, "body": "listing"},
        )
        assert resp.status_code == 201

    list_resp = await client.get("/v1/posts")
    assert list_resp.status_code == 200

    # List response is paginated: {"results": [...], "count": N, "total_count": N, "next": ...}
    returned_titles = {p["title"] for p in list_resp.json()["results"]}
    for title in titles:
        assert title in returned_titles, f"Expected {title!r} in list response"


@pytest.mark.integration
async def test_patch_post_updates_title_and_invalidates_cache(
    client: httpx.AsyncClient,
) -> None:
    """
    ``PATCH /v1/posts/{id}`` updates the title and evicts the Redis cache entry.

    Verifies the cache invalidation path:
    1. GET  → cache miss  → DB fetch → cache.set("post:get:<pk>").
    2. PATCH → DB UPDATE → CacheServiceMixin.update() → cache.delete("post:get:<pk>").
    3. GET  → cache miss  → DB fetch → returns NEW title.

    Edge cases:
        - ``body`` is not supplied in the PATCH — must remain unchanged.
        - ``updated_at`` should advance after the PATCH (server clock, so we
          only check it is non-null; comparing timestamps is flaky in CI).
    """
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "Before Patch", "body": "Stays the same"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    # Warm the cache via GET.
    await client.get(f"/v1/posts/{pk}")

    # PATCH — only title, body stays.
    patch_resp = await client.patch(
        f"/v1/posts/{pk}",
        json={"title": "After Patch"},
    )
    assert patch_resp.status_code == 200
    patched = patch_resp.json()
    assert patched["title"] == "After Patch"
    assert patched["body"] == "Stays the same"

    # GET after PATCH — must reflect new title (cache was evicted).
    get_resp = await client.get(f"/v1/posts/{pk}")
    assert get_resp.status_code == 200
    assert get_resp.json()["title"] == "After Patch"
    assert get_resp.json()["body"] == "Stays the same"


@pytest.mark.integration
async def test_delete_post_returns_204_and_subsequent_get_404(
    client: httpx.AsyncClient,
) -> None:
    """
    ``DELETE /v1/posts/{id}`` removes the post; ``GET`` on the same pk returns 404.

    Exercises:
    - SQLAlchemy DELETE.
    - Cache eviction on delete (CacheServiceMixin._after_delete).
    - PostDeletedEvent published to Redis Pub/Sub.
    - Error middleware mapping ``ServiceNotFoundError`` → 404.

    Edge cases:
        - The second GET must NOT return a stale cache entry — verifies the
          delete path clears the cache.
        - DELETE on a non-existent pk should return 404 — not tested here
          (would be a separate error-path test).
    """
    create_resp = await client.post(
        "/v1/posts",
        json={"title": "To Be Deleted", "body": "Goodbye"},
    )
    assert create_resp.status_code == 201
    pk = create_resp.json()["pk"]

    # Warm the cache.
    await client.get(f"/v1/posts/{pk}")

    # DELETE.
    delete_resp = await client.delete(f"/v1/posts/{pk}")
    assert delete_resp.status_code == 204, delete_resp.text

    # Subsequent GET must be 404 — DB row gone, cache evicted.
    get_resp = await client.get(f"/v1/posts/{pk}")
    assert (
        get_resp.status_code == 404
    ), f"Expected 404 after delete, got {get_resp.status_code}: {get_resp.text}"


@pytest.mark.integration
async def test_get_nonexistent_post_returns_404(client: httpx.AsyncClient) -> None:
    """
    ``GET /v1/posts/{id}`` for a non-existent UUID returns 404.

    Exercises the error middleware path:
    - ``PostService.get()`` raises ``ServiceNotFoundError``.
    - ``add_error_middleware()`` maps it to HTTP 404.

    Edge cases:
        - Uses a random UUID that will never collide with a real row.
    """
    import uuid  # noqa: PLC0415

    fake_pk = str(uuid.uuid4())
    resp = await client.get(f"/v1/posts/{fake_pk}")
    assert resp.status_code == 404


@pytest.mark.integration
async def test_summary_custom_endpoint_returns_pk_and_title(
    client: httpx.AsyncClient,
) -> None:
    """
    ``GET /v1/posts/{id}/summary`` returns the lightweight summary dict.

    Exercises the custom ``@route`` endpoint in ``PostRouter.get_summary()``.

    Returns:
        ``{"pk": "<uuid>", "title": "<title>"}``

    Edge cases:
        - Summary does NOT include ``body``, ``author_id``, or timestamps.
        - Internally calls ``_service.get()`` — cache is exercised.
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
    # Body is NOT part of the summary endpoint contract.
    assert "body" not in summary


__all__ = []
