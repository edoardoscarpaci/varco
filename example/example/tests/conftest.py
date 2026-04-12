"""
conftest.py
===========
Shared pytest fixtures for the varco example integration test suite.

All integration tests require Docker (testcontainers) and are tagged with
``@pytest.mark.integration``.  Run the full suite with::

    cd example
    uv run pytest example/tests/ -m integration -v

Skip integration tests (fast unit tests only)::

    uv run pytest example/tests/ -m "not integration" -v

Fixture topology
----------------
The session-scoped fixtures form a startup chain::

    postgres_container  ─┐
                         ├─→ running_server (uvicorn + FastAPI)
    redis_container     ─┘         │
                                   ▼
                             client (httpx.AsyncClient)  — per test
                             alice_token (JWT)           — per session
                             bob_token   (JWT)           — per session

A single Docker + uvicorn instance is shared across all tests in the session,
keeping the test suite fast.  Tests achieve isolation at the data level by
using unique titles / UUIDs rather than resetting the database between tests.

Session-scoped event loop
-------------------------
A single asyncio event loop runs for the entire test session so that session-
scoped async fixtures (``running_server``, ``alice_token``) and function-scoped
async test functions all share the same loop.  Without this, uvicorn's background
task runs in one loop while ``httpx.AsyncClient`` runs in another —
requests never complete.

DESIGN: conftest.py fixtures over per-file fixture definitions
    ✅ Both ``test_e2e_integration.py`` and ``test_api_auth.py`` share one
       running server — startup cost paid once per session.
    ✅ ``conftest.py`` is always discovered regardless of pytest rootdir.
    ❌ A failing fixture here blocks ALL integration tests — acceptable trade-off
       given the shared infrastructure nature.

Thread safety:  ⚠️ Tests share one uvicorn server — do NOT run with
                   ``-n`` (pytest-xdist) without a per-worker port offset.
Async safety:   ✅ All fixtures are ``async def``; pytest-asyncio handles them.

📚 Docs
- 🔍 https://testcontainers-python.readthedocs.io/ — PostgresContainer, RedisContainer
- 🔍 https://www.uvicorn.org/settings/ — uvicorn.Config / uvicorn.Server
- 🐍 https://www.python-httpx.org/async/ — httpx.AsyncClient
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


# ── Port helper ────────────────────────────────────────────────────────────────


def _free_port() -> int:
    """
    Find a free TCP port on the loopback interface.

    Opens a socket, binds to port 0 (OS assigns the next free ephemeral port),
    reads the assigned number, then closes the socket.  There is a short race
    window between ``close()`` and uvicorn binding — acceptable in single-process
    test environments.

    Returns:
        A free port number at the moment of return.

    Edge cases:
        - Rarely the same port is allocated twice concurrently.  Using session
          scope for the server creation reduces the chance to near-zero.
        - Port 0 cannot be used directly in ``httpx.AsyncClient`` base URL.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # SO_REUSEADDR avoids "address already in use" if the last process exited
        # in TIME_WAIT state less than 60 seconds ago.
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# ── Session-scoped container fixtures ─────────────────────────────────────────


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[PostgresContainer]:
    """
    Start a PostgreSQL 16 container once for the entire test session.

    Uses testcontainers' ephemeral port mapping — no fixed host port is
    reserved.  ``get_connection_url()`` returns the mapped URL.

    Yields:
        A running ``PostgresContainer`` with the test database ready.

    Edge cases:
        - Docker must be running on the host.
        - ``docker pull`` on a cold cache adds 5–20 s on first run.
    """
    with PostgresContainer("postgres:16-alpine") as pg:
        _logger.info("PostgresContainer started: %s", pg.get_connection_url())
        yield pg


@pytest.fixture(scope="session")
def redis_container() -> Iterator[RedisContainer]:
    """
    Start a Redis 7 container once for the entire test session.

    Used for BOTH the event bus (``VARCO_REDIS_URL``) and the cache backend
    (``VARCO_REDIS_CACHE_URL``), matching the ``create_app()`` bootstrap.

    Yields:
        A running ``RedisContainer``.

    Edge cases:
        - Redis Pub/Sub is ephemeral — messages published before any subscriber
          connects are lost.  The bus starts before any HTTP request arrives so
          no events are lost in practice.
    """
    with RedisContainer("redis:7-alpine") as redis:
        _logger.info("RedisContainer started at port %s", redis.get_exposed_port(6379))
        yield redis


# ── Session-scoped server fixture ─────────────────────────────────────────────


@pytest.fixture(scope="session")
async def running_server(
    postgres_container: PostgresContainer,
    redis_container: RedisContainer,
) -> AsyncIterator[str]:
    """
    Create the FastAPI application, run it under real uvicorn, and yield
    the base URL for HTTP clients.

    Steps:
    1. Build ``DATABASE_URL`` and ``VARCO_REDIS_URL`` from container ports.
    2. Patch environment variables (session scope, restored on teardown).
    3. Call ``create_app()`` — synchronous factory that sets up DI + middleware.
    4. Start ``uvicorn.Server`` in a background ``asyncio.Task``.
    5. Poll until uvicorn reports ``server.started``, then yield the base URL.
    6. Signal uvicorn to exit; await the server task to completion.

    DESIGN: real uvicorn over ``httpx.AsyncClient(app=app, transport=ASGITransport)``
        Using a real server means:
        ✅ Full HTTP stack (TCP socket, HTTP/1.1 framing) is exercised.
        ✅ Middleware stack (CORS, error handler, request context) runs as in prod.
        ✅ VarcoLifespan (bus.start, consumer.start, create_tables) is triggered.
        ❌ Requires an ephemeral port — mitigated by ``_free_port()`` helper.

    DESIGN: asyncio.create_task over threading.Thread for uvicorn
        ✅ Server shares the same event loop — DI singletons (bus, cache)
           created inside the lifespan are reachable from the test.
        ❌ Blocking ``uvicorn.Server.serve()`` would freeze the test loop.

    Yields:
        Base URL string, e.g. ``"http://127.0.0.1:54321"``.

    Raises:
        TimeoutError: uvicorn did not start within 10 seconds.

    Edge cases:
        - ``asyncpg`` requires the ``postgresql+asyncpg://`` scheme; testcontainers
          returns a psycopg2 DSN — we normalise the scheme before setting the env var.
        - ``create_app()`` is synchronous — do NOT await it.
        - Schema creation (CREATE TABLE) happens inside VarcoLifespan (``_bootstrap``),
          not in this fixture.  The lifespan runs when uvicorn starts.

    Thread safety:  ✅ Session-scoped — created once, shared read-only.
    Async safety:   ✅ ``async def`` fixture.
    """
    # ── Build connection URLs from container ports ────────────────────────────
    raw_pg_url = postgres_container.get_connection_url()
    # Normalise scheme: psycopg2 DSN → asyncpg DSN (SQLAlchemy async engine).
    db_url = raw_pg_url.replace("postgresql+psycopg2://", "postgresql+asyncpg://", 1)
    if "postgresql+asyncpg://" not in db_url:
        db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

    redis_host = redis_container.get_container_host_ip()
    redis_port = redis_container.get_exposed_port(6379)
    redis_url = f"redis://{redis_host}:{redis_port}/0"

    # ── Patch env vars (restore on teardown) ─────────────────────────────────
    # Store originals so the finally block can restore them even if creation fails.
    prev_db = os.environ.get("DATABASE_URL")
    prev_redis = os.environ.get("VARCO_REDIS_URL")
    prev_redis_cache = os.environ.get("VARCO_REDIS_CACHE_URL")

    os.environ["DATABASE_URL"] = db_url
    os.environ["VARCO_REDIS_URL"] = redis_url
    os.environ["VARCO_REDIS_CACHE_URL"] = redis_url

    _logger.info("E2E: DATABASE_URL=%s", db_url)
    _logger.info("E2E: VARCO_REDIS_URL=%s", redis_url)

    server: uvicorn.Server | None = None
    server_task: asyncio.Task | None = None

    try:
        # ── Build app (synchronous — do NOT await) ────────────────────────────
        # Import inside the fixture so env vars are set before module-level
        # settings classes (e.g. RedisEventBusSettings) are instantiated.
        from example.app import create_app  # noqa: PLC0415

        # create_app() is a synchronous factory.  It sets up the DI container,
        # scans all packages, and builds the FastAPI app + middleware stack.
        # All async initialisation (create_tables, cache start, bus start) happens
        # inside VarcoLifespan._bootstrap when uvicorn triggers the ASGI lifespan.
        app = create_app()

        # ── Start uvicorn in a background asyncio.Task ────────────────────────
        port = _free_port()
        config = uvicorn.Config(
            app=app,
            host="127.0.0.1",
            port=port,
            workers=1,
            access_log=False,  # suppress access logs in test output
            log_level="warning",
        )
        server = uvicorn.Server(config)
        server_task = asyncio.create_task(server.serve())

        # ── Wait until uvicorn is accepting connections ────────────────────────
        # Poll server.started rather than making HTTP requests — avoids spurious
        # 404/502 errors during the lifespan startup window.
        deadline = asyncio.get_event_loop().time() + 15.0
        while not server.started:
            if asyncio.get_event_loop().time() > deadline:
                server.should_exit = True
                await server_task
                raise TimeoutError(
                    f"uvicorn did not start within 15 seconds on port {port}"
                )
            await asyncio.sleep(0.05)

        base_url = f"http://127.0.0.1:{port}"
        _logger.info("E2E server ready at %s", base_url)
        yield base_url

    finally:
        # ── Graceful shutdown ─────────────────────────────────────────────────
        if server is not None:
            server.should_exit = True
        if server_task is not None:
            await server_task

        # Restore env vars so the test shell / other sessions are unaffected.
        for key, prev in [
            ("DATABASE_URL", prev_db),
            ("VARCO_REDIS_URL", prev_redis),
            ("VARCO_REDIS_CACHE_URL", prev_redis_cache),
        ]:
            if prev is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = prev


# ── Function-scoped client fixture ────────────────────────────────────────────


@pytest.fixture
async def client(running_server: str) -> AsyncIterator[httpx.AsyncClient]:
    """
    Yield a fresh ``httpx.AsyncClient`` for each test function.

    A new client per test avoids connection-state bleed (keep-alive sockets,
    auth header leaking across tests).

    Args:
        running_server: Base URL from the session-scoped server fixture.

    Yields:
        An ``httpx.AsyncClient`` configured with a 30-second timeout.

    Edge cases:
        - ``aclose()`` is called even if the test raises (``async with`` ensures it).
    """
    async with httpx.AsyncClient(
        base_url=running_server,
        timeout=httpx.Timeout(30.0),
    ) as http:
        yield http


# ── Session-scoped token fixtures ─────────────────────────────────────────────


@pytest.fixture(scope="session")
async def alice_token(running_server: str) -> str:
    """
    Log in as alice (admin) and return the JWT access token for the session.

    The token is obtained once per session and reused across tests — alice's
    1-hour expiry is never reached in a normal test run.

    Args:
        running_server: Base URL of the running server.

    Returns:
        Alice's RS256 JWT access token string.

    Edge cases:
        - If the server fails to issue the token, all auth-dependent tests fail
          with a clear ``AssertionError`` here rather than a silent 401 later.
    """
    async with httpx.AsyncClient(
        base_url=running_server, timeout=httpx.Timeout(30.0)
    ) as http:
        resp = await http.post(
            "/auth/login",
            json={"username": "alice", "password": "alice123"},
        )
        assert resp.status_code == 200, f"alice login failed: {resp.text}"
        return resp.json()["access_token"]


@pytest.fixture(scope="session")
async def bob_token(running_server: str) -> str:
    """
    Log in as bob (editor) and return the JWT access token for the session.

    Args:
        running_server: Base URL of the running server.

    Returns:
        Bob's RS256 JWT access token string.
    """
    async with httpx.AsyncClient(
        base_url=running_server, timeout=httpx.Timeout(30.0)
    ) as http:
        resp = await http.post(
            "/auth/login",
            json={"username": "bob", "password": "bob123"},
        )
        assert resp.status_code == 200, f"bob login failed: {resp.text}"
        return resp.json()["access_token"]
