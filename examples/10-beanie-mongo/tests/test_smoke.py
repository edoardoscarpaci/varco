"""
test_smoke.py
=============
Integration smoke tests for the ``10-beanie-mongo`` example.

All tests require a running MongoDB instance — tagged ``pytest.mark.integration``
and skipped by default.  Run with::

    uv run pytest examples/10-beanie-mongo/tests/ -v -m integration

What these tests verify
-----------------------
1. **Create + read round-trip** — POST a post, GET it back by ID.
2. **Update** — PUT a post with new values, GET it back, verify changes.
3. **Delete** — DELETE a post, confirm GET returns 404.
4. **List** — GET /v1/posts returns a paged envelope with ``results``.
5. **Health check** — GET /health returns 200.

DESIGN: session-scoped fixtures
    ✅ One MongoDB container + one ``create_app()`` call shared across all
       tests — fast overall (only one init_beanie call).
    ✅ ``BeanieRepositoryProvider.init()`` is called once before the client
       is created.  Tests rely on the same Beanie Document registry.
    ✅ Tests are additive — each creates its own posts and asserts only on
       those, so shared state is not a problem.

    ``ASGITransport`` does NOT trigger FastAPI ``lifespan`` or
    ``@app.on_event("startup")``.  ``create_app`` accepts a pre-initialized
    ``provider`` argument so the fixture can call ``await provider.init()``
    before any requests.

Thread safety:  ❌ Single asyncio event loop.
Async safety:   ✅ All test functions are ``async def``.
"""

from __future__ import annotations

import sys
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from httpx import ASGITransport
from pymongo import AsyncMongoClient  # noqa: E402
from testcontainers.mongodb import MongoDbContainer  # noqa: E402

# ── sys.path guard (belt-and-suspenders) ──────────────────────────────────────
_EXAMPLE_ROOT = str(Path(__file__).parent.parent.resolve())
if _EXAMPLE_ROOT not in sys.path:
    sys.path.insert(0, _EXAMPLE_ROOT)

pytestmark = pytest.mark.integration


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def mongo_url():
    """
    Start a MongoDB 7 container and yield the connection URL.

    Scope is ``"session"`` — the container is shared across all tests.

    Yields:
        MongoDB connection URL string, e.g. ``"mongodb://127.0.0.1:32768"``.

    Edge cases:
        - Requires Docker daemon to be running.
        - Uses port 27017 inside the container; host port is ephemeral.
    """
    with MongoDbContainer("mongo:7") as m:
        yield m.get_connection_url()


@pytest.fixture(scope="session")
async def app_client(mongo_url):
    """
    Initialize Beanie and yield a session-scoped ``httpx.AsyncClient``.

    ``ASGITransport`` does NOT trigger ``@app.on_event("startup")``, so
    ``BeanieRepositoryProvider.init()`` (which calls ``init_beanie()``) must
    be called explicitly before creating the HTTP client.

    Steps:
    1. Build a ``BeanieRepositoryProvider`` and call ``await provider.init()``.
    2. Pass the pre-initialized provider to ``create_app()`` so the startup
       hook is skipped (init_beanie would fail on re-registration otherwise).
    3. Create the ``httpx.AsyncClient`` with ``ASGITransport``.

    Yields:
        An ``httpx.AsyncClient`` connected to the in-process FastAPI app.

    Edge cases:
        - ``await provider.init()`` calls ``init_beanie()`` globally — subsequent
          calls with the same Document classes are safe (idempotent).
        - The Motor client is shared across the fixture and the provider.
    """
    from app import create_app  # noqa: PLC0415
    from models import Post  # noqa: PLC0415
    from varco_beanie.config import BeanieSettings  # noqa: PLC0415
    from varco_beanie.provider import BeanieRepositoryProvider  # noqa: PLC0415

    # Build and initialize the provider explicitly — ASGITransport doesn't fire startup.
    client = AsyncMongoClient(mongo_url)
    settings = BeanieSettings(
        mongo_client=client,
        db_name="test_db",
        entity_classes=(Post,),
    )
    provider = BeanieRepositoryProvider(settings=settings)
    await provider.init()  # registers all Beanie Document classes

    # Pass the pre-initialized provider so create_app skips the startup hook.
    fastapi_app = create_app(mongo_url, provider=provider)

    async with httpx.AsyncClient(
        transport=ASGITransport(app=fastapi_app),
        base_url="http://test",
    ) as http_client:
        yield http_client


# ── Helper ─────────────────────────────────────────────────────────────────────


async def _create_post(client: httpx.AsyncClient, *, title: str, content: str, author: str) -> dict:
    """
    POST /v1/posts and return the parsed JSON response body.

    Args:
        client:  Session-scoped HTTP client.
        title:   Post headline.
        content: Post body text.
        author:  Author display name.

    Returns:
        Parsed JSON body of the 201 Created response.

    Raises:
        AssertionError: Non-201 status.
    """
    resp = await client.post(
        "/v1/posts", json={"title": title, "content": content, "author": author}
    )
    assert resp.status_code == 201, f"Expected 201, got {resp.status_code}: {resp.text}"
    return resp.json()


# ── Tests ──────────────────────────────────────────────────────────────────────


class TestHealthCheck:
    """GET /health/live returns 200."""

    async def test_health_returns_ok(self, app_client) -> None:
        """Liveness probe must return 200 with ``{"status": "healthy"}``.

        The app defines no ``/health`` route of its own: ``create_varco_app``
        mounts varco's ``HealthRouter`` under ``/health`` (aggregate at
        ``/health/``, so a bare ``/health`` is a 307 redirect).  ``/health/live``
        is the dependency-free probe, so its body is stable.
        """
        resp = await app_client.get("/health/live")
        assert resp.status_code == 200
        assert resp.json() == {"status": "healthy"}


class TestCreateAndRead:
    """POST a post, GET it back by ID."""

    async def test_create_returns_201(self, app_client) -> None:
        """POST /v1/posts returns 201 with the created post data."""
        data = await _create_post(
            app_client,
            title="Hello MongoDB",
            content="Beanie ODM example",
            author="alice",
        )
        assert data["title"] == "Hello MongoDB"
        assert data["content"] == "Beanie ODM example"
        assert data["author"] == "alice"
        UUID(data["pk"])  # must be a valid UUID

    async def test_get_by_id_returns_post(self, app_client) -> None:
        """GET /v1/posts/{id} returns the correct post."""
        created = await _create_post(
            app_client,
            title="Readable Post",
            content="This is the content",
            author="bob",
        )
        pk = created["pk"]

        resp = await app_client.get(f"/v1/posts/{pk}")
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["title"] == "Readable Post"
        assert data["author"] == "bob"
        assert data["pk"] == pk


class TestUpdate:
    """PUT /v1/posts/{id} updates the post."""

    async def test_put_updates_post(self, app_client) -> None:
        """PUT /v1/posts/{id} returns 200 with the updated post."""
        created = await _create_post(
            app_client,
            title="Original Title",
            content="Original content",
            author="carol",
        )
        pk = created["pk"]

        resp = await app_client.put(
            f"/v1/posts/{pk}",
            json={
                "title": "Updated Title",
                "content": "Updated content",
                "author": "carol",
            },
        )
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        data = resp.json()
        assert data["title"] == "Updated Title"
        assert data["content"] == "Updated content"
        assert data["pk"] == pk

        # Verify persistence via GET
        resp2 = await app_client.get(f"/v1/posts/{pk}")
        assert resp2.json()["title"] == "Updated Title"


class TestDelete:
    """DELETE /v1/posts/{id} removes the post."""

    async def test_delete_returns_204(self, app_client) -> None:
        """DELETE /v1/posts/{id} returns 204; subsequent GET returns 404."""
        created = await _create_post(
            app_client,
            title="To Delete",
            content="Will be gone",
            author="dave",
        )
        pk = created["pk"]

        resp = await app_client.delete(f"/v1/posts/{pk}")
        assert resp.status_code == 204, f"Expected 204, got {resp.status_code}: {resp.text}"

        resp2 = await app_client.get(f"/v1/posts/{pk}")
        assert resp2.status_code == 404


class TestList:
    """GET /v1/posts returns a paged envelope."""

    async def test_list_returns_results(self, app_client) -> None:
        """
        GET /v1/posts returns a paged envelope with a ``results`` key.

        Creates two posts with a unique author tag and verifies both appear
        in the list — without assuming the DB is empty.
        """
        tag = "ListTestBeanie"
        await _create_post(app_client, title=f"{tag} 1", content="c1", author=tag)
        await _create_post(app_client, title=f"{tag} 2", content="c2", author=tag)

        resp = await app_client.get("/v1/posts")
        assert resp.status_code == 200
        body = resp.json()
        all_posts = body["results"] if isinstance(body, dict) and "results" in body else body

        tagged = [p for p in all_posts if p.get("author") == tag]
        assert len(tagged) >= 2, f"Expected at least 2 posts with author={tag!r}, got {len(tagged)}"
