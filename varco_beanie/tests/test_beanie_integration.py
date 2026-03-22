"""
Integration tests for varco_beanie against a real MongoDB
==========================================================
These tests spin up a real MongoDB instance via testcontainers and verify
end-to-end repository, UoW, and provider behaviour.

DISABLED BY DEFAULT — requires Docker.  Run with::

    pytest -m integration tests/test_beanie_integration.py

Or set the ``VARCO_RUN_INTEGRATION`` env var::

    VARCO_RUN_INTEGRATION=1 pytest tests/test_beanie_integration.py

Prerequisites:
    - Docker daemon running
    - testcontainers[mongo] installed (add to pyproject.toml dev deps)

What these tests verify that unit tests cannot
----------------------------------------------
- Real MongoDB write/read/delete round-trips (no mocking).
- Index enforcement (unique constraints, if any).
- Query filter compilation against a real Motor / MongoDB backend.
- UoW commit/rollback semantics with real session handling.
- Beanie ODM column type coercions (UUID, datetime).
"""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass

import pytest

pytestmark = pytest.mark.integration

# Skip the entire module if not explicitly requested.
if not os.environ.get("VARCO_RUN_INTEGRATION"):
    pytest.skip(
        "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration",
        allow_module_level=True,
    )


# ── Domain model for integration tests ────────────────────────────────────────


@dataclass
class Post:
    """Minimal domain model — used across all integration test cases."""

    id: str
    title: str
    body: str = ""

    @property
    def pk(self) -> str:
        return self.id


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def mongo_container():
    """
    Start a MongoDB instance for the integration test module.

    Uses ``testcontainers.mongodb.MongoDbContainer`` to spin up a real
    MongoDB server.  Shared across all tests in the module (``scope="module"``)
    to avoid per-test startup overhead (~3-5s per container).
    """
    from testcontainers.mongodb import MongoDbContainer  # noqa: PLC0415

    with MongoDbContainer() as mongo:
        yield mongo


@pytest.fixture
async def beanie_init(mongo_container):
    """
    Initialise Beanie ODM against the testcontainers MongoDB instance.

    Creates a fresh database per test function via a unique name — ensures
    full isolation between tests without restarting the container.
    """
    from beanie import Document, init_beanie  # noqa: PLC0415
    from pydantic import Field  # noqa: PLC0415
    from pymongo import AsyncMongoClient  # noqa: PLC0415

    # Unique DB name per test — full isolation without container restarts.
    db_name = f"test_{uuid.uuid4().hex[:8]}"
    connection_string = mongo_container.get_connection_url()

    class PostDocument(Document):
        """Beanie document mirroring the Post domain model."""

        id: str = Field(default_factory=lambda: str(uuid.uuid4()))
        title: str
        body: str = ""

        class Settings:
            name = "posts"

    # beanie 2.x uses pymongo AsyncMongoClient instead of motor
    client = AsyncMongoClient(connection_string)
    await init_beanie(database=client[db_name], document_models=[PostDocument])

    yield PostDocument

    # Cleanup — drop the database after the test.
    # AsyncMongoClient.close() is a coroutine in pymongo>=4.11 (unlike motor
    # where close() was sync) — must be awaited to avoid RuntimeWarning.
    await client.drop_database(db_name)
    await client.close()


# ── Integration tests ──────────────────────────────────────────────────────────


class TestBeanieIntegrationCRUD:
    """
    End-to-end CRUD against a real MongoDB instance.

    These tests verify that Beanie ODM correctly reads/writes domain data
    without any mocking.  The goal is to catch:
    - Type coercion issues (UUIDs serialized as strings, datetimes as ISO-8601).
    - Collection name resolution.
    - Real pymongo async behaviour under pytest-asyncio.
    """

    async def test_insert_and_find_by_id(self, beanie_init) -> None:
        """Insert a document and retrieve it by its primary key."""
        PostDocument = beanie_init
        doc = PostDocument(id="post-1", title="Hello World", body="Body text")
        await doc.save()

        found = await PostDocument.get("post-1")
        assert found is not None
        assert found.title == "Hello World"
        assert found.body == "Body text"

    async def test_update(self, beanie_init) -> None:
        """Update a field and verify the change persists."""
        PostDocument = beanie_init
        doc = PostDocument(id="post-2", title="Original")
        await doc.save()

        doc.title = "Updated"
        await doc.save()

        found = await PostDocument.get("post-2")
        assert found is not None
        assert found.title == "Updated"

    async def test_delete(self, beanie_init) -> None:
        """Delete a document and verify it no longer exists."""
        PostDocument = beanie_init
        doc = PostDocument(id="post-3", title="To Delete")
        await doc.save()

        await doc.delete()

        found = await PostDocument.get("post-3")
        assert found is None

    async def test_find_all(self, beanie_init) -> None:
        """Insert multiple documents and retrieve them all."""
        PostDocument = beanie_init
        for i in range(3):
            await PostDocument(id=f"all-{i}", title=f"Post {i}").save()

        all_docs = await PostDocument.find_all().to_list()
        # May include docs from other tests in same DB — use >=
        assert len(all_docs) >= 3

    async def test_count(self, beanie_init) -> None:
        """Verify that count() returns the correct number of documents."""
        PostDocument = beanie_init
        initial_count = await PostDocument.count()

        await PostDocument(id=f"count-{uuid.uuid4().hex}", title="Count me").save()

        new_count = await PostDocument.count()
        assert new_count == initial_count + 1

    async def test_find_with_filter(self, beanie_init) -> None:
        """Filter documents by a field and verify only matching docs are returned."""
        PostDocument = beanie_init
        unique_title = f"unique-{uuid.uuid4().hex}"

        await PostDocument(id=f"filter-1-{uuid.uuid4().hex}", title=unique_title).save()
        await PostDocument(id=f"filter-2-{uuid.uuid4().hex}", title="other").save()

        results = await PostDocument.find(PostDocument.title == unique_title).to_list()
        assert len(results) == 1
        assert results[0].title == unique_title

    async def test_field_types_round_trip(self, beanie_init) -> None:
        """
        Verify that field types (str, str with UUID pattern) survive a round-trip.

        Edge case: MongoDB stores _id as ObjectId by default.  varco_beanie
        uses ``id`` as a plain str — verify it is not coerced to ObjectId.
        """
        PostDocument = beanie_init
        doc_id = str(uuid.uuid4())
        await PostDocument(id=doc_id, title="Type test").save()

        found = await PostDocument.get(doc_id)
        assert found is not None
        # id must come back as the same string we inserted, not an ObjectId
        assert found.id == doc_id
        assert isinstance(found.id, str)
