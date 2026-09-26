"""
Pytest configuration for varco_beanie tests.

DESIGN: Beanie Document collection bypass
    Beanie Document.__init__ calls get_pymongo_collection() which raises
    CollectionWasNotInitialized unless init_beanie() has been called against
    a real MongoDB.  Unit tests bypass this by patching the classmethod to
    return a MagicMock — by the time the check runs (line 209 of documents.py),
    Pydantic has already committed all field values, so attribute assertions
    remain valid.

    Tradeoffs:
        ✅ No MongoDB connection required — tests run in any CI environment
        ✅ Field values are set correctly — Pydantic runs before the check
        ❌ Does not verify MongoDB-level constraints (e.g., index enforcement)
        Alternative: run against a real motor/mongo — better for integration
                     tests, too heavy for unit tests.

Thread safety:  ✅ — monkeypatch is scoped to each test function
Async safety:   ✅ — no async code involved in this fixture
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

# ── Session-scoped MongoDB container (Plan 012 / RT1, Steps 7-8) ──────────────
#
# ``mongo_url`` starts a single MongoDB container ONCE per test session,
# replacing the per-file ``MongoDbContainer`` fixtures previously scattered
# across ~10 test files.
#
# Per-test namespacing rule: the container is shared, so every test must use
# its own uniquely-named database (e.g. ``f"test_{uuid4().hex[:8]}"``) via
# ``AsyncIOMotorClient(mongo_url)[db_name]`` / ``init_beanie(database=...)``.
# A test that needs a pristine server (e.g. asserting on the full list of
# databases) must declare its own function-scoped ``mongo_container_fresh``
# fixture instead.
#
# ``VARCO_TEST_MONGO_URL`` overrides the container entirely (Open Question 1)
# — when set, no container is started; the value is used as-is and reported
# via ``request.config.stash``, never silently falling back to a container.


@pytest.fixture(scope="session")
def mongo_url(request: pytest.FixtureRequest) -> str:
    """
    Session-scoped MongoDB connection URL — real container or override.

    See the module docstring for the per-test namespacing rule and the
    ``VARCO_TEST_MONGO_URL`` override contract.

    Yields:
        A ``mongodb://`` connection URL for the shared server.
    """
    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip(
            "Integration tests disabled — set VARCO_RUN_INTEGRATION=1 or use -m integration"
        )

    override = os.environ.get("VARCO_TEST_MONGO_URL")
    if override:
        request.config.stash.setdefault("varco_test_overrides", []).append(("mongo", override))
        yield override
        return

    from testcontainers.mongodb import MongoDbContainer  # noqa: PLC0415

    with MongoDbContainer("mongo:7") as container:
        yield container.get_connection_url()


@pytest.fixture(autouse=True)
def bypass_beanie_collection_check(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """
    Patch the Document collection accessor so unit tests don't need MongoDB.

    Beanie's Document.__init__ calls a collection accessor classmethod which
    raises CollectionWasNotInitialized if init_beanie() was never called.
    Patching it to return a MagicMock lets Documents be instantiated in unit
    tests without a real database connection.

    The method name varies by beanie version:
        - beanie 2.x:       get_pymongo_collection  (motor dropped)
        - beanie 1.27–1.30: get_motor_collection
    This fixture tries both names so it works across versions without pinning.

    Args:
        request:     pytest's request fixture — used to detect integration
                     markers so the bypass is skipped for those tests.
        monkeypatch: pytest's monkeypatch fixture — automatically scoped to
                     the current test function, so the patch is reversed after
                     each test.

    Edge cases:
        - Tests marked with ``@pytest.mark.integration`` skip this bypass
          because they use a real MongoDB container and call init_beanie().
        - If neither accessor method exists, the bypass is silently skipped —
          the test will fail naturally if a collection operation is triggered.
    """
    # Integration tests use a real MongoDB — do not apply the mock bypass,
    # otherwise init_beanie() would never be reached and CRUD ops would fail.
    if request.node.get_closest_marker("integration"):
        return

    from beanie.odm.documents import (
        Document,
    )  # local import — avoid top-level beanie init

    # The collection accessor classmethod was renamed across beanie versions:
    #   - beanie 2.x:      get_pymongo_collection  (motor dropped; native pymongo)
    #   - beanie 1.27–1.30: get_motor_collection
    # Try both names so this bypass works across major versions without
    # pinning to a specific beanie release.
    for _method_name in ("get_pymongo_collection", "get_motor_collection"):
        if hasattr(Document, _method_name):
            monkeypatch.setattr(
                Document,
                _method_name,
                classmethod(lambda cls: MagicMock()),
            )
            break


@pytest.fixture(autouse=True)
def _select_beanie_backends(monkeypatch: pytest.MonkeyPatch) -> None:
    """Plan 047 §D-tests — see varco_kafka's twin fixture docstring."""
    monkeypatch.setenv("VARCO_DLQ_BACKEND", "beanie")
    monkeypatch.setenv("VARCO_IDEMPOTENCY_BACKEND", "beanie")
    monkeypatch.setenv("VARCO_JOB_STORE_BACKEND", "beanie")
