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

from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def bypass_beanie_collection_check(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Patch Document.get_pymongo_collection so unit tests don't need MongoDB.

    Beanie's Document.__init__ calls get_pymongo_collection() which raises
    CollectionWasNotInitialized if init_beanie() was never called.  Patching
    it to return a MagicMock lets Documents be instantiated in unit tests
    without a real database connection.

    Args:
        monkeypatch: pytest's monkeypatch fixture — automatically scoped to
                     the current test function, so the patch is reversed after
                     each test.

    Edge cases:
        - Tests that check pymongo collection behaviour will need a real
          MongoDB instead of relying on this fixture.
    """
    from beanie.odm.documents import (
        Document,
    )  # local import — avoid top-level beanie init

    # get_pymongo_collection is a classmethod — wrap with classmethod() so
    # Python's descriptor protocol routes it correctly when called via instance.
    monkeypatch.setattr(
        Document,
        "get_pymongo_collection",
        classmethod(lambda cls: MagicMock()),
    )
