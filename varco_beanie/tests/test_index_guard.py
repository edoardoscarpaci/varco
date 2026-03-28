"""
Unit tests for varco_beanie.index_guard.BeanieIndexGuard
=========================================================
Uses a mock ``AsyncIOMotorDatabase`` whose ``index_information()`` returns
hand-crafted dicts — no real MongoDB connection required.

Sections
--------
- ``IndexDriftReport``      — has_drift, format(), repr
- ``IndexDrift``            — exception carries report
- ``BeanieIndexGuard``      — check() / report() logic:
    - clean state           — all expected indexes present
    - missing unique index  — has_drift, check() raises
    - missing plain index   — has_drift
    - unexpected index      — informational only, no raise
    - compound UniqueConstraint — multi-field expectation
    - composite PK fields   — unique index expected per PK field
    - empty entity list     — no-op, clean report
    - match by key+unique not by name — renaming index name is OK
- Integration               — real MongoDB (marked integration)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from varco_core.meta import FieldHint, PrimaryKey, UniqueConstraint
from varco_core.model import DomainModel
from varco_beanie.index_guard import (
    BeanieIndexGuard,
    IndexDrift,
    IndexDriftReport,
)


# ── Domain model fixtures ─────────────────────────────────────────────────────


@dataclass
class _User(DomainModel):
    """User with a unique email field and a plain index on name."""

    # FieldHint is applied via Annotated — MetaReader reads from type hints.
    name: Annotated[str, FieldHint(index=True)]
    email: Annotated[str, FieldHint(unique=True)]

    class Meta:
        table = "users_ig"


@dataclass
class _Post(DomainModel):
    """Post with a compound UniqueConstraint on (author_id, slug)."""

    author_id: int
    slug: str

    class Meta:
        table = "posts_ig"
        constraints = [UniqueConstraint("author_id", "slug", name="uq_author_slug")]


@dataclass
class _Membership(DomainModel):
    """Composite-PK model — both fields should have unique indexes in Beanie."""

    # Composite PKs via PrimaryKey() annotation — MetaReader extracts
    # composite_pk_fields from these.
    user_id: Annotated[int, PrimaryKey()]
    group_id: Annotated[int, PrimaryKey()]

    class Meta:
        table = "memberships_ig"


# ── Mock DB builder ───────────────────────────────────────────────────────────


def _mock_db(collections: dict[str, dict[str, Any]]) -> MagicMock:
    """
    Build a mock Motor database where each collection's index_information()
    returns the provided dict.

    The MongoDB index_information() format:
        {"<index_name>": {"key": [("<field>", 1), ...], "unique": True/False}, ...}

    Args:
        collections: Mapping of collection_name → index_information dict.

    Returns:
        A MagicMock that behaves like AsyncIOMotorDatabase.
    """
    # Cache collections so the same mock is returned for repeated accesses.
    _colls: dict[str, MagicMock] = {}

    def _get_collection(name: str) -> MagicMock:
        if name not in _colls:
            coll = MagicMock()
            info = collections.get(name, {"_id_": {"key": [("_id", 1)]}})
            coll.index_information = AsyncMock(return_value=info)
            _colls[name] = coll
        return _colls[name]

    # MagicMock.__getitem__ is called as db[name] — use side_effect (not method)
    # so no extra `self` argument is injected.
    db = MagicMock()
    db.__getitem__ = MagicMock(side_effect=_get_collection)
    return db


def _idx(fields: list[str], *, unique: bool = False) -> dict[str, Any]:
    """Build a minimal index_information entry."""
    result: dict[str, Any] = {"key": [(f, 1) for f in fields]}
    if unique:
        result["unique"] = True
    return result


# ── IndexDriftReport ──────────────────────────────────────────────────────────


class TestIndexDriftReport:
    def test_has_drift_false_when_empty(self) -> None:
        report = IndexDriftReport()
        assert report.has_drift is False

    def test_has_drift_true_when_missing_indexes(self) -> None:
        report = IndexDriftReport(
            missing_indexes={"users": ["unique index on 'email'"]}
        )
        assert report.has_drift is True

    def test_has_drift_false_for_only_unexpected(self) -> None:
        # Unexpected indexes alone do NOT constitute drift — they are informational.
        report = IndexDriftReport(unexpected_indexes={"users": ["extra_idx"]})
        assert report.has_drift is False

    def test_format_clean_report(self) -> None:
        report = IndexDriftReport()
        assert "No index drift" in report.format()

    def test_format_missing_indexes(self) -> None:
        report = IndexDriftReport(
            missing_indexes={"users": ["unique index on 'email'"]}
        )
        text = report.format()
        assert "MISSING" in text
        assert "email" in text

    def test_format_unexpected_indexes(self) -> None:
        report = IndexDriftReport(unexpected_indexes={"posts": ["legacy_idx"]})
        text = report.format()
        assert "UNEXPECTED" in text
        assert "legacy_idx" in text

    def test_format_both_sections(self) -> None:
        report = IndexDriftReport(
            missing_indexes={"users": ["unique index on 'email'"]},
            unexpected_indexes={"posts": ["old_idx"]},
        )
        text = report.format()
        assert "MISSING" in text
        assert "UNEXPECTED" in text

    def test_repr(self) -> None:
        report = IndexDriftReport(
            missing_indexes={"u": ["a", "b"]},
            unexpected_indexes={"p": ["c"]},
        )
        text = repr(report)
        assert "missing=2" in text
        assert "unexpected=1" in text

    def test_frozen(self) -> None:
        report = IndexDriftReport()
        with pytest.raises(Exception):
            report.missing_indexes = {}  # type: ignore[misc]


# ── IndexDrift exception ──────────────────────────────────────────────────────


class TestIndexDrift:
    def test_carries_report(self) -> None:
        report = IndexDriftReport(missing_indexes={"users": ["x"]})
        exc = IndexDrift(report)
        assert exc.report is report

    def test_str_contains_format(self) -> None:
        report = IndexDriftReport(
            missing_indexes={"users": ["unique index on 'email'"]}
        )
        exc = IndexDrift(report)
        assert "email" in str(exc)


# ── BeanieIndexGuard.report() ─────────────────────────────────────────────────


class TestBeanieIndexGuardReport:
    async def test_clean_when_all_expected_indexes_present(self) -> None:
        db = _mock_db(
            {
                "users_ig": {
                    "_id_": _idx(["_id"]),
                    "email_1": _idx(["email"], unique=True),
                    "name_1": _idx(["name"]),
                }
            }
        )
        guard = BeanieIndexGuard(_User)
        report = await guard.report(db)
        assert report.has_drift is False

    async def test_missing_unique_index_reported(self) -> None:
        # email unique index is absent from the collection.
        db = _mock_db(
            {
                "users_ig": {
                    "_id_": _idx(["_id"]),
                    "name_1": _idx(["name"]),
                    # email unique index missing
                }
            }
        )
        guard = BeanieIndexGuard(_User)
        report = await guard.report(db)
        assert report.has_drift is True
        assert "users_ig" in report.missing_indexes

    async def test_missing_plain_index_reported(self) -> None:
        # name non-unique index absent.
        db = _mock_db(
            {
                "users_ig": {
                    "_id_": _idx(["_id"]),
                    "email_1": _idx(["email"], unique=True),
                    # name_1 missing
                }
            }
        )
        guard = BeanieIndexGuard(_User)
        report = await guard.report(db)
        assert report.has_drift is True

    async def test_unexpected_index_not_drift(self) -> None:
        # Extra index present but not expected → informational only.
        db = _mock_db(
            {
                "users_ig": {
                    "_id_": _idx(["_id"]),
                    "email_1": _idx(["email"], unique=True),
                    "name_1": _idx(["name"]),
                    "legacy_score_idx": _idx(["score"]),  # unexpected
                }
            }
        )
        guard = BeanieIndexGuard(_User)
        report = await guard.report(db)
        assert report.has_drift is False
        assert "users_ig" in report.unexpected_indexes
        assert "legacy_score_idx" in report.unexpected_indexes["users_ig"]

    async def test_compound_unique_constraint(self) -> None:
        # UniqueConstraint on (author_id, slug) → compound unique index expected.
        db = _mock_db(
            {
                "posts_ig": {
                    "_id_": _idx(["_id"]),
                    "uq_author_slug": _idx(["author_id", "slug"], unique=True),
                }
            }
        )
        guard = BeanieIndexGuard(_Post)
        report = await guard.report(db)
        assert report.has_drift is False

    async def test_compound_unique_constraint_missing(self) -> None:
        db = _mock_db(
            {
                "posts_ig": {
                    "_id_": _idx(["_id"]),
                    # compound index missing
                }
            }
        )
        guard = BeanieIndexGuard(_Post)
        report = await guard.report(db)
        assert report.has_drift is True

    async def test_id_index_never_expected(self) -> None:
        # The _id_ index is ALWAYS present in MongoDB — guard must not flag
        # its absence or double-report it as expected.
        db = _mock_db(
            {
                "users_ig": {
                    "_id_": _idx(["_id"]),
                    "email_1": _idx(["email"], unique=True),
                    "name_1": _idx(["name"]),
                }
            }
        )
        guard = BeanieIndexGuard(_User)
        report = await guard.report(db)
        # _id_ is in actual but never in expected — must not appear as unexpected.
        for names in report.unexpected_indexes.values():
            assert "_id_" not in names

    async def test_empty_entity_list_returns_clean(self) -> None:
        # No entity classes registered — nothing to check.
        db = _mock_db({})
        guard = BeanieIndexGuard()
        report = await guard.report(db)
        assert report.has_drift is False
        assert not report.missing_indexes
        assert not report.unexpected_indexes

    async def test_multiple_entities_checked_independently(self) -> None:
        # Both _User and _Post are registered; only _Post's index is missing.
        db = _mock_db(
            {
                "users_ig": {
                    "_id_": _idx(["_id"]),
                    "email_1": _idx(["email"], unique=True),
                    "name_1": _idx(["name"]),
                },
                "posts_ig": {
                    "_id_": _idx(["_id"]),
                    # compound index missing
                },
            }
        )
        guard = BeanieIndexGuard(_User, _Post)
        report = await guard.report(db)
        assert "posts_ig" in report.missing_indexes
        assert "users_ig" not in report.missing_indexes

    async def test_match_by_key_not_by_name(self) -> None:
        # The index name "my_custom_email_idx" differs from Beanie's expected
        # "email_1" — matching by key+unique tuple means it still satisfies the
        # expectation.  This is the core DESIGN decision: no false positives for
        # manually-created indexes with custom names.
        db = _mock_db(
            {
                "users_ig": {
                    "_id_": _idx(["_id"]),
                    "my_custom_email_idx": _idx(["email"], unique=True),  # custom name
                    "name_1": _idx(["name"]),
                }
            }
        )
        guard = BeanieIndexGuard(_User)
        report = await guard.report(db)
        # Should satisfy the unique index expectation despite different name.
        assert report.has_drift is False

    async def test_unique_and_nonunique_are_distinct(self) -> None:
        # An index on email WITHOUT unique=True does NOT satisfy
        # FieldHint(unique=True) — unique flag is part of the match key.
        db = _mock_db(
            {
                "users_ig": {
                    "_id_": _idx(["_id"]),
                    "email_1": _idx(["email"], unique=False),  # NOT unique
                    "name_1": _idx(["name"]),
                }
            }
        )
        guard = BeanieIndexGuard(_User)
        report = await guard.report(db)
        # The unique expectation for email is unmet.
        assert report.has_drift is True


# ── BeanieIndexGuard.check() ──────────────────────────────────────────────────


class TestBeanieIndexGuardCheck:
    async def test_check_does_not_raise_when_clean(self) -> None:
        db = _mock_db(
            {
                "users_ig": {
                    "_id_": _idx(["_id"]),
                    "email_1": _idx(["email"], unique=True),
                    "name_1": _idx(["name"]),
                }
            }
        )
        guard = BeanieIndexGuard(_User)
        # Must not raise
        await guard.check(db)

    async def test_check_raises_index_drift_on_missing(self) -> None:
        db = _mock_db(
            {
                "users_ig": {
                    "_id_": _idx(["_id"]),
                    # email unique index missing
                }
            }
        )
        guard = BeanieIndexGuard(_User)
        with pytest.raises(IndexDrift) as exc_info:
            await guard.check(db)

        # Exception carries the full drift report.
        assert exc_info.value.report.has_drift is True

    async def test_check_does_not_raise_for_unexpected_only(self) -> None:
        db = _mock_db(
            {
                "users_ig": {
                    "_id_": _idx(["_id"]),
                    "email_1": _idx(["email"], unique=True),
                    "name_1": _idx(["name"]),
                    "extra_idx": _idx(["score"]),  # unexpected but non-blocking
                }
            }
        )
        guard = BeanieIndexGuard(_User)
        # Unexpected indexes → warning only, must not raise.
        await guard.check(db)

    async def test_repr(self) -> None:
        guard = BeanieIndexGuard(_User, _Post)
        text = repr(guard)
        assert "_User" in text
        assert "_Post" in text


# ── Integration: real MongoDB ─────────────────────────────────────────────────


@pytest.mark.integration
async def test_integration_guard_against_real_mongodb() -> None:
    """
    Requires a running MongoDB on localhost:27017.
    Run with: VARCO_RUN_INTEGRATION=1 pytest -m integration
    """
    import os

    if not os.environ.get("VARCO_RUN_INTEGRATION"):
        pytest.skip("Set VARCO_RUN_INTEGRATION=1 to run integration tests")

    from motor.motor_asyncio import AsyncIOMotorClient

    mongo_url = os.environ.get("MONGO_URL", "mongodb://localhost:27017/")
    client = AsyncIOMotorClient(mongo_url)

    # Create the test collection and ensure expected indexes exist.
    db = client["varco_test_ig"]
    await db["users_ig"].create_index("email", unique=True)
    await db["users_ig"].create_index("name")

    try:
        guard = BeanieIndexGuard(_User)
        report = await guard.report(db)
        assert report.has_drift is False
    finally:
        await db["users_ig"].drop()
        client.close()
