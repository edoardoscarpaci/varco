"""
Integration tests for SchemaGuard — startup schema drift detector.

Uses SQLite in-memory so no external service is required.  Each test
creates a fresh engine to control exactly what tables/columns exist in
the live DB vs what Base.metadata declares.
"""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import DeclarativeBase

from varco_core.model import DomainModel
from varco_sa.factory import SAModelFactory
from varco_sa.schema_guard import SchemaGuard, SchemaDrift, SchemaDriftReport


# ── Domain models used across tests ───────────────────────────────────────────


@dataclass
class _Widget(DomainModel):
    name: str
    price: float = 0.0

    class Meta:
        table = "widgets_guard"


@dataclass
class _Tag(DomainModel):
    label: str

    class Meta:
        table = "tags_guard"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_base() -> type[DeclarativeBase]:
    """Return a fresh DeclarativeBase so metadata is isolated per test."""

    class _Base(DeclarativeBase):
        pass

    return _Base


# ── clean DB — no drift ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_check_passes_when_schema_is_in_sync():
    """check() must not raise when the DB matches metadata exactly."""
    base = _make_base()
    factory = SAModelFactory(base)
    factory.build(_Widget)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(base.metadata.create_all)

    guard = SchemaGuard(base)
    # Must not raise
    await guard.check(engine)
    await engine.dispose()


@pytest.mark.asyncio
async def test_report_has_no_drift_when_schema_is_in_sync():
    base = _make_base()
    factory = SAModelFactory(base)
    factory.build(_Widget)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(base.metadata.create_all)

    guard = SchemaGuard(base)
    drift = await guard.report(engine)

    assert drift.has_drift is False
    assert drift.missing_tables == []
    assert drift.missing_columns == {}
    await engine.dispose()


# ── missing table ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_check_raises_when_table_is_missing():
    """check() must raise SchemaDrift when a registered table is absent from DB."""
    base = _make_base()
    factory = SAModelFactory(base)
    factory.build(_Widget)
    factory.build(_Tag)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    # Create only one of the two tables — tags_guard will be missing
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: base.metadata.tables["widgets_guard"].create(c))

    guard = SchemaGuard(base)
    with pytest.raises(SchemaDrift) as exc_info:
        await guard.check(engine)

    assert "tags_guard" in exc_info.value.report.missing_tables
    await engine.dispose()


@pytest.mark.asyncio
async def test_report_missing_table_listed():
    base = _make_base()
    factory = SAModelFactory(base)
    factory.build(_Widget)
    factory.build(_Tag)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(lambda c: base.metadata.tables["widgets_guard"].create(c))

    guard = SchemaGuard(base)
    drift = await guard.report(engine)

    assert "tags_guard" in drift.missing_tables
    assert drift.has_drift is True
    await engine.dispose()


# ── missing column ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_check_raises_when_column_is_missing():
    """
    check() must raise SchemaDrift when a table exists but is missing a column.

    We simulate this by creating the table with only the id and name columns,
    then building a metadata that also expects a price column.
    """
    base = _make_base()
    factory = SAModelFactory(base)
    factory.build(_Widget)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    # Create the table without the 'price' column to simulate a stale migration
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "CREATE TABLE widgets_guard (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
            )
        )

    guard = SchemaGuard(base)
    with pytest.raises(SchemaDrift) as exc_info:
        await guard.check(engine)

    missing = exc_info.value.report.missing_columns.get("widgets_guard", [])
    assert "price" in missing
    await engine.dispose()


@pytest.mark.asyncio
async def test_report_missing_column_listed():
    base = _make_base()
    factory = SAModelFactory(base)
    factory.build(_Widget)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "CREATE TABLE widgets_guard (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"
            )
        )

    guard = SchemaGuard(base)
    drift = await guard.report(engine)

    assert "price" in drift.missing_columns.get("widgets_guard", [])
    await engine.dispose()


# ── extra column — informational only ────────────────────────────────────────


@pytest.mark.asyncio
async def test_check_passes_with_extra_column_in_db():
    """
    Extra columns in the DB (beyond what metadata declares) must NOT cause
    check() to raise — they are safe and belong in extra_columns only.
    """
    base = _make_base()
    factory = SAModelFactory(base)
    factory.build(_Widget)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    # Create the table with an extra column not in metadata
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "CREATE TABLE widgets_guard "
                "(id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
                "price REAL, legacy_field TEXT)"
            )
        )

    guard = SchemaGuard(base)
    # Must NOT raise — extra columns are informational
    await guard.check(engine)
    await engine.dispose()


@pytest.mark.asyncio
async def test_report_extra_column_in_extra_columns():
    base = _make_base()
    factory = SAModelFactory(base)
    factory.build(_Widget)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "CREATE TABLE widgets_guard "
                "(id INTEGER PRIMARY KEY, name TEXT NOT NULL, "
                "price REAL, legacy_field TEXT)"
            )
        )

    guard = SchemaGuard(base)
    drift = await guard.report(engine)

    assert drift.has_drift is False  # extra columns don't count as blocking drift
    assert "legacy_field" in drift.extra_columns.get("widgets_guard", [])
    await engine.dispose()


# ── SchemaDriftReport helpers ─────────────────────────────────────────────────


def test_report_format_clean():
    report = SchemaDriftReport()
    assert report.format() == "No schema drift detected."


def test_report_format_with_missing_table():
    report = SchemaDriftReport(missing_tables=["users"])
    text_out = report.format()
    assert "users" in text_out
    assert "Missing tables" in text_out


def test_report_format_with_missing_column():
    report = SchemaDriftReport(missing_columns={"posts": ["author_id"]})
    text_out = report.format()
    assert "posts" in text_out
    assert "author_id" in text_out
    assert "Missing columns" in text_out


def test_report_has_drift_false_for_extra_only():
    """Extra columns alone must not set has_drift — they are safe."""
    report = SchemaDriftReport(extra_columns={"posts": ["legacy_col"]})
    assert report.has_drift is False


def test_schema_drift_exception_message_contains_report():
    report = SchemaDriftReport(missing_tables=["orders"])
    exc = SchemaDrift(report)
    assert "orders" in str(exc)
    assert exc.report is report


# ── empty metadata ────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_check_passes_with_empty_metadata():
    """An empty metadata (no models built) must produce a clean report."""
    base = _make_base()  # no factory.build() calls

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    guard = SchemaGuard(base)
    await guard.check(engine)  # must not raise
    await engine.dispose()


# ── repr ──────────────────────────────────────────────────────────────────────


def test_schema_guard_repr():
    base = _make_base()
    factory = SAModelFactory(base)
    factory.build(_Widget)
    guard = SchemaGuard(base)
    assert "SchemaGuard" in repr(guard)
    assert "1" in repr(guard)  # 1 table in metadata
