"""
Integration tests for AsyncSQLAlchemyRepository with SQLite in-memory.

Covers INSERT, UPDATE, DELETE, find_by_id, find_all, find_by_query,
StaleEntityError, and the migration chain on load.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from fastrest_core.exception.repository import StaleEntityError
from fastrest_core.meta import FieldHint
from fastrest_core.migrator import DomainMigrator
from fastrest_core.model import DomainModel, VersionedDomainModel
from fastrest_core.query.builder import QueryBuilder
from fastrest_core.query.params import QueryParams
from fastrest_sa.factory import SAModelFactory
from fastrest_sa.repository import AsyncSQLAlchemyRepository


# ── Domain models ─────────────────────────────────────────────────────────────


@dataclass
class _Item(DomainModel):
    name: Annotated[str, FieldHint(max_length=200)]
    price: float = 0.0

    class Meta:
        table = "items_repo"


@dataclass
class _VersionedItem(VersionedDomainModel):
    name: str
    price: float = 0.0

    class Meta:
        table = "versioned_items_repo"


# ── Per-test fixtures ─────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def repo_and_session():
    """
    Yields (repo_plain, repo_versioned, session) backed by fresh SQLite.
    Both table schemas are created before the test and dropped after.

    DESIGN: _Base is defined inside the fixture so each test gets a fresh
    DeclarativeBase with an empty MetaData — prevents SA "Table already
    defined" errors when the same domain class is built in multiple tests.
    """

    class _Base(DeclarativeBase):
        pass

    factory = SAModelFactory(_Base)
    _, mapper_plain = factory.build(_Item)
    _, mapper_versioned = factory.build(_VersionedItem)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(_Base.metadata.create_all)

    async_session = async_sessionmaker(engine, expire_on_commit=False)
    async with async_session() as session:
        yield (
            AsyncSQLAlchemyRepository(session, mapper_plain),
            AsyncSQLAlchemyRepository(session, mapper_versioned),
            session,
        )

    async with engine.begin() as conn:
        await conn.run_sync(_Base.metadata.drop_all)
    await engine.dispose()


# ── INSERT / find_by_id ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_save_insert_assigns_pk(repo_and_session):
    repo, _, _ = repo_and_session
    saved = await repo.save(_Item(name="Widget", price=9.99))
    assert saved.pk is not None
    assert saved.pk > 0


@pytest.mark.asyncio
async def test_save_insert_find_by_id_roundtrip(repo_and_session):
    repo, _, _ = repo_and_session
    saved = await repo.save(_Item(name="Gadget", price=19.99))
    found = await repo.find_by_id(saved.pk)
    assert found is not None
    assert found.name == "Gadget"
    assert found.price == 19.99


@pytest.mark.asyncio
async def test_find_by_id_returns_none_for_missing(repo_and_session):
    repo, _, _ = repo_and_session
    found = await repo.find_by_id(9999)
    assert found is None


# ── UPDATE ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_save_update_changes_field(repo_and_session):
    repo, _, _ = repo_and_session
    saved = await repo.save(_Item(name="Old", price=1.0))
    object.__setattr__(saved, "name", "New")
    updated = await repo.save(saved)
    assert updated.name == "New"
    found = await repo.find_by_id(saved.pk)
    assert found.name == "New"


# ── DELETE ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_delete_removes_entity(repo_and_session):
    repo, _, _ = repo_and_session
    saved = await repo.save(_Item(name="ToDelete", price=0.0))
    await repo.delete(saved)
    found = await repo.find_by_id(saved.pk)
    assert found is None


@pytest.mark.asyncio
async def test_delete_unpersisted_raises(repo_and_session):
    repo, _, _ = repo_and_session
    with pytest.raises(ValueError, match="not yet persisted"):
        await repo.delete(_Item(name="Ghost"))


# ── find_all ──────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_find_all_returns_all_entities(repo_and_session):
    repo, _, _ = repo_and_session
    await repo.save(_Item(name="A", price=1.0))
    await repo.save(_Item(name="B", price=2.0))
    all_items = await repo.find_all()
    assert len(all_items) == 2


# ── find_by_query ─────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_find_by_query_filter(repo_and_session):
    repo, _, _ = repo_and_session
    await repo.save(_Item(name="Alpha", price=10.0))
    await repo.save(_Item(name="Beta", price=20.0))
    # QueryBuilder().build() returns the raw AST node — wrap it in QueryParams
    params = QueryParams(node=QueryBuilder().eq("name", "Alpha").build())
    results = await repo.find_by_query(params)
    assert len(results) == 1
    assert results[0].name == "Alpha"


@pytest.mark.asyncio
async def test_find_by_query_pagination(repo_and_session):
    repo, _, _ = repo_and_session
    for i in range(5):
        await repo.save(_Item(name=f"Item{i}", price=float(i)))
    params = QueryParams(limit=2, offset=1)
    results = await repo.find_by_query(params)
    assert len(results) == 2


@pytest.mark.asyncio
async def test_count_all(repo_and_session):
    repo, _, _ = repo_and_session
    await repo.save(_Item(name="X", price=0.0))
    await repo.save(_Item(name="Y", price=0.0))
    assert await repo.count() == 2


@pytest.mark.asyncio
async def test_count_with_filter(repo_and_session):
    repo, _, _ = repo_and_session
    await repo.save(_Item(name="Match", price=5.0))
    await repo.save(_Item(name="NoMatch", price=5.0))
    params = QueryParams(node=QueryBuilder().eq("name", "Match").build())
    assert await repo.count(params) == 1


# ── VersionedDomainModel: system fields ───────────────────────────────────────


@pytest.mark.asyncio
async def test_versioned_insert_sets_system_fields(repo_and_session):
    _, repo, _ = repo_and_session
    saved = await repo.save(_VersionedItem(name="V1", price=1.0))
    assert saved.row_version == 1
    assert saved.definition_version == 1
    assert saved.created_at is not None
    assert saved.updated_at is not None


@pytest.mark.asyncio
async def test_versioned_update_increments_row_version(repo_and_session):
    _, repo, _ = repo_and_session
    saved = await repo.save(_VersionedItem(name="V1", price=1.0))
    assert saved.row_version == 1
    object.__setattr__(saved, "name", "V2")
    updated = await repo.save(saved)
    assert updated.row_version == 2


@pytest.mark.asyncio
async def test_versioned_update_does_not_change_created_at(repo_and_session):
    _, repo, _ = repo_and_session
    saved = await repo.save(_VersionedItem(name="V1", price=1.0))
    original_created = saved.created_at
    object.__setattr__(saved, "name", "V2")
    updated = await repo.save(saved)
    assert updated.created_at == original_created


# ── StaleEntityError ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stale_entity_error_on_concurrent_update(repo_and_session):
    _, repo, session = repo_and_session
    saved = await repo.save(_VersionedItem(name="Original", price=1.0))

    # Load a second copy — same pk, same initial row_version
    copy = await repo.find_by_id(saved.pk)

    # First copy saves successfully, increments row_version to 2
    object.__setattr__(saved, "name", "First Update")
    await repo.save(saved)

    # Second copy still holds row_version=1 — stale
    object.__setattr__(copy, "name", "Stale Update")
    with pytest.raises(StaleEntityError) as exc_info:
        await repo.save(copy)
    assert exc_info.value.expected_version == 1


# ── Migration on load ─────────────────────────────────────────────────────────


def upper_name(data: dict) -> dict:
    data["name"] = data["name"].upper()
    return data


class _UpperMigrator(DomainMigrator):
    steps = [upper_name]


@dataclass
class _MigratedItem(VersionedDomainModel):
    name: str
    price: float = 0.0

    class Meta:
        table = "migrated_items_repo"
        migrator = _UpperMigrator


@pytest_asyncio.fixture
async def migrated_repo():
    # Fresh DeclarativeBase — same isolation rationale as repo_and_session
    class _MBase(DeclarativeBase):
        pass

    factory = SAModelFactory(_MBase)
    _, mapper = factory.build(_MigratedItem)
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(_MBase.metadata.create_all)
    async_session = async_sessionmaker(engine, expire_on_commit=False)
    async with async_session() as session:
        yield AsyncSQLAlchemyRepository(session, mapper), session
    async with engine.begin() as conn:
        await conn.run_sync(_MBase.metadata.drop_all)
    await engine.dispose()


@pytest.mark.asyncio
async def test_migration_runs_on_load(migrated_repo):
    """
    Row saved with definition_version=1 (simulated by inserting directly)
    should be migrated to v2 (upper_name) when loaded via the mapper.
    """
    repo, session = migrated_repo
    from fastrest_sa.factory import SAModelRegistry

    ORM = SAModelRegistry.get(_MigratedItem)

    # Insert a stale row directly, bypassing the mapper so definition_version=1
    stale_row = ORM(
        name="lowercase",
        price=5.0,
        definition_version=1,
        row_version=1,
        created_at=None,
        updated_at=None,
    )
    session.add(stale_row)
    await session.flush()

    loaded = await repo.find_by_id(stale_row.id)
    assert loaded.name == "LOWERCASE"  # migration ran
    assert loaded.definition_version == 2  # bumped
