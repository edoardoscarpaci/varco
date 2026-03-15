"""
Tests for AsyncBeanieRepository.

All Beanie Document methods are mocked — no MongoDB connection required.
Each test creates an isolated mock mapper and exercises one method of the
repository in isolation.

Coverage:
- find_by_id:    single PK (found / not found), composite PK (found / not found)
- find_all:      populated list, empty collection
- save INSERT:   fresh entity → orm_obj.insert()
- save UPDATE:   persisted entity → sync_to_orm + raw.save()
- delete:        persisted entity via _raw_orm, unpersisted → ValueError
- find_by_query: no filter, with AST filter, with sort, with pagination, combined
- count:         no filter, with AST filter

Thread safety:  N/A (unit tests)
Async safety:   ✅ Uses AsyncMock throughout
"""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock

import pytest

from fastrest_core.model import DomainModel
from fastrest_core.query.builder import QueryBuilder
from fastrest_core.query.params import QueryParams
from fastrest_core.query.type import SortField, SortOrder
from fastrest_beanie.repository import AsyncBeanieRepository


# ── Test domain model ─────────────────────────────────────────────────────────


@dataclass
class _User(DomainModel):
    """Minimal domain entity for repository testing."""

    name: str = ""
    email: str = ""


# ── Helper factories ──────────────────────────────────────────────────────────


def _make_cursor(orm_objects: list | None = None, count: int = 0) -> MagicMock:
    """
    Build a mock Beanie FindMany cursor that supports the full fluent API.

    DESIGN: all chain methods return the same cursor so that .sort().skip().limit()
    all land on the same object and the final .to_list() / .count() is reachable
    regardless of which combination of methods the code calls.

    Args:
        orm_objects: Objects returned by to_list().  Defaults to empty list.
        count:       Value returned by count().

    Returns:
        A MagicMock cursor with sort / skip / limit / to_list / count.
    """
    cursor = MagicMock()
    # Chain methods — each returns self so the builder pattern works
    cursor.sort.return_value = cursor
    cursor.skip.return_value = cursor
    cursor.limit.return_value = cursor
    cursor.to_list = AsyncMock(return_value=orm_objects or [])
    cursor.count = AsyncMock(return_value=count)
    return cursor


def _make_entity(*, pk: int = 1, persisted: bool = True) -> _User:
    """
    Create a _User entity with optional backing ORM object.

    Args:
        pk:        Primary key to assign.
        persisted: Whether to attach a mock _raw_orm (making is_persisted True).

    Returns:
        A _User entity ready for use in save / delete tests.
    """
    entity = _User(name="Alice", email="a@test.com")
    entity.pk = pk
    if persisted:
        entity._raw_orm = MagicMock()
    return entity


def _make_mapper(
    *,
    orm_cls: MagicMock | None = None,
    from_orm_entity: _User | None = None,
) -> MagicMock:
    """
    Build a mock AbstractMapper with sensible defaults.

    Args:
        orm_cls:         Overrides mapper._orm_cls.  A fresh MagicMock otherwise.
        from_orm_entity: Value returned by mapper.from_orm().

    Returns:
        A mock mapper ready for injection into AsyncBeanieRepository.
    """
    mapper = MagicMock()
    mapper._orm_cls = orm_cls or MagicMock()
    mapper._pk_orm_attrs = ["id"]

    if from_orm_entity is None:
        from_orm_entity = _make_entity()
    mapper.from_orm.return_value = from_orm_entity
    mapper.to_orm.return_value = MagicMock()
    return mapper


# ── find_by_id — single PK ────────────────────────────────────────────────────


async def test_find_by_id_single_pk_found() -> None:
    """find_by_id(pk) resolves via Document.get() and returns the domain entity."""
    mock_orm = MagicMock()
    mapper = _make_mapper()
    mapper._orm_cls.get = AsyncMock(return_value=mock_orm)

    result = await AsyncBeanieRepository(mapper=mapper).find_by_id(pk=1)

    mapper._orm_cls.get.assert_called_once_with(1)
    mapper.from_orm.assert_called_once_with(mock_orm)
    assert result is mapper.from_orm.return_value


async def test_find_by_id_single_pk_not_found_returns_none() -> None:
    """find_by_id returns None and never calls from_orm when Document.get returns None."""
    mapper = _make_mapper()
    mapper._orm_cls.get = AsyncMock(return_value=None)

    result = await AsyncBeanieRepository(mapper=mapper).find_by_id(pk=99)

    assert result is None
    mapper.from_orm.assert_not_called()


# ── find_by_id — composite PK ─────────────────────────────────────────────────


async def test_find_by_id_composite_pk_found() -> None:
    """
    Composite PKs are resolved via find_one() with a field-value filter dict.

    DESIGN: MongoDB has no native composite _id, so composite PKs are stored
    as regular indexed fields and matched with a filter dict.
    """
    mock_orm = MagicMock()
    mapper = _make_mapper()
    mapper._pk_orm_attrs = ["tenant_id", "user_id"]
    mapper._orm_cls.find_one = AsyncMock(return_value=mock_orm)

    result = await AsyncBeanieRepository(mapper=mapper).find_by_id(pk=(10, 42))

    mapper._orm_cls.find_one.assert_called_once_with({"tenant_id": 10, "user_id": 42})
    mapper.from_orm.assert_called_once_with(mock_orm)
    assert result is mapper.from_orm.return_value


async def test_find_by_id_composite_pk_not_found_returns_none() -> None:
    """Composite PK miss returns None without calling from_orm."""
    mapper = _make_mapper()
    mapper._pk_orm_attrs = ["a", "b"]
    mapper._orm_cls.find_one = AsyncMock(return_value=None)

    result = await AsyncBeanieRepository(mapper=mapper).find_by_id(pk=(1, 2))

    assert result is None
    mapper.from_orm.assert_not_called()


# ── find_all ──────────────────────────────────────────────────────────────────


async def test_find_all_returns_mapped_entities() -> None:
    """find_all converts every ORM document to a domain entity."""
    orm_a, orm_b = MagicMock(), MagicMock()
    entity_a = _User(name="A")
    entity_b = _User(name="B")

    mapper = _make_mapper()
    mapper._orm_cls.find_all.return_value = _make_cursor([orm_a, orm_b])
    mapper.from_orm.side_effect = [entity_a, entity_b]

    results = await AsyncBeanieRepository(mapper=mapper).find_all()

    assert results == [entity_a, entity_b]
    assert mapper.from_orm.call_count == 2


async def test_find_all_empty_collection_returns_empty_list() -> None:
    """find_all returns [] and never calls from_orm when no documents exist."""
    mapper = _make_mapper()
    mapper._orm_cls.find_all.return_value = _make_cursor([])

    results = await AsyncBeanieRepository(mapper=mapper).find_all()

    assert results == []
    mapper.from_orm.assert_not_called()


# ── save — INSERT path ────────────────────────────────────────────────────────


async def test_save_insert_calls_orm_insert() -> None:
    """
    save() with _raw_orm=None triggers INSERT:
    to_orm() → orm_obj.insert() → from_orm(orm_obj).
    """
    orm_obj = MagicMock()
    orm_obj.insert = AsyncMock()

    mapper = _make_mapper()
    mapper.to_orm.return_value = orm_obj
    # Freshly constructed entity — _raw_orm is None → INSERT
    entity = _User(name="Bob")

    result = await AsyncBeanieRepository(mapper=mapper).save(entity)

    mapper.to_orm.assert_called_once_with(entity)
    orm_obj.insert.assert_called_once()
    mapper.from_orm.assert_called_once_with(orm_obj)
    assert result is mapper.from_orm.return_value


async def test_save_insert_does_not_call_sync_to_orm() -> None:
    """sync_to_orm is never called on the INSERT path (no raw to sync)."""
    orm_obj = MagicMock()
    orm_obj.insert = AsyncMock()

    mapper = _make_mapper()
    mapper.to_orm.return_value = orm_obj
    entity = _User(name="Bob")

    await AsyncBeanieRepository(mapper=mapper).save(entity)

    mapper.sync_to_orm.assert_not_called()


# ── save — UPDATE path ────────────────────────────────────────────────────────


async def test_save_update_calls_sync_and_raw_save() -> None:
    """
    save() with _raw_orm set triggers UPDATE:
    sync_to_orm(entity, raw) → raw.save() → from_orm(raw).
    """
    raw = MagicMock()
    raw.save = AsyncMock()
    entity = _User(name="Bob")
    entity._raw_orm = raw  # marks entity as persisted

    mapper = _make_mapper()

    result = await AsyncBeanieRepository(mapper=mapper).save(entity)

    mapper.sync_to_orm.assert_called_once_with(entity, raw)
    raw.save.assert_called_once()
    mapper.from_orm.assert_called_once_with(raw)
    assert result is mapper.from_orm.return_value


async def test_save_update_does_not_call_to_orm() -> None:
    """to_orm is never called on the UPDATE path (ORM object already exists)."""
    raw = MagicMock()
    raw.save = AsyncMock()
    entity = _User(name="Bob")
    entity._raw_orm = raw

    mapper = _make_mapper()
    await AsyncBeanieRepository(mapper=mapper).save(entity)

    mapper.to_orm.assert_not_called()


# ── delete ────────────────────────────────────────────────────────────────────


async def test_delete_persisted_entity_calls_raw_delete() -> None:
    """delete() delegates to _raw_orm.delete() for a persisted entity."""
    raw = MagicMock()
    raw.delete = AsyncMock()
    entity = _make_entity(pk=5)
    entity._raw_orm = raw

    await AsyncBeanieRepository(mapper=_make_mapper()).delete(entity)

    raw.delete.assert_called_once()


async def test_delete_unpersisted_entity_raises_value_error() -> None:
    """
    delete() raises ValueError when the entity has never been persisted.

    Edge case: is_persisted() returns False when _raw_orm is None,
    regardless of whether pk is set (STR_ASSIGNED entities have pk before save).
    """
    entity = _User(name="Ghost")  # _raw_orm is None → not persisted

    with pytest.raises(ValueError, match="not yet persisted"):
        await AsyncBeanieRepository(mapper=_make_mapper()).delete(entity)


# ── find_by_query — filter ────────────────────────────────────────────────────


async def test_find_by_query_no_filter_returns_all() -> None:
    """QueryParams with no AST node → find() called with empty filter dict."""
    orm_obj = MagicMock()
    entity = _make_entity()
    cursor = _make_cursor([orm_obj])

    mapper = _make_mapper(from_orm_entity=entity)
    mapper._orm_cls.find.return_value = cursor

    results = await AsyncBeanieRepository(mapper=mapper).find_by_query(QueryParams())

    # Empty dict = no filter
    mapper._orm_cls.find.assert_called_once_with({})
    assert results == [entity]


async def test_find_by_query_with_ast_filter() -> None:
    """QueryParams with an AST node compiles to a MongoDB filter dict."""
    cursor = _make_cursor([])
    mapper = _make_mapper()
    mapper._orm_cls.find.return_value = cursor

    node = QueryBuilder().eq("active", True).build()
    await AsyncBeanieRepository(mapper=mapper).find_by_query(QueryParams(node=node))

    # Compiler should have produced a non-empty filter
    call_args = mapper._orm_cls.find.call_args[0][0]
    assert isinstance(call_args, dict)
    assert call_args  # not empty — AST was compiled


async def test_find_by_query_with_sort() -> None:
    """Sort directives are forwarded to cursor.sort() with pymongo convention."""
    cursor = _make_cursor([])
    mapper = _make_mapper()
    mapper._orm_cls.find.return_value = cursor

    sort = [SortField("name", SortOrder.ASC), SortField("age", SortOrder.DESC)]
    await AsyncBeanieRepository(mapper=mapper).find_by_query(QueryParams(sort=sort))

    # cursor.sort() must be called with the pymongo-format list
    cursor.sort.assert_called_once_with([("name", 1), ("age", -1)])


async def test_find_by_query_with_pagination() -> None:
    """offset and limit are forwarded to cursor.skip() / cursor.limit()."""
    cursor = _make_cursor([])
    mapper = _make_mapper()
    mapper._orm_cls.find.return_value = cursor

    await AsyncBeanieRepository(mapper=mapper).find_by_query(
        QueryParams(offset=10, limit=5)
    )

    cursor.skip.assert_called_once_with(10)
    cursor.limit.assert_called_once_with(5)


async def test_find_by_query_no_sort_skips_cursor_sort() -> None:
    """cursor.sort() is NOT called when no sort directives are provided."""
    cursor = _make_cursor([])
    mapper = _make_mapper()
    mapper._orm_cls.find.return_value = cursor

    await AsyncBeanieRepository(mapper=mapper).find_by_query(QueryParams())

    cursor.sort.assert_not_called()


async def test_find_by_query_no_pagination_skips_skip_and_limit() -> None:
    """cursor.skip() and cursor.limit() are NOT called when offset/limit are None."""
    cursor = _make_cursor([])
    mapper = _make_mapper()
    mapper._orm_cls.find.return_value = cursor

    await AsyncBeanieRepository(mapper=mapper).find_by_query(QueryParams())

    cursor.skip.assert_not_called()
    cursor.limit.assert_not_called()


# ── count ─────────────────────────────────────────────────────────────────────


async def test_count_no_filter_returns_all_document_count() -> None:
    """count(None) issues find({}) and returns the count of all documents."""
    cursor = _make_cursor(count=42)
    mapper = _make_mapper()
    mapper._orm_cls.find.return_value = cursor

    result = await AsyncBeanieRepository(mapper=mapper).count()

    mapper._orm_cls.find.assert_called_once_with({})
    assert result == 42


async def test_count_with_ast_filter_compiles_to_mongo_filter() -> None:
    """count() with an AST filter compiles it and passes a non-empty filter."""
    cursor = _make_cursor(count=7)
    mapper = _make_mapper()
    mapper._orm_cls.find.return_value = cursor

    node = QueryBuilder().eq("active", True).build()
    result = await AsyncBeanieRepository(mapper=mapper).count(QueryParams(node=node))

    call_args = mapper._orm_cls.find.call_args[0][0]
    assert call_args  # non-empty filter compiled from AST
    assert result == 7


async def test_count_with_empty_query_params_uses_no_filter() -> None:
    """count(QueryParams()) with no node uses an empty filter dict."""
    cursor = _make_cursor(count=3)
    mapper = _make_mapper()
    mapper._orm_cls.find.return_value = cursor

    await AsyncBeanieRepository(mapper=mapper).count(QueryParams())

    mapper._orm_cls.find.assert_called_once_with({})
