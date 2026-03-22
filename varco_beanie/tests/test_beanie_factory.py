"""
Unit tests for BeanieModelFactory — Document class generation and mapper.

No real MongoDB connection is needed: tests inspect the generated class
structure (Pydantic annotations, Settings, indexes) and exercise the mapper
translation logic using simple in-memory objects.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Annotated
from uuid import UUID, uuid4

import pytest
from beanie import Document

from varco_core.exception.repository import StaleEntityError
from varco_core.meta import (
    FieldHint,
    PrimaryKey,
    PKStrategy,
    UniqueConstraint,
    pk_field,
)
from varco_core.migrator import DomainMigrator
from varco_core.model import DomainModel, VersionedDomainModel
from varco_beanie.factory import BeanieModelFactory, BeanieDocRegistry


# ── Helpers ───────────────────────────────────────────────────────────────────


def _factory() -> BeanieModelFactory:
    return BeanieModelFactory()


# ── Document class generation ─────────────────────────────────────────────────


@dataclass
class _Article(DomainModel):
    title: str
    body: str = ""

    class Meta:
        table = "articles_beanie"


def test_build_generates_document_subclass():
    doc_cls, _ = _factory().build(_Article)
    assert issubclass(doc_cls, Document)


def test_build_document_class_name():
    # The domain class is named _Article (test-private prefix), so the
    # generated Document class name mirrors it: "_ArticleDoc".
    doc_cls, _ = _factory().build(_Article)
    assert doc_cls.__name__ == "_ArticleDoc"


def test_build_collection_name_from_meta():
    doc_cls, _ = _factory().build(_Article)
    assert doc_cls.Settings.name == "articles_beanie"


def test_build_registers_document():
    doc_cls, _ = _factory().build(_Article)
    assert BeanieDocRegistry.get(_Article) is doc_cls


def test_build_is_idempotent():
    f = _factory()
    doc1, _ = f.build(_Article)
    doc2, _ = f.build(_Article)
    assert doc1 is doc2


# ── Field annotations on generated Document ──────────────────────────────────


@dataclass
class _Profile(DomainModel):
    username: Annotated[str, FieldHint(unique=True)]
    bio: str = ""
    metadata: dict = None
    tags: list = None

    class Meta:
        table = "profiles_beanie"


def test_build_annotates_fields():
    doc_cls, _ = _factory().build(_Profile)
    hints = doc_cls.__annotations__
    assert "username" in hints
    assert "bio" in hints


def test_build_dict_field_present():
    """dict fields should appear in the generated document annotations."""
    doc_cls, _ = _factory().build(_Profile)
    assert "metadata" in doc_cls.__annotations__


def test_build_list_field_present():
    doc_cls, _ = _factory().build(_Profile)
    assert "tags" in doc_cls.__annotations__


# ── UUID pk ───────────────────────────────────────────────────────────────────


@dataclass
class _Post(DomainModel):
    pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
    title: str = ""

    class Meta:
        table = "posts_beanie"


def test_build_uuid_pk_annotation():
    doc_cls, _ = _factory().build(_Post)
    assert doc_cls.__annotations__.get("id") is UUID


# ── Compound unique index ────────────────────────────────────────────────────


@dataclass
class _Slot(DomainModel):
    day: str
    hour: int

    class Meta:
        table = "slots_beanie"
        constraints = [UniqueConstraint("day", "hour", name="uq_slot")]


def test_build_compound_unique_index_in_settings():
    from pymongo import IndexModel

    doc_cls, _ = _factory().build(_Slot)
    indexes = doc_cls.Settings.indexes
    assert len(indexes) == 1
    assert isinstance(indexes[0], IndexModel)


# ── Mapper: to_orm ────────────────────────────────────────────────────────────


@dataclass
class _Note(DomainModel):
    content: str

    class Meta:
        table = "notes_beanie"


def test_mapper_to_orm_sets_business_fields():
    _, mapper = _factory().build(_Note)
    doc = mapper.to_orm(_Note(content="Hello"))
    assert doc.content == "Hello"


# ── Mapper: from_orm ─────────────────────────────────────────────────────────


def test_mapper_from_orm_populates_business_fields():
    _, mapper = _factory().build(_Note)
    doc = mapper.to_orm(_Note(content="World"))
    doc.id = uuid4()
    entity = mapper.from_orm(doc)
    assert entity.content == "World"
    assert entity.pk == doc.id


def test_mapper_from_orm_sets_raw_orm():
    _, mapper = _factory().build(_Note)
    doc = mapper.to_orm(_Note(content="X"))
    doc.id = uuid4()
    entity = mapper.from_orm(doc)
    assert entity._raw_orm is doc


# ── Mapper: VersionedDomainModel system fields ────────────────────────────────


@dataclass
class _VersionedNote(VersionedDomainModel):
    content: str

    class Meta:
        table = "versioned_notes_beanie"


def test_mapper_to_orm_versioned_sets_row_version():
    _, mapper = _factory().build(_VersionedNote)
    doc = mapper.to_orm(_VersionedNote(content="Hi"))
    assert doc.row_version == 1


def test_mapper_to_orm_versioned_sets_timestamps():
    _, mapper = _factory().build(_VersionedNote)
    before = datetime.now(tz=timezone.utc)
    doc = mapper.to_orm(_VersionedNote(content="Hi"))
    assert doc.created_at >= before
    assert doc.updated_at >= before


def test_mapper_sync_to_orm_increments_row_version():
    _, mapper = _factory().build(_VersionedNote)
    doc = mapper.to_orm(_VersionedNote(content="Old"))
    doc.id = uuid4()
    entity = mapper.from_orm(doc)

    object.__setattr__(entity, "content", "New")
    mapper.sync_to_orm(entity, doc)
    assert doc.row_version == 2
    assert doc.content == "New"


def test_mapper_sync_to_orm_raises_stale_entity():
    _, mapper = _factory().build(_VersionedNote)
    doc = mapper.to_orm(_VersionedNote(content="Old"))
    doc.id = uuid4()
    entity = mapper.from_orm(doc)

    # Simulate another writer incrementing row_version in the "DB"
    doc.row_version = 99

    with pytest.raises(StaleEntityError):
        mapper.sync_to_orm(entity, doc)


# ── Mapper: migration chain ───────────────────────────────────────────────────


def upper_content(data: dict) -> dict:
    data["content"] = data["content"].upper()
    return data


class _ContentMigrator(DomainMigrator):
    steps = [upper_content]


@dataclass
class _MigratedNote(VersionedDomainModel):
    content: str

    class Meta:
        table = "migrated_notes_beanie"
        migrator = _ContentMigrator


def test_mapper_from_orm_triggers_migration():
    _, mapper = _factory().build(_MigratedNote)
    # Build a doc at v1
    doc = mapper.to_orm(_MigratedNote(content="hello"))
    doc.id = uuid4()
    doc.definition_version = 1  # force stale version

    entity = mapper.from_orm(doc)
    assert entity.content == "HELLO"
    assert entity.definition_version == 2


def test_mapper_from_orm_skips_migration_when_current():
    _, mapper = _factory().build(_MigratedNote)
    doc = mapper.to_orm(_MigratedNote(content="already"))
    doc.id = uuid4()
    # definition_version already at current (2)
    entity = mapper.from_orm(doc)
    assert entity.content == "already"


# ── Migrator wired to mapper ──────────────────────────────────────────────────


def test_factory_passes_migrator_to_mapper():
    _, mapper = _factory().build(_MigratedNote)
    assert isinstance(mapper._migrator, _ContentMigrator)


def test_factory_no_migrator_mapper_has_none():
    _, mapper = _factory().build(_Note)
    assert mapper._migrator is None
