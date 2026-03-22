"""
Unit tests for varco_core.mapper (AbstractMapper)

Uses a minimal stub ORM class and a concrete mapper subclass so no real DB
is needed.  Tests cover:
  - to_orm:       business fields, system fields, PK strategies
  - from_orm:     business fields, system fields, migration chain
  - sync_to_orm:  business update, updated_at refresh, row_version increment,
                  StaleEntityError on version mismatch
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import pytest

from varco_core.exception.repository import StaleEntityError
from varco_core.mapper import AbstractMapper
from varco_core.migrator import DomainMigrator
from varco_core.model import AuditedDomainModel, DomainModel, VersionedDomainModel


# ── Stub ORM ──────────────────────────────────────────────────────────────────


class _ORM:
    """Minimal stand-in for a SA ORM / Beanie Document."""

    def __init__(self, **kwargs: Any) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)


# ── Concrete mapper fixture ───────────────────────────────────────────────────


class _SinglePKMapper(AbstractMapper):
    @property
    def _pk_orm_attrs(self) -> list[str]:
        return ["id"]


def _make_mapper(domain_cls: type, migrator: Any = None) -> _SinglePKMapper:
    return _SinglePKMapper(domain_cls, _ORM, migrator=migrator)


# ── Domain model fixtures ─────────────────────────────────────────────────────


@dataclass
class _Plain(DomainModel):
    name: str
    score: int = 0


@dataclass
class _Audited(AuditedDomainModel):
    name: str


@dataclass
class _Versioned(VersionedDomainModel):
    name: str
    email: str = "default@example.com"


# ── to_orm ────────────────────────────────────────────────────────────────────


def test_to_orm_copies_business_fields():
    mapper = _make_mapper(_Plain)
    orm = mapper.to_orm(_Plain(name="Edo", score=42))
    assert orm.name == "Edo"
    assert orm.score == 42


def test_to_orm_plain_no_pk_omitted_when_none():
    mapper = _make_mapper(_Plain)
    orm = mapper.to_orm(_Plain(name="X"))
    assert not hasattr(orm, "id")  # pk is None → not injected


def test_to_orm_plain_pk_injected_when_set():
    mapper = _make_mapper(_Plain)
    entity = _Plain(name="X")
    object.__setattr__(entity, "pk", 99)
    orm = mapper.to_orm(entity)
    assert orm.id == 99


def test_to_orm_audited_sets_created_and_updated_at():
    mapper = _make_mapper(_Audited)
    before = datetime.now(tz=timezone.utc)
    orm = mapper.to_orm(_Audited(name="Edo"))
    after = datetime.now(tz=timezone.utc)
    assert before <= orm.created_at <= after
    assert before <= orm.updated_at <= after


def test_to_orm_versioned_sets_row_version_1():
    mapper = _make_mapper(_Versioned)
    orm = mapper.to_orm(_Versioned(name="Edo"))
    assert orm.row_version == 1


def test_to_orm_versioned_no_migrator_sets_definition_version_1():
    mapper = _make_mapper(_Versioned)
    orm = mapper.to_orm(_Versioned(name="Edo"))
    assert orm.definition_version == 1


def test_to_orm_versioned_with_migrator_stamps_current_version():
    class M(DomainMigrator):
        steps = [lambda d: d, lambda d: d]  # current_version = 3

    mapper = _make_mapper(_Versioned, migrator=M)
    orm = mapper.to_orm(_Versioned(name="Edo"))
    assert orm.definition_version == 3


# ── from_orm ──────────────────────────────────────────────────────────────────


def test_from_orm_populates_business_fields():
    mapper = _make_mapper(_Plain)
    orm = _ORM(id=1, name="Edo", score=7)
    entity = mapper.from_orm(orm)
    assert entity.name == "Edo"
    assert entity.score == 7


def test_from_orm_sets_pk_scalar():
    mapper = _make_mapper(_Plain)
    orm = _ORM(id=5, name="X", score=0)
    entity = mapper.from_orm(orm)
    assert entity.pk == 5


def test_from_orm_sets_raw_orm():
    mapper = _make_mapper(_Plain)
    orm = _ORM(id=1, name="X", score=0)
    entity = mapper.from_orm(orm)
    assert entity._raw_orm is orm


def test_from_orm_audited_reads_timestamps():
    mapper = _make_mapper(_Audited)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    orm = _ORM(id=1, name="Edo", created_at=ts, updated_at=ts)
    entity = mapper.from_orm(orm)
    assert entity.created_at == ts
    assert entity.updated_at == ts


def test_from_orm_versioned_reads_system_fields():
    mapper = _make_mapper(_Versioned)
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    orm = _ORM(
        id=1,
        name="Edo",
        email="a@b.com",
        created_at=ts,
        updated_at=ts,
        definition_version=1,
        row_version=3,
    )
    entity = mapper.from_orm(orm)
    assert entity.row_version == 3
    assert entity.definition_version == 1


# ── from_orm with migration ───────────────────────────────────────────────────


def add_slug(data: dict) -> dict:
    data["slug"] = data["name"].lower()
    return data


@dataclass
class _VersionedWithSlug(VersionedDomainModel):
    name: str
    slug: str = ""


class SlugMigrator(DomainMigrator):
    steps = [add_slug]  # current_version = 2


def test_from_orm_triggers_migration_when_stale():
    mapper = _make_mapper(_VersionedWithSlug, migrator=SlugMigrator)
    ts = datetime.now(tz=timezone.utc)
    orm = _ORM(
        id=1,
        name="Hello World",
        slug="",
        created_at=ts,
        updated_at=ts,
        definition_version=1,
        row_version=1,
    )
    entity = mapper.from_orm(orm)
    assert entity.slug == "hello world"
    assert entity.definition_version == 2


def test_from_orm_skips_migration_when_current():
    mapper = _make_mapper(_VersionedWithSlug, migrator=SlugMigrator)
    ts = datetime.now(tz=timezone.utc)
    orm = _ORM(
        id=1,
        name="Already",
        slug="already",
        created_at=ts,
        updated_at=ts,
        definition_version=2,
        row_version=1,
    )
    entity = mapper.from_orm(orm)
    assert entity.slug == "already"  # untouched


def test_from_orm_no_migrator_reads_stored_version():
    mapper = _make_mapper(_Versioned)
    ts = datetime.now(tz=timezone.utc)
    orm = _ORM(
        id=1,
        name="Edo",
        email="a@b.com",
        created_at=ts,
        updated_at=ts,
        definition_version=5,
        row_version=2,
    )
    entity = mapper.from_orm(orm)
    assert entity.definition_version == 5  # passed through as-is


# ── sync_to_orm ───────────────────────────────────────────────────────────────


def test_sync_to_orm_updates_business_fields():
    mapper = _make_mapper(_Plain)
    entity = _Plain(name="Updated", score=99)
    orm = _ORM(id=1, name="Old", score=0)
    mapper.sync_to_orm(entity, orm)
    assert orm.name == "Updated"
    assert orm.score == 99


def test_sync_to_orm_never_touches_created_at():
    mapper = _make_mapper(_Audited)
    original_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    entity = _Audited(name="Edo")
    object.__setattr__(entity, "created_at", original_ts)
    object.__setattr__(entity, "updated_at", original_ts)
    orm = _ORM(id=1, name="Old", created_at=original_ts, updated_at=original_ts)
    mapper.sync_to_orm(entity, orm)
    assert orm.created_at == original_ts  # unchanged


def test_sync_to_orm_refreshes_updated_at():
    mapper = _make_mapper(_Audited)
    original_ts = datetime(2020, 1, 1, tzinfo=timezone.utc)
    entity = _Audited(name="Edo")
    object.__setattr__(entity, "created_at", original_ts)
    object.__setattr__(entity, "updated_at", original_ts)
    orm = _ORM(id=1, name="Old", created_at=original_ts, updated_at=original_ts)
    before = datetime.now(tz=timezone.utc)
    mapper.sync_to_orm(entity, orm)
    assert orm.updated_at >= before


def test_sync_to_orm_increments_row_version():
    mapper = _make_mapper(_Versioned)
    ts = datetime.now(tz=timezone.utc)
    entity = _Versioned(name="Edo")
    object.__setattr__(entity, "row_version", 3)
    object.__setattr__(entity, "definition_version", 1)
    object.__setattr__(entity, "created_at", ts)
    object.__setattr__(entity, "updated_at", ts)
    orm = _ORM(
        id=1,
        name="Old",
        email="old@b.com",
        created_at=ts,
        updated_at=ts,
        definition_version=1,
        row_version=3,
    )
    mapper.sync_to_orm(entity, orm)
    assert orm.row_version == 4


def test_sync_to_orm_raises_stale_entity_error_on_conflict():
    mapper = _make_mapper(_Versioned)
    ts = datetime.now(tz=timezone.utc)
    entity = _Versioned(name="Edo")
    object.__setattr__(entity, "row_version", 2)  # loaded at v2
    object.__setattr__(entity, "definition_version", 1)
    object.__setattr__(entity, "created_at", ts)
    object.__setattr__(entity, "updated_at", ts)
    orm = _ORM(
        id=1,
        name="Concurrent",
        email="x@x.com",
        created_at=ts,
        updated_at=ts,
        definition_version=1,
        row_version=5,  # another writer got there first
    )
    with pytest.raises(StaleEntityError) as exc_info:
        mapper.sync_to_orm(entity, orm)
    assert exc_info.value.expected_version == 2
    assert exc_info.value.actual_version == 5


# ── migrator normalisation ────────────────────────────────────────────────────


def test_mapper_normalises_class_to_instance():
    mapper = _make_mapper(_VersionedWithSlug, migrator=SlugMigrator)
    assert isinstance(mapper._migrator, SlugMigrator)


def test_mapper_accepts_instance_directly():
    instance = SlugMigrator()
    mapper = _make_mapper(_VersionedWithSlug, migrator=instance)
    assert mapper._migrator is instance


def test_mapper_accepts_callable_factory():
    factory = lambda: SlugMigrator()  # noqa: E731
    mapper = _make_mapper(_VersionedWithSlug, migrator=factory)
    assert isinstance(mapper._migrator, SlugMigrator)


def test_mapper_none_migrator_stays_none():
    mapper = _make_mapper(_Plain, migrator=None)
    assert mapper._migrator is None


# ── composite PK ─────────────────────────────────────────────────────────────


@dataclass
class _Composite(DomainModel):
    user_id: int
    role_id: int


class _CompositePKMapper(AbstractMapper):
    @property
    def _pk_orm_attrs(self) -> list[str]:
        return ["user_id", "role_id"]


def test_from_orm_composite_pk_becomes_tuple():
    mapper = _CompositePKMapper(_Composite, _ORM)
    orm = _ORM(user_id=1, role_id=2)
    entity = mapper.from_orm(orm)
    assert entity.pk == (1, 2)
