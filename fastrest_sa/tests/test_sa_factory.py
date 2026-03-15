"""
Tests for SAModelFactory — ORM class generation.

Verifies that the generated SA classes have the correct columns, types,
constraints, and that the mapper translates domain objects correctly.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

import sqlalchemy as sa

from fastrest_core.meta import (
    FieldHint,
    ForeignKey,
    PrimaryKey,
    PKStrategy,
    pk_field,
    UniqueConstraint,
)
from fastrest_core.model import DomainModel, VersionedDomainModel
from fastrest_core.migrator import DomainMigrator
from fastrest_sa.factory import SAModelRegistry


# ── Helpers ───────────────────────────────────────────────────────────────────


def _col(orm_cls: type, name: str) -> sa.Column:
    return orm_cls.__table__.c[name]


# ── Basic column generation ───────────────────────────────────────────────────


@dataclass
class _User(DomainModel):
    name: Annotated[str, FieldHint(max_length=120)]
    email: Annotated[str, FieldHint(unique=True)]
    score: float = 0.0
    active: bool = True

    class Meta:
        table = "users_factory"


def test_factory_generates_orm_class(factory):
    orm_cls, _ = factory.build(_User)
    assert orm_cls.__tablename__ == "users_factory"
    assert hasattr(orm_cls, "id")
    assert hasattr(orm_cls, "name")
    assert hasattr(orm_cls, "email")


def test_factory_str_with_max_length(factory):
    orm_cls, _ = factory.build(_User)
    col = _col(orm_cls, "name")
    assert isinstance(col.type, sa.String)
    assert col.type.length == 120


def test_factory_str_unique(factory):
    orm_cls, _ = factory.build(_User)
    col = _col(orm_cls, "email")
    assert col.unique is True


def test_factory_float_column(factory):
    orm_cls, _ = factory.build(_User)
    col = _col(orm_cls, "score")
    assert isinstance(col.type, sa.Float)


def test_factory_bool_column(factory):
    orm_cls, _ = factory.build(_User)
    col = _col(orm_cls, "active")
    assert isinstance(col.type, sa.Boolean)


def test_factory_pk_int_auto(factory):
    orm_cls, _ = factory.build(_User)
    pk_col = _col(orm_cls, "id")
    assert pk_col.primary_key is True
    assert isinstance(pk_col.type, sa.Integer)


# ── UUID pk ───────────────────────────────────────────────────────────────────


@dataclass
class _Doc(DomainModel):
    pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
    title: str = ""

    class Meta:
        table = "docs_factory"


def test_factory_uuid_pk(factory):
    from sqlalchemy.dialects.postgresql import UUID as PG_UUID

    orm_cls, _ = factory.build(_Doc)
    pk_col = _col(orm_cls, "id")
    assert pk_col.primary_key is True
    assert isinstance(pk_col.type, PG_UUID)


# ── JSON columns ──────────────────────────────────────────────────────────────


@dataclass
class _WithJSON(DomainModel):
    # NOTE: "metadata" is reserved by DeclarativeBase (it holds the MetaData
    # instance).  Using "extra" avoids the SA attribute conflict.
    extra: dict
    tags: list

    class Meta:
        table = "with_json_factory"


def test_factory_dict_maps_to_json(factory):
    orm_cls, _ = factory.build(_WithJSON)
    col = _col(orm_cls, "extra")
    assert isinstance(col.type, sa.JSON)


def test_factory_list_maps_to_json(factory):
    orm_cls, _ = factory.build(_WithJSON)
    col = _col(orm_cls, "tags")
    assert isinstance(col.type, sa.JSON)


# ── Foreign key ───────────────────────────────────────────────────────────────


@dataclass
class _Comment(DomainModel):
    post_id: Annotated[int, ForeignKey("posts.id")]
    body: str = ""

    class Meta:
        table = "comments_factory"


def test_factory_foreign_key_wired(factory):
    orm_cls, _ = factory.build(_Comment)
    col = _col(orm_cls, "post_id")
    fk_targets = {fk.target_fullname for fk in col.foreign_keys}
    assert "posts.id" in fk_targets


# ── Composite PK ─────────────────────────────────────────────────────────────


@dataclass
class _Membership(DomainModel):
    user_id: Annotated[int, PrimaryKey()]
    group_id: Annotated[int, PrimaryKey()]

    class Meta:
        table = "memberships_factory"


def test_factory_composite_pk_columns(factory):
    orm_cls, _ = factory.build(_Membership)
    assert _col(orm_cls, "user_id").primary_key is True
    assert _col(orm_cls, "group_id").primary_key is True


def test_factory_composite_pk_mapper_attrs(factory):
    _, mapper = factory.build(_Membership)
    assert mapper._pk_orm_attrs == ["user_id", "group_id"]


# ── Table constraints ─────────────────────────────────────────────────────────


@dataclass
class _Product(DomainModel):
    sku: str
    category: str

    class Meta:
        table = "products_factory"
        constraints = [UniqueConstraint("sku", "category", name="uq_sku_cat")]


def test_factory_unique_constraint_in_table_args(factory):
    orm_cls, _ = factory.build(_Product)
    constraint_names = {
        c.name for c in orm_cls.__table__.constraints if hasattr(c, "name") and c.name
    }
    assert "uq_sku_cat" in constraint_names


# ── VersionedDomainModel columns ──────────────────────────────────────────────


@dataclass
class _Entity(VersionedDomainModel):
    name: str

    class Meta:
        table = "entities_factory"


def test_factory_versioned_has_system_columns(factory):
    orm_cls, _ = factory.build(_Entity)
    for col_name in ("created_at", "updated_at", "definition_version", "row_version"):
        assert col_name in orm_cls.__table__.c, f"Missing column: {col_name}"


# ── Registry ─────────────────────────────────────────────────────────────────


def test_factory_registers_orm_class(factory):
    orm_cls, _ = factory.build(_User)
    assert SAModelRegistry.get(_User) is orm_cls


def test_factory_is_idempotent(factory):
    orm1, _ = factory.build(_User)
    orm2, _ = factory.build(_User)
    assert orm1 is orm2


# ── Migrator wired to mapper ──────────────────────────────────────────────────


def add_upper(data: dict) -> dict:
    data["name"] = data["name"].upper()
    return data


class _Migrator(DomainMigrator):
    steps = [add_upper]


@dataclass
class _VersionedEntity(VersionedDomainModel):
    name: str

    class Meta:
        table = "versioned_entities_factory"
        migrator = _Migrator


def test_factory_passes_migrator_to_mapper(factory):
    _, mapper = factory.build(_VersionedEntity)
    assert isinstance(mapper._migrator, _Migrator)
