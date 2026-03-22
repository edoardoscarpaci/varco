"""
Unit tests for varco_core.meta (MetaReader, ParsedMeta, hints)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated
from uuid import UUID

import pytest

from varco_core.meta import (
    CheckConstraint,
    FieldHint,
    ForeignKey,
    MetaReader,
    PKStrategy,
    PrimaryKey,
    UniqueConstraint,
    pk_field,
)
from varco_core.migrator import DomainMigrator
from varco_core.model import DomainModel, VersionedDomainModel


# ── FieldHint ─────────────────────────────────────────────────────────────────


def test_field_hint_unique_and_index_raises():
    with pytest.raises(ValueError, match="unique.*index"):
        FieldHint(unique=True, index=True)


def test_field_hint_zero_max_length_raises():
    with pytest.raises(ValueError, match="max_length"):
        FieldHint(max_length=0)


def test_field_hint_negative_max_length_raises():
    with pytest.raises(ValueError, match="max_length"):
        FieldHint(max_length=-5)


def test_field_hint_valid():
    h = FieldHint(unique=True, max_length=120)
    assert h.unique is True
    assert h.max_length == 120
    assert h.index is False


# ── ForeignKey.resolve ────────────────────────────────────────────────────────


def test_fk_resolve_string_form():
    fk = ForeignKey("users.id")
    assert fk.resolve() == "users.id"


def test_fk_resolve_domain_class_form():
    @dataclass
    class Author(DomainModel):
        name: str

        class Meta:
            table = "authors"

    fk = ForeignKey(Author, field="pk")
    assert fk.resolve() == "authors.id"


def test_fk_resolve_domain_class_non_pk_field():
    @dataclass
    class Tag(DomainModel):
        slug: str

        class Meta:
            table = "tags"

    fk = ForeignKey(Tag, field="slug")
    assert fk.resolve() == "tags.slug"


# ── UniqueConstraint ──────────────────────────────────────────────────────────


def test_unique_constraint_requires_two_fields():
    with pytest.raises(ValueError, match="at least 2 fields"):
        UniqueConstraint("only_one")


def test_unique_constraint_accepts_two_fields():
    uc = UniqueConstraint("a", "b", name="uq_test")
    assert uc.fields == ("a", "b")
    assert uc.name == "uq_test"


# ── MetaReader.read — basic model ─────────────────────────────────────────────


@dataclass
class _Simple(DomainModel):
    name: Annotated[str, FieldHint(max_length=100)]
    score: int = 0

    class Meta:
        table = "simple"


def test_meta_reader_table_name():
    meta = MetaReader.read(_Simple)
    assert meta.table == "simple"


def test_meta_reader_default_pk_int_auto():
    meta = MetaReader.read(_Simple)
    assert meta.pk_type is int
    assert meta.pk_strategy is PKStrategy.INT_AUTO


def test_meta_reader_field_hints():
    meta = MetaReader.read(_Simple)
    assert meta.fields["name"].max_length == 100
    assert meta.fields["score"] is None


def test_meta_reader_no_foreign_keys():
    meta = MetaReader.read(_Simple)
    assert meta.foreign_keys == {}


# ── MetaReader.read — UUID pk ─────────────────────────────────────────────────


@dataclass
class _UUIDModel(DomainModel):
    pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
    title: str = ""

    class Meta:
        table = "uuid_model"


def test_meta_reader_uuid_pk_strategy():
    meta = MetaReader.read(_UUIDModel)
    assert meta.pk_type is UUID
    assert meta.pk_strategy is PKStrategy.UUID_AUTO


# ── MetaReader.read — composite PK ───────────────────────────────────────────


@dataclass
class _Composite(DomainModel):
    user_id: Annotated[int, PrimaryKey()]
    role_id: Annotated[int, PrimaryKey()]

    class Meta:
        table = "user_roles"


def test_meta_reader_composite_pk_fields():
    meta = MetaReader.read(_Composite)
    assert meta.is_composite_pk is True
    assert meta.composite_pk_fields == ["user_id", "role_id"]


def test_meta_reader_composite_pk_no_strategy():
    meta = MetaReader.read(_Composite)
    assert meta.pk_strategy is None
    assert meta.pk_type is None


# ── MetaReader.read — FK ──────────────────────────────────────────────────────


@dataclass
class _Post(DomainModel):
    author_id: Annotated[int, ForeignKey("users.id")]
    title: str = ""

    class Meta:
        table = "posts"


def test_meta_reader_foreign_key_captured():
    meta = MetaReader.read(_Post)
    assert "author_id" in meta.foreign_keys
    assert meta.foreign_keys["author_id"].resolve() == "users.id"


# ── MetaReader.read — constraints ─────────────────────────────────────────────


@dataclass
class _Constrained(DomainModel):
    email: str
    username: str

    class Meta:
        table = "constrained"
        constraints = [
            UniqueConstraint("email", "username", name="uq_email_user"),
            CheckConstraint("len(email) > 0", name="ck_email"),
        ]


def test_meta_reader_constraints_captured():
    meta = MetaReader.read(_Constrained)
    assert len(meta.constraints) == 2
    names = [c.name for c in meta.constraints]
    assert "uq_email_user" in names
    assert "ck_email" in names


def test_meta_reader_constraint_unknown_field_raises():
    @dataclass
    class _Bad(DomainModel):
        name: str

        class Meta:
            table = "bad"
            constraints = [UniqueConstraint("name", "nonexistent")]

    with pytest.raises(ValueError, match="nonexistent"):
        MetaReader.read(_Bad)


# ── MetaReader.read — migrator ────────────────────────────────────────────────


def add_slug(data: dict) -> dict:
    data["slug"] = data["name"].lower()
    return data


class _TestMigrator(DomainMigrator):
    steps = [add_slug]


@dataclass
class _VersionedPost(VersionedDomainModel):
    name: str
    slug: str = ""

    class Meta:
        table = "versioned_posts"
        migrator = _TestMigrator


def test_meta_reader_migrator_captured():
    meta = MetaReader.read(_VersionedPost)
    assert meta.migrator is _TestMigrator


def test_meta_reader_no_migrator_is_none():
    meta = MetaReader.read(_Simple)
    assert meta.migrator is None


# ── MetaReader.read — table name defaults to class name ───────────────────────


@dataclass
class _NoMeta(DomainModel):
    value: int = 0


def test_meta_reader_default_table_name():
    meta = MetaReader.read(_NoMeta)
    assert meta.table == "_NoMeta".lower()


# ── MetaReader.domain_fields ──────────────────────────────────────────────────


def test_domain_fields_excludes_pk_and_underscore():
    fields = MetaReader.domain_fields(_Simple)
    names = [f.name for f in fields]
    assert "pk" not in names
    assert "_raw_orm" not in names
    assert "name" in names
    assert "score" in names


def test_domain_fields_versioned_includes_system_fields():
    """System fields (init=False) appear in domain_fields — mapper handles them."""
    fields = MetaReader.domain_fields(_VersionedPost)
    names = [f.name for f in fields]
    assert "name" in names
    assert "created_at" in names
    assert "updated_at" in names
    assert "definition_version" in names
    assert "row_version" in names
