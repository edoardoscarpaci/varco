"""
Tests for SAModelFactory — ORM relationship and many-to-many generation.

Uses an in-memory SQLite database (aiosqlite) so no external DB is needed
for integration-style checks (table creation, round-trips via relationships).

Sections
--------
- One-to-many (Relationship, uselist=True)
    - relationship attribute present on source ORM class
    - back_populates wiring on both sides
- Many-to-one (Relationship, uselist=False)
- One-to-one (Relationship, uselist=False, explicit FK)
- ManyToMany
    - relationship attribute on source
    - association table created in metadata
    - back_populates wiring
    - association table reuse across two sides
    - custom source_fk / target_fk column names
- Circular references
    - build() does not hang on A→B and B→A
- Integration (SQLite)
    - relationship actually loads related ORM objects after INSERT

Thread safety:  ✅ Each test gets a fresh factory + base (from conftest fixtures).
Async safety:   ✅ Integration tests use async SQLite sessions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated

import pytest
import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import RelationshipProperty

from varco_core.meta import ForeignKey, Relationship, ManyToMany
from varco_core.model import DomainModel


# ── Domain model helpers ──────────────────────────────────────────────────────


def _col(orm_cls: type, name: str) -> sa.Column:
    """Convenience accessor for a column by name."""
    return orm_cls.__table__.c[name]


def _rel(orm_cls: type, name: str) -> RelationshipProperty:
    """Return the SA RelationshipProperty for attr_name on orm_cls."""
    mapper = sa_inspect(orm_cls)
    return mapper.relationships[name]


# ── Domain models ─────────────────────────────────────────────────────────────


@dataclass
class _Author(DomainModel):
    """
    Author entity — owns many Posts.
    Declares a one-to-many relationship 'posts' on the generated ORM class.
    """

    name: str = ""

    class Meta:
        table = "authors_rel"
        relationships = [
            Relationship(
                attr_name="posts",
                target=None,  # set below to avoid forward-ref
                uselist=True,
                back_populates="author",
            )
        ]


@dataclass
class _Post(DomainModel):
    """
    Post entity — belongs to one Author.
    Declares a FK to authors_rel and a many-to-one relationship 'author'.
    """

    title: str = ""
    author_id: Annotated[int, ForeignKey("authors_rel.id")] = 0

    class Meta:
        table = "posts_rel"
        relationships = [
            Relationship(
                attr_name="author",
                target=None,  # set below
                foreign_keys=("author_id",),
                uselist=False,
                back_populates="posts",
            )
        ]


# Resolve forward refs now that both classes exist.
_Author.Meta.relationships[0] = Relationship(
    attr_name="posts",
    target=_Post,
    uselist=True,
    back_populates="author",
)
_Post.Meta.relationships[0] = Relationship(
    attr_name="author",
    target=_Author,
    foreign_keys=("author_id",),
    uselist=False,
    back_populates="posts",
)


@dataclass
class _Tag(DomainModel):
    """Tag entity — participates in M2M with Article."""

    label: str = ""

    class Meta:
        table = "tags_m2m"


@dataclass
class _Article(DomainModel):
    """
    Article entity — has many Tags via 'article_tags' association table.
    Declares a ManyToMany relationship 'tags'.
    """

    title: str = ""

    class Meta:
        table = "articles_m2m"
        relationships = [
            ManyToMany(
                attr_name="tags",
                target=_Tag,
                through="article_tags",
                back_populates="articles",
            )
        ]


# Wire back-populates on Tag side.
_Tag.Meta.relationships = [
    ManyToMany(
        attr_name="articles",
        target=_Article,
        through="article_tags",
        back_populates="tags",
    )
]


# ── One-to-many / many-to-one ─────────────────────────────────────────────────


class TestOneToManyRelationship:
    def test_relationship_attr_on_source(self, factory) -> None:
        # After build(), the source ORM class should have a 'posts' SA relationship.
        orm_cls, _ = factory.build(_Author)
        assert hasattr(orm_cls, "posts")

    def test_relationship_is_sa_relationship_property(self, factory) -> None:
        orm_cls, _ = factory.build(_Author)
        assert isinstance(_rel(orm_cls, "posts"), RelationshipProperty)

    def test_uselist_true_for_one_to_many(self, factory) -> None:
        orm_cls, _ = factory.build(_Author)
        assert _rel(orm_cls, "posts").uselist is True

    def test_back_populates_wired(self, factory) -> None:
        # Build both sides — back_populates must be wired on both.
        author_orm, _ = factory.build(_Author)
        post_orm, _ = factory.build(_Post)
        assert _rel(author_orm, "posts").back_populates == "author"
        assert _rel(post_orm, "author").back_populates == "posts"

    def test_many_to_one_uselist_false(self, factory) -> None:
        factory.build(_Author)  # build source first
        post_orm, _ = factory.build(_Post)
        assert _rel(post_orm, "author").uselist is False

    def test_fk_column_constraint_present(self, factory) -> None:
        # The FK column itself should still be present and constrained.
        _, _ = factory.build(_Author)
        post_orm, _ = factory.build(_Post)
        col = _col(post_orm, "author_id")
        fk_targets = {fk.target_fullname for fk in col.foreign_keys}
        assert "authors_rel.id" in fk_targets

    def test_build_is_idempotent(self, factory) -> None:
        a1, _ = factory.build(_Author)
        a2, _ = factory.build(_Author)
        assert a1 is a2


# ── ManyToMany relationship ────────────────────────────────────────────────────


class TestManyToManyRelationship:
    def test_m2m_attr_on_source(self, factory) -> None:
        orm_cls, _ = factory.build(_Article)
        assert hasattr(orm_cls, "tags")

    def test_m2m_is_sa_relationship_property(self, factory) -> None:
        orm_cls, _ = factory.build(_Article)
        assert isinstance(_rel(orm_cls, "tags"), RelationshipProperty)

    def test_m2m_uselist_true(self, factory) -> None:
        orm_cls, _ = factory.build(_Article)
        assert _rel(orm_cls, "tags").uselist is True

    def test_association_table_in_metadata(self, factory, base) -> None:
        factory.build(_Article)
        # The through-table must be registered in the shared metadata.
        assert "article_tags" in base.metadata.tables

    def test_association_table_has_fk_columns(self, factory, base) -> None:
        factory.build(_Article)
        assoc = base.metadata.tables["article_tags"]
        col_names = set(assoc.c.keys())
        # Default FK column naming: {source_table}_id and {target_table}_id
        assert "articles_m2m_id" in col_names or any("article" in n for n in col_names)
        assert "tags_m2m_id" in col_names or any("tag" in n for n in col_names)

    def test_back_populates_on_both_sides(self, factory) -> None:
        article_orm, _ = factory.build(_Article)
        tag_orm, _ = factory.build(_Tag)
        assert _rel(article_orm, "tags").back_populates == "articles"
        assert _rel(tag_orm, "articles").back_populates == "tags"

    def test_association_table_reused_on_second_build(self, factory, base) -> None:
        # Building _Article first creates the assoc table.
        # Building _Tag (which also references the same through table) must
        # reuse the existing table — not try to create a duplicate.
        factory.build(_Article)
        table_count_before = len(base.metadata.tables)
        factory.build(_Tag)
        # No new tables should appear after building the second side.
        assert len(base.metadata.tables) == table_count_before


# ── Custom FK column names ─────────────────────────────────────────────────────


@dataclass
class _Student(DomainModel):
    name: str = ""

    class Meta:
        table = "students_m2m"


@dataclass
class _Course(DomainModel):
    title: str = ""

    class Meta:
        table = "courses_m2m"
        relationships = [
            ManyToMany(
                attr_name="students",
                target=_Student,
                through="enrollments",
                source_fk="course_fk",
                target_fk="student_fk",
            )
        ]


class TestManyToManyCustomFK:
    def test_custom_fk_column_names_in_assoc_table(self, factory, base) -> None:
        factory.build(_Course)
        assoc = base.metadata.tables["enrollments"]
        assert "course_fk" in assoc.c
        assert "student_fk" in assoc.c


# ── Circular reference safety ─────────────────────────────────────────────────


@dataclass
class _A(DomainModel):
    b_id: Annotated[int, ForeignKey("b_circ.id")] = 0

    class Meta:
        table = "a_circ"


@dataclass
class _B(DomainModel):
    class Meta:
        table = "b_circ"
        relationships = [
            Relationship(
                attr_name="a_list",
                target=_A,
                uselist=True,
            )
        ]


class TestCircularReferenceSafety:
    def test_build_a_then_b_does_not_hang(self, factory) -> None:
        # Build _A first — it should trigger build of _B via its FK relationship.
        orm_a, _ = factory.build(_A)
        orm_b, _ = factory.build(_B)
        assert orm_a is not None
        assert orm_b is not None

    def test_build_b_then_a_does_not_hang(self, factory) -> None:
        orm_b, _ = factory.build(_B)
        orm_a, _ = factory.build(_A)
        assert orm_b is not None
        assert orm_a is not None

    def test_both_classes_in_registry(self, factory) -> None:
        from varco_sa.factory import SAModelRegistry

        factory.build(_A)
        factory.build(_B)
        assert SAModelRegistry.get(_A) is not None
        assert SAModelRegistry.get(_B) is not None


# ── Lazy loading strategy ──────────────────────────────────────────────────────


@dataclass
class _Owner(DomainModel):
    name: str = ""

    class Meta:
        table = "owners_lazy"
        relationships = [
            Relationship(
                attr_name="items",
                target=None,  # resolved below
                uselist=True,
                lazy="noload",
            )
        ]


@dataclass
class _Item(DomainModel):
    owner_id: Annotated[int, ForeignKey("owners_lazy.id")] = 0

    class Meta:
        table = "items_lazy"


_Owner.Meta.relationships[0] = Relationship(
    attr_name="items",
    target=_Item,
    uselist=True,
    lazy="noload",
)


class TestLazyLoadingStrategy:
    def test_lazy_strategy_applied(self, factory) -> None:
        orm_cls, _ = factory.build(_Owner)
        rel = _rel(orm_cls, "items")
        # SA stores lazy strategy as a string on the relationship.
        assert rel.lazy == "noload"


# ── Integration: real SQLite DB ────────────────────────────────────────────────


@pytest.fixture
async def session_with_rels(base, factory):
    """
    Async session backed by in-memory SQLite with author/post tables created.

    Builds both ORM classes (wires relationships), creates all tables, yields a
    session, then drops tables and disposes the engine.
    """
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    # Build both ORM classes so relationships are wired before create_all.
    factory.build(_Author)
    factory.build(_Post)

    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with engine.begin() as conn:
        await conn.run_sync(base.metadata.create_all)

    async_session = async_sessionmaker(engine, expire_on_commit=False)
    async with async_session() as s:
        yield s

    async with engine.begin() as conn:
        await conn.run_sync(base.metadata.drop_all)
    await engine.dispose()


async def test_relationship_loads_related_objects(
    base, factory, session_with_rels
) -> None:
    """
    End-to-end: insert an Author + Post via raw ORM, then load the Author and
    verify that the 'posts' relationship collection is populated.
    """
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    author_orm_cls = factory.build(_Author)[0]
    post_orm_cls = factory.build(_Post)[0]

    session = session_with_rels

    # Insert via ORM
    author_row = author_orm_cls(name="Alice")
    session.add(author_row)
    await session.flush()

    post_row = post_orm_cls(title="Hello", author_id=author_row.id)
    session.add(post_row)
    await session.commit()

    # Reload with relationship loaded — selectinload avoids N+1 in async.
    stmt = (
        select(author_orm_cls)
        .options(selectinload(author_orm_cls.posts))
        .where(author_orm_cls.id == author_row.id)
    )
    result = await session.execute(stmt)
    loaded_author = result.scalar_one()

    assert len(loaded_author.posts) == 1
    assert loaded_author.posts[0].title == "Hello"


async def test_many_to_one_loads_parent(base, factory, session_with_rels) -> None:
    """
    Insert Author + Post, load Post with 'author' relationship.
    """
    from sqlalchemy import select
    from sqlalchemy.orm import selectinload

    author_orm_cls = factory.build(_Author)[0]
    post_orm_cls = factory.build(_Post)[0]

    session = session_with_rels

    author_row = author_orm_cls(name="Bob")
    session.add(author_row)
    await session.flush()

    post_row = post_orm_cls(title="Test Post", author_id=author_row.id)
    session.add(post_row)
    await session.commit()

    stmt = (
        select(post_orm_cls)
        .options(selectinload(post_orm_cls.author))
        .where(post_orm_cls.id == post_row.id)
    )
    result = await session.execute(stmt)
    loaded_post = result.scalar_one()

    assert loaded_post.author is not None
    assert loaded_post.author.name == "Bob"
