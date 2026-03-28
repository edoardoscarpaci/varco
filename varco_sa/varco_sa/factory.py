"""
varco_sa.factory
====================
Runtime SQLAlchemy ORM class generator.

Reads ``ParsedMeta`` from ``varco_core`` and produces a
``DeclarativeBase`` subclass with correct columns, types, FK constraints,
table-level constraints, and a post-build ``Meta.customize`` hook.

Type mapping
------------
    str      → String(max_length) or Text
    int      → Integer
    float    → Float
    bool     → Boolean
    datetime → DateTime(timezone=True)
    date     → Date
    UUID     → PG_UUID(as_uuid=True)
    bytes    → LargeBinary
    T | None → same column, nullable=True

Unsupported types raise ``TypeError`` at startup — fail-fast.
"""

from __future__ import annotations

import typing
from datetime import date, datetime
from functools import cached_property
from typing import Any, TypeVar
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    JSON,
    LargeBinary,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import DeclarativeBase

from varco_core.mapper import AbstractMapper
from varco_core.meta import (
    CheckConstraint as DomainCheckConstraint,
    MetaReader,
    ParsedMeta,
    PKStrategy,
    UniqueConstraint as DomainUniqueConstraint,
)
from varco_core.model import DomainModel

D = TypeVar("D", bound=DomainModel)

# ── Python type → SA column type ──────────────────────────────────────────────
_SA_TYPE_MAP: dict[type, Any] = {
    str: lambda ml: String(ml) if ml else Text(),
    int: lambda _: Integer(),
    float: lambda _: Float(),
    bool: lambda _: Boolean(),
    datetime: lambda _: DateTime(timezone=True),
    date: lambda _: Date(),
    UUID: lambda _: PG_UUID(as_uuid=True),
    bytes: lambda _: LargeBinary(),
    dict: lambda _: JSON(),
    list: lambda _: JSON(),
}


def _sa_type(py_type: type, field_name: str, max_length: int | None) -> Any:
    """
    Map a bare Python type to a SQLAlchemy column type instance.

    Args:
        py_type:    Bare (non-Optional, non-Annotated) Python type.
        field_name: Used in the error message only.
        max_length: Only meaningful for ``str``.

    Raises:
        TypeError: ``py_type`` not in the supported type map.
    """
    factory = _SA_TYPE_MAP.get(py_type)
    if factory is None:
        raise TypeError(
            f"Field {field_name!r} has type {py_type!r} which cannot be "
            "automatically mapped to a SQLAlchemy column type. "
            f"Supported types: {list(_SA_TYPE_MAP.keys())}."
        )
    return factory(max_length)


# ── Auto-generated mapper ─────────────────────────────────────────────────────


class _SAAutoMapper(AbstractMapper[D, Any]):
    """
    Mapper for auto-generated SA models.

    ``_pk_orm_attrs`` is ``["id"]`` for single PKs or the list of composite
    field names — set at construction by the factory.

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ Sync allocation only.
    """

    def __init__(
        self,
        domain_cls: type[D],
        orm_cls: type,
        pk_orm_attrs: list[str],
        migrator: Any = None,
    ) -> None:
        super().__init__(domain_cls, orm_cls, migrator=migrator)
        self._pk_attrs = pk_orm_attrs

    @cached_property
    def _pk_orm_attrs(self) -> list[str]:
        return self._pk_attrs


# ── Registry ──────────────────────────────────────────────────────────────────


class SAModelRegistry:
    """
    Process-level registry mapping ``DomainModel`` subclasses to their
    auto-generated SQLAlchemy ORM classes.

    Retrieve the generated class for ``cast_raw()`` or post-build
    customisation via ``SAModelRegistry.get(MyDomainClass)``::

        UserORM = SAModelRegistry.get(User)
        # Add SA relationships, event listeners, etc.
        UserORM.posts = relationship("PostORM", back_populates="author")

    Thread safety:  ⚠️ Write at startup only; reads are safe after that.
    """

    _registry: dict[type, type] = {}

    @classmethod
    def get(cls, domain_cls: type[D]) -> type:
        """
        Return the auto-generated SA ORM class for ``domain_cls``.

        Raises:
            KeyError: ``domain_cls`` was never built by ``SAModelFactory``.
        """
        try:
            return cls._registry[domain_cls]
        except KeyError:
            raise KeyError(
                f"No SA ORM class generated for {domain_cls.__name__!r}. "
                "Call provider.register(YourClass) first."
            ) from None

    @classmethod
    def _register(cls, domain_cls: type, orm_cls: type) -> None:
        cls._registry[domain_cls] = orm_cls


# ── Main factory ──────────────────────────────────────────────────────────────


class SAModelFactory:
    """
    Generates a SQLAlchemy ORM class + companion mapper from a
    ``DomainModel`` subclass at runtime.

    Supports:
    - Single PKs (INT_AUTO, UUID_AUTO, STR_ASSIGNED, CUSTOM)
    - Composite PKs (``PrimaryKey()`` on business fields)
    - Foreign keys (string and domain-class form)
    - Table-level UniqueConstraint and CheckConstraint
    - Post-build ``Meta.customize`` hook

    DESIGN: ``type()`` dynamic class creation
      ✅ No user-authored ORM class needed
      ✅ Generated class is a full ``DeclarativeBase`` subclass — SA
         relationships, events, and any SA feature available via
         ``SAModelRegistry.get()`` or ``Meta.customize``
      ❌ Dynamic classes are harder to inspect in a debugger — ``__name__``
         is set to ``{ClassName}ORM`` for readability
      ❌ Relationships must be wired post-build (SA requires both sides
         to exist first)

    Thread safety:  ⚠️ Call ``build()`` at startup before concurrent use.
    Async safety:   ✅ ``build()`` is synchronous.

    Args:
        base: Shared ``DeclarativeBase`` subclass — all generated tables land
              in ``base.metadata``.  Pass this to ``metadata.create_all()``.

    Edge cases:
        - ``build()`` is idempotent — returns the cached result on repeat calls.
        - Unsupported field types raise ``TypeError`` at startup.
    """

    def __init__(self, base: type[DeclarativeBase]) -> None:
        self._base = base
        self._cache: dict[type, tuple[type, AbstractMapper]] = {}
        # Association tables created by ManyToMany declarations.
        # Keyed by through-table name to avoid duplicate Table registrations
        # if two domain classes declare the same M2M through table.
        self._assoc_tables: dict[str, sa.Table] = {}

    def build(self, domain_cls: type[D]) -> tuple[type, _SAAutoMapper[D, Any]]:
        """
        Generate (or return cached) SA ORM class and mapper for ``domain_cls``.

        Args:
            domain_cls: Any ``DomainModel`` subclass.

        Returns:
            ``(generated_orm_class, configured_mapper)``

        Raises:
            TypeError:  Unsupported field type or PK/strategy mismatch.
            ValueError: Composite PK field carries a strategy, or a constraint
                        references an unknown field name.
        """
        if domain_cls in self._cache:
            return self._cache[domain_cls]  # type: ignore[return-value]

        meta = MetaReader.read(domain_cls)

        if meta.is_composite_pk:
            pk_cols, pk_attrs = self._build_composite_pk_columns(domain_cls, meta)
            non_pk_cols = self._build_non_pk_columns(domain_cls, meta)
            all_cols = {**pk_cols, **non_pk_cols}
        else:
            pk_col = self._build_single_pk_column(meta)
            non_pk_cols = self._build_non_pk_columns(domain_cls, meta)
            all_cols = {"id": pk_col, **non_pk_cols}
            pk_attrs = ["id"]

        table_args = self._build_table_args(meta)

        orm_attrs: dict[str, Any] = {
            "__tablename__": meta.table,
            "__table_args__": table_args,
            **all_cols,
        }

        orm_cls = type(f"{domain_cls.__name__}ORM", (self._base,), orm_attrs)

        mapper = _SAAutoMapper(
            domain_cls=domain_cls,
            orm_cls=orm_cls,
            pk_orm_attrs=pk_attrs,
            migrator=meta.migrator,
        )

        # Cache the ORM class BEFORE processing relationships so that recursive
        # build() calls (triggered by building the related entity's ORM class)
        # find this entry in the cache and return early, breaking circular refs.
        self._cache[domain_cls] = (orm_cls, mapper)
        SAModelRegistry._register(domain_cls, orm_cls)

        # Wire SA relationship() attributes now that both sides can be resolved.
        # Relationships use lambdas for lazy target-class lookup, so order of
        # registration does not matter — they are evaluated at mapper init time.
        if meta.relationships:
            self._build_relationships(orm_cls, meta)
        if meta.many_to_many:
            self._build_many_to_many(orm_cls, meta)

        if meta.customize is not None:
            # customize runs last so it can override anything the auto-builder
            # set (e.g. replace a generated relationship with a custom one).
            meta.customize(orm_cls)

        return orm_cls, mapper

    # ── PK column builders ────────────────────────────────────────────────────

    @staticmethod
    def _build_single_pk_column(meta: ParsedMeta) -> Column:
        """
        Build the ``id`` PK column from ``meta.pk_strategy``.

        INT_AUTO     → Integer, autoincrement=True
        UUID_AUTO    → PG_UUID, default=uuid4 (Python-side, no extra SELECT)
        STR_ASSIGNED → String, no default
        CUSTOM       → inferred from meta.pk_type via _SA_TYPE_MAP

        Raises:
            TypeError: CUSTOM strategy with an unsupported pk_type.
        """
        from uuid import uuid4

        strategy = meta.pk_strategy
        if strategy is PKStrategy.INT_AUTO:
            return Column("id", Integer(), primary_key=True, autoincrement=True)
        if strategy is PKStrategy.UUID_AUTO:
            return Column("id", PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
        if strategy is PKStrategy.STR_ASSIGNED:
            return Column("id", String(), primary_key=True)
        col_type = _sa_type(meta.pk_type, "pk", None)
        return Column("id", col_type, primary_key=True)

    @staticmethod
    def _build_composite_pk_columns(
        domain_cls: type,
        meta: ParsedMeta,
    ) -> tuple[dict[str, Column], list[str]]:
        """
        Build ``primary_key=True`` columns for each composite PK field.

        Returns:
            ``(columns_dict, pk_attr_names_in_declaration_order)``
        """
        raw_hints = typing.get_type_hints(domain_cls, include_extras=True)
        columns: dict[str, Column] = {}
        pk_attr_names: list[str] = []

        for field_name in meta.composite_pk_fields:
            ann = raw_hints.get(field_name)
            inner_type, _ = MetaReader.extract_inner_type(ann or Any)
            hint = meta.fields.get(field_name)
            col_type = _sa_type(
                inner_type, field_name, hint.max_length if hint else None
            )
            sa_fk_args = SAModelFactory._resolve_fk(field_name, meta)
            columns[field_name] = Column(
                field_name, col_type, *sa_fk_args, primary_key=True
            )
            pk_attr_names.append(field_name)

        return columns, pk_attr_names

    # ── Non-PK column builder ─────────────────────────────────────────────────

    @staticmethod
    def _build_non_pk_columns(domain_cls: type, meta: ParsedMeta) -> dict[str, Column]:
        """
        Build all business columns that are NOT part of the primary key.

        Applies FieldHint (nullable, unique, index, max_length) and
        ForeignKey hints from ParsedMeta.

        Raises:
            TypeError: Unsupported or unannotated field type.
        """
        raw_hints = typing.get_type_hints(domain_cls, include_extras=True)
        columns: dict[str, Column] = {}

        for field in MetaReader.domain_fields(domain_cls):
            if field.name in meta.composite_pk_fields:
                continue
            ann = raw_hints.get(field.name)
            if ann is None:
                raise TypeError(
                    f"Field {field.name!r} on {domain_cls.__name__!r} has no "
                    "type annotation. All domain fields must be annotated."
                )
            inner_type, is_optional = MetaReader.extract_inner_type(ann)
            hint = meta.fields.get(field.name)
            col_type = _sa_type(
                inner_type, field.name, hint.max_length if hint else None
            )
            nullable = hint.nullable if hint is not None else is_optional
            unique = hint.unique if hint else False
            index = hint.index if hint else False
            sa_fk_args = SAModelFactory._resolve_fk(field.name, meta)
            columns[field.name] = Column(
                field.name,
                col_type,
                *sa_fk_args,
                nullable=nullable,
                unique=unique,
                index=index,
            )

        return columns

    # ── FK and constraint helpers ─────────────────────────────────────────────

    @staticmethod
    def _resolve_fk(field_name: str, meta: ParsedMeta) -> list:
        """Build ``[sa.ForeignKey(...)]`` for a field, or ``[]`` if no FK."""
        fk_hint = meta.foreign_keys.get(field_name)
        if fk_hint is None:
            return []
        return [
            sa.ForeignKey(
                fk_hint.resolve(),
                ondelete=fk_hint.on_delete,
                onupdate=fk_hint.on_update,
            )
        ]

    @staticmethod
    def _build_table_args(meta: ParsedMeta) -> tuple:
        """Translate domain constraints to SA ``__table_args__`` entries."""
        args: list[Any] = []
        for c in meta.constraints:
            if isinstance(c, DomainUniqueConstraint):
                args.append(sa.UniqueConstraint(*c.fields, name=c.name))
            elif isinstance(c, DomainCheckConstraint):
                args.append(sa.CheckConstraint(c.condition, name=c.name))
        return tuple(args) if args else ()

    # ── Relationship builders ─────────────────────────────────────────────────

    def _build_relationships(self, orm_cls: type, meta: ParsedMeta) -> None:
        """
        Wire SQLAlchemy ``relationship()`` attributes for all one-to-many /
        many-to-one / one-to-one hints declared in ``Meta.relationships``.

        Each relationship is attached to ``orm_cls`` via ``setattr`` using a
        **lazy callable** for the target class — SA resolves the lambda during
        mapper configuration, not at ``relationship()`` call time.  This means
        the target ORM class does not need to exist yet when this method runs.

        If the target domain class has not been built yet, ``build()`` is called
        recursively.  Because ``build()`` caches the ORM class *before* calling
        this method, circular references are broken at the cache level.

        Args:
            orm_cls: The source ORM class that will gain the relationship attribute.
            meta:    Parsed metadata for the source domain class.

        Edge cases:
            - ``foreign_keys`` tuple is empty → SA infers the FK automatically
              (works when there is exactly one FK to the target table).
            - ``back_populates`` is ``None`` → one-way relationship; no
              back-reference attribute is set on the target.
            - Same relationship declared on both sides → wired independently;
              SA resolves the ``back_populates`` string at mapper init.

        Thread safety:  ⚠️ Call at startup only (same constraint as ``build()``).
        Async safety:   ✅ Synchronous; no I/O.
        """
        from sqlalchemy.orm import relationship

        for rel in meta.relationships:
            # Build the target ORM class if it hasn't been built yet.
            # The recursive call is safe because build() caches before
            # calling _build_relationships(), so any cycle terminates.
            target_orm_cls, _ = self.build(rel.target)

            # Capture target in a local variable to avoid late-binding in the lambda.
            # Without this, all lambdas in the loop would reference the LAST target_orm_cls.
            _target = target_orm_cls

            # Resolve foreign_keys to actual Column objects on the source ORM class.
            # SA accepts a list of Column objects or an empty list (auto-infer).
            fk_cols = (
                [getattr(orm_cls, fk) for fk in rel.foreign_keys]
                if rel.foreign_keys
                else []
            )

            # DESIGN: lambda form for lazy target resolution
            #   ✅ Defers class lookup to mapper init — both sides can be registered in any order
            #   ✅ Avoids "mapper not yet configured" errors during circular builds
            #   ❌ Slightly harder to debug — target is a closure, not a string
            sa_rel = relationship(
                lambda t=_target: t,
                foreign_keys=fk_cols or None,  # None → SA auto-infers from declared FKs
                back_populates=rel.back_populates,
                lazy=rel.lazy,
                uselist=rel.uselist,
            )
            setattr(orm_cls, rel.attr_name, sa_rel)

    def _build_many_to_many(self, orm_cls: type, meta: ParsedMeta) -> None:
        """
        Wire SQLAlchemy ``relationship()`` attributes for all many-to-many hints
        declared in ``Meta.many_to_many``, creating the association table if it
        does not already exist.

        Association tables are cached in ``self._assoc_tables`` keyed by table
        name.  If two domain classes declare the same ``through`` table name,
        only the first encountered creates the table; subsequent calls reuse it.

        The association table columns are resolved from the *source* and *target*
        ORM classes' primary key columns.  If ``source_fk`` / ``target_fk`` are
        not specified, the convention ``{tablename}_id`` is used.

        Args:
            orm_cls: The source ORM class that will gain the relationship attribute.
            meta:    Parsed metadata for the source domain class.

        Edge cases:
            - Two models declare the same ``through`` table from opposite sides →
              the first call registers the table; the second reuses it from cache.
            - ``back_populates`` is ``None`` → one-way M2M relationship.
            - ``source_fk`` / ``target_fk`` default to ``{tablename}_id`` if not
              specified — this matches the common Rails/Django naming convention.

        Raises:
            KeyError: If the source or target ORM class has no ``__tablename__``.

        Thread safety:  ⚠️ Call at startup only.
        Async safety:   ✅ Synchronous; no I/O.
        """
        from sqlalchemy.orm import relationship

        for m2m in meta.many_to_many:
            # Recursively build target if not yet registered.
            target_orm_cls, _ = self.build(m2m.target)

            # Resolve association table — create once, reuse on duplicate declarations.
            through = m2m.through
            if through not in self._assoc_tables:
                src_table = orm_cls.__tablename__  # type: ignore[attr-defined]
                tgt_table = target_orm_cls.__tablename__  # type: ignore[attr-defined]

                # Default FK column names follow the ``{tablename}_id`` convention.
                src_fk_col = m2m.source_fk or f"{src_table}_id"
                tgt_fk_col = m2m.target_fk or f"{tgt_table}_id"

                # Determine the SA column type from the source / target PK columns.
                # We look up the mapped table's PK to infer the correct type rather
                # than hard-coding Integer — domain PKs can be UUID or String.
                src_pk_col = list(sa.inspect(orm_cls).mapper.primary_key)[0]  # type: ignore[attr-defined]
                tgt_pk_col = list(sa.inspect(target_orm_cls).mapper.primary_key)[0]  # type: ignore[attr-defined]

                # DESIGN: assoc table FK column types mirror the actual PK types
                #   ✅ Handles UUID, String, and Integer PKs without hard-coding
                #   ✅ Type mismatch errors surfaced at table-creation time, not runtime
                #   ❌ Requires both ORM classes to have a mapped table (always true here)
                assoc_table = sa.Table(
                    through,
                    self._base.metadata,
                    sa.Column(
                        src_fk_col,
                        src_pk_col.type,
                        sa.ForeignKey(f"{src_table}.{src_pk_col.name}"),
                        primary_key=True,
                    ),
                    sa.Column(
                        tgt_fk_col,
                        tgt_pk_col.type,
                        sa.ForeignKey(f"{tgt_table}.{tgt_pk_col.name}"),
                        primary_key=True,
                    ),
                )
                self._assoc_tables[through] = assoc_table

            assoc_table = self._assoc_tables[through]

            # Capture loop variables to avoid late-binding closure issues.
            _target = target_orm_cls
            _assoc = assoc_table

            sa_rel = relationship(
                lambda t=_target: t,
                secondary=_assoc,
                back_populates=m2m.back_populates,
            )
            setattr(orm_cls, m2m.attr_name, sa_rel)
