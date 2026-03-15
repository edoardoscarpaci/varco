"""
fastrest_beanie.factory
========================
Runtime Beanie Document class generator.

Reads ``ParsedMeta`` from ``fastrest_core`` and produces a Beanie
``Document`` subclass with the correct field types, indexes, compound
indexes, and a post-build ``Meta.customize`` hook.

Notes on composite PKs in MongoDB
----------------------------------
MongoDB does not support composite ``_id`` fields.  When composite PK is
declared the factory generates a standard auto-id and stores the composite
PK fields as regular indexed fields with a compound unique index.
The domain object's ``pk`` attribute is a tuple; ``find_by_id(pk_tuple)``
issues a ``find_one`` query on those fields.

Notes on CheckConstraint
-------------------------
MongoDB has no SQL CHECK constraints.  Any ``CheckConstraint`` in
``Meta.constraints`` emits a ``warnings.warn`` at startup and is ignored.
"""

from __future__ import annotations

import dataclasses
import typing
import warnings
from typing import Any, ClassVar, Optional, TypeVar

from beanie import Document, Indexed
from pydantic import Field as PydanticField

from fastrest_core.mapper import AbstractMapper
from fastrest_core.meta import (
    CheckConstraint as DomainCheckConstraint,
    MetaReader,
    ParsedMeta,
    PKStrategy,
    UniqueConstraint as DomainUniqueConstraint,
)
from fastrest_core.model import DomainModel

D = TypeVar("D", bound=DomainModel)


# ── Auto-generated mapper ─────────────────────────────────────────────────────


class _BeanieAutoMapper(AbstractMapper[D, Any]):
    """
    Mapper for auto-generated Beanie Documents.

    Single PK  → ``_pk_orm_attrs = ["id"]``
    Composite  → ``_pk_orm_attrs = [field1, field2, ...]``

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

    @property
    def _pk_orm_attrs(self) -> list[str]:
        return self._pk_attrs


# ── Registry ──────────────────────────────────────────────────────────────────


class BeanieDocRegistry:
    """
    Process-level registry mapping ``DomainModel`` subclasses to their
    auto-generated Beanie Document classes.

    ``BeanieDocRegistry.get(User)`` is the escape hatch — retrieve the
    generated Document class for ``cast_raw()`` or Pydantic customisation.

    Thread safety:  ⚠️ Write at startup only; reads are safe after that.
    """

    _registry: dict[type, type] = {}

    @classmethod
    def get(cls, domain_cls: type[D]) -> type:
        """Return the auto-generated Document class for ``domain_cls``."""
        try:
            return cls._registry[domain_cls]
        except KeyError:
            raise KeyError(
                f"No Beanie Document generated for {domain_cls.__name__!r}. "
                "Call provider.register(YourClass) first."
            ) from None

    @classmethod
    def all_documents(cls) -> list[type]:
        """All registered Document classes — pass to ``init_beanie()``."""
        return list(cls._registry.values())

    @classmethod
    def _register(cls, domain_cls: type, doc_cls: type) -> None:
        cls._registry[domain_cls] = doc_cls


# ── Main factory ──────────────────────────────────────────────────────────────


class BeanieModelFactory:
    """
    Generates a Beanie ``Document`` subclass + companion mapper from a
    ``DomainModel`` subclass at runtime.

    Thread safety:  ⚠️ Call ``build()`` at startup before concurrent use.
    Async safety:   ✅ ``build()`` is synchronous.

    Edge cases:
        - ``build()`` is idempotent.
        - ``CheckConstraint`` entries emit a warning and are ignored.
        - ``Meta.customize`` is called post-``type()`` with the Document class.
    """

    def __init__(self) -> None:
        self._cache: dict[type, tuple[type, AbstractMapper]] = {}

    def build(self, domain_cls: type[D]) -> tuple[type, _BeanieAutoMapper[D, Any]]:
        if domain_cls in self._cache:
            return self._cache[domain_cls]  # type: ignore[return-value]

        meta = MetaReader.read(domain_cls)

        for c in meta.constraints:
            if isinstance(c, DomainCheckConstraint):
                warnings.warn(
                    f"{domain_cls.__name__}: CheckConstraint({c.condition!r}) "
                    "is not supported on MongoDB and will be ignored.",
                    stacklevel=2,
                )

        annotations, field_defaults = self._build_field_specs(domain_cls, meta)
        settings_cls = self._build_settings(meta)

        doc_attrs: dict[str, Any] = {
            # Pydantic v2 requires __module__ in the namespace when a model
            # class is created dynamically via type() — without it, Pydantic's
            # _model_construction.inspect_namespace raises KeyError: '__module__'.
            "__module__": domain_cls.__module__,
            # Pydantic v2 treats every unannotated attribute as a field and
            # raises PydanticUserError for non-annotated attributes.  Beanie's
            # inner Settings class must be declared ClassVar so Pydantic skips
            # field inference for it.  This only matters when the class is
            # constructed dynamically via type() — in normal class bodies Python
            # detects inner classes implicitly.
            "__annotations__": {**annotations, "Settings": ClassVar[type]},
            "Settings": settings_cls,
            **field_defaults,
        }

        doc_cls = type(f"{domain_cls.__name__}Doc", (Document,), doc_attrs)

        if meta.customize is not None:
            meta.customize(doc_cls)

        pk_attrs = meta.composite_pk_fields if meta.is_composite_pk else ["id"]
        mapper = _BeanieAutoMapper(
            domain_cls=domain_cls,
            orm_cls=doc_cls,
            pk_orm_attrs=pk_attrs,
            migrator=meta.migrator,
        )
        self._cache[domain_cls] = (doc_cls, mapper)
        BeanieDocRegistry._register(domain_cls, doc_cls)
        return doc_cls, mapper

    # ── Settings ──────────────────────────────────────────────────────────────

    @staticmethod
    def _build_settings(meta: ParsedMeta) -> type:
        """Build Beanie Settings with collection name + compound indexes."""
        from pymongo import ASCENDING, IndexModel

        compound_indexes: list[IndexModel] = []
        for c in meta.constraints:
            if isinstance(c, DomainUniqueConstraint):
                compound_indexes.append(
                    IndexModel(
                        [(f, ASCENDING) for f in c.fields],
                        unique=True,
                        name=c.name,
                    )
                )

        attrs: dict[str, Any] = {"name": meta.table}
        if compound_indexes:
            attrs["indexes"] = compound_indexes
        return type("Settings", (), attrs)

    # ── Field specs ───────────────────────────────────────────────────────────

    @staticmethod
    def _build_field_specs(
        domain_cls: type,
        meta: ParsedMeta,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Build ``__annotations__`` and field defaults for the Beanie Document.

        Single PK: injects the correct ``id`` annotation per strategy.
        Composite PK: composite fields become regular indexed fields.

        Beanie receives bare inner types (``Annotated`` stripped) because
        Pydantic would misinterpret ``FieldHint`` as a field constraint.
        """
        raw_hints = typing.get_type_hints(domain_cls, include_extras=True)
        annotations: dict[str, Any] = {}
        field_defaults: dict[str, Any] = {}

        if not meta.is_composite_pk:
            id_ann, id_default = BeanieModelFactory._build_id_spec(meta)
            annotations["id"] = id_ann
            if id_default is not dataclasses.MISSING:
                field_defaults["id"] = id_default

        for dc_field in MetaReader.domain_fields(domain_cls):
            raw_ann = raw_hints.get(dc_field.name, Any)
            inner_type, _ = MetaReader.extract_inner_type(raw_ann)
            hint = meta.fields.get(dc_field.name)
            is_composite_pk_col = dc_field.name in meta.composite_pk_fields

            if is_composite_pk_col or (hint and hint.unique):
                ann_type = Indexed(inner_type, unique=True)
            elif hint and hint.index:
                ann_type = Indexed(inner_type)
            else:
                ann_type = raw_ann

            annotations[dc_field.name] = ann_type

            if (
                dc_field.default is not dataclasses.MISSING
                and dc_field.default is not None
            ):
                field_defaults[dc_field.name] = PydanticField(default=dc_field.default)

        return annotations, field_defaults

    @staticmethod
    def _build_id_spec(meta: ParsedMeta) -> tuple[Any, Any]:
        """
        Return ``(id_annotation, default_or_MISSING)`` per PK strategy.

        INT_AUTO     → Optional[int], default=None  (unusual for Mongo)
        UUID_AUTO    → UUID, default_factory=uuid4
        STR_ASSIGNED → str, no default
        CUSTOM       → meta.pk_type, no default
        """
        from uuid import UUID, uuid4

        strategy = meta.pk_strategy
        if strategy is PKStrategy.INT_AUTO:
            return Optional[int], PydanticField(default=None)
        if strategy is PKStrategy.UUID_AUTO:
            return UUID, PydanticField(default_factory=uuid4)
        if strategy is PKStrategy.STR_ASSIGNED:
            return str, dataclasses.MISSING
        return meta.pk_type, dataclasses.MISSING
