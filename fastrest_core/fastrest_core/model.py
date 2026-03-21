"""
orm_abstraction.model
=====================
Pure-Python domain entity base class and typed escape hatch.

The user defines their entity **once** as a plain ``@dataclass`` subclass.
No ORM class, no mapper, no session — the backend generates all of that
automatically from the field types and the inner ``Meta`` class.

Usage::

    from dataclasses import dataclass
    from typing import Annotated
    from uuid import UUID
    from orm_abstraction import DomainModel, register
    from orm_abstraction.meta import FieldHint, PrimaryKey, PKStrategy, pk_field

    @register
    @dataclass
    class User(DomainModel):
        pk:    Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        name:  Annotated[str,  FieldHint(max_length=120)]
        email: Annotated[str,  FieldHint(unique=True, nullable=False)]

        class Meta:
            table = "users"
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Annotated, Any, TypeVar

from fastrest_core.meta import FieldHint

OT = TypeVar("OT")  # ORM type — only used in cast_raw


@dataclass(kw_only=True)
class DomainModel:
    """
    Base class for all domain entities.

    Subclass with ``@dataclass`` and declare only business fields.
    ``pk`` and ``_raw_orm`` are managed exclusively by the translation
    layer — never set them directly (except for ``STR_ASSIGNED`` / ``CUSTOM``
    strategies using ``pk_field(init=True)``).

    Primary key field
    -----------------
    Override ``pk`` on the subclass via ``pk_field()`` to declare a specific
    PK type and generation strategy::

        pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
        pk: Annotated[str,  PrimaryKey(PKStrategy.STR_ASSIGNED)] = pk_field(init=True)

    If ``pk`` is not overridden the factory defaults to ``int`` + ``INT_AUTO``
    (auto-increment integer).

    Persistence detection
    ---------------------
    ``is_persisted()`` returns ``True`` only after the entity has passed
    through a repository operation (``save()``, ``find_by_id()``, etc.).
    It is based on ``_raw_orm``, not on ``pk``, so a freshly constructed
    ``STR_ASSIGNED`` entity with ``pk`` already set is correctly treated as
    unpersisted until ``save()`` is called.

    DESIGN: ``init=False`` on ``pk`` and ``_raw_orm``
      ✅ No argument-ordering conflict — subclass non-default fields slot
         cleanly above the base-class defaults in dataclass inheritance
      ✅ Constructor stays clean for auto-generated PKs
      ✅ ``object.__setattr__`` sets both fields post-construction; works on
         ``frozen=True`` dataclasses too
      ✅ ``pk_field(init=True)`` opts STR_ASSIGNED / CUSTOM into the constructor
         without any base-class changes

    Thread safety:  ❌ Not thread-safe — mutate from one task only.
    Async safety:   ✅ Safe to pass across ``await`` boundaries once built.

    Edge cases:
        - ``raw()`` raises ``RuntimeError`` on a freshly constructed entity
          (``_raw_orm is None``) — prevents silent None bugs downstream.
        - ``is_persisted()`` returns ``False`` for a new ``STR_ASSIGNED``
          entity even when ``pk`` is already set.
    """

    # Primary key — populated by the translation layer after INSERT / SELECT.
    # ``Any`` because type varies: int (INT_AUTO), UUID (UUID_AUTO), str
    # (STR_ASSIGNED), or any custom type.
    pk: Any = field(default=None, init=False, repr=True, compare=True)

    # Backing ORM object — set by the mapper after every repository operation.
    # ``None`` means the entity has never been loaded from or saved to the DB.
    # ``compare=False`` — two domain objects with identical field values are
    # equal regardless of whether they carry a backing ORM reference.
    _raw_orm: Any = field(default=None, init=False, repr=False, compare=False)

    # ── Public API ────────────────────────────────────────────────────────────

    def raw(self) -> Any:
        """
        Return the backing ORM object (escape hatch).

        Prefer ``cast_raw(entity, OrmType)`` for type-checker support.

        Returns:
            The raw SA ORM instance or Beanie Document.

        Raises:
            RuntimeError: Entity has not yet passed through a repository.

        Edge cases:
            - Valid only after a repository operation (``find_by_id``,
              ``find_all``, ``save``).
            - Always valid on the object *returned* by ``repo.save()``.
        """
        if self._raw_orm is None:
            raise RuntimeError(
                f"{type(self).__name__}.raw() called before entity was loaded "
                "from or saved to the database. raw() is only available after "
                "a repository operation (find_by_id, find_all, save)."
            )
        return self._raw_orm

    def is_persisted(self) -> bool:
        """
        Return ``True`` if this entity has been saved to or loaded from the DB.

        Based on ``_raw_orm``, not on ``pk`` — correctly handles
        ``STR_ASSIGNED`` entities that have ``pk`` set at construction time
        but have not yet been inserted into the database.

        Returns:
            ``True`` after any repository operation; ``False`` for freshly
            constructed entities that have not passed through ``save()`` or
            ``find_by_id()`` yet.
        """
        return self._raw_orm is not None


# ── Typed escape hatch ────────────────────────────────────────────────────────


def cast_raw(entity: DomainModel, orm_type: type[OT]) -> OT:
    """
    Type-safe cast of ``entity.raw()`` to a concrete ORM type.

    Use this instead of bare ``entity.raw()`` when the type checker should
    know the exact ORM class — e.g. to access SA relationships, Beanie
    ``fetch_link()``, or any other backend-specific API.

    Args:
        entity:   Any persisted ``DomainModel`` instance.
        orm_type: The expected ORM class (auto-generated; retrieve via
                  ``SAModelRegistry.get(MyClass)``).

    Returns:
        The raw ORM object typed as ``orm_type``.

    Raises:
        RuntimeError: Entity has no backing ORM object (not yet persisted).
        TypeError:    Backing object is not an instance of ``orm_type``.

    Example::

        from orm_abstraction.sqlalchemy.factory import SAModelRegistry

        user = await repo.find_by_id(some_uuid)
        UserORM = SAModelRegistry.get(User)
        sa_user = cast_raw(user, UserORM)
        await session.refresh(sa_user, ["posts"])
    """
    raw = entity.raw()
    if not isinstance(raw, orm_type):
        raise TypeError(
            f"Expected raw ORM type {orm_type.__name__!r}, "
            f"got {type(raw).__name__!r}. "
            "Check that the correct backend is active."
        )
    return raw  # type: ignore[return-value]


# ── Audited base ───────────────────────────────────────────────────────────────


@dataclass(kw_only=True)
class AuditedDomainModel(DomainModel):
    """
    ``DomainModel`` extension that adds audit timestamps.

    Fields
    ------
    created_at
        Set once by the mapper's ``to_orm`` (INSERT path) to
        ``datetime.now(UTC)``.  Never overwritten on updates.
    updated_at
        Set by ``to_orm`` on INSERT and refreshed by ``sync_to_orm`` on
        every UPDATE.

    Both fields have ``init=False`` — the mapper manages them exclusively
    via ``object.__setattr__``.  They are ``None`` on a freshly constructed
    entity and populated after the first repository operation.

    ``compare=False`` — equality is based on business fields only.
    """

    created_at: datetime | None = field(
        default=None, init=False, repr=True, compare=False
    )
    updated_at: datetime | None = field(
        default=None, init=False, repr=True, compare=False
    )


# ── Versioned base ─────────────────────────────────────────────────────────────


@dataclass(kw_only=True)
class VersionedDomainModel(AuditedDomainModel):
    """
    ``AuditedDomainModel`` extension that adds schema versioning and
    optimistic locking.

    Fields
    ------
    definition_version
        Schema version of the stored data.  Set to
        ``migrator.current_version()`` on INSERT; read back on SELECT and
        used by the mapper to trigger the migration chain when stale.
        Defaults to ``1`` (no migrations run yet).

    row_version
        Optimistic lock counter.  Set to ``1`` on INSERT; incremented by the
        mapper on every UPDATE.  ``sync_to_orm`` raises
        ``StaleEntityError`` when the stored value does not match the domain
        object's value, signalling a concurrent modification.
        Defaults to ``0`` (not yet persisted).

    Both fields have ``init=False`` and ``compare=False``.

    Registering a migrator
    ----------------------
    Attach a ``DomainMigrator`` subclass to ``Meta.migrator``::

        from fastrest_core.migrator import DomainMigrator

        def add_slug(data: dict) -> dict:
            data["slug"] = data["name"].lower().replace(" ", "-")
            return data

        class ProductMigrator(DomainMigrator):
            steps = [add_slug]

        @register
        @dataclass
        class Product(VersionedDomainModel):
            name: str
            slug: str

            class Meta:
                table    = "products"
                migrator = ProductMigrator
    """

    definition_version: int = field(default=1, init=False, repr=False, compare=False)
    row_version: int = field(default=0, init=False, repr=False, compare=False)


# ── Tenant-aware bases ─────────────────────────────────────────────────────────


@dataclass(kw_only=True)
class TenantMixin:
    """
    Single-field dataclass mixin that contributes ``tenant_id`` to any
    domain model hierarchy without repeating the field declaration.

    Designed to be combined with ``DomainModel``, ``AuditedDomainModel``,
    or ``VersionedDomainModel`` via multiple inheritance::

        class TenantDomainModel(TenantMixin, DomainModel): ...
        class TenantAuditedDomainModel(TenantMixin, AuditedDomainModel): ...
        class TenantVersionedDomainModel(TenantMixin, VersionedDomainModel): ...

    Fields
    ------
    tenant_id
        Tenant discriminator.  Defaults to ``""`` so that subclass
        constructors work without requiring a tenant-ID argument — the
        ``TenantAwareService.create()`` stamps the real value from
        ``ctx.metadata["tenant_id"]`` via ``dataclasses.replace`` before
        the entity is persisted.

        The ``FieldHint(index=True, nullable=False)`` annotation tells
        both the SA and Beanie backends to:
        - Create a secondary index — every list/get/update/delete query
          filters by this column, so an index is essential at scale.
        - Disallow NULL / missing values — a row without a tenant is a
          data-integrity violation.

    ``init=True`` is intentional: ``dataclasses.replace(entity, tenant_id=tid)``
    requires the field to be an init parameter.  Contrast with ``created_at``
    / ``updated_at`` on ``AuditedDomainModel`` which are mapper-managed and
    therefore ``init=False``.

    ``compare=True`` (the dataclass default) — two entities with the same
    business fields but different tenant IDs are genuinely distinct rows
    and must not compare equal.

    DESIGN: ``default=""`` instead of ``default=None``
        ✅ ``tenant_id: str`` is non-nullable — ``None`` would violate
           both the type annotation and the DB constraint.
        ✅ ``TenantAwareService.create()`` always overwrites the empty
           string immediately after assembly; it is never persisted.
        ❌ An empty string can slip through if the service layer is
           bypassed (e.g. direct repository access).  This is acceptable
           — direct repo access is an explicit escape hatch that bypasses
           all guards by design.

    DESIGN: mixin as a separate ``@dataclass``, not a plain class
        Python's dataclass machinery collects fields by walking the MRO
        from most-base to most-derived.  Marking the mixin with
        ``@dataclass`` registers ``tenant_id`` so the decorator on each
        combined class (e.g. ``TenantDomainModel``) picks it up
        automatically.

        Alternative considered: inherit directly (e.g.
        ``TenantAuditedDomainModel(AuditedDomainModel)`` with a
        repeated field) — rejected because it duplicates the field
        declaration once per base and makes future changes error-prone.

    DESIGN: ``TenantMixin`` listed FIRST in the MRO
        ``class TenantXxx(TenantMixin, BaseXxx)`` places TenantMixin
        between the concrete class and the base in the MRO.  Python
        collects dataclass fields base-first, so ``tenant_id`` is
        appended after all base fields.  This guarantees:
        - ``pk`` / ``_raw_orm`` from ``DomainModel`` come first (both
          ``init=False`` — not in ``__init__``)
        - ``created_at`` / ``updated_at`` from ``AuditedDomainModel``
          next (``init=False``)
        - ``definition_version`` / ``row_version`` from
          ``VersionedDomainModel`` next (``init=False``)
        - ``tenant_id`` last — the only ``init=True`` field; a trailing
          default argument never causes an ordering TypeError.

    Thread safety:  ❌ No shared state — thread safety depends on the
                    concrete subclass (inherits DomainModel contract).
    Async safety:   ✅ Safe to pass across await boundaries once built.

    Edge cases:
        - ``tenant_id = ""`` after construction — overwritten by
          ``TenantAwareService.create()`` before the first ``save()``.
        - ``dataclasses.replace(entity, tenant_id=tid)`` works because
          ``init=True``.
        - Services that override ``_tenant_field`` do NOT need these
          bases — they can declare the field themselves with any name.
        - Do NOT call ``super().__init__()`` from combined-class
          ``__init__`` — the generated ``__init__`` handles all fields.
    """

    # Indexed non-nullable discriminator column.
    # FieldHint(index=True) → SA creates Index("ix_<table>_tenant_id", col)
    #                        → Beanie registers a secondary index on the field.
    # default="" — never persisted as-is; TenantAwareService.create() stamps
    # the real value from ctx.metadata["tenant_id"] before save().
    tenant_id: Annotated[str, FieldHint(index=True, nullable=False)] = field(
        default="", init=True, repr=True, compare=True
    )


@dataclass(kw_only=True)
class TenantDomainModel(TenantMixin, DomainModel):
    """
    ``DomainModel`` + ``TenantMixin``.

    Row-level multi-tenancy for entities that do not need audit timestamps.
    For the full ``tenant_id`` contract see ``TenantMixin``.

    Inheritance chain::

        DomainModel  (pk, _raw_orm)
        TenantMixin  (tenant_id)
        └── TenantDomainModel

    Usage::

        @register
        @dataclass
        class Widget(TenantDomainModel):
            pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
            name: str

            class Meta:
                table = "widgets"

    Thread safety:  ❌ Inherits DomainModel — mutate from one task only.
    Async safety:   ✅ Safe to pass across await boundaries once built.
    """


@dataclass(kw_only=True)
class TenantAuditedDomainModel(TenantMixin, AuditedDomainModel):
    """
    ``AuditedDomainModel`` + ``TenantMixin``.

    Row-level multi-tenancy with ``created_at`` / ``updated_at`` audit
    timestamps.  This is the recommended base for most tenant-scoped
    entities.  For the full ``tenant_id`` contract see ``TenantMixin``.

    Inheritance chain::

        DomainModel        (pk, _raw_orm)
        AuditedDomainModel (created_at, updated_at)
        TenantMixin        (tenant_id)
        └── TenantAuditedDomainModel

    Usage::

        @register
        @dataclass
        class Post(TenantAuditedDomainModel):
            pk: Annotated[int, PrimaryKey(PKStrategy.INT_AUTO)] = pk_field()
            title: str

            class Meta:
                table = "posts"

    Thread safety:  ❌ Inherits DomainModel — mutate from one task only.
    Async safety:   ✅ Safe to pass across await boundaries once built.
    """


@dataclass(kw_only=True)
class TenantVersionedDomainModel(TenantMixin, VersionedDomainModel):
    """
    ``VersionedDomainModel`` + ``TenantMixin``.

    Row-level multi-tenancy with audit timestamps **and** optimistic
    locking / schema versioning.  Use this base when entities must
    survive concurrent updates without silent overwrites and may need
    forward migrations.  For the full ``tenant_id`` contract see
    ``TenantMixin``; for the versioning contract see
    ``VersionedDomainModel``.

    Inheritance chain::

        DomainModel           (pk, _raw_orm)
        AuditedDomainModel    (created_at, updated_at)
        VersionedDomainModel  (definition_version, row_version)
        TenantMixin           (tenant_id)
        └── TenantVersionedDomainModel

    Usage::

        @register
        @dataclass
        class Document(TenantVersionedDomainModel):
            pk: Annotated[UUID, PrimaryKey(PKStrategy.UUID_AUTO)] = pk_field()
            body: str

            class Meta:
                table    = "documents"
                migrator = DocumentMigrator  # optional

    Thread safety:  ❌ Inherits DomainModel — mutate from one task only.
    Async safety:   ✅ Safe to pass across await boundaries once built.

    Edge cases:
        - ``row_version`` starts at ``0`` (not persisted) and is set to
          ``1`` on first INSERT by the mapper — same as
          ``VersionedDomainModel``.
        - ``tenant_id`` is stamped by ``TenantAwareService.create()``
          before the first save; ``row_version`` is stamped by the
          mapper after — the two are independent and do not interact.
    """
