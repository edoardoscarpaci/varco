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
from typing import Any, TypeVar

OT = TypeVar("OT")  # ORM type — only used in cast_raw


@dataclass
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
