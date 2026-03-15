"""
orm_abstraction.mapper
======================
Bidirectional translation between a DomainModel and a backend ORM object.

Supports both single and composite primary keys — the composite case sets
``entity.pk`` to a tuple of values in declaration order.

System fields (``init=False``)
-------------------------------
Fields declared with ``init=False`` on the domain class are treated as
*system-managed*:

- ``to_orm`` (INSERT): computes their values from context (``datetime.now``,
  migrator version, constant ``1``) rather than copying domain object values
  (which are all ``None`` / default at construction time).
- ``from_orm`` (SELECT): reads them from the ORM object via
  ``object.__setattr__``, bypassing the constructor.
- ``sync_to_orm`` (UPDATE): applies special logic per field name
  (``updated_at`` → now, ``row_version`` → old+1, ``created_at`` → skipped,
  ``definition_version`` → carry through migration result).

Known system field names:
    created_at         — set once on INSERT, never touched on UPDATE
    updated_at         — refreshed on every INSERT and UPDATE
    definition_version — set to migrator.current_version() on INSERT;
                         carries the post-migration value on UPDATE
    row_version        — starts at 1 on INSERT; checked and incremented
                         on UPDATE (optimistic locking)
"""

from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Generic, TypeVar

from .model import DomainModel

D = TypeVar("D", bound=DomainModel)
O = TypeVar("O")  # noqa: E741

# System field names that receive special treatment in to_orm / sync_to_orm.
_SYSTEM_FIELDS = frozenset(
    {"created_at", "updated_at", "definition_version", "row_version"}
)


class AbstractMapper(ABC, Generic[D, O]):
    """
    Bidirectional mapper between a ``DomainModel`` subclass and an ORM class.

    Single vs composite PK
    ----------------------
    Subclasses implement ``_pk_orm_attrs`` (note the plural) — a list of ORM
    attribute names that form the primary key.  For single PKs the list has
    one element; for composite PKs it has two or more.

    The domain object's ``pk`` field reflects this:
    - Single PK  → ``entity.pk = scalar_value``
    - Composite  → ``entity.pk = (val1, val2, ...)`` in declaration order

    System fields vs business fields
    ---------------------------------
    The mapper inspects each dataclass field's ``init`` flag:

    - ``init=True``  → *business field*: passed as constructor kwargs in
      ``from_orm``; copied verbatim in ``to_orm`` and ``sync_to_orm``.
    - ``init=False`` → *system field*: set via ``object.__setattr__`` in
      ``from_orm``; computed in ``to_orm`` and ``sync_to_orm``.

    Migrator
    --------
    When ``migrator`` is provided, ``from_orm`` checks
    ``definition_version`` on the loaded ORM object and runs the migration
    chain if the stored version is behind ``migrator.current_version()``.
    The caller always receives a fully-upgraded domain object.  The DB row
    is updated lazily on the next ``save()`` call.

    DESIGN: list of pk attrs instead of single string
      ✅ Composite PK support with zero changes to from_orm / to_orm callers
      ✅ find_by_id(pk) passes a scalar or a tuple — SQLAlchemy session.get()
         accepts both forms transparently
      ❌ Slightly more complex _pk_orm_attrs property compared to the old single
         string — worth it for the unified interface

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ All methods are sync and allocation-only.
    """

    def __init__(
        self,
        domain_cls: type[D],
        orm_cls: type[O],
        migrator: Any = None,
    ) -> None:
        self._domain_cls = domain_cls
        self._orm_cls = orm_cls
        # Normalise Meta.migrator to a DomainMigrator instance.  Three forms
        # are accepted:
        #   class    → UserMigrator          (called with no args)
        #   instance → UserMigrator("IT")    (used as-is)
        #   callable → lambda: UserMigrator("IT")  or  a factory function
        #              (called with no args; useful for DI containers)
        from .migrator import DomainMigrator

        if migrator is None or isinstance(migrator, DomainMigrator):
            self._migrator = migrator
        else:
            self._migrator = migrator()

        # Split fields into business (init=True) and system (init=False).
        # Excludes ``pk`` and ``_``-prefixed bookkeeping fields.
        all_fields = [
            f
            for f in dataclasses.fields(domain_cls)
            if f.name != "pk" and not f.name.startswith("_")
        ]
        self._init_fields: list[str] = [f.name for f in all_fields if f.init]
        self._post_init_fields: list[str] = [f.name for f in all_fields if not f.init]
        # Kept for any external code that iterated _domain_fields directly.
        self._domain_fields: list[str] = self._init_fields + self._post_init_fields

    @property
    @abstractmethod
    def _pk_orm_attrs(self) -> list[str]:
        """
        ORM attribute names that form the primary key, in declaration order.

        Single PK  → ``["id"]``
        Composite  → ``["user_id", "role_id"]``
        """

    # ── Core translation ──────────────────────────────────────────────────────

    def to_orm(self, domain: D) -> O:
        """
        Translate a ``DomainModel`` into a new ORM object (INSERT path).

        Business fields (``init=True``) are copied verbatim from the domain
        object.  System fields (``init=False``) receive computed values:

        - ``created_at`` / ``updated_at`` → ``datetime.now(UTC)``
        - ``row_version``        → ``1``
        - ``definition_version`` → ``migrator.current_version()`` or ``1``

        For single PKs, the PK column is set only when ``domain.pk`` is not
        ``None`` (app-assigned strategies).  For composite PKs the values are
        always embedded in the business fields and copied normally.

        Args:
            domain: Domain entity to translate.

        Returns:
            A new ORM object with all fields set.
        """
        now = datetime.now(tz=timezone.utc)

        kwargs: dict[str, Any] = {
            name: getattr(domain, name) for name in self._init_fields
        }

        for name in self._post_init_fields:
            if name in ("created_at", "updated_at"):
                kwargs[name] = now
            elif name == "row_version":
                kwargs[name] = 1
            elif name == "definition_version":
                kwargs[name] = self._migrator.current_version() if self._migrator else 1
            else:
                kwargs[name] = getattr(domain, name)

        # Single PK only: inject pk value when app-assigned
        if len(self._pk_orm_attrs) == 1 and domain.pk is not None:
            kwargs[self._pk_orm_attrs[0]] = domain.pk

        return self._orm_cls(**kwargs)

    def from_orm(self, orm_obj: O) -> D:
        """
        Translate an ORM object into a ``DomainModel`` (SELECT / after INSERT).

        Business fields are passed to the constructor; system fields are
        applied via ``object.__setattr__``.  If a migrator is registered and
        the stored ``definition_version`` is behind ``current_version``, the
        migration chain runs before the domain object is returned.

        Sets ``pk`` and ``_raw_orm`` via ``object.__setattr__`` — works on
        both regular and ``frozen=True`` dataclasses.

        Args:
            orm_obj: Loaded or freshly inserted ORM object.

        Returns:
            ``DomainModel`` with all fields, ``pk``, and ``_raw_orm`` set.
            Always at the latest schema version when a migrator is registered.

        Edge cases:
            - Single PK  → ``entity.pk = scalar``
            - Composite  → ``entity.pk = (val1, val2, ...)``
            - ``definition_version`` already at current → no migration runs.
        """
        # ── Business fields → constructor ─────────────────────────────────────
        kwargs: dict[str, Any] = {
            name: getattr(orm_obj, name) for name in self._init_fields
        }
        instance = self._domain_cls(**kwargs)

        # ── System fields → object.__setattr__ ───────────────────────────────
        for name in self._post_init_fields:
            object.__setattr__(instance, name, getattr(orm_obj, name, None))

        # ── Migration chain ───────────────────────────────────────────────────
        if self._migrator is not None:
            stored_v = getattr(instance, "definition_version", None)
            if stored_v is not None and stored_v < self._migrator.current_version():
                data: dict[str, Any] = {
                    name: getattr(instance, name) for name in self._domain_fields
                }
                migrated = self._migrator.migrate(data, stored_v)
                for name in self._domain_fields:
                    if name in migrated:
                        object.__setattr__(instance, name, migrated[name])

        # ── PK ────────────────────────────────────────────────────────────────
        if len(self._pk_orm_attrs) == 1:
            pk_val = getattr(orm_obj, self._pk_orm_attrs[0])
        else:
            pk_val = tuple(getattr(orm_obj, attr) for attr in self._pk_orm_attrs)

        object.__setattr__(instance, "pk", pk_val)
        object.__setattr__(instance, "_raw_orm", orm_obj)
        return instance

    def sync_to_orm(self, domain: D, orm_obj: O) -> O:
        """
        Apply domain field values onto an existing ORM object in-place (UPDATE).

        Mutates the session-tracked ORM object so the identity map and dirty
        tracking remain intact.

        Optimistic locking
        ------------------
        When ``row_version`` is a system field, the stored value on ``orm_obj``
        must match ``domain.row_version``.  A mismatch means another process
        committed a write between this entity's load and the current save —
        ``StaleEntityError`` is raised and the update is not applied.

        System field behaviour on UPDATE:
        - ``created_at``         → **never touched**
        - ``updated_at``         → set to ``datetime.now(UTC)``
        - ``row_version``        → incremented by 1
        - ``definition_version`` → written as-is (carries migration result)

        Args:
            domain:  Domain entity with updated values.
            orm_obj: Existing, session-tracked ORM object.

        Returns:
            The same ``orm_obj``, mutated in-place.

        Raises:
            StaleEntityError: ``row_version`` conflict detected.
        """
        # ── Optimistic lock check ─────────────────────────────────────────────
        if "row_version" in self._post_init_fields:
            stored_rv = getattr(orm_obj, "row_version", None)
            if stored_rv is not None and stored_rv != domain.row_version:
                from .exception.repository import StaleEntityError

                raise StaleEntityError(
                    entity_cls=type(domain),
                    expected_version=domain.row_version,
                    actual_version=stored_rv,
                )

        # ── Business fields ───────────────────────────────────────────────────
        for name in self._init_fields:
            setattr(orm_obj, name, getattr(domain, name))

        # ── System fields ─────────────────────────────────────────────────────
        now = datetime.now(tz=timezone.utc)
        for name in self._post_init_fields:
            if name == "created_at":
                pass  # immutable after INSERT
            elif name == "updated_at":
                setattr(orm_obj, name, now)
            elif name == "row_version":
                setattr(orm_obj, name, domain.row_version + 1)
            else:
                # definition_version and any future system fields
                setattr(orm_obj, name, getattr(domain, name))

        return orm_obj
