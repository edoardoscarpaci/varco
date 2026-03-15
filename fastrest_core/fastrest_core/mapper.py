"""
orm_abstraction.mapper
======================
Bidirectional translation between a DomainModel and a backend ORM object.

Supports both single and composite primary keys — the composite case sets
``entity.pk`` to a tuple of values in declaration order.
"""

from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from .model import DomainModel

D = TypeVar("D", bound=DomainModel)
O = TypeVar("O")  # noqa: E741


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

    DESIGN: list of pk attrs instead of single string
      ✅ Composite PK support with zero changes to from_orm / to_orm callers
      ✅ find_by_id(pk) passes a scalar or a tuple — SQLAlchemy session.get()
         accepts both forms transparently
      ❌ Slightly more complex _pk_orm_attrs property compared to the old single
         string — worth it for the unified interface

    Thread safety:  ✅ Stateless after construction.
    Async safety:   ✅ All methods are sync and allocation-only.
    """

    def __init__(self, domain_cls: type[D], orm_cls: type[O]) -> None:
        self._domain_cls = domain_cls
        self._orm_cls = orm_cls
        # Cache business field names once — avoids repeated dataclass reflection
        self._domain_fields: list[str] = [
            f.name
            for f in dataclasses.fields(domain_cls)
            if f.name != "pk" and not f.name.startswith("_")
        ]

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

        For single PKs, the PK column is set only when ``domain.pk`` is not
        ``None`` (app-assigned strategies).  For composite PKs the values are
        always embedded in the business fields and copied normally.

        Args:
            domain: Domain entity to translate.

        Returns:
            A new ORM object with all fields set.

        Edge cases:
            - Single PK, ``domain.pk is None`` → PK not set; DB generates it.
            - Single PK, ``domain.pk is not None`` → PK explicitly set.
            - Composite PK → PK values live in business fields, always copied.
        """
        kwargs: dict[str, Any] = {
            name: getattr(domain, name) for name in self._domain_fields
        }
        # Single PK only: inject pk value when app-assigned
        if len(self._pk_orm_attrs) == 1 and domain.pk is not None:
            kwargs[self._pk_orm_attrs[0]] = domain.pk
        return self._orm_cls(**kwargs)

    def from_orm(self, orm_obj: O) -> D:
        """
        Translate an ORM object into a ``DomainModel`` (SELECT / after INSERT).

        Sets ``pk`` and ``_raw_orm`` via ``object.__setattr__`` — works on
        both regular and ``frozen=True`` dataclasses.

        Args:
            orm_obj: Loaded or freshly inserted ORM object.

        Returns:
            ``DomainModel`` with all fields, ``pk``, and ``_raw_orm`` set.

        Edge cases:
            - Single PK  → ``entity.pk = scalar``
            - Composite  → ``entity.pk = (val1, val2, ...)``
        """
        kwargs: dict[str, Any] = {
            name: getattr(orm_obj, name) for name in self._domain_fields
        }
        instance = self._domain_cls(**kwargs)

        # Compute the pk value: scalar for single, tuple for composite
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
        tracking remain intact.  The PK is never synced — changing a PK
        mid-session is almost always a bug.

        Args:
            domain:  Domain entity with updated values.
            orm_obj: Existing, session-tracked ORM object.

        Returns:
            The same ``orm_obj``, mutated in-place.
        """
        for name in self._domain_fields:
            setattr(orm_obj, name, getattr(domain, name))
        return orm_obj
