"""
varco_sa.models
===================
Pre-built SQLAlchemy declarative base models with common timestamp and
primary-key patterns.

These are **optional** conveniences — projects that generate their schema
entirely through ``SAModelFactory`` do not need them.  They are most useful
when hand-crafting SA models that need standard ``created_at``, ``updated_at``,
or string ``id`` columns without repeating the boilerplate every time.

Hierarchy::

    DeclarativeBase
    └── BaseDatabaseModel    — created_at, updated_at, model_version
        └── IndexedDatabaseModel — adds uuid-string id PK
            └── AuthorizedModel  — adds auth_context string column

DESIGN: moved from ``varco_core.models.database_model``
  Rationale: ``BaseDatabaseModel`` directly subclasses SQLAlchemy's
  ``DeclarativeBase``, making it SA-specific.  Placing it in
  ``varco_core`` would force a SQLAlchemy import in a package that
  is otherwise ORM-agnostic.  It now lives in ``varco_sa`` alongside
  all other SA-specific code.

Thread safety:  ✅ SQLAlchemy mapped classes are read-only after creation.
Async safety:   ✅ Class definitions; no I/O.
"""

from __future__ import annotations

from datetime import datetime
from typing import TypeVar

from sqlalchemy import DateTime, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BaseDatabaseModel(DeclarativeBase):
    """
    Abstract SA base model providing audit timestamp columns.

    Attributes:
        created_at:    Server-side default timestamp set on INSERT.
        updated_at:    Server-side default timestamp updated on UPDATE.
        model_version: Integer version counter — useful for optimistic locking
                       (increment in application code on each save).

    Thread safety:  ✅ Mapped class is immutable after definition.
    Async safety:   ✅ No I/O.

    Edge cases:
        - ``created_at`` / ``updated_at`` use ``server_default`` — the DB sets
          the value on INSERT/UPDATE, so in-memory objects won't have the column
          populated until after a flush/commit and a re-load.
        - ``model_version`` is not auto-incremented — the application layer must
          increment it manually if used for optimistic locking.
    """

    __abstract__ = True

    created_at: Mapped[datetime] = mapped_column(
        # server_default = DB function; no Python-side value needed
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        # onupdate = SA fires func.now() on every UPDATE statement
        onupdate=func.now(),
    )
    # Integer version counter — starts at 1, caller increments on save
    model_version: Mapped[int] = mapped_column(default=1)


# Bound TypeVar used by generic repository / factory signatures.
TDatabaseModel = TypeVar("TDatabaseModel", bound=BaseDatabaseModel)
