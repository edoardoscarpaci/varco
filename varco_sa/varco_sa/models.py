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
        updated_at:    ORM-side default timestamp refreshed on every ORM UPDATE.
                       See ``DESIGN`` note below for the critical distinction
                       between ORM-side and server-side update hooks.
        model_version: Integer version counter — useful for optimistic locking
                       (increment in application code on each save).

    Thread safety:  ✅ Mapped class is immutable after definition.
    Async safety:   ✅ No I/O.

    Edge cases:
        - ``created_at`` uses ``server_default=func.now()`` — the DB sets
          the value on INSERT, so in-memory objects won't have the column
          populated until after a flush/commit and a re-load.
        - ``updated_at`` uses ``onupdate=func.now()`` (ORM-side) — see the
          DESIGN block below for the full list of cases where it does NOT fire.
        - ``model_version`` is not auto-incremented — the application layer must
          increment it manually if used for optimistic locking.
        - Bulk UPDATE via ``session.execute(update(Model).where(...))`` bypasses
          the ORM event system entirely: ``updated_at`` will be silently stale.
          Fix: ``session.execute(update(Model).where(...).values(updated_at=func.now()))``.
        - Raw SQL and Alembic data migrations bypass SQLAlchemy and will NOT
          update ``updated_at``.  Update the column explicitly in those scripts.
    """

    __abstract__ = True

    # DESIGN: updated_at uses onupdate=func.now() (ORM-side), NOT server_onupdate
    #
    # ``onupdate`` is evaluated by SQLAlchemy when it constructs an UPDATE
    # statement via the ORM (session.add() + flush / session.merge() etc.).
    # It is NOT a database-level ON UPDATE trigger or ``server_onupdate``
    # (which would require a DB trigger or generated column definition).
    #
    # Tradeoffs:
    #   ✅ Portable — no DB-specific trigger DDL required; works on PostgreSQL,
    #      MySQL, SQLite without any extra migration steps.
    #   ✅ SQLAlchemy knows the value immediately after flush (it was computed
    #      here, not on the server), so no extra RETURNING / SELECT reload needed.
    #   ❌ Bulk ORM updates — ``session.execute(update(Model).where(...))``
    #      bypasses ORM instrumentation; ``updated_at`` is silently NOT updated.
    #      Workaround: append ``.values(updated_at=func.now())`` explicitly.
    #   ❌ Raw SQL (psql, Alembic data migrations, ``text()`` queries) bypasses
    #      SQLAlchemy entirely — ``updated_at`` will be stale after those writes.
    #   ❌ External writers (other services, DB admin tools) do not trigger
    #      ``onupdate`` — the column will not reflect their changes.
    #
    # Alternative considered: ``server_onupdate`` + DB trigger
    #   Would fire on ALL writes regardless of ORM/raw SQL, but requires
    #   a trigger migration per database dialect and makes local testing harder
    #   (SQLite does not support ON UPDATE triggers natively).
    #   Rejected in favour of portability for a general-purpose library.

    created_at: Mapped[datetime] = mapped_column(
        # server_default = DB function; no Python-side value needed.
        # The DB fills this on INSERT; in-memory value is absent until reload.
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        # server_default provides the initial value on INSERT (same behaviour
        # as created_at — DB fills it so the row is consistent from the start).
        server_default=func.now(),
        # onupdate fires on every ORM-generated UPDATE — SQLAlchemy substitutes
        # func.now() into the SET clause.  See DESIGN block above for the cases
        # where this does NOT fire (bulk updates, raw SQL, external writers).
        onupdate=func.now(),
    )
    # Integer version counter — starts at 1, caller increments on save
    model_version: Mapped[int] = mapped_column(default=1)


# Bound TypeVar used by generic repository / factory signatures.
TDatabaseModel = TypeVar("TDatabaseModel", bound=BaseDatabaseModel)
