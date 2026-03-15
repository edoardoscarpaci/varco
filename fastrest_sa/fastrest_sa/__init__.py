"""
fastrest_sa
============
SQLAlchemy async backend for fastrest.

All stable public symbols are importable directly from ``fastrest_sa``::

    from fastrest_sa import SQLAlchemyRepositoryProvider, SAModelRegistry
    from fastrest_sa import BaseDatabaseModel, IndexedDatabaseModel
    from fastrest_sa import SQLAlchemyQueryApplicator

    # Query usage
    from fastrest_core import QueryBuilder, QueryParams
    from fastrest_sa import SQLAlchemyRepositoryProvider

    provider = SQLAlchemyRepositoryProvider(base=Base, session_factory=async_session)
    provider.register(User, Post)

    async with provider.make_uow() as uow:
        active_users = await uow.users.find_by_query(
            QueryParams(
                node=QueryBuilder().eq("active", True).build(),
                limit=20,
            )
        )

Sub-package layout
------------------
    fastrest_sa/
    ├── factory.py    — SAModelFactory (DomainModel → SA ORM class at runtime)
    │                   SAModelRegistry (process-level ORM class registry)
    ├── provider.py   — SQLAlchemyRepositoryProvider
    ├── repository.py — AsyncSQLAlchemyRepository (CRUD + find_by_query + count)
    ├── uow.py        — SQLAlchemyUnitOfWork
    └── models.py     — BaseDatabaseModel
"""

from __future__ import annotations

from fastrest_sa.factory import SAModelFactory, SAModelRegistry
from fastrest_sa.models import BaseDatabaseModel
from fastrest_sa.provider import SQLAlchemyRepositoryProvider
from fastrest_sa.repository import AsyncSQLAlchemyRepository
from fastrest_sa.schema_guard import SchemaGuard, SchemaDrift, SchemaDriftReport
from fastrest_sa.uow import SQLAlchemyUnitOfWork

# SA-specific applicator is in fastrest_core (no session; pure SA expressions)
from fastrest_core.query.applicator.sqlalchemy import SQLAlchemyQueryApplicator
from fastrest_sa.type_coercion import registry_from_sa_model

__all__ = [
    # ── Factory + registry ─────────────────────────────────────────────────────
    "SAModelFactory",
    "SAModelRegistry",
    # ── Repository + UoW ──────────────────────────────────────────────────────
    "AsyncSQLAlchemyRepository",
    "SQLAlchemyUnitOfWork",
    # ── Provider ──────────────────────────────────────────────────────────────
    "SQLAlchemyRepositoryProvider",
    # ── Base models ───────────────────────────────────────────────────────────
    "BaseDatabaseModel",
    # ── Query applicator ──────────────────────────────────────────────────────
    "SQLAlchemyQueryApplicator",
    # ── Schema guard ──────────────────────────────────────────────────────────
    "SchemaGuard",
    "SchemaDrift",
    "SchemaDriftReport",
    # ── Type coercion ─────────────────────────────────────────────────────────
    "registry_from_sa_model",
]
