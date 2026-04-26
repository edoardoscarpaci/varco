"""
varco_sa
============
SQLAlchemy async backend for varco.

All stable public symbols are importable directly from ``varco_sa``::

    from varco_sa import SQLAlchemyRepositoryProvider, SAModelRegistry
    from varco_sa import BaseDatabaseModel, IndexedDatabaseModel
    from varco_sa import SQLAlchemyQueryApplicator

    # Query usage
    from varco_core import QueryBuilder, QueryParams
    from varco_sa import SQLAlchemyRepositoryProvider

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
    varco_sa/
    ├── factory.py    — SAModelFactory (DomainModel → SA ORM class at runtime)
    │                   SAModelRegistry (process-level ORM class registry)
    ├── provider.py   — SQLAlchemyRepositoryProvider
    ├── repository.py — AsyncSQLAlchemyRepository (CRUD + find_by_query + count)
    ├── uow.py        — SQLAlchemyUnitOfWork
    └── models.py     — BaseDatabaseModel
"""

from __future__ import annotations

from varco_sa.alembic_helpers import get_target_metadata, print_create_ddl
from varco_sa.bootstrap import SAConfig, SAFastrestApp
from varco_sa.di import SAModule, bind_repositories
from varco_sa.factory import SAModelFactory, SAModelRegistry
from varco_sa.models import BaseDatabaseModel
from varco_sa.inbox import (
    InboxEntryModel,
    SAInboxRepository,
    SAPollerInboxRepository,
    inbox_metadata,
)
from varco_sa.job_store import SAJobStore, jobs_metadata
from varco_sa.advisory_lock import SAAdvisoryLock
from varco_sa.conversation import SAConversationStore, conversation_metadata
from varco_sa.saga import SASagaRepository, sagas_metadata
from varco_sa.outbox import (
    OutboxEntryModel,
    SAOutboxRepository,
    SARelayOutboxRepository,
    outbox_metadata,
)
from varco_sa.provider import SQLAlchemyRepositoryProvider
from varco_sa.repository import AsyncSQLAlchemyRepository
from varco_sa.schema_guard import SchemaGuard, SchemaDrift, SchemaDriftReport
from varco_sa.uow import SQLAlchemyUnitOfWork

# SA-specific applicator is in varco_core (no session; pure SA expressions)
from varco_core.query.applicator.sqlalchemy import SQLAlchemyQueryApplicator
from varco_sa.type_coercion import registry_from_sa_model

__all__ = [
    # ── DI integration ────────────────────────────────────────────────────────
    "SAModule",
    "bind_repositories",
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
    # ── Alembic helpers ───────────────────────────────────────────────────────
    "get_target_metadata",
    "print_create_ddl",
    # ── Bootstrap ─────────────────────────────────────────────────────────────
    "SAConfig",
    "SAFastrestApp",
    # ── Outbox pattern ────────────────────────────────────────────────────────
    "OutboxEntryModel",
    "outbox_metadata",
    "SAOutboxRepository",
    "SARelayOutboxRepository",
    # ── Inbox pattern ─────────────────────────────────────────────────────────
    "InboxEntryModel",
    "inbox_metadata",
    "SAInboxRepository",
    "SAPollerInboxRepository",
    # ── Job store ─────────────────────────────────────────────────────────────
    "SAJobStore",
    "jobs_metadata",
    # ── Saga repository ───────────────────────────────────────────────────────
    "SASagaRepository",
    "sagas_metadata",
    # ── Conversation store ────────────────────────────────────────────────────
    "SAConversationStore",
    "conversation_metadata",
    # ── Advisory lock ─────────────────────────────────────────────────────────
    "SAAdvisoryLock",
]
