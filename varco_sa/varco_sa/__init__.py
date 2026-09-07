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

    provider = SQLAlchemyRepositoryProvider.from_components(
        base=Base, session_factory=async_session
    )
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

from varco_sa.advisory_lock import SAAdvisoryLock, SAXactAdvisoryLock
from varco_sa.alembic_helpers import get_target_metadata, print_create_ddl
from varco_sa.audit import audit_metadata
from varco_sa.bootstrap import SAConfig, SAFastrestApp
from varco_sa.conversation import SAConversationStore, conversation_metadata
from varco_sa.deduplication import SADedupConfig, SADeduplicator, dedup_metadata
from varco_sa.di import SAModule, bind_repositories
from varco_sa.dlq import dead_letters_metadata
from varco_sa.encryption_store import encryption_metadata
from varco_sa.factory import SAModelFactory, SAModelRegistry
from varco_sa.health import SAHealthCheck, SAPoolSaturationCheck
from varco_sa.idempotency import SAIdempotencyStore, idempotency_metadata
from varco_sa.inbox import (
    InboxEntryModel,
    SAInboxRepository,
    SAPollerInboxRepository,
    inbox_metadata,
)
from varco_sa.job_store import SAJobStore, jobs_metadata
from varco_sa.metadata import (
    framework_metadata,
    framework_table_names,
    register_framework_metadata,
)
from varco_sa.models import BaseDatabaseModel
from varco_sa.outbox import (
    OutboxEntryModel,
    SAOutboxRepository,
    SARelayOutboxRepository,
    outbox_metadata,
)
from varco_sa.provider import SQLAlchemyRepositoryProvider

# SA-specific applicator is in varco_core (no session; pure SA expressions)
from varco_sa.query.applicator import SQLAlchemyQueryApplicator
from varco_sa.repository import AsyncSQLAlchemyRepository

# Plan 037 / S12 — RLS-by-default: the generated-for-you DDL path, the
# after_begin GUC-setter hook, and the BYPASSRLS/owner posture check.
from varco_sa.rls_autogen import (
    NullTenantPolicy,
    RlsTablePlan,
    plan_tenant_rls,
    render_tenant_rls_ddl,
    tenant_rls_downgrade,
    tenant_rls_upgrade,
)
from varco_sa.rls_framework import framework_rls_tables
from varco_sa.saga import SASagaRepository, sagas_metadata
from varco_sa.schema_guard import SchemaDrift, SchemaDriftReport, SchemaGuard
from varco_sa.tenancy.rls_check import RlsPosture, inspect_rls_posture
from varco_sa.tenancy.rls_session import install_rls_tenant_hook
from varco_sa.type_coercion import registry_from_sa_model
from varco_sa.uow import SQLAlchemyUnitOfWork

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
    "SAXactAdvisoryLock",
    # ── Health probes ──────────────────────────────────────────────────────────
    "SAHealthCheck",
    "SAPoolSaturationCheck",
    # ── Deduplication ─────────────────────────────────────────────────────────
    "SADeduplicator",
    "SADedupConfig",
    "dedup_metadata",
    # ── Framework schema ──────────────────────────────────────────────────────
    "framework_metadata",
    "framework_table_names",
    "register_framework_metadata",
    "audit_metadata",
    "dead_letters_metadata",
    "encryption_metadata",
    # ── Idempotency store (Plan 029 / D1b) ────────────────────────────────────
    "SAIdempotencyStore",
    "idempotency_metadata",
    # ── RLS-by-default (Plan 037 / S12) ───────────────────────────────────────
    "NullTenantPolicy",
    "RlsPosture",
    "RlsTablePlan",
    "framework_rls_tables",
    "inspect_rls_posture",
    "install_rls_tenant_hook",
    "plan_tenant_rls",
    "render_tenant_rls_ddl",
    "tenant_rls_downgrade",
    "tenant_rls_upgrade",
]
