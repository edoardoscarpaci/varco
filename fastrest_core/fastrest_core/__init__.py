"""
fastrest_core
=============
Backend-agnostic domain model and query layer.

All stable public symbols are importable directly from ``fastrest_core``::

    # Domain
    from fastrest_core import DomainModel, AuditedDomainModel, cast_raw, register
    from fastrest_core import TenantDomainModel, TenantAuditedDomainModel
    from fastrest_core import AbstractMapper, AsyncRepository, AsyncUnitOfWork
    from fastrest_core import DomainModelRegistry, RepositoryProvider

    # Metadata
    from fastrest_core.meta import (
        FieldHint, ForeignKey, PrimaryKey, PKStrategy,
        UniqueConstraint, CheckConstraint, pk_field,
    )

    # Query system
    from fastrest_core import QueryBuilder, QueryParams, QueryParser
    from fastrest_core import SortField, SortOrder, Operation

    # DTOs (API layer)
    from fastrest_core import CreateDTO, ReadDTO, UpdateDTO, UpdateOperation

Sub-package layout
------------------
    fastrest_core/
    ├── model.py          — DomainModel, cast_raw
    ├── meta.py           — FieldHint, ForeignKey, PKStrategy, constraints
    ├── mapper.py         — AbstractMapper (bidirectional ORM ↔ domain)
    ├── registry.py       — DomainModelRegistry, @register decorator
    ├── repository.py     — AsyncRepository ABC (CRUD + query)
    ├── providers.py      — RepositoryProvider ABC + autodiscover
    ├── uow.py            — AsyncUnitOfWork ABC
    ├── dto.py            — CreateDTO, ReadDTO, UpdateDTO, UpdateOperation
    └── query/
        ├── type.py       — AST node types
        ├── builder.py    — Fluent QueryBuilder
        ├── params.py     — QueryParams value object
        ├── parser.py     — QueryParser (string → AST via Lark)
        ├── transformer.py— Lark transformer
        ├── grammar.lark  — Query grammar
        ├── visitor/      — ASTVisitor, optimizer, type coercion, SA compiler
        └── applicator/   — QueryApplicator ABC + SA implementation
"""

from __future__ import annotations

# ── Domain layer ───────────────────────────────────────────────────────────────
from fastrest_core.exception.repository import StaleEntityError
from fastrest_core.mapper import AbstractMapper
from fastrest_core.migrator import DomainMigrator, MigrationError
from fastrest_core.model import (
    AuditedDomainModel,
    DomainModel,
    TenantAuditedDomainModel,
    TenantDomainModel,
    TenantMixin,
    TenantVersionedDomainModel,
    VersionedDomainModel,
    cast_raw,
)
from fastrest_core.providers import RepositoryProvider
from fastrest_core.registry import DomainModelRegistry, register
from fastrest_core.repository import AsyncRepository
from fastrest_core.uow import AsyncUnitOfWork

# ── DTO layer ──────────────────────────────────────────────────────────────────
from fastrest_core.dto import (
    CreateDTO,
    ReadDTO,
    TCreateDTO,
    TReadDTO,
    TUpdateDTO,
    UpdateDTO,
    UpdateOperation,
)

# ── Query system ───────────────────────────────────────────────────────────────
from fastrest_core.query.builder import QueryBuilder
from fastrest_core.query.params import QueryParams
from fastrest_core.query.parser import QueryParser
from fastrest_core.query.type import Operation, SortField, SortOrder

# ── Multi-tenancy ───────────────────────────────────────────────────────────────
from fastrest_core.tenant import (
    TenantAwareService,
    TenantUoWProvider,
    current_tenant,
    tenant_context,
)

__all__ = [
    # ── Domain base ────────────────────────────────────────────────────────────
    "DomainModel",
    "AuditedDomainModel",
    "VersionedDomainModel",
    "TenantMixin",
    "TenantDomainModel",
    "TenantAuditedDomainModel",
    "TenantVersionedDomainModel",
    "cast_raw",
    # ── Migration ──────────────────────────────────────────────────────────────
    "DomainMigrator",
    "MigrationError",
    "StaleEntityError",
    # ── Abstraction layer ──────────────────────────────────────────────────────
    "AbstractMapper",
    "AsyncRepository",
    "AsyncUnitOfWork",
    # ── Registration ───────────────────────────────────────────────────────────
    "DomainModelRegistry",
    "register",
    # ── Provider ABC ───────────────────────────────────────────────────────────
    "RepositoryProvider",
    # ── DTO layer ──────────────────────────────────────────────────────────────
    "CreateDTO",
    "ReadDTO",
    "UpdateDTO",
    "UpdateOperation",
    "TCreateDTO",
    "TReadDTO",
    "TUpdateDTO",
    # ── Query system ───────────────────────────────────────────────────────────
    "QueryBuilder",
    "QueryParams",
    "QueryParser",
    "SortField",
    "SortOrder",
    "Operation",
    # ── Multi-tenancy ───────────────────────────────────────────────────────────
    "TenantAwareService",
    "TenantUoWProvider",
    "current_tenant",
    "tenant_context",
]
