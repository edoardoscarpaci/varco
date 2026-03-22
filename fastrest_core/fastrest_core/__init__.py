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
    SoftDeleteAuditedDomainModel,
    SoftDeleteDomainModel,
    SoftDeleteMixin,
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
from fastrest_core.dto.factory import DTOSet, generate_dtos
from fastrest_core.dto.pagination import (
    PageCursor,
    PagedReadDTO,
    SortCursorField,
    paged_response,
)

# ── Query system ───────────────────────────────────────────────────────────────
from fastrest_core.query.builder import QueryBuilder
from fastrest_core.query.params import QueryParams
from fastrest_core.query.parser import QueryParser
from fastrest_core.query.type import Operation, SortField, SortOrder

# ── Multi-tenancy ───────────────────────────────────────────────────────────────
from fastrest_core.service.tenant import (
    TenantAwareService,
    TenantUoWProvider,
    current_tenant,
    tenant_context,
)

# ── Soft delete ─────────────────────────────────────────────────────────────────
from fastrest_core.service.soft_delete import SoftDeleteService

# ── Service type aliases and protocols ──────────────────────────────────────────
from fastrest_core.service.types import Assembler, ServiceProtocol

# ── Tracing / correlation ID ────────────────────────────────────────────────────
from fastrest_core.tracing import (
    CorrelationIdFilter,
    correlation_context,
    current_correlation_id,
    generate_correlation_id,
)

# ── Auth helpers ─────────────────────────────────────────────────────────────────
from fastrest_core.auth.helpers import (
    GrantBasedAuthorizer,
    OwnershipAuthorizer,
    RoleBasedAuthorizer,
)

# ── Error codes and HTTP error mapping ──────────────────────────────────────────
from fastrest_core.exception.codes import ErrorCode, FastrestErrorCodes
from fastrest_core.exception.http import (
    AnyErrorCode,
    ErrorMessage,
    error_code_for,
    error_message_for,
    register_error_code,
)

# ── JWT layer ──────────────────────────────────────────────────────────────────
from fastrest_core.jwt import (
    SYSTEM_ISSUER,
    JsonWebToken,
    JwtBuilder,
    JwtParser,
    JwtUtil,
)

# ── JWK layer ──────────────────────────────────────────────────────────────────
from fastrest_core.jwk import (
    JsonWebKey,
    JsonWebKeySet,
    JwkBuilder,
)

# ── Authority layer ─────────────────────────────────────────────────────────────
from fastrest_core.authority import (
    AuthorizationConfig,
    AuthorityError,
    AuthoritySource,
    IssuerNotFoundError,
    JwtAuthority,
    KeyLoadError,
    MultiKeyAuthority,
    TrustedIssuerEntry,
    TrustedIssuerRegistry,
    UnknownKidError,
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
    # ── Soft delete domain mixins ───────────────────────────────────────────────
    "SoftDeleteMixin",
    "SoftDeleteDomainModel",
    "SoftDeleteAuditedDomainModel",
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
    "DTOSet",
    "generate_dtos",
    # ── Pagination ──────────────────────────────────────────────────────────────
    "SortCursorField",
    "PageCursor",
    "PagedReadDTO",
    "paged_response",
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
    # ── Soft delete service ─────────────────────────────────────────────────────
    "SoftDeleteService",
    # ── Service type aliases ────────────────────────────────────────────────────
    "Assembler",
    "ServiceProtocol",
    # ── Tracing ─────────────────────────────────────────────────────────────────
    "CorrelationIdFilter",
    "correlation_context",
    "current_correlation_id",
    "generate_correlation_id",
    # ── Auth helpers ─────────────────────────────────────────────────────────────
    "GrantBasedAuthorizer",
    "OwnershipAuthorizer",
    "RoleBasedAuthorizer",
    # ── Error codes and HTTP error mapping ───────────────────────────────────────
    "AnyErrorCode",
    "ErrorCode",
    "FastrestErrorCodes",
    "ErrorMessage",
    "error_code_for",
    "error_message_for",
    "register_error_code",
    # ── JWT layer ───────────────────────────────────────────────────────────────
    "SYSTEM_ISSUER",
    "JsonWebToken",
    "JwtBuilder",
    "JwtParser",
    "JwtUtil",
    # ── JWK layer ───────────────────────────────────────────────────────────────
    "JsonWebKey",
    "JsonWebKeySet",
    "JwkBuilder",
    # ── Authority layer ─────────────────────────────────────────────────────────
    "JwtAuthority",
    "MultiKeyAuthority",
    "TrustedIssuerRegistry",
    "TrustedIssuerEntry",
    "AuthoritySource",
    "AuthorizationConfig",
    "AuthorityError",
    "UnknownKidError",
    "IssuerNotFoundError",
    "KeyLoadError",
]
