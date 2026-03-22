"""
varco_core
=============
Backend-agnostic domain model and query layer.

All stable public symbols are importable directly from ``varco_core``::

    # Domain
    from varco_core import DomainModel, AuditedDomainModel, cast_raw, register
    from varco_core import TenantDomainModel, TenantAuditedDomainModel
    from varco_core import AbstractMapper, AsyncRepository, AsyncUnitOfWork
    from varco_core import DomainModelRegistry, RepositoryProvider

    # Metadata
    from varco_core.meta import (
        FieldHint, ForeignKey, PrimaryKey, PKStrategy,
        UniqueConstraint, CheckConstraint, pk_field,
    )

    # Query system
    from varco_core import QueryBuilder, QueryParams, QueryParser
    from varco_core import SortField, SortOrder, Operation

    # DTOs (API layer)
    from varco_core import CreateDTO, ReadDTO, UpdateDTO, UpdateOperation

Sub-package layout
------------------
    varco_core/
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
from varco_core.exception.repository import StaleEntityError
from varco_core.mapper import AbstractMapper
from varco_core.migrator import DomainMigrator, MigrationError
from varco_core.model import (
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
from varco_core.providers import RepositoryProvider
from varco_core.registry import DomainModelRegistry, register
from varco_core.repository import AsyncRepository
from varco_core.uow import AsyncUnitOfWork

# ── DTO layer ──────────────────────────────────────────────────────────────────
from varco_core.dto import (
    CreateDTO,
    ReadDTO,
    TCreateDTO,
    TReadDTO,
    TUpdateDTO,
    UpdateDTO,
    UpdateOperation,
)
from varco_core.dto.factory import DTOSet, generate_dtos
from varco_core.dto.pagination import (
    PageCursor,
    PagedReadDTO,
    SortCursorField,
    paged_response,
)

# ── Query system ───────────────────────────────────────────────────────────────
from varco_core.query.builder import QueryBuilder
from varco_core.query.params import QueryParams
from varco_core.query.parser import QueryParser
from varco_core.query.type import Operation, SortField, SortOrder

# ── Multi-tenancy ───────────────────────────────────────────────────────────────
from varco_core.service.tenant import (
    TenantAwareService,
    TenantUoWProvider,
    current_tenant,
    tenant_context,
)

# ── Soft delete ─────────────────────────────────────────────────────────────────
from varco_core.service.soft_delete import SoftDeleteService

# ── Service type aliases and protocols ──────────────────────────────────────────
from varco_core.service.types import Assembler, ServiceProtocol

# ── Event system ────────────────────────────────────────────────────────────────
from varco_core.event import (
    CHANNEL_ALL,
    CHANNEL_DEFAULT,
    AbstractEventBus,
    AbstractEventProducer,
    BusEventProducer,
    EntityCreatedEvent,
    EntityDeletedEvent,
    EntityEvent,
    EntityUpdatedEvent,
    ErrorPolicy,
    Event,
    EventConsumer,
    EventMiddleware,
    InMemoryEventBus,
    JsonEventSerializer,
    NoopEventProducer,
    Subscription,
    listen,
)

# ── Tracing / correlation ID ────────────────────────────────────────────────────
from varco_core.tracing import (
    CorrelationIdFilter,
    correlation_context,
    current_correlation_id,
    generate_correlation_id,
)

# ── Auth helpers ─────────────────────────────────────────────────────────────────
from varco_core.auth.helpers import (
    GrantBasedAuthorizer,
    OwnershipAuthorizer,
    RoleBasedAuthorizer,
)

# ── Error codes and HTTP error mapping ──────────────────────────────────────────
from varco_core.exception.codes import ErrorCode, FastrestErrorCodes
from varco_core.exception.http import (
    AnyErrorCode,
    ErrorMessage,
    error_code_for,
    error_message_for,
    register_error_code,
)

# ── JWT layer ──────────────────────────────────────────────────────────────────
from varco_core.jwt import (
    SYSTEM_ISSUER,
    JsonWebToken,
    JwtBuilder,
    JwtParser,
    JwtUtil,
)

# ── JWK layer ──────────────────────────────────────────────────────────────────
from varco_core.jwk import (
    JsonWebKey,
    JsonWebKeySet,
    JwkBuilder,
)

# ── Authority layer ─────────────────────────────────────────────────────────────
from varco_core.authority import (
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
    # ── Event system ─────────────────────────────────────────────────────────────
    "CHANNEL_ALL",
    "CHANNEL_DEFAULT",
    "AbstractEventBus",
    "AbstractEventProducer",
    "BusEventProducer",
    "EntityCreatedEvent",
    "EntityDeletedEvent",
    "EntityEvent",
    "EntityUpdatedEvent",
    "ErrorPolicy",
    "Event",
    "EventConsumer",
    "EventMiddleware",
    "InMemoryEventBus",
    "JsonEventSerializer",
    "NoopEventProducer",
    "Subscription",
    "listen",
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
