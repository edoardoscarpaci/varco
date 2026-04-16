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
from varco_core.migrator import (
    DomainMigrator,
    MigrationError,
    MigrationPlan,  # noqa: F401
    StepSpec,  # noqa: F401
)
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

# ── Validation layer ─────────────────────────────────────────────────────────────
from varco_core.validation import (
    VALID,
    CompositeValidator,
    DomainModelValidator,
    ValidationError,
    ValidationResult,
    Validator,
)
from varco_core.service.validation import ValidatorServiceMixin

# ── Settings base ───────────────────────────────────────────────────────────────
from varco_core.config import VarcoSettings

# ── Serialization ────────────────────────────────────────────────────────────────
from varco_core.serialization import JsonSerializer, NoOpSerializer, Serializer

# ── Event system ────────────────────────────────────────────────────────────────
from varco_core.event import (
    CHANNEL_ALL,
    CHANNEL_DEFAULT,
    AbstractDeadLetterQueue,
    AbstractEventBus,
    AbstractEventProducer,
    BusEventProducer,
    ChannelManager,
    DeadLetterEntry,
    EntityCreatedEvent,
    EntityDeletedEvent,
    EntityEvent,
    EntityUpdatedEvent,
    ErrorPolicy,
    Event,
    EventBusSettings,
    EventConsumer,
    EventMiddleware,
    EventSerializer,
    InMemoryDeadLetterQueue,
    InMemoryEventBus,
    JsonEventSerializer,
    NoopEventProducer,
    Subscription,
    listen,
)

# ── Resilience patterns ──────────────────────────────────────────────────────────
from varco_core.resilience import (
    CallTimeoutError,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
    RetryExhaustedError,
    RetryPolicy,
    circuit_breaker,
    retry,
    timeout,
)

# ── Cache system ────────────────────────────────────────────────────────────────
from varco_core.cache import (
    AsyncCache,
    CacheBackend,
    CacheInvalidated,
    CacheInvalidationConsumer,
    CacheInvalidationEvent,
    CacheServiceMixin,
    CacheSettings,
    CachedService,
    CompositeStrategy,
    ExplicitStrategy,
    InMemoryCache,
    InvalidationStrategy,
    LayeredCache,
    NoOpCache,
    TTLStrategy,
    TaggedStrategy,
    cached,
)

# ── Tracing / correlation ID ────────────────────────────────────────────────────
from varco_core.tracing import (
    CorrelationIdFilter,
    correlation_context,
    current_correlation_id,
    generate_correlation_id,
)

# ── OpenTelemetry observability ──────────────────────────────────────────────────
from varco_core.observability import (
    CounterConfig,
    HistogramConfig,
    Metric,
    MetricKind,
    OtelConfig,
    OtelConfiguration,
    SpanConfig,
    TracingServiceMixin,
    counter,
    create_counter,
    create_histogram,
    create_span,
    histogram,
    register_gauge,
    span,
)

# ── Health probes ────────────────────────────────────────────────────────────────
from varco_core.health import (
    CompositeHealthCheck,
    HealthCheck,
    HealthResult,
    HealthStatus,
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

# ── Job layer ────────────────────────────────────────────────────────────────────
from varco_core.job import (
    AbstractJobRunner,
    AbstractJobStore,
    Job,
    JobStatus,
    auth_context_from_snapshot,
    auth_context_to_snapshot,
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
from varco_core.connection import (
    BasicAuthConfig,
    ConnectionSettings,
    OAuth2Config,
    SaslConfig,
    SSLConfig,
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
    # ── Validation layer ─────────────────────────────────────────────────────────
    "VALID",
    "CompositeValidator",
    "DomainModelValidator",
    "ValidationError",
    "ValidationResult",
    "Validator",
    "ValidatorServiceMixin",
    # ── Settings base ─────────────────────────────────────────────────────────────
    "VarcoSettings",
    # ── Serialization ────────────────────────────────────────────────────────────
    "Serializer",
    "JsonSerializer",
    "NoOpSerializer",
    # ── Event system ─────────────────────────────────────────────────────────────
    "CHANNEL_ALL",
    "CHANNEL_DEFAULT",
    "AbstractDeadLetterQueue",
    "AbstractEventBus",
    "AbstractEventProducer",
    "BusEventProducer",
    "ChannelManager",
    "DeadLetterEntry",
    "EntityCreatedEvent",
    "EntityDeletedEvent",
    "EntityEvent",
    "EntityUpdatedEvent",
    "ErrorPolicy",
    "Event",
    "EventBusSettings",
    "EventConsumer",
    "EventMiddleware",
    "EventSerializer",
    "InMemoryDeadLetterQueue",
    "InMemoryEventBus",
    "JsonEventSerializer",
    "NoopEventProducer",
    "Subscription",
    "listen",
    # ── Cache system ──────────────────────────────────────────────────────────────
    "AsyncCache",
    "CacheBackend",
    "CacheSettings",
    "NoOpCache",
    "InMemoryCache",
    "LayeredCache",
    "InvalidationStrategy",
    "TTLStrategy",
    "ExplicitStrategy",
    "TaggedStrategy",
    "CompositeStrategy",
    "CacheServiceMixin",
    "CacheInvalidationConsumer",
    "cached",
    "CacheInvalidated",
    "CacheInvalidationEvent",
    "CachedService",
    # ── Tracing / correlation ID ─────────────────────────────────────────────────
    "CorrelationIdFilter",
    "correlation_context",
    "current_correlation_id",
    "generate_correlation_id",
    # ── OpenTelemetry observability ───────────────────────────────────────────────
    "OtelConfig",
    "OtelConfiguration",
    "span",
    "SpanConfig",
    "create_span",
    "counter",
    "CounterConfig",
    "histogram",
    "HistogramConfig",
    "create_counter",
    "create_histogram",
    "TracingServiceMixin",
    "Metric",
    "MetricKind",
    "register_gauge",
    # ── Health probes ─────────────────────────────────────────────────────────────
    "HealthCheck",
    "HealthResult",
    "HealthStatus",
    "CompositeHealthCheck",
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
    # ── Resilience patterns ──────────────────────────────────────────────────────
    "CallTimeoutError",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitOpenError",
    "CircuitState",
    "RetryExhaustedError",
    "RetryPolicy",
    "circuit_breaker",
    "retry",
    "timeout",
    # ── Job layer ────────────────────────────────────────────────────────────────
    "Job",
    "JobStatus",
    "AbstractJobStore",
    "AbstractJobRunner",
    "auth_context_to_snapshot",
    "auth_context_from_snapshot",
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
    # ── Connection abstraction layer ─────────────────────────────────────────────
    "SSLConfig",
    "BasicAuthConfig",
    "OAuth2Config",
    "SaslConfig",
    "ConnectionSettings",
]
