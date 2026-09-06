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

import importlib
from typing import TYPE_CHECKING, Any, Final

# ─────────────────────────────────────────────────────────────────────────────
# DESIGN: PEP 562 module ``__getattr__``, with the eager block kept under
# ``TYPE_CHECKING`` (Plan 028 / Phase 0, §D-P1-mechanism)
#
# ``import varco_core`` used to cost ~290 ms on the implementer's machine
# (419 ms on the scout's, ``BACKLOG.md:53-57``) purely because this file
# imported ~700 modules eagerly to bind 235 names. It now binds nothing: the
# ``_LAZY`` map below records which submodule defines each ``__all__`` name,
# and ``__getattr__`` imports that submodule on first access only.
#
# ✅ PEP 562 is Final since 3.7 and is the *shipped* mechanism (brief 002 §1);
#    PEP 690 was rejected and PEP 810 needs 3.15. There is no third option.
# ✅ ``globals()[name] = value`` means the ``__getattr__`` cost is paid **once
#    per name per process** — brief 002 §1's "runtime overhead on every lazy
#    access, not suitable for tight loops" caveat does not apply, because after
#    the first access the name is a normal module global.
# ✅ The ``TYPE_CHECKING`` block keeps mypy ``strict`` (root
#    ``pyproject.toml:163``) fully informed; without it, ``__getattr__ -> Any``
#    would silently make every ``from varco_core import X`` an ``Any`` and
#    quietly erase type checking across the repo. **This is the single most
#    important detail of the phase** — a name added to ``_LAZY`` but not to the
#    block below degrades to ``Any`` repo-wide, which mypy cannot report.
#    ``varco_core/tests/test_lazy_init.py`` parses this file with ``ast`` and
#    asserts the two sets are equal, so the drift is impossible rather than
#    merely discouraged.
# ✅ ``scripts/api_surface.py`` does ``getattr(module, name)`` for every
#    ``__all__`` entry and reads ``obj.__module__`` for the defining module — a
#    lazy attribute resolves to the identical object, so the committed snapshot
#    is unchanged and the CI gate stays green with no regeneration.
# ✅ PEP 562 is already in this repo:
#    ``varco_beanie/varco_beanie/__init__.py:125`` and
#    ``varco_beanie/varco_beanie/bootstrap.py:173`` (``deprecated_alias``).
# ❌ An ``ImportError`` inside a submodule now surfaces at **first attribute
#    access**, not at ``import varco_core``. That is a genuine ergonomic loss (a
#    typo'd optional dependency fails later and further from its cause).
#    Mitigated by ``test_lazy_init.py``, which resolves **every** name in
#    ``__all__`` and would catch a broken submodule in CI on every run.
# ❌ ``from varco_core import *`` still materialises everything. Fine — nobody
#    does that in production code, and it remains correct.
# ❌ Accepted incompatibility: ``varco_core.__dict__["DomainModel"]`` raises
#    ``KeyError`` before the name's first access, where it previously
#    succeeded. Attribute access (``varco_core.DomainModel``,
#    ``from varco_core import DomainModel``, ``getattr``) is unaffected —
#    only reaching into ``__dict__`` directly, which nothing does.
#
# The side-effect audit that bounds this change (the two ``rg`` sweeps, the
# module-scope decorator sweep, and the ``sys.modules`` differential proving no
# ``varco_core`` module is imported for its side effect) is committed at
# ``design/async-performance-patterns/measurements/p1-side-effect-audit.md``.
# ─────────────────────────────────────────────────────────────────────────────

if TYPE_CHECKING:
    # ── Auth helpers ─────────────────────────────────────────────────────────────────
    from varco_core.auth.helpers import (
        GrantBasedAuthorizer,
        OwnershipAuthorizer,
        RoleBasedAuthorizer,
    )

    # ── Authority layer ─────────────────────────────────────────────────────────────
    from varco_core.authority import (
        AuthorityError,
        AuthoritySource,
        AuthorizationConfig,
        IssuerNotFoundError,
        JwtAuthority,
        KeyLoadError,
        MultiKeyAuthority,
        TrustedIssuerEntry,
        TrustedIssuerRegistry,
        UnknownKidError,
    )

    # ── Cache system ────────────────────────────────────────────────────────────────
    from varco_core.cache import (
        AsyncCache,
        CacheBackend,
        CachedService,
        CacheInvalidated,
        CacheInvalidationConsumer,
        CacheInvalidationEvent,
        CacheServiceMixin,
        CacheSettings,
        CompositeStrategy,
        ExplicitStrategy,
        InMemoryCache,
        InvalidationStrategy,
        LayeredCache,
        NoOpCache,
        TaggedStrategy,
        TTLStrategy,
        cached,
    )

    # ── Settings base ───────────────────────────────────────────────────────────────
    from varco_core.config import VarcoSettings
    from varco_core.connection import (
        BasicAuthConfig,
        ConnectionSettings,
        OAuth2Config,
        SaslConfig,
        SSLConfig,
    )

    # ── Ambient request context (Plan 011 X1) ────────────────────────────────────
    from varco_core.context import (
        AmbientVar,
        NullTenantDefaults,
        RequestContext,
        Resolved,
        StaticTenantDefaults,
        TenantDefaultsProvider,
        TenantLocalizationDefaults,
        current_locale,
        current_request_context,
        current_timezone,
        request_context,
        resolve_precedence,
    )

    # ── Deprecation mechanism (Plan 022 / §D-DEP) ─────────────────────────────────
    from varco_core.deprecation import deprecated, deprecated_alias

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
        InMemoryDeadLetterQueue,
        InMemoryEventBus,
        JsonEventSerializer,
        NoopEventProducer,
        Subscription,
        listen,
    )

    # ── Error codes and HTTP error mapping ──────────────────────────────────────────
    from varco_core.exception.codes import ErrorCode, FastrestErrorCodes, VarcoErrorCodes
    from varco_core.exception.http import (
        AnyErrorCode,
        ErrorMessage,
        FieldError,
        MessageResolver,
        error_code_for,
        error_message_for,
        register_error_code,
    )

    # ── Domain layer ───────────────────────────────────────────────────────────────
    from varco_core.exception.repository import StaleEntityError
    from varco_core.exception.settings import ErrorEnvelopeSettings

    # ── Health probes ────────────────────────────────────────────────────────────────
    from varco_core.health import (
        CompositeHealthCheck,
        HealthCheck,
        HealthResult,
        HealthStatus,
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

    # ── JWK layer ──────────────────────────────────────────────────────────────────
    from varco_core.jwk import (
        JsonWebKey,
        JsonWebKeySet,
        JwkBuilder,
    )

    # ── JWT layer ──────────────────────────────────────────────────────────────────
    from varco_core.jwt import (
        IDENTITY,
        PROFILE_METADATA_KEY,
        SYSTEM_ISSUER,
        CanonicalClaim,
        ClaimMapping,
        ClaimPath,
        ClaimRule,
        ClaimTransformer,
        ClaimTransformError,
        IdentityClaimTransformer,
        JsonWebToken,
        JwtBuilder,
        JwtException,
        JwtParser,
        JwtUtil,
        JwtVerificationSettings,
        MappingClaimTransformer,
        TokenProfile,
        TokenProfileError,
        TokenProfileRegistry,
        ValueShape,
        configure_claim_transforms,
        configure_token_profiles,
        read_claim,
        reset_claim_transforms,
        reset_token_profiles,
        resolve_claim_transformer,
        resolve_token_profile,
    )
    from varco_core.mapper import AbstractMapper

    # NOTE: varco_core.migrator (DomainMigrator — data/field migration for domain
    # models) owns the top-level names "MigrationError" and "MigrationPlan". Until
    # 3.0.0 the *schema*-migration package owned those same two names, so they were
    # deliberately NOT re-exported here and had to be imported from
    # varco_core.migration explicitly. Plan 022 / AB-2 renamed the newer, narrower
    # schema pair to SchemaMigrationError / SchemaMigrationPlan, which closes that
    # hole: both concepts are now re-exported below and an import site says which
    # one it means. The old varco_core.migration names still resolve there as
    # deprecated aliases until 4.0.0.
    from varco_core.migration import (
        AbstractMigrator,
        InMemoryMigrator,
        IrreversibleMigrationError,
        MigrationBackendUnavailable,
        MigrationLockTimeout,
        MigrationReport,
        MigrationSettings,
        PendingMigrationsError,
        Revision,
        SchemaMigrationError,
        SchemaMigrationPlan,
    )
    from varco_core.migrator import (
        DomainMigrator,
        MigrationError,
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
    from varco_core.providers import RepositoryProvider

    # ── Query system ───────────────────────────────────────────────────────────────
    from varco_core.query.builder import QueryBuilder
    from varco_core.query.params import QueryParams
    from varco_core.query.parser import QueryParser
    from varco_core.query.type import Operation, SortField, SortOrder
    from varco_core.registry import DomainModelRegistry, register
    from varco_core.repository import AsyncRepository

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

    # ── Serialization ────────────────────────────────────────────────────────────────
    from varco_core.serialization import JsonSerializer, NoOpSerializer, Serializer

    # ── Soft delete ─────────────────────────────────────────────────────────────────
    from varco_core.service.soft_delete import SoftDeleteService

    # ── Multi-tenancy ───────────────────────────────────────────────────────────────
    from varco_core.service.tenant import (
        TenantAwareService,
        TenantUoWProvider,
        current_tenant,
        tenant_context,
    )

    # ── Service type aliases and protocols ──────────────────────────────────────────
    from varco_core.service.types import Assembler, ServiceProtocol
    from varco_core.service.validation import ValidatorServiceMixin

    # ── Multitenancy (Plan 007) ─────────────────────────────────────────────────
    # Grep-checked for collisions against every existing top-level name before
    # export (the MigrationError/MigrationPlan lesson from Plan 006) — none of
    # these names were previously bound at the top level.
    # ── Tenant identity provenance & membership (Plan 033 / S6, S5) ─────────────
    # Same collision-check discipline as the Plan 007 block above — none of
    # these names were previously bound at the top level.
    from varco_core.tenancy import (
        AbstractTenantCatalog,
        AbstractTenantMembership,
        AbstractTenantProvisioner,
        ClaimTenantMembership,
        CrossCheckMode,
        CrossTenantAccessError,
        DestructiveOperationRefused,
        DynamicTenantUoWProvider,
        ExternalTenantProvisioner,
        JwtClaimTenantSource,
        LegacyTenantSource,
        MembershipDecision,
        MissingClaimPolicy,
        NullTenantMembership,
        StaticTenantCatalog,
        SubdomainTenantSource,
        TenancySettings,
        TenantClaim,
        TenantDescriptor,
        TenantIsolation,
        TenantIsolationError,
        TenantMembershipError,
        TenantMembershipSettings,
        TenantNotFoundError,
        TenantProvenance,
        TenantProvenancePosture,
        TenantProvenanceSettings,
        TenantRequest,
        TenantResourcePool,
        TenantScope,
        TenantSource,
        TenantSourceChain,
        TenantStatus,
        TenantTrust,
        assert_tenant_matches,
        build_tenant_source_chain,
        current_tenant_provenance,
        enable_tenant_membership,
        inspect_tenant_provenance,
        provenance_context,
    )

    # ── Tracing / correlation ID ────────────────────────────────────────────────────
    from varco_core.tracing import (
        CorrelationIdFilter,
        correlation_context,
        current_correlation_id,
        generate_correlation_id,
    )
    from varco_core.uow import AsyncUnitOfWork

    # ── Validation layer ─────────────────────────────────────────────────────────────
    from varco_core.validation import (
        VALID,
        CompositeValidator,
        DomainModelValidator,
        ValidationError,
        ValidationResult,
        Validator,
    )

    # NOTE (Plan 025 / T1, T2): varco_core.watch and varco_core.reload are deliberately NOT
    # re-exported here — import them explicitly (`from varco_core.watch import StatPollWatcher`,
    # `from varco_core.reload import ReloadableResource`). Plan 028 / P1 is about *shrinking* this
    # file's eager import graph, and varco_core.watch is meant to be usable from a sidecar or CLI
    # without paying for the whole framework's import cost.

# Modules that must be imported by ``import varco_core`` itself because
# something observable happens at *their* import time rather than at first
# attribute access. Every entry needs an inline reason.
#
# **Empty, and empty on measured evidence** — not on the assumption that no
# side effect exists. The audit file's §5 differential shows the set of
# ``varco_core`` modules reachable by touching every ``__all__`` name is
# byte-identical to the set imported eagerly before this change (175 == 175,
# difference empty in both directions). The only genuine module-scope
# registration in the package (``profiling/backends/__init__.py:24-25``) was
# already never reached by ``import varco_core``. The mechanism is kept so a
# future finding has a documented home instead of a special case.
_EAGER: Final[tuple[str, ...]] = ()

# ``__all__`` name → the submodule that defines it. Generated once from the
# eager import block this file used to carry; **committed, never computed at
# runtime** (computing it would mean importing everything, which is the cost
# being removed). Kept in sync with ``__all__`` and with the ``TYPE_CHECKING``
# block above by ``varco_core/tests/test_lazy_init.py``.
_LAZY: Final[dict[str, str]] = {
    "AbstractDeadLetterQueue": "varco_core.event",
    "AbstractEventBus": "varco_core.event",
    "AbstractEventProducer": "varco_core.event",
    "AbstractJobRunner": "varco_core.job",
    "AbstractJobStore": "varco_core.job",
    "AbstractMapper": "varco_core.mapper",
    "AbstractMigrator": "varco_core.migration",
    "AbstractTenantCatalog": "varco_core.tenancy",
    "AbstractTenantMembership": "varco_core.tenancy",
    "AbstractTenantProvisioner": "varco_core.tenancy",
    "AmbientVar": "varco_core.context",
    "AnyErrorCode": "varco_core.exception.http",
    "Assembler": "varco_core.service.types",
    "AsyncCache": "varco_core.cache",
    "AsyncRepository": "varco_core.repository",
    "AsyncUnitOfWork": "varco_core.uow",
    "AuditedDomainModel": "varco_core.model",
    "AuthorityError": "varco_core.authority",
    "AuthoritySource": "varco_core.authority",
    "AuthorizationConfig": "varco_core.authority",
    "BasicAuthConfig": "varco_core.connection",
    "BusEventProducer": "varco_core.event",
    "CHANNEL_ALL": "varco_core.event",
    "CHANNEL_DEFAULT": "varco_core.event",
    "CacheBackend": "varco_core.cache",
    "CacheInvalidated": "varco_core.cache",
    "CacheInvalidationConsumer": "varco_core.cache",
    "CacheInvalidationEvent": "varco_core.cache",
    "CacheServiceMixin": "varco_core.cache",
    "CacheSettings": "varco_core.cache",
    "CachedService": "varco_core.cache",
    "CallTimeoutError": "varco_core.resilience",
    "CanonicalClaim": "varco_core.jwt",
    "ChannelManager": "varco_core.event",
    "CircuitBreaker": "varco_core.resilience",
    "CircuitBreakerConfig": "varco_core.resilience",
    "CircuitOpenError": "varco_core.resilience",
    "CircuitState": "varco_core.resilience",
    "ClaimMapping": "varco_core.jwt",
    "ClaimPath": "varco_core.jwt",
    "ClaimRule": "varco_core.jwt",
    "ClaimTenantMembership": "varco_core.tenancy",
    "ClaimTransformError": "varco_core.jwt",
    "ClaimTransformer": "varco_core.jwt",
    "CompositeHealthCheck": "varco_core.health",
    "CompositeStrategy": "varco_core.cache",
    "CompositeValidator": "varco_core.validation",
    "ConnectionSettings": "varco_core.connection",
    "CorrelationIdFilter": "varco_core.tracing",
    "CounterConfig": "varco_core.observability",
    "CreateDTO": "varco_core.dto",
    "CrossCheckMode": "varco_core.tenancy",
    "CrossTenantAccessError": "varco_core.tenancy",
    "DTOSet": "varco_core.dto.factory",
    "DeadLetterEntry": "varco_core.event",
    "DestructiveOperationRefused": "varco_core.tenancy",
    "DomainMigrator": "varco_core.migrator",
    "DomainModel": "varco_core.model",
    "DomainModelRegistry": "varco_core.registry",
    "DomainModelValidator": "varco_core.validation",
    "DynamicTenantUoWProvider": "varco_core.tenancy",
    "EntityCreatedEvent": "varco_core.event",
    "EntityDeletedEvent": "varco_core.event",
    "EntityEvent": "varco_core.event",
    "EntityUpdatedEvent": "varco_core.event",
    "ErrorCode": "varco_core.exception.codes",
    "ErrorEnvelopeSettings": "varco_core.exception.settings",
    "ErrorMessage": "varco_core.exception.http",
    "ErrorPolicy": "varco_core.event",
    "Event": "varco_core.event",
    "EventBusSettings": "varco_core.event",
    "EventConsumer": "varco_core.event",
    "EventMiddleware": "varco_core.event",
    "ExplicitStrategy": "varco_core.cache",
    "ExternalTenantProvisioner": "varco_core.tenancy",
    "FastrestErrorCodes": "varco_core.exception.codes",
    "FieldError": "varco_core.exception.http",
    "GrantBasedAuthorizer": "varco_core.auth.helpers",
    "HealthCheck": "varco_core.health",
    "HealthResult": "varco_core.health",
    "HealthStatus": "varco_core.health",
    "HistogramConfig": "varco_core.observability",
    "IDENTITY": "varco_core.jwt",
    "IdentityClaimTransformer": "varco_core.jwt",
    "InMemoryCache": "varco_core.cache",
    "InMemoryDeadLetterQueue": "varco_core.event",
    "InMemoryEventBus": "varco_core.event",
    "InMemoryMigrator": "varco_core.migration",
    "InvalidationStrategy": "varco_core.cache",
    "IrreversibleMigrationError": "varco_core.migration",
    "IssuerNotFoundError": "varco_core.authority",
    "Job": "varco_core.job",
    "JobStatus": "varco_core.job",
    "JsonEventSerializer": "varco_core.event",
    "JsonSerializer": "varco_core.serialization",
    "JsonWebKey": "varco_core.jwk",
    "JsonWebKeySet": "varco_core.jwk",
    "JsonWebToken": "varco_core.jwt",
    "JwkBuilder": "varco_core.jwk",
    "JwtAuthority": "varco_core.authority",
    "JwtBuilder": "varco_core.jwt",
    "JwtClaimTenantSource": "varco_core.tenancy",
    "JwtException": "varco_core.jwt",
    "JwtParser": "varco_core.jwt",
    "JwtUtil": "varco_core.jwt",
    "JwtVerificationSettings": "varco_core.jwt",
    "KeyLoadError": "varco_core.authority",
    "LayeredCache": "varco_core.cache",
    "LegacyTenantSource": "varco_core.tenancy",
    "MappingClaimTransformer": "varco_core.jwt",
    "MembershipDecision": "varco_core.tenancy",
    "MessageResolver": "varco_core.exception.http",
    "Metric": "varco_core.observability",
    "MetricKind": "varco_core.observability",
    "MigrationBackendUnavailable": "varco_core.migration",
    "MigrationError": "varco_core.migrator",
    "MigrationLockTimeout": "varco_core.migration",
    "MigrationReport": "varco_core.migration",
    "MigrationSettings": "varco_core.migration",
    "MissingClaimPolicy": "varco_core.tenancy",
    "MultiKeyAuthority": "varco_core.authority",
    "NoOpCache": "varco_core.cache",
    "NoOpSerializer": "varco_core.serialization",
    "NoopEventProducer": "varco_core.event",
    "NullTenantDefaults": "varco_core.context",
    "NullTenantMembership": "varco_core.tenancy",
    "OAuth2Config": "varco_core.connection",
    "Operation": "varco_core.query.type",
    "OtelConfig": "varco_core.observability",
    "OtelConfiguration": "varco_core.observability",
    "OwnershipAuthorizer": "varco_core.auth.helpers",
    "PROFILE_METADATA_KEY": "varco_core.jwt",
    "PageCursor": "varco_core.dto.pagination",
    "PagedReadDTO": "varco_core.dto.pagination",
    "PendingMigrationsError": "varco_core.migration",
    "QueryBuilder": "varco_core.query.builder",
    "QueryParams": "varco_core.query.params",
    "QueryParser": "varco_core.query.parser",
    "ReadDTO": "varco_core.dto",
    "RepositoryProvider": "varco_core.providers",
    "RequestContext": "varco_core.context",
    "Resolved": "varco_core.context",
    "RetryExhaustedError": "varco_core.resilience",
    "RetryPolicy": "varco_core.resilience",
    "Revision": "varco_core.migration",
    "RoleBasedAuthorizer": "varco_core.auth.helpers",
    "SSLConfig": "varco_core.connection",
    "SYSTEM_ISSUER": "varco_core.jwt",
    "SaslConfig": "varco_core.connection",
    "SchemaMigrationError": "varco_core.migration",
    "SchemaMigrationPlan": "varco_core.migration",
    "Serializer": "varco_core.serialization",
    "ServiceProtocol": "varco_core.service.types",
    "SoftDeleteAuditedDomainModel": "varco_core.model",
    "SoftDeleteDomainModel": "varco_core.model",
    "SoftDeleteMixin": "varco_core.model",
    "SoftDeleteService": "varco_core.service.soft_delete",
    "SortCursorField": "varco_core.dto.pagination",
    "SortField": "varco_core.query.type",
    "SortOrder": "varco_core.query.type",
    "SpanConfig": "varco_core.observability",
    "StaleEntityError": "varco_core.exception.repository",
    "StaticTenantCatalog": "varco_core.tenancy",
    "StaticTenantDefaults": "varco_core.context",
    "SubdomainTenantSource": "varco_core.tenancy",
    "Subscription": "varco_core.event",
    "TCreateDTO": "varco_core.dto",
    "TReadDTO": "varco_core.dto",
    "TTLStrategy": "varco_core.cache",
    "TUpdateDTO": "varco_core.dto",
    "TaggedStrategy": "varco_core.cache",
    "TenancySettings": "varco_core.tenancy",
    "TenantAuditedDomainModel": "varco_core.model",
    "TenantAwareService": "varco_core.service.tenant",
    "TenantClaim": "varco_core.tenancy",
    "TenantDefaultsProvider": "varco_core.context",
    "TenantDescriptor": "varco_core.tenancy",
    "TenantDomainModel": "varco_core.model",
    "TenantIsolation": "varco_core.tenancy",
    "TenantIsolationError": "varco_core.tenancy",
    "TenantLocalizationDefaults": "varco_core.context",
    "TenantMembershipError": "varco_core.tenancy",
    "TenantMembershipSettings": "varco_core.tenancy",
    "TenantMixin": "varco_core.model",
    "TenantNotFoundError": "varco_core.tenancy",
    "TenantProvenance": "varco_core.tenancy",
    "TenantProvenancePosture": "varco_core.tenancy",
    "TenantProvenanceSettings": "varco_core.tenancy",
    "TenantRequest": "varco_core.tenancy",
    "TenantResourcePool": "varco_core.tenancy",
    "TenantScope": "varco_core.tenancy",
    "TenantSource": "varco_core.tenancy",
    "TenantSourceChain": "varco_core.tenancy",
    "TenantStatus": "varco_core.tenancy",
    "TenantTrust": "varco_core.tenancy",
    "TenantUoWProvider": "varco_core.service.tenant",
    "TenantVersionedDomainModel": "varco_core.model",
    "TokenProfile": "varco_core.jwt",
    "TokenProfileError": "varco_core.jwt",
    "TokenProfileRegistry": "varco_core.jwt",
    "TracingServiceMixin": "varco_core.observability",
    "TrustedIssuerEntry": "varco_core.authority",
    "TrustedIssuerRegistry": "varco_core.authority",
    "UnknownKidError": "varco_core.authority",
    "UpdateDTO": "varco_core.dto",
    "UpdateOperation": "varco_core.dto",
    "VALID": "varco_core.validation",
    "ValidationError": "varco_core.validation",
    "ValidationResult": "varco_core.validation",
    "Validator": "varco_core.validation",
    "ValidatorServiceMixin": "varco_core.service.validation",
    "ValueShape": "varco_core.jwt",
    "VarcoErrorCodes": "varco_core.exception.codes",
    "VarcoSettings": "varco_core.config",
    "VersionedDomainModel": "varco_core.model",
    "assert_tenant_matches": "varco_core.tenancy",
    "auth_context_from_snapshot": "varco_core.job",
    "auth_context_to_snapshot": "varco_core.job",
    "build_tenant_source_chain": "varco_core.tenancy",
    "cached": "varco_core.cache",
    "cast_raw": "varco_core.model",
    "circuit_breaker": "varco_core.resilience",
    "configure_claim_transforms": "varco_core.jwt",
    "configure_token_profiles": "varco_core.jwt",
    "correlation_context": "varco_core.tracing",
    "counter": "varco_core.observability",
    "create_counter": "varco_core.observability",
    "create_histogram": "varco_core.observability",
    "create_span": "varco_core.observability",
    "current_correlation_id": "varco_core.tracing",
    "current_locale": "varco_core.context",
    "current_request_context": "varco_core.context",
    "current_tenant": "varco_core.service.tenant",
    "current_tenant_provenance": "varco_core.tenancy",
    "current_timezone": "varco_core.context",
    "deprecated": "varco_core.deprecation",
    "deprecated_alias": "varco_core.deprecation",
    "enable_tenant_membership": "varco_core.tenancy",
    "error_code_for": "varco_core.exception.http",
    "error_message_for": "varco_core.exception.http",
    "generate_correlation_id": "varco_core.tracing",
    "generate_dtos": "varco_core.dto.factory",
    "histogram": "varco_core.observability",
    "inspect_tenant_provenance": "varco_core.tenancy",
    "listen": "varco_core.event",
    "paged_response": "varco_core.dto.pagination",
    "provenance_context": "varco_core.tenancy",
    "read_claim": "varco_core.jwt",
    "register": "varco_core.registry",
    "register_error_code": "varco_core.exception.http",
    "register_gauge": "varco_core.observability",
    "request_context": "varco_core.context",
    "reset_claim_transforms": "varco_core.jwt",
    "reset_token_profiles": "varco_core.jwt",
    "resolve_claim_transformer": "varco_core.jwt",
    "resolve_precedence": "varco_core.context",
    "resolve_token_profile": "varco_core.jwt",
    "retry": "varco_core.resilience",
    "span": "varco_core.observability",
    "tenant_context": "varco_core.service.tenant",
    "timeout": "varco_core.resilience",
}

# Two names that ``varco_core`` has always bound but has deliberately never
# listed in ``__all__``: ``varco_core.migrator``'s ``MigrationPlan`` and
# ``StepSpec`` (see the AB-2 note in the ``TYPE_CHECKING`` block above — the
# *schema*-migration pair was renamed to ``SchemaMigration*`` precisely so
# these two could keep the unqualified names). They are excluded from
# ``__all__``, from ``__dir__()`` and from the ``TYPE_CHECKING`` block, exactly
# as before, but they must keep *resolving* —
# ``varco_core/tests/test_deprecated_aliases.py:174`` asserts
# ``varco_core.MigrationPlan is DomainMigrationPlan``. A separate map, rather
# than an ``_LAZY`` entry, because ``_LAZY`` is contractually equal to
# ``__all__`` minus ``_EAGER``.
_LAZY_UNEXPORTED: Final[dict[str, str]] = {
    "MigrationPlan": "varco_core.migrator",
    "StepSpec": "varco_core.migrator",
}


def __getattr__(name: str) -> Any:
    """Resolve an exported name by importing its defining submodule (PEP 562).

    Called by the interpreter only for names not already in this module's
    ``globals()`` — so exactly once per name per process, after which the
    resolved object is a normal module global and this function is never
    consulted for it again.

    Args:
        name: The attribute being looked up on the ``varco_core`` module.

    Returns:
        The object of that name defined by the submodule recorded in
        ``_LAZY`` (or ``_LAZY_UNEXPORTED``), identical to what
        ``from varco_core.<submodule> import <name>`` yields.

    Raises:
        AttributeError: If ``name`` is not an exported ``varco_core`` symbol.
            The message names both the module and the attribute, matching
            CPython's own wording for a missing module attribute.
        ImportError: Propagated unchanged if the defining submodule cannot be
            imported — the ❌ accepted above. It surfaces here rather than at
            ``import varco_core``.

    Example:
        >>> import varco_core
        >>> varco_core.DomainModel  # imports varco_core.model on first access
        <class 'varco_core.model.DomainModel'>

    Async safety:
        Not applicable — module import is protected by CPython's per-module
        import lock, and the ``globals()`` write is a single bytecode under the
        GIL. A concurrent double-resolve would bind the same object twice,
        which is harmless.
    """
    module = _LAZY.get(name) or _LAZY_UNEXPORTED.get(name)
    if module is None:
        raise AttributeError(f"module 'varco_core' has no attribute {name!r}")
    value = getattr(importlib.import_module(module), name)
    # Cache: __getattr__ is only consulted for names absent from globals().
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    """List the module's public surface for ``dir()`` and tab completion.

    Without this, ``dir(varco_core)`` would report only the handful of names
    that happen to have been resolved already, which is both useless and
    non-deterministic.

    Returns:
        ``sorted(__all__)`` — the same 235 names as before this module became
        lazy. ``_LAZY_UNEXPORTED``'s two legacy names are deliberately absent,
        as they were absent from ``dir()`` before too.

    Example:
        >>> import varco_core
        >>> "DomainModel" in dir(varco_core)
        True
    """
    return sorted(__all__)


__all__ = [
    # ── Ambient request context (Plan 011 X1) ──────────────────────────────────
    "AmbientVar",
    "Resolved",
    "resolve_precedence",
    "RequestContext",
    "current_request_context",
    "current_locale",
    "current_timezone",
    "request_context",
    "TenantDefaultsProvider",
    "TenantLocalizationDefaults",
    "NullTenantDefaults",
    "StaticTenantDefaults",
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
    # ── Deprecation mechanism (Plan 022 / §D-DEP) ──────────────────────────────
    "deprecated",
    "deprecated_alias",
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
    # ── Multitenancy — isolation strategies & control plane (Plan 007) ──────────
    "TenantIsolation",
    "TenantScope",
    "TenantStatus",
    "TenancySettings",
    "TenantDescriptor",
    "AbstractTenantCatalog",
    "StaticTenantCatalog",
    "TenantNotFoundError",
    "TenantIsolationError",
    "TenantResourcePool",
    "DynamicTenantUoWProvider",
    "AbstractTenantProvisioner",
    "ExternalTenantProvisioner",
    "DestructiveOperationRefused",
    # ── Tenant identity provenance & membership (Plan 033 / S6, S5) ─────────────
    "TenantTrust",
    "TenantRequest",
    "TenantClaim",
    "TenantSource",
    "TenantSourceChain",
    "CrossCheckMode",
    "TenantProvenance",
    "JwtClaimTenantSource",
    "SubdomainTenantSource",
    "LegacyTenantSource",
    "TenantProvenanceSettings",
    "build_tenant_source_chain",
    "current_tenant_provenance",
    "provenance_context",
    "assert_tenant_matches",
    "CrossTenantAccessError",
    "MembershipDecision",
    "AbstractTenantMembership",
    "NullTenantMembership",
    "ClaimTenantMembership",
    "MissingClaimPolicy",
    "TenantMembershipError",
    "TenantMembershipSettings",
    "enable_tenant_membership",
    "TenantProvenancePosture",
    "inspect_tenant_provenance",
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
    "JsonEventSerializer",
    "InMemoryDeadLetterQueue",
    "InMemoryEventBus",
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
    "VarcoErrorCodes",
    "ErrorMessage",
    "FieldError",
    "MessageResolver",
    "ErrorEnvelopeSettings",
    "error_code_for",
    "error_message_for",
    "register_error_code",
    # ── JWT layer ───────────────────────────────────────────────────────────────
    "SYSTEM_ISSUER",
    "JsonWebToken",
    "JwtBuilder",
    "JwtParser",
    "JwtUtil",
    # ── JWT claim transformation + token profiles (Plan 002) ────────────────────
    "JwtException",
    "ClaimTransformError",
    "TokenProfileError",
    "JwtVerificationSettings",
    "PROFILE_METADATA_KEY",
    "TokenProfile",
    "TokenProfileRegistry",
    "configure_token_profiles",
    "reset_token_profiles",
    "resolve_token_profile",
    "ClaimPath",
    "read_claim",
    "ValueShape",
    "CanonicalClaim",
    "ClaimMapping",
    "ClaimRule",
    "ClaimTransformer",
    "IdentityClaimTransformer",
    "IDENTITY",
    "MappingClaimTransformer",
    "configure_claim_transforms",
    "reset_claim_transforms",
    "resolve_claim_transformer",
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
    # ── Schema migrations (varco_core.migration) ────────────────────────────────
    # Renamed from MigrationError/MigrationPlan in 3.0.0 (Plan 022 / AB-2) so
    # they no longer collide with varco_core.migrator's domain pair — see the
    # NOTE above the `from varco_core.migration import (...)` block.
    "SchemaMigrationError",
    "SchemaMigrationPlan",
    "AbstractMigrator",
    "MigrationReport",
    "Revision",
    "MigrationSettings",
    "PendingMigrationsError",
    "MigrationLockTimeout",
    "IrreversibleMigrationError",
    "MigrationBackendUnavailable",
    "InMemoryMigrator",
]
