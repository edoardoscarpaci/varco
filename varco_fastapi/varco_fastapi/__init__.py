"""
varco_fastapi — FastAPI adapter for the varco framework.

Public surface::

    # Router layer
    from varco_fastapi import (
        VarcoRouter,
        RouterMixin,
        AllRouteMixin,
        CRUDRouter,
        ReadOnlyRouter,
        WriteRouter,
        NoDeleteRouter,
        CreateMixin, ReadMixin, UpdateMixin, PatchMixin, DeleteMixin, ListMixin,
        HttpQueryParams,
        AsyncModeParams,
        HealthRouter,
        route, ws_route, sse_route,
        introspect_routes,
        ResolvedRoute,
    )

    # Auth layer
    from varco_fastapi import (
        AbstractServerAuth,
        JwtBearerAuth,
        ApiKeyAuth,
        PassthroughAuth,
        AnonymousAuth,
        WebSocketAuth,
        AbstractClientAuth,
        JwtClientAuth,
        TrustStore,
    )

    # Middleware
    from varco_fastapi import (
        ErrorMiddleware,
        RequestContextMiddleware,
        RequestLoggingMiddleware,
        TracingMiddleware,
        CORSConfig,
        install_cors,
    )

    # Job layer
    from varco_fastapi import (
        InMemoryJobStore,
        JobRunner,
        JobPoller,
        JobAcceptedResponse,
        JobStatusResponse,
        JobProgressEvent,
        job_progress,
    )

    # Client layer
    from varco_fastapi import (
        AsyncVarcoClient,
        VarcoClient,
        ClientProfile,
        ClientConfigurator,
        ClientProtocol,
        SyncVarcoClient,
        JobHandle,
        JobFailedError,
    )

    # DI
    from varco_fastapi import VarcoFastAPIModule, bind_clients

    # Context
    from varco_fastapi import (
        auth_context_var,
        request_token_var,
        get_auth_context,
        get_auth_context_or_none,
    )
"""

from __future__ import annotations

# ── App factory ───────────────────────────────────────────────────────────────
from varco_fastapi.app import create_varco_app
from varco_fastapi.auth.client_auth import (
    AbstractClientAuth,
    JwtClientAuth,
)
from varco_fastapi.auth.guard import (
    RouteGuard,
    allow_anonymous,
    require_grant,
    require_predicate,
    require_roles,
    require_scopes,
)

# ── Auth — server side ────────────────────────────────────────────────────────
from varco_fastapi.auth.posture import AuthPostureReport, inspect_auth_posture
from varco_fastapi.auth.server_auth import (
    AbstractServerAuth,
    AnonymousAuth,
    ApiKeyAuth,
    JwtBearerAuth,
    PassthroughAuth,
    WebSocketAuth,
)
from varco_fastapi.auth.trust_store import TrustStore

# ── Client layer ──────────────────────────────────────────────────────────────
from varco_fastapi.client import (
    AbstractClientMiddleware,
    AsyncVarcoClient,
    AuthForwardMiddleware,
    ClientProfile,
    ClientProtocol,
    CorrelationIdMiddleware,
    HeadersMiddleware,
    JobFailedError,
    JobHandle,
    JwtMiddleware,
    LoggingMiddleware,
    OTelClientMiddleware,
    PreparedRequest,
    RetryMiddleware,
    SyncVarcoClient,
    TimeoutMiddleware,
    VarcoClient,
)

# ClientConfigurator is demoted out of varco_fastapi.client's front door
# (Plan 009, Phase 3 / C1) but stays available at the varco_fastapi
# top-level for backward compatibility — import it from its advanced shelf.
from varco_fastapi.client.advanced import ClientConfigurator
from varco_fastapi.client.front_door import client_class_for, client_for

# ── Composite / all-in-one deployment ─────────────────────────────────────────
from varco_fastapi.composite import (
    CompositeLifespan,
    ServiceMount,
    build_service,
    create_composite_app,
)

# ── Context ───────────────────────────────────────────────────────────────────
from varco_fastapi.context import (
    JwtContext,
    RequestContext,
    auth_context_var,
    get_auth_context,
    get_auth_context_or_none,
    get_jwt_context,
    get_request_context,
    get_request_id,
    get_request_token,
    request_id_var,
    request_token_var,
)

# ── DI ────────────────────────────────────────────────────────────────────────
from varco_fastapi.di import VarcoFastAPIModule, bind_clients, bind_clients_from

# ── Exceptions ────────────────────────────────────────────────────────────────
from varco_fastapi.exceptions import add_exception_handlers

# ── Job layer ─────────────────────────────────────────────────────────────────
from varco_fastapi.job import (
    InMemoryJobStore,
    JobAcceptedResponse,
    JobPoller,
    JobProgressEvent,
    JobRunner,
    JobStatusResponse,
    job_progress,
)

# ── Lifecycle ─────────────────────────────────────────────────────────────────
from varco_fastapi.lifespan import VarcoLifespan
from varco_fastapi.middleware.cors import CORSConfig, install_cors

# ── Middleware ────────────────────────────────────────────────────────────────
from varco_fastapi.middleware.error import ErrorMiddleware
from varco_fastapi.middleware.introspect import HttpEdgeFinding, HttpEdgePosture, inspect_http_edge
from varco_fastapi.middleware.logging import RequestLoggingMiddleware
from varco_fastapi.middleware.metrics import MetricsMiddleware
from varco_fastapi.middleware.request_context import RequestContextMiddleware
from varco_fastapi.middleware.tracing import TracingMiddleware
from varco_fastapi.migrate import MigrationLifecycle

# ── Router layer ──────────────────────────────────────────────────────────────
from varco_fastapi.router.base import (
    AsyncModeParams,
    HttpQueryParams,
    RouterMixin,
    VarcoRouter,
)
from varco_fastapi.router.crud import VarcoCRUDRouter
from varco_fastapi.router.endpoint import route, sse_route, ws_route
from varco_fastapi.router.health import HealthRouter
from varco_fastapi.router.introspection import ResolvedRoute, introspect_routes

# ── MCP adapter ───────────────────────────────────────────────────────────────
from varco_fastapi.router.mcp import (
    MCPAdapter,
    MCPAuthMiddleware,
    MCPToolDefinition,
    bind_mcp_adapter,
)
from varco_fastapi.router.metrics import MetricsRouter
from varco_fastapi.router.mixins import (
    CreateMixin,
    DeleteMixin,
    ListMixin,
    PatchMixin,
    ReadMixin,
    UpdateMixin,
)
from varco_fastapi.router.pagination import (
    PagedReadDTO,
    add_pagination_headers,
    paged_response,
)
from varco_fastapi.router.presets import (
    AllRouteMixin,
    CRUDRouter,
    GenericRouter,
    NoDeleteRouter,
    ReadOnlyRouter,
    WriteRouter,
)

# ── Skill / A2A adapter ───────────────────────────────────────────────────────
from varco_fastapi.router.skill import SkillAdapter, SkillDefinition, bind_skill_adapter

# ── Validation ────────────────────────────────────────────────────────────────
from varco_fastapi.validation import (
    ConfigurationError,
    validate_container_bindings,
    validate_router_class,
)

__all__ = [
    # Context
    "RequestContext",
    "JwtContext",
    "get_request_context",
    "get_jwt_context",
    "auth_context_var",
    "request_token_var",
    "request_id_var",
    "get_auth_context",
    "get_auth_context_or_none",
    "get_request_token",
    "get_request_id",
    # Auth
    "AbstractServerAuth",
    "JwtBearerAuth",
    "ApiKeyAuth",
    "PassthroughAuth",
    "AnonymousAuth",
    "WebSocketAuth",
    "AuthPostureReport",
    "inspect_auth_posture",
    "AbstractClientAuth",
    "JwtClientAuth",
    "TrustStore",
    # Route-level authorization guards
    "RouteGuard",
    "require_scopes",
    "require_roles",
    "require_grant",
    "require_predicate",
    "allow_anonymous",
    # Middleware
    "ErrorMiddleware",
    "MetricsMiddleware",
    "RequestContextMiddleware",
    "RequestLoggingMiddleware",
    "TracingMiddleware",
    "CORSConfig",
    "install_cors",
    # Plan 035 / §D-seam — cross-plan contract with Plan 036, top-level too
    "HttpEdgeFinding",
    "HttpEdgePosture",
    "inspect_http_edge",
    # Router
    "VarcoRouter",
    "GenericRouter",
    "VarcoCRUDRouter",
    "RouterMixin",
    "AllRouteMixin",
    "CRUDRouter",
    "ReadOnlyRouter",
    "WriteRouter",
    "NoDeleteRouter",
    "CreateMixin",
    "ReadMixin",
    "UpdateMixin",
    "PatchMixin",
    "DeleteMixin",
    "ListMixin",
    "HttpQueryParams",
    "AsyncModeParams",
    "HealthRouter",
    "MetricsRouter",
    "route",
    "ws_route",
    "sse_route",
    "ResolvedRoute",
    "introspect_routes",
    "PagedReadDTO",
    "paged_response",
    "add_pagination_headers",
    # Job
    "InMemoryJobStore",
    "JobRunner",
    "JobPoller",
    "JobAcceptedResponse",
    "JobStatusResponse",
    "JobProgressEvent",
    "job_progress",
    # Client
    "AsyncVarcoClient",
    "VarcoClient",
    "ClientProfile",
    "ClientConfigurator",
    "ClientProtocol",
    "SyncVarcoClient",
    "JobHandle",
    "JobFailedError",
    "PreparedRequest",
    "AbstractClientMiddleware",
    "HeadersMiddleware",
    "CorrelationIdMiddleware",
    "AuthForwardMiddleware",
    "JwtMiddleware",
    "RetryMiddleware",
    "LoggingMiddleware",
    "TimeoutMiddleware",
    "OTelClientMiddleware",
    # Lifecycle
    "VarcoLifespan",
    "MigrationLifecycle",
    # Exceptions
    "add_exception_handlers",
    # DI
    "VarcoFastAPIModule",
    "bind_clients",
    "bind_clients_from",
    "client_for",
    "client_class_for",
    # App factory
    "create_varco_app",
    # ── Composite / all-in-one deployment ──
    "CompositeLifespan",
    "ServiceMount",
    "build_service",
    "create_composite_app",
    # Validation
    "ConfigurationError",
    "validate_router_class",
    "validate_container_bindings",
    # MCP adapter
    "MCPAdapter",
    "MCPAuthMiddleware",
    "MCPToolDefinition",
    "bind_mcp_adapter",
    # Skill / A2A adapter
    "SkillAdapter",
    "SkillDefinition",
    "bind_skill_adapter",
]
