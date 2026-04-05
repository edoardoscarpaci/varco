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

# ── Auth — server side ────────────────────────────────────────────────────────
from varco_fastapi.auth.server_auth import (
    AbstractServerAuth,
    AnonymousAuth,
    ApiKeyAuth,
    JwtBearerAuth,
    PassthroughAuth,
    WebSocketAuth,
)
from varco_fastapi.auth.client_auth import (
    AbstractClientAuth,
    JwtClientAuth,
)
from varco_fastapi.auth.trust_store import TrustStore

# ── Middleware ────────────────────────────────────────────────────────────────
from varco_fastapi.middleware.error import ErrorMiddleware
from varco_fastapi.middleware.request_context import RequestContextMiddleware
from varco_fastapi.middleware.logging import RequestLoggingMiddleware
from varco_fastapi.middleware.tracing import TracingMiddleware
from varco_fastapi.middleware.cors import CORSConfig, install_cors

# ── Router layer ──────────────────────────────────────────────────────────────
from varco_fastapi.router.base import (
    AsyncModeParams,
    HttpQueryParams,
    RouterMixin,
    VarcoRouter,
)
from varco_fastapi.router.crud import VarcoCRUDRouter
from varco_fastapi.router.mixins import (
    CreateMixin,
    DeleteMixin,
    ListMixin,
    PatchMixin,
    ReadMixin,
    UpdateMixin,
)
from varco_fastapi.router.presets import (
    AllRouteMixin,
    CRUDRouter,
    NoDeleteRouter,
    ReadOnlyRouter,
    WriteRouter,
)
from varco_fastapi.router.endpoint import route, ws_route, sse_route
from varco_fastapi.router.health import HealthRouter
from varco_fastapi.router.introspection import ResolvedRoute, introspect_routes
from varco_fastapi.router.pagination import (
    PagedReadDTO,
    add_pagination_headers,
    paged_response,
)

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

# ── Client layer ──────────────────────────────────────────────────────────────
from varco_fastapi.client import (
    AbstractClientMiddleware,
    AsyncVarcoClient,
    AuthForwardMiddleware,
    ClientConfigurator,
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

# ── Lifecycle ─────────────────────────────────────────────────────────────────
from varco_fastapi.lifespan import VarcoLifespan

# ── Exceptions ────────────────────────────────────────────────────────────────
from varco_fastapi.exceptions import add_exception_handlers

# ── DI ────────────────────────────────────────────────────────────────────────
from varco_fastapi.di import VarcoFastAPIModule, bind_clients

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
    "AbstractClientAuth",
    "JwtClientAuth",
    "TrustStore",
    # Middleware
    "ErrorMiddleware",
    "RequestContextMiddleware",
    "RequestLoggingMiddleware",
    "TracingMiddleware",
    "CORSConfig",
    "install_cors",
    # Router
    "VarcoRouter",
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
    # Exceptions
    "add_exception_handlers",
    # DI
    "VarcoFastAPIModule",
    "bind_clients",
]
