"""
varco_fastapi.auth
==================
Server-side and client-side authentication for FastAPI.

Server-side (incoming requests):
    ``AbstractServerAuth``    — base ABC for FastAPI callable dependencies
    ``JwtBearerAuth``         — verify Bearer JWT via TrustedIssuerRegistry
    ``ApiKeyAuth``            — verify X-API-Key header (query param opt-in via param=)
    ``PassthroughAuth``       — decode JWT claims WITHOUT verifying signature
    ``AnonymousAuth``         — always returns anonymous AuthContext
    ``CompositeServerAuth``   — try each strategy in order; first success wins
    ``WebSocketAuth``         — auth for WebSocket upgrade (header/protocol/query)

Posture introspection (Plan 034 / Phase 4, §D-034-seam — facts only, Plan
036 owns the judgement):
    ``AuthPostureReport``     — frozen report walked from an auth tree
    ``inspect_auth_posture``  — pure function producing that report

Route-level authorization guards (service-free routers):
    ``RouteGuard``            — immutable declarative guard for @route handlers
    ``require_scopes``        — guard requiring OAuth scope(s)
    ``require_roles``         — guard requiring role name(s)
    ``require_grant``         — guard requiring ctx.can(action, resource_key)
    ``require_predicate``     — guard backed by an arbitrary callable
    ``require_token_profile`` — guard requiring a resolved JWT token profile
    ``allow_anonymous``       — guard that passes anonymous callers through

Client-side (outbound requests):
    ``AbstractClientAuth``    — base ABC for header injection
    ``BearerTokenAuth``       — static Bearer token
    ``JwtClientAuth``         — fresh JWT from JwtAuthority (cached until near expiry)
    ``ApiKeyClientAuth``      — static API key header
    ``ForwardingClientAuth``  — forward originating request's token downstream
    ``CompositeClientAuth``   — merge multiple auth strategies

TLS:
    ``TrustStore``            — CA bundle + mTLS config for outbound connections
"""

from varco_fastapi.auth.client_auth import (
    AbstractClientAuth,
    ApiKeyClientAuth,
    BearerTokenAuth,
    CompositeClientAuth,
    ForwardingClientAuth,
    JwtClientAuth,
)
from varco_fastapi.auth.guard import (
    RouteGuard,
    allow_anonymous,
    require_grant,
    require_predicate,
    require_roles,
    require_scopes,
    require_token_profile,
)
from varco_fastapi.auth.posture import AuthPostureReport, inspect_auth_posture
from varco_fastapi.auth.server_auth import (
    AbstractServerAuth,
    AnonymousAuth,
    ApiKeyAuth,
    CompositeServerAuth,
    JwtBearerAuth,
    PassthroughAuth,
    WebSocketAuth,
)
from varco_fastapi.auth.trust_store import TrustStore

__all__ = [
    # Server-side
    "AbstractServerAuth",
    "JwtBearerAuth",
    "ApiKeyAuth",
    "PassthroughAuth",
    "AnonymousAuth",
    "CompositeServerAuth",
    "WebSocketAuth",
    # Posture introspection (Plan 034 / Phase 4, §D-034-seam)
    "AuthPostureReport",
    "inspect_auth_posture",
    # Route-level authorization guards
    "RouteGuard",
    "require_scopes",
    "require_roles",
    "require_grant",
    "require_predicate",
    "require_token_profile",
    "allow_anonymous",
    # Client-side
    "AbstractClientAuth",
    "BearerTokenAuth",
    "JwtClientAuth",
    "ApiKeyClientAuth",
    "ForwardingClientAuth",
    "CompositeClientAuth",
    # TLS
    "TrustStore",
]
