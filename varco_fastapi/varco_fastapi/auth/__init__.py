"""
varco_fastapi.auth
==================
Server-side and client-side authentication for FastAPI.

Server-side (incoming requests):
    ``AbstractServerAuth``    — base ABC for FastAPI callable dependencies
    ``JwtBearerAuth``         — verify Bearer JWT via TrustedIssuerRegistry
    ``ApiKeyAuth``            — verify X-API-Key header or ?api_key= param
    ``PassthroughAuth``       — decode JWT claims WITHOUT verifying signature
    ``AnonymousAuth``         — always returns anonymous AuthContext
    ``CompositeServerAuth``   — try each strategy in order; first success wins
    ``WebSocketAuth``         — auth for WebSocket upgrade (header/protocol/query)

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
