"""
example.auth
============
Authentication module for the Post API.

Provides:
- ``build_jwt_authority()``  — constructs a ``JwtAuthority`` from env or generates
  a fresh RSA-2048 key pair at startup (dev-mode quickstart).
- ``DEMO_USERS``             — in-memory user registry (two hardcoded users).
- ``AuthRouter``             — FastAPI router exposing ``POST /auth/login`` and
  ``GET /auth/me`` so clients can obtain and inspect JWT Bearer tokens.

JWT Flow
--------
::

    POST /auth/login  →  {access_token: "eyJ…"}   (HS256 or RS256 JWT)
    Authorization: Bearer eyJ…   →   AuthContext in every subsequent request
                                      (set by RequestContextMiddleware)

Demo users (username / password / roles)
-----------------------------------------
    alice / alice123 / admin    — full CRUD on all posts
    bob   / bob123   / editor   — create + read; can update/delete OWN posts only

DESIGN: in-memory user store over a real user table
    ✅ Zero extra dependencies for the quickstart — no user ORM model needed.
    ✅ Demonstrates the JWT auth pipeline end-to-end without auth service overhead.
    ❌ Not suitable for production — passwords are plaintext, no hashing.
    Swap for a real ``UserService`` + bcrypt/argon2 in a production app.

DESIGN: RSA-2048 auto-generation over HMAC (HS256)
    ✅ Compatible with ``TrustedIssuerRegistry`` / JWKS endpoints — asymmetric
       keys can be exposed publicly for multi-service verification.
    ✅ Demonstrates the full varco authority pipeline (JwtAuthority, registry).
    ❌ Key is regenerated on every restart in dev mode — existing tokens are
       invalidated.  Set ``VARCO_JWT_PRIVATE_KEY_PEM`` for persistence.

Thread safety:  ✅ ``build_jwt_authority()`` is called once at startup.
Async safety:   ✅ Login handler is ``async def``; token signing is CPU-only.
"""

from __future__ import annotations

import logging
import os
from datetime import timedelta

from fastapi import APIRouter, HTTPException, Request, status
from pydantic import BaseModel

from varco_core.auth.base import AuthContext
from varco_core.authority.jwt_authority import JwtAuthority

_logger = logging.getLogger(__name__)

# ── Demo user registry ────────────────────────────────────────────────────────
# Plaintext passwords — acceptable for a quickstart demo, not for production.
# Replace with a hashed credential store (e.g. bcrypt, argon2) in real apps.
#
# Structure: username → {user_id, password, roles}
# user_id values are stable fake UUIDs so ``author_id`` in posts is realistic.
DEMO_USERS: dict[str, dict[str, str | list[str]]] = {
    "alice": {
        "user_id": "00000000-0000-0000-0000-000000000001",
        "password": "alice123",
        # admin role: full CRUD on all posts (PostAuthorizer honours this)
        "roles": ["admin"],
    },
    "bob": {
        "user_id": "00000000-0000-0000-0000-000000000002",
        "password": "bob123",
        # editor role: create + read + update/delete OWN posts only
        "roles": ["editor"],
    },
}

# JWT lifetime — 1 hour is reasonable for a dev quickstart.
# Shorten to 15 min in production and add refresh-token rotation.
_TOKEN_LIFETIME = timedelta(hours=1)

# Issuer string — must match TrustedIssuerRegistry registration in app.py
# so that verify() succeeds and tokens are not rejected as unknown issuers.
ISSUER = "varco-example"

# Key ID — stable across restarts only when a PEM is supplied via env var.
KID = "varco-example-key-v1"


# ── JWT authority factory ─────────────────────────────────────────────────────


def build_jwt_authority() -> JwtAuthority:
    """
    Build a ``JwtAuthority`` suitable for signing and verifying tokens.

    Priority order:
    1. ``VARCO_JWT_PRIVATE_KEY_PEM`` env var (PEM-encoded RSA or EC private key).
    2. Auto-generated RSA-2048 key pair (dev quickstart mode).

    Dev mode auto-generation
    ~~~~~~~~~~~~~~~~~~~~~~~~
    When no PEM is configured, a fresh RSA-2048 key pair is generated every
    time the app starts.  This means **all tokens issued before a restart
    become invalid** — clients must re-login.  This is intentional: dev mode
    prioritises zero-config over token persistence.

    Production
    ~~~~~~~~~~
    Set ``VARCO_JWT_PRIVATE_KEY_PEM`` to a base64-encoded or raw PEM private
    key.  The ``Dockerfile`` ARG / ``docker-compose.yml`` secret injection
    pattern is documented in the accompanying ``docker-compose.yml``.

    Returns:
        A fully initialised ``JwtAuthority`` using ``RS256``.

    Raises:
        KeyLoadError: ``VARCO_JWT_PRIVATE_KEY_PEM`` is set but the PEM is
                      malformed or is not a private key.

    Edge cases:
        - If ``VARCO_JWT_PRIVATE_KEY_PEM`` contains literal ``\\n`` (escaped
          newlines from a shell env var assignment), they are normalised to
          real newline characters automatically.
        - A multi-line PEM in ``docker-compose.yml`` ``environment:`` block
          is passed verbatim — no normalisation needed.
    """
    raw_pem = os.environ.get("VARCO_JWT_PRIVATE_KEY_PEM")

    if raw_pem:
        # Normalise escaped newlines that some deployment tools introduce
        # (e.g. ``-e VARCO_JWT_PRIVATE_KEY_PEM="-----BEGIN...\\n...END-----"``).
        pem_bytes = raw_pem.replace("\\n", "\n").encode()
        _logger.info("auth: loading JWT private key from VARCO_JWT_PRIVATE_KEY_PEM")
        return JwtAuthority.from_pem(
            pem_bytes, kid=KID, issuer=ISSUER, algorithm="RS256"
        )

    # Dev mode: generate a fresh RSA-2048 key pair.
    # cryptography is a transitive dependency of PyJWT — always available.
    _logger.warning(
        "auth: VARCO_JWT_PRIVATE_KEY_PEM not set — generating an ephemeral RSA-2048 "
        "key pair.  All tokens will be invalidated on restart.  "
        "Set VARCO_JWT_PRIVATE_KEY_PEM for persistent token verification."
    )

    from cryptography.hazmat.primitives.asymmetric import rsa  # noqa: PLC0415
    from cryptography.hazmat.primitives import serialization  # noqa: PLC0415

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        # 2048-bit key: strong enough for dev; use 4096 in long-lived production keys.
        key_size=2048,
    )
    pem_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return JwtAuthority.from_pem(pem_bytes, kid=KID, issuer=ISSUER, algorithm="RS256")


# ── HTTP contract models ───────────────────────────────────────────────────────


class LoginRequest(BaseModel):
    """
    Request body for ``POST /auth/login``.

    Args:
        username: Demo user name (``"alice"`` or ``"bob"``).
        password: Plaintext password for the demo user.
    """

    username: str
    password: str


class TokenResponse(BaseModel):
    """
    Response body for ``POST /auth/login``.

    Args:
        access_token: Signed RS256 JWT.  Include as ``Authorization: Bearer <token>``.
        token_type:   Always ``"bearer"`` — RFC 6750 convention.
        expires_in:   Token lifetime in seconds (3600 = 1 hour).
    """

    access_token: str
    token_type: str = "bearer"
    expires_in: int = int(_TOKEN_LIFETIME.total_seconds())


class MeResponse(BaseModel):
    """
    Response body for ``GET /auth/me``.

    Args:
        user_id: Subject claim from the verified JWT.
        roles:   Role list from the JWT ``roles`` claim.
        anonymous: Whether the caller is unauthenticated.
    """

    user_id: str | None
    roles: list[str]
    anonymous: bool


# ── AuthRouter ────────────────────────────────────────────────────────────────


class AuthRouter:
    """
    FastAPI router for authentication endpoints.

    Exposes:
        POST /auth/login  — exchange username/password for a JWT
        GET  /auth/me     — inspect the current caller's identity

    The router is constructed with a pre-built ``JwtAuthority`` so it can
    sign tokens without importing the authority as a module-level singleton.

    Thread safety:  ✅ ``authority`` is read-only after construction.
    Async safety:   ✅ All handlers are ``async def``.

    Args:
        authority: The signing authority used to produce JWT access tokens.
    """

    def __init__(self, authority: JwtAuthority) -> None:
        """
        Args:
            authority: Pre-built ``JwtAuthority`` (from ``build_jwt_authority()``).
        """
        # Stored to avoid re-importing or re-building the authority per request.
        self._authority = authority

    def build_router(self) -> APIRouter:
        """
        Construct and return the ``APIRouter`` with all auth endpoints.

        Returns:
            ``APIRouter`` prefixed at ``/auth`` tagged ``["auth"]``.

        Edge cases:
            - Calling ``build_router()`` multiple times returns independent
              ``APIRouter`` instances — safe but wasteful; call once at startup.
        """
        # Capture self._authority in a local so the closures below don't need
        # to hold a reference to the entire AuthRouter instance.
        authority = self._authority
        router = APIRouter(prefix="/auth", tags=["auth"])

        @router.post(
            "/login",
            response_model=TokenResponse,
            summary="Exchange credentials for a JWT access token",
            description=(
                "Demo users: **alice / alice123** (admin) and **bob / bob123** (editor). "
                "Include the returned ``access_token`` as "
                "``Authorization: Bearer <token>`` on all protected endpoints."
            ),
        )
        async def login(body: LoginRequest) -> TokenResponse:
            """
            Validate username/password and return a signed JWT.

            The JWT carries:
            - ``sub``   — the user's stable UUID (used as ``author_id`` on posts)
            - ``roles`` — list of role names (``"admin"`` or ``"editor"``)
            - ``exp``   — expiry timestamp (now + 1 hour)
            - ``iss``   — issuer string (``"varco-example"``)

            Args:
                body: Login credentials.

            Returns:
                ``TokenResponse`` with a signed RS256 JWT.

            Raises:
                HTTPException 401: Invalid username or password.

            Edge cases:
                - Usernames are case-sensitive (``"Alice"`` != ``"alice"``).
                - Passwords are compared in plaintext (demo only — not prod).
            """
            # DESIGN: constant-time comparison would prevent timing attacks in prod.
            # For this demo, a plain dict lookup is sufficient.
            user = DEMO_USERS.get(body.username)
            if user is None or user["password"] != body.password:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=(
                        "Invalid username or password.  "
                        "Demo users: alice/alice123 (admin), bob/bob123 (editor)."
                    ),
                    headers={"WWW-Authenticate": "Bearer"},
                )

            # Build token: sub = user_id, roles via AuthContext, 1-hour expiry.
            # Roles are set through .with_auth_ctx() — .claim("roles", ...) is
            # reserved and raises ValueError since varco encodes roles into the
            # structured AuthContext payload, not as a raw JWT claim.
            from varco_core.auth import AuthContext  # noqa: PLC0415

            auth_ctx = AuthContext(
                user_id=str(user["user_id"]),
                roles=frozenset(user["roles"]),  # type: ignore[arg-type]
            )
            builder = (
                authority.token()
                .subject(str(user["user_id"]))
                .with_auth_ctx(auth_ctx)
                .expires_in(_TOKEN_LIFETIME)
            )
            token_str = authority.sign(builder)

            _logger.info(
                "auth: issued token for user=%s roles=%s", body.username, user["roles"]
            )
            return TokenResponse(access_token=token_str)

        @router.get(
            "/me",
            response_model=MeResponse,
            summary="Inspect the current caller's identity",
            description=(
                "Returns the authenticated caller's ``user_id`` and ``roles`` "
                "from the verified JWT.  Returns ``anonymous=true`` when no "
                "valid Bearer token is present."
            ),
        )
        async def me(request: Request) -> MeResponse:
            """
            Return the caller's identity from the request-scoped ``AuthContext``.

            Does NOT require authentication — anonymous callers receive
            ``anonymous=true`` so this endpoint doubles as a health/connectivity
            check for API clients.

            Args:
                request: Injected by FastAPI — used to read the ``auth_context_var``
                         set by ``RequestContextMiddleware``.

            Returns:
                ``MeResponse`` reflecting the current auth state.

            Edge cases:
                - ``RequestContextMiddleware`` must be installed on the app for
                  the ContextVar to be set.  Without it, ``get_auth_context_or_none``
                  returns ``None`` → ``anonymous=True``.
            """
            # Import here to avoid a circular import at module load time.
            # get_auth_context_or_none() reads the ContextVar set by the middleware
            # — it never raises, making this endpoint always-available.
            from varco_fastapi.context import get_auth_context_or_none  # noqa: PLC0415

            ctx: AuthContext | None = get_auth_context_or_none()
            if ctx is None or ctx.is_anonymous():
                return MeResponse(user_id=None, roles=[], anonymous=True)

            return MeResponse(
                user_id=ctx.user_id,
                roles=sorted(ctx.roles),
                anonymous=False,
            )

        return router


__all__ = [
    "build_jwt_authority",
    "AuthRouter",
    "LoginRequest",
    "TokenResponse",
    "MeResponse",
    "DEMO_USERS",
    "ISSUER",
    "KID",
]
