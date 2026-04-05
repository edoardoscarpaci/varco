"""
varco_fastapi.context
=====================
Request-scoped ``ContextVar`` storage and async context managers for
per-request state in FastAPI applications.

Why ContextVars?
----------------
FastAPI runs multiple concurrent requests in the same asyncio event loop.
Thread-local storage doesn't work for async code.  ``ContextVar`` objects
are natively supported by asyncio — each Task (one per request) gets its
own copy that is automatically isolated from other Tasks.

Relationship to varco_core
--------------------------
``varco_core`` already uses ``ContextVar`` for two concerns:

- ``varco_core.tracing.correlation_context()``  — correlation ID
- ``varco_core.service.tenant.tenant_context()``  — tenant ID

This module adds two more per-request vars and provides:

- ``auth_context_var``   — decoded ``AuthContext`` (identity + grants)
- ``request_token_var``  — raw Bearer JWT (for audit / forwarding)
- ``request_id_var``     — unique request identifier (= correlation ID by default)
- ``auth_context()``     — async context manager (mirrors ``correlation_context``)
- ``request_scope()``    — async context manager for the full request lifecycle

All three context managers follow the same pattern as ``correlation_context``:
enter → set ContextVar token → yield → reset to previous value.  This ensures
correct isolation even when context managers are nested.

Usage (in RequestContextMiddleware)::

    from varco_fastapi.context import auth_context, request_scope
    from varco_core.tracing import correlation_context

    async with request_scope(request_id=request_id):
        async with auth_context(ctx, token=raw_jwt):
            response = await call_next(request)

Usage (in tests)::

    async def test_my_endpoint():
        ctx = AuthContext(user_id="usr_1", roles=frozenset({"admin"}))
        async with auth_context(ctx):
            result = await my_service.do_something()

DESIGN: separate ContextVars per concern
    ✅ Each var is independently settable — middleware can set auth first,
       then another middleware sets tenant later, without coupling
    ✅ ``get_auth_context()`` has precise type (``AuthContext``), not
       ``dict["auth"]`` — better IDE support
    ✅ Mirrors existing ``_current_tenant`` and ``_correlation_id`` pattern
       in varco_core — no new patterns introduced
    ❌ More vars to manage — mitigated by ``RequestContextMiddleware`` which
       sets all of them in one place per request

Thread safety:  ✅ ContextVar is designed for async/concurrent use.
Async safety:   ✅ All context managers are ``async def``.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import AsyncIterator

from varco_core.tracing import correlation_context
from providify import Provider
from varco_core.auth.base import AuthContext
from varco_core.jwt import JsonWebToken, JwtParser

# ── Typed DI context tokens ────────────────────────────────────────────────────
#
# These frozen dataclasses act as typed DI tokens — injectable as Live[RequestContext]
# or Live[JwtContext] in services that need request-scoped data without importing
# individual ContextVar accessors.
#
# DESIGN: Aggregated dataclass tokens over individual ContextVar injection
#   ✅ Single injection point — one Live[RequestContext] covers auth + id + token
#   ✅ IDE autocomplete works — fields are fully typed, not buried in a plain dict
#   ✅ Consistent with the project's "typed DI tokens as dataclasses" pattern
#   ❌ Slightly more indirection than calling get_auth_context() directly — but
#      justified for singleton services that need request-scoped state via Live[]


@dataclass(frozen=True)
class RequestContext:
    """
    Aggregated snapshot of the current HTTP request's runtime context.

    Designed to be injected via ``Live[RequestContext]`` in providify-wired
    singletons.  Because it is wrapped in ``Live[]``, the DI container will
    call the provider function on every access, ensuring the singleton always
    sees the *current* request's values (not a stale capture).

    Attributes:
        auth:       The decoded ``AuthContext`` for this request.
        request_id: Unique identifier for the current request (= correlation ID).
        token:      Raw Bearer JWT from the ``Authorization`` header, or ``None``
                    for anonymous / API-key-authenticated requests.

    Thread safety:  ✅ frozen=True — immutable after construction.
    Async safety:   ✅ Pure value object; reads from ContextVars at construction time.

    Edge cases:
        - ``token=None`` does NOT mean unauthenticated — it means no Bearer token was
          present.  Check ``auth.user_id`` for identity.
        - The instance is built fresh on every DI resolution (via ``Live[]``), so
          ``auth``, ``request_id``, and ``token`` always reflect the current request.

    Example::

        class OrderService:
            def __init__(self, ctx: Live[RequestContext]) -> None:
                self._ctx = ctx

            async def create(self, body: OrderCreate) -> OrderRead:
                # Always gets the current request's context
                auth = self._ctx.get().auth
                ...
    """

    auth: AuthContext
    request_id: str
    token: str | None


@dataclass(frozen=True)
class JwtContext:
    """
    Parsed JWT data for the current request.

    Like ``RequestContext``, designed for injection via ``Live[JwtContext]``.
    Provides both the raw token string (for forwarding) and the parsed
    ``JsonWebToken`` (for reading claims without a full verification pass).

    Attributes:
        token: The parsed ``JsonWebToken`` if a Bearer token was present,
               or ``None`` for anonymous requests.
        raw:   The raw Bearer token string, or ``None``.

    Thread safety:  ✅ frozen=True — immutable after construction.
    Async safety:   ✅ ``JwtParser.parse_unverified()`` is synchronous; no I/O.

    Edge cases:
        - ``parse_unverified()`` does NOT verify the signature — it just decodes
          the claims.  Use ``TrustedIssuerRegistry.verify()`` for full verification.
        - If the token is malformed (not valid Base64/JSON), ``JwtParser.parse_unverified``
          may raise — the provider function guards this with a ``None`` fallback.

    Example::

        class AuditService:
            def __init__(self, jwt: Live[JwtContext]) -> None:
                self._jwt = jwt

            async def log(self, action: str) -> None:
                raw = self._jwt.get().raw  # forward the original token downstream
    """

    token: JsonWebToken | None
    raw: str | None


# ── @Provider functions for RequestContext and JwtContext ──────────────────────


@Provider
def get_request_context() -> RequestContext:
    """
    Build a ``RequestContext`` from the current request's ContextVars.

    Called by the DI container on every ``Live[RequestContext]`` resolution —
    always reflects the *current* request, not a stale singleton snapshot.

    Returns:
        ``RequestContext`` with auth, request_id, and token from the current request.

    Raises:
        RuntimeError: If called outside a request scope (no auth context set).

    Thread safety:  ✅ Reads ContextVars — task-local by design.
    Async safety:   ✅ Synchronous; no I/O.
    """
    return RequestContext(
        auth=get_auth_context(),
        request_id=get_request_id(),
        token=get_request_token(),
    )


@Provider
def get_jwt_context() -> JwtContext:
    """
    Build a ``JwtContext`` from the current request's raw Bearer token.

    Parses the token without verifying the signature.  Returns ``None``
    for both fields if no Bearer token is present in the request.

    Returns:
        ``JwtContext`` with parsed token and raw string; both ``None`` for anonymous.

    Thread safety:  ✅ Reads ContextVars — task-local by design.
    Async safety:   ✅ ``JwtParser.parse_unverified()`` is synchronous.

    Edge cases:
        - Malformed token → ``parse_unverified`` may raise; callers should handle
          via the ``ErrorMiddleware`` which catches ``JwtDecodeError``.
    """
    raw = get_request_token()
    parsed: JsonWebToken | None = None
    if raw:
        try:
            parsed = JwtParser.parse_unverified(raw)
        except Exception:
            # Malformed token — keep parsed=None; downstream auth will reject it
            parsed = None
    return JwtContext(token=parsed, raw=raw)


# ── ContextVars ────────────────────────────────────────────────────────────────

# Decoded auth context for the current request.
# Set by RequestContextMiddleware after running AbstractServerAuth.
# None means no auth middleware has run (do not confuse with anonymous auth;
# anonymous auth produces an AuthContext with user_id=None).
auth_context_var: ContextVar[AuthContext | None] = ContextVar(
    "varco_auth_context",
    default=None,
)

# Raw Bearer JWT from the Authorization header.
# Preserved for audit logging, callback authentication, and auth forwarding
# to downstream services via JwtClientAuth.
# None if the request has no Authorization: Bearer header.
request_token_var: ContextVar[str | None] = ContextVar(
    "varco_request_token",
    default=None,
)

# Unique identifier for the current request.
# By default this is the same value as the correlation ID (correlation_context
# from varco_core.tracing).  Set by request_scope() before entering
# correlation_context() so both share the same ID.
# None before request_scope() is entered.
request_id_var: ContextVar[str | None] = ContextVar(
    "varco_request_id",
    default=None,
)


# ── Accessors (fail-fast, typed) ──────────────────────────────────────────────
def get_auth_context() -> AuthContext:
    """
    Read the ``AuthContext`` for the current request.

    Returns:
        The ``AuthContext`` set by ``RequestContextMiddleware``.

    Raises:
        RuntimeError: If called outside of a request context (i.e. before
            ``auth_context()`` context manager has been entered, typically
            meaning the middleware is not installed or is incorrectly ordered).

    Edge cases:
        - Returns an ``AuthContext`` with ``user_id=None`` for anonymous
          requests (where ``PassthroughAuth`` or ``AnonymousAuth`` is used) —
          this is NOT the same as the ``ContextVar`` being unset.
        - Call ``get_auth_context_or_none()`` instead if you need to handle
          the "middleware not installed" case gracefully.

    Thread safety:  ✅ ContextVar reads are task-local.
    Async safety:   ✅ Pure read; no I/O.
    """
    ctx = auth_context_var.get()
    if ctx is None:
        raise RuntimeError(
            "No AuthContext in current request context. "
            "Ensure RequestContextMiddleware is installed and runs before "
            "any code that calls get_auth_context()."
        )
    return ctx


@Provider
def get_auth_context_or_none() -> AuthContext | None:
    """
    Read the ``AuthContext`` for the current request, or ``None`` if unset.

    Unlike ``get_auth_context()``, this does not raise — use it when you want
    to conditionally apply auth checks only when middleware is present.

    Returns:
        The ``AuthContext`` if set, or ``None`` if called outside a request context.

    Thread safety:  ✅ ContextVar reads are task-local.
    Async safety:   ✅ Pure read; no I/O.
    """
    return auth_context_var.get()


def get_request_token() -> str | None:
    """
    Read the raw Bearer JWT for the current request.

    Used by ``JobRunner`` to capture the original request token for async
    job execution, and by ``ForwardingClientAuth`` to forward the token
    to downstream services.

    Returns:
        The raw Bearer token string, or ``None`` if no Authorization:
        Bearer header was present (anonymous or API-key auth).

    Thread safety:  ✅ ContextVar reads are task-local.
    Async safety:   ✅ Pure read; no I/O.
    """
    return request_token_var.get()


def get_jwt_token() -> JsonWebToken | None:
    token = request_token_var.get()

    if token:
        return JwtParser.parse_unverified(token)

    return None


def get_request_id() -> str:
    """
    Read the unique request ID for the current request.

    By default this equals the correlation ID (see ``request_scope()``).

    Returns:
        The request ID string.

    Raises:
        RuntimeError: If called outside of a request context (before
            ``request_scope()`` has been entered).

    Thread safety:  ✅ ContextVar reads are task-local.
    Async safety:   ✅ Pure read; no I/O.
    """
    rid = request_id_var.get()
    if rid is None:
        raise RuntimeError(
            "No request_id in current request context. "
            "Ensure RequestContextMiddleware is installed."
        )
    return rid


# ── Context managers ───────────────────────────────────────────────────────────


@asynccontextmanager
async def auth_context(
    ctx: AuthContext,
    token: str | None = None,
) -> AsyncIterator[None]:
    """
    Set ``auth_context_var`` and optionally ``request_token_var`` for the
    duration of the ``async with`` block, then restore the previous values.

    Used by ``RequestContextMiddleware`` after running ``AbstractServerAuth``.
    Also useful in tests to inject a specific auth context without middleware.

    Args:
        ctx:   The ``AuthContext`` to set for this scope.
        token: Optional raw Bearer JWT to store in ``request_token_var``.

    Yields:
        Nothing — the context is managed implicitly.

    Edge cases:
        - Nesting ``auth_context()`` is safe — each nesting level restores
          the previous value when it exits (token-based reset).
        - ``token=None`` does NOT clear an existing ``request_token_var`` —
          the previous value is preserved.  Pass ``token=""`` to explicitly
          clear it if needed.

    Thread safety:  ✅ Token-based ContextVar reset is async-task-local.
    Async safety:   ✅ Uses ``asynccontextmanager``; all operations are sync.

    Example::

        ctx = AuthContext(user_id="usr_1", roles=frozenset({"admin"}))
        async with auth_context(ctx, token="Bearer eyJ..."):
            result = await my_service.create(dto, get_auth_context())
    """
    auth_token = auth_context_var.set(ctx)
    token_token = request_token_var.set(token) if token is not None else None
    try:
        yield
    finally:
        auth_context_var.reset(auth_token)
        if token_token is not None:
            request_token_var.reset(token_token)


@asynccontextmanager
async def request_scope(
    *,
    request_id: str | None = None,
) -> AsyncIterator[str]:
    """
    Set ``request_id_var`` and enter ``correlation_context()`` for the
    duration of the ``async with`` block.

    This is the outermost context manager in the request lifecycle.
    ``RequestContextMiddleware`` enters ``request_scope()`` first, then
    ``auth_context()`` inside it.

    If ``request_id`` is not provided, a fresh ``uuid4`` is generated.
    The same ID is used for both ``request_id_var`` and ``correlation_context``
    so all log lines from a single request share one ID.

    Args:
        request_id: Optional pre-existing request/correlation ID
                    (e.g. read from ``X-Request-ID`` header).
                    Generated if not provided.

    Yields:
        The ``request_id`` string in use for this scope.

    Edge cases:
        - If the request carries an ``X-Request-ID`` header, it is passed
          in here so the same ID propagates through logs and downstream calls.
        - ``correlation_context()`` from varco_core is always entered, so
          ``current_correlation_id()`` works throughout the request.

    Thread safety:  ✅ Each Task gets its own ContextVar copy.
    Async safety:   ✅ ``correlation_context`` is also ``asynccontextmanager``.

    Example::

        async with request_scope(request_id="req_abc123") as rid:
            # current_correlation_id() == rid == "req_abc123"
            async with auth_context(ctx):
                response = await call_next(request)
    """
    rid = request_id or str(uuid.uuid4())
    request_token = request_id_var.set(rid)
    try:
        async with correlation_context(rid):
            yield rid
    finally:
        request_id_var.reset(request_token)


# ── Public API ────────────────────────────────────────────────────────────────

__all__ = [
    # DI context tokens
    "RequestContext",
    "JwtContext",
    "get_request_context",
    "get_jwt_context",
    # ContextVars (importable for low-level access)
    "auth_context_var",
    "request_token_var",
    "request_id_var",
    # Typed accessors
    "get_auth_context",
    "get_auth_context_or_none",
    "get_request_token",
    "get_request_id",
    # Context managers
    "auth_context",
    "request_scope",
]
