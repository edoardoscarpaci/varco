"""
varco_fastapi.middleware.request_context
=========================================
ASGI middleware that populates all request-scoped ``ContextVar`` values.

This is the central middleware in the varco_fastapi stack.  It wires together:

- ``request_scope()``   — request ID + correlation ID
- ``AbstractServerAuth.__call__()`` — auth context from JWT/API-key
- ``auth_context()``    — stores ``AuthContext`` + raw token

It never sets the tenant — since 4.0 the only HTTP writer of
``current_tenant()`` is ``TenantResolutionMiddleware`` (Plan 055 / FLIP-9).

Middleware order (outermost first)::

    CORSMiddleware
      ErrorMiddleware          ← catches errors from ALL middleware below
        TracingMiddleware      ← OTel span wrapping
          RequestContextMiddleware  ← sets ContextVars (THIS FILE)
            SessionMiddleware  ← DI request scope / DB session lifecycle
              FastAPI router

``RequestContextMiddleware`` MUST run before any code that calls
``get_auth_context()``.

DESIGN: all ContextVars in one middleware over per-concern middleware
    ✅ Single point of truth for request setup — easier to reason about ordering
    ✅ Auth runs once per request; downstream code reads from ContextVar (no I/O)
    ✅ Compatible with both sync and async route handlers (ASGI-level, not HTTP-level)
    ❌ If auth is optional, RequestContextMiddleware still runs for every request
       — mitigated by using AnonymousAuth as a no-op strategy

DESIGN: no tenant setter here (Plan 055 / FLIP-9)
    The pre-4.0 ``tenant_field``/``enable_tenant_context`` kwargs and the
    ``_maybe_tenant_context()`` helper are removed outright.
    ✅ One decision point: a tenant read from a claim alone — no chain, no
       catalog check, no membership check — can no longer become
       ``current_tenant()``; ``test_tenant_single_writer.py`` pins that
       ``TenantResolutionMiddleware`` is the only HTTP writer.
    ✅ A 3.x caller passing either kwarg gets Python's own ``TypeError`` at
       construction — loud and ``mypy``-visible. The kwargs were wiring
       shapes, never deprecated, so no 12-month floor applies.
    ❌ An app that relied on the claim setter loses ``current_tenant()``
       until it adopts ``TenancyModule`` + ``VARCO_TENANT_SOURCES=jwt``
       (``catalog``/``pool`` are optional for ``SHARED`` isolation).
    Rejected — ``enable_tenant_context: bool = False`` with ``True`` raising:
    ❌ a tombstone kwarg that exists only to raise.
    Rejected — keeping it as "mirror the chain's tenant": ❌ the chain path
    already enters ``tenant_context()``; a second entry is a no-op or a
    disagreement.

Thread safety:  ✅ ContextVar tokens are task-local — no shared state.
Async safety:   ✅ All setup is ``async def`` via context managers.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

from varco_fastapi.auth.server_auth import AbstractServerAuth, AnonymousAuth
from varco_fastapi.context import auth_context, auth_context_var, request_scope

if TYPE_CHECKING:
    pass

_logger = logging.getLogger(__name__)

# Header names for reading/writing the request ID
_REQUEST_ID_HEADER = "X-Request-ID"
_CORRELATION_ID_HEADER = "X-Correlation-ID"


class RequestContextMiddleware(BaseHTTPMiddleware):
    """
    ASGI middleware that populates ContextVars for the request lifecycle.

    Order of operations per request:
    1. Read ``X-Request-ID`` header (or generate a fresh UUID).
    2. Enter ``request_scope(request_id)`` — sets ``request_id_var`` AND
       enters ``correlation_context(request_id)`` from varco_core.tracing.
    3. Extract ``Authorization: Bearer <token>`` from headers.
    4. Run ``server_auth(request)`` → ``AuthContext``.
    5. Enter ``auth_context(ctx, token)`` — sets ``auth_context_var`` +
       ``request_token_var``.
    6. Never enters ``tenant_context()`` — since 4.0 the only HTTP writer
       of ``current_tenant()`` is ``TenantResolutionMiddleware`` (Plan 055 /
       FLIP-9).
    7. Set ``X-Request-ID`` on the response.
    8. Call next middleware / route handler.
    9. On exit: all ContextVars are automatically reset via token-based restore.

    **Auth reuse from an upstream tenant-provenance chain (Plan 033 / S6,
    §D-S6-wiring).** When ``current_tenant_provenance()`` is already set (a
    ``TenantResolutionMiddleware`` ran upstream) **and** ``auth_context_var``
    is set too (that middleware ran ``server_auth`` and entered
    ``auth_context()`` itself), this middleware calls ``server_auth`` **zero**
    additional times and reuses the upstream verdict. Otherwise (no chain
    upstream, or a chain installed with ``server_auth=None``) it runs
    ``server_auth`` and enters ``auth_context()`` normally (steps 3-5).

    Args:
        app:                    The ASGI application to wrap.
        server_auth:            The auth strategy to run per request.
                                Default: ``AnonymousAuth()`` (no auth required).

    Thread safety:  ✅ Stateless per-request — ContextVar tokens are task-local.
    Async safety:   ✅ Uses async context managers for guaranteed cleanup.

    Edge cases:
        - Auth failure (``HTTPException 401``) propagates up to ``ErrorMiddleware``
          which formats the JSON response.  ContextVars are cleaned up regardless
          because the ``async with`` finally blocks always run.
        - If ``server_auth`` raises any exception other than ``HTTPException``,
          it propagates up as an internal error.
        - The ``X-Request-ID`` header is always added to the response for
          client-side log correlation.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        server_auth: AbstractServerAuth | None = None,
    ) -> None:
        super().__init__(app)
        self._server_auth: AbstractServerAuth = server_auth or AnonymousAuth()

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """
        Set up all request-scoped ContextVars, call next, then clean up.

        Args:
            request:   Incoming HTTP request.
            call_next: Next middleware / route handler callable.

        Returns:
            Response from the downstream handler.
        """
        # Step 1: Determine request ID (prefer incoming header for tracing)
        request_id = request.headers.get(_REQUEST_ID_HEADER) or request.headers.get(
            _CORRELATION_ID_HEADER
        )

        # §D-S6-wiring auth-reuse marker: a TenantSourceChain already ran
        # upstream (TenantResolutionMiddleware) iff provenance is published.
        # varco owns this ambient var, so this is a structural signal — not a
        # heuristic on request.state or a Starlette internal.
        from varco_core.tenancy.provenance import current_tenant_provenance

        chain_already_ran = current_tenant_provenance() is not None

        # Steps 2–5: Enter all context managers
        async with request_scope(request_id=request_id) as rid:
            # Step 3: Extract raw Bearer token for forwarding/audit
            authorization = request.headers.get("Authorization", "")
            raw_token: str | None = None
            if authorization.startswith("Bearer "):
                raw_token = authorization.removeprefix("Bearer ").strip() or None

            if chain_already_ran and auth_context_var.get() is not None:
                # The chain middleware already ran server_auth AND entered
                # auth_context() itself — reuse it verbatim (zero additional
                # server_auth calls).
                response = await call_next(request)
            else:
                # Step 4: Run auth strategy — unconditionally when no chain
                # ran, OR when a chain ran with server_auth=None upstream
                # (nothing has authenticated yet, so this middleware still
                # must).
                ctx = await self._server_auth(request)

                # Step 5: Set auth context. Step 6: never the tenant (FLIP-9).
                async with auth_context(ctx, token=raw_token):
                    response = await call_next(request)

        # Step 7: Set request ID on response for client-side correlation
        response.headers[_REQUEST_ID_HEADER] = rid
        return response


__all__ = ["RequestContextMiddleware"]
