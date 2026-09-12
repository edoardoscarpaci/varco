"""
varco_fastapi.middleware.request_context
=========================================
ASGI middleware that populates all request-scoped ``ContextVar`` values.

This is the central middleware in the varco_fastapi stack.  It wires together:

- ``request_scope()``   — request ID + correlation ID
- ``AbstractServerAuth.__call__()`` — auth context from JWT/API-key
- ``tenant_context()``  — tenant isolation (from varco_core.service.tenant)
- ``auth_context()``    — stores ``AuthContext`` + raw token

Middleware order (outermost first)::

    CORSMiddleware
      ErrorMiddleware          ← catches errors from ALL middleware below
        TracingMiddleware      ← OTel span wrapping
          RequestContextMiddleware  ← sets ContextVars (THIS FILE)
            SessionMiddleware  ← DI request scope / DB session lifecycle
              FastAPI router

``RequestContextMiddleware`` MUST run before any code that calls
``get_auth_context()`` or ``current_tenant()``.

DESIGN: all ContextVars in one middleware over per-concern middleware
    ✅ Single point of truth for request setup — easier to reason about ordering
    ✅ Auth runs once per request; downstream code reads from ContextVar (no I/O)
    ✅ Compatible with both sync and async route handlers (ASGI-level, not HTTP-level)
    ❌ If auth is optional, RequestContextMiddleware still runs for every request
       — mitigated by using AnonymousAuth as a no-op strategy

Thread safety:  ✅ ContextVar tokens are task-local — no shared state.
Async safety:   ✅ All setup is ``async def`` via context managers.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
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
    6. If ``enable_tenant_context`` is ``True``, extract ``tenant_field``
       from ``AuthContext.metadata`` and enter ``tenant_context()`` from
       varco_core.service.tenant.
    7. Set ``X-Request-ID`` on the response.
    8. Call next middleware / route handler.
    9. On exit: all ContextVars are automatically reset via token-based restore.

    **Deferral to an upstream tenant-provenance chain (Plan 033 / S6,
    §D-S6-wiring — Correction 1/2 fix).** Steps 4-6 above describe the
    behaviour when no ``TenantResolutionMiddleware(chain=...)`` ran upstream
    (``chain=None``, or this middleware installed alone — byte-identical to
    pre-3.2). When ``current_tenant_provenance()`` is already set (a chain
    DID run upstream), two sub-cases apply instead:

    - ``auth_context_var`` is **also** already set (the chain middleware ran
      ``server_auth`` and entered ``auth_context()`` itself) — this
      middleware calls ``server_auth`` **zero** additional times and does
      **not** re-enter ``tenant_context()`` from the token claim (the
      resolved-tenant question is already answered by the chain's winner;
      re-deriving it from a possibly-disagreeing claim is exactly
      Correction 2's bug).
    - ``auth_context_var`` is **not** set (a chain installed with
      ``server_auth=None``, so nothing upstream authenticated yet) — this
      middleware still runs ``server_auth`` and enters ``auth_context()``
      normally (steps 3-5 unchanged), but **still never** re-enters
      ``tenant_context()`` from the claim — the chain already decided the
      tenant, and provenance being set is a stronger signal than any claim
      this middleware could read on its own.

    Args:
        app:                    The ASGI application to wrap.
        server_auth:            The auth strategy to run per request.
                                Default: ``AnonymousAuth()`` (no auth required).
        tenant_field:           Key in ``AuthContext.metadata`` for the tenant ID.
                                Default: ``"tenant_id"``.
        enable_tenant_context:  Whether to enter ``tenant_context()``.
                                Set ``False`` if multi-tenancy is not used.
                                Default: ``True``.

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
        tenant_field: str = "tenant_id",
        enable_tenant_context: bool = True,
    ) -> None:
        super().__init__(app)
        self._server_auth: AbstractServerAuth = server_auth or AnonymousAuth()
        self._tenant_field = tenant_field
        self._enable_tenant_context = enable_tenant_context

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

        # §D-S6-wiring deferral marker: a TenantSourceChain already ran
        # upstream (TenantResolutionMiddleware(chain=...)) iff provenance is
        # published. varco owns this ambient var, so this is a structural
        # signal — not a heuristic on request.state or a Starlette internal.
        from varco_core.tenancy.provenance import current_tenant_provenance

        chain_already_ran = current_tenant_provenance() is not None

        # Steps 2–6: Enter all context managers
        async with request_scope(request_id=request_id) as rid:
            # Step 3: Extract raw Bearer token for forwarding/audit
            authorization = request.headers.get("Authorization", "")
            raw_token: str | None = None
            if authorization.startswith("Bearer "):
                raw_token = authorization.removeprefix("Bearer ").strip() or None

            if chain_already_ran and auth_context_var.get() is not None:
                # The chain middleware already ran server_auth AND entered
                # auth_context() itself — reuse it verbatim. Zero additional
                # server_auth calls, and (like every chain_already_ran branch
                # below) no re-entry into tenant_context() from a claim.
                response = await call_next(request)
            else:
                # Step 4: Run auth strategy — unconditionally when no chain
                # ran, OR when a chain ran with server_auth=None upstream
                # (nothing has authenticated yet, so this middleware still
                # must).
                ctx = await self._server_auth(request)

                # Steps 5–6: Set auth context + optionally enter tenant context.
                # chain_already_ran is checked again here (not only above) —
                # once a chain has decided the tenant, this middleware must
                # never re-derive it from ctx.metadata, even on the "still
                # authenticates" branch (§D-S6-wiring, Correction 2's fix).
                async with auth_context(ctx, token=raw_token):
                    if self._enable_tenant_context and not chain_already_ran:
                        tenant_id = ctx.metadata.get(self._tenant_field)
                        async with _maybe_tenant_context(tenant_id):
                            response = await call_next(request)
                    else:
                        response = await call_next(request)

        # Step 7: Set request ID on response for client-side correlation
        response.headers[_REQUEST_ID_HEADER] = rid
        return response


# ── Tenant context helper ─────────────────────────────────────────────────────


@asynccontextmanager
async def _maybe_tenant_context(tenant_id: str | None) -> AsyncIterator[None]:
    """
    Enter ``tenant_context(tenant_id)`` if tenant_id is not None.
    Otherwise yield immediately (no-op context manager).
    """
    if tenant_id:
        from varco_core.service.tenant import tenant_context

        # BUG (surfaced by RL-6's mypy gate, plans/017): `tenant_context()` is
        # a sync `@contextmanager` (it only sets a ContextVar — no I/O), but
        # this was entered with `async with`, which requires __aenter__/
        # __aexit__ and would raise AttributeError at runtime on every
        # request carrying a tenant_id. No test exercises this exact path
        # (zero hits for `_maybe_tenant_context`/`tenant_context` under
        # varco_fastapi/tests/), which is why it went undetected. Fixed to
        # plain `with` — `tenant_context()` does no I/O, so no `async` is
        # needed to enter it from this async function.
        with tenant_context(tenant_id):
            yield
    else:
        yield


__all__ = ["RequestContextMiddleware"]
