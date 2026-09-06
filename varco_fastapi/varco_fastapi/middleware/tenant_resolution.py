"""
varco_fastapi.middleware.tenant_resolution
=============================================
``TenantResolutionMiddleware`` — resolves the request's tenant status
**before** ``pool.ensure()`` (Plan 007, Phase 10, step 3-4), and, since
Plan 033 / S6 (§D-S6-wiring), **the one tenant decision point** when a
``TenantSourceChain`` is configured.

DESIGN: catalog status checked before ``pool.ensure()``, always
    Mirrors ``varco_core.tenancy.routing.route_request`` — a non-``active``
    tenant never causes an engine/binding to be created. This middleware
    is the HTTP-layer caller of that routing decision; a request with no
    tenant header passes through untouched (public routes still work), and
    a request that resolves to a status other than ``active`` gets the
    documented HTTP code (503/403/410/404) — never a default-database
    fallback, never a bare 500.

DESIGN: ``chain=None`` builds an implicit ``LegacyTenantSource`` chain — one code path (§D-S6-wiring, §D-S6-blast)
    ✅ Unifies what used to be (pre-3.2) a bespoke header-read into the same
       ``TenantSourceChain.resolve()`` machinery every explicit chain goes
       through — one dispatch implementation, not two.
    ✅ ``chain=None`` -> ``TenantSourceChain(sources=(LegacyTenantSource(header),))``
       resolves byte-identically to the old header-only code: header absent
       -> zero claims -> ``tenant_id=None`` -> pass through untouched;
       header present -> one claim -> ``LENIENT`` (the chain default) never
       rejects a lone claim -> ``tenant_id=<header value>``.
    ✅ Emits exactly **one** ``DeprecationWarning``, at construction, never
       per request (matches the ``varco_fastapi.auth.TrustStore`` shim
       shape). An **explicit** ``LegacyTenantSource`` in a caller-supplied
       chain warns **not at all** — it is the named, documented escape
       hatch the locked decision promises.
    ❌ A caller passing a custom ``header=`` AND an explicit ``chain=`` gets
       ``header`` silently ignored (the chain decides what to read).
       Accepted — ``header`` is documented as "still honoured when chain is
       None" only.
    Rejected — decoding the token's claims without checking its signature to
    make a routing decision before ``RequestContextMiddleware`` verifies it:
    ❌ this is the bug §D-S6-wiring exists to fix, not an optimisation — a
    grep-based CI gate guards against this method name ever appearing in
    this package's middleware or in ``varco_core.tenancy``.

DESIGN: verify the token in this (outer) middleware and let the inner
``RequestContextMiddleware`` reuse it (§D-S6-wiring)
    ✅ ``AbstractServerAuth`` is already a plain callable invoked at
       middleware time elsewhere in this stack — nothing new is invented.
    ✅ ``current_tenant_provenance() is not None`` is the coordination
       marker ``RequestContextMiddleware`` reads to defer — see that
       module's own docstring for the other half of this design.
    ❌ ``extra_middleware=`` sits OUTSIDE ``ErrorMiddleware``
       (``app.py:530-544``) — an ``HTTPException`` raised by ``server_auth``
       here would otherwise surface as Starlette's default plain-text
       401/500. **Mitigation is mandatory**: this middleware catches
       ``HTTPException`` from ``server_auth`` and returns a ``JSONResponse``
       itself, exactly as it already does for a non-routable tenant.
"""

from __future__ import annotations

import warnings
from dataclasses import replace
from typing import Any

from starlette.exceptions import HTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp
from varco_core.auth.base import AuthContext
from varco_core.tenancy.catalog import TenantNotFoundError
from varco_core.tenancy.provenance import provenance_context
from varco_core.tenancy.routing import routing_decision_for_status
from varco_core.tenancy.settings import TenantStatus
from varco_core.tenancy.source import TenantRequest, TenantSourceChain
from varco_core.tenancy.sources import ActAsTenantSource, LegacyTenantSource

# Opaque body for every rejection this middleware produces — deliberately
# names no tenant id and no rejection reason (§D-S6-chain: the reason token
# is for internal/audit use; the HTTP body is opaque-safe by construction).
_REJECTED_BODY = {"detail": "Tenant resolution could not be completed."}


class TenantResolutionMiddleware(BaseHTTPMiddleware):
    """
    Resolves the request's tenant identity via a ``TenantSourceChain`` (or
    an implicit legacy header chain), rejecting a disagreement/non-member
    before any resource is created, then applies the existing catalog-status
    + ``pool.ensure()`` gate unchanged.

    Args:
        app:     The ASGI application to wrap.
        catalog: An ``AbstractTenantCatalog``-shaped object (``get()``).
        pool:    A resource-pool-shaped object with an async ``ensure()``.
        header:  The HTTP header carrying the tenant id. Defaults to
                 ``"X-Tenant-Id"``. Only consulted when ``chain=None``
                 builds the implicit ``LegacyTenantSource``.
        chain:   A ``TenantSourceChain``, or ``None`` (default) — today's
                 header-only behaviour, now routed through an internally
                 built ``TenantSourceChain((LegacyTenantSource(header),))``.
                 Emits exactly one ``DeprecationWarning`` at construction.
        server_auth: An ``AbstractServerAuth``-shaped callable, or ``None``.
                 Needed to give ``JwtClaimTenantSource`` (or any claim-based
                 source) a verified ``AuthContext`` to read. ``None`` means
                 no source in the chain will see an ``auth`` value.
        membership: An ``AbstractTenantMembership``, or ``None`` (default —
                 no check runs). Runs **after** the chain resolves and
                 **before** the catalog lookup / ``pool.ensure()``.
        reject_status: HTTP status for a chain/membership rejection.
                 Defaults to ``403`` (§D-S6-oq3's resolved open question:
                 one code, configurable, so a caller cannot use the status
                 as an oracle for *which* check failed).

    Edge cases:
        - No tenant resolved at all (``chain`` produced zero claims) ->
          passes through untouched (public routes still work) — same as
          "no tenant header" pre-3.2.
        - A rejected verdict (conflict / non-member) never reaches the
          catalog lookup or ``pool.ensure()``.
        - ``ensure()`` is called at most once per request, and only for a
          routable, resolved tenant.
        - A ``server_auth`` that raises ``HTTPException`` produces a JSON
          response from THIS middleware — see the module ``DESIGN:`` block
          on why that is mandatory here specifically.

    Thread safety:  ✅ Per-request state only; the chain/sources/membership
        provider are shared, immutable-or-documented-otherwise instances.
    Async safety:   ✅ ``dispatch`` is ``async def``; the one sync boundary
        (``chain.resolve()``) is itself synchronous and pure.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        catalog: Any,
        pool: Any,
        header: str = "X-Tenant-Id",
        chain: TenantSourceChain | None = None,
        server_auth: Any | None = None,
        membership: Any | None = None,
        reject_status: int = 403,
    ) -> None:
        super().__init__(app)
        self._catalog = catalog
        self._pool = pool
        self._header = header
        self._server_auth = server_auth
        self._membership = membership
        self._reject_status = reject_status

        if chain is None:
            warnings.warn(
                "TenantResolutionMiddleware(chain=None) is deprecated and will "
                "be removed in 4.0.0 — pass an explicit TenantSourceChain "
                "(varco_core.tenancy.source.TenantSourceChain). The implicit "
                "fallback below reproduces today's X-Tenant-Id-header-only "
                "behaviour via a LegacyTenantSource, which you can also name "
                "explicitly to silence this warning: "
                "TenantSourceChain(sources=(LegacyTenantSource(),)). See "
                "technical_docs/features/tenant-provenance.md.",
                DeprecationWarning,
                stacklevel=2,
            )
            self._chain = TenantSourceChain(sources=(LegacyTenantSource(header),))
        else:
            self._chain = chain

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        tenant_request = TenantRequest(
            headers=dict(request.headers),
            host=_strip_port(request.headers.get("host")),
            path=request.url.path,
        )

        auth_ctx: AuthContext | None = None
        if self._server_auth is not None:
            try:
                auth_ctx = await self._server_auth(request)
            except HTTPException as exc:
                return JSONResponse(
                    status_code=exc.status_code, content={"detail": str(exc.detail)}
                )
            tenant_request = replace(tenant_request, auth=auth_ctx)

        provenance = self._chain.resolve(tenant_request)

        # Plan 033 / S16: an ActAsTenantSource's decision is not visible in
        # `provenance.claims` when it denies (resolve() returns None, same
        # as "this source had nothing to say") — but a denied/unbound act-as
        # attempt must still reject the request rather than silently
        # falling through to "no tenant resolved". Its last_record (built on
        # every resolve() call, allow AND deny — §D-S16-shape, request-scoped
        # via AmbientVar — see sources.py) carries that verdict; attach it
        # to the provenance so rejection_reason picks it up via the
        # "delegation_denied" branch.
        for src in self._chain.sources:
            if isinstance(src, ActAsTenantSource) and src.last_record is not None:
                provenance = replace(provenance, delegation=src.last_record)
                break

        if (
            self._membership is not None
            and provenance.tenant_id is not None
            and not provenance.rejected
        ):
            decision = await self._membership.check(auth_ctx or AuthContext(), provenance.tenant_id)
            provenance = replace(provenance, membership=decision)

        if provenance.rejected:
            return JSONResponse(status_code=self._reject_status, content=_REJECTED_BODY)

        with provenance_context(provenance):
            if auth_ctx is not None:
                from varco_fastapi.context import auth_context

                authorization = request.headers.get("authorization", "")
                raw_token = (
                    authorization.removeprefix("Bearer ").strip() or None
                    if authorization.startswith("Bearer ")
                    else None
                )
                async with auth_context(auth_ctx, token=raw_token):
                    return await self._continue(request, call_next, provenance.tenant_id)
            return await self._continue(request, call_next, provenance.tenant_id)

    async def _continue(
        self, request: Request, call_next: RequestResponseEndpoint, tenant_id: str | None
    ) -> Response:
        """
        Existing (unchanged) catalog-status + ``pool.ensure()`` gate, now
        triggered by the chain's resolved ``tenant_id`` rather than a raw
        header read.
        """
        if tenant_id is None:
            return await call_next(request)

        try:
            descriptor = await self._catalog.get(tenant_id)
            status_value = (
                descriptor.status.value
                if hasattr(descriptor.status, "value")
                else descriptor.status
            )
        except TenantNotFoundError:
            status_value = TenantStatus.DELETED.value

        decision = routing_decision_for_status(status_value)
        if not decision.routable:
            return JSONResponse(
                status_code=decision.http_status,
                content={"detail": f"Tenant {tenant_id!r}: {decision.reason}"},
            )

        from varco_core.service.tenant import tenant_context

        with tenant_context(tenant_id):
            await self._pool.ensure(tenant_id)
            return await call_next(request)


def _strip_port(host: str | None) -> str | None:
    """Strip a trailing ``:port`` from a ``Host`` header value, if present."""
    if not host:
        return host
    if host.startswith("["):
        return host  # bracketed IPv6 literal — leave untouched
    return host.split(":", 1)[0]


__all__ = ["TenantResolutionMiddleware"]
