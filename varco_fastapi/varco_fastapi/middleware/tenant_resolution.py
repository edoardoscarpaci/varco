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

DESIGN: ``chain=None`` means "the configured chain"; the implicit legacy fallback stays, deprecated for 5.0.0 (Plan 055 / FLIP-6, §D-flip6-floor)
    ``chain=None`` resolves, in order: the env-derived chain
    (``build_tenant_source_chain()`` reading ``VARCO_TENANT_SOURCES``) — and,
    when ``TenancyModule`` constructs this middleware, the container-resolved
    chain it passes in explicitly — and only when *nothing* is configured,
    the implicit ``TenantSourceChain((LegacyTenantSource(header),))``.
    ✅ Delivers "tenant resolution defaults to the chain" on both
       construction paths: a hand-wirer with ``VARCO_TENANT_SOURCES=jwt``
       exported no longer silently stays header-only.
    ✅ The implicit fallback resolves byte-identically to the pre-3.2
       header-only code (header absent -> pass through; present -> that
       tenant), so a 3.2 caller who ignored the warning keeps working
       through 4.x — ``CONTRIBUTING.md``'s 12-month floor from 3.2.0
       (2026-09-12) forbids removing it before 2027-09-12.
    ✅ The fallback emits exactly **one** ``varco_core.deprecation``
       warning (since 3.2.0, removed in 5.0.0) at construction, never per
       request — so the one removal site is greppable by its
       ``removed_in`` keyword, unlike the 3.2 ad-hoc stdlib warning. An **explicit** ``LegacyTenantSource``
       (in code, or ``VARCO_TENANT_SOURCES=legacy``) warns not at all — it
       is the named, permanent escape hatch.
    ✅ The loud half is posture, not an exception: wired through
       ``TenancyModule``, the implicit fallback reports
       ``tenant.no_chain`` + ``tenant.legacy_source_implicit`` (WARN), which
       refuse a production start under Plan 054's derived ``refuse``.
    ❌ The ``TypeError`` the 3.2 warning promised does not land in 4.0 — it
       moves to 5.0.0's removal list.
    ❌ Two resolution inputs: env-only when hand-wired, container (incl.
       multi-bound ``TenantSource`` plugins) when module-wired. A hand-wirer
       naming a plugin source gets a ``ValueError`` pointing at
       ``chain=``/``TenancyModule`` — fail closed, never a header fallback.
    ❌ A caller passing a custom ``header=`` AND an explicit ``chain=`` gets
       ``header`` silently ignored (the chain decides what to read).
    Rejected — ``chain=None`` -> ``TypeError`` (the BACKLOG row verbatim):
    ❌ removes a deprecated path 12 months early.
    Rejected — ``chain=None`` with no sources -> ``ValueError``: ❌ the same
    removal under another exception class.
    Rejected — decoding the token's claims without checking its signature to
    make a routing decision before ``RequestContextMiddleware`` verifies it:
    ❌ this is the bug §D-S6-wiring exists to fix, not an optimisation — a
    grep-based CI gate guards against this method name ever appearing in
    this package's middleware or in ``varco_core.tenancy``.

DESIGN: ``catalog``/``pool`` are optional (Plan 055 §D-flip9-shared)
    ✅ A ``SHARED``-isolation app with a JWT ``tenant_id`` claim and no
       control plane — the class of app that lost the
       ``RequestContextMiddleware`` claim setter in 4.0 — can adopt the
       chain with no catalog and no pool.
    ❌ No catalog means no suspend/delete gate at the edge — but the setter
       it replaces checked nothing either, and ``TenancyModule`` reports
       ``tenant.no_catalog_gate`` (INFO). ``TenancyModule`` refuses
       startup (``ConfigurationError`` from ``async_init()``) for
       ``pool=None`` under ``SCHEMA``/``DATABASE`` isolation.
    Rejected — a ``NullTenantCatalog`` returning ``ACTIVE`` for every id:
    ❌ a Null Object asserting a status it does not know is a lie in the
    routing decision; ``None`` says "no gate".

DESIGN: verify the token in this (outer) middleware and let the inner
``RequestContextMiddleware`` reuse it (§D-S6-wiring)
    ✅ ``AbstractServerAuth`` is already a plain callable invoked at
       middleware time elsewhere in this stack — nothing new is invented.
    ✅ ``current_tenant_provenance() is not None`` is the coordination
       marker ``RequestContextMiddleware`` reads to defer — see that
       module's own docstring for the other half of this design.
    ✅ Since 4.0 ``TenancyModule`` places this middleware at
       ``EdgeSlot.TENANT_RESOLUTION`` — inside ``ErrorMiddleware``, so a
       rejection is enveloped, logged, traced and metric'd.
    ❌ A hand-wired placement at ``EdgeSlot.EXTRA`` (``MiddlewareModule``)
       still sits OUTSIDE ``ErrorMiddleware`` — an ``HTTPException`` raised
       by ``server_auth`` would otherwise surface as Starlette's default
       plain-text 401/500. So this middleware keeps catching
       ``HTTPException`` from ``server_auth`` and returns a ``JSONResponse``
       itself, exactly as it does for a non-routable tenant.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from starlette.exceptions import HTTPException
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp
from varco_core.auth.base import AuthContext
from varco_core.deprecation import warn_deprecated
from varco_core.tenancy.catalog import TenantNotFoundError
from varco_core.tenancy.provenance import provenance_context
from varco_core.tenancy.routing import routing_decision_for_status
from varco_core.tenancy.settings import TenantStatus, build_tenant_source_chain
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
        catalog: An ``AbstractTenantCatalog``-shaped object (``get()``), or
                 ``None`` (default) — no status gate (``SHARED`` isolation
                 without a control plane).
        pool:    A resource-pool-shaped object with an async ``ensure()``,
                 or ``None`` (default) — no ``ensure()`` call.
        header:  The HTTP header carrying the tenant id. Defaults to
                 ``"X-Tenant-Id"``. Only consulted by the deprecated
                 implicit ``LegacyTenantSource`` fallback.
        chain:   A ``TenantSourceChain``, or ``None`` (default) — the
                 configured chain from ``VARCO_TENANT_SOURCES``; with
                 nothing configured, the deprecated implicit
                 ``LegacyTenantSource(header)`` (removed in 5.0.0), which
                 emits exactly one ``DeprecationWarning`` at construction.
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

    Raises:
        ValueError: ``chain=None`` and ``VARCO_TENANT_SOURCES`` names a
            source the env-only builder cannot construct (a multi-bound
            plugin source, or ``act_as`` without a ``DelegationPolicy``) —
            raised at construction, i.e. on the app's first request.

    Edge cases:
        - ``catalog=None`` -> no status gate; an id unknown to any catalog
          is still set as ``current_tenant()``.
        - ``pool=None`` -> no ``ensure()``; legal only under ``SHARED``
          isolation (``TenancyModule.async_init()`` refuses startup
          otherwise; a hand-wired middleware is not checked).
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
        catalog: Any | None = None,
        pool: Any | None = None,
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

        self._chain_implicit = False
        if chain is None:
            try:
                chain = build_tenant_source_chain()
            except ValueError as exc:
                # The env-only builder cannot see multi-bound TenantSources or a
                # bound DelegationPolicy — point the hand-wirer at the two paths
                # that can, rather than leaving a bare "unknown source" message.
                raise ValueError(
                    f"{exc} — pass chain= explicitly or wire TenantResolutionMiddleware "
                    "through TenancyModule, which can see multi-bound TenantSources"
                ) from exc
        if chain is None:
            warn_deprecated(
                "TenantResolutionMiddleware(chain=None) with no VARCO_TENANT_SOURCES configured",
                since="3.2.0",
                removed_in="5.0.0",
                replacement=(
                    "an explicit TenantSourceChain(sources=(LegacyTenantSource(),)) for "
                    "header-only resolution, or VARCO_TENANT_SOURCES=jwt[,…] "
                    "(see technical_docs/features/tenant-provenance.md)"
                ),
                stacklevel=3,
            )
            chain = TenantSourceChain(sources=(LegacyTenantSource(header),))
            self._chain_implicit = True
        self._chain = chain

    @property
    def chain(self) -> TenantSourceChain:
        """The resolved ``TenantSourceChain`` this middleware dispatches through."""
        return self._chain

    @property
    def chain_implicit(self) -> bool:
        """``True`` iff the deprecated implicit ``LegacyTenantSource`` fallback is active."""
        return self._chain_implicit

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
        Catalog-status + ``pool.ensure()`` gate, triggered by the chain's
        resolved ``tenant_id``; each half is skipped when its collaborator
        is ``None`` (Plan 055 §D-flip9-shared).

        Args:
            request: The incoming request.
            call_next: The next ASGI handler.
            tenant_id: The chain's resolved tenant, or ``None``.

        Returns:
            The downstream response, or a JSON rejection for a non-routable
            tenant.
        """
        if tenant_id is None:
            return await call_next(request)

        if self._catalog is not None:
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

        # The one HTTP writer of current_tenant() since 4.0 (Plan 055 / FLIP-9)
        # — test_tenant_single_writer.py enforces that no second one appears.
        with tenant_context(tenant_id):
            if self._pool is not None:
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
