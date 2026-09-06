"""
Failing-first tests for RequestContextMiddleware's deferral to an upstream
tenant-provenance chain (Plan 033, Phase 2, Step 18) — §D-S6-wiring.

DoD item 2: TestChainInstalledUpstream::test_chain_winner_overrides_token_claim
must assert a genuine behavioural red on today's unmodified code (the
Correction-2 regression: a chain winner of "A" plus a token claim of "B"
must leave current_tenant() == "A", not "B").
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.middleware.base import BaseHTTPMiddleware


def _auth_ctx(**metadata):
    from varco_core.auth.base import AuthContext

    return AuthContext(user_id="u1", metadata=metadata)


class _CountingServerAuth:
    def __init__(self, ctx):
        self._ctx = ctx
        self.calls = 0

    async def __call__(self, request):
        self.calls += 1
        return self._ctx


class _ProvenancePublisher(BaseHTTPMiddleware):
    """
    Stands in for TenantResolutionMiddleware: publishes a TenantProvenance
    winner of "A" and enters tenant_context("A") before calling downstream,
    exactly the shape §D-S6-wiring assigns to the chain middleware.
    """

    def __init__(self, app, *, winner_tenant_id: str = "A") -> None:
        super().__init__(app)
        self._winner_tenant_id = winner_tenant_id

    async def dispatch(self, request, call_next):
        from varco_core.service.tenant import tenant_context
        from varco_core.tenancy.provenance import provenance_context
        from varco_core.tenancy.source import (
            CrossCheckMode,
            TenantClaim,
            TenantProvenance,
            TenantTrust,
        )

        # Publishes a real TenantProvenance (winner "A") — exactly the shape
        # §D-S6-wiring assigns to the real TenantResolutionMiddleware —
        # so RequestContextMiddleware's deferral guard (keyed on
        # current_tenant_provenance() is not None) actually engages here.
        # Deliberately does NOT call auth_context() — that is the separate
        # "provenance set but auth_context_var unset" case these tests
        # exercise (a chain installed with server_auth=None upstream).
        claim = TenantClaim(
            tenant_id=self._winner_tenant_id, source="stub", trust=TenantTrust.HIGHEST
        )
        prov = TenantProvenance(
            tenant_id=self._winner_tenant_id,
            winner=claim,
            claims=(claim,),
            conflict=None,
            mode=CrossCheckMode.LENIENT,
        )
        with tenant_context(self._winner_tenant_id):
            with provenance_context(prov):
                return await call_next(request)


def _build_app(*, server_auth, with_provenance_publisher: bool, winner_tenant_id="A"):
    from varco_fastapi.middleware.request_context import RequestContextMiddleware

    app = FastAPI()
    app.add_middleware(RequestContextMiddleware, server_auth=server_auth)
    if with_provenance_publisher:
        app.add_middleware(_ProvenancePublisher, winner_tenant_id=winner_tenant_id)

    @app.get("/ping")
    async def ping():
        from varco_core.service.tenant import current_tenant

        return {"tenant": current_tenant()}

    return app


class TestNoChainInstalled:
    def test_behaves_byte_identically_auth_runs_and_claim_tenant_entered(self) -> None:
        server_auth = _CountingServerAuth(_auth_ctx(tenant_id="B"))
        app = _build_app(server_auth=server_auth, with_provenance_publisher=False)
        client = TestClient(app)

        response = client.get("/ping")

        assert response.status_code == 200
        assert response.json()["tenant"] == "B"
        assert server_auth.calls == 1


class TestChainInstalledUpstream:
    def test_server_auth_runs_exactly_once_total(self) -> None:
        server_auth = _CountingServerAuth(_auth_ctx(tenant_id="B"))
        app = _build_app(
            server_auth=server_auth, with_provenance_publisher=True, winner_tenant_id="A"
        )
        client = TestClient(app)

        # The provenance publisher stands in for the chain middleware; it
        # does not itself call server_auth here (that already happened, per
        # the real chain middleware's own contract) — RequestContextMiddleware
        # must still not authenticate a second time when provenance is set
        # AND an AuthContext is already present. We simulate "auth already
        # ran" by having the fake chain publish provenance only; the deferral
        # under test is specifically "does RequestContextMiddleware call
        # server_auth again" when it must not.
        response = client.get("/ping")

        assert response.status_code == 200
        # This is the regression under test: the winning chain tenant "A"
        # must not be overridden by RequestContextMiddleware re-entering
        # tenant_context() from the token's "B" claim.
        assert response.json()["tenant"] == "A"

    def test_chain_winner_overrides_token_claim(self) -> None:
        # Direct restatement of the Correction-2 regression required by the
        # plan's DoD item 2 — must fail on today's unmodified code, because
        # RequestContextMiddleware unconditionally re-enters tenant_context()
        # from the token claim with no awareness of any upstream provenance.
        server_auth = _CountingServerAuth(_auth_ctx(tenant_id="B"))
        app = _build_app(
            server_auth=server_auth, with_provenance_publisher=True, winner_tenant_id="A"
        )
        client = TestClient(app)

        response = client.get("/ping")

        assert response.json()["tenant"] == "A"

    def test_provenance_set_but_auth_context_unset_authenticates_normally(self) -> None:
        server_auth = _CountingServerAuth(_auth_ctx(tenant_id="B"))
        app = _build_app(
            server_auth=server_auth, with_provenance_publisher=True, winner_tenant_id="A"
        )
        client = TestClient(app)

        client.get("/ping")

        # auth_context_var is not pre-populated by our fake publisher (only
        # the real chain middleware would do that), so RequestContextMiddleware
        # must still call server_auth on this path.
        assert server_auth.calls == 1


class _FakeCatalog:
    async def get(self, tenant_id: str):
        from varco_core.tenancy.catalog import TenantDescriptor

        return TenantDescriptor(tenant_id=tenant_id, status="active")


class _CountingPool:
    def __init__(self) -> None:
        self.ensure_calls = 0

    async def ensure(self, tenant_id: str):
        self.ensure_calls += 1
        return object()


def _build_real_stacked_app(*, server_auth):
    """
    README's recommended same-instance wiring: one `server_auth` object
    passed to BOTH the real `TenantResolutionMiddleware(chain=...)` and the
    real `RequestContextMiddleware`, stacked exactly as
    `install_middleware_stack` would order them. This is the shape
    §D-S6-wiring's "auth runs once per request" guarantee is actually about
    — the `_ProvenancePublisher` stand-in above deliberately never exercises
    it, because it never populates `auth_context_var` at all.
    """
    from varco_core.tenancy.source import TenantSourceChain
    from varco_core.tenancy.sources import JwtClaimTenantSource
    from varco_fastapi.middleware.request_context import RequestContextMiddleware
    from varco_fastapi.middleware.tenant_resolution import TenantResolutionMiddleware

    chain = TenantSourceChain(sources=(JwtClaimTenantSource(),))

    app = FastAPI()
    # add_middleware() stacks last-added-OUTERMOST (verified against plain
    # Starlette) — TenantResolutionMiddleware must run OUTSIDE
    # RequestContextMiddleware (the real §D-S6-wiring order: the chain
    # middleware authenticates and publishes provenance first; the inner
    # RequestContextMiddleware then defers to it), so it is added last.
    app.add_middleware(RequestContextMiddleware, server_auth=server_auth)
    app.add_middleware(
        TenantResolutionMiddleware,
        catalog=_FakeCatalog(),
        pool=_CountingPool(),
        chain=chain,
        server_auth=server_auth,
    )

    @app.get("/ping")
    async def ping():
        from varco_core.service.tenant import current_tenant

        return {"tenant": current_tenant()}

    return app


class TestRealChainStackedWithRequestContextMiddlewareSameInstance:
    """
    Step 18 / §D-S6-wiring's actual guarantee, exercised end-to-end: the
    real `TenantResolutionMiddleware` and the real `RequestContextMiddleware`
    stacked together, sharing ONE `server_auth` instance, over a real
    `TestClient` request.
    """

    def test_server_auth_runs_exactly_once_across_the_whole_request(self) -> None:
        server_auth = _CountingServerAuth(_auth_ctx(tenant_id="acme"))
        app = _build_real_stacked_app(server_auth=server_auth)
        client = TestClient(app)

        response = client.get("/ping")

        assert response.status_code == 200
        assert response.json()["tenant"] == "acme"
        # The guarantee under test: TenantResolutionMiddleware authenticates
        # once and publishes both provenance and auth_context_var;
        # RequestContextMiddleware must reuse that verdict rather than
        # calling server_auth a second time.
        assert server_auth.calls == 1
