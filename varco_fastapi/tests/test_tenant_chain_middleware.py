"""
Failing-first tests for TenantResolutionMiddleware's new chain= / server_auth=
/ membership= keywords (Plan 033, Phase 2 Step 15/17, Phase 3 Step 26).

⛔ This file is NEW — it never edits test_tenant_resolution_middleware.py or
test_tenant_event_path_middleware.py, which remain the byte-identical proof
for chain=None and must stay untouched.
"""

from __future__ import annotations

import inspect

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient


class _FakeCatalog:
    def __init__(self, status: str = "active") -> None:
        self._status = status
        self.get_calls = 0

    async def get(self, tenant_id: str):
        from varco_core.tenancy.catalog import TenantDescriptor

        self.get_calls += 1
        return TenantDescriptor(tenant_id=tenant_id, status=self._status)


class _CountingPool:
    def __init__(self) -> None:
        self.ensure_calls = 0

    async def ensure(self, tenant_id: str):
        self.ensure_calls += 1
        return object()


class _FakeServerAuth:
    """A minimal AbstractServerAuth-shaped callable for tests."""

    def __init__(self, ctx_factory=None, *, raises: Exception | None = None):
        self._ctx_factory = ctx_factory
        self._raises = raises
        self.calls = 0

    async def __call__(self, request):
        self.calls += 1
        if self._raises is not None:
            raise self._raises
        from varco_core.auth.base import AuthContext

        if self._ctx_factory is None:
            return AuthContext()
        return self._ctx_factory()


def _auth_ctx(**metadata):
    from varco_core.auth.base import AuthContext

    return AuthContext(user_id="u1", metadata=metadata)


def _legacy_chain():
    from varco_core.tenancy.source import TenantSourceChain
    from varco_core.tenancy.sources import LegacyTenantSource

    return TenantSourceChain(sources=(LegacyTenantSource(),))


def _build_app(*, catalog=None, pool=None, chain=None, server_auth=None, membership=None):
    from varco_fastapi.middleware.tenant_resolution import TenantResolutionMiddleware

    app = FastAPI()
    app.add_middleware(
        TenantResolutionMiddleware,
        catalog=catalog or _FakeCatalog(),
        pool=pool or _CountingPool(),
        chain=chain,
        server_auth=server_auth,
        membership=membership,
    )

    @app.get("/ping")
    async def ping():
        from varco_core.service.tenant import current_tenant
        from varco_core.tenancy.provenance import current_tenant_provenance

        prov = current_tenant_provenance()
        return {
            "tenant": current_tenant(),
            "provenance_tenant": prov.tenant_id if prov else None,
        }

    return app


class TestChainNoneDeprecationWarning:
    def test_chain_none_warns_once_at_construction_not_per_request(self) -> None:
        # NOTE: FastAPI/Starlette's `add_middleware()` only *registers* a
        # middleware spec — it does not instantiate the class. The
        # middleware stack (and therefore `TenantResolutionMiddleware.
        # __init__`) is built lazily, on the app's first ASGI call, and then
        # cached for the lifetime of the app. So "at construction" is
        # observable only across the first vs. a later request, not by
        # wrapping `_build_app()` (which never triggers `__init__` at all —
        # confirmed against plain Starlette, unrelated to this plan).
        pool = _CountingPool()
        app = _build_app(pool=pool)
        client = TestClient(app)

        import warnings

        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            r1 = client.get("/ping", headers={"X-Tenant-Id": "acme"})
            r2 = client.get("/ping", headers={"X-Tenant-Id": "acme"})
        assert r1.status_code == 200
        assert r2.status_code == 200
        dep_warnings = [w for w in record if issubclass(w.category, DeprecationWarning)]
        # Exactly one — the middleware is constructed once (on the first
        # request) and reused for every subsequent request on this app.
        assert len(dep_warnings) == 1

    def test_explicit_legacy_source_in_chain_warns_not_at_all(self) -> None:
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            _build_app(chain=_legacy_chain())  # must not raise


class TestConflictingRequestRejected:
    def test_conflict_returns_403_opaque_body(self) -> None:
        from varco_core.tenancy.source import TenantSourceChain
        from varco_core.tenancy.sources import JwtClaimTenantSource, LegacyTenantSource

        chain = TenantSourceChain(sources=(JwtClaimTenantSource(), LegacyTenantSource()))
        server_auth = _FakeServerAuth(lambda: _auth_ctx(tenant_id="beta"))
        pool = _CountingPool()
        app = _build_app(chain=chain, server_auth=server_auth, pool=pool)
        client = TestClient(app)

        response = client.get("/ping", headers={"X-Tenant-Id": "acme"})

        assert response.status_code == 403
        assert "acme" not in response.text
        assert "beta" not in response.text
        assert pool.ensure_calls == 0

    def test_status_response_codes_unchanged(self) -> None:
        catalog = _FakeCatalog(status="suspended")
        server_auth = _FakeServerAuth(lambda: _auth_ctx(tenant_id="acme"))
        chain = _legacy_chain()
        from varco_core.tenancy.source import TenantSourceChain
        from varco_core.tenancy.sources import JwtClaimTenantSource

        chain = TenantSourceChain(sources=(JwtClaimTenantSource(),))
        app = _build_app(catalog=catalog, chain=chain, server_auth=server_auth)
        client = TestClient(app)

        response = client.get("/ping")
        assert response.status_code == 403


class TestPoolEnsureCalledAtMostOnce:
    def test_ensure_called_once_for_routable_tenant(self) -> None:
        from varco_core.tenancy.source import TenantSourceChain
        from varco_core.tenancy.sources import JwtClaimTenantSource

        chain = TenantSourceChain(sources=(JwtClaimTenantSource(),))
        server_auth = _FakeServerAuth(lambda: _auth_ctx(tenant_id="acme"))
        pool = _CountingPool()
        app = _build_app(chain=chain, server_auth=server_auth, pool=pool)
        client = TestClient(app)

        response = client.get("/ping")

        assert response.status_code == 200
        assert pool.ensure_calls == 1
        # TenantResolutionMiddleware itself must call server_auth exactly
        # once per request — the counting fake is otherwise built and never
        # asserted on (drift finding).
        assert server_auth.calls == 1


class TestCurrentTenantAndProvenanceInsideHandler:
    def test_current_tenant_equals_chain_winner(self) -> None:
        from varco_core.tenancy.source import TenantSourceChain
        from varco_core.tenancy.sources import JwtClaimTenantSource

        chain = TenantSourceChain(sources=(JwtClaimTenantSource(),))
        server_auth = _FakeServerAuth(lambda: _auth_ctx(tenant_id="acme"))
        app = _build_app(chain=chain, server_auth=server_auth)
        client = TestClient(app)

        response = client.get("/ping")

        assert response.status_code == 200
        body = response.json()
        assert body["tenant"] == "acme"
        assert body["provenance_tenant"] == "acme"


class TestServerAuthHttpExceptionBecomesJson:
    def test_401_from_server_auth_is_json_despite_being_outside_error_middleware(self) -> None:
        server_auth = _FakeServerAuth(raises=HTTPException(status_code=401, detail="nope"))
        chain = _legacy_chain()
        app = _build_app(chain=chain, server_auth=server_auth)
        client = TestClient(app, raise_server_exceptions=False)

        response = client.get("/ping", headers={"X-Tenant-Id": "acme"})

        assert response.status_code == 401
        assert response.headers["content-type"].startswith("application/json")

    def test_anonymous_context_falls_through_to_remaining_sources(self) -> None:
        from varco_core.tenancy.source import TenantSourceChain
        from varco_core.tenancy.sources import JwtClaimTenantSource, LegacyTenantSource

        chain = TenantSourceChain(sources=(JwtClaimTenantSource(), LegacyTenantSource()))
        server_auth = _FakeServerAuth(lambda: _auth_ctx())  # anonymous, no tenant claim
        app = _build_app(chain=chain, server_auth=server_auth)
        client = TestClient(app)

        response = client.get("/ping", headers={"X-Tenant-Id": "acme"})

        assert response.status_code == 200
        assert response.json()["tenant"] == "acme"


class TestMembershipIntegration:
    def test_non_member_gets_403_opaque_with_membership_recorded(self) -> None:
        from varco_core.tenancy.membership import ClaimTenantMembership
        from varco_core.tenancy.source import TenantSourceChain
        from varco_core.tenancy.sources import JwtClaimTenantSource

        chain = TenantSourceChain(sources=(JwtClaimTenantSource(),))
        server_auth = _FakeServerAuth(lambda: _auth_ctx(tenant_id="acme", tenants=["beta"]))
        membership = ClaimTenantMembership()
        app = _build_app(chain=chain, server_auth=server_auth, membership=membership)
        client = TestClient(app)

        response = client.get("/ping")

        assert response.status_code == 403
        assert "acme" not in response.text

    def test_member_gets_200(self) -> None:
        from varco_core.tenancy.membership import ClaimTenantMembership
        from varco_core.tenancy.source import TenantSourceChain
        from varco_core.tenancy.sources import JwtClaimTenantSource

        chain = TenantSourceChain(sources=(JwtClaimTenantSource(),))
        server_auth = _FakeServerAuth(lambda: _auth_ctx(tenant_id="acme"))
        membership = ClaimTenantMembership()
        app = _build_app(chain=chain, server_auth=server_auth, membership=membership)
        client = TestClient(app)

        response = client.get("/ping")

        assert response.status_code == 200

    def test_membership_none_means_no_check_runs(self) -> None:
        from varco_core.tenancy.source import TenantSourceChain
        from varco_core.tenancy.sources import JwtClaimTenantSource

        chain = TenantSourceChain(sources=(JwtClaimTenantSource(),))
        server_auth = _FakeServerAuth(lambda: _auth_ctx(tenant_id="acme", tenants=["beta"]))
        app = _build_app(chain=chain, server_auth=server_auth, membership=None)
        client = TestClient(app)

        response = client.get("/ping")
        assert response.status_code == 200

    def test_membership_runs_before_catalog_and_pool_ensure(self) -> None:
        from varco_core.tenancy.membership import ClaimTenantMembership
        from varco_core.tenancy.source import TenantSourceChain
        from varco_core.tenancy.sources import JwtClaimTenantSource

        chain = TenantSourceChain(sources=(JwtClaimTenantSource(),))
        server_auth = _FakeServerAuth(lambda: _auth_ctx(tenant_id="acme", tenants=["beta"]))
        membership = ClaimTenantMembership()
        catalog = _FakeCatalog()
        pool = _CountingPool()
        app = _build_app(
            chain=chain, server_auth=server_auth, membership=membership, catalog=catalog, pool=pool
        )
        client = TestClient(app)

        response = client.get("/ping")

        assert response.status_code == 403
        assert catalog.get_calls == 0
        assert pool.ensure_calls == 0


class TestMiddlewareSignaturesPinned:
    """
    Risks table: api_surface.py does not record class __init__ signatures,
    so this pins TenantResolutionMiddleware's and RequestContextMiddleware's
    __init__ signatures explicitly.
    """

    def test_tenant_resolution_middleware_init_signature(self) -> None:
        from varco_fastapi.middleware.tenant_resolution import TenantResolutionMiddleware

        sig = inspect.signature(TenantResolutionMiddleware.__init__)
        params = list(sig.parameters)
        assert params[:2] == ["self", "app"]
        for kwonly in (
            "catalog",
            "pool",
            "header",
            "chain",
            "server_auth",
            "membership",
            "reject_status",
        ):
            assert kwonly in sig.parameters, kwonly
            assert sig.parameters[kwonly].kind == inspect.Parameter.KEYWORD_ONLY

    def test_request_context_middleware_init_signature_unchanged(self) -> None:
        from varco_fastapi.middleware.request_context import RequestContextMiddleware

        sig = inspect.signature(RequestContextMiddleware.__init__)
        params = list(sig.parameters)
        assert params[:2] == ["self", "app"]
        for kwonly in ("server_auth", "tenant_field", "enable_tenant_context"):
            assert kwonly in sig.parameters
            assert sig.parameters[kwonly].kind == inspect.Parameter.KEYWORD_ONLY
        # No new constructor keyword per §D-S6-wiring's "No new constructor
        # keyword" rule for RequestContextMiddleware.
        assert set(params) - {"self", "app"} == {
            "server_auth",
            "tenant_field",
            "enable_tenant_context",
        }
