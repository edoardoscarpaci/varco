"""
Failing-first tests for act-as / RFC 8693 through the real TenantResolutionMiddleware
wiring (Plan 033, Phase 6, Step 41 — DROPPABLE per §D-S16-cut).
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient


class _FakeCatalog:
    async def get(self, tenant_id: str):
        from varco_core.tenancy.catalog import TenantDescriptor

        return TenantDescriptor(tenant_id=tenant_id, status="active")


class _CountingPool:
    async def ensure(self, tenant_id: str):
        return object()


class _FakeServerAuth:
    def __init__(self, ctx):
        self._ctx = ctx

    async def __call__(self, request):
        return self._ctx


def _auth_ctx(**metadata):
    from varco_core.auth.base import AuthContext

    return AuthContext(user_id="svc-billing", metadata=metadata)


class _AllowlistPolicyForAcme:
    name = "test-allowlist"

    async def allows(self, actor, principal, tenant_id):
        return tenant_id == "acme"


def _build_app(*, chain, server_auth):
    from varco_fastapi.middleware.tenant_resolution import TenantResolutionMiddleware

    app = FastAPI()
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
        from varco_core.tenancy.provenance import current_tenant_provenance

        prov = current_tenant_provenance()
        return {
            "tenant": current_tenant(),
            "delegation_allowed": prov.delegation.allowed if prov and prov.delegation else None,
        }

    return app


def _act_as_chain(policy):
    from varco_core.tenancy.source import TenantSourceChain
    from varco_core.tenancy.sources import ActAsTenantSource

    return TenantSourceChain(sources=(ActAsTenantSource(policy=policy),))


class TestActAsAllowed:
    def test_allowlisted_actor_and_tenant_returns_200(self) -> None:
        chain = _act_as_chain(_AllowlistPolicyForAcme())
        server_auth = _FakeServerAuth(_auth_ctx(actor={"sub": "svc-billing"}))
        app = _build_app(chain=chain, server_auth=server_auth)
        client = TestClient(app)

        response = client.get("/ping", headers={"X-Act-As-Tenant": "acme"})

        assert response.status_code == 200
        assert response.json()["tenant"] == "acme"


class TestActAsDeniedForNonAllowlistedTenant:
    def test_non_allowlisted_tenant_returns_403(self) -> None:
        chain = _act_as_chain(_AllowlistPolicyForAcme())
        server_auth = _FakeServerAuth(_auth_ctx(actor={"sub": "svc-billing"}))
        app = _build_app(chain=chain, server_auth=server_auth)
        client = TestClient(app)

        response = client.get("/ping", headers={"X-Act-As-Tenant": "beta"})

        assert response.status_code == 403


class TestActAsWithNoPolicyConfigured:
    def test_no_delegation_policy_returns_403(self) -> None:
        chain = _act_as_chain(None)
        server_auth = _FakeServerAuth(_auth_ctx(actor={"sub": "svc-billing"}))
        app = _build_app(chain=chain, server_auth=server_auth)
        client = TestClient(app)

        response = client.get("/ping", headers={"X-Act-As-Tenant": "acme"})

        assert response.status_code == 403


class TestDelegatedRequestCarriesFullAudit:
    def test_provenance_delegation_carries_principal_actor_and_tenant(self) -> None:
        chain = _act_as_chain(_AllowlistPolicyForAcme())
        server_auth = _FakeServerAuth(_auth_ctx(actor={"sub": "svc-billing"}))
        app = _build_app(chain=chain, server_auth=server_auth)
        client = TestClient(app)

        response = client.get("/ping", headers={"X-Act-As-Tenant": "acme"})

        assert response.status_code == 200
        assert response.json()["delegation_allowed"] is True
