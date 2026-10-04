"""
Failing tests for varco_fastapi.middleware.tenant_resolution (Plan 007,
Phase 10, step 3).
"""

from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests._tenant_fixtures import legacy_chain


class _FakeCatalog:
    def __init__(self, status: str) -> None:
        self._status = status

    async def get(self, tenant_id: str):
        from varco_core.tenancy.catalog import TenantDescriptor

        return TenantDescriptor(tenant_id=tenant_id, status=self._status)


class _CountingPool:
    def __init__(self) -> None:
        self.ensure_calls = 0

    async def ensure(self, tenant_id: str):
        self.ensure_calls += 1
        return object()


def _build_app(catalog, pool, tenant_header: str = "X-Tenant-Id"):
    from varco_fastapi.middleware.tenant_resolution import TenantResolutionMiddleware

    app = FastAPI()
    # Plan 055 / FLIP-6: header-only resolution named explicitly — the implicit
    # chain=None fallback is deprecated (and an error under this suite's config).
    app.add_middleware(
        TenantResolutionMiddleware, catalog=catalog, pool=pool, chain=legacy_chain(tenant_header)
    )

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    return app


@pytest.mark.parametrize(
    "status,expected_code",
    [("pending", 503), ("suspended", 403), ("deprovisioning", 410), ("deleted", 404)],
)
def test_status_maps_to_documented_http_code(status: str, expected_code: int) -> None:
    pool = _CountingPool()
    app = _build_app(_FakeCatalog(status), pool)
    client = TestClient(app, raise_server_exceptions=False)

    response = client.get("/ping", headers={"X-Tenant-Id": "acme"})

    assert response.status_code == expected_code
    assert pool.ensure_calls == 0  # status checked before ensure()


def test_request_with_no_tenant_header_passes_through() -> None:
    pool = _CountingPool()
    app = _build_app(_FakeCatalog("active"), pool)
    client = TestClient(app)

    response = client.get("/ping")

    assert response.status_code == 200
    assert pool.ensure_calls == 0


def test_active_tenant_calls_ensure_at_most_once() -> None:
    pool = _CountingPool()
    app = _build_app(_FakeCatalog("active"), pool)
    client = TestClient(app)

    response = client.get("/ping", headers={"X-Tenant-Id": "acme"})

    assert response.status_code == 200
    assert pool.ensure_calls == 1
