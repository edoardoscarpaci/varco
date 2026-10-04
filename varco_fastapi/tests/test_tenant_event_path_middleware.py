"""
Failing test for Plan 008 Phase 1, step 2 — the middleware-layer closure of
the event-path routability fix: onboard a tenant purely over the bus, then a
request through ``TenantResolutionMiddleware`` must return 200, not 404.

Imports only ``varco_core.tenancy`` (seam rule) — never ``varco_sa``/
``varco_beanie``.
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from tests._tenant_fixtures import legacy_chain


class _CountingPool:
    def __init__(self) -> None:
        self.ensure_calls = 0

    async def ensure(self, tenant_id: str):
        self.ensure_calls += 1
        return object()


class _CountingProvisioner:
    def __init__(self) -> None:
        self.provision_calls = 0

    async def provision(self, tenant_id: str, **kwargs) -> None:
        self.provision_calls += 1


def _build_control_service(pool: _CountingPool):
    from varco_core.event.memory import InMemoryEventBus
    from varco_core.event.producer import BusEventProducer
    from varco_core.tenancy.catalog import StaticTenantCatalog
    from varco_core.tenancy.control.service import TenantControlService

    bus = InMemoryEventBus()
    producer = BusEventProducer(bus)
    provisioner = _CountingProvisioner()
    service = TenantControlService(
        catalog=StaticTenantCatalog(),
        provisioner=provisioner,
        producer=producer,
    )
    return service, bus, provisioner


def _build_app(catalog, pool):
    from varco_fastapi.middleware.tenant_resolution import TenantResolutionMiddleware

    app = FastAPI()
    # Plan 055 / FLIP-6: header-only resolution named explicitly.
    app.add_middleware(TenantResolutionMiddleware, catalog=catalog, pool=pool, chain=legacy_chain())

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    return app


async def test_bus_onboarded_tenant_is_200_not_404_through_middleware() -> None:
    from varco_core.tenancy.control.consumer import TenantProvisionConsumer
    from varco_core.tenancy.control.events import (
        CHANNEL_TENANCY,
        TenantProvisionRequested,
    )

    pool = _CountingPool()
    control_service, bus, provisioner = _build_control_service(pool)

    consumer = TenantProvisionConsumer(control_service=control_service)
    consumer.register_to(bus)

    await bus.publish(TenantProvisionRequested(tenant_id="acme"), channel=CHANNEL_TENANCY)
    await bus.drain()

    catalog = control_service._catalog  # type: ignore[attr-defined]
    app = _build_app(catalog, pool)
    client = TestClient(app)

    response = client.get("/ping", headers={"X-Tenant-Id": "acme"})

    assert response.status_code == 200
    assert pool.ensure_calls == 1
