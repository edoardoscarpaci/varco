"""
Failing tests for migrations inside a composite deployment (Plan 006,
Phase 4, step 46). Two sub-apps, each with their own ``InMemoryMigrator`` —
both must run in mount order, and one failing must abort the whole composite
startup (documented fail-fast composite behaviour).
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from providify import DIContainer
from varco_core.di.module import CoreModule
from varco_fastapi.app import create_varco_app
from varco_fastapi.composite import ServiceMount, create_composite_app
from varco_fastapi.modules.lifecycle import MigrationsModule
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.presets import GenericRouter


class _PingRouter(GenericRouter):
    _prefix = "/ping"

    @route("GET", "")
    async def ping(self) -> dict:
        return {"ok": True}


def _build_service(events: list[str], name: str, migrator) -> FastAPI:  # noqa: F821
    from varco_core.migration.settings import MigrationSettings

    return create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[
            CoreModule(),
            MigrationsModule(migrator, settings=MigrationSettings(mode="upgrade")),
        ],
        validate=False,
    )


async def test_both_services_migrate_in_mount_order() -> None:
    from varco_core.migration.base import Revision
    from varco_core.migration.inmemory import InMemoryMigrator

    events: list[str] = []

    class _RecordingMigrator(InMemoryMigrator):
        def __init__(self, *args, tag: str, **kwargs) -> None:
            super().__init__(*args, **kwargs)
            self._tag = tag

        async def upgrade(self, target: str = "heads", *, dry_run: bool = False):
            events.append(self._tag)
            return await super().upgrade(target, dry_run=dry_run)

    migrator_orders = _RecordingMigrator(
        revisions=[Revision(id="0001", label="init")], tag="orders"
    )
    migrator_billing = _RecordingMigrator(
        revisions=[Revision(id="0001", label="init")], tag="billing"
    )

    orders_app = _build_service(events, "orders", migrator_orders)
    billing_app = _build_service(events, "billing", migrator_billing)

    composite = create_composite_app(
        [
            ServiceMount("/orders", orders_app),
            ServiceMount("/billing", billing_app),
        ]
    )

    with TestClient(composite):
        pass

    assert events == ["orders", "billing"]


async def test_one_service_failing_migration_aborts_whole_composite_startup() -> None:
    from varco_core.migration.base import Revision
    from varco_core.migration.inmemory import InMemoryMigrator

    migrator_orders = InMemoryMigrator(revisions=[Revision(id="0001", label="init")])
    migrator_billing = InMemoryMigrator(
        revisions=[Revision(id="0001", label="init")],
        fail_on_upgrade_call=1,
    )

    orders_app = _build_service([], "orders", migrator_orders)
    billing_app = _build_service([], "billing", migrator_billing)

    composite = create_composite_app(
        [
            ServiceMount("/orders", orders_app),
            ServiceMount("/billing", billing_app),
        ]
    )

    with pytest.raises(Exception):  # noqa: B017 — fail-fast composite startup
        with TestClient(composite):
            pytest.fail("composite must never serve traffic on partial startup failure")
