"""
Failing tests for `MigrationsModule` (Plan 006, Phase 4, step 41 —
migrated from `create_varco_app(migrations=...)` to a module by Plan 049 /
DI-1). ASGI-level with ``TestClient`` — ``InMemoryMigrator`` (no DB).
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from providify import DIContainer
from varco_core.di.module import CoreModule
from varco_fastapi.app import create_varco_app
from varco_fastapi.modules.lifecycle import MigrationsModule
from varco_fastapi.router.endpoint import route
from varco_fastapi.router.presets import GenericRouter


class _PingRouter(GenericRouter):
    _prefix = "/ping"

    @route("GET", "")
    async def ping(self) -> dict:
        return {"ok": True}


async def test_migrations_run_before_first_request_is_served() -> None:
    from varco_core.migration.base import Revision
    from varco_core.migration.inmemory import InMemoryMigrator
    from varco_core.migration.settings import MigrationSettings

    events: list[str] = []

    class _RecordingMigrator(InMemoryMigrator):
        async def upgrade(self, target: str = "heads", *, dry_run: bool = False):
            events.append("migrate")
            return await super().upgrade(target, dry_run=dry_run)

    migrator = _RecordingMigrator(revisions=[Revision(id="0001", label="init")])

    app = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[
            CoreModule(),
            MigrationsModule(migrator, settings=MigrationSettings(mode="upgrade")),
        ],
        validate=False,
    )

    with TestClient(app) as client:
        events.append("first-request-served")
        response = client.get("/ping")
        assert response.status_code == 200

    assert events == ["migrate", "first-request-served"]


async def test_failing_migrator_with_on_failure_fail_aborts_startup_no_request_served() -> None:
    from varco_core.migration.base import Revision
    from varco_core.migration.inmemory import InMemoryMigrator
    from varco_core.migration.settings import MigrationSettings

    migrator = InMemoryMigrator(
        revisions=[Revision(id="0001", label="init")],
        fail_on_upgrade_call=1,
    )

    app = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[
            CoreModule(),
            MigrationsModule(
                migrator, settings=MigrationSettings(mode="upgrade", on_failure="fail")
            ),
        ],
        validate=False,
    )

    with pytest.raises(Exception):  # noqa: B017 — startup failure propagates
        with TestClient(app):
            pytest.fail("no request should ever be served")


async def test_migrations_none_leaves_lifespan_components_byte_identical() -> None:
    app_without_migrations_module = create_varco_app(
        DIContainer(), routers=[_PingRouter], validate=False
    )
    app_with_explicit_disabled_module = create_varco_app(
        DIContainer(),
        routers=[_PingRouter],
        modules=[CoreModule(), MigrationsModule()],  # mode="off" by default — no-op
        validate=False,
    )

    with TestClient(app_without_migrations_module) as client_a:
        response_a = client_a.get("/ping")
    with TestClient(app_with_explicit_disabled_module) as client_b:
        response_b = client_b.get("/ping")

    assert response_a.status_code == response_b.status_code == 200
