"""
Failing tests for varco_fastapi.tenancy.lifecycle.TenancyLifecycle (Plan 007,
Phase 10, step 1).
"""

from __future__ import annotations

import logging


async def test_start_starts_sweeper_invalidation_subscription_and_supervisor() -> None:
    from varco_fastapi.tenancy.lifecycle import TenancyLifecycle

    calls: list[str] = []

    class _FakePool:
        async def start_sweeper(self) -> None:
            calls.append("sweeper")

        async def aclose(self) -> None:
            calls.append("pool.aclose")

    class _FakeSupervisor:
        async def start(self) -> None:
            calls.append("supervisor.start")

        async def stop(self) -> None:
            calls.append("supervisor.stop")

    lifecycle = TenancyLifecycle(pool=_FakePool(), supervisor=_FakeSupervisor())

    await lifecycle.start()

    assert "sweeper" in calls
    assert "supervisor.start" in calls


async def test_stop_stops_supervisor_before_pool_aclose() -> None:
    from varco_fastapi.tenancy.lifecycle import TenancyLifecycle

    calls: list[str] = []

    class _FakePool:
        async def start_sweeper(self) -> None:
            pass

        async def aclose(self) -> None:
            calls.append("pool.aclose")

    class _FakeSupervisor:
        async def start(self) -> None:
            pass

        async def stop(self) -> None:
            calls.append("supervisor.stop")

    lifecycle = TenancyLifecycle(pool=_FakePool(), supervisor=_FakeSupervisor())
    await lifecycle.start()
    await lifecycle.stop()

    assert calls.index("supervisor.stop") < calls.index("pool.aclose")


def test_create_varco_app_with_tenancy_none_registers_nothing() -> None:
    from providify import DIContainer
    from varco_fastapi.app import create_varco_app

    # validate=False — an empty DIContainer() has no AbstractServerAuth
    # binding, so default validate=True would fail on unrelated DI
    # validation before this test's actual assertion is ever reached (same
    # pattern as test_app_migrations.py's equivalent create_varco_app()
    # calls).
    app = create_varco_app(DIContainer(), validate=False)

    assert not any("tenancy" in str(route.path).lower() for route in app.router.routes)


def test_isolation_env_var_without_tenancy_kwarg_logs_one_warning(
    monkeypatch, caplog: logging.LogCaptureFixture
) -> None:
    """
    Plan 052: the build-time ``_warn_unowned_settings`` log became the
    ``settings.unowned`` wiring finding, logged at WARNING when the lifespan
    runs the startup preflight (same text, new place).
    """
    monkeypatch.setenv("VARCO_TENANCY_ISOLATION", "schema")
    from fastapi.testclient import TestClient
    from providify import DIContainer
    from varco_fastapi.app import create_varco_app

    app = create_varco_app(DIContainer(), validate=False)
    with caplog.at_level(logging.WARNING), TestClient(app):
        pass

    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert any("VARCO_TENANCY_ISOLATION" in r.getMessage() for r in warnings)
