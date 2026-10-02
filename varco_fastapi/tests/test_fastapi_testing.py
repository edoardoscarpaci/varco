"""Plan 052 Step 10(e) — ``varco_fastapi.testing.in_memory_app_modules()`` boots under TestClient."""

from __future__ import annotations

from fastapi.testclient import TestClient
from providify import DIContainer
from varco_core.di import bind_instance
from varco_core.di.module import CoreModule
from varco_core.testing import InMemoryProfile
from varco_fastapi.app import create_varco_app
from varco_fastapi.auth.server_auth import AbstractServerAuth, AnonymousAuth
from varco_fastapi.di import bootstrap as fastapi_bootstrap
from varco_fastapi.modules import JwksRefreshModule, RetentionModule, SecurityPostureModule
from varco_fastapi.testing import in_memory_app_modules


def test_in_memory_app_modules_shape() -> None:
    modules = in_memory_app_modules()

    posture = next(m for m in modules if isinstance(m, SecurityPostureModule))
    assert posture._settings_arg is not None
    assert posture._settings_arg.enforce == "warn"
    assert posture._settings_arg.environment == "development"
    assert not any(isinstance(m, (JwksRefreshModule, RetentionModule)) for m in modules)
    assert isinstance(modules[-1], InMemoryProfile)
    core = next(m for m in modules if isinstance(m, CoreModule))
    assert core._job_runner is False


def test_in_memory_app_modules_boots_under_testclient() -> None:
    container = DIContainer()
    fastapi_bootstrap(container)
    bind_instance(container, AbstractServerAuth, AnonymousAuth())
    app = create_varco_app(container, modules=in_memory_app_modules())

    with TestClient(app) as client:
        assert client.get("/health").status_code == 200

    report = app.state.varco_wiring_report
    assert report.enforce == "warn"
    assert report.environment == "development"
    assert report.enforce_source == "code"
