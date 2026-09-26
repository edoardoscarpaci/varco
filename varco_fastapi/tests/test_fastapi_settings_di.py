"""
New DI test file for ``varco_fastapi`` settings wiring (Plan 048 / Step 13).

Depends on ``varco_core.settings.bind_core_settings``/``bind_settings`` and
on ``varco_fastapi.di.bootstrap`` calling them instead of its own hand-rolled
``@Provider``s for ``CORSConfig``/``ClientProfile``/``ProfilingSettings``/
``SecurityPostureSettings``/``SecurityHeadersSettings``/``BodyLimitSettings``/
``RateLimitSettings`` — none of which exists yet. Every test fails with
``ImportError`` until Step 2 + Step 13 land.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_core.settings import provenance_of
from varco_fastapi.client.settings import ClientProfileSettings
from varco_fastapi.di import bootstrap
from varco_fastapi.middleware.body_limit import BodyLimitSettings
from varco_fastapi.middleware.cors import CORSConfig
from varco_fastapi.middleware.rate_limit import RateLimitSettings
from varco_fastapi.middleware.security_headers import SecurityHeadersSettings
from varco_fastapi.posture import SecurityPostureSettings

_SETTINGS_CLASSES = (
    CORSConfig,
    SecurityPostureSettings,
    SecurityHeadersSettings,
    BodyLimitSettings,
    RateLimitSettings,
    ClientProfileSettings,
)


class TestFastAPISettingsResolvable:
    def test_every_settings_class_resolvable_after_bootstrap(
        self, di_container: DIContainer
    ) -> None:
        bootstrap(di_container)

        for cls in _SETTINGS_CLASSES:
            assert di_container.get(cls) is not None

    def test_each_settings_class_has_exactly_one_binding(self, di_container: DIContainer) -> None:
        bootstrap(di_container)
        bootstrap(di_container)

        for cls in _SETTINGS_CLASSES:
            assert len(di_container.get_all_bindings(cls)) == 1

    def test_validate_is_clean_after_bootstrap(self, di_container: DIContainer) -> None:
        # varco_core.di.bootstrap() must run first — it supplies
        # AbstractJobStore (and other Class-A defaults) that varco_fastapi's
        # own JobPoller/JobRunner @Singleton classes require. Mirrors the
        # two-call sequence documented on varco_fastapi.di.bootstrap()
        # itself and used by test_fastapi_di_wiring.py's bootstrap fixture.
        from varco_core.di import bootstrap as core_bootstrap

        core_bootstrap(di_container)
        bootstrap(di_container)

        report = di_container.validate(raise_on_error=False)

        assert report.errors == ()


class TestExplicitConstructorKwargWinsOverEnv:
    def test_create_varco_app_body_limit_kwarg_wins_over_env(
        self, di_container: DIContainer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from varco_core.settings import bind_settings

        monkeypatch.setenv("VARCO_BODY_LIMIT_MAX_BYTES", "2")

        result = bind_settings(
            di_container, BodyLimitSettings, instance=BodyLimitSettings(max_bytes=1)
        )

        assert result.max_bytes == 1
        prov = provenance_of(di_container, BodyLimitSettings)
        assert prov.fields["max_bytes"] == "code"
