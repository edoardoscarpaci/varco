"""
New DI test file for ``varco_casbin`` (Plan 048 / Step 12) — the package has
no DI test today (scout §10). Asserts ``CasbinSettings`` is bound via
``bind_settings()`` inside ``bootstrap()``, exactly once across two calls,
and that a bad env value fails bootstrap loudly.

Depends on ``varco_core.settings`` and on ``varco_casbin.di.bootstrap`` no
longer using its own hand-rolled ``@Provider`` for ``CasbinSettings`` — both
of which are pre-Step-12/Step-2 state. Every test fails today, for the right
reason: ``ImportError`` on ``varco_core.settings``.
"""

from __future__ import annotations

import pytest
from providify import DIContainer
from varco_casbin.config import CasbinSettings
from varco_casbin.di import bootstrap
from varco_core.settings import SettingsValidationError


class TestCasbinSettingsBoundOnce:
    def test_settings_bound_once_after_two_bootstrap_calls(self, di_container: DIContainer) -> None:
        bootstrap(di_container)
        bootstrap(di_container)

        assert len(di_container.get_all_bindings(CasbinSettings)) == 1

    def test_bootstrap_resolves_a_casbin_settings_instance(self, di_container: DIContainer) -> None:
        bootstrap(di_container)

        settings = di_container.get(CasbinSettings)

        assert isinstance(settings, CasbinSettings)


class TestBadEnvFailsBootstrap:
    def test_bad_env_fails_bootstrap_naming_the_var(
        self, di_container: DIContainer, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("VARCO_CASBIN_MODEL_PRESET", "not-a-real-preset")

        with pytest.raises(SettingsValidationError) as exc:
            bootstrap(di_container)

        assert "VARCO_CASBIN_MODEL_PRESET" in str(exc.value)
