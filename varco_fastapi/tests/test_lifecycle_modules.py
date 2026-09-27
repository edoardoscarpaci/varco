"""
Plan 049 / Step 7 — subsystem lifecycle modules
(`varco_fastapi/varco_fastapi/modules/lifecycle.py`), Docker-free.

RED MODE: `varco_fastapi.modules.lifecycle` does not exist yet — every test
below is expected to fail with `ImportError`.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from providify import DIContainer
from varco_fastapi.modules.lifecycle import (
    JwksRefreshModule,
    MigrationsModule,
    ReliabilityModule,
    RetentionModule,
    SecurityPostureModule,
    TenancyModule,
)


class TestMigrationsModule:
    async def test_disabled_when_mode_unset(self) -> None:
        # PLAN-AMBIGUITY: MigrationSettings' default `mode` value — assumed
        # "off" per §D-core/§Context row's "off (default, nothing
        # registered)" language re-used from the schema-migrations feature.
        container = DIContainer()
        m = MigrationsModule()
        m.register(container)

        assert m.is_enabled(container) is False

    async def test_enabled_with_settings_mode_check(self) -> None:
        from varco_core.migration import MigrationSettings

        container = DIContainer()
        m = MigrationsModule(settings=MigrationSettings(mode="check"))
        m.register(container)

        assert m.is_enabled(container) is True

    async def test_start_stop_delegate_to_migration_lifecycle(self) -> None:
        from varco_fastapi import migrate as migrate_module

        mock_lifecycle = MagicMock(spec=migrate_module.MigrationLifecycle)
        mock_lifecycle.start = AsyncMock()
        mock_lifecycle.stop = AsyncMock()

        container = DIContainer()
        m = MigrationsModule()
        m.register(container)
        m._lifecycle = mock_lifecycle  # PLAN-AMBIGUITY: the internal attribute
        # name the module stores its wrapped Lifecycle under is not pinned
        # by the plan text ("wraps MigrationLifecycle"); assumed `_lifecycle`.

        await m.start(container)
        await m.stop(container)

        mock_lifecycle.start.assert_awaited_once()
        mock_lifecycle.stop.assert_awaited_once()


class TestTenancyModule:
    async def test_wraps_tenancy_lifecycle(self) -> None:
        from varco_fastapi import tenancy as tenancy_module

        mock_lifecycle = MagicMock(spec=tenancy_module.TenancyLifecycle)
        mock_lifecycle.start = AsyncMock()
        mock_lifecycle.stop = AsyncMock()

        container = DIContainer()
        m = TenancyModule(pool=object())
        m.register(container)
        m._lifecycle = mock_lifecycle

        await m.start(container)
        await m.stop(container)

        mock_lifecycle.start.assert_awaited_once()
        mock_lifecycle.stop.assert_awaited_once()


class TestReliabilityModule:
    async def test_built_in_start_with_containers_own_container(self) -> None:
        preset = object()
        container = DIContainer()
        m = ReliabilityModule(preset)
        m.register(container)

        assert m._preset is preset  # PLAN-AMBIGUITY: internal attr name assumed


class TestRetentionModule:
    async def test_default_interval_is_zero(self) -> None:
        registry = object()
        container = DIContainer()
        m = RetentionModule(registry)
        m.register(container)

        assert m._interval == 0.0  # PLAN-AMBIGUITY: internal attr name assumed


class TestJwksRefreshModule:
    async def test_default_interval_is_none(self) -> None:
        registry = object()
        container = DIContainer()
        m = JwksRefreshModule(registry)
        m.register(container)

        assert m._interval is None  # PLAN-AMBIGUITY: internal attr name assumed


class TestSecurityPostureModule:
    async def test_default_is_enabled_true(self) -> None:
        container = DIContainer()
        m = SecurityPostureModule()
        m.register(container)

        assert m.is_enabled(container) is True

    async def test_env_var_disables(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VARCO_SECURITY_POSTURE_ENABLED", "false")
        container = DIContainer()
        m = SecurityPostureModule()
        m.register(container)

        assert m.is_enabled(container) is False

    async def test_enabled_true_beats_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VARCO_SECURITY_POSTURE_ENABLED", "false")
        container = DIContainer()
        m = SecurityPostureModule(enabled=True)
        m.register(container)

        assert m.is_enabled(container) is True

    async def test_start_populates_report_with_at_least_one_finding(self) -> None:
        # PLAN-AMBIGUITY: the plan states "after start() on a default
        # container module.report has one finding per default collector
        # family (the existing test_security_posture*.py shape)" but does
        # not name the exact family count here; this test asserts the
        # report exists and is non-empty, deferring the exact per-family
        # count to test_security_posture*.py's own assertions.
        container = DIContainer()
        m = SecurityPostureModule()
        m.register(container)

        await m.start(container)

        assert len(m.report.findings) >= 1

    async def test_collectors_kwarg_replaces_default_set(self) -> None:
        called = {"count": 0}

        def _collector() -> list[object]:
            called["count"] += 1
            return []

        container = DIContainer()
        m = SecurityPostureModule(collectors=[_collector])
        m.register(container)

        await m.start(container)

        assert called["count"] == 1
