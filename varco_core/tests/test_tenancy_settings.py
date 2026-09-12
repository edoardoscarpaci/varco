"""
Failing tests for varco_core.tenancy.settings (Plan 007, Phase 1, step 1).

TenantIsolation / TenantScope / TenantStatus enums + TenancySettings
(frozen, from_env()) — no backend deps.
"""

from __future__ import annotations

import pytest


def test_default_settings_match_documented_defaults() -> None:
    from varco_core.tenancy.settings import TenancySettings, TenantIsolation

    settings = TenancySettings()

    assert settings.isolation == TenantIsolation.SHARED
    assert settings.enforce_rls is False
    assert settings.max_entries == 50
    assert settings.idle_ttl_s == 300.0
    assert settings.catalog_ttl_s == 60.0
    assert settings.fanout_framework_tables is False
    assert settings.global_writable is False


def test_settings_is_frozen() -> None:
    from varco_core.tenancy.settings import TenancySettings

    settings = TenancySettings()

    with pytest.raises(Exception):  # noqa: B017 - FrozenInstanceError from dataclasses
        settings.max_entries = 100  # type: ignore[misc]


def test_from_env_with_empty_environ_yields_documented_defaults() -> None:
    from varco_core.tenancy.settings import TenancySettings, TenantIsolation

    settings = TenancySettings.from_env({})

    assert settings.isolation == TenantIsolation.SHARED
    assert settings.enforce_rls is False
    assert settings.max_entries == 50
    assert settings.idle_ttl_s == 300.0
    assert settings.catalog_ttl_s == 60.0
    assert settings.fanout_framework_tables is False
    assert settings.global_writable is False


def test_from_env_parses_all_documented_keys() -> None:
    from varco_core.tenancy.settings import TenancySettings, TenantIsolation

    settings = TenancySettings.from_env(
        {
            "VARCO_TENANCY_ISOLATION": "schema",
            "VARCO_TENANCY_ENFORCE_RLS": "true",
            "VARCO_TENANCY_SCHEMA_TEMPLATE": "t_{tenant_id}",
            "VARCO_TENANCY_DB_TEMPLATE": "db_{tenant_id}",
            "VARCO_TENANCY_MAX_ENTRIES": "10",
            "VARCO_TENANCY_IDLE_TTL": "15.0",
            "VARCO_TENANCY_CATALOG_TTL": "5.0",
            "VARCO_TENANCY_FANOUT_FRAMEWORK_TABLES": "true",
            "VARCO_TENANCY_GLOBAL_DSN": "postgresql://global",
            "VARCO_TENANCY_GLOBAL_WRITABLE": "true",
        }
    )

    assert settings.isolation == TenantIsolation.SCHEMA
    assert settings.enforce_rls is True
    assert settings.schema_template == "t_{tenant_id}"
    assert settings.db_template == "db_{tenant_id}"
    assert settings.max_entries == 10
    assert settings.idle_ttl_s == 15.0
    assert settings.catalog_ttl_s == 5.0
    assert settings.fanout_framework_tables is True
    assert settings.global_dsn == "postgresql://global"
    assert settings.global_writable is True


def test_from_env_invalid_isolation_raises_value_error() -> None:
    from varco_core.tenancy.settings import TenancySettings

    with pytest.raises(ValueError):
        TenancySettings.from_env({"VARCO_TENANCY_ISOLATION": "nonsense"})


def test_from_env_ignores_mount_admin_env_var_entirely() -> None:
    """RD-9: no env var can ever mount/enable the privileged admin surface.

    TenancySettings has no field corresponding to a "mount admin" env var,
    and setting a plausible-looking one must have zero effect on the
    settings object (it must not even be recognised/parsed).
    """
    from varco_core.tenancy.settings import TenancySettings

    settings = TenancySettings.from_env({"VARCO_TENANCY_MOUNT_ADMIN": "true"})

    assert not hasattr(settings, "mount_admin")
    assert "mount_admin" not in settings.__dataclass_fields__  # type: ignore[attr-defined]


# ════════════════════════════════════════════════════════════════════════════════
# Plan 037 / Step 12 — §D-S12-hook: two new, additive, off-by-default flags.
#
# ``enforce_rls`` means one thing only (RLS is enabled on every routed
# table); these two flags are about the GUC-setting hook, not the assertion,
# and must default to False so an app that upgrades and changes no code sees
# byte-identical behaviour (§D-S12-hook's "DESIGN" block).
# ════════════════════════════════════════════════════════════════════════════════


def test_rls_set_tenant_and_rls_require_tenant_default_false() -> None:
    from varco_core.tenancy.settings import TenancySettings

    settings = TenancySettings()

    assert settings.rls_set_tenant is False
    assert settings.rls_require_tenant is False


def test_from_env_empty_environ_rls_flags_still_default_false() -> None:
    from varco_core.tenancy.settings import TenancySettings

    settings = TenancySettings.from_env({})

    assert settings.rls_set_tenant is False
    assert settings.rls_require_tenant is False


def test_from_env_parses_rls_set_tenant_true() -> None:
    from varco_core.tenancy.settings import TenancySettings

    settings = TenancySettings.from_env({"VARCO_TENANCY_RLS_SET_TENANT": "true"})

    assert settings.rls_set_tenant is True


def test_from_env_parses_rls_require_tenant_via_numeric_one() -> None:
    # Reuses the existing _bool helper (settings.py:148-152), which accepts
    # "1"/"true"/"yes"/"on" — asserted here via the "1" spelling specifically.
    from varco_core.tenancy.settings import TenancySettings

    settings = TenancySettings.from_env({"VARCO_TENANCY_RLS_REQUIRE_TENANT": "1"})

    assert settings.rls_require_tenant is True


def test_byte_identical_defaults_assertion_still_passes_with_new_fields_present() -> None:
    # Re-affirms the plan's "nothing happens on upgrade" claim: every
    # pre-existing documented default (test_default_settings_match_documented_
    # defaults, above) must remain true even though two new fields exist.
    from varco_core.tenancy.settings import TenancySettings, TenantIsolation

    settings = TenancySettings()

    assert settings.isolation == TenantIsolation.SHARED
    assert settings.enforce_rls is False
    assert settings.max_entries == 50
    assert settings.idle_ttl_s == 300.0
    assert settings.catalog_ttl_s == 60.0
    assert settings.fanout_framework_tables is False
    assert settings.global_writable is False
    assert settings.rls_set_tenant is False
    assert settings.rls_require_tenant is False
