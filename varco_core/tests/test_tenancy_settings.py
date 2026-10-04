"""
Tests for varco_core.tenancy.settings (Plan 007, Phase 1, step 1;
converted to pydantic ``VarcoSettings`` by Plan 048 / DI-6, Step 5).

TenantIsolation / TenantScope / TenantStatus enums + TenancySettings
(frozen, ``from_env()``) — no backend deps.
"""

from __future__ import annotations

import pytest


def test_default_settings_match_documented_defaults() -> None:
    from varco_core.tenancy.settings import TenancySettings, TenantIsolation

    settings = TenancySettings()

    assert settings.isolation == TenantIsolation.SHARED
    assert settings.enforce_rls is True  # Plan 056 / FLIP-12: default flipped
    assert settings.max_entries == 50
    assert settings.idle_ttl_s == 300.0
    assert settings.catalog_ttl_s == 60.0
    assert settings.fanout_framework_tables is False
    assert settings.global_writable is False


def test_settings_is_frozen() -> None:
    from varco_core.tenancy.settings import TenancySettings

    settings = TenancySettings()

    with pytest.raises(Exception):  # noqa: B017 - pydantic ValidationError on a frozen model
        settings.max_entries = 100  # type: ignore[misc]


def test_from_env_with_empty_environ_yields_documented_defaults() -> None:
    from varco_core.tenancy.settings import TenancySettings, TenantIsolation

    settings = TenancySettings.from_env()

    assert settings.isolation == TenantIsolation.SHARED
    assert settings.enforce_rls is True  # Plan 056 / FLIP-12: default flipped
    assert settings.max_entries == 50
    assert settings.idle_ttl_s == 300.0
    assert settings.catalog_ttl_s == 60.0
    assert settings.fanout_framework_tables is False
    assert settings.global_writable is False


def test_from_env_parses_all_documented_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_core.tenancy.settings import TenancySettings, TenantIsolation

    monkeypatch.setenv("VARCO_TENANCY_ISOLATION", "schema")
    monkeypatch.setenv("VARCO_TENANCY_ENFORCE_RLS", "true")
    monkeypatch.setenv("VARCO_TENANCY_SCHEMA_TEMPLATE", "t_{tenant_id}")
    monkeypatch.setenv("VARCO_TENANCY_DB_TEMPLATE", "db_{tenant_id}")
    monkeypatch.setenv("VARCO_TENANCY_MAX_ENTRIES", "10")
    monkeypatch.setenv("VARCO_TENANCY_IDLE_TTL", "15.0")
    monkeypatch.setenv("VARCO_TENANCY_CATALOG_TTL", "5.0")
    monkeypatch.setenv("VARCO_TENANCY_FANOUT_FRAMEWORK_TABLES", "true")
    monkeypatch.setenv("VARCO_TENANCY_GLOBAL_DSN", "postgresql://global")
    monkeypatch.setenv("VARCO_TENANCY_GLOBAL_WRITABLE", "true")

    settings = TenancySettings.from_env()

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


def test_from_env_invalid_isolation_raises_value_error(monkeypatch: pytest.MonkeyPatch) -> None:
    from pydantic import ValidationError
    from varco_core.tenancy.settings import TenancySettings

    monkeypatch.setenv("VARCO_TENANCY_ISOLATION", "nonsense")

    with pytest.raises(ValidationError):
        TenancySettings.from_env()


def test_from_env_ignores_mount_admin_env_var_entirely(monkeypatch: pytest.MonkeyPatch) -> None:
    """RD-9: no env var can ever mount/enable the privileged admin surface.

    TenancySettings has no field corresponding to a "mount admin" env var,
    and setting a plausible-looking one must have zero effect on the
    settings object (it must not even be recognised/parsed).
    """
    from varco_core.tenancy.settings import TenancySettings

    monkeypatch.setenv("VARCO_TENANCY_MOUNT_ADMIN", "true")

    settings = TenancySettings.from_env()

    assert not hasattr(settings, "mount_admin")
    assert "mount_admin" not in type(settings).model_fields


# ════════════════════════════════════════════════════════════════════════════════
# Plan 037 / Step 12 — §D-S12-hook: two new, additive, off-by-default flags.
#
# ``enforce_rls`` (default True since 4.0, Plan 056 / FLIP-12) gates the
# startup RLS posture read and never sets the GUC; these two flags are about the GUC-setting hook, not the assertion,
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

    settings = TenancySettings.from_env()

    assert settings.rls_set_tenant is False
    assert settings.rls_require_tenant is False


def test_from_env_parses_rls_set_tenant_true(monkeypatch: pytest.MonkeyPatch) -> None:
    from varco_core.tenancy.settings import TenancySettings

    monkeypatch.setenv("VARCO_TENANCY_RLS_SET_TENANT", "true")

    settings = TenancySettings.from_env()

    assert settings.rls_set_tenant is True


def test_from_env_parses_rls_require_tenant_via_numeric_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from varco_core.tenancy.settings import TenancySettings

    monkeypatch.setenv("VARCO_TENANCY_RLS_REQUIRE_TENANT", "1")

    settings = TenancySettings.from_env()

    assert settings.rls_require_tenant is True


def test_byte_identical_defaults_assertion_still_passes_with_new_fields_present() -> None:
    # Re-affirms the plan's "nothing happens on upgrade" claim: every
    # pre-existing documented default (test_default_settings_match_documented_
    # defaults, above) must remain true even though two new fields exist.
    from varco_core.tenancy.settings import TenancySettings, TenantIsolation

    settings = TenancySettings()

    assert settings.isolation == TenantIsolation.SHARED
    assert settings.enforce_rls is True  # Plan 056 / FLIP-12: default flipped
    assert settings.max_entries == 50
    assert settings.idle_ttl_s == 300.0
    assert settings.catalog_ttl_s == 60.0
    assert settings.fanout_framework_tables is False
    assert settings.global_writable is False
    assert settings.rls_set_tenant is False
    assert settings.rls_require_tenant is False


def test_node_id_and_store_id_default_none_and_read_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Plan 048 / §D-loose: fold of the loose VARCO_TENANCY_NODE_ID/STORE_ID reads."""
    from varco_core.tenancy.settings import TenancySettings

    assert TenancySettings().node_id is None
    assert TenancySettings().store_id is None

    monkeypatch.setenv("VARCO_TENANCY_NODE_ID", "node-1")
    monkeypatch.setenv("VARCO_TENANCY_STORE_ID", "store-1")

    settings = TenancySettings.from_env()

    assert settings.node_id == "node-1"
    assert settings.store_id == "store-1"


# ════════════════════════════════════════════════════════════════════════════════
# Plan 056 / FLIP-12 — enforce_rls defaults to True; env "false" is the hatch.
# ════════════════════════════════════════════════════════════════════════════════


@pytest.mark.parametrize("raw", ["false", "0", "no", "off"])
def test_enforce_rls_env_false_is_the_hatch(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    # The one greppable opt-out line must turn the flipped default back off.
    from varco_core.tenancy.settings import TenancySettings

    monkeypatch.setenv("VARCO_TENANCY_ENFORCE_RLS", raw)

    assert TenancySettings.from_env().enforce_rls is False


def test_enforce_rls_default_is_true_field_guard() -> None:
    # Class-level default guard: api_surface.py --check cannot see class fields.
    from varco_core.tenancy.settings import TenancySettings

    assert TenancySettings.model_fields["enforce_rls"].default is True


def test_every_other_default_is_unchanged() -> None:
    # "Byte-identical to pre-Plan-007 except enforce_rls" — made mechanical.
    from varco_core.tenancy.settings import TenancySettings, TenantIsolation

    s = TenancySettings()

    assert s.isolation == TenantIsolation.SHARED
    assert s.rls_set_tenant is False
    assert s.rls_require_tenant is False
    assert s.assert_tenant_filter is False
    assert s.fanout_framework_tables is False
    assert s.global_writable is False
    assert s.global_dsn is None
    assert s.schema_template == "t_{tenant_id}"
    assert s.db_template == "db_{tenant_id}"
    assert s.max_entries == 50
    assert s.idle_ttl_s == 300.0
    assert s.catalog_ttl_s == 60.0
