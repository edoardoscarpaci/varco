"""
Failing-first test for the tenant-membership DI wiring (Plan 033, Phase 3,
Step 25) — NullTenantMembership is the scanned @Singleton default after
container.scan("varco_core", recursive=True); enable_tenant_membership() is
the opt-in, never a scanned @Configuration.
"""

from __future__ import annotations

from providify import DIContainer


class TestNullTenantMembershipIsScannedDefault:
    async def test_scanning_varco_core_binds_null_tenant_membership(self) -> None:
        from varco_core.tenancy.membership import AbstractTenantMembership, NullTenantMembership

        container = DIContainer()
        container.scan("varco_core", recursive=True)

        provider = await container.aget(AbstractTenantMembership)

        assert isinstance(provider, NullTenantMembership)

    async def test_enable_tenant_membership_opts_in_claim_membership(self) -> None:
        from varco_core.tenancy.di import enable_tenant_membership
        from varco_core.tenancy.membership import AbstractTenantMembership, ClaimTenantMembership

        container = DIContainer()
        container.scan("varco_core", recursive=True)
        enable_tenant_membership(container)

        provider = await container.aget(AbstractTenantMembership)

        assert isinstance(provider, ClaimTenantMembership)


class TestEnableTenantMembershipSettingsSignature:
    """
    Drift-repair Decision 4: `enable_tenant_membership(container, settings=None)`
    — the plan's signature — not the interim `*, claim_key=..., on_missing_claim=...`
    keyword form.
    """

    async def test_settings_none_uses_env_backed_defaults(self) -> None:
        from varco_core.tenancy.di import enable_tenant_membership
        from varco_core.tenancy.membership import AbstractTenantMembership, ClaimTenantMembership

        container = DIContainer()
        container.scan("varco_core", recursive=True)
        enable_tenant_membership(container, settings=None)

        provider = await container.aget(AbstractTenantMembership)

        assert isinstance(provider, ClaimTenantMembership)
        assert provider.claim_key == "tenants"

    async def test_explicit_settings_object_is_honoured(self) -> None:
        from varco_core.tenancy.di import enable_tenant_membership
        from varco_core.tenancy.membership import (
            AbstractTenantMembership,
            ClaimTenantMembership,
            MissingClaimPolicy,
            TenantMembershipSettings,
        )

        container = DIContainer()
        container.scan("varco_core", recursive=True)
        enable_tenant_membership(
            container,
            settings=TenantMembershipSettings(
                claim_key="organizations", on_missing_claim=MissingClaimPolicy.DENY
            ),
        )

        provider = await container.aget(AbstractTenantMembership)

        assert isinstance(provider, ClaimTenantMembership)
        assert provider.claim_key == "organizations"
        assert provider.on_missing_claim is MissingClaimPolicy.DENY

    async def test_settings_is_positional_not_keyword_only(self) -> None:
        # The plan's signature is `enable_tenant_membership(container, settings=None)`
        # — settings is a plain positional-or-keyword parameter, not
        # keyword-only (unlike the interim claim_key=/on_missing_claim= form).
        import inspect

        from varco_core.tenancy.di import enable_tenant_membership

        sig = inspect.signature(enable_tenant_membership)
        params = list(sig.parameters.values())
        assert [p.name for p in params] == ["container", "settings"]
        assert params[1].kind in (
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.POSITIONAL_ONLY,
        )
        assert params[1].default is None


class TestTenantMembershipSettingsFromEnv:
    def test_defaults_with_no_env(self) -> None:
        from varco_core.tenancy.membership import MissingClaimPolicy, TenantMembershipSettings

        settings = TenantMembershipSettings.from_env({})

        assert settings.claim_key == "tenants"
        assert settings.on_missing_claim is MissingClaimPolicy.ALLOW

    def test_round_trips_both_vars(self) -> None:
        from varco_core.tenancy.membership import MissingClaimPolicy, TenantMembershipSettings

        settings = TenantMembershipSettings.from_env(
            {
                "VARCO_TENANT_MEMBERSHIP_CLAIM": "organizations",
                "VARCO_TENANT_MEMBERSHIP_ON_MISSING": "deny",
            }
        )

        assert settings.claim_key == "organizations"
        assert settings.on_missing_claim is MissingClaimPolicy.DENY

    def test_invalid_on_missing_raises(self) -> None:
        import pytest
        from varco_core.tenancy.membership import TenantMembershipSettings

        with pytest.raises(ValueError, match="VARCO_TENANT_MEMBERSHIP_ON_MISSING"):
            TenantMembershipSettings.from_env({"VARCO_TENANT_MEMBERSHIP_ON_MISSING": "bogus"})
