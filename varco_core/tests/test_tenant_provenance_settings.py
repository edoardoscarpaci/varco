"""
Tests for varco_core.tenancy.settings.TenantProvenanceSettings and
build_tenant_source_chain() (Plan 033, Phase 2, Step 12) — §D-S6-settings.
Converted to pydantic ``VarcoSettings`` by Plan 048 / DI-6, Step 5.
"""

from __future__ import annotations

import pytest


def _mod():
    from varco_core.tenancy import settings

    return settings


class TestDefaultsProduceNoChain:
    def test_no_env_means_no_chain(self) -> None:
        mod = _mod()
        settings_obj = mod.TenantProvenanceSettings.from_env()
        assert mod.build_tenant_source_chain(settings_obj) is None

    def test_build_with_no_settings_arg_reads_real_env_and_defaults_to_none(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        mod = _mod()
        monkeypatch.delenv("VARCO_TENANT_SOURCES", raising=False)
        assert mod.build_tenant_source_chain() is None


class TestSubdomainRequiresBaseDomains:
    def test_missing_base_domains_raises_naming_the_var(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from pydantic import ValidationError

        mod = _mod()
        monkeypatch.setenv("VARCO_TENANT_SOURCES", "jwt,subdomain")
        with pytest.raises(ValidationError, match="VARCO_TENANT_BASE_DOMAINS"):
            mod.TenantProvenanceSettings.from_env()


class TestFullParseRoundTrip:
    def test_every_var_round_trips(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mod = _mod()
        monkeypatch.setenv("VARCO_TENANT_SOURCES", "jwt,subdomain,legacy")
        monkeypatch.setenv("VARCO_TENANT_CROSS_CHECK", "strict")
        monkeypatch.setenv("VARCO_TENANT_MIN_TRUST", "medium")
        monkeypatch.setenv("VARCO_TENANT_CLAIM_METADATA_KEY", "org_id")
        monkeypatch.setenv("VARCO_TENANT_BASE_DOMAINS", "example.com,example.co.uk")
        monkeypatch.setenv("VARCO_TENANT_TRUST_FORWARDED_HOST", "true")
        monkeypatch.setenv("VARCO_TENANT_FORWARDED_HOST_HEADER", "X-Real-Host")
        monkeypatch.setenv("VARCO_TENANT_RESERVED_LABELS", "www,internal")
        monkeypatch.setenv("VARCO_TENANT_LEGACY_HEADER", "X-Org-Id")

        settings_obj = mod.TenantProvenanceSettings.from_env()

        assert settings_obj.sources == ("jwt", "subdomain", "legacy")
        assert settings_obj.cross_check.value == "strict"
        assert settings_obj.base_domains == ("example.com", "example.co.uk")
        assert settings_obj.trust_forwarded_host is True
        assert settings_obj.forwarded_host_header == "X-Real-Host"
        assert settings_obj.legacy_header == "X-Org-Id"

        chain = mod.build_tenant_source_chain(settings_obj)
        assert chain is not None
        assert [s.name for s in chain.sources] == ["jwt", "subdomain", "legacy"]


class TestUnknownSourceName:
    def test_unknown_source_raises_listing_legal_set(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from pydantic import ValidationError

        mod = _mod()
        monkeypatch.setenv("VARCO_TENANT_SOURCES", "jwt,bogus")
        with pytest.raises(ValidationError) as exc:
            mod.TenantProvenanceSettings.from_env()
        message = str(exc.value)
        assert "bogus" in message
        for legal in ("jwt", "subdomain", "legacy", "act_as"):
            assert legal in message


class TestCrossCheckStrictParses:
    def test_strict_parses(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mod = _mod()
        monkeypatch.setenv("VARCO_TENANT_SOURCES", "legacy")
        monkeypatch.setenv("VARCO_TENANT_CROSS_CHECK", "strict")
        settings_obj = mod.TenantProvenanceSettings.from_env()
        assert settings_obj.cross_check.value == "strict"
