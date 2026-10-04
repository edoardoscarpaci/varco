"""
Unit tests for varco_core.revocation.posture.inspect_revocation_posture()
(Plan 034 / Phase 4, Step 38, §D-034-seam). Pure function, facts only.
"""

from __future__ import annotations

import pytest


class TestInspectRevocationPostureDefaults:
    def test_no_registry_no_store_reports_unbound_and_unwired(self):
        from varco_core.revocation.posture import inspect_revocation_posture

        report = inspect_revocation_posture(registry=None, store=None)
        assert report.store_bound is False
        assert report.registry_wired is False

    def test_never_raises_with_no_args(self):
        from varco_core.revocation.posture import inspect_revocation_posture

        report = inspect_revocation_posture()
        assert report is not None


class TestInspectRevocationPostureTwoStepFootgun:
    def test_store_bound_but_registry_unwired(self):
        # §D-S13-di's two-step: binding a store in DI does not by itself
        # wire the registry to consult it.
        from varco_core.revocation.memory import InMemoryTokenRevocationStore
        from varco_core.revocation.posture import inspect_revocation_posture

        store = InMemoryTokenRevocationStore()
        report = inspect_revocation_posture(registry=None, store=store)
        assert report.store_bound is True
        assert report.registry_wired is False

    def test_registry_wired_when_registry_carries_a_non_null_store(self):
        from varco_core.authority.registry import TrustedIssuerRegistry
        from varco_core.revocation.memory import InMemoryTokenRevocationStore
        from varco_core.revocation.posture import inspect_revocation_posture

        store = InMemoryTokenRevocationStore()
        registry = TrustedIssuerRegistry(revocation_store=store)
        report = inspect_revocation_posture(registry=registry)
        assert report.registry_wired is True
        assert report.store_kind == "InMemoryTokenRevocationStore"

    def test_null_store_reports_store_kind_name(self):
        from varco_core.revocation.null import NullTokenRevocationStore
        from varco_core.revocation.posture import inspect_revocation_posture

        report = inspect_revocation_posture(store=NullTokenRevocationStore())
        assert report.store_kind == "NullTokenRevocationStore"


# ── Plan 053 / FLIP-034-3: require_jti_derived ───────────────────────────────────────────


_REQUIRE_JTI_ENV = "VARCO_JWT_REVOCATION_REQUIRE_JTI"


class TestRevocationPostureDerived:
    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        monkeypatch.delenv(_REQUIRE_JTI_ENV, raising=False)

    async def test_in_memory_wired_is_derived_true(self):
        from varco_core.authority.registry import TrustedIssuerRegistry
        from varco_core.revocation.memory import InMemoryTokenRevocationStore
        from varco_core.revocation.posture import inspect_revocation_posture

        r = inspect_revocation_posture(
            registry=TrustedIssuerRegistry(revocation_store=InMemoryTokenRevocationStore())
        )
        assert (r.require_jti, r.require_jti_derived, r.token_scope_usable) == (True, True, True)

    async def test_null_wired_is_derived_false(self):
        from varco_core.authority.registry import TrustedIssuerRegistry
        from varco_core.revocation.null import NullTokenRevocationStore
        from varco_core.revocation.posture import inspect_revocation_posture

        r = inspect_revocation_posture(
            registry=TrustedIssuerRegistry(revocation_store=NullTokenRevocationStore())
        )
        assert (r.require_jti, r.require_jti_derived, r.token_scope_usable) == (False, True, False)

    async def test_explicit_env_false_is_not_derived(self, monkeypatch):
        from varco_core.authority.registry import TrustedIssuerRegistry
        from varco_core.revocation.memory import InMemoryTokenRevocationStore
        from varco_core.revocation.posture import inspect_revocation_posture

        monkeypatch.setenv(_REQUIRE_JTI_ENV, "false")
        r = inspect_revocation_posture(
            registry=TrustedIssuerRegistry(revocation_store=InMemoryTokenRevocationStore())
        )
        assert (r.require_jti, r.require_jti_derived, r.token_scope_usable) == (False, False, False)

    async def test_no_registry_no_store(self):
        from varco_core.revocation.posture import inspect_revocation_posture

        r = inspect_revocation_posture(registry=None, store=None)
        assert r.require_jti is False
        assert r.require_jti_derived is True
