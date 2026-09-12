"""
Unit tests for varco_core.revocation.posture.inspect_revocation_posture()
(Plan 034 / Phase 4, Step 38, §D-034-seam). Pure function, facts only.
"""

from __future__ import annotations


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
