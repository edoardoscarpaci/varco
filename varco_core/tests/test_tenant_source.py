"""
Failing-first tests for varco_core.tenancy.source primitives (Plan 033,
Phase 0, Step 1).

Covers: TenantTrust ordering, TenantRequest/TenantClaim frozen-ness,
TenantSource ABC enforcement of `name`/`trust` ClassVars, and the
abstract-ness of `resolve`.
"""

from __future__ import annotations

import dataclasses

import pytest


def _import_source_module():
    # Import lazily so a missing module produces one clear ImportError per
    # test rather than a collection-time failure for the whole file.
    from varco_core.tenancy import source

    return source


class TestTenantTrustOrdering:
    def test_trust_levels_are_ordered_low_to_highest(self) -> None:
        # The whole chain's tie-break and winner logic depends on this
        # ordering being a real IntEnum comparison, not just distinct values.
        source = _import_source_module()

        assert source.TenantTrust.LOW < source.TenantTrust.MEDIUM
        assert source.TenantTrust.MEDIUM < source.TenantTrust.HIGH
        assert source.TenantTrust.HIGH < source.TenantTrust.HIGHEST

    def test_trust_is_an_int_enum(self) -> None:
        source = _import_source_module()
        import enum

        assert issubclass(source.TenantTrust, enum.IntEnum)


class TestTenantRequestFrozen:
    def test_tenant_request_is_frozen(self) -> None:
        source = _import_source_module()
        req = source.TenantRequest(headers={"host": "example.com"})

        with pytest.raises(dataclasses.FrozenInstanceError):
            req.host = "other.com"  # type: ignore[misc]

    def test_tenant_request_defaults(self) -> None:
        source = _import_source_module()
        req = source.TenantRequest(headers={})

        assert req.host is None
        assert req.path == "/"
        assert req.auth is None


class TestTenantClaimFrozen:
    def test_tenant_claim_is_frozen(self) -> None:
        source = _import_source_module()
        claim = source.TenantClaim(tenant_id="acme", source="jwt", trust=source.TenantTrust.HIGHEST)

        with pytest.raises(dataclasses.FrozenInstanceError):
            claim.tenant_id = "other"  # type: ignore[misc]

    def test_tenant_claim_fields(self) -> None:
        source = _import_source_module()
        claim = source.TenantClaim(tenant_id="acme", source="jwt", trust=source.TenantTrust.HIGHEST)

        assert claim.tenant_id == "acme"
        assert claim.source == "jwt"
        assert claim.trust is source.TenantTrust.HIGHEST


class TestTenantSourceAbc:
    def test_resolve_is_abstract(self) -> None:
        source = _import_source_module()

        class _Incomplete(source.TenantSource):
            name = "incomplete"
            trust = source.TenantTrust.LOW

        with pytest.raises(TypeError):
            _Incomplete()  # type: ignore[abstract]

    def test_subclass_missing_name_and_trust_fails_loudly(self) -> None:
        # §D-S6-abc: name/trust are ClassVar contract data an implementer
        # must declare. A subclass that forgets them and tries to use them
        # must fail loudly rather than silently default to something.
        source = _import_source_module()

        class _NoContractData(source.TenantSource):
            def resolve(self, request):  # type: ignore[override]
                return None

        # Constructing is allowed (ABC only enforces resolve()), but the
        # class-level contract data must be genuinely absent so a caller
        # relying on it explodes rather than silently reading None/wrong type.
        instance = _NoContractData()
        with pytest.raises(AttributeError):
            _ = instance.name
        with pytest.raises(AttributeError):
            _ = instance.trust

    def test_full_subclass_can_be_instantiated_and_resolves(self) -> None:
        source = _import_source_module()

        class _Stub(source.TenantSource):
            name = "stub"
            trust = source.TenantTrust.LOW

            def resolve(self, request):  # type: ignore[override]
                return None

        stub = _Stub()
        req = source.TenantRequest(headers={})
        assert stub.resolve(req) is None
