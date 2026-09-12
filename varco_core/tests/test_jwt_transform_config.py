"""
Unit tests for env-driven JWT claim transform configuration.

Covers (Plan 002, Phase 2, step 16):
    - VARCO_JWT_TRANSFORM_ROLES_FIELD headline scenario (no explicit transformer=).
    - Comma fallback chains + MERGE_SOURCES.
    - SCOPES_FIELD with space-delimited OAuth2 scope claim.
    - TENANT_FIELD → metadata["tenant_id"] (TenantAwareService integration).
    - ROLES_STRIP_PREFIX / ROLES_SHAPE / ROLES_REQUIRED.
    - PATH_SEPARATOR / STRICT.
    - Per-issuer mappings (VARCO_JWT_TRANSFORM__<LABEL>__*), inheritance,
      __ISS fallback to FASTREST_AUTHORIZATION__<LABEL>__ISS, label normalisation.
    - Unmapped issuer → IDENTITY, no error.
    - Conflicting __ISS across two labels → ClaimTransformError at load time.
    - Unknown claim path → empty (non-required) / ClaimTransformError (required).
    - extra="ignore" safety for per-issuer vars sharing the global prefix.
    - configure_claim_transforms()/reset_claim_transforms() override + restore.

These tests are written RED — ``varco_core.jwt.transform.config``,
``varco_core.jwt.transform.registry``, and the env-aware
``varco_core.jwt.transform.runtime`` do not exist yet. They must fail with
ImportError until Phase 2 lands.

Uses ``monkeypatch.setenv`` exclusively — the autouse fixture in conftest.py
resets the global claim-transform registry before and after every test.
"""

from __future__ import annotations

import jwt as _pyjwt
import pytest
from varco_core.jwt import JwtParser

_SECRET = "test-secret-do-not-use-in-production"


def _sign(**claims) -> str:
    """
    Sign a token carrying arbitrary raw claims, including claim names that
    ``JwtBuilder.claim()`` would normally reject as reserved (e.g. a raw
    ``roles``/``grants`` claim shaped in a non-canonical way, for testing
    the claim-transform layer's handling of the pre-transform payload).

    TEST-FIXTURE NOTE: encodes via PyJWT directly rather than
    ``JwtBuilder`` — the reserved-claim-key guard on
    ``JwtBuilder.claim()`` is an app-code safety net (see
    ``test_jwt.py::test_claim_raises_on_reserved_key``); it must not apply
    to a test helper whose entire purpose is constructing raw claim dicts
    the claim-transform layer is supposed to reshape.
    """
    payload: dict = {"sub": "usr_1", **claims}
    return _pyjwt.encode(payload, _SECRET, algorithm="HS256")


class TestGlobalRolesField:
    def test_roles_field_env_var_maps_foreign_claim(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "sofy-roles")
        signed = _sign(**{"sofy-roles": ["editor"]})
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx is not None
        assert token.auth_ctx.roles == frozenset({"editor"})


class TestFallbackChainAndMerge:
    def test_comma_chain_first_non_empty_wins(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "sofy-roles,realm_access.roles")
        signed = _sign(realm_access={"roles": ["viewer"]})
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"viewer"})

    def test_merge_sources_true_unions_chain(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "sofy-roles,realm_access.roles")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_MERGE_SOURCES", "true")
        signed = _sign(**{"sofy-roles": ["editor"]}, realm_access={"roles": ["viewer"]})
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"editor", "viewer"})


class TestScopesField:
    def test_scopes_field_space_delimited(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_SCOPES_FIELD", "scope")
        signed = _sign(scope="read write")
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.scopes == frozenset({"read", "write"})


class TestTenantField:
    def test_tenant_field_lands_in_metadata(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_TENANT_FIELD", "org.id")
        signed = _sign(org={"id": "t_1"})
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.metadata["tenant_id"] == "t_1"

    def test_tenant_field_satisfies_tenant_aware_service_requirement(self, monkeypatch):
        from varco_core.service.tenant import TenantAwareService

        monkeypatch.setenv("VARCO_JWT_TRANSFORM_TENANT_FIELD", "org.id")
        signed = _sign(org={"id": "t_1"})
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        # Just proves the metadata key TenantAwareService reads is populated —
        # not exercising the full service instantiation here.
        assert "tenant_id" in token.auth_ctx.metadata
        assert TenantAwareService is not None


class TestStripPrefixShapeRequired:
    def test_roles_strip_prefix(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "roles")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_STRIP_PREFIX", "ROLE_")
        signed = _sign(roles=["ROLE_admin"])
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"admin"})

    def test_roles_shape_csv(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "roles")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_SHAPE", "csv")
        signed = _sign(roles="admin,editor")
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"admin", "editor"})

    def test_roles_required_true_missing_raises(self, monkeypatch):
        from varco_core.jwt.exceptions import ClaimTransformError

        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "sofy-roles")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_REQUIRED", "true")
        signed = _sign(sub_marker=True)  # no sofy-roles claim
        with pytest.raises(ClaimTransformError):
            JwtParser.parse(signed, _SECRET, algorithms=["HS256"])


class TestSeparatorAndStrict:
    def test_path_separator_override(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "realm_access:roles")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_PATH_SEPARATOR", ":")
        signed = _sign(realm_access={"roles": ["editor"]})
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"editor"})

    def test_strict_true_raises_on_bad_shape(self, monkeypatch):
        from varco_core.jwt.exceptions import ClaimTransformError

        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "roles")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_STRICT", "true")
        signed = _sign(roles=5)
        with pytest.raises(ClaimTransformError):
            JwtParser.parse(signed, _SECRET, algorithms=["HS256"])


class TestPerIssuerMapping:
    def test_per_issuer_mapping_applies_only_to_matching_iss(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__KEYCLOAK__ISS", "kc-issuer")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__KEYCLOAK__ROLES_FIELD", "realm_access.roles")
        signed = _sign(iss="kc-issuer", realm_access={"roles": ["kc-role"]})
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"kc-role"})

    def test_other_issuer_gets_global_mapping(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__KEYCLOAK__ISS", "kc-issuer")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__KEYCLOAK__ROLES_FIELD", "realm_access.roles")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "roles")
        signed = _sign(iss="some-other-issuer", roles=["global-role"])
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"global-role"})

    def test_per_issuer_inherits_unspecified_global_fields(self, monkeypatch):
        # Label declares only ROLES_FIELD; global SCOPES_FIELD/STRICT still apply.
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__KEYCLOAK__ISS", "kc-issuer")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__KEYCLOAK__ROLES_FIELD", "realm_access.roles")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_SCOPES_FIELD", "scope")
        signed = _sign(
            iss="kc-issuer",
            realm_access={"roles": ["kc-role"]},
            scope="read write",
        )
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"kc-role"})
        assert token.auth_ctx.scopes == frozenset({"read", "write"})

    def test_iss_fallback_to_authorization_config_label(self, monkeypatch):
        # No __ISS on the transform label — falls back to
        # FASTREST_AUTHORIZATION__KEYCLOAK__ISS.
        monkeypatch.setenv("FASTREST_AUTHORIZATION__KEYCLOAK__ISS", "kc-fallback-iss")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__KEYCLOAK__ROLES_FIELD", "realm_access.roles")
        signed = _sign(iss="kc-fallback-iss", realm_access={"roles": ["kc-role"]})
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"kc-role"})

    def test_iss_fallback_to_normalised_label_when_neither_set(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__KEYCLOAK__ROLES_FIELD", "realm_access.roles")
        signed = _sign(iss="keycloak", realm_access={"roles": ["kc-role"]})
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"kc-role"})

    def test_unmapped_issuer_falls_back_to_identity_no_error(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__KEYCLOAK__ISS", "kc-issuer")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__KEYCLOAK__ROLES_FIELD", "realm_access.roles")
        signed = _sign(iss="unrelated-issuer", roles=["editor"])
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        # Canonical "roles" claim parses normally via IDENTITY.
        assert token.auth_ctx.roles == frozenset({"editor"})

    def test_conflicting_iss_across_two_labels_raises_at_load(self, monkeypatch):
        from varco_core.jwt.exceptions import ClaimTransformError
        from varco_core.jwt.transform.config import JwtTransformConfig

        monkeypatch.setenv("VARCO_JWT_TRANSFORM__A__ISS", "same-issuer")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM__B__ISS", "same-issuer")
        with pytest.raises(ClaimTransformError, match="A"):
            JwtTransformConfig.from_env()


class TestUnknownClaimPath:
    def test_unknown_path_non_required_yields_empty_roles(self, monkeypatch):
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "nope.nada")
        signed = _sign(sub_marker=True)
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx is None or token.auth_ctx.roles == frozenset()

    def test_unknown_path_required_raises(self, monkeypatch):
        from varco_core.jwt.exceptions import ClaimTransformError

        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "nope.nada")
        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_REQUIRED", "true")
        signed = _sign(sub_marker=True)
        with pytest.raises(ClaimTransformError):
            JwtParser.parse(signed, _SECRET, algorithms=["HS256"])


class TestExtraEnvSafety:
    def test_unknown_labelled_env_var_does_not_raise(self, monkeypatch):
        from varco_core.jwt.transform.config import JwtTransformSettings

        monkeypatch.setenv("VARCO_JWT_TRANSFORM__X__ROLES_FIELD", "whatever")
        # extra="ignore" must be set — construction must not raise.
        JwtTransformSettings()


class TestConfigureAndResetOverride:
    def test_configure_claim_transforms_overrides_env(self, monkeypatch):
        from varco_core.jwt.transform.mapper import MappingClaimTransformer
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath
        from varco_core.jwt.transform.registry import ClaimTransformerRegistry
        from varco_core.jwt.transform.runtime import configure_claim_transforms

        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "sofy-roles")

        # Explicit override registry always takes priority over env.
        override_mapping = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ROLES,
                    sources=(ClaimPath.parse("other-roles"),),
                ),
            )
        )
        reg = ClaimTransformerRegistry()
        reg.set_default(MappingClaimTransformer(override_mapping))
        configure_claim_transforms(reg)

        signed = _sign(**{"other-roles": ["from-override"]})
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"from-override"})

    def test_reset_claim_transforms_restores_lazy_env_resolution(self, monkeypatch):
        from varco_core.jwt.transform.registry import ClaimTransformerRegistry
        from varco_core.jwt.transform.runtime import (
            configure_claim_transforms,
            reset_claim_transforms,
        )

        configure_claim_transforms(ClaimTransformerRegistry())
        reset_claim_transforms()

        monkeypatch.setenv("VARCO_JWT_TRANSFORM_ROLES_FIELD", "sofy-roles")
        signed = _sign(**{"sofy-roles": ["editor"]})
        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])
        assert token.auth_ctx.roles == frozenset({"editor"})
