"""
Unit tests for varco_core.jwt.transform — shape normalization + ClaimMapping +
ClaimTransformer + parser wiring.

Covers (Plan 002):
    - Phase 0 step 3: ValueShape.normalize() for every AUTO-table row, explicit
      shapes, strip_prefix, strict vs. non-strict, GRANTS validation.
    - Phase 1 step 6: ClaimMapping.apply()/merged_with()/invert(), IDENTITY,
      MappingClaimTransformer, JwtParser integration (transformer= kwarg),
      the byte-identical regression test, and the malformed-grants error.

These tests are written RED — the ``varco_core.jwt.transform`` sub-package and
``varco_core.jwt.exceptions`` do not exist yet.  They must fail with
ImportError / AttributeError until Phase 0-1 lands.
"""

from __future__ import annotations

import logging

import pytest
from varco_core.jwt import JsonWebToken, JwtBuilder, JwtParser

_SECRET = "test-secret-do-not-use-in-production"


# ── ValueShape.normalize — AUTO table ──────────────────────────────────────────


class TestValueShapeAuto:
    def test_auto_list_passthrough(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        assert normalize(["a", "b"], ValueShape.AUTO) == ["a", "b"]

    def test_auto_space_delimited_string_splits(self):
        # The OAuth2 "scope" claim is space-delimited.
        from varco_core.jwt.transform.shape import ValueShape, normalize

        assert normalize("read write", ValueShape.AUTO) == ["read", "write"]

    def test_auto_comma_delimited_string_splits(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        assert normalize("a,b", ValueShape.AUTO) == ["a", "b"]

    def test_auto_scalar_string_becomes_single_item_list(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        assert normalize("admin", ValueShape.AUTO) == ["admin"]

    def test_auto_none_becomes_empty_list(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        assert normalize(None, ValueShape.AUTO) == []

    def test_auto_dict_becomes_sorted_keys_list(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        assert normalize({"y": 1, "x": 2}, ValueShape.AUTO) == ["x", "y"]


# ── Explicit shapes ─────────────────────────────────────────────────────────────


class TestValueShapeExplicit:
    def test_space_shape_splits_on_space(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        assert normalize("read write", ValueShape.SPACE) == ["read", "write"]

    def test_csv_shape_splits_on_comma(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        assert normalize("a,b,c", ValueShape.CSV) == ["a", "b", "c"]

    def test_scalar_shape_keeps_single_value_with_comma(self):
        # A single role legitimately containing a comma requires SCALAR.
        from varco_core.jwt.transform.shape import ValueShape, normalize

        assert normalize("a,b", ValueShape.SCALAR) == ["a,b"]

    def test_dict_keys_shape_returns_sorted_keys(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        assert normalize({"b": 1, "a": 2}, ValueShape.DICT_KEYS) == ["a", "b"]

    def test_raw_shape_passes_value_through_unmodified(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        assert normalize({"weird": "shape"}, ValueShape.RAW) == {"weird": "shape"}


# ── strip_prefix ────────────────────────────────────────────────────────────────


class TestStripPrefix:
    def test_strip_prefix_applied_per_element(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        result = normalize(["ROLE_admin", "ROLE_editor"], ValueShape.AUTO, strip_prefix="ROLE_")
        assert result == ["admin", "editor"]

    def test_strip_prefix_no_match_leaves_element_unchanged(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        result = normalize(["admin"], ValueShape.AUTO, strip_prefix="ROLE_")
        assert result == ["admin"]


# ── strict vs. non-strict scalar coercion ───────────────────────────────────────


class TestStrictness:
    def test_int_input_strict_false_warns_and_coerces(self, caplog):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        with caplog.at_level(logging.WARNING):
            result = normalize(5, ValueShape.AUTO, strict=False, target="roles")
        assert result == ["5"]
        assert any("roles" in rec.message for rec in caplog.records)

    def test_int_input_strict_true_raises(self):
        from varco_core.jwt.transform.shape import (
            ClaimTransformError,
            ValueShape,
            normalize,
        )

        with pytest.raises(ClaimTransformError):
            normalize(5, ValueShape.AUTO, strict=True, target="roles")


# ── GRANTS shape ────────────────────────────────────────────────────────────────


class TestGrantsShape:
    def test_valid_grants_list_accepted(self):
        from varco_core.jwt.transform.shape import ValueShape, normalize

        grants = [{"resource": "posts", "actions": ["read"]}]
        assert normalize(grants, ValueShape.GRANTS) == grants

    def test_malformed_grants_missing_actions_raises_naming_index(self):
        from varco_core.jwt.transform.shape import (
            ClaimTransformError,
            ValueShape,
            normalize,
        )

        with pytest.raises(ClaimTransformError, match="0"):
            normalize([{"resource": "posts"}], ValueShape.GRANTS)


# ── ClaimMapping / ClaimRule / CanonicalClaim ──────────────────────────────────


class TestClaimMappingApply:
    def test_maps_foreign_role_claim_to_canonical_and_preserves_original(self):
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath

        mapping = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ROLES,
                    sources=(ClaimPath.parse("sofy-roles"),),
                ),
            )
        )
        raw = {"sofy-roles": ["editor"]}
        out = mapping.apply(raw)
        assert out["roles"] == ["editor"]
        assert out["sofy-roles"] == ["editor"]  # original preserved

    def test_fallback_chain_first_non_empty_wins(self):
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath

        mapping = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ROLES,
                    sources=(
                        ClaimPath.parse("sofy-roles"),
                        ClaimPath.parse("realm_access.roles"),
                        ClaimPath.parse("roles"),
                    ),
                ),
            )
        )
        raw = {"realm_access": {"roles": ["editor"]}, "roles": ["viewer"]}
        out = mapping.apply(raw)
        assert out["roles"] == ["editor"]

    def test_merge_true_unions_all_sources(self):
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath

        mapping = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ROLES,
                    sources=(
                        ClaimPath.parse("sofy-roles"),
                        ClaimPath.parse("roles"),
                    ),
                    merge=True,
                ),
            )
        )
        raw = {"sofy-roles": ["editor"], "roles": ["viewer"]}
        out = mapping.apply(raw)
        assert sorted(out["roles"]) == ["editor", "viewer"]

    def test_required_missing_raises_naming_target_and_paths(self):
        from varco_core.jwt.exceptions import ClaimTransformError
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath

        mapping = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ROLES,
                    sources=(ClaimPath.parse("sofy-roles"),),
                    required=True,
                ),
            )
        )
        with pytest.raises(ClaimTransformError, match="roles"):
            mapping.apply({})

    def test_metadata_fields_promote_extra_claim_to_metadata_key(self):
        from varco_core.jwt.transform.mapping import ClaimMapping
        from varco_core.jwt.transform.path import ClaimPath

        mapping = ClaimMapping(metadata_fields=(("tenant_id", ClaimPath.parse("org.id")),))
        raw = {"org": {"id": "t_123"}}
        out = mapping.apply(raw)
        assert out["tenant_id"] == "t_123"

    def test_merged_with_inherits_unspecified_rules(self):
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath

        base = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ROLES,
                    sources=(ClaimPath.parse("roles"),),
                ),
                ClaimRule(
                    target=CanonicalClaim.SCOPES,
                    sources=(ClaimPath.parse("scope"),),
                ),
            )
        )
        override = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ROLES,
                    sources=(ClaimPath.parse("realm_access.roles"),),
                ),
            )
        )
        merged = base.merged_with(override)
        targets = {r.target for r in merged.rules}
        assert CanonicalClaim.ROLES in targets
        assert CanonicalClaim.SCOPES in targets
        roles_rule = next(r for r in merged.rules if r.target == CanonicalClaim.ROLES)
        assert roles_rule.sources[0].segments == ("realm_access", "roles")

    def test_invert_round_trips_simple_single_source_rule(self):
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath

        mapping = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ROLES,
                    sources=(ClaimPath.parse("sofy-roles"),),
                ),
            )
        )
        inverted = mapping.invert()
        assert inverted["roles"] == "sofy-roles"

    def test_invert_raises_for_multi_source_rule(self):
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath

        mapping = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ROLES,
                    sources=(
                        ClaimPath.parse("sofy-roles"),
                        ClaimPath.parse("roles"),
                    ),
                ),
            )
        )
        with pytest.raises(ValueError, match="roles"):
            mapping.invert()


# ── ClaimTransformer Protocol / IDENTITY ────────────────────────────────────────


class TestIdentityTransformer:
    def test_identity_transform_returns_same_object_no_copy(self):
        from varco_core.jwt.transform.protocol import IDENTITY

        d = {"sub": "u1"}
        assert IDENTITY.transform(d) is d

    def test_identity_satisfies_claim_transformer_protocol(self):
        from varco_core.jwt.transform.protocol import IDENTITY, ClaimTransformer

        assert isinstance(IDENTITY, ClaimTransformer)


class TestMappingClaimTransformer:
    def test_mapping_transformer_applies_configured_mapping(self):
        from varco_core.jwt.transform.mapper import MappingClaimTransformer
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath

        mapping = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ROLES,
                    sources=(ClaimPath.parse("sofy-roles"),),
                ),
            )
        )
        tf = MappingClaimTransformer(mapping)
        out = tf.transform({"sofy-roles": ["editor"]})
        assert out["roles"] == ["editor"]


# ── Parser integration ──────────────────────────────────────────────────────────


class TestParserIntegrationWithExplicitTransformer:
    def test_parse_with_explicit_transformer_maps_foreign_roles(self):
        from varco_core.jwt.transform.mapper import MappingClaimTransformer
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath

        mapping = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ROLES,
                    sources=(ClaimPath.parse("sofy-roles"),),
                ),
            )
        )
        tf = MappingClaimTransformer(mapping)

        signed = JwtBuilder().subject("usr_1").claim("sofy-roles", ["editor"]).encode(_SECRET)
        token = JwtParser.parse(signed, _SECRET, transformer=tf, algorithms=["HS256"])

        assert token.auth_ctx is not None
        assert token.auth_ctx.roles == frozenset({"editor"})
        # Foreign claim name still visible for audit/debug
        assert token.extra_claims["sofy-roles"] == ["editor"]
        # sub is untouched
        assert token.sub == "usr_1"

    def test_tenant_id_canonical_lands_in_metadata(self):
        from varco_core.jwt.transform.mapper import MappingClaimTransformer
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath

        mapping = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.TENANT_ID,
                    sources=(ClaimPath.parse("org.id"),),
                ),
            )
        )
        tf = MappingClaimTransformer(mapping)

        signed = JwtBuilder().subject("usr_1").claim("org", {"id": "t_1"}).encode(_SECRET)
        token = JwtParser.parse(signed, _SECRET, transformer=tf, algorithms=["HS256"])

        assert token.auth_ctx is not None
        assert token.auth_ctx.metadata["tenant_id"] == "t_1"

    def test_actor_canonical_lands_in_metadata(self):
        from varco_core.jwt.transform.mapper import MappingClaimTransformer
        from varco_core.jwt.transform.mapping import (
            CanonicalClaim,
            ClaimMapping,
            ClaimRule,
        )
        from varco_core.jwt.transform.path import ClaimPath

        mapping = ClaimMapping(
            rules=(
                ClaimRule(
                    target=CanonicalClaim.ACTOR,
                    sources=(ClaimPath.parse("act.sub"), ClaimPath.parse("act")),
                ),
            )
        )
        tf = MappingClaimTransformer(mapping)

        signed = JwtBuilder().subject("usr_1").claim("act", {"sub": "svc_admin"}).encode(_SECRET)
        token = JwtParser.parse(signed, _SECRET, transformer=tf, algorithms=["HS256"])

        assert token.auth_ctx is not None
        assert token.auth_ctx.metadata["actor"] == "svc_admin"

    def test_malformed_grants_raises_claim_transform_error_not_keyerror(self):
        # TEST-FIXTURE NOTE: encoded via PyJWT directly (not JwtBuilder) —
        # "grants" is a reserved claim key on JwtBuilder.claim() (app-code
        # safety net; see test_jwt.py::test_claim_raises_on_reserved_key),
        # but this test needs a malformed *raw* grants claim to prove the
        # parser now raises ClaimTransformError instead of a bare KeyError.
        import jwt as _pyjwt
        from varco_core.jwt.exceptions import ClaimTransformError

        signed = _pyjwt.encode(
            {"sub": "usr_1", "grants": [{"resource": "posts"}]},  # missing "actions"
            _SECRET,
            algorithm="HS256",
        )
        with pytest.raises(ClaimTransformError):
            JwtParser.parse(signed, _SECRET, algorithms=["HS256"])


class TestByteIdenticalRegression:
    def test_parse_with_no_transformer_is_byte_identical_to_pre_transform_era(self):
        """
        Regression guard (plan Edge cases table, row 1): with zero
        VARCO_JWT_TRANSFORM*/PROFILE* config and no explicit transformer=,
        parsing a canonical token must produce the exact JsonWebToken that
        today's (pre-transform) parser produces.
        """
        from varco_core.auth import Action, AuthContext, ResourceGrant

        signed = (
            JwtBuilder()
            .subject("usr_123")
            .issuer("my-service")
            .type("access")
            .with_auth_ctx(
                AuthContext(
                    user_id="usr_123",
                    roles=frozenset({"editor"}),
                    scopes=frozenset({"write:posts"}),
                    grants=(ResourceGrant("posts", frozenset({Action.READ})),),
                )
            )
            .encode(_SECRET)
        )

        token = JwtParser.parse(signed, _SECRET, algorithms=["HS256"])

        expected = JsonWebToken(
            sub="usr_123",
            iss="my-service",
            token_type="access",
            auth_ctx=AuthContext(
                user_id="usr_123",
                roles=frozenset({"editor"}),
                scopes=frozenset({"write:posts"}),
                grants=(ResourceGrant("posts", frozenset({Action.READ})),),
            ),
        )
        # JsonWebToken equality excludes extra_claims/timestamps we didn't set.
        assert token.sub == expected.sub
        assert token.iss == expected.iss
        assert token.token_type == expected.token_type
        assert token.auth_ctx == expected.auth_ctx
