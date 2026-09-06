"""
varco_core.jwt.transform.config
====================================

``JwtTransformSettings`` — pydantic-settings model for the flat
``VARCO_JWT_TRANSFORM_*`` env vars — and ``JwtTransformConfig`` — the
top-level parsed configuration (global mapping + per-issuer overrides)
built from the full environment, mirroring
``varco_core.authority.config.AuthorizationConfig``'s label-grouping style
for the dynamic per-issuer vars.

⚠️ Field-lists are declared ``str``, not ``list[str]``, on purpose.
pydantic-settings parses a ``list[str]`` field from a single env var as
**JSON**, which would force ``ROLES_FIELD='["sofy-roles","roles"]'``.
Declaring ``str`` and splitting on ``,`` keeps the human-friendly
``"sofy-roles,realm_access.roles"`` form (see plan §A).

⚠️ ``model_config`` sets ``extra="ignore"`` — the per-issuer vars
(``VARCO_JWT_TRANSFORM__KEYCLOAK__ROLES_FIELD``) share the
``VARCO_JWT_TRANSFORM_`` prefix and would otherwise be rejected as unknown
fields when pydantic-settings scans the environment for this flat model.

Thread safety:  ✅ ``JwtTransformSettings``/``JwtTransformConfig`` are
                   frozen/immutable after construction.
Async safety:   ✅ No I/O — ``from_env()`` reads ``os.environ`` synchronously.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings
from varco_core.jwt.exceptions import ClaimTransformError
from varco_core.jwt.transform.mapper import MappingClaimTransformer
from varco_core.jwt.transform.mapping import CanonicalClaim, ClaimMapping, ClaimRule
from varco_core.jwt.transform.path import ClaimPath
from varco_core.jwt.transform.registry import ClaimTransformerRegistry
from varco_core.jwt.transform.shape import ValueShape

# ── Constants ─────────────────────────────────────────────────────────────────

# The flat (non-labelled) env var prefix — pydantic-settings scans this.
_FLAT_PREFIX = "VARCO_JWT_TRANSFORM_"

# The per-issuer (labelled) env var prefix — a superset of _FLAT_PREFIX with
# one extra separator, so plain-os.environ scanning never collides with the
# flat vars above (flat vars have no "__" immediately after the prefix).
_LABEL_PREFIX = "VARCO_JWT_TRANSFORM__"

# Fallback source for a label's expected issuer when __ISS is not set —
# mirrors the authority.config precedent so the two config blocks share one
# label namespace (decision D-6).
_AUTHORIZATION_ISS_PREFIX = "FASTREST_AUTHORIZATION__"

# CanonicalClaim -> (settings field prefix, "canonical fallback path spec")
_TARGET_FIELD_PREFIX: dict[CanonicalClaim, str] = {
    CanonicalClaim.ROLES: "roles",
    CanonicalClaim.SCOPES: "scopes",
    CanonicalClaim.GRANTS: "grants",
    CanonicalClaim.USER_ID: "user_id",
    CanonicalClaim.TENANT_ID: "tenant",
    CanonicalClaim.ACTOR: "actor",
    CanonicalClaim.TOKEN_TYPE: "token_type",
    CanonicalClaim.TENANTS: "tenants",
}

# Default fallback sources appended last per D-7 ("canonical fallback is
# always appended last") — the canonical claim name itself, except tenant_id
# (no canonical raw claim named "tenant_id" pre-existing convention) and
# actor (RFC 8693 default sources).
_DEFAULT_CANONICAL_SOURCE: dict[CanonicalClaim, str] = {
    CanonicalClaim.ROLES: "roles",
    CanonicalClaim.SCOPES: "scopes",
    CanonicalClaim.GRANTS: "grants",
    CanonicalClaim.USER_ID: "sub",
    CanonicalClaim.TENANT_ID: "tenant_id",
    CanonicalClaim.ACTOR: "act",
    CanonicalClaim.TOKEN_TYPE: "token_type",
    CanonicalClaim.TENANTS: "tenants",
}


# ── JwtTransformSettings ────────────────────────────────────────────────────────


class JwtTransformSettings(VarcoSettings):
    """
    Flat ``VARCO_JWT_TRANSFORM_*`` env vars — the global claim-transform
    configuration.

    See the plan's env var table (§A) for every field's meaning.  Every
    field defaults to a value that produces an empty ``ClaimMapping``
    (i.e. the global default resolves to ``IDENTITY`` — zero-config is
    byte-identical to today).

    Thread safety:  ✅ ``frozen=True``.
    """

    model_config = SettingsConfigDict(
        env_prefix=_FLAT_PREFIX,
        frozen=True,
        extra="ignore",
    )

    roles_field: str | None = None
    scopes_field: str | None = None
    grants_field: str | None = None
    user_id_field: str | None = None
    tenant_field: str | None = None
    actor_field: str | None = None
    token_type_field: str | None = None
    tenants_field: str | None = None
    metadata_fields: str | None = None

    roles_shape: str = "auto"
    scopes_shape: str = "auto"
    grants_shape: str = "auto"
    user_id_shape: str = "auto"
    tenant_shape: str = "auto"
    actor_shape: str = "auto"
    token_type_shape: str = "auto"
    tenants_shape: str = "auto"

    roles_strip_prefix: str | None = None
    scopes_strip_prefix: str | None = None
    grants_strip_prefix: str | None = None
    user_id_strip_prefix: str | None = None
    tenant_strip_prefix: str | None = None
    actor_strip_prefix: str | None = None
    token_type_strip_prefix: str | None = None
    tenants_strip_prefix: str | None = None

    roles_required: bool = False
    scopes_required: bool = False
    grants_required: bool = False
    user_id_required: bool = False
    tenant_required: bool = False
    actor_required: bool = False
    token_type_required: bool = False
    tenants_required: bool = False

    merge_sources: bool = False
    path_separator: str = "."
    strict: bool = False

    def to_mapping(self) -> ClaimMapping:
        """
        Build the ``ClaimMapping`` this settings instance describes.

        Returns:
            A ``ClaimMapping`` with one ``ClaimRule`` per canonical target
            that has a configured ``*_field``.  Targets with no configured
            field produce NO rule at all — an entirely unconfigured
            settings instance yields ``ClaimMapping(rules=())``, which
            ``JwtTransformConfig.to_registry()`` recognises as "use
            IDENTITY" (the zero-config hot path).

        Edge cases:
            - A target with a configured field always gets the canonical
              claim name appended as the last fallback source (D-7), so a
              mixed fleet (some canonical, some foreign tokens) works with
              one config.
        """
        rules: list[ClaimRule] = []
        for target, prefix in _TARGET_FIELD_PREFIX.items():
            field_value = getattr(self, f"{prefix}_field")
            if field_value is None:
                continue
            sources = self._build_sources(target, field_value)
            shape = ValueShape(getattr(self, f"{prefix}_shape"))
            rules.append(
                ClaimRule(
                    target=target,
                    sources=sources,
                    shape=shape,
                    strip_prefix=getattr(self, f"{prefix}_strip_prefix"),
                    merge=self.merge_sources,
                    required=getattr(self, f"{prefix}_required"),
                )
            )

        return ClaimMapping(
            rules=tuple(rules),
            metadata_fields=self._parse_metadata_fields(),
            separator=self.path_separator,
            strict=self.strict,
        )

    def _build_sources(self, target: CanonicalClaim, field_value: str) -> tuple[ClaimPath, ...]:
        """Parse a comma-separated fallback chain, appending the canonical
        fallback path last (D-7) unless it is already present."""
        specs = [chunk.strip() for chunk in field_value.split(",") if chunk.strip()]
        canonical_spec = _DEFAULT_CANONICAL_SOURCE[target]
        if canonical_spec not in specs:
            specs.append(canonical_spec)
        return tuple(ClaimPath.parse(spec, separator=self.path_separator) for spec in specs)

    def _parse_metadata_fields(self) -> tuple[tuple[str, ClaimPath], ...]:
        """Parse ``key=path,key2=path2`` into ``(key, ClaimPath)`` pairs."""
        if not self.metadata_fields:
            return ()
        pairs: list[tuple[str, ClaimPath]] = []
        for chunk in self.metadata_fields.split(","):
            chunk = chunk.strip()
            if not chunk or "=" not in chunk:
                continue
            key, _, spec = chunk.partition("=")
            pairs.append(
                (
                    key.strip(),
                    ClaimPath.parse(spec.strip(), separator=self.path_separator),
                )
            )
        return tuple(pairs)


# ── JwtTransformConfig ───────────────────────────────────────────────────────────


@dataclass(frozen=True)
class JwtTransformConfig:
    """
    Fully parsed JWT claim-transform configuration: the global mapping plus
    every per-issuer override, already merged (D-8: per-issuer inherits the
    global mapping field-by-field).

    Attributes:
        default:    The global ``ClaimMapping`` (from ``VARCO_JWT_TRANSFORM_*``).
        per_issuer: ``(iss_value, merged_mapping)`` pairs — one per
                    configured label, keyed by the *resolved* issuer value
                    (not the label), already merged with ``default``.

    Thread safety:  ✅ frozen=True.
    """

    default: ClaimMapping = field(default_factory=ClaimMapping)
    per_issuer: tuple[tuple[str, ClaimMapping], ...] = ()

    @classmethod
    def from_env(cls) -> JwtTransformConfig:
        """
        Parse both the flat global vars and the labelled per-issuer vars
        from the process environment.

        Returns:
            A fully parsed, already-merged ``JwtTransformConfig``.

        Raises:
            ClaimTransformError: Two labels declare (directly or via the
                ``FASTREST_AUTHORIZATION__<LABEL>__ISS`` fallback) the same
                resolved issuer value — a genuine ambiguity, rejected at
                load time rather than silently picking one (fail fast).
        """
        global_mapping = JwtTransformSettings.from_env().to_mapping()

        groups: dict[str, dict[str, str]] = {}
        for key, value in os.environ.items():
            if not key.startswith(_LABEL_PREFIX):
                continue
            rest = key[len(_LABEL_PREFIX) :]
            if "__" not in rest:
                continue
            label, suffix = rest.rsplit("__", 1)
            groups.setdefault(label, {})[suffix] = value.strip()

        iss_to_label: dict[str, str] = {}
        per_issuer: list[tuple[str, ClaimMapping]] = []

        for label, fields_dict in groups.items():
            iss = fields_dict.get("ISS") or os.environ.get(
                f"{_AUTHORIZATION_ISS_PREFIX}{label}__ISS"
            )
            if iss is None:
                iss = label.lower().replace("_", "-")

            existing_label = iss_to_label.get(iss)
            if existing_label is not None and existing_label != label:
                raise ClaimTransformError(
                    f"Conflicting VARCO_JWT_TRANSFORM__* configuration: "
                    f"labels {existing_label!r} and {label!r} both resolve "
                    f"to iss={iss!r}. Each issuer may only be claimed by "
                    f"one label — rename one of them."
                )
            iss_to_label[iss] = label

            label_fields = {k.lower(): v for k, v in fields_dict.items() if k != "ISS"}
            label_settings = JwtTransformSettings.from_dict(label_fields)
            label_mapping = label_settings.to_mapping()
            merged = global_mapping.merged_with(label_mapping)
            per_issuer.append((iss, merged))

        return cls(default=global_mapping, per_issuer=tuple(per_issuer))

    def to_registry(self) -> ClaimTransformerRegistry:
        """
        Build a ``ClaimTransformerRegistry`` from this parsed configuration.

        Returns:
            A registry with one entry per ``per_issuer`` pair and a default
            transformer — ``IDENTITY`` when ``self.default`` has no rules
            and no metadata_fields at all (the true zero-config hot path),
            otherwise a ``MappingClaimTransformer`` wrapping ``self.default``.
        """
        registry = ClaimTransformerRegistry()

        if self.default.rules or self.default.metadata_fields:
            registry.set_default(MappingClaimTransformer(self.default))
        # else: leave the registry's built-in IDENTITY default in place.

        for iss, mapping in self.per_issuer:
            registry.register(iss, MappingClaimTransformer(mapping))

        return registry


__all__ = [
    "JwtTransformSettings",
    "JwtTransformConfig",
]
