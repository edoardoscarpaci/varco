"""
varco_core.jwt.transform.mapping
=====================================

``CanonicalClaim``, ``ClaimRule``, ``ClaimMapping`` — the code-configured
claim-transformation value objects (Plan 002 §A).

``ClaimMapping.apply()`` is the non-destructive transform at the heart of
the pipeline: it returns ``dict(claims)`` with canonical keys added/
overwritten, keeping every original key so ``extra_claims`` (built from the
*raw* dict by the parser) still shows foreign claim names for audit/debug.

Thread safety:  ✅ All value objects are frozen — safe to share/cache.
Async safety:   ✅ Pure — no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from varco_core.jwt.exceptions import ClaimTransformError
from varco_core.jwt.transform.path import MISSING, ClaimPath
from varco_core.jwt.transform.shape import ValueShape, normalize

# Canonical targets whose AUTO default should resolve to a bare scalar (not
# a role/scope-style list) — see ClaimRule.resolve(). Populated after
# CanonicalClaim is defined (see bottom of the CanonicalClaim class body).


# ── CanonicalClaim ─────────────────────────────────────────────────────────────


class CanonicalClaim(StrEnum):
    """
    The canonical target claim set a ``ClaimRule`` can map onto.

    Deliberately excludes ``sub``/``iss``/``aud``/``exp``/``iat``/``nbf``/
    ``jti`` (decision D-2) — those are verified by PyJWT before this layer
    runs, and ``iss`` is the mapping *selector*, so remapping it would be
    circular.

    Members:
        USER_ID:    → ``AuthContext.user_id``.  Default source is ``sub``,
                    but a rule can source it elsewhere (e.g.
                    ``preferred_username``) — ``token.sub`` still keeps the
                    raw RFC value (D-3).
        ROLES:      → ``AuthContext.roles``.
        SCOPES:     → ``AuthContext.scopes``.
        GRANTS:     → ``AuthContext.grants``.
        TENANT_ID:  → ``AuthContext.metadata["tenant_id"]``.
        ACTOR:      → ``AuthContext.metadata["actor"]`` (RFC 8693 ``act``).
        TOKEN_TYPE: → ``JsonWebToken.token_type``.
        TENANTS:    → ``AuthContext.metadata["tenants"]``, a **list** — the
                    tenant↔subject membership claim consumed by
                    ``varco_core.tenancy.membership.ClaimTenantMembership``
                    (Plan 033 / S5, §D-S5-claim). Deliberately NOT added to
                    ``_SCALAR_TARGETS`` below — it is a list target, same
                    shape family as ROLES/SCOPES.
    """

    USER_ID = "user_id"
    ROLES = "roles"
    SCOPES = "scopes"
    GRANTS = "grants"
    TENANT_ID = "tenant_id"
    ACTOR = "actor"
    TOKEN_TYPE = "token_type"
    TENANTS = "tenants"


# Canonical targets whose AUTO default should resolve to a bare scalar (not
# a role/scope-style list) — see ClaimRule.resolve().
_SCALAR_TARGETS: frozenset[CanonicalClaim] = frozenset(
    {
        CanonicalClaim.TENANT_ID,
        CanonicalClaim.ACTOR,
        CanonicalClaim.USER_ID,
        CanonicalClaim.TOKEN_TYPE,
    }
)


# ── ClaimRule ──────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ClaimRule:
    """
    A single canonical-claim mapping rule: read from a fallback chain of
    source paths, shape the result, and (optionally) require it.

    Attributes:
        target:       The ``CanonicalClaim`` this rule populates.
        sources:      Fallback chain of ``ClaimPath``, tried in order.
        shape:        How to normalize the raw source value — see
                      ``ValueShape``.
        strip_prefix: Stripped from each resulting string element (applied
                      after shaping).
        merge:        ``False`` (default) — first non-empty source wins.
                      ``True`` — union every source in the chain.
        required:     ``True`` → missing (empty after trying every source)
                      raises ``ClaimTransformError`` naming ``target`` and
                      every path tried.

    Thread safety:  ✅ frozen=True.
    """

    target: CanonicalClaim
    sources: tuple[ClaimPath, ...] = ()
    shape: ValueShape = ValueShape.AUTO
    strip_prefix: str | None = None
    merge: bool = False
    required: bool = False

    def resolve(self, claims: dict[str, Any], *, strict: bool) -> Any:
        """
        Resolve this rule's value against ``claims``.

        Args:
            claims: The (partially transformed) claims dict.
            strict: Whether shape violations raise (``True``) or
                    warn-and-coerce (``False``) — see ``normalize()``.

        Returns:
            The shaped value — ``list[str]`` for ROLES/SCOPES, validated
            ``list[dict]`` for GRANTS, or the raw scalar for others.

        Raises:
            ClaimTransformError: ``required=True`` and every source path is
                missing/empty, or a shape violation under ``strict=True``.
        """
        shape = self.shape
        if shape is ValueShape.AUTO:
            if self.target is CanonicalClaim.GRANTS:
                # GRANTS always needs structural validation — never treat it
                # as a role/scope-style string-list under the AUTO default.
                shape = ValueShape.GRANTS
            elif self.target in _SCALAR_TARGETS:
                # TENANT_ID / ACTOR / USER_ID / TOKEN_TYPE are single scalar
                # values, not string lists — AUTO's list-wrapping behaviour
                # (designed for ROLES/SCOPES) would turn "t_1" into ["t_1"].
                # RAW passes the resolved value through unmodified.
                shape = ValueShape.RAW

        empty_default = normalize(
            None,
            shape,
            strip_prefix=self.strip_prefix,
            strict=strict,
            target=self.target.value,
        )

        if self.merge:
            collected: list[Any] = []
            found_any = False
            for path in self.sources:
                raw = path.read(claims)
                if raw is MISSING:
                    continue
                found_any = True
                shaped = normalize(
                    raw,
                    shape,
                    strip_prefix=self.strip_prefix,
                    strict=strict,
                    target=self.target.value,
                )
                if isinstance(shaped, list):
                    collected.extend(shaped)
                else:
                    collected.append(shaped)
            if not found_any:
                self._raise_if_required()
                return empty_default
            return collected

        # First-non-empty-wins (default): try each source path in order;
        # the first one that both exists AND shapes to a truthy value wins.
        # A source that exists but shapes empty (e.g. an empty list claim)
        # keeps the chain going — the *last* shaped value becomes the
        # fallback result if nothing in the chain is non-empty.
        found_any = False
        last_shaped: Any = None
        for path in self.sources:
            raw = path.read(claims)
            if raw is MISSING:
                continue
            found_any = True
            shaped = normalize(
                raw,
                shape,
                strip_prefix=self.strip_prefix,
                strict=strict,
                target=self.target.value,
            )
            last_shaped = shaped
            if shaped:
                return shaped

        if not found_any:
            self._raise_if_required()
            return empty_default

        return last_shaped if last_shaped is not None else empty_default

    def _raise_if_required(self) -> None:
        if not self.required:
            return
        tried = ", ".join(".".join(p.segments) for p in self.sources) or "(no sources configured)"
        raise ClaimTransformError(
            f"Required claim {self.target.value!r} is missing — tried source path(s): {tried}."
        )

    def invert(self) -> str:
        """
        Return the single foreign source-claim name this rule maps from —
        the inverse of ``target``.

        Returns:
            The dotted spec of the (single) source path.

        Raises:
            ValueError: This rule is not invertible — more than one source
                (fallback chain), a nested path, a non-default shape, or a
                ``strip_prefix`` are all lossy/ambiguous in reverse.
        """
        if len(self.sources) != 1:
            raise ValueError(
                f"Rule for target {self.target.value!r} is not invertible: "
                f"it has {len(self.sources)} source(s) (fallback chains have "
                f"no well-defined inverse)."
            )
        source = self.sources[0]
        if len(source.segments) != 1:
            raise ValueError(
                f"Rule for target {self.target.value!r} is not invertible: "
                f"its source path {'.'.join(source.segments)!r} is nested."
            )
        if self.shape is not ValueShape.AUTO or self.strip_prefix is not None:
            raise ValueError(
                f"Rule for target {self.target.value!r} is not invertible: "
                f"it has a non-default shape/strip_prefix."
            )
        return source.segments[0]


# ── ClaimMapping ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class ClaimMapping:
    """
    A complete set of ``ClaimRule``s plus ``metadata_fields``, ready to
    ``apply()`` to a raw claims dict.

    Attributes:
        rules:           Rules keyed by their (unique) ``target``.
        metadata_fields: Extra ``(metadata_key, ClaimPath)`` pairs promoted
                         directly into the output dict under
                         ``metadata_key`` — used for arbitrary extra
                         metadata beyond the fixed ``CanonicalClaim`` set.
        separator:       Path separator used when building/rendering paths
                         from this mapping's own tooling (parsing itself
                         happens in ``ClaimPath.parse``; kept here for
                         ``JwtTransformConfig`` to thread through).
        strict:          Shape/type violations raise (``True``) vs.
                         log-and-skip (``False``, default).

    Thread safety:  ✅ frozen=True.
    """

    rules: tuple[ClaimRule, ...] = ()
    metadata_fields: tuple[tuple[str, ClaimPath], ...] = ()
    separator: str = "."
    strict: bool = False

    def apply(self, claims: dict[str, Any]) -> dict[str, Any]:
        """
        Apply this mapping to ``claims``, returning a new dict with
        canonical keys added/overwritten.

        Args:
            claims: Raw (or already partially transformed) claims dict.
                    Never mutated.

        Returns:
            ``dict(claims)`` with every rule's ``target`` key set to its
            resolved value, plus ``metadata_fields`` promoted to their
            target keys.

        Raises:
            ClaimTransformError: A ``required=True`` rule found no value in
                any of its source paths.

        Edge cases:
            - Original claim keys are always preserved — even when a
              mapped source name IS the canonical name, the canonical
              (possibly reshaped) value simply overwrites that key.
        """
        out = dict(claims)
        for rule in self.rules:
            out[rule.target.value] = rule.resolve(claims, strict=self.strict)
        for key, path in self.metadata_fields:
            value = path.read(claims)
            if value is not MISSING:
                out[key] = value
        return out

    def merged_with(self, override: ClaimMapping) -> ClaimMapping:
        """
        Return a new ``ClaimMapping`` where ``override``'s rules replace
        this mapping's rules with the same ``target`` (field-by-field
        inheritance), keeping every rule this mapping declares that
        ``override`` does not (D-8).

        Args:
            override: The more-specific mapping (e.g. a per-issuer config).

        Returns:
            The merged ``ClaimMapping``.  ``metadata_fields``/``strict``/
            ``separator`` come from ``override`` when it declares any
            rules or metadata_fields, otherwise fall back to ``self``'s.

        Edge cases:
            - ``override`` with zero rules and zero metadata_fields is a
              no-op merge — everything inherits from ``self``.
        """
        by_target: dict[CanonicalClaim, ClaimRule] = {r.target: r for r in self.rules}
        for rule in override.rules:
            by_target[rule.target] = rule

        return ClaimMapping(
            rules=tuple(by_target.values()),
            metadata_fields=override.metadata_fields or self.metadata_fields,
            separator=(
                override.separator if override.rules or override.metadata_fields else self.separator
            ),
            strict=(override.strict if override.rules or override.metadata_fields else self.strict),
        )

    def invert(self) -> dict[str, str]:
        """
        Build the inverse ``{canonical_target: source_claim_name}`` mapping.

        Returns:
            A ``dict`` from canonical claim name to the single foreign
            source claim name each invertible rule maps from.

        Raises:
            ValueError: Any rule is non-invertible (multi-source, nested
                path, non-default shape, or ``strip_prefix``) — names the
                offending rule's target.
        """
        return {rule.target.value: rule.invert() for rule in self.rules}


__all__ = [
    "CanonicalClaim",
    "ClaimRule",
    "ClaimMapping",
]
