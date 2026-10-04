"""
varco_core.jwt.config
=========================

``JwtVerificationSettings`` — env-driven defaults for JWT *verification*
hardening: clock-skew leeway (C-1) and audience enforcement (C-2).

Deliberately a separate module from ``varco_core.jwt.transform.config``:
verification settings (how strict/lenient PyJWT's own temporal/audience
checks are) are not a claim-*transformation* concern — they configure
PyJWT's ``decode()`` call itself, not the claim-renaming layer that runs
after it.

Thread safety:  ✅ ``frozen=True`` — immutable after construction.
Async safety:   ✅ No I/O.
"""

from __future__ import annotations

from pydantic import AliasChoices, Field, field_validator
from pydantic_settings import SettingsConfigDict

from varco_core.config import VarcoSettings
from varco_core.revocation import RevocationFailureMode


class JwtVerificationSettings(VarcoSettings):
    """
    ``VARCO_JWT_*`` env vars controlling PyJWT verification strictness.

    Attributes:
        leeway_seconds: Clock-skew leeway (seconds) applied to ``exp``/``nbf``
                        checks.  Default ``0.0`` — identical to today's
                        behaviour (no leeway).  A classic fix for cross-host
                        401s caused by clock drift; ``30`` is a common value.
        audience:       This service's expected ``aud`` claim value.  ``None``
                        (default) — audience is NOT enforced by
                        ``TrustedIssuerRegistry.verify()`` at this layer
                        (decision D-17). ``JwtBearerAuth`` (varco_fastapi)
                        layers a stricter, fail-closed rule on top — see
                        ``allow_any_audience`` below and Plan 005 Phase 2.
        enforce_issuer: Whether ``TrustedIssuerRegistry.verify()`` checks the
                        token's ``iss`` claim against the resolved issuer's
                        registered value.  Default ``True`` — Plan 005
                        Phase 2 / U-13, a BREAKING security-default change:
                        pre-Phase-2 releases never enforced ``iss`` here.
                        Set ``VARCO_JWT_ENFORCE_ISS=false`` (or pass
                        ``enforce_issuer=False`` to ``verify()``) to restore
                        the old behaviour.
        allow_any_audience: Whether ``JwtBearerAuth`` (varco_fastapi) may be
                        constructed with no configured audience. Default
                        ``False`` — Plan 005 Phase 2 / U-13, a BREAKING
                        security-default change: pre-Phase-2 releases logged
                        a warning and proceeded. Set
                        ``VARCO_JWT_ALLOW_ANY_AUDIENCE=true`` (or pass
                        ``allow_any_audience=True`` to ``JwtBearerAuth``) to
                        restore the old (warn + proceed) behaviour.
        revocation_failure_mode: How ``TrustedIssuerRegistry.verify()``
                        behaves when a bound ``AbstractTokenRevocationStore``
                        raises. Default ``FAIL_CLOSED`` (Plan 034 / S13,
                        §D-S13-fail) — a store outage is a 503, not a
                        silently-admitted possibly-revoked token. Set
                        ``VARCO_JWT_REVOCATION_FAILURE_MODE=fail_open`` for
                        an availability-first posture (pair with
                        short-lived tokens). Only ever consulted when a
                        non-``Null`` store is actually wired to the
                        registry — see ``varco_core.revocation.di``'s
                        two-step warning.
        revocation_require_jti: Whether a token with no ``jti`` claim is
                        treated as revoked at ``TOKEN`` scope. ``None``
                        (the 4.0 default, Plan 053 / FLIP-034-3): derived
                        at check time — ``True`` iff the registry's wired
                        store is not a ``NullTokenRevocationStore``.
                        ``true``/``false`` (env or instance) override the
                        derivation in either direction. Auth0, Keycloak,
                        Cognito emit no ``jti`` by default (brief 009 §2):
                        set ``VARCO_JWT_REVOCATION_REQUIRE_JTI=false`` for
                        such an issuer, or use SUBJECT/TENANT/ISSUER scope.
        revocation_skew_seconds: Clock-skew tolerance (seconds) added to a
                        ``TOKEN``-scope entry's TTL beyond the token's own
                        ``exp`` (brief 009 §5). Default ``60.0`` (OWASP/RFC
                        7519 recommend 30-60s).
        revocation_enabled: Master kill-switch: consult the bound store *if
                        one is wired* at all. Default ``True``. Set
                        ``VARCO_JWT_REVOCATION_ENABLED=false`` as an
                        incident-response override to disable revocation
                        checking without unwiring the store or a
                        redeploy — see the Open Question in the plan about
                        this being a foot-gun an operator could leave set.
        jwks_min_refresh_seconds: Rate limit (seconds) between kid-not-found
                        triggered global JWKS refreshes. Default ``10.0``.
                        **Renamed in 4.0.0** (Plan 048 / DI-6, §D-prefix rule
                        1 — every loose ``VARCO_*`` read folds into its
                        owning settings class): was the loose
                        ``VARCO_JWKS_MIN_REFRESH_SECONDS`` env read in
                        ``TrustedIssuerRegistry.__init__``.
        jwks_ttl_seconds: Proactive JWKS reload age threshold (seconds,
                        Plan 002 C-4); ``0.0`` disables (only a kid-miss
                        triggers a refresh). **Renamed in 4.0.0**: was the
                        loose ``VARCO_JWKS_TTL_SECONDS`` env read.

    Thread safety:  ✅ ``frozen=True``.
    """

    model_config = SettingsConfigDict(
        env_prefix="VARCO_JWT_",
        frozen=True,
        extra="ignore",
    )

    leeway_seconds: float = 0.0
    audience: str | None = None
    # Field name would otherwise map to VARCO_JWT_ENFORCE_ISSUER by the
    # default env_prefix + FIELD_NAME.upper() rule — the plan's chosen env
    # var name is the shorter VARCO_JWT_ENFORCE_ISS, so it needs an explicit
    # validation_alias naming the full var (bypasses the prefix rule).
    enforce_issuer: bool = Field(
        default=True, validation_alias=AliasChoices("VARCO_JWT_ENFORCE_ISS")
    )
    allow_any_audience: bool = False

    # ── Revocation (Plan 034 / S13) ──────────────────────────────────────────
    revocation_failure_mode: RevocationFailureMode = RevocationFailureMode.FAIL_CLOSED
    # None = derived from the registry's wired store (Plan 053 / FLIP-034-3).
    revocation_require_jti: bool | None = None
    revocation_skew_seconds: float = 60.0
    revocation_enabled: bool = True

    # ── JWKS caching (folded from loose reads, Plan 048 / DI-6) ──────────────
    jwks_min_refresh_seconds: float = 10.0
    jwks_ttl_seconds: float = 0.0

    @property
    def enforce_iss(self) -> bool:
        """
        Short alias for ``enforce_issuer`` — matches the shorter
        ``VARCO_JWT_ENFORCE_ISS`` env var name this field's
        ``validation_alias`` already reads. Read-only; ``enforce_issuer``
        remains the canonical field name.
        """
        return self.enforce_issuer

    @field_validator("revocation_failure_mode", mode="before")
    @classmethod
    def _lowercase_revocation_failure_mode(cls, value: object) -> object:
        """
        Accept ``VARCO_JWT_REVOCATION_FAILURE_MODE`` case-insensitively.

        Args:
            value: The raw value — a string from the environment, an
                   already-constructed ``RevocationFailureMode``, or
                   anything else pydantic will reject on its own.

        Returns:
            The lowercased string when given a string (so ``"FAIL_OPEN"``
            and ``"fail_open"`` both resolve to
            ``RevocationFailureMode.FAIL_OPEN``); any other value passes
            through unchanged for pydantic's own enum coercion/error.
        """
        return value.lower() if isinstance(value, str) else value


__all__ = [
    "JwtVerificationSettings",
]
