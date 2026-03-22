"""
varco_core.jwt.model
========================

``JsonWebToken`` — the core immutable value object for this JWT layer.

Also exports the two internal timestamp converters and the reserved-claim-key
set that both ``JwtBuilder`` and ``JwtParser`` need.

Thread safety:  ✅ frozen=True — safe to share across threads and tasks.
Async safety:   ✅ Pure value object — no I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Final

from varco_core.auth import AuthContext


# ── Reserved claim keys ───────────────────────────────────────────────────────

# The full set of claim names that varco manages explicitly.
# JwtBuilder.claim() blocks writes to these; JwtParser._from_raw_claims() uses
# this set to separate known standard claims from unknown extra_claims.
_RESERVED_CLAIM_KEYS: Final[frozenset[str]] = frozenset(
    {
        "sub",
        "iss",
        "aud",
        "exp",
        "iat",
        "nbf",
        "jti",
        "token_type",
        "roles",
        "scopes",
        "grants",
    }
)


# ── Timestamp helpers ─────────────────────────────────────────────────────────


def _to_utc_timestamp(dt: datetime) -> int:
    """
    Convert a ``datetime`` to an integer UTC Unix timestamp.

    Naive datetimes are assumed to be UTC — mirrors PyJWT's own convention to
    avoid double-offset bugs when callers mix aware and naive datetimes.

    Args:
        dt: Datetime to convert.  Tz-aware datetimes are handled correctly;
            naive datetimes are treated as UTC (document this assumption to
            callers — they should always pass ``datetime.now(timezone.utc)``).

    Returns:
        Integer UTC Unix timestamp suitable for embedding in JWT claims.

    Edge cases:
        - Sub-second precision is truncated (int cast) — standard for JWT.
        - Naive datetimes silently treated as UTC — always prefer tz-aware.
    """
    if dt.tzinfo is None:
        # Treat naive as UTC — caller footgun, but mirrors PyJWT's convention
        return int(dt.replace(tzinfo=timezone.utc).timestamp())
    return int(dt.timestamp())


def _from_utc_timestamp(ts: int | float) -> datetime:
    """
    Convert a UTC Unix timestamp to a timezone-aware ``datetime`` (UTC).

    Args:
        ts: Unix timestamp as returned by PyJWT.decode() for exp / iat / nbf.

    Returns:
        UTC-aware ``datetime`` with ``tzinfo=timezone.utc``.
    """
    return datetime.fromtimestamp(ts, tz=timezone.utc)


# ── JsonWebToken ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class JsonWebToken:
    """
    Immutable value object representing a decoded JSON Web Token.

    Maps all seven RFC 7519 registered claims to typed Python attributes,
    plus a ``token_type`` custom claim for application-level token
    discrimination and an optional ``auth_ctx`` field for the deserialized
    ``AuthContext`` when identity/permission claims are present.

    Fields absent from the source token are ``None`` — always guard before use
    unless the field's presence is guaranteed by the issuing service.

    Attributes:
        sub:          Subject — unique identifier of the token's principal.
        iss:          Issuer — authority that signed the token.
        aud:          Audience — intended recipient(s).  Either a single
                      string or a ``frozenset`` for multi-audience tokens.
        exp:          Expiration time — UTC-aware datetime; reject at/after.
        iat:          Issued-at — UTC-aware datetime; when token was minted.
        nbf:          Not-before — UTC-aware datetime; reject before.
        jti:          JWT ID — unique per-token identifier.
        token_type:   Application-defined discriminator (``"access"``,
                      ``"refresh"``, ``"system"``, etc.).
        auth_ctx:     Deserialized ``AuthContext``; ``None`` when the token
                      carries no identity/permission claims.
        extra_claims: Any non-standard claims present in the token.
                      Excluded from equality and hashing so that unhashable
                      values (nested dicts, lists) don't break ``frozen=True``.

    Thread safety:  ✅ frozen=True — immutable; safe to share across tasks.
    Async safety:   ✅ Pure value object — no I/O.

    Edge cases:
        - A minimal token may have all fields as ``None`` / empty dict.
        - ``aud`` may be ``str`` or ``frozenset[str]`` — use
          ``JwtUtil.is_audience()`` to compare without caring which form.
        - ``extra_claims`` is excluded from ``__eq__`` and ``__hash__`` —
          two tokens that differ only in extra claims are considered equal.
        - ``auth_ctx.user_id`` should equal ``sub`` when both are present;
          ``JwtParser`` enforces this on decode.
        - ``auth_ctx`` is ``None`` when only ``sub`` / ``iss`` are present
          and no auth-specific claims (``roles``, ``scopes``, ``grants``)
          exist — ``sub`` is already accessible as ``token.sub``.

    Example::

        tok = JsonWebToken(
            sub="usr_123",
            iss="my-service",
            token_type="access",
            auth_ctx=AuthContext(
                user_id="usr_123",
                roles=frozenset({"editor"}),
            ),
        )
    """

    # ── RFC 7519 registered claims ────────────────────────────────────────────

    # Subject — who the token is about; None for anonymous / machine tokens
    sub: str | None = None

    # Issuer — who signed/minted this token
    iss: str | None = None

    # Audience — single string for simple cases; frozenset for multi-audience
    aud: str | frozenset[str] | None = None

    # Expiration time — UTC-aware; token is rejected at or after this moment
    exp: datetime | None = None

    # Issued-at — UTC-aware; purely informational (when this token was minted)
    iat: datetime | None = None

    # Not-before — UTC-aware; token is rejected before this moment
    nbf: datetime | None = None

    # JWT ID — globally unique per-token; useful for revocation lists
    jti: str | None = None

    # ── Custom varco claims ────────────────────────────────────────────────

    # Application-level token discriminator: "access", "refresh", "system" …
    token_type: str | None = None

    # Deserialized authorization context.  None when the token carries no
    # identity/permission claims (e.g., bare service-to-service ping tokens).
    auth_ctx: AuthContext | None = None

    # Catch-all for application-specific or unknown non-standard claims.
    # compare=False, hash=False: unhashable claim values (lists, dicts) would
    # break frozen=True; excluding them keeps the dataclass safely hashable.
    extra_claims: dict[str, Any] = field(
        default_factory=dict, compare=False, hash=False
    )

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_claims(self) -> dict[str, Any]:
        """
        Serialize this token to a raw claims dict suitable for PyJWT encoding.

        Converts ``datetime`` fields to integer UTC Unix timestamps as required
        by RFC 7519.  Omits ``None`` fields to keep the payload compact.
        Serializes ``auth_ctx`` into ``roles``, ``scopes``, and ``grants``
        custom claims.  Merges ``extra_claims`` last.

        Returns:
            ``dict`` mapping claim names to JSON-serializable values.

        Edge cases:
            - ``aud`` ``frozenset`` → sorted ``list`` (JSON-serializable).
            - ``extra_claims`` entries are merged last and can overwrite
              standard claims — callers are responsible for avoiding conflicts.
            - ``auth_ctx.metadata`` is NOT emitted; use ``extra_claims`` for
              arbitrary metadata that must survive the round-trip.
            - Empty ``roles`` / ``scopes`` / ``grants`` are omitted to keep
              the token compact; they parse back as empty collections.

        Example::

            signed = jwt.encode(token.to_claims(), secret, algorithm="HS256")
        """
        claims: dict[str, Any] = {}

        # ── Standard registered claims — omit None to stay compact ────────────
        if self.sub is not None:
            claims["sub"] = self.sub
        if self.iss is not None:
            claims["iss"] = self.iss
        if self.aud is not None:
            # PyJWT expects str or list — frozenset must be normalised to list
            claims["aud"] = (
                sorted(self.aud) if isinstance(self.aud, frozenset) else self.aud
            )
        if self.exp is not None:
            claims["exp"] = _to_utc_timestamp(self.exp)
        if self.iat is not None:
            claims["iat"] = _to_utc_timestamp(self.iat)
        if self.nbf is not None:
            claims["nbf"] = _to_utc_timestamp(self.nbf)
        if self.jti is not None:
            claims["jti"] = self.jti
        if self.token_type is not None:
            claims["token_type"] = self.token_type

        # ── AuthContext → custom claims ────────────────────────────────────────
        if self.auth_ctx is not None:
            ctx = self.auth_ctx
            # Only emit non-empty collections — keeps tokens compact
            if ctx.roles:
                claims["roles"] = sorted(ctx.roles)
            if ctx.scopes:
                claims["scopes"] = sorted(ctx.scopes)
            if ctx.grants:
                claims["grants"] = [
                    {
                        "resource": g.resource,
                        # Sort for deterministic output; Action is StrEnum → str
                        "actions": sorted(str(a) for a in g.actions),
                    }
                    for g in ctx.grants
                ]

        # Extra claims merged last — callers must ensure no key conflicts with
        # the standard claims above (JwtBuilder.claim() blocks reserved keys).
        claims.update(self.extra_claims)

        return claims

    def __repr__(self) -> str:
        return (
            f"JsonWebToken("
            f"sub={self.sub!r}, "
            f"iss={self.iss!r}, "
            f"token_type={self.token_type!r}, "
            f"exp={self.exp!r}, "
            f"has_auth_ctx={self.auth_ctx is not None})"
        )
