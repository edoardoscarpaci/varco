"""
varco_core.jwt.parser
=========================

``JwtParser`` — stateless JWT decoder.

Decodes and optionally verifies a raw JWT string, then reconstructs
a typed ``JsonWebToken`` — including the embedded ``AuthContext`` when
identity/permission claims (``roles``, ``scopes``, ``grants``) are present.

DESIGN: class with classmethods over module-level functions
    ✅ Groups all parsing logic in one namespace.
    ✅ Subclassable: override ``_build_auth_ctx`` for custom claim schemas.
    ✅ Symmetrical with JwtBuilder — parse() inverts encode().
    ❌ Slight import indirection vs. plain functions — minor inconvenience.

Thread safety:  ✅ Stateless — all methods are classmethods.
Async safety:   ✅ No async operations; safe to call inside async contexts.
"""

from __future__ import annotations

from typing import Any

# PyJWT — aliased to avoid shadowing the local `jwt` package name
import jwt as _jwt

from varco_core.auth import Action, AuthContext, ResourceGrant
from varco_core.jwt.model import (
    JsonWebToken,
    _RESERVED_CLAIM_KEYS,
    _from_utc_timestamp,
)


class JwtParser:
    """
    Stateless JWT decoder.

    Decodes and optionally verifies a raw JWT string, then reconstructs
    a typed ``JsonWebToken`` — including the embedded ``AuthContext`` when
    identity/permission claims are present.

    Thread safety:  ✅ Stateless — all methods are classmethods.
    Async safety:   ✅ No async operations; safe to call inside async contexts.
    """

    @classmethod
    def parse(
        cls,
        token: str,
        secret: str | bytes | None = None,
        *,
        algorithms: list[str] | None = None,
        audience: str | list[str] | None = None,
        options: dict[str, Any] | None = None,
    ) -> JsonWebToken:
        """
        Decode and verify a JWT string, returning a ``JsonWebToken``.

        Args:
            token:      Raw JWT string (``header.payload.signature``).
            secret:     Verification key.  Pass ``None`` only when
                        ``options={"verify_signature": False}`` — unverified
                        inspection is for debugging only, never production.
            algorithms: Accepted algorithms.  Defaults to ``["HS256"]``.
                        Always specify explicitly in production — accepting
                        any algorithm is a known JWT attack vector.
            audience:   Expected ``aud`` value(s).  ``None`` skips audience
                        verification (safe only for internal tokens that don't
                        carry an ``aud`` claim).
            options:    Raw PyJWT ``decode_options`` dict.  Use with caution
                        (e.g. ``{"verify_exp": False}`` disables expiry checks).

        Returns:
            ``JsonWebToken`` with all claims populated.  ``auth_ctx`` is set
            when ``roles``, ``scopes``, or ``grants`` claims are present.

        Raises:
            jwt.ExpiredSignatureError:  Token has passed its ``exp`` time.
            jwt.InvalidSignatureError:  Signature does not match ``secret``.
            jwt.DecodeError:            Token is malformed or not a valid JWT.
            jwt.InvalidAudienceError:   ``aud`` claim doesn't match ``audience``.
            ValueError:                 An action string in ``grants`` is not a
                                        valid ``Action`` member — indicates a
                                        token from an untrusted or misconfigured
                                        issuer.

        Edge cases:
            - ``auth_ctx.user_id`` is populated from ``sub`` when auth claims
              are present.  If only ``sub`` is set and no auth claims exist,
              ``auth_ctx`` is ``None`` — ``sub`` is still on ``token.sub``.
            - ``auth_ctx.metadata`` is always empty — JWT claims map to typed
              fields, not the metadata bag.
            - Unknown non-standard claims go into ``extra_claims``.

        Example::

            tok = JwtParser.parse(raw, "my-secret", algorithms=["HS256"])
            util = JwtUtil(tok)
            util.has_auth_ctx()  # True when auth claims were present
        """
        if algorithms is None:
            # Default HS256; always pass algorithms explicitly in production
            algorithms = ["HS256"]

        decode_kwargs: dict[str, Any] = {"algorithms": algorithms}
        if audience is not None:
            decode_kwargs["audience"] = audience
        if options is not None:
            decode_kwargs["options"] = options

        raw: dict[str, Any] = _jwt.decode(
            token,
            # PyJWT still requires a key arg even with verify_signature=False;
            # passing empty string satisfies the call without confusion.
            secret or "",
            **decode_kwargs,
        )

        return cls._from_raw_claims(raw)

    @classmethod
    def parse_unverified(cls, token: str) -> JsonWebToken:
        """
        Decode a JWT string WITHOUT verifying the signature.

        **For debugging and introspection only.**  Never trust the claims
        produced by this method in a security-sensitive context.

        Args:
            token: Raw JWT string.

        Returns:
            ``JsonWebToken`` with claims extracted from the payload section.
            Signature is NOT checked — any token passes.

        Raises:
            jwt.DecodeError: Token is not a well-formed three-segment JWT.

        Edge cases:
            - Expired tokens decode successfully — expiry is NOT checked.
            - Forged tokens decode successfully — use only with trusted input
              or for debugging; never for authorization decisions.
        """
        # verify_signature=False tells PyJWT to skip all cryptographic checks.
        # Broad algorithm list so any token can be inspected regardless of alg.
        raw: dict[str, Any] = _jwt.decode(
            token,
            options={"verify_signature": False},
            algorithms=[
                "HS256",
                "HS384",
                "HS512",
                "RS256",
                "RS384",
                "RS512",
                "ES256",
                "ES384",
                "ES512",
                "PS256",
                "PS384",
                "PS512",
            ],
        )
        return cls._from_raw_claims(raw)

    @classmethod
    def _from_raw_claims(cls, raw: dict[str, Any]) -> JsonWebToken:
        """
        Construct a ``JsonWebToken`` from a raw decoded claims dict.

        Internal helper shared by ``parse()`` and ``parse_unverified()``.

        Args:
            raw: Claims dict as returned by ``jwt.decode()``.

        Returns:
            Fully populated ``JsonWebToken``.

        Edge cases:
            - Missing standard claims → ``None`` fields.
            - ``aud`` as a list → ``frozenset[str]``.
            - Unknown claims → ``extra_claims`` dict.
        """
        # PyJWT decodes exp / iat / nbf as integer Unix timestamps;
        # convert to tz-aware datetimes for ergonomic Python comparisons.
        exp = _from_utc_timestamp(raw["exp"]) if "exp" in raw else None
        iat = _from_utc_timestamp(raw["iat"]) if "iat" in raw else None
        nbf = _from_utc_timestamp(raw["nbf"]) if "nbf" in raw else None

        # ``aud`` can arrive as str or list[str] depending on audience count;
        # normalise to the typed form used by JsonWebToken.
        raw_aud = raw.get("aud")
        aud: str | frozenset[str] | None = None
        if isinstance(raw_aud, list):
            aud = frozenset(raw_aud)
        elif isinstance(raw_aud, str):
            aud = raw_aud

        # Reconstruct AuthContext only when auth-specific custom claims exist
        auth_ctx = cls._build_auth_ctx(raw)

        # Everything not in the known standard-claim set goes into extra_claims
        extra_claims = {k: v for k, v in raw.items() if k not in _RESERVED_CLAIM_KEYS}

        return JsonWebToken(
            sub=raw.get("sub"),
            iss=raw.get("iss"),
            aud=aud,
            exp=exp,
            iat=iat,
            nbf=nbf,
            jti=raw.get("jti"),
            token_type=raw.get("token_type"),
            auth_ctx=auth_ctx,
            extra_claims=extra_claims,
        )

    @classmethod
    def _build_auth_ctx(cls, raw: dict[str, Any]) -> AuthContext | None:
        """
        Reconstruct an ``AuthContext`` from raw JWT claims.

        Returns ``None`` when no auth-specific claims (``roles``, ``scopes``,
        ``grants``) are present — ``sub`` alone does not create an
        ``AuthContext``, since it is already available as ``token.sub``.

        Args:
            raw: Raw decoded claims dict.

        Returns:
            ``AuthContext`` when ``roles``, ``scopes``, or ``grants`` claims
            are present; ``None`` otherwise.

        Raises:
            ValueError: An action string in ``grants[*].actions`` is not a
                        member of ``Action`` — indicates an untrusted issuer
                        or schema mismatch.

        Edge cases:
            - ``sub`` is used as ``auth_ctx.user_id`` when auth claims exist.
            - Empty ``roles`` / ``scopes`` / ``grants`` in the token → empty
              frozenset / tuple in the resulting ``AuthContext``.
            - ``metadata`` is always empty — JWT claims map to typed fields.
        """
        roles_raw: list[str] = raw.get("roles", [])
        scopes_raw: list[str] = raw.get("scopes", [])
        grants_raw: list[dict[str, Any]] = raw.get("grants", [])

        # Only materialise an AuthContext when at least one auth-specific claim
        # is present.  sub alone is not sufficient — it lives on token.sub.
        if not roles_raw and not scopes_raw and not grants_raw:
            return None

        # Action() constructor raises ValueError for unknown action strings —
        # this is intentional: callers must ensure the token source is trusted.
        grants: tuple[ResourceGrant, ...] = tuple(
            ResourceGrant(
                resource=g["resource"],
                actions=frozenset(Action(a) for a in g["actions"]),
            )
            for g in grants_raw
        )

        return AuthContext(
            user_id=raw.get("sub"),
            roles=frozenset(roles_raw),
            scopes=frozenset(scopes_raw),
            grants=grants,
        )
