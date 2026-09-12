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

from typing import TYPE_CHECKING, Any

# PyJWT — aliased to avoid shadowing the local `jwt` package name
import jwt as _jwt

from varco_core.auth import Action, AuthContext, ResourceGrant
from varco_core.jwt.model import (
    _RESERVED_CLAIM_KEYS,
    JsonWebToken,
    _from_utc_timestamp,
)
from varco_core.jwt.transform.protocol import ClaimTransformer
from varco_core.jwt.transform.runtime import resolve_claim_transformer
from varco_core.jwt.transform.shape import ValueShape, normalize

if TYPE_CHECKING:
    # TYPE_CHECKING-only — avoids importing varco_core.jwt.profile at module
    # load time (profile.py imports JsonWebToken from this package's sibling
    # model.py; no cycle exists today, but keeping the profile import local
    # to the one function that needs it documents the intentional layering:
    # parser.py is the lower layer, profile.py builds on top of it).
    from varco_core.jwt.profile import TokenProfileRegistry


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
        algorithms: list[str],
        audience: str | list[str] | None = None,
        options: dict[str, Any] | None = None,
        transformer: ClaimTransformer | None = None,
        profiles: TokenProfileRegistry | None = None,
        leeway: float | None = None,
    ) -> JsonWebToken:
        """
        Decode and verify a JWT string, returning a ``JsonWebToken``.

        Args:
            token:       Raw JWT string (``header.payload.signature``).
            secret:      Verification key.  Pass ``None`` only when
                         ``options={"verify_signature": False}`` — unverified
                         inspection is for debugging only, never production.
            algorithms:  Accepted algorithms.  **Required, keyword-only.**
                         There is no default and never will be — see the
                         ``DESIGN:`` block below (§D-S1-required). Pass the
                         algorithm(s) the token was actually signed with,
                         e.g. ``["HS256"]``; accepting an unbounded/implicit
                         set of algorithms is a known JWT attack vector
                         (algorithm confusion — a token signed with an
                         asymmetric key can be forged if a verifier will also
                         accept ``HS256`` using the public key as an HMAC
                         secret).
            audience:    Expected ``aud`` value(s).  ``None`` skips audience
                         verification (safe only for internal tokens that don't
                         carry an ``aud`` claim).
            options:     Raw PyJWT ``decode_options`` dict.  Use with caution
                         (e.g. ``{"verify_exp": False}`` disables expiry checks).
            transformer: Explicit ``ClaimTransformer`` — always wins over the
                         process-global registry (``resolve_claim_transformer``,
                         Plan 002 §A).  ``None`` (default) resolves it from
                         ``VARCO_JWT_TRANSFORM*``/``VARCO_JWT_TRANSFORM__<LABEL>__*``
                         env vars (or ``IDENTITY`` when none are set).
            profiles:    Explicit ``TokenProfileRegistry`` — always wins over
                         the process-global profile registry (Plan 002 §B).
                         ``None`` (default) resolves it from
                         ``VARCO_JWT_PROFILE__<NAME>__*`` env vars.
            leeway:      Clock-skew leeway in seconds for ``exp``/``nbf``
                         checks.  ``None`` (default) reads
                         ``VARCO_JWT_LEEWAY_SECONDS`` (default ``0.0`` — no
                         leeway, today's behaviour).

        Returns:
            ``JsonWebToken`` with all claims populated.  ``auth_ctx`` is set
            when ``roles``, ``scopes``, ``grants``, ``tenant_id``, or
            ``actor`` (canonical, post-transform) are present, or when a
            matched ``TokenProfile`` declares implied roles/scopes.

        Raises:
            jwt.ExpiredSignatureError:  Token has passed its ``exp`` time
                                        (beyond any configured ``leeway``).
            jwt.InvalidSignatureError:  Signature does not match ``secret``.
            jwt.DecodeError:            Token is malformed or not a valid JWT.
            jwt.InvalidAudienceError:   ``aud`` claim doesn't match ``audience``.
            ClaimTransformError:        A ``required=True`` claim-mapping rule
                                        found no value, a shape violation
                                        occurred under ``strict=True``, or the
                                        ``grants`` claim is malformed (missing
                                        ``resource``/``actions``).

        Edge cases:
            - ``auth_ctx.user_id`` is populated from ``sub`` (or a mapped
              ``user_id`` canonical source) when auth claims are present.
              If only ``sub`` is set and nothing else materialises an
              ``AuthContext``, ``auth_ctx`` is ``None`` — ``sub`` is still on
              ``token.sub``.
            - Unknown non-standard claims go into ``extra_claims`` (built
              from the *raw*, pre-transform dict — foreign claim names like
              ``sofy-roles`` remain visible for audit/debug).

        Example::

            tok = JwtParser.parse(raw, "my-secret", algorithms=["HS256"])
            util = JwtUtil(tok)
            util.has_auth_ctx()  # True when auth claims were present

        DESIGN: required keyword over a defaulted-to-``None``-that-raises (§D-S1-required)
            ✅ The failure is *static*: mypy (``strict = true``) flags every
               in-repo omission at ``make type-check`` time, before a test
               runs. A ``ValueError`` raised from a ``None`` default is only
               found at runtime, on the unhappy path — possibly in
               production.
            ✅ ``inspect.signature()``/an IDE state the requirement directly;
               the previous docstring's "always specify explicitly in
               production" becomes enforced, not advisory.
            ✅ Keyword-only, so no positional reshuffling: every existing
               call gains one keyword argument and nothing else changes.
            ❌ A bare ``TypeError`` is less self-explanatory than a curated
               message. Mitigated by this docstring, the CHANGELOG BREAKING
               entry, and the parameter name appearing verbatim in Python's
               own error message.
            ❌ A caller invoking ``parse(*args, **kwargs)`` dynamically fails
               at runtime, not at type-check time — verified absent in this
               repository (Plan 034 Step 1's grep).
            Rejected — keep ``algorithms`` optional and raise ``ValueError``
              on ``None``: gives up the mypy-time catch, which is the whole
              point, and trades a ``TypeError`` at dev time for a
              ``ValueError`` in a hot auth path at request time.
            Rejected — default to ``["RS256"]`` instead of ``["HS256"]``:
              still a silent default, still algorithm confusion in the other
              direction, and does not close the row.
            Rejected — derive ``algorithms`` from the token header's own
              ``alg``: that *is* the algorithm-confusion attack this row
              exists to close.

            There is deliberately no environment-variable escape hatch
            (e.g. ``VARCO_JWT_DEFAULT_ALGORITHMS``) — that would restore
            exactly the ambient, invisible default this change removes,
            settable by an operator who never reads this code. The fix is
            always the explicit keyword at the call site.
        """
        if leeway is None:
            # Local import — avoids a hard import-time dependency between
            # parser.py (imported very early, by authority/registry.py too)
            # and config.py's pydantic-settings machinery for callers that
            # never need leeway at all.
            from varco_core.jwt.config import JwtVerificationSettings

            leeway = JwtVerificationSettings.from_env().leeway_seconds

        decode_kwargs: dict[str, Any] = {"algorithms": algorithms, "leeway": leeway}
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

        return cls._from_raw_claims(raw, transformer=transformer, profiles=profiles)

    @classmethod
    def parse_unverified(
        cls,
        token: str,
        *,
        transformer: ClaimTransformer | None = None,
        profiles: TokenProfileRegistry | None = None,
    ) -> JsonWebToken:
        """
        Decode a JWT string WITHOUT verifying the signature.

        **For debugging and introspection only.**  Never trust the claims
        produced by this method in a security-sensitive context.

        Args:
            token:       Raw JWT string.
            transformer: Explicit ``ClaimTransformer`` override — see
                         ``parse()``.
            profiles:    Explicit ``TokenProfileRegistry`` override — see
                         ``parse()``.

        Returns:
            ``JsonWebToken`` with claims extracted from the payload section.
            Signature is NOT checked — any token passes.  The same claim
            transformer / token-profile pipeline as ``parse()`` runs, so
            gateway-fronted callers (``PassthroughAuth``) benefit from it
            without duplicating parsing logic.

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
        return cls._from_raw_claims(raw, transformer=transformer, profiles=profiles)

    @classmethod
    def _from_raw_claims(
        cls,
        raw: dict[str, Any],
        *,
        transformer: ClaimTransformer | None = None,
        profiles: TokenProfileRegistry | None = None,
    ) -> JsonWebToken:
        """
        Construct a ``JsonWebToken`` from a raw decoded claims dict.

        Internal helper shared by ``parse()``, ``parse_unverified()`` **and**
        ``TrustedIssuerRegistry.verify()`` — the single funnel that makes
        claim transformation and token-profile resolution work identically
        across every JWT entry point (Plan 002 design: "one insertion point,
        two seams covered").

        Args:
            raw:         Claims dict as returned by ``jwt.decode()``.
            transformer: Explicit ``ClaimTransformer`` override — see
                         ``parse()``.  ``None`` resolves via
                         ``resolve_claim_transformer(raw.get("iss"))``.
            profiles:    Explicit ``TokenProfileRegistry`` override.  ``None``
                         resolves via the process-global profile registry.

        Returns:
            Fully populated ``JsonWebToken``, profile-augmented.

        Edge cases:
            - Missing standard claims → ``None`` fields.
            - ``aud`` as a list → ``frozenset[str]``.
            - Unknown claims → ``extra_claims`` dict (built from ``raw``,
              i.e. BEFORE transformation — foreign claim names stay visible).
        """
        tf: ClaimTransformer = (
            transformer if transformer is not None else resolve_claim_transformer(raw.get("iss"))
        )
        # Non-destructive: IDENTITY returns `raw` itself (no copy — the
        # zero-config hot path); MappingClaimTransformer returns a new dict
        # with canonical keys added/overwritten on top of every original key.
        canonical: dict[str, Any] = dict(tf.transform(raw))

        # PyJWT decodes exp / iat / nbf as integer Unix timestamps;
        # convert to tz-aware datetimes for ergonomic Python comparisons.
        # exp/iat/nbf/aud/jti/iss are NOT remappable (decision D-2) — read
        # from `raw` directly (identical to reading from `canonical`, since
        # no rule ever touches these keys, but `raw` makes that invariant
        # explicit at the call site).
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

        # Reconstruct AuthContext from the (possibly transformed) canonical
        # claims — this is where foreign claim names actually take effect.
        auth_ctx = cls._build_auth_ctx(canonical)

        # extra_claims is built from RAW (pre-transform) — foreign claim
        # names (e.g. "sofy-roles") stay visible for audit/debug even though
        # they were also consumed into a canonical field above.
        extra_claims = {k: v for k, v in raw.items() if k not in _RESERVED_CLAIM_KEYS}

        token = JsonWebToken(
            sub=raw.get("sub"),
            iss=raw.get("iss"),
            aud=aud,
            exp=exp,
            iat=iat,
            nbf=nbf,
            jti=raw.get("jti"),
            # token_type IS a canonical remap target (Keycloak "typ",
            # Cognito "token_use", …) — read from canonical, not raw.
            token_type=canonical.get("token_type"),
            auth_ctx=auth_ctx,
            extra_claims=extra_claims,
        )

        # Local import — varco_core.jwt.profile depends on this module
        # (JsonWebToken) and JwtBuilder; importing it at module level here
        # would create a cycle. resolve_token_profile() is a no-op passthrough
        # when no profiles are registered anywhere (env or explicit).
        from varco_core.jwt.profile import resolve_token_profile

        return resolve_token_profile(token, registry=profiles)

    @classmethod
    def _build_auth_ctx(cls, canonical: dict[str, Any]) -> AuthContext | None:
        """
        Reconstruct an ``AuthContext`` from (post-transform) canonical claims.

        Returns ``None`` when none of the auth-specific canonical claims
        (``roles``, ``scopes``, ``grants``, ``tenant_id``, ``actor``) are
        present — ``sub`` alone does not create an ``AuthContext``, since it
        is already available as ``token.sub``.

        Args:
            canonical: The (possibly transformed) claims dict — already run
                       through the resolved ``ClaimTransformer``.

        Returns:
            ``AuthContext`` when any auth-specific canonical claim is
            present; ``None`` otherwise.

        Raises:
            ClaimTransformError: The ``grants`` claim is structurally
                invalid (not a list, or an entry missing ``resource``/
                ``actions``) — replaces the previous bare ``KeyError``.
            ValueError: An action string in ``grants[*].actions`` is not a
                        member of ``Action`` — indicates an untrusted issuer
                        or schema mismatch.

        Edge cases:
            - ``sub`` (or a mapped ``user_id`` canonical source) is used as
              ``auth_ctx.user_id`` when an ``AuthContext`` is materialised.
            - Empty ``roles``/``scopes``/``grants`` in the token → empty
              frozenset/tuple in the resulting ``AuthContext``.
            - ⚠️ Widened trigger vs. pre-Plan-002 behaviour: a token
              carrying only ``tenant_id``/``actor`` (no roles/scopes/grants)
              now also materialises an ``AuthContext`` — see the plan's
              Edge cases table and Risks section.
            - ⚠️ Plan 033 / S5 widens the trigger again, the same way: a
              token carrying only a ``tenants`` claim (no roles/scopes/
              grants/tenant_id/actor) now also materialises an
              ``AuthContext`` where it previously produced ``None``.
        """
        roles_raw: list[str] = canonical.get("roles", [])
        scopes_raw: list[str] = canonical.get("scopes", [])

        # ValueShape.GRANTS validates structure and gives an actionable
        # error naming the offending index — replaces the old bare KeyError.
        grants_raw: list[dict[str, Any]] = normalize(
            canonical.get("grants", []), ValueShape.GRANTS, target="grants"
        )

        tenant_id = canonical.get("tenant_id")
        actor = canonical.get("actor")
        # normalize() here (not just via a ClaimRule) so the zero-config
        # IDENTITY path — a raw scalar "tenants" claim with no
        # VARCO_JWT_TRANSFORM_TENANTS_FIELD configured — still produces a
        # list, matching the ROLES/SCOPES shape family §D-S5-claim promises.
        tenants_raw: list[str] = normalize(
            canonical.get("tenants"), ValueShape.AUTO, target="tenants"
        )

        if (
            not roles_raw
            and not scopes_raw
            and not grants_raw
            and tenant_id is None
            and actor is None
            and not tenants_raw
        ):
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

        metadata: dict[str, Any] = {}
        if tenant_id is not None:
            metadata["tenant_id"] = tenant_id
        if actor is not None:
            metadata["actor"] = actor
        if tenants_raw:
            metadata["tenants"] = tenants_raw

        return AuthContext(
            user_id=canonical.get("user_id", canonical.get("sub")),
            roles=frozenset(roles_raw),
            scopes=frozenset(scopes_raw),
            grants=grants,
            metadata=metadata,
        )
