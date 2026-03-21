"""
fastrest_core.jwt.builder
==========================

``JwtBuilder`` — fluent builder for constructing JWT tokens.

DESIGN: mutable builder over a direct frozen-dataclass constructor
    ✅ Natural chaining syntax — omit any field you don't need.
    ✅ build() is side-effect-free — useful in tests without a real secret.
    ✅ encode() is the single production path; no separate signing step.
    ❌ Builder instances are NOT thread-safe — create one per token.
    ❌ Slight overhead vs. direct dataclass construction — irrelevant for
       the sub-million tokens-per-second workloads this targets.

Thread safety:  ❌ Mutable — do NOT share across threads or async tasks.
Async safety:   ✅ No async operations; safe to call inside async contexts.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Self

# PyJWT — the only external dependency for this module.
# Aliased to avoid shadowing the local `jwt` package name.
import jwt as _jwt

from fastrest_core.auth import AuthContext
from fastrest_core.jwt.model import JsonWebToken, _RESERVED_CLAIM_KEYS


class JwtBuilder:
    """
    Fluent builder for constructing JWT tokens.

    Each setter returns ``self`` for chaining.  Call ``build()`` to produce
    an unsigned ``JsonWebToken`` value object, or ``encode()`` to sign and
    return the raw JWT string in one step.

    Thread safety:  ❌ Mutable — do NOT share across threads or async tasks.
    Async safety:   ✅ No async operations; safe to call inside async contexts.

    Example::

        token_str = (
            JwtBuilder()
            .subject("usr_123")
            .issuer("my-service")
            .type("access")
            .issued_now()
            .expires_in(timedelta(hours=1))
            .with_random_jti()
            .with_auth_ctx(auth_ctx)
            .encode("my-secret")
        )
    """

    # __slots__ avoids accidental attribute additions on builder instances
    __slots__ = (
        "_sub",
        "_iss",
        "_aud",
        "_exp",
        "_iat",
        "_nbf",
        "_jti",
        "_token_type",
        "_auth_ctx",
        "_extra_claims",
    )

    def __init__(self) -> None:
        """
        Initialise builder with all fields unset.

        Call the setter methods to configure the token, then call
        ``build()`` or ``encode()`` to produce the final result.
        """
        self._sub: str | None = None
        self._iss: str | None = None
        self._aud: str | frozenset[str] | None = None
        self._exp: datetime | None = None
        self._iat: datetime | None = None
        self._nbf: datetime | None = None
        self._jti: str | None = None
        self._token_type: str | None = None
        self._auth_ctx: AuthContext | None = None
        self._extra_claims: dict[str, Any] = {}

    # ── Identity / issuer setters ─────────────────────────────────────────────

    def subject(self, sub: str) -> Self:
        """
        Set the ``sub`` (subject) claim.

        Args:
            sub: Unique identifier of the token's principal (e.g. user ID).

        Returns:
            This builder for chaining.
        """
        self._sub = sub
        return self

    def issuer(self, iss: str) -> Self:
        """
        Set the ``iss`` (issuer) claim.

        Args:
            iss: Identifier of the authority that issued this token.

        Returns:
            This builder for chaining.
        """
        self._iss = iss
        return self

    def audience(self, aud: str | frozenset[str]) -> Self:
        """
        Set the ``aud`` (audience) claim.

        Args:
            aud: Intended recipient — a single service name or a ``frozenset``
                 of names for multi-audience tokens.

        Returns:
            This builder for chaining.
        """
        self._aud = aud
        return self

    # ── Temporal setters ──────────────────────────────────────────────────────

    def expires_at(self, exp: datetime) -> Self:
        """
        Set ``exp`` to an absolute UTC datetime.

        Args:
            exp: UTC-aware datetime after which the token is invalid.

        Returns:
            This builder for chaining.
        """
        self._exp = exp
        return self

    def expires_in(self, delta: timedelta) -> Self:
        """
        Set ``exp`` as an offset from the current UTC time.

        Args:
            delta: Duration until expiry (e.g. ``timedelta(hours=1)``).

        Returns:
            This builder for chaining.
        """
        # Compute from "now" at call time — deterministic for the current call
        self._exp = datetime.now(timezone.utc) + delta
        return self

    def issued_at(self, iat: datetime) -> Self:
        """
        Set the ``iat`` (issued-at) claim to an explicit datetime.

        Args:
            iat: UTC-aware datetime when this token was minted.

        Returns:
            This builder for chaining.
        """
        self._iat = iat
        return self

    def issued_now(self) -> Self:
        """
        Set ``iat`` to the current UTC time.

        Returns:
            This builder for chaining.
        """
        self._iat = datetime.now(timezone.utc)
        return self

    def not_before(self, nbf: datetime) -> Self:
        """
        Set the ``nbf`` (not-before) claim.

        Args:
            nbf: UTC-aware datetime before which the token must be rejected.

        Returns:
            This builder for chaining.
        """
        self._nbf = nbf
        return self

    # ── JTI setters ───────────────────────────────────────────────────────────

    def token_id(self, jti: str) -> Self:
        """
        Set the ``jti`` (JWT ID) claim to an explicit value.

        Args:
            jti: Unique token identifier (e.g. a UUID or database ID).

        Returns:
            This builder for chaining.
        """
        self._jti = jti
        return self

    def with_random_jti(self) -> Self:
        """
        Generate and set a random UUID v4 as the ``jti`` claim.

        Useful for replay-protection without managing IDs yourself.

        Returns:
            This builder for chaining.
        """
        # uuid4 provides ~122 bits of randomness — collision probability negligible
        self._jti = str(uuid.uuid4())
        return self

    # ── Custom claim setters ──────────────────────────────────────────────────

    def type(self, token_type: str) -> Self:
        """
        Set the application-level ``token_type`` custom claim.

        Args:
            token_type: Discriminator string such as ``"access"``,
                        ``"refresh"``, or ``"system"``.

        Returns:
            This builder for chaining.
        """
        self._token_type = token_type
        return self

    def with_auth_ctx(self, ctx: AuthContext) -> Self:
        """
        Attach an ``AuthContext`` to this token.

        The context is serialized into ``roles``, ``scopes``, and ``grants``
        custom claims by ``JsonWebToken.to_claims()``.

        Args:
            ctx: The caller's identity and permission snapshot.

        Returns:
            This builder for chaining.
        """
        self._auth_ctx = ctx
        return self

    def claim(self, key: str, value: Any) -> Self:
        """
        Add an arbitrary non-standard custom claim.

        Args:
            key:   Claim name.  Must not conflict with any reserved claim name
                   (``sub``, ``iss``, ``aud``, ``exp``, ``iat``, ``nbf``,
                   ``jti``, ``token_type``, ``roles``, ``scopes``, ``grants``).
            value: Any JSON-serializable value.

        Returns:
            This builder for chaining.

        Raises:
            ValueError: ``key`` conflicts with a reserved claim name — use the
                        dedicated setter method (e.g. ``.subject()``) instead.

        Example::

            builder.claim("tenant_id", "t_abc")
        """
        if key in _RESERVED_CLAIM_KEYS:
            raise ValueError(
                f"Claim key {key!r} is reserved by fastrest JWT encoding. "
                f"Use the dedicated setter method instead "
                f"(e.g. .subject(), .issuer(), .type(), .with_auth_ctx()). "
                f"Reserved keys: {sorted(_RESERVED_CLAIM_KEYS)}"
            )
        self._extra_claims[key] = value
        return self

    # ── Terminal operations ───────────────────────────────────────────────────

    def build(self) -> JsonWebToken:
        """
        Construct a ``JsonWebToken`` value object from the current builder state.

        Does NOT sign or encode — use ``encode()`` for production use.
        Useful in tests that only need a token object without real cryptography.

        Returns:
            Immutable ``JsonWebToken`` with all configured fields populated.

        Example::

            tok = JwtBuilder().subject("usr_1").type("access").build()
        """
        return JsonWebToken(
            sub=self._sub,
            iss=self._iss,
            aud=self._aud,
            exp=self._exp,
            iat=self._iat,
            nbf=self._nbf,
            jti=self._jti,
            token_type=self._token_type,
            auth_ctx=self._auth_ctx,
            # Copy extra_claims — builder is mutable, token must be isolated
            extra_claims=dict(self._extra_claims),
        )

    def encode(self, secret: str | bytes, *, algorithm: str = "HS256") -> str:
        """
        Sign and encode the token to a JWT string.

        Calls ``build()`` internally, then delegates signing to PyJWT.

        Args:
            secret:    Secret key for HMAC algorithms (HS256/HS384/HS512) or
                       the private key bytes/str for RS* / ES* / PS* algorithms.
            algorithm: JWT signing algorithm.  Defaults to ``"HS256"``.
                       Supported algorithms depend on installed PyJWT extras
                       (``pip install pyjwt[crypto]`` for RSA / EC).

        Returns:
            URL-safe base64-encoded JWT string (``header.payload.signature``).

        Raises:
            jwt.exceptions.InvalidKeyError: ``secret`` is incompatible with
                the chosen ``algorithm``.

        Example::

            s = JwtBuilder().subject("u1").expires_in(timedelta(hours=1)).encode("s3cr3t")
        """
        token = self.build()
        return _jwt.encode(token.to_claims(), secret, algorithm=algorithm)

    def __repr__(self) -> str:
        return (
            f"JwtBuilder("
            f"sub={self._sub!r}, "
            f"iss={self._iss!r}, "
            f"type={self._token_type!r})"
        )
