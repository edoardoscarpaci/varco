"""
fastrest_core.authority.jwt_authority
=======================================

``JwtAuthority`` — the core single-key authority: owns one asymmetric key
pair, signs tokens with the private key, and verifies tokens with the
public key.

This is the building block.  ``MultiKeyAuthority`` wraps multiple
``JwtAuthority`` instances to support key rotation.

DESIGN: plain class with __slots__ over frozen dataclass
    ✅ __slots__ prevents accidental attribute addition — close to immutable.
    ✅ Crypto key objects (RSAPrivateKey, EllipticCurvePrivateKey) are not
       hashable — frozen dataclass would break because __hash__ tries to hash
       all fields.
    ✅ Private fields with property accessors give a clean public API without
       exposing the raw key objects.
    ❌ Not truly immutable — a determined caller could do
       object.__setattr__(authority, '_kid', 'hacked').  Acceptable tradeoff:
       JwtAuthority is an internal service object, not a user-facing value.
    Alternative considered: @dataclass(frozen=True, eq=False) with
       field(compare=False, hash=False) for key fields — viable but verbose
       and still not truly immutable.

Thread safety:  ✅ Safe — all fields set once in __init__, never mutated.
                   Multiple threads / tasks may call sign() and verify()
                   concurrently without synchronisation.
Async safety:   ✅ sign() and verify() are pure CPU operations (no I/O).
                   Safe to call from async context without to_thread().
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

# PyJWT — aliased to avoid shadowing the local jwt package
import jwt as _jwt

from fastrest_core.jwk.builder import JwkBuilder
from fastrest_core.jwk.model import JsonWebKey, JsonWebKeySet
from fastrest_core.jwt.builder import JwtBuilder
from fastrest_core.jwt.model import JsonWebToken
from fastrest_core.jwt.parser import JwtParser

from fastrest_core.authority.exceptions import KeyLoadError, UnknownKidError

if TYPE_CHECKING:
    # Heavy crypto imports only for type checking — at runtime the key objects
    # are passed in already constructed, so we don't need to import them here.
    pass


# ── JwtAuthority ──────────────────────────────────────────────────────────────


class JwtAuthority:
    """
    Single-key JWT authority — owns one asymmetric key pair.

    Combines JWK (key material) with JWT (token operations) into one object:
    - ``token()`` returns a ``JwtBuilder`` pre-seeded with this issuer.
    - ``sign(builder)`` encodes the token with the private key + injects kid.
    - ``verify(token_str)`` verifies the signature using the public key.
    - ``jwk()`` / ``jwks()`` expose the public key for JWKS endpoints.

    Only asymmetric algorithms (RSA, EC) are supported — symmetric HMAC keys
    have no public key material to expose in a JWKS and belong in the
    symmetric-key path (JwtBuilder.encode() with a shared secret).

    Thread safety:  ✅ All fields set once; sign() and verify() are safe to
                       call concurrently from multiple threads or async tasks.
    Async safety:   ✅ Pure CPU — no I/O, no blocking.

    Attributes (read-only properties):
        kid:       Key ID — injected into JWT headers on sign().
        issuer:    Issuer string — pre-seeded into JwtBuilder on token().
        algorithm: JWT algorithm — e.g. ``"RS256"``, ``"ES256"``.

    Edge cases:
        - ``verify()`` checks the ``kid`` header claim when present in the
          token.  A token with no ``kid`` header is still accepted (some
          legacy tokens omit it).
        - ``verify()`` does NOT enforce the ``iss`` claim — that is the
          caller's responsibility.  Use ``JwtUtil(token).is_issuer(...)`` or
          the ``TrustedIssuerRegistry`` for ``iss``-aware verification.
        - ``sign()`` always injects ``kid`` into the JWT header — tokens
          produced by this authority are always routable by ``MultiKeyAuthority``.

    Example::

        with open("/etc/certs/service.pem", "rb") as f:
            authority = JwtAuthority.from_pem(
                f.read(),
                kid="svc:auth-2025-A",
                issuer="my-service",
                algorithm="RS256",
            )

        token_str = (
            authority.token()
            .subject("usr_123")
            .expires_in(timedelta(hours=1))
            .type("access")
            |> authority.sign
        )
        # Or more naturally:
        builder = authority.token().subject("usr_123").expires_in(timedelta(hours=1))
        token_str = authority.sign(builder)

        verified = authority.verify(token_str)
        assert verified.iss == "my-service"
    """

    # __slots__ prevents accidental attribute addition and saves memory.
    # Private key is stored as raw cryptography object — never serialise it.
    __slots__ = (
        "_private_key",
        "_public_key",
        "_public_jwk",
        "_kid",
        "_issuer",
        "_algorithm",
    )

    def __init__(
        self,
        *,
        private_key: Any,  # RSAPrivateKey | EllipticCurvePrivateKey
        public_key: Any,  # RSAPublicKey | EllipticCurvePublicKey
        public_jwk: JsonWebKey,
        kid: str,
        issuer: str,
        algorithm: str,
    ) -> None:
        """
        Construct a ``JwtAuthority`` from pre-loaded key material.

        Use ``from_pem()`` instead of calling this directly — it handles key
        loading, public key extraction, and JWK construction.

        Args:
            private_key: Cryptography private key object.  Used for signing.
            public_key:  Cryptography public key object.  Used for verification.
            public_jwk:  ``JsonWebKey`` representing the public key.  Used
                         for JWKS endpoint exposure.
            kid:         Key ID injected into all signed JWT headers.
            issuer:      Issuer string pre-seeded into ``JwtBuilder`` on
                         ``token()``.
            algorithm:   JWT signing algorithm (e.g. ``"RS256"``).

        Raises:
            Nothing — callers are responsible for passing valid key objects.
        """
        self._private_key = private_key
        self._public_key = public_key
        self._public_jwk = public_jwk
        self._kid = kid
        self._issuer = issuer
        self._algorithm = algorithm

    # ── Construction ──────────────────────────────────────────────────────────

    @classmethod
    def from_pem(
        cls,
        pem_bytes: bytes,
        *,
        kid: str,
        issuer: str,
        algorithm: str,
        password: bytes | None = None,
    ) -> JwtAuthority:
        """
        Construct a ``JwtAuthority`` from PEM-encoded private key bytes.

        Loads the private key, extracts its public key, and builds the
        corresponding ``JsonWebKey``.  Only private key PEMs are accepted —
        you cannot sign tokens with a public key.

        Args:
            pem_bytes: PEM-encoded private key bytes.  Accepts PKCS#8
                       (``BEGIN PRIVATE KEY``) and traditional formats
                       (``BEGIN RSA PRIVATE KEY``, ``BEGIN EC PRIVATE KEY``).
            kid:       Key ID — injected into all JWT headers signed by this
                       authority.  Should be globally unique across all issuers.
            issuer:    Issuer string pre-seeded into ``JwtBuilder`` via ``token()``.
            algorithm: JWT algorithm — must match the key type:
                       RSA keys: ``"RS256"``, ``"RS384"``, ``"RS512"``,
                                 ``"PS256"``, ``"PS384"``, ``"PS512"``
                       EC keys:  ``"ES256"`` (P-256), ``"ES384"`` (P-384),
                                 ``"ES512"`` (P-521)
            password:  Passphrase for encrypted PEM files.  ``None`` for
                       unencrypted keys.

        Returns:
            Fully initialised ``JwtAuthority``.

        Raises:
            KeyLoadError: PEM is malformed, is a public key (not private),
                          or the key type is unsupported (only RSA and EC
                          are supported).

        Edge cases:
            - Passing a public key PEM raises ``KeyLoadError`` — you cannot
              sign with a public key.
            - Encrypted PEM files require the ``password`` parameter.
        """
        from cryptography.hazmat.primitives.serialization import load_pem_private_key

        try:
            private_key = load_pem_private_key(pem_bytes, password=password)
        except (ValueError, TypeError, UnicodeDecodeError) as e:
            raise KeyLoadError(
                f"Cannot load private key for authority kid={kid!r}: {e}. "
                f"Ensure pem_bytes contains a PEM-encoded private key "
                f"(PKCS#8 or traditional RSA/EC format).  "
                f"Public key PEMs cannot be used with JwtAuthority — "
                f"signing requires the private key."
            ) from e

        public_key = private_key.public_key()

        # JwkBuilder.from_pem() strips private material automatically (include_private=False)
        try:
            public_jwk = JwkBuilder.from_pem(
                pem_bytes,
                kid=kid,
                use="sig",
                alg=algorithm,
                password=password,
            )
        except (ValueError, TypeError) as e:
            raise KeyLoadError(
                f"Cannot build JWK for authority kid={kid!r}: {e}. "
                f"Supported key types: RSA, EC (P-256, P-384, P-521)."
            ) from e

        return cls(
            private_key=private_key,
            public_key=public_key,
            public_jwk=public_jwk.public_key(),  # ensure no private material
            kid=kid,
            issuer=issuer,
            algorithm=algorithm,
        )

    # ── Read-only properties ───────────────────────────────────────────────────

    @property
    def kid(self) -> str:
        """Key ID — injected into all JWT headers signed by this authority."""
        return self._kid

    @property
    def issuer(self) -> str:
        """Issuer string — pre-seeded into ``JwtBuilder`` via ``token()``."""
        return self._issuer

    @property
    def algorithm(self) -> str:
        """JWT signing algorithm (e.g. ``"RS256"``, ``"ES256"``)."""
        return self._algorithm

    # ── Token operations ──────────────────────────────────────────────────────

    def token(self) -> JwtBuilder:
        """
        Return a ``JwtBuilder`` pre-seeded with this authority's issuer.

        The returned builder is independent — call ``sign()`` to produce the
        final JWT string.  The ``kid`` header is injected automatically in
        ``sign()``, not here.

        Returns:
            Fresh ``JwtBuilder`` with ``iss`` already set.

        Example::

            token_str = authority.sign(
                authority.token()
                .subject("usr_123")
                .expires_in(timedelta(hours=1))
            )
        """
        # Pre-seed issuer — caller only needs to set sub, exp, type, etc.
        return JwtBuilder().issuer(self._issuer)

    def sign(self, builder: JwtBuilder) -> str:
        """
        Sign the token and return the encoded JWT string.

        Calls ``builder.build()`` internally to get a ``JsonWebToken``, then
        encodes it with PyJWT using the private key and injects the ``kid``
        claim into the JOSE header.

        Args:
            builder: Configured ``JwtBuilder``.  The builder's ``iss`` field
                     is used as-is — it is NOT overwritten here.  Call
                     ``authority.token()`` to get a pre-seeded builder.

        Returns:
            URL-safe base64-encoded JWT string (``header.payload.signature``).

        Raises:
            jwt.exceptions.InvalidKeyError: Private key is incompatible with
                the configured algorithm.

        Edge cases:
            - The ``kid`` is injected into the JWT **header**, not the payload
              claims.  ``JsonWebToken.extra_claims`` will never contain ``kid``.
            - The builder's ``iss`` is used as-is — pass ``authority.token()``
              to ensure the issuer is set correctly.
        """
        token = builder.build()
        # headers= injects into JOSE header (first JWT segment), not payload.
        # This is the standard mechanism for kid — RFC 7515 §4.1.4.
        return _jwt.encode(
            token.to_claims(),
            self._private_key,
            algorithm=self._algorithm,
            headers={"kid": self._kid},
        )

    def verify(
        self,
        token_str: str,
        *,
        audience: str | list[str] | None = None,
    ) -> JsonWebToken:
        """
        Verify a JWT string and return the decoded ``JsonWebToken``.

        Checks that the signature is valid for this authority's public key.
        Optionally checks the ``aud`` claim.  Does NOT enforce ``iss`` — use
        ``JwtUtil(token).is_issuer(self.issuer)`` after this call if needed.

        Args:
            token_str: Raw JWT string (``header.payload.signature``).
            audience:  Expected ``aud`` value(s).  ``None`` skips audience
                       verification.

        Returns:
            ``JsonWebToken`` with all claims populated.

        Raises:
            UnknownKidError:            Token header contains a ``kid`` that
                                        does not match this authority's kid.
                                        Use ``MultiKeyAuthority`` for multi-kid
                                        verification.
            jwt.ExpiredSignatureError:  Token has passed its ``exp`` time.
            jwt.InvalidSignatureError:  Signature verification failed.
            jwt.DecodeError:            Token is malformed.
            jwt.InvalidAudienceError:   ``aud`` mismatch when ``audience``
                                        is provided.

        Edge cases:
            - A token with no ``kid`` header is accepted — kid check is skipped.
            - Expired tokens raise ``jwt.ExpiredSignatureError`` from PyJWT.
            - Does NOT check ``iss`` — that is intentional (see module docstring).
        """
        # Peek at the header to check kid before doing the expensive crypto.
        # get_unverified_header() only base64-decodes the first segment —
        # no signature verification, no payload parsing.
        header = _jwt.get_unverified_header(token_str)
        token_kid: str | None = header.get("kid")

        if token_kid is not None and token_kid != self._kid:
            raise UnknownKidError(
                f"Token kid={token_kid!r} does not match this authority's kid={self._kid!r}. "
                f"Use MultiKeyAuthority for tokens signed by multiple keys.",
                kid=token_kid,
            )

        decode_kwargs: dict[str, Any] = {"algorithms": [self._algorithm]}
        if audience is not None:
            decode_kwargs["audience"] = audience

        # _jwt.decode() raises specific exceptions on failure — let them
        # propagate unchanged so callers can handle ExpiredSignatureError,
        # InvalidSignatureError, etc. directly.
        raw = _jwt.decode(token_str, self._public_key, **decode_kwargs)

        # Reuse JwtParser's claim reconstruction — keeps AuthContext rebuilding
        # logic in one place rather than duplicating it here.
        return JwtParser._from_raw_claims(raw)

    # ── JWKS exposure ─────────────────────────────────────────────────────────

    def jwk(self) -> JsonWebKey:
        """
        Return the public ``JsonWebKey`` for this authority.

        Safe to expose externally — contains only public key material.

        Returns:
            Public ``JsonWebKey`` with ``kid``, ``alg``, ``use`` populated.
        """
        return self._public_jwk

    def jwks(self) -> JsonWebKeySet:
        """
        Return a ``JsonWebKeySet`` containing this authority's public key.

        Produces a single-key JWKS.  Use ``MultiKeyAuthority.jwks()`` to get
        a merged JWKS for all active + retired keys.

        Returns:
            ``JsonWebKeySet`` with exactly one public key.
        """
        return JwkBuilder.keyset(self._public_jwk)

    def __repr__(self) -> str:
        return (
            f"JwtAuthority("
            f"kid={self._kid!r}, "
            f"issuer={self._issuer!r}, "
            f"algorithm={self._algorithm!r})"
        )
