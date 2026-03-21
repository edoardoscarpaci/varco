"""
fastrest_core.jwk.builder
==========================

``JwkBuilder`` — stateless factory for constructing ``JsonWebKey`` and
``JsonWebKeySet`` instances from cryptographic key objects or PEM bytes.

DESIGN: classmethods over instance methods
    ✅ Stateless — mirrors JwtParser's classmethod style for symmetry.
    ✅ No builder state to manage; each from_* call is a pure transformation.
    ✅ Subclassable: override ``_ec_curve_name`` for custom curve support.
    ❌ Cannot chain setters like JwtBuilder — unnecessary here since JWK
       parameters are derived fully from the key object, not assembled piece
       by piece.

Thread safety:  ✅ Stateless — all methods are classmethods.
Async safety:   ✅ No async operations; safe to call inside async contexts.
"""

from __future__ import annotations

from dataclasses import replace as _dataclass_replace
from typing import TYPE_CHECKING

from fastrest_core.jwk.model import JsonWebKey, JsonWebKeySet, _int_to_b64url

# Cryptography imports — use TYPE_CHECKING for the abstract base types so that
# this module can still be imported even on systems where cryptography is not
# installed (though at runtime the from_* calls will obviously fail).
# In practice, fastrest-core declares cryptography as a hard dependency, so
# TYPE_CHECKING vs runtime matters only for type-checker performance.
if TYPE_CHECKING:
    from cryptography.hazmat.primitives.asymmetric.ec import (
        EllipticCurvePrivateKey,
        EllipticCurvePublicKey,
    )
    from cryptography.hazmat.primitives.asymmetric.rsa import (
        RSAPrivateKey,
        RSAPublicKey,
    )


# ── EC curve metadata ──────────────────────────────────────────────────────────

# Mapping from cryptography's internal curve name (OpenSSL convention) to
# the RFC 7518 §6.2.1.1 "crv" string and the coordinate byte size.
# Coordinate byte size = ceil(key_size / 8) — x and y must be zero-padded
# to exactly this length; using bit_length() alone would drop leading zeros.
_EC_CURVE_INFO: dict[str, tuple[str, int]] = {
    # secp256r1 is the NIST P-256 curve — most common for ES256
    "secp256r1": ("P-256", 32),
    # secp384r1 is the NIST P-384 curve — used for ES384
    "secp384r1": ("P-384", 48),
    # secp521r1 is the NIST P-521 curve — 66 bytes (521 bits → 66 bytes)
    "secp521r1": ("P-521", 66),
}


# ── JwkBuilder ────────────────────────────────────────────────────────────────


class JwkBuilder:
    """
    Stateless factory for ``JsonWebKey`` and ``JsonWebKeySet``.

    All methods are classmethods — no instance needs to be created.  Each
    ``from_*`` method accepts a cryptographic key object and returns a fully
    populated ``JsonWebKey``.

    Usage pattern mirrors ``JwtParser`` — the caller constructs a key from
    whatever source they have (PEM file, generated key object, HSM), passes it
    to the appropriate factory, and receives an immutable value object.

    Thread safety:  ✅ Stateless — all methods are classmethods.
    Async safety:   ✅ No async operations; safe to call inside async contexts.

    Example::

        import fastrest_core.jwk as jwk

        # Load an RSA private key from disk and expose its public JWK:
        with open("rsa_private.pem", "rb") as f:
            key = jwk.JwkBuilder.from_pem(
                f.read(),
                kid="auth-2025",
                use="sig",
                alg="RS256",
            )

        keyset = jwk.JwkBuilder.keyset(key.public_key())
        # Serve keyset.public_set().to_dict() at /.well-known/jwks.json
    """

    # ── RSA factories ─────────────────────────────────────────────────────────

    @classmethod
    def from_rsa_public_key(
        cls,
        key: RSAPublicKey,
        *,
        kid: str,
        use: str,
        alg: str,
    ) -> JsonWebKey:
        """
        Construct a ``JsonWebKey`` from an RSA public key object.

        Extracts the public modulus (``n``) and exponent (``e``) from the
        key's public numbers and encodes them as base64url strings per RFC 7518
        §6.3.1.

        Args:
            key: An RSA public key object from the ``cryptography`` library
                 (e.g. ``rsa.generate_private_key(...).public_key()``).
            kid: Key ID — used by verifiers to select the correct key.
                 Use ``JsonWebKey.thumbprint()`` for a self-derived ``kid``.
            use: Public Key Use — ``"sig"`` for signature keys (typical) or
                 ``"enc"`` for encryption keys.
            alg: Algorithm — e.g. ``"RS256"``, ``"RS384"``, ``"RS512"``
                 or ``"PS256"`` / ``"PS384"`` / ``"PS512"`` for RSASSA-PSS.

        Returns:
            ``JsonWebKey`` with ``kty="RSA"``, ``n``, ``e``, ``kid``, ``use``,
            ``alg`` populated; all private fields are ``None``.

        Raises:
            TypeError: ``key`` is not an RSA public key instance.

        Edge cases:
            - ``n`` byte length varies with key size (2048-bit → 256 bytes).
            - ``e`` is almost always 65537, encoded as ``"AQAB"`` (3 bytes).
        """
        # Importing inside the method defers the ImportError to call time,
        # giving a clear error if cryptography is not installed rather than
        # an obscure AttributeError elsewhere.
        from cryptography.hazmat.primitives.asymmetric.rsa import (
            RSAPublicKey as _RSAPublicKey,
        )

        if not isinstance(key, _RSAPublicKey):
            raise TypeError(
                f"from_rsa_public_key expects an RSAPublicKey, got {type(key).__name__!r}. "
                f"Call .public_key() on an RSAPrivateKey first if needed."
            )

        nums = key.public_numbers()

        return JsonWebKey(
            kty="RSA",
            kid=kid,
            use=use,
            alg=alg,
            # Natural byte-length encoding — RSA moduli have no fixed size
            n=_int_to_b64url(nums.n),
            e=_int_to_b64url(nums.e),
        )

    @classmethod
    def from_rsa_private_key(
        cls,
        key: RSAPrivateKey,
        *,
        kid: str,
        use: str,
        alg: str,
        include_private: bool = False,
    ) -> JsonWebKey:
        """
        Construct a ``JsonWebKey`` from an RSA private key object.

        By default returns only the public JWK (safe for JWKS endpoints).
        Pass ``include_private=True`` to include all CRT parameters — useful
        for serialising a key to storage or for key exchange scenarios that
        require the full private JWK.

        Args:
            key:             An RSA private key object.
            kid:             Key ID.
            use:             Public Key Use — ``"sig"`` or ``"enc"``.
            alg:             Algorithm — e.g. ``"RS256"``.
            include_private: When ``True``, include ``d``, ``p``, ``q``,
                             ``dp``, ``dq``, ``qi``.  Defaults to ``False``
                             (public key only) — the safe default.

        Returns:
            ``JsonWebKey`` with public params always populated.  Private params
            (``d``, ``p``, ``q``, ``dp``, ``dq``, ``qi``) populated only when
            ``include_private=True``.

        Raises:
            TypeError: ``key`` is not an RSA private key instance.

        Edge cases:
            - CRT param naming: ``cryptography`` uses ``dmp1``/``dmq1``/``iqmp``
              (OpenSSL convention); RFC 7518 uses ``dp``/``dq``/``qi``.
              The mapping is explicit here to prevent silent mismatch.
            - Even with ``include_private=True``, callers must never expose the
              result via a public JWKS endpoint.  Use ``JsonWebKey.public_key()``
              or ``JsonWebKeySet.public_set()`` before exposing externally.
        """
        from cryptography.hazmat.primitives.asymmetric.rsa import (
            RSAPrivateKey as _RSAPrivateKey,
        )

        if not isinstance(key, _RSAPrivateKey):
            raise TypeError(
                f"from_rsa_private_key expects an RSAPrivateKey, got {type(key).__name__!r}."
            )

        if not include_private:
            # Delegate to public-key path — avoids duplicating public-param logic
            return cls.from_rsa_public_key(key.public_key(), kid=kid, use=use, alg=alg)

        priv = key.private_numbers()
        pub = priv.public_numbers

        # Explicit OpenSSL→RFC name mapping:
        #   dmp1 → dp  (d mod (p-1))
        #   dmq1 → dq  (d mod (q-1))
        #   iqmp → qi  (q^-1 mod p)
        return JsonWebKey(
            kty="RSA",
            kid=kid,
            use=use,
            alg=alg,
            n=_int_to_b64url(pub.n),
            e=_int_to_b64url(pub.e),
            d=_int_to_b64url(priv.d),
            p=_int_to_b64url(priv.p),
            q=_int_to_b64url(priv.q),
            dp=_int_to_b64url(priv.dmp1),  # OpenSSL name → JWK name
            dq=_int_to_b64url(priv.dmq1),  # OpenSSL name → JWK name
            qi=_int_to_b64url(priv.iqmp),  # OpenSSL name → JWK name
        )

    # ── EC factories ──────────────────────────────────────────────────────────

    @classmethod
    def from_ec_public_key(
        cls,
        key: EllipticCurvePublicKey,
        *,
        kid: str,
        use: str,
        alg: str,
    ) -> JsonWebKey:
        """
        Construct a ``JsonWebKey`` from an EC public key object.

        Extracts the curve name and public coordinates (``x``, ``y``) from the
        key.  Coordinates are zero-padded to the curve's fixed coordinate byte
        size (RFC 7518 §6.2.1.2) — this is distinct from RSA where natural
        byte length is used.

        Args:
            key: An EC public key object from the ``cryptography`` library.
            kid: Key ID.
            use: Public Key Use — ``"sig"`` or ``"enc"``.
            alg: Algorithm — e.g. ``"ES256"``, ``"ES384"``, ``"ES512"``.

        Returns:
            ``JsonWebKey`` with ``kty="EC"``, ``crv``, ``x``, ``y``,
            ``kid``, ``use``, ``alg`` populated; ``d`` is ``None``.

        Raises:
            TypeError:  ``key`` is not an EC public key instance.
            ValueError: The key's curve is not P-256, P-384, or P-521.

        Edge cases:
            - **Zero-padding is critical**: ``x`` and ``y`` must be padded to
              ``(key.key_size + 7) // 8`` bytes, not their natural bit-length.
              A P-256 coordinate of value 1 must encode as 32 zero bytes + ``0x01``,
              not just ``0x01`` — using bit_length() would produce a 1-byte JWK
              that decodes to a completely different public key.
        """
        from cryptography.hazmat.primitives.asymmetric.ec import (
            EllipticCurvePublicKey as _ECPublicKey,
        )

        if not isinstance(key, _ECPublicKey):
            raise TypeError(
                f"from_ec_public_key expects an EllipticCurvePublicKey, got {type(key).__name__!r}."
            )

        crv, coord_size = cls._ec_curve_info(key.curve)
        nums = key.public_numbers()

        # Fixed-size encoding for EC coordinates — see Edge cases above
        x = _int_to_b64url(nums.x, byte_length=coord_size)
        y = _int_to_b64url(nums.y, byte_length=coord_size)

        return JsonWebKey(
            kty="EC",
            kid=kid,
            use=use,
            alg=alg,
            crv=crv,
            x=x,
            y=y,
        )

    @classmethod
    def from_ec_private_key(
        cls,
        key: EllipticCurvePrivateKey,
        *,
        kid: str,
        use: str,
        alg: str,
        include_private: bool = False,
    ) -> JsonWebKey:
        """
        Construct a ``JsonWebKey`` from an EC private key object.

        Args:
            key:             An EC private key object.
            kid:             Key ID.
            use:             Public Key Use — ``"sig"`` or ``"enc"``.
            alg:             Algorithm — e.g. ``"ES256"``.
            include_private: When ``True``, include the private scalar ``d``.
                             Defaults to ``False`` (public key only).

        Returns:
            ``JsonWebKey`` with EC public params always populated.  ``d`` is
            populated only when ``include_private=True``.

        Raises:
            TypeError:  ``key`` is not an EC private key instance.
            ValueError: The key's curve is unsupported.

        Edge cases:
            - The private scalar ``d`` is also zero-padded to the curve's
              coordinate size (same as ``x`` and ``y``) per RFC 7518 §6.2.2.1.
        """
        from cryptography.hazmat.primitives.asymmetric.ec import (
            EllipticCurvePrivateKey as _ECPrivateKey,
        )

        if not isinstance(key, _ECPrivateKey):
            raise TypeError(
                f"from_ec_private_key expects an EllipticCurvePrivateKey, got {type(key).__name__!r}."
            )

        if not include_private:
            return cls.from_ec_public_key(key.public_key(), kid=kid, use=use, alg=alg)

        priv = key.private_numbers()
        _, coord_size = cls._ec_curve_info(key.curve)

        # Build the public portion first, then add the private scalar d
        pub_jwk = cls.from_ec_public_key(key.public_key(), kid=kid, use=use, alg=alg)

        # d must be padded to coord_size — same rule as x and y
        return _dataclass_replace(
            pub_jwk, d=_int_to_b64url(priv.private_value, byte_length=coord_size)
        )

    # ── PEM factory (auto-detect) ──────────────────────────────────────────────

    @classmethod
    def from_pem(
        cls,
        pem_bytes: bytes,
        *,
        kid: str,
        use: str,
        alg: str,
        password: bytes | None = None,
        include_private: bool = False,
    ) -> JsonWebKey:
        """
        Construct a ``JsonWebKey`` from PEM-encoded key bytes.

        Auto-detects whether the PEM contains a private or public key, and
        whether it is RSA or EC.  Internally delegates to the appropriate
        ``from_rsa_*`` or ``from_ec_*`` classmethod.

        Args:
            pem_bytes:       PEM-encoded bytes.  Accepts both private
                             (PKCS#8 ``BEGIN PRIVATE KEY`` or
                             ``BEGIN RSA/EC PRIVATE KEY``) and public
                             (``BEGIN PUBLIC KEY``) formats.
            kid:             Key ID.
            use:             Public Key Use — ``"sig"`` or ``"enc"``.
            alg:             Algorithm — e.g. ``"RS256"``, ``"ES256"``.
            password:        Passphrase for encrypted PEM files.  ``None``
                             for unencrypted keys.  Only used when loading
                             private keys — public PEM files are never
                             encrypted.
            include_private: Whether to include private key material in the
                             output JWK.  Defaults to ``False`` (public only).
                             Irrelevant when the PEM contains only a public key.

        Returns:
            ``JsonWebKey`` for the given key material.

        Raises:
            ValueError:     PEM bytes are not a recognised RSA or EC key, or
                            the key type is not supported.
            TypeError:      ``pem_bytes`` is not bytes.

        Edge cases:
            - A PKCS#8 private key PEM with ``include_private=False`` returns
              the same JWK as loading the corresponding public PEM directly.
            - ``password`` is silently ignored when the PEM is a public key
              (public keys are never encrypted).
            - Malformed PEM raises ``ValueError`` from the ``cryptography``
              library; this propagates unchanged for debuggability.
        """
        from cryptography.hazmat.primitives.asymmetric.ec import (
            EllipticCurvePrivateKey as _ECPrivKey,
            EllipticCurvePublicKey as _ECPubKey,
        )
        from cryptography.hazmat.primitives.asymmetric.rsa import (
            RSAPrivateKey as _RSAPrivKey,
            RSAPublicKey as _RSAPubKey,
        )
        from cryptography.hazmat.primitives.serialization import (
            load_pem_private_key,
            load_pem_public_key,
        )

        # Try private key first — a private PEM always starts with BEGIN *PRIVATE KEY.
        # load_pem_private_key raises ValueError when the PEM is a public key.
        key: object
        is_private: bool

        try:
            key = load_pem_private_key(pem_bytes, password=password)
            is_private = True
        except (ValueError, TypeError):
            # ValueError: PEM is not a private key (e.g. it is a public key)
            # TypeError:  password arg was rejected (public keys don't take passwords)
            key = load_pem_public_key(pem_bytes)
            is_private = False

        # Dispatch on key type — RSA and EC are the only supported asymmetric types
        if isinstance(key, _RSAPrivKey):
            return cls.from_rsa_private_key(
                key,
                kid=kid,
                use=use,
                alg=alg,
                include_private=is_private and include_private,
            )
        if isinstance(key, _RSAPubKey):
            return cls.from_rsa_public_key(key, kid=kid, use=use, alg=alg)
        if isinstance(key, _ECPrivKey):
            return cls.from_ec_private_key(
                key,
                kid=kid,
                use=use,
                alg=alg,
                include_private=is_private and include_private,
            )
        if isinstance(key, _ECPubKey):
            return cls.from_ec_public_key(key, kid=kid, use=use, alg=alg)

        raise ValueError(
            f"Unsupported key type {type(key).__name__!r} in PEM. "
            f"Supported types: RSA and EC (P-256, P-384, P-521)."
        )

    # ── Keyset factory ────────────────────────────────────────────────────────

    @classmethod
    def keyset(cls, *keys: JsonWebKey) -> JsonWebKeySet:
        """
        Wrap one or more ``JsonWebKey`` instances into a ``JsonWebKeySet``.

        Args:
            *keys: Zero or more ``JsonWebKey`` instances.  Order is preserved
                   in the resulting JWKS.

        Returns:
            ``JsonWebKeySet`` containing all provided keys.

        Edge cases:
            - Calling with no arguments produces an empty ``JsonWebKeySet``.
            - Duplicate ``kid`` values are not validated — callers are
              responsible for uniqueness.

        Example::

            keyset = JwkBuilder.keyset(rsa_jwk, ec_jwk)
        """
        return JsonWebKeySet(keys=keys)

    # ── Internal helpers ──────────────────────────────────────────────────────

    @classmethod
    def _ec_curve_info(cls, curve: object) -> tuple[str, int]:
        """
        Map a ``cryptography`` curve object to its RFC 7518 name and coordinate
        byte size.

        Args:
            curve: A ``cryptography`` EC curve instance
                   (e.g. ``SECP256R1()``).

        Returns:
            Tuple of ``(crv_name, coord_byte_size)``.

        Raises:
            ValueError: The curve is not P-256, P-384, or P-521.

        Edge cases:
            - Override this classmethod in a subclass to add support for
              additional curves (e.g. brainpoolP256r1) without forking
              the builder.
        """
        # Use the curve's .name attribute (lowercase OpenSSL convention)
        # rather than isinstance() to avoid importing all curve classes —
        # this is also the approach used by cryptography internally.
        curve_name: str = getattr(curve, "name", "")
        info = _EC_CURVE_INFO.get(curve_name)

        if info is None:
            supported = sorted(_EC_CURVE_INFO.keys())
            raise ValueError(
                f"Unsupported EC curve {curve_name!r}. "
                f"Supported curves: {supported}. "
                f"Override JwkBuilder._ec_curve_info() to add custom curve support."
            )

        return info
