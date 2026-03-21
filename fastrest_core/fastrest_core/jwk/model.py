"""
fastrest_core.jwk.model
========================

``JsonWebKey`` and ``JsonWebKeySet`` — immutable RFC 7517 value objects.

Also exports the two base64url helpers (``_b64url_encode``, ``_int_to_b64url``)
used by ``JwkBuilder`` so the encoding logic lives in one place.

Sub-package consumers
---------------------
    JwkBuilder (builder.py) imports:
        _b64url_encode, _int_to_b64url
    jwk/__init__.py re-exports:
        JsonWebKey, JsonWebKeySet

Thread safety:  ✅ frozen=True — both dataclasses are immutable.
Async safety:   ✅ Pure value objects — no I/O.
"""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any


# ── Base64url helpers ──────────────────────────────────────────────────────────

# RFC 7515 §2: base64url encoding with no padding characters ("=").
# Used for all binary-to-string conversions in JWK fields and thumbprints.


def _b64url_encode(data: bytes) -> str:
    """
    Encode raw bytes to a base64url string with no trailing padding.

    RFC 7515 §2 requires base64url without ``"="`` padding for all JWK
    and JWT binary values.

    Args:
        data: Raw bytes to encode.

    Returns:
        ASCII base64url string with no trailing ``"="`` characters.

    Edge cases:
        - Empty bytes → empty string ``""``.
    """
    # rstrip removes trailing padding; decode converts bytes→str
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _int_to_b64url(n: int, *, byte_length: int | None = None) -> str:
    """
    Encode a non-negative integer to a base64url string.

    Used for RSA/EC key parameters which are unsigned big-endian integers.

    Args:
        n:           Non-negative integer to encode.
        byte_length: Fixed output byte length.  Pass this for EC coordinates,
                     which must be zero-padded to the curve's coordinate size
                     (32, 48, or 66 bytes for P-256/P-384/P-521).  When
                     ``None``, the natural minimum byte length is used —
                     correct for RSA moduli and exponents.

    Returns:
        Base64url string (no padding) representing the big-endian encoding.

    Raises:
        OverflowError: ``n`` requires more bytes than ``byte_length``.

    Edge cases:
        - ``n == 0`` with no ``byte_length`` → ``"AA"`` (1 zero byte).
        - EC coordinates: always pass ``byte_length`` — a coordinate whose
          high bit is 0 would otherwise lose a leading zero byte, producing
          an invalid JWK.
    """
    if byte_length is None:
        # Natural minimum: ceil(bit_length / 8), at least 1 byte for n=0
        byte_length = max(1, (n.bit_length() + 7) // 8)
    # to_bytes raises OverflowError if n doesn't fit — intentional; signals
    # a programming error (e.g. passing wrong byte_length for the curve)
    return _b64url_encode(n.to_bytes(byte_length, "big"))


# ── JsonWebKey ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class JsonWebKey:
    """
    Immutable RFC 7517 JSON Web Key value object.

    Supports RSA, EC, and oct (symmetric) key types.  Fields absent from the
    source key are ``None`` — ``to_dict()`` omits them automatically so the
    JSON output is always spec-compliant and compact.

    Private key parameters (``d``, ``p``, ``q``, ``dp``, ``dq``, ``qi``, ``k``)
    are excluded from equality checks and hashing — two keys that differ only
    in private material are considered the same public key.  This means
    ``JsonWebKey`` instances can safely be used as dict keys or set members
    without leaking private key identity.

    Attributes:
        kty:      Key Type — ``"RSA"``, ``"EC"``, or ``"oct"`` (required).
        kid:      Key ID — opaque identifier, often a thumbprint or UUID.
        use:      Public Key Use — ``"sig"`` (signature) or ``"enc"`` (encryption).
        alg:      Algorithm — e.g. ``"RS256"``, ``"ES256"``, ``"HS256"``.
        key_ops:  Key Operations — e.g. ``("sign",)``, ``("verify",)``.

        n, e:     RSA modulus and public exponent (base64url, public).
        d:        RSA private exponent or EC private scalar (base64url, private).
        p, q:     RSA CRT primes (base64url, private).
        dp, dq:   RSA CRT exponents (base64url, private).
        qi:       RSA CRT coefficient (base64url, private).

        crv:      EC curve name — ``"P-256"``, ``"P-384"``, ``"P-521"``.
        x, y:     EC public coordinates (base64url, public).

        k:        Symmetric key value for ``"oct"`` type (base64url, private).

    Thread safety:  ✅ frozen=True — immutable; safe to share across tasks.
    Async safety:   ✅ Pure value object — no I/O.

    Edge cases:
        - ``key_ops`` is ``tuple[str, ...]`` (hashable); ``to_dict()`` emits it
          as a ``list`` as required by RFC 7517.
        - Private fields (``d``, ``p``, ``q``, ``dp``, ``dq``, ``qi``, ``k``)
          are ``compare=False, hash=False`` — they do not influence ``__eq__``
          or ``__hash__``.  This mirrors the ``extra_claims`` pattern in
          ``JsonWebToken`` and ensures hashing never depends on secret material.
        - ``public_key()`` raises ``TypeError`` for ``oct`` keys — symmetric
          keys have no public equivalent and must not appear in public JWKS.

    Example::

        from fastrest_core.jwk import JwkBuilder

        with open("private.pem", "rb") as f:
            jwk = JwkBuilder.from_pem(f.read(), kid="my-key", use="sig", alg="RS256")

        keyset = JwkBuilder.keyset(jwk.public_key())
        # expose keyset.to_dict() at /.well-known/jwks.json
    """

    # ── RFC 7517 §4 common parameters ────────────────────────────────────────

    # Key Type — discriminates RSA / EC / oct; always required
    kty: str

    # Key ID — opaque label used to select a key during verification
    kid: str | None = None

    # Public Key Use — intent of the key: signature or encryption
    use: str | None = None

    # Algorithm — intended signing/encryption algorithm (e.g. "RS256", "ES256")
    alg: str | None = None

    # Key Operations — fine-grained capability list (RFC 7517 §4.3)
    # Stored as tuple for hashability; emitted as list in to_dict()
    key_ops: tuple[str, ...] | None = None

    # ── RFC 7518 §6.3.1 RSA public key parameters ────────────────────────────

    # Modulus — base64url big-endian unsigned integer
    n: str | None = None

    # Public exponent — typically base64url("AQAB") == 65537
    e: str | None = None

    # ── RFC 7518 §6.3.2 RSA private key parameters ───────────────────────────
    # All excluded from equality/hash — private material must not influence
    # identity comparisons (two RSAPublicKeys with the same n,e are equal
    # regardless of which private key they came from)

    # Private exponent (RSA) or private scalar (EC) — kept together since
    # RFC 7518 uses the same "d" field name for both key types
    d: str | None = field(default=None, compare=False, hash=False)

    # First CRT prime (p) — optional optimisation; included for full JWK export
    p: str | None = field(default=None, compare=False, hash=False)

    # Second CRT prime (q)
    q: str | None = field(default=None, compare=False, hash=False)

    # First factor CRT exponent (d mod p-1) — named "dmp1" in OpenSSL/cryptography
    dp: str | None = field(default=None, compare=False, hash=False)

    # Second factor CRT exponent (d mod q-1) — named "dmq1" in OpenSSL/cryptography
    dq: str | None = field(default=None, compare=False, hash=False)

    # First CRT coefficient (q^-1 mod p) — named "iqmp" in OpenSSL/cryptography
    qi: str | None = field(default=None, compare=False, hash=False)

    # ── RFC 7518 §6.2 EC public key parameters ────────────────────────────────

    # Curve name — "P-256", "P-384", "P-521"
    crv: str | None = None

    # Public x coordinate — zero-padded to curve byte size
    x: str | None = None

    # Public y coordinate — zero-padded to curve byte size
    y: str | None = None

    # ── RFC 7518 §6.4 oct (symmetric) key parameter ───────────────────────────

    # The symmetric key value — excluded from equality/hash (private material)
    k: str | None = field(default=None, compare=False, hash=False)

    # ── Serialization ─────────────────────────────────────────────────────────

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> JsonWebKey:
        """
        Construct a ``JsonWebKey`` from a raw RFC 7517 JWK dict.

        Inverse of ``to_dict()``.  Used when parsing remote JWKS responses
        or deserializing stored JWK JSON.  Unknown fields are silently ignored
        so the parser remains forward-compatible with future JWK extensions.

        Args:
            raw: A ``dict`` as returned by ``json.loads()`` on a single JWK
                 object.  Must contain at least ``"kty"``.

        Returns:
            A ``JsonWebKey`` with all recognised fields populated.  Fields
            absent from ``raw`` default to ``None``.

        Raises:
            KeyError:  ``"kty"`` is missing — not a valid JWK object.

        Edge cases:
            - ``key_ops`` arrives as ``list`` in JSON; converted to ``tuple``
              for hashability.  ``None`` when the field is absent.
            - Extra fields not part of RFC 7517 are silently dropped —
              forward compatibility for future JWK extensions.
            - Private fields (``d``, ``p``, etc.) are populated when present
              in ``raw`` — caller is responsible for not passing private JWKs
              to public endpoints.

        Example::

            with urllib.request.urlopen(jwks_url) as resp:
                data = json.loads(resp.read())
            keys = [JsonWebKey.from_dict(k) for k in data["keys"]]
        """
        # key_ops is a JSON array — must be tuple for frozen dataclass hashability.
        # All other fields map 1:1 by name; absent fields default to None naturally.
        raw_key_ops = raw.get("key_ops")
        key_ops: tuple[str, ...] | None = (
            tuple(raw_key_ops) if raw_key_ops is not None else None
        )

        return cls(
            kty=raw["kty"],  # required — raises KeyError when missing
            kid=raw.get("kid"),
            use=raw.get("use"),
            alg=raw.get("alg"),
            key_ops=key_ops,
            # RSA public
            n=raw.get("n"),
            e=raw.get("e"),
            # RSA/EC private — populated only when present in raw
            d=raw.get("d"),
            p=raw.get("p"),
            q=raw.get("q"),
            dp=raw.get("dp"),
            dq=raw.get("dq"),
            qi=raw.get("qi"),
            # EC
            crv=raw.get("crv"),
            x=raw.get("x"),
            y=raw.get("y"),
            # oct
            k=raw.get("k"),
        )

    def to_dict(self) -> dict[str, Any]:
        """
        Serialize to a RFC 7517 JSON-serializable dict.

        Omits all ``None`` fields to keep the output compact and spec-compliant
        (RFC 7517 §4 says optional members should be omitted when absent).
        Converts ``key_ops`` tuple to a list for JSON compatibility.

        Returns:
            Dict with only the non-``None`` JWK fields populated.

        Edge cases:
            - ``key_ops`` tuple → list (JSON arrays must not be tuples).
            - ``None`` fields are completely absent — not present as ``null``.
            - Order is not guaranteed (callers relying on order should sort).
        """
        # Build a flat map of all possible JWK fields in RFC 7517 declaration
        # order, then filter out None entries in one pass — cleaner than
        # a long chain of if-statements and easier to extend with new fields.
        _MISSING = object()  # sentinel distinct from None

        raw: dict[str, Any] = {
            "kty": self.kty,  # always present — never None
            "kid": self.kid,
            "use": self.use,
            "alg": self.alg,
            "key_ops": list(self.key_ops) if self.key_ops is not None else _MISSING,
            # RSA public
            "n": self.n,
            "e": self.e,
            # RSA private
            "d": self.d,
            "p": self.p,
            "q": self.q,
            "dp": self.dp,
            "dq": self.dq,
            "qi": self.qi,
            # EC
            "crv": self.crv,
            "x": self.x,
            "y": self.y,
            # oct
            "k": self.k,
        }
        # Filter: keep kty unconditionally, skip anything that is None or
        # the _MISSING sentinel (used for key_ops to distinguish None from absent)
        return {k: v for k, v in raw.items() if v is not None and v is not _MISSING}

    # ── RFC 7638 thumbprint ────────────────────────────────────────────────────

    def thumbprint(self) -> str:
        """
        Compute the RFC 7638 JWK thumbprint (SHA-256, base64url, no padding).

        The thumbprint is a stable, algorithm-independent identifier for a key,
        computed from the *required* members for the key type in lexicographic
        order with no whitespace.  Useful as a ``kid`` value that is derived
        from key material rather than assigned externally.

        Returns:
            Base64url-encoded SHA-256 digest of the canonical JSON.

        Raises:
            ValueError: ``kty`` is not ``"RSA"``, ``"EC"``, or ``"oct"``.
            ValueError: A required field for the ``kty`` is ``None`` —
                        the key is malformed (e.g. an EC key with no ``crv``).

        Edge cases:
            - RFC 7638 §3.2 requires *only* the required members — optional
              fields (``kid``, ``use``, ``alg``) are intentionally excluded.
            - Key order in the canonical JSON must be lexicographic — Python
              ``json.dumps(..., sort_keys=True)`` satisfies this.

        Example::

            jwk = JwkBuilder.from_rsa_public_key(pub_key, kid="x", use="sig", alg="RS256")
            kid = jwk.thumbprint()   # deterministic, reproducible from key material
        """
        # RFC 7638 §3.2 — required-member sets per key type:
        #   RSA: {e, kty, n}
        #   EC:  {crv, kty, x, y}
        #   oct: {k, kty}
        if self.kty == "RSA":
            if self.n is None or self.e is None:
                raise ValueError(
                    "Cannot compute thumbprint: RSA key is missing 'n' or 'e'. "
                    "Ensure the key was built with JwkBuilder.from_rsa_*()."
                )
            required: dict[str, str] = {"e": self.e, "kty": self.kty, "n": self.n}

        elif self.kty == "EC":
            if self.crv is None or self.x is None or self.y is None:
                raise ValueError(
                    "Cannot compute thumbprint: EC key is missing 'crv', 'x', or 'y'. "
                    "Ensure the key was built with JwkBuilder.from_ec_*()."
                )
            required = {
                "crv": self.crv,
                "kty": self.kty,
                "x": self.x,
                "y": self.y,
            }

        elif self.kty == "oct":
            if self.k is None:
                raise ValueError(
                    "Cannot compute thumbprint: oct key is missing 'k'. "
                    "Ensure the key was built with a valid symmetric key value."
                )
            required = {"k": self.k, "kty": self.kty}

        else:
            raise ValueError(
                f"Cannot compute thumbprint for unsupported kty={self.kty!r}. "
                f"Supported key types: 'RSA', 'EC', 'oct'."
            )

        # Canonical JSON: sort_keys=True ensures lexicographic member order;
        # separators=(",", ":") eliminates all whitespace per RFC 7638 §3.2
        canonical = json.dumps(required, separators=(",", ":"), sort_keys=True)
        digest = hashlib.sha256(canonical.encode("ascii")).digest()
        return _b64url_encode(digest)

    # ── Public key extraction ──────────────────────────────────────────────────

    def public_key(self) -> JsonWebKey:
        """
        Return a new ``JsonWebKey`` with all private key material stripped.

        Creates a new frozen instance with ``d``, ``p``, ``q``, ``dp``, ``dq``,
        ``qi`` set to ``None`` (RSA private params and EC private scalar).
        The public parameters (``n``, ``e`` for RSA; ``crv``, ``x``, ``y`` for
        EC) are preserved unchanged.

        Returns:
            A new ``JsonWebKey`` carrying only public parameters.

        Raises:
            TypeError: The key is of type ``"oct"`` (symmetric).  Symmetric keys
                       have no public equivalent — the key value IS the secret,
                       and returning ``self`` would silently expose private
                       material in a context expecting only public keys.

        Edge cases:
            - Calling ``public_key()`` on an already-public JWK is a no-op —
              the private fields are already ``None``, so the result is equal
              to ``self`` (modulo frozen identity).
            - ``kid``, ``use``, ``alg``, ``key_ops`` are preserved unchanged.
        """
        if self.kty == "oct":
            # Symmetric keys have no meaningful "public" form.
            # Returning self would be a security footgun — callers must
            # explicitly exclude oct keys before calling public_set().
            raise TypeError(
                "Symmetric ('oct') keys have no public equivalent. "
                "The key value IS the secret — it cannot be stripped. "
                "Use JsonWebKeySet.public_set() which excludes oct keys automatically."
            )

        # dataclasses.replace() on a frozen dataclass returns a new instance —
        # frozen=True does not prevent replace(); it only blocks direct mutation.
        return dataclasses.replace(
            self,
            d=None,  # RSA private exponent / EC private scalar
            p=None,  # RSA CRT prime
            q=None,  # RSA CRT prime
            dp=None,  # RSA CRT exponent
            dq=None,  # RSA CRT exponent
            qi=None,  # RSA CRT coefficient
            # k is already None for RSA/EC; left unset to avoid accidents
        )

    def __repr__(self) -> str:
        return (
            f"JsonWebKey("
            f"kty={self.kty!r}, "
            f"kid={self.kid!r}, "
            f"alg={self.alg!r}, "
            f"use={self.use!r}, "
            f"has_private={self.d is not None or self.k is not None})"
        )


# ── JsonWebKeySet ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class JsonWebKeySet:
    """
    Immutable RFC 7517 JSON Web Key Set value object.

    Wraps an ordered tuple of ``JsonWebKey`` instances and produces the
    ``{"keys": [...]}`` JSON structure expected by JWKS endpoints.

    Attributes:
        keys: Ordered tuple of ``JsonWebKey`` entries.  Order is preserved
              in ``to_dict()`` output — callers may rely on insertion order.

    Thread safety:  ✅ frozen=True — immutable; safe to share across tasks.
    Async safety:   ✅ Pure value object — no I/O.

    Edge cases:
        - An empty ``keys`` tuple is valid — produces ``{"keys": []}``.
        - Duplicate ``kid`` values are not validated — callers are responsible
          for ensuring uniqueness.  ``find_by_kid()`` returns the first match.
        - ``public_set()`` silently drops ``"oct"`` keys — callers relying on
          a symmetric key being present must check before calling.

    Example::

        keyset = JwkBuilder.keyset(rsa_jwk, ec_jwk)
        response_body = keyset.public_set().to_dict()
        # → {"keys": [{...rsa_public...}, {...ec_public...}]}
    """

    # Ordered key list — tuple preserves insertion order and is hashable
    keys: tuple[JsonWebKey, ...]

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_dict(self) -> dict[str, list[dict[str, Any]]]:
        """
        Serialize to the RFC 7517 JWKS JSON structure.

        Returns:
            ``{"keys": [key.to_dict(), ...]}`` — JSON-serializable.

        Edge cases:
            - Empty ``keys`` → ``{"keys": []}``.
            - Each key's private fields are included if present —
              call ``public_set()`` first to strip private material.
        """
        return {"keys": [k.to_dict() for k in self.keys]}

    # ── Key lookup ────────────────────────────────────────────────────────────

    def find_by_kid(self, kid: str) -> JsonWebKey | None:
        """
        Find a key by its ``kid`` (Key ID) claim.

        Linear scan — adequate for the typical JWKS size (1–10 keys).
        Call once during request handling and cache the result if the
        same ``kid`` is looked up repeatedly in a hot path.

        Args:
            kid: Key ID to look up.  Case-sensitive exact match.

        Returns:
            The first ``JsonWebKey`` whose ``kid`` equals the argument,
            or ``None`` when no match is found.

        Edge cases:
            - Returns ``None`` when ``keys`` is empty.
            - Returns the first match if multiple keys share the same ``kid``
              (ambiguous configuration — callers should ensure uniqueness).
            - Keys with ``kid=None`` never match.
        """
        # next() with a default avoids materialising a full list — short-circuits
        # on the first match, which is the common case.
        return next(
            (k for k in self.keys if k.kid == kid),
            None,
        )

    # ── Public view ───────────────────────────────────────────────────────────

    def public_set(self) -> JsonWebKeySet:
        """
        Return a new ``JsonWebKeySet`` with all private key material stripped.

        For each non-oct key, calls ``JsonWebKey.public_key()`` to remove
        private parameters.  Symmetric (``"oct"``) keys are excluded entirely
        because they have no public equivalent — returning them would expose
        secret material.

        Returns:
            A new ``JsonWebKeySet`` containing only public key parameters.
            ``"oct"`` keys are absent from the result.

        Edge cases:
            - A keyset containing only ``"oct"`` keys → ``JsonWebKeySet(keys=())``.
            - Already-public keys pass through unchanged (no-op strip).
            - ``kid``, ``use``, ``alg``, ``key_ops`` are preserved on each key.

        Example::

            # Safe to expose to external callers:
            public_jwks = keyset.public_set().to_dict()
        """
        # Filter out oct keys first — they cannot produce a public equivalent.
        # Then strip private params from each remaining key.
        public_keys = tuple(k.public_key() for k in self.keys if k.kty != "oct")
        return JsonWebKeySet(keys=public_keys)

    def __repr__(self) -> str:
        return (
            f"JsonWebKeySet("
            f"count={len(self.keys)}, "
            f"kids={[k.kid for k in self.keys]!r})"
        )
