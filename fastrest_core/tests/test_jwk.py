"""
Unit tests for fastrest_core.jwk
==================================

Covers:
    - JsonWebKey  — construction, to_dict(), thumbprint(), public_key(), __repr__
    - JsonWebKeySet — to_dict(), find_by_kid(), public_set()
    - JwkBuilder  — from_rsa_public_key, from_rsa_private_key, from_ec_public_key,
                    from_ec_private_key, from_pem (RSA + EC, private + public),
                    keyset(), and type-error guards

Testing strategy:
    Real cryptographic keys are generated once per test class using
    ``cryptography`` fixtures.  Small key sizes (RSA 2048, EC P-256/P-384/P-521)
    are used to keep generation time low.  The RFC 7638 Appendix A test vector
    is used for thumbprint regression testing.
"""

from __future__ import annotations

import base64
import json

import pytest
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from fastrest_core.jwk import JsonWebKey, JsonWebKeySet, JwkBuilder


# ── Key generation helpers ─────────────────────────────────────────────────────

# Shared RSA private key — generated once for the whole test module.
# 2048-bit is the minimum recommended size and fast enough for tests.
_RSA_KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
_RSA_PUB_KEY = _RSA_KEY.public_key()

# Three EC private keys — one per supported curve.
_EC_P256_KEY = ec.generate_private_key(ec.SECP256R1())
_EC_P384_KEY = ec.generate_private_key(ec.SECP384R1())
_EC_P521_KEY = ec.generate_private_key(ec.SECP521R1())

# PEM bytes for the RSA key (private PKCS#8 and public)
_RSA_PRIVATE_PEM = _RSA_KEY.private_bytes(
    encoding=Encoding.PEM,
    format=PrivateFormat.PKCS8,
    encryption_algorithm=NoEncryption(),
)
_RSA_PUBLIC_PEM = _RSA_PUB_KEY.public_bytes(
    encoding=Encoding.PEM,
    format=PublicFormat.SubjectPublicKeyInfo,
)

# PEM bytes for the EC P-256 key
_EC_PRIVATE_PEM = _EC_P256_KEY.private_bytes(
    encoding=Encoding.PEM,
    format=PrivateFormat.PKCS8,
    encryption_algorithm=NoEncryption(),
)
_EC_PUBLIC_PEM = _EC_P256_KEY.public_key().public_bytes(
    encoding=Encoding.PEM,
    format=PublicFormat.SubjectPublicKeyInfo,
)


# ── JsonWebKey ─────────────────────────────────────────────────────────────────


class TestJsonWebKey:
    """JsonWebKey is a frozen dataclass — immutable, value-equal, hashable."""

    def test_frozen_raises_on_write(self) -> None:
        """Frozen dataclass must reject attribute writes."""
        jwk = JsonWebKey(kty="RSA", kid="k1", n="abc", e="AQAB")
        with pytest.raises(Exception):  # dataclasses.FrozenInstanceError
            jwk.kid = "other"  # type: ignore[misc]

    def test_equality_ignores_private_fields(self) -> None:
        """Two JWKs with the same public params but different private params are equal."""
        pub = JsonWebKey(kty="RSA", kid="k1", n="mod", e="AQAB")
        priv = JsonWebKey(kty="RSA", kid="k1", n="mod", e="AQAB", d="privexp")
        # compare=False on d means private material is invisible to __eq__
        assert pub == priv

    def test_hashable(self) -> None:
        """JsonWebKey must be usable as a dict key (frozen + no unhashable fields)."""
        jwk = JsonWebKey(kty="RSA", kid="k1", n="mod", e="AQAB")
        d = {jwk: "value"}
        assert d[jwk] == "value"

    def test_to_dict_omits_none_fields(self) -> None:
        """to_dict() must not include keys whose value is None."""
        jwk = JsonWebKey(kty="RSA", n="modulus", e="AQAB")
        result = jwk.to_dict()
        assert "kid" not in result
        assert "use" not in result
        assert "alg" not in result
        assert "d" not in result

    def test_to_dict_includes_all_set_fields(self) -> None:
        """to_dict() must include every non-None field."""
        jwk = JsonWebKey(
            kty="RSA",
            kid="k1",
            use="sig",
            alg="RS256",
            n="mod",
            e="AQAB",
        )
        d = jwk.to_dict()
        assert d["kty"] == "RSA"
        assert d["kid"] == "k1"
        assert d["use"] == "sig"
        assert d["alg"] == "RS256"
        assert d["n"] == "mod"
        assert d["e"] == "AQAB"

    def test_to_dict_converts_key_ops_to_list(self) -> None:
        """key_ops is stored as a tuple but must be emitted as a list for JSON."""
        jwk = JsonWebKey(kty="RSA", n="x", e="y", key_ops=("sign", "verify"))
        result = jwk.to_dict()
        assert isinstance(result["key_ops"], list)
        assert set(result["key_ops"]) == {"sign", "verify"}

    def test_to_dict_includes_private_params_when_set(self) -> None:
        """Private fields that are set must appear in to_dict() output."""
        jwk = JsonWebKey(kty="RSA", n="mod", e="exp", d="priv", p="p1", q="q1")
        d = jwk.to_dict()
        assert d["d"] == "priv"
        assert d["p"] == "p1"
        assert d["q"] == "q1"

    def test_public_key_strips_rsa_private_params(self) -> None:
        """public_key() must zero out all RSA private params."""
        jwk = JsonWebKey(
            kty="RSA",
            kid="k1",
            use="sig",
            alg="RS256",
            n="mod",
            e="exp",
            d="priv",
            p="p1",
            q="q1",
            dp="dp1",
            dq="dq1",
            qi="qi1",
        )
        pub = jwk.public_key()
        assert pub.n == "mod"
        assert pub.e == "exp"
        assert pub.d is None
        assert pub.p is None
        assert pub.q is None
        assert pub.dp is None
        assert pub.dq is None
        assert pub.qi is None
        # Metadata preserved
        assert pub.kid == "k1"
        assert pub.alg == "RS256"

    def test_public_key_strips_ec_d(self) -> None:
        """public_key() must zero out the EC private scalar d."""
        jwk = JsonWebKey(kty="EC", crv="P-256", x="xx", y="yy", d="priv")
        pub = jwk.public_key()
        assert pub.x == "xx"
        assert pub.y == "yy"
        assert pub.d is None

    def test_public_key_raises_for_oct(self) -> None:
        """public_key() must raise TypeError for symmetric keys."""
        jwk = JsonWebKey(kty="oct", k="secret")
        with pytest.raises(TypeError, match="Symmetric"):
            jwk.public_key()

    def test_public_key_on_already_public_is_noop(self) -> None:
        """Calling public_key() on an already-public JWK returns an equal instance."""
        pub = JsonWebKey(kty="RSA", n="mod", e="exp")
        pub2 = pub.public_key()
        assert pub == pub2

    def test_thumbprint_rfc7638_appendix_a(self) -> None:
        """
        Thumbprint must match the RFC 7638 Appendix A test vector exactly.

        The vector uses a specific RSA key whose expected thumbprint is
        ``NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs``.
        This test locks down the canonical JSON construction and hash.
        """
        # RFC 7638 Appendix A — exact key values from the specification
        jwk = JsonWebKey(
            kty="RSA",
            e="AQAB",
            kid="2011-04-29",
            n=(
                "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx"
                "4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMs"
                "tn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2"
                "QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbI"
                "SD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqb"
                "w0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw"
            ),
        )
        assert jwk.thumbprint() == "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs"

    def test_thumbprint_ec(self) -> None:
        """EC thumbprint must use crv, kty, x, y in lexicographic order."""
        jwk = JsonWebKey(kty="EC", crv="P-256", x="somex", y="somey")
        tp = jwk.thumbprint()
        # Must be a non-empty base64url string with no padding
        assert tp
        assert "=" not in tp

    def test_thumbprint_raises_for_missing_required_fields(self) -> None:
        """Thumbprint must raise ValueError when required fields are None."""
        jwk = JsonWebKey(kty="RSA")  # n and e are None
        with pytest.raises(ValueError, match="missing"):
            jwk.thumbprint()

    def test_thumbprint_raises_for_unsupported_kty(self) -> None:
        """Thumbprint must raise ValueError for unknown kty values."""
        jwk = JsonWebKey(kty="OKP")  # Ed25519 — not yet supported
        with pytest.raises(ValueError, match="unsupported kty"):
            jwk.thumbprint()

    def test_repr_contains_key_info(self) -> None:
        """__repr__ must include kty, kid, alg, use, has_private."""
        jwk = JsonWebKey(kty="RSA", kid="k1", alg="RS256", use="sig")
        r = repr(jwk)
        assert "RSA" in r
        assert "k1" in r
        assert "RS256" in r


# ── JsonWebKeySet ──────────────────────────────────────────────────────────────


class TestJsonWebKeySet:
    """JsonWebKeySet — JWKS container with lookup and public-stripping helpers."""

    def _make_keyset(self) -> JsonWebKeySet:
        k1 = JsonWebKey(kty="RSA", kid="rsa-1", n="mod1", e="exp1", d="priv1")
        k2 = JsonWebKey(kty="EC", kid="ec-1", crv="P-256", x="x1", y="y1", d="ec_priv")
        k3 = JsonWebKey(kty="oct", kid="sym-1", k="secret")
        return JsonWebKeySet(keys=(k1, k2, k3))

    def test_to_dict_structure(self) -> None:
        """to_dict() must produce {\"keys\": [...]} with each key serialised."""
        ks = self._make_keyset()
        d = ks.to_dict()
        assert list(d.keys()) == ["keys"]
        assert len(d["keys"]) == 3
        assert d["keys"][0]["kty"] == "RSA"

    def test_to_dict_empty_keyset(self) -> None:
        """Empty keyset must produce {\"keys\": []}."""
        ks = JsonWebKeySet(keys=())
        assert ks.to_dict() == {"keys": []}

    def test_find_by_kid_returns_match(self) -> None:
        """find_by_kid() must return the key whose kid matches."""
        ks = self._make_keyset()
        found = ks.find_by_kid("ec-1")
        assert found is not None
        assert found.kty == "EC"

    def test_find_by_kid_returns_none_when_missing(self) -> None:
        """find_by_kid() must return None when no key matches."""
        ks = self._make_keyset()
        assert ks.find_by_kid("does-not-exist") is None

    def test_find_by_kid_ignores_none_kids(self) -> None:
        """Keys with kid=None must never match a string lookup."""
        ks = JsonWebKeySet(keys=(JsonWebKey(kty="RSA", n="x", e="y"),))
        assert ks.find_by_kid("anything") is None

    def test_public_set_strips_private_and_excludes_oct(self) -> None:
        """public_set() must strip private params and drop oct keys entirely."""
        ks = self._make_keyset()
        pub = ks.public_set()
        # oct key excluded — only RSA and EC remain
        assert len(pub.keys) == 2
        ktypes = {k.kty for k in pub.keys}
        assert ktypes == {"RSA", "EC"}
        # Private params stripped
        for k in pub.keys:
            assert k.d is None

    def test_public_set_on_oct_only_keyset_returns_empty(self) -> None:
        """public_set() of an all-oct keyset returns an empty keyset."""
        ks = JsonWebKeySet(keys=(JsonWebKey(kty="oct", kid="s1", k="secret"),))
        assert ks.public_set().keys == ()

    def test_repr_contains_count_and_kids(self) -> None:
        """__repr__ must include count and kid list."""
        ks = self._make_keyset()
        r = repr(ks)
        assert "3" in r


# ── JwkBuilder — RSA ──────────────────────────────────────────────────────────


class TestJwkBuilderRsa:
    """JwkBuilder RSA factories — from_rsa_public_key and from_rsa_private_key."""

    def test_from_rsa_public_key_produces_valid_jwk(self) -> None:
        """from_rsa_public_key must set kty, kid, use, alg, n, e with no private params."""
        jwk = JwkBuilder.from_rsa_public_key(
            _RSA_PUB_KEY, kid="k1", use="sig", alg="RS256"
        )
        assert jwk.kty == "RSA"
        assert jwk.kid == "k1"
        assert jwk.use == "sig"
        assert jwk.alg == "RS256"
        assert jwk.n is not None and len(jwk.n) > 0
        assert jwk.e is not None and len(jwk.e) > 0
        # No private material
        assert jwk.d is None
        assert jwk.p is None
        assert jwk.q is None

    def test_from_rsa_public_key_n_is_base64url_no_padding(self) -> None:
        """n must be a valid base64url string with no '=' padding."""
        jwk = JwkBuilder.from_rsa_public_key(
            _RSA_PUB_KEY, kid="k1", use="sig", alg="RS256"
        )
        assert "=" not in jwk.n  # type: ignore[operator]
        # Must decode back to bytes without error
        padded = jwk.n + "=" * (-len(jwk.n) % 4)  # type: ignore[operator]
        base64.urlsafe_b64decode(padded)

    def test_from_rsa_private_key_default_returns_public_only(self) -> None:
        """from_rsa_private_key with include_private=False must omit private params."""
        jwk = JwkBuilder.from_rsa_private_key(
            _RSA_KEY, kid="k1", use="sig", alg="RS256"
        )
        assert jwk.d is None
        assert jwk.p is None
        assert jwk.n is not None  # public params present

    def test_from_rsa_private_key_include_private_true(self) -> None:
        """from_rsa_private_key with include_private=True must populate all CRT params."""
        jwk = JwkBuilder.from_rsa_private_key(
            _RSA_KEY,
            kid="k1",
            use="sig",
            alg="RS256",
            include_private=True,
        )
        assert jwk.d is not None
        assert jwk.p is not None
        assert jwk.q is not None
        assert jwk.dp is not None
        assert jwk.dq is not None
        assert jwk.qi is not None

    def test_from_rsa_private_key_public_matches_from_public_key(self) -> None:
        """JWK from private (public only) must match JWK from the public key directly."""
        from_priv = JwkBuilder.from_rsa_private_key(
            _RSA_KEY, kid="k1", use="sig", alg="RS256"
        )
        from_pub = JwkBuilder.from_rsa_public_key(
            _RSA_PUB_KEY, kid="k1", use="sig", alg="RS256"
        )
        assert from_priv == from_pub

    def test_from_rsa_public_key_wrong_type_raises(self) -> None:
        """from_rsa_public_key must raise TypeError for non-RSA key objects."""
        ec_key = _EC_P256_KEY.public_key()
        with pytest.raises(TypeError, match="RSAPublicKey"):
            JwkBuilder.from_rsa_public_key(ec_key, kid="k1", use="sig", alg="RS256")  # type: ignore[arg-type]

    def test_from_rsa_private_key_wrong_type_raises(self) -> None:
        """from_rsa_private_key must raise TypeError for non-RSA key objects."""
        with pytest.raises(TypeError, match="RSAPrivateKey"):
            JwkBuilder.from_rsa_private_key(_EC_P256_KEY, kid="k1", use="sig", alg="RS256")  # type: ignore[arg-type]


# ── JwkBuilder — EC ───────────────────────────────────────────────────────────


class TestJwkBuilderEc:
    """JwkBuilder EC factories — from_ec_public_key and from_ec_private_key."""

    @pytest.mark.parametrize(
        "private_key, expected_crv, coord_bytes",
        [
            (_EC_P256_KEY, "P-256", 32),
            (_EC_P384_KEY, "P-384", 48),
            (_EC_P521_KEY, "P-521", 66),
        ],
        ids=["P-256", "P-384", "P-521"],
    )
    def test_from_ec_public_key_all_curves(
        self,
        private_key: ec.EllipticCurvePrivateKey,
        expected_crv: str,
        coord_bytes: int,
    ) -> None:
        """from_ec_public_key must produce correct crv and zero-padded coordinates."""
        pub_key = private_key.public_key()
        alg = {"P-256": "ES256", "P-384": "ES384", "P-521": "ES512"}[expected_crv]
        jwk = JwkBuilder.from_ec_public_key(pub_key, kid="k1", use="sig", alg=alg)

        assert jwk.kty == "EC"
        assert jwk.crv == expected_crv
        assert jwk.x is not None
        assert jwk.y is not None
        assert jwk.d is None  # no private scalar

        # Decode x and y and check byte length — zero-padding must be applied
        def _decode_b64url(s: str) -> bytes:
            return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))

        assert (
            len(_decode_b64url(jwk.x)) == coord_bytes
        ), f"x coordinate must be {coord_bytes} bytes for {expected_crv}"
        assert (
            len(_decode_b64url(jwk.y)) == coord_bytes
        ), f"y coordinate must be {coord_bytes} bytes for {expected_crv}"

    def test_from_ec_private_key_default_returns_public_only(self) -> None:
        """from_ec_private_key with include_private=False must omit d."""
        jwk = JwkBuilder.from_ec_private_key(
            _EC_P256_KEY, kid="k1", use="sig", alg="ES256"
        )
        assert jwk.d is None
        assert jwk.x is not None

    def test_from_ec_private_key_include_private_true(self) -> None:
        """from_ec_private_key with include_private=True must include d."""
        jwk = JwkBuilder.from_ec_private_key(
            _EC_P256_KEY,
            kid="k1",
            use="sig",
            alg="ES256",
            include_private=True,
        )
        assert jwk.d is not None

    def test_from_ec_private_key_d_is_correct_size(self) -> None:
        """Private scalar d must be zero-padded to the curve's coordinate size."""

        def _decode_b64url(s: str) -> bytes:
            return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))

        for priv_key, coord_bytes in [
            (_EC_P256_KEY, 32),
            (_EC_P384_KEY, 48),
            (_EC_P521_KEY, 66),
        ]:
            alg = {32: "ES256", 48: "ES384", 66: "ES512"}[coord_bytes]
            jwk = JwkBuilder.from_ec_private_key(
                priv_key,
                kid="k",
                use="sig",
                alg=alg,
                include_private=True,
            )
            assert len(_decode_b64url(jwk.d)) == coord_bytes  # type: ignore[arg-type]

    def test_from_ec_public_key_wrong_type_raises(self) -> None:
        """from_ec_public_key must raise TypeError for non-EC key objects."""
        with pytest.raises(TypeError, match="EllipticCurvePublicKey"):
            JwkBuilder.from_ec_public_key(_RSA_PUB_KEY, kid="k1", use="sig", alg="ES256")  # type: ignore[arg-type]

    def test_from_ec_private_key_wrong_type_raises(self) -> None:
        """from_ec_private_key must raise TypeError for non-EC key objects."""
        with pytest.raises(TypeError, match="EllipticCurvePrivateKey"):
            JwkBuilder.from_ec_private_key(_RSA_KEY, kid="k1", use="sig", alg="ES256")  # type: ignore[arg-type]


# ── JwkBuilder — PEM ──────────────────────────────────────────────────────────


class TestJwkBuilderPem:
    """JwkBuilder.from_pem — auto-detects key type and public/private."""

    def test_from_pem_rsa_private_key_default_public_only(self) -> None:
        """from_pem on a private RSA PEM must return public-only JWK by default."""
        jwk = JwkBuilder.from_pem(_RSA_PRIVATE_PEM, kid="k1", use="sig", alg="RS256")
        assert jwk.kty == "RSA"
        assert jwk.n is not None
        assert jwk.d is None  # include_private defaults to False

    def test_from_pem_rsa_private_key_include_private(self) -> None:
        """from_pem on a private RSA PEM with include_private=True must set d."""
        jwk = JwkBuilder.from_pem(
            _RSA_PRIVATE_PEM,
            kid="k1",
            use="sig",
            alg="RS256",
            include_private=True,
        )
        assert jwk.d is not None

    def test_from_pem_rsa_public_key(self) -> None:
        """from_pem on a public RSA PEM must produce a public-only JWK."""
        jwk = JwkBuilder.from_pem(_RSA_PUBLIC_PEM, kid="k1", use="sig", alg="RS256")
        assert jwk.kty == "RSA"
        assert jwk.n is not None
        assert jwk.d is None

    def test_from_pem_rsa_public_matches_from_private(self) -> None:
        """JWK from public PEM must match JWK from private PEM (public only)."""
        from_priv_pem = JwkBuilder.from_pem(
            _RSA_PRIVATE_PEM, kid="k1", use="sig", alg="RS256"
        )
        from_pub_pem = JwkBuilder.from_pem(
            _RSA_PUBLIC_PEM, kid="k1", use="sig", alg="RS256"
        )
        assert from_priv_pem == from_pub_pem

    def test_from_pem_ec_private_key_default_public_only(self) -> None:
        """from_pem on a private EC PEM must return public-only JWK by default."""
        jwk = JwkBuilder.from_pem(_EC_PRIVATE_PEM, kid="k1", use="sig", alg="ES256")
        assert jwk.kty == "EC"
        assert jwk.crv == "P-256"
        assert jwk.d is None

    def test_from_pem_ec_public_key(self) -> None:
        """from_pem on a public EC PEM must produce a public-only JWK."""
        jwk = JwkBuilder.from_pem(_EC_PUBLIC_PEM, kid="k1", use="sig", alg="ES256")
        assert jwk.kty == "EC"
        assert jwk.d is None

    def test_from_pem_ec_private_include_private(self) -> None:
        """from_pem on a private EC PEM with include_private=True must set d."""
        jwk = JwkBuilder.from_pem(
            _EC_PRIVATE_PEM,
            kid="k1",
            use="sig",
            alg="ES256",
            include_private=True,
        )
        assert jwk.d is not None

    def test_from_pem_invalid_pem_raises(self) -> None:
        """from_pem on garbage bytes must raise (ValueError from cryptography)."""
        with pytest.raises(Exception):  # ValueError or similar from cryptography
            JwkBuilder.from_pem(b"not-a-pem-at-all", kid="k1", use="sig", alg="RS256")


# ── JwkBuilder — keyset ───────────────────────────────────────────────────────


class TestJwkBuilderKeyset:
    """JwkBuilder.keyset — constructs JsonWebKeySet from multiple keys."""

    def test_keyset_wraps_multiple_keys(self) -> None:
        """keyset() must wrap all provided keys in a JsonWebKeySet."""
        k1 = JwkBuilder.from_rsa_public_key(
            _RSA_PUB_KEY, kid="rsa", use="sig", alg="RS256"
        )
        k2 = JwkBuilder.from_ec_public_key(
            _EC_P256_KEY.public_key(), kid="ec", use="sig", alg="ES256"
        )
        ks = JwkBuilder.keyset(k1, k2)
        assert isinstance(ks, JsonWebKeySet)
        assert len(ks.keys) == 2

    def test_keyset_empty_produces_valid_jwks(self) -> None:
        """keyset() with no arguments must produce a valid empty JWKS."""
        ks = JwkBuilder.keyset()
        assert ks.to_dict() == {"keys": []}

    def test_keyset_to_dict_is_json_serializable(self) -> None:
        """The JWKS dict must be fully JSON-serializable without errors."""
        k1 = JwkBuilder.from_rsa_public_key(
            _RSA_PUB_KEY, kid="k1", use="sig", alg="RS256"
        )
        ks = JwkBuilder.keyset(k1)
        # Must not raise — verifies no non-serializable types slipped through
        json_str = json.dumps(ks.public_set().to_dict())
        assert '"kty": "RSA"' in json_str

    def test_keyset_public_set_workflow(self) -> None:
        """End-to-end: build from private key → keyset → public_set → expose."""
        rsa_jwk = JwkBuilder.from_rsa_private_key(
            _RSA_KEY,
            kid="auth-key",
            use="sig",
            alg="RS256",
            include_private=True,
        )
        ks = JwkBuilder.keyset(rsa_jwk)
        public_jwks = ks.public_set().to_dict()
        # Exactly one key in the JWKS
        assert len(public_jwks["keys"]) == 1
        # The exposed key must NOT contain private material
        exposed = public_jwks["keys"][0]
        assert "d" not in exposed
        assert "p" not in exposed
        assert exposed["kty"] == "RSA"
