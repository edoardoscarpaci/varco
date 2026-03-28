"""
Unit tests for JWK enc key support
=====================================
Covers ``JwkBuilder.generate_oct_enc_key`` and ``JwkEncryptorBridge``.

Sections
--------
- ``JwkBuilder.generate_oct_enc_key`` — key generation, field values, uniqueness
- ``JwkEncryptorBridge.from_jwk``     — conversion to FernetFieldEncryptor,
                                        validation errors
- ``JwkEncryptorBridge.to_jwk``       — conversion from raw bytes, validation errors
- Round-trip                          — generate → bridge → encrypt → decrypt
"""

from __future__ import annotations

import base64

import pytest
from cryptography.fernet import Fernet

from varco_core.encryption import FernetFieldEncryptor, JwkEncryptorBridge
from varco_core.jwk.builder import JwkBuilder
from varco_core.jwk.model import JsonWebKey


# ════════════════════════════════════════════════════════════════════════════════
# JwkBuilder.generate_oct_enc_key
# ════════════════════════════════════════════════════════════════════════════════


class TestGenerateOctEncKey:
    def test_returns_jwk(self) -> None:
        jwk = JwkBuilder.generate_oct_enc_key("my-kid")
        assert isinstance(jwk, JsonWebKey)

    def test_kty_is_oct(self) -> None:
        jwk = JwkBuilder.generate_oct_enc_key("k")
        assert jwk.kty == "oct"

    def test_use_is_enc(self) -> None:
        jwk = JwkBuilder.generate_oct_enc_key("k")
        assert jwk.use == "enc"

    def test_kid_is_set(self) -> None:
        jwk = JwkBuilder.generate_oct_enc_key("tenant:acme:v1")
        assert jwk.kid == "tenant:acme:v1"

    def test_alg_defaults_to_a256gcm(self) -> None:
        jwk = JwkBuilder.generate_oct_enc_key("k")
        assert jwk.alg == "A256GCM"

    def test_alg_can_be_overridden(self) -> None:
        jwk = JwkBuilder.generate_oct_enc_key("k", alg="A128CBC-HS256")
        assert jwk.alg == "A128CBC-HS256"

    def test_k_field_is_set(self) -> None:
        jwk = JwkBuilder.generate_oct_enc_key("k")
        assert jwk.k is not None
        assert len(jwk.k) > 0

    def test_k_is_base64url_no_padding(self) -> None:
        """``k`` must contain no '=' padding characters (RFC 7518 §6.4.1)."""
        jwk = JwkBuilder.generate_oct_enc_key("k")
        assert "=" not in jwk.k  # type: ignore[operator]

    def test_k_decodes_to_32_bytes(self) -> None:
        """256-bit key → 32 raw bytes."""
        jwk = JwkBuilder.generate_oct_enc_key("k", bits=256)
        raw = JwkEncryptorBridge._decode_k(jwk.k)  # type: ignore[arg-type]
        assert len(raw) == 32

    def test_k_decodes_to_16_bytes_for_128_bits(self) -> None:
        jwk = JwkBuilder.generate_oct_enc_key("k", bits=128)
        raw = JwkEncryptorBridge._decode_k(jwk.k)  # type: ignore[arg-type]
        assert len(raw) == 16

    def test_two_calls_produce_different_keys(self) -> None:
        """Random key generation — two calls must not return the same key."""
        jwk1 = JwkBuilder.generate_oct_enc_key("k1")
        jwk2 = JwkBuilder.generate_oct_enc_key("k2")
        assert jwk1.k != jwk2.k

    def test_to_dict_round_trip(self) -> None:
        """JWK can be serialised and deserialised without losing key material."""
        original = JwkBuilder.generate_oct_enc_key("rt-key")
        d = original.to_dict()
        restored = JsonWebKey.from_dict(d)
        assert restored.k == original.k
        assert restored.kid == original.kid


# ════════════════════════════════════════════════════════════════════════════════
# JwkEncryptorBridge.from_jwk
# ════════════════════════════════════════════════════════════════════════════════


class TestJwkEncryptorBridgeFromJwk:
    def test_returns_fernet_encryptor(self) -> None:
        jwk = JwkBuilder.generate_oct_enc_key("k")
        enc = JwkEncryptorBridge.from_jwk(jwk)
        assert isinstance(enc, FernetFieldEncryptor)

    def test_wrong_kty_raises_value_error(self) -> None:
        jwk = JsonWebKey(kty="RSA", kid="k", use="enc")
        with pytest.raises(ValueError, match="kty='oct'"):
            JwkEncryptorBridge.from_jwk(jwk)

    def test_missing_k_raises_value_error(self) -> None:
        jwk = JsonWebKey(kty="oct", kid="k", use="enc", k=None)
        with pytest.raises(ValueError, match="k=None"):
            JwkEncryptorBridge.from_jwk(jwk)

    def test_wrong_key_length_raises_value_error(self) -> None:
        """128-bit key (16 bytes) is not valid for Fernet (requires 32 bytes)."""
        jwk = JwkBuilder.generate_oct_enc_key("k", bits=128)
        with pytest.raises(ValueError, match="32-byte"):
            JwkEncryptorBridge.from_jwk(jwk)

    def test_encryptor_can_encrypt_and_decrypt(self) -> None:
        jwk = JwkBuilder.generate_oct_enc_key("k")
        enc = JwkEncryptorBridge.from_jwk(jwk)
        ct = enc.encrypt(b"hello")
        assert enc.decrypt(ct) == b"hello"


# ════════════════════════════════════════════════════════════════════════════════
# JwkEncryptorBridge.to_jwk
# ════════════════════════════════════════════════════════════════════════════════


class TestJwkEncryptorBridgeToJwk:
    def test_returns_jwk_with_oct_kty(self) -> None:
        raw = b"\x00" * 32
        jwk = JwkEncryptorBridge.to_jwk(raw, "my-key")
        assert jwk.kty == "oct"
        assert jwk.use == "enc"
        assert jwk.kid == "my-key"

    def test_k_field_is_base64url_no_padding(self) -> None:
        raw = b"\xab" * 32
        jwk = JwkEncryptorBridge.to_jwk(raw, "k")
        assert jwk.k is not None
        assert "=" not in jwk.k

    def test_default_alg_is_a256gcm(self) -> None:
        jwk = JwkEncryptorBridge.to_jwk(b"\x00" * 32, "k")
        assert jwk.alg == "A256GCM"

    def test_wrong_length_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="32 bytes"):
            JwkEncryptorBridge.to_jwk(b"\x00" * 16, "k")

    def test_round_trip_raw_bytes(self) -> None:
        """to_jwk → from_jwk must preserve key material."""
        fernet_key = Fernet.generate_key()  # 44-char base64url bytes
        raw = base64.urlsafe_b64decode(fernet_key)  # 32 raw bytes
        jwk = JwkEncryptorBridge.to_jwk(raw, "rt")
        enc = JwkEncryptorBridge.from_jwk(jwk)
        # Must produce the same Fernet token as a direct FernetFieldEncryptor
        direct = FernetFieldEncryptor(fernet_key)
        ct = direct.encrypt(b"round-trip")
        assert enc.decrypt(ct) == b"round-trip"


# ════════════════════════════════════════════════════════════════════════════════
# Full round-trip: generate → store dict → reload → encrypt/decrypt
# ════════════════════════════════════════════════════════════════════════════════


class TestJwkEncRoundTrip:
    def test_generate_store_reload_round_trip(self) -> None:
        """
        Simulate a key lifecycle:
        1. Generate a JWK enc key.
        2. Serialise to dict (for storage in DB or Redis).
        3. Deserialise and build a FernetFieldEncryptor.
        4. Verify encrypt/decrypt round-trip.
        """
        # Step 1: generate
        original_jwk = JwkBuilder.generate_oct_enc_key("svc:myapp:v1")

        # Step 2: serialise (store in DB)
        stored = original_jwk.to_dict()
        assert stored["kty"] == "oct"
        assert stored["kid"] == "svc:myapp:v1"

        # Step 3: deserialise and build encryptor (on next pod startup)
        reloaded_jwk = JsonWebKey.from_dict(stored)
        enc = JwkEncryptorBridge.from_jwk(reloaded_jwk)

        # Step 4: encrypt with original, decrypt with reloaded
        original_enc = JwkEncryptorBridge.from_jwk(original_jwk)
        ct = original_enc.encrypt(b"sensitive payload")
        assert enc.decrypt(ct) == b"sensitive payload"

    def test_two_encryptors_from_same_jwk_are_compatible(self) -> None:
        """Two encryptors built from the same JWK must interoperate."""
        jwk = JwkBuilder.generate_oct_enc_key("shared-kid")
        enc_a = JwkEncryptorBridge.from_jwk(jwk)
        enc_b = JwkEncryptorBridge.from_jwk(jwk)
        ct = enc_a.encrypt(b"shared secret")
        assert enc_b.decrypt(ct) == b"shared secret"
