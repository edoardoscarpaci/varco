"""
Unit tests for ``varco_core.authority.jwt_authority.JwtAuthority``.

Plan 053 / FLIP-034-2 — ``JwtAuthority``-minted tokens carry a uuid4 ``jti``
by default (seeded in ``token()``); ``mint_jti=False`` is the escape hatch.
"""

from __future__ import annotations

import inspect
import uuid
from datetime import timedelta

import pytest


def _pem() -> bytes:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@pytest.fixture(scope="module")
def pem() -> bytes:
    return _pem()


def _authority(pem: bytes, *, kid: str = "k1", **kwargs):
    from varco_core.authority.jwt_authority import JwtAuthority

    return JwtAuthority.from_pem(pem, kid=kid, issuer="svc", algorithm="RS256", **kwargs)


class TestMintJti:
    async def test_token_carries_uuid4_jti_by_default(self, pem):
        jti = _authority(pem).token().build().jti
        assert jti is not None
        assert uuid.UUID(jti, version=4).version == 4

    async def test_two_tokens_differ(self, pem):
        a = _authority(pem)
        first, second = a.token().build().jti, a.token().build().jti
        assert first is not None and second is not None
        assert first != second

    async def test_signed_token_carries_jti(self, pem):
        a = _authority(pem)
        raw = a.sign(a.token().subject("u").expires_in(timedelta(minutes=5)))
        assert a.verify(raw).jti is not None

    async def test_token_id_overrides_minted(self, pem):
        # Last write wins (builder.py:203-214) — the per-token override.
        assert _authority(pem).token().token_id("fixed").build().jti == "fixed"

    async def test_hand_built_builder_is_not_minted(self, pem):
        # Control (passes today): sign() uses the builder as-is.
        from varco_core.jwt.builder import JwtBuilder

        a = _authority(pem)
        raw = a.sign(JwtBuilder().issuer(a.issuer).subject("u").expires_in(timedelta(minutes=5)))
        assert a.verify(raw).jti is None

    async def test_mint_jti_false_from_pem(self, pem):
        assert _authority(pem, mint_jti=False).token().build().jti is None

    async def test_mint_jti_false_direct_constructor(self, pem):
        from varco_core.authority.jwt_authority import JwtAuthority

        base = _authority(pem)
        a = JwtAuthority(
            private_key=base._private_key,
            public_key=base._public_key,
            public_jwk=base._public_jwk,
            kid=base.kid,
            issuer=base.issuer,
            algorithm="RS256",
            mint_jti=False,
        )
        assert a.token().build().jti is None

    async def test_multi_key_authority_inherits_per_authority(self, pem):
        from varco_core.authority.multi_key_authority import MultiKeyAuthority

        ring = MultiKeyAuthority(_authority(pem, kid="a", mint_jti=True))
        assert ring.token().build().jti is not None
        ring.rotate(_authority(_pem(), kid="b", mint_jti=False))
        assert ring.token().build().jti is None

    async def test_signature_default_is_true(self):
        # Class signatures are invisible to api_surface --check (CLAUDE.md).
        from varco_core.authority.jwt_authority import JwtAuthority

        for fn in (JwtAuthority.from_pem, JwtAuthority.__init__):
            param = inspect.signature(fn).parameters["mint_jti"]
            assert param.default is True
            assert param.kind is inspect.Parameter.KEYWORD_ONLY
