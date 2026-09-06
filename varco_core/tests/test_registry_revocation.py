"""
Unit tests for TrustedIssuerRegistry's revocation-store wiring
(Plan 034 / S13b, Step 25/26, §D-S13-hook, §D-S13-order, §D-S13-scope).

Mirrors test_trusted_issuer_registry.py's RSA-authority helper pattern
(``register_authority`` + ``JwtAuthority``) rather than hand-building
``JsonWebKey``/``TrustedIssuerEntry`` objects.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest


def _make_authority(*, issuer: str = "svc", kid: str = "k1"):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from varco_core.authority.jwt_authority import JwtAuthority

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return JwtAuthority.from_pem(pem, kid=kid, issuer=issuer, algorithm="RS256")


async def _make_registry(authority, *, revocation_store=None):
    from varco_core.authority.registry import TrustedIssuerRegistry

    registry = TrustedIssuerRegistry(revocation_store=revocation_store)
    registry.register_authority(authority, label="A")
    await registry.load_all()
    return registry


class TestNoStoreConfigured:
    async def test_verify_with_no_store_never_touches_it(self):
        # §D-S13-hook: revocation_store=None → zero-config, byte-identical.
        authority = _make_authority()
        registry = await _make_registry(authority, revocation_store=None)
        token = authority.sign(authority.token().subject("usr_1"))
        result = await registry.verify(token)
        assert result.sub == "usr_1"


class TestRevokedToken:
    async def test_revoked_jti_raises_token_revoked_error(self):
        from varco_core.authority.exceptions import TokenRevokedError
        from varco_core.revocation import RevocationEntry, RevocationScope
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()
        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-1",
                revoked_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        authority = _make_authority()
        registry = await _make_registry(authority, revocation_store=store)
        token = authority.sign(authority.token().subject("usr_1").token_id("jti-1"))
        with pytest.raises(TokenRevokedError):
            await registry.verify(token)

    async def test_subject_watermark_revokes_older_admits_newer(self):
        from varco_core.authority.exceptions import TokenRevokedError
        from varco_core.revocation import RevocationEntry, RevocationScope
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        watermark = datetime.now(UTC)
        store = InMemoryTokenRevocationStore()
        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.SUBJECT,
                key="svc|usr_1",
                revoked_at=watermark,
                expires_at=None,
            )
        )
        authority = _make_authority()
        registry = await _make_registry(authority, revocation_store=store)

        old_token = authority.sign(
            authority.token().subject("usr_1").issued_at(watermark - timedelta(hours=1))
        )
        new_token = authority.sign(
            authority.token().subject("usr_1").issued_at(watermark + timedelta(hours=1))
        )

        with pytest.raises(TokenRevokedError):
            await registry.verify(old_token)

        result = await registry.verify(new_token)
        assert result.sub == "usr_1"

    async def test_check_revocation_false_bypasses_the_check(self):
        from varco_core.revocation import RevocationEntry, RevocationScope
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()
        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-1",
                revoked_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        authority = _make_authority()
        registry = await _make_registry(authority, revocation_store=store)
        token = authority.sign(authority.token().subject("usr_1").token_id("jti-1"))
        result = await registry.verify(token, check_revocation=False)
        assert result.sub == "usr_1"


class TestRequireJti:
    async def test_require_jti_true_and_jtiless_token_raises(self):
        from varco_core.authority.exceptions import TokenRevokedError
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()
        authority = _make_authority()
        registry = await _make_registry(authority, revocation_store=store)
        token = authority.sign(authority.token().subject("usr_1"))
        with pytest.raises(TokenRevokedError):
            await registry.verify(token, revocation_require_jti=True)  # type: ignore[call-arg]

    async def test_require_jti_false_and_jtiless_token_with_no_watermark_verifies(self):
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()
        authority = _make_authority()
        registry = await _make_registry(authority, revocation_store=store)
        token = authority.sign(authority.token().subject("usr_1"))
        result = await registry.verify(token)
        assert result.sub == "usr_1"


def _broken_store():
    from varco_core.revocation.base import AbstractTokenRevocationStore

    class _BrokenStore(AbstractTokenRevocationStore):
        async def revoke(self, entry):
            raise NotImplementedError

        async def unrevoke(self, scope, key):
            raise NotImplementedError

        async def is_revoked(self, *, jti, subject, issuer, tenant_id, issued_at):
            raise RuntimeError("store outage")

        async def list_entries(self, scope=None):
            raise NotImplementedError

        async def delete_expired(self):
            raise NotImplementedError

    return _BrokenStore()


class TestStoreFailure:
    async def test_store_raising_under_fail_closed_maps_to_store_unavailable(self):
        from varco_core.authority.exceptions import RevocationStoreUnavailableError

        authority = _make_authority()
        registry = await _make_registry(authority, revocation_store=_broken_store())
        token = authority.sign(authority.token().subject("usr_1"))
        with pytest.raises(RevocationStoreUnavailableError):
            await registry.verify(token)

    async def test_store_raising_under_fail_open_verifies_and_logs_error(self, caplog):
        authority = _make_authority()
        registry = await _make_registry(authority, revocation_store=_broken_store())
        token = authority.sign(authority.token().subject("usr_1"))
        with caplog.at_level(logging.ERROR):
            result = await registry.verify(
                token,
                revocation_failure_mode="fail_open",  # type: ignore[call-arg]
            )
        assert result.sub == "usr_1"
        assert any(r.levelno == logging.ERROR for r in caplog.records)


class TestOrdering:
    async def test_revocation_lookup_never_runs_for_a_token_failing_iss(self):
        # §D-S13-order: signature/issuer failures must never reach the store.
        from unittest.mock import AsyncMock

        spy_store = AsyncMock()
        authority = _make_authority(issuer="issuer-a")
        registry = await _make_registry(authority, revocation_store=spy_store)

        forged = authority.sign(authority.token().subject("usr_1").issuer("issuer-b"))
        with pytest.raises(Exception):
            await registry.verify(forged)
        spy_store.is_revoked.assert_not_called()
