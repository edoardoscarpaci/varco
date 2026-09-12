"""
TokenRevocationStoreConformance — shared contract tests for
``AbstractTokenRevocationStore`` implementations (Plan 034 / S13b, Step 30).

Subclass and override the ``store`` fixture to opt a backend in::

    from varco_conformance.token_revocation import TokenRevocationStoreConformance

    class TestRedisRevocationConformance(TokenRevocationStoreConformance):
        @pytest.fixture
        async def store(self, redis_url):
            yield RedisTokenRevocationStore(url=redis_url, namespace=uuid4().hex[:8])

Not named ``Test*`` — never collected standalone (see package docstring on
the other conformance modules).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest


class TokenRevocationStoreConformance:
    """Shared behavioural contract for ``AbstractTokenRevocationStore``."""

    @pytest.fixture
    async def store(self):
        """Abstract — must be overridden by every subclass."""
        raise NotImplementedError(
            "TokenRevocationStoreConformance subclasses must override the `store` fixture."
        )

    async def test_token_scope_revoke_then_is_revoked(self, store):
        from varco_core.revocation import RevocationEntry, RevocationScope

        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-conformance-1",
                revoked_at=datetime.now(UTC),
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
        )
        verdict = await store.is_revoked(
            jti="jti-conformance-1",
            subject=None,
            issuer=None,
            tenant_id=None,
            issued_at=None,
        )
        assert verdict.revoked is True

    async def test_watermark_rule_iat_before_revoked_at_is_revoked(self, store):
        from varco_core.revocation import RevocationEntry, RevocationScope

        watermark = datetime.now(UTC)
        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.SUBJECT,
                key="conformance|subject-1",
                revoked_at=watermark,
                expires_at=None,
            )
        )
        older = await store.is_revoked(
            jti=None,
            subject="conformance|subject-1",
            issuer=None,
            tenant_id=None,
            issued_at=watermark - timedelta(seconds=1),
        )
        newer = await store.is_revoked(
            jti=None,
            subject="conformance|subject-1",
            issuer=None,
            tenant_id=None,
            issued_at=watermark + timedelta(seconds=1),
        )
        assert older.revoked is True
        assert newer.revoked is False

    async def test_missing_iat_is_treated_as_revoked(self, store):
        from varco_core.revocation import RevocationEntry, RevocationScope

        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TENANT,
                key="conformance-tenant-1",
                revoked_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        verdict = await store.is_revoked(
            jti=None,
            subject=None,
            issuer=None,
            tenant_id="conformance-tenant-1",
            issued_at=None,
        )
        assert verdict.revoked is True

    async def test_expired_entry_no_longer_matches(self, store):
        from varco_core.revocation import RevocationEntry, RevocationScope

        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-conformance-expired",
                revoked_at=datetime.now(UTC) - timedelta(hours=2),
                expires_at=datetime.now(UTC) - timedelta(hours=1),
            )
        )
        verdict = await store.is_revoked(
            jti="jti-conformance-expired",
            subject=None,
            issuer=None,
            tenant_id=None,
            issued_at=None,
        )
        assert verdict.revoked is False

    async def test_unrevoke_removes_entry(self, store):
        from varco_core.revocation import RevocationEntry, RevocationScope

        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-conformance-unrevoke",
                revoked_at=datetime.now(UTC),
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
        )
        removed = await store.unrevoke(RevocationScope.TOKEN, "jti-conformance-unrevoke")
        assert removed is True
        verdict = await store.is_revoked(
            jti="jti-conformance-unrevoke",
            subject=None,
            issuer=None,
            tenant_id=None,
            issued_at=None,
        )
        assert verdict.revoked is False

    async def test_unrevoke_absent_key_returns_false(self, store):
        from varco_core.revocation import RevocationScope

        removed = await store.unrevoke(RevocationScope.TOKEN, "jti-conformance-never-existed")
        assert removed is False

    async def test_delete_expired_returns_count(self, store):
        from varco_core.revocation import RevocationEntry, RevocationScope

        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-conformance-delete-expired",
                revoked_at=datetime.now(UTC) - timedelta(hours=2),
                expires_at=datetime.now(UTC) - timedelta(hours=1),
            )
        )
        count = await store.delete_expired()
        assert count >= 1

    async def test_list_entries_filters_by_scope(self, store):
        from varco_core.revocation import RevocationEntry, RevocationScope

        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-conformance-list-1",
                revoked_at=datetime.now(UTC),
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            )
        )
        entries = await store.list_entries(RevocationScope.TOKEN)
        assert any(e.key == "jti-conformance-list-1" for e in entries)

    async def test_double_revoke_is_idempotent(self, store):
        from varco_core.revocation import RevocationEntry, RevocationScope

        entry = RevocationEntry(
            scope=RevocationScope.TOKEN,
            key="jti-conformance-double",
            revoked_at=datetime.now(UTC),
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        )
        await store.revoke(entry)
        await store.revoke(entry)  # must not raise
        verdict = await store.is_revoked(
            jti="jti-conformance-double",
            subject=None,
            issuer=None,
            tenant_id=None,
            issued_at=None,
        )
        assert verdict.revoked is True
