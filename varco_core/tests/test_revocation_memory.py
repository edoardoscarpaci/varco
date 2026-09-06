"""
Unit tests for varco_core.revocation.memory.InMemoryTokenRevocationStore
(Plan 034 / S13a, Step 17).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta


class TestTokenScope:
    async def test_revoke_then_is_revoked_true(self):
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
        verdict = await store.is_revoked(
            jti="jti-1", subject=None, issuer=None, tenant_id=None, issued_at=None
        )
        assert verdict.revoked is True
        assert verdict.scope == RevocationScope.TOKEN

    async def test_unknown_jti_not_revoked(self):
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()
        verdict = await store.is_revoked(
            jti="unknown", subject=None, issuer=None, tenant_id=None, issued_at=None
        )
        assert verdict.revoked is False


class TestWatermarkScopes:
    async def _watermark_case(self, scope_name: str, kwarg: str):
        from varco_core.revocation import RevocationEntry, RevocationScope
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()
        scope = getattr(RevocationScope, scope_name)
        watermark = datetime(2026, 1, 1, tzinfo=UTC)
        await store.revoke(
            RevocationEntry(scope=scope, key="key-1", revoked_at=watermark, expires_at=None)
        )

        older = watermark - timedelta(seconds=1)
        newer = watermark + timedelta(seconds=1)

        base = dict(jti=None, subject=None, issuer=None, tenant_id=None, issued_at=None)
        base[kwarg] = "key-1"

        revoked_verdict = await store.is_revoked(**{**base, "issued_at": older})
        admitted_verdict = await store.is_revoked(**{**base, "issued_at": newer})
        return revoked_verdict, admitted_verdict

    async def test_subject_scope_revokes_older_admits_newer(self):
        revoked, admitted = await self._watermark_case("SUBJECT", "subject")
        assert revoked.revoked is True
        assert admitted.revoked is False

    async def test_tenant_scope_revokes_older_admits_newer(self):
        revoked, admitted = await self._watermark_case("TENANT", "tenant_id")
        assert revoked.revoked is True
        assert admitted.revoked is False

    async def test_issuer_scope_revokes_older_admits_newer(self):
        revoked, admitted = await self._watermark_case("ISSUER", "issuer")
        assert revoked.revoked is True
        assert admitted.revoked is False

    async def test_missing_iat_treated_as_revoked_by_matching_watermark(self):
        # §D-S13-noiat: fail-closed on the ambiguous case.
        from varco_core.revocation import RevocationEntry, RevocationScope
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()
        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.SUBJECT,
                key="usr|iss",
                revoked_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        verdict = await store.is_revoked(
            jti=None, subject="usr|iss", issuer=None, tenant_id=None, issued_at=None
        )
        assert verdict.revoked is True


class TestExpiryAndHousekeeping:
    async def test_expired_entry_stops_matching(self):
        from varco_core.revocation import RevocationEntry, RevocationScope
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()
        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-1",
                revoked_at=datetime.now(UTC) - timedelta(hours=2),
                expires_at=datetime.now(UTC) - timedelta(hours=1),
            )
        )
        verdict = await store.is_revoked(
            jti="jti-1", subject=None, issuer=None, tenant_id=None, issued_at=None
        )
        assert verdict.revoked is False

    async def test_delete_expired_returns_count_removed(self):
        from varco_core.revocation import RevocationEntry, RevocationScope
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()
        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-1",
                revoked_at=datetime.now(UTC) - timedelta(hours=2),
                expires_at=datetime.now(UTC) - timedelta(hours=1),
            )
        )
        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-2",
                revoked_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        count = await store.delete_expired()
        assert count == 1

    async def test_unrevoke_returns_false_for_absent_key(self):
        from varco_core.revocation import RevocationScope
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()
        result = await store.unrevoke(RevocationScope.TOKEN, "nope")
        assert result is False

    async def test_unrevoke_returns_true_and_removes_entry(self):
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
        assert await store.unrevoke(RevocationScope.TOKEN, "jti-1") is True
        verdict = await store.is_revoked(
            jti="jti-1", subject=None, issuer=None, tenant_id=None, issued_at=None
        )
        assert verdict.revoked is False

    async def test_double_revoke_is_idempotent_last_write_wins(self):
        from varco_core.revocation import RevocationEntry, RevocationScope
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()
        first = RevocationEntry(
            scope=RevocationScope.TOKEN,
            key="jti-1",
            revoked_at=datetime.now(UTC),
            expires_at=None,
            reason="first",
        )
        second = RevocationEntry(
            scope=RevocationScope.TOKEN,
            key="jti-1",
            revoked_at=datetime.now(UTC),
            expires_at=None,
            reason="second",
        )
        await store.revoke(first)
        await store.revoke(second)
        entries = await store.list_entries(RevocationScope.TOKEN)
        assert len(entries) == 1
        assert entries[0].reason == "second"

    async def test_concurrent_revokes_do_not_lose_entries(self):
        from varco_core.revocation import RevocationEntry, RevocationScope
        from varco_core.revocation.memory import InMemoryTokenRevocationStore

        store = InMemoryTokenRevocationStore()

        async def _revoke(i: int) -> None:
            await store.revoke(
                RevocationEntry(
                    scope=RevocationScope.TOKEN,
                    key=f"jti-{i}",
                    revoked_at=datetime.now(UTC),
                    expires_at=None,
                )
            )

        await asyncio.gather(*(_revoke(i) for i in range(50)))
        entries = await store.list_entries(RevocationScope.TOKEN)
        assert len(entries) == 50
