"""
Unit tests for varco_core.revocation — the model/ABC layer (Plan 034 / S13a,
§D-S13-shape).  Pure addition; no wiring here (see test_registry_revocation.py).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest


class TestRevocationScope:
    def test_has_four_members(self):
        from varco_core.revocation import RevocationScope

        assert {m.value for m in RevocationScope} == {
            "token",
            "subject",
            "tenant",
            "issuer",
        }


class TestRevocationEntry:
    def test_is_frozen(self):
        from varco_core.revocation import RevocationEntry, RevocationScope

        entry = RevocationEntry(
            scope=RevocationScope.TOKEN,
            key="jti-1",
            revoked_at=datetime.now(UTC),
            expires_at=None,
        )
        with pytest.raises(Exception):
            entry.key = "jti-2"  # type: ignore[misc]

    def test_rejects_naive_revoked_at_datetime(self):
        from varco_core.revocation import RevocationEntry, RevocationScope

        with pytest.raises(ValueError):
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-1",
                revoked_at=datetime.now(),  # naive — house rule: aware UTC only
                expires_at=None,
            )

    def test_rejects_naive_expires_at_datetime(self):
        from varco_core.revocation import RevocationEntry, RevocationScope

        with pytest.raises(ValueError):
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-1",
                revoked_at=datetime.now(UTC),
                expires_at=datetime.now(),  # naive
            )

    def test_for_token_sets_expires_at_to_exp_plus_skew(self):
        # brief 009 §5's TTL rule, asserted as an arithmetic property.
        from varco_core.revocation import RevocationEntry, RevocationScope

        exp = datetime(2026, 1, 1, tzinfo=UTC)
        entry = RevocationEntry.for_token("jti-1", exp, skew=60)
        assert entry.scope is RevocationScope.TOKEN
        assert entry.key == "jti-1"
        assert entry.expires_at == exp + timedelta(seconds=60)

    def test_for_token_with_none_jti_raises_value_error(self):
        # Edge case: a TOKEN-scope revocation cannot be expressed for a
        # token that never had a jti.
        from varco_core.revocation import RevocationEntry

        with pytest.raises(ValueError):
            RevocationEntry.for_token(None, datetime.now(UTC), skew=60)


class TestRevocationVerdict:
    def test_default_not_revoked_has_no_scope(self):
        from varco_core.revocation import RevocationVerdict

        verdict = RevocationVerdict(revoked=False)
        assert verdict.scope is None
        assert verdict.key is None
