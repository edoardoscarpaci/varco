"""
Integration tests for RedisTokenRevocationStore (Plan 034 / S13b, Step 32).

Requires Docker (session-scoped ``redis_url`` fixture, CLAUDE.md's shared-
container convention) — per-test key namespacing via a uuid4 run id.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from varco_conformance.token_revocation import TokenRevocationStoreConformance

pytestmark = pytest.mark.integration


class TestRedisTokenRevocationStoreConformance(TokenRevocationStoreConformance):
    @pytest.fixture
    async def store(self, redis_url: str):
        from varco_redis.revocation import RedisTokenRevocationStore

        namespace = f"revocation-conformance-{uuid.uuid4().hex[:8]}"
        rstore = RedisTokenRevocationStore(url=redis_url, namespace=namespace)
        yield rstore


class TestRedisRevocationTtlBehaviour:
    async def test_token_entry_ttl_within_a_second_of_expected(self, redis_url: str):
        from varco_core.revocation import RevocationEntry, RevocationScope
        from varco_redis.revocation import RedisTokenRevocationStore

        namespace = f"revocation-ttl-{uuid.uuid4().hex[:8]}"
        store = RedisTokenRevocationStore(url=redis_url, namespace=namespace)
        expires_at = datetime.now(UTC) + timedelta(seconds=30)
        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TOKEN,
                key="jti-ttl-1",
                revoked_at=datetime.now(UTC),
                expires_at=expires_at,
            )
        )
        ttl_ms = await store._client.pttl(f"{namespace}:token:jti-ttl-1")  # type: ignore[attr-defined]
        expected_ms = 30_000
        assert abs(ttl_ms - expected_ms) < 2_000

    async def test_kill_switch_with_no_expiry_has_no_ttl(self, redis_url: str):
        from varco_core.revocation import RevocationEntry, RevocationScope
        from varco_redis.revocation import RedisTokenRevocationStore

        namespace = f"revocation-killswitch-{uuid.uuid4().hex[:8]}"
        store = RedisTokenRevocationStore(url=redis_url, namespace=namespace)
        await store.revoke(
            RevocationEntry(
                scope=RevocationScope.TENANT,
                key="tenant-1",
                revoked_at=datetime.now(UTC),
                expires_at=None,
            )
        )
        ttl_ms = await store._client.pttl(f"{namespace}:tenant:tenant-1")  # type: ignore[attr-defined]
        # -1 = key exists with no TTL (redis-py pttl convention).
        assert ttl_ms == -1
