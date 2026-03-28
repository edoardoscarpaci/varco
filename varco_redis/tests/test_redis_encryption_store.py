"""
Unit tests for varco_redis.encryption_store
=============================================
Covers ``RedisEncryptionKeyStore`` using an in-process fake Redis client.

No real Redis instance is required for these tests — a ``FakeRedis`` class
mirrors the exact redis.asyncio commands used by the store: ``hset``,
``hgetall``, ``sadd``, ``smembers``, ``srem``, ``scard``, ``delete``,
and ``scan``.

Integration tests (``pytest.mark.integration``) require a real Redis server
and are skipped by default.  Run with::

    VARCO_RUN_INTEGRATION=1 pytest varco_redis/tests/test_redis_encryption_store.py -m integration

Sections
--------
- ``FakeRedis``                  — in-process test double
- ``RedisEncryptionKeyStore``    — construction, key-name helpers, repr
- ``save``                       — hash write, tenant index updates, global key
- ``load``                       — found, not found
- ``load_for_tenant``            — tenant index, global keys, ordering
- ``list_tenants``               — global tenants set
- ``delete``                     — hash removal, index cleanup
- ``_repair_tenant_index``       — SCAN-based index rebuild
- Protocol conformance
"""

from __future__ import annotations
import os

import base64
from collections import defaultdict
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from cryptography.fernet import Fernet

from varco_core.encryption_store import EncryptionKeyEntry, EncryptionKeyStore
from varco_redis.encryption_store import RedisEncryptionKeyStore


# ── FakeRedis ─────────────────────────────────────────────────────────────────


class FakeRedis:
    """
    In-memory fake for ``redis.asyncio.Redis``.

    Supports exactly the commands used by ``RedisEncryptionKeyStore``:
    ``hset``, ``hgetall``, ``sadd``, ``smembers``, ``srem``, ``scard``,
    ``delete``, and ``scan``.

    DESIGN: defaultdict-backed plain dicts
      ✅ Minimal implementation — only what the store actually calls
      ✅ Returns bytes, matching the real redis.asyncio default behaviour
         (``decode_responses=False``)
      ❌ Does not support TTLs, expiry, or Lua scripts
    """

    def __init__(self) -> None:
        # Hash store: key → {field_bytes: value_bytes}
        self._hashes: dict[str, dict[bytes, bytes]] = defaultdict(dict)
        # Set store: key → set of bytes members
        self._sets: dict[str, set[bytes]] = defaultdict(set)

    async def hset(self, key: str, mapping: dict[str, str]) -> int:
        """HSET key field value [field value ...] — stores hash fields."""
        h = self._hashes[key]
        for field, value in mapping.items():
            h[field.encode("utf-8")] = value.encode("utf-8")
        return len(mapping)

    async def hgetall(self, key: str | bytes) -> dict[bytes, bytes]:
        """HGETALL key — returns all fields and values of a hash."""
        # Normalise bytes key (returned by SCAN) to str for internal dict lookup
        str_key = key.decode("utf-8") if isinstance(key, bytes) else key
        result = dict(self._hashes.get(str_key, {}))
        # Real Redis returns {} when key does not exist — not None
        return result

    async def sadd(self, key: str, *members: str) -> int:
        """SADD key member [member ...] — adds members to a set."""
        s = self._sets[key]
        added = 0
        for m in members:
            encoded = m.encode("utf-8") if isinstance(m, str) else m
            if encoded not in s:
                s.add(encoded)
                added += 1
        return added

    async def smembers(self, key: str) -> set[bytes]:
        """SMEMBERS key — returns all members of the set."""
        return set(self._sets.get(key, set()))

    async def srem(self, key: str, *members: str) -> int:
        """SREM key member [member ...] — removes members from set."""
        s = self._sets.get(key, set())
        removed = 0
        for m in members:
            encoded = m.encode("utf-8") if isinstance(m, str) else m
            if encoded in s:
                s.discard(encoded)
                removed += 1
        return removed

    async def scard(self, key: str) -> int:
        """SCARD key — returns number of members in set."""
        return len(self._sets.get(key, set()))

    async def delete(self, key: str) -> int:
        """DEL key — removes the hash for this key."""
        existed = key in self._hashes
        self._hashes.pop(key, None)
        return 1 if existed else 0

    async def scan(
        self,
        cursor: int,
        match: str = "*",
        count: int = 100,
    ) -> tuple[int, list[bytes]]:
        """
        SCAN cursor [MATCH pattern] [COUNT count] — iterates keys.

        Simplified implementation: returns all matching keys on the first
        call (cursor=0) then signals completion with cursor=0.  Adequate
        for testing ``_repair_tenant_index``.
        """
        # Only match hash keys (hashes represent encryption key entries)
        prefix = match.rstrip("*")
        matching = [k.encode("utf-8") for k in self._hashes if k.startswith(prefix)]
        # Single-pass: return all matches with cursor=0 (done)
        return 0, matching

    async def aclose(self) -> None:
        pass


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_store(prefix: str = "varco:enc") -> tuple[FakeRedis, RedisEncryptionKeyStore]:
    client = FakeRedis()
    store = RedisEncryptionKeyStore(client, prefix=prefix)  # type: ignore[arg-type]
    return client, store


def _make_entry(
    *,
    kid: str = "kid-1",
    tenant_id: str | None = "acme",
    is_primary: bool = True,
    wrapped: bool = False,
    offset_seconds: int = 0,
) -> EncryptionKeyEntry:
    raw = base64.urlsafe_b64decode(Fernet.generate_key())
    key_material = base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")
    return EncryptionKeyEntry(
        kid=kid,
        algorithm="fernet",
        key_material=key_material,
        created_at=datetime.now(UTC) + timedelta(seconds=offset_seconds),
        tenant_id=tenant_id,
        is_primary=is_primary,
        wrapped=wrapped,
    )


# ════════════════════════════════════════════════════════════════════════════════
# Construction and helpers
# ════════════════════════════════════════════════════════════════════════════════


class TestRedisEncryptionKeyStoreConstruction:
    def test_repr(self) -> None:
        _, store = _make_store()
        assert "RedisEncryptionKeyStore" in repr(store)

    def test_prefix_trailing_colon_stripped(self) -> None:
        """Trailing colon in prefix is normalised away."""
        client = FakeRedis()
        store = RedisEncryptionKeyStore(client, prefix="myapp:enc:")  # type: ignore[arg-type]
        # Internal prefix must not have trailing colon
        assert not store._prefix.endswith(":")

    def test_hash_key_format(self) -> None:
        _, store = _make_store()
        assert store._hash_key("my-kid") == "varco:enc:key:my-kid"

    def test_tenant_set_key_format(self) -> None:
        _, store = _make_store()
        assert store._tenant_set_key("acme") == "varco:enc:tenant:acme"

    def test_all_tenants_set_key_format(self) -> None:
        _, store = _make_store()
        assert store._all_tenants_set_key() == "varco:enc:tenants"


# ════════════════════════════════════════════════════════════════════════════════
# save
# ════════════════════════════════════════════════════════════════════════════════


class TestRedisEncryptionKeyStoreSave:
    async def test_save_writes_hash(self) -> None:
        client, store = _make_store()
        entry = _make_entry(kid="s1")
        await store.save(entry)
        # Hash must exist for the kid
        raw = await client.hgetall("varco:enc:key:s1")
        assert raw  # non-empty

    async def test_save_tenant_entry_updates_tenant_set(self) -> None:
        client, store = _make_store()
        entry = _make_entry(kid="s2", tenant_id="acme")
        await store.save(entry)
        members = await client.smembers("varco:enc:tenant:acme")
        assert b"s2" in members

    async def test_save_tenant_entry_updates_global_tenants_set(self) -> None:
        client, store = _make_store()
        entry = _make_entry(kid="s3", tenant_id="globex")
        await store.save(entry)
        members = await client.smembers("varco:enc:tenants")
        assert b"globex" in members

    async def test_save_global_key_no_tenant_set(self) -> None:
        """Global keys (tenant_id=None) must NOT add to any tenant Set."""
        client, store = _make_store()
        entry = _make_entry(kid="gl", tenant_id=None)
        await store.save(entry)
        # No tenant set created for None tenant
        assert b"gl" not in await client.smembers("varco:enc:tenant:None")

    async def test_save_global_key_not_in_global_tenants_set(self) -> None:
        """Global keys must NOT add None to the tenants set."""
        client, store = _make_store()
        await store.save(_make_entry(kid="gl", tenant_id=None))
        members = await client.smembers("varco:enc:tenants")
        assert b"None" not in members and b"__global__" not in members

    async def test_save_preserves_all_fields(self) -> None:
        client, store = _make_store()
        entry = _make_entry(kid="full", tenant_id="t1", is_primary=False, wrapped=True)
        await store.save(entry)
        loaded = await store.load("full")
        assert loaded is not None
        assert loaded.algorithm == "fernet"
        assert loaded.tenant_id == "t1"
        assert loaded.is_primary is False
        assert loaded.wrapped is True

    async def test_save_upserts_existing(self) -> None:
        import dataclasses

        client, store = _make_store()
        entry = _make_entry(kid="u1", is_primary=True)
        await store.save(entry)
        demoted = dataclasses.replace(entry, is_primary=False)
        await store.save(demoted)
        loaded = await store.load("u1")
        assert loaded is not None
        assert loaded.is_primary is False


# ════════════════════════════════════════════════════════════════════════════════
# load
# ════════════════════════════════════════════════════════════════════════════════


class TestRedisEncryptionKeyStoreLoad:
    async def test_load_returns_none_when_missing(self) -> None:
        _, store = _make_store()
        assert await store.load("ghost") is None

    async def test_load_round_trip(self) -> None:
        _, store = _make_store()
        entry = _make_entry(kid="rt1")
        await store.save(entry)
        loaded = await store.load("rt1")
        assert loaded is not None
        assert loaded.kid == entry.kid
        assert loaded.key_material == entry.key_material

    async def test_load_global_key(self) -> None:
        _, store = _make_store()
        entry = _make_entry(kid="gl", tenant_id=None)
        await store.save(entry)
        loaded = await store.load("gl")
        assert loaded is not None
        assert loaded.tenant_id is None

    async def test_load_created_at_is_datetime(self) -> None:
        _, store = _make_store()
        entry = _make_entry(kid="dt1")
        await store.save(entry)
        loaded = await store.load("dt1")
        assert loaded is not None
        assert isinstance(loaded.created_at, datetime)


# ════════════════════════════════════════════════════════════════════════════════
# load_for_tenant
# ════════════════════════════════════════════════════════════════════════════════


class TestRedisEncryptionKeyStoreLoadForTenant:
    async def test_returns_only_matching_tenant(self) -> None:
        _, store = _make_store()
        await store.save(_make_entry(kid="a1", tenant_id="acme"))
        await store.save(_make_entry(kid="g1", tenant_id="globex"))
        result = await store.load_for_tenant("acme")
        assert len(result) == 1
        assert result[0].kid == "a1"

    async def test_returns_global_keys(self) -> None:
        _, store = _make_store()
        await store.save(_make_entry(kid="gl", tenant_id=None))
        await store.save(_make_entry(kid="t1", tenant_id="acme"))
        result = await store.load_for_tenant(None)
        assert len(result) == 1
        assert result[0].tenant_id is None

    async def test_empty_when_no_entries(self) -> None:
        _, store = _make_store()
        assert await store.load_for_tenant("nobody") == []

    async def test_sorted_by_created_at(self) -> None:
        _, store = _make_store()
        newer = _make_entry(kid="newer", tenant_id="t", offset_seconds=10)
        older = _make_entry(kid="older", tenant_id="t", offset_seconds=0)
        await store.save(newer)
        await store.save(older)
        result = await store.load_for_tenant("t")
        assert [e.kid for e in result] == ["older", "newer"]

    async def test_inconsistent_set_member_silently_skipped(self) -> None:
        """
        An orphaned kid in the tenant Set (no corresponding Hash) must be
        skipped — not raise.  This handles crashed-mid-write scenarios.
        """
        client, store = _make_store()
        # Add a kid to the tenant Set without creating the Hash
        await client.sadd("varco:enc:tenant:broken", "ghost-kid")
        # Real entry
        await store.save(_make_entry(kid="real", tenant_id="broken"))
        result = await store.load_for_tenant("broken")
        # Only the real entry is returned; ghost is silently skipped
        assert len(result) == 1
        assert result[0].kid == "real"


# ════════════════════════════════════════════════════════════════════════════════
# list_tenants
# ════════════════════════════════════════════════════════════════════════════════


class TestRedisEncryptionKeyStoreListTenants:
    async def test_returns_distinct_sorted(self) -> None:
        _, store = _make_store()
        await store.save(_make_entry(kid="k1", tenant_id="zebra"))
        await store.save(_make_entry(kid="k2", tenant_id="acme"))
        await store.save(_make_entry(kid="k3", tenant_id="acme"))
        await store.save(_make_entry(kid="k4", tenant_id=None))
        tenants = await store.list_tenants()
        assert tenants == ["acme", "zebra"]

    async def test_empty_store(self) -> None:
        _, store = _make_store()
        assert await store.list_tenants() == []

    async def test_global_keys_excluded(self) -> None:
        _, store = _make_store()
        await store.save(_make_entry(kid="gl", tenant_id=None))
        assert await store.list_tenants() == []


# ════════════════════════════════════════════════════════════════════════════════
# delete
# ════════════════════════════════════════════════════════════════════════════════


class TestRedisEncryptionKeyStoreDelete:
    async def test_delete_removes_hash(self) -> None:
        client, store = _make_store()
        entry = _make_entry(kid="del1")
        await store.save(entry)
        await store.delete("del1")
        raw = await client.hgetall("varco:enc:key:del1")
        assert raw == {}

    async def test_delete_removes_from_tenant_set(self) -> None:
        client, store = _make_store()
        entry = _make_entry(kid="del2", tenant_id="acme")
        await store.save(entry)
        await store.delete("del2")
        members = await client.smembers("varco:enc:tenant:acme")
        assert b"del2" not in members

    async def test_delete_removes_tenant_from_global_set_when_empty(self) -> None:
        client, store = _make_store()
        entry = _make_entry(kid="del3", tenant_id="solo")
        await store.save(entry)
        await store.delete("del3")
        # "solo" should be gone from the global tenants set
        tenants = await client.smembers("varco:enc:tenants")
        assert b"solo" not in tenants

    async def test_delete_keeps_tenant_in_global_set_when_others_remain(self) -> None:
        client, store = _make_store()
        await store.save(_make_entry(kid="del4a", tenant_id="acme"))
        await store.save(_make_entry(kid="del4b", tenant_id="acme"))
        await store.delete("del4a")
        # "acme" still has del4b — must remain in global tenants set
        tenants = await client.smembers("varco:enc:tenants")
        assert b"acme" in tenants

    async def test_delete_noop_when_missing(self) -> None:
        """No-op — must not raise."""
        _, store = _make_store()
        await store.delete("ghost")

    async def test_delete_global_key_removes_from_global_set(self) -> None:
        client, store = _make_store()
        await store.save(_make_entry(kid="gl", tenant_id=None))
        await store.delete("gl")
        # Global key removed from :global set
        members = await client.smembers("varco:enc:global")
        assert b"gl" not in members


# ════════════════════════════════════════════════════════════════════════════════
# _repair_tenant_index
# ════════════════════════════════════════════════════════════════════════════════


class TestRedisEncryptionKeyStoreRepairIndex:
    async def test_repair_rebuilds_tenant_set(self) -> None:
        """
        Simulate a crash: entry exists in Hash but Set is empty.
        ``_repair_tenant_index`` must rebuild the Set from scanning Hashes.
        """
        client, store = _make_store()
        # Save normally (creates Hash + Set)
        await store.save(_make_entry(kid="r1", tenant_id="acme"))
        # Manually remove from the tenant Set (simulate crash mid-write)
        await client.srem("varco:enc:tenant:acme", "r1")
        # Set is now inconsistent
        assert b"r1" not in await client.smembers("varco:enc:tenant:acme")

        # Repair must rebuild it
        await store._repair_tenant_index()
        assert b"r1" in await client.smembers("varco:enc:tenant:acme")

    async def test_repair_handles_global_keys(self) -> None:
        client, store = _make_store()
        await store.save(_make_entry(kid="gl", tenant_id=None))
        # Remove from global Set
        await client.srem("varco:enc:global", "gl")
        await store._repair_tenant_index()
        members = await client.smembers("varco:enc:global")
        assert b"gl" in members

    async def test_repair_tolerates_malformed_hash(self) -> None:
        """A malformed entry (missing required fields) must be silently skipped."""
        client, store = _make_store()
        # Insert a malformed hash with missing required fields
        await client.hset("varco:enc:key:bad", mapping={"kid": "bad"})
        # Must not raise
        await store._repair_tenant_index()


# ════════════════════════════════════════════════════════════════════════════════
# Protocol conformance
# ════════════════════════════════════════════════════════════════════════════════


class TestRedisEncryptionKeyStoreProtocol:
    def test_isinstance_check(self) -> None:
        _, store = _make_store()
        assert isinstance(store, EncryptionKeyStore)


# ════════════════════════════════════════════════════════════════════════════════
# Integration tests — real Redis (requires Docker)
# ════════════════════════════════════════════════════════════════════════════════


if not os.environ.get("VARCO_RUN_INTEGRATION"):
    _skip_integration = True
else:
    _skip_integration = False


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("VARCO_RUN_INTEGRATION"),
    reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
)
class TestRedisEncryptionKeyStoreIntegration:
    """
    End-to-end tests against a real Redis instance.

    DISABLED BY DEFAULT — requires Docker + testcontainers[redis].

    Run with::

        VARCO_RUN_INTEGRATION=1 pytest varco_redis/tests/test_redis_encryption_store.py -m integration

    What these tests verify that unit tests (FakeRedis) cannot
    ----------------------------------------------------------
    - Real ``HSET`` / ``HGETALL`` / ``SADD`` / ``SMEMBERS`` behaviour.
    - Bytes vs str handling from real redis.asyncio client.
    - Connection pool behaviour and error propagation.
    """

    @pytest.fixture(scope="class")
    def redis_container(self):
        from testcontainers.redis import RedisContainer

        with RedisContainer() as r:
            yield r

    @pytest_asyncio.fixture
    async def real_store(self, redis_container):
        import redis.asyncio as aioredis

        host = redis_container.get_container_host_ip()
        port = redis_container.get_exposed_port(6379)
        url = f"redis://{host}:{port}/0"
        client = aioredis.from_url(url, decode_responses=False)
        store = RedisEncryptionKeyStore(client, prefix=f"test:{id(self)}")
        yield store
        # Flush only our test namespace
        pattern = f"{store._prefix}:*"
        async for key in client.scan_iter(pattern):
            await client.delete(key)
        await client.aclose()

    async def test_full_lifecycle(self, real_store: RedisEncryptionKeyStore) -> None:
        """Save → load → load_for_tenant → list_tenants → delete."""

        entry = _make_entry(kid="int-1", tenant_id="int-acme")
        await real_store.save(entry)

        loaded = await real_store.load("int-1")
        assert loaded is not None
        assert loaded.kid == "int-1"
        assert loaded.tenant_id == "int-acme"

        tenant_entries = await real_store.load_for_tenant("int-acme")
        assert len(tenant_entries) == 1

        tenants = await real_store.list_tenants()
        assert "int-acme" in tenants

        await real_store.delete("int-1")
        assert await real_store.load("int-1") is None

    async def test_upsert_via_save(self, real_store: RedisEncryptionKeyStore) -> None:
        import dataclasses

        entry = _make_entry(kid="int-u", tenant_id="int-t", is_primary=True)
        await real_store.save(entry)
        demoted = dataclasses.replace(entry, is_primary=False)
        await real_store.save(demoted)
        loaded = await real_store.load("int-u")
        assert loaded is not None
        assert loaded.is_primary is False
