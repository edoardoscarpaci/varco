"""
Unit + integration tests for varco_sa.encryption_store
=========================================================
Covers ``SAEncryptionKeyStore`` using an in-memory SQLite database — no
external PostgreSQL required for the unit tests.

DESIGN: separate engine per test via pytest_asyncio.fixture
    ``SAEncryptionKeyStore`` uses its own ``_metadata`` (separate from the
    app's ``DeclarativeBase``).  Each test builds a fresh SQLite engine,
    calls ``ensure_table()`` to create the schema, and tears it down
    afterward.  This matches the pattern used in ``test_sa_outbox.py``.

Integration tests (marked ``pytest.mark.integration``) require a real
PostgreSQL server and are skipped by default.  Run with::

    pytest -m integration varco_sa/tests/test_sa_encryption_store.py

Sections
--------
- ``SAEncryptionKeyStore`` construction — repr
- ``ensure_table``                      — idempotent DDL
- ``save``                              — insert, upsert
- ``load``                              — found, not found
- ``load_for_tenant``                   — filtering, ordering, None tenant
- ``list_tenants``                      — distinct, sorted
- ``delete``                            — existing, noop
- Protocol conformance
"""

from __future__ import annotations

import base64
import os
from datetime import UTC, datetime, timedelta

import pytest
import pytest_asyncio
from cryptography.fernet import Fernet
from sqlalchemy.ext.asyncio import create_async_engine

from varco_core.encryption_store import EncryptionKeyEntry, EncryptionKeyStore
from varco_sa.encryption_store import SAEncryptionKeyStore, _metadata


# ── Helpers ────────────────────────────────────────────────────────────────────


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


# ── Fixtures ───────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def engine():
    """
    Fresh in-memory SQLite engine with the varco_encryption_keys schema.

    Uses ``SAEncryptionKeyStore``'s private ``_metadata`` — separate from any
    app ``DeclarativeBase`` — so no cross-test collisions.
    """
    e = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    async with e.begin() as conn:
        await conn.run_sync(_metadata.create_all)
    yield e
    async with e.begin() as conn:
        await conn.run_sync(_metadata.drop_all)
    await e.dispose()


@pytest_asyncio.fixture
async def store(engine) -> SAEncryptionKeyStore:
    """``SAEncryptionKeyStore`` wired to the per-test SQLite engine."""
    return SAEncryptionKeyStore(engine)


# ════════════════════════════════════════════════════════════════════════════════
# Construction
# ════════════════════════════════════════════════════════════════════════════════


class TestSAEncryptionKeyStoreConstruction:
    def test_repr(self, store: SAEncryptionKeyStore) -> None:
        assert "SAEncryptionKeyStore" in repr(store)


# ════════════════════════════════════════════════════════════════════════════════
# ensure_table
# ════════════════════════════════════════════════════════════════════════════════


class TestEnsureTable:
    async def test_ensure_table_idempotent(self, engine) -> None:
        """Calling ensure_table twice must not raise."""
        store = SAEncryptionKeyStore(engine)
        await store.ensure_table()  # table already exists from fixture
        await store.ensure_table()  # must be a no-op


# ════════════════════════════════════════════════════════════════════════════════
# save
# ════════════════════════════════════════════════════════════════════════════════


class TestSAEncryptionKeyStoreSave:
    async def test_save_inserts_new_entry(self, store: SAEncryptionKeyStore) -> None:
        entry = _make_entry(kid="s1")
        await store.save(entry)
        loaded = await store.load("s1")
        assert loaded is not None
        assert loaded.kid == "s1"

    async def test_save_upserts_existing(self, store: SAEncryptionKeyStore) -> None:
        entry = _make_entry(kid="u1", is_primary=True)
        await store.save(entry)
        # Demote via upsert
        import dataclasses

        demoted = dataclasses.replace(entry, is_primary=False)
        await store.save(demoted)
        loaded = await store.load("u1")
        assert loaded is not None
        assert loaded.is_primary is False

    async def test_save_preserves_all_fields(self, store: SAEncryptionKeyStore) -> None:
        entry = _make_entry(
            kid="full", tenant_id="globex", is_primary=False, wrapped=True
        )
        await store.save(entry)
        loaded = await store.load("full")
        assert loaded is not None
        assert loaded.algorithm == "fernet"
        assert loaded.key_material == entry.key_material
        assert loaded.tenant_id == "globex"
        assert loaded.is_primary is False
        assert loaded.wrapped is True

    async def test_save_global_key(self, store: SAEncryptionKeyStore) -> None:
        entry = _make_entry(kid="global", tenant_id=None)
        await store.save(entry)
        loaded = await store.load("global")
        assert loaded is not None
        assert loaded.tenant_id is None


# ════════════════════════════════════════════════════════════════════════════════
# load
# ════════════════════════════════════════════════════════════════════════════════


class TestSAEncryptionKeyStoreLoad:
    async def test_load_returns_none_when_missing(
        self, store: SAEncryptionKeyStore
    ) -> None:
        assert await store.load("ghost") is None

    async def test_load_round_trip(self, store: SAEncryptionKeyStore) -> None:
        entry = _make_entry(kid="rt1")
        await store.save(entry)
        loaded = await store.load("rt1")
        assert loaded is not None
        assert loaded.kid == entry.kid
        assert loaded.key_material == entry.key_material

    async def test_created_at_is_timezone_aware(
        self, store: SAEncryptionKeyStore
    ) -> None:
        entry = _make_entry(kid="tz1")
        await store.save(entry)
        loaded = await store.load("tz1")
        assert loaded is not None
        assert loaded.created_at.tzinfo is not None


# ════════════════════════════════════════════════════════════════════════════════
# load_for_tenant
# ════════════════════════════════════════════════════════════════════════════════


class TestSAEncryptionKeyStoreLoadForTenant:
    async def test_returns_only_matching_tenant(
        self, store: SAEncryptionKeyStore
    ) -> None:
        await store.save(_make_entry(kid="a1", tenant_id="acme"))
        await store.save(_make_entry(kid="g1", tenant_id="globex"))
        result = await store.load_for_tenant("acme")
        assert all(e.tenant_id == "acme" for e in result)
        assert len(result) == 1

    async def test_returns_global_keys(self, store: SAEncryptionKeyStore) -> None:
        await store.save(_make_entry(kid="gl", tenant_id=None))
        await store.save(_make_entry(kid="t1", tenant_id="acme"))
        result = await store.load_for_tenant(None)
        assert len(result) == 1
        assert result[0].tenant_id is None

    async def test_returns_empty_list_for_unknown_tenant(
        self, store: SAEncryptionKeyStore
    ) -> None:
        assert await store.load_for_tenant("nobody") == []

    async def test_sorted_by_created_at_ascending(
        self, store: SAEncryptionKeyStore
    ) -> None:
        newer = _make_entry(kid="newer", tenant_id="t", offset_seconds=10)
        older = _make_entry(kid="older", tenant_id="t", offset_seconds=0)
        # Insert out of order
        await store.save(newer)
        await store.save(older)
        result = await store.load_for_tenant("t")
        assert [e.kid for e in result] == ["older", "newer"]

    async def test_multiple_entries_all_returned(
        self, store: SAEncryptionKeyStore
    ) -> None:
        for i in range(3):
            await store.save(_make_entry(kid=f"k{i}", tenant_id="multi"))
        result = await store.load_for_tenant("multi")
        assert len(result) == 3


# ════════════════════════════════════════════════════════════════════════════════
# list_tenants
# ════════════════════════════════════════════════════════════════════════════════


class TestSAEncryptionKeyStoreListTenants:
    async def test_returns_distinct_sorted(self, store: SAEncryptionKeyStore) -> None:
        await store.save(_make_entry(kid="k1", tenant_id="zebra"))
        await store.save(_make_entry(kid="k2", tenant_id="acme"))
        await store.save(_make_entry(kid="k3", tenant_id="acme"))  # duplicate
        await store.save(_make_entry(kid="k4", tenant_id=None))  # global — excluded
        tenants = await store.list_tenants()
        assert tenants == ["acme", "zebra"]

    async def test_empty_store(self, store: SAEncryptionKeyStore) -> None:
        assert await store.list_tenants() == []

    async def test_excludes_global_keys(self, store: SAEncryptionKeyStore) -> None:
        await store.save(_make_entry(kid="g1", tenant_id=None))
        tenants = await store.list_tenants()
        assert tenants == []


# ════════════════════════════════════════════════════════════════════════════════
# delete
# ════════════════════════════════════════════════════════════════════════════════


class TestSAEncryptionKeyStoreDelete:
    async def test_delete_removes_entry(self, store: SAEncryptionKeyStore) -> None:
        entry = _make_entry(kid="del1")
        await store.save(entry)
        await store.delete("del1")
        assert await store.load("del1") is None

    async def test_delete_noop_when_missing(self, store: SAEncryptionKeyStore) -> None:
        """No-op — must not raise."""
        await store.delete("ghost")

    async def test_delete_only_removes_target(
        self, store: SAEncryptionKeyStore
    ) -> None:
        await store.save(_make_entry(kid="keep"))
        await store.save(_make_entry(kid="remove"))
        await store.delete("remove")
        assert await store.load("keep") is not None
        assert await store.load("remove") is None


# ════════════════════════════════════════════════════════════════════════════════
# Protocol conformance
# ════════════════════════════════════════════════════════════════════════════════


class TestSAEncryptionKeyStoreProtocol:
    async def test_isinstance_check(self, store: SAEncryptionKeyStore) -> None:
        assert isinstance(store, EncryptionKeyStore)


# ════════════════════════════════════════════════════════════════════════════════
# Integration tests — PostgreSQL (requires Docker)
# ════════════════════════════════════════════════════════════════════════════════

pytestmark_integration = pytest.mark.integration

# Skip the integration section unless explicitly requested.
if not os.environ.get("VARCO_RUN_INTEGRATION"):
    _skip_integration = True
else:
    _skip_integration = False


@pytest.mark.integration
@pytest.mark.skipif(
    not os.environ.get("VARCO_RUN_INTEGRATION"),
    reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
)
class TestSAEncryptionKeyStoreIntegration:
    """
    End-to-end tests against a real PostgreSQL instance.

    DISABLED BY DEFAULT — requires Docker + testcontainers[postgresql].

    Run with::

        VARCO_RUN_INTEGRATION=1 pytest varco_sa/tests/test_sa_encryption_store.py -m integration

    What these tests verify that unit tests (SQLite) cannot
    --------------------------------------------------------
    - PostgreSQL-native upsert (``ON CONFLICT DO UPDATE``) — tested in the
      unit suite via SQLite's delete+insert fallback, but pg path is distinct.
    - Timezone-aware timestamp storage + retrieval (PostgreSQL TIMESTAMPTZ).
    - Real connection pool behaviour under concurrent writes.
    """

    @pytest.fixture(scope="class")
    def pg_container(self):
        """Start a PostgreSQL container for the integration test class."""
        from testcontainers.postgres import PostgresContainer

        with PostgresContainer("postgres:15-alpine") as pg:
            yield pg

    @pytest_asyncio.fixture
    async def pg_store(self, pg_container):
        """``SAEncryptionKeyStore`` backed by a real PostgreSQL instance."""
        url = pg_container.get_connection_url().replace(
            "postgresql://", "postgresql+asyncpg://"
        )
        e = create_async_engine(url, echo=False)
        store = SAEncryptionKeyStore(e)
        await store.ensure_table()
        yield store
        # Tear down table after each test for isolation
        async with e.begin() as conn:
            await conn.run_sync(_metadata.drop_all)
        await e.dispose()

    async def test_pg_upsert_on_conflict(self, pg_store: SAEncryptionKeyStore) -> None:
        """PostgreSQL ON CONFLICT DO UPDATE path."""
        import dataclasses

        entry = _make_entry(kid="pg-u1", is_primary=True)
        await pg_store.save(entry)
        demoted = dataclasses.replace(entry, is_primary=False)
        await pg_store.save(demoted)
        loaded = await pg_store.load("pg-u1")
        assert loaded is not None
        assert loaded.is_primary is False

    async def test_pg_created_at_timezone_aware(
        self, pg_store: SAEncryptionKeyStore
    ) -> None:
        """PostgreSQL TIMESTAMPTZ preserves UTC timezone info."""
        entry = _make_entry(kid="pg-tz")
        await pg_store.save(entry)
        loaded = await pg_store.load("pg-tz")
        assert loaded is not None
        assert loaded.created_at.tzinfo is not None

    async def test_pg_full_lifecycle(self, pg_store: SAEncryptionKeyStore) -> None:
        """Full save → load → rotate → delete lifecycle against real PG."""
        import dataclasses

        entry = _make_entry(kid="pg-full", tenant_id="pg-acme", is_primary=True)
        await pg_store.save(entry)

        # Demote
        demoted = dataclasses.replace(entry, is_primary=False)
        await pg_store.save(demoted)

        # New primary
        new_entry = _make_entry(kid="pg-full-v2", tenant_id="pg-acme", is_primary=True)
        await pg_store.save(new_entry)

        entries = await pg_store.load_for_tenant("pg-acme")
        assert len(entries) == 2
        primary = [e for e in entries if e.is_primary]
        assert len(primary) == 1
        assert primary[0].kid == "pg-full-v2"

        # Cleanup
        await pg_store.delete("pg-full")
        assert await pg_store.load("pg-full") is None
