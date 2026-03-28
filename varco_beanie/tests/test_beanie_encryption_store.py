"""
Unit + integration tests for varco_beanie.encryption_store
============================================================
Covers ``BeanieEncryptionKeyStore`` and ``EncryptionKeyDocument``.

Unit tests (no MongoDB required)
---------------------------------
Beanie Document class methods are patched with ``AsyncMock`` / ``MagicMock``
so no ``init_beanie()`` call is needed.  The conftest ``bypass_beanie_collection_check``
fixture (autouse) allows instantiating ``EncryptionKeyDocument`` without a
live collection.

Integration tests (``pytest.mark.integration``)
------------------------------------------------
Require Docker + testcontainers[mongodb].  A real MongoDB container is
started once per class.  Each test gets a fresh database via a unique name.
Run with::

    VARCO_RUN_INTEGRATION=1 pytest varco_beanie/tests/test_beanie_encryption_store.py -m integration

Sections
--------
- ``EncryptionKeyDocument``         — collection name, field defaults, repr
- ``BeanieEncryptionKeyStore``      — repr
- ``save``                          — calls doc.save(); field mapping
- ``load``                          — get() returns entry or None
- ``load_for_tenant``               — tenant filter, None tenant, sorting
- ``list_tenants``                  — distinct non-null tenant_ids
- ``delete``                        — get() + delete()
- Protocol conformance
- Integration tests                 — full lifecycle against real MongoDB
"""

from __future__ import annotations
import os as _os
import base64
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio
from cryptography.fernet import Fernet

from varco_beanie.encryption_store import (
    BeanieEncryptionKeyStore,
    EncryptionKeyDocument,
    _doc_to_entry,
)
from varco_core.encryption_store import EncryptionKeyEntry, EncryptionKeyStore


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


def _make_doc(entry: EncryptionKeyEntry) -> EncryptionKeyDocument:
    """Build an ``EncryptionKeyDocument`` from an ``EncryptionKeyEntry``."""
    return EncryptionKeyDocument(
        id=entry.kid,
        algorithm=entry.algorithm,
        key_material=entry.key_material,
        created_at=entry.created_at,
        tenant_id=entry.tenant_id,
        is_primary=entry.is_primary,
        wrapped=entry.wrapped,
    )


# ════════════════════════════════════════════════════════════════════════════════
# EncryptionKeyDocument
# ════════════════════════════════════════════════════════════════════════════════


class TestEncryptionKeyDocument:
    def test_collection_name(self) -> None:
        assert EncryptionKeyDocument.Settings.name == "varco_encryption_keys"

    def test_id_is_kid(self) -> None:
        """The ``id`` field (MongoDB ``_id``) is set to the kid value."""
        entry = _make_entry(kid="my-kid")
        doc = _make_doc(entry)
        assert doc.id == "my-kid"

    def test_default_is_primary_true(self) -> None:
        doc = EncryptionKeyDocument(
            id="k",
            algorithm="fernet",
            key_material="abc",
            created_at=datetime.now(UTC),
        )
        assert doc.is_primary is True

    def test_default_wrapped_false(self) -> None:
        doc = EncryptionKeyDocument(
            id="k",
            algorithm="fernet",
            key_material="abc",
            created_at=datetime.now(UTC),
        )
        assert doc.wrapped is False

    def test_tenant_id_defaults_none(self) -> None:
        doc = EncryptionKeyDocument(
            id="k",
            algorithm="fernet",
            key_material="abc",
            created_at=datetime.now(UTC),
        )
        assert doc.tenant_id is None

    def test_repr_contains_key_info(self) -> None:
        doc = _make_doc(_make_entry(kid="repr-k", tenant_id="acme"))
        r = repr(doc)
        assert "EncryptionKeyDocument" in r
        assert "repr-k" in r


# ════════════════════════════════════════════════════════════════════════════════
# _doc_to_entry helper
# ════════════════════════════════════════════════════════════════════════════════


class TestDocToEntry:
    def test_round_trip(self) -> None:
        entry = _make_entry(
            kid="rt", tenant_id="globex", is_primary=False, wrapped=True
        )
        doc = _make_doc(entry)
        restored = _doc_to_entry(doc)
        assert restored.kid == entry.kid
        assert restored.tenant_id == entry.tenant_id
        assert restored.is_primary is False
        assert restored.wrapped is True

    def test_created_at_timezone_aware(self) -> None:
        """Naive ``created_at`` (pymongo may return naive UTC) is patched to UTC-aware."""
        entry = _make_entry(kid="tz")
        doc = _make_doc(entry)
        # Simulate pymongo stripping tzinfo
        object.__setattr__(doc, "created_at", doc.created_at.replace(tzinfo=None))
        result = _doc_to_entry(doc)
        assert result.created_at.tzinfo is not None

    def test_none_tenant_preserved(self) -> None:
        entry = _make_entry(kid="gl", tenant_id=None)
        doc = _make_doc(entry)
        result = _doc_to_entry(doc)
        assert result.tenant_id is None


# ════════════════════════════════════════════════════════════════════════════════
# BeanieEncryptionKeyStore — unit tests (mocked Beanie)
# ════════════════════════════════════════════════════════════════════════════════


class TestBeanieEncryptionKeyStoreUnit:
    """Unit tests that patch Beanie Document methods to avoid MongoDB."""

    def test_repr(self) -> None:
        store = BeanieEncryptionKeyStore()
        assert "BeanieEncryptionKeyStore" in repr(store)

    def test_isinstance_protocol(self) -> None:
        store = BeanieEncryptionKeyStore()
        assert isinstance(store, EncryptionKeyStore)

    async def test_save_calls_doc_save(self) -> None:
        store = BeanieEncryptionKeyStore()
        entry = _make_entry(kid="s1")
        mock_save = AsyncMock()
        with patch.object(EncryptionKeyDocument, "save", mock_save):
            await store.save(entry)
        mock_save.assert_awaited_once()

    async def test_load_returns_entry_when_found(self) -> None:
        store = BeanieEncryptionKeyStore()
        entry = _make_entry(kid="l1")
        doc = _make_doc(entry)
        with patch.object(EncryptionKeyDocument, "get", AsyncMock(return_value=doc)):
            result = await store.load("l1")
        assert result is not None
        assert result.kid == "l1"

    async def test_load_returns_none_when_not_found(self) -> None:
        store = BeanieEncryptionKeyStore()
        with patch.object(EncryptionKeyDocument, "get", AsyncMock(return_value=None)):
            result = await store.load("ghost")
        assert result is None

    async def test_delete_calls_doc_delete_when_found(self) -> None:
        store = BeanieEncryptionKeyStore()
        # Use a MagicMock for doc so we can set delete freely without Pydantic's
        # field validation blocking attribute assignment.
        doc = MagicMock()
        doc.id = "d1"
        doc.delete = AsyncMock()
        with patch.object(EncryptionKeyDocument, "get", AsyncMock(return_value=doc)):
            await store.delete("d1")
        doc.delete.assert_awaited_once()

    async def test_delete_noop_when_not_found(self) -> None:
        """When ``get()`` returns None, ``delete()`` must not raise."""
        store = BeanieEncryptionKeyStore()
        with patch.object(EncryptionKeyDocument, "get", AsyncMock(return_value=None)):
            await store.delete("ghost")  # must not raise

    async def test_load_for_tenant_returns_entries(self) -> None:
        store = BeanieEncryptionKeyStore()
        entries = [_make_entry(kid=f"k{i}", tenant_id="acme") for i in range(2)]
        docs = [_make_doc(e) for e in entries]

        # Mock the chained find({"tenant_id": ...}).sort("+created_at").to_list() call.
        # sort() is called with a string "+created_at" and returns a FindMany-like object
        # whose to_list() is the coroutine we care about.
        mock_to_list = AsyncMock(return_value=docs)
        mock_sorted = MagicMock()
        mock_sorted.to_list = mock_to_list
        mock_find = MagicMock()
        mock_find.sort.return_value = mock_sorted

        with patch.object(EncryptionKeyDocument, "find", return_value=mock_find):
            result = await store.load_for_tenant("acme")

        assert len(result) == 2

    async def test_load_for_tenant_none_uses_null_filter(self) -> None:
        """
        ``load_for_tenant(None)`` must pass ``{"tenant_id": None}`` as the
        filter dict to ``EncryptionKeyDocument.find()``.
        """
        store = BeanieEncryptionKeyStore()
        mock_to_list = AsyncMock(return_value=[])
        mock_sorted = MagicMock()
        mock_sorted.to_list = mock_to_list
        mock_find = MagicMock()
        mock_find.sort.return_value = mock_sorted

        with patch.object(
            EncryptionKeyDocument, "find", return_value=mock_find
        ) as mock_f:
            await store.load_for_tenant(None)
        # find() called once with the None-tenant filter dict
        mock_f.assert_called_once_with({"tenant_id": None})

    async def test_list_tenants_returns_sorted_distinct(self) -> None:
        store = BeanieEncryptionKeyStore()
        docs = [
            _make_doc(_make_entry(kid="k1", tenant_id="zebra")),
            _make_doc(_make_entry(kid="k2", tenant_id="acme")),
            _make_doc(_make_entry(kid="k3", tenant_id="acme")),  # duplicate
        ]
        # Updated store uses find({"tenant_id": {"$ne": None}}).to_list()
        mock_to_list = AsyncMock(return_value=docs)
        mock_find = MagicMock()
        mock_find.to_list = mock_to_list

        with patch.object(EncryptionKeyDocument, "find", return_value=mock_find):
            tenants = await store.list_tenants()

        assert tenants == ["acme", "zebra"]


# ════════════════════════════════════════════════════════════════════════════════
# Integration tests — real MongoDB (requires Docker)
# ════════════════════════════════════════════════════════════════════════════════


if not _os.environ.get("VARCO_RUN_INTEGRATION"):
    _skip_integration = True
else:
    _skip_integration = False


@pytest.mark.integration
@pytest.mark.skipif(
    not _os.environ.get("VARCO_RUN_INTEGRATION"),
    reason="Integration tests disabled — set VARCO_RUN_INTEGRATION=1",
)
class TestBeanieEncryptionKeyStoreIntegration:
    """
    End-to-end tests against a real MongoDB instance.

    DISABLED BY DEFAULT — requires Docker + testcontainers[mongodb].

    Run with::

        VARCO_RUN_INTEGRATION=1 pytest varco_beanie/tests/test_beanie_encryption_store.py -m integration

    What these tests verify that unit tests (mocked) cannot
    -------------------------------------------------------
    - Real MongoDB CRUD round-trips.
    - Beanie Document upsert behaviour (save() with pre-set id).
    - Query compilation for tenant filtering (None vs str tenant_id).
    - Ordering via ``sort(+created_at)`` on a real index.
    - ``list_tenants()`` deduplication across real documents.
    """

    @pytest.fixture(scope="class")
    def mongo_container(self):
        """Start a MongoDB container for the integration test class."""
        from testcontainers.mongodb import MongoDbContainer

        with MongoDbContainer() as mongo:
            yield mongo

    @pytest_asyncio.fixture
    async def beanie_store(self, mongo_container):
        """
        Initialise Beanie with ``EncryptionKeyDocument`` and return a
        fresh ``BeanieEncryptionKeyStore``.

        Each test gets an isolated database via a unique name.
        """
        from beanie import init_beanie
        from pymongo import AsyncMongoClient

        db_name = f"test_enc_{uuid.uuid4().hex[:8]}"
        connection_string = mongo_container.get_connection_url()
        client = AsyncMongoClient(connection_string)

        await init_beanie(
            database=client[db_name],
            document_models=[EncryptionKeyDocument],
        )

        store = BeanieEncryptionKeyStore()
        yield store

        await client.drop_database(db_name)
        await client.close()

    async def test_save_and_load(self, beanie_store: BeanieEncryptionKeyStore) -> None:
        """Insert an entry and retrieve it by kid."""
        entry = _make_entry(kid="int-s1")
        await beanie_store.save(entry)
        loaded = await beanie_store.load("int-s1")
        assert loaded is not None
        assert loaded.kid == "int-s1"
        assert loaded.key_material == entry.key_material

    async def test_load_returns_none_for_missing(
        self, beanie_store: BeanieEncryptionKeyStore
    ) -> None:
        result = await beanie_store.load("ghost-kid")
        assert result is None

    async def test_save_upserts_existing(
        self, beanie_store: BeanieEncryptionKeyStore
    ) -> None:
        import dataclasses

        entry = _make_entry(kid="int-u1", is_primary=True)
        await beanie_store.save(entry)
        demoted = dataclasses.replace(entry, is_primary=False)
        await beanie_store.save(demoted)
        loaded = await beanie_store.load("int-u1")
        assert loaded is not None
        assert loaded.is_primary is False

    async def test_load_for_tenant_filters_correctly(
        self, beanie_store: BeanieEncryptionKeyStore
    ) -> None:
        await beanie_store.save(_make_entry(kid="int-a1", tenant_id="int-acme"))
        await beanie_store.save(_make_entry(kid="int-g1", tenant_id="int-globex"))
        result = await beanie_store.load_for_tenant("int-acme")
        assert all(e.tenant_id == "int-acme" for e in result)
        assert any(e.kid == "int-a1" for e in result)

    async def test_load_for_tenant_none_returns_global_keys(
        self, beanie_store: BeanieEncryptionKeyStore
    ) -> None:
        await beanie_store.save(_make_entry(kid="int-gl", tenant_id=None))
        result = await beanie_store.load_for_tenant(None)
        global_kids = [e.kid for e in result if e.tenant_id is None]
        assert "int-gl" in global_kids

    async def test_list_tenants_distinct_sorted(
        self, beanie_store: BeanieEncryptionKeyStore
    ) -> None:
        await beanie_store.save(_make_entry(kid="int-lt1", tenant_id="int-zebra"))
        await beanie_store.save(_make_entry(kid="int-lt2", tenant_id="int-acme"))
        await beanie_store.save(_make_entry(kid="int-lt3", tenant_id="int-acme"))
        tenants = await beanie_store.list_tenants()
        assert "int-acme" in tenants
        assert "int-zebra" in tenants
        assert tenants == sorted(tenants)

    async def test_delete_removes_entry(
        self, beanie_store: BeanieEncryptionKeyStore
    ) -> None:
        entry = _make_entry(kid="int-del1")
        await beanie_store.save(entry)
        await beanie_store.delete("int-del1")
        assert await beanie_store.load("int-del1") is None

    async def test_delete_noop_when_missing(
        self, beanie_store: BeanieEncryptionKeyStore
    ) -> None:
        await beanie_store.delete("int-ghost")  # must not raise

    async def test_full_lifecycle(self, beanie_store: BeanieEncryptionKeyStore) -> None:
        """
        Simulate the ``EncryptionKeyManager`` startup + rotation lifecycle.
        """
        from varco_core.encryption_store import EncryptionKeyManager

        # EncryptionKeyManager uses this store
        manager = EncryptionKeyManager(beanie_store)

        # Generate key for new tenant
        enc1 = await manager.get_or_create_encryptor("int-lifecycle")
        ct = enc1.encrypt(b"sensitive data")

        # Simulate restart — new manager, same store
        manager2 = EncryptionKeyManager(beanie_store)
        enc2 = await manager2.get_or_create_encryptor("int-lifecycle")
        assert enc2.decrypt(ct) == b"sensitive data"

        # Rotate
        new_enc = await manager2.rotate("int-lifecycle")
        ct2 = new_enc.encrypt(b"new data")
        assert new_enc.decrypt(ct2) == b"new data"

        # Old key still in store (not deleted)
        entries = await beanie_store.load_for_tenant("int-lifecycle")
        assert len(entries) == 2
        primaries = [e for e in entries if e.is_primary]
        assert len(primaries) == 1
