"""
Unit tests for varco_core.encryption_store
============================================
Covers ``EncryptionKeyEntry``, ``InMemoryEncryptionKeyStore``, and
``EncryptionKeyManager``.

No external dependencies required — all tests use the in-memory store.

Sections
--------
- ``EncryptionKeyEntry``         — construction, to_dict, from_dict, round-trip
- ``InMemoryEncryptionKeyStore`` — save, load, load_for_tenant, list_tenants, delete
- ``EncryptionKeyManager``       — get_or_create_encryptor, build_tenant_registry,
                                   rotate, envelope encryption (wrapped=True),
                                   cache behaviour
"""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta

import pytest
from cryptography.fernet import Fernet

from varco_core.encryption import FernetFieldEncryptor
from varco_core.encryption_store import (
    EncryptionKeyEntry,
    EncryptionKeyManager,
    EncryptionKeyStore,
    InMemoryEncryptionKeyStore,
)


# ── Helpers ────────────────────────────────────────────────────────────────────


def _make_entry(
    *,
    kid: str = "kid-1",
    tenant_id: str | None = "acme",
    is_primary: bool = True,
    wrapped: bool = False,
    offset_seconds: int = 0,
) -> EncryptionKeyEntry:
    """Build an ``EncryptionKeyEntry`` for testing."""
    key = Fernet.generate_key()
    raw = base64.urlsafe_b64decode(key)
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


def _make_encryptor() -> FernetFieldEncryptor:
    return FernetFieldEncryptor(Fernet.generate_key())


# ════════════════════════════════════════════════════════════════════════════════
# EncryptionKeyEntry
# ════════════════════════════════════════════════════════════════════════════════


class TestEncryptionKeyEntry:
    def test_fields_accessible(self) -> None:
        e = _make_entry(kid="k1", tenant_id="t1", is_primary=True, wrapped=False)
        assert e.kid == "k1"
        assert e.tenant_id == "t1"
        assert e.is_primary is True
        assert e.wrapped is False
        assert e.algorithm == "fernet"

    def test_frozen_immutable(self) -> None:
        """Entry is frozen — attribute assignment must raise."""
        e = _make_entry()
        with pytest.raises((AttributeError, TypeError)):
            e.kid = "new-kid"  # type: ignore[misc]

    def test_to_dict_round_trip(self) -> None:
        original = _make_entry(
            kid="rt-1", tenant_id="globex", is_primary=False, wrapped=True
        )
        d = original.to_dict()
        restored = EncryptionKeyEntry.from_dict(d)
        assert restored == original

    def test_to_dict_none_tenant(self) -> None:
        """``tenant_id=None`` serialises as ``None``, not a sentinel."""
        e = _make_entry(tenant_id=None)
        d = e.to_dict()
        assert d["tenant_id"] is None

    def test_from_dict_none_tenant(self) -> None:
        e = _make_entry(tenant_id=None)
        restored = EncryptionKeyEntry.from_dict(e.to_dict())
        assert restored.tenant_id is None

    def test_from_dict_created_at_utc_naive(self) -> None:
        """``from_dict`` normalises naive datetimes to UTC-aware."""
        data = _make_entry().to_dict()
        # Strip timezone info from the ISO string
        data["created_at"] = data["created_at"].replace("+00:00", "")  # type: ignore[union-attr]
        entry = EncryptionKeyEntry.from_dict(data)
        assert entry.created_at.tzinfo is not None

    def test_from_dict_z_suffix(self) -> None:
        """``from_dict`` handles 'Z' timezone suffix (Python < 3.11 fallback)."""
        data = _make_entry().to_dict()
        data["created_at"] = data["created_at"].replace("+00:00", "Z")  # type: ignore[union-attr]
        entry = EncryptionKeyEntry.from_dict(data)
        assert entry.created_at.tzinfo is not None

    def test_from_dict_with_datetime_object(self) -> None:
        """``from_dict`` accepts a datetime object directly (not just ISO string)."""
        now = datetime.now(UTC)
        data = _make_entry().to_dict()
        data["created_at"] = now
        entry = EncryptionKeyEntry.from_dict(data)
        assert entry.created_at == now

    def test_default_is_primary_true(self) -> None:
        """Default value for ``is_primary`` is ``True``."""
        e = _make_entry()
        d = e.to_dict()
        d.pop("is_primary")  # simulate missing field
        restored = EncryptionKeyEntry.from_dict(d)
        assert restored.is_primary is True

    def test_default_wrapped_false(self) -> None:
        """Default value for ``wrapped`` is ``False``."""
        e = _make_entry()
        d = e.to_dict()
        d.pop("wrapped")
        restored = EncryptionKeyEntry.from_dict(d)
        assert restored.wrapped is False


# ════════════════════════════════════════════════════════════════════════════════
# InMemoryEncryptionKeyStore
# ════════════════════════════════════════════════════════════════════════════════


class TestInMemoryEncryptionKeyStore:
    async def test_save_and_load(self) -> None:
        store = InMemoryEncryptionKeyStore()
        entry = _make_entry(kid="x1")
        await store.save(entry)
        loaded = await store.load("x1")
        assert loaded == entry

    async def test_load_missing_returns_none(self) -> None:
        store = InMemoryEncryptionKeyStore()
        assert await store.load("does-not-exist") is None

    async def test_save_upserts_existing(self) -> None:
        store = InMemoryEncryptionKeyStore()
        entry = _make_entry(kid="u1", is_primary=True)
        await store.save(entry)
        demoted = EncryptionKeyEntry(
            kid=entry.kid,
            algorithm=entry.algorithm,
            key_material=entry.key_material,
            created_at=entry.created_at,
            tenant_id=entry.tenant_id,
            is_primary=False,
            wrapped=entry.wrapped,
        )
        await store.save(demoted)
        loaded = await store.load("u1")
        assert loaded is not None
        assert loaded.is_primary is False

    async def test_delete_existing(self) -> None:
        store = InMemoryEncryptionKeyStore()
        entry = _make_entry(kid="d1")
        await store.save(entry)
        await store.delete("d1")
        assert await store.load("d1") is None

    async def test_delete_noop_if_missing(self) -> None:
        store = InMemoryEncryptionKeyStore()
        # Should not raise
        await store.delete("ghost")

    async def test_load_for_tenant_returns_only_matching(self) -> None:
        store = InMemoryEncryptionKeyStore()
        a1 = _make_entry(kid="a1", tenant_id="acme")
        a2 = _make_entry(kid="a2", tenant_id="acme")
        g1 = _make_entry(kid="g1", tenant_id="globex")
        await store.save(a1)
        await store.save(a2)
        await store.save(g1)
        result = await store.load_for_tenant("acme")
        kids = {e.kid for e in result}
        assert kids == {"a1", "a2"}

    async def test_load_for_tenant_none_returns_global(self) -> None:
        store = InMemoryEncryptionKeyStore()
        global_entry = _make_entry(kid="gl", tenant_id=None)
        tenant_entry = _make_entry(kid="t1", tenant_id="acme")
        await store.save(global_entry)
        await store.save(tenant_entry)
        result = await store.load_for_tenant(None)
        assert len(result) == 1
        assert result[0].kid == "gl"

    async def test_load_for_tenant_sorted_by_created_at(self) -> None:
        store = InMemoryEncryptionKeyStore()
        # Insert out of order — older entry second
        newer = _make_entry(kid="newer", tenant_id="t", offset_seconds=10)
        older = _make_entry(kid="older", tenant_id="t", offset_seconds=0)
        await store.save(newer)
        await store.save(older)
        result = await store.load_for_tenant("t")
        assert [e.kid for e in result] == ["older", "newer"]

    async def test_load_for_tenant_empty(self) -> None:
        store = InMemoryEncryptionKeyStore()
        result = await store.load_for_tenant("nonexistent")
        assert result == []

    async def test_list_tenants_distinct_sorted(self) -> None:
        store = InMemoryEncryptionKeyStore()
        await store.save(_make_entry(kid="k1", tenant_id="zebra"))
        await store.save(_make_entry(kid="k2", tenant_id="acme"))
        await store.save(_make_entry(kid="k3", tenant_id="acme"))  # duplicate tenant
        await store.save(_make_entry(kid="k4", tenant_id=None))  # global — excluded
        tenants = await store.list_tenants()
        assert tenants == ["acme", "zebra"]

    async def test_list_tenants_empty_store(self) -> None:
        store = InMemoryEncryptionKeyStore()
        assert await store.list_tenants() == []

    async def test_protocol_isinstance_check(self) -> None:
        """``InMemoryEncryptionKeyStore`` satisfies the ``EncryptionKeyStore`` Protocol."""
        store = InMemoryEncryptionKeyStore()
        assert isinstance(store, EncryptionKeyStore)

    def test_repr(self) -> None:
        store = InMemoryEncryptionKeyStore()
        assert "InMemoryEncryptionKeyStore" in repr(store)


# ════════════════════════════════════════════════════════════════════════════════
# EncryptionKeyManager
# ════════════════════════════════════════════════════════════════════════════════


class TestEncryptionKeyManager:
    async def test_get_or_create_generates_key_on_first_call(self) -> None:
        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        enc = await manager.get_or_create_encryptor("acme")
        assert enc is not None
        # Verify the key was saved to the store
        entries = await store.load_for_tenant("acme")
        assert len(entries) == 1
        assert entries[0].is_primary is True

    async def test_get_or_create_returns_same_on_second_call(self) -> None:
        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        enc1 = await manager.get_or_create_encryptor("acme")
        enc2 = await manager.get_or_create_encryptor("acme")
        # Same object from cache
        assert enc1 is enc2

    async def test_get_or_create_loads_existing_from_store(self) -> None:
        """A second manager (simulating pod restart) loads the existing key."""
        store = InMemoryEncryptionKeyStore()
        manager1 = EncryptionKeyManager(store)
        await manager1.get_or_create_encryptor("acme")

        # Simulate restart — new manager, same store
        manager2 = EncryptionKeyManager(store)
        enc = await manager2.get_or_create_encryptor("acme")

        # Verify it can decrypt data encrypted by manager1
        enc1 = await manager1.get_or_create_encryptor("acme")
        ct = enc1.encrypt(b"hello")
        assert enc.decrypt(ct) == b"hello"

    async def test_get_or_create_global_key(self) -> None:
        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        enc = await manager.get_or_create_encryptor(None)
        assert enc is not None
        entries = await store.load_for_tenant(None)
        assert len(entries) == 1
        assert entries[0].tenant_id is None

    async def test_different_tenants_get_different_keys(self) -> None:
        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        enc_acme = await manager.get_or_create_encryptor("acme")
        enc_globex = await manager.get_or_create_encryptor("globex")
        # Different encryptors — acme ciphertext cannot be decrypted with globex key
        ct = enc_acme.encrypt(b"secret")
        from varco_core.encryption import EncryptionError

        with pytest.raises(EncryptionError):
            enc_globex.decrypt(ct)

    async def test_rotate_creates_new_primary(self) -> None:
        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        await manager.get_or_create_encryptor("acme")
        original_entries = await store.load_for_tenant("acme")
        old_kid = original_entries[0].kid

        new_enc = await manager.rotate("acme")
        assert new_enc is not None

        all_entries = await store.load_for_tenant("acme")
        primary = [e for e in all_entries if e.is_primary]
        non_primary = [e for e in all_entries if not e.is_primary]

        assert len(primary) == 1
        assert len(non_primary) == 1
        assert non_primary[0].kid == old_kid

    async def test_rotate_updates_cache(self) -> None:
        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        old_enc = await manager.get_or_create_encryptor("acme")
        new_enc = await manager.rotate("acme")
        # Subsequent call returns the rotated key
        cached = await manager.get_or_create_encryptor("acme")
        assert cached is new_enc
        assert cached is not old_enc

    async def test_rotate_no_primary_raises_key_error(self) -> None:
        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        with pytest.raises(KeyError, match="no primary key"):
            await manager.rotate("never-created")

    async def test_build_tenant_registry_empty_store(self) -> None:
        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        registry = await manager.build_tenant_registry()
        # Empty registry — unregistered tenant raises EncryptionError
        from varco_core.encryption import EncryptionError

        with pytest.raises(EncryptionError):
            registry.encrypt(b"x", context="nonexistent")

    async def test_build_tenant_registry_loads_all_tenants(self) -> None:
        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        await manager.get_or_create_encryptor("acme")
        await manager.get_or_create_encryptor("globex")

        # New manager for registry build (simulates startup)
        manager2 = EncryptionKeyManager(store)
        registry = await manager2.build_tenant_registry()

        # Both tenants should be registered
        ct = registry.encrypt(b"hello", context="acme")
        pt = registry.decrypt(ct, context="acme")
        assert pt == b"hello"

        ct2 = registry.encrypt(b"world", context="globex")
        pt2 = registry.decrypt(ct2, context="globex")
        assert pt2 == b"world"

    async def test_build_registry_skips_tenant_with_no_primary(self) -> None:
        """Tenants with only non-primary entries are not added to the registry."""
        store = InMemoryEncryptionKeyStore()
        # Manually save an entry with is_primary=False
        entry = _make_entry(kid="orphan", tenant_id="orphaned", is_primary=False)
        await store.save(entry)
        # Save the tenant_id to the "tenants" view via a separate save first
        # but we need list_tenants to include it — we do that by patching the store
        # Actually InMemory uses set comprehension so it will include it
        manager = EncryptionKeyManager(store)
        registry = await manager.build_tenant_registry()
        from varco_core.encryption import EncryptionError

        with pytest.raises(EncryptionError):
            registry.encrypt(b"x", context="orphaned")


class TestEncryptionKeyManagerEnvelopeEncryption:
    """Tests for the ``master_encryptor`` (KEK) / envelope encryption path."""

    async def test_wrapped_flag_set_when_kek_present(self) -> None:
        store = InMemoryEncryptionKeyStore()
        kek = _make_encryptor()
        manager = EncryptionKeyManager(store, master_encryptor=kek)
        await manager.get_or_create_encryptor("acme")
        entries = await store.load_for_tenant("acme")
        assert entries[0].wrapped is True

    async def test_wrapped_flag_not_set_without_kek(self) -> None:
        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        await manager.get_or_create_encryptor("acme")
        entries = await store.load_for_tenant("acme")
        assert entries[0].wrapped is False

    async def test_encrypted_decrypt_round_trip_with_kek(self) -> None:
        """Encrypt data with wrapped key, rebuild encryptor from store, decrypt."""
        store = InMemoryEncryptionKeyStore()
        kek = _make_encryptor()
        manager = EncryptionKeyManager(store, master_encryptor=kek)

        enc = await manager.get_or_create_encryptor("acme")
        ciphertext = enc.encrypt(b"envelope test")

        # Simulate restart — new manager, same store, same KEK
        manager2 = EncryptionKeyManager(store, master_encryptor=kek)
        enc2 = await manager2.get_or_create_encryptor("acme")
        assert enc2.decrypt(ciphertext) == b"envelope test"

    async def test_missing_kek_on_materialize_raises(self) -> None:
        """
        If a key was stored as ``wrapped=True`` but the new manager has
        no master_encryptor, materialisation must fail with RuntimeError.
        """
        store = InMemoryEncryptionKeyStore()
        kek = _make_encryptor()
        manager = EncryptionKeyManager(store, master_encryptor=kek)
        await manager.get_or_create_encryptor("acme")

        # Manager without KEK tries to materialise a wrapped entry
        manager_no_kek = EncryptionKeyManager(store)
        with pytest.raises(RuntimeError, match="master_encryptor"):
            await manager_no_kek.get_or_create_encryptor("acme")

    def test_repr_contains_info(self) -> None:
        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        r = repr(manager)
        assert "EncryptionKeyManager" in r
        assert "fernet" in r
