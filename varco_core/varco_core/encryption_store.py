"""
varco_core.encryption_store
============================
Persistent encryption key management — ``EncryptionKeyStore`` protocol,
``EncryptionKeyEntry`` value object, in-memory implementation, and the
high-level ``EncryptionKeyManager`` that ties everything together.

Purpose
-------
The earlier ``TenantAwareEncryptorRegistry`` solves *routing* (which key to use
for which tenant at runtime).  This module solves *persistence*: how to:

1. **Generate** a fresh DEK (Data Encryption Key) per tenant.
2. **Store** it durably so it survives pod/container restarts.
3. **Share** it across multiple replicas (the same store can be Redis or a DB
   table that all pods read).
4. **Recover** it on startup by loading all entries from the store and
   rebuilding a ready-to-use ``TenantAwareEncryptorRegistry``.
5. **Rotate** it without downtime — new DEK stored alongside the old one until
   all existing ciphertext has been re-encrypted.

Envelope encryption pattern
----------------------------
Raw key material (the DEK) is optionally wrapped by a master KEK before being
written to the store::

    store_bytes = kek.encrypt(dek_bytes)

On load::

    dek_bytes = kek.decrypt(store_bytes)

When no KEK is configured, ``key_material`` is stored **in plaintext** — this
is acceptable for development / trusted-storage scenarios (e.g. a table
protected by DB-level access controls) but a KEK is strongly recommended for
production.  The ``wrapped`` flag on each entry records whether wrapping was
applied so that the manager uses the correct unwrap path.

DESIGN: Protocol + in-memory + backend implementations
  ✅ ``EncryptionKeyStore`` is a structural Protocol — any class with the right
     async methods satisfies it.  No ABC needed.
  ✅ ``InMemoryEncryptionKeyStore`` is the test double — no Docker required.
  ✅ Backend stores (``SAEncryptionKeyStore``, ``RedisEncryptionKeyStore``) live
     in their respective packages to avoid adding optional deps to varco-core.
  ❌ The manager is eagerly synchronous for ``get_or_create_encryptor`` — callers
     in async contexts should ``await`` the async overload or use
     ``build_tenant_registry()`` at startup rather than on every request.

Usage example::

    from cryptography.fernet import Fernet
    from varco_core.encryption import FernetFieldEncryptor
    from varco_core.encryption_store import EncryptionKeyManager
    from varco_sa.encryption_store import SAEncryptionKeyStore

    # Master key — load from Vault / env; never store in code
    kek = FernetFieldEncryptor(Fernet.generate_key())

    store = SAEncryptionKeyStore(session_factory)
    manager = EncryptionKeyManager(store, master_encryptor=kek)

    # At startup: load all existing tenant keys
    registry = await manager.build_tenant_registry()

    # On first request for a new tenant: generate + save
    enc = await manager.get_or_create_encryptor("acme")

    # Key rotation (zero-downtime):
    new_enc = await manager.rotate("acme")

Thread safety:  ⚠️ ``EncryptionKeyManager._cache`` is a plain dict — safe after
                ``build_tenant_registry()`` completes but NOT during concurrent
                ``get_or_create_encryptor`` calls.  For concurrent first-tenant
                access, lock externally or rely on ``build_tenant_registry()``
                at startup to warm the cache before requests begin.
Async safety:   ✅ All ``EncryptionKeyManager`` methods are async — safe to
                ``await`` from any async context.
"""

from __future__ import annotations

import base64
import dataclasses
import uuid
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from varco_core.encryption import FieldEncryptor


# ════════════════════════════════════════════════════════════════════════════════
# EncryptionKeyEntry — immutable value object
# ════════════════════════════════════════════════════════════════════════════════


@dataclasses.dataclass(frozen=True)
class EncryptionKeyEntry:
    """
    Immutable record representing one stored encryption key.

    This is the unit of storage — every ``EncryptionKeyStore`` implementation
    persists and retrieves ``EncryptionKeyEntry`` instances.

    Attributes:
        kid:          Key ID — globally unique identifier (e.g. a UUID or
                      ``"tenant:acme:v2"``).
        algorithm:    Encryption algorithm name — used by the manager to select
                      the right ``FieldEncryptor`` subclass.  Currently only
                      ``"fernet"`` is supported.
        key_material: The key bytes, base64url-encoded (no padding).  When
                      ``wrapped=True``, this is the *ciphertext* of the raw
                      key bytes (encrypted with the master KEK).  When
                      ``wrapped=False``, this is the raw key bytes directly.
        created_at:   UTC timestamp of when this entry was created.  Used to
                      order keys for rotation history.
        tenant_id:    Optional tenant this key belongs to.  ``None`` means a
                      global / application-level key.
        is_primary:   Whether this is the currently active encryption key for
                      its tenant.  At most one entry per ``(tenant_id, algorithm)``
                      should be primary — stores do not enforce this; the
                      manager does.
        wrapped:      True if ``key_material`` was encrypted with the master
                      KEK before storage.  ``EncryptionKeyManager`` uses this
                      flag to decide whether to unwrap before materialising
                      a ``FieldEncryptor``.

    Thread safety:  ✅ frozen=True — immutable; safe to share across threads.
    Async safety:   ✅ Pure value object — no I/O.
    """

    # Unique key identifier — use UUID4 for generated keys
    kid: str

    # Algorithm hint — currently "fernet" only; extensible to "chacha20" etc.
    algorithm: str

    # base64url-encoded key material (possibly wrapped with master KEK)
    key_material: str

    # UTC creation timestamp — used for rotation ordering
    created_at: datetime

    # Nullable — None = global / app-level key
    tenant_id: str | None = None

    # Whether this entry is the active encryption key for its tenant
    is_primary: bool = True

    # Whether key_material was encrypted with the master KEK before storage
    wrapped: bool = False

    # ── Serialisation helpers ─────────────────────────────────────────────────

    def to_dict(self) -> dict[str, object]:
        """
        Serialise to a JSON-compatible dict for persistence (DB, Redis, files).

        Returns:
            Dict with all fields; ``created_at`` as ISO-8601 string.

        Edge cases:
            - ``tenant_id=None`` is serialised as ``null`` / ``None``.
        """
        return {
            "kid": self.kid,
            "algorithm": self.algorithm,
            "key_material": self.key_material,
            "created_at": self.created_at.isoformat(),
            "tenant_id": self.tenant_id,
            "is_primary": self.is_primary,
            "wrapped": self.wrapped,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> EncryptionKeyEntry:
        """
        Deserialise from a dict produced by ``to_dict()``.

        Args:
            data: Dict with the same keys as ``to_dict()`` output.

        Returns:
            ``EncryptionKeyEntry`` with all fields populated.

        Raises:
            KeyError:    A required field is missing.
            ValueError:  ``created_at`` is not a valid ISO-8601 string.
        """
        # datetime.fromisoformat handles the ISO-8601 strings we produce —
        # Python 3.11+ accepts timezone suffixes; on 3.10 we use strptime fallback.
        created_at_raw = data["created_at"]
        if isinstance(created_at_raw, datetime):
            created_at = created_at_raw
        else:
            try:
                created_at = datetime.fromisoformat(str(created_at_raw))
            except ValueError:
                # Python <3.11 does not accept the "Z" suffix — replace it
                created_at = datetime.fromisoformat(
                    str(created_at_raw).replace("Z", "+00:00")
                )
            # Ensure timezone-aware (assume UTC if naive)
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=UTC)

        return cls(
            kid=str(data["kid"]),
            algorithm=str(data["algorithm"]),
            key_material=str(data["key_material"]),
            created_at=created_at,
            tenant_id=data.get("tenant_id"),  # type: ignore[arg-type]
            is_primary=bool(data.get("is_primary", True)),
            wrapped=bool(data.get("wrapped", False)),
        )


# ════════════════════════════════════════════════════════════════════════════════
# EncryptionKeyStore — structural protocol
# ════════════════════════════════════════════════════════════════════════════════


@runtime_checkable
class EncryptionKeyStore(Protocol):
    """
    Async key persistence protocol — any class that implements these five
    methods satisfies the protocol without inheriting from it.

    Implementations are provided in the backend packages:
    - ``varco_sa.encryption_store.SAEncryptionKeyStore``   (PostgreSQL / SQLite)
    - ``varco_redis.encryption_store.RedisEncryptionKeyStore``  (Redis)
    - ``InMemoryEncryptionKeyStore`` (this module, for tests)

    Thread safety:  ✅ Implementations must be safe to call concurrently.
    Async safety:   ✅ All methods are async — use ``await`` in async contexts.
    """

    async def save(self, entry: EncryptionKeyEntry) -> None:
        """
        Persist an ``EncryptionKeyEntry``.

        Upsert semantics — if an entry with the same ``kid`` already exists,
        replace it.  This allows ``rotate()`` to promote a new key as primary
        without a separate "update is_primary" call.

        Args:
            entry: The key entry to persist.

        Raises:
            Exception: Implementation-specific storage error.
        """
        ...

    async def load(self, kid: str) -> EncryptionKeyEntry | None:
        """
        Load a single entry by ``kid``.

        Args:
            kid: Key ID to retrieve.

        Returns:
            The entry, or ``None`` when not found.
        """
        ...

    async def load_for_tenant(self, tenant_id: str | None) -> list[EncryptionKeyEntry]:
        """
        Load all entries for a given tenant (or global entries when
        ``tenant_id=None``).

        Returns:
            List of all matching entries, ordered by ``created_at`` ascending.
            Empty list when no entries exist for this tenant.
        """
        ...

    async def list_tenants(self) -> list[str]:
        """
        Return distinct ``tenant_id`` values for all stored entries.

        Used by ``EncryptionKeyManager.build_tenant_registry()`` to discover
        which tenants have keys without loading all key material upfront.

        Returns:
            Sorted list of tenant ID strings.  Global (``tenant_id=None``)
            entries are excluded.
        """
        ...

    async def delete(self, kid: str) -> None:
        """
        Remove an entry by ``kid``.

        No-op if the entry does not exist.  Use this after re-encrypting all
        rows encrypted with this key — equivalent to ``retire()`` in the
        ``MultiKeyEncryptorRegistry`` rotation workflow.

        Args:
            kid: Key ID to remove.
        """
        ...


# ════════════════════════════════════════════════════════════════════════════════
# InMemoryEncryptionKeyStore — test double
# ════════════════════════════════════════════════════════════════════════════════


class InMemoryEncryptionKeyStore:
    """
    In-memory ``EncryptionKeyStore`` implementation for unit tests.

    All data is stored in a plain dict — no external dependencies required.
    Data does NOT survive process restarts (by design — use a real backend
    for production).

    Thread safety:  ❌ Not thread-safe — plain dict with no locking.
                    For async unit tests this is fine because asyncio is
                    single-threaded per event loop.
    Async safety:   ✅ All methods are ``async`` — safe to await.

    Edge cases:
        - ``save()`` is upsert — existing entries with the same ``kid`` are
          silently replaced.
        - ``load_for_tenant(None)`` returns entries where ``tenant_id`` is ``None``
          (global keys), not all entries.
        - Concurrent reads are safe; concurrent write+read is not.

    Example::

        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)
        enc = await manager.get_or_create_encryptor("acme")
    """

    def __init__(self) -> None:
        # kid → entry; plain dict is sufficient for single-threaded async tests
        self._entries: dict[str, EncryptionKeyEntry] = {}

    def __repr__(self) -> str:
        return f"InMemoryEncryptionKeyStore(count={len(self._entries)})"

    async def save(self, entry: EncryptionKeyEntry) -> None:
        """Upsert an entry by kid."""
        self._entries[entry.kid] = entry

    async def load(self, kid: str) -> EncryptionKeyEntry | None:
        """Return the entry for ``kid`` or ``None``."""
        return self._entries.get(kid)

    async def load_for_tenant(self, tenant_id: str | None) -> list[EncryptionKeyEntry]:
        """
        Return all entries for the given ``tenant_id``, sorted by ``created_at``.
        """
        matches = [e for e in self._entries.values() if e.tenant_id == tenant_id]
        return sorted(matches, key=lambda e: e.created_at)

    async def list_tenants(self) -> list[str]:
        """Return sorted list of distinct non-None tenant IDs."""
        tenants = {
            e.tenant_id for e in self._entries.values() if e.tenant_id is not None
        }
        return sorted(tenants)

    async def delete(self, kid: str) -> None:
        """Remove an entry by kid — no-op if not found."""
        self._entries.pop(kid, None)


# ════════════════════════════════════════════════════════════════════════════════
# EncryptionKeyManager — high-level key lifecycle management
# ════════════════════════════════════════════════════════════════════════════════


class EncryptionKeyManager:
    """
    High-level manager for per-tenant encryption key lifecycle.

    Wraps an ``EncryptionKeyStore`` with:

    - **Generation**: create a fresh DEK per tenant on first use.
    - **Persistence**: save to the store so all replicas share the same key.
    - **Recovery**: load all tenant keys from the store on startup.
    - **Rotation**: generate a new primary DEK while keeping old ones for
      reading existing ciphertext (same zero-downtime model as
      ``MultiKeyEncryptorRegistry``).
    - **Wrapping**: optionally encrypt DEK bytes with a master KEK before
      storing them — so raw key material never sits unprotected in the store.

    Startup pattern
    ---------------
    Call ``build_tenant_registry()`` once at startup to warm the in-process
    cache and build a ``TenantAwareEncryptorRegistry``::

        manager = EncryptionKeyManager(store, master_encryptor=kek)
        registry = await manager.build_tenant_registry()
        orm_cls, mapper = factory.build(Patient, encryptor=registry, tenant_id_field="tenant_id")

    Per-request pattern
    -------------------
    If tenants are created dynamically, call ``get_or_create_encryptor(tenant_id)``
    which generates + saves a new key on first call and caches it in memory::

        enc = await manager.get_or_create_encryptor(request.tenant_id)

    Args:
        store:            Persistent key store (DB, Redis, or in-memory for tests).
        master_encryptor: Optional KEK for envelope encryption.  When set, DEK
                          bytes are encrypted before being written to the store
                          (``wrapped=True``).  The KEK itself is never stored —
                          it must be loaded from a secure source (Vault, env var,
                          HSM) on every startup.
        algorithm:        Key algorithm to use when generating new DEKs.
                          Currently only ``"fernet"`` is supported.

    DESIGN: in-process cache
      ✅ ``FieldEncryptor`` instances are expensive to create (imports Fernet,
         does base64 decoding).  Caching avoids repeated deserialisation on
         every request.
      ✅ Cache is populated lazily by ``get_or_create_encryptor`` and eagerly
         by ``build_tenant_registry()`` — both paths converge.
      ❌ Cache is a plain dict — NOT safe for concurrent first-access from
         multiple coroutines.  Warm the cache at startup via
         ``build_tenant_registry()`` before accepting requests.

    Thread safety:  ⚠️ See DESIGN note above.
    Async safety:   ✅ All public methods are async.

    Edge cases:
        - Calling ``get_or_create_encryptor`` for the same new tenant
          concurrently from multiple coroutines may generate and save multiple
          DEKs.  Use ``build_tenant_registry()`` at startup to avoid this.
        - ``rotate()`` marks the *old* primary ``is_primary=False`` before
          creating the new one — an atomic DB operation is not guaranteed.
          In practice, the window where both are ``is_primary=True`` is
          sub-millisecond.

    Example::

        from varco_core.encryption import FernetFieldEncryptor
        from varco_core.encryption_store import EncryptionKeyManager, InMemoryEncryptionKeyStore

        store = InMemoryEncryptionKeyStore()
        manager = EncryptionKeyManager(store)

        enc = await manager.get_or_create_encryptor("acme")
        registry = await manager.build_tenant_registry()
        new_enc = await manager.rotate("acme")
    """

    def __init__(
        self,
        store: EncryptionKeyStore,
        *,
        master_encryptor: FieldEncryptor | None = None,
        algorithm: str = "fernet",
    ) -> None:
        self._store = store
        self._master = master_encryptor
        self._algorithm = algorithm

        # tenant_id → FernetFieldEncryptor — warmed at startup or lazily
        # Plain dict: safe after build_tenant_registry(); not safe for concurrent
        # first-access from multiple coroutines (see class docstring).
        self._cache: dict[str | None, FieldEncryptor] = {}

    def __repr__(self) -> str:
        return (
            f"EncryptionKeyManager("
            f"algorithm={self._algorithm!r}, "
            f"cached_tenants={list(self._cache.keys())}, "
            f"wrapped={self._master is not None})"
        )

    # ── Public API ────────────────────────────────────────────────────────────

    async def get_or_create_encryptor(
        self, tenant_id: str | None = None
    ) -> FieldEncryptor:
        """
        Return the active ``FieldEncryptor`` for ``tenant_id``, creating one
        if none exists.

        On first call for a new tenant:
        1. Generates a fresh DEK.
        2. Optionally wraps it with the master KEK.
        3. Saves the entry to the store.
        4. Caches the ``FieldEncryptor`` in memory.

        On subsequent calls (cache hit):
        - Returns the cached ``FieldEncryptor`` immediately — no store I/O.

        Args:
            tenant_id: Tenant identifier.  ``None`` means a global / app-level
                       key (not per-tenant).

        Returns:
            The active ``FieldEncryptor`` for this tenant.

        Raises:
            Exception: Store save failed.

        Edge cases:
            - Concurrent first-access may generate multiple DEKs.  Warm the
              cache at startup with ``build_tenant_registry()`` to avoid this.

        Thread safety:  ⚠️ Not safe for concurrent first-access (see class docs).
        Async safety:   ✅ Awaits ``store.save()``.
        """
        # Fast path — return cached encryptor without any I/O
        cached = self._cache.get(tenant_id, None)
        if cached is not None:
            return cached

        # Check store for an existing primary key (maybe another pod stored it)
        entries = await self._store.load_for_tenant(tenant_id)
        primary = next((e for e in entries if e.is_primary), None)

        if primary is not None:
            enc = self._materialize(primary)
            self._cache[tenant_id] = enc
            return enc

        # No key exists — generate and save a new one
        enc, entry = self._generate(tenant_id=tenant_id, is_primary=True)
        await self._store.save(entry)
        self._cache[tenant_id] = enc
        return enc

    async def build_tenant_registry(
        self,
    ) -> TenantAwareEncryptorRegistry:
        """
        Load all tenant keys from the store and build a ready-to-use
        ``TenantAwareEncryptorRegistry``.

        Call this **once at startup** before accepting requests.  It warms the
        in-process cache and returns a registry that the mapper can use for
        tenant-aware encryption::

            registry = await manager.build_tenant_registry()
            orm_cls, mapper = factory.build(
                Patient,
                encryptor=registry,
                tenant_id_field="tenant_id",
            )

        Returns:
            ``TenantAwareEncryptorRegistry`` with one encryptor per tenant that
            has a primary key in the store.  Tenants with no primary key are
            absent from the registry.

        Raises:
            Exception: Store I/O error.

        Thread safety:  ⚠️ Mutates ``self._cache`` — call once at startup before
                        concurrent request handling begins.
        Async safety:   ✅ Awaits ``store.list_tenants()`` and
                        ``store.load_for_tenant()``.

        Edge cases:
            - Tenants with entries but no ``is_primary=True`` entry are skipped.
            - A tenant with multiple ``is_primary=True`` entries (corrupt state)
              uses the most recently created one.
        """
        # Import lazily — TenantAwareEncryptorRegistry is defined in this package
        from varco_core.encryption import TenantAwareEncryptorRegistry

        registry = TenantAwareEncryptorRegistry()

        tenant_ids = await self._store.list_tenants()

        for tenant_id in tenant_ids:
            entries = await self._store.load_for_tenant(tenant_id)

            # When multiple primaries exist (corrupt state), take the newest one
            primaries = sorted(
                (e for e in entries if e.is_primary),
                key=lambda e: e.created_at,
                reverse=True,
            )
            if not primaries:
                # No primary for this tenant — skip (old/retired entries only)
                continue

            primary = primaries[0]
            enc = self._materialize(primary)

            # Warm the cache while building the registry — avoids duplicate work
            self._cache[tenant_id] = enc
            registry.register(tenant_id, enc)

        return registry

    async def rotate(self, tenant_id: str | None = None) -> FieldEncryptor:
        """
        Rotate the encryption key for ``tenant_id``.

        Rotation workflow:
        1. Load the current primary entry and mark it ``is_primary=False``
           (the old key stays in the store for decrypting existing ciphertext).
        2. Generate a fresh DEK, save it as the new primary.
        3. Update the in-process cache.

        The old key is NOT deleted — callers must re-encrypt all existing
        ciphertext before calling ``store.delete(old_kid)``.  This mirrors
        the ``MultiKeyEncryptorRegistry.retire()`` safety contract.

        Args:
            tenant_id: Tenant to rotate.  ``None`` = global key.

        Returns:
            The new ``FieldEncryptor`` (now the active key for this tenant).

        Raises:
            KeyError:   No primary key found for this tenant — cannot rotate
                        a tenant that has never been initialised.  Call
                        ``get_or_create_encryptor()`` first.
            Exception:  Store I/O error.

        Thread safety:  ⚠️ Not atomic across two store writes.
        Async safety:   ✅ Awaits store operations.

        Edge cases:
            - If ``tenant_id`` has no primary key (never initialised), raises
              ``KeyError`` rather than silently creating a new one — use
              ``get_or_create_encryptor`` for that.
            - The old entry (``is_primary=False``) remains in the store
              indefinitely — callers manage its lifecycle via ``store.delete()``.
        """
        # Find the current primary so we can demote it
        entries = await self._store.load_for_tenant(tenant_id)
        current_primary = next((e for e in entries if e.is_primary), None)

        if current_primary is None:
            raise KeyError(
                f"Cannot rotate key for tenant {tenant_id!r} — no primary key "
                f"exists in the store.  Call get_or_create_encryptor() first."
            )

        # Demote the current primary — preserve all other fields
        demoted = dataclasses.replace(current_primary, is_primary=False)
        await self._store.save(demoted)

        # Generate and save the new primary
        new_enc, new_entry = self._generate(tenant_id=tenant_id, is_primary=True)
        await self._store.save(new_entry)

        # Update cache — the old encryptor is no longer needed for new writes
        self._cache[tenant_id] = new_enc
        return new_enc

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _generate(
        self,
        *,
        tenant_id: str | None,
        is_primary: bool,
    ) -> tuple[FieldEncryptor, EncryptionKeyEntry]:
        """
        Generate a fresh DEK, optionally wrap it, and build the ``FieldEncryptor``
        and ``EncryptionKeyEntry`` pair.

        Args:
            tenant_id:  Tenant for this entry.
            is_primary: Whether this is the active key.

        Returns:
            ``(FernetFieldEncryptor, EncryptionKeyEntry)`` ready to use and save.

        Edge cases:
            - ``kid`` is a UUID4 string — globally unique without coordination.
        """
        # Import lazily — cryptography.fernet is optional at module level but
        # is a hard dep of varco-core (via jwk + encryption)
        from cryptography.fernet import Fernet

        # Generate fresh raw key bytes via Fernet (uses os.urandom internally)
        fernet_key: bytes = Fernet.generate_key()  # 44-char base64url bytes

        # Decode to raw bytes for storage
        raw_bytes = base64.urlsafe_b64decode(fernet_key)

        # Optionally wrap raw bytes with the master KEK for at-rest protection
        if self._master is not None:
            key_material_bytes = self._master.encrypt(raw_bytes)
            wrapped = True
        else:
            key_material_bytes = raw_bytes
            wrapped = False

        # Encode as base64url (no padding) for storage — works for both
        # raw bytes and ciphertext bytes
        key_material = (
            base64.urlsafe_b64encode(key_material_bytes).rstrip(b"=").decode("ascii")
        )

        entry = EncryptionKeyEntry(
            kid=str(uuid.uuid4()),
            algorithm=self._algorithm,
            key_material=key_material,
            created_at=datetime.now(UTC),
            tenant_id=tenant_id,
            is_primary=is_primary,
            wrapped=wrapped,
        )

        # Build the FernetFieldEncryptor from the raw (un-wrapped) key
        from varco_core.encryption import FernetFieldEncryptor

        enc = FernetFieldEncryptor(fernet_key)
        return enc, entry

    def _materialize(self, entry: EncryptionKeyEntry) -> FieldEncryptor:
        """
        Reconstruct a ``FieldEncryptor`` from a stored ``EncryptionKeyEntry``.

        If the entry was wrapped (``entry.wrapped=True``), the master KEK is
        used to decrypt the key material first.

        Args:
            entry: A stored key entry.

        Returns:
            A ``FernetFieldEncryptor`` backed by the entry's key material.

        Raises:
            ValueError: ``entry.algorithm`` is not ``"fernet"``.
            EncryptionError: Master KEK decryption failed (wrong master key or
                tampered entry).
            RuntimeError: The entry was stored with ``wrapped=True`` but no
                master encryptor is configured.

        Edge cases:
            - Only ``algorithm="fernet"`` is currently supported.
              Future algorithms (``"chacha20"``, ``"aes-gcm"``) can be added
              by extending this dispatch.
        """
        if entry.algorithm != "fernet":
            raise ValueError(
                f"EncryptionKeyManager._materialize: unsupported algorithm "
                f"{entry.algorithm!r}.  Only 'fernet' is supported.  "
                f"Extend this method to add support for other algorithms."
            )

        # Decode from storage format (base64url no padding) back to bytes
        padding = 4 - len(entry.key_material) % 4
        padded = entry.key_material + ("=" * padding if padding != 4 else "")
        raw_or_wrapped = base64.urlsafe_b64decode(padded)

        if entry.wrapped:
            # Unwrap — decrypt with master KEK to recover raw key bytes
            if self._master is None:
                raise RuntimeError(
                    f"Key entry {entry.kid!r} was stored with wrapped=True but "
                    f"no master_encryptor is configured in EncryptionKeyManager. "
                    f"Provide the same master KEK that was used when the key was generated."
                )
            raw_bytes = self._master.decrypt(raw_or_wrapped)
        else:
            raw_bytes = raw_or_wrapped

        # Re-encode raw bytes as Fernet key (base64url with padding)
        fernet_key = base64.urlsafe_b64encode(raw_bytes)

        from varco_core.encryption import FernetFieldEncryptor

        return FernetFieldEncryptor(fernet_key)


# ── Avoid circular import — TenantAwareEncryptorRegistry is in encryption.py ──
# ``build_tenant_registry`` imports it lazily inside the method body,
# so this module-level import only provides the type annotation for readers.
if TYPE_CHECKING:
    from varco_core.encryption import TenantAwareEncryptorRegistry
